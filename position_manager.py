"""
QF×JP Bot v7.8 — Position Manager TRAILING STOP DINÁMICO
═══════════════════════════════════════════════════════════════════════════════
FIX v7.8 — Momentum exit: salida anticipada por deceleración de RSI
  Nuevo _check_momentum_exit(): sale de posiciones en profit cuando el RSI
  empieza a decelerarse en zona overbought/oversold, antes de que el precio
  revierta. Inspirado en la señal de TP del Turbo Oscillator (RunRox):
  avg > 60 AND speed_now < speed_prev (el momentum pierde fuerza).

  Solo aplica cuando trailing_active=True (posición ya en profit) — si el
  trailing aún no se activó, el time_stop y EMA exit se encargan.
  Desactivado por defecto (MOMENTUM_EXIT_ENABLED=False).

  Configurable con:
    MOMENTUM_EXIT_ENABLED=true/false (default false)
    MOMENTUM_EXIT_RSI_PERIOD=14      (periodo RSI)
    MOMENTUM_EXIT_OB=60              (zona overbought para LONG)
    MOMENTUM_EXIT_OS=40              (zona oversold para SHORT)
    MOMENTUM_EXIT_MIN_HOLD_MIN=5     (mínimo de minutos antes de evaluar)

FIX v7.7 — CRÍTICO: doble SL en BingX tras trail activation
  Causa raíz en _activate_trail(): dos bugs que se combinaban:

  Bug A) Cuando el precio cae por debajo de entry antes de que el trail
  pueda ejecutarse (condición habitual en dip-buys: el precio sube
  brevemente para activar el trail, luego retrocede), el `else` branch
  ("Precio revertió") logeaba correctamente que NO se debía cancelar el
  SL original — pero IGUALMENTE caía al código de emergency SL que hace
  cancel_all_orders() + coloca SL nuevo. Resultado: si el cancel_all
  fallaba (Bug B), quedaban el SL original Y el nuevo en BingX. Si el
  cancel_all tenía éxito, se reemplazaba el SL original (0.002738) por
  uno de emergencia peor calculado (0.002706 = mark * 0.985), perdiendo
  la protección del nivel de liquidez original.

  Bug B) El `except Exception: pass` en el path de emergency SL swallows
  errores de cancel_all_orders() en silencio — el código procedía a
  colocar el SL de emergencia SIN haber cancelado el original, creando
  el doble SL visible en BingX.

  Fix:
  - Cuando BE SL es inválido (precio revertió): return INMEDIATO desde
    el else branch. SL original en BingX sigue activo y es MEJOR
    protección que cualquier emergency SL calculado desde mark < entry.
    trailing_active=True pero trail_order_id="" → v7.3 reintentará en
    el próximo ciclo cuando el precio se recupere y BE sea válido.
  - Cuando cancel_all falla: log.error + return (reset trailing_active)
    en vez de swallow + proceder. Nunca colocar SL nuevo si no se pudo
    cancelar el viejo.
  - Emergency SL solo se llama en el único path legítimo: BE válido +
    cancel_all OK + BE placement falló (cancel_all ya corrió, hay que
    proteger la posición desnuda).
  - Caso real: ANIME-USDT, dos SL activos simultáneos (0.002738 original
    + 0.002706 de emergencia) tras la primera iteración del monitor.

FIX v7.6 (sin cambios):
  ✅ Auto-corrección de trade.direction contra BingX real en cada ciclo
     del monitor (_check_all_positions).

FIX v7.5 (sin cambios):
  ✅ reconcile: opened_at conservador (mitad del presupuesto de tiempo)
     en vez de 0.0 → time_stop y EMA exit no se desactivan en redeploys.

FIX v7.4 (sin cambios):
  ✅ EMA EXIT independiente del time_stop, más rápido.

FIX v7.3 (sin cambios):
  ✅ open_count solo de ESTE bot, no de toda la cuenta BingX.
  ✅ Reintento de _activate_trail si trailing_active=True pero
     trail_order_id="" (posición sin protección real).
  ✅ Si trailing_active=True pero trail_order_id vacío: reintentar
     _activate_trail() cada ciclo.

FIXES v7.0-v7.2 (sin cambios):
  ✅ Anti-loop 110412 (margen 0.5%, re-fetch mark, last_failed_sl)
  ✅ place-then-cancel: nunca sin SL durante el update del trail
  ✅ Qty sync con BingX real (TPs parciales)
═══════════════════════════════════════════════════════════════════════════════
"""
import asyncio
import logging
import time
from dataclasses import dataclass

import config as C
from bingx_client import BingXClient
from risk_manager import RiskManager
import telegram_client as tg

log = logging.getLogger("position_mgr")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _extract_order_id(resp: dict) -> str:
    """Extrae orderId de la respuesta de BingX (maneja varios formatos)."""
    data = resp.get("data", {})
    if isinstance(data, dict):
        oid = (data.get("order") or {}).get("orderId") or data.get("orderId", "")
        return str(oid) if oid else ""
    return ""


def _is_position_closed_error(resp: dict) -> bool:
    """
    BingX error 109420: 'position not exist' — la posición ya fue cerrada
    externamente (SL/TP disparado) pero el tracker interno aún no lo sabe.
    También captura 110025 (order would trigger immediately) como señal de cierre.
    """
    code = resp.get("code", 0) if isinstance(resp, dict) else 0
    return code in (109420, 110025)


def _sl_valid(sl_price: float, mark: float, direction: str) -> bool:
    """
    Valida que el precio de SL sea aceptable para BingX antes de enviarlo.
    - LONG SELL STOP: sl_price debe ser < mark (se dispara cuando baja)
    - SHORT BUY STOP: sl_price debe ser > mark (se dispara cuando sube)
    Margen 0.5% cubre spread + latencia en pares de precio bajo.
    """
    if sl_price <= 0:
        return False
    if direction == "LONG":
        return sl_price < mark * 0.995
    else:
        return sl_price > mark * 1.005


def _rsi_simple(closes: list, period: int = 14) -> list:
    """RSI sin dependencias — para _check_momentum_exit."""
    n = len(closes)
    if n < 2:
        return [50.0] * n
    out = [50.0] * n
    for i in range(1, n):
        lo = max(1, i - period + 1)
        gains  = [max(closes[j] - closes[j-1], 0) for j in range(lo, i+1)]
        losses = [max(closes[j-1] - closes[j], 0) for j in range(lo, i+1)]
        ag = sum(gains)  / len(gains)  if gains  else 0.0
        al = sum(losses) / len(losses) if losses else 0.0
        if al < 1e-12:
            out[i] = 100.0
        elif ag < 1e-12:
            out[i] = 0.0
        else:
            out[i] = 100.0 - 100.0 / (1.0 + ag / al)
    return out


def _ema(values: list, period: int) -> list:
    if not values:
        return []
    k = 2.0 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(out[-1] + k * (v - out[-1]))
    return out


# ── Dataclass ─────────────────────────────────────────────────────────────────

@dataclass
class OpenTrade:
    symbol:        str
    direction:     str
    entry:         float
    sl:            float
    tp1:           float
    tp2:           float
    qty:           float
    atr:           float
    order_id:      str
    be_moved:      bool  = False
    tp1_hit:       bool  = False
    position_side: str   = ""

    trailing_active:     bool  = False
    trail_sl:            float = 0.0
    peak_price:          float = 0.0
    trail_order_id:      str   = ""

    last_failed_sl:      float = 0.0
    activation_attempts: int   = 0
    opened_at:           float = 0.0


# ── Manager ───────────────────────────────────────────────────────────────────

class PositionManager:
    def __init__(self, client: BingXClient, risk: RiskManager, journal=None):
        self.client   = client
        self.risk     = risk
        self._journal = journal
        self._trades: dict[str, OpenTrade] = {}
        self._lock   = asyncio.Lock()
        self._trail_last_notify: dict[str, float] = {}

    # ── Reconciliar al arrancar ───────────────────────────────────────────────

    async def reconcile_on_startup(self):
        try:
            positions = await self.client.get_open_positions()
        except Exception as e:
            log.warning("reconcile_on_startup error: %s", e)
            return

        if not positions:
            log.info("reconcile: sin posiciones abiertas")
            return

        count = 0
        for pos in positions:
            sym = pos.get("symbol", "")
            amt = float(pos.get("positionAmt", 0) or 0)
            if not sym or amt == 0:
                continue
            direction_from_amt = "LONG" if amt > 0 else "SHORT"
            pos_side  = pos.get("positionSide", "BOTH")
            if pos_side not in ("LONG", "SHORT", "BOTH"):
                pos_side = "BOTH"

            if pos_side in ("LONG", "SHORT"):
                direction = pos_side
                if direction != direction_from_amt:
                    log.warning(
                        "[%s] ⚠️ Discrepancia dirección: amt sugiere %s pero "
                        "positionSide=%s (Hedge mode) — usando positionSide",
                        sym, direction_from_amt, pos_side,
                    )
            else:
                direction = direction_from_amt

            entry = float(pos.get("avgPrice", pos.get("entryPrice", 0)) or 0)
            qty   = abs(amt)
            sl    = entry * (0.99 if direction == "LONG" else 1.01)
            tp1   = entry * (1.02 if direction == "LONG" else 0.98)
            tp2   = entry * (1.04 if direction == "LONG" else 0.96)
            async with self._lock:
                self._trades[sym] = OpenTrade(
                    symbol=sym, direction=direction, entry=entry,
                    sl=sl, tp1=tp1, tp2=tp2, qty=qty,
                    atr=entry * 0.005,
                    order_id="reconciled",
                    position_side=pos_side,
                    trail_sl=sl,
                    peak_price=entry,
                    opened_at=time.time() - (getattr(C, 'MAX_HOLD_MINUTES', 60) * 60 * 0.5),
                )
            count += 1
            log.info("[%s] Reconciliado: %s qty=%.4f @ %.6f", sym, direction, qty, entry)

        if count:
            log.info("reconcile: %d posición(es) — colocando SL emergencia...", count)
            await self._place_emergency_sl_all()

    async def _place_emergency_sl_all(self):
        """
        Coloca SL inmediato en todas las posiciones reconciliadas.
        Cancela órdenes anteriores antes de colocar la nueva.
        """
        async with self._lock:
            trades = dict(self._trades)
        for sym, trade in trades.items():
            try:
                ticker = await self.client.get_ticker(sym)
                mark   = float(ticker.get("lastPrice", trade.entry) or trade.entry)
                if mark <= 0:
                    mark = trade.entry

                side_close = "SELL" if trade.direction == "LONG" else "BUY"
                sl_price   = mark * 0.98 if trade.direction == "LONG" else mark * 1.02

                try:
                    await self.client.cancel_all_orders(sym)
                    await asyncio.sleep(0.3)
                except Exception as ce:
                    log.debug("[%s] cancel_all_orders previo a SL emergencia: %s", sym, ce)

                log.info("[%s] SL emergencia: mark=%.6f sl=%.6f", sym, mark, sl_price)

                resp = await self.client.place_stop_market_order(
                    sym, side_close, trade.qty, sl_price,
                    trade.direction, order_type="STOP_MARKET",
                )
                if resp.get("code", -1) == 0:
                    oid = _extract_order_id(resp)
                    trade.sl             = sl_price
                    trade.trail_sl       = sl_price
                    trade.trail_order_id = oid
                    log.info("[%s] SL emergencia OK @ %.6f (oid=%s)", sym, sl_price, oid)
                else:
                    log.error("[%s] SL emergencia FALLIDO: %s", sym, resp)
            except Exception as e:
                log.error("[%s] _place_emergency_sl_all: %s", sym, e)
            await asyncio.sleep(0.4)

    # ── Registro ──────────────────────────────────────────────────────────────

    async def register_trade(self, trade: OpenTrade):
        if trade.opened_at == 0.0:
            trade.opened_at = time.time()
        async with self._lock:
            self._trades[trade.symbol] = trade
        await self.risk.on_trade_opened(symbol=trade.symbol, direction=trade.direction)
        log.info("[%s] Trade registrado %s @ %.6f", trade.symbol, trade.direction, trade.entry)

    async def remove_trade(self, symbol: str, pnl: float = 0.0):
        existed = False
        async with self._lock:
            if symbol in self._trades:
                del self._trades[symbol]
                existed = True
        if existed:
            self._trail_last_notify.pop(symbol, None)
            await self.risk.on_trade_closed(pnl=pnl, symbol=symbol)
            if self._journal is not None:
                await self._journal.on_close(symbol, pnl)

    # ── Monitor loop ──────────────────────────────────────────────────────────

    async def monitor_loop(self):
        log.info("Position monitor v7.8 — trailing stop + EMA exit + momentum exit + auto-corrección | intervalo=%ds",
                 C.POSITION_CHECK_INTERVAL)
        while True:
            try:
                await self._check_all_positions()
            except Exception as e:
                log.error("monitor_loop error: %s", e)
                await tg.notify_error("position_monitor", str(e))
            await asyncio.sleep(C.POSITION_CHECK_INTERVAL)

    async def _check_all_positions(self):
        try:
            real_positions = await self.client.get_open_positions()
        except Exception as e:
            log.warning("get_open_positions failed: %s", e)
            return

        real_map: dict[str, dict] = {
            p["symbol"]: p for p in real_positions
            if p.get("symbol") and float(p.get("positionAmt", 0)) != 0
        }

        async with self._lock:
            own_symbols = set(self._trades.keys())
        own_open_count = len(own_symbols & set(real_map.keys()))
        await self.risk.update_open_count(own_open_count)

        async with self._lock:
            tracked = dict(self._trades)

        for symbol, trade in tracked.items():

            if symbol not in real_map:
                try:
                    ticker      = await self.client.get_ticker(symbol)
                    close_price = float(ticker.get("lastPrice", trade.entry))
                except Exception:
                    close_price = trade.entry
                pnl = self._calc_pnl(trade, close_price)
                log.info("[%s] Cerrada externamente. PnL≈%.2f USDT", symbol, pnl)
                await tg.notify_trade_closed(
                    symbol, trade.direction, trade.entry,
                    close_price, trade.qty, "sl_tp_auto", pnl,
                )
                await self.remove_trade(symbol, pnl)
                continue

            pos = real_map[symbol]

            # ── FIX v7.6: auto-corrección de dirección ────────────────────────
            real_amt = float(pos.get("positionAmt", 0) or 0)
            real_ps  = pos.get("positionSide", "")
            real_direction = (
                real_ps if real_ps in ("LONG", "SHORT")
                else ("LONG" if real_amt > 0 else "SHORT")
            )
            if real_direction != trade.direction:
                log.warning(
                    "[%s] ⚠️ DIRECCIÓN CORREGIDA: tracker tenía %s, BingX confirma "
                    "%s (positionSide=%s, positionAmt=%.6f). Actualizando.",
                    symbol, trade.direction, real_direction, real_ps, real_amt,
                )
                trade.direction = real_direction

            # ── Mark price ────────────────────────────────────────────────────
            try:
                mark = float(pos.get("markPrice", 0) or 0)
                if mark <= 0:
                    ticker = await self.client.get_ticker(symbol)
                    mark   = float(ticker.get("lastPrice", trade.entry))
            except Exception:
                continue
            if mark <= 0:
                continue

            # ── Sync qty real ────────────────────────────────────────────────
            real_qty = abs(float(pos.get("positionAmt", trade.qty) or trade.qty))
            if real_qty > 0:
                drift = abs(real_qty - trade.qty) / max(trade.qty, 1e-12)
                if drift > 0.05:
                    log.info("[%s] qty sync: %.6f → %.6f (parcial TP?)",
                             symbol, trade.qty, real_qty)
                    trade.qty = real_qty

            # ── TP1 tracking ──────────────────────────────────────────────────
            if not trade.tp1_hit:
                tp1_hit = (
                    (trade.direction == "LONG"  and mark >= trade.tp1) or
                    (trade.direction == "SHORT" and mark <= trade.tp1)
                )
                if tp1_hit:
                    trade.tp1_hit = True
                    log.info("[%s] TP1 alcanzado @ %.6f", symbol, mark)

            # ── TIME STOP ─────────────────────────────────────────────────────
            if await self._check_time_stop(trade, mark, symbol):
                continue

            # ── EMA EXIT ──────────────────────────────────────────────────────
            if await self._check_ema_exit(trade, symbol):
                continue

            # ── MOMENTUM EXIT (FIX v7.8) ──────────────────────────────────────
            if await self._check_momentum_exit(trade, symbol):
                continue

            # ── Trailing Stop ─────────────────────────────────────────────────
            if not trade.trailing_active:
                activate_at = (
                    trade.entry + trade.atr * C.BREAKEVEN_ATR_MULT
                    if trade.direction == "LONG"
                    else trade.entry - trade.atr * C.BREAKEVEN_ATR_MULT
                )
                should_activate = (
                    (trade.direction == "LONG"  and mark >= activate_at) or
                    (trade.direction == "SHORT" and mark <= activate_at)
                )
                if should_activate:
                    await self._activate_trail(trade, mark)
            elif not trade.trail_order_id:
                # FIX v7.3: posición sin protección real — reintentar
                trade.activation_attempts += 1
                if trade.activation_attempts == 1 or trade.activation_attempts % 10 == 0:
                    log.warning(
                        "[%s] ⚠️ trailing_active=True pero SIN SL real en BingX "
                        "(intento #%d) — reintentando activación",
                        symbol, trade.activation_attempts,
                    )
                await self._activate_trail(trade, mark)
            else:
                await self._update_trail(trade, mark)

    # ── Activación del trailing ───────────────────────────────────────────────

    async def _activate_trail(self, trade: OpenTrade, current_mark: float):
        """
        Activa el trailing stop por primera vez.

        FIX v7.7 — CRÍTICO: eliminados los dos paths que creaban doble SL:

        Path A (Bug corregido): cuando _sl_valid(sl_be) era False (precio
        revertió bajo entry), el else logeaba "SL original sigue activo"
        pero IGUALMENTE caía al código de emergency SL que llamaba a
        cancel_all_orders(). Si ese cancel fallaba (Bug B), quedaban dos SLs.
        Fix: return inmediato desde el else — SL original en BingX es mejor
        protección. trailing_active=True, trail_order_id="" → v7.3 reintentará.

        Path B (Bug corregido): `except Exception: pass` en cancel_all del
        emergency path swallowaba errores en silencio y procedía a colocar
        SL nuevo aunque el cancel hubiera fallado. Fix: log.error + return.

        Emergency SL solo en el único path legítimo que lo necesita: BE
        válido + cancel_all OK + BE placement falló (originales ya cancelados,
        posición desnuda en BingX).
        """
        symbol = trade.symbol
        log.info("[%s] Trail activation — mark=%.6f entry=%.6f atr=%.6f",
                 symbol, current_mark, trade.entry, trade.atr)

        # Marcar activo AL INICIO — fix del loop 110412 heredado de v7.0
        trade.trailing_active = True
        trade.be_moved        = True
        trade.peak_price      = current_mark

        try:
            ticker = await self.client.get_ticker(symbol)
            mark   = float(ticker.get("lastPrice", current_mark) or current_mark)
            if mark <= 0:
                mark = current_mark
            trade.peak_price = mark

            sl_be      = trade.entry
            side_close = "SELL" if trade.direction == "LONG" else "BUY"

            if _sl_valid(sl_be, mark, trade.direction):
                # ── Precio favorable: intentar SL en breakeven ────────────────

                # FIX v7.7: cancel_all con manejo de error explícito.
                # Si falla, NO procedemos — no colocar SL nuevo sin haber
                # cancelado el original (causa del doble SL).
                try:
                    await self.client.cancel_all_orders(symbol)
                    await asyncio.sleep(0.3)
                except Exception as ce:
                    log.error(
                        "[%s] cancel_all_orders FALLÓ en trail activation: %s "
                        "— abortando para no crear SL duplicado. "
                        "Reintentará próximo ciclo (trail_order_id sigue vacío).",
                        symbol, ce,
                    )
                    # Reset para que v7.3 reintente limpiamente
                    trade.trailing_active = False
                    trade.be_moved        = False
                    return

                resp = await self.client.place_stop_market_order(
                    symbol, side_close, trade.qty, sl_be,
                    trade.direction, order_type="STOP_MARKET",
                )

                if resp.get("code", -1) == 0:
                    oid = _extract_order_id(resp)
                    trade.trail_sl       = sl_be
                    trade.trail_order_id = oid
                    trade.sl             = sl_be
                    self._trail_last_notify[symbol] = sl_be
                    log.info("[%s] 🎯 Trail ACTIVADO — SL @ breakeven %.6f | peak=%.6f | oid=%s",
                             symbol, sl_be, mark, oid)
                    await tg.send(
                        f"🎯 *TRAIL ACTIVADO* — `{symbol}` "
                        f"{'🟢' if trade.direction == 'LONG' else '🔴'}\n"
                        f"SL → breakeven `{sl_be:.6f}` | Mark: `{mark:.6f}`\n"
                        f"ATR: `{trade.atr:.6f}` | Peak: `{mark:.6f}`"
                    )
                    return

                if _is_position_closed_error(resp):
                    log.info("[%s] Trail BE: posición ya cerrada (109420) — limpiando", symbol)
                    pnl = self._calc_pnl(trade, mark)
                    await tg.notify_trade_closed(symbol, trade.direction, trade.entry,
                                                  mark, trade.qty, "sl_tp_auto(trail_detect)", pnl)
                    await self.remove_trade(symbol, pnl)
                    return

                # ── BE falló DESPUÉS de que cancel_all tuvo éxito ─────────────
                # Los originales (SL + TP1 + TP2) ya fueron cancelados.
                # La posición está desnuda → ESTE es el único caso legítimo
                # para emergency SL. Caemos al bloque de abajo.
                log.warning("[%s] BE @ entry falló tras cancel_all: %s — emergency SL", symbol, resp)

            else:
                # ── FIX v7.7 CRÍTICO: precio revertió bajo entry ──────────────
                # El SL original en BingX (colocado por open_trade()) está
                # ACTIVO y es MEJOR protección que cualquier emergency SL
                # calculado desde mark < entry. No cancelar nada, no colocar
                # nada nuevo.
                #
                # Antes: caía al bloque de emergency SL con already_cancelled=False
                # → cancel_all + place nuevo SL. Si el cancel fallaba silenciosamente
                # (except Exception: pass), quedaban DOS SLs activos. Si el cancel
                # tenía éxito, se reemplazaba el SL original por uno peor calculado
                # (mark * 0.985 cuando ya estamos bajo entry).
                #
                # trailing_active=True pero trail_order_id="" → v7.3 reintentará
                # cada ciclo. Cuando el precio se recupere y BE sea válido, la
                # activación tendrá éxito normalmente.
                log.warning(
                    "[%s] Trail activation: precio revertió antes de BE "
                    "(mark=%.6f, entry=%.6f, dir=%s). "
                    "SL original en BingX sigue activo — reintentando próximo ciclo.",
                    symbol, mark, trade.entry, trade.direction,
                )
                return  # EXIT EARLY — nada que hacer aquí

            # ── Emergency SL — SOLO aquí (BE válido + cancel_all OK + BE falló) ──
            # El cancel_all ya corrió, la posición está sin protección.
            # Hay que colocar algo cueste lo que cueste.
            try:
                t2 = await self.client.get_ticker(symbol)
                m2 = float(t2.get("lastPrice", mark) or mark)
                if m2 > 0:
                    mark = m2
                    trade.peak_price = mark
            except Exception:
                pass

            em_sl = mark * 0.985 if trade.direction == "LONG" else mark * 1.015

            if _sl_valid(em_sl, mark, trade.direction):
                em_resp = await self.client.place_stop_market_order(
                    symbol, side_close, trade.qty, em_sl,
                    trade.direction, order_type="STOP_MARKET",
                )
                if em_resp.get("code", -1) == 0:
                    oid = _extract_order_id(em_resp)
                    trade.trail_sl       = em_sl
                    trade.trail_order_id = oid
                    trade.sl             = em_sl
                    self._trail_last_notify[symbol] = em_sl
                    log.info("[%s] 🎯 Trail ACTIVADO (SL emergencia) @ %.6f | mark=%.6f",
                             symbol, em_sl, mark)
                    await tg.send(
                        f"🎯 *TRAIL ACTIVADO* (emergencia) — `{symbol}`\n"
                        f"SL @ `{em_sl:.6f}` | Mark: `{mark:.6f}`"
                    )
                elif _is_position_closed_error(em_resp):
                    log.info("[%s] Trail: posición ya cerrada (109420) — limpiando", symbol)
                    pnl = self._calc_pnl(trade, mark)
                    await tg.notify_trade_closed(symbol, trade.direction, trade.entry,
                                                  mark, trade.qty, "sl_tp_auto(trail_detect)", pnl)
                    await self.remove_trade(symbol, pnl)
                elif self.client.is_api_disabled_error(em_resp):
                    # Error 109400: BingX deshabilita órdenes durante volatilidad extrema.
                    # Es TEMPORAL (1-5 min). No es un bug — el bot reintentará en el
                    # próximo ciclo vía trail_order_id="". No enviar alerta Telegram.
                    log.warning(
                        "[%s] Trail activation: BingX API DESHABILITADA (109400) — "
                        "volatilidad extrema. Reintentará en próximo ciclo (FIX v7.3). "
                        "Posición temporalmente sin SL pero en profit (mark=%.6f entry=%.6f).",
                        symbol, mark, trade.entry,
                    )
                else:
                    # cancel_all ya corrió — esto es crítico, posición desnuda
                    log.error(
                        "[%s] Trail activation: SL emergencia FALLIDO tras cancel_all: %s "
                        "— POSICIÓN SIN PROTECCIÓN, reintentará próximo ciclo (v7.3)",
                        symbol, em_resp,
                    )
                    if trade.activation_attempts <= 1 or trade.activation_attempts % 10 == 0:
                        await tg.notify_error(
                            f"trail_activation({symbol})",
                            f"SL emergencia fallido TRAS cancel_all (intento #{trade.activation_attempts}) "
                            f"— POSICIÓN SIN PROTECCIÓN\n{em_resp}"
                        )
            else:
                log.error("[%s] Trail: no se puede calcular SL emergencia válido "
                          "para mark=%.6f dir=%s", symbol, mark, trade.direction)

        except Exception as e:
            log.error("[%s] _activate_trail error: %s — reintentará próximo ciclo", symbol, e)

    # ── Actualización del trailing ────────────────────────────────────────────

    async def _update_trail(self, trade: OpenTrade, mark: float):
        """
        Actualiza el trailing SL cuando el precio alcanza un nuevo peak.
        Estrategia PLACE-THEN-CANCEL: nunca sin SL durante el update.
        """
        symbol     = trade.symbol
        trail_dist = trade.atr * C.TRAIL_DISTANCE_ATR

        if trade.direction == "LONG":
            if mark <= trade.peak_price:
                return
            new_peak = mark
            new_sl   = new_peak - trail_dist
            if new_sl <= trade.trail_sl:
                trade.peak_price = new_peak
                return
        else:
            if trade.peak_price > 0 and mark >= trade.peak_price:
                return
            new_peak = mark
            new_sl   = new_peak + trail_dist
            if trade.trail_sl > 0 and new_sl >= trade.trail_sl:
                trade.peak_price = new_peak
                return

        if not _sl_valid(new_sl, mark, trade.direction):
            trade.peak_price = new_peak
            if trade.last_failed_sl and abs(new_sl - trade.last_failed_sl) < trade.atr * 0.05:
                log.debug("[%s] Trail: new_sl=%.6f repetido e inválido (mark=%.6f) — esperando nuevo peak",
                          symbol, new_sl, mark)
            else:
                log.debug("[%s] Trail: new_sl=%.6f inválido para mark=%.6f dir=%s",
                          symbol, new_sl, mark, trade.direction)
            trade.last_failed_sl = new_sl
            return

        fresh_mark = mark
        try:
            t = await self.client.get_ticker(symbol)
            fm = float(t.get("lastPrice", mark) or mark)
            if fm > 0:
                fresh_mark = fm
        except Exception:
            pass

        if not _sl_valid(new_sl, fresh_mark, trade.direction):
            trade.peak_price     = new_peak
            trade.last_failed_sl = new_sl
            log.debug("[%s] Trail: new_sl=%.6f inválido tras refresh (mark fresco=%.6f)",
                      symbol, new_sl, fresh_mark)
            return

        try:
            side_close = "SELL" if trade.direction == "LONG" else "BUY"

            resp = await self.client.place_stop_market_order(
                symbol, side_close, trade.qty, new_sl,
                trade.direction, order_type="STOP_MARKET",
            )

            if resp.get("code", -1) == 0:
                new_oid       = _extract_order_id(resp)
                old_oid       = trade.trail_order_id
                old_sl        = trade.trail_sl
                profit_locked = self._calc_pnl(trade, new_sl)

                trade.peak_price     = new_peak
                trade.trail_sl       = new_sl
                trade.trail_order_id = new_oid
                trade.sl             = new_sl
                trade.last_failed_sl = 0.0

                log.info("[%s] 📈 Trail: %.6f→%.6f | peak=%.6f | mark=%.6f | PnL@SL≈%.2f USDT",
                         symbol, old_sl, new_sl, new_peak, fresh_mark, profit_locked)

                # FIX v7.9 CANCEL RETRY: el cancel del SL viejo era best-effort
                # con un solo intento silencioso. Si falla repetidamente, cada
                # update del trail deja una orden huérfana — en producción se
                # acumularon 70 órdenes activas para 9 posiciones (10-11 por par).
                # Fix: 2 reintentos con backoff. Si los dos fallan: cancel_all_orders
                # (nuclear pero necesario — cancela todo el par y el nuevo SL
                # se recolocará en el próximo ciclo por el mecanismo v7.3).
                if old_oid and old_oid != new_oid:
                    await asyncio.sleep(0.15)
                    cancelled = False
                    for attempt in range(2):
                        try:
                            cr = await self.client.cancel_order(symbol, old_oid)
                            if isinstance(cr, dict) and cr.get("code", -1) == 0:
                                log.debug("[%s] Old trail SL %s cancelado (intento %d)",
                                          symbol, old_oid, attempt + 1)
                                cancelled = True
                                break
                            log.debug("[%s] cancel_order intento %d falló: %s",
                                      symbol, attempt + 1, cr)
                        except Exception as ce:
                            log.debug("[%s] cancel_order intento %d excepción: %s",
                                      symbol, attempt + 1, ce)
                        if attempt == 0:
                            await asyncio.sleep(0.5)

                    if not cancelled:
                        # Fallback: cancelar TODAS las órdenes del par y marcar
                        # trail_order_id vacío para que v7.3 recoloque en el
                        # próximo ciclo. Es seguro: el nuevo SL ya está activo
                        # en BingX (se colocó primero), el cancel_all solo borra
                        # órdenes viejas acumuladas.
                        log.warning(
                            "[%s] ⚠️ cancel_order viejo FALLIDO x2 — usando cancel_all "
                            "para limpiar órdenes huérfanas (oid=%s). "
                            "trail_order_id se resetea → v7.3 recolocará SL próximo ciclo.",
                            symbol, old_oid,
                        )
                        try:
                            await self.client.cancel_all_orders(symbol)
                            await asyncio.sleep(0.3)
                            trade.trail_order_id = ""  # v7.3 recolocará
                        except Exception as ca:
                            log.error("[%s] cancel_all_orders fallback también falló: %s",
                                      symbol, ca)

                last_sl = self._trail_last_notify.get(symbol, trade.entry)
                if abs(new_sl - last_sl) >= trade.atr:
                    self._trail_last_notify[symbol] = new_sl
                    pnl_icon = "💚" if profit_locked > 0 else "⚡"
                    await tg.send(
                        f"{pnl_icon} *TRAIL* — `{symbol}` "
                        f"{'🟢' if trade.direction == 'LONG' else '🔴'}\n"
                        f"SL: `{old_sl:.6f}` → `{new_sl:.6f}`\n"
                        f"Peak: `{new_peak:.6f}` | PnL@SL: `{profit_locked:+.2f} USDT`"
                    )

            else:
                if _is_position_closed_error(resp):
                    log.info("[%s] Trail update: posición ya cerrada (109420) — limpiando", symbol)
                    pnl = self._calc_pnl(trade, fresh_mark)
                    await tg.notify_trade_closed(symbol, trade.direction, trade.entry,
                                                  fresh_mark, trade.qty, "sl_tp_auto(trail_detect)", pnl)
                    await self.remove_trade(symbol, pnl)
                    return
                trade.peak_price     = new_peak
                trade.last_failed_sl = new_sl
                log.warning("[%s] Trail update falló new_sl=%.6f: %s", symbol, new_sl, resp)

        except Exception as e:
            trade.peak_price     = new_peak
            trade.last_failed_sl = new_sl
            log.error("[%s] _update_trail error: %s", symbol, e)

    # ── Time Stop ─────────────────────────────────────────────────────────────

    async def _check_time_stop(self, trade: OpenTrade, mark: float, symbol: str) -> bool:
        if trade.trailing_active:
            return False

        if trade.opened_at <= 0:
            trade.opened_at = time.time()
            return False

        elapsed_min = (time.time() - trade.opened_at) / 60.0
        max_hold    = getattr(C, 'MAX_HOLD_MINUTES', 60)
        if elapsed_min < max_hold:
            return False

        atr      = trade.atr if trade.atr > 0 else mark * 0.005
        progress = (mark - trade.entry) if trade.direction == "LONG" else (trade.entry - mark)
        min_prog = atr * getattr(C, 'TIME_STOP_MIN_PROGRESS_ATR', 0.5)

        if progress >= min_prog:
            return False

        log.warning(
            "[%s] ⏱ TIME STOP — %.0fmin sin progreso (prog=%.6f < min=%.6f). Cerrando.",
            symbol, elapsed_min, progress, min_prog,
        )
        await tg.notify_time_stop(symbol, trade.direction, trade.entry, mark,
                                   int(elapsed_min), progress)
        await self.close_position_emergency(symbol, reason="time_stop")
        return True

    # ── EMA Exit ─────────────────────────────────────────────────────────────

    async def _check_ema_exit(self, trade: OpenTrade, symbol: str) -> bool:
        if not getattr(C, 'EMA_EXIT_ENABLED', False):
            return False
        if trade.trailing_active:
            return False

        min_hold_min = getattr(C, 'EMA_EXIT_MIN_HOLD_MIN', 6)
        if trade.opened_at > 0:
            elapsed_min = (time.time() - trade.opened_at) / 60.0
            if elapsed_min < min_hold_min:
                return False

        period = getattr(C, 'EMA_EXIT_PERIOD', 9)
        try:
            klines = await self.client.get_klines(symbol, C.TIMEFRAME, period + 30)
        except Exception as e:
            log.debug("[%s] EMA exit klines error: %s", symbol, e)
            return False

        if len(klines) < period + 2:
            return False

        closes = [c[4] for c in klines]
        ema    = _ema(closes, period)
        if len(ema) < 2:
            return False

        last_closed_close = closes[-2]
        last_closed_ema   = ema[-2]

        exit_triggered = (
            (trade.direction == "LONG"  and last_closed_close < last_closed_ema) or
            (trade.direction == "SHORT" and last_closed_close > last_closed_ema)
        )
        if not exit_triggered:
            return False

        log.warning(
            "[%s] 📉 EMA(%d) EXIT — última vela cerrada %.6f %s EMA %.6f (%s). Cerrando.",
            symbol, period, last_closed_close,
            "<" if trade.direction == "LONG" else ">", last_closed_ema, trade.direction,
        )
        await self.close_position_emergency(symbol, reason=f"ema{period}_exit")
        return True

    # ── Momentum Exit (FIX v7.8) ─────────────────────────────────────────────

    async def _check_momentum_exit(self, trade: OpenTrade, symbol: str) -> bool:
        """
        MOMENTUM EXIT — sale cuando el RSI decelaera en zona OB/OS con la
        posición ya en profit (trailing activo). Inspirado en la señal de TP
        del Turbo Oscillator (RunRox): momentum pierde velocidad justo cuando
        el precio aún está en máximos/mínimos del movimiento.

        Condición para LONG:
          RSI > MOMENTUM_EXIT_OB (60) AND speed_now < speed_prev < 0
          → el RSI está en zona alta pero cada vez sube menos → techo inminente

        Condición para SHORT:
          RSI < MOMENTUM_EXIT_OS (40) AND speed_now > speed_prev > 0
          → el RSI está en zona baja pero cada vez cae menos → suelo inminente

        Solo actúa si:
          - trailing_active=True (posición ya en profit)
          - Han pasado MOMENTUM_EXIT_MIN_HOLD_MIN minutos desde la apertura
          - MOMENTUM_EXIT_ENABLED=True (default: False)

        Retorna True si cerró (el caller debe hacer `continue`).
        """
        if not getattr(C, 'MOMENTUM_EXIT_ENABLED', False):
            return False
        if not trade.trailing_active:
            return False   # solo para posiciones en profit con trailing activo

        min_hold = getattr(C, 'MOMENTUM_EXIT_MIN_HOLD_MIN', 5)
        if trade.opened_at > 0 and (time.time() - trade.opened_at) / 60 < min_hold:
            return False

        rsi_period = getattr(C, 'MOMENTUM_EXIT_RSI_PERIOD', 14)
        n_bars = rsi_period + 10   # suficientes para calcular velocidad

        try:
            klines = await self.client.get_klines(symbol, C.TIMEFRAME, n_bars)
        except Exception as e:
            log.debug("[%s] momentum exit klines error: %s", symbol, e)
            return False

        if len(klines) < rsi_period + 6:
            return False

        closes = [c[4] for c in klines]
        rsi_vals = _rsi_simple(closes, rsi_period)

        if len(rsi_vals) < 6:
            return False

        rsi_now = rsi_vals[-1]
        # Velocidad del RSI: cambio en las últimas 2 barras vs las 2 anteriores
        speed_now  = rsi_vals[-1] - rsi_vals[-3]
        speed_prev = rsi_vals[-3] - rsi_vals[-6]

        ob = getattr(C, 'MOMENTUM_EXIT_OB', 60.0)
        os = getattr(C, 'MOMENTUM_EXIT_OS', 40.0)

        if trade.direction == "LONG":
            # RSI en zona alta pero desacelerando (cada vez sube menos)
            exit_trigger = rsi_now > ob and speed_now < speed_prev and speed_now < 0
        else:
            # RSI en zona baja pero desacelerando (cada vez cae menos)
            exit_trigger = rsi_now < os and speed_now > speed_prev and speed_now > 0

        if not exit_trigger:
            return False

        log.warning(
            "[%s] ⚡ MOMENTUM EXIT — RSI=%.1f speed=%.2f→%.2f (dir=%s). Cerrando.",
            symbol, rsi_now, speed_prev, speed_now, trade.direction,
        )
        await self.close_position_emergency(symbol, reason="momentum_exit")
        return True

    # ── Cierre de emergencia ──────────────────────────────────────────────────

    async def close_position_emergency(self, symbol: str, reason: str = "emergency"):
        async with self._lock:
            trade = self._trades.get(symbol)
        if not trade:
            log.warning("[%s] close_emergency: no registrado", symbol)
            return
        try:
            await self.client.cancel_all_orders(symbol)
            await asyncio.sleep(0.2)
            await self.client.close_position_market(symbol, trade.qty, trade.direction)
            ticker      = await self.client.get_ticker(symbol)
            close_price = float(ticker.get("lastPrice", trade.entry))
            pnl         = self._calc_pnl(trade, close_price)
            log.info("[%s] Cierre emergencia. PnL=%.2f", symbol, pnl)
            await tg.notify_trade_closed(symbol, trade.direction, trade.entry,
                                         close_price, trade.qty, reason, pnl)
            await self.remove_trade(symbol, pnl)
        except Exception as e:
            log.error("[%s] close_emergency error: %s", symbol, e)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _calc_pnl(self, trade: OpenTrade, close_price: float) -> float:
        if trade.direction == "LONG":
            raw = (close_price - trade.entry) * trade.qty
        else:
            raw = (trade.entry - close_price) * trade.qty
        return round(raw * C.LEVERAGE, 4)

    def get_tracked(self) -> dict[str, OpenTrade]:
        return dict(self._trades)

    def is_trading(self, symbol: str) -> bool:
        return symbol in self._trades

    async def get_unrealized_pnl(self) -> float:
        async with self._lock:
            tracked = dict(self._trades)
        if not tracked:
            return 0.0

        try:
            real_positions = await self.client.get_open_positions()
        except Exception as e:
            log.warning("get_unrealized_pnl: get_open_positions failed: %s", e)
            return 0.0

        real_map: dict[str, dict] = {
            p["symbol"]: p for p in real_positions
            if p.get("symbol") and float(p.get("positionAmt", 0)) != 0
        }

        total = 0.0
        for symbol, trade in tracked.items():
            pos = real_map.get(symbol)
            if not pos:
                continue
            try:
                mark = float(pos.get("markPrice", 0) or 0)
            except Exception:
                continue
            if mark <= 0:
                continue
            total += self._calc_pnl(trade, mark)

        return round(total, 4)
