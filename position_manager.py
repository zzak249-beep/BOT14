"""
QF×JP Bot v7.2 — Position Manager TRAILING STOP DINÁMICO (FIX cuenta compartida)
═══════════════════════════════════════════════════════════════════════════════
FIX v7.2:
  ✅ open_count YA NO cuenta toda la cuenta BingX — solo las posiciones que
     ESTE bot trackea. Antes, con renewed-love + joyful-art + GEMMI
     compartiendo la misma cuenta/API, cada bot veía las posiciones de
     los OTROS bots reflejadas en su propio MAX_OPEN_TRADES, causando
     bloqueos (o desbloqueos) por actividad ajena al bot.

FIXES vs v7.0 (sin cambios):
  ✅ Loop infinito 110412 en pares de bajo precio (CATI-USDT, etc.):
     - Margen de _sl_valid ampliado 0.2% → 0.5% (cubre spread/tick/latencia)
     - Re-fetch de mark price justo antes de enviar el STOP_MARKET en
       _update_trail (la misma técnica que ya usaba _activate_trail)
     - Nuevo campo last_failed_sl: si el new_sl calculado es ~igual al que
       falló en el ciclo anterior y sigue siendo inválido, se descarta en
       silencio (log.debug) en vez de reintentar y volver a fallar igual

  (resto de v7.0 sin cambios: place-then-cancel, qty sync, BE activation,
   reconcile, notificaciones throttled)

NUEVO — Trailing Stop dinámico (sin cambios funcionales vs v7.0):
  • Se activa a BREAKEVEN_ATR_MULT ATR de beneficio (default 1.0)
  • El SL sigue el peak del precio a TRAIL_DISTANCE_ATR ATR de distancia
  • Estrategia place-then-cancel: máxima seguridad, nunca sin protección
  • Solo mueve SL a favor (LONG → arriba, SHORT → abajo), nunca en contra
  • Qty sincronizada con BingX real (maneja cierres parciales de TP1/TP2)
  • Notificaciones Telegram throttled: solo cada 1 ATR de mejora

EJEMPLO con ATR=0.010, entry=1.000 USDT, LONG:
  t=0:   Entrada | SL=0.980 (2 ATR) | TP1=1.020 | TP2=1.040
  t=1:   mark=1.010 → TRAIL ACTIVA → SL@entry=1.000 (breakeven)
  t=2:   mark=1.025 → peak=1.025 → SL=1.010 (+1% locked)
  t=3:   mark=1.040 → peak=1.040 → SL=1.025 (+2.5% locked)
  t=4:   mark=1.035 → sin cambio (no nuevo peak)
  t=5:   mark=1.060 → peak=1.060 → SL=1.045 (+4.5% locked!)
═══════════════════════════════════════════════════════════════════════════════
"""
import asyncio
import logging
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
    Detectar esto permite auto-limpiar el trade en vez de seguir reintentando.
    También captura 110025 (order would trigger immediately) como señal de cierre.
    """
    code = resp.get("code", 0) if isinstance(resp, dict) else 0
    return code in (109420, 110025)


def _sl_valid(sl_price: float, mark: float, direction: str) -> bool:
    """
    Valida que el precio de SL sea aceptable para BingX antes de enviarlo.
    - LONG SELL STOP: sl_price debe ser < mark (se dispara cuando baja)
    - SHORT BUY STOP: sl_price debe ser > mark (se dispara cuando sube)
    Evita el error 110412 "Stop Loss price should be greater/less than current price"

    FIX v7.1: margen ampliado de 0.2% → 0.5%. En pares de precio bajo
    (CATI-USDT, etc.) un margen de 0.2% no cubre el spread + latencia
    entre el cálculo y la ejecución de la orden, lo que provocaba que
    BingX rechazara repetidamente el mismo new_sl.
    """
    if sl_price <= 0:
        return False
    if direction == "LONG":
        return sl_price < mark * 0.995
    else:
        return sl_price > mark * 1.005


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
    be_moved:      bool  = False   # compat legacy — True cuando trailing activo
    tp1_hit:       bool  = False
    position_side: str   = ""      # LONG/SHORT/BOTH leído de BingX

    # ── Trailing stop ─────────────────────────────────────────────────────────
    trailing_active:  bool  = False  # trailing activado
    trail_sl:         float = 0.0    # precio del SL activo en BingX
    peak_price:       float = 0.0    # mejor precio visto en dirección favorable
    trail_order_id:   str   = ""     # orderId del STOP_MARKET activo en BingX

    # ── FIX v7.1: anti-loop de retries idénticos ─────────────────────────────
    last_failed_sl:   float = 0.0    # último new_sl que fue inválido/rechazado


# ── Manager ───────────────────────────────────────────────────────────────────

class PositionManager:
    def __init__(self, client: BingXClient, risk: RiskManager):
        self.client  = client
        self.risk    = risk
        self._trades: dict[str, OpenTrade] = {}
        self._lock   = asyncio.Lock()
        # Throttle Telegram: notificar trail solo cada 1 ATR de mejora por símbolo
        self._trail_last_notify: dict[str, float] = {}

    # ── Reconciliar al arrancar ───────────────────────────────────────────────

    async def reconcile_on_startup(self):
        """Lee posiciones reales de BingX. NO toca _open_count."""
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
            direction = "LONG" if amt > 0 else "SHORT"
            pos_side  = pos.get("positionSide", "BOTH")
            if pos_side not in ("LONG", "SHORT", "BOTH"):
                pos_side = "BOTH"
            entry = float(pos.get("avgPrice", pos.get("entryPrice", 0)) or 0)
            qty   = abs(amt)
            sl    = entry * (0.99 if direction == "LONG" else 1.01)
            tp1   = entry * (1.02 if direction == "LONG" else 0.98)
            tp2   = entry * (1.04 if direction == "LONG" else 0.96)
            async with self._lock:
                self._trades[sym] = OpenTrade(
                    symbol=sym, direction=direction, entry=entry,
                    sl=sl, tp1=tp1, tp2=tp2, qty=qty,
                    atr=entry * 0.005,    # estimación conservadora para reconcile
                    order_id="reconciled",
                    position_side=pos_side,
                    trail_sl=sl,          # SL inicial = SL de emergencia
                    peak_price=entry,     # peak inicial = entry
                )
            count += 1
            log.info("[%s] Reconciliado: %s qty=%.4f @ %.6f", sym, direction, qty, entry)

        if count:
            log.info("reconcile: %d posición(es) — colocando SL emergencia...", count)
            await self._place_emergency_sl_all()

    async def _place_emergency_sl_all(self):
        """
        Coloca SL inmediato en todas las posiciones reconciliadas.
        SL calculado desde mark price actual con 2% offset → siempre válido.
        Guarda el orderId para el sistema de trailing.
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

    # ── Monitor loop ──────────────────────────────────────────────────────────

    async def monitor_loop(self):
        log.info("Position monitor v7.1 — trailing stop | intervalo=%ds",
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

        # Mapa real de BingX (fuente de verdad)
        real_map: dict[str, dict] = {
            p["symbol"]: p for p in real_positions
            if p.get("symbol") and float(p.get("positionAmt", 0)) != 0
        }

        # ── FIX v7.2: open_count solo de ESTE bot, no de la cuenta entera ──────
        # get_open_positions() devuelve TODAS las posiciones de la cuenta BingX,
        # incluidas las de OTROS bots (renewed-love, joyful-art, GEMMI comparten
        # la misma cuenta/API). Antes: update_open_count(len(real_map)) contaba
        # posiciones de otros bots contra el MAX_OPEN_TRADES de este — podía
        # bloquear (o desbloquear) trades por actividad ajena.
        # Ahora: solo cuenta cuántos símbolos que ESTE bot trackea siguen
        # realmente abiertos en BingX (intersección tracked ∩ real).
        async with self._lock:
            own_symbols = set(self._trades.keys())
        own_open_count = len(own_symbols & set(real_map.keys()))
        await self.risk.update_open_count(own_open_count)

        async with self._lock:
            tracked = dict(self._trades)

        for symbol, trade in tracked.items():

            # ── Posición cerrada externamente (SL/TP disparado por BingX) ─────
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

            # ── Sync qty real (TP parciales ejecutados por BingX) ────────────
            real_qty = abs(float(pos.get("positionAmt", trade.qty) or trade.qty))
            if real_qty > 0:
                drift = abs(real_qty - trade.qty) / max(trade.qty, 1e-12)
                if drift > 0.05:   # >5% de diferencia = parcial ejecutado
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

            # ── Trailing Stop ─────────────────────────────────────────────────
            if not trade.trailing_active:
                # Umbral de activación = BREAKEVEN_ATR_MULT ATR favorable
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
            else:
                await self._update_trail(trade, mark)

    # ── Activación del trailing ───────────────────────────────────────────────

    async def _activate_trail(self, trade: OpenTrade, current_mark: float):
        """
        Activa el trailing stop por primera vez:
        1. Re-fetch precio fresco (fix race condition)
        2. Marca trailing_active=True ANTES de cualquier operación
           → ESTO es el fix definitivo del loop infinito 110412
        3. Valida el precio SL antes de cancelar nada
        4. Solo si precio válido: cancel_all → place BE SL
        5. Si falla: coloca SL de emergencia desde mark actual
        """
        symbol = trade.symbol
        log.info("[%s] Trail activation — mark=%.6f entry=%.6f atr=%.6f",
                 symbol, current_mark, trade.entry, trade.atr)

        # ── FIX DEFINITIVO: marcar activo AL INICIO, no al final ─────────────
        # Antes: be_moved se ponía True solo en éxito → retry infinito en fallo
        # Ahora: trailing_active=True impide cualquier reintento en ciclos futuros
        trade.trailing_active = True
        trade.be_moved        = True    # compat con código legacy
        trade.peak_price      = current_mark

        already_cancelled = False   # FIX v7.1: evitar doble cancel_all_orders

        try:
            # Re-fetch precio fresco para detectar reversiones rápidas
            ticker = await self.client.get_ticker(symbol)
            mark   = float(ticker.get("lastPrice", current_mark) or current_mark)
            if mark <= 0:
                mark = current_mark
            trade.peak_price = mark     # usar precio más fresco

            # Precio de breakeven
            sl_be = trade.entry
            side_close = "SELL" if trade.direction == "LONG" else "BUY"

            if _sl_valid(sl_be, mark, trade.direction):
                # ── Caso normal: precio sigue favorable ──────────────────────
                # Cancelar SL+TP originales (solo si el BE va a ser válido)
                try:
                    await self.client.cancel_all_orders(symbol)
                    already_cancelled = True    # FIX v7.1
                    await asyncio.sleep(0.3)
                except Exception as ce:
                    log.debug("[%s] cancel_all_orders: %s", symbol, ce)

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

                # FIX v7.3: 109420 en el BE path = posición ya cerrada
                if _is_position_closed_error(resp):
                    log.info("[%s] Trail BE: posición ya cerrada (109420) — "
                             "limpiando tracker", symbol)
                    pnl = self._calc_pnl(trade, mark)
                    await tg.notify_trade_closed(
                        symbol, trade.direction, trade.entry,
                        mark, trade.qty, "sl_tp_auto(trail_detect)", pnl,
                    )
                    await self.remove_trade(symbol, pnl)
                    return

                # ── SL en breakeven falló: fallback a offset de mark ──────────
                log.warning("[%s] BE @ entry falló: %s — probando SL offset", symbol, resp)

            else:
                # ── Precio revertió entre trigger y ahora: NO cancelar SL original
                log.warning("[%s] Precio revertió (mark=%.6f, entry=%.6f, dir=%s) "
                            "— SL original sigue activo, usando offset de mark",
                            symbol, mark, trade.entry, trade.direction)
                # No hacemos cancel_all — el SL original sigue protegiendo

            # ── Fallback universal: SL en mark offset ─────────────────────────
            # FIX v7.1: re-fetch mark FRESCO antes de calcular em_sl.
            # El mark inicial puede estar 1-2s obsoleto tras cancel_all + sleep + fallo BE.
            try:
                t2 = await self.client.get_ticker(symbol)
                m2 = float(t2.get("lastPrice", mark) or mark)
                if m2 > 0:
                    mark = m2
                    trade.peak_price = mark
            except Exception:
                pass    # usar mark anterior si el re-fetch falla

            # Para LONG: 1.5% bajo mark | Para SHORT: 1.5% sobre mark
            em_sl = mark * 0.985 if trade.direction == "LONG" else mark * 1.015

            if _sl_valid(em_sl, mark, trade.direction):
                # FIX v7.1: solo cancelar si NO cancelamos ya en el path del BE
                if not already_cancelled:
                    try:
                        await self.client.cancel_all_orders(symbol)
                        await asyncio.sleep(0.3)
                    except Exception:
                        pass

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
                    # FIX v7.3: BingX 109420 = posición ya cerrada externamente
                    # El SL/TP original se disparó antes de que llegáramos aquí.
                    # Auto-limpiar el trade del tracker — el monitor lo habría
                    # detectado en el siguiente ciclo de todas formas.
                    log.info("[%s] Trail activation: posición ya cerrada (109420) — "
                             "limpiando tracker", symbol)
                    pnl = self._calc_pnl(trade, mark)
                    await tg.notify_trade_closed(
                        symbol, trade.direction, trade.entry,
                        mark, trade.qty, "sl_tp_auto(trail_detect)", pnl,
                    )
                    await self.remove_trade(symbol, pnl)
                else:
                    log.error("[%s] Trail activation: SL emergencia FALLIDO: %s — "
                              "posición sin protección, monitorizar manual", symbol, em_resp)
                    await tg.notify_error(
                        f"trail_activation({symbol})",
                        f"SL emergencia fallido — POSICIÓN SIN PROTECCIÓN\n{em_resp}"
                    )
            else:
                log.error("[%s] Trail activation: no se puede calcular SL válido "
                          "para mark=%.6f dir=%s", symbol, mark, trade.direction)

        except Exception as e:
            log.error("[%s] _activate_trail error: %s", symbol, e)
            # trailing_active ya es True — no volverá a reintentar

    # ── Actualización del trailing ────────────────────────────────────────────

    async def _update_trail(self, trade: OpenTrade, mark: float):
        """
        Actualiza el trailing SL cuando el precio alcanza un nuevo peak.
        Estrategia PLACE-THEN-CANCEL:
          1. Calcula nuevo SL desde peak - TRAIL_DISTANCE_ATR * atr
          2. Si es mejor que el SL actual y válido para BingX:
             a. Re-fetch mark fresco (FIX v7.1) y re-valida
             b. Coloca NUEVO STOP_MARKET (posición protegida durante el update)
             c. Si OK: cancela el VIEJO (nunca queda sin SL)
             d. Actualiza trail_sl, trail_order_id
        El SL solo se mueve a favor del trade (LONG → arriba, SHORT → abajo).

        FIX v7.1 (anti-loop 110412):
          - Margen de _sl_valid ampliado a 0.5% (ver _sl_valid)
          - Antes de enviar la orden, se re-fetch el mark price y se
            revalida con el precio más reciente posible
          - Si new_sl es ~igual al último new_sl que falló y sigue
            siendo inválido, se descarta en debug sin loguear warning
            repetido (evita spam de logs en pares volátiles)
        """
        symbol     = trade.symbol
        trail_dist = trade.atr * C.TRAIL_DISTANCE_ATR

        # ── Calcular nuevo peak ───────────────────────────────────────────────
        if trade.direction == "LONG":
            if mark <= trade.peak_price:
                return   # sin nuevo máximo → sin cambio
            new_peak = mark
            new_sl   = new_peak - trail_dist
            # Solo subir el SL (nunca bajar)
            if new_sl <= trade.trail_sl:
                trade.peak_price = new_peak   # guardar peak aunque el SL no mejore
                return
        else:  # SHORT
            if trade.peak_price > 0 and mark >= trade.peak_price:
                return   # sin nuevo mínimo → sin cambio
            new_peak = mark
            new_sl   = new_peak + trail_dist
            # Solo bajar el SL (nunca subir para SHORT)
            if trade.trail_sl > 0 and new_sl >= trade.trail_sl:
                trade.peak_price = new_peak
                return

        # ── Validar precio SL para BingX (con mark actual) ───────────────────
        if not _sl_valid(new_sl, mark, trade.direction):
            trade.peak_price = new_peak
            # FIX v7.1: anti-spam — si es básicamente el mismo new_sl que ya
            # falló antes y sigue inválido, no repetir el warning/retry ruidoso
            if trade.last_failed_sl and abs(new_sl - trade.last_failed_sl) < trade.atr * 0.05:
                log.debug("[%s] Trail: new_sl=%.6f repetido e inválido (mark=%.6f) — esperando nuevo peak",
                          symbol, new_sl, mark)
            else:
                log.debug("[%s] Trail: new_sl=%.6f inválido para mark=%.6f dir=%s",
                          symbol, new_sl, mark, trade.direction)
            trade.last_failed_sl = new_sl
            return

        # ── FIX v7.1: re-fetch mark fresco justo antes de enviar ─────────────
        # El mark usado para calcular new_sl puede tener 1 ciclo de antigüedad
        # (hasta POSITION_CHECK_INTERVAL segundos). Revalidar con precio fresco
        # evita el 110412 cuando el precio se movió en contra justo antes del envío.
        fresh_mark = mark
        try:
            t = await self.client.get_ticker(symbol)
            fm = float(t.get("lastPrice", mark) or mark)
            if fm > 0:
                fresh_mark = fm
        except Exception:
            pass    # si el refresh falla, seguimos con el mark del ciclo

        if not _sl_valid(new_sl, fresh_mark, trade.direction):
            trade.peak_price     = new_peak
            trade.last_failed_sl = new_sl
            log.debug("[%s] Trail: new_sl=%.6f inválido tras refresh (mark fresco=%.6f, dir=%s) — "
                      "se reintentará con próximo peak",
                      symbol, new_sl, fresh_mark, trade.direction)
            return

        # ── PLACE-THEN-CANCEL ─────────────────────────────────────────────────
        try:
            side_close = "SELL" if trade.direction == "LONG" else "BUY"

            # 1. Colocar NUEVO SL primero (nunca sin protección)
            resp = await self.client.place_stop_market_order(
                symbol, side_close, trade.qty, new_sl,
                trade.direction, order_type="STOP_MARKET",
            )

            if resp.get("code", -1) == 0:
                new_oid       = _extract_order_id(resp)
                old_oid       = trade.trail_order_id
                old_sl        = trade.trail_sl
                profit_locked = self._calc_pnl(trade, new_sl)

                # 2. Actualizar estado
                trade.peak_price     = new_peak
                trade.trail_sl       = new_sl
                trade.trail_order_id = new_oid
                trade.sl             = new_sl   # mantener .sl en sync
                trade.last_failed_sl = 0.0      # FIX v7.1: reset tras éxito

                log.info("[%s] 📈 Trail: %.6f→%.6f | peak=%.6f | mark=%.6f | PnL@SL≈%.2f USDT",
                         symbol, old_sl, new_sl, new_peak, fresh_mark, profit_locked)

                # 3. Cancelar SL viejo (best-effort, el nuevo ya está activo)
                if old_oid and old_oid != new_oid:
                    await asyncio.sleep(0.1)
                    try:
                        await self.client.cancel_order(symbol, old_oid)
                        log.debug("[%s] Old trail SL %s cancelado", symbol, old_oid)
                    except Exception as ce:
                        log.debug("[%s] cancel_order viejo %s: %s", symbol, old_oid, ce)

                # 4. Notificación Telegram (throttle: 1 ATR de mejora mínima)
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
                # FIX v7.3: detectar 109420 = posición ya cerrada
                if _is_position_closed_error(resp):
                    log.info("[%s] Trail update: posición ya cerrada (109420) — "
                             "limpiando tracker", symbol)
                    pnl = self._calc_pnl(trade, fresh_mark)
                    await tg.notify_trade_closed(
                        symbol, trade.direction, trade.entry,
                        fresh_mark, trade.qty, "sl_tp_auto(trail_detect)", pnl,
                    )
                    await self.remove_trade(symbol, pnl)
                    return
                # Fallo al actualizar trail — no es crítico, el SL viejo sigue activo
                trade.peak_price     = new_peak   # guardar peak, reintentar próximo ciclo
                trade.last_failed_sl = new_sl     # FIX v7.1: recordar para anti-spam
                log.warning("[%s] Trail update falló new_sl=%.6f: %s",
                            symbol, new_sl, resp)

        except Exception as e:
            trade.peak_price     = new_peak
            trade.last_failed_sl = new_sl
            log.error("[%s] _update_trail error: %s", symbol, e)

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
        """
        FIX v7.1: suma el PnL no realizado de TODAS las posiciones trackeadas,
        usando el mark price actual de BingX. Se usa para alimentar
        risk.can_trade(unrealized_pnl=...) y así el límite de pérdida diaria
        contempla el drawdown real de la cuenta, no solo lo ya cerrado.
        Si falla la consulta a BingX, retorna 0.0 (fail-safe: no bloquea
        operaciones por un error de red, pero tampoco las habilita de más).
        """
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
