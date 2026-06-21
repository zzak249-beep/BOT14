"""
QF×JP Bot v7.6 — Position Manager TRAILING STOP DINÁMICO (FIX dirección real)
═══════════════════════════════════════════════════════════════════════════════
FIX v7.6 — CRÍTICO: auto-corrección de trade.direction contra BingX real,
  en cada ciclo del monitor (ver _check_all_positions). reconcile_on_startup()
  ya priorizaba positionSide sobre el signo de positionAmt al reconciliar,
  pero nada volvía a verificar esto después — si trade.direction se quedó
  mal por cualquier vía, se quedaba mal para siempre, invirtiendo
  side_close en _activate_trail()/_update_trail(), _calc_pnl(), el EMA
  exit y el time stop. Caso confirmado: BTWUSDT en renewed-love, 130+
  reintentos de SL fallidos durante horas (110424 "order size must be
  less than the available amount" — el patrón exacto de una orden de
  cierre construida como si fuera apertura). Complementa el fix de
  bingx_client.py v7.10 (que protege el punto de envío de la orden) —
  este corrige la fuente, para que todo lo que depende de trade.direction
  esté bien, no solo la orden de protección.

FIX v7.5 — time_stop/EMA exit desactivados de facto por redeploys frecuentes:
  reconcile_on_startup() corre en CADA redeploy y no sabe cuánto lleva
  realmente abierta una posición ya existente (BingX no expone el
  timestamp de apertura original en /v2/user/positions). Antes, eso
  dejaba opened_at=0.0 hasta el primer ciclo de monitor, que lo fijaba a
  "ahora" — dando una ventana de MAX_HOLD_MINUTES COMPLETA Y FRESCA en
  cada redeploy. Con redeploys más frecuentes que MAX_HOLD_MINUTES
  (sesión de desarrollo activa, muchos redeploys por hora), cualquier
  posición que sobreviviera entre dos redeploys consecutivos nunca
  llegaba a cumplir el tiempo para que time_stop o EMA exit dispararan.
  Caso real: ETH-USDT abierto 07:31, sobrevivió un redeploy a las 9:34,
  cerró recién a las 15:24 — casi 8h sin que nada disparara.
  Fix: en reconcile, opened_at se fija conservadoramente a "ya se gastó
  la mitad del presupuesto de MAX_HOLD_MINUTES" en vez de "recién abierto".

FIX v7.4 — EMA EXIT independiente (más rápido que time_stop):
  Investigado: para el rango de timeframe de este bot (TIMEFRAME=3m,
  holds típicos bajo MAX_HOLD_MINUTES), el estándar de facto en scalping
  cripto es usar una EMA corta (5-13 períodos, converge en EMA9 como el
  más citado) como línea de salida dinámica — mientras el precio cierra
  velas del lado correcto de la EMA, la micro-tendencia sigue intacta;
  un CIERRE de vela (no una mecha) del lado contrario señala que la
  tendencia murió.

  Antes, la única forma de salir de un trade que no progresa era
  _check_time_stop() esperando hasta MAX_HOLD_MINUTES completos (60min
  por defecto) — mucho más lento que detectar la muerte de la tendencia
  por EMA. _check_ema_exit() es un chequeo NUEVO E INDEPENDIENTE:
    - Mismo alcance que time_stop: solo aplica si el trailing NO se ha
      activado todavía (si ya va ganando, el trailing se encarga — no
      cerramos un ganador por una sola vela en contra).
    - Guarda mínima de EMA_EXIT_MIN_HOLD_MIN (6min por defecto, ~2 velas
      de 3m) antes de evaluar — evita whipsaw inmediato justo tras entrar
      con el precio rondando la EMA.
    - Usa klines[-2] (última vela CERRADA), nunca la vela en curso —
      algunas APIs devuelven la vela actual sin cerrar como último
      elemento, evaluarla daría señales prematuras.
    - Desactivado por defecto (EMA_EXIT_ENABLED=False) — activar solo
      tras confirmar en logs que el comportamiento es el esperado.

FIX v7.3 (sin cambios):
  ✅ open_count YA NO cuenta toda la cuenta BingX — solo las posiciones que
     ESTE bot trackea. Antes, con renewed-love + joyful-art + GEMMI
     compartiendo la misma cuenta/API, cada bot veía las posiciones de
     los OTROS bots reflejadas en su propio MAX_OPEN_TRADES, causando
     bloqueos (o desbloqueos) por actividad ajena al bot.

FIX v7.3 — posiciones desnudas permanentes (sin cambios):
  ✅ Si trailing_active=True pero trail_order_id está vacío (ambos
     intentos de SL fallaron en _activate_trail), el monitor reintenta
     _activate_trail() cada ciclo en vez de quedarse esperando un nuevo
     peak favorable que nunca iba a llegar en una posición perdedora.
     Throttle de logs/Telegram cada 10 reintentos. Caso confirmado:
     INJ-USDT, ZEC-USDT, LAB-USDT en joyful-art.

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


def _ema(values: list[float], period: int) -> list[float]:
    """
    EMA simple sin dependencias externas (igual de mínima que los demás
    helpers de este archivo). Usada solo por _check_ema_exit() — para el
    cálculo de STC/indicadores del scanner, ver stc_asymmetry.py (módulo
    separado a propósito: position_manager.py no debe depender de un
    filtro experimental del lado del scanner).
    """
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

    # ── FIX v7.3: reintentos de activación cuando ambos intentos de SL fallan ──
    activation_attempts: int = 0     # veces que se reintentó _activate_trail()
                                      # sin lograr colocar una orden real

    # ── Time Stop / EMA Exit (previene FHEU/SXT/LDO: horas open sin progresar) ─
    opened_at:        float = 0.0    # timestamp apertura (0 = usar tiempo actual)


# ── Manager ───────────────────────────────────────────────────────────────────

class PositionManager:
    def __init__(self, client: BingXClient, risk: RiskManager, journal=None):
        self.client   = client
        self.risk     = risk
        self._journal = journal   # TradeJournal opcional — notifica W/L al cerrar
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
            direction_from_amt = "LONG" if amt > 0 else "SHORT"
            pos_side  = pos.get("positionSide", "BOTH")
            if pos_side not in ("LONG", "SHORT", "BOTH"):
                pos_side = "BOTH"

            # ── FIX CRÍTICO ──────────────────────────────────────────────────
            # En modo Hedge, BingX puede reportar positionAmt SIEMPRE positivo
            # sin importar si la posición es LONG o SHORT — la dirección real
            # vive en positionSide, no en el signo de positionAmt. Usar solo
            # `amt > 0` causó que posiciones SHORT reconciliadas (tras CADA
            # redeploy) se trackearan internamente como LONG, invirtiendo la
            # lógica de profit/pérdida del trailing stop y dejando posiciones
            # sin protección real. Caso confirmado: HYPE-USDT SHORT trackeado
            # como LONG → trail activation se disparó con el precio SUBIENDO
            # (pérdida real para un SHORT) creyendo que era ganancia →
            # SL de breakeven calculado al revés → BingX rechazó con 110412
            # en bucle → posición corrió sin protección real → -6.84 USDT.
            #
            # positionSide es la fuente de verdad cuando es LONG/SHORT
            # explícito (Hedge mode). Solo se usa el signo de amt como
            # fallback cuando viene "BOTH" (One-Way mode, donde sí es fiable).
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
                    atr=entry * 0.005,    # estimación conservadora para reconcile
                    order_id="reconciled",
                    position_side=pos_side,
                    trail_sl=sl,          # SL inicial = SL de emergencia
                    peak_price=entry,     # peak inicial = entry
                    # ── FIX v7.5 ──────────────────────────────────────────────
                    # NO asumir que la posición "acaba de abrir". Este endpoint
                    # de BingX no expone el timestamp de apertura original, así
                    # que antes opened_at se quedaba en 0.0 y _check_time_stop()
                    # lo fijaba a "ahora" en el primer ciclo — dando una ventana
                    # de MAX_HOLD_MINUTES COMPLETA y fresca en CADA redeploy.
                    # Con redeploys más frecuentes que MAX_HOLD_MINUTES (sesión
                    # de desarrollo activa), esto dejaba el time_stop y el EMA
                    # exit efectivamente desactivados para siempre en cualquier
                    # posición que sobreviviera entre dos redeploys. Caso real:
                    # ETH-USDT abierto 07:31, sobrevivió un redeploy a las 9:34,
                    # cerró recién a las 15:24 — casi 8h sin que nada disparara.
                    # Fix: asumir conservadoramente que ya se gastó la MITAD del
                    # presupuesto de tiempo — ni pánico inmediato (cierre falso
                    # de un trade que progresaba bien) ni ventana infinita.
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
        SL calculado desde mark price actual con 2% offset → siempre válido.
        Guarda el orderId para el sistema de trailing.

        FIX CRÍTICO: esta función se ejecuta en CADA reconcile_on_startup(),
        es decir, en CADA redeploy del bot. Antes NO cancelaba las órdenes
        previas antes de colocar la nueva SL de emergencia — cada redeploy
        apilaba una orden más sin limpiar las anteriores. Con las decenas
        de redeploys de una sesión de desarrollo activa, esto acumuló 75
        órdenes huérfanas para solo 5 posiciones reales (~15 por símbolo).
        Ahora cancela TODO lo pendiente del símbolo antes de colocar la
        SL nueva — igual que ya hacía correctamente _activate_trail().
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

                # FIX: limpiar órdenes huérfanas de redeploys anteriores
                # ANTES de colocar la nueva — evita la acumulación de
                # decenas de SL/TP fantasma a lo largo de muchos restarts.
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
        # Marcar timestamp de apertura para el time-stop / EMA exit
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
            # Notificar al journal para win rate adaptativo
            if self._journal is not None:
                await self._journal.on_close(symbol, pnl)

    # ── Monitor loop ──────────────────────────────────────────────────────────

    async def monitor_loop(self):
        log.info("Position monitor v7.6 — trailing stop + EMA exit + auto-corrección de dirección | intervalo=%ds",
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

            # ── FIX v7.6 CRÍTICO: auto-corregir trade.direction si no coincide
            # con la verdad de BingX ───────────────────────────────────────────
            # reconcile_on_startup() ya prioriza positionSide sobre el signo de
            # positionAmt en hedge mode al RECONCILIAR — pero nada volvía a
            # verificar esto después, en cada ciclo del monitor. Si
            # trade.direction se quedó mal por cualquier vía (registro inicial,
            # una reconciliación anterior a este fix, lo que sea), se quedaba
            # mal PARA SIEMPRE — cada side_close calculado en _activate_trail()/
            # _update_trail() salía invertido sin que nada lo detectara.
            # Caso confirmado: BTWUSDT en renewed-love, 130+ reintentos de SL
            # fallidos durante horas, error 110424 "order size must be less
            # than the available amount" — el patrón exacto de una orden de
            # cierre construida con la dirección al revés (BingX la trata como
            # abrir posición nueva, necesita margen fresco, en vez de cerrar la
            # existente). bingx_client.py v7.10 ya protege el punto de envío de
            # la orden con la misma verificación — esto corrige la FUENTE: si
            # trade.direction está mal, _calc_pnl(), el EMA exit y el time stop
            # también calculaban mal, no solo la orden de protección.
            real_amt = float(pos.get("positionAmt", 0) or 0)
            real_ps  = pos.get("positionSide", "")
            real_direction = (
                real_ps if real_ps in ("LONG", "SHORT")
                else ("LONG" if real_amt > 0 else "SHORT")
            )
            if real_direction != trade.direction:
                log.warning(
                    "[%s] ⚠️ DIRECCIÓN CORREGIDA: tracker tenía %s, BingX confirma "
                    "%s (positionSide=%s, positionAmt=%.6f). Actualizando — esta "
                    "es la causa raíz del bug que dejó BTWUSDT sin protección "
                    "durante horas.", symbol, trade.direction, real_direction,
                    real_ps, real_amt,
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

            # ── TIME STOP ─────────────────────────────────────────────────────
            # Previene el patrón FHEU/SXT/LDO: posiciones LONG abiertas 9-11h
            # que bajaban lentamente sin llegar al SL (demasiado ancho a 2.0 ATR).
            # Si MAX_HOLD_MINUTES sin progreso mínimo Y trailing no activo → cierre.
            if await self._check_time_stop(trade, mark, symbol):
                continue

            # ── EMA EXIT (FIX v7.4) ───────────────────────────────────────────
            # Independiente del time_stop — detecta muerte de tendencia mucho
            # más rápido (cierre de vela cruzando la EMA corta) en vez de
            # esperar hasta MAX_HOLD_MINUTES completos. Ver _check_ema_exit().
            if await self._check_ema_exit(trade, symbol):
                continue

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
            elif not trade.trail_order_id:
                # ── FIX v7.3: posición SIN protección real en BingX ────────────
                # trailing_active=True se marcó al inicio de _activate_trail()
                # (fix necesario del loop 110412 de v7.0), pero si AMBOS
                # intentos de SL fallaron ahí dentro (breakeven y emergencia),
                # trail_order_id se quedó vacío. _update_trail() de abajo solo
                # actúa si hay un NUEVO peak favorable — si el precio se queda
                # en pérdida tras el fallo, nunca se reintentaba nada. Caso
                # confirmado: INJ-USDT, ZEC-USDT, LAB-USDT en joyful-art.
                # Fix: reintentar _activate_trail() cada ciclo hasta lograr
                # una orden real. Las validaciones de v7.1 (margen 0.5% +
                # refetch de mark fresco) hacen que el reintento normalmente
                # tenga éxito rápido.
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
        Activa el trailing stop por primera vez (o reintenta si v7.3 detectó
        que quedó sin orden real tras un fallo anterior):
        1. Re-fetch precio fresco (fix race condition)
        2. Marca trailing_active=True ANTES de cualquier operación
           → ESTO es el fix definitivo del loop infinito 110412
        3. Valida el precio SL antes de cancelar nada
        4. Solo si precio válido: cancel_all → place BE SL
        5. Si falla: coloca SL de emergencia desde mark actual
        6. Si AMBOS fallan: el caller (_check_all_positions) reintentará en
           el próximo ciclo gracias al fix v7.3 — no se queda atascado.
        """
        symbol = trade.symbol
        log.info("[%s] Trail activation — mark=%.6f entry=%.6f atr=%.6f",
                 symbol, current_mark, trade.entry, trade.atr)

        # ── FIX DEFINITIVO: marcar activo AL INICIO, no al final ─────────────
        # Antes: be_moved se ponía True solo en éxito → retry infinito en fallo
        # Ahora: trailing_active=True impide cualquier reintento INMEDIATO en
        # este mismo ciclo, pero v7.3 sí reintenta en ciclos siguientes si
        # trail_order_id sigue vacío (ver _check_all_positions).
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
                              "posición sin protección, reintentará en próximo "
                              "ciclo (FIX v7.3)", symbol, em_resp)
                    # FIX v7.3: throttle — antes esto se mandaba en CADA fallo.
                    # Con el reintento automático por ciclo (ver
                    # _check_all_positions), un fallo persistente saturaría
                    # Telegram. Ahora solo el primer intento y luego cada 10.
                    if trade.activation_attempts <= 1 or trade.activation_attempts % 10 == 0:
                        await tg.notify_error(
                            f"trail_activation({symbol})",
                            f"SL emergencia fallido (intento #{trade.activation_attempts}) "
                            f"— POSICIÓN SIN PROTECCIÓN, reintentando cada ciclo\n{em_resp}"
                        )
            else:
                log.error("[%s] Trail activation: no se puede calcular SL válido "
                          "para mark=%.6f dir=%s — reintentará en próximo ciclo",
                          symbol, mark, trade.direction)

        except Exception as e:
            log.error("[%s] _activate_trail error: %s — reintentará en próximo ciclo "
                      "(FIX v7.3, trail_order_id sigue vacío)", symbol, e)
            # trailing_active ya es True, pero como trail_order_id sigue vacío
            # (no se llegó a colocar ninguna orden), v7.3 reintentará solo.

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

    # ── Time Stop ─────────────────────────────────────────────────────────────

    async def _check_time_stop(self, trade: OpenTrade, mark: float, symbol: str) -> bool:
        """
        TIME STOP — cierra trades sin progreso tras MAX_HOLD_MINUTES.

        Caso real prevenido:
          SXT-USDT  Long 10X: 09:29 → 20:45 (11h16m) → -8.87 USDT
          LDO-USDT  Long 10X: 11:48 → 20:45 ( 8h57m) → -9.50 USDT
          FHE-USDT  Long 10X: 11:34 → 20:45 ( 9h11m) → -5.10 USDT
          XNY-USDT  Long 10X: 17:51 → 21:30 ( 3h39m) → -4.49 USDT

        Regla:
        - Si trailing_active=True (ya va ganando) → NUNCA cierra por tiempo
        - Si no ha pasado MAX_HOLD_MINUTES → no evalúa aún
        - Si ha pasado y el precio no avanzó TIME_STOP_MIN_PROGRESS_ATR*ATR → cierra

        Retorna True si cerró (el caller debe hacer `continue`).

        Ver también _check_ema_exit() (FIX v7.4) — chequeo independiente,
        normalmente mucho más rápido que este, que detecta muerte de
        tendencia por cierre de vela cruzando una EMA corta.
        """
        if trade.trailing_active:
            return False  # el trailing se encarga

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
            return False  # va avanzando, aunque lento

        log.warning(
            "[%s] ⏱ TIME STOP — %.0fmin sin progreso (prog=%.6f < min=%.6f). Cerrando.",
            symbol, elapsed_min, progress, min_prog,
        )
        await tg.notify_time_stop(symbol, trade.direction, trade.entry, mark,
                                   int(elapsed_min), progress)
        await self.close_position_emergency(symbol, reason="time_stop")
        return True

    # ── EMA Exit (FIX v7.4) ─────────────────────────────────────────────────────

    async def _check_ema_exit(self, trade: OpenTrade, symbol: str) -> bool:
        """
        Salida por EMA corta — independiente de _check_time_stop() y del
        trailing. Investigado: para el rango de timeframe de este bot
        (TIMEFRAME=3m), el estándar de facto en scalping cripto es usar
        una EMA corta (5-13 períodos, EMA9 el más citado) como línea de
        salida dinámica. Mientras el precio cierra velas del lado correcto
        de la EMA, la micro-tendencia sigue intacta; un CIERRE de vela
        (no una mecha) del lado contrario señala que la tendencia murió —
        mucho más rápido que esperar los MAX_HOLD_MINUTES completos del
        time_stop.

        Mismo alcance que _check_time_stop(): solo aplica si el trailing
        NO se ha activado todavía. Si ya va ganando y el trailing está
        activo, el trailing se encarga de dejar correr el beneficio — no
        queremos cerrar un ganador solo porque una vela cerró del lado
        contrario.

        Guarda EMA_EXIT_MIN_HOLD_MIN (6min/~2 velas de 3m por defecto)
        antes de evaluar, para no salir en el primer whipsaw justo tras
        la entrada con el precio rondando la EMA.

        Usa klines[-2] (última vela CERRADA) — klines[-1] puede ser la
        vela en curso todavía sin cerrar dependiendo de la API, evaluarla
        daría señales prematuras basadas en datos incompletos.

        Desactivado por defecto (EMA_EXIT_ENABLED=False). Retorna True si
        cerró (el caller debe hacer `continue`).
        """
        if not getattr(C, 'EMA_EXIT_ENABLED', False):
            return False
        if trade.trailing_active:
            return False  # el trailing se encarga, igual que time_stop

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

        # Última vela CERRADA — ver docstring sobre por qué -2, no -1
        last_closed_close = closes[-2]
        last_closed_ema    = ema[-2]

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
