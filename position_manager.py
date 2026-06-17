"""
QF×JP Bot v6.6 — Position Manager CORREGIDO + MEJORAS
Fixes v6.5:
  - BE usa real_map del ciclo (no llamada extra → elimina 'position not exist')
  - open_count sincronizado solo desde BingX real
  - reconcile NO toca _open_count
  - remove_trade pasa symbol para cooldown

Mejoras v6.6 [Item de mayor impacto tras análisis de 33 páginas de historial]:
  - OpenTrade.opened_at: timestamp de apertura (time.time()). Para trades
    reconciliados al arrancar se usa el momento del reconcile como
    aproximación — no sabemos cuándo se abrió realmente, pero es preferible
    cerrar "pronto" un reconciliado por error de estimación que dejarlo
    abierto indefinidamente.
  - _check_all_positions ahora chequea MAX_HOLD_MINUTES en cada ciclo: si
    un trade lleva abierto más tiempo del configurado sin resolver por
    SL/TP/BE, se cierra por mercado vía close_position_emergency con
    reason="max_hold_time". Este es el fix de mayor impacto identificado:
    3 de 3 liquidaciones en el historial analizado ocurrieron en trades
    expuestos 1h17m-29h a leverage 6-10x, no por tamaño de posición.
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

import config as C
from bingx_client import BingXClient
from risk_manager import RiskManager
import telegram_client as tg

log = logging.getLogger("position_mgr")


@dataclass
class OpenTrade:
    symbol:     str
    direction:  str
    entry:      float
    sl:         float
    tp1:        float
    tp2:        float
    qty:        float
    atr:        float
    order_id:   str
    be_moved:   bool  = False
    tp1_hit:    bool  = False
    opened_at:  float = field(default_factory=time.time)   # v6.6: para MAX_HOLD_MINUTES
    tier:       str   = ""    # v6.6: para log_closed_trade / stats
    score:      float = 0.0   # v6.6: para log_closed_trade / stats


class PositionManager:
    def __init__(self, client: BingXClient, risk: RiskManager, on_close=None):
        """
        v6.6: on_close es un callback opcional
        (symbol, tier, score, direction, pnl, reason, hold_minutes) -> None
        invocado en cada cierre real (no en BE-skip de posición ya
        inexistente). Lo conecta main.py a scanner.log_closed_trade para
        alimentar /stats, sin crear un import circular position_manager
        <-> scanner.
        """
        self.client    = client
        self.risk      = risk
        self.on_close  = on_close
        self._trades: dict[str, OpenTrade] = {}
        self._lock  = asyncio.Lock()

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
            entry = float(pos.get("avgPrice", pos.get("entryPrice", 0)) or 0)
            qty   = abs(amt)
            sl    = entry * (0.99 if direction == "LONG" else 1.01)
            tp1   = entry * (1.02 if direction == "LONG" else 0.98)
            tp2   = entry * (1.04 if direction == "LONG" else 0.96)
            async with self._lock:
                self._trades[sym] = OpenTrade(
                    symbol=sym, direction=direction, entry=entry,
                    sl=sl, tp1=tp1, tp2=tp2, qty=qty,
                    atr=entry * 0.005, order_id="reconciled",
                )
            count += 1
            log.info("[%s] Reconciliado: %s qty=%.4f @ %.6f", sym, direction, qty, entry)

        if count:
            log.info("reconcile: %d posición(es) — open_count se sincronizará en primer ciclo", count)

    # ── Registro ──────────────────────────────────────────────────────────────

    async def register_trade(self, trade: OpenTrade):
        async with self._lock:
            self._trades[trade.symbol] = trade
        await self.risk.on_trade_opened(symbol=trade.symbol)
        log.info("[%s] Trade registrado %s @ %.6f", trade.symbol, trade.direction, trade.entry)

    async def remove_trade(self, symbol: str, pnl: float = 0.0, reason: str = "unknown"):
        """
        v6.6: ahora captura el OpenTrade completo antes de borrarlo y,
        si existe, invoca self.on_close(symbol, tier, score, direction,
        pnl, reason, hold_minutes) para alimentar /stats vía
        scanner.log_closed_trade. reason es informativo (sl_tp_auto,
        max_hold_time, manual_close, etc.) — no afecta la lógica de
        riesgo, solo el registro.
        """
        existed = False
        trade: Optional[OpenTrade] = None
        async with self._lock:
            if symbol in self._trades:
                trade = self._trades.pop(symbol)
                existed = True
        if existed:
            await self.risk.on_trade_closed(pnl=pnl, symbol=symbol)
            if self.on_close is not None and trade is not None:
                hold_minutes = (time.time() - trade.opened_at) / 60.0
                try:
                    self.on_close(symbol, trade.tier, trade.score,
                                  trade.direction, pnl, reason, hold_minutes)
                except Exception as e:
                    log.warning("on_close callback error: %s", e)

    # ── Monitor loop ──────────────────────────────────────────────────────────

    async def monitor_loop(self):
        log.info("Position monitor iniciado (intervalo=%ds)", C.POSITION_CHECK_INTERVAL)
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

        # Mapa real de BingX
        real_map: dict[str, dict] = {
            p["symbol"]: p for p in real_positions
            if p.get("symbol") and float(p.get("positionAmt", 0)) != 0
        }

        # ── FIX: sincronizar open_count con BingX real ────────────────────────
        await self.risk.update_open_count(len(real_map))

        async with self._lock:
            tracked = dict(self._trades)

        for symbol, trade in tracked.items():

            # Posición cerrada externamente
            if symbol not in real_map:
                try:
                    ticker      = await self.client.get_ticker(symbol)
                    close_price = float(ticker.get("lastPrice", trade.entry))
                except Exception:
                    close_price = trade.entry
                pnl = self._calc_pnl(trade, close_price)
                log.info("[%s] Cerrada externamente. PnL≈%.2f", symbol, pnl)
                await tg.notify_trade_closed(
                    symbol, trade.direction, trade.entry,
                    close_price, trade.qty, "sl_tp_auto", pnl,
                )
                await self.remove_trade(symbol, pnl, reason="sl_tp_auto")
                continue

            # Posición abierta
            pos = real_map[symbol]
            try:
                mark = float(pos.get("markPrice", 0) or 0)
                if mark <= 0:
                    ticker = await self.client.get_ticker(symbol)
                    mark   = float(ticker.get("lastPrice", trade.entry))
            except Exception:
                continue
            if mark <= 0:
                continue

            # ── v6.6: cierre forzado por tiempo máximo de exposición ──────────
            # Fix de mayor impacto (ver docstring del módulo): trades que no
            # se resuelven en el tiempo esperado para esta estrategia (5-15
            # min normalmente) y siguen abiertos horas son el origen de las
            # liquidaciones observadas, no el tamaño de la posición.
            if C.MAX_HOLD_MINUTES > 0:
                held_min = (time.time() - trade.opened_at) / 60.0
                if held_min >= C.MAX_HOLD_MINUTES:
                    log.warning(
                        "[%s] MAX_HOLD_MINUTES superado (%.1f/%d min) — cierre forzado",
                        symbol, held_min, C.MAX_HOLD_MINUTES,
                    )
                    await self.close_position_emergency(symbol, reason="max_hold_time")
                    continue

            # TP1 tracking
            if not trade.tp1_hit:
                tp1_hit = (
                    (trade.direction == "LONG"  and mark >= trade.tp1) or
                    (trade.direction == "SHORT" and mark <= trade.tp1)
                )
                if tp1_hit:
                    trade.tp1_hit = True
                    log.info("[%s] TP1 alcanzado @ %.6f", symbol, mark)

            # Breakeven
            if not trade.be_moved:
                be_trigger = (
                    trade.entry + trade.atr * C.BREAKEVEN_ATR_MULT
                    if trade.direction == "LONG"
                    else trade.entry - trade.atr * C.BREAKEVEN_ATR_MULT
                )
                be_reached = (
                    (trade.direction == "LONG"  and mark >= be_trigger) or
                    (trade.direction == "SHORT" and mark <= be_trigger)
                )
                if be_reached:
                    # ── FIX: pasar real_map para no hacer llamada extra ────────
                    await self._move_to_breakeven(trade, mark, real_map)

    async def _move_to_breakeven(self, trade: OpenTrade, current_price: float,
                                  real_map: dict = None):
        """
        FIX DEFINITIVO 'position not exist':
        Usa real_map del ciclo actual — sin llamada extra a BingX.
        Si BE falla, re-pone el SL original para no dejar posición sin protección.
        """
        try:
            # Verificar con real_map ya disponible
            if real_map is not None and trade.symbol not in real_map:
                log.info("[%s] BE skip — no en real_map", trade.symbol)
                await self.remove_trade(trade.symbol, 0.0, reason="be_skip_already_closed")
                return

            # FIX 109420: cancel_all_orders falla si no hay órdenes abiertas.
            # Ignorar el error y continuar siempre al STOP_MARKET.
            try:
                await self.client.cancel_all_orders(trade.symbol)
            except Exception as ce:
                log.debug("[%s] cancel_all_orders ignorado: %s", trade.symbol, ce)
            await asyncio.sleep(0.3)

            side_close = "SELL" if trade.direction == "LONG" else "BUY"
            resp = await self.client.place_stop_market_order(
                trade.symbol, side_close, trade.qty, trade.entry,
                trade.direction, order_type="STOP_MARKET",
            )
            if resp.get("code", -1) == 0:
                trade.be_moved = True
                log.info("[%s] SL → breakeven @ %.6f", trade.symbol, trade.entry)
            else:
                log.warning("[%s] BE fallo: %s — colocando SL", trade.symbol, resp)
                await asyncio.sleep(0.2)
                # Fallback: SL original, o -3%/+3% si la posición no tenía SL
                sl_price = trade.sl if trade.sl > 0 else (
                    trade.entry * 0.97 if trade.direction == "LONG" else trade.entry * 1.03
                )
                sl_resp = await self.client.place_stop_market_order(
                    trade.symbol, side_close, trade.qty, sl_price,
                    trade.direction, order_type="STOP_MARKET",
                )
                if sl_resp.get("code", -1) == 0:
                    log.info("[%s] SL colocado @ %.6f", trade.symbol, sl_price)
                else:
                    log.error("[%s] SL NO colocado: %s — POSICIÓN SIN PROTECCIÓN",
                              trade.symbol, sl_resp)
        except Exception as e:
            log.error("[%s] _move_to_breakeven error: %s", trade.symbol, e)

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
            await self.remove_trade(symbol, pnl, reason=reason)
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
