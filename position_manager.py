"""
QF×JP Bot v6.5.1 — Position Manager
Fixes:
  - BE 109420 'position not exist': elimina posición del tracker cuando BingX
    confirma que no existe (código 109420 o symbol no en real_map)
  - BE loop infinito: be_moved=True aunque falle, para no reintentar
  - Retry silencioso: si position ya no existe, solo limpiar sin spam
"""
import asyncio
import logging
from dataclasses import dataclass

import config as C
from bingx_client import BingXClient
from risk_manager import RiskManager
import telegram_client as tg

log = logging.getLogger("position_mgr")


@dataclass
class OpenTrade:
    symbol:    str
    direction: str
    entry:     float
    sl:        float
    tp1:       float
    tp2:       float
    qty:       float
    atr:       float
    order_id:  str
    be_moved:  bool = False
    tp1_hit:   bool = False


class PositionManager:
    def __init__(self, client: BingXClient, risk: RiskManager):
        self.client = client
        self.risk   = risk
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

    async def remove_trade(self, symbol: str, pnl: float = 0.0):
        existed = False
        async with self._lock:
            if symbol in self._trades:
                del self._trades[symbol]
                existed = True
        if existed:
            await self.risk.on_trade_closed(pnl=pnl, symbol=symbol)

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

        # Sincronizar open_count con BingX real
        await self.risk.update_open_count(len(real_map))

        async with self._lock:
            tracked = dict(self._trades)

        for symbol, trade in tracked.items():

            # Posición cerrada externamente (SL/TP ejecutado por BingX)
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
                await self.remove_trade(symbol, pnl)
                continue

            # Posición abierta — obtener precio mark
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

            # TP1 tracking
            if not trade.tp1_hit:
                tp1_hit = (
                    (trade.direction == "LONG"  and mark >= trade.tp1) or
                    (trade.direction == "SHORT" and mark <= trade.tp1)
                )
                if tp1_hit:
                    trade.tp1_hit = True
                    log.info("[%s] TP1 alcanzado @ %.6f", symbol, mark)

            # Breakeven — solo si aún no se ha movido
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
                    await self._move_to_breakeven(trade, mark, real_map)

    async def _move_to_breakeven(self, trade: OpenTrade, current_price: float,
                                  real_map: dict = None):
        """
        FIX v6.5.1:
        - Marca be_moved=True ANTES del intento para evitar loop infinito
        - Si BingX responde 109420 (position not exist) → limpiar trade
        - No envía positionSide en STOP_MARKET de cierre (One-Way mode)
        """
        symbol = trade.symbol

        # Verificar con real_map ya disponible
        if real_map is not None and symbol not in real_map:
            log.info("[%s] BE skip — no en real_map, limpiando", symbol)
            await self.remove_trade(symbol, 0.0)
            return

        # FIX CRÍTICO: marcar como movido ANTES del intento
        # Así si falla, no vuelve a intentarlo en el siguiente ciclo
        trade.be_moved = True

        try:
            await self.client.cancel_all_orders(symbol)
            await asyncio.sleep(0.3)

            side_close = "SELL" if trade.direction == "LONG" else "BUY"

            resp = await self.client.place_stop_market_order(
                symbol, side_close, trade.qty, trade.entry,
                trade.direction, close_position=True, order_type="STOP_MARKET",
            )

            code = resp.get("code", -1)

            if code == 0:
                log.info("[%s] SL → breakeven @ %.6f ✓", symbol, trade.entry)

            elif code == 109420:
                # BingX confirma que la posición no existe → limpiar
                log.info("[%s] BE 109420: posición ya cerrada por BingX → limpiando", symbol)
                await self.remove_trade(symbol, 0.0)

            else:
                log.warning("[%s] BE fallo code=%s: %s", symbol, code, resp.get("msg", ""))
                # be_moved ya es True → no reintentará; trade se limpiará
                # en el próximo ciclo cuando no esté en real_map

        except Exception as e:
            log.error("[%s] _move_to_breakeven error: %s", symbol, e)
            # be_moved=True ya está seteado, no hay loop

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
