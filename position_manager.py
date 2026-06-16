"""
QF×JP Bot v6.6 — Position Manager COMPLETO
Implementaciones nuevas:
  1. Trailing Stop dinámico (sigue precio con ATR)
  2. Partial close en TP1 (cierra 50%, deja correr el resto)
  3. SL emergencia al reconciliar (desde mark price, no entry)
  4. Fix 109420 definitivo: positionSide LONG/SHORT en Hedge Mode
  5. Emergency SL al arrancar para posiciones sin protección
"""
import asyncio
import logging
from dataclasses import dataclass, field

import config as C
from bingx_client import BingXClient
from risk_manager import RiskManager
import telegram_client as tg
try:
    from trade_analytics import analytics
except ImportError:
    analytics = None

log = logging.getLogger("position_mgr")


@dataclass
class OpenTrade:
    symbol:         str
    direction:      str
    entry:          float
    sl:             float
    tp1:            float
    tp2:            float
    qty:            float
    atr:            float
    order_id:       str
    be_moved:       bool  = False
    tp1_hit:        bool  = False
    position_side:  str   = ""      # LONG/SHORT/BOTH — leído de BingX
    trailing_active: bool = False   # trailing stop activado
    trailing_sl:    float = 0.0     # precio actual del trailing SL
    qty_remaining:  float = 0.0     # qty tras partial close en TP1


class PositionManager:
    def __init__(self, client: BingXClient, risk: RiskManager):
        self.client = client
        self.risk   = risk
        self._trades: dict[str, OpenTrade] = {}
        self._lock   = asyncio.Lock()

    # ══════════════════════════════════════════════════════════════════════════
    # RECONCILIAR AL ARRANCAR
    # ══════════════════════════════════════════════════════════════════════════

    async def reconcile_on_startup(self):
        """Lee posiciones reales de BingX y coloca SL emergencia inmediato."""
        try:
            positions = await self.client.get_open_positions()
        except Exception as e:
            log.warning("reconcile error: %s", e)
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
            pos_side  = pos.get("positionSide", "")
            if pos_side not in ("LONG", "SHORT", "BOTH"):
                pos_side = direction   # Hedge Mode default
            entry = float(pos.get("avgPrice", pos.get("entryPrice", 0)) or 0)
            qty   = abs(amt)
            atr   = entry * 0.005
            sl    = entry * (0.97 if direction == "LONG" else 1.03)
            tp1   = entry * (1.02 if direction == "LONG" else 0.98)
            tp2   = entry * (1.04 if direction == "LONG" else 0.96)
            async with self._lock:
                self._trades[sym] = OpenTrade(
                    symbol=sym, direction=direction, entry=entry,
                    sl=sl, tp1=tp1, tp2=tp2, qty=qty, atr=atr,
                    order_id="reconciled", position_side=pos_side,
                    qty_remaining=qty,
                )
            count += 1
            log.info("[%s] Reconciliado %s qty=%.4f @ %.6f ps=%s",
                     sym, direction, qty, entry, pos_side)

        if count:
            log.info("reconcile: %d posición(es) — colocando SL emergencia...", count)
            await self._place_emergency_sl_all()

    async def _place_emergency_sl_all(self):
        """
        SL inmediato desde mark price actual (no entry).
        Evita 'Stop Loss price should be greater/less than current price'.
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
                # 2.5% desde mark — suficiente margen para no tocarlo inmediatamente
                sl_price = mark * 0.975 if trade.direction == "LONG" else mark * 1.025

                log.info("[%s] SL emergencia mark=%.6f sl=%.6f %s",
                         sym, mark, sl_price, trade.direction)

                resp = await self.client.place_stop_market_order(
                    sym, side_close, trade.qty, sl_price, trade.direction,
                )
                if resp.get("code", -1) == 0:
                    trade.sl = sl_price
                    log.info("[%s] SL emergencia OK @ %.6f", sym, sl_price)
                else:
                    log.error("[%s] SL emergencia FALLIDO: %s", sym, resp)
            except Exception as e:
                log.error("[%s] _place_emergency_sl_all: %s", sym, e)
            await asyncio.sleep(0.4)

    # ══════════════════════════════════════════════════════════════════════════
    # REGISTRO
    # ══════════════════════════════════════════════════════════════════════════

    async def register_trade(self, trade: OpenTrade):
        if trade.qty_remaining == 0.0:
            trade.qty_remaining = trade.qty
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

    # ══════════════════════════════════════════════════════════════════════════
    # MONITOR LOOP
    # ══════════════════════════════════════════════════════════════════════════

    async def monitor_loop(self):
        log.info("Monitor iniciado (intervalo=%ds)", C.POSITION_CHECK_INTERVAL)
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

        await self.risk.update_open_count(len(real_map))

        async with self._lock:
            tracked = dict(self._trades)

        for symbol, trade in tracked.items():

            # ── Posición cerrada externamente ─────────────────────────────────
            if symbol not in real_map:
                try:
                    ticker      = await self.client.get_ticker(symbol)
                    close_price = float(ticker.get("lastPrice", trade.entry))
                except Exception:
                    close_price = trade.entry
                pnl = self._calc_pnl(trade, close_price)
                log.info("[%s] Cerrada externamente PnL≈%.2f", symbol, pnl)
                await tg.notify_trade_closed(
                    symbol, trade.direction, trade.entry,
                    close_price, trade.qty, "sl_tp_auto", pnl,
                )
                if analytics:
                    await analytics.on_trade_closed(
                        symbol, trade.direction, trade.entry, close_price, pnl, "sl_tp_auto"
                    )
                await self.remove_trade(symbol, pnl)
                continue

            # ── Mark price ────────────────────────────────────────────────────
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

            # ── 1. PARTIAL CLOSE en TP1 ───────────────────────────────────────
            if not trade.tp1_hit:
                tp1_reached = (
                    (trade.direction == "LONG"  and mark >= trade.tp1) or
                    (trade.direction == "SHORT" and mark <= trade.tp1)
                )
                if tp1_reached:
                    await self._partial_close_tp1(trade, mark, real_map)

            # ── 2. TRAILING STOP ──────────────────────────────────────────────
            if trade.tp1_hit:
                await self._update_trailing_stop(trade, mark, real_map)

            # ── 3. BREAKEVEN (solo si trailing no activo) ─────────────────────
            elif not trade.be_moved:
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

    # ══════════════════════════════════════════════════════════════════════════
    # 1. PARTIAL CLOSE EN TP1
    # ══════════════════════════════════════════════════════════════════════════

    async def _partial_close_tp1(self, trade: OpenTrade, mark: float, real_map: dict):
        """
        Cierra 50% de la posición en TP1.
        La otra mitad sigue con trailing stop.
        """
        try:
            import math
            precision  = 6
            qty_half   = math.floor(trade.qty / 2 * 10**precision) / 10**precision
            qty_remain = round(trade.qty - qty_half, precision)

            if qty_half <= 0:
                trade.tp1_hit = True
                return

            side_close = "SELL" if trade.direction == "LONG" else "BUY"

            # Cancelar órdenes existentes primero
            try:
                await self.client.cancel_all_orders(trade.symbol)
                await asyncio.sleep(0.3)
            except Exception:
                pass

            # Cerrar 50% a mercado
            resp = await self.client.close_position_market(
                trade.symbol, qty_half, trade.direction
            )

            if resp.get("code", -1) == 0:
                pnl_half = self._calc_pnl_qty(trade, mark, qty_half)
                trade.tp1_hit      = True
                trade.qty_remaining = qty_remain
                trade.trailing_active = True
                trade.trailing_sl     = trade.entry  # empieza en BE

                log.info("[%s] PARTIAL CLOSE 50%% @ %.6f pnl_half=%.4f — resto trailing",
                         trade.symbol, mark, pnl_half)
                await tg.send(
                    f"📤 *PARTIAL CLOSE* — `{trade.symbol}`\n"
                    f"TP1 alcanzado @ `{mark:.6f}`\n"
                    f"Cerrado 50% | PnL parcial: `{pnl_half:+.4f}` USDT\n"
                    f"Resto `{qty_remain:.4f}` con trailing stop activo 🎯"
                )

                # Colocar nuevo SL en BE para la cantidad restante
                await asyncio.sleep(0.3)
                sl_resp = await self.client.place_stop_market_order(
                    trade.symbol, side_close, qty_remain,
                    trade.entry, trade.direction,
                )
                if sl_resp.get("code", -1) == 0:
                    trade.be_moved = True
                    log.info("[%s] SL resto → BE @ %.6f", trade.symbol, trade.entry)
                else:
                    log.warning("[%s] SL BE fallido tras partial: %s", trade.symbol, sl_resp)
            else:
                log.warning("[%s] partial close fallido: %s — marcando tp1_hit igual",
                            trade.symbol, resp)
                trade.tp1_hit = True

        except Exception as e:
            log.error("[%s] _partial_close_tp1: %s", trade.symbol, e)
            trade.tp1_hit = True

    # ══════════════════════════════════════════════════════════════════════════
    # 2. TRAILING STOP DINÁMICO
    # ══════════════════════════════════════════════════════════════════════════

    async def _update_trailing_stop(self, trade: OpenTrade, mark: float, real_map: dict):
        """
        Trailing stop dinámico basado en ATR.
        LONG:  trailing_sl = max(trailing_sl, mark - ATR * TRAIL_MULT)
        SHORT: trailing_sl = min(trailing_sl, mark + ATR * TRAIL_MULT)
        
        Solo actualiza cuando el nuevo SL mejora el anterior (nunca retrocede).
        """
        if not trade.trailing_active:
            return

        trail_mult = getattr(C, "TRAIL_ATR_MULT", 1.5)
        atr        = trade.atr

        if trade.direction == "LONG":
            new_sl = mark - atr * trail_mult
            if new_sl <= trade.trailing_sl:
                return   # no mejora → no actualizar
            # Verificar que el nuevo SL es menor que el mark (válido para LONG)
            if new_sl >= mark:
                return
        else:
            new_sl = mark + atr * trail_mult
            if new_sl >= trade.trailing_sl and trade.trailing_sl > 0:
                return   # no mejora → no actualizar
            if new_sl <= mark:
                return

        # Solo actualizar si la mejora es significativa (>0.1 ATR)
        if abs(new_sl - trade.trailing_sl) < atr * 0.1:
            return

        old_sl = trade.trailing_sl
        trade.trailing_sl = new_sl

        log.info("[%s] TRAILING: %.6f → %.6f (mark=%.6f)",
                 trade.symbol, old_sl, new_sl, mark)

        # Cancelar SL anterior y colocar nuevo
        try:
            await self.client.cancel_all_orders(trade.symbol)
            await asyncio.sleep(0.2)
        except Exception:
            pass

        qty       = trade.qty_remaining if trade.qty_remaining > 0 else trade.qty
        side_close = "SELL" if trade.direction == "LONG" else "BUY"

        resp = await self.client.place_stop_market_order(
            trade.symbol, side_close, qty, new_sl, trade.direction,
        )
        if resp.get("code", -1) == 0:
            trade.sl = new_sl
            log.info("[%s] Trailing SL actualizado @ %.6f", trade.symbol, new_sl)
        else:
            # Revertir si falla
            trade.trailing_sl = old_sl
            log.warning("[%s] Trailing SL fallido: %s", trade.symbol, resp)

    # ══════════════════════════════════════════════════════════════════════════
    # 3. BREAKEVEN
    # ══════════════════════════════════════════════════════════════════════════

    async def _move_to_breakeven(self, trade: OpenTrade, current_price: float,
                                  real_map: dict = None):
        try:
            if real_map is not None and trade.symbol not in real_map:
                log.info("[%s] BE skip — no en real_map", trade.symbol)
                await self.remove_trade(trade.symbol, 0.0)
                return

            try:
                await self.client.cancel_all_orders(trade.symbol)
            except Exception:
                pass
            await asyncio.sleep(0.3)

            side_close = "SELL" if trade.direction == "LONG" else "BUY"
            qty        = trade.qty_remaining if trade.qty_remaining > 0 else trade.qty

            resp = await self.client.place_stop_market_order(
                trade.symbol, side_close, qty,
                trade.entry, trade.direction,
            )
            if resp.get("code", -1) == 0:
                trade.be_moved    = True
                trade.trailing_sl = trade.entry
                log.info("[%s] SL → BE @ %.6f", trade.symbol, trade.entry)
            else:
                log.warning("[%s] BE fallo: %s", trade.symbol, resp)
                # Fallback: SL desde mark con 2%
                sl_fallback = (
                    current_price * 0.98 if trade.direction == "LONG"
                    else current_price * 1.02
                )
                sl_resp = await self.client.place_stop_market_order(
                    trade.symbol, side_close, qty,
                    sl_fallback, trade.direction,
                )
                if sl_resp.get("code", -1) == 0:
                    trade.sl = sl_fallback
                    log.info("[%s] SL fallback @ %.6f", trade.symbol, sl_fallback)
                else:
                    log.error("[%s] SL NO colocado: %s", trade.symbol, sl_resp)
        except Exception as e:
            log.error("[%s] _move_to_breakeven: %s", trade.symbol, e)

    # ══════════════════════════════════════════════════════════════════════════
    # CIERRE DE EMERGENCIA
    # ══════════════════════════════════════════════════════════════════════════

    async def close_position_emergency(self, symbol: str, reason: str = "emergency"):
        async with self._lock:
            trade = self._trades.get(symbol)
        if not trade:
            log.warning("[%s] close_emergency: no registrado", symbol)
            return
        try:
            try:
                await self.client.cancel_all_orders(symbol)
            except Exception:
                pass
            await asyncio.sleep(0.2)
            qty = trade.qty_remaining if trade.qty_remaining > 0 else trade.qty
            await self.client.close_position_market(symbol, qty, trade.direction)
            ticker      = await self.client.get_ticker(symbol)
            close_price = float(ticker.get("lastPrice", trade.entry))
            pnl         = self._calc_pnl(trade, close_price)
            log.info("[%s] Cierre emergencia PnL=%.2f", symbol, pnl)
            await tg.notify_trade_closed(symbol, trade.direction, trade.entry,
                                         close_price, qty, reason, pnl)
            if analytics:
                await analytics.on_trade_closed(
                    symbol, trade.direction, trade.entry, close_price, pnl, reason
                )
            await self.remove_trade(symbol, pnl)
        except Exception as e:
            log.error("[%s] close_emergency error: %s", symbol, e)

    # ══════════════════════════════════════════════════════════════════════════
    # HELPERS
    # ══════════════════════════════════════════════════════════════════════════

    def _calc_pnl(self, trade: OpenTrade, close_price: float) -> float:
        qty = trade.qty_remaining if trade.qty_remaining > 0 else trade.qty
        if trade.direction == "LONG":
            raw = (close_price - trade.entry) * qty
        else:
            raw = (trade.entry - close_price) * qty
        return round(raw * C.LEVERAGE, 4)

    def _calc_pnl_qty(self, trade: OpenTrade, close_price: float, qty: float) -> float:
        if trade.direction == "LONG":
            raw = (close_price - trade.entry) * qty
        else:
            raw = (trade.entry - close_price) * qty
        return round(raw * C.LEVERAGE, 4)

    def get_tracked(self) -> dict[str, OpenTrade]:
        return dict(self._trades)

    def is_trading(self, symbol: str) -> bool:
        return symbol in self._trades
