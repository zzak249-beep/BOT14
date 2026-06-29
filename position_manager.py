"""
PositionManager — joyful-art SHORT-only scalper.

Fixes vs previous version:
  1. entry_time persisted via state.py (survives Railway restart)
  2. Trail stop persisted via state.py (survives restart)
  3. TP1 flag persisted → TRAIL_DISTANCE_ATR_POST_TP1 tighter trail after TP1
  4. cancel_all_open_orders() BEFORE every TP/SL placement
     → fixes EURUSD 24-orders accumulation bug
  5. Notional check after rounding → skip if < MIN_NOTIONAL_USDT * 0.9
  6. Score-based tier sizing (STD / FUEL / SUP)
"""

import logging
import math

import config
import state
from bingx_client import BingXClient

log = logging.getLogger("pos_mgr")


# ── Expose trail functions from strategy module ────────────────

def update_trailing_stop(side, price, atr, mult, current):
    if side == "SHORT":
        candidate = price + atr * mult
        return candidate if current is None else min(current, candidate)
    else:
        candidate = price - atr * mult
        return candidate if current is None else max(current, candidate)


def trail_stop_hit(side, price, stop):
    if stop is None:
        return False
    return price >= stop if side == "SHORT" else price <= stop


# ─────────────────────────────────────────────────────────────

class PositionManager:
    def __init__(self, client: BingXClient):
        self.client = client

    # ── Symbol info ───────────────────────────────────────────

    def _sym_info(self, symbol: str) -> dict:
        try:
            return self.client.get_symbol_info(symbol)
        except Exception:
            return {}

    def _round_qty(self, symbol: str, qty: float) -> float:
        info   = self._sym_info(symbol)
        scale  = int(info.get("quantityScale", 3))
        factor = 10 ** scale
        return math.floor(qty * factor) / factor

    def _min_qty(self, symbol: str) -> float:
        info = self._sym_info(symbol)
        return float(info.get("tradeMinQuantity", 0.001))

    # ── Position sizing ───────────────────────────────────────

    def calc_qty(self, symbol: str, mark_price: float,
                 atr: float, equity: float, score: int = 0) -> float | None:
        """
        Risk-based sizing with score-tier multiplier.
        Returns None → caller should skip this symbol.

        FIX: checks actual notional AFTER rounding.
        Coins with very low prices get lot-size rounded below MIN_NOTIONAL.
        """
        if mark_price <= 0 or atr <= 0:
            return None

        risk_usdt = equity * (config.RISK_PCT / 100.0)
        sl_usdt   = atr * config.SL_ATR_MULT
        qty       = risk_usdt / sl_usdt

        # Score-based tier multiplier
        if score >= config.SUP_SCORE:
            qty *= 1.5
        elif score >= config.FUEL_SCORE:
            qty *= 1.0
        else:
            qty *= 0.7

        # Notional cap
        if mark_price > 0:
            qty = min(qty, config.MAX_NOTIONAL_USDT / mark_price)

        # Round to symbol precision
        qty = self._round_qty(symbol, qty)
        qty = max(qty, self._min_qty(symbol))

        # FIX: reject if actual notional is below minimum after rounding
        actual = qty * mark_price
        if actual < config.MIN_NOTIONAL_USDT * 0.90:
            log.warning(f"SKIP {symbol}: notional {actual:.2f} < min {config.MIN_NOTIONAL_USDT}")
            return None

        return qty

    # ── Position queries ──────────────────────────────────────

    def get_position(self, symbol: str, side: str) -> dict | None:
        for p in self.client.get_positions(symbol):
            if p["positionSide"] == side:
                return p
        return None

    def has_position(self, symbol: str, side: str = None) -> bool:
        return self.get_position(symbol, side or "SHORT") is not None

    # ── Max hold ──────────────────────────────────────────────

    def is_max_hold_expired(self, symbol: str, side: str) -> bool:
        """
        FIX: reads entry_time from disk (state.py).
        Previously wiped on every Railway redeploy → positions held 20-26h.
        """
        return state.is_max_hold_expired(symbol, side, config.MAX_HOLD_MINUTES)

    # ── Entries ───────────────────────────────────────────────

    def open_short(self, symbol: str, qty: float, atr: float) -> bool:
        try:
            self.client.set_leverage(symbol, config.LEVERAGE)
            self.client.place_market_order(symbol, "SELL", "SHORT", qty)
            state.save_entry(symbol, "SHORT")
            state.set_tp1_hit(symbol, "SHORT", False)
            state.set_be_moved(symbol, "SHORT", False)
            mark = self.client.get_mark_price(symbol)
            init_stop = mark + atr * config.TRAIL_DISTANCE_ATR
            state.save_trail(symbol, "SHORT", init_stop)
            log.info(f"OPEN SHORT {symbol}  qty={qty}  stop={init_stop:.6g}")
            return True
        except Exception as e:
            log.error(f"open_short {symbol}: {e}")
            return False

    # ── Exits ─────────────────────────────────────────────────

    def close_short(self, symbol: str, qty: float, reason: str = "") -> bool:
        try:
            self.client.cancel_all_open_orders(symbol)   # FIX: cancel first
            self.client.close_position(symbol, "SHORT", qty)
            state.clear(symbol, "SHORT")
            log.info(f"CLOSE SHORT {symbol}  qty={qty}  [{reason}]")
            return True
        except Exception as e:
            log.error(f"close_short {symbol}: {e}")
            return False

    # ── Trail stop ────────────────────────────────────────────

    def tick_trail(self, symbol: str, side: str,
                   price: float, atr: float) -> tuple:
        """
        FIX post-TP1: uses TRAIL_DISTANCE_ATR_POST_TP1 (tighter) after TP1 hit.
        Previously trail stayed at original width → gave back large profits.
        Returns (new_stop: float, is_hit: bool).
        """
        tp1_done = state.is_tp1_hit(symbol, side)
        mult     = (config.TRAIL_DISTANCE_ATR_POST_TP1
                    if tp1_done else config.TRAIL_DISTANCE_ATR)
        current  = state.get_trail(symbol, side)

        if side == "SHORT":
            candidate = price + atr * mult
            new_stop  = candidate if current is None else min(current, candidate)
            hit       = price >= new_stop
        else:
            candidate = price - atr * mult
            new_stop  = candidate if current is None else max(current, candidate)
            hit       = price <= new_stop

        state.save_trail(symbol, side, new_stop)
        return new_stop, hit

    # ── TP / Breakeven ─────────────────────────────────────────

    def should_take_tp1(self, symbol: str, side: str,
                         price: float, entry: float, atr: float) -> bool:
        if state.is_tp1_hit(symbol, side):
            return False
        dist = config.TP1_ATR_MULT * atr
        return price <= entry - dist if side == "SHORT" else price >= entry + dist

    def should_take_tp2(self, symbol: str, side: str,
                         price: float, entry: float, atr: float) -> bool:
        dist = config.TP2_ATR_MULT * atr
        return price <= entry - dist if side == "SHORT" else price >= entry + dist

    def should_move_breakeven(self, symbol: str, side: str,
                               price: float, entry: float, atr: float) -> bool:
        if state.is_be_moved(symbol, side):
            return False
        dist = config.BREAKEVEN_ATR_MULT * atr
        return price <= entry - dist if side == "SHORT" else price >= entry + dist

    def mark_tp1_hit(self, symbol: str, side: str):
        state.set_tp1_hit(symbol, side, True)
        log.info(f"TP1 hit {symbol} {side} → trail → {config.TRAIL_DISTANCE_ATR_POST_TP1}×ATR")

    def mark_be_moved(self, symbol: str, side: str):
        state.set_be_moved(symbol, side, True)

    # ── TP/SL placement ────────────────────────────────────────

    def place_tp_sl(self, symbol: str, side: str,
                    entry_price: float, qty: float, atr: float):
        """
        FIX: cancel ALL orders BEFORE placing → eliminates accumulation bug.
        Places: 1 SL stop-market + 1 TP1 limit order.
        """
        try:
            self.client.cancel_all_open_orders(symbol)
        except Exception as e:
            log.warning(f"cancel_all {symbol}: {e}")

        sl_price  = entry_price + atr * config.SL_ATR_MULT   # SHORT SL above entry
        tp1_price = entry_price - atr * config.TP1_ATR_MULT  # SHORT TP1 below entry
        tp_qty    = self._round_qty(symbol, qty * 0.5)

        try:
            self.client.place_stop_market(symbol, side, sl_price, qty)
        except Exception as e:
            log.error(f"place_sl {symbol}: {e}")

        if tp_qty >= self._min_qty(symbol):
            try:
                self.client.place_limit_order(symbol, "BUY", side, tp1_price, tp_qty)
            except Exception as e:
                log.error(f"place_tp1 {symbol}: {e}")

    def move_sl_to_breakeven(self, symbol: str, side: str,
                              entry_price: float, qty: float):
        try:
            self.client.cancel_all_open_orders(symbol)
            self.client.place_stop_market(symbol, side, entry_price, qty)
            self.mark_be_moved(symbol, side)
            log.info(f"BE moved {symbol} {side} @ {entry_price:.6g}")
        except Exception as e:
            log.error(f"move_be {symbol}: {e}")
