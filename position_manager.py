"""
PositionManager — renewed-love EMA9×VWAP scanner.

Fixes applied vs previous version:
  1. entry_time persisted via state.py (survives Railway restart)
  2. Trail stop persisted via state.py (survives restart)
  3. TP1 flag persisted → tighter trail post-TP1
  4. cancel_all_open_orders() BEFORE every place_tp_sl → fixes 18-orders bug
  5. Notional check after qty rounding → skip if < MIN_NOTIONAL_USDT*0.9
  6. Breakeven SL persisted → not re-placed after restart
"""

import logging
import math
import time

import config
import state
from bingx_client import BingXClient

log = logging.getLogger("pos_mgr")


class PositionManager:
    def __init__(self, client: BingXClient):
        self.client = client

    # ── Symbol info helpers ───────────────────────────────────

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

    def calc_qty(self, symbol: str, mark_price: float) -> float | None:
        """
        Fixed notional sizing with notional sanity check.
        Returns None → caller should SKIP this symbol (don't open).

        Fix: coins with micro prices (e.g. SAHARA 0.011) get rounded down
        below MIN_NOTIONAL after BingX applies lot-size step constraints.
        Previously the bot opened anyway → 11 USDT instead of 15 USDT target.
        """
        if mark_price <= 0:
            return None

        qty_raw = config.FIXED_NOTIONAL_USDT / mark_price
        qty     = self._round_qty(symbol, qty_raw)
        min_q   = self._min_qty(symbol)
        qty     = max(qty, min_q)

        # ── FIX: check actual notional AFTER rounding ─────────
        actual_notional = qty * mark_price
        if actual_notional < config.MIN_NOTIONAL_USDT * 0.90:
            log.warning(
                f"SKIP {symbol}: notional after rounding = {actual_notional:.2f} USDT "
                f"< MIN {config.MIN_NOTIONAL_USDT} (price={mark_price})"
            )
            return None

        # Cap by MAX_NOTIONAL_USDT
        if actual_notional > config.MAX_NOTIONAL_USDT:
            qty = self._round_qty(symbol, config.MAX_NOTIONAL_USDT / mark_price)

        return qty

    # ── Position queries ──────────────────────────────────────

    def get_position(self, symbol: str, side: str) -> dict | None:
        for p in self.client.get_positions(symbol):
            if p["positionSide"] == side:
                return p
        return None

    def has_position(self, symbol: str, side: str) -> bool:
        return self.get_position(symbol, side) is not None

    def count_open_positions(self) -> int:
        return len(self.client.get_positions())

    # ── Max hold check ────────────────────────────────────────

    def is_max_hold_expired(self, symbol: str, side: str) -> bool:
        """
        FIX: reads entry_time from disk (state.py) instead of RAM dict.
        Previously wiped on every Railway redeploy → positions held 24h+.
        """
        return state.is_max_hold_expired(symbol, side, config.MAX_HOLD_MINUTES)

    # ── Entries ───────────────────────────────────────────────

    def open_long(self, symbol: str, qty: float, atr: float) -> bool:
        try:
            self.client.set_leverage(symbol, config.LEVERAGE)
            self.client.place_market_order(symbol, "BUY", "LONG", qty)
            state.save_entry(symbol, "LONG")
            state.set_tp1_hit(symbol, "LONG", False)
            state.set_be_moved(symbol, "LONG", False)
            mark = self.client.get_mark_price(symbol)
            state.save_trail(symbol, "LONG", mark - atr * config.TRAIL_DISTANCE_ATR)
            log.info(f"OPEN LONG  {symbol}  qty={qty}  atr={atr:.4f}")
            return True
        except Exception as e:
            log.error(f"open_long {symbol}: {e}")
            return False

    def open_short(self, symbol: str, qty: float, atr: float) -> bool:
        try:
            self.client.set_leverage(symbol, config.LEVERAGE)
            self.client.place_market_order(symbol, "SELL", "SHORT", qty)
            state.save_entry(symbol, "SHORT")
            state.set_tp1_hit(symbol, "SHORT", False)
            state.set_be_moved(symbol, "SHORT", False)
            mark = self.client.get_mark_price(symbol)
            state.save_trail(symbol, "SHORT", mark + atr * config.TRAIL_DISTANCE_ATR)
            log.info(f"OPEN SHORT {symbol}  qty={qty}  atr={atr:.4f}")
            return True
        except Exception as e:
            log.error(f"open_short {symbol}: {e}")
            return False

    # ── Exits ─────────────────────────────────────────────────

    def close_long(self, symbol: str, qty: float, reason: str = "") -> bool:
        try:
            self.client.cancel_all_open_orders(symbol)  # FIX: cancel TP/SL first
            self.client.close_position(symbol, "LONG", qty)
            state.clear(symbol, "LONG")
            log.info(f"CLOSE LONG  {symbol}  qty={qty}  [{reason}]")
            return True
        except Exception as e:
            log.error(f"close_long {symbol}: {e}")
            return False

    def close_short(self, symbol: str, qty: float, reason: str = "") -> bool:
        try:
            self.client.cancel_all_open_orders(symbol)  # FIX: cancel TP/SL first
            self.client.close_position(symbol, "SHORT", qty)
            state.clear(symbol, "SHORT")
            log.info(f"CLOSE SHORT {symbol}  qty={qty}  [{reason}]")
            return True
        except Exception as e:
            log.error(f"close_short {symbol}: {e}")
            return False

    # ── ATR Trailing stop ─────────────────────────────────────

    def tick_trail(self, symbol: str, side: str, price: float, atr: float) -> tuple:
        """
        Update trailing stop, persisted to disk.

        FIX post-TP1: after TP1 hit, uses TRAIL_DISTANCE_ATR_POST_TP1 (tighter)
        instead of the original wide TRAIL_DISTANCE_ATR.
        Previously trail stayed at 2.5×ATR always → positions reverted from
        peak and closed with far less profit than the maximum.

        Returns (new_stop: float, is_hit: bool)
        """
        tp1_done = state.is_tp1_hit(symbol, side)
        mult     = (
            config.TRAIL_DISTANCE_ATR_POST_TP1
            if tp1_done
            else config.TRAIL_DISTANCE_ATR
        )
        current  = state.get_trail(symbol, side)

        if side == "LONG":
            candidate = price - atr * mult
            new_stop  = candidate if current is None else max(current, candidate)
            hit       = price <= new_stop
        else:
            candidate = price + atr * mult
            new_stop  = candidate if current is None else min(current, candidate)
            hit       = price >= new_stop

        state.save_trail(symbol, side, new_stop)
        return new_stop, hit

    # ── Breakeven ─────────────────────────────────────────────

    def should_move_breakeven(self, symbol: str, side: str,
                              price: float, entry: float, atr: float) -> bool:
        """Returns True if breakeven should be moved (not moved yet, threshold reached)."""
        if state.is_be_moved(symbol, side):
            return False
        dist = config.BREAKEVEN_ATR_MULT * atr
        if side == "LONG":
            return price >= entry + dist
        else:
            return price <= entry - dist

    # ── TP1 detection ─────────────────────────────────────────

    def should_take_tp1(self, symbol: str, side: str,
                         price: float, entry: float, atr: float) -> bool:
        if state.is_tp1_hit(symbol, side):
            return False
        dist = config.TP1_ATR_MULT * atr
        if side == "LONG":
            return price >= entry + dist
        else:
            return price <= entry - dist

    def should_take_tp2(self, symbol: str, side: str,
                         price: float, entry: float, atr: float) -> bool:
        dist = config.TP2_ATR_MULT * atr
        if side == "LONG":
            return price >= entry + dist
        else:
            return price <= entry - dist

    def mark_tp1_hit(self, symbol: str, side: str):
        state.set_tp1_hit(symbol, side, True)
        mult = config.TRAIL_DISTANCE_ATR_POST_TP1
        log.info(f"TP1 hit {symbol} {side} → trail tightened to {mult}×ATR")

    def mark_be_moved(self, symbol: str, side: str):
        state.set_be_moved(symbol, side, True)

    # ── TP/SL order placement ─────────────────────────────────

    def place_tp_sl(self, symbol: str, side: str, entry_price: float,
                    qty: float, atr: float):
        """
        FIX: cancel ALL existing orders BEFORE placing new ones.
        Previously: new orders placed on every iteration → SAHARA accumulated 18 orders.
        Now: cancel → place exactly 1 SL + 1 TP1 = 2 orders max.
        """
        # ── FIX: always cancel first ──────────────────────────
        try:
            self.client.cancel_all_open_orders(symbol)
        except Exception as e:
            log.warning(f"cancel_all_open_orders {symbol}: {e}")

        sl_price  = (
            entry_price - atr * config.SL_ATR_MULT  if side == "LONG"
            else entry_price + atr * config.SL_ATR_MULT
        )
        tp1_price = (
            entry_price + atr * config.TP1_ATR_MULT if side == "LONG"
            else entry_price - atr * config.TP1_ATR_MULT
        )

        tp_qty = self._round_qty(symbol, qty * 0.5)  # TP1 closes 50%
        min_q  = self._min_qty(symbol)

        try:
            self.client.place_stop_market(symbol, side, sl_price, qty)
            log.info(f"SL placed  {symbol} {side} @ {sl_price:.6g}")
        except Exception as e:
            log.error(f"place_sl {symbol}: {e}")

        if tp_qty >= min_q:
            try:
                close_side = "SELL" if side == "LONG" else "BUY"
                self.client.place_limit_order(symbol, close_side, side, tp1_price, tp_qty)
                log.info(f"TP1 placed {symbol} {side} @ {tp1_price:.6g} qty={tp_qty}")
            except Exception as e:
                log.error(f"place_tp1 {symbol}: {e}")

    def move_sl_to_breakeven(self, symbol: str, side: str,
                              entry_price: float, qty: float):
        """Reprices SL at entry (breakeven) — cancel all orders first."""
        try:
            self.client.cancel_all_open_orders(symbol)
            self.client.place_stop_market(symbol, side, entry_price, qty)
            self.mark_be_moved(symbol, side)
            log.info(f"BE moved {symbol} {side} @ {entry_price:.6g}")
        except Exception as e:
            log.error(f"move_sl_to_breakeven {symbol}: {e}")
