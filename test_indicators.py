"""
Sanity tests for indicators.py - no external dependencies.
Run with:  python -m unittest discover -s tests -v
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from indicators import (
    StrategyParams,
    compute_rsi,
    compute_supertrend,
    ema,
    evaluate_htf_trend,
    find_pivot_low_at,
    generate_signals,
    rma,
    validate_double_bottom_pair,
)


class TestRma(unittest.TestCase):
    def test_seed_is_simple_average(self):
        vals = [1, 2, 3, 4, 5]
        out = rma(vals, 5)
        self.assertAlmostEqual(out[4], sum(vals) / 5)
        self.assertIsNone(out[3])

    def test_recursive_step(self):
        vals = [10, 10, 10, 10, 10, 20]
        out = rma(vals, 5)
        self.assertAlmostEqual(out[5], 12.0)  # (1/5)*20 + (4/5)*10


class TestRsi(unittest.TestCase):
    def test_all_gains_is_100(self):
        closes = [float(i) for i in range(1, 30)]
        self.assertAlmostEqual(compute_rsi(closes, 10)[-1], 100.0)

    def test_all_losses_is_0(self):
        closes = [float(i) for i in range(30, 1, -1)]
        self.assertAlmostEqual(compute_rsi(closes, 10)[-1], 0.0)

    def test_flat_is_100_by_convention(self):
        closes = [100.0] * 30
        self.assertAlmostEqual(compute_rsi(closes, 10)[-1], 100.0)


class TestSupertrend(unittest.TestCase):
    def test_uptrend_direction(self):
        closes = [100 + i * 2 for i in range(60)]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        st, direction = compute_supertrend(highs, lows, closes, 10, 2.5)
        self.assertEqual(direction[-1], -1)
        self.assertLess(st[-1], closes[-1])

    def test_downtrend_direction(self):
        closes = [500 - i * 2 for i in range(60)]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        st, direction = compute_supertrend(highs, lows, closes, 10, 2.5)
        self.assertEqual(direction[-1], 1)
        self.assertGreater(st[-1], closes[-1])

    def test_flip_sets_st_sell(self):
        up = [100 + i * 3 for i in range(40)]
        down = [up[-1] - i * 6 for i in range(1, 40)]
        closes = up + down
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        params = StrategyParams(st_atr_period=10, st_factor=2.5)
        sig = generate_signals(highs, lows, closes, params)
        self.assertTrue(any(sig["st_sell"]), "expected at least one SuperTrend flip to SELL")


class TestPivotLow(unittest.TestCase):
    def test_simple_v_shape(self):
        lows = [10, 9, 8, 7, 6, 7, 8, 9, 10]
        # index 4 (value 6) is the minimum of the whole series -> pivot
        self.assertTrue(find_pivot_low_at(lows, 4, 3, 3))
        self.assertFalse(find_pivot_low_at(lows, 3, 3, 3))

    def test_needs_full_window(self):
        lows = [10, 9, 8, 7, 6]
        # not enough bars on the right to confirm index 4 with right=3
        self.assertFalse(find_pivot_low_at(lows, 4, 2, 3))


def _build_double_bottom(l1=100.0, l2=101.0, neckline=112.0, bars_between=10, rsi_l1=25.0, rsi_l2=40.0, tail=15):
    """Builds a synthetic OHLC series shaped like a valid W: dip to l1,
    bounce to `neckline`, dip to l2 (with the given RSI drift baked in via
    price shape), then a clean push back through the neckline."""
    highs, lows, closes = [], [], []

    def add(c, spread=1.0):
        closes.append(c)
        highs.append(c + spread)
        lows.append(c - spread)

    # warm-up trend down into the first low so RSI has real gain/loss history
    for i in range(20):
        add(150 - i * 2.5)
    add(l1)  # first pivot low candidate
    # bounce to the neckline
    up_steps = bars_between // 2
    for i in range(1, up_steps + 1):
        add(l1 + (neckline - l1) * i / up_steps)
    # gentle pull back down to the second low (shallower than the first leg,
    # which combined with the RSI seed drift gives RSI(l2) > RSI(l1))
    down_steps = bars_between - up_steps
    for i in range(1, down_steps + 1):
        add(neckline - (neckline - l2) * i / down_steps)
    # confirmation bars to the right of L2 (pivot needs `right` bars to confirm) -
    # start at i=1 so we don't accidentally repeat the exact L2 price (that
    # would register as a second, spurious adjacent pivot low).
    for i in range(1, 7):
        add(l2 + i * 0.2)
    # clean push back above the neckline
    for i in range(1, tail + 1):
        add(l2 + (neckline - l2 + 5) * i / tail)

    return highs, lows, closes


class TestDoubleBottomDetector(unittest.TestCase):
    def test_valid_double_bottom_fires_on_neckline_break(self):
        highs, lows, closes = _build_double_bottom()
        params = StrategyParams(
            rsi_length=10, trigger_level=50, pivot_left=3, pivot_right=3,
            max_bottom_diff_pct=3.0, min_bars_between_lows=3, max_bars_between_lows=30,
            min_neckline_bounce_pct=1.0, require_rsi_divergence=False,  # isolate the structural check first
            max_wait_bars=30, st_atr_period=10, st_factor=2.5,
        )
        sig = generate_signals(highs, lows, closes, params)
        self.assertTrue(any(sig["special_buy"]), "expected the neckline-break entry to fire")
        fire_idx = sig["special_buy"].index(True)
        self.assertGreater(closes[fire_idx], sig["setup_neckline"][fire_idx])

    def test_too_far_apart_lows_do_not_fire(self):
        highs, lows, closes = _build_double_bottom(bars_between=4)  # below default min spacing of ~ not really testable directly; use param instead
        params = StrategyParams(
            pivot_left=3, pivot_right=3, max_bottom_diff_pct=3.0,
            min_bars_between_lows=25, max_bars_between_lows=50,  # force a spacing violation
            min_neckline_bounce_pct=1.0, require_rsi_divergence=False, max_wait_bars=30,
        )
        sig = generate_signals(highs, lows, closes, params)
        self.assertFalse(any(sig["special_buy"]), "spacing filter should have rejected this pair")

    def test_price_too_far_apart_does_not_fire(self):
        highs, lows, closes = _build_double_bottom(l1=100.0, l2=115.0)  # ~15% apart
        params = StrategyParams(
            pivot_left=3, pivot_right=3, max_bottom_diff_pct=2.0,  # only allow 2%
            min_bars_between_lows=3, max_bars_between_lows=40,
            min_neckline_bounce_pct=1.0, require_rsi_divergence=False, max_wait_bars=30,
        )
        sig = generate_signals(highs, lows, closes, params)
        self.assertFalse(any(sig["special_buy"]), "price-proximity filter should have rejected this pair")


class TestPairValidation(unittest.TestCase):
    """Direct tests of the validation rule, independent of pivot scanning -
    lets us hand-pick RSI values instead of fighting synthetic price data
    into producing a specific RSI drift."""

    def base_params(self, **overrides):
        defaults = dict(
            trigger_level=50, min_bars_between_lows=3, max_bars_between_lows=50,
            max_bottom_diff_pct=2.0, min_neckline_bounce_pct=1.0, require_rsi_divergence=True,
        )
        defaults.update(overrides)
        return StrategyParams(**defaults)

    def test_bullish_divergence_passes(self):
        # similar price lows, RSI higher on the 2nd low -> valid
        ok = validate_double_bottom_pair(
            l1_price=100.0, l2_price=101.0, l1_rsi=22.0, l2_rsi=38.0,
            bars_between=10, neckline=112.0, params=self.base_params(),
        )
        self.assertTrue(ok)

    def test_missing_divergence_fails(self):
        # same structure, but RSI is LOWER on the 2nd low -> no real divergence -> reject
        ok = validate_double_bottom_pair(
            l1_price=100.0, l2_price=101.0, l1_rsi=38.0, l2_rsi=22.0,
            bars_between=10, neckline=112.0, params=self.base_params(),
        )
        self.assertFalse(ok)

    def test_divergence_not_required_when_disabled(self):
        ok = validate_double_bottom_pair(
            l1_price=100.0, l2_price=101.0, l1_rsi=38.0, l2_rsi=22.0,
            bars_between=10, neckline=112.0,
            params=self.base_params(require_rsi_divergence=False),
        )
        self.assertTrue(ok)

    def test_second_low_above_trigger_level_fails(self):
        # RSI recovered too much by the 2nd low - no longer "buying a dip"
        ok = validate_double_bottom_pair(
            l1_price=100.0, l2_price=101.0, l1_rsi=22.0, l2_rsi=61.0,
            bars_between=10, neckline=112.0, params=self.base_params(),
        )
        self.assertFalse(ok)

    def test_flat_neckline_fails_bounce_filter(self):
        # lows are close but there's barely any bounce between them
        ok = validate_double_bottom_pair(
            l1_price=100.0, l2_price=100.5, l1_rsi=22.0, l2_rsi=30.0,
            bars_between=10, neckline=100.6, params=self.base_params(min_neckline_bounce_pct=1.0),
        )
        self.assertFalse(ok)


class TestEma(unittest.TestCase):
    def test_seed_is_simple_average(self):
        vals = [1, 2, 3, 4, 5]
        out = ema(vals, 5)
        self.assertAlmostEqual(out[4], 3.0)
        self.assertIsNone(out[3])

    def test_reacts_faster_than_rma(self):
        # EMA(2/(n+1)) has a bigger alpha than RMA(1/n) for the same length,
        # so it should move further toward a new value in one step.
        vals = [10, 10, 10, 10, 10, 30]
        e = ema(vals, 5)[5]
        r = rma(vals, 5)[5]
        self.assertGreater(e, r)


class TestHtfTrend(unittest.TestCase):
    def test_rising_ema_is_ok(self):
        closes = [100 + i * 1.5 for i in range(150)]
        info = evaluate_htf_trend(closes, ema_length=100, slope_lookback=10, max_down_slope_pct=0.5)
        self.assertTrue(info["trend_ok"])
        self.assertGreater(info["slope_pct"], 0)

    def test_sharply_falling_ema_is_blocked(self):
        closes = [500 - i * 4 for i in range(150)]
        info = evaluate_htf_trend(closes, ema_length=100, slope_lookback=10, max_down_slope_pct=0.5)
        self.assertFalse(info["trend_ok"])
        self.assertLess(info["slope_pct"], -0.5)

    def test_mild_dip_within_tolerance_is_ok(self):
        # small, slow drift down - within the allowed tolerance
        closes = [200 - i * 0.02 for i in range(150)]
        info = evaluate_htf_trend(closes, ema_length=100, slope_lookback=10, max_down_slope_pct=0.5)
        self.assertTrue(info["trend_ok"])

    def test_insufficient_data_fails_closed(self):
        closes = [100.0] * 20  # fewer bars than ema_length
        info = evaluate_htf_trend(closes, ema_length=100, slope_lookback=10, max_down_slope_pct=0.5)
        self.assertFalse(info["trend_ok"])
        self.assertIsNone(info["ema"])


if __name__ == "__main__":
    unittest.main()
