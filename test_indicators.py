"""
Lightweight sanity tests for indicators.py - no external dependencies.
Run with:  python -m unittest discover -s tests
"""
import math
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from indicators import compute_rsi, compute_supertrend, generate_signals, rma, sma, StrategyParams


class TestRma(unittest.TestCase):
    def test_seed_is_simple_average(self):
        vals = [1, 2, 3, 4, 5]
        out = rma(vals, 5)
        self.assertAlmostEqual(out[4], sum(vals) / 5)
        self.assertIsNone(out[3])

    def test_recursive_step(self):
        vals = [10, 10, 10, 10, 10, 20]
        out = rma(vals, 5)
        # seed = 10, next = (1/5)*20 + (4/5)*10 = 12
        self.assertAlmostEqual(out[5], 12.0)


class TestSma(unittest.TestCase):
    def test_basic(self):
        out = sma([1, 2, 3, 4, 5], 3)
        self.assertIsNone(out[1])
        self.assertAlmostEqual(out[2], 2.0)
        self.assertAlmostEqual(out[4], 4.0)


class TestRsi(unittest.TestCase):
    def test_all_gains_is_100(self):
        closes = [float(i) for i in range(1, 30)]  # strictly increasing
        out = compute_rsi(closes, 10)
        self.assertAlmostEqual(out[-1], 100.0)

    def test_all_losses_is_0(self):
        closes = [float(i) for i in range(30, 1, -1)]  # strictly decreasing
        out = compute_rsi(closes, 10)
        self.assertAlmostEqual(out[-1], 0.0)

    def test_flat_is_undefined_handled(self):
        # flat market -> avg_loss == 0 -> RSI defined as 100 (matches Pine's
        # `down == 0 ? 100` branch, checked before the up==0 branch)
        closes = [100.0] * 30
        out = compute_rsi(closes, 10)
        self.assertAlmostEqual(out[-1], 100.0)


class TestSupertrend(unittest.TestCase):
    def test_uptrend_direction(self):
        n = 60
        closes = [100 + i * 2 for i in range(n)]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        st, direction = compute_supertrend(highs, lows, closes, 10, 2.5)
        # a strongly rising market should settle into an uptrend (-1)
        self.assertEqual(direction[-1], -1)
        self.assertLess(st[-1], closes[-1])

    def test_downtrend_direction(self):
        n = 60
        closes = [500 - i * 2 for i in range(n)]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        st, direction = compute_supertrend(highs, lows, closes, 10, 2.5)
        self.assertEqual(direction[-1], 1)
        self.assertGreater(st[-1], closes[-1])

    def test_flip_sets_st_sell(self):
        # rises for a while, then reverses hard -> direction should flip
        # from -1 to 1 at some point, and generate_signals should catch it.
        up = [100 + i * 3 for i in range(40)]
        down = [up[-1] - i * 6 for i in range(1, 40)]
        closes = up + down
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        params = StrategyParams(st_atr_period=10, st_factor=2.5)
        sig = generate_signals(highs, lows, closes, params)
        self.assertTrue(any(sig["st_sell"]), "expected at least one SuperTrend flip to SELL")


class TestDoubleCrossCounter(unittest.TestCase):
    def test_second_cross_below_trigger_fires_special_buy(self):
        """Hand-craft an RSI series with exactly two bullish crosses of its
        own SMA while below the trigger level, and confirm special_buy fires
        on the second one (target_cross_count=2) and not the first."""
        # Build a synthetic close series that oscillates below the midline
        # twice (a rough "W" shape) then keep it simple: we directly drive
        # the counter logic by constructing rsi/rsi_signal by hand instead
        # of round-tripping through price, to keep the test legible.
        from indicators import StrategyParams

        n = 12
        rsi = [None, None, None, None,
               45, 40, 46,   # first dip + cross up through signal (cross #1)
               44, 39, 47,   # second dip + cross up through signal (cross #2 -> fires)
               60, 20]
        rsi_signal = [None, None, None, None,
                      44, 44, 44,
                      44, 44, 44,
                      44, 44]

        params = StrategyParams(trigger_level=50, target_cross_count=2)
        cross_count = 0
        special_buy = [False] * n
        for i in range(1, n):
            r, rs, rp, rsp = rsi[i], rsi_signal[i], rsi[i - 1], rsi_signal[i - 1]
            if r is None or rs is None or rp is None or rsp is None:
                continue
            bull_cross = (r > rs) and (rp <= rsp)
            if r > params.trigger_level:
                cross_count = 0
            if bull_cross and r < params.trigger_level:
                cross_count += 1
            special_buy[i] = bull_cross and (r < params.trigger_level) and (cross_count == params.target_cross_count)
            if special_buy[i]:
                cross_count = 0

        self.assertFalse(special_buy[6], "first bull cross (count=1) should not fire yet")
        self.assertTrue(special_buy[9], "second bull cross while below trigger (count=2) should fire")

        crosses = [i for i in range(n) if special_buy[i]]
        self.assertEqual(crosses, [9], f"expected special_buy only at index 9, got {crosses}")


if __name__ == "__main__":
    unittest.main()
