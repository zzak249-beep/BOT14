"""
indicators.py

Pure-Python (no pandas/numpy) port of the Pine Script v6 strategy:
"ProBorsa: RSI & SuperTrend Ozel Dip Stratejisi"

Everything here is a deterministic function of a list of OHLC candles, so it
can be unit tested and re-run safely on every polling cycle.

Signal logic (mirrors the Pine source 1:1):
  1. RSI(rsi_length) computed with Wilder smoothing (ta.rma).
  2. rsi_signal = SMA(rsi, rsi_signal_length).
  3. bull_cross = rsi crosses over rsi_signal.
  4. A running counter increments every bull_cross that happens while
     rsi < trigger_level. The counter resets to 0 whenever rsi rises above
     trigger_level. When the counter reaches target_cross_count, a
     "special_buy" (double-dip / W) signal fires and the counter resets.
  5. SuperTrend(st_atr_period, st_factor) is used purely as the exit:
     st_sell fires the bar the trend direction flips from up (-1) to down (1).

direction convention (matches Pine's ta.supertrend): -1 = uptrend, 1 = downtrend.
"""

from dataclasses import dataclass


@dataclass
class StrategyParams:
    rsi_length: int = 10
    rsi_signal_length: int = 10
    trigger_level: float = 50.0
    target_cross_count: int = 2
    st_atr_period: int = 10
    st_factor: float = 2.5


def rma(values, length):
    """Wilder's moving average - matches Pine Script's ta.rma exactly.

    First valid value (index length-1) is a plain SMA of the first `length`
    values; every value after that follows the recursive smoothing formula.
    """
    n = len(values)
    out = [None] * n
    if length <= 0 or n < length:
        return out
    seed = sum(values[:length]) / length
    out[length - 1] = seed
    alpha = 1.0 / length
    for i in range(length, n):
        out[i] = alpha * values[i] + (1 - alpha) * out[i - 1]
    return out


def sma(values, length):
    """Simple moving average, None-safe (propagates None until a full window
    of non-None values is available)."""
    n = len(values)
    out = [None] * n
    if length <= 0:
        return out
    for i in range(n):
        if i < length - 1:
            continue
        window = values[i - length + 1 : i + 1]
        if any(v is None for v in window):
            continue
        out[i] = sum(window) / length
    return out


def compute_rsi(closes, length):
    """RSI using Wilder smoothing, matching the Pine source's exact
    tie-break order: avg_loss==0 -> 100, elif avg_gain==0 -> 0, else formula."""
    n = len(closes)
    changes = [0.0] + [closes[i] - closes[i - 1] for i in range(1, n)]
    gains = [max(c, 0.0) for c in changes]
    losses = [max(-c, 0.0) for c in changes]
    avg_gain = rma(gains, length)
    avg_loss = rma(losses, length)

    rsi = [None] * n
    for i in range(n):
        ag, al = avg_gain[i], avg_loss[i]
        if ag is None or al is None:
            continue
        if al == 0:
            rsi[i] = 100.0
        elif ag == 0:
            rsi[i] = 0.0
        else:
            rsi[i] = 100.0 - (100.0 / (1.0 + ag / al))
    return rsi


def compute_supertrend(highs, lows, closes, period, multiplier):
    """Standard SuperTrend. Returns (supertrend_line, direction) where
    direction is -1 for uptrend, 1 for downtrend (Pine convention)."""
    n = len(closes)
    trs = [None] * n
    for i in range(n):
        if i == 0:
            trs[i] = highs[i] - lows[i]
        else:
            trs[i] = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
    atr = rma(trs, period)

    final_upper = [None] * n
    final_lower = [None] * n
    st = [None] * n
    direction = [None] * n

    for i in range(n):
        if atr[i] is None:
            continue
        hl2 = (highs[i] + lows[i]) / 2.0
        basic_upper = hl2 + multiplier * atr[i]
        basic_lower = hl2 - multiplier * atr[i]

        prev_final_upper = final_upper[i - 1] if i > 0 else None
        prev_final_lower = final_lower[i - 1] if i > 0 else None

        if prev_final_upper is None:
            final_upper[i] = basic_upper
        else:
            final_upper[i] = (
                basic_upper
                if (basic_upper < prev_final_upper or closes[i - 1] > prev_final_upper)
                else prev_final_upper
            )

        if prev_final_lower is None:
            final_lower[i] = basic_lower
        else:
            final_lower[i] = (
                basic_lower
                if (basic_lower > prev_final_lower or closes[i - 1] < prev_final_lower)
                else prev_final_lower
            )

        prev_st = st[i - 1] if i > 0 else None
        if prev_st is None:
            if closes[i] <= final_upper[i]:
                st[i] = final_upper[i]
                direction[i] = 1
            else:
                st[i] = final_lower[i]
                direction[i] = -1
        elif prev_st == prev_final_upper:
            if closes[i] <= final_upper[i]:
                st[i] = final_upper[i]
                direction[i] = 1
            else:
                st[i] = final_lower[i]
                direction[i] = -1
        else:  # prev_st == prev_final_lower
            if closes[i] >= final_lower[i]:
                st[i] = final_lower[i]
                direction[i] = -1
            else:
                st[i] = final_upper[i]
                direction[i] = 1

    return st, direction


def generate_signals(highs, lows, closes, params: StrategyParams):
    """Runs the full bar-by-bar port of the Pine strategy over a candle
    window and returns per-bar series. Only the LAST index needs to be acted
    on live; earlier bars are kept so the running cross-counter state is
    correct by the time it reaches the latest bar."""
    n = len(closes)
    rsi = compute_rsi(closes, params.rsi_length)
    rsi_signal = sma(rsi, params.rsi_signal_length)
    st, direction = compute_supertrend(highs, lows, closes, params.st_atr_period, params.st_factor)

    bull_cross = [False] * n
    special_buy = [False] * n
    st_sell = [False] * n
    cross_count_series = [0] * n

    cross_count = 0
    for i in range(1, n):
        if direction[i] is not None and direction[i - 1] is not None:
            st_sell[i] = (direction[i] - direction[i - 1]) > 0

        r, rs, rp, rsp = rsi[i], rsi_signal[i], rsi[i - 1], rsi_signal[i - 1]
        if r is None or rs is None or rp is None or rsp is None:
            cross_count_series[i] = cross_count
            continue

        bull_cross[i] = (r > rs) and (rp <= rsp)

        if r > params.trigger_level:
            cross_count = 0
        if bull_cross[i] and r < params.trigger_level:
            cross_count += 1

        special_buy[i] = bull_cross[i] and (r < params.trigger_level) and (cross_count == params.target_cross_count)
        if special_buy[i]:
            cross_count = 0

        cross_count_series[i] = cross_count

    return {
        "rsi": rsi,
        "rsi_signal": rsi_signal,
        "supertrend": st,
        "direction": direction,
        "bull_cross": bull_cross,
        "special_buy": special_buy,
        "st_sell": st_sell,
        "cross_count": cross_count_series,
    }
