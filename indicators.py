"""
indicators.py

Signal engine v2 - replaces the RSI-crossover counter with genuine
price-structure double-bottom detection confirmed by RSI bullish divergence.

Why this changed from v1:
  The original Pine script's "Ikili Dip" (double dip) signal counted how many
  times RSI crossed above its own moving average while under 50. That is a
  momentum-oscillator heuristic, not an actual double-bottom pattern - it has
  no requirement that PRICE forms two similar lows, so it can (and did, per
  the live chart) fire while price is still making a fresh lower low. The
  script's own divergence section already computes real bullish divergence
  (pivot lows + comparing price/RSI at them) but was wired up as a purely
  visual, optional overlay - never connected to strategy.entry.

v2 detects an actual "W" in price:
  1. Find confirmed pivot lows in PRICE (ta.pivotlow-equivalent).
  2. Take the two most recent pivot lows (L1 older, L2 newer). Validate:
       - the two lows are within `max_bottom_diff_pct` of each other
       - they are `min/max_bars_between_lows` bars apart
       - the peak between them (the "neckline") rises at least
         `min_neckline_bounce_pct` above the lows
       - RSI at L2 > RSI at L1 (bullish divergence: price holds/matches the
         prior low while momentum improves) - the "real" version of what the
         original crossover counter was trying to approximate
       - RSI at L2 is still below `trigger_level` (keeps the "buying a dip"
         character of the original strategy)
  3. Only THEN arm a pending setup, and only fire the buy signal once price
     actually closes back above the neckline - i.e. wait for proof of
     reversal instead of buying into an unconfirmed dip. This is the direct
     fix for the "catches the falling knife" entries visible on the live
     chart.

v2.1 adds an optional higher-timeframe trend gate (evaluate_htf_trend):
  skips the entry when a higher-timeframe EMA is dropping too fast, the
  same multi-timeframe-confirmation idea already used elsewhere. This does
  not change what counts as a double bottom - it only decides whether the
  bot is allowed to act on one.

SuperTrend exit logic is unchanged from v1.
"""

from dataclasses import dataclass


@dataclass
class StrategyParams:
    rsi_length: int = 10
    trigger_level: float = 50.0          # RSI at the 2nd low must be below this

    pivot_left: int = 5
    pivot_right: int = 5                 # confirmation lag: a pivot is only knowable `right` bars later
    max_bottom_diff_pct: float = 2.0     # how close the two lows must be, in %
    min_bars_between_lows: int = 3
    max_bars_between_lows: int = 50
    min_neckline_bounce_pct: float = 1.0
    require_rsi_divergence: bool = True
    max_wait_bars: int = 20              # bars to wait for the neckline break before the setup expires

    st_atr_period: int = 10
    st_factor: float = 2.5


def rma(values, length):
    """Wilder's moving average - matches Pine Script's ta.rma exactly."""
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


def ema(values, length):
    """Classic EMA (seeded with a plain SMA of the first `length` values,
    same seeding convention as rma() above)."""
    n = len(values)
    out = [None] * n
    if length <= 0 or n < length:
        return out
    seed = sum(values[:length]) / length
    out[length - 1] = seed
    alpha = 2.0 / (length + 1)
    for i in range(length, n):
        out[i] = alpha * values[i] + (1 - alpha) * out[i - 1]
    return out


def evaluate_htf_trend(closes, ema_length, slope_lookback, max_down_slope_pct):
    """Higher-timeframe trend gate for entries. Compares the HTF EMA now
    against `slope_lookback` bars ago; if it has dropped more than
    `max_down_slope_pct` (%), the trend is considered too weak for a new
    long. Fails CLOSED on insufficient data - i.e. trend_ok=False rather
    than assuming it's fine, since skipping one trade is cheaper than
    entering blind into an unconfirmed regime."""
    n = len(closes)
    e = ema(closes, ema_length)
    idx_now = n - 1
    idx_then = idx_now - slope_lookback
    if idx_then < 0 or idx_now < 0 or e[idx_now] is None or e[idx_then] is None or e[idx_then] == 0:
        return {"trend_ok": False, "ema": None, "slope_pct": None}
    slope_pct = (e[idx_now] - e[idx_then]) / abs(e[idx_then]) * 100.0
    trend_ok = slope_pct >= -abs(max_down_slope_pct)
    return {"trend_ok": trend_ok, "ema": e[idx_now], "slope_pct": slope_pct}


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


def find_pivot_low_at(lows, idx, left, right):
    """True if lows[idx] is the minimum over the centered window
    [idx-left, idx+right]. Only meaningful once `right` bars have passed
    idx - callers must not query this before that (see detection loop)."""
    if idx - left < 0 or idx + right >= len(lows):
        return False
    window = lows[idx - left: idx + right + 1]
    return lows[idx] <= min(window)


def validate_double_bottom_pair(l1_price, l2_price, l1_rsi, l2_rsi, bars_between, neckline, params: StrategyParams):
    """Pure validation of a candidate (L1, L2) pivot-low pair against the
    double-bottom + divergence rules. Split out from generate_signals so it
    can be unit tested directly with hand-picked numbers."""
    if not l1_price or l2_price is None or neckline is None:
        return False
    diff_pct = abs(l2_price - l1_price) / l1_price * 100.0
    lo_level = min(l1_price, l2_price)
    if not lo_level:
        return False
    bounce_pct = (neckline - lo_level) / lo_level * 100.0

    divergence_ok = (not params.require_rsi_divergence) or (l1_rsi is not None and l2_rsi is not None and l2_rsi > l1_rsi)
    rsi_zone_ok = l2_rsi is not None and l2_rsi < params.trigger_level

    return (
        params.min_bars_between_lows <= bars_between <= params.max_bars_between_lows
        and diff_pct <= params.max_bottom_diff_pct
        and bounce_pct >= params.min_neckline_bounce_pct
        and divergence_ok
        and rsi_zone_ok
    )


def generate_signals(highs, lows, closes, params: StrategyParams):
    """Runs the double-bottom + divergence detector and the SuperTrend exit
    over a candle window. Only the LAST index needs to be acted on live;
    earlier bars are kept so pivot/setup state is correct by the time it
    reaches the latest bar. Output keys are stable across versions:
    rsi, supertrend, direction, special_buy, st_sell - plus new
    setup_l1_price / setup_l2_price / setup_neckline / setup_l1_rsi /
    setup_l2_rsi for richer alerts."""
    n = len(closes)
    rsi = compute_rsi(closes, params.rsi_length)
    st, direction = compute_supertrend(highs, lows, closes, params.st_atr_period, params.st_factor)

    special_buy = [False] * n
    st_sell = [False] * n
    setup_l1_price = [None] * n
    setup_l2_price = [None] * n
    setup_neckline = [None] * n
    setup_l1_rsi = [None] * n
    setup_l2_rsi = [None] * n

    recent_pivots = []   # up to the last two confirmed pivot-low indices, oldest first
    active_setup = None  # dict: l1_price, l2_price, neckline, l1_rsi, l2_rsi, confirmed_at

    for i in range(1, n):
        if direction[i] is not None and direction[i - 1] is not None:
            st_sell[i] = (direction[i] - direction[i - 1]) > 0

        confirm_idx = i - params.pivot_right
        if confirm_idx - params.pivot_left >= 0 and find_pivot_low_at(lows, confirm_idx, params.pivot_left, params.pivot_right):
            recent_pivots.append(confirm_idx)
            recent_pivots = recent_pivots[-2:]
            active_setup = None  # a fresh swing low always resets a pending pattern

            if len(recent_pivots) == 2:
                l1_idx, l2_idx = recent_pivots
                l1_price, l2_price = lows[l1_idx], lows[l2_idx]
                bars_between = l2_idx - l1_idx
                neckline = max(highs[l1_idx:l2_idx + 1])
                r1, r2 = rsi[l1_idx], rsi[l2_idx]

                if validate_double_bottom_pair(l1_price, l2_price, r1, r2, bars_between, neckline, params):
                    active_setup = {
                        "l1_price": l1_price,
                        "l2_price": l2_price,
                        "neckline": neckline,
                        "l1_rsi": r1,
                        "l2_rsi": r2,
                        "confirmed_at": i,
                    }

        if active_setup is not None:
            setup_l1_price[i] = active_setup["l1_price"]
            setup_l2_price[i] = active_setup["l2_price"]
            setup_neckline[i] = active_setup["neckline"]
            setup_l1_rsi[i] = active_setup["l1_rsi"]
            setup_l2_rsi[i] = active_setup["l2_rsi"]

            invalidation_price = active_setup["l2_price"] * (1 - params.max_bottom_diff_pct / 100.0)
            waited = i - active_setup["confirmed_at"]

            if closes[i] > active_setup["neckline"]:
                special_buy[i] = True
                active_setup = None
            elif lows[i] < invalidation_price or waited > params.max_wait_bars:
                active_setup = None

    return {
        "rsi": rsi,
        "supertrend": st,
        "direction": direction,
        "special_buy": special_buy,
        "st_sell": st_sell,
        "setup_l1_price": setup_l1_price,
        "setup_l2_price": setup_l2_price,
        "setup_neckline": setup_neckline,
        "setup_l1_rsi": setup_l1_rsi,
        "setup_l2_rsi": setup_l2_rsi,
    }
