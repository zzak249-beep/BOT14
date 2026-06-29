"""
ema9_vwap_filter.py — EMA9 × VWAP position score.

EMA9 < VWAP → price is trading below VWAP with momentum bearish.
Adds EMA9_VWAP_BOOST (9.0) to score when aligned for SHORT.
Subtracts boost when misaligned (price has strength, counter-trend SHORT).

Score: -(boost) to +(boost) added to composite score.
"""

import config


def ema9_vwap_score(indicators: dict, boost: float = None) -> float:
    """
    Returns +boost if EMA9 < VWAP (SHORT aligned),
            -boost if EMA9 > VWAP (SHORT counter-trend),
            0 if data missing.
    """
    if not config.EMA9_VWAP_ENABLED:
        return 0.0

    b    = boost if boost is not None else config.EMA9_VWAP_BOOST
    ema9 = indicators.get("ema9")
    vwap = indicators.get("vwap")

    if None in (ema9, vwap) or vwap == 0:
        return 0.0

    if ema9 < vwap:
        return b          # bearish: EMA9 below VWAP → SHORT boost
    else:
        return -b         # bullish: EMA9 above VWAP → SHORT penalty
