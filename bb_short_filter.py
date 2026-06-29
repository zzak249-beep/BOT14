"""
bb_short_filter.py — Bollinger Band short-side score.

Price near/at the upper Bollinger Band = resistance zone → SHORT boost.
Price inside the band is neutral.
Price at/below mid band = not a short setup → 0 pts.

Score: 0 – 10 points added to composite score.
"""

import config


def bb_short_score(indicators: dict) -> float:
    """
    Returns 0-10 additional score points for BB position.
    """
    if not config.BB_SHORT_ENABLED:
        return 0.0

    price   = indicators.get("close")
    bb_upper = indicators.get("bb_upper")
    bb_mid  = indicators.get("bb_mid")

    if None in (price, bb_upper, bb_mid) or bb_upper == bb_mid:
        return 0.0

    band_width = bb_upper - bb_mid
    if band_width <= 0:
        return 0.0

    # Position within the upper half of the band
    dist = (price - bb_mid) / band_width   # 0 = at mid, 1 = at upper, >1 = above upper

    if dist >= 1.0:       # above upper band (extreme)
        return 10.0
    if dist >= 0.85:      # very near upper band
        return 8.0
    if dist >= 0.70:      # near upper band
        return 5.0
    if dist >= 0.50:      # upper half
        return 2.0
    return 0.0
