"""
ibs_filter.py — Internal Bar Strength pullback score.

IBS = (close - low) / (high - low)
High IBS (> 0.7): price closed near the candle high → bearish rejection zone.
Used as a SHORT-entry confirmation: when price rallies into EMA9 with
high IBS, it signals a weak rally that may get rejected.

Score: 0 – 12 points added to composite score.
"""

import config


def ibs_score(indicators: dict) -> float:
    """
    Returns 0-12 additional score points based on IBS.
    High IBS (>0.7) on a pullback = strong SHORT signal.
    """
    if not config.IBS_PULLBACK_ENABLED:
        return 0.0

    ibs = indicators.get("ibs", 0.5)
    if ibs is None:
        return 0.0

    if ibs >= 0.85:
        return 12.0
    if ibs >= 0.75:
        return 8.0
    if ibs >= 0.65:
        return 4.0
    if ibs >= 0.55:
        return 1.0
    return 0.0
