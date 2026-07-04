"""
Regime Filter — Choppiness Index
===================================
El Unicorn Model genera breakers falsos constantemente en mercados en
rango (chop). Este filtro calcula el Choppiness Index sobre el TF de bias
y bloquea nuevas señales cuando el mercado está claramente "picado" /
sin dirección clara — sin tocar la lógica de sweep/breaker en sí.

CHOP > 61.8  → mercado en rango (ranging / choppy)
CHOP < 38.2  → mercado en tendencia clara
Entre medio  → zona ambigua, se permite operar (no se filtra)
"""
import math
import logging

log = logging.getLogger("regime_filter")


def _true_range(candles):
    trs = []
    for i in range(1, len(candles)):
        c, cp = candles[i], candles[i - 1]
        trs.append(max(
            c["high"] - c["low"],
            abs(c["high"] - cp["close"]),
            abs(c["low"] - cp["close"]),
        ))
    return trs


def choppiness_index(candles, length=14):
    """Devuelve el CHOP (0-100) sobre las últimas `length` velas, o None si no hay datos."""
    if len(candles) < length + 1:
        return None
    window = candles[-(length + 1):]
    trs = _true_range(window)
    if not trs or len(trs) < length:
        return None
    tr_sum = sum(trs[-length:])
    highest = max(c["high"] for c in window[-length:])
    lowest = min(c["low"] for c in window[-length:])
    rng = highest - lowest
    if rng <= 0 or tr_sum <= 0:
        return None
    chop = 100 * math.log10(tr_sum / rng) / math.log10(length)
    return chop


def is_trending_regime(candles, config):
    """
    Devuelve {"trending": bool|None, "chop": float|None, "reason": str}
    trending=True → se permite operar; False → régimen de rango, se bloquea
    """
    length = getattr(config, "REGIME_CHOP_LENGTH", 14)
    threshold = getattr(config, "REGIME_CHOP_THRESHOLD", 61.8)

    chop = choppiness_index(candles, length)
    if chop is None:
        return {"trending": None, "chop": None, "reason": "datos_insuficientes"}

    trending = chop < threshold
    reason = (f"CHOP={chop:.1f} < {threshold} (tendencia)" if trending
               else f"CHOP={chop:.1f} >= {threshold} (mercado en rango, se bloquea)")
    return {"trending": trending, "chop": chop, "reason": reason}
