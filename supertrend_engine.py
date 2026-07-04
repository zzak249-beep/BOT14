"""
Custom Supertrend (BigBeluga) — Filtro de bias macro
======================================================
Réplica del Supertrend usado en "Volume-Trend Order Block Engine [BigBeluga]".
Usa SMA(high-low) en vez de ATR de Wilder para el rango de volatilidad.
Se usa como filtro de dirección permitida: el Unicorn Model solo puede
operar en la dirección que marca este Supertrend en el TF de bias (ej. 1H).
"""
import logging

log = logging.getLogger("supertrend_engine")


def _range_sma(candles, length):
    if len(candles) < length:
        return 0.0
    window = candles[-length:]
    return sum(c["high"] - c["low"] for c in window) / length


def get_trend(candles, st_len=50, st_mult=3.5):
    """
    Calcula la tendencia del custom Supertrend sobre una serie de velas.
    Devuelve {"trend": 1|-1|0, "trend_stop": float|None, "changed": bool}
    trend=1 → alcista (LONG permitido), trend=-1 → bajista (SHORT permitido)
    """
    if len(candles) < st_len + 5:
        return {"trend": 0, "trend_stop": None, "changed": False}

    trend = 1
    trend_stop = None
    changed = False

    # Warm-up: arrancamos el cálculo desde donde ya hay ventana suficiente
    start_idx = st_len
    for i in range(start_idx, len(candles)):
        window = candles[max(0, i - st_len):i]
        rng = _range_sma(window, min(st_len, len(window)))
        c = candles[i]
        hl2 = (c["high"] + c["low"]) / 2

        upper = hl2 + st_mult * rng
        lower = hl2 - st_mult * rng

        if trend_stop is None:
            trend_stop = lower if trend == 1 else upper

        prev_stop = trend_stop
        trend_stop = max(lower, prev_stop) if trend == 1 else min(upper, prev_stop)

        prev_close = candles[i - 1]["close"]
        prev_trend = trend
        if c["close"] > prev_stop and prev_close <= prev_stop:
            trend = 1
            trend_stop = lower
        elif c["close"] < prev_stop and prev_close >= prev_stop:
            trend = -1
            trend_stop = upper

        changed = trend != prev_trend

    return {"trend": trend, "trend_stop": trend_stop, "changed": changed}
