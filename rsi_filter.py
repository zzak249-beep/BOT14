"""
RSI Filter — momento actual, no una foto congelada del pasado
==================================================================
Mismo motivo que se agregó al superscript de TradingView: el % de volumen
del Order Block Engine es una foto de CUANDO se formó el pivote — el RSI
se recalcula en cada vela, así que evalúa el momento ACTUAL, complementario
a todo lo demás (que mira velas cerradas o eventos pasados).

Implementa el RSI de Wilder (suavizado exponencial tras la semilla inicial),
igual fórmula que usa `ta.rsi()` en Pine — no el promedio simple, para que
los valores coincidan con lo que ves en el superscript.
"""
import logging

log = logging.getLogger("rsi_filter")


def compute_rsi(candles, length=14):
    """
    candles: velas OHLC, orden ascendente. Se asume que la última puede
             estar en formación (mismo criterio que el resto del bot).
    Devuelve el valor de RSI (0-100) sobre la última vela CERRADA, o None
    si no hay velas suficientes.
    """
    closed = candles[:-1] if len(candles) > 1 else candles
    if len(closed) < length + 1:
        return None

    closes = [c["close"] for c in closed]
    changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(c, 0.0) for c in changes]
    losses = [max(-c, 0.0) for c in changes]

    avg_gain = sum(gains[:length]) / length
    avg_loss = sum(losses[:length]) / length

    # Suavizado de Wilder para el resto de la serie (no un promedio simple)
    for i in range(length, len(gains)):
        avg_gain = (avg_gain * (length - 1) + gains[i]) / length
        avg_loss = (avg_loss * (length - 1) + losses[i]) / length

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def confirms_direction(candles, direction, config):
    """
    direction: "LONG" o "SHORT". LONG confirma con RSI > 50 (momentum
    alcista ahora), SHORT con RSI < 50 — mismo criterio que el superscript.
    Devuelve {"confirms": bool|None, "rsi": float|None, "reason": str}
    """
    length = getattr(config, "RSI_LENGTH", 14)
    rsi_val = compute_rsi(candles, length)

    if rsi_val is None:
        return {"confirms": None, "rsi": None, "reason": "velas_insuficientes"}

    if direction == "LONG":
        confirms = rsi_val > 50
        reason = f"RSI={rsi_val:.1f} > 50 (momentum alcista)" if confirms else f"RSI={rsi_val:.1f} <= 50 (sin momentum alcista)"
    else:
        confirms = rsi_val < 50
        reason = f"RSI={rsi_val:.1f} < 50 (momentum bajista)" if confirms else f"RSI={rsi_val:.1f} >= 50 (sin momentum bajista)"

    return {"confirms": confirms, "rsi": rsi_val, "reason": reason}
