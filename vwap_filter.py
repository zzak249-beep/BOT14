"""
VWAP Filter — precio ponderado por volumen, reinicia por sesión (día UTC)
=============================================================================
Mismo motivo que se agregó al superscript: mira DÓNDE se movió realmente el
dinero, no solo dónde cerró la vela. Complementa al Supertrend/EMA, que solo
miran precio.

Reinicia el cálculo en cada nuevo día UTC, igual que `ta.vwap()` en Pine por
defecto (ancla de sesión).
"""
import logging
from datetime import datetime, timezone

log = logging.getLogger("vwap_filter")


def compute_vwap(candles):
    """
    candles: velas OHLCV con "time" (epoch ms), orden ascendente. Se asume
             que la última puede estar en formación.
    Devuelve el VWAP acumulado hasta la última vela CERRADA, o None si no
    hay datos.
    """
    closed = candles[:-1] if len(candles) > 1 else candles
    if not closed:
        return None

    cum_pv = 0.0
    cum_vol = 0.0
    last_day = None

    for c in closed:
        day = datetime.fromtimestamp(c["time"] / 1000, tz=timezone.utc).date()
        if day != last_day:
            cum_pv = 0.0
            cum_vol = 0.0
            last_day = day
        typical = (c["high"] + c["low"] + c["close"]) / 3
        cum_pv += typical * c["volume"]
        cum_vol += c["volume"]

    if cum_vol <= 0:
        return None
    return cum_pv / cum_vol


def confirms_direction(candles, direction, config):
    """
    direction: "LONG" o "SHORT". LONG confirma con precio arriba del VWAP,
    SHORT con precio debajo — mismo criterio que el superscript.
    Devuelve {"confirms": bool|None, "vwap": float|None, "reason": str}
    """
    if not candles:
        return {"confirms": None, "vwap": None, "reason": "sin_velas"}

    vwap_val = compute_vwap(candles)
    if vwap_val is None:
        return {"confirms": None, "vwap": None, "reason": "velas_insuficientes"}

    last_close = candles[-2]["close"] if len(candles) > 1 else candles[-1]["close"]

    if direction == "LONG":
        confirms = last_close > vwap_val
        reason = f"precio {last_close:.6f} > VWAP {vwap_val:.6f}" if confirms else f"precio {last_close:.6f} <= VWAP {vwap_val:.6f}"
    else:
        confirms = last_close < vwap_val
        reason = f"precio {last_close:.6f} < VWAP {vwap_val:.6f}" if confirms else f"precio {last_close:.6f} >= VWAP {vwap_val:.6f}"

    return {"confirms": confirms, "vwap": vwap_val, "reason": reason}
