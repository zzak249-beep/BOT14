"""
CVD Filter — Cumulative Volume Delta real (intrabar), sin llamada extra a la API
====================================================================================
Inspirado en la sección "CVD REAL" de tu script "QFxJP Confluence Gate [BB OB x
AMD x CVD real]" de TradingView, que usa `request.security_lower_tf` para mirar
volumen 1m dentro de cada vela y sumarlo con signo (comprador/vendedor).

Réplica del CONCEPTO, no una traducción 1:1: en vez de pedir velas 1m aparte
(una llamada de klines más por símbolo — justo lo que acabamos de reducir por
el rate limit 100410), reusamos `candles_entry` (ENTRY_TF, ej. 3m) como el
"timeframe fino" respecto al timeframe de la señal a confirmar (Unicorn Model
en ENTRY_TF mismo, u Order Block Engine en OB_TF, típicamente 5x más ancho).
Es más grueso que 1m real, pero bastante más fino que clasificar la vela
entera de 15m como 100% compradora o vendedora (que es lo que hace
`_pivot_volume_ratio` en order_block_engine.py) — y no cuesta ninguna llamada
adicional.

Mismo criterio de señal que el Pine original: CVD > 0 confirma LONG, CVD < 0
confirma SHORT. Sin umbral de magnitud (igual que el script fuente).
"""
import logging

log = logging.getLogger("cvd_filter")


def compute_cvd(candles, lookback):
    """
    candles: velas del timeframe FINO (ej. candles_entry / ENTRY_TF), orden
             ascendente. Se asume que la última puede estar en formación
             (mismo criterio que el resto del bot) — se excluye del cálculo.
    lookback: cuántas velas finas cerradas mirar hacia atrás.

    Devuelve {"cvd": float, "bullish": bool, "bearish": bool, "bars_used": int}
    """
    if not candles or len(candles) < 2:
        return {"cvd": 0.0, "bullish": False, "bearish": False, "bars_used": 0}

    closed = candles[:-1]  # última vela = potencialmente en formación
    window = closed[-lookback:] if lookback > 0 else closed

    cvd = sum(c["volume"] if c["close"] >= c["open"] else -c["volume"] for c in window)
    return {
        "cvd": cvd,
        "bullish": cvd > 0,
        "bearish": cvd < 0,
        "bars_used": len(window),
    }


def confirms_direction(candles, direction, config):
    """
    direction: "LONG" o "SHORT" (la dirección ya propuesta por Unicorn Model
               o el Order Block Engine).
    Devuelve {"confirms": bool, "cvd": float, "reason": str}
    """
    lookback = getattr(config, "CVD_LOOKBACK", 20)
    result = compute_cvd(candles, lookback)

    if result["bars_used"] == 0:
        return {"confirms": None, "cvd": 0.0, "reason": "sin_velas_suficientes"}

    if direction == "LONG":
        confirms = result["bullish"]
        reason = (f"CVD={result['cvd']:.2f} > 0 (confirma LONG)" if confirms
                   else f"CVD={result['cvd']:.2f} <= 0 (no confirma LONG)")
    else:
        confirms = result["bearish"]
        reason = (f"CVD={result['cvd']:.2f} < 0 (confirma SHORT)" if confirms
                   else f"CVD={result['cvd']:.2f} >= 0 (no confirma SHORT)")

    return {"confirms": confirms, "cvd": result["cvd"], "reason": reason}
