"""
Motor de la estrategia. Traducción literal de reversion_5m.pine.

Si algún día cambias uno, cambia el otro. Un bot que opera algo
distinto de lo que backtesteaste no es un bot: es una sorpresa.

REGLA DE ORO DEL CÁLCULO: solo se usan velas CERRADAS. La última vela
que devuelve el exchange está en curso y sus valores cambian hasta que
cierra; usarla es la forma clásica de que el backtest y el bot no
coincidan.
"""
from __future__ import annotations

from dataclasses import dataclass

import config


@dataclass
class Signal:
    symbol: str
    side: str          # "BUY" (largo) | "SELL" (corto)
    entry: float
    sl: float
    tp: float
    rr: float
    atr_pct: float
    stretch: float
    cost_cover: float  # cuántas veces cubre el ATR el coste de ida y vuelta


def ema(values: list[float], length: int) -> list[float]:
    if not values:
        return []
    k = 2.0 / (length + 1.0)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1.0 - k))
    return out


def atr(highs: list[float], lows: list[float], closes: list[float], length: int) -> list[float]:
    """ATR de Wilder, igual que ta.atr de Pine."""
    if len(closes) < 2:
        return []
    trs = [highs[0] - lows[0]]
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(tr)
    out = [trs[0]]
    for tr in trs[1:]:
        out.append((out[-1] * (length - 1) + tr) / length)
    return out


def evaluate(symbol: str, candles: list[dict]) -> tuple[Signal | None, str]:
    """
    Devuelve (señal, motivo). El motivo explica por qué NO hay señal,
    que en un escáner es más útil que el silencio: sin él, "no pasa
    nada" y "está roto" se parecen demasiado.
    """
    need = max(config.MA_LEN, config.ATR_LEN) + config.MAX_BARS_STRETCH + 5
    if len(candles) < need:
        return None, "pocas velas"

    # Se descarta la última: está en curso.
    c = candles[:-1]
    closes = [x["close"] for x in c]
    highs = [x["high"] for x in c]
    lows = [x["low"] for x in c]
    opens = [x["open"] for x in c]

    ma_series = ema(closes, config.MA_LEN)
    atr_series = atr(highs, lows, closes, config.ATR_LEN)
    if not ma_series or not atr_series:
        return None, "sin indicadores"

    close = closes[-1]
    ma = ma_series[-1]
    a = atr_series[-1]
    if a <= 0 or close <= 0:
        return None, "atr cero"

    atr_pct = a / close * 100.0
    cover = atr_pct / config.COST_ROUNDTRIP_PCT if config.COST_ROUNDTRIP_PCT > 0 else 0.0

    # EL FILTRO QUE MANDA. Va primero a propósito: sin recorrido no hay
    # negocio, por mucho que el patrón sea de libro.
    if atr_pct < config.MIN_ATR_PCT or cover < config.MIN_COST_COVER:
        return None, f"sin amplitud ({atr_pct:.2f}%, {cover:.0f}x)"

    # Ratio de eficiencia de fondo: recorrido neto / suma de movimientos.
    # Alto = línea recta = no es terreno de reversión.
    er_len = min(180, len(closes) - 1)
    if er_len > 20:
        neto = abs(closes[-1] - closes[-1 - er_len])
        total = sum(abs(closes[i] - closes[i - 1]) for i in range(len(closes) - er_len, len(closes)))
        er_long = neto / total if total > 0 else 0.0
        if er_long > config.MAX_ER_LONG:
            return None, f"vertical (ER {er_long:.2f} > {config.MAX_ER_LONG})"

    stretch = (close - ma) / a

    # El estiramiento tiene que ser RÁPIDO: hace N velas aún no lo estaba.
    idx_prev = -1 - config.MAX_BARS_STRETCH
    if abs(idx_prev) > len(closes) or abs(idx_prev) > len(ma_series):
        return None, "historial corto"
    prev_stretch = (closes[idx_prev] - ma_series[idx_prev]) / atr_series[idx_prev]
    was_flat = abs(prev_stretch) < config.STRETCH_ATR * 0.5

    over_up = stretch >= config.STRETCH_ATR and was_flat
    over_dn = stretch <= -config.STRETCH_ATR and was_flat
    if not (over_up or over_dn):
        return None, f"sin estirón ({stretch:+.2f} ATR)"

    # Vela de agotamiento: la primera que empuja en contra.
    exhaust_up = over_up and closes[-1] < opens[-1] and closes[-1] < closes[-2]
    exhaust_dn = over_dn and closes[-1] > opens[-1] and closes[-1] > closes[-2]
    if not (exhaust_up or exhaust_dn):
        return None, "estirado, sin vela de agotamiento"

    if exhaust_up:  # corto contra la subida
        sl = max(highs[-1], highs[-2]) + a * config.SL_ATR
        risk = sl - close
        tp = ma if config.TP_MODE == "MEAN" else close - risk * config.RR_FIXED
        rr = (close - tp) / risk if risk > 0 else 0.0
        side = "SELL"
    else:           # largo contra la caída
        sl = min(lows[-1], lows[-2]) - a * config.SL_ATR
        risk = close - sl
        tp = ma if config.TP_MODE == "MEAN" else close + risk * config.RR_FIXED
        rr = (tp - close) / risk if risk > 0 else 0.0
        side = "BUY"

    if risk <= 0:
        return None, "riesgo no válido"
    if rr < config.MIN_RR:
        return None, f"R:R insuficiente ({rr:.2f})"

    return (
        Signal(
            symbol=symbol,
            side=side,
            entry=close,
            sl=sl,
            tp=tp,
            rr=rr,
            atr_pct=atr_pct,
            stretch=stretch,
            cost_cover=cover,
        ),
        "ok",
    )


def position_size(equity: float, entry: float, sl: float) -> float:
    """Tamaño para arriesgar RISK_PCT del capital si salta el stop."""
    risk_per_unit = abs(entry - sl)
    if risk_per_unit <= 0 or entry <= 0:
        return 0.0
    risk_cash = equity * config.RISK_PCT / 100.0
    return risk_cash / risk_per_unit
