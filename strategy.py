"""
Motor: descomposición multiescala causal (tipo à trous) + cruce sobre la
aproximación, con el filtro de régimen normalizado por escala.

═══════════════════════════════════════════════════════════════════════
EL FALLO DEL ORIGINAL (sigue siendo cierto y sigue corregido)
═══════════════════════════════════════════════════════════════════════
Comparar la energía de las escalas gruesas contra las finas SIN
normalizar da mediana 3.04 en ruido puro y supera 1.5 el 92.6% del
tiempo: el filtro se enciende casi siempre. La corrección es dividir la
energía de cada escala por su longitud; entonces el ruido puro da 0.75 y
un umbral de 1.30 significa algo.

═══════════════════════════════════════════════════════════════════════
LO QUE SE HA REPARADO AHORA: EL FILTRO DE AMPLITUD
═══════════════════════════════════════════════════════════════════════
Había dos puertas midiendo lo mismo con números incompatibles:

    MIN_COST_COVER=6 con coste 0.25%  ->  exigía ATR >= 1.50%
    MAX_COST_IN_R=0.20 con SL 1.5 ATR ->  exigía ATR >= 0.83%

En velas de 5 minutos un ATR del 1.5% prácticamente no ocurre: 316 de
326 símbolos morían ahí cada ciclo. Ahora el umbral efectivo se calcula
en config.atr_minimo_efectivo() y el motivo del descarte dice el número
concreto, no "sin amplitud" a secas.

Nada de esto cambia la estrategia: cambia que pueda ejecutarse.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

import config

log = logging.getLogger("strategy")

ESCALAS_FINAS = (1, 2)
ESCALAS_GRUESAS = (4, 8)


@dataclass
class Signal:
    symbol: str
    side: str            # "BUY" | "SELL"
    entry: float
    sl: float
    tp: float
    ratio: float
    umbral: float
    h8: float
    atr_pct: float
    riesgo_pct: float
    coste_r: float
    timeframe: str = ""
    btc_24h: float | None = None
    funding: float | None = None


@dataclass
class Analisis:
    """Todo lo que se sabe de un símbolo tras UNA lectura de velas.

    Existe para no descargar el histórico dos veces (una para buscar
    señal y otra para la lista de vigilancia) y para poder medir el
    embudo: sin esto, un filtro mal calibrado es invisible.
    """
    symbol: str
    motivo: str = "sin datos"
    signal: Signal | None = None
    atr_pct: float = 0.0
    ratio: float = 0.0
    h8: float = 0.0
    dist_aprox: float = 0.0
    dominante: bool = False
    timeframe: str = ""


def sma(values: list[float], length: int) -> list[float]:
    out, acc = [], 0.0
    for i, v in enumerate(values):
        acc += v
        if i >= length:
            acc -= values[i - length]
        out.append(acc / min(i + 1, length))
    return out


def atr_series(highs, lows, closes, length: int) -> list[float]:
    if len(closes) < 2:
        return []
    trs = [highs[0] - lows[0]]
    for i in range(1, len(closes)):
        trs.append(max(highs[i] - lows[i],
                       abs(highs[i] - closes[i - 1]),
                       abs(lows[i] - closes[i - 1])))
    out = [trs[0]]
    for tr in trs[1:]:
        out.append((out[-1] * (length - 1) + tr) / length)
    return out


def haar_detail(closes: list[float], n: int) -> list[float]:
    """Detalle causal a escala n: solo mira hacia atrás, sin repintado."""
    m = sma(closes, n)
    out = []
    for i in range(len(closes)):
        j = i - n
        out.append(0.0 if j < 0 else (m[i] - m[j]) / (2 ** 0.5))
    return out


def regime(closes: list[float], lookback: int, normalizar: bool) -> tuple[float, float]:
    detalles = {n: haar_detail(closes, n) for n in (1, 2, 4, 8)}
    energia = {}
    for n, serie in detalles.items():
        ventana = serie[-lookback:]
        e = sum(x * x for x in ventana)
        energia[n] = e / n if normalizar else e
    fino = sum(energia[n] for n in ESCALAS_FINAS)
    grueso = sum(energia[n] for n in ESCALAS_GRUESAS)
    ratio = grueso / fino if fino > 0 else 0.0
    return ratio, detalles[8][-1]


def analizar(symbol: str, candles: list[dict], timeframe: str = "") -> Analisis:
    a_out = Analisis(symbol=symbol, timeframe=timeframe or config.TIMEFRAME)

    need = max(config.LOOKBACK_ENERGY + 32,
               (config.HTF_MA_LEN + 20) if config.USE_HTF_FILTER else 0)
    if len(candles) < need:
        a_out.motivo = "pocas velas"
        return a_out

    c = candles[:-1]  # solo velas cerradas: la última aún se mueve
    closes = [x["close"] for x in c]
    highs = [x["high"] for x in c]
    lows = [x["low"] for x in c]

    a = atr_series(highs, lows, closes, config.ATR_LEN)
    if not a or a[-1] <= 0 or closes[-1] <= 0:
        a_out.motivo = "sin indicadores"
        return a_out

    atr_pct = a[-1] / closes[-1] * 100.0
    a_out.atr_pct = atr_pct

    ratio, h8 = regime(closes, config.LOOKBACK_ENERGY, config.NORMALIZE_SCALES)
    aprox = sma(closes, config.APPROX_LEN)
    a_out.ratio = ratio
    a_out.h8 = h8
    a_out.dominante = ratio >= config.DOMINANCE_THRESHOLD
    a_out.dist_aprox = (closes[-1] - aprox[-1]) / a[-1] if a[-1] > 0 else 0.0

    # ── amplitud: UN solo criterio, con el número a la vista ──────────
    minimo = config.atr_minimo_efectivo()
    if atr_pct < minimo:
        a_out.motivo = f"sin amplitud ({atr_pct:.2f}% < {minimo:.2f}% mínimo)"
        return a_out

    if not a_out.dominante:
        a_out.motivo = f"sin dominancia ({ratio:.2f} de {config.DOMINANCE_THRESHOLD})"
        return a_out

    cruza_arriba = closes[-1] > aprox[-1] and closes[-2] <= aprox[-2]
    cruza_abajo = closes[-1] < aprox[-1] and closes[-2] >= aprox[-2]

    largo = cruza_arriba and h8 > 0 and config.ALLOW_LONG
    corto = cruza_abajo and h8 < 0 and config.ALLOW_SHORT

    if not (largo or corto):
        a_out.motivo = ("cruce contra la escala gruesa" if (cruza_arriba or cruza_abajo)
                        else f"sin cruce (ratio {ratio:.2f})")
        return a_out

    # Tendencia de fondo sobre el mismo feed: SMA(200) en 5m son ~16 h.
    if config.USE_HTF_FILTER and len(closes) > config.HTF_MA_LEN:
        ma_larga = sma(closes, config.HTF_MA_LEN)[-1]
        if largo and closes[-1] < ma_larga:
            a_out.motivo = "largo por debajo de la media larga"
            return a_out
        if corto and closes[-1] > ma_larga:
            a_out.motivo = "corto por encima de la media larga"
            return a_out

    if config.USE_VOL_FILTER:
        vols = [x["volume"] for x in c]
        vsma = sma(vols, config.VOL_LEN)
        if vsma[-1] <= 0 or vols[-1] < vsma[-1] * config.VOL_MULT:
            a_out.motivo = "sin volumen"
            return a_out

    entrada = closes[-1]
    if largo:
        sl = entrada - a[-1] * config.SL_ATR
        tp = entrada + a[-1] * config.TP_ATR
        side = "BUY"
    else:
        sl = entrada + a[-1] * config.SL_ATR
        tp = entrada - a[-1] * config.TP_ATR
        side = "SELL"

    riesgo = abs(entrada - sl)
    if riesgo <= 0:
        a_out.motivo = "riesgo no válido"
        return a_out
    riesgo_pct = riesgo / entrada * 100.0
    coste_r = config.COST_ROUNDTRIP_PCT / riesgo_pct if riesgo_pct > 0 else 99.0

    if coste_r > config.MAX_COST_IN_R:
        a_out.motivo = f"stop demasiado cerca (coste {coste_r:.2f}R)"
        return a_out
    if riesgo_pct > config.MAX_RISK_PCT:
        a_out.motivo = f"stop demasiado lejos ({riesgo_pct:.1f}%)"
        return a_out

    a_out.signal = Signal(
        symbol=symbol, side=side, entry=entrada, sl=sl, tp=tp,
        ratio=ratio, umbral=config.DOMINANCE_THRESHOLD, h8=h8,
        atr_pct=atr_pct, riesgo_pct=riesgo_pct, coste_r=coste_r,
        timeframe=a_out.timeframe,
    )
    a_out.motivo = "ok"
    return a_out


def evaluate(symbol: str, candles: list[dict]) -> tuple[Signal | None, str]:
    """Interfaz que usan backtest.py y sweep.py. Misma lógica, dos valores."""
    a = analizar(symbol, candles)
    return a.signal, a.motivo


def trailing_stop(candles: list[dict], side: str, stop_actual: float) -> float:
    """Stop que solo se mueve a favor. Disponible y apagado: es una
    hipótesis que hay que medir en el backtester, no una certeza."""
    c = candles[:-1]
    a = atr_series([x["high"] for x in c], [x["low"] for x in c],
                   [x["close"] for x in c], config.ATR_LEN)
    if not a:
        return stop_actual
    if side == "BUY":
        cand = max(x["high"] for x in c[-3:]) - a[-1] * config.TRAIL_ATR
        return max(stop_actual, cand)
    cand = min(x["low"] for x in c[-3:]) + a[-1] * config.TRAIL_ATR
    return min(stop_actual, cand)


def position_size(equity: float, entry: float, sl: float) -> float:
    """
    Riesgo fijo por operación. Kelly se queda fuera a propósito: necesita
    el edge REAL y estimarlo con las primeras decenas de operaciones
    produce tamaños disparatados justo cuando menos se sabe.
    """
    riesgo = abs(entry - sl)
    if riesgo <= 0 or entry <= 0 or equity <= 0:
        return 0.0
    return (equity * config.RISK_PCT / 100.0) / riesgo


def watch_status(candles: list[dict]) -> dict | None:
    """Compatibilidad: el estado del régimen para la lista de vigilancia."""
    a = analizar("?", candles)
    if a.atr_pct <= 0:
        return None
    return {"ratio": a.ratio, "h8": a.h8, "atr_pct": a.atr_pct,
            "dist_aprox": a.dist_aprox, "dominante": a.dominante}
