"""
Motor: descomposición multiescala tipo à trous + cruce sobre la
aproximación, con el filtro de régimen CORREGIDO.

═══════════════════════════════════════════════════════════════════════
EL FALLO DEL ORIGINAL, Y POR QUÉ IMPORTA
═══════════════════════════════════════════════════════════════════════
El script de partida compara la energía de las escalas gruesas (4 y 8
barras) contra las finas (1 y 2) y llama "tendencia" a que el ratio
supere 1.5.

Simulado sobre un PASEO ALEATORIO PURO —ruido sin ninguna tendencia—
ese ratio da:

    mediana 3.04  ·  por encima de 1.5 el 92.6% del tiempo

O sea que el filtro se enciende casi siempre aunque no pase nada. No
distingue tendencia de aleatoriedad: solo descarta mercados
fuertemente oscilantes (ahí el ratio baja a ~1.2).

La causa es matemática, no de implementación: la diferencia entre dos
medias de 8 barras tiene mucha más varianza que entre dos de 1 barra,
así que el numerador arranca inflado. En un análisis wavelet serio la
energía se NORMALIZA por escala antes de compararla.

LA CORRECCIÓN: dividir la energía de cada escala por su longitud. Con
eso, el mismo ruido puro da mediana 0.75 y percentil 75 en 1.00 — y
entonces un umbral alrededor de 1.0-1.3 significa algo de verdad.

Se deja el modo original disponible (NORMALIZE=false) para poder
comparar los dos en el backtester en vez de creerme a mí.

═══════════════════════════════════════════════════════════════════════
LO QUE ESTO ES Y NO ES
═══════════════════════════════════════════════════════════════════════
No es una DWT ortogonal. Es una aproximación redundante y CAUSAL del
algoritmo à trous: usa solo datos pasados, sin repintado. Eso es una
virtud frente a la mayoría de "wavelet denoising" que circula, que
aplica la transformada sobre la serie completa —futuro incluido— y
produce backtests preciosos e irreproducibles.

Y no asumas el 71% / Sharpe 2.44 del hilo original: esos números salen
de una estrategia con el filtro encendido el 92% del tiempo, o sea de
un cruce de medias sin filtro efectivo.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

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
    ratio: float         # dominancia grueso/fino ya normalizada
    umbral: float
    h8: float            # pendiente de la escala gruesa
    atr_pct: float
    riesgo_pct: float
    coste_r: float
    timeframe: str = ""
    btc_24h: float | None = None


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
    """
    Detalle causal a escala n: diferencia entre la media de las últimas
    n barras y la media de las n anteriores. Es el paso "à trous" del
    original, y solo mira hacia atrás.
    """
    m = sma(closes, n)
    out = []
    for i in range(len(closes)):
        j = i - n
        out.append(0.0 if j < 0 else (m[i] - m[j]) / (2 ** 0.5))
    return out


def regime(closes: list[float], lookback: int, normalizar: bool) -> tuple[float, float]:
    """Devuelve (ratio, h8). El ratio ya viene normalizado si procede."""
    detalles = {n: haar_detail(closes, n) for n in (1, 2, 4, 8)}
    energia = {}
    for n, serie in detalles.items():
        ventana = serie[-lookback:]
        e = sum(x * x for x in ventana)
        # LA CORRECCIÓN: cada escala se divide por su longitud. Sin esto,
        # las gruesas ganan siempre por pura acumulación de varianza.
        energia[n] = e / n if normalizar else e

    fino = sum(energia[n] for n in ESCALAS_FINAS)
    grueso = sum(energia[n] for n in ESCALAS_GRUESAS)
    ratio = grueso / fino if fino > 0 else 0.0
    return ratio, detalles[8][-1]


def evaluate(symbol: str, candles: list[dict]) -> tuple[Signal | None, str]:
    need = config.LOOKBACK_ENERGY + 32
    if len(candles) < need:
        return None, "pocas velas"

    c = candles[:-1]  # solo velas cerradas: la última aún se mueve
    closes = [x["close"] for x in c]
    highs = [x["high"] for x in c]
    lows = [x["low"] for x in c]

    a = atr_series(highs, lows, closes, config.ATR_LEN)
    if not a or a[-1] <= 0 or closes[-1] <= 0:
        return None, "sin indicadores"

    atr_pct = a[-1] / closes[-1] * 100.0
    cover = atr_pct / config.COST_ROUNDTRIP_PCT if config.COST_ROUNDTRIP_PCT > 0 else 0.0
    if atr_pct < config.MIN_ATR_PCT or cover < config.MIN_COST_COVER:
        return None, f"sin amplitud ({atr_pct:.2f}%, {cover:.0f}x)"

    ratio, h8 = regime(closes, config.LOOKBACK_ENERGY, config.NORMALIZE_SCALES)
    if ratio < config.DOMINANCE_THRESHOLD:
        return None, f"sin dominancia ({ratio:.2f} de {config.DOMINANCE_THRESHOLD})"

    # Cruce del precio sobre su aproximación (SMA corta).
    aprox = sma(closes, config.APPROX_LEN)
    cruza_arriba = closes[-1] > aprox[-1] and closes[-2] <= aprox[-2]
    cruza_abajo = closes[-1] < aprox[-1] and closes[-2] >= aprox[-2]

    # La escala gruesa debe apuntar en la misma dirección que el cruce:
    # sin esto se compran cruces contra la estructura de fondo.
    largo = cruza_arriba and h8 > 0 and config.ALLOW_LONG
    corto = cruza_abajo and h8 < 0 and config.ALLOW_SHORT

    if not (largo or corto):
        if cruza_arriba or cruza_abajo:
            return None, "cruce contra la escala gruesa"
        return None, f"sin cruce (ratio {ratio:.2f})"

    if config.USE_VOL_FILTER:
        vols = [x["volume"] for x in c]
        vsma = sma(vols, config.VOL_LEN)
        if vsma[-1] <= 0 or vols[-1] < vsma[-1] * config.VOL_MULT:
            return None, "sin volumen"

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
        return None, "riesgo no válido"
    riesgo_pct = riesgo / entrada * 100.0
    coste_r = config.COST_ROUNDTRIP_PCT / riesgo_pct if riesgo_pct > 0 else 99.0

    if coste_r > config.MAX_COST_IN_R:
        return None, f"stop demasiado cerca (coste {coste_r:.2f}R)"
    if riesgo_pct > config.MAX_RISK_PCT:
        return None, f"stop demasiado lejos ({riesgo_pct:.1f}%)"

    return (
        Signal(symbol=symbol, side=side, entry=entrada, sl=sl, tp=tp,
               ratio=ratio, umbral=config.DOMINANCE_THRESHOLD, h8=h8,
               atr_pct=atr_pct, riesgo_pct=riesgo_pct, coste_r=coste_r),
        "ok",
    )


def position_size(equity: float, entry: float, sl: float) -> float:
    """
    Riesgo fijo por operación. El original ofrecía además Kelly, que se
    ha dejado fuera a propósito: Kelly necesita conocer el edge REAL, y
    estimarlo con las primeras decenas de operaciones produce tamaños
    disparatados justo cuando menos se sabe. Cuando el diario tenga
    varios cientos de operaciones, será el momento de plantearlo.
    """
    riesgo = abs(entry - sl)
    if riesgo <= 0 or entry <= 0:
        return 0.0
    return (equity * config.RISK_PCT / 100.0) / riesgo


def watch_status(candles: list[dict]) -> dict | None:
    """Estado del régimen para el aviso de vigilancia."""
    if len(candles) < config.LOOKBACK_ENERGY + 32:
        return None
    c = candles[:-1]
    closes = [x["close"] for x in c]
    a = atr_series([x["high"] for x in c], [x["low"] for x in c], closes, config.ATR_LEN)
    if not a or closes[-1] <= 0:
        return None
    ratio, h8 = regime(closes, config.LOOKBACK_ENERGY, config.NORMALIZE_SCALES)
    aprox = sma(closes, config.APPROX_LEN)
    return {
        "ratio": ratio,
        "h8": h8,
        "atr_pct": a[-1] / closes[-1] * 100.0,
        "dist_aprox": (closes[-1] - aprox[-1]) / a[-1] if a[-1] > 0 else 0.0,
        "dominante": ratio >= config.DOMINANCE_THRESHOLD,
    }
