"""
Motor: descomposición Haar À TROUS causal + cruce sobre la aproximación,
con el filtro de régimen corregido (energía normalizada por escala).

═══════════════════════════════════════════════════════════════════════
QUÉ CAMBIA RESPECTO A LA VERSIÓN ANTERIOR
═══════════════════════════════════════════════════════════════════════
La versión anterior calculaba el "detalle a escala n" como la diferencia
entre dos medias móviles de n barras separadas n barras. Es causal, sí,
pero NO es la transformada à trous: no reconstruye la serie, las escalas
se solapan de forma no controlada, y dividir por n no corresponde a la
longitud real del filtro de esa escala.

Aquí está la recursión de Renaud, Starck & Murtagh:

    S_0(t)     = close(t)
    S_{j+1}(t) = [S_j(t) + S_j(t - 2^j)] / 2
    w_{j+1}(t) = S_j(t) - S_{j+1}(t)

Propiedades que ahora sí se cumplen:
  · RECONSTRUCCIÓN EXACTA: close = S_J + Σ w_j
  · CAUSAL POR CONSTRUCCIÓN: w_j(t) usa close[t-2^j … t] y nada más. El
    valor de la barra t no cambia nunca al llegar t+1. Sin ventana
    deslizante, sin efecto de borde, sin recómputo.
  · Es la ÚNICA familia wavelet que cumple esa restricción cuando los
    coeficientes se usan para predecir. Todo el "wavelet denoising" que
    aplica la transformada sobre la serie completa —futuro incluido—
    produce backtests preciosos e irreproducibles.

═══════════════════════════════════════════════════════════════════════
EL FILTRO DE RÉGIMEN, Y POR QUÉ SE NORMALIZA
═══════════════════════════════════════════════════════════════════════
El script de partida comparaba energía gruesa contra fina y llamaba
"tendencia" a un ratio > 1.5. Sobre PASEO ALEATORIO PURO ese ratio da
mediana ~3.0 y supera 1.5 más del 92% del tiempo: no filtra nada.

La causa es matemática: cada escala acumula varianza proporcional a su
longitud, así que el numerador arranca inflado. La corrección es dividir
la energía de cada escala por 2^j — su longitud REAL en la à trous.

Con la normalización correcta, el mismo ruido puro da mediana ~0.70.

NORMALIZE_SCALES=false deja el modo original para poder comparar.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import config
import tca

log = logging.getLogger("strategy")


@dataclass
class Signal:
    symbol: str
    side: str            # "BUY" | "SELL"
    entry: float
    sl: float
    tp: float
    ratio: float         # dominancia grueso/fino ya normalizada
    umbral: float
    h8: float            # detalle de la escala más gruesa (dirección de fondo)
    persist: float       # ER de la tendencia: 1 = línea recta, 0 = va y vuelve
    atr_pct: float
    riesgo_pct: float
    coste_r: float
    coste_pct: float = 0.0   # coste usado: medido si lo hay, estimado si no
    coste_ops: int = 0       # operaciones sobre las que se midió (0 = estimado)
    timeframe: str = ""
    btc_24h: float | None = None
    funding: float | None = None


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


# ── Haar à trous ──────────────────────────────────────────────────────
def atrous(closes: list[float], levels: int | None = None):
    """
    Devuelve (S_J, [w_1 … w_J]), todos de la misma longitud que closes.
    Reconstrucción exacta: closes[t] == S_J[t] + Σ w_j[t].
    """
    levels = levels or config.MRA_LEVELS
    n = len(closes)
    if n < 2 ** levels + 2:
        return None, None
    s = list(closes)
    detalles: list[list[float]] = []
    for j in range(levels):
        lag = 2 ** j
        nxt = list(s)
        for t in range(lag, n):
            nxt[t] = (s[t] + s[t - lag]) / 2.0
        detalles.append([s[t] - nxt[t] for t in range(n)])
        s = nxt
    return s, detalles


def regime(closes: list[float], lookback: int, normalizar: bool) -> tuple[float, float]:
    """
    Devuelve (ratio, h_gruesa).

    ratio = energía de las escalas gruesas / energía de las finas, cada
    escala dividida por su longitud 2^j si normalizar. Las primeras 2^J
    muestras se descartan: ahí el filtro aún no está saturado.
    """
    trend, det = atrous(closes)
    if trend is None:
        return 0.0, 0.0
    warm = 2 ** len(det)
    energia = []
    for j, serie in enumerate(det):
        ventana = serie[max(warm, len(serie) - lookback):]
        e = sum(x * x for x in ventana)
        energia.append(e / (2 ** (j + 1)) if normalizar else e)
    mitad = max(1, len(energia) // 2)
    fino = sum(energia[:mitad])
    grueso = sum(energia[mitad:])
    ratio = grueso / fino if fino > 0 else 0.0
    return ratio, det[-1][-1]


def persistencia(closes: list[float], lookback: int) -> float:
    """
    Eficiencia (Kaufman) calculada SOBRE LA TENDENCIA WAVELET, no sobre
    el precio: distancia recorrida / camino andado en la ventana.

    POR QUÉ HACE FALTA — medido, no supuesto. Con la à trous correcta,
    el ratio de dominancia NO distingue tendencia de oscilación: sobre
    series sintéticas da mediana 1.12 en tendencia moderada y 1.44 en
    OSCILANTE. Es lógico: una oscilación de amplitud grande también
    concentra energía en las escalas gruesas. El ratio mide TAMAÑO por
    escala, no DIRECCIÓN.

    El ER sobre la tendencia sí separa: mediana 1.00 en tendencia y
    0.26 en oscilante. Combinando los dos filtros, el paso de series
    oscilantes cae del 64% al 6% sin tocar las de tendencia (36% y 94%
    se mantienen).

    Se calcula sobre S_J y no sobre el precio a propósito: el ER crudo
    es ruidosísimo en 5m, y suavizarlo con una media introduciría
    retardo. La tendencia wavelet ya está suavizada y es causal.
    """
    trend, _ = atrous(closes)
    if trend is None:
        return 0.0
    seg = trend[-lookback - 1:]
    if len(seg) < 3:
        return 0.0
    camino = sum(abs(seg[i] - seg[i - 1]) for i in range(1, len(seg)))
    return abs(seg[-1] - seg[0]) / camino if camino > 0 else 0.0


def _aprox(closes: list[float]) -> list[float]:
    """
    Serie sobre la que se busca el cruce.

    CROSS_SOURCE=trend  -> S_J, la tendencia wavelet. Coherente con el
                           motor y con el Pine.
    CROSS_SOURCE=price  -> el precio crudo, como la versión anterior,
                           para poder comparar sin cambiar nada más.
    """
    if getattr(config, "CROSS_SOURCE", "trend").lower() == "price":
        return list(closes)
    trend, _ = atrous(closes)
    return trend if trend is not None else list(closes)


def min_velas() -> int:
    return max(config.LOOKBACK_ENERGY + 2 ** config.MRA_LEVELS + 4,
               config.APPROX_LEN + 2 ** config.MRA_LEVELS + 4,
               (config.HTF_MA_LEN + 20) if config.USE_HTF_FILTER else 0)


def evaluate(symbol: str, candles: list[dict]) -> tuple[Signal | None, str]:
    if len(candles) < min_velas():
        return None, "pocas velas"

    c = candles[:-1]  # solo velas cerradas: la última aún se mueve
    closes = [x["close"] for x in c]
    highs = [x["high"] for x in c]
    lows = [x["low"] for x in c]

    a = atr_series(highs, lows, closes, config.ATR_LEN)
    if not a or a[-1] <= 0 or closes[-1] <= 0:
        return None, "sin indicadores"

    # COSTE MEDIDO para ESTE símbolo, no la constante global.
    if tca.sospechoso(symbol):
        c_med, n_med = tca.medido(symbol)
        return None, f"coste real prohibitivo ({c_med:.3f}% en {n_med} ops)"
    coste_pct = tca.coste(symbol)
    coste_pct_med, coste_ops = tca.medido(symbol)

    atr_pct = a[-1] / closes[-1] * 100.0
    cover = atr_pct / coste_pct if coste_pct > 0 else 0.0
    if atr_pct < config.MIN_ATR_PCT or cover < config.MIN_COST_COVER:
        return None, f"sin amplitud ({atr_pct:.2f}%, {cover:.0f}x)"

    ratio, h8 = regime(closes, config.LOOKBACK_ENERGY, config.NORMALIZE_SCALES)
    if ratio < config.DOMINANCE_THRESHOLD:
        return None, f"sin dominancia ({ratio:.2f} de {config.DOMINANCE_THRESHOLD})"

    persist = persistencia(closes, config.LOOKBACK_ENERGY)
    if config.USE_PERSISTENCE and persist < config.MIN_PERSISTENCE:
        return None, f"oscilante (ER {persist:.2f} de {config.MIN_PERSISTENCE})"

    base = _aprox(closes)
    m = sma(base, config.APPROX_LEN)
    cruza_arriba = base[-1] > m[-1] and base[-2] <= m[-2]
    cruza_abajo = base[-1] < m[-1] and base[-2] >= m[-2]

    # La escala gruesa debe apuntar en la misma dirección que el cruce:
    # sin esto se compran cruces contra la estructura de fondo.
    largo = cruza_arriba and h8 > 0 and config.ALLOW_LONG
    corto = cruza_abajo and h8 < 0 and config.ALLOW_SHORT

    if not (largo or corto):
        if cruza_arriba or cruza_abajo:
            return None, "cruce contra la escala gruesa"
        return None, f"sin cruce (ratio {ratio:.2f})"

    if config.USE_HTF_FILTER and len(closes) > config.HTF_MA_LEN:
        ma_larga = sma(closes, config.HTF_MA_LEN)[-1]
        if largo and closes[-1] < ma_larga:
            return None, "largo por debajo de la media larga"
        if corto and closes[-1] > ma_larga:
            return None, "corto por encima de la media larga"

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
    coste_r = coste_pct / riesgo_pct if riesgo_pct > 0 else 99.0

    if coste_r > config.MAX_COST_IN_R:
        return None, f"stop demasiado cerca (coste {coste_r:.2f}R)"
    if riesgo_pct > config.MAX_RISK_PCT:
        return None, f"stop demasiado lejos ({riesgo_pct:.1f}%)"
    if riesgo_pct < config.MIN_RISK_PCT:
        return None, f"riesgo bajo ({riesgo_pct:.2f}%)"

    return (
        Signal(symbol=symbol, side=side, entry=entrada, sl=sl, tp=tp,
               ratio=ratio, umbral=config.DOMINANCE_THRESHOLD, h8=h8,
               persist=persist,
               atr_pct=atr_pct, riesgo_pct=riesgo_pct, coste_r=coste_r,
               coste_pct=coste_pct,
               coste_ops=coste_ops if coste_ops >= config.MIN_TCA_SAMPLES else 0),
        "ok",
    )


def exit_cross(candles: list[dict], side: str) -> bool:
    """Cruce contrario sobre la misma serie que generó la entrada."""
    c = candles[:-1]
    closes = [x["close"] for x in c]
    if len(closes) < min_velas():
        return False
    base = _aprox(closes)
    m = sma(base, config.APPROX_LEN)
    if side == "BUY":
        return base[-1] < m[-1] and base[-2] >= m[-2]
    return base[-1] > m[-1] and base[-2] <= m[-2]


def trailing_stop(candles: list[dict], side: str, stop_actual: float) -> float:
    """
    Stop que solo se mueve a favor. Apagado por defecto: las fuentes
    dicen que un trailing supera a la salida fija porque captura la
    continuación, pero eso es hipótesis a medir, no certeza.
    """
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


def position_size(equity: float, entry: float, sl: float,
                  factor: float = 1.0) -> float:
    """
    Riesgo fijo. NO se divide por el apalancamiento ni otra vez por el
    precio: los dos errores clásicos de sizing. Kelly se deja fuera a
    propósito — estimar el edge con decenas de operaciones produce
    tamaños disparatados justo cuando menos se sabe.
    """
    riesgo = abs(entry - sl)
    if riesgo <= 0 or entry <= 0:
        return 0.0
    return (equity * config.RISK_PCT * factor / 100.0) / riesgo


def watch_status(candles: list[dict]) -> dict | None:
    """Estado del régimen para el aviso de vigilancia."""
    if len(candles) < min_velas():
        return None
    c = candles[:-1]
    closes = [x["close"] for x in c]
    a = atr_series([x["high"] for x in c], [x["low"] for x in c], closes, config.ATR_LEN)
    if not a or closes[-1] <= 0 or a[-1] <= 0:
        return None
    ratio, h8 = regime(closes, config.LOOKBACK_ENERGY, config.NORMALIZE_SCALES)
    base = _aprox(closes)
    m = sma(base, config.APPROX_LEN)
    return {
        "ratio": ratio,
        "h8": h8,
        "persist": persistencia(closes, config.LOOKBACK_ENERGY),
        "atr_pct": a[-1] / closes[-1] * 100.0,
        "dist_aprox": (base[-1] - m[-1]) / a[-1],
        "dominante": (ratio >= config.DOMINANCE_THRESHOLD
                      and (not config.USE_PERSISTENCE
                           or persistencia(closes, config.LOOKBACK_ENERGY)
                           >= config.MIN_PERSISTENCE)),
    }


def calidad(sig: Signal) -> tuple:
    """
    Clave de orden para elegir ENTRE candidatos del mismo ciclo.

    Menos coste primero (es el único término cierto de la ecuación),
    luego más persistencia y más dominancia. Nada de esto pretende
    predecir cuál ganará: solo evita que el orden alfabético del
    universo decida por ti cuando hay un hueco y veinte señales.
    """
    return (round(sig.coste_r, 3),
            -round(getattr(sig, "persist", 0.0), 3),
            -round(sig.ratio, 3))
