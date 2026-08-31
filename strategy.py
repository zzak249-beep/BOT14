"""
Motor RSI "doble suelo" + salida por SuperTrend.

Traducción literal del Pine de ProBorsa (RSI & SuperTrend Özel Dip
Stratejisi). Si cambias uno, cambia el otro.

LA IDEA
El RSI cruza al alza su propia media móvil. Si eso pasa POR DEBAJO de
un nivel de disparo (50 por defecto), se cuenta como un intento. El
primer intento suele fallar; el SEGUNDO es el que se opera. Eso es lo
que dibuja una figura de doble suelo (W): el precio hace mínimo, rebota
sin fuerza, vuelve a caer y entonces sí gira.

El contador se REINICIA en cuanto el RSI sube por encima del nivel de
disparo: si el mercado ya se recuperó, el intento anterior dejó de
contar.

SALIDA: cuando el SuperTrend cambia de dirección. No hay stop fijo — y
eso hay que tenerlo muy presente, porque significa que el riesgo por
operación NO está acotado de antemano.

ADVERTENCIA IMPORTANTE
Esta estrategia no tiene ni una sola operación medida en este proyecto.
El Pine original viene con RSI de 10 (en vez de 14) y multiplicador
2.5, ajustes que su autor describe como hechos para dar más señales y
salidas más rentables — es decir, parámetros ya optimizados sobre algún
histórico. Mídela antes de ponerle dinero.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import config

log = logging.getLogger("strategy")


@dataclass
class Signal:
    symbol: str
    side: str            # siempre "BUY": la estrategia es solo de largos
    entry: float
    sl: float
    tp: float | None
    rsi: float
    cross_count: int
    st_value: float
    atr_pct: float
    timeframe: str = ""
    btc_24h: float | None = None


def sma(values: list[float], length: int) -> list[float]:
    out: list[float] = []
    acc = 0.0
    for i, v in enumerate(values):
        acc += v
        if i >= length:
            acc -= values[i - length]
        out.append(acc / min(i + 1, length))
    return out


def rma(values: list[float], length: int) -> list[float]:
    """Media de Wilder, que es la que usa el RSI de Pine."""
    if not values:
        return []
    out = [values[0]]
    for v in values[1:]:
        out.append((out[-1] * (length - 1) + v) / length)
    return out


def rsi_series(closes: list[float], length: int) -> list[float]:
    if len(closes) < 2:
        return []
    subidas = [0.0]
    bajadas = [0.0]
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i - 1]
        subidas.append(max(ch, 0.0))
        bajadas.append(max(-ch, 0.0))
    up = rma(subidas, length)
    dn = rma(bajadas, length)
    out: list[float] = []
    for u, d in zip(up, dn):
        if d == 0:
            out.append(100.0)
        elif u == 0:
            out.append(0.0)
        else:
            out.append(100.0 - (100.0 / (1.0 + u / d)))
    return out


def atr_series(highs: list[float], lows: list[float], closes: list[float], length: int) -> list[float]:
    if len(closes) < 2:
        return []
    trs = [highs[0] - lows[0]]
    for i in range(1, len(closes)):
        trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    return rma(trs, length)


def supertrend(
    highs: list[float], lows: list[float], closes: list[float], factor: float, period: int
) -> tuple[list[float], list[int]]:
    """
    SuperTrend igual que ta.supertrend de Pine.
    Dirección: -1 alcista (la línea va por debajo), +1 bajista.
    """
    a = atr_series(highs, lows, closes, period)
    if not a:
        return [], []
    st: list[float] = []
    dirs: list[int] = []
    upper_prev = 0.0
    lower_prev = 0.0
    st_prev = 0.0
    dir_prev = 1
    for i in range(len(closes)):
        hl2 = (highs[i] + lows[i]) / 2.0
        upper = hl2 + factor * a[i]
        lower = hl2 - factor * a[i]
        if i == 0:
            st.append(upper)
            dirs.append(1)
            upper_prev, lower_prev, st_prev, dir_prev = upper, lower, upper, 1
            continue
        lower = lower if (lower > lower_prev or closes[i - 1] < lower_prev) else lower_prev
        upper = upper if (upper < upper_prev or closes[i - 1] > upper_prev) else upper_prev
        if st_prev == upper_prev:
            d = -1 if closes[i] > upper else 1
        else:
            d = 1 if closes[i] < lower else -1
        valor = lower if d == -1 else upper
        st.append(valor)
        dirs.append(d)
        upper_prev, lower_prev, st_prev, dir_prev = upper, lower, valor, d
    return st, dirs


def evaluate(symbol: str, candles: list[dict]) -> tuple[Signal | None, str]:
    """
    Devuelve (señal, motivo). El motivo dice por qué NO hay señal, que en
    un escáner vale más que el silencio.
    """
    need = max(config.RSI_LEN, config.SIG_LEN, config.ST_PERIOD) * 4 + 20
    if len(candles) < need:
        return None, "pocas velas"

    c = candles[:-1]  # la última está en curso
    closes = [x["close"] for x in c]
    highs = [x["high"] for x in c]
    lows = [x["low"] for x in c]

    rsi = rsi_series(closes, config.RSI_LEN)
    if len(rsi) < config.SIG_LEN + 2:
        return None, "sin rsi"
    rsi_sig = sma(rsi, config.SIG_LEN)
    a = atr_series(highs, lows, closes, 14)
    st, dirs = supertrend(highs, lows, closes, config.ST_FACTOR, config.ST_PERIOD)
    if not st or not a:
        return None, "sin supertrend"

    atr_pct = a[-1] / closes[-1] * 100.0 if closes[-1] > 0 else 0.0
    if atr_pct < config.MIN_ATR_PCT:
        return None, f"sin amplitud ({atr_pct:.2f}%)"

    # ── El contador, recorrido sobre todo el histórico disponible ──
    # Se recalcula entero en cada evaluación en vez de guardarlo entre
    # ciclos: así el estado del bot no puede desincronizarse del gráfico
    # si se reinicia, que es de donde salen los fallos más difíciles de
    # encontrar.
    cross_count = 0
    señal_idx = -1
    for i in range(1, len(rsi)):
        if rsi[i] > config.TRIGGER_LEVEL:
            cross_count = 0
            continue
        cruce = rsi[i] > rsi_sig[i] and rsi[i - 1] <= rsi_sig[i - 1]
        if cruce and rsi[i] < config.TRIGGER_LEVEL:
            cross_count += 1
            if cross_count == config.TARGET_CROSS:
                señal_idx = i
                cross_count = 0

    if señal_idx != len(rsi) - 1:
        return None, f"sin señal (contador en {cross_count} de {config.TARGET_CROSS}, RSI {rsi[-1]:.0f})"

    entrada = closes[-1]

    # El SuperTrend bajista deja su línea POR ENCIMA del precio: no
    # sirve de stop. Dos salidas posibles, ambas defendibles.
    if dirs[-1] == 1:
        if config.REQUIRE_ST_BULL:
            return None, "señal, pero el SuperTrend sigue bajista"
        # Fiel al original: se entra igual, con el stop bajo el mínimo
        # reciente. Sin esto la posición quedaría sin protección real
        # hasta el próximo giro, que puede tardar días.
        ventana = lows[-config.SL_SWING_LOOKBACK:]
        stop = min(ventana) - a[-1] * config.SL_SWING_ATR
    else:
        stop = st[-1]

    if stop >= entrada:
        return None, "stop por encima del precio"

    riesgo_pct = (entrada - stop) / entrada * 100.0
    if riesgo_pct > config.MAX_RISK_PCT:
        return None, f"stop demasiado lejos ({riesgo_pct:.1f}%)"

    # Coste en múltiplos de R: lo que la operación pierde de salida.
    coste_r = config.COST_ROUNDTRIP_PCT / riesgo_pct if riesgo_pct > 0 else 99.0
    if riesgo_pct < config.MIN_RISK_PCT or coste_r > config.MAX_COST_IN_R:
        return None, f"stop demasiado cerca ({riesgo_pct:.2f}%, coste {coste_r:.2f}R)"

    tp = entrada + (entrada - stop) * config.RR_TARGET if config.USE_TP else None

    return (
        Signal(
            symbol=symbol,
            side="BUY",
            entry=entrada,
            sl=stop,
            tp=tp,
            rsi=rsi[-1],
            cross_count=config.TARGET_CROSS,
            st_value=st[-1],
            atr_pct=atr_pct,
        ),
        "ok",
    )


def exit_signal(candles: list[dict]) -> bool:
    """SuperTrend girando a bajista: es la salida del Pine original."""
    if len(candles) < config.ST_PERIOD * 4:
        return False
    c = candles[:-1]
    _, dirs = supertrend(
        [x["high"] for x in c], [x["low"] for x in c], [x["close"] for x in c],
        config.ST_FACTOR, config.ST_PERIOD,
    )
    return len(dirs) >= 2 and dirs[-1] == 1 and dirs[-2] == -1


def position_size(equity: float, entry: float, sl: float) -> float:
    riesgo_unidad = abs(entry - sl)
    if riesgo_unidad <= 0 or entry <= 0:
        return 0.0
    return (equity * config.RISK_PCT / 100.0) / riesgo_unidad


# ══════════════════════════════════════════════════════════════════════
# ESTADO DE VIGILANCIA
# Para el aviso periódico: no solo las que YA dan señal, sino en qué
# punto del patrón está cada una. Un símbolo con el contador en 1 de 2
# ya tocó suelo una vez y está a un cruce de disparar — eso es lo que
# se quiere ver venir, no enterarse cuando ya pasó.
# ══════════════════════════════════════════════════════════════════════

@dataclass
class Watch:
    symbol: str
    rsi: float
    rsi_sig: float
    cross_count: int
    st_alcista: bool
    atr_pct: float
    bajo_umbral: bool
    listo: bool


def watch_status(candles: list[dict]) -> Watch | None:
    """Dónde está este símbolo dentro del patrón, dispare o no."""
    need = max(config.RSI_LEN, config.SIG_LEN, config.ST_PERIOD) * 4 + 20
    if len(candles) < need:
        return None

    c = candles[:-1]
    closes = [x["close"] for x in c]
    highs = [x["high"] for x in c]
    lows = [x["low"] for x in c]

    rsi = rsi_series(closes, config.RSI_LEN)
    if len(rsi) < config.SIG_LEN + 2:
        return None
    rsi_sig = sma(rsi, config.SIG_LEN)
    a = atr_series(highs, lows, closes, config.ATR_LEN)
    _, dirs = supertrend(highs, lows, closes, config.ST_FACTOR, config.ST_PERIOD)
    if not a or not dirs:
        return None

    cross_count = 0
    for i in range(1, len(rsi)):
        if rsi[i] > config.TRIGGER_LEVEL:
            cross_count = 0
            continue
        if rsi[i] > rsi_sig[i] and rsi[i - 1] <= rsi_sig[i - 1] and rsi[i] < config.TRIGGER_LEVEL:
            cross_count += 1
            if cross_count == config.TARGET_CROSS:
                cross_count = 0 if i != len(rsi) - 1 else config.TARGET_CROSS

    atr_pct = a[-1] / closes[-1] * 100.0 if closes[-1] > 0 else 0.0
    return Watch(
        symbol="", rsi=rsi[-1], rsi_sig=rsi_sig[-1], cross_count=cross_count,
        st_alcista=dirs[-1] == -1, atr_pct=atr_pct,
        bajo_umbral=rsi[-1] < config.TRIGGER_LEVEL,
        listo=cross_count >= config.TARGET_CROSS,
    )
