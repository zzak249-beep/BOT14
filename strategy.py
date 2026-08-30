"""
strategy.py
-----------
Réplica en Python de la estrategia del Pine Script original:
"ProBorsa: RSI & SuperTrend Özel Dip Stratejisi"

Lógica (idéntica al script de TradingView):
  1. RSI (suavizado de Wilder / ta.rma) de longitud `rsi_length`.
  2. Señal = media móvil simple del RSI, longitud `signal_length`.
  3. Cada vez que el RSI cruza al alza su señal ESTANDO por debajo de
     `trigger_level`, se incrementa un contador. Si el RSI sube por encima
     de `trigger_level`, el contador se reinicia a 0.
  4. Señal de COMPRA ("doble suelo") cuando el contador alcanza
     `target_cross_count` (2 = patrón W) en el mismo cruce que lo dispara.
  5. Señal de VENTA cuando el SuperTrend (ATR `atr_period`, factor
     `st_factor`) cambia de tendencia alcista a bajista.

Todas las funciones trabajan sobre un DataFrame con columnas:
  'open', 'high', 'low', 'close' (una fila = una vela, orden cronológico ascendente).
"""

import numpy as np
import pandas as pd


def rma(series: pd.Series, length: int) -> pd.Series:
    """Wilder's RMA — equivalente exacto de ta.rma() en Pine Script.

    Semilla: media simple de los primeros `length` valores.
    A partir de ahí: rma[i] = alpha*src[i] + (1-alpha)*rma[i-1], alpha = 1/length.
    """
    result = pd.Series(np.nan, index=series.index, dtype="float64")
    if len(series) < length:
        return result
    seed = series.iloc[:length].mean()
    result.iloc[length - 1] = seed
    alpha = 1.0 / length
    prev = seed
    for i in range(length, len(series)):
        prev = alpha * series.iloc[i] + (1 - alpha) * prev
        result.iloc[i] = prev
    return result


def compute_rsi(close: pd.Series, length: int) -> pd.Series:
    """RSI de Wilder, replicando exactamente la fórmula del Pine Script:
    down==0 ? 100 : up==0 ? 0 : 100 - 100/(1+up/down)
    """
    change = close.diff()
    up = rma(change.clip(lower=0), length)
    down = rma((-change).clip(lower=0), length)

    out = pd.Series(np.nan, index=close.index, dtype="float64")
    for i in range(len(close)):
        u, d = up.iloc[i], down.iloc[i]
        if pd.isna(u) or pd.isna(d):
            continue
        if d == 0:
            out.iloc[i] = 100.0
        elif u == 0:
            out.iloc[i] = 0.0
        else:
            out.iloc[i] = 100 - (100 / (1 + u / d))
    return out


def _true_range(df: pd.DataFrame) -> pd.Series:
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    tr.iloc[0] = df["high"].iloc[0] - df["low"].iloc[0]
    return tr


def supertrend(df: pd.DataFrame, period: int, multiplier: float):
    """Devuelve (linea_supertrend, tendencia_alcista_bool) para cada vela.

    Réplica del algoritmo estándar de SuperTrend (ATR suavizado con RMA,
    igual que ta.atr() en Pine). Nota: TradingView no publica el código
    fuente exacto de su función incorporada ta.supertrend(), así que esta
    es una reimplementación fiel al algoritmo estándar ampliamente documentado;
    puede haber diferencias mínimas de 1 vela en casos límite. Se recomienda
    validar en DRY_RUN antes de operar en real.
    """
    atr = rma(_true_range(df), period)
    hl2 = (df["high"] + df["low"]) / 2
    upper_basic = hl2 + multiplier * atr
    lower_basic = hl2 - multiplier * atr

    n = len(df)
    final_upper = pd.Series(np.nan, index=df.index, dtype="float64")
    final_lower = pd.Series(np.nan, index=df.index, dtype="float64")
    trend_up = pd.Series(True, index=df.index, dtype="object")

    first_valid = atr.first_valid_index()
    if first_valid is None:
        return pd.Series(np.nan, index=df.index), trend_up

    start = df.index.get_loc(first_valid)
    final_upper.iloc[start] = upper_basic.iloc[start]
    final_lower.iloc[start] = lower_basic.iloc[start]
    trend_up.iloc[start] = df["close"].iloc[start] >= (final_upper.iloc[start] + final_lower.iloc[start]) / 2

    for i in range(start + 1, n):
        if (upper_basic.iloc[i] < final_upper.iloc[i - 1]) or (df["close"].iloc[i - 1] > final_upper.iloc[i - 1]):
            final_upper.iloc[i] = upper_basic.iloc[i]
        else:
            final_upper.iloc[i] = final_upper.iloc[i - 1]

        if (lower_basic.iloc[i] > final_lower.iloc[i - 1]) or (df["close"].iloc[i - 1] < final_lower.iloc[i - 1]):
            final_lower.iloc[i] = lower_basic.iloc[i]
        else:
            final_lower.iloc[i] = final_lower.iloc[i - 1]

        prev_trend_up = trend_up.iloc[i - 1]
        if prev_trend_up and df["close"].iloc[i] < final_lower.iloc[i]:
            trend_up.iloc[i] = False
        elif (not prev_trend_up) and df["close"].iloc[i] > final_upper.iloc[i]:
            trend_up.iloc[i] = True
        else:
            trend_up.iloc[i] = prev_trend_up

    st_line = pd.Series(np.where(trend_up, final_lower, final_upper), index=df.index, dtype="float64")
    st_line.iloc[:start] = np.nan
    trend_up.iloc[:start] = np.nan
    return st_line, trend_up


def compute_signals(
    df: pd.DataFrame,
    rsi_length: int = 10,
    signal_length: int = 10,
    trigger_level: float = 50.0,
    target_cross_count: int = 2,
    atr_period: int = 10,
    st_factor: float = 2.5,
) -> pd.DataFrame:
    """Calcula todos los indicadores y señales para cada vela del DataFrame.

    Devuelve un DataFrame con, entre otras, las columnas:
      - 'special_buy'  (bool): señal de compra "doble suelo"
      - 'sell_signal'  (bool): señal de venta (giro bajista del SuperTrend)
    """
    rsi = compute_rsi(df["close"], rsi_length)
    rsi_signal = rsi.rolling(signal_length).mean()

    bull_cross = (rsi.shift(1) <= rsi_signal.shift(1)) & (rsi > rsi_signal)

    cross_count = 0
    special_buy = pd.Series(False, index=df.index)
    cross_count_series = pd.Series(0, index=df.index)
    for i in range(len(df)):
        r = rsi.iloc[i]
        if pd.isna(r) or pd.isna(rsi_signal.iloc[i]):
            cross_count_series.iloc[i] = cross_count
            continue
        if r > trigger_level:
            cross_count = 0
        if bool(bull_cross.iloc[i]) and r < trigger_level:
            cross_count += 1
        if bool(bull_cross.iloc[i]) and (r < trigger_level) and (cross_count == target_cross_count):
            special_buy.iloc[i] = True
            cross_count = 0
        cross_count_series.iloc[i] = cross_count

    st_line, trend_up = supertrend(df, atr_period, st_factor)
    sell_signal = (trend_up.shift(1) == True) & (trend_up == False)  # noqa: E712

    return pd.DataFrame(
        {
            "close": df["close"],
            "rsi": rsi,
            "rsi_signal": rsi_signal,
            "bull_cross": bull_cross,
            "cross_count": cross_count_series,
            "special_buy": special_buy,
            "supertrend": st_line,
            "trend_up": trend_up,
            "sell_signal": sell_signal,
        },
        index=df.index,
    )
