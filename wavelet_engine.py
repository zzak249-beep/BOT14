"""
wavelet_engine.py — Puerto a Python/pandas del script Pine
"Wavelet MRA Haar 5m — BingX".

Cada función replica exactamente su equivalente Pine:

    haar_detail(s, len) =>
        avg_recent = ta.sma(s, len)
        avg_prior  = ta.sma(s[len], len)
        (avg_recent - avg_prior) / math.sqrt(2)

`s[len]` en Pine ("hace `len` barras") equivale a `s.shift(len)` en
pandas — el resto de la cadena (rolling mean) es igual en ambos.

IMPORTANTE: `compute_signal` debe recibir SIEMPRE un DataFrame que
termine en la última vela YA CERRADA (nunca la vela en formación),
que es el equivalente exacto de `barstate.isconfirmed` en Pine.
"""

import math

import numpy as np
import pandas as pd

SQRT2 = math.sqrt(2.0)


def _haar_detail(s: pd.Series, length: int) -> pd.Series:
    avg_recent = s.rolling(length).mean()
    avg_prior = s.shift(length).rolling(length).mean()
    return (avg_recent - avg_prior) / SQRT2


def _rolling_energy(detail: pd.Series, lookback: int) -> pd.Series:
    # nz(x) -> 0 antes de sumar, igual que en el Pine original,
    # para que el warm-up no propague NaN a la suma.
    return (detail.fillna(0.0) ** 2).rolling(lookback).sum()


def compute_atr(df: pd.DataFrame, length: int) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    # RMA de Wilder == EMA con alpha=1/length
    return tr.ewm(alpha=1.0 / length, adjust=False, min_periods=length).mean()


def compute_signal(df: pd.DataFrame, params) -> dict | None:
    """
    df: DataFrame con columnas open/high/low/close/volume, ordenado
        cronológicamente ascendente, SOLO con velas cerradas.
    params: objeto Config (o similar) con los atributos
        LOOKBACK_ENERGY, K_DOMINANCE, USE_VOL_FILTER, VOL_LEN, VOL_MULT.

    Devuelve None si aún no hay suficiente histórico en `df` para que
    los cálculos rolling sean válidos (equivalente al warm-up de
    Pine). Si no, devuelve un dict con el estado de la última vela
    cerrada: is_trending, long_cond, short_cond, close, approx, atr_ready.
    """
    min_len = params.LOOKBACK_ENERGY + 16 + 2  # energía + escala más ancha (8) + margen
    if len(df) < min_len:
        return None

    src = df["close"]

    h1 = _haar_detail(src, 1)
    h2 = _haar_detail(src, 2)
    h4 = _haar_detail(src, 4)
    h8 = _haar_detail(src, 8)

    e1 = _rolling_energy(h1, params.LOOKBACK_ENERGY)
    e2 = _rolling_energy(h2, params.LOOKBACK_ENERGY)
    e4 = _rolling_energy(h4, params.LOOKBACK_ENERGY)
    e8 = _rolling_energy(h8, params.LOOKBACK_ENERGY)

    fine = e1 + e2
    coarse = e4 + e8

    approx = src.rolling(8).mean()

    last = len(df) - 1
    prev = last - 1
    if pd.isna(coarse.iloc[last]) or pd.isna(fine.iloc[last]) or pd.isna(approx.iloc[prev]):
        return None  # warm-up: todavía no hay ventana completa

    is_trending = bool(coarse.iloc[last] > params.K_DOMINANCE * fine.iloc[last])

    crossover = src.iloc[prev] <= approx.iloc[prev] and src.iloc[last] > approx.iloc[last]
    crossunder = src.iloc[prev] >= approx.iloc[prev] and src.iloc[last] < approx.iloc[last]

    if params.USE_VOL_FILTER:
        vol_sma = df["volume"].rolling(params.VOL_LEN).mean()
        vol_ok = bool(df["volume"].iloc[last] > vol_sma.iloc[last] * params.VOL_MULT) if not pd.isna(vol_sma.iloc[last]) else False
    else:
        vol_ok = True

    h8_last = h8.iloc[last]
    long_cond = is_trending and vol_ok and crossover and (h8_last > 0)
    short_cond = is_trending and vol_ok and crossunder and (h8_last < 0)

    return {
        "time": int(df["time"].iloc[last]) if "time" in df.columns else None,
        "close": float(src.iloc[last]),
        "approx": float(approx.iloc[last]),
        "is_trending": is_trending,
        "h8": float(h8_last) if not pd.isna(h8_last) else 0.0,
        "long_cond": long_cond,
        "short_cond": short_cond,
    }
