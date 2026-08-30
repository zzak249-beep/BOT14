"""
Tests de strategy.py: verifican que la traducción a Python de la lógica del
Pine Script se comporta correctamente, incluyendo una comparación contra
una simulación independiente escrita por separado (no solo releer el mismo
código) y, cuando está disponible, contra pandas_ta.
"""

import numpy as np
import pandas as pd
import pytest

from strategy import compute_rsi, compute_signals, rma, supertrend


@pytest.fixture
def synthetic_df():
    np.random.seed(42)
    n = 400
    ret = np.random.normal(0, 0.004, n)
    close = 100 * np.exp(np.cumsum(ret))
    high = close * (1 + np.abs(np.random.normal(0, 0.002, n)))
    low = close * (1 - np.abs(np.random.normal(0, 0.002, n)))
    open_ = close * (1 + np.random.normal(0, 0.001, n))
    return pd.DataFrame({"open": open_, "high": high, "low": low, "close": close})


def test_rsi_bounded_between_0_and_100(synthetic_df):
    rsi = compute_rsi(synthetic_df["close"], 10)
    assert rsi.dropna().between(0, 100).all()


def test_rsi_matches_pandas_ta_after_warmup(synthetic_df):
    pta = pytest.importorskip("pandas_ta")
    mine = compute_rsi(synthetic_df["close"], 10)
    theirs = pta.rsi(synthetic_df["close"], length=10)
    # Tras el calentamiento (la semilla de RMA deja de influir), deben coincidir
    # prácticamente a precisión de punto flotante.
    tail_diff = (mine.tail(50) - theirs.tail(50)).abs().max()
    assert tail_diff < 1e-6


def test_special_buy_matches_independent_reimplementation(synthetic_df):
    """Traduce el pseudocódigo Pine línea por línea en un bucle separado
    (sin reutilizar compute_signals) y compara vela a vela."""
    rsi = compute_rsi(synthetic_df["close"], 10)
    rsi_signal = rsi.rolling(10).mean()
    trigger, target = 50.0, 2

    cc = 0
    reference = []
    for i in range(len(synthetic_df)):
        r, rs = rsi.iloc[i], rsi_signal.iloc[i]
        if pd.isna(r) or pd.isna(rs):
            reference.append(False)
            continue
        bull = (
            i > 0
            and not pd.isna(rsi.iloc[i - 1])
            and not pd.isna(rsi_signal.iloc[i - 1])
            and rsi.iloc[i - 1] <= rsi_signal.iloc[i - 1]
            and r > rs
        )
        if r > trigger:
            cc = 0
        if bull and r < trigger:
            cc += 1
        sb = bull and (r < trigger) and (cc == target)
        if sb:
            cc = 0
        reference.append(sb)

    out = compute_signals(synthetic_df)
    assert (pd.Series(reference) == out["special_buy"]).all()


def test_supertrend_has_no_nan_propagation_bug(synthetic_df):
    st_line, trend_up = supertrend(synthetic_df, period=10, multiplier=2.5)
    # Tras el periodo de calentamiento no debe quedar ningún NaN "atascado".
    warmed_up = st_line.iloc[30:]
    assert warmed_up.notna().all()


def test_supertrend_flips_direction_at_least_once(synthetic_df):
    _st_line, trend_up = supertrend(synthetic_df, period=10, multiplier=2.5)
    flips = (trend_up.dropna().astype(bool).diff().fillna(False)).sum()
    assert flips > 0, "En 400 velas de un random walk, el SuperTrend debería cambiar de tendencia al menos una vez"


def test_rma_seed_is_simple_average_of_first_window():
    s = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], dtype="float64")
    out = rma(s, length=5)
    assert pd.isna(out.iloc[:4]).all()
    assert out.iloc[4] == pytest.approx(s.iloc[:5].mean())


def test_compute_signals_output_has_expected_columns(synthetic_df):
    out = compute_signals(synthetic_df)
    expected = {"close", "rsi", "rsi_signal", "bull_cross", "cross_count", "special_buy", "supertrend", "trend_up", "sell_signal"}
    assert expected.issubset(set(out.columns))
    assert len(out) == len(synthetic_df)
