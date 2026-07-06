"""
Tests sintéticos para cvd_filter.py — sin red.
Correr: python3 test_cvd_filter.py
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cvd_filter import compute_cvd, confirms_direction


def mk(o, h, l, c, v, t=0):
    return {"open": o, "high": h, "low": l, "close": c, "volume": v, "time": t}


class Cfg:
    ENABLE_CVD_FILTER = True
    CVD_LOOKBACK = 5


def test_compute_cvd_bullish():
    # 5 velas verdes de volumen 10 + 1 en formación (se descarta) -> CVD = +50
    candles = [mk(1, 2, 0, 2, 10) for _ in range(5)] + [mk(2, 2.1, 1.9, 2.05, 3)]
    result = compute_cvd(candles, lookback=5)
    assert result["cvd"] == 50.0, result
    assert result["bullish"] is True and result["bearish"] is False
    assert result["bars_used"] == 5
    print("OK  test_compute_cvd_bullish")


def test_compute_cvd_bearish():
    candles = [mk(2, 2, 0, 1, 10) for _ in range(5)] + [mk(1, 1.1, 0.9, 1.0, 3)]
    result = compute_cvd(candles, lookback=5)
    assert result["cvd"] == -50.0, result
    assert result["bearish"] is True and result["bullish"] is False
    print("OK  test_compute_cvd_bearish")


def test_compute_cvd_mixed_and_lookback_window():
    # 3 verdes (vol 10) + 3 rojas (vol 4) + 1 en formación; lookback=5 sobre
    # las 6 CERRADAS descarta solo la más vieja (verde), quedando
    # 2 verdes + 3 rojas -> (2*10) - (3*4) = 20 - 12 = +8
    candles = (
        [mk(1, 2, 0, 2, 10) for _ in range(3)]
        + [mk(2, 2, 0, 1, 4) for _ in range(3)]
        + [mk(1, 1.1, 0.9, 1.0, 99)]  # en formación, no debe contar
    )
    result = compute_cvd(candles, lookback=5)
    assert result["bars_used"] == 5
    assert result["cvd"] == 8.0, result
    print("OK  test_compute_cvd_mixed_and_lookback_window")


def test_compute_cvd_insufficient_data():
    result = compute_cvd([], lookback=5)
    assert result["bars_used"] == 0
    result2 = compute_cvd([mk(1, 1, 1, 1, 1)], lookback=5)  # solo 1 vela -> nada "cerrado"
    assert result2["bars_used"] == 0
    print("OK  test_compute_cvd_insufficient_data")


def test_confirms_direction_long_and_short():
    cfg = Cfg()
    bullish_candles = [mk(1, 2, 0, 2, 10) for _ in range(6)]
    bearish_candles = [mk(2, 2, 0, 1, 10) for _ in range(6)]

    r_long_ok = confirms_direction(bullish_candles, "LONG", cfg)
    assert r_long_ok["confirms"] is True, r_long_ok

    r_long_bad = confirms_direction(bearish_candles, "LONG", cfg)
    assert r_long_bad["confirms"] is False, r_long_bad

    r_short_ok = confirms_direction(bearish_candles, "SHORT", cfg)
    assert r_short_ok["confirms"] is True, r_short_ok

    r_short_bad = confirms_direction(bullish_candles, "SHORT", cfg)
    assert r_short_bad["confirms"] is False, r_short_bad
    print("OK  test_confirms_direction_long_and_short")


def test_confirms_direction_no_data_does_not_block():
    cfg = Cfg()
    r = confirms_direction([], "LONG", cfg)
    assert r["confirms"] is None, "Sin datos no debería bloquear (confirms=None, no False)"
    print("OK  test_confirms_direction_no_data_does_not_block")


if __name__ == "__main__":
    tests = [
        test_compute_cvd_bullish,
        test_compute_cvd_bearish,
        test_compute_cvd_mixed_and_lookback_window,
        test_compute_cvd_insufficient_data,
        test_confirms_direction_long_and_short,
        test_confirms_direction_no_data_does_not_block,
    ]
    failed = 0
    for t in tests:
        try:
            t()
        except AssertionError as e:
            failed += 1
            print(f"FAIL {t.__name__}: {e}")
        except Exception as e:
            failed += 1
            print(f"ERROR {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} tests OK")
    sys.exit(1 if failed else 0)
