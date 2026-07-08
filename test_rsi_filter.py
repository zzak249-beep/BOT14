"""Tests sintéticos para rsi_filter.py — sin red."""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rsi_filter import compute_rsi, confirms_direction


class Cfg:
    RSI_LENGTH = 14


def mk(c, t=0):
    return {"open": c, "high": c, "low": c, "close": c, "volume": 10, "time": t}


def test_compute_rsi_all_gains_is_100():
    # Serie estrictamente creciente -> sin pérdidas -> RSI = 100
    candles = [mk(100 + i) for i in range(20)] + [mk(200)]  # última en formación
    rsi = compute_rsi(candles, length=14)
    assert rsi == 100.0, rsi
    print("OK  test_compute_rsi_all_gains_is_100")


def test_compute_rsi_all_losses_is_0():
    candles = [mk(200 - i) for i in range(20)] + [mk(0)]
    rsi = compute_rsi(candles, length=14)
    assert rsi == 0.0, rsi
    print("OK  test_compute_rsi_all_losses_is_0")


def test_compute_rsi_flat_is_50_ish():
    # Precio plano -> sin ganancias ni pérdidas -> avg_gain=avg_loss=0 -> caso especial (100.0 por avg_loss==0)
    # En cambio con MUY pequeñas fluctuaciones simétricas debería rondar 50
    prices = []
    p = 100.0
    for i in range(20):
        p += 1.0 if i % 2 == 0 else -1.0
        prices.append(p)
    candles = [mk(p, i) for i, p in enumerate(prices)] + [mk(prices[-1] + 0.01)]
    rsi = compute_rsi(candles, length=14)
    assert 40 < rsi < 60, f"Esperaba RSI cerca de 50 con oscilación simétrica, fue {rsi}"
    print(f"OK  test_compute_rsi_flat_is_50_ish (rsi={rsi:.1f})")


def test_compute_rsi_insufficient_data():
    candles = [mk(100 + i) for i in range(5)]
    rsi = compute_rsi(candles, length=14)
    assert rsi is None
    print("OK  test_compute_rsi_insufficient_data")


def test_confirms_direction_long_and_short():
    cfg = Cfg()
    bullish = [mk(100 + i) for i in range(20)] + [mk(200)]
    bearish = [mk(200 - i) for i in range(20)] + [mk(0)]

    r_long_ok = confirms_direction(bullish, "LONG", cfg)
    assert r_long_ok["confirms"] is True, r_long_ok

    r_long_bad = confirms_direction(bearish, "LONG", cfg)
    assert r_long_bad["confirms"] is False, r_long_bad

    r_short_ok = confirms_direction(bearish, "SHORT", cfg)
    assert r_short_ok["confirms"] is True, r_short_ok

    r_short_bad = confirms_direction(bullish, "SHORT", cfg)
    assert r_short_bad["confirms"] is False, r_short_bad
    print("OK  test_confirms_direction_long_and_short")


def test_confirms_direction_no_data_does_not_block():
    cfg = Cfg()
    r = confirms_direction([mk(100)], "LONG", cfg)
    assert r["confirms"] is None
    print("OK  test_confirms_direction_no_data_does_not_block")


if __name__ == "__main__":
    tests = [
        test_compute_rsi_all_gains_is_100,
        test_compute_rsi_all_losses_is_0,
        test_compute_rsi_flat_is_50_ish,
        test_compute_rsi_insufficient_data,
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
