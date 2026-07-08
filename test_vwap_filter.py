"""Tests sintéticos para vwap_filter.py — sin red."""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from vwap_filter import compute_vwap, confirms_direction


class Cfg:
    pass


def mk(o, h, l, c, v, t):
    return {"open": o, "high": h, "low": l, "close": c, "volume": v, "time": t}


DAY_MS = 86_400_000


def test_compute_vwap_simple_average():
    # 2 velas del mismo día, mismo volumen -> VWAP = promedio simple de los típicos
    candles = [
        mk(100, 102, 98, 100, 10, 0),      # típico = (102+98+100)/3 = 100
        mk(100, 106, 94, 100, 10, 60_000), # típico = (106+94+100)/3 = 100
        mk(100, 100, 100, 100, 1, 120_000),  # última, en formación, no cuenta
    ]
    vwap = compute_vwap(candles)
    assert abs(vwap - 100.0) < 1e-6, vwap
    print("OK  test_compute_vwap_simple_average")


def test_compute_vwap_weights_by_volume():
    candles = [
        mk(90, 90, 90, 90, 100, 0),    # típico=90, volumen grande
        mk(110, 110, 110, 110, 1, 60_000),  # típico=110, volumen chico
        mk(100, 100, 100, 100, 1, 120_000),  # en formación
    ]
    vwap = compute_vwap(candles)
    # Debería estar mucho más cerca de 90 que de 110, por el peso del volumen
    assert vwap < 91, f"Esperaba VWAP cerca de 90 (volumen dominante), fue {vwap}"
    print(f"OK  test_compute_vwap_weights_by_volume (vwap={vwap:.4f})")


def test_compute_vwap_resets_each_day():
    # Vela del día 1 con precio muy alto, después el día 2 empieza de cero
    candles = [
        mk(1000, 1000, 1000, 1000, 100, 0),           # día 1
        mk(100, 100, 100, 100, 10, DAY_MS),            # día 2, primera vela cerrada
        mk(100, 100, 100, 100, 1, DAY_MS + 60_000),    # día 2, en formación
    ]
    vwap = compute_vwap(candles)
    # Si NO reiniciara por día, el 1000 del día 1 arrastraría el promedio muy arriba
    assert vwap < 105, f"El VWAP debería haber reiniciado en el día 2, fue {vwap}"
    print(f"OK  test_compute_vwap_resets_each_day (vwap={vwap:.4f})")


def test_confirms_direction_long_and_short():
    cfg = Cfg()
    above = [
        mk(90, 90, 90, 90, 10, 0),
        mk(90, 90, 90, 90, 10, 60_000),
        mk(120, 120, 120, 120, 1, 120_000),  # última (en formación) irrelevante, se usa candles[-2]
    ]
    below = [
        mk(90, 90, 90, 90, 10, 0),
        mk(90, 90, 90, 90, 10, 60_000),
        mk(60, 60, 60, 60, 1, 120_000),
    ]
    # candles[-2] (última cerrada) = 90 en ambos casos, VWAP ~90 -> confirms depende de igualdad exacta
    # Ajustamos: la penúltima vela ES la que se usa para "precio actual", construyamos casos más claros
    above2 = [
        mk(80, 80, 80, 80, 10, 0),
        mk(120, 120, 120, 120, 10, 60_000),  # candles[-2] = 120, arriba del VWAP (~100)
        mk(999, 999, 999, 999, 1, 120_000),
    ]
    below2 = [
        mk(120, 120, 120, 120, 10, 0),
        mk(80, 80, 80, 80, 10, 60_000),  # candles[-2] = 80, debajo del VWAP (~100)
        mk(999, 999, 999, 999, 1, 120_000),
    ]

    r_long_ok = confirms_direction(above2, "LONG", cfg)
    assert r_long_ok["confirms"] is True, r_long_ok

    r_long_bad = confirms_direction(below2, "LONG", cfg)
    assert r_long_bad["confirms"] is False, r_long_bad

    r_short_ok = confirms_direction(below2, "SHORT", cfg)
    assert r_short_ok["confirms"] is True, r_short_ok

    r_short_bad = confirms_direction(above2, "SHORT", cfg)
    assert r_short_bad["confirms"] is False, r_short_bad
    print("OK  test_confirms_direction_long_and_short")


def test_confirms_direction_no_data_does_not_block():
    cfg = Cfg()
    r = confirms_direction([], "LONG", cfg)
    assert r["confirms"] is None
    print("OK  test_confirms_direction_no_data_does_not_block")


if __name__ == "__main__":
    tests = [
        test_compute_vwap_simple_average,
        test_compute_vwap_weights_by_volume,
        test_compute_vwap_resets_each_day,
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
