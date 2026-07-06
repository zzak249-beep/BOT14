"""
Tests sintéticos para order_book_imbalance.py — sin red.
Correr: python3 test_order_book_imbalance.py
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from order_book_imbalance import compute_obi, confirms_direction


class Cfg:
    ENABLE_OBI_FILTER = True
    OBI_LEVELS = 20
    OBI_THRESHOLD = 0.15


def mk_book(bid_qtys, ask_qtys, start_price=100.0):
    bids = [[start_price - i * 0.1, q] for i, q in enumerate(bid_qtys)]
    asks = [[start_price + i * 0.1, q] for i, q in enumerate(ask_qtys)]
    return {"bids": bids, "asks": asks}


def test_compute_obi_bullish():
    book = mk_book([10, 10, 10], [2, 2, 2])
    result = compute_obi(book, levels=20)
    expected = (30 - 6) / 36
    assert abs(result["obi"] - expected) < 1e-9
    assert result["bullish"] is True and result["bearish"] is False
    print("OK  test_compute_obi_bullish")


def test_compute_obi_bearish():
    book = mk_book([2, 2, 2], [10, 10, 10])
    result = compute_obi(book, levels=20)
    assert result["bearish"] is True and result["bullish"] is False
    print("OK  test_compute_obi_bearish")


def test_compute_obi_respects_levels():
    # 5 niveles de cada lado, pero levels=2 -> solo deben contar los primeros 2
    book = mk_book([100, 100, 1, 1, 1], [1, 1, 100, 100, 100])
    result_full = compute_obi(book, levels=20)
    result_top2 = compute_obi(book, levels=2)
    assert result_top2["bid_vol"] == 200 and result_top2["ask_vol"] == 2
    assert result_top2["bullish"] is True
    # Con todos los niveles, el desequilibrio se diluye/invierte
    assert result_full["bid_vol"] == 203 and result_full["ask_vol"] == 302
    print("OK  test_compute_obi_respects_levels")


def test_compute_obi_empty_book():
    result = compute_obi({}, levels=20)
    assert result["obi"] == 0.0
    result2 = compute_obi(None, levels=20)
    assert result2["obi"] == 0.0
    print("OK  test_compute_obi_empty_book")


def test_confirms_direction_long_and_short():
    cfg = Cfg()
    bullish_book = mk_book([10, 10, 10], [2, 2, 2])
    bearish_book = mk_book([2, 2, 2], [10, 10, 10])

    r_long_ok = confirms_direction(bullish_book, "LONG", cfg)
    assert r_long_ok["confirms"] is True, r_long_ok

    r_long_bad = confirms_direction(bearish_book, "LONG", cfg)
    assert r_long_bad["confirms"] is False, r_long_bad

    r_short_ok = confirms_direction(bearish_book, "SHORT", cfg)
    assert r_short_ok["confirms"] is True, r_short_ok

    r_short_bad = confirms_direction(bullish_book, "SHORT", cfg)
    assert r_short_bad["confirms"] is False, r_short_bad
    print("OK  test_confirms_direction_long_and_short")


def test_confirms_direction_below_threshold():
    cfg = Cfg()
    # Desequilibrio leve (obi ~0.09), por debajo del umbral 0.15 -> no confirma
    book = mk_book([11, 11, 11], [9, 9, 9])
    result = confirms_direction(book, "LONG", cfg)
    assert result["confirms"] is False, result
    print("OK  test_confirms_direction_below_threshold  (obi=%.3f)" % result["obi"])


def test_confirms_direction_no_book_does_not_block():
    cfg = Cfg()
    result = confirms_direction({}, "LONG", cfg)
    assert result["confirms"] is None, "Sin order book no debería bloquear (confirms=None, no False)"
    print("OK  test_confirms_direction_no_book_does_not_block")


if __name__ == "__main__":
    tests = [
        test_compute_obi_bullish,
        test_compute_obi_bearish,
        test_compute_obi_respects_levels,
        test_compute_obi_empty_book,
        test_confirms_direction_long_and_short,
        test_confirms_direction_below_threshold,
        test_confirms_direction_no_book_does_not_block,
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
