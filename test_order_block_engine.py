"""
Tests sintéticos para order_block_engine.py — sin llamadas de red, mismo
espíritu que tests/test_unicorn_model.py: datos fabricados a mano, valida
lógica pura de cada pieza del motor.

Correr: python3 test_order_block_engine.py
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from order_block_engine import (
    _compute_trend_atr_series,
    _confirmed_pivots,
    _pivot_volume_ratio,
    _simulate,
    _retest_at,
    get_signal,
)


class Cfg:
    ST_LEN = 10
    ST_MULT = 2.0
    OB_PIVOT_LEN = 2
    OB_MIN_BUY_PCT = 50.0
    OB_MIN_SELL_PCT = 50.0
    OB_DELETE_ON_BREAK = True
    DIRECTION = "BOTH"
    OB_RR = 1.5
    OB_SL_ATR_BUFFER = 0.2


def mk(o, h, l, c, v, t=0):
    return {"open": o, "high": h, "low": l, "close": c, "volume": v, "time": t}


# ─────────────────────────────────────────────────────────────────────────
def test_trend_flip():
    cfg = Cfg()
    candles = [mk(100, 100.5, 99.5, 100, 10, i) for i in range(cfg.ST_LEN + 2)]

    price = 100.0
    for _ in range(30):  # tendencia bajista sostenida -> debe flipear a -1
        price -= 3.0
        candles.append(mk(price + 1, price + 1.2, price - 1.2, price, 10.0, len(candles)))

    for _ in range(30):  # tendencia alcista sostenida -> debe volver a flipear a 1
        price += 3.0
        candles.append(mk(price - 1, price + 1.2, price - 1.2, price, 10.0, len(candles)))

    trend_arr, atr_arr = _compute_trend_atr_series(candles, cfg.ST_LEN, cfg.ST_MULT)
    assert -1 in trend_arr, "Se esperaba al menos un tramo bajista"
    assert trend_arr[-1] == 1, f"Se esperaba trend final alcista, fue {trend_arr[-1]}"
    print("OK  test_trend_flip")


def test_pivot_detection():
    pivot_len = 2
    candles = [mk(10, 10.5, 9.5, 10, 5) for _ in range(2 * pivot_len + 1)]
    # low mucho menor en el centro; high ligeramente MENOR que el resto para
    # que no empate también como pivote de máximo (ver nota de aproximación
    # por empates en el docstring del módulo)
    candles[pivot_len] = mk(10, 10.2, 5.0, 10, 5)
    confirmed_lo, confirmed_hi = _confirmed_pivots(candles, pivot_len)
    assert confirmed_lo == {2 * pivot_len}, f"Esperaba pivote lo en {2*pivot_len}, obtuve {confirmed_lo}"
    assert confirmed_hi == set(), f"No se esperaba pivote hi, obtuve {confirmed_hi}"
    print("OK  test_pivot_detection")


def test_volume_ratio():
    pivot_len = 2
    candles = [
        mk(1, 2, 0, 2, 10),   # verde (close>=open) -> compra
        mk(2, 2, 0, 1, 5),    # roja -> venta
        mk(1, 2, 0, 2, 10),   # verde -> compra
    ]
    ratio = _pivot_volume_ratio(candles, 0, pivot_len)
    expected = 20 / 25
    assert abs(ratio - expected) < 1e-9, f"Esperaba {expected}, obtuve {ratio}"
    print("OK  test_volume_ratio")


def test_retest_boolean_logic():
    """Testea _retest_at de forma aislada, con historia/trend fabricados
    a mano (sin depender de que _simulate produzca el escenario)."""
    cfg = Cfg()
    candles = [
        mk(100, 101, 99, 100.5, 10, 0),
        mk(100.5, 100.8, 94.0, 95.0, 10, 1),   # low toca/perfora el top del OB (95 <= 96)
        mk(95.0, 97.5, 94.5, 97.0, 10, 2),      # low=94.5 <= 96 (aún dentro/tocando)
        mk(97.0, 99.0, 96.5, 98.5, 10, 3),      # low=96.5 > 96 -> cruce hacia arriba
    ]
    trend_arr = [1, 1, 1, 1]
    # Order Block activo con top=96, bottom=90, buy_ratio alto (0.8)
    history = [
        {"top": None, "bottom": None, "buy_ratio": None, "ob_trend": 0},
        {"top": 96.0, "bottom": 90.0, "buy_ratio": 0.8, "ob_trend": 1},
        {"top": 96.0, "bottom": 90.0, "buy_ratio": 0.8, "ob_trend": 1},
        {"top": 96.0, "bottom": 90.0, "buy_ratio": 0.8, "ob_trend": 1},
    ]
    confirmed_lo, confirmed_hi = set(), set()

    # Barra 2: low[1]=94.0<=96 y low[2]=94.5... no cruza (94.5 no es >96)
    buy2, _ = _retest_at(2, candles, trend_arr, history, confirmed_lo, confirmed_hi, cfg)
    assert buy2 is False, "No debería haber retest todavía (low sigue por debajo del top)"

    # Barra 3: low[2]=94.5<=96 y low[3]=96.5>96 -> cruce confirmado
    buy3, _ = _retest_at(3, candles, trend_arr, history, confirmed_lo, confirmed_hi, cfg)
    assert buy3 is True, "Se esperaba buy_retest en la barra 3"

    # Si el buy_ratio no llega al umbral, no debe confirmar aunque haya cruce
    history_low_ratio = [dict(h) for h in history]
    history_low_ratio[3]["buy_ratio"] = 0.3
    buy3_low, _ = _retest_at(3, candles, trend_arr, history_low_ratio, confirmed_lo, confirmed_hi, cfg)
    assert buy3_low is False, "Con buy_ratio bajo el umbral no debería confirmar"

    # Si hubo cambio de tendencia en esa barra, tampoco debe confirmar
    trend_arr_change = [1, 1, -1, 1]
    buy3_change, _ = _retest_at(3, candles, trend_arr_change, history, confirmed_lo, confirmed_hi, cfg)
    assert buy3_change is False, "marketChange en la barra anterior debería suprimir el retest"

    # Si en esa misma barra se acaba de confirmar un pivote de mínimo, se suprime (anti-repintado)
    buy3_pivot, _ = _retest_at(3, candles, trend_arr, history, {3}, confirmed_hi, cfg)
    assert buy3_pivot is False, "pivot_low confirmado en la misma barra debería suprimir buy_retest"

    print("OK  test_retest_boolean_logic")


def test_invalidation_breaks_ob():
    cfg = Cfg()
    candles = [
        mk(100, 101, 99, 100, 10, 0),
    ]
    trend_arr = [1]
    confirmed_lo, confirmed_hi = set(), set()

    # Simulamos manualmente 2 barras: una con OB activo (top=96, bottom=90),
    # la siguiente con high=89 (< bottom) -> debería invalidarse dentro de _simulate.
    # Para testear _simulate real, construimos velas que fuercen justo ese caso.
    pivot_len = cfg.OB_PIVOT_LEN
    st_len = cfg.ST_LEN
    seq = [mk(100, 100.5, 99.5, 100, 10, i) for i in range(st_len + 2)]

    price = 100.0
    for _ in range(12):  # tendencia alcista para asentar market_trend=1
        price += 1.0
        seq.append(mk(price - 0.5, price + 0.3, price - 0.7, price, 10.0, len(seq)))

    # pivote de mínimo claro
    pivot_low_price = price - 3.0
    seq.append(mk(pivot_low_price + 0.2, pivot_low_price + 0.8, pivot_low_price - 2.0,
                   pivot_low_price + 0.1, 12.0, len(seq)))
    for i in range(pivot_len):
        p = pivot_low_price + 1.5 + i
        seq.append(mk(p - 0.3, p + 0.5, p - 0.2, p + 0.3, 10.0, len(seq)))

    trend_arr, atr_arr, history, c_lo, c_hi = _simulate(seq, cfg)
    ob_formed_idx = next((i for i, h in enumerate(history) if h["top"] is not None), None)
    assert ob_formed_idx is not None, "Se esperaba que se formara un Order Block alcista"
    top_before = history[ob_formed_idx]["top"]
    bottom_before = history[ob_formed_idx]["bottom"]

    # Ahora forzamos una vela que rompa completamente por debajo del bottom
    crash_price = bottom_before - 5.0
    seq.append(mk(crash_price + 0.5, crash_price + 0.8, crash_price - 0.5, crash_price, 10.0, len(seq)))

    trend_arr2, atr_arr2, history2, c_lo2, c_hi2 = _simulate(seq, cfg)
    assert history2[-1]["top"] is None, "El Order Block debería invalidarse tras la ruptura completa"
    print("OK  test_invalidation_breaks_ob  (OB previo top=%.4f bottom=%.4f)" % (top_before, bottom_before))


class CfgWide(Cfg):
    # Banda de Supertrend más ancha (mult mayor) a propósito: da margen para
    # que el precio retroceda hasta el Order Block sin que la tendencia
    # flipee antes de completar el retest — con mult=3.5 (default real, ya
    # más ancho que el mult=2.0 de Cfg) un retroceso hacia un OB reciente
    # rara vez alcanza a invalidar el Supertrend en el mismo tramo.
    ST_MULT = 6.0


def _build_uptrend_with_ob(cfg, pivot_body_green, continuation_bars=3):
    """
    Construye: warm-up plano -> tendencia alcista suave (el close SIEMPRE
    sube, así nunca hay flip de tendencia por una caída de close) -> vela
    pivote con mecha profunda (crea el pivote de mínimo) y cuerpo
    verde/rojo según `pivot_body_green` -> continuación alcista corta.
    Devuelve (candles, price_final, ob_top, ob_bottom, buy_ratio) del Order
    Block activo resultante.
    """
    candles = [mk(100, 100.5, 99.5, 100, 10, i) for i in range(cfg.ST_LEN + 2)]

    price = 100.0
    for _ in range(12):
        price += 1.2
        candles.append(mk(price - 1.0, price + 0.3, price - 1.2, price, 10.0, len(candles)))

    price += 0.3
    if pivot_body_green:
        candles.append(mk(price - 1.0, price + 0.3, price - 5.0, price, 20.0, len(candles)))
    else:
        candles.append(mk(price + 1.0, price + 1.3, price - 5.0, price, 20.0, len(candles)))

    for _ in range(cfg.OB_PIVOT_LEN):
        price += 0.3
        if pivot_body_green:
            candles.append(mk(price - 1.0, price + 0.3, price - 1.2, price, 18.0, len(candles)))
        else:
            candles.append(mk(price + 1.0, price + 1.3, price - 1.2, price, 18.0, len(candles)))

    for _ in range(continuation_bars):
        price += 1.0
        candles.append(mk(price - 1.0, price + 0.3, price - 1.2, price, 10.0, len(candles)))

    trend_arr, atr_arr, history, c_lo, c_hi = _simulate(candles, cfg)
    assert trend_arr[-1] == 1, "La tendencia no debería haber flipeado durante la construcción"
    last_ob_idx = max(i for i in range(len(history)) if history[i]["top"] is not None)
    ob = history[last_ob_idx]
    return candles, price, ob["top"], ob["bottom"], ob["buy_ratio"]


def _append_retest(candles, price, top):
    """Añade una vela de dip (cierra bajo el top) y una de retest (cruza de
    vuelta por encima) más una vela final aún 'en formación'."""
    dip_close = top - 0.5
    candles.append(mk(price, price + 0.2, dip_close - 0.3, dip_close, 10.0, len(candles)))
    back_close = top + 1.0
    candles.append(mk(dip_close, back_close + 0.3, top + 0.1, back_close, 10.0, len(candles)))
    candles.append(mk(back_close, back_close + 0.1, back_close - 0.1, back_close, 5.0, len(candles)))
    return candles


def test_get_signal_end_to_end_long():
    """Escenario natural completo: tendencia alcista, se forma un OB alcista
    con alto ratio comprador, retroceso y retest -> debe devolver LONG."""
    cfg = CfgWide()
    candles, price, ob_top, ob_bottom, buy_ratio = _build_uptrend_with_ob(cfg, pivot_body_green=True)
    assert buy_ratio >= 0.5, f"Se esperaba buy_ratio alto en el pivote, fue {buy_ratio}"

    candles = _append_retest(candles, price, ob_top)

    result = get_signal(candles, cfg)
    assert result["signal"] == "LONG", f"Se esperaba señal LONG, resultado: {result}"
    assert result["sl_price"] < result["entry_price"] < result["tp_price"], (
        f"SL/entry/TP inconsistentes: {result['sl_price']} / {result['entry_price']} / {result['tp_price']}"
    )
    print("OK  test_get_signal_end_to_end_long  (entry=%.4f sl=%.4f tp=%.4f buy_ratio=%.2f)"
          % (result["entry_price"], result["sl_price"], result["tp_price"], buy_ratio))


def test_get_signal_rejects_low_volume_retest():
    """Mismo escenario pero con volumen vendedor dominante en el pivote ->
    NO debe confirmar el retest aunque el precio cruce igual (el ratio de
    volumen es justo lo que exige la estrategia para 'confirmar' la entrada)."""
    cfg = CfgWide()
    candles, price, ob_top, ob_bottom, buy_ratio = _build_uptrend_with_ob(cfg, pivot_body_green=False)
    assert buy_ratio < 0.5, f"Se esperaba buy_ratio bajo en este escenario, fue {buy_ratio}"

    candles = _append_retest(candles, price, ob_top)

    result = get_signal(candles, cfg)
    assert result["signal"] is None, f"No debería confirmar con buy_ratio bajo, resultado: {result}"
    print("OK  test_get_signal_rejects_low_volume_retest  (buy_ratio=%.2f)" % buy_ratio)


if __name__ == "__main__":
    tests = [
        test_trend_flip,
        test_pivot_detection,
        test_volume_ratio,
        test_retest_boolean_logic,
        test_invalidation_breaks_ob,
        test_get_signal_end_to_end_long,
        test_get_signal_rejects_low_volume_retest,
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
