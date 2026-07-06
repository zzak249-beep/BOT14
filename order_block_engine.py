"""
Order Block Engine (BigBeluga) — Motor de entrada por Order Blocks + Volumen
================================================================================
Réplica funcional completa de "Volume-Trend Order Block Engine [BigBeluga]"
(Pine Script v6, © BigBeluga, CC BY-NC-SA 4.0) — no solo el Supertrend que ya
vive en supertrend_engine.py, sino el motor completo:

  1. Tendencia (Custom Supertrend, mismos ST_LEN/ST_MULT que supertrend_engine.py)
  2. Order Blocks por pivote (OB_PIVOT_LEN barras a cada lado) en la dirección
     de la tendencia — en tendencia alcista busca pivotes de mínimo, en
     tendencia bajista busca pivotes de máximo
  3. Ratio de volumen comprador/vendedor de la ventana del pivote (desde la
     vela pivote hasta la vela de confirmación, ambas incluidas)
  4. Señal de "retest": el precio vuelve a tocar el borde del Order Block
     activo y rebota — SOLO se acepta si el ratio de volumen de ESE Order
     Block supera el umbral configurado. Así es como el volumen "confirma"
     la entrada, igual que en el indicador original.
  5. Invalidación: si el precio rompe por completo el Order Block activo,
     se descarta y deja de generar señales de retest.

Es una simulación barra-por-barra fiel a la máquina de estados de Pine (los
`var` de Pine son exactamente eso: estado que persiste entre barras). No se
vectoriza para no perder fidelidad — un Order Block activo persiste hasta
que aparece uno nuevo no solapado, o hasta que se invalida.

APROXIMACIONES CONOCIDAS frente al Pine original (documentadas, no bugs
ocultos — mismo espíritu que las notas de exchange_client.py):
  - Pivote (ta.pivotlow/pivothigh): un candidato se marca pivote si su
    low/high es el extremo (<=/>=) de TODA la ventana
    [candidato-pivot_len, candidato+pivot_len]. Con empates exactos podría
    marcar más de una barra como pivote donde Pine marcaría solo una — sin
    impacto práctico esperado sobre precios float de cripto.
  - El guard "no repintar en la misma barra" (equivalente a `na(pivot_low)` /
    `na(pivot_high)` en las condiciones de retest de Pine) solo cubre el
    caso de que el Order Block se haya actualizado por un pivote DEL MISMO
    TIPO en esa misma barra. El caso borde de que se actualice por un
    pivote del tipo CONTRARIO en esa misma barra usa el valor YA actualizado
    de top/bottom para el cruce, en vez de la semántica exacta de Pine de
    comparar valores pre/post-actualización dentro de la misma barra. Caso
    raro, documentado por transparencia — no se espera que afecte símbolos
    reales de forma perceptible.
  - Fiel al original: el retest de compra/venta se evalúa contra el Order
    Block activo SIN importar si ese OB es de tipo alcista o bajista (el
    Pine original tampoco lo comprueba) — solo importa el ratio de volumen.
    Esto es intencional en el indicador fuente, no una omisión nuestra.

Se asume que la ÚLTIMA vela de la lista puede estar aún en formación (mismo
criterio que unicorn_model.py con candles[-2]) — toda señal se evalúa sobre
la penúltima vela (última vela CERRADA).
"""
import logging

log = logging.getLogger("order_block_engine")


# ── Tendencia + ATR (misma fórmula que supertrend_engine.py, pero
#    conservando la serie completa barra-por-barra, necesaria para el
#    resto del motor) ─────────────────────────────────────────────────────

def _compute_trend_atr_series(candles, st_len, st_mult):
    """
    Devuelve (trend_arr, atr_arr): listas paralelas a `candles`.

    Replica el orden exacto de operaciones de Pine:
      1. ratchet de trend_stop usando el market_trend AÚN NO actualizado
      2. el cruce de close se evalúa contra el trend_stop PRE-ratchet
         (trend_stop[1] de Pine, no el valor recién ratcheado)
      3. si hay flip, se sobreescribe trend_stop con la banda fresca

    NOTA: custom_atr[i] aquí incluye la barra actual (ta.sma estándar de
    Pine). supertrend_engine.py excluye la barra actual en su ventana
    (off-by-one frente a Pine) — discrepancia menor preexistente, ver
    respuesta acompañante.
    """
    n = len(candles)
    trend_arr = [1] * n
    atr_arr = [None] * n

    market_trend = 1
    trend_stop = None

    for i in range(n):
        if i < st_len - 1:
            trend_arr[i] = market_trend
            continue

        window = candles[i - st_len + 1:i + 1]
        atr_i = sum(c["high"] - c["low"] for c in window) / st_len
        atr_arr[i] = atr_i

        c = candles[i]
        hl2 = (c["high"] + c["low"]) / 2
        upper_band = hl2 + st_mult * atr_i
        lower_band = hl2 - st_mult * atr_i

        trend_stop_prev = trend_stop if trend_stop is not None else (
            lower_band if market_trend == 1 else upper_band
        )
        ratcheted = (max(lower_band, trend_stop_prev) if market_trend == 1
                     else min(upper_band, trend_stop_prev))

        if i > 0:
            prev_close = candles[i - 1]["close"]
            if c["close"] > trend_stop_prev and prev_close <= trend_stop_prev:
                market_trend = 1
                trend_stop = lower_band
            elif c["close"] < trend_stop_prev and prev_close >= trend_stop_prev:
                market_trend = -1
                trend_stop = upper_band
            else:
                trend_stop = ratcheted
        else:
            trend_stop = ratcheted

        trend_arr[i] = market_trend

    return trend_arr, atr_arr


def _confirmed_pivots(candles, pivot_len):
    """
    Devuelve (confirmed_lo, confirmed_hi): sets con el índice de barra en el
    que, en ESA barra, se confirmaría un pivote de mínimo/máximo ocurrido
    `pivot_len` barras atrás (equivalente a `not na(ta.pivotlow(...))`).
    """
    n = len(candles)
    confirmed_lo, confirmed_hi = set(), set()
    for p in range(pivot_len, n - pivot_len):
        window = candles[p - pivot_len:p + pivot_len + 1]
        if candles[p]["low"] <= min(c["low"] for c in window):
            confirmed_lo.add(p + pivot_len)
        if candles[p]["high"] >= max(c["high"] for c in window):
            confirmed_hi.add(p + pivot_len)
    return confirmed_lo, confirmed_hi


def _pivot_volume_ratio(candles, pivot_idx, pivot_len):
    """
    Replica get_window_volume_ratio(pivot_len) de Pine evaluada en la barra
    de confirmación (pivot_idx + pivot_len): ventana de pivot_len+1 velas,
    desde la vela pivote hasta la vela de confirmación (ambas incluidas).
    """
    confirm_idx = pivot_idx + pivot_len
    if confirm_idx >= len(candles):
        return 0.5
    window = candles[pivot_idx:confirm_idx + 1]
    buy_vol = sum(c["volume"] for c in window if c["close"] >= c["open"])
    sell_vol = sum(c["volume"] for c in window if c["close"] < c["open"])
    total = buy_vol + sell_vol
    return buy_vol / total if total > 0 else 0.5


def _simulate(candles, config):
    """Ejecuta la máquina de estados completa (tendencia + Order Blocks +
    invalidación) barra por barra. Devuelve todo el estado necesario para
    evaluar señales de retest en cualquier barra."""
    st_len = getattr(config, "ST_LEN", 50)
    st_mult = getattr(config, "ST_MULT", 3.5)
    pivot_len = getattr(config, "OB_PIVOT_LEN", 7)
    delete_on_break = getattr(config, "OB_DELETE_ON_BREAK", True)

    trend_arr, atr_arr = _compute_trend_atr_series(candles, st_len, st_mult)
    confirmed_lo, confirmed_hi = _confirmed_pivots(candles, pivot_len)

    n = len(candles)
    active_top = active_bot = active_buy_ratio = None
    active_ob_trend = 0
    history = []

    for i in range(n):
        atr_i = atr_arr[i]
        trend_i = trend_arr[i]

        if atr_i is not None:
            if trend_i == 1 and i in confirmed_lo:
                pivot_idx = i - pivot_len
                ob_top = min(candles[pivot_idx]["open"], candles[pivot_idx]["close"])
                ob_bot = ob_top - atr_i
                overlapping = (
                    active_top is not None
                    and not (ob_bot > active_top or ob_top < active_bot)
                )
                if not overlapping:
                    active_top, active_bot = ob_top, ob_bot
                    active_buy_ratio = _pivot_volume_ratio(candles, pivot_idx, pivot_len)
                    active_ob_trend = 1

            if trend_i == -1 and i in confirmed_hi:
                pivot_idx = i - pivot_len
                ob_bot = max(candles[pivot_idx]["open"], candles[pivot_idx]["close"])
                ob_top = ob_bot + atr_i
                overlapping = (
                    active_top is not None
                    and not (ob_bot > active_top or ob_top < active_bot)
                )
                if not overlapping:
                    active_top, active_bot = ob_top, ob_bot
                    active_buy_ratio = _pivot_volume_ratio(candles, pivot_idx, pivot_len)
                    active_ob_trend = -1

        if delete_on_break and active_top is not None:
            c = candles[i]
            is_broken = (
                (active_ob_trend == 1 and c["high"] < active_bot)
                or (active_ob_trend == -1 and c["low"] > active_top)
            )
            if is_broken:
                active_top = active_bot = active_buy_ratio = None
                active_ob_trend = 0

        history.append({
            "top": active_top, "bottom": active_bot,
            "buy_ratio": active_buy_ratio, "ob_trend": active_ob_trend,
        })

    return trend_arr, atr_arr, history, confirmed_lo, confirmed_hi


def _retest_at(i, candles, trend_arr, history, confirmed_lo, confirmed_hi, config):
    """Evalúa buy_retest / sell_retest en la barra `i`, replicando las
    condiciones exactas de Pine (ver docstring del módulo)."""
    if i <= 0:
        return False, False

    min_buy = getattr(config, "OB_MIN_BUY_PCT", 50.0) / 100.0
    min_sell = getattr(config, "OB_MIN_SELL_PCT", 50.0) / 100.0

    state = history[i]
    top, bot, buy_ratio = state["top"], state["bottom"], state["buy_ratio"]
    market_change = trend_arr[i] != trend_arr[i - 1]
    c, cp = candles[i], candles[i - 1]

    buy_retest = (
        top is not None and buy_ratio is not None
        and i not in confirmed_lo and not market_change
        and buy_ratio >= min_buy
        and cp["low"] <= top and c["low"] > top
    )
    sell_retest = (
        bot is not None and buy_ratio is not None
        and i not in confirmed_hi and not market_change
        and (1.0 - buy_ratio) >= min_sell
        and cp["high"] >= bot and c["high"] < bot
    )
    return buy_retest, sell_retest


def get_signal(candles, config):
    """
    candles: velas OHLCV (con "volume") del timeframe OB_TF, orden ascendente.
    Devuelve dict con trend, active_ob, signal (LONG/SHORT/None) y precios,
    en el mismo "shape" que unicorn_model.get_signal() para que
    combined_engine.py pueda tratar ambos motores de forma simétrica.
    """
    out = {
        "trend": 0, "signal": None, "active_ob": None, "reason": "sin_datos",
        "entry_price": 0.0, "sl_price": 0.0, "tp_price": 0.0, "risk": 0.0,
        "atr": 0.0,
    }
    if not candles:
        return out

    st_len = getattr(config, "ST_LEN", 50)
    pivot_len = getattr(config, "OB_PIVOT_LEN", 7)
    min_needed = st_len + pivot_len * 2 + 10

    if len(candles) < min_needed:
        out["reason"] = f"velas_insuficientes ({len(candles)} < {min_needed})"
        return out

    trend_arr, atr_arr, history, confirmed_lo, confirmed_hi = _simulate(candles, config)

    idx = len(candles) - 2  # última vela CERRADA (la última puede seguir en formación)
    if idx <= 0:
        return out

    state = history[idx]
    out["trend"] = trend_arr[idx]
    out["atr"] = atr_arr[idx] or 0.0
    if state["top"] is not None:
        out["active_ob"] = {
            "top": state["top"], "bottom": state["bottom"],
            "buy_ratio": state["buy_ratio"], "ob_trend": state["ob_trend"],
        }

    buy_retest, sell_retest = _retest_at(
        idx, candles, trend_arr, history, confirmed_lo, confirmed_hi, config
    )

    direction_cfg = getattr(config, "DIRECTION", "BOTH")
    rr = getattr(config, "OB_RR", 1.5)
    sl_buffer_mult = getattr(config, "OB_SL_ATR_BUFFER", 0.2)
    atr_i = atr_arr[idx] or 0.0
    entry = candles[idx]["close"]

    if buy_retest and direction_cfg in ("LONG", "BOTH"):
        sl = state["bottom"] - atr_i * sl_buffer_mult
        risk = entry - sl
        if risk > 0:
            out.update({
                "signal": "LONG", "entry_price": entry, "sl_price": sl,
                "tp_price": entry + rr * risk, "risk": risk,
                "reason": f"retest_alcista confirmado (buy_ratio={state['buy_ratio']:.2f})",
            })
            log.info("[OB] retest LONG | entry=%.6f sl=%.6f tp=%.6f buy_ratio=%.2f",
                      entry, sl, out["tp_price"], state["buy_ratio"])
            return out
    elif sell_retest and direction_cfg in ("SHORT", "BOTH"):
        sl = state["top"] + atr_i * sl_buffer_mult
        risk = sl - entry
        if risk > 0:
            out.update({
                "signal": "SHORT", "entry_price": entry, "sl_price": sl,
                "tp_price": entry - rr * risk, "risk": risk,
                "reason": f"retest_bajista confirmado (sell_ratio={1 - state['buy_ratio']:.2f})",
            })
            log.info("[OB] retest SHORT | entry=%.6f sl=%.6f tp=%.6f sell_ratio=%.2f",
                      entry, sl, out["tp_price"], 1 - state["buy_ratio"])
            return out

    out["reason"] = "sin_retest_confirmado"
    return out
