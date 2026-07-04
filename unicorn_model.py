"""
Unicorn Model — Motor de entrada (sweep de liquidez + breaker + FVG)
=====================================================================
1. Niveles HTF: OHLC + Swing de las fuentes configuradas (15m/30m/1H)
2. Sweep: mecha rompe el nivel, cierre queda del otro lado
3. Breaker: 2+ velas consecutivas en dirección contraria al sweep
4. Confirmación: el cierre de la última vela rompe el breaker
5. FVG (Unicorn Mode): gap sin mitigar solapado con el breaker
6. SL: extremo opuesto del breaker + colchón ATR; TP: R:R configurable
"""
import logging

log = logging.getLogger("unicorn_model")


def _atr(candles, period=14):
    if len(candles) < 2:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        c, cp = candles[i], candles[i - 1]
        trs.append(max(
            c["high"] - c["low"],
            abs(c["high"] - cp["close"]),
            abs(c["low"] - cp["close"]),
        ))
    if not trs:
        return 0.0
    a = trs[0]
    for t in trs[1:]:
        a = t / period + a * (1 - 1 / period)
    return a


def _get_htf_levels(candles_htf, htf_label):
    """OHLC (vela anterior cerrada) + Swing highs/lows del HTF."""
    levels = []
    if len(candles_htf) < 3:
        return levels
    prev = candles_htf[-2]
    levels.append({"price": prev["high"], "is_high": True, "type": "OHLC", "htf": htf_label})
    levels.append({"price": prev["low"], "is_high": False, "type": "OHLC", "htf": htf_label})
    for i in range(2, min(len(candles_htf) - 1, 22)):
        c, cp = candles_htf[-i], candles_htf[-i - 1]
        if c["high"] > cp["high"] and c["close"] < cp["close"]:
            levels.append({"price": c["high"], "is_high": True, "type": "Swing", "htf": htf_label})
        if c["low"] < cp["low"] and c["close"] > cp["close"]:
            levels.append({"price": c["low"], "is_high": False, "type": "Swing", "htf": htf_label})
    return levels


def _check_sweep(candles, level, lookback=30):
    is_high = level["is_high"]
    price = level["price"]
    check = candles[-lookback:]
    for i in range(len(check) - 1, -1, -1):
        c = check[i]
        if is_high and c["high"] >= price and c["close"] < price:
            return len(candles) - lookback + i
        if not is_high and c["low"] <= price and c["close"] > price:
            return len(candles) - lookback + i
    return None


def _find_breaker(candles, sweep_idx, direction, max_span=40):
    start = sweep_idx + 1
    end = min(start + max_span, len(candles) - 1)
    for i in range(start, end):
        c = candles[i]
        is_match = (c["close"] > c["open"]) if direction == "BULL" else (c["close"] < c["open"])
        if not is_match:
            continue
        run_end = i
        for j in range(i + 1, min(i + 20, end)):
            nxt = candles[j]
            nxt_match = (nxt["close"] > nxt["open"]) if direction == "BULL" else (nxt["close"] < nxt["open"])
            if nxt_match:
                run_end = j
            else:
                break
        if run_end - i + 1 >= 2:
            top = max(c["high"] for c in candles[i:run_end + 1])
            bot = min(c["low"] for c in candles[i:run_end + 1])
            return i, run_end, top, bot
    return None


def _find_fvg(candles, b_top, b_bot, direction, max_lookback=100):
    window = candles[-max_lookback:] if len(candles) > max_lookback else candles
    if direction == "BULL":
        for i in range(2, len(window)):
            f_top = window[i]["low"]
            f_bot = window[i - 2]["high"]
            if f_top <= f_bot:
                continue
            if min(f_top, b_top) > max(f_bot, b_bot):
                if not any(c["low"] < f_bot for c in window[i:]):
                    return f_top, f_bot
    else:
        for i in range(2, len(window)):
            f_top = window[i - 2]["low"]
            f_bot = window[i]["high"]
            if f_bot <= f_top:
                continue
            if min(f_top, b_top) > max(f_bot, b_bot):
                if not any(c["high"] > f_top for c in window[i:]):
                    return f_top, f_bot
    return None, None


def _size_filter_ok(b_top, b_bot, atr, min_atr, max_atr):
    if atr <= 0:
        return True
    size_in_atr = (b_top - b_bot) / atr
    return min_atr <= size_in_atr <= max_atr


def get_signal(candles_entry, candles_1h, config, candles_15m=None, candles_30m=None):
    """
    Devuelve un dict con la señal (o None) más todo el contexto del setup.
    candles_entry → velas del TF de entrada (ej. 3m)
    candles_1h    → niveles HTF fuente C
    candles_15m/30m → niveles HTF fuentes A/B (opcional)
    """
    result = {
        "signal": None, "entry_price": 0.0, "sl_price": 0.0, "tp_price": 0.0,
        "swept_level": 0.0, "breaker_top": 0.0, "breaker_bottom": 0.0,
        "fvg_top": None, "fvg_bottom": None, "has_fvg": False, "atr": 0.0,
        "level_type": "", "htf": "", "risk": 0.0, "sweep_candle_time": None,
    }

    sweep_lb = getattr(config, "UNICORN_SWEEP_LB", 30)
    require_fvg = getattr(config, "UNICORN_REQUIRE_FVG", True)
    rr = getattr(config, "UNICORN_RR", 1.5)
    sl_buffer_mult = getattr(config, "UNICORN_SL_ATR_BUFFER", 0.2)
    direction_cfg = getattr(config, "DIRECTION", "BOTH")
    min_atr = getattr(config, "BREAKER_MIN_ATR", 0.0)
    max_atr = getattr(config, "BREAKER_MAX_ATR", 999.0)

    if len(candles_entry) < 80 or len(candles_1h) < 3:
        return result

    atr = _atr(candles_entry, 14)
    result["atr"] = atr

    levels = []
    if candles_15m and len(candles_15m) >= 3:
        levels += _get_htf_levels(candles_15m, "15m")
    if candles_30m and len(candles_30m) >= 3:
        levels += _get_htf_levels(candles_30m, "30m")
    levels += _get_htf_levels(candles_1h, "1H")
    if not levels:
        return result

    def _try_setup(level, bull):
        sweep_idx = _check_sweep(candles_entry, level, sweep_lb)
        if sweep_idx is None:
            return None

        sweep_candle_time = candles_entry[sweep_idx].get("time")

        breaker = _find_breaker(candles_entry, sweep_idx, "BULL" if bull else "BEAR")
        if breaker is None:
            return None
        _, _, b_top, b_bot = breaker

        if not _size_filter_ok(b_top, b_bot, atr, min_atr, max_atr):
            return None

        if bull:
            sweep_extreme = min(c["low"] for c in candles_entry[sweep_idx:min(sweep_idx + 5, len(candles_entry))])
            if b_bot < sweep_extreme:
                return None
            last_close = candles_entry[-2]["close"]
            if last_close <= b_top:
                return None
        else:
            sweep_extreme = max(c["high"] for c in candles_entry[sweep_idx:min(sweep_idx + 5, len(candles_entry))])
            if b_top > sweep_extreme:
                return None
            last_close = candles_entry[-2]["close"]
            if last_close >= b_bot:
                return None

        fvg_top, fvg_bot = _find_fvg(candles_entry, b_top, b_bot, "BULL" if bull else "BEAR")
        has_fvg = fvg_top is not None
        if require_fvg and not has_fvg:
            return None

        entry = last_close
        if bull:
            sl = b_bot - atr * sl_buffer_mult
            risk = entry - sl
            if risk <= 0:
                return None
            tp = entry + rr * risk
        else:
            sl = b_top + atr * sl_buffer_mult
            risk = sl - entry
            if risk <= 0:
                return None
            tp = entry - rr * risk

        return {
            "signal": "LONG" if bull else "SHORT",
            "entry_price": entry, "sl_price": sl, "tp_price": tp,
            "swept_level": level["price"], "breaker_top": b_top, "breaker_bottom": b_bot,
            "fvg_top": fvg_top, "fvg_bottom": fvg_bot, "has_fvg": has_fvg,
            "level_type": level.get("type", ""), "htf": level.get("htf", "1H"),
            "atr": atr, "risk": risk, "sweep_candle_time": sweep_candle_time,
        }

    if direction_cfg in ("LONG", "BOTH"):
        for lv in sorted([l for l in levels if not l["is_high"]], key=lambda x: x["price"], reverse=True):
            res = _try_setup(lv, bull=True)
            if res:
                result.update(res)
                return result

    if direction_cfg in ("SHORT", "BOTH"):
        for lv in sorted([l for l in levels if l["is_high"]], key=lambda x: x["price"]):
            res = _try_setup(lv, bull=False)
            if res:
                result.update(res)
                return result

    return result
