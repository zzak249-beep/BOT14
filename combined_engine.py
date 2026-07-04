"""
Combined Signal Engine — Supertrend (bias) + Unicorn Model (timing)
======================================================================
Jerarquía de filtros:
  1. Supertrend en BIAS_TF (ej. 1H) define la dirección permitida.
  2. Unicorn Model en ENTRY_TF (ej. 3m) da el punto exacto de entrada.
  3. Solo se emite señal si ambos coinciden en dirección.
"""
import logging

from supertrend_engine import get_trend
from unicorn_model import get_signal
from regime_filter import is_trending_regime

log = logging.getLogger("combined_engine")


def evaluate_symbol(symbol, candles_entry, candles_bias, candles_1h,
                     config, candles_15m=None, candles_30m=None):
    """
    Evalúa un símbolo y devuelve una señal combinada o None.
    candles_entry → velas de ENTRY_TF (ej. 3m)
    candles_bias  → velas de BIAS_TF (ej. 1H) para el Supertrend y el régimen
    candles_1h/15m/30m → fuentes de liquidez del Unicorn Model
    """
    out = {
        "symbol": symbol, "signal": None, "reason": None,
        "supertrend": None, "unicorn": None, "regime": None,
    }

    if getattr(config, "ENABLE_REGIME_FILTER", True):
        regime = is_trending_regime(candles_bias, config)
        out["regime"] = regime
        if regime["trending"] is False:
            out["reason"] = f"regime_blocked: {regime['reason']}"
            return out

    st = get_trend(candles_bias, st_len=config.ST_LEN, st_mult=config.ST_MULT)
    out["supertrend"] = st

    if st["trend"] == 0:
        out["reason"] = "insufficient_data_supertrend"
        return out

    uni = get_signal(candles_entry, candles_1h, config, candles_15m, candles_30m)
    out["unicorn"] = uni

    if uni["signal"] is None:
        out["reason"] = "no_unicorn_setup"
        return out

    uni_dir = 1 if uni["signal"] == "LONG" else -1
    if uni_dir != st["trend"]:
        out["reason"] = (
            f"direction_conflict: unicorn={uni['signal']} "
            f"supertrend={'BULLISH' if st['trend'] == 1 else 'BEARISH'}"
        )
        return out

    out["signal"] = uni["signal"]
    out["entry_price"] = uni["entry_price"]
    out["sl_price"] = uni["sl_price"]
    out["tp_price"] = uni["tp_price"]
    out["risk"] = uni["risk"]
    out["has_fvg"] = uni["has_fvg"]
    out["swept_level"] = uni["swept_level"]
    out["htf_source"] = uni["htf"]
    out["setup_key"] = f"{uni['htf']}|{uni['level_type']}|fvg={uni['has_fvg']}|{uni['signal']}"
    out["reason"] = "confirmed: supertrend + unicorn aligned"
    log.info(
        "[%s] SEÑAL %s | entry=%.6f sl=%.6f tp=%.6f | FVG=%s | HTF=%s",
        symbol, uni["signal"], uni["entry_price"], uni["sl_price"],
        uni["tp_price"], uni["has_fvg"], uni["htf"],
    )
    return out
