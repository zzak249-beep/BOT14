"""
Combined Signal Engine — Supertrend (bias) + Unicorn Model + Order Block Engine
==================================================================================
Jerarquía:
  1. Supertrend en BIAS_TF (ej. 1H) define la dirección permitida — gobierna
     a AMBOS motores de entrada de abajo; ninguno puede operar en contra.
  2. Motores de entrada, evaluados EN ORDEN (se usa el primero que confirme):
       a) Unicorn Model en ENTRY_TF (ej. 3m) — sweep + breaker + FVG
       b) Order Block Engine (BigBeluga) en OB_TF (ej. 15m) — pivote +
          Order Block + retest, aceptado solo si el ratio de volumen de esa
          zona supera el umbral configurado (así es como el volumen
          "confirma" la entrada en este segundo motor)
  3. CVD Filter (opcional, ENABLE_CVD_FILTER) — si está activo, exige que el
     Cumulative Volume Delta de candles_entry también apunte en la misma
     dirección. Si un motor confirma pero el CVD no acompaña, no se corta
     la evaluación: se sigue probando el siguiente motor.
  4. Solo se emite señal si el motor que confirma coincide en dirección con
     el Supertrend (y con el CVD, si el filtro está activo).

Se usan dos motores en PARALELO (no un AND-gate entre ambos) porque exigir
que Unicorn Model y el retest del Order Block coincidan en la misma vela
sería estadísticamente casi imposible — son dos eventos raros e
independientes. Cada uno ya trae su propia confirmación interna.
"""
import logging

from supertrend_engine import get_trend
from unicorn_model import get_signal as get_unicorn_signal
from order_block_engine import get_signal as get_ob_signal
from cvd_filter import confirms_direction as cvd_confirms
from regime_filter import is_trending_regime

log = logging.getLogger("combined_engine")


def _check_cvd(symbol, direction, candles_entry, config):
    """Devuelve (ok: bool, info: dict). ok=True si el filtro está apagado,
    si no hay datos suficientes (no se penaliza por falta de info), o si
    el CVD confirma la dirección propuesta."""
    if not getattr(config, "ENABLE_CVD_FILTER", False):
        return True, {"skipped": True}
    cvd_info = cvd_confirms(candles_entry, direction, config)
    if cvd_info["confirms"] is None:
        return True, cvd_info  # sin datos suficientes -> no bloquea, mismo criterio que setup_memory
    if not cvd_info["confirms"]:
        log.info("[%s] %s descartado por CVD: %s", symbol, direction, cvd_info["reason"])
    return bool(cvd_info["confirms"]), cvd_info


def evaluate_symbol(symbol, candles_entry, candles_bias, candles_1h,
                     config, candles_15m=None, candles_30m=None, candles_ob=None):
    """
    Evalúa un símbolo y devuelve una señal combinada o None.
    candles_entry → velas de ENTRY_TF (ej. 3m), para el Unicorn Model
    candles_bias  → velas de BIAS_TF (ej. 1H) para el Supertrend y el régimen
    candles_1h/15m/30m → fuentes de liquidez del Unicorn Model
    candles_ob    → velas de OB_TF (ej. 15m) para el Order Block Engine;
                    si no se provee, ese motor simplemente se omite
    """
    out = {
        "symbol": symbol, "signal": None, "reason": None,
        "supertrend": None, "unicorn": None, "order_block": None,
        "regime": None, "engine": None, "cvd": None,
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

    # ── Motor 1: Unicorn Model ──────────────────────────────────────────
    uni = get_unicorn_signal(candles_entry, candles_1h, config, candles_15m, candles_30m)
    out["unicorn"] = uni

    if uni["signal"] is not None:
        uni_dir = 1 if uni["signal"] == "LONG" else -1
        if uni_dir == st["trend"]:
            cvd_ok, cvd_info = _check_cvd(symbol, uni["signal"], candles_entry, config)
            out["cvd"] = cvd_info
            if cvd_ok:
                out["signal"] = uni["signal"]
                out["entry_price"] = uni["entry_price"]
                out["sl_price"] = uni["sl_price"]
                out["tp_price"] = uni["tp_price"]
                out["risk"] = uni["risk"]
                out["has_fvg"] = uni["has_fvg"]
                out["swept_level"] = uni["swept_level"]
                out["htf_source"] = uni["htf"]
                out["setup_key"] = f"{uni['htf']}|{uni['level_type']}|fvg={uni['has_fvg']}|{uni['signal']}"
                out["engine"] = "unicorn"
                out["reason"] = "confirmed: supertrend + unicorn aligned"
                log.info(
                    "[%s] SEÑAL (unicorn) %s | entry=%.6f sl=%.6f tp=%.6f | FVG=%s | HTF=%s",
                    symbol, uni["signal"], uni["entry_price"], uni["sl_price"],
                    uni["tp_price"], uni["has_fvg"], uni["htf"],
                )
                return out
            out["reason"] = f"cvd_rejected_unicorn: {cvd_info.get('reason')}"
        else:
            out["reason"] = (
                f"direction_conflict: unicorn={uni['signal']} "
                f"supertrend={'BULLISH' if st['trend'] == 1 else 'BEARISH'}"
            )

    # ── Motor 2: Order Block Engine (BigBeluga) — solo si Unicorn no confirmó ──
    if getattr(config, "ENABLE_OB_ENGINE", True) and candles_ob:
        ob = get_ob_signal(candles_ob, config)
        out["order_block"] = ob

        if ob["signal"] is not None:
            ob_dir = 1 if ob["signal"] == "LONG" else -1
            if ob_dir == st["trend"]:
                cvd_ok, cvd_info = _check_cvd(symbol, ob["signal"], candles_entry, config)
                out["cvd"] = cvd_info
                if cvd_ok:
                    out["signal"] = ob["signal"]
                    out["entry_price"] = ob["entry_price"]
                    out["sl_price"] = ob["sl_price"]
                    out["tp_price"] = ob["tp_price"]
                    out["risk"] = ob["risk"]
                    out["active_ob"] = ob["active_ob"]
                    out["setup_key"] = f"OB_ENGINE|{getattr(config, 'OB_TF', '15m')}|{ob['signal']}"
                    out["engine"] = "order_block"
                    out["reason"] = "confirmed: supertrend + order_block retest aligned"
                    log.info(
                        "[%s] SEÑAL (order_block) %s | entry=%.6f sl=%.6f tp=%.6f | %s",
                        symbol, ob["signal"], ob["entry_price"], ob["sl_price"],
                        ob["tp_price"], ob["reason"],
                    )
                    return out
                out["reason"] = f"cvd_rejected_order_block: {cvd_info.get('reason')}"

    if out["reason"] is None:
        out["reason"] = "no_setup_from_any_engine"
    return out
