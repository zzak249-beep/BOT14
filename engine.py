"""
QF×JP Bot v6.0 — engine.py
Motor de señales mejorado:
  • Fix np.errstate para RuntimeWarning de división
  • Composite entry score: Score + CVD + Momentum + Decay
  • Multi-TF alignment bonus (1m/3m/15m/1h)
  • OFI / FR / OI integrados
  • ATR dinámico para SL/TP
  • Tier system: STD / FUEL / SUP
"""
import logging
from typing import Optional

import numpy as np

log = logging.getLogger("ENGINE")


# ── helpers numpy safe ─────────────────────────────────────
def _safe_div(a: np.ndarray, b: np.ndarray, fill: float = 0.0) -> np.ndarray:
    with np.errstate(divide="ignore", invalid="ignore"):
        result = np.where(np.abs(b) > 1e-12, a / b, fill)
    return np.nan_to_num(result, nan=fill, posinf=fill, neginf=fill)


def _ema(arr: np.ndarray, period: int) -> np.ndarray:
    out = np.zeros_like(arr, dtype=float)
    k   = 2.0 / (period + 1)
    out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = arr[i] * k + out[i - 1] * (1 - k)
    return out


def _rsi(close: np.ndarray, period: int = 14) -> np.ndarray:
    delta = np.diff(close, prepend=close[0])
    gain  = np.where(delta > 0, delta, 0.0)
    loss  = np.where(delta < 0, -delta, 0.0)
    ag    = _ema(gain, period)
    al    = _ema(loss, period)
    rs    = _safe_div(ag, al, fill=1.0)
    return 100 - 100 / (1 + rs)


def _atr(h: np.ndarray, l: np.ndarray, c: np.ndarray, period: int = 14) -> np.ndarray:
    prev_c = np.roll(c, 1); prev_c[0] = c[0]
    tr = np.maximum(h - l, np.maximum(np.abs(h - prev_c), np.abs(l - prev_c)))
    return _ema(tr, period)


def _macd(close: np.ndarray, fast: int = 12, slow: int = 26, sig: int = 9):
    macd_line = _ema(close, fast) - _ema(close, slow)
    signal    = _ema(macd_line, sig)
    hist      = macd_line - signal
    return macd_line, signal, hist


def _vwap(o, h, l, c, v):
    tp  = (h + l + c) / 3
    cum_tv = np.cumsum(tp * v)
    cum_v  = np.cumsum(v)
    return _safe_div(cum_tv, cum_v, fill=c[-1])


def _cvd(o, c, h, l, v) -> float:
    """Cumulative Volume Delta (simplificado)."""
    hl_r = h - l
    with np.errstate(divide="ignore", invalid="ignore"):
        bvol = np.where(hl_r > 1e-12, ((c - l) / np.where(hl_r > 1e-12, hl_r, 1)) * v, v * 0.5)
        svol = np.where(hl_r > 1e-12, ((h - c) / np.where(hl_r > 1e-12, hl_r, 1)) * v, v * 0.5)
    delta = bvol - svol
    return float(np.sum(delta))


def _swing_highs(h: np.ndarray, window: int = 5) -> np.ndarray:
    out = np.zeros(len(h))
    for i in range(window, len(h) - window):
        if h[i] == np.max(h[i - window:i + window + 1]):
            out[i] = h[i]
    return out


def _swing_lows(l: np.ndarray, window: int = 5) -> np.ndarray:
    out = np.zeros(len(l))
    for i in range(window, len(l) - window):
        if l[i] == np.min(l[i - window:i + window + 1]):
            out[i] = l[i]
    return out


def _klines_to_arrays(klines: list):
    o = np.array([k["o"] for k in klines], dtype=float)
    h = np.array([k["h"] for k in klines], dtype=float)
    l = np.array([k["l"] for k in klines], dtype=float)
    c = np.array([k["c"] for k in klines], dtype=float)
    v = np.array([k["v"] for k in klines], dtype=float)
    return o, h, l, c, v


class QFJPEngine:
    """Motor de señales QF×JP v6."""

    # ─────────────────────────────────────────────────────
    def compute(
        self,
        klines_3m:  list,
        klines_15m: list,
        klines_1h:  list,
        klines_1m:  list,
        mctx:       dict,
    ) -> dict:
        blank = self._blank()
        if len(klines_3m) < 100:
            return blank

        o3, h3, l3, c3, v3 = _klines_to_arrays(klines_3m)

        # ── Indicadores base ──────────────────────────────
        atr14  = _atr(h3, l3, c3, 14)
        atr_v  = float(atr14[-1])
        rsi14  = _rsi(c3, 14)
        rsi_v  = float(rsi14[-1])

        macd_l, macd_s, macd_h = _macd(c3)
        macd_hv = float(macd_h[-1])

        vwap_v = float(_vwap(o3, h3, l3, c3, v3)[-1])
        price  = float(c3[-1])

        # ── Momentum compuesto ────────────────────────────
        # RSI normalizado (-100 → +100)
        rsi_norm = (rsi_v - 50) * 2          # [-100, +100]
        # MACD normalizado por ATR
        macd_norm = float(np.clip(_safe_div(
            np.array([macd_hv]),
            np.array([atr_v or 1.0])
        )[0] * 100, -200, 200))
        momentum_raw = rsi_norm * 0.5 + macd_norm * 0.5

        # ── CVD (3m) ──────────────────────────────────────
        cvd_raw  = _cvd(o3, c3, h3, l3, v3)
        # Normalizar CVD por volumen total
        vol_total = float(np.sum(v3)) or 1.0
        cvd_norm  = float(np.clip(cvd_raw / vol_total, -1.0, 1.0))

        # ── Swing structure ───────────────────────────────
        sh3  = _swing_highs(h3, 5)
        sl3  = _swing_lows(l3, 5)
        last_sh = float(sh3[sh3 > 0][-1]) if np.any(sh3 > 0) else price
        last_sl = float(sl3[sl3 > 0][-1]) if np.any(sl3 > 0) else price

        # ── VWAP bias ─────────────────────────────────────
        vwap_bull = price > vwap_v
        vwap_bear = price < vwap_v

        # ── Volumen relativo ──────────────────────────────
        vol_ma  = float(np.mean(v3[-20:]))
        vol_cur = float(v3[-1])
        with np.errstate(divide="ignore", invalid="ignore"):
            vol_ratio = vol_cur / vol_ma if vol_ma > 1e-12 else 1.0
        vol_regime = "HIGH" if vol_ratio > 1.5 else "LOW" if vol_ratio < 0.5 else "MED"

        # ── Asymmetry (riesgo/recompensa natural) ─────────
        dist_res = abs(last_sh - price)
        dist_sup = abs(price - last_sl)
        with np.errstate(divide="ignore", invalid="ignore"):
            asym_bull = dist_res / dist_sup if dist_sup > 1e-12 else 1.0
            asym_bear = dist_sup / dist_res if dist_res > 1e-12 else 1.0

        # ── Multi-TF alignment ────────────────────────────
        tf_bull = 0; tf_bear = 0
        # 15m
        if len(klines_15m) >= 20:
            _, _, _, c15, _ = _klines_to_arrays(klines_15m)
            ema20_15 = float(_ema(c15, 20)[-1])
            if c15[-1] > ema20_15: tf_bull += 1
            else:                  tf_bear += 1
        # 1h
        if len(klines_1h) >= 20:
            _, _, _, c1h, _ = _klines_to_arrays(klines_1h)
            ema20_1h = float(_ema(c1h, 20)[-1])
            if c1h[-1] > ema20_1h: tf_bull += 1
            else:                  tf_bear += 1
        # 1m momentum
        if len(klines_1m) >= 10:
            _, _, _, c1m, _ = _klines_to_arrays(klines_1m)
            if c1m[-1] > c1m[-5]: tf_bull += 1
            else:                  tf_bear += 1

        tf_score_bull = tf_bull / 3.0    # 0..1
        tf_score_bear = tf_bear / 3.0

        # ── OFI / FR / OI ─────────────────────────────────
        ofi  = float(mctx.get("ofi", 0))
        fr   = float(mctx.get("funding_rate", 0))
        oi   = float(mctx.get("open_interest", 0))
        oi_p = float(mctx.get("prev_open_interest", oi))
        with np.errstate(divide="ignore", invalid="ignore"):
            oi_delta = (oi - oi_p) / oi_p if oi_p > 1e-12 else 0.0

        # FR extremo → señal contraria (funding squeeze)
        fr_extreme_long  = fr > 0.005    # longs muy cargados → corto
        fr_extreme_short = fr < -0.005   # shorts muy cargados → largo

        # ── Score base (normalizado 0..1) ─────────────────
        components = {
            # RSI: bulls si 45-70, bears si 30-55
            "rsi_bull":  max(0, min(1, (rsi_v - 45) / 25)),
            "rsi_bear":  max(0, min(1, (55 - rsi_v) / 25)),
            # MACD histogram positivo
            "macd_bull": 1.0 if macd_hv > 0 else 0.0,
            "macd_bear": 1.0 if macd_hv < 0 else 0.0,
            # VWAP
            "vwap_bull": 1.0 if vwap_bull else 0.0,
            "vwap_bear": 1.0 if vwap_bear else 0.0,
            # OFI
            "ofi_bull":  max(0, ofi),
            "ofi_bear":  max(0, -ofi),
            # TF alignment
            "tf_bull":   tf_score_bull,
            "tf_bear":   tf_score_bear,
            # OI delta (OI subiendo = momentum real)
            "oi_bull":   max(0, oi_delta) * 10,
            "oi_bear":   max(0, -oi_delta) * 10,
            # FR contra-tendencia
            "fr_squeeze_bull":  1.0 if fr_extreme_short else 0.0,
            "fr_squeeze_bear":  1.0 if fr_extreme_long  else 0.0,
        }

        weights = {
            "rsi_bull": .15, "rsi_bear": .15,
            "macd_bull": .10, "macd_bear": .10,
            "vwap_bull": .10, "vwap_bear": .10,
            "ofi_bull":  .20, "ofi_bear":  .20,
            "tf_bull":   .25, "tf_bear":   .25,
            "oi_bull":   .10, "oi_bear":   .10,
            "fr_squeeze_bull": .10, "fr_squeeze_bear": .10,
        }

        raw_bull = sum(components[k] * weights[k] for k in
                       ["rsi_bull","macd_bull","vwap_bull","ofi_bull","tf_bull","oi_bull","fr_squeeze_bull"])
        raw_bear = sum(components[k] * weights[k] for k in
                       ["rsi_bear","macd_bear","vwap_bear","ofi_bear","tf_bear","oi_bear","fr_squeeze_bear"])

        total_w = sum([.15,.10,.10,.20,.25,.10,.10])
        score_bull = raw_bull / total_w
        score_bear = raw_bear / total_w

        # ── Composite entry (Score + CVD + Mom + Decay) ────
        # Decay: cuánto de fresca es la señal (vol_ratio proxy)
        decay_ratio = float(np.clip(vol_ratio / 2.0, 0.3, 1.0))

        def composite(score: float, cvd: float, mom: float) -> float:
            """score [0,1], cvd [-1,1], mom [-200,200]"""
            cvd_scaled = (cvd + 1) / 2        # [0,1]
            mom_scaled = (mom + 200) / 400    # [0,1]
            decay_scaled = decay_ratio         # [0,1]
            return (
                0.40 * score +
                0.25 * cvd_scaled +
                0.20 * mom_scaled +
                0.15 * decay_scaled
            )

        comp_bull = composite(score_bull, cvd_norm,  momentum_raw)
        comp_bear = composite(score_bear, -cvd_norm, -momentum_raw)

        # ── Dirección y tier ──────────────────────────────
        direction: Optional[str] = None
        norm_score: float = 0.0
        asym: float = 1.0

        THR = 0.58   # composite mínimo para señal

        if comp_bull > comp_bear and comp_bull >= THR:
            if not fr_extreme_long:   # no entrar long si FR extremo
                direction  = "LONG"
                norm_score = comp_bull
                asym       = asym_bull
        elif comp_bear > comp_bull and comp_bear >= THR:
            if not fr_extreme_short:
                direction  = "SHORT"
                norm_score = comp_bear
                asym       = asym_bear

        # ── Tier ─────────────────────────────────────────
        tier = "STD"
        if direction:
            if vol_regime == "HIGH" and abs(ofi) > 0.45:
                tier = "SUP"
            elif vol_regime == "HIGH" or abs(ofi) > 0.25:
                tier = "FUEL"

        # ── Convicción (0-10) ─────────────────────────────
        conviction = int(np.clip(norm_score * 10, 0, 10))

        # ── SL / TP dinámico ─────────────────────────────
        sl_val: Optional[float] = None
        tp_val: Optional[float] = None
        if direction and atr_v > 0:
            sl_mult = 1.5 if tier == "SUP" else 1.8
            tp_mult = sl_mult * 2.0
            if direction == "LONG":
                sl_val = price - atr_v * sl_mult
                tp_val = price + atr_v * tp_mult
            else:
                sl_val = price + atr_v * sl_mult
                tp_val = price - atr_v * tp_mult

        # ── CVD bias string ───────────────────────────────
        if cvd_norm > 0.15:   cvd_bias = "BULL"
        elif cvd_norm < -0.15: cvd_bias = "BEAR"
        else:                  cvd_bias = "NEUTRAL"

        return {
            "direction":    direction,
            "tier":         tier,
            "conviction":   conviction,
            "norm_score":   norm_score,
            "score_bull":   score_bull,
            "score_bear":   score_bear,
            "comp_bull":    comp_bull,
            "comp_bear":    comp_bear,
            "decay_ratio":  decay_ratio,
            "momentum":     momentum_raw,
            "cvd_norm":     cvd_norm,
            "cvd_bias":     cvd_bias,
            "ofi":          ofi,
            "funding_rate": fr,
            "oi_delta":     oi_delta,
            "vol_regime":   vol_regime,
            "atr_last":     atr_v,
            "vwap":         vwap_v,
            "sl":           sl_val,
            "tp":           tp_val,
            "tf_bull":      tf_score_bull,
            "tf_bear":      tf_score_bear,
            "asym":         asym,
        }

    @staticmethod
    def _blank() -> dict:
        return {
            "direction": None, "tier": "STD", "conviction": 0,
            "norm_score": 0.0, "score_bull": 0.0, "score_bear": 0.0,
            "comp_bull": 0.0, "comp_bear": 0.0,
            "decay_ratio": 0.0, "momentum": 0.0,
            "cvd_norm": 0.0, "cvd_bias": "NEUTRAL",
            "ofi": 0.0, "funding_rate": 0.0, "oi_delta": 0.0,
            "vol_regime": "MED", "atr_last": 0.0, "vwap": 0.0,
            "sl": None, "tp": None,
            "tf_bull": 0.0, "tf_bear": 0.0, "asym": 1.0,
        }

    # ── Validación compuesta para entrada ─────────────────
    @staticmethod
    def should_enter(sig: dict, min_composite: float = 0.58) -> bool:
        """
        Filtro final: Score + CVD alineado + Momentum + Decay.
        Retorna True solo si todos los pilares apuntan en la misma dirección.
        """
        d    = sig["direction"]
        comp = sig["comp_bull"] if d == "LONG" else sig["comp_bear"]
        cvd  = sig["cvd_bias"]
        mom  = sig["momentum"]
        dec  = sig["decay_ratio"]
        vol  = sig["vol_regime"]

        if not d:                       return False
        if comp < min_composite:        return False
        if dec < 0.55:                  return False   # señal muerta
        if vol == "LOW":                return False   # sin liquidez

        # CVD debe estar alineado o neutro (nunca contrario)
        if d == "LONG"  and cvd == "BEAR": return False
        if d == "SHORT" and cvd == "BULL": return False

        # Momentum no puede ser extremadamente contrario
        if d == "LONG"  and mom < -80: return False
        if d == "SHORT" and mom > 80:  return False

        return True


# ═══════════════════════════════════════════════════════════
# INTEGRACIÓN DE EDGE EN EL ENGINE
# ═══════════════════════════════════════════════════════════
from edge import EdgeEngine, EdgeResult

_edge_engine = EdgeEngine()


def compute_with_edge(
    klines_3m:  list,
    klines_15m: list,
    klines_1h:  list,
    klines_1m:  list,
    mctx:       dict,
) -> dict:
    """
    Wrapper que combina QFJPEngine + EdgeEngine.
    El edge score pondera el composite final:
      final = 0.55 * composite + 0.45 * edge_score_norm
    Solo abre trade si AMBOS están alineados.
    """
    engine = QFJPEngine()
    sig    = engine.compute(klines_3m, klines_15m, klines_1h, klines_1m, mctx)

    edge   = _edge_engine.compute(
        klines=klines_3m,
        klines_1m=klines_1m,
        funding_rate=mctx.get("funding_rate", 0.0),
        ofi=mctx.get("ofi", 0.0),
    )

    sig["edge_score"]   = edge.edge_score
    sig["edge_dir"]     = edge.edge_dir
    sig["edge_detail"]  = edge.detail
    sig["edge_signals_bull"] = edge.signals_count_bull
    sig["edge_signals_bear"] = edge.signals_count_bear

    # ── Edge normalizado [0,1] según la dirección ────────
    if sig["direction"] == "LONG":
        edge_norm = (edge.edge_score + 1) / 2
    elif sig["direction"] == "SHORT":
        edge_norm = (1 - edge.edge_score) / 2
    else:
        edge_norm = 0.5

    # ── Score final combinado ─────────────────────────────
    comp_key = "comp_bull" if sig["direction"] == "LONG" else "comp_bear"
    composite = sig.get(comp_key, 0.0)
    final = 0.55 * composite + 0.45 * edge_norm
    sig["final_score"] = final

    # ── Alineación: edge debe coincidir con dirección ─────
    edge_aligned = (
        (sig["direction"] == "LONG"  and edge.edge_dir in ("LONG",  None)) or
        (sig["direction"] == "SHORT" and edge.edge_dir in ("SHORT", None)) or
        edge.edge_dir is None
    )

    # Si edge contradice dirección → cancelar señal
    if edge.edge_dir and edge.edge_dir != sig["direction"]:
        sig["direction"] = None
        sig["final_score"] = 0.0
        sig["edge_veto"] = True
    else:
        sig["edge_veto"] = False

    return sig
