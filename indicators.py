"""
QF×JP Bot v6.4 — Indicators
Basado en análisis de trades ganadores:
  - Todos SHORT, leverage 5-10x, duración 5-15 min
  - BoS↓ / CHoCH↓ priorizados en composite_score
  - CVD negativo muy correlacionado con ganancia (peso ++CVD)
  - HTF SHORT bias pesado
  - SL ajustado (0.8 ATR), TP1 rápido (1.2 ATR)
  - funding_rate integrado en analyze()
"""
import logging
import warnings
from dataclasses import dataclass

import numpy as np

warnings.filterwarnings("ignore", category=RuntimeWarning)

log = logging.getLogger("indicators")

# ── Signal dataclass ──────────────────────────────────────────────────────────

@dataclass
class Signal:
    symbol:          str
    direction:       str    # LONG | SHORT | NONE
    score:           float
    tier:            str    # STD | FUEL | SUP | NONE
    entry:           float
    sl:              float
    tp1:             float
    tp2:             float
    atr:             float
    adx:             float
    mfi:             float
    vdi:             float
    cvd:             float
    momentum:        float
    htf_score:       float
    structure:       str
    tl_break:        str
    tl_break_active: bool  = False
    circuit_breaker: bool  = False
    funding_rate:    float = 0.0
    reason:          str   = ""

# ── Helpers ───────────────────────────────────────────────────────────────────

def _ema(arr: np.ndarray, period: int) -> np.ndarray:
    k   = 2.0 / (period + 1)
    out = np.empty_like(arr, dtype=float)
    out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = arr[i] * k + out[i - 1] * (1 - k)
    return out


def _rma(arr: np.ndarray, period: int) -> np.ndarray:
    k   = 1.0 / period
    out = np.empty_like(arr, dtype=float)
    out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = arr[i] * k + out[i - 1] * (1 - k)
    return out


def _sma(arr: np.ndarray, period: int) -> np.ndarray:
    out = np.full(len(arr), np.nan, dtype=float)
    for i in range(period - 1, len(arr)):
        out[i] = arr[i - period + 1 : i + 1].mean()
    return out


def _safe(val, default: float = 0.0) -> float:
    """Convierte a float seguro descartando NaN/inf."""
    try:
        v = float(val)
        return v if np.isfinite(v) else default
    except Exception:
        return default

# ── ATR ───────────────────────────────────────────────────────────────────────

def calc_atr(high, low, close, period: int = 10) -> np.ndarray:
    h, l, c = np.asarray(high, float), np.asarray(low, float), np.asarray(close, float)
    tr = np.maximum(h[1:] - l[1:],
         np.maximum(np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1])))
    tr = np.concatenate([[tr[0]], tr])
    return _rma(tr, period)

# ── ADX ───────────────────────────────────────────────────────────────────────

def calc_adx(high, low, close, period: int = 14):
    h, l, c = np.asarray(high, float), np.asarray(low, float), np.asarray(close, float)
    up   = h[1:] - h[:-1]
    down = l[:-1] - l[1:]
    plus_dm  = np.where((up > down) & (up > 0),     up,   0.0)
    minus_dm = np.where((down > up) & (down > 0),   down, 0.0)
    tr = np.maximum(h[1:] - l[1:],
         np.maximum(np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1])))
    plus_dm  = np.concatenate([[0.0], plus_dm])
    minus_dm = np.concatenate([[0.0], minus_dm])
    tr       = np.concatenate([[tr[0]], tr])
    atr14    = _rma(tr, period)
    safe_atr = np.where(atr14 > 1e-12, atr14, 1e-12)
    pdi = 100 * np.divide(_rma(plus_dm,  period), safe_atr,
                          out=np.zeros_like(atr14), where=safe_atr > 0)
    mdi = 100 * np.divide(_rma(minus_dm, period), safe_atr,
                          out=np.zeros_like(atr14), where=safe_atr > 0)
    denom = pdi + mdi
    dx    = 100 * np.divide(np.abs(pdi - mdi), denom,
                            out=np.zeros_like(denom), where=denom > 0)
    return _rma(dx, period), pdi, mdi

# ── OBV / Momentum ────────────────────────────────────────────────────────────

def calc_obv(close, volume) -> np.ndarray:
    c, v = np.asarray(close, float), np.asarray(volume, float)
    return np.cumsum(np.concatenate([[0], np.sign(np.diff(c))]) * v)


def calc_momentum(close, period: int = 10) -> np.ndarray:
    c   = np.asarray(close, float)
    mom = np.zeros_like(c)
    for i in range(period, len(c)):
        d = c[i - period] if c[i - period] != 0 else 1e-9
        mom[i] = (c[i] - c[i - period]) / d
    return mom

# ── CVD ───────────────────────────────────────────────────────────────────────

def calc_cvd(open_, close, volume) -> np.ndarray:
    o, c, v = (np.asarray(x, float) for x in (open_, close, volume))
    bull  = np.where(c > o, v, 0.0)
    bear  = np.where(c <= o, v, 0.0)
    delta = bull - bear
    total = bull + bear
    cvd   = np.divide(delta, total, out=np.zeros_like(delta), where=total > 0)
    return _ema(cvd, 5)

# ── MFI ───────────────────────────────────────────────────────────────────────

def calc_mfi(high, low, close, volume, period: int = 14) -> np.ndarray:
    h, l, c, v = (np.asarray(x, float) for x in (high, low, close, volume))
    tp  = (h + l + c) / 3
    mf  = tp * v
    mfi = np.full_like(c, 50.0)
    for i in range(period, len(c)):
        sl      = slice(i - period + 1, i + 1)
        sl_prev = slice(i - period,     i)
        up_mask = tp[sl] > tp[sl_prev]
        pos = np.sum(mf[sl][up_mask])
        neg = np.sum(mf[sl][~up_mask])
        mfi[i] = 100.0 if neg == 0 else 100 - 100 / (1 + pos / (neg + 1e-12))
    return mfi

# ── VDI ───────────────────────────────────────────────────────────────────────

def calc_vdi(close, volume, period: int = 20) -> np.ndarray:
    c, v     = np.asarray(close, float), np.asarray(volume, float)
    vwap_d   = (c - _sma(c, period)) * v
    std      = np.nanstd(vwap_d[-period:])
    result   = np.divide(vwap_d, std + 1e-9,
                         out=np.zeros_like(vwap_d), where=(std + 1e-9) > 0)
    return np.nan_to_num(result, nan=0.0, posinf=0.0, neginf=0.0)

# ── Estructura CHoCH / BoS ────────────────────────────────────────────────────

def detect_structure(high, low, close, lookback: int = 5) -> str:
    h, l, c = np.asarray(high, float), np.asarray(low, float), np.asarray(close, float)
    if len(h) < lookback * 2 + 5:
        return "NONE"
    prev_hh = h[-lookback * 2 - 1 : -lookback - 1].max()
    prev_ll = l[-lookback * 2 - 1 : -lookback - 1].min()
    curr_h  = h[-lookback - 1:].max()
    curr_l  = l[-lookback - 1:].min()
    cc      = c[-1]
    prev_c  = c[-lookback - 2]
    if cc > prev_hh and curr_h > prev_hh:
        return "BoS↑" if prev_c > prev_ll else "CHoCH↑"
    if cc < prev_ll and curr_l < prev_ll:
        return "BoS↓" if prev_c < prev_hh else "CHoCH↓"
    return "NONE"

# ── TL Ruptura ────────────────────────────────────────────────────────────────

def detect_tl_break(high, low, close, lookback: int = 20) -> str:
    h, l, c = np.asarray(high, float), np.asarray(low, float), np.asarray(close, float)
    if len(h) < lookback + 5:
        return "NONE"
    hh         = h[-lookback:]
    ll         = l[-lookback:]
    bear_slope = (hh[-2] - hh[0]) / lookback
    bear_now   = hh[0] + bear_slope * (lookback - 1)
    bull_slope = (ll[-2] - ll[0]) / lookback
    bull_now   = ll[0] + bull_slope * (lookback - 1)
    if c[-1] > bear_now and c[-2] <= bear_now:
        return "LONG"
    if c[-1] < bull_now and c[-2] >= bull_now:
        return "SHORT"
    return "NONE"

# ── FVG ───────────────────────────────────────────────────────────────────────

def detect_fvg(high, low) -> str:
    h, l = np.asarray(high, float), np.asarray(low, float)
    for i in range(len(h) - 1, max(len(h) - 6, 1), -1):
        if l[i] > h[i - 2]:
            return "BULL"
        if h[i] < l[i - 2]:
            return "BEAR"
    return "NONE"

# ── Circuit Breaker ───────────────────────────────────────────────────────────

def check_circuit_breaker(high, low, atr: np.ndarray,
                          mult: float = 3.0, bars: int = 10) -> bool:
    h, l = np.asarray(high, float), np.asarray(low, float)
    for i in range(len(h) - 1, max(len(h) - bars - 1, 0), -1):
        if atr[i] > 0 and (h[i] - l[i]) > mult * atr[i]:
            return True
    return False

# ── HTF EHM (Enhanced HTF Momentum) ──────────────────────────────────────────

def htf_score(klines_15m, klines_1h, klines_4h) -> float:
    """
    Score 0-1:  0.5 = neutral, >0.5 = bullish bias, <0.5 = bearish bias.
    Pondera 4H doble que 1H y cuádruple que 15m.
    """
    scores, weights = [], []
    for klines, weight in [(klines_15m, 1), (klines_1h, 2), (klines_4h, 4)]:
        if len(klines) < 30:
            continue
        arr   = np.array(klines)
        c     = arr[:, 4].astype(float)
        ema20 = _ema(c, 20)
        ema50 = _ema(c, 50) if len(c) >= 50 else _ema(c, 20)
        trend = 1 if ema20[-1] > ema50[-1] else -1
        mom   = _safe(calc_momentum(c, 10)[-1])
        s     = 0.5 + 0.5 * trend * min(abs(mom) * 10, 1.0)
        scores.append(s * weight)
        weights.append(weight)
    return sum(scores) / sum(weights) if weights else 0.5

# ── Score compuesto optimizado ────────────────────────────────────────────────

def composite_score(
    direction: str,
    adx:       float,
    cvd:       float,
    momentum:  float,
    mfi:       float,
    vdi:       float,
    structure: str,
    tl_break:  str,
    htf_s:     float,
    fvg:       str,
    funding:   float = 0.0,
) -> float:
    """
    Score 0-100 ponderado al perfil SHORT ganador.

    Pesos (suma = 100 + 2 FVG bonus + 3 funding bonus):
      ADX:        20  — tendencia necesaria
      CVD:        20  — confirmación volumen (++ SHORT)
      Momentum:   15
      MFI:        12  — extremos premiados
      VDI:         8
      Estructura: 15  — BoS↓/CHoCH↓ muy fiables
      HTF:         8
      FVG:         2  (bonus)
      Funding:     3  (bonus cuando confirma dirección)
    """
    s = 0.0

    # ADX (20 pts)
    s += min(_safe(adx) / 40.0, 1.0) * 20

    # CVD (20 pts)
    cvd_v = _safe(cvd)
    s += max(0.0, min(cvd_v if direction == "LONG" else -cvd_v, 1.0)) * 20

    # Momentum (15 pts)
    mom = _safe(momentum)
    s += max(0.0, min((mom if direction == "LONG" else -mom) * 30, 1.0)) * 15

    # MFI (12 pts)
    mfi_v = _safe(mfi, 50.0)
    if direction == "LONG":
        s += max(0.0, (mfi_v - 50) / 50) * 12
    else:
        s += max(0.0, (50 - mfi_v) / 50) * 12

    # VDI (8 pts)
    vdi_v = _safe(vdi)
    s += max(0.0, min((vdi_v if direction == "LONG" else -vdi_v) / 3.0, 1.0)) * 8

    # Estructura (15 pts)
    struct_pts = {
        "CHoCH↑": (15 if direction == "LONG"  else 0),
        "CHoCH↓": (15 if direction == "SHORT" else 0),
        "BoS↑":   (10 if direction == "LONG"  else 0),
        "BoS↓":   (10 if direction == "SHORT" else 0),
    }
    s += struct_pts.get(structure, 0)

    # HTF (8 pts)
    htf_v = _safe(htf_s, 0.5)
    s += (htf_v if direction == "LONG" else 1.0 - htf_v) * 8

    # FVG bonus (2 pts)
    if (direction == "LONG" and fvg == "BULL") or (direction == "SHORT" and fvg == "BEAR"):
        s += 2

    # Funding bonus (3 pts)
    # Funding positivo → longs pagan shorts → presión bajista → favorece SHORT
    fr = _safe(funding)
    if direction == "SHORT" and fr > 0.0001:
        s += min(fr / 0.001, 1.0) * 3
    elif direction == "LONG" and fr < -0.0001:
        s += min(abs(fr) / 0.001, 1.0) * 3

    return round(min(s, 100.0), 1)


def score_to_tier(score: float) -> str:
    import math
    import config as C
    if not math.isfinite(score):
        return "NONE"
    if score >= C.SUP_SCORE:
        return "SUP"
    if score >= C.FUEL_SCORE:
        return "FUEL"
    if score >= C.MIN_SCORE:
        return "STD"
    return "NONE"

# ── analyze() ─────────────────────────────────────────────────────────────────

def analyze(
    symbol:      str,
    klines_3m:   list,
    klines_15m:  list,
    klines_1h:   list,
    klines_4h:   list,
    funding_rate: float = 0.0,
) -> Signal:
    """
    Entrada principal del módulo. Recibe klines y devuelve Signal.
    funding_rate: float del endpoint get_funding_rate() (0.0 si no disponible).
    """
    import config as C

    def _no_signal(reason: str) -> Signal:
        log.debug("[%s] descartado: %s", symbol, reason)
        return Signal(
            symbol=symbol, direction="NONE", score=0, tier="NONE",
            entry=0, sl=0, tp1=0, tp2=0, atr=0, adx=0, mfi=50,
            vdi=0, cvd=0, momentum=0, htf_score=0,
            structure="NONE", tl_break="NONE",
            funding_rate=funding_rate, reason=reason,
        )

    if len(klines_3m) < 60:
        return _no_signal("insufficient_data")

    arr = np.array(klines_3m, dtype=float)
    o, h, l, c, v = arr[:, 1], arr[:, 2], arr[:, 3], arr[:, 4], arr[:, 5]

    # ── Indicadores base ──────────────────────────────────────────────────────
    atr_arr             = calc_atr(h, l, c, C.ATR_LEN)
    adx_arr, pdi, mdi   = calc_adx(h, l, c, C.ADX_LEN)

    atr  = _safe(atr_arr[-1])
    adx  = _safe(adx_arr[-1])
    pdim = _safe(pdi[-1])
    mdim = _safe(mdi[-1])

    if atr <= 0 or not np.isfinite(atr):
        return _no_signal("invalid_atr")

    cvd_val = _safe(calc_cvd(o, c, v)[-1])
    mom_val = _safe(calc_momentum(c, 10)[-1])
    mfi_val = _safe(calc_mfi(h, l, c, v, 14)[-1], 50.0)
    vdi_val = _safe(calc_vdi(c, v, 20)[-1])

    cb        = C.CB_ENABLED and check_circuit_breaker(h, l, atr_arr, C.CB_ATR_MULT, C.CB_BARS)
    structure = detect_structure(h, l, c, 5)
    tl_break  = detect_tl_break(h, l, c, 20)
    fvg       = detect_fvg(h, l)
    htf_s     = _safe(htf_score(klines_15m, klines_1h, klines_4h), 0.5)

    # ── Dirección ─────────────────────────────────────────────────────────────
    if C.REQUIRE_TL_BREAK and tl_break == "NONE":
        return _no_signal("no_tl_break")

    direction = tl_break if tl_break != "NONE" else ("LONG" if pdim > mdim else "SHORT")

    # ── HTF alignment check ───────────────────────────────────────────────────
    htf_aligned = 0
    for klines, _ in [(klines_15m, 1), (klines_1h, 2), (klines_4h, 4)]:
        if len(klines) < 30:
            continue
        a   = np.array(klines, dtype=float)
        cc  = a[:, 4]
        e20 = _ema(cc, 20)
        e50 = _ema(cc, 50) if len(cc) >= 50 else e20
        if (direction == "LONG"  and e20[-1] > e50[-1]) or \
           (direction == "SHORT" and e20[-1] < e50[-1]):
            htf_aligned += 1
    if htf_aligned < C.HTF_MIN_ALIGNED:
        return _no_signal(f"htf_not_aligned({htf_aligned}/{C.HTF_MIN_ALIGNED})")

    # ── Score y tier ──────────────────────────────────────────────────────────
    score = composite_score(
        direction, adx, cvd_val, mom_val, mfi_val,
        vdi_val, structure, tl_break, htf_s, fvg, funding_rate,
    )
    tier = score_to_tier(score)

    # ── SL / TP ───────────────────────────────────────────────────────────────
    entry    = _safe(c[-1])
    sl_mult  = C.SL_ATR_MULT
    tp1_mult = C.TP1_ATR_MULT
    tp2_mult = C.TP2_ATR_MULT

    if direction == "LONG":
        sl  = entry - atr * sl_mult
        tp1 = entry + atr * tp1_mult
        tp2 = entry + atr * tp2_mult
    else:
        sl  = entry + atr * sl_mult
        tp1 = entry - atr * tp1_mult
        tp2 = entry - atr * tp2_mult

    return Signal(
        symbol=symbol, direction=direction, score=score, tier=tier,
        entry=entry, sl=sl, tp1=tp1, tp2=tp2, atr=atr, adx=adx,
        mfi=mfi_val, vdi=vdi_val, cvd=cvd_val, momentum=mom_val,
        htf_score=htf_s, structure=structure, tl_break=tl_break,
        tl_break_active=(tl_break != "NONE"),
        circuit_breaker=cb,
        funding_rate=funding_rate,
        reason="ok",
    )
