"""
QF×JP Bot v6.5 — Indicators PINE SYNC
Integra lógica del Pine Script QF×JP v3.6 [PREDATOR]:
  [RSI3]  Consenso RSI 7/14/21 — todos > o < 50
  [SQP]   Squeeze pull-back post-squeeze fire
  [VDA]   Aceleración VDI (vdi_z acelerando → refuerzo)
  [VWAP2] VWAP + bandas ±1σ — near_vwap_lo / near_vwap_hi
  [OBP2]  Aproximación OB premium (<1.5×ATR del OB)
  [CVD2]  Rolling window 60 barras (más reactivo en 3min)
  [MFI2]  Divergencia en ventana corta 5 barras
  [EQH/EQL] Equal highs/lows con sweep detection
  [BRAKER] Breaker blocks (OB roto → inversión de rol)
  [EHM]   Pesos exponenciales HTF: 15m=1, 1h=2, 4h=4
  [CONV]  Convicción 0-20 items (alineado con Pine)
  [PRE]   Pre-señal anticipatoria (score < STD pero acelerando)
  Funding rate integrado en composite_score (3 pts bonus)
"""
import logging
import math
import warnings
from dataclasses import dataclass, field

import numpy as np

import config as C

warnings.filterwarnings("ignore", category=RuntimeWarning)

log = logging.getLogger("indicators")

# ── Signal dataclass ──────────────────────────────────────────────────────────

@dataclass
class Signal:
    symbol:          str
    direction:       str    # LONG | SHORT | NONE
    score:           float
    tier:            str    # PRE | STD | FUEL | SUP | NONE
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
    # ── Nuevos campos v3.6 ────────────────────────────────────────────────
    conviction:      int   = 0    # 0-20
    rsi3_consensus:  bool  = False
    sqp_setup:       bool  = False
    vdi_accel:       bool  = False
    near_vwap_band:  str   = "NONE"  # NONE | LOWER | UPPER
    ob_premium:      bool  = False
    eql_sweep:       bool  = False
    eqh_sweep:       bool  = False
    breaker_block:   bool  = False
    pre_signal:      bool  = False

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
    plus_dm  = np.where((up > down) & (up > 0),   up,   0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
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

# ── CVD [CVD2] v3.6: rolling window configurable (default 60 barras) ──────────

def calc_cvd(open_, close, volume, roll: int = 60) -> np.ndarray:
    """
    [CVD2] Rolling reducido 100→60 para mayor reactividad en 3min.
    Mantiene normalización z-score para comparar entre símbolos.
    """
    o, c, v = (np.asarray(x, float) for x in (open_, close, volume))
    hl_rng  = np.abs(c - o)  # estimación cuerpo de vela
    # buy/sell volume estimado por posición del cierre en el rango
    # (idéntico al método Pine: bvol = (close-low)/hl_rng * vol)
    bull = np.where(c > o, v, 0.0)
    bear = np.where(c <= o, v, 0.0)
    delta = bull - bear
    total = bull + bear
    # Normalizar por volumen total de vela
    cvd_norm = np.divide(delta, total, out=np.zeros_like(delta), where=total > 0)
    # EMA5 como en v6.4
    cvd_ema5 = _ema(cvd_norm, 5)
    # Rolling sum sobre ventana configurable (Pine: sma * roll)
    roll_sum = np.zeros_like(cvd_ema5)
    for i in range(roll - 1, len(cvd_ema5)):
        roll_sum[i] = cvd_ema5[i - roll + 1 : i + 1].sum()
    return roll_sum

# ── MFI + [MFI2] ventana corta ────────────────────────────────────────────────

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


def calc_mfi_divergence(close, mfi_arr: np.ndarray, window_long: int = 14,
                        window_short: int = 5) -> tuple[bool, bool]:
    """
    [MFI2] v3.6: divergencia en ventana normal Y ventana corta 5 barras.
    Returns (bull_div, bear_div) — OR de ambas ventanas.
    """
    c = np.asarray(close, float)
    if len(c) < window_long + 2:
        return False, False
    mfi = mfi_arr

    # Ventana normal
    bull_div_n = (c[-1] < c[-window_long - 1] and
                  mfi[-1] > mfi[-window_long - 1] and
                  mfi[-1] < 50)
    bear_div_n = (c[-1] > c[-window_long - 1] and
                  mfi[-1] < mfi[-window_long - 1] and
                  mfi[-1] > 50)

    # Ventana corta [MFI2]
    bull_div_s = False
    bear_div_s = False
    if len(c) > window_short + 1:
        bull_div_s = (c[-1] < c[-window_short - 1] and
                      mfi[-1] > mfi[-window_short - 1] and
                      mfi[-1] < 50)
        bear_div_s = (c[-1] > c[-window_short - 1] and
                      mfi[-1] < mfi[-window_short - 1] and
                      mfi[-1] > 50)

    return (bull_div_n or bull_div_s), (bear_div_n or bear_div_s)

# ── VDI + [VDA] Aceleración ───────────────────────────────────────────────────

def calc_vdi(close, volume, period: int = 20) -> np.ndarray:
    c, v   = np.asarray(close, float), np.asarray(volume, float)
    vwap_d = (c - _sma(c, period)) * v
    std    = np.nanstd(vwap_d[-period:])
    result = np.divide(vwap_d, std + 1e-9,
                       out=np.zeros_like(vwap_d), where=(std + 1e-9) > 0)
    return np.nan_to_num(result, nan=0.0, posinf=0.0, neginf=0.0)


def vdi_z_score(vdi_arr: np.ndarray, period: int = 20) -> np.ndarray:
    """Z-score del VDI para detectar desequilibrio anómalo."""
    out = np.zeros_like(vdi_arr)
    for i in range(period, len(vdi_arr)):
        window = vdi_arr[i - period : i + 1]
        m, s   = window.mean(), window.std()
        out[i] = (vdi_arr[i] - m) / (s + 1e-9)
    return out


def calc_vdi_accel(vdi_z: np.ndarray, threshold: float = 0.5) -> tuple[bool, bool]:
    """
    [VDA] v3.6: VDI z-score acelerando respecto a 2 barras atrás.
    Returns (accel_bull, accel_bear)
    """
    if len(vdi_z) < 3:
        return False, False
    curr, prev2 = vdi_z[-1], vdi_z[-3]
    accel_bull = (curr > prev2) and (curr > threshold)
    accel_bear = (curr < prev2) and (curr < -threshold)
    return accel_bull, accel_bear

# ── RSI + [RSI3] Multi-período ────────────────────────────────────────────────

def calc_rsi(close, period: int = 14) -> np.ndarray:
    c    = np.asarray(close, float)
    diff = np.diff(c)
    gain = np.where(diff > 0, diff, 0.0)
    loss = np.where(diff < 0, -diff, 0.0)
    gain = np.concatenate([[gain[0]], gain])
    loss = np.concatenate([[loss[0]], loss])
    avg_gain = _rma(gain, period)
    avg_loss = _rma(loss, period)
    rs  = np.divide(avg_gain, avg_loss + 1e-12,
                    out=np.ones_like(avg_gain), where=avg_loss > 0)
    return 100.0 - (100.0 / (1.0 + rs))


def calc_rsi3_consensus(close) -> tuple[bool, bool, float, float, float]:
    """
    [RSI3] v3.6: consenso RSI 7/14/21.
    Returns (bull_consensus, bear_consensus, rsi7, rsi14, rsi21)
    Bull consensus: los 3 RSI > 50
    Bear consensus: los 3 RSI < 50
    """
    if len(close) < 25:
        return False, False, 50.0, 50.0, 50.0
    rsi7  = _safe(calc_rsi(close, 7)[-1],  50.0)
    rsi14 = _safe(calc_rsi(close, 14)[-1], 50.0)
    rsi21 = _safe(calc_rsi(close, 21)[-1], 50.0)
    bull  = rsi7 > 50 and rsi14 > 50 and rsi21 > 50
    bear  = rsi7 < 50 and rsi14 < 50 and rsi21 < 50
    return bull, bear, rsi7, rsi14, rsi21

# ── VWAP + [VWAP2] Bandas ±1σ ────────────────────────────────────────────────

def calc_vwap_bands(high, low, close, volume, std_window: int = 20) -> tuple[float, float, float]:
    """
    [VWAP2] v3.6: VWAP + desviación estándar del precio vs VWAP.
    Returns (vwap, upper_1sigma, lower_1sigma)
    """
    h, l, c, v = (np.asarray(x, float) for x in (high, low, close, volume))
    hlc3 = (h + l + c) / 3.0
    cum_vol  = np.cumsum(v)
    cum_pv   = np.cumsum(hlc3 * v)
    vwap_arr = np.divide(cum_pv, cum_vol + 1e-12,
                         out=np.zeros_like(cum_pv), where=cum_vol > 0)
    vwap = _safe(vwap_arr[-1])
    # Desviación estándar del hlc3 de las últimas std_window barras
    window   = min(std_window, len(hlc3))
    dev      = float(np.std(hlc3[-window:])) if window > 1 else 0.0
    return vwap, vwap + dev, vwap - dev

# ── Squeeze Momentum + [SQP] Pull-back ────────────────────────────────────────

def detect_squeeze(close, high, low, length: int = 20,
                   bb_mult: float = 2.0, kc_mult: float = 1.5) -> tuple[bool, bool, float]:
    """
    Squeeze momentum: BB dentro de KC → squeeze.
    Returns (sq_fire_bull, sq_fire_bear, sq_val)
    sq_fire: squeeze acaba de liberar.
    """
    c, h, l = np.asarray(close, float), np.asarray(high, float), np.asarray(low, float)
    if len(c) < length + 2:
        return False, False, 0.0

    # Bollinger Bands
    basis = _sma(c, length)
    dev   = np.array([c[i - length + 1:i + 1].std() for i in range(length - 1, len(c))])
    dev   = np.concatenate([np.full(length - 1, dev[0]), dev])
    bb_hi = basis + bb_mult * dev
    bb_lo = basis - bb_mult * dev

    # Keltner Channels
    atr_sq = calc_atr(h, l, c, length)
    ema_c  = _ema(c, length)
    kc_hi  = ema_c + kc_mult * atr_sq
    kc_lo  = ema_c - kc_mult * atr_sq

    # Squeeze on/off
    sq_on_prev = bb_hi[-2] < kc_hi[-2] and bb_lo[-2] > kc_lo[-2]
    sq_on_curr = bb_hi[-1] < kc_hi[-1] and bb_lo[-1] > kc_lo[-1]
    sq_fire    = not sq_on_curr and sq_on_prev  # liberó en esta barra

    # Squeeze value (linreg del cierre menos midpoint)
    win = min(length, len(c))
    highest = np.max(h[-win:])
    lowest  = np.min(l[-win:])
    mid_val = (highest + lowest) / 2
    sq_val  = _safe(c[-1] - (mid_val + _safe(basis[-1])) / 2)

    return sq_fire and sq_val > 0, sq_fire and sq_val < 0, sq_val


def detect_sqp(close, high, low, ema_arr: np.ndarray,
               sq_bull_fired: bool, sq_bear_fired: bool,
               cvd_rising: bool, bars_since_fire: int,
               sqp_bars: int = 5) -> tuple[bool, bool]:
    """
    [SQP] v3.6: Pull-back a la EMA luego de squeeze fire.
    Bull SQP: precio retrocede a EMA tras sq_bull_fire, CVD sigue alcista.
    Bear SQP: precio sube a EMA tras sq_bear_fire, CVD sigue bajista.
    """
    if len(close) < 3 or len(ema_arr) < 1:
        return False, False
    c      = close[-1]
    ema    = _safe(ema_arr[-1])
    is_bullish_candle = close[-1] > close[-2]
    is_bearish_candle = close[-1] < close[-2]

    sqp_long  = (sq_bull_fired and
                 bars_since_fire <= sqp_bars and
                 c <= ema * 1.001 and
                 is_bullish_candle and
                 cvd_rising)
    sqp_short = (sq_bear_fired and
                 bars_since_fire <= sqp_bars and
                 c >= ema * 0.999 and
                 is_bearish_candle and
                 not cvd_rising)
    return sqp_long, sqp_short

# ── Equal Highs / Equal Lows [SMC2] ──────────────────────────────────────────

def detect_eqh_eql(high, low, close, atr: float,
                   lookback: int = 20, tol_atr: float = 0.15) -> tuple[bool, bool, bool, bool]:
    """
    [SMC2/EQH/EQL] Detecta igualdad de máximos/mínimos y sus sweeps.
    Returns (eqh_detected, eql_detected, eqh_sweep, eql_sweep)
    """
    h, l, c = np.asarray(high, float), np.asarray(low, float), np.asarray(close, float)
    if len(h) < lookback + 2:
        return False, False, False, False

    tol = atr * tol_atr
    curr_h = h[-1]
    curr_l = l[-1]
    curr_c = c[-1]
    prev_c = c[-2]

    # Buscar igual máximo en lookback
    eqh_detected = False
    eqh_price    = None
    for i in range(2, min(lookback + 1, len(h))):
        diff = abs(curr_h - h[-i])
        if diff < tol and h[-i] > 0:
            eqh_detected = True
            eqh_price    = h[-i]
            break

    # Buscar igual mínimo en lookback
    eql_detected = False
    eql_price    = None
    for i in range(2, min(lookback + 1, len(l))):
        diff = abs(curr_l - l[-i])
        if diff < tol and l[-i] > 0:
            eql_detected = True
            eql_price    = l[-i]
            break

    # Sweep: precio cruza la zona y cierra al otro lado
    eqh_sweep = (eqh_detected and eqh_price is not None and
                 h[-1] > eqh_price and curr_c < eqh_price)
    eql_sweep = (eql_detected and eql_price is not None and
                 l[-1] < eql_price and curr_c > eql_price)

    return eqh_detected, eql_detected, eqh_sweep, eql_sweep

# ── Order Block + Premium Zone + [OBP2] aproximación ─────────────────────────

def detect_ob_and_premium(high, low, close, open_, atr: float,
                          ob_imp: float = 1.5, ob_bars: int = 50,
                          prem_pct: float = 0.33,
                          obp2_dist: float = 1.5) -> dict:
    """
    Detecta Order Blocks alcistas/bajistas y zonas premium.
    Returns dict con:
      - in_bull_ob, in_bear_ob
      - in_bull_ob_premium, in_bear_ob_premium
      - ob_bull_approach, ob_bear_approach   [OBP2]
      - bull_ob_hi, bull_ob_lo, bear_ob_hi, bear_ob_lo
    """
    h, l, c, o = (np.asarray(x, float) for x in (high, low, close, open_))
    if len(c) < ob_bars + 3:
        return {k: False for k in [
            "in_bull_ob", "in_bear_ob",
            "in_bull_ob_premium", "in_bear_ob_premium",
            "ob_bull_approach", "ob_bear_approach",
        ]}

    curr = c[-1]
    # Buscar el OB bajista más reciente (vela bajista fuerte seguida de impulso)
    bob_hi, bob_lo = None, None
    sob_hi, sob_lo = None, None

    for i in range(1, min(ob_bars, len(c) - 2)):
        # Bull OB: impulso alcista tras vela bajista
        if (c[-i] - o[-i]) > atr * ob_imp and c[-(i+1)] < o[-(i+1)]:
            bob_hi = o[-(i+1)]
            bob_lo = c[-(i+1)]
            break

    for i in range(1, min(ob_bars, len(c) - 2)):
        # Bear OB: impulso bajista tras vela alcista
        if (o[-i] - c[-i]) > atr * ob_imp and c[-(i+1)] > o[-(i+1)]:
            sob_hi = c[-(i+1)]
            sob_lo = o[-(i+1)]
            break

    result = {
        "in_bull_ob":           False,
        "in_bear_ob":           False,
        "in_bull_ob_premium":   False,
        "in_bear_ob_premium":   False,
        "ob_bull_approach":     False,
        "ob_bear_approach":     False,
        "bull_ob_hi": bob_hi,
        "bull_ob_lo": bob_lo,
        "bear_ob_hi": sob_hi,
        "bear_ob_lo": sob_lo,
    }

    # Bull OB checks
    if bob_hi is not None and bob_lo is not None:
        result["in_bull_ob"] = bob_lo <= curr <= bob_hi
        prem_lo = bob_hi - (bob_hi - bob_lo) * prem_pct
        result["in_bull_ob_premium"] = result["in_bull_ob"] and curr >= prem_lo
        # [OBP2]: acercándose desde abajo
        result["ob_bull_approach"] = (not result["in_bull_ob"] and
                                      prem_lo - atr * obp2_dist < curr < prem_lo)

    # Bear OB checks
    if sob_hi is not None and sob_lo is not None:
        result["in_bear_ob"] = sob_lo <= curr <= sob_hi
        prem_hi = sob_lo + (sob_hi - sob_lo) * prem_pct
        result["in_bear_ob_premium"] = result["in_bear_ob"] and curr <= prem_hi
        # [OBP2]: acercándose desde arriba
        result["ob_bear_approach"] = (not result["in_bear_ob"] and
                                      prem_hi < curr < prem_hi + atr * obp2_dist)

    return result

# ── Breaker Blocks ────────────────────────────────────────────────────────────

def detect_breaker_blocks(high, low, close, open_, atr: float,
                           ob_imp: float = 1.5) -> tuple[bool, bool]:
    """
    Breaker Block: OB roto → inversión de rol.
    Bull breaker: OB bajista roto por cierre arriba → ahora soporte.
    Bear breaker: OB alcista roto por cierre abajo → ahora resistencia.
    Returns (in_bull_breaker, in_bear_breaker)
    """
    h, l, c, o = (np.asarray(x, float) for x in (high, low, close, open_))
    if len(c) < 10:
        return False, False

    curr = c[-1]
    # Buscar OBs rotos recientes
    in_bull_brk = False
    in_bear_brk = False

    for i in range(2, min(30, len(c) - 2)):
        # OB alcista roto → bear breaker
        if (c[-i] - o[-i]) > atr * ob_imp and c[-(i+1)] < o[-(i+1)]:
            bob_hi = o[-(i+1)]
            bob_lo = c[-(i+1)]
            # ¿Fue roto (precio cerró por debajo)?
            was_broken = any(c[-(i-j)] < bob_lo for j in range(1, min(i, 5)))
            if was_broken and bob_lo <= curr <= bob_hi:
                in_bear_brk = True
                break

        # OB bajista roto → bull breaker
        if (o[-i] - c[-i]) > atr * ob_imp and c[-(i+1)] > o[-(i+1)]:
            sob_hi = c[-(i+1)]
            sob_lo = o[-(i+1)]
            was_broken = any(c[-(i-j)] > sob_hi for j in range(1, min(i, 5)))
            if was_broken and sob_lo <= curr <= sob_hi:
                in_bull_brk = True
                break

    return in_bull_brk, in_bear_brk

# ── Dark Pool [L4] ────────────────────────────────────────────────────────────

def detect_dark_pool(high, low, close, open_, volume,
                     vol_mult: float = 2.5, vol_base_len: int = 20) -> tuple[bool, bool]:
    """
    [L4] Dark Pool: vela con volumen anómalo (>2.5x base) y rango estrecho
    (<0.6×ATR de la propia vela), señal de bloque institucional.
    Returns (dp_buy, dp_sell)
    """
    h, l, c, o, v = (np.asarray(x, float) for x in (high, low, close, open_, volume))
    if len(v) < vol_base_len + 1:
        return False, False

    vol_base = v[-vol_base_len:].mean()
    vol_spike = v[-1] > vol_base * vol_mult

    rng       = h[-1] - l[-1]
    rng_avg   = (h[-vol_base_len:] - l[-vol_base_len:]).mean()
    rng_narrow = rng < rng_avg * 0.6

    dp_buy  = vol_spike and rng_narrow and c[-1] > o[-1]
    dp_sell = vol_spike and rng_narrow and c[-1] < o[-1]
    return dp_buy, dp_sell

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

# ── HTF Score [EHM] Pesos exponenciales (15m=1, 1h=2, 4h=4) ──────────────────

def htf_score(klines_15m, klines_1h, klines_4h) -> float:
    """
    [EHM] Score 0-1 con pesos exponenciales: 4H pesa 4×, 1H pesa 2×, 15m pesa 1×.
    Total = 7. Pine usa 15m=1, 1h=2, 4h=4, W=8 (15 total).
    Sin W aquí porque no lo fetcha el bot.
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

# ── Score compuesto v3.6 ──────────────────────────────────────────────────────

def composite_score(
    direction:  str,
    adx:        float,
    cvd:        float,
    momentum:   float,
    mfi:        float,
    vdi:        float,
    structure:  str,
    tl_break:   str,
    htf_s:      float,
    fvg:        str,
    funding:    float = 0.0,
    # v3.6 extras
    rsi3_bull:  bool  = False,
    rsi3_bear:  bool  = False,
    sqp_setup:  bool  = False,
    vdi_accel:  bool  = False,
    near_vwap_lo: bool = False,
    near_vwap_hi: bool = False,
    ob_premium: bool  = False,
    eql_sweep:  bool  = False,
    eqh_sweep:  bool  = False,
    breaker_bl: bool  = False,
    mfi_div:    bool  = False,
) -> float:
    """
    Score 0-100 ponderado al perfil SHORT ganador.
    Pesos base (suma = 100):
      ADX:        20  — tendencia necesaria
      CVD:        20  — confirmación volumen
      Momentum:   15
      MFI:        12  — extremos premiados
      VDI:         8
      Estructura: 15  — BoS↓/CHoCH↓ fiables
      HTF:         8
      FVG:         2  (bonus)
      Funding:     3  (bonus)
    Bonus v3.6 (extra, cap en 100):
      RSI3 consenso:  +2
      SQP pull-back:  +2
      VDI accel:      +1
      VWAP banda:     +1
      OB Premium:     +2
      EQH/EQL sweep:  +2
      Breaker block:  +1
      MFI div:        +1
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

    # HTF (8 pts) — EHM pondera 4h doble que 1h
    htf_v = _safe(htf_s, 0.5)
    s += (htf_v if direction == "LONG" else 1.0 - htf_v) * 8

    # FVG bonus (2 pts)
    if (direction == "LONG" and fvg == "BULL") or (direction == "SHORT" and fvg == "BEAR"):
        s += 2

    # Funding bonus (3 pts)
    fr = _safe(funding)
    if direction == "SHORT" and fr > 0.0001:
        s += min(fr / 0.001, 1.0) * 3
    elif direction == "LONG" and fr < -0.0001:
        s += min(abs(fr) / 0.001, 1.0) * 3

    # ── Bonus v3.6 Pine ──────────────────────────────────────────────────────
    if direction == "LONG":
        if rsi3_bull:   s += 2   # RSI3 todos > 50
        if sqp_setup:   s += 2   # Pull-back post-squeeze
        if vdi_accel:   s += 1   # VDI acelerando
        if near_vwap_lo: s += 1  # Precio en banda inferior VWAP
        if ob_premium:  s += 2   # En zona premium OB alcista
        if eql_sweep:   s += 2   # EQL barridos → reversal long
        if breaker_bl:  s += 1   # Breaker block alcista
        if mfi_div:     s += 1   # Divergencia MFI alcista
    else:
        if rsi3_bear:   s += 2   # RSI3 todos < 50
        if sqp_setup:   s += 2   # Pull-back post-squeeze
        if vdi_accel:   s += 1   # VDI acelerando bajista
        if near_vwap_hi: s += 1  # Precio en banda superior VWAP
        if ob_premium:  s += 2   # En zona premium OB bajista
        if eqh_sweep:   s += 2   # EQH barridos → reversal short
        if breaker_bl:  s += 1   # Breaker block bajista
        if mfi_div:     s += 1   # Divergencia MFI bajista

    return round(min(s, 100.0), 1)


def score_to_tier(score: float) -> str:
    if not math.isfinite(score):
        return "NONE"
    if score >= C.SUP_SCORE:
        return "SUP"
    if score >= C.FUEL_SCORE:
        return "FUEL"
    if score >= C.MIN_SCORE:
        return "STD"
    return "NONE"


def calc_conviction(
    direction: str,
    norm_score: float,
    sig_alive:  bool,
    htf_score_val: int,
    asym_bull: bool, asym_bear: bool,
    sell_exhausted: bool, buy_exhausted: bool,
    tl_break: str,
    dp_buy: bool, dp_sell: bool,
    cvd_rising: bool,
    sq_bull: bool, sq_bear: bool,
    in_bull_fvg: bool, in_bear_fvg: bool,
    in_bull_ob: bool, in_bear_ob: bool,
    in_brk_bull: bool, in_brk_bear: bool,
    vdi_bull: bool, vdi_bear: bool,
    in_bull_ob_premium: bool, in_bear_ob_premium: bool,
    mfi_os: bool, mfi_ob: bool,
    mfi_bull_div: bool, mfi_bear_div: bool,
    eql_sweep: bool, eqh_sweep: bool,
    rsi3_bull: bool, rsi3_bear: bool,
    sqp_long: bool, sqp_short: bool,
    vdi_accel_bull: bool, vdi_accel_bear: bool,
    near_vwap_lo: bool, near_vwap_hi: bool,
    liq_bull_sweep: bool = False, liq_bear_sweep: bool = False,
    choch_bull: bool = False, choch_bear: bool = False,
) -> int:
    """
    [CONV] v3.6: 0-20 items de convicción.
    Alineado con Pine Script v3.6 long_conv / short_conv.
    """
    if direction == "LONG":
        score = int(norm_score > 0.10)
        score += int(sig_alive)
        score += int(htf_score_val >= 2)
        score += int(asym_bull)
        score += int(sell_exhausted)
        score += int(tl_break == "LONG" or liq_bull_sweep or choch_bull)
        score += int(dp_buy)
        score += int(cvd_rising)
        score += int(sq_bull or in_bull_fvg or in_bull_ob or in_brk_bull)
        score += int(vdi_bull)
        score += int(in_bull_ob_premium)
        score += int(mfi_os or mfi_bull_div)
        score += int(eql_sweep)
        score += int(rsi3_bull)
        score += int(sqp_long)
        score += int(vdi_accel_bull)
        score += int(near_vwap_lo)
        return min(score, 20)
    else:  # SHORT
        score = int(norm_score < -0.10)
        score += int(sig_alive)
        score += int(htf_score_val >= 2)
        score += int(asym_bear)
        score += int(buy_exhausted)
        score += int(tl_break == "SHORT" or liq_bear_sweep or choch_bear)
        score += int(dp_sell)
        score += int(not cvd_rising)
        score += int(sq_bear or in_bear_fvg or in_bear_ob or in_brk_bear)
        score += int(vdi_bear)
        score += int(in_bear_ob_premium)
        score += int(mfi_ob or mfi_bear_div)
        score += int(eqh_sweep)
        score += int(rsi3_bear)
        score += int(sqp_short)
        score += int(vdi_accel_bear)
        score += int(near_vwap_hi)
        return min(score, 20)

# ── analyze() principal ───────────────────────────────────────────────────────

def analyze(
    symbol:       str,
    klines_3m:    list,
    klines_15m:   list,
    klines_1h:    list,
    klines_4h:    list,
    funding_rate: float = 0.0,
) -> Signal:
    """
    Entrada principal. Devuelve Signal enriquecido con lógica Pine v3.6.
    """

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
    atr_arr           = calc_atr(h, l, c, C.ATR_LEN)
    adx_arr, pdi, mdi = calc_adx(h, l, c, C.ADX_LEN)

    atr  = _safe(atr_arr[-1])
    adx  = _safe(adx_arr[-1])
    pdim = _safe(pdi[-1])
    mdim = _safe(mdi[-1])

    if atr <= 0 or not np.isfinite(atr):
        return _no_signal("invalid_atr")

    # [CVD2] rolling 60 barras
    cvd_arr = calc_cvd(o, c, v, roll=C.CVD_ROLL_WINDOW)
    cvd_val = _safe(cvd_arr[-1])
    cvd_rising = len(cvd_arr) > 3 and cvd_arr[-1] > cvd_arr[-4]

    mom_val = _safe(calc_momentum(c, 10)[-1])
    mfi_arr = calc_mfi(h, l, c, v, 14)
    mfi_val = _safe(mfi_arr[-1], 50.0)

    # [MFI2] divergencia ventana corta
    mfi_bull_div, mfi_bear_div = calc_mfi_divergence(c, mfi_arr, 14, 5)

    # VDI + [VDA] aceleración
    vdi_arr     = calc_vdi(c, v, 20)
    vdi_val     = _safe(vdi_arr[-1])
    vdi_z_arr   = vdi_z_score(vdi_arr, 20)
    vdi_z_val   = _safe(vdi_z_arr[-1])
    vdi_bull    = vdi_z_val >  1.5
    vdi_bear    = vdi_z_val < -1.5
    vdi_ab, vdi_accel_bear = calc_vdi_accel(vdi_z_arr)
    vdi_accel_bull = vdi_ab

    # [RSI3] multi-período
    rsi3_bull, rsi3_bear, rsi7, rsi14, rsi21 = calc_rsi3_consensus(c)

    # [VWAP2] bandas ±1σ
    vwap_val, vwap_hi, vwap_lo = calc_vwap_bands(h, l, c, v, 20)
    near_vwap_lo = c[-1] <= vwap_lo * 1.001
    near_vwap_hi = c[-1] >= vwap_hi * 0.999
    above_vwap   = c[-1] > vwap_val
    near_vwap_band = "LOWER" if near_vwap_lo else ("UPPER" if near_vwap_hi else "NONE")

    # [EQH/EQL] equal highs/lows
    eqh_det, eql_det, eqh_sweep, eql_sweep = detect_eqh_eql(
        h, l, c, atr, C.EQL_LEN, C.EQL_TOL
    )

    # Order Blocks + Premium + [OBP2]
    ob_data = detect_ob_and_premium(
        h, l, c, o, atr,
        ob_imp=1.5, ob_bars=50,
        prem_pct=0.33, obp2_dist=C.OBP2_DIST,
    )
    in_bull_ob_premium = ob_data["in_bull_ob_premium"]
    in_bear_ob_premium = ob_data["in_bear_ob_premium"]
    ob_bull_approach   = ob_data["ob_bull_approach"]
    ob_bear_approach   = ob_data["ob_bear_approach"]
    in_bull_ob         = ob_data["in_bull_ob"]
    in_bear_ob         = ob_data["in_bear_ob"]

    # Breaker Blocks
    in_brk_bull, in_brk_bear = detect_breaker_blocks(h, l, c, o, atr)

    # Squeeze + [SQP] pull-back
    sq_fire_bull, sq_fire_bear, sq_val = detect_squeeze(c, h, l, 20)
    ema20 = _ema(c, 20)
    # Estado simplificado de SQP: si hubo squeeze fire reciente
    sqp_long  = sq_fire_bull and cvd_rising and c[-1] <= ema20[-1] * 1.001 and c[-1] > c[-2]
    sqp_short = sq_fire_bear and not cvd_rising and c[-1] >= ema20[-1] * 0.999 and c[-1] < c[-2]

    # Circuit breaker, estructura, TL, FVG
    cb        = C.CB_ENABLED and check_circuit_breaker(h, l, atr_arr, C.CB_ATR_MULT, C.CB_BARS)
    structure = detect_structure(h, l, c, 5)
    tl_break  = detect_tl_break(h, l, c, 20)
    fvg       = detect_fvg(h, l)
    htf_s     = _safe(htf_score(klines_15m, klines_1h, klines_4h), 0.5)

    # MFI extremos
    mfi_os = mfi_val < 20
    mfi_ob = mfi_val > 80

    # Asimetría de momentum (Pine L6)
    win = min(10, len(c) - 1)
    up_rng = np.array([h[i] - l[i] if c[i] > o[i] else 0.0 for i in range(-win, 0)])
    dn_rng = np.array([h[i] - l[i] if c[i] < o[i] else 0.0 for i in range(-win, 0)])
    avg_up = up_rng.mean() if up_rng.sum() > 0 else 0.001
    avg_dn = dn_rng.mean() if dn_rng.sum() > 0 else 0.001
    asym_bull = (avg_up / avg_dn) >= 1.2 if avg_dn > 0 else False
    asym_bear = (avg_dn / avg_up) >= 1.2 if avg_up > 0 else False

    # Sell/buy exhaustion (HL/LH count simplificado)
    lows  = l[-20:]
    highs = h[-20:]
    hl_count = sum(1 for i in range(1, len(lows))  if lows[i] > lows[i-1])
    lh_count = sum(1 for i in range(1, len(highs)) if highs[i] < highs[i-1])
    sell_exhausted = hl_count >= 2
    buy_exhausted  = lh_count >= 2

    # [L4] Dark Pool
    dp_buy, dp_sell = detect_dark_pool(h, l, c, o, v)

    # Sig alive (decay): IC correlación simplificada
    if len(c) > 40:
        fwd    = np.diff(c[-41:]) / (c[-41:-1] + 1e-12)
        sc_arr = np.diff(c[-41:]) / (np.std(c[-41:]) + 1e-12)
        corr   = float(np.corrcoef(sc_arr[:-1], fwd[1:])[0, 1]) if len(fwd) > 5 else 0
        sig_alive = abs(corr) > 0.2
    else:
        sig_alive = True

    # ── Dirección ─────────────────────────────────────────────────────────────
    if C.REQUIRE_TL_BREAK and tl_break == "NONE":
        return _no_signal("no_tl_break")

    direction = tl_break if tl_break != "NONE" else ("LONG" if pdim > mdim else "SHORT")

    # ── HTF alignment ─────────────────────────────────────────────────────────
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

    # ── Score compuesto v3.6 ──────────────────────────────────────────────────
    score = composite_score(
        direction, adx, cvd_val, mom_val, mfi_val, vdi_val,
        structure, tl_break, htf_s, fvg, funding_rate,
        rsi3_bull  = rsi3_bull,
        rsi3_bear  = rsi3_bear,
        sqp_setup  = (sqp_long if direction == "LONG" else sqp_short),
        vdi_accel  = (vdi_accel_bull if direction == "LONG" else vdi_accel_bear),
        near_vwap_lo = near_vwap_lo,
        near_vwap_hi = near_vwap_hi,
        ob_premium = (in_bull_ob_premium if direction == "LONG" else in_bear_ob_premium),
        eql_sweep  = eql_sweep,
        eqh_sweep  = eqh_sweep,
        breaker_bl = (in_brk_bull if direction == "LONG" else in_brk_bear),
        mfi_div    = (mfi_bull_div if direction == "LONG" else mfi_bear_div),
    )
    tier = score_to_tier(score)

    # ── Pre-señal anticipatoria [PRE] ──────────────────────────────────────────
    pre_signal = (tier == "NONE" and score >= C.PRE_SCORE and
                  (vdi_bull if direction == "LONG" else vdi_bear))

    # ── Convicción [CONV] ─────────────────────────────────────────────────────
    conviction = calc_conviction(
        direction     = direction,
        norm_score    = cvd_val,
        sig_alive     = sig_alive,
        htf_score_val = htf_aligned,
        asym_bull     = asym_bull,
        asym_bear     = asym_bear,
        sell_exhausted= sell_exhausted,
        buy_exhausted = buy_exhausted,
        tl_break      = tl_break,
        dp_buy        = dp_buy,
        dp_sell       = dp_sell,
        cvd_rising    = cvd_rising,
        sq_bull       = sq_fire_bull,
        sq_bear       = sq_fire_bear,
        in_bull_fvg   = fvg == "BULL",
        in_bear_fvg   = fvg == "BEAR",
        in_bull_ob    = in_bull_ob,
        in_bear_ob    = in_bear_ob,
        in_brk_bull   = in_brk_bull,
        in_brk_bear   = in_brk_bear,
        vdi_bull      = vdi_bull,
        vdi_bear      = vdi_bear,
        in_bull_ob_premium = in_bull_ob_premium,
        in_bear_ob_premium = in_bear_ob_premium,
        mfi_os        = mfi_os,
        mfi_ob        = mfi_ob,
        mfi_bull_div  = mfi_bull_div,
        mfi_bear_div  = mfi_bear_div,
        eql_sweep     = eql_sweep,
        eqh_sweep     = eqh_sweep,
        rsi3_bull     = rsi3_bull,
        rsi3_bear     = rsi3_bear,
        sqp_long      = sqp_long,
        sqp_short     = sqp_short,
        vdi_accel_bull = vdi_accel_bull,
        vdi_accel_bear = vdi_accel_bear,
        near_vwap_lo  = near_vwap_lo,
        near_vwap_hi  = near_vwap_hi,
        choch_bull    = structure in ("CHoCH↑",),
        choch_bear    = structure in ("CHoCH↓",),
    )

    # ── SL / TP ───────────────────────────────────────────────────────────────
    entry = _safe(c[-1])
    if direction == "LONG":
        sl  = entry - atr * C.SL_ATR_MULT
        tp1 = entry + atr * C.TP1_ATR_MULT
        tp2 = entry + atr * C.TP2_ATR_MULT
    else:
        sl  = entry + atr * C.SL_ATR_MULT
        tp1 = entry - atr * C.TP1_ATR_MULT
        tp2 = entry - atr * C.TP2_ATR_MULT

    return Signal(
        symbol         = symbol,
        direction      = direction,
        score          = score,
        tier           = tier,
        entry          = entry,
        sl             = sl,
        tp1            = tp1,
        tp2            = tp2,
        atr            = atr,
        adx            = adx,
        mfi            = mfi_val,
        vdi            = vdi_val,
        cvd            = cvd_val,
        momentum       = mom_val,
        htf_score      = htf_s,
        structure      = structure,
        tl_break       = tl_break,
        tl_break_active= (tl_break != "NONE"),
        circuit_breaker= cb,
        funding_rate   = funding_rate,
        reason         = "ok",
        # v3.6
        conviction     = conviction,
        rsi3_consensus = (rsi3_bull if direction == "LONG" else rsi3_bear),
        sqp_setup      = (sqp_long  if direction == "LONG" else sqp_short),
        vdi_accel      = (vdi_accel_bull if direction == "LONG" else vdi_accel_bear),
        near_vwap_band = near_vwap_band,
        ob_premium     = (in_bull_ob_premium if direction == "LONG" else in_bear_ob_premium),
        eql_sweep      = eql_sweep,
        eqh_sweep      = eqh_sweep,
        breaker_block  = (in_brk_bull if direction == "LONG" else in_brk_bear),
        pre_signal     = pre_signal,
    )
