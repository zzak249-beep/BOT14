"""
GUA-USDT Bot v3 — Indicadores Técnicos
Fixes: EMA warm-up · LiqSweep tolerancia dinámica
Nuevo: MFI · Compresión pre-breakout · Fortaleza relativa · Liquidaciones
"""

from __future__ import annotations
import numpy as np
from typing import Dict, List, Optional, Tuple


# ══════════════════════════════════════════════════════════════════════
#  CLÁSICOS
# ══════════════════════════════════════════════════════════════════════

def ema(values: List[float], period: int) -> np.ndarray:
    arr = np.array(values, dtype=float)
    k   = 2.0 / (period + 1)
    out = np.empty(len(arr))
    out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = arr[i] * k + out[i-1] * (1 - k)
    return out

def sma(values: List[float], period: int) -> np.ndarray:
    arr = np.array(values, dtype=float)
    out = np.full(len(arr), np.nan)
    for i in range(period - 1, len(arr)):
        out[i] = arr[i - period + 1:i + 1].mean()
    return out

def rsi(closes: List[float], period: int = 14) -> np.ndarray:
    arr = np.array(closes, dtype=float)
    d   = np.diff(arr)
    g   = np.where(d > 0, d, 0.0)
    l   = np.where(d < 0, -d, 0.0)
    n   = len(arr)
    ag  = np.zeros(n)
    al  = np.zeros(n)
    ag[period] = g[:period].mean()
    al[period] = l[:period].mean()
    for i in range(period + 1, n):
        ag[i] = (ag[i-1] * (period - 1) + g[i-1]) / period
        al[i] = (al[i-1] * (period - 1) + l[i-1]) / period
    rs  = np.where(al == 0, 100.0, ag / al)
    out = np.where(al == 0, 100.0, 100.0 - 100.0 / (1.0 + rs))
    out[:period] = np.nan
    return out

def atr(highs: List[float], lows: List[float], closes: List[float],
        period: int = 14) -> np.ndarray:
    h  = np.array(highs,  dtype=float)
    l  = np.array(lows,   dtype=float)
    c  = np.array(closes, dtype=float)
    pc = np.roll(c, 1); pc[0] = c[0]
    tr  = np.maximum(h - l, np.maximum(np.abs(h - pc), np.abs(l - pc)))
    out = np.zeros(len(tr))
    out[period - 1] = tr[:period].mean()
    for i in range(period, len(tr)):
        out[i] = (out[i-1] * (period - 1) + tr[i]) / period
    return out

def adx(highs: List[float], lows: List[float], closes: List[float],
        period: int = 14) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    h  = np.array(highs,  dtype=float)
    l  = np.array(lows,   dtype=float)
    c  = np.array(closes, dtype=float)
    n  = len(c)
    pc = np.roll(c, 1); pc[0] = c[0]
    ph = np.roll(h, 1); ph[0] = h[0]
    pl = np.roll(l, 1); pl[0] = l[0]
    tr    = np.maximum(h - l, np.maximum(np.abs(h - pc), np.abs(l - pc)))
    dmp   = np.where((h - ph) > (pl - l), np.maximum(h - ph, 0), 0)
    dmm   = np.where((pl - l) > (h - ph), np.maximum(pl - l, 0), 0)
    atr14 = np.zeros(n); dmp14 = np.zeros(n); dmm14 = np.zeros(n)
    atr14[period] = tr[1:period+1].sum()
    dmp14[period] = dmp[1:period+1].sum()
    dmm14[period] = dmm[1:period+1].sum()
    for i in range(period + 1, n):
        atr14[i] = atr14[i-1] - atr14[i-1] / period + tr[i]
        dmp14[i] = dmp14[i-1] - dmp14[i-1] / period + dmp[i]
        dmm14[i] = dmm14[i-1] - dmm14[i-1] / period + dmm[i]
    dip = np.where(atr14 == 0, 0, 100 * dmp14 / atr14)
    dim = np.where(atr14 == 0, 0, 100 * dmm14 / atr14)
    den = dip + dim
    dx  = np.where(den == 0, 0, 100 * np.abs(dip - dim) / den)
    adxv = np.zeros(n)
    s = 2 * period
    if s < n:
        adxv[s] = dx[period:s+1].mean()
        for i in range(s + 1, n):
            adxv[i] = (adxv[i-1] * (period - 1) + dx[i]) / period
    return adxv, dip, dim

def cvd(opens: List[float], closes: List[float], volumes: List[float],
        window: int = 20) -> np.ndarray:
    o = np.array(opens,   dtype=float)
    c = np.array(closes,  dtype=float)
    v = np.array(volumes, dtype=float)
    delta = np.where(c > o, v, np.where(c < o, -v, 0.0))
    n = len(delta); out = np.zeros(n)
    for i in range(n):
        s = max(0, i - window + 1)
        out[i] = delta[s:i+1].sum()
    return out

def slope(arr: np.ndarray, n: int = 5) -> float:
    y = arr[-n:]; x = np.arange(len(y), dtype=float)
    return float(np.polyfit(x, y, 1)[0]) if len(y) >= 2 else 0.0

def atr_percentile(atr_arr: np.ndarray, window: int = 50) -> float:
    hist = atr_arr[-window:]
    hist = hist[hist > 0]
    if len(hist) < 5:
        return 50.0
    return float(np.mean(hist <= atr_arr[-1]) * 100)


# ══════════════════════════════════════════════════════════════════════
#  MFI — Money Flow Index  (precio × volumen, más robusto que RSI)
# ══════════════════════════════════════════════════════════════════════

def mfi(highs: List[float], lows: List[float], closes: List[float],
        volumes: List[float], period: int = 14) -> np.ndarray:
    """
    MFI = 100 - 100/(1 + PMF/NMF)
    Más difícil de falsificar que RSI porque requiere volumen real.
    """
    h  = np.array(highs,   dtype=float)
    l  = np.array(lows,    dtype=float)
    c  = np.array(closes,  dtype=float)
    v  = np.array(volumes, dtype=float)
    tp = (h + l + c) / 3.0
    mf = tp * v
    n  = len(c)
    out = np.full(n, 50.0)
    for i in range(period, n):
        tp_w  = tp[i - period:i + 1]
        mf_w  = mf[i - period:i + 1]
        pos   = mf_w[np.diff(np.concatenate([[tp_w[0]], tp_w])) > 0].sum()
        neg   = mf_w[np.diff(np.concatenate([[tp_w[0]], tp_w])) < 0].sum()
        if neg == 0:
            out[i] = 100.0
        else:
            out[i] = 100.0 - 100.0 / (1.0 + pos / neg)
    return out


# ══════════════════════════════════════════════════════════════════════
#  TTM SQUEEZE MOMENTUM
# ══════════════════════════════════════════════════════════════════════

def squeeze_momentum(
    highs: List[float], lows: List[float], closes: List[float],
    bb_period: int = 20, bb_mult: float = 2.0,
    kc_period: int = 20, kc_mult: float = 1.5,
    mom_period: int = 12,
) -> Tuple[np.ndarray, np.ndarray]:
    h  = np.array(highs,  dtype=float)
    l  = np.array(lows,   dtype=float)
    c  = np.array(closes, dtype=float)
    n  = len(c)
    bb_mid = sma(closes, bb_period)
    bb_std = np.array([
        c[max(0, i - bb_period + 1):i + 1].std() if i >= bb_period - 1 else 0
        for i in range(n)
    ])
    bb_up  = bb_mid + bb_mult * bb_std
    bb_dn  = bb_mid - bb_mult * bb_std
    atr_kc = atr(highs, lows, closes, kc_period)
    kc_up  = bb_mid + kc_mult * atr_kc
    kc_dn  = bb_mid - kc_mult * atr_kc
    sqz    = (bb_up <= kc_up) & (bb_dn >= kc_dn)
    don_hi = np.array([h[max(0, i - mom_period):i + 1].max() for i in range(n)])
    don_lo = np.array([l[max(0, i - mom_period):i + 1].min() for i in range(n)])
    don_mid = (don_hi + don_lo) / 2
    delta   = c - (don_mid + bb_mid) / 2
    mom = np.zeros(n)
    for i in range(mom_period, n):
        y = delta[i - mom_period:i]
        x = np.arange(mom_period, dtype=float)
        mom[i] = float(np.polyfit(x, y, 1)[0]) * mom_period
    return sqz, mom


# ══════════════════════════════════════════════════════════════════════
#  RVOL · VWAP · CVD DIVERGENCIA
# ══════════════════════════════════════════════════════════════════════

def rvol(volumes: List[float], period: int = 20) -> np.ndarray:
    v   = np.array(volumes, dtype=float)
    avg = sma(volumes, period)
    return np.where(avg > 0, v / avg, 1.0)

def vwap_bands(
    highs: List[float], lows: List[float], closes: List[float],
    volumes: List[float], period: int = 60, band_mult: float = 1.5,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    h  = np.array(highs,   dtype=float)
    l  = np.array(lows,    dtype=float)
    c  = np.array(closes,  dtype=float)
    v  = np.array(volumes, dtype=float)
    tp = (h + l + c) / 3.0
    n  = len(c)
    vw = np.zeros(n); vw_up = np.zeros(n); vw_dn = np.zeros(n)
    for i in range(period - 1, n):
        s  = i - period + 1
        sv = v[s:i+1].sum()
        if sv == 0:
            vw[i] = tp[i]; vw_up[i] = tp[i]; vw_dn[i] = tp[i]; continue
        vw[i]    = (tp[s:i+1] * v[s:i+1]).sum() / sv
        dev      = np.sqrt(((tp[s:i+1] - vw[i])**2 * v[s:i+1]).sum() / sv)
        vw_up[i] = vw[i] + band_mult * dev
        vw_dn[i] = vw[i] - band_mult * dev
    return vw, vw_up, vw_dn

def cvd_divergence(closes: List[float], cvd_arr: np.ndarray,
                   lookback: int = 10) -> Tuple[bool, bool]:
    c  = np.array(closes, dtype=float)
    lb = min(lookback, len(c) - 1)
    price_slope = slope(c,       lb)
    cvd_slope   = slope(cvd_arr, lb)
    bearish_div = (price_slope > 0) and (cvd_slope < 0)
    bullish_div = (price_slope < 0) and (cvd_slope > 0)
    return bearish_div, bullish_div


# ══════════════════════════════════════════════════════════════════════
#  FVG — Fair Value Gaps
# ══════════════════════════════════════════════════════════════════════

def detect_fvg(
    highs: List[float], lows: List[float], closes: List[float],
    lookback: int = 30, min_size_pct: float = 0.003,
) -> Tuple[Optional[Dict], Optional[Dict]]:
    h = np.array(highs,  dtype=float)
    l = np.array(lows,   dtype=float)
    c = np.array(closes, dtype=float)
    n = len(c)
    bear_fvg = None
    bull_fvg = None
    for i in range(n - 2, max(n - lookback - 2, 2), -1):
        if bear_fvg is None:
            if h[i] < l[i-2]:
                size = (l[i-2] - h[i]) / h[i]
                if size >= min_size_pct:
                    mid = (l[i-2] + h[i]) / 2
                    if c[-1] < mid or c[-1] < l[i-2]:
                        bear_fvg = {"top": l[i-2], "bottom": h[i],
                                    "midpoint": mid, "age": n - 1 - i}
        if bull_fvg is None:
            if l[i] > h[i-2]:
                size = (l[i] - h[i-2]) / h[i-2]
                if size >= min_size_pct:
                    mid = (l[i] + h[i-2]) / 2
                    if c[-1] > mid or c[-1] > h[i-2]:
                        bull_fvg = {"top": l[i], "bottom": h[i-2],
                                    "midpoint": mid, "age": n - 1 - i}
        if bear_fvg and bull_fvg:
            break
    return bear_fvg, bull_fvg


# ══════════════════════════════════════════════════════════════════════
#  ORDER BLOCKS
# ══════════════════════════════════════════════════════════════════════

def detect_order_blocks(
    opens: List[float], highs: List[float], lows: List[float],
    closes: List[float], lookback: int = 40, impulse_bars: int = 3,
) -> Tuple[Optional[Dict], Optional[Dict]]:
    o = np.array(opens,  dtype=float)
    h = np.array(highs,  dtype=float)
    l = np.array(lows,   dtype=float)
    c = np.array(closes, dtype=float)
    n = len(c)
    bear_ob = None
    bull_ob = None
    for i in range(n - 1, max(n - lookback, impulse_bars + 2), -1):
        if bear_ob is None:
            reds = sum(1 for j in range(i, min(i + impulse_bars, n)) if c[j] < o[j])
            if reds >= impulse_bars:
                for k in range(i - 1, max(i - 6, 0), -1):
                    if c[k] > o[k]:
                        bear_ob = {"high": h[k], "low": l[k],
                                   "mid": (h[k] + l[k]) / 2, "age": n - 1 - k}
                        break
        if bull_ob is None:
            greens = sum(1 for j in range(i, min(i + impulse_bars, n)) if c[j] > o[j])
            if greens >= impulse_bars:
                for k in range(i - 1, max(i - 6, 0), -1):
                    if c[k] < o[k]:
                        bull_ob = {"high": h[k], "low": l[k],
                                   "mid": (h[k] + l[k]) / 2, "age": n - 1 - k}
                        break
        if bear_ob and bull_ob:
            break
    return bear_ob, bull_ob


# ══════════════════════════════════════════════════════════════════════
#  LIQUIDITY SWEEPS  (tolerancia dinámica basada en ATR)
# ══════════════════════════════════════════════════════════════════════

def detect_liquidity_sweep(
    highs: List[float], lows: List[float], closes: List[float],
    opens: List[float], lookback: int = 25,
    tolerance: float = 0.002,       # fallback fijo
    atr_value: float = 0.0,         # si > 0 usa tolerancia dinámica
    price: float = 0.0,
) -> Tuple[bool, bool]:
    """
    Tolerancia dinámica: 0.5 × ATR relativo al precio.
    Escala automáticamente con volatilidad del activo.
    """
    h = np.array(highs,  dtype=float)
    l = np.array(lows,   dtype=float)
    c = np.array(closes, dtype=float)

    # Tolerancia dinámica (mejor que fija)
    if atr_value > 0 and price > 0:
        tol = (atr_value / price) * 0.5
    else:
        tol = tolerance

    win = h[-lookback - 2:-3]
    wl  = l[-lookback - 2:-3]
    cur_h, cur_l, cur_c = h[-2], l[-2], c[-2]

    swept_highs = False
    swept_lows  = False

    if len(win) >= 2:
        for i in range(len(win)):
            for j in range(i + 1, len(win)):
                if abs(win[i] - win[j]) / max(win[i], 1e-9) < tol:
                    eq_level = (win[i] + win[j]) / 2
                    if cur_h > eq_level * (1 + tol) and cur_c < eq_level:
                        swept_highs = True
                        break
            if swept_highs:
                break

    if len(wl) >= 2:
        for i in range(len(wl)):
            for j in range(i + 1, len(wl)):
                if abs(wl[i] - wl[j]) / max(wl[i], 1e-9) < tol:
                    eq_level = (wl[i] + wl[j]) / 2
                    if cur_l < eq_level * (1 - tol) and cur_c > eq_level:
                        swept_lows = True
                        break
            if swept_lows:
                break

    return swept_highs, swept_lows


# ══════════════════════════════════════════════════════════════════════
#  MACD
# ══════════════════════════════════════════════════════════════════════

def macd(closes: List[float], fast: int = 12, slow: int = 26,
         signal: int = 9) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    e_fast = ema(closes, fast)
    e_slow = ema(closes, slow)
    ml     = e_fast - e_slow
    sl     = ema(ml.tolist(), signal)
    hist   = ml - sl
    return ml, sl, hist


# ══════════════════════════════════════════════════════════════════════
#  MARKET STRUCTURE — BOS / CHoCH
# ══════════════════════════════════════════════════════════════════════

def market_structure(
    highs: List[float], lows: List[float], closes: List[float],
    swing_len: int = 5,
) -> Dict[str, str]:
    h = np.array(highs,  dtype=float)
    l = np.array(lows,   dtype=float)
    c = np.array(closes, dtype=float)
    n = len(c)
    win = min(30, n - swing_len - 1)
    swing_highs = []
    swing_lows  = []
    for i in range(swing_len, win + swing_len):
        idx = n - 1 - i
        if idx < swing_len or idx >= n - swing_len:
            continue
        if h[idx] == h[idx - swing_len:idx + swing_len + 1].max():
            swing_highs.append((idx, h[idx]))
        if l[idx] == l[idx - swing_len:idx + swing_len + 1].min():
            swing_lows.append((idx, l[idx]))
    bos   = "NONE"
    choch = "NONE"
    cur   = c[-2]
    if len(swing_highs) >= 2:
        last_sh = swing_highs[0][1]
        prev_sh = swing_highs[1][1]
        if cur > last_sh and last_sh > prev_sh:
            bos = "BULL"
        elif cur > last_sh and last_sh < prev_sh:
            choch = "BULL"
    if len(swing_lows) >= 2:
        last_sl = swing_lows[0][1]
        prev_sl = swing_lows[1][1]
        if cur < last_sl and last_sl < prev_sl:
            bos = "BEAR"
        elif cur < last_sl and last_sl > prev_sl:
            choch = "BEAR"
    return {"bos": bos, "choch": choch}


# ══════════════════════════════════════════════════════════════════════
#  COMPRESIÓN PRE-BREAKOUT  ← nuevo, anticipación institucional
# ══════════════════════════════════════════════════════════════════════

def detect_compression(
    highs: List[float], lows: List[float], volumes: List[float],
    n: int = 8,
) -> Tuple[bool, float]:
    """
    Detecta acumulación silenciosa antes de un breakout.
    Condición: rango de velas cae ≥30% pero volumen se mantiene ≥80%.
    Devuelve (compresión_activa, ratio_compresión 0-1).
    """
    if len(highs) < n * 2:
        return False, 0.0
    h = np.array(highs,   dtype=float)
    l = np.array(lows,    dtype=float)
    v = np.array(volumes, dtype=float)
    recent_range = np.mean(h[-n:]   - l[-n:])
    prev_range   = np.mean(h[-n*2:-n] - l[-n*2:-n])
    recent_vol   = np.mean(v[-n:])
    prev_vol     = np.mean(v[-n*2:-n])
    if prev_range == 0 or prev_vol == 0:
        return False, 0.0
    range_ratio  = recent_range / prev_range   # <0.7 = compresión
    vol_ratio    = recent_vol   / prev_vol     # >0.8 = volumen aguanta
    compressing  = range_ratio < 0.70 and vol_ratio > 0.80
    # Score de compresión: cuanto más bajo el rango con más volumen, mejor
    compression_score = max(0.0, (1.0 - range_ratio) * vol_ratio)
    return compressing, round(compression_score, 3)


# ══════════════════════════════════════════════════════════════════════
#  FORTALEZA RELATIVA vs BTC  ← nuevo
# ══════════════════════════════════════════════════════════════════════

def relative_strength(
    closes: List[float],
    btc_closes: List[float],
    lookback: int = 4,
) -> float:
    """
    Retorno relativo del activo vs BTC en las últimas N velas.
    Positivo: activo más fuerte que BTC → favorece LONG
    Negativo: activo más débil que BTC → favorece SHORT
    """
    if len(closes) < lookback + 1 or len(btc_closes) < lookback + 1:
        return 0.0
    asset_ret = (closes[-1] - closes[-lookback]) / max(closes[-lookback], 1e-9)
    btc_ret   = (btc_closes[-1] - btc_closes[-lookback]) / max(btc_closes[-lookback], 1e-9)
    return round(asset_ret - btc_ret, 5)


# ══════════════════════════════════════════════════════════════════════
#  DETECCIÓN DE LIQUIDACIONES (proxy por wick + volumen)  ← nuevo
# ══════════════════════════════════════════════════════════════════════

def detect_liquidation_candle(
    opens: List[float], highs: List[float], lows: List[float],
    closes: List[float], volumes: List[float],
    atr_value: float, vol_mult: float = 3.0, wick_mult: float = 2.0,
) -> Tuple[bool, bool]:
    """
    Cascada de liquidaciones proxy:
    - Vela con wick ≥ wick_mult×ATR + volumen ≥ vol_mult×media
    liq_long:  wick inferior enorme → liquidación de longs → rebote posible
    liq_short: wick superior enorme → liquidación de shorts → caída posible
    """
    if len(closes) < 21 or atr_value <= 0:
        return False, False
    v      = np.array(volumes, dtype=float)
    vol_ma = float(np.mean(v[-20:]))
    cur_v  = v[-2]
    high_vol = cur_v >= vol_ma * vol_mult
    o = opens[-2]; h = highs[-2]; l = lows[-2]; c = closes[-2]
    body     = abs(c - o)
    wick_lo  = min(o, c) - l
    wick_hi  = h - max(o, c)
    liq_long  = high_vol and wick_lo  >= atr_value * wick_mult and wick_lo  > body * 2
    liq_short = high_vol and wick_hi  >= atr_value * wick_mult and wick_hi  > body * 2
    return liq_long, liq_short


# ══════════════════════════════════════════════════════════════════════
#  FUNDING PREDICTIVO — minutos al próximo pago  ← nuevo
# ══════════════════════════════════════════════════════════════════════

def minutes_to_next_funding(funding_hours: Tuple[int, ...] = (0, 8, 16)) -> int:
    """
    Devuelve los minutos que faltan para el próximo pago de funding.
    BingX paga cada 8h en 00:00 · 08:00 · 16:00 UTC.
    """
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    current_minutes = now.hour * 60 + now.minute
    for fh in funding_hours:
        target = fh * 60
        if target > current_minutes:
            return target - current_minutes
    # Siguiente día
    return (funding_hours[0] + 24) * 60 - current_minutes
