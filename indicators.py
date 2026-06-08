"""
GUA-USDT Bot v2 — Indicadores Técnicos Modernos
EMA · RSI · ATR · ADX · CVD · Squeeze · RVOL · VWAP · FVG · OB · LiqSweep
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
    out = np.empty(len(arr)); out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = arr[i] * k + out[i-1] * (1-k)
    return out

def sma(values: List[float], period: int) -> np.ndarray:
    arr = np.array(values, dtype=float)
    out = np.full(len(arr), np.nan)
    for i in range(period-1, len(arr)):
        out[i] = arr[i-period+1:i+1].mean()
    return out

def rsi(closes: List[float], period: int = 14) -> np.ndarray:
    arr = np.array(closes, dtype=float)
    d   = np.diff(arr)
    g   = np.where(d > 0, d, 0.0)
    l   = np.where(d < 0, -d, 0.0)
    n   = len(arr)
    ag  = np.zeros(n); al = np.zeros(n)
    ag[period] = g[:period].mean()
    al[period] = l[:period].mean()
    for i in range(period+1, n):
        ag[i] = (ag[i-1]*(period-1) + g[i-1]) / period
        al[i] = (al[i-1]*(period-1) + l[i-1]) / period
    rs  = np.where(al == 0, 100.0, ag/al)
    out = np.where(al == 0, 100.0, 100.0 - 100.0/(1.0+rs))
    out[:period] = np.nan
    return out

def atr(highs: List[float], lows: List[float], closes: List[float],
        period: int = 14) -> np.ndarray:
    h = np.array(highs, dtype=float)
    l = np.array(lows,  dtype=float)
    c = np.array(closes,dtype=float)
    pc = np.roll(c,1); pc[0] = c[0]
    tr  = np.maximum(h-l, np.maximum(np.abs(h-pc), np.abs(l-pc)))
    out = np.zeros(len(tr))
    out[period-1] = tr[:period].mean()
    for i in range(period, len(tr)):
        out[i] = (out[i-1]*(period-1) + tr[i]) / period
    return out

def adx(highs: List[float], lows: List[float], closes: List[float],
        period: int = 14) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    h  = np.array(highs, dtype=float)
    l  = np.array(lows,  dtype=float)
    c  = np.array(closes,dtype=float)
    n  = len(c)
    pc = np.roll(c,1); pc[0]=c[0]
    ph = np.roll(h,1); ph[0]=h[0]
    pl = np.roll(l,1); pl[0]=l[0]
    tr   = np.maximum(h-l, np.maximum(np.abs(h-pc), np.abs(l-pc)))
    dmp  = np.where((h-ph)>(pl-l), np.maximum(h-ph,0), 0)
    dmm  = np.where((pl-l)>(h-ph), np.maximum(pl-l,0), 0)
    atr14= np.zeros(n); dmp14=np.zeros(n); dmm14=np.zeros(n)
    atr14[period]=tr[1:period+1].sum()
    dmp14[period]=dmp[1:period+1].sum()
    dmm14[period]=dmm[1:period+1].sum()
    for i in range(period+1,n):
        atr14[i]=atr14[i-1]-atr14[i-1]/period+tr[i]
        dmp14[i]=dmp14[i-1]-dmp14[i-1]/period+dmp[i]
        dmm14[i]=dmm14[i-1]-dmm14[i-1]/period+dmm[i]
    dip  = np.where(atr14==0,0,100*dmp14/atr14)
    dim  = np.where(atr14==0,0,100*dmm14/atr14)
    den  = dip+dim
    dx   = np.where(den==0,0,100*np.abs(dip-dim)/den)
    adxv = np.zeros(n); s=2*period
    if s<n:
        adxv[s]=dx[period:s+1].mean()
        for i in range(s+1,n):
            adxv[i]=(adxv[i-1]*(period-1)+dx[i])/period
    return adxv, dip, dim

def cvd(opens: List[float], closes: List[float], volumes: List[float],
        window: int = 20) -> np.ndarray:
    o = np.array(opens, dtype=float)
    c = np.array(closes,dtype=float)
    v = np.array(volumes,dtype=float)
    delta = np.where(c>o, v, np.where(c<o,-v, 0.0))
    n = len(delta); out = np.zeros(n)
    for i in range(n):
        s=max(0,i-window+1); out[i]=delta[s:i+1].sum()
    return out

def slope(arr: np.ndarray, n: int = 5) -> float:
    y = arr[-n:]; x = np.arange(len(y), dtype=float)
    return float(np.polyfit(x,y,1)[0]) if len(y) >= 2 else 0.0

def atr_percentile(atr_arr: np.ndarray, window: int = 50) -> float:
    """Percentil del ATR actual vs su historia reciente (0-100)."""
    hist = atr_arr[-window:]
    hist = hist[hist > 0]
    if len(hist) < 5:
        return 50.0
    cur  = atr_arr[-1]
    return float(np.mean(hist <= cur) * 100)


# ══════════════════════════════════════════════════════════════════════
#  TTM SQUEEZE MOMENTUM
# ══════════════════════════════════════════════════════════════════════

def squeeze_momentum(
    highs: List[float], lows: List[float], closes: List[float],
    bb_period: int = 20, bb_mult: float = 2.0,
    kc_period: int = 20, kc_mult: float = 1.5,
    mom_period: int = 12,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    TTM Squeeze: detecta cuando Bollinger Bands están dentro de Keltner Channels.
    Devuelve (squeeze_bool_arr, momentum_arr).
    squeeze=True  → compresión activa (energía acumulando)
    squeeze=False → liberación (momentum saliendo)
    momentum > 0 y creciente → impulso alcista
    momentum < 0 y decreciente → impulso bajista
    """
    h  = np.array(highs, dtype=float)
    l  = np.array(lows,  dtype=float)
    c  = np.array(closes,dtype=float)
    n  = len(c)

    # Bollinger Bands
    bb_mid = sma(closes, bb_period)
    bb_std = np.array([
        c[max(0,i-bb_period+1):i+1].std() if i >= bb_period-1 else 0
        for i in range(n)
    ])
    bb_up  = bb_mid + bb_mult * bb_std
    bb_dn  = bb_mid - bb_mult * bb_std

    # Keltner Channels (sobre bb_mid)
    atr_kc = atr(highs, lows, closes, kc_period)
    kc_up  = bb_mid + kc_mult * atr_kc
    kc_dn  = bb_mid - kc_mult * atr_kc

    # Squeeze: BB inside KC
    sqz    = (bb_up <= kc_up) & (bb_dn >= kc_dn)

    # Momentum: delta vs midpoint(Donchian, SMA)
    don_hi = np.array([h[max(0,i-mom_period):i+1].max() for i in range(n)])
    don_lo = np.array([l[max(0,i-mom_period):i+1].min() for i in range(n)])
    don_mid= (don_hi + don_lo) / 2
    delta  = c - (don_mid + bb_mid) / 2

    # Regresión lineal del delta (suavizado)
    mom = np.zeros(n)
    for i in range(mom_period, n):
        y = delta[i-mom_period:i]
        x = np.arange(mom_period, dtype=float)
        mom[i] = float(np.polyfit(x, y, 1)[0]) * mom_period

    return sqz, mom


# ══════════════════════════════════════════════════════════════════════
#  RVOL — Relative Volume
# ══════════════════════════════════════════════════════════════════════

def rvol(volumes: List[float], period: int = 20) -> np.ndarray:
    """Volumen actual / media de las últimas N velas."""
    v   = np.array(volumes, dtype=float)
    avg = sma(volumes, period)
    return np.where(avg > 0, v / avg, 1.0)


# ══════════════════════════════════════════════════════════════════════
#  VWAP con bandas de desviación estándar
# ══════════════════════════════════════════════════════════════════════

def vwap_bands(
    highs: List[float], lows: List[float], closes: List[float],
    volumes: List[float], period: int = 60, band_mult: float = 1.5,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    VWAP rodante con bandas ±1.5σ.
    Devuelve (vwap, upper_band, lower_band).
    """
    h  = np.array(highs,  dtype=float)
    l  = np.array(lows,   dtype=float)
    c  = np.array(closes, dtype=float)
    v  = np.array(volumes,dtype=float)
    tp = (h + l + c) / 3.0
    n  = len(c)

    vw = np.zeros(n); vw_up = np.zeros(n); vw_dn = np.zeros(n)
    for i in range(period-1, n):
        s   = i - period + 1
        sv  = v[s:i+1].sum()
        if sv == 0:
            vw[i] = tp[i]; vw_up[i] = tp[i]; vw_dn[i] = tp[i]; continue
        vw[i]    = (tp[s:i+1] * v[s:i+1]).sum() / sv
        dev      = np.sqrt(((tp[s:i+1] - vw[i])**2 * v[s:i+1]).sum() / sv)
        vw_up[i] = vw[i] + band_mult * dev
        vw_dn[i] = vw[i] - band_mult * dev

    return vw, vw_up, vw_dn


# ══════════════════════════════════════════════════════════════════════
#  CVD DIVERGENCIA
# ══════════════════════════════════════════════════════════════════════

def cvd_divergence(closes: List[float], cvd_arr: np.ndarray,
                   lookback: int = 10) -> Tuple[bool, bool]:
    """
    Divergencia CVD vs precio.
    Bearish: precio hace HH pero CVD hace LH  → presión vendedora oculta
    Bullish: precio hace LL pero CVD hace HL  → presión compradora oculta
    """
    c  = np.array(closes, dtype=float)
    lb = min(lookback, len(c)-1)
    price_slope = slope(c,  lb)
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
    """
    Detecta el FVG más reciente alcista y bajista no rellenado.

    Bearish FVG: high[i] < low[i-2]  — hueco bajista entre i-2 y i
    Bullish FVG: low[i]  > high[i-2] — hueco alcista entre i-2 y i

    Devuelve (bearish_fvg, bullish_fvg) como dicts con keys:
      top, bottom, midpoint, age (velas desde formación)
    """
    h = np.array(highs, dtype=float)
    l = np.array(lows,  dtype=float)
    c = np.array(closes,dtype=float)
    n = len(c)

    bear_fvg = None
    bull_fvg = None

    for i in range(n-2, max(n-lookback-2, 2), -1):
        # Bearish FVG
        if bear_fvg is None:
            if h[i] < l[i-2]:
                size = (l[i-2] - h[i]) / h[i]
                if size >= min_size_pct:
                    mid = (l[i-2] + h[i]) / 2
                    # Solo válido si NO ha sido rellenado (precio actual no cruzó mid)
                    if c[-1] < mid or c[-1] < l[i-2]:
                        bear_fvg = {
                            "top":      l[i-2],
                            "bottom":   h[i],
                            "midpoint": mid,
                            "age":      n - 1 - i,
                        }

        # Bullish FVG
        if bull_fvg is None:
            if l[i] > h[i-2]:
                size = (l[i] - h[i-2]) / h[i-2]
                if size >= min_size_pct:
                    mid = (l[i] + h[i-2]) / 2
                    if c[-1] > mid or c[-1] > h[i-2]:
                        bull_fvg = {
                            "top":      l[i],
                            "bottom":   h[i-2],
                            "midpoint": mid,
                            "age":      n - 1 - i,
                        }

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
    """
    Bearish OB: última vela verde antes de impulso bajista (≥N rojas).
    Bullish OB: última vela roja antes de impulso alcista (≥N verdes).
    """
    o = np.array(opens, dtype=float)
    h = np.array(highs, dtype=float)
    l = np.array(lows,  dtype=float)
    c = np.array(closes,dtype=float)
    n = len(c)

    bear_ob = None
    bull_ob = None

    for i in range(n-1, max(n-lookback, impulse_bars+2), -1):
        # Verificar impulso bajista (≥ impulse_bars rojas seguidas)
        if bear_ob is None:
            reds = sum(1 for j in range(i, min(i+impulse_bars, n)) if c[j] < o[j])
            if reds >= impulse_bars:
                # Buscar la última verde antes del impulso
                for k in range(i-1, max(i-6, 0), -1):
                    if c[k] > o[k]:
                        bear_ob = {
                            "high": h[k], "low": l[k],
                            "mid":  (h[k]+l[k])/2,
                            "age":  n-1-k,
                        }
                        break

        # Verificar impulso alcista (≥ impulse_bars verdes)
        if bull_ob is None:
            greens = sum(1 for j in range(i, min(i+impulse_bars, n)) if c[j] > o[j])
            if greens >= impulse_bars:
                for k in range(i-1, max(i-6, 0), -1):
                    if c[k] < o[k]:
                        bull_ob = {
                            "high": h[k], "low": l[k],
                            "mid":  (h[k]+l[k])/2,
                            "age":  n-1-k,
                        }
                        break

        if bear_ob and bull_ob:
            break

    return bear_ob, bull_ob


# ══════════════════════════════════════════════════════════════════════
#  LIQUIDITY SWEEPS
# ══════════════════════════════════════════════════════════════════════

def detect_liquidity_sweep(
    highs: List[float], lows: List[float], closes: List[float],
    opens: List[float], lookback: int = 25, tolerance: float = 0.002,
) -> Tuple[bool, bool]:
    """
    Detecta barrido de liquidez en la última vela cerrada ([-2]).

    Swept highs (bajista): vela wick por encima de equal highs → cierre bajo ellos
    Swept lows  (alcista): vela wick por debajo de equal lows  → cierre sobre ellos

    Devuelve (swept_highs, swept_lows).
    """
    h = np.array(highs, dtype=float)
    l = np.array(lows,  dtype=float)
    c = np.array(closes,dtype=float)
    o = np.array(opens, dtype=float)

    win  = h[-lookback-2:-3]
    wl   = l[-lookback-2:-3]
    cur_h, cur_l, cur_c, cur_o = h[-2], l[-2], c[-2], o[-2]

    swept_highs = False
    swept_lows  = False

    # Equal highs: dos o más máximos dentro de tolerancia
    if len(win) >= 2:
        for i in range(len(win)):
            for j in range(i+1, len(win)):
                if abs(win[i]-win[j]) / max(win[i], 0.000001) < tolerance:
                    eq_level = (win[i]+win[j]) / 2
                    # Wick por encima, cierre bajo el nivel → barrido bajista
                    if cur_h > eq_level*(1+tolerance) and cur_c < eq_level:
                        swept_highs = True
                        break
            if swept_highs:
                break

    # Equal lows
    if len(wl) >= 2:
        for i in range(len(wl)):
            for j in range(i+1, len(wl)):
                if abs(wl[i]-wl[j]) / max(wl[i], 0.000001) < tolerance:
                    eq_level = (wl[i]+wl[j]) / 2
                    if cur_l < eq_level*(1-tolerance) and cur_c > eq_level:
                        swept_lows = True
                        break
            if swept_lows:
                break

    return swept_highs, swept_lows


# ══════════════════════════════════════════════════════════════════════
#  MACD (señal de momentum adicional)
# ══════════════════════════════════════════════════════════════════════

def macd(closes: List[float], fast: int = 12, slow: int = 26,
         signal: int = 9) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Devuelve (macd_line, signal_line, histogram)."""
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
    """
    Detecta Break of Structure (BOS) y Change of Character (CHoCH).
    Devuelve {"bos": "BULL"|"BEAR"|"NONE", "choch": "BULL"|"BEAR"|"NONE"}
    """
    h = np.array(highs, dtype=float)
    l = np.array(lows,  dtype=float)
    c = np.array(closes,dtype=float)
    n = len(c)

    # Encontrar swings en ventana reciente
    win = min(30, n-swing_len-1)

    swing_highs = []
    swing_lows  = []

    for i in range(swing_len, win+swing_len):
        idx = n - 1 - i
        if idx < swing_len or idx >= n-swing_len:
            continue
        if h[idx] == h[idx-swing_len:idx+swing_len+1].max():
            swing_highs.append((idx, h[idx]))
        if l[idx] == l[idx-swing_len:idx+swing_len+1].min():
            swing_lows.append((idx, l[idx]))

    bos   = "NONE"
    choch = "NONE"

    cur = c[-2]

    if len(swing_highs) >= 2:
        last_sh  = swing_highs[0][1]
        prev_sh  = swing_highs[1][1] if len(swing_highs)>1 else last_sh
        if cur > last_sh and last_sh > prev_sh:
            bos = "BULL"          # precio rompe HH → BOS alcista
        elif cur > last_sh and last_sh < prev_sh:
            choch = "BULL"        # rompe LH → CHoCH alcista

    if len(swing_lows) >= 2:
        last_sl  = swing_lows[0][1]
        prev_sl  = swing_lows[1][1] if len(swing_lows)>1 else last_sl
        if cur < last_sl and last_sl < prev_sl:
            bos = "BEAR"
        elif cur < last_sl and last_sl > prev_sl:
            choch = "BEAR"

    return {"bos": bos, "choch": choch}
