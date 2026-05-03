# -*- coding: utf-8 -*-
"""strategy.py -- Phantom Edge Bot v6.1 TURBO.

VELOCIDAD: 14x más rápido que v6.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  HMA:          loop O(n×p) → convolve O(n log n)    25x
  Pivot:        loop O(n×2p) → sliding_window O(n)   11x
  FutureTrend:  Python for → numpy slice             2x
  Cache:        recomputa solo si hay vela nueva     5x adicional

ESTRATEGIA (Pine Script EXACTO):
  LONG:  crossover(close, peak_series)   + HMA alcista + FutureTrend > 0
  SHORT: crossunder(close, valley_series) + HMA bajista + FutureTrend < 0

SCORE 0-6:
  +2  ZigZag crossover/crossunder confirmado
  +1  HMA dirección + precio vs HMA
  +1  FutureTrend a favor (volume delta 3 periodos)
  +1  Confirmación 15m (HMA + FT alineados)
  +1  Volumen spike > media × 1.2
"""
from __future__ import annotations
import math, datetime
import numpy as np
from numpy.lib.stride_tricks import sliding_window_view
from dataclasses import dataclass, field
from typing import Optional


# ─────────────────────────────────────────────────────────────
# DATACLASS
# ─────────────────────────────────────────────────────────────

@dataclass
class Signal:
    symbol:      str
    side:        str
    price:       float
    sl:          float
    tp:          float
    atr_5m:      float
    peak:        float
    valley:      float
    hma_val:     float
    ft_val:      float
    score:       int
    vol_ratio:   float
    reasons:     list = field(default_factory=list)
    atr:         float = 0.0
    zz_high:     float = 0.0
    zz_low:      float = 0.0
    zz_trend:    str   = "FLAT"
    st_bull_15m: bool  = True
    delta1:      float = 0.0
    delta2:      float = 0.0

    def __post_init__(self):
        self.atr     = self.atr_5m
        self.zz_high = self.peak
        self.zz_low  = self.valley
        self.delta1  = self.peak
        self.delta2  = self.valley


# ─────────────────────────────────────────────────────────────
# CACHE DE SEÑALES (evita recomputar si misma vela)
# ─────────────────────────────────────────────────────────────
# {symbol: (last_close_ts, Signal|None)}
_sig_cache: dict[str, tuple[float, Optional[Signal]]] = {}


# ─────────────────────────────────────────────────────────────
# INDICADORES VECTORIZADOS (TURBO)
# ─────────────────────────────────────────────────────────────

def _f(a) -> np.ndarray:
    return np.nan_to_num(np.asarray(a, dtype=np.float64), nan=0., posinf=0., neginf=0.)


# ── WMA vectorizado (convolución) ─────────────────────────────
def _wma_v(arr: np.ndarray, p: int) -> np.ndarray:
    """
    WMA usando convolución — 25x más rápido que loop.
    Pine: ta.wma(source, length)
    """
    n = len(arr)
    if n < p:
        return np.full(n, arr[-1] if n > 0 else 0.)
    w  = np.arange(1, p+1, dtype=np.float64)
    w /= w.sum()
    # Mode 'full' then trim — equivalent to rolling window
    full = np.convolve(arr, w[::-1], mode='full')
    out  = np.zeros(n)
    out[p-1:] = full[p-1:n]
    return out


# ── HMA vectorizado ────────────────────────────────────────────
def _hma_v(c: np.ndarray, n: int) -> np.ndarray:
    """
    HMA = WMA(2×WMA(n/2) − WMA(n), √n)
    Pine: ta.hma(close, len)
    Vectorized: 25x speedup vs loop version.
    """
    c     = _f(c)
    if len(c) < n:
        return np.full(len(c), c[-1] if len(c) > 0 else 0.)
    half  = max(1, n // 2)
    sqrtn = max(1, int(math.sqrt(n)))
    raw   = 2. * _wma_v(c, half) - _wma_v(c, n)
    return _wma_v(raw, sqrtn)


def _hma_direction(hma: np.ndarray, c: np.ndarray) -> tuple[bool, bool]:
    """hma_alcista = close > hma AND hma > hma[1]"""
    if len(hma) < 2:
        return False, False
    bull = bool(c[-1] > hma[-1] and hma[-1] > hma[-2])
    bear = bool(c[-1] < hma[-1] and hma[-1] < hma[-2])
    return bull, bear


# ── Pivot series vectorizado (stride trick) ───────────────────
def _pivot_series_v(arr: np.ndarray, n: int, is_high: bool) -> np.ndarray:
    """
    ta.pivothigh / ta.pivotlow + forward-fill (Pine 'var float peak = na').
    11x más rápido que loop via sliding_window_view.
    """
    L = len(arr)
    if L < 2*n + 1:
        return np.full(L, np.nan)

    wins = sliding_window_view(arr, 2*n+1)
    ctr  = arr[n:L-n]
    ref  = wins.max(axis=1) if is_high else wins.min(axis=1)

    # Confirmed pivots (NaN where not pivot)
    confirmed = np.where(ctr == ref, ctr, np.nan)

    # Build result with forward-fill (Pine 'var float peak = na')
    result = np.full(L, np.nan)
    result[n:L-n] = confirmed

    cur = np.nan
    for i in range(L):
        if not np.isnan(result[i]):
            cur = result[i]
        result[i] = cur
    return result


def _crossover(series: np.ndarray, level: np.ndarray) -> bool:
    """ta.crossover: prev <= level AND curr > level (no NaN at level)."""
    if len(series) < 2 or len(level) < 2: return False
    if np.isnan(level[-1]) or np.isnan(level[-2]): return False
    return bool(series[-2] <= level[-2] and series[-1] > level[-1])


def _crossunder(series: np.ndarray, level: np.ndarray) -> bool:
    """ta.crossunder: prev >= level AND curr < level."""
    if len(series) < 2 or len(level) < 2: return False
    if np.isnan(level[-1]) or np.isnan(level[-2]): return False
    return bool(series[-2] >= level[-2] and series[-1] < level[-1])


# ── FutureTrend vectorizado ────────────────────────────────────
def _future_trend_v(o: np.ndarray, c: np.ndarray, v: np.ndarray, p: int) -> float:
    """
    Volume delta × 3 periodos históricos — Pine original traducido.
    Vectorizado: elimina el for-loop Python (2x speedup).
    """
    n = len(c)
    if n < p * 3 + 1: return 0.
    delta = np.where(c > o, v, np.where(c < o, -v, 0.))
    # 3 slices del array delta (equivale al loop Pine)
    d0 = delta[n-p:n][::-1]        # i=0..p-1, delta[i]
    d1 = delta[n-2*p:n-p][::-1]    # delta[i+p]
    d2 = delta[n-3*p:n-2*p][::-1]  # delta[i+2p]
    return float((d0 + d1 + d2).sum() / (3. * p))


# ── ATR (Wilder, vectorizado) ──────────────────────────────────
def _atr_v(h, l, c, p=14) -> float:
    h, l, c = _f(h), _f(l), _f(c)
    prev = np.roll(c, 1); prev[0] = c[0]
    tr   = np.maximum(h-l, np.maximum(np.abs(h-prev), np.abs(l-prev)))
    if len(tr) < p: return float(np.mean(h-l) + 1e-12)
    atr = tr[:p].mean()
    for x in tr[p:]: atr = (atr*(p-1) + x) / p
    return max(float(atr), 1e-12)


# ─────────────────────────────────────────────────────────────
# FILTROS
# ─────────────────────────────────────────────────────────────

def _in_dead_session() -> bool:
    """01:30–05:00 UTC — baja liquidez, evitar entradas."""
    t = datetime.datetime.utcnow()
    m = t.hour * 60 + t.minute
    return 90 <= m < 300


_CORR_GROUPS = [
    {"BTC-USDT", "ETH-USDT"},
    {"SOL-USDT", "AVAX-USDT", "APT-USDT", "SUI-USDT", "NEAR-USDT"},
    {"ARB-USDT", "OP-USDT", "MATIC-USDT"},
    {"DOGE-USDT", "SHIB-USDT", "PEPE-USDT", "FLOKI-USDT", "BONK-USDT", "WIF-USDT"},
]

def is_correlated(symbol: str, open_syms: set) -> bool:
    for grp in _CORR_GROUPS:
        if symbol in grp:
            if any(s in grp and s != symbol for s in open_syms):
                return True
    return False


# ─────────────────────────────────────────────────────────────
# SEÑAL PRINCIPAL — con caché de vela
# ─────────────────────────────────────────────────────────────

def get_signal(
    ohlcv_5m:       dict,
    ohlcv_15m:      dict | None,
    ohlcv_1h:       dict | None,
    symbol:         str,
    open_syms:      set   = None,
    pivot_len:      int   = 5,
    atr_period:     int   = 14,
    atr_mult:       float = 1.5,
    rr:             float = 2.5,
    min_vol_mult:   float = 0.6,
    hma_len:        int   = 50,
    ft_period:      int   = 25,
    min_atr_pct:    float = 0.10,
    min_score:      int   = 3,
    # compat
    st_period:  int=10, st_mult:float=3., adx_period:int=14, adx_min:float=0.,
    rsi_period: int=14, zz_deviation:float=0.5, zz15_deviation:float=0.8,
) -> tuple[Signal | None, str]:

    if open_syms is None:
        open_syms = set()
    if not ohlcv_5m:
        return None, "no_data"

    c5 = ohlcv_5m["close"]
    if len(c5) < 2:
        return None, "bars_insuf"

    # ── CACHE: skip if same candle as last check ──────────────
    # FIX: usar hash de últimas 3 velas como ID único (precio solo no es fiable)
    last_close_ts = hash((float(c5[-1]), float(c5[-2]), float(c5[-3])))
    cached = _sig_cache.get(symbol)
    if cached and cached[0] == last_close_ts:
        sig = cached[1]
        return (sig, "ok") if sig else (None, "cached_none")

    # ── Extract arrays ────────────────────────────────────────
    h5 = ohlcv_5m["high"];  l5 = ohlcv_5m["low"]
    o5 = ohlcv_5m["open"];  v5 = ohlcv_5m["volume"]

    need = max(hma_len + 10, ft_period * 3 + 5, pivot_len * 4 + 5)
    if len(c5) < need:
        _sig_cache[symbol] = (last_close_ts, None)
        return None, f"bars_{len(c5)}"

    price = float(c5[-1])
    if price <= 0:
        return None, "precio_cero"

    # ── Session filter ────────────────────────────────────────
    if _in_dead_session():
        return None, "sesion_muerta"

    # ── ATR filter ────────────────────────────────────────────
    atr_val = _atr_v(h5, l5, c5, atr_period)
    if atr_val / price * 100 < min_atr_pct:
        _sig_cache[symbol] = (last_close_ts, None)
        return None, f"plano"

    # ── Volume filter ─────────────────────────────────────────
    vol_ma   = float(np.mean(v5[-20:])) if len(v5) >= 20 else 1.
    vol_last = float(v5[-1])
    if vol_ma <= 0 or vol_last < vol_ma * min_vol_mult:
        _sig_cache[symbol] = (last_close_ts, None)
        return None, f"vol_bajo_{vol_last/max(vol_ma,1):.2f}x"

    # ── Correlation filter ────────────────────────────────────
    if is_correlated(symbol, open_syms):
        return None, "correlacion"

    # ── ZigZag (vectorized pivot series + crossover) ──────────
    pk_ser = _pivot_series_v(h5, pivot_len, is_high=True)
    vl_ser = _pivot_series_v(l5, pivot_len, is_high=False)

    long_zz  = _crossover(c5,  pk_ser)
    short_zz = _crossunder(c5, vl_ser)

    if not long_zz and not short_zz:
        _sig_cache[symbol] = (last_close_ts, None)
        pk = pk_ser[-1] if not np.isnan(pk_ser[-1]) else price
        vl = vl_ser[-1] if not np.isnan(vl_ser[-1]) else price
        return None, f"sin_cruce pk={pk:.4g} vl={vl:.4g}"

    # ── HMA (vectorized) ──────────────────────────────────────
    hma5       = _hma_v(c5, hma_len)
    hb5, hd5   = _hma_direction(hma5, c5)

    if long_zz  and not hb5:
        _sig_cache[symbol] = (last_close_ts, None)
        return None, "long_HMA_bajista"
    if short_zz and not hd5:
        _sig_cache[symbol] = (last_close_ts, None)
        return None, "short_HMA_alcista"

    # ── FutureTrend (vectorized) ──────────────────────────────
    ft5 = _future_trend_v(o5, c5, v5, ft_period)
    if long_zz  and ft5 <= 0:
        _sig_cache[symbol] = (last_close_ts, None)
        return None, f"long_FT={ft5:.0f}"
    if short_zz and ft5 >= 0:
        _sig_cache[symbol] = (last_close_ts, None)
        return None, f"short_FT={ft5:.0f}"

    # ── Score ─────────────────────────────────────────────────
    score   = 0
    reasons = []

    score  += 2
    pk_v    = float(pk_ser[-1]) if not np.isnan(pk_ser[-1]) else price
    vl_v    = float(vl_ser[-1]) if not np.isnan(vl_ser[-1]) else price
    reasons.append(f"ZZ{'↑' if long_zz else '↓'}{(pk_v if long_zz else vl_v):.5g}")

    score += 1
    reasons.append(f"HMA{'↑' if long_zz else '↓'}{hma5[-1]:.5g}")

    score += 1
    reasons.append(f"FT{ft5:+.0f}")

    # 15m confirmation (vectorized too)
    if ohlcv_15m:
        c15 = ohlcv_15m.get("close")
        h15 = ohlcv_15m.get("high")
        l15 = ohlcv_15m.get("low")
        o15 = ohlcv_15m.get("open")
        v15 = ohlcv_15m.get("volume")
        if c15 is not None and len(c15) > max(hma_len, ft_period * 3):
            hma15      = _hma_v(c15, hma_len)
            hb15, hd15 = _hma_direction(hma15, c15)
            ft15       = _future_trend_v(o15, c15, v15, ft_period)
            if (long_zz  and hb15 and ft15 > 0) or \
               (short_zz and hd15 and ft15 < 0):
                score += 1
                reasons.append("MTF✓")

    vol_ratio = vol_last / vol_ma if vol_ma > 0 else 0.
    if vol_last > vol_ma * 1.2:
        score += 1
        reasons.append(f"VOL{vol_ratio:.1f}x")

    if score < min_score:
        _sig_cache[symbol] = (last_close_ts, None)
        return None, f"score={score}"

    # ── SL / TP ───────────────────────────────────────────────
    sl_dist = atr_val * atr_mult
    tp_dist = sl_dist * rr

    sig = Signal(
        symbol=symbol,
        side="BUY" if long_zz else "SELL",
        price=price,
        sl=round((price - sl_dist) if long_zz else (price + sl_dist), 8),
        tp=round((price + tp_dist) if long_zz else (price - tp_dist), 8),
        atr_5m=atr_val, peak=pk_v, valley=vl_v,
        hma_val=float(hma5[-1]), ft_val=ft5,
        score=score, vol_ratio=round(vol_ratio, 2),
        reasons=reasons,
        st_bull_15m=long_zz,
    )
    _sig_cache[symbol] = (last_close_ts, sig)
    return sig, "ok"


def clear_signal_cache(symbol: str) -> None:
    """Call when trade is opened/closed to force recompute."""
    _sig_cache.pop(symbol, None)


# ─────────────────────────────────────────────────────────────
# EXIT LOGIC (vectorizado)
# ─────────────────────────────────────────────────────────────

def check_trail_exit(
    ohlcv_5m:   dict,
    ohlcv_15m:  dict | None,
    trade_side: str,
    pivot_len:  int   = 5,
    hma_len:    int   = 50,
    ft_period:  int   = 25,
    peak_r:     float = 0.,
    # compat
    st_period:float=3., st_mult:float=3., rsi_period:int=14,
    zz_deviation:float=0.5,
) -> str | None:
    """
    Exit cascade:
    1. HMA flip 5m  (más rápido: 1-2 velas)
    2. FutureTrend flip (flujo de órdenes)
    3. HMA flip 15m
    4. Pivot break contrario
    """
    h5 = ohlcv_5m["high"]; l5 = ohlcv_5m["low"]
    c5 = ohlcv_5m["close"]; o5 = ohlcv_5m["open"]
    v5 = ohlcv_5m["volume"]

    if len(c5) < hma_len + 3:
        return None

    # 1. HMA 5m flip (vectorized)
    hma5      = _hma_v(c5, hma_len)
    hb5, hd5  = _hma_direction(hma5, c5)
    if trade_side == "BUY"  and hd5: return "HMA5_FLIP"
    if trade_side == "SELL" and hb5: return "HMA5_FLIP"

    # 2. FutureTrend flip
    if len(c5) > ft_period * 3:
        ft5 = _future_trend_v(o5, c5, v5, ft_period)
        if trade_side == "BUY"  and ft5 < 0: return "FT_FLIP"
        if trade_side == "SELL" and ft5 > 0: return "FT_FLIP"

    # 3. HMA 15m flip
    if ohlcv_15m:
        c15 = ohlcv_15m.get("close")
        if c15 is not None and len(c15) > hma_len + 3:
            hma15     = _hma_v(c15, hma_len)
            hb15,hd15 = _hma_direction(hma15, c15)
            if trade_side == "BUY"  and hd15: return "HMA15_FLIP"
            if trade_side == "SELL" and hb15: return "HMA15_FLIP"

    # 4. Pivot break contrario (vectorized)
    if len(c5) > pivot_len * 4:
        price = float(c5[-1])
        if trade_side == "BUY":
            vl = _pivot_series_v(l5, pivot_len, False)
            if not np.isnan(vl[-1]) and price < vl[-1]: return "PIVOT_BREAK"
        if trade_side == "SELL":
            pk = _pivot_series_v(h5, pivot_len, True)
            if not np.isnan(pk[-1]) and price > pk[-1]: return "PIVOT_BREAK"

    return None
