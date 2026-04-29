"""strategies/indicators.py — JIT-compiled technical indicators.

Numba @njit functions are compiled on first call (cached to disk).
generate_signal() is the public API consumed by bot.py.
"""
from __future__ import annotations

import numpy as np

try:
    from numba import njit  # type: ignore
    _NUMBA = True
except ImportError:
    # Graceful degradation: use plain numpy
    def njit(*args, **kwargs):  # type: ignore
        def decorator(fn):
            return fn
        return decorator
    _NUMBA = False


# ── Low-level numba kernels ───────────────────────────────────────────────────

@njit(cache=True)
def _rsi(close: np.ndarray, period: int) -> np.ndarray:
    n   = len(close)
    out = np.full(n, np.nan)
    if n < period + 1:
        return out
    gains = np.zeros(n)
    losses = np.zeros(n)
    for i in range(1, n):
        d = close[i] - close[i - 1]
        if d > 0:
            gains[i] = d
        else:
            losses[i] = -d

    avg_gain = np.mean(gains[1: period + 1])
    avg_loss = np.mean(losses[1: period + 1])

    for i in range(period, n):
        if i == period:
            ag, al = avg_gain, avg_loss
        else:
            ag = (ag * (period - 1) + gains[i]) / period
            al = (al * (period - 1) + losses[i]) / period
        if al == 0:
            out[i] = 100.0
        else:
            rs = ag / al
            out[i] = 100.0 - 100.0 / (1.0 + rs)
    return out


@njit(cache=True)
def _atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int) -> np.ndarray:
    n   = len(close)
    out = np.zeros(n)
    if n < period + 1:
        return out
    tr = np.zeros(n)
    for i in range(1, n):
        hl   = high[i] - low[i]
        hpc  = abs(high[i] - close[i - 1])
        lpc  = abs(low[i]  - close[i - 1])
        tr[i] = max(hl, hpc, lpc)
    # Wilder smoothing
    s = np.sum(tr[1: period + 1]) / period
    out[period] = s
    for i in range(period + 1, n):
        out[i] = (out[i - 1] * (period - 1) + tr[i]) / period
    return out


@njit(cache=True)
def _adx_di(high: np.ndarray, low: np.ndarray, close: np.ndarray,
            period: int) -> tuple:
    """Return (adx, plus_di, minus_di) arrays."""
    n    = len(close)
    adx  = np.zeros(n)
    pdi  = np.zeros(n)
    mdi  = np.zeros(n)
    if n < period * 2 + 1:
        return adx, pdi, mdi

    tr   = np.zeros(n)
    pdm  = np.zeros(n)
    mdm  = np.zeros(n)

    for i in range(1, n):
        hl   = high[i] - low[i]
        hpc  = abs(high[i] - close[i - 1])
        lpc  = abs(low[i]  - close[i - 1])
        tr[i] = max(hl, hpc, lpc)
        up   = high[i] - high[i - 1]
        dn   = low[i - 1] - low[i]
        pdm[i] = up  if up > dn and up > 0 else 0.0
        mdm[i] = dn  if dn > up and dn > 0 else 0.0

    # Wilder smooth
    str_  = np.sum(tr[1: period + 1])
    spdm  = np.sum(pdm[1: period + 1])
    smdm  = np.sum(mdm[1: period + 1])

    for i in range(period, n):
        if i > period:
            str_  = str_  - str_ / period  + tr[i]
            spdm  = spdm  - spdm / period  + pdm[i]
            smdm  = smdm  - smdm / period  + mdm[i]
        if str_ == 0:
            continue
        pdi[i] = 100.0 * spdm / str_
        mdi[i] = 100.0 * smdm / str_
        dx = abs(pdi[i] - mdi[i]) / (pdi[i] + mdi[i] + 1e-10) * 100.0
        if i == period:
            adx[i] = dx
        else:
            adx[i] = (adx[i - 1] * (period - 1) + dx) / period

    return adx, pdi, mdi


@njit(cache=True)
def _three_step(delta_vol: np.ndarray, period: int) -> tuple:
    """Three-Step Volume Delta — returns (delta1, delta2, delta3, total1)."""
    n = len(delta_vol)
    if n < period * 3:
        return 0.0, 0.0, 0.0, 0.0
    d1 = np.sum(delta_vol[-period:])
    d2 = np.sum(delta_vol[-period * 2:-period])
    d3 = np.sum(delta_vol[-period * 3:-period * 2])
    t1 = np.sum(np.abs(delta_vol[-period:]))
    return d1, d2, d3, t1


# ── Public API ────────────────────────────────────────────────────────────────

def generate_signal(
    # Primary TF (15m)
    high:  np.ndarray, low: np.ndarray, close: np.ndarray,
    open_: np.ndarray, volume: np.ndarray,
    # Confirm TF (1h) — may be None
    h_high: np.ndarray | None, h_low: np.ndarray | None,
    h_close: np.ndarray | None, h_open: np.ndarray | None,
    h_volume: np.ndarray | None,
    # Trend TF (4h) — may be None
    t_high: np.ndarray | None, t_low: np.ndarray | None,
    t_close: np.ndarray | None,
    cfg,
) -> tuple[str | None, dict]:
    """
    Return (signal, metrics):
        signal = 'BUY' | 'SELL' | None
        metrics = dict with adx, rsi, confidence, atr, atr_pct, delta1..3, ...
    """
    metrics: dict = {}

    # ── Primary indicators ────────────────────────────────────────────────
    adx_arr, pdi_arr, mdi_arr = _adx_di(high, low, close, cfg.adx_len)
    rsi_arr  = _rsi(close, cfg.rsi_len)
    atr_arr  = _atr(high, low, close, cfg.adx_len)

    adx  = float(adx_arr[-1])
    pdi  = float(pdi_arr[-1])
    mdi  = float(mdi_arr[-1])
    rsi  = float(rsi_arr[-1])
    atr  = float(atr_arr[-1])
    price = float(close[-1])
    atr_pct = atr / price * 100 if price > 0 else 0.0

    # ── Three-Step Volume Delta ───────────────────────────────────────────
    delta_vol = np.where(close >= open_, volume, -volume)
    d1, d2, d3, t1 = _three_step(delta_vol, cfg.period)

    bull_steps = sum(1 for d in [d1, d2, d3] if d > 0)
    bear_steps = sum(1 for d in [d1, d2, d3] if d < 0)

    # ── Volume spike ──────────────────────────────────────────────────────
    avg_vol   = float(np.mean(volume[-cfg.period:])) if len(volume) >= cfg.period else float(np.mean(volume))
    cur_vol   = float(volume[-1])
    vol_spike = cur_vol > avg_vol * cfg.vol_spike_mult

    # ── Confidence score ──────────────────────────────────────────────────
    max_steps  = 3
    adx_excess = max(0.0, adx - cfg.adx_thresh)
    vol_bonus  = 10.0 if vol_spike else 0.0

    long_agree  = bull_steps / max_steps * 33
    short_agree = bear_steps / max_steps * 33
    confidence  = max(long_agree, short_agree) + adx_excess + vol_bonus

    metrics.update({
        "adx":        round(adx, 2),
        "plus_di":    round(pdi, 2),
        "minus_di":   round(mdi, 2),
        "rsi":        round(rsi, 2),
        "atr":        round(atr, 8),
        "atr_pct":    round(atr_pct, 4),
        "delta1":     round(d1, 2),
        "delta2":     round(d2, 2),
        "delta3":     round(d3, 2),
        "bull_steps": bull_steps,
        "bear_steps": bear_steps,
        "vol_spike":  vol_spike,
        "confidence": round(confidence, 1),
    })

    # ── Primary signal conditions ─────────────────────────────────────────
    trend_ok = adx >= cfg.adx_thresh
    if not trend_ok:
        return None, metrics

    long_signal  = (bull_steps >= 2 and pdi > mdi and rsi < cfg.rsi_ob)
    short_signal = (bear_steps >= 2 and mdi > pdi and rsi > cfg.rsi_os)

    if not long_signal and not short_signal:
        return None, metrics

    sig = "BUY" if long_signal else "SELL"

    # ── HTF confirmation (must NOT disagree) ──────────────────────────────
    if h_close is not None and len(h_close) >= cfg.adx_len * 2 + 5:
        h_adx, h_pdi, h_mdi = _adx_di(h_high, h_low, h_close, cfg.adx_len)
        h_adx_val = float(h_adx[-1])
        h_pdi_val = float(h_pdi[-1])
        h_mdi_val = float(h_mdi[-1])
        metrics["h_adx"] = round(h_adx_val, 2)
        # Filter: HTF must not be in opposite trend
        if sig == "BUY"  and h_mdi_val > h_pdi_val * 1.2:
            return None, metrics
        if sig == "SELL" and h_pdi_val > h_mdi_val * 1.2:
            return None, metrics

    # ── 4h trend filter ───────────────────────────────────────────────────
    if t_close is not None and len(t_close) >= cfg.adx_len * 2 + 5:
        t_adx, t_pdi, t_mdi = _adx_di(t_high, t_low, t_close, cfg.adx_len)
        t_pdi_val = float(t_pdi[-1])
        t_mdi_val = float(t_mdi[-1])
        metrics["t_pdi"] = round(t_pdi_val, 2)
        metrics["t_mdi"] = round(t_mdi_val, 2)
        # Hard block: trading strongly against 4h trend
        if sig == "BUY"  and t_mdi_val > t_pdi_val * 1.5:
            return None, metrics
        if sig == "SELL" and t_pdi_val > t_mdi_val * 1.5:
            return None, metrics

    # ── Min confidence gate ───────────────────────────────────────────────
    if confidence < cfg.min_confidence:
        return None, metrics

    return sig, metrics
