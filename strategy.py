"""
strategy.py — indicator calculations (pure Python, no numpy).
All functions return lists aligned to input candle length.
"""

from datetime import datetime, timezone


# ── EMA ───────────────────────────────────────────────────────

def _ema(prices: list, period: int) -> list:
    n = len(prices)
    if n < period:
        return [None] * n
    k = 2.0 / (period + 1)
    out = [None] * (period - 1)
    out.append(sum(prices[:period]) / period)
    for p in prices[period:]:
        out.append(out[-1] * (1 - k) + p * k)
    return out


# ── VWAP (daily reset at UTC midnight) ───────────────────────

def _vwap(highs, lows, closes, volumes, timestamps) -> list:
    out = []
    cum_tpv = cum_v = 0.0
    prev_day = None
    for h, l, c, v, ts in zip(highs, lows, closes, volumes, timestamps):
        day = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).date()
        if day != prev_day:
            cum_tpv = cum_v = 0.0
            prev_day = day
        tp = (h + l + c) / 3.0
        cum_tpv += tp * v
        cum_v   += v
        out.append(cum_tpv / cum_v if cum_v > 0 else c)
    return out


# ── ATR (Wilder) ──────────────────────────────────────────────

def _atr(highs, lows, closes, period: int) -> list:
    n = len(closes)
    if n < period + 1:
        return [None] * n
    trs = [None]
    for i in range(1, n):
        tr = max(highs[i] - lows[i],
                 abs(highs[i] - closes[i-1]),
                 abs(lows[i]  - closes[i-1]))
        trs.append(tr)
    out = [None] * period
    out.append(sum(trs[1:period+1]) / period)
    for i in range(period + 1, n):
        out.append((out[-1] * (period - 1) + trs[i]) / period)
    return out


# ── RSI (Wilder) ──────────────────────────────────────────────

def _rsi(closes, period: int = 14) -> list:
    n = len(closes)
    if n < period + 1:
        return [None] * n
    gains, losses = [], []
    for i in range(1, n):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    out = [None] * period
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    rs = ag / al if al > 0 else 1e9
    out.append(100 - 100 / (1 + rs))
    for i in range(period, len(gains)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
        rs = ag / al if al > 0 else 1e9
        out.append(100 - 100 / (1 + rs))
    return out


# ── MACD ─────────────────────────────────────────────────────

def _macd(closes, fast=12, slow=26, signal=9):
    ef = _ema(closes, fast)
    es = _ema(closes, slow)
    line = [
        (f - s if f is not None and s is not None else None)
        for f, s in zip(ef, es)
    ]
    valid = [x for x in line if x is not None]
    sig_raw = _ema(valid, signal) if len(valid) >= signal else [None] * len(valid)
    n_none = len(line) - len(valid)
    sig = [None] * n_none + sig_raw
    hist = [
        (l - s if l is not None and s is not None else None)
        for l, s in zip(line, sig)
    ]
    return line, sig, hist


# ── Bollinger Bands ────────────────────────────────────────────

def _bb(closes, period=20, mult=2.0):
    n = len(closes)
    uppers, mids, lowers = [], [], []
    for i in range(n):
        if i < period - 1:
            uppers.append(None); mids.append(None); lowers.append(None)
            continue
        w   = closes[i - period + 1 : i + 1]
        mid = sum(w) / period
        std = (sum((x - mid) ** 2 for x in w) / period) ** 0.5
        uppers.append(mid + mult * std)
        mids.append(mid)
        lowers.append(mid - mult * std)
    return uppers, mids, lowers


# ── Volume MA ─────────────────────────────────────────────────

def _vol_ma(volumes, period=20) -> list:
    n = len(volumes)
    out = [None] * (period - 1)
    for i in range(period - 1, n):
        out.append(sum(volumes[i - period + 1 : i + 1]) / period)
    return out


# ── ADX (Wilder) ─────────────────────────────────────────────

def _adx(highs, lows, closes, period=14) -> list:
    n = len(closes)
    result = [None] * n
    if n < 2 * period + 2:
        return result

    p_dms, m_dms, trs = [0.0], [0.0], [0.0]
    for i in range(1, n):
        up  = highs[i] - highs[i - 1]
        dn  = lows[i - 1] - lows[i]
        tr  = max(highs[i] - lows[i],
                  abs(highs[i] - closes[i - 1]),
                  abs(lows[i]  - closes[i - 1]))
        trs.append(tr)
        p_dms.append(up if up > dn and up > 0 else 0.0)
        m_dms.append(dn if dn > up and dn > 0 else 0.0)

    def _ws(arr):
        if len(arr) <= period:
            return [None] * len(arr)
        out = [None] * period
        out.append(sum(arr[1 : period + 1]))
        for i in range(period + 1, len(arr)):
            out.append(out[-1] - out[-1] / period + arr[i])
        return out

    s_tr  = _ws(trs)
    s_pd  = _ws(p_dms)
    s_md  = _ws(m_dms)

    dx = [None] * n
    for i in range(period, n):
        if s_tr[i] is None or s_tr[i] == 0:
            continue
        pdi = 100 * s_pd[i] / s_tr[i]
        mdi = 100 * s_md[i] / s_tr[i]
        tot = pdi + mdi
        dx[i] = 100 * abs(pdi - mdi) / tot if tot > 0 else 0.0

    valid_dx = [(i, v) for i, v in enumerate(dx) if v is not None]
    if len(valid_dx) < period:
        return result

    adx_val = sum(v for _, v in valid_dx[:period]) / period
    result[valid_dx[period - 1][0]] = adx_val
    for j in range(period, len(valid_dx)):
        adx_val = (adx_val * (period - 1) + valid_dx[j][1]) / period
        result[valid_dx[j][0]] = adx_val

    return result


# ── Master indicator builder ──────────────────────────────────

def get_indicators(candles: list, atr_period: int = None) -> dict:
    """
    candles: list of {timestamp, open, high, low, close, volume} oldest→newest.
    Returns dict of latest indicator values.
    """
    import config as _cfg
    atr_p = atr_period or _cfg.ATR_LEN

    if len(candles) < atr_p + 5:
        return {}

    ts  = [c["timestamp"] for c in candles]
    hi  = [c["high"]      for c in candles]
    lo  = [c["low"]       for c in candles]
    cl  = [c["close"]     for c in candles]
    vo  = [c["volume"]    for c in candles]

    ema9_s  = _ema(cl, 9)
    ema21_s = _ema(cl, 21)
    ema55_s = _ema(cl, 55)
    vwap_s  = _vwap(hi, lo, cl, vo, ts)
    atr_s   = _atr(hi, lo, cl, atr_p)
    rsi_s   = _rsi(cl, 14)
    macd_l, macd_sig, macd_h = _macd(cl, 12, 26, 9)
    bb_u, bb_m, bb_l = _bb(cl, 20, 2.0)
    vol_ma_s = _vol_ma(vo, 20)
    adx_s   = _adx(hi, lo, cl, 14)

    return {
        "close":    cl[-1],
        "high":     hi[-1],
        "low":      lo[-1],
        "volume":   vo[-1],
        "ema9":     ema9_s[-1],
        "ema21":    ema21_s[-1],
        "ema55":    ema55_s[-1],
        "vwap":     vwap_s[-1],
        "atr":      atr_s[-1],
        "rsi":      rsi_s[-1],
        "macd":     macd_l[-1],
        "macd_sig": macd_sig[-1],
        "macd_hist":macd_h[-1],
        "bb_upper": bb_u[-1],
        "bb_mid":   bb_m[-1],
        "bb_lower": bb_l[-1],
        "vol_ma":   vol_ma_s[-1],
        "adx":      adx_s[-1],
        # IBS for the last bar
        "ibs": (cl[-1] - lo[-1]) / (hi[-1] - lo[-1])
               if (hi[-1] - lo[-1]) > 0 else 0.5,
    }
