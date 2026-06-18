"""
QF×JP Bot v6.5.2 — Indicators MEJORADO
Mejoras de anticipación (basadas en Pine Script v3.6):

  [ANT1] detect_structure: ventana dinámica + micro-CHoCH temprano
  [ANT2] detect_tl_break: pivotes reales + buffer ATR (antes era slope lineal)
  [ANT3] calc_cvd: ventana rolling adaptativa 20-60 barras según volatilidad
  [ANT4] detect_fvg: busca FVG reciente Y precio dentro del gap (entry timing)
  [ANT5] detect_sweep: EQL/EQH sweeps — señal de reversal anticipada
  [ANT6] calc_squeeze: squeeze momentum (BB dentro KC = compresión → explosión)
  [ANT7] pre_signal_score: puntuación 0-100 de pre-señal antes del umbral
  [ANT8] composite_score: OBI + sweep + squeeze + pre_signal incorporados
  [ANT9] calc_vwap_deviation: precio vs VWAP bandas ±1σ para timing de salida
  [ANT10] calc_rsi_multi: consenso RSI 7/14/21 para filtro adicional
"""
import logging
import warnings
from dataclasses import dataclass, field

import numpy as np

warnings.filterwarnings("ignore", category=RuntimeWarning)

log = logging.getLogger("indicators")

# ── Signal dataclass ──────────────────────────────────────────────────────────

@dataclass
class Signal:
    symbol:           str
    direction:        str    # LONG | SHORT | NONE
    score:            float
    tier:             str    # STD | FUEL | SUP | NONE
    entry:            float
    sl:               float
    tp1:              float
    tp2:              float
    atr:              float
    adx:              float
    mfi:              float
    vdi:              float
    cvd:              float
    momentum:         float
    htf_score:        float
    structure:        str
    tl_break:         str
    tl_break_active:  bool  = False
    circuit_breaker:  bool  = False
    funding_rate:     float = 0.0
    reason:           str   = ""
    # [ANT] Nuevos campos de anticipación
    pre_score:        float = 0.0   # pre-señal score 0-100
    sweep:            str   = "NONE"  # EQL | EQH | NONE
    squeeze:          bool  = False   # squeeze comprimido
    squeeze_fire:     bool  = False   # squeeze liberado (entry timing)
    vwap_dev:         float = 0.0    # desviación VWAP en ATRs
    rsi_consensus:    int   = 0      # +1 bull / -1 bear / 0 neutral
    fvg_active:       str   = "NONE" # BULL | BEAR | NONE (precio dentro del gap)

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
    pdi = 100 * _rma(plus_dm,  period) / safe_atr
    mdi = 100 * _rma(minus_dm, period) / safe_atr
    denom = pdi + mdi
    dx    = 100 * np.where(denom > 0, np.abs(pdi - mdi) / denom, 0.0)
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

# ── CVD [ANT3] — ventana adaptativa según volatilidad ────────────────────────

def calc_cvd(open_, close, volume, atr_arr=None) -> np.ndarray:
    """
    [ANT3] Rolling window adaptativa:
    - Volatilidad alta (ATR > media) → window corta (20) = más reactivo
    - Volatilidad baja               → window normal (40)
    """
    o, c, v = (np.asarray(x, float) for x in (open_, close, volume))
    hl_range = np.abs(c - o)
    bvol     = np.where(c > o, v, 0.0)
    svol     = np.where(c <= o, v, 0.0)
    delta    = bvol - svol
    total    = bvol + svol
    cvd_raw  = np.divide(delta, total, out=np.zeros_like(delta), where=total > 0)

    # Ventana adaptativa
    if atr_arr is not None and len(atr_arr) >= 20:
        atr_mean = np.mean(atr_arr[-20:])
        window   = 20 if atr_arr[-1] > atr_mean * 1.1 else 40
    else:
        window = 30

    return _ema(cvd_raw, window)

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
    c, v   = np.asarray(close, float), np.asarray(volume, float)
    vwap_d = (c - _sma(c, period)) * v
    std    = np.nanstd(vwap_d[-period:])
    result = np.divide(vwap_d, std + 1e-9, out=np.zeros_like(vwap_d), where=(std + 1e-9) > 0)
    return np.nan_to_num(result, nan=0.0, posinf=0.0, neginf=0.0)

# ── [ANT9] VWAP Deviation ─────────────────────────────────────────────────────

def calc_vwap_deviation(high, low, close, volume, atr: float) -> float:
    """
    Retorna desviación del precio vs VWAP en unidades de ATR.
    Positivo = sobre VWAP, negativo = bajo VWAP.
    > +1.0 ATR = zona de sobrecompra (SHORT timing)
    < -1.0 ATR = zona de sobreventa  (LONG timing)
    """
    h, l, c, v = (np.asarray(x, float) for x in (high, low, close, volume))
    hlc3    = (h + l + c) / 3
    cum_vol = np.cumsum(v)
    cum_vp  = np.cumsum(hlc3 * v)
    vwap    = np.divide(cum_vp, cum_vol, out=np.full_like(cum_vp, hlc3[0]), where=cum_vol > 0)
    dev     = (c[-1] - vwap[-1]) / (atr + 1e-9)
    return _safe(dev, 0.0)

# ── [ANT10] RSI Multi-período ─────────────────────────────────────────────────

def calc_rsi_multi(close) -> int:
    """
    Consenso RSI 7/14/21.
    Retorna: +1 (todos > 50 = bull), -1 (todos < 50 = bear), 0 (mixto)
    """
    c = np.asarray(close, float)
    if len(c) < 25:
        return 0

    def _rsi(arr, period):
        delta = np.diff(arr)
        gain  = np.where(delta > 0, delta, 0.0)
        loss  = np.where(delta < 0, -delta, 0.0)
        avg_g = _rma(gain, period)
        avg_l = _rma(loss, period)
        rs    = np.divide(avg_g, avg_l + 1e-9, out=np.ones_like(avg_g), where=avg_l > 0)
        return 100 - 100 / (1 + rs)

    r7  = _safe(_rsi(c, 7)[-1],  50.0)
    r14 = _safe(_rsi(c, 14)[-1], 50.0)
    r21 = _safe(_rsi(c, 21)[-1], 50.0)

    if r7 > 50 and r14 > 50 and r21 > 50:
        return +1
    if r7 < 50 and r14 < 50 and r21 < 50:
        return -1
    return 0

# ── [ANT1] Estructura CHoCH / BoS — dinámica + micro-CHoCH ───────────────────

def detect_structure(high, low, close, lookback: int = 5) -> str:
    """
    [ANT1] Mejoras:
    - Busca pivotes reales (local max/min) en vez de ventana fija
    - Detecta micro-CHoCH: precio toca 99.9% del swing high/low
    - Ventana dinámica: ajusta según cantidad de barras disponibles
    """
    h, l, c = np.asarray(high, float), np.asarray(low, float), np.asarray(close, float)
    n = len(h)
    if n < lookback * 2 + 5:
        return "NONE"

    # Pivotes reales en últimas 40 barras
    window = min(40, n - 1)
    seg_h  = h[-window:]
    seg_l  = l[-window:]
    seg_c  = c[-window:]

    # Swing high/low recientes
    mid = window // 2
    prev_hh = seg_h[:mid].max() if mid > 0 else seg_h[0]
    prev_ll = seg_l[:mid].min() if mid > 0 else seg_l[0]
    curr_h  = seg_h[mid:].max()
    curr_l  = seg_l[mid:].min()
    cc      = seg_c[-1]
    prev_c  = seg_c[mid - 1] if mid > 0 else seg_c[0]

    # BoS / CHoCH estándar
    if cc > prev_hh and curr_h > prev_hh:
        return "BoS↑" if prev_c > prev_ll else "CHoCH↑"
    if cc < prev_ll and curr_l < prev_ll:
        return "BoS↓" if prev_c < prev_hh else "CHoCH↓"

    # [MC2] Micro-CHoCH: precio a 99.9% del nivel = anticipación temprana
    if cc >= prev_hh * 0.9990 and cc < prev_hh:
        return "CHoCH↑"  # inminente ruptura alcista
    if cc <= prev_ll * 1.0010 and cc > prev_ll:
        return "CHoCH↓"  # inminente ruptura bajista

    return "NONE"

# ── [ANT5] EQL / EQH Sweeps ───────────────────────────────────────────────────

def detect_sweep(high, low, close, atr: float, lookback: int = 20, tol_mult: float = 0.15) -> str:
    """
    [ANT5] Detecta barrido de liquidez en Equal Highs / Equal Lows.
    EQL sweep = low barre zona de iguales mínimos → rebote alcista inminente
    EQH sweep = high barre zona de iguales máximos → rechazo bajista inminente
    """
    h, l, c = np.asarray(high, float), np.asarray(low, float), np.asarray(close, float)
    n = len(h)
    if n < lookback + 5:
        return "NONE"

    tol = atr * tol_mult
    seg_h = h[-(lookback + 5):-1]
    seg_l = l[-(lookback + 5):-1]

    # EQL: buscar zona de mínimos iguales
    local_lows = []
    for i in range(2, len(seg_l) - 2):
        if seg_l[i] <= seg_l[i-1] and seg_l[i] <= seg_l[i+1]:
            local_lows.append(seg_l[i])

    if len(local_lows) >= 2:
        ref_low = min(local_lows)
        # Precio actual barrió por debajo pero cerró por encima
        if l[-1] < ref_low - tol * 0.5 and c[-1] > ref_low:
            return "EQL"  # sweep alcista

    # EQH: buscar zona de máximos iguales
    local_highs = []
    for i in range(2, len(seg_h) - 2):
        if seg_h[i] >= seg_h[i-1] and seg_h[i] >= seg_h[i+1]:
            local_highs.append(seg_h[i])

    if len(local_highs) >= 2:
        ref_high = max(local_highs)
        # Precio actual barrió por encima pero cerró por debajo
        if h[-1] > ref_high + tol * 0.5 and c[-1] < ref_high:
            return "EQH"  # sweep bajista

    return "NONE"

# ── [ANT6] Squeeze Momentum ───────────────────────────────────────────────────

def calc_squeeze(close, high, low, period: int = 20, bb_mult: float = 2.0, kc_mult: float = 1.5):
    """
    [ANT6] BB dentro de KC = squeeze comprimido (energía acumulada).
    squeeze_fire = squeeze acaba de liberar = entry timing ideal.
    Retorna (in_squeeze: bool, fire: bool, direction: str)
    """
    c, h, l = np.asarray(close, float), np.asarray(high, float), np.asarray(low, float)
    if len(c) < period + 5:
        return False, False, "NONE"

    basis  = _sma(c, period)
    dev    = np.array([np.std(c[max(0, i-period+1):i+1]) for i in range(len(c))])
    bb_hi  = basis + bb_mult * dev
    bb_lo  = basis - bb_mult * dev

    atr_kc = calc_atr(h, l, c, period)
    kc_hi  = _ema(c, period) + kc_mult * atr_kc
    kc_lo  = _ema(c, period) - kc_mult * atr_kc

    in_sq  = bb_hi[-1] < kc_hi[-1] and bb_lo[-1] > kc_lo[-1]
    was_sq = bb_hi[-2] < kc_hi[-2] and bb_lo[-2] > kc_lo[-2] if len(c) > period + 1 else False
    fire   = was_sq and not in_sq  # acaba de liberar

    # Dirección del squeeze: linreg del momento
    mid    = np.array([(max(h[max(0,i-period+1):i+1]) + min(l[max(0,i-period+1):i+1])) / 2 for i in range(len(c))])
    sq_val = c - (mid + basis) / 2
    direction = "LONG" if sq_val[-1] > 0 else "SHORT"

    return in_sq, fire, direction

# ── [ANT2] TL Break — pivotes reales ─────────────────────────────────────────

def detect_tl_break(high, low, close, atr: float = 0.0, lookback: int = 30,
                    pivot_l: int = 5, pivot_r: int = 3, buffer_mult: float = 0.15) -> str:
    """
    [ANT2] Pivotes reales left/right en vez de slope lineal.
    Buffer ATR elimina falsas rupturas.
    """
    h, l, c = np.asarray(high, float), np.asarray(low, float), np.asarray(close, float)
    n = len(h)
    if n < lookback + pivot_l + pivot_r + 5:
        return "NONE"

    buf = atr * buffer_mult if atr > 0 else 0.0

    # Pivotes altos (TL bajista)
    ph_list = []
    for i in range(pivot_l, n - pivot_r - 1):
        if h[i] == h[i - pivot_l:i + pivot_r + 1].max():
            ph_list.append((i, h[i]))
    ph_list = ph_list[-2:]  # los 2 últimos

    # Pivotes bajos (TL alcista)
    pl_list = []
    for i in range(pivot_l, n - pivot_r - 1):
        if l[i] == l[i - pivot_l:i + pivot_r + 1].min():
            pl_list.append((i, l[i]))
    pl_list = pl_list[-2:]

    # TL bajista: 2 máximos decrecientes → ruptura al alza
    if len(ph_list) >= 2:
        i1, h1 = ph_list[-2]
        i2, h2 = ph_list[-1]
        if h2 < h1 and (n - 1 - i2) <= lookback:
            slope    = (h2 - h1) / max(i2 - i1, 1)
            tl_now   = h2 + slope * (n - 1 - i2)
            if c[-1] > tl_now + buf and c[-2] <= tl_now + buf:
                return "LONG"

    # TL alcista: 2 mínimos crecientes → ruptura a la baja
    if len(pl_list) >= 2:
        i1, l1 = pl_list[-2]
        i2, l2 = pl_list[-1]
        if l2 > l1 and (n - 1 - i2) <= lookback:
            slope    = (l2 - l1) / max(i2 - i1, 1)
            tl_now   = l2 + slope * (n - 1 - i2)
            if c[-1] < tl_now - buf and c[-2] >= tl_now - buf:
                return "SHORT"

    return "NONE"

# ── [ANT4] FVG mejorado — precio dentro del gap ───────────────────────────────

def detect_fvg(high, low, close=None) -> str:
    """
    [ANT4] Retorna si precio está DENTRO de un FVG activo reciente.
    Más útil para timing de entrada que detectar solo el FVG.
    """
    h, l = np.asarray(high, float), np.asarray(low, float)
    c    = np.asarray(close, float) if close is not None else h  # fallback

    # Buscar FVG en últimas 10 barras
    for i in range(len(h) - 1, max(len(h) - 11, 2), -1):
        # Bull FVG: gap alcista entre barra i-2 high y barra i low
        if l[i] > h[i - 2]:
            gap_top = l[i]
            gap_bot = h[i - 2]
            if c[-1] <= gap_top and c[-1] >= gap_bot:
                return "BULL"  # precio retrocedió al FVG = entry long
            elif c[-1] > gap_bot:
                return "BULL_ABOVE"  # FVG activo pero ya pasó
        # Bear FVG: gap bajista entre barra i high y barra i-2 low
        if h[i] < l[i - 2]:
            gap_bot = h[i]
            gap_top = l[i - 2]
            if c[-1] >= gap_bot and c[-1] <= gap_top:
                return "BEAR"  # precio retrocedió al FVG = entry short
            elif c[-1] < gap_top:
                return "BEAR_BELOW"

    return "NONE"

# ── Circuit Breaker ───────────────────────────────────────────────────────────

def check_circuit_breaker(high, low, atr: np.ndarray,
                          mult: float = 3.0, bars: int = 10) -> bool:
    h, l = np.asarray(high, float), np.asarray(low, float)
    for i in range(len(h) - 1, max(len(h) - bars - 1, 0), -1):
        if atr[i] > 0 and (h[i] - l[i]) > mult * atr[i]:
            return True
    return False

# ── HTF Score ─────────────────────────────────────────────────────────────────

def htf_score(klines_15m, klines_1h, klines_4h) -> float:
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

# ── [ANT7] Pre-Signal Score ───────────────────────────────────────────────────

def pre_signal_score(
    direction: str,
    cvd:       float,
    vdi:       float,
    sweep:     str,
    structure: str,
    squeeze:   bool,
    mfi:       float,
    vwap_dev:  float,
    rsi_cons:  int,
) -> float:
    """
    [ANT7] Score 0-100 de condiciones precursoras ANTES de cruzar umbral.
    Permite al scanner priorizar símbolos que van a señal próximamente.
    """
    s = 0.0

    # CVD acumulando en dirección (25 pts)
    cvd_v = _safe(cvd)
    if direction == "LONG" and cvd_v > 0.1:
        s += min(cvd_v * 25, 25.0)
    elif direction == "SHORT" and cvd_v < -0.1:
        s += min(abs(cvd_v) * 25, 25.0)

    # VDI desequilibrio (20 pts)
    vdi_v = _safe(vdi)
    if direction == "LONG" and vdi_v > 0.5:
        s += min(vdi_v / 3.0 * 20, 20.0)
    elif direction == "SHORT" and vdi_v < -0.5:
        s += min(abs(vdi_v) / 3.0 * 20, 20.0)

    # Sweep de liquidez (20 pts) — señal más fuerte de reversión
    if (direction == "LONG"  and sweep == "EQL") or \
       (direction == "SHORT" and sweep == "EQH"):
        s += 20.0

    # Estructura (15 pts)
    struct_pre = {
        "CHoCH↑": (15 if direction == "LONG"  else 0),
        "CHoCH↓": (15 if direction == "SHORT" else 0),
        "BoS↑":   (10 if direction == "LONG"  else 0),
        "BoS↓":   (10 if direction == "SHORT" else 0),
    }
    s += struct_pre.get(structure, 0)

    # Squeeze comprimido (10 pts) — energía acumulada = explosión inminente
    if squeeze:
        s += 10.0

    # MFI extremo (5 pts)
    mfi_v = _safe(mfi, 50.0)
    if direction == "LONG" and mfi_v < 25:
        s += 5.0
    elif direction == "SHORT" and mfi_v > 75:
        s += 5.0

    # VWAP extremo (5 pts)
    if direction == "LONG" and vwap_dev < -1.0:
        s += 5.0
    elif direction == "SHORT" and vwap_dev > 1.0:
        s += 5.0

    return round(min(s, 100.0), 1)

# ── Score compuesto [ANT8] ────────────────────────────────────────────────────

def composite_score(
    direction:   str,
    adx:         float,
    cvd:         float,
    momentum:    float,
    mfi:         float,
    vdi:         float,
    structure:   str,
    tl_break:    str,
    htf_s:       float,
    fvg:         str,
    funding:     float = 0.0,
    sweep:       str   = "NONE",
    squeeze_fire: bool = False,
    rsi_cons:    int   = 0,
    vwap_dev:    float = 0.0,
) -> float:
    """
    [ANT8] Score 0-100 ampliado con sweep, squeeze_fire, RSI consenso, VWAP.
    Pesos:
      ADX:          18
      CVD:          20
      Momentum:     14
      MFI:          10
      VDI:           8
      Estructura:   15
      HTF:           8
      FVG:           2  bonus
      Funding:       3  bonus
      Sweep:         5  bonus (anticipación)
      Squeeze Fire:  3  bonus (timing)
      RSI consenso:  2  bonus
      VWAP extremo:  2  bonus
    """
    s = 0.0

    # ADX (18 pts)
    s += min(_safe(adx) / 40.0, 1.0) * 18

    # CVD (20 pts)
    cvd_v = _safe(cvd)
    s += max(0.0, min(cvd_v if direction == "LONG" else -cvd_v, 1.0)) * 20

    # Momentum (14 pts)
    mom = _safe(momentum)
    s += max(0.0, min((mom if direction == "LONG" else -mom) * 30, 1.0)) * 14

    # MFI (10 pts)
    mfi_v = _safe(mfi, 50.0)
    if direction == "LONG":
        s += max(0.0, (mfi_v - 50) / 50) * 10
    else:
        s += max(0.0, (50 - mfi_v) / 50) * 10

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

    # FVG bonus (2 pts) — precio dentro del gap = timing óptimo
    if (direction == "LONG"  and fvg in ("BULL",)):
        s += 2
    elif (direction == "SHORT" and fvg in ("BEAR",)):
        s += 2

    # Funding bonus (3 pts)
    fr = _safe(funding)
    if direction == "SHORT" and fr > 0.0001:
        s += min(fr / 0.001, 1.0) * 3
    elif direction == "LONG" and fr < -0.0001:
        s += min(abs(fr) / 0.001, 1.0) * 3

    # [ANT5] Sweep bonus (5 pts) — mayor anticipación
    if (direction == "LONG"  and sweep == "EQL") or \
       (direction == "SHORT" and sweep == "EQH"):
        s += 5

    # [ANT6] Squeeze fire bonus (3 pts)
    if squeeze_fire:
        s += 3

    # [ANT10] RSI consenso bonus (2 pts)
    if (direction == "LONG"  and rsi_cons == +1) or \
       (direction == "SHORT" and rsi_cons == -1):
        s += 2

    # [ANT9] VWAP extremo bonus (2 pts) — timing de salida y entrada
    if direction == "LONG"  and vwap_dev < -1.0:
        s += 2
    elif direction == "SHORT" and vwap_dev > 1.0:
        s += 2

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
    symbol:       str,
    klines_3m:    list,
    klines_15m:   list,
    klines_1h:    list,
    klines_4h:    list,
    funding_rate: float = 0.0,
) -> Signal:
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
    atr_arr           = calc_atr(h, l, c, C.ATR_LEN)
    adx_arr, pdi, mdi = calc_adx(h, l, c, C.ADX_LEN)

    atr  = _safe(atr_arr[-1])
    adx  = _safe(adx_arr[-1])
    pdim = _safe(pdi[-1])
    mdim = _safe(mdi[-1])

    if atr <= 0 or not np.isfinite(atr):
        return _no_signal("invalid_atr")

    # [ANT3] CVD adaptativo
    cvd_val = _safe(calc_cvd(o, c, v, atr_arr)[-1])
    mom_val = _safe(calc_momentum(c, 10)[-1])
    mfi_val = _safe(calc_mfi(h, l, c, v, 14)[-1], 50.0)
    vdi_val = _safe(calc_vdi(c, v, 20)[-1])

    # [ANT9] VWAP deviation
    vwap_dev = _safe(calc_vwap_deviation(h, l, c, v, atr), 0.0)

    # [ANT10] RSI consenso
    rsi_cons = calc_rsi_multi(c)

    # [ANT6] Squeeze
    in_sq, sq_fire, sq_dir = calc_squeeze(c, h, l, period=20)

    cb        = C.CB_ENABLED and check_circuit_breaker(h, l, atr_arr, C.CB_ATR_MULT, C.CB_BARS)
    structure = detect_structure(h, l, c, 5)
    fvg       = detect_fvg(h, l, c)
    htf_s     = _safe(htf_score(klines_15m, klines_1h, klines_4h), 0.5)

    # [ANT2] TL break con pivotes reales y buffer ATR
    tl_break = detect_tl_break(h, l, c, atr=atr, lookback=30,
                                pivot_l=5, pivot_r=3, buffer_mult=0.15)

    # ── Dirección ─────────────────────────────────────────────────────────────
    if C.REQUIRE_TL_BREAK and tl_break == "NONE":
        return _no_signal("no_tl_break")

    direction = tl_break if tl_break != "NONE" else ("LONG" if pdim > mdim else "SHORT")

    # [ANT5] Sweep detectado post-dirección
    sweep = detect_sweep(h, l, c, atr, lookback=20, tol_mult=0.15)

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

    # ── Score y tier ──────────────────────────────────────────────────────────
    score = composite_score(
        direction, adx, cvd_val, mom_val, mfi_val,
        vdi_val, structure, tl_break, htf_s, fvg,
        funding=funding_rate,
        sweep=sweep,
        squeeze_fire=sq_fire,
        rsi_cons=rsi_cons,
        vwap_dev=vwap_dev,
    )
    tier = score_to_tier(score)

    # [ANT7] Pre-signal score
    pre_sc = pre_signal_score(
        direction, cvd_val, vdi_val, sweep, structure,
        in_sq, mfi_val, vwap_dev, rsi_cons,
    )

    # ── SL / TP ───────────────────────────────────────────────────────────────
    entry = _safe(c[-1])

    # [ANT] SL dinámico: si hay sweep confirmado → SL más ajustado (0.8x)
    sl_mult_adj = C.SL_ATR_MULT * (0.85 if sweep != "NONE" else 1.0)

    if direction == "LONG":
        sl  = entry - atr * sl_mult_adj
        tp1 = entry + atr * C.TP1_ATR_MULT
        tp2 = entry + atr * C.TP2_ATR_MULT
    else:
        sl  = entry + atr * sl_mult_adj
        tp1 = entry - atr * C.TP1_ATR_MULT
        tp2 = entry - atr * C.TP2_ATR_MULT

    log.debug(
        "[%s] %s score=%.1f pre=%.1f sweep=%s sq_fire=%s rsi=%+d vwap=%.2f",
        symbol, direction, score, pre_sc, sweep, sq_fire, rsi_cons, vwap_dev,
    )

    return Signal(
        symbol=symbol, direction=direction, score=score, tier=tier,
        entry=entry, sl=sl, tp1=tp1, tp2=tp2, atr=atr, adx=adx,
        mfi=mfi_val, vdi=vdi_val, cvd=cvd_val, momentum=mom_val,
        htf_score=htf_s, structure=structure, tl_break=tl_break,
        tl_break_active=(tl_break != "NONE"),
        circuit_breaker=cb,
        funding_rate=funding_rate,
        reason="ok",
        pre_score=pre_sc,
        sweep=sweep,
        squeeze=in_sq,
        squeeze_fire=sq_fire,
        vwap_dev=vwap_dev,
        rsi_consensus=rsi_cons,
        fvg_active=fvg,
    )
