"""
QF Machine × JP Fusion — Motor de Señales v3.0
Traducción fiel del indicador Pine Script a Python/NumPy.

Capas:
  L1  Microestructura (spread, ATR)
  L2  Motor de Factores (momentum, mean-rev, volumen)
  L3  Decaimiento de señal (IC rolling)
  L4  Dark Pool & Liquidez
  L5  Score de Ejecución
  L6  Asimetría de Momentum
  L7  Ruptura de Trendline
  L8  Análisis Swing Highs/Lows
  L9  Fair Value Gaps (ICT)
  L10 Order Blocks institucionales
  L11 Delta CVD (proxy buy/sell pressure)
  L12 Squeeze Momentum (BB dentro de KC)
"""

import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import Optional
from config import cfg


# ─── Helpers ──────────────────────────────────────────────────

def tanh_safe(x: np.ndarray) -> np.ndarray:
    return np.tanh(np.clip(x, -10, 10))

def ema(series: np.ndarray, period: int) -> np.ndarray:
    alpha = 2 / (period + 1)
    out   = np.empty_like(series, dtype=float)
    out[0] = series[0]
    for i in range(1, len(series)):
        out[i] = alpha * series[i] + (1 - alpha) * out[i - 1]
    return out

def sma(series: np.ndarray, period: int) -> np.ndarray:
    return pd.Series(series).rolling(period, min_periods=1).mean().values

def stdev(series: np.ndarray, period: int) -> np.ndarray:
    return pd.Series(series).rolling(period, min_periods=2).std(ddof=0).values

def highest(series: np.ndarray, period: int) -> np.ndarray:
    return pd.Series(series).rolling(period, min_periods=1).max().values

def lowest(series: np.ndarray, period: int) -> np.ndarray:
    return pd.Series(series).rolling(period, min_periods=1).min().values

def rolling_corr(a: np.ndarray, b: np.ndarray, period: int) -> np.ndarray:
    s_a = pd.Series(a)
    s_b = pd.Series(b)
    return s_a.rolling(period, min_periods=max(5, period // 2)).corr(s_b).values

def linreg(series: np.ndarray, period: int) -> np.ndarray:
    out = np.full(len(series), np.nan)
    for i in range(period - 1, len(series)):
        y = series[i - period + 1 : i + 1]
        x = np.arange(period)
        p = np.polyfit(x, y, 1)
        out[i] = np.polyval(p, period - 1)
    return out

def pivot_high(high: np.ndarray, left: int, right: int) -> np.ndarray:
    """Devuelve array con el valor del pivot high en la barra correspondiente, nan en el resto."""
    n   = len(high)
    out = np.full(n, np.nan)
    for i in range(left, n - right):
        window = high[i - left : i + right + 1]
        if high[i] == window.max() and np.sum(window == high[i]) == 1:
            out[i] = high[i]
    return out

def pivot_low(low: np.ndarray, left: int, right: int) -> np.ndarray:
    n   = len(low)
    out = np.full(n, np.nan)
    for i in range(left, n - right):
        window = low[i - left : i + right + 1]
        if low[i] == window.min() and np.sum(window == low[i]) == 1:
            out[i] = low[i]
    return out

def atr_calc(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int) -> np.ndarray:
    tr  = np.maximum(high - low,
          np.maximum(np.abs(high - np.roll(close, 1)),
                     np.abs(low  - np.roll(close, 1))))
    tr[0] = high[0] - low[0]
    return ema(tr, period)

def obv_calc(close: np.ndarray, volume: np.ndarray) -> np.ndarray:
    direction = np.sign(np.diff(close, prepend=close[0]))
    return np.cumsum(direction * volume)


# ─── Dataclass de señal ────────────────────────────────────────

@dataclass
class Signal:
    direction : Optional[str] = None   # "LONG" | "SHORT" | None
    tier      : str           = "STD"  # "STD" | "FUEL" | "SUP"
    conviction: int           = 0      # 0-10
    sl        : float         = 0.0
    tp        : Optional[float] = None

    # Componentes internos (para logging/dashboard)
    norm_score : float = 0.0
    sig_alive  : bool  = False
    exec_ok    : bool  = False
    htf_bull   : bool  = False
    htf_bear   : bool  = False
    asym_bull  : bool  = False
    asym_bear  : bool  = False
    sell_exhausted: bool = False
    buy_exhausted : bool = False
    tl_break_long : bool = False
    tl_break_short: bool = False
    dp_buy     : bool  = False
    dp_sell    : bool  = False
    cvd_rising : bool  = False
    cvd_bull_div: bool = False
    cvd_bear_div: bool = False
    sq_bull    : bool  = False
    sq_bear    : bool  = False
    in_bull_fvg: bool  = False
    in_bear_fvg: bool  = False
    in_bull_ob : bool  = False
    in_bear_ob : bool  = False
    squeeze_on : bool  = False
    above_vwap : bool  = False
    session    : str   = "OFF"


# ─── Motor principal ───────────────────────────────────────────

class QFJPEngine:
    """
    Recibe DataFrames OHLCV (3m y 15m), devuelve Signal.
    Todos los parámetros se leen de config.cfg.
    """

    def compute(self, ohlcv_3m: list[list], ohlcv_15m: list[list]) -> dict:
        sig = self._compute_internal(ohlcv_3m, ohlcv_15m)
        # Convertir a dict serializable
        return sig.__dict__

    def _compute_internal(self, raw_3m: list, raw_15m: list) -> Signal:
        # ── Parse ───────────────────────────────────────────────
        df3  = self._to_df(raw_3m)
        df15 = self._to_df(raw_15m)

        o, h, l, c, v = (df3["open"].values, df3["high"].values,
                         df3["low"].values, df3["close"].values,
                         df3["volume"].values)
        n = len(c)

        # ── L1 Microestructura ──────────────────────────────────
        atr_v      = atr_calc(h, l, c, cfg.ATR_LEN)
        hi_lo_r    = np.log(h / np.where(l > 0, l, 1e-9))
        spread_est = sma(hi_lo_r, cfg.SPL_LEN) * c
        bp_drain   = (spread_est / np.where(c > 0, c, 1e-9)) * 100

        # ── L2 Factores ─────────────────────────────────────────
        mom_shift = np.roll(c, cfg.MOM_LEN)
        mom_shift[:cfg.MOM_LEN] = c[:cfg.MOM_LEN]
        roc_raw   = (c - mom_shift) / np.where(mom_shift > 0, mom_shift, 1e-9)
        vol_norm_v = stdev(c, cfg.MOM_LEN) / np.where(sma(c, cfg.MOM_LEN) > 0,
                                                       sma(c, cfg.MOM_LEN), 1e-9)
        f_mom_v    = np.where(vol_norm_v != 0, roc_raw / vol_norm_v, 0)

        basis_v    = sma(c, cfg.REV_LEN)
        basis_std  = stdev(c, cfg.REV_LEN)
        f_rev_v    = np.where(basis_std != 0, -(c - basis_v) / basis_std, 0)

        obv_v      = obv_calc(c, v)
        obv_ma_v   = ema(obv_v, cfg.VOL_LEN)
        obv_std_v  = stdev(obv_v, cfg.VOL_LEN)
        f_vol_v    = np.where(obv_std_v != 0, (obv_v - obv_ma_v) / obv_std_v, 0)

        raw_score  = cfg.W_MOM * f_mom_v + cfg.W_REV * f_rev_v + cfg.W_VOL * f_vol_v
        comp_score = ema(raw_score, cfg.SMO_LEN)
        sc_std_v   = stdev(comp_score, cfg.DECAY_LEN)
        norm_score_arr = np.where(sc_std_v != 0,
                                  tanh_safe(comp_score / sc_std_v), 0)

        # ── L3 Decaimiento ──────────────────────────────────────
        fwd_ret   = np.diff(c, prepend=c[0]) / np.where(c > 0, c, 1e-9)
        ic_num    = rolling_corr(np.roll(norm_score_arr, 1), fwd_ret, cfg.DECAY_LEN)
        ic_roll   = ema(np.abs(np.nan_to_num(ic_num)), cfg.SMO_LEN)
        ic_peak   = highest(ic_roll, cfg.DECAY_LEN)
        decay_r   = np.where(ic_peak > 0, ic_roll / ic_peak, 0.5)
        sig_alive_arr = decay_r >= cfg.DECAY_THR

        # ── L4 Dark Pool ────────────────────────────────────────
        vol_base  = sma(v, cfg.DP_BASE)
        vol_spike = v > vol_base * cfg.DP_MULT
        rng_narrow = (h - l) < atr_v * 0.6
        dp_buy_arr  = vol_spike & rng_narrow & (c > o)
        dp_sell_arr = vol_spike & rng_narrow & (c < o)
        vac_up_arr  = ((h - l) > atr_v * 1.8) & (v < vol_base * 0.6) & (c > o)
        vac_dn_arr  = ((h - l) > atr_v * 1.8) & (v < vol_base * 0.6) & (c < o)

        # ── L5 Ejecución ────────────────────────────────────────
        exec_ok_arr = bp_drain < cfg.BP_THR

        # ── HTF Régimen ─────────────────────────────────────────
        c15  = df15["close"].values
        ema9_15  = ema(c15, 9)
        ema21_15 = ema(c15, 21)
        # El último valor del HTF es el régimen vigente
        htf_bull_val = bool(ema9_15[-1] > ema21_15[-1])
        htf_bear_val = bool(ema9_15[-1] < ema21_15[-1])

        # ── L6 Asimetría de Momentum ────────────────────────────
        up_rng_v = np.where(c > o, h - l, 0.0)
        dn_rng_v = np.where(c < o, h - l, 0.0)
        avg_up_r = sma(up_rng_v, cfg.ASY_LEN)
        avg_dn_r = sma(dn_rng_v, cfg.ASY_LEN)
        rng_ratio_bull = np.where(avg_dn_r > 0, avg_up_r / avg_dn_r, 1.0)
        rng_ratio_bear = np.where(avg_up_r > 0, avg_dn_r / avg_up_r, 1.0)
        asym_bull_arr  = rng_ratio_bull >= cfg.ARR
        asym_bear_arr  = rng_ratio_bear >= cfg.ABR

        # ── L7 Ruptura Trendline ────────────────────────────────
        ph_arr = pivot_high(h, cfg.TL_LEFT, cfg.TL_RIGHT)
        pl_arr = pivot_low(l,  cfg.PL_LEFT, cfg.PL_RIGHT)
        tl_break_long_arr, tl_break_short_arr = self._trendline_breaks(
            h, l, c, atr_v, ph_arr, pl_arr, n)

        # ── L8 Swing Lows/Highs ─────────────────────────────────
        sell_exhausted_arr, buy_exhausted_arr, last_sl_arr, last_sh_arr = \
            self._swing_analysis(h, l, c, pl_arr, ph_arr, n)

        # ── L9 Fair Value Gaps ──────────────────────────────────
        bull_fvg_arr, bear_fvg_arr, in_bull_fvg_arr, in_bear_fvg_arr = \
            self._fvg(h, l, c, atr_v)

        # ── L10 Order Blocks ────────────────────────────────────
        bull_ob_arr, bear_ob_arr, in_bull_ob_arr, in_bear_ob_arr = \
            self._order_blocks(o, h, l, c, atr_v)

        # ── L11 CVD Delta ───────────────────────────────────────
        hl_rng_v  = h - l
        bvol_est  = np.where(hl_rng_v > 0, ((c - l) / hl_rng_v) * v, v * 0.5)
        svol_est  = np.where(hl_rng_v > 0, ((h - c) / hl_rng_v) * v, v * 0.5)
        delta_bar = bvol_est - svol_est
        cvd_arr   = np.cumsum(delta_bar)
        cvd_ema_v = ema(cvd_arr, cfg.CVD_LEN)
        cvd_rising_arr = cvd_arr > cvd_ema_v

        div_win = cfg.CVD_DIV
        cvd_bull_div_arr = np.zeros(n, bool)
        cvd_bear_div_arr = np.zeros(n, bool)
        if n > div_win:
            cvd_bull_div_arr[div_win:] = (c[div_win:] < c[:-div_win]) & \
                                          (cvd_arr[div_win:] > cvd_arr[:-div_win])
            cvd_bear_div_arr[div_win:] = (c[div_win:] > c[:-div_win]) & \
                                          (cvd_arr[div_win:] < cvd_arr[:-div_win])

        # ── L12 Squeeze Momentum ────────────────────────────────
        sq_bull_arr, sq_bear_arr, sq_on_arr = self._squeeze(h, l, c, atr_v)

        # ── VWAP ────────────────────────────────────────────────
        hlc3      = (h + l + c) / 3
        cum_tp_v  = np.cumsum(hlc3 * v)
        cum_v     = np.cumsum(v)
        vwap_arr  = np.where(cum_v > 0, cum_tp_v / cum_v, c)
        above_vwap_arr = c > vwap_arr

        # ── Extraer último valor (barra actual) ─────────────────
        idx = n - 1

        ns     = float(norm_score_arr[idx])
        alive  = bool(sig_alive_arr[idx])
        exok   = bool(exec_ok_arr[idx])
        dpb    = bool(dp_buy_arr[idx])
        dps    = bool(dp_sell_arr[idx])
        ab     = bool(asym_bull_arr[idx])
        abe    = bool(asym_bear_arr[idx])
        se     = bool(sell_exhausted_arr[idx])
        be     = bool(buy_exhausted_arr[idx])
        tlbl   = bool(tl_break_long_arr[idx])
        tlbs   = bool(tl_break_short_arr[idx])
        ibfvg  = bool(in_bull_fvg_arr[idx])
        iberfvg= bool(in_bear_fvg_arr[idx])
        ibob   = bool(in_bull_ob_arr[idx])
        iberob = bool(in_bear_ob_arr[idx])
        cvdr   = bool(cvd_rising_arr[idx])
        cvdbd  = bool(cvd_bull_div_arr[idx])
        cvdad  = bool(cvd_bear_div_arr[idx])
        sqb    = bool(sq_bull_arr[idx])
        sqbe   = bool(sq_bear_arr[idx])
        sqon   = bool(sq_on_arr[idx])
        avwap  = bool(above_vwap_arr[idx])
        last_sl = float(last_sl_arr[idx]) if not np.isnan(last_sl_arr[idx]) else None
        last_sh = float(last_sh_arr[idx]) if not np.isnan(last_sh_arr[idx]) else None

        # ── Lógica señal final ─────────────────────────────────
        long_std  = ns > 0.15 and alive and exok and htf_bull_val and ab and se
        long_fuel = long_std and (tlbl or sqb or ((ibfvg or ibob) and cvdr))
        long_sup  = long_fuel and (dpb or cvdbd)

        short_std  = ns < -0.15 and alive and exok and htf_bear_val and abe and be
        short_fuel = short_std and (tlbs or sqbe or ((iberfvg or iberob) and not cvdr))
        short_sup  = short_fuel and (dps or cvdad)

        # ── Conviction score ────────────────────────────────────
        long_conv = sum([
            ns > 0.15, alive, exok, htf_bull_val, ab, se,
            tlbl, dpb, cvdr, (sqb or ibfvg or ibob)
        ])
        short_conv = sum([
            ns < -0.15, alive, exok, htf_bear_val, abe, be,
            tlbs, dps, not cvdr, (sqbe or iberfvg or iberob)
        ])

        # ── Determinar dirección y tier ─────────────────────────
        direction = None
        tier      = "STD"
        conviction = 0
        sl_price   = 0.0
        tp_price   = None

        if long_sup or long_fuel or long_std:
            direction  = "LONG"
            tier       = "SUP" if long_sup else ("FUEL" if long_fuel else "STD")
            conviction = long_conv
            sl_price   = last_sl if last_sl else c[idx] - atr_v[idx] * 2.0
            tp_price   = c[idx] + (c[idx] - sl_price) * cfg.TP_RR
        elif short_sup or short_fuel or short_std:
            direction  = "SHORT"
            tier       = "SUP" if short_sup else ("FUEL" if short_fuel else "STD")
            conviction = short_conv
            sl_price   = last_sh if last_sh else c[idx] + atr_v[idx] * 2.0
            tp_price   = c[idx] - (sl_price - c[idx]) * cfg.TP_RR

        return Signal(
            direction=direction, tier=tier, conviction=conviction,
            sl=sl_price, tp=tp_price,
            norm_score=ns, sig_alive=alive, exec_ok=exok,
            htf_bull=htf_bull_val, htf_bear=htf_bear_val,
            asym_bull=ab, asym_bear=abe,
            sell_exhausted=se, buy_exhausted=be,
            tl_break_long=tlbl, tl_break_short=tlbs,
            dp_buy=dpb, dp_sell=dps,
            cvd_rising=cvdr, cvd_bull_div=cvdbd, cvd_bear_div=cvdad,
            sq_bull=sqb, sq_bear=sqbe,
            in_bull_fvg=ibfvg, in_bear_fvg=iberfvg,
            in_bull_ob=ibob, in_bear_ob=iberob,
            squeeze_on=sqon, above_vwap=avwap,
        )

    # ── Sub-módulos ─────────────────────────────────────────────

    def _to_df(self, raw: list) -> pd.DataFrame:
        df = pd.DataFrame(raw, columns=["timestamp","open","high","low","close","volume"])
        for col in ["open","high","low","close","volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.dropna().reset_index(drop=True)

    def _trendline_breaks(self, h, l, c, atr_v, ph_arr, pl_arr, n):
        tl_break_long  = np.zeros(n, bool)
        tl_break_short = np.zeros(n, bool)

        ph_indices = np.where(~np.isnan(ph_arr))[0]
        pl_indices = np.where(~np.isnan(pl_arr))[0]

        if len(ph_indices) >= 2:
            ph2i, ph1i = ph_indices[-2], ph_indices[-1]
            if ph_arr[ph2i] > ph_arr[ph1i] and (n - 1 - ph2i) <= cfg.TL_LOOKBACK:
                slope = (ph_arr[ph1i] - ph_arr[ph2i]) / max(ph1i - ph2i, 1)
                for i in range(ph1i + 1, n):
                    tl_now   = ph_arr[ph1i] + slope * (i - ph1i)
                    tl_prev  = ph_arr[ph1i] + slope * (i - 1 - ph1i)
                    buf      = atr_v[i] * cfg.TL_BUF
                    if c[i] > tl_now + buf and c[i-1] <= tl_prev + buf:
                        tl_break_long[i] = True

        if len(pl_indices) >= 2:
            pl2i, pl1i = pl_indices[-2], pl_indices[-1]
            if pl_arr[pl2i] < pl_arr[pl1i] and (n - 1 - pl2i) <= cfg.TL_LOOKBACK:
                slope = (pl_arr[pl1i] - pl_arr[pl2i]) / max(pl1i - pl2i, 1)
                for i in range(pl1i + 1, n):
                    tl_now   = pl_arr[pl1i] + slope * (i - pl1i)
                    tl_prev  = pl_arr[pl1i] + slope * (i - 1 - pl1i)
                    buf      = atr_v[i] * cfg.TL_BUF
                    if c[i] < tl_now - buf and c[i-1] >= tl_prev - buf:
                        tl_break_short[i] = True

        return tl_break_long, tl_break_short

    def _swing_analysis(self, h, l, c, pl_arr, ph_arr, n):
        sell_ex = np.zeros(n, bool)
        buy_ex  = np.zeros(n, bool)
        last_sl = np.full(n, np.nan)
        last_sh = np.full(n, np.nan)

        for i in range(cfg.HL_WINDOW, n):
            # Últimos swing lows en ventana
            sl_vals = [pl_arr[j] for j in range(max(0, i - cfg.HL_WINDOW), i + 1)
                       if not np.isnan(pl_arr[j])]
            sh_vals = [ph_arr[j] for j in range(max(0, i - cfg.HL_WINDOW), i + 1)
                       if not np.isnan(ph_arr[j])]

            if sl_vals:
                last_sl[i] = sl_vals[-1]
                hl_count = sum(sl_vals[k] > sl_vals[k-1] for k in range(1, len(sl_vals)))
                sell_ex[i] = hl_count >= cfg.HL_COUNT
            if sh_vals:
                last_sh[i] = sh_vals[-1]
                lh_count = sum(sh_vals[k] < sh_vals[k-1] for k in range(1, len(sh_vals)))
                buy_ex[i] = lh_count >= cfg.HH_COUNT

        return sell_ex, buy_ex, last_sl, last_sh

    def _fvg(self, h, l, c, atr_v):
        n = len(c)
        bull_fvg = np.zeros(n, bool)
        bear_fvg = np.zeros(n, bool)
        in_bull  = np.zeros(n, bool)
        in_bear  = np.zeros(n, bool)

        bfvg_top = bfvg_bot = np.nan
        sfvg_top = sfvg_bot = np.nan
        bfvg_age = sfvg_age = 0

        for i in range(2, n):
            min_size = atr_v[i] * cfg.FVG_MIN
            b_fvg_raw = l[i] > h[i-2] and (l[i] - h[i-2]) > min_size
            s_fvg_raw = h[i] < l[i-2] and (l[i-2] - h[i]) > min_size

            if b_fvg_raw:
                bfvg_top = l[i]
                bfvg_bot = h[i-2]
                bfvg_age = 0
                bull_fvg[i] = True
            else:
                bfvg_age += 1
                if bfvg_age > cfg.FVG_BARS or (cfg.FVG_MITI and c[i] < bfvg_bot):
                    bfvg_top = bfvg_bot = np.nan

            if s_fvg_raw:
                sfvg_top = l[i-2]
                sfvg_bot = h[i]
                sfvg_age = 0
                bear_fvg[i] = True
            else:
                sfvg_age += 1
                if sfvg_age > cfg.FVG_BARS or (cfg.FVG_MITI and c[i] > sfvg_top):
                    sfvg_top = sfvg_bot = np.nan

            if not np.isnan(bfvg_top) and bfvg_bot <= c[i] <= bfvg_top:
                in_bull[i] = True
            if not np.isnan(sfvg_top) and sfvg_bot <= c[i] <= sfvg_top:
                in_bear[i] = True

        return bull_fvg, bear_fvg, in_bull, in_bear

    def _order_blocks(self, o, h, l, c, atr_v):
        n = len(c)
        bull_ob = np.zeros(n, bool)
        bear_ob = np.zeros(n, bool)
        in_bull = np.zeros(n, bool)
        in_bear = np.zeros(n, bool)

        bob_hi = bob_lo = np.nan
        sob_hi = sob_lo = np.nan
        bob_age = sob_age = 0

        for i in range(2, n):
            imp = atr_v[i] * cfg.OB_IMP
            strong_bull = (c[i] - o[i]) > imp and c[i] > c[i-1]
            strong_bear = (o[i] - c[i]) > imp and c[i] < c[i-1]

            if strong_bull and c[i-1] < o[i-1]:
                bob_hi = o[i-1]
                bob_lo = c[i-1]
                bob_age = 0
                bull_ob[i] = True
            else:
                bob_age += 1
                if bob_age > cfg.OB_BARS or c[i] < bob_lo:
                    bob_hi = bob_lo = np.nan

            if strong_bear and c[i-1] > o[i-1]:
                sob_hi = c[i-1]
                sob_lo = o[i-1]
                sob_age = 0
                bear_ob[i] = True
            else:
                sob_age += 1
                if sob_age > cfg.OB_BARS or c[i] > sob_hi:
                    sob_hi = sob_lo = np.nan

            if not np.isnan(bob_hi) and bob_lo <= c[i] <= bob_hi:
                in_bull[i] = True
            if not np.isnan(sob_hi) and sob_lo <= c[i] <= sob_hi:
                in_bear[i] = True

        return bull_ob, bear_ob, in_bull, in_bear

    def _squeeze(self, h, l, c, atr_v):
        n      = len(c)
        length = cfg.SQ_LEN
        basis  = sma(c, length)
        dev    = stdev(c, length)
        bb_hi  = basis + cfg.SQ_BBM * dev
        bb_lo  = basis - cfg.SQ_BBM * dev

        kc_basis = ema(c, length)
        kc_hi    = kc_basis + cfg.SQ_KCM * atr_v
        kc_lo    = kc_basis - cfg.SQ_KCM * atr_v

        sq_on   = (bb_hi < kc_hi) & (bb_lo > kc_lo)
        sq_fire = ~sq_on & np.roll(sq_on, 1)
        sq_fire[0] = False

        hi_mid  = highest(h, length)
        lo_mid  = lowest(l, length)
        mid_val = (hi_mid + lo_mid) / 2
        sq_val  = linreg(c - (mid_val + basis) / 2, length)

        sq_bull = sq_fire & (sq_val > 0)
        sq_bear = sq_fire & (sq_val < 0)

        return sq_bull, sq_bear, sq_on
