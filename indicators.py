"""
strategy/indicators.py — QF×JP v3.4 Indicadores
=================================================
Port completo de todos los indicadores del Pine Script v3.4.
Recibe DataFrame OHLCV y devuelve dict con todas las señales.
"""
from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Optional
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ─── RESULTADO DE INDICADORES ─────────────────────────────────────────────────

@dataclass
class IndicatorResult:
    # Precios y ATR
    price: float = 0.0
    atr: float   = 0.0
    atr_avg: float = 0.0

    # L2 Factores
    norm_score: float = 0.0
    f_mom: float = 0.0
    f_rev: float = 0.0
    f_vol: float = 0.0
    sig_alive: bool = False
    decay_r: float = 0.0

    # M2 ADX
    adx: float = 0.0
    dmi_plus: float = 0.0
    dmi_minus: float = 0.0
    trend_strong: bool = False
    trend_up: bool = False
    trend_dn: bool = False
    is_lateral: bool = False
    regime: str = "NEUTRAL"

    # L4 Dark Pool
    dp_buy: bool = False
    dp_sell: bool = False
    vac_up: bool = False
    vac_dn: bool = False

    # L5 Ejecución
    exec_ok: bool = True
    bp_drain: float = 0.0

    # L6 Asimetría
    asym_bull: bool = False
    asym_bear: bool = False
    rng_ratio_bull: float = 1.0
    rng_ratio_bear: float = 1.0

    # L7 Trendline
    tl_break_long: bool = False
    tl_break_short: bool = False

    # L8 Swing HL/LH
    sell_exhausted: bool = False
    buy_exhausted: bool = False
    last_sl: Optional[float] = None   # último swing low
    last_sh: Optional[float] = None   # último swing high
    hl_count: int = 0
    lh_count: int = 0

    # L9 FVG
    in_bull_fvg: bool = False
    in_bear_fvg: bool = False
    bull_fvg_new: bool = False
    bear_fvg_new: bool = False

    # L10 Order Blocks
    in_bull_ob: bool = False
    in_bear_ob: bool = False
    bull_ob_new: bool = False
    bear_ob_new: bool = False
    in_brk_bull: bool = False
    in_brk_bear: bool = False
    ob_quality_long: int = 0
    ob_quality_short: int = 0

    # L11 CVD
    cvd: float = 0.0
    cvd_rising: bool = False
    cvd_score: float = 0.5
    cvd_bull_div: bool = False
    cvd_bear_div: bool = False

    # L12 Squeeze
    sq_on: bool = False
    sq_fire: bool = False
    sq_bull: bool = False
    sq_bear: bool = False

    # RSI Divergencias
    rsi_val: float = 50.0
    rsi_bull_div_reg: bool = False
    rsi_bull_div_hid: bool = False
    rsi_bear_div_reg: bool = False
    rsi_bear_div_hid: bool = False

    # L13 CHoCH / BoS
    choch_bull: bool = False
    choch_bear: bool = False
    bos_bull: bool = False
    bos_bear: bool = False
    mkt_bullish: bool = True

    # L14 Liquidity Sweeps
    liq_bull_sweep: bool = False
    liq_bear_sweep: bool = False

    # L15 Volume Profile
    poc: Optional[float] = None
    vah: Optional[float] = None
    val: Optional[float] = None
    near_poc: bool = False
    above_poc: bool = True
    near_vah: bool = False
    near_val: bool = False
    vp_long_signal: bool = False
    vp_short_signal: bool = False

    # L16 OI Delta sintético
    oi_conf_long: bool = False
    oi_conf_short: bool = False
    oi_squeeze: bool = False
    oi_ratio: float = 0.5

    # L17 LS Ratio sentiment
    ls_contrarian_long: bool = False
    ls_contrarian_short: bool = False
    ls_extreme_long: bool = False
    ls_extreme_short: bool = False
    rsi_ls: float = 50.0

    # VWAP
    vwap: float = 0.0
    above_vwap: bool = True

    # Filtros
    vol_ok: bool = True
    vol_pct: int = 100

    # Circuit Breaker
    circuit_ok: bool = True

    # SL/TP dinámicos
    sl_long: float = 0.0
    sl_short: float = 0.0
    tp0_long: float = 0.0    # partial TP
    tp0_short: float = 0.0
    tp1_long: float = 0.0
    tp1_short: float = 0.0
    tp2_long: float = 0.0
    tp2_short: float = 0.0
    rr1_long: float = 0.0
    rr1_short: float = 0.0

    # Entry refinement
    ent_reject_bull: bool = False
    ent_reject_bear: bool = False

    # Sessions
    in_asia: bool = False
    in_london: bool = False
    in_ny: bool = False
    in_overlap: bool = False
    ses_active: bool = True
    ses_label: str = "OFF"


# ─── HELPER: tanh ─────────────────────────────────────────────────────────────

def f_tanh(x: float) -> float:
    x = max(-20.0, min(20.0, x * 2.0))
    e = np.exp(x)
    return (e - 1.0) / (e + 1.0)


# ─── CALCULADORA PRINCIPAL ────────────────────────────────────────────────────

class QFJPIndicators:
    """
    Calcula todos los indicadores del QF×JP v3.4 sobre un DataFrame OHLCV.

    Uso:
        calc = QFJPIndicators(config)
        res  = calc.compute(df_3m, df_15m, df_1h, df_1w, df_1m)
    """

    def __init__(self, cfg):
        self.cfg = cfg
        # Estado persistente para CHoCH/BoS y OBs
        self._mkt_bullish = True
        self._bob_hi: Optional[float] = None
        self._bob_lo: Optional[float] = None
        self._bob_age: int = 0
        self._sob_hi: Optional[float] = None
        self._sob_lo: Optional[float] = None
        self._sob_age: int = 0
        self._brk_bull_zones: list = []  # [(hi,lo), ...]
        self._brk_bear_zones: list = []
        self._bfvg_zones: list = []       # bull FVG: [(top, bot, age)]
        self._sfvg_zones: list = []       # bear FVG: [(top, bot, age)]
        self._wf_long_signals = 0
        self._wf_long_wins = 0
        self._wf_short_signals = 0
        self._wf_short_wins = 0
        self._cb_bars_left = 0

    def compute(
        self,
        df:    pd.DataFrame,
        df15m: Optional[pd.DataFrame] = None,
        df1h:  Optional[pd.DataFrame] = None,
        df1w:  Optional[pd.DataFrame] = None,
        df1m:  Optional[pd.DataFrame] = None,
    ) -> IndicatorResult:
        r = IndicatorResult()
        if df is None or len(df) < 50:
            return r

        c  = df["close"].values.astype(float)
        o  = df["open"].values.astype(float)
        h  = df["high"].values.astype(float)
        lo = df["low"].values.astype(float)
        v  = df["volume"].values.astype(float)
        n  = len(c)

        r.price = c[-1]

        try:
            self._calc_atr(r, h, lo, c, n)
            self._calc_sessions(r, df)
            self._calc_circuit_breaker(r, o, c, n)
            self._calc_adx(r, h, lo, c, n)
            self._calc_factors(r, c, o, v, n)
            self._calc_decay(r, c, n)
            self._calc_dark_pool(r, o, h, lo, c, v, n)
            self._calc_exec(r, h, lo, c, n)
            self._calc_asymmetry(r, o, h, lo, c, n)
            self._calc_swings(r, h, lo, c, n)
            self._calc_trendline(r, h, lo, c, n)
            self._calc_choch_bos(r)
            self._calc_liq_sweeps(r, h, lo, c, n)
            self._calc_fvg(r, h, lo, c, n)
            self._calc_ob(r, o, h, lo, c, n)
            self._calc_cvd(r, o, h, lo, c, v, n)
            self._calc_squeeze(r, h, lo, c, n)
            self._calc_rsi_divs(r, c, n)
            self._calc_vwap(r, h, lo, c, v)
            self._calc_volume_profile(r, h, lo, c, v, n)
            self._calc_oi_delta(r, o, h, lo, c, v, n)
            self._calc_ls_ratio(r, c, n)
            self._calc_entry_refinement(r, df1m)
            self._calc_htf(r, df15m, df1h, df1w)
            self._calc_sl_tp(r)
            self._calc_vol_filter(r)
        except Exception as e:
            logger.error(f"[Indicators] Error: {e}", exc_info=True)

        return r

    # ─── ATR ──────────────────────────────────────────────────────────────────

    def _calc_atr(self, r, h, lo, c, n):
        p = self.cfg.ATR_LEN
        tr = np.zeros(n)
        for i in range(1, n):
            tr[i] = max(h[i]-lo[i], abs(h[i]-c[i-1]), abs(lo[i]-c[i-1]))
        atr = np.zeros(n)
        if n > p:
            atr[p] = np.mean(tr[1:p+1])
            for i in range(p+1, n):
                atr[i] = (atr[i-1]*(p-1) + tr[i]) / p
        r.atr     = float(atr[-1]) or r.price * 0.005
        r.atr_avg = float(np.mean(atr[max(0,n-20):]))

    # ─── SESSIONS ─────────────────────────────────────────────────────────────

    def _calc_sessions(self, r, df):
        from datetime import timezone
        import datetime
        now = datetime.datetime.now(timezone.utc)
        h = now.hour
        r.in_asia    = 0 <= h < 8
        r.in_london  = 7 <= h < 16
        r.in_ny      = 13 <= h < 22
        r.in_overlap = r.in_london and r.in_ny
        r.ses_active = r.in_ny or r.in_london or (self.cfg.SESSION_ASIA and r.in_asia)
        r.ses_label  = "OVL" if r.in_overlap else "NY" if r.in_ny else "LDN" if r.in_london else "ASIA" if r.in_asia else "OFF"

    # ─── CIRCUIT BREAKER ──────────────────────────────────────────────────────

    def _calc_circuit_breaker(self, r, o, c, n):
        if not self.cfg.CB_ENABLED:
            r.circuit_ok = True; return
        giant = abs(c[-1] - o[-1]) > r.atr_avg * self.cfg.CB_MULT
        if giant:
            self._cb_bars_left = self.cfg.CB_BARS
        elif self._cb_bars_left > 0:
            self._cb_bars_left -= 1
        r.circuit_ok = self._cb_bars_left == 0

    # ─── ADX ──────────────────────────────────────────────────────────────────

    def _calc_adx(self, r, h, lo, c, n):
        p = self.cfg.ADX_LEN
        if n < p * 2: return
        plus_dm  = np.zeros(n)
        minus_dm = np.zeros(n)
        tr       = np.zeros(n)
        for i in range(1, n):
            up   = h[i]  - h[i-1]
            down = lo[i-1] - lo[i]
            plus_dm[i]  = up   if (up > down and up > 0)   else 0
            minus_dm[i] = down if (down > up and down > 0) else 0
            tr[i] = max(h[i]-lo[i], abs(h[i]-c[i-1]), abs(lo[i]-c[i-1]))

        def rma(arr, period):
            out = np.zeros(len(arr))
            out[period] = np.sum(arr[1:period+1])
            for i in range(period+1, len(arr)):
                out[i] = out[i-1] - out[i-1]/period + arr[i]
            return out

        tr14   = rma(tr, p);   pd14 = rma(plus_dm, p);  md14 = rma(minus_dm, p)
        pdi    = np.where(tr14 > 0, 100*np.divide(pd14, tr14, where=tr14>0, out=np.zeros_like(tr14)), 0)
        mdi    = np.where(tr14 > 0, 100*np.divide(md14, tr14, where=tr14>0, out=np.zeros_like(tr14)), 0)
        dx     = np.where((pdi+mdi) > 0, 100*np.divide(np.abs(pdi-mdi),(pdi+mdi), where=(pdi+mdi)>0, out=np.zeros_like(pdi)), 0)
        adx    = rma(dx, p)

        r.dmi_plus  = float(pdi[-1])
        r.dmi_minus = float(mdi[-1])
        r.adx       = float(adx[-1])
        r.trend_strong = r.adx >= self.cfg.ADX_TREND
        r.trend_up     = r.dmi_plus  > r.dmi_minus and r.trend_strong
        r.trend_dn     = r.dmi_minus > r.dmi_plus  and r.trend_strong
        r.is_lateral   = r.adx < self.cfg.ADX_LATERAL
        r.regime       = ("TEND↑" if r.trend_up else "TEND↓") if r.trend_strong else ("LATERAL" if r.is_lateral else "NEUTRAL")

    # ─── FACTORES L2 ──────────────────────────────────────────────────────────

    def _calc_factors(self, r, c, o, v, n):
        p = self.cfg.MOM_LEN
        if n < p * 2: return
        roc = (c[-1] - c[-1-p]) / c[-1-p] if c[-1-p] else 0
        std = np.std(c[-p:]) / np.mean(c[-p:]) if np.mean(c[-p:]) else 1
        r.f_mom = roc / std if std else 0

        rev_p = self.cfg.REV_LEN
        basis = np.mean(c[-rev_p:])
        bstd  = np.std(c[-rev_p:])
        r.f_rev = -(c[-1] - basis) / bstd if bstd else 0

        obv = np.zeros(n)
        for i in range(1, n):
            obv[i] = obv[i-1] + (v[i] if c[i] > c[i-1] else -v[i] if c[i] < c[i-1] else 0)
        vp  = self.cfg.VOL_LEN
        obv_ma  = np.mean(obv[-vp:])
        obv_std = np.std(obv[-vp:])
        r.f_vol = (obv[-1] - obv_ma) / obv_std if obv_std else 0

        adx_f   = min(1.0, r.adx / (self.cfg.ADX_TREND * 2.0))
        w_mom   = 0.40 + adx_f * 0.40 * 0.40
        w_rev   = max(0.30 * 0.30, 0.30 - adx_f * 0.30 * 0.50)
        w_vol   = 0.30
        w_total = w_mom + w_rev + w_vol
        raw     = (w_mom * r.f_mom + w_rev * r.f_rev + w_vol * r.f_vol) / w_total if w_total else 0
        # EMA suavizado 3
        comp = raw  # simplificado
        dl   = self.cfg.DECAY_LEN
        wnd  = c[-dl:] if n >= dl else c
        sc_std = np.std(wnd) if len(wnd) else 1
        r.norm_score = f_tanh(comp / sc_std) if sc_std else 0

    # ─── DECAY L3 ─────────────────────────────────────────────────────────────

    def _calc_decay(self, r, c, n):
        dl = self.cfg.DECAY_LEN
        if n < dl + 5: r.sig_alive = True; r.decay_r = 0.8; return
        # IC = correlación entre returns pasados (proxy de norm_score) y fwd returns
        rets = np.diff(c[-(dl+1):]) / c[-(dl+1):-1]  # dl returns
        fwd  = np.roll(rets, -1)[:-1]                  # forward returns
        src  = rets[:-1]
        if len(src) < 5 or np.std(src) < 1e-10 or np.std(fwd) < 1e-10:
            r.sig_alive = True; r.decay_r = 0.8; return
        ic     = float(np.corrcoef(src, fwd)[0,1])
        ic_abs = min(1.0, abs(ic) * 3)  # amplify small correlations
        r.decay_r   = ic_abs
        r.sig_alive = True  # en datos reales siempre activo salvo IC negativo fuerte
        if ic < -0.5:  # solo bloquear si IC fuertemente negativo
            r.sig_alive = False

    # ─── DARK POOL L4 ─────────────────────────────────────────────────────────

    def _calc_dark_pool(self, r, o, h, lo, c, v, n):
        p = self.cfg.VOL_LEN
        if n < p: return
        vol_base = np.mean(v[-p:])
        vol_spike = v[-1] > vol_base * self.cfg.__dict__.get("DP_MULT", 2.5)
        rng_narrow = (h[-1]-lo[-1]) < r.atr * 0.6
        r.dp_buy  = bool(vol_spike and rng_narrow and c[-1] > o[-1])
        r.dp_sell = bool(vol_spike and rng_narrow and c[-1] < o[-1])
        r.vac_up  = bool((h[-1]-lo[-1]) > r.atr*1.8 and v[-1] < vol_base*0.6 and c[-1] > o[-1])
        r.vac_dn  = bool((h[-1]-lo[-1]) > r.atr*1.8 and v[-1] < vol_base*0.6 and c[-1] < o[-1])

    # ─── EJECUCIÓN L5 ─────────────────────────────────────────────────────────

    def _calc_exec(self, r, h, lo, c, n):
        spl = 5
        if n < spl: r.exec_ok = True; return
        hi_lo_r    = np.log(h[-spl:] / lo[-spl:])
        spread_est = float(np.mean(hi_lo_r) * c[-1])
        r.bp_drain = (spread_est / c[-1]) * 100
        r.exec_ok  = r.bp_drain < self.cfg.EXEC_BPT

    # ─── ASIMETRÍA L6 ────────────────────────────────────────────────────────

    def _calc_asymmetry(self, r, o, h, lo, c, n):
        p = 10
        if n < p: return
        up = np.where(c[-p:] > o[-p:], h[-p:]-lo[-p:], 0.0)
        dn = np.where(c[-p:] < o[-p:], h[-p:]-lo[-p:], 0.0)
        avg_up = np.mean(up); avg_dn = np.mean(dn)
        r.rng_ratio_bull = avg_up/avg_dn if avg_dn > 0 else 1.0
        r.rng_ratio_bear = avg_dn/avg_up if avg_up > 0 else 1.0
        r.asym_bull = r.rng_ratio_bull >= 1.20
        r.asym_bear = r.rng_ratio_bear >= 1.20

    # ─── SWINGS L8 ────────────────────────────────────────────────────────────

    def _calc_swings(self, r, h, lo, c, n):
        win = min(self.cfg.HL_WINDOW, n)
        lft, rgt = 5, 3
        pls = []; phs = []
        for i in range(lft, win - rgt):
            idx = n - win + i
            if idx < lft or idx >= n - rgt: continue
            if all(lo[idx] <= lo[idx-j] for j in range(1, lft+1)) and \
               all(lo[idx] <= lo[idx+j] for j in range(1, rgt+1)):
                pls.append(lo[idx])
            if all(h[idx] >= h[idx-j] for j in range(1, lft+1)) and \
               all(h[idx] >= h[idx+j] for j in range(1, rgt+1)):
                phs.append(h[idx])

        r.last_sl = float(pls[-1]) if pls else None
        r.last_sh = float(phs[-1]) if phs else None

        # HL count
        hl = sum(1 for i in range(1, len(pls)) if pls[i] > pls[i-1])
        lh = sum(1 for i in range(1, len(phs)) if phs[i] < phs[i-1])
        r.hl_count = hl; r.lh_count = lh
        r.sell_exhausted = hl >= 2
        r.buy_exhausted  = lh >= 2

    # ─── TRENDLINE L7 ─────────────────────────────────────────────────────────

    def _calc_trendline(self, r, h, lo, c, n):
        win = min(self.cfg.TL_LOOKBACK, n-5)
        lft, rgt = 5, 3
        phs = []; pls = []
        for i in range(lft, win - rgt):
            idx = n - win + i
            if all(h[idx] >= h[idx-j] for j in range(1, lft+1)) and \
               all(h[idx] >= h[idx+j] for j in range(1, rgt+1)):
                phs.append((idx, h[idx]))
            if all(lo[idx] <= lo[idx-j] for j in range(1, lft+1)) and \
               all(lo[idx] <= lo[idx+j] for j in range(1, rgt+1)):
                pls.append((idx, lo[idx]))

        r.tl_break_long  = False
        r.tl_break_short = False

        if len(phs) >= 2:
            i1, p1 = phs[-2]; i2, p2 = phs[-1]
            if p2 < p1:  # descending highs → trendline bajista
                slope = (p2 - p1) / max(i2 - i1, 1)
                tl_now = p2 + slope * (n-1 - i2)
                if c[-1] > tl_now + r.atr * 0.15 and c[-2] <= tl_now:
                    r.tl_break_long = True

        if len(pls) >= 2:
            i1, p1 = pls[-2]; i2, p2 = pls[-1]
            if p2 > p1:  # ascending lows → trendline alcista
                slope = (p2 - p1) / max(i2 - i1, 1)
                tl_now = p2 + slope * (n-1 - i2)
                if c[-1] < tl_now - r.atr * 0.15 and c[-2] >= tl_now:
                    r.tl_break_short = True

    # ─── CHoCH / BoS L13 ─────────────────────────────────────────────────────

    def _calc_choch_bos(self, r):
        sl = r.last_sl; sh = r.last_sh
        price = r.price
        r.choch_bull = False; r.choch_bear = False
        r.bos_bull   = False; r.bos_bear   = False

        if sh is not None and price > sh:
            if self._mkt_bullish:
                r.bos_bull = True
            else:
                r.choch_bull = True
            self._mkt_bullish = True

        if sl is not None and price < sl:
            if not self._mkt_bullish:
                r.bos_bear = True
            else:
                r.choch_bear = True
            self._mkt_bullish = False

        r.mkt_bullish = self._mkt_bullish

    # ─── LIQUIDITY SWEEPS L14 ─────────────────────────────────────────────────

    def _calc_liq_sweeps(self, r, h, lo, c, n):
        sl = r.last_sl; sh = r.last_sh
        r.liq_bull_sweep = sl is not None and lo[-1] < sl and c[-1] > sl
        r.liq_bear_sweep = sh is not None and h[-1]  > sh and c[-1] < sh

    # ─── FVG L9 ───────────────────────────────────────────────────────────────

    def _calc_fvg(self, r, h, lo, c, n):
        if n < 3: return
        atr = r.atr; min_sz = atr * self.cfg.FVG_MIN_MULT

        # Nueva FVG alcista: low > high[2]
        bull_new = lo[-1] > h[-3] and (lo[-1] - h[-3]) > min_sz
        bear_new = h[-1] < lo[-3] and (lo[-3] - h[-1]) > min_sz

        # Actualizar zonas
        new_bull = [(lo[-1], h[-3], 0)] if bull_new else []
        new_bear = [(lo[-3], h[-1], 0)] if bear_new else []

        updated_bull = []
        for top, bot, age in self._bfvg_zones:
            age += 1
            if age <= self.cfg.FVG_BARS and not (c[-1] < bot):
                updated_bull.append((top, bot, age))
        updated_bull.extend(new_bull)
        self._bfvg_zones = updated_bull[-self.cfg.FVG_MAX:]

        updated_bear = []
        for top, bot, age in self._sfvg_zones:
            age += 1
            if age <= self.cfg.FVG_BARS and not (c[-1] > top):
                updated_bear.append((top, bot, age))
        updated_bear.extend(new_bear)
        self._sfvg_zones = updated_bear[-self.cfg.FVG_MAX:]

        r.bull_fvg_new = bool(bull_new)
        r.bear_fvg_new = bool(bear_new)
        r.in_bull_fvg  = any(bot <= c[-1] <= top for top, bot, _ in self._bfvg_zones)
        r.in_bear_fvg  = any(bot <= c[-1] <= top for top, bot, _ in self._sfvg_zones)

    # ─── ORDER BLOCKS L10 ─────────────────────────────────────────────────────

    def _calc_ob(self, r, o, h, lo, c, n):
        if n < 3: return
        atr = r.atr
        strong_bull = (c[-1]-o[-1]) > atr*self.cfg.OB_IMP_MULT and c[-1] > c[-2]
        strong_bear = (o[-1]-c[-1]) > atr*self.cfg.OB_IMP_MULT and c[-1] < c[-2]

        # Actualizar bull OB
        if strong_bull and c[-2] < o[-2]:
            prev_hi = self._bob_hi
            self._bob_hi  = o[-2]; self._bob_lo = c[-2]; self._bob_age = 0
            imp = min(10.0, (c[-1]-o[-1])/(max(atr,0.0001)*self.cfg.OB_IMP_MULT)*5+3)
            r.ob_quality_long = int(imp)
            if prev_hi is not None:
                self._brk_bear_zones.append((prev_hi, self._bob_lo))
                self._brk_bear_zones = self._brk_bear_zones[-4:]
            r.bull_ob_new = True
        else:
            self._bob_age += 1
            if self._bob_age > self.cfg.OB_BARS or (self._bob_lo is not None and c[-1] < self._bob_lo):
                self._bob_hi = None; self._bob_lo = None

        if strong_bear and c[-2] > o[-2]:
            prev_hi = self._sob_hi
            self._sob_hi  = c[-2]; self._sob_lo = o[-2]; self._sob_age = 0
            imp = min(10.0, (o[-1]-c[-1])/(max(atr,0.0001)*self.cfg.OB_IMP_MULT)*5+3)
            r.ob_quality_short = int(imp)
            if prev_hi is not None:
                self._brk_bull_zones.append((prev_hi, self._sob_lo))
                self._brk_bull_zones = self._brk_bull_zones[-4:]
            r.bear_ob_new = True
        else:
            self._sob_age += 1
            if self._sob_age > self.cfg.OB_BARS or (self._sob_hi is not None and c[-1] > self._sob_hi):
                self._sob_hi = None; self._sob_lo = None

        r.in_bull_ob = (self._bob_hi is not None and self._bob_lo is not None and
                        self._bob_lo <= c[-1] <= self._bob_hi)
        r.in_bear_ob = (self._sob_hi is not None and self._sob_lo is not None and
                        self._sob_lo <= c[-1] <= self._sob_hi)
        r.in_brk_bull = any(lo <= c[-1] <= hi for hi, lo in self._brk_bull_zones)
        r.in_brk_bear = any(lo <= c[-1] <= hi for hi, lo in self._brk_bear_zones)

    # ─── CVD L11 ──────────────────────────────────────────────────────────────

    def _calc_cvd(self, r, o, h, lo, c, v, n):
        roll = min(self.cfg.CVD_ROLL, n)
        hl   = h[-roll:] - lo[-roll:]
        bvol = np.where(hl > 0, (c[-roll:]-lo[-roll:]) / np.maximum(hl,1e-8) * v[-roll:], v[-roll:]*0.5)
        svol = np.where(hl > 0, (h[-roll:]-c[-roll:]) / np.maximum(hl,1e-8) * v[-roll:], v[-roll:]*0.5)
        delta = bvol - svol
        cvd = float(np.sum(delta))

        ema_p = self.cfg.CVD_LEN
        if n > ema_p:
            cvd_series = np.cumsum(delta)
            k = 2/(ema_p+1)
            ema = cvd_series[0]
            for x in cvd_series[1:]:
                ema = x*k + ema*(1-k)
            r.cvd_rising = cvd > ema
        else:
            r.cvd_rising = True

        r.cvd = cvd
        cvd_std = float(np.std(delta[-ema_p:])) if n > ema_p else 1
        cvd_z   = (cvd - float(np.mean(delta[-ema_p:]))) / cvd_std if cvd_std else 0
        r.cvd_score = max(0, min(1, (f_tanh(cvd_z)+1)/2))

        # Divergencias
        dv = min(5, n//2)
        r.cvd_bull_div = c[-1] < c[-1-dv] and cvd > float(np.sum(delta[-1-dv:-dv]))
        r.cvd_bear_div = c[-1] > c[-1-dv] and cvd < float(np.sum(delta[-1-dv:-dv]))

    # ─── SQUEEZE L12 ──────────────────────────────────────────────────────────

    def _calc_squeeze(self, r, h, lo, c, n):
        p = self.cfg.SQ_LEN
        if n < p: return
        basis = np.mean(c[-p:])
        dev   = np.std(c[-p:])
        bb_hi = basis + 2.0 * dev
        bb_lo = basis - 2.0 * dev

        atr_sq = float(np.mean([max(h[i]-lo[i], abs(h[i]-c[i-1]), abs(lo[i]-c[i-1]))
                                 for i in range(max(1,n-p), n)]))
        kc_hi = np.mean(c[-p:]) + 1.5 * atr_sq
        kc_lo = np.mean(c[-p:]) - 1.5 * atr_sq

        sq_on  = bb_hi < kc_hi and bb_lo > kc_lo
        # Previous state (simplified)
        r.sq_on   = sq_on
        r.sq_fire = not sq_on  # simplified: fire whenever BB expands

        # Momentum value
        highest = max(h[-p:])
        lowest  = min(lo[-p:])
        sq_mid  = (highest + lowest + basis) / 3
        sq_val  = c[-1] - sq_mid
        r.sq_bull = r.sq_fire and sq_val > 0
        r.sq_bear = r.sq_fire and sq_val < 0

    # ─── RSI DIVERGENCIAS ─────────────────────────────────────────────────────

    def _calc_rsi_divs(self, r, c, n):
        p = self.cfg.RSI_LEN
        if n < p*2: r.rsi_val = 50; return
        delta = np.diff(c[-p*2:], prepend=c[-p*2])
        gain  = np.where(delta>0, delta, 0.)
        loss  = np.where(delta<0, -delta, 0.)
        ag = np.mean(gain[-p:]); al = np.mean(loss[-p:])
        rs = ag/al if al > 0 else 100
        r.rsi_val = float(100 - 100/(1+rs))
        dv = min(self.cfg.RSI_LEN, n//2)
        r.rsi_bull_div_reg = c[-1] < c[-1-dv] and r.rsi_val > 50-dv and r.rsi_val < 50
        r.rsi_bull_div_hid = c[-1] > c[-1-dv] and r.rsi_val < 50 and r.rsi_val < 50
        r.rsi_bear_div_reg = c[-1] > c[-1-dv] and r.rsi_val > 50
        r.rsi_bear_div_hid = c[-1] < c[-1-dv] and r.rsi_val > 50

    # ─── VWAP ─────────────────────────────────────────────────────────────────

    def _calc_vwap(self, r, h, lo, c, v):
        tp  = (h + lo + c) / 3
        vol = np.maximum(v, 1e-8)
        r.vwap     = float((tp * vol).sum() / vol.sum())
        r.above_vwap = c[-1] > r.vwap

    # ─── VOLUME PROFILE L15 ───────────────────────────────────────────────────

    def _calc_volume_profile(self, r, h, lo, c, v, n):
        p = min(self.cfg.VP_LEN, n)
        bins = self.cfg.VP_BINS
        h_s = h[-p:]; lo_s = lo[-p:]; c_s = c[-p:]; v_s = v[-p:]
        hi_max = float(np.max(h_s)); lo_min = float(np.min(lo_s))
        rng = hi_max - lo_min
        if rng < 1e-8: return

        bin_sz  = rng / bins
        bin_vol = np.zeros(bins)
        for i in range(p):
            avg_p = (h_s[i]+lo_s[i]+c_s[i])/3
            idx   = min(bins-1, int((avg_p-lo_min)/bin_sz))
            bin_vol[idx] += v_s[i]

        poc_idx   = int(np.argmax(bin_vol))
        r.poc     = lo_min + poc_idx * bin_sz + bin_sz/2
        total_vol = float(np.sum(bin_vol)); vol_70 = total_vol * 0.70

        cum = 0; r.vah = hi_max
        for i in range(bins-1, -1, -1):
            cum += bin_vol[i]
            if cum <= vol_70:
                r.vah = lo_min + i * bin_sz + bin_sz

        cum = 0; r.val = lo_min
        for i in range(bins):
            cum += bin_vol[i]
            if cum <= vol_70:
                r.val = lo_min + i * bin_sz

        price = c[-1]
        r.near_poc = abs(price - r.poc) / r.atr < 0.5 if r.atr else False
        r.above_poc = price > r.poc
        r.near_vah  = abs(price - r.vah) / r.atr < 0.5 if r.atr else False
        r.near_val  = abs(price - r.val) / r.atr < 0.5 if r.atr else False
        r.vp_long_signal  = r.near_val and c[-1] > (c[-2] if n>1 else c[-1])
        r.vp_short_signal = r.near_vah and c[-1] < (c[-2] if n>1 else c[-1])

    # ─── OI DELTA L16 ────────────────────────────────────────────────────────

    def _calc_oi_delta(self, r, o, h, lo, c, v, n):
        p = self.cfg.OI_LEN
        if n < p: return
        buy_v  = np.where(c[-p:]>o[-p:], v[-p:], 0.)
        sell_v = np.where(c[-p:]<o[-p:], v[-p:], 0.)
        oi_buy  = float(np.mean(buy_v))
        oi_sell = float(np.mean(sell_v))
        total   = oi_buy + oi_sell
        r.oi_ratio = oi_buy/total if total > 0 else 0.5

        price_up = c[-1] > c[-1-p]
        price_dn = c[-1] < c[-1-p]
        r.oi_conf_long  = price_up and r.oi_ratio > 0.55
        r.oi_conf_short = price_dn and r.oi_ratio < 0.45
        r.oi_squeeze    = (h[-1]-lo[-1]) > r.atr*1.5 and r.oi_ratio < 0.40 and price_up

    # ─── LS RATIO L17 ────────────────────────────────────────────────────────

    def _calc_ls_ratio(self, r, c, n):
        p = self.cfg.LS_LEN
        if n < p*2: return
        delta = np.diff(c[-p*2:], prepend=c[-p*2])
        gain  = np.where(delta>0, delta, 0.)
        loss  = np.where(delta<0, -delta, 0.)
        ag = np.mean(gain[-p:]); al = np.mean(loss[-p:])
        rs = ag/al if al > 0 else 100
        r.rsi_ls = float(100 - 100/(1+rs))

        pct_hi = float(np.percentile(np.array([r.rsi_ls]*p), 80))
        pct_lo = float(np.percentile(np.array([r.rsi_ls]*p), 20))
        r.ls_extreme_long  = r.rsi_ls > max(pct_hi, 70)
        r.ls_extreme_short = r.rsi_ls < min(pct_lo, 30)
        r.ls_contrarian_long  = r.ls_extreme_short
        r.ls_contrarian_short = r.ls_extreme_long

    # ─── ENTRY REFINEMENT ENT ─────────────────────────────────────────────────

    def _calc_entry_refinement(self, r, df1m):
        if df1m is None or len(df1m) < 2:
            r.ent_reject_bull = True
            r.ent_reject_bear = True
            return
        o1 = float(df1m["open"].iloc[-1]); c1 = float(df1m["close"].iloc[-1])
        h1 = float(df1m["high"].iloc[-1]); l1 = float(df1m["low"].iloc[-1])
        body = abs(c1 - o1)
        body_sz = max(body, r.atr * 0.05)
        wick_lo = min(c1,o1) - l1
        wick_hi = h1 - max(c1,o1)
        r.ent_reject_bull = (wick_lo / body_sz) >= 0.6 and c1 > o1
        r.ent_reject_bear = (wick_hi / body_sz) >= 0.6 and c1 < o1

    # ─── HTF ALINEACIÓN ───────────────────────────────────────────────────────

    def _calc_htf(self, r, df15m, df1h, df1w):
        def ema(closes, p):
            if len(closes) < p: return closes[-1]
            k = 2/(p+1); e = closes[0]
            for x in closes[1:]: e = x*k+e*(1-k)
            return e

        # 15m
        htf_bull = htf_bear = False
        if df15m is not None and len(df15m) >= 21:
            c15 = df15m["close"].values.astype(float)
            htf_bull = ema(c15,9) > ema(c15,21)
            htf_bear = not htf_bull

        # 1h
        htf2_bull = htf2_bear = False
        if df1h is not None and len(df1h) >= 21:
            c1h = df1h["close"].values.astype(float)
            htf2_bull = ema(c1h,9) > ema(c1h,21)
            htf2_bear = not htf2_bull

        # Weekly
        htf3_bull = htf3_bear = False
        if df1w is not None and len(df1w) >= 10:
            c1w = df1w["close"].values.astype(float)
            htf3_bull = c1w[-1] > ema(c1w,10)
            htf3_bear = not htf3_bull

        # Contar scores HTF
        r.__dict__.setdefault("htf_score_long",  0)
        r.__dict__.setdefault("htf_score_short", 0)
        r.__dict__.setdefault("htf3_bull", False)
        r.__dict__.setdefault("htf3_bear", False)
        r.__dict__["htf3_bull"] = htf3_bull
        r.__dict__["htf3_bear"] = htf3_bear
        r.__dict__["htf_score_long"]  = (1 if htf_bull else 0)+(1 if htf2_bull else 0)+(1 if r.mkt_bullish else 0)
        r.__dict__["htf_score_short"] = (1 if htf_bear else 0)+(1 if htf2_bear else 0)+(1 if not r.mkt_bullish else 0)

    # ─── SL/TP DINÁMICOS ─────────────────────────────────────────────────────

    def _calc_sl_tp(self, r):
        price = r.price; atr = r.atr
        cfg = self.cfg

        # SL dinámico: máx(estructura, ATR×mult)
        sl_dist_l = max(atr * cfg.ATR_SL_MULT,
                        price - r.last_sl if r.last_sl else atr)
        sl_dist_s = max(atr * cfg.ATR_SL_MULT,
                        r.last_sh - price if r.last_sh else atr)

        r.sl_long  = round(price - sl_dist_l, 6)
        r.sl_short = round(price + sl_dist_s, 6)

        r.tp0_long  = round(price + atr * cfg.ATR_PTP_MULT, 6)
        r.tp0_short = round(price - atr * cfg.ATR_PTP_MULT, 6)
        r.tp1_long  = round(price + atr * cfg.ATR_TP1_MULT, 6)
        r.tp1_short = round(price - atr * cfg.ATR_TP1_MULT, 6)
        r.tp2_long  = r.last_sh if r.last_sh else round(price + atr * cfg.ATR_TP2_MULT, 6)
        r.tp2_short = r.last_sl if r.last_sl else round(price - atr * cfg.ATR_TP2_MULT, 6)

        r.rr1_long  = (r.tp1_long-price)/sl_dist_l  if sl_dist_l > 0 else 0
        r.rr1_short = (price-r.tp1_short)/sl_dist_s if sl_dist_s > 0 else 0

    # ─── FILTRO VOLATILIDAD ───────────────────────────────────────────────────

    def _calc_vol_filter(self, r):
        if not self.cfg.VOL_FILTER_ON:
            r.vol_ok = True; r.vol_pct = 100; return
        r.vol_pct = int(r.atr / r.atr_avg * 100) if r.atr_avg > 0 else 100
        r.vol_ok  = r.atr > r.atr_avg * self.cfg.VOL_FILTER_THR
