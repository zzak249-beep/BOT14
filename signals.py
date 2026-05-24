"""
QF Machine × JP Fusion — Signal Engine v3.0
Port del indicador Pine Script a Python para trading en vivo
"""
import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import Optional
import logging

logger = logging.getLogger(__name__)


@dataclass
class SignalResult:
    direction: str          # "LONG" | "SHORT" | "FLAT"
    tier: str               # "SUPREMA" | "FUEL" | "STD" | "NONE"
    conviction: int         # 0-10
    norm_score: float
    sl_price: float
    entry_price: float
    details: dict


class QFSignalEngine:
    def __init__(self, cfg: dict):
        self.c = cfg

    @staticmethod
    def _tanh(x):
        x = np.clip(2.0 * x, -20, 20)
        e2x = np.exp(x)
        return (e2x - 1.0) / (e2x + 1.0)

    @staticmethod
    def _ema(series: pd.Series, span: int) -> pd.Series:
        return series.ewm(span=span, adjust=False).mean()

    @staticmethod
    def _sma(series: pd.Series, window: int) -> pd.Series:
        return series.rolling(window).mean()

    @staticmethod
    def _stdev(series: pd.Series, window: int) -> pd.Series:
        return series.rolling(window).std()

    @staticmethod
    def _atr(df: pd.DataFrame, period: int) -> pd.Series:
        hl = df['high'] - df['low']
        hc = (df['high'] - df['close'].shift(1)).abs()
        lc = (df['low']  - df['close'].shift(1)).abs()
        tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
        return tr.rolling(period).mean()

    @staticmethod
    def _pivothigh(series: pd.Series, left: int, right: int) -> pd.Series:
        result = pd.Series(np.nan, index=series.index)
        for i in range(left, len(series) - right):
            window = series.iloc[i - left:i + right + 1]
            if series.iloc[i] == window.max():
                result.iloc[i] = series.iloc[i]
        return result

    @staticmethod
    def _pivotlow(series: pd.Series, left: int, right: int) -> pd.Series:
        result = pd.Series(np.nan, index=series.index)
        for i in range(left, len(series) - right):
            window = series.iloc[i - left:i + right + 1]
            if series.iloc[i] == window.min():
                result.iloc[i] = series.iloc[i]
        return result

    # ── L2: Factor Engine ────────────────────────────────────
    def _l2_factors(self, df: pd.DataFrame) -> dict:
        c = self.c
        close = df['close']

        roc      = (close - close.shift(c['mom'])) / close.shift(c['mom'])
        vol_norm = self._stdev(close, c['mom']) / self._sma(close, c['mom'])
        f_mom    = (roc / vol_norm.replace(0, np.nan)).fillna(0)

        basis     = self._sma(close, c['rev'])
        basis_std = self._stdev(close, c['rev'])
        f_rev     = (-(close - basis) / basis_std.replace(0, np.nan)).fillna(0)

        obv     = (np.sign(close.diff()) * df['volume']).cumsum()
        obv_ma  = self._ema(obv, c['vol_len'])
        obv_std = self._stdev(obv, c['vol_len'])
        f_vol   = ((obv - obv_ma) / obv_std.replace(0, np.nan)).fillna(0)

        raw_score  = c['w1'] * f_mom + c['w2'] * f_rev + c['w3'] * f_vol
        comp_score = self._ema(raw_score, c['smo'])
        sc_std     = self._stdev(comp_score, c['dlen'])
        norm_score = self._tanh(comp_score / sc_std.replace(0, np.nan)).fillna(0)

        return {'f_mom': f_mom, 'f_rev': f_rev, 'f_vol': f_vol,
                'norm_score': norm_score, 'basis': basis}

    # ── L3: Signal Decay ─────────────────────────────────────
    def _l3_decay(self, df: pd.DataFrame, norm_score: pd.Series) -> bool:
        c = self.c
        fwd_ret = df['close'].pct_change()
        ic_corr = norm_score.shift(1).rolling(c['dlen']).corr(fwd_ret)
        ic_roll = self._ema(ic_corr.abs(), c['smo'])
        ic_peak = ic_roll.rolling(c['dlen']).max()
        decay_r = ic_roll / ic_peak.replace(0, np.nan)
        val = decay_r.fillna(0.5).iloc[-1]
        return bool(val >= c['dthr'])

    # ── L4: Dark Pool ────────────────────────────────────────
    def _l4_darkpool(self, df: pd.DataFrame) -> dict:
        c = self.c
        atr       = self._atr(df, c['atr_len'])
        vol_base  = self._sma(df['volume'], c['dpb'])
        vol_spike = df['volume'] > vol_base * c['dpm']
        rng_narrow= (df['high'] - df['low']) < atr * 0.6
        dp_buy    = bool((vol_spike & rng_narrow & (df['close'] > df['open'])).iloc[-1])
        dp_sell   = bool((vol_spike & rng_narrow & (df['close'] < df['open'])).iloc[-1])
        vac_up    = bool(((df['high']-df['low']) > atr*1.8) & (df['volume'] < vol_base*0.6) & (df['close'] > df['open'])).iloc[-1] if False else \
                    bool(((df['high']-df['low']).iloc[-1] > atr.iloc[-1]*1.8) and
                         (df['volume'].iloc[-1] < vol_base.iloc[-1]*0.6) and
                         (df['close'].iloc[-1] > df['open'].iloc[-1]))
        vac_dn    = bool(((df['high']-df['low']).iloc[-1] > atr.iloc[-1]*1.8) and
                         (df['volume'].iloc[-1] < vol_base.iloc[-1]*0.6) and
                         (df['close'].iloc[-1] < df['open'].iloc[-1]))
        return {'dp_buy': dp_buy, 'dp_sell': dp_sell,
                'vac_up': vac_up, 'vac_dn': vac_dn, 'atr': atr}

    # ── L5: Execution Score ──────────────────────────────────
    def _l5_exec(self, df: pd.DataFrame) -> bool:
        c = self.c
        hi_lo_r    = np.log(df['high'] / df['low'])
        spread_est = self._sma(hi_lo_r, c['spl']) * df['close']
        bp_drain   = (spread_est / df['close']) * 100
        return bool(bp_drain.iloc[-1] < c['bpt'])

    # ── L6: Momentum Asymmetry ───────────────────────────────
    def _l6_asym(self, df: pd.DataFrame) -> dict:
        c = self.c
        is_up  = df['close'] > df['open']
        is_dn  = df['close'] < df['open']
        up_rng = (df['high'] - df['low']).where(is_up, 0)
        dn_rng = (df['high'] - df['low']).where(is_dn, 0)

        avg_up = float(self._sma(up_rng, c['asl']).iloc[-1])
        avg_dn = float(self._sma(dn_rng, c['asl']).iloc[-1])

        rng_bull = avg_up / avg_dn if avg_dn > 0 else 1.0
        rng_bear = avg_dn / avg_up if avg_up > 0 else 1.0

        return {
            'asym_bull': bool(rng_bull >= c['arr']),
            'asym_bear': bool(rng_bear >= c['abr']),
            'rng_bull':  rng_bull,
            'rng_bear':  rng_bear,
        }

    # ── L7: Trendline Break ──────────────────────────────────
    def _l7_trendline(self, df: pd.DataFrame) -> dict:
        c   = self.c
        atr = self._atr(df, c['atr_len'])
        ph  = self._pivothigh(df['high'], c['tll'], c['tlr'])
        pl  = self._pivotlow(df['low'],   c['pll'], c['plr'])

        tl_break_long  = False
        tl_break_short = False

        ph_vals = ph.dropna()
        pl_vals = pl.dropna()

        if len(ph_vals) >= 2:
            i1, i2 = ph_vals.index[-1], ph_vals.index[-2]
            p1, p2 = float(ph_vals.iloc[-1]), float(ph_vals.iloc[-2])
            n1     = df.index.get_loc(i1)
            n2     = df.index.get_loc(i2)
            if p2 > p1 and (len(df) - 1 - n2) <= c['tlb']:
                slope      = (p1 - p2) / max(n1 - n2, 1)
                current_tl = p1 + slope * (len(df) - 1 - n1)
                prev_tl    = p1 + slope * (len(df) - 2 - n1)
                buf        = float(atr.iloc[-1]) * c['tlm']
                if (float(df['close'].iloc[-1]) > current_tl + buf and
                        float(df['close'].iloc[-2]) <= prev_tl + buf):
                    tl_break_long = True

        if len(pl_vals) >= 2:
            i1, i2 = pl_vals.index[-1], pl_vals.index[-2]
            p1, p2 = float(pl_vals.iloc[-1]), float(pl_vals.iloc[-2])
            n1     = df.index.get_loc(i1)
            n2     = df.index.get_loc(i2)
            if p2 < p1 and (len(df) - 1 - n2) <= c['tlb']:
                slope      = (p1 - p2) / max(n1 - n2, 1)
                current_tl = p1 + slope * (len(df) - 1 - n1)
                prev_tl    = p1 + slope * (len(df) - 2 - n1)
                buf        = float(atr.iloc[-1]) * c['tlm']
                if (float(df['close'].iloc[-1]) < current_tl - buf and
                        float(df['close'].iloc[-2]) >= prev_tl - buf):
                    tl_break_short = True

        return {'tl_break_long': tl_break_long, 'tl_break_short': tl_break_short}

    # ── L8: Swing Analysis ───────────────────────────────────
    def _l8_swings(self, df: pd.DataFrame) -> dict:
        c  = self.c
        pl = self._pivotlow(df['low'],   c['pll'], c['plr'])
        ph = self._pivothigh(df['high'], c['phl'], c['phr'])

        recent_pl = pl.dropna().tail(c['hlw'])
        recent_ph = ph.dropna().tail(c['hlw'])

        hl_count = int(sum(
            float(recent_pl.iloc[i]) > float(recent_pl.iloc[i - 1])
            for i in range(1, len(recent_pl))
        ))
        lh_count = int(sum(
            float(recent_ph.iloc[i]) < float(recent_ph.iloc[i - 1])
            for i in range(1, len(recent_ph))
        ))

        last_sl = float(recent_pl.iloc[-1]) if len(recent_pl) > 0 else np.nan
        last_sh = float(recent_ph.iloc[-1]) if len(recent_ph) > 0 else np.nan

        return {
            'sell_exhausted': bool(hl_count >= c['hlc']),
            'buy_exhausted':  bool(lh_count >= c['hhc']),
            'last_sl':        last_sl,
            'last_sh':        last_sh,
            'hl_count':       hl_count,
            'lh_count':       lh_count,
        }

    # ── L9: FVG ──────────────────────────────────────────────
    def _l9_fvg(self, df: pd.DataFrame, atr: pd.Series) -> dict:
        c = self.c
        bull_fvg = (df['low'] > df['high'].shift(2)) & \
                   ((df['low'] - df['high'].shift(2)) > atr * c['fvg_min'])
        bear_fvg = (df['high'] < df['low'].shift(2)) & \
                   ((df['low'].shift(2) - df['high']) > atr * c['fvg_min'])

        n = min(c['fvg_bars'], len(df))
        in_bull_fvg = False
        in_bear_fvg = False

        recent_bull = bull_fvg.iloc[-n:]
        if recent_bull.any():
            idx = recent_bull[recent_bull].index[-1]
            top = float(df.loc[idx, 'low'])
            loc = df.index.get_loc(idx)
            if loc >= 2:
                bot = float(df.iloc[loc - 2]['high'])
                cp  = float(df['close'].iloc[-1])
                in_bull_fvg = bool(cp <= top and cp >= bot)

        recent_bear = bear_fvg.iloc[-n:]
        if recent_bear.any():
            idx = recent_bear[recent_bear].index[-1]
            top = float(df.loc[idx, 'low'])
            bot = float(df.loc[idx, 'high'])
            cp  = float(df['close'].iloc[-1])
            in_bear_fvg = bool(cp >= bot and cp <= top)

        return {
            'bull_fvg_raw': bool(bull_fvg.iloc[-1]),
            'bear_fvg_raw': bool(bear_fvg.iloc[-1]),
            'in_bull_fvg':  in_bull_fvg,
            'in_bear_fvg':  in_bear_fvg,
        }

    # ── L10: Order Blocks ────────────────────────────────────
    def _l10_ob(self, df: pd.DataFrame, atr: pd.Series) -> dict:
        c = self.c
        strong_bull = ((df['close'] - df['open']) > atr * c['ob_imp']) & \
                      (df['close'] > df['close'].shift(1))
        strong_bear = ((df['open'] - df['close']) > atr * c['ob_imp']) & \
                      (df['close'] < df['close'].shift(1))

        bull_ob = strong_bull & (df['close'].shift(1) < df['open'].shift(1))
        bear_ob = strong_bear & (df['close'].shift(1) > df['open'].shift(1))

        n = min(c['ob_bars'], len(df))
        in_bull_ob, in_bear_ob = False, False

        recent_bull = bull_ob.iloc[-n:]
        if recent_bull.any():
            idx = recent_bull[recent_bull].index[-1]
            hi  = float(df.loc[idx, 'open'])
            lo  = float(df.loc[idx, 'close'])
            cp  = float(df['close'].iloc[-1])
            in_bull_ob = bool(cp <= hi and cp >= lo)

        recent_bear = bear_ob.iloc[-n:]
        if recent_bear.any():
            idx = recent_bear[recent_bear].index[-1]
            hi  = float(df.loc[idx, 'close'])
            lo  = float(df.loc[idx, 'open'])
            cp  = float(df['close'].iloc[-1])
            in_bear_ob = bool(cp >= lo and cp <= hi)

        return {
            'bull_ob_raw': bool(bull_ob.iloc[-1]),
            'bear_ob_raw': bool(bear_ob.iloc[-1]),
            'in_bull_ob':  in_bull_ob,
            'in_bear_ob':  in_bear_ob,
        }

    # ── L11: CVD Delta ───────────────────────────────────────
    def _l11_cvd(self, df: pd.DataFrame) -> dict:
        c = self.c
        hl_rng = df['high'] - df['low']
        bvol   = ((df['close'] - df['low']) / hl_rng.replace(0, np.nan)).fillna(0.5) * df['volume']
        svol   = ((df['high'] - df['close']) / hl_rng.replace(0, np.nan)).fillna(0.5) * df['volume']
        cvd    = (bvol - svol).cumsum()
        cvd_ema = self._ema(cvd, c['cvd_len'])

        cv  = float(cvd.iloc[-1])
        ce  = float(cvd_ema.iloc[-1])
        cp  = float(df['close'].iloc[-1])
        cpp = float(df['close'].iloc[-c['cvd_div']])
        cvp = float(cvd.iloc[-c['cvd_div']])

        return {
            'cvd_rising':   bool(cv > ce),
            'cvd_bull_div': bool(cp < cpp and cv > cvp),
            'cvd_bear_div': bool(cp > cpp and cv < cvp),
        }

    # ── L12: Squeeze Momentum ────────────────────────────────
    def _l12_squeeze(self, df: pd.DataFrame) -> dict:
        c  = self.c
        n  = c['sq_len']
        basis  = self._sma(df['close'], n)
        dev    = self._stdev(df['close'], n)
        bb_hi  = basis + c['sq_bbm'] * dev
        bb_lo  = basis - c['sq_bbm'] * dev
        kc_atr = self._atr(df, n)
        kc_ema = self._ema(df['close'], n)
        kc_hi  = kc_ema + c['sq_kcm'] * kc_atr
        kc_lo  = kc_ema - c['sq_kcm'] * kc_atr
        sq_on  = (bb_hi < kc_hi) & (bb_lo > kc_lo)
        sq_fire = ~sq_on & sq_on.shift(1).fillna(False)

        highest = df['high'].rolling(n).max()
        lowest  = df['low'].rolling(n).min()
        sq_mid  = ((highest + lowest) / 2 + basis) / 2
        sq_val  = df['close'] - sq_mid

        fired = bool(sq_fire.iloc[-1])
        val   = float(sq_val.iloc[-1])

        return {
            'sq_bull': bool(fired and val > 0),
            'sq_bear': bool(fired and val < 0),
            'sq_on':   bool(sq_on.iloc[-1]),
        }

    # ── HTF Regime ───────────────────────────────────────────
    def _htf_regime(self, df_htf: pd.DataFrame) -> dict:
        ema9  = self._ema(df_htf['close'], 9)
        ema21 = self._ema(df_htf['close'], 21)
        return {
            'htf_bull': bool(float(ema9.iloc[-1]) > float(ema21.iloc[-1])),
            'htf_bear': bool(float(ema9.iloc[-1]) < float(ema21.iloc[-1])),
        }

    # ── MAIN COMPUTE ─────────────────────────────────────────
    def compute(self, df: pd.DataFrame, df_htf: pd.DataFrame) -> SignalResult:
        if len(df) < 100:
            return SignalResult("FLAT", "NONE", 0, 0.0, np.nan, float(df['close'].iloc[-1]), {})

        c = self.c

        l2   = self._l2_factors(df)
        atr  = self._atr(df, c['atr_len'])
        l4   = self._l4_darkpool(df)
        l5   = self._l5_exec(df)
        l6   = self._l6_asym(df)
        l7   = self._l7_trendline(df)
        l8   = self._l8_swings(df)
        l9   = self._l9_fvg(df, atr)
        l10  = self._l10_ob(df, atr)
        l11  = self._l11_cvd(df)
        l12  = self._l12_squeeze(df)
        htf  = self._htf_regime(df_htf)
        l3   = self._l3_decay(df, l2['norm_score'])

        # — todos son Python bool/float nativos desde aquí —
        ns       = float(l2['norm_score'].iloc[-1])
        alive    = bool(l3)
        exec_ok  = bool(l5)
        dp_buy   = bool(l4['dp_buy'])
        dp_sell  = bool(l4['dp_sell'])
        asym_bull= bool(l6['asym_bull'])
        asym_bear= bool(l6['asym_bear'])
        tl_long  = bool(l7['tl_break_long'])
        tl_short = bool(l7['tl_break_short'])
        sell_ex  = bool(l8['sell_exhausted'])
        buy_ex   = bool(l8['buy_exhausted'])
        htf_bull = bool(htf['htf_bull'])
        htf_bear = bool(htf['htf_bear'])
        cvd_up   = bool(l11['cvd_rising'])
        cvd_bd   = bool(l11['cvd_bull_div'])
        cvd_sd   = bool(l11['cvd_bear_div'])
        sq_bull  = bool(l12['sq_bull'])
        sq_bear  = bool(l12['sq_bear'])
        bfvg     = bool(l9['in_bull_fvg'])
        sfvg     = bool(l9['in_bear_fvg'])
        bob      = bool(l10['in_bull_ob'])
        sob      = bool(l10['in_bear_ob'])

        # ── LONG ──────────────────────────────────────────────
        long_std  = ns > 0.15 and alive and exec_ok and htf_bull and asym_bull and sell_ex
        long_fuel = long_std and (tl_long or sq_bull or ((bfvg or bob) and cvd_up))
        long_sup  = long_fuel and (dp_buy or cvd_bd)

        # ── SHORT ─────────────────────────────────────────────
        short_std  = ns < -0.15 and alive and exec_ok and htf_bear and asym_bear and buy_ex
        short_fuel = short_std and (tl_short or sq_bear or ((sfvg or sob) and not cvd_up))
        short_sup  = short_fuel and (dp_sell or cvd_sd)

        # ── Conviction ────────────────────────────────────────
        long_conv = int(sum([
            ns > 0.15, alive, exec_ok, htf_bull,
            asym_bull, sell_ex, tl_long, dp_buy, cvd_up,
            (sq_bull or bfvg or bob)
        ]))
        short_conv = int(sum([
            ns < -0.15, alive, exec_ok, htf_bear,
            asym_bear, buy_ex, tl_short, dp_sell, not cvd_up,
            (sq_bear or sfvg or sob)
        ]))

        entry = float(df['close'].iloc[-1])
        atr_v = float(atr.iloc[-1])
        sl_l  = float(l8['last_sl']) if not np.isnan(l8['last_sl']) else entry - atr_v * 2
        sl_s  = float(l8['last_sh']) if not np.isnan(l8['last_sh']) else entry + atr_v * 2

        det = self._details(l2, l4, l6, l7, l8, l9, l10, l11, l12, htf, l3, l5, atr)

        if long_sup:
            return SignalResult("LONG",  "SUPREMA", long_conv,  ns, sl_l, entry, det)
        if long_fuel:
            return SignalResult("LONG",  "FUEL",    long_conv,  ns, sl_l, entry, det)
        if long_std:
            return SignalResult("LONG",  "STD",     long_conv,  ns, sl_l, entry, det)
        if short_sup:
            return SignalResult("SHORT", "SUPREMA", short_conv, ns, sl_s, entry, det)
        if short_fuel:
            return SignalResult("SHORT", "FUEL",    short_conv, ns, sl_s, entry, det)
        if short_std:
            return SignalResult("SHORT", "STD",     short_conv, ns, sl_s, entry, det)

        return SignalResult("FLAT", "NONE", max(long_conv, short_conv), ns, np.nan, entry, {})

    def _details(self, l2, l4, l6, l7, l8, l9, l10, l11, l12, htf, l3, l5, atr):
        return {
            'norm_score':    round(float(l2['norm_score'].iloc[-1]) * 100, 1),
            'f_mom':         round(float(l2['f_mom'].iloc[-1]) * 100, 1),
            'f_rev':         round(float(l2['f_rev'].iloc[-1]) * 100, 1),
            'f_vol':         round(float(l2['f_vol'].iloc[-1]) * 100, 1),
            'htf_bull':      bool(htf['htf_bull']),
            'htf_bear':      bool(htf['htf_bear']),
            'sig_alive':     bool(l3),
            'exec_ok':       bool(l5),
            'asym_bull':     bool(l6['asym_bull']),
            'asym_bear':     bool(l6['asym_bear']),
            'tl_long':       bool(l7['tl_break_long']),
            'tl_short':      bool(l7['tl_break_short']),
            'sell_exhausted':bool(l8['sell_exhausted']),
            'hl_count':      int(l8['hl_count']),
            'in_bull_fvg':   bool(l9['in_bull_fvg']),
            'in_bear_fvg':   bool(l9['in_bear_fvg']),
            'in_bull_ob':    bool(l10['in_bull_ob']),
            'in_bear_ob':    bool(l10['in_bear_ob']),
            'cvd_rising':    bool(l11['cvd_rising']),
            'cvd_bull_div':  bool(l11['cvd_bull_div']),
            'cvd_bear_div':  bool(l11['cvd_bear_div']),
            'sq_bull':       bool(l12['sq_bull']),
            'sq_bear':       bool(l12['sq_bear']),
            'sq_on':         bool(l12['sq_on']),
            'dp_buy':        bool(l4['dp_buy']),
            'dp_sell':       bool(l4['dp_sell']),
            'atr':           round(float(atr.iloc[-1]), 6),
        }
