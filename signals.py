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

    # ── helpers ──────────────────────────────────────────────
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
        """Detecta pivot highs"""
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

        # Momentum factor
        roc = (close - close.shift(c['mom'])) / close.shift(c['mom'])
        vol_norm = self._stdev(close, c['mom']) / self._sma(close, c['mom'])
        f_mom = (roc / vol_norm.replace(0, np.nan)).fillna(0)

        # Mean-reversion factor
        basis = self._sma(close, c['rev'])
        basis_std = self._stdev(close, c['rev'])
        f_rev = (-(close - basis) / basis_std.replace(0, np.nan)).fillna(0)

        # Volume factor (OBV)
        obv = (np.sign(close.diff()) * df['volume']).cumsum()
        obv_ma  = self._ema(obv, c['vol_len'])
        obv_std = self._stdev(obv, c['vol_len'])
        f_vol = ((obv - obv_ma) / obv_std.replace(0, np.nan)).fillna(0)

        raw_score  = c['w1'] * f_mom + c['w2'] * f_rev + c['w3'] * f_vol
        comp_score = self._ema(raw_score, c['smo'])
        sc_std     = self._stdev(comp_score, c['dlen'])
        norm_score = self._tanh(comp_score / sc_std.replace(0, np.nan)).fillna(0)

        return {'f_mom': f_mom, 'f_rev': f_rev, 'f_vol': f_vol,
                'norm_score': norm_score, 'basis': basis}

    # ── L3: Signal Decay ─────────────────────────────────────
    def _l3_decay(self, df: pd.DataFrame, norm_score: pd.Series) -> pd.Series:
        c = self.c
        fwd_ret  = df['close'].pct_change()
        ic_corr  = norm_score.shift(1).rolling(c['dlen']).corr(fwd_ret)
        ic_roll  = self._ema(ic_corr.abs(), c['smo'])
        ic_peak  = ic_roll.rolling(c['dlen']).max()
        decay_r  = ic_roll / ic_peak.replace(0, np.nan)
        return decay_r.fillna(0.5) >= c['dthr']

    # ── L4: Dark Pool ────────────────────────────────────────
    def _l4_darkpool(self, df: pd.DataFrame) -> dict:
        c = self.c
        atr       = self._atr(df, c['atr_len'])
        vol_base  = self._sma(df['volume'], c['dpb'])
        vol_spike = df['volume'] > vol_base * c['dpm']
        rng_narrow= (df['high'] - df['low']) < atr * 0.6
        dp_buy    = vol_spike & rng_narrow & (df['close'] > df['open'])
        dp_sell   = vol_spike & rng_narrow & (df['close'] < df['open'])
        vac_up    = ((df['high']-df['low']) > atr*1.8) & (df['volume'] < vol_base*0.6) & (df['close'] > df['open'])
        vac_dn    = ((df['high']-df['low']) > atr*1.8) & (df['volume'] < vol_base*0.6) & (df['close'] < df['open'])
        return {'dp_buy': dp_buy, 'dp_sell': dp_sell, 'vac_up': vac_up, 'vac_dn': vac_dn, 'atr': atr}

    # ── L5: Execution Score ──────────────────────────────────
    def _l5_exec(self, df: pd.DataFrame) -> pd.Series:
        c = self.c
        hi_lo_r   = np.log(df['high'] / df['low'])
        spread_est= self._sma(hi_lo_r, c['spl']) * df['close']
        bp_drain  = (spread_est / df['close']) * 100
        return bp_drain < c['bpt']

    # ── L6: Momentum Asymmetry ───────────────────────────────
    def _l6_asym(self, df: pd.DataFrame) -> dict:
        c = self.c
        is_up = df['close'] > df['open']
        is_dn = df['close'] < df['open']
        up_rng = (df['high'] - df['low']).where(is_up, 0)
        dn_rng = (df['high'] - df['low']).where(is_dn, 0)

        avg_up = self._sma(up_rng, c['asl'])
        avg_dn = self._sma(dn_rng, c['asl'])

        rng_bull = avg_up / avg_dn.replace(0, np.nan)
        rng_bear = avg_dn / avg_up.replace(0, np.nan)

        asym_bull = rng_bull >= c['arr']
        asym_bear = rng_bear >= c['abr']
        return {'asym_bull': asym_bull, 'asym_bear': asym_bear,
                'rng_bull': rng_bull, 'rng_bear': rng_bear}

    # ── L7: Trendline Break ──────────────────────────────────
    def _l7_trendline(self, df: pd.DataFrame) -> dict:
        c = self.c
        atr = self._atr(df, c['atr_len'])
        ph  = self._pivothigh(df['high'], c['tll'], c['tlr'])
        pl  = self._pivotlow(df['low'],   c['pll'], c['plr'])

        tl_break_long  = pd.Series(False, index=df.index)
        tl_break_short = pd.Series(False, index=df.index)

        ph_vals  = ph.dropna()
        pl_vals  = pl.dropna()

        if len(ph_vals) >= 2:
            i1, i2 = ph_vals.index[-1], ph_vals.index[-2]
            p1, p2 = ph_vals.iloc[-1], ph_vals.iloc[-2]
            n1, n2 = df.index.get_loc(i1), df.index.get_loc(i2)
            if p2 > p1 and (len(df) - 1 - n2) <= c['tlb']:
                slope    = (p1 - p2) / max(n1 - n2, 1)
                current_tl = p1 + slope * (len(df) - 1 - n1)
                prev_tl    = p1 + slope * (len(df) - 2 - n1)
                buf = atr.iloc[-1] * c['tlm']
                if (df['close'].iloc[-1] > current_tl + buf and
                        df['close'].iloc[-2] <= prev_tl + buf):
                    tl_break_long.iloc[-1] = True

        if len(pl_vals) >= 2:
            i1, i2 = pl_vals.index[-1], pl_vals.index[-2]
            p1, p2 = pl_vals.iloc[-1], pl_vals.iloc[-2]
            n1, n2 = df.index.get_loc(i1), df.index.get_loc(i2)
            if p2 < p1 and (len(df) - 1 - n2) <= c['tlb']:
                slope    = (p1 - p2) / max(n1 - n2, 1)
                current_tl = p1 + slope * (len(df) - 1 - n1)
                prev_tl    = p1 + slope * (len(df) - 2 - n1)
                buf = atr.iloc[-1] * c['tlm']
                if (df['close'].iloc[-1] < current_tl - buf and
                        df['close'].iloc[-2] >= prev_tl - buf):
                    tl_break_short.iloc[-1] = True

        return {'tl_break_long': tl_break_long, 'tl_break_short': tl_break_short}

    # ── L8: Swing Analysis ───────────────────────────────────
    def _l8_swings(self, df: pd.DataFrame) -> dict:
        c = self.c
        pl = self._pivotlow(df['low'],   c['pll'], c['plr'])
        ph = self._pivothigh(df['high'], c['phl'], c['phr'])

        recent_pl = pl.dropna().tail(c['hlw'])
        recent_ph = ph.dropna().tail(c['hlw'])

        hl_count = sum(
            recent_pl.iloc[i] > recent_pl.iloc[i - 1]
            for i in range(1, len(recent_pl))
        )
        lh_count = sum(
            recent_ph.iloc[i] < recent_ph.iloc[i - 1]
            for i in range(1, len(recent_ph))
        )

        sell_exhausted = hl_count >= c['hlc']
        buy_exhausted  = lh_count >= c['hhc']
        last_sl = recent_pl.iloc[-1] if len(recent_pl) > 0 else np.nan
        last_sh = recent_ph.iloc[-1] if len(recent_ph) > 0 else np.nan

        return {
            'sell_exhausted': sell_exhausted,
            'buy_exhausted':  buy_exhausted,
            'last_sl': last_sl,
            'last_sh': last_sh,
            'hl_count': hl_count,
            'lh_count': lh_count,
        }

    # ── L9: FVG ──────────────────────────────────────────────
    def _l9_fvg(self, df: pd.DataFrame, atr: pd.Series) -> dict:
        c = self.c
        bull_fvg = (df['low'] > df['high'].shift(2)) & \
                   ((df['low'] - df['high'].shift(2)) > atr * c['fvg_min'])
        bear_fvg = (df['high'] < df['low'].shift(2)) & \
                   ((df['low'].shift(2) - df['high']) > atr * c['fvg_min'])

        # Zona activa (últimas N barras)
        n = min(c['fvg_bars'], len(df))
        recent_bull = bull_fvg.iloc[-n:].any()
        recent_bear = bear_fvg.iloc[-n:].any()

        # ¿precio dentro de zona FVG reciente?
        in_bull_fvg = False
        if bull_fvg.iloc[-n:].any():
            idx = bull_fvg.iloc[-n:][bull_fvg.iloc[-n:]].index[-1]
            _top = df.loc[idx, 'low']
            _bot = df.loc[idx, 'high'] if idx - 2 >= df.index[0] else np.nan
            top = float(_top.iloc[-1]) if isinstance(_top, pd.Series) else float(_top)
            bot = float(_bot.iloc[-1]) if isinstance(_bot, pd.Series) else float(_bot)
            if not np.isnan(bot):
                in_bull_fvg = bool(df['close'].iloc[-1] <= top and df['close'].iloc[-1] >= bot)

        in_bear_fvg = False
        if bear_fvg.iloc[-n:].any():
            idx = bear_fvg.iloc[-n:][bear_fvg.iloc[-n:]].index[-1]
            _top = df.loc[idx, 'low']
            _bot = df.loc[idx, 'high']
            top = float(_top.iloc[-1]) if isinstance(_top, pd.Series) else float(_top)
            bot = float(_bot.iloc[-1]) if isinstance(_bot, pd.Series) else float(_bot)
            in_bear_fvg = bool(df['close'].iloc[-1] >= bot and df['close'].iloc[-1] <= top)

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

        if bull_ob.iloc[-n:].any():
            idx = bull_ob.iloc[-n:][bull_ob.iloc[-n:]].index[-1]
            _hi = df.loc[idx, 'open']
            _lo = df.loc[idx, 'close']
            hi  = float(_hi.iloc[-1]) if isinstance(_hi, pd.Series) else float(_hi)
            lo  = float(_lo.iloc[-1]) if isinstance(_lo, pd.Series) else float(_lo)
            in_bull_ob = bool(df['close'].iloc[-1] <= hi and df['close'].iloc[-1] >= lo)

        if bear_ob.iloc[-n:].any():
            idx = bear_ob.iloc[-n:][bear_ob.iloc[-n:]].index[-1]
            _hi = df.loc[idx, 'close']
            _lo = df.loc[idx, 'open']
            hi  = float(_hi.iloc[-1]) if isinstance(_hi, pd.Series) else float(_hi)
            lo  = float(_lo.iloc[-1]) if isinstance(_lo, pd.Series) else float(_lo)
            in_bear_ob = bool(df['close'].iloc[-1] >= lo and df['close'].iloc[-1] <= hi)

        return {
            'bull_ob_raw': bool(bull_ob.iloc[-1]),
            'bear_ob_raw': bool(bear_ob.iloc[-1]),
            'in_bull_ob':  in_bull_ob,
            'in_bear_ob':  in_bear_ob,
        }

    # ── L11: CVD Delta ───────────────────────────────────────
    def _l11_cvd(self, df: pd.DataFrame) -> dict:
        c = self.c
        hl_rng   = df['high'] - df['low']
        bvol     = ((df['close'] - df['low']) / hl_rng.replace(0, np.nan)).fillna(0.5) * df['volume']
        svol     = ((df['high'] - df['close']) / hl_rng.replace(0, np.nan)).fillna(0.5) * df['volume']
        delta    = bvol - svol
        cvd      = delta.cumsum()
        cvd_ema  = self._ema(cvd, c['cvd_len'])
        cvd_rising   = bool(cvd.iloc[-1] > cvd_ema.iloc[-1])
        cvd_bull_div = bool(df['close'].iloc[-1] < df['close'].iloc[-c['cvd_div']] and
                            cvd.iloc[-1] > cvd.iloc[-c['cvd_div']])
        cvd_bear_div = bool(df['close'].iloc[-1] > df['close'].iloc[-c['cvd_div']] and
                            cvd.iloc[-1] < cvd.iloc[-c['cvd_div']])
        return {'cvd_rising': cvd_rising, 'cvd_bull_div': cvd_bull_div, 'cvd_bear_div': cvd_bear_div}

    # ── L12: Squeeze Momentum ────────────────────────────────
    def _l12_squeeze(self, df: pd.DataFrame) -> dict:
        c  = self.c
        n  = c['sq_len']
        basis   = self._sma(df['close'], n)
        dev     = self._stdev(df['close'], n)
        bb_hi   = basis + c['sq_bbm'] * dev
        bb_lo   = basis - c['sq_bbm'] * dev
        kc_atr  = self._atr(df, n)
        kc_ema  = self._ema(df['close'], n)
        kc_hi   = kc_ema + c['sq_kcm'] * kc_atr
        kc_lo   = kc_ema - c['sq_kcm'] * kc_atr
        sq_on   = (bb_hi < kc_hi) & (bb_lo > kc_lo)
        sq_fire = (~sq_on.astype(bool)) & sq_on.astype(bool).shift(1, fill_value=False)

        highest = df['high'].rolling(n).max()
        lowest  = df['low'].rolling(n).min()
        sq_mid  = (highest + lowest) / 2
        sq_mid  = (sq_mid + basis) / 2
        sq_val  = df['close'] - sq_mid

        _sq_fire_last = bool(sq_fire.iloc[-1])
        _sq_val_last  = float(sq_val.iloc[-1]) if not pd.isna(sq_val.iloc[-1]) else 0.0
        sq_bull   = bool(_sq_fire_last and _sq_val_last > 0)
        sq_bear   = bool(_sq_fire_last and _sq_val_last < 0)
        sq_active = bool(sq_on.astype(bool).iloc[-1])

        return {'sq_bull': sq_bull, 'sq_bear': sq_bear, 'sq_on': sq_active}

    # ── HTF (simulated from same df at higher TF) ────────────
    def _htf_regime(self, df_htf: pd.DataFrame) -> dict:
        ema9  = self._ema(df_htf['close'], 9)
        ema21 = self._ema(df_htf['close'], 21)
        htf_bull = bool(ema9.iloc[-1] > ema21.iloc[-1])
        htf_bear = bool(ema9.iloc[-1] < ema21.iloc[-1])
        return {'htf_bull': htf_bull, 'htf_bear': htf_bear}

    # ── MAIN COMPUTE ─────────────────────────────────────────
    def compute(self, df: pd.DataFrame, df_htf: pd.DataFrame) -> SignalResult:
        """
        df:     DataFrame con columnas open/high/low/close/volume (3m)
        df_htf: DataFrame de la TF superior (15m)
        """
        if len(df) < 100:
            return SignalResult("FLAT", "NONE", 0, 0.0, np.nan, df['close'].iloc[-1], {})

        c = self.c

        l2     = self._l2_factors(df)
        atr    = self._atr(df, c['atr_len'])
        l4     = self._l4_darkpool(df)
        l5_ok  = self._l5_exec(df)
        l6     = self._l6_asym(df)
        l7     = self._l7_trendline(df)
        l8     = self._l8_swings(df)
        l9     = self._l9_fvg(df, atr)
        l10    = self._l10_ob(df, atr)
        l11    = self._l11_cvd(df)
        l12    = self._l12_squeeze(df)
        htf    = self._htf_regime(df_htf)
        l3_ok  = self._l3_decay(df, l2['norm_score'])

        ns     = float(l2['norm_score'].iloc[-1])
        alive  = bool(l3_ok.iloc[-1])
        exec_ok= bool(l5_ok.iloc[-1])
        dp_buy = bool(l4['dp_buy'].iloc[-1])
        dp_sell= bool(l4['dp_sell'].iloc[-1])
        asym_bull = bool(l6['asym_bull'].iloc[-1])
        asym_bear = bool(l6['asym_bear'].iloc[-1])
        tl_long   = bool(l7['tl_break_long'].iloc[-1])
        tl_short  = bool(l7['tl_break_short'].iloc[-1])

        # ── LONG signals ──
        long_std  = (ns > 0.15 and alive and exec_ok and
                     htf['htf_bull'] and asym_bull and l8['sell_exhausted'])
        long_fuel = long_std and (tl_long or l12['sq_bull'] or
                                  ((l9['in_bull_fvg'] or l10['in_bull_ob']) and l11['cvd_rising']))
        long_sup  = long_fuel and (dp_buy or l11['cvd_bull_div'])

        # ── SHORT signals ──
        short_std  = (ns < -0.15 and alive and exec_ok and
                      htf['htf_bear'] and asym_bear and l8['buy_exhausted'])
        short_fuel = short_std and (tl_short or l12['sq_bear'] or
                                    ((l9['in_bear_fvg'] or l10['in_bear_ob']) and not l11['cvd_rising']))
        short_sup  = short_fuel and (dp_sell or l11['cvd_bear_div'])

        # ── Conviction score ──
        long_conv = int(sum([
            ns > 0.15, alive, exec_ok, htf['htf_bull'],
            asym_bull, l8['sell_exhausted'], tl_long,
            dp_buy, l11['cvd_rising'],
            (l12['sq_bull'] or l9['in_bull_fvg'] or l10['in_bull_ob'])
        ]))
        short_conv = int(sum([
            ns < -0.15, alive, exec_ok, htf['htf_bear'],
            asym_bear, l8['buy_exhausted'], tl_short,
            dp_sell, not l11['cvd_rising'],
            (l12['sq_bear'] or l9['in_bear_fvg'] or l10['in_bear_ob'])
        ]))

        entry = float(df['close'].iloc[-1])
        atr_v = float(atr.iloc[-1])

        if long_sup:
            return SignalResult("LONG", "SUPREMA", long_conv, ns,
                                float(l8['last_sl']) if not np.isnan(l8['last_sl']) else entry - atr_v * 2,
                                entry, self._details(l2, l4, l6, l7, l8, l9, l10, l11, l12, htf, l3_ok, l5_ok, atr))
        if long_fuel:
            return SignalResult("LONG", "FUEL", long_conv, ns,
                                float(l8['last_sl']) if not np.isnan(l8['last_sl']) else entry - atr_v * 2,
                                entry, self._details(l2, l4, l6, l7, l8, l9, l10, l11, l12, htf, l3_ok, l5_ok, atr))
        if long_std:
            return SignalResult("LONG", "STD", long_conv, ns,
                                float(l8['last_sl']) if not np.isnan(l8['last_sl']) else entry - atr_v * 2,
                                entry, self._details(l2, l4, l6, l7, l8, l9, l10, l11, l12, htf, l3_ok, l5_ok, atr))
        if short_sup:
            return SignalResult("SHORT", "SUPREMA", short_conv, ns,
                                float(l8['last_sh']) if not np.isnan(l8['last_sh']) else entry + atr_v * 2,
                                entry, self._details(l2, l4, l6, l7, l8, l9, l10, l11, l12, htf, l3_ok, l5_ok, atr))
        if short_fuel:
            return SignalResult("SHORT", "FUEL", short_conv, ns,
                                float(l8['last_sh']) if not np.isnan(l8['last_sh']) else entry + atr_v * 2,
                                entry, self._details(l2, l4, l6, l7, l8, l9, l10, l11, l12, htf, l3_ok, l5_ok, atr))
        if short_std:
            return SignalResult("SHORT", "STD", short_conv, ns,
                                float(l8['last_sh']) if not np.isnan(l8['last_sh']) else entry + atr_v * 2,
                                entry, self._details(l2, l4, l6, l7, l8, l9, l10, l11, l12, htf, l3_ok, l5_ok, atr))

        return SignalResult("FLAT", "NONE",
                            max(long_conv, short_conv), ns,
                            np.nan, entry, {})

    def _details(self, l2, l4, l6, l7, l8, l9, l10, l11, l12, htf, l3, l5, atr):
        return {
            'norm_score':    round(float(l2['norm_score'].iloc[-1]) * 100, 1),
            'f_mom':         round(float(l2['f_mom'].iloc[-1]) * 100, 1),
            'f_rev':         round(float(l2['f_rev'].iloc[-1]) * 100, 1),
            'f_vol':         round(float(l2['f_vol'].iloc[-1]) * 100, 1),
            'htf_bull':      htf['htf_bull'],
            'htf_bear':      htf['htf_bear'],
            'sig_alive':     bool(l3.iloc[-1]),
            'exec_ok':       bool(l5.iloc[-1]),
            'asym_bull':     bool(l6['asym_bull'].iloc[-1]),
            'asym_bear':     bool(l6['asym_bear'].iloc[-1]),
            'tl_long':       bool(l7['tl_break_long'].iloc[-1]),
            'tl_short':      bool(l7['tl_break_short'].iloc[-1]),
            'sell_exhausted':l8['sell_exhausted'],
            'hl_count':      l8['hl_count'],
            'in_bull_fvg':   l9['in_bull_fvg'],
            'in_bear_fvg':   l9['in_bear_fvg'],
            'in_bull_ob':    l10['in_bull_ob'],
            'in_bear_ob':    l10['in_bear_ob'],
            'cvd_rising':    l11['cvd_rising'],
            'cvd_bull_div':  l11['cvd_bull_div'],
            'cvd_bear_div':  l11['cvd_bear_div'],
            'sq_bull':       l12['sq_bull'],
            'sq_bear':       l12['sq_bear'],
            'sq_on':         l12['sq_on'],
            'dp_buy':        bool(l4['dp_buy'].iloc[-1]),
            'dp_sell':       bool(l4['dp_sell'].iloc[-1]),
            'atr':           round(float(atr.iloc[-1]), 6),
        }
