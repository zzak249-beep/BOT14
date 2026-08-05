"""
FibStruct Strategy v2 — Python port fiel de Pine Script v1.5.2
Mejoras:
  - initial_bars_since_signal: cooldown persiste entre reinicios
  - volume_confirm: señal requiere volumen > EMA20 del volumen
  - Signal incluye volume y closest_fib para el notifier
  - Pivot strict: igual que Pine (>= max del rango)
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import Optional
import config as cfg


@dataclass
class Signal:
    action:           str            # "BUY" | "SELL"
    trigger:          str            # "choch", "sweep+engulf", etc.
    confluence_score: float
    fib_618:          Optional[float]
    fib_target:       Optional[float]
    fib_tgt50:        Optional[float]
    fib_direction:    int
    structure_bias:   int
    atr:              float
    close:            float
    sw_high1:         Optional[float]
    sw_low1:          Optional[float]
    volume:           float = 0.0
    near_fib:         Optional[str]  = None   # nombre del nivel más cercano
    in_premium:       bool = False
    in_discount:      bool = False


# ── Indicadores ───────────────────────────────────────────────
def _rma(s: pd.Series, p: int) -> pd.Series:
    """Wilder's MA — ta.atr de Pine."""
    return s.ewm(alpha=1/p, min_periods=p, adjust=False).mean()

def _ema(s: pd.Series, p: int) -> pd.Series:
    return s.ewm(span=p, adjust=False).mean()

def compute_atr(df: pd.DataFrame, p: int = 14) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"].shift(1)
    tr = pd.concat([df["high"]-df["low"], (df["high"]-c).abs(), (df["low"]-c).abs()], axis=1).max(axis=1)
    return _rma(tr, p)


# ── Estrategia ────────────────────────────────────────────────
class FibStructStrategy:
    def __init__(
        self,
        swing_len:      int   = cfg.SWING_LEN,
        atr_filter:     bool  = cfg.ATR_FILTER,
        atr_mult:       float = cfg.ATR_MULT,
        cooldown:       int   = cfg.COOLDOWN_BARS,
        eq_tol:         float = cfg.EQ_TOL,
        conf_tol:       float = cfg.CONFLUENCE_TOL,
        sweep_conf:     bool  = True,
        strict_engulf:  bool  = cfg.STRICT_ENGULF,
        min_confluence: float = cfg.MIN_CONFLUENCE,
        volume_confirm: bool  = cfg.VOLUME_CONFIRM,
    ):
        self.SL          = swing_len
        self.atr_filter  = atr_filter
        self.atr_mult    = atr_mult
        self.cooldown    = cooldown
        self.eq_tol      = eq_tol
        self.conf_tol    = conf_tol
        self.sweep_conf  = sweep_conf
        self.strict      = strict_engulf
        self.min_conf    = min_confluence
        self.vol_confirm = volume_confirm

    def analyze(
        self,
        df: pd.DataFrame,
        initial_bars_since_signal: int = 999,
    ) -> Optional[Signal]:
        """
        Procesa el DataFrame barra a barra (máquina de estados).
        Devuelve Signal SOLO si la señal ocurrió en la última barra.

        Args:
            df:                         DataFrame OHLCV ordenado ascendente.
            initial_bars_since_signal:  Barras transcurridas desde la última
                                        señal (calculadas por el bot desde state.json).
                                        Permite preservar el cooldown tras reinicios.
        """
        SL     = self.SL
        WARMUP = max(SL * 3, 50)
        n      = len(df)
        if n < WARMUP + SL + 5:
            return None

        df = df.copy().reset_index(drop=True)
        df["atr"]      = compute_atr(df, 14)
        df["body"]     = (df["close"] - df["open"]).abs()
        df["body_ema"] = _ema(df["body"], 14)
        df["vol_ema"]  = _ema(df["volume"], 20)

        # ── Variables de estado ──────────────────────────────────
        sw_h1 = sw_h2 = None;  sh1i = sh2i = -1
        sw_l1 = sw_l2 = None;  sl1i = sl2i = -1

        eqh_on = eql_on = False
        eqhp   = eqlp   = None
        eqh_s2i = eql_s2i = -1

        swept_hi = swept_li = -1
        bias = 0;  brk_hi = brk_li = -1;  choch_dir = 0

        fsh = fsl = None;  fshi = fsli = -1
        fh_live = fl_live = False;  fdir = 0

        bars_cd   = initial_bars_since_signal
        last_sig  = -1
        final_sig: Optional[Signal] = None

        for i in range(n):
            r   = df.iloc[i]
            atr = r["atr"] if not np.isnan(r["atr"]) else 0.0
            ok  = i >= WARMUP
            bars_cd += 1

            # ── PIVOTS ──────────────────────────────────────────
            pi     = i - SL
            new_sh = new_sl = False

            if ok and pi >= SL and pi + SL < n:
                ws  = pi - SL;  we = pi + SL + 1
                ph  = df["high"].iloc[pi]
                pl  = df["low"].iloc[pi]
                amin = atr * self.atr_mult if self.atr_filter else 0.0

                if ph >= df["high"].iloc[ws:we].max():
                    if sw_l1 is None or (ph - sw_l1) >= amin:
                        sw_h2,sh2i = sw_h1,sh1i
                        sw_h1,sh1i = ph, pi
                        new_sh = True

                if pl <= df["low"].iloc[ws:we].min():
                    if sw_h1 is None or (sw_h1 - pl) >= amin:
                        sw_l2,sl2i = sw_l1,sl1i
                        sw_l1,sl1i = pl, pi
                        new_sl = True

            # ── EQH / EQL ───────────────────────────────────────
            eq_tol = atr * self.eq_tol if atr > 0 else 0.0

            if new_sh and sw_h2 is not None and abs(sw_h1 - sw_h2) <= eq_tol:
                eqh_on = True;  eqhp = (sw_h1+sw_h2)/2;  eqh_s2i = sh2i
            if new_sl and sw_l2 is not None and abs(sw_l1 - sw_l2) <= eq_tol:
                eql_on = True;  eqlp = (sw_l1+sw_l2)/2;  eql_s2i = sl2i

            # ── SWEEPS ──────────────────────────────────────────
            sph = spl = False

            if ok:
                rh = eqhp if eqh_on else sw_h1;  rhi = eqh_s2i if eqh_on else sh1i
                rl = eqlp if eql_on else sw_l1;  rli = eql_s2i if eql_on else sl1i

                if rh and rhi != swept_hi and r["high"] > rh and r["close"] < rh and r["open"] < rh:
                    sph = True;  swept_hi = rhi
                    if eqh_on: eqh_on = False

                if rl and rli != swept_li and r["low"] < rl and r["close"] > rl and r["open"] > rl:
                    spl = True;  swept_li = rli
                    if eql_on: eql_on = False

            # ── BOS / CHoCH ─────────────────────────────────────
            is_bos = is_choch = bull = bear = False

            if ok:
                b_bull = sw_h1 and r["close"] > sw_h1 and sh1i != brk_hi
                b_bear = sw_l1 and r["close"] < sw_l1 and sl1i != brk_li

                if b_bull and b_bear:
                    (b_bear := False) if bias <= 0 else (b_bull := False)

                if b_bull:
                    if bias <= 0:
                        is_choch = True;  choch_dir = 1
                        swept_hi = swept_li = -1
                    else:
                        is_bos = True
                    bias = 1;  bull = True;  brk_hi = sh1i

                if b_bear:
                    if bias >= 0:
                        is_choch = True;  choch_dir = -1
                        swept_hi = swept_li = -1
                    else:
                        is_bos = True
                    bias = -1;  bear = True;  brk_li = sl1i

            # ── FIBONACCI ENGINE ─────────────────────────────────
            def _anchor_bull():
                nonlocal fsh,fshi,fsl,fsli,fh_live,fl_live,fdir
                fdir=1; fsh=r["high"]; fshi=i
                fsl=sw_l1; fsli=sl1i if sl1i>=0 else max(0,i-10)
                fh_live=True; fl_live=False

            def _anchor_bear():
                nonlocal fsh,fshi,fsl,fsli,fh_live,fl_live,fdir
                fdir=-1; fsl=r["low"]; fsli=i
                fsh=sw_h1; fshi=sh1i if sh1i>=0 else max(0,i-10)
                fl_live=True; fh_live=False

            if is_choch and bull:  _anchor_bull()
            if is_choch and bear:  _anchor_bear()
            if is_bos   and bull:  _anchor_bull()
            if is_bos   and bear:  _anchor_bear()

            if not bull and not bear:
                if fh_live and fsh and r["high"] > fsh: fsh=r["high"]; fshi=i
                if fl_live and fsl and r["low"]  < fsl: fsl=r["low"];  fsli=i

            if new_sh and fh_live and sw_h1: fsh=sw_h1; fshi=sh1i; fh_live=False
            if new_sl and fl_live and sw_l1: fsl=sw_l1; fsli=sl1i; fl_live=False
            if new_sh and not fh_live and sw_h1 and fsh and sw_h1!=fsh: fsh=sw_h1; fshi=sh1i
            if new_sl and not fl_live and sw_l1 and fsl and sw_l1!=fsl: fsl=sw_l1; fsli=sl1i

            # ── FIB LEVELS ──────────────────────────────────────
            fv = fsh and fsl and fsh > fsl and fdir != 0
            f236=f382=f500=f618=f786=ftgt=ftgt50=None

            if fv:
                rng = fsh - fsl
                if fdir == 1:
                    f236=fsh-rng*0.236; f382=fsh-rng*0.382; f500=fsh-rng*0.500
                    f618=fsh-rng*0.618; f786=fsh-rng*0.786
                    ftgt50=fsh+rng*0.5; ftgt=fsh+rng*0.618
                else:
                    f236=fsl+rng*0.236; f382=fsl+rng*0.382; f500=fsl+rng*0.500
                    f618=fsl+rng*0.618; f786=fsl+rng*0.786
                    ftgt50=fsl-rng*0.5; ftgt=fsl-rng*0.618

            # ── PREMIUM / DISCOUNT ──────────────────────────────
            prem = disc = False
            if f500 and fv:
                if fdir==1: prem=r["close"]>f500; disc=not prem
                else:       prem=r["close"]<f500; disc=not prem

            # ── CONFLUENCE ──────────────────────────────────────
            ct = atr * self.conf_tol if atr > 0 else 0.001

            def near(lvl):
                if lvl is None: return False
                return abs(r["close"]-lvl)<=ct or (r["low"]<=lvl+ct and r["high"]>=lvl-ct)

            cw = 0.0
            if near(f236): cw+=1.0
            if near(f382): cw+=1.5
            if near(f500): cw+=2.0
            if near(f618): cw+=2.5
            if near(f786): cw+=1.5
            if sw_h1 and near(sw_h1): cw+=1.0
            if sw_l1 and near(sw_l1): cw+=1.0
            if self.sweep_conf and (sph or spl): cw+=2.0
            score = min(cw*10.0, 100.0)

            # Nearest fib label
            fib_map = {"0.236":f236,"0.382":f382,"0.500":f500,
                       "0.618":f618,"0.786":f786,"-0.5":ftgt50,"-0.618":ftgt}
            near_fib = min(
                ((k,abs(r["close"]-v)) for k,v in fib_map.items() if v is not None),
                key=lambda x: x[1], default=(None, None)
            )[0]

            # ── ENGULFING ────────────────────────────────────────
            bec = uec = False
            if i > 0 and ok:
                pv    = df.iloc[i-1]
                body  = abs(r["close"]-r["open"])
                bavg  = r["body_ema"]
                pbody = abs(pv["close"]-pv["open"])
                pbavg = pv["body_ema"]
                lb    = body > bavg
                sp    = pbody < pbavg
                bg    = body > pbody

                if self.strict:
                    be = r["close"]<r["open"] and lb and bg and pv["close"]>pv["open"] and sp and r["open"]>pv["close"] and r["close"]<pv["open"]
                    ue = r["close"]>r["open"] and lb and bg and pv["close"]<pv["open"] and sp and r["open"]<pv["close"] and r["close"]>pv["open"]
                else:
                    be = r["close"]<r["open"] and lb and bg and pv["close"]>pv["open"] and sp and r["close"]<=pv["open"] and r["open"]>=pv["close"] and (r["close"]<pv["open"] or r["open"]>pv["close"])
                    ue = r["close"]>r["open"] and lb and bg and pv["close"]<pv["open"] and sp and r["close"]>=pv["open"] and r["open"]<=pv["close"] and (r["close"]>pv["open"] or r["open"]<pv["close"])

                bec = be and (prem or cw>=1.5)
                uec = ue and (disc or cw>=1.5)

            # ── VOLUME CONFIRM ───────────────────────────────────
            vol_ok = True
            if self.vol_confirm:
                vol_ok = r["volume"] >= r["vol_ema"] if not np.isnan(r["vol_ema"]) else True

            # ── SIGNALS ──────────────────────────────────────────
            sw_buy  = spl and ok and (disc or cw>=2.0)
            sw_sell = sph and ok and (prem or cw>=2.0)

            b_eng = uec and bias==1  and cw>=1.5
            b_cho = is_choch and bull
            b_swp = sw_buy
            s_eng = bec and bias==-1 and cw>=1.5
            s_cho = is_choch and bear
            s_swp = sw_sell

            buy_r  = (b_eng or b_cho or b_swp) and cw>=self.min_conf
            sell_r = (s_eng or s_cho or s_swp) and cw>=self.min_conf

            if buy_r and sell_r: buy_r = sell_r = False

            cbuy  = buy_r  and bars_cd>=self.cooldown and ok and vol_ok
            csell = sell_r and bars_cd>=self.cooldown and ok and vol_ok

            if cbuy or csell:
                bars_cd = 0;  last_sig = i
                parts = []
                if cbuy:
                    if b_cho: parts.append("choch")
                    if b_swp: parts.append("sweep")
                    if b_eng: parts.append("engulf")
                    final_sig = Signal(
                        action="BUY", trigger="+".join(parts),
                        confluence_score=score, fib_618=f618,
                        fib_target=ftgt, fib_tgt50=ftgt50,
                        fib_direction=fdir, structure_bias=bias,
                        atr=atr, close=float(r["close"]),
                        sw_high1=sw_h1, sw_low1=sw_l1,
                        volume=float(r["volume"]), near_fib=near_fib,
                        in_premium=prem, in_discount=disc,
                    )
                else:
                    if s_cho: parts.append("choch")
                    if s_swp: parts.append("sweep")
                    if s_eng: parts.append("engulf")
                    final_sig = Signal(
                        action="SELL", trigger="+".join(parts),
                        confluence_score=score, fib_618=f618,
                        fib_target=ftgt, fib_tgt50=ftgt50,
                        fib_direction=fdir, structure_bias=bias,
                        atr=atr, close=float(r["close"]),
                        sw_high1=sw_h1, sw_low1=sw_l1,
                        volume=float(r["volume"]), near_fib=near_fib,
                        in_premium=prem, in_discount=disc,
                    )
        # end loop
        return final_sig if last_sig == n - 1 else None
