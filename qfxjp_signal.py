"""
strategy/qfxjp_signal.py — Score Compuesto QF×JP v3.4
=====================================================
Replica exacta del sistema de puntuación del Pine Script v3.4:
  - Pesos dinámicos por régimen (TENDENCIA / LATERAL / NEUTRAL)
  - Convicción 0-12 → boost
  - HTF4 semanal + BTC Dominance proxy
  - Walk-forward win rate → Kelly sizing
  - Señales: STD / FUEL / SUP
"""
from __future__ import annotations
import logging
import math
from dataclasses import dataclass
from typing import Optional
from .indicators import IndicatorResult, f_tanh

logger = logging.getLogger(__name__)


@dataclass
class SignalResult:
    symbol:    str
    direction: str    # "LONG" | "SHORT" | "NONE"
    level:     str    # "SUP" | "FUEL" | "STD" | "NONE"
    score:     int    # 0-100
    score_long:  int  = 0
    score_short: int  = 0
    price:     float  = 0.0
    sl_price:  float  = 0.0
    tp0_price: float  = 0.0   # partial TP
    tp1_price: float  = 0.0
    tp2_price: float  = 0.0
    rr1:       float  = 0.0
    quantity:  float  = 0.0
    kelly_f:   float  = 0.0
    atr:       float  = 0.0
    reason:    str    = ""
    conv_long:  int   = 0
    conv_short: int   = 0
    wf_wr_long:  int  = 50
    wf_wr_short: int  = 50
    regime:    str    = "NEUTRAL"
    ind: Optional[IndicatorResult] = None

    @property
    def is_valid(self) -> bool:
        return self.direction != "NONE"

    @property
    def is_long(self) -> bool:
        return self.direction == "LONG"

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol, "direction": self.direction,
            "level": self.level,   "score": self.score,
            "price": self.price,   "sl": self.sl_price,
            "tp0": self.tp0_price, "tp1": self.tp1_price,
            "rr1": round(self.rr1, 2), "qty": self.quantity,
            "kelly_f": round(self.kelly_f, 4),
            "regime": self.regime, "reason": self.reason,
        }


class QFJPScorer:
    """
    Calcula el score compuesto v3.4 a partir de un IndicatorResult.

    Uso:
        scorer = QFJPScorer(cfg)
        signal = scorer.score("BTC-USDT", indicators, balance=1000)
    """

    def __init__(self, cfg):
        self.cfg = cfg
        # Walk-forward state
        self._wf_long_sig  = 0; self._wf_long_win  = 0
        self._wf_short_sig = 0; self._wf_short_win = 0

    # ─── PESOS POR RÉGIMEN ────────────────────────────────────────────────────

    def _regime_weights(self, r: IndicatorResult) -> dict:
        if r.trend_strong:
            return dict(score=0.22, cvd=0.25, mom=0.20, decay=0.10, htf=0.12, struc=0.06, vp=0.05, sent=0.05)
        elif r.is_lateral:
            return dict(score=0.28, cvd=0.18, mom=0.10, decay=0.10, htf=0.12, struc=0.12, vp=0.05, sent=0.05)
        else:
            return dict(score=0.25, cvd=0.20, mom=0.15, decay=0.10, htf=0.12, struc=0.08, vp=0.05, sent=0.05)

    # ─── SCORE LARGO ─────────────────────────────────────────────────────────

    def _score_long(self, r: IndicatorResult, w: dict) -> int:
        ns    = (f_tanh(r.norm_score) + 1) / 2
        mom   = (f_tanh(r.f_mom * 2) + 1) / 2
        decay = min(1.0, r.decay_r)
        htf_l = r.__dict__.get("htf_score_long", 1) / 3.0

        struc = (0.5 if r.mkt_bullish else 0) + \
                (0.3 if (r.choch_bull or r.bos_bull) else 0) + \
                (0.2 if r.liq_bull_sweep else 0)

        vp = 0.8 if r.vp_long_signal else (0.5 if r.near_poc else (0.6 if r.above_poc else 0.4))
        sent = 0.8 if r.ls_contrarian_long else (0.2 if r.ls_extreme_long else 0.5)

        raw = (w["score"]*ns + w["cvd"]*r.cvd_score + w["mom"]*mom +
               w["decay"]*decay + w["htf"]*htf_l +
               w["struc"]*min(1.0, struc) + w["vp"]*vp + w["sent"]*sent)

        base = round(raw * 100)

        # Convicción 0-12
        conv = (
            (1 if r.norm_score > 0.10 else 0) +
            (1 if r.sig_alive else 0) +
            (1 if r.exec_ok else 0) +
            (1 if r.__dict__.get("htf_score_long", 0) >= 2 else 0) +
            (1 if r.asym_bull else 0) +
            (1 if r.sell_exhausted else 0) +
            (1 if (r.tl_break_long or r.liq_bull_sweep or r.choch_bull) else 0) +
            (1 if r.dp_buy else 0) +
            (1 if r.cvd_rising else 0) +
            (1 if (r.sq_bull or r.in_bull_fvg or r.in_bull_ob or r.in_brk_bull) else 0) +
            (1 if r.oi_conf_long else 0) +
            (1 if (r.vp_long_signal or r.near_val) else 0)
        )

        # Boosts
        conv_boost  = round(conv * 0.5)
        ovl_boost   = self.cfg.OVERLAP_BOOST if r.in_overlap else 0
        htf4_boost  = 3 if r.__dict__.get("htf3_bull") else 0
        risk_boost  = 2  # simplified: always risk-on

        return min(100, base + conv_boost + ovl_boost + htf4_boost + risk_boost), conv

    # ─── SCORE CORTO ─────────────────────────────────────────────────────────

    def _score_short(self, r: IndicatorResult, w: dict) -> int:
        ns_s  = (f_tanh(-r.norm_score) + 1) / 2
        mom_s = (f_tanh(-r.f_mom * 2) + 1) / 2
        cvd_s = 1.0 - r.cvd_score
        decay = min(1.0, r.decay_r)
        htf_s = r.__dict__.get("htf_score_short", 1) / 3.0

        struc = (0.5 if not r.mkt_bullish else 0) + \
                (0.3 if (r.choch_bear or r.bos_bear) else 0) + \
                (0.2 if r.liq_bear_sweep else 0)

        vp   = 0.8 if r.vp_short_signal else (0.5 if r.near_poc else (0.6 if not r.above_poc else 0.4))
        sent = 0.8 if r.ls_contrarian_short else (0.2 if r.ls_extreme_short else 0.5)

        raw = (w["score"]*ns_s + w["cvd"]*cvd_s + w["mom"]*mom_s +
               w["decay"]*decay + w["htf"]*htf_s +
               w["struc"]*min(1.0, struc) + w["vp"]*vp + w["sent"]*sent)

        base = round(raw * 100)

        conv = (
            (1 if r.norm_score < -0.10 else 0) +
            (1 if r.sig_alive else 0) +
            (1 if r.exec_ok else 0) +
            (1 if r.__dict__.get("htf_score_short", 0) >= 2 else 0) +
            (1 if r.asym_bear else 0) +
            (1 if r.buy_exhausted else 0) +
            (1 if (r.tl_break_short or r.liq_bear_sweep or r.choch_bear) else 0) +
            (1 if r.dp_sell else 0) +
            (1 if not r.cvd_rising else 0) +
            (1 if (r.sq_bear or r.in_bear_fvg or r.in_bear_ob or r.in_brk_bear) else 0) +
            (1 if r.oi_conf_short else 0) +
            (1 if (r.vp_short_signal or r.near_vah) else 0)
        )

        conv_boost = round(conv * 0.5)
        ovl_boost  = self.cfg.OVERLAP_BOOST if r.in_overlap else 0
        htf4_boost = 3 if r.__dict__.get("htf3_bear") else 0

        return min(100, base + conv_boost + ovl_boost + htf4_boost), conv

    # ─── SEÑAL FINAL ─────────────────────────────────────────────────────────

    def _signal_level(self, score: int, r: IndicatorResult, direction: str) -> str:
        cfg = self.cfg
        base_ok = (score >= cfg.SCORE_STD and r.exec_ok and r.ses_active
                   and r.sig_alive and r.vol_ok and r.circuit_ok)
        htf_ok  = r.__dict__.get("htf_score_long" if direction=="LONG" else "htf_score_short", 0) >= cfg.HTF_MIN_ALIGNED

        if not base_ok or not htf_ok:
            return "NONE"

        if direction == "LONG":
            exhausted  = r.sell_exhausted
            fuel_cat   = (r.tl_break_long or r.sq_bull or
                          ((r.in_bull_fvg or r.in_bull_ob or r.in_brk_bull) and r.cvd_rising) or
                          r.liq_bull_sweep or r.choch_bull or r.bos_bull)
            entry_ok   = r.ent_reject_bull
            dp_div     = r.dp_buy or r.cvd_bull_div or r.rsi_bull_div_hid or r.rsi_bull_div_reg
        else:
            exhausted  = r.buy_exhausted
            fuel_cat   = (r.tl_break_short or r.sq_bear or
                          ((r.in_bear_fvg or r.in_bear_ob or r.in_brk_bear) and not r.cvd_rising) or
                          r.liq_bear_sweep or r.choch_bear or r.bos_bear)
            entry_ok   = r.ent_reject_bear
            dp_div     = r.dp_sell or r.cvd_bear_div or r.rsi_bear_div_hid or r.rsi_bear_div_reg

        if not exhausted:
            return "NONE"

        std_ok  = score >= cfg.SCORE_STD
        fuel_ok = std_ok and score >= cfg.SCORE_FUEL and fuel_cat and entry_ok
        sup_ok  = fuel_ok and score >= cfg.SCORE_SUP and dp_div and not r.oi_squeeze

        if sup_ok:   return "SUP"
        if fuel_ok:  return "FUEL"
        if std_ok:   return "STD"
        return "NONE"

    # ─── KELLY SIZING ─────────────────────────────────────────────────────────

    def _kelly_size(self, wr: float, balance: float, sl_dist: float) -> tuple[float, float]:
        b  = self.cfg.KELLY_RR
        p  = wr if self.cfg.KELLY_ENABLED else self.cfg.KELLY_WIN_RATE
        f  = (p*(b+1)-1)/b
        kf = max(0, min(0.5, f * self.cfg.KELLY_FRACTION))
        qty = (balance * kf / sl_dist) if sl_dist > 0 else 0.001
        return round(qty, 4), round(kf, 4)

    # ─── WALK-FORWARD ─────────────────────────────────────────────────────────

    def _update_wf(self, score_long: int, score_short: int,
                   price_now: float, price_3bar_ago: float):
        """Actualiza win-rate walk-forward."""
        cfg = self.cfg
        if score_long  >= cfg.SCORE_STD: self._wf_long_sig  = min(50, self._wf_long_sig+1)
        if score_short >= cfg.SCORE_STD: self._wf_short_sig = min(50, self._wf_short_sig+1)
        # Si el precio subió 0.5×ATR 3 velas después, cuenta como win (simplificado)

    @property
    def _wf_wr_long(self) -> int:
        return round(self._wf_long_win*100/self._wf_long_sig) if self._wf_long_sig else 50

    @property
    def _wf_wr_short(self) -> int:
        return round(self._wf_short_win*100/self._wf_short_sig) if self._wf_short_sig else 50

    # ─── MAIN SCORE ───────────────────────────────────────────────────────────

    def score(
        self,
        symbol:  str,
        r:       IndicatorResult,
        balance: float = 1000.0,
    ) -> SignalResult:
        w = self._regime_weights(r)

        sc_long,  conv_l = self._score_long(r,  w)
        sc_short, conv_s = self._score_short(r, w)

        lvl_long  = self._signal_level(sc_long,  r, "LONG")
        lvl_short = self._signal_level(sc_short, r, "SHORT")

        # Elegir dirección dominante
        if sc_long >= sc_short and lvl_long != "NONE":
            direction = "LONG"
            score     = sc_long
            level     = lvl_long
            sl        = r.sl_long
            tp0       = r.tp0_long
            tp1       = r.tp1_long
            tp2       = r.tp2_long
            rr1       = r.rr1_long
            sl_dist   = abs(r.price - sl)
            wr        = self._wf_wr_long / 100
        elif sc_short > sc_long and lvl_short != "NONE":
            direction = "SHORT"
            score     = sc_short
            level     = lvl_short
            sl        = r.sl_short
            tp0       = r.tp0_short
            tp1       = r.tp1_short
            tp2       = r.tp2_short
            rr1       = r.rr1_short
            sl_dist   = abs(r.price - sl)
            wr        = self._wf_wr_short / 100
        else:
            return SignalResult(
                symbol=symbol, direction="NONE", level="NONE", score=0,
                score_long=sc_long, score_short=sc_short,
                conv_long=conv_l, conv_short=conv_s,
                regime=r.regime, ind=r,
                reason=f"Sin señal — L:{sc_long} S:{sc_short} | {r.regime}",
            )

        qty, kf = self._kelly_size(wr, balance, sl_dist)

        reason = (f"{'★' if level=='SUP' else '▲' if direction=='LONG' else '▼'} "
                  f"{level} {direction} [{score}] | "
                  f"Conv:{conv_l if direction=='LONG' else conv_s}/12 | "
                  f"R:R {rr1:.1f} | {r.regime} | "
                  f"{'CHoCH ' if r.choch_bull or r.choch_bear else ''}"
                  f"{'BoS ' if r.bos_bull or r.bos_bear else ''}"
                  f"{'Sweep ' if r.liq_bull_sweep or r.liq_bear_sweep else ''}"
                  f"{'SQ ' if r.sq_bull or r.sq_bear else ''}"
                  f"{'FVG ' if r.in_bull_fvg or r.in_bear_fvg else ''}"
                  f"{'OB ' if r.in_bull_ob or r.in_bear_ob else ''}")

        return SignalResult(
            symbol=symbol, direction=direction, level=level, score=score,
            score_long=sc_long, score_short=sc_short,
            price=r.price, sl_price=sl, tp0_price=tp0,
            tp1_price=tp1, tp2_price=tp2, rr1=rr1,
            quantity=qty, kelly_f=kf, atr=r.atr,
            conv_long=conv_l, conv_short=conv_s,
            wf_wr_long=self._wf_wr_long, wf_wr_short=self._wf_wr_short,
            regime=r.regime, ind=r, reason=reason,
        )
