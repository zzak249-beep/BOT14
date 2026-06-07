"""
GUA-USDT Bot v2 — Motor de Estrategia Moderno

Técnicas integradas:
  • SMC/ICT : FVG · Order Blocks · Liquidity Sweeps · BOS/CHoCH
  • Momentum : TTM Squeeze · MACD histogram · CVD divergencia
  • Volumen  : RVOL · CVD acumulado
  • VWAP     : precio vs VWAP y bandas de desviación
  • Derivados: Funding Rate extremo · OI Delta
  • Régimen  : ATR percentil (alta/baja volatilidad)
  • MTF      : 3m entrada · 15m sesgo · 1h estructura macro
  • Sesiones : London + NY únicamente
"""

from __future__ import annotations
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np

import config
import indicators as ind

log = logging.getLogger("strategy")


# ── Dataclasses ────────────────────────────────────────────────────────────────

@dataclass
class Signal:
    direction: str        # "LONG" | "SHORT"
    score:     float      # 0.0 – 1.0
    price:     float
    atr:       float
    atr_pct:   float      # percentil ATR 0-100
    sl:        float
    tp1:       float
    tp2:       float
    rsi:       float
    adx:       float
    funding:   float
    squeeze:   bool       # ¿compresión activa?
    rvol:      float
    reason:    str
    # SMC extras
    fvg_hit:   bool = False
    ob_hit:    bool = False
    liq_sweep: bool = False
    bos:       str  = "NONE"
    choch:     str  = "NONE"


@dataclass
class StrategyState:
    last_candle_time: int = 0
    oi_history: List[float] = field(default_factory=list)


_state = StrategyState()


# ── Función principal ──────────────────────────────────────────────────────────

def analyze(
    candles:       List[Dict],
    candles_trend: List[Dict],
    candles_macro: List[Dict],
    funding_rate:  float = 0.0,
    open_interest: float = 0.0,
) -> Optional[Signal]:

    if len(candles) < 80:
        log.warning("Pocas velas: %d", len(candles))
        return None

    # ── Antiduplicado ──────────────────────────────────────────────────────────
    last_time = candles[-1]["time"]
    if last_time == _state.last_candle_time:
        return None
    _state.last_candle_time = last_time

    # ── Filtro de sesión ───────────────────────────────────────────────────────
    if config.SESSION_FILTER and not _in_session():
        log.info("Fuera de sesión London/NY — skip")
        return None

    # ── Arrays 3m ─────────────────────────────────────────────────────────────
    opens   = [c["open"]   for c in candles]
    highs   = [c["high"]   for c in candles]
    lows    = [c["low"]    for c in candles]
    closes  = [c["close"]  for c in candles]
    volumes = [c["volume"] for c in candles]

    # ── Indicadores clásicos ───────────────────────────────────────────────────
    ema9   = ind.ema(closes, config.EMA_FAST)
    ema21  = ind.ema(closes, config.EMA_SLOW)
    ema50  = ind.ema(closes, config.EMA_TREND)
    ema200 = ind.ema(closes, config.EMA_MACRO)
    rsi14  = ind.rsi(closes, config.RSI_PERIOD)
    atr14  = ind.atr(highs, lows, closes, 14)
    adx14, di_p, di_m = ind.adx(highs, lows, closes, config.ADX_PERIOD)
    cvd20  = ind.cvd(opens, closes, volumes, config.CVD_LB)
    ml, sl_line, hist = ind.macd(closes)

    # Valores en [-2] (última vela completa)
    i = -2
    price   = closes[i]
    e9      = ema9[i];  e21 = ema21[i]; e50 = ema50[i]; e200 = ema200[i]
    rsi_v   = rsi14[i]
    atr_v   = atr14[i]
    adx_v   = adx14[i]
    cvd_v   = cvd20[i]
    macd_h  = hist[i]
    macd_h_prev = hist[i-1]

    # ── ATR Percentil (régimen volatilidad) ────────────────────────────────────
    atr_pct = ind.atr_percentile(atr14, config.ATR_PERCENTILE_LB)
    high_vol = atr_pct >= 75   # régimen de alta volatilidad
    low_vol  = atr_pct <= 25   # régimen de baja volatilidad → evitar entradas

    # ── TTM Squeeze ────────────────────────────────────────────────────────────
    sqz_arr, mom_arr = ind.squeeze_momentum(
        highs, lows, closes,
        config.BB_PERIOD, config.BB_MULT,
        config.KC_PERIOD, config.KC_MULT,
        config.MOM_PERIOD,
    )
    in_squeeze     = bool(sqz_arr[i])
    prev_squeeze   = bool(sqz_arr[i-1])
    squeeze_release= (prev_squeeze and not in_squeeze)   # acaba de liberar
    mom_v          = mom_arr[i]
    mom_prev       = mom_arr[i-1]
    mom_bearish    = squeeze_release and mom_v < 0 and mom_v < mom_prev
    mom_bullish    = squeeze_release and mom_v > 0 and mom_v > mom_prev

    # ── RVOL ──────────────────────────────────────────────────────────────────
    rvol_arr = ind.rvol(volumes, config.RVOL_PERIOD)
    rvol_v   = float(rvol_arr[i])

    # ── VWAP ──────────────────────────────────────────────────────────────────
    vwap_arr, vwap_up, vwap_dn = ind.vwap_bands(
        highs, lows, closes, volumes,
        config.VWAP_PERIOD, config.VWAP_BAND_MULT,
    )
    vwap_v    = vwap_arr[i]
    above_vwap= price > vwap_v
    below_vwap= price < vwap_v
    near_vwap = abs(price - vwap_v) / max(vwap_v, 0.000001) < 0.005  # ± 0.5%
    extended_up   = price > vwap_up[i]
    extended_down = price < vwap_dn[i]

    # ── CVD Divergencia ────────────────────────────────────────────────────────
    cvd_bear_div, cvd_bull_div = ind.cvd_divergence(closes, cvd20, config.CVD_DIV_LB)

    # ── FVG ───────────────────────────────────────────────────────────────────
    bear_fvg, bull_fvg = ind.detect_fvg(
        highs, lows, closes, config.FVG_LOOKBACK, config.FVG_MIN_SIZE
    )
    price_in_bear_fvg = (
        bear_fvg is not None and
        bear_fvg["bottom"] <= price <= bear_fvg["top"]
    )
    price_below_bear_fvg = (
        bear_fvg is not None and price < bear_fvg["bottom"]
    )
    price_in_bull_fvg = (
        bull_fvg is not None and
        bull_fvg["bottom"] <= price <= bull_fvg["top"]
    )

    # ── Order Blocks ──────────────────────────────────────────────────────────
    bear_ob, bull_ob = ind.detect_order_blocks(
        opens, highs, lows, closes, config.OB_LOOKBACK, config.OB_IMPULSE_BARS
    )
    price_in_bear_ob = (
        bear_ob is not None and
        bear_ob["low"] <= price <= bear_ob["high"]
    )
    price_in_bull_ob = (
        bull_ob is not None and
        bull_ob["low"] <= price <= bull_ob["high"]
    )

    # ── Liquidity Sweep ───────────────────────────────────────────────────────
    swept_highs, swept_lows = ind.detect_liquidity_sweep(
        highs, lows, closes, opens, config.LIQ_LOOKBACK, config.LIQ_TOLERANCE
    )

    # ── Market Structure ──────────────────────────────────────────────────────
    ms = ind.market_structure(highs, lows, closes)

    # ── OI Delta ─────────────────────────────────────────────────────────────
    _state.oi_history.append(open_interest)
    if len(_state.oi_history) > config.OI_HISTORY_LEN:
        _state.oi_history.pop(0)
    oi_delta = _oi_delta()

    # ── Tendencia 15m ─────────────────────────────────────────────────────────
    trend_bias = "NEUTRAL"
    if len(candles_trend) >= 55:
        tc   = [c["close"] for c in candles_trend]
        te9  = ind.ema(tc, config.EMA_FAST)[-1]
        te21 = ind.ema(tc, config.EMA_SLOW)[-1]
        te50 = ind.ema(tc, config.EMA_TREND)[-1]
        trend_bias = (
            "DOWN" if te9 < te21 and te21 < te50 else
            "UP"   if te9 > te21 and te21 > te50 else
            "NEUTRAL"
        )

    # ── Estructura macro 1h ───────────────────────────────────────────────────
    macro_bias = "NEUTRAL"
    if len(candles_macro) >= 55:
        mc   = [c["close"] for c in candles_macro]
        me50 = ind.ema(mc, config.EMA_TREND)[-1]
        me200= ind.ema(mc, config.EMA_MACRO)[-1]
        macro_bias = (
            "DOWN" if mc[-1] < me50 < me200 else
            "UP"   if mc[-1] > me50 > me200 else
            "NEUTRAL"
        )

    # ── Log contexto ─────────────────────────────────────────────────────────
    log.info(
        "price=%.5f rsi=%.1f adx=%.1f atrPct=%.0f sqz=%s "
        "rvol=%.2fx cvdBearDiv=%s liqSweepH=%s bias15m=%s macro=%s",
        price, rsi_v, adx_v, atr_pct, in_squeeze,
        rvol_v, cvd_bear_div, swept_highs, trend_bias, macro_bias,
    )

    # ── Evitar entrar en volatilidad extremadamente baja ──────────────────────
    if low_vol and adx_v < config.ADX_MIN:
        log.info("Baja volatilidad + ADX bajo — skip")
        return None

    # ── Construir señal ───────────────────────────────────────────────────────
    short_score, short_parts = _score_short(
        price, e9, e21, e50, e200,
        rsi_v, adx_v, atr_pct,
        mom_bearish, in_squeeze,
        rvol_v, above_vwap, extended_up,
        cvd_bear_div,
        swept_highs, price_in_bear_fvg, price_in_bear_ob,
        ms, oi_delta, funding_rate,
        trend_bias, macro_bias,
    )

    long_score, long_parts = _score_long(
        price, e9, e21, e50, e200,
        rsi_v, adx_v, atr_pct,
        mom_bullish, in_squeeze,
        rvol_v, below_vwap, extended_down,
        cvd_bull_div,
        swept_lows, price_in_bull_fvg, price_in_bull_ob,
        ms, oi_delta, funding_rate,
        trend_bias, macro_bias,
    )

    direction = None
    score     = 0.0
    parts     = ""
    if short_score > long_score and short_score >= config.SCORE_THR:
        direction, score, parts = "SHORT", short_score, short_parts
    elif long_score >= config.SCORE_THR:
        direction, score, parts = "LONG", long_score, long_parts

    if direction is None:
        return None

    # ── SL/TP dinámico según régimen ATR ──────────────────────────────────────
    sl_mult = config.ATR_HIGHVOL_MULT if high_vol else config.ATR_SL_MULT
    if direction == "SHORT":
        sl  = price + atr_v * sl_mult
        tp1 = price - atr_v * config.ATR_TP1_MULT
        tp2 = price - atr_v * config.ATR_TP2_MULT
    else:
        sl  = price - atr_v * sl_mult
        tp1 = price + atr_v * config.ATR_TP1_MULT
        tp2 = price + atr_v * config.ATR_TP2_MULT

    return Signal(
        direction  = direction,
        score      = round(score, 3),
        price      = round(price, 6),
        atr        = round(atr_v, 6),
        atr_pct    = round(atr_pct, 1),
        sl         = round(sl,  6),
        tp1        = round(tp1, 6),
        tp2        = round(tp2, 6),
        rsi        = round(rsi_v, 1),
        adx        = round(adx_v, 1),
        funding    = round(funding_rate, 6),
        squeeze    = in_squeeze,
        rvol       = round(rvol_v, 2),
        reason     = parts,
        fvg_hit    = price_in_bear_fvg if direction == "SHORT" else price_in_bull_fvg,
        ob_hit     = price_in_bear_ob  if direction == "SHORT" else price_in_bull_ob,
        liq_sweep  = swept_highs       if direction == "SHORT" else swept_lows,
        bos        = ms["bos"],
        choch      = ms["choch"],
    )


# ══════════════════════════════════════════════════════════════════════
#  SCORERS
# ══════════════════════════════════════════════════════════════════════

def _score_short(
    price, e9, e21, e50, e200,
    rsi, adx, atr_pct,
    mom_bearish, in_squeeze,
    rvol, above_vwap, extended_up,
    cvd_div,
    swept_highs, in_bear_fvg, in_bear_ob,
    ms, oi_delta, funding,
    trend_15m, macro_1h,
) -> Tuple[float, str]:

    parts  = []
    score  = 0.0
    MAX    = 1.0

    # ── [REQUERIDO] EMA estructura bajista ────────────────────────────────────
    if not (e9 < e21):
        return 0.0, ""   # mínimo absoluto

    score += 0.18; parts.append(f"EMA9<EMA21 ✅")
    if e21 < e50:
        score += 0.07; parts.append("EMA21<EMA50 ✅")
    if price < e200:
        score += 0.05; parts.append("Bajo EMA200 macro ✅")

    # ── RSI zona de carga ─────────────────────────────────────────────────────
    rsi_ok = (config.RSI_OS + 15) <= rsi <= config.RSI_OB
    if rsi_ok:
        score += 0.12; parts.append(f"RSI={rsi:.0f} zona cargada ✅")
    elif rsi > config.RSI_OB:
        score += 0.07; parts.append(f"RSI={rsi:.0f} sobrecompra ⚠️")

    # ── SMC: Liquidity Sweep de equal highs ───────────────────────────────────
    if swept_highs:
        score += 0.14; parts.append("🎣 Barrido liquidez equal highs ✅")

    # ── SMC: FVG bajista overhead ─────────────────────────────────────────────
    if in_bear_fvg:
        score += 0.10; parts.append("📦 En FVG bajista ✅")

    # ── SMC: Order Block bajista ───────────────────────────────────────────────
    if in_bear_ob:
        score += 0.08; parts.append("🧱 En OB bajista ✅")

    # ── SMC: BOS/CHoCH bajista ────────────────────────────────────────────────
    if ms["bos"] == "BEAR":
        score += 0.07; parts.append("⚡ BOS bajista ✅")
    elif ms["choch"] == "BEAR":
        score += 0.05; parts.append("🔄 CHoCH bajista ✅")

    # ── Squeeze liberando bajista ─────────────────────────────────────────────
    if mom_bearish:
        score += 0.10; parts.append("💥 Squeeze liberando bajista ✅")
    elif in_squeeze:
        score += 0.03; parts.append("🌀 Squeeze activo ⏳")

    # ── CVD divergencia bajista ───────────────────────────────────────────────
    if cvd_div:
        score += 0.08; parts.append("📊 CVD divergencia bajista ✅")

    # ── VWAP: precio extendido por encima ─────────────────────────────────────
    if extended_up:
        score += 0.06; parts.append("📈 Precio sobre banda VWAP sup ✅")
    elif above_vwap:
        score += 0.02; parts.append("Sobre VWAP ⚠️")

    # ── RVOL confirmación ─────────────────────────────────────────────────────
    if rvol >= config.RVOL_MIN:
        score += 0.05; parts.append(f"📣 RVOL={rvol:.1f}x ✅")

    # ── ADX tendencia activa ──────────────────────────────────────────────────
    if adx >= config.ADX_MIN:
        score += 0.05; parts.append(f"ADX={adx:.1f} ✅")
    else:
        score -= 0.05; parts.append(f"ADX={adx:.1f} bajo ❌")

    # ── OI Delta: dinero nuevo entrando ──────────────────────────────────────
    if oi_delta > 0:
        score += 0.04; parts.append(f"OI↑ dinero nuevo ✅")

    # ── Funding extremo (longs pagando fuerte) ────────────────────────────────
    if funding >= config.FUNDING_EXTREME_LONG:
        score += 0.05; parts.append(f"💰 Funding extremo +{funding:.4%} ✅")
    elif funding > 0:
        score += 0.02; parts.append(f"Funding positivo ✅")

    # ── Bias 15m ──────────────────────────────────────────────────────────────
    if trend_15m == "DOWN":
        score += 0.08; parts.append("📉 Bias 15m bajista ✅")
    elif trend_15m == "UP":
        score -= 0.12; parts.append("📈 Bias 15m alcista PENALIZA ❌")

    # ── Macro 1h ──────────────────────────────────────────────────────────────
    if macro_1h == "DOWN":
        score += 0.06; parts.append("🏔 Macro 1h bajista ✅")
    elif macro_1h == "UP":
        score -= 0.08; parts.append("🏔 Macro 1h alcista PENALIZA ❌")

    # ── Régimen ATR: alta volatilidad es buena para este setup ───────────────
    if 50 <= atr_pct <= 85:
        score += 0.03; parts.append(f"ATR pct={atr_pct:.0f} óptimo ✅")
    elif atr_pct > 90:
        score -= 0.05; parts.append(f"ATR pct={atr_pct:.0f} extremo, riesgo ⚠️")

    return min(max(score, 0.0), MAX), " | ".join(parts)


def _score_long(
    price, e9, e21, e50, e200,
    rsi, adx, atr_pct,
    mom_bullish, in_squeeze,
    rvol, below_vwap, extended_down,
    cvd_div,
    swept_lows, in_bull_fvg, in_bull_ob,
    ms, oi_delta, funding,
    trend_15m, macro_1h,
) -> Tuple[float, str]:

    parts = []
    score = 0.0

    # ── [REQUERIDO] RSI sobreventa ─────────────────────────────────────────────
    if rsi > config.RSI_OS:
        return 0.0, ""

    score += 0.20; parts.append(f"RSI={rsi:.0f} sobreventa ✅")

    # ── SMC: Liquidity sweep de equal lows ────────────────────────────────────
    if swept_lows:
        score += 0.16; parts.append("🎣 Barrido liquidez equal lows ✅")

    # ── SMC: FVG alcista abajo ────────────────────────────────────────────────
    if in_bull_fvg:
        score += 0.10; parts.append("📦 En FVG alcista ✅")

    # ── SMC: Order Block alcista ──────────────────────────────────────────────
    if in_bull_ob:
        score += 0.08; parts.append("🧱 En OB alcista ✅")

    # ── SMC: CHoCH alcista (cambio de carácter) ───────────────────────────────
    if ms["choch"] == "BULL":
        score += 0.08; parts.append("🔄 CHoCH alcista ✅")
    elif ms["bos"] == "BULL":
        score += 0.05; parts.append("⚡ BOS alcista ✅")

    # ── Squeeze liberando alcista ─────────────────────────────────────────────
    if mom_bullish:
        score += 0.10; parts.append("💥 Squeeze liberando alcista ✅")

    # ── CVD divergencia alcista ───────────────────────────────────────────────
    if cvd_div:
        score += 0.08; parts.append("📊 CVD divergencia alcista ✅")

    # ── VWAP: precio extendido por debajo ─────────────────────────────────────
    if extended_down:
        score += 0.06; parts.append("📉 Precio bajo banda VWAP inf ✅")

    # ── RVOL confirmación ─────────────────────────────────────────────────────
    if rvol >= config.RVOL_MIN:
        score += 0.05; parts.append(f"📣 RVOL={rvol:.1f}x ✅")

    # ── OI Delta: caída (short covering) ─────────────────────────────────────
    if oi_delta < 0:
        score += 0.04; parts.append("OI↓ short covering posible ✅")

    # ── Funding extremo negativo ──────────────────────────────────────────────
    if funding <= config.FUNDING_EXTREME_SHORT:
        score += 0.07; parts.append(f"💰 Funding extremo {funding:.4%} ✅")

    # ── ADX: tendencia débil (counter-trend funciona mejor) ───────────────────
    if adx < 25:
        score += 0.05; parts.append(f"ADX={adx:.1f} bajo, counter-trend ✅")
    elif adx > 35:
        score -= 0.08; parts.append(f"ADX={adx:.1f} tendencia fuerte PENALIZA ❌")

    # ── Bias 15m ──────────────────────────────────────────────────────────────
    if trend_15m == "UP":
        score += 0.07; parts.append("📈 Bias 15m alcista ✅")
    elif trend_15m == "DOWN":
        score -= 0.12; parts.append("📉 Bias 15m bajista PENALIZA ❌")

    # ── Macro 1h ──────────────────────────────────────────────────────────────
    if macro_1h == "DOWN":
        score -= 0.10; parts.append("🏔 Macro 1h bajista PENALIZA ❌")

    return min(max(score, 0.0), 1.0), " | ".join(parts)


# ══════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════

def _in_session() -> bool:
    """True si estamos en London Open (7-12 UTC) o NY Open (13-18 UTC)."""
    hour = datetime.now(timezone.utc).hour
    return any(start <= hour < end for start, end in config.SESSION_HOURS)


def _oi_delta() -> float:
    """Delta del OI: diferencia entre el último y el primer valor del historial."""
    h = _state.oi_history
    if len(h) < 2:
        return 0.0
    return h[-1] - h[0]
