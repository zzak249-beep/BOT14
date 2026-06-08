"""
GUA-USDT Bot v3 — Motor de Estrategia

FIXES v3:
  • EMA200 válida (LOOKBACK=260 en config)
  • RSI_OS=42 en lugar de 37 (antes casi nunca disparaba LONG)
  • MACD histogram usado en score (antes calculado pero ignorado)
  • LiqSweep con tolerancia dinámica basada en ATR
  • OI Delta con ventana 20 (1h de historia, antes 5 = 15min)

NUEVO v3 (anticipación):
  • MFI — Money Flow Index (precio×volumen, más difícil de falsificar)
  • Compresión pre-breakout — entra durante acumulación, 2-5 velas antes
  • Funding predictivo — actúa 45min antes del pago de funding
  • Fortaleza relativa BTC — detecta cuando GUA supera/debajo de BTC
  • Liquidaciones proxy — wick + volumen = cascade detectada
  • Walk-forward — umbral adaptativo basado en win rate real
  • Score con pesos explícitos — calibrable y logueable
  • CSV trade logging — historial para mejora continua
  • Cooldown dinámico — más tiempo tras SL que tras TP
"""

from __future__ import annotations
import csv
import logging
import pathlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np

import config
import indicators as ind

log = logging.getLogger("strategy")


# ══════════════════════════════════════════════════════════════════════
#  DATACLASSES
# ══════════════════════════════════════════════════════════════════════

@dataclass
class Signal:
    direction:   str        # "LONG" | "SHORT"
    score:       float      # 0.0 – 1.0
    price:       float
    atr:         float
    atr_pct:     float
    sl:          float
    tp1:         float
    tp2:         float
    rsi:         float
    adx:         float
    mfi:         float      # nuevo v3
    funding:     float
    squeeze:     bool
    rvol:        float
    reason:      str
    components:  Dict[str, float] = field(default_factory=dict)  # desglose score
    # SMC
    fvg_hit:     bool = False
    ob_hit:      bool = False
    liq_sweep:   bool = False
    bos:         str  = "NONE"
    choch:       str  = "NONE"
    # Anticipación
    compression: bool  = False    # compresión pre-breakout activa
    rel_strength: float = 0.0     # fortaleza vs BTC
    liq_candle:  bool  = False    # liquidación detectada
    funding_pre: bool  = False    # dentro de ventana pre-funding


@dataclass
class StrategyState:
    last_candle_time: int         = 0
    oi_history:       List[float] = field(default_factory=list)
    # Walk-forward
    wf_results:       List[bool]  = field(default_factory=list)
    last_was_loss:    bool        = False


_state = StrategyState()


# ══════════════════════════════════════════════════════════════════════
#  WALK-FORWARD — umbral adaptativo
# ══════════════════════════════════════════════════════════════════════

def record_trade_result(win: bool) -> None:
    """Llamar desde position_manager al cerrar cada trade."""
    _state.wf_results.append(win)
    _state.last_was_loss = not win
    if len(_state.wf_results) > config.WF_WINDOW:
        _state.wf_results.pop(0)
    log.info("WF: %d trades | WR=%.0f%%", len(_state.wf_results), _wf_win_rate() * 100)


def _wf_win_rate() -> float:
    if not _state.wf_results:
        return 0.50
    return sum(_state.wf_results) / len(_state.wf_results)


def _adaptive_threshold() -> float:
    """Sube el umbral si el WR cae, lo baja si el WR es alto."""
    wr  = _wf_win_rate()
    thr = config.SCORE_THR
    if len(_state.wf_results) < 10:
        return thr                             # no suficiente historia
    if wr < config.WF_MIN_WR:
        return min(thr + config.WF_THR_ADJUST, 0.80)   # más selectivo
    if wr > config.WF_MAX_WR:
        return max(thr - config.WF_THR_ADJUST, 0.45)   # más agresivo
    return thr


# ══════════════════════════════════════════════════════════════════════
#  CSV TRADE LOGGING
# ══════════════════════════════════════════════════════════════════════

def log_trade_csv(sig: Signal, outcome: str, pnl: float) -> None:
    """Registra cada trade con todos los indicadores para análisis posterior."""
    if not config.TRADE_LOG_ENABLED:
        return
    path = pathlib.Path(config.TRADE_LOG_FILE)
    write_header = not path.exists()
    try:
        with open(path, "a", newline="") as f:
            w = csv.writer(f)
            if write_header:
                w.writerow([
                    "ts", "direction", "score", "price", "sl", "tp1", "tp2",
                    "rsi", "mfi", "adx", "rvol", "atr_pct", "funding",
                    "squeeze", "fvg", "ob", "sweep", "bos", "choch",
                    "compression", "rel_strength", "liq_candle", "funding_pre",
                    "outcome", "pnl", "wf_wr",
                ])
            w.writerow([
                datetime.utcnow().isoformat(),
                sig.direction, round(sig.score, 3),
                round(sig.price, 6), round(sig.sl, 6),
                round(sig.tp1, 6), round(sig.tp2, 6),
                round(sig.rsi, 1), round(sig.mfi, 1),
                round(sig.adx, 1), round(sig.rvol, 2),
                round(sig.atr_pct, 1), round(sig.funding, 6),
                sig.squeeze, sig.fvg_hit, sig.ob_hit,
                sig.liq_sweep, sig.bos, sig.choch,
                sig.compression, round(sig.rel_strength, 5),
                sig.liq_candle, sig.funding_pre,
                outcome, round(pnl, 4), round(_wf_win_rate(), 3),
            ])
    except Exception as e:
        log.error("CSV log error: %s", e)


# ══════════════════════════════════════════════════════════════════════
#  FUNCIÓN PRINCIPAL
# ══════════════════════════════════════════════════════════════════════

def analyze(
    candles:        List[Dict],
    candles_trend:  List[Dict],
    candles_macro:  List[Dict],
    funding_rate:   float = 0.0,
    open_interest:  float = 0.0,
    btc_candles:    Optional[List[Dict]] = None,   # nuevo v3
) -> Optional[Signal]:

    # Mínimo 80 velas para indicadores cortos; EMA200 necesita 200+ (config lo garantiza)
    if len(candles) < 80:
        log.warning("Pocas velas: %d (mínimo 80)", len(candles))
        return None

    # ── Anti-duplicado ────────────────────────────────────────────────────────
    last_time = candles[-1]["time"]
    if last_time == _state.last_candle_time:
        return None
    _state.last_candle_time = last_time

    # ── Filtro de sesión ──────────────────────────────────────────────────────
    if config.SESSION_FILTER and not _in_session():
        log.info("Fuera de sesión London/NY — skip")
        return None

    # ── Arrays 3m ─────────────────────────────────────────────────────────────
    opens   = [c["open"]   for c in candles]
    highs   = [c["high"]   for c in candles]
    lows    = [c["low"]    for c in candles]
    closes  = [c["close"]  for c in candles]
    volumes = [c["volume"] for c in candles]

    # ── Indicadores clásicos ──────────────────────────────────────────────────
    ema9   = ind.ema(closes, config.EMA_FAST)
    ema21  = ind.ema(closes, config.EMA_SLOW)
    ema50  = ind.ema(closes, config.EMA_TREND)
    # EMA200 ahora válida porque LOOKBACK=260 ≥ 200
    ema200 = ind.ema(closes, config.EMA_MACRO)
    rsi14  = ind.rsi(closes, config.RSI_PERIOD)
    atr14  = ind.atr(highs, lows, closes, 14)
    adx14, di_p, di_m = ind.adx(highs, lows, closes, config.ADX_PERIOD)
    cvd20  = ind.cvd(opens, closes, volumes, config.CVD_LB)

    # MACD — ahora SÍ se usa en el score
    _, _, macd_hist = ind.macd(closes)

    # MFI — nuevo v3
    mfi14 = ind.mfi(highs, lows, closes, volumes, config.MFI_PERIOD)

    i = -2   # última vela cerrada
    price      = closes[i]
    e9, e21    = float(ema9[i]),   float(ema21[i])
    e50, e200  = float(ema50[i]),  float(ema200[i])
    rsi_v      = float(rsi14[i])
    atr_v      = float(atr14[i])
    adx_v      = float(adx14[i])
    mfi_v      = float(mfi14[i])
    macd_h     = float(macd_hist[i])
    macd_h_pre = float(macd_hist[i-1])

    # ── ATR Percentil ─────────────────────────────────────────────────────────
    atr_pct  = ind.atr_percentile(atr14, config.ATR_PERCENTILE_LB)
    high_vol = atr_pct >= 75
    low_vol  = atr_pct <= 25

    # ── TTM Squeeze ───────────────────────────────────────────────────────────
    sqz_arr, mom_arr = ind.squeeze_momentum(
        highs, lows, closes,
        config.BB_PERIOD, config.BB_MULT,
        config.KC_PERIOD, config.KC_MULT,
        config.MOM_PERIOD,
    )
    in_squeeze      = bool(sqz_arr[i])
    squeeze_release = bool(sqz_arr[i-1]) and not in_squeeze
    mom_v           = float(mom_arr[i])
    mom_prev        = float(mom_arr[i-1])
    mom_bearish     = squeeze_release and mom_v < 0 and mom_v < mom_prev
    mom_bullish     = squeeze_release and mom_v > 0 and mom_v > mom_prev

    # ── RVOL ─────────────────────────────────────────────────────────────────
    rvol_arr = ind.rvol(volumes, config.RVOL_PERIOD)
    rvol_v   = float(rvol_arr[i])

    # ── VWAP + Bandas ─────────────────────────────────────────────────────────
    vwap_arr, vwap_up, vwap_dn = ind.vwap_bands(
        highs, lows, closes, volumes,
        config.VWAP_PERIOD, config.VWAP_BAND_MULT,
    )
    vwap_v        = float(vwap_arr[i])
    above_vwap    = price > vwap_v
    below_vwap    = price < vwap_v
    extended_up   = price > float(vwap_up[i])
    extended_down = price < float(vwap_dn[i])

    # ── CVD Divergencia ───────────────────────────────────────────────────────
    cvd_bear_div, cvd_bull_div = ind.cvd_divergence(closes, cvd20, config.CVD_DIV_LB)

    # ── FVG ───────────────────────────────────────────────────────────────────
    bear_fvg, bull_fvg = ind.detect_fvg(
        highs, lows, closes, config.FVG_LOOKBACK, config.FVG_MIN_SIZE,
    )
    price_in_bear_fvg = bear_fvg is not None and bear_fvg["bottom"] <= price <= bear_fvg["top"]
    price_in_bull_fvg = bull_fvg is not None and bull_fvg["bottom"] <= price <= bull_fvg["top"]

    # ── Order Blocks ──────────────────────────────────────────────────────────
    bear_ob, bull_ob = ind.detect_order_blocks(
        opens, highs, lows, closes, config.OB_LOOKBACK, config.OB_IMPULSE_BARS,
    )
    price_in_bear_ob = bear_ob is not None and bear_ob["low"] <= price <= bear_ob["high"]
    price_in_bull_ob = bull_ob is not None and bull_ob["low"] <= price <= bull_ob["high"]

    # ── Liquidity Sweeps — tolerancia dinámica (fix v3) ───────────────────────
    swept_highs, swept_lows = ind.detect_liquidity_sweep(
        highs, lows, closes, opens,
        config.LIQ_LOOKBACK, config.LIQ_TOLERANCE,
        atr_value=atr_v, price=price,   # tolerancia dinámica
    )

    # ── Market Structure ──────────────────────────────────────────────────────
    ms = ind.market_structure(highs, lows, closes)

    # ── OI Delta — ahora con ventana 20 (fix v3) ─────────────────────────────
    _state.oi_history.append(open_interest)
    if len(_state.oi_history) > config.OI_HISTORY_LEN:
        _state.oi_history.pop(0)
    oi_delta = _oi_delta()

    # ── NUEVO: Compresión pre-breakout ────────────────────────────────────────
    compressing, compression_score = ind.detect_compression(
        highs, lows, volumes, config.COMPRESSION_BARS,
    )

    # ── NUEVO: Funding predictivo ─────────────────────────────────────────────
    mins_to_funding = ind.minutes_to_next_funding(config.FUNDING_HOURS_UTC)
    funding_pre_window = mins_to_funding <= config.FUNDING_PRE_MINUTES

    # ── NUEVO: Fortaleza relativa vs BTC ─────────────────────────────────────
    rel_str = 0.0
    if btc_candles and len(btc_candles) >= config.REL_STRENGTH_LB + 1:
        btc_closes = [c["close"] for c in btc_candles]
        rel_str = ind.relative_strength(closes, btc_closes, config.REL_STRENGTH_LB)

    # ── NUEVO: Liquidaciones proxy ────────────────────────────────────────────
    liq_long_candle, liq_short_candle = ind.detect_liquidation_candle(
        opens, highs, lows, closes, volumes, atr_v,
    )

    # ── Tendencia 15m ─────────────────────────────────────────────────────────
    trend_bias = "NEUTRAL"
    if len(candles_trend) >= 55:
        tc   = [c["close"] for c in candles_trend]
        te9  = float(ind.ema(tc, config.EMA_FAST)[-1])
        te21 = float(ind.ema(tc, config.EMA_SLOW)[-1])
        te50 = float(ind.ema(tc, config.EMA_TREND)[-1])
        trend_bias = (
            "DOWN" if te9 < te21 and te21 < te50 else
            "UP"   if te9 > te21 and te21 > te50 else
            "NEUTRAL"
        )

    # ── Estructura macro 1h — EMA200 válida aquí (72 velas × 1h = 72h) ───────
    macro_bias = "NEUTRAL"
    if len(candles_macro) >= 55:
        mc    = [c["close"] for c in candles_macro]
        me50  = float(ind.ema(mc, config.EMA_TREND)[-1])
        me200 = float(ind.ema(mc, config.EMA_MACRO)[-1])
        macro_bias = (
            "DOWN" if mc[-1] < me50 < me200 else
            "UP"   if mc[-1] > me50 > me200 else
            "NEUTRAL"
        )

    # ── Log de contexto ───────────────────────────────────────────────────────
    log.info(
        "price=%.5f rsi=%.1f mfi=%.1f adx=%.1f atrPct=%.0f sqz=%s "
        "rvol=%.2fx relStr=%.4f compress=%s fundPre=%s bias15m=%s macro=%s "
        "liqL=%s liqS=%s minsToFund=%d",
        price, rsi_v, mfi_v, adx_v, atr_pct, in_squeeze,
        rvol_v, rel_str, compressing, funding_pre_window,
        trend_bias, macro_bias, liq_long_candle, liq_short_candle, mins_to_funding,
    )

    # ── Baja volatilidad + ADX bajo → skip ───────────────────────────────────
    if low_vol and adx_v < config.ADX_MIN:
        log.info("Baja volatilidad + ADX bajo — skip")
        return None

    # ── Umbral adaptativo walk-forward ────────────────────────────────────────
    score_thr = _adaptive_threshold()
    if score_thr != config.SCORE_THR:
        log.info("WF umbral adaptado: %.2f → %.2f (WR=%.0f%%)",
                 config.SCORE_THR, score_thr, _wf_win_rate() * 100)

    # ── SCORES ────────────────────────────────────────────────────────────────
    short_score, short_parts, short_comp = _score_short(
        price=price, e9=e9, e21=e21, e50=e50, e200=e200,
        rsi=rsi_v, mfi=mfi_v, adx=adx_v, atr_pct=atr_pct,
        macd_h=macd_h, macd_h_pre=macd_h_pre,
        mom_bearish=mom_bearish, in_squeeze=in_squeeze,
        rvol=rvol_v, above_vwap=above_vwap, extended_up=extended_up,
        cvd_div=cvd_bear_div,
        swept_highs=swept_highs, in_bear_fvg=price_in_bear_fvg, in_bear_ob=price_in_bear_ob,
        ms=ms, oi_delta=oi_delta, funding=funding_rate,
        trend_15m=trend_bias, macro_1h=macro_bias,
        compressing=compressing, compression_score=compression_score,
        rel_str=rel_str, liq_short=liq_short_candle,
        funding_pre=funding_pre_window,
    )

    long_score, long_parts, long_comp = _score_long(
        price=price, e9=e9, e21=e21, e50=e50, e200=e200,
        rsi=rsi_v, mfi=mfi_v, adx=adx_v, atr_pct=atr_pct,
        macd_h=macd_h, macd_h_pre=macd_h_pre,
        mom_bullish=mom_bullish, in_squeeze=in_squeeze,
        rvol=rvol_v, below_vwap=below_vwap, extended_down=extended_down,
        cvd_div=cvd_bull_div,
        swept_lows=swept_lows, in_bull_fvg=price_in_bull_fvg, in_bull_ob=price_in_bull_ob,
        ms=ms, oi_delta=oi_delta, funding=funding_rate,
        trend_15m=trend_bias, macro_1h=macro_bias,
        compressing=compressing, compression_score=compression_score,
        rel_str=rel_str, liq_long=liq_long_candle,
        funding_pre=funding_pre_window,
    )

    # ── Selección de dirección ────────────────────────────────────────────────
    direction  = None
    score      = 0.0
    parts      = ""
    components = {}

    if short_score > long_score and short_score >= score_thr:
        direction, score, parts, components = "SHORT", short_score, short_parts, short_comp
    elif long_score >= score_thr:
        direction, score, parts, components = "LONG", long_score, long_parts, long_comp

    if direction is None:
        log.info("Sin señal (LONG=%.2f SHORT=%.2f thr=%.2f)", long_score, short_score, score_thr)
        return None

    # ── SL/TP dinámico ────────────────────────────────────────────────────────
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
        direction    = direction,
        score        = round(score, 3),
        price        = round(price, 6),
        atr          = round(atr_v, 6),
        atr_pct      = round(atr_pct, 1),
        sl           = round(sl,  6),
        tp1          = round(tp1, 6),
        tp2          = round(tp2, 6),
        rsi          = round(rsi_v, 1),
        mfi          = round(mfi_v, 1),
        adx          = round(adx_v, 1),
        funding      = round(funding_rate, 6),
        squeeze      = in_squeeze,
        rvol         = round(rvol_v, 2),
        reason       = parts,
        components   = components,
        fvg_hit      = price_in_bear_fvg if direction == "SHORT" else price_in_bull_fvg,
        ob_hit       = price_in_bear_ob  if direction == "SHORT" else price_in_bull_ob,
        liq_sweep    = swept_highs       if direction == "SHORT" else swept_lows,
        bos          = ms["bos"],
        choch        = ms["choch"],
        compression  = compressing,
        rel_strength = round(rel_str, 5),
        liq_candle   = liq_short_candle if direction == "SHORT" else liq_long_candle,
        funding_pre  = funding_pre_window,
    )


# ══════════════════════════════════════════════════════════════════════
#  SCORER SHORT
# ══════════════════════════════════════════════════════════════════════

def _score_short(
    price, e9, e21, e50, e200,
    rsi, mfi, adx, atr_pct,
    macd_h, macd_h_pre,
    mom_bearish, in_squeeze,
    rvol, above_vwap, extended_up,
    cvd_div,
    swept_highs, in_bear_fvg, in_bear_ob,
    ms, oi_delta, funding,
    trend_15m, macro_1h,
    compressing, compression_score,
    rel_str, liq_short,
    funding_pre,
) -> Tuple[float, str, Dict[str, float]]:

    c: Dict[str, float] = {}   # componentes para logging
    parts = []

    # ── REQUERIDO: estructura bajista mínima ──────────────────────────────────
    if not (e9 < e21):
        return 0.0, "", {}

    # ── Estructura EMA ────────────────────────────────────────────────────────
    c["ema_struct"] = 0.18
    parts.append("EMA9<EMA21 ✅")
    if e21 < e50:
        c["ema_21_50"] = 0.07; parts.append("EMA21<EMA50 ✅")
    if price < e200:
        c["ema_200"]   = 0.05; parts.append("Bajo EMA200 ✅")

    # ── RSI zona cargada (SHORT: RSI medio-alto, no sobrecompra extrema) ──────
    rsi_zone = (config.RSI_OS + 15) <= rsi <= config.RSI_OB
    if rsi > config.RSI_OB:
        c["rsi_ob"]   = 0.09; parts.append(f"RSI={rsi:.0f} sobrecompra ✅")
    elif rsi_zone:
        c["rsi_zone"] = 0.06; parts.append(f"RSI={rsi:.0f} zona carga ✅")

    # ── MFI sobrecompra (nuevo v3 — precio×volumen) ───────────────────────────
    if mfi > config.MFI_OB:
        c["mfi_ob"]   = 0.08; parts.append(f"MFI={mfi:.0f} sobrecompra ✅")
    elif mfi > 60:
        c["mfi_mid"]  = 0.03; parts.append(f"MFI={mfi:.0f} elevado ⚠️")

    # ── MACD histograma bajista (fix v3 — antes ignorado) ─────────────────────
    if macd_h < 0 and macd_h < macd_h_pre:
        c["macd"]     = 0.06; parts.append("MACD hist bajista ✅")
    elif macd_h < 0:
        c["macd_neg"] = 0.03; parts.append("MACD negativo ⚠️")

    # ── SMC: Liquidity Sweep equal highs ──────────────────────────────────────
    if swept_highs:
        c["liq_sweep"] = 0.14; parts.append("🎣 Barrido equal highs ✅")

    # ── SMC: FVG bajista ──────────────────────────────────────────────────────
    if in_bear_fvg:
        c["fvg"]       = 0.10; parts.append("📦 FVG bajista ✅")

    # ── SMC: Order Block bajista ──────────────────────────────────────────────
    if in_bear_ob:
        c["ob"]        = 0.08; parts.append("🧱 OB bajista ✅")

    # ── SMC: BOS/CHoCH ───────────────────────────────────────────────────────
    if ms["bos"] == "BEAR":
        c["bos"]       = 0.07; parts.append("⚡ BOS bajista ✅")
    elif ms["choch"] == "BEAR":
        c["choch"]     = 0.05; parts.append("🔄 CHoCH bajista ✅")

    # ── Squeeze liberando bajista ─────────────────────────────────────────────
    if mom_bearish:
        c["squeeze"]   = 0.10; parts.append("💥 Squeeze bajista ✅")
    elif in_squeeze:
        c["squeeze_w"] = 0.03; parts.append("🌀 Squeeze activo ⏳")

    # ── CVD divergencia bajista ───────────────────────────────────────────────
    if cvd_div:
        c["cvd_div"]   = 0.08; parts.append("📊 CVD div bajista ✅")

    # ── VWAP extendido ────────────────────────────────────────────────────────
    if extended_up:
        c["vwap_ext"]  = 0.06; parts.append("📈 Extendido VWAP ✅")
    elif above_vwap:
        c["vwap"]      = 0.02; parts.append("Sobre VWAP ⚠️")

    # ── RVOL ─────────────────────────────────────────────────────────────────
    if rvol >= config.RVOL_MIN:
        c["rvol"]      = 0.05; parts.append(f"📣 RVOL={rvol:.1f}x ✅")

    # ── ADX ──────────────────────────────────────────────────────────────────
    if adx >= config.ADX_MIN:
        c["adx"]       = 0.05; parts.append(f"ADX={adx:.1f} ✅")
    else:
        c["adx_pen"]   = -0.05; parts.append(f"ADX={adx:.1f} bajo ❌")

    # ── OI Delta ─────────────────────────────────────────────────────────────
    if oi_delta > 0:
        c["oi_delta"]  = 0.04; parts.append("OI↑ dinero nuevo ✅")

    # ── Funding ───────────────────────────────────────────────────────────────
    if funding >= config.FUNDING_EXTREME_LONG:
        c["funding"]   = 0.07; parts.append(f"💰 Funding extremo {funding:.4%} ✅")
    elif funding > 0:
        c["funding_p"] = 0.02; parts.append("Funding positivo ⚠️")

    # ── NUEVO: Funding predictivo (45min antes del pago) ─────────────────────
    if funding_pre and funding >= config.FUNDING_EXTREME_LONG * 0.5:
        c["funding_pre"] = 0.06; parts.append("⏰ Pre-funding SHORT ✅")

    # ── Bias 15m ─────────────────────────────────────────────────────────────
    if trend_15m == "DOWN":
        c["bias_15m"]  = 0.08; parts.append("📉 Bias 15m bajista ✅")
    elif trend_15m == "UP":
        c["bias_15m_p"]= -0.12; parts.append("📈 Bias 15m alcista ❌")

    # ── Macro 1h ─────────────────────────────────────────────────────────────
    if macro_1h == "DOWN":
        c["macro"]     = 0.06; parts.append("🏔 Macro 1h bajista ✅")
    elif macro_1h == "UP":
        c["macro_p"]   = -0.08; parts.append("🏔 Macro 1h alcista ❌")

    # ── NUEVO: Compresión pre-breakout bajista ────────────────────────────────
    # Compresión con bias bajista = acumulación de ventas
    if compressing and trend_15m == "DOWN":
        c["compression"] = round(compression_score * 0.08, 3)
        parts.append(f"🗜 Compresión bajista [{compression_score:.2f}] ✅")

    # ── NUEVO: Fortaleza relativa BTC ─────────────────────────────────────────
    # GUA más débil que BTC → favorece SHORT
    if rel_str < -config.REL_STRENGTH_THR:
        c["rel_str"]   = 0.05; parts.append(f"📉 Debilidad relativa BTC {rel_str:.4f} ✅")

    # ── NUEVO: Liquidación de longs (wick inferior = shorts se benefician) ────
    if liq_short:
        c["liq_candle"] = 0.07; parts.append("⚡ Liquidación longs detectada ✅")

    # ── ATR régimen ──────────────────────────────────────────────────────────
    if 50 <= atr_pct <= 85:
        c["atr_reg"]   = 0.03; parts.append(f"ATR={atr_pct:.0f}% óptimo ✅")
    elif atr_pct > 90:
        c["atr_ext"]   = -0.05; parts.append(f"ATR={atr_pct:.0f}% extremo ⚠️")

    score = sum(c.values())
    return round(min(max(score, 0.0), 1.0), 3), " | ".join(parts), c


# ══════════════════════════════════════════════════════════════════════
#  SCORER LONG
# ══════════════════════════════════════════════════════════════════════

def _score_long(
    price, e9, e21, e50, e200,
    rsi, mfi, adx, atr_pct,
    macd_h, macd_h_pre,
    mom_bullish, in_squeeze,
    rvol, below_vwap, extended_down,
    cvd_div,
    swept_lows, in_bull_fvg, in_bull_ob,
    ms, oi_delta, funding,
    trend_15m, macro_1h,
    compressing, compression_score,
    rel_str, liq_long,
    funding_pre,
) -> Tuple[float, str, Dict[str, float]]:

    c: Dict[str, float] = {}
    parts = []

    # ── REQUERIDO: RSI no en sobrecompra (fix v3: RSI_OS=42, era 37) ─────────
    if rsi > config.RSI_OB:
        return 0.0, "", {}

    # ── RSI sobreventa ────────────────────────────────────────────────────────
    if rsi <= config.RSI_OS:
        c["rsi_os"]    = 0.20; parts.append(f"RSI={rsi:.0f} sobreventa ✅")
    else:
        c["rsi_mid"]   = 0.08; parts.append(f"RSI={rsi:.0f} zona media ⚠️")

    # ── MFI sobreventa (nuevo v3) ─────────────────────────────────────────────
    if mfi < config.MFI_OS:
        c["mfi_os"]    = 0.09; parts.append(f"MFI={mfi:.0f} sobreventa ✅")
    elif mfi < 40:
        c["mfi_low"]   = 0.04; parts.append(f"MFI={mfi:.0f} bajo ⚠️")

    # ── MACD histograma alcista (fix v3 — antes ignorado) ─────────────────────
    if macd_h > 0 and macd_h > macd_h_pre:
        c["macd"]      = 0.06; parts.append("MACD hist alcista ✅")
    elif macd_h > 0:
        c["macd_pos"]  = 0.03; parts.append("MACD positivo ⚠️")

    # ── SMC: Liquidity sweep equal lows ───────────────────────────────────────
    if swept_lows:
        c["liq_sweep"] = 0.16; parts.append("🎣 Barrido equal lows ✅")

    # ── SMC: FVG alcista ──────────────────────────────────────────────────────
    if in_bull_fvg:
        c["fvg"]       = 0.10; parts.append("📦 FVG alcista ✅")

    # ── SMC: Order Block alcista ──────────────────────────────────────────────
    if in_bull_ob:
        c["ob"]        = 0.08; parts.append("🧱 OB alcista ✅")

    # ── SMC: CHoCH/BOS alcista ────────────────────────────────────────────────
    if ms["choch"] == "BULL":
        c["choch"]     = 0.08; parts.append("🔄 CHoCH alcista ✅")
    elif ms["bos"] == "BULL":
        c["bos"]       = 0.05; parts.append("⚡ BOS alcista ✅")

    # ── Squeeze liberando alcista ─────────────────────────────────────────────
    if mom_bullish:
        c["squeeze"]   = 0.10; parts.append("💥 Squeeze alcista ✅")
    elif in_squeeze:
        c["squeeze_w"] = 0.03; parts.append("🌀 Squeeze activo ⏳")

    # ── CVD divergencia alcista ───────────────────────────────────────────────
    if cvd_div:
        c["cvd_div"]   = 0.08; parts.append("📊 CVD div alcista ✅")

    # ── VWAP extendido ────────────────────────────────────────────────────────
    if extended_down:
        c["vwap_ext"]  = 0.06; parts.append("📉 Extendido bajo VWAP ✅")

    # ── RVOL ─────────────────────────────────────────────────────────────────
    if rvol >= config.RVOL_MIN:
        c["rvol"]      = 0.05; parts.append(f"📣 RVOL={rvol:.1f}x ✅")

    # ── ADX (LONG funciona mejor con ADX bajo — counter-trend) ───────────────
    if adx < 25:
        c["adx"]       = 0.05; parts.append(f"ADX={adx:.1f} bajo ✅")
    elif adx > 35:
        c["adx_pen"]   = -0.08; parts.append(f"ADX={adx:.1f} fuerte ❌")

    # ── OI Delta ─────────────────────────────────────────────────────────────
    if oi_delta < 0:
        c["oi_delta"]  = 0.04; parts.append("OI↓ short covering ✅")

    # ── Funding extremo negativo ──────────────────────────────────────────────
    if funding <= config.FUNDING_EXTREME_SHORT:
        c["funding"]   = 0.07; parts.append(f"💰 Funding extremo {funding:.4%} ✅")

    # ── NUEVO: Funding predictivo ─────────────────────────────────────────────
    if funding_pre and funding <= config.FUNDING_EXTREME_SHORT * 0.5:
        c["funding_pre"] = 0.06; parts.append("⏰ Pre-funding LONG ✅")

    # ── Bias 15m ─────────────────────────────────────────────────────────────
    if trend_15m == "UP":
        c["bias_15m"]  = 0.07; parts.append("📈 Bias 15m alcista ✅")
    elif trend_15m == "DOWN":
        c["bias_15m_p"]= -0.12; parts.append("📉 Bias 15m bajista ❌")

    # ── Macro 1h ─────────────────────────────────────────────────────────────
    if macro_1h == "DOWN":
        c["macro_p"]   = -0.10; parts.append("🏔 Macro 1h bajista ❌")
    elif macro_1h == "UP":
        c["macro"]     = 0.05; parts.append("🏔 Macro 1h alcista ✅")

    # ── NUEVO: Compresión + bias alcista ──────────────────────────────────────
    if compressing and trend_15m == "UP":
        c["compression"] = round(compression_score * 0.08, 3)
        parts.append(f"🗜 Compresión alcista [{compression_score:.2f}] ✅")

    # ── NUEVO: Fortaleza relativa BTC ─────────────────────────────────────────
    if rel_str > config.REL_STRENGTH_THR:
        c["rel_str"]   = 0.05; parts.append(f"📈 Fortaleza relativa BTC {rel_str:.4f} ✅")

    # ── NUEVO: Liquidación de shorts ──────────────────────────────────────────
    if liq_long:
        c["liq_candle"] = 0.07; parts.append("⚡ Liquidación shorts detectada ✅")

    # ── EMA estructura alcista (bonus) ────────────────────────────────────────
    if e9 > e21:
        c["ema_struct"] = 0.05; parts.append("EMA9>EMA21 ✅")
    if e21 > e50:
        c["ema_21_50"]  = 0.03; parts.append("EMA21>EMA50 ✅")

    score = sum(c.values())
    return round(min(max(score, 0.0), 1.0), 3), " | ".join(parts), c


# ══════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════

def _in_session() -> bool:
    hour = datetime.now(timezone.utc).hour
    return any(start <= hour < end for start, end in config.SESSION_HOURS)


def _oi_delta() -> float:
    h = _state.oi_history
    if len(h) < 2:
        return 0.0
    # Compara último vs promedio de la primera mitad (más robusto que primero vs último)
    mid = len(h) // 2
    return h[-1] - float(np.mean(h[:mid]))


def cooldown_duration(is_sl: bool, atr_pct: float) -> int:
    """Cooldown dinámico: más tiempo tras SL y en alta volatilidad."""
    base = config.COOLDOWN_MIN_LOSS if is_sl else config.COOLDOWN_MIN
    if atr_pct >= 75:
        base = int(base * config.COOLDOWN_HIGHVOL_MULT)
    return base
