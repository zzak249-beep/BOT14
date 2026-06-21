"""
QF×JP Bot #5 — PREDATOR Scanner (port completo del Pine v3.6)
═══════════════════════════════════════════════════════════════════════════
Cablea composite_score_engine.py + decay_ic_engine.py + walk_forward_kelly.py
con tus módulos YA EXISTENTES (order_block_km, sniper_vsa_matrix,
trend_magic_rmi, price_action_framework, stc_asymmetry, fibonacci_mtf) para
construir el conviction counter y las señales externas del score compuesto.

LO QUE SE REUTILIZA (no se reimplementa):
  - Order Blocks / estructura → order_block_km.py
  - VSA + Sniper Engine       → sniper_vsa_matrix.py
  - RMI + Trend Magic         → trend_magic_rmi.py
  - Patrones de vela          → price_action_framework.py
  - STC + Volumen + Slope     → stc_asymmetry.py
  - Zona dorada Fibonacci     → fibonacci_mtf.py

LO QUE SE CALCULA LOCAL (no había módulo previo que encajara, son piezas
simples que no merecía la pena forzar sobre algo que no es eso):
  - CVD (Cumulative Volume Delta) básico, z-scored
  - VDI (Volume Delta Imbalance) en ventana corta
  - Volume Profile simplificado (POC vía bin de mayor volumen)
  - LS Ratio sentiment contrarian (percentil de RSI)
  - HTF alignment (15m/1h/4h/Semanal, EMA9 vs EMA21)

SL/TP: dinámico desde swing low/high reciente (igual que el Pine [SLD]),
acotado por un mínimo de ATR — no un múltiplo fijo genérico.

Drop-in para main.py: misma firma scan_loop(client, risk, pos_mgr,
complement, journal) que el resto de scanners de esta sesión.
═══════════════════════════════════════════════════════════════════════════
"""
import asyncio
import logging
import time
from collections import Counter

import config as C
from bingx_client import BingXClient
from risk_manager import RiskManager
from position_manager import PositionManager, OpenTrade
import telegram_client as tg

from composite_score_engine import score_engine, _ema, _rma, _atr, _stdev, _sma, _tanh
from decay_ic_engine import decay_engine
from walk_forward_kelly import wf_kelly
from order_block_km import ob_engine, order_block_km_filter
from sniper_vsa_matrix import sniper_vsa_filter
from trend_magic_rmi import trend_magic_rmi_filter
from price_action_framework import price_action_filter
from stc_asymmetry import stc_volume_slope_filter
from fibonacci_mtf import fib_mtf_filter

log = logging.getLogger("qfjp_predator")


# ── Piezas locales simples (sin módulo previo que encajara) ────────────────

def _cvd_score(klines: list, roll: int = 60, ema_len: int = 20) -> float:
    """CVD básico z-scored → 0-1. bv/sv estimados desde posición del cierre
    en el rango de la vela (misma técnica que usa guardian_mode en
    complement_engine.py, aquí expuesto como función reutilizable)."""
    n = len(klines)
    if n < roll + ema_len:
        return 0.5
    deltas = []
    for c in klines[-(roll + ema_len):]:
        o, h, l, cl, v = c[1], c[2], c[3], c[4], c[5]
        rng = h - l
        bv = ((cl - l) / rng) * v if rng > 0 else v * 0.5
        sv = ((h - cl) / rng) * v if rng > 0 else v * 0.5
        deltas.append(bv - sv)
    cvd_series = []
    acc = 0.0
    for d in deltas:
        acc += d
        cvd_series.append(acc)
    cvd_ema = _ema(cvd_series, ema_len)
    std = _stdev(cvd_series, ema_len * 2)[-1]
    z = (cvd_series[-1] - cvd_ema[-1]) / std if std > 1e-9 else 0.0
    return max(0.0, min(1.0, (_tanh(z) + 1) / 2))


def _vdi_score(klines: list, window: int = 3, lookback: int = 20) -> float:
    """Volume Delta Imbalance en ventana corta → 0-1."""
    n = len(klines)
    if n < lookback + window:
        return 0.5
    deltas = []
    for c in klines[-(lookback + window):]:
        o, h, l, cl, v = c[1], c[2], c[3], c[4], c[5]
        rng = h - l
        bv = ((cl - l) / rng) * v if rng > 0 else v * 0.5
        sv = ((h - cl) / rng) * v if rng > 0 else v * 0.5
        deltas.append(bv - sv)
    vdi_sums = []
    for i in range(window, len(deltas) + 1):
        vdi_sums.append(sum(deltas[i - window:i]))
    if len(vdi_sums) < 5:
        return 0.5
    avg = sum(vdi_sums) / len(vdi_sums)
    std = (sum((v - avg) ** 2 for v in vdi_sums) / len(vdi_sums)) ** 0.5
    z = (vdi_sums[-1] - avg) / std if std > 1e-9 else 0.0
    return max(0.0, min(1.0, (_tanh(z) + 1) / 2))


def _vp_poc_score(klines: list, lookback: int = 50) -> float:
    """Volume Profile simplificado: posición del precio vs el bin de mayor
    volumen reciente. >0.5 = por encima del POC (zona de mayor interés)."""
    window = klines[-lookback:] if len(klines) >= lookback else klines
    if not window:
        return 0.5
    poc_close = max(window, key=lambda c: c[5])[4]
    close = klines[-1][4]
    atr = _atr(klines, 14)[-1]
    if atr <= 0:
        return 0.5
    dist = (close - poc_close) / atr
    return max(0.0, min(1.0, (_tanh(dist) + 1) / 2))


def _sentiment_score(klines: list, rsi_len: int = 20, window: int = 40) -> float:
    """LS Ratio sentiment contrarian: percentil del RSI — extremos
    recientes dan señal contraria (sobreventa reciente → score alto)."""
    closes = [c[4] for c in klines]
    n = len(closes)
    if n < rsi_len + window:
        return 0.5
    gains = [max(closes[i] - closes[i - 1], 0.0) for i in range(1, n)]
    losses = [max(closes[i - 1] - closes[i], 0.0) for i in range(1, n)]
    up = _rma(gains, rsi_len)
    down = _rma(losses, rsi_len)
    rsi = 100.0 - 100.0 / (1.0 + up[-1] / down[-1]) if down[-1] > 1e-9 else 100.0
    return max(0.0, min(1.0, (100.0 - rsi) / 100.0))  # invertido = contrarian


async def _compute_htf_score(client, symbol: str) -> tuple:
    tasks = [
        client.get_klines(symbol, "15m", 30),
        client.get_klines(symbol, "1h", 30),
        client.get_klines(symbol, "4h", 30),
        client.get_klines(symbol, "1w", 15),
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    biases = []
    for i, res in enumerate(results):
        if isinstance(res, Exception) or len(res) < 12:
            biases.append(None)
            continue
        closes = [c[4] for c in res]
        if i == 3:  # semanal
            ema10 = _ema(closes, 10)[-1]
            biases.append(closes[-1] > ema10)
        else:
            e9, e21 = _ema(closes, 9)[-1], _ema(closes, 21)[-1]
            biases.append(e9 > e21)

    weights = [1.0, 2.0, 4.0, 8.0]
    total = sum(weights)
    long_raw  = sum(w for b, w in zip(biases, weights) if b is True)
    short_raw = sum(w for b, w in zip(biases, weights) if b is False)
    return long_raw / total, short_raw / total


# ── Diagnóstico ──────────────────────────────────────────────────────────────

def _new_diag():
    return {"counts": Counter(), "signals": 0}


# ── Procesamiento por símbolo ───────────────────────────────────────────────

async def _process_symbol(symbol, client, risk, pos_mgr, diag, journal=None):
    if pos_mgr.is_trading(symbol):
        diag["counts"]["already_trading"] += 1
        return

    try:
        klines = await client.get_klines(symbol, getattr(C, 'TIMEFRAME', '3m'), 200)
    except Exception as e:
        log.debug("[%s] fetch error: %s", symbol, e)
        diag["counts"]["fetch_error"] += 1
        return

    min_bars = max(
        getattr(C, 'PRED_VSA_LOOKBACK', 120) + getattr(C, 'PRED_VSA_EXPIRY', 8) + 2,
        160,
    )
    if len(klines) < min_bars:
        diag["counts"]["insufficient_data"] += 1
        return

    ts = klines[-1][0]
    close = klines[-1][4]
    atr = _atr(klines, 14)[-1]
    if atr <= 0:
        diag["counts"]["atr_invalido"] += 1
        return

    # ── Señales externas, reutilizando módulos ya existentes ─────────────────
    # Order Block KM (siempre se actualiza, igual que en scanner.py principal)
    try:
        ob_engine.update(symbol, klines)
    except Exception as e:
        log.debug("[%s] ob_engine update error: %s", symbol, e)

    ob_long_boost,  ob_long_reason,  _ = order_block_km_filter(symbol, "LONG",  min_samples=getattr(C, 'PRED_OB_MIN_SAMPLES', 5))
    ob_short_boost, ob_short_reason, _ = order_block_km_filter(symbol, "SHORT", min_samples=getattr(C, 'PRED_OB_MIN_SAMPLES', 5))

    sv_long_boost,  sv_long_reason,  sv_long_block  = sniper_vsa_filter(klines, "LONG")
    sv_short_boost, sv_short_reason, sv_short_block = sniper_vsa_filter(klines, "SHORT")

    tm_long_boost,  tm_long_reason,  tm_long_block  = trend_magic_rmi_filter(klines, "LONG")
    tm_short_boost, tm_short_reason, tm_short_block = trend_magic_rmi_filter(klines, "SHORT")

    pa_long_boost,  pa_long_reason,  pa_long_block  = price_action_filter(klines, "LONG")
    pa_short_boost, pa_short_reason, pa_short_block = price_action_filter(klines, "SHORT")

    stcv_long_boost,  stcv_long_reason,  stcv_long_block  = stc_volume_slope_filter(klines, "LONG")
    stcv_short_boost, stcv_short_reason, stcv_short_block = stc_volume_slope_filter(klines, "SHORT")

    fib_long_boost, fib_long_reason, fib_long_block = 0.0, "", False
    fib_short_boost, fib_short_reason, fib_short_block = 0.0, "", False
    if getattr(C, 'PRED_FIB_ENABLED', True):
        try:
            k_daily = await client.get_klines(symbol, "1d", 3)
            if len(k_daily) >= 2:
                fib_long_boost, fib_long_reason, fib_long_block = fib_mtf_filter(k_daily, close, "LONG")
                fib_short_boost, fib_short_reason, fib_short_block = fib_mtf_filter(k_daily, close, "SHORT")
        except Exception as e:
            log.debug("[%s] fib fetch error: %s", symbol, e)

    # ── Conviction counter: cuántas de las confirmaciones externas dicen sí ───
    conviction_long = sum([
        ob_long_boost > 0, sv_long_boost > 0, tm_long_boost > 0,
        pa_long_boost > 0, stcv_long_boost > 0, fib_long_boost > 0,
    ])
    conviction_short = sum([
        ob_short_boost > 0, sv_short_boost > 0, tm_short_boost > 0,
        pa_short_boost > 0, stcv_short_boost > 0, fib_short_boost > 0,
    ])

    struc_score_long  = 0.3 + (0.4 if ob_long_boost  > 0 else 0.0) + (0.3 if pa_long_boost  > 0 else 0.0)
    struc_score_short = 0.3 + (0.4 if ob_short_boost > 0 else 0.0) + (0.3 if pa_short_boost > 0 else 0.0)

    cvd = _cvd_score(klines)
    vdi = _vdi_score(klines)
    vp  = _vp_poc_score(klines)
    sent = _sentiment_score(klines)
    htf_long, htf_short = await _compute_htf_score(client, symbol)

    # ── Score compuesto ────────────────────────────────────────────────────
    result = score_engine.compute(
        symbol, klines, ts,
        cvd_score=cvd,
        htf_score_long=htf_long, htf_score_short=htf_short,
        struc_score_long=struc_score_long, struc_score_short=struc_score_short,
        vp_score_long=vp, vp_score_short=1.0 - vp,
        sent_score_long=sent, sent_score_short=sent,
        vdi_score=vdi,
        conviction_long=conviction_long, conviction_short=conviction_short,
    )
    if not result.get("ok"):
        diag["counts"]["score_engine_sin_datos"] += 1
        return

    comp_long, comp_short = result["comp_long"], result["comp_short"]

    # ── Walk-forward Kelly: actualizar y consultar ────────────────────────────
    thr_std = getattr(C, 'PRED_THR_STD', 55)
    wf_kelly.update(symbol, comp_long, comp_short, close, atr, thr_std)

    # ── Umbral mínimo + decay vivo ─────────────────────────────────────────
    if not result["decay_alive"]:
        diag["counts"]["decay_muerto"] += 1
        return

    direction = "LONG" if comp_long >= comp_short else "SHORT"
    comp = comp_long if direction == "LONG" else comp_short

    if comp < thr_std:
        diag["counts"]["score_bajo"] += 1
        return

    thr_fuel = getattr(C, 'PRED_THR_FUEL', 68)
    thr_sup  = getattr(C, 'PRED_THR_SUP', 80)
    tier = "SUP" if comp >= thr_sup else ("FUEL" if comp >= thr_fuel else "STD")

    if not risk.tier_ok(tier):
        diag["counts"][f"tier_bajo({tier})"] += 1
        return

    diag["signals"] += 1
    diag["counts"]["signal_qualified"] += 1

    # ── SL dinámico desde swing (no múltiplo fijo genérico) ───────────────────
    highs = [c[2] for c in klines[-40:]]
    lows  = [c[3] for c in klines[-40:]]
    sld_min_atr = getattr(C, 'PRED_SLD_MIN_ATR', 1.0)
    if direction == "LONG":
        swing_low = min(lows)
        sl_dist = max(close - swing_low, atr * sld_min_atr)
        sl = close - sl_dist
        tp1 = close + sl_dist * getattr(C, 'PRED_TP1_RR', 1.5)
        tp2 = close + sl_dist * getattr(C, 'PRED_TP2_RR', 3.0)
    else:
        swing_high = max(highs)
        sl_dist = max(swing_high - close, atr * sld_min_atr)
        sl = close + sl_dist
        tp1 = close - sl_dist * getattr(C, 'PRED_TP1_RR', 1.5)
        tp2 = close - sl_dist * getattr(C, 'PRED_TP2_RR', 3.0)

    log.info(
        "[%s] 🦅 PREDATOR %s tier=%s comp=%.0f régimen=%s decay_r=%.2f conv=%d/%d entry=%.6f SL=%.6f",
        symbol, direction, tier, comp, result["regime"], result["decay_r"],
        conviction_long if direction == "LONG" else conviction_short, 6,
        close, sl,
    )

    if C.MODE == "SIGNAL":
        await tg.send(
            f"🦅 *PREDATOR* — `{symbol}` {direction} tier=`{tier}`\n"
            f"Score: `{comp}` | Régimen: `{result['regime']}` | Decay: `{result['decay_r']:.2f}`\n"
            f"Entry: `{close:.6f}` | SL: `{sl:.6f}` | TP1: `{tp1:.6f}` | TP2: `{tp2:.6f}`"
        )
        return

    # ── LIVE ─────────────────────────────────────────────────────────────────
    unrealized = await pos_mgr.get_unrealized_pnl()
    can, reason = await risk.can_trade(unrealized_pnl=unrealized)
    if not can:
        diag["counts"]["risk_blocked"] += 1
        return

    trade_confirmed = False
    try:
        sym_ok, _ = risk.symbol_allowed(symbol)
        if not sym_ok:
            diag["counts"]["symbol_blocked"] += 1
            return
        dir_ok, _ = risk.direction_allowed(direction)
        if not dir_ok:
            diag["counts"]["correlation_blocked"] += 1
            return

        try:
            balance = await client.get_balance()
        except Exception:
            return
        if balance < 5.0:
            balance = C.CAPITAL

        kelly_f, wr_avg = wf_kelly.kelly_fraction(
            symbol, kelly_frac_cap=getattr(C, 'PRED_KELLY_FRAC', 0.25),
            rr=getattr(C, 'PRED_KELLY_RR', 1.8),
        )
        qty = risk.kelly_position_size(balance, close, sl, score=float(comp), tier=tier, symbol=symbol)
        if qty <= 0:
            return

        results = await client.open_trade(
            symbol=symbol, direction=direction, quantity=qty,
            sl_price=sl, tp1_price=tp1, tp2_price=tp2,
        )
        entry_resp = results.get("entry", {})
        if entry_resp.get("code", -1) != 0:
            log.error("[%s] Entrada rechazada: %s", symbol, entry_resp)
            return

        order_id = str(
            entry_resp.get("data", {}).get("order", {}).get("orderId", "unknown")
            or entry_resp.get("data", {}).get("orderId", "unknown")
        )
        trade = OpenTrade(
            symbol=symbol, direction=direction, entry=close, sl=sl,
            tp1=tp1, tp2=tp2, qty=qty, atr=atr, order_id=order_id,
        )
        await pos_mgr.register_trade(trade)
        await tg.notify_trade_opened(
            type("S", (), {"symbol": symbol, "direction": direction, "entry": close,
                           "sl": sl, "tp1": tp1, "tp2": tp2, "score": float(comp), "tier": tier})(),
            qty, order_id,
        )
        trade_confirmed = True

        if journal:
            journal.on_open(
                symbol=symbol, direction=direction, tier=tier, score=float(comp),
                filter_tags={
                    "predator_regime": result["regime"],
                    "predator_decay": f"{result['decay_r']:.2f}",
                    "predator_wf_kelly": f"f={kelly_f:.3f} wr={wr_avg:.2f}",
                },
            )
    except Exception as e:
        log.error("[%s] _process_symbol error: %s", symbol, e)
    finally:
        if not trade_confirmed:
            await risk.release_reservation()


# ── Loop principal ───────────────────────────────────────────────────────────

async def scan_loop(client, risk, pos_mgr, complement=None, journal=None):
    log.info("PREDATOR Scanner v1.0 | Modo=%s", C.MODE)

    iteration = 0
    while True:
        start = time.time()
        iteration += 1
        diag = _new_diag()

        try:
            symbols = await client.get_all_symbols()
        except Exception as e:
            log.error("get_all_symbols error: %s", e)
            await asyncio.sleep(30)
            continue

        if not symbols:
            await asyncio.sleep(10)
            continue

        BATCH = 10  # más liviano que el scalper — cada símbolo hace bastantes más llamadas
        for i in range(0, len(symbols), BATCH):
            batch = symbols[i:i + BATCH]
            await asyncio.gather(
                *[_process_symbol(s, client, risk, pos_mgr, diag, journal) for s in batch],
                return_exceptions=True,
            )
            await asyncio.sleep(0.5)

        elapsed = time.time() - start
        top5 = diag["counts"].most_common(5)
        log.info("Iter %d | %d símbolos | %d señales | %.1fs | %s",
                 iteration, len(symbols), diag["signals"], elapsed,
                 " | ".join(f"{k}={v}" for k, v in top5) if top5 else "—")

        await asyncio.sleep(max(0.0, getattr(C, 'SCAN_INTERVAL', 60) - elapsed))
