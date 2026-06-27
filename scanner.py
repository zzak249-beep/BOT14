"""
EMA9×VWAP Bot — ema9_vwap_scanner.py
════════════════════════════════════════════════════════════════
Estrategia base: Pine v5 "EMA 9 + VWAP Strategy with ATR Trailing Stop"
  Señal LONG:  EMA9 cruza hacia arriba VWAP
  Señal SHORT: EMA9 cruza hacia abajo VWAP
  Exit:        ATR trailing stop dinámico

Confirmaciones:
  1. MACD: histograma en dirección de la señal
  2. RSI:  > 50 LONG / < 50 SHORT
  3. Volumen: > media × VOL_MIN_MULT (opcional)
  4. EMA21: alineada (opcional)

FIX: logs de debug en todos los puntos de bloqueo LIVE
════════════════════════════════════════════════════════════════
"""
import asyncio
import logging
import time
from collections import Counter
from dataclasses import dataclass
from typing import Optional

import numpy as np

import config as C
from bingx_client import BingXClient
from risk_manager import RiskManager
from position_manager import PositionManager, OpenTrade
import telegram_client as tg

try:
    from btc_correlation import compute_correlation, btc_guard
    _BTC_CORR_AVAILABLE = True
except ImportError:
    _BTC_CORR_AVAILABLE = False

try:
    from volatility_regime import vol_engine, Regime as VolRegime
    _VOL_REGIME_AVAILABLE = True
except ImportError:
    _VOL_REGIME_AVAILABLE = False

log = logging.getLogger("ema9_vwap_scanner")

_cb_blacklist: dict[str, float] = {}
CB_COOLDOWN = 600


# ═══════════════════════════════════════════════════════════════
# INDICADORES
# ═══════════════════════════════════════════════════════════════

def _ema(arr: np.ndarray, period: int) -> np.ndarray:
    k = 2.0 / (period + 1)
    out = np.empty_like(arr, dtype=float)
    out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = arr[i] * k + out[i - 1] * (1 - k)
    return out


def _rma(arr: np.ndarray, period: int) -> np.ndarray:
    k = 1.0 / period
    out = np.empty_like(arr, dtype=float)
    out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = arr[i] * k + out[i - 1] * (1 - k)
    return out


def _vwap(klines_arr: np.ndarray) -> np.ndarray:
    h    = klines_arr[:, 2]
    l    = klines_arr[:, 3]
    c    = klines_arr[:, 4]
    v    = klines_arr[:, 5]
    hlc3 = (h + l + c) / 3.0
    cum_pv  = np.cumsum(hlc3 * v)
    cum_vol = np.cumsum(v)
    return np.divide(cum_pv, cum_vol + 1e-12,
                     out=np.zeros_like(cum_pv), where=cum_vol > 0)


def _atr(klines_arr: np.ndarray, period: int) -> np.ndarray:
    h = klines_arr[:, 2]
    l = klines_arr[:, 3]
    c = klines_arr[:, 4]
    tr = np.maximum(h[1:] - l[1:],
         np.maximum(np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1])))
    tr = np.concatenate([[tr[0]], tr])
    return _rma(tr, period)


def _rsi(c: np.ndarray, period: int) -> np.ndarray:
    diff = np.diff(c)
    gain = np.where(diff > 0, diff, 0.0)
    loss = np.where(diff < 0, -diff, 0.0)
    gain = np.concatenate([[gain[0]], gain])
    loss = np.concatenate([[loss[0]], loss])
    ag = _rma(gain, period)
    al = _rma(loss, period)
    rs = np.divide(ag, al + 1e-12, out=np.ones_like(ag), where=al > 0)
    return 100.0 - (100.0 / (1.0 + rs))


def _macd(c: np.ndarray, fast: int, slow: int, signal: int):
    ema_f    = _ema(c, fast)
    ema_s    = _ema(c, slow)
    macd_l   = ema_f - ema_s
    signal_l = _ema(macd_l, signal)
    hist     = macd_l - signal_l
    return macd_l, signal_l, hist


def _crossover(a: np.ndarray, b: np.ndarray, lookback: int = 1) -> bool:
    for i in range(1, lookback + 2):
        if len(a) <= i:
            break
        if a[-i] > b[-i] and a[-(i+1)] <= b[-(i+1)]:
            return True
    return False


def _crossunder(a: np.ndarray, b: np.ndarray, lookback: int = 1) -> bool:
    for i in range(1, lookback + 2):
        if len(a) <= i:
            break
        if a[-i] < b[-i] and a[-(i+1)] >= b[-(i+1)]:
            return True
    return False


# ═══════════════════════════════════════════════════════════════
# SEÑAL
# ═══════════════════════════════════════════════════════════════

@dataclass
class EV_Signal:
    symbol:    str
    direction: str
    score:     float
    entry:     float
    sl:        float
    tp1:       float
    tp2:       float
    atr:       float
    rsi:       float
    macd_hist: float
    vol_ratio: float
    reason:    str = ""


def _analyze(symbol: str, klines: list) -> EV_Signal:
    def _none(reason: str) -> EV_Signal:
        return EV_Signal(symbol=symbol, direction="NONE", score=0,
                         entry=0, sl=0, tp1=0, tp2=0, atr=0,
                         rsi=50, macd_hist=0, vol_ratio=1, reason=reason)

    if len(klines) < 60:
        return _none("insufficient_data")

    arr = np.array(klines, dtype=float)
    c   = arr[:, 4]
    v   = arr[:, 5]

    ema9_arr  = _ema(c, getattr(C, 'EMA9_PERIOD',  9))
    ema21_arr = _ema(c, getattr(C, 'EMA21_PERIOD', 21))
    vwap_arr  = _vwap(arr)
    atr_arr   = _atr(arr, getattr(C, 'ATR_LEN', 14))

    atr   = float(atr_arr[-1])
    price = float(c[-1])

    if atr <= 0:
        return _none("invalid_atr")

    rsi_arr = _rsi(c, getattr(C, 'RSI_PERIOD', 14))
    rsi     = float(rsi_arr[-1])

    macd_l, sig_l, hist = _macd(
        c,
        getattr(C, 'MACD_FAST',   12),
        getattr(C, 'MACD_SLOW',   26),
        getattr(C, 'MACD_SIGNAL',  9),
    )
    macd_hist   = float(hist[-1])
    macd_rising = hist[-1] > hist[-2] if len(hist) > 1 else True

    vol_period = getattr(C, 'VOL_MA_PERIOD', 20)
    if len(v) >= vol_period:
        vol_ma    = float(np.mean(v[-vol_period:]))
        vol_curr  = float(v[-1])
        vol_ratio = vol_curr / vol_ma if vol_ma > 0 else 1.0
    else:
        vol_ratio = 1.0

    lookback    = getattr(C, 'CROSS_LOOKBACK', 3)
    long_cross  = _crossover(ema9_arr,  vwap_arr, lookback)
    short_cross = _crossunder(ema9_arr, vwap_arr, lookback)

    if not long_cross and not short_cross:
        return _none("no_cross")

    direction = "LONG" if long_cross else "SHORT"
    score     = 50.0

    rsi_mid = getattr(C, 'RSI_MID', 50.0)
    rsi_ob  = getattr(C, 'RSI_OB',  70.0)
    rsi_os  = getattr(C, 'RSI_OS',  30.0)

    if direction == "LONG":
        rsi_ok = rsi > rsi_mid and rsi < rsi_ob
        if rsi_ok:
            score += 15 + (5 if rsi > 55 else 0)
    else:
        rsi_ok = rsi < rsi_mid and rsi > rsi_os
        if rsi_ok:
            score += 15 + (5 if rsi < 45 else 0)

    if getattr(C, 'RSI_REQUIRED', True) and not rsi_ok:
        return _none(f"rsi_fail(rsi={rsi:.1f} dir={direction})")

    if direction == "LONG":
        macd_ok = macd_hist > 0 or (macd_hist < 0 and macd_rising)
        if macd_hist > 0:
            score += 20 + (5 if float(macd_l[-1]) > 0 else 0)
        elif macd_rising:
            score += 8
    else:
        macd_ok = macd_hist < 0 or (macd_hist > 0 and not macd_rising)
        if macd_hist < 0:
            score += 20 + (5 if float(macd_l[-1]) < 0 else 0)
        elif not macd_rising:
            score += 8

    if getattr(C, 'MACD_REQUIRED', True) and not macd_ok:
        return _none(f"macd_fail(hist={macd_hist:.4f} dir={direction})")

    vol_mult = getattr(C, 'VOL_MIN_MULT', 1.3)
    vol_ok   = vol_ratio >= vol_mult
    if vol_ok:
        score += min((vol_ratio - 1) * 10, 10)
    elif getattr(C, 'VOL_REQUIRED', False):
        return _none(f"vol_fail(ratio={vol_ratio:.2f}<{vol_mult})")

    ema9  = float(ema9_arr[-1])
    ema21 = float(ema21_arr[-1])
    ema21_ok = (ema9 > ema21) if direction == "LONG" else (ema9 < ema21)
    if ema21_ok:
        score += 10
    elif getattr(C, 'EMA21_REQUIRED', False):
        return _none(f"ema21_fail")

    score = min(round(score, 1), 100.0)

    trail_mult = getattr(C, 'ATR_TRAIL_MULT', 2.0)
    tp1_mult   = getattr(C, 'TP1_ATR_MULT',   2.0)
    tp2_mult   = getattr(C, 'TP2_ATR_MULT',   4.0)

    if direction == "LONG":
        sl  = price - atr * trail_mult
        tp1 = price + atr * tp1_mult
        tp2 = price + atr * tp2_mult
    else:
        sl  = price + atr * trail_mult
        tp1 = price - atr * tp1_mult
        tp2 = price - atr * tp2_mult

    return EV_Signal(
        symbol=symbol, direction=direction, score=score,
        entry=price, sl=sl, tp1=tp1, tp2=tp2, atr=atr,
        rsi=rsi, macd_hist=macd_hist, vol_ratio=vol_ratio,
        reason="ok",
    )


# ═══════════════════════════════════════════════════════════════
# PROCESO POR SÍMBOLO — CON DEBUG LOGS EN TODOS LOS BLOQUEOS
# ═══════════════════════════════════════════════════════════════

async def _process_symbol(
    symbol: str, client: BingXClient,
    risk: RiskManager, pos_mgr: PositionManager,
    diag: dict, btc_klines: list = None,
) -> Optional[EV_Signal]:

    if pos_mgr.is_trading(symbol):
        diag["counts"]["already_trading"] += 1
        return None

    now = time.time()
    if symbol in _cb_blacklist and now - _cb_blacklist[symbol] < CB_COOLDOWN:
        diag["counts"]["cb_cooldown"] += 1
        return None

    try:
        klines = await client.get_klines(symbol, C.TIMEFRAME, 200)
    except Exception as e:
        log.debug("[%s] fetch error: %s", symbol, e)
        diag["counts"]["fetch_error"] += 1
        return None

    if len(klines) < 60:
        diag["counts"]["insufficient_data"] += 1
        return None

    sig = _analyze(symbol, klines)

    if sig.direction == "NONE":
        diag["counts"][sig.reason or "no_signal"] += 1
        return None

    if sig.score < getattr(C, 'MIN_SCORE', 50.0):
        diag["counts"]["score_bajo"] += 1
        return None

    diag["score_n"]   += 1
    diag["score_sum"] += sig.score
    if sig.score > diag["score_max"]:
        diag["score_max"]        = sig.score
        diag["score_max_symbol"] = symbol
        diag["score_max_dir"]    = sig.direction

    log.info(
        "[%s] 📊 %s score=%.1f rsi=%.1f macd_h=%.4f vol=%.2f×",
        symbol, sig.direction, sig.score, sig.rsi, sig.macd_hist, sig.vol_ratio,
    )

    if C.MODE == "SIGNAL":
        await tg.send(
            f"📊 *EMA9×VWAP* — `{symbol}` {sig.direction}\n"
            f"Entry: `{sig.entry:.6f}` | Score: `{sig.score:.1f}`\n"
            f"RSI: `{sig.rsi:.1f}` | MACD: `{sig.macd_hist:.4f}` | "
            f"Vol: `{sig.vol_ratio:.2f}×`\n"
            f"SL: `{sig.sl:.6f}` | TP1: `{sig.tp1:.6f}`"
        )
        diag["counts"]["signal_sent"] += 1
        return sig

    # ── LIVE ─────────────────────────────────────────────────────────────
    unrealized = await pos_mgr.get_unrealized_pnl()
    can, reason = await risk.can_trade(unrealized_pnl=unrealized)
    if not can:
        log.info("[%s] 🚫 BLOCKED can_trade: %s", symbol, reason)
        diag["counts"][f"risk_blocked({reason[:30]})"] += 1
        return None

    trade_confirmed = False
    dir_reserved    = False
    dir_token       = None
    btc_reserved    = False
    btc_token       = None
    btc_corr        = 0.0

    try:
        sym_ok, sym_reason = risk.symbol_allowed(symbol)
        if not sym_ok:
            log.info("[%s] 🚫 BLOCKED symbol_allowed: %s", symbol, sym_reason)
            diag["counts"]["symbol_blocked"] += 1
            return None

        dir_ok, dir_reason, dir_token = risk.direction_allowed(sig.direction)
        if not dir_ok:
            log.info("[%s] 🚫 BLOCKED direction: %s", symbol, dir_reason)
            diag["counts"]["correlation_blocked"] += 1
            return None
        dir_reserved = True

        if (btc_klines and _BTC_CORR_AVAILABLE and
                getattr(C, 'BTC_CORR_ENABLED', True) and symbol != "BTC-USDT"):
            btc_corr = compute_correlation(klines, btc_klines)
            btc_guard.threshold  = getattr(C, 'BTC_CORR_THRESHOLD', 0.5)
            btc_guard.window_sec = getattr(C, 'BTC_CORR_WINDOW_SEC', 1800)
            btc_guard.max_same   = getattr(C, 'BTC_CORR_MAX_SAME', 3)
            btc_reserved = abs(btc_corr) >= btc_guard.threshold
            if btc_reserved:
                btc_ok, btc_reason, btc_token = btc_guard.allowed(sig.direction, btc_corr)
                if not btc_ok:
                    log.info("[%s] 🚫 BLOCKED btc_corr: %s", symbol, btc_reason)
                    diag["counts"]["btc_corr_blocked"] += 1
                    btc_reserved = False
                    return None

        try:
            balance = await client.get_balance()
            log.info("[%s] balance=%.2f USDT", symbol, balance)
        except Exception as e:
            log.error("[%s] get_balance error: %s", symbol, e)
            return None
        if balance < 5.0:
            balance = C.CAPITAL

        qty = risk.kelly_position_size(
            balance, sig.entry, sig.sl, sig.score, "STD", symbol=symbol
        )
        log.info("[%s] qty calculado=%.6f entry=%.6f sl=%.6f notional=%.2f",
                 symbol, qty, sig.entry, sig.sl, qty * sig.entry)

        if qty <= 0:
            log.warning("[%s] 🚫 BLOCKED qty=0 — notional demasiado pequeño "
                        "(FIXED_NOTIONAL_USDT=%.1f MIN_NOTIONAL=%.1f)",
                        symbol,
                        getattr(C, 'FIXED_NOTIONAL_USDT', 0.0),
                        getattr(C, 'MIN_NOTIONAL_USDT', 10.0))
            diag["counts"]["qty_zero"] += 1
            return None

        # Entrada límite con fallback a market
        entry_resp = {}
        used_limit = False
        if getattr(C, 'LIMIT_ORDERS_ENABLED', True):
            lmt = await client.place_limit_entry(
                symbol, sig.direction, qty, sig.entry,
                sl_price=sig.sl, tp1_price=sig.tp1, tp2_price=sig.tp2,
                timeout_s=getattr(C, 'LIMIT_TIMEOUT_SECS', 15),
            )
            if lmt.get("code", -1) == 0:
                entry_resp = lmt
                used_limit = True
                log.info("[%s] Entrada LÍMITE OK ✅", symbol)
            else:
                log.info("[%s] Límite no llenado, intentando market...", symbol)

        if not used_limit:
            try:
                results = await client.open_trade(
                    symbol=symbol, direction=sig.direction, quantity=qty,
                    sl_price=sig.sl, tp1_price=sig.tp1, tp2_price=sig.tp2,
                )
            except Exception as e:
                log.error("[%s] open_trade error: %s", symbol, e)
                return None
            entry_resp = results.get("entry", {})

        if entry_resp.get("code", -1) != 0:
            log.error("[%s] 🚫 entrada rechazada code=%s: %s",
                      symbol, entry_resp.get("code"), entry_resp)
            return None

        order_id = str(
            entry_resp.get("data", {}).get("order", {}).get("orderId", "unknown")
            or entry_resp.get("data", {}).get("orderId", "unknown")
        )

        trade = OpenTrade(
            symbol=symbol, direction=sig.direction,
            entry=sig.entry, sl=sig.sl, tp1=sig.tp1, tp2=sig.tp2,
            qty=qty, atr=sig.atr, order_id=order_id,
        )
        await pos_mgr.register_trade(trade)
        await tg.send(
            f"✅ *TRADE ABIERTO* — `{symbol}` {sig.direction}\n"
            f"Entry: `{sig.entry:.6f}` qty=`{qty:.4f}` id=`{order_id}`\n"
            f"SL: `{sig.sl:.6f}` | TP1: `{sig.tp1:.6f}`\n"
            f"RSI: `{sig.rsi:.1f}` | MACD: `{sig.macd_hist:.4f}`"
        )
        trade_confirmed = True
        diag["counts"]["trade_opened"] += 1
        return sig

    finally:
        if not trade_confirmed:
            await risk.release_reservation()
            if dir_reserved:
                risk.release_direction_reservation(sig.direction, dir_token)
            if btc_reserved and _BTC_CORR_AVAILABLE:
                btc_guard.release(sig.direction, btc_corr, btc_token)


# ═══════════════════════════════════════════════════════════════
# LOOP PRINCIPAL
# ═══════════════════════════════════════════════════════════════

def _new_diag() -> dict:
    return {
        "counts": Counter(), "score_n": 0, "score_sum": 0.0,
        "score_max": 0.0, "score_max_symbol": "", "score_max_dir": "",
    }


async def scan_loop(client, risk, pos_mgr, complement=None, journal=None):
    log.info(
        "EMA9×VWAP Scanner | Modo=%s | TF=%s | "
        "MACD_req=%s RSI_req=%s VOL_req=%s EMA21_req=%s | "
        "FIXED_NOTIONAL=%.1f MIN_NOTIONAL=%.1f",
        C.MODE, C.TIMEFRAME,
        getattr(C, 'MACD_REQUIRED',  True),
        getattr(C, 'RSI_REQUIRED',   True),
        getattr(C, 'VOL_REQUIRED',   False),
        getattr(C, 'EMA21_REQUIRED', False),
        getattr(C, 'FIXED_NOTIONAL_USDT', 0.0),
        getattr(C, 'MIN_NOTIONAL_USDT',  10.0),
    )

    symbols:   list[str] = []
    iteration: int       = 0

    while True:
        start = time.time()
        iteration += 1
        diag = _new_diag()

        if iteration == 1 or iteration % 10 == 0 or not symbols:
            try:
                all_syms = await client.get_all_symbols()
                if all_syms:
                    symbols = all_syms
                    log.info("Símbolos activos: %d", len(symbols))
            except Exception as e:
                log.error("get_all_symbols error: %s", e)
                if not symbols:
                    await asyncio.sleep(30)
                    continue

        if not symbols:
            await asyncio.sleep(10)
            continue

        if iteration % 20 == 0:
            try:
                balance    = await client.get_balance()
                unrealized = await pos_mgr.get_unrealized_pnl()
                await tg.notify_status(
                    risk.status(unrealized_pnl=unrealized), balance, len(symbols)
                )
            except Exception:
                pass

        btc_klines = None
        if _BTC_CORR_AVAILABLE and getattr(C, 'BTC_CORR_ENABLED', True):
            try:
                btc_klines = await client.get_klines("BTC-USDT", C.TIMEFRAME, 80)
            except Exception:
                pass

        BATCH         = 20
        signals_found = 0
        for i in range(0, len(symbols), BATCH):
            batch   = symbols[i:i + BATCH]
            results = await asyncio.gather(
                *[_process_symbol(s, client, risk, pos_mgr, diag, btc_klines)
                  for s in batch],
                return_exceptions=True,
            )
            for r in results:
                if isinstance(r, EV_Signal) and r.direction != "NONE":
                    signals_found += 1
            await asyncio.sleep(0.2)

        elapsed = time.time() - start
        top5    = diag["counts"].most_common(8)
        avg_sc  = diag["score_sum"] / diag["score_n"] if diag["score_n"] else 0.0
        top_str = " | ".join(f"{k}={v}" for k, v in top5) if top5 else "—"

        log.info(
            "Iter %d | %d símbolos | %d señales | %.1fs | "
            "con_cruce=%d avg=%.1f max=%.1f(%s %s) | %s",
            iteration, len(symbols), signals_found, elapsed,
            diag["score_n"], avg_sc, diag["score_max"],
            diag["score_max_symbol"], diag["score_max_dir"], top_str,
        )

        await asyncio.sleep(max(0.0, C.SCAN_INTERVAL - elapsed))
