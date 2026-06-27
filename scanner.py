"""
EMA9×VWAP Bot — ema9_vwap_scanner.py v2
FIX: try/except global en bloque LIVE para capturar cualquier excepción silenciosa
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

log = logging.getLogger("ema9_vwap_scanner")
_cb_blacklist: dict[str, float] = {}
CB_COOLDOWN = 600


def _ema(arr, period):
    k = 2.0 / (period + 1)
    out = np.empty_like(arr, dtype=float)
    out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = arr[i] * k + out[i-1] * (1-k)
    return out

def _rma(arr, period):
    k = 1.0 / period
    out = np.empty_like(arr, dtype=float)
    out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = arr[i] * k + out[i-1] * (1-k)
    return out

def _vwap(arr):
    h, l, c, v = arr[:,2], arr[:,3], arr[:,4], arr[:,5]
    hlc3 = (h+l+c)/3.0
    return np.cumsum(hlc3*v) / (np.cumsum(v) + 1e-12)

def _atr(arr, period):
    h, l, c = arr[:,2], arr[:,3], arr[:,4]
    tr = np.maximum(h[1:]-l[1:], np.maximum(np.abs(h[1:]-c[:-1]), np.abs(l[1:]-c[:-1])))
    tr = np.concatenate([[tr[0]], tr])
    return _rma(tr, period)

def _rsi(c, period):
    diff = np.diff(c)
    gain = np.concatenate([[0], np.where(diff>0, diff, 0)])
    loss = np.concatenate([[0], np.where(diff<0, -diff, 0)])
    ag = _rma(gain, period)
    al = _rma(loss, period)
    rs = np.divide(ag, al+1e-12, out=np.ones_like(ag), where=al>0)
    return 100.0 - (100.0/(1.0+rs))

def _macd(c, fast, slow, signal):
    ml = _ema(c, fast) - _ema(c, slow)
    sl = _ema(ml, signal)
    return ml, sl, ml - sl

def _crossover(a, b, lookback=1):
    for i in range(1, lookback+2):
        if len(a) <= i: break
        if a[-i] > b[-i] and a[-(i+1)] <= b[-(i+1)]: return True
    return False

def _crossunder(a, b, lookback=1):
    for i in range(1, lookback+2):
        if len(a) <= i: break
        if a[-i] < b[-i] and a[-(i+1)] >= b[-(i+1)]: return True
    return False


@dataclass
class EV_Signal:
    symbol: str; direction: str; score: float
    entry: float; sl: float; tp1: float; tp2: float; atr: float
    rsi: float; macd_hist: float; vol_ratio: float; reason: str = ""


def _analyze(symbol, klines):
    def _none(r): return EV_Signal(symbol=symbol, direction="NONE", score=0,
        entry=0, sl=0, tp1=0, tp2=0, atr=0, rsi=50, macd_hist=0, vol_ratio=1, reason=r)

    if len(klines) < 60: return _none("insufficient_data")
    arr = np.array(klines, dtype=float)
    c, v = arr[:,4], arr[:,5]

    ema9_arr  = _ema(c, getattr(C,'EMA9_PERIOD',9))
    ema21_arr = _ema(c, getattr(C,'EMA21_PERIOD',21))
    vwap_arr  = _vwap(arr)
    atr_arr   = _atr(arr, getattr(C,'ATR_LEN',14))
    atr   = float(atr_arr[-1])
    price = float(c[-1])
    if atr <= 0: return _none("invalid_atr")

    rsi_arr    = _rsi(c, getattr(C,'RSI_PERIOD',14))
    rsi        = float(rsi_arr[-1])
    macd_l, _, hist = _macd(c, getattr(C,'MACD_FAST',12), getattr(C,'MACD_SLOW',26), getattr(C,'MACD_SIGNAL',9))
    macd_hist  = float(hist[-1])
    macd_rising = hist[-1] > hist[-2] if len(hist)>1 else True

    vol_period = getattr(C,'VOL_MA_PERIOD',20)
    vol_ratio  = float(v[-1]) / (float(np.mean(v[-vol_period:])) + 1e-12) if len(v)>=vol_period else 1.0

    lookback    = getattr(C,'CROSS_LOOKBACK',3)
    long_cross  = _crossover(ema9_arr, vwap_arr, lookback)
    short_cross = _crossunder(ema9_arr, vwap_arr, lookback)
    if not long_cross and not short_cross: return _none("no_cross")

    direction = "LONG" if long_cross else "SHORT"
    score = 50.0
    rsi_mid = getattr(C,'RSI_MID',50.0)
    rsi_ob  = getattr(C,'RSI_OB',75.0)
    rsi_os  = getattr(C,'RSI_OS',25.0)

    if direction == "LONG":
        rsi_ok = rsi > rsi_mid and rsi < rsi_ob
        if rsi_ok: score += 20
    else:
        rsi_ok = rsi < rsi_mid and rsi > rsi_os
        if rsi_ok: score += 20

    if getattr(C,'RSI_REQUIRED',True) and not rsi_ok:
        return _none(f"rsi_fail(rsi={rsi:.1f})")

    if direction == "LONG":
        macd_ok = macd_hist > 0 or macd_rising
        if macd_hist > 0: score += 20
        elif macd_rising: score += 8
    else:
        macd_ok = macd_hist < 0 or not macd_rising
        if macd_hist < 0: score += 20
        elif not macd_rising: score += 8

    if getattr(C,'MACD_REQUIRED',True) and not macd_ok:
        return _none(f"macd_fail(hist={macd_hist:.4f})")

    vol_mult = getattr(C,'VOL_MIN_MULT',1.3)
    if vol_ratio >= vol_mult: score += min((vol_ratio-1)*10, 10)
    elif getattr(C,'VOL_REQUIRED',False): return _none(f"vol_fail({vol_ratio:.2f})")

    ema9, ema21 = float(ema9_arr[-1]), float(ema21_arr[-1])
    if (direction=="LONG" and ema9>ema21) or (direction=="SHORT" and ema9<ema21): score += 10
    elif getattr(C,'EMA21_REQUIRED',False): return _none("ema21_fail")

    score = min(round(score,1), 100.0)
    mult = getattr(C,'ATR_TRAIL_MULT',2.0)
    tp1m = getattr(C,'TP1_ATR_MULT',2.0)
    tp2m = getattr(C,'TP2_ATR_MULT',4.0)

    if direction == "LONG":
        sl, tp1, tp2 = price-atr*mult, price+atr*tp1m, price+atr*tp2m
    else:
        sl, tp1, tp2 = price+atr*mult, price-atr*tp1m, price-atr*tp2m

    return EV_Signal(symbol=symbol, direction=direction, score=score,
        entry=price, sl=sl, tp1=tp1, tp2=tp2, atr=atr,
        rsi=rsi, macd_hist=macd_hist, vol_ratio=vol_ratio, reason="ok")


async def _process_symbol(symbol, client, risk, pos_mgr, diag, btc_klines=None):
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
        diag["counts"]["fetch_error"] += 1
        return None

    if len(klines) < 60:
        diag["counts"]["insufficient_data"] += 1
        return None

    sig = _analyze(symbol, klines)
    if sig.direction == "NONE":
        diag["counts"][sig.reason or "no_signal"] += 1
        return None

    if sig.score < getattr(C,'MIN_SCORE',50.0):
        diag["counts"]["score_bajo"] += 1
        return None

    diag["score_n"]   += 1
    diag["score_sum"] += sig.score
    if sig.score > diag["score_max"]:
        diag["score_max"] = sig.score
        diag["score_max_symbol"] = symbol
        diag["score_max_dir"]    = sig.direction

    log.info("[%s] 📊 %s score=%.1f rsi=%.1f macd_h=%.4f vol=%.2f×",
             symbol, sig.direction, sig.score, sig.rsi, sig.macd_hist, sig.vol_ratio)

    if C.MODE == "SIGNAL":
        try:
            await tg.send(
                f"📊 *EMA9×VWAP* `{symbol}` {sig.direction} score={sig.score:.0f}\n"
                f"RSI:{sig.rsi:.1f} MACD:{sig.macd_hist:.4f} Vol:{sig.vol_ratio:.2f}x\n"
                f"Entry:{sig.entry:.6f} SL:{sig.sl:.6f} TP1:{sig.tp1:.6f}"
            )
        except Exception as e:
            log.warning("[%s] tg.send error: %s", symbol, e)
        diag["counts"]["signal_sent"] += 1
        return sig

    # ── LIVE — con try/except completo para capturar cualquier error ──────────
    try:
        unrealized = await pos_mgr.get_unrealized_pnl()
        can, reason = await risk.can_trade(unrealized_pnl=unrealized)
        if not can:
            log.info("[%s] 🚫 can_trade: %s", symbol, reason)
            diag["counts"][f"risk({reason[:25]})"] += 1
            return None

        log.info("[%s] ✅ can_trade OK — ejecutando trade...", symbol)

        sym_ok, sym_reason = risk.symbol_allowed(symbol)
        if not sym_ok:
            log.info("[%s] 🚫 symbol_allowed: %s", symbol, sym_reason)
            diag["counts"]["symbol_blocked"] += 1
            await risk.release_reservation()
            return None

        result_dir = risk.direction_allowed(sig.direction)
        if len(result_dir) == 3:
            dir_ok, dir_reason, dir_token = result_dir
        else:
            dir_ok, dir_reason = result_dir
            dir_token = None
        if not dir_ok:
            log.info("[%s] 🚫 direction: %s", symbol, dir_reason)
            diag["counts"]["corr_blocked"] += 1
            await risk.release_reservation()
            return None

        # BTC correlation
        btc_corr, btc_token, btc_reserved = 0.0, None, False
        if btc_klines and _BTC_CORR_AVAILABLE and getattr(C,'BTC_CORR_ENABLED',True) and symbol != "BTC-USDT":
            try:
                btc_corr = compute_correlation(klines, btc_klines)
                btc_guard.threshold  = getattr(C,'BTC_CORR_THRESHOLD',0.5)
                btc_guard.window_sec = getattr(C,'BTC_CORR_WINDOW_SEC',1800)
                btc_guard.max_same   = getattr(C,'BTC_CORR_MAX_SAME',3)
                if abs(btc_corr) >= btc_guard.threshold:
                    btc_ok, btc_r, btc_token = btc_guard.allowed(sig.direction, btc_corr)
                    btc_reserved = True
                    if not btc_ok:
                        log.info("[%s] 🚫 btc_corr: %s", symbol, btc_r)
                        diag["counts"]["btc_blocked"] += 1
                        await risk.release_reservation()
                        try:
                risk.release_direction_reservation(sig.direction, dir_token)
            except AttributeError:
                pass
                        return None
            except Exception as e:
                log.warning("[%s] btc_corr error (ignorado): %s", symbol, e)

        try:
            balance = await client.get_balance()
            log.info("[%s] balance=%.2f", symbol, balance)
        except Exception as e:
            log.error("[%s] get_balance error: %s", symbol, e)
            await risk.release_reservation()
            try:
                risk.release_direction_reservation(sig.direction, dir_token)
            except AttributeError:
                pass
            return None
        if balance < 5.0:
            balance = C.CAPITAL

        try:
            qty = risk.kelly_position_size(
                balance, sig.entry, sig.sl, sig.score, "STD", symbol=symbol
            )
        except Exception as e:
            log.error("[%s] kelly_position_size error: %s", symbol, e)
            diag["counts"]["kelly_error"] += 1
            await risk.release_reservation()
            try:
                risk.release_direction_reservation(sig.direction, dir_token)
            except AttributeError:
                pass
            return None

        log.info("[%s] qty=%.6f notional=%.2f USDT entry=%.6f",
                 symbol, qty, qty*sig.entry, sig.entry)

        if qty <= 0:
            log.warning("[%s] 🚫 qty=0 — FIXED_NOTIONAL=%.1f MIN_NOTIONAL=%.1f balance=%.2f",
                        symbol, getattr(C,'FIXED_NOTIONAL_USDT',0), getattr(C,'MIN_NOTIONAL_USDT',10), balance)
            diag["counts"]["qty_zero"] += 1
            await risk.release_reservation()
            try:
                risk.release_direction_reservation(sig.direction, dir_token)
            except AttributeError:
                pass
            return None

        # Entrada
        entry_resp = {}
        used_limit = False
        if getattr(C,'LIMIT_ORDERS_ENABLED',True):
            try:
                lmt = await client.place_limit_entry(
                    symbol, sig.direction, qty, sig.entry,
                    sl_price=sig.sl, tp1_price=sig.tp1, tp2_price=sig.tp2,
                    timeout_s=getattr(C,'LIMIT_TIMEOUT_SECS',15),
                )
                if lmt.get("code",-1) == 0:
                    entry_resp = lmt
                    used_limit = True
                    log.info("[%s] ✅ límite OK", symbol)
            except Exception as e:
                log.warning("[%s] place_limit_entry error: %s", symbol, e)

        if not used_limit:
            try:
                results = await client.open_trade(
                    symbol=symbol, direction=sig.direction, quantity=qty,
                    sl_price=sig.sl, tp1_price=sig.tp1, tp2_price=sig.tp2,
                )
                entry_resp = results.get("entry", {})
            except Exception as e:
                log.error("[%s] open_trade error: %s", symbol, e)
                await risk.release_reservation()
                try:
                risk.release_direction_reservation(sig.direction, dir_token)
            except AttributeError:
                pass
                if btc_reserved and _BTC_CORR_AVAILABLE:
                    btc_guard.release(sig.direction, btc_corr, btc_token)
                return None

        if entry_resp.get("code",-1) != 0:
            log.error("[%s] 🚫 entrada rechazada: %s", symbol, entry_resp)
            diag["counts"]["entrada_rechazada"] += 1
            await risk.release_reservation()
            try:
                risk.release_direction_reservation(sig.direction, dir_token)
            except AttributeError:
                pass
            if btc_reserved and _BTC_CORR_AVAILABLE:
                btc_guard.release(sig.direction, btc_corr, btc_token)
            return None

        order_id = str(
            entry_resp.get("data",{}).get("order",{}).get("orderId","unknown")
            or entry_resp.get("data",{}).get("orderId","unknown")
        )
        trade = OpenTrade(
            symbol=symbol, direction=sig.direction,
            entry=sig.entry, sl=sig.sl, tp1=sig.tp1, tp2=sig.tp2,
            qty=qty, atr=sig.atr, order_id=order_id,
        )
        await pos_mgr.register_trade(trade)
        try:
            await tg.send(
                f"✅ *TRADE* `{symbol}` {sig.direction}\n"
                f"Entry:{sig.entry:.6f} qty:{qty:.4f} id:{order_id}\n"
                f"SL:{sig.sl:.6f} TP1:{sig.tp1:.6f}"
            )
        except Exception:
            pass
        diag["counts"]["trade_opened"] += 1
        return sig

    except Exception as e:
        log.error("[%s] ❌ EXCEPCIÓN en bloque LIVE: %s — %s",
                  symbol, type(e).__name__, e, exc_info=True)
        diag["counts"][f"exception({type(e).__name__})"] += 1
        try:
            await risk.release_reservation()
        except Exception:
            pass
        return None


def _new_diag():
    return {"counts": Counter(), "score_n": 0, "score_sum": 0.0,
            "score_max": 0.0, "score_max_symbol": "", "score_max_dir": ""}


async def scan_loop(client, risk, pos_mgr, complement=None, journal=None):
    log.info(
        "EMA9×VWAP Scanner v2 | Modo=%s | TF=%s | "
        "MACD=%s RSI=%s VOL=%s | FIXED=%.1f MIN_NOT=%.1f RSI_OB=%.0f RSI_OS=%.0f",
        C.MODE, C.TIMEFRAME,
        getattr(C,'MACD_REQUIRED',True), getattr(C,'RSI_REQUIRED',True),
        getattr(C,'VOL_REQUIRED',False),
        getattr(C,'FIXED_NOTIONAL_USDT',0.0), getattr(C,'MIN_NOTIONAL_USDT',10.0),
        getattr(C,'RSI_OB',75.0), getattr(C,'RSI_OS',25.0),
    )

    symbols:   list[str] = []
    iteration: int       = 0

    while True:
        start = time.time()
        iteration += 1
        diag = _new_diag()

        if iteration == 1 or iteration % 10 == 0 or not symbols:
            try:
                syms = await client.get_all_symbols()
                if syms:
                    symbols = syms
                    log.info("Símbolos: %d", len(symbols))
            except Exception as e:
                log.error("get_all_symbols: %s", e)
                if not symbols:
                    await asyncio.sleep(30)
                    continue

        if not symbols:
            await asyncio.sleep(10)
            continue

        btc_klines = None
        if _BTC_CORR_AVAILABLE and getattr(C,'BTC_CORR_ENABLED',True):
            try:
                btc_klines = await client.get_klines("BTC-USDT", C.TIMEFRAME, 80)
            except Exception:
                pass

        signals_found = 0
        for i in range(0, len(symbols), 20):
            batch = symbols[i:i+20]
            results = await asyncio.gather(
                *[_process_symbol(s, client, risk, pos_mgr, diag, btc_klines) for s in batch],
                return_exceptions=True,
            )
            for r in results:
                if isinstance(r, Exception):
                    log.error("asyncio.gather excepción: %s", r)
                elif isinstance(r, EV_Signal) and r.direction != "NONE":
                    signals_found += 1
            await asyncio.sleep(0.2)

        elapsed = time.time() - start
        top8    = diag["counts"].most_common(8)
        avg_sc  = diag["score_sum"] / diag["score_n"] if diag["score_n"] else 0.0
        top_str = " | ".join(f"{k}={v}" for k,v in top8) if top8 else "—"

        log.info("Iter %d | %d símbolos | %d señales | %.1fs | cruce=%d avg=%.1f max=%.1f(%s %s) | %s",
                 iteration, len(symbols), signals_found, elapsed,
                 diag["score_n"], avg_sc, diag["score_max"],
                 diag["score_max_symbol"], diag["score_max_dir"], top_str)

        await asyncio.sleep(max(0.0, C.SCAN_INTERVAL - elapsed))
