"""
QF×JP Bot v7.2 — Scanner
Fixes vs v7.1:
  - DIAGNÓSTICO DE RECHAZO: indicators.py ya calculaba sig.reason
    (no_tl_break, htf_not_aligned(x/y), insufficient_data, invalid_atr...)
    pero scanner.py lo descartaba siempre en el camino sig.direction=="NONE".
    Resultado: "0 señales" en los logs sin ninguna pista de POR QUÉ.
    Ahora se acumula un Counter por iteración completa y se loguea +
    se manda a Telegram cada N iteraciones, para saber exactamente
    qué puerta está bloqueando todo (TL break, HTF alignment, tier...).

FIX v7.1 (sin cambios):
  ✅ can_trade() recibe unrealized_pnl (PnL no realizado de PositionManager)
     para que el límite de pérdida diaria no ignore drawdown abierto.
"""
import asyncio
import logging
import time
from collections import Counter
from typing import Optional

import config as C
from bingx_client import BingXClient
from indicators import analyze, Signal, score_to_tier
from risk_manager import RiskManager
from position_manager import PositionManager, OpenTrade
import telegram_client as tg

log = logging.getLogger("scanner")

_cb_blacklist: dict[str, float] = {}
CB_COOLDOWN = 600


async def _fetch_all(client: BingXClient, symbol: str):
    results = await asyncio.gather(
        client.get_klines(symbol, C.TIMEFRAME,      200),
        client.get_klines(symbol, C.HTF_TIMEFRAME,  100),
        client.get_klines(symbol, C.HTF2_TIMEFRAME, 100),
        client.get_klines(symbol, C.HTF5_TIMEFRAME, 100),
        client.get_order_book(symbol, 10),
        client.get_funding_rate(symbol),
        return_exceptions=True,
    )
    def _l(r): return r if isinstance(r, list) else []
    def _d(r): return r if isinstance(r, dict) else {}
    def _f(r): return r if isinstance(r, float) else 0.0
    return _l(results[0]), _l(results[1]), _l(results[2]), _l(results[3]), \
           _d(results[4]), _f(results[5])


def _obi(ob: dict) -> float:
    try:
        bv = sum(float(b[1]) for b in ob.get("bids", [])[:5] if len(b) >= 2)
        av = sum(float(a[1]) for a in ob.get("asks", [])[:5] if len(a) >= 2)
        t  = bv + av
        return (bv - av) / t if t > 0 else 0.0
    except Exception:
        return 0.0


async def _process_symbol(symbol, client, risk, pos_mgr, diag: dict) -> Optional[Signal]:
    if pos_mgr.is_trading(symbol):
        diag["counts"]["already_trading"] += 1
        return None

    now = time.time()
    if symbol in _cb_blacklist and now - _cb_blacklist[symbol] < CB_COOLDOWN:
        diag["counts"]["cb_cooldown"] += 1
        return None

    try:
        k3m, k15m, k1h, k4h, ob, fr = await _fetch_all(client, symbol)
    except Exception as e:
        log.debug("[%s] fetch error: %s", symbol, e)
        diag["counts"]["fetch_error"] += 1
        return None

    if len(k3m) < 60:
        diag["counts"]["insufficient_data"] += 1
        return None

    obi = _obi(ob)

    try:
        sig = analyze(symbol, k3m, k15m, k1h, k4h, funding_rate=fr)
    except Exception as e:
        log.warning("[%s] analyze error: %s", symbol, e)
        diag["counts"]["analyze_error"] += 1
        return None

    if sig.direction == "NONE":
        # FIX v7.2: surface el motivo real (antes se perdía silenciosamente)
        diag["counts"][sig.reason or "no_direction"] += 1
        return None

    # Hubo dirección — registrar score para saber qué tan cerca estamos del umbral
    diag["score_n"]   += 1
    diag["score_sum"] += sig.score
    if sig.score > diag["score_max"]:
        diag["score_max"]        = sig.score
        diag["score_max_symbol"] = symbol
        diag["score_max_dir"]    = sig.direction

    # OBI boost
    if abs(obi) > 0.1:
        boost = 0.0
        if sig.direction == "SHORT" and obi < -0.1:
            boost = abs(obi) * 5
        elif sig.direction == "LONG" and obi > 0.1:
            boost = obi * 5
        if boost > 0:
            sig.score = min(sig.score + boost, 100.0)
            sig.tier  = score_to_tier(sig.score)

    if sig.circuit_breaker:
        _cb_blacklist[symbol] = now
        await tg.notify_circuit_breaker(symbol)
        diag["counts"]["circuit_breaker"] += 1
        return None

    if not risk.tier_ok(sig.tier):
        diag["counts"][f"tier_bajo({sig.tier})"] += 1
        return None

    diag["counts"]["signal_qualified"] += 1
    log.info("[%s] Señal %s tier=%s score=%.1f fr=%.4f",
             symbol, sig.direction, sig.tier, sig.score, fr)

    if C.MODE == "SIGNAL":
        await tg.notify_signal(sig)
        return sig

    # ── LIVE ──────────────────────────────────────────────────────────────────
    # FIX v7.1: incluir PnL no realizado en el chequeo de riesgo diario.
    # Sin esto, can_trade() solo veía PnL cerrado y seguía aprobando trades
    # nuevos mientras el drawdown no realizado ya superaba DAILY_LOSS_PCT.
    unrealized = await pos_mgr.get_unrealized_pnl()
    can, reason = await risk.can_trade(unrealized_pnl=unrealized)
    if not can:
        log.info("[%s] Bloqueado por risk: %s", symbol, reason)
        diag["counts"]["risk_blocked"] += 1
        return None

    # Cooldown por símbolo
    sym_ok, sym_reason = risk.symbol_allowed(symbol)
    if not sym_ok:
        log.debug("[%s] Bloqueado por símbolo: %s", symbol, sym_reason)
        diag["counts"]["symbol_blocked"] += 1
        return None

    # Correlation Guard — evita apilar varios LONG o SHORT simultáneos
    # que se mueven juntos (caso FHEU+XNY)
    dir_ok, dir_reason = risk.direction_allowed(sig.direction)
    if not dir_ok:
        log.info("[%s] Bloqueado por correlación: %s", symbol, dir_reason)
        diag["counts"]["correlation_blocked"] += 1
        return None

    try:
        balance = await client.get_balance()
    except Exception as e:
        log.error("[%s] get_balance error: %s", symbol, e)
        return None

    if balance < 5.0:
        log.warning("Balance=%.4f — usando CAPITAL=%.2f", balance, C.CAPITAL)
        balance = C.CAPITAL

    qty = risk.kelly_position_size(balance, sig.entry, sig.sl, sig.score, sig.tier)
    if qty <= 0:
        log.warning("[%s] qty=0, skip", symbol)
        return None

    log.info("[%s] qty=%.6f notional=%.2f USDT", symbol, qty, qty * sig.entry)
    await tg.notify_signal(sig)

    try:
        results = await client.open_trade(
            symbol=symbol, direction=sig.direction, quantity=qty,
            sl_price=sig.sl, tp1_price=sig.tp1, tp2_price=sig.tp2,
        )
    except Exception as e:
        log.error("[%s] open_trade error: %s", symbol, e)
        await tg.notify_error(f"open_trade({symbol})", str(e))
        return None

    entry_resp = results.get("entry", {})
    if entry_resp.get("code", -1) != 0:
        log.error("[%s] Entrada rechazada: %s", symbol, entry_resp)
        await tg.notify_error(f"entrada_rechazada({symbol})", str(entry_resp))
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
    await tg.notify_trade_opened(sig, qty, order_id)
    return sig


def _new_diag() -> dict:
    return {
        "counts":          Counter(),
        "score_n":         0,
        "score_sum":       0.0,
        "score_max":       0.0,
        "score_max_symbol": "",
        "score_max_dir":   "",
    }


async def scan_loop(client, risk, pos_mgr, complement=None):
    log.info("Scanner v7.2 | Modo=%s | Interval=%ds | Batch=20",
             C.MODE, C.SCAN_INTERVAL)
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
                    # Si complement activo: usar solo símbolos exclusivos de joyful-art
                    if complement and complement.get_exclusive_symbols():
                        symbols = complement.get_exclusive_symbols()
                        log.info("Modo EXCLUSIVO: %d símbolos (top por volumen)", len(symbols))
                    else:
                        symbols = all_syms
                        log.info("Símbolos activos: %d", len(symbols))
                else:
                    log.warning("get_all_symbols vacío (iter=%d)", iteration)
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
                balance = await client.get_balance()
                unrealized = await pos_mgr.get_unrealized_pnl()
                await tg.notify_status(risk.status(unrealized_pnl=unrealized), balance, len(symbols))
            except Exception:
                pass

        BATCH = 20
        signals_found = 0
        for i in range(0, len(symbols), BATCH):
            batch   = symbols[i:i+BATCH]
            results = await asyncio.gather(
                *[_process_symbol(s, client, risk, pos_mgr, diag) for s in batch],
                return_exceptions=True,
            )
            for r in results:
                if isinstance(r, Signal) and r.direction != "NONE":
                    signals_found += 1
            await asyncio.sleep(0.2)

        elapsed = time.time() - start

        # ── FIX v7.2: diagnóstico de rechazo ───────────────────────────────────
        top5     = diag["counts"].most_common(5)
        avg_sc   = diag["score_sum"] / diag["score_n"] if diag["score_n"] else 0.0
        top_str  = " | ".join(f"{k}={v}" for k, v in top5) if top5 else "—"

        log.info(
            "Iter %d | %d símbolos | %d señales | %.1fs | "
            "direccionales=%d avg_score=%.1f max_score=%.1f(%s %s) | %s",
            iteration, len(symbols), signals_found, elapsed,
            diag["score_n"], avg_sc, diag["score_max"],
            diag["score_max_symbol"], diag["score_max_dir"], top_str,
        )

        # Cada 5 iteraciones, mandar el diagnóstico también a Telegram
        # (más fácil de revisar desde el móvil que entrar a Railway)
        if iteration % 5 == 0 and signals_found == 0:
            try:
                await tg.notify_diagnostics(
                    iteration, len(symbols), diag["score_n"], avg_sc,
                    diag["score_max"], diag["score_max_symbol"], diag["score_max_dir"],
                    top5,
                )
            except Exception:
                pass

        await asyncio.sleep(max(0.0, C.SCAN_INTERVAL - elapsed))
