"""
QF×JP Bot v6.5.2 — Scanner con anticipación
Nuevas funciones:
  [ANT] Pre-señal: notifica símbolos calientes antes de cruzar umbral
  [ANT] Hot queue: prioriza símbolos con pre_score alto en siguiente ciclo
  [ANT] notify_pre_signal: Telegram con ⚡ pre-alerta
"""
import asyncio
import logging
import time
from typing import Optional

import config as C
from bingx_client import BingXClient
from indicators import analyze, Signal, score_to_tier
from risk_manager import RiskManager
from position_manager import PositionManager, OpenTrade
import telegram_client as tg

log = logging.getLogger("scanner")

_cb_blacklist:  dict[str, float] = {}
_hot_symbols:   dict[str, float] = {}  # [ANT] symbol → pre_score reciente
CB_COOLDOWN  = 600
HOT_DECAY    = 300   # segundos hasta olvidar pre-señal


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
    return (_l(results[0]), _l(results[1]), _l(results[2]), _l(results[3]),
            _d(results[4]), _f(results[5]))


def _obi(ob: dict) -> float:
    try:
        bv = sum(float(b[1]) for b in ob.get("bids", [])[:5] if len(b) >= 2)
        av = sum(float(a[1]) for a in ob.get("asks", [])[:5] if len(a) >= 2)
        t  = bv + av
        return (bv - av) / t if t > 0 else 0.0
    except Exception:
        return 0.0


async def _process_symbol(symbol, client, risk, pos_mgr) -> Optional[Signal]:
    if pos_mgr.is_trading(symbol):
        return None

    now = time.time()
    if symbol in _cb_blacklist and now - _cb_blacklist[symbol] < CB_COOLDOWN:
        return None

    try:
        k3m, k15m, k1h, k4h, ob, fr = await _fetch_all(client, symbol)
    except Exception as e:
        log.debug("[%s] fetch error: %s", symbol, e)
        return None

    if len(k3m) < 60:
        return None

    obi = _obi(ob)

    try:
        sig = analyze(symbol, k3m, k15m, k1h, k4h, funding_rate=fr)
    except Exception as e:
        log.warning("[%s] analyze error: %s", symbol, e)
        return None

    if sig.direction == "NONE":
        return None

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
        return None

    # [ANT] Pre-señal: score insuficiente pero pre_score alto
    if sig.score < C.MIN_SCORE and sig.pre_score >= 60:
        _hot_symbols[symbol] = now  # marcar como caliente
        log.info("[%s] PRE-SEÑAL %s pre=%.1f score=%.1f sweep=%s sq_fire=%s",
                 symbol, sig.direction, sig.pre_score, sig.score,
                 sig.sweep, sig.squeeze_fire)
        await tg.notify_pre_signal(sig)
        return None

    if not risk.tier_ok(sig.tier):
        return None

    log.info("[%s] Señal %s tier=%s score=%.1f pre=%.1f sweep=%s fr=%.4f",
             symbol, sig.direction, sig.tier, sig.score, sig.pre_score,
             sig.sweep, fr)

    if C.MODE == "SIGNAL":
        await tg.notify_signal(sig)
        return sig

    # ── LIVE ──────────────────────────────────────────────────────────────────
    can, reason = await risk.can_trade()
    if not can:
        log.info("[%s] Bloqueado por risk: %s", symbol, reason)
        return None

    sym_ok, sym_reason = risk.symbol_allowed(symbol)
    if not sym_ok:
        log.debug("[%s] Bloqueado por símbolo: %s", symbol, sym_reason)
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

    # [ANT] Limpiar de hot_symbols al abrir trade
    _hot_symbols.pop(symbol, None)
    return sig


async def scan_loop(client, risk, pos_mgr):
    log.info("Scanner v6.5.2 | Modo=%s | Interval=%ds | Batch=20",
             C.MODE, C.SCAN_INTERVAL)
    symbols:   list[str] = []
    iteration: int       = 0

    while True:
        start = time.time()
        iteration += 1

        if iteration == 1 or iteration % 10 == 0 or not symbols:
            try:
                new = await client.get_all_symbols()
                if new:
                    symbols = new
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
                await tg.notify_status(risk.status(), balance, len(symbols))
            except Exception:
                pass

        # [ANT] Limpiar hot_symbols expirados
        now_ts = time.time()
        expired = [s for s, ts in _hot_symbols.items() if now_ts - ts > HOT_DECAY]
        for s in expired:
            _hot_symbols.pop(s, None)

        # [ANT] Ordenar: hot symbols primero → respuesta más rápida
        hot  = [s for s in symbols if s in _hot_symbols]
        cold = [s for s in symbols if s not in _hot_symbols]
        symbols_ordered = hot + cold

        BATCH = 20
        signals_found = 0
        for i in range(0, len(symbols_ordered), BATCH):
            batch   = symbols_ordered[i:i+BATCH]
            results = await asyncio.gather(
                *[_process_symbol(s, client, risk, pos_mgr) for s in batch],
                return_exceptions=True,
            )
            for r in results:
                if isinstance(r, Signal) and r.direction != "NONE":
                    signals_found += 1
            await asyncio.sleep(0.2)

        elapsed = time.time() - start
        hot_count = len(_hot_symbols)
        log.info("Iter %d | %d símbolos | %d señales | %d hot | %.1fs",
                 iteration, len(symbols), signals_found, hot_count, elapsed)

        await asyncio.sleep(max(0.0, C.SCAN_INTERVAL - elapsed))
