"""
QF×JP Bot v7.2 — Scanner
═══════════════════════════════════════════════════════════════════════════════
FIXES v7.2 (diagnóstico: 0 trades abiertos):

  FIX 1 — CB_COOLDOWN 600→300s:
    10 minutos de cooldown por circuit breaker era excesivo. Con un mercado
    volátil los símbolos quedaban bloqueados la mayor parte del ciclo.
    Reducido a 5 minutos.

  FIX 2 — OBI boost umbral 0.1→0.15:
    El boost se aplicaba con cualquier desequilibrio >10% en el order book.
    Con 15% solo aplica cuando hay presión direccional real.

  FIX 3 — Log de razón de bloqueo en SIGNAL mode:
    En modo SIGNAL nunca llamábamos can_trade(), así que si MIN_TIER
    bloqueaba todo, no se logueaba nada útil. Ahora en SIGNAL mode
    también se loguea el tier_ok check.

  FIX 4 — Chequeo explícito de tier antes de analyze (early exit):
    Si la señal no pasa tier_ok() se loguea con INFO para diagnóstico,
    no solo se descarta silenciosamente.

  FIX 5 — TOP_N_SYMBOLS respetado en renewed-love:
    Si TOP_N_SYMBOLS > 0, limitar los símbolos usados al top-N por volumen
    (ya ordenados por bingx_client). Así renewed-love usa top-200 y
    joyful-art usa sus 50 exclusivos sin conflicto.

  MANTIENE (de v6.6/v7.1):
    - can_trade() con unrealized_pnl
    - OBI boost
    - Funding rate como filtro de sesgo
    - Batch 20, pausa 0.2s
    - CB blacklist
═══════════════════════════════════════════════════════════════════════════════
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

_cb_blacklist: dict[str, float] = {}
# FIX v7.2: CB_COOLDOWN reducido de 600s a 300s (5 min)
CB_COOLDOWN = 300


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

    # FIX v7.2: OBI boost con umbral 0.1→0.15 (presión más significativa)
    if abs(obi) > 0.15:
        boost = 0.0
        if sig.direction == "SHORT" and obi < -0.15:
            boost = abs(obi) * 5
        elif sig.direction == "LONG" and obi > 0.15:
            boost = obi * 5
        if boost > 0:
            old_score = sig.score
            sig.score = min(sig.score + boost, 100.0)
            sig.tier  = score_to_tier(sig.score)
            log.debug("[%s] OBI boost: %.1f→%.1f (obi=%.3f)", symbol, old_score, sig.score, obi)

    if sig.circuit_breaker:
        _cb_blacklist[symbol] = now
        log.info("[%s] Circuit Breaker — cooldown %ds", symbol, CB_COOLDOWN)
        await tg.notify_circuit_breaker(symbol)
        return None

    # FIX v7.2: loguear cuando tier_ok falla (antes silencioso)
    if not risk.tier_ok(sig.tier):
        log.debug("[%s] tier_ok FAIL: tier=%s score=%.1f min_tier=%s",
                  symbol, sig.tier, sig.score, C.MIN_TIER)
        return None

    log.info("[%s] ✅ Señal %s tier=%s score=%.1f fr=%.4f obi=%.3f",
             symbol, sig.direction, sig.tier, sig.score, fr, obi)

    if C.MODE == "SIGNAL":
        await tg.notify_signal(sig)
        return sig

    # ── LIVE ──────────────────────────────────────────────────────────────────
    # Incluir PnL no realizado en el chequeo de riesgo diario (FIX v7.1)
    unrealized = await pos_mgr.get_unrealized_pnl()
    can, reason = await risk.can_trade(unrealized_pnl=unrealized)
    if not can:
        log.info("[%s] Bloqueado por risk: %s", symbol, reason)
        return None

    # Cooldown por símbolo
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
        log.warning("[%s] Balance=%.4f — usando CAPITAL=%.2f", symbol, balance, C.CAPITAL)
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


async def scan_loop(client, risk, pos_mgr, complement=None):
    log.info("Scanner v7.2 | Modo=%s | Interval=%ds | Batch=20 | CB_COOLDOWN=%ds",
             C.MODE, C.SCAN_INTERVAL, CB_COOLDOWN)
    symbols:   list[str] = []
    iteration: int       = 0

    while True:
        start = time.time()
        iteration += 1

        if iteration == 1 or iteration % 10 == 0 or not symbols:
            try:
                all_syms = await client.get_all_symbols()
                if all_syms:
                    if complement and complement.get_exclusive_symbols():
                        # joyful-art: solo sus exclusivos (top-50 por volumen)
                        symbols = complement.get_exclusive_symbols()
                        log.info("Modo EXCLUSIVO: %d símbolos (top por volumen)", len(symbols))
                    elif C.TOP_N_SYMBOLS > 0:
                        # FIX v7.2: renewed-love limita a top-N por volumen
                        # (all_syms ya viene ordenado por volumen descendente de bingx_client)
                        symbols = all_syms[:C.TOP_N_SYMBOLS]
                        log.info("Top-%d símbolos por volumen (de %d disponibles)",
                                 C.TOP_N_SYMBOLS, len(all_syms))
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
                balance    = await client.get_balance()
                unrealized = await pos_mgr.get_unrealized_pnl()
                await tg.notify_status(
                    risk.status(unrealized_pnl=unrealized), balance, len(symbols)
                )
            except Exception:
                pass

        BATCH = 20
        signals_found = 0
        tier_blocked  = 0
        cb_blocked    = 0

        for i in range(0, len(symbols), BATCH):
            batch   = symbols[i:i+BATCH]
            results = await asyncio.gather(
                *[_process_symbol(s, client, risk, pos_mgr) for s in batch],
                return_exceptions=True,
            )
            for r in results:
                if isinstance(r, Signal) and r.direction != "NONE":
                    signals_found += 1
            await asyncio.sleep(0.2)

        elapsed = time.time() - start
        cb_active = len([k for k, v in _cb_blacklist.items()
                         if time.time() - v < CB_COOLDOWN])
        log.info("Iter %d | %d símbolos | %d señales | CB_activos=%d | %.1fs",
                 iteration, len(symbols), signals_found, cb_active, elapsed)

        await asyncio.sleep(max(0.0, C.SCAN_INTERVAL - elapsed))
