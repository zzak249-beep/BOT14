"""
QF×JP Bot v6.6 — Scanner CORREGIDO + MEJORAS
Fixes v6.5:
  - symbol_allowed check (cooldown + límite/día)
  - OBI boost desde order book
  - Funding rate como filtro de sesgo
  - Batch 20, pausa 0.2s

Mejoras v6.6:
  - Item 1: OBI ahora usa C.OBI_DEPTH niveles (antes fijo a 5, default 20)
    para una lectura más representativa de la presión real del libro.
  - Item 4: filtro de funding extremo BLOQUEANTE (distinto del bonus de
    composite_score) — no abre en la dirección ya abarrotada. Filtro de
    spread mínimo de liquidez — descarta pares con spread bid-ask >
    MAX_SPREAD_PCT, evita ejecuciones lejos del precio esperado en
    altcoins de baja liquidez.
  - Item 5: registro estructurado de cada cierre en _trade_log (memoria,
    últimos 200) vía log_closed_trade(), consumido por el endpoint
    /stats en main.py para win-rate por símbolo/tier/hora.
"""
import asyncio
import logging
import time
from collections import deque
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

# ── v6.6 Item 5: registro de trades cerrados para /stats ──────────────────────
# Deque acotada en memoria (no persiste entre redeploys — suficiente para
# una vista táctica de las últimas sesiones; si se quiere histórico real
# habría que persistir a disco/DB, fuera de alcance de este cambio).
_trade_log: deque = deque(maxlen=200)


def log_closed_trade(symbol: str, tier: str, score: float, direction: str,
                      pnl: float, reason: str, hold_minutes: float) -> None:
    """Registra un trade cerrado para agregación en /stats."""
    _trade_log.append({
        "ts":           time.time(),
        "symbol":       symbol,
        "tier":         tier,
        "score":        score,
        "direction":    direction,
        "pnl":          pnl,
        "reason":       reason,
        "hold_minutes": round(hold_minutes, 1),
        "hour_utc":     time.gmtime().tm_hour,
    })


def get_stats() -> dict:
    """
    Agrega _trade_log en win-rate por símbolo, por tier y por hora UTC.
    Consumido por GET /stats en main.py.
    """
    entries = list(_trade_log)
    if not entries:
        return {"count": 0, "by_symbol": {}, "by_tier": {}, "by_hour": {}, "total_pnl": 0.0}

    def _agg(key_fn):
        groups: dict = {}
        for e in entries:
            k = key_fn(e)
            g = groups.setdefault(k, {"trades": 0, "wins": 0, "pnl": 0.0})
            g["trades"] += 1
            g["pnl"]    += e["pnl"]
            if e["pnl"] > 0:
                g["wins"] += 1
        for g in groups.values():
            g["win_rate"] = round(g["wins"] / g["trades"] * 100, 1) if g["trades"] else 0.0
            g["pnl"]      = round(g["pnl"], 4)
        return groups

    return {
        "count":     len(entries),
        "total_pnl": round(sum(e["pnl"] for e in entries), 4),
        "by_symbol": _agg(lambda e: e["symbol"]),
        "by_tier":   _agg(lambda e: e["tier"]),
        "by_hour":   _agg(lambda e: e["hour_utc"]),
    }


async def _fetch_all(client: BingXClient, symbol: str):
    results = await asyncio.gather(
        client.get_klines(symbol, C.TIMEFRAME,      200),
        client.get_klines(symbol, C.HTF_TIMEFRAME,  100),
        client.get_klines(symbol, C.HTF2_TIMEFRAME, 100),
        client.get_klines(symbol, C.HTF5_TIMEFRAME, 100),
        client.get_order_book(symbol, C.OBI_DEPTH),
        client.get_funding_rate(symbol),
        return_exceptions=True,
    )
    def _l(r): return r if isinstance(r, list) else []
    def _d(r): return r if isinstance(r, dict) else {}
    def _f(r): return r if isinstance(r, float) else 0.0
    return _l(results[0]), _l(results[1]), _l(results[2]), _l(results[3]), \
           _d(results[4]), _f(results[5])


def _obi(ob: dict, depth: int = 20) -> float:
    """v6.6: profundidad configurable (antes fijo a 5)."""
    try:
        bv = sum(float(b[1]) for b in ob.get("bids", [])[:depth] if len(b) >= 2)
        av = sum(float(a[1]) for a in ob.get("asks", [])[:depth] if len(a) >= 2)
        t  = bv + av
        return (bv - av) / t if t > 0 else 0.0
    except Exception:
        return 0.0


def _spread_pct(ob: dict) -> float:
    """
    v6.6 Item 4: spread bid-ask como % del mid price. Devuelve un valor
    alto (999.0) si no se puede calcular, para que el filtro de
    MAX_SPREAD_PCT descarte el símbolo de forma segura ante datos
    incompletos en vez de asumir liquidez buena por defecto.
    """
    try:
        bids = ob.get("bids", [])
        asks = ob.get("asks", [])
        if not bids or not asks:
            return 999.0
        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        mid = (best_bid + best_ask) / 2
        if mid <= 0:
            return 999.0
        return (best_ask - best_bid) / mid * 100
    except Exception:
        return 999.0


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

    obi    = _obi(ob, depth=C.OBI_DEPTH)
    spread = _spread_pct(ob)

    # v6.6 Item 4: spread mínimo de liquidez — descarta antes de gastar
    # análisis en un par donde la ejecución real se alejaría del precio
    # esperado.
    if spread > C.MAX_SPREAD_PCT:
        log.debug("[%s] spread=%.3f%% > MAX_SPREAD_PCT=%.3f%% — skip",
                  symbol, spread, C.MAX_SPREAD_PCT)
        return None

    try:
        sig = analyze(symbol, k3m, k15m, k1h, k4h, funding_rate=fr)
    except Exception as e:
        log.warning("[%s] analyze error: %s", symbol, e)
        return None

    if sig.direction == "NONE":
        return None

    # v6.6 Item 4: funding extremo BLOQUEANTE (distinto del bonus de
    # composite_score). Si el funding ya está muy cargado a favor de
    # nuestra dirección, el lado opuesto está pagando mucho por mantenerse
    # — eso indica que el movimiento puede estar exhausto/a punto de
    # revertir por presión de financiación, no que tengamos más edge.
    # Funding muy positivo (longs pagan) -> bloquea abrir LONG (perseguir
    # un long ya masificado). Funding muy negativo -> bloquea SHORT.
    if sig.direction == "LONG" and fr > C.FUNDING_RATE_MAX_ABS:
        log.debug("[%s] funding=%.4f > +%.4f bloquea LONG (lado abarrotado)",
                  symbol, fr, C.FUNDING_RATE_MAX_ABS)
        return None
    if sig.direction == "SHORT" and fr < -C.FUNDING_RATE_MAX_ABS:
        log.debug("[%s] funding=%.4f < -%.4f bloquea SHORT (lado abarrotado)",
                  symbol, fr, C.FUNDING_RATE_MAX_ABS)
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

    if not risk.tier_ok(sig.tier):
        return None

    log.info("[%s] Señal %s tier=%s score=%.1f fr=%.4f",
             symbol, sig.direction, sig.tier, sig.score, fr)

    if C.MODE == "SIGNAL":
        await tg.notify_signal(sig)
        return sig

    # ── LIVE ──────────────────────────────────────────────────────────────────
    can, reason = await risk.can_trade()
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
        tier=sig.tier, score=sig.score,
    )
    await pos_mgr.register_trade(trade)
    await tg.notify_trade_opened(sig, qty, order_id)
    return sig


async def scan_loop(client, risk, pos_mgr):
    log.info("Scanner v6.5 | Modo=%s | Interval=%ds | Batch=20",
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

        BATCH = 20
        signals_found = 0
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
        log.info("Iter %d | %d símbolos | %d señales | %.1fs",
                 iteration, len(symbols), signals_found, elapsed)

        await asyncio.sleep(max(0.0, C.SCAN_INTERVAL - elapsed))
