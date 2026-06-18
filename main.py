"""
QF×JP Bot v7.1 — Main
FastAPI con lifespan moderno + reconciliación al arrancar + trailing stop info
+ daily loss real (PnL no realizado incluido) en /status
"""
import asyncio
import logging
import sys
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

import config as C
from bingx_client import BingXClient
from risk_manager import RiskManager
from position_manager import PositionManager
from scanner import scan_loop
import telegram_client as tg
from copier_client import MasterClient
from complement_engine import ComplementEngine
from trade_journal import TradeJournal

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-16s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("main")

client:     BingXClient       = None
risk:       RiskManager       = None
pos_mgr:    PositionManager   = None
master:     MasterClient      = None
complement: ComplementEngine   = None
journal:    TradeJournal       = None


async def _run_scanner():
    try:
        await scan_loop(client, risk, pos_mgr, complement, journal)
    except Exception as e:
        log.critical("Scanner crash: %s", e, exc_info=True)
        await tg.notify_error("scanner_crash", str(e))


async def _run_monitor():
    if C.MODE == "LIVE":
        try:
            await pos_mgr.monitor_loop()
        except Exception as e:
            log.critical("Monitor crash: %s", e, exc_info=True)
            await tg.notify_error("monitor_crash", str(e))
    else:
        log.info("Monitor desactivado en modo SIGNAL")


async def _run_complement():
    import os
    if os.getenv("COMPLEMENT_MODE", "") == "DISABLED":
        log.info("Complement engine desactivado")
        return
    if C.MODE == "LIVE":
        try:
            await complement.run_loop()
        except Exception as e:
            log.critical("Complement crash: %s", e, exc_info=True)
            await tg.notify_error("complement_crash", str(e))


@asynccontextmanager
async def lifespan(app: FastAPI):
    global client, risk, pos_mgr, master, complement, journal

    log.info("═" * 54)
    log.info("  QF×JP Bot v7.7 — ROADMAP COMPLETO")
    log.info("  Modo: %s | Capital: %.2f USDT", C.MODE, C.CAPITAL)
    log.info("  Leverage: %dx | Min tier: %s", C.LEVERAGE, C.MIN_TIER)
    log.info("  Max notional: %.0f USDT | Daily loss: %.1f%%",
             C.MAX_NOTIONAL_USDT, C.DAILY_LOSS_PCT)
    log.info("  SL mult: %.1f | Trail activation: %.1f ATR | Trail dist: %.1f ATR",
             C.SL_ATR_MULT, C.BREAKEVEN_ATR_MULT, C.TRAIL_DISTANCE_ATR)
    log.info("  Max open: %d | Max daily: %d", C.MAX_OPEN_TRADES, C.MAX_DAILY_TRADES)
    log.info("  Session: %02d:00-%02d:00 UTC | Limit orders: %s",
             getattr(C, 'TRADE_START_UTC', 0), getattr(C, 'TRADE_END_UTC', 24),
             getattr(C, 'LIMIT_ORDERS_ENABLED', False))
    log.info("═" * 54)

    journal    = TradeJournal()
    client     = BingXClient()
    risk       = RiskManager()
    pos_mgr    = PositionManager(client, risk, journal=journal)
    master     = MasterClient()
    complement = ComplementEngine(client, risk, pos_mgr, master)

    if not C.BINGX_API_KEY or not C.BINGX_SECRET_KEY:
        log.error("BINGX_API_KEY / BINGX_SECRET_KEY no configurados")
    if not C.TELEGRAM_TOKEN or not C.TELEGRAM_CHAT_ID:
        log.warning("Telegram no configurado")

    try:
        balance = await client.get_balance()
        log.info("Balance: %.4f USDT", balance)
    except Exception as e:
        log.warning("Balance inicial no disponible: %s", e)
        balance = 0.0

    if C.MODE == "LIVE":
        try:
            await pos_mgr.reconcile_on_startup()
        except Exception as e:
            log.warning("reconcile_on_startup error: %s", e)

    await tg.notify_status(risk.status(), balance, 0)

    scanner_task    = asyncio.create_task(_run_scanner())
    monitor_task    = asyncio.create_task(_run_monitor())
    complement_task = asyncio.create_task(_run_complement())
    log.info("Loops iniciados (scanner + monitor + complement)")

    yield

    scanner_task.cancel()
    monitor_task.cancel()
    complement_task.cancel()
    if master:
        await master.close()
    if client:
        await client.close()
    log.info("Bot detenido.")


app = FastAPI(
    title="QF×JP Bot v7.1",
    docs_url=None,
    redoc_url=None,
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    return {"status": "ok", "version": "7.1", "mode": C.MODE}


@app.get("/status")
async def status():
    if risk is None:
        return JSONResponse({"error": "not_ready"}, status_code=503)
    try:
        balance = await client.get_balance()
    except Exception:
        balance = -1.0

    # FIX v7.1: PnL no realizado real, incluido en el status de riesgo
    try:
        unrealized = await pos_mgr.get_unrealized_pnl() if pos_mgr else 0.0
    except Exception:
        unrealized = 0.0

    tracked = pos_mgr.get_tracked() if pos_mgr else {}
    return {
        "version": "7.1",
        "mode":    C.MODE,
        "balance": round(balance, 2),
        "risk":    risk.status(unrealized_pnl=unrealized),
        "trades":  {
            sym: {
                "direction":       t.direction,
                "entry":           t.entry,
                "sl":              t.sl,
                "tp1":             t.tp1,
                "tp2":             t.tp2,
                "qty":             t.qty,
                "be_moved":        t.be_moved,
                # ── Trailing info ─────────────────────────────────────────────
                "trailing_active": t.trailing_active,
                "trail_sl":        round(t.trail_sl, 8) if t.trail_sl else None,
                "peak_price":      round(t.peak_price, 8) if t.peak_price else None,
                "pnl_at_trail_sl": round(
                    (t.trail_sl - t.entry) * t.qty * C.LEVERAGE
                    if t.direction == "LONG" and t.trail_sl > 0
                    else (t.entry - t.trail_sl) * t.qty * C.LEVERAGE
                    if t.direction == "SHORT" and t.trail_sl > 0
                    else 0.0, 2
                ),
            }
            for sym, t in tracked.items()
        },
    }


@app.post("/close/{symbol}")
async def close_symbol(symbol: str):
    if C.MODE != "LIVE":
        raise HTTPException(400, "Solo en modo LIVE")
    if pos_mgr is None:
        raise HTTPException(503, "not_ready")
    symbol = symbol.upper()
    if not pos_mgr.is_trading(symbol):
        raise HTTPException(404, f"{symbol} sin posición")
    await pos_mgr.close_position_emergency(symbol, reason="manual_close")
    return {"status": "ok", "symbol": symbol}


@app.get("/positions")
async def positions():
    if client is None:
        raise HTTPException(503, "not_ready")
    try:
        raw = await client.get_open_positions()
        return {"count": len(raw), "positions": raw}
    except Exception as e:
        raise HTTPException(500, str(e))


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=C.PORT,
        log_level="info",
        access_log=False,
    )
