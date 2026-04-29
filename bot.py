"""
bot.py — UltraBot v3: Main async orchestrator.

Four concurrent loops:
  1. scan_loop         — fetches universe, runs indicators, executes signals
  2. position_monitor  — trailing stop, flip detection, PnL tracking
  3. performance_loop  — periodic Telegram reports + DB stats
  4. dashboard         — FastAPI + WebSocket live dashboard

Speed: uvloop + aiohttp pool + numba JIT + concurrent kline fetch
"""
from __future__ import annotations
import asyncio
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print("✅ uvloop active")
except ImportError:
    print("ℹ️  uvloop not available")

from loguru import logger
from rich.console import Console
from rich.table import Table
from rich import box as rbox

from core.config import cfg
from core.risk import RiskManager
from core.database import init_db, save_trade_open, save_trade_close, save_signal, get_performance_stats, get_recent_trades
from exchange.client import (
    fetch_all_tickers, fetch_universe_concurrent,
    get_balance, get_all_positions, set_leverage,
    place_market_order, close_position, cancel_all_orders,
    ws_price_stream, close_session, get_price, _ws_prices
)
from strategies.indicators import generate_signal
from notifications.telegram import (
    send, send_now, start_sender,
    msg_start, msg_entry, msg_close, msg_performance, msg_halt,
    msg_cooldown, msg_error
)
from dashboard.server import start_dashboard, update_state

console  = Console()
risk     = RiskManager()
executor = ThreadPoolExecutor(max_workers=12, thread_name_prefix="indicator")

# ── Shared state ───────────────────────────────────────────────────────────────
open_trades:    dict[str, dict] = {}   # symbol → {side, entry, sl, tp, size, db_id, opened_at, metrics}
trailing_peaks: dict[str, float] = {}
last_signals:   dict[str, str]  = {}
scan_stats:     dict = {"last_ms": 0, "n_buy": 0, "n_sell": 0, "n_scanned": 0}
trade_metrics:  dict[str, dict] = {}  # symbol → last metrics (for dashboard)


# ── Universe filter ────────────────────────────────────────────────────────────

async def get_universe() -> list[str]:
    tickers = await fetch_all_tickers()
    candidates = []
    for t in tickers:
        sym = t.get("symbol", "")
        if not sym.endswith("-USDT"):
            continue
        if sym in cfg.blacklist:
            continue
        try:
            vol = float(t.get("quoteVolume") or t.get("volume") or 0)
        except (TypeError, ValueError):
            continue
        if vol >= cfg.min_volume_usdt:
            candidates.append((sym, vol))

    candidates.sort(key=lambda x: x[1], reverse=True)
    syms = [s[0] for s in candidates[:cfg.top_n_symbols]]
    logger.info(f"Universe: {len(syms)} symbols (from {len(tickers)} total)")
    return syms


# ── Indicator runner (CPU, runs in ThreadPool) ─────────────────────────────────

def _run_indicators(symbol: str, p: dict, h: dict | None, t: dict | None) -> tuple[str, str | None, dict]:
    try:
        sig, m = generate_signal(
            p["high"], p["low"], p["close"], p["open"], p["volume"],
            h["high"] if h else None, h["low"] if h else None,
            h["close"] if h else None, h["open"] if h else None,
            h["volume"] if h else None,
            t["high"] if t else None, t["low"] if t else None,
            t["close"] if t else None,
            cfg,
        )
    except Exception as e:
        return symbol, None, {"error": str(e)}
    return symbol, sig, m


# ── Execution ──────────────────────────────────────────────────────────────────

async def execute_entry(symbol: str, sig: str, metrics: dict, balance: float, n_open: int) -> bool:
    """Open a new position. Returns True if successful."""
    can, reason = risk.can_trade(balance)
    if not can:
        logger.debug(f"Risk gate blocked {symbol}: {reason}")
        return False

    if not risk.correlation_ok(symbol):
        logger.debug(f"Correlation guard blocked {symbol}")
        return False

    atr_val = metrics.get("atr", 0)
    conf    = metrics.get("confidence", 0)
    size    = risk.position_size(balance, n_open, conf, metrics.get("atr_pct", 0))
    if size < 5:
        logger.debug(f"Size too small ({size}) for {symbol}")
        return False

    price = await get_price(symbol)
    if price == 0:
        return False

    sl, tp, sl_pct, tp_pct = risk.dynamic_sl_tp(price, sig, atr_val)

    try:
        await set_leverage(symbol, cfg.leverage)
        resp = await place_market_order(symbol, sig, size, sl, tp)
        if resp.get("code") and int(resp.get("code", 0)) != 0:
            logger.warning(f"Order error {symbol}: {resp}")
            return False
    except Exception as e:
        logger.error(f"Order failed {symbol}: {e}")
        return False

    db_id = await save_trade_open(symbol, sig, price, size * cfg.leverage / price, size, sl, tp, metrics)
    risk.record_open(symbol)

    trade = {
        "side": sig, "entry": price, "sl": sl, "tp": tp,
        "size": size, "db_id": db_id,
        "opened_at": datetime.now(timezone.utc).isoformat(),
        "metrics": metrics, "sl_pct": sl_pct, "tp_pct": tp_pct,
    }
    open_trades[symbol]    = trade
    trailing_peaks[symbol] = price
    last_signals[symbol]   = sig
    trade_metrics[symbol]  = metrics

    msg = msg_entry(symbol, sig, price, size, sl, tp, sl_pct, tp_pct, metrics)
    await send(msg)
    logger.success(f"OPENED {sig} {symbol} @ {price:.6g} | conf={conf:.0f}% | size={size:.1f}")
    return True


async def execute_close(symbol: str, position: dict, reason: str) -> None:
    """Close a position and record."""
    try:
        await close_position(symbol, position)
        await cancel_all_orders(symbol)
    except Exception as e:
        logger.error(f"Close failed {symbol}: {e}")
        return

    pnl = float(position.get("unrealizedProfit", 0))
    balance = await get_balance()
    side = "LONG" if float(position["positionAmt"]) > 0 else "SHORT"
    entry_price = float(position.get("entryPrice", 0))
    mark  = float(position.get("markPrice", entry_price))
    pnl_pct = (mark - entry_price) / entry_price * 100 * (1 if side == "LONG" else -1) * cfg.leverage

    trade = open_trades.pop(symbol, {})
    risk.record_close(symbol, pnl, balance)

    if trade.get("db_id"):
        await save_trade_close(
            trade["db_id"], mark, pnl, pnl_pct, reason,
            entry_price, trade.get("opened_at", "")
        )

    trailing_peaks.pop(symbol, None)
    last_signals.pop(symbol, None)

    opened = trade.get("opened_at", "")
    duration_s = 0
    if opened:
        duration_s = int((datetime.now(timezone.utc) - datetime.fromisoformat(opened)).total_seconds())

    await send(msg_close(symbol, side, pnl, pnl_pct, reason, duration_s))
    logger.info(f"CLOSED {side} {symbol} — {reason} | PnL {pnl:+.2f} USDT")


# ── Scan loop ──────────────────────────────────────────────────────────────────

async def scan_loop() -> None:
    symbols: list[str] = []
    universe_refresh = 0

    while True:
        t0 = time.perf_counter()
        try:
            # Refresh universe every 5 minutes
            if time.time() - universe_refresh > 300:
                symbols = await get_universe()
                universe_refresh = time.time()
                # Start WS price stream for top symbols
                asyncio.create_task(ws_price_stream(symbols[:30]))

            # Fetch all timeframes concurrently
            universe_data = await fetch_universe_concurrent(symbols)

            # Run indicators in thread pool
            loop = asyncio.get_event_loop()
            tasks = [
                loop.run_in_executor(executor, _run_indicators, sym, p, h, t)
                for sym, (p, h, t) in universe_data.items()
                if p is not None and len(p.get("close", [])) >= cfg.candles_needed
            ]
            results: list[tuple] = await asyncio.gather(*tasks)

            # Collect and rank signals
            signals = [
                (sym, sig, m) for sym, sig, m in results
                if sig and m.get("confidence", 0) >= cfg.min_confidence
            ]
            signals.sort(key=lambda x: x[2].get("confidence", 0), reverse=True)

            n_buy  = sum(1 for _, s, _ in signals if s == "BUY")
            n_sell = sum(1 for _, s, _ in signals if s == "SELL")
            elapsed_ms = (time.perf_counter() - t0) * 1000

            scan_stats.update({
                "last_ms": elapsed_ms, "n_buy": n_buy,
                "n_sell": n_sell, "n_scanned": len(results),
                "max_open": cfg.max_open_trades,
            })

            # Save top signals to DB (async, non-blocking)
            for sym, sig, m in signals[:10]:
                asyncio.create_task(save_signal(sym, sig, m, False))

            logger.info(
                f"Scan {elapsed_ms:.0f}ms | {len(results)} syms | "
                f"🟢{n_buy} 🔴{n_sell} signals | bal={await get_balance():.1f}"
            )

            # ── Execute ───────────────────────────────────────────────────────
            balance   = await get_balance()
            positions = await get_all_positions()
            risk.set_balance(balance)
            n_open    = len(positions)

            if risk.is_halted:
                logger.warning("Halted — skip execution")
                await asyncio.sleep(cfg.scan_interval)
                continue

            for sym, sig, metrics in signals:
                if n_open >= cfg.max_open_trades:
                    break
                if sym in positions:
                    # Signal flip check
                    pos = positions[sym]
                    pos_is_long = float(pos["positionAmt"]) > 0
                    if (sig == "SELL" and pos_is_long) or (sig == "BUY" and not pos_is_long):
                        await execute_close(sym, pos, "FLIP")
                        positions = await get_all_positions()
                        n_open = len(positions)
                    continue

                ok = await execute_entry(sym, sig, metrics, balance, n_open)
                if ok:
                    n_open += 1
                    balance = await get_balance()
                    asyncio.create_task(save_signal(sym, sig, metrics, True))

            # Update dashboard state
            positions = await get_all_positions()
            perf = await get_performance_stats()
            update_state(
                status="running", balance=balance,
                positions=positions, scan_stats=scan_stats,
                risk=risk.summary(), perf=perf,
                last_signals=[
                    {"symbol": s, "signal": sig, **m}
                    for s, sig, m in signals[:15]
                ],
                trade_metrics=trade_metrics,
            )

        except Exception as e:
            logger.error(f"Scan loop error: {e}", exc_info=True)
            await send(msg_error(str(e)), silent=True)

        await asyncio.sleep(cfg.scan_interval)


# ── Position monitor ───────────────────────────────────────────────────────────

async def position_monitor() -> None:
    while True:
        await asyncio.sleep(5)
        try:
            positions = await get_all_positions()
            for sym, pos in positions.items():
                amt   = float(pos.get("positionAmt", 0))
                mark  = float(pos.get("markPrice") or pos.get("entryPrice", 0) or 0)
                side  = "LONG" if amt > 0 else "SHORT"

                if mark == 0:
                    continue

                # Trailing stop
                if cfg.trailing_sl and sym in trailing_peaks:
                    peak = trailing_peaks[sym]
                    if   side == "LONG"  and mark > peak: trailing_peaks[sym] = mark
                    elif side == "SHORT" and mark < peak: trailing_peaks[sym] = mark

                    peak_now = trailing_peaks[sym]
                    entry    = float(pos.get("entryPrice", mark))
                    trade    = open_trades.get(sym, {})
                    sl_pct   = trade.get("sl_pct", cfg.sl_pct)

                    trailing_hit = (
                        (side == "LONG"  and mark < peak_now * (1 - sl_pct / 100)) or
                        (side == "SHORT" and mark > peak_now * (1 + sl_pct / 100))
                    )
                    if trailing_hit:
                        logger.info(f"Trailing SL triggered: {side} {sym} mark={mark} peak={peak_now:.6g}")
                        await execute_close(sym, pos, "Trailing SL")

        except Exception as e:
            logger.error(f"Monitor error: {e}")


# ── Performance loop ───────────────────────────────────────────────────────────

async def performance_loop() -> None:
    """Send performance report every 4 hours."""
    await asyncio.sleep(60)
    while True:
        try:
            perf = await get_performance_stats()
            await send(msg_performance(perf, risk.summary()), silent=True)
        except Exception as e:
            logger.debug(f"Perf loop error: {e}")
        await asyncio.sleep(4 * 3600)


# ── Terminal dashboard ─────────────────────────────────────────────────────────

async def terminal_loop() -> None:
    while True:
        await asyncio.sleep(15)
        try:
            positions = await get_all_positions()
            balance   = await get_balance()
            t = Table(title="⚡ UltraBot v3", box=rbox.ROUNDED, show_lines=True)
            t.add_column("Symbol", style="cyan")
            t.add_column("Side")
            t.add_column("Entry",   justify="right")
            t.add_column("Mark",    justify="right")
            t.add_column("PnL",     justify="right")
            t.add_column("Conf%",   justify="right")
            t.add_column("ADX",     justify="right")

            for sym, pos in positions.items():
                pnl  = float(pos.get("unrealizedProfit", 0))
                side = "🟢 LONG" if float(pos["positionAmt"]) > 0 else "🔴 SHORT"
                m    = trade_metrics.get(sym, {})
                t.add_row(
                    sym, side,
                    f"{float(pos.get('entryPrice',0)):.4f}",
                    f"{float(pos.get('markPrice', pos.get('entryPrice',0))):.4f}",
                    f"[{'green' if pnl>=0 else 'red'}]{pnl:+.2f}[/]",
                    f"{m.get('confidence',0):.0f}%",
                    f"{m.get('adx',0):.1f}",
                )

            console.print(t)
            rs = risk.summary()
            console.print(
                f"💰 Bal: [bold]{balance:.2f}[/] | "
                f"Scan: {scan_stats.get('last_ms',0):.0f}ms | "
                f"🟢{scan_stats.get('n_buy',0)} 🔴{scan_stats.get('n_sell',0)} | "
                f"Day: [{'green' if rs['daily_pnl_usdt']>=0 else 'red'}]{rs['daily_pnl_usdt']:+.2f}[/] | "
                f"WR: {rs['win_rate']}% | "
                f"{'[red]HALTED[/]' if rs['halted'] else '[green]RUNNING[/]'}"
            )
        except Exception:
            pass


# ── Entry point ────────────────────────────────────────────────────────────────

async def main() -> None:
    logger.remove()
    logger.add(sys.stderr, level="INFO",
               format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
    logger.add("data/ultrabot.log", rotation="1 day", retention="14 days", level="DEBUG")
    os.makedirs("data", exist_ok=True)

    console.print("[bold cyan]⚡ UltraBot v3 initializing...[/bold cyan]")

    # Init DB
    await init_db()

    # Init balance
    balance = await get_balance()
    risk.set_balance(balance)
    console.print(f"💰 Balance: {balance:.2f} USDT")

    # Universe
    symbols = await get_universe()

    # Start Telegram sender
    start_sender()
    await send(msg_start(len(symbols)))

    # Start dashboard
    if cfg.dashboard_enabled:
        await start_dashboard()

    console.print(f"[green]✅ Ready — scanning {len(symbols)} symbols[/green]")

    # Run all loops concurrently
    await asyncio.gather(
        scan_loop(),
        position_monitor(),
        performance_loop(),
        terminal_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[bold red]Stopped by user[/bold red]")
    finally:
        asyncio.run(close_session())
