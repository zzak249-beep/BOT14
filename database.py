"""core/database.py — Async SQLite for trades, signals, performance stats."""
from __future__ import annotations
import json
from datetime import datetime, timezone
from loguru import logger
import aiosqlite

DB_PATH = "data/ultrabot.db"


async def init_db() -> None:
    """Create tables if they don't exist."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
        CREATE TABLE IF NOT EXISTS trades (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT    NOT NULL,
            side        TEXT    NOT NULL,
            entry_price REAL    NOT NULL,
            exit_price  REAL,
            qty         REAL,
            size_usdt   REAL,
            sl          REAL,
            tp          REAL,
            pnl         REAL,
            pnl_pct     REAL,
            reason      TEXT,
            metrics     TEXT,
            opened_at   TEXT    NOT NULL,
            closed_at   TEXT,
            duration_s  INTEGER
        );

        CREATE TABLE IF NOT EXISTS signals (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol     TEXT    NOT NULL,
            signal     TEXT    NOT NULL,
            metrics    TEXT,
            executed   INTEGER DEFAULT 0,
            ts         TEXT    NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_trades_symbol  ON trades(symbol);
        CREATE INDEX IF NOT EXISTS idx_trades_opened  ON trades(opened_at);
        CREATE INDEX IF NOT EXISTS idx_signals_ts     ON signals(ts);
        """)
        await db.commit()
    logger.info("Database initialised")


async def save_trade_open(
    symbol: str, side: str, entry: float,
    qty: float, size_usdt: float, sl: float, tp: float, metrics: dict
) -> int:
    opened_at = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """INSERT INTO trades
               (symbol, side, entry_price, qty, size_usdt, sl, tp, metrics, opened_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (symbol, side, entry, qty, size_usdt, sl, tp,
             json.dumps(metrics), opened_at)
        )
        await db.commit()
        return cur.lastrowid  # type: ignore[return-value]


async def save_trade_close(
    trade_id: int, exit_price: float, pnl: float, pnl_pct: float,
    reason: str, entry_price: float, opened_at: str
) -> None:
    closed_at = datetime.now(timezone.utc).isoformat()
    duration_s = 0
    if opened_at:
        try:
            dur = datetime.now(timezone.utc) - datetime.fromisoformat(opened_at)
            duration_s = int(dur.total_seconds())
        except Exception:
            pass

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE trades SET
               exit_price=?, pnl=?, pnl_pct=?, reason=?, closed_at=?, duration_s=?
               WHERE id=?""",
            (exit_price, pnl, pnl_pct, reason, closed_at, duration_s, trade_id)
        )
        await db.commit()


async def save_signal(symbol: str, signal: str, metrics: dict, executed: bool) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO signals (symbol, signal, metrics, executed, ts) VALUES (?,?,?,?,?)",
            (symbol, signal, json.dumps(metrics), int(executed), ts)
        )
        await db.commit()


async def get_performance_stats() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # All closed trades
        cur = await db.execute(
            "SELECT pnl, pnl_pct, duration_s, symbol FROM trades WHERE closed_at IS NOT NULL"
        )
        rows = await cur.fetchall()

    if not rows:
        return {
            "total_trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0,
            "total_pnl": 0.0, "avg_win": 0.0, "avg_loss": 0.0,
            "best_trade": 0.0, "worst_trade": 0.0, "avg_duration_m": 0.0,
        }

    pnls   = [r["pnl"] for r in rows if r["pnl"] is not None]
    wins   = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    durs   = [r["duration_s"] for r in rows if r["duration_s"]]

    return {
        "total_trades":  len(pnls),
        "wins":          len(wins),
        "losses":        len(losses),
        "win_rate":      round(len(wins) / len(pnls) * 100, 1) if pnls else 0.0,
        "total_pnl":     round(sum(pnls), 2),
        "avg_win":       round(sum(wins) / len(wins), 2) if wins else 0.0,
        "avg_loss":      round(sum(losses) / len(losses), 2) if losses else 0.0,
        "best_trade":    round(max(pnls), 2) if pnls else 0.0,
        "worst_trade":   round(min(pnls), 2) if pnls else 0.0,
        "avg_duration_m": round(sum(durs) / len(durs) / 60, 1) if durs else 0.0,
    }


async def get_recent_trades(limit: int = 20) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)
        )
        rows = await cur.fetchall()
    return [dict(r) for r in rows]
