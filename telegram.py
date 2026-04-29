"""notifications/telegram.py — Async Telegram notifications with rate limiting."""
from __future__ import annotations
import asyncio
import time
from datetime import datetime, timezone
from loguru import logger

_queue: asyncio.Queue[tuple[str, bool]] = asyncio.Queue(maxsize=200)
_last_send: float = 0.0
_MIN_INTERVAL = 0.5   # seconds between messages (Telegram limit: 30/sec)


# ── Sender task ───────────────────────────────────────────────────────────────

def start_sender() -> None:
    """Start the background Telegram sender coroutine."""
    asyncio.create_task(_sender_loop())


async def _sender_loop() -> None:
    global _last_send
    import aiohttp
    from core.config import cfg

    while True:
        msg, silent = await _queue.get()
        # Rate limit
        gap = time.time() - _last_send
        if gap < _MIN_INTERVAL:
            await asyncio.sleep(_MIN_INTERVAL - gap)
        try:
            url = f"https://api.telegram.org/bot{cfg.telegram_token}/sendMessage"
            payload = {
                "chat_id":    cfg.telegram_chat_id,
                "text":       msg[:4096],
                "parse_mode": "HTML",
                "disable_notification": silent,
            }
            async with aiohttp.ClientSession() as s:
                async with s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status != 200:
                        body = await r.text()
                        logger.warning(f"Telegram {r.status}: {body[:200]}")
        except Exception as e:
            logger.debug(f"Telegram send error: {e}")
        finally:
            _last_send = time.time()
            _queue.task_done()


async def send(msg: str, silent: bool = False) -> None:
    """Queue a message (non-blocking)."""
    try:
        _queue.put_nowait((msg, silent))
    except asyncio.QueueFull:
        logger.debug("Telegram queue full — dropping message")


async def send_now(msg: str) -> None:
    """Fire-and-forget immediate send (bypasses queue)."""
    from core.config import cfg
    import aiohttp
    try:
        url = f"https://api.telegram.org/bot{cfg.telegram_token}/sendMessage"
        payload = {
            "chat_id": cfg.telegram_chat_id,
            "text": msg[:4096],
            "parse_mode": "HTML",
        }
        async with aiohttp.ClientSession() as s:
            await s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8))
    except Exception as e:
        logger.debug(f"send_now error: {e}")


# ── Message builders ──────────────────────────────────────────────────────────

def _bar(confidence: float) -> str:
    filled = int(confidence / 10)
    return "█" * filled + "░" * (10 - filled)


def msg_start(n_symbols: int) -> str:
    from core.config import cfg
    return (
        f"⚡ <b>UltraBot v3 — Online</b>\n\n"
        f"📊 Universe: <b>{n_symbols} symbols</b>\n"
        f"⏱ Timeframe: {cfg.timeframe} / {cfg.confirm_tf} / {cfg.trend_tf}\n"
        f"🎯 Period: {cfg.period} | ADX≥{cfg.adx_thresh} | RSI OB/OS {cfg.rsi_ob}/{cfg.rsi_os}\n"
        f"⚖️ Leverage: {cfg.leverage}x | Risk: {cfg.risk_pct}% | Max trades: {cfg.max_open_trades}\n"
        f"🛡 SL: {cfg.sl_pct}% | TP: {cfg.tp_pct}% | Trailing: {cfg.trailing_sl}\n"
        f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC"
    )


def msg_entry(
    symbol: str, side: str, price: float, size: float,
    sl: float, tp: float, sl_pct: float, tp_pct: float, metrics: dict
) -> str:
    emoji = "🟢 LONG" if side == "BUY" else "🔴 SHORT"
    conf  = metrics.get("confidence", 0)
    return (
        f"{emoji} <b>{symbol}</b>\n\n"
        f"💰 Entry: <code>{price:.6g}</code>\n"
        f"🎯 TP: <code>{tp:.6g}</code> (+{tp_pct:.1f}%)\n"
        f"🛡 SL: <code>{sl:.6g}</code> (-{sl_pct:.1f}%)\n"
        f"📦 Size: <b>{size:.1f} USDT</b>\n\n"
        f"📈 ADX: {metrics.get('adx', 0):.1f} | "
        f"+DI: {metrics.get('plus_di', 0):.1f} | "
        f"-DI: {metrics.get('minus_di', 0):.1f}\n"
        f"📊 RSI: {metrics.get('rsi', 0):.1f} | "
        f"ATR: {metrics.get('atr_pct', 0):.2f}%\n"
        f"🔵 Δ1: {metrics.get('delta1', 0):+.0f} | "
        f"Δ2: {metrics.get('delta2', 0):+.0f} | "
        f"Δ3: {metrics.get('delta3', 0):+.0f}\n"
        f"⚡ Confidence: {conf:.0f}% {_bar(conf)}"
    )


def msg_close(
    symbol: str, side: str, pnl: float, pnl_pct: float,
    reason: str, duration_s: int
) -> str:
    pnl_emoji  = "💚" if pnl >= 0 else "🔴"
    side_emoji = "🟢" if side == "LONG" else "🔴"
    mins = duration_s // 60
    return (
        f"{pnl_emoji} <b>{symbol}</b> {side_emoji} {side} — <b>{reason}</b>\n\n"
        f"PnL: <b>{pnl:+.2f} USDT</b> ({pnl_pct:+.2f}%)\n"
        f"Duration: {mins}m"
    )


def msg_performance(perf: dict, risk: dict) -> str:
    return (
        f"📊 <b>Performance Report</b>\n\n"
        f"Trades: {perf.get('total_trades', 0)} | "
        f"WR: {perf.get('win_rate', 0):.1f}%\n"
        f"Total PnL: <b>{perf.get('total_pnl', 0):+.2f} USDT</b>\n"
        f"Avg Win: +{perf.get('avg_win', 0):.2f} | "
        f"Avg Loss: {perf.get('avg_loss', 0):.2f}\n"
        f"Best: {perf.get('best_trade', 0):+.2f} | "
        f"Worst: {perf.get('worst_trade', 0):+.2f}\n"
        f"Avg duration: {perf.get('avg_duration_m', 0):.0f}m\n\n"
        f"💰 Balance: {risk.get('balance', 0):.2f} USDT | "
        f"Day PnL: {risk.get('daily_pnl_usdt', 0):+.2f}"
    )


def msg_halt(reason: str) -> str:
    return f"🚨 <b>BOT HALTED</b>\n\nReason: {reason}"


def msg_cooldown(seconds: int, consec: int) -> str:
    return f"⏸ Cooldown {seconds}s ({consec} consecutive losses)"


def msg_error(error: str) -> str:
    return f"⚠️ Error: <code>{error[:300]}</code>"
