"""
QF×JP Bot v6.5.2 — Telegram Client
Añade notify_pre_signal para anticipación de entradas.
"""
import asyncio
import logging

import aiohttp

import config as C

log = logging.getLogger("telegram")

_BASE = f"https://api.telegram.org/bot{C.TELEGRAM_TOKEN}/sendMessage"

async def send(text: str, parse_mode: str = "Markdown") -> bool:
    if not C.TELEGRAM_TOKEN or not C.TELEGRAM_CHAT_ID:
        log.debug("Telegram no configurado — skip")
        return False
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(_BASE, json={
                "chat_id":    C.TELEGRAM_CHAT_ID,
                "text":       text,
                "parse_mode": parse_mode,
            }, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    body = await r.text()
                    log.warning("Telegram %d: %s", r.status, body[:200])
                return r.status == 200
    except Exception as e:
        log.warning("Telegram send error: %s", e)
        return False


async def notify_signal(sig) -> None:
    tier_icon = {"SUP": "🔥🔥", "FUEL": "🔥", "STD": "⚡"}.get(sig.tier, "📡")
    dir_icon  = "🟢" if sig.direction == "LONG" else "🔴"
    sweep_txt = f"Sweep: `{sig.sweep}`\n" if getattr(sig, "sweep", "NONE") != "NONE" else ""
    sq_txt    = "SQ Fire ⚡\n" if getattr(sig, "squeeze_fire", False) else ""
    msg = (
        f"{tier_icon} *{sig.symbol}* {dir_icon} `{sig.direction}`\n"
        f"Score: `{sig.score:.1f}` | Tier: `{sig.tier}`\n"
        f"Entry: `{sig.entry:.6f}`\n"
        f"SL:    `{sig.sl:.6f}`\n"
        f"TP1:   `{sig.tp1:.6f}`\n"
        f"TP2:   `{sig.tp2:.6f}`\n"
        f"ADX: `{sig.adx:.1f}` | MFI: `{sig.mfi:.1f}` | CVD: `{sig.cvd:.3f}`\n"
        f"Estructura: `{sig.structure}` | TL: `{sig.tl_break}`\n"
        f"{sweep_txt}{sq_txt}"
        f"HTF: `{sig.htf_score:.2f}` | FR: `{sig.funding_rate:.4f}`"
    )
    await send(msg)


async def notify_pre_signal(sig) -> None:
    """[ANT] Pre-señal — condiciones formando, score insuficiente aún."""
    dir_icon  = "🟢" if sig.direction == "LONG" else "🔴"
    sweep_txt = f"Sweep: `{sig.sweep}`\n" if getattr(sig, "sweep", "NONE") != "NONE" else ""
    sq_txt    = ("SQ Fire ⚡\n" if getattr(sig, "squeeze_fire", False)
                 else "SQ Comprimido 🔋\n" if getattr(sig, "squeeze", False) else "")
    rsi_txt   = ("RSI3 ↑ consenso\n" if getattr(sig, "rsi_consensus", 0) == 1
                 else "RSI3 ↓ consenso\n" if getattr(sig, "rsi_consensus", 0) == -1 else "")
    vwap_dev  = getattr(sig, "vwap_dev", 0.0)
    vwap_txt  = f"VWAP dev: `{vwap_dev:+.2f}` ATR\n" if abs(vwap_dev) > 0.8 else ""
    fvg_txt   = f"FVG: `{sig.fvg_active}`\n" if getattr(sig, "fvg_active", "NONE") not in ("NONE", "") else ""

    msg = (
        f"⚡ *PRE-SEÑAL* — `{sig.symbol}` {dir_icon} `{sig.direction}`\n"
        f"Pre-score: `{sig.pre_score:.1f}` | Score actual: `{sig.score:.1f}`\n"
        f"Entry≈: `{sig.entry:.6f}`\n"
        f"{sweep_txt}{sq_txt}{rsi_txt}{vwap_txt}{fvg_txt}"
        f"Estructura: `{sig.structure}`\n"
        f"CVD: `{sig.cvd:.3f}` | VDI: `{sig.vdi:.2f}` | FR: `{sig.funding_rate:.4f}`"
    )
    await send(msg)


async def notify_trade_opened(sig, qty: float, order_id: str) -> None:
    dir_icon = "🟢 LONG" if sig.direction == "LONG" else "🔴 SHORT"
    sweep_txt = f"Sweep: `{sig.sweep}` ✓\n" if getattr(sig, "sweep", "NONE") != "NONE" else ""
    msg = (
        f"✅ *TRADE ABIERTO* — {sig.symbol}\n"
        f"Dirección: {dir_icon}\n"
        f"Entry: `{sig.entry:.6f}` | Qty: `{qty}`\n"
        f"SL: `{sig.sl:.6f}` | TP1: `{sig.tp1:.6f}` | TP2: `{sig.tp2:.6f}`\n"
        f"Score: `{sig.score:.1f}` ({sig.tier})\n"
        f"{sweep_txt}"
        f"Order ID: `{order_id}`"
    )
    await send(msg)


async def notify_trade_closed(
    symbol: str, direction: str, entry: float,
    close_price: float, qty: float, reason: str, pnl: float,
) -> None:
    pnl_icon = "💚" if pnl >= 0 else "💔"
    dir_icon = "🟢" if direction == "LONG" else "🔴"
    msg = (
        f"{pnl_icon} *TRADE CERRADO* — {symbol} {dir_icon}\n"
        f"Entry: `{entry:.6f}` → Close: `{close_price:.6f}`\n"
        f"Qty: `{qty}` | Razón: `{reason}`\n"
        f"PnL: `{pnl:+.4f} USDT`"
    )
    await send(msg)


async def notify_circuit_breaker(symbol: str) -> None:
    msg = f"⚠️ *CIRCUIT BREAKER* — `{symbol}`\nVela extrema detectada. En cooldown 10 min."
    await send(msg)


async def notify_status(status: dict, balance: float, n_symbols: int) -> None:
    msg = (
        f"📊 *STATUS QF×JP Bot v6.5.2*\n"
        f"Modo: `{status.get('mode', '?')}`\n"
        f"Balance: `{balance:.2f} USDT`\n"
        f"Trades abiertos: `{status.get('open_trades', 0)}/{status.get('max_open', 0)}`\n"
        f"Trades hoy: `{status.get('daily_trades', 0)}/{status.get('max_daily', 0)}`\n"
        f"PnL diario: `{status.get('daily_pnl', 0):+.4f} USDT`\n"
        f"Símbolos escaneados: `{n_symbols}`"
    )
    await send(msg)


async def notify_error(context: str, error: str) -> None:
    msg = f"🚨 *ERROR* — `{context}`\n`{error[:300]}`"
    await send(msg)
