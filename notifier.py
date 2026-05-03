# -*- coding: utf-8 -*-
"""notifier.py -- Three Step Bot v4 -- Rich Telegram Notifications (FIXED).

Fixes:
  - Switched to MarkdownV2 with proper escaping (old Markdown silently fails)
  - Retry logic (3 attempts) on network errors
  - test_telegram() for startup diagnostics
  - Full error body logged on failure
"""
from __future__ import annotations
import asyncio
import re
import aiohttp
from loguru import logger


# ── MarkdownV2 escaping ───────────────────────────────────────────────────────
_MD_SPECIAL = r'\_*[]()~`>#+-=|{}.!'

def _esc(text: str) -> str:
    """Escape special chars for Telegram MarkdownV2."""
    return re.sub(r'([_\*\[\]()~`>#+\-=|{}.!])', r'\\\1', str(text))


def _fmt(n: float, decimals: int = 6) -> str:
    return _esc(f"{n:.{decimals}f}")

def _fmtp(n: float) -> str:
    return _esc(f"{n:+.2f}")

def _fmtu(n: float) -> str:
    return _esc(f"{n:+.4f}")


# ── Core sender ───────────────────────────────────────────────────────────────

async def _send(text: str, retries: int = 3) -> bool:
    from config import cfg
    if not cfg.telegram_token or not cfg.telegram_chat_id:
        logger.warning("[TELEGRAM] Token o chat_id no configurados")
        return False

    url = f"https://api.telegram.org/bot{cfg.telegram_token}/sendMessage"
    payload = {
        "chat_id":    cfg.telegram_chat_id,
        "text":       text,
        "parse_mode": "MarkdownV2",
    }

    for attempt in range(1, retries + 1):
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    url, json=payload,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    body = await r.text()
                    if r.status == 200:
                        return True
                    logger.warning(
                        f"[TELEGRAM] HTTP {r.status} intento {attempt}/{retries}: {body[:200]}"
                    )
                    # Bad Request = formatting error, no reintentar
                    if r.status == 400:
                        logger.error(f"[TELEGRAM] Mensaje con error de formato:\n{text[:300]}")
                        return False
        except asyncio.TimeoutError:
            logger.warning(f"[TELEGRAM] Timeout intento {attempt}/{retries}")
        except Exception as e:
            logger.warning(f"[TELEGRAM] Error intento {attempt}/{retries}: {e}")

        if attempt < retries:
            await asyncio.sleep(2 ** attempt)

    return False


# ── Startup test ──────────────────────────────────────────────────────────────

async def test_telegram() -> None:
    """Call on startup to verify Telegram connection."""
    from config import cfg
    logger.info("[TELEGRAM] Probando conexion...")
    if not cfg.telegram_token or not cfg.telegram_chat_id:
        logger.error("[TELEGRAM] TELEGRAM_TOKEN o TELEGRAM_CHAT_ID vacios en variables de entorno")
        return
    ok = await _send(
        "*Three Step Bot v4* \\- Telegram OK ✅\n"
        "Notificaciones de entradas/salidas activas\\."
    )
    if ok:
        logger.success("[TELEGRAM] Conexion OK")
    else:
        logger.error("[TELEGRAM] FALLO \\- revisa token y chat_id en Railway Variables")


# ── Public API ────────────────────────────────────────────────────────────────

async def notify(text: str) -> None:
    """Generic notification -- auto-escapes plain text."""
    await _send(_esc(text))


async def notify_entry(
    symbol: str, side: str, price: float,
    sl: float, tp: float, size_usdt: float,
    leverage: int, qty: float, score: int,
    delta1: float, delta2: float, vol_ratio: float,
) -> None:
    side_emoji = "BUY  LONG" if side == "BUY" else "SELL SHORT"
    stars = "★" * score + "☆" * (5 - score)
    sl_pct = abs(price - sl) / price * 100
    tp_pct = abs(tp - price) / price * 100
    rr = round(tp_pct / sl_pct, 1) if sl_pct > 0 else 0
    exposure = size_usdt * leverage

    text = (
        f"🚀 *ENTRADA* \\— `{_esc(symbol)}`\n"
        f"┣ Dir: *{_esc(side_emoji)}*\n"
        f"┣ Precio:  `{_fmt(price)}`\n"
        f"┣ SL:      `{_fmt(sl)}` \\({_fmtp(-sl_pct)}%\\)\n"
        f"┣ TP:      `{_fmt(tp)}` \\(\\+{_esc(f'{tp_pct:.2f}')}%\\)\n"
        f"┣ RR: `1:{_esc(str(rr))}` \\| Score: `{_esc(stars)}` \\({score}/5\\)\n"
        f"┣ Tamaño: `{_esc(str(size_usdt))} USDT ×{leverage}` \\= `{_esc(f'{exposure:.0f}')} exp`\n"
        f"┣ Qty: `{_fmt(qty)}`\n"
        f"┗ Vol: `{_esc(f'{vol_ratio:.1f}')}x` \\| D1:`{_fmtp(delta1)}` D2:`{_fmtp(delta2)}`"
    )
    await _send(text)


async def notify_breakeven(
    symbol: str, side: str, entry: float, r_at_be: float
) -> None:
    text = (
        f"🔒 *BREAKEVEN* \\— `{_esc(symbol)}`\n"
        f"┣ SL movido a entrada: `{_fmt(entry)}`\n"
        f"┣ Dir: `{_esc(side)}`\n"
        f"┗ R actual: `{_esc(f'{r_at_be:.2f}')}R` \\— riesgo eliminado ✅"
    )
    await _send(text)


async def notify_partial(
    symbol: str, qty_closed: float, qty_remaining: float,
    price: float, pnl_usdt: float,
) -> None:
    emoji = "✅" if pnl_usdt >= 0 else "❌"
    text = (
        f"✂️ *CIERRE PARCIAL* \\— `{_esc(symbol)}`\n"
        f"┣ Cerrado 50% en breakeven\n"
        f"┣ Precio: `{_fmt(price)}`\n"
        f"┣ Qty cerrada:    `{_fmt(qty_closed)}`\n"
        f"┣ Qty restante:   `{_fmt(qty_remaining)}`\n"
        f"┗ PnL parcial: {emoji} `{_fmtu(pnl_usdt)} USDT`"
    )
    await _send(text)


async def notify_exit(
    symbol: str, side: str,
    entry: float, exit_price: float,
    qty: float, size_usdt: float, leverage: int,
    r_achieved: float, peak_r: float,
    exit_reason: str,
) -> None:
    pnl_pct = ((exit_price - entry) / entry * 100) if side == "BUY" \
              else ((entry - exit_price) / entry * 100)
    pnl_usdt = pnl_pct / 100 * size_usdt * leverage

    if exit_reason == "TP":
        header = "🎯 *TAKE PROFIT*"
        result = "✅ GANANCIA"
    elif exit_reason == "SL":
        header = "🛑 *STOP LOSS*"
        result = "❌ PERDIDA"
    elif exit_reason == "TRAIL":
        header = "🏃 *SALIDA TRAILING*"
        result = "✅ GANANCIA" if pnl_usdt >= 0 else "❌ PERDIDA"
    else:
        header = "📤 *SALIDA MANUAL*"
        result = "✅ GANANCIA" if pnl_usdt >= 0 else "❌ PERDIDA"

    pnl_sign = "+" if pnl_usdt >= 0 else ""
    text = (
        f"{header} \\— `{_esc(symbol)}`\n"
        f"┣ Resultado: *{_esc(result)}*\n"
        f"┣ Dir: `{_esc(side)}`\n"
        f"┣ Entrada:  `{_fmt(entry)}`\n"
        f"┣ Salida:   `{_fmt(exit_price)}`\n"
        f"┣ PnL: `{_esc(pnl_sign)}{_esc(f'{pnl_usdt:.4f}')} USDT` "
        f"\\({_fmtp(pnl_pct)}%\\)\n"
        f"┣ R logrado: `{_esc(f'{r_achieved:.2f}')}R` \\| Peak: `{_esc(f'{peak_r:.2f}')}R`\n"
        f"┗ Razon: `{_esc(exit_reason)}`"
    )
    await _send(text)


async def notify_daily_summary(
    total_trades: int, wins: int, losses: int,
    net_pnl: float, balance: float,
) -> None:
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
    emoji = "📈" if net_pnl >= 0 else "📉"
    pnl_sign = "+" if net_pnl >= 0 else ""
    text = (
        f"{emoji} *RESUMEN DIARIO*\n"
        f"┣ Trades: `{total_trades}` \\| Win rate: `{_esc(f'{win_rate:.1f}')}%`\n"
        f"┣ ✅ Ganados: `{wins}` \\| ❌ Perdidos: `{losses}`\n"
        f"┣ PnL neto: `{_esc(pnl_sign)}{_esc(f'{net_pnl:.4f}')} USDT`\n"
        f"┗ Balance: `{_esc(f'{balance:.2f}')} USDT`"
    )
    await _send(text)
