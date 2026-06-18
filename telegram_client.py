"""
QF×JP Bot v6.4 — Telegram Client
Envía notificaciones al canal configurado.
Todas las funciones son fire-and-forget (no bloquean el bot).
"""
import asyncio
import logging

import aiohttp

import config as C

log = logging.getLogger("telegram")

_BASE = f"https://api.telegram.org/bot{C.TELEGRAM_TOKEN}/sendMessage"

# ── Envío base ────────────────────────────────────────────────────────────────

async def send(text: str, parse_mode: str = "Markdown") -> bool:
    """Envía un mensaje al chat configurado. Silencia errores para no romper el bot."""
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

# ── Notificaciones específicas ────────────────────────────────────────────────

async def notify_signal(sig) -> None:
    """Señal detectada (modo SIGNAL o antes de abrir en LIVE)."""
    tier_icon = {"SUP": "🔥🔥", "FUEL": "🔥", "STD": "⚡"}.get(sig.tier, "📡")
    dir_icon  = "🟢" if sig.direction == "LONG" else "🔴"

    msg = (
        f"{tier_icon} *{sig.symbol}* {dir_icon} `{sig.direction}`\n"
        f"Score: `{sig.score:.1f}` | Tier: `{sig.tier}`\n"
        f"Entry: `{sig.entry:.6f}`\n"
        f"SL:    `{sig.sl:.6f}`\n"
        f"TP1:   `{sig.tp1:.6f}`\n"
        f"TP2:   `{sig.tp2:.6f}`\n"
        f"ADX: `{sig.adx:.1f}` | MFI: `{sig.mfi:.1f}` | CVD: `{sig.cvd:.3f}`\n"
        f"Estructura: `{sig.structure}` | TL: `{sig.tl_break}`\n"
        f"HTF: `{sig.htf_score:.2f}` | FR: `{sig.funding_rate:.4f}`"
    )
    await send(msg)


async def notify_trade_opened(sig, qty: float, order_id: str) -> None:
    """Trade abierto en BingX."""
    dir_icon = "🟢 LONG" if sig.direction == "LONG" else "🔴 SHORT"
    msg = (
        f"✅ *TRADE ABIERTO* — {sig.symbol}\n"
        f"Dirección: {dir_icon}\n"
        f"Entry: `{sig.entry:.6f}` | Qty: `{qty}`\n"
        f"SL: `{sig.sl:.6f}` | TP1: `{sig.tp1:.6f}` | TP2: `{sig.tp2:.6f}`\n"
        f"Score: `{sig.score:.1f}` ({sig.tier})\n"
        f"Order ID: `{order_id}`"
    )
    await send(msg)


async def notify_trade_closed(
    symbol: str,
    direction: str,
    entry: float,
    close_price: float,
    qty: float,
    reason: str,
    pnl: float,
) -> None:
    """Trade cerrado (SL/TP auto o manual)."""
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
    """Status periódico del bot — incluye PnL no realizado (v7.1)."""
    pnl_total = status.get("daily_pnl_total", status.get("daily_pnl", 0))
    pnl_real  = status.get("daily_pnl", 0)
    pnl_unrealized = status.get("daily_pnl_no_real", 0)
    limit     = status.get("daily_limit", 0)
    pnl_icon  = "💚" if pnl_total >= 0 else "💔"
    msg = (
        f"📊 *STATUS QF×JP Bot*\n"
        f"Modo: `{status.get('mode', '?')}`\n"
        f"Balance: `{balance:.2f} USDT`\n"
        f"Trades abiertos: `{status.get('open_trades', 0)}/{status.get('max_open', 0)}`\n"
        f"Trades hoy: `{status.get('daily_trades', 0)}/{status.get('max_daily', 0)}`\n"
        f"{pnl_icon} PnL cerrado: `{pnl_real:+.4f}` | No realizado: `{pnl_unrealized:+.4f}` | Total: `{pnl_total:+.4f} USDT`\n"
        f"Límite diario: `{limit:.2f} USDT`\n"
        f"Símbolos escaneados: `{n_symbols}`"
    )
    await send(msg)


async def notify_error(context: str, error: str) -> None:
    """Error interno del bot."""
    msg = (
        f"🚨 *ERROR* — `{context}`\n"
        f"`{error[:300]}`"
    )
    await send(msg)


async def notify_diagnostics(
    iteration: int,
    n_symbols: int,
    n_direccionales: int,
    avg_score: float,
    max_score: float,
    max_symbol: str,
    max_dir: str,
    top_reasons: list,
) -> None:
    """
    FIX v7.2 — Diagnóstico de 0 señales.
    Se envía cada 5 iteraciones SOLO cuando no se abrió ningún trade,
    para ver desde el móvil exactamente qué puerta está bloqueando todo
    (TL break, alineación HTF, tier insuficiente, etc.) sin entrar a Railway.
    """
    reasons_str = "\n".join(f"  • `{k}`: {v}" for k, v in top_reasons) if top_reasons else "  • (sin datos)"
    msg = (
        f"🔍 *DIAGNÓSTICO* — Iter {iteration}\n"
        f"Símbolos: `{n_symbols}` | Con dirección: `{n_direccionales}`\n"
        f"Score prom: `{avg_score:.1f}` | Score máx: `{max_score:.1f}` "
        f"({max_symbol} {max_dir})\n"
        f"Top razones de rechazo:\n{reasons_str}"
    )
    await send(msg)


# ── Trade Journal ─────────────────────────────────────────────────────────────

async def notify_journal_report(stats: dict) -> None:
    """Reporte periódico del Trade Journal — win rate, tier, mejores horas."""
    if not stats or stats.get("total", 0) == 0:
        return
    n          = stats["total"]
    wr         = stats.get("win_rate", 0)
    recent_wr  = stats.get("recent_wr", 0)
    pnl        = stats.get("total_pnl", 0)
    opt_score  = stats.get("opt_score", 0)
    offset     = stats.get("adaptive_offset", 0)
    best_hours = stats.get("best_hours_utc", [])

    # Tier breakdown
    tier_lines = []
    for tier, d in stats.get("by_tier", {}).items():
        tier_lines.append(f"  `{tier}`: wr={d['wr']}% | pnl={d['pnl']:+.4f} | n={d['n']}")
    tier_str = "\n".join(tier_lines) if tier_lines else "  —"

    hours_str = ", ".join(f"{h}:00" for h in best_hours) or "—"
    offset_str = f"\nScore adaptativo: `{offset:+.0f} pts`" if offset != 0 else ""

    msg = (
        f"📖 *TRADE JOURNAL* — {n} trades\n"
        f"Win rate: `{wr}%` | Últimas 20: `{recent_wr}%`\n"
        f"PnL total: `{pnl:+.4f} USDT`\n"
        f"Score óptimo empírico: `{opt_score:.1f}`{offset_str}\n"
        f"Mejores horas UTC: `{hours_str}`\n"
        f"Por tier:\n{tier_str}"
    )
    await send(msg)


async def notify_limit_filled(symbol: str, direction: str, price: float, qty: float) -> None:
    """Orden límite llenada — ahorro de comisiones."""
    dir_icon = "🟢" if direction == "LONG" else "🔴"
    msg = (
        f"💰 *LIMIT FILLED* — {symbol} {dir_icon}\n"
        f"Precio: `{price:.6f}` | Qty: `{qty}`\n"
        f"_Fee maker 0.02% en vez de 0.05% taker_"
    )
    await send(msg)


async def notify_time_stop(
    symbol: str, direction: str, entry: float, mark: float,
    elapsed_min: int, progress: float
) -> None:
    """
    Trade cerrado por time stop — sin progreso tras MAX_HOLD_MINUTES.
    Previene el patrón FHEU/SXT/LDO: horas open con pérdida silenciosa.
    """
    dir_icon = "🟢" if direction == "LONG" else "🔴"
    pnl_pct  = (mark - entry) / entry * 100 if direction == "LONG" else (entry - mark) / entry * 100
    msg = (
        f"⏱ *TIME STOP* — {symbol} {dir_icon}\n"
        f"Cerrado tras `{elapsed_min}min` sin progreso\n"
        f"Entry: `{entry:.6f}` → Mark: `{mark:.6f}` (`{pnl_pct:+.2f}%`)\n"
        f"_Previene el patrón FHEU/SXT/LDO_"
    )
    await send(msg)
