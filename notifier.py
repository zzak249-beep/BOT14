"""
Notifier v2 — mensajes Telegram ricos con todas las métricas.
"""
from __future__ import annotations
import logging
from typing import Optional
import requests
from strategy import Signal
from risk_manager import rr_ratio
from utils import fmt_price, fmt_pct, fmt_usdt, esc
import config as cfg

logger = logging.getLogger(__name__)


def _send(text: str):
    if not cfg.TELEGRAM_TOKEN or not cfg.TELEGRAM_CHAT_ID:
        logger.debug("Telegram not configured, skip")
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{cfg.TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": cfg.TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=12,
        )
        if not r.ok:
            logger.warning(f"Telegram {r.status_code}: {r.text[:200]}")
    except Exception as e:
        logger.warning(f"Telegram error: {e}")


# ── Señal de entrada ──────────────────────────────────────────
def send_signal(
    symbol:   str,
    signal:   Signal,
    entry:    float,
    sl:       float,
    tp:       float,
    qty:      float,
    balance:  float,
    order_id: Optional[str] = None,
):
    is_long  = signal.action == "BUY"
    emoji    = "🟢" if is_long else "🔴"
    direct   = "LONG  ▲" if is_long else "SHORT ▼"
    dry      = "<b>[PAPER]</b> " if cfg.DRY_RUN else ""
    rr       = rr_ratio(entry, sl, tp)
    sl_pct   = fmt_pct(entry, sl)
    tp_pct   = fmt_pct(entry, tp)
    risk_u   = balance * cfg.RISK_PCT / 100

    conf_lbl = ("🔥 Strong" if signal.confluence_score >= 60
                else "🟡 Moderate" if signal.confluence_score >= 30
                else "⚪ Weak")
    bias_lbl = "▲ Bullish" if signal.structure_bias > 0 else "▼ Bearish" if signal.structure_bias < 0 else "— Neutral"

    near = f"Fib cercano:  {esc(signal.near_fib)}" if signal.near_fib else ""
    oid  = f"\nOrder ID:     <code>{esc(order_id)}</code>" if order_id else ""

    msg = (
        f"{emoji} {dry}<b>{signal.action} — {esc(symbol)}</b>  [{cfg.TIMEFRAME}]\n"
        f"{'━'*30}\n"
        f"Dirección:    <b>{direct}</b>\n"
        f"Trigger:      <code>{esc(signal.trigger)}</code>\n"
        f"Confluencia:  {conf_lbl} ({signal.confluence_score:.0f}/100)\n"
        f"Estructura:   {bias_lbl}\n"
        f"{f'Zona:         Premium' if signal.in_premium else 'Zona:         Discount'}\n"
        f"{near}\n"
        f"{'━'*30}\n"
        f"Entrada:      <b>{fmt_price(entry)}</b> USDT\n"
        f"Stop Loss:    {fmt_price(sl)}  ({sl_pct})\n"
        f"Take Profit:  {fmt_price(tp)}  ({tp_pct})\n"
        f"R:R:          <b>1 : {rr}</b>\n"
        f"{'━'*30}\n"
        f"Cantidad:     {qty:.6f}\n"
        f"Riesgo:       {risk_u:.2f} USDT  ({cfg.RISK_PCT}%)\n"
        f"Leverage:     {cfg.LEVERAGE}x  ({cfg.MARGIN_TYPE})\n"
        f"ATR(14):      {fmt_price(signal.atr)}"
        f"{oid}"
    )
    _send(msg)


# ── Cierre de posición ────────────────────────────────────────
def send_close(
    symbol:      str,
    action:      str,  # BUY | SELL (la acción de entrada)
    entry:       float,
    exit_price:  float,
    qty:         float,
    reason:      str = "SL/TP",
):
    is_long  = action == "BUY"
    pnl_raw  = (exit_price - entry) * qty * (1 if is_long else -1) * cfg.LEVERAGE
    won      = pnl_raw > 0
    emoji    = "✅" if won else "❌"
    pnl_str  = fmt_usdt(pnl_raw)
    pct      = fmt_pct(entry, exit_price) if is_long else fmt_pct(exit_price, entry)

    _send(
        f"{emoji} <b>CLOSE — {esc(symbol)}</b>  [{reason}]\n"
        f"{'━'*28}\n"
        f"Entrada:   {fmt_price(entry)}\n"
        f"Salida:    {fmt_price(exit_price)}  ({pct})\n"
        f"Cantidad:  {qty:.6f}\n"
        f"P&amp;L:   <b>{pnl_str}</b>"
    )


# ── Startup ───────────────────────────────────────────────────
def send_startup(symbols_count: int, balance: float, equity: float):
    mode = "🟡 PAPER TRADING" if cfg.DRY_RUN else "🔴 LIVE TRADING"
    _send(
        f"🤖 <b>FibStruct Bot ON</b>  [{mode}]\n"
        f"{'━'*30}\n"
        f"Símbolos:    {symbols_count}\n"
        f"TF:          {cfg.TIMEFRAME}\n"
        f"Balance:     {balance:.2f} USDT\n"
        f"Equity:      {equity:.2f} USDT\n"
        f"Max pos:     {cfg.MAX_POSITIONS}\n"
        f"Riesgo/op:   {cfg.RISK_PCT}%\n"
        f"Leverage:    {cfg.LEVERAGE}x  ({cfg.MARGIN_TYPE})\n"
        f"SL:          {cfg.SL_METHOD}  ({cfg.SL_ATR_MULT}×ATR)\n"
        f"TP:          {cfg.TP_METHOD}  ({cfg.TP_ATR_MULT}×ATR)\n"
        f"Min R:R:     {cfg.MIN_RR}\n"
        f"Loss limit:  {cfg.DAILY_LOSS_LIMIT_PCT}%/día\n"
        f"Workers:     {cfg.FETCH_WORKERS} threads\n"
        f"Scan cada:   {cfg.SCAN_INTERVAL}s"
    )


# ── Resumen diario ────────────────────────────────────────────
def send_daily_summary(stats: dict, equity: float):
    trades = stats.get("trades", 0)
    wins   = stats.get("wins", 0)
    losses = stats.get("losses", 0)
    pnl    = stats.get("pnl", 0.0)
    wr     = wins / trades * 100 if trades else 0
    emoji  = "🟢" if pnl >= 0 else "🔴"

    _send(
        f"📊 <b>Resumen diario</b>\n"
        f"{'━'*28}\n"
        f"Trades:    {trades}  (W:{wins} / L:{losses})\n"
        f"Win Rate:  {wr:.1f}%\n"
        f"{emoji} P&amp;L: <b>{fmt_usdt(pnl)}</b>\n"
        f"Equity:    {equity:.2f} USDT"
    )


# ── Alertas ───────────────────────────────────────────────────
def send_daily_loss_limit(pnl: float, limit_pct: float):
    _send(
        f"⛔ <b>Límite de pérdida diaria alcanzado</b>\n"
        f"Pérdida: {fmt_usdt(pnl)}\n"
        f"Límite:  {limit_pct}% del balance\n"
        f"No se abrirán nuevas posiciones hoy."
    )

def send_error(msg: str):
    _send(f"🔴 <b>ERROR</b>\n<code>{esc(msg[:500])}</code>")

def send_info(msg: str):
    _send(f"ℹ️ {esc(msg)}")
