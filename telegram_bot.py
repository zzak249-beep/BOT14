"""
Telegram Bot — QF Machine × JP Fusion
Señales, control y monitoreo en tiempo real
"""
import asyncio
import logging
from datetime import datetime
from typing import Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes
)
from telegram.constants import ParseMode

logger = logging.getLogger(__name__)


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str, risk_manager=None, bot_state=None):
        self.token       = token
        self.chat_id     = chat_id
        self.risk        = risk_manager
        self.bot_state   = bot_state  # referencia al estado global del bot
        self._app: Optional[Application] = None

    # ─────────────────────────────────────────────────────────
    #  MENSAJES DE SEÑAL
    # ─────────────────────────────────────────────────────────
    async def send_signal(self, signal, symbol: str, qty: float,
                          tp: float, paper: bool):
        tier_emoji = {
            "SUPREMA": "⭐⭐⭐",
            "FUEL":    "🔥",
            "STD":     "▶️",
        }.get(signal.tier, "")

        dir_emoji  = "🟢 LONG" if signal.direction == "LONG" else "🔴 SHORT"
        mode_tag   = "📋 PAPER" if paper else "💵 REAL"
        conv_bar   = "█" * signal.conviction + "░" * (10 - signal.conviction)

        d = signal.details
        text = (
            f"{tier_emoji} *{signal.tier} {dir_emoji}* {tier_emoji}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 *{symbol}*  |  {mode_tag}\n"
            f"⏱ `{datetime.utcnow().strftime('%H:%M:%S')} UTC`\n\n"
            f"📊 *Precio entrada:* `{signal.entry_price:.6f}`\n"
            f"🛑 *Stop Loss:*     `{signal.sl_price:.6f}`\n"
            f"🎯 *Take Profit:*   `{tp:.6f}`\n"
            f"📦 *Tamaño:*        `{qty:.6f}`\n\n"
            f"🧠 *Convicción:*  `{conv_bar}` {signal.conviction}/10\n"
            f"📈 *Score:*        `{d.get('norm_score',0):+.1f}/100`\n\n"
            f"*— Filtros activos —*\n"
            f"{'✅' if d.get('htf_bull') or d.get('htf_bear') else '❌'} HTF Régimen\n"
            f"{'✅' if d.get('sig_alive') else '❌'} Señal viva\n"
            f"{'✅' if d.get('exec_ok') else '❌'} Ejecución limpia\n"
            f"{'✅' if d.get('asym_bull') or d.get('asym_bear') else '❌'} Asimetría momentum\n"
            f"{'✅' if d.get('sell_exhausted') or d.get('buy_exhausted', False) else '❌'} Agotamiento swing\n"
            f"{'✅' if d.get('tl_long') or d.get('tl_short') else '⬜'} Ruptura trendline\n"
            f"{'✅' if d.get('dp_buy') or d.get('dp_sell') else '⬜'} Dark Pool bloque\n"
            f"{'✅' if d.get('cvd_bull_div') or d.get('cvd_bear_div') else '⬜'} CVD divergencia\n"
            f"{'✅' if d.get('sq_bull') or d.get('sq_bear') else '⬜'} Squeeze fire\n"
            f"{'✅' if d.get('in_bull_fvg') or d.get('in_bear_fvg') else '⬜'} FVG zona\n"
            f"{'✅' if d.get('in_bull_ob') or d.get('in_bear_ob') else '⬜'} Order Block retest\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"*ATR:* `{d.get('atr', 0):.6f}`  "
            f"*MOM:* `{d.get('f_mom',0):+.0f}`  "
            f"*REV:* `{d.get('f_rev',0):+.0f}`"
        )
        await self._send(text)

    async def send_trade_close(self, symbol: str, direction: str, pnl: float,
                               entry: float, exit_price: float, reason: str, paper: bool):
        emoji  = "💚" if pnl > 0 else "❤️"
        mode   = "📋 PAPER" if paper else "💵 REAL"
        pct    = (exit_price - entry) / entry * 100 * (1 if direction == "LONG" else -1)
        text = (
            f"{emoji} *CIERRE {direction}* — {mode}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 *{symbol}*\n"
            f"📥 Entrada: `{entry:.6f}`\n"
            f"📤 Salida:  `{exit_price:.6f}`  ({pct:+.2f}%)\n"
            f"💰 PnL: `{pnl:+.2f} USDT`\n"
            f"📋 Motivo: _{reason}_\n"
            f"⏱ `{datetime.utcnow().strftime('%H:%M:%S')} UTC`"
        )
        await self._send(text)

    async def send_alert(self, msg: str):
        await self._send(f"⚠️ *ALERTA*\n{msg}")

    async def send_circuit_breaker(self, reason: str):
        text = (
            f"🔴 *CIRCUIT BREAKER ACTIVADO*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{reason}\n\n"
            f"El bot ha dejado de operar.\n"
            f"Usa /reset para reactivar manualmente."
        )
        await self._send(text)

    async def send_status(self, status: dict, paper: bool):
        dd_bar = "█" * int(status['drawdown_pct'] / 5) + "░" * (20 - int(status['drawdown_pct'] / 5))
        circuit = "🔴 ABIERTO" if status['circuit_open'] else "🟢 OK"
        mode    = "📋 PAPER MODE" if paper else "💵 LIVE MODE"
        text = (
            f"📊 *QF Bot — Estado* | {mode}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💼 Equity:     `{status['equity']:.2f} USDT`\n"
            f"🏔 Pico:       `{status['peak_equity']:.2f} USDT`\n"
            f"📉 Drawdown:   `{status['drawdown_pct']:.1f}%`\n"
            f"`{dd_bar}`\n\n"
            f"📅 PnL hoy:    `{status['daily_pnl']:+.2f} USDT`\n"
            f"🔢 Ops hoy:    `{status['daily_trades']}`\n"
            f"❌ Consec. pérdidas: `{status['consec_losses']}`\n\n"
            f"⚡ Circuit:    {circuit}\n"
            f"Límite diario: `{status['daily_loss_limit']}`\n"
            f"Límite DD:     `{status['max_dd_limit']}`"
        )
        if status['circuit_open']:
            text += f"\n\n🔴 Razón: _{status['circuit_reason']}_"
        await self._send(text)

    # ─────────────────────────────────────────────────────────
    #  COMANDOS
    # ─────────────────────────────────────────────────────────
    async def start_polling(self):
        """Arranca el polling de comandos Telegram en background"""
        self._app = Application.builder().token(self.token).build()

        self._app.add_handler(CommandHandler("start",   self._cmd_start))
        self._app.add_handler(CommandHandler("status",  self._cmd_status))
        self._app.add_handler(CommandHandler("reset",   self._cmd_reset))
        self._app.add_handler(CommandHandler("pause",   self._cmd_pause))
        self._app.add_handler(CommandHandler("resume",  self._cmd_resume))
        self._app.add_handler(CommandHandler("mode",    self._cmd_mode))
        self._app.add_handler(CommandHandler("help",    self._cmd_help))
        self._app.add_handler(CallbackQueryHandler(self._callback))

        await self._app.initialize()
        await self._app.start()
        await self._app.updater.start_polling(drop_pending_updates=True)
        logger.info("✅ Telegram polling iniciado")

    async def stop_polling(self):
        if self._app:
            await self._app.updater.stop()
            await self._app.stop()

    async def _cmd_start(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update): return
        kb = [
            [InlineKeyboardButton("📊 Estado", callback_data="status"),
             InlineKeyboardButton("⏸ Pausar",  callback_data="pause")],
            [InlineKeyboardButton("▶️ Reanudar", callback_data="resume"),
             InlineKeyboardButton("🔄 Reset CB", callback_data="reset")],
        ]
        await update.message.reply_text(
            "🤖 *QF Machine × JP Fusion Bot v3*\n\n"
            "Sistema activo. Usa los botones o comandos:\n"
            "/status — estado actual\n"
            "/pause  — pausar trading\n"
            "/resume — reanudar\n"
            "/reset  — resetear circuit breaker\n"
            "/mode   — ver modo paper/live",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(kb)
        )

    async def _cmd_status(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update): return
        if self.risk:
            paper = self.bot_state.get("paper", True) if self.bot_state else True
            await self.send_status(self.risk.status_dict(), paper)

    async def _cmd_reset(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update): return
        if self.risk:
            self.risk.reset_circuit_manual()
            await update.message.reply_text("✅ Circuit breaker reseteado.", parse_mode=ParseMode.MARKDOWN)

    async def _cmd_pause(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update): return
        if self.bot_state is not None:
            self.bot_state["paused"] = True
            await update.message.reply_text("⏸ Bot *pausado*. No se abrirán nuevas posiciones.",
                                            parse_mode=ParseMode.MARKDOWN)

    async def _cmd_resume(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update): return
        if self.bot_state is not None:
            self.bot_state["paused"] = False
            await update.message.reply_text("▶️ Bot *reanudado*.", parse_mode=ParseMode.MARKDOWN)

    async def _cmd_mode(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update): return
        paper = self.bot_state.get("paper", True) if self.bot_state else True
        mode  = "📋 PAPER MODE" if paper else "💵 LIVE MODE"
        await update.message.reply_text(
            f"Modo actual: *{mode}*\n\n"
            f"Para cambiar edita `PAPER_MODE` en las variables de entorno y reinicia.",
            parse_mode=ParseMode.MARKDOWN
        )

    async def _cmd_help(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not self._auth(update): return
        await update.message.reply_text(
            "*Comandos disponibles:*\n"
            "/start  — panel principal\n"
            "/status — equity, PnL, drawdown\n"
            "/pause  — detener nuevas entradas\n"
            "/resume — reanudar\n"
            "/reset  — desbloquear circuit breaker\n"
            "/mode   — ver modo paper vs live\n"
            "/help   — esta ayuda",
            parse_mode=ParseMode.MARKDOWN
        )

    async def _callback(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        await q.answer()
        if not self._auth_query(q): return
        data = q.data
        if data == "status":   await self._cmd_status(q, ctx)
        elif data == "pause":  await self._cmd_pause(q, ctx)
        elif data == "resume": await self._cmd_resume(q, ctx)
        elif data == "reset":  await self._cmd_reset(q, ctx)

    # ─────────────────────────────────────────────────────────
    #  UTILS
    # ─────────────────────────────────────────────────────────
    def _auth(self, update: Update) -> bool:
        uid = str(update.effective_user.id)
        allowed = str(self.chat_id)
        if uid != allowed:
            logger.warning(f"Acceso no autorizado: {uid}")
            return False
        return True

    def _auth_query(self, q) -> bool:
        uid = str(q.from_user.id)
        return uid == str(self.chat_id)

    async def _send(self, text: str):
        if self._app:
            await self._app.bot.send_message(
                chat_id=self.chat_id,
                text=text,
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            # Fallback via aiohttp si no hay app inicializada
            import aiohttp
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            payload = {"chat_id": self.chat_id, "text": text,
                       "parse_mode": "Markdown"}
            async with aiohttp.ClientSession() as s:
                await s.post(url, json=payload)
