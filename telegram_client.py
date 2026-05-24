"""
Cliente Telegram — Notificaciones del bot QF×JP
"""
import asyncio
import logging
import aiohttp
from datetime import datetime

log = logging.getLogger("Telegram")

API = "https://api.telegram.org/bot{token}/{method}"


class TelegramClient:
    def __init__(self, token: str, chat_id: str):
        self.token   = token
        self.chat_id = chat_id
        self._session: aiohttp.ClientSession | None = None

    async def _sess(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            )
        return self._session

    async def send_message(self, text: str, parse_mode: str = "Markdown"):
        url  = API.format(token=self.token, method="sendMessage")
        sess = await self._sess()
        try:
            async with sess.post(url, json={
                "chat_id"   : self.chat_id,
                "text"      : text,
                "parse_mode": parse_mode,
            }) as r:
                if r.status != 200:
                    log.error(f"TG error {r.status}: {await r.text()}")
        except Exception as e:
            log.error(f"TG send failed: {e}")

    async def send_entry(self, symbol: str, sig: dict, price: float,
                         size: float, order_id: str):
        d      = sig["direction"]
        tier   = sig["tier"]
        conv   = sig["conviction"]
        sl     = sig["sl"]
        tp     = sig.get("tp")
        ns     = sig.get("norm_score", 0)

        tier_emoji = {"SUP": "⭐", "FUEL": "🔥", "STD": "📍"}.get(tier, "")
        dir_emoji  = "🟢" if d == "LONG" else "🔴"

        # Barra de convicción visual
        bars  = "█" * conv + "░" * (10 - conv)
        rr    = abs((tp - price) / (price - sl)) if tp and sl and (price - sl) != 0 else 0

        msg = (
            f"{dir_emoji} *{tier_emoji} {d} {tier}* — `{symbol}`\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"💰 Entrada : `{price:.4f}`\n"
            f"🛡 Stop-Loss: `{sl:.4f}`\n"
            f"🎯 Take-Prof: `{tp:.4f if tp else '—'}`\n"
            f"📐 R/R       : `{rr:.2f}`\n"
            f"📦 Tamaño   : `{size:.4f}`\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"🧠 Score : `{round(ns*100)}/100`\n"
            f"🏆 Conv  : `[{bars}] {conv}/10`\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"📋 *Componentes*\n"
            f"  HTF  : {'✅' if sig.get('htf_bull' if d=='LONG' else 'htf_bear') else '❌'}\n"
            f"  Asym : {'✅' if sig.get('asym_bull' if d=='LONG' else 'asym_bear') else '❌'}\n"
            f"  CVD  : {'✅' if sig.get('cvd_rising' if d=='LONG' else 'cvd_rising')==(d=='LONG') else '❌'}\n"
            f"  TL   : {'✅' if sig.get('tl_break_long' if d=='LONG' else 'tl_break_short') else '—'}\n"
            f"  FVG  : {'✅' if sig.get('in_bull_fvg' if d=='LONG' else 'in_bear_fvg') else '—'}\n"
            f"  OB   : {'✅' if sig.get('in_bull_ob' if d=='LONG' else 'in_bear_ob') else '—'}\n"
            f"  SQ   : {'✅' if sig.get('sq_bull' if d=='LONG' else 'sq_bear') else '—'}\n"
            f"  DP   : {'✅' if sig.get('dp_buy' if d=='LONG' else 'dp_sell') else '—'}\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"🆔 Order: `{order_id}`\n"
            f"⏱ {datetime.utcnow().strftime('%H:%M:%S')} UTC"
        )
        await self.send_message(msg)

    async def send_close(self, symbol: str, side: str, entry: float,
                         exit_price: float, pnl_pct: float, reason: str):
        emoji = "💹" if pnl_pct >= 0 else "💸"
        sign  = "+" if pnl_pct >= 0 else ""
        msg   = (
            f"{emoji} *CIERRE {side}* — `{symbol}`\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"📥 Entrada : `{entry:.4f}`\n"
            f"📤 Salida  : `{exit_price:.4f}`\n"
            f"📊 PnL     : `{sign}{pnl_pct:.2f}%`\n"
            f"📝 Razón   : {reason}\n"
            f"⏱ {datetime.utcnow().strftime('%H:%M:%S')} UTC"
        )
        await self.send_message(msg)

    async def send_status(self, balance: float, positions: dict):
        pos_lines = ""
        for sym, p in positions.items():
            pos_lines += (
                f"  • `{sym}` {p['side']} | "
                f"Entry: `{p['entry']:.4f}` | "
                f"SL: `{p['sl']:.4f}` | "
                f"Conv: `{p['conv']}/10`\n"
            )
        if not pos_lines:
            pos_lines = "  _Sin posiciones abiertas_\n"

        msg = (
            f"📊 *QF×JP Bot — Reporte Horario*\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"💵 Balance USDT: `{balance:.2f}`\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"📌 *Posiciones activas:*\n{pos_lines}"
            f"⏱ {datetime.utcnow().strftime('%H:%M UTC')}"
        )
        await self.send_message(msg)

    async def send_error(self, error: str):
        msg = (
            f"⚠️ *Bot Error*\n"
            f"```\n{error[:300]}\n```\n"
            f"⏱ {datetime.utcnow().strftime('%H:%M:%S')} UTC"
        )
        await self.send_message(msg)

    async def send_signal_only(self, symbol: str, sig: dict, price: float):
        """Modo señales sin ejecución real."""
        d    = sig.get("direction")
        if not d:
            return
        tier = sig.get("tier", "STD")
        conv = sig.get("conviction", 0)
        sl   = sig.get("sl", 0)
        tp   = sig.get("tp")
        emoji = {"SUP": "⭐", "FUEL": "🔥", "STD": "📍"}.get(tier, "")
        dir_e = "🟢" if d == "LONG" else "🔴"
        msg = (
            f"{dir_e} *{emoji} SEÑAL {d} [{tier}]* `{symbol}`\n"
            f"Precio: `{price:.4f}` | SL: `{sl:.4f}` | TP: `{tp:.4f if tp else '—'}`\n"
            f"Convicción: `{conv}/10`\n"
            f"⏱ {datetime.utcnow().strftime('%H:%M:%S')} UTC"
        )
        await self.send_message(msg)

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
