"""telegram_client.py — Telegram notifications."""

import logging
import requests

log = logging.getLogger("telegram")


class TelegramClient:
    def __init__(self, token: str, chat_id: str):
        self.token   = token
        self.chat_id = chat_id
        self._url    = f"https://api.telegram.org/bot{token}/sendMessage"

    def _send(self, text: str):
        if not self.token or not self.chat_id:
            return
        try:
            requests.post(
                self._url,
                json={"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"},
                timeout=8,
            )
        except Exception as e:
            log.warning(f"Telegram: {e}")

    def startup(self, bot: str, tf: str, lev: int):
        self._send(f"🤖 <b>{bot}</b> iniciado\nTF: {tf}  Lev: {lev}x  SHORT_ONLY")

    def entry(self, bot: str, symbol: str, side: str, price: float,
              qty: float, stop: float, equity: float, score: int = 0):
        self._send(
            f"🔴 <b>{bot}</b> — ENTRADA {side}\n"
            f"<code>{symbol}</code>\n"
            f"Price:  {price:.6g}\n"
            f"Qty:    {qty}\n"
            f"Stop:   {stop:.6g}\n"
            f"Score:  {score}\n"
            f"Equity: {equity:.2f} USDT"
        )

    def exit_trade(self, bot: str, symbol: str, side: str, price: float,
                   reason: str, pnl: float):
        icon = "✅" if pnl >= 0 else "❌"
        self._send(
            f"{icon} <b>{bot}</b> — SALIDA {side}\n"
            f"<code>{symbol}</code>\n"
            f"Price:  {price:.6g}\n"
            f"Razón:  {reason}\n"
            f"PnL:    {pnl:+.2f} USDT"
        )

    def blocked(self, bot: str, reason: str):
        self._send(f"⛔ <b>{bot}</b> BLOQUEADO\n{reason}")

    def error(self, bot: str, msg: str):
        self._send(f"⚠️ <b>{bot}</b> ERROR\n{msg}")

    def info(self, bot: str, msg: str):
        self._send(f"ℹ️ <b>{bot}</b>\n{msg}")
