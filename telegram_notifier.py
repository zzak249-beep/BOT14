"""
telegram_notifier.py — Envío de notificaciones a Telegram.

Se usa `requests` directo contra la Bot API (sin dependencias extra)
porque el bot solo necesita ENVIAR mensajes, nunca recibir comandos.
"""

import logging

import requests

logger = logging.getLogger("wavelet_bot.telegram")


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str, timeout: float = 10.0):
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self.chat_id = chat_id
        self.timeout = timeout

    def send(self, text: str) -> None:
        try:
            resp = requests.post(
                f"{self.base_url}/sendMessage",
                json={"chat_id": self.chat_id, "text": text, "parse_mode": "HTML",
                      "disable_web_page_preview": True},
                timeout=self.timeout,
            )
            if not resp.ok:
                logger.warning("Telegram respondió %s: %s", resp.status_code, resp.text)
        except requests.RequestException as exc:
            logger.warning("No se pudo enviar mensaje a Telegram: %s", exc)

    # ── Formateadores ────────────────────────────────────────────────
    def signal(self, symbol: str, side: str, price: float, sl: float, tp: float,
               executed: bool, reason: str = "") -> None:
        emoji = "🟢" if side == "LONG" else "🔴"
        estado = "✅ ejecutada en BingX" if executed else ("⚠️ solo señal (no ejecutada" + (f": {reason}" if reason else "") + ")")
        self.send(
            f"{emoji} <b>{side}</b> {symbol}\n"
            f"Estrategia: Wavelet MRA Haar 5m\n"
            f"Precio: {price:.6g}\n"
            f"SL: {sl:.6g}   TP: {tp:.6g}\n"
            f"{estado}"
        )

    def exit_notice(self, symbol: str, side: str, price: float, reason: str = "SL/TP") -> None:
        self.send(
            f"⚪ <b>Cierre {side}</b> {symbol}\n"
            f"Precio aprox.: {price:.6g}\n"
            f"Motivo: {reason}"
        )

    def error(self, context: str, detail: str) -> None:
        self.send(f"❌ <b>Error</b> ({context})\n{detail}")

    def info(self, text: str) -> None:
        self.send(f"ℹ️ {text}")
