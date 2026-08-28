"""
telegram_notifier.py

Thin async wrapper around the Telegram Bot API's sendMessage call. Failures
are logged, never raised - a Telegram outage must never stop the trading
loop or crash the bot.
"""

import logging

import aiohttp

logger = logging.getLogger("telegram")


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self._enabled = bool(bot_token and chat_id)
        if not self._enabled:
            logger.warning("Telegram not configured (TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID missing) - notifications disabled.")

    async def send(self, text: str):
        if not self._enabled:
            logger.info("[telegram disabled] %s", text.replace("\n", " | "))
            return
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.error("Telegram send failed (HTTP %s): %s", resp.status, body[:300])
        except Exception as e:  # never let a notification failure break the bot
            logger.error("Telegram send exception: %s", e)
