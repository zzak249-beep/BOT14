"""
notifier.py
-----------
Envío de mensajes a Telegram usando la API HTTP directa (sin dependencias
pesadas). Cualquier error de red al notificar se registra pero NUNCA debe
tumbar el bot: las notificaciones son secundarias a la lógica de trading.

Incluye `send_throttled`: evita repetir el MISMO aviso (p. ej. un error de
red que persiste durante varios ciclos seguidos) antes de que pase un
tiempo mínimo configurable, para no saturar el chat de Telegram.
"""

import logging
import time

import requests

logger = logging.getLogger("bot")


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str):
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self.chat_id = chat_id
        self._last_sent_at = {}  # key -> timestamp (para send_throttled)

    def send(self, text: str) -> None:
        try:
            resp = requests.post(
                f"{self.base_url}/sendMessage",
                data={
                    "chat_id": self.chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=15,
            )
            if resp.status_code != 200:
                logger.warning(f"Telegram respondió {resp.status_code}: {resp.text}")
        except Exception as exc:  # nunca debe romper el bot por un fallo de notificación
            logger.warning(f"No se pudo enviar el mensaje a Telegram: {exc}")

    def send_throttled(self, text: str, key: str, cooldown_minutes: float) -> None:
        """Como send(), pero si ya se envió un mensaje con la misma `key` hace
        menos de `cooldown_minutes`, solo lo registra en el log (no reenvía)."""
        now = time.time()
        last = self._last_sent_at.get(key)
        if last is not None and (now - last) < cooldown_minutes * 60:
            logger.info(f"(aviso repetido de '{key}' silenciado por cooldown) {text}")
            return
        self._last_sent_at[key] = now
        self.send(text)
