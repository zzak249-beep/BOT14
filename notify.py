"""
Telegram y estado en disco.

El estado va en /data/state.json (volumen de Railway). Sin volumen
montado, el contador de operaciones y el circuit breaker se reinician
en cada despliegue — que es exactamente lo que ya pasó una vez en este
proyecto y borró un historial entero.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import httpx

import config

log = logging.getLogger("notify")


class Telegram:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._c = client
        self.enabled = bool(config.TELEGRAM_TOKEN and config.TELEGRAM_CHAT_ID)
        if not self.enabled:
            log.warning("Telegram sin configurar: las señales solo saldrán en el log")

    async def send(self, text: str) -> bool:
        """Devuelve si se entregó DE VERDAD, no si se intentó."""
        if not self.enabled:
            log.info("[telegram-off] %s", text)
            return False
        url = f"https://api.telegram.org/bot{config.TELEGRAM_TOKEN}/sendMessage"
        try:
            r = await self._c.post(
                url,
                json={
                    "chat_id": config.TELEGRAM_CHAT_ID,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=15,
            )
            ok = r.status_code == 200 and r.json().get("ok") is True
            if not ok:
                log.error("Telegram rechazó el envío: %s %s", r.status_code, r.text[:200])
            return ok
        except Exception as exc:  # noqa: BLE001
            log.error("Telegram falló: %s", exc)
            return False


class State:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.data: dict[str, Any] = {
            "closed_trades": 0,
            "wins": 0,
            "losses": 0,
            "consecutive_losses": 0,
            "cooldown_until": 0,
            "open": {},          # symbol -> posición confirmada por el exchange
            "pending": {},       # symbol -> orden limitada enviada, aún sin ejecutar
            "last_signal": {},   # symbol -> timestamp de la última señal
        }
        self._load()

    def _load(self) -> None:
        try:
            if self.path.exists():
                cargado = json.loads(self.path.read_text())
                # ESTADO DE OTRO BOT: si el archivo viene de un bot
                # anterior que compartía volumen, sus contadores producen
                # incoherencias tipo "0 cerradas · 4 aciertos". Se
                # detecta y se descarta la parte numérica en vez de
                # arrastrar un historial que no es de este bot.
                cerradas = int(cargado.get("closed_trades", 0) or 0)
                wins = int(cargado.get("wins", 0) or 0)
                losses = int(cargado.get("losses", 0) or 0)
                if wins + losses > cerradas:
                    log.warning(
                        "Estado incoherente (cerradas=%d, aciertos=%d, fallos=%d): "
                        "parece de otro bot. Se reinician los contadores.",
                        cerradas, wins, losses,
                    )
                    for k in ("closed_trades", "wins", "losses", "consecutive_losses"):
                        cargado.pop(k, None)
                    cargado["contadores_reiniciados"] = True
                self.data.update(cargado)
                log.info("Estado cargado de %s", self.path)
            else:
                log.warning(
                    "No hay estado previo en %s — si Railway no tiene volumen montado ahí, "
                    "se perderá en cada despliegue",
                    self.path,
                )
        except Exception as exc:  # noqa: BLE001
            log.error("No se pudo leer el estado: %s", exc)

    def save(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.data, indent=2))
            os.replace(tmp, self.path)  # atómico: o el viejo o el nuevo, nunca a medias
        except Exception as exc:  # noqa: BLE001
            log.error("No se pudo guardar el estado: %s", exc)
