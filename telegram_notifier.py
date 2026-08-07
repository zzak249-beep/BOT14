"""
Notificador de Telegram con cola y limite de velocidad.

Escanear 500+ simbolos puede generar varias señales en el mismo
ciclo. Mandarlas todas a la vez dispara 429 de Telegram (ya paso
con MODE=SIGNAL en un bot anterior). Aqui se encolan y se envian
espaciadas.
"""
import asyncio
import logging
from typing import Optional

import aiohttp

import config as cfg

log = logging.getLogger("telegram")

MIN_INTERVAL_SEC = 1.2


class TelegramNotifier:
    def __init__(self, token: Optional[str], chat_id: Optional[str]):
        self.token = token
        self.chat_id = chat_id
        self._queue: asyncio.Queue = asyncio.Queue()
        self._session: Optional[aiohttp.ClientSession] = None
        self._task: Optional[asyncio.Task] = None
        self.enabled = bool(token and chat_id)

    async def start(self) -> None:
        if not self.enabled:
            log.warning("Telegram deshabilitado (faltan token o chat_id).")
            return
        self._session = aiohttp.ClientSession()
        self._task = asyncio.create_task(self._worker())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
        if self._session:
            await self._session.close()

    async def send(self, text: str) -> None:
        if not self.enabled:
            log.info("[telegram deshabilitado] %s", text.replace("\n", " | "))
            return
        await self._queue.put(text)

    async def _worker(self) -> None:
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        while True:
            text = await self._queue.get()
            try:
                async with self._session.post(
                    url,
                    json={"chat_id": self.chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status == 429:
                        body = await resp.json(content_type=None)
                        retry_after = (body.get("parameters") or {}).get("retry_after", 3)
                        log.warning("Telegram 429, reintentando en %ss", retry_after)
                        await asyncio.sleep(retry_after)
                        await self._queue.put(text)
                    elif resp.status >= 400:
                        body = await resp.text()
                        log.error("Telegram %d: %s", resp.status, body[:200])
            except aiohttp.ClientError as e:
                log.error("Error de red enviando a Telegram: %s", e)
            await asyncio.sleep(MIN_INTERVAL_SEC)


def format_signal(sig, mode: str) -> str:
    emoji = "🟢" if sig.direction == "LONG" else "🔴"
    tag = "SEÑAL" if mode == "SIGNAL" else "ENTRADA"
    extra = ""
    if sig.funding_rate is not None:
        extra += f"\nFunding: <code>{sig.funding_rate * 100:.4f}%</code>"
    if sig.oi_change_pct is not None:
        extra += f"  OI: <code>{sig.oi_change_pct:+.2f}%</code>"
    if sig.lead_confirmed is not None:
        extra += f"\n{'✅' if sig.lead_confirmed else '⚠️'} Lead {cfg.LEAD_SYMBOL}: {'a favor' if sig.lead_confirmed else 'en contra'}"
    return (
        f"{emoji} <b>{tag} {sig.direction}</b> — {sig.symbol}\n"
        f"Ruta: {sig.path}  ·  KZ: {sig.kill_zone}  ·  R:R {sig.rr:.2f}\n"
        f"Entrada: <code>{sig.entry:g}</code>\n"
        f"SL: <code>{sig.sl:g}</code>\n"
        f"TP1: <code>{sig.tp1:g}</code>  TP2: <code>{sig.tp2:g}</code>"
        f"{extra}"
    )


def format_position_closed(symbol: str, reason: str, pnl_pct: Optional[float]) -> str:
    icon = "✅" if (pnl_pct or 0) > 0 else "❌" if (pnl_pct or 0) < 0 else "⚪"
    pnl_txt = f" ({pnl_pct:+.2f}%)" if pnl_pct is not None else ""
    return f"{icon} <b>Cierre {reason}</b> — {symbol}{pnl_txt}"
