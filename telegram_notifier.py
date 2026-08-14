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

    async def send_direct(self, text: str) -> bool:
        """Envio directo, SIN cola -- para el respaldo diario especificamente.
        send() normal solo encola; el worker que envia de verdad captura
        sus propios errores y solo los registra en el log, nunca se los
        devuelve a quien llamo a send(). Eso basta para señales/cierres
        (perder una notificacion no es grave), pero NO para el respaldo:
        si se marca 'enviado' sin haberse entregado de verdad, el dia
        queda sin respaldo real y nadie se entera hasta que ya sea tarde.
        Devuelve True solo con confirmacion real de entrega (HTTP 2xx)."""
        if not self.enabled:
            log.warning("Telegram deshabilitado -- no se puede confirmar el respaldo diario.")
            return False
        if not self._session:
            log.error("Sesion de Telegram no iniciada -- no se puede enviar el respaldo diario.")
            return False
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            async with self._session.post(
                url,
                json={"chat_id": self.chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status < 300:
                    return True
                body = await resp.text()
                log.error("Telegram %d en respaldo diario: %s", resp.status, body[:200])
                return False
        except aiohttp.ClientError as e:
            log.error("Error de red en respaldo diario: %s", e)
            return False

    async def send_direct(self, text: str) -> bool:
        """Envio directo, SIN cola -- devuelve si de verdad se entrego.
        send() solo encola y vuelve al momento; un try/except alrededor
        de eso nunca detectaria un fallo real de Telegram (encolar casi
        nunca lanza excepcion), asi que marcar 'respaldo enviado' tras
        un send() normal seria una falsa sensacion de seguridad. Solo
        para el respaldo diario, donde importa la confirmacion real --
        el resto de notificaciones (señales, cierres) siguen con send(),
        donde el rate-limit de la cola importa mas que la certeza."""
        if not self.enabled or not self._session:
            return False
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            async with self._session.post(
                url,
                json={"chat_id": self.chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    return True
                body = await resp.text()
                log.error("Telegram %d en respaldo diario: %s", resp.status, body[:200])
                return False
        except aiohttp.ClientError as e:
            log.error("Error de red enviando respaldo diario: %s", e)
            return False

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


def format_backup(snapshot_json: str, total_w: int, total_l: int, win_rate: float) -> str:
    """Respaldo diario -- si el volumen persistente de Railway se pierde
    (como ya paso una vez: racha de miles de operaciones en Telegram vs.
    un puñado en el estado que arranco despues), este mensaje es el unico
    registro fuera de Railway con el que reconstruir el archivo a mano.
    Telegram limita ~4096 caracteres por mensaje -- el snapshot agregado
    (sin symbol_states NI positions, ver backup_snapshot_json) se queda
    muy por debajo de eso en la practica."""
    return (
        f"🗄 <b>Respaldo diario</b>\n"
        f"Total: {total_w}W/{total_l}L ({win_rate:.0f}%)\n\n"
        f"Si el volumen persistente se pierde, copia el bloque de abajo "
        f"y pégalo como <code>state.json</code> en /data para restaurar "
        f"el historial hasta este punto (perderás lo cerrado después Y "
        f"las posiciones que estuvieran abiertas en ese momento -- no "
        f"vienen incluidas aquí a propósito, para mantener el mensaje "
        f"dentro del límite de Telegram):\n\n"
        f"<pre>{snapshot_json}</pre>"
    )
