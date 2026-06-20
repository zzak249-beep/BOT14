"""
QF×JP Bot — Query Assistant: preguntas en lenguaje natural sobre los 3 bots
═══════════════════════════════════════════════════════════════════════════
SOLO se activa en renewed-love (el master). Escucha mensajes de texto
normales (no comandos, no empiezan con "/") en el chat de Telegram
configurado, junta /status + /journal de los 3 bots, y le pasa todo a
Claude para que responda en lenguaje natural — el equivalente a "Mr. Whale"
pero sobre tus propios bots.

telegram_client.py es solo de ENVÍO (fire-and-forget, sin getUpdates) — por
eso el listener vive en este módulo aparte, con su propio long polling.

Desactivado por defecto (QUERY_ASSISTANT_ENABLED=False). Variables nuevas
necesarias, SOLO en renewed-love:
  QUERY_ASSISTANT_ENABLED = true
  ANTHROPIC_API_KEY       = tu clave de la API de Claude
  JOYFUL_ART_URL          = URL pública del servicio joyful-art en Railway
  ZESTY_URL               = URL pública del servicio zesty-reverence en Railway

joyful-art y zesty-reverence NO necesitan ninguna de estas variables — solo
necesitan tener este archivo presente (main.py lo importa) y el endpoint
/journal nuevo respondiendo, nada más se activa en ellos.

Seguridad: solo procesa mensajes que vengan del TELEGRAM_CHAT_ID ya
configurado — cualquier otro chat_id se ignora en silencio, para no
exponer datos de tus cuentas si alguien más le escribe al bot.
═══════════════════════════════════════════════════════════════════════════
"""
import asyncio
import json
import logging
import os

import aiohttp

import config as C

log = logging.getLogger("query_assistant")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
JOYFUL_ART_URL     = os.getenv("JOYFUL_ART_URL", "").rstrip("/")
ZESTY_URL          = os.getenv("ZESTY_URL", "").rstrip("/")
CLAUDE_MODEL       = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6")

SYSTEM_PROMPT = (
    "Eres un asistente que responde preguntas sobre tres bots de trading "
    "algorítmico en BingX (perpetuos cripto): renewed-love, joyful-art y "
    "zesty-reverence. Te paso el estado actual (/status: balance, riesgo, "
    "posiciones abiertas) y las estadísticas del journal (/journal: win "
    "rate, PnL, rendimiento por filtro/tier/símbolo) de cada uno, en JSON. "
    "Responde la pregunta del usuario en español, de forma directa y breve "
    "— números concretos, sin rodeos. Basa la respuesta SOLO en los datos "
    "que te paso; si algo no se puede responder con ellos, dilo claramente "
    "en vez de inventar."
)


async def _fetch_json(session: aiohttp.ClientSession, url: str) -> dict:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                return await r.json()
    except Exception as e:
        log.debug("fetch %s error: %s", url, e)
    return {}


async def fetch_all_context(self_port: int) -> dict:
    """
    Junta /status + /journal de los 3 bots. renewed-love se consulta a sí
    mismo vía localhost — no necesita conocer su propia URL pública.
    """
    bases = {
        "renewed-love":    f"http://localhost:{self_port}",
        "joyful-art":      JOYFUL_ART_URL,
        "zesty-reverence": ZESTY_URL,
    }
    context: dict = {}
    async with aiohttp.ClientSession() as session:
        for name, base in bases.items():
            if not base:
                context[name] = {"error": "URL no configurada (revisa JOYFUL_ART_URL/ZESTY_URL)"}
                continue
            status  = await _fetch_json(session, f"{base}/status")
            journal = await _fetch_json(session, f"{base}/journal")
            context[name] = {"status": status, "journal": journal}
    return context


async def ask_claude(question: str, context: dict) -> str:
    if not ANTHROPIC_API_KEY:
        return "Falta ANTHROPIC_API_KEY en las variables de Railway de renewed-love."

    body = {
        "model":      CLAUDE_MODEL,
        "max_tokens": 700,
        "system":     SYSTEM_PROMPT,
        "messages": [{
            "role": "user",
            "content": (
                f"DATOS DE LOS 3 BOTS:\n{json.dumps(context, indent=2, default=str)}\n\n"
                f"PREGUNTA: {question}"
            ),
        }],
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key":         ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type":      "application/json",
                },
                json=body,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as r:
                data = await r.json()
                if r.status != 200:
                    log.error("Claude API error %s: %s", r.status, data)
                    return f"Error consultando a Claude (código {r.status})."
                parts = data.get("content", [])
                text = "".join(p.get("text", "") for p in parts if p.get("type") == "text")
                return text or "Claude no devolvió respuesta."
    except Exception as e:
        log.error("ask_claude error: %s", e)
        return f"Error consultando a Claude: {e}"


class TelegramListener:
    """
    Long polling sobre getUpdates de Telegram — necesario porque
    telegram_client.py es solo de envío. Ignora mensajes que no vengan de
    TELEGRAM_CHAT_ID, y los que empiezan con "/" (son comandos, no preguntas).
    """

    def __init__(self, self_port: int):
        self.self_port = self_port
        self._offset = 0
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=35))
        return self._session

    async def _send_reply(self, text: str):
        if not C.TELEGRAM_TOKEN or not C.TELEGRAM_CHAT_ID:
            return
        try:
            s = await self._get_session()
            url = f"https://api.telegram.org/bot{C.TELEGRAM_TOKEN}/sendMessage"
            await s.post(url, json={"chat_id": C.TELEGRAM_CHAT_ID, "text": text})
        except Exception as e:
            log.warning("telegram reply error: %s", e)

    async def _handle_question(self, text: str):
        log.info("[query] pregunta: %s", text)
        context = await fetch_all_context(self.self_port)
        answer  = await ask_claude(text, context)
        await self._send_reply(answer)

    async def poll_loop(self):
        if not getattr(C, 'QUERY_ASSISTANT_ENABLED', False):
            log.info("Query assistant desactivado (QUERY_ASSISTANT_ENABLED=false)")
            return
        if not ANTHROPIC_API_KEY:
            log.warning("Query assistant: falta ANTHROPIC_API_KEY — no arranca")
            return

        log.info("Query assistant iniciado — escuchando preguntas en Telegram")
        while True:
            try:
                s = await self._get_session()
                url = f"https://api.telegram.org/bot{C.TELEGRAM_TOKEN}/getUpdates"
                async with s.get(url, params={"offset": self._offset, "timeout": 25}) as r:
                    data = await r.json()

                for upd in data.get("result", []):
                    self._offset = upd["update_id"] + 1
                    msg  = upd.get("message", {})
                    text = msg.get("text", "")
                    chat = str(msg.get("chat", {}).get("id", ""))

                    if chat != str(C.TELEGRAM_CHAT_ID):
                        continue
                    if not text or text.startswith("/"):
                        continue

                    asyncio.create_task(self._handle_question(text))

            except Exception as e:
                log.warning("telegram poll error: %s", e)
                await asyncio.sleep(5)
