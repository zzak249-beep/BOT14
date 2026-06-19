"""
QF×JP Bot — WebSocket Market Data v1.0
═══════════════════════════════════════════════════════════════════════════════
Reduce la latencia de detección de hasta SCAN_INTERVAL segundos (60s con
polling REST) a near-instant, suscribiéndose al stream de klines de BingX
en tiempo real.

DISEÑO DELIBERADAMENTE CONSERVADOR — OPCIONAL Y AISLADO:
  Tras el crash de los 3 bots por un módulo faltante, este componente se
  diseñó para que NUNCA pueda tumbar el bot ni cambiar su comportamiento
  por defecto:
    - WS_ENABLED=False por defecto — el bot sigue exactamente igual hasta
      que se active explícitamente en Railway.
    - Si el WS falla, se desconecta, o BingX cambia el formato — el
      scanner cae automáticamente a REST sin ningún error visible para
      el resto del sistema. ws_cache.get_latest() simplemente devuelve
      None si no hay datos frescos, y el caller ya sabe usar REST en
      ese caso (ver scanner.py _fetch_all).
    - Reconexión automática con backoff exponencial — nunca propaga una
      excepción hacia main.py, corre como tarea de fondo aislada.

ENDPOINT REAL DE BINGX (verificado contra la documentación oficial):
  wss://open-api-ws.bingx.com/market
  - Todas las respuestas vienen comprimidas en GZIP — hay que descomprimir
  - El servidor manda "Ping" cada ~5s — el cliente DEBE responder "Pong"
    o la conexión se cierra
  - Suscripción: {"id": "...", "dataType": "{symbol}@kline_{interval}"}
  - Los intervalos en WS usan sufijo "min" (1min, 5min, 15min), DISTINTO
    del REST que usa "1m", "3m", "15m" — fácil de confundir, por eso
    existe el INTERVAL_MAP explícito.

USO (opt-in, no afecta nada si no se activa):
  Railway → Variables → WS_ENABLED=true
  main.py arranca run_ws_client() como tarea de fondo si está activado.
  scanner.py consulta ws_cache.get_latest() antes de la llamada REST.
═══════════════════════════════════════════════════════════════════════════════
"""
import asyncio
import gzip
import json
import logging
import time
from collections import defaultdict, deque

import aiohttp

log = logging.getLogger("ws_market")

WS_URL = "wss://open-api-ws.bingx.com/market"
MAX_SYMBOLS_PER_CONN = 200  # margen conservador bajo el límite real de BingX

# REST usa "1m","3m","15m"... WS usa "1min","3min","15min"... — NO son iguales.
INTERVAL_MAP = {
    "1m": "1min", "3m": "3min", "5m": "5min", "15m": "15min",
    "30m": "30min", "1h": "1h", "4h": "4h", "1d": "1day",
}


class WSKlineCache:
    """
    Caché en memoria de las últimas velas recibidas por WebSocket.
    Thread-safe a nivel de evento único de asyncio (sin locks — todas las
    escrituras ocurren en la misma tarea del WS client, todas las lecturas
    son no bloqueantes desde el scanner).
    """

    def __init__(self):
        self._klines:      dict[tuple, deque] = defaultdict(lambda: deque(maxlen=250))
        self._last_update: dict[tuple, float] = {}
        self.connected = False

    def update(self, symbol: str, interval_rest: str, candle: list):
        key = (symbol, interval_rest)
        dq  = self._klines[key]
        # Si la vela tiene el mismo open_time que la última, es la MISMA
        # vela actualizándose en curso → reemplazar. Si no, es una vela
        # nueva cerrada → añadir.
        if dq and dq[-1][0] == candle[0]:
            dq[-1] = candle
        else:
            dq.append(candle)
        self._last_update[key] = time.time()

    def get_latest(self, symbol: str, interval_rest: str, max_age_s: float = 90.0):
        """
        Retorna la lista de velas en caché si está fresca (< max_age_s),
        o None si no hay datos o están obsoletos — el caller debe usar
        REST en ese caso. NUNCA lanza excepción.
        """
        key  = (symbol, interval_rest)
        last = self._last_update.get(key, 0)
        if time.time() - last > max_age_s:
            return None
        dq = self._klines.get(key)
        return list(dq) if dq and len(dq) >= 20 else None

    def stats(self) -> dict:
        now = time.time()
        fresh = sum(1 for t in self._last_update.values() if now - t <= 90.0)
        return {"connected": self.connected, "symbols_fresh": fresh,
                "symbols_total": len(self._last_update)}


# ── Singleton global — importado por scanner.py ───────────────────────────────
ws_cache = WSKlineCache()


def _decompress(data: bytes):
    """Descomprime GZIP; si falla, intenta texto plano (algunos frames no van comprimidos)."""
    try:
        return json.loads(gzip.decompress(data).decode("utf-8"))
    except Exception:
        try:
            return json.loads(data.decode("utf-8"))
        except Exception:
            return None


async def run_ws_client(get_symbols_fn, interval_rest: str = "3m"):
    """
    Mantiene una conexión WS persistente. Reconecta con backoff exponencial
    si cae. NUNCA propaga excepciones — diseñado para correr indefinidamente
    como tarea de fondo (asyncio.create_task) sin poder tumbar el bot.

    get_symbols_fn: función (sin argumentos) que retorna la lista actual de
    símbolos a suscribir — se llama en cada reconexión para tomar la lista
    más reciente del scanner (que cambia cada 10 iteraciones).
    """
    interval_ws = INTERVAL_MAP.get(interval_rest, "3min")
    backoff = 2

    while True:
        try:
            symbols = get_symbols_fn()
            if not symbols:
                await asyncio.sleep(5)
                continue

            timeout = aiohttp.ClientTimeout(total=None, sock_read=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.ws_connect(WS_URL) as ws:
                    log.info("[WS] Conectado — suscribiendo %d símbolos (%s)",
                             min(len(symbols), MAX_SYMBOLS_PER_CONN), interval_ws)
                    ws_cache.connected = True
                    backoff = 2  # reset tras conexión exitosa

                    for i, sym in enumerate(symbols[:MAX_SYMBOLS_PER_CONN]):
                        sub = {"id": f"k{i}", "dataType": f"{sym}@kline_{interval_ws}"}
                        await ws.send_str(json.dumps(sub))
                        if i % 20 == 19:
                            await asyncio.sleep(0.1)  # no saturar de golpe

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.BINARY:
                            payload = _decompress(msg.data)
                            if payload is None:
                                continue
                            if isinstance(payload, str) and payload == "Ping":
                                await ws.send_str("Pong")
                                continue
                            if not isinstance(payload, dict):
                                continue
                            data_type = payload.get("dataType", "")
                            if "@kline_" in data_type:
                                k = payload.get("data", {}).get("K", {})
                                if not k:
                                    continue
                                sym = data_type.split("@")[0]
                                try:
                                    candle = [
                                        int(k.get("t", 0)), float(k.get("o", 0)),
                                        float(k.get("h", 0)), float(k.get("l", 0)),
                                        float(k.get("c", 0)), float(k.get("v", 0)),
                                    ]
                                    ws_cache.update(sym, interval_rest, candle)
                                except (TypeError, ValueError):
                                    continue
                        elif msg.type == aiohttp.WSMsgType.TEXT:
                            if msg.data == "Ping":
                                await ws.send_str("Pong")
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR,
                                          aiohttp.WSMsgType.CLOSE):
                            log.warning("[WS] Conexión cerrada — reconectando")
                            break

        except asyncio.CancelledError:
            raise  # respetar shutdown limpio del bot
        except Exception as e:
            log.warning("[WS] Error: %s — reintentando en %ds", e, backoff)
        finally:
            ws_cache.connected = False

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)
