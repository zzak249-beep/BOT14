"""
Cliente BingX Perpetual Futures (Swap V2) — asíncrono
========================================================
Implementa lo mínimo necesario para este bot:
  - listado de símbolos + volumen 24h
  - klines (velas)
  - balance de cuenta
  - set leverage
  - abrir/cerrar posición con SL/TP

Firma HMAC-SHA256 según especificación estándar de BingX Swap V2 API.
Requiere BINGX_API_KEY / BINGX_API_SECRET como variables de entorno.

RESUELTO (confirmado en producción, no solo en teoría): la advertencia que
tenía esta nota — que aiohttp podía serializar params= en un orden distinto
al usado para firmar — SÍ era el problema real: el primer POST de apertura
de posición en real falló con 100001 "Signature verification failed". La
causa exacta: `_request` armaba el dict de params y dejaba que aiohttp lo
serializara en orden de INSERCIÓN, mientras `_sign` calculaba el hash sobre
el mismo dict en orden ALFABÉTICO — nunca iban a coincidir salvo por
casualidad. Fix: `_request` ahora construye la URL completa a mano con
`urlencode(sorted(params.items()))`, la MISMA función usada para firmar,
así la query string que se firma y la que se manda son byte por byte
idénticas. Verificado parseando la URL resultante con yarl (el mismo parser
que usa aiohttp por debajo) para confirmar que no la reordena de nuevo.
"""
import asyncio
import hashlib
import hmac
import logging
import random
import re
import time
from urllib.parse import urlencode

import aiohttp

log = logging.getLogger("exchange_client")

RATE_LIMIT_CODE = 100410
RATE_LIMIT_SAFETY_CEILING_S = 600.0  # techo de SEGURIDAD solo por si el mensaje viniera
                                      # corrupto/con un timestamp absurdo — NO es el caso normal.
                                      # A diferencia de antes, ya NO reintentamos dentro del mismo
                                      # request: cada intento adicional durante el "disabled period"
                                      # parece hacer que BingX EXTIENDA el bloqueo (el log mostraba
                                      # el tiempo de espera calculado sin bajar hacia 0, ciclo tras
                                      # ciclo) — reintentar activamente perpetuaba el problema.
DEFAULT_MIN_REQUEST_INTERVAL_S = 0.2  # ~5 req/s — más conservador que el intento anterior (~8 req/s);
                                        # si el rate limit se extiende con cada request adicional
                                        # recibida durante el bloqueo, más vale pecar de lento


def _parse_unblock_wait_s(msg):
    """
    Extrae el epoch en ms de mensajes tipo:
    "code:100410:The endpoint trigger frequency limit rule is currently in
    the disabled period and will be unblocked after 1783232851826"
    Devuelve segundos a esperar (>=0), o un default conservador si no
    puede parsear el mensaje (BingX podría cambiar el formato del texto).
    """
    match = re.search(r"unblocked after (\d+)", msg or "")
    if not match:
        return 3.0
    unblock_ms = int(match.group(1))
    wait_s = (unblock_ms / 1000) - time.time()
    return max(0.0, wait_s)


class BingXClient:
    def __init__(self, api_key, api_secret, base_url, dry_run=True,
                 min_request_interval=DEFAULT_MIN_REQUEST_INTERVAL_S):
        # .strip() por si alguien instancia el cliente directamente sin pasar
        # por config.py (que ya limpia) — un espacio/salto de línea invisible
        # en la key o el secret produce el mismo síntoma que una firma mal
        # calculada: "Signature verification failed".
        self.api_key = (api_key or "").strip()
        self.api_secret = (api_secret or "").strip()
        self.base_url = base_url.rstrip("/")
        self.dry_run = dry_run
        self._session = None
        # Estado COMPARTIDO entre todas las corutinas que usan esta misma
        # instancia (todo scan_universe corre sobre un único BingXClient).
        # Antes cada corutina calculaba su propia espera de forma aislada al
        # recibir un 100410 -> con SCAN_CONCURRENCY en paralelo, todas volvían
        # a pegarle casi al mismo tiempo y se re-bloqueaban en cadena. Ahora
        # una sola marca de tiempo gobierna a todas.
        self._rate_limit_until = 0.0     # epoch seconds
        self._last_request_at = 0.0
        self._pacing_lock = asyncio.Lock()
        self._min_request_interval = min_request_interval

    async def _wait_for_slot(self):
        """Espera compartida antes de CUALQUIER request: respeta el cooldown
        activo (si BingX ya nos rate-limitó) y un espaciado mínimo entre
        requests consecutivas, para no volver a disparar una ráfaga."""
        async with self._pacing_lock:
            now = time.time()
            wait_cooldown = max(0.0, self._rate_limit_until - now)
            if wait_cooldown > 0:
                wait_cooldown += random.uniform(0, 1.0)  # jitter: no todas despiertan juntas
            wait_pacing = max(0.0, self._last_request_at + self._min_request_interval - now)
            wait_s = max(wait_cooldown, wait_pacing)
            if wait_s > 0:
                await asyncio.sleep(wait_s)
            self._last_request_at = time.time()

    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *exc):
        if self._session:
            await self._session.close()

    def _sign(self, params: dict) -> str:
        qs = urlencode(sorted(params.items()))
        return hmac.new(
            self.api_secret.encode("utf-8"), qs.encode("utf-8"), hashlib.sha256
        ).hexdigest()

    async def _request(self, method, path, params=None, signed=False):
        await self._wait_for_slot()
        params = dict(params or {})
        if signed:
            params["timestamp"] = int(time.time() * 1000)
            params["recvWindow"] = 10000  # igual que el diagnóstico que confirmó funcionar
            params["signature"] = self._sign(params)
        headers = {"X-BX-APIKEY": self.api_key}

        # CRÍTICO: la query string real tiene que ser BYTE POR BYTE la misma
        # que se usó para calcular la firma (mismo orden alfabético). Si se
        # deja que aiohttp serialice un dict vía params=, lo hace en orden de
        # INSERCIÓN, no alfabético — casi nunca coinciden, y BingX rechaza la
        # firma con 100001 "Signature verification failed" (confirmado en
        # producción: el primer POST real de apertura de posición falló
        # exactamente por esto). Se construye la URL completa a mano en vez
        # de confiar en el parámetro params= de aiohttp.
        query_string = urlencode(sorted(params.items())) if params else ""
        url = f"{self.base_url}{path}"
        if query_string:
            url = f"{url}?{query_string}"

        try:
            async with self._session.request(method, url, headers=headers, timeout=15) as resp:
                data = await resp.json(content_type=None)
                code = data.get("code")
                if code == RATE_LIMIT_CODE:
                    wait_s = min(_parse_unblock_wait_s(data.get("msg", "")), RATE_LIMIT_SAFETY_CEILING_S)
                    # Cooldown COMPARTIDO, sin reintento: cualquier otra corutina (para
                    # este u otro endpoint) va a esperar este "hasta cuándo" en su
                    # próximo _wait_for_slot ANTES de disparar su request. No reintentamos
                    # aquí mismo porque cada intento adicional durante el "disabled period"
                    # parece extender el bloqueo en vez de solo rechazarlo (ver nota arriba).
                    self._rate_limit_until = max(self._rate_limit_until, time.time() + wait_s)
                    log.warning(
                        "Rate limit BingX (100410) en [%s %s] — cooldown compartido ~%.1fs, "
                        "se abandona este request (sin reintentar)",
                        method, path, wait_s,
                    )
                elif code not in (0, None):
                    log.warning("BingX API error [%s %s]: %s", method, path, data)
                return data
        except Exception as e:
            log.error("Error en request BingX [%s %s]: %s", method, path, e)
            return {"code": -1, "msg": str(e)}

    # ── Datos públicos ────────────────────────────────────────────────
    async def get_all_symbols_with_volume(self):
        """Devuelve [{symbol, volume_24h_usdt}, ...] para todos los perpetuos."""
        data = await self._request("GET", "/openApi/swap/v2/quote/ticker", signed=False)
        items = data.get("data", []) if isinstance(data, dict) else []
        out = []
        for it in items:
            try:
                out.append({
                    "symbol": it["symbol"],
                    "volume_24h_usdt": float(it.get("quoteVolume", 0)),
                })
            except (KeyError, ValueError, TypeError):
                continue
        return out

    async def get_klines(self, symbol, interval, limit=200):
        """interval: '3m','5m','15m','30m','1h','4h','1d' (formato BingX)."""
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        data = await self._request("GET", "/openApi/swap/v3/quote/klines", params, signed=False)
        raw = data.get("data", []) if isinstance(data, dict) else []
        candles = []
        for k in raw:
            try:
                candles.append({
                    "open": float(k["open"]), "high": float(k["high"]),
                    "low": float(k["low"]), "close": float(k["close"]),
                    "volume": float(k["volume"]), "time": int(k["time"]),
                })
            except (KeyError, ValueError, TypeError):
                continue
        candles.sort(key=lambda c: c["time"])
        return candles

    # ── Cuenta / trading ──────────────────────────────────────────────
    async def get_balance_usdt(self):
        data = await self._request("GET", "/openApi/swap/v2/user/balance", signed=True)
        try:
            return float(data["data"]["balance"]["balance"])
        except (KeyError, TypeError, ValueError):
            log.warning("No se pudo leer balance: %s", data)
            return 0.0

    async def set_leverage(self, symbol, leverage, side="LONG"):
        if self.dry_run:
            log.info("[DRY_RUN] set_leverage %s x%s (%s)", symbol, leverage, side)
            return True
        params = {"symbol": symbol, "side": side, "leverage": leverage}
        data = await self._request("POST", "/openApi/swap/v2/trade/leverage", params, signed=True)
        ok = data.get("code") == 0
        if not ok:
            log.error("set_leverage falló para %s: %s", symbol, data)
        return ok

    async def open_position(self, symbol, side, quantity, sl_price=None, tp_price=None):
        """side: 'LONG' o 'SHORT'."""
        if self.dry_run:
            log.info(
                "[DRY_RUN] open_position %s %s qty=%s SL=%s TP=%s",
                symbol, side, quantity, sl_price, tp_price,
            )
            return {"code": 0, "dry_run": True}

        order_side = "BUY" if side == "LONG" else "SELL"
        params = {
            "symbol": symbol, "side": order_side, "positionSide": side,
            "type": "MARKET", "quantity": quantity,
        }
        data = await self._request("POST", "/openApi/swap/v2/trade/order", params, signed=True)
        if data.get("code") != 0:
            log.error("Error abriendo posición %s %s: %s", symbol, side, data)
            return data

        if sl_price:
            await self._place_stop(symbol, side, "STOP_MARKET", sl_price, quantity)
        if tp_price:
            await self._place_stop(symbol, side, "TAKE_PROFIT_MARKET", tp_price, quantity)
        return data

    async def _place_stop(self, symbol, position_side, order_type, stop_price, quantity):
        close_side = "SELL" if position_side == "LONG" else "BUY"
        params = {
            "symbol": symbol, "side": close_side, "positionSide": position_side,
            "type": order_type, "stopPrice": stop_price, "quantity": quantity,
            "workingType": "MARK_PRICE",
        }
        data = await self._request("POST", "/openApi/swap/v2/trade/order", params, signed=True)
        if data.get("code") != 0:
            log.error("Error colocando %s para %s: %s", order_type, symbol, data)
        return data

    async def get_recent_trades(self, symbol, limit=1000):
        """
        Trades públicos recientes (se agregan client-side en order_flow.py).
        Devuelve lista de {price, qty, time, is_buyer_maker}.

        NOTA: este endpoint devuelve los N trades MÁS RECIENTES, no permite
        rango de tiempo arbitrario — por eso el filtro de order flow solo es
        útil para validar el sweep/breaker MÁS RECIENTE (señal en vivo), no
        para backtesting histórico. Verificar el nombre exacto del endpoint
        contra la documentación vigente de BingX antes de operar en real.
        """
        params = {"symbol": symbol, "limit": limit}
        data = await self._request("GET", "/openApi/swap/v2/quote/trades", params, signed=False)
        raw = data.get("data", []) if isinstance(data, dict) else []
        trades = []
        for t in raw:
            try:
                trades.append({
                    "price": float(t["price"]),
                    "qty": float(t.get("qty", t.get("volume", 0))),
                    "time": int(t["time"]),
                    "is_buyer_maker": bool(t.get("buyerMaker", t.get("isBuyerMaker", False))),
                })
            except (KeyError, ValueError, TypeError):
                continue
        trades.sort(key=lambda x: x["time"])
        return trades

    async def get_order_book(self, symbol, limit=20):
        """
        Libro de órdenes público (bids/asks) para Order Book Imbalance
        (order_book_imbalance.py). Devuelve {"bids": [[price, qty], ...],
        "asks": [[price, qty], ...]} o {} si falla.

        NOTA: el path exacto de este endpoint NO se confirmó contra tráfico
        real de BingX — se infirió de wrappers de terceros (ej. clientes PHP/
        C# no oficiales) que exponen un método `getDepth(symbol, limit)`,
        pero no de la documentación oficial verificada en vivo. Mismo criterio
        que get_recent_trades: revisar contra la documentación vigente antes
        de operar en real, y no sorprenderse si el nombre de campo real
        difiere (bids/asks vs a/b, strings vs floats, etc. — ya se maneja
        defensivamente abajo, pero solo hasta donde se pudo anticipar).
        """
        params = {"symbol": symbol, "limit": limit}
        data = await self._request("GET", "/openApi/swap/v2/quote/depth", params, signed=False)
        raw = data.get("data", {}) if isinstance(data, dict) else {}
        try:
            bids = [[float(p), float(q)] for p, q in raw.get("bids", [])]
            asks = [[float(p), float(q)] for p, q in raw.get("asks", [])]
            return {"bids": bids, "asks": asks}
        except (KeyError, ValueError, TypeError):
            log.warning("No se pudo parsear order book de %s: %s", symbol, raw)
            return {}

    async def get_funding_rate(self, symbol):
        """Funding rate actual del símbolo. Devuelve float (ej. 0.0001 = 0.01%)."""
        params = {"symbol": symbol}
        data = await self._request("GET", "/openApi/swap/v2/quote/premiumIndex", params, signed=False)
        try:
            return float(data["data"]["lastFundingRate"])
        except (KeyError, TypeError, ValueError):
            return None

    async def get_open_interest(self, symbol):
        """Open Interest actual en unidades del contrato. Devuelve float o None."""
        params = {"symbol": symbol}
        data = await self._request("GET", "/openApi/swap/v2/quote/openInterest", params, signed=False)
        try:
            return float(data["data"]["openInterest"])
        except (KeyError, TypeError, ValueError):
            return None

    async def get_income_history(self, symbol, limit=20):
        """Historial de PnL realizado (para el position_monitor)."""
        params = {"symbol": symbol, "limit": limit, "incomeType": "REALIZED_PNL"}
        data = await self._request("GET", "/openApi/swap/v2/user/income", params, signed=True)
        raw = data.get("data", []) if isinstance(data, dict) else []
        out = []
        for it in raw:
            try:
                out.append({"symbol": it["symbol"], "income": float(it["income"]), "time": int(it["time"])})
            except (KeyError, ValueError, TypeError):
                continue
        return out

    async def get_open_positions(self):
        data = await self._request("GET", "/openApi/swap/v2/user/positions", signed=True)
        items = data.get("data", []) if isinstance(data, dict) else []
        return [p for p in items if float(p.get("positionAmt", 0)) != 0]
