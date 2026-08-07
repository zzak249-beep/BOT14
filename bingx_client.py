"""
Cliente async para BingX Perpetual Swap V2 (USDT-M).

Firma: HMAC-SHA256 sobre urlencode(sorted(params)), hex digest,
header X-BX-APIKEY. Este patron de firma viene de bots ya
funcionando en produccion.

Los NOMBRES DE ENDPOINT de aqui abajo siguen la forma documentada
de swapV2 (https://bingx-api.github.io/docs/#/en-us/swapV2/). Si
BingX cambia algo, es el UNICO archivo que hay que tocar: todo lo
demas habla con esta clase, no con la API directamente.

Antes de MODE=LIVE: corre check_connectivity() (main.py ya lo hace
al arrancar) y revisa los logs. Si algun endpoint devuelve un
"code" distinto de 0, aqui se convierte en BingXError con el
codigo y mensaje originales de BingX -- eso es lo que hay que
buscar en su documentacion si algo no cuadra.
"""
import asyncio
import hashlib
import hmac
import logging
import time
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urlencode

import aiohttp

log = logging.getLogger("bingx")

RECV_WINDOW = 5000


class BingXError(Exception):
    def __init__(self, code: Any, msg: str, params: Optional[dict] = None):
        self.code = code
        self.msg = msg
        self.params = params
        super().__init__(f"BingX error {code}: {msg}")


@dataclass
class Candle:
    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: Optional[int] = None  # None si el endpoint no lo trajo -- se estima en el punto de uso


def parse_klines(raw: list) -> list:
    """
    Normaliza la respuesta de klines a una lista de Candle ordenada de
    mas antigua a mas reciente. Soporta formato dict y formato array,
    por si el endpoint devuelve una forma distinta a la esperada.
    """
    out = []
    for k in raw or []:
        try:
            if isinstance(k, dict):
                ot = k.get("time") or k.get("openTime") or k.get("t")
                o = k.get("open") if k.get("open") is not None else k.get("o")
                h = k.get("high") if k.get("high") is not None else k.get("h")
                l = k.get("low") if k.get("low") is not None else k.get("l")
                c = k.get("close") if k.get("close") is not None else k.get("c")
                v = k.get("volume") if k.get("volume") is not None else k.get("v")
                ct = k.get("closeTime") or k.get("close_time") or k.get("ct")
                out.append(Candle(int(ot), float(o), float(h), float(l), float(c), float(v or 0), int(ct) if ct is not None else None))
            elif isinstance(k, (list, tuple)) and len(k) >= 6:
                ct = int(k[6]) if len(k) >= 7 and k[6] is not None else None
                out.append(Candle(int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5]), ct))
        except (TypeError, ValueError):
            continue
    out.sort(key=lambda c: c.open_time)
    if raw and not out:
        log.warning("No se pudo parsear NINGUNA vela. Muestra cruda: %s", raw[:1])
    return out


def normalize_symbol(s: str, quote_asset: str = "USDT") -> str:
    """'btcusdt', 'BTC/USDT', 'BTC_USDT' -> 'BTC-USDT'. Evita huecos de
    normalizacion al comparar contra blacklist/whitelist."""
    s = s.strip().upper().replace("/", "-").replace("_", "-")
    if "-" not in s and s.endswith(quote_asset):
        s = s[: -len(quote_asset)] + "-" + quote_asset
    return s


class BingXClient:
    def __init__(self, api_key: str, api_secret: str, base_url: str, max_concurrent: int = 12):
        self.api_key = api_key or ""
        self.api_secret = api_secret or ""
        self.base_url = base_url.rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None
        self._sema = asyncio.Semaphore(max_concurrent)
        self.contract_meta: dict = {}

    async def __aenter__(self) -> "BingXClient":
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *exc) -> None:
        if self._session:
            await self._session.close()

    def _sign(self, query_string: str) -> str:
        return hmac.new(self.api_secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()

    @staticmethod
    def _param_str(v: Any) -> str:
        # Evita notacion cientifica en floats pequeños (1e-05) que BingX
        # podria no aceptar en qty/price de tokens de precio muy bajo.
        if isinstance(v, float):
            s = f"{v:.10f}".rstrip("0").rstrip(".")
            return s if s else "0"
        return str(v)

    def _build_query(self, params: Optional[dict], signed: bool) -> str:
        """Devuelve la query string EXACTA que se firma y se envia -- la
        misma variable, sin reserializar en otro punto del codigo."""
        clean = {k: self._param_str(v) for k, v in (params or {}).items() if v is not None}
        if signed:
            clean["timestamp"] = str(int(time.time() * 1000))
            clean["recvWindow"] = str(RECV_WINDOW)
        query_string = urlencode(sorted(clean.items()))
        if signed:
            signature = self._sign(query_string)
            query_string = f"{query_string}&signature={signature}" if query_string else f"signature={signature}"
        return query_string

    async def _request(self, method: str, path: str, params: Optional[dict] = None, signed: bool = False) -> Any:
        # La query string se construye UNA vez (_build_query) y esa misma
        # string es la que se firma y la que se envia -- ya no hay un
        # segundo punto de serializacion que pueda desincronizarse.
        query_string = self._build_query(params, signed)

        # FIX v1.1.2: confirmado contra la referencia oficial de BingX para
        # agentes de IA (github.com/BingX-API/api-ai-skills) -- en POST la
        # query firmada va en el BODY (application/x-www-form-urlencoded),
        # no en la URL. GET y DELETE si van en la URL, como ya estaba y
        # como se confirmo funcionando en get_balance(). Este era un bug
        # latente: no habia reventado porque set_leverage/place_order (los
        # unicos POST firmados) no se habian probado todavia en MODE=LIVE.
        url = f"{self.base_url}{path}"
        body = None
        headers = {"X-BX-APIKEY": self.api_key} if self.api_key else {}
        if method == "POST":
            body = query_string
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        elif query_string:
            url = f"{url}?{query_string}"

        async with self._sema:
            last_exc = None
            for attempt in range(3):
                try:
                    async with self._session.request(
                        method, url, data=body, headers=headers,
                        timeout=aiohttp.ClientTimeout(total=15),
                    ) as resp:
                        raw = await resp.text()
                        try:
                            data = await resp.json(content_type=None)
                        except Exception:
                            log.error("Respuesta no-JSON de %s (%d): %s", path, resp.status, raw[:200])
                            raise BingXError(resp.status, "respuesta no-JSON", params)

                        if resp.status == 429:
                            wait = 1.5 * (attempt + 1)
                            log.warning("429 rate limit en %s, esperando %.1fs", path, wait)
                            await asyncio.sleep(wait)
                            continue

                        if isinstance(data, dict) and data.get("code") not in (0, None):
                            raise BingXError(data.get("code"), data.get("msg"), params)

                        return data.get("data", data) if isinstance(data, dict) else data
                except aiohttp.ClientError as e:
                    last_exc = e
                    await asyncio.sleep(1.0 * (attempt + 1))
            log.error("Fallo de red persistente en %s: %s", path, last_exc)
            raise BingXError(-1, f"reintentos agotados: {last_exc}", params)

    # ── Mercado (publico) ──
    async def get_contracts(self) -> list:
        data = await self._request("GET", "/openApi/swap/v2/quote/contracts")
        contracts = data if isinstance(data, list) else data.get("contracts", []) if isinstance(data, dict) else []
        for c in contracts:
            sym = c.get("symbol")
            if sym:
                self.contract_meta[sym] = c
        return contracts

    async def get_klines(self, symbol: str, interval: str, limit: int = 300) -> list:
        data = await self._request(
            "GET", "/openApi/swap/v2/quote/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
        )
        raw = data if isinstance(data, list) else []
        return parse_klines(raw)

    async def get_open_interest(self, symbol: str) -> Optional[float]:
        """Interes abierto actual del contrato. None si el endpoint falla
        o el simbolo no lo reporta -- tratar como 'sin dato', no como 0.
        NOTA: endpoint no verificado en vivo (ver bingx_client en README)."""
        try:
            data = await self._request("GET", "/openApi/swap/v2/quote/openInterest", {"symbol": symbol})
            if isinstance(data, dict):
                oi = data.get("openInterest")
                return float(oi) if oi is not None else None
        except (BingXError, TypeError, ValueError):
            return None
        return None

    async def get_funding_rate(self, symbol: str) -> Optional[float]:
        """Funding rate actual/mas reciente como fraccion (0.0001 = 0.01%).
        None si el endpoint falla. NOTA: endpoint no verificado en vivo."""
        try:
            data = await self._request("GET", "/openApi/swap/v2/quote/premiumIndex", {"symbol": symbol})
            if isinstance(data, list) and data:
                data = data[0]
            if isinstance(data, dict):
                fr = data.get("lastFundingRate", data.get("fundingRate"))
                return float(fr) if fr is not None else None
        except (BingXError, TypeError, ValueError):
            return None
        return None

    # ── Cuenta (firmado) ──
    async def get_balance(self) -> dict:
        return await self._request("GET", "/openApi/swap/v2/user/balance", signed=True)

    async def get_all_account_balance(self) -> list:
        """Saldo por tipo de cuenta (spot/fund, futuros USDT-M, standard
        futures, etc.) en una sola llamada. Confirmado contra la
        referencia oficial de BingX para agentes de IA:
        github.com/BingX-API/api-ai-skills -- /openApi/account/v1/allAccountBalance.
        Pensado como diagnostico: si get_balance() (solo USDT-M Perp)
        da 0 pero aqui aparece saldo en otro accountType, el dinero
        esta ahi, no en el monedero de futuros."""
        data = await self._request("GET", "/openApi/account/v1/allAccountBalance", signed=True)
        return data if isinstance(data, list) else []

    async def get_positions(self, symbol: Optional[str] = None) -> list:
        data = await self._request("GET", "/openApi/swap/v2/user/positions", {"symbol": symbol}, signed=True)
        return data if isinstance(data, list) else []

    async def get_position_mode(self) -> Optional[str]:
        """'HEDGE' o 'ONEWAY'. None si el endpoint falla (usar config.POSITION_MODE como fallback)."""
        try:
            data = await self._request("GET", "/openApi/swap/v2/user/positionSide/dual", signed=True)
            dual = data.get("dualSidePosition") if isinstance(data, dict) else None
            if dual is None:
                return None
            return "HEDGE" if dual in (True, "true", 1, "1") else "ONEWAY"
        except BingXError as e:
            log.warning("No se pudo consultar el modo de posicion (%s). Se usara el de config.py.", e)
            return None

    async def set_leverage(self, symbol: str, side: str, leverage: int) -> dict:
        return await self._request(
            "POST", "/openApi/swap/v2/trade/leverage",
            {"symbol": symbol, "side": side, "leverage": leverage}, signed=True,
        )

    # ── Ordenes (firmado) ──
    async def place_order(
        self, symbol: str, side: str, position_side: str, order_type: str,
        quantity: Optional[float] = None, price: Optional[float] = None,
        stop_price: Optional[float] = None, reduce_only: Optional[bool] = None,
        working_type: str = "MARK_PRICE",
    ) -> dict:
        params = {"symbol": symbol, "side": side, "positionSide": position_side, "type": order_type}
        if quantity is not None:
            params["quantity"] = quantity
        if price is not None:
            params["price"] = price
        if stop_price is not None:
            params["stopPrice"] = stop_price
            params["workingType"] = working_type
        if reduce_only is not None:
            params["reduceOnly"] = "true" if reduce_only else "false"
        return await self._request("POST", "/openApi/swap/v2/trade/order", params, signed=True)

    async def cancel_order(self, symbol: str, order_id: Any) -> dict:
        return await self._request(
            "DELETE", "/openApi/swap/v2/trade/order", {"symbol": symbol, "orderId": order_id}, signed=True,
        )

    async def get_open_orders(self, symbol: Optional[str] = None) -> list:
        data = await self._request("GET", "/openApi/swap/v2/trade/openOrders", {"symbol": symbol}, signed=True)
        if isinstance(data, dict):
            return data.get("orders", [])
        return data or []

    async def cancel_all_open_orders(self, symbol: str) -> dict:
        return await self._request(
            "DELETE", "/openApi/swap/v2/trade/allOpenOrders", {"symbol": symbol}, signed=True,
        )

    # ── Precision de contrato ──
    def round_qty(self, symbol: str, qty: float) -> float:
        meta = self.contract_meta.get(symbol, {})
        prec = int(meta.get("quantityPrecision", 3))
        return round(qty, max(prec, 0))

    def round_price(self, symbol: str, price: float) -> float:
        meta = self.contract_meta.get(symbol, {})
        prec = int(meta.get("pricePrecision", 4))
        return round(price, max(prec, 0))
