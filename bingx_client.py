"""
bingx_client.py

Minimal async client for BingX USDT-M Perpetual Futures (swap v2 API),
built directly on aiohttp (no ccxt dependency) - matches the auth pattern
already proven in production:

  * HMAC-SHA256 signature over the query string built in INSERTION order
    (NOT alphabetically sorted - BingX's signature check fails on sorted
    params even though most public examples sort them).
  * API key sent via the `X-BX-APIKEY` header.
  * Every order call sends an explicit `quantity`; exits use
    `reduceOnly=true` with an explicit quantity rather than
    `closePosition=true` + `quantity=0`.

Endpoints used (verify against https://bingx-api.github.io/docs/#/swapV2/introduce
before going live - BingX does revise paths/params over time):
  GET  /openApi/swap/v2/quote/klines
  GET  /openApi/swap/v3/user/balance
  GET  /openApi/swap/v2/user/positions
  POST /openApi/swap/v2/trade/marginType
  POST /openApi/swap/v2/trade/leverage
  POST /openApi/swap/v2/trade/order
  DELETE /openApi/swap/v2/trade/order
"""

import hashlib
import hmac
import json
import logging
import time

import aiohttp

logger = logging.getLogger("bingx_client")


class BingXAPIError(Exception):
    def __init__(self, code, msg, raw=None):
        self.code = code
        self.msg = msg
        self.raw = raw
        super().__init__(f"BingX API error {code}: {msg}")


def parse_klines(raw):
    """Normalizes a BingX klines response into an ascending-by-time list of
    dicts: {open_time, open, high, low, close, volume}.

    Defensive on purpose: BingX has historically returned newest-first, and
    candle shape has varied between a list-of-dicts and list-of-lists across
    endpoints/SDKs, so both are handled here rather than assumed."""
    parsed = []
    for item in raw:
        if isinstance(item, dict):
            t = item.get("time", item.get("openTime"))
            parsed.append(
                {
                    "open_time": int(t),
                    "open": float(item["open"]),
                    "high": float(item["high"]),
                    "low": float(item["low"]),
                    "close": float(item["close"]),
                    "volume": float(item.get("volume", 0) or 0),
                }
            )
        elif isinstance(item, (list, tuple)) and len(item) >= 5:
            parsed.append(
                {
                    "open_time": int(item[0]),
                    "open": float(item[1]),
                    "high": float(item[2]),
                    "low": float(item[3]),
                    "close": float(item[4]),
                    "volume": float(item[5]) if len(item) > 5 else 0.0,
                }
            )
    parsed.sort(key=lambda c: c["open_time"])
    return parsed


class BingXClient:
    def __init__(self, api_key, api_secret, base_url="https://open-api.bingx.com", recv_window_ms=5000):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")
        self.recv_window_ms = recv_window_ms
        self._session = None

    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    def _sign(self, params: dict) -> str:
        # CRITICAL: insertion order, never sorted() - see module docstring.
        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        return hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    async def _request(self, method: str, path: str, params: dict = None, signed: bool = True):
        params = dict(params or {})
        if signed:
            params["timestamp"] = str(int(time.time() * 1000))
            params["recvWindow"] = str(self.recv_window_ms)
            params["signature"] = self._sign(params)

        url = f"{self.base_url}{path}"
        headers = {"X-BX-APIKEY": self.api_key} if self.api_key else {}
        session = await self._ensure_session()

        try:
            async with session.request(
                method, url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                text = await resp.text()
                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    raise BingXAPIError(-1, f"Non-JSON response (HTTP {resp.status}) from {path}: {text[:300]}")
        except aiohttp.ClientError as e:
            raise BingXAPIError(-2, f"Network error calling {path}: {e}")

        code = data.get("code")
        if code not in (0, None):
            raise BingXAPIError(code, data.get("msg", "unknown error"), raw=data)
        return data.get("data", data)

    # ---------------------------------------------------------------- market
    async def get_klines(self, symbol: str, interval: str, limit: int = 500):
        data = await self._request(
            "GET",
            "/openApi/swap/v2/quote/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
            signed=False,
        )
        # some deployments wrap candles under data["list"] or return the list directly
        if isinstance(data, dict):
            data = data.get("list") or data.get("klines") or []
        return data

    # --------------------------------------------------------------- account
    async def get_balance(self):
        return await self._request("GET", "/openApi/swap/v3/user/balance", {})

    async def get_positions(self, symbol: str = None):
        params = {"symbol": symbol} if symbol else {}
        data = await self._request("GET", "/openApi/swap/v2/user/positions", params)
        return data if isinstance(data, list) else []

    async def set_margin_mode(self, symbol: str, margin_type: str = "ISOLATED"):
        return await self._request(
            "POST", "/openApi/swap/v2/trade/marginType", {"symbol": symbol, "marginType": margin_type}
        )

    async def set_leverage(self, symbol: str, position_side: str, leverage: int):
        # `side` here means POSITION side (LONG/SHORT/BOTH), not order side.
        return await self._request(
            "POST", "/openApi/swap/v2/trade/leverage", {"symbol": symbol, "side": position_side, "leverage": leverage}
        )

    # --------------------------------------------------------------- trading
    async def place_market_order(self, symbol: str, side: str, position_side: str, quantity: float):
        return await self._request(
            "POST",
            "/openApi/swap/v2/trade/order",
            {
                "symbol": symbol,
                "side": side,  # BUY / SELL
                "positionSide": position_side,  # LONG / SHORT (hedge mode)
                "type": "MARKET",
                "quantity": quantity,
            },
        )

    async def place_stop_market_order(self, symbol: str, side: str, position_side: str, quantity: float, stop_price: float):
        return await self._request(
            "POST",
            "/openApi/swap/v2/trade/order",
            {
                "symbol": symbol,
                "side": side,
                "positionSide": position_side,
                "type": "STOP_MARKET",
                "quantity": quantity,
                "stopPrice": stop_price,
                "reduceOnly": "true",
            },
        )

    async def close_position_market(self, symbol: str, position_side: str, quantity: float):
        """Closes a hedge-mode position with an explicit reduceOnly market
        order (never closePosition=true + quantity=0)."""
        side = "SELL" if position_side == "LONG" else "BUY"
        return await self._request(
            "POST",
            "/openApi/swap/v2/trade/order",
            {
                "symbol": symbol,
                "side": side,
                "positionSide": position_side,
                "type": "MARKET",
                "quantity": quantity,
                "reduceOnly": "true",
            },
        )

    async def cancel_order(self, symbol: str, order_id):
        return await self._request(
            "DELETE", "/openApi/swap/v2/trade/order", {"symbol": symbol, "orderId": order_id}
        )
