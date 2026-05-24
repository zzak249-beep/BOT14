"""
Cliente BingX — Perpetual Futures (USDT-M)
Docs: https://bingx-api.github.io/docs/swapV2/
"""
import asyncio
import hashlib
import hmac
import time
import logging
from urllib.parse import urlencode

import aiohttp

log = logging.getLogger("BingX")

BASE = "https://open-api.bingx.com"


class BingXClient:
    def __init__(self, api_key: str, secret: str):
        self.api_key = api_key
        self.secret  = secret
        self._session: aiohttp.ClientSession | None = None

    # ── HTTP session (lazy) ──────────────────────────────────

    async def _sess(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"X-BX-APIKEY": self.api_key},
                timeout=aiohttp.ClientTimeout(total=10),
            )
        return self._session

    def _sign(self, params: dict) -> str:
        query   = urlencode(sorted(params.items()))
        raw     = query.encode()
        sig     = hmac.new(self.secret.encode(), raw, hashlib.sha256).hexdigest()
        return sig

    async def _get(self, path: str, params: dict = None, signed: bool = False):
        params = params or {}
        if signed:
            params["timestamp"] = int(time.time() * 1000)
            params["signature"] = self._sign(params)
        sess = await self._sess()
        async with sess.get(BASE + path, params=params) as r:
            data = await r.json()
        if data.get("code") != 0:
            raise RuntimeError(f"BingX GET {path} error: {data}")
        return data.get("data", data)

    async def _post(self, path: str, params: dict = None):
        params = params or {}
        params["timestamp"] = int(time.time() * 1000)
        params["signature"] = self._sign(params)
        sess = await self._sess()
        async with sess.post(BASE + path, params=params) as r:
            data = await r.json()
        if data.get("code") != 0:
            raise RuntimeError(f"BingX POST {path} error: {data}")
        return data.get("data", data)

    # ── Datos de mercado ─────────────────────────────────────

    async def get_klines(self, symbol: str, interval: str, limit: int = 200) -> list:
        """
        Retorna lista de [timestamp, open, high, low, close, volume].
        interval: "1m","3m","5m","15m","1h","4h"
        """
        data = await self._get("/openApi/swap/v2/quote/klines", {
            "symbol"  : symbol,
            "interval": interval,
            "limit"   : limit,
        })
        # BingX devuelve lista de dicts
        result = []
        for k in data:
            result.append([
                int(k["time"]),
                float(k["open"]),
                float(k["high"]),
                float(k["low"]),
                float(k["close"]),
                float(k["volume"]),
            ])
        return sorted(result, key=lambda x: x[0])

    async def get_ticker(self, symbol: str) -> dict:
        data = await self._get("/openApi/swap/v2/quote/ticker", {"symbol": symbol})
        return {
            "last"   : float(data["lastPrice"]),
            "bid"    : float(data["bidPrice"]),
            "ask"    : float(data["askPrice"]),
            "volume" : float(data["volume"]),
        }

    # ── Cuenta ───────────────────────────────────────────────

    async def get_balance(self) -> float:
        """Retorna balance disponible USDT."""
        data = await self._get("/openApi/swap/v2/user/balance", signed=True)
        for asset in data.get("balance", []):
            if asset.get("asset") == "USDT":
                return float(asset.get("availableMargin", 0))
        return 0.0

    async def get_positions(self, symbol: str = "") -> list:
        params = {}
        if symbol:
            params["symbol"] = symbol
        data = await self._get("/openApi/swap/v2/user/positions", params, signed=True)
        return data if isinstance(data, list) else []

    # ── Trading ──────────────────────────────────────────────

    async def set_leverage(self, symbol: str, leverage: int, side: str = "LONG"):
        """Configura apalancamiento antes de operar."""
        try:
            await self._post("/openApi/swap/v2/trade/leverage", {
                "symbol"    : symbol,
                "leverage"  : leverage,
                "side"      : side,
            })
        except Exception as e:
            log.warning(f"set_leverage {symbol}: {e}")

    async def place_order(self, symbol: str, side: str, size: float,
                          leverage: int, sl_price: float,
                          tp_price: float | None = None) -> dict | None:
        """
        side: "LONG" | "SHORT"
        Abre posición market con SL integrado.
        Retorna dict con orderId o None si falla.
        """
        # Configurar leverage
        await self.set_leverage(symbol, leverage, side)
        await asyncio.sleep(0.2)

        pos_side = side   # LONG / SHORT (hedge mode)
        order_side = "BUY" if side == "LONG" else "SELL"

        params = {
            "symbol"          : symbol,
            "side"            : order_side,
            "positionSide"    : pos_side,
            "type"            : "MARKET",
            "quantity"        : f"{size:.4f}",
            "stopLossPrice"   : f"{sl_price:.4f}",
        }
        if tp_price:
            params["takeProfitPrice"] = f"{tp_price:.4f}"

        try:
            data = await self._post("/openApi/swap/v2/trade/order", params)
            log.info(f"Order placed: {symbol} {side} {size} → {data}")
            return data
        except Exception as e:
            log.error(f"place_order failed: {e}")
            return None

    async def close_position(self, symbol: str, side: str) -> dict | None:
        """Cierra posición existente con orden market."""
        pos_side  = side
        order_side = "SELL" if side == "LONG" else "BUY"

        positions = await self.get_positions(symbol)
        size = 0.0
        for p in positions:
            if p.get("positionSide") == side and float(p.get("positionAmt", 0)) != 0:
                size = abs(float(p["positionAmt"]))
                break

        if size == 0:
            log.warning(f"close_position: no position found for {symbol} {side}")
            return None

        params = {
            "symbol"       : symbol,
            "side"         : order_side,
            "positionSide" : pos_side,
            "type"         : "MARKET",
            "quantity"     : f"{size:.4f}",
            "reduceOnly"   : "true",
        }
        try:
            data = await self._post("/openApi/swap/v2/trade/order", params)
            log.info(f"Position closed: {symbol} {side} size={size}")
            return data
        except Exception as e:
            log.error(f"close_position failed: {e}")
            return None

    async def cancel_all_orders(self, symbol: str):
        try:
            await self._post("/openApi/swap/v2/trade/allOpenOrders", {"symbol": symbol})
        except Exception as e:
            log.warning(f"cancel_all_orders {symbol}: {e}")

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
