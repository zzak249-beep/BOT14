"""
BingX Perpetual Futures — REST + WebSocket connector
Documentación: https://bingx-api.github.io/docs/
"""
import asyncio
import hashlib
import hmac
import json
import time
import logging
from typing import Optional
from urllib.parse import urlencode

import aiohttp
import pandas as pd

logger = logging.getLogger(__name__)

BASE_URL = "https://open-api.bingx.com"


class BingXClient:
    def __init__(self, api_key: str, secret: str, paper: bool = True):
        self.api_key = api_key
        self.secret  = secret
        self.paper   = paper
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"X-BX-APIKEY": self.api_key}
            )
        return self._session

    def _sign(self, params: dict) -> str:
        qs = urlencode(sorted(params.items()))
        return hmac.new(self.secret.encode(), qs.encode(), hashlib.sha256).hexdigest()

    async def _get(self, path: str, params: dict = None) -> dict:
        params = params or {}
        params["timestamp"] = int(time.time() * 1000)
        params["signature"] = self._sign(params)
        sess = await self._get_session()
        async with sess.get(BASE_URL + path, params=params) as r:
            data = await r.json()
            if data.get("code", 0) != 0:
                raise RuntimeError(f"BingX GET error {path}: {data}")
            return data

    async def _post(self, path: str, params: dict = None) -> dict:
        params = params or {}
        params["timestamp"] = int(time.time() * 1000)
        params["signature"] = self._sign(params)
        sess = await self._get_session()
        async with sess.post(BASE_URL + path, params=params) as r:
            data = await r.json()
            if data.get("code", 0) != 0:
                raise RuntimeError(f"BingX POST error {path}: {data}")
            return data

    # ── Symbols ──────────────────────────────────────────────
    async def get_all_symbols(self, min_volume_usdt: float = 5_000_000) -> list[str]:
        """
        Retorna todos los pares USDT de futuros perpetuos con volumen
        24h superior al mínimo indicado (default 5M USDT).
        Filtra automáticamente pares ilíquidos.
        """
        try:
            # Contratos disponibles
            data = await self._get("/openApi/swap/v2/quote/contracts")
            contracts = data.get("data", [])
            usdt_pairs = [
                c["symbol"] for c in contracts
                if c.get("symbol", "").endswith("-USDT")
                and c.get("status", 1) == 1   # activo
            ]

            if not usdt_pairs:
                return []

            # Tickers para filtrar por volumen 24h
            ticker_data = await self._get("/openApi/swap/v2/quote/ticker")
            tickers = {t["symbol"]: t for t in ticker_data.get("data", [])}

            filtered = []
            for sym in usdt_pairs:
                t = tickers.get(sym, {})
                vol = float(t.get("quoteVolume", 0))   # volumen en USDT
                if vol >= min_volume_usdt:
                    filtered.append(sym)

            filtered.sort()
            logger.info(f"Símbolos activos con vol ≥ {min_volume_usdt/1e6:.0f}M USDT: {len(filtered)}")
            return filtered

        except Exception as e:
            logger.error(f"Error obteniendo símbolos: {e}")
            return ["BTC-USDT", "ETH-USDT"]   # fallback seguro

    # ── Market data ──────────────────────────────────────────
    async def get_klines(self, symbol: str, interval: str, limit: int = 200) -> pd.DataFrame:
        """interval: '3m' | '15m' | '1h' | ..."""
        data = await self._get(
            "/openApi/swap/v2/quote/klines",
            {"symbol": symbol, "interval": interval, "limit": limit}
        )
        rows = data["data"]
        df = pd.DataFrame(rows, columns=["open_time", "open", "high", "low", "close", "volume"])
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
        df.set_index("open_time", inplace=True)
        return df

    async def get_ticker(self, symbol: str) -> dict:
        data = await self._get("/openApi/swap/v2/quote/ticker", {"symbol": symbol})
        return data["data"]

    async def get_balance(self) -> float:
        """Retorna USDT disponible en la cuenta de futuros"""
        data = await self._get("/openApi/swap/v2/user/balance")
        for asset in data["data"]["balance"]:
            if asset["asset"] == "USDT":
                return float(asset["availableMargin"])
        return 0.0

    async def get_positions(self, symbol: str) -> list:
        data = await self._get("/openApi/swap/v2/user/positions", {"symbol": symbol})
        return data.get("data", [])

    # ── Orders ───────────────────────────────────────────────
    async def place_order(
        self,
        symbol: str,
        side: str,
        position_side: str,
        qty: float,
        order_type: str = "MARKET",
        price: float = None,
        reduce_only: bool = False,
    ) -> dict:
        if self.paper:
            logger.info(f"[PAPER] {side} {position_side} {qty} {symbol}")
            return {"orderId": f"paper_{int(time.time())}", "paper": True}

        params = {
            "symbol":       symbol,
            "side":         side,
            "positionSide": position_side,
            "type":         order_type,
            "quantity":     qty,
        }
        if price and order_type == "LIMIT":
            params["price"] = price
        if reduce_only:
            params["reduceOnly"] = "true"

        return await self._post("/openApi/swap/v2/trade/order", params)

    async def set_sl_tp(
        self,
        symbol: str,
        position_side: str,
        sl_price: float,
        tp_price: float,
        qty: float,
    ) -> dict:
        if self.paper:
            logger.info(f"[PAPER] SL={sl_price} TP={tp_price}")
            return {"paper": True}

        sl_side = "SELL" if position_side == "LONG" else "BUY"
        await self._post("/openApi/swap/v2/trade/order", {
            "symbol":       symbol,
            "side":         sl_side,
            "positionSide": position_side,
            "type":         "STOP_MARKET",
            "stopPrice":    sl_price,
            "quantity":     qty,
            "reduceOnly":   "true",
        })
        await self._post("/openApi/swap/v2/trade/order", {
            "symbol":       symbol,
            "side":         sl_side,
            "positionSide": position_side,
            "type":         "TAKE_PROFIT_MARKET",
            "stopPrice":    tp_price,
            "quantity":     qty,
            "reduceOnly":   "true",
        })
        return {"ok": True}

    async def close_position(self, symbol: str, position_side: str, qty: float) -> dict:
        side = "SELL" if position_side == "LONG" else "BUY"
        return await self.place_order(symbol, side, position_side, qty, reduce_only=True)

    async def set_leverage(self, symbol: str, leverage: int) -> dict:
        if self.paper:
            return {"paper": True}
        return await self._post("/openApi/swap/v2/trade/leverage", {
            "symbol": symbol, "side": "LONG", "leverage": leverage
        })

    async def close(self):
        if self._session:
            await self._session.close()
