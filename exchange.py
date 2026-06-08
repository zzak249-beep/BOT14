"""
GUA-USDT Bot v2 — BingX Exchange Client
HMAC-SHA256 · One-Way mode · Sin positionSide.
"""

from __future__ import annotations
import asyncio, hashlib, hmac, json, logging, time
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import aiohttp
import config

log = logging.getLogger("exchange")


def _sign(params: Dict[str, Any], secret: str) -> str:
    query = urlencode(sorted(params.items()))
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()

def _ts() -> int:
    return int(time.time() * 1000)


class BingXClient:

    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None

    async def _sess(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"X-BX-APIKEY": config.BINGX_API_KEY,
                         "Content-Type": "application/json"}
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    # ── Requests ───────────────────────────────────────────────────────────────

    async def _get(self, path: str, params: Dict) -> Dict:
        params["timestamp"] = _ts()
        params["signature"] = _sign(params, config.BINGX_SECRET)
        s = await self._sess()
        async with s.get(config.BASE_URL + path, params=params) as r:
            data = json.loads(await r.text())
        if data.get("code") != 0:
            raise RuntimeError(f"GET {path}: {data}")
        return data

    async def _get_pub(self, path: str, params: Dict) -> Dict:
        s = await self._sess()
        async with s.get(config.BASE_URL + path, params=params) as r:
            data = json.loads(await r.text())
        if data.get("code") != 0:
            raise RuntimeError(f"GET_PUB {path}: {data}")
        return data

    async def _post(self, path: str, params: Dict) -> Dict:
        params["timestamp"] = _ts()
        sig = _sign(params, config.BINGX_SECRET)
        s   = await self._sess()
        async with s.post(config.BASE_URL + path + f"?signature={sig}",
                          json=params) as r:
            data = json.loads(await r.text())
        if data.get("code") != 0:
            raise RuntimeError(f"POST {path}: {data}")
        return data

    async def _delete(self, path: str, params: Dict) -> Dict:
        params["timestamp"] = _ts()
        params["signature"] = _sign(params, config.BINGX_SECRET)
        s = await self._sess()
        async with s.delete(config.BASE_URL + path, params=params) as r:
            data = json.loads(await r.text())
        if data.get("code") != 0:
            raise RuntimeError(f"DELETE {path}: {data}")
        return data

    # ── Klines ─────────────────────────────────────────────────────────────────

    async def get_klines(self, symbol: str, interval: str, limit: int = 150) -> List[Dict]:
        """
        Obtiene velas OHLCV.
        Usa v2 (arrays) con fallback a v3 (dicts).
        """
        data = await self._get_pub(
            "/openApi/swap/v2/quote/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
        )
        candles = []
        for r in data.get("data", []):
            try:
                if isinstance(r, (list, tuple)):
                    # Formato v2: [time, open, high, low, close, volume, ...]
                    candles.append({
                        "time":   int(r[0]),
                        "open":   float(r[1]),
                        "high":   float(r[2]),
                        "low":    float(r[3]),
                        "close":  float(r[4]),
                        "volume": float(r[5]),
                    })
                elif isinstance(r, dict):
                    # Formato v3: {"time":..., "open":..., ...}
                    candles.append({
                        "time":   int(r.get("time",   r.get("t", 0))),
                        "open":   float(r.get("open",   r.get("o", 0))),
                        "high":   float(r.get("high",   r.get("h", 0))),
                        "low":    float(r.get("low",    r.get("l", 0))),
                        "close":  float(r.get("close",  r.get("c", 0))),
                        "volume": float(r.get("volume", r.get("v", 0))),
                    })
            except (IndexError, KeyError, TypeError, ValueError) as e:
                log.warning("Error parseando vela %s: %s", r, e)
                continue
        return candles

    # ── Balance ────────────────────────────────────────────────────────────────

    async def get_balance(self) -> float:
        data = await self._get("/openApi/swap/v2/user/balance", {"currency": "USDT"})
        for asset in data.get("data", {}).get("balance", []):
            if asset.get("asset") == "USDT":
                return float(asset.get("availableMargin", 0))
        bal = data.get("data", {}).get("balance", {})
        if isinstance(bal, dict):
            return float(bal.get("availableMargin", 0))
        return 0.0

    # ── Posiciones ─────────────────────────────────────────────────────────────

    async def get_positions(self, symbol: str) -> List[Dict]:
        data = await self._get("/openApi/swap/v2/user/positions", {"symbol": symbol})
        return [p for p in data.get("data", []) if abs(float(p.get("positionAmt", 0))) > 0]

    # ── Leverage ───────────────────────────────────────────────────────────────

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        for side in ("LONG", "SHORT"):
            await self._post("/openApi/swap/v2/trade/leverage",
                             {"symbol": symbol, "side": side, "leverage": leverage})
        log.info("Leverage %sx en %s", leverage, symbol)

    # ── Precio ─────────────────────────────────────────────────────────────────

    async def get_price(self, symbol: str) -> float:
        data = await self._get_pub("/openApi/swap/v2/quote/price", {"symbol": symbol})
        return float(data.get("data", {}).get("price", 0))

    # ── Órdenes ────────────────────────────────────────────────────────────────

    async def place_market_order(self, symbol: str, side: str,
                                  quantity: float, reduce_only: bool = False) -> Dict:
        params: Dict[str, Any] = {
            "symbol":   symbol,
            "side":     side,
            "type":     "MARKET",
            "quantity": round(quantity, 4),
        }
        if reduce_only:
            params["reduceOnly"] = "true"
        return await self._post("/openApi/swap/v2/trade/order", params)

    async def cancel_all_orders(self, symbol: str) -> None:
        try:
            await self._delete("/openApi/swap/v2/trade/allOpenOrders", {"symbol": symbol})
        except Exception as e:
            log.warning("cancel_all_orders: %s", e)

    # ── Datos de mercado ───────────────────────────────────────────────────────

    async def get_funding_rate(self, symbol: str) -> float:
        try:
            data = await self._get_pub("/openApi/swap/v2/quote/fundingRate",
                                        {"symbol": symbol})
            return float(data.get("data", {}).get("fundingRate", 0))
        except Exception:
            return 0.0

    async def get_open_interest(self, symbol: str) -> float:
        try:
            data = await self._get_pub("/openApi/swap/v2/quote/openInterest",
                                        {"symbol": symbol})
            return float(data.get("data", {}).get("openInterest", 0))
        except Exception:
            return 0.0

    async def get_order_book_imbalance(self, symbol: str, depth: int = 20) -> float:
        """
        Imbalance del order book: (bid_vol - ask_vol) / (bid_vol + ask_vol)
        Positivo → presión compradora · Negativo → presión vendedora
        """
        try:
            data = await self._get_pub("/openApi/swap/v2/quote/depth",
                                        {"symbol": symbol, "limit": depth})
            bids = data.get("data", {}).get("bids", [])
            asks = data.get("data", {}).get("asks", [])
            bid_vol = sum(float(b[1]) for b in bids)
            ask_vol = sum(float(a[1]) for a in asks)
            total   = bid_vol + ask_vol
            return (bid_vol - ask_vol) / total if total > 0 else 0.0
        except Exception:
            return 0.0
