"""
QF×JP Bot v6.0 — bingx_client.py
Cliente BingX API v2 con:
  • Balance cacheado (evita error 100410)
  • Retry exponencial en errores de red
  • Maker limit → timeout → market fallback
  • Endpoints correctos open-api.bingx.com
  • Firma HMAC-SHA256
"""
import asyncio
import hashlib
import hmac
import logging
import time
from typing import Any, Optional
from urllib.parse import urlencode

import aiohttp

log = logging.getLogger("BINGX")
BASE = "https://open-api.bingx.com"


class BingXClient:
    def __init__(self, api_key: str, secret: str):
        self._key    = api_key
        self._secret = secret.encode()
        self._session: Optional[aiohttp.ClientSession] = None

        # ── Balance cache ────────────────────────────────
        self._bal_value: float = 0.0
        self._bal_ts:    float = 0.0
        self._bal_ttl:   int   = 60          # segundos

    # ── Sesión HTTP ──────────────────────────────────────
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=15)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    # ── Firma ────────────────────────────────────────────
    def _sign(self, params: dict) -> str:
        qs = urlencode(sorted(params.items()))
        return hmac.new(self._secret, qs.encode(), hashlib.sha256).hexdigest()

    def _ts(self) -> int:
        return int(time.time() * 1000)

    # ── Request con retry ────────────────────────────────
    async def _request(
        self,
        method: str,
        path: str,
        params: dict = None,
        body: dict = None,
        signed: bool = True,
        retry: int = 3,
    ) -> Optional[dict]:
        params = dict(params or {})
        if signed:
            params["timestamp"] = self._ts()
            params["signature"] = self._sign(params)

        headers = {"X-BX-APIKEY": self._key, "Content-Type": "application/json"}
        url = BASE + path
        session = await self._get_session()

        for attempt in range(retry):
            try:
                if method == "GET":
                    async with session.get(url, params=params, headers=headers) as r:
                        data = await r.json()
                else:
                    async with session.post(url, params=params, json=body or {}, headers=headers) as r:
                        data = await r.json()

                code = data.get("code", 0)

                # ── Rate limit / disabled period ─────────
                if code == 100410:
                    wait = min(10 * (attempt + 1), 30)
                    log.warning(f"Rate limit 100410 en {path} — espera {wait}s")
                    await asyncio.sleep(wait)
                    continue

                if code not in (0, 200):
                    log.error(f"API error {code} en {path}: {data.get('msg')}")
                    if attempt < retry - 1:
                        await asyncio.sleep(2 ** attempt)
                    continue

                return data.get("data") or data

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                log.error(f"Network error {path} intento {attempt+1}: {e}")
                if attempt < retry - 1:
                    await asyncio.sleep(2 ** attempt)

        return None

    # ── Balance (cacheado) ────────────────────────────────
    async def get_balance(self, force: bool = False) -> float:
        now = time.time()
        if not force and (now - self._bal_ts) < self._bal_ttl:
            return self._bal_value

        data = await self._request(
            "GET", "/openApi/swap/v2/user/balance",
            params={"currency": "USDT"}
        )
        if data:
            try:
                bal = float(data["balance"]["availableMargin"])
                self._bal_value = bal
                self._bal_ts    = now
                return bal
            except (KeyError, TypeError, ValueError) as e:
                log.error(f"Balance parse error: {e} | data={data}")

        return self._bal_value   # devuelve el último valor cacheado

    def invalidate_balance_cache(self):
        self._bal_ts = 0.0

    # ── Klines ────────────────────────────────────────────
    async def get_klines(self, symbol: str, interval: str, limit: int = 250) -> list:
        data = await self._request(
            "GET", "/openApi/swap/v3/quote/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
            signed=False,
        )
        if not data:
            return []
        # data = list de [ts, o, h, l, c, v, ...]
        rows = []
        for k in data:
            try:
                rows.append({
                    "ts": int(k[0]),
                    "o":  float(k[1]),
                    "h":  float(k[2]),
                    "l":  float(k[3]),
                    "c":  float(k[4]),
                    "v":  float(k[5]),
                })
            except Exception:
                continue
        return rows

    # ── Ticker ────────────────────────────────────────────
    async def get_ticker(self, symbol: str) -> dict:
        data = await self._request(
            "GET", "/openApi/swap/v2/quote/ticker",
            params={"symbol": symbol},
            signed=False,
        )
        if data and isinstance(data, list):
            d = data[0]
        elif data and isinstance(data, dict):
            d = data
        else:
            return {"last": 0.0, "bid": 0.0, "ask": 0.0}
        return {
            "last": float(d.get("lastPrice", d.get("last", 0))),
            "bid":  float(d.get("bidPrice",  0)),
            "ask":  float(d.get("askPrice",  0)),
        }

    # ── Order Book (para OFI) ─────────────────────────────
    async def get_orderbook(self, symbol: str, depth: int = 20) -> dict:
        data = await self._request(
            "GET", "/openApi/swap/v2/quote/depth",
            params={"symbol": symbol, "limit": depth},
            signed=False,
        )
        if not data:
            return {"bids": [], "asks": []}
        return {
            "bids": [[float(x[0]), float(x[1])] for x in data.get("bids", [])],
            "asks": [[float(x[0]), float(x[1])] for x in data.get("asks", [])],
        }

    # ── Funding Rate ──────────────────────────────────────
    async def get_funding_rate(self, symbol: str) -> float:
        data = await self._request(
            "GET", "/openApi/swap/v2/quote/premiumIndex",
            params={"symbol": symbol},
            signed=False,
        )
        if not data:
            return 0.0
        if isinstance(data, list):
            data = data[0]
        return float(data.get("lastFundingRate", 0))

    # ── Open Interest ──────────────────────────────────────
    async def get_open_interest(self, symbol: str) -> float:
        data = await self._request(
            "GET", "/openApi/swap/v2/quote/openInterest",
            params={"symbol": symbol},
            signed=False,
        )
        if not data:
            return 0.0
        if isinstance(data, list):
            data = data[0]
        return float(data.get("openInterest", 0))

    # ── Market context completo ────────────────────────────
    async def get_market_context(self, symbol: str, ofi_levels: int = 5) -> dict:
        book, fr, oi = await asyncio.gather(
            self.get_orderbook(symbol, ofi_levels * 2),
            self.get_funding_rate(symbol),
            self.get_open_interest(symbol),
            return_exceptions=True,
        )
        # Calcular OFI del orderbook
        ofi = 0.0
        if isinstance(book, dict) and book["bids"] and book["asks"]:
            bid_vol = sum(b[1] for b in book["bids"][:ofi_levels])
            ask_vol = sum(a[1] for a in book["asks"][:ofi_levels])
            total   = bid_vol + ask_vol
            ofi     = (bid_vol - ask_vol) / total if total > 0 else 0.0

        return {
            "ofi":            ofi,
            "funding_rate":   fr if isinstance(fr, float) else 0.0,
            "open_interest":  oi if isinstance(oi, float) else 0.0,
            "prev_open_interest": 0.0,   # se rellena en main
        }

    # ── Símbolos del mercado ───────────────────────────────
    async def get_all_symbols(self) -> list:
        data = await self._request(
            "GET", "/openApi/swap/v2/quote/ticker",
            signed=False,
        )
        if not data or not isinstance(data, list):
            return []
        result = []
        for d in data:
            sym = d.get("symbol", "")
            if not sym.endswith("-USDT"):
                continue
            try:
                result.append({
                    "symbol": sym,
                    "volume": float(d.get("quoteVolume", d.get("volume", 0))),
                    "last":   float(d.get("lastPrice", 0)),
                })
            except Exception:
                continue
        return result

    # ── Posiciones actuales en BingX ──────────────────────
    async def get_open_positions(self) -> list:
        data = await self._request(
            "GET", "/openApi/swap/v2/user/positions",
        )
        if not data:
            return []
        positions = []
        for p in (data if isinstance(data, list) else []):
            try:
                size = float(p.get("positionAmt", 0))
                if abs(size) < 1e-9:
                    continue
                positions.append({
                    "symbol":    p["symbol"],
                    "side":      "LONG" if size > 0 else "SHORT",
                    "size":      abs(size),
                    "entry":     float(p.get("avgPrice", 0)),
                    "unrealized": float(p.get("unrealizedProfit", 0)),
                })
            except Exception:
                continue
        return positions

    # ── Set leverage ──────────────────────────────────────
    async def set_leverage(self, symbol: str, leverage: int):
        for side in ("LONG", "SHORT"):
            await self._request(
                "POST", "/openApi/swap/v2/trade/leverage",
                params={"symbol": symbol, "side": side, "leverage": leverage},
            )

    # ── Colocar orden ─────────────────────────────────────
    async def place_order(
        self,
        symbol: str,
        side: str,          # "LONG" | "SHORT"
        size: float,
        leverage: int,
        sl: float,
        tp: Optional[float],
        use_maker: bool = True,
        maker_timeout: int = 20,
        maker_offset_pct: float = 0.015,
    ) -> Optional[dict]:
        await self.set_leverage(symbol, leverage)

        action    = "BUY"  if side == "LONG" else "SELL"
        pos_side  = side
        order_type = "LIMIT" if use_maker else "MARKET"

        # Precio maker con pequeño offset para queue de maker
        ticker = await self.get_ticker(symbol)
        price  = ticker["last"]

        params: dict[str, Any] = {
            "symbol":           symbol,
            "side":             action,
            "positionSide":     pos_side,
            "type":             order_type,
            "quantity":         f"{size:.6f}",
        }

        if order_type == "LIMIT":
            offset  = price * maker_offset_pct / 100
            lmt     = price - offset if side == "LONG" else price + offset
            params["price"]     = f"{lmt:.6f}"
            params["timeInForce"] = "GTC"

        # Colocar orden principal
        order = await self._request("POST", "/openApi/swap/v2/trade/order", params=params)
        if not order:
            return None

        order_id = order.get("orderId") or order.get("order", {}).get("orderId")

        # Si es maker, esperar fill o cancelar y enviar market
        if order_type == "LIMIT" and order_id:
            filled = await self._wait_fill(symbol, order_id, maker_timeout)
            if not filled:
                await self._cancel_order(symbol, order_id)
                # Fallback a market
                params["type"]  = "MARKET"
                params.pop("price",        None)
                params.pop("timeInForce",  None)
                order = await self._request("POST", "/openApi/swap/v2/trade/order", params=params)
                if not order:
                    return None
                order_id = order.get("orderId") or order.get("order", {}).get("orderId")

        # SL automático
        await self._place_sl(symbol, side, size, sl)

        # TP automático
        if tp:
            await self._place_tp(symbol, side, size, tp)

        self.invalidate_balance_cache()
        return {"orderId": order_id}

    async def _wait_fill(self, symbol: str, order_id: str, timeout: int) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            await asyncio.sleep(2)
            data = await self._request(
                "GET", "/openApi/swap/v2/trade/order",
                params={"symbol": symbol, "orderId": order_id},
            )
            if data:
                status = data.get("status") or data.get("order", {}).get("status", "")
                if status in ("FILLED", "PARTIALLY_FILLED"):
                    return True
                if status in ("CANCELED", "EXPIRED", "REJECTED"):
                    return False
        return False

    async def _cancel_order(self, symbol: str, order_id: str):
        await self._request(
            "POST", "/openApi/swap/v2/trade/cancel",
            params={"symbol": symbol, "orderId": order_id},
        )

    async def _place_sl(self, symbol: str, side: str, size: float, sl: float):
        sl_side   = "SELL" if side == "LONG" else "BUY"
        stop_type = "STOP_MARKET"
        params = {
            "symbol":       symbol,
            "side":         sl_side,
            "positionSide": side,
            "type":         stop_type,
            "quantity":     f"{size:.6f}",
            "stopPrice":    f"{sl:.6f}",
            "workingType":  "MARK_PRICE",
        }
        await self._request("POST", "/openApi/swap/v2/trade/order", params=params)

    async def _place_tp(self, symbol: str, side: str, size: float, tp: float):
        tp_side = "SELL" if side == "LONG" else "BUY"
        params = {
            "symbol":       symbol,
            "side":         tp_side,
            "positionSide": side,
            "type":         "TAKE_PROFIT_MARKET",
            "quantity":     f"{size:.6f}",
            "stopPrice":    f"{tp:.6f}",
            "workingType":  "MARK_PRICE",
        }
        await self._request("POST", "/openApi/swap/v2/trade/order", params=params)

    # ── Cerrar posición ────────────────────────────────────
    async def close_position(self, symbol: str, side: str) -> bool:
        cl_side = "SELL" if side == "LONG" else "BUY"
        params = {
            "symbol":       symbol,
            "side":         cl_side,
            "positionSide": side,
            "type":         "MARKET",
            "quantity":     "0",          # BingX cierra todo con 0
            "reduceOnly":   "true",
        }
        r = await self._request("POST", "/openApi/swap/v2/trade/closePosition", params=params)
        self.invalidate_balance_cache()
        return r is not None
