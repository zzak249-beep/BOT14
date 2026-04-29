"""exchange/client.py — Async BingX Perpetual Futures client.

Uses aiohttp connection pool for low-latency concurrent requests.
"""
from __future__ import annotations
import asyncio
import hashlib
import hmac
import time
from typing import Any
from urllib.parse import urlencode

import aiohttp
from loguru import logger

BASE_URL = "https://open-api.bingx.com"

# ── Session singleton ─────────────────────────────────────────────────────────

_session: aiohttp.ClientSession | None = None
_ws_prices: dict[str, float] = {}   # symbol → latest mark price from WS


def _get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        connector = aiohttp.TCPConnector(
            limit=200, ttl_dns_cache=300, ssl=False
        )
        _session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=10),
        )
    return _session


async def close_session() -> None:
    global _session
    if _session and not _session.closed:
        await _session.close()


# ── Auth helpers ──────────────────────────────────────────────────────────────

def _sign(params: dict, secret: str) -> str:
    qs = urlencode(sorted(params.items()))
    return hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()


def _auth_params(params: dict | None = None) -> dict:
    from core.config import cfg
    p = dict(params or {})
    p["timestamp"] = int(time.time() * 1000)
    p["signature"] = _sign(p, cfg.bingx_secret_key)
    return p


def _headers() -> dict:
    from core.config import cfg
    return {"X-BX-APIKEY": cfg.bingx_api_key}


# ── Generic request ───────────────────────────────────────────────────────────

async def _get(path: str, params: dict | None = None, auth: bool = False) -> Any:
    sess = _get_session()
    p = _auth_params(params) if auth else (params or {})
    try:
        async with sess.get(BASE_URL + path, params=p, headers=_headers() if auth else {}) as r:
            return await r.json(content_type=None)
    except Exception as e:
        logger.warning(f"GET {path} error: {e}")
        return {}


async def _post(path: str, params: dict | None = None) -> Any:
    sess = _get_session()
    p = _auth_params(params)
    try:
        async with sess.post(BASE_URL + path, params=p, headers=_headers()) as r:
            return await r.json(content_type=None)
    except Exception as e:
        logger.warning(f"POST {path} error: {e}")
        return {}


async def _delete(path: str, params: dict | None = None) -> Any:
    sess = _get_session()
    p = _auth_params(params)
    try:
        async with sess.delete(BASE_URL + path, params=p, headers=_headers()) as r:
            return await r.json(content_type=None)
    except Exception as e:
        logger.warning(f"DELETE {path} error: {e}")
        return {}


# ── Market data ───────────────────────────────────────────────────────────────

async def fetch_all_tickers() -> list[dict]:
    """Return all perpetual ticker stats."""
    resp = await _get("/openApi/swap/v2/quote/ticker")
    data = resp.get("data", resp) if isinstance(resp, dict) else resp
    if isinstance(data, list):
        return data
    # Sometimes wrapped differently
    if isinstance(data, dict):
        return list(data.values())
    return []


async def fetch_klines(symbol: str, interval: str, limit: int = 200) -> list[list]:
    """Fetch OHLCV klines for one symbol/timeframe."""
    resp = await _get("/openApi/swap/v3/quote/klines", {
        "symbol": symbol, "interval": interval, "limit": limit
    })
    data = resp.get("data", []) if isinstance(resp, dict) else []
    return data if isinstance(data, list) else []


async def _fetch_ohlcv(symbol: str, tf: str) -> dict | None:
    """Fetch klines and return dict of numpy arrays."""
    import numpy as np
    raw = await fetch_klines(symbol, tf, limit=200)
    if len(raw) < 50:
        return None
    try:
        # BingX returns [open_time, open, high, low, close, volume, close_time]
        opens  = np.array([float(c[1]) for c in raw], dtype=np.float64)
        highs  = np.array([float(c[2]) for c in raw], dtype=np.float64)
        lows   = np.array([float(c[3]) for c in raw], dtype=np.float64)
        closes = np.array([float(c[4]) for c in raw], dtype=np.float64)
        vols   = np.array([float(c[5]) for c in raw], dtype=np.float64)
        return {"open": opens, "high": highs, "low": lows, "close": closes, "volume": vols}
    except Exception as e:
        logger.debug(f"OHLCV parse error {symbol} {tf}: {e}")
        return None


async def fetch_universe_concurrent(symbols: list[str]) -> dict[str, dict]:
    """
    Fetch primary + confirmation + trend timeframes for all symbols concurrently.
    Returns {symbol: {"p": primary_ohlcv, "h": confirm_ohlcv, "t": trend_ohlcv}}
    """
    from core.config import cfg

    async def _fetch_one(sym: str) -> tuple[str, dict | None, dict | None, dict | None]:
        p, h, t = await asyncio.gather(
            _fetch_ohlcv(sym, cfg.timeframe),
            _fetch_ohlcv(sym, cfg.confirm_tf),
            _fetch_ohlcv(sym, cfg.trend_tf),
        )
        return sym, p, h, t

    tasks = [asyncio.create_task(_fetch_one(sym)) for sym in symbols]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    out: dict[str, dict] = {}
    for r in results:
        if isinstance(r, Exception):
            continue
        sym, p, h, t = r
        if p is not None:
            out[sym] = {"p": p, "h": h, "t": t}
    return out


# ── Account ───────────────────────────────────────────────────────────────────

async def get_balance() -> float:
    resp = await _get("/openApi/swap/v2/user/balance", auth=True)
    try:
        data = resp.get("data", {})
        if isinstance(data, dict):
            bal = data.get("balance", {})
            if isinstance(bal, dict):
                return float(bal.get("availableMargin", bal.get("balance", 0)))
            return float(data.get("availableMargin", data.get("equity", 0)))
    except Exception as e:
        logger.warning(f"get_balance parse error: {e} | resp={resp}")
    return 0.0


async def get_all_positions() -> dict[str, dict]:
    """Return {symbol: position_dict} for non-zero positions."""
    resp = await _get("/openApi/swap/v2/user/positions", auth=True)
    try:
        data = resp.get("data", [])
        if isinstance(data, list):
            return {
                p["symbol"]: p
                for p in data
                if abs(float(p.get("positionAmt", 0))) > 1e-9
            }
    except Exception as e:
        logger.warning(f"get_positions parse error: {e}")
    return {}


# ── Trading ───────────────────────────────────────────────────────────────────

async def set_leverage(symbol: str, leverage: int) -> Any:
    return await _post("/openApi/swap/v2/trade/leverage", {
        "symbol": symbol, "side": "LONG", "leverage": leverage
    })


async def place_market_order(
    symbol: str, side: str, size_usdt: float,
    sl: float, tp: float
) -> dict:
    """Place a market order with SL/TP. side = 'BUY' | 'SELL'."""
    from core.config import cfg
    params: dict[str, Any] = {
        "symbol":     symbol,
        "side":       side,
        "positionSide": "LONG" if side == "BUY" else "SHORT",
        "type":       "MARKET",
        "quoteOrderQty": size_usdt,   # size in USDT
        "stopLoss":   str(sl),
        "takeProfit": str(tp),
    }
    resp = await _post("/openApi/swap/v2/trade/order", params)
    return resp if isinstance(resp, dict) else {"raw": resp}


async def close_position(symbol: str, position: dict) -> Any:
    """Market-close an open position."""
    amt  = float(position.get("positionAmt", 0))
    side = "SELL" if amt > 0 else "BUY"
    pos_side = "LONG" if amt > 0 else "SHORT"
    return await _post("/openApi/swap/v2/trade/closePosition", {
        "symbol": symbol, "positionSide": pos_side,
    })


async def cancel_all_orders(symbol: str) -> Any:
    return await _delete("/openApi/swap/v2/trade/allOpenOrders", {"symbol": symbol})


async def get_price(symbol: str) -> float:
    """Return latest price (from WS cache or REST fallback)."""
    if symbol in _ws_prices:
        return _ws_prices[symbol]
    resp = await _get("/openApi/swap/v2/quote/price", {"symbol": symbol})
    try:
        data = resp.get("data", {})
        return float(data.get("price", 0))
    except Exception:
        return 0.0


# ── WebSocket price stream ────────────────────────────────────────────────────

async def ws_price_stream(symbols: list[str]) -> None:
    """Subscribe to mark price WS stream and populate _ws_prices."""
    import json as _json
    import websockets  # type: ignore

    streams = "/".join(f"{s.replace('-','').lower()}@markPrice" for s in symbols)
    url = f"wss://open-api-ws.bingx.com/market?streams={streams}"

    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                logger.info(f"WS price stream connected ({len(symbols)} symbols)")
                async for msg in ws:
                    try:
                        if isinstance(msg, bytes):
                            import gzip
                            msg = gzip.decompress(msg).decode()
                        d = _json.loads(msg)
                        data = d.get("data", d)
                        sym = data.get("s", "")
                        p   = data.get("p", data.get("mp", 0))
                        if sym and p:
                            # normalise back to BingX format e.g. BTCUSDT → BTC-USDT
                            if "-" not in sym and sym.endswith("USDT"):
                                sym = sym[:-4] + "-USDT"
                            _ws_prices[sym] = float(p)
                    except Exception:
                        pass
        except Exception as e:
            logger.debug(f"WS reconnecting: {e}")
            await asyncio.sleep(5)
