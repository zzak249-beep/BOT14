# -*- coding: utf-8 -*-
"""client.py -- BingX Perpetual Client v3: Speed-optimized.

Improvements:
  - Single persistent aiohttp session for ALL requests (no reconnect overhead)
  - Connection pool: 300 slots, keepalive 60s, DNS cache 600s
  - Parallel leverage set (LONG+SHORT simultaneously, not sequential)
  - get_all_tickers(): 1 call for all prices (used by pos_manager)
  - Correct BingX HMAC-SHA256 signature
"""
from __future__ import annotations
import asyncio, hashlib, hmac, random, time
from typing import Any
from urllib.parse import urlencode

import aiohttp
from loguru import logger

BASE_URL = "https://open-api.bingx.com"
_session: aiohttp.ClientSession | None = None


def _get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        from config import cfg
        conn = aiohttp.TCPConnector(
            limit             = 300,
            limit_per_host    = 100,
            ttl_dns_cache     = 600,
            keepalive_timeout = 60,
            ssl               = False,
            force_close       = False,
        )
        _session = aiohttp.ClientSession(
            connector = conn,
            timeout   = aiohttp.ClientTimeout(total=cfg.http_timeout, connect=4),
        )
    return _session


async def close_session() -> None:
    global _session
    if _session and not _session.closed:
        await _session.close()


def _sign(params: dict, secret: str) -> str:
    qs  = urlencode(sorted(params.items()))
    return hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()


def _auth(extra: dict | None = None) -> dict:
    from config import cfg
    p = dict(extra or {})
    p["timestamp"] = int(time.time() * 1000)
    p["signature"] = _sign(p, cfg.bingx_secret_key)
    return p


def _hdrs() -> dict:
    from config import cfg
    return {"X-BX-APIKEY": cfg.bingx_api_key}


async def _request(method: str, path: str,
                   params: dict | None = None,
                   auth: bool = True,
                   retries: int = 3) -> Any:
    sess = _get_session()
    p    = _auth(params) if auth else (params or {})
    hdrs = _hdrs()
    url  = BASE_URL + path

    for attempt in range(retries):
        try:
            if method == "GET":
                async with sess.get(url, params=p, headers=hdrs) as r:
                    data = await r.json(content_type=None)
                    if isinstance(data, dict):
                        code = data.get("code", 0)
                        msg  = data.get("msg", "")
                        if code not in (0, 200, None) and "Signature" in msg:
                            logger.error(f"FIRMA INVALIDA {path} — verifica API KEY/SECRET en Railway")
                    return data
            elif method == "POST":
                async with sess.post(url, params=p, headers=hdrs) as r:
                    return await r.json(content_type=None)
            elif method == "DELETE":
                async with sess.delete(url, params=p, headers=hdrs) as r:
                    return await r.json(content_type=None)
        except asyncio.TimeoutError:
            w = 1.5**attempt + random.uniform(0, 0.3)
            if attempt < retries-1:
                await asyncio.sleep(w)
        except Exception as e:
            logger.debug(f"{method} {path}: {e}")
            return {}
    return {}


async def _get(path, params=None, auth=False):
    return await _request("GET", path, params, auth=auth)

async def _post(path, params=None):
    return await _request("POST", path, params, auth=True)

async def _delete(path, params=None):
    return await _request("DELETE", path, params, auth=True)


# ── Market data ───────────────────────────────────────────────────────────────

async def fetch_klines(symbol: str, interval: str, limit: int = 200) -> list:
    resp = await _get("/openApi/swap/v3/quote/klines",
                      {"symbol": symbol, "interval": interval, "limit": limit})
    data = resp.get("data", []) if isinstance(resp, dict) else []
    return data if isinstance(data, list) else []


async def fetch_ohlcv(symbol: str, tf: str, limit: int = 200) -> dict | None:
    import numpy as np
    raw = await fetch_klines(symbol, tf, limit)
    if len(raw) < 50: return None
    try:
        return {
            "open":   np.array([float(c[1]) for c in raw], np.float64),
            "high":   np.array([float(c[2]) for c in raw], np.float64),
            "low":    np.array([float(c[3]) for c in raw], np.float64),
            "close":  np.array([float(c[4]) for c in raw], np.float64),
            "volume": np.array([float(c[5]) for c in raw], np.float64),
        }
    except Exception as e:
        logger.debug(f"ohlcv parse {symbol}: {e}")
        return None


# ── Account ───────────────────────────────────────────────────────────────────

async def get_balance() -> float:
    resp = await _get("/openApi/swap/v2/user/balance", auth=True)
    try:
        if not isinstance(resp, dict): return 0.0
        if resp.get("code", 0) not in (0, 200, None): return 0.0
        data = resp.get("data", {})
        if isinstance(data, dict):
            bal = data.get("balance", {})
            if isinstance(bal, dict):
                for k in ("availableMargin","available","balance"):
                    if k in bal: return float(bal[k])
            for k in ("availableMargin","available","equity","balance"):
                if k in data: return float(data[k])
    except Exception as e:
        logger.warning(f"get_balance: {e}")
    return 0.0


async def get_all_tickers() -> dict[str, float]:
    """1 call → prices for ALL symbols."""
    resp = await _get("/openApi/swap/v2/quote/ticker")
    out: dict[str, float] = {}
    try:
        for item in (resp.get("data", []) or []):
            sym = item.get("symbol","")
            p   = float(item.get("lastPrice", 0) or 0)
            if sym and p > 0: out[sym] = p
    except Exception as e:
        logger.warning(f"tickers: {e}")
    return out


async def get_all_positions() -> dict[str, dict]:
    resp = await _get("/openApi/swap/v2/user/positions", auth=True)
    try:
        data = resp.get("data", [])
        if isinstance(data, list):
            return {p["symbol"]: p for p in data
                    if abs(float(p.get("positionAmt",0))) > 1e-9}
    except Exception as e:
        logger.warning(f"positions: {e}")
    return {}


async def get_price(symbol: str) -> float:
    resp = await _get("/openApi/swap/v2/quote/price", {"symbol": symbol})
    try:
        p = float(resp.get("data",{}).get("price",0))
        if p > 0: return p
    except Exception: pass
    return 0.0


# ── Trading ───────────────────────────────────────────────────────────────────

async def set_leverage(symbol: str, leverage: int) -> None:
    """Set LONG+SHORT leverage in parallel (2x faster than sequential)."""
    from scanner import leverage_already_set, mark_leverage_set
    if leverage_already_set(symbol, leverage):
        return   # cached — skip 2 API calls + 150ms sleep
    await asyncio.gather(
        _post("/openApi/swap/v2/trade/leverage",
              {"symbol": symbol, "side": "LONG",  "leverage": leverage}),
        _post("/openApi/swap/v2/trade/leverage",
              {"symbol": symbol, "side": "SHORT", "leverage": leverage}),
    )
    mark_leverage_set(symbol, leverage)


async def place_market_order(symbol: str, side: str, size_usdt: float,
                             sl: float, tp: float) -> dict:
    resp = await _post("/openApi/swap/v2/trade/order", {
        "symbol":        symbol,
        "side":          side,
        "type":          "MARKET",
        "quoteOrderQty": size_usdt,
        "stopLoss":      str(round(sl, 8)),
        "takeProfit":    str(round(tp, 8)),
    })
    code = resp.get("code", -1) if isinstance(resp, dict) else -1
    if code not in (0, 200, None):
        logger.warning(f"[ORDER FAIL] {symbol} {side} code={code} {resp.get('msg','')}")
    return resp if isinstance(resp, dict) else {}


async def place_reduce_order(symbol: str, side: str, qty: float) -> dict:
    resp = await _post("/openApi/swap/v2/trade/order", {
        "symbol": symbol, "side": side, "type": "MARKET",
        "quantity": qty, "reduceOnly": "true",
    })
    return resp if isinstance(resp, dict) else {}


async def close_position(symbol: str, position: dict) -> Any:
    resp = await _post("/openApi/swap/v2/trade/closePosition", {"symbol": symbol})
    if isinstance(resp, dict) and resp.get("code",-1) in (0,200):
        return resp
    amt = float(position.get("positionAmt", 0))
    if abs(amt) < 1e-9: return {}
    return await _post("/openApi/swap/v2/trade/order", {
        "symbol": symbol,
        "side":   "SELL" if amt > 0 else "BUY",
        "type":   "MARKET", "quantity": abs(amt),
    })


async def cancel_all_orders(symbol: str) -> Any:
    return await _delete("/openApi/swap/v2/trade/allOpenOrders", {"symbol": symbol})
