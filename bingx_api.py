"""
BingX REST API client — retry + rate limiter + firma HMAC-SHA256.
"""
from __future__ import annotations
import hashlib
import hmac
import logging
import time
from typing import List, Optional
import requests
from utils import retry, market_rl, trading_rl
import config as cfg

logger = logging.getLogger(__name__)
BASE_URL = "https://open-api.bingx.com"


class BingXError(Exception):
    pass


class BingXAPI:
    def __init__(self):
        self.api_key    = cfg.BINGX_API_KEY
        self.secret_key = cfg.BINGX_SECRET_KEY
        self._session   = requests.Session()
        self._session.headers.update({"X-BX-APIKEY": self.api_key})

    # ── Firma ──────────────────────────────────────────────────
    def _sign(self, params: dict) -> dict:
        params["timestamp"] = int(time.time() * 1000)
        qs  = "&".join(f"{k}={v}" for k, v in params.items())
        sig = hmac.new(self.secret_key.encode(), qs.encode(), hashlib.sha256).hexdigest()
        params["signature"] = sig
        return params

    def _raw(self, method: str, path: str, params: dict, signed: bool, rl) -> dict:
        rl.acquire()
        p = dict(params)
        if signed:
            p = self._sign(p)
        url = BASE_URL + path
        try:
            r = (self._session.get if method == "GET" else self._session.post)(
                url, params=p, timeout=15
            )
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            raise BingXError(f"{method} {path}: {e}")
        code = data.get("code", 0)
        if code != 0:
            raise BingXError(f"API {path} code={code} msg={data.get('msg','')}")
        return data

    @retry(attempts=3, delay=1.0, backoff=2.0, exc=(BingXError, requests.RequestException))
    def _get(self, path: str, params: dict = None, signed: bool = False, rl=None):
        return self._raw("GET", path, params or {}, signed, rl or market_rl)

    @retry(attempts=3, delay=1.0, backoff=2.0, exc=(BingXError, requests.RequestException))
    def _post(self, path: str, params: dict = None):
        return self._raw("POST", path, params or {}, True, trading_rl)

    # ── Market data ────────────────────────────────────────────
    def get_contracts(self) -> List[dict]:
        data = self._get("/openApi/swap/v2/quote/contracts")
        return (data or {}).get("data", [])

    def get_tickers(self) -> List[dict]:
        data = self._get("/openApi/swap/v2/quote/ticker")
        result = (data or {}).get("data", [])
        return result if isinstance(result, list) else ([result] if result else [])

    def get_klines(self, symbol: str, interval: str, limit: int = 200) -> List[dict]:
        data = self._get(
            "/openApi/swap/v3/quote/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
        )
        raw = (data or {}).get("data", [])
        out = []
        for c in raw:
            if isinstance(c, (list, tuple)):
                out.append({"time": int(c[0]),   "open":  float(c[1]),
                             "high": float(c[2]), "low":   float(c[3]),
                             "close": float(c[4]),"volume":float(c[5])})
            elif isinstance(c, dict):
                out.append({"time":   int(c.get("time", c.get("openTime", 0))),
                             "open":  float(c["open"]),  "high":  float(c["high"]),
                             "low":   float(c["low"]),   "close": float(c["close"]),
                             "volume":float(c.get("volume", 0))})
        return out

    def get_mark_price(self, symbol: str) -> float:
        data = self._get("/openApi/swap/v2/quote/premiumIndex", {"symbol": symbol})
        return float((data or {}).get("data", {}).get("markPrice", 0))

    def get_price(self, symbol: str) -> float:
        """Precio mark (fallback a last price si falla)."""
        try:
            p = self.get_mark_price(symbol)
            if p > 0:
                return p
        except Exception:
            pass
        data = self._get("/openApi/swap/v2/quote/price", {"symbol": symbol})
        return float((data or {}).get("data", {}).get("price", 0))

    # ── Cuenta ─────────────────────────────────────────────────
    def get_balance(self) -> dict:
        data = self._get("/openApi/swap/v2/user/balance", signed=True)
        bal  = (data or {}).get("data", {})
        return bal.get("balance", bal) if isinstance(bal, dict) else {}

    def get_available_balance(self) -> float:
        try:
            bal = self.get_balance()
            return float(bal.get("availableMargin", bal.get("available", 0)))
        except Exception:
            return 0.0

    def get_equity(self) -> float:
        try:
            bal = self.get_balance()
            return float(bal.get("equity", 0))
        except Exception:
            return 0.0

    def get_positions(self, symbol: str = None) -> List[dict]:
        params = {"symbol": symbol} if symbol else {}
        data   = self._get("/openApi/swap/v2/user/positions", params, signed=True)
        return (data or {}).get("data", []) or []

    def get_active_positions_set(self) -> set:
        return {
            p["symbol"]
            for p in self.get_positions()
            if float(p.get("positionAmt", 0)) != 0
        }

    def get_open_orders(self, symbol: str = None) -> List[dict]:
        params = {"symbol": symbol} if symbol else {}
        data   = self._get("/openApi/swap/v2/trade/openOrders", params, signed=True, rl=trading_rl)
        return (data or {}).get("data", {}).get("orders", []) or []

    def get_last_orders(self, symbol: str, limit: int = 10) -> List[dict]:
        """Últimas órdenes ejecutadas para calcular PnL de cierre."""
        data = self._get(
            "/openApi/swap/v2/trade/allOrders",
            {"symbol": symbol, "limit": limit},
            signed=True, rl=trading_rl,
        )
        return (data or {}).get("data", {}).get("orders", []) or []

    # ── Configuración ──────────────────────────────────────────
    def set_leverage(self, symbol: str, leverage: int):
        for side in ("LONG", "SHORT"):
            try:
                self._post("/openApi/swap/v2/trade/leverage",
                           {"symbol": symbol, "side": side, "leverage": leverage})
            except Exception as e:
                logger.debug(f"set_leverage {symbol} {side}: {e}")

    def set_margin_type(self, symbol: str, margin_type: str = "ISOLATED"):
        try:
            self._post("/openApi/swap/v2/trade/marginType",
                       {"symbol": symbol, "marginType": margin_type})
        except Exception as e:
            logger.debug(f"set_margin_type {symbol}: {e}")

    # ── Órdenes ────────────────────────────────────────────────
    def place_order(
        self,
        symbol: str,
        side: str,
        position_side: str,
        quantity: float,
        order_type: str = "MARKET",
        price: float = None,
        stop_price: float = None,
        working_type: str = "MARK_PRICE",
    ) -> dict:
        params = {
            "symbol":       symbol,
            "side":         side,
            "positionSide": position_side,
            "type":         order_type,
            "quantity":     f"{quantity:.6f}",
        }
        if price:      params["price"]       = f"{price:.8f}"
        if stop_price: params["stopPrice"]   = f"{stop_price:.8f}"
        if stop_price: params["workingType"] = working_type

        if cfg.DRY_RUN:
            logger.info(f"[DRY] ORDER {params}")
            return {"dry": True, "params": params, "orderId": f"DRY-{int(time.time())}"}

        result = self._post("/openApi/swap/v2/trade/order", params)
        return (result or {}).get("data", {})

    def cancel_all_orders(self, symbol: str):
        try:
            self._post("/openApi/swap/v2/trade/cancelAllOrders", {"symbol": symbol})
        except Exception as e:
            logger.debug(f"cancel_all {symbol}: {e}")

    def close_position(self, symbol: str, position_side: str, quantity: float) -> dict:
        side = "SELL" if position_side == "LONG" else "BUY"
        return self.place_order(symbol, side, position_side, quantity)


api = BingXAPI()
