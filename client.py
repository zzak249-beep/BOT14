"""
bingx/client.py — BingX Perpetuals API Client (HMAC-SHA256)
============================================================
Reglas críticas:
  - Parámetros SIEMPRE ordenados alfabéticamente
  - One-Way mode: NO positionSide
  - Balance: parsing defensivo multi-formato
  - POST firma body como query string
"""
from __future__ import annotations
import hashlib, hmac, logging, time
from typing import Optional
import requests
import pandas as pd

logger = logging.getLogger(__name__)


class BingXClient:
    def __init__(self, api_key: str, secret_key: str,
                 base_url: str = "https://open-api.bingx.com", timeout: int = 15):
        self.api_key    = api_key
        self.secret_key = secret_key
        self.base_url   = base_url.rstrip("/")
        self.timeout    = timeout
        self.session    = requests.Session()
        self.session.headers.update({"X-BX-APIKEY": self.api_key})

    def _sign(self, s: str) -> str:
        return hmac.new(self.secret_key.encode(), s.encode(), hashlib.sha256).hexdigest()

    def _get(self, path: str, params: dict = None) -> Optional[dict]:
        p = dict(params or {}); p["timestamp"] = int(time.time()*1000)
        qs = "&".join(f"{k}={v}" for k, v in sorted(p.items()))
        qs += f"&signature={self._sign(qs)}"
        try:
            r = self.session.get(f"{self.base_url}{path}?{qs}", timeout=self.timeout)
            r.raise_for_status()
            j = r.json()
            if j.get("code") == 0: return j.get("data")
            logger.warning(f"[BX GET] {path} → {j.get('code')} {j.get('msg')}")
        except Exception as e:
            logger.error(f"[BX GET] {path}: {e}")
        return None

    def _post(self, path: str, params: dict) -> Optional[dict]:
        p = dict(sorted(params.items())); p["timestamp"] = int(time.time()*1000)
        p = dict(sorted(p.items()))
        body = "&".join(f"{k}={v}" for k, v in p.items())
        url  = f"{self.base_url}{path}?{body}&signature={self._sign(body)}"
        try:
            r = self.session.post(url, timeout=self.timeout)
            r.raise_for_status()
            j = r.json()
            if j.get("code") == 0: return j.get("data") or {}
            logger.warning(f"[BX POST] {path} → {j.get('code')} {j.get('msg')}")
        except Exception as e:
            logger.error(f"[BX POST] {path}: {e}")
        return None

    # ── CUENTA ────────────────────────────────────────────────────────────────

    def get_balance(self) -> Optional[float]:
        data = self._get("/openApi/swap/v2/user/balance")
        if data is None: return None
        try:
            if "balance" in data:
                usdt = data["balance"].get("USDT", {})
                for k in ("availableMargin","available","free","equity"):
                    if k in usdt: return float(usdt[k])
            if isinstance(data, list):
                for item in data:
                    if item.get("asset") == "USDT":
                        for k in ("availableMargin","available","free"):
                            if k in item: return float(item[k])
            for k in ("availableMargin","available","free"):
                if k in data: return float(data[k])
        except Exception as e:
            logger.warning(f"[BX] Balance parse: {e}")
        return None

    # ── MERCADO ───────────────────────────────────────────────────────────────

    def get_klines_raw(self, symbol: str, interval: str, limit: int = 300) -> list:
        data = self._get("/openApi/swap/v2/quote/klines",
                         {"symbol": symbol, "interval": interval, "limit": limit})
        return data if isinstance(data, list) else []

    def get_last_price(self, symbol: str) -> Optional[float]:
        data = self._get("/openApi/swap/v2/quote/ticker", {"symbol": symbol})
        if isinstance(data, list) and data: data = data[0]
        if isinstance(data, dict):
            for k in ("lastPrice","last","price","c"):
                if k in data: return float(data[k])
        return None

    # ── APALANCAMIENTO ────────────────────────────────────────────────────────

    def set_leverage(self, symbol: str, leverage: int) -> bool:
        ok = True
        for side in (2, 3):
            r = self._post("/openApi/swap/v2/trade/leverage",
                           {"symbol": symbol, "side": side, "leverage": leverage})
            if r is None: ok = False
        return ok

    # ── POSICIONES ────────────────────────────────────────────────────────────

    def get_positions(self, symbol: str = None) -> list:
        data = self._get("/openApi/swap/v2/user/positions",
                         {"symbol": symbol} if symbol else {})
        if isinstance(data, list):
            return [p for p in data if abs(float(p.get("positionAmt",0))) > 0]
        return []

    def get_position(self, symbol: str) -> Optional[dict]:
        pos = self.get_positions(symbol)
        return pos[0] if pos else None

    def count_open(self) -> int:
        return len(self.get_positions())

    # ── ÓRDENES ───────────────────────────────────────────────────────────────

    def market_order(self, symbol: str, side: str, qty: float) -> Optional[dict]:
        return self._post("/openApi/swap/v2/trade/order", {
            "symbol": symbol, "side": side.upper(),
            "type": "MARKET", "quantity": round(qty, 6),
        })

    def stop_market(self, symbol: str, side: str, qty: float,
                    stop_px: float, reduce: bool = True) -> Optional[dict]:
        p = {"symbol": symbol, "side": side.upper(), "type": "STOP_MARKET",
             "quantity": round(qty,6), "stopPrice": round(stop_px,4)}
        if reduce: p["reduceOnly"] = "true"
        return self._post("/openApi/swap/v2/trade/order", p)

    def tp_market(self, symbol: str, side: str, qty: float,
                  stop_px: float) -> Optional[dict]:
        return self._post("/openApi/swap/v2/trade/order", {
            "symbol": symbol, "side": side.upper(), "type": "TAKE_PROFIT_MARKET",
            "quantity": round(qty,6), "stopPrice": round(stop_px,4), "reduceOnly": "true",
        })

    def close_position(self, symbol: str) -> Optional[dict]:
        pos = self.get_position(symbol)
        if not pos: return None
        amt  = float(pos.get("positionAmt",0))
        side = "SELL" if amt > 0 else "BUY"
        return self.market_order(symbol, side, abs(amt))

    def cancel_all_orders(self, symbol: str) -> bool:
        return self._post("/openApi/swap/v2/trade/cancelAllOrders",
                          {"symbol": symbol}) is not None

    def get_funding_rate(self, symbol: str) -> Optional[float]:
        data = self._get("/openApi/swap/v2/quote/premiumIndex", {"symbol": symbol})
        if isinstance(data, dict):
            for k in ("lastFundingRate","fundingRate"):
                if k in data: return float(data[k])
        return None


# ─── KLINE PARSER ─────────────────────────────────────────────────────────────

def parse_klines(raw: list) -> pd.DataFrame:
    """Convierte raw klines de BingX a DataFrame OHLCV con index UTC."""
    if not raw: return pd.DataFrame()
    rows = []
    for c in raw:
        try:
            rows.append({
                "timestamp": int(c.get("time", c.get("t", 0))),
                "open":   float(c.get("open",   c.get("o", 0))),
                "high":   float(c.get("high",   c.get("h", 0))),
                "low":    float(c.get("low",    c.get("l", 0))),
                "close":  float(c.get("close",  c.get("c", 0))),
                "volume": float(c.get("volume", c.get("v", 0))),
            })
        except Exception:
            continue
    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df.set_index("timestamp").sort_index()
