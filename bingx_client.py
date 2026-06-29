"""
BingX Perpetual Futures REST client — Cross margin / Hedge mode.
Signing: HMAC-SHA256 over urlencode(sorted(params)).
"""

import hashlib
import hmac
import logging
import time
import urllib.parse

import requests

log = logging.getLogger("bingx")


class BingXClient:
    def __init__(self, api_key: str, secret_key: str, base_url: str):
        self.api_key    = api_key
        self.secret_key = secret_key
        self.base_url   = base_url.rstrip("/")
        self._session   = requests.Session()
        self._session.headers.update({"X-BX-APIKEY": api_key})
        self._ticker_cache: dict  = {}
        self._ticker_ts: float    = 0

    # ── Signing ───────────────────────────────────────────────

    def _sign(self, params: dict) -> str:
        qs = urllib.parse.urlencode(sorted(params.items()))
        return hmac.new(
            self.secret_key.encode(), qs.encode(), hashlib.sha256
        ).hexdigest()

    @staticmethod
    def _ts() -> int:
        return int(time.time() * 1000)

    # ── HTTP ──────────────────────────────────────────────────

    def _get(self, path: str, params: dict = None):
        p = dict(params or {})
        p["timestamp"] = self._ts()
        p["signature"] = self._sign(p)
        r = self._session.get(f"{self.base_url}{path}", params=p, timeout=12)
        r.raise_for_status()
        d = r.json()
        if d.get("code") != 0:
            raise RuntimeError(f"BingX [{d.get('code')}] {d.get('msg')}")
        return d.get("data") or {}

    def _post(self, path: str, params: dict):
        p = dict(params)
        p["timestamp"] = self._ts()
        p["signature"] = self._sign(p)
        r = self._session.post(f"{self.base_url}{path}", data=p, timeout=12)  # body not URL
        r.raise_for_status()
        d = r.json()
        if d.get("code") != 0:
            raise RuntimeError(f"BingX [{d.get('code')}] {d.get('msg')}")
        return d.get("data") or {}

    def _delete(self, path: str, params: dict):
        p = dict(params)
        p["timestamp"] = self._ts()
        p["signature"] = self._sign(p)
        r = self._session.delete(f"{self.base_url}{path}", params=p, timeout=12)
        r.raise_for_status()
        d = r.json()
        if d.get("code") != 0:
            raise RuntimeError(f"BingX [{d.get('code')}] {d.get('msg')}")
        return d.get("data") or {}

    # ── Market data ───────────────────────────────────────────

    def get_klines(self, symbol: str, interval: str, limit: int = 120) -> list:
        raw = self._get(
            "/openApi/swap/v3/quote/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
        )
        candles = [
            {
                "timestamp": int(k["time"]),
                "open":   float(k["open"]),
                "high":   float(k["high"]),
                "low":    float(k["low"]),
                "close":  float(k["close"]),
                "volume": float(k["volume"]),
            }
            for k in (raw if isinstance(raw, list) else [])
        ]
        return sorted(candles, key=lambda x: x["timestamp"])

    def get_mark_price(self, symbol: str) -> float:
        d = self._get("/openApi/swap/v2/quote/premiumIndex", {"symbol": symbol})
        return float(d.get("markPrice", 0))

    def get_funding_rate(self, symbol: str) -> float:
        """Current funding rate for harvest filter."""
        try:
            d = self._get("/openApi/swap/v2/quote/premiumIndex", {"symbol": symbol})
            return float(d.get("lastFundingRate", 0))
        except Exception:
            return 0.0

    def get_symbol_info(self, symbol: str) -> dict:
        for s in self.get_all_contracts():
            if s.get("symbol") == symbol:
                return s
        return {}

    def get_all_contracts(self) -> list:
        data = self._get("/openApi/swap/v2/quote/contracts")
        return data if isinstance(data, list) else []

    def get_all_tickers(self) -> list:
        """All 24h tickers — cached 60s to avoid hammering API."""
        if time.time() - self._ticker_ts < 60:
            return list(self._ticker_cache.values())
        try:
            data = self._get("/openApi/swap/v2/quote/ticker")
            tickers = data if isinstance(data, list) else []
            self._ticker_cache = {t.get("symbol"): t for t in tickers}
            self._ticker_ts = time.time()
            return tickers
        except Exception as e:
            log.warning(f"get_all_tickers: {e}")
            return []

    def get_top_symbols(self, top_n: int, min_volume_usdt: float) -> list:
        """
        Returns top_n symbols by 24h quote volume filtered by min_volume_usdt.
        Enriches contract list with ticker volume (logs: 'enriqueciendo con /ticker').
        """
        contracts = self.get_all_contracts()
        tickers   = {t.get("symbol"): t for t in self.get_all_tickers()}

        valid = []
        no_vol = 0
        for c in contracts:
            sym = c.get("symbol", "")
            t   = tickers.get(sym, {})
            vol = float(t.get("quoteVolume", t.get("volume", 0)))
            if vol == 0:
                no_vol += 1
            if vol >= min_volume_usdt:
                valid.append((sym, vol))

        if no_vol:
            log.info(f"contracts sin volumen → enriqueciendo con /ticker | {no_vol} sin datos")

        valid.sort(key=lambda x: x[1], reverse=True)
        log.info(f"get_top_symbols: {len(contracts)} total, {len(valid)} ≥ {min_volume_usdt:,.0f} vol")
        return [s for s, _ in valid[:top_n]]

    # ── Account ───────────────────────────────────────────────

    def _balance_usdt(self) -> dict:
        data = self._get("/openApi/swap/v2/user/balance")
        bal = data.get("balance") or {}
        if isinstance(bal, dict):
            return bal
        if isinstance(bal, list):
            for a in bal:
                if isinstance(a, dict) and a.get("asset") == "USDT":
                    return a
        return {}

    def get_equity(self) -> float:
        b = self._balance_usdt()
        return float(b.get("equity", b.get("balance", 0)))

    def get_available_margin(self) -> float:
        b = self._balance_usdt()
        return float(b.get("availableMargin", 0))

    # ── Positions ─────────────────────────────────────────────

    def get_positions(self, symbol: str = None) -> list:
        params = {}
        if symbol:
            params["symbol"] = symbol
        raw = self._get("/openApi/swap/v2/user/positions", params)
        result = []
        for p in (raw if isinstance(raw, list) else []):
            size = abs(float(p.get("positionAmt", 0)))
            if size < 1e-9:
                continue
            result.append({
                "symbol":        p.get("symbol"),
                "positionSide":  p.get("positionSide"),
                "size":          size,
                "entryPrice":    float(p.get("avgPrice", 0)),
                "unrealizedPnl": float(p.get("unrealizedProfit", 0)),
                "leverage":      int(p.get("leverage", 1)),
            })
        return result

    # ── Account setup ─────────────────────────────────────────

    def set_leverage(self, symbol: str, leverage: int):
        for side in ("LONG", "SHORT"):
            try:
                self._post("/openApi/swap/v2/trade/leverage",
                           {"symbol": symbol, "leverage": leverage, "side": side})
            except Exception as e:
                log.warning(f"set_leverage {side} {symbol}: {e}")

    # ── Orders ────────────────────────────────────────────────

    def place_market_order(self, symbol: str, side: str,
                           position_side: str, quantity: float) -> dict:
        return self._post("/openApi/swap/v2/trade/order", {
            "symbol": symbol, "side": side,
            "positionSide": position_side,
            "type": "MARKET", "quantity": str(quantity),
        })

    def place_limit_order(self, symbol: str, side: str,
                          position_side: str, price: float, quantity: float) -> dict:
        return self._post("/openApi/swap/v2/trade/order", {
            "symbol": symbol, "side": side,
            "positionSide": position_side,
            "type": "LIMIT", "price": f"{price:.8g}",
            "quantity": str(quantity), "timeInForce": "GTC",
        })

    def place_stop_market(self, symbol: str, position_side: str,
                          stop_price: float, quantity: float) -> dict:
        """Stop-market order in hedge mode — no closePosition, use quantity."""
        side = "SELL" if position_side == "LONG" else "BUY"
        return self._post("/openApi/swap/v2/trade/order", {
            "symbol":       symbol,
            "side":         side,
            "positionSide": position_side,
            "type":         "STOP_MARKET",
            "stopPrice":    f"{stop_price:.6f}",
            "quantity":     f"{quantity:.6f}".rstrip("0").rstrip("."),
        })

    def close_position(self, symbol: str, position_side: str, quantity: float) -> dict:
        side = "SELL" if position_side == "LONG" else "BUY"
        return self.place_market_order(symbol, side, position_side, quantity)

    # ── Order management ──────────────────────────────────────

    def get_open_orders(self, symbol: str) -> list:
        try:
            raw = self._get("/openApi/swap/v2/trade/openOrders", {"symbol": symbol})
            return raw if isinstance(raw, list) else raw.get("orders", [])
        except Exception as e:
            log.warning(f"get_open_orders {symbol}: {e}")
            return []

    def cancel_order(self, symbol: str, order_id: str):
        try:
            self._delete("/openApi/swap/v2/trade/order",
                         {"symbol": symbol, "orderId": order_id})
        except Exception as e:
            log.warning(f"cancel_order {order_id}: {e}")

    def cancel_all_open_orders(self, symbol: str):
        """
        FIX: cancel all pending orders before placing new TP/SL.
        Prevents order accumulation (EURUSD had 24 orders).
        """
        try:
            self._delete("/openApi/swap/v2/trade/allOpenOrders", {"symbol": symbol})
            log.debug(f"cancel_all_open_orders {symbol} (batch)")
        except Exception:
            orders = self.get_open_orders(symbol)
            for o in orders:
                oid = o.get("orderId") or o.get("id")
                if oid:
                    self.cancel_order(symbol, str(oid))
            if orders:
                log.debug(f"cancel_all_open_orders {symbol} (individual n={len(orders)})")
