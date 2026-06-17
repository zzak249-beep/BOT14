"""
QF×JP Bot v6.4 — BingX Client
FIRMA: parseParam oficial BingX
  sorted(params) + &timestamp=xxx al final → HMAC-SHA256 → &signature=xxx

Ref: https://bingx-api.github.io/docs/#/swapV2/authentication.html
"""
import asyncio
import hashlib
import hmac
import logging
import math
import time
from typing import Optional
from urllib.parse import urlencode

import aiohttp
import config as C

log = logging.getLogger("bingx")

# ── Firma ─────────────────────────────────────────────────────────────────────

def _ts() -> str:
    return str(int(time.time() * 1000))


def _build_signed_qs(params: dict) -> str:
    sorted_keys = sorted(params.keys())
    parts       = [f"{k}={params[k]}" for k in sorted_keys]
    base        = "&".join(parts)
    ts          = _ts()
    payload     = (base + "&timestamp=" + ts) if base else ("timestamp=" + ts)
    signature   = hmac.new(
        C.BINGX_SECRET_KEY.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    qs = payload + "&signature=" + signature
    log.debug("FIRMA payload=%s sig=%s...", payload[:80], signature[:12])
    return qs

# ── Cliente HTTP ──────────────────────────────────────────────────────────────

class BingXClient:
    BASE = C.BINGX_BASE_URL

    # v6.5: cache de positionSide detectado — compartido entre instancias
    _ps_cache:     dict = {}
    _account_mode: str  = ""   # "HEDGE" o "ONEWAY" — se detecta solo en el primer error

    def __init__(self):
        self._session:       Optional[aiohttp.ClientSession] = None
        self._precision_map: dict[str, int]   = {}
        self._min_qty_map:   dict[str, float] = {}
        log.info("BingXClient v6.5 iniciado — firma: sorted+ts_al_final (parseParam oficial)")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"X-BX-APIKEY": C.BINGX_API_KEY},
                timeout=aiohttp.ClientTimeout(total=15),
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    # ── HTTP primitives ───────────────────────────────────────────────────────

    async def _get(self, path: str, params: dict | None = None, signed: bool = False) -> dict:
        session = await self._get_session()
        base    = params or {}
        for attempt in range(3):
            try:
                if signed:
                    qs  = _build_signed_qs(base)
                    url = f"{self.BASE}{path}?{qs}"
                elif base:
                    url = f"{self.BASE}{path}?{urlencode(base)}"
                else:
                    url = f"{self.BASE}{path}"
                async with session.get(url) as r:
                    return await r.json(content_type=None)
            except Exception as e:
                if attempt == 2:
                    log.error("GET %s error: %s", path, e)
                    raise
                await asyncio.sleep(1.5 ** attempt)
        return {}

    async def _post(self, path: str, params: dict) -> dict:
        session = await self._get_session()
        for attempt in range(3):
            try:
                qs  = _build_signed_qs(params)
                url = f"{self.BASE}{path}?{qs}"
                async with session.post(url) as r:
                    return await r.json(content_type=None)
            except Exception as e:
                if attempt == 2:
                    log.error("POST %s error: %s", path, e)
                    raise
                await asyncio.sleep(1.5 ** attempt)
        return {}

    async def _delete(self, path: str, params: dict) -> dict:
        session = await self._get_session()
        for attempt in range(3):
            try:
                qs  = _build_signed_qs(params)
                url = f"{self.BASE}{path}?{qs}"
                async with session.delete(url) as r:
                    return await r.json(content_type=None)
            except Exception as e:
                if attempt == 2:
                    log.error("DELETE %s error: %s", path, e)
                    raise
                await asyncio.sleep(1.5 ** attempt)
        return {}

    # ── Redondeo de cantidad ──────────────────────────────────────────────────

    def _round_qty(self, symbol: str, qty: float) -> float:
        precision = self._precision_map.get(symbol, 6)
        if precision == 0:
            return float(math.floor(qty))
        factor = 10 ** precision
        return math.floor(qty * factor) / factor

    def _check_min_qty(self, symbol: str, qty: float) -> bool:
        min_q = self._min_qty_map.get(symbol, 0.0)
        return qty >= min_q if min_q > 0 else True

    def _safe_qty_for_sl(self, symbol: str, qty: float) -> float:
        """Reduce ligeramente la qty para evitar rechazo por redondeo en SL/TP."""
        precision = self._precision_map.get(symbol, 6)
        factor    = 10 ** precision
        return math.floor(qty * factor * 0.999) / factor

    # ── positionSide: detección por cache, sin llamadas extra ────────────────

    def _resolve_ps(self, symbol: str, direction: str) -> str:
        """
        v6.5 FIX 109420: resuelve positionSide sin golpear la API extra.
        - Si ya detectamos que la cuenta es One-Way → siempre BOTH.
        - Si ya cacheamos el valor que funcionó para este símbolo → usarlo.
        - Si no hay info → usar direction (LONG/SHORT), válido en Hedge Mode,
          que es el modo por defecto en cuentas BingX nuevas.
        """
        if self.__class__._account_mode == "ONEWAY":
            return "BOTH"
        return self.__class__._ps_cache.get(symbol, direction)

    def _handle_ps_error(self, msg: str, symbol: str, direction: str, params: dict) -> bool:
        """
        Ajusta params['positionSide'] según el mensaje de error de BingX.
        Retorna True si hay que reintentar la petición con los params ajustados.
        """
        m = msg.lower()
        if "long or short" in m or ("positionside" in m and "only" in m):
            self.__class__._account_mode = "HEDGE"
            self.__class__._ps_cache[symbol] = direction
            params["positionSide"] = direction
            log.warning("[%s] Hedge mode confirmado → positionSide=%s", symbol, direction)
            return True
        if "position not exist" in m and params.get("positionSide") != "BOTH":
            self.__class__._account_mode = "ONEWAY"
            params["positionSide"] = "BOTH"
            log.warning("[%s] One-Way mode confirmado → positionSide=BOTH", symbol)
            return True
        return False

    # ── Símbolos ──────────────────────────────────────────────────────────────

    async def get_all_symbols(self) -> list[str]:
        data = await self._get("/openApi/swap/v2/quote/contracts")
        raw  = data.get("data", [])
        if isinstance(raw, dict):
            raw = raw.get("contracts", raw.get("list", []))
        if not isinstance(raw, list):
            raw = []

        symbols      = []
        vol_map:     dict[str, float] = {}
        vol_detected = 0

        _bad_prefixes = ("BEAR", "BULL", "PUMP", "NCS")

        for item in raw:
            if not isinstance(item, dict):
                continue
            sym = item.get("symbol", "")
            if not sym:
                continue
            if "-" not in sym and sym.endswith("USDT"):
                sym = sym[:-4] + "-USDT"
            if not sym.endswith("-USDT"):
                continue
            if sym in C.BLACKLIST:
                continue
            base_coin = sym.replace("-USDT", "")
            if any(base_coin.startswith(p) for p in _bad_prefixes):
                continue

            self._precision_map[sym] = int(item.get("volumePrecision",    6) or 6)
            self._min_qty_map[sym]   = float(item.get("tradeMinQuantity", 0) or 0)

            vol_raw = (
                item.get("volume24h") or item.get("vol24h") or
                item.get("quoteVolume") or item.get("turnover24h") or
                item.get("tradeAmt") or item.get("vol") or 0
            )
            vol = float(vol_raw) if vol_raw else 0.0
            if vol > 0:
                vol_detected += 1
            vol_map[sym] = vol
            symbols.append(sym)

        if vol_detected == 0 and symbols:
            log.info("contracts sin volumen → enriqueciendo con /ticker")
            try:
                td = await self._get("/openApi/swap/v2/quote/ticker")
                for t in (td.get("data", []) or []):
                    s = t.get("symbol", "")
                    if "-" not in s and s.endswith("USDT"):
                        s = s[:-4] + "-USDT"
                    qv = float(t.get("quoteVolume", 0) or t.get("volume", 0) or 0)
                    if s in vol_map:
                        vol_map[s] = qv
                        if qv > 0:
                            vol_detected += 1
            except Exception as e:
                log.warning("ticker fallback error: %s", e)

        if vol_detected > 0 and C.MIN_VOLUME_USDT > 0:
            symbols = [s for s in symbols if vol_map.get(s, 0) >= C.MIN_VOLUME_USDT]

        symbols.sort(key=lambda s: vol_map.get(s, 0), reverse=True)
        if C.TOP_N_SYMBOLS > 0:
            symbols = symbols[:C.TOP_N_SYMBOLS]

        log.info("get_all_symbols: %d símbolos (raw=%d, con_vol=%d)",
                 len(symbols), len(raw), vol_detected)
        return symbols

    async def get_klines(self, symbol: str, interval: str, limit: int = 200) -> list[list]:
        data = await self._get(
            "/openApi/swap/v3/quote/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
        )
        raw = data.get("data", [])
        if isinstance(raw, dict):
            raw = raw.get("klines", [])
        if not raw:
            return []

        result = []
        for c in raw:
            try:
                if isinstance(c, dict):
                    result.append([
                        int(c.get("time",   c.get("openTime", 0))),
                        float(c.get("open",  c.get("o", 0))),
                        float(c.get("high",  c.get("h", 0))),
                        float(c.get("low",   c.get("l", 0))),
                        float(c.get("close", c.get("c", 0))),
                        float(c.get("volume", c.get("v", 0))),
                    ])
                elif isinstance(c, (list, tuple)) and len(c) >= 6:
                    result.append([int(c[0]), float(c[1]), float(c[2]),
                                   float(c[3]), float(c[4]), float(c[5])])
            except Exception:
                continue
        return sorted(result, key=lambda x: x[0])

    async def get_ticker(self, symbol: str) -> dict:
        data = await self._get("/openApi/swap/v2/quote/ticker", {"symbol": symbol})
        raw = data.get("data", {})
        if isinstance(raw, list):
            return raw[0] if raw else {}
        return raw if isinstance(raw, dict) else {}

    async def get_order_book(self, symbol: str, limit: int = 10) -> dict:
        data = await self._get(
            "/openApi/swap/v2/quote/depth",
            {"symbol": symbol, "limit": limit},
        )
        raw = data.get("data", data)
        if isinstance(raw, dict):
            return {
                "bids": raw.get("bids", []),
                "asks": raw.get("asks", []),
            }
        return {"bids": [], "asks": []}

    async def get_funding_rate(self, symbol: str) -> float:
        try:
            data = await self._get(
                "/openApi/swap/v2/quote/fundingRate",
                {"symbol": symbol},
            )
            raw = data.get("data", {})
            if isinstance(raw, list):
                raw = raw[0] if raw else {}
            return float(raw.get("fundingRate", 0) or 0)
        except Exception:
            return 0.0

    # ── Cuenta ────────────────────────────────────────────────────────────────

    async def get_balance(self) -> float:
        data = await self._get(
            "/openApi/swap/v3/user/balance",
            {"currency": "USDT"},
            signed=True,
        )
        raw = data.get("data", {})

        def _extract(d: dict) -> float:
            avail  = float(d.get("availableMargin", 0) or 0)
            equity = float(d.get("equity",          0) or 0)
            if avail > 0:
                return avail
            if equity > 0:
                log.debug("availableMargin=0, usando equity=%.4f", equity)
                return equity
            return 0.0

        if isinstance(raw, list):
            for a in raw:
                if isinstance(a, dict) and a.get("asset", "") == "USDT":
                    return _extract(a)
            for a in raw:
                if isinstance(a, dict) and ("availableMargin" in a or "equity" in a):
                    return _extract(a)
            return 0.0

        if isinstance(raw, dict):
            bal = raw.get("balance", raw)
            if isinstance(bal, list):
                for a in bal:
                    if isinstance(a, dict) and a.get("asset", "") == "USDT":
                        return _extract(a)
            if isinstance(bal, dict):
                return _extract(bal)

        log.warning("get_balance: formato inesperado %s", str(data)[:200])
        return 0.0

    # ── Posiciones ────────────────────────────────────────────────────────────

    async def get_open_positions(self) -> list[dict]:
        data = await self._get("/openApi/swap/v2/user/positions", None, signed=True)
        positions = data.get("data", [])
        if not isinstance(positions, list):
            return []
        return [p for p in positions if float(p.get("positionAmt", 0) or 0) != 0]

    async def get_open_orders(self, symbol: str) -> list[dict]:
        data = await self._get(
            "/openApi/swap/v2/trade/openOrders",
            {"symbol": symbol},
            signed=True,
        )
        return data.get("data", {}).get("orders", [])

    # ── Apalancamiento ────────────────────────────────────────────────────────

    async def set_leverage(self, symbol: str, leverage: int, side: str = "LONG") -> bool:
        results = await asyncio.gather(
            self._post("/openApi/swap/v2/trade/leverage",
                       {"symbol": symbol, "side": "LONG", "leverage": leverage}),
            self._post("/openApi/swap/v2/trade/leverage",
                       {"symbol": symbol, "side": "SHORT", "leverage": leverage}),
            return_exceptions=True,
        )
        ok = True
        for s, r in zip(["LONG", "SHORT"], results):
            if isinstance(r, Exception):
                log.warning("[%s] set_leverage %s error: %s", symbol, s, r)
                ok = False
            elif r.get("code", -1) != 0:
                log.warning("[%s] set_leverage %s code=%s", symbol, s, r.get("code"))
        return ok

    # ── Órdenes ───────────────────────────────────────────────────────────────

    async def place_market_order(
        self,
        symbol:        str,
        side:          str,
        quantity:      float,
        position_side: str = "LONG",
    ) -> dict:
        qty = self._round_qty(symbol, quantity)
        if not self._check_min_qty(symbol, qty):
            log.warning("[%s] qty %.6f < min_qty — skip", symbol, qty)
            return {"code": -1, "msg": "qty_below_minimum"}
        params = {
            "symbol":       symbol,
            "side":         side,
            "positionSide": position_side,
            "type":         "MARKET",
            "quantity":     str(qty),
        }
        log.info("[%s] MARKET side=%s positionSide=%s qty=%s", symbol, side, position_side, qty)
        return await self._post("/openApi/swap/v2/trade/order", params)

    async def place_stop_market_order(
        self,
        symbol:         str,
        side:           str,
        quantity:       float,
        stop_price:     float,
        position_side:  str  = "LONG",
        close_position: bool = True,
        order_type:     str  = "STOP_MARKET",
    ) -> dict:
        """
        v6.5 FIX 109420: positionSide se resuelve vía cache (_resolve_ps).
        Si BingX rechaza por incompatibilidad de modo, se reintenta una vez
        con el valor correcto y se cachea para todas las llamadas futuras
        de ese símbolo — sin llamadas extra a get_open_positions.
        """
        qty = self._round_qty(symbol, quantity)
        ps  = self._resolve_ps(symbol, position_side)
        params = {
            "symbol":       symbol,
            "side":         side,
            "positionSide": ps,
            "type":         order_type,
            "stopPrice":    str(round(stop_price, 8)),
            "quantity":     str(qty),
            "workingType":  "MARK_PRICE",
            "priceProtect": "true",
        }
        log.debug("[%s] %s side=%s positionSide=%s stopPrice=%s qty=%s",
                  symbol, order_type, side, ps, stop_price, qty)
        resp = await self._post("/openApi/swap/v2/trade/order", params)

        if isinstance(resp, dict) and resp.get("code", -1) != 0:
            msg = str(resp.get("msg", ""))
            if self._handle_ps_error(msg, symbol, position_side, params):
                resp = await self._post("/openApi/swap/v2/trade/order", params)
            elif "stop loss price" in msg.lower() or "greater than" in msg.lower() or "less than" in msg.lower():
                log.error("[%s] SL/TP price inválido stop=%.6f dir=%s: %s",
                          symbol, stop_price, position_side, msg)

        if isinstance(resp, dict) and resp.get("code", -1) == 0:
            self.__class__._ps_cache[symbol] = params["positionSide"]

        return resp if isinstance(resp, dict) else {"code": -1, "msg": str(resp)}

    async def cancel_order(self, symbol: str, order_id: str) -> dict:
        return await self._delete(
            "/openApi/swap/v2/trade/order",
            {"symbol": symbol, "orderId": order_id},
        )

    async def cancel_all_orders(self, symbol: str) -> dict:
        return await self._delete(
            "/openApi/swap/v2/trade/allOpenOrders",
            {"symbol": symbol},
        )

    async def close_position_market(
        self,
        symbol:        str,
        quantity:      float,
        position_side: str,
    ) -> dict:
        """v6.5 FIX 109420: misma resolución de positionSide que place_stop_market_order."""
        side   = "SELL" if position_side == "LONG" else "BUY"
        qty    = self._round_qty(symbol, quantity)
        ps     = self._resolve_ps(symbol, position_side)
        params = {
            "symbol":       symbol,
            "side":         side,
            "positionSide": ps,
            "type":         "MARKET",
            "quantity":     str(qty),
        }
        log.info("[%s] CLOSE MARKET side=%s positionSide=%s qty=%s", symbol, side, ps, qty)
        resp = await self._post("/openApi/swap/v2/trade/order", params)

        if isinstance(resp, dict) and resp.get("code", -1) != 0:
            msg = str(resp.get("msg", ""))
            if self._handle_ps_error(msg, symbol, position_side, params):
                resp = await self._post("/openApi/swap/v2/trade/order", params)

        if isinstance(resp, dict) and resp.get("code", -1) == 0:
            self.__class__._ps_cache[symbol] = params["positionSide"]

        return resp if isinstance(resp, dict) else {"code": -1, "msg": str(resp)}

    # ── open_trade completo (entrada + SL + TP1 + TP2) ───────────────────────

    async def open_trade(
        self,
        symbol:    str,
        direction: str,
        quantity:  float,
        sl_price:  float,
        tp1_price: float,
        tp2_price: float,
    ) -> dict:
        side_entry = "BUY"  if direction == "LONG" else "SELL"
        side_close = "SELL" if direction == "LONG" else "BUY"
        results: dict = {}

        await self.set_leverage(symbol, C.LEVERAGE, direction)

        qty = self._round_qty(symbol, quantity)
        if not self._check_min_qty(symbol, qty):
            log.warning("[%s] qty %.6f < min → skip", symbol, qty)
            return {"entry": {"code": -1, "msg": "qty_below_minimum"}}

        entry_resp = await self.place_market_order(symbol, side_entry, qty, direction)
        results["entry"] = entry_resp
        if entry_resp.get("code", -1) != 0:
            log.error("[%s] Entrada fallida: %s", symbol, entry_resp)
            return results

        await asyncio.sleep(0.5)

        precision  = self._precision_map.get(symbol, 6)
        factor     = 10 ** precision
        qty_half   = math.floor(qty / 2 * factor) / factor
        qty_remain = math.floor((qty - qty_half) * factor) / factor

        # ── SL — con qty real ────────────────────────────────────────────────
        sl_resp = await self.place_stop_market_order(
            symbol, side_close, qty, sl_price, direction, order_type="STOP_MARKET",
        )
        results["sl"] = sl_resp
        if sl_resp.get("code", -1) == 0:
            log.info("[%s] SL OK @ %.6f qty=%.6f", symbol, sl_price, qty)
        else:
            log.error("[%s] SL FALLIDO: %s", symbol, sl_resp)
            qty_safe = self._safe_qty_for_sl(symbol, qty)
            if qty_safe != qty and qty_safe > 0:
                log.info("[%s] SL retry con qty_safe=%.6f", symbol, qty_safe)
                sl_resp2 = await self.place_stop_market_order(
                    symbol, side_close, qty_safe, sl_price, direction, order_type="STOP_MARKET",
                )
                results["sl"] = sl_resp2
                if sl_resp2.get("code", -1) == 0:
                    log.info("[%s] SL OK (retry) @ %.6f qty=%.6f", symbol, sl_price, qty_safe)
                else:
                    log.error("[%s] SL FALLIDO también en retry: %s", symbol, sl_resp2)

        # ── TP1 ───────────────────────────────────────────────────────────────
        if qty_half > 0:
            tp1_resp = await self.place_stop_market_order(
                symbol, side_close, qty_half, tp1_price, direction, order_type="TAKE_PROFIT_MARKET",
            )
            results["tp1"] = tp1_resp
            if tp1_resp.get("code", -1) == 0:
                log.info("[%s] TP1 OK @ %.6f qty=%.6f", symbol, tp1_price, qty_half)
            else:
                log.error("[%s] TP1 FALLIDO: %s", symbol, tp1_resp)

        # ── TP2 ───────────────────────────────────────────────────────────────
        if qty_remain > 0:
            tp2_resp = await self.place_stop_market_order(
                symbol, side_close, qty_remain, tp2_price, direction, order_type="TAKE_PROFIT_MARKET",
            )
            results["tp2"] = tp2_resp
            if tp2_resp.get("code", -1) == 0:
                log.info("[%s] TP2 OK @ %.6f qty=%.6f", symbol, tp2_price, qty_remain)
            else:
                log.error("[%s] TP2 FALLIDO: %s", symbol, tp2_resp)

        return results
