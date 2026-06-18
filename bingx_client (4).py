"""
QF×JP Bot v6.5.4 — BingX Client
FIXES críticos:
  - HEDGE mode: positionSide = "LONG" o "SHORT" según dirección (nunca "BOTH")
  - place_market_order: acepta position_side y lo envía a BingX
  - place_stop_market_order: usa position_side real (antes ignorado)
  - close_position_market: usa position_side real (antes ignorado)
  - open_trade: pasa direction como position_side a todas las órdenes
  - SL breakeven en position_manager ya pasa trade.direction → compatible
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


class BingXClient:
    BASE = C.BINGX_BASE_URL

    def __init__(self):
        self._session:       Optional[aiohttp.ClientSession] = None
        self._precision_map: dict[str, int]   = {}
        self._min_qty_map:   dict[str, float] = {}
        log.info("BingXClient v6.5.4 — HEDGE mode: positionSide=LONG/SHORT")

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

    def _round_qty(self, symbol: str, qty: float) -> float:
        precision = self._precision_map.get(symbol, 6)
        if precision == 0:
            return float(math.floor(qty))
        factor = 10 ** precision
        return math.floor(qty * factor) / factor

    def _check_min_qty(self, symbol: str, qty: float) -> bool:
        min_q = self._min_qty_map.get(symbol, 0.0)
        return qty >= min_q if min_q > 0 else True

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
            return {"bids": raw.get("bids", []), "asks": raw.get("asks", [])}
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
        return 0.0

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
        data = await self._post(
            "/openApi/swap/v2/trade/leverage",
            {"symbol": symbol, "side": side, "leverage": leverage},
        )
        ok = data.get("code", -1) == 0
        if not ok:
            log.warning("[%s] set_leverage code=%s — continuando", symbol, data.get("code"))
        return ok

    # ── Órdenes — HEDGE mode: positionSide = LONG o SHORT ────────────────────

    async def place_market_order(
        self,
        symbol:        str,
        side:          str,
        quantity:      float,
        position_side: str = "LONG",   # HEDGE: LONG para abrir/cerrar longs, SHORT para shorts
    ) -> dict:
        """
        HEDGE mode: positionSide debe ser LONG o SHORT según la posición.
          - Abrir LONG:  side=BUY,  positionSide=LONG
          - Abrir SHORT: side=SELL, positionSide=SHORT
          - Cerrar LONG: side=SELL, positionSide=LONG
          - Cerrar SHORT:side=BUY,  positionSide=SHORT
        """
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
        log.info("[%s] MARKET order params: %s", symbol, params)
        return await self._post("/openApi/swap/v2/trade/order", params)

    async def place_stop_market_order(
        self,
        symbol:         str,
        side:           str,
        quantity:       float,
        stop_price:     float,
        position_side:  str  = "LONG",   # HEDGE: dirección de la posición (LONG o SHORT)
        close_position: bool = True,
        order_type:     str  = "STOP_MARKET",
    ) -> dict:
        """
        HEDGE mode:
        - positionSide = dirección de la posición que se cierra (LONG o SHORT)
        - quantity REAL nunca "0"
        - closePosition solo para SL (STOP_MARKET)
        - TP usa quantity real sin closePosition
        position_manager.py ya pasa trade.direction → compatible sin cambios.
        """
        qty = self._round_qty(symbol, quantity)
        if qty <= 0:
            log.warning("[%s] place_stop_market qty=0 skip", symbol)
            return {"code": -1, "msg": "qty_zero"}

        # Precio redondeado a 8 decimales
        sp = round(stop_price, 8)

        if order_type == "STOP_MARKET":
            # SL: closePosition=true + positionSide real + quantity real
            params = {
                "symbol":        symbol,
                "side":          side,
                "positionSide":  position_side,
                "type":          "STOP_MARKET",
                "stopPrice":     str(sp),
                "quantity":      str(qty),
                "closePosition": "true",
                "workingType":   "MARK_PRICE",
                "priceProtect":  "true",
            }
        else:
            # TP: quantity real + positionSide real
            params = {
                "symbol":       symbol,
                "side":         side,
                "positionSide": position_side,
                "type":         "TAKE_PROFIT_MARKET",
                "stopPrice":    str(sp),
                "quantity":     str(qty),
                "workingType":  "MARK_PRICE",
                "priceProtect": "true",
            }

        log.info("[%s] %s params: %s", symbol, order_type, params)
        resp = await self._post("/openApi/swap/v2/trade/order", params)
        if resp.get("code", -1) != 0:
            log.warning("[%s] %s fallo: %s", symbol, order_type, resp)
        return resp

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
        position_side: str,   # HEDGE: LONG o SHORT según la posición a cerrar
    ) -> dict:
        """HEDGE mode: positionSide = dirección de la posición que se cierra."""
        side = "SELL" if position_side == "LONG" else "BUY"
        qty  = self._round_qty(symbol, quantity)
        params = {
            "symbol":       symbol,
            "side":         side,
            "positionSide": position_side,
            "type":         "MARKET",
            "quantity":     str(qty),
        }
        log.info("[%s] close_market params: %s", symbol, params)
        return await self._post("/openApi/swap/v2/trade/order", params)

    # ── open_trade: entrada + SL + TP1 + TP2 ─────────────────────────────────

    async def open_trade(
        self,
        symbol:    str,
        direction: str,
        quantity:  float,
        sl_price:  float,
        tp1_price: float,
        tp2_price: float,
    ) -> dict:
        """
        HEDGE mode v6.5.4:
        - Entrada:  side=BUY/SELL,  positionSide=direction (LONG/SHORT)
        - SL/TP:    side=SELL/BUY,  positionSide=direction (misma posición)
        - Si SL falla → cierre de emergencia con positionSide correcto
        """
        side_entry = "BUY"  if direction == "LONG" else "SELL"
        side_close = "SELL" if direction == "LONG" else "BUY"
        results: dict = {}

        # Apalancamiento (fallo no crítico)
        await self.set_leverage(symbol, C.LEVERAGE, direction)

        qty = self._round_qty(symbol, quantity)
        if not self._check_min_qty(symbol, qty):
            log.warning("[%s] qty %.6f < min → skip", symbol, qty)
            return {"entry": {"code": -1, "msg": "qty_below_minimum"}}

        # 1. Orden de entrada — positionSide = dirección
        entry_resp = await self.place_market_order(
            symbol, side_entry, qty, position_side=direction
        )
        results["entry"] = entry_resp
        if entry_resp.get("code", -1) != 0:
            log.error("[%s] Entrada fallida: %s", symbol, entry_resp)
            return results

        # Esperar confirmación de BingX
        await asyncio.sleep(0.8)

        # 2. Verificar posición abierta
        try:
            positions = await self.get_open_positions()
            pos_map = {p.get("symbol", ""): p for p in positions}
            if symbol not in pos_map:
                log.warning("[%s] Posición no confirmada tras entrada", symbol)
        except Exception as e:
            log.warning("[%s] No pude verificar posición: %s", symbol, e)

        # 3. Qty para TP (mitad cada uno)
        qty_half = self._round_qty(symbol, qty / 2)
        if qty_half <= 0:
            qty_half = qty

        # 4. SL + TP1 + TP2 en paralelo — todos con positionSide=direction
        sl_task  = self.place_stop_market_order(
            symbol, side_close, qty, sl_price,
            position_side=direction,
            order_type="STOP_MARKET", close_position=True,
        )
        tp1_task = self.place_stop_market_order(
            symbol, side_close, qty_half, tp1_price,
            position_side=direction,
            order_type="TAKE_PROFIT_MARKET", close_position=False,
        )
        tp2_task = self.place_stop_market_order(
            symbol, side_close, qty_half, tp2_price,
            position_side=direction,
            order_type="TAKE_PROFIT_MARKET", close_position=False,
        )

        sl_r, tp1_r, tp2_r = await asyncio.gather(
            sl_task, tp1_task, tp2_task, return_exceptions=True
        )

        results["sl"]  = sl_r  if isinstance(sl_r,  dict) else {"code": -1, "msg": str(sl_r)}
        results["tp1"] = tp1_r if isinstance(tp1_r, dict) else {"code": -1, "msg": str(tp1_r)}
        results["tp2"] = tp2_r if isinstance(tp2_r, dict) else {"code": -1, "msg": str(tp2_r)}

        # Log resultado SL/TP
        sl_ok  = results["sl"].get("code",  -1) == 0
        tp1_ok = results["tp1"].get("code", -1) == 0
        tp2_ok = results["tp2"].get("code", -1) == 0
        log.info("[%s] SL=%s TP1=%s TP2=%s",
                 symbol,
                 "✓" if sl_ok  else f"✗{results['sl'].get('code')}",
                 "✓" if tp1_ok else f"✗{results['tp1'].get('code')}",
                 "✓" if tp2_ok else f"✗{results['tp2'].get('code')}")

        # Si SL falló → cerrar posición inmediatamente (anti-liquidación)
        if not sl_ok:
            log.error("[%s] SL FALLÓ — cerrando posición por seguridad", symbol)
            await asyncio.sleep(0.3)
            await self.close_position_market(symbol, qty, direction)
            results["entry"] = {"code": -1, "msg": "sl_failed_position_closed"}

        return results
