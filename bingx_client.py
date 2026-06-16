"""
QF×JP Bot v6.6 — BingX Client COMPLETO
Fixes:
  - positionSide auto-detección (Hedge=LONG/SHORT, One-Way=BOTH)
  - Fallback inteligente por mensaje de error BingX
  - SL/TP sin reduceOnly (Hedge Mode)
  - qty split correcto para TP1/TP2
"""
import asyncio
import hashlib
import hmac
import logging
import math
import time
from urllib.parse import urlencode

import aiohttp

import config as C

log = logging.getLogger("bingx")


class BingXClient:
    def __init__(self):
        self._session       = None
        self._precision_map: dict[str, int]   = {}
        self._min_qty_map:   dict[str, float] = {}
        self._step_map:      dict[str, float] = {}

    async def _get_session(self):
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            )
        return self._session

    async def close(self):
        if self._session:
            await self._session.close()

    # ── Auth ──────────────────────────────────────────────────────────────────

    def _sign(self, params: dict) -> str:
        qs = urlencode(sorted(params.items()))
        return hmac.new(
            C.BINGX_SECRET_KEY.encode(),
            qs.encode(),
            hashlib.sha256,
        ).hexdigest()

    async def _get(self, path: str, params: dict = None) -> dict:
        params = params or {}
        params["timestamp"]  = int(time.time() * 1000)
        params["recvWindow"] = 10000
        params["signature"]  = self._sign(params)
        url = C.BINGX_BASE_URL + path
        s   = await self._get_session()
        async with s.get(url, params=params,
                         headers={"X-BX-APIKEY": C.BINGX_API_KEY}) as r:
            return await r.json()

    async def _post(self, path: str, params: dict) -> dict:
        params["timestamp"]  = int(time.time() * 1000)
        params["recvWindow"] = 10000
        params["signature"]  = self._sign(params)
        url = C.BINGX_BASE_URL + path
        s   = await self._get_session()
        async with s.post(url, params=params,
                          headers={"X-BX-APIKEY": C.BINGX_API_KEY}) as r:
            return await r.json()

    # ── Precisión ─────────────────────────────────────────────────────────────

    def _round_qty(self, symbol: str, qty: float) -> float:
        step = self._step_map.get(symbol, 0)
        if step > 0:
            qty = math.floor(qty / step) * step
            precision = max(0, round(-math.log10(step)))
            qty = round(qty, precision)
        else:
            precision = self._precision_map.get(symbol, 4)
            qty = round(qty, precision)
        min_qty = self._min_qty_map.get(symbol, 0)
        return max(qty, min_qty) if qty > 0 else 0.0

    # ── Symbols ───────────────────────────────────────────────────────────────

    async def get_all_symbols(self) -> list[str]:
        try:
            r = await self._get("/openApi/swap/v2/quote/contracts")
            contracts = r.get("data", [])
            if not contracts:
                log.info("contracts sin volumen → enriqueciendo con /ticker")
                r2   = await self._get("/openApi/swap/v2/quote/ticker")
                data = r2.get("data", [])
                syms = []
                for t in data:
                    sym = t.get("symbol", "")
                    vol = float(t.get("quoteVolume", 0) or 0)
                    if sym.endswith("-USDT") and vol >= C.MIN_VOLUME_USDT:
                        syms.append((sym, vol))
                syms.sort(key=lambda x: x[1], reverse=True)
                result = [s[0] for s in syms]
                log.info("get_all_symbols: %d símbolos (raw=%d, con_vol=%d)",
                         len(result), len(data), len(result))
                return result[:C.TOP_N_SYMBOLS] if C.TOP_N_SYMBOLS else result

            # Enriquecer con volumen via ticker
            r2     = await self._get("/openApi/swap/v2/quote/ticker")
            vol_map = {t["symbol"]: float(t.get("quoteVolume", 0) or 0)
                       for t in r2.get("data", []) if "symbol" in t}

            result = []
            for c in contracts:
                sym      = c.get("symbol", "")
                vol      = vol_map.get(sym, 0)
                min_qty  = float(c.get("minOrderQty", c.get("minQty", 0)) or 0)
                qty_step = float(c.get("qtyStep", c.get("stepSize", 0)) or 0)
                prec     = int(c.get("quantityPrecision", 4))

                if not sym.endswith("-USDT"):
                    continue
                if sym in C.BLACKLIST:
                    continue
                if vol < C.MIN_VOLUME_USDT:
                    continue

                self._precision_map[sym] = prec
                self._min_qty_map[sym]   = min_qty
                self._step_map[sym]      = qty_step
                result.append((sym, vol))

            result.sort(key=lambda x: x[1], reverse=True)
            symbols = [s[0] for s in result]
            log.info("get_all_symbols: %d símbolos (raw=%d, con_vol=%d)",
                     len(symbols), len(contracts), len(symbols))
            return symbols[:C.TOP_N_SYMBOLS] if C.TOP_N_SYMBOLS else symbols
        except Exception as e:
            log.error("get_all_symbols error: %s", e)
            return []

    # ── Market data ───────────────────────────────────────────────────────────

    async def get_klines(self, symbol: str, interval: str, limit: int = 200) -> list:
        try:
            r = await self._get("/openApi/swap/v3/quote/klines", {
                "symbol": symbol, "interval": interval, "limit": limit,
            })
            data = r.get("data", [])
            result = []
            for k in data:
                try:
                    result.append([
                        float(k.get("time", k.get("t", 0))),
                        float(k.get("open",  k.get("o", 0))),
                        float(k.get("high",  k.get("h", 0))),
                        float(k.get("low",   k.get("l", 0))),
                        float(k.get("close", k.get("c", 0))),
                        float(k.get("volume", k.get("v", 0))),
                    ])
                except Exception:
                    pass
            return result
        except Exception as e:
            log.debug("[%s] get_klines error: %s", symbol, e)
            return []

    async def get_ticker(self, symbol: str) -> dict:
        try:
            r = await self._get("/openApi/swap/v2/quote/ticker", {"symbol": symbol})
            data = r.get("data", {})
            if isinstance(data, list):
                data = data[0] if data else {}
            return data
        except Exception as e:
            log.debug("[%s] get_ticker error: %s", symbol, e)
            return {}

    async def get_order_book(self, symbol: str, limit: int = 20) -> dict:
        try:
            r = await self._get("/openApi/swap/v2/quote/depth", {
                "symbol": symbol, "limit": limit,
            })
            return r.get("data", {})
        except Exception:
            return {}

    async def get_funding_rate(self, symbol: str) -> float:
        try:
            r = await self._get("/openApi/swap/v2/quote/fundingRate", {"symbol": symbol})
            data = r.get("data", {})
            if isinstance(data, list):
                data = data[0] if data else {}
            return float(data.get("fundingRate", 0) or 0)
        except Exception:
            return 0.0

    # ── Account ───────────────────────────────────────────────────────────────

    async def get_balance(self) -> float:
        try:
            r    = await self._get("/openApi/swap/v2/user/balance")
            data = r.get("data", {})
            bal  = data.get("balance", {})
            return float(bal.get("availableMargin", bal.get("equity", 0)) or 0)
        except Exception as e:
            log.warning("get_balance error: %s", e)
            return 0.0

    async def get_open_positions(self) -> list:
        try:
            r = await self._get("/openApi/swap/v2/user/positions")
            return r.get("data", []) or []
        except Exception as e:
            log.warning("get_open_positions error: %s", e)
            return []

    async def cancel_all_orders(self, symbol: str) -> dict:
        try:
            return await self._post("/openApi/swap/v2/trade/allOpenOrders",
                                    {"symbol": symbol})
        except Exception as e:
            log.debug("[%s] cancel_all_orders: %s", symbol, e)
            return {"code": -1}

    # ── positionSide auto-detección ───────────────────────────────────────────

    async def _get_real_position_side(self, symbol: str, direction: str) -> str:
        """
        Lee positionSide real de BingX para el símbolo.
        Hedge Mode  → LONG o SHORT
        One-Way     → BOTH
        Si no encuentra la posición → usa direction (correcto para Hedge)
        """
        try:
            positions = await self.get_open_positions()
            for p in positions:
                if p.get("symbol") != symbol:
                    continue
                ps = p.get("positionSide", "")
                if ps in ("LONG", "SHORT", "BOTH"):
                    log.debug("[%s] positionSide real: %s", symbol, ps)
                    return ps
        except Exception as e:
            log.debug("[%s] _get_real_position_side error: %s", symbol, e)
        # Fallback: direction = LONG/SHORT (correcto para Hedge Mode)
        return direction

    def _parse_bingx_error(self, resp: dict) -> str:
        """Extrae mensaje de error de respuesta BingX."""
        if not isinstance(resp, dict):
            return ""
        return str(resp.get("msg", resp.get("message", ""))).lower()

    # ── Orders ────────────────────────────────────────────────────────────────

    async def place_stop_market_order(
        self,
        symbol:        str,
        side:          str,
        quantity:      float,
        stop_price:    float,
        direction:     str = "LONG",
        order_type:    str = "STOP_MARKET",
    ) -> dict:
        qty     = self._round_qty(symbol, quantity)
        real_ps = await self._get_real_position_side(symbol, direction)

        params = {
            "symbol":       symbol,
            "side":         side,
            "positionSide": real_ps,
            "type":         order_type,
            "stopPrice":    str(round(stop_price, 8)),
            "quantity":     str(qty),
            "workingType":  "MARK_PRICE",
            "priceProtect": "true",
        }
        log.debug("[%s] %s side=%s ps=%s stop=%.6f qty=%s",
                  symbol, order_type, side, real_ps, stop_price, qty)

        resp = await self._post("/openApi/swap/v2/trade/order", params)

        if isinstance(resp, dict) and resp.get("code", -1) != 0:
            msg = self._parse_bingx_error(resp)
            if "positionside" in msg or "position side" in msg:
                # Hedge Mode confirmado → forzar LONG/SHORT
                log.warning("[%s] Hedge mode → forzando positionSide=%s", symbol, direction)
                params["positionSide"] = direction
                resp = await self._post("/openApi/swap/v2/trade/order", params)
            elif "position not exist" in msg and real_ps != "BOTH":
                # One-Way mode → probar BOTH
                log.warning("[%s] position not exist → probando BOTH", symbol)
                params["positionSide"] = "BOTH"
                resp = await self._post("/openApi/swap/v2/trade/order", params)
            elif "stop loss price" in msg or "greater than" in msg or "less than" in msg:
                log.error("[%s] SL price inválido stop=%.6f: %s", symbol, stop_price, msg)

        return resp if isinstance(resp, dict) else {"code": -1, "msg": str(resp)}

    async def close_position_market(self, symbol: str, quantity: float,
                                     direction: str) -> dict:
        side    = "SELL" if direction == "LONG" else "BUY"
        qty     = self._round_qty(symbol, quantity)
        real_ps = await self._get_real_position_side(symbol, direction)

        params = {
            "symbol":       symbol,
            "side":         side,
            "positionSide": real_ps,
            "type":         "MARKET",
            "quantity":     str(qty),
        }
        log.info("[%s] CLOSE MARKET ps=%s qty=%s", symbol, real_ps, qty)
        resp = await self._post("/openApi/swap/v2/trade/order", params)

        if isinstance(resp, dict) and resp.get("code", -1) != 0:
            msg = self._parse_bingx_error(resp)
            if "positionside" in msg or "position side" in msg:
                params["positionSide"] = direction
                resp = await self._post("/openApi/swap/v2/trade/order", params)
            elif "position not exist" in msg and real_ps != "BOTH":
                params["positionSide"] = "BOTH"
                resp = await self._post("/openApi/swap/v2/trade/order", params)

        return resp if isinstance(resp, dict) else {"code": -1}

    async def open_trade(self, symbol: str, direction: str, quantity: float,
                          sl_price: float, tp1_price: float, tp2_price: float) -> dict:
        """
        Abre posición + SL + TP1 (50%) + TP2 (50%).
        Verifica SL y loguea resultado de cada orden.
        """
        qty       = self._round_qty(symbol, quantity)
        side_open = "BUY" if direction == "LONG" else "SELL"
        side_cls  = "SELL" if direction == "LONG" else "BUY"

        # Para nuevas posiciones: usar direction directamente
        # (la posición aún no existe en BingX, no podemos leerla)
        position_side = direction

        results = {}

        # ── Entrada a mercado ─────────────────────────────────────────────────
        entry_params = {
            "symbol":       symbol,
            "side":         side_open,
            "positionSide": position_side,
            "type":         "MARKET",
            "quantity":     str(qty),
        }
        log.info("[%s] MARKET %s ps=%s qty=%s", symbol, side_open, position_side, qty)
        entry_resp = await self._post("/openApi/swap/v2/trade/order", entry_params)
        results["entry"] = entry_resp

        if entry_resp.get("code", -1) != 0:
            return results

        await asyncio.sleep(0.6)

        # ── Split qty: TP1=50%, TP2=50% ───────────────────────────────────────
        precision  = max(0, round(-math.log10(self._step_map.get(symbol, 0.0001) or 0.0001)))
        factor     = 10 ** precision
        qty_half   = math.floor(qty / 2 * factor) / factor
        qty_remain = math.floor((qty - qty_half) * factor) / factor

        # ── SL ────────────────────────────────────────────────────────────────
        sl_resp = await self.place_stop_market_order(
            symbol, side_cls, qty, sl_price, direction, "STOP_MARKET",
        )
        results["sl"] = sl_resp
        if sl_resp.get("code", -1) == 0:
            log.info("[%s] SL OK @ %.6f", symbol, sl_price)
        else:
            log.error("[%s] SL FALLIDO: %s", symbol, sl_resp)

        # ── TP1 ───────────────────────────────────────────────────────────────
        if qty_half > 0:
            tp1_resp = await self.place_stop_market_order(
                symbol, side_cls, qty_half, tp1_price, direction, "TAKE_PROFIT_MARKET",
            )
            results["tp1"] = tp1_resp
            if tp1_resp.get("code", -1) == 0:
                log.info("[%s] TP1 OK @ %.6f", symbol, tp1_price)
            else:
                log.error("[%s] TP1 FALLIDO: %s", symbol, tp1_resp)

        # ── TP2 ───────────────────────────────────────────────────────────────
        if qty_remain > 0:
            tp2_resp = await self.place_stop_market_order(
                symbol, side_cls, qty_remain, tp2_price, direction, "TAKE_PROFIT_MARKET",
            )
            results["tp2"] = tp2_resp
            if tp2_resp.get("code", -1) == 0:
                log.info("[%s] TP2 OK @ %.6f", symbol, tp2_price)
            else:
                log.error("[%s] TP2 FALLIDO: %s", symbol, tp2_resp)

        return results
