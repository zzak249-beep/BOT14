"""
QF×JP Bot v7.3 — BingX Client — FUSIÓN v6.4 + v7.2
═══════════════════════════════════════════════════════════════════════════════
Base: v7.2 (mantiene todos sus fixes). Se añade de v6.4 lo que v7.2 había perdido:

  FUSIÓN 1 — Retry HTTP (3 intentos, backoff exponencial):
    v7.2 hacía una sola llamada aiohttp sin reintento — cualquier timeout
    o error de red transitorio tumbaba la petición sin recuperación.
    Restaurado el patrón de v6.4: 3 intentos con sleep 1.5**attempt.

  FUSIÓN 2 — cancel_order con verbo DELETE real:
    v7.2 usaba self._post(...) contra un endpoint que BingX espera en
    DELETE, con un segundo intento adivinando otro endpoint — frágil.
    Restaurado _delete() de v6.4 (HTTP DELETE real) y cancel_order /
    cancel_all_orders lo usan correctamente, sin adivinar endpoints.

  FUSIÓN 3 — set_leverage() restaurado (NO estaba en la tabla del usuario,
    hallazgo adicional durante la fusión):
    v7.2 eliminó set_leverage() y open_trade() ya no lo llamaba. Esto
    significa que las posiciones se abrían con el leverage que hubiera
    quedado puesto manualmente en BingX, no con C.LEVERAGE — afecta
    directamente los cálculos de MAX_NOTIONAL_USDT y Kelly sizing.
    Restaurado de v6.4 y reconectado en open_trade().

  MANTENIDO DE v7.2 (no se toca):
    - .strip() en API key y secret key (fix error 100001)
    - positionSide auto-detección Hedge/One-Way vía _get_real_position_side
    - qty REAL ejecutada extraída de la respuesta de entrada (fix 110424)
    - sleep 1.2s post-entrada antes de colocar SL/TP
    - TOP_N_SYMBOLS solo aplica slice si > 0
    - _round_qty / _safe_qty_for_sl con stepSize y precision
═══════════════════════════════════════════════════════════════════════════════
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
        log.info("BingXClient v7.3 iniciado (fusión v6.4 retry/DELETE/leverage + v7.2 strip/hedge/qty_real)")

    async def _get_session(self):
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            )
        return self._session

    async def close(self):
        if self._session:
            await self._session.close()

    # ── Auth (de v7.2 — strip() fix error 100001) ─────────────────────────────

    def _sign(self, params: dict) -> str:
        qs  = urlencode(sorted(params.items()))
        key = C.BINGX_SECRET_KEY.strip()   # FIX v7.2: strip() evita 100001
        return hmac.new(key.encode(), qs.encode(), hashlib.sha256).hexdigest()

    def _api_key(self) -> str:
        return C.BINGX_API_KEY.strip()     # FIX v7.2: strip() también en header

    # ── HTTP con retry (restaurado de v6.4) ───────────────────────────────────

    async def _get(self, path: str, params: dict = None) -> dict:
        params = dict(params or {})
        params["timestamp"]  = int(time.time() * 1000)
        params["recvWindow"] = 10000
        params["signature"]  = self._sign(params)
        url = C.BINGX_BASE_URL + path
        s   = await self._get_session()
        for attempt in range(3):
            try:
                async with s.get(url, params=params,
                                 headers={"X-BX-APIKEY": self._api_key()}) as r:
                    return await r.json()
            except Exception as e:
                if attempt == 2:
                    log.error("GET %s error: %s", path, e)
                    return {"code": -1, "msg": str(e)}
                await asyncio.sleep(1.5 ** attempt)
        return {"code": -1, "msg": "retry_exhausted"}

    async def _post(self, path: str, params: dict) -> dict:
        params = dict(params)
        params["timestamp"]  = int(time.time() * 1000)
        params["recvWindow"] = 10000
        params["signature"]  = self._sign(params)
        url = C.BINGX_BASE_URL + path
        s   = await self._get_session()
        for attempt in range(3):
            try:
                async with s.post(url, params=params,
                                  headers={"X-BX-APIKEY": self._api_key()}) as r:
                    return await r.json()
            except Exception as e:
                if attempt == 2:
                    log.error("POST %s error: %s", path, e)
                    return {"code": -1, "msg": str(e)}
                await asyncio.sleep(1.5 ** attempt)
        return {"code": -1, "msg": "retry_exhausted"}

    async def _delete(self, path: str, params: dict) -> dict:
        """
        FUSIÓN: restaurado de v6.4. BingX espera verbo DELETE real para
        cancelar órdenes — v7.2 lo simulaba con POST y un segundo intento
        a ciegas contra otro endpoint. Con HTTP DELETE real no hace falta.
        """
        params = dict(params)
        params["timestamp"]  = int(time.time() * 1000)
        params["recvWindow"] = 10000
        params["signature"]  = self._sign(params)
        url = C.BINGX_BASE_URL + path
        s   = await self._get_session()
        for attempt in range(3):
            try:
                async with s.delete(url, params=params,
                                    headers={"X-BX-APIKEY": self._api_key()}) as r:
                    return await r.json()
            except Exception as e:
                if attempt == 2:
                    log.error("DELETE %s error: %s", path, e)
                    return {"code": -1, "msg": str(e)}
                await asyncio.sleep(1.5 ** attempt)
        return {"code": -1, "msg": "retry_exhausted"}

    # ── Precisión (de v7.2) ────────────────────────────────────────────────────

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

    def _safe_qty_for_sl(self, symbol: str, qty: float) -> float:
        """FIX v7.2 (110424): qty segura ≤ qty ejecutada real por BingX."""
        step = self._step_map.get(symbol, 0)
        if step > 0:
            qty = math.floor(qty / step) * step
            precision = max(0, round(-math.log10(step)))
            qty = round(qty, precision)
        else:
            precision = self._precision_map.get(symbol, 4)
            qty = round(qty * 0.9999, precision)
        min_qty = self._min_qty_map.get(symbol, 0)
        return max(qty, min_qty) if qty > 0 else 0.0

    def _extract_executed_qty(self, entry_resp: dict, fallback_qty: float) -> float:
        """FIX v7.2 (110424): extrae qty REAL ejecutada de la respuesta de entrada."""
        try:
            data  = entry_resp.get("data", {})
            order = data.get("order", data)
            for field in ("executedQty", "origQty", "quantity"):
                val = order.get(field, "")
                if val and str(val) not in ("", "0", "0.0"):
                    extracted = float(val)
                    if extracted > 0:
                        log.debug("qty_real de entrada: %s=%s", field, val)
                        return extracted
        except Exception as e:
            log.debug("_extract_executed_qty error: %s", e)
        return self._safe_qty_for_sl("", fallback_qty)

    # ── Symbols (de v7.2 — slice solo si TOP_N_SYMBOLS > 0) ───────────────────

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
                return result[:C.TOP_N_SYMBOLS] if C.TOP_N_SYMBOLS > 0 else result

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
            return symbols[:C.TOP_N_SYMBOLS] if C.TOP_N_SYMBOLS > 0 else symbols
        except Exception as e:
            log.error("get_all_symbols error: %s", e)
            return []

    # ── Market data (de v7.2) ──────────────────────────────────────────────────

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

    # ── Account (de v7.2) ───────────────────────────────────────────────────────

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

    # ── Cancelación de órdenes (FUSIÓN: DELETE real de v6.4) ──────────────────

    async def cancel_all_orders(self, symbol: str) -> dict:
        """FUSIÓN: usa _delete (HTTP DELETE real), no POST como en v7.2."""
        return await self._delete("/openApi/swap/v2/trade/allOpenOrders",
                                  {"symbol": symbol})

    async def cancel_order(self, symbol: str, order_id: str) -> dict:
        """
        FUSIÓN: restaurado de v6.4 — HTTP DELETE real contra el endpoint
        correcto, sin el hack de v7.2 (POST + segundo intento a ciegas
        contra otro endpoint distinto).
        """
        return await self._delete("/openApi/swap/v2/trade/order",
                                  {"symbol": symbol, "orderId": order_id})

    # ── Apalancamiento (FUSIÓN: restaurado de v6.4, v7.2 lo había perdido) ────

    async def set_leverage(self, symbol: str, leverage: int, side: str = "LONG") -> bool:
        """
        FUSIÓN — hallazgo adicional: v7.2 eliminó este método y open_trade()
        ya no lo llamaba, por lo que las posiciones se abrían con el
        leverage que hubiera quedado puesto manualmente en BingX en vez
        de C.LEVERAGE. Afecta directamente MAX_NOTIONAL_USDT y Kelly sizing.
        Llama LONG y SHORT en paralelo: soporta tanto Hedge mode (donde es
        obligatorio fijar cada lado por separado) como One-Way mode (donde
        BingX acepta ambas llamadas sin error).
        """
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
            elif isinstance(r, dict) and r.get("code", -1) != 0:
                log.warning("[%s] set_leverage %s code=%s", symbol, s, r.get("code"))
        return ok

    # ── positionSide auto-detección (de v7.2) ──────────────────────────────────

    async def _get_real_position_side(self, symbol: str, direction: str) -> str:
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
        return direction

    def _parse_bingx_error(self, resp: dict) -> str:
        if not isinstance(resp, dict):
            return ""
        return str(resp.get("msg", resp.get("message", ""))).lower()

    # ── Orders (de v7.2) ─────────────────────────────────────────────────────

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
                log.warning("[%s] Hedge mode → forzando positionSide=%s", symbol, direction)
                params["positionSide"] = direction
                resp = await self._post("/openApi/swap/v2/trade/order", params)
            elif "position not exist" in msg and real_ps != "BOTH":
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

        FUSIÓN: se reincorpora la llamada a set_leverage() antes de la
        entrada (existía en v6.4, v7.2 la había perdido por completo).

        De v7.2 se mantiene:
          - qty REAL ejecutada extraída de la respuesta de entrada (110424)
          - sleep 1.2s post-entrada antes de SL/TP
        """
        qty       = self._round_qty(symbol, quantity)
        side_open = "BUY" if direction == "LONG" else "SELL"
        side_cls  = "SELL" if direction == "LONG" else "BUY"
        position_side = direction

        results = {}

        # FUSIÓN: restaurado — fijar leverage antes de abrir
        await self.set_leverage(symbol, C.LEVERAGE, direction)

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

        # FIX v7.2 (110424): extraer qty REAL ejecutada por BingX
        real_qty = self._extract_executed_qty(entry_resp, qty)
        if abs(real_qty - qty) > qty * 0.001:
            log.info("[%s] qty ajustada: calculada=%.6f real_BingX=%.6f",
                     symbol, qty, real_qty)
        qty = real_qty

        # FIX v7.2: sleep 1.2s para que BingX registre la posición
        await asyncio.sleep(1.2)

        # ── Split qty: TP1=50%, TP2=50% ───────────────────────────────────────
        step = self._step_map.get(symbol, 0)
        if step > 0:
            precision = max(0, round(-math.log10(step)))
        else:
            precision = self._precision_map.get(symbol, 4)
        factor     = 10 ** precision
        qty_half   = math.floor(qty / 2 * factor) / factor
        qty_remain = math.floor((qty - qty_half) * factor) / factor

        if qty_half + qty_remain > qty:
            qty_remain = math.floor((qty - qty_half) * factor) / factor

        # ── SL — con qty real de BingX ────────────────────────────────────────
        sl_resp = await self.place_stop_market_order(
            symbol, side_cls, qty, sl_price, direction, "STOP_MARKET",
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
                    symbol, side_cls, qty_safe, sl_price, direction, "STOP_MARKET",
                )
                results["sl"] = sl_resp2
                if sl_resp2.get("code", -1) == 0:
                    log.info("[%s] SL OK (retry) @ %.6f qty=%.6f", symbol, sl_price, qty_safe)
                else:
                    log.error("[%s] SL FALLIDO también en retry: %s", symbol, sl_resp2)

        # ── TP1 ───────────────────────────────────────────────────────────────
        if qty_half > 0:
            tp1_resp = await self.place_stop_market_order(
                symbol, side_cls, qty_half, tp1_price, direction, "TAKE_PROFIT_MARKET",
            )
            results["tp1"] = tp1_resp
            if tp1_resp.get("code", -1) == 0:
                log.info("[%s] TP1 OK @ %.6f qty=%.6f", symbol, tp1_price, qty_half)
            else:
                log.error("[%s] TP1 FALLIDO: %s", symbol, tp1_resp)

        # ── TP2 ───────────────────────────────────────────────────────────────
        if qty_remain > 0:
            tp2_resp = await self.place_stop_market_order(
                symbol, side_cls, qty_remain, tp2_price, direction, "TAKE_PROFIT_MARKET",
            )
            results["tp2"] = tp2_resp
            if tp2_resp.get("code", -1) == 0:
                log.info("[%s] TP2 OK @ %.6f qty=%.6f", symbol, tp2_price, qty_remain)
            else:
                log.error("[%s] TP2 FALLIDO: %s", symbol, tp2_resp)

        return results
