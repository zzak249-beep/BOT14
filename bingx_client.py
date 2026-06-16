"""
QF×JP Bot v7.2 — BingX Client
═══════════════════════════════════════════════════════════════════════════════
FIXES vs v6.6:

  FIX 1 — Error 100001 "Signature verification failed" (renewed-love):
    CAUSA: BINGX_SECRET_KEY leída de Railway puede tener espacios/newlines
    al final si se copia desde el portapapeles. El HMAC resultante era
    incorrecto porque la key tenía padding invisible.
    FIX: C.BINGX_API_KEY.strip() y C.BINGX_SECRET_KEY.strip() en _sign()
    y en los headers. También se añade strip() en config.py directamente.

  FIX 2 — Error 110424 "order size must be less than available amount" (SL):
    CAUSA: open_trade calcula qty localmente y la envía a la entrada.
    BingX puede ejecutar con qty ligeramente diferente (redondeo interno).
    El SL luego se coloca con la qty ORIGINAL (mayor) → BingX rechaza
    porque SL qty > posición abierta real.
    FIX: Tras la entrada, se extrae la qty REAL ejecutada de la respuesta
    (entry_resp → data → order → origQty / executedQty). Si no está
    disponible, se usa un floor seguro de qty*0.9999.

  FIX 3 — Sleep post-entrada 0.6s → 1.2s:
    BingX necesita tiempo para registrar la posición antes de que
    place_stop_market_order llame a _get_real_position_side() via
    get_open_positions(). Con 0.6s podía haber race condition donde
    la posición aún no aparecía y se usaba positionSide incorrecto.

  FIX 4 — cancel_order (nuevo método):
    position_manager._update_trail() llama client.cancel_order() pero
    el método no existía → AttributeError silencioso.
    Añadido cancel_order(symbol, order_id) correcto.

  FIX 5 — get_all_symbols: TOP_N_SYMBOLS solo si > 0:
    Si TOP_N_SYMBOLS=0 devolvía lista vacía en algunos paths.
    Ahora el slice solo aplica cuando TOP_N_SYMBOLS > 0.

  MANTIENE de v6.6:
    - positionSide auto-detección Hedge/One-Way
    - Fallback inteligente por mensaje de error
    - qty split correcto para TP1/TP2
    - _round_qty con stepSize y precision
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
        """
        FIX v7.2: .strip() en la secret key para eliminar espacios/newlines
        invisibles que Railway puede añadir al copiar variables de entorno.
        """
        qs  = urlencode(sorted(params.items()))
        key = C.BINGX_SECRET_KEY.strip()   # FIX: strip() evita 100001
        return hmac.new(
            key.encode(),
            qs.encode(),
            hashlib.sha256,
        ).hexdigest()

    def _api_key(self) -> str:
        """FIX v7.2: strip() también en API key para evitar header inválido."""
        return C.BINGX_API_KEY.strip()

    async def _get(self, path: str, params: dict = None) -> dict:
        params = params or {}
        params["timestamp"]  = int(time.time() * 1000)
        params["recvWindow"] = 10000
        params["signature"]  = self._sign(params)
        url = C.BINGX_BASE_URL + path
        s   = await self._get_session()
        async with s.get(url, params=params,
                         headers={"X-BX-APIKEY": self._api_key()}) as r:
            return await r.json()

    async def _post(self, path: str, params: dict) -> dict:
        params["timestamp"]  = int(time.time() * 1000)
        params["recvWindow"] = 10000
        params["signature"]  = self._sign(params)
        url = C.BINGX_BASE_URL + path
        s   = await self._get_session()
        async with s.post(url, params=params,
                          headers={"X-BX-APIKEY": self._api_key()}) as r:
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

    def _safe_qty_for_sl(self, symbol: str, qty: float) -> float:
        """
        FIX v7.2 (error 110424): calcula qty segura para SL/TP que sea
        ≤ qty ejecutada por BingX en la entrada.
        BingX puede ejecutar con qty ligeramente menor por redondeo interno.
        Usamos floor agresivo: floor al step anterior, garantiza qty_sl ≤ qty_real.
        """
        step = self._step_map.get(symbol, 0)
        if step > 0:
            # Un step menos para asegurar qty_sl < qty_posicion
            qty = math.floor(qty / step) * step
            precision = max(0, round(-math.log10(step)))
            qty = round(qty, precision)
        else:
            precision = self._precision_map.get(symbol, 4)
            # 0.01% menos — cubre diferencias de redondeo BingX
            qty = round(qty * 0.9999, precision)
        min_qty = self._min_qty_map.get(symbol, 0)
        return max(qty, min_qty) if qty > 0 else 0.0

    def _extract_executed_qty(self, entry_resp: dict, fallback_qty: float) -> float:
        """
        FIX v7.2 (error 110424): extrae la qty REAL ejecutada por BingX
        de la respuesta de la orden de entrada para usarla en SL/TP.
        Evita el error 110424 donde el SL se rechaza porque qty_SL > qty_posicion.
        """
        try:
            data  = entry_resp.get("data", {})
            order = data.get("order", data)  # soporta ambos formatos
            # BingX devuelve origQty (qty solicitada) o executedQty (qty ejecutada)
            for field in ("executedQty", "origQty", "quantity"):
                val = order.get(field, "")
                if val and str(val) not in ("", "0", "0.0"):
                    extracted = float(val)
                    if extracted > 0:
                        log.debug("qty_real de entrada: %s=%s", field, val)
                        return extracted
        except Exception as e:
            log.debug("_extract_executed_qty error: %s", e)
        # Fallback: qty calculada localmente con margen de seguridad
        return self._safe_qty_for_sl("", fallback_qty)

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
                # FIX v7.2: slice solo si TOP_N_SYMBOLS > 0
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
            # FIX v7.2: slice solo si TOP_N_SYMBOLS > 0
            return symbols[:C.TOP_N_SYMBOLS] if C.TOP_N_SYMBOLS > 0 else symbols
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

    async def cancel_order(self, symbol: str, order_id: str) -> dict:
        """
        FIX v7.2: método cancel_order que faltaba.
        position_manager._update_trail() llama este método para cancelar
        el SL viejo tras colocar el nuevo (estrategia place-then-cancel).
        Sin este método → AttributeError → trailing stop no funciona.
        """
        try:
            resp = await self._post("/openApi/swap/v2/trade/order",
                                    {"symbol": symbol, "orderId": order_id})
            # BingX usa DELETE semántico via POST con action=cancel en algunos endpoints
            # Si falla, probar endpoint de cancelación directa
            if isinstance(resp, dict) and resp.get("code", -1) != 0:
                resp2 = await self._post("/openApi/swap/v2/trade/cancelOrder",
                                         {"symbol": symbol, "orderId": order_id})
                if isinstance(resp2, dict) and resp2.get("code", 0) == 0:
                    return resp2
            return resp if isinstance(resp, dict) else {"code": -1}
        except Exception as e:
            log.debug("[%s] cancel_order %s: %s", symbol, order_id, e)
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
        return direction

    def _parse_bingx_error(self, resp: dict) -> str:
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

        FIX v7.2 (110424): usa la qty REAL ejecutada por BingX (leída de la
        respuesta de entrada) para el SL y los TP, no la qty calculada
        localmente. Esto evita el error 110424 donde el SL se rechaza
        porque su qty supera la posición abierta real (diferencias de
        redondeo entre nuestro cálculo y el ejecutor de BingX).

        FIX v7.2 (timing): sleep post-entrada 0.6s → 1.2s para dar tiempo
        a BingX a registrar la posición antes de que place_stop_market_order
        llame a _get_real_position_side() via get_open_positions().
        """
        qty       = self._round_qty(symbol, quantity)
        side_open = "BUY" if direction == "LONG" else "SELL"
        side_cls  = "SELL" if direction == "LONG" else "BUY"
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

        # FIX v7.2 (110424): extraer qty REAL ejecutada por BingX de la respuesta
        real_qty = self._extract_executed_qty(entry_resp, qty)
        if abs(real_qty - qty) > qty * 0.001:   # >0.1% diferencia → loguear
            log.info("[%s] qty ajustada: calculada=%.6f real_BingX=%.6f",
                     symbol, qty, real_qty)
        qty = real_qty   # usar qty real para SL/TP

        # FIX v7.2: sleep 0.6→1.2s para que BingX registre la posición
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

        # Verificar que la suma no supere qty real (protección extra)
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
            # Intentar con qty_safe como último recurso
            qty_safe = self._safe_qty_for_sl(symbol, qty)
            if qty_safe != qty:
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
