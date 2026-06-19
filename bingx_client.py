"""
QF×JP Bot v7.6 — BingX Client DEFINITIVO (fix firma byte-a-byte)
═══════════════════════════════════════════════════════════════════════════════
FIX v7.6 — causa real del 100001 "Signature mismatch" que persistía en v7.5:

  v7.5 firmaba con:
      qs  = urlencode(sorted(params.items()))
      sig = hmac.new(secret, qs.encode(), sha256).hexdigest()
  ... pero luego enviaba la petición con `session.get(url, params=p)`,
  dejando que aiohttp/yarl reconstruyera el query string POR SU CUENTA a
  partir del dict `p`. aiohttp serializa por orden de inserción del dict
  (no alfabético) y con sus propias reglas de encoding — NO garantizado
  idéntico byte a byte a `urlencode(sorted(...))`. BingX exige que la
  cadena firmada y la cadena realmente transmitida sean EXACTAMENTE la
  misma; cualquier diferencia de orden o encoding rompe la firma aunque
  el secret sea 100% correcto. Por eso .strip() y regenerar la clave no
  arreglaba nada: el secret nunca fue el problema.

  FIX: se construye la URL completa (incluyendo "&signature=...") UNA
  sola vez, y esa URL exacta es la que se pasa a aiohttp — sin ningún
  parámetro params= adicional que pueda volver a serializar nada.

DE v7.5 (features que se conservan):
  ✅ .strip() en API key y secret (config.py ya lo aplica también)
  ✅ _get_real_position_side() — Hedge/One-Way auto-detección
  ✅ _extract_executed_qty() — qty real de BingX (fix error 110424)
  ✅ _safe_qty_for_sl()
  ✅ Sleep 1.2s post-entrada
  ✅ Retry HTTP 3 intentos con backoff exponencial
  ✅ cancel_order / cancel_all_orders con DELETE (no POST)
  ✅ Sin closePosition=true (fix error 10940)

FIX v6.4 reincorporado (visibilidad de errores):
  ✅ get_balance() y get_open_positions() ahora revisan data["code"]
     ANTES de extraer — un error de firma/permiso llega como HTTP 200
     + {'code': 100001}, y antes se traducía en silencio a "balance=0"
     o "0 posiciones" sin ningún log de error visible.
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
        self._session:        aiohttp.ClientSession | None = None
        self._precision_map:  dict[str, int]   = {}
        self._min_qty_map:    dict[str, float] = {}
        self._step_map:       dict[str, float] = {}
        log.info("BingXClient v7.6 — firma byte-a-byte (fix double-serialización)")
        # Diagnóstico seguro: longitud de claves, NUNCA el valor real.
        log.info("[auth] API_KEY len=%d | SECRET_KEY len=%d",
                  len(C.BINGX_API_KEY), len(C.BINGX_SECRET_KEY))
        if not C.BINGX_API_KEY or not C.BINGX_SECRET_KEY:
            log.error("[auth] BINGX_API_KEY o BINGX_SECRET_KEY vacíos — revisa Railway → Variables")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15),
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    # ── Firma + construcción de URL — TODO en un solo paso ───────────────────

    def _build_url(self, path: str, params: dict | None, signed: bool) -> tuple[str, dict]:
        """
        Construye la URL completa con el query string EXACTO que se firma.

        CRÍTICO: el string que se firma y el string que se transmite deben
        ser BYTE-IDÉNTICOS. Por eso esta función no devuelve un dict para
        que aiohttp lo vuelva a serializar — devuelve la URL ya completa
        (con &signature= incluido si signed=True) para pasarla directo a
        session.get/post/delete SIN el argumento params=.
        """
        p = dict(params or {})
        headers = {}

        if signed:
            p["timestamp"]  = int(time.time() * 1000)
            p["recvWindow"] = 10000
            qs  = urlencode(sorted(p.items()))
            sig = hmac.new(
                C.BINGX_SECRET_KEY.encode("utf-8"),
                qs.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
            qs = f"{qs}&signature={sig}"
            headers = {"X-BX-APIKEY": C.BINGX_API_KEY}
        else:
            qs = urlencode(sorted(p.items())) if p else ""

        url = f"{C.BINGX_BASE_URL}{path}"
        if qs:
            url = f"{url}?{qs}"
        return url, headers

    # ── HTTP con retry (3 intentos, backoff exponencial) ─────────────────────
    # FIX v7.6: nunca se pasa params= aquí — la URL ya viene completa y
    # firmada desde _build_url(), así que session.get/post/delete(url) la
    # usa tal cual, sin ninguna re-serialización intermedia.

    async def _get(self, path: str, params: dict | None = None,
                   signed: bool = True) -> dict:
        for attempt in range(3):
            try:
                s = await self._get_session()
                url, headers = self._build_url(path, params, signed)
                async with s.get(url, headers=headers) as r:
                    return await r.json(content_type=None)
            except Exception as e:
                if attempt == 2:
                    log.error("GET %s error: %s", path, e)
                    raise
                await asyncio.sleep(1.5 ** attempt)
        return {}

    async def _post(self, path: str, params: dict) -> dict:
        for attempt in range(3):
            try:
                s = await self._get_session()
                url, headers = self._build_url(path, params, signed=True)
                async with s.post(url, headers=headers) as r:
                    return await r.json(content_type=None)
            except Exception as e:
                if attempt == 2:
                    log.error("POST %s error: %s", path, e)
                    raise
                await asyncio.sleep(1.5 ** attempt)
        return {}

    async def _delete(self, path: str, params: dict) -> dict:
        """DELETE correcto para cancel_order y cancel_all_orders."""
        for attempt in range(3):
            try:
                s = await self._get_session()
                url, headers = self._build_url(path, params, signed=True)
                async with s.delete(url, headers=headers) as r:
                    return await r.json(content_type=None)
            except Exception as e:
                if attempt == 2:
                    log.error("DELETE %s error: %s", path, e)
                    raise
                await asyncio.sleep(1.5 ** attempt)
        return {}

    # ── Precisión ─────────────────────────────────────────────────────────────

    def _round_qty(self, symbol: str, qty: float) -> float:
        step = self._step_map.get(symbol, 0)
        if step > 0:
            qty       = math.floor(qty / step) * step
            precision = max(0, round(-math.log10(step)))
            qty       = round(qty, precision)
        else:
            precision = self._precision_map.get(symbol, 4)
            qty       = math.floor(qty * 10**precision) / 10**precision
        min_qty = self._min_qty_map.get(symbol, 0)
        return max(qty, min_qty) if qty > 0 else 0.0

    def _safe_qty_for_sl(self, symbol: str, qty: float) -> float:
        step = self._step_map.get(symbol, 0)
        if step > 0:
            qty       = math.floor(qty / step) * step
            precision = max(0, round(-math.log10(step)))
            qty       = round(qty, precision)
        else:
            precision = self._precision_map.get(symbol, 4)
            qty       = round(qty * 0.9999, precision)
        min_qty = self._min_qty_map.get(symbol, 0)
        return max(qty, min_qty) if qty > 0 else 0.0

    def _extract_executed_qty(self, entry_resp: dict, fallback_qty: float) -> float:
        try:
            data  = entry_resp.get("data", {})
            order = data.get("order", data)
            for field in ("executedQty", "origQty", "quantity"):
                val = order.get(field, "")
                if val and str(val) not in ("", "0", "0.0"):
                    extracted = float(val)
                    if extracted > 0:
                        return extracted
        except Exception:
            pass
        return self._safe_qty_for_sl("", fallback_qty)

    def _parse_error(self, resp: dict) -> str:
        if not isinstance(resp, dict):
            return ""
        return str(resp.get("msg", resp.get("message", ""))).lower()

    # ── positionSide auto-detección ───────────────────────────────────────────

    async def _get_real_position_side(self, symbol: str, direction: str) -> str:
        try:
            positions = await self.get_open_positions()
            for p in positions:
                if p.get("symbol") != symbol:
                    continue
                ps = p.get("positionSide", "")
                if ps in ("LONG", "SHORT", "BOTH"):
                    return ps
        except Exception as e:
            log.debug("[%s] _get_real_position_side error: %s", symbol, e)
        return direction

    # ── Símbolos ──────────────────────────────────────────────────────────────

    async def get_all_symbols(self) -> list[str]:
        data = await self._get("/openApi/swap/v2/quote/contracts", signed=False)
        raw  = data.get("data", [])
        if isinstance(raw, dict):
            raw = raw.get("contracts", raw.get("list", []))
        if not isinstance(raw, list):
            raw = []

        symbols      = []
        vol_map:     dict[str, float] = {}
        vol_detected = 0
        _bad = ("BEAR", "BULL", "PUMP", "NCS")

        for item in raw:
            if not isinstance(item, dict):
                continue
            sym = item.get("symbol", "")
            if not sym:
                continue
            if "-" not in sym and sym.endswith("USDT"):
                sym = sym[:-4] + "-USDT"
            # FIX CRÍTICO: BLACKLIST se configura SIN sufijo ("SYN", "ESPORTS")
            # pero `sym` aquí ya está normalizado CON sufijo ("SYN-USDT").
            # `sym in C.BLACKLIST` nunca coincidía → el blacklist nunca filtró
            # nada, ni siquiera ESPORTS pese a estar añadido explícitamente.
            # Se compara contra ambas formas para cubrir cualquier config.
            base_sym = sym.replace("-USDT", "")
            if not sym.endswith("-USDT") or base_sym in C.BLACKLIST or sym in C.BLACKLIST:
                continue
            if any(sym.replace("-USDT", "").startswith(p) for p in _bad):
                continue

            self._precision_map[sym] = int(item.get("volumePrecision",
                                            item.get("quantityPrecision", 4)) or 4)
            self._min_qty_map[sym]   = float(item.get("tradeMinQuantity",
                                              item.get("minOrderQty", 0)) or 0)
            self._step_map[sym]      = float(item.get("qtyStep",
                                              item.get("stepSize", 0)) or 0)

            vol = float(item.get("volume24h") or item.get("vol24h") or
                        item.get("quoteVolume") or item.get("tradeAmt") or 0)
            if vol > 0:
                vol_detected += 1
            vol_map[sym] = vol
            symbols.append(sym)

        if vol_detected == 0 and symbols:
            log.info("contracts sin volumen → enriqueciendo con /ticker")
            try:
                td = await self._get("/openApi/swap/v2/quote/ticker", signed=False)
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
                log.warning("ticker fallback: %s", e)

        if vol_detected > 0 and C.MIN_VOLUME_USDT > 0:
            symbols = [s for s in symbols if vol_map.get(s, 0) >= C.MIN_VOLUME_USDT]

        symbols.sort(key=lambda s: vol_map.get(s, 0), reverse=True)
        if C.TOP_N_SYMBOLS > 0:
            symbols = symbols[:C.TOP_N_SYMBOLS]

        log.info("get_all_symbols: %d símbolos (raw=%d, con_vol=%d)",
                 len(symbols), len(raw), vol_detected)
        return symbols

    # ── Market data ───────────────────────────────────────────────────────────

    async def get_klines(self, symbol: str, interval: str, limit: int = 200) -> list:
        data = await self._get("/openApi/swap/v3/quote/klines",
                               {"symbol": symbol, "interval": interval, "limit": limit},
                               signed=False)
        raw = data.get("data", [])
        if isinstance(raw, dict):
            raw = raw.get("klines", [])
        result = []
        for c in raw:
            try:
                if isinstance(c, dict):
                    result.append([int(c.get("time", c.get("openTime", 0))),
                                   float(c.get("open",   c.get("o", 0))),
                                   float(c.get("high",   c.get("h", 0))),
                                   float(c.get("low",    c.get("l", 0))),
                                   float(c.get("close",  c.get("c", 0))),
                                   float(c.get("volume", c.get("v", 0)))])
                elif isinstance(c, (list, tuple)) and len(c) >= 6:
                    result.append([int(c[0]), float(c[1]), float(c[2]),
                                   float(c[3]), float(c[4]), float(c[5])])
            except Exception:
                continue
        return sorted(result, key=lambda x: x[0])

    async def get_ticker(self, symbol: str) -> dict:
        data = await self._get("/openApi/swap/v2/quote/ticker",
                               {"symbol": symbol}, signed=False)
        raw = data.get("data", {})
        if isinstance(raw, list):
            return raw[0] if raw else {}
        return raw if isinstance(raw, dict) else {}

    async def get_order_book(self, symbol: str, limit: int = 10) -> dict:
        data = await self._get("/openApi/swap/v2/quote/depth",
                               {"symbol": symbol, "limit": limit}, signed=False)
        raw = data.get("data", data)
        if isinstance(raw, dict):
            return {"bids": raw.get("bids", []), "asks": raw.get("asks", [])}
        return {"bids": [], "asks": []}

    async def get_funding_rate(self, symbol: str) -> float:
        try:
            data = await self._get("/openApi/swap/v2/quote/fundingRate",
                                   {"symbol": symbol}, signed=False)
            raw = data.get("data", {})
            if isinstance(raw, list):
                raw = raw[0] if raw else {}
            return float(raw.get("fundingRate", 0) or 0)
        except Exception:
            return 0.0

    # ── Cuenta ────────────────────────────────────────────────────────────────

    async def get_balance(self) -> float:
        """
        FIX v7.6 (reincorporado de v6.4): revisa data["code"] ANTES de
        extraer. Un error de firma/permiso llega como HTTP 200 +
        {'code': 100001}, y antes se traducía en silencio a balance=0.0
        sin ningún log de error — ahora queda visible explícitamente.
        """
        data = await self._get("/openApi/swap/v3/user/balance", {"currency": "USDT"})

        code = data.get("code", 0)
        if code not in (0, None):
            log.error("[auth] get_balance código=%s msg=%s — firma/permiso rechazado por BingX",
                      code, data.get("msg", ""))
            return 0.0

        raw = data.get("data", {})

        def _extract(d: dict) -> float:
            avail  = float(d.get("availableMargin", 0) or 0)
            equity = float(d.get("equity",          0) or 0)
            return avail if avail > 0 else equity

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

    async def get_open_positions(self) -> list:
        """FIX v7.6: mismo chequeo de código que get_balance() — ver ahí."""
        data = await self._get("/openApi/swap/v2/user/positions")

        code = data.get("code", 0)
        if code not in (0, None):
            log.error("[auth] get_open_positions código=%s msg=%s — firma/permiso rechazado por BingX",
                      code, data.get("msg", ""))
            return []

        pos = data.get("data", [])
        if not isinstance(pos, list):
            return []
        return [p for p in pos if float(p.get("positionAmt", 0) or 0) != 0]

    async def get_open_orders(self, symbol: str) -> list:
        data = await self._get("/openApi/swap/v2/trade/openOrders", {"symbol": symbol})
        return data.get("data", {}).get("orders", [])

    # ── Apalancamiento ────────────────────────────────────────────────────────

    async def set_leverage(self, symbol: str, leverage: int, side: str = "LONG") -> bool:
        results = await asyncio.gather(
            self._post("/openApi/swap/v2/trade/leverage",
                       {"symbol": symbol, "side": "LONG",  "leverage": leverage}),
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
                log.warning("[%s] set_leverage %s: %s", symbol, s, r.get("code"))
        return ok

    # ── Órdenes ───────────────────────────────────────────────────────────────

    async def place_stop_market_order(
        self,
        symbol:     str,
        side:       str,
        quantity:   float,
        stop_price: float,
        direction:  str = "LONG",
        order_type: str = "STOP_MARKET",
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
        resp = await self._post("/openApi/swap/v2/trade/order", params)

        if isinstance(resp, dict) and resp.get("code", -1) != 0:
            msg = self._parse_error(resp)
            if "positionside" in msg or "position side" in msg:
                log.warning("[%s] positionSide fallback → %s", symbol, direction)
                params["positionSide"] = direction
                resp = await self._post("/openApi/swap/v2/trade/order", params)
            elif "position not exist" in msg and real_ps != "BOTH":
                log.warning("[%s] position not exist → probando BOTH", symbol)
                params["positionSide"] = "BOTH"
                resp = await self._post("/openApi/swap/v2/trade/order", params)

        return resp if isinstance(resp, dict) else {"code": -1, "msg": str(resp)}

    async def cancel_order(self, symbol: str, order_id: str) -> dict:
        """DELETE correcto — v7.2 usaba POST (no cancelaba nada)."""
        return await self._delete(
            "/openApi/swap/v2/trade/order",
            {"symbol": symbol, "orderId": order_id},
        )

    async def cancel_all_orders(self, symbol: str) -> dict:
        """DELETE correcto."""
        return await self._delete(
            "/openApi/swap/v2/trade/allOpenOrders",
            {"symbol": symbol},
        )

    async def close_position_market(self, symbol: str, quantity: float,
                                     direction: str) -> dict:
        side    = "SELL" if direction == "LONG" else "BUY"
        qty     = self._round_qty(symbol, quantity)
        real_ps = await self._get_real_position_side(symbol, direction)

        params = {"symbol": symbol, "side": side, "positionSide": real_ps,
                  "type": "MARKET", "quantity": str(qty)}
        log.info("[%s] CLOSE MARKET ps=%s qty=%s", symbol, real_ps, qty)
        resp = await self._post("/openApi/swap/v2/trade/order", params)

        if isinstance(resp, dict) and resp.get("code", -1) != 0:
            msg = self._parse_error(resp)
            if "positionside" in msg or "position side" in msg:
                params["positionSide"] = direction
                resp = await self._post("/openApi/swap/v2/trade/order", params)
            elif "position not exist" in msg and real_ps != "BOTH":
                params["positionSide"] = "BOTH"
                resp = await self._post("/openApi/swap/v2/trade/order", params)

        return resp if isinstance(resp, dict) else {"code": -1}

    # ── open_trade ────────────────────────────────────────────────────────────

    async def open_trade(self, symbol: str, direction: str, quantity: float,
                          sl_price: float, tp1_price: float, tp2_price: float) -> dict:
        qty       = self._round_qty(symbol, quantity)
        side_open = "BUY"  if direction == "LONG" else "SELL"
        side_cls  = "SELL" if direction == "LONG" else "BUY"
        results   = {}

        await self.set_leverage(symbol, C.LEVERAGE, direction)

        if qty <= 0:
            return {"entry": {"code": -1, "msg": "qty_zero"}}

        # Entrada
        entry_resp = await self._post("/openApi/swap/v2/trade/order", {
            "symbol":       symbol,
            "side":         side_open,
            "positionSide": direction,
            "type":         "MARKET",
            "quantity":     str(qty),
        })
        results["entry"] = entry_resp
        log.info("[%s] MARKET %s qty=%s → code=%s",
                 symbol, side_open, qty, entry_resp.get("code", "?"))

        if entry_resp.get("code", -1) != 0:
            return results

        # qty real ejecutada (fix 110424)
        real_qty = self._extract_executed_qty(entry_resp, qty)
        if abs(real_qty - qty) > qty * 0.001:
            log.info("[%s] qty ajustada: local=%.6f real=%.6f", symbol, qty, real_qty)
        qty = real_qty

        await asyncio.sleep(1.2)

        # Split para TP
        step = self._step_map.get(symbol, 0)
        prec = max(0, round(-math.log10(step))) if step > 0 else self._precision_map.get(symbol, 4)
        f    = 10 ** prec
        qty_half   = math.floor(qty / 2 * f) / f
        qty_remain = math.floor((qty - qty_half) * f) / f

        # SL + TP1 + TP2 en paralelo
        sl_r, tp1_r, tp2_r = await asyncio.gather(
            self.place_stop_market_order(symbol, side_cls, qty,        sl_price,  direction, "STOP_MARKET"),
            self.place_stop_market_order(symbol, side_cls, qty_half,   tp1_price, direction, "TAKE_PROFIT_MARKET"),
            self.place_stop_market_order(symbol, side_cls, qty_remain, tp2_price, direction, "TAKE_PROFIT_MARKET"),
            return_exceptions=True,
        )

        for label, r, price in [("SL", sl_r, sl_price), ("TP1", tp1_r, tp1_price), ("TP2", tp2_r, tp2_price)]:
            resp = r if isinstance(r, dict) else {"code": -1, "msg": str(r)}
            results[label.lower()] = resp
            if resp.get("code", -1) == 0:
                log.info("[%s] %s OK @ %.6f", symbol, label, price)
            else:
                log.error("[%s] %s FALLIDO: %s", symbol, label, resp)
                if label == "SL":
                    qty_safe = self._safe_qty_for_sl(symbol, qty)
                    if qty_safe != qty:
                        resp2 = await self.place_stop_market_order(
                            symbol, side_cls, qty_safe, sl_price, direction, "STOP_MARKET")
                        results["sl"] = resp2
                        if resp2.get("code", -1) == 0:
                            log.info("[%s] SL OK (retry) @ %.6f", symbol, sl_price)

        return results

    # ── Open Interest ─────────────────────────────────────────────────────────

    async def get_open_interest(self, symbol: str) -> float:
        """
        Retorna el Open Interest actual en USD del símbolo.
        Endpoint: /openApi/swap/v2/quote/openInterest
        El OI creciente en dirección de la señal confirma la tendencia;
        OI decreciente sugiere que las posiciones se están cerrando (trampa).
        """
        try:
            data = await self._get(
                "/openApi/swap/v2/quote/openInterest",
                {"symbol": symbol},
                signed=False,
            )
            raw = data.get("data", {})
            if isinstance(raw, dict):
                oi = float(raw.get("openInterest", raw.get("openInterestValue", 0)) or 0)
                return oi
        except Exception as e:
            log.debug("[%s] get_open_interest error: %s", symbol, e)
        return 0.0

    # ── Limit order con timeout → fallback market ─────────────────────────────

    async def place_limit_entry(
        self,
        symbol:     str,
        direction:  str,
        qty:        float,
        price:      float,
        timeout_s:  int = 25,
    ) -> dict:
        """
        Orden límite real MAKER (fee 0.02% vs 0.05% taker = ahorro 60%).

        Precio:
          LONG:  price * 0.9995 (0.05% bajo el mark) → añade liquidez = MAKER
          SHORT: price * 1.0005 (0.05% sobre el mark) → añade liquidez = MAKER

        Si no se llena en timeout_s cancela y devuelve {} para fallback a market.
        Con mercados en tendencia se llena en 2-5 segundos.
        """
        qty_r     = self._round_qty(symbol, qty)
        side_open = "BUY" if direction == "LONG" else "SELL"
        prec      = max(self._precision_map.get(symbol, 4), 2)

        # Precio ligeramente mejor que el mark → garantiza maker
        if direction == "LONG":
            lmt_price = round(price * 0.9995, prec + 2)
        else:
            lmt_price = round(price * 1.0005, prec + 2)

        params = {
            "symbol":       symbol,
            "side":         side_open,
            "positionSide": direction,
            "type":         "LIMIT",
            "price":        str(lmt_price),
            "quantity":     str(qty_r),
            "timeInForce":  "GTC",
        }
        resp = await self._post("/openApi/swap/v2/trade/order", params)
        if isinstance(resp, dict) and resp.get("code", -1) != 0:
            log.debug("[%s] limit entry rechazado (code=%s): %s",
                      symbol, resp.get("code"), resp.get("msg", ""))
            return {}

        order_id = str(
            resp.get("data", {}).get("order", {}).get("orderId", "")
            or resp.get("data", {}).get("orderId", "")
        )
        if not order_id:
            return {}

        log.info("[%s] 📋 Limit entry @ %.6f (maker) — esperando fill (%ds)",
                 symbol, lmt_price, timeout_s)

        # Polling hasta timeout
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            await asyncio.sleep(3)
            try:
                orders = await self.get_open_orders(symbol)
                still_open = any(str(o.get("orderId")) == order_id for o in orders)
                if not still_open:
                    log.info("[%s] ✅ Limit LLENA — fee maker 0.02%%", symbol)
                    return resp
            except Exception:
                pass

        # Timeout — cancelar y fallback a market
        try:
            await self.cancel_order(symbol, order_id)
            log.info("[%s] Limit timeout (%ds) — cancelada → market order", symbol, timeout_s)
        except Exception as e:
            log.debug("[%s] cancel limit: %s", symbol, e)
        return {}   # vacío = caller usa market
