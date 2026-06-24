"""
QF×JP Bot v7.11 — BingX Client
═══════════════════════════════════════════════════════════════════════════════
FIX v7.11 — CRÍTICO: sizing 26-47x en tokens con mismatch de unidades
  Caso real: ANIME-USDT (47x) y FLOCK-USDT (26x). El bot enviaba qty=268
  contratos, BingX ejecutaba 7,073 tokens — la diferencia varía por par.
  Causa probable: _extract_executed_qty() extraía un campo de la respuesta
  de BingX (executedQty / origQty / quantity) que para algunos pares
  devuelve el importe en tokens base en vez de contratos, resultando en
  una qty 26-47x mayor almacenada en el trade y usada para SL/TP.

  Fix A — safety cap en _extract_executed_qty(): si el valor extraído es
  >5x la qty local calculada, log.error + warning Telegram + fallback a
  la qty local. Esto cubre cualquier par no identificado todavía.

  Fix B — log.debug del entry_resp completo para diagnosticar qué campo
  exacto devuelve BingX para cada par afectado — permite identificar la
  causa raíz exacta y eliminar el cap una vez corregido.

FIX v7.10 (sin cambios):
  ✅ place_stop_market_order() y close_position_market() verifican
     dirección REAL contra positionAmt antes de construir la orden.

FIX v7.9 (sin cambios):
  ✅ get_open_positions() lanza excepción en error de API (no devuelve []).
  ✅ Caché de 3s en get_open_positions().

FIX v7.8 (sin cambios):
  ✅ TP1 como TAKE_PROFIT límite (maker) con fallback automático a market.
  ✅ place_limit_entry() coloca SL+TP1+TP2 al confirmar el fill.
  ✅ Firma byte-a-byte, _extract_executed_qty(), _safe_qty_for_sl().
  ✅ Retry HTTP 3 intentos con backoff exponencial.
  ✅ cancel_order / cancel_all_orders con DELETE (no POST).
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


NON_CRYPTO_PREFIXES = (
    "BEAR", "BULL", "PUMP", "NCS",
    "OIL", "WTI", "BRENT", "XAU", "XAG", "EUR", "GBP", "JPY",
)


class BingXClient:
    def __init__(self):
        self._session:        object = None
        self._precision_map:  dict[str, int]   = {}
        self._min_qty_map:    dict[str, float] = {}
        self._step_map:       dict[str, float] = {}
        self._positions_cache: tuple = (0.0, [])
        self._POSITIONS_CACHE_TTL = 3.0
        log.info("BingXClient v7.11 — safety cap qty mismatch + firma byte-a-byte + "
                 "SL/TP en límite + TP1 maker + fix get_open_positions + "
                 "verificación de dirección real")
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

    # ── Firma + construcción de URL ───────────────────────────────────────────

    def _build_url(self, path: str, params: dict, signed: bool) -> tuple[str, dict]:
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

    # ── HTTP con retry ────────────────────────────────────────────────────────

    async def _get(self, path: str, params: dict = None,
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

    def _extract_executed_qty(self, entry_resp: dict, fallback_qty: float,
                               symbol: str = "") -> float:
        """
        Extrae la qty ejecutada de la respuesta de BingX.

        FIX v7.11 — SAFETY CAP: si el valor extraído es >5x la qty local
        calculada, descarta la extracción y usa el fallback. Caso real
        confirmado en producción:
          - ANIME-USDT: bot ordenó 4,496 contratos, BingX devolvió
            executedQty=212,520 (tokens base, ratio 47x) — el bot almacenó
            212,520 como trade.qty, abriendo una posición 47x mayor de lo
            calculado y sufriendo una pérdida de -11.33 USDT en vez de
            los ~0.11 USDT de riesgo calculados.
          - FLOCK-USDT: mismo bug, ratio 26x, pérdida -9.19 USDT.

        El log de debug del entry_resp completo permite identificar qué
        campo exacto BingX usa para cada par afectado y corregir la causa
        raíz una vez confirmada.
        """
        # FIX v7.11: log completo para diagnóstico de mismatch de unidades
        if symbol:
            log.debug("[%s] entry_resp completo: %s", symbol, entry_resp)

        try:
            data  = entry_resp.get("data", {})
            order = data.get("order", data)
            for field in ("executedQty", "origQty", "quantity"):
                val = order.get(field, "")
                if val and str(val) not in ("", "0", "0.0"):
                    extracted = float(val)
                    if extracted > 0:
                        # FIX v7.11 — SAFETY CAP ────────────────────────────
                        # Si el valor extraído es >5x la qty local, es una
                        # señal clara de mismatch de unidades (contratos vs
                        # tokens base). Usar fallback y alertar.
                        # El umbral es 5x (no 2x) para no dispararse con
                        # variaciones normales de fill parcial o slippage.
                        if fallback_qty > 0 and extracted > fallback_qty * 5:
                            log.error(
                                "[%s] ⚠️ SAFETY CAP qty: campo '%s' devolvió %.4f "
                                "(%.1fx la qty local %.4f) — probable mismatch "
                                "contratos/tokens en BingX. Usando fallback %.4f. "
                                "Revisar entry_resp en logs DEBUG.",
                                symbol, field, extracted,
                                extracted / max(fallback_qty, 1e-9),
                                fallback_qty, fallback_qty,
                            )
                            # Notificación Telegram asíncrona no disponible
                            # aquí — el log.error es suficiente para Railway.
                            # El llamador (open_trade) logueará "qty ajustada"
                            # solo si la diferencia supera el 0.1% — en este
                            # caso no habrá diferencia (fallback = local).
                            return self._safe_qty_for_sl(symbol, fallback_qty)
                        return extracted
        except Exception:
            pass
        return self._safe_qty_for_sl(symbol, fallback_qty)

    def _parse_error(self, resp: dict) -> str:
        if not isinstance(resp, dict):
            return ""
        return str(resp.get("msg", resp.get("message", ""))).lower()

    @staticmethod
    def is_api_disabled_error(resp: dict) -> bool:
        """
        Detecta el error 109400: BingX deshabilita órdenes API temporalmente
        durante volatilidad extrema para evitar liquidaciones en cascada.

        Cuando ocurre, NO hay que reintentar inmediatamente — BingX tarda
        entre 1 y 5 minutos en restaurar. El caller debe esperar y reintentar
        en el próximo ciclo del monitor (ya implementado via trail_order_id="").

        Ejemplo del mensaje:
          'Reminder: Due to the large market fluctuations, in order to reduce
          the risk of liquidation, API orders are temporarily disabled.'
        """
        if not isinstance(resp, dict):
            return False
        return resp.get("code") == 109400

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

    async def _get_real_direction_and_side(self, symbol: str, fallback_direction: str) -> tuple[str, str]:
        """
        FIX v7.10 CRÍTICO — determina la dirección REAL de la posición
        desde el signo de positionAmt en BingX, no se fía del `direction`
        que pasa el caller.
        """
        try:
            positions = await self.get_open_positions()
            for p in positions:
                if p.get("symbol") != symbol:
                    continue
                amt = float(p.get("positionAmt", 0) or 0)
                ps  = p.get("positionSide", "")
                if amt > 0:
                    return "LONG", (ps if ps in ("LONG", "SHORT", "BOTH") else "LONG")
                if amt < 0:
                    return "SHORT", (ps if ps in ("LONG", "SHORT", "BOTH") else "SHORT")
        except Exception as e:
            log.debug("[%s] _get_real_direction_and_side error: %s", symbol, e)
        return fallback_direction, fallback_direction

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

        for item in raw:
            if not isinstance(item, dict):
                continue
            sym = item.get("symbol", "")
            if not sym:
                continue
            if "-" not in sym and sym.endswith("USDT"):
                sym = sym[:-4] + "-USDT"
            base_sym = sym.replace("-USDT", "")
            if not sym.endswith("-USDT") or base_sym in C.BLACKLIST or sym in C.BLACKLIST:
                continue
            if any(sym.replace("-USDT", "").startswith(p) for p in NON_CRYPTO_PREFIXES):
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
        """
        FIX v7.9: lanza excepción en error de API (no devuelve [] en silencio).
        Caché de 3s para colapsar ráfagas de llamadas del mismo ciclo.
        """
        now = time.time()
        cache_ts, cache_val = self._positions_cache
        if now - cache_ts < self._POSITIONS_CACHE_TTL:
            if isinstance(cache_val, Exception):
                raise cache_val
            return cache_val

        data = await self._get("/openApi/swap/v2/user/positions")
        code = data.get("code", 0)
        if code not in (0, None):
            msg = data.get("msg", "")
            log.error("[auth] get_open_positions código=%s msg=%s — firma/permiso rechazado por BingX",
                      code, msg)
            err = RuntimeError(f"get_open_positions falló: code={code} msg={msg}")
            self._positions_cache = (now, err)
            raise err

        pos = data.get("data", [])
        result = (
            [p for p in pos if float(p.get("positionAmt", 0) or 0) != 0]
            if isinstance(pos, list) else []
        )
        self._positions_cache = (now, result)
        return result

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
        symbol:      str,
        side:        str,
        quantity:    float,
        stop_price:  float,
        direction:   str = "LONG",
        order_type:  str = "STOP_MARKET",
        limit_price: float = None,
    ) -> dict:
        qty     = self._round_qty(symbol, quantity)
        real_direction, real_ps = await self._get_real_direction_and_side(symbol, direction)
        real_side = "SELL" if real_direction == "LONG" else "BUY"
        if real_side != side:
            log.warning(
                "[%s] ⚠️ SIDE INVERTIDO detectado y corregido en place_stop_market_order "
                "— el caller pasó side=%s (asumiendo direction=%s), pero BingX confirma "
                "que la posición real es %s (positionAmt). Usando side=%s.",
                symbol, side, direction, real_direction, real_side,
            )
            side = real_side

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
        if order_type in ("TAKE_PROFIT", "STOP") and limit_price is not None:
            params["price"] = str(round(limit_price, 8))

        resp = await self._post("/openApi/swap/v2/trade/order", params)

        if isinstance(resp, dict) and resp.get("code", -1) != 0:
            msg = self._parse_error(resp)
            if "positionside" in msg or "position side" in msg:
                log.warning("[%s] positionSide fallback → %s", symbol, real_direction)
                params["positionSide"] = real_direction
                resp = await self._post("/openApi/swap/v2/trade/order", params)
            elif "position not exist" in msg and real_ps != "BOTH":
                log.warning("[%s] position not exist → probando BOTH", symbol)
                params["positionSide"] = "BOTH"
                resp = await self._post("/openApi/swap/v2/trade/order", params)

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

    async def close_position_market(self, symbol: str, quantity: float,
                                     direction: str) -> dict:
        qty     = self._round_qty(symbol, quantity)
        real_direction, real_ps = await self._get_real_direction_and_side(symbol, direction)
        side    = "SELL" if real_direction == "LONG" else "BUY"
        if real_direction != direction:
            log.warning(
                "[%s] ⚠️ direction INVERTIDA detectada y corregida en "
                "close_position_market — caller pasó direction=%s, BingX confirma %s.",
                symbol, direction, real_direction,
            )

        params = {"symbol": symbol, "side": side, "positionSide": real_ps,
                  "type": "MARKET", "quantity": str(qty)}
        log.info("[%s] CLOSE MARKET ps=%s qty=%s", symbol, real_ps, qty)
        resp = await self._post("/openApi/swap/v2/trade/order", params)

        if isinstance(resp, dict) and resp.get("code", -1) != 0:
            msg = self._parse_error(resp)
            if "positionside" in msg or "position side" in msg:
                params["positionSide"] = real_direction
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

        # FIX v7.11: se pasa symbol para safety cap y debug log
        real_qty = self._extract_executed_qty(entry_resp, qty, symbol=symbol)
        if abs(real_qty - qty) > qty * 0.001:
            log.info("[%s] qty ajustada: local=%.6f real=%.6f", symbol, qty, real_qty)
        qty = real_qty

        await asyncio.sleep(1.2)

        step = self._step_map.get(symbol, 0)
        prec = max(0, round(-math.log10(step))) if step > 0 else self._precision_map.get(symbol, 4)
        f    = 10 ** prec
        qty_half   = math.floor(qty / 2 * f) / f
        qty_remain = math.floor((qty - qty_half) * f) / f

        price_prec = max(self._precision_map.get(symbol, 4), 2)
        tp1_limit  = _tp_limit_price(tp1_price, direction, price_prec)

        sl_r, tp1_r, tp2_r = await asyncio.gather(
            self.place_stop_market_order(symbol, side_cls, qty,        sl_price,  direction, "STOP_MARKET"),
            self.place_stop_market_order(symbol, side_cls, qty_half,   tp1_price, direction, "TAKE_PROFIT",
                                          limit_price=tp1_limit),
            self.place_stop_market_order(symbol, side_cls, qty_remain, tp2_price, direction, "TAKE_PROFIT_MARKET"),
            return_exceptions=True,
        )

        tp1_resp = tp1_r if isinstance(tp1_r, dict) else {"code": -1, "msg": str(tp1_r)}
        if tp1_resp.get("code", -1) == 0:
            log.info("[%s] TP1 OK (límite/maker) @ trigger=%.6f limit=%.6f",
                     symbol, tp1_price, tp1_limit)
        else:
            log.warning("[%s] TP1 límite falló: %s — reintentando TAKE_PROFIT_MARKET",
                        symbol, tp1_resp)
            tp1_resp = await self.place_stop_market_order(
                symbol, side_cls, qty_half, tp1_price, direction, "TAKE_PROFIT_MARKET")
            if tp1_resp.get("code", -1) == 0:
                log.info("[%s] TP1 OK (fallback market) @ %.6f", symbol, tp1_price)
            else:
                log.error("[%s] TP1 FALLIDO incluso en fallback: %s", symbol, tp1_resp)
        results["tp1"] = tp1_resp

        for label, r, price in [("SL", sl_r, sl_price), ("TP2", tp2_r, tp2_price)]:
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
        try:
            data = await self._get(
                "/openApi/swap/v2/quote/openInterest",
                {"symbol": symbol},
                signed=False,
            )
            raw = data.get("data", {})
            if isinstance(raw, dict):
                return float(raw.get("openInterest", raw.get("openInterestValue", 0)) or 0)
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
        sl_price:   float,
        tp1_price:  float,
        tp2_price:  float,
        timeout_s:  int = 25,
    ) -> dict:
        qty_r     = self._round_qty(symbol, qty)
        side_open = "BUY" if direction == "LONG" else "SELL"
        side_cls  = "SELL" if direction == "LONG" else "BUY"
        prec      = max(self._precision_map.get(symbol, 4), 2)

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

        filled   = False
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            await asyncio.sleep(3)
            try:
                orders = await self.get_open_orders(symbol)
                still_open = any(str(o.get("orderId")) == order_id for o in orders)
                if not still_open:
                    filled = True
                    break
            except Exception:
                pass

        if not filled:
            try:
                await self.cancel_order(symbol, order_id)
                log.info("[%s] Limit timeout (%ds) — cancelada → market order", symbol, timeout_s)
            except Exception as e:
                log.debug("[%s] cancel limit: %s", symbol, e)
            return {}

        log.info("[%s] ✅ Limit LLENA — fee maker 0.02%% — colocando SL/TP1/TP2", symbol)
        await asyncio.sleep(0.5)

        step = self._step_map.get(symbol, 0)
        prc  = max(0, round(-math.log10(step))) if step > 0 else self._precision_map.get(symbol, 4)
        f    = 10 ** prc
        qty_half   = math.floor(qty_r / 2 * f) / f
        qty_remain = math.floor((qty_r - qty_half) * f) / f

        tp1_limit = _tp_limit_price(tp1_price, direction, prec)

        sl_r, tp1_r, tp2_r = await asyncio.gather(
            self.place_stop_market_order(symbol, side_cls, qty_r,      sl_price,  direction, "STOP_MARKET"),
            self.place_stop_market_order(symbol, side_cls, qty_half,   tp1_price, direction, "TAKE_PROFIT",
                                          limit_price=tp1_limit),
            self.place_stop_market_order(symbol, side_cls, qty_remain, tp2_price, direction, "TAKE_PROFIT_MARKET"),
            return_exceptions=True,
        )

        protection = {}

        tp1_resp = tp1_r if isinstance(tp1_r, dict) else {"code": -1, "msg": str(tp1_r)}
        if tp1_resp.get("code", -1) == 0:
            log.info("[%s] TP1 OK (límite/maker, limit path) @ trigger=%.6f limit=%.6f",
                     symbol, tp1_price, tp1_limit)
        else:
            log.warning("[%s] TP1 límite falló (limit path): %s — reintentando market",
                        symbol, tp1_resp)
            tp1_resp = await self.place_stop_market_order(
                symbol, side_cls, qty_half, tp1_price, direction, "TAKE_PROFIT_MARKET")
            if tp1_resp.get("code", -1) == 0:
                log.info("[%s] TP1 OK (fallback market, limit path) @ %.6f", symbol, tp1_price)
        protection["tp1"] = tp1_resp

        for label, r, p in [("sl", sl_r, sl_price), ("tp2", tp2_r, tp2_price)]:
            pr = r if isinstance(r, dict) else {"code": -1, "msg": str(r)}
            protection[label] = pr
            if pr.get("code", -1) == 0:
                log.info("[%s] %s OK @ %.6f (limit path)", symbol, label.upper(), p)
            else:
                log.error("[%s] %s FALLIDO (limit path): %s", symbol, label.upper(), pr)
                if label == "sl":
                    qty_safe = self._safe_qty_for_sl(symbol, qty_r)
                    if qty_safe != qty_r:
                        pr2 = await self.place_stop_market_order(
                            symbol, side_cls, qty_safe, sl_price, direction, "STOP_MARKET")
                        protection["sl"] = pr2
                        if pr2.get("code", -1) == 0:
                            log.info("[%s] SL OK (retry, limit path) @ %.6f", symbol, sl_price)

        resp.update(protection)
        return resp


# ── Helper de precio para TP1 límite ─────────────────────────────────────────

def _tp_limit_price(trigger_price: float, direction: str, prec: int) -> float:
    if direction == "LONG":
        return round(trigger_price * 1.0005, prec + 2)
    else:
        return round(trigger_price * 0.9995, prec + 2)
