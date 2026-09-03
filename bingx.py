"""
Cliente mínimo de BingX (USDT-M perpetuos).

═══════════════════════════════════════════════════════════════════════
LO QUE SE HA REPARADO AQUÍ
═══════════════════════════════════════════════════════════════════════
1. FIRMA. Antes se firmaba `urlencode(params)` y luego se pasaba el
   dict a httpx con `params=`, dejando que httpx volviera a serializar.
   Con órdenes normales coincide por casualidad; con stopLoss/takeProfit
   —que llevan JSON con llaves y comillas— el escapado puede diferir y
   BingX rechaza la firma. Ahora la cadena se construye UNA vez y esa
   misma cadena se firma y se envía.

2. recvWindow. Su ausencia produce rechazos intermitentes por deriva de
   reloj, que es la peor clase de fallo: parece aleatorio.

3. MODO DE POSICIÓN. Si la cuenta está en modo unidireccional y se manda
   positionSide=LONG, BingX rechaza TODAS las órdenes. Ahora se detecta
   una vez y se usa BOTH cuando toca.

4. SALDO. Antes se leía solo `availableMargin` y solo si venía como
   dict. En una cuenta compartida con otros bots el margen disponible
   puede ser casi cero mientras el capital sigue ahí, y el tamaño salía
   0 -> "sin ejecutar: saldo 0". Ahora se devuelve capital y disponible
   por separado: el capital dimensiona, el disponible se comprueba.

5. REDONDEO A LA BAJA. round() puede subir la cantidad por encima de lo
   que el margen permite. Para cantidades siempre se trunca.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import math
import time
from typing import Any
from urllib.parse import urlencode

import httpx

import config

log = logging.getLogger("bingx")


class BingXError(RuntimeError):
    pass


class BingX:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._c = client
        self._base = config.BINGX_BASE_URL.rstrip("/")
        self._precision: dict[str, dict] = {}
        self._dual: bool | None = None

    # ── firma ─────────────────────────────────────────────────────────
    def _signed_query(self, params: dict[str, Any] | None) -> str:
        p = {k: v for k, v in (params or {}).items() if v is not None}
        p["timestamp"] = int(time.time() * 1000)
        p["recvWindow"] = config.RECV_WINDOW
        query = urlencode(sorted(p.items()))
        firma = hmac.new(
            config.BINGX_API_SECRET.encode(), query.encode(), hashlib.sha256
        ).hexdigest()
        return f"{query}&signature={firma}"

    @staticmethod
    def _desempaquetar(path: str, data: Any) -> Any:
        if isinstance(data, dict):
            code = data.get("code")
            if code not in (0, None, "0"):
                raise BingXError(f"{path} -> code={code} msg={data.get('msg')}")
            return data.get("data", data)
        return data

    async def _public(self, path: str, params: dict[str, Any] | None = None) -> Any:
        r = await self._c.get(f"{self._base}{path}", params=params or {}, timeout=20)
        r.raise_for_status()
        return self._desempaquetar(path, r.json())

    async def _private(self, method: str, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self._base}{path}?{self._signed_query(params)}"
        headers = {"X-BX-APIKEY": config.BINGX_API_KEY}
        r = await self._c.request(method, url, headers=headers, timeout=25)
        r.raise_for_status()
        return self._desempaquetar(path, r.json())

    # ── modo de posición ──────────────────────────────────────────────
    async def dual_mode(self) -> bool:
        if config.POSITION_MODE == "HEDGE":
            return True
        if config.POSITION_MODE in ("ONEWAY", "ONE_WAY", "UNIDIRECCIONAL"):
            return False
        if self._dual is None:
            try:
                d = await self._private("GET", "/openApi/swap/v1/positionSide/dual")
                v = d.get("dualSidePosition") if isinstance(d, dict) else None
                self._dual = str(v).strip().lower() in ("true", "1")
                log.info("Modo de posición detectado: %s", "cobertura" if self._dual else "unidireccional")
            except Exception as exc:  # noqa: BLE001
                log.warning("No se pudo leer el modo de posición (%s): se asume cobertura", exc)
                self._dual = True
        return bool(self._dual)

    async def position_side(self, side: str) -> str:
        if await self.dual_mode():
            return "LONG" if side == "BUY" else "SHORT"
        return "BOTH"

    # ── público ───────────────────────────────────────────────────────
    async def symbols(self) -> list[str]:
        data = await self._public("/openApi/swap/v2/quote/contracts")
        out: list[str] = []
        for item in data or []:
            sym = str(item.get("symbol", ""))
            if not sym.endswith("-USDT"):
                continue
            estado = str(item.get("status", 1))
            if estado not in ("1", "None", ""):
                continue  # contrato en mantenimiento o retirado
            base = sym.split("-")[0].upper()
            if any(base.startswith(pref) for pref in config.EXCLUDE_PREFIXES):
                continue
            out.append(sym)
            self._precision[sym] = {
                "qty": int(item.get("quantityPrecision", 4) or 0),
                "price": int(item.get("pricePrecision", 6) or 0),
                "min_qty": float(item.get("tradeMinQuantity", 0) or 0),
                "min_usdt": float(item.get("tradeMinUSDT", 0) or 0),
            }
        return out

    def _prec(self, symbol: str, clave: str, defecto: int) -> int:
        return int(self._precision.get(symbol, {}).get(clave, defecto))

    def floor_qty(self, symbol: str, qty: float) -> float:
        d = self._prec(symbol, "qty", 4)
        f = 10 ** d
        return math.floor(qty * f) / f

    def ceil_qty(self, symbol: str, qty: float) -> float:
        d = self._prec(symbol, "qty", 4)
        f = 10 ** d
        return math.ceil(qty * f) / f

    def round_qty(self, symbol: str, qty: float) -> float:
        return self.floor_qty(symbol, qty)

    def round_price(self, symbol: str, price: float) -> float:
        return round(price, self._prec(symbol, "price", 6))

    def min_qty(self, symbol: str) -> float:
        return float(self._precision.get(symbol, {}).get("min_qty", 0.0) or 0.0)

    def min_notional(self, symbol: str) -> float:
        v = float(self._precision.get(symbol, {}).get("min_usdt", 0.0) or 0.0)
        return v if v > 0 else config.MIN_NOTIONAL_USDT

    async def tickers_24h(self) -> dict[str, float]:
        data = await self._public("/openApi/swap/v2/quote/ticker")
        out: dict[str, float] = {}
        for t in data or []:
            sym = str(t.get("symbol", ""))
            vol = t.get("quoteVolume") or t.get("turnover") or 0
            try:
                out[sym] = float(vol)
            except (TypeError, ValueError):
                out[sym] = 0.0
        return out

    async def funding_rates(self) -> dict[str, float]:
        """Tasa actual por símbolo en %. El funding es contexto: si el
        endpoint cambia, devuelve vacío y el bot sigue operando."""
        try:
            data = await self._public("/openApi/swap/v2/quote/premiumIndex")
        except Exception as exc:  # noqa: BLE001
            log.warning("No se pudo leer el funding: %s", exc)
            return {}
        out: dict[str, float] = {}
        for item in data or []:
            sym = str(item.get("symbol", ""))
            try:
                out[sym] = float(item.get("lastFundingRate", 0) or 0) * 100.0
            except (TypeError, ValueError):
                continue
        return out

    async def klines(self, symbol: str, interval: str, limit: int = 300) -> list[dict]:
        data = await self._public(
            "/openApi/swap/v3/quote/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
        )
        rows: list[dict] = []
        for k in data or []:
            try:
                if isinstance(k, dict):
                    rows.append({
                        "time": int(k.get("time", 0)),
                        "open": float(k["open"]), "high": float(k["high"]),
                        "low": float(k["low"]), "close": float(k["close"]),
                        "volume": float(k.get("volume", 0)),
                    })
                else:
                    rows.append({
                        "time": int(k[0]), "open": float(k[1]), "high": float(k[2]),
                        "low": float(k[3]), "close": float(k[4]), "volume": float(k[5]),
                    })
            except (KeyError, IndexError, TypeError, ValueError):
                continue
        rows.sort(key=lambda r: r["time"])
        return rows

    # ── privado ───────────────────────────────────────────────────────
    async def balance(self) -> dict[str, float]:
        """
        Devuelve capital y margen disponible por separado.

        El capital dimensiona la operación; el disponible decide si cabe.
        Confundirlos es lo que produce "saldo 0" en una cuenta que sí
        tiene dinero pero con el margen ocupado por otro bot.
        """
        vacio = {"equity": 0.0, "balance": 0.0, "available": 0.0, "used": 0.0}
        try:
            data = await self._private("GET", "/openApi/swap/v2/user/balance")
        except Exception as exc:  # noqa: BLE001
            log.error("No se pudo leer el saldo: %s", exc)
            return vacio

        candidatos: list[dict] = []
        if isinstance(data, dict):
            b = data.get("balance", data)
            if isinstance(b, dict):
                candidatos = [b]
            elif isinstance(b, list):
                candidatos = [x for x in b if isinstance(x, dict)]
        elif isinstance(data, list):
            candidatos = [x for x in data if isinstance(x, dict)]

        for item in candidatos:
            if str(item.get("asset", "USDT")).upper() not in ("USDT", ""):
                continue

            def f(k: str) -> float:
                try:
                    return float(item.get(k, 0) or 0)
                except (TypeError, ValueError):
                    return 0.0

            equity = f("equity") or f("balance") or f("walletBalance")
            disponible = f("availableMargin") or f("availableBalance") or equity
            return {"equity": equity, "balance": f("balance"),
                    "available": disponible, "used": f("usedMargin")}
        return vacio

    async def balance_usdt(self) -> float:
        return (await self.balance())["equity"]

    async def set_margin_mode(self, symbol: str, modo: str = "ISOLATED") -> None:
        """AISLADO: con cruzado toda la cuenta respalda cada posición y
        una cascada puede liquidarla antes de que salte el stop. Si ya
        estaba en ese modo BingX devuelve error y se ignora."""
        try:
            await self._private("POST", "/openApi/swap/v2/trade/marginType",
                                {"symbol": symbol, "marginType": modo})
        except Exception as exc:  # noqa: BLE001
            log.debug("%s: margen ya en %s o no se pudo cambiar (%s)", symbol, modo, exc)

    async def set_leverage(self, symbol: str, side: str, leverage: int) -> None:
        lado = await self.position_side(side)
        try:
            await self._private("POST", "/openApi/swap/v2/trade/leverage",
                                {"symbol": symbol, "side": lado, "leverage": leverage})
        except Exception as exc:  # noqa: BLE001
            # Apalancamiento ya fijado, o el símbolo no admite ese valor:
            # no es motivo para no operar, pero sí para dejar rastro.
            log.warning("%s: no se pudo fijar apalancamiento x%s (%s)", symbol, leverage, exc)

    def _sl_tp(self, sl: float, tp: float) -> dict:
        d = {}
        if sl and sl > 0:
            d["stopLoss"] = '{"type":"STOP_MARKET","stopPrice":%s,"workingType":"MARK_PRICE"}' % sl
        if tp and tp > 0:
            d["takeProfit"] = '{"type":"TAKE_PROFIT_MARKET","stopPrice":%s,"workingType":"MARK_PRICE"}' % tp
        return d

    async def market_order(self, symbol: str, side: str, quantity: float,
                           sl: float, tp: float, client_id: str | None = None) -> dict:
        """El stop y el objetivo van EN LA MISMA orden: enviarlos después
        deja una ventana en la que una desconexión te quita la protección."""
        params = {
            "symbol": symbol, "side": side,
            "positionSide": await self.position_side(side),
            "type": "MARKET", "quantity": quantity,
            "clientOrderID": client_id or f"wav{int(time.time()*1000)}",
        }
        params.update(self._sl_tp(sl, tp))
        return await self._private("POST", "/openApi/swap/v2/trade/order", params)

    async def limit_order(self, symbol: str, side: str, quantity: float, price: float,
                          sl: float, tp: float, client_id: str | None = None) -> dict:
        params = {
            "symbol": symbol, "side": side,
            "positionSide": await self.position_side(side),
            "type": "LIMIT", "price": price, "quantity": quantity,
            "timeInForce": "GTC",
            "clientOrderID": client_id or f"wav{int(time.time()*1000)}",
        }
        params.update(self._sl_tp(sl, tp))
        return await self._private("POST", "/openApi/swap/v2/trade/order", params)

    async def cancel_open_orders(self, symbol: str) -> dict:
        return await self._private("DELETE", "/openApi/swap/v2/trade/allOpenOrders",
                                   {"symbol": symbol})

    async def close_position(self, symbol: str, side: str, quantity: float) -> dict:
        """side es el lado ORIGINAL: para salir de un largo se vende."""
        exit_side = "SELL" if side == "BUY" else "BUY"
        params = {
            "symbol": symbol, "side": exit_side,
            "positionSide": await self.position_side(side),
            "type": "MARKET", "quantity": quantity,
        }
        if not await self.dual_mode():
            params["reduceOnly"] = "true"
        return await self._private("POST", "/openApi/swap/v2/trade/order", params)

    async def open_orders(self, symbol: str) -> list[dict]:
        data = await self._private("GET", "/openApi/swap/v2/trade/openOrders", {"symbol": symbol})
        if isinstance(data, dict):
            data = data.get("orders", [])
        return data if isinstance(data, list) else []

    async def open_positions(self) -> list[dict]:
        data = await self._private("GET", "/openApi/swap/v2/user/positions")
        if isinstance(data, dict):
            data = data.get("positions", [])
        return data if isinstance(data, list) else []

    @staticmethod
    def cantidad_posicion(p: dict) -> float:
        for k in ("positionAmt", "availableAmt", "positionAmount", "amount"):
            try:
                v = float(p.get(k, 0) or 0)
            except (TypeError, ValueError):
                continue
            if v:
                return v
        return 0.0

    @staticmethod
    def precio_entrada(p: dict) -> float:
        for k in ("avgPrice", "entryPrice", "averagePrice", "avgOpenPrice"):
            try:
                v = float(p.get(k, 0) or 0)
            except (TypeError, ValueError):
                continue
            if v:
                return v
        return 0.0

    async def order_exists(self, symbol: str, client_id: str) -> bool:
        """Se llama cuando el envío falló por red: la petición pudo llegar
        y perderse la respuesta. Sin esto el bot abre una segunda encima."""
        try:
            for o in await self.open_orders(symbol):
                if str(o.get("clientOrderID") or o.get("clientOrderId") or "") == client_id:
                    return True
        except Exception:  # noqa: BLE001
            pass
        try:
            for p in await self.open_positions():
                if str(p.get("symbol")) == symbol and self.cantidad_posicion(p) != 0:
                    return True
        except Exception:  # noqa: BLE001
            pass
        return False
