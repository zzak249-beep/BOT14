"""
Cliente mínimo de BingX (USDT-M perpetuos).

Solo lo que el bot necesita: listar símbolos, bajar velas, consultar
saldo y enviar órdenes. Nada más — cada endpoint extra es superficie
que hay que mantener.

AVISO: los endpoints de BingX cambian de versión de vez en cuando. Si
algo devuelve 404 o un código raro, contrasta con la documentación
oficial antes de tocar la lógica del bot: casi siempre es la ruta, no
el código.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
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

    # ── firma ─────────────────────────────────────────────────────────
    def _sign(self, params: dict[str, Any]) -> str:
        query = urlencode(params)
        return hmac.new(
            config.BINGX_API_SECRET.encode(), query.encode(), hashlib.sha256
        ).hexdigest()

    async def _public(self, path: str, params: dict[str, Any] | None = None) -> Any:
        r = await self._c.get(f"{self._base}{path}", params=params or {}, timeout=20)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and data.get("code") not in (0, None, "0"):
            raise BingXError(f"{path} -> code={data.get('code')} msg={data.get('msg')}")
        return data.get("data", data) if isinstance(data, dict) else data

    async def _private(self, method: str, path: str, params: dict[str, Any] | None = None) -> Any:
        p = dict(params or {})
        p["timestamp"] = int(time.time() * 1000)
        p["signature"] = self._sign(p)
        headers = {"X-BX-APIKEY": config.BINGX_API_KEY}
        url = f"{self._base}{path}"
        if method == "GET":
            r = await self._c.get(url, params=p, headers=headers, timeout=20)
        else:
            r = await self._c.post(url, params=p, headers=headers, timeout=20)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and data.get("code") not in (0, None, "0"):
            raise BingXError(f"{path} -> code={data.get('code')} msg={data.get('msg')}")
        return data.get("data", data) if isinstance(data, dict) else data

    # ── público ───────────────────────────────────────────────────────
    async def symbols(self) -> list[str]:
        """Además de la lista, guarda la PRECISIÓN de cada símbolo."""
        data = await self._public("/openApi/swap/v2/quote/contracts")
        out: list[str] = []
        for item in data or []:
            sym = str(item.get("symbol", ""))
            if not sym.endswith("-USDT"):
                continue
            base = sym.split("-")[0].upper()
            if any(base.startswith(pref) for pref in config.EXCLUDE_PREFIXES):
                continue
            out.append(sym)
            # BingX RECHAZA las órdenes con más decimales de los que
            # admite el contrato. Sin esto, en LIVE la primera orden se
            # rechaza y el bot parece roto sin estarlo: manda una
            # cantidad como 13847.293847 donde solo se aceptan enteros.
            self._precision[sym] = {
                "qty": int(item.get("quantityPrecision", 4) or 0),
                "price": int(item.get("pricePrecision", 6) or 0),
                "min_qty": float(item.get("tradeMinQuantity", 0) or 0),
            }
        return out

    def round_qty(self, symbol: str, qty: float) -> float:
        p = self._precision.get(symbol, {})
        return round(qty, int(p.get("qty", 4)))

    def round_price(self, symbol: str, price: float) -> float:
        p = self._precision.get(symbol, {})
        return round(price, int(p.get("price", 6)))

    def min_qty(self, symbol: str) -> float:
        return float(self._precision.get(symbol, {}).get("min_qty", 0.0))

    async def tickers_24h(self) -> dict[str, float]:
        """Volumen de 24h en USDT por símbolo, en UNA llamada."""
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

    async def klines(self, symbol: str, interval: str, limit: int = 300) -> list[dict]:
        data = await self._public(
            "/openApi/swap/v3/quote/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
        )
        rows: list[dict] = []
        for k in data or []:
            # BingX devuelve dicts o listas según versión; se aceptan ambos.
            if isinstance(k, dict):
                rows.append(
                    {
                        "time": int(k.get("time", 0)),
                        "open": float(k["open"]),
                        "high": float(k["high"]),
                        "low": float(k["low"]),
                        "close": float(k["close"]),
                        "volume": float(k.get("volume", 0)),
                    }
                )
            else:
                rows.append(
                    {
                        "time": int(k[0]),
                        "open": float(k[1]),
                        "high": float(k[2]),
                        "low": float(k[3]),
                        "close": float(k[4]),
                        "volume": float(k[5]),
                    }
                )
        rows.sort(key=lambda r: r["time"])
        return rows

    # ── privado ───────────────────────────────────────────────────────
    async def balance_usdt(self) -> float:
        data = await self._private("GET", "/openApi/swap/v2/user/balance")
        if isinstance(data, dict):
            bal = data.get("balance", data)
            if isinstance(bal, dict):
                return float(bal.get("availableMargin", bal.get("balance", 0)) or 0)
        return 0.0

    async def set_margin_mode(self, symbol: str, modo: str = "ISOLATED") -> None:
        """
        Fija el modo de margen del símbolo.

        AISLADO por defecto, y no es un detalle: con margen CRUZADO toda
        la cuenta respalda cada posición, así que una cascada puede
        liquidar el saldo entero antes de que salte el stop. Con varios
        bots compartiendo cuenta el problema se multiplica — una
        posición que amenaza liquidación consume balance común y acerca
        a las otras a la suya, aunque estén yendo bien.

        Si el símbolo ya está en ese modo, BingX devuelve error y se
        ignora: no es un fallo, es que ya estaba puesto.
        """
        try:
            await self._private("POST", "/openApi/swap/v2/trade/marginType",
                                {"symbol": symbol, "marginType": modo})
        except Exception as exc:  # noqa: BLE001
            log.debug("%s: margen ya en %s o no se pudo cambiar (%s)", symbol, modo, exc)

    async def set_leverage(self, symbol: str, side: str, leverage: int) -> None:
        await self._private(
            "POST",
            "/openApi/swap/v2/trade/leverage",
            {"symbol": symbol, "side": side, "leverage": leverage},
        )

    async def market_order(
        self, symbol: str, side: str, quantity: float, sl: float, tp: float,
        client_id: str | None = None
    ) -> dict:
        """
        side: 'BUY' (largo) o 'SELL' (corto).
        El stop y el objetivo van EN LA MISMA orden: si se enviaran
        después, una desconexión entre medias dejaría una posición sin
        protección — que es la forma más tonta de perder una cuenta.
        """
        position_side = "LONG" if side == "BUY" else "SHORT"
        params = {
            "symbol": symbol,
            "side": side,
            "positionSide": position_side,
            "type": "MARKET",
            "quantity": quantity,
            # Identificador propio: permite comprobar DESPUÉS si la orden
            # existe cuando la respuesta se pierde por el camino.
            "clientOrderID": client_id or f"rev{int(time.time()*1000)}",
            "stopLoss": (
                '{"type":"STOP_MARKET","stopPrice":%s,"workingType":"MARK_PRICE"}' % sl
            ),
            "takeProfit": (
                '{"type":"TAKE_PROFIT_MARKET","stopPrice":%s,"workingType":"MARK_PRICE"}' % tp
            ),
        }
        return await self._private("POST", "/openApi/swap/v2/trade/order", params)

    async def limit_order(
        self, symbol: str, side: str, quantity: float, price: float, sl: float, tp: float,
        client_id: str | None = None
    ) -> dict:
        """
        Entrada con precio límite. Puede no ejecutarse, y eso es
        deliberado: en un libro fino, no entrar es mejor que entrar al
        precio que le quede al libro.
        """
        position_side = "LONG" if side == "BUY" else "SHORT"
        return await self._private(
            "POST",
            "/openApi/swap/v2/trade/order",
            {
                "symbol": symbol,
                "side": side,
                "positionSide": position_side,
                "type": "LIMIT",
                "price": price,
                "quantity": quantity,
                "timeInForce": "GTC",
                "clientOrderID": client_id or f"rev{int(time.time()*1000)}",
                "stopLoss": '{"type":"STOP_MARKET","stopPrice":%s,"workingType":"MARK_PRICE"}' % sl,
                "takeProfit": '{"type":"TAKE_PROFIT_MARKET","stopPrice":%s,"workingType":"MARK_PRICE"}' % tp,
            },
        )

    async def cancel_open_orders(self, symbol: str) -> dict:
        return await self._private(
            "POST", "/openApi/swap/v2/trade/allOpenOrders", {"symbol": symbol}
        )

    async def close_position(self, symbol: str, side: str, quantity: float) -> dict:
        """
        Cierra a mercado. side es el lado ORIGINAL de la posición: para
        salir de un largo se vende, y al revés.
        """
        exit_side = "SELL" if side == "BUY" else "BUY"
        position_side = "LONG" if side == "BUY" else "SHORT"
        return await self._private(
            "POST",
            "/openApi/swap/v2/trade/order",
            {
                "symbol": symbol,
                "side": exit_side,
                "positionSide": position_side,
                "type": "MARKET",
                "quantity": quantity,
            },
        )

    async def open_orders(self, symbol: str) -> list[dict]:
        data = await self._private("GET", "/openApi/swap/v2/trade/openOrders", {"symbol": symbol})
        if isinstance(data, dict):
            data = data.get("orders", [])
        return data if isinstance(data, list) else []

    async def order_exists(self, symbol: str, client_id: str) -> bool:
        """
        ¿Existe esta orden en el exchange?

        Se llama cuando el envío falló por red: la petición pudo llegar
        igualmente y la respuesta perderse. Sin esta comprobación, el
        bot da por fallida una orden que SÍ existe — y luego abre otra.
        """
        try:
            for o in await self.open_orders(symbol):
                if str(o.get("clientOrderID") or o.get("clientOrderId") or "") == client_id:
                    return True
        except Exception:  # noqa: BLE001
            pass
        try:
            for p in await self.open_positions():
                if str(p.get("symbol")) == symbol and float(p.get("positionAmt", 0) or 0) != 0:
                    return True
        except Exception:  # noqa: BLE001
            pass
        return False

    async def open_positions(self) -> list[dict]:
        data = await self._private("GET", "/openApi/swap/v2/user/positions")
        return data if isinstance(data, list) else []
