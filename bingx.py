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


def _fmt(value: float, precision: int) -> str:
    """
    Formatea con decimales FIJOS, nunca notación científica.

    BUG QUE ESTO EVITA: al pasar un float crudo en los parámetros de la
    petición, tanto el %-format de Python como httpx lo convierten con
    str(), que para valores pequeños usa notación científica —
    str(0.0000012) -> '1.2e-06'. BingX no acepta ese formato en precio
    ni en cantidad, y es justo lo que le pasa a las monedas micro-cap
    de precio muy bajo, que son buena parte del universo que escanea
    este bot. El error llega silencioso: la API rechaza la orden con
    un código, no con un traceback, así que sin vigilar los logs de
    "BingX rechazó la orden" pasa desapercibido.
    """
    return f"{value:.{max(precision, 0)}f}"


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

    def fmt_qty(self, symbol: str, qty: float) -> str:
        p = self._precision.get(symbol, {})
        return _fmt(qty, int(p.get("qty", 4)))

    def fmt_price(self, symbol: str, price: float) -> str:
        p = self._precision.get(symbol, {})
        return _fmt(price, int(p.get("price", 6)))

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

    async def set_leverage(self, symbol: str, side: str, leverage: int) -> None:
        await self._private(
            "POST",
            "/openApi/swap/v2/trade/leverage",
            {"symbol": symbol, "side": side, "leverage": leverage},
        )

    async def market_order(
        self, symbol: str, side: str, quantity: float, sl: float, tp: float
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
            "quantity": self.fmt_qty(symbol, quantity),
            "stopLoss": (
                '{"type":"STOP_MARKET","stopPrice":%s,"workingType":"MARK_PRICE"}'
                % self.fmt_price(symbol, sl)
            ),
            "takeProfit": (
                '{"type":"TAKE_PROFIT_MARKET","stopPrice":%s,"workingType":"MARK_PRICE"}'
                % self.fmt_price(symbol, tp)
            ),
        }
        return await self._private("POST", "/openApi/swap/v2/trade/order", params)

    async def limit_order(
        self, symbol: str, side: str, quantity: float, price: float, sl: float, tp: float
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
                "price": self.fmt_price(symbol, price),
                "quantity": self.fmt_qty(symbol, quantity),
                "timeInForce": "GTC",
                "stopLoss": '{"type":"STOP_MARKET","stopPrice":%s,"workingType":"MARK_PRICE"}'
                % self.fmt_price(symbol, sl),
                "takeProfit": '{"type":"TAKE_PROFIT_MARKET","stopPrice":%s,"workingType":"MARK_PRICE"}'
                % self.fmt_price(symbol, tp),
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
                "quantity": self.fmt_qty(symbol, quantity),
            },
        )

    async def open_positions(self) -> list[dict]:
        data = await self._private("GET", "/openApi/swap/v2/user/positions")
        return data if isinstance(data, list) else []

    async def open_interest(self, symbol: str) -> float | None:
        """
        Foto ACTUAL del Open Interest — BingX no da serie histórica en
        este endpoint (confirmado contra su esquema real: solo devuelve
        openInterest/symbol/time). Para saber si SUBÍA o BAJABA durante
        el estirón hay que muestrear esto nosotros mismos y guardar nuestro
        propio historial corto — eso es lo que hace OpenInterestTracker
        en oi_confirm.py, exactamente igual que liquidations.py construye
        su propia ventana a partir de streams que tampoco dan histórico.
        """
        try:
            data = await self._public("/openApi/swap/v2/quote/openInterest", {"symbol": symbol})
        except Exception:  # noqa: BLE001
            return None
        try:
            valor = data.get("openInterest") if isinstance(data, dict) else None
            return float(valor) if valor is not None else None
        except (TypeError, ValueError):
            return None

    async def spread_pct(self, symbol: str) -> float | None:
        """
        Spread bid-ask actual, en % del precio medio. None si no se pudo
        leer el libro — no bloquea la entrada por un fallo de red, solo
        cuando SÍ hay dato y el spread es ancho de verdad.

        POR QUÉ HACE FALTA: el volumen de 24h (MIN_QUOTE_VOLUME_24H) es
        una foto de todo el día, no del libro AHORA MISMO. Un símbolo con
        volumen suficiente puede tener el libro vacío en el instante de
        la señal — típico justo después del estirón que dispara esta
        estrategia, cuando el flujo forzado ya se llevó la liquidez
        cercana. Sin este chequeo, una entrada a mercado en ese momento
        paga el spread completo, no el que viste en el ranking.
        """
        try:
            data = await self._public(
                "/openApi/swap/v2/quote/depth", {"symbol": symbol, "limit": 5}
            )
        except Exception:  # noqa: BLE001
            return None
        try:
            bids = data.get("bids") if isinstance(data, dict) else None
            asks = data.get("asks") if isinstance(data, dict) else None
            if not bids or not asks:
                return None
            mejor_bid = float(bids[0][0])
            mejor_ask = float(asks[0][0])
            if mejor_bid <= 0 or mejor_ask <= 0:
                return None
            medio = (mejor_bid + mejor_ask) / 2.0
            return (mejor_ask - mejor_bid) / medio * 100.0
        except (TypeError, ValueError, IndexError, KeyError):
            return None
