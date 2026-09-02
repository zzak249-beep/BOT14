"""
bingx_client.py — Cliente REST para BingX Perpetual Futures (USDT-M).

Notas de implementación importantes (aprendidas de bots anteriores en
esta misma cuenta, ver README):

1. TODO va en el query string de la URL, incluida la firma, incluso en
   peticiones POST. El body se manda siempre vacío. Si se deja que
   `requests` serialice el dict por su cuenta (params=... o data=...)
   puede re-codificarlo de forma distinta a como se firmó -> error de
   firma 100001. Por eso aquí se construye la URL final a mano y se
   pasa tal cual, sin params/data adicionales.
2. Cuenta en modo Hedge: para abrir se usa side=BUY/SELL con
   positionSide=LONG/SHORT; para cerrar se manda el side contrario
   manteniendo el mismo positionSide (NUNCA reduceOnly ni
   positionSide=BOTH).
3. Los klines se devuelven como lista de objetos con claves nombradas
   ("open","close","high","low","volume","time") y NO como arrays
   posicionales — nótese que "close" va antes que "high" en la
   respuesta real de BingX. Aquí se parsea siempre por nombre de
   clave para no repetir ese bug.
4. Se manda siempre recvWindow para evitar firmas rechazadas por
   desfases de reloj.
5. Símbolos en formato "BASE-QUOTE" (ej. "BTC-USDT"). En DEMO_MODE se
   usa el sufijo "-VST" (saldo de práctica de BingX) en vez de
   "-USDT", sobre el mismo API.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import time
from typing import Any, Optional
from urllib.parse import quote_plus

import requests

logger = logging.getLogger("wavelet_bot.bingx")

# Códigos de error de negocio de BingX que el bot necesita reconocer
# explícitamente (el resto se trata como error genérico).
ERR_SIGNATURE_FAILED = 100001
ERR_INSUFFICIENT_BALANCE = 100202
ERR_INVALID_PARAMETER = 100400
ERR_PRICE_DEVIATION = 100440
ERR_POSITION_NOT_EXIST = 109420


class BingXAPIError(Exception):
    def __init__(self, code: int, msg: str, path: str):
        self.code = code
        self.msg = msg
        self.path = path
        super().__init__(f"BingX [{path}] code={code} msg={msg}")


class BingXClient:
    def __init__(self, api_key: str, api_secret: str, base_url: str, recv_window_ms: int = 5000,
                 timeout: float = 15.0, max_retries: int = 3):
        self.api_key = api_key
        self.api_secret = api_secret.encode("utf-8")
        self.base_url = base_url.rstrip("/")
        self.recv_window_ms = recv_window_ms
        self.timeout = timeout
        self.max_retries = max_retries
        self._session = requests.Session()
        self._session.headers.update({"X-BX-APIKEY": self.api_key})

    # ── Firma ────────────────────────────────────────────────────────
    @staticmethod
    def _build_query_string(params: dict[str, Any]) -> str:
        # Orden ALFABÉTICO por clave: verificado con el vector de prueba
        # oficial de BingX (REST API.md) — con orden de inserción
        # arbitrario la firma NO coincide con la esperada por el
        # servidor. Además de firmar, esta es la misma cadena que se
        # manda, así que ambas quedan siempre consistentes entre sí.
        parts = []
        for key, value in sorted(params.items()):
            if value is None:
                continue
            if isinstance(value, bool):
                value = "true" if value else "false"
            elif isinstance(value, float):
                # format(x, "f") por defecto trunca a 6 decimales, lo que
                # PIERDE precisión real en cantidades pequeñas (monedas de
                # precio alto). 8 decimales cubre la máxima precisión real
                # de cualquier contrato BingX sin exponer ruido de coma
                # flotante binaria (con más decimales sí aparece, ej.
                # 49250.123456 -> "...000001" de más con 12 decimales).
                value = f"{value:.8f}".rstrip("0").rstrip(".")
                if value == "" or value == "-":
                    value = "0"
            parts.append(f"{key}={quote_plus(str(value))}")
        return "&".join(parts)

    def _sign(self, query_string: str) -> str:
        return hmac.new(self.api_secret, query_string.encode("utf-8"), hashlib.sha256).hexdigest()

    def _request(self, method: str, path: str, params: Optional[dict[str, Any]] = None,
                 signed: bool = True) -> Any:
        params = dict(params or {})
        if signed:
            params["timestamp"] = int(time.time() * 1000)
            params["recvWindow"] = self.recv_window_ms

        query_string = self._build_query_string(params)

        if signed:
            signature = self._sign(query_string)
            query_string = f"{query_string}&signature={signature}"

        url = f"{self.base_url}{path}"
        if query_string:
            url = f"{url}?{query_string}"

        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self._session.request(method, url, timeout=self.timeout)
                data = resp.json()
                code = data.get("code")
                if code not in (0, None):
                    raise BingXAPIError(code, data.get("msg", ""), path)
                return data.get("data", data)
            except BingXAPIError:
                raise  # error de negocio de BingX: no tiene sentido reintentar igual
            except (requests.RequestException, ValueError) as exc:
                last_exc = exc
                wait = min(2 ** attempt, 8)
                logger.warning("Fallo de red en %s (intento %d/%d): %s — reintento en %ds",
                                path, attempt, self.max_retries, exc, wait)
                time.sleep(wait)
        raise RuntimeError(f"No se pudo completar {path} tras {self.max_retries} intentos: {last_exc}")

    # ── Mercado ──────────────────────────────────────────────────────
    def get_contracts(self) -> list[dict]:
        """Lista de contratos USDT-M con precisión de cantidad/precio y mínimos."""
        return self._request("GET", "/openApi/swap/v2/quote/contracts", signed=False)

    def get_klines(self, symbol: str, interval: str, limit: int = 200) -> list[dict]:
        """
        Devuelve velas ordenadas cronológicamente ascendente (la más
        antigua primero, la más reciente al final), cada una como dict
        con claves: open, high, low, close, volume, time (ms).
        """
        raw = self._request(
            "GET", "/openApi/swap/v2/quote/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
            signed=False,
        )
        candles = [
            {
                "time": int(c["time"]),
                "open": float(c["open"]),
                "high": float(c["high"]),
                "low": float(c["low"]),
                "close": float(c["close"]),
                "volume": float(c["volume"]),
            }
            for c in raw
        ]
        candles.sort(key=lambda c: c["time"])
        return candles

    # ── Cuenta ───────────────────────────────────────────────────────
    def get_balance(self) -> dict:
        """Balance de la cuenta USDT-M (equity, disponible, etc.)."""
        data = self._request("GET", "/openApi/swap/v2/user/balance", {})
        # La respuesta trae {"balance": {...}} en algunas cuentas y
        # el dict plano en otras; se normaliza aquí.
        if isinstance(data, dict) and "balance" in data:
            return data["balance"]
        return data

    def get_positions(self, symbol: Optional[str] = None) -> list[dict]:
        params = {"symbol": symbol} if symbol else {}
        data = self._request("GET", "/openApi/swap/v2/user/positions", params)
        return data or []

    def set_leverage(self, symbol: str, side: str, leverage: int) -> None:
        """side: 'LONG' o 'SHORT' (modo Hedge)."""
        self._request("POST", "/openApi/swap/v2/trade/leverage", {
            "symbol": symbol, "side": side, "leverage": leverage,
        })

    # ── Trading ──────────────────────────────────────────────────────
    def place_market_order(self, symbol: str, side: str, position_side: str, quantity: float) -> dict:
        return self._request("POST", "/openApi/swap/v2/trade/order", {
            "symbol": symbol,
            "side": side,                # BUY / SELL
            "positionSide": position_side,  # LONG / SHORT
            "type": "MARKET",
            "quantity": quantity,
        })

    def place_stop_market(self, symbol: str, side: str, position_side: str,
                           stop_price: float, close_position: bool = True,
                           quantity: Optional[float] = None) -> dict:
        params = {
            "symbol": symbol,
            "side": side,
            "positionSide": position_side,
            "type": "STOP_MARKET",
            "stopPrice": stop_price,
        }
        if close_position:
            params["closePosition"] = True
        elif quantity is not None:
            params["quantity"] = quantity
        return self._request("POST", "/openApi/swap/v2/trade/order", params)

    def place_take_profit_market(self, symbol: str, side: str, position_side: str,
                                  stop_price: float, close_position: bool = True,
                                  quantity: Optional[float] = None) -> dict:
        params = {
            "symbol": symbol,
            "side": side,
            "positionSide": position_side,
            "type": "TAKE_PROFIT_MARKET",
            "stopPrice": stop_price,
        }
        if close_position:
            params["closePosition"] = True
        elif quantity is not None:
            params["quantity"] = quantity
        return self._request("POST", "/openApi/swap/v2/trade/order", params)

    def cancel_all_open_orders(self, symbol: str) -> dict:
        return self._request("DELETE", "/openApi/swap/v2/trade/allOpenOrders", {"symbol": symbol})

    def close_position_market(self, symbol: str, side: str, position_side: str) -> dict:
        """Cierre de emergencia a mercado (no depende de que el SL/TP se haya ejecutado)."""
        return self._request("POST", "/openApi/swap/v2/trade/order", {
            "symbol": symbol,
            "side": side,
            "positionSide": position_side,
            "type": "MARKET",
            "closePosition": True,
        })
