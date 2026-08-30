"""
exchange_client.py
-------------------
Envoltorio sobre ccxt para operar en BingX (spot).

Mejoras sobre la v1:
  - Reintentos con backoff exponencial ante errores de red TRANSITORIOS
    (ccxt.NetworkError / timeouts). Los errores del exchange (fondos
    insuficientes, símbolo inválido, etc.) NO se reintentan: se propagan
    de inmediato porque reintentarlos no los arregla.
  - `get_market_limits` cachea `load_markets()` para poder validar el
    tamaño mínimo de orden antes de enviarla.
  - `check_buy_preconditions` hace una comprobación previa de saldo/mínimos,
    para fallar con un mensaje claro en vez de una excepción críptica del exchange.
  - `estimate_entry_price` intenta reconstruir el precio de entrada desde el
    historial real de operaciones (fetch_my_trades) si el estado local
    (state.json) se hubiera perdido, por ejemplo tras un redeploy en Railway.
"""

import logging
import time

import ccxt
import pandas as pd

logger = logging.getLogger("bot")

# Errores de ccxt que consideramos transitorios y por tanto reintentables.
RETRYABLE_ERRORS = (ccxt.NetworkError, ccxt.RequestTimeout, ccxt.ExchangeNotAvailable, ccxt.DDoSProtection)


class ExchangeClient:
    def __init__(self, api_key: str, api_secret: str, demo: bool = True,
                 max_retries: int = 3, retry_backoff_seconds: float = 2.0):
        self.exchange = ccxt.bingx(
            {
                "apiKey": api_key,
                "secret": api_secret,
                "enableRateLimit": True,
                "options": {"defaultType": "spot"},
            }
        )
        if demo:
            # Redirige al entorno de práctica de BingX (VST / dinero ficticio).
            # Necesitas generar las API keys específicamente en modo demo
            # dentro de tu cuenta de BingX para que esto funcione.
            self.exchange.set_sandbox_mode(True)
            logger.info("ExchangeClient: modo DEMO (sandbox VST) activado.")
        else:
            logger.info("ExchangeClient: modo REAL activado. Las órdenes moverán fondos de verdad.")

        self.demo = demo
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self._markets_loaded = False

    # ------------------------------------------------------------------
    # Infraestructura: reintentos con backoff exponencial
    # ------------------------------------------------------------------
    def _with_retries(self, func, *args, **kwargs):
        attempt = 0
        while True:
            try:
                return func(*args, **kwargs)
            except RETRYABLE_ERRORS as exc:
                attempt += 1
                if attempt > self.max_retries:
                    logger.error(f"Se agotaron los reintentos ({self.max_retries}) llamando a {func.__name__}: {exc}")
                    raise
                wait = self.retry_backoff_seconds * (2 ** (attempt - 1))
                logger.warning(
                    f"Error de red transitorio en {func.__name__} (intento {attempt}/{self.max_retries}): "
                    f"{exc}. Reintentando en {wait:.1f}s..."
                )
                time.sleep(wait)
            # ccxt.ExchangeError (fondos insuficientes, símbolo inválido, permisos, etc.)
            # y cualquier otra excepción NO se reintentan: se propagan tal cual.

    # ------------------------------------------------------------------
    # Utilidades
    # ------------------------------------------------------------------
    @staticmethod
    def split_symbol(symbol: str):
        base, quote = symbol.split("/")
        return base, quote

    def ensure_markets_loaded(self):
        if not self._markets_loaded:
            self._with_retries(self.exchange.load_markets)
            self._markets_loaded = True

    def get_market_limits(self, symbol: str) -> dict:
        """Devuelve los límites mínimos del mercado (coste/cantidad), si BingX los publica."""
        self.ensure_markets_loaded()
        market = self.exchange.markets.get(symbol, {})
        return market.get("limits", {})

    # ------------------------------------------------------------------
    # Datos de mercado
    # ------------------------------------------------------------------
    def fetch_ohlcv_df(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        raw = self._with_retries(self.exchange.fetch_ohlcv, symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        return df

    def fetch_ohlcv_raw(self, symbol: str, timeframe: str, since=None, limit: int = 500):
        """Devuelve las velas en formato lista cruda de ccxt (usado para paginar
        históricos largos en backtest.py)."""
        return self._with_retries(self.exchange.fetch_ohlcv, symbol, timeframe=timeframe, since=since, limit=limit)

    def get_last_price(self, symbol: str) -> float:
        ticker = self._with_retries(self.exchange.fetch_ticker, symbol)
        return float(ticker["last"])

    def get_free_balance(self, currency: str) -> float:
        balance = self._with_retries(self.exchange.fetch_balance)
        return float(balance.get(currency, {}).get("free", 0) or 0)

    def get_position_value_usdt(self, symbol: str) -> float:
        """Valor aproximado (en USDT) del balance libre de la moneda base."""
        base, _quote = self.split_symbol(symbol)
        base_balance = self.get_free_balance(base)
        if base_balance <= 0:
            return 0.0
        price = self.get_last_price(symbol)
        return base_balance * price

    def estimate_entry_price(self, symbol: str):
        """Intenta recuperar el precio de la última compra desde el historial real
        de operaciones en BingX. Sirve de red de seguridad si state.json se perdió
        (p. ej. tras un redeploy) y necesitamos el precio de entrada para un stop-loss."""
        try:
            trades = self._with_retries(self.exchange.fetch_my_trades, symbol, None, 20)
        except Exception as exc:
            logger.warning(f"No se pudo recuperar el historial de operaciones para estimar la entrada: {exc}")
            return None
        buys = [t for t in trades if t.get("side") == "buy"]
        if not buys:
            return None
        buys.sort(key=lambda t: t.get("timestamp") or 0)
        return float(buys[-1]["price"])

    # ------------------------------------------------------------------
    # Comprobaciones previas (evitan errores confusos del exchange)
    # ------------------------------------------------------------------
    def check_buy_preconditions(self, symbol: str, cost_usdt: float):
        """Devuelve None si todo OK, o un mensaje de error legible si algo impide comprar."""
        base, quote = self.split_symbol(symbol)
        free_quote = self.get_free_balance(quote)
        if free_quote < cost_usdt:
            return f"Saldo insuficiente: tienes {free_quote:.2f} {quote} disponibles, se necesitan {cost_usdt:.2f} {quote}."

        try:
            limits = self.get_market_limits(symbol)
            min_cost = (limits.get("cost") or {}).get("min")
            if min_cost and cost_usdt < min_cost:
                return f"El importe configurado ({cost_usdt} {quote}) está por debajo del mínimo del mercado ({min_cost} {quote})."
        except Exception as exc:
            logger.warning(f"No se pudieron comprobar los límites del mercado (se continúa igualmente): {exc}")
        return None

    # ------------------------------------------------------------------
    # Órdenes
    # ------------------------------------------------------------------
    def market_buy_with_cost(self, symbol: str, cost_usdt: float):
        """Compra a mercado gastando `cost_usdt` de la moneda de cotización."""
        logger.info(f"Enviando orden de COMPRA a mercado: {symbol} por {cost_usdt} USDT")
        return self._with_retries(self.exchange.create_market_buy_order_with_cost, symbol, cost_usdt)

    def market_sell_all(self, symbol: str):
        """Vende el 100% del balance libre de la moneda base (cierra la posición)."""
        base, _quote = self.split_symbol(symbol)
        amount = self.get_free_balance(base)
        if amount <= 0:
            raise ValueError(f"No hay balance de {base} para vender.")
        amount = float(self.exchange.amount_to_precision(symbol, amount))
        logger.info(f"Enviando orden de VENTA a mercado: {amount} {base}")
        return self._with_retries(self.exchange.create_market_sell_order, symbol, amount)
