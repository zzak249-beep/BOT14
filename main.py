"""
main.py — Wavelet MRA Haar 5m Bot — BingX

Bot standalone (no depende de webhooks de TradingView): recalcula la
lógica del script Pine directamente desde velas de BingX y opera de
forma autónoma, además de avisar por Telegram en cada señal para
poder operar en paralelo a mano si se quiere.

Flujo de cada ciclo:
  1. Reconcilia posiciones abiertas reales en BingX (detecta cierres
     por SL/TP y avisa por Telegram).
  2. Refresca el balance de la cuenta.
  3. Recorre el universo de símbolos en tandas pequeñas:
       - se salta si ya hay posición abierta en ese símbolo (de este
         bot o de cualquier otro proceso en la misma cuenta),
       - se salta si está en cooldown,
       - se salta si se llegó al máximo de posiciones simultáneas,
       - calcula la señal wavelet sobre velas cerradas,
       - si hay señal: dimensiona, calcula SL/TP, avisa por Telegram
         y (si LIVE_TRADING) manda la orden real + SL + TP a BingX.
  4. Duerme POLL_INTERVAL_SECONDS y repite.
"""

import logging
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, HTTPServer

import pandas as pd

from bingx_client import BingXClient, BingXAPIError, ERR_POSITION_NOT_EXIST
from config import Config
import risk_manager
import wavelet_engine
from state_manager import StateManager, timeframe_to_ms
from telegram_notifier import TelegramNotifier

logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("wavelet_bot.main")

QUOTE_SUFFIX = "-VST" if Config.DEMO_MODE else "-USDT"


# ── Servidor de salud (para healthcheck de Railway / monitoreo manual) ──
class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args):
        pass  # silencia el log de acceso por request, ya tenemos el logger propio


def start_health_server(port: int) -> None:
    def _serve():
        try:
            HTTPServer(("0.0.0.0", port), _HealthHandler).serve_forever()
        except OSError as exc:
            logger.warning("No se pudo levantar el servidor de salud en :%d (%s)", port, exc)

    threading.Thread(target=_serve, daemon=True).start()
    logger.info("Servidor de salud escuchando en :%d/health", port)


class Bot:
    def __init__(self):
        self.client = BingXClient(
            Config.BINGX_API_KEY, Config.BINGX_API_SECRET, Config.BINGX_BASE_URL,
            recv_window_ms=Config.BINGX_RECV_WINDOW_MS,
        )
        self.tg = TelegramNotifier(Config.TELEGRAM_BOT_TOKEN, Config.TELEGRAM_CHAT_ID)
        self.state = StateManager()
        self.timeframe_ms = timeframe_to_ms(Config.TIMEFRAME)
        self._contracts: dict[str, dict] = {}
        self._contracts_fetched_at = 0.0

    # ── Universo de símbolos y metadatos de contrato ────────────────
    def refresh_contracts(self, force: bool = False) -> None:
        if not force and (time.time() - self._contracts_fetched_at) < 3600:
            return
        raw = self.client.get_contracts()
        contracts = {}
        for c in raw:
            symbol = c.get("symbol", "")
            if not symbol.endswith("-USDT"):
                continue
            if int(c.get("status", 0)) != 1:
                continue
            contracts[symbol] = {
                "quantityPrecision": int(c.get("quantityPrecision", 4)),
                "pricePrecision": int(c.get("pricePrecision", 4)),
                "tradeMinQuantity": float(c.get("tradeMinQuantity", 0) or 0),
                "tradeMinUSDT": float(c.get("tradeMinUSDT", 0) or 0),
            }
        self._contracts = contracts
        self._contracts_fetched_at = time.time()
        logger.info("Contratos USDT-M activos: %d", len(contracts))

    def symbol_universe(self) -> list[str]:
        if Config.SYMBOLS.strip().upper() == "ALL":
            base_symbols = list(self._contracts.keys())
        else:
            base_symbols = [s.strip() for s in Config.SYMBOLS.split(",") if s.strip()]
        if Config.DEMO_MODE:
            return [s.replace("-USDT", "-VST") for s in base_symbols]
        return base_symbols

    def contract_meta(self, symbol: str) -> dict:
        # en DEMO_MODE la precisión se toma del contrato USDT equivalente
        key = symbol.replace("-VST", "-USDT")
        return self._contracts.get(key, {
            "quantityPrecision": 4, "pricePrecision": 4,
            "tradeMinQuantity": 0.0, "tradeMinUSDT": 0.0,
        })

    # ── Reconciliación de posiciones (fuente de verdad = BingX) ─────
    def reconcile_positions(self) -> dict:
        try:
            positions = self.client.get_positions()
        except Exception as exc:
            logger.error("No se pudieron leer posiciones: %s", exc)
            return self.state.known_positions

        current = {}
        for p in positions:
            amt = float(p.get("positionAmt", p.get("positionSize", 0)) or 0)
            if amt == 0:
                continue
            key = (p.get("symbol"), p.get("positionSide", "BOTH"))
            current[key] = p

        for key, old in self.state.known_positions.items():
            if key not in current:
                symbol, side = key
                exit_price = old.get("markPrice") or old.get("avgPrice") or 0
                self.tg.exit_notice(symbol, side, float(exit_price or 0))
                logger.info("Posición cerrada detectada: %s %s", symbol, side)

        self.state.known_positions = current
        return current

    def get_equity(self) -> float:
        try:
            bal = self.client.get_balance()
            for key in ("equity", "balance", "availableMargin"):
                if key in bal:
                    return float(bal[key])
            # algunas respuestas anidan en una lista de assets
            if isinstance(bal, list) and bal:
                return float(bal[0].get("equity", bal[0].get("balance", 0)))
        except Exception as exc:
            logger.error("No se pudo leer el balance: %s", exc)
        return 0.0

    # ── Procesamiento de un símbolo ──────────────────────────────────
    def process_symbol(self, symbol: str, open_positions: dict, equity: float) -> None:
        try:
            if Config.SKIP_IF_SYMBOL_HAS_POSITION:
                if any(sym == symbol for sym, _side in open_positions.keys()):
                    return

            candles = self.client.get_klines(symbol, Config.TIMEFRAME, limit=max(250, Config.LOOKBACK_ENERGY + 64))
            if len(candles) < 20:
                return

            # descarta la vela en formación (todavía no cerrada)
            now_ms = int(time.time() * 1000)
            if candles[-1]["time"] + self.timeframe_ms > now_ms:
                candles = candles[:-1]
            if not candles:
                return

            df = pd.DataFrame(candles)
            signal = wavelet_engine.compute_signal(df, Config)
            if signal is None:
                return

            candle_time = signal["time"]
            if not self.state.can_signal(symbol, candle_time, Config.COOLDOWN_BARS, self.timeframe_ms):
                return

            side = None
            if signal["long_cond"]:
                side = "LONG"
            elif signal["short_cond"]:
                side = "SHORT"
            if side is None:
                return

            self.state.mark_signal(symbol, candle_time)
            self._handle_entry(symbol, side, signal, df, equity, open_positions)

        except BingXAPIError as exc:
            if exc.code == ERR_POSITION_NOT_EXIST:
                return
            logger.warning("Error de API en %s: %s", symbol, exc)
        except Exception as exc:
            logger.exception("Error inesperado procesando %s: %s", symbol, exc)

    def _handle_entry(self, symbol: str, side: str, signal: dict, df: pd.DataFrame,
                       equity: float, open_positions: dict) -> None:
        meta = self.contract_meta(symbol)
        atr_series = wavelet_engine.compute_atr(df, Config.ATR_LENGTH)
        atr_value = float(atr_series.iloc[-1]) if not pd.isna(atr_series.iloc[-1]) else None

        entry_price = signal["close"]
        is_long = side == "LONG"
        sl_price, tp_price = risk_manager.compute_sl_tp(entry_price, is_long, atr_value, Config)

        # límite de posiciones simultáneas en toda la cuenta (no solo las de este bot)
        if len(open_positions) >= Config.MAX_CONCURRENT_POSITIONS:
            self.tg.signal(symbol, side, entry_price, sl_price, tp_price, executed=False,
                            reason="máximo de posiciones simultáneas alcanzado")
            return

        if Config.MIN_BALANCE_USDT and equity < Config.MIN_BALANCE_USDT:
            self.tg.signal(symbol, side, entry_price, sl_price, tp_price, executed=False,
                            reason="balance por debajo del mínimo configurado")
            return

        sizing = risk_manager.compute_position_size(
            equity, Config.QTY_PCT, entry_price,
            meta["quantityPrecision"], meta["tradeMinQuantity"], meta["tradeMinUSDT"],
        )
        if not sizing.ok:
            self.tg.signal(symbol, side, entry_price, sl_price, tp_price, executed=False, reason=sizing.reason)
            return

        if not Config.LIVE_TRADING:
            self.tg.signal(symbol, side, entry_price, sl_price, tp_price, executed=False,
                            reason="LIVE_TRADING desactivado")
            return

        try:
            if not self.state.leverage_already_set(symbol):
                self.client.set_leverage(symbol, side, Config.LEVERAGE)
                self.state.mark_leverage_set(symbol)

            entry_side = "BUY" if is_long else "SELL"
            exit_side = "SELL" if is_long else "BUY"

            self.client.place_market_order(symbol, entry_side, side, sizing.quantity)
            self.client.place_stop_market(symbol, exit_side, side, sl_price, close_position=True)
            self.client.place_take_profit_market(symbol, exit_side, side, tp_price, close_position=True)

            self.tg.signal(symbol, side, entry_price, sl_price, tp_price, executed=True)
            logger.info("Entrada ejecutada: %s %s qty=%s @ %.6g (SL=%.6g TP=%.6g)",
                        symbol, side, sizing.quantity, entry_price, sl_price, tp_price)

        except Exception as exc:
            logger.exception("Fallo al ejecutar la entrada en %s: %s", symbol, exc)
            self.tg.error(f"entrada {symbol} {side}", str(exc))

    # ── Bucle principal ──────────────────────────────────────────────
    def run(self) -> None:
        Config.validate()
        start_health_server(Config.HEALTH_PORT)
        logger.info("Iniciando bot.\n%s", Config.summary())
        self.tg.info("Bot iniciado.\n" + Config.summary())

        self.refresh_contracts(force=True)

        while True:
            cycle_start = time.time()
            try:
                self.refresh_contracts()
                open_positions = self.reconcile_positions()
                equity = self.get_equity()
                symbols = self.symbol_universe()

                for i in range(0, len(symbols), Config.SYMBOL_BATCH_SIZE):
                    batch = symbols[i:i + Config.SYMBOL_BATCH_SIZE]
                    with ThreadPoolExecutor(max_workers=len(batch)) as pool:
                        list(pool.map(lambda s: self.process_symbol(s, open_positions, equity), batch))
                    time.sleep(Config.SYMBOL_BATCH_DELAY_SECONDS)

            except Exception as exc:
                logger.exception("Error en el ciclo principal: %s", exc)
                self.tg.error("ciclo principal", str(exc))

            elapsed = time.time() - cycle_start
            sleep_for = max(1.0, Config.POLL_INTERVAL_SECONDS - elapsed)
            time.sleep(sleep_for)


if __name__ == "__main__":
    Bot().run()
