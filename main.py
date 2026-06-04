"""
main.py — QF×JP v3.4 Bot — Orquestador Principal
==================================================
- Health check server (Railway requiere puerto abierto)
- APScheduler cada 3 minutos
- Ciclo completo: fetch candles → indicadores → score → trade
"""
import logging, asyncio, os, sys, time
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler

# ── CONFIG PRIMERO ─────────────────────────────────────────────────────────────
import config as cfg

logging.basicConfig(
    level=getattr(logging, cfg.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")

# ── IMPORTS ───────────────────────────────────────────────────────────────────
from bingx.client         import BingXClient, parse_klines
from strategy.indicators  import QFJPIndicators
from strategy.qfxjp_signal import QFJPScorer
from trader.position_manager import PositionManager
from trader.risk_manager   import RiskManager
from notifications.telegram_notifier import TelegramNotifier

try:
    from market_mechanics import MarketContextEngine
    MECHANICS_OK = True
except ImportError:
    MECHANICS_OK = False
    logger.warning("market_mechanics no disponible — continuando sin él")


# ═══════════════════════════════════════════════════════════════════════════════
#  HEALTH CHECK SERVER
# ═══════════════════════════════════════════════════════════════════════════════

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b"QF x JP v3.4 BOT OK"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass  # silenciar logs HTTP


def start_health_server():
    server = HTTPServer(("0.0.0.0", cfg.PORT), HealthHandler)
    logger.info(f"Health check server en puerto {cfg.PORT}")
    Thread(target=server.serve_forever, daemon=True).start()


# ═══════════════════════════════════════════════════════════════════════════════
#  BOT CORE
# ═══════════════════════════════════════════════════════════════════════════════

class QFJPBot:
    def __init__(self):
        logger.info("Inicializando QF×JP v3.4 Bot...")

        self.client  = BingXClient(cfg.BINGX_API_KEY, cfg.BINGX_SECRET_KEY, cfg.BINGX_BASE_URL)
        self.tg      = TelegramNotifier(cfg.TELEGRAM_TOKEN, cfg.TELEGRAM_CHAT_ID)
        self.risk_mgr = RiskManager(cfg, self.client)
        self.pos_mgr  = PositionManager(self.client, cfg)

        # Un conjunto de indicadores + scorer por símbolo (estado persistente)
        self.indicators: dict[str, QFJPIndicators] = {s: QFJPIndicators(cfg) for s in cfg.SYMBOLS}
        self.scorers:    dict[str, QFJPScorer]     = {s: QFJPScorer(cfg)     for s in cfg.SYMBOLS}

        self.mechanics_engine = None
        if MECHANICS_OK:
            self.mechanics_engine = MarketContextEngine(
                api_key    = cfg.BINGX_API_KEY,
                secret_key = cfg.BINGX_SECRET_KEY,
                enable_session      = True,
                enable_funding      = True,
                enable_oi           = True,
                enable_liquidations = True,
            )

        self._cycle_count = 0
        logger.info(f"Símbolos: {cfg.SYMBOLS}")

    # ─── FETCH CANDLES ────────────────────────────────────────────────────────

    def _fetch_df(self, symbol: str, tf: str, limit: int = 300):
        raw = self.client.get_klines_raw(symbol, tf, limit)
        return parse_klines(raw)

    def _fetch_all_tfs(self, symbol: str) -> dict:
        """Fetches 3m, 15m, 1h, 1w, 1m candles."""
        return {
            "3m":  self._fetch_df(symbol, cfg.CANDLE_TF,  cfg.CANDLE_LIMIT),
            "15m": self._fetch_df(symbol, cfg.HTF_15M_TF, 50),
            "1h":  self._fetch_df(symbol, cfg.HTF_1H_TF,  50),
            "1w":  self._fetch_df(symbol, cfg.HTF_W_TF,   20),
            "1m":  self._fetch_df(symbol, cfg.HTF_1M_TF,  5),
        }

    # ─── CICLO PRINCIPAL ──────────────────────────────────────────────────────

    def run_cycle(self):
        self._cycle_count += 1
        logger.info(f"═══ Ciclo #{self._cycle_count} ═══")

        # 1. Verificar riesgo global
        can, reason = self.risk_mgr.can_trade()
        if not can:
            logger.warning(f"[Risk] No se puede operar: {reason}")
            if self._cycle_count % 20 == 0:
                self.tg.risk_alert(reason)
            return

        # 2. Balance
        balance = self.risk_mgr.refresh()
        if balance is None:
            logger.error("No se pudo obtener balance — saltando ciclo")
            return

        # 3. Verificar posiciones abiertas
        prices = {}
        for s in cfg.SYMBOLS:
            p = self.client.get_last_price(s)
            if p: prices[s] = p
        closed = self.pos_mgr.check_positions(prices)
        for sym in closed:
            self.tg.position_closed(sym, "?", 0.0, "cerrado externamente")

        # 4. Scan de símbolos
        scan_results = []
        new_signals  = []

        for symbol in cfg.SYMBOLS:
            try:
                result = self._analyze_symbol(symbol, balance)
                scan_results.append(result)
                if result.get("signal"):
                    new_signals.append(result)
            except Exception as e:
                logger.error(f"[Bot] Error analizando {symbol}: {e}", exc_info=True)

        # 5. Enviar resumen de scan cada 5 ciclos
        if self._cycle_count % 5 == 0:
            self.tg.scan_summary(scan_results)

        # 6. Ejecutar señales
        for result in new_signals:
            signal = result["signal"]
            if self.pos_mgr.open_count >= cfg.MAX_OPEN_TRADES:
                logger.info(f"MAX_OPEN_TRADES alcanzado — saltando {signal.symbol}")
                break
            if self.pos_mgr.has_position(signal.symbol):
                continue
            ok = self.pos_mgr.open_position(signal, balance)
            if ok:
                self.tg.signal_entry(signal, result.get("ctx"))
                logger.info(f"✅ Posición abierta: {signal.symbol} {signal.direction} [{signal.level}] score={signal.score}")

        # 7. Reporte diario a las 00:05 UTC
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        if now.hour == 0 and now.minute < cfg.CYCLE_MINUTES:
            stats = self.risk_mgr.get_stats()
            self.tg.daily_report(stats, self.pos_mgr.get_positions_summary())

    # ─── ANÁLISIS POR SÍMBOLO ─────────────────────────────────────────────────

    def _analyze_symbol(self, symbol: str, balance: float) -> dict:
        tfs = self._fetch_all_tfs(symbol)
        df  = tfs["3m"]

        if df is None or len(df) < 50:
            return {"symbol": symbol, "sc_l": 0, "sc_s": 0, "ses": "?", "regime": "?"}

        # Indicadores
        ind = self.indicators[symbol].compute(
            df, tfs["15m"], tfs["1h"], tfs["1w"], tfs["1m"]
        )

        # Score
        signal = self.scorers[symbol].score(symbol, ind, balance)

        # Market Mechanics (contexto de mercado)
        ctx = None
        if self.mechanics_engine:
            try:
                price = ind.price
                ctx   = self.mechanics_engine.analyze(
                    symbol, price, df,
                    high = float(df["high"].iloc[-1]),
                    low  = float(df["low"].iloc[-1]),
                )
                # Aplicar modificadores
                if signal.is_valid:
                    if not ctx.entry_allowed(signal.direction):
                        logger.info(f"[Ctx] {symbol}: veto de market context — {ctx.veto_reason}")
                        signal.direction = "NONE"
                        signal.level     = "NONE"
                    else:
                        bonus = ctx.score_modifier + ctx.get_judas_bonus(signal.direction)
                        signal.score = max(0, min(100, signal.score + bonus))
                        logger.debug(f"[Ctx] {symbol}: bonus={bonus:+d} → score={signal.score}")
            except Exception as e:
                logger.warning(f"[Ctx] {symbol}: error en market context: {e}")

        result = {
            "symbol":  symbol,
            "sc_l":    signal.score_long,
            "sc_s":    signal.score_short,
            "ses":     ind.ses_label,
            "regime":  ind.regime,
            "level":   signal.level if signal.is_valid else None,
            "signal":  signal if signal.is_valid else None,
            "ctx":     ctx,
        }

        logger.info(
            f"[Scan] {symbol:12} "
            f"L:{signal.score_long:3d} S:{signal.score_short:3d} "
            f"| {ind.ses_label:4} | {ind.regime:8} "
            f"| {signal.level if signal.is_valid else '----'}"
        )

        return result


# ═══════════════════════════════════════════════════════════════════════════════
#  SCHEDULER
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    logger.info("╔══════════════════════════════════════╗")
    logger.info("║   QF×JP v3.4 Bot  — Railway Ready    ║")
    logger.info("╚══════════════════════════════════════╝")

    # Health check server (Railway lo necesita)
    start_health_server()

    # Instanciar bot
    bot = QFJPBot()

    # Notificar inicio
    bot.tg.bot_started(cfg.SYMBOLS, cfg)

    # Primer ciclo inmediato
    try:
        bot.run_cycle()
    except Exception as e:
        logger.error(f"Error en primer ciclo: {e}", exc_info=True)

    # Loop principal
    interval = cfg.CYCLE_MINUTES * 60
    logger.info(f"Ciclos cada {cfg.CYCLE_MINUTES} minutos")

    while True:
        try:
            time.sleep(interval)
            bot.run_cycle()
        except KeyboardInterrupt:
            logger.info("Bot detenido por el usuario")
            bot.pos_mgr.close_all("shutdown")
            bot.tg.send("🔴 *Bot DETENIDO* — Todas las posiciones cerradas")
            break
        except Exception as e:
            logger.error(f"Error en ciclo: {e}", exc_info=True)
            bot.tg.send(f"⚠️ Error en ciclo: `{str(e)[:200]}`")
            time.sleep(30)


if __name__ == "__main__":
    main()
