"""
TradingBot v2 — Multi-symbol scanner
Flow:
  1. Fetch all BingX USDT perpetual futures symbols
  2. Bootstrap: parallel historical klines → warm up all indicators
  3. Open N WebSocket connections (50 symbols each)
  4. On each closed candle → scanner → ranked signals → strategy → execute
  5. Rich dashboard runs concurrently
"""
import asyncio, logging, os
from bot.bingx_client import BingXClient
from bot.scanner      import Scanner
from bot.strategy     import PortfolioStrategy, Action
from bot.dashboard    import Dashboard

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt = "%H:%M:%S",
)
log = logging.getLogger("bot")


class TradingBot:
    def __init__(self):
        self.interval    = os.environ.get("INTERVAL",   "1m")
        self.period      = int(os.environ.get("PERIOD", "25"))
        self.leverage    = int(os.environ.get("LEVERAGE", "10"))
        self.dry_run     = os.environ.get("DRY_RUN", "true").lower() == "true"
        self.max_pos     = int(os.environ.get("MAX_POSITIONS", "3"))
        self.risk_pct    = float(os.environ.get("RISK_PCT",    "1.5"))
        self.min_vol_m   = float(os.environ.get("MIN_VOL_MUSDT", "5"))   # million USDT

        self.client   = BingXClient(
            api_key  = os.environ["BINGX_API_KEY"],
            secret   = os.environ["BINGX_SECRET"],
            interval = self.interval,
        )
        self.scanner  = Scanner(
            period        = self.period,
            adx_threshold = float(os.environ.get("ADX_THRESHOLD", "35")),
            min_score     = float(os.environ.get("MIN_SCORE",     "0.25")),
        )
        self.strategy = PortfolioStrategy(
            max_positions   = self.max_pos,
            risk_pct        = self.risk_pct,
            leverage        = self.leverage,
            adx_threshold   = float(os.environ.get("ADX_THRESHOLD", "35")),
            stop_loss_pct   = float(os.environ.get("STOP_LOSS_PCT",  "2.0")),
            take_profit_pct = float(os.environ.get("TP_PCT",          "4.0")),
        )
        self.dashboard    = Dashboard(self.scanner, self.strategy, self.dry_run)
        self._balance     = 1000.0   # updated periodically
        self._trade_count = 0
        self._symbols: list[str] = []

    # ── BOOTSTRAP ─────────────────────────────────────────────────────────────
    async def _bootstrap(self):
        log.info("Fetching active symbols (min vol %.0fM USDT)…", self.min_vol_m)
        self._symbols = await self.client.get_all_symbols(
            min_vol_usdt=self.min_vol_m * 1_000_000
        )

        log.info("Registering %d symbols in scanner…", len(self._symbols))
        for sym in self._symbols:
            self.scanner.register(sym)

        log.info("Fetching historical klines for warm-up (parallel)…")
        all_klines = await self.client.fetch_all_klines(self._symbols, limit=200)

        warmed = 0
        for sym, klines in all_klines.items():
            for candle in klines:
                self.scanner._engines[sym].update(candle)
            if klines:
                warmed += 1
        log.info("Warm-up complete: %d/%d symbols ready.", warmed, len(self._symbols))

        if not self.dry_run:
            self._balance = await self.client.get_balance()
            log.info("Balance: %.2f USDT", self._balance)

    # ── SIGNAL PROCESSOR ──────────────────────────────────────────────────────
    async def _process_signals(self):
        """Consume signals from scanner queue and act on them."""
        while True:
            sig = await self.scanner.signal_queue.get()
            decision = self.strategy.decide(sig, self._balance)

            if decision.action == Action.HOLD:
                continue

            log.info("⚡ %s %s  qty=%.4f  reason=%s  score=%.3f",
                     decision.action.value, decision.symbol,
                     decision.quantity, decision.reason, decision.score)

            self._trade_count += 1
            self.dashboard.trade_count = self._trade_count

            if not self.dry_run:
                asyncio.create_task(self._execute(decision))

    async def _execute(self, decision):
        try:
            if decision.action in (Action.LONG, Action.SHORT):
                await self.client.place_order(
                    decision.symbol, decision.action.value, decision.quantity
                )
            elif decision.action == Action.CLOSE:
                # Determine close side from position
                pos = self.strategy.positions.get(decision.symbol)
                close_side = "SELL" if (pos and pos.side == Action.LONG) else "BUY"
                await self.client.place_order(
                    decision.symbol, close_side, decision.quantity
                )
        except Exception as e:
            log.error("Order failed %s: %s", decision.symbol, e)

    # ── BALANCE REFRESH ───────────────────────────────────────────────────────
    async def _refresh_balance(self):
        while True:
            await asyncio.sleep(60)
            if not self.dry_run:
                try:
                    self._balance = await self.client.get_balance()
                except Exception:
                    pass

    # ── CANDLE CALLBACK ───────────────────────────────────────────────────────
    def _on_candle(self, symbol: str, candle: dict):
        self.scanner.on_candle(symbol, candle)

    # ── MAIN ──────────────────────────────────────────────────────────────────
    async def run(self):
        log.info("═══ BingX Multi-Symbol Bot v2  DRY=%s ═══", self.dry_run)
        await self._bootstrap()

        # Run all tasks concurrently
        await asyncio.gather(
            self.client.stream_all(self._symbols, self._on_candle),
            self._process_signals(),
            self._refresh_balance(),
            self.dashboard.run(refresh_secs=1.0),
        )
