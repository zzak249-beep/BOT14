"""
main.py

Entry point. Polls BingX for closed 15m candles (configurable), runs the
RSI double-dip + SuperTrend signal engine from indicators.py, and trades
the result on BingX Perpetual Futures with Telegram notifications.

Long-only, one position per symbol at a time - mirrors the source Pine
strategy exactly (it only ever calls strategy.entry on the long side).

Safety notes (see README for the full list):
  - DRY_RUN=true by default: computes and logs/notifies signals without
    sending any order. Flip to false only after you've watched it run.
  - An optional hard STOP_LOSS_PCT is placed as a real reduceOnly
    STOP_MARKET order on the exchange (not just checked locally), so
    protection survives even if the bot's process is down.
  - On every cycle (and on startup) the bot re-checks BingX's own position
    endpoint rather than trusting local state alone.
"""

import asyncio
import logging
import signal
from datetime import datetime, timezone

from bingx_client import BingXAPIError, BingXClient, parse_klines
from config import Config
from indicators import StrategyParams, generate_signals
from state_manager import StateManager
from telegram_notifier import TelegramNotifier

logger = logging.getLogger("main")


def _fmt_ts(open_time_ms):
    return datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


class SymbolWorker:
    def __init__(self, symbol, cfg: Config, client: BingXClient, notifier: TelegramNotifier, state_mgr: StateManager):
        self.symbol = symbol
        self.cfg = cfg
        self.client = client
        self.notifier = notifier
        self.state_mgr = state_mgr
        self.state = {"in_position": False}
        self.last_processed_open_time = None
        self.params = StrategyParams(
            rsi_length=cfg.rsi_length,
            rsi_signal_length=cfg.rsi_signal_length,
            trigger_level=cfg.trigger_level,
            target_cross_count=cfg.target_cross_count,
            st_atr_period=cfg.st_atr_period,
            st_factor=cfg.st_factor,
        )

    # ------------------------------------------------------------ persistence
    def _persist(self):
        all_state = self.state_mgr.load()
        all_state[self.symbol] = self.state
        self.state_mgr.save(all_state)

    async def reconcile(self):
        """Startup check: trust BingX's live position over the local state
        file (which may be stale or absent after a redeploy)."""
        all_state = self.state_mgr.load()
        self.state = all_state.get(self.symbol, {"in_position": False})
        try:
            positions = await self.client.get_positions(self.symbol)
            live = next(
                (p for p in positions if p.get("positionSide", "LONG") == "LONG" and abs(float(p.get("positionAmt", 0) or 0)) > 0),
                None,
            )
            if live:
                self.state = {
                    "in_position": True,
                    "quantity": abs(float(live.get("positionAmt", 0))),
                    "entry_price": float(live.get("avgPrice", 0) or 0),
                    "entered_at": self.state.get("entered_at") or datetime.now(timezone.utc).isoformat(),
                    "stop_order_id": self.state.get("stop_order_id"),
                    "reconciled": True,
                }
                logger.info("[%s] Reconciled open position from exchange: qty=%s entry=%s",
                            self.symbol, self.state["quantity"], self.state["entry_price"])
            else:
                if self.state.get("in_position"):
                    logger.warning("[%s] Local state said in-position but exchange shows none; clearing.", self.symbol)
                self.state = {"in_position": False}
        except BingXAPIError as e:
            logger.error("[%s] Could not reconcile positions on startup (leaving local state as-is): %s", self.symbol, e)
        self._persist()

    async def _check_external_close(self):
        """If the exchange shows the position gone (e.g. the resting
        stop-loss filled) but local state still thinks we're in it, sync up
        and notify instead of drifting out of sync."""
        if not self.state.get("in_position") or self.cfg.dry_run:
            return
        try:
            positions = await self.client.get_positions(self.symbol)
            still_open = any(
                p.get("positionSide", "LONG") == "LONG" and abs(float(p.get("positionAmt", 0) or 0)) > 0
                for p in positions
            )
        except BingXAPIError as e:
            if e.code == 109420:  # "position not exist" -> treat as a clean close
                still_open = False
            else:
                logger.warning("[%s] Could not verify position status this cycle: %s", self.symbol, e)
                return
        if not still_open:
            logger.info("[%s] Position is closed on the exchange (stop-loss or manual) - syncing state.", self.symbol)
            await self.notifier.send(f"ℹ️ <b>{self.symbol}</b>: la posición ya está cerrada en BingX (probablemente saltó el stop-loss).")
            self.state = {"in_position": False}
            self._persist()

    # ------------------------------------------------------------------ sizing
    async def _get_equity(self):
        try:
            bal = await self.client.get_balance()
            if isinstance(bal, dict) and "balance" in bal and isinstance(bal["balance"], (dict, list)):
                bal = bal["balance"]
            if isinstance(bal, list):
                bal = next((b for b in bal if b.get("asset", "USDT") == "USDT"), bal[0] if bal else {})
            equity = float(bal.get("equity") or bal.get("balance") or bal.get("availableMargin") or 0)
            return equity
        except Exception as e:
            logger.error("[%s] Failed to fetch balance: %s", self.symbol, e)
            return None

    def _compute_quantity(self, price, equity):
        """Returns (qty, margin_usdt), or (None, None) if it can't be sized."""
        if self.cfg.position_sizing_mode == "FIXED_MARGIN":
            margin = self.cfg.fixed_margin_usdt
        else:
            if not equity:
                return None, None
            margin = equity * (self.cfg.risk_percent_equity / 100.0)
        if price <= 0:
            return None, None
        notional = margin * self.cfg.leverage
        qty = round(notional / price, self.cfg.quantity_precision)
        return qty, round(margin, 2)

    # ------------------------------------------------------------------- core
    async def evaluate(self):
        await self._check_external_close()

        try:
            raw = await self.client.get_klines(self.symbol, self.cfg.timeframe, self.cfg.klines_lookback)
            candles = parse_klines(raw)
        except BingXAPIError as e:
            logger.error("[%s] Failed to fetch klines: %s", self.symbol, e)
            return

        min_bars = max(self.cfg.rsi_length, self.cfg.st_atr_period, self.cfg.rsi_signal_length) * 3
        if len(candles) < min_bars:
            logger.warning("[%s] Not enough candle history yet (%d/%d bars)", self.symbol, len(candles), min_bars)
            return

        closed = candles[:-1]  # drop the still-forming candle
        latest = closed[-1]
        if self.last_processed_open_time == latest["open_time"]:
            return  # already evaluated this closed bar

        highs = [c["high"] for c in closed]
        lows = [c["low"] for c in closed]
        closes = [c["close"] for c in closed]

        signals = generate_signals(highs, lows, closes, self.params)
        self.last_processed_open_time = latest["open_time"]
        i = len(closed) - 1

        logger.info(
            "[%s] %s close=%.6f rsi=%s direction=%s in_position=%s",
            self.symbol,
            _fmt_ts(latest["open_time"]),
            latest["close"],
            f"{signals['rsi'][i]:.2f}" if signals["rsi"][i] is not None else "n/a",
            signals["direction"][i],
            self.state.get("in_position"),
        )

        if not self.state.get("in_position") and signals["special_buy"][i]:
            await self._enter_long(latest["close"], signals, i, latest["open_time"])
        elif self.state.get("in_position") and signals["st_sell"][i]:
            await self._exit_long(latest["close"], "Giro de SuperTrend a bajista", signals, i, latest["open_time"])

    async def _enter_long(self, price, signals, i, open_time):
        equity = None
        if self.cfg.position_sizing_mode != "FIXED_MARGIN":
            equity = await self._get_equity()
        qty, margin = self._compute_quantity(price, equity)
        if not qty or qty <= 0:
            logger.error("[%s] Could not size a position (equity=%s); skipping entry.", self.symbol, equity)
            await self.notifier.send(f"⚠️ <b>{self.symbol}</b>: señal de compra pero falló el cálculo de tamaño (equity={equity}).")
            return

        rsi_val = signals["rsi"][i]
        st_val = signals["supertrend"][i]
        rsi_display = f"{rsi_val:.1f}" if rsi_val is not None else "n/d"
        st_display = round(st_val, self.cfg.price_precision) if st_val is not None else "n/d"

        lines = [
            f"🟢 <b>COMPRA — {self.symbol}</b> ({self.cfg.timeframe})",
            f"Doble cruce de RSI bajo {self.cfg.trigger_level:.0f} — patrón dip {self.cfg.target_cross_count}x",
            "",
            f"Precio: <code>{price}</code>",
            f"RSI: {rsi_display}",
            f"SuperTrend (ref. de salida): <code>{st_display}</code>",
        ]

        stop_price = None
        if self.cfg.stop_loss_pct > 0:
            stop_price = round(price * (1 - self.cfg.stop_loss_pct / 100.0), self.cfg.price_precision)
            lines.append(f"Stop sugerido: <code>{stop_price}</code> (-{self.cfg.stop_loss_pct:g}%)")

        lines += [
            "",
            f"Qty sugerida: <code>{qty}</code>",
            f"Leverage: {self.cfg.leverage}x · Margen: {margin} USDT ({self.cfg.position_sizing_mode})",
            "",
            f"🕐 {_fmt_ts(open_time)} · vela 15m cerrada",
            "🧪 DRY RUN — no se envió orden, señal solo informativa" if self.cfg.dry_run else "✅ Enviando orden a BingX...",
        ]
        await self.notifier.send("\n".join(lines))

        order_result = None
        stop_order_id = None
        if not self.cfg.dry_run:
            try:
                await self.client.set_margin_mode(self.symbol, self.cfg.margin_mode)
            except BingXAPIError as e:
                logger.warning("[%s] set_margin_mode: %s", self.symbol, e)
            try:
                await self.client.set_leverage(self.symbol, "LONG", self.cfg.leverage)
            except BingXAPIError as e:
                logger.warning("[%s] set_leverage: %s", self.symbol, e)
            try:
                order_result = await self.client.place_market_order(self.symbol, "BUY", "LONG", qty)
                logger.info("[%s] Entry order placed: %s", self.symbol, order_result)
            except BingXAPIError as e:
                logger.error("[%s] Entry order FAILED: %s", self.symbol, e)
                await self.notifier.send(f"⚠️ <b>{self.symbol}</b>: la orden de entrada FALLÓ: {e}")
                return

            if stop_price is not None:
                try:
                    sl_result = await self.client.place_stop_market_order(self.symbol, "SELL", "LONG", qty, stop_price)
                    stop_order_id = (sl_result or {}).get("order", {}).get("orderId") if isinstance(sl_result, dict) else None
                    logger.info("[%s] Stop-loss resting order placed at %s (id=%s)", self.symbol, stop_price, stop_order_id)
                except BingXAPIError as e:
                    logger.error("[%s] Stop-loss order FAILED to place: %s", self.symbol, e)
                    await self.notifier.send(f"⚠️ <b>{self.symbol}</b>: entró, pero la orden de stop-loss FALLÓ: {e}")

        self.state = {
            "in_position": True,
            "quantity": qty,
            "entry_price": price,
            "entered_at": datetime.now(timezone.utc).isoformat(),
            "stop_order_id": stop_order_id,
        }
        self._persist()

    async def _exit_long(self, price, reason, signals, i, open_time):
        qty = self.state.get("quantity", 0)
        entry = self.state.get("entry_price", 0) or 0
        stop_order_id = self.state.get("stop_order_id")
        pnl_pct = ((price - entry) / entry * 100.0) if entry else 0.0

        rsi_val = signals["rsi"][i]
        st_val = signals["supertrend"][i]
        rsi_display = f"{rsi_val:.1f}" if rsi_val is not None else "n/d"
        st_display = round(st_val, self.cfg.price_precision) if st_val is not None else "n/d"

        lines = [
            f"🔴 <b>CIERRE — {self.symbol}</b> ({self.cfg.timeframe})",
            f"Motivo: {reason}",
            "",
            f"Entrada: <code>{entry}</code>",
            f"Salida: <code>{price}</code>",
            f"PnL estimado: {pnl_pct:+.2f}%",
            "",
            f"SuperTrend: <code>{st_display}</code> · RSI: {rsi_display}",
            "",
            f"🕐 {_fmt_ts(open_time)} · vela 15m cerrada",
            "🧪 DRY RUN — no se envió orden, señal solo informativa" if self.cfg.dry_run else "✅ Enviando orden a BingX...",
        ]
        await self.notifier.send("\n".join(lines))

        if not self.cfg.dry_run:
            if stop_order_id:
                try:
                    await self.client.cancel_order(self.symbol, stop_order_id)
                except BingXAPIError as e:
                    logger.warning("[%s] Could not cancel resting stop order %s (may already be filled/gone): %s", self.symbol, stop_order_id, e)
            try:
                result = await self.client.close_position_market(self.symbol, "LONG", qty)
                logger.info("[%s] Close order placed: %s", self.symbol, result)
            except BingXAPIError as e:
                logger.error("[%s] Exit order FAILED: %s", self.symbol, e)
                await self.notifier.send(f"⚠️ <b>{self.symbol}</b>: la orden de salida FALLÓ: {e} — revisá la posición manualmente.")
                return

        self.state = {"in_position": False}
        self._persist()


async def run():
    cfg = Config()
    cfg.validate()

    logging.basicConfig(
        level=getattr(logging, cfg.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    logger.info(
        "Starting RSI double-dip + SuperTrend bot | symbols=%s | timeframe=%s | DRY_RUN=%s",
        cfg.symbols, cfg.timeframe, cfg.dry_run,
    )

    client = BingXClient(cfg.api_key, cfg.api_secret, cfg.base_url, cfg.recv_window_ms)
    notifier = TelegramNotifier(cfg.telegram_bot_token, cfg.telegram_chat_id)
    state_mgr = StateManager(cfg.state_file_path)
    workers = [SymbolWorker(sym, cfg, client, notifier, state_mgr) for sym in cfg.symbols]

    stop_event = asyncio.Event()

    def _handle_signal(*_):
        logger.info("Shutdown signal received, stopping after the current cycle...")
        stop_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            pass  # not available on some platforms (e.g. Windows)

    await notifier.send(
        f"🤖 Bot iniciado — {', '.join(cfg.symbols)} @ {cfg.timeframe} | "
        f"{'DRY RUN (solo señales)' if cfg.dry_run else 'EN VIVO'}"
    )

    if not cfg.dry_run:
        for w in workers:
            await w.reconcile()

    try:
        while not stop_event.is_set():
            for w in workers:
                try:
                    await w.evaluate()
                except Exception as e:  # a bug in one symbol must not kill the others
                    logger.exception("[%s] Unhandled error in evaluate(): %s", w.symbol, e)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=cfg.poll_interval_seconds)
            except asyncio.TimeoutError:
                pass
    finally:
        await client.close()
        await notifier.send("🛑 Bot detenido.")


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
