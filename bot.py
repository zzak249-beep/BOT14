"""
Bot v2 — loop principal con:
  - Fetch paralelo (ThreadPoolExecutor en scanner)
  - Cooldown entre reinicios (initial_bars_since_signal desde state)
  - Filtro de R:R mínimo
  - Filtro de distancia máxima de entrada (ATR)
  - Límite de pérdida diaria
  - Cooldown de re-entrada por símbolo
  - Monitoreo de posiciones (detección de cierre)
  - Resumen diario por Telegram a SUMMARY_HOUR UTC
"""
from __future__ import annotations
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Set

from bingx_api import BingXAPI, BingXError
from strategy  import FibStructStrategy
from scanner   import get_active_symbols, fetch_all_candles_parallel, build_contract_map
from risk_manager import calculate_sl_tp, calculate_quantity, rr_ratio, is_rr_valid
from notifier  import (send_startup, send_signal, send_close,
                       send_error, send_info, send_daily_summary,
                       send_daily_loss_limit)
from state     import BotState
from utils     import interval_to_seconds
import config as cfg

logger = logging.getLogger(__name__)


class FibStructBot:
    def __init__(self):
        self.api      = BingXAPI()
        self.strategy = FibStructStrategy()
        self.state    = BotState()
        self.contracts: Dict[str, dict] = {}
        self._bar_secs = interval_to_seconds(cfg.TIMEFRAME)
        self._symbols: list = []
        self._last_sym_refresh = 0.0
        self._daily_limit_hit  = False

    # ── Inicialización ────────────────────────────────────────
    def _refresh_symbols(self):
        syms = get_active_symbols(self.api)
        if syms:
            self._symbols  = syms
            self.contracts = build_contract_map(syms)
            self._last_sym_refresh = time.time()
            logger.info(f"Symbols refreshed: {len(syms)}")

    def _setup_symbol(self, symbol: str):
        if cfg.DRY_RUN:
            return
        try:
            self.api.set_margin_type(symbol, cfg.MARGIN_TYPE)
            self.api.set_leverage(symbol, cfg.LEVERAGE)
        except Exception as e:
            logger.debug(f"setup {symbol}: {e}")

    # ── Position monitoring ───────────────────────────────────
    def _monitor_positions(self):
        """
        Detecta posiciones que se han cerrado desde el último ciclo.
        Actualiza stats diarias y notifica Telegram.
        """
        tracked = self.state.get_active_positions()
        if not tracked:
            return

        if cfg.DRY_RUN:
            # En dry-run no hay posiciones reales; skip
            return

        try:
            open_set: Set[str] = self.api.get_active_positions_set()
        except Exception as e:
            logger.debug(f"monitor positions: {e}")
            return

        for symbol, pos in list(tracked.items()):
            if symbol not in open_set:
                # Posición cerrada
                entry      = float(pos.get("entry", 0))
                action     = pos.get("action", "BUY")
                qty        = float(pos.get("qty", 0))

                try:
                    exit_price = self.api.get_price(symbol)
                except Exception:
                    exit_price = entry

                is_long = action == "BUY"
                pnl     = (exit_price - entry) * qty * (1 if is_long else -1) * cfg.LEVERAGE
                won     = pnl > 0

                self.state.remove_active_position(symbol)
                self.state.set_last_exit(symbol)
                self.state.update_daily_result(pnl, won)
                self.state.add_trade({
                    "symbol": symbol, "action": action,
                    "entry": entry, "exit": exit_price,
                    "qty": qty, "pnl": pnl,
                })

                send_close(symbol, action, entry, exit_price, qty, "SL/TP")
                logger.info(f"Position closed {symbol} pnl={pnl:.2f}")

    # ── Daily limit & summary ─────────────────────────────────
    def _check_daily_limit(self, balance: float) -> bool:
        """True si se superó el límite de pérdida diaria."""
        if cfg.DAILY_LOSS_LIMIT_PCT <= 0:
            return False
        pnl   = self.state.get_daily_pnl()
        limit = balance * cfg.DAILY_LOSS_LIMIT_PCT / 100
        if pnl < -limit:
            if not self._daily_limit_hit:
                self._daily_limit_hit = True
                send_daily_loss_limit(pnl, cfg.DAILY_LOSS_LIMIT_PCT)
                logger.warning(f"Daily loss limit: pnl={pnl:.2f} limit={-limit:.2f}")
            return True
        return False

    def _maybe_send_daily_summary(self):
        today   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        hour    = datetime.now(timezone.utc).hour
        if hour == cfg.SUMMARY_HOUR and self.state.get_last_summary_date() != today:
            stats  = self.state.get_daily_stats()
            equity = self.api.get_equity()
            send_daily_summary(stats, equity)
            self.state.set_last_summary_date(today)
            self._daily_limit_hit = False  # reset para el nuevo día

    # ── Ejecución ─────────────────────────────────────────────
    def _execute(
        self,
        symbol:  str,
        signal,
        entry:   float,
        sl:      float,
        tp:      float,
        qty:     float,
        balance: float,
    ):
        is_long  = signal.action == "BUY"
        pos_side = "LONG" if is_long else "SHORT"
        ord_side = "BUY"  if is_long else "SELL"
        sl_side  = "SELL" if is_long else "BUY"

        order_id = None
        try:
            # Entrada
            resp     = self.api.place_order(symbol, ord_side, pos_side, qty)
            order_id = str(resp.get("orderId", resp.get("data", {}).get("orderId", "DRY")))
            time.sleep(0.3)
            # Stop loss
            self.api.place_order(symbol, sl_side, pos_side, qty,
                                 order_type="STOP_MARKET", stop_price=sl)
            time.sleep(0.2)
            # Take profit
            self.api.place_order(symbol, sl_side, pos_side, qty,
                                 order_type="TAKE_PROFIT_MARKET", stop_price=tp)
        except Exception as e:
            logger.error(f"execute {symbol}: {e}")
            send_error(f"{symbol}: {e}")
            return

        # Persistir
        self.state.set_last_signal(symbol, signal.action, int(time.time()), entry)
        self.state.add_active_position(symbol, {
            "action": signal.action, "entry": entry,
            "sl": sl, "tp": tp, "qty": qty,
        })
        if cfg.DRY_RUN:
            self.state.add_paper_position(symbol, {
                "action": signal.action, "entry": entry,
                "sl": sl, "tp": tp, "qty": qty,
            })

        send_signal(symbol, signal, entry, sl, tp, qty, balance, order_id)
        logger.info(
            f"TRADE {signal.action} {symbol} | "
            f"entry={entry:.4f} sl={sl:.4f} tp={tp:.4f} "
            f"RR={rr_ratio(entry,sl,tp)} qty={qty:.6f}"
        )

    # ── Ciclo de scan ─────────────────────────────────────────
    def _scan_cycle(self, balance: float):
        # Posiciones abiertas
        if cfg.DRY_RUN:
            open_syms = set(self.state.get_paper_positions().keys())
        else:
            try:
                open_syms = self.api.get_active_positions_set()
            except Exception:
                open_syms = set()

        slots = cfg.MAX_POSITIONS - len(open_syms)
        if slots <= 0:
            logger.info(f"Max positions ({cfg.MAX_POSITIONS}), skip scan")
            return

        # Fetch paralelo de todas las velas
        candles = fetch_all_candles_parallel(self.api, self._symbols)
        new_trades = 0

        for contract in self._symbols:
            if new_trades >= slots:
                break

            symbol = contract["symbol"]

            if symbol in open_syms:
                continue

            # Re-entry cooldown
            exit_ts = self.state.get_last_exit_ts(symbol)
            if exit_ts > 0:
                bars_since_exit = (time.time() - exit_ts) / self._bar_secs
                if bars_since_exit < cfg.REENTRY_COOLDOWN_BARS:
                    logger.debug(f"{symbol}: re-entry cooldown ({bars_since_exit:.1f}/{cfg.REENTRY_COOLDOWN_BARS} bars)")
                    continue

            df = candles.get(symbol)
            if df is None:
                continue

            # Nueva vela?
            last_ts = int(df["time"].iloc[-1])
            if not self.state.is_new_candle(symbol, last_ts):
                continue
            self.state.set_last_candle_ts(symbol, last_ts)

            # Calcular initial cooldown desde state
            last_sig_ts = self.state.get_last_signal_ts(symbol)
            if last_sig_ts > 0:
                initial_cd = min(int((time.time() - last_sig_ts) / self._bar_secs), 999)
            else:
                initial_cd = 999

            # Analizar
            try:
                signal = self.strategy.analyze(df, initial_bars_since_signal=initial_cd)
            except Exception as e:
                logger.error(f"strategy {symbol}: {e}", exc_info=False)
                continue

            if signal is None:
                continue

            # Filtro de dirección
            if signal.action == "BUY"  and not cfg.LONG_ENABLED:  continue
            if signal.action == "SELL" and not cfg.SHORT_ENABLED: continue

            # Precio actual
            try:
                entry = self.api.get_price(symbol)
            except Exception:
                entry = signal.close

            if entry <= 0:
                continue

            # Filtro: distancia máxima desde señal
            atr_dist = abs(entry - signal.close) / signal.atr if signal.atr > 0 else 0
            if atr_dist > cfg.MAX_ENTRY_ATR_DIST:
                logger.info(f"{symbol}: price moved {atr_dist:.2f} ATR from signal, skip")
                continue

            self._setup_symbol(symbol)

            c_info = self.contracts.get(symbol, {})
            sl, tp = calculate_sl_tp(signal, entry, c_info)

            # Filtro R:R
            if not is_rr_valid(entry, sl, tp):
                logger.info(f"{symbol}: RR={rr_ratio(entry,sl,tp)} < min={cfg.MIN_RR}, skip")
                continue

            qty = calculate_quantity(balance, entry, sl, c_info)
            if qty <= 0:
                continue

            self._execute(symbol, signal, entry, sl, tp, qty, balance)
            new_trades += 1
            open_syms.add(symbol)

    # ── Loop principal ────────────────────────────────────────
    def run(self):
        logger.info("FibStruct Bot v2 starting...")

        # Validación mínima de config
        if not cfg.BINGX_API_KEY:
            logger.error("BINGX_API_KEY not set!")
            return

        # Refresh inicial de símbolos
        self._refresh_symbols()
        if not self._symbols:
            logger.error("No symbols loaded")
            return

        # Balance y equity iniciales
        balance = self.api.get_available_balance()
        equity  = self.api.get_equity()

        send_startup(len(self._symbols), balance, equity)
        logger.info(f"Ready | {len(self._symbols)} symbols | balance={balance:.2f} equity={equity:.2f}")

        SYM_REFRESH = 4 * 3600   # refrescar lista cada 4h

        while True:
            try:
                # Reset daily limit al mediodía UTC si es nuevo día
                self._maybe_send_daily_summary()

                balance = self.api.get_available_balance()

                # Refresh de símbolos periódico
                if time.time() - self._last_sym_refresh > SYM_REFRESH:
                    self._refresh_symbols()

                # Monitoreo de posiciones cerradas
                self._monitor_positions()

                # Límite de pérdida diaria
                if self._check_daily_limit(balance):
                    logger.info("Daily loss limit active, sleeping...")
                    time.sleep(cfg.SCAN_INTERVAL)
                    continue

                logger.info(f"Scan | {len(self._symbols)} syms | bal={balance:.2f}")
                self._scan_cycle(balance)

            except KeyboardInterrupt:
                send_info("Bot detenido manualmente")
                logger.info("Bot stopped by user")
                break
            except Exception as e:
                logger.exception(f"Main loop: {e}")
                send_error(str(e)[:400])

            time.sleep(cfg.SCAN_INTERVAL)
