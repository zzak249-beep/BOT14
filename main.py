"""
QF×JP Bot v6.0 — main.py
Mejoras vs v5:
  ✅ Balance cacheado (fix error 100410)
  ✅ Fix RuntimeWarning divide (np.errstate en engine)
  ✅ Composite entry: Score + CVD + Momentum + Decay
  ✅ Volumen 50M-600M USDT (sweet spot BingX)
  ✅ Reconciliación de posiciones con BingX real
  ✅ Retry exponencial en todos los endpoints
  ✅ Maker → timeout → Market fallback
  ✅ Anti-cancelación (respeta regla BingX 80%)
  ✅ SL/TP automáticos con ATR dinámico
  ✅ Trailing SL por ATR
"""
import asyncio
import logging
import signal
import sys
import time
from datetime import datetime, timezone

from config      import cfg
from engine      import QFJPEngine, compute_with_edge
from edge        import EdgeEngine
from bingx_client import BingXClient
from telegram_client import TelegramClient
from risk_manager import RiskManager
from session_filter import SessionFilter
from scanner     import MarketScanner
from performance import PerformanceTracker, TradeRecord

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("MAIN")

# ── Estado global ─────────────────────────────────────────
# symbol → {side, entry, sl, tp, size, conv, tier, time, atr, trail_active, trail_sl}
active_positions: dict = {}
prev_oi: dict[str, float] = {}

# ── Locks por símbolo para evitar condición de carrera ────
_sym_locks: dict[str, asyncio.Lock] = {}
def _lock(symbol: str) -> asyncio.Lock:
    if symbol not in _sym_locks:
        _sym_locks[symbol] = asyncio.Lock()
    return _sym_locks[symbol]


# ── Reconciliación: sincronizar estado con BingX ──────────
async def reconcile_positions(exchange: BingXClient):
    """Sincroniza active_positions con las posiciones reales en BingX."""
    try:
        real_pos = await exchange.get_open_positions()
        real_syms = {p["symbol"] for p in real_pos}

        # Cerrar locales que ya no existen en BingX
        for sym in list(active_positions.keys()):
            if sym not in real_syms:
                log.info(f"[{sym}] Posición cerrada externamente — eliminando local")
                del active_positions[sym]

        # Añadir posiciones reales que no conocemos
        for p in real_pos:
            sym = p["symbol"]
            if sym not in active_positions:
                log.info(f"[{sym}] Posición externa detectada — agregando estado")
                active_positions[sym] = {
                    "side": p["side"], "entry": p["entry"],
                    "sl": None, "tp": None, "size": p["size"],
                    "conv": 0, "tier": "STD",
                    "time": datetime.utcnow(),
                    "atr": 0.0, "trail_active": False, "trail_sl": None,
                }
    except Exception as e:
        log.error(f"reconcile error: {e}")


# ── Loop de un símbolo ────────────────────────────────────
async def run_symbol(
    symbol: str, exchange: BingXClient, tg: TelegramClient,
    risk: RiskManager, session: SessionFilter,
    engine: QFJPEngine, perf: PerformanceTracker,
    start_bal: float,
):
    log.info(f"[{symbol}] Task iniciada")
    daily_bal = [start_bal]

    while True:
        try:
            async with _lock(symbol):
                # ── 1. Sesión ────────────────────────────
                if not session.is_tradeable():
                    await asyncio.sleep(30)
                    continue

                # ── 2. Performance filter ─────────────────
                if not perf.is_tradeable(symbol):
                    await asyncio.sleep(60)
                    continue

                # ── 3. Drawdown diario (cacheado) ─────────
                bal = await exchange.get_balance()
                if not risk.max_daily_loss_ok(daily_bal[0], bal, cfg.MAX_DAILY_DD_PCT):
                    await tg.send_message(f"⛔ *DD diario en {symbol}* — pausa 1h")
                    await asyncio.sleep(3600)
                    continue

                # ── 4. Límite posiciones ──────────────────
                if symbol not in active_positions and len(active_positions) >= cfg.MAX_OPEN_POSITIONS:
                    await asyncio.sleep(cfg.LOOP_INTERVAL)
                    continue

                # ── 5. Velas multi-TF ──────────────────────
                ohlcv_3m, ohlcv_15m, ohlcv_1h, ohlcv_1m = await asyncio.gather(
                    exchange.get_klines(symbol, "3m",  250),
                    exchange.get_klines(symbol, "15m", 100),
                    exchange.get_klines(symbol, "1h",  60),
                    exchange.get_klines(symbol, "1m",  60),
                    return_exceptions=True,
                )
                if isinstance(ohlcv_3m, Exception) or len(ohlcv_3m) < 100:
                    await asyncio.sleep(10)
                    continue
                ohlcv_15m = ohlcv_15m if not isinstance(ohlcv_15m, Exception) else []
                ohlcv_1h  = ohlcv_1h  if not isinstance(ohlcv_1h,  Exception) else []
                ohlcv_1m  = ohlcv_1m  if not isinstance(ohlcv_1m,  Exception) else []

                # ── 6. Market context ─────────────────────
                mctx = await exchange.get_market_context(symbol, cfg.OFI_LEVELS)
                mctx["prev_open_interest"] = prev_oi.get(symbol, mctx["open_interest"])
                prev_oi[symbol] = mctx["open_interest"]

                # ── 7. Señal ──────────────────────────────
                sig = compute_with_edge(ohlcv_3m, ohlcv_15m, ohlcv_1h, ohlcv_1m, mctx)

                # ── 8. Precio actual ──────────────────────
                ticker = await exchange.get_ticker(symbol)
                price  = ticker["last"]
                if price <= 0:
                    await asyncio.sleep(10)
                    continue

                # ── 9. Gestión posición activa ────────────
                pos = active_positions.get(symbol)
                if pos:
                    atr_pos = pos.get("atr", 0)

                    # Trailing SL
                    if atr_pos > 0 and pos["sl"] is not None:
                        activate = atr_pos * cfg.TRAIL_ACTIVATE_ATR

                        if pos["side"] == "LONG":
                            profit = price - pos["entry"]
                            if not pos.get("trail_active") and profit >= activate:
                                pos["trail_active"] = True
                                pos["trail_sl"]     = price - atr_pos * cfg.TRAIL_ATR_MULT
                                log.info(f"[{symbol}] Trail LONG activado @ {pos['trail_sl']:.6f}")
                            if pos.get("trail_active"):
                                new_tsl = price - atr_pos * cfg.TRAIL_ATR_MULT
                                if new_tsl > pos.get("trail_sl", pos["sl"]):
                                    pos["trail_sl"] = new_tsl
                                pos["sl"] = max(pos["sl"], pos["trail_sl"])

                        elif pos["side"] == "SHORT":
                            profit = pos["entry"] - price
                            if not pos.get("trail_active") and profit >= activate:
                                pos["trail_active"] = True
                                pos["trail_sl"]     = price + atr_pos * cfg.TRAIL_ATR_MULT
                                log.info(f"[{symbol}] Trail SHORT activado @ {pos['trail_sl']:.6f}")
                            if pos.get("trail_active"):
                                new_tsl = price + atr_pos * cfg.TRAIL_ATR_MULT
                                if new_tsl < pos.get("trail_sl", pos["sl"]):
                                    pos["trail_sl"] = new_tsl
                                pos["sl"] = min(pos["sl"], pos["trail_sl"])

                    # SL / TP / Reversal check
                    sl_hit = (
                        (pos["side"] == "LONG"  and price <= pos["sl"]) or
                        (pos["side"] == "SHORT" and price >= pos["sl"])
                    ) if pos["sl"] else False

                    tp_hit = (
                        pos.get("tp") and (
                            (pos["side"] == "LONG"  and price >= pos["tp"]) or
                            (pos["side"] == "SHORT" and price <= pos["tp"])
                        )
                    )

                    rev_signal = (
                        sig["direction"] and
                        sig["direction"] != pos["side"] and
                        sig["conviction"] >= 7 and
                        engine.should_enter(sig, cfg.ENTRY_MIN_COMPOSITE)
                    )

                    close_reason = None
                    if sl_hit:
                        close_reason = "SL" + (" (trailing)" if pos.get("trail_active") else "")
                    elif tp_hit:
                        close_reason = "TP"
                    elif rev_signal:
                        close_reason = "Señal contraria"

                    if close_reason:
                        if cfg.MODE == "LIVE":
                            await exchange.close_position(symbol, pos["side"])
                        pnl = (
                            (price - pos["entry"]) / pos["entry"] * 100
                            if pos["side"] == "LONG"
                            else (pos["entry"] - price) / pos["entry"] * 100
                        )
                        await tg.send_close(
                            symbol, pos["side"], pos["entry"], price, pnl,
                            close_reason, pos.get("trail_active", False)
                        )
                        perf.record(TradeRecord(
                            symbol=symbol, side=pos["side"],
                            entry=pos["entry"], exit=price,
                            pnl_pct=pnl, conviction=pos["conv"],
                            tier=pos["tier"],
                        ))
                        del active_positions[symbol]

                # ── 10. Nueva entrada ─────────────────────
                if symbol not in active_positions:
                    # Filtro compuesto Score+CVD+Momentum+Decay
                    if not engine.should_enter(sig, cfg.ENTRY_MIN_COMPOSITE):
                        await asyncio.sleep(cfg.LOOP_INTERVAL)
                        continue

                    direction = sig["direction"]
                    tier      = sig["tier"]
                    conv      = sig["conviction"]

                    min_c = (cfg.MIN_CONV_SUP  if tier == "SUP"  else
                             cfg.MIN_CONV_FUEL if tier == "FUEL" else
                             cfg.MIN_CONV_STD)
                    if conv < min_c:
                        await asyncio.sleep(cfg.LOOP_INTERVAL)
                        continue

                    sl = sig["sl"]; tp = sig.get("tp")
                    if not sl:
                        await asyncio.sleep(cfg.LOOP_INTERVAL)
                        continue

                    size = risk.position_size(
                        bal, price, sl, cfg.RISK_PER_TRADE_PCT, cfg.LEVERAGE
                    )
                    if size <= 0:
                        await asyncio.sleep(cfg.LOOP_INTERVAL)
                        continue

                    order_id = "SIGNAL_ONLY"
                    if cfg.MODE == "LIVE":
                        order = await exchange.place_order(
                            symbol, direction, size, cfg.LEVERAGE, sl, tp,
                            use_maker=cfg.USE_MAKER_ORDERS,
                            maker_timeout=cfg.MAKER_TIMEOUT,
                            maker_offset_pct=cfg.MAKER_OFFSET_PCT,
                        )
                        if not order:
                            await asyncio.sleep(cfg.LOOP_INTERVAL)
                            continue
                        order_id = order.get("orderId", "?")
                        # Refrescar balance tras orden
                        bal = await exchange.get_balance(force=True)

                    active_positions[symbol] = dict(
                        side=direction, entry=price, sl=sl, tp=tp,
                        size=size, conv=conv, tier=tier,
                        time=datetime.utcnow(),
                        atr=sig.get("atr_last", 0),
                        trail_active=False, trail_sl=None,
                    )

                    await tg.send_entry(symbol, sig, price, size, order_id, mctx)
                    log.info(
                        f"[{symbol}] {direction} {tier} conv={conv}/10 "
                        f"FINAL={sig.get('final_score',0):.2f} "
                        f"edge={sig.get('edge_score',0):+.2f}({sig.get('edge_dir','?')}) "
                        f"signals B{sig.get('edge_signals_bull',0)}/S{sig.get('edge_signals_bear',0)} "
                        f"cvd={sig['cvd_bias']} mom={sig['momentum']:+.0f} "
                        f"decay={sig['decay_ratio']:.2f} OFI={sig['ofi']:+.3f}"
                    )

        except asyncio.CancelledError:
            break
        except Exception as e:
            log.error(f"[{symbol}] {e}", exc_info=True)
            await tg.send_error(f"[{symbol}] {e}")

        await asyncio.sleep(cfg.LOOP_INTERVAL)


# ── Scanner loop ──────────────────────────────────────────
async def scanner_loop(
    exchange: BingXClient, tg: TelegramClient,
    perf: PerformanceTracker, engine: QFJPEngine,
    risk: RiskManager, session: SessionFilter,
):
    scanner = MarketScanner(exchange)
    tasks: dict[str, asyncio.Task] = {}
    reconcile_counter = 0

    while True:
        try:
            symbols = await scanner.get_tradeable_symbols()
            gs      = perf.global_stats()

            if gs:
                await tg.send_message(
                    f"🔍 *Scanner QF×JP v6 — {len(symbols)} pares*\n"
                    f"📊 WR={gs['win_rate']:.0%} | PF={gs['profit_factor']:.2f} | "
                    f"avg={gs['avg_pnl']:+.2f}% | trades={gs['total_trades']}\n"
                    f"⛔ Suspendidos: {', '.join(gs['suspended']) or 'ninguno'}"
                )

            bal = await exchange.get_balance()

            # Reconciliar cada 10 ciclos de scanner
            reconcile_counter += 1
            if reconcile_counter % 10 == 0 and cfg.MODE == "LIVE":
                await reconcile_positions(exchange)

            for sym in symbols:
                if sym not in tasks or tasks[sym].done():
                    t = asyncio.create_task(
                        run_symbol(sym, exchange, tg, risk, session, engine, perf, bal)
                    )
                    tasks[sym] = t
                    log.info(f"Task iniciada: {sym}")

            for sym in list(tasks.keys()):
                if sym not in symbols and not tasks[sym].done():
                    tasks[sym].cancel()
                    del tasks[sym]
                    log.info(f"Task cancelada: {sym}")

        except Exception as e:
            log.error(f"scanner_loop: {e}", exc_info=True)

        await asyncio.sleep(cfg.SCANNER_INTERVAL)


# ── Status loop ───────────────────────────────────────────
async def status_loop(tg: TelegramClient, exchange: BingXClient, perf: PerformanceTracker):
    while True:
        await asyncio.sleep(3600)
        try:
            bal = await exchange.get_balance(force=True)
            gs  = perf.global_stats()
            await tg.send_status(bal, active_positions, gs)
        except Exception as e:
            log.error(f"status_loop: {e}")


# ── Main ──────────────────────────────────────────────────
async def main():
    log.info("═══════════════════════════════════════════════")
    log.info("  QF×JP Bot v6.0  |  BingX Perpetual Futures")
    log.info(f"  MODE={cfg.MODE} | LEVERAGE={cfg.LEVERAGE}× | RISK={cfg.RISK_PER_TRADE_PCT}%")
    log.info(f"  ENTRY_MIN_COMPOSITE={cfg.ENTRY_MIN_COMPOSITE}")
    log.info(f"  VOL_RANGE={cfg.MIN_VOLUME_USDT/1e6:.0f}M–{cfg.MAX_VOLUME_USDT/1e6:.0f}M USDT")
    log.info(f"  MAKER={'ON' if cfg.USE_MAKER_ORDERS else 'OFF'} | TRAIL={cfg.TRAIL_ACTIVATE_ATR}×ATR")
    log.info(f"  MAX_POS={cfg.MAX_OPEN_POSITIONS} | MAX_DD={cfg.MAX_DAILY_DD_PCT}%")
    log.info("═══════════════════════════════════════════════")

    tg       = TelegramClient(cfg.TG_TOKEN, cfg.TG_CHAT_ID)
    exchange = BingXClient(cfg.BINGX_API_KEY, cfg.BINGX_SECRET)
    risk     = RiskManager()
    session  = SessionFilter()
    engine   = QFJPEngine()
    perf     = PerformanceTracker(cfg.PF_WINDOW, cfg.MIN_PF)

    bal = await exchange.get_balance(force=True)

    maker_fee = "0.04% (Maker)" if cfg.USE_MAKER_ORDERS else "0.15% (Market)"
    await tg.send_message(
        f"🟢 *QF×JP Bot v6.0 iniciado*\n"
        f"Modo: {'🔴 *LIVE*' if cfg.MODE == 'LIVE' else '🟡 SIGNAL ONLY'}\n"
        f"Balance: `{bal:.2f} USDT`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 Score final min: `{cfg.ENTRY_MIN_COMPOSITE:.0%}`\n"
        f"  Composite `55%` + Edge institucional `45%`\n"
        f"  (Score`40%`+CVD`25%`+Mom`20%`+Decay`15%`)\n"
        f"🔬 Edge: FVG·OB·BOS·CHoCH·LiqSweep·CVDdiv·DeltaExh·VPOC·DarkPool\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 Vol: `{cfg.MIN_VOLUME_USDT/1e6:.0f}M–{cfg.MAX_VOLUME_USDT/1e6:.0f}M USDT`\n"
        f"💵 Lev: `{cfg.LEVERAGE}×` | Riesgo: `{cfg.RISK_PER_TRADE_PCT}%`\n"
        f"🛑 Max DD: `{cfg.MAX_DAILY_DD_PCT}%` | MaxPos: `{cfg.MAX_OPEN_POSITIONS}`\n"
        f"💸 Fee: `{maker_fee}`\n"
        f"🔁 Trail: `@{cfg.TRAIL_ACTIVATE_ATR}×ATR`\n"
        f"🌐 Multi-TF: `1m+3m+15m+1h`\n"
        f"📅 Sesiones: `{', '.join(cfg.ALLOWED_SESSIONS)}`"
    )

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig, lambda: [t.cancel() for t in asyncio.all_tasks()]
        )

    try:
        await asyncio.gather(
            scanner_loop(exchange, tg, perf, engine, risk, session),
            status_loop(tg, exchange, perf),
            return_exceptions=True,
        )
    finally:
        await exchange.close()
        await tg.send_message("🔴 *QF×JP Bot v6 detenido*")


if __name__ == "__main__":
    asyncio.run(main())
