"""
QF Machine × JP Fusion — Trading Bot v3.0
Exchanges: BingX Perpetual Futures
Timeframe: 3min primary | 15min HTF confirmation
"""
import asyncio
import logging
import signal
import sys
from datetime import datetime

from config import cfg
from bot.engine import QFJPEngine
from bot.bingx_client import BingXClient
from bot.telegram_client import TelegramClient
from bot.risk_manager import RiskManager
from bot.session_filter import SessionFilter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("QF-BOT")

# ─── Estado global de posiciones activas ───────────────────────
active_positions: dict = {}   # symbol → {side, entry, sl, size, conv}

async def run_symbol(symbol: str, exchange: BingXClient,
                     tg: TelegramClient, risk: RiskManager,
                     session: SessionFilter, engine: QFJPEngine):
    """Loop por símbolo — corre en paralelo."""
    log.info(f"[{symbol}] Iniciando loop")

    while True:
        try:
            # ── 1. Verificar sesión ─────────────────────────────
            if not session.is_tradeable():
                await asyncio.sleep(30)
                continue

            # ── 2. Obtener velas 3min (200 barras) ─────────────
            ohlcv_3m  = await exchange.get_klines(symbol, "3m",  limit=250)
            ohlcv_15m = await exchange.get_klines(symbol, "15m", limit=100)

            if len(ohlcv_3m) < 100 or len(ohlcv_15m) < 30:
                await asyncio.sleep(5)
                continue

            # ── 3. Calcular señal ───────────────────────────────
            sig = engine.compute(ohlcv_3m, ohlcv_15m)
            log.debug(f"[{symbol}] Signal: {sig['direction']} | Conv: {sig['conviction']}/10")

            # ── 4. Gestión de posición activa ───────────────────
            pos = active_positions.get(symbol)
            if pos:
                # Check SL dinámico
                ticker = await exchange.get_ticker(symbol)
                price  = ticker["last"]
                sl_hit = (pos["side"] == "LONG"  and price <= pos["sl"]) or \
                         (pos["side"] == "SHORT" and price >= pos["sl"])
                if sl_hit:
                    await exchange.close_position(symbol, pos["side"])
                    pnl_pct = ((price - pos["entry"]) / pos["entry"] * 100
                               if pos["side"] == "LONG"
                               else (pos["entry"] - price) / pos["entry"] * 100)
                    await tg.send_close(symbol, pos["side"], pos["entry"],
                                        price, pnl_pct, reason="SL alcanzado")
                    del active_positions[symbol]
                # Señal contraria → cerrar y esperar
                elif sig["direction"] and sig["direction"] != pos["side"] and sig["conviction"] >= 7:
                    await exchange.close_position(symbol, pos["side"])
                    ticker = await exchange.get_ticker(symbol)
                    price  = ticker["last"]
                    pnl_pct = ((price - pos["entry"]) / pos["entry"] * 100
                               if pos["side"] == "LONG"
                               else (pos["entry"] - price) / pos["entry"] * 100)
                    await tg.send_close(symbol, pos["side"], pos["entry"],
                                        price, pnl_pct, reason="Señal contraria")
                    del active_positions[symbol]

            # ── 5. Nueva entrada ────────────────────────────────
            if symbol not in active_positions and sig["direction"]:
                tier = sig["tier"]   # "STD" | "FUEL" | "SUP"
                conv = sig["conviction"]

                # Filtro mínimo de calidad
                min_conv = cfg.MIN_CONV_STD  if tier == "STD" else \
                           cfg.MIN_CONV_FUEL if tier == "FUEL" else \
                           cfg.MIN_CONV_SUP

                if conv < min_conv:
                    await asyncio.sleep(cfg.LOOP_INTERVAL)
                    continue

                ticker = await exchange.get_ticker(symbol)
                price  = ticker["last"]

                # Tamaño de posición basado en riesgo
                size = risk.position_size(
                    balance   = await exchange.get_balance(),
                    entry     = price,
                    stop_loss = sig["sl"],
                    risk_pct  = cfg.RISK_PER_TRADE_PCT,
                    leverage  = cfg.LEVERAGE,
                )

                if size <= 0:
                    log.warning(f"[{symbol}] Tamaño 0 — balance insuficiente")
                    await asyncio.sleep(cfg.LOOP_INTERVAL)
                    continue

                # Ejecutar orden
                order = await exchange.place_order(
                    symbol   = symbol,
                    side     = sig["direction"],
                    size     = size,
                    leverage = cfg.LEVERAGE,
                    sl_price = sig["sl"],
                    tp_price = sig.get("tp"),
                )

                if order and order.get("orderId"):
                    active_positions[symbol] = {
                        "side"  : sig["direction"],
                        "entry" : price,
                        "sl"    : sig["sl"],
                        "size"  : size,
                        "conv"  : conv,
                        "tier"  : tier,
                        "time"  : datetime.utcnow(),
                    }
                    await tg.send_entry(symbol, sig, price, size, order["orderId"])
                    log.info(f"[{symbol}] ✅ {sig['direction']} | {tier} | Conv:{conv}/10 | SL:{sig['sl']:.4f}")

        except asyncio.CancelledError:
            log.info(f"[{symbol}] Loop cancelado")
            break
        except Exception as exc:
            log.error(f"[{symbol}] Error: {exc}", exc_info=True)
            await tg.send_error(str(exc))

        await asyncio.sleep(cfg.LOOP_INTERVAL)


async def status_loop(tg: TelegramClient, exchange: BingXClient):
    """Envía resumen cada hora."""
    while True:
        await asyncio.sleep(3600)
        try:
            bal = await exchange.get_balance()
            await tg.send_status(bal, active_positions)
        except Exception as e:
            log.error(f"Status loop error: {e}")


async def main():
    log.info("═══════════════════════════════════════")
    log.info("  QF × JP Bot v3.0  |  BingX Futures  ")
    log.info("═══════════════════════════════════════")

    tg       = TelegramClient(cfg.TG_TOKEN, cfg.TG_CHAT_ID)
    exchange = BingXClient(cfg.BINGX_API_KEY, cfg.BINGX_SECRET)
    risk     = RiskManager()
    session  = SessionFilter()
    engine   = QFJPEngine()

    await tg.send_message("🟢 *QF×JP Bot v3 iniciado*\nMercados: " +
                          ", ".join(cfg.SYMBOLS) +
                          f"\nLeverage: {cfg.LEVERAGE}× | Riesgo/trade: {cfg.RISK_PER_TRADE_PCT}%")

    # Verificar conexión exchange
    bal = await exchange.get_balance()
    log.info(f"Balance USDT: {bal:.2f}")

    tasks = [run_symbol(s, exchange, tg, risk, session, engine)
             for s in cfg.SYMBOLS]
    tasks.append(status_loop(tg, exchange))

    # Shutdown limpio
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: [t.cancel() for t in asyncio.all_tasks()])

    await asyncio.gather(*tasks, return_exceptions=True)
    await tg.send_message("🔴 *Bot detenido*")


if __name__ == "__main__":
    asyncio.run(main())
