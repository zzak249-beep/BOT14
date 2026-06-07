"""
GUA-USDT Bot v2 — Orquestador Principal
3 timeframes (3m · 15m · 1h) · Order Book Imbalance · Sesiones London+NY.
"""

from __future__ import annotations
import asyncio, logging, signal, sys, time

from apscheduler.schedulers.asyncio import AsyncIOScheduler

import config
import health
from exchange import BingXClient
from notifier import Notifier
from position_manager import PositionManager
import strategy

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
    handlers= [logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("main")

# ── Globals ────────────────────────────────────────────────────────────────────
_client   = BingXClient()
_notifier = Notifier()
_pm       = PositionManager(_client, _notifier)
_running  = True


# ── Scan principal (cada 3 min) ────────────────────────────────────────────────

async def scan_task() -> None:
    health.register_tick()
    try:
        log.info("── SCAN %s ──", config.SYMBOL)

        # Fetch datos en paralelo (más rápido)
        (candles, candles_trend, candles_macro,
         funding, oi, ob_imbalance) = await asyncio.gather(
            _client.get_klines(config.SYMBOL, config.INTERVAL,       config.LOOKBACK),
            _client.get_klines(config.SYMBOL, config.INTERVAL_TREND, config.LOOKBACK_TREND),
            _client.get_klines(config.SYMBOL, config.INTERVAL_MACRO, config.LOOKBACK_MACRO),
            _client.get_funding_rate(config.SYMBOL),
            _client.get_open_interest(config.SYMBOL),
            _client.get_order_book_imbalance(config.SYMBOL),
        )

        if not candles:
            log.warning("Sin datos"); return

        price = candles[-1]["close"]
        log.info(
            "price=%.5f funding=%.4f%% OI=%.0f OB_imbalance=%.3f",
            price, funding*100, oi, ob_imbalance,
        )

        # Monitor posición activa
        if _pm.has_position:
            await _pm.monitor(price)
            return

        if _pm.in_cooldown:
            log.info("En cooldown"); return

        # Señal
        sig = strategy.analyze(
            candles, candles_trend, candles_macro, funding, oi
        )

        if sig is None:
            log.info("Sin señal"); return

        # Filtro extra: order book imbalance debe alinearse con la dirección
        if sig.direction == "SHORT" and ob_imbalance > 0.3:
            log.info("OB imbalance positivo (%.2f) vs SHORT — descartando", ob_imbalance)
            return
        if sig.direction == "LONG" and ob_imbalance < -0.3:
            log.info("OB imbalance negativo (%.2f) vs LONG — descartando", ob_imbalance)
            return

        log.info("SEÑAL %s score=%.0f%% rsi=%.1f adx=%.1f rvol=%.2fx sqz=%s",
                 sig.direction, sig.score*100, sig.rsi, sig.adx, sig.rvol, sig.squeeze)

        health.register_signal()
        await _notifier.send_signal(sig)

        if config.MODE == "LIVE":
            await _pm.open_position(sig)

    except Exception as e:
        log.error("scan_task: %s", e, exc_info=True)
        await _notifier.send_error(f"scan_task: {e}")


# ── Monitor precio cada 5s (LIVE) ─────────────────────────────────────────────

async def monitor_task() -> None:
    if not _pm.has_position:
        return
    try:
        price = await _client.get_price(config.SYMBOL)
        await _pm.monitor(price)
    except Exception as e:
        log.error("monitor_task: %s", e)


# ── Heartbeat cada 30 min ──────────────────────────────────────────────────────

async def heartbeat_task() -> None:
    try:
        price = await _client.get_price(config.SYMBOL)
    except Exception:
        price = 0.0
    status = _pm.status(price)
    await _notifier.send_status(f"GUA @ `{price:.5f}`\n{status}")


# ── Main ───────────────────────────────────────────────────────────────────────

async def main() -> None:
    global _running
    log.info("═══════════════════════════════════")
    log.info("  GUA-USDT Bot v2 — SMC Edition")
    log.info("  Modo: %s | %s", config.MODE, config.SYMBOL)
    log.info("  TFs: %s · %s · %s", config.INTERVAL, config.INTERVAL_TREND, config.INTERVAL_MACRO)
    log.info("═══════════════════════════════════")

    await health.start_health_server()
    await _notifier.send_startup()

    scheduler = AsyncIOScheduler(timezone="UTC")

    scheduler.add_job(scan_task, "cron",
                      minute="*/3", second=5,
                      id="scan", max_instances=1,
                      misfire_grace_time=30)

    if config.MODE == "LIVE":
        scheduler.add_job(monitor_task, "interval",
                          seconds=5, id="monitor", max_instances=1)

    scheduler.add_job(heartbeat_task, "interval",
                      minutes=30, id="heartbeat")

    scheduler.start()
    log.info("Scheduler activo. Primer scan en el siguiente múltiplo de 3 min")

    await asyncio.sleep(3)
    await scan_task()   # scan inmediato al arrancar

    loop = asyncio.get_event_loop()
    def _stop(*_):
        global _running; _running = False
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _stop)

    while _running:
        await asyncio.sleep(1)

    scheduler.shutdown(wait=False)
    await _client.close()
    await _notifier.close()
    log.info("Bot detenido")


if __name__ == "__main__":
    asyncio.run(main())
