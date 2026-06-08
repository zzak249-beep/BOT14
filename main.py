"""
GUA-USDT Bot v2 — Orquestador Principal
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

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
    handlers= [logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("main")

_client   = BingXClient()
_notifier = Notifier()
_pm       = PositionManager(_client, _notifier)
_running  = True


async def scan_task() -> None:
    health.register_tick()
    try:
        log.info("── SCAN %s [MODE=%s] ──", config.SYMBOL, config.MODE)

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
            log.warning("Sin datos de velas"); return

        price = candles[-1]["close"]
        log.info("price=%.5f funding=%.4f%% OI=%.0f OB_imbalance=%.3f",
                 price, funding*100, oi, ob_imbalance)

        # ── Monitor posición activa ────────────────────────────────────────────
        if _pm.has_position:
            await _pm.monitor(price)
            return

        if _pm.in_cooldown:
            log.info("En cooldown — esperando"); return

        # ── Señal ──────────────────────────────────────────────────────────────
        sig = strategy.analyze(candles, candles_trend, candles_macro, funding, oi)

        if sig is None:
            log.info("Sin señal (score bajo o condiciones no cumplidas)"); return

        # ── Filtro Order Book ──────────────────────────────────────────────────
        if sig.direction == "SHORT" and ob_imbalance > config.OB_IMBALANCE_THR:
            log.info("OB imbalance +%.2f bloquea SHORT (thr=%.2f) — skip",
                     ob_imbalance, config.OB_IMBALANCE_THR)
            return
        if sig.direction == "LONG" and ob_imbalance < -config.OB_IMBALANCE_THR:
            log.info("OB imbalance %.2f bloquea LONG (thr=%.2f) — skip",
                     ob_imbalance, config.OB_IMBALANCE_THR)
            return

        log.info("✅ SEÑAL %s score=%.0f%% rsi=%.1f adx=%.1f rvol=%.2fx",
                 sig.direction, sig.score*100, sig.rsi, sig.adx, sig.rvol)

        health.register_signal()

        # Telegram siempre
        await _notifier.send_signal(sig)

        # Ejecutar solo en LIVE
        if config.MODE == "LIVE":
            log.info("Ejecutando orden en BingX...")
            await _pm.open_position(sig)
        else:
            log.info("MODE=SIGNAL — señal enviada por Telegram, sin orden real")

    except Exception as e:
        log.error("scan_task error: %s", e, exc_info=True)
        await _notifier.send_error(f"scan_task: {e}")


async def monitor_task() -> None:
    if not _pm.has_position:
        return
    try:
        price = await _client.get_price(config.SYMBOL)
        await _pm.monitor(price)
    except Exception as e:
        log.error("monitor_task: %s", e)


async def heartbeat_task() -> None:
    try:
        price = await _client.get_price(config.SYMBOL)
    except Exception:
        price = 0.0
    status = _pm.status(price)
    await _notifier.send_status(f"GUA @ `{price:.5f}`\n{status}\n⚙️ Modo: *{config.MODE}*")


async def main() -> None:
    global _running
    log.info("═══════════════════════════════════")
    log.info("  GUA-USDT Bot v2 — SMC Edition")
    log.info("  Modo: %s | %s", config.MODE, config.SYMBOL)
    log.info("  Session filter: %s", config.SESSION_FILTER)
    log.info("  Score threshold: %.0f%%", config.SCORE_THR * 100)
    log.info("  OB imbalance thr: %.2f", config.OB_IMBALANCE_THR)
    log.info("═══════════════════════════════════")

    await health.start_health_server()
    await _notifier.send_startup()

    scheduler = AsyncIOScheduler(timezone="UTC")

    scheduler.add_job(scan_task, "cron",
                      minute="*/3", second=5,
                      id="scan", max_instances=1,
                      misfire_grace_time=30)

    # Monitor de posición cada 5s siempre activo (necesario para cerrar trades en LIVE)
    scheduler.add_job(monitor_task, "interval",
                      seconds=5, id="monitor", max_instances=1)

    scheduler.add_job(heartbeat_task, "interval",
                      minutes=30, id="heartbeat")

    scheduler.start()
    log.info("Scheduler iniciado. Primer scan en 3s...")

    await asyncio.sleep(3)
    await scan_task()

    loop = asyncio.get_event_loop()
    def _stop(*_):
        global _running
        _running = False
        log.info("Señal de parada recibida")
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _stop)

    while _running:
        await asyncio.sleep(1)

    scheduler.shutdown(wait=False)
    await _client.close()
    await _notifier.close()
    log.info("Bot detenido limpiamente")


if __name__ == "__main__":
    asyncio.run(main())
