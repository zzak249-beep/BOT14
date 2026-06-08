"""
GUA-USDT Bot v3 — Orquestador Principal
Nuevo: BTC paralelo · microstructure task cada 30s · cooldown dinámico · trade CSV logging
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

# Cache de microestructura para el scan principal
_ob_imbalance_cache: float = 0.0


# ── Microstructure task — cada 30s (anticipa cambios de dirección) ────────────

async def microstructure_task() -> None:
    """
    Lee el order book cada 30s mientras el scan es cada 3min.
    Detecta presión compradora/vendedora ANTES del cierre de vela.
    """
    global _ob_imbalance_cache
    try:
        _ob_imbalance_cache = await _client.get_order_book_imbalance(config.SYMBOL, depth=50)
        log.debug("OB imbalance: %.3f", _ob_imbalance_cache)
    except Exception as e:
        log.debug("microstructure_task: %s", e)


# ── Scan principal — cada 3 min ───────────────────────────────────────────────

async def scan_task() -> None:
    health.register_tick()
    try:
        log.info("── SCAN %s ──", config.SYMBOL)

        # Fetch en paralelo: klines 3m + 15m + 1h + BTC 3m + funding + OI
        (candles, candles_trend, candles_macro, btc_candles,
         funding, oi) = await asyncio.gather(
            _client.get_klines(config.SYMBOL,     config.INTERVAL,       config.LOOKBACK),
            _client.get_klines(config.SYMBOL,     config.INTERVAL_TREND, config.LOOKBACK_TREND),
            _client.get_klines(config.SYMBOL,     config.INTERVAL_MACRO, config.LOOKBACK_MACRO),
            _client.get_klines(config.BTC_SYMBOL, config.INTERVAL,       60),  # BTC 3m para rel strength
            _client.get_funding_rate(config.SYMBOL),
            _client.get_open_interest(config.SYMBOL),
        )

        if not candles:
            log.warning("Sin datos de klines"); return

        price = candles[-1]["close"]
        log.info(
            "price=%.5f funding=%.4f%% OI=%.0f OB_imbalance=%.3f",
            price, funding * 100, oi, _ob_imbalance_cache,
        )

        # Gestión de posición activa
        if _pm.has_position:
            await _pm.monitor(price)
            return

        if _pm.in_cooldown:
            log.info("En cooldown"); return

        # Análisis — ahora incluye BTC para fortaleza relativa
        sig = strategy.analyze(
            candles        = candles,
            candles_trend  = candles_trend,
            candles_macro  = candles_macro,
            funding_rate   = funding,
            open_interest  = oi,
            btc_candles    = btc_candles,
        )

        if sig is None:
            log.info("Sin señal"); return

        # Filtro OB imbalance alineado con dirección
        if sig.direction == "SHORT" and _ob_imbalance_cache > 0.35:
            log.info("OB imbalance +%.2f vs SHORT — descartando", _ob_imbalance_cache)
            return
        if sig.direction == "LONG" and _ob_imbalance_cache < -0.35:
            log.info("OB imbalance %.2f vs LONG — descartando", _ob_imbalance_cache)
            return

        log.info(
            "SEÑAL %s score=%.0f%% rsi=%.1f mfi=%.1f adx=%.1f rvol=%.2fx "
            "compress=%s relStr=%.4f liqCandle=%s fundPre=%s",
            sig.direction, sig.score * 100, sig.rsi, sig.mfi, sig.adx, sig.rvol,
            sig.compression, sig.rel_strength, sig.liq_candle, sig.funding_pre,
        )

        # Log desglose de score
        if sig.components:
            top = sorted(sig.components.items(), key=lambda x: -abs(x[1]))[:5]
            log.info("Score top-5: %s", " | ".join(f"{k}={v:.3f}" for k, v in top))

        health.register_signal()
        await _notifier.send_signal(sig)

        if config.MODE == "LIVE":
            await _pm.open_position(sig)

    except Exception as e:
        log.error("scan_task: %s", e, exc_info=True)
        await _notifier.send_error(f"scan_task: {e}")


# ── Monitor precio cada 5s (LIVE) ────────────────────────────────────────────

async def monitor_task() -> None:
    if not _pm.has_position:
        return
    try:
        price = await _client.get_price(config.SYMBOL)
        await _pm.monitor(price)
    except Exception as e:
        log.error("monitor_task: %s", e)


# ── Heartbeat cada 30 min ─────────────────────────────────────────────────────

async def heartbeat_task() -> None:
    try:
        price = await _client.get_price(config.SYMBOL)
    except Exception:
        price = 0.0
    wr = strategy._wf_win_rate()
    thr = strategy._adaptive_threshold()
    status = _pm.status(price)
    await _notifier.send_status(
        f"GUA @ `{price:.5f}`\n"
        f"WR={wr:.0%} | Umbral={thr:.0%} | OB={_ob_imbalance_cache:.3f}\n"
        f"{status}"
    )


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    global _running
    log.info("═══════════════════════════════════════")
    log.info("  GUA-USDT Bot v3 — SMC + Anticipación")
    log.info("  Modo: %s | %s", config.MODE, config.SYMBOL)
    log.info("  TFs: %s · %s · %s | BTC: %s",
             config.INTERVAL, config.INTERVAL_TREND, config.INTERVAL_MACRO, config.BTC_SYMBOL)
    log.info("  LOOKBACK=%d (EMA200 válida)", config.LOOKBACK)
    log.info("  OI_HISTORY=%d (%.0f min)", config.OI_HISTORY_LEN, config.OI_HISTORY_LEN * 3)
    log.info("═══════════════════════════════════════")

    await health.start_health_server()
    await _notifier.send_startup()

    scheduler = AsyncIOScheduler(timezone="UTC")

    scheduler.add_job(scan_task, "cron",
                      minute="*/3", second=5,
                      id="scan", max_instances=1,
                      misfire_grace_time=30)

    # Microestructura cada 30s — anticipa el cierre de vela
    scheduler.add_job(microstructure_task, "interval",
                      seconds=30, id="microstructure", max_instances=1)

    if config.MODE == "LIVE":
        scheduler.add_job(monitor_task, "interval",
                          seconds=5, id="monitor", max_instances=1)

    scheduler.add_job(heartbeat_task, "interval",
                      minutes=30, id="heartbeat")

    scheduler.start()
    log.info("Scheduler activo. Primer scan inmediato.")

    # Scan inmediato al arrancar
    await asyncio.sleep(2)
    await microstructure_task()
    await scan_task()

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
    log.info("Bot v3 detenido")


if __name__ == "__main__":
    asyncio.run(main())
