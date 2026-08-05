"""
Punto de entrada. Arranca el healthcheck, valida configuracion,
reconcilia estado contra BingX y lanza el loop de escaneo 24/7.
"""
import asyncio
import logging
import signal
import sys

import config as cfg
import executor
import healthcheck
import scanner
from bingx_client import BingXClient
from state import StateManager
from telegram_notifier import TelegramNotifier

log = logging.getLogger("main")


async def check_connectivity(client: BingXClient) -> None:
    """Prueba de humo al arrancar: confirma que la API responde y que
    las credenciales (si las hay) funcionan, antes de meterse al loop."""
    try:
        contracts = await client.get_contracts()
        log.info("Conectividad OK: %d contratos disponibles en BingX.", len(contracts))
    except Exception as e:
        log.error("No se pudo conectar a BingX (datos publicos): %s", e)
        return

    if cfg.BINGX_API_KEY and cfg.BINGX_API_SECRET:
        try:
            bal = await client.get_balance()
            log.info("Autenticacion OK. Balance USDT-M Perp: %s", str(bal)[:200])
        except Exception as e:
            log.error("Las credenciales BingX no funcionan: %s", e)
            if cfg.MODE == "LIVE":
                log.error("MODE=LIVE sin autenticacion valida. Abortando.")
                sys.exit(1)

        # DIAGNOSTICO: saldo por tipo de cuenta. Si el USDT-M Perp de arriba
        # da 0 pero aqui aparece saldo en "sopt" (spot/fund) u otro
        # accountType, el dinero esta ahi -- hace falta transferirlo, no
        # tocar nada del bot.
        try:
            overview = await client.get_all_account_balance()
            resumen = ", ".join(
                f"{a.get('accountType')}={a.get('usdtBalance')}" for a in overview
            ) if overview else "(vacio)"
            log.info("Saldo por tipo de cuenta: %s", resumen)
        except Exception as e:
            log.warning("No se pudo consultar el resumen de cuentas: %s", e)

        detected = await client.get_position_mode()
        if detected and detected != cfg.POSITION_MODE:
            log.warning(
                "POSITION_MODE=%s en config pero BingX reporta %s. "
                "Ajusta la variable de entorno POSITION_MODE o corrigelo en la app de BingX.",
                cfg.POSITION_MODE, detected,
            )


async def run() -> None:
    cfg.setup_logging()
    cfg.validate()
    healthcheck.start(cfg.HEALTHCHECK_PORT)

    state = StateManager(cfg.STATE_FILE)
    notifier = TelegramNotifier(cfg.TELEGRAM_BOT_TOKEN, cfg.TELEGRAM_CHAT_ID)
    await notifier.start()

    async with BingXClient(cfg.BINGX_API_KEY, cfg.BINGX_API_SECRET, cfg.BINGX_BASE_URL,
                            max_concurrent=cfg.MAX_CONCURRENT_REQUESTS) as client:
        await check_connectivity(client)
        await scanner.get_symbol_universe(client, force=True)
        await executor.reconcile_on_startup(client, state, notifier)

        await notifier.send(
            f"🤖 Bot iniciado ({cfg.CODE_VERSION})\nMODE={cfg.MODE}  TF={cfg.TIMEFRAME}  "
            f"Simbolos={len(await scanner.get_symbol_universe(client))}"
        )

        stop_event = asyncio.Event()

        def _handle_stop(*_):
            log.info("Señal de apagado recibida, terminando limpio...")
            stop_event.set()

        for sig_name in ("SIGTERM", "SIGINT"):
            try:
                asyncio.get_running_loop().add_signal_handler(getattr(signal, sig_name), _handle_stop)
            except (NotImplementedError, AttributeError):
                pass  # Windows / entornos sin add_signal_handler

        loop_task = asyncio.create_task(
            scanner.main_loop(client, state, notifier, on_cycle=healthcheck.mark_cycle_ok)
        )
        stop_task = asyncio.create_task(stop_event.wait())
        done, pending = await asyncio.wait({loop_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)

        for t in pending:
            t.cancel()
        state.save()
        await notifier.send("🛑 Bot detenido.")
        await notifier.stop()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
    except Exception:
        logging.getLogger("main").exception("Fallo fatal no controlado.")
        sys.exit(1)
