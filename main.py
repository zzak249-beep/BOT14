"""
Main — Bot standalone: Supertrend + Unicorn Model + Order Block Engine + filtros
===============================================================================
Corren DOS loops concurrentes sobre el mismo BingXClient (comparten el
pacing/cooldown de rate limit — no suman presión extra "gratis" entre sí):

  - loop LENTO: barrido completo (SCAN_ALL_SYMBOLS), cada SCAN_INTERVAL_SEC
  - loop RÁPIDO: top FAST_SCAN_TOP_N por volumen, cada FAST_SCAN_INTERVAL_SEC

Un asyncio.Lock (`exec_lock`) serializa la sección de apertura de posición
entre ambos loops — sin esto, si los dos encuentran señal casi al mismo
tiempo, podrían leer el mismo conteo de posiciones abiertas ANTES de que
cualquiera registre la suya, y terminar abriendo más posiciones que
MAX_ACTIVE_POSITIONS o más riesgo que MAX_CONCURRENT_RISK_PCT.

No comparte código ni estado con otros bots (renewed-love / joyful-art).
"""
import asyncio
import logging
import sys
import time

import config
from exchange_client import BingXClient
from journal import TradeJournal
from risk_manager import RiskManager
from setup_memory import SetupMemory
from correlation_manager import CorrelationManager
from position_monitor import PositionMonitor
from scanner import get_symbol_universe, get_top_n_symbols, scan_universe
from order_book_imbalance import confirms_direction as obi_confirms
from state_store import StateStore

# Fingerprint de versión — subilo cada vez que cambies algo importante.
# Sirve para confirmar en el log de arranque que un redeploy realmente
# trajo el código nuevo, en vez de asumirlo por el ID de deploy de Railway.
CODE_VERSION = "2026-07-09-rr2-min-notional"

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("main")

BTC_SYMBOL = "BTC-USDT"


def _save_state(state, recently_opened, recently_closed, pos_monitor, risk_mgr, corr_mgr):
    state.save(recently_opened, recently_closed, pos_monitor.tracked,
               risk_mgr.snapshot(), corr_mgr.open_exposure)


async def _reconcile_positions(client, pos_monitor, risk_mgr, corr_mgr):
    """Al arrancar: adopta posiciones REALES abiertas en BingX que no estén
    en pos_monitor.tracked (abiertas antes del primer deploy con state_store,
    o manualmente). Sin esto quedan huérfanas: su cierre no alimenta el PnL
    diario ni libera riesgo, y no cuentan para el límite de correlación.
    Se asume su SL/TP ya colocado (no se dispara auto-repair a ciegas) y se
    les imputa RISK_PCT_PER_TRADE como aproximación de riesgo abierto."""
    try:
        positions = await client.get_open_positions()
    except Exception as e:
        log.warning("No se pudieron leer posiciones para reconciliar al arranque: %s", e)
        return
    adopted = 0
    for p in positions:
        symbol = p.get("symbol")
        if not symbol or symbol in pos_monitor.tracked:
            continue
        side = p.get("positionSide") or (
            "LONG" if float(p.get("positionAmt", 0)) > 0 else "SHORT")
        pos_monitor.register_open(
            symbol, "adopted|preexistente", config.RISK_PCT_PER_TRADE,
            int(time.time() * 1000), side, sl_placed=True, tp_placed=True)
        risk_mgr.register_open_risk(config.RISK_PCT_PER_TRADE)
        corr_mgr.register_open(symbol, side, None)
        adopted += 1
        log.info("[%s] Posición preexistente ADOPTADA (%s) — su cierre ahora sí alimenta riesgo/journal", symbol, side)
    if adopted:
        log.info("Reconciliación: %d posiciones adoptadas al arranque", adopted)


async def execute_signal(client, journal, risk_mgr, setup_mem, corr_mgr, sig, balance, btc_candles,
                          exec_lock, recently_opened, recently_closed):
    symbol = sig["symbol"]
    side = sig["signal"]
    entry = sig["entry_price"]
    sl = sig["sl_price"]
    tp = sig["tp_price"]
    setup_key = sig.get("setup_key", "unknown")

    # Todo lo de acá abajo (chequeo de riesgo -> apertura -> registro) queda
    # serializado entre el loop rápido y el lento — ver docstring del módulo.
    async with exec_lock:
        now_ms = int(time.time() * 1000)
        cooldown_ms = getattr(config, "DEDUP_COOLDOWN_SEC", 300) * 1000
        last_opened = recently_opened.get(symbol)
        if last_opened is not None and (now_ms - last_opened) < cooldown_ms:
            log.info("[%s] Señal descartada por dedup (ya se abrió hace %.0fs, cooldown=%ss)",
                      symbol, (now_ms - last_opened) / 1000, cooldown_ms // 1000)
            journal.record({"symbol": symbol, "event": "signal_rejected",
                             "reason": "dedup_cooldown", "signal": sig})
            return None

        # Cooldown post-cierre: no reabrir el mismo símbolo enseguida después
        # de cerrar una posición en él (observado en real con UNI-USDT:
        # cierre 16:26, reapertura 16:27 — 3 trades el mismo día). El dedup
        # de arriba no cubre esto porque mira la APERTURA anterior, no el cierre.
        post_close_ms = getattr(config, "POST_CLOSE_COOLDOWN_SEC", 900) * 1000
        last_closed = recently_closed.get(symbol)
        if last_closed is not None and (now_ms - last_closed) < post_close_ms:
            log.info("[%s] Señal descartada por cooldown post-cierre (cerró hace %.0fs, cooldown=%ss)",
                      symbol, (now_ms - last_closed) / 1000, post_close_ms // 1000)
            journal.record({"symbol": symbol, "event": "signal_rejected",
                             "reason": "post_close_cooldown", "signal": sig})
            return None

        open_positions = await client.get_open_positions()
        if any(p["symbol"] == symbol for p in open_positions):
            log.info("[%s] Ya hay posición abierta, se omite señal", symbol)
            return None

        if getattr(config, "ENABLE_SETUP_MEMORY_FILTER", True):
            allow, reason = setup_mem.should_allow(setup_key, config)
            if not allow:
                log.info("[%s] Señal descartada por setup memory: %s", symbol, reason)
                journal.record({"symbol": symbol, "event": "signal_rejected",
                                 "reason": f"setup_memory: {reason}", "signal": sig})
                return None

        qty = risk_mgr.calc_position_size(balance, entry, sl)
        if qty <= 0:
            log.warning("[%s] Tamaño de posición inválido, se omite", symbol)
            return None

        # ── Notional mínimo: inflar trades demasiado chicos, con tope ──
        # Los notionals chicos vienen de SLs lejanos (caso LDO: 4 USDT de
        # notional = SL a ~11%) — inflarlos multiplica el riesgo real de ESE
        # trade, por eso el tope MIN_NOTIONAL_MAX_RISK_PCT.
        min_notional = getattr(config, "MIN_NOTIONAL_USDT", 0.0)
        bumped = False
        if min_notional > 0 and qty * entry < min_notional:
            qty = min_notional / entry
            bumped = True

        specs = await client.get_contract_specs(symbol)
        if specs:
            qty = client.round_qty(qty, specs["quantityPrecision"])
            if bumped and 0 < qty * entry < min_notional:
                # el redondeo truncó por debajo del mínimo: un paso de
                # precisión hacia arriba para no quedar corto
                qty = client.round_qty(qty + 10 ** -specs["quantityPrecision"],
                                        specs["quantityPrecision"])
            sl = client.round_price(sl, specs["pricePrecision"])
            tp = client.round_price(tp, specs["pricePrecision"])
            if qty <= 0:
                log.warning("[%s] Tamaño quedó en 0 tras redondear a la precisión del símbolo (%s decimales), se omite",
                             symbol, specs["quantityPrecision"])
                journal.record({"symbol": symbol, "event": "signal_rejected",
                                 "reason": "qty_cero_tras_redondeo", "signal": sig})
                return None
        else:
            log.warning("[%s] Sin especificaciones de contrato disponibles — se ordena sin redondear (riesgo de 110424 en SL/TP)", symbol)

        # Riesgo EFECTIVO con la cantidad final (refleja el bump y el
        # redondeo) — es lo que se chequea, se reserva y luego se libera.
        risk_pct = qty * abs(entry - sl) / balance * 100
        if bumped:
            max_risk = getattr(config, "MIN_NOTIONAL_MAX_RISK_PCT", 1.5)
            if risk_pct > max_risk:
                log.info("[%s] Señal descartada: inflar a %.0f USDT de notional dejaría el riesgo en %.2f%% (> tope %.2f%%)",
                          symbol, min_notional, risk_pct, max_risk)
                journal.record({"symbol": symbol, "event": "signal_rejected",
                                 "reason": f"min_notional_riesgo_excesivo: {risk_pct:.2f}%", "signal": sig})
                return None
            log.info("[%s] Notional inflado al mínimo: qty=%.6f (~%.2f USDT), riesgo efectivo %.2f%%",
                      symbol, qty, qty * entry, risk_pct)

        can_open, reason = risk_mgr.can_open_new_position(balance, len(open_positions), risk_pct)
        if not can_open:
            log.warning("[%s] Señal descartada por risk manager: %s", symbol, reason)
            journal.record({"symbol": symbol, "event": "signal_rejected", "reason": reason, "signal": sig})
            return None

        corr = None
        if getattr(config, "ENABLE_CORRELATION_FILTER", True) and btc_candles:
            try:
                candles_symbol_bias = await client.get_klines(symbol, config.BIAS_TF, limit=60)
                can_open_corr, corr, corr_reason = corr_mgr.evaluate(symbol, side, candles_symbol_bias, btc_candles)
                if not can_open_corr:
                    log.info("[%s] Señal descartada por correlación: %s", symbol, corr_reason)
                    journal.record({"symbol": symbol, "event": "signal_rejected",
                                     "reason": f"correlation: {corr_reason}", "signal": sig})
                    return None
            except Exception as e:
                log.warning("[%s] No se pudo evaluar correlación, se permite por defecto: %s", symbol, e)

        obi_info = {"skipped": True}
        if getattr(config, "ENABLE_OBI_FILTER", False):
            # Se pide el order book ACÁ, lo más tarde posible (justo antes de
            # ejecutar) — es la confirmación más fresca que existe en el bot,
            # tiene sentido pedirla al final, no durante el scan.
            try:
                order_book = await client.get_order_book(symbol, getattr(config, "OBI_LEVELS", 20))
                obi_info = obi_confirms(order_book, side, config)
                if obi_info["confirms"] is False:
                    log.info("[%s] Señal descartada por OBI: %s", symbol, obi_info["reason"])
                    journal.record({"symbol": symbol, "event": "signal_rejected",
                                     "reason": f"obi: {obi_info['reason']}", "signal": sig})
                    return None
            except Exception as e:
                log.warning("[%s] No se pudo evaluar OBI, se permite por defecto: %s", symbol, e)
                obi_info = {"confirms": None, "reason": f"error: {e}"}

        await client.set_leverage(symbol, config.LEVERAGE, side)
        result = await client.open_position(symbol, side, qty, sl_price=sl, tp_price=tp)

        journal.record({
            "symbol": symbol, "event": "position_opened", "side": side,
            "entry": entry, "sl": sl, "tp": tp, "qty": qty, "setup_key": setup_key,
            "engine": sig.get("engine"),
            "htf_source": sig.get("htf_source"), "has_fvg": sig.get("has_fvg"),
            "supertrend": sig.get("supertrend"), "order_flow": sig.get("order_flow"),
            "funding_oi": sig.get("funding_oi"), "regime": sig.get("regime"),
            "order_block": sig.get("order_block"), "cvd": sig.get("cvd"), "obi": obi_info,
            "correlation_btc": corr, "result": result,
            "sl_placed": result.get("sl_placed"), "tp_placed": result.get("tp_placed"),
        })

        if result.get("code") == 0:
            risk_mgr.register_open_risk(risk_pct)
            corr_mgr.register_open(symbol, side, corr)
            recently_opened[symbol] = now_ms
            log.info("[%s] Posición %s abierta: qty=%.6f entry=%.6f SL=%.6f TP=%.6f",
                      symbol, side, qty, entry, sl, tp)
            if result.get("sl_placed") is False:
                log.error(
                    "🚨 [%s] La posición abrió pero el SL NO quedó puesto en BingX — "
                    "revisar y proteger manualmente ya mismo.", symbol,
                )
            return {"symbol": symbol, "setup_key": setup_key, "risk_pct": risk_pct,
                    "opened_at_ms": int(time.time() * 1000), "side": side,
                    "sl_price": sl, "tp_price": tp,
                    "sl_placed": result.get("sl_placed") is not False,
                    "tp_placed": result.get("tp_placed") is not False}

        log.error("[%s] Falló apertura de posición: %s", symbol, result)
        return None


async def run_cycle(client, journal, risk_mgr, setup_mem, corr_mgr, pos_monitor, symbols, exec_lock,
                     recently_opened, recently_closed, state, tag="slow"):
    balance = await client.get_balance_usdt()
    if balance <= 0:
        log.warning("[%s] Balance no disponible o cero, se omite ciclo", tag)
        return

    async with exec_lock:
        tracked_before = len(pos_monitor.tracked)
        await pos_monitor.check_closures(balance)
        if len(pos_monitor.tracked) != tracked_before:
            _save_state(state, recently_opened, recently_closed, pos_monitor, risk_mgr, corr_mgr)

    if risk_mgr.daily_loss_breached(balance):
        log.warning("[%s] Circuit breaker diario activo — no se buscan nuevas señales", tag)
        return

    btc_candles = None
    if getattr(config, "ENABLE_CORRELATION_FILTER", True):
        try:
            btc_candles = await client.get_klines(BTC_SYMBOL, config.BIAS_TF, limit=60)
        except Exception as e:
            log.warning("[%s] No se pudo obtener velas de BTC para correlación: %s", tag, e)

    signals = await scan_universe(client, symbols, config)
    for sig in signals:
        opened = await execute_signal(client, journal, risk_mgr, setup_mem, corr_mgr,
                                       sig, balance, btc_candles, exec_lock, recently_opened, recently_closed)
        if opened:
            async with exec_lock:
                pos_monitor.register_open(opened["symbol"], opened["setup_key"],
                                          opened["risk_pct"], opened["opened_at_ms"], opened["side"],
                                          sl_price=opened.get("sl_price"), tp_price=opened.get("tp_price"),
                                          sl_placed=opened.get("sl_placed", True),
                                          tp_placed=opened.get("tp_placed", True))
                _save_state(state, recently_opened, recently_closed, pos_monitor, risk_mgr, corr_mgr)


async def _slow_loop(client, journal, risk_mgr, setup_mem, corr_mgr, pos_monitor, exec_lock, recently_opened, recently_closed, state):
    symbols = await get_symbol_universe(client, config)
    last_refresh = asyncio.get_event_loop().time()

    while True:
        try:
            now = asyncio.get_event_loop().time()
            if now - last_refresh > 1800:
                symbols = await get_symbol_universe(client, config)
                last_refresh = now

            await run_cycle(client, journal, risk_mgr, setup_mem, corr_mgr, pos_monitor,
                             symbols, exec_lock, recently_opened, recently_closed, state, tag="slow")
        except Exception as e:
            log.exception("[slow] Error en ciclo: %s", e)

        await asyncio.sleep(config.SCAN_INTERVAL_SEC)


async def _fast_loop(client, journal, risk_mgr, setup_mem, corr_mgr, pos_monitor, exec_lock, recently_opened, recently_closed, state):
    top_n = getattr(config, "FAST_SCAN_TOP_N", 60)
    interval = getattr(config, "FAST_SCAN_INTERVAL_SEC", 60)

    while True:
        try:
            symbols = await get_top_n_symbols(client, config, top_n)
            await run_cycle(client, journal, risk_mgr, setup_mem, corr_mgr, pos_monitor,
                             symbols, exec_lock, recently_opened, recently_closed, state, tag="fast")
        except Exception as e:
            log.exception("[fast] Error en ciclo: %s", e)

        await asyncio.sleep(interval)


async def main():
    if not config.BINGX_API_KEY or not config.BINGX_API_SECRET:
        log.warning("BINGX_API_KEY / BINGX_API_SECRET no configuradas — solo funcionará en DRY_RUN")

    journal = TradeJournal(config.JOURNAL_FILE)
    risk_mgr = RiskManager(config)
    setup_mem = SetupMemory(config.SETUP_MEMORY_FILE)
    corr_mgr = CorrelationManager(config)
    exec_lock = asyncio.Lock()
    recently_opened = {}  # symbol -> timestamp_ms, dedup entre loop rápido y lento
    recently_closed = {}  # symbol -> timestamp_ms del último cierre, cooldown post-cierre

    async with BingXClient(
        config.BINGX_API_KEY, config.BINGX_API_SECRET,
        config.BINGX_BASE_URL, dry_run=config.DRY_RUN,
    ) as client:

        pos_monitor = PositionMonitor(client, journal, risk_mgr, setup_mem, corr_mgr, recently_closed)
        fast_on = getattr(config, "ENABLE_FAST_SCAN", True)

        # ── Restauración de estado (sobrevive redeploys de Railway) ──────
        # Antes TODO esto era RAM: cada redeploy borraba cooldowns, riesgo
        # abierto, exposición correlacionada, circuit breaker diario y el
        # tracking de posiciones (confirmado en el deploy 2026-07-09 08:47).
        state = StateStore(config.STATE_FILE)
        snap = state.load()
        recently_opened.update(snap.get("recently_opened", {}))
        recently_closed.update(snap.get("recently_closed", {}))
        pos_monitor.tracked.update(snap.get("tracked", {}))
        risk_mgr.restore(snap.get("risk"))
        corr_mgr.open_exposure.update(snap.get("corr_exposure", {}))
        if snap:
            log.info(
                "Estado restaurado de %s | tracked=%d recently_opened=%d recently_closed=%d corr_exposure=%d",
                config.STATE_FILE, len(pos_monitor.tracked), len(recently_opened),
                len(recently_closed), len(corr_mgr.open_exposure),
            )
        else:
            log.info("Sin estado previo en %s — arranque en frío", config.STATE_FILE)

        # Posiciones reales abiertas que no estén en tracked (abiertas antes
        # de este fix, o a mano) se adoptan para que cuenten en riesgo y
        # correlación y su cierre alimente el sistema. En DRY_RUN no: las
        # posiciones reales de la cuenta no son de este bot simulado.
        if not config.DRY_RUN:
            await _reconcile_positions(client, pos_monitor, risk_mgr, corr_mgr)
            _save_state(state, recently_opened, recently_closed, pos_monitor, risk_mgr, corr_mgr)

        # Longitud exacta de las credenciales EN ESTE PROCESO, no lo que se
        # piense haber pegado en Railway. Comparar este número contra lo que
        # dio el diagnóstico local (85/82 la última vez que funcionó) — si no
        # coincide, confirma sin dudas que Railway tiene algo distinto.
        log.info(
            "Credenciales cargadas | BINGX_API_KEY: %d caracteres | BINGX_API_SECRET: %d caracteres",
            len(config.BINGX_API_KEY), len(config.BINGX_API_SECRET),
        )

        log.info(
            "Bot iniciado | CODE_VERSION=%s | DRY_RUN=%s | BINGX_BASE_URL=%s (demo_mode=%s) | ENTRY_TF=%s | BIAS_TF=%s | OB_TF=%s | "
            "regime=%s corr=%s order_flow=%s funding_oi=%s setup_memory=%s "
            "ob_engine=%s scan_all_symbols=%s | fast_scan=%s top_n=%s interval=%ss",
            CODE_VERSION, config.DRY_RUN, config.BINGX_BASE_URL, getattr(config, "BINGX_DEMO_MODE", False),
            config.ENTRY_TF, config.BIAS_TF, getattr(config, "OB_TF", "15m"),
            config.ENABLE_REGIME_FILTER, config.ENABLE_CORRELATION_FILTER,
            config.ENABLE_ORDER_FLOW_FILTER, config.ENABLE_FUNDING_OI_FILTER,
            config.ENABLE_SETUP_MEMORY_FILTER, getattr(config, "ENABLE_OB_ENGINE", True),
            getattr(config, "SCAN_ALL_SYMBOLS", True), fast_on,
            getattr(config, "FAST_SCAN_TOP_N", 60), getattr(config, "FAST_SCAN_INTERVAL_SEC", 60),
        )


        loops = [_slow_loop(client, journal, risk_mgr, setup_mem, corr_mgr, pos_monitor, exec_lock, recently_opened, recently_closed, state)]
        if fast_on:
            loops.append(_fast_loop(client, journal, risk_mgr, setup_mem, corr_mgr, pos_monitor, exec_lock, recently_opened, recently_closed, state))

        await asyncio.gather(*loops)


if __name__ == "__main__":
    asyncio.run(main())
