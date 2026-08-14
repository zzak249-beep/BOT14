"""
Orquesta el escaneo de todo el universo de simbolos BingX USDT-M en
cada ciclo: trae klines en paralelo (limitado por semaforo dentro de
BingXClient), evalua la estrategia por simbolo y despacha las señales.
"""
import asyncio
import logging
import time
from typing import Optional

import config as cfg
import executor
from bingx_client import BingXClient, BingXError, normalize_symbol
from state import StateManager
from strategy import evaluate_symbol, reset_cycle_stats, get_cycle_stats
from telegram_notifier import TelegramNotifier, format_backup

log = logging.getLogger("scanner")

_symbol_cache: list = []
_symbol_cache_at: float = 0.0


def _is_tradable(contract: dict) -> bool:
    for key in ("status", "apiStateOpen", "state"):
        if key in contract:
            v = contract[key]
            if isinstance(v, bool):
                return v
            sv = str(v).upper()
            if sv in ("1", "TRADING", "ONLINE", "TRUE"):
                return True
            if sv in ("0", "OFFLINE", "FALSE", "DELISTED", "PAUSED"):
                return False
    return True  # campo desconocido -> no filtramos por si acaso


async def get_symbol_universe(client: BingXClient, force: bool = False) -> list:
    global _symbol_cache, _symbol_cache_at
    now = time.time()
    if not force and _symbol_cache and (now - _symbol_cache_at) < cfg.SYMBOL_REFRESH_MIN * 60:
        return _symbol_cache

    try:
        contracts = await client.get_contracts()
    except BingXError as e:
        log.error("No se pudo obtener la lista de contratos: %s", e)
        return _symbol_cache  # lo que hubiera en cache, aunque este viejo

    total = len(contracts)
    out = []
    for c in contracts:
        sym = c.get("symbol")
        if not sym:
            continue
        norm = normalize_symbol(sym, cfg.QUOTE_ASSET)
        if not norm.endswith("-" + cfg.QUOTE_ASSET):
            continue
        if not _is_tradable(c):
            continue
        if cfg.SYMBOL_WHITELIST and norm not in cfg.SYMBOL_WHITELIST:
            continue
        if norm in cfg.SYMBOL_BLACKLIST:
            continue
        if cfg.EXCLUDE_MAJORS and norm in cfg.MAJOR_SYMBOLS:
            continue
        if cfg.MIN_24H_VOLUME_USDT > 0:
            vol = float(c.get("quoteVolume24h", c.get("volume24h", 0)) or 0)
            if vol < cfg.MIN_24H_VOLUME_USDT:
                continue
        out.append(sym)  # se usa el symbol tal cual lo devuelve BingX para las llamadas a la API

    log.info("Universo de simbolos: %d contratos -> %d tras filtros (blacklist=%d whitelist=%d).",
              total, len(out), len(cfg.SYMBOL_BLACKLIST), len(cfg.SYMBOL_WHITELIST))
    if total > 0 and len(out) < total * 0.1:
        log.warning("Se filtro mas del 90%% del universo. Revisa MIN_24H_VOLUME_USDT / el campo de estado del contrato.")

    _symbol_cache = out
    _symbol_cache_at = now
    return out


async def _fetch_and_evaluate(client: BingXClient, state: StateManager, symbol: str):
    try:
        ltf = await client.get_klines(symbol, cfg.TIMEFRAME, cfg.KLINES_LOOKBACK)
        if len(ltf) < 60:
            return None
        htf = await client.get_klines(symbol, cfg.HTF_TIMEFRAME, cfg.HTF_EMA_LEN + 20) if cfg.USE_HTF_BIAS else []
        daily = await client.get_klines(symbol, "1d", 5)
        funding_rate = await client.get_funding_rate(symbol) if cfg.USE_FUNDING_FILTER else None
        current_oi = await client.get_open_interest(symbol) if cfg.USE_OI_FILTER else None
    except BingXError as e:
        log.debug("%s: fallo al traer klines (%s)", symbol, e)
        return None
    except Exception as e:  # defensivo: un simbolo raro no debe tumbar el ciclo entero
        log.warning("%s: error inesperado trayendo datos: %s", symbol, e)
        return None

    sym_state = state.get_symbol_state(symbol)
    try:
        new_state, signal = evaluate_symbol(symbol, ltf, htf, daily, sym_state, funding_rate, current_oi)
    except Exception as e:
        log.warning("%s: error inesperado evaluando la estrategia: %s", symbol, e)
        return None
    state.symbol_states[symbol] = new_state
    return signal


async def _send_daily_backup(state: StateManager, notifier: TelegramNotifier, total_w: int, total_l: int, win_rate: float) -> None:
    """Envia el respaldo diario y marca como enviado SOLO si de verdad se
    entrego (send_direct devuelve la confirmacion real, no solo si se
    encolo). Si falla, no se marca -- se reintenta en el proximo ciclo.
    Si Telegram esta deshabilitado, no hay nada que reintentar: se marca
    igual para no repetir el intento (y el log) cada ciclo sin sentido."""
    if not notifier.enabled:
        state.mark_backup_sent()
        state.save()
        return
    snapshot = state.backup_snapshot_json(include_positions=False)
    msg = format_backup(snapshot, total_w, total_l, win_rate)
    delivered = await notifier.send_direct(msg)
    if delivered:
        state.mark_backup_sent()
        state.save()
        log.info("Respaldo diario entregado por Telegram (%d caracteres).", len(snapshot))
    else:
        log.error("Respaldo diario NO se pudo entregar, se reintenta el proximo ciclo.")


async def run_scan_cycle(client: BingXClient, state: StateManager, notifier: TelegramNotifier) -> int:
    t0 = time.time()
    reset_cycle_stats()
    symbols = await get_symbol_universe(client)
    if not symbols:
        log.warning("Universo de simbolos vacio, se omite el ciclo.")
        return 0

    tasks = [_fetch_and_evaluate(client, state, s) for s in symbols]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    signals = [r for r in results if r is not None]

    for sig in signals:
        try:
            await executor.handle_signal(sig, client, state, notifier)
        except Exception as e:
            log.error("%s: error despachando señal: %s", sig.symbol, e)

    try:
        await executor.manage_open_positions(client, state, notifier)
        await executor.manage_paper_positions(client, state, notifier)
    except Exception as e:
        log.error("Error gestionando posiciones abiertas: %s", e)

    state.save()

    elapsed = time.time() - t0
    log.info(
        "Ciclo completo: %d simbolos, %d señales, %d posiciones abiertas, %.1fs",
        len(symbols), len(signals), state.open_position_count(), elapsed,
    )
    if cfg.MODE == "SIGNAL":
        total_w = sum(v.get("w", 0) for v in state.kz_stats.values())
        total_l = sum(v.get("l", 0) for v in state.kz_stats.values())
        wr = (total_w * 100.0 / (total_w + total_l)) if (total_w + total_l) else 0.0
        log.info("Papel: %d abiertas | %dW/%dL (%.0f%% de %d cerradas)",
                  state.open_position_count(), total_w, total_l, wr, total_w + total_l)
        rev = state.path_stats.get("REV", {"w": 0, "l": 0})
        cont = state.path_stats.get("CONT", {"w": 0, "l": 0})
        path_total = rev["w"] + rev["l"] + cont["w"] + cont["l"]
        unclassified = (total_w + total_l) - path_total
        log.info("Por ruta: REV %dW/%dL | CONT %dW/%dL%s", rev["w"], rev["l"], cont["w"], cont["l"],
                  f" | sin clasificar: {unclassified} (posiciones abiertas antes de que se guardara 'path')" if unclassified > 0 else "")
        major = state.tier_stats.get("major", {"w": 0, "l": 0})
        altcoin = state.tier_stats.get("altcoin", {"w": 0, "l": 0})
        maj_t, alt_t = major["w"] + major["l"], altcoin["w"] + altcoin["l"]
        log.info("Por tier: major %dW/%dL%s | altcoin %dW/%dL%s",
                  major["w"], major["l"], f" ({major['w']*100.0/maj_t:.0f}%)" if maj_t else "",
                  altcoin["w"], altcoin["l"], f" ({altcoin['w']*100.0/alt_t:.0f}%)" if alt_t else "")
        n_days = len(state.active_days)
        avg_per_day = (total_w + total_l) / n_days if n_days else 0.0
        log.info("Muestra: %d cerradas en %d dias distintos, ~%.0f/dia", total_w + total_l, n_days, avg_per_day)

        if state.needs_daily_backup():
            await _send_daily_backup(state, notifier, total_w, total_l, wr)
    st = get_cycle_stats()
    log.info(
        "Embudo: sweeps=%d fvgs=%d confirmaciones=%d | rechazadas por RR=%d direccion=%d "
        "kz_only=%d htf=%d premium/discount=%d funding=%d oi=%d | señales=%d",
        st["sweeps"], st["fvgs_formed"], st["confirmations"],
        st["rejected_rr"], st["rejected_direction"], st["rejected_kz_only"],
        st["rejected_htf"], st["rejected_premium_discount"],
        st["rejected_funding"], st["rejected_oi"], st["signals"],
    )
    if elapsed > cfg.SCAN_INTERVAL_SEC:
        log.warning(
            "El ciclo tardo %.1fs, mas que SCAN_INTERVAL_SEC=%ds. Sube MAX_CONCURRENT_REQUESTS "
            "o SCAN_INTERVAL_SEC, o reduce el universo con SYMBOL_WHITELIST/MIN_24H_VOLUME_USDT.",
            elapsed, cfg.SCAN_INTERVAL_SEC,
        )
    return len(signals)


async def main_loop(client: BingXClient, state: StateManager, notifier: TelegramNotifier, on_cycle=None) -> None:
    log.info("Escaneo iniciado. Intervalo=%ds  Simbolos=refrescados cada %dmin", cfg.SCAN_INTERVAL_SEC, cfg.SYMBOL_REFRESH_MIN)
    while True:
        cycle_start = time.time()
        try:
            await run_scan_cycle(client, state, notifier)
            if on_cycle:
                on_cycle()
        except Exception as e:
            log.error("Ciclo de escaneo fallo por completo (se reintenta en el siguiente): %s", e, exc_info=True)
        elapsed = time.time() - cycle_start
        await asyncio.sleep(max(1.0, cfg.SCAN_INTERVAL_SEC - elapsed))
