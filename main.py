"""
Main — Bot standalone: Supertrend + Unicorn Model + filtros de confluencia
===============================================================================
Cascada completa de filtros (cada uno solo se evalúa si el anterior confirma,
para minimizar llamadas a la API sobre 500+ símbolos):

  1. Regime Filter (Choppiness Index)      → bloquea mercados en rango
  2. Supertrend (1H)                        → bias macro direccional
  3. Unicorn Model (3m)                     → timing: sweep + breaker + FVG
  4. Order Flow / Absorción                 → confirma el sweep con trades reales
  5. Funding Rate + Open Interest           → confirma "combustible" del movimiento
  6. Correlation Manager                    → evita exposición oculta a BTC
  7. Setup Memory                           → aprende de setups históricos propios

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
from scanner import get_symbol_universe, scan_universe

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("main")

BTC_SYMBOL = "BTC-USDT"


async def execute_signal(client, journal, risk_mgr, setup_mem, corr_mgr, sig, balance, btc_candles):
    symbol = sig["symbol"]
    side = sig["signal"]
    entry = sig["entry_price"]
    sl = sig["sl_price"]
    tp = sig["tp_price"]
    setup_key = sig.get("setup_key", "unknown")

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

    risk_pct = config.RISK_PCT_PER_TRADE
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

    qty = risk_mgr.calc_position_size(balance, entry, sl)
    if qty <= 0:
        log.warning("[%s] Tamaño de posición inválido, se omite", symbol)
        return None

    await client.set_leverage(symbol, config.LEVERAGE, side)
    result = await client.open_position(symbol, side, qty, sl_price=sl, tp_price=tp)

    journal.record({
        "symbol": symbol, "event": "position_opened", "side": side,
        "entry": entry, "sl": sl, "tp": tp, "qty": qty, "setup_key": setup_key,
        "htf_source": sig.get("htf_source"), "has_fvg": sig.get("has_fvg"),
        "supertrend": sig.get("supertrend"), "order_flow": sig.get("order_flow"),
        "funding_oi": sig.get("funding_oi"), "regime": sig.get("regime"),
        "correlation_btc": corr, "result": result,
    })

    if result.get("code") == 0:
        risk_mgr.register_open_risk(risk_pct)
        corr_mgr.register_open(symbol, side, corr)
        log.info("[%s] Posición %s abierta: qty=%.6f entry=%.6f SL=%.6f TP=%.6f",
                  symbol, side, qty, entry, sl, tp)
        return {"symbol": symbol, "setup_key": setup_key, "risk_pct": risk_pct,
                "opened_at_ms": int(time.time() * 1000)}

    log.error("[%s] Falló apertura de posición: %s", symbol, result)
    return None


async def run_cycle(client, journal, risk_mgr, setup_mem, corr_mgr, pos_monitor, symbols):
    balance = await client.get_balance_usdt()
    if balance <= 0:
        log.warning("Balance no disponible o cero, se omite ciclo")
        return

    await pos_monitor.check_closures(balance)

    if risk_mgr.daily_loss_breached(balance):
        log.warning("Circuit breaker diario activo — no se buscan nuevas señales")
        return

    btc_candles = None
    if getattr(config, "ENABLE_CORRELATION_FILTER", True):
        try:
            btc_candles = await client.get_klines(BTC_SYMBOL, config.BIAS_TF, limit=60)
        except Exception as e:
            log.warning("No se pudo obtener velas de BTC para correlación: %s", e)

    signals = await scan_universe(client, symbols, config)
    for sig in signals:
        opened = await execute_signal(client, journal, risk_mgr, setup_mem, corr_mgr, sig, balance, btc_candles)
        if opened:
            pos_monitor.register_open(opened["symbol"], opened["setup_key"],
                                      opened["risk_pct"], opened["opened_at_ms"])


async def main():
    if not config.BINGX_API_KEY or not config.BINGX_API_SECRET:
        log.warning("BINGX_API_KEY / BINGX_API_SECRET no configuradas — solo funcionará en DRY_RUN")

    journal = TradeJournal(config.JOURNAL_FILE)
    risk_mgr = RiskManager(config)
    setup_mem = SetupMemory(config.SETUP_MEMORY_FILE)
    corr_mgr = CorrelationManager(config)

    async with BingXClient(
        config.BINGX_API_KEY, config.BINGX_API_SECRET,
        config.BINGX_BASE_URL, dry_run=config.DRY_RUN,
    ) as client:

        pos_monitor = PositionMonitor(client, journal, risk_mgr, setup_mem, corr_mgr)

        log.info(
            "Bot iniciado | DRY_RUN=%s | ENTRY_TF=%s | BIAS_TF=%s | "
            "regime=%s corr=%s order_flow=%s funding_oi=%s setup_memory=%s",
            config.DRY_RUN, config.ENTRY_TF, config.BIAS_TF,
            config.ENABLE_REGIME_FILTER, config.ENABLE_CORRELATION_FILTER,
            config.ENABLE_ORDER_FLOW_FILTER, config.ENABLE_FUNDING_OI_FILTER,
            config.ENABLE_SETUP_MEMORY_FILTER,
        )

        symbols = await get_symbol_universe(client, config)
        last_universe_refresh = asyncio.get_event_loop().time()

        while True:
            try:
                now = asyncio.get_event_loop().time()
                if now - last_universe_refresh > 1800:
                    symbols = await get_symbol_universe(client, config)
                    last_universe_refresh = now

                await run_cycle(client, journal, risk_mgr, setup_mem, corr_mgr, pos_monitor, symbols)
            except Exception as e:
                log.exception("Error en ciclo principal: %s", e)

            await asyncio.sleep(config.SCAN_INTERVAL_SEC)


if __name__ == "__main__":
    asyncio.run(main())
