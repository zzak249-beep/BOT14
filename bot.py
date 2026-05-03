# -*- coding: utf-8 -*-
"""bot.py -- Phantom Edge Bot v6.2 — Fixed: warmup phase + AUTO_TRADING support."""
from __future__ import annotations
import asyncio, signal, sys, time
from collections import Counter

from loguru import logger
from aiohttp import web

from config import cfg
import client as ex
from scanner import fetch_universe, get_symbols, warmup_all
from strategy import get_signal, _in_dead_session
from pos_manager import (
    Trade, add_trade, open_symbols, trade_count, is_halted,
    manage_positions, sync_from_exchange, get_stats, consecutive_losses,
)
import notifier

logger.remove()
logger.add(sys.stdout, level="INFO",
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <7}</level> | {message}")

_active_symbols: list[str] = []
_cycle: int = 0
_times: list[float] = []
_warmed_up: bool = False
SYMBOL_REFRESH_CYCLES = 120


async def _health(_: web.Request) -> web.Response:
    stats = get_stats()
    avg = sum(_times[-5:])/len(_times[-5:]) if _times else 0
    return web.json_response({
        "status":      "halted" if stats["halted"] else "ok",
        "version":     "phantom-v6.2",
        "strategy":    "ZigZag+HMA+FutureTrend",
        "warmed_up":   _warmed_up,
        "open_trades": stats["open"],
        "daily_pnl":   stats["daily_pnl"],
        "daily_wins":  stats["daily_wins"],
        "daily_losses":stats["daily_losses"],
        "symbols":     len(_active_symbols),
        "open_syms":   list(open_symbols()),
        "avg_cycle_s": round(avg, 1),
    })

async def start_health_server() -> None:
    app = web.Application()
    app.router.add_get("/", _health)
    app.router.add_get("/health", _health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", cfg.health_port)
    await site.start()
    logger.info(f"Health :{cfg.health_port}")


async def enter_trade(sig) -> None:
    if sig.symbol in open_symbols(): return
    if trade_count() >= cfg.max_positions: return
    if is_halted(): return

    min_s = cfg.min_score + 1 if consecutive_losses() >= 3 else cfg.min_score
    if sig.score < min_s: return

    size   = max(cfg.trade_usdt, 9.)
    lev    = cfg.leverage
    margin = (size / lev) * 1.3

    bal = await ex.get_balance()
    if bal < cfg.min_balance_usdt or bal < margin:
        logger.warning(f"[SKIP] {sig.symbol} bal={bal:.2f} insuficiente")
        return

    if sig.side == "BUY"  and (sig.sl >= sig.price or sig.tp <= sig.price): return
    if sig.side == "SELL" and (sig.sl <= sig.price or sig.tp >= sig.price): return
    if abs(sig.price - sig.sl) / sig.price * 100 < 0.05: return

    await ex.set_leverage(sig.symbol, lev)

    t_ord = time.perf_counter()
    resp = await ex.place_market_order(
        symbol=sig.symbol, side=sig.side,
        size_usdt=size, sl=sig.sl, tp=sig.tp,
    )
    ms = int((time.perf_counter()-t_ord)*1000)

    code = resp.get("code", -1)
    if code not in (0, 200, None):
        logger.warning(f"[FAIL] {sig.symbol} code={code} {resp.get('msg','')}")
        return

    od  = resp.get("data", {})
    if isinstance(od, dict): od = od.get("order", od)
    qty = float(od.get("executedQty",0) or od.get("origQty",0))
    if qty <= 0: qty = (size * lev) / sig.price

    add_trade(Trade(
        symbol=sig.symbol, side=sig.side,
        entry=sig.price, sl=sig.sl, tp=sig.tp,
        atr=sig.atr_5m, size_usdt=size, leverage=lev,
        qty=qty, score=sig.score, vol_ratio=sig.vol_ratio,
        delta1=sig.peak, delta2=sig.valley,
        order_id=str(od.get("orderId","")), bot_opened=True,
    ))

    reasons = " · ".join(sig.reasons)
    logger.success(
        f"[ENTRADA ⚡{ms}ms] {sig.symbol} {sig.side} @ {sig.price:.6f} | "
        f"SL={sig.sl:.6f} TP={sig.tp:.6f} | score={sig.score}/6 | {reasons}"
    )
    await notifier.notify_entry(
        symbol=sig.symbol, side=sig.side, price=sig.price,
        sl=sig.sl, tp=sig.tp, size_usdt=size, leverage=lev,
        qty=qty, score=sig.score,
        delta1=sig.peak, delta2=sig.valley, vol_ratio=sig.vol_ratio,
    )


async def scan_cycle(ohlcv_map: dict) -> None:
    signals:    list = []
    rejections: Counter = Counter()
    open_syms = open_symbols()

    t_sig = time.perf_counter()
    for sym, data in ohlcv_map.items():
        if sym in open_syms: continue
        sig, reason = get_signal(
            ohlcv_5m     = data.get(cfg.timeframe, {}),
            ohlcv_15m    = data.get(cfg.timeframe_slow, None),
            ohlcv_1h     = None,
            symbol       = sym,
            open_syms    = open_syms,
            pivot_len    = cfg.pivot_len,
            atr_period   = cfg.atr_period,
            atr_mult     = cfg.atr_mult,
            rr           = cfg.rr,
            min_vol_mult = cfg.min_vol_mult,
            hma_len      = cfg.hma_len,
            ft_period    = cfg.ft_period,
            min_atr_pct  = cfg.min_atr_pct,
            min_score    = cfg.min_score,
        )
        if sig:
            signals.append(sig)
        else:
            bucket = reason.split("_")[0].split("=")[0].split(" ")[0]
            rejections[bucket] += 1

    sig_ms = int((time.perf_counter()-t_sig)*1000)
    scanned = len(ohlcv_map) - len(open_syms)
    logger.info(
        f"[ANALISIS] {scanned} pares | {len(signals)} senales | "
        f"compute={sig_ms}ms | rechazos: {dict(rejections.most_common(5))}"
    )

    if not signals: return

    signals.sort(key=lambda s: s.score, reverse=True)
    logger.info("[SENALES]")
    for s in signals[:6]:
        logger.info(f"  {s.symbol:16s} {s.side:4s} {s.score}/6 | {' · '.join(s.reasons)}")

    for sig in signals:
        if trade_count() >= cfg.max_positions: break
        await enter_trade(sig)


async def main_loop() -> None:
    global _active_symbols, _cycle, _warmed_up

    await start_health_server()

    logger.info("=" * 65)
    logger.info("  PHANTOM EDGE BOT v6.2 — ZigZag + HMA + FutureTrend")
    logger.info(f"  Pivot:{cfg.pivot_len} HMA:{cfg.hma_len} FT:{cfg.ft_period}")
    logger.info(f"  SL=ATR×{cfg.atr_mult} RR=1:{cfg.rr} Score≥{cfg.min_score}/6")
    logger.info(f"  {cfg.trade_usdt}USDT × {cfg.leverage}x MaxPos={cfg.max_positions}")
    logger.info("=" * 65)

    bal = await ex.get_balance()
    logger.info(f"  Balance: {bal:.4f} USDT")

    _active_symbols = await get_symbols(cfg.symbols_raw)
    logger.info(f"  Simbolos: {len(_active_symbols)}")

    # ── WARMUP PHASE (runs once, LOW concurrency = reliable) ──
    logger.info(f"[WARMUP] Iniciando carga de {len(_active_symbols)} símbolos...")
    logger.info(f"[WARMUP] ~{len(_active_symbols)//8//6} minutos estimados...")
    await notifier.notify(
        f"Phantom Edge Bot v6.2 iniciando\n"
        f"Cargando {len(_active_symbols)} pares en memoria...\n"
        f"Esto toma ~2-3 min. Se notificará cuando esté listo."
    )

    warmed = await warmup_all(
        _active_symbols,
        tf_5m=cfg.timeframe,
        tf_15m=cfg.timeframe_slow,
        batch=8,  # conservative concurrency for cold fetch
    )
    _warmed_up = True

    await notifier.test_telegram()
    await notifier.notify(
        f"Phantom Edge Bot v6.2 LISTO\n"
        f"ZigZag(pivot={cfg.pivot_len}) + HMA({cfg.hma_len}) + FT({cfg.ft_period})\n"
        f"Pares calentados: {warmed}/{len(_active_symbols)}\n"
        f"Score≥{cfg.min_score}/6 | RR 1:{cfg.rr} | x{cfg.leverage}\n"
        f"Balance: {bal:.4f} USDT | Escaneando cada {cfg.scan_interval}s"
    )

    await sync_from_exchange()

    loop = asyncio.get_event_loop()
    stop_event = asyncio.Event()
    for s in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(s, lambda: stop_event.set())

    while not stop_event.is_set():
        try:
            if is_halted():
                await asyncio.sleep(60); continue

            if _cycle > 0 and _cycle % SYMBOL_REFRESH_CYCLES == 0:
                _active_symbols = await get_symbols(cfg.symbols_raw)
                logger.info(f"[SYMBOLS] {len(_active_symbols)} pares")

            _cycle += 1
            t0   = time.perf_counter()
            dead = _in_dead_session()

            logger.info(
                f"CICLO {_cycle:04d} | {len(_active_symbols)} pares | "
                f"open={trade_count()}/{cfg.max_positions} | "
                f"loss={consecutive_losses()} | {'MUERTA' if dead else 'OK'}"
            )

            ohlcv_map = await fetch_universe(
                _active_symbols,
                tf_5m=cfg.timeframe, tf_15m=cfg.timeframe_slow,
                max_concurrent=cfg.max_concurrent,
                min_vol_mult=cfg.min_vol_mult,
            )

            await manage_positions(ohlcv_map)
            if not dead:
                await scan_cycle(ohlcv_map)

            elapsed = time.perf_counter() - t0
            _times.append(elapsed)
            if len(_times) > 50: _times.pop(0)
            avg = sum(_times[-5:])/min(len(_times),5)
            sleep_for = max(0., cfg.scan_interval - elapsed)
            logger.info(f"Ciclo {elapsed:.1f}s (avg={avg:.1f}s) | sleep {sleep_for:.0f}s")

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=sleep_for)
            except asyncio.TimeoutError:
                pass

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[ERROR] {e}")
            await notifier.notify(f"Error: {e}")
            await asyncio.sleep(30)

    logger.info("Bot detenido")
    await ex.close_session()


if __name__ == "__main__":
    asyncio.run(main_loop())
