"""
QF×JP Bot v7.3 — Scanner COMPLETO
═══════════════════════════════════════════════════════════════════════════════
NUEVO en v7.3 (todas las mejoras del roadmap de anticipación):

  1. SESSION FILTER — evita operar en horas de bajo volumen (00:00-08:00 UTC)
     donde las pérdidas se concentran. Variables: TRADE_START_UTC / TRADE_END_UTC

  2. FUNDING RATE EXTREMO como señal:
     - FR > FR_EXTREME_THR: longs sobrecomprados → bloquea LONG, boosta SHORT +8
     - FR < -FR_EXTREME_THR: shorts sobrecomprados → bloquea SHORT, boosta LONG +8
     Anticipa la reversión ANTES de que el precio lo muestre.

  3. OPEN INTEREST DELTA como filtro de confirmación:
     - OI subiendo en dirección de señal = tendencia respaldada por posiciones reales
     - OI bajando = cierre de posiciones, trampa probable → bloquea la señal
     Cache de OI por símbolo con decay de 2 minutos.

  4. TRADE JOURNAL integrado:
     - on_open() al registrar cada trade
     - Aplica adaptive offset sobre MIN_SCORE según win rate reciente

  5. LIMIT ORDERS con fallback a market:
     - Si LIMIT_ORDERS_ENABLED=true: intenta entrada límite al mark price
     - Si no se llena en LIMIT_TIMEOUT_SECS: usa market (sin dejar posición
       sin protección)
     Ahorra ~60% en comisiones cuando el mercado coopera.

  6. CORRELATION GUARD (ya existente, sin cambios)
  7. DIAGNÓSTICO Telegram cada 5 iter sin señales (ya existente, sin cambios)
═══════════════════════════════════════════════════════════════════════════════
"""
import asyncio
import logging
import time
import datetime
from collections import Counter
from typing import Optional

import config as C
from bingx_client import BingXClient
from indicators import analyze, Signal, score_to_tier
from risk_manager import RiskManager
from position_manager import PositionManager, OpenTrade
from funding_regime import regime_engine, Regime, Window
from volatility_regime import vol_engine, Regime as VolRegime
import telegram_client as tg

log = logging.getLogger("scanner")

_cb_blacklist: dict[str, float] = {}
CB_COOLDOWN = 600

# Cache de Open Interest: symbol → (oi_value, timestamp)
_oi_cache: dict[str, tuple[float, float]] = {}
OI_CACHE_TTL = 120  # segundos


async def _fetch_all(client: BingXClient, symbol: str):
    results = await asyncio.gather(
        client.get_klines(symbol, C.TIMEFRAME,      200),
        client.get_klines(symbol, C.HTF_TIMEFRAME,  100),
        client.get_klines(symbol, C.HTF2_TIMEFRAME, 100),
        client.get_klines(symbol, C.HTF5_TIMEFRAME, 100),
        client.get_order_book(symbol, 10),
        client.get_funding_rate(symbol),
        return_exceptions=True,
    )
    def _l(r): return r if isinstance(r, list) else []
    def _d(r): return r if isinstance(r, dict) else {}
    def _f(r): return r if isinstance(r, float) else 0.0
    return (_l(results[0]), _l(results[1]), _l(results[2]), _l(results[3]),
            _d(results[4]), _f(results[5]))


def _obi(ob: dict) -> float:
    try:
        bv = sum(float(b[1]) for b in ob.get("bids", [])[:5] if len(b) >= 2)
        av = sum(float(a[1]) for a in ob.get("asks", [])[:5] if len(a) >= 2)
        t  = bv + av
        return (bv - av) / t if t > 0 else 0.0
    except Exception:
        return 0.0


async def _get_oi_delta(client: BingXClient, symbol: str) -> float:
    """
    Retorna el delta normalizado del OI:
      >0: OI creciendo (posiciones abriéndose) — tendencia confirmada
      <0: OI bajando  (posiciones cerrándose) — trampa posible
      0:  sin datos o sin cambio

    Usa cache de 120s para no spammear la API con 683 símbolos.
    """
    if not getattr(C, 'OI_FILTER_ENABLED', False):
        return 0.0
    now = time.time()
    prev_oi, prev_ts = _oi_cache.get(symbol, (0.0, 0.0))
    try:
        oi = await client.get_open_interest(symbol)
        _oi_cache[symbol] = (oi, now)
        if prev_oi > 0 and (now - prev_ts) < OI_CACHE_TTL * 3:
            return (oi - prev_oi) / prev_oi  # delta relativo
    except Exception:
        pass
    return 0.0


def _session_allowed() -> bool:
    """Retorna True si la hora UTC actual está dentro de la ventana de trading."""
    start = getattr(C, 'TRADE_START_UTC', 0)
    end   = getattr(C, 'TRADE_END_UTC',   24)
    if start == 0 and end == 24:
        return True  # desactivado = operar 24h
    h = datetime.datetime.utcnow().hour
    if start < end:
        return start <= h < end
    else:  # wrap sobre medianoche, ej: 20:00-08:00
        return h >= start or h < end


def _fr_boost_block(fr: float, direction: str) -> tuple[float, bool]:
    """
    Funding rate extremo como señal de reversión anticipada.
    Retorna (boost_pts, blocked).
    """
    thr = getattr(C, 'FR_EXTREME_THR', 0.0005)  # 0.05% por defecto
    if thr <= 0:
        return 0.0, False
    if fr > thr:
        # Longs sobrecomprados: bloquear LONG, boostear SHORT
        if direction == "LONG":
            return 0.0, True
        if direction == "SHORT":
            return 8.0, False  # confirmación adicional del SHORT
    if fr < -thr:
        # Shorts sobrecomprados: bloquear SHORT, boostear LONG
        if direction == "SHORT":
            return 0.0, True
        if direction == "LONG":
            return 8.0, False
    return 0.0, False


async def _process_symbol(
    symbol, client, risk, pos_mgr, diag: dict,
    journal=None,
) -> Optional[Signal]:

    if pos_mgr.is_trading(symbol):
        diag["counts"]["already_trading"] += 1
        return None

    # ── 1. Session filter ───────────────────────────────────────────────────
    if not _session_allowed():
        diag["counts"]["session_filter"] += 1
        return None

    now = time.time()
    if symbol in _cb_blacklist and now - _cb_blacklist[symbol] < CB_COOLDOWN:
        diag["counts"]["cb_cooldown"] += 1
        return None

    try:
        k3m, k15m, k1h, k4h, ob, fr = await _fetch_all(client, symbol)
    except Exception as e:
        log.debug("[%s] fetch error: %s", symbol, e)
        diag["counts"]["fetch_error"] += 1
        return None

    if len(k3m) < 60:
        diag["counts"]["insufficient_data"] += 1
        return None

    obi = _obi(ob)

    try:
        sig = analyze(symbol, k3m, k15m, k1h, k4h, funding_rate=fr)
    except Exception as e:
        log.warning("[%s] analyze error: %s", symbol, e)
        diag["counts"]["analyze_error"] += 1
        return None

    if sig.direction == "NONE":
        diag["counts"][sig.reason or "no_direction"] += 1
        return None

    # ── VOLATILITY REGIME — ajusta SL/TP según percentil de ATR propio ───────
    vol_sig = vol_engine.update(symbol, sig.atr, sig.entry)
    if getattr(C, 'VOL_REGIME_ENABLED', True):
        if vol_sig.block_entry:
            diag["counts"]["vol_extreme_block"] += 1
            return None
        if vol_sig.regime != VolRegime.NORMAL:
            # Reescalar distancias SL/TP según régimen de volatilidad
            sl_dist  = abs(sig.entry - sig.sl)  * vol_sig.sl_mult
            tp1_dist = abs(sig.tp1   - sig.entry) * vol_sig.tp_mult
            tp2_dist = abs(sig.tp2   - sig.entry) * vol_sig.tp_mult
            if sig.direction == "LONG":
                sig.sl  = sig.entry - sl_dist
                sig.tp1 = sig.entry + tp1_dist
                sig.tp2 = sig.entry + tp2_dist
            else:
                sig.sl  = sig.entry + sl_dist
                sig.tp1 = sig.entry - tp1_dist
                sig.tp2 = sig.entry - tp2_dist
            diag["counts"][f"vol_{vol_sig.regime.lower()}"] += 1

    # Registrar score para diagnóstico
    diag["score_n"]   += 1
    diag["score_sum"] += sig.score
    if sig.score > diag["score_max"]:
        diag["score_max"]         = sig.score
        diag["score_max_symbol"]  = symbol
        diag["score_max_dir"]     = sig.direction

    # OBI boost
    if abs(obi) > 0.1:
        boost = 0.0
        if sig.direction == "SHORT" and obi < -0.1:
            boost = abs(obi) * 5
        elif sig.direction == "LONG" and obi > 0.1:
            boost = obi * 5
        if boost > 0:
            sig.score = min(sig.score + boost, 100.0)
            sig.tier  = score_to_tier(sig.score)

    # ── 2. FUNDING REGIME — el edge profesional ─────────────────────────────
    # Actualizar historia del FR y obtener boosts de régimen + timing
    regime_sig = regime_engine.update(symbol, fr)
    regime_boost = (
        regime_sig.short_boost if sig.direction == "SHORT"
        else regime_sig.long_boost
    )
    if regime_boost != 0:
        sig.score = max(0.0, min(sig.score + regime_boost, 100.0))
        sig.tier  = score_to_tier(sig.score)
        if abs(regime_boost) >= 8:
            log.info("[%s] 💰 Regime boost %+.0f (%s) → score=%.1f",
                     symbol, regime_boost, regime_sig.reason, sig.score)
        diag["counts"][f"regime_{regime_sig.regime.lower()}"] += 1
        if regime_boost < -5:
            # Señal penalizada fuertemente → descartar
            diag["counts"]["regime_block"] += 1
            return None

    # ── 3. Funding Rate extremo (filtro binario original + regime) ──────────
    fr_boost, fr_blocked = _fr_boost_block(fr, sig.direction)
    if fr_blocked:
        diag["counts"]["fr_extreme_block"] += 1
        return None
    if fr_boost > 0:
        sig.score = min(sig.score + fr_boost, 100.0)
        sig.tier  = score_to_tier(sig.score)
        diag["counts"]["fr_extreme_boost"] += 1

    if sig.circuit_breaker:
        _cb_blacklist[symbol] = now
        await tg.notify_circuit_breaker(symbol)
        diag["counts"]["circuit_breaker"] += 1
        return None

    # ── 4. Adaptive threshold (feed del TradeJournal) ──────────────────────
    adaptive_offset = journal.get_adaptive_offset() if journal else 0.0
    effective_min   = C.MIN_SCORE + adaptive_offset
    if sig.score < effective_min:
        diag["counts"]["score_bajo"] += 1
        return None

    if not risk.tier_ok(sig.tier):
        diag["counts"][f"tier_bajo({sig.tier})"] += 1
        return None

    diag["counts"]["signal_qualified"] += 1
    log.info("[%s] Señal %s tier=%s score=%.1f fr=%.4f obi=%.2f",
             symbol, sig.direction, sig.tier, sig.score, fr, obi)

    if C.MODE == "SIGNAL":
        await tg.notify_signal(sig)
        return sig

    # ── LIVE ─────────────────────────────────────────────────────────────────
    unrealized = await pos_mgr.get_unrealized_pnl()
    can, reason = await risk.can_trade(unrealized_pnl=unrealized)
    if not can:
        log.info("[%s] Bloqueado por risk: %s", symbol, reason)
        diag["counts"]["risk_blocked"] += 1
        return None

    sym_ok, sym_reason = risk.symbol_allowed(symbol)
    if not sym_ok:
        log.debug("[%s] Bloqueado por símbolo: %s", symbol, sym_reason)
        diag["counts"]["symbol_blocked"] += 1
        return None

    # Correlation guard
    dir_ok, dir_reason = risk.direction_allowed(sig.direction)
    if not dir_ok:
        log.info("[%s] Bloqueado por correlación: %s", symbol, dir_reason)
        diag["counts"]["correlation_blocked"] += 1
        return None

    # ── 4. Open Interest delta ───────────────────────────────────────────────
    oi_delta = await _get_oi_delta(client, symbol)
    if getattr(C, 'OI_FILTER_ENABLED', False) and oi_delta < -0.05:
        # OI bajando >5%: posiciones cerrándose — señal de trampa
        log.info("[%s] OI delta negativo (%.2f) — señal descartada", symbol, oi_delta)
        diag["counts"]["oi_declining"] += 1
        return None
    if oi_delta > 0.02:
        # OI creciendo: boost leve de confirmación
        sig.score = min(sig.score + 3, 100.0)
        sig.tier  = score_to_tier(sig.score)

    try:
        balance = await client.get_balance()
    except Exception as e:
        log.error("[%s] get_balance error: %s", symbol, e)
        return None

    if balance < 5.0:
        log.warning("Balance=%.4f — usando CAPITAL=%.2f", balance, C.CAPITAL)
        balance = C.CAPITAL

    qty = risk.kelly_position_size(balance, sig.entry, sig.sl, sig.score, sig.tier, symbol=symbol)
    if qty <= 0:
        log.warning("[%s] qty=0, skip", symbol)
        return None

    log.info("[%s] qty=%.6f notional=%.2f USDT", symbol, qty, qty * sig.entry)
    await tg.notify_signal(sig)

    # ── 5. Limit order con fallback a market ─────────────────────────────────
    entry_resp = {}
    used_limit = False
    if getattr(C, 'LIMIT_ORDERS_ENABLED', False):
        lmt_resp = await client.place_limit_entry(
            symbol, sig.direction, qty, sig.entry,
            timeout_s=getattr(C, 'LIMIT_TIMEOUT_SECS', 15),
        )
        if lmt_resp.get("code", -1) == 0:
            entry_resp = lmt_resp
            used_limit = True
            log.info("[%s] Entrada LÍMITE OK ✅ (fee ahorro 60%%)", symbol)
            await tg.notify_limit_filled(symbol, sig.direction, sig.entry, qty)

    if not used_limit:
        try:
            results = await client.open_trade(
                symbol=symbol, direction=sig.direction, quantity=qty,
                sl_price=sig.sl, tp1_price=sig.tp1, tp2_price=sig.tp2,
            )
        except Exception as e:
            log.error("[%s] open_trade error: %s", symbol, e)
            await tg.notify_error(f"open_trade({symbol})", str(e))
            return None
        entry_resp = results.get("entry", {})

    if entry_resp.get("code", -1) != 0:
        log.error("[%s] Entrada rechazada: %s", symbol, entry_resp)
        await tg.notify_error(f"entrada_rechazada({symbol})", str(entry_resp))
        return None

    order_id = str(
        entry_resp.get("data", {}).get("order", {}).get("orderId", "unknown")
        or entry_resp.get("data", {}).get("orderId", "unknown")
    )

    trade = OpenTrade(
        symbol=symbol, direction=sig.direction,
        entry=sig.entry, sl=sig.sl, tp1=sig.tp1, tp2=sig.tp2,
        qty=qty, atr=sig.atr, order_id=order_id,
    )
    await pos_mgr.register_trade(trade)
    await tg.notify_trade_opened(sig, qty, order_id)

    # Registrar en journal
    if journal:
        journal.on_open(
            symbol=symbol, direction=sig.direction, tier=sig.tier,
            score=sig.score, fr=fr, obi=obi, oi_delta=oi_delta,
            htf_score=sig.htf_score, adx=sig.adx,
        )

    return sig


def _new_diag() -> dict:
    return {
        "counts": Counter(), "score_n": 0, "score_sum": 0.0,
        "score_max": 0.0, "score_max_symbol": "", "score_max_dir": "",
    }


async def _harvest_scan(
    symbols: list, client: BingXClient,
    risk: RiskManager, pos_mgr: PositionManager,
    diag: dict, journal=None,
):
    """
    Funding Harvest Scanner — ejecuta cada 8 iteraciones (~8 min).

    Busca símbolos con:
      - FR > HARVEST_FR_THR (0.10%/8h) → oportunidad SHORT pre-funding
      - FR < -HARVEST_FR_THR/2          → oportunidad LONG pre-funding

    El harvest aprovecha que los traders apalancados CIERRAN posiciones
    en las 2h previas al pago de funding → movimiento predecible.
    Sizing más pequeño que señales normales, SL más ajustado (1.0 ATR).
    """
    harvest_thr = getattr(C, 'HARVEST_FR_THR', 0.0010)
    if harvest_thr <= 0:
        return

    window = regime_engine._classify_window()
    if window not in (Window.PREFUND_MAX, Window.PREFUND_PREP):
        return  # Solo activo en ventana pre-funding

    htf = regime_engine.hours_to_next_funding()
    log.info("🌾 Harvest scan — ventana %s (%.1fh hasta funding) | %d símbolos",
             window, htf, len(symbols))

    candidates = []
    for symbol in symbols[:30]:
        if pos_mgr.is_trading(symbol):
            continue
        try:
            fr = await client.get_funding_rate(symbol)
        except Exception:
            continue
        is_harv, direction, yield_pct = regime_engine.is_harvest_opportunity(
            symbol, fr, harvest_thr
        )
        if is_harv:
            candidates.append((symbol, direction, fr, yield_pct))

    if not candidates:
        return

    # Ordenar por yield y tomar el mejor
    candidates.sort(key=lambda x: x[3], reverse=True)
    log.info("🌾 Harvest candidates: %s",
             [(s, d, f'{fr*100:.3f}%') for s, d, fr, _ in candidates[:3]])

    # Solo abrir el mejor candidato por scan (no apilar harvests)
    symbol, direction, fr, yield_pct = candidates[0]

    # Check de riesgo
    unrealized = await pos_mgr.get_unrealized_pnl()
    can, reason = await risk.can_trade(unrealized_pnl=unrealized)
    if not can:
        log.debug("Harvest bloqueado por risk: %s", reason)
        return

    try:
        balance = await client.get_balance()
    except Exception:
        return
    if balance < 5:
        balance = C.CAPITAL

    # Klines para ATR
    try:
        k3m = await client.get_klines(symbol, C.TIMEFRAME, 50)
        if len(k3m) < 20:
            return
        import numpy as np
        highs  = np.array([c[2] for c in k3m[-20:]])
        lows   = np.array([c[3] for c in k3m[-20:]])
        closes = np.array([c[4] for c in k3m[-20:]])
        tr = np.maximum(highs - lows,
             np.maximum(abs(highs - np.roll(closes, 1)),
                        abs(lows  - np.roll(closes, 1))))
        atr   = float(np.mean(tr[1:]))
        price = float(k3m[-1][4])
    except Exception as e:
        log.debug("Harvest klines error: %s", e)
        return

    # Sizing harvest: MAX_NOTIONAL * 0.25 (más pequeño que señales normales)
    harvest_notional = getattr(C, 'MAX_NOTIONAL_USDT', 200) * 0.25
    qty = harvest_notional / price / C.LEVERAGE
    qty = client._round_qty(symbol, qty)
    if qty <= 0:
        return

    # SL más ajustado para harvest (1.0 ATR vs 2.0 de señales normales)
    sl_mult = 1.0
    if direction == "LONG":
        sl_price  = price - atr * sl_mult
        tp1_price = price + atr * 1.0
        tp2_price = price + atr * 2.0
    else:
        sl_price  = price + atr * sl_mult
        tp1_price = price - atr * 1.0
        tp2_price = price - atr * 2.0

    log.info("🌾 HARVEST %s %s @ %.6f | yield=%.3f%%/8h | SL @ %.6f",
             symbol, direction, price, yield_pct*100, sl_price)

    await tg.notify_harvest_opportunity(symbol, direction, fr, yield_pct, htf)

    try:
        results = await client.open_trade(
            symbol=symbol, direction=direction, quantity=qty,
            sl_price=sl_price, tp1_price=tp1_price, tp2_price=tp2_price,
        )
    except Exception as e:
        log.error("Harvest open_trade error: %s", e)
        return

    entry_resp = results.get("entry", {})
    if entry_resp.get("code", -1) != 0:
        log.warning("Harvest entrada rechazada: %s", entry_resp)
        return

    order_id = str(
        entry_resp.get("data", {}).get("order", {}).get("orderId", "harvest")
        or "harvest"
    )
    trade = OpenTrade(
        symbol=symbol, direction=direction,
        entry=price, sl=sl_price, tp1=tp1_price, tp2=tp2_price,
        qty=qty, atr=atr, order_id=order_id,
    )
    await pos_mgr.register_trade(trade)
    if journal:
        journal.on_open(symbol=symbol, direction=direction, tier="HARVEST",
                        score=90.0, fr=fr, obi=0.0, oi_delta=0.0)
    diag["counts"]["harvest_opened"] += 1


async def scan_loop(client, risk, pos_mgr, complement=None, journal=None):
    log.info("Scanner v7.3 | Modo=%s | Interval=%ds | Batch=20",
             C.MODE, C.SCAN_INTERVAL)
    symbols:   list[str] = []
    iteration: int       = 0

    while True:
        start = time.time()
        iteration += 1
        diag = _new_diag()

        if iteration == 1 or iteration % 10 == 0 or not symbols:
            try:
                all_syms = await client.get_all_symbols()
                if all_syms:
                    if complement and complement.get_exclusive_symbols():
                        symbols = complement.get_exclusive_symbols()
                        log.info("Modo EXCLUSIVO: %d símbolos (top por volumen)", len(symbols))
                    else:
                        symbols = all_syms
                        log.info("Símbolos activos: %d", len(symbols))
                else:
                    log.warning("get_all_symbols vacío (iter=%d)", iteration)
            except Exception as e:
                log.error("get_all_symbols error: %s", e)
                if not symbols:
                    await asyncio.sleep(30)
                    continue

        if not symbols:
            await asyncio.sleep(10)
            continue

        if iteration % 20 == 0:
            try:
                balance    = await client.get_balance()
                unrealized = await pos_mgr.get_unrealized_pnl()
                await tg.notify_status(risk.status(unrealized_pnl=unrealized), balance, len(symbols))
            except Exception:
                pass

        BATCH = 20
        signals_found = 0
        for i in range(0, len(symbols), BATCH):
            batch   = symbols[i:i+BATCH]
            results = await asyncio.gather(
                *[_process_symbol(s, client, risk, pos_mgr, diag, journal)
                  for s in batch],
                return_exceptions=True,
            )
            for r in results:
                if isinstance(r, Signal) and r.direction != "NONE":
                    signals_found += 1
            await asyncio.sleep(0.2)

        elapsed = time.time() - start

        # Diagnóstico de rechazo
        top5    = diag["counts"].most_common(5)
        avg_sc  = diag["score_sum"] / diag["score_n"] if diag["score_n"] else 0.0
        top_str = " | ".join(f"{k}={v}" for k, v in top5) if top5 else "—"

        # Adaptive offset del journal
        adaptive_str = ""
        if journal and journal.get_adaptive_offset() != 0.0:
            adaptive_str = f" | adaptive_offset={journal.get_adaptive_offset():+.0f}"

        log.info(
            "Iter %d | %d símbolos | %d señales | %.1fs | "
            "direccionales=%d avg_score=%.1f max_score=%.1f(%s %s)%s | %s",
            iteration, len(symbols), signals_found, elapsed,
            diag["score_n"], avg_sc, diag["score_max"],
            diag["score_max_symbol"], diag["score_max_dir"],
            adaptive_str, top_str,
        )

        # Telegram: diagnóstico cada 5 iter sin señales
        if iteration % 5 == 0 and signals_found == 0:
            try:
                await tg.notify_diagnostics(
                    iteration, len(symbols), diag["score_n"], avg_sc,
                    diag["score_max"], diag["score_max_symbol"], diag["score_max_dir"],
                    top5,
                )
            except Exception:
                pass

        # Telegram: journal report cada 50 iter (si hay datos)
        if journal and iteration % 50 == 0 and journal.total_closed() > 0:
            try:
                await tg.notify_journal_report(journal.stats())
            except Exception:
                pass

        # ── HARVEST SCAN — funding market-neutral (cada 8 iteraciones) ─────
        # Busca símbolos con FR extremo + ventana pre-funding para capturar
        # el movimiento de longs/shorts que cierran antes del pago.
        if iteration % 8 == 0 and C.MODE == "LIVE":
            await _harvest_scan(symbols[:50], client, risk, pos_mgr, diag, journal)

        await asyncio.sleep(max(0.0, C.SCAN_INTERVAL - elapsed))
