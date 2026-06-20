"""
QF×JP Bot v7.9 — Scanner COMPLETO
═══════════════════════════════════════════════════════════════════════════════
FIX v7.9:
  ✅ Nuevo filtro Order Blocks + Kaplan-Meier Survival — ver
     order_block_km.py. A diferencia de los demás filtros nuevos, este es
     STATEFUL (mismo patrón que funding_regime/volatility_regime): el
     motor ob_engine.update() siempre corre para acumular historial,
     aunque el filtro esté desactivado — así cuando lo actives ya tendrá
     muestra. OB_KM_ENABLED=False por defecto. Nunca veta, solo confirma
     o queda neutral.

FIX v7.8:
  ✅ Nuevo filtro opcional Trend Magic + RMI Sniper — ver trend_magic_rmi.py.
     Portado de un indicador Pine compartido, solo la parte con señal
     tradeable real. Reutiliza k3m. Desactivado por defecto.

FIX v7.7:
  ✅ Cada señal acumula filter_tags{} con los filtros de confirmación que
     dispararon (stc_asym, stc_vol_slope, price_action, trend_magic_rmi)
     y se pasa a journal.on_open(). Permite a trade_journal.py v7.9 medir,
     con datos reales (win rate + Deflated Sharpe), si cada filtro nuevo
     aporta o solo añade ruido.

FIX v7.6:
  ✅ Nuevo filtro opcional Price Action Framework (Large Bodies / Wicks
     Into Levels / Grindy Staircase / Choppy Range) — ver
     price_action_framework.py. Reutiliza k3m, sin llamada extra a la
     API. Desactivado por defecto (PRICE_ACTION_ENABLED=False). Solo
     para test en MODE=SIGNAL por ahora, no comprometido a ningún bot
     en vivo todavía.

FIX v7.5:
  ✅ Nuevo filtro opcional STC + Asimetría de precio (1m) — ver
     stc_asymmetry.py para el detalle completo y el aviso de que la
     fórmula de asimetría está sin verificar contra el Pine real.
     Desactivado por defecto (STC_ASYM_ENABLED=False).

FIX v7.4:
  ✅ place_limit_entry() ahora recibe sl_price/tp1_price/tp2_price — son
     obligatorios desde bingx_client.py v7.7, que coloca SL+TP1+TP2 en
     cuanto la entrada límite se llena.

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
from edge_filters import candle_turn_boost, multi_tf_slope_alignment
from btc_correlation import compute_correlation, btc_guard
from stc_asymmetry import stc_asymmetry_filter, stc_volume_slope_filter
from price_action_framework import price_action_filter
from trend_magic_rmi import trend_magic_rmi_filter
from order_block_km import ob_engine, order_block_km_filter
from ws_market_data import ws_cache
import telegram_client as tg

log = logging.getLogger("scanner")

_cb_blacklist: dict[str, float] = {}
CB_COOLDOWN = 600

# Cache de Open Interest: symbol → (oi_value, timestamp)
_oi_cache: dict[str, tuple[float, float]] = {}
OI_CACHE_TTL = 120  # segundos


async def _get_k_primary(client: BingXClient, symbol: str):
    """
    Timeframe principal (TIMEFRAME, ej. 3m) — el más sensible a latencia,
    usado para timing de entrada. Si WS_ENABLED y hay datos frescos en
    caché (>=20 velas, <90s de antigüedad), los usa en vez de REST.

    Comportamiento por defecto SIN CAMBIOS: WS_ENABLED=False → siempre
    REST, exactamente igual que antes de este módulo existir.
    """
    if getattr(C, 'WS_ENABLED', False):
        cached = ws_cache.get_latest(symbol, C.TIMEFRAME)
        if cached is not None:
            return cached
    return await client.get_klines(symbol, C.TIMEFRAME, 200)


async def _fetch_all(client: BingXClient, symbol: str):
    results = await asyncio.gather(
        _get_k_primary(client, symbol),
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
    journal=None, btc_klines: list = None,
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

    # ── 4. Turn-of-Candle boost (conservador, solo LONG) ────────────────────
    if getattr(C, 'CANDLE_TURN_ENABLED', True):
        ct_boost, ct_reason = candle_turn_boost(
            sig.direction,
            tolerance_min=getattr(C, 'CANDLE_TURN_TOLERANCE_MIN', 1),
            boost=getattr(C, 'CANDLE_TURN_BOOST', 3.0),
        )
        if ct_boost > 0:
            sig.score = min(sig.score + ct_boost, 100.0)
            sig.tier  = score_to_tier(sig.score)
            diag["counts"]["candle_turn_boost"] += 1
            log.debug("[%s] %s", symbol, ct_reason)

    # ── 5. Slope Multi-Timeframe — confluencia + anti-whipsaw ───────────────
    # FIX v7.5: slope_adj/slope_block ahora con default (0.0, False) ANTES
    # del if — el paso 5c (STC+Volumen+Slope) los necesita aunque
    # SLOPE_FILTER_ENABLED esté desactivado, para no fallar con NameError.
    slope_adj, slope_block = 0.0, False
    if getattr(C, 'SLOPE_FILTER_ENABLED', True):
        slope_adj, slope_reason, slope_block = multi_tf_slope_alignment(
            k15m, k1h, k4h, sig.direction
        )
        if slope_block:
            log.info("[%s] 🚫 Slope whipsaw block: %s", symbol, slope_reason)
            diag["counts"]["slope_block"] += 1
            return None
        if slope_adj != 0:
            sig.score = max(0.0, min(sig.score + slope_adj, 100.0))
            sig.tier  = score_to_tier(sig.score)
            diag["counts"][f"slope_adj_{slope_adj:+.0f}"] += 1
            if slope_adj >= 10:
                log.info("[%s] 📈 %s → score=%.1f", symbol, slope_reason, sig.score)

    # ── FIX v7.7: acumula qué filtros de confirmación dispararon esta señal
    # — se pasa al journal en on_open() para medir después si cada filtro
    # nuevo aporta de verdad (ver trade_journal.py v7.8, _filter_breakdown).
    filter_tags: dict = {}

    # ── 5b. STC + Asimetría de precio (1m) — confirmación de giro ───────────
    # FIX v7.5: filtro nuevo, desactivado por defecto. Ver stc_asymmetry.py
    # para el aviso completo sobre la fórmula de asimetría sin verificar
    # contra el Pine real ("QF×JP v3.6 PREDATOR"). Activar solo después de
    # confirmar en logs que el ratio calculado coincide con el panel.
    if getattr(C, 'STC_ASYM_ENABLED', False):
        try:
            k1m = await client.get_klines(symbol, "1m", 100)
        except Exception as e:
            k1m = []
            log.debug("[%s] k1m fetch error: %s", symbol, e)
        if len(k1m) >= 60:
            stc_boost, stc_reason, stc_block = stc_asymmetry_filter(
                k1m, sig.direction,
                stc_length=getattr(C, 'STC_LENGTH', 10),
                stc_fast=getattr(C, 'STC_FAST', 23),
                stc_slow=getattr(C, 'STC_SLOW', 50),
                stc_factor=getattr(C, 'STC_FACTOR', 0.5),
                stc_oversold=getattr(C, 'STC_OVERSOLD', 25.0),
                stc_overbought=getattr(C, 'STC_OVERBOUGHT', 75.0),
                asym_window=getattr(C, 'ASYM_WINDOW', 20),
                asym_veto_threshold=getattr(C, 'ASYM_VETO_THRESHOLD', 1.5),
                asym_boost_per_x=getattr(C, 'ASYM_BOOST_PER_X', 3.0),
                asym_boost_max=getattr(C, 'ASYM_BOOST_MAX', 12.0),
            )
            if stc_block:
                log.info("[%s] 🚫 STC/Asimetría veto: %s", symbol, stc_reason)
                diag["counts"]["stc_asym_veto"] += 1
                return None
            if stc_boost > 0:
                sig.score = min(sig.score + stc_boost, 100.0)
                sig.tier  = score_to_tier(sig.score)
                diag["counts"]["stc_asym_boost"] += 1
                filter_tags["stc_asym"] = stc_reason
                log.info("[%s] 🌀 %s", symbol, stc_reason)

    # ── 5c. STC + Volumen + Slope (1m) — confirmación alternativa ───────────
    # FIX v7.5: alternativa a 5b sin ninguna fórmula adivinada — volumen es
    # directo del kline, slope reutiliza multi_tf_slope_alignment ya
    # calculado arriba (no se duplica el cálculo). Desactivado por defecto,
    # independiente de STC_ASYM_ENABLED — puedes activar este, el otro, o
    # ninguno.
    if getattr(C, 'STC_VOL_SLOPE_ENABLED', False):
        try:
            k1m_vs = await client.get_klines(symbol, "1m", 100)
        except Exception as e:
            k1m_vs = []
            log.debug("[%s] k1m (vol/slope) fetch error: %s", symbol, e)
        if len(k1m_vs) >= 60:
            vs_boost, vs_reason, vs_block = stc_volume_slope_filter(
                k1m_vs, sig.direction,
                slope_adj=slope_adj, slope_block=slope_block,
                stc_length=getattr(C, 'STC_LENGTH', 10),
                stc_fast=getattr(C, 'STC_FAST', 23),
                stc_slow=getattr(C, 'STC_SLOW', 50),
                stc_factor=getattr(C, 'STC_FACTOR', 0.5),
                stc_oversold=getattr(C, 'STC_OVERSOLD', 25.0),
                stc_overbought=getattr(C, 'STC_OVERBOUGHT', 75.0),
                vol_window=getattr(C, 'STC_VOL_WINDOW', 20),
                vol_recent_n=getattr(C, 'STC_VOL_RECENT_N', 3),
                vol_min_ratio=getattr(C, 'STC_VOL_MIN_RATIO', 1.3),
                vol_boost_max=getattr(C, 'STC_VOL_BOOST_MAX', 8.0),
                slope_boost_mult=getattr(C, 'STC_SLOPE_BOOST_MULT', 0.5),
            )
            if vs_block:
                log.info("[%s] 🚫 STC/Vol/Slope veto: %s", symbol, vs_reason)
                diag["counts"]["stc_vol_slope_veto"] += 1
                return None
            if vs_boost > 0:
                sig.score = min(sig.score + vs_boost, 100.0)
                sig.tier  = score_to_tier(sig.score)
                diag["counts"]["stc_vol_slope_boost"] += 1
                filter_tags["stc_vol_slope"] = vs_reason
                log.info("[%s] 🌀 %s", symbol, vs_reason)

    # ── 5d. Price Action Framework (Zero Complexity Trading) ────────────────
    # NUEVO — solo para test en MODE=SIGNAL, no comprometido a ningún bot
    # en vivo todavía. Reutiliza k3m (ya fetcheado), sin llamada extra a la
    # API. Ver price_action_framework.py para el detalle de los 4 patrones.
    if getattr(C, 'PRICE_ACTION_ENABLED', False):
        pa_boost, pa_reason, pa_block = price_action_filter(
            k3m, sig.direction,
            lookback=getattr(C, 'PA_LOOKBACK', 20),
            body_mult=getattr(C, 'PA_BODY_MULT', 2.0),
            wick_mult=getattr(C, 'PA_WICK_MULT', 1.5),
            touch_tol_pct=getattr(C, 'PA_TOUCH_TOL_PCT', 0.1),
            min_touches=getattr(C, 'PA_MIN_TOUCHES', 3),
            boost_amount=getattr(C, 'PA_BOOST_AMOUNT', 6.0),
        )
        if pa_block:
            log.info("[%s] 🚫 Price Action veto: %s", symbol, pa_reason)
            diag["counts"]["price_action_veto"] += 1
            return None
        if pa_boost > 0:
            sig.score = min(sig.score + pa_boost, 100.0)
            sig.tier  = score_to_tier(sig.score)
            diag["counts"]["price_action_boost"] += 1
            filter_tags["price_action"] = pa_reason
            log.info("[%s] 📐 %s", symbol, pa_reason)

    # ── 5e. Trend Magic + RMI Sniper ─────────────────────────────────────────
    # NUEVO — portado de un indicador Pine compartido. Reutiliza k3m, sin
    # llamada extra a la API. Ver trend_magic_rmi.py para el detalle y lo
    # que se dejó fuera a propósito (Band/RWMA, puramente visual en el
    # original). Desactivado por defecto.
    if getattr(C, 'TREND_MAGIC_RMI_ENABLED', False):
        tm_boost, tm_reason, tm_block = trend_magic_rmi_filter(
            k3m, sig.direction,
            cci_len=getattr(C, 'TM_CCI_LEN', 20),
            atr_len=getattr(C, 'TM_ATR_LEN', 5),
            atr_mult=getattr(C, 'TM_ATR_MULT', 1.0),
            rmi_len=getattr(C, 'TM_RMI_LEN', 14),
            pmom=getattr(C, 'TM_PMOM', 66.0),
            nmom=getattr(C, 'TM_NMOM', 30.0),
            boost_amount=getattr(C, 'TM_BOOST_AMOUNT', 7.0),
        )
        if tm_block:
            log.info("[%s] 🚫 Trend Magic/RMI veto: %s", symbol, tm_reason)
            diag["counts"]["trend_magic_rmi_veto"] += 1
            return None
        if tm_boost > 0:
            sig.score = min(sig.score + tm_boost, 100.0)
            sig.tier  = score_to_tier(sig.score)
            diag["counts"]["trend_magic_rmi_boost"] += 1
            filter_tags["trend_magic_rmi"] = tm_reason
            log.info("[%s] 🧲 %s", symbol, tm_reason)

    # ── 5f. Order Blocks + Kaplan-Meier Survival ─────────────────────────────
    # NUEVO — motor STATEFUL (ver order_block_km.py), a diferencia de los
    # demás filtros de hoy. update() siempre corre (acumula historial
    # incluso si el filtro está desactivado, para que cuando lo actives
    # ya tenga muestra) — el filtro en sí, que sí puede afectar el score,
    # solo actúa si OB_KM_ENABLED=true. Nunca veta, solo confirma o queda
    # neutral (ver docstring del módulo).
    try:
        ob_engine.update(symbol, k3m,
                          z_len=getattr(C, 'OB_Z_LEN', 50),
                          max_age_bars=getattr(C, 'OB_MAX_AGE_BARS', 2000))
    except Exception as e:
        log.debug("[%s] order_block update error: %s", symbol, e)

    if getattr(C, 'OB_KM_ENABLED', False):
        ob_boost, ob_reason, ob_block = order_block_km_filter(
            symbol, sig.direction,
            survival_threshold=getattr(C, 'OB_SURVIVAL_THR', 0.6),
            boost_amount=getattr(C, 'OB_BOOST_AMOUNT', 8.0),
            min_samples=getattr(C, 'OB_MIN_SAMPLES', 5),
        )
        if ob_block:
            diag["counts"]["order_block_km_veto"] += 1
            return None
        if ob_boost > 0:
            sig.score = min(sig.score + ob_boost, 100.0)
            sig.tier  = score_to_tier(sig.score)
            diag["counts"]["order_block_km_boost"] += 1
            filter_tags["order_block_km"] = ob_reason
            log.info("[%s] 📦 %s", symbol, ob_reason)

    # ── 6. BTC Correlation Guard se evalúa DENTRO del bloque LIVE (más abajo)
    # — moverlo aquí causaba fuga de reserva si MODE=SIGNAL o si score/tier
    # rechazaban la señal después, ya que esos `return` ocurren ANTES del
    # try/finally que libera la reserva. Solo importa si de verdad se va a
    # abrir una posición real, así que solo se calcula y reserva en LIVE.

    # ── 6b. Auto-blacklist por símbolo + Streak Breaker global ───────────────
    # Aprendido de pérdidas reales, no requiere mantenimiento manual.
    if journal:
        auto_bl, auto_bl_reason = journal.is_symbol_auto_blacklisted(symbol)
        if auto_bl:
            diag["counts"]["auto_blacklist"] += 1
            return None

        streak_paused, streak_reason = journal.is_streak_paused()
        if streak_paused:
            diag["counts"]["streak_breaker"] += 1
            return None

    # ── 7. Adaptive threshold (feed del TradeJournal) ──────────────────────
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

    # FIX v7.8: can_trade() YA reservó open_count/daily_trades atómicamente.
    # A partir de aquí, CUALQUIER salida sin completar el trade DEBE liberar
    # esa reserva (y la de dirección, si llega a hacerse) — si no, el
    # contador queda inflado y bloquea trades válidos el resto del día.
    # El try/finally garantiza la liberación sin tener que repetir el
    # release en cada uno de los ~7 puntos de salida posibles.
    trade_confirmed  = False
    dir_reserved     = False
    btc_corr         = 0.0
    btc_reserved     = False

    try:
        sym_ok, sym_reason = risk.symbol_allowed(symbol)
        if not sym_ok:
            log.debug("[%s] Bloqueado por símbolo: %s", symbol, sym_reason)
            diag["counts"]["symbol_blocked"] += 1
            return None

        # Correlation guard — reserva atómica si pasa (fix v7.8)
        dir_ok, dir_reason = risk.direction_allowed(sig.direction)
        if not dir_ok:
            log.info("[%s] Bloqueado por correlación: %s", symbol, dir_reason)
            diag["counts"]["correlation_blocked"] += 1
            return None
        dir_reserved = True

        # BTC Correlation Guard — evita apilar la MISMA apuesta sobre BTC.
        # Caso real: SXT+LDO+FHE, 3 símbolos "distintos" LONG, todos
        # correlacionados con BTC, cerrados juntos con -23.47 USDT.
        # Se calcula AQUÍ (dentro del try/finally) para que la reserva
        # quede cubierta por la liberación automática si algo falla después.
        if btc_klines and getattr(C, 'BTC_CORR_ENABLED', True) and symbol != "BTC-USDT":
            btc_corr = compute_correlation(k3m, btc_klines)
            btc_guard.threshold  = getattr(C, 'BTC_CORR_THRESHOLD', 0.5)
            btc_guard.window_sec = getattr(C, 'BTC_CORR_WINDOW_SEC', 1800)
            btc_guard.max_same   = getattr(C, 'BTC_CORR_MAX_SAME', 3)
            btc_reserved = abs(btc_corr) >= btc_guard.threshold
            if btc_reserved:
                btc_ok, btc_reason = btc_guard.allowed(sig.direction, btc_corr)
                if not btc_ok:
                    log.info("[%s] 🔗 %s", symbol, btc_reason)
                    diag["counts"]["btc_correlation_blocked"] += 1
                    btc_reserved = False  # allowed() no reservó si devolvió False
                    return None

        # ── 4. Open Interest delta ───────────────────────────────────────────
        oi_delta = await _get_oi_delta(client, symbol)
        if getattr(C, 'OI_FILTER_ENABLED', False) and oi_delta < -0.05:
            log.info("[%s] OI delta negativo (%.2f) — señal descartada", symbol, oi_delta)
            diag["counts"]["oi_declining"] += 1
            return None
        if oi_delta > 0.02:
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

        # ── 5. Limit order con fallback a market ─────────────────────────────
        entry_resp = {}
        used_limit = False
        if getattr(C, 'LIMIT_ORDERS_ENABLED', False):
            # FIX v7.4: se pasan sl_price/tp1_price/tp2_price — desde
            # bingx_client.py v7.7+, place_limit_entry() los EXIGE para
            # poder colocar la protección en cuanto la entrada se llena.
            lmt_resp = await client.place_limit_entry(
                symbol, sig.direction, qty, sig.entry,
                sl_price=sig.sl, tp1_price=sig.tp1, tp2_price=sig.tp2,
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
        trade_confirmed = True

        # Registrar en journal
        if journal:
            journal.on_open(
                symbol=symbol, direction=sig.direction, tier=sig.tier,
                score=sig.score, fr=fr, obi=obi, oi_delta=oi_delta,
                htf_score=sig.htf_score, adx=sig.adx,
                filter_tags=filter_tags,
            )

        return sig

    finally:
        # FIX v7.8: si el trade NO se confirmó, liberar TODAS las reservas
        # hechas en el camino (open_count/daily_trades, dirección, BTC corr)
        # para que no queden contadores inflados bloqueando trades válidos.
        if not trade_confirmed:
            await risk.release_reservation()
            if dir_reserved:
                risk.release_direction_reservation(sig.direction)
            if btc_reserved:
                btc_guard.release(sig.direction, btc_corr)


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


# Lista de símbolos activos del scan más reciente — leída por ws_market_data
# para saber a qué símbolos suscribirse en el WebSocket. Actualizada
# automáticamente cada vez que scan_loop refresca su universo.
_current_symbols: list[str] = []


def get_current_symbols() -> list[str]:
    """Callback para ws_market_data.run_ws_client(). Nunca lanza excepción."""
    return list(_current_symbols)


async def scan_loop(client, risk, pos_mgr, complement=None, journal=None):
    log.info("Scanner v7.9 | Modo=%s | Interval=%ds | Batch=20",
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
                    _current_symbols[:] = symbols  # sincronizar para el WS client
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

        # ── BTC klines — UNA sola llamada por iteración, reutilizada por
        # todos los símbolos del scan vía BTC Correlation Guard. ───────────
        btc_klines = None
        if getattr(C, 'BTC_CORR_ENABLED', True):
            try:
                btc_klines = await client.get_klines("BTC-USDT", C.TIMEFRAME, 80)
            except Exception as e:
                log.debug("BTC klines fetch error: %s", e)

        BATCH = 20
        signals_found = 0
        for i in range(0, len(symbols), BATCH):
            batch   = symbols[i:i+BATCH]
            results = await asyncio.gather(
                *[_process_symbol(s, client, risk, pos_mgr, diag, journal, btc_klines)
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
