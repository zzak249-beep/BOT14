"""
joyful-art — SHORT-only scalper scanner.

Loop:
  Every POSITION_CHECK_INTERVAL s : manage open positions
  Every SCAN_INTERVAL s           : scan for new SHORT entries

Log format (matches existing Railway logs):
  scanner | Iter N | 100 símbolos | N señales | Ns |
  direccionales=N avg=X max=X | session_filter=N

FIX (parche temporal): _startup_reconcile() / _startup_cleanup_orders()
desactivadas en main(). Las dos llaman a client.get_positions() sin
symbol → traen TODAS las posiciones de la cuenta, no solo las de
joyful-art. _startup_cleanup_orders() además CANCELA las órdenes de
cualquier posición SHORT que encuentre y coloca un SL/TP propio
encima — si la cuenta se comparte con otro bot, le borra y sustituye
su protección real en cada restart. Ver comentario en main().

FIX: TP1 partial ahora comprueba el resultado de close_short() antes
de marcar tp1_hit y notificar — antes lo hacía incondicionalmente,
igual que el bug ya corregido en renewed-love.

PENDIENTE (necesita state.py / position_manager.py / risk_manager.py
de este bot para arreglarse con seguridad): n_open/slots en main() y
el bucle de _manage_positions() también usan client.get_positions()
sin filtrar — mismo patrón de fondo que el bug de MAX_OPEN_TRADES ya
arreglado en renewed-love, aquí sin resolver todavía.
"""

import logging
import time
import traceback

import config
import state
import utils
from bingx_client import BingXClient
from ibs_filter import ibs_score
from bb_short_filter import bb_short_score
from ema9_vwap_filter import ema9_vwap_score
from position_manager import PositionManager
from risk_manager import RiskManager
from strategy import get_indicators
from telegram_client import TelegramClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
log = logging.getLogger("scanner")




def _start_health_server():
    """Minimal HTTP server for Railway healthcheck on /health."""
    import threading
    from http.server import HTTPServer, BaseHTTPRequestHandler

    class _H(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        def log_message(self, *a):
            pass  # suppress access logs

    try:
        port = getattr(config, "PORT", 8080)
        srv = HTTPServer(("0.0.0.0", port), _H)
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()
        log.info(f"Health server :{ port}/health")
    except Exception as e:
        log.warning(f"Health server failed: {e}")

def main():
    _start_health_server()
    log.info(f"=== {config.BOT_NAME} starting (SHORT_ONLY={config.SHORT_ONLY}) ===")

    client  = BingXClient(config.API_KEY, config.SECRET_KEY, config.BASE_URL)
    pos_mgr = PositionManager(client)
    risk    = RiskManager(config)
    tg      = TelegramClient(config.TELEGRAM_TOKEN, config.TELEGRAM_CHAT)

    tg.startup(config.BOT_NAME, config.TIMEFRAME, config.LEVERAGE)

    # FIX: desactivado — ver docstring del módulo. Operan sobre TODA la
    # cuenta (client.get_positions() sin symbol), no solo posiciones
    # propias. _startup_cleanup_orders() cancela y reemplaza SL/TP de
    # cualquier SHORT que encuentre, sea de este bot o de otro.
    # Reactivar solo tras filtrar a posiciones trackeadas en state.py.
    # _startup_reconcile(client, tg)
    # _startup_cleanup_orders(client, pos_mgr, tg)

    iteration    = 0
    last_scan_t  = 0.0
    last_pos_t   = 0.0

    while True:
        now = time.time()
        try:
            # ── Position management (every POSITION_CHECK_INTERVAL) ──
            if now - last_pos_t >= config.POSITION_CHECK_INTERVAL:
                _manage_positions(client, pos_mgr, risk, tg)
                last_pos_t = time.time()

            # ── Signal scan (every SCAN_INTERVAL) ───────────────────
            if now - last_scan_t >= config.SCAN_INTERVAL:
                t0 = time.time()
                iteration += 1

                in_session = utils.in_trading_session()
                equity     = client.get_equity()

                if not in_session:
                    elapsed = time.time() - t0
                    log.info(
                        f"scanner | Iter {iteration} | {config.TOP_N_SYMBOLS} símbolos "
                        f"| 0 señales | {elapsed:.1f}s | direccionales=0 avg=0.0 max=0.0 "
                        f"| session_filter={config.TOP_N_SYMBOLS}"
                    )
                    last_scan_t = time.time()
                    time.sleep(5)
                    continue

                allowed, reason = risk.can_trade(equity)
                if not allowed:
                    log.warning(f"blocked: {reason}")
                    tg.blocked(config.BOT_NAME, reason)
                    last_scan_t = time.time()
                    time.sleep(5)
                    continue

                n_open = len(client.get_positions())
                slots  = config.MAX_OPEN_TRADES - n_open
                n_sig  = avg_sc = max_sc = 0

                if slots > 0:
                    n_sig, avg_sc, max_sc = _scan_and_enter(
                        client, pos_mgr, risk, tg, slots, equity
                    )

                elapsed = time.time() - t0
                log.info(
                    f"scanner | Iter {iteration} | {config.TOP_N_SYMBOLS} símbolos "
                    f"| {n_sig} señales | {elapsed:.1f}s "
                    f"| direccionales={n_sig} avg={avg_sc:.1f} max={max_sc:.1f}"
                )
                last_scan_t = time.time()

        except KeyboardInterrupt:
            log.info("Stopped.")
            break
        except Exception as e:
            log.error(f"Loop error: {e}\n{traceback.format_exc()}")
            tg.error(config.BOT_NAME, str(e)[:400])
            time.sleep(30)

        time.sleep(2)


# ── Startup reconciliation ─────────────────────────────────────

def _startup_reconcile(client: BingXClient, tg: TelegramClient):
    """
    FIX: save entry_ts NOW for any position not already in state.py.
    Ensures MAX_HOLD works after Railway restart even for existing positions.
    Also alerts on blacklisted symbols still open.

    DESACTIVADA en main() — ver docstring del módulo. client.get_positions()
    trae toda la cuenta; esto adoptaría como propia cualquier posición de
    otro bot que no tenga entry_ts en el state.py de joyful-art.
    """
    positions = client.get_positions()
    reconciled = 0
    for pos in positions:
        sym  = pos["symbol"]
        side = pos["positionSide"]
        if state.get_entry_ts(sym, side) is None:
            state.save_entry(sym, side)
            log.info(f"Reconcile: {sym} {side} → entry_ts=now")
            reconciled += 1
        if utils.is_blacklisted(sym):
            log.warning(f"Blacklisted symbol {sym} has open position!")
            tg.info(config.BOT_NAME,
                    f"⚠ {sym} está en blacklist pero tiene posición abierta. "
                    f"Cierra manualmente o espera a max_hold.")
    if reconciled:
        log.info(f"Reconciled {reconciled} existing positions into state.py")


def _startup_cleanup_orders(client: BingXClient, pos_mgr: PositionManager,
                             tg: TelegramClient):
    """
    FIX: cancel ALL orphan orders for every open position on startup.
    Solves the 20-orders accumulation bug caused by cancel failures.
    Then re-places a fresh SL + TP1 based on entry price and current ATR.
    Also re-initializes trail stop from current price so it works immediately.

    DESACTIVADA en main() — ver docstring del módulo. client.get_positions()
    trae toda la cuenta; esto cancela y reemplaza el SL/TP de cualquier
    posición SHORT que encuentre, sea de este bot o de otro compartiendo
    la misma cuenta de BingX.
    """
    positions = client.get_positions()
    if not positions:
        return

    log.info(f"Startup order cleanup: {len(positions)} positions")
    for pos in positions:
        sym   = pos["symbol"]
        side  = pos["positionSide"]
        size  = pos["size"]
        entry = pos["entryPrice"]

        if side != "SHORT":
            continue

        try:
            # Step 1: Cancel ALL existing orders (kills accumulation)
            client.cancel_all_open_orders(sym)
            log.info(f"Cleanup: cancelled all orders for {sym}")

            # Step 2: Get current ATR
            candles = client.get_klines(sym, config.TIMEFRAME, 80)
            if len(candles) < 20:
                continue
            ind = get_indicators(candles)
            atr = ind.get("atr")
            if not atr:
                continue

            mark = client.get_mark_price(sym)

            # Step 3: Re-initialize trail stop from current price
            # (avoids stale trail that never fires after restart)
            current_trail = state.get_trail(sym, side)
            if current_trail is None:
                fresh_stop = mark + atr * config.TRAIL_DISTANCE_ATR
                state.save_trail(sym, side, fresh_stop)
                log.info(f"Cleanup: trail reset {sym} → {fresh_stop:.6g}")

            # Step 4: Re-place fresh SL + TP1 (max 2 orders)
            pos_mgr.place_tp_sl(sym, side, entry, size, atr)
            log.info(f"Cleanup: fresh TP/SL placed for {sym}")

        except Exception as e:
            log.error(f"Cleanup {sym}: {e}")

    tg.info(config.BOT_NAME, f"✅ Startup cleanup: {len(positions)} posiciones saneadas")


# ── Position management ────────────────────────────────────────

def _manage_positions(client: BingXClient, pos_mgr: PositionManager,
                      risk: RiskManager, tg: TelegramClient):
    try:
        positions = client.get_positions()
    except Exception as e:
        log.error(f"get_positions: {e}")
        return

    for pos in positions:
        sym   = pos["symbol"]
        side  = pos["positionSide"]
        size  = pos["size"]
        entry = pos["entryPrice"]
        pnl   = pos["unrealizedPnl"]

        if side != "SHORT":
            continue

        try:
            mark    = client.get_mark_price(sym)
            candles = client.get_klines(sym, config.TIMEFRAME, 80)
            if len(candles) < 20:
                continue
            ind = get_indicators(candles)
            atr = ind.get("atr")
            if not atr:
                continue

            # 1. MAX_HOLD (reads from disk — survives restarts)
            if pos_mgr.is_max_hold_expired(sym, "SHORT"):
                _exit(pos_mgr, risk, tg, sym, "SHORT", size, mark, pnl, "max_hold")
                continue

            # 2. TP2 full close
            if state.is_tp1_hit(sym, "SHORT") and \
               pos_mgr.should_take_tp2(sym, "SHORT", mark, entry, atr):
                _exit(pos_mgr, risk, tg, sym, "SHORT", size, mark, pnl, "tp2")
                continue

            # 3. TP1 partial (50%)
            if pos_mgr.should_take_tp1(sym, "SHORT", mark, entry, atr):
                tp_qty = pos_mgr._round_qty(sym, size * 0.5)
                if tp_qty >= pos_mgr._min_qty(sym):
                    if pos_mgr.close_short(sym, tp_qty, "tp1_partial"):   # FIX: comprobar éxito
                        pos_mgr.mark_tp1_hit(sym, "SHORT")
                        tg.exit_trade(config.BOT_NAME, sym, "SHORT", mark,
                                      "TP1 partial", pnl * 0.5)

            # 4. ATR trail stop
            stop, hit = pos_mgr.tick_trail(sym, "SHORT", mark, atr)
            if hit:
                _exit(pos_mgr, risk, tg, sym, "SHORT", size, mark, pnl, "trail_stop")
                continue

            # 5. Breakeven SL
            if pos_mgr.should_move_breakeven(sym, "SHORT", mark, entry, atr):
                pos_mgr.move_sl_to_breakeven(sym, "SHORT", entry, size)

            # 6. EMA exit (price crosses above EMA9 = momentum lost)
            if config.EMA_EXIT_ENABLED:
                ema9 = ind.get("ema9")
                if ema9 and mark > ema9:
                    entry_ts = state.get_entry_ts(sym, "SHORT")
                    held_min = (time.time() - entry_ts) / 60 if entry_ts else 99
                    if held_min >= config.EMA_EXIT_MIN_HOLD_MIN:
                        _exit(pos_mgr, risk, tg, sym, "SHORT", size,
                              mark, pnl, "ema_exit")
                        continue

        except Exception as e:
            log.error(f"manage {sym}: {e}")


def _exit(pos_mgr, risk, tg, sym, side, size, price, pnl, reason):
    if pos_mgr.close_short(sym, size, reason):
        risk.record_trade(pnl)
        tg.exit_trade(config.BOT_NAME, sym, side, price, reason, pnl)


# ── Signal scanning ────────────────────────────────────────────

def _scan_and_enter(client: BingXClient, pos_mgr: PositionManager,
                    risk: RiskManager, tg: TelegramClient,
                    slots: int, equity: float) -> tuple:
    n_sig   = 0

    # ── BTC trend filter: solo SHORT cuando BTC bajista ──────────────────────
    if getattr(config, "BTC_TREND_FILTER", True):
        try:
            c = client.get_klines("BTC-USDT", "1h", 60)
            if len(c) >= 52:
                cls = [x["close"] for x in c]
                def _ema(v, p):
                    k=2/(p+1); e=v[0]
                    for x in v[1:]: e=x*k+e*(1-k)
                    return e
                e20 = _ema(cls[-20:], 20)
                e50 = _ema(cls[-50:], 50)
                if e20 > e50:
                    log.info("BTC trend=UP — SHORTs bloqueados")
                    return n_sig, 0.0, 0.0
                log.info("BTC trend=DOWN — SHORTs permitidos")
        except Exception as e:
            log.warning(f"BTC trend check: {e}")
    scores  = []

    try:
        symbols = client.get_top_symbols(config.TOP_N_SYMBOLS, config.MIN_VOLUME_USDT)
    except Exception as e:
        log.error(f"get_top_symbols: {e}")
        return 0, 0.0, 0.0

    open_syms = {p["symbol"] for p in client.get_positions()}

    for sym in symbols:
        if n_sig >= slots:
            break
        if utils.is_blacklisted(sym):        # FIX: normalized blacklist check
            continue
        if sym in open_syms:
            continue

        try:
            candles = client.get_klines(sym, config.TIMEFRAME, 120)
            if len(candles) < 60:
                continue

            ind   = get_indicators(candles)
            atr   = ind.get("atr")
            price = ind.get("close")
            ema9  = ind.get("ema9")
            ema21 = ind.get("ema21")
            vol   = ind.get("volume")
            vol_ma = ind.get("vol_ma")

            if None in (atr, price, ema9, ema21):
                continue

            # Primary filter: EMA9 < EMA21 (downtrend required for SHORT)
            if ema9 >= ema21:
                continue

            # EMA9_RALLY: price near EMA9 (pullback entry)
            if config.EMA9_RALLY_ENABLED:
                distance_pct = abs(price - ema9) / ema9 * 100
                if distance_pct > config.EMA9_NEAR_PCT:
                    continue
                if vol and vol_ma and vol < vol_ma * config.EMA9_VOL_HIGH_MULT:
                    continue

            # RSI 15m filter
            rsi15m_penalty = 0
            if config.RSI15M_FILTER_ENABLED:
                try:
                    c15 = client.get_klines(sym, config.HTF_TIMEFRAME, 30)
                    i15 = get_indicators(c15)
                    rsi15m = i15.get("rsi")
                    if rsi15m and rsi15m > config.RSI15M_SHORT_MAX:
                        if config.RSI15M_REQUIRED:
                            continue
                        rsi15m_penalty = 8
                except Exception:
                    pass

            # Composite score
            score = _compute_score(price, ind, sym, client)
            score -= rsi15m_penalty
            score  = max(0, score)
            scores.append(score)

            if score < config.MIN_SCORE:
                continue

            # Sizing
            qty = pos_mgr.calc_qty(sym, price, atr, equity, score)
            if qty is None:
                continue

            # Margin check
            margin_needed = qty * price / config.LEVERAGE
            if client.get_available_margin() < margin_needed + config.MIN_MARGIN_USDT:
                log.warning(f"Low margin for {sym}")
                continue

            # Open SHORT
            ok = pos_mgr.open_short(sym, qty, atr)
            if ok:
                open_syms.add(sym)
                n_sig += 1
                risk.increment_daily_trades()
                stop = state.get_trail(sym, "SHORT") or 0
                tg.entry(config.BOT_NAME, sym, "SHORT", price, qty, stop, equity, score)
                # Place initial SL + TP1
                pos_mgr.place_tp_sl(sym, "SHORT", price, qty, atr)

        except Exception as e:
            log.error(f"scan {sym}: {e}")

    avg_s = round(sum(scores) / len(scores), 1) if scores else 0.0
    max_s = round(max(scores), 1) if scores else 0.0
    return n_sig, avg_s, max_s


# ── Composite scoring ──────────────────────────────────────────

def _compute_score(price: float, ind: dict, sym: str,
                   client: BingXClient) -> int:
    """
    Score 0-100 for SHORT setups.

    Breakdown:
      Base          : 40 pts  (passed primary filter)
      ADX           : 0-15   (trend strength)
      EMA9_VWAP     : ±9     (aligned=+9, counter=-9)
      EMA55         : +6     (price below EMA55)
      IBS pullback  : 0-12   (ibs_filter)
      BB position   : 0-10   (bb_short_filter)
      HTF counter   : -12    (counter_trend_penalty)

    MIN_SCORE=55 → base(40) + ADX≥20(7) + small_extras(8)
    FUEL_SCORE=62 → base(40) + ADX≥25(15) + EMA9_VWAP(9) - 2
    SUP_SCORE=78  → base(40) + ADX≥25(15) + EMA9_VWAP(9) + EMA55(6) + IBS(8)
    """
    score = 40

    # ADX strength
    adx = ind.get("adx")
    if adx:
        if adx >= config.ADX_TREND:    score += 15
        elif adx >= config.ADX_LATERAL: score += 7

    # EMA9_VWAP (±9)
    if config.EMA9_VWAP_ENABLED:
        score += int(ema9_vwap_score(ind, config.EMA9_VWAP_BOOST))

    # EMA55 boost
    ema55 = ind.get("ema55")
    if config.EMA55_BOOST_ENABLED and ema55 and price < ema55:
        score += 6

    # IBS pullback (0-12)
    if config.IBS_PULLBACK_ENABLED:
        score += int(ibs_score(ind))

    # BB short (0-10)
    if config.BB_SHORT_ENABLED:
        score += int(bb_short_score(ind))

    # HTF counter-trend penalty (-12)
    try:
        htf = client.get_klines(sym, config.HTF_TIMEFRAME, 40)
        htf_ind = get_indicators(htf)
        htf_ema9  = htf_ind.get("ema9")
        htf_ema21 = htf_ind.get("ema21")
        if htf_ema9 and htf_ema21 and htf_ema9 > htf_ema21:
            score -= int(config.COUNTER_TREND_PENALTY)
    except Exception:
        pass

    return max(0, score)


if __name__ == "__main__":
    main()
