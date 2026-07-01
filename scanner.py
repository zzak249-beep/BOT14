"""
renewed-love — Dual Scanner
============================
Estrategia 1 (rápida, cada 60s): EMA9 × VWAP en 500+ símbolos
Estrategia 2 (profunda, cada 5min): Unicorn Model en top 150 símbolos
  → Sweep de liquidez HTF + Breaker Block + FVG
  → Win rate esperado: 60-65%  R/R: 2:1

Sin TradingView de pago — todo Python, 100% automático.
"""
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import config
from bingx_client      import BingXClient
from position_manager  import PositionManager
from risk_manager      import RiskManager
from strategy          import get_indicators      # EMA9×VWAP indicators
import strategy_unicorn as unicorn               # Unicorn Model
from telegram_client   import TelegramClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
log = logging.getLogger("scanner")

# ── Health server ─────────────────────────────────────────────

def _start_health():
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200); self.end_headers()
            self.wfile.write(b"ok")
        def log_message(self, *a): pass
    try:
        s = HTTPServer(("0.0.0.0", config.PORT), H)
        threading.Thread(target=s.serve_forever, daemon=True).start()
        log.info(f"Health server :{config.PORT}/health")
    except Exception as e:
        log.warning(f"Health server: {e}")


# ── Shared state: open position tracking ─────────────────────

_open_symbols: set[str] = set()   # symbols with open positions
_cooldown:     dict[str, float] = {}  # last close ts per symbol


def _in_cooldown(symbol: str) -> bool:
    cd = getattr(config, "TRADE_COOLDOWN_SEC", 180)
    return time.time() - _cooldown.get(symbol, 0) < cd


def _close_position(client, pos_mgr, risk, tg,
                    symbol, side, reason):
    pos = pos_mgr.get_position(symbol, side)
    if not pos:
        return
    price = client.get_mark_price(symbol)
    pnl   = pos["unrealizedPnl"]
    if side == "LONG":
        pos_mgr.close_long(symbol, pos["size"], reason)
    else:
        pos_mgr.close_short(symbol, pos["size"], reason)
    risk.record_trade(pnl)
    tg.exit_trade(config.BOT_NAME, symbol, side, price, reason, pnl)
    _open_symbols.discard(symbol)
    _cooldown[symbol] = time.time()


# ── Position manager ──────────────────────────────────────────

def _manage_open_positions(client, pos_mgr, risk, tg):
    positions = client.get_positions()
    for pos in positions:
        sym  = pos["symbol"]
        side = pos["positionSide"]
        if side not in ("LONG", "SHORT"):
            continue
        try:
            price   = client.get_mark_price(sym)
            candles = client.get_klines(sym, config.TIMEFRAME, 50)
            if len(candles) < 20:
                continue

            ind = get_indicators(candles)
            atr = ind.get("atr") or 0
            if not atr:
                continue

            # Max hold check
            if pos_mgr.is_max_hold_expired(sym, side):
                _close_position(client, pos_mgr, risk, tg,
                                sym, side, "max_hold")
                continue

            # ATR trail stop
            stop, hit = pos_mgr.tick_trail(sym, side, price, atr)
            if hit:
                _close_position(client, pos_mgr, risk, tg,
                                sym, side, "trail_stop")
                continue

        except Exception as e:
            log.error(f"manage {sym}: {e}")


# ── EMA9×VWAP fast scan ───────────────────────────────────────

def _scan_ema9_vwap(client, pos_mgr, risk, tg, symbols, equity) -> int:
    opened = 0
    for sym in symbols:
        if sym in config.BLACKLIST or sym in _open_symbols or _in_cooldown(sym):
            continue
        if pos_mgr.count_open() >= config.MAX_OPEN_TRADES:
            break
        allowed, _ = risk.can_trade(equity)
        if not allowed:
            break
        try:
            candles = client.get_klines(sym, config.TIMEFRAME, 120)
            if len(candles) < 30:
                continue
            ind  = get_indicators(candles)
            ind2 = get_indicators(candles[:-1]) if len(candles) > 2 else ind
            ema9 = ind.get("ema9"); vwap = ind.get("vwap"); atr_v = ind.get("atr")
            if not ema9 or not vwap or not atr_v: continue
            prev_ema9 = ind2.get("ema9", ema9); prev_vwap = ind2.get("vwap", vwap)
            cross_up   = ema9 > vwap and prev_ema9 <= prev_vwap
            cross_down = ema9 < vwap and prev_ema9 >= prev_vwap
            if not cross_up and not cross_down: continue
            direction = "LONG" if cross_up else "SHORT"
            if direction == "LONG"  and config.DIRECTION not in ("LONG", "BOTH"): continue
            if direction == "SHORT" and config.DIRECTION not in ("SHORT", "BOTH"): continue
            if pos_mgr.has_position(sym, direction): continue

            mark = client.get_mark_price(sym)
            qty  = pos_mgr.calc_qty(sym, mark, atr_v, equity)
            if not qty: continue

            if direction == "LONG":
                ok = pos_mgr.open_long(sym, qty, sig["atr"])
            else:
                ok = pos_mgr.open_short(sym, qty, sig["atr"])
            if ok:
                pos_mgr.place_tp_sl(sym, direction, qty, mark, sig["atr"])
                tg.entry(config.BOT_NAME, sym, direction, mark, qty, None, equity)
                _open_symbols.add(sym)
                opened += 1
                log.info(f"EMA9_VWAP {direction} {sym}  entry={mark:.6g}  ema9={ema9:.6g}  vwap={vwap:.6g}")

        except Exception as e:
            log.error(f"ema9_vwap {sym}: {e}")
    return opened


# ── Unicorn Model deep scan ───────────────────────────────────

def _scan_unicorn(client, pos_mgr, risk, tg, symbols, equity) -> int:
    """
    Unicorn Model: Sweep HTF level + Breaker Block + FVG
    Usa 5m + 1H candles. Alta precisión, señales más raras.
    Win rate esperado: 60-65% con R/R 2:1
    """
    opened = 0
    uni_symbols = symbols[:getattr(config, "UNICORN_TOP_N", 150)]

    for sym in uni_symbols:
        if sym in config.BLACKLIST or sym in _open_symbols or _in_cooldown(sym):
            continue
        if pos_mgr.count_open() >= config.MAX_OPEN_TRADES:
            break
        allowed, _ = risk.can_trade(equity)
        if not allowed:
            break
        try:
            c5m = client.get_klines(sym, "5m",  200)
            c1h = client.get_klines(sym, "1h",   60)
            c15 = client.get_klines(sym, "15m",  30)
            c30 = client.get_klines(sym, "30m",  20)
            sig = unicorn.get_signal(c5m, c1h, config,
                                     candles_15m=c15, candles_30m=c30)

            if not sig["signal"]:
                continue

            direction = sig["signal"]
            if direction == "LONG"  and config.DIRECTION not in ("LONG", "BOTH"): continue
            if direction == "SHORT" and config.DIRECTION not in ("SHORT", "BOTH"): continue
            if pos_mgr.has_position(sym, direction): continue

            mark = client.get_mark_price(sym)
            atr  = sig["atr"]
            qty  = pos_mgr.calc_qty(sym, mark, atr, equity)
            if not qty: continue

            fvg_tag = "+FVG" if sig["has_fvg"] else ""
            log.info(
                f"UNICORN{fvg_tag} {direction} {sym}  "
                f"swept={sig['swept_level']:.6g}  "
                f"breaker={sig['breaker_bottom']:.6g}-{sig['breaker_top']:.6g}  "
                f"tp={sig['tp_price']:.6g}"
            )

            if direction == "LONG":
                ok = pos_mgr.open_long(sym, qty, atr)
            else:
                ok = pos_mgr.open_short(sym, qty, atr)

            if ok:
                pos_mgr.place_tp_sl(sym, direction, qty, mark, atr)
                tg.entry(config.BOT_NAME, sym, direction, mark, qty,
                         sig["sl_price"], equity)
                _open_symbols.add(sym)
                opened += 1

        except Exception as e:
            log.error(f"unicorn {sym}: {e}")
    return opened


# ── Main loop ─────────────────────────────────────────────────

def main():
    _start_health()
    log.info(f"=== {config.BOT_NAME} starting (SHORT_ONLY={config.SHORT_ONLY}) ===")

    client  = BingXClient(config.API_KEY, config.SECRET_KEY, config.BASE_URL)
    pos_mgr = PositionManager(client, config)
    risk    = RiskManager(config)
    tg      = TelegramClient(config.TELEGRAM_TOKEN, config.TELEGRAM_CHAT)

    equity = client.get_equity()
    risk.new_day(equity)
    log.info(f"New day — equity: {equity:.2f} USDT")

    last_ema_t     = 0.0
    last_unicorn_t = 0.0
    iteration      = 0

    while True:
        try:
            now    = time.time()
            equity = client.get_equity()

            _manage_open_positions(client, pos_mgr, risk, tg)

            # Get symbol universe (cached internally by bingx_client)
            symbols = client.get_top_symbols(config.TOP_N_SYMBOLS,
                                             config.MIN_VOLUME_USDT)

            # ── Unicorn scan every 5 min (alta calidad) ───────────────
            unicorn_interval = getattr(config, "UNICORN_SCAN_SEC", 300)
            if now - last_unicorn_t >= unicorn_interval:
                last_unicorn_t = now
                t0 = time.time()
                u_opened = _scan_unicorn(client, pos_mgr, risk, tg, symbols, equity)
                log.info(
                    f"UNICORN scan | {min(len(symbols), getattr(config,'UNICORN_TOP_N',150))} símbolos "
                    f"| {u_opened} abiertos | {time.time()-t0:.1f}s"
                )

            # ── EMA9×VWAP scan every 60s (volumen) ───────────────────
            if now - last_ema_t >= config.SCAN_INTERVAL:
                last_ema_t = now
                iteration += 1
                t0 = time.time()
                e_opened = _scan_ema9_vwap(client, pos_mgr, risk, tg, symbols, equity)
                log.info(
                    f"scanner | Iter {iteration} | {len(symbols)} símbolos "
                    f"| {e_opened} señales | {time.time()-t0:.1f}s"
                )

        except KeyboardInterrupt:
            log.info("Stopping.")
            break
        except Exception as e:
            log.error(f"Loop error: {e}")
            tg.error(config.BOT_NAME, str(e)[:400])
            time.sleep(30)

        time.sleep(getattr(config, "TRAILING_CHECK_SEC", 30))


if __name__ == "__main__":
    main()
