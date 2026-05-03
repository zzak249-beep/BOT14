"""
Maki Bot v2 — ZigZag + 20MA 4H para BingX Futures
- Escanea TODOS los pares USDT disponibles
- Apalancamiento 10x fijo
- TP/SL automático basado en ATR
- Notificaciones Telegram detalladas con PnL real
- Resumen diario automático
- Estado persistente en disco
"""
import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

from bingx   import BingXClient
from strategy import signal as get_signal, tp_sl, risk_reward, _atr
import telegram as tg

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("bot")

# ── Config desde entorno ──────────────────────────────────────────────────────
API_KEY    = os.environ["BINGX_API_KEY"]
API_SECRET = os.environ["BINGX_API_SECRET"]
TG_TOKEN   = os.environ["TELEGRAM_BOT_TOKEN"]
TG_CHAT    = os.environ["TELEGRAM_CHAT_ID"]

TRADE_USDT   = float(os.environ.get("TRADE_AMOUNT_USDT", "10"))
MAX_TRADES   = int(os.environ.get("MAX_OPEN_TRADES", "5"))
SCAN_SEC     = int(os.environ.get("SCAN_INTERVAL_SECONDS", "60"))
LEVERAGE     = int(os.environ.get("LEVERAGE", "10"))
COOLDOWN_SEC = int(os.environ.get("COOLDOWN_SECONDS", "300"))

# Concurrencia: cuántos pares escanear en paralelo (evitar rate-limit)
SCAN_BATCH   = int(os.environ.get("SCAN_BATCH_SIZE", "10"))
# Mínimo volumen 24h en USDT para incluir un par (filtra basura)
MIN_VOLUME   = float(os.environ.get("MIN_VOLUME_USDT", "1000000"))

# ── Estado persistente ────────────────────────────────────────────────────────
STATE_FILE = Path("/tmp/maki_state.json")


def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"trades": {}, "cooldowns": {}, "daily": {}}


def _save_state():
    try:
        STATE_FILE.write_text(json.dumps({
            "trades": open_trades,
            "cooldowns": cooldowns,
            "daily": daily_stats,
        }, default=str))
    except Exception as e:
        logger.warning(f"save_state: {e}")


state       = _load_state()
open_trades: dict = state.get("trades", {})    # symbol -> trade info
cooldowns:   dict = state.get("cooldowns", {}) # symbol -> timestamp fin cooldown
daily_stats: dict = state.get("daily", {
    "date": "", "trades": 0, "wins": 0, "losses": 0,
    "total_pnl": 0.0, "best": None, "worst": None,
})

# ── Helpers ───────────────────────────────────────────────────────────────────

async def notify(text: str):
    await tg.send(TG_TOKEN, TG_CHAT, text)


def _in_cooldown(symbol: str) -> bool:
    return datetime.now(timezone.utc).timestamp() < cooldowns.get(symbol, 0)


def _set_cooldown(symbol: str):
    cooldowns[symbol] = datetime.now(timezone.utc).timestamp() + COOLDOWN_SEC


def _reset_daily_if_new_day():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if daily_stats.get("date") != today:
        daily_stats.update({
            "date": today, "trades": 0, "wins": 0,
            "losses": 0, "total_pnl": 0.0,
            "best": None, "worst": None,
        })


def _record_closed_trade(symbol: str, pnl: float):
    _reset_daily_if_new_day()
    daily_stats["trades"] += 1
    daily_stats["total_pnl"] += pnl
    if pnl >= 0:
        daily_stats["wins"] += 1
        if daily_stats["best"] is None or pnl > daily_stats["best"][1]:
            daily_stats["best"] = [symbol, pnl]
    else:
        daily_stats["losses"] += 1
        if daily_stats["worst"] is None or pnl < daily_stats["worst"][1]:
            daily_stats["worst"] = [symbol, pnl]
    _save_state()


# ── Monitor de posiciones abiertas ────────────────────────────────────────────

async def monitor(client: BingXClient):
    """
    Comprueba precio actual vs TP/SL para cada trade abierto.
    Actúa como red de seguridad por si la stop order de BingX no se ejecuta.
    También detecta si BingX cerró la posición por su cuenta (TP/SL alcanzado).
    """
    if not open_trades:
        return

    # Obtener posiciones reales en la exchange para detectar cierres automáticos
    try:
        real_positions = {p["symbol"]: p for p in await client.get_open_positions()}
    except Exception:
        real_positions = {}

    for symbol, trade in list(open_trades.items()):
        try:
            side  = trade["side"]
            entry = trade["entry"]
            qty   = trade["qty"]
            tp    = trade["tp"]
            sl    = trade["sl"]

            # ── Detectar cierre automático por BingX (TP/SL ya ejecutado) ──
            if symbol not in real_positions:
                # La posición ya no existe en BingX → cerró sola
                price = await client.last_price(symbol)
                if side == "LONG":
                    pnl_pct = (price - entry) / entry * 100
                else:
                    pnl_pct = (entry - price) / entry * 100
                pnl_usdt = TRADE_USDT * LEVERAGE * (pnl_pct / 100)
                reason   = "TP ✅ (auto)" if pnl_usdt > 0 else "SL ❌ (auto)"

                msg = tg.msg_trade_close(symbol, side, entry, price, qty, TRADE_USDT, LEVERAGE, reason)
                logger.info(f"Auto-cerrado {symbol} {side} {reason} pnl={pnl_usdt:.2f}")
                await notify(msg)
                _record_closed_trade(symbol, pnl_usdt)
                del open_trades[symbol]
                _set_cooldown(symbol)
                _save_state()
                continue

            # ── Red de seguridad manual ──────────────────────────────────────
            price  = await client.last_price(symbol)
            hit_tp = price >= tp if side == "LONG" else price <= tp
            hit_sl = price <= sl if side == "LONG" else price >= sl

            if hit_tp or hit_sl:
                reason = "TP ✅" if hit_tp else "SL ❌"
                try:
                    exit_price = await client.close_position(symbol, side, qty)
                    await client.cancel_all_orders(symbol)
                except Exception as e:
                    logger.warning(f"close_position {symbol}: {e}")
                    exit_price = price

                if side == "LONG":
                    pnl_pct = (exit_price - entry) / entry * 100
                else:
                    pnl_pct = (entry - exit_price) / entry * 100
                pnl_usdt = TRADE_USDT * LEVERAGE * (pnl_pct / 100)

                msg = tg.msg_trade_close(symbol, side, entry, exit_price, qty, TRADE_USDT, LEVERAGE, reason)
                logger.info(f"Cerrado {symbol} {side} {reason} @ {exit_price:.6f} pnl={pnl_usdt:+.2f}")
                await notify(msg)
                _record_closed_trade(symbol, pnl_usdt)
                del open_trades[symbol]
                _set_cooldown(symbol)
                _save_state()

        except Exception as e:
            logger.warning(f"monitor {symbol}: {e}")


# ── Scanner de señales ────────────────────────────────────────────────────────

async def scan_symbol(client: BingXClient, symbol: str) -> bool:
    """
    Analiza un símbolo. Retorna True si se abrió trade.
    """
    if symbol in open_trades or _in_cooldown(symbol):
        return False
    if len(open_trades) >= MAX_TRADES:
        return False

    try:
        # Descargar velas en paralelo
        c15, c4h = await asyncio.gather(
            client.klines(symbol, "15m", limit=70),
            client.klines(symbol, "4h",  limit=30),
        )

        sig = get_signal(c15, c4h)
        if sig is None:
            return False

        # Precio de la última vela cerrada (no la que se está formando)
        entry  = c15[-2]["c"]
        tp, sl = tp_sl(entry, sig, c15)

        # Filtro R/R mínimo
        rr = risk_reward(tp, sl, entry, sig)
        if rr < 1.2:
            logger.debug(f"{symbol} {sig}: R/R={rr:.2f} < 1.2, skip")
            return False

        # ATR para info en notificación
        atr     = _atr(c15)
        atr_pct = (atr / entry * 100) if atr else None

        # Abrir orden
        order_id, qty, real_entry = await client.open_order(
            symbol, sig, TRADE_USDT, tp, sl, leverage=LEVERAGE,
        )

        # Recalcular TP/SL con precio real de ejecución
        tp, sl = tp_sl(real_entry, sig, c15)

        open_trades[symbol] = {
            "side": sig, "entry": real_entry, "tp": tp, "sl": sl,
            "qty": qty, "order_id": order_id,
            "opened_at": datetime.now(timezone.utc).isoformat(),
            "usdt": TRADE_USDT, "leverage": LEVERAGE,
        }
        _save_state()

        msg = tg.msg_trade_open(
            symbol, sig, real_entry, tp, sl, qty,
            TRADE_USDT, LEVERAGE, rr, atr_pct,
        )
        logger.info(f"Trade abierto: {symbol} {sig} @ {real_entry:.6f} R/R={rr:.2f}")
        await notify(msg)
        return True

    except Exception as e:
        logger.warning(f"scan {symbol}: {e}")
        return False


async def scan_all(client: BingXClient, symbols: list[str]):
    """
    Escanea todos los pares en batches paralelos para no saturar la API.
    """
    if len(open_trades) >= MAX_TRADES:
        return

    total    = len(symbols)
    scanned  = 0
    opened   = 0

    for i in range(0, total, SCAN_BATCH):
        if len(open_trades) >= MAX_TRADES:
            break
        batch   = symbols[i: i + SCAN_BATCH]
        results = await asyncio.gather(*[scan_symbol(client, s) for s in batch], return_exceptions=True)
        opened  += sum(1 for r in results if r is True)
        scanned += len(batch)
        # Pausa entre batches para respetar rate limits
        await asyncio.sleep(1)

    logger.info(f"Scan: {scanned}/{total} pares | {opened} trades abiertos | total={len(open_trades)}")


# ── Resumen diario ────────────────────────────────────────────────────────────

_last_summary_hour = -1


async def maybe_send_daily_summary():
    global _last_summary_hour
    now  = datetime.now(timezone.utc)
    hour = now.hour
    # Enviar resumen a las 00:00 UTC
    if hour == 0 and _last_summary_hour != 0:
        _last_summary_hour = 0
        _reset_daily_if_new_day()
        if daily_stats.get("trades", 0) > 0:
            msg = tg.msg_daily_summary(daily_stats)
            await notify(msg)
    elif hour != 0:
        _last_summary_hour = hour


# ── Loop principal ────────────────────────────────────────────────────────────

async def main():
    client = BingXClient(API_KEY, API_SECRET)

    # Precargar contratos
    logger.info("Cargando metadatos de contratos...")
    try:
        await client.load_contracts_cache()
    except Exception as e:
        logger.warning(f"load_contracts_cache: {e}")

    # Datos iniciales
    try:
        balance = await client.balance_usdt()
        symbols_raw = await client.all_usdt_symbols()
    except Exception as e:
        logger.critical(f"Error al iniciar: {e}")
        await notify(f"❌ <b>Error al iniciar Maki Bot</b>\n<code>{e}</code>")
        await client.close()
        return

    # Filtrar por volumen mínimo (elimina pares sin liquidez)
    # Necesitamos el volumen — lo obtenemos del ticker completo
    try:
        ticker_data = await client._get("/openApi/swap/v2/quote/ticker")
        vol_map = {t["symbol"]: float(t.get("quoteVolume", 0)) for t in ticker_data}
    except Exception:
        vol_map = {}

    symbols = [s for s in symbols_raw if vol_map.get(s, 0) >= MIN_VOLUME]
    logger.info(f"Pares con volumen ≥ {MIN_VOLUME:,.0f} USDT: {len(symbols)} de {len(symbols_raw)}")

    if open_trades:
        logger.info(f"Trades recuperados: {list(open_trades.keys())}")

    await notify(tg.msg_bot_start(balance, len(symbols), TRADE_USDT, LEVERAGE, MAX_TRADES))

    # Señal de apagado limpio
    loop       = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _shutdown():
        logger.info("Apagando bot...")
        stop_event.set()

    for sig_name in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig_name, _shutdown)
        except NotImplementedError:
            pass  # Windows

    symbol_refresh_ticks = 0
    REFRESH_EVERY = max(1, 3600 // SCAN_SEC)  # ~1h

    while not stop_event.is_set():
        try:
            _reset_daily_if_new_day()

            # Monitor primero (cierra trades si TP/SL alcanzado)
            await monitor(client)

            # Escanear todos los pares
            await scan_all(client, symbols)

            # Resumen diario
            await maybe_send_daily_summary()

            # Refrescar lista de pares cada hora
            symbol_refresh_ticks += 1
            if symbol_refresh_ticks >= REFRESH_EVERY:
                symbol_refresh_ticks = 0
                symbols_raw = await client.all_usdt_symbols()
                symbols     = [s for s in symbols_raw if vol_map.get(s, 0) >= MIN_VOLUME]
                logger.info(f"Pares actualizados: {len(symbols)}")

        except Exception as e:
            logger.error(f"Loop error: {e}", exc_info=True)
            await notify(tg.msg_error(str(e)))

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=SCAN_SEC)
        except asyncio.TimeoutError:
            pass

    # Cierre
    await client.close()
    await notify("🛑 <b>Maki Bot detenido.</b>")


if __name__ == "__main__":
    asyncio.run(main())
