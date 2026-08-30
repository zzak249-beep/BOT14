"""
bot.py
------
Punto de entrada del bot. Cada vez que cierra una vela de 15 minutos (o el
timeframe configurado):
  1. Descarga las últimas velas de BingX (con reintentos ante fallos de red).
  2. Calcula la estrategia (strategy.compute_signals) — lógica idéntica al
     Pine Script original, ya verificada.
  3. Si NO hay posición abierta y aparece la señal de compra -> comprueba
     saldo/mínimos, compra a mercado en BingX y avisa por Telegram.
  4. Si HAY una posición abierta y el SuperTrend gira a bajista (o salta el
     stop-loss opcional) -> cierra la posición a mercado y avisa.

Novedades sobre la v1:
  - Reintentos con backoff ante errores de red transitorios (exchange_client.py).
  - Guarda de idempotencia: no vuelve a procesar la misma vela dos veces
    (protege sobre todo contra avisos duplicados en DRY_RUN tras un reinicio;
    la protección financiera real ya la daba comprobar el balance real).
  - Comprobación previa de saldo y de mínimos del mercado antes de comprar.
  - Stop-loss opcional (STOP_LOSS_PCT, desactivado por defecto = mismo
    comportamiento que el Pine original).
  - Heartbeat periódico opcional ("sigo vivo") por Telegram.
  - Comandos remotos por Telegram: /status /pause /resume /close /help.

IMPORTANTE - LEE ESTO ANTES DE OPERAR EN REAL:
  - Por defecto el bot arranca en DRY_RUN=true: analiza y avisa por
    Telegram pero NO envía ninguna orden real. Cambia DRY_RUN=false (y
    revisa BINGX_DEMO) solo cuando hayas verificado que las señales y los
    mensajes se comportan como esperas.
  - Esto no es un consejo financiero. Operar de forma automática conlleva
    riesgo real de pérdida de capital.
"""

import json
import logging
import os
import signal
import time

from config import load_settings
from exchange_client import ExchangeClient
from notifier import TelegramNotifier
from strategy import compute_signals
from telegram_commands import BotControlState, CommandListener

STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "state.json")

logger = logging.getLogger("bot")


class GracefulExit(Exception):
    pass


def _handle_sigterm(signum, frame):
    raise GracefulExit()


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def timeframe_to_seconds(tf: str) -> int:
    units = {"m": 60, "h": 3600, "d": 86400, "w": 604800}
    unit = tf[-1]
    if unit not in units:
        raise ValueError(f"Timeframe no soportado: {tf!r}")
    return int(tf[:-1]) * units[unit]


def seconds_until_next_close(tf_seconds: int, buffer_seconds: int) -> float:
    now = time.time()
    next_close = (int(now // tf_seconds) + 1) * tf_seconds
    return (next_close - now) + buffer_seconds


def load_state() -> dict:
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state: dict) -> None:
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as exc:
        logger.warning(f"No se pudo guardar state.json (no crítico): {exc}")


def drop_unclosed_candle(df, tf_seconds: int):
    """BingX puede devolver la vela en curso como última fila. La descartamos
    si todavía no ha cerrado, para no operar con datos incompletos."""
    now_ms = int(time.time() * 1000)
    if len(df) == 0:
        return df
    last_ts = int(df["timestamp"].iloc[-1])
    if last_ts + tf_seconds * 1000 > now_ms:
        return df.iloc[:-1].reset_index(drop=True)
    return df


def extract_fill_price(order: dict, fallback: float) -> float:
    for key in ("average", "price"):
        val = order.get(key)
        if val:
            return float(val)
    return fallback


def format_pct(value: float) -> str:
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.2f}%"


def get_known_entry_price(exchange, settings, state):
    """Precio de entrada: primero desde el estado local; si no está
    disponible (p. ej. tras un redeploy en Railway), intenta reconstruirlo
    desde el historial real de operaciones en BingX."""
    entry_price = state.get("entry_price")
    if entry_price:
        return float(entry_price)
    if settings.dry_run:
        return None
    estimated = exchange.estimate_entry_price(settings.symbol)
    if estimated:
        logger.info(f"Precio de entrada recuperado desde el historial de BingX: {estimated}")
    return estimated


def handle_buy_signal(exchange, notifier, settings, state, last_row, candle_time):
    header = "🟢 <b>SEÑAL DE COMPRA</b> (Doble Suelo RSI + SuperTrend)"
    lines = [
        header,
        f"Par: <b>{settings.symbol}</b> ({settings.timeframe})",
        f"Vela: {candle_time.strftime('%Y-%m-%d %H:%M UTC')}",
        f"Precio de cierre: {last_row['close']:.6f}",
        f"RSI: {last_row['rsi']:.2f}",
    ]

    if settings.dry_run:
        lines.append("")
        lines.append("⚙️ <i>DRY_RUN activo: no se ha enviado ninguna orden real a BingX.</i>")
        notifier.send("\n".join(lines))
        logger.info("Señal de compra (DRY_RUN, sin orden real).")
        return

    problem = exchange.check_buy_preconditions(settings.symbol, settings.trade_amount_usdt)
    if problem:
        logger.warning(f"Compra bloqueada por comprobación previa: {problem}")
        notifier.send(f"⚠️ Señal de compra detectada, pero NO se ha enviado la orden:\n{problem}")
        return

    try:
        lines.insert(3, f"Importe a invertir: {settings.trade_amount_usdt} USDT")
        order = exchange.market_buy_with_cost(settings.symbol, settings.trade_amount_usdt)
        fill_price = extract_fill_price(order, fallback=float(last_row["close"]))
        state["entry_price"] = fill_price
        state["entry_time"] = candle_time.isoformat()
        save_state(state)
        lines.append("")
        lines.append(f"✅ Orden ejecutada en BingX (precio aprox: {fill_price:.6f})")
        notifier.send("\n".join(lines))
        logger.info(f"Compra ejecutada a ~{fill_price}")
    except Exception as exc:
        logger.exception("Fallo al ejecutar la orden de compra")
        notifier.send(f"❌ Señal de compra detectada pero la orden FALLÓ:\n<code>{exc}</code>")


def handle_sell_signal(exchange, notifier, settings, state, last_row, candle_time, reason="SuperTrend"):
    icon = "🛑" if reason == "Stop-Loss" else "🔴"
    header = f"{icon} <b>SEÑAL DE VENTA</b> ({reason})"
    lines = [
        header,
        f"Par: <b>{settings.symbol}</b> ({settings.timeframe})",
        f"Vela: {candle_time.strftime('%Y-%m-%d %H:%M UTC')}",
        f"Precio de cierre: {last_row['close']:.6f}",
    ]

    entry_price = get_known_entry_price(exchange, settings, state)
    if entry_price:
        pnl_pct = (float(last_row["close"]) / float(entry_price) - 1) * 100
        lines.append(f"Precio de entrada: {float(entry_price):.6f}")
        lines.append(f"Resultado estimado: {format_pct(pnl_pct)}")

    if settings.dry_run:
        lines.append("")
        lines.append("⚙️ <i>DRY_RUN activo: no se ha enviado ninguna orden real a BingX.</i>")
        notifier.send("\n".join(lines))
        logger.info("Señal de venta (DRY_RUN, sin orden real).")
        return

    try:
        order = exchange.market_sell_all(settings.symbol)
        fill_price = extract_fill_price(order, fallback=float(last_row["close"]))
        state["entry_price"] = None
        state["entry_time"] = None
        save_state(state)
        lines.append("")
        lines.append(f"✅ Posición cerrada en BingX (precio aprox: {fill_price:.6f})")
        notifier.send("\n".join(lines))
        logger.info(f"Venta ejecutada a ~{fill_price} (motivo: {reason})")
    except Exception as exc:
        logger.exception("Fallo al ejecutar la orden de venta")
        notifier.send(f"❌ Señal de venta detectada pero la orden FALLÓ:\n<code>{exc}</code>")


def maybe_send_heartbeat(notifier, settings, runtime, snapshot) -> None:
    if settings.heartbeat_every_hours <= 0:
        return
    now = time.time()
    last = runtime.get("last_heartbeat")
    if last is not None and (now - last) < settings.heartbeat_every_hours * 3600:
        return
    runtime["last_heartbeat"] = now
    notifier.send(
        "💓 <b>Sigo activo</b>\n"
        f"Par: {settings.symbol} ({settings.timeframe})\n"
        f"Último precio: {snapshot.get('close', '—')}\n"
        f"En posición: {'sí' if snapshot.get('in_position') else 'no'}"
    )


def run_analysis_cycle(exchange: ExchangeClient, notifier: TelegramNotifier, settings, state: dict,
                        control: BotControlState, runtime: dict) -> None:
    tf_seconds = timeframe_to_seconds(settings.timeframe)

    df = exchange.fetch_ohlcv_df(settings.symbol, settings.timeframe, settings.candles_lookback)
    df = drop_unclosed_candle(df, tf_seconds)

    min_bars = max(settings.rsi_length + settings.signal_length, settings.atr_period) + 10
    if len(df) < min_bars:
        logger.warning(f"Solo hay {len(df)} velas cerradas disponibles (se necesitan >= {min_bars}). Esperando más historial.")
        return

    last_ts = int(df["timestamp"].iloc[-1])
    if state.get("last_processed_ts") == last_ts:
        logger.info("Esta vela ya se procesó en un ciclo anterior (posible reinicio). Se omite para no duplicar avisos.")
        return

    signals = compute_signals(
        df,
        rsi_length=settings.rsi_length,
        signal_length=settings.signal_length,
        trigger_level=settings.trigger_level,
        target_cross_count=settings.target_cross_count,
        atr_period=settings.atr_period,
        st_factor=settings.st_factor,
    )
    last = signals.iloc[-1]
    candle_time = df["datetime"].iloc[-1]

    position_value = exchange.get_position_value_usdt(settings.symbol)
    in_position = position_value >= settings.min_position_value_usdt

    logger.info(
        f"[{candle_time}] close={last['close']:.4f} rsi={last['rsi']:.2f} "
        f"cross_count={int(last['cross_count'])} tendencia_alcista={last['trend_up']} "
        f"en_posicion={in_position} (~{position_value:.2f} USDT) pausado={control.is_paused()}"
    )

    control.update_snapshot(
        candle_time=candle_time.strftime("%Y-%m-%d %H:%M UTC"),
        close=round(float(last["close"]), 6),
        rsi=round(float(last["rsi"]), 2),
        in_position=in_position,
        position_value=position_value,
    )

    # --- Stop-loss opcional (desactivado por defecto) ---
    stop_loss_hit = False
    if in_position and settings.stop_loss_pct > 0:
        entry_price = get_known_entry_price(exchange, settings, state)
        if entry_price:
            drop_pct = (float(entry_price) - float(last["close"])) / float(entry_price) * 100
            if drop_pct >= settings.stop_loss_pct:
                stop_loss_hit = True
                logger.warning(f"Stop-loss alcanzado: caída de {drop_pct:.2f}% desde la entrada ({entry_price}).")

    buy_triggered = (not in_position) and bool(last["special_buy"])
    sell_triggered = in_position and (bool(last["sell_signal"]) or stop_loss_hit)

    if control.is_paused() and (buy_triggered or sell_triggered):
        notifier.send(
            "⏸️ Se detectó una señal pero el bot está en <b>pausa</b> (usa /resume para reanudar). "
            "No se ha realizado ninguna acción."
        )
    elif buy_triggered:
        handle_buy_signal(exchange, notifier, settings, state, last, candle_time)
    elif sell_triggered:
        handle_sell_signal(exchange, notifier, settings, state, last, candle_time,
                            reason="Stop-Loss" if stop_loss_hit else "SuperTrend")

    maybe_send_heartbeat(notifier, settings, runtime, control.get_snapshot())

    state["last_processed_ts"] = last_ts
    save_state(state)


def build_startup_message(settings) -> str:
    modo = "🧪 DEMO (BingX VST)" if settings.bingx_demo else "💰 REAL"
    orden = "🟡 DRY_RUN (solo aviso, sin órdenes)" if settings.dry_run else "🟢 ÓRDENES REALES ACTIVAS"
    sl = f"{settings.stop_loss_pct}%" if settings.stop_loss_pct > 0 else "desactivado"
    hb = f"cada {settings.heartbeat_every_hours}h" if settings.heartbeat_every_hours > 0 else "desactivado"
    return (
        "🤖 <b>Bot RSI + SuperTrend iniciado</b>\n"
        f"Par: <b>{settings.symbol}</b>\n"
        f"Temporalidad: {settings.timeframe}\n"
        f"Modo cuenta BingX: {modo}\n"
        f"Modo órdenes: {orden}\n"
        f"Importe por operación: {settings.trade_amount_usdt} USDT\n"
        f"Stop-loss: {sl} | Heartbeat: {hb}\n"
        f"RSI: longitud {settings.rsi_length}, señal {settings.signal_length}, "
        f"disparo {settings.trigger_level}, cruces objetivo {settings.target_cross_count}\n"
        f"SuperTrend: ATR {settings.atr_period}, factor {settings.st_factor}\n"
        f"Comandos remotos: {'activados (/help)' if settings.telegram_commands_enabled else 'desactivados'}"
    )


def main() -> None:
    settings = load_settings()
    setup_logging(settings.log_level)
    signal.signal(signal.SIGTERM, _handle_sigterm)

    logger.info("Iniciando bot...")
    exchange = ExchangeClient(
        settings.bingx_api_key, settings.bingx_api_secret, demo=settings.bingx_demo,
        max_retries=settings.max_retries, retry_backoff_seconds=settings.retry_backoff_seconds,
    )
    notifier = TelegramNotifier(settings.telegram_bot_token, settings.telegram_chat_id)
    state = load_state()
    control = BotControlState()
    runtime = {}

    notifier.send(build_startup_message(settings))

    listener = None
    if settings.telegram_commands_enabled:
        listener = CommandListener(
            settings.telegram_bot_token, settings.telegram_chat_id, control,
            exchange, settings, notifier, state, save_state,
        )
        listener.start()

    tf_seconds = timeframe_to_seconds(settings.timeframe)

    # Análisis inmediato al arrancar, para tener feedback sin esperar al cierre de vela.
    try:
        run_analysis_cycle(exchange, notifier, settings, state, control, runtime)
    except Exception:
        logger.exception("Error en el análisis inicial")

    try:
        while True:
            sleep_s = seconds_until_next_close(tf_seconds, settings.poll_buffer_seconds)
            logger.info(f"Esperando {sleep_s:.0f}s hasta el cierre de la próxima vela de {settings.timeframe}...")
            time.sleep(max(sleep_s, 1))
            try:
                run_analysis_cycle(exchange, notifier, settings, state, control, runtime)
            except Exception as exc:
                logger.exception("Error durante el ciclo de análisis")
                notifier.send_throttled(
                    f"⚠️ Error en el bot durante el análisis:\n<code>{exc}</code>",
                    key="cycle_error",
                    cooldown_minutes=settings.error_notify_cooldown_minutes,
                )
                time.sleep(30)
    except (KeyboardInterrupt, GracefulExit):
        logger.info("Apagando el bot...")
        if listener:
            listener.stop()
        notifier.send("🛑 Bot detenido.")


if __name__ == "__main__":
    main()
