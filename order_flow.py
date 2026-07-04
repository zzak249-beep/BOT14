"""
Order Flow / Absorption Filter
=================================
La mayoría de bots que replican ICT/SMC solo miran velas (OHLCV). Este
filtro añade una capa que casi nadie usa en bots retail: valida el sweep
con TRADES REALES (agresión compradora/vendedora), no solo con la mecha.

Idea central: un sweep de liquidez "genuino" (institucional) debería
mostrar ABSORCIÓN — volumen agresivo en la dirección de la mecha que es
"tragado" sin que el precio siga ese lado, porque hay un jugador grande
absorbiendo esa liquidez para reversar el precio.

Ejemplo (sweep de un mínimo, buscando LONG):
  - Se espera ver volumen vendedor agresivo alto durante la mecha hacia abajo
    (gente vendiendo / stops de largos disparando)
  - Pero el precio no sigue cayendo — cierra arriba del nivel
  - Eso indica que alguien absorbió esa venta con compras → absorción real

Si en cambio el sweep tiene volumen bajo o el ratio comprador/vendedor no
muestra absorción, es más probable que sea ruido / mecha random de un
par ilíquido, no un sweep institucional real.
"""
import logging

log = logging.getLogger("order_flow")

_TF_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
    "30m": 1_800_000, "1h": 3_600_000, "1H": 3_600_000,
    "4h": 14_400_000, "4H": 14_400_000, "1d": 86_400_000, "1D": 86_400_000,
}


def _interval_ms(tf_label):
    return _TF_MS.get(tf_label, _TF_MS.get(tf_label.lower(), 180_000))


def compute_volume_ratio(trades, start_ms, end_ms):
    """
    Calcula el ratio de volumen comprador vs vendedor dentro de una ventana
    de tiempo, usando trades reales.
    Devuelve {"buy_vol": float, "sell_vol": float, "buy_ratio": float, "total_vol": float}
    """
    buy_vol = 0.0
    sell_vol = 0.0
    for t in trades:
        if start_ms <= t["time"] <= end_ms:
            # is_buyer_maker=True → el taker fue vendedor (presión de venta)
            if t["is_buyer_maker"]:
                sell_vol += t["qty"]
            else:
                buy_vol += t["qty"]
    total = buy_vol + sell_vol
    buy_ratio = buy_vol / total if total > 0 else 0.5
    return {"buy_vol": buy_vol, "sell_vol": sell_vol, "buy_ratio": buy_ratio, "total_vol": total}


def evaluate_absorption(trades, sweep_candle_time, entry_tf_label, direction, config):
    """
    Evalúa si hubo absorción real durante la vela de sweep.
    direction: "LONG" o "SHORT" (dirección de la señal ya confirmada)

    Devuelve {"confirms": bool, "buy_ratio": float, "total_vol": float, "reason": str}
    """
    min_vol = getattr(config, "ORDER_FLOW_MIN_VOLUME", 0)
    min_ratio = getattr(config, "ORDER_FLOW_MIN_ABSORPTION_RATIO", 0.55)

    interval = _interval_ms(entry_tf_label)
    start_ms = sweep_candle_time
    end_ms = sweep_candle_time + interval

    if not trades:
        return {"confirms": False, "buy_ratio": None, "total_vol": 0.0,
                "reason": "sin_trades_disponibles"}

    stats = compute_volume_ratio(trades, start_ms, end_ms)

    if stats["total_vol"] < min_vol:
        return {"confirms": False, **stats,
                "reason": f"volumen_insuficiente ({stats['total_vol']:.2f} < {min_vol})"}

    if direction == "LONG":
        # Para LONG: absorción = compradores dominaron pese al sweep bajista
        confirms = stats["buy_ratio"] >= min_ratio
        reason = (f"buy_ratio={stats['buy_ratio']:.2f} >= {min_ratio}" if confirms
                   else f"sin absorción compradora clara (buy_ratio={stats['buy_ratio']:.2f})")
    else:
        sell_ratio = 1 - stats["buy_ratio"]
        confirms = sell_ratio >= min_ratio
        reason = (f"sell_ratio={sell_ratio:.2f} >= {min_ratio}" if confirms
                   else f"sin absorción vendedora clara (sell_ratio={sell_ratio:.2f})")

    return {"confirms": confirms, **stats, "reason": reason}


async def confirm_with_order_flow(client, symbol, combined_signal, config):
    """
    Punto de entrada async: llama a la API solo si ya hay señal combinada
    válida (Supertrend + Unicorn alineados), para no gastar rate limit
    pidiendo trades de los 500+ símbolos en cada ciclo de scan.
    """
    if combined_signal.get("signal") is None:
        return combined_signal

    if not getattr(config, "ENABLE_ORDER_FLOW_FILTER", False):
        combined_signal["order_flow"] = {"skipped": True}
        return combined_signal

    uni = combined_signal.get("unicorn") or {}
    sweep_time = uni.get("sweep_candle_time")
    if sweep_time is None:
        # unicorn_model no expone el timestamp del sweep por defecto —
        # si no está disponible, no podemos validar order flow con precisión
        combined_signal["order_flow"] = {"confirms": None, "reason": "sin_timestamp_de_sweep"}
        return combined_signal

    try:
        trades = await client.get_recent_trades(symbol, limit=getattr(config, "ORDER_FLOW_TRADES_LIMIT", 1000))
    except Exception as e:
        log.warning("[%s] No se pudo obtener trades para order flow: %s", symbol, e)
        combined_signal["order_flow"] = {"confirms": None, "reason": f"error_api: {e}"}
        return combined_signal

    of_result = evaluate_absorption(
        trades, sweep_time, config.ENTRY_TF, combined_signal["signal"], config,
    )
    combined_signal["order_flow"] = of_result

    if not of_result["confirms"]:
        log.info("[%s] Señal %s descartada por order flow: %s",
                  symbol, combined_signal["signal"], of_result["reason"])
        combined_signal["signal"] = None
        combined_signal["reason"] = f"order_flow_rejected: {of_result['reason']}"
    else:
        log.info("[%s] Order flow CONFIRMA %s: %s",
                  symbol, combined_signal["signal"], of_result["reason"])

    return combined_signal
