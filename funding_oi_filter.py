"""
Funding Rate + Open Interest — Filtro de confluencia
=======================================================
Idea: un funding rate extremo indica posicionamiento sobrecargado en un
lado del mercado. Si el Unicorn Model detecta un sweep que va a liquidar
ese lado sobrecargado, la señal tiene más "combustible" real detrás.

- Funding muy POSITIVO (longs pagan a shorts) → exceso de longs apalancados
  → un sweep BAJISTA que los liquide es más probable que continúe (SHORT confirma)
- Funding muy NEGATIVO → exceso de shorts → sweep ALCISTA (LONG confirma)

Open Interest: un salto de OI justo antes/durante el sweep indica que
entraron posiciones nuevas que luego quedan atrapadas y se liquidan —
"combustible" adicional para el movimiento post-sweep.
"""
import logging

log = logging.getLogger("funding_oi_filter")


def evaluate_funding_bias(funding_rate, direction, config):
    """
    funding_rate: float (ej. 0.0003 = 0.03%)
    direction: "LONG" o "SHORT" (dirección de la señal ya confirmada)
    Devuelve {"confirms": bool, "funding_rate": float, "reason": str}
    """
    threshold = getattr(config, "FUNDING_EXTREME_THRESHOLD", 0.0005)

    if funding_rate is None:
        return {"confirms": None, "funding_rate": None, "reason": "sin_dato_funding"}

    if direction == "SHORT":
        # Funding muy positivo = longs sobrecargados = confirma short
        confirms = funding_rate >= threshold
        reason = (f"funding={funding_rate:.5f} >= {threshold} (longs sobrecargados)" if confirms
                   else f"funding={funding_rate:.5f} no muestra exceso de longs")
    else:
        confirms = funding_rate <= -threshold
        reason = (f"funding={funding_rate:.5f} <= -{threshold} (shorts sobrecargados)" if confirms
                   else f"funding={funding_rate:.5f} no muestra exceso de shorts")

    return {"confirms": confirms, "funding_rate": funding_rate, "reason": reason}


def evaluate_oi_confirmation(oi_before, oi_after, config):
    """
    Compara OI antes/después del sweep. Un salto relevante indica que
    entraron posiciones nuevas que alimentan el movimiento.
    Devuelve {"confirms": bool|None, "oi_change_pct": float|None, "reason": str}
    """
    min_change_pct = getattr(config, "OI_MIN_CHANGE_PCT", 1.0)

    if oi_before is None or oi_after is None or oi_before <= 0:
        return {"confirms": None, "oi_change_pct": None, "reason": "sin_dato_oi"}

    change_pct = (oi_after - oi_before) / oi_before * 100
    confirms = abs(change_pct) >= min_change_pct
    reason = (f"OI cambió {change_pct:+.2f}% (>= {min_change_pct}%)" if confirms
               else f"OI cambió solo {change_pct:+.2f}%, sin combustible adicional")

    return {"confirms": confirms, "oi_change_pct": change_pct, "reason": reason}


async def confirm_with_funding_oi(client, symbol, combined_signal, config):
    """
    Confirmación final async. Igual que order_flow: solo se consulta la API
    cuando ya hay señal válida (Supertrend + Unicorn + Order Flow si aplica).
    Requiere que config.FUNDING_OI_MODE sea 'confirm' (rechaza si no confirma)
    o 'inform' (solo informa, no rechaza) — por defecto 'inform' para no ser
    demasiado restrictivo hasta validar con datos reales.
    """
    if combined_signal.get("signal") is None:
        return combined_signal

    if not getattr(config, "ENABLE_FUNDING_OI_FILTER", False):
        combined_signal["funding_oi"] = {"skipped": True}
        return combined_signal

    mode = getattr(config, "FUNDING_OI_MODE", "inform")

    try:
        funding_rate = await client.get_funding_rate(symbol)
        oi_now = await client.get_open_interest(symbol)
    except Exception as e:
        log.warning("[%s] No se pudo obtener funding/OI: %s", symbol, e)
        combined_signal["funding_oi"] = {"confirms": None, "reason": f"error_api: {e}"}
        return combined_signal

    funding_eval = evaluate_funding_bias(funding_rate, combined_signal["signal"], config)
    combined_signal["funding_oi"] = {**funding_eval, "open_interest": oi_now}

    if mode == "confirm" and funding_eval["confirms"] is False:
        log.info("[%s] Señal %s descartada por funding: %s",
                  symbol, combined_signal["signal"], funding_eval["reason"])
        combined_signal["signal"] = None
        combined_signal["reason"] = f"funding_rejected: {funding_eval['reason']}"
    else:
        log.info("[%s] Funding/OI info: %s", symbol, funding_eval["reason"])

    return combined_signal
