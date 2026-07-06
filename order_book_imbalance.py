"""
Order Book Imbalance (OBI) — confirmación de microestructura, libro de órdenes en vivo
==========================================================================================
A diferencia de TODO lo demás en este bot (Unicorn Model, Order Block Engine, CVD,
Order Flow, Funding/OI) — que miran velas cerradas o trades ya ejecutados — OBI mira
el LIBRO DE ÓRDENES ahora mismo: cuánto volumen hay parado en bids vs asks en los
primeros N niveles de profundidad. Es una fuente de datos completamente distinta.

Fundamento (no es intuición, hay research real detrás):
  "Explainable Patterns in Cryptocurrency Microstructure" (arXiv 2602.00776, ene 2026):
  order flow imbalance es predictivo de la dirección del precio en horizontes de
  segundos a ~1 minuto, con importancia de feature estable across BTC, LTC, ETC, ENJ,
  ROSE (de gran a pequeña capitalización) usando un pipeline CatBoost + SHAP sobre
  order books de Binance Futures. Validado con backtest taker conservador.

Por qué se usa como CONFIRMACIÓN FINAL y no como motor de entrada:
  El horizonte predictivo (segundos a un minuto) es mucho más corto que nuestros
  timeframes de entrada (3m/15m). No tiene sentido generar la señal a partir de OBI
  sola — pero SÍ tiene sentido, en el momento exacto de ejecutar una señal que ya
  vino de Unicorn Model o el Order Block Engine, chequear que el libro de órdenes
  EN ESE INSTANTE todavía apoya la dirección — es la comprobación más "fresca" y de
  más baja latencia que tenemos disponible, más rápida que esperar el cierre de la
  siguiente vela.

Riesgo conocido (documentado en el mismo paper, no lo escondemos): si muchos
participantes usan la misma señal, se vuelve reflexiva — puede amplificar cascadas
de liquidación en vez de solo predecirlas. No es una razón para no usarla, pero sí
para no operarla con tamaño excesivo ni asumirla infalible.

INCERTIDUMBRE DE ENDPOINT (igual honestidad que el resto de exchange_client.py):
el endpoint REST de profundidad de BingX no se confirmó contra tráfico real todavía
— ver la nota en exchange_client.py.get_order_book().
"""
import logging

log = logging.getLogger("order_book_imbalance")


def compute_obi(order_book, levels=20):
    """
    order_book: {"bids": [[price, qty], ...], "asks": [[price, qty], ...]}
    levels: cuántos niveles de profundidad considerar desde el top del libro.

    Devuelve {"obi": float (-1 a 1), "bullish": bool, "bearish": bool,
              "bid_vol": float, "ask_vol": float}
    obi > 0 -> más volumen comprador parado (presión de compra)
    obi < 0 -> más volumen vendedor parado (presión de venta)
    """
    if not order_book:
        return {"obi": 0.0, "bullish": False, "bearish": False, "bid_vol": 0.0, "ask_vol": 0.0}

    bids = (order_book.get("bids") or [])[:levels]
    asks = (order_book.get("asks") or [])[:levels]

    bid_vol = sum(float(q) for _, q in bids)
    ask_vol = sum(float(q) for _, q in asks)
    total = bid_vol + ask_vol

    if total <= 0:
        return {"obi": 0.0, "bullish": False, "bearish": False, "bid_vol": bid_vol, "ask_vol": ask_vol}

    obi = (bid_vol - ask_vol) / total
    return {"obi": obi, "bullish": obi > 0, "bearish": obi < 0, "bid_vol": bid_vol, "ask_vol": ask_vol}


def confirms_direction(order_book, direction, config):
    """
    direction: "LONG" o "SHORT" (la dirección ya propuesta por otro motor).
    Devuelve {"confirms": bool|None, "obi": float, "reason": str}
    """
    threshold = getattr(config, "OBI_THRESHOLD", 0.15)
    levels = getattr(config, "OBI_LEVELS", 20)

    if not order_book:
        return {"confirms": None, "obi": 0.0, "reason": "sin_order_book"}

    result = compute_obi(order_book, levels)

    if direction == "LONG":
        confirms = result["obi"] >= threshold
        reason = (f"OBI={result['obi']:.3f} >= {threshold} (presión compradora)" if confirms
                   else f"OBI={result['obi']:.3f} < {threshold} (sin presión compradora suficiente)")
    else:
        confirms = result["obi"] <= -threshold
        reason = (f"OBI={result['obi']:.3f} <= -{threshold} (presión vendedora)" if confirms
                   else f"OBI={result['obi']:.3f} > -{threshold} (sin presión vendedora suficiente)")

    return {"confirms": confirms, "obi": result["obi"], "reason": reason}
