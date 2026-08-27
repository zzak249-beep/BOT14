"""
Confirmación por RSI de doble cruce — traducción del script Pine
"ProBorsa: RSI & SuperTrend" (RSI + SMA de señal, contador de cruces
por debajo/encima de 50).

QUÉ HACE EL ORIGINAL: no es un cruce de RSI cualquiera. Cuenta cuántas
veces el RSI cruza por ENCIMA de su propia media móvil MIENTRAS sigue
por debajo de 50 (zona débil), y solo dispara la compra en el
`targetCrossCount`-ésimo cruce (2 por defecto) desde la última vez que
el RSI superó 50. Con 2, es literalmente un doble suelo (formación W)
visto en el RSI en vez de en el precio: el primer rebote no cuenta, el
segundo sí.

LO QUE SE AÑADE AQUÍ, QUE EL ORIGINAL NO TENÍA: el script solo opera
en largo (specialBuy / cierre por SuperTrend). Nuestro bot opera los
dos lados, así que aquí se añade el ESPEJO exacto para el lado corto:
un doble techo (formación M) — el RSI cruza por DEBAJO de su media
mientras sigue por encima de 50, dos veces desde la última vez que
bajó de 50. Es la misma lógica del autor, aplicada al lado que le
faltaba, no un concepto nuevo inventado aquí.

CÓMO SE USA: como CONFIRMACIÓN sobre la señal que ya genera
strategy.evaluate() — no sustituye la vela de agotamiento ni el resto
de filtros. Se reevalúa contra el MISMO array de velas de 5m que ya
usa la estrategia, sin llamadas a la API adicionales.
"""
from __future__ import annotations

from dataclasses import dataclass


def _rma(values: list[float], length: int) -> list[float]:
    """Wilder RMA — misma convención de siembra que ya usa strategy.atr()
    en este proyecto: seed con el primer valor, no con una SMA previa."""
    if not values:
        return []
    out = [values[0]]
    for v in values[1:]:
        out.append((out[-1] * (length - 1) + v) / length)
    return out


def rsi(closes: list[float], length: int) -> list[float]:
    if len(closes) < 2:
        return []
    ups = [0.0]
    downs = [0.0]
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i - 1]
        ups.append(max(ch, 0.0))
        downs.append(max(-ch, 0.0))
    up_rma = _rma(ups, length)
    down_rma = _rma(downs, length)
    out: list[float] = []
    for u, d in zip(up_rma, down_rma):
        if d == 0:
            out.append(100.0)
        elif u == 0:
            out.append(0.0)
        else:
            out.append(100.0 - 100.0 / (1.0 + u / d))
    return out


def sma(values: list[float], length: int) -> list[float | None]:
    out: list[float | None] = []
    for i in range(len(values)):
        if i + 1 < length:
            out.append(None)
        else:
            out.append(sum(values[i - length + 1 : i + 1]) / length)
    return out


@dataclass
class RsiConfirm:
    señal: str | None       # "BUY" | "SELL" | None — el bar ACTUAL (última vela cerrada)
    señal_reciente: str | None  # igual, pero permite hasta 'ventana' velas atrás
    rsi_actual: float
    velas_desde_señal: int  # cuántas velas cerradas han pasado desde el último cruce especial


def evaluate(
    candles: list[dict],
    length: int = 10,
    sig_length: int = 10,
    trigger: float = 50.0,
    target_count: int = 2,
    ventana: int = 3,
) -> RsiConfirm | None:
    """
    candles: mismo formato que usa strategy.evaluate() — se descarta la
    última vela (en curso), igual que allí, para no repintar.
    """
    need = length + sig_length + target_count + 5
    if len(candles) < need:
        return None
    c = candles[:-1]
    closes = [x["close"] for x in c]

    r = rsi(closes, length)
    s = sma(r, sig_length)
    n = len(r)
    if n < 2:
        return None

    bull_count = 0
    bear_count = 0
    historial: list[str | None] = [None] * n  # especial_buy/sell por barra

    for i in range(1, n):
        if s[i] is None or s[i - 1] is None:
            continue
        bull_cross = r[i - 1] <= s[i - 1] and r[i] > s[i]
        bear_cross = r[i - 1] >= s[i - 1] and r[i] < s[i]

        # Reseteo — igual que el script original: salir de la zona
        # débil (cruzar 50) borra el progreso del contador.
        if r[i] > trigger:
            bull_count = 0
        if r[i] < (100.0 - trigger):
            bear_count = 0

        if bull_cross and r[i] < trigger:
            bull_count += 1
            if bull_count == target_count:
                historial[i] = "BUY"
                bull_count = 0
        if bear_cross and r[i] > (100.0 - trigger):
            bear_count += 1
            if bear_count == target_count:
                historial[i] = "SELL"
                bear_count = 0

    señal_actual = historial[-1]

    señal_reciente = None
    velas_desde = 999
    for offset, valor in enumerate(reversed(historial[-ventana:])):
        if valor is not None:
            señal_reciente = valor
            velas_desde = offset
            break

    return RsiConfirm(
        señal=señal_actual,
        señal_reciente=señal_reciente,
        rsi_actual=r[-1],
        velas_desde_señal=velas_desde,
    )


def confirms(signal_side: str, rsi_result: RsiConfirm | None) -> bool:
    """¿La señal reciente del RSI confirma la dirección de la operación?"""
    if rsi_result is None or rsi_result.señal_reciente is None:
        return False
    return rsi_result.señal_reciente == signal_side
