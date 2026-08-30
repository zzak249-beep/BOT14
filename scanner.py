"""
Escáner del universo completo de BingX.

Sustituye el trabajo manual de elegir diez símbolos a mano en el radar
de TradingView: recorre TODOS los perpetuos, mide amplitud y estado, y
publica un ranking.

Sobre el límite de peticiones: mil símbolos son mil llamadas. Se lanzan
con un semáforo y una pausa entre tandas. Si BingX empieza a devolver
429, baja CONCURRENCY antes que subir el intervalo — es la palanca que
no te cuesta frescura en los datos.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

import config
import strategy
import liquidations
from bingx import BingX

log = logging.getLogger("scanner")


@dataclass
class Row:
    symbol: str
    atr_pct: float
    cover: float
    er_short: float
    er_long: float
    stretch: float
    state: str      # "en rango" | "estirado" | "fuera"
    verdict: str     # "LISTA" | "estirado" | "RUPTURA" | "vertical" | "en espera" | "sin amplitud"
    signal: "strategy.Signal | None" = None  # solo si verdict == "LISTA"


def efficiency_ratio(closes: list[float], length: int) -> float:
    """Ratio de Kaufman: recorrido neto entre suma de movimientos."""
    if len(closes) < length + 1:
        return 0.0
    net = abs(closes[-1] - closes[-1 - length])
    total = sum(abs(closes[i] - closes[i - 1]) for i in range(len(closes) - length, len(closes)))
    return net / total if total > 0 else 0.0


def analyse(symbol: str, candles: list[dict]) -> Row | None:
    if len(candles) < 200:
        return None
    c = candles[:-1]  # la última está en curso
    closes = [x["close"] for x in c]
    highs = [x["high"] for x in c]
    lows = [x["low"] for x in c]

    ma = strategy.ema(closes, config.MA_LEN)
    a = strategy.atr(highs, lows, closes, config.ATR_LEN)
    if not ma or not a or a[-1] <= 0 or closes[-1] <= 0:
        return None

    atr_pct = a[-1] / closes[-1] * 100.0
    cover = atr_pct / config.COST_ROUNDTRIP_PCT if config.COST_ROUNDTRIP_PCT > 0 else 0.0
    stretch = (closes[-1] - ma[-1]) / a[-1]

    er_s = efficiency_ratio(closes, config.ER_SHORT)
    er_l = efficiency_ratio(closes, config.ER_LONG)

    hi = max(highs[-config.RANGE_LEN - 1 : -1])
    lo = min(lows[-config.RANGE_LEN - 1 : -1])
    if closes[-1] > hi or closes[-1] < lo:
        state = "fuera"
    elif abs(stretch) >= config.STRETCH_ATR:
        state = "estirado"
    else:
        state = "en rango"

    # ANTES el ranking tenía su PROPIO criterio de "REVERSIÓN" (solo
    # amplitud + estirón), distinto del que usa strategy.evaluate() para
    # disparar de verdad (que además exige que no sea vertical, que haya
    # cerrado la vela de agotamiento y que el R:R alcance el mínimo). Un
    # símbolo podía salir marcado "REVERSIÓN" en el ranking y no generar
    # jamás una señal — o al revés. Ahora se llama a strategy.evaluate()
    # directamente: el ranking usa el MISMO criterio que abre la
    # operación, ni más laxo ni más estricto.
    sig, motivo = strategy.evaluate(symbol, candles)

    if sig is not None:
        verdict = "LISTA"  # cumple TODO el patrón ahora mismo, incluida la vela de agotamiento
    elif atr_pct < config.MIN_ATR_PCT or cover < config.MIN_COST_COVER:
        verdict = "sin amplitud"
    elif motivo.startswith("vertical"):
        verdict = "vertical"
    elif motivo == "estirado, sin vela de agotamiento" or motivo.startswith("R:R insuficiente"):
        verdict = "estirado"  # candidata cercana: falta la vela o el R:R no llega
    elif state == "fuera" and er_l >= config.ER_TREND:
        verdict = "RUPTURA"  # informativo: la estrategia no opera rupturas
    else:
        verdict = "en espera"

    return Row(symbol, atr_pct, cover, er_s, er_l, stretch, state, verdict, sig)


def bias_from_row(r: Row) -> str:
    """
    Sesgo direccional de un símbolo en el timeframe de este Row —
    pensado para usarse con un Scanner en 30m como filtro de
    contra-tendencia sobre las señales de 5m.

    Solo se marca sesgo cuando hay una RUPTURA de verdad (fuera de
    rango + ER de tendencia alto) — el mismo criterio que ya usa
    scanner.analyse() para RUPTURA, no uno nuevo. Sin eso, "estirado"
    o "en rango" en 30m no dicen nada sobre la tendencia de fondo,
    así que se devuelve NEUTRAL y no se bloquea nada: es mejor dejar
    pasar una señal con sesgo desconocido que bloquear todo por falta
    de dato.
    """
    if r.verdict == "RUPTURA":
        return "ALCISTA" if r.stretch > 0 else "BAJISTA"
    return "NEUTRAL"


class Scanner:
    def __init__(self, api: BingX, timeframe: str | None = None) -> None:
        self.api = api
        self.timeframe = timeframe or config.TIMEFRAME
        self.sem = asyncio.Semaphore(config.SCAN_CONCURRENCY)
        self.last_run = 0.0

    async def _one(self, symbol: str) -> Row | None:
        async with self.sem:
            try:
                candles = await self.api.klines(symbol, self.timeframe, limit=300)
                return analyse(symbol, candles)
            except Exception as exc:  # noqa: BLE001
                log.debug("%s: %s", symbol, exc)
                return None

    async def run(self, symbols: list[str]) -> list[Row]:
        t0 = time.time()
        results = await asyncio.gather(*(self._one(s) for s in symbols))
        rows = [r for r in results if r is not None]
        rows.sort(key=lambda r: r.cover, reverse=True)
        self.last_run = time.time()
        log.info(
            "Escaneo (%s): %d/%d símbolos analizados en %.0fs · %d con amplitud",
            self.timeframe,
            len(rows),
            len(symbols),
            time.time() - t0,
            sum(1 for r in rows if r.verdict != "sin amplitud"),
        )
        return rows


def format_ranking(rows: list[Row], top: int) -> str:
    con_amplitud = [r for r in rows if r.verdict != "sin amplitud"]
    if not con_amplitud:
        return (
            f"📡 <b>Escaneo BingX</b>\n"
            f"{len(rows)} símbolos analizados.\n"
            f"<b>Ninguno con amplitud suficiente</b> (≥{config.MIN_ATR_PCT}% y "
            f"≥{config.MIN_COST_COVER:.0f}× el coste).\n"
            f"Mercado tranquilo: no es un fallo, es que no hay nada que operar."
        )

    lineas = [f"📡 <b>Escaneo BingX</b> — {len(rows)} símbolos, {len(con_amplitud)} con amplitud\n"]
    marcas = {"LISTA": "🏆", "estirado": "🔶", "RUPTURA": "🟩", "vertical": "▫"}
    for r in con_amplitud[:top]:
        marca = marcas.get(r.verdict, "·")
        base = r.symbol.split("-")[0]
        lineas.append(
            f"{marca} <b>{base}</b>  {r.atr_pct:.2f}% ({r.cover:.0f}×)  "
            f"ER {r.er_short:.2f}/{r.er_long:.2f}  {r.state} {r.stretch:+.1f}"
        )
    lineas.append(
        "\n🏆 lista para operar YA · 🔶 estirada, falta vela · 🟩 ruptura (informativo) "
        "· ▫ vertical, descartada · · con amplitud, en espera"
    )
    return "\n".join(lineas)


def format_favorites(rows: list[Row], top: int, cascade_lookup=None) -> str | None:
    """
    El mensaje que responde a "¿cuáles son las favoritas de este
    escaneo?". Distinto del ranking: el ranking ordena por amplitud
    aunque el patrón no esté completo; esto solo cuenta lo que
    strategy.evaluate() aceptaría abrir en este mismo instante.

    cascade_lookup, si se pasa, es una función symbol -> dict|None
    (normalmente LiquidationTracker.cascade_status). Cuando la
    cascada activa confirma la dirección de la señal, se añade una
    línea aparte — es información extra, no cambia qué símbolos
    entran en la lista ni el orden.

    Devuelve None si no hay nada que decir (ni listas ni candidatas
    cercanas), para que el bot no mande un mensaje vacío.
    """
    listas = [r for r in rows if r.verdict == "LISTA" and r.signal is not None]
    if listas:
        lineas = [f"🏆 <b>Favoritas de este escaneo</b> — {len(listas)} lista(s) para operar\n"]
        for r in listas[:top]:
            sig = r.signal
            base = r.symbol.split("-")[0]
            lado = "LARGO" if sig.side == "BUY" else "CORTO"
            emoji = "🟢" if sig.side == "BUY" else "🔴"
            linea = (
                f"{emoji} <b>{base}</b> {lado} — entrada <code>{sig.entry:.8g}</code>  "
                f"SL <code>{sig.sl:.8g}</code>  TP <code>{sig.tp:.8g}</code>\n"
                f"   R:R {sig.rr:.2f} · ATR {sig.atr_pct:.2f}% ({sig.cost_cover:.0f}×) "
                f"· estirón {sig.stretch:+.2f} ATR"
            )
            if cascade_lookup:
                casc = cascade_lookup(r.symbol)
                if casc and casc["activa"] and liquidations.cascade_confirms(sig.side, casc["lado"]):
                    linea += (
                        f"\n   🔥 <b>Confirmada por cascada de liquidaciones</b>: "
                        f"{casc['multiplicador']:.1f}× lo normal, {casc['n_eventos']} "
                        f"liquidaciones de {casc['lado'].lower()} en los últimos "
                        f"{config.LIQ_SHORT_WINDOW_SEC // 60} min"
                    )
            lineas.append(linea)
        lineas.append(
            "\n<i>Cumplen el patrón completo ahora mismo. Si hay hueco libre "
            "(MAX_CONCURRENT) el bot ya las está procesando por su cuenta.</i>"
        )
        return "\n".join(lineas)

    casi = sorted(
        (r for r in rows if r.verdict == "estirado"),
        key=lambda r: abs(r.stretch),
        reverse=True,
    )[:5]
    if not casi:
        return None

    lineas = ["👀 <b>Ninguna lista todavía</b> — las más cerca de estarlo:\n"]
    for r in casi:
        base = r.symbol.split("-")[0]
        direccion = "sobrecomprada" if r.stretch > 0 else "sobrevendida"
        lineas.append(
            f"· <b>{base}</b> {direccion}, estirón {r.stretch:+.2f} ATR "
            f"— falta la vela de agotamiento o el R:R no llega"
        )
    return "\n".join(lineas)
