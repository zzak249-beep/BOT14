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
import math
import time
from dataclasses import dataclass

import config
import strategy
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
    verdict: str


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

    # Mismo criterio que el radar de TradingView: manda la amplitud.
    if atr_pct < config.MIN_ATR_PCT or cover < config.MIN_COST_COVER:
        verdict = "sin amplitud"
    elif state == "estirado":
        verdict = "REVERSIÓN"
    elif state == "fuera" and er_l >= config.ER_TREND:
        verdict = "RUPTURA"
    else:
        verdict = "en espera"

    return Row(symbol, atr_pct, cover, er_s, er_l, stretch, state, verdict)


class Scanner:
    def __init__(self, api: BingX) -> None:
        self.api = api
        self.sem = asyncio.Semaphore(config.SCAN_CONCURRENCY)
        self.last_run = 0.0

    async def _one(self, symbol: str) -> Row | None:
        async with self.sem:
            try:
                candles = await self.api.klines(symbol, config.TIMEFRAME, limit=300)
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
            "Escaneo: %d/%d símbolos analizados en %.0fs · %d con amplitud",
            len(rows),
            len(symbols),
            time.time() - t0,
            sum(1 for r in rows if r.verdict != "sin amplitud"),
        )
        return rows


def funnel(rows: list[Row], closes_ok: int, total: int, liquidez_ok: int) -> str:
    """
    EMBUDO: cuántos símbolos caen en cada filtro.

    Responde con números a "¿por qué no entra nunca?", que si no se
    contesta deduciendo. Y deja a la vista una interacción que es fácil
    no ver: el umbral efectivo de ATR no es MIN_ATR_PCT, sino el mayor
    de los dos — MIN_COST_COVER × COST_ROUNDTRIP_PCT. Con 30× y 0.25%
    de coste se están pidiendo 7.5% de ATR, no 4%.
    """
    umbral_efectivo = max(config.MIN_ATR_PCT, config.MIN_COST_COVER * config.COST_ROUNDTRIP_PCT)
    con_amp = [r for r in rows if r.atr_pct >= umbral_efectivo]
    tras_er = [r for r in con_amp if r.er_long <= config.MAX_ER_LONG]
    estirados = [r for r in tras_er if abs(r.stretch) >= config.STRETCH_ATR]

    return (
        f"🔻 <b>Embudo del escaneo</b>\n"
        f"Universo: {total}\n"
        f"→ liquidez ≥{config.MIN_QUOTE_VOLUME_24H/1e6:.1f}M: <b>{liquidez_ok}</b>\n"
        f"→ con datos suficientes: {closes_ok}\n"
        f"→ ATR ≥{umbral_efectivo:.1f}%: <b>{len(con_amp)}</b>\n"
        f"→ no vertical (ER ≤{config.MAX_ER_LONG}): <b>{len(tras_er)}</b>\n"
        f"→ estirado ≥{config.STRETCH_ATR} ATR: <b>{len(estirados)}</b>\n\n"
        f"<i>El umbral de ATR que se aplica de verdad es {umbral_efectivo:.1f}% "
        f"({config.MIN_COST_COVER:.0f}× × {config.COST_ROUNDTRIP_PCT}% de coste), "
        f"no el {config.MIN_ATR_PCT}% de MIN_ATR_PCT.</i>"
    )


def market_temperature(rows: list[Row]) -> dict:
    """
    Termómetro del universo. La pregunta que responde no es "¿hay
    candidatos?" sino "¿se está calentando el mercado?", que es la que
    permite anticipar. Un mercado con la mediana de ATR subiendo va a
    dar candidatos pronto aunque hoy no dé ninguno; uno con la mediana
    plana puede pasarse semanas sin dar nada, y eso también es una
    respuesta.
    """
    if not rows:
        return {"n": 0}
    atrs = sorted(r.atr_pct for r in rows)
    n = len(atrs)
    mediana = atrs[n // 2]
    p90 = atrs[int(n * 0.90)] if n >= 10 else atrs[-1]
    umbral = config.MIN_ATR_PCT
    return {
        "n": n,
        "mediana": mediana,
        "p90": p90,
        "maximo": atrs[-1],
        "cerca": sum(1 for a in atrs if umbral * 0.5 <= a < umbral),
        "listos": sum(1 for a in atrs if a >= umbral),
    }


def _tf_minutes(tf: str) -> int:
    return {"1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "2h": 120, "4h": 240}.get(tf, 5)


def candidates_at_timeframe(rows: list[Row], tf_destino: str) -> int:
    """
    Cuántos símbolos pasarían el filtro en OTRO timeframe.

    La volatilidad escala con la RAÍZ del tiempo (ATR de 15m ≈ 1.73× el
    de 5m), pero el coste de operar NO escala: la comisión y el spread
    son los mismos entres en la vela que entres. Por eso subir de
    timeframe da más candidatos SIN rebajar el listón — no es aflojar el
    filtro, es que el mismo movimiento pesa más frente al mismo coste.

    Es una estimación por la regla de la raíz, no una medición: sirve
    para decidir si merece la pena mirarlo de verdad.
    """
    if not rows:
        return 0
    factor = math.sqrt(_tf_minutes(tf_destino) / _tf_minutes(config.TIMEFRAME))
    n = 0
    for r in rows:
        atr_esc = r.atr_pct * factor
        cover_esc = atr_esc / config.COST_ROUNDTRIP_PCT if config.COST_ROUNDTRIP_PCT > 0 else 0.0
        if atr_esc >= config.MIN_ATR_PCT and cover_esc >= config.MIN_COST_COVER:
            n += 1
    return n


def format_temperature(t: dict) -> str:
    if not t.get("n"):
        return "Sin datos del universo."
    return (
        f"🌡️ <b>Temperatura del mercado</b>\n"
        f"ATR mediano {t['mediana']:.2f}%  ·  percentil 90: {t['p90']:.2f}%  ·  máx {t['maximo']:.2f}%\n"
        f"A media distancia del umbral: {t['cerca']}  ·  listos: {t['listos']}\n"
        f"(umbral {config.MIN_ATR_PCT}% sobre {t['n']} símbolos)"
    )


def format_watchlist(rows: list[Row], top: int) -> str:
    """
    Top por amplitud AUNQUE ninguno sea operable todavía.
    Es la lista de vigilancia: las mejores situadas del mercado ahora
    mismo, con una marca de si pasan el listón o solo se acercan.
    """
    if not rows:
        return "📡 <b>Escaneo BingX</b>\nSin datos."
    mejores = sorted(rows, key=lambda r: r.cover, reverse=True)[:top]
    t = market_temperature(rows)
    lineas = [f"📡 <b>Mejores situadas</b> — {len(rows)} símbolos\n"]
    for r in mejores:
        if r.verdict == "REVERSIÓN":
            marca = "🔶"
        elif r.verdict == "RUPTURA":
            marca = "🟩"
        elif r.verdict == "sin amplitud":
            marca = "·"
        else:
            marca = "✅"
        base = r.symbol.split("-")[0]
        lineas.append(
            f"{marca} <b>{base}</b>  {r.atr_pct:.2f}% ({r.cover:.0f}×)  "
            f"ER {r.er_short:.2f}/{r.er_long:.2f}  {r.state} {r.stretch:+.1f}"
        )
    lineas.append(
        f"\n🔶 reversión lista · 🟩 ruptura lista · ✅ pasa el listón, esperando · · aún no"
    )
    lineas.append(f"Umbral: ≥{config.MIN_ATR_PCT}% y ≥{config.MIN_COST_COVER:.0f}×  ·  ATR mediano {t.get('mediana', 0):.2f}%")
    return "\n".join(lineas)


def format_ranking(rows: list[Row], top: int) -> str:
    con_amplitud = [r for r in rows if r.verdict != "sin amplitud"]
    if not con_amplitud:
        en15 = candidates_at_timeframe(rows, "15m")
        en30 = candidates_at_timeframe(rows, "30m")
        extra = ""
        if en15 or en30:
            extra = (
                f"\n\n💡 Con el MISMO listón, en 15m pasarían <b>{en15}</b> y en 30m <b>{en30}</b>.\n"
                f"La volatilidad crece con la raíz del tiempo y el coste no: "
                f"subir de timeframe da más candidatos sin rebajar el filtro."
            )
        return (
            f"📡 <b>Escaneo BingX</b>\n"
            f"{len(rows)} símbolos analizados.\n"
            f"<b>Ninguno con amplitud suficiente</b> (≥{config.MIN_ATR_PCT}% y "
            f"≥{config.MIN_COST_COVER:.0f}× el coste).\n"
            f"Mercado tranquilo: no es un fallo, es que no hay nada que operar."
            f"{extra}"
        )

    lineas = [f"📡 <b>Escaneo BingX</b> — {len(rows)} símbolos, {len(con_amplitud)} con amplitud\n"]
    for r in con_amplitud[:top]:
        marca = "🔶" if r.verdict == "REVERSIÓN" else "🟩" if r.verdict == "RUPTURA" else "·"
        base = r.symbol.split("-")[0]
        lineas.append(
            f"{marca} <b>{base}</b>  {r.atr_pct:.2f}% ({r.cover:.0f}×)  "
            f"ER {r.er_short:.2f}/{r.er_long:.2f}  {r.state} {r.stretch:+.1f}"
        )
    lineas.append("\n🔶 reversión lista · 🟩 ruptura lista · · con amplitud, en espera")
    return "\n".join(lineas)
