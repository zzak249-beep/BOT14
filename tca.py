"""
TCA — coste de operar MEDIDO por símbolo, no supuesto.

═══════════════════════════════════════════════════════════════════════
EL PROBLEMA QUE RESUELVE
═══════════════════════════════════════════════════════════════════════
COST_ROUNDTRIP_PCT era UNA constante aplicada a los 400 símbolos. Pero
el coste real depende de la profundidad del libro, del spread y del
tamaño, y varía por símbolo en un orden de magnitud. Con un solo
número estabas rechazando símbolos baratos y aceptando caros con el
mismo listón — y ese listón manda sobre todo lo demás, porque el
umbral efectivo de amplitud es MIN_COST_COVER x COST_ROUNDTRIP_PCT.

EL BENCHMARK: ARRIVAL PRICE
En trading sistemático el benchmark correcto es el precio en el
momento en que se generó la señal, porque el backtest supone que se
ejecuta ahí. La diferencia entre ese precio y el realmente conseguido
es el deslizamiento de llegada, y es lo único que mide de verdad la
distancia entre el backtest y la cuenta.

El diario ya guarda entrada_esperada y entrada_real desde el primer
día. Este módulo simplemente lo lee.

CÓMO SE COMPONE EL COSTE
    coste = comisión_entrada + comisión_salida + 2 x deslizamiento

La comisión de entrada es MAKER si POST_ONLY está activo (la orden no
cruza el spread nunca) y TAKER si no. La de salida es siempre TAKER:
el SL y el TP son STOP_MARKET / TAKE_PROFIT_MARKET.

El deslizamiento se mide solo en la ENTRADA, así que se cuenta dos
veces como aproximación de la ida y vuelta. Es conservador: la salida
por stop en un mercado que se mueve suele deslizar más que la entrada,
no menos.

QUÉ NO HACE
No estima impacto de mercado. Con este tamaño la orden no mueve el
libro, así que ese término es cero y meterlo sería teatro.
"""
from __future__ import annotations

import csv
import logging
import os
import statistics
import time

import config

log = logging.getLogger("tca")

_cache: dict[str, tuple[float, int]] = {}   # symbol -> (coste_pct, n_ops)
_cache_ts = 0.0
_CACHE_SEG = 900   # 15 min: el diario no cambia tan rápido


def _ruta_diario() -> str:
    base = os.path.dirname(config.STATE_PATH) or "/data"
    return os.path.join(base, "operaciones_wavelet.csv")


def comision_ida_vuelta() -> float:
    """Comisiones puras, sin deslizamiento, en %."""
    entrada = config.FEE_MAKER_PCT if config.POST_ONLY else config.FEE_TAKER_PCT
    return entrada + config.FEE_TAKER_PCT   # la salida (SL/TP) siempre es taker


def _cargar() -> None:
    """Relee el diario y recalcula el coste mediano por símbolo."""
    global _cache, _cache_ts
    _cache_ts = time.time()
    nuevo: dict[str, list[float]] = {}
    ruta = _ruta_diario()
    if not os.path.exists(ruta):
        _cache = {}
        return
    try:
        with open(ruta, newline="") as f:
            for fila in csv.DictReader(f):
                sym = (fila.get("symbol") or "").strip()
                bruto = (fila.get("deslizamiento_pct") or "").strip()
                if not sym or not bruto:
                    continue
                try:
                    nuevo.setdefault(sym, []).append(abs(float(bruto)))
                except ValueError:
                    continue
    except OSError as exc:
        log.warning("No se pudo leer el diario para TCA: %s", exc)
        _cache = {}
        return

    fijo = comision_ida_vuelta()
    _cache = {}
    for sym, desl in nuevo.items():
        # MEDIANA, no media: un solo fill catastrófico no debe decidir
        # el listón de un símbolo para siempre.
        _cache[sym] = (fijo + 2.0 * statistics.median(desl), len(desl))


def coste(symbol: str) -> float:
    """
    Coste de ida y vuelta en % para este símbolo.

    Con menos de MIN_TCA_SAMPLES operaciones se devuelve la constante:
    tres fills no son una medición, son tres anécdotas.
    """
    if not config.USE_TCA:
        return config.COST_ROUNDTRIP_PCT
    if time.time() - _cache_ts > _CACHE_SEG:
        _cargar()
    medido, n = _cache.get(symbol, (0.0, 0))
    if n < config.MIN_TCA_SAMPLES:
        return config.COST_ROUNDTRIP_PCT
    return medido


def medido(symbol: str) -> tuple[float, int]:
    """(coste medido, nº de operaciones). n=0 significa sin datos."""
    if time.time() - _cache_ts > _CACHE_SEG:
        _cargar()
    return _cache.get(symbol, (0.0, 0))


def sospechoso(symbol: str) -> bool:
    """
    ¿Este símbolo cuesta mucho más de lo que suponíamos?

    Si el coste medido supera TCA_BLACKLIST_MULT veces la estimación,
    el símbolo se descarta: no es que la estrategia falle ahí, es que
    operarlo es más caro de lo que ninguna expectativa razonable puede
    cubrir.
    """
    if not config.USE_TCA:
        return False
    c, n = medido(symbol)
    if n < config.MIN_TCA_SAMPLES:
        return False
    return c > config.COST_ROUNDTRIP_PCT * config.TCA_BLACKLIST_MULT


def informe(top: int = 12) -> str:
    """Resumen para el aviso diario."""
    if time.time() - _cache_ts > _CACHE_SEG:
        _cargar()
    if not _cache:
        return ("📐 <b>TCA</b>: sin datos todavía.\n"
                f"<i>Hacen falta {config.MIN_TCA_SAMPLES} operaciones por símbolo "
                f"para sustituir la estimación de {config.COST_ROUNDTRIP_PCT}%.</i>")
    filas = [(s, c, n) for s, (c, n) in _cache.items() if n >= config.MIN_TCA_SAMPLES]
    if not filas:
        n_tot = sum(n for _, n in _cache.values())
        return (f"📐 <b>TCA</b>: {n_tot} operaciones repartidas, ninguna con "
                f"{config.MIN_TCA_SAMPLES}+ en el mismo símbolo todavía.")
    filas.sort(key=lambda x: -x[1])
    fijo = comision_ida_vuelta()
    out = [f"📐 <b>Coste medido</b> (comisión {fijo:.3f}% + deslizamiento x2)", ""]
    for sym, c, n in filas[:top]:
        marca = "🔴" if c > config.COST_ROUNDTRIP_PCT * config.TCA_BLACKLIST_MULT else "·"
        out.append(f"{marca} <b>{sym.split('-')[0]}</b>  {c:.3f}%  ({n} ops)")
    caros = [s for s, c, n in filas
             if c > config.COST_ROUNDTRIP_PCT * config.TCA_BLACKLIST_MULT]
    out.append("")
    out.append(f"<i>Estimación usada por defecto: {config.COST_ROUNDTRIP_PCT}%. "
               f"🔴 = descartado automáticamente ({len(caros)}).</i>")
    return "\n".join(out)
