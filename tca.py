"""
tca.py — Análisis del coste de transacción. El módulo que main.py
importaba y que no existía en el repo.

═══════════════════════════════════════════════════════════════════════
POR QUÉ IMPORTA MÁS EN SCALPING QUE EN NINGÚN OTRO SITIO
═══════════════════════════════════════════════════════════════════════
El coste en R = (coste% x precio) / distancia_al_stop. Cuanto más corto
el stop, más pesa la comisión:

    stop 2.08% (15m)  ->  0.07% de coste = 0.03 R
    stop 0.72% (3m)   ->  0.07% de coste = 0.10 R
    stop 0.42% (1m)   ->  0.07% de coste = 0.17 R

Por eso en scalping el coste deja de ser un detalle contable y pasa a
ser el filtro principal. Y por eso no vale ASUMIRLO: hay que medirlo.

COST_ROUNDTRIP_PCT=0.25 era una estimación conservadora que incluía
deslizamiento. Las comisiones reales de BingX son maker 0.02% y taker
0.05%. La diferencia no es menor: en 5m con stop 1.5xATR el coste pasa
de 0.22 R a 0.09 R. La verdad está en medio y DEPENDE DEL SÍMBOLO.

Este módulo la mide con las operaciones reales del diario: deslizamiento
observado por símbolo, comisión teórica según cómo se entre, y una lista
negra de los símbolos donde el coste medido se dispara.

═══════════════════════════════════════════════════════════════════════
LO QUE NO PUEDE HACER
═══════════════════════════════════════════════════════════════════════
Con el diario vacío devuelve la comisión teórica y lo dice. No inventa
un deslizamiento que no ha visto. Y hasta MIN_TCA_SAMPLES operaciones
por símbolo no lista a nadie: con tres fills, el deslizamiento medio es
ruido.
"""
from __future__ import annotations

import csv
import logging
import os
from pathlib import Path

import config

log = logging.getLogger("tca")


def _cfg(nombre: str, defecto):
    return getattr(config, nombre, defecto)


def comision_ida_vuelta() -> float:
    """
    Comisión teórica de abrir y cerrar, en % del nocional.

    La SALIDA es siempre taker: el stop y el objetivo van embebidos como
    STOP_MARKET / TAKE_PROFIT_MARKET, que son órdenes a mercado. Solo la
    entrada puede ser maker, y solo si la limitada no cruza el spread.
    """
    maker = float(_cfg("FEE_MAKER_PCT", 0.02))
    taker = float(_cfg("FEE_TAKER_PCT", 0.05))
    entrada_maker = (str(_cfg("ENTRY_TYPE", "LIMIT")).upper() == "LIMIT"
                     and bool(_cfg("POST_ONLY", True)))
    return (maker if entrada_maker else taker) + taker


def _diario() -> list[dict]:
    ruta = Path(os.path.dirname(_cfg("STATE_PATH", "/data/state.json")) or "/data")
    ruta = ruta / "operaciones_wavelet.csv"
    try:
        if not ruta.exists():
            return []
        with ruta.open(newline="") as f:
            return list(csv.DictReader(f))
    except Exception as exc:  # noqa: BLE001
        log.warning("No se pudo leer el diario para TCA: %s", exc)
        return []


def _num(x, defecto=None):
    try:
        return float(x)
    except (TypeError, ValueError):
        return defecto


def por_simbolo() -> dict[str, dict]:
    """
    Deslizamiento REAL medido por símbolo, de las operaciones del diario.

    deslizamiento_pct lo escribe journal.abrir() cuando la limitada se
    ejecuta: es (entrada_real - entrada_esperada) / esperada. Con
    entradas maker suele ser favorable o cero; con órdenes a mercado en
    pares finos es donde aparece el coste que nadie contabiliza.
    """
    filas = _diario()
    if not filas:
        return {}
    acc: dict[str, list[float]] = {}
    for f in filas:
        d = _num(f.get("deslizamiento_pct"))
        if d is None:
            continue
        acc.setdefault(f.get("symbol", "?"), []).append(abs(d))
    minimo = int(_cfg("MIN_TCA_SAMPLES", 10))
    out = {}
    for sym, ds in acc.items():
        if len(ds) < minimo:
            continue
        medio = sum(ds) / len(ds)
        out[sym] = {
            "n": len(ds),
            "desliz_medio": medio,
            "coste_total": comision_ida_vuelta() + medio * 2,  # entrada y salida
        }
    return out


def coste_real(symbol: str) -> float:
    """
    Coste de ida y vuelta a usar para ESTE símbolo, en %.

    Si hay muestra suficiente, comisión + deslizamiento medido. Si no,
    la comisión teórica — y sin inflarla con un deslizamiento inventado.
    """
    datos = por_simbolo().get(symbol)
    if not datos:
        return comision_ida_vuelta()
    return datos["coste_total"]


def en_lista_negra(symbol: str) -> tuple[bool, str]:
    """
    ¿El coste medido de este símbolo se dispara respecto a la comisión?

    TCA_BLACKLIST_MULT=2.0 significa: si operar este símbolo cuesta más
    del doble de la comisión teórica, el deslizamiento manda sobre la
    ventaja y no compensa. Es el filtro que ningún backtest puede dar,
    porque el deslizamiento solo se conoce operando.
    """
    if not bool(_cfg("USE_TCA", True)):
        return False, ""
    datos = por_simbolo().get(symbol)
    if not datos:
        return False, ""
    mult = float(_cfg("TCA_BLACKLIST_MULT", 2.0))
    teorico = comision_ida_vuelta()
    if teorico > 0 and datos["coste_total"] > teorico * mult:
        return True, (f"coste medido {datos['coste_total']:.3f}% = "
                      f"{datos['coste_total']/teorico:.1f}x la comisión "
                      f"({datos['n']} ops)")
    return False, ""


def informe() -> str:
    """Resumen para el aviso diario."""
    teorico = comision_ida_vuelta()
    datos = por_simbolo()
    L = [f"💱 <b>Coste de ejecución</b>",
         f"Comisión teórica ida y vuelta: <b>{teorico:.3f}%</b>"]

    entrada_maker = (str(_cfg("ENTRY_TYPE", "LIMIT")).upper() == "LIMIT"
                     and bool(_cfg("POST_ONLY", True)))
    L.append(f"Entrada {'maker' if entrada_maker else 'taker'} · salida taker "
             f"(el stop es orden a mercado)")

    if not datos:
        minimo = int(_cfg("MIN_TCA_SAMPLES", 10))
        L.append(f"\n<i>Sin símbolos con {minimo}+ operaciones todavía. "
                 f"Hasta entonces se usa la comisión teórica: el deslizamiento "
                 f"no se estima, se mide.</i>")
        return "\n".join(L)

    peores = sorted(datos.items(), key=lambda kv: -kv[1]["coste_total"])[:8]
    L.append("\n<b>Coste real medido</b> (comisión + deslizamiento):")
    for sym, d in peores:
        negra, _ = en_lista_negra(sym)
        marca = "🚫" if negra else "·"
        L.append(f"{marca} {sym.split('-')[0]}: {d['coste_total']:.3f}% "
                 f"(desliz {d['desliz_medio']:.3f}%, n={d['n']})")

    n_negros = sum(1 for s in datos if en_lista_negra(s)[0])
    if n_negros:
        L.append(f"\n🚫 = coste por encima de {_cfg('TCA_BLACKLIST_MULT', 2.0)}x "
                 f"la comisión. {n_negros} símbolo(s).")

    # En scalping el coste en R es lo que decide, no el coste en %.
    sl_atr = float(_cfg("SL_ATR", 1.5))
    L.append(f"\n<i>Con stop de {sl_atr}xATR, este coste equivale a:</i>")
    for tf, atr in (("3m", 0.48), ("5m", 0.76), ("15m", 1.39)):
        stop = atr * sl_atr
        L.append(f"  {tf}: {teorico / stop:.2f} R"
                 + ("  ⚠️" if teorico / stop > float(_cfg("MAX_COST_IN_R", 0.20)) else ""))
    return "\n".join(L)
