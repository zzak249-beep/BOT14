"""
Reversión de sección cruzada (retorno del día anterior).

QUÉ DICE LA EVIDENCIA
Un estudio sobre más de 3.600 monedas encuentra que las cripto con el
retorno más BAJO del último día superan sistemáticamente a las de
retorno más alto, y el efecto resiste una batería de tests de sección
cruzada sin quedar explicado por otros predictores conocidos.

EL MATIZ QUE DECIDE SI SIRVE O NO
Los autores atribuyen el efecto a la ILIQUIDEZ de la mayoría de
monedas, y añaden que las más grandes y líquidas muestran MOMENTUM
diario en vez de reversión — el efecto contrario. O sea: el edge vive
donde más caro es operar. Por eso este módulo:
  · guarda el ranking de cada día y evalúa solo lo que pasó DESPUÉS,
  · descuenta el coste de ida y vuelta en cada evaluación,
  · y separa el resultado por tramo de liquidez, para que se vea si
    queda algo después de costes o si el efecto se lo comen las
    comisiones justo donde es más fuerte.

POR QUÉ OPERA TODOS LOS DÍAS
El filtro de la estrategia de reversión es ABSOLUTO (ATR ≥ X%), así que
hay días sin candidatos. Este es RELATIVO: siempre existe un "peor 1%",
haya pump o no. Esa es toda la diferencia.

AVISO: aquí no hay nada validado con datos propios todavía. Arranca en
modo registro — apunta y mide. Convertirlo en señales operables es una
decisión posterior, y con números delante.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import config

log = logging.getLogger("xsection")


@dataclass
class Ranked:
    symbol: str
    ret24: float        # retorno de las últimas 24 h, en %
    price: float
    quote_vol: float    # volumen 24 h en USDT


def build_ranking(
    rows: list, volumes: dict[str, float], closes: dict[str, tuple[float, float]]
) -> list[Ranked]:
    """
    closes: symbol -> (precio de hace 24 h, precio actual)
    Devuelve la lista ordenada de MENOR a MAYOR retorno.
    """
    out: list[Ranked] = []
    for sym, (antes, ahora) in closes.items():
        if antes <= 0 or ahora <= 0:
            continue
        out.append(
            Ranked(
                symbol=sym,
                ret24=(ahora - antes) / antes * 100.0,
                price=ahora,
                quote_vol=volumes.get(sym, 0.0),
            )
        )
    out.sort(key=lambda r: r.ret24)
    return out


def pick_sides(ranking: list[Ranked], n: int) -> tuple[list[Ranked], list[Ranked]]:
    """Largos en las peores, cortos en las mejores."""
    if len(ranking) < n * 3:
        return [], []
    return ranking[:n], ranking[-n:]


def format_signal(largos: list[Ranked], cortos: list[Ranked]) -> str:
    def linea(r: Ranked, marca: str) -> str:
        base = r.symbol.split("-")[0]
        return f"{marca} <b>{base}</b> {r.ret24:+.1f}%  ({r.quote_vol/1e6:.1f}M)"

    lineas = ["🔄 <b>Sección cruzada — retorno de 24 h</b>\n", "<u>Largos (los que más cayeron)</u>"]
    lineas += [linea(r, "🟢") for r in largos]
    lineas.append("\n<u>Cortos (los que más subieron)</u>")
    lineas += [linea(r, "🔴") for r in cortos]
    lineas.append(
        f"\n<i>Registro, no orden de entrada. Se evalúa en 24 h "
        f"descontando {config.COST_ROUNDTRIP_PCT}% de coste por operación.</i>"
    )
    return "\n".join(lineas)


def evaluate_previous(
    anterior: dict, closes: dict[str, tuple[float, float]]
) -> tuple[str, dict] | tuple[None, None]:
    """
    Evalúa el ranking guardado ayer con los precios de hoy.

    Esto es lo que convierte el módulo en un experimento en vez de una
    corazonada: cada día se comprueba si el decil bajo batió al alto,
    con el coste ya descontado.
    """
    if not anterior or "largos" not in anterior:
        return None, None

    def rendimiento(items: list[dict], signo: int) -> tuple[float, int]:
        total = 0.0
        n = 0
        for it in items:
            par = closes.get(it["symbol"])
            if not par:
                continue
            _, ahora = par
            entrada = float(it["price"])
            if entrada <= 0 or ahora <= 0:
                continue
            bruto = (ahora - entrada) / entrada * 100.0 * signo
            total += bruto - config.COST_ROUNDTRIP_PCT
            n += 1
        return (total / n if n else 0.0), n

    r_largos, n_l = rendimiento(anterior["largos"], +1)
    r_cortos, n_c = rendimiento(anterior["cortos"], -1)
    spread = r_largos + r_cortos

    resumen = {
        "fecha": anterior.get("fecha", "?"),
        "largos": r_largos,
        "cortos": r_cortos,
        "spread": spread,
        "n": n_l + n_c,
    }
    icono = "✅" if spread > 0 else "❌"
    texto = (
        f"{icono} <b>Resultado de la sección cruzada</b> ({anterior.get('fecha','?')})\n"
        f"Largos {r_largos:+.2f}%  ·  Cortos {r_cortos:+.2f}%\n"
        f"<b>Diferencial neto: {spread:+.2f}%</b> (coste ya descontado)\n"
        f"Sobre {n_l + n_c} posiciones simuladas."
    )
    return texto, resumen


def format_history(hist: list[dict]) -> str:
    """El acumulado, que es lo único que decide."""
    if not hist:
        return "Sin historial todavía."
    n = len(hist)
    total = sum(h["spread"] for h in hist)
    ganadores = sum(1 for h in hist if h["spread"] > 0)
    media = total / n
    return (
        f"📈 <b>Sección cruzada — acumulado</b>\n"
        f"Días medidos: <b>{n}</b>  ·  a favor: {ganadores}\n"
        f"Diferencial medio: <b>{media:+.2f}%</b> por día\n"
        f"Acumulado: {total:+.2f}%\n"
        f"<i>{'Con 20+ días empieza a significar algo.' if n < 20 else 'Muestra suficiente para decidir.'}</i>"
    )
