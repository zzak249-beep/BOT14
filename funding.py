"""
Funding: dos usos, uno inviable y otro valioso.

═══════════════════════════════════════════════════════════════════════
CORRECCIÓN DE LA VERSIÓN ANTERIOR (09/09/2026)
═══════════════════════════════════════════════════════════════════════
El ✅ mentía. Marcaba "con X USDT el carry cubriría su coste en ≤3 días",
pero los días para cubrir NO DEPENDEN DEL SALDO:

    días = coste/ganancia = (2·COST_ROUNDTRIP) / (3·|rate|)

La ganancia y el coste escalan IGUAL con el tamaño, así que el saldo se
cancela. Daba 1,12 días para ORCA con 10 USDT y los mismos 1,12 con
100.000. El aviso contestaba una pregunta que no depende de tu cuenta y la
presentaba como si fuera personalizada.

Ahora el ✅ exige DOS cosas:
  1. que cubra el coste en pocos días (esto es scale-free, informativo)
  2. que la GANANCIA ABSOLUTA con tu saldo llegue a un mínimo mensual

La segunda es la que decide de verdad. Con 135 USDT repartidos en dos
piernas, un funding de 0,15%/8h da unos 9 USDT/mes; con 2.000 daría 134.

═══════════════════════════════════════════════════════════════════════
Y ADEMÁS: EL FUNDING NEGATIVO NO ES EJECUTABLE PARA TI
═══════════════════════════════════════════════════════════════════════
Con funding POSITIVO cobras estando corto en el perpetuo y largo en el
contado. Las dos piernas se pueden montar.

Con funding NEGATIVO habría que estar largo en el perpetuo y CORTO EN EL
CONTADO — vender algo que no tienes, o sea pedirlo prestado, con su
interés y su disponibilidad. En la práctica el carry retail solo funciona
con funding positivo. La versión anterior marcaba con ✅ símbolos de
funding negativo como si fueran oportunidades.

═══════════════════════════════════════════════════════════════════════
LO QUE SÍ VALE HOY: EL FUNDING COMO POSICIONAMIENTO
═══════════════════════════════════════════════════════════════════════
El funding dice quién está pagando por mantenerse dentro. Positivo y
extremo significa largos amontonados pagando para no soltar; negativo y
extremo, cortos amontonados.

Eso conecta con las cascadas: donde hay posicionamiento amontonado hay
combustible, que es exactamente el terreno donde la reversión funciona.

Y con tu propia posición: si estás largo en un símbolo de funding
negativo, COBRAS por estar dentro. Si estás corto, PAGAS. Con operaciones
de dos horas apenas roza, pero en una que se alarga cuenta.

Así que el funding entra como CONTEXTO, no como disparador.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import config

log = logging.getLogger("funding")

# Suelo de ganancia para que el carry valga la pena montarlo. No es una
# constante del mercado: es cuánto tiene que rendir para compensar el
# trabajo, el margen ocupado y el riesgo de que el funding se dé la vuelta.
MIN_CARRY_MES_USDT = 5.0


@dataclass
class Funding:
    symbol: str
    rate: float               # tasa del intervalo, en %
    anual_pct: float          # equivalente anualizado
    dias_para_cubrir: float   # scale-free: NO depende del saldo
    ganancia_dia: float       # esto SÍ depende del saldo
    ganancia_mes: float
    ejecutable: bool          # el carry con funding negativo exige vender contado
    compensa: bool            # cubre coste rápido Y gana lo suficiente Y es ejecutable


def anualizar(rate_pct: float, intervalos_dia: int = 3) -> float:
    return rate_pct * intervalos_dia * 365


def evaluar(symbol: str, rate_pct: float, saldo: float) -> Funding:
    """
    ¿Compensaría montar el carry en este símbolo con este saldo?

    Dos criterios distintos y los dos hacen falta:

    - DÍAS PARA CUBRIR: cuántos días de funding pagan la comisión de abrir
      y cerrar las dos piernas. Es scale-free (el saldo se cancela), así
      que mide la CALIDAD de la tasa, no si te conviene a ti.

    - GANANCIA MENSUAL: lo que te llevas de verdad con tu saldo. Esto es
      lo que decide, y es lo que faltaba antes.
    """
    coste_rt = getattr(config, "COST_ROUNDTRIP_PCT", 0.25)
    por_pierna = max(saldo, 0.0) / 2.0
    dia = por_pierna * abs(rate_pct) / 100.0 * 3

    # Scale-free y por tanto calculable aunque el saldo sea 0 o ilegible.
    dias = (2.0 * coste_rt) / (3.0 * abs(rate_pct)) if abs(rate_pct) > 1e-9 else 999.0

    mes = dia * 30.0
    ejecutable = rate_pct > 0        # ver la cabecera: el negativo exige vender contado
    max_dias = getattr(config, "CARRY_MAX_DIAS_COBERTURA", 3.0)

    return Funding(
        symbol=symbol,
        rate=rate_pct,
        anual_pct=anualizar(abs(rate_pct)),
        dias_para_cubrir=dias,
        ganancia_dia=dia,
        ganancia_mes=mes,
        ejecutable=ejecutable,
        compensa=(dias <= max_dias and mes >= MIN_CARRY_MES_USDT and ejecutable),
    )


def format_extremos(items: list[Funding], saldo: float) -> str | None:
    """
    Aviso de funding fuera de lo normal.

    Lo primero es el POSICIONAMIENTO, que es lo que se usa. El carry va
    después y casi siempre en negativo, que es la verdad.
    """
    if not items:
        return None
    items = sorted(items, key=lambda f: -abs(f.rate))

    largos = [f for f in items if f.rate > 0]
    cortos = [f for f in items if f.rate < 0]

    lineas = [f"💸 <b>Funding extremo</b> — {len(items)} símbolo(s)", ""]
    if largos:
        lineas.append("<b>Largos amontonados</b> (pagan por no soltar):")
        for f in largos[:6]:
            lineas.append(
                f"  🔴 <b>{f.symbol.split('-')[0]}</b> {f.rate:+.4f}%/8h "
                f"({f.anual_pct:.0f}% anual)")
    if cortos:
        if largos:
            lineas.append("")
        lineas.append("<b>Cortos amontonados</b> (pagan por no soltar):")
        for f in cortos[:6]:
            lineas.append(
                f"  🟢 <b>{f.symbol.split('-')[0]}</b> {f.rate:+.4f}%/8h "
                f"({f.anual_pct:.0f}% anual)")

    lineas.append("")
    if saldo <= 0:
        lineas.append("⚠️ <b>Saldo 0 o ilegible</b>: no se puede calcular el carry. "
                      "Revisa la lectura de cuenta.")
    else:
        viables = [f for f in items if f.compensa]
        if viables:
            lineas.append(f"<b>Carry viable con {saldo:.0f} USDT:</b>")
            for f in viables[:4]:
                lineas.append(
                    f"  ✅ {f.symbol.split('-')[0]}: {f.ganancia_mes:.2f} USDT/mes "
                    f"· cubre coste en {f.dias_para_cubrir:.1f} días")
        else:
            mejor = max(items, key=lambda f: f.ganancia_mes if f.ejecutable else -1)
            if mejor.ejecutable:
                lineas.append(
                    f"<i>Carry NO viable: el mejor ejecutable es "
                    f"{mejor.symbol.split('-')[0]} con {mejor.ganancia_mes:.2f} USDT/mes "
                    f"(hace falta ≥{MIN_CARRY_MES_USDT:.0f}).</i>")
            else:
                lineas.append(
                    "<i>Carry NO viable: todos los extremos son de funding "
                    "NEGATIVO, y cobrarlo exigiría vender contado prestado.</i>")

    lineas.append("")
    lineas.append(
        "<i>Lo que se usa de aquí es el POSICIONAMIENTO, no el carry. Funding "
        "muy positivo = largos amontonados pagando por no soltar, que es donde "
        "una cascada encuentra combustible. Y si tienes una posición abierta, "
        "el signo te dice si cobras o pagas por estar dentro.</i>")
    return "\n".join(lineas)


def sesgo(rate_pct: float) -> str:
    """
    Lectura de posicionamiento para acompañar una señal direccional.

    No cambia si se abre o no — es contexto para el diario. Un corto con
    funding muy positivo va A FAVOR del desequilibrio (los largos
    amontonados son los que sufren si el precio cae); un largo con funding
    muy positivo va en contra, y además paga por estar dentro.
    """
    umbral = getattr(config, "FUNDING_EXTREMO", 0.05)
    if rate_pct >= umbral:
        return "largos amontonados"
    if rate_pct <= -umbral:
        return "cortos amontonados"
    return "equilibrado"


def coste_por_hora(rate_pct: float, nocional: float) -> float:
    """
    Lo que cuesta (o cobra) mantener una posición LARGA, en USDT/hora.

    Negativo = cobras. Útil para operaciones que se alargan: con
    MAX_TRADE_MINUTES=120 apenas roza, pero una zombi de 8 horas ya paga
    un intervalo entero.
    """
    return nocional * rate_pct / 100.0 / 8.0
