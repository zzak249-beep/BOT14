"""
Funding: dos usos, uno inviable y otro valioso.

═══════════════════════════════════════════════════════════════════════
1. EL CARRY (delta-neutral) NO COMPENSA CON ESTA CUENTA
═══════════════════════════════════════════════════════════════════════
Comprar spot y vender el perpetuo cobra el funding sin riesgo
direccional. Es edge ESTRUCTURAL —viene de la mecánica del contrato, no
de predecir— y por eso los fondos que lo hacen reportan drawdowns por
debajo del 1%.

Pero con 135 USDT repartidos en dos piernas de 67,5:

    funding normal  (0.01%/8h) -> 0.020 USDT/día · 11% anual
    funding alto    (0.05%/8h) -> 0.101 USDT/día · 55% anual
    funding extremo (0.10%/8h) -> 0.203 USDT/día · 110% anual

Las fuentes citan 2.000 USD como capital mínimo razonable. No es
cuestión de optimizar: el porcentaje es correcto y la base es demasiado
pequeña. Por eso este módulo NO monta carry: avisa.

CORRECCIÓN DE ESTA VERSIÓN: los "días para cubrir el coste" son
INDEPENDIENTES del saldo —el capital se cancela en la división— así que
el ✅ salía siempre igual con 135 USDT que con 0. Ahora el ✅ exige dos
cosas: que cubra el coste rápido Y que el ingreso diario en USDT llegue
a un mínimo. Con una cuenta pequeña casi nunca lo hará, que es
justamente la información útil.

═══════════════════════════════════════════════════════════════════════
2. LO QUE SÍ VALE HOY: EL FUNDING COMO POSICIONAMIENTO
═══════════════════════════════════════════════════════════════════════
El funding dice quién está pagando por mantenerse dentro. Positivo y
extremo significa largos amontonados pagando para no soltar. Donde hay
posicionamiento amontonado hay combustible para una cascada, que es
exactamente el terreno donde la reversión funciona. Entra como
CONTEXTO en el diario, no como disparador.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import config

log = logging.getLogger("funding")


@dataclass
class Funding:
    symbol: str
    rate: float            # tasa del intervalo, en %
    anual_pct: float
    dias_para_cubrir: float
    usdt_dia: float
    compensa: bool


def anualizar(rate_pct: float, intervalos_dia: int = 3) -> float:
    return rate_pct * intervalos_dia * 365


def evaluar(symbol: str, rate_pct: float, saldo: float) -> Funding:
    """
    ¿Compensaría montar el carry en este símbolo con este saldo?

    Dos criterios, no uno: cuántos días de funding pagan la comisión de
    las dos piernas (independiente del tamaño) y cuántos USDT al día
    entran de verdad (muy dependiente del tamaño).
    """
    por_pierna = max(0.0, saldo) / 2.0
    dia = por_pierna * abs(rate_pct) / 100.0 * 3
    coste = por_pierna * config.COST_ROUNDTRIP_PCT / 100.0 * 2
    dias = coste / dia if dia > 0 else 999.0
    minimo_dia = getattr(config, "FUNDING_MIN_USDT_DIA", 0.10)
    return Funding(
        symbol=symbol,
        rate=rate_pct,
        anual_pct=anualizar(abs(rate_pct)),
        dias_para_cubrir=dias,
        usdt_dia=dia,
        compensa=(dias <= config.CARRY_MAX_DIAS_COBERTURA and dia >= minimo_dia),
    )


def format_extremos(items: list[Funding], saldo: float) -> str | None:
    """Aviso solo cuando hay funding fuera de lo normal."""
    if not items:
        return None
    items = sorted(items, key=lambda f: -abs(f.rate))
    lineas = [f"💸 <b>Funding extremo</b> — {len(items)} símbolo(s)  ·  saldo {saldo:.2f} USDT", ""]
    for f in items[:10]:
        base = f.symbol.split("-")[0]
        lado = "largos pagan" if f.rate > 0 else "cortos pagan"
        marca = "✅" if f.compensa else "·"
        lineas.append(
            f"{marca} <b>{base}</b>  {f.rate:+.4f}%/8h  ({f.anual_pct:.0f}% anual)  {lado}  ·  "
            f"{f.usdt_dia:.3f} USDT/día · cubre coste en {f.dias_para_cubrir:.1f} días"
        )
    lineas.append("")
    lineas.append(
        f"<i>✅ = cubriría su coste en ≤{config.CARRY_MAX_DIAS_COBERTURA} días Y daría al menos "
        f"{getattr(config, 'FUNDING_MIN_USDT_DIA', 0.10):.2f} USDT/día con {saldo:.0f} USDT. "
        f"El resto es interesante como posicionamiento, no como negocio: funding muy positivo "
        f"significa largos amontonados pagando por no soltar, que es donde una cascada encuentra "
        f"combustible.</i>"
    )
    return "\n".join(lineas)


def sesgo(rate_pct: float) -> str:
    """
    Lectura de posicionamiento para acompañar una señal direccional. No
    cambia si se abre o no. Un corto con funding muy positivo va A FAVOR
    del desequilibrio; un largo con ese mismo funding va en contra.
    """
    if rate_pct >= config.FUNDING_EXTREMO:
        return "largos amontonados"
    if rate_pct <= -config.FUNDING_EXTREMO:
        return "cortos amontonados"
    return "equilibrado"
