"""
Funding: dos usos, uno inviable y otro valioso.

═══════════════════════════════════════════════════════════════════════
1. EL CARRY (delta-neutral) NO COMPENSA CON ESTA CUENTA
═══════════════════════════════════════════════════════════════════════
Comprar spot y vender el perpetuo cobra el funding sin riesgo
direccional. Es edge ESTRUCTURAL —viene de la mecánica del contrato, no
de predecir— y por eso los fondos que lo hacen reportan drawdowns por
debajo del 1%.

Pero los números con 135 USDT, repartidos en dos piernas de 67,5:

    funding normal  (0.01%/8h) -> 0.020 USDT/día · 11% anual
    funding alto    (0.05%/8h) -> 0.101 USDT/día · 55% anual
    funding extremo (0.10%/8h) -> 0.203 USDT/día · 110% anual

    coste de abrir y cerrar las dos piernas: 0.27 USDT
    días de funding solo para cubrirlo:
        a 0.01% -> 13.3 días     a 0.05% -> 2.7 días
        a 0.03% ->  4.4 días     a 0.10% -> 1.3 días

Con funding normal son dos céntimos al día y casi dos semanas
recuperando la comisión. Las fuentes citan 2.000 USD como capital
mínimo razonable. No es cuestión de optimizar: el porcentaje es
correcto y la base es demasiado pequeña.

Por eso este módulo NO monta carry automáticamente. Avisa cuando el
funding está tan alto que sí compensaría, con el cálculo hecho para tu
saldo real.

═══════════════════════════════════════════════════════════════════════
2. LO QUE SÍ VALE HOY: EL FUNDING COMO POSICIONAMIENTO
═══════════════════════════════════════════════════════════════════════
El funding dice quién está pagando por mantenerse dentro. Positivo y
extremo significa largos amontonados pagando para no soltar; negativo
y extremo, lo contrario.

Eso conecta con algo ya investigado en este proyecto: el funding
extremo sostenido ha precedido a reversiones fuertes, aunque sin umbral
universal fiable. Y con las cascadas de liquidación: donde hay
posicionamiento amontonado hay combustible para una cascada, que es
exactamente el terreno donde la reversión funciona.

Así que el funding entra como CONTEXTO, no como disparador. Igual que
el registro de BTC: se apunta en el diario y dentro de un mes puedes
comprobar si tus ganadoras se concentran donde el funding era extremo.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import config

log = logging.getLogger("funding")


@dataclass
class Funding:
    symbol: str
    rate: float          # tasa del intervalo, en %
    anual_pct: float     # equivalente anualizado
    dias_para_cubrir: float
    compensa: bool


def anualizar(rate_pct: float, intervalos_dia: int = 3) -> float:
    return rate_pct * intervalos_dia * 365


def evaluar(symbol: str, rate_pct: float, saldo: float) -> Funding:
    """
    ¿Compensaría montar el carry en este símbolo con este saldo?

    El criterio no es el porcentaje anualizado —que suena siempre
    estupendo— sino cuántos días de funding hacen falta para pagar la
    comisión de abrir y cerrar las dos piernas. Si son más de unos
    pocos, cualquier cambio de régimen te deja en pérdida.
    """
    por_pierna = saldo / 2.0
    dia = por_pierna * abs(rate_pct) / 100.0 * 3
    coste = por_pierna * config.COST_ROUNDTRIP_PCT / 100.0 * 2
    dias = coste / dia if dia > 0 else 999.0
    return Funding(
        symbol=symbol,
        rate=rate_pct,
        anual_pct=anualizar(abs(rate_pct)),
        dias_para_cubrir=dias,
        compensa=dias <= config.CARRY_MAX_DIAS_COBERTURA,
    )


def format_extremos(items: list[Funding], saldo: float) -> str | None:
    """Aviso solo cuando hay funding fuera de lo normal."""
    if not items:
        return None
    items = sorted(items, key=lambda f: -abs(f.rate))
    lineas = [f"💸 <b>Funding extremo</b> — {len(items)} símbolo(s)\n"]
    for f in items[:10]:
        base = f.symbol.split("-")[0]
        lado = "largos pagan" if f.rate > 0 else "cortos pagan"
        marca = "✅" if f.compensa else "·"
        lineas.append(
            f"{marca} <b>{base}</b>  {f.rate:+.4f}%/8h  ({f.anual_pct:.0f}% anual)  "
            f"{lado}  ·  cubre coste en {f.dias_para_cubrir:.1f} días"
        )
    lineas.append("")
    lineas.append(
        f"<i>✅ = con {saldo:.0f} USDT el carry cubriría su coste en "
        f"≤{config.CARRY_MAX_DIAS_COBERTURA} días. El resto es interesante como "
        f"posicionamiento, no como negocio: funding muy positivo significa "
        f"largos amontonados pagando por no soltar, que es donde una cascada "
        f"encuentra combustible.</i>"
    )
    return "\n".join(lineas)


def sesgo(rate_pct: float) -> str:
    """
    Lectura de posicionamiento para acompañar una señal direccional.

    No cambia si se abre o no — es contexto para el diario. Un corto
    con funding muy positivo va A FAVOR del desequilibrio (los largos
    amontonados son los que sufren si el precio cae); un largo con
    funding muy positivo va en contra.
    """
    if rate_pct >= config.FUNDING_EXTREMO:
        return "largos amontonados"
    if rate_pct <= -config.FUNDING_EXTREMO:
        return "cortos amontonados"
    return "equilibrado"
