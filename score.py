"""
Puntuación de confianza (0-100) de una señal, construida a partir de
piezas que YA existen — no añade indicadores nuevos, combina en un
solo número lo que hasta ahora se mostraba disperso (RSI, cascada,
margen sobre los mínimos de R:R y cobertura de coste).

PARA QUÉ SIRVE, EXACTAMENTE:
  1. Ordenar el universo por calidad ANTES de escanear en busca de
     señal (ver main.py, _priority_order): con MAX_CONCURRENT limitado,
     el hueco libre debería llenarlo el mejor candidato disponible en
     el ciclo, no el primero que aparezca en el orden de la API.
  2. Se guarda junto a cada operación cerrada (state.data['trades']),
     para poder comprobar CON DATOS PROPIOS si el score predice algo
     de verdad — ver stats.buckets_por_score(). No se asume que sirva,
     se mide, con la misma disciplina que el resto del proyecto.
  3. SCORE_MIN, si se configura por encima de 0, es un umbral adicional
     y graduado — más fino que un simple sí/no de un único filtro.

QUÉ NO HACE: no sustituye ningún bloqueo existente. El filtro de
contra-tendencia de 30m sigue siendo un bloqueo DURO, no un matiz que
sume o reste puntos — el propio histórico del proyecto lo señaló como
la causa #1 de pérdidas, y eso no se diluye en una puntuación. Esto es
una capa por ENCIMA de lo que ya se decidió, para ordenar y medir.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import config
import liquidations
import oi_confirm
import rsi_confirm
import strategy


@dataclass
class EntryScore:
    total: float
    detalle: dict[str, float] = field(default_factory=dict)


def compute(
    sig: "strategy.Signal",
    rsi_result: "rsi_confirm.RsiConfirm | None",
    cascade: dict | None,
    bias30m: str | None,
    oi_dir: str | None = None,
) -> EntryScore:
    detalle: dict[str, float] = {}

    # Base: la señal ya pasó amplitud + ER + vela de agotamiento + R:R
    # mínimo — es la parte más validada del sistema (ver README), así
    # que arranca con la mayoría de los puntos ya en el bolsillo.
    detalle["base"] = 40.0

    # R:R por encima del mínimo exigido, hasta +15.
    exceso_rr = max(0.0, sig.rr - config.MIN_RR)
    detalle["r:r"] = min(15.0, exceso_rr * 15.0)

    # Cobertura de coste por encima del mínimo, hasta +15 — cuanto más
    # ATR cubre el coste de operar, menos pesa el slippage relativo.
    exceso_cover = max(0.0, sig.cost_cover - config.MIN_COST_COVER)
    detalle["cobertura"] = min(15.0, exceso_cover / config.MIN_COST_COVER * 15.0) if config.MIN_COST_COVER > 0 else 0.0

    # RSI: confirma (+15), contradice activamente (-10), o sin dato (0).
    if rsi_result is not None and rsi_result.señal_reciente is not None:
        detalle["rsi"] = 15.0 if rsi_confirm.confirms(sig.side, rsi_result) else -10.0
    else:
        detalle["rsi"] = 0.0

    # Cascada de liquidación confirmando la dirección.
    if cascade and cascade.get("activa") and liquidations.cascade_confirms(sig.side, cascade["lado"]):
        detalle["cascada"] = 15.0
    else:
        detalle["cascada"] = 0.0

    # Open Interest — ASIMÉTRICO A PROPÓSITO (ver oi_confirm.py para el
    # motivo completo, incluido el aviso de que el número de la
    # asimetría no está verificado de forma independiente). BUY con OI
    # cayendo (liquidación de largos) confirma; SELL con OI cayendo
    # (cobertura de cortos) NO suma nada, ni a favor ni en contra.
    # Peso menor (+10, no +15) porque es el componente más nuevo y
    # menos contrastado del score: hasta que stats.py no diga lo
    # contrario con datos propios, pesa menos que RSI o cascada.
    if config.OI_CONFIRM_ENABLED and sig.side == "BUY" and oi_confirm.confirms_buy(oi_dir):
        detalle["oi"] = 10.0
    else:
        detalle["oi"] = 0.0

    # El total se recorta a 100 — con todo maximizado (base+rr+cobertura
    # +rsi+cascada ya suman 100 antes de contar OI), el bonus de OI solo
    # se nota cuando algún otro componente no está al máximo. Es
    # deliberado: no se ha subido el techo para no diluir el peso
    # relativo de lo ya validado (RSI, cascada) frente a lo nuevo.
    total = max(0.0, min(100.0, sum(detalle.values())))
    return EntryScore(total=total, detalle=detalle)


def format_breakdown(score: EntryScore) -> str:
    partes = " · ".join(f"{k} {v:+.0f}" for k, v in score.detalle.items())
    return f"Score {score.total:.0f}/100 ({partes})"
