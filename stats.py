"""
Análisis de rentabilidad a partir del registro de operaciones (R por
operación cerrada), no del contador simple wins/losses.

POR QUÉ NO BASTA CON EL WIN RATE: dos estrategias con el mismo % de
aciertos pueden ser una ganadora y otra perdedora según el tamaño de
los ganadores frente a los perdedores. Lo que decide si hay ventaja
es la ESPERANZA (expectancy) en R — la media de R por operación — no
el porcentaje de aciertos por sí solo.

POR QUÉ HACE FALTA EL INTERVALO DE CONFIANZA Y NO SOLO LA MEDIA: con
pocas operaciones, una media positiva puede ser pura suerte. Con la
varianza típica de esta familia de estrategias (reversión, R medio
moderado, dispersión alta — std≈1.2R es razonable de partida), hacen
falta del orden de 100+ operaciones para que el intervalo de
confianza al 95% deje de tocar cero. Por debajo de eso, el SIGNO de
la media todavía no es fiable, por prometedor que parezca el número.
Esto no es una opinión — es aritmética de error estándar
(σ/√n) aplicada a los datos reales del bot, symbol por símbolo no,
pero sí por régimen (SIGNAL vs LIVE, que tienen slippage distinto).
"""
from __future__ import annotations

import math
from dataclasses import dataclass


def compute_r(entry: float, sl: float, side: str, exit_price: float) -> float:
    """
    R conseguido en UNA operación ya cerrada, en múltiplos de lo
    arriesgado (distancia entrada-stop). Sirve igual para un cierre
    por TP, por SL o por tiempo — no hace falta saber la razón del
    cierre para calcularlo, solo el precio de salida real.
    """
    riesgo = abs(entry - sl)
    if riesgo <= 0:
        return 0.0
    direccion = 1.0 if side == "BUY" else -1.0
    return (exit_price - entry) * direccion / riesgo


@dataclass
class Verdict:
    n: int
    mean_r: float
    std_r: float
    se: float
    t_stat: float
    ci_low: float
    ci_high: float
    win_rate: float
    profit_factor: float
    max_drawdown_r: float
    etiqueta: str  # "insuficiente" | "indicios" | "fiable"


def _std(values: list[float], mean: float) -> float:
    if len(values) < 2:
        return 0.0
    var = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(var)


def analyse(rs: list[float]) -> Verdict | None:
    n = len(rs)
    if n == 0:
        return None
    mean = sum(rs) / n
    std = _std(rs, mean)
    se = std / math.sqrt(n) if n > 0 else 0.0
    t = mean / se if se > 0 else 0.0
    ci = 1.96 * se
    ganadoras = [r for r in rs if r > 0]
    perdedoras = [r for r in rs if r <= 0]
    win_rate = len(ganadoras) / n * 100.0
    suma_gan = sum(ganadoras)
    suma_perd = abs(sum(perdedoras))
    if suma_perd > 0:
        pf = suma_gan / suma_perd
    else:
        pf = float("inf") if suma_gan > 0 else 0.0

    # Drawdown máximo en R acumulado — no es dinero, es la peor racha
    # medida en el mismo múltiplo de riesgo que todo lo demás aquí.
    acumulado = 0.0
    pico = 0.0
    dd_max = 0.0
    for r in rs:
        acumulado += r
        pico = max(pico, acumulado)
        dd_max = max(dd_max, pico - acumulado)

    # El intervalo cruza cero: con estos mismos números, una estrategia
    # SIN ventaja real podría dar una media así solo por azar.
    if n < 50 or (mean - ci) < 0 < (mean + ci):
        etiqueta = "insuficiente"
    elif n < 100:
        etiqueta = "indicios"
    else:
        etiqueta = "fiable"

    return Verdict(
        n=n, mean_r=mean, std_r=std, se=se, t_stat=t,
        ci_low=mean - ci, ci_high=mean + ci,
        win_rate=win_rate, profit_factor=pf, max_drawdown_r=dd_max,
        etiqueta=etiqueta,
    )


def buckets_por_score(trades: list[dict]) -> str | None:
    """
    Responde a la pregunta que score.py deja abierta a propósito: ¿el
    score de entrada predice algo de verdad, o es una intuición que
    suena bien pero no se sostiene con datos? Se agrupan las
    operaciones cerradas por franja de score y se compara la
    expectativa de cada franja — si las franjas altas no rinden mejor
    que las bajas, el score no está aportando nada y hay que decirlo,
    no seguir usándolo por inercia.

    None si no hay al menos 5 operaciones con score guardado — las
    cerradas antes de tener score (campo ausente) se ignoran aquí sin
    romper nada, ya cuentan en el informe general de format_report().
    """
    con_score = [t for t in trades if t.get("score") is not None]
    if len(con_score) < 5:
        return None

    rangos = [(0.0, 40.0, "<40"), (40.0, 60.0, "40-60"), (60.0, 80.0, "60-80"), (80.0, 101.0, "80-100")]
    bloques: list[str] = []
    for lo, hi, etiqueta in rangos:
        rs = [float(t["r"]) for t in con_score if lo <= float(t["score"]) < hi]
        if not rs:
            continue
        v = analyse(rs)
        if v is None:
            continue
        pf = "∞" if v.profit_factor == float("inf") else f"{v.profit_factor:.2f}"
        bloques.append(f"Score {etiqueta} · n={v.n} · media {v.mean_r:+.2f}R · PF {pf}")

    if len(bloques) < 2:
        return None  # con una sola franja poblada no hay nada que comparar
    return "🎯 <b>¿El score predice algo?</b> (comparar franjas)\n\n" + "\n".join(bloques)
    """rs_por_modo: {'SIGNAL': [...], 'LIVE': [...]}. Se informan por
    separado a propósito: tienen slippage distinto y mezclarlos
    escondería justo la diferencia que el README avisa que va a doler."""
    etiquetas = {
        "insuficiente": "⚠️ muestra insuficiente — el intervalo cruza cero, podría ser azar",
        "indicios": "🟡 hay indicios, todavía no es concluyente",
        "fiable": "✅ muestra suficiente para confiar en el signo",
    }
    bloques: list[str] = []
    for modo in ("LIVE", "SIGNAL"):
        rs = rs_por_modo.get(modo)
        if not rs:
            continue
        v = analyse(rs)
        if v is None:
            continue
        bloques.append(
            f"<u>{modo}</u> · n={v.n}\n"
            f"Media: {v.mean_r:+.3f}R  ·  IC95%: [{v.ci_low:+.2f}, {v.ci_high:+.2f}]\n"
            f"Win rate: {v.win_rate:.0f}%  ·  Profit factor: {v.profit_factor:.2f}\n"
            f"Drawdown máx: {v.max_drawdown_r:.2f}R\n"
            f"{etiquetas[v.etiqueta]}"
        )
    if not bloques:
        return "📈 <b>Rentabilidad</b>: todavía no hay operaciones cerradas registradas."
    return "📈 <b>Rentabilidad — expectancy en R</b>\n\n" + "\n\n".join(bloques)
