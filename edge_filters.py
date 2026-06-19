"""
QF×JP Bot — Edge Filters v1.0 (Turn-of-Candle + Slope Multi-TF)
═══════════════════════════════════════════════════════════════════════════════
FILTRO 1 — TURN OF THE CANDLE (timing boost, conservador)
─────────────────────────────────────────────────────────────────────────────
Shanaev, Vasenin & Stepanov (2023), Heliyon — "Turn-of-the-candle effect in
bitcoin returns": los retornos positivos se concentran desproporcionadamente
en los minutos 0, 15, 30 y 45 de cada hora (giros de vela de 15min). Efecto
con t-stat > 9 en 7 exchanges, persiste fuera de muestra.

⚠️ LIMITACIÓN HONESTA: el propio cuerpo de literatura académica (Caporale &
Plastun y otros) documenta que las anomalías encontradas en Bitcoin
GENERALMENTE NO SE MANTIENEN en otras criptomonedas. El estudio probó esto
en BTC en exchanges con fees casi cero (Bitfinex) — nuestro universo son
683 altcoins con fees normales. Por eso este filtro:
  - Aplica un boost PEQUEÑO (no decisivo) — nunca abre un trade por sí solo
  - Solo favorece LONG (el paper muestra sesgo positivo en los giros, no
    encontró el patrón simétrico inverso para SHORT)
  - Es un factor de CONFLUENCIA, no un trigger — actúa sobre señales que
    YA pasaron el resto de filtros del scanner

FILTRO 2 — SLOPE MULTI-TIMEFRAME (confirmación de tendencia real)
─────────────────────────────────────────────────────────────────────────────
La pendiente de regresión lineal mide la fuerza y dirección de la tendencia
de forma más estable que un cruce de medias — al considerar todos los puntos
del rango por igual, es menos propensa a whipsaws que indicadores basados en
EMA. Usar la pendiente en MÚLTIPLES timeframes simultáneamente (15m/1h/4h,
ya descargados por el scanner, sin coste de API adicional) da una vista en
capas de la salud de la tendencia: cuando las tres coinciden en dirección y
fuerza, hay "respaldo institucional" cruzando varios horizontes temporales.

Uso en el bot:
  - 3/3 timeframes alineados con la señal y con fuerza ≥MODERATE → +10 pts
  - 2/3 alineados → +5 pts
  - 1/3 o mixto → 0 (neutral)
  - 0/3 alineados Y la mayoría en contra con fuerza STRONG → BLOQUEA la
    señal — es la firma clásica de un whipsaw: entrar contra una tendencia
    establecida en varios timeframes a la vez.
═══════════════════════════════════════════════════════════════════════════════
"""
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import numpy as np

log = logging.getLogger("edge_filters")


# ── Turn of the Candle ──────────────────────────────────────────────────────

TURN_MINUTES = (0, 15, 30, 45)


def candle_turn_boost(
    direction: str,
    tolerance_min: int = 1,
    boost: float = 3.0,
    now: datetime | None = None,
) -> tuple[float, str]:
    """
    Boost pequeño y conservador para señales LONG que coinciden con el
    giro de una vela de 15 minutos (±tolerancia).

    Retorna (boost_pts, reason). boost_pts = 0.0 si no aplica.
    Solo aplica a LONG — el paper documenta sesgo positivo, no negativo.
    """
    if direction != "LONG":
        return 0.0, ""

    now = now or datetime.now(timezone.utc)
    minute = now.minute

    for turn in TURN_MINUTES:
        dist = min(abs(minute - turn), 60 - abs(minute - turn))
        if dist <= tolerance_min:
            return boost, f"turn_of_candle(min={minute}≈{turn})"

    return 0.0, ""


# ── Slope multi-timeframe ────────────────────────────────────────────────────

class SlopeStrength:
    FLAT     = "FLAT"
    MODERATE = "MODERATE"
    STRONG   = "STRONG"


@dataclass
class SlopeResult:
    pct_per_bar: float = 0.0     # % de cambio por barra (normalizado por precio)
    direction:   str   = "FLAT"  # "UP" / "DOWN" / "FLAT"
    strength:    str   = SlopeStrength.FLAT


def compute_slope(klines: list, lookback: int = 20) -> SlopeResult:
    """
    Calcula la pendiente de regresión lineal sobre los cierres de las
    últimas `lookback` velas, normalizada como % de cambio por barra
    (comparable entre símbolos de precio muy distinto).

    Umbrales de fuerza (calibrados de forma conservadora):
      |pct_per_bar| < 0.02%  → FLAT     (sin tendencia clara, mercado lateral)
      0.02% - 0.08%          → MODERATE
      > 0.08%                → STRONG
    """
    if not klines or len(klines) < 5:
        return SlopeResult()

    closes = np.array([c[4] for c in klines[-lookback:]], dtype=float)
    if len(closes) < 5 or closes.mean() <= 0:
        return SlopeResult()

    x = np.arange(len(closes))
    try:
        slope, _ = np.polyfit(x, closes, 1)
    except Exception:
        return SlopeResult()

    pct_per_bar = (slope / closes.mean()) * 100.0
    abs_pct     = abs(pct_per_bar)

    if abs_pct < 0.02:
        strength = SlopeStrength.FLAT
    elif abs_pct < 0.08:
        strength = SlopeStrength.MODERATE
    else:
        strength = SlopeStrength.STRONG

    direction = "UP" if pct_per_bar > 0 else ("DOWN" if pct_per_bar < 0 else "FLAT")

    return SlopeResult(pct_per_bar=pct_per_bar, direction=direction, strength=strength)


def multi_tf_slope_alignment(
    k15m: list, k1h: list, k4h: list,
    direction: str,
    lookback: int = 20,
) -> tuple[float, str, bool]:
    """
    Confluencia de pendiente en 3 timeframes (15m/1h/4h) — ya descargados
    por el scanner, sin coste de API adicional.

    Retorna (score_adjustment, reason, block_entry).

    block_entry=True cuando los 3 timeframes están alineados CONTRA la
    dirección de la señal con fuerza STRONG — la firma clásica de whipsaw:
    entrar contra una tendencia ya establecida en varios horizontes a la vez.
    """
    want_dir = "UP" if direction == "LONG" else "DOWN"
    against_dir = "DOWN" if direction == "LONG" else "UP"

    slopes = {
        "15m": compute_slope(k15m, lookback),
        "1h":  compute_slope(k1h,  lookback),
        "4h":  compute_slope(k4h,  lookback),
    }

    aligned = sum(
        1 for s in slopes.values()
        if s.direction == want_dir and s.strength != SlopeStrength.FLAT
    )
    against_strong = sum(
        1 for s in slopes.values()
        if s.direction == against_dir and s.strength == SlopeStrength.STRONG
    )

    detail = " | ".join(f"{tf}={s.direction}/{s.strength}" for tf, s in slopes.items())

    if against_strong >= 2:
        # Mayoría en contra con fuerza fuerte → bloquear (anti-whipsaw)
        return -10.0, f"slope_block({detail})", True

    if aligned == 3:
        return 10.0, f"slope_3of3({detail})", False
    if aligned == 2:
        return 5.0, f"slope_2of3({detail})", False
    return 0.0, "", False
