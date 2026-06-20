"""
QF×JP Bot — Price Action Framework (estilo Zero Complexity Trading)
═══════════════════════════════════════════════════════════════════════════
SOLO PARA MODE=SIGNAL por ahora — no comprometido a ningún bot en vivo.
Se prueba en paralelo a STC_ASYM_ENABLED / STC_VOL_SLOPE_ENABLED /
EMA_EXIT_ENABLED antes de decidir cuál(es) activar en LIVE y en qué bot.

Clasifica qué "edge" está activo en el precio reciente:
  MOMENTUM       → esperar continuación (los niveles se rompen)
  MEAN_REVERSION → esperar reversión (los niveles aguantan)
  NONE           → ningún patrón claro — "no trade" según el framework

4 patrones, cada uno activa un edge distinto:
  1. Large Bodies      (MOMENTUM)       — cuerpo de la última vela 2-3x el
     promedio reciente: una vela domina por completo, expansión vertical
     rápida. Trampa del framework: no tratarlo como "sobrecomprado".
  2. Wicks Into Levels (MEAN_REVERSION) — el precio supera un nivel
     reciente (máximo/mínimo de la ventana) pero CIERRA de vuelta dentro:
     invasión fallida, rechazo. Mecha más grande que el promedio reciente
     = rechazo más fuerte.
  3. Grindy Staircase  (MOMENTUM)       — máximos y mínimos crecientes
     (o decrecientes) en TODAS las velas de la ventana, sin pullback que
     rompa el patrón. El entorno de momentum de mayor probabilidad según
     el framework.
  4. Choppy/Range      (MEAN_REVERSION) — el mismo nivel (máximo o mínimo
     de la ventana) se toca y rechaza 3+ veces sin romper: "sin tendencia"
     es en sí mismo el patrón.

Se evalúan en este orden: wicks y large body primero (señales de UNA
vela, más frescas), luego staircase y choppy (estructurales, varias
velas). El primero que dispara gana. Si ninguno dispara → NONE.

Como filtro de confirmación sobre la señal que ya generó analyze():
  - NONE → no actúa (boost=0, no bloquea). Este framework no viendo nada
    claro no significa que analyze() esté equivocado — no penaliza por
    no confirmar, mismo principio que stc_asymmetry_filter.
  - Edge detectado A FAVOR de la dirección de la señal → boost fijo.
  - Edge detectado EN CONTRA → veto. No luchar contra el patrón
    dominante — mismo principio que slope_block / funding_regime ya
    aplican en scanner.py.

Reutiliza las velas del TIMEFRAME principal (k3m, ya fetcheadas en
scanner.py) — sin llamada extra a la API, a diferencia de los filtros
STC (que necesitan 1m específicamente).
═══════════════════════════════════════════════════════════════════════════
"""
import logging
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger("price_action")

MOMENTUM       = "MOMENTUM"
MEAN_REVERSION = "MEAN_REVERSION"
NONE_EDGE      = "NONE"


@dataclass
class PriceActionRead:
    edge:      str    # MOMENTUM / MEAN_REVERSION / NONE
    pattern:   str    # nombre del patrón que disparó, "" si NONE
    direction: str    # "UP" / "DOWN" / "NONE" — hacia dónde apunta el edge
    detail:    str    # texto descriptivo para logs


def _body(c) -> float:
    return abs(c[4] - c[1])


def _upper_wick(c) -> float:
    return c[2] - max(c[1], c[4])


def _lower_wick(c) -> float:
    return min(c[1], c[4]) - c[3]


def _is_green(c) -> bool:
    return c[4] > c[1]


# ── Patrón 1: Large Bodies (MOMENTUM) ───────────────────────────────────────

def _check_large_body(klines: list, lookback: int, body_mult: float) -> Optional[PriceActionRead]:
    if len(klines) < lookback + 1:
        return None
    recent_bodies = [_body(c) for c in klines[-(lookback + 1):-1]]
    avg_body = sum(recent_bodies) / len(recent_bodies) if recent_bodies else 0.0
    if avg_body <= 1e-12:
        return None
    last = klines[-1]
    body = _body(last)
    if body >= avg_body * body_mult:
        direction = "UP" if _is_green(last) else "DOWN"
        return PriceActionRead(
            edge=MOMENTUM, pattern="large_body", direction=direction,
            detail=f"body={body:.6f} vs avg={avg_body:.6f} ({body/avg_body:.1f}x)",
        )
    return None


# ── Patrón 2: Wicks Into Levels (MEAN_REVERSION) ────────────────────────────

def _check_wick_rejection(klines: list, lookback: int, wick_mult: float) -> Optional[PriceActionRead]:
    if len(klines) < lookback + 1:
        return None
    window = klines[-(lookback + 1):-1]
    level_high = max(c[2] for c in window)
    level_low  = min(c[3] for c in window)
    recent_wicks_up   = [_upper_wick(c) for c in window]
    recent_wicks_down = [_lower_wick(c) for c in window]
    avg_wick_up   = sum(recent_wicks_up)   / len(recent_wicks_up)   if recent_wicks_up   else 0.0
    avg_wick_down = sum(recent_wicks_down) / len(recent_wicks_down) if recent_wicks_down else 0.0

    last = klines[-1]
    # Invasión fallida por ARRIBA: high supera el nivel reciente, pero cierra debajo
    if last[2] > level_high and last[4] < level_high:
        wick = _upper_wick(last)
        if avg_wick_up <= 1e-12 or wick >= avg_wick_up * wick_mult:
            return PriceActionRead(
                edge=MEAN_REVERSION, pattern="wick_rejection_high", direction="DOWN",
                detail=f"high={last[2]:.6f} > nivel={level_high:.6f}, cierre={last[4]:.6f} dentro",
            )
    # Invasión fallida por ABAJO: low rompe el nivel reciente, pero cierra encima
    if last[3] < level_low and last[4] > level_low:
        wick = _lower_wick(last)
        if avg_wick_down <= 1e-12 or wick >= avg_wick_down * wick_mult:
            return PriceActionRead(
                edge=MEAN_REVERSION, pattern="wick_rejection_low", direction="UP",
                detail=f"low={last[3]:.6f} < nivel={level_low:.6f}, cierre={last[4]:.6f} dentro",
            )
    return None


# ── Patrón 3: Grindy Staircase (MOMENTUM) ───────────────────────────────────

def _check_staircase(klines: list, lookback: int) -> Optional[PriceActionRead]:
    if len(klines) < lookback:
        return None
    window = klines[-lookback:]
    hh_hl = all(window[i][2] > window[i - 1][2] and window[i][3] > window[i - 1][3]
                for i in range(1, len(window)))
    lh_ll = all(window[i][2] < window[i - 1][2] and window[i][3] < window[i - 1][3]
                for i in range(1, len(window)))
    if hh_hl:
        return PriceActionRead(
            edge=MOMENTUM, pattern="staircase_up", direction="UP",
            detail=f"{lookback} velas con HH+HL consecutivos, sin pullback que rompa el patrón",
        )
    if lh_ll:
        return PriceActionRead(
            edge=MOMENTUM, pattern="staircase_down", direction="DOWN",
            detail=f"{lookback} velas con LH+LL consecutivos, sin pullback que rompa el patrón",
        )
    return None


# ── Patrón 4: Choppy/Range (MEAN_REVERSION) ─────────────────────────────────

def _check_choppy_range(klines: list, lookback: int, touch_tol_pct: float,
                         min_touches: int) -> Optional[PriceActionRead]:
    if len(klines) < lookback:
        return None
    window = klines[-lookback:]
    highs = [c[2] for c in window]
    lows  = [c[3] for c in window]
    level_high = max(highs)
    level_low  = min(lows)
    rng = level_high - level_low
    if rng <= 1e-12:
        return None
    tol = rng * touch_tol_pct

    touches_high = sum(1 for h in highs if h >= level_high - tol)
    touches_low  = sum(1 for l in lows  if l <= level_low + tol)

    if touches_high >= min_touches and touches_low >= min_touches:
        return PriceActionRead(
            edge=MEAN_REVERSION, pattern="choppy_range", direction="NONE",
            detail=f"rango {level_low:.6f}-{level_high:.6f} rechazado "
                   f"{touches_high}x arriba / {touches_low}x abajo — sin ganador",
        )
    return None


# ── Clasificador principal ──────────────────────────────────────────────────

def classify_price_action(
    klines: list,
    lookback: int = 20,
    body_mult: float = 2.0,
    wick_mult: float = 1.5,
    touch_tol_pct: float = 0.1,
    min_touches: int = 3,
) -> PriceActionRead:
    """
    Evalúa los 4 patrones en orden: wicks y large body (1 vela, más
    frescos) primero, luego staircase y choppy (estructurales). El
    primero que dispara gana. Si ninguno dispara → NONE ("ninguno de
    los 4 está claro", según el framework).
    """
    for check in (
        lambda: _check_wick_rejection(klines, lookback, wick_mult),
        lambda: _check_large_body(klines, lookback, body_mult),
        lambda: _check_staircase(klines, lookback),
        lambda: _check_choppy_range(klines, lookback, touch_tol_pct, min_touches),
    ):
        result = check()
        if result is not None:
            return result
    return PriceActionRead(edge=NONE_EDGE, pattern="", direction="NONE",
                            detail="ninguno de los 4 patrones activo")


# ── Filtro de confirmación para scanner.py ──────────────────────────────────

def price_action_filter(
    klines: list,
    direction: str,
    lookback: int = 20,
    body_mult: float = 2.0,
    wick_mult: float = 1.5,
    touch_tol_pct: float = 0.1,
    min_touches: int = 3,
    boost_amount: float = 6.0,
) -> tuple[float, str, bool]:
    """
    Mismo contrato que los demás filtros de scanner.py: (boost_pts, reason, block).

    - Edge NONE, o el patrón detectado no apunta a una dirección concreta
      (choppy_range marca direction="NONE" — es una advertencia de "cuidado
      con rupturas aquí", no una dirección) → no actúa, boost=0, no bloquea.
    - Edge a FAVOR de la dirección de la señal → boost fijo (PA_BOOST_AMOUNT).
    - Edge EN CONTRA → veto, no luchar contra el patrón dominante.
    """
    read = classify_price_action(
        klines, lookback, body_mult, wick_mult, touch_tol_pct, min_touches
    )

    if read.edge == NONE_EDGE or read.direction == "NONE":
        return 0.0, f"price_action_none({read.detail})", False

    edge_dir = "LONG" if read.direction == "UP" else "SHORT"

    if edge_dir == direction:
        return boost_amount, f"price_action_{read.pattern}_confirma({read.detail})", False
    else:
        return 0.0, f"price_action_{read.pattern}_contradice({read.detail})", True
