"""
QF×JP Bot — Fibonacci Retracement MTF (filtro de confirmación)
═══════════════════════════════════════════════════════════════════════════
Portado de "Fibonacci Retracement MTF/LOG". Usa el día COMPLETO anterior
(no el día en curso, que todavía se está formando) como referencia — da
niveles estables durante toda la sesión actual en vez de niveles que se
mueven cada vez que aparece un nuevo extremo intradía.

DIRECCIÓN — limitación honesta de datos: el script Pine original determina
si el mínimo ocurrió ANTES o DESPUÉS del máximo dentro del día rastreando
timestamps intrabar. Aquí solo se tienen velas DIARIAS (un solo high y un
solo low por día, sin saber cuál ocurrió primero). Se usa una heurística
razonable en su lugar: si el cierre quedó en la mitad superior del rango
del día (cerca del high), se asume que el low ocurrió primero y luego
subió (día alcista, dir=1); si cerró en la mitad inferior, se asume lo
contrario (día bajista, dir=-1). No es una réplica exacta del indicador
visual, es una aproximación defendible — documentada como tal, no como
hecho.

ZONA DORADA: 61.8%-78.6% de retroceso — la franja donde clásicamente se
espera reacción del precio en un pullback. Si el precio actual cae en esa
zona Y la dirección de la señal coincide con lo que el día anterior
sugiere (día alcista → zona actúa de soporte → favorece LONG; día bajista
→ zona actúa de resistencia → favorece SHORT) → confirma. Si la señal va
en contra de esa lectura → veta, mismo principio que slope_block.

Necesita una llamada extra a la API (klines diarias) — no reutiliza k3m,
ya que necesita un timeframe distinto (1D) que el scanner no fetchea por
defecto.
═══════════════════════════════════════════════════════════════════════════
"""
import logging

log = logging.getLogger("fib_mtf")

FIB_RATIOS = (0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0)


def _infer_day_direction(day_candle: list) -> int:
    """+1 = día alcista asumido (low antes que high), -1 = bajista asumido."""
    o, h, l, c = day_candle[1], day_candle[2], day_candle[3], day_candle[4]
    rng = h - l
    if rng <= 1e-12:
        return 1
    close_pos = (c - l) / rng
    return 1 if close_pos >= 0.5 else -1


def compute_fib_levels(day_candle: list) -> tuple[dict, int]:
    h, l = day_candle[2], day_candle[3]
    direction = _infer_day_direction(day_candle)
    y1, y2 = (l, h) if direction == 1 else (h, l)
    levels = {k: y1 * k + y2 * (1 - k) for k in FIB_RATIOS}
    return levels, direction


def fib_mtf_filter(
    daily_klines: list, current_price: float, direction: str,
    lookback_days: int = 1, golden_zone: tuple[float, float] = (0.618, 0.786),
    tolerance_pct: float = 0.3, boost_amount: float = 6.0,
) -> tuple[float, str, bool]:
    """Mismo contrato que los demás filtros (boost_pts, reason, block)."""
    if len(daily_klines) < lookback_days + 1:
        return 0.0, "fib_insufficient_data", False

    day = daily_klines[-(lookback_days + 1)]
    levels, day_dir = compute_fib_levels(day)

    lo_k, hi_k = min(golden_zone), max(golden_zone)
    lo_price, hi_price = levels[lo_k], levels[hi_k]
    if lo_price > hi_price:
        lo_price, hi_price = hi_price, lo_price

    full_range = abs(levels[1.0] - levels[0.0])
    tol = full_range * (tolerance_pct / 100.0)

    in_golden_zone = (lo_price - tol) <= current_price <= (hi_price + tol)
    if not in_golden_zone:
        return 0.0, "fib_fuera_de_zona_dorada", False

    expected_dir = "LONG" if day_dir == 1 else "SHORT"
    if direction == expected_dir:
        return (boost_amount,
                f"fib_zona_dorada_confirma({direction}, dia_dir={day_dir:+d})",
                False)
    return (0.0,
            f"fib_zona_dorada_contradice(dia_dir={day_dir:+d} favorece {expected_dir})",
            True)
