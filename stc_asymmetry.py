"""
QF×JP Bot — STC + Asimetría de Precio (filtro de confirmación, timeframe 1m)
═══════════════════════════════════════════════════════════════════════════
⚠️ ASUNCIÓN SIN VERIFICAR — LEE ESTO ANTES DE ACTIVAR EN LIVE:
La fórmula de ASIMETRÍA aquí (magnitud media de vela alcista vs bajista en
una ventana reciente) es mi mejor interpretación de lo que muestra tu panel
"QF×JP v3.6 PREDATOR" en Pine Script (fila ASIMETRÍA: ▼ 1.53×) — no he
visto el código Pine real. compute_asymmetry() loguea/devuelve el mismo
formato (ratio + dirección) que el panel — compara el valor para el mismo
símbolo/vela contra lo que ves en TradingView. Si no coincide, pásame el
.pine y lo corrijo para que calce exacto.

QUÉ HACE:
  1. STC (Schaff Trend Cycle) sobre velas de 1 minuto — oscilador derivado
     de un MACD doblemente estocástico, más rápido/sensible que MACD para
     detectar giros de ciclo. Parámetros ESTÁNDAR de la literatura
     (length=10, fast=23, slow=50, factor=0.5, umbrales 25/75) — a
     propósito no inventados/ajustados para 1m sin testear primero, mismo
     criterio que ya aplicaste con CANDLE_TURN (validar antes de afinar).

  2. Asimetría de precio — ratio entre el tamaño medio de las velas
     alcistas vs bajistas en una ventana reciente (ASYM_WINDOW velas).

  3. Combinación — STC dice CUÁNDO (gira el ciclo desde sobreventa/
     sobrecompra), Asimetría dice si hay que CONFIAR en ese giro o no:
       - Asimetría fuerte EN CONTRA del giro → veto (no se lucha contra la
         presión direccional dominante — mismo principio que slope_block
         y funding_regime ya aplican).
       - Asimetría a FAVOR → boost de score proporcional al ratio.
       - STC no está girando → no hace nada (boost=0, no bloquea). Esto es
         un filtro de CONFIRMACIÓN sobre la señal que ya generó analyze(),
         no un trigger independiente.

INTEGRACIÓN: enganchado en scanner.py como filtro adicional (mismo patrón
que candle_turn_boost / multi_tf_slope_alignment) — no toca indicators.py
ni la función analyze() existente. Desactivado por defecto
(STC_ASYM_ENABLED=False) hasta que verifiques la fórmula de asimetría.
═══════════════════════════════════════════════════════════════════════════
"""
import logging

log = logging.getLogger("stc_asymmetry")


# ── STC (Schaff Trend Cycle) ────────────────────────────────────────────────

def _ema_series(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    k = 2.0 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(out[-1] + k * (v - out[-1]))
    return out


def _stoch_series(values: list[float], length: int) -> list[float]:
    """%K clásico: posición del valor actual dentro del rango [min,max] de `length` barras."""
    out: list[float] = []
    for i in range(len(values)):
        window = values[max(0, i - length + 1): i + 1]
        lo, hi = min(window), max(window)
        rng = hi - lo
        out.append(100.0 * (values[i] - lo) / rng if rng > 1e-12 else (out[-1] if out else 50.0))
    return out


def _smooth_series(values: list[float], factor: float) -> list[float]:
    """Suavizado exponencial recursivo: smoothed[i] = smoothed[i-1] + factor*(v[i]-smoothed[i-1])."""
    if not values:
        return []
    out = [values[0]]
    for v in values[1:]:
        out.append(out[-1] + factor * (v - out[-1]))
    return out


def compute_stc(closes: list[float], length: int = 10, fast: int = 23,
                 slow: int = 50, factor: float = 0.5) -> list[float]:
    """
    Schaff Trend Cycle estándar (Doug Schaff): doble estocástico sobre un
    MACD(fast,slow), cada paso suavizado exponencialmente. Devuelve la
    serie completa 0-100 — usar [-1] (actual) y [-2] (anterior) para
    detectar el giro de ciclo.

    Necesita al menos slow + length barras para que los valores dejen de
    ser ruido de arranque — con 100 velas de 1m y los defaults (50+10) hay
    margen de sobra.
    """
    if len(closes) < slow + length:
        return []

    ema_fast = _ema_series(closes, fast)
    ema_slow = _ema_series(closes, slow)
    macd = [f - s for f, s in zip(ema_fast, ema_slow)]

    k1 = _stoch_series(macd, length)
    d1 = _smooth_series(k1, factor)
    k2 = _stoch_series(d1, length)
    stc = _smooth_series(k2, factor)

    return stc


# ── Asimetría de precio ─────────────────────────────────────────────────────

def compute_asymmetry(klines: list, window: int = 20) -> tuple[float, str]:
    """
    Ratio de magnitud media de vela alcista vs bajista en las últimas
    `window` velas. klines: formato estándar del bot [ts, open, high, low,
    close, volume].

    Retorna (ratio, direction):
      direction="DOWN" → velas bajistas más grandes en promedio (ratio>=1)
      direction="UP"   → velas alcistas más grandes en promedio (ratio>=1)
      direction="NONE" → datos insuficientes / sin velas de un lado
    Ratio capado a 5.0 para evitar valores infinitos cuando un lado no
    tiene ninguna vela en la ventana.
    """
    if len(klines) < window:
        return 0.0, "NONE"

    recent = klines[-window:]
    up_bodies   = [c[4] - c[1] for c in recent if c[4] > c[1]]
    down_bodies = [c[1] - c[4] for c in recent if c[4] < c[1]]

    avg_up   = sum(up_bodies)   / len(up_bodies)   if up_bodies   else 0.0
    avg_down = sum(down_bodies) / len(down_bodies) if down_bodies else 0.0

    if avg_up <= 1e-12 and avg_down <= 1e-12:
        return 0.0, "NONE"
    if avg_down >= avg_up:
        ratio = (avg_down / avg_up) if avg_up > 1e-12 else 5.0
        return min(ratio, 5.0), "DOWN"
    else:
        ratio = (avg_up / avg_down) if avg_down > 1e-12 else 5.0
        return min(ratio, 5.0), "UP"


# ── Combinación STC + Asimetría ─────────────────────────────────────────────

def stc_asymmetry_filter(
    klines_1m: list,
    direction: str,
    stc_length: int = 10, stc_fast: int = 23, stc_slow: int = 50, stc_factor: float = 0.5,
    stc_oversold: float = 25.0, stc_overbought: float = 75.0,
    asym_window: int = 20, asym_veto_threshold: float = 1.5,
    asym_boost_per_x: float = 3.0, asym_boost_max: float = 12.0,
) -> tuple[float, str, bool]:
    """
    Filtro de confirmación STC+Asimetría para una señal YA generada por
    analyze() en `direction` (LONG/SHORT). Mismo contrato que los demás
    filtros de scanner.py (candle_turn_boost, multi_tf_slope_alignment):
    retorna (boost_pts, reason, block).

    No es un trigger independiente — solo actúa cuando STC está girando
    de ciclo EN la misma dirección que la señal; si no está girando,
    devuelve boost=0 sin bloquear (neutral, no penaliza por no confirmar).
    El único bloqueo real es la asimetría fuerte en contra de un giro que
    sí está ocurriendo.
    """
    closes = [c[4] for c in klines_1m]
    stc = compute_stc(closes, stc_length, stc_fast, stc_slow, stc_factor)
    if len(stc) < 2:
        return 0.0, "stc_insufficient_data", False

    stc_now, stc_prev = stc[-1], stc[-2]
    asym_ratio, asym_dir = compute_asymmetry(klines_1m, asym_window)

    # ── ¿Hay giro de ciclo en la dirección de la señal? ──────────────────────
    turning_up   = stc_prev < stc_oversold   and stc_now >= stc_oversold
    turning_down = stc_prev > stc_overbought and stc_now <= stc_overbought

    if direction == "LONG" and not turning_up:
        return 0.0, f"stc_no_turn(stc={stc_now:.1f})", False
    if direction == "SHORT" and not turning_down:
        return 0.0, f"stc_no_turn(stc={stc_now:.1f})", False

    # ── Veto de asimetría fuerte en contra del giro ──────────────────────────
    if direction == "LONG" and asym_dir == "DOWN" and asym_ratio >= asym_veto_threshold:
        return 0.0, f"asym_veto(DOWN {asym_ratio:.2f}x vs LONG)", True
    if direction == "SHORT" and asym_dir == "UP" and asym_ratio >= asym_veto_threshold:
        return 0.0, f"asym_veto(UP {asym_ratio:.2f}x vs SHORT)", True

    # ── Confirmación a favor → boost proporcional al ratio ───────────────────
    boost = 0.0
    if (direction == "LONG" and asym_dir == "UP") or (direction == "SHORT" and asym_dir == "DOWN"):
        boost = min(asym_ratio * asym_boost_per_x, asym_boost_max)

    reason = f"stc_turn({direction}) stc={stc_now:.1f} asym={asym_dir}{asym_ratio:.2f}x boost={boost:.1f}"
    return boost, reason, False


# ── Confirmación de volumen ──────────────────────────────────────────────────

def compute_volume_confirmation(klines: list, window: int = 20,
                                 recent_n: int = 3) -> tuple[float, bool]:
    """
    Ratio de volumen reciente (últimas `recent_n` velas, las del giro)
    contra el promedio de las `window` velas previas (línea base, sin
    solaparse con las recientes).

    ratio >= 1.0 = volumen normal o por encima — deseable como
    confirmación de un giro genuino. ratio bajo = posible fakeout de
    baja convicción, poca participación real detrás del movimiento.

    Retorna (ratio, datos_suficientes).
    """
    if len(klines) < window + recent_n:
        return 0.0, False

    baseline = klines[-(window + recent_n): -recent_n]
    recent   = klines[-recent_n:]
    if not baseline or not recent:
        return 0.0, False

    avg_baseline_vol = sum(c[5] for c in baseline) / len(baseline)
    avg_recent_vol   = sum(c[5] for c in recent) / len(recent)

    if avg_baseline_vol <= 1e-12:
        return 0.0, False

    return avg_recent_vol / avg_baseline_vol, True


# ── Combinación STC + Volumen + Slope ───────────────────────────────────────

def stc_volume_slope_filter(
    klines_1m: list,
    direction: str,
    slope_adj: float = 0.0,
    slope_block: bool = False,
    stc_length: int = 10, stc_fast: int = 23, stc_slow: int = 50, stc_factor: float = 0.5,
    stc_oversold: float = 25.0, stc_overbought: float = 75.0,
    vol_window: int = 20, vol_recent_n: int = 3, vol_min_ratio: float = 1.3,
    vol_boost_max: float = 8.0, slope_boost_mult: float = 0.5,
) -> tuple[float, str, bool]:
    """
    Filtro de confirmación STC + Volumen + Slope para una señal YA
    generada por analyze() en `direction`. Mismo contrato que los demás
    filtros (boost_pts, reason, block).

    slope_adj/slope_block: NO se recalculan aquí — se pasan ya calculados
    desde el paso 5 de scanner.py (multi_tf_slope_alignment), para no
    duplicar el cálculo ni arriesgarse a una versión distinta del slope
    que la que ya está validada en producción. Si SLOPE_FILTER_ENABLED
    está desactivado, scanner.py pasa (0.0, False) por defecto.

    Solo actúa cuando STC está girando de ciclo en la dirección de la
    señal (igual que stc_asymmetry_filter) — si no hay giro, boost=0 sin
    bloquear.

    Veta si:
      - slope_block ya era True (la propia multi_tf_slope_alignment
        decidió que hay tendencia fuerte en contra en HTF — se respeta).
      - El volumen del giro está por debajo de vol_min_ratio (posible
        fakeout de baja convicción).
    """
    closes = [c[4] for c in klines_1m]
    stc = compute_stc(closes, stc_length, stc_fast, stc_slow, stc_factor)
    if len(stc) < 2:
        return 0.0, "stc_insufficient_data", False

    stc_now, stc_prev = stc[-1], stc[-2]
    turning_up   = stc_prev < stc_oversold   and stc_now >= stc_oversold
    turning_down = stc_prev > stc_overbought and stc_now <= stc_overbought

    if direction == "LONG" and not turning_up:
        return 0.0, f"stc_no_turn(stc={stc_now:.1f})", False
    if direction == "SHORT" and not turning_down:
        return 0.0, f"stc_no_turn(stc={stc_now:.1f})", False

    # ── Slope: respeta el veto que ya calculó scanner.py paso 5 ───────────────
    if slope_block:
        return 0.0, "stc_turn_pero_slope_ya_bloqueo", True

    # ── Volumen: confirma participación real detrás del giro ─────────────────
    vol_ratio, vol_ok = compute_volume_confirmation(klines_1m, vol_window, vol_recent_n)
    if not vol_ok:
        return 0.0, "vol_insufficient_data", False
    if vol_ratio < vol_min_ratio:
        return 0.0, f"vol_bajo({vol_ratio:.2f}x < {vol_min_ratio}x) — posible fakeout", True

    # ── Boost combinado: volumen por encima de lo normal + slope a favor ─────
    vol_boost   = min(max(0.0, vol_ratio - 1.0) * 10, vol_boost_max)
    slope_extra = max(0.0, slope_adj) * slope_boost_mult  # solo si slope ya era favorable
    boost = vol_boost + slope_extra

    reason = (f"stc_turn({direction}) stc={stc_now:.1f} vol={vol_ratio:.2f}x "
              f"slope_adj={slope_adj:+.1f} boost={boost:.1f}")
    return boost, reason, False
