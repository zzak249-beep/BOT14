"""
QF×JP Bot — Sniper Predator x VSA Matrix (filtro de confirmación)
═══════════════════════════════════════════════════════════════════════════
Portado de "Sniper Predator x VSA Matrix V9" (estrategia Pine completa,
no solo indicador). Se porta la lógica de SEÑAL — Sniper Engine, VSA
Engine, y el sistema de Anticipación — como filtro de confirmación sobre
tu pipeline ya existente. El SL/TP/R:R propio del script (3:1, SL a 1.2
ATR, trailing a 1.5x riesgo) NO se porta — tu risk_manager.py ya tiene
sizing y gestión de riesgo validados; no se reemplazan con los de un
script de tercero sin testear.

COMPONENTES:

  1. Sniper Engine — confluencia completa:
     - Magic Slope: pendiente normalizada de EMA(5) sobre ATR corto(5).
     - "POC-lite": cierre de la vela con MÁS VOLUMEN en la ventana — OJO,
       esto NO es un Point of Control real (perfil de volumen por nivel
       de precio), es una aproximación más cruda. Se nombra distinto
       aquí (highest_volume_close) a propósito, para no confundir con
       POC de verdad si algún día se compara contra una fuente que sí lo
       calcula correctamente.
     - RVOL: volumen actual sobre su SMA(40).
     - Pivotes (peak/valley): barrido de liquidez bajo un soporte/sobre
       una resistencia reciente — el patrón "spring" cuando se combina
       con pendiente ya fuertemente a favor.
     - STC rápido (8,13,34) — variante de un solo estocástico sobre MACD,
       distinta de la versión de doble estocástico de stc_asymmetry.py
       (módulos independientes a propósito, no se comparte código entre
       filtros para no acoplar fragilidad).
     - ADX < 35 en ambas direcciones — caza el INICIO de un movimiento
       (slope ya fuerte) antes de que ADX confirme tendencia madura, no
       persigue tendencia ya consolidada.

  2. VSA Engine — Volume Spread Analysis con regresión:
     Ajusta una regresión lineal entre volumen normalizado y rango
     normalizado sobre vsa_lookback velas. Solo confía en la desviación
     de esa regresión (dev_filtered) cuando la pendiente es positiva Y
     la correlación |r| supera r_threshold — si esa relación volumen↔rango
     no se sostiene, anula la señal en vez de usarla ciegamente. Clasifica
     8 patrones VSA (SC, DT, SV, HB, BC, UT, EoM, SPR) según morfología de
     vela + desviación de la regresión.

     SIMPLIFICACIÓN PRAGMÁTICA: en el Pine original la regresión se
     recalcula CADA vela (rolling). Aquí se calcula UNA vez con las
     últimas vsa_lookback velas (snapshot), y se usa esa misma regresión
     para clasificar las últimas vsa_expiry+1 velas. Dado que la
     regresión es esencialmente un SMA de vsa_lookback velas (120 por
     defecto), cambia muy poco en una ventana de solo vsa_expiry velas
     (8 por defecto) — la diferencia frente a recalcularla en cada una es
     mínima, y evita computar una serie histórica completa que el filtro
     no necesita.

  3. Anticipación (pre-alerta): umbral de pendiente parcial + cruce de
     EMA micro(3) sobre EMA rápida + volumen creciente + ADX<40. Avisa
     1-2 velas antes de la confluencia completa — boost menor que la
     señal completa.

  NOTA: la condición original `close < vwap_val` (LONG) / `close > vwap_val`
  (SHORT) del Pine NO se porta — VWAP intradía requiere anclarse a un
  inicio de sesión que este sistema no trackea de la misma forma; se omite
  en vez de aproximarla mal. El resto de la confluencia (slope, STC, ADX,
  pivotes, POC-lite, RVOL) sí se porta completo.

Como filtro de confirmación:
  - Confluencia COMPLETA (Sniper + VSA permite esa dirección) → boost alto.
  - Solo Anticipación (sin confluencia completa) → boost menor.
  - Confluencia completa en la dirección CONTRARIA a la señal → veto.
  - Nada de lo anterior → neutral, no penaliza.

Reutiliza k3m (TIMEFRAME principal) — sin llamada extra a la API. Necesita
~200 velas para que vsa_lookback (120 por defecto) tenga margen.
═══════════════════════════════════════════════════════════════════════════
"""
import logging
import math

log = logging.getLogger("sniper_vsa")

BULLISH_VSA_PATTERNS = {"SC", "DT", "SV", "HB", "SPR", "EoM"}
BEARISH_VSA_PATTERNS = {"BC", "UT"}


# ── Helpers genéricos ────────────────────────────────────────────────────────

def _ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    k = 2.0 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(out[-1] + k * (v - out[-1]))
    return out


def _rma(values: list[float], period: int) -> list[float]:
    n = len(values)
    out = [0.0] * n
    if n == 0:
        return out
    alpha = 1.0 / period
    for i in range(n):
        out[i] = (sum(values[:i + 1]) / (i + 1)) if i < period else (out[i - 1] + alpha * (values[i] - out[i - 1]))
    return out


def _sma(values: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(values)):
        lo = max(0, i - period + 1)
        window = values[lo:i + 1]
        out.append(sum(window) / len(window))
    return out


def _true_range(klines: list) -> list[float]:
    tr = [klines[0][2] - klines[0][3]]
    for i in range(1, len(klines)):
        h, l, pc = klines[i][2], klines[i][3], klines[i - 1][4]
        tr.append(max(h - l, abs(h - pc), abs(l - pc)))
    return tr


def _atr(klines: list, period: int) -> list[float]:
    return _rma(_true_range(klines), period)


def _stoch(values: list[float], length: int) -> list[float]:
    out = []
    for i in range(len(values)):
        lo = max(0, i - length + 1)
        window = values[lo:i + 1]
        mn, mx = min(window), max(window)
        rng = mx - mn
        out.append(100.0 * (values[i] - mn) / rng if rng > 1e-12 else (out[-1] if out else 50.0))
    return out


def _stc_v9(closes: list[float], stoch_len: int = 8, fast: int = 13, slow: int = 34) -> list[float]:
    """
    Variante de UN SOLO estocástico sobre MACD — distinta del STC clásico
    de doble estocástico (ver stc_asymmetry.py, módulo independiente).
    """
    ema_fast = _ema(closes, fast)
    ema_slow = _ema(closes, slow)
    macd = [ema_fast[i] - ema_slow[i] for i in range(len(closes))]
    st = _stoch(macd, stoch_len)
    return _ema(st, 3)


def _dmi_adx(klines: list, period: int = 14) -> list[float]:
    n = len(klines)
    plus_dm  = [0.0] * n
    minus_dm = [0.0] * n
    for i in range(1, n):
        up_move   = klines[i][2] - klines[i - 1][2]
        down_move = klines[i - 1][3] - klines[i][3]
        plus_dm[i]  = up_move   if (up_move > down_move and up_move > 0)   else 0.0
        minus_dm[i] = down_move if (down_move > up_move and down_move > 0) else 0.0

    tr_rma       = _atr(klines, period)
    plus_dm_rma  = _rma(plus_dm, period)
    minus_dm_rma = _rma(minus_dm, period)

    plus_di  = [100 * plus_dm_rma[i]  / tr_rma[i] if tr_rma[i] > 1e-12 else 0.0 for i in range(n)]
    minus_di = [100 * minus_dm_rma[i] / tr_rma[i] if tr_rma[i] > 1e-12 else 0.0 for i in range(n)]
    dx = [
        100 * abs(plus_di[i] - minus_di[i]) / (plus_di[i] + minus_di[i])
        if (plus_di[i] + minus_di[i]) > 1e-12 else 0.0
        for i in range(n)
    ]
    return _rma(dx, period)


def _find_last_pivot(values: list[float], left: int, right: int, find_high: bool) -> float | None:
    """Último pivote CONFIRMADO en toda la serie — equivalente a la
    persistencia `var float peak/valley` del Pine original."""
    n = len(values)
    last = None
    for idx in range(left, n - right):
        window = values[idx - left:idx] + values[idx + 1:idx + right + 1]
        center = values[idx]
        if not window:
            continue
        if find_high and center > max(window):
            last = center
        elif not find_high and center < min(window):
            last = center
    return last


def _highest_volume_close(klines: list, lookback: int) -> float | None:
    """'POC-lite' — NO es Point of Control real, ver docstring del módulo."""
    window = klines[-lookback:] if len(klines) >= lookback else klines
    if not window:
        return None
    return max(window, key=lambda c: c[5])[4]


# ── VSA Engine ───────────────────────────────────────────────────────────────

def vsa_engine(
    klines: list, vsa_lookback: int = 120, r_threshold: float = 0.45,
    vsa_threshold: float = 0.85, vsa_expiry: int = 8,
) -> dict | None:
    n = len(klines)
    if n < vsa_lookback + vsa_expiry + 2:
        return None

    opens  = [c[1] for c in klines]
    highs  = [c[2] for c in klines]
    lows   = [c[3] for c in klines]
    closes = [c[4] for c in klines]
    vols   = [c[5] for c in klines]

    vsa_atr     = _atr(klines, vsa_lookback)
    vsa_vol_sma = _sma(vols, vsa_lookback)

    norm_range = [(highs[i] - lows[i]) / vsa_atr[i] if vsa_atr[i] > 1e-12 else 0.0 for i in range(n)]
    norm_vol   = [vols[i] / vsa_vol_sma[i] if vsa_vol_sma[i] > 1e-12 else 1.0 for i in range(n)]

    # Regresión: snapshot único sobre la ventana más reciente — ver
    # docstring del módulo sobre por qué esto es una aproximación honesta.
    x = norm_vol[-vsa_lookback:]
    y = norm_range[-vsa_lookback:]
    mean_x, mean_y = sum(x) / len(x), sum(y) / len(y)
    mean_xx = sum(v * v for v in x) / len(x)
    mean_xy = sum(x[i] * y[i] for i in range(len(x))) / len(x)
    std_x = math.sqrt(sum((v - mean_x) ** 2 for v in x) / len(x))
    std_y = math.sqrt(sum((v - mean_y) ** 2 for v in y) / len(y))

    denom = mean_xx - mean_x * mean_x
    slope_vsa = (mean_xy - mean_x * mean_y) / denom if abs(denom) > 1e-12 else 0.0
    intercept = mean_y - slope_vsa * mean_x
    r_denom = std_x * std_y
    r_val = (mean_xy - mean_x * mean_y) / r_denom if abs(r_denom) > 1e-12 else 0.0

    patterns = []
    for off in range(vsa_expiry, -1, -1):
        i = n - 1 - off
        if i < 0:
            continue
        pred_range   = intercept + slope_vsa * norm_vol[i]
        dev          = norm_range[i] - pred_range
        dev_filtered = 0.0 if (slope_vsa <= 0 or abs(r_val) < r_threshold) else dev

        bar_body, bar_range = abs(closes[i] - opens[i]), highs[i] - lows[i]
        up_wick  = highs[i] - max(opens[i], closes[i])
        low_wick = min(opens[i], closes[i]) - lows[i]
        up_wick_ratio  = up_wick  / bar_range if bar_range > 0 else 0.0
        low_wick_ratio = low_wick / bar_range if bar_range > 0 else 0.0
        body_ratio     = bar_body / bar_range if bar_range > 0 else 0.0

        is_bullish, is_bearish = closes[i] > opens[i], closes[i] < opens[i]
        is_wide, is_narrow     = norm_range[i] > 1.4, norm_range[i] < 0.6
        is_high_vol, is_low_vol = norm_vol[i] > 1.4, norm_vol[i] < 0.7
        has_long_low_wick = low_wick_ratio > 0.35
        has_long_up_wick  = up_wick_ratio > 0.35
        is_doji = body_ratio < 0.2
        close_upper_half = (closes[i] - lows[i]) / bar_range > 0.6 if bar_range > 0 else False

        signal_neg = dev_filtered < -vsa_threshold
        signal_pos = dev_filtered >  vsa_threshold

        ptype = None
        if   signal_neg and is_bearish and is_wide and is_high_vol: ptype = "SC"
        elif signal_neg and (is_doji or is_narrow) and is_high_vol: ptype = "DT"
        elif signal_neg and is_bearish and is_high_vol and close_upper_half: ptype = "SV"
        elif signal_neg and is_bullish and is_high_vol: ptype = "HB"
        elif signal_pos and is_bullish and is_wide and is_high_vol and not close_upper_half: ptype = "BC"
        elif signal_pos and has_long_up_wick and is_high_vol: ptype = "UT"
        elif signal_pos and is_wide and is_low_vol and is_bullish: ptype = "EoM"
        elif signal_pos and has_long_low_wick and is_high_vol and is_bullish: ptype = "SPR"

        if ptype:
            patterns.append((off, ptype))

    if not patterns:
        return {"pattern": None, "age": None, "r_val": r_val, "slope_vsa": slope_vsa}

    age, ptype = min(patterns, key=lambda p: p[0])
    return {"pattern": ptype, "age": age, "r_val": r_val, "slope_vsa": slope_vsa}


def vsa_allows(direction: str, vsa_result: dict | None, vsa_expiry: int) -> bool:
    if not vsa_result or vsa_result.get("pattern") is None:
        return False
    if vsa_result["age"] > vsa_expiry:
        return False
    p = vsa_result["pattern"]
    return p in BULLISH_VSA_PATTERNS if direction == "LONG" else p in BEARISH_VSA_PATTERNS


# ── Sniper Engine + Anticipación ────────────────────────────────────────────

def sniper_engine(
    klines: list,
    pivot_len: int = 3, slope_min: float = 25.0, poc_lookback: int = 40,
    rvol_threshold: float = 1.3, ema_fast_len: int = 5, ema_slow_len: int = 13,
    pre_slope_pct: float = 0.6, pre_stc_bars: int = 2,
) -> dict:
    """Retorna toda la info de confluencia (completa + anticipación) del último bar."""
    n = len(klines)
    closes = [c[4] for c in klines]
    highs  = [c[2] for c in klines]
    lows   = [c[3] for c in klines]
    vols   = [c[5] for c in klines]

    atr_5  = _atr(klines, 5)
    ema_fast  = _ema(closes, ema_fast_len)
    ema_slow  = _ema(closes, ema_slow_len)

    magic_slope = [0.0] * n
    for i in range(1, n):
        magic_slope[i] = ((ema_fast[i] - ema_fast[i - 1]) / atr_5[i] * 100) if atr_5[i] > 1e-12 else 0.0

    stc = _stc_v9(closes)
    adx = _dmi_adx(klines, 14)

    vol_avg = _sma(vols, 40)
    rvol = vols[-1] / vol_avg[-1] if vol_avg[-1] > 1e-12 else 1.0

    atr_14 = _atr(klines, 14)
    poc_level = _highest_volume_close(klines, poc_lookback)
    distancia_poc = abs(closes[-1] - poc_level) > atr_14[-1] * 1.2 if poc_level is not None else False

    valley = _find_last_pivot(lows,  pivot_len, pivot_len, find_high=False)
    peak   = _find_last_pivot(highs, pivot_len, pivot_len, find_high=True)

    ema_trend_long = ema_fast[-1] > ema_slow[-1]
    ema_trend_sht  = ema_fast[-1] < ema_slow[-1]
    cond_vol = rvol > rvol_threshold

    stc_rising  = len(stc) > pre_stc_bars and stc[-1] > stc[-1 - pre_stc_bars]
    stc_falling = len(stc) > pre_stc_bars and stc[-1] < stc[-1 - pre_stc_bars]

    # NOTA: el filtro original de VWAP (close < vwap_val / close > vwap_val)
    # se omite a propósito — ver docstring del módulo.
    sniper_long = (
        valley is not None and lows[-1] < valley
        and magic_slope[-1] > slope_min and len(stc) > 1 and stc[-1] > stc[-2]
        and adx[-1] < 35 and distancia_poc and cond_vol and ema_trend_long
    )
    sniper_short = (
        peak is not None and highs[-1] > peak
        and magic_slope[-1] < -slope_min and len(stc) > 1 and stc[-1] < stc[-2]
        and adx[-1] < 35 and distancia_poc and cond_vol and ema_trend_sht
    )

    # ── Anticipación ──────────────────────────────────────────────────────────
    pre_slope_long  = magic_slope[-1] >  slope_min * pre_slope_pct
    pre_slope_short = magic_slope[-1] < -slope_min * pre_slope_pct
    vol_building = n > 2 and vols[-1] > vols[-2] and vols[-1] > vols[-3]

    pre_alert_long  = ema_trend_long and pre_slope_long  and stc_rising  and vol_building and adx[-1] < 40
    pre_alert_short = ema_trend_sht  and pre_slope_short and stc_falling and vol_building and adx[-1] < 40

    return {
        "sniper_long": sniper_long, "sniper_short": sniper_short,
        "pre_alert_long": pre_alert_long, "pre_alert_short": pre_alert_short,
        "magic_slope": magic_slope[-1], "rvol": rvol, "adx": adx[-1],
    }


# ── Filtro de confirmación para scanner.py ──────────────────────────────────

def sniper_vsa_filter(
    klines: list, direction: str,
    pivot_len: int = 3, slope_min: float = 25.0, poc_lookback: int = 40,
    rvol_threshold: float = 1.3, ema_fast_len: int = 5, ema_slow_len: int = 13,
    pre_slope_pct: float = 0.6, pre_stc_bars: int = 2,
    vsa_lookback: int = 120, r_threshold: float = 0.45,
    vsa_threshold: float = 0.85, vsa_expiry: int = 8,
    boost_full: float = 10.0, boost_pre_alert: float = 4.0,
) -> tuple[float, str, bool]:
    """Mismo contrato que los demás filtros (boost_pts, reason, block)."""
    min_bars = max(vsa_lookback + vsa_expiry + 2, poc_lookback, 60)
    if len(klines) < min_bars:
        return 0.0, "sniper_vsa_insufficient_data", False

    eng = sniper_engine(
        klines, pivot_len, slope_min, poc_lookback, rvol_threshold,
        ema_fast_len, ema_slow_len, pre_slope_pct, pre_stc_bars,
    )
    vsa = vsa_engine(klines, vsa_lookback, r_threshold, vsa_threshold, vsa_expiry)

    full_long  = eng["sniper_long"]  and vsa_allows("LONG",  vsa, vsa_expiry)
    full_short = eng["sniper_short"] and vsa_allows("SHORT", vsa, vsa_expiry)

    if direction == "LONG":
        if full_long:
            return boost_full, f"sniper_vsa_full(LONG) slope={eng['magic_slope']:.1f} vsa={vsa['pattern']}", False
        if full_short:
            return 0.0, "sniper_vsa_contradice(confluencia SHORT activa)", True
        if eng["pre_alert_long"]:
            return boost_pre_alert, f"sniper_pre_alert(LONG) slope={eng['magic_slope']:.1f}", False
        return 0.0, "sniper_vsa_sin_confluencia", False
    else:
        if full_short:
            return boost_full, f"sniper_vsa_full(SHORT) slope={eng['magic_slope']:.1f} vsa={vsa['pattern']}", False
        if full_long:
            return 0.0, "sniper_vsa_contradice(confluencia LONG activa)", True
        if eng["pre_alert_short"]:
            return boost_pre_alert, f"sniper_pre_alert(SHORT) slope={eng['magic_slope']:.1f}", False
        return 0.0, "sniper_vsa_sin_confluencia", False
