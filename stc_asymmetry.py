"""
STC Asymmetry Module v2.0 — Schaff Trend Cycle (80-27-50)
═══════════════════════════════════════════════════════════════════════════════
Cambio de params vs v1.x:
  Antes: length=10, fast=12, slow=26 (reactivo, señal rápida → mucho ruido)
  Ahora: length=80, fast=27, slow=50 (ciclo largo → filtro de régimen de 4H)

  En velas de 3m:
    80 barras × 3m = 240 min = 4H de ciclo efectivo
    Esto convierte el STC en un filtro de tendencia de medio plazo, no un
    oscilador de scalping. Su función ahora es filtrar el RÉGIMEN, no generar
    señales de entrada.

  Zonas:
    STC > 75 → tendencia alcista confirmada (boost +5 a señales LONG)
    STC < 25 → tendencia bajista confirmada (boost +5 a señales SHORT,
               penaliza LONG −8 para no nadar contra la corriente)
    STC 25-75 → mercado lateral/transición (penaliza −3 todas las señales)

  Asimetría (la razón del nombre del módulo):
    En régimen alcista (STC>75): el boost LONG es mayor que el boost SHORT
    En régimen bajista (STC<25): el boost SHORT es mayor, LONG penalizado
    El sistema no es simétrico porque los mercados de crypto no lo son
    (drawdowns más rápidos que rallies, colas asimétricas).

Interface con scanner.py:
    from stc_asymmetry import get_stc_signal
    result = get_stc_signal(klines_3m, direction="LONG")
    score += result["score_boost"]
    if result["blocks_direction"]: return  # filtro duro en laterales extremos
═══════════════════════════════════════════════════════════════════════════════
"""
import logging

log = logging.getLogger("stc_asym")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ema(values: list, period: int) -> list:
    if not values:
        return []
    k = 2.0 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(out[-1] + k * (v - out[-1]))
    return out


def _stc_series(closes: list,
                length: int = 80,
                fast: int   = 27,
                slow: int   = 50,
                factor: float = 0.5) -> list:
    """
    Schaff Trend Cycle — doble estocástico suavizado del MACD.

    Algoritmo idéntico al Pine Script de referencia:
      1. MACD   = EMA(close, fast) - EMA(close, slow)
      2. Stoch1 = Estocástico de MACD en ventana `length`, suavizado
                  exponencialmente con factor 0.5 (no una EMA estándar)
      3. STC    = Estocástico de Stoch1 en ventana `length`, suavizado igual

    El suavizado exponencial con factor fijo (DDD = DDD[-1] + 0.5*(raw - DDD[-1]))
    es distinto de una EMA estándar — es un IIR de primer orden con alpha=0.5.
    """
    n = len(closes)
    if n < 2:
        return [50.0] * n

    # Paso 1: MACD
    fe = _ema(closes, fast)
    se = _ema(closes, slow)
    macd = [f - s for f, s in zip(fe, se)]

    # Paso 2: primer estocástico del MACD con suavizado IIR
    s1 = [50.0] * n
    for i in range(n):
        lo   = max(0, i - length + 1)
        win  = macd[lo:i + 1]
        low  = min(win)
        high = max(win)
        rng  = high - low
        raw  = (macd[i] - low) / rng * 100 if rng > 1e-12 else (s1[i - 1] if i > 0 else 50.0)
        s1[i] = (s1[i - 1] + factor * (raw - s1[i - 1])) if i > 0 else raw

    # Paso 3: segundo estocástico de s1 con suavizado IIR → STC final
    stc = [50.0] * n
    for i in range(n):
        lo   = max(0, i - length + 1)
        win  = s1[lo:i + 1]
        low  = min(win)
        high = max(win)
        rng  = high - low
        raw  = (s1[i] - low) / rng * 100 if rng > 1e-12 else (stc[i - 1] if i > 0 else 50.0)
        stc[i] = (stc[i - 1] + factor * (raw - stc[i - 1])) if i > 0 else raw

    return stc


# ── Interfaz principal ────────────────────────────────────────────────────────

def get_stc_signal(
    klines:    list,
    direction: str   = "LONG",
    length:    int   = 80,
    fast:      int   = 27,
    slow:      int   = 50,
    factor:    float = 0.5,
    bull_thr:  float = 75.0,   # STC > bull_thr → régimen alcista
    bear_thr:  float = 25.0,   # STC < bear_thr → régimen bajista
) -> dict:
    """
    Calcula el STC y devuelve el ajuste de score para la dirección dada.

    Args:
        klines:    lista de klines (cada elemento: [ts, open, high, low, close, vol])
        direction: "LONG" o "SHORT" — la dirección de la señal a evaluar
        length:    periodo del estocástico (default 80)
        fast:      EMA rápida del MACD (default 27)
        slow:      EMA lenta del MACD (default 50)
        factor:    factor de suavizado IIR (default 0.5)
        bull_thr:  umbral de régimen alcista (default 75)
        bear_thr:  umbral de régimen bajista (default 25)

    Returns:
        dict con:
          stc:             valor actual del STC (0-100)
          stc_prev:        valor del STC en la barra anterior
          regime:          'BULL' | 'BEAR' | 'NEUTRAL'
          rising:          bool (STC subiendo)
          score_boost:     puntos a añadir al score (puede ser negativo)
          blocks_direction: bool — True si el STC contradice fuertemente la dirección
          label:           string descriptivo para logs
    """
    min_bars = slow + length
    if len(klines) < min_bars:
        log.debug("STC: insuficientes barras (%d < %d) — neutral", len(klines), min_bars)
        return {
            "stc": 50.0, "stc_prev": 50.0, "regime": "NEUTRAL",
            "rising": True, "score_boost": 0.0,
            "blocks_direction": False,
            "label": f"STC=50.0 (insuficiente, min={min_bars}bars)",
        }

    closes = [c[4] for c in klines]
    stc_vals = _stc_series(closes, length, fast, slow, factor)

    stc_now  = stc_vals[-1]
    stc_prev = stc_vals[-2] if len(stc_vals) > 1 else stc_now
    rising   = stc_now > stc_prev

    # ── Régimen y boost asimétrico ────────────────────────────────────────────
    if stc_now > bull_thr:
        regime = "BULL"
        if direction == "LONG":
            # Tendencia alcista + señal alcista = máximo boost
            boost = 6.0 if rising else 3.0
        else:
            # SHORT contra tendencia alcista = penalización
            boost = -8.0
        blocks = direction == "SHORT" and stc_now > 85

    elif stc_now < bear_thr:
        regime = "BEAR"
        if direction == "SHORT":
            # Tendencia bajista + señal bajista = boost moderado
            boost = 5.0 if not rising else 2.0
        else:
            # LONG contra tendencia bajista = penalización
            boost = -8.0
        blocks = direction == "LONG" and stc_now < 15

    else:
        # STC en zona neutral (25-75): mercado sin dirección clara
        regime = "NEUTRAL"
        # Penalización ligera para ambas direcciones — evita operar en lateral
        boost  = -3.0
        blocks = False

    label = (
        f"STC={stc_now:.1f} "
        f"({'↑' if rising else '↓'}) "
        f"regime={regime} "
        f"boost={boost:+.0f} "
        f"{'⛔BLOCK' if blocks else ''}"
    )

    log.debug("[stc_asym] %s | dir=%s", label, direction)

    return {
        "stc":              round(stc_now, 2),
        "stc_prev":         round(stc_prev, 2),
        "regime":           regime,
        "rising":           rising,
        "score_boost":      boost,
        "blocks_direction": blocks,
        "label":            label,
    }


# ── Uso mínimo en scanner.py ──────────────────────────────────────────────────
#
# INTEGRACIÓN EN scanner.py (reemplaza las llamadas al módulo anterior):
#
#   from stc_asymmetry import get_stc_signal
#
#   # Dentro de analyze() o _score_signal(), tras tener klines:
#   stc = get_stc_signal(klines_3m, direction=direction)
#
#   if stc["blocks_direction"]:
#       return None, "stc_block"  # filtro duro — STC extremo contra la señal
#
#   score += stc["score_boost"]
#   log.debug("[%s] %s", symbol, stc["label"])
#
# ── Si scanner.py usaba score_stc_asymmetry() antes ─────────────────────────
# La función anterior probablemente devolvía solo un float (el boost).
# Ahora devuelve un dict — actualizar el caller:
#   boost = stc["score_boost"]  # en vez de: boost = score_stc_asymmetry(...)
#
# O añadir este alias para compatibilidad sin cambiar scanner.py:

def score_stc_asymmetry(klines: list, direction: str = "LONG") -> float:
    """Alias de compatibilidad — devuelve solo el score_boost como float."""
    return get_stc_signal(klines, direction)["score_boost"]


# ── Funciones requeridas por scanner.py ──────────────────────────────────────
# scanner.py hace: from stc_asymmetry import stc_asymmetry_filter, stc_volume_slope_filter
# Estas funciones deben existir con exactamente estos nombres.

def stc_asymmetry_filter(klines: list, direction: str = "LONG") -> dict:
    """
    Filtro STC asimétrico para scanner.py.

    Interfaz compatible con la versión anterior:
      result = stc_asymmetry_filter(klines, direction)
      score  += result["score_boost"]
      if result["blocks"]: return None, "stc_block"

    Cambio vs v1.x: usa params 80-27-50 en vez de 10-12-26.
    El STC ahora actúa como filtro de régimen de 4H, no como oscilador rápido.
    """
    sig = get_stc_signal(klines, direction)
    return {
        "score_boost":  sig["score_boost"],
        "blocks":       sig["blocks_direction"],
        "regime":       sig["regime"],
        "stc":          sig["stc"],
        "label":        sig["label"],
        # Compat con campos que scanner.py pueda leer directamente
        "bull":         sig["regime"] == "BULL",
        "bear":         sig["regime"] == "BEAR",
        "neutral":      sig["regime"] == "NEUTRAL",
    }


def stc_volume_slope_filter(klines: list, direction: str = "LONG",
                             vol_ma_period: int = 20) -> dict:
    """
    Filtro combinado STC + confirmación de volumen para scanner.py.

    Añade un boost adicional cuando el volumen de la última vela está
    por encima de su media — confirma que el movimiento tiene liquidez real.
    El STC sigue siendo el filtro principal de régimen.

    Interfaz compatible:
      result = stc_volume_slope_filter(klines, direction)
      score  += result["score_boost"]
      if result["blocks"]: return None, "stc_vol_block"
    """
    if not klines:
        return {"score_boost": 0.0, "blocks": False, "label": "no_data"}

    # Base: STC asimétrico
    base = stc_asymmetry_filter(klines, direction)
    boost = base["score_boost"]

    # Confirmación de volumen (sin importar librerías)
    try:
        volumes = [c[5] for c in klines if len(c) > 5 and c[5] > 0]
        if len(volumes) >= vol_ma_period + 1:
            vol_now = volumes[-1]
            vol_ma  = sum(volumes[-vol_ma_period - 1:-1]) / vol_ma_period
            if vol_ma > 0:
                vol_ratio = vol_now / vol_ma
                if vol_ratio > 1.5:
                    # Volumen alto — confirma momentum
                    vol_boost = 3.0 if not base["blocks"] else 0.0
                    boost += vol_boost
                elif vol_ratio < 0.7:
                    # Volumen bajo — señal débil
                    boost -= 2.0
    except Exception:
        pass   # si falla el cálculo de volumen, usar solo el STC

    label = f"{base['label']} vol_adj={boost - base['score_boost']:+.0f}"

    return {
        "score_boost": round(boost, 1),
        "blocks":      base["blocks"],
        "regime":      base["regime"],
        "stc":         base["stc"],
        "label":       label,
        "bull":        base["bull"],
        "bear":        base["bear"],
        "neutral":     base["neutral"],
    }


# ── Test rápido ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import math
    # Generar una serie sintética con tendencia alcista
    closes = [100 + i * 0.5 + math.sin(i / 10) * 2 for i in range(200)]
    fake_klines = [[i, c - 0.5, c + 1, c - 1, c, 1000] for i, c in enumerate(closes)]
    result = get_stc_signal(fake_klines, direction="LONG")
    print("Test LONG en tendencia alcista:")
    for k, v in result.items():
        print(f"  {k}: {v}")

    closes_down = [200 - i * 0.5 + math.sin(i / 10) * 2 for i in range(200)]
    fake_klines_down = [[i, c - 0.5, c + 1, c - 1, c, 1000] for i, c in enumerate(closes_down)]
    result2 = get_stc_signal(fake_klines_down, direction="LONG")
    print("\nTest LONG en tendencia bajista:")
    for k, v in result2.items():
        print(f"  {k}: {v}")
