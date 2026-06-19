"""
QF×JP Bot — Volatility Regime Engine v1.0
═══════════════════════════════════════════════════════════════════════════════
EL EDGE: la mayoría de bots retail usan RISK_PCT fijo y SL_ATR_MULT fijo sin
importar si el símbolo se mueve 1%/día o 8%/día. Eso significa que el riesgo
REAL asumido varía enormemente entre símbolos aunque el % de capital parezca
constante.

VOLATILITY TARGETING (lo que usan los profesionales):
  Ajustar el tamaño de posición de forma INVERSA al ATR% mantiene una
  exposición de riesgo constante real, no solo nominal.
  Símbolo tranquilo (ATR% bajo)  → posición más grande, mismo riesgo €
  Símbolo volátil  (ATR% alto)   → posición más pequeña, mismo riesgo €

RÉGIMEN POR PERCENTIL DE ATR (no por valor absoluto):
  Cada símbolo tiene su propia "personalidad" de volatilidad. Comparamos el
  ATR actual contra SU PROPIA historia reciente (percentil), no contra un
  umbral fijo que no tiene sentido para 683 símbolos distintos.

  COMPRESSED (percentil <20%): mercado muy calmado.
    → Las rupturas tras compresión suelen ser explosivas (3-5x el rango
      normal). SL ajustado, TP más cercano, position size normal/alto.
  NORMAL (20-70%): condiciones estándar. Sin ajustes.
  EXPANDED (70-90%): volatilidad alta, tendencia probable.
    → SL más ancho (evita whipsaw), TP más lejano, size reducido.
  EXTREME (>90%): volatilidad extrema, posible cascada de liquidaciones.
    → Size fuertemente reducido o bloqueo de nuevas entradas.

CÁLCULO:
  atr_pct = ATR / close * 100  (normalizado por precio, comparable entre símbolos)
  percentile = posición del atr_pct actual en su propia historia (deque de N lecturas)
═══════════════════════════════════════════════════════════════════════════════
"""
import logging
from collections import deque
from dataclasses import dataclass, field

log = logging.getLogger("vol_regime")

# ── Umbrales de percentil ──────────────────────────────────────────────────────
PCT_COMPRESSED = 20.0   # por debajo de este percentil = comprimido
PCT_EXPANDED   = 70.0   # por encima de este percentil = expandido
PCT_EXTREME    = 90.0   # por encima de este percentil = extremo

# ── Multiplicadores de sizing ──────────────────────────────────────────────────
SIZE_MULT_COMPRESSED = 1.15   # +15% size — riesgo €/ATR% más bajo de lo normal
SIZE_MULT_NORMAL     = 1.00
SIZE_MULT_EXPANDED   = 0.70   # -30% size — riesgo €/ATR% más alto de lo normal
SIZE_MULT_EXTREME    = 0.40   # -60% size — protección fuerte en caos

# ── Multiplicadores de SL/TP ───────────────────────────────────────────────────
SL_MULT_COMPRESSED = 0.85   # SL más ajustado — el ATR ya es pequeño
SL_MULT_NORMAL      = 1.00
SL_MULT_EXPANDED    = 1.25   # SL más ancho — evita whipsaw en volatilidad alta
SL_MULT_EXTREME      = 1.50   # SL muy ancho — o mejor no operar

TP_MULT_COMPRESSED = 1.30   # TP más lejano — anticipar ruptura explosiva
TP_MULT_NORMAL       = 1.00
TP_MULT_EXPANDED     = 1.15   # TP algo más lejano, tendencia ya en marcha
TP_MULT_EXTREME       = 0.80   # TP más cercano — tomar profit rápido en caos

MIN_READINGS_FOR_PERCENTILE = 15   # mínimo de lecturas antes de confiar en el percentil


class Regime:
    COMPRESSED = "COMPRESSED"
    NORMAL     = "NORMAL"
    EXPANDED   = "EXPANDED"
    EXTREME    = "EXTREME"


@dataclass
class VolSignal:
    regime:        str   = Regime.NORMAL
    atr_pct:       float = 0.0     # ATR como % del precio
    percentile:    float = 50.0    # percentil dentro de la historia propia
    size_mult:     float = 1.0
    sl_mult:       float = 1.0
    tp_mult:       float = 1.0
    block_entry:   bool  = False
    reason:        str   = ""


@dataclass
class ATRHistory:
    readings: deque = field(default_factory=lambda: deque(maxlen=100))

    def add(self, atr_pct: float):
        self.readings.append(atr_pct)

    def percentile_of(self, value: float) -> float:
        """Percentil de `value` dentro de la historia acumulada (0-100)."""
        if len(self.readings) < MIN_READINGS_FOR_PERCENTILE:
            return 50.0  # sin suficiente historia → asumir normal
        sorted_vals = sorted(self.readings)
        below = sum(1 for v in sorted_vals if v <= value)
        return (below / len(sorted_vals)) * 100.0


class VolatilityRegimeEngine:
    """
    Singleton que mantiene historia de ATR% por símbolo y calcula el régimen
    de volatilidad actual con ajustes de sizing y SL/TP.
    """

    def __init__(self):
        self._history: dict[str, ATRHistory] = {}
        self._last_signal: dict[str, VolSignal] = {}
        log.info("VolatilityRegimeEngine v1.0 — volatility targeting activo")

    @staticmethod
    def _classify(percentile: float) -> str:
        if percentile >= PCT_EXTREME:
            return Regime.EXTREME
        if percentile >= PCT_EXPANDED:
            return Regime.EXPANDED
        if percentile <= PCT_COMPRESSED:
            return Regime.COMPRESSED
        return Regime.NORMAL

    @staticmethod
    def _multipliers(regime: str) -> tuple[float, float, float, bool]:
        """Retorna (size_mult, sl_mult, tp_mult, block_entry)."""
        if regime == Regime.COMPRESSED:
            return SIZE_MULT_COMPRESSED, SL_MULT_COMPRESSED, TP_MULT_COMPRESSED, False
        if regime == Regime.EXPANDED:
            return SIZE_MULT_EXPANDED, SL_MULT_EXPANDED, TP_MULT_EXPANDED, False
        if regime == Regime.EXTREME:
            return SIZE_MULT_EXTREME, SL_MULT_EXTREME, TP_MULT_EXTREME, True
        return SIZE_MULT_NORMAL, SL_MULT_NORMAL, TP_MULT_NORMAL, False

    def update(self, symbol: str, atr: float, close: float) -> VolSignal:
        """
        Actualiza la historia de ATR% del símbolo y calcula el régimen actual.
        Llamar una vez por scan cycle, después de calcular el ATR del símbolo.
        """
        if close <= 0 or atr < 0:
            return VolSignal()

        atr_pct = (atr / close) * 100.0

        if symbol not in self._history:
            self._history[symbol] = ATRHistory()
        hist = self._history[symbol]

        percentile = hist.percentile_of(atr_pct)
        hist.add(atr_pct)  # añadir DESPUÉS de calcular percentil (no auto-incluir)

        regime = self._classify(percentile)
        size_mult, sl_mult, tp_mult, block = self._multipliers(regime)

        # EXTREME solo bloquea si hay suficiente historia para confiar en la lectura
        if block and len(hist.readings) < MIN_READINGS_FOR_PERCENTILE:
            block = False

        reason = f"atr%={atr_pct:.2f} pctl={percentile:.0f} regime={regime}"

        sig = VolSignal(
            regime=regime, atr_pct=atr_pct, percentile=percentile,
            size_mult=size_mult, sl_mult=sl_mult, tp_mult=tp_mult,
            block_entry=block, reason=reason,
        )
        self._last_signal[symbol] = sig

        if regime in (Regime.EXTREME, Regime.COMPRESSED):
            log.info("[%s] 📊 Vol regime: %s", symbol, reason)

        return sig

    def get_signal(self, symbol: str) -> VolSignal:
        return self._last_signal.get(symbol, VolSignal())

    def summary(self) -> dict:
        """Resumen de regímenes activos para diagnóstico."""
        out = {}
        for sym, sig in self._last_signal.items():
            if sig.regime != Regime.NORMAL:
                out[sym] = {
                    "regime": sig.regime,
                    "atr_pct": round(sig.atr_pct, 2),
                    "percentile": round(sig.percentile, 0),
                    "size_mult": sig.size_mult,
                }
        return out


# ── Singleton global ────────────────────────────────────────────────────────
vol_engine = VolatilityRegimeEngine()
