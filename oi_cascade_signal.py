"""
OI + Funding Rate Cascade Signal Engine v1.0
══════════════════════════════════════════════════════════════════════════════
Detecta cascadas de liquidación inminentes combinando:

  1. Open Interest Delta (40% del score)
     OI subió >20% en las últimas horas → mucho apalancamiento nuevo
     Cuanto más OI acumulado, más violenta será la cascade

  2. Funding Rate extremo (40% del score)
     FR muy positivo → longs sobreextendidos → cascade bajista inminente
     FR muy negativo → shorts sobreextendidos → cascade alcista inminente

  3. Divergencia OI/Precio (20% del score)
     OI sube pero precio no sube → momentum falso → cascade probable
     (Si el precio acompañara el OI, los longs tendrían razón)

Score de -100 a +100:
  +70 a +100 = CASCADE BAJISTA muy probable → ABRIR SHORT
  +30 a +70  = Señal bajista moderada → boost SHORT en scanner
   -30 a +30 = Sin señal clara
  -30 a -70  = Señal alcista moderada → boost LONG en scanner
  -70 a -100 = CASCADE ALCISTA muy probable → ABRIR LONG

Historia de OI: almacena snapshots cada 30 minutos para calcular
el cambio en distintos períodos (1H, 4H, 24H) sin queries extra a API.

Integración en scanner.py de renewed-love:
  from oi_cascade_signal import oi_cascade_engine
  cascade = oi_cascade_engine.update(symbol, oi_value, fr, close, atr)
  sig.score += cascade.boost
  if cascade.hard_block: return None
══════════════════════════════════════════════════════════════════════════════
"""
import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger("oi_cascade")

# ── Umbrales configurables ────────────────────────────────────────────────────

FR_EXTREME_LONG  =  0.0005   # FR > 0.05%/8H → longs dominan extremamente
FR_HIGH_LONG     =  0.0002   # FR > 0.02%/8H → longs dominan
FR_EXTREME_SHORT = -0.0005   # FR < -0.05%/8H → shorts dominan extremamente
FR_HIGH_SHORT    = -0.0002   # FR < -0.02%/8H → shorts dominan

OI_SPIKE_HIGH    =  0.25     # OI +25% en 4H → acumulación fuerte
OI_SPIKE_MED     =  0.12     # OI +12% en 4H → acumulación moderada
OI_DROP          = -0.10     # OI -10% en 4H → posiciones cerrando (cascade pasando)

DIV_THRESHOLD    =  0.005    # 0.5% de divergencia mínima OI/precio


@dataclass
class CascadeSignal:
    """Señal de cascade para un símbolo."""
    symbol:         str
    score:          float   # -100 a +100
    direction:      str     # "SHORT_CASCADE" | "LONG_CASCADE" | "NEUTRAL"
    boost:          float   # pts a añadir al score del scanner
    hard_block:     bool    # bloquear señales contrarias
    fr_score:       float
    oi_score:       float
    div_score:      float
    fr_value:       float
    oi_delta_4h:    float
    oi_delta_1h:    float
    reason:         str

    def __str__(self):
        return (
            f"CASCADE={self.direction} score={self.score:+.0f} "
            f"(fr={self.fr_score:+.0f} oi={self.oi_score:+.0f} div={self.div_score:+.0f}) "
            f"FR={self.fr_value*100:.3f}% OI_4H={self.oi_delta_4h*100:+.1f}%"
        )


# ── Historia de OI por símbolo ────────────────────────────────────────────────

class OIHistory:
    """
    Almacena snapshots de (timestamp, oi, price) para un símbolo.
    Mantiene 48 horas de historia con resolución de ~30 minutos.
    """
    MAX_SNAPSHOTS = 100   # ~50 horas de historia con 30 min entre snapshots

    def __init__(self):
        self.snapshots: deque = deque(maxlen=self.MAX_SNAPSHOTS)
        self.last_snapshot_ts: float = 0.0
        self.SNAPSHOT_INTERVAL = 1800  # 30 minutos entre snapshots

    def add(self, oi: float, price: float) -> bool:
        """Añade snapshot si han pasado suficientes segundos. Retorna True si añadió."""
        now = time.time()
        if now - self.last_snapshot_ts < self.SNAPSHOT_INTERVAL and self.snapshots:
            return False
        self.snapshots.append((now, oi, price))
        self.last_snapshot_ts = now
        return True

    def delta(self, hours: float) -> Optional[float]:
        """
        Cambio relativo del OI en las últimas `hours` horas.
        Retorna None si no hay suficiente historia.
        """
        if not self.snapshots:
            return None
        now = time.time()
        cutoff = now - hours * 3600
        # Buscar el snapshot más antiguo dentro del período
        past = None
        for ts, oi, price in self.snapshots:
            if ts >= cutoff:
                past = (ts, oi, price)
                break
        if past is None:
            return None
        current_oi = self.snapshots[-1][1]
        past_oi = past[1]
        if past_oi <= 0:
            return None
        return (current_oi - past_oi) / past_oi

    def price_delta(self, hours: float) -> Optional[float]:
        """Cambio relativo del precio en el período."""
        if not self.snapshots:
            return None
        now = time.time()
        cutoff = now - hours * 3600
        past = None
        for ts, oi, price in self.snapshots:
            if ts >= cutoff:
                past = (ts, oi, price)
                break
        if past is None:
            return None
        current_price = self.snapshots[-1][2]
        past_price = past[2]
        if past_price <= 0:
            return None
        return (current_price - past_price) / past_price

    def oi_current(self) -> float:
        return self.snapshots[-1][1] if self.snapshots else 0.0

    def ready(self, min_hours: float = 1.0) -> bool:
        """True si tenemos al menos `min_hours` de historia."""
        if len(self.snapshots) < 2:
            return False
        span = self.snapshots[-1][0] - self.snapshots[0][0]
        return span >= min_hours * 3600


# ── Motor principal ───────────────────────────────────────────────────────────

class OICascadeEngine:
    """
    Singleton que rastrea OI + FR para todos los símbolos y detecta
    cascadas de liquidación inminentes.
    """

    def __init__(self):
        self._history: dict[str, OIHistory] = {}
        self._last_signals: dict[str, CascadeSignal] = {}

    def update(
        self,
        symbol:    str,
        oi:        float,
        fr:        float,
        close:     float,
        atr:       float = 0.0,
    ) -> CascadeSignal:
        """
        Actualiza la historia del símbolo y calcula la señal de cascade.

        Args:
            symbol: par de trading
            oi:     Open Interest actual (en contratos o USDT)
            fr:     Funding Rate actual (ej. 0.0001 = 0.01%/8H)
            close:  precio de cierre actual
            atr:    ATR actual (para normalizar divergencia)

        Returns:
            CascadeSignal con score, boost y dirección
        """
        if symbol not in self._history:
            self._history[symbol] = OIHistory()

        hist = self._history[symbol]
        hist.add(oi, close)

        # Sin historia suficiente → señal neutra
        if not hist.ready(min_hours=0.5):
            return self._neutral(symbol, fr)

        # ── 1. Funding Rate Score (40 pts max) ───────────────────────────────
        if fr >= FR_EXTREME_LONG:
            fr_score = 40.0    # Longs dominan extremamente → cascade bajista
        elif fr >= FR_HIGH_LONG:
            fr_score = 20.0
        elif fr <= FR_EXTREME_SHORT:
            fr_score = -40.0   # Shorts dominan extremamente → cascade alcista
        elif fr <= FR_HIGH_SHORT:
            fr_score = -20.0
        else:
            fr_score = 0.0

        # ── 2. OI Delta Score (40 pts max) ───────────────────────────────────
        oi_d4h = hist.delta(4.0) or 0.0
        oi_d1h = hist.delta(1.0) or 0.0

        # El OI creciendo en la misma dirección que el FR amplifica el riesgo
        if oi_d4h >= OI_SPIKE_HIGH:
            oi_score = 40.0 if fr_score >= 0 else -40.0
        elif oi_d4h >= OI_SPIKE_MED:
            oi_score = 20.0 if fr_score >= 0 else -20.0
        elif oi_d4h <= OI_DROP:
            # OI cayendo → cascade posiblemente ya en curso, señal débil
            oi_score = 0.0
        else:
            oi_score = 0.0

        # ── 3. Divergencia OI/Precio (20 pts max) ────────────────────────────
        # Si OI sube mucho pero precio no → longs atrapados → cascade bajista
        price_d4h = hist.price_delta(4.0) or 0.0
        div_score = 0.0

        if oi_d4h > DIV_THRESHOLD:
            # OI subió, precio no subió igual → divergencia bajista
            divergence = oi_d4h - max(price_d4h, 0.0)
            if divergence > 0.15:
                div_score = 20.0
            elif divergence > 0.08:
                div_score = 10.0

        elif oi_d4h < -DIV_THRESHOLD:
            # OI bajó, precio no bajó igual → divergencia alcista
            divergence = abs(oi_d4h) - abs(min(price_d4h, 0.0))
            if divergence > 0.15:
                div_score = -20.0
            elif divergence > 0.08:
                div_score = -10.0

        # ── Score total ───────────────────────────────────────────────────────
        raw_score = fr_score + oi_score + div_score
        score = max(-100.0, min(100.0, raw_score))

        # ── Clasificación ─────────────────────────────────────────────────────
        if score >= 70:
            direction  = "SHORT_CASCADE"
            boost      = 12.0
            hard_block = True    # bloquear LONGs — cascade bajista inminente
        elif score >= 30:
            direction  = "SHORT_CASCADE"
            boost      = 6.0
            hard_block = False
        elif score <= -70:
            direction  = "LONG_CASCADE"
            boost      = 12.0
            hard_block = True    # bloquear SHORTs — cascade alcista inminente
        elif score <= -30:
            direction  = "LONG_CASCADE"
            boost      = 6.0
            hard_block = False
        else:
            direction  = "NEUTRAL"
            boost      = 0.0
            hard_block = False

        reason = (
            f"FR={fr*100:.3f}% OI_4H={oi_d4h*100:+.1f}% "
            f"OI_1H={oi_d1h*100:+.1f}% PRICE_4H={price_d4h*100:+.1f}% "
            f"→ {direction}"
        )

        sig = CascadeSignal(
            symbol=symbol,
            score=round(score, 1),
            direction=direction,
            boost=boost if (
                (direction == "SHORT_CASCADE" and boost > 0) or
                (direction == "LONG_CASCADE" and boost > 0)
            ) else 0.0,
            hard_block=hard_block,
            fr_score=fr_score,
            oi_score=oi_score,
            div_score=div_score,
            fr_value=fr,
            oi_delta_4h=oi_d4h,
            oi_delta_1h=oi_d1h,
            reason=reason,
        )

        # Aplicar boost solo en la dirección correcta
        # (un signal de SHORT_CASCADE da boost a SHORTs, no a LONGs)
        self._last_signals[symbol] = sig

        if abs(score) >= 30:
            log.info("[%s] ⚡ %s", symbol, sig)

        return sig

    def signal_for_direction(self, symbol: str, direction: str) -> tuple:
        """
        Retorna (boost, reason, hard_block) para una dirección específica.
        Aplica el boost solo si la cascade coincide con la dirección de la señal.
        """
        sig = self._last_signals.get(symbol)
        if sig is None:
            return 0.0, "no_oi_data", False

        if direction == "SHORT" and sig.direction == "SHORT_CASCADE":
            return sig.boost, sig.reason, False
        elif direction == "LONG" and sig.direction == "LONG_CASCADE":
            return sig.boost, sig.reason, False
        elif direction == "SHORT" and sig.direction == "LONG_CASCADE" and sig.hard_block:
            return 0.0, f"CASCADE_BLOCK_SHORT {sig.reason}", True
        elif direction == "LONG" and sig.direction == "SHORT_CASCADE" and sig.hard_block:
            return 0.0, f"CASCADE_BLOCK_LONG {sig.reason}", True
        else:
            return 0.0, sig.reason, False

    def get_top_cascades(self, n: int = 5) -> list:
        """Retorna los símbolos con mayor score de cascade (candidatos a trade)."""
        signals = [(sym, sig) for sym, sig in self._last_signals.items()
                   if abs(sig.score) >= 30]
        signals.sort(key=lambda x: abs(x[1].score), reverse=True)
        return signals[:n]

    @staticmethod
    def _neutral(symbol: str, fr: float) -> CascadeSignal:
        return CascadeSignal(
            symbol=symbol, score=0.0, direction="NEUTRAL",
            boost=0.0, hard_block=False,
            fr_score=0.0, oi_score=0.0, div_score=0.0,
            fr_value=fr, oi_delta_4h=0.0, oi_delta_1h=0.0,
            reason="initializing_oi_history",
        )


# Singleton global
oi_cascade_engine = OICascadeEngine()
