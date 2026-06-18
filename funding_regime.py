"""
QF×JP Bot — Funding Regime Engine v1.0
═══════════════════════════════════════════════════════════════════════════════
EL EDGE PROFESIONAL QUE POCOS USAN:

Los pagos de funding en BingX ocurren a las 00:00, 08:00 y 16:00 UTC.
En las 2 horas ANTERIORES a cada pago, los traders apalancados en la
dirección más costosa cierran posiciones para EVITAR pagar el funding.
Esto crea un patrón predecible de presión de precio que los algoritmos
institucionales explotan sistemáticamente.

Ejemplo real:
  FR = +0.10%/8h en LONGXIA → longs pagan 0.10% cada 8h
  A las 14:00 UTC (2h antes del pago de las 16:00 UTC):
    → Los longs empiezan a cerrar para no pagar
    → Presión vendedora artificial y predecible
    → SHORT aquí + cierre post-pago = captura el movimiento + el funding

CLASIFICACIÓN DE REGÍMENES:
  NEUTRAL  │ |FR| < 0.01%  │ Sin señal especial
  CARRY    │ FR 0.01-0.05% │ Positivo estable — leve boost SHORT
  SQUEEZE  │ FR 0.05-0.10% │ Longs apretados — boost SHORT moderado
  EXTREME  │ FR > 0.10%    │ Bomba de tiempo — máximo boost SHORT
  STRESS   │ FR < -0.03%   │ Shorts apretados — boost LONG moderado

SEÑAL DE ANTICIPACIÓN (la más valiosa):
  Si FR lleva 3+ lecturas subiendo sin que el precio reaccione aún
  → señal contraria anticipada: el mercado aún no lo ha descontado

TIMING PRE-FUNDING:
  ventana [-2h, 0h] antes del pago → máxima convicción en dirección contraria
  ventana [-4h, -2h] antes del pago → preparación, boost moderado
  ventana [0h, +1h] después del pago → reversión, dirección se normaliza
═══════════════════════════════════════════════════════════════════════════════
"""
import datetime
import logging
import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger("funding_regime")

# ── Constantes ────────────────────────────────────────────────────────────────
CARRY_THR   = 0.0001   # 0.01% — inicio de régimen carry
SQUEEZE_THR = 0.0005   # 0.05% — squeeze (ya implementado antes)
EXTREME_THR = 0.0010   # 0.10% — extremo, pre-funding trade
STRESS_THR  = -0.0003  # -0.03% — stress (shorts apretados)

PREFUND_WINDOW_H  = 2.0   # horas antes del pago = máxima convicción
PREFUND_PREP_H    = 4.0   # horas antes = preparación
POSTFUND_WINDOW_H = 1.0   # hora después del pago = reversión

# Boosts al score de señal
BOOST_EXTREME_PREFUND  = 15.0  # +15 pts: MAX convicción — 2h antes, FR >0.1%
BOOST_EXTREME_NORMAL   =  8.0  # + 8 pts: FR extremo fuera de ventana
BOOST_SQUEEZE_PREFUND  = 10.0  # +10 pts: squeeze dentro ventana pre-funding
BOOST_SQUEEZE_NORMAL   =  5.0  # + 5 pts: squeeze fuera ventana
BOOST_CARRY            =  2.0  # + 2 pts: carry moderado
BOOST_STRESS           =  6.0  # + 6 pts para LONG en régimen stress
BOOST_POSTFUND_REVERSAL=  8.0  # + 8 pts para dirección contraria post-pago
PENALTY_AGAINST_REGIME = -8.0  # - 8 pts señal en contra del régimen


# ── Regímenes ─────────────────────────────────────────────────────────────────

class Regime:
    NEUTRAL = "NEUTRAL"
    CARRY   = "CARRY"
    SQUEEZE = "SQUEEZE"
    EXTREME = "EXTREME"
    STRESS  = "STRESS"

class Trend:
    RISING  = "RISING"
    STABLE  = "STABLE"
    FALLING = "FALLING"

class Window:
    PREFUND_MAX  = "PREFUND_MAX"   # -2h a 0h
    PREFUND_PREP = "PREFUND_PREP"  # -4h a -2h
    POSTFUND     = "POSTFUND"      # 0h a +1h
    NORMAL       = "NORMAL"        # el resto


@dataclass
class RegimeSignal:
    regime:    str = Regime.NEUTRAL
    trend:     str = Trend.STABLE
    window:    str = Window.NORMAL
    fr:        float = 0.0
    hours_to_funding: float = 8.0
    # Score adjustments calculados
    short_boost: float = 0.0
    long_boost:  float = 0.0
    # Para logging
    reason:    str = ""


# ── Historia de FR por símbolo ────────────────────────────────────────────────

@dataclass
class FRHistory:
    readings: deque = field(default_factory=lambda: deque(maxlen=6))
    # Cada reading: (timestamp, fr_value)

    def add(self, fr: float):
        self.readings.append((time.time(), fr))

    def trend(self) -> str:
        if len(self.readings) < 3:
            return Trend.STABLE
        vals = [r[1] for r in self.readings]
        # Comparar últimas 3 lecturas
        recent = vals[-3:]
        rises  = sum(1 for i in range(1, len(recent)) if recent[i] > recent[i-1] * 1.02)
        falls  = sum(1 for i in range(1, len(recent)) if recent[i] < recent[i-1] * 0.98)
        if rises >= 2:
            return Trend.RISING
        if falls >= 2:
            return Trend.FALLING
        return Trend.STABLE

    def acceleration(self) -> float:
        """Velocidad de cambio del FR — positivo = acelerando subida."""
        if len(self.readings) < 3:
            return 0.0
        vals = [r[1] for r in self.readings]
        if len(vals) >= 4:
            recent_change = vals[-1] - vals[-2]
            prev_change   = vals[-2] - vals[-3]
            return recent_change - prev_change
        return vals[-1] - vals[-2]


# ── Motor principal ───────────────────────────────────────────────────────────

class FundingRegimeEngine:
    """
    Singleton que mantiene historia de FR por símbolo y calcula el régimen
    actual con señales de anticipación y timing relativo al pago de funding.
    """

    def __init__(self):
        self._history: dict[str, FRHistory] = {}
        self._last_regime: dict[str, RegimeSignal] = {}
        log.info("FundingRegimeEngine v1.0 — anticipación pre-funding activa")

    # ── Timing ───────────────────────────────────────────────────────────────

    @staticmethod
    def hours_to_next_funding() -> float:
        """
        BingX paga funding a las 00:00, 08:00, 16:00 UTC.
        Retorna horas hasta el próximo pago (0.0-8.0).
        """
        now     = datetime.datetime.utcnow()
        h_float = now.hour + now.minute / 60.0 + now.second / 3600.0
        # Próximo pago cada 8h
        next_payment = math.ceil(h_float / 8.0) * 8.0
        if next_payment == h_float:
            next_payment += 8.0
        hours_left = next_payment - h_float
        return hours_left % 8.0 or 8.0

    @staticmethod
    def hours_since_last_funding() -> float:
        """Horas transcurridas desde el último pago."""
        now     = datetime.datetime.utcnow()
        h_float = now.hour + now.minute / 60.0 + now.second / 3600.0
        return h_float % 8.0

    @classmethod
    def _classify_window(cls) -> str:
        htf = cls.hours_to_next_funding()
        hsl = cls.hours_since_last_funding()
        if htf <= PREFUND_WINDOW_H:
            return Window.PREFUND_MAX    # ventana crítica: 2h antes del pago
        if htf <= PREFUND_PREP_H:
            return Window.PREFUND_PREP   # preparación: 2-4h antes del pago
        if hsl <= POSTFUND_WINDOW_H:
            return Window.POSTFUND       # post-pago: reversión esperada
        return Window.NORMAL

    # ── Clasificación de régimen ──────────────────────────────────────────────

    @staticmethod
    def _classify_regime(fr: float) -> str:
        if fr >= EXTREME_THR:
            return Regime.EXTREME
        if fr >= SQUEEZE_THR:
            return Regime.SQUEEZE
        if fr >= CARRY_THR:
            return Regime.CARRY
        if fr <= STRESS_THR:
            return Regime.STRESS
        return Regime.NEUTRAL

    # ── Score adjustments ────────────────────────────────────────────────────

    def _calc_boosts(
        self,
        regime:  str,
        trend:   str,
        window:  str,
        accel:   float,
    ) -> tuple[float, float]:
        """
        Retorna (short_boost, long_boost) — boost al score de la señal.
        Los valores positivos se SUMAN al score.
        Los negativos penalizan señales en la dirección equivocada.

        Lógica:
          EXTREME/SQUEEZE + ventana pre-funding → boost SHORT (longs van a cerrar)
          STRESS + ventana pre-funding           → boost LONG  (shorts van a cerrar)
          Régimen RISING trend                   → boost contrario anticipado
          Post-funding                           → boost hacia la dirección que vuelve
        """
        sb = 0.0   # short boost
        lb = 0.0   # long boost

        # ── Régimen base ─────────────────────────────────────────────────────
        if regime == Regime.EXTREME:
            if window == Window.PREFUND_MAX:
                sb = BOOST_EXTREME_PREFUND   # +15: máxima convicción
                lb = PENALTY_AGAINST_REGIME  # -8: NO abrir LONG aquí
            elif window == Window.PREFUND_PREP:
                sb = BOOST_SQUEEZE_PREFUND   # +10
                lb = PENALTY_AGAINST_REGIME
            elif window == Window.POSTFUND:
                lb = BOOST_POSTFUND_REVERSAL # +8: longs vuelven post-pago
                sb = 0.0
            else:
                sb = BOOST_EXTREME_NORMAL    # +8: extremo normal

        elif regime == Regime.SQUEEZE:
            if window == Window.PREFUND_MAX:
                sb = BOOST_SQUEEZE_PREFUND   # +10
                lb = PENALTY_AGAINST_REGIME / 2
            elif window == Window.PREFUND_PREP:
                sb = BOOST_SQUEEZE_NORMAL    # +5
            elif window == Window.POSTFUND:
                lb = BOOST_POSTFUND_REVERSAL / 2  # +4
            else:
                sb = BOOST_SQUEEZE_NORMAL    # +5

        elif regime == Regime.CARRY:
            sb = BOOST_CARRY                 # +2: carry leve favorece SHORT

        elif regime == Regime.STRESS:
            if window in (Window.PREFUND_MAX, Window.PREFUND_PREP):
                lb = BOOST_STRESS            # +6: shorts van a cerrar
                sb = PENALTY_AGAINST_REGIME  # -8: NO abrir SHORT
            elif window == Window.POSTFUND:
                sb = BOOST_POSTFUND_REVERSAL / 2  # shorts vuelven post-pago
            else:
                lb = BOOST_STRESS / 2        # +3: stress moderado

        # ── Bonus por tendencia acelerada (anticipación) ──────────────────────
        # Si el FR lleva subiendo 3 lecturas y aún NO es extreme → el mercado
        # no lo ha descontado todavía → señal de anticipación extra
        if trend == Trend.RISING and accel > 0 and regime in (Regime.CARRY, Regime.SQUEEZE):
            sb += 4.0   # anticipar el squeeze futuro
        if trend == Trend.FALLING and accel < 0 and regime in (Regime.STRESS, Regime.NEUTRAL):
            lb += 4.0   # anticipar el stress futuro

        return round(sb, 2), round(lb, 2)

    # ── API principal ─────────────────────────────────────────────────────────

    def update(self, symbol: str, fr: float) -> RegimeSignal:
        """
        Actualiza la historia de FR del símbolo y calcula el régimen actual.
        Llamar una vez por scan cycle (cada 60s con caché del API).
        """
        if symbol not in self._history:
            self._history[symbol] = FRHistory()
        hist = self._history[symbol]
        hist.add(fr)

        regime  = self._classify_regime(fr)
        trend   = hist.trend()
        accel   = hist.acceleration()
        window  = self._classify_window()
        htf     = self.hours_to_next_funding()

        sb, lb = self._calc_boosts(regime, trend, window, accel)

        reason_parts = []
        if regime != Regime.NEUTRAL:
            reason_parts.append(f"regime={regime}")
        if window != Window.NORMAL:
            reason_parts.append(f"window={window} ({htf:.1f}h to funding)")
        if trend != Trend.STABLE:
            reason_parts.append(f"trend={trend}")
        if sb != 0:
            reason_parts.append(f"short_boost={sb:+.0f}")
        if lb != 0:
            reason_parts.append(f"long_boost={lb:+.0f}")

        sig = RegimeSignal(
            regime=regime, trend=trend, window=window,
            fr=fr, hours_to_funding=htf,
            short_boost=sb, long_boost=lb,
            reason=" | ".join(reason_parts) if reason_parts else "neutral",
        )
        self._last_regime[symbol] = sig

        if regime in (Regime.EXTREME, Regime.STRESS) or window == Window.PREFUND_MAX:
            log.info("[%s] 💰 FR=%+.4f%% %s %s — %s",
                     symbol, fr*100, regime, window, sig.reason)

        return sig

    def get_score_adjustment(self, symbol: str, direction: str) -> float:
        """
        Retorna el ajuste de score para una señal dada.
        direction: "LONG" o "SHORT"
        """
        sig = self._last_regime.get(symbol)
        if sig is None:
            return 0.0
        if direction == "SHORT":
            return sig.short_boost
        if direction == "LONG":
            return sig.long_boost
        return 0.0

    def is_harvest_opportunity(
        self,
        symbol: str,
        fr: float,
        harvest_fr_thr: float = 0.0010,
    ) -> tuple[bool, str, float]:
        """
        Detecta oportunidades de Funding Harvest.
        Retorna (is_opportunity, harvest_direction, expected_yield_per_8h).

        Condiciones óptimas:
          - FR > harvest_fr_thr (0.10%/8h) → SHORT harvest
          - FR < -harvest_fr_thr/2 (-0.05%/8h) → LONG harvest
          - Ventana: PREFUND_MAX o PREFUND_PREP para máxima anticipación

        El harvest es market-neutral en teoría (spot hedge) pero sin spot
        funciona como un SHORT/LONG con SL muy ajustado aprovechando que
        el funding paga el riesgo asumido.
        """
        window = self._classify_window()
        htf    = self.hours_to_next_funding()

        if fr >= harvest_fr_thr:
            # SHORT harvest: longs pagan, nosotros cobramos
            in_window = window in (Window.PREFUND_MAX, Window.PREFUND_PREP)
            if in_window or fr >= harvest_fr_thr * 1.5:
                yield_pct = fr  # cobrado por 8h si mantenemos hasta pago
                reason    = f"FR={fr*100:.3f}%/8h {window} {htf:.1f}h"
                return True, "SHORT", yield_pct
        elif fr <= -harvest_fr_thr / 2:
            # LONG harvest: shorts pagan, nosotros cobramos
            in_window = window in (Window.PREFUND_MAX, Window.PREFUND_PREP)
            if in_window or fr <= -harvest_fr_thr:
                yield_pct = abs(fr)
                reason    = f"FR={fr*100:.3f}%/8h {window} {htf:.1f}h"
                return True, "LONG", yield_pct
        return False, "", 0.0

    def summary(self) -> dict:
        """Resumen de regímenes activos para Telegram/logging."""
        active = {}
        for sym, sig in self._last_regime.items():
            if sig.regime != Regime.NEUTRAL:
                active[sym] = {
                    "regime": sig.regime,
                    "fr": sig.fr,
                    "window": sig.window,
                    "htf": sig.hours_to_funding,
                    "boosts": (sig.short_boost, sig.long_boost),
                }
        return active


# ── Singleton global (importado por scanner.py) ───────────────────────────────
regime_engine = FundingRegimeEngine()
