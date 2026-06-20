"""
QF×JP Bot — Order Blocks + Kaplan-Meier Survival (motor stateful)
═══════════════════════════════════════════════════════════════════════════
Portado de "High Probability Order Blocks [AlgoAlpha]". A diferencia de
los demás filtros nuevos de hoy (STC, slope, price action, trend magic),
ESTE necesita estado persistente entre ciclos de scan — mismo patrón que
funding_regime.py / volatility_regime.py (singleton actualizado cada
ciclo, no recalculado desde cero) — porque la curva de supervivencia
Kaplan-Meier necesita acumular historial de order blocks mitigados a lo
largo de muchas horas/días, no de las ~200 velas que se fetchean en cada
ciclo de scan.

QUÉ HACE:
  1. Detecta creación de order blocks: cuando el movimiento direccional
     acumulado en racha (updist/downdist — suma de cuerpos de velas del
     mismo color consecutivas) tiene un z-score que CRUZA 4 desviaciones
     sobre su propia media de los últimos `z_len` velas, se marca la
     última vela del color OPUESTO antes de ese impulso como el order
     block (lógica SMC/ICT: las órdenes límite que alimentaron el
     impulso quedaron ahí).
  2. Sigue cada order block activo: si el precio cierra 2 velas seguidas
     más allá del bloque → MITIGADO ("evento": murió a esa edad, en
     barras). Si pasa `max_age_bars` sin mitigarse → CENSURADO (no
     sabemos si habría sobrevivido más — igual que un paciente que
     abandona un estudio médico sin que ocurra el evento observado).
  3. Detecta "rechazo": el precio toca el bloque y cierra de vuelta del
     lado correcto en la siguiente vela — el bloque aguantó.
  4. Kaplan-Meier (fórmula exacta, verificada término a término contra el
     Pine original — incluye purga correcta de censurados del risk-set y
     manejo de empates): dado el historial de edades de mitigación/
     censura de bloques pasados del mismo símbolo y lado, calcula la
     probabilidad de que un bloque de la edad actual siga "vivo".

Como filtro de confirmación: si hay un rechazo activo en la dirección de
la señal Y el bloque que lo originó tiene supervivencia KM alta (umbral
configurable) con muestra suficiente → boost. Con muestra insuficiente
(símbolo nuevo, pocos bloques mitigados todavía) → neutral, ni boost ni
veto — se va llenando solo con el tiempo que lleve corriendo el bot.

ADVERTENCIA: el estado vive en memoria del proceso — se pierde en cada
redeploy de Railway, igual que el TradeJournal. Con los redeploys
frecuentes de una sesión de desarrollo activa, tardará en acumular
muestra suficiente para que el KM diga algo. No es un bug, es la misma
limitación que ya conoces del journal — y otra razón más para que los
redeploys no sean tan frecuentes una vez esto esté en marcha.
═══════════════════════════════════════════════════════════════════════════
"""
import logging
import math
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger("order_block_km")

_MAX_KM_SAMPLES = 70   # kmLookback del original — máximo de muestras guardadas por lado


@dataclass
class _Box:
    top:    float
    bottom: float
    start_bar: int
    rejected_recently: bool = False


@dataclass
class _SymbolState:
    last_ts:    int = 0     # timestamp de la última vela ya procesada (para detectar nuevas)
    bar_count:  int = 0     # contador interno de velas procesadas — la "edad" se mide en esto,
                             # no en timestamp, para que maxAge/kmLookback signifiquen lo mismo
                             # que en el Pine original (barras, no milisegundos)
    updist:     float = 0.0
    downdist:   float = 0.0
    up_window:   list = field(default_factory=list)    # ventana rodante (z_len) para mean/stdev
    down_window: list = field(default_factory=list)
    prev_z_up: float = 0.0
    prev_z_dn: float = 0.0
    last_down_candle: Optional[tuple] = None   # (high, low) última vela roja vista
    last_up_candle:   Optional[tuple] = None   # (high, low) última vela verde vista
    bull_boxes: list = field(default_factory=list)   # list[_Box]
    bear_boxes: list = field(default_factory=list)
    prev_close: Optional[float] = None
    km_event_ages_bull:  list = field(default_factory=list)
    km_censor_ages_bull: list = field(default_factory=list)
    km_event_ages_bear:  list = field(default_factory=list)
    km_censor_ages_bear: list = field(default_factory=list)


def _stdev(values: list[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    mu = sum(values) / n
    return math.sqrt(sum((v - mu) ** 2 for v in values) / n)


def kaplan_meier_survival(event_ages: list[float], censored_ages: list[float],
                           total_n: int, age: float, min_samples: int = 5) -> Optional[float]:
    """
    Estimador de Kaplan-Meier — misma fórmula que f_km_survival() del Pine
    original, verificada término a término: purga del risk-set los
    censurados con edad menor a ti ANTES de aplicar la actualización de
    supervivencia, cuenta empates de eventos en el mismo ti, y resta del
    risk-set tanto los eventos como los censurados de ese instante.
    Retorna None si no hay muestra suficiente (igual que "N.E.D." del original).
    """
    if total_n < min_samples or not event_ages:
        return None

    E = sorted(event_ages)
    C = sorted(censored_ages)
    S = 1.0
    n_risk = total_n
    ei = ci = 0

    while ei < len(E):
        ti = E[ei]
        if ti > age:
            break
        while ci < len(C) and C[ci] < ti:
            n_risk -= 1
            ci += 1
        d = 0
        while ei < len(E) and E[ei] == ti:
            d += 1
            ei += 1
        if n_risk > 0:
            S *= (1.0 - d / n_risk)
        n_risk -= d
        c_eq = 0
        while ci < len(C) and C[ci] == ti:
            c_eq += 1
            ci += 1
        n_risk -= c_eq

    return S


class OrderBlockEngine:
    """
    Singleton stateful — mismo patrón que funding_regime.regime_engine y
    volatility_regime.vol_engine. update() procesa solo las velas NUEVAS
    desde la última llamada para este símbolo (incremental, no recalcula
    todo el historial cada vez — solo el primer ciclo procesa la ventana
    completa fetcheada, para sembrar el estado inicial).
    """

    def __init__(self):
        self._state: dict[str, _SymbolState] = {}

    def _get_state(self, symbol: str) -> _SymbolState:
        if symbol not in self._state:
            self._state[symbol] = _SymbolState()
        return self._state[symbol]

    def update(self, symbol: str, klines: list, z_len: int = 50,
               max_age_bars: int = 2000) -> None:
        """Procesa las velas nuevas de `klines` para este símbolo. Consultar el
        resultado después con get_active_rejection()."""
        st = self._get_state(symbol)
        new_candles = [c for c in klines if c[0] > st.last_ts]
        for c in new_candles:
            self._process_candle(st, c, z_len, max_age_bars)
            st.last_ts = c[0]

    def _process_candle(self, st: _SymbolState, c: list, z_len: int, max_age_bars: int):
        st.bar_count += 1
        o, h, l, close = c[1], c[2], c[3], c[4]

        # ── updist/downdist: racha de movimiento direccional ──────────────────
        if close > o:
            st.updist, st.downdist = st.updist + (close - o), 0.0
        elif close < o:
            st.downdist, st.updist = st.downdist + (o - close), 0.0
        else:
            st.updist = st.downdist = 0.0

        st.up_window.append(st.updist)
        st.down_window.append(st.downdist)
        if len(st.up_window) > z_len:
            st.up_window.pop(0)
        if len(st.down_window) > z_len:
            st.down_window.pop(0)

        up_mean, up_std = sum(st.up_window) / len(st.up_window), _stdev(st.up_window)
        dn_mean, dn_std = sum(st.down_window) / len(st.down_window), _stdev(st.down_window)
        z_up = (st.updist - up_mean) / up_std if up_std > 1e-12 else 0.0
        z_dn = (st.downdist - dn_mean) / dn_std if dn_std > 1e-12 else 0.0

        # ── Cruce real (ta.crossover), no "está por encima de 4" persistente ──
        bullish_trigger = st.prev_z_up <= 4.0 and z_up > 4.0 and st.prev_z_up != 0.0
        bearish_trigger = st.prev_z_dn <= 4.0 and z_dn > 4.0 and st.prev_z_dn != 0.0
        st.prev_z_up, st.prev_z_dn = z_up, z_dn

        # ── Creación de order blocks ──────────────────────────────────────────
        if bullish_trigger and st.last_down_candle is not None:
            dh, dl = st.last_down_candle
            st.bull_boxes.append(_Box(top=dh, bottom=dl, start_bar=st.bar_count))
        if bearish_trigger and st.last_up_candle is not None:
            uh, ul = st.last_up_candle
            st.bear_boxes.append(_Box(top=uh, bottom=ul, start_bar=st.bar_count))

        if close < o:
            st.last_down_candle = (h, l)
        elif close > o:
            st.last_up_candle = (h, l)

        # ── Ciclo de vida: mitigación / expiración / rechazo ──────────────────
        prev_close = st.prev_close

        still_bull = []
        for box in st.bull_boxes:
            age = st.bar_count - box.start_bar
            mitigated = prev_close is not None and close < box.bottom and prev_close < box.bottom
            expired   = age >= max_age_bars
            if mitigated:
                self._record(st, "bull", "event", age)
            elif expired:
                self._record(st, "bull", "censor", age)
            else:
                box.rejected_recently = bool(
                    prev_close is not None and close > box.top and prev_close < box.top
                )
                still_bull.append(box)
        st.bull_boxes = still_bull

        still_bear = []
        for box in st.bear_boxes:
            age = st.bar_count - box.start_bar
            mitigated = prev_close is not None and close > box.top and prev_close > box.top
            expired   = age >= max_age_bars
            if mitigated:
                self._record(st, "bear", "event", age)
            elif expired:
                self._record(st, "bear", "censor", age)
            else:
                box.rejected_recently = bool(
                    prev_close is not None and close < box.bottom and prev_close > box.bottom
                )
                still_bear.append(box)
        st.bear_boxes = still_bear

        st.prev_close = close

    def _record(self, st: _SymbolState, side: str, kind: str, age: float):
        if side == "bull":
            lst = st.km_event_ages_bull if kind == "event" else st.km_censor_ages_bull
        else:
            lst = st.km_event_ages_bear if kind == "event" else st.km_censor_ages_bear
        lst.append(age)
        if len(lst) > _MAX_KM_SAMPLES:
            lst.pop(0)

    def get_active_rejection(self, symbol: str, direction: str,
                              min_samples: int = 5) -> tuple[bool, Optional[float], int]:
        """Retorna (hay_rechazo_activo, supervivencia_km, n_muestras)."""
        st = self._get_state(symbol)
        boxes   = st.bull_boxes        if direction == "LONG" else st.bear_boxes
        events  = st.km_event_ages_bull  if direction == "LONG" else st.km_event_ages_bear
        censors = st.km_censor_ages_bull if direction == "LONG" else st.km_censor_ages_bear
        total_n = len(events) + len(censors)

        active = [b for b in boxes if b.rejected_recently]
        if not active:
            return False, None, total_n

        newest = max(active, key=lambda b: b.start_bar)
        age = st.bar_count - newest.start_bar
        surv = kaplan_meier_survival(events, censors, total_n, age, min_samples)
        return True, surv, total_n


ob_engine = OrderBlockEngine()


def order_block_km_filter(
    symbol: str, direction: str,
    survival_threshold: float = 0.6, boost_amount: float = 8.0,
    min_samples: int = 5,
) -> tuple[float, str, bool]:
    """
    Mismo contrato que los demás filtros (boost_pts, reason, block).
    Llamar SIEMPRE DESPUÉS de ob_engine.update(symbol, klines) para este
    símbolo en el ciclo actual — este filtro solo LEE el estado, no lo
    actualiza.

    - Sin rechazo de order block activo → neutral.
    - Rechazo activo pero muestra insuficiente para el KM → neutral (no
      penaliza por falta de historial, se llenará con el tiempo).
    - Rechazo activo + supervivencia KM por debajo del umbral → neutral
      (NO veto — a diferencia de otros filtros, aquí "supervivencia baja"
      no contradice la señal de analyze(), solo significa que este order
      block en concreto no añade confianza extra).
    - Rechazo activo + supervivencia KM por encima del umbral → boost.
    """
    has_rejection, survival, n = ob_engine.get_active_rejection(symbol, direction, min_samples)
    if not has_rejection:
        return 0.0, "sin_rechazo_de_order_block_activo", False
    if survival is None:
        return 0.0, f"order_block_datos_insuficientes(n={n}, min={min_samples})", False
    if survival >= survival_threshold:
        return (boost_amount,
                f"order_block_rechazo_confirma(supervivencia_km={survival:.0%}, n={n})",
                False)
    return 0.0, f"order_block_supervivencia_baja({survival:.0%} < {survival_threshold:.0%}, n={n})", False
