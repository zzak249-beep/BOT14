"""
QF×JP Bot #5 — Walk-Forward Kelly Calibration
═══════════════════════════════════════════════════════════════════════════
Port de las secciones [WFWD] y [KEL] del Pine original. A diferencia del
Kelly de tus otros bots (que usa un win rate/RR ESTIMADO fijo en config),
esto recalcula el win rate en VIVO, sobre una ventana rodante de los
últimos N disparos de señal — el Kelly se ajusta solo según cómo está
funcionando de verdad el sistema ahora mismo, no según una suposición
estática.

CÓMO FUNCIONA:
  1. Cada vez que el score cruza POR ENCIMA del umbral STD (un "disparo"),
     se cuenta como una señal nueva en la ventana.
  2. N barras después (delay_bars, 3 por defecto — replica el comp_long[3]
     vs comp_long[4] del Pine), se comprueba si el precio avanzó al menos
     min_progress_atr × ATR en la dirección correcta desde el momento del
     disparo — si sí, cuenta como "ganadora".
  3. win_rate = ganadoras / disparos, sobre una ventana de tamaño fijo
     (window=50) que va rotando — los disparos más viejos se descartan.
  4. Ese win_rate rodante alimenta la fórmula de Kelly (fracción óptima
     de capital a arriesgar) en cada ciclo, no un valor fijo de config.

STATEFUL, por símbolo y por dirección (LONG/SHORT por separado, ya que
pueden tener comportamiento muy distinto). Necesita historial — con pocos
disparos todavía, usa un win_rate neutral (50%) por defecto, igual que el
Pine original.
═══════════════════════════════════════════════════════════════════════════
"""
import logging
from collections import deque
from dataclasses import dataclass, field

log = logging.getLogger("wf_kelly")


@dataclass
class _PendingTrigger:
    bar_count:   int
    direction:   str     # "LONG" / "SHORT"
    entry_close: float
    atr:         float


@dataclass
class _SymbolWFState:
    bar_count:        int   = 0
    prev_above_std:   dict  = field(default_factory=lambda: {"LONG": False, "SHORT": False})
    pending:          list  = field(default_factory=list)   # list[_PendingTrigger]
    long_results:     deque = field(default_factory=lambda: deque(maxlen=50))   # bool, True=ganó
    short_results:    deque = field(default_factory=lambda: deque(maxlen=50))


class WalkForwardKelly:
    def __init__(self, window: int = 50):
        self.window = window
        self._state: dict[str, _SymbolWFState] = {}

    def _get(self, symbol: str) -> _SymbolWFState:
        if symbol not in self._state:
            st = _SymbolWFState()
            st.long_results  = deque(maxlen=self.window)
            st.short_results = deque(maxlen=self.window)
            self._state[symbol] = st
        return self._state[symbol]

    def update(
        self, symbol: str, comp_long: float, comp_short: float,
        close: float, atr: float, thr_std: float,
        delay_bars: int = 3, min_progress_atr: float = 0.5,
    ) -> None:
        """
        Llamar una vez por símbolo por ciclo de scan, con el bar más
        reciente. Registra nuevos disparos (cruce sobre thr_std) y resuelve
        los pendientes que ya cumplieron delay_bars.
        """
        st = self._get(symbol)
        st.bar_count += 1

        above_long  = comp_long  >= thr_std
        above_short = comp_short >= thr_std
        if above_long and not st.prev_above_std["LONG"]:
            st.pending.append(_PendingTrigger(st.bar_count, "LONG", close, atr))
        if above_short and not st.prev_above_std["SHORT"]:
            st.pending.append(_PendingTrigger(st.bar_count, "SHORT", close, atr))
        st.prev_above_std["LONG"]  = above_long
        st.prev_above_std["SHORT"] = above_short

        still_pending = []
        for trig in st.pending:
            age = st.bar_count - trig.bar_count
            if age < delay_bars:
                still_pending.append(trig)
                continue
            min_prog = trig.atr * min_progress_atr if trig.atr > 0 else 0.0
            if trig.direction == "LONG":
                won = (close - trig.entry_close) >= min_prog
                st.long_results.append(won)
            else:
                won = (trig.entry_close - close) >= min_prog
                st.short_results.append(won)
        st.pending = still_pending

    def win_rate(self, symbol: str) -> tuple[float, float, float]:
        """Retorna (wr_long, wr_short, wr_avg), 0-1. Sin datos → 0.5 (neutral)."""
        st = self._get(symbol)
        wr_long  = (sum(st.long_results)  / len(st.long_results))  if st.long_results  else 0.5
        wr_short = (sum(st.short_results) / len(st.short_results)) if st.short_results else 0.5
        return wr_long, wr_short, (wr_long + wr_short) / 2.0

    def kelly_fraction(
        self, symbol: str, kelly_frac_cap: float = 0.25,
        rr: float = 1.8, max_f: float = 0.5,
    ) -> tuple[float, float]:
        """
        Kelly fraccionado usando el win rate rodante real en vez de un
        valor fijo de config. Retorna (kelly_f, win_rate_promedio).
        f* = (p*(b+1) - 1) / b, recortado por kelly_frac_cap y max_f — mismo
        criterio conservador (fracción de Kelly completo) que ya usas en
        risk_manager.py.
        """
        _, _, wr_avg = self.win_rate(symbol)
        b = rr
        f_raw = (wr_avg * (b + 1) - 1) / b if b > 0 else 0.0
        f = max(0.0, min(max_f, f_raw * kelly_frac_cap))
        return f, wr_avg


wf_kelly = WalkForwardKelly()
