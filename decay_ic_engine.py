"""
QF×JP Bot #5 — Decay / Information Coefficient Engine
═══════════════════════════════════════════════════════════════════════════
Port de la sección L3 "DECAIMIENTO" del Pine original (QF Machine × JP
Fusion v3.6 PREDATOR). Mide si el score sigue teniendo poder predictivo
real sobre el retorno futuro, o si se degradó — un concepto genuino de
quant (Information Coefficient: correlación entre la predicción y el
retorno que de verdad ocurrió después), no solo "el score es alto".

CÓMO FUNCIONA:
  1. Cada vela nueva: calcula el retorno de ESA vela (fwd_ret) y lo
     empareja con el score de la vela ANTERIOR (norm_score[1] en Pine) —
     "¿lo que predijo el score de ayer se cumplió hoy?"
  2. Sobre una ventana rodante (ic_window, 40 velas por defecto), calcula
     la correlación de Pearson entre esos pares (score_pasado, retorno) —
     esto ES el Information Coefficient.
  3. Suaviza el |IC| con una media corta, y lo compara contra su propio
     pico reciente (decay_r = ic_actual / ic_pico) — si decay_r cae mucho,
     el poder predictivo se está degradando AHORA MISMO, aunque el score
     en sí siga pareciendo "alto".
  4. Umbral adaptativo: en vez de un corte fijo, usa el percentil 30 de
     |IC| de una ventana más larga — se ajusta solo al "ruido normal" de
     cada símbolo en cada momento.

STATEFUL — mismo patrón que order_block_km.py/funding_regime.py: hay que
llamar a update() cada ciclo de scan con el score y el close actuales, y
luego is_alive() para consultar el veredicto. No tiene sentido sin
historial — un símbolo nuevo siempre empieza con poder predictivo
"desconocido", no "muerto" (ver el caso n<5 en is_alive).
═══════════════════════════════════════════════════════════════════════════
"""
import logging
from dataclasses import dataclass, field

log = logging.getLogger("decay_ic")


def _pearson(x: list, y: list) -> float:
    n = len(x)
    if n < 2:
        return 0.0
    mx, my = sum(x) / n, sum(y) / n
    cov = sum((x[i] - mx) * (y[i] - my) for i in range(n))
    vx = sum((v - mx) ** 2 for v in x)
    vy = sum((v - my) ** 2 for v in y)
    denom = (vx * vy) ** 0.5
    return cov / denom if denom > 1e-12 else 0.0


def _percentile(values: list, pct: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * (pct / 100.0)
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


@dataclass
class _SymbolDecayState:
    last_ts:      int   = 0
    score_hist:   list  = field(default_factory=list)
    close_hist:   list  = field(default_factory=list)
    ic_roll_hist: list  = field(default_factory=list)


class DecayICEngine:
    def __init__(self):
        self._state: dict[str, _SymbolDecayState] = {}

    def _get(self, symbol: str) -> _SymbolDecayState:
        if symbol not in self._state:
            self._state[symbol] = _SymbolDecayState()
        return self._state[symbol]

    def update(
        self, symbol: str, ts: int, norm_score: float, close: float,
        ic_window: int = 40, peak_window: int = 40,
        adapt_window_mult: int = 3,
    ) -> None:
        """Llamar UNA vez por símbolo por ciclo de scan, con el bar más reciente."""
        st = self._get(symbol)
        if ts <= st.last_ts:
            return
        st.last_ts = ts
        st.score_hist.append(norm_score)
        st.close_hist.append(close)

        max_keep = max(ic_window, peak_window, ic_window * adapt_window_mult) + 10
        if len(st.score_hist) > max_keep:
            st.score_hist = st.score_hist[-max_keep:]
            st.close_hist = st.close_hist[-max_keep:]

        n = len(st.close_hist)
        if n < ic_window + 2:
            return

        fwd_rets, lagged_scores = [], []
        for i in range(n - ic_window, n):
            if i < 1:
                continue
            prev_c = st.close_hist[i - 1]
            if prev_c == 0:
                continue
            fwd_rets.append((st.close_hist[i] - prev_c) / prev_c)
            lagged_scores.append(st.score_hist[i - 1])

        ic = _pearson(lagged_scores, fwd_rets)
        st.ic_roll_hist.append(abs(ic))

        cap = max(peak_window, ic_window * adapt_window_mult) + 5
        if len(st.ic_roll_hist) > cap:
            st.ic_roll_hist = st.ic_roll_hist[-cap:]

    def is_alive(
        self, symbol: str, decay_threshold: float = 0.40,
        peak_window: int = 40, adapt_window_mult: int = 3,
        adapt_pct: float = 30.0, use_adaptive: bool = True,
        smooth_n: int = 3,
    ) -> tuple[bool, float, str]:
        """
        Retorna (vivo, decay_ratio, detalle). Con menos de 5 lecturas de IC
        todavía, retorna vivo=True por defecto (símbolo nuevo, no "señal
        muerta" — simplemente no hay historial suficiente para juzgar).
        """
        st = self._get(symbol)
        if len(st.ic_roll_hist) < 5:
            return True, 0.5, "datos_insuficientes_para_IC"

        sn = min(smooth_n, len(st.ic_roll_hist))
        ic_roll = sum(st.ic_roll_hist[-sn:]) / sn

        peak_data = st.ic_roll_hist[-peak_window:] if len(st.ic_roll_hist) >= peak_window else st.ic_roll_hist
        ic_peak = max(peak_data) if peak_data else 0.0
        decay_r = (ic_roll / ic_peak) if ic_peak > 1e-9 else 0.5

        adapt_n = peak_window * adapt_window_mult
        adapt_data = st.ic_roll_hist[-adapt_n:] if len(st.ic_roll_hist) >= adapt_n else st.ic_roll_hist
        ic_adapt_thr = _percentile(adapt_data, adapt_pct)

        alive = decay_r >= decay_threshold or (use_adaptive and ic_roll >= ic_adapt_thr)
        detail = (f"decay_r={decay_r:.2f} ic_roll={ic_roll:.3f} "
                  f"ic_peak={ic_peak:.3f} thr_adapt={ic_adapt_thr:.3f}")
        return alive, decay_r, detail


decay_engine = DecayICEngine()
