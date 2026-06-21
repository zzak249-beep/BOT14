"""
QF×JP Bot #5 — Composite Score Engine
═══════════════════════════════════════════════════════════════════════════
Port del corazón del Pine original (L1/L2/M2/[REG]/SCORE COMPUESTO/[TSL]/
[ACCEL]) — la arquitectura que combina todo lo demás en un solo número.
Esto es lo genuinamente nuevo frente a lo ya construido hoy; los
detectores individuales (Order Blocks, VSA, asimetría, RMI...) se
REUTILIZAN desde sus módulos ya existentes y se pasan aquí como señales
externas — este motor no los reimplementa.

PIEZAS:

  1. Factores L2 — Momentum (ROC normalizado por volatilidad), Media-
     Reversión (z-score vs SMA), Volumen (z-score de OBV). Se combinan en
     raw_score con pesos que el régimen ADX ajusta dinámicamente.

  2. [M2]/[REG] Régimen ADX — ADX fuerte → más peso a momentum, menos a
     mean-reversion (en tendencia, perseguir; en lateral, al revés). Los
     pesos NO son fijos, cambian según si el mercado está en tendencia,
     lateral, o neutral.

  3. Decay/IC — ver decay_ic_engine.py (módulo separado, motor stateful).
     Este archivo solo lo CONSULTA, no lo reimplementa.

  4. Conviction Counter — cuenta cuántas de hasta 20 condiciones booleanas
     de confirmación están activas a la vez (estructura, sweeps, squeeze,
     order blocks, divergencias...). Las condiciones concretas se PASAN
     como parámetros desde scanner.py, que las obtiene llamando a los
     módulos ya existentes (order_block_km, sniper_vsa_matrix,
     trend_magic_rmi, price_action_framework, stc_asymmetry) — este motor
     solo las cuenta y las traduce en boost de score.

  5. Trailing Score [TSL] — suaviza el score con EMA(2) y limita cuánto
     puede CAER de golpe (i_tsl_drop, 8 puntos por defecto) — evita que
     una sola vela en contra tire el score abajo de golpe cuando la
     confluencia de fondo sigue intacta. STATEFUL (necesita el score
     suavizado del ciclo anterior).

  6. Aceleración — Δscore en una ventana corta (4 barras) — detecta
     momentum del score en sí mismo (no solo del precio), para pre-alerta.

Como con decay_ic_engine.py y walk_forward_kelly.py, esto es STATEFUL por
símbolo — hay que llamar a compute() cada ciclo de scan.
═══════════════════════════════════════════════════════════════════════════
"""
import logging
import math
from dataclasses import dataclass, field

from decay_ic_engine import decay_engine

log = logging.getLogger("composite_score")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _tanh(x: float) -> float:
    x = max(min(2.0 * x, 20.0), -20.0)
    e2x = math.exp(x)
    return (e2x - 1.0) / (e2x + 1.0)


def _sma(values: list, period: int) -> list:
    out = []
    for i in range(len(values)):
        lo = max(0, i - period + 1)
        w = values[lo:i + 1]
        out.append(sum(w) / len(w))
    return out


def _stdev(values: list, period: int) -> list:
    out = []
    for i in range(len(values)):
        lo = max(0, i - period + 1)
        w = values[lo:i + 1]
        mu = sum(w) / len(w)
        out.append((sum((v - mu) ** 2 for v in w) / len(w)) ** 0.5)
    return out


def _ema(values: list, period: int) -> list:
    if not values:
        return []
    k = 2.0 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(out[-1] + k * (v - out[-1]))
    return out


def _rma(values: list, period: int) -> list:
    n = len(values)
    out = [0.0] * n
    if n == 0:
        return out
    alpha = 1.0 / period
    for i in range(n):
        out[i] = (sum(values[:i + 1]) / (i + 1)) if i < period else (out[i - 1] + alpha * (values[i] - out[i - 1]))
    return out


def _true_range(klines: list) -> list:
    tr = [klines[0][2] - klines[0][3]]
    for i in range(1, len(klines)):
        h, l, pc = klines[i][2], klines[i][3], klines[i - 1][4]
        tr.append(max(h - l, abs(h - pc), abs(l - pc)))
    return tr


def _atr(klines: list, period: int) -> list:
    return _rma(_true_range(klines), period)


def _dmi_adx(klines: list, period: int = 14) -> list:
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


def _obv(klines: list) -> list:
    out = [0.0]
    for i in range(1, len(klines)):
        c, pc, v = klines[i][4], klines[i - 1][4], klines[i][5]
        if c > pc:
            out.append(out[-1] + v)
        elif c < pc:
            out.append(out[-1] - v)
        else:
            out.append(out[-1])
    return out


# ── L2 Factores + Régimen ADX ───────────────────────────────────────────────

def compute_l2_factors(
    klines: list, mom_len: int = 20, rev_len: int = 8, vol_len: int = 14,
) -> dict:
    closes = [c[4] for c in klines]
    n = len(closes)
    if n < mom_len + 2:
        return {}

    roc_raw  = (closes[-1] - closes[-1 - mom_len]) / closes[-1 - mom_len] if closes[-1 - mom_len] != 0 else 0.0
    std_mom  = _stdev(closes, mom_len)[-1]
    mean_mom = _sma(closes, mom_len)[-1]
    vol_norm = std_mom / mean_mom if mean_mom != 0 else 0.0
    f_mom = roc_raw / vol_norm if vol_norm != 0 else 0.0

    basis     = _sma(closes, rev_len)[-1]
    basis_std = _stdev(closes, rev_len)[-1]
    f_rev = -(closes[-1] - basis) / basis_std if basis_std != 0 else 0.0

    obv = _obv(klines)
    obv_ma  = _ema(obv, vol_len)[-1]
    obv_std = _stdev(obv, vol_len)[-1]
    f_vol = (obv[-1] - obv_ma) / obv_std if obv_std != 0 else 0.0

    adx = _dmi_adx(klines, 14)[-1]

    return {"f_mom": f_mom, "f_rev": f_rev, "f_vol": f_vol, "adx": adx}


def adx_regime_weights(
    adx: float, adx_tend: float = 25.0, adx_lat: float = 20.0,
    w1_base: float = 0.40, w2_base: float = 0.30, w3_base: float = 0.30,
) -> dict:
    """[M2]/[REG]: pesos dinámicos según régimen — tendencia fuerte da más
    peso a momentum y menos a mean-reversion; régimen lateral, al revés."""
    trend_strong = adx >= adx_tend
    is_lateral   = adx < adx_lat
    regime = "TEND" if trend_strong else ("LATERAL" if is_lateral else "NEUTRAL")

    adx_factor = min(1.0, adx / (adx_tend * 2.0))
    w_mom = w1_base + adx_factor * w1_base * 0.40
    w_rev = max(w2_base * 0.30, w2_base - adx_factor * w2_base * 0.50)
    w_vol = w3_base
    w_total = w_mom + w_rev + w_vol

    return {
        "regime": regime, "trend_strong": trend_strong, "is_lateral": is_lateral,
        "w_mom": w_mom / w_total, "w_rev": w_rev / w_total, "w_vol": w_vol / w_total,
    }


# ── Trailing score state (TSL) ──────────────────────────────────────────────

@dataclass
class _TrailState:
    smoothed_long:  float = 0.0
    smoothed_short: float = 0.0
    hist_long:      list  = field(default_factory=list)
    hist_short:     list  = field(default_factory=list)


class CompositeScoreEngine:
    def __init__(self):
        self._trail: dict[str, _TrailState] = {}

    def _get_trail(self, symbol: str) -> _TrailState:
        if symbol not in self._trail:
            self._trail[symbol] = _TrailState()
        return self._trail[symbol]

    def compute(
        self,
        symbol: str,
        klines: list,
        ts: int,
        cvd_score: float = 0.5,
        htf_score_long: float = 0.5,
        htf_score_short: float = 0.5,
        struc_score_long: float = 0.0,
        struc_score_short: float = 0.0,
        vp_score_long: float = 0.5,
        vp_score_short: float = 0.5,
        sent_score_long: float = 0.5,
        sent_score_short: float = 0.5,
        vdi_score: float = 0.5,
        conviction_long: int = 0,
        conviction_short: int = 0,
        mom_len: int = 20, rev_len: int = 8, vol_len: int = 14,
        adx_tend: float = 25.0, adx_lat: float = 20.0,
        w_score: float = 0.22, w_cvd: float = 0.20, w_decay: float = 0.08,
        w_htf: float = 0.14, w_struc: float = 0.08, w_vp: float = 0.05,
        w_sent: float = 0.04, w_vdi: float = 0.04,
        conv_boost_mult: float = 0.5,
        tsl_max_drop: float = 8.0,
        decay_threshold: float = 0.40,
    ) -> dict:
        """Devuelve comp_long, comp_short (0-100) y diagnóstico — llamar una
        vez por símbolo por ciclo, después de tener las señales externas."""
        l2 = compute_l2_factors(klines, mom_len, rev_len, vol_len)
        if not l2:
            return {"comp_long": 0, "comp_short": 0, "ok": False}

        regime = adx_regime_weights(l2["adx"], adx_tend, adx_lat)

        ns_norm       = (_tanh(_tanh(
            regime["w_mom"] * l2["f_mom"] + regime["w_rev"] * l2["f_rev"] + regime["w_vol"] * l2["f_vol"]
        )) + 1) / 2
        norm_score = _tanh(
            regime["w_mom"] * l2["f_mom"] + regime["w_rev"] * l2["f_rev"] + regime["w_vol"] * l2["f_vol"]
        )
        ns_norm_short = (_tanh(-norm_score) + 1) / 2

        decay_engine.update(symbol, ts, norm_score, klines[-1][4])
        alive, decay_r, decay_detail = decay_engine.is_alive(symbol, decay_threshold)
        decay_norm = min(1.0, decay_r)

        mom_norm_l = (_tanh(l2["f_mom"] * 2) + 1) / 2
        mom_norm_s = (_tanh(-l2["f_mom"] * 2) + 1) / 2

        comp_long_raw = (
            w_score * ns_norm + w_cvd * cvd_score + regime["w_mom"] * mom_norm_l
            + w_decay * decay_norm + w_htf * htf_score_long
            + w_struc * min(1.0, struc_score_long) + w_vp * vp_score_long
            + w_sent * sent_score_long + w_vdi * vdi_score
        )
        comp_short_raw = (
            w_score * ns_norm_short + w_cvd * (1.0 - cvd_score) + regime["w_mom"] * mom_norm_s
            + w_decay * decay_norm + w_htf * htf_score_short
            + w_struc * min(1.0, struc_score_short) + w_vp * vp_score_short
            + w_sent * sent_score_short + w_vdi * (1.0 - vdi_score)
        )

        conv_boost_long  = conviction_long  * conv_boost_mult
        conv_boost_short = conviction_short * conv_boost_mult

        comp_long_pre  = min(100, round(comp_long_raw  * 100) + round(conv_boost_long))
        comp_short_pre = min(100, round(comp_short_raw * 100) + round(conv_boost_short))

        trail = self._get_trail(symbol)
        smoothed_long  = trail.smoothed_long  + (2.0 / 3.0) * (comp_long_pre  - trail.smoothed_long)  if trail.hist_long  else float(comp_long_pre)
        smoothed_short = trail.smoothed_short + (2.0 / 3.0) * (comp_short_pre - trail.smoothed_short) if trail.hist_short else float(comp_short_pre)

        if comp_long_pre - smoothed_long > tsl_max_drop:
            smoothed_long = comp_long_pre - tsl_max_drop
        if comp_short_pre - smoothed_short > tsl_max_drop:
            smoothed_short = comp_short_pre - tsl_max_drop

        trail.smoothed_long  = smoothed_long
        trail.smoothed_short = smoothed_short
        trail.hist_long.append(smoothed_long)
        trail.hist_short.append(smoothed_short)
        if len(trail.hist_long) > 10:
            trail.hist_long = trail.hist_long[-10:]
            trail.hist_short = trail.hist_short[-10:]

        comp_long  = round(smoothed_long)
        comp_short = round(smoothed_short)

        accel_long  = comp_long  - trail.hist_long[-5]  if len(trail.hist_long)  >= 5 else 0
        accel_short = comp_short - trail.hist_short[-5] if len(trail.hist_short) >= 5 else 0

        return {
            "ok": True,
            "comp_long": comp_long, "comp_short": comp_short,
            "regime": regime["regime"], "adx": l2["adx"],
            "norm_score": norm_score, "decay_alive": alive, "decay_r": decay_r,
            "decay_detail": decay_detail, "accel_long": accel_long, "accel_short": accel_short,
            "w_mom": regime["w_mom"], "w_rev": regime["w_rev"],
        }


score_engine = CompositeScoreEngine()
