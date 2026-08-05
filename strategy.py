"""
Motor de senales -- puerto Python de ict_killzone_v2.pine.

Trabaja siempre sobre velas YA CERRADAS (nunca sobre la vela en curso,
igual que barstate.isconfirmed en Pine). evaluate_symbol() es una
funcion de (estado_previo, velas_nuevas) -> (estado_nuevo, señal?),
para que scanner.py solo tenga que guardar y pasar el estado por
simbolo entre ciclos.
"""
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import config as cfg
from bingx_client import Candle

log = logging.getLogger("strategy")

NY_TZ = ZoneInfo("America/New_York")

TF_MS = {"1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
         "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "1d": 86_400_000}


def tf_to_ms(tf: str) -> int:
    return TF_MS.get(tf, 300_000)


@dataclass
class FVG:
    top: float
    bot: float
    ce: float
    bull: bool
    touched: bool = False
    done: bool = False

    def to_dict(self) -> dict:
        return {"top": self.top, "bot": self.bot, "ce": self.ce, "bull": self.bull,
                "touched": self.touched, "done": self.done}

    @staticmethod
    def from_dict(d: Optional[dict]) -> Optional["FVG"]:
        if not d:
            return None
        return FVG(d["top"], d["bot"], d["ce"], d["bull"], d.get("touched", False), d.get("done", False))


@dataclass
class Signal:
    symbol: str
    direction: str  # "LONG" | "SHORT"
    entry: float
    sl: float
    tp1: float
    tp2: float
    rr: float
    kill_zone: str
    reason: str
    funding_rate: Optional[float] = None
    oi_change_pct: Optional[float] = None


@dataclass
class SymbolState:
    last_open_time: int = 0
    setup_side: Optional[str] = None       # "bull" | "bear" | None
    setup_open_time: int = 0
    fvg: Optional[FVG] = None
    fvg_open_time: int = 0
    oi_at_setup: Optional[float] = None    # OI en el momento del barrido, para comparar en la confirmacion

    def to_dict(self) -> dict:
        return {
            "last_open_time": self.last_open_time,
            "setup_side": self.setup_side,
            "setup_open_time": self.setup_open_time,
            "fvg": self.fvg.to_dict() if self.fvg else None,
            "fvg_open_time": self.fvg_open_time,
            "oi_at_setup": self.oi_at_setup,
        }

    @staticmethod
    def from_dict(d: dict) -> "SymbolState":
        return SymbolState(
            last_open_time=d.get("last_open_time", 0),
            setup_side=d.get("setup_side"),
            setup_open_time=d.get("setup_open_time", 0),
            fvg=FVG.from_dict(d.get("fvg")),
            fvg_open_time=d.get("fvg_open_time", 0),
            oi_at_setup=d.get("oi_at_setup"),
        )


# ══════════════════════════════════════════════════════
# Indicadores base
# ══════════════════════════════════════════════════════
def atr(candles: list, period: int = 14) -> float:
    if len(candles) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        c, p = candles[i], candles[i - 1]
        trs.append(max(c.high - c.low, abs(c.high - p.close), abs(c.low - p.close)))
    recent = trs[-period:]
    return sum(recent) / len(recent) if recent else 0.0


def ema(values: list, length: int) -> float:
    if not values:
        return 0.0
    k = 2.0 / (length + 1)
    e = values[0]
    for v in values[1:]:
        e = v * k + e * (1 - k)
    return e


# ══════════════════════════════════════════════════════
# Kill zones -- DST-aware via zoneinfo (mejora sobre el Pine,
# que dependia de offsets fijos y de la sesion del exchange)
# ══════════════════════════════════════════════════════
def active_kill_zone(ts_ms: int) -> Optional[str]:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(NY_TZ)
    hm = dt.hour * 100 + dt.minute
    if cfg.KZ_LONDON and 200 <= hm < 500:
        return "LON"
    if cfg.KZ_NY_AM and 830 <= hm < 1100:
        return "NYam"
    if cfg.KZ_NY_PM and 1330 <= hm < 1600:
        return "NYpm"
    if cfg.KZ_ASIA and hm >= 2000:
        return "ASIA"
    return None


def _parse_session(session_str: str) -> tuple:
    a, b = session_str.split("-")
    return int(a), int(b)


def range_from_session(candles: list, sess_start_hm: int, sess_end_hm: int) -> tuple:
    """Maximo/minimo de la ventana horaria (hora NY) mas reciente YA CERRADA.
    Devuelve (None, None) mientras la ventana de hoy sigue abierta -- igual
    que el Pine, que solo sella sRngH/sRngL cuando termina la sesion."""
    if not candles:
        return None, None
    last_dt = datetime.fromtimestamp(candles[-1].open_time / 1000, tz=timezone.utc).astimezone(NY_TZ)
    last_hm = last_dt.hour * 100 + last_dt.minute

    if sess_start_hm <= last_hm < sess_end_hm:
        return None, None  # ventana de hoy aun en curso, no ha sellado

    target_day = last_dt.date()
    if last_hm < sess_start_hm:
        target_day = target_day - timedelta(days=1)

    highs, lows = [], []
    for c in candles:
        dt = datetime.fromtimestamp(c.open_time / 1000, tz=timezone.utc).astimezone(NY_TZ)
        hm = dt.hour * 100 + dt.minute
        if dt.date() == target_day and sess_start_hm <= hm < sess_end_hm:
            highs.append(c.high)
            lows.append(c.low)
    if not highs:
        return None, None
    return max(highs), min(lows)


# ══════════════════════════════════════════════════════
# Liquidez
# ══════════════════════════════════════════════════════
def prev_day_high_low(daily: list) -> tuple:
    if len(daily) < 2:
        return None, None
    d = daily[-2]  # ultimo dia YA CERRADO
    return d.high, d.low


def pivots(candles: list, length: int) -> tuple:
    """(pivot_highs, pivot_lows) confirmados con `length` velas a cada lado."""
    highs, lows = [], []
    n = len(candles)
    for i in range(length, n - length):
        window = candles[i - length: i + length + 1]
        c = candles[i]
        if c.high == max(w.high for w in window):
            highs.append(c.high)
        if c.low == min(w.low for w in window):
            lows.append(c.low)
    return highs, lows


def detect_eq_levels(candles: list, atr_val: float) -> tuple:
    highs, lows = pivots(candles, cfg.EQ_PIVOT_LEN)
    tol = atr_val * cfg.EQ_TOL_ATR
    eqh = eql = None
    if len(highs) >= 2 and abs(highs[-1] - highs[-2]) <= tol:
        eqh = (highs[-1] + highs[-2]) / 2
    if len(lows) >= 2 and abs(lows[-1] - lows[-2]) <= tol:
        eql = (lows[-1] + lows[-2]) / 2
    return eqh, eql


def detect_sweep(last: Candle, levels_high: list, levels_low: list) -> tuple:
    """Evalua SOLO la ultima vela cerrada. Devuelve (swp_high, swp_low, ref_high, ref_low)."""
    swp_h, ref_h = False, None
    for lvl in levels_high:
        if lvl is not None and last.high > lvl and last.close < lvl and last.open < lvl:
            swp_h, ref_h = True, lvl
            break
    swp_l, ref_l = False, None
    for lvl in levels_low:
        if lvl is not None and last.low < lvl and last.close > lvl and last.open > lvl:
            swp_l, ref_l = True, lvl
            break
    return swp_h, swp_l, ref_h, ref_l


# ══════════════════════════════════════════════════════
# FVG
# ══════════════════════════════════════════════════════
def find_fvg(candles: list, atr_val: float, want_bull: bool, want_bear: bool) -> Optional[FVG]:
    """Busca un gap de 3 velas usando las 3 ultimas velas cerradas.
    c1 (la del medio) debe ser una vela de displacement real."""
    if len(candles) < 3:
        return None
    c0, c1, c2 = candles[-3], candles[-2], candles[-1]

    disp_body = abs(c1.close - c1.open)
    if cfg.DISPLACEMENT_ATR > 0 and not (atr_val > 0 and disp_body >= atr_val * cfg.DISPLACEMENT_ATR):
        return None

    if want_bear:
        gap = c0.low - c2.high
        gap_ok = gap > 0 and (cfg.MIN_GAP_ATR <= 0 or (atr_val > 0 and gap >= atr_val * cfg.MIN_GAP_ATR))
        if gap_ok:
            top, bot = c0.low, c2.high
            return FVG(top=top, bot=bot, ce=(top + bot) / 2, bull=False)

    if want_bull:
        gap = c2.low - c0.high
        gap_ok = gap > 0 and (cfg.MIN_GAP_ATR <= 0 or (atr_val > 0 and gap >= atr_val * cfg.MIN_GAP_ATR))
        if gap_ok:
            top, bot = c2.low, c0.high
            return FVG(top=top, bot=bot, ce=(top + bot) / 2, bull=True)

    return None


# ══════════════════════════════════════════════════════
# Evaluacion completa de un simbolo
# ══════════════════════════════════════════════════════
def evaluate_symbol(
    symbol: str, ltf: list, htf: list, daily: list, state: SymbolState,
    funding_rate: Optional[float] = None, current_oi: Optional[float] = None,
) -> tuple:
    """Devuelve (nuevo_estado, Signal o None). No lanza excepciones por
    datos insuficientes: simplemente no genera señal."""
    if len(ltf) < max(60, cfg.EQ_PIVOT_LEN * 3):
        return state, None

    last = ltf[-1]
    if last.open_time == state.last_open_time:
        return state, None  # ya se evaluo esta vela en un ciclo anterior
    state.last_open_time = last.open_time

    tf_ms = tf_to_ms(cfg.TIMEFRAME)
    atr_val = atr(ltf, 14)
    if atr_val <= 0:
        return state, None

    kz = active_kill_zone(last.open_time)
    kz_ok = (not cfg.USE_KILL_ZONES) or (kz is not None)

    pdh, pdl = prev_day_high_low(daily)
    s_start, s_end = _parse_session(cfg.REFERENCE_RANGE)
    rng_h, rng_l = range_from_session(ltf, s_start, s_end)
    eqh, eql = detect_eq_levels(ltf, atr_val) if cfg.USE_EQ else (None, None)

    # ── Expirar el setup activo si se paso de ventana ──
    if state.setup_side and (last.open_time - state.setup_open_time) // tf_ms > cfg.SWEEP_EXPIRY_BARS:
        state.setup_side = None
        state.fvg = None

    # ── Sweep (solo cuenta si estamos en kill zone, cuando esta activo el filtro) ──
    if kz_ok:
        swp_h, swp_l, ref_h, ref_l = detect_sweep(last, [pdh, rng_h, eqh], [pdl, rng_l, eql])
        if swp_h:
            state.setup_side = "bear"
            state.setup_open_time = last.open_time
            state.fvg = None
            state.oi_at_setup = current_oi
        elif swp_l:
            state.setup_side = "bull"
            state.setup_open_time = last.open_time
            state.fvg = None
            state.oi_at_setup = current_oi

    # ── Buscar FVG para el setup activo ──
    if state.setup_side and state.fvg is None:
        fvg = find_fvg(ltf, atr_val, want_bull=(state.setup_side == "bull"), want_bear=(state.setup_side == "bear"))
        if fvg:
            state.fvg = fvg
            state.fvg_open_time = last.open_time

    signal = None
    if state.fvg and not state.fvg.done:
        max_bars = cfg.CE_EXPIRY_BARS if cfg.ENTRY_MODE == "CE" else cfg.FVG_EXPIRY_BARS
        if (last.open_time - state.fvg_open_time) // tf_ms > max_bars:
            state.fvg = None
        else:
            f = state.fvg
            use_ce = cfg.ENTRY_MODE == "CE"

            if f.bull:
                if last.low <= f.top:
                    f.touched = True
                triggered = (last.low <= f.ce) if use_ce else (f.touched and last.close > f.top)
                if triggered:
                    f.done = True
                    entry = f.ce if use_ce else last.close
                    sl = f.bot - cfg.SL_BUFFER_ATR * atr_val
                    r = entry - sl
                    if r > 0:
                        tgt = entry + cfg.RR_FIXED_FALLBACK * r
                        if cfg.USE_RANGE_TP:
                            candidates = [x for x in (rng_h, pdh) if x is not None and x > entry]
                            if candidates:
                                tgt = max(candidates)
                        rr = (tgt - entry) / r
                        signal = Signal(symbol, "LONG", entry, sl, entry + cfg.PARTIAL_TP_R * r, tgt, rr, kz or "off", "sweep+fvg")
            else:
                if last.high >= f.bot:
                    f.touched = True
                triggered = (last.high >= f.ce) if use_ce else (f.touched and last.close < f.bot)
                if triggered:
                    f.done = True
                    entry = f.ce if use_ce else last.close
                    sl = f.top + cfg.SL_BUFFER_ATR * atr_val
                    r = sl - entry
                    if r > 0:
                        tgt = entry - cfg.RR_FIXED_FALLBACK * r
                        if cfg.USE_RANGE_TP:
                            candidates = [x for x in (rng_l, pdl) if x is not None and x < entry]
                            if candidates:
                                tgt = min(candidates)
                        rr = (entry - tgt) / r
                        signal = Signal(symbol, "SHORT", entry, sl, entry - cfg.PARTIAL_TP_R * r, tgt, rr, kz or "off", "sweep+fvg")

    if signal is None:
        return state, None

    # ── Filtros sobre la señal ya formada ──
    if signal.rr < cfg.MIN_RR:
        return state, None

    if cfg.DIRECTION == "LONG" and signal.direction == "SHORT":
        return state, None
    if cfg.DIRECTION == "SHORT" and signal.direction == "LONG":
        return state, None

    if cfg.KZ_ONLY_ENTRY and not kz_ok:
        return state, None

    if cfg.USE_HTF_BIAS and len(htf) >= cfg.HTF_EMA_LEN + 5:
        closes = [c.close for c in htf[-(cfg.HTF_EMA_LEN * 3):]]
        htf_ema = ema(closes, cfg.HTF_EMA_LEN)
        htf_close = htf[-1].close
        if signal.direction == "LONG" and htf_close <= htf_ema:
            return state, None
        if signal.direction == "SHORT" and htf_close >= htf_ema:
            return state, None

    if cfg.USE_PREMIUM_DISCOUNT:
        levels = [x for x in (rng_h, rng_l, pdh, pdl) if x is not None]
        if len(levels) >= 2:
            mid = (max(levels) + min(levels)) / 2
            if signal.direction == "LONG" and signal.entry >= mid:
                return state, None
            if signal.direction == "SHORT" and signal.entry <= mid:
                return state, None

    if cfg.USE_FUNDING_FILTER:
        if funding_rate is None:
            return state, None  # sin dato -> no se arriesga, se descarta
        if signal.direction == "LONG" and funding_rate > -cfg.FUNDING_MIN_ABS:
            return state, None
        if signal.direction == "SHORT" and funding_rate < cfg.FUNDING_MIN_ABS:
            return state, None
    signal.funding_rate = funding_rate

    oi_change_pct = None
    if state.oi_at_setup is not None and current_oi is not None and state.oi_at_setup > 0:
        oi_change_pct = (current_oi - state.oi_at_setup) / state.oi_at_setup * 100.0
    signal.oi_change_pct = oi_change_pct

    if cfg.USE_OI_FILTER:
        if oi_change_pct is None:
            return state, None  # sin dato -> no se arriesga, se descarta
        if oi_change_pct > cfg.OI_MAX_INCREASE_PCT:
            return state, None  # OI subiendo = posicion nueva contra la reversion, no flush

    return state, signal
