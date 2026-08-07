"""
Motor de senales -- puerto Python de ict_killzone_v2.pine.

Trabaja siempre sobre velas YA CERRADAS (nunca sobre la vela en curso,
igual que barstate.isconfirmed en Pine). evaluate_symbol() es una
funcion de (estado_previo, velas_nuevas) -> (estado_nuevo, señal?),
para que scanner.py solo tenga que guardar y pasar el estado por
simbolo entre ciclos.
"""
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import config as cfg
from bingx_client import Candle

log = logging.getLogger("strategy")

NY_TZ = ZoneInfo("America/New_York")

# ══════════════════════════════════════════════════════
# Diagnostico del embudo -- cuenta en que etapa se cae cada intento,
# para poder ver en el log POR QUE un ciclo da 0 señales en vez de
# adivinarlo. Se resetea una vez por ciclo desde scanner.py.
# ══════════════════════════════════════════════════════
_stats = {
    "sweeps": 0, "fvgs_formed": 0, "confirmations": 0,
    "rejected_rr": 0, "rejected_direction": 0, "rejected_kz_only": 0,
    "rejected_htf": 0, "rejected_premium_discount": 0,
    "rejected_funding": 0, "rejected_oi": 0, "signals": 0,
}


def reset_cycle_stats() -> None:
    for k in _stats:
        _stats[k] = 0


def get_cycle_stats() -> dict:
    return dict(_stats)


# ══════════════════════════════════════════════════════
# Lead-lag: estado del simbolo lider (BTC), compartido entre TODOS los
# simbolos -- no es por-simbolo como SymbolState. En memoria, se
# resetea al reiniciar (aceptable: se repuebla con el siguiente sweep
# de BTC, no hace falta persistirlo en disco).
# ══════════════════════════════════════════════════════
_lead_state = {"direction": None, "at_ms": 0}


def _update_lead_state(symbol: str, direction: str, at_ms: int) -> None:
    if symbol.upper() == cfg.LEAD_SYMBOL.upper():
        _lead_state["direction"] = direction
        _lead_state["at_ms"] = at_ms


def _lead_confirms(direction: str, now_ms: int) -> Optional[bool]:
    """None = LEAD_LAG apagado o sin dato reciente del lider (no se
    etiqueta). True/False = hay un lead reciente y coincide o no."""
    if not cfg.USE_LEAD_LAG or _lead_state["direction"] is None:
        return None
    age_min = (now_ms - _lead_state["at_ms"]) / 60000.0
    if age_min > cfg.LEAD_LAG_WINDOW_MIN or age_min < 0:
        return None
    lead_bull = _lead_state["direction"] == "bull"
    return lead_bull == (direction == "LONG")

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
    path: str = "REV"  # "REV" (Ruta A: sweep+FVG) | "CONT" (Ruta B: CHoCH+golden zone)
    lead_confirmed: Optional[bool] = None  # None=sin dato/apagado, True=el simbolo lider se movio igual reciente, False=en contra


@dataclass
class SymbolState:
    last_open_time: int = 0
    setup_side: Optional[str] = None       # "bull" | "bear" | None
    setup_open_time: int = 0
    setup_provisional: bool = False        # el sweep vino de una vela AUN en formacion, pendiente de revalidar al cierre real
    fvg: Optional[FVG] = None
    fvg_open_time: int = 0
    oi_at_setup: Optional[float] = None    # OI en el momento del barrido, para comparar en la confirmacion
    # ── Ruta B (Continuacion): estructura + fib golden zone ──
    structure_bias: int = 0                # +1 alcista, -1 bajista, 0 sin definir
    last_broken_high: Optional[float] = None  # precio del ultimo swing high roto -- evita recontar el mismo
    last_broken_low: Optional[float] = None
    fib_swing_high: Optional[float] = None
    fib_swing_low: Optional[float] = None
    fib_direction: int = 0                 # +1 continuacion alcista, -1 bajista
    fib_high_is_live: bool = False         # el ancla alta sigue extendiendose con nuevos maximos
    fib_low_is_live: bool = False          # el ancla baja sigue extendiendose con nuevos minimos
    cont_consumed: bool = False            # ya se disparo una entrada Ruta B para este anclaje

    def to_dict(self) -> dict:
        return {
            "last_open_time": self.last_open_time,
            "setup_side": self.setup_side,
            "setup_provisional": self.setup_provisional,
            "setup_open_time": self.setup_open_time,
            "fvg": self.fvg.to_dict() if self.fvg else None,
            "fvg_open_time": self.fvg_open_time,
            "oi_at_setup": self.oi_at_setup,
            "structure_bias": self.structure_bias,
            "last_broken_high": self.last_broken_high,
            "last_broken_low": self.last_broken_low,
            "fib_swing_high": self.fib_swing_high,
            "fib_swing_low": self.fib_swing_low,
            "fib_direction": self.fib_direction,
            "fib_high_is_live": self.fib_high_is_live,
            "fib_low_is_live": self.fib_low_is_live,
            "cont_consumed": self.cont_consumed,
        }

    @staticmethod
    def from_dict(d: dict) -> "SymbolState":
        return SymbolState(
            last_open_time=d.get("last_open_time", 0),
            setup_side=d.get("setup_side"),
            setup_provisional=d.get("setup_provisional", False),
            setup_open_time=d.get("setup_open_time", 0),
            fvg=FVG.from_dict(d.get("fvg")),
            fvg_open_time=d.get("fvg_open_time", 0),
            oi_at_setup=d.get("oi_at_setup"),
            structure_bias=d.get("structure_bias", 0),
            last_broken_high=d.get("last_broken_high"),
            last_broken_low=d.get("last_broken_low"),
            fib_swing_high=d.get("fib_swing_high"),
            fib_swing_low=d.get("fib_swing_low"),
            fib_direction=d.get("fib_direction", 0),
            fib_high_is_live=d.get("fib_high_is_live", False),
            fib_low_is_live=d.get("fib_low_is_live", False),
            cont_consumed=d.get("cont_consumed", False),
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


def latest_swing_levels(highs: list, lows: list) -> tuple:
    """Swing high/low confirmado mas reciente -- referencia de liquidez
    mucho mas frecuente que PDH/PDL (1/dia) o EQH/EQL (necesita dos
    pivotes casi iguales). FALTABA de detect_sweep(): con 900 simbolos,
    la mayoria del tiempo pdh/rng_h/eqh estan los tres vacios a la vez
    para un simbolo dado, y el sweep nunca llega a evaluarse siquiera."""
    return (highs[-1] if highs else None), (lows[-1] if lows else None)


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
    rng = max(last.high - last.low, 1e-12)  # evita division por cero en velas doji perfectas

    swp_h, ref_h = False, None
    for lvl in levels_high:
        if lvl is not None and last.high > lvl and last.close < lvl and last.open < lvl:
            wick_pct = (last.high - lvl) / rng * 100
            if wick_pct >= cfg.SWEEP_MIN_WICK_PCT:
                swp_h, ref_h = True, lvl
                break
    swp_l, ref_l = False, None
    for lvl in levels_low:
        if lvl is not None and last.low < lvl and last.close > lvl and last.open > lvl:
            wick_pct = (lvl - last.low) / rng * 100
            if wick_pct >= cfg.SWEEP_MIN_WICK_PCT:
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


def is_bullish_engulf(c0: Candle, c1: Candle) -> bool:
    """c0 = vela anterior, c1 = vela actual. Envolvente alcista clasica:
    cuerpo actual mayor que el anterior, cierra por encima de la apertura
    anterior y abre por debajo del cierre anterior."""
    return (c1.close > c1.open and c0.close < c0.open
            and c1.close > c0.open and c1.open < c0.close)


def is_bearish_engulf(c0: Candle, c1: Candle) -> bool:
    return (c1.close < c1.open and c0.close > c0.open
            and c1.close < c0.open and c1.open > c0.close)


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
    now_ms = int(time.time() * 1000)
    tf_ms = tf_to_ms(cfg.TIMEFRAME)
    est_close = last.close_time if last.close_time is not None else last.open_time + tf_ms - 1
    is_closed = now_ms >= est_close

    if not is_closed:
        if not cfg.INTRA_CANDLE_SWEEP:
            return state, None  # vela sin cerrar, modo intra-vela apagado -- comportamiento por defecto: esperar al cierre
        # Intra-vela activo: NO se marca last_open_time todavia -- se
        # sigue revisando esta misma vela en formacion cada ciclo, y se
        # revalida contra los valores finales en cuanto cierre de verdad.
    else:
        if last.open_time == state.last_open_time:
            return state, None  # esta vela cerrada ya se proceso del todo
        if state.setup_provisional and state.setup_open_time == last.open_time:
            # El setup activo vino de un sweep visto a medio formar en
            # ESTA vela -- se descarta y se re-evalua desde cero contra
            # el cierre real, mas abajo. Un sweep provisional que ya no
            # se sostiene con el cierre definitivo no debe quedar vivo.
            state.setup_side = None
            state.setup_provisional = False
        state.last_open_time = last.open_time

    atr_val = atr(ltf, 14)
    if atr_val <= 0:
        return state, None

    kz = active_kill_zone(last.open_time)
    kz_ok = (not cfg.USE_KILL_ZONES) or (kz is not None)

    pdh, pdl = prev_day_high_low(daily)
    s_start, s_end = _parse_session(cfg.REFERENCE_RANGE)
    rng_h, rng_l = range_from_session(ltf, s_start, s_end)
    piv_highs, piv_lows = pivots(ltf, cfg.EQ_PIVOT_LEN)
    sw_h, sw_l = latest_swing_levels(piv_highs, piv_lows)
    eqh = eql = None
    if cfg.USE_EQ:
        tol = atr_val * cfg.EQ_TOL_ATR
        if len(piv_highs) >= 2 and abs(piv_highs[-1] - piv_highs[-2]) <= tol:
            eqh = (piv_highs[-1] + piv_highs[-2]) / 2
        if len(piv_lows) >= 2 and abs(piv_lows[-1] - piv_lows[-2]) <= tol:
            eql = (piv_lows[-1] + piv_lows[-2]) / 2

    # ── Expirar el setup activo si se paso de ventana ──
    if state.setup_side and (last.open_time - state.setup_open_time) // tf_ms > cfg.SWEEP_EXPIRY_BARS:
        state.setup_side = None
        state.fvg = None

    # ── Sweep (solo cuenta si estamos en kill zone, cuando esta activo el filtro) ──
    if cfg.USE_PATH_A and kz_ok:
        swp_h, swp_l, ref_h, ref_l = detect_sweep(last, [sw_h, pdh, rng_h, eqh], [sw_l, pdl, rng_l, eql])
        if swp_h:
            _stats["sweeps"] += 1
            state.setup_side = "bear"
            state.setup_open_time = last.open_time
            state.setup_provisional = not is_closed
            state.fvg = None
            state.oi_at_setup = current_oi
            _update_lead_state(symbol, "bear", last.open_time)
        elif swp_l:
            _stats["sweeps"] += 1
            state.setup_side = "bull"
            state.setup_open_time = last.open_time
            state.setup_provisional = not is_closed
            state.fvg = None
            state.oi_at_setup = current_oi
            _update_lead_state(symbol, "bull", last.open_time)

    # ── Buscar FVG para el setup activo (exige vela cerrada -- la
    # geometria del gap depende de c2=ultima vela, que no puede ser la
    # que aun se esta formando aunque el sweep si sea provisional) ──
    if state.setup_side and state.fvg is None and is_closed:
        fvg = find_fvg(ltf, atr_val, want_bull=(state.setup_side == "bull"), want_bear=(state.setup_side == "bear"))
        if fvg:
            state.fvg = fvg
            state.fvg_open_time = last.open_time
            _stats["fvgs_formed"] += 1

    signal_a = None
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
                triggered = is_closed and ((last.low <= f.ce) if use_ce else (f.touched and last.close > f.top))
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
                        _stats["confirmations"] += 1
                        signal_a = Signal(symbol, "LONG", entry, sl, entry + cfg.PARTIAL_TP_R * r, tgt, rr, kz or "off", "sweep+fvg", path="REV")
            else:
                if last.high >= f.bot:
                    f.touched = True
                triggered = is_closed and ((last.high >= f.ce) if use_ce else (f.touched and last.close < f.bot))
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
                        _stats["confirmations"] += 1
                        signal_a = Signal(symbol, "SHORT", entry, sl, entry - cfg.PARTIAL_TP_R * r, tgt, rr, kz or "off", "sweep+fvg", path="REV")

    # ══════════════════════════════════════════════════════
    # RUTA B: Continuacion (CHoCH + retroceso a golden zone)
    # Independiente de la Ruta A -- comparte pivotes de estructura
    # con SWING_LEN propio (no el EQ_PIVOT_LEN de arriba).
    # ══════════════════════════════════════════════════════
    signal_b = None
    if cfg.USE_PATH_B and len(ltf) >= cfg.SWING_LEN * 3:
        struct_highs, struct_lows = pivots(ltf, cfg.SWING_LEN)
        struct_sw_h = struct_highs[-1] if struct_highs else None
        struct_sw_l = struct_lows[-1] if struct_lows else None

        is_choch = False
        is_bull_break = False
        is_bear_break = False

        bull_break = (struct_sw_h is not None and last.close > struct_sw_h
                      and struct_sw_h != state.last_broken_high)
        bear_break = (struct_sw_l is not None and last.close < struct_sw_l
                      and struct_sw_l != state.last_broken_low)

        if bull_break and bear_break:
            # mismo criterio que el Pine: prioriza CHoCH sobre BOS
            if state.structure_bias <= 0:
                bear_break = False
            else:
                bull_break = False

        if bull_break:
            is_choch = state.structure_bias <= 0
            state.structure_bias = 1
            is_bull_break = True
            state.last_broken_high = struct_sw_h
        if bear_break:
            is_choch = state.structure_bias >= 0
            state.structure_bias = -1
            is_bear_break = True
            state.last_broken_low = struct_sw_l

        if is_choch and is_bull_break:
            state.fib_direction = 1
            state.fib_swing_high = last.high
            state.fib_high_is_live = True
            state.fib_swing_low = struct_sw_l
            state.fib_low_is_live = False
            state.cont_consumed = False
        if is_choch and is_bear_break:
            state.fib_direction = -1
            state.fib_swing_low = last.low
            state.fib_low_is_live = True
            state.fib_swing_high = struct_sw_h
            state.fib_high_is_live = False
            state.cont_consumed = False

        # ── Seguimiento en vivo del ancla: sigue el impulso mientras el
        # precio hace nuevos extremos, hasta que se consuma la Ruta B o
        # llegue un nuevo CHoCH. GAP REAL encontrado probando: sin esto,
        # la golden zone se quedaba fija en el primer maximo/minimo del
        # CHoCH aunque el precio siguiera corriendo mucho mas lejos --
        # zona equivocada, no solo en pruebas, tambien en produccion.
        if not is_bull_break and not is_bear_break and state.fib_direction != 0 and not state.cont_consumed:
            if state.fib_high_is_live and state.fib_swing_high is not None and last.high > state.fib_swing_high:
                state.fib_swing_high = last.high
            if state.fib_low_is_live and state.fib_swing_low is not None and last.low < state.fib_swing_low:
                state.fib_swing_low = last.low

        fib_ok = (state.fib_swing_high is not None and state.fib_swing_low is not None
                  and state.fib_swing_high > state.fib_swing_low and state.fib_direction != 0)

        if fib_ok and not state.cont_consumed:
            rng = state.fib_swing_high - state.fib_swing_low
            if state.fib_direction == 1:
                gz_top_raw = state.fib_swing_high - rng * cfg.GZ_LOW
                gz_bot_raw = state.fib_swing_high - rng * cfg.GZ_HIGH
                fib_target = state.fib_swing_high + rng * 0.618
            else:
                gz_top_raw = state.fib_swing_low + rng * cfg.GZ_HIGH
                gz_bot_raw = state.fib_swing_low + rng * cfg.GZ_LOW
                fib_target = state.fib_swing_low - rng * 0.618
            gz_top, gz_bot = max(gz_top_raw, gz_bot_raw), min(gz_top_raw, gz_bot_raw)

            in_gz = last.low <= gz_top and last.high >= gz_bot
            if in_gz and len(ltf) >= 2:
                confirm = True
                if cfg.CONT_CONFIRM == "ENGULF":
                    confirm = (is_bullish_engulf(ltf[-2], last) if state.fib_direction == 1
                               else is_bearish_engulf(ltf[-2], last))
                if confirm:
                    entry = last.close
                    if state.fib_direction == 1:
                        sl = state.fib_swing_low - cfg.SL_BUFFER_ATR * atr_val
                        r = entry - sl
                        if r > 0:
                            tgt = entry + cfg.RR_FIXED_FALLBACK * r
                            if fib_target > entry + r * 0.5:
                                tgt = fib_target
                            rr = (tgt - entry) / r
                            state.cont_consumed = True
                            _stats["confirmations"] += 1
                            signal_b = Signal(symbol, "LONG", entry, sl, entry + cfg.PARTIAL_TP_R * r, tgt, rr, kz or "off", "choch+goldenzone", path="CONT")
                    else:
                        sl = state.fib_swing_high + cfg.SL_BUFFER_ATR * atr_val
                        r = sl - entry
                        if r > 0:
                            tgt = entry - cfg.RR_FIXED_FALLBACK * r
                            if fib_target < entry - r * 0.5:
                                tgt = fib_target
                            rr = (entry - tgt) / r
                            state.cont_consumed = True
                            _stats["confirmations"] += 1
                            signal_b = Signal(symbol, "SHORT", entry, sl, entry - cfg.PARTIAL_TP_R * r, tgt, rr, kz or "off", "choch+goldenzone", path="CONT")

    # ── Combinar rutas: si disparan direcciones contrarias, se anulan las dos ──
    if signal_a and signal_b and signal_a.direction != signal_b.direction:
        signal_a = None
        signal_b = None
    signal = signal_a or signal_b

    if signal is None:
        return state, None

    # ── Filtros sobre la señal ya formada ──
    if signal.rr < cfg.MIN_RR:
        _stats["rejected_rr"] += 1
        return state, None

    if cfg.DIRECTION == "LONG" and signal.direction == "SHORT":
        _stats["rejected_direction"] += 1
        return state, None
    if cfg.DIRECTION == "SHORT" and signal.direction == "LONG":
        _stats["rejected_direction"] += 1
        return state, None

    if cfg.KZ_ONLY_ENTRY and not kz_ok:
        _stats["rejected_kz_only"] += 1
        return state, None

    if cfg.USE_HTF_BIAS and len(htf) >= cfg.HTF_EMA_LEN + 5:
        closes = [c.close for c in htf[-(cfg.HTF_EMA_LEN * 3):]]
        htf_ema = ema(closes, cfg.HTF_EMA_LEN)
        htf_close = htf[-1].close
        if signal.direction == "LONG" and htf_close <= htf_ema:
            _stats["rejected_htf"] += 1
            return state, None
        if signal.direction == "SHORT" and htf_close >= htf_ema:
            _stats["rejected_htf"] += 1
            return state, None

    if cfg.USE_PREMIUM_DISCOUNT:
        levels = [x for x in (rng_h, rng_l, pdh, pdl) if x is not None]
        if len(levels) >= 2:
            mid = (max(levels) + min(levels)) / 2
            if signal.direction == "LONG" and signal.entry >= mid:
                _stats["rejected_premium_discount"] += 1
                return state, None
            if signal.direction == "SHORT" and signal.entry <= mid:
                _stats["rejected_premium_discount"] += 1
                return state, None

    if cfg.USE_FUNDING_FILTER:
        if funding_rate is None:
            _stats["rejected_funding"] += 1
            return state, None  # sin dato -> no se arriesga, se descarta
        if signal.direction == "LONG" and funding_rate > -cfg.FUNDING_MIN_ABS:
            _stats["rejected_funding"] += 1
            return state, None
        if signal.direction == "SHORT" and funding_rate < cfg.FUNDING_MIN_ABS:
            _stats["rejected_funding"] += 1
            return state, None
    signal.funding_rate = funding_rate

    oi_change_pct = None
    if state.oi_at_setup is not None and current_oi is not None and state.oi_at_setup > 0:
        oi_change_pct = (current_oi - state.oi_at_setup) / state.oi_at_setup * 100.0
    signal.oi_change_pct = oi_change_pct
    signal.lead_confirmed = _lead_confirms(signal.direction, last.open_time)

    if cfg.USE_OI_FILTER:
        if oi_change_pct is None:
            _stats["rejected_oi"] += 1
            return state, None  # sin dato -> no se arriesga, se descarta
        if oi_change_pct > cfg.OI_MAX_INCREASE_PCT:
            _stats["rejected_oi"] += 1
            return state, None  # OI subiendo = posicion nueva contra la reversion, no flush

    _stats["signals"] += 1
    return state, signal
