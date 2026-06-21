"""
QF×JP Bot — Trend Magic + RMI Sniper (filtro de confirmación)
═══════════════════════════════════════════════════════════════════════════
Portado del indicador Pine "Trend Magic + EMA + MA Smoothing + RMI Trend
Sniper". Solo se porta la parte que genera señal TRADEABLE — el resto del
indicador original (Band/RWMA, smoothing MA, Bollinger) es puramente
visual y no participa en absoluto en los BUY/SELL del script original
(esos los disparan únicamente las transiciones de `positive`/`negative`,
que dependen solo de p_mom/n_mom — verificado leyendo el código fuente,
no asumido). No se porta lo que no se entiende del todo.

DOS PIEZAS:

  1. Trend Magic — un CCI(20) decide el régimen (alcista si CCI≥0,
     bajista si CCI≤0). Una línea "trinquete" (x) sigue el extremo
     reciente (high-ATR o low+ATR según el régimen) pero SOLO puede
     moverse a favor de la tendencia actual, nunca en contra — el mismo
     principio que ya usa _update_trail() en position_manager.py, solo
     que aquí el disparador de "resetear" es un cruce de CCI por cero en
     vez de un nuevo peak. Complementario a tu slope_filter (pendiente
     de regresión multi-timeframe) — esto capta momentum relativo a su
     propia media reciente, no dirección de tendencia en sí.

  2. RMI Sniper — combina RSI (momentum de precio) y MFI (momentum
     confirmado por volumen) en un solo oscilador (rsi_mfi = promedio de
     ambos). Dispara momentum alcista cuando rsi_mfi cruza por encima de
     pmom (66) viniendo de abajo Y una EMA(5) corta tiene pendiente
     positiva; momentum bajista cuando rsi_mfi cae por debajo de nmom
     (30) con la EMA(5) cayendo. El estado (positive/negative) es
     "pegajoso" — se mantiene hasta que la condición contraria dispara,
     igual que en el Pine original.

Como filtro de confirmación: solo actúa si RMI Sniper tiene momentum en
la dirección de la señal Y Trend Magic está en el mismo régimen. Si RMI
Sniper no tiene momentum en esa dirección, neutral (no penaliza). Si SÍ
tiene momentum pero Trend Magic está en el régimen CONTRARIO, veta.

NOTA: aproximaciones honestas en el port — _rma() usa un warmup de media
móvil simple en vez de la inicialización exacta de Wilder (converge igual
tras suficientes barras, difiere un poco en las primeras). _sma() del ATR
usa SMA simple sobre True Range, igual que el original (ta.sma(ta.tr,...)),
no el ATR suavizado de Wilder — así está en el Pine fuente, no es un error
de este port.

Reutiliza k3m (TIMEFRAME principal, ya fetcheado) — sin llamada extra a
la API, igual que price_action_framework.py.
═══════════════════════════════════════════════════════════════════════════
"""
import logging

log = logging.getLogger("trend_magic_rmi")


# ── Helpers genéricos ────────────────────────────────────────────────────────

def _sma(values: list[float], length: int) -> list[float]:
    out = []
    for i in range(len(values)):
        if i < length - 1:
            out.append(sum(values[:i + 1]) / (i + 1))
        else:
            out.append(sum(values[i - length + 1:i + 1]) / length)
    return out


def _ema(values: list[float], length: int) -> list[float]:
    if not values:
        return []
    k = 2.0 / (length + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(out[-1] + k * (v - out[-1]))
    return out


def _rma(values: list[float], length: int) -> list[float]:
    """
    Aproximación de ta.rma() de Pine (suavizado de Wilder). Warmup con
    media móvil simple de los primeros `length` valores en vez de la
    inicialización exacta — converge al mismo resultado tras suficientes
    barras, difiere ligeramente al principio. Suficiente para un filtro
    de confirmación, no para algo que requiera precisión de céntimo.
    """
    n = len(values)
    out = [0.0] * n
    if n == 0:
        return out
    alpha = 1.0 / length
    for i in range(n):
        if i < length:
            out[i] = sum(values[:i + 1]) / (i + 1)
        else:
            out[i] = out[i - 1] + alpha * (values[i] - out[i - 1])
    return out


def _true_range(klines: list) -> list[float]:
    tr = [klines[0][2] - klines[0][3]]
    for i in range(1, len(klines)):
        h, l, pc = klines[i][2], klines[i][3], klines[i - 1][4]
        tr.append(max(h - l, abs(h - pc), abs(l - pc)))
    return tr


def _cci(klines: list, length: int) -> list[float]:
    tps = [(c[2] + c[3] + c[4]) / 3.0 for c in klines]
    out = []
    for i in range(len(tps)):
        if i < length - 1:
            out.append(0.0)
            continue
        window = tps[i - length + 1:i + 1]
        sma = sum(window) / length
        mean_dev = sum(abs(v - sma) for v in window) / length
        out.append((tps[i] - sma) / (0.015 * mean_dev) if mean_dev > 1e-12 else 0.0)
    return out


def _wilder_rsi(closes: list[float], length: int) -> list[float]:
    n = len(closes)
    gains  = [0.0] * n
    losses = [0.0] * n
    for i in range(1, n):
        change = closes[i] - closes[i - 1]
        gains[i]  = max(change, 0.0)
        losses[i] = max(-change, 0.0)
    up_rma   = _rma(gains, length)
    down_rma = _rma(losses, length)
    rsi = []
    for i in range(n):
        if down_rma[i] == 0:
            rsi.append(100.0)
        elif up_rma[i] == 0:
            rsi.append(0.0)
        else:
            rsi.append(100.0 - 100.0 / (1.0 + up_rma[i] / down_rma[i]))
    return rsi


def _mfi(klines: list, length: int) -> list[float]:
    n = len(klines)
    tp = [(c[2] + c[3] + c[4]) / 3.0 for c in klines]
    raw_mf = [tp[i] * klines[i][5] for i in range(n)]
    pos_mf = [0.0] * n
    neg_mf = [0.0] * n
    for i in range(1, n):
        if tp[i] > tp[i - 1]:
            pos_mf[i] = raw_mf[i]
        elif tp[i] < tp[i - 1]:
            neg_mf[i] = raw_mf[i]
    mfi = [50.0] * n
    for i in range(length, n):
        pos_sum = sum(pos_mf[i - length + 1:i + 1])
        neg_sum = sum(neg_mf[i - length + 1:i + 1])
        if neg_sum == 0:
            mfi[i] = 100.0
        elif pos_sum == 0:
            mfi[i] = 0.0
        else:
            mfi[i] = 100.0 - 100.0 / (1.0 + pos_sum / neg_sum)
    return mfi


# ── Trend Magic ──────────────────────────────────────────────────────────────

def compute_trend_magic(klines: list, cci_len: int = 20, atr_len: int = 5,
                         mult: float = 1.0) -> tuple[list[float], list[int], list[float]]:
    """
    Port fiel del Trend Magic original: CCI decide el régimen, una línea
    trinquete (x) sigue bufferUp/bufferDn pero solo se mueve a favor del
    régimen actual. Retorna (x, swap, cci) — usar [-1] para el valor actual.
    """
    n = len(klines)
    cci = _cci(klines, cci_len)
    tr  = _true_range(klines)
    atr_sma = _sma(tr, atr_len)
    highs = [c[2] for c in klines]
    lows  = [c[3] for c in klines]

    buffer_up = [0.0] * n
    buffer_dn = [0.0] * n
    x    = [0.0] * n
    swap = [1] * n

    for i in range(n):
        buffer_dn[i] = highs[i] + mult * atr_sma[i]
        buffer_up[i] = lows[i]  - mult * atr_sma[i]

        if i == 0:
            x[i] = buffer_up[i] if cci[i] >= 0 else buffer_dn[i]
            continue

        last_cci, this_cci = cci[i - 1], cci[i]

        if this_cci >= 0 and last_cci < 0:
            buffer_up[i] = buffer_dn[i - 1]
        if this_cci <= 0 and last_cci > 0:
            buffer_dn[i] = buffer_up[i - 1]

        if this_cci >= 0:
            if buffer_up[i] < buffer_up[i - 1]:
                buffer_up[i] = buffer_up[i - 1]
        elif this_cci <= 0:
            if buffer_dn[i] > buffer_dn[i - 1]:
                buffer_dn[i] = buffer_dn[i - 1]

        if this_cci >= 0:
            x[i] = buffer_up[i]
        elif this_cci <= 0:
            x[i] = buffer_dn[i]
        else:
            x[i] = x[i - 1]

        swap[i] = 1 if x[i] > x[i - 1] else (-1 if x[i] < x[i - 1] else swap[i - 1])

    return x, swap, cci


# ── RMI Sniper ───────────────────────────────────────────────────────────────

def compute_rmi_sniper(
    klines: list, rmi_len: int = 14, pmom: float = 66.0, nmom: float = 30.0,
) -> tuple[list[bool], list[bool], list[float]]:
    """
    rsi_mfi = promedio(RSI, MFI). Momentum alcista (positive) dispara al
    cruzar pmom hacia arriba con EMA(5) subiendo; bajista (negative) al
    caer bajo nmom con EMA(5) bajando. Estado pegajoso — se mantiene
    hasta que dispare la condición contraria. Retorna (positive, negative,
    rsi_mfi) — usar [-1] para el estado actual.
    """
    closes = [c[4] for c in klines]
    rsi = _wilder_rsi(closes, rmi_len)
    mfi = _mfi(klines, rmi_len)
    rsi_mfi = [(rsi[i] + mfi[i]) / 2.0 for i in range(len(klines))]
    ema5 = _ema(closes, 5)

    n = len(klines)
    positive = [False] * n
    negative = [False] * n

    for i in range(1, n):
        p_mom = (rsi_mfi[i - 1] < pmom and rsi_mfi[i] > pmom and
                  rsi_mfi[i] > nmom and (ema5[i] - ema5[i - 1]) > 0)
        n_mom = (rsi_mfi[i] < nmom and (ema5[i] - ema5[i - 1]) < 0)

        positive[i] = positive[i - 1]
        negative[i] = negative[i - 1]
        if p_mom:
            positive[i], negative[i] = True, False
        if n_mom:
            positive[i], negative[i] = False, True

    return positive, negative, rsi_mfi


# ── Filtro de confirmación para scanner.py ──────────────────────────────────

def trend_magic_rmi_filter(
    klines: list,
    direction: str,
    cci_len: int = 20, atr_len: int = 5, atr_mult: float = 1.0,
    rmi_len: int = 14, pmom: float = 66.0, nmom: float = 30.0,
    boost_amount: float = 7.0,
) -> tuple[float, str, bool]:
    """
    Mismo contrato que los demás filtros (boost_pts, reason, block).

    - RMI Sniper sin momentum en la dirección de la señal → neutral
      (boost=0, no bloquea) — no penaliza por no confirmar.
    - RMI Sniper SÍ tiene momentum a favor, pero Trend Magic está en el
      régimen CONTRARIO (CCI del signo opuesto) → veto. No luchar contra
      el régimen dominante, mismo principio que slope_block.
    - Ambos a favor → boost fijo.
    """
    min_bars = max(cci_len, atr_len, rmi_len) + 5
    if len(klines) < min_bars:
        return 0.0, "trend_magic_rmi_insufficient_data", False

    _, _, cci = compute_trend_magic(klines, cci_len, atr_len, atr_mult)
    positive, negative, rsi_mfi = compute_rmi_sniper(klines, rmi_len, pmom, nmom)

    trend_bullish = cci[-1] >= 0
    rmi_bullish   = positive[-1]
    rmi_bearish   = negative[-1]

    if direction == "LONG":
        if not rmi_bullish:
            return 0.0, f"rmi_sniper_sin_momentum_alcista(rsi_mfi={rsi_mfi[-1]:.1f})", False
        if not trend_bullish:
            return 0.0, f"trend_magic_contradice(cci={cci[-1]:.1f}<0 vs LONG)", True
        return (boost_amount,
                f"trend_magic_rmi_confirma(LONG) cci={cci[-1]:.1f} rsi_mfi={rsi_mfi[-1]:.1f}",
                False)
    else:
        if not rmi_bearish:
            return 0.0, f"rmi_sniper_sin_momentum_bajista(rsi_mfi={rsi_mfi[-1]:.1f})", False
        if trend_bullish:
            return 0.0, f"trend_magic_contradice(cci={cci[-1]:.1f}>0 vs SHORT)", True
        return (boost_amount,
                f"trend_magic_rmi_confirma(SHORT) cci={cci[-1]:.1f} rsi_mfi={rsi_mfi[-1]:.1f}",
                False)
