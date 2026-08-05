"""
Risk Manager v2 — SL/TP + sizing + validación R:R + precisión por contrato.
"""
from __future__ import annotations
import logging
from typing import Optional, Tuple
from strategy import Signal
from utils import floor_qty
import config as cfg

logger = logging.getLogger(__name__)


def calculate_sl_tp(
    signal:      Signal,
    entry:       float,
    contract:    dict,
) -> Tuple[float, float]:
    """
    Calcula SL y TP para una señal.
    Prioridad TP: nivel fib válido → ATR fallback.
    Prioridad SL: estructura (swing) → ATR fallback.
    """
    atr     = signal.atr
    is_long = signal.action == "BUY"

    # ── STOP LOSS ────────────────────────────────────────────
    if cfg.SL_METHOD == "STRUCTURE":
        if is_long:
            sl = (signal.sw_low1 - atr * 0.1
                  if signal.sw_low1 and signal.sw_low1 < entry
                  else entry - atr * cfg.SL_ATR_MULT)
        else:
            sl = (signal.sw_high1 + atr * 0.1
                  if signal.sw_high1 and signal.sw_high1 > entry
                  else entry + atr * cfg.SL_ATR_MULT)
    else:
        sl = (entry - atr * cfg.SL_ATR_MULT if is_long
              else entry + atr * cfg.SL_ATR_MULT)

    # Seguridad dirección
    if is_long  and sl >= entry: sl = entry - atr * cfg.SL_ATR_MULT
    if not is_long and sl <= entry: sl = entry + atr * cfg.SL_ATR_MULT

    # ── TAKE PROFIT ──────────────────────────────────────────
    raw_tp: Optional[float] = None
    if cfg.TP_METHOD == "FIB_TARGET":
        raw_tp = signal.fib_target
    elif cfg.TP_METHOD == "FIB_HALF":
        raw_tp = signal.fib_tgt50

    if raw_tp and is_long  and raw_tp > entry + atr * 0.5:
        tp = raw_tp
    elif raw_tp and not is_long and raw_tp < entry - atr * 0.5:
        tp = raw_tp
    else:
        tp = (entry + atr * cfg.TP_ATR_MULT if is_long
              else entry - atr * cfg.TP_ATR_MULT)

    # Seguridad dirección
    if is_long  and tp <= entry: tp = entry + atr * cfg.TP_ATR_MULT
    if not is_long and tp >= entry: tp = entry - atr * cfg.TP_ATR_MULT

    return round(sl, 8), round(tp, 8)


def rr_ratio(entry: float, sl: float, tp: float) -> float:
    risk   = abs(entry - sl)
    reward = abs(entry - tp)
    return round(reward / risk, 2) if risk > 0 else 0.0


def is_rr_valid(entry: float, sl: float, tp: float) -> bool:
    return rr_ratio(entry, sl, tp) >= cfg.MIN_RR


def calculate_quantity(
    balance:  float,
    entry:    float,
    sl:       float,
    contract: dict,
) -> float:
    """
    Sizing por riesgo fijo:
        qty = (balance × RISK_PCT% × LEVERAGE) / |entry − SL|

    Aplica:
      - Máximo posición = balance × MAX_POSITION_PCT%
      - Mínimo del contrato (tradeMinQuantity)
      - Mínimo notional (tradeMinUSDT)
      - Redondeo al step size del contrato
    """
    sl_dist = abs(entry - sl)
    if sl_dist <= 0 or entry <= 0:
        return 0.0

    # Qty por riesgo
    risk_usdt = balance * cfg.RISK_PCT / 100
    qty       = (risk_usdt * cfg.LEVERAGE) / sl_dist

    # Cap por posición máxima
    max_usdt = balance * cfg.MAX_POSITION_PCT / 100
    max_qty  = (max_usdt * cfg.LEVERAGE) / entry
    qty      = min(qty, max_qty)

    # Precisión del contrato
    step = float(contract.get("tradeMinQuantity", 0.001))
    qty  = floor_qty(qty, step)

    # Validaciones mínimas
    min_qty      = float(contract.get("tradeMinQuantity", 0))
    min_notional = float(contract.get("tradeMinUSDT", 5))

    if min_qty > 0 and qty < min_qty:
        logger.debug(f"qty {qty:.6f} < min {min_qty:.6f}")
        return 0.0
    if qty * entry < min_notional:
        logger.debug(f"notional {qty*entry:.2f} < min {min_notional:.2f}")
        return 0.0

    return qty


def breakeven_price(entry: float, sl: float) -> float:
    """Precio de SL de breakeven (entrada más pequeño buffer)."""
    buf = abs(entry - sl) * 0.05  # 5% del risk como buffer
    return entry + buf if entry > sl else entry - buf
