"""
risk_manager.py — Tamaño de posición y cálculo de SL/TP.

Replica la lógica de riesgo del script Pine:

    entry_qty = strategy.equity * (qty_pct / 100) / close
    sl/tp     = por ATR (atr_mult_sl/tp) o por porcentaje fijo
"""

import math
from dataclasses import dataclass


@dataclass
class SizingResult:
    quantity: float
    notional: float
    ok: bool
    reason: str = ""


def round_step(value: float, precision: int) -> float:
    """Redondea hacia abajo (floor) al número de decimales del contrato,
    para nunca mandar una cantidad ligeramente por encima de lo que
    BingX acepta."""
    factor = 10 ** precision
    return math.floor(value * factor) / factor


def compute_position_size(equity: float, qty_pct: float, price: float,
                           quantity_precision: int, trade_min_quantity: float,
                           trade_min_usdt: float) -> SizingResult:
    if price <= 0 or equity <= 0:
        return SizingResult(0.0, 0.0, False, "precio o equity inválidos")

    raw_qty = (equity * (qty_pct / 100.0)) / price
    qty = round_step(raw_qty, quantity_precision)
    notional = qty * price

    if qty <= 0:
        return SizingResult(0.0, 0.0, False, "cantidad redondeada a 0")
    if trade_min_quantity and qty < trade_min_quantity:
        return SizingResult(qty, notional, False, f"por debajo de tradeMinQuantity ({trade_min_quantity})")
    if trade_min_usdt and notional < trade_min_usdt:
        return SizingResult(qty, notional, False, f"nocional por debajo de tradeMinUSDT ({trade_min_usdt})")

    return SizingResult(qty, notional, True)


def compute_sl_tp(entry_price: float, is_long: bool, atr_value: float | None, params) -> tuple[float, float]:
    """Devuelve (stop_loss_price, take_profit_price)."""
    if params.USE_ATR_SL and atr_value and atr_value > 0:
        sl_dist = atr_value * params.ATR_MULT_SL
        tp_dist = atr_value * params.ATR_MULT_TP
        if is_long:
            return entry_price - sl_dist, entry_price + tp_dist
        return entry_price + sl_dist, entry_price - tp_dist

    if is_long:
        return entry_price * (1 - params.SL_PERCENT), entry_price * (1 + params.TP_PERCENT)
    return entry_price * (1 + params.SL_PERCENT), entry_price * (1 - params.TP_PERCENT)
