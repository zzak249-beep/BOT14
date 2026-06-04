"""
trader/position_manager.py — Gestión de Posiciones con Partial TP
=================================================================
- Abre posición con SL + TP1 + TP0.5 (partial)
- Cierra 25% en TP0.5 → mueve SL a breakeven
- Gestión de estado en memoria
"""
from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


@dataclass
class OpenPosition:
    symbol:       str
    direction:    str     # "LONG" | "SHORT"
    level:        str     # "STD" | "FUEL" | "SUP"
    score:        int
    entry_price:  float
    quantity:     float
    sl_price:     float
    tp0_price:    float   # partial TP 25%
    tp1_price:    float
    tp2_price:    float
    atr:          float
    kelly_f:      float
    opened_at:    datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    partial_done: bool = False   # se tomó el partial TP
    sl_at_be:     bool = False   # SL movido a breakeven
    order_id:     str = ""

    @property
    def age_minutes(self) -> float:
        return (datetime.now(timezone.utc) - self.opened_at).total_seconds() / 60

    @property
    def close_side(self) -> str:
        return "SELL" if self.direction == "LONG" else "BUY"

    def pnl_pct(self, current_price: float) -> float:
        if self.direction == "LONG":
            return (current_price - self.entry_price) / self.entry_price * 100
        return (self.entry_price - current_price) / self.entry_price * 100


class PositionManager:
    """
    Gestiona el ciclo de vida de posiciones en BingX.

    Uso:
        pm = PositionManager(bx_client, cfg)
        ok = pm.open_position(signal, balance)
        pm.check_positions(current_prices)
    """

    def __init__(self, client, cfg):
        self.client   = client
        self.cfg      = cfg
        self._positions: dict[str, OpenPosition] = {}

    @property
    def open_count(self) -> int:
        return len(self._positions)

    def has_position(self, symbol: str) -> bool:
        # Verificar también en BingX real
        pos = self.client.get_position(symbol)
        if pos:
            return True
        self._positions.pop(symbol, None)
        return False

    # ─── ABRIR POSICIÓN ────────────────────────────────────────────────────────

    def open_position(self, signal, balance: float) -> bool:
        """
        Abre una posición con SL + TP0.5 (partial) + TP1.
        Returns True si se ejecutó.
        """
        from strategy.qfxjp_signal import SignalResult
        s: SignalResult = signal
        cfg = self.cfg

        # Verificaciones previas
        if self.open_count >= cfg.MAX_OPEN_TRADES:
            logger.info(f"[PM] {s.symbol}: MAX_OPEN_TRADES={cfg.MAX_OPEN_TRADES} alcanzado")
            return False

        if self.has_position(s.symbol):
            logger.info(f"[PM] {s.symbol}: ya hay posición abierta")
            return False

        # Configurar apalancamiento
        self.client.set_leverage(s.symbol, cfg.LEVERAGE)

        entry_side = "BUY" if s.direction == "LONG" else "SELL"
        close_side = "SELL" if s.direction == "LONG" else "BUY"

        # Orden de mercado principal
        order = self.client.market_order(s.symbol, entry_side, s.quantity)
        if not order:
            logger.error(f"[PM] {s.symbol}: fallo al abrir orden de mercado")
            return False

        order_id = str(order.get("orderId", ""))
        logger.info(f"[PM] {s.symbol} {s.direction} abierto @ {s.price:.4f} qty={s.quantity} [{s.level}]")

        # SL
        sl = self.client.stop_market(s.symbol, close_side, s.quantity, s.sl_price)
        if not sl:
            logger.warning(f"[PM] {s.symbol}: SL no colocado — cerrando posición por seguridad")
            self.client.close_position(s.symbol)
            return False

        # TP0.5 parcial (25%)
        partial_qty = round(s.quantity * cfg.PTP_PCT, 6) if cfg.PTP_ENABLED else 0

        if cfg.PTP_ENABLED and partial_qty > 0:
            self.client.tp_market(s.symbol, close_side, partial_qty, s.tp0_price)

        # TP1 completo
        remaining_qty = round(s.quantity - partial_qty, 6)
        if remaining_qty > 0:
            self.client.tp_market(s.symbol, close_side, remaining_qty, s.tp1_price)

        # Registrar
        self._positions[s.symbol] = OpenPosition(
            symbol      = s.symbol,
            direction   = s.direction,
            level       = s.level,
            score       = s.score,
            entry_price = s.price,
            quantity    = s.quantity,
            sl_price    = s.sl_price,
            tp0_price   = s.tp0_price,
            tp1_price   = s.tp1_price,
            tp2_price   = s.tp2_price,
            atr         = s.atr,
            kelly_f     = s.kelly_f,
            order_id    = order_id,
        )
        return True

    # ─── VERIFICAR POSICIONES ──────────────────────────────────────────────────

    def check_positions(self, current_prices: dict[str, float]) -> list[str]:
        """
        Verifica posiciones abiertas. Aplica lógica partial TP → breakeven.
        Returns lista de símbolos cerrados.
        """
        closed = []
        for symbol, pos in list(self._positions.items()):
            price = current_prices.get(symbol)
            if not price:
                price = self.client.get_last_price(symbol)

            # Verificar si BingX todavía tiene la posición
            bx_pos = self.client.get_position(symbol)
            if not bx_pos:
                logger.info(f"[PM] {symbol}: posición cerrada externamente")
                del self._positions[symbol]
                closed.append(symbol)
                continue

            if not price:
                continue

            pnl = pos.pnl_pct(price)

            # Mover SL a breakeven después del partial TP
            if self.cfg.PTP_ENABLED and not pos.sl_at_be:
                tp0_hit = (pos.direction == "LONG" and price >= pos.tp0_price) or \
                          (pos.direction == "SHORT" and price <= pos.tp0_price)
                if tp0_hit:
                    logger.info(f"[PM] {symbol}: TP0.5 alcanzado → moviendo SL a BE @ {pos.entry_price:.4f}")
                    self.client.cancel_all_orders(symbol)
                    remaining = float(bx_pos.get("positionAmt", pos.quantity))
                    remaining = abs(remaining) * (1 - self.cfg.PTP_PCT)
                    if remaining > 0:
                        self.client.stop_market(symbol, pos.close_side,
                                                round(remaining, 6), pos.entry_price)
                        self.client.tp_market(symbol, pos.close_side,
                                              round(remaining, 6), pos.tp1_price)
                    pos.sl_at_be    = True
                    pos.partial_done = True

            logger.debug(f"[PM] {symbol} {pos.direction} PnL={pnl:+.2f}%")

        return closed

    # ─── CERRAR MANUAL ────────────────────────────────────────────────────────

    def close_all(self, reason: str = "manual"):
        for symbol in list(self._positions.keys()):
            self.client.cancel_all_orders(symbol)
            self.client.close_position(symbol)
            logger.info(f"[PM] {symbol}: cerrado — {reason}")
            del self._positions[symbol]

    def get_positions_summary(self) -> list[dict]:
        summary = []
        for symbol, pos in self._positions.items():
            price = self.client.get_last_price(symbol) or pos.entry_price
            summary.append({
                "symbol":  symbol,
                "dir":     pos.direction,
                "level":   pos.level,
                "score":   pos.score,
                "entry":   pos.entry_price,
                "current": price,
                "pnl_pct": round(pos.pnl_pct(price), 2),
                "age_min": round(pos.age_minutes, 1),
                "partial": pos.partial_done,
                "sl_be":   pos.sl_at_be,
            })
        return summary
