"""
Position Tracker — gestiona posiciones abiertas y simula paper trading
"""
import json
import logging
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional
from datetime import datetime

logger = logging.getLogger(__name__)
POS_FILE = Path("logs/positions.json")


@dataclass
class Position:
    symbol:        str
    direction:     str    # "LONG" | "SHORT"
    tier:          str
    entry:         float
    sl:            float
    tp:            float
    qty:           float
    open_time:     str
    order_id:      str
    paper:         bool
    trailing_sl:   float  # SL actualizado por trailing


class PositionTracker:
    def __init__(self):
        POS_FILE.parent.mkdir(exist_ok=True)
        self.positions: dict[str, Position] = self._load()

    def _load(self) -> dict:
        if POS_FILE.exists():
            try:
                d = json.loads(POS_FILE.read_text())
                return {k: Position(**v) for k, v in d.items()}
            except Exception:
                pass
        return {}

    def _save(self):
        d = {k: asdict(v) for k, v in self.positions.items()}
        POS_FILE.write_text(json.dumps(d, indent=2))

    def open(self, pos: Position):
        self.positions[pos.symbol] = pos
        self._save()
        logger.info(f"📂 Posición abierta: {pos.direction} {pos.symbol} "
                    f"entry={pos.entry} sl={pos.sl} tp={pos.tp} qty={pos.qty}")

    def close(self, symbol: str) -> Optional[Position]:
        pos = self.positions.pop(symbol, None)
        self._save()
        return pos

    def get(self, symbol: str) -> Optional[Position]:
        return self.positions.get(symbol)

    def has(self, symbol: str) -> bool:
        return symbol in self.positions

    def update_trailing_sl(self, symbol: str, new_sl: float):
        if symbol in self.positions:
            self.positions[symbol].trailing_sl = new_sl
            self._save()

    def check_exit(self, symbol: str, current_price: float) -> Optional[str]:
        """
        Comprueba si la posición debe cerrarse.
        Retorna el motivo o None si sigue abierta.
        """
        pos = self.positions.get(symbol)
        if not pos:
            return None

        sl = pos.trailing_sl  # usa trailing SL si está activo

        if pos.direction == "LONG":
            if current_price <= sl:
                return "stop_loss"
            if current_price >= pos.tp:
                return "take_profit"
        else:  # SHORT
            if current_price >= sl:
                return "stop_loss"
            if current_price <= pos.tp:
                return "take_profit"

        return None

    def calc_pnl(self, symbol: str, exit_price: float) -> float:
        pos = self.positions.get(symbol)
        if not pos:
            return 0.0
        if pos.direction == "LONG":
            return (exit_price - pos.entry) * pos.qty
        else:
            return (pos.entry - exit_price) * pos.qty

    def calc_trailing_sl(self, pos: Position, current_price: float,
                          atr: float, trail_atr_mult: float = 1.5) -> float:
        """
        Mueve el SL en la dirección favorable si el precio avanza.
        """
        if pos.direction == "LONG":
            proposed = current_price - atr * trail_atr_mult
            return max(proposed, pos.trailing_sl)
        else:
            proposed = current_price + atr * trail_atr_mult
            return min(proposed, pos.trailing_sl)

    def summary(self) -> str:
        if not self.positions:
            return "Sin posiciones abiertas."
        lines = []
        for sym, p in self.positions.items():
            lines.append(
                f"{'📋' if p.paper else '💵'} {p.direction} {sym} "
                f"entry={p.entry:.6f} sl={p.trailing_sl:.6f} tp={p.tp:.6f}"
            )
        return "\n".join(lines)
