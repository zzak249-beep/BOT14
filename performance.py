"""
QF×JP Bot v6.0 — performance.py
"""
import logging
from collections import deque, defaultdict
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger("PERF")


@dataclass
class TradeRecord:
    symbol:     str
    side:       str
    entry:      float
    exit:       float
    pnl_pct:    float
    conviction: int
    tier:       str


class PerformanceTracker:
    def __init__(self, window: int = 20, min_pf: float = 1.2):
        self._window   = window
        self._min_pf   = min_pf
        self._global:  deque = deque(maxlen=window * 5)
        self._by_sym:  dict  = defaultdict(lambda: deque(maxlen=window))
        self._suspended: set = set()

    def record(self, trade: TradeRecord):
        self._global.append(trade)
        self._by_sym[trade.symbol].append(trade)
        self._update_suspension(trade.symbol)

    def _profit_factor(self, trades) -> float:
        gross_win  = sum(t.pnl_pct for t in trades if t.pnl_pct > 0)
        gross_loss = sum(-t.pnl_pct for t in trades if t.pnl_pct < 0)
        return gross_win / gross_loss if gross_loss > 0 else 999.0

    def _update_suspension(self, symbol: str):
        trades = self._by_sym[symbol]
        if len(trades) < 5:
            return
        pf = self._profit_factor(trades)
        if pf < self._min_pf:
            self._suspended.add(symbol)
            log.warning(f"[{symbol}] suspendido: PF={pf:.2f} < {self._min_pf}")
        elif symbol in self._suspended:
            self._suspended.discard(symbol)
            log.info(f"[{symbol}] rehabilitado: PF={pf:.2f}")

    def is_tradeable(self, symbol: str) -> bool:
        return symbol not in self._suspended

    def global_stats(self) -> Optional[dict]:
        trades = list(self._global)
        if not trades:
            return None
        wins     = [t for t in trades if t.pnl_pct > 0]
        losses   = [t for t in trades if t.pnl_pct <= 0]
        pf       = self._profit_factor(trades)
        avg_pnl  = sum(t.pnl_pct for t in trades) / len(trades)
        wr       = len(wins) / len(trades)
        return {
            "total_trades":  len(trades),
            "win_rate":      wr,
            "profit_factor": pf,
            "avg_pnl":       avg_pnl,
            "suspended":     list(self._suspended),
        }
