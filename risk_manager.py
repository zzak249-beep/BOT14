"""risk_manager.py — daily loss limit + trade counter."""

import logging
from datetime import datetime, timezone

log = logging.getLogger("risk_mgr")


class RiskManager:
    def __init__(self, cfg):
        self.cfg             = cfg
        self._day_pnl        = 0.0
        self._day_trades     = 0
        self._day_start_eq   = None
        self._today          = None

    def _reset(self, equity: float):
        today = datetime.now(tz=timezone.utc).date()
        if today != self._today:
            self._today        = today
            self._day_pnl      = 0.0
            self._day_trades   = 0
            self._day_start_eq = equity
            log.info(f"New day — equity: {equity:.2f} USDT")

    def can_trade(self, equity: float) -> tuple:
        """Returns (allowed: bool, reason: str)."""
        self._reset(equity)
        if self._day_trades >= self.cfg.MAX_DAILY_TRADES:
            return False, f"Daily trades limit: {self._day_trades}"
        if self._day_start_eq and self._day_start_eq > 0:
            loss_pct = (-self._day_pnl / self._day_start_eq) * 100.0
            if loss_pct >= self.cfg.DAILY_LOSS_PCT:
                return False, f"Daily loss {loss_pct:.1f}% >= {self.cfg.DAILY_LOSS_PCT}%"
        return True, "ok"

    def record_trade(self, pnl_usdt: float):
        self._day_pnl    += pnl_usdt
        self._day_trades += 1
        log.info(f"Trade recorded pnl={pnl_usdt:+.2f} day_pnl={self._day_pnl:+.2f} trades={self._day_trades}")

    def increment_daily_trades(self):
        self._day_trades += 1

    @property
    def day_pnl(self) -> float:
        return self._day_pnl
