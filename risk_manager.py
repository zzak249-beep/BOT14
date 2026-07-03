"""
risk_manager.py — daily loss limit + trade counter.

FIX: _day_pnl / _day_trades / _day_start_eq / _today ahora persisten
vía state.py. Antes vivían solo en RAM — cada redeploy los reseteaba
a cero, permitiendo saltarse MAX_DAILY_TRADES y DAILY_LOSS_PCT de
facto si había varios redeploys en el mismo día (como ha pasado hoy
mismo, varias veces). Mismo patrón de bug ya corregido en el
risk_manager.py de daring-spontaneity.
"""
import logging
from datetime import datetime, timezone, date

import state

log = logging.getLogger("risk_mgr")


class RiskManager:
    def __init__(self, cfg):
        self.cfg = cfg
        saved_pnl, saved_trades, saved_eq, saved_day = state.get_day_state()
        self._day_pnl      = saved_pnl if saved_pnl is not None else 0.0
        self._day_trades   = saved_trades if saved_trades is not None else 0
        self._day_start_eq = saved_eq
        self._today        = date.fromisoformat(saved_day) if saved_day else None
        if saved_day:
            log.info(f"RiskManager: día restaurado desde state.py — "
                     f"day_pnl={self._day_pnl:+.2f}  trades={self._day_trades}  "
                     f"day_start_eq={self._day_start_eq}")

    def _reset(self, equity: float):
        today = datetime.now(tz=timezone.utc).date()
        if today != self._today:
            self._today        = today
            self._day_pnl      = 0.0
            self._day_trades   = 0
            self._day_start_eq = equity
            state.save_day_state(self._day_pnl, self._day_trades,
                                 self._day_start_eq, self._today.isoformat())
            log.info(f"New day — equity: {equity:.2f} USDT")

    def _today_str(self) -> str:
        # FIX: fallback defensivo — si record_trade()/increment_daily_trades()
        # se llama antes de que _reset() haya corrido ni una vez en este
        # proceso (p.ej. _manage_open_positions cierra algo en el primer
        # tick, antes del primer can_trade()), _today puede seguir en None.
        if self._today:
            return self._today.isoformat()
        return datetime.now(tz=timezone.utc).date().isoformat()

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
        state.save_day_state(self._day_pnl, self._day_trades,
                             self._day_start_eq, self._today_str())
        log.info(f"Trade recorded pnl={pnl_usdt:+.2f} day_pnl={self._day_pnl:+.2f} trades={self._day_trades}")

    def increment_daily_trades(self):
        self._day_trades += 1
        state.save_day_state(self._day_pnl, self._day_trades,
                             self._day_start_eq, self._today_str())

    # ── Missing methods called by position_monitor ────────────────────────────
    def update_open_count(self, n: int):
        """Track number of open positions (called by position_monitor)."""
        self._open_count = n

    def on_trade_opened(self, symbol: str = "", side: str = "", **kwargs):
        """Hook called when a trade is opened."""
        pass

    def on_trade_closed(self, symbol: str = "", side: str = "", pnl: float = 0.0, **kwargs):
        """Hook called when a trade is closed."""
        if pnl != 0.0:
            self.record_trade(pnl)

    def new_day(self, equity: float):
        """Force new day reset."""
        self._reset(equity)

    def can_open(self, equity: float) -> tuple:
        """Alias for can_trade used by some position monitors."""
        return self.can_trade(equity)

    @property
    def open_count(self) -> int:
        return getattr(self, "_open_count", 0)

    @property
    def day_pnl(self) -> float:
        return self._day_pnl
