"""core/risk.py — Risk engine: position sizing, drawdown guard, daily limits."""
from __future__ import annotations
import time
from datetime import datetime, timezone
from loguru import logger


class RiskManager:
    def __init__(self) -> None:
        self._balance:     float = 0.0
        self._peak:        float = 0.0
        self._day_start:   float = 0.0       # balance at start of today
        self._day_ts:      float = time.time()
        self._daily_pnl:   float = 0.0
        self._consec_loss: int   = 0
        self._cooldown_until: float = 0.0
        self._halted:      bool  = False
        self._halt_reason: str   = ""
        self._open_symbols: set[str] = set()
        self._win_count:   int   = 0
        self._loss_count:  int   = 0
        self._total_pnl:   float = 0.0

    # ── State ─────────────────────────────────────────────────────────────

    def set_balance(self, balance: float) -> None:
        from core.config import cfg
        if self._peak == 0:
            self._peak = balance
        if self._day_start == 0:
            self._day_start = balance

        self._balance = balance

        # Reset daily stats at new UTC day
        now = time.time()
        if now - self._day_ts > 86400:
            self._day_ts    = now
            self._day_start = balance
            self._daily_pnl = 0.0
            logger.info("Daily stats reset")

        # Check drawdown
        if balance > self._peak:
            self._peak = balance

        drawdown_pct = (self._peak - balance) / self._peak * 100 if self._peak > 0 else 0
        if drawdown_pct >= cfg.max_drawdown_pct and not self._halted:
            self._halt(f"Max drawdown {drawdown_pct:.1f}% reached (limit {cfg.max_drawdown_pct}%)")

    # ── Trade gates ───────────────────────────────────────────────────────

    def can_trade(self, balance: float) -> tuple[bool, str]:
        from core.config import cfg
        if self._halted:
            return False, f"Halted: {self._halt_reason}"
        if time.time() < self._cooldown_until:
            remaining = int(self._cooldown_until - time.time())
            return False, f"Cooldown ({remaining}s remaining)"
        if len(self._open_symbols) >= cfg.max_open_trades:
            return False, "Max open trades reached"

        # Daily loss limit (% of day-start balance)
        if self._day_start > 0:
            daily_loss_pct = -self._daily_pnl / self._day_start * 100
            if daily_loss_pct >= cfg.daily_loss_limit:
                return False, f"Daily loss limit {daily_loss_pct:.1f}% >= {cfg.daily_loss_limit}%"

        return True, "ok"

    def correlation_ok(self, symbol: str) -> bool:
        """Prevent holding multiple highly-correlated assets (basic: same base)."""
        base = symbol.split("-")[0]
        for s in self._open_symbols:
            if s.split("-")[0] == base and s != symbol:
                return False
        return True

    # ── Sizing ────────────────────────────────────────────────────────────

    def position_size(self, balance: float, n_open: int, confidence: float, atr_pct: float) -> float:
        """USDT size to allocate for this trade."""
        from core.config import cfg
        base = balance * cfg.risk_pct / 100

        # Scale up with confidence (50–100 → 0.7–1.3×)
        conf_scale = 0.7 + (confidence / 100) * 0.6

        # Reduce per open trade (each slot reduces by 30%)
        slot_scale = (0.7 ** n_open) if n_open > 0 else 1.0

        # Volatility guard: if ATR% is high, reduce size
        vol_scale = 1.0
        if atr_pct > 0:
            vol_scale = min(1.0, 1.5 / (atr_pct + 0.5))

        size = base * conf_scale * slot_scale * vol_scale
        return round(max(5.0, min(size, balance * 0.20)), 2)  # cap at 20% of balance

    def dynamic_sl_tp(
        self, price: float, side: str, atr: float
    ) -> tuple[float, float, float, float]:
        """Return (sl_price, tp_price, sl_pct, tp_pct)."""
        from core.config import cfg
        # ATR-based SL if ATR is available, else fixed %
        if atr > 0 and price > 0:
            atr_pct = atr / price * 100
            sl_pct  = max(cfg.sl_pct, min(atr_pct * 1.5, cfg.sl_pct * 2))
            tp_pct  = sl_pct * (cfg.tp_pct / cfg.sl_pct)   # keep risk/reward ratio
        else:
            sl_pct = cfg.sl_pct
            tp_pct = cfg.tp_pct

        if side == "BUY":
            sl = round(price * (1 - sl_pct / 100), 8)
            tp = round(price * (1 + tp_pct / 100), 8)
        else:
            sl = round(price * (1 + sl_pct / 100), 8)
            tp = round(price * (1 - tp_pct / 100), 8)

        return sl, tp, round(sl_pct, 2), round(tp_pct, 2)

    # ── Recording ─────────────────────────────────────────────────────────

    def record_open(self, symbol: str) -> None:
        self._open_symbols.add(symbol)

    def record_close(self, symbol: str, pnl: float, balance: float) -> None:
        from core.config import cfg
        self._open_symbols.discard(symbol)
        self._daily_pnl += pnl
        self._total_pnl += pnl
        self.set_balance(balance)

        if pnl >= 0:
            self._win_count   += 1
            self._consec_loss  = 0
        else:
            self._loss_count  += 1
            self._consec_loss += 1
            if self._consec_loss >= cfg.max_consecutive_losses:
                self._cooldown_until = time.time() + cfg.cooldown_after_loss
                logger.warning(
                    f"{cfg.max_consecutive_losses} consecutive losses — "
                    f"cooldown {cfg.cooldown_after_loss}s"
                )

    # ── Halt ─────────────────────────────────────────────────────────────

    def _halt(self, reason: str) -> None:
        self._halted      = True
        self._halt_reason = reason
        logger.critical(f"BOT HALTED: {reason}")

    @property
    def is_halted(self) -> bool:
        return self._halted

    # ── Summary ───────────────────────────────────────────────────────────

    def summary(self) -> dict:
        total_trades = self._win_count + self._loss_count
        win_rate = round(self._win_count / total_trades * 100, 1) if total_trades else 0.0
        return {
            "balance":       round(self._balance, 2),
            "peak":          round(self._peak, 2),
            "daily_pnl_usdt": round(self._daily_pnl, 2),
            "total_pnl":     round(self._total_pnl, 2),
            "win_rate":      win_rate,
            "wins":          self._win_count,
            "losses":        self._loss_count,
            "consec_losses": self._consec_loss,
            "open_count":    len(self._open_symbols),
            "halted":        self._halted,
            "halt_reason":   self._halt_reason,
            "cooldown":      max(0, int(self._cooldown_until - time.time())),
        }
