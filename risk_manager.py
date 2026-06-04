"""
trader/risk_manager.py — Gestor de Riesgo
==========================================
- Límite de pérdida diaria
- Drawdown máximo
- Circuit breaker global
- Registro de trades del día
"""
from __future__ import annotations
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, date
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class DailyStats:
    date:         date
    start_balance: float
    current_balance: float = 0.0
    trades:       int = 0
    wins:         int = 0
    losses:       int = 0
    total_pnl:    float = 0.0
    circuit_open: bool = False

    @property
    def loss_pct(self) -> float:
        if self.start_balance <= 0: return 0
        return (self.start_balance - self.current_balance) / self.start_balance

    @property
    def win_rate(self) -> float:
        return self.wins / self.trades if self.trades else 0


class RiskManager:
    """
    Controla el riesgo global del bot.

    Uso:
        rm = RiskManager(cfg, bx_client)
        if rm.can_trade():
            pm.open_position(signal)
        rm.record_trade(pnl_pct)
    """

    def __init__(self, cfg, client):
        self.cfg    = cfg
        self.client = client
        self._stats: Optional[DailyStats] = None
        self._peak_balance: float = 0.0
        self._global_circuit: bool = False

    def _reset_daily(self, balance: float):
        today = date.today()
        if self._stats is None or self._stats.date != today:
            logger.info(f"[Risk] Nuevo día {today} | Balance: {balance:.2f} USDT")
            self._stats = DailyStats(
                date             = today,
                start_balance    = balance,
                current_balance  = balance,
            )
            if balance > self._peak_balance:
                self._peak_balance = balance

    def refresh(self) -> Optional[float]:
        """Actualiza el balance desde BingX."""
        bal = self.client.get_balance()
        if bal is None:
            return None
        self._reset_daily(bal)
        self._stats.current_balance = bal
        if bal > self._peak_balance:
            self._peak_balance = bal
        return bal

    # ─── VALIDACIONES ────────────────────────────────────────────────────────

    def can_trade(self) -> tuple[bool, str]:
        if self._global_circuit:
            return False, "Circuit breaker GLOBAL activo"

        if self._stats is None:
            bal = self.refresh()
            if bal is None:
                return False, "No se pudo obtener balance de BingX"

        stats = self._stats

        # Pérdida diaria
        if stats.loss_pct >= self.cfg.DAILY_LOSS_LIMIT:
            msg = f"Pérdida diaria {stats.loss_pct*100:.1f}% ≥ límite {self.cfg.DAILY_LOSS_LIMIT*100:.0f}%"
            logger.warning(f"[Risk] BLOQUEADO: {msg}")
            return False, msg

        # Drawdown máximo
        if self._peak_balance > 0:
            dd = (self._peak_balance - stats.current_balance) / self._peak_balance
            if dd >= self.cfg.MAX_DRAWDOWN:
                self._global_circuit = True
                msg = f"Drawdown máximo {dd*100:.1f}% — Bot pausado"
                logger.error(f"[Risk] CIRCUIT GLOBAL: {msg}")
                return False, msg

        return True, "OK"

    def record_trade(self, pnl_pct: float):
        if self._stats is None: return
        self._stats.trades += 1
        self._stats.total_pnl += pnl_pct
        if pnl_pct > 0:
            self._stats.wins += 1
        else:
            self._stats.losses += 1
        logger.info(f"[Risk] Trade registrado: PnL={pnl_pct:+.2f}% | "
                    f"Hoy: {self._stats.wins}W/{self._stats.losses}L "
                    f"PnL={self._stats.total_pnl:+.2f}%")

    def reset_global_circuit(self):
        self._global_circuit = False
        logger.info("[Risk] Circuit breaker global RESETEADO")

    def get_stats(self) -> dict:
        if self._stats is None:
            return {"status": "sin datos"}
        s = self._stats
        return {
            "date":      str(s.date),
            "balance":   round(s.current_balance, 2),
            "start_bal": round(s.start_balance, 2),
            "loss_pct":  round(s.loss_pct*100, 2),
            "trades":    s.trades,
            "wins":      s.wins,
            "losses":    s.losses,
            "win_rate":  round(s.win_rate*100, 1),
            "total_pnl": round(s.total_pnl, 2),
            "dd_pct":    round((self._peak_balance-s.current_balance)/self._peak_balance*100, 2) if self._peak_balance > 0 else 0,
            "circuit":   self._global_circuit,
        }
