"""
Risk Manager — sizing por riesgo fijo, circuit breaker diario,
límite de riesgo concurrente total.
"""
import datetime
import logging

log = logging.getLogger("risk_manager")


class RiskManager:
    def __init__(self, config):
        self.config = config
        self.daily_pnl = 0.0
        self.daily_start_balance = None
        self.current_day = datetime.date.today()
        self.open_risk_pct = 0.0  # suma del % de riesgo de posiciones abiertas

    def _reset_if_new_day(self, balance):
        today = datetime.date.today()
        if today != self.current_day:
            log.info("Nuevo día de trading — reseteando PnL diario")
            self.current_day = today
            self.daily_pnl = 0.0
            self.daily_start_balance = balance

    def register_realized_pnl(self, pnl_usdt, balance):
        self._reset_if_new_day(balance)
        self.daily_pnl += pnl_usdt

    def daily_loss_breached(self, balance):
        self._reset_if_new_day(balance)
        if self.daily_start_balance is None:
            self.daily_start_balance = balance
            return False
        if self.daily_start_balance <= 0:
            return False
        loss_pct = -self.daily_pnl / self.daily_start_balance * 100
        breached = loss_pct >= self.config.DAILY_MAX_LOSS_PCT
        if breached:
            log.warning(
                "Circuit breaker diario activado: pérdida %.2f%% >= límite %.2f%%",
                loss_pct, self.config.DAILY_MAX_LOSS_PCT,
            )
        return breached

    def calc_position_size(self, balance, entry_price, sl_price):
        """
        Tamaño de posición (en unidades del activo) según riesgo fijo % del balance.
        """
        risk_usdt = balance * (self.config.RISK_PCT_PER_TRADE / 100)
        risk_per_unit = abs(entry_price - sl_price)
        if risk_per_unit <= 0:
            return 0.0
        qty = risk_usdt / risk_per_unit
        return qty

    def can_open_new_position(self, balance, open_positions_count, new_risk_pct):
        if self.daily_loss_breached(balance):
            return False, "daily_loss_breached"
        if open_positions_count >= self.config.MAX_ACTIVE_POSITIONS:
            return False, "max_active_positions_reached"
        if self.open_risk_pct + new_risk_pct > self.config.MAX_CONCURRENT_RISK_PCT:
            return False, "max_concurrent_risk_reached"
        return True, "ok"

    def register_open_risk(self, risk_pct):
        self.open_risk_pct += risk_pct

    def release_open_risk(self, risk_pct):
        self.open_risk_pct = max(0.0, self.open_risk_pct - risk_pct)
