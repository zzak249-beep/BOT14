"""
QF×JP Bot v6.0 — risk_manager.py
"""
import logging
log = logging.getLogger("RISK")


class RiskManager:
    def max_daily_loss_ok(self, start_bal: float, cur_bal: float, max_pct: float) -> bool:
        if start_bal <= 0:
            return True
        dd = (start_bal - cur_bal) / start_bal * 100
        if dd >= max_pct:
            log.warning(f"DD diario {dd:.2f}% >= límite {max_pct}%")
            return False
        return True

    def position_size(
        self, balance: float, price: float, sl: float,
        risk_pct: float, leverage: int
    ) -> float:
        if price <= 0 or sl is None or sl <= 0:
            return 0.0
        risk_usdt = balance * (risk_pct / 100)
        sl_dist   = abs(price - sl)
        if sl_dist < 1e-12:
            return 0.0
        # Tamaño en contratos (notional = size * price)
        size = (risk_usdt * leverage) / sl_dist
        # Asegurar mínimo notional de 5 USDT
        min_size = 5.0 / price
        if size < min_size:
            log.warning(f"Tamaño {size:.6f} < mínimo {min_size:.6f} — trade omitido")
            return 0.0
        return round(size, 4)
