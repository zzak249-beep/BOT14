"""
Gestión de Riesgo — QF Bot
Controla position sizing, drawdown máximo, pérdida diaria y circuit breakers.
PAPER MODE activo por defecto hasta confirmación explícita.
"""
import json
import logging
from dataclasses import dataclass, asdict
from datetime import date, datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

STATE_FILE = Path("logs/risk_state.json")


@dataclass
class RiskState:
    # Equity tracking
    initial_equity:   float = 1000.0
    peak_equity:      float = 1000.0
    current_equity:   float = 1000.0

    # Daily tracking (reset cada día)
    daily_start:      float = 1000.0
    daily_pnl:        float = 0.0
    daily_trades:     int   = 0
    last_reset_date:  str   = ""

    # Sesión
    consecutive_losses: int  = 0
    circuit_open:       bool = False   # True = bot detenido por pérdida
    circuit_reason:     str  = ""


class RiskManager:
    def __init__(self, cfg: dict):
        self.c = cfg
        self.state = self._load_state()

    # ── Persistencia ─────────────────────────────────────────
    def _load_state(self) -> RiskState:
        STATE_FILE.parent.mkdir(exist_ok=True)
        if STATE_FILE.exists():
            try:
                d = json.loads(STATE_FILE.read_text())
                return RiskState(**d)
            except Exception:
                pass
        s = RiskState(
            initial_equity=self.c['initial_equity'],
            peak_equity=self.c['initial_equity'],
            current_equity=self.c['initial_equity'],
            daily_start=self.c['initial_equity'],
        )
        return s

    def _save(self):
        STATE_FILE.write_text(json.dumps(asdict(self.state), indent=2))

    # ── Reset diario ─────────────────────────────────────────
    def _check_daily_reset(self):
        today = date.today().isoformat()
        if self.state.last_reset_date != today:
            logger.info("📅 Reset diario de estadísticas")
            self.state.daily_start  = self.state.current_equity
            self.state.daily_pnl    = 0.0
            self.state.daily_trades = 0
            self.state.last_reset_date = today
            # Circuit breaker diario se resetea al inicio del día
            if self.state.circuit_open and "diario" in self.state.circuit_reason:
                self.state.circuit_open   = False
                self.state.circuit_reason = ""
                logger.info("✅ Circuit breaker diario reseteado")
            self._save()

    # ── Circuit Breaker ───────────────────────────────────────
    def check_circuit(self) -> tuple[bool, str]:
        """
        Retorna (puede_operar, razon)
        """
        self._check_daily_reset()

        if self.state.circuit_open:
            return False, self.state.circuit_reason

        c = self.c
        s = self.state

        # 1. Pérdida diaria máxima
        daily_loss_pct = (s.daily_start - (s.daily_start + s.daily_pnl)) / s.daily_start * 100
        if s.daily_pnl < 0 and abs(s.daily_pnl) / s.daily_start * 100 >= c['max_daily_loss_pct']:
            reason = f"⛔ Pérdida diaria {abs(s.daily_pnl):.2f} USDT ({daily_loss_pct:.1f}%)"
            self._trigger_circuit(reason + " — reset mañana (diario)")
            return False, reason

        # 2. Drawdown máximo desde pico
        dd = (s.peak_equity - s.current_equity) / s.peak_equity * 100
        if dd >= c['max_drawdown_pct']:
            reason = f"⛔ Drawdown máximo {dd:.1f}% — bot detenido"
            self._trigger_circuit(reason)
            return False, reason

        # 3. Pérdidas consecutivas
        if s.consecutive_losses >= c['max_consecutive_losses']:
            reason = f"⛔ {s.consecutive_losses} pérdidas consecutivas — pausa forzada"
            self._trigger_circuit(reason + " (diario)")
            return False, reason

        # 4. Máximo de operaciones diarias
        if s.daily_trades >= c['max_daily_trades']:
            return False, f"⏸ Límite diario de {c['max_daily_trades']} operaciones alcanzado"

        return True, ""

    def _trigger_circuit(self, reason: str):
        self.state.circuit_open   = True
        self.state.circuit_reason = reason
        self._save()
        logger.warning(f"🔴 CIRCUIT BREAKER: {reason}")

    def reset_circuit_manual(self):
        """Llamado manualmente vía Telegram /reset"""
        self.state.circuit_open   = False
        self.state.circuit_reason = ""
        self.state.consecutive_losses = 0
        self._save()
        logger.info("✅ Circuit breaker reseteado manualmente")

    # ── Position Sizing ───────────────────────────────────────
    def calc_position_size(
        self,
        entry: float,
        sl: float,
        tier: str,
        conviction: int,
    ) -> Optional[float]:
        """
        Retorna el tamaño en contratos (base asset) o None si no es operable.
        Usa el método de riesgo fijo por operación (% del equity).
        """
        can, reason = self.check_circuit()
        if not can:
            logger.warning(f"No se abre posición: {reason}")
            return None

        c  = self.c
        eq = self.state.current_equity

        # Risk % según tier
        risk_pct_map = {
            "SUPREMA": c['risk_pct_suprema'],
            "FUEL":    c['risk_pct_fuel'],
            "STD":     c['risk_pct_std'],
        }
        base_risk = risk_pct_map.get(tier, c['risk_pct_std'])

        # Escalar levemente por convicción (0-10 → 0.8x - 1.2x)
        conv_scale = 0.8 + 0.04 * conviction   # 10 → 1.2, 5 → 1.0, 0 → 0.8
        risk_pct   = base_risk * conv_scale
        risk_pct   = min(risk_pct, c['risk_pct_max'])  # techo absoluto

        risk_usdt  = eq * risk_pct / 100.0
        sl_dist    = abs(entry - sl)

        if sl_dist <= 0:
            logger.warning("SL distance = 0, skipping")
            return None

        # Tamaño sin apalancamiento: qty_usdt = risk_usdt / (sl_dist / entry)
        qty_usdt = risk_usdt / (sl_dist / entry)
        qty_base = qty_usdt / entry

        # Aplicar apalancamiento (el exchange multiplica el margen)
        leverage  = c['leverage']
        margin_required = qty_usdt / leverage

        if margin_required > eq * 0.5:
            # No usar más del 50% del equity como margen en una operación
            qty_usdt  = eq * 0.5 * leverage
            qty_base  = qty_usdt / entry

        logger.info(
            f"Position sizing: equity={eq:.2f} risk={risk_pct:.2f}% "
            f"risk_usdt={risk_usdt:.2f} sl_dist={sl_dist:.6f} "
            f"qty_base={qty_base:.6f} margin={margin_required:.2f}"
        )
        return round(qty_base, 6)

    # ── TP Calculation ────────────────────────────────────────
    def calc_tp(self, entry: float, sl: float, direction: str, tier: str) -> float:
        """R:R según tier"""
        rr_map = {
            "SUPREMA": self.c['rr_suprema'],
            "FUEL":    self.c['rr_fuel'],
            "STD":     self.c['rr_std'],
        }
        rr = rr_map.get(tier, self.c['rr_std'])
        dist = abs(entry - sl) * rr
        return (entry + dist) if direction == "LONG" else (entry - dist)

    # ── PnL Tracking ─────────────────────────────────────────
    def record_trade(self, pnl: float):
        s = self.state
        s.daily_pnl   += pnl
        s.daily_trades += 1
        s.current_equity += pnl

        if pnl < 0:
            s.consecutive_losses += 1
        else:
            s.consecutive_losses = 0

        if s.current_equity > s.peak_equity:
            s.peak_equity = s.current_equity

        self._save()
        logger.info(
            f"Trade PnL: {pnl:+.2f} USDT | "
            f"Equity: {s.current_equity:.2f} | "
            f"Daily: {s.daily_pnl:+.2f} | "
            f"Consec. losses: {s.consecutive_losses}"
        )

    def update_equity(self, equity: float):
        s = self.state
        s.current_equity = equity
        if equity > s.peak_equity:
            s.peak_equity = equity
        self._save()

    def status_dict(self) -> dict:
        self._check_daily_reset()
        s = self.state
        c = self.c
        dd = (s.peak_equity - s.current_equity) / s.peak_equity * 100 if s.peak_equity > 0 else 0
        return {
            "equity":         round(s.current_equity, 2),
            "peak_equity":    round(s.peak_equity, 2),
            "drawdown_pct":   round(dd, 2),
            "daily_pnl":      round(s.daily_pnl, 2),
            "daily_trades":   s.daily_trades,
            "consec_losses":  s.consecutive_losses,
            "circuit_open":   s.circuit_open,
            "circuit_reason": s.circuit_reason,
            "daily_loss_limit": f"{c['max_daily_loss_pct']}%",
            "max_dd_limit":     f"{c['max_drawdown_pct']}%",
        }
