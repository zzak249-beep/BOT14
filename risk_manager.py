"""
QF×JP Bot v7.6 — Risk Manager ANTI-LIQUIDACIÓN + DAILY LOSS REAL + CORRELATION GUARD
═══════════════════════════════════════════════════════════════════════════════
FIXES vs v7.1:
  ✅ Correlation Guard: direction_allowed() limita trades en la misma
     dirección (LONG o SHORT) dentro de CORRELATION_WINDOW_SEC.
     Evita apilar el mismo riesgo de mercado (caso FHEU+XNY: dos LONG
     abiertos casi a la vez, cerrados juntos por el mismo movimiento bajista).
  ✅ on_trade_opened() acepta direction= para registrar la dirección en
     _direction_ts (necesario para que el guard funcione)

SIN CAMBIOS vs v7.1:
  ✅ daily_loss_limit incluye PnL no realizado (unrealized_pnl)
  ✅ Notional cap duro MAX_NOTIONAL_USDT
  ✅ Cooldown 2h por símbolo tras pérdida
  ✅ open_count sincronizado solo desde BingX real
═══════════════════════════════════════════════════════════════════════════════
"""
import asyncio
import logging
import time
import math
from datetime import date

import config as C

log = logging.getLogger("risk")


class RiskManager:
    def __init__(self):
        self._lock             = asyncio.Lock()
        self._open_count       = 0
        self._daily_trades     = 0
        self._daily_pnl        = 0.0
        self._last_reset       = date.today()
        # Anti-overtrading y anti-liquidación
        self._symbol_loss_ts:    dict[str, float] = {}  # symbol → ts última pérdida
        self._symbol_trade_cnt:  dict[str, int]   = {}  # symbol → trades hoy
        self._LOSS_COOLDOWN    = 7200.0   # 2h cooldown tras pérdida en mismo par
        self._MAX_PER_SYMBOL   = 2        # máx 2 trades por par al día
        # FIX v7.1: último PnL no realizado conocido (para status/logging)
        self._last_unrealized  = 0.0
        # Correlation Guard: timestamps de apertura por dirección
        self._direction_ts: dict[str, list] = {"LONG": [], "SHORT": []}

    # ── Reset diario ──────────────────────────────────────────────────────────

    def _check_reset(self):
        today = date.today()
        if today != self._last_reset:
            log.info("Reset diario: trades=%d pnl_cerrado=%.2f pnl_no_real=%.2f",
                     self._daily_trades, self._daily_pnl, self._last_unrealized)
            self._daily_trades     = 0
            self._daily_pnl        = 0.0
            self._last_reset       = today
            self._symbol_trade_cnt = {}
            self._last_unrealized  = 0.0

    # ── Consultas de permisos ─────────────────────────────────────────────────

    async def can_trade(self, unrealized_pnl: float = 0.0) -> tuple[bool, str]:
        """
        Verifica si el bot puede abrir un nuevo trade.

        FIX v7.1: `unrealized_pnl` es la suma del PnL no realizado de TODAS
        las posiciones abiertas trackeadas (calculado por PositionManager
        con el mark price actual). El límite de pérdida diaria ahora se
        evalúa sobre (daily_pnl_cerrado + unrealized_pnl), no solo el
        cerrado. Si no se pasa, se asume 0.0 (comportamiento legacy).
        """
        async with self._lock:
            self._check_reset()
            self._last_unrealized = unrealized_pnl

            if self._open_count >= C.MAX_OPEN_TRADES:
                return False, f"max_open_trades({self._open_count}/{C.MAX_OPEN_TRADES})"
            if self._daily_trades >= C.MAX_DAILY_TRADES:
                return False, f"max_daily_trades({self._daily_trades}/{C.MAX_DAILY_TRADES})"

            # ── Daily loss limit: ahora incluye PnL no realizado ──────────────
            daily_limit  = C.CAPITAL * (C.DAILY_LOSS_PCT / 100.0)
            total_pnl    = self._daily_pnl + unrealized_pnl
            if total_pnl < -daily_limit:
                return False, (
                    f"daily_drawdown(cerrado={self._daily_pnl:.2f} "
                    f"no_real={unrealized_pnl:.2f} total={total_pnl:.2f} "
                    f"< -{daily_limit:.2f}, limit={C.DAILY_LOSS_PCT}%)"
                )
            return True, ""

    def symbol_allowed(self, symbol: str) -> tuple[bool, str]:
        """Verifica cooldown y límite de trades por símbolo."""
        now = time.time()
        last_loss = self._symbol_loss_ts.get(symbol, 0)
        if now - last_loss < self._LOSS_COOLDOWN:
            mins = int((self._LOSS_COOLDOWN - (now - last_loss)) / 60)
            return False, f"cooldown({symbol},{mins}min)"
        cnt = self._symbol_trade_cnt.get(symbol, 0)
        if cnt >= self._MAX_PER_SYMBOL:
            return False, f"max_trades_symbol({symbol},{cnt}/{self._MAX_PER_SYMBOL})"
        return True, ""

    def direction_allowed(self, direction: str) -> tuple[bool, str]:
        """
        Correlation Guard — evita apilar el mismo riesgo de mercado.
        Caso real: FHEU+XNY, dos LONG abiertos casi a la vez que cerraron
        juntos por el mismo movimiento bajista — no eran independientes.

        Limita cuántos trades en la misma dirección (LONG o SHORT) se pueden
        abrir dentro de CORRELATION_WINDOW_SEC. Si ya hay MAX_SAME_DIRECTION
        recientes, bloquea nuevas entradas en esa dirección.
        """
        now = time.time()
        ts_list = self._direction_ts.get(direction, [])
        # Purgar timestamps fuera de la ventana
        ts_list = [t for t in ts_list if now - t < C.CORRELATION_WINDOW_SEC]
        self._direction_ts[direction] = ts_list
        if len(ts_list) >= C.MAX_SAME_DIRECTION:
            mins = int(C.CORRELATION_WINDOW_SEC / 60)
            return False, f"correlation_guard({direction},{len(ts_list)}/{C.MAX_SAME_DIRECTION} en {mins}min)"
        return True, ""

    def tier_ok(self, tier: str) -> bool:
        order = {"NONE": 0, "STD": 1, "FUEL": 2, "SUP": 3}
        return order.get(tier, 0) >= order.get(C.MIN_TIER, 1)

    # ── Eventos ───────────────────────────────────────────────────────────────

    async def on_trade_opened(self, symbol: str = "", direction: str = ""):
        async with self._lock:
            self._open_count   += 1
            self._daily_trades += 1
            if symbol:
                self._symbol_trade_cnt[symbol] = self._symbol_trade_cnt.get(symbol, 0) + 1
            if direction in ("LONG", "SHORT"):
                self._direction_ts.setdefault(direction, []).append(time.time())
            log.info("Trade abierto — open=%d daily=%d symbol=%s dir=%s",
                     self._open_count, self._daily_trades, symbol, direction)

    async def on_trade_closed(self, pnl: float = 0.0, symbol: str = ""):
        async with self._lock:
            self._open_count = max(0, self._open_count - 1)
            self._daily_pnl += pnl
            if symbol and pnl < 0:
                self._symbol_loss_ts[symbol] = time.time()
                log.info("Cooldown 2h activado para %s (pérdida %.4f)", symbol, pnl)
            log.info("Trade cerrado — pnl=%.4f daily_pnl_cerrado=%.4f open=%d",
                     pnl, self._daily_pnl, self._open_count)

    async def update_open_count(self, real_count: int):
        """Sincroniza con BingX real — fuente de verdad."""
        async with self._lock:
            if self._open_count != real_count:
                log.debug("open_count %d → %d (BingX real)", self._open_count, real_count)
                self._open_count = real_count

    # ── Kelly sizing con cap duro ─────────────────────────────────────────────

    def kelly_position_size(self, balance: float, entry: float,
                             sl: float, score: float, tier: str) -> float:
        if entry <= 0 or sl <= 0 or abs(entry - sl) < 1e-12:
            return 0.0

        w = C.KELLY_WIN_RATE
        r = C.KELLY_RR
        kelly = max(0.0, (w * r - (1 - w)) / r) * C.KELLY_FRACTION
        tier_mult = {"STD": 1.0, "FUEL": 1.2, "SUP": 1.5}.get(tier, 1.0)
        kelly *= tier_mult

        risk_usdt = balance * (C.RISK_PCT / 100) * kelly
        sl_dist   = abs(entry - sl)
        qty       = (risk_usdt * C.LEVERAGE) / (sl_dist * entry) if sl_dist * entry > 0 else 0.0

        # ── CAP DURO ANTI-LIQUIDACIÓN ─────────────────────────────────────────
        # ILV -43%, ADA -52%, PI -35% → posiciones demasiado grandes
        notional = qty * entry
        cap = C.MAX_NOTIONAL_USDT
        if notional > cap:
            log.info("[sizing] %s notional %.0f→%.0f USDT (cap=%.0f)",
                     tier, notional, cap, cap)
            qty = cap / entry

        log.info("[sizing] %s score=%.1f risk=%.4f USDT qty=%.6f notional=%.2f USDT",
                 tier, score, risk_usdt, qty, qty * entry)
        return max(0.0, qty)

    # ── Status ────────────────────────────────────────────────────────────────

    def status(self, unrealized_pnl: float = None) -> dict:
        self._check_reset()
        daily_limit = C.CAPITAL * (C.DAILY_LOSS_PCT / 100.0)

        # FIX v7.1: si no se pasa unrealized_pnl explícito, usar el último
        # valor conocido (actualizado en cada can_trade()).
        unreal = unrealized_pnl if unrealized_pnl is not None else self._last_unrealized
        total_pnl = self._daily_pnl + unreal

        return {
            "open_trades":       self._open_count,
            "daily_trades":      self._daily_trades,
            "daily_pnl":         round(self._daily_pnl, 4),       # solo cerrado (compat legacy)
            "daily_pnl_no_real": round(unreal, 4),                # FIX v7.1
            "daily_pnl_total":   round(total_pnl, 4),             # FIX v7.1: cerrado + no realizado
            "daily_limit":       round(-daily_limit, 2),
            "max_open":          C.MAX_OPEN_TRADES,
            "max_daily":         C.MAX_DAILY_TRADES,
            "mode":              C.MODE,
        }
