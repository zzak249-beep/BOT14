"""
QF×JP Bot v7.1 — Risk Manager ANTI-LIQUIDACIÓN (renewed-love)
Fixes vs v6.5:
  - NUEVO: daily_drawdown ahora incluye PnL NO REALIZADO de posiciones abiertas
           (antes solo contaba PnL cerrado → -329 USDT no bloqueaba nuevas entradas)
  - can_trade() y status() usan effective_daily_pnl = _daily_pnl(realizado) + unrealized_pnl(abiertas)
  - Requiere registrar un proveedor de PnL no realizado vía set_unrealized_pnl_provider()
  - Resto de lógica v6.5 sin cambios (cooldown, max por símbolo, Kelly cap)
"""
import asyncio
import logging
import time
import math
from datetime import date
from typing import Callable, Awaitable, Optional, Union

import config as C

log = logging.getLogger("risk")

# Tipo del provider: función (sync o async) que devuelve el PnL no realizado total (float)
PnLProvider = Callable[[], Union[float, Awaitable[float]]]


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

        # ── NUEVO v7.1: proveedor de PnL no realizado ──────────────────────────
        self._unrealized_pnl_provider: Optional[PnLProvider] = None

    # ── Registro del proveedor de PnL no realizado ─────────────────────────────

    def set_unrealized_pnl_provider(self, provider: PnLProvider):
        """
        Registra una función que devuelve el PnL no realizado TOTAL (suma de todas
        las posiciones abiertas trackeadas por position_manager), en USDT.

        Puede ser sync (-> float) o async (-> Awaitable[float]).
        Ejemplo de uso en main.py:
            risk.set_unrealized_pnl_provider(position_manager.get_total_unrealized_pnl)
        """
        self._unrealized_pnl_provider = provider

    async def _get_unrealized_pnl(self) -> float:
        if self._unrealized_pnl_provider is None:
            return 0.0
        try:
            result = self._unrealized_pnl_provider()
            if asyncio.iscoroutine(result):
                result = await result
            return float(result or 0.0)
        except Exception:
            log.exception("Error obteniendo PnL no realizado para daily_drawdown check")
            return 0.0

    # ── Reset diario ──────────────────────────────────────────────────────────

    def _check_reset(self):
        today = date.today()
        if today != self._last_reset:
            log.info("Reset diario: trades=%d pnl_realizado=%.2f", self._daily_trades, self._daily_pnl)
            self._daily_trades     = 0
            self._daily_pnl        = 0.0
            self._last_reset       = today
            self._symbol_trade_cnt = {}

    # ── Consultas de permisos ─────────────────────────────────────────────────

    async def can_trade(self) -> tuple[bool, str]:
        # _get_unrealized_pnl no debe llamarse dentro del lock (puede hacer I/O / await externo)
        unrealized = await self._get_unrealized_pnl()

        async with self._lock:
            self._check_reset()
            if self._open_count >= C.MAX_OPEN_TRADES:
                return False, f"max_open_trades({self._open_count}/{C.MAX_OPEN_TRADES})"
            if self._daily_trades >= C.MAX_DAILY_TRADES:
                return False, f"max_daily_trades({self._daily_trades}/{C.MAX_DAILY_TRADES})"

            # ── FIX v7.1: daily loss limit ahora incluye PnL no realizado ──────
            effective_pnl = self._daily_pnl + unrealized
            daily_limit = C.CAPITAL * (C.DAILY_LOSS_PCT / 100.0)
            if effective_pnl < -daily_limit:
                return False, (
                    f"daily_drawdown(realizado={self._daily_pnl:.2f} "
                    f"no_realizado={unrealized:.2f} "
                    f"total={effective_pnl:.2f} < -{daily_limit:.2f}, "
                    f"limit={C.DAILY_LOSS_PCT}%)"
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

    def tier_ok(self, tier: str) -> bool:
        order = {"NONE": 0, "STD": 1, "FUEL": 2, "SUP": 3}
        return order.get(tier, 0) >= order.get(C.MIN_TIER, 1)

    # ── Eventos ───────────────────────────────────────────────────────────────

    async def on_trade_opened(self, symbol: str = ""):
        async with self._lock:
            self._open_count   += 1
            self._daily_trades += 1
            if symbol:
                self._symbol_trade_cnt[symbol] = self._symbol_trade_cnt.get(symbol, 0) + 1
            log.info("Trade abierto — open=%d daily=%d symbol=%s",
                     self._open_count, self._daily_trades, symbol)

    async def on_trade_closed(self, pnl: float = 0.0, symbol: str = ""):
        async with self._lock:
            self._open_count = max(0, self._open_count - 1)
            self._daily_pnl += pnl
            if symbol and pnl < 0:
                self._symbol_loss_ts[symbol] = time.time()
                log.info("Cooldown 2h activado para %s (pérdida %.4f)", symbol, pnl)
            log.info("Trade cerrado — pnl=%.4f daily_pnl_realizado=%.4f open=%d",
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

    async def status(self) -> dict:
        unrealized = await self._get_unrealized_pnl()
        self._check_reset()
        daily_limit = C.CAPITAL * (C.DAILY_LOSS_PCT / 100.0)
        effective_pnl = self._daily_pnl + unrealized
        return {
            "open_trades":         self._open_count,
            "daily_trades":        self._daily_trades,
            "daily_pnl_realized":  round(self._daily_pnl, 4),
            "daily_pnl_unrealized": round(unrealized, 4),
            "daily_pnl_effective": round(effective_pnl, 4),
            "daily_limit":         round(-daily_limit, 2),
            "max_open":            C.MAX_OPEN_TRADES,
            "max_daily":           C.MAX_DAILY_TRADES,
            "mode":                C.MODE,
        }
