"""
GUA-USDT Bot v2 — Gestor de Posiciones
Dynamic SL según régimen ATR · Partial TP → BE → Trailing · Force-close.
"""

from __future__ import annotations
import logging, time
from dataclasses import dataclass, field
from typing import Optional

import config
from exchange import BingXClient
from notifier import Notifier
from strategy import Signal

log = logging.getLogger("pm")


@dataclass
class ActivePosition:
    symbol:    str
    direction: str
    entry:     float
    sl:        float
    tp1:       float
    tp2:       float
    atr:       float
    atr_pct:   float
    qty_total: float
    qty_left:  float
    tp1_hit:   bool  = False
    trail_sl:  float = 0.0
    opened_at: float = field(default_factory=time.time)
    peak_price: float = 0.0   # máximo favorable alcanzado


class PositionManager:

    def __init__(self, client: BingXClient, notifier: Notifier) -> None:
        self._c   = client
        self._n   = notifier
        self._pos: Optional[ActivePosition] = None
        self._cooldown_until: float = 0.0

    @property
    def has_position(self) -> bool:
        return self._pos is not None

    @property
    def in_cooldown(self) -> bool:
        return time.time() < self._cooldown_until

    # ── Apertura ───────────────────────────────────────────────────────────────

    async def open_position(self, signal: Signal) -> None:
        if self.has_position or self.in_cooldown:
            return

        try:
            balance = await self._c.get_balance()
        except Exception as e:
            log.error("Balance error: %s", e); return

        if balance <= 0:
            log.error("Balance cero"); return

        # Tamaño dinámico: más pequeño en alta volatilidad
        vol_factor = 0.8 if signal.atr_pct >= 75 else 1.0
        risk_usd   = balance * config.RISK_PCT * config.LEVERAGE * vol_factor
        sl_dist    = signal.atr * (
            config.ATR_HIGHVOL_MULT if signal.atr_pct >= 75 else config.ATR_SL_MULT
        )
        qty = round(risk_usd / max(sl_dist, 0.000001), 4)
        if qty <= 0:
            log.error("Qty inválida: %s", qty); return

        side = "BUY" if signal.direction == "LONG" else "SELL"

        if config.MODE == "LIVE":
            try:
                await self._c.set_leverage(config.SYMBOL, config.LEVERAGE)
                result = await self._c.place_market_order(config.SYMBOL, side, qty)
                log.info("Orden ejecutada: %s", result)
            except Exception as e:
                log.error("Error orden: %s", e)
                await self._n.send_error(f"❌ Error apertura: {e}")
                return
        else:
            log.info("[SIGNAL] %s qty=%.4f (no ejecutado)", signal.direction, qty)

        init_trail = (
            signal.price - signal.atr * config.ATR_TRAIL_MULT
            if signal.direction == "LONG"
            else signal.price + signal.atr * config.ATR_TRAIL_MULT
        )

        self._pos = ActivePosition(
            symbol     = config.SYMBOL,
            direction  = signal.direction,
            entry      = signal.price,
            sl         = signal.sl,
            tp1        = signal.tp1,
            tp2        = signal.tp2,
            atr        = signal.atr,
            atr_pct    = signal.atr_pct,
            qty_total  = qty,
            qty_left   = qty,
            trail_sl   = init_trail,
            peak_price = signal.price,
        )
        await self._n.send_entry(signal, qty, balance)

    # ── Monitor ────────────────────────────────────────────────────────────────

    async def monitor(self, price: float) -> None:
        if not self._pos:
            return
        p = self._pos

        # Actualizar peak
        if p.direction == "LONG"  and price > p.peak_price: p.peak_price = price
        if p.direction == "SHORT" and price < p.peak_price: p.peak_price = price

        # TP1
        if not p.tp1_hit:
            tp1_hit = (p.direction=="LONG" and price>=p.tp1) or \
                      (p.direction=="SHORT" and price<=p.tp1)
            if tp1_hit:
                await self._partial_close(p, price, "TP1")
                p.tp1_hit = True
                p.sl = p.entry      # BE
                return

        # Trailing actualizado dinámicamente
        if p.tp1_hit:
            if p.direction == "LONG":
                new_t = price - p.atr * config.ATR_TRAIL_MULT
                if new_t > p.trail_sl: p.trail_sl = new_t
            else:
                new_t = price + p.atr * config.ATR_TRAIL_MULT
                if new_t < p.trail_sl: p.trail_sl = new_t

        # TP2
        tp2_hit = (p.direction=="LONG" and price>=p.tp2) or \
                  (p.direction=="SHORT" and price<=p.tp2)
        if tp2_hit:
            await self._full_close(p, price, "TP2"); return

        # SL / Trailing SL
        sl_ref = p.trail_sl if p.tp1_hit else p.sl
        sl_hit = (p.direction=="LONG"  and price<=sl_ref) or \
                 (p.direction=="SHORT" and price>=sl_ref)
        if sl_hit:
            label = "Trailing SL" if p.tp1_hit else "SL"
            await self._full_close(p, price, label)

    # ── Cierres ────────────────────────────────────────────────────────────────

    async def _partial_close(self, p: ActivePosition, price: float, label: str) -> None:
        qty  = round(p.qty_total * 0.5, 4)
        side = "SELL" if p.direction == "LONG" else "BUY"
        pnl  = self._pnl(p, price, qty)
        if config.MODE == "LIVE":
            try:
                await self._c.place_market_order(config.SYMBOL, side, qty, reduce_only=True)
            except Exception as e:
                log.error("Partial close error: %s", e); return
        p.qty_left -= qty
        await self._n.send_tp(label, price, pnl, partial=True)
        log.info("%s parcial @%.5f | PnL=%.4f", label, price, pnl)

    async def _full_close(self, p: ActivePosition, price: float, label: str) -> None:
        side = "SELL" if p.direction == "LONG" else "BUY"
        pnl  = self._pnl(p, price, p.qty_left)
        if config.MODE == "LIVE":
            try:
                await self._c.place_market_order(config.SYMBOL, side, p.qty_left, reduce_only=True)
            except Exception as e:
                log.error("Full close error: %s", e); return
        is_sl = label in ("SL", "Trailing SL")
        await self._n.send_close(label, price, pnl, is_sl=is_sl)
        log.info("%s @%.5f | PnL=%.4f", label, price, pnl)
        self._pos = None
        self._cooldown_until = time.time() + config.COOLDOWN_MIN * 60

    def _pnl(self, p: ActivePosition, price: float, qty: float) -> float:
        mult = config.LEVERAGE
        return ((price - p.entry) * qty * mult if p.direction == "LONG"
                else (p.entry - price) * qty * mult)

    async def force_close(self, price: float, reason: str = "manual") -> None:
        if self._pos:
            await self._full_close(self._pos, price, f"Cierre forzado ({reason})")

    def status(self, price: float) -> str:
        if not self._pos:
            return "Sin posición activa"
        p = self._pos
        pnl    = self._pnl(p, price, p.qty_left)
        trail  = f"\n🔄 Trail SL: {p.trail_sl:.5f}" if p.tp1_hit else ""
        volreg = "🌋 Alta volatilidad" if p.atr_pct >= 75 else "📊 Normal"
        return (
            f"{'📈 LONG' if p.direction=='LONG' else '📉 SHORT'} GUA-USDT\n"
            f"Entry: {p.entry:.5f} | Precio: {price:.5f}\n"
            f"SL: {p.sl:.5f}{trail}\n"
            f"TP1: {p.tp1:.5f} {'✅' if p.tp1_hit else '⏳'}\n"
            f"TP2: {p.tp2:.5f}\n"
            f"Qty left: {p.qty_left:.4f} | PnL: {pnl:+.4f} USDT\n"
            f"Peak: {p.peak_price:.5f} | {volreg}"
        )
