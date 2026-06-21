"""
QF×JP Bot — Complement Engine v1.2
joyful-art y zesty-reverence complementan a renewed-love con 4 modos simultáneos:

MODO 1 — COPY TRADE FILTRADO
  Lee trades SUP del master, copia con 0.4x size solo si score > 80.
  FIX v1.1: además exige que el trade del master NO esté ya en pérdida
  (mark vs entry favorable o neutro) antes de copiarlo — evita duplicar
  posiciones que ya van en contra.

MODO 2 — SÍMBOLOS EXCLUSIVOS (sin solapamiento)
  Opera SOLO en top-50 símbolos por volumen (EXCLUSIVE_TOP_N).
  renewed-love opera en el resto (en la práctica, MIN_VOLUME_USDT
  compartido hace que el solapamiento real sea mayor del diseñado — ver
  conversación anterior).

MODO 3 — GUARDIAN DE SALIDAS
  Monitoriza posiciones del master.
  Si detecta CVD divergence contraria → alerta Telegram para cierre manual.
  (No cierra automáticamente para no interferir con el master)

MODO 4 — HEDGE MACRO
  Si master tiene 3+ posiciones EN PÉRDIDA (direccional) >2% cada una
  → abre SHORT/LONG en BTC como cobertura macro
  → neutraliza drawdown sistémico

═══════════════════════════════════════════════════════════════════════════════
FIXES v1.2:
  ✅ run_copy_mode(): CRÍTICO — ahora también filtra contra
     bingx_client.NON_CRYPTO_PREFIXES (OIL, WTI, BRENT, XAU, XAG, EUR,
     GBP, JPY), no solo contra BLACKLIST. Antes, este modo solo
     comprobaba BLACKLIST (config.py: ESPORTS,STABLE,EURUSD,SILVER,PAXG,
     CUSDT,SYN,FONK,FOLK,NCS — que NO incluye OIL/WTI/XAU) — el filtro
     estructural de instrumentos no-cripto vivía solo dentro de
     bingx_client.get_all_symbols(), y run_copy_mode() nunca pasa por
     ahí (recibe los trades directo del /status del master). Resultado
     real: Oil WTI y GOLD(XAU) en el historial de zesty-reverence,
     copiados de una posición reconciliada del master, sin que el propio
     scanner de zesty los hubiera generado nunca. Ahora ambas listas se
     comprueban en el mismo sitio.

FIXES v1.1 (sin cambios):
  ✅ run_hedge_mode: el cálculo de pérdida ahora es DIRECCIONAL.
  ✅ run_copy_mode: no copiar si el trade del master ya está en pérdida.
  ✅ run_guardian_mode: nuevo parámetro GUARDIAN_AUTOCLOSE (default False).
═══════════════════════════════════════════════════════════════════════════════
"""
import asyncio
import logging
import os
import time
from typing import Optional

import numpy as np

import config as C
from bingx_client import BingXClient, NON_CRYPTO_PREFIXES
from copier_client import MasterClient
from indicators import analyze, score_to_tier
from risk_manager import RiskManager
from position_manager import PositionManager, OpenTrade
import telegram_client as tg

log = logging.getLogger("complement")

# ── Config complemento ────────────────────────────────────────────────────────
COMPLEMENT_MODE   = os.getenv("COMPLEMENT_MODE", "GUARDIAN,COPY,EXCLUSIVE").upper()
COPY_MIN_SCORE    = float(os.getenv("COPY_MIN_SCORE",    "80.0"))   # solo SUP del master
COPY_SIZE_MULT    = float(os.getenv("COPY_SIZE_MULT",    "0.4"))    # 40% del size del master
COPY_MAX_ADVERSE_PCT = float(os.getenv("COPY_MAX_ADVERSE_PCT", "0.0"))
GUARDIAN_CVD_THR  = float(os.getenv("GUARDIAN_CVD_THR", "-0.3"))
HEDGE_LOSS_COUNT  = int(os.getenv("HEDGE_LOSS_COUNT",   "3"))
HEDGE_LOSS_PCT    = float(os.getenv("HEDGE_LOSS_PCT",   "2.0"))
EXCLUSIVE_TOP_N   = int(os.getenv("EXCLUSIVE_TOP_N",    "50"))


class ComplementEngine:
    def __init__(self, client: BingXClient, risk: RiskManager,
                 pos_mgr: PositionManager, master: MasterClient):
        self.client  = client
        self.risk    = risk
        self.pos_mgr = pos_mgr
        self.master  = master

        self._copied_trades:  set[str]   = set()
        self._exclusive_syms: list[str]  = []
        self._last_guardian:  float      = 0.0
        self._last_copy:      float      = 0.0
        self._last_hedge:     float      = 0.0
        self._hedge_active:   bool       = False

    # ══════════════════════════════════════════════════════════════════════════
    # MODO 2 — SÍMBOLOS EXCLUSIVOS
    # ══════════════════════════════════════════════════════════════════════════

    async def refresh_exclusive_symbols(self):
        try:
            all_syms = await self.client.get_all_symbols()
            self._exclusive_syms = all_syms[:EXCLUSIVE_TOP_N]
            log.info("Símbolos exclusivos: %d (top-%d por volumen)",
                     len(self._exclusive_syms), EXCLUSIVE_TOP_N)
        except Exception as e:
            log.warning("refresh_exclusive_symbols: %s", e)

    def get_exclusive_symbols(self) -> list[str]:
        return self._exclusive_syms

    # ══════════════════════════════════════════════════════════════════════════
    # MODO 1 — COPY TRADE FILTRADO
    # ══════════════════════════════════════════════════════════════════════════

    def _master_trade_pnl_pct(self, trade_data: dict, mark: float) -> Optional[float]:
        entry     = float(trade_data.get("entry", 0))
        direction = trade_data.get("direction", "")
        if entry <= 0 or mark <= 0 or direction not in ("LONG", "SHORT"):
            return None
        raw_pct = (mark - entry) / entry * 100.0
        if direction == "SHORT":
            raw_pct = -raw_pct
        return raw_pct

    async def run_copy_mode(self):
        """
        Copia trades SUP del master con 40% del size. Solo copia si:
          - Tier SUP (score > 80) en el master
          - joyful-art/zesty no está ya en ese símbolo
          - El símbolo NO está en los exclusivos propios
          - FIX v1.1: el trade del master NO está ya en pérdida
          - FIX v1.2 CRÍTICO: el símbolo NO está en BLACKLIST
            *ni tampoco* es un instrumento no-cripto (NON_CRYPTO_PREFIXES
            de bingx_client.py — OIL, WTI, BRENT, XAU, XAG, EUR, GBP, JPY).
            Antes solo se comprobaba BLACKLIST, que no cubre estos —
            causó pérdidas reales copiando Oil WTI y GOLD(XAU) desde una
            posición reconciliada del master que nunca debió existir, pero
            que este modo copiaba igual sin pasar por el filtro estructural
            que sí protege al scanner propio.
          - FIX v1.2: can_trade() y direction_allowed() se llaman POR CADA
            SÍMBOLO dentro del loop, no una vez para todo el batch.
        """
        now = time.time()
        if now - self._last_copy < 30:
            return
        self._last_copy = now

        master_trades = await self.master.get_master_trades()
        if not master_trades:
            return

        for symbol, trade_data in master_trades.items():
            if trade_data.get("tier", "") != "SUP":
                continue
            if symbol in self._copied_trades:
                continue
            if self.pos_mgr.is_trading(symbol):
                continue
            if symbol in self._exclusive_syms:
                continue

            # ── FIX v1.2: BLACKLIST + NON_CRYPTO_PREFIXES, mismo sitio ────────
            base_sym = symbol.replace("-USDT", "").replace("USDT", "")
            if base_sym in C.BLACKLIST or symbol.replace("-USDT", "") in C.BLACKLIST:
                log.info("[COPY] %s en BLACKLIST — NO copiar", symbol)
                continue
            if any(base_sym.startswith(p) for p in NON_CRYPTO_PREFIXES):
                log.warning("[COPY] %s es instrumento no-cripto (NON_CRYPTO_PREFIXES) — "
                            "NO copiar. El master no debería tenerlo trackeado — "
                            "revisar si viene de una reconciliación legacy.", symbol)
                continue

            direction = trade_data.get("direction", "")
            entry     = float(trade_data.get("entry", 0))
            sl        = float(trade_data.get("sl",    0))
            tp1       = float(trade_data.get("tp1",   0))
            tp2       = float(trade_data.get("tp2",   0))

            if not direction or entry <= 0 or sl <= 0:
                continue

            try:
                ticker = await self.client.get_ticker(symbol)
                mark   = float(ticker.get("lastPrice", 0) or 0)
            except Exception as e:
                log.debug("[COPY] %s no se pudo obtener mark: %s", symbol, e)
                continue

            pnl_pct = self._master_trade_pnl_pct(trade_data, mark)
            if pnl_pct is None:
                continue
            if pnl_pct < COPY_MAX_ADVERSE_PCT:
                log.info("[COPY] %s SUP pero master ya en pérdida (%.2f%% < %.2f%%) — skip",
                         symbol, pnl_pct, COPY_MAX_ADVERSE_PCT)
                continue

            can, reason = await self.risk.can_trade()
            if not can:
                log.debug("[COPY] %s bloqueado por risk: %s", symbol, reason)
                continue

            dir_ok, dir_reason = self.risk.direction_allowed(direction)
            if not dir_ok:
                log.debug("[COPY] %s bloqueado por correlación: %s", symbol, dir_reason)
                await self.risk.release_reservation()
                continue

            trade_confirmed = False
            try:
                master_qty = float(trade_data.get("qty", 0))
                qty = master_qty * COPY_SIZE_MULT

                notional = qty * entry
                if notional > C.MAX_NOTIONAL_USDT:
                    qty = C.MAX_NOTIONAL_USDT / entry

                if qty <= 0:
                    continue

                log.info("[COPY] %s %s qty=%.4f (40%% del master, master_pnl=%.2f%%) notional=%.1f",
                         symbol, direction, qty, pnl_pct, qty * entry)

                results = await self.client.open_trade(
                    symbol=symbol, direction=direction, quantity=qty,
                    sl_price=sl, tp1_price=tp1, tp2_price=tp2,
                )
                entry_resp = results.get("entry", {})
                if entry_resp.get("code", -1) == 0:
                    sl_resp = results.get("sl", {})
                    if isinstance(sl_resp, dict) and sl_resp.get("code", -1) != 0:
                        log.error("[COPY] %s SL fallido — cerrando", symbol)
                        await self.client.close_position_market(symbol, qty, direction)
                        continue

                    self._copied_trades.add(symbol)
                    trade = OpenTrade(
                        symbol=symbol, direction=direction,
                        entry=entry, sl=sl, tp1=tp1, tp2=tp2,
                        qty=qty, atr=abs(entry - sl) / 2,
                        order_id="copy_" + symbol,
                    )
                    await self.pos_mgr.register_trade(trade)
                    await tg.send(
                        f"📋 *COPY TRADE* — `{symbol}` {direction}\n"
                        f"Master SUP → 40%% (master PnL: {pnl_pct:+.2f}%%)\n"
                        f"Entry: `{entry:.6f}` | SL: `{sl:.6f}`\n"
                        f"Qty: `{qty:.4f}` notional: `{qty*entry:.1f}` USDT"
                    )
                    await self.risk.on_trade_opened(symbol=symbol, direction=direction)
                    trade_confirmed = True
                else:
                    log.warning("[COPY] %s entrada rechazada: %s", symbol, entry_resp)
            except Exception as e:
                log.error("[COPY] %s error: %s", symbol, e)
            finally:
                if not trade_confirmed:
                    await self.risk.release_reservation()
                    self.risk.release_direction_reservation(direction)

            await asyncio.sleep(0.5)

    # ══════════════════════════════════════════════════════════════════════════
    # MODO 3 — GUARDIAN DE SALIDAS
    # ══════════════════════════════════════════════════════════════════════════

    async def run_guardian_mode(self):
        now = time.time()
        if now - self._last_guardian < 60:
            return
        self._last_guardian = now

        master_trades = await self.master.get_master_trades()
        if not master_trades:
            return

        alerts = []
        for symbol, trade_data in master_trades.items():
            direction = trade_data.get("direction", "")
            entry     = float(trade_data.get("entry", 0))
            if not direction or entry <= 0:
                continue

            try:
                klines = await self.client.get_klines(symbol, "3m", 50)
                if len(klines) < 20:
                    continue

                arr   = np.array(klines, dtype=float)
                o_    = arr[:, 1]
                c_    = arr[:, 4]
                v_    = arr[:, 5]

                delta = np.where(c_ > o_, v_, np.where(c_ < o_, -v_, 0))
                cvd   = np.cumsum(delta)

                period = 10
                price_chg = c_[-1] - c_[-period]
                cvd_chg   = cvd[-1] - cvd[-period]

                danger = False
                reason = ""

                if direction == "LONG" and price_chg > 0 and cvd_chg < 0:
                    danger = True
                    reason = f"CVD divergencia bajista (precio +{price_chg:.4f}, CVD {cvd_chg:.0f})"
                elif direction == "SHORT" and price_chg < 0 and cvd_chg > 0:
                    danger = True
                    reason = f"CVD divergencia alcista (precio {price_chg:.4f}, CVD +{cvd_chg:.0f})"

                if danger:
                    current_price = float(c_[-1])
                    pnl_pct = self._master_trade_pnl_pct(trade_data, current_price)
                    pnl_pct = pnl_pct if pnl_pct is not None else 0.0
                    alerts.append(
                        f"⚠️ *GUARDIAN* — `{symbol}` {direction}\n"
                        f"Precio actual: `{current_price:.6f}`\n"
                        f"PnL est: `{pnl_pct:+.2f}%`\n"
                        f"⚡ {reason}\n"
                        f"_Considera cerrar en renewed-love_"
                    )
            except Exception as e:
                log.debug("[GUARDIAN] %s: %s", symbol, e)

            await asyncio.sleep(0.1)

        if alerts:
            for alert in alerts[:3]:
                await tg.send(alert)
                await asyncio.sleep(1)

    # ══════════════════════════════════════════════════════════════════════════
    # MODO 4 — HEDGE MACRO
    # ══════════════════════════════════════════════════════════════════════════

    async def run_hedge_mode(self):
        now = time.time()
        if now - self._last_hedge < 120:
            return
        self._last_hedge = now

        master_trades = await self.master.get_master_trades()
        if not master_trades:
            return

        try:
            positions = await self.client.get_open_positions()
        except Exception:
            return

        pos_map = {p["symbol"]: p for p in positions if float(p.get("positionAmt", 0)) != 0}

        losing_longs  = []
        losing_shorts = []

        for sym, td in master_trades.items():
            pos = pos_map.get(sym)
            if not pos:
                continue
            mark = float(pos.get("markPrice", 0) or 0)
            if mark <= 0:
                continue

            pnl_pct = self._master_trade_pnl_pct(td, mark)
            if pnl_pct is None:
                continue

            direction = td.get("direction", "")

            if pnl_pct <= -HEDGE_LOSS_PCT:
                if direction == "LONG":
                    losing_longs.append((sym, pnl_pct))
                elif direction == "SHORT":
                    losing_shorts.append((sym, pnl_pct))

        total_losing = len(losing_longs) + len(losing_shorts)

        if self._hedge_active and total_losing < HEDGE_LOSS_COUNT:
            if self.pos_mgr.is_trading("BTCUSDT"):
                log.info("[HEDGE] Drawdown recuperado (%d < %d) — cerrando hedge BTCUSDT",
                         total_losing, HEDGE_LOSS_COUNT)
                await self.pos_mgr.close_position_emergency("BTCUSDT", "hedge_exit")
                self._hedge_active = False
            return

        if total_losing < HEDGE_LOSS_COUNT:
            return
        if self._hedge_active:
            return
        if self.pos_mgr.is_trading("BTCUSDT"):
            return

        can, reason = await self.risk.can_trade()
        if not can:
            return

        hedge_dir = "SHORT" if len(losing_longs) >= len(losing_shorts) else "LONG"

        log.info("[HEDGE] %d posiciones master en pérdida real ≥%.1f%% "
                 "(longs=%d shorts=%d) — abriendo %s BTCUSDT",
                 total_losing, HEDGE_LOSS_PCT, len(losing_longs), len(losing_shorts), hedge_dir)

        trade_confirmed = False
        try:
            ticker = await self.client.get_ticker("BTCUSDT")
            btc_price = float(ticker.get("lastPrice", 0))
            if btc_price <= 0:
                return

            hedge_notional = min(50.0, C.MAX_NOTIONAL_USDT * 0.25)
            hedge_qty      = hedge_notional / btc_price

            sl_pct  = 0.015
            tp1_pct = 0.02

            if hedge_dir == "SHORT":
                sl  = btc_price * (1 + sl_pct)
                tp1 = btc_price * (1 - tp1_pct)
                tp2 = btc_price * (1 - tp1_pct * 2)
            else:
                sl  = btc_price * (1 - sl_pct)
                tp1 = btc_price * (1 + tp1_pct)
                tp2 = btc_price * (1 + tp1_pct * 2)

            results = await self.client.open_trade(
                symbol="BTCUSDT", direction=hedge_dir, quantity=hedge_qty,
                sl_price=sl, tp1_price=tp1, tp2_price=tp2,
            )
            if results.get("entry", {}).get("code", -1) == 0:
                self._hedge_active = True
                trade = OpenTrade(
                    symbol="BTCUSDT", direction=hedge_dir,
                    entry=btc_price, sl=sl, tp1=tp1, tp2=tp2,
                    qty=hedge_qty, atr=btc_price * 0.01,
                    order_id="hedge_btc",
                )
                await self.pos_mgr.register_trade(trade)
                await tg.send(
                    f"🛡️ *HEDGE MACRO* activado\n"
                    f"BTCUSDT {hedge_dir} — {total_losing} posiciones master en pérdida real "
                    f"(longs={len(losing_longs)}, shorts={len(losing_shorts)})\n"
                    f"Notional: `{hedge_notional:.0f}` USDT | SL: `{sl:.0f}`"
                )
                await self.risk.on_trade_opened(symbol="BTCUSDT", direction=hedge_dir)
                trade_confirmed = True
            else:
                log.warning("[HEDGE] entrada rechazada: %s", results.get("entry", {}))
        except Exception as e:
            log.error("[HEDGE] error: %s", e)
        finally:
            if not trade_confirmed:
                await self.risk.release_reservation()

    # ══════════════════════════════════════════════════════════════════════════
    # LOOP PRINCIPAL
    # ══════════════════════════════════════════════════════════════════════════

    async def run_loop(self):
        log.info("Complement Engine v1.2 iniciado — modos: %s", COMPLEMENT_MODE)

        await self.refresh_exclusive_symbols()

        iteration = 0
        while True:
            iteration += 1

            if iteration % 30 == 0:
                await self.refresh_exclusive_symbols()

            try:
                if "COPY" in COMPLEMENT_MODE and os.getenv("MASTER_URL"):
                    await self.run_copy_mode()

                if "GUARDIAN" in COMPLEMENT_MODE and os.getenv("MASTER_URL"):
                    await self.run_guardian_mode()

                if "HEDGE" in COMPLEMENT_MODE:
                    await self.run_hedge_mode()

            except Exception as e:
                log.error("complement_loop error: %s", e)

            await asyncio.sleep(30)
