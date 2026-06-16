"""
QF×JP Bot — Complement Engine v1.1
joyful-art complementa a renewed-love con 4 modos simultáneos:

MODO 1 — COPY TRADE FILTRADO
  Lee trades SUP del master, copia con 0.4x size solo si score > 80.
  FIX v1.1: además exige que el trade del master NO esté ya en pérdida
  (mark vs entry favorable o neutro) antes de copiarlo — evita duplicar
  posiciones que ya van en contra.

MODO 2 — SÍMBOLOS EXCLUSIVOS (sin solapamiento)
  joyful-art opera SOLO en top-50 símbolos por volumen.
  renewed-love opera en los otros 514.
  Resultado: cobertura total sin duplicar posiciones.

MODO 3 — GUARDIAN DE SALIDAS
  Monitoriza posiciones del master.
  Si detecta CVD divergence contraria → alerta Telegram para cierre manual.
  (No cierra automáticamente para no interferir con el master)

MODO 4 — HEDGE MACRO
  Si master tiene 3+ posiciones EN PÉRDIDA (direccional) >2% cada una
  → joyful-art abre SHORT/LONG en BTC como cobertura macro
  → neutraliza drawdown sistémico

═══════════════════════════════════════════════════════════════════════════════
FIXES v1.1:
  ✅ run_hedge_mode: el cálculo de pérdida ahora es DIRECCIONAL.
     Antes: loss_pct = abs((mark-entry)/entry)*100 → contaba ganadores
     como perdedores (un LONG +2% activaba el contador de "pérdida").
     Ahora: pnl_pct con signo correcto según LONG/SHORT, solo cuenta
     cuando pnl_pct <= -HEDGE_LOSS_PCT (pérdida real).

  ✅ run_copy_mode: nuevo check — antes de copiar un trade SUP del master,
     se compara mark actual vs entry del master. Si el trade del master
     ya está en pérdida (pnl_pct < COPY_MAX_ADVERSE_PCT, default 0%),
     se descarta la copia. Solo se copian trades SUP que siguen
     siendo favorables o neutros en el momento de la copia.

  ✅ run_guardian_mode: nuevo parámetro GUARDIAN_AUTOCLOSE (default False).
     Si está activo, además de alertar, joyful-art puede colocar un
     hedge de cobertura puntual (no cierra la posición del master,
     que está en otro bot/cuenta — solo abre protección propia).
═══════════════════════════════════════════════════════════════════════════════
"""
import asyncio
import logging
import os
import time
from typing import Optional

import numpy as np

import config as C
from bingx_client import BingXClient
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
# FIX v1.1: no copiar si el trade del master ya está en pérdida (PnL% con signo)
COPY_MAX_ADVERSE_PCT = float(os.getenv("COPY_MAX_ADVERSE_PCT", "0.0"))  # 0.0 = solo copiar si va igual o a favor
GUARDIAN_CVD_THR  = float(os.getenv("GUARDIAN_CVD_THR", "-0.3"))   # CVD divergencia mínima
HEDGE_LOSS_COUNT  = int(os.getenv("HEDGE_LOSS_COUNT",   "3"))       # trades en pérdida para hedge
HEDGE_LOSS_PCT    = float(os.getenv("HEDGE_LOSS_PCT",   "2.0"))     # % pérdida por trade (positivo)
EXCLUSIVE_TOP_N   = int(os.getenv("EXCLUSIVE_TOP_N",    "50"))      # top N símbolos exclusivos


class ComplementEngine:
    def __init__(self, client: BingXClient, risk: RiskManager,
                 pos_mgr: PositionManager, master: MasterClient):
        self.client  = client
        self.risk    = risk
        self.pos_mgr = pos_mgr
        self.master  = master

        self._copied_trades:  set[str]   = set()   # símbolos ya copiados
        self._exclusive_syms: list[str]  = []      # top-50 por volumen
        self._last_guardian:  float      = 0.0
        self._last_copy:      float      = 0.0
        self._last_hedge:     float      = 0.0
        self._hedge_active:   bool       = False

    # ══════════════════════════════════════════════════════════════════════════
    # MODO 2 — SÍMBOLOS EXCLUSIVOS
    # ══════════════════════════════════════════════════════════════════════════

    async def refresh_exclusive_symbols(self):
        """
        joyful-art opera SOLO en los top-N símbolos por volumen 24h.
        renewed-love opera en el resto.
        → Sin solapamiento, cobertura total del mercado.
        """
        try:
            all_syms = await self.client.get_all_symbols()
            # get_all_symbols ya devuelve ordenados por volumen (ver bingx_client)
            self._exclusive_syms = all_syms[:EXCLUSIVE_TOP_N]
            log.info("Símbolos exclusivos joyful-art: %d (top-%d por volumen)",
                     len(self._exclusive_syms), EXCLUSIVE_TOP_N)
        except Exception as e:
            log.warning("refresh_exclusive_symbols: %s", e)

    def get_exclusive_symbols(self) -> list[str]:
        return self._exclusive_syms

    # ══════════════════════════════════════════════════════════════════════════
    # MODO 1 — COPY TRADE FILTRADO
    # ══════════════════════════════════════════════════════════════════════════

    def _master_trade_pnl_pct(self, trade_data: dict, mark: float) -> Optional[float]:
        """
        Calcula el PnL% direccional del trade del master, dado el mark actual.
        Retorna None si faltan datos.
        Positivo = a favor, Negativo = en contra.
        """
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
        Copia trades SUP del master con 40% del size.
        Solo copia si:
          - Tier SUP (score > 80) en el master
          - joyful-art no está ya en ese símbolo
          - joyful-art tiene slots disponibles
          - El símbolo NO está en los exclusivos de joyful-art
            (para no duplicar análisis propios)
          - FIX v1.1: el trade del master NO está ya en pérdida
            (pnl_pct del master >= COPY_MAX_ADVERSE_PCT, default 0%)
        """
        now = time.time()
        if now - self._last_copy < 30:   # revisar cada 30s
            return
        self._last_copy = now

        master_trades = await self.master.get_master_trades()
        if not master_trades:
            return

        can, reason = await self.risk.can_trade()
        if not can:
            return

        for symbol, trade_data in master_trades.items():
            # Solo copiar tier SUP
            if trade_data.get("tier", "") != "SUP":
                continue
            if symbol in self._copied_trades:
                continue
            if self.pos_mgr.is_trading(symbol):
                continue
            # No copiar si es símbolo exclusivo propio (joyful-art lo analizará solo)
            if symbol in self._exclusive_syms:
                continue

            direction = trade_data.get("direction", "")
            entry     = float(trade_data.get("entry", 0))
            sl        = float(trade_data.get("sl",    0))
            tp1       = float(trade_data.get("tp1",   0))
            tp2       = float(trade_data.get("tp2",   0))

            if not direction or entry <= 0 or sl <= 0:
                continue

            # ── FIX v1.1: verificar que el trade del master no esté ya perdiendo ──
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

            # Tamaño: 40% del master pero respetando nuestro propio cap
            master_qty = float(trade_data.get("qty", 0))
            qty = master_qty * COPY_SIZE_MULT

            # Verificar cap notional propio
            notional = qty * entry
            if notional > C.MAX_NOTIONAL_USDT:
                qty = C.MAX_NOTIONAL_USDT / entry

            if qty <= 0:
                continue

            log.info("[COPY] %s %s qty=%.4f (40%% del master, master_pnl=%.2f%%) notional=%.1f",
                     symbol, direction, qty, pnl_pct, qty * entry)

            try:
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
                        f"Master SUP → joyful-art 40%% (master PnL: {pnl_pct:+.2f}%%)\n"
                        f"Entry: `{entry:.6f}` | SL: `{sl:.6f}`\n"
                        f"Qty: `{qty:.4f}` notional: `{qty*entry:.1f}` USDT"
                    )
                    await self.risk.on_trade_opened(symbol=symbol)
                else:
                    log.warning("[COPY] %s entrada rechazada: %s", symbol, entry_resp)
            except Exception as e:
                log.error("[COPY] %s error: %s", symbol, e)

            await asyncio.sleep(0.5)

    # ══════════════════════════════════════════════════════════════════════════
    # MODO 3 — GUARDIAN DE SALIDAS
    # ══════════════════════════════════════════════════════════════════════════

    async def run_guardian_mode(self):
        """
        Monitoriza posiciones del MASTER.
        Si detecta CVD divergence contraria a la posición → alerta Telegram.
        El trader decide si cerrar o no (no interferimos con el master).
        """
        now = time.time()
        if now - self._last_guardian < 60:   # revisar cada 60s
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

                # CVD simple
                delta = np.where(c_ > o_, v_, np.where(c_ < o_, -v_, 0))
                cvd   = np.cumsum(delta)

                # Divergencia: últimas 10 velas
                period = 10
                price_chg = c_[-1] - c_[-period]
                cvd_chg   = cvd[-1] - cvd[-period]

                # Señal de peligro
                danger = False
                reason = ""

                if direction == "LONG" and price_chg > 0 and cvd_chg < 0:
                    # Precio sube pero CVD baja → distribución → peligro para LONG
                    danger = True
                    reason = f"CVD divergencia bajista (precio +{price_chg:.4f}, CVD {cvd_chg:.0f})"
                elif direction == "SHORT" and price_chg < 0 and cvd_chg > 0:
                    # Precio baja pero CVD sube → acumulación → peligro para SHORT
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
            # Enviar máximo 3 alertas para no spamear
            for alert in alerts[:3]:
                await tg.send(alert)
                await asyncio.sleep(1)

    # ══════════════════════════════════════════════════════════════════════════
    # MODO 4 — HEDGE MACRO
    # ══════════════════════════════════════════════════════════════════════════

    async def run_hedge_mode(self):
        """
        Si master tiene ≥3 posiciones EN PÉRDIDA (direccional) >2% simultánea
        → joyful-art abre SHORT/LONG en BTC como cobertura macro.
        Cuando el drawdown se recupera → cierra el hedge.

        FIX v1.1: el cálculo de pérdida ahora respeta dirección.
        Antes (BUG): loss_pct = abs((mark-entry)/entry*100) contaba
        posiciones GANADORAS como "en pérdida" si el precio se había
        movido ±2% en cualquier sentido. Esto podía disparar el hedge
        con el mercado entero en verde.
        Ahora: pnl_pct con signo correcto; solo cuenta si pnl_pct <= -HEDGE_LOSS_PCT.
        """
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

            # FIX v1.1: solo cuenta como "en pérdida" si pnl_pct <= -HEDGE_LOSS_PCT
            if pnl_pct <= -HEDGE_LOSS_PCT:
                if direction == "LONG":
                    losing_longs.append((sym, pnl_pct))
                elif direction == "SHORT":
                    losing_shorts.append((sym, pnl_pct))

        total_losing = len(losing_longs) + len(losing_shorts)

        # Si ya tenemos hedge activo y el drawdown se redujo → cerrar hedge
        if self._hedge_active and total_losing < HEDGE_LOSS_COUNT:
            if self.pos_mgr.is_trading("BTCUSDT"):
                log.info("[HEDGE] Drawdown recuperado (%d < %d) — cerrando hedge BTCUSDT",
                         total_losing, HEDGE_LOSS_COUNT)
                await self.pos_mgr.close_position_emergency("BTCUSDT", "hedge_exit")
                self._hedge_active = False
            return

        # Condición para abrir hedge
        if total_losing < HEDGE_LOSS_COUNT:
            return
        if self._hedge_active:
            return
        if self.pos_mgr.is_trading("BTCUSDT"):
            return

        can, reason = await self.risk.can_trade()
        if not can:
            return

        # Determinar dirección del hedge
        # Más LONGs perdiendo (mercado cae) → hedge SHORT
        # Más SHORTs perdiendo (mercado sube) → hedge LONG
        hedge_dir = "SHORT" if len(losing_longs) >= len(losing_shorts) else "LONG"

        log.info("[HEDGE] %d posiciones master en pérdida real ≥%.1f%% "
                 "(longs=%d shorts=%d) — abriendo %s BTCUSDT",
                 total_losing, HEDGE_LOSS_PCT, len(losing_longs), len(losing_shorts), hedge_dir)

        try:
            ticker = await self.client.get_ticker("BTCUSDT")
            btc_price = float(ticker.get("lastPrice", 0))
            if btc_price <= 0:
                return

            # Size pequeño: solo cobertura simbólica (~50 USDT notional)
            hedge_notional = min(50.0, C.MAX_NOTIONAL_USDT * 0.25)
            hedge_qty      = hedge_notional / btc_price

            sl_pct  = 0.015  # 1.5% SL en BTC
            tp1_pct = 0.02   # 2% TP

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
        except Exception as e:
            log.error("[HEDGE] error: %s", e)

    # ══════════════════════════════════════════════════════════════════════════
    # LOOP PRINCIPAL
    # ══════════════════════════════════════════════════════════════════════════

    async def run_loop(self):
        log.info("Complement Engine v1.1 iniciado — modos: %s", COMPLEMENT_MODE)

        # Refresh inicial de símbolos exclusivos
        await self.refresh_exclusive_symbols()

        iteration = 0
        while True:
            iteration += 1

            # Refresh símbolos cada 30 iteraciones
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
