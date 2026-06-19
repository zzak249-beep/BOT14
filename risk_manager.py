"""
QF×JP Bot v7.7 — Risk Manager COMPLETO
═══════════════════════════════════════════════════════════════════════════════
NUEVO en v7.7:
  ✅ direction_allowed() — Correlation Guard (evita FHEU+XNY: 2 LONG correlados)
  ✅ on_trade_opened() acepta direction= para registrar en correlation guard
  ✅ record_result() — TradeJournal llama esto para alimentar umbral adaptativo
  ✅ min_score_effective() — MIN_SCORE + offset adaptativo del journal

SIN CAMBIOS vs v7.1:
  ✅ daily_loss_limit incluye PnL no realizado (unrealized_pnl)
  ✅ Notional cap duro MAX_NOTIONAL_USDT
  ✅ Cooldown 2h por símbolo tras pérdida
  ✅ open_count sincronizado solo desde BingX real (solo posiciones propias)
═══════════════════════════════════════════════════════════════════════════════
"""
import asyncio
import logging
import time
import math
from datetime import date

import config as C
from volatility_regime import vol_engine

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

        FIX v7.8 — RACE CONDITION CRÍTICA: el scanner procesa hasta 20
        símbolos en paralelo (asyncio.gather). Antes, este método solo
        CHEQUEABA open_count < MAX_OPEN_TRADES, y el incremento real
        ocurría mucho después (en on_trade_opened(), tras el round-trip
        de red a BingX). Eso dejaba una ventana en la que 5+ símbolos
        concurrentes podían ver TODOS "open_count=3<5", pasar el check
        a la vez, y abrir 5+ trades de golpe — superando el límite
        configurado (caso real: "Trades abiertos: 6/5" en producción).

        FIX: si todos los chequeos pasan, el slot se RESERVA de inmediato
        (incrementando open_count/daily_trades) DENTRO del mismo lock,
        antes de devolver True. Así, la siguiente llamada concurrente ve
        el contador ya actualizado y se bloquea correctamente.

        Si el trade finalmente NO se concreta (entrada rechazada, qty=0,
        excepción, etc.), el caller DEBE llamar a release_reservation()
        para liberar el slot reservado — si no, el contador queda
        inflado y bloquea trades válidos el resto del día.

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

            # FIX: reserva atómica — incrementar AQUÍ, dentro del lock,
            # antes de soltar el control a otra corrutina concurrente.
            self._open_count   += 1
            self._daily_trades += 1
            return True, ""

    async def release_reservation(self):
        """
        Libera un slot reservado por can_trade() cuando el trade
        finalmente NO se concreta. Llamar SIEMPRE que can_trade()
        devolvió True pero el flujo termina sin abrir la posición real.
        """
        async with self._lock:
            self._open_count   = max(0, self._open_count - 1)
            self._daily_trades = max(0, self._daily_trades - 1)
            log.debug("Reserva liberada (trade no concretado) — open=%d daily=%d",
                     self._open_count, self._daily_trades)

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

        FIX v7.8: misma race condition que can_trade() — antes el chequeo
        y el registro (en on_trade_opened) estaban separados por el
        round-trip de red, permitiendo que el batch concurrente de hasta
        20 símbolos abriera varios LONG/SHORT "correlacionados" a la vez,
        justo el escenario que este guard debía prevenir. Ahora reserva
        el timestamp INMEDIATAMENTE si pasa el check.

        Si el trade finalmente no se concreta, llamar a
        release_direction_reservation(direction) para liberar el cupo.
        """
        now = time.time()
        ts_list = self._direction_ts.get(direction, [])
        # Purgar timestamps fuera de la ventana
        ts_list = [t for t in ts_list if now - t < C.CORRELATION_WINDOW_SEC]
        if len(ts_list) >= C.MAX_SAME_DIRECTION:
            self._direction_ts[direction] = ts_list
            mins = int(C.CORRELATION_WINDOW_SEC / 60)
            return False, f"correlation_guard({direction},{len(ts_list)}/{C.MAX_SAME_DIRECTION} en {mins}min)"
        # FIX: reservar de inmediato — no esperar a on_trade_opened()
        ts_list.append(now)
        self._direction_ts[direction] = ts_list
        return True, ""

    def release_direction_reservation(self, direction: str):
        """Libera una reserva de dirección cuando el trade no se concreta."""
        ts_list = self._direction_ts.get(direction, [])
        if ts_list:
            ts_list.pop()  # quita la más reciente (la que reservamos ahora)
            self._direction_ts[direction] = ts_list

    def tier_ok(self, tier: str) -> bool:
        order = {"NONE": 0, "STD": 1, "FUEL": 2, "SUP": 3}
        return order.get(tier, 0) >= order.get(C.MIN_TIER, 1)

    # ── Eventos ───────────────────────────────────────────────────────────────

    async def on_trade_opened(self, symbol: str = "", direction: str = ""):
        """
        Llamar cuando el trade se CONFIRMA realmente abierto en BingX.

        FIX v7.8: open_count, daily_trades y direction_ts YA se reservaron
        atómicamente en can_trade()/direction_allowed() cuando pasaron el
        check — incrementarlos otra vez aquí los duplicaría. Esta función
        ahora solo registra el cooldown por símbolo (symbol_trade_cnt),
        que no tiene el mismo riesgo de carrera porque cada símbolo se
        procesa una sola vez por ciclo de scan.
        """
        async with self._lock:
            if symbol:
                self._symbol_trade_cnt[symbol] = self._symbol_trade_cnt.get(symbol, 0) + 1
            log.info("Trade confirmado — open=%d daily=%d symbol=%s dir=%s",
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
                             sl: float, score: float, tier: str,
                             symbol: str = "") -> float:
        if entry <= 0 or sl <= 0 or abs(entry - sl) < 1e-12:
            return 0.0

        w = C.KELLY_WIN_RATE
        r = C.KELLY_RR
        kelly = max(0.0, (w * r - (1 - w)) / r) * C.KELLY_FRACTION
        tier_mult = {"STD": 1.0, "FUEL": 1.2, "SUP": 1.5, "HARVEST": 0.5}.get(tier, 1.0)
        kelly *= tier_mult

        risk_usdt = balance * (C.RISK_PCT / 100) * kelly
        sl_dist   = abs(entry - sl)

        # ── FIX CRÍTICO: bug dimensional de sizing ────────────────────────────
        # Fórmula anterior: qty = (risk_usdt * LEVERAGE) / (sl_dist * entry)
        # Esto dividía por `entry` dentro Y por `LEVERAGE/entry` de forma
        # implícita, haciendo que la pérdida REAL al tocar SL dependiera del
        # precio del token — verificado con números reales: para el MISMO
        # risk_usdt configurado, SUI-USDT (precio ~0.71) arriesgaba 17.8x más
        # dólares reales que LAB-USDT (precio ~12.6) al tocar el stop loss.
        # Por eso LAB abría con qty=0.3 (notional ~3.8 USDT, margen <1 USDT)
        # mientras símbolos baratos abrían posiciones proporcionalmente
        # enormes para el mismo "riesgo" nominal.
        #
        # FIX: sizing por riesgo correcto y estándar — qty = risk_usdt / sl_dist.
        # Esto garantiza que CUALQUIER símbolo, sin importar su precio en
        # USDT, pierda exactamente risk_usdt si toca el SL. El leverage NO
        # debe multiplicar la qty (solo determina cuánto margen hace falta
        # para abrir esa qty, vía notional/leverage — ver cap más abajo).
        qty = risk_usdt / sl_dist if sl_dist > 0 else 0.0

        # ── VOLATILITY TARGETING ─────────────────────────────────────────────
        # Ajusta el size de forma inversa al régimen de volatilidad del símbolo:
        # COMPRESSED (ATR% bajo relativo a su historia) → +15% size
        # EXPANDED   (ATR% alto)                         → -30% size
        # EXTREME    (ATR% muy alto, posible cascada)    → -60% size
        vol_mult = 1.0
        if symbol and getattr(C, 'VOL_REGIME_ENABLED', True):
            vol_sig  = vol_engine.get_signal(symbol)
            vol_mult = vol_sig.size_mult
            if vol_mult != 1.0:
                log.info("[sizing] %s vol_regime=%s mult=%.2f",
                         symbol, vol_sig.regime, vol_mult)
        qty *= vol_mult

        # ── CAP DURO ANTI-LIQUIDACIÓN ─────────────────────────────────────────
        # ILV -43%, ADA -52%, PI -35% → posiciones demasiado grandes
        notional = qty * entry
        cap = C.MAX_NOTIONAL_USDT
        if notional > cap:
            log.info("[sizing] %s notional %.0f→%.0f USDT (cap=%.0f)",
                     tier, notional, cap, cap)
            qty = cap / entry
            notional = cap

        # ── PISO MÍNIMO DE NOTIONAL ────────────────────────────────────────────
        # Con la fórmula corregida, símbolos caros + riesgo conservador pueden
        # seguir dando posiciones diminutas donde las comisiones (0.02-0.05%
        # por lado) se comen una fracción enorme del edge real. Si el notional
        # calculado no llega al mínimo configurado, NO merece la pena abrir
        # — mejor saltarse la señal que pagar fees por una posición simbólica.
        min_notional = getattr(C, 'MIN_NOTIONAL_USDT', 5.0)
        if notional < min_notional:
            log.info("[sizing] %s notional %.2f < mínimo %.2f USDT — skip (fees dominarían)",
                     tier, notional, min_notional)
            return 0.0

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
