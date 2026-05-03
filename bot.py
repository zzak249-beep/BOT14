"""
╔══════════════════════════════════════════════════════════════════╗
║          PHANTOM EDGE BOT — v3                                   ║
║   ZigZag++ 15m · HMA · Volume Delta · ATR · BingX Futures       ║
║                                                                  ║
║   Señal:  Ruptura de pivot con confirmación HMA + Volume Delta   ║
║   TP/SL:  Dinámicos basados en ATR                               ║
║   Filtros: Volumen 24h · ATR % · Cooldown · R/R mínimo          ║
╚══════════════════════════════════════════════════════════════════╝
"""

import asyncio
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from bingx   import BingXClient
from strategy import signal, qty_by_risk, risk_reward
import telegram as tg

# ─────────────────────────────────────────────────────────────
#  CONFIGURACIÓN — 100% desde variables de entorno
# ─────────────────────────────────────────────────────────────
API_KEY    = os.getenv("BINGX_API_KEY",    "")
API_SECRET = os.getenv("BINGX_API_SECRET", "")
TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT    = os.getenv("TELEGRAM_CHAT_ID",   "")

AUTO_TRADE   = os.getenv("AUTO_TRADING_ENABLED", "false").lower() == "true"
LEVERAGE     = int(os.getenv("LEVERAGE",             "10"))
TRADE_USDT   = float(os.getenv("TRADE_AMOUNT_USDT", "10"))
MAX_POS      = int(os.getenv("MAX_OPEN_TRADES",       "5"))
SCAN_SEC     = int(os.getenv("SCAN_INTERVAL_SECONDS", "60"))
BATCH_SIZE   = int(os.getenv("SCAN_BATCH_SIZE",       "10"))
COOLDOWN_SEC = int(os.getenv("COOLDOWN_SECONDS",      "300"))
MIN_VOL_24H  = float(os.getenv("MIN_VOLUME_USDT",  "1000000"))
PORT         = int(os.getenv("PORT", "8080"))
TIMEFRAME    = "15m"   # fijo según estrategia

# ─────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-5s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("PhantomEdge")


# ══════════════════════════════════════════════════════════════
#  BOT PRINCIPAL
# ══════════════════════════════════════════════════════════════
class PhantomEdgeBot:
    def __init__(self):
        self.bx          = BingXClient(API_KEY, API_SECRET)
        self.symbols:    list[str]        = []
        self.candles:    dict[str, list]  = {}
        self.warm:       set[str]         = set()
        self.open_pos:   dict[str, dict]  = {}
        self.cooldown:   dict[str, float] = defaultdict(float)
        self.balance:    float            = 0.0
        self.cycle:      int              = 0
        self.start_t:    float            = time.time()

        # Estadísticas diarias
        self.daily_trades = 0
        self.daily_wins   = 0
        self.daily_losses = 0
        self.daily_pnl    = 0.0
        self.daily_best:  Optional[tuple[str, float]] = None
        self.daily_worst: Optional[tuple[str, float]] = None
        self.last_day     = datetime.now(timezone.utc).date()

        # Sesión Telegram dedicada
        self._tg_sess: Optional[aiohttp.ClientSession] = None

    @property
    def tg_sess(self) -> aiohttp.ClientSession:
        if self._tg_sess is None or self._tg_sess.closed:
            self._tg_sess = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            )
        return self._tg_sess

    # ── Notificaciones ────────────────────────────────────────
    async def notify(self, msg: str):
        await tg.send(TG_TOKEN, TG_CHAT, msg)

    # ── Warm-up ───────────────────────────────────────────────
    async def _warmup_sym(self, sym: str) -> bool:
        try:
            klines = await self.bx.klines(sym, TIMEFRAME, 150)
            if len(klines) >= 100:
                self.candles[sym] = klines
                self.warm.add(sym)
                return True
        except Exception as e:
            log.debug(f"warmup {sym}: {e}")
        return False

    async def _warmup_all(self):
        syms = self.symbols
        done = 0
        log.info(f"🔥 WarmUp de {len(syms)} pares en {TIMEFRAME}...")
        for i in range(0, len(syms), BATCH_SIZE):
            batch = syms[i: i + BATCH_SIZE]
            res = await asyncio.gather(
                *[self._warmup_sym(s) for s in batch],
                return_exceptions=True,
            )
            done += sum(1 for r in res if r is True)
            log.info(f"  WarmUp {done}/{len(syms)}")
            await asyncio.sleep(0.3)
        log.info(f"✅ {done} pares calientes — iniciando ciclos\n")

    # ── Actualización incremental de velas ────────────────────
    async def _update_sym(self, sym: str):
        try:
            new = await self.bx.klines(sym, TIMEFRAME, 3)
        except Exception:
            return
        existing = self.candles.get(sym, [])
        last_t   = existing[-1]["t"] if existing else 0
        for k in new:
            if k["t"] > last_t:
                existing.append(k)
            elif k["t"] == last_t and existing:
                existing[-1] = k       # actualiza vela en formación
        self.candles[sym] = existing[-150:]

    # ── Control diario ────────────────────────────────────────
    async def _check_daily_reset(self):
        today = datetime.now(timezone.utc).date()
        if today == self.last_day:
            return
        # Resumen del día anterior
        await self.notify(tg.msg_daily_summary({
            "trades":    self.daily_trades,
            "wins":      self.daily_wins,
            "losses":    self.daily_losses,
            "total_pnl": self.daily_pnl,
            "best":      self.daily_best,
            "worst":     self.daily_worst,
            "balance":   self.balance,
        }))
        self.daily_trades = self.daily_wins = self.daily_losses = 0
        self.daily_pnl = 0.0
        self.daily_best = self.daily_worst = None
        self.last_day = today
        log.info("📅 Estadísticas diarias reseteadas")

    def _record_trade(self, pnl: float, symbol: str):
        self.daily_trades += 1
        self.daily_pnl    += pnl
        if pnl > 0:
            self.daily_wins += 1
            if self.daily_best is None or pnl > self.daily_best[1]:
                self.daily_best = (symbol, pnl)
        elif pnl < 0:
            self.daily_losses += 1
            if self.daily_worst is None or pnl < self.daily_worst[1]:
                self.daily_worst = (symbol, pnl)

    # ── Ciclo de escaneo ──────────────────────────────────────
    async def scan(self):
        self.cycle += 1
        await self._check_daily_reset()

        # Estado actual: balance + posiciones en paralelo
        self.balance, pos_raw = await asyncio.gather(
            self.bx.balance_usdt(),
            self.bx.open_positions(),
        )
        # Renovar caché de contratos si es necesario
        await self.bx.load_contracts()

        self.open_pos = {p["symbol"]: p for p in pos_raw}
        now = time.time()

        log.info(
            f"[C{self.cycle:04d}] Bal:{self.balance:.2f}U │ "
            f"Pos:{len(self.open_pos)}/{MAX_POS} │ "
            f"Warm:{len(self.warm)}/{len(self.symbols)} │ "
            f"{'AUTO ✅' if AUTO_TRADE else 'SIM 🟡'}"
        )

        # Guard: máximo de posiciones
        if len(self.open_pos) >= MAX_POS:
            log.info(f"  ⛔ MAX_POSITIONS={MAX_POS}")
            return

        # Candidatos: calientes, sin posición abierta, sin cooldown
        candidates = [
            s for s in self.warm
            if s not in self.open_pos
            and now - self.cooldown[s] > COOLDOWN_SEC
        ]

        if not candidates:
            log.info("  ℹ️  Sin candidatos disponibles en este ciclo")
            return

        # Actualizar velas en lotes paralelos
        for i in range(0, len(candidates), BATCH_SIZE):
            await asyncio.gather(
                *[self._update_sym(s) for s in candidates[i: i + BATCH_SIZE]],
                return_exceptions=True,
            )

        signals_found = 0

        for sym in candidates:
            if len(self.open_pos) >= MAX_POS:
                break

            klines = self.candles.get(sym, [])
            sig    = signal(klines)
            if not sig:
                continue

            signals_found += 1
            self.cooldown[sym] = now
            entry = sig["entry"]
            side  = sig["side"]
            direction = "LONG" if side == "BUY" else "SHORT"
            side_emoji = "🟢" if side == "BUY" else "🔴"

            tp_pct = abs(entry - sig["tp"]) / entry * 100
            sl_pct = abs(entry - sig["sl"]) / entry * 100

            log.info(
                f"  {side_emoji} {sym} {direction} │ "
                f"E:{entry:.4f} TP:{sig['tp']:.4f}(+{tp_pct:.2f}%) "
                f"SL:{sig['sl']:.4f}(-{sl_pct:.2f}%) │ "
                f"RR:{sig['rr']} │ {' · '.join(sig['reasons'])}"
            )

            if AUTO_TRADE:
                await self._execute_trade(sym, sig)
            else:
                await self.notify(tg.msg_sim_signal(
                    symbol=sym, side=side, entry=entry,
                    tp=sig["tp"], sl=sig["sl"], rr=sig["rr"],
                    leverage=LEVERAGE, reasons=sig["reasons"],
                ))

        log.info(f"  📊 Candidatos:{len(candidates)} │ Señales:{signals_found}")

    async def _execute_trade(self, sym: str, sig: dict):
        """Ejecuta la orden en BingX y notifica."""
        entry = sig["entry"]
        side  = sig["side"]
        direction = "LONG" if side == "BUY" else "SHORT"
        side_emoji = "🟢" if side == "BUY" else "🔴"

        # Calcular qty según riesgo fijo
        step = self.bx.step_size(sym)
        qty  = qty_by_risk(
            entry=entry,
            sl=sig["sl"],
            risk_usdt=TRADE_USDT,
            leverage=LEVERAGE,
            step=step,
        )
        if qty <= 0:
            log.warning(f"  ⚠️ {sym}: qty inválida, saltando")
            return

        try:
            order_id, qty_exec, entry_real = await self.bx.place_order(
                symbol=sym, side=side, qty=qty,
                tp_price=sig["tp"], sl_price=sig["sl"], leverage=LEVERAGE,
            )
        except Exception as e:
            log.error(f"  ❌ {sym}: orden fallida — {e}")
            await self.notify(tg.msg_error(str(e), ctx=f"{sym} {direction}"))
            return

        self.open_pos[sym] = {
            "symbol": sym, "side": direction,
            "entry": entry_real, "qty": qty_exec,
            "usdt": TRADE_USDT, "order_id": order_id,
        }
        self.daily_trades += 1

        await self.notify(tg.msg_trade_open(
            symbol=sym,   side=side,  entry=entry_real,
            tp=sig["tp"], sl=sig["sl"], qty=qty_exec,
            usdt=TRADE_USDT, leverage=LEVERAGE, rr=sig["rr"],
            atr_pct=sig.get("atr_pct"), reasons=sig["reasons"],
        ))

    # ── Loop principal ────────────────────────────────────────
    async def run(self):
        log.info("═" * 60)
        log.info("  PHANTOM EDGE BOT v3")
        log.info(f"  Estrategia: ZigZag++ {TIMEFRAME} | Lev:x{LEVERAGE}")
        log.info(f"  {'AUTO-TRADE' if AUTO_TRADE else 'SIMULACIÓN'} | "
                 f"MaxPos:{MAX_POS} | USDT/trade:{TRADE_USDT}")
        log.info("═" * 60)

        # Balance inicial
        self.balance = await self.bx.balance_usdt()
        log.info(f"💰 Balance: {self.balance:.2f} USDT")

        if self.balance == 0.0 and API_KEY:
            log.warning("⚠️  Balance=0. Verifica BINGX_API_KEY y permisos Futures.")

        # Cargar contratos
        await self.bx.load_contracts(force=True)

        # Filtrar símbolos por volumen 24h
        log.info("📡 Obteniendo lista de símbolos...")
        all_syms = await self.bx.all_usdt_symbols()
        log.info(f"   Total disponibles: {len(all_syms)}")

        filtered: list[str] = []
        for sym in all_syms[:300]:  # top 300 por volumen
            filtered.append(sym)   # ya vienen ordenados por quoteVolume
            # Nota: all_usdt_symbols ya devuelve ordenados por volumen 24h
            # Para un filtro estricto usa ticker_24h aquí si lo necesitas

        self.symbols = filtered
        log.info(f"✅ {len(self.symbols)} pares seleccionados")

        if not self.symbols:
            log.error("❌ Sin símbolos. Revisa la conexión o MIN_VOLUME_USDT.")
            return

        # Notificar inicio
        await self.notify(tg.msg_bot_start(
            balance=self.balance,
            total_pairs=len(self.symbols),
            trade_usdt=TRADE_USDT,
            leverage=LEVERAGE,
            max_trades=MAX_POS,
            timeframe=TIMEFRAME,
            auto_trade=AUTO_TRADE,
        ))

        # Warm-up inicial
        await self._warmup_all()

        # Loop de trading
        while True:
            try:
                t0 = time.time()
                await self.scan()
                elapsed = time.time() - t0
                sleep   = max(5.0, SCAN_SEC - elapsed)
                log.info(f"  ⏱ {elapsed:.1f}s │ próximo en {sleep:.0f}s\n")
                await asyncio.sleep(sleep)

            except asyncio.CancelledError:
                log.info("Bot cancelado.")
                break
            except Exception as e:
                log.error(f"❌ Error en ciclo: {e}", exc_info=True)
                await self.notify(tg.msg_error(str(e), ctx=f"Ciclo #{self.cycle}"))
                await asyncio.sleep(30)

        # Cierre limpio
        await self.bx.close()
        if self._tg_sess and not self._tg_sess.closed:
            await self._tg_sess.close()
        log.info("Bot detenido correctamente.")


# ══════════════════════════════════════════════════════════════
#  HEALTH CHECK (Railway necesita /health para monitorear)
# ══════════════════════════════════════════════════════════════
async def health_server(bot: PhantomEdgeBot):
    from aiohttp import web

    async def handle(_req: web.Request) -> web.Response:
        uptime = round((time.time() - bot.start_t) / 60, 1)
        return web.json_response({
            "status":          "running",
            "uptime_min":      uptime,
            "cycle":           bot.cycle,
            "balance_usdt":    round(bot.balance, 2),
            "warm_symbols":    len(bot.warm),
            "total_symbols":   len(bot.symbols),
            "open_positions":  len(bot.open_pos),
            "daily_trades":    bot.daily_trades,
            "daily_pnl":       round(bot.daily_pnl, 2),
            "auto_trade":      AUTO_TRADE,
            "strategy":        f"ZigZag++ {TIMEFRAME}",
            "leverage":        LEVERAGE,
        })

    app = web.Application()
    app.router.add_get("/",       handle)
    app.router.add_get("/health", handle)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info(f"🌐 Health → http://0.0.0.0:{PORT}/health")


# ══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════
async def main():
    bot = PhantomEdgeBot()
    await asyncio.gather(
        health_server(bot),
        bot.run(),
    )


if __name__ == "__main__":
    asyncio.run(main())
