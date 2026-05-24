"""
QF Machine × JP Fusion Bot — Orquestador Principal
Loop de trading: datos → señal → riesgo → orden → gestión → Telegram
Escáner multi-símbolo: analiza todos los pares USDT de BingX
"""
import asyncio
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from exchange     import BingXClient
from signals      import QFSignalEngine
from risk         import RiskManager
from positions    import Position, PositionTracker
from telegram_bot import TelegramNotifier
from config       import SIGNAL_CFG, RISK_CFG, SYMBOLS, HTF

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler("logs/bot.log"),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger("QFBot")

BOT_STATE = {
    "paused":  False,
    "paper":   os.getenv("PAPER_MODE", "true").lower() != "false",
    "symbols": [],   # se rellena en run()
}

# Cuántos símbolos procesar en paralelo (evita rate limit)
CONCURRENCY = int(os.getenv("SCAN_CONCURRENCY", "10"))

# Volumen mínimo 24h en USDT para incluir un par (default 5M)
MIN_VOL_USDT = float(os.getenv("MIN_VOL_USDT", "5000000"))

# Cada cuántos ticks refresca la lista de símbolos (~cada 10 min)
SYMBOL_REFRESH_TICKS = int(os.getenv("SYMBOL_REFRESH_TICKS", "20"))


class QFBot:
    def __init__(self):
        paper = BOT_STATE["paper"]

        self.exchange = BingXClient(
            api_key=os.environ["BINGX_API_KEY"],
            secret=os.environ["BINGX_SECRET"],
            paper=paper,
        )
        self.signal_engine = QFSignalEngine(SIGNAL_CFG)
        self.risk      = RiskManager(RISK_CFG)
        self.positions = PositionTracker()
        self.tg        = TelegramNotifier(
            token=os.environ["TELEGRAM_TOKEN"],
            chat_id=os.environ["TELEGRAM_CHAT_ID"],
            risk_manager=self.risk,
            bot_state=BOT_STATE,
        )
        self._tick_count = 0

        mode = "📋 PAPER MODE" if paper else "💵 LIVE MODE"
        logger.info(f"QF Bot iniciado — {mode}")

    # ──────────────────────────────────────────────────────────
    #  LOOP PRINCIPAL
    # ──────────────────────────────────────────────────────────
    async def run(self):
        await self.tg.start_polling()

        # Carga inicial de símbolos
        await self._refresh_symbols()

        mode = "📋 PAPER MODE" if BOT_STATE["paper"] else "💵 LIVE MODE"
        n    = len(BOT_STATE["symbols"])
        await self.tg._send(
            f"🤖 *QF Machine Bot v3 arrancado*\n"
            f"Modo: *{mode}*\n"
            f"🔍 Escaneando *{n} pares* USDT (vol ≥ {MIN_VOL_USDT/1e6:.0f}M)\n"
            f"Temporalidad: `3m`  HTF: `{HTF}`\n"
            f"Leverage: `{RISK_CFG['leverage']}x`  "
            f"Concurrencia: `{CONCURRENCY}`\n\n"
            f"{'⚠️ OPERANDO CON DINERO REAL' if not BOT_STATE['paper'] else '✅ Sin riesgo real — paper trading'}"
        )

        try:
            while True:
                await self._tick()
                await asyncio.sleep(int(os.getenv("LOOP_SECONDS", "30")))
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.exception(f"Error crítico en loop: {e}")
            await self.tg.send_alert(f"❌ Error crítico: {e}\nBot detenido.")
        finally:
            await self.tg.stop_polling()
            await self.exchange.close()

    # ──────────────────────────────────────────────────────────
    #  REFRESH LISTA DE SÍMBOLOS
    # ──────────────────────────────────────────────────────────
    async def _refresh_symbols(self):
        """
        Obtiene todos los pares USDT de BingX con volumen suficiente.
        Si falla, usa la lista estática de config.py como fallback.
        """
        try:
            syms = await self.exchange.get_all_symbols(min_volume_usdt=MIN_VOL_USDT)
            if syms:
                BOT_STATE["symbols"] = syms
                logger.info(f"📋 Lista actualizada: {len(syms)} símbolos")
            else:
                BOT_STATE["symbols"] = SYMBOLS
                logger.warning("Lista vacía, usando SYMBOLS de config.py")
        except Exception as e:
            logger.error(f"Error refrescando símbolos: {e}")
            if not BOT_STATE["symbols"]:
                BOT_STATE["symbols"] = SYMBOLS

    # ──────────────────────────────────────────────────────────
    #  TICK
    # ──────────────────────────────────────────────────────────
    async def _tick(self):
        if BOT_STATE.get("paused"):
            return

        self._tick_count += 1

        # Refresca lista de símbolos periódicamente
        if self._tick_count % SYMBOL_REFRESH_TICKS == 0:
            await self._refresh_symbols()

        symbols = list(BOT_STATE["symbols"])
        if not symbols:
            return

        # Semáforo para limitar peticiones simultáneas al exchange
        sem = asyncio.Semaphore(CONCURRENCY)

        async def _guarded(sym):
            async with sem:
                try:
                    await self._process_symbol(sym)
                except Exception as e:
                    logger.error(f"Error en {sym}: {e}")

        await asyncio.gather(*[_guarded(s) for s in symbols])

    # ──────────────────────────────────────────────────────────
    #  PROCESAR SÍMBOLO
    # ──────────────────────────────────────────────────────────
    async def _process_symbol(self, symbol: str):
        # 1. Datos
        df_3m  = await self.exchange.get_klines(symbol, "3m",  limit=250)
        df_htf = await self.exchange.get_klines(symbol, HTF,   limit=100)

        if len(df_3m) < 100:
            return

        current_price = float(df_3m['close'].iloc[-1])

        # 2. Gestionar posición abierta
        if self.positions.has(symbol):
            await self._manage_open_position(symbol, current_price, df_3m)
            return

        # 3. Señal
        signal = self.signal_engine.compute(df_3m, df_htf)

        if signal.direction == "FLAT" or signal.tier == "NONE":
            return

        # 4. Convicción mínima
        min_conv = int(os.getenv("MIN_CONVICTION", "6"))
        if signal.conviction < min_conv:
            return

        # 5. Tamaño
        qty = self.risk.calc_position_size(
            entry=signal.entry_price,
            sl=signal.sl_price,
            tier=signal.tier,
            conviction=signal.conviction,
        )
        if qty is None or qty <= 0:
            return

        tp = self.risk.calc_tp(signal.entry_price, signal.sl_price,
                               signal.direction, signal.tier)

        # 6. Orden
        await self.exchange.set_leverage(symbol, RISK_CFG['leverage'])
        side          = "BUY" if signal.direction == "LONG" else "SELL"
        position_side = signal.direction

        order = await self.exchange.place_order(symbol, side, position_side, qty)
        await self.exchange.set_sl_tp(symbol, position_side, signal.sl_price, tp, qty)

        # 7. Registrar
        pos = Position(
            symbol=symbol,
            direction=signal.direction,
            tier=signal.tier,
            entry=signal.entry_price,
            sl=signal.sl_price,
            tp=tp,
            qty=qty,
            open_time=datetime.utcnow().isoformat(),
            order_id=str(order.get("orderId", "?")),
            paper=BOT_STATE["paper"],
            trailing_sl=signal.sl_price,
        )
        self.positions.open(pos)

        # 8. Notificar
        await self.tg.send_signal(signal, symbol, qty, tp, BOT_STATE["paper"])

        logger.info(
            f"✅ ORDEN {signal.direction} {symbol} "
            f"tier={signal.tier} conv={signal.conviction}/10 "
            f"qty={qty} entry={signal.entry_price:.6f} "
            f"sl={signal.sl_price:.6f} tp={tp:.6f}"
        )

    # ──────────────────────────────────────────────────────────
    #  GESTIÓN POSICIÓN ABIERTA
    # ──────────────────────────────────────────────────────────
    async def _manage_open_position(self, symbol: str, price: float, df: pd.DataFrame):
        pos = self.positions.get(symbol)
        if not pos:
            return

        atr = float(df['high'].sub(df['low']).rolling(10).mean().iloc[-1])

        new_sl = self.positions.calc_trailing_sl(
            pos, price, atr,
            trail_atr_mult=float(os.getenv("TRAIL_ATR", "1.5"))
        )
        if new_sl != pos.trailing_sl:
            self.positions.update_trailing_sl(symbol, new_sl)

        exit_reason = self.positions.check_exit(symbol, price)
        if not exit_reason:
            return

        pnl = self.positions.calc_pnl(symbol, price)

        if not BOT_STATE["paper"]:
            await self.exchange.close_position(symbol, pos.direction, pos.qty)

        self.risk.record_trade(pnl)

        reason_txt = {
            "stop_loss":   "Stop Loss",
            "take_profit": "Take Profit ✨",
        }.get(exit_reason, exit_reason)

        await self.tg.send_trade_close(
            symbol=symbol,
            direction=pos.direction,
            pnl=pnl,
            entry=pos.entry,
            exit_price=price,
            reason=reason_txt,
            paper=BOT_STATE["paper"],
        )

        self.positions.close(symbol)

        can, reason = self.risk.check_circuit()
        if not can:
            await self.tg.send_circuit_breaker(reason)


# ──────────────────────────────────────────────────────────────
#  ENTRY POINT
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    bot = QFBot()
    asyncio.run(bot.run())
