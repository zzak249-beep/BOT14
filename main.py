"""
QF Machine × JP Fusion Bot — Orquestador Principal
Loop de trading: datos → señal → riesgo → orden → gestión → Telegram
"""
import asyncio
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from exchange    import BingXClient
from signals     import QFSignalEngine
from risk        import RiskManager
from positions   import Position, PositionTracker
from telegram_bot import TelegramNotifier
from config      import SIGNAL_CFG, RISK_CFG, SYMBOLS, HTF

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler("logs/bot.log"),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger("QFBot")

# ─────────────────────────────────────────────────────────────
#  Estado global compartido con Telegram
# ─────────────────────────────────────────────────────────────
BOT_STATE = {
    "paused": False,
    "paper":  os.getenv("PAPER_MODE", "true").lower() != "false",
}


class QFBot:
    def __init__(self):
        paper = BOT_STATE["paper"]

        self.exchange  = BingXClient(
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

        mode = "📋 PAPER MODE" if paper else "💵 LIVE MODE"
        logger.info(f"QF Bot iniciado — {mode}")

    # ─────────────────────────────────────────────────────────
    #  LOOP PRINCIPAL
    # ─────────────────────────────────────────────────────────
    async def run(self):
        await self.tg.start_polling()

        mode = "📋 PAPER MODE" if BOT_STATE["paper"] else "💵 LIVE MODE"
        await self.tg._send(
            f"🤖 *QF Machine Bot v3 arrancado*\n"
            f"Modo: *{mode}*\n"
            f"Símbolos: `{'  '.join(SYMBOLS)}`\n"
            f"Temporalidad: `3m`  HTF: `{HTF}`\n"
            f"Leverage: `{RISK_CFG['leverage']}x`\n\n"
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

    # ─────────────────────────────────────────────────────────
    #  TICK — ejecutado cada N segundos
    # ─────────────────────────────────────────────────────────
    async def _tick(self):
        if BOT_STATE.get("paused"):
            return

        for symbol in SYMBOLS:
            try:
                await self._process_symbol(symbol)
            except Exception as e:
                logger.error(f"Error en {symbol}: {e}")

    async def _process_symbol(self, symbol: str):
        # ── 1. Obtener datos ──────────────────────────────────
        df_3m  = await self.exchange.get_klines(symbol, "3m",  limit=250)
        df_htf = await self.exchange.get_klines(symbol, HTF,   limit=100)

        if len(df_3m) < 100:
            return

        current_price = float(df_3m['close'].iloc[-1])

        # ── 2. Gestionar posición abierta ─────────────────────
        if self.positions.has(symbol):
            await self._manage_open_position(symbol, current_price, df_3m)
            return   # no buscar nuevas señales si ya hay posición

        # ── 3. Calcular señal ─────────────────────────────────
        signal = self.signal_engine.compute(df_3m, df_htf)

        if signal.direction == "FLAT" or signal.tier == "NONE":
            return

        # ── 4. Filtro de convicción mínima ────────────────────
        min_conv = int(os.getenv("MIN_CONVICTION", "6"))
        if signal.conviction < min_conv:
            logger.debug(f"{symbol}: señal {signal.tier} convicción {signal.conviction} < {min_conv}, skip")
            return

        # ── 5. Calcular tamaño ────────────────────────────────
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

        # ── 6. Ejecutar orden ─────────────────────────────────
        await self.exchange.set_leverage(symbol, RISK_CFG['leverage'])

        side          = "BUY"  if signal.direction == "LONG"  else "SELL"
        position_side = signal.direction  # "LONG" | "SHORT"

        order = await self.exchange.place_order(symbol, side, position_side, qty)

        # SL/TP en exchange (en live)
        await self.exchange.set_sl_tp(symbol, position_side, signal.sl_price, tp, qty)

        # ── 7. Registrar posición ─────────────────────────────
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

        # ── 8. Notificar ──────────────────────────────────────
        await self.tg.send_signal(signal, symbol, qty, tp, BOT_STATE["paper"])

        logger.info(
            f"✅ ORDEN {signal.direction} {symbol} "
            f"tier={signal.tier} conv={signal.conviction}/10 "
            f"qty={qty} entry={signal.entry_price:.6f} "
            f"sl={signal.sl_price:.6f} tp={tp:.6f}"
        )

    # ─────────────────────────────────────────────────────────
    #  GESTIÓN DE POSICIÓN ABIERTA
    # ─────────────────────────────────────────────────────────
    async def _manage_open_position(self, symbol: str, price: float, df: pd.DataFrame):
        pos = self.positions.get(symbol)
        if not pos:
            return

        atr = float(df['high'].sub(df['low']).rolling(10).mean().iloc[-1])

        # Trailing stop
        new_sl = self.positions.calc_trailing_sl(pos, price, atr,
                                                  trail_atr_mult=float(os.getenv("TRAIL_ATR", "1.5")))
        if new_sl != pos.trailing_sl:
            self.positions.update_trailing_sl(symbol, new_sl)
            logger.debug(f"Trailing SL {symbol}: {pos.trailing_sl:.6f} → {new_sl:.6f}")

        # Comprobar salida
        exit_reason = self.positions.check_exit(symbol, price)
        if not exit_reason:
            return

        # Cerrar
        pnl = self.positions.calc_pnl(symbol, price)

        # En live cerramos en exchange; en paper es inmediato
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

        # Circuit breaker check después de registrar pérdida
        can, reason = self.risk.check_circuit()
        if not can:
            await self.tg.send_circuit_breaker(reason)


# ─────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    bot = QFBot()
    asyncio.run(bot.run())
