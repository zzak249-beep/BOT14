"""
telegram_commands.py
---------------------
Escucha comandos entrantes de Telegram (/status, /pause, /resume, /close,
/help) en un hilo en segundo plano, usando long-polling sobre getUpdates.

Seguridad: solo se atienden mensajes que vengan del chat_id configurado
en TELEGRAM_CHAT_ID. Cualquier otro chat se ignora, para que nadie más
pueda controlar el bot aunque encuentre su usuario de Telegram.
"""

import logging
import threading
import time

import requests

logger = logging.getLogger("bot")


class BotControlState:
    """Estado compartido y thread-safe entre el bucle principal y el listener de comandos."""

    def __init__(self):
        self._lock = threading.Lock()
        self._paused = False
        self._snapshot = {}

    def is_paused(self) -> bool:
        with self._lock:
            return self._paused

    def set_paused(self, value: bool) -> None:
        with self._lock:
            self._paused = value

    def update_snapshot(self, **kwargs) -> None:
        with self._lock:
            self._snapshot.update(kwargs)

    def get_snapshot(self) -> dict:
        with self._lock:
            return dict(self._snapshot)


class CommandListener:
    def __init__(self, bot_token, chat_id, control_state, exchange, settings, notifier, state, save_state_fn):
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self.chat_id = str(chat_id)
        self.control = control_state
        self.exchange = exchange
        self.settings = settings
        self.notifier = notifier
        self.state = state
        self.save_state_fn = save_state_fn
        self._offset = None
        self._thread = None
        self._stop = threading.Event()

    def start(self) -> None:
        self._thread = threading.Thread(target=self._poll_loop, daemon=True, name="telegram-commands")
        self._thread.start()
        logger.info("Listener de comandos de Telegram iniciado (/status, /pause, /resume, /close, /help).")

    def stop(self) -> None:
        self._stop.set()

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            try:
                params = {"timeout": 25}
                if self._offset is not None:
                    params["offset"] = self._offset
                resp = requests.get(f"{self.base_url}/getUpdates", params=params, timeout=35)
                resp.raise_for_status()
                data = resp.json()
                for update in data.get("result", []):
                    self._offset = update["update_id"] + 1
                    self._handle_update(update)
            except Exception as exc:
                logger.warning(f"Error escuchando comandos de Telegram (se reintenta en 5s): {exc}")
                time.sleep(5)

    def _handle_update(self, update: dict) -> None:
        message = update.get("message") or update.get("edited_message")
        if not message:
            return
        chat_id = str(message.get("chat", {}).get("id", ""))
        text = (message.get("text") or "").strip()
        if chat_id != self.chat_id:
            logger.warning(f"Comando ignorado: llegó de un chat_id no autorizado ({chat_id}).")
            return
        if not text.startswith("/"):
            return

        command = text.split()[0].lower().split("@")[0]  # soporta "/status@MiBot"
        handlers = {
            "/status": self._cmd_status,
            "/pause": self._cmd_pause,
            "/resume": self._cmd_resume,
            "/close": self._cmd_close,
            "/help": self._cmd_help,
            "/start": self._cmd_help,
        }
        handler = handlers.get(command)
        if handler is None:
            self.notifier.send(f"Comando no reconocido: {command}\nUsa /help para ver la lista.")
            return
        try:
            handler()
        except Exception as exc:
            logger.exception(f"Error ejecutando el comando {command}")
            self.notifier.send(f"❌ Error ejecutando {command}:\n<code>{exc}</code>")

    def _cmd_help(self) -> None:
        self.notifier.send(
            "<b>Comandos disponibles</b>\n"
            "/status - estado actual del bot\n"
            "/pause - pausa las operaciones (sigue analizando y avisando)\n"
            "/resume - reanuda las operaciones\n"
            "/close - cierra la posición abierta ahora mismo (venta a mercado)\n"
            "/help - esta ayuda"
        )

    def _cmd_status(self) -> None:
        snap = self.control.get_snapshot()
        paused = self.control.is_paused()
        modo = "DEMO" if self.settings.bingx_demo else "REAL"
        ordenes = "DRY_RUN (sin órdenes)" if self.settings.dry_run else "ÓRDENES REALES"
        lines = [
            "📊 <b>Estado del bot</b>",
            f"Par: {self.settings.symbol} ({self.settings.timeframe})",
            f"Cuenta BingX: {modo} | Órdenes: {ordenes}",
            f"Pausado: {'sí ⏸️' if paused else 'no ▶️'}",
        ]
        if snap:
            lines.append(f"Última vela analizada: {snap.get('candle_time', '—')}")
            lines.append(f"Último precio de cierre: {snap.get('close', '—')}")
            lines.append(f"RSI: {snap.get('rsi', '—')}")
            in_pos = snap.get("in_position")
            pos_val = snap.get("position_value", 0) or 0
            lines.append(f"En posición: {'sí' if in_pos else 'no'} (~{pos_val:.2f} USDT)")
        else:
            lines.append("(todavía no se ha completado ningún ciclo de análisis)")
        self.notifier.send("\n".join(lines))

    def _cmd_pause(self) -> None:
        self.control.set_paused(True)
        self.notifier.send("⏸️ Bot en pausa: seguirá analizando y avisando, pero no enviará órdenes hasta /resume.")

    def _cmd_resume(self) -> None:
        self.control.set_paused(False)
        self.notifier.send("▶️ Bot reanudado: volverá a operar con normalidad en el próximo ciclo.")

    def _cmd_close(self) -> None:
        position_value = self.exchange.get_position_value_usdt(self.settings.symbol)
        if position_value < self.settings.min_position_value_usdt:
            self.notifier.send("No hay ninguna posición abierta que cerrar.")
            return
        if self.settings.dry_run:
            self.notifier.send(
                f"⚙️ DRY_RUN activo: en modo real esto habría vendido la posición "
                f"(~{position_value:.2f} USDT). No se ha enviado ninguna orden."
            )
            return
        order = self.exchange.market_sell_all(self.settings.symbol)
        price = order.get("average") or order.get("price") or "—"
        self.state["entry_price"] = None
        self.state["entry_time"] = None
        self.save_state_fn(self.state)
        self.notifier.send(f"🔴 Posición cerrada manualmente vía /close (precio aprox: {price}).")
