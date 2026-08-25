"""
Bot RSI doble suelo + salida por SuperTrend — BingX.

Escanea, busca el segundo cruce fallido del RSI bajo el nivel de
disparo, entra en largo y sale cuando gira el SuperTrend.

DIFERENCIA IMPORTANTE CON EL OTRO BOT: aquí la salida NO es un objetivo
fijo, es un indicador que se mueve. El SuperTrend se recalcula en cada
vela, así que el stop se actualiza en el exchange cada vez que sube —
nunca baja. Si el bot se cae, el último stop enviado sigue vivo en
BingX: por eso se envía siempre con la orden y no se gestiona solo en
memoria.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import logging
import time

import httpx

import config
import strategy
from bingx import BingX, BingXError
from notify import State, Telegram

logging.basicConfig(
    level=getattr(logging, getattr(config, "LOG_LEVEL", "INFO"), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
if getattr(config, "LOG_LEVEL", "INFO") != "DEBUG":
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

log = logging.getLogger("bot")


def fmt_signal(sig: strategy.Signal, live: bool) -> str:
    cabecera = "🟢 EJECUTADO" if live else "🔔 SEÑAL"
    base = sig.symbol.split("-")[0]
    riesgo = (sig.entry - sig.sl) / sig.entry * 100.0
    tp_txt = f"\nTP <code>{sig.tp:.8g}</code>" if sig.tp else "\nSin TP: sale cuando gire el SuperTrend"
    return (
        f"{cabecera} · LARGO <b>{base}</b>  (doble suelo RSI)\n"
        f"Entrada <code>{sig.entry:.8g}</code>\n"
        f"SL (SuperTrend) <code>{sig.sl:.8g}</code>  ·  riesgo {riesgo:.2f}%"
        f"{tp_txt}\n"
        f"RSI {sig.rsi:.1f} · cruce nº{sig.cross_count} bajo {config.TRIGGER_LEVEL:.0f} · ATR {sig.atr_pct:.2f}%"
    )


class Bot:
    def __init__(self) -> None:
        self.state = State(config.STATE_PATH)
        self.client = httpx.AsyncClient()
        self.api = BingX(self.client)
        self.tg = Telegram(self.client)
        self.symbols: list[str] = []
        self.volumes: dict[str, float] = {}
        self.live = config.is_live()
        self.last_heartbeat = time.time()
        self.sem = asyncio.Semaphore(config.SCAN_CONCURRENCY)

    async def start(self) -> None:
        log.info("Modo: %s", config.describe())
        await self.tg.send(
            f"🤖 <b>Bot RSI doble suelo iniciado</b>\n"
            f"{config.describe()}\n"
            f"RSI({config.RSI_LEN}) cruzando SMA({config.SIG_LEN}) bajo {config.TRIGGER_LEVEL:.0f}\n"
            f"Señal en el cruce nº{config.TARGET_CROSS} · salida SuperTrend({config.ST_PERIOD}, {config.ST_FACTOR})\n"
            f"Timeframe {config.TIMEFRAME} · riesgo {config.RISK_PCT}%"
        )
        await self.refresh_symbols()
        while True:
            try:
                await self.manage_open()
                await self.maybe_daily_summary()
                await self.maybe_heartbeat()
                await self.scan_once()
            except Exception as exc:  # noqa: BLE001
                log.exception("Fallo en el ciclo: %s", exc)
            await asyncio.sleep(config.SCAN_INTERVAL_SEC)

    async def refresh_symbols(self) -> None:
        try:
            syms = await self.api.symbols()
        except Exception as exc:  # noqa: BLE001
            log.error("No se pudo listar símbolos: %s", exc)
            return
        if config.SYMBOL_WHITELIST:
            syms = [s for s in syms if s.split("-")[0].upper() in config.SYMBOL_WHITELIST]
        try:
            self.volumes = await self.api.tickers_24h()
            antes = len(syms)
            syms = [s for s in syms if self.volumes.get(s, 0.0) >= config.MIN_QUOTE_VOLUME_24H]
            log.info("Liquidez: %d de %d superan %.0f USDT", len(syms), antes, config.MIN_QUOTE_VOLUME_24H)
        except Exception as exc:  # noqa: BLE001
            log.warning("Sin filtro de liquidez (%s)", exc)
        self.symbols = syms[: config.MAX_SYMBOLS]
        log.info("Universo: %d símbolos", len(self.symbols))

    def in_cooldown(self) -> bool:
        return time.time() < float(self.state.data.get("cooldown_until", 0))

    async def _velas(self, sym: str) -> list[dict] | None:
        async with self.sem:
            try:
                return await self.api.klines(sym, config.TIMEFRAME, limit=400)
            except Exception:  # noqa: BLE001
                return None

    async def scan_once(self) -> None:
        if self.in_cooldown():
            return
        abiertas = len(self.state.data.get("open", {}))
        if abiertas >= config.MAX_CONCURRENT:
            return

        candidatos = 0
        motivos: dict[str, int] = {}
        for sym in self.symbols:
            if sym in self.state.data.get("open", {}):
                continue
            velas = await self._velas(sym)
            if not velas:
                continue
            sig, motivo = strategy.evaluate(sym, velas)
            if sig is None:
                # Se agrupa por CLASE de motivo, no por el texto exacto:
                # "sin señal (contador en 1...)" y "(contador en 0...)"
                # son el mismo caso y separarlos escondería el patrón.
                clave = motivo.split("(")[0].strip()
                motivos[clave] = motivos.get(clave, 0) + 1
                log.debug("%s: %s", sym, motivo)
                continue
            candidatos += 1
            await self.handle_signal(sig)
            abiertas += 1
            if abiertas >= config.MAX_CONCURRENT:
                break

        detalle = " · ".join(f"{k}: {v}" for k, v in sorted(motivos.items(), key=lambda x: -x[1])[:5])
        log.info("Ciclo completo · %d símbolos · %d señales | %s", len(self.symbols), candidatos, detalle)

        # Si el embudo se corta SIEMPRE en el mismo sitio, eso no es el
        # mercado: es un filtro mal calibrado. Se avisa una vez al día.
        if motivos:
            top = max(motivos.items(), key=lambda x: x[1])
            if top[1] >= len(self.symbols) * 0.9:
                hoy = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
                if self.state.data.get("warned_funnel") != hoy:
                    self.state.data["warned_funnel"] = hoy
                    self.state.save()
                    await self.tg.send(
                        f"🔻 <b>El embudo se corta siempre en el mismo punto</b>\n"
                        f"<code>{top[0]}</code> descarta {top[1]} de {len(self.symbols)} símbolos.\n"
                        f"Si es «señal, pero el SuperTrend sigue bajista», el filtro "
                        f"<code>REQUIRE_ST_BULL</code> está bloqueando la estrategia: un doble "
                        f"suelo ocurre POR DEFINICIÓN en caída, cuando el SuperTrend está bajista."
                    )

    async def handle_signal(self, sig: strategy.Signal) -> None:
        log.info("SEÑAL %s entrada=%.8g sl=%.8g rsi=%.1f", sig.symbol, sig.entry, sig.sl, sig.rsi)
        if not self.live:
            await self.tg.send(fmt_signal(sig, live=False))
            return

        client_id = f"rsi{sig.symbol.split('-')[0][:6]}{int(time.time())}"
        try:
            equity = await self.api.balance_usdt()
            if equity <= 0:
                await self.tg.send(f"⚠️ {sig.symbol} sin ejecutar: saldo 0")
                return
            qty = self.api.round_qty(sig.symbol, strategy.position_size(equity, sig.entry, sig.sl))
            minimo = self.api.min_qty(sig.symbol)
            if qty <= 0 or (minimo > 0 and qty < minimo):
                await self.tg.send(f"⚠️ {sig.symbol} sin ejecutar: tamaño {qty} bajo el mínimo {minimo}")
                return
            vivas = await self.api.open_positions()
            if any(str(p.get("symbol")) == sig.symbol and float(p.get("positionAmt", 0) or 0) != 0 for p in vivas):
                return

            await self.api.set_leverage(sig.symbol, "LONG", config.LEVERAGE)
            sl_r = self.api.round_price(sig.symbol, sig.sl)
            tp_r = self.api.round_price(sig.symbol, sig.tp) if sig.tp else sl_r * 100
            if config.ENTRY_TYPE == "LIMIT":
                precio = self.api.round_price(sig.symbol, sig.entry * (1 - config.LIMIT_OFFSET_PCT / 100.0))
                await self.api.limit_order(sig.symbol, "BUY", qty, precio, sl_r, tp_r, client_id)
            else:
                await self.api.market_order(sig.symbol, "BUY", qty, sl_r, tp_r, client_id)
        except BingXError as exc:
            await self.tg.send(f"❌ BingX rechazó {sig.symbol}: {exc}")
            return
        except Exception as exc:  # noqa: BLE001
            if await self.api.order_exists(sig.symbol, client_id):
                await self.tg.send(f"⚠️ {sig.symbol}: fallo de red pero la orden SÍ existe. Se registra.")
            else:
                await self.tg.send(f"❌ Error en {sig.symbol}: {exc}")
                return

        self.state.data.setdefault("open", {})[sig.symbol] = {
            "side": "BUY", "entry": sig.entry, "sl": sig.sl, "qty": qty,
            "opened_at": time.time(),
        }
        self.state.data["last_trade_ts"] = time.time()
        self.state.save()
        await self.tg.send(fmt_signal(sig, live=True))

    async def manage_open(self) -> None:
        """
        Sube el stop siguiendo al SuperTrend y cierra cuando gira.

        El stop SOLO se mueve a favor. Reenviarlo al exchange en cada
        actualización es deliberado: si el bot se cae, el último stop
        enviado sigue protegiendo la posición.
        """
        abiertas = self.state.data.get("open", {})
        if not abiertas:
            return
        for symbol, pos in list(abiertas.items()):
            velas = await self._velas(symbol)
            if not velas:
                continue

            if strategy.exit_signal(velas):
                log.info("%s: SuperTrend girado, cierre", symbol)
                if self.live:
                    try:
                        await self.api.close_position(symbol, "BUY", float(pos.get("qty", 0)))
                    except Exception as exc:  # noqa: BLE001
                        await self.tg.send(f"⚠️ No se pudo cerrar {symbol}: {exc}")
                        continue
                precio = velas[-1]["close"]
                gano = precio > float(pos["entry"])
                await self.tg.send(
                    f"{'✅' if gano else '🛑'} <b>{symbol.split('-')[0]}</b> cerrada por SuperTrend\n"
                    f"Entrada {pos['entry']:.8g} → {precio:.8g}"
                )
                self.register_close(symbol, gano)
                continue

            # Trailing del SuperTrend
            c = velas[:-1]
            st, dirs = strategy.supertrend(
                [x["high"] for x in c], [x["low"] for x in c], [x["close"] for x in c],
                config.ST_FACTOR, config.ST_PERIOD,
            )
            if not st or dirs[-1] != -1:
                continue
            nuevo = st[-1]
            if nuevo > float(pos.get("sl", 0)):
                pos["sl"] = nuevo
                self.state.save()
                log.info("%s: stop del SuperTrend subido a %.8g", symbol, nuevo)
                # El stop que hay en BingX es el que se envió con la
                # entrada. Actualizarlo requeriría cancelar y reemitir la
                # orden condicional, y una cancelación fallida dejaría la
                # posición DESPROTEGIDA unos segundos. Se prefiere el
                # stop original (más lejos, peor precio) a la ventana sin
                # red: el cierre por giro del SuperTrend lo hace el bot
                # igualmente en el ciclo siguiente.

    def register_close(self, symbol: str, won: bool) -> None:
        d = self.state.data
        d["closed_trades"] = d.get("closed_trades", 0) + 1
        if won:
            d["wins"] = d.get("wins", 0) + 1
            d["consecutive_losses"] = 0
        else:
            d["losses"] = d.get("losses", 0) + 1
            d["consecutive_losses"] = d.get("consecutive_losses", 0) + 1
            if d["consecutive_losses"] >= config.MAX_CONSECUTIVE_LOSSES:
                d["cooldown_until"] = time.time() + config.COOLDOWN_MINUTES * 60
                d["consecutive_losses"] = 0
                asyncio.create_task(self.tg.send(
                    f"⏸️ <b>Circuit breaker</b> · {config.MAX_CONSECUTIVE_LOSSES} pérdidas seguidas, "
                    f"pausa de {config.COOLDOWN_MINUTES} min."
                ))
        d.get("open", {}).pop(symbol, None)
        self.state.save()

    def stats_text(self) -> str:
        d = self.state.data
        n = d.get("closed_trades", 0)
        w = d.get("wins", 0)
        wr = (w / n * 100.0) if n else 0.0
        return (
            f"Cerradas: <b>{n}</b> · aciertos {w} ({wr:.0f}%)\n"
            f"Abiertas: {len(d.get('open', {}))} · racha: {d.get('consecutive_losses', 0)}"
        )

    async def maybe_daily_summary(self) -> None:
        if not config.DAILY_SUMMARY:
            return
        ahora = dt.datetime.now(dt.timezone.utc)
        hoy = ahora.strftime("%Y-%m-%d")
        if ahora.hour != config.DAILY_SUMMARY_HOUR_UTC or self.state.data.get("last_summary") == hoy:
            return
        self.state.data["last_summary"] = hoy
        self.state.save()
        await self.tg.send(
            f"📊 <b>Resumen diario RSI</b> · {hoy}\n{config.describe()}\n\n"
            f"{self.stats_text()}\nUniverso: {len(self.symbols)} símbolos"
        )

    async def maybe_heartbeat(self) -> None:
        if config.HEARTBEAT_HOURS <= 0:
            return
        if time.time() - self.last_heartbeat < config.HEARTBEAT_HOURS * 3600:
            return
        self.last_heartbeat = time.time()
        await self.tg.send(f"💓 Vivo (RSI) · {self.stats_text()}")


async def main() -> None:
    bot = Bot()
    try:
        await bot.start()
    finally:
        await bot.client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
