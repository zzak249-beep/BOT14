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
import os
import time

import httpx

import config
import journal
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



_DEFAULTS = {
    "RSI_LEN": 10, "SIG_LEN": 10, "TRIGGER_LEVEL": 50.0, "TARGET_CROSS": 2,
    "ATR_LEN": 14, "WATCHLIST_MIN": 30, "TIMEFRAMES": ["15m"], "MAX_DAILY_LOSS_R": 3.0, "BTC_CONTEXT": True, "BTC_FILTER": False, "BTC_MIN_24H": -3.0, "ST_PERIOD": 10, "ST_FACTOR": 2.5, "REQUIRE_ST_BULL": True,
    "SL_SWING_ATR": 0.5, "SL_SWING_LOOKBACK": 20, "USE_TP": False, "RR_TARGET": 2.0,
    "TIMEFRAME": "15m", "SCAN_INTERVAL_SEC": 90, "MAX_SYMBOLS": 400,
    "SYMBOL_WHITELIST": [], "EXCLUDE_PREFIXES": ["NC"],
    "MIN_QUOTE_VOLUME_24H": 3_000_000.0, "MIN_ATR_PCT": 0.5,
    "MAX_RISK_PCT": 4.0, "MIN_RISK_PCT": 1.5, "MAX_COST_IN_R": 0.20,
    "COST_ROUNDTRIP_PCT": 0.25, "SCAN_CONCURRENCY": 8,
    "RISK_PCT": 0.25, "MAX_CONCURRENT": 2, "LEVERAGE": 2,
    "MAX_CONSECUTIVE_LOSSES": 3, "COOLDOWN_MINUTES": 180,
    "ENTRY_TYPE": "LIMIT", "LIMIT_OFFSET_PCT": 0.05, "SIGNAL_COOLDOWN_MIN": 60,
    "DAILY_SUMMARY": True, "DAILY_SUMMARY_HOUR_UTC": 7, "HEARTBEAT_HOURS": 12,
    "IDLE_ALERT_DAYS": 5, "STATE_PATH": "/data/state_rsi.json", "LOG_LEVEL": "INFO",
}


def ensure_config() -> list[str]:
    """Un config.py antiguo no puede tumbar un bot con dinero real."""
    faltan = []
    for nombre, valor in _DEFAULTS.items():
        if not hasattr(config, nombre):
            setattr(config, nombre, valor)
            faltan.append(nombre)
    return faltan


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
        f"RSI {sig.rsi:.1f} · cruce nº{sig.cross_count} bajo {config.TRIGGER_LEVEL:.0f} · ATR {sig.atr_pct:.2f}%\n"
        f"Timeframe <b>{getattr(sig, 'timeframe', config.TIMEFRAME)}</b>"
        + (f"  ·  BTC {sig.btc_24h:+.1f}% 24h" if getattr(sig, "btc_24h", None) is not None else "")
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
        self.last_watchlist = 0.0
        self.btc_24h: float | None = None
        self.btc_ts = 0.0
        self.sem = asyncio.Semaphore(config.SCAN_CONCURRENCY)
        self.journal = journal.Journal(
            os.path.join(os.path.dirname(config.STATE_PATH) or "/data", "operaciones_rsi-st-bot.csv")
        )

    async def start(self) -> None:
        faltan = ensure_config()
        if faltan:
            log.error("config.py desactualizado, faltaban: %s", ", ".join(faltan))
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
                await self.reconcile()
                await self.maybe_watchlist()
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

    async def contexto_btc(self) -> float | None:
        """Variación de BTC en 24h. Se refresca cada 15 minutos."""
        if not config.BTC_CONTEXT:
            return None
        if time.time() - self.btc_ts < 900 and self.btc_24h is not None:
            return self.btc_24h
        try:
            velas = await self.api.klines("BTC-USDT", "1h", limit=30)
            if len(velas) >= 25:
                self.btc_24h = (velas[-1]["close"] - velas[-25]["close"]) / velas[-25]["close"] * 100.0
                self.btc_ts = time.time()
        except Exception:  # noqa: BLE001
            pass
        return self.btc_24h

    def dia_actual(self) -> str:
        return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")

    def limite_diario_alcanzado(self) -> bool:
        """
        Stop de pérdida DIARIA. Distinto del circuit breaker por rachas:
        seis pérdidas alternadas con dos ganancias pequeñas no disparan
        una racha de tres, y el día acaba igual de mal. Se reinicia solo
        al cambiar de día UTC.
        """
        if config.MAX_DAILY_LOSS_R <= 0:
            return False
        d = self.state.data
        if d.get("dia_r") != self.dia_actual():
            d["dia_r"] = self.dia_actual()
            d["r_hoy"] = 0.0
            self.state.save()
            return False
        return float(d.get("r_hoy", 0.0)) <= -abs(config.MAX_DAILY_LOSS_R)

    def sumar_r_dia(self, r: float) -> None:
        d = self.state.data
        if d.get("dia_r") != self.dia_actual():
            d["dia_r"] = self.dia_actual()
            d["r_hoy"] = 0.0
        d["r_hoy"] = float(d.get("r_hoy", 0.0)) + r
        self.state.save()

    async def _velas(self, sym: str, tf: str | None = None) -> list[dict] | None:
        async with self.sem:
            try:
                return await self.api.klines(sym, tf or config.TIMEFRAME, limit=400)
            except Exception:  # noqa: BLE001
                return None

    async def scan_once(self) -> None:
        if self.in_cooldown():
            return
        btc = await self.contexto_btc()
        if config.BTC_FILTER and btc is not None and btc < config.BTC_MIN_24H:
            log.info("BTC %.1f%% en 24h: por debajo del mínimo, no se abre", btc)
            return

        if self.limite_diario_alcanzado():
            if self.state.data.get("aviso_dia") != self.dia_actual():
                self.state.data["aviso_dia"] = self.dia_actual()
                self.state.save()
                await self.tg.send(
                    f"🛑 <b>Límite de pérdida diaria alcanzado</b>\n"
                    f"{float(self.state.data.get('r_hoy', 0)):.2f} R hoy "
                    f"(límite {config.MAX_DAILY_LOSS_R} R).\n"
                    f"No se abren más posiciones hasta mañana (UTC)."
                )
            return
        abiertas = len(self.state.data.get("open", {}))
        # NO se corta el escaneo aunque no haya hueco: avisar y ejecutar
        # son cosas distintas. Si el bot no puede entrar, la señal sigue
        # existiendo y quieres verla — para operarla a mano o para saber
        # qué se está perdiendo.
        hay_hueco = abiertas < config.MAX_CONCURRENT

        candidatos = 0
        motivos: dict[str, int] = {}
        for sym in self.symbols:
            if sym in self.state.data.get("open", {}):
                continue
            # Cada timeframe se evalúa por separado: el patrón puede
            # completarse en 5m y no en 15m, o al revés.
            sig = None
            motivo = "sin datos"
            tf_señal = config.TIMEFRAME
            for tf in config.TIMEFRAMES:
                velas = await self._velas(sym, tf)
                if not velas:
                    continue
                s_tf, m_tf = strategy.evaluate(sym, velas)
                if s_tf is not None:
                    sig, motivo, tf_señal = s_tf, m_tf, tf
                    break
                motivo = m_tf
            if sig is None:
                # Se agrupa por CLASE de motivo, no por el texto exacto:
                # "sin señal (contador en 1...)" y "(contador en 0...)"
                # son el mismo caso y separarlos escondería el patrón.
                clave = motivo.split("(")[0].strip()
                motivos[clave] = motivos.get(clave, 0) + 1
                log.debug("%s: %s", sym, motivo)
                continue
            candidatos += 1
            sig.timeframe = tf_señal
            sig.btc_24h = btc
            ejecutada = await self.handle_signal(sig, hay_hueco)
            if ejecutada:
                abiertas += 1
                hay_hueco = abiertas < config.MAX_CONCURRENT

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

    async def handle_signal(self, sig: strategy.Signal, hay_hueco: bool = True) -> bool:
        """
        Devuelve si se ABRIÓ posición. El aviso se manda siempre (con
        enfriamiento por símbolo), ejecute o no: son dos cosas
        independientes y mezclarlas hacía que en LIVE, con el hueco
        lleno, no te enteraras de las señales.
        """
        log.info("SEÑAL %s entrada=%.8g sl=%.8g rsi=%.1f", sig.symbol, sig.entry, sig.sl, sig.rsi)

        # AVISO — siempre, con enfriamiento para no repetir cada ciclo.
        ultimos = self.state.data.setdefault("last_signal", {})
        previo = float(ultimos.get(sig.symbol, 0) or 0)
        if time.time() - previo >= config.SIGNAL_COOLDOWN_MIN * 60:
            ultimos[sig.symbol] = time.time()
            self.state.save()
            nota = ""
            if self.live and not hay_hueco:
                nota = f"\n<i>Sin hueco libre ({config.MAX_CONCURRENT} posición máx.): el bot no la abre.</i>"
            elif not self.live:
                nota = "\n<i>Modo SIGNAL: el bot no la abre.</i>"
            entregado = await self.tg.send(fmt_signal(sig, live=False) + nota)
            if not entregado:
                log.error("SEÑAL %s NO entregada por Telegram", sig.symbol)

        if not self.live or not hay_hueco:
            return False

        client_id = f"rsi{sig.symbol.split('-')[0][:6]}{int(time.time())}"
        try:
            equity = await self.api.balance_usdt()
            if equity <= 0:
                await self.tg.send(f"⚠️ {sig.symbol} sin ejecutar: saldo 0")
                return False
            qty = self.api.round_qty(sig.symbol, strategy.position_size(equity, sig.entry, sig.sl))
            minimo = self.api.min_qty(sig.symbol)
            if qty <= 0 or (minimo > 0 and qty < minimo):
                riesgo_min = minimo * abs(sig.entry - sig.sl)
                pct = (riesgo_min / equity * 100.0) if equity > 0 else 0.0
                await self.tg.send(
                    f"⚠️ <b>{sig.symbol}</b> sin ejecutar: tamaño {qty} bajo el lote mínimo ({minimo}).\n"
                    f"Harían falta <b>{pct:.2f}%</b> de riesgo (ahora {config.RISK_PCT}%)."
                )
                return False
            vivas = await self.api.open_positions()
            if any(str(p.get("symbol")) == sig.symbol and float(p.get("positionAmt", 0) or 0) != 0 for p in vivas):
                return False

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
            return False
        except Exception as exc:  # noqa: BLE001
            if await self.api.order_exists(sig.symbol, client_id):
                await self.tg.send(f"⚠️ {sig.symbol}: fallo de red pero la orden SÍ existe. Se registra.")
            else:
                await self.tg.send(f"❌ Error en {sig.symbol}: {exc}")
                return False

        self.state.data.setdefault("open", {})[sig.symbol] = {
            "side": "BUY", "entry": sig.entry, "sl": sig.sl, "sl_inicial": sig.sl, "qty": qty,
            "opened_at": time.time(),
        }
        self.journal.abrir(sig, qty, "LIVE")
        self.state.data["last_trade_ts"] = time.time()
        self.state.save()
        await self.tg.send(fmt_signal(sig, live=True))
        return True

    async def maybe_watchlist(self) -> None:
        """
        Aviso periódico con las que están cerca del patrón.

        Las señales llegan cuando ya se dispararon. Esto enseña lo que
        viene: las que tienen el contador en 1 de 2 ya tocaron suelo una
        vez y están a un cruce de disparar. Es la diferencia entre
        enterarte y verlo venir.
        """
        if config.WATCHLIST_MIN <= 0:
            return
        if time.time() - self.last_watchlist < config.WATCHLIST_MIN * 60:
            return
        self.last_watchlist = time.time()

        cerca = []
        for sym in self.symbols:
            velas = await self._velas(sym)
            if not velas:
                continue
            w = strategy.watch_status(velas)
            if not w or not w.bajo_umbral:
                continue
            if w.cross_count >= 1 and w.atr_pct >= config.MIN_ATR_PCT:
                cerca.append((sym, w))

        if not cerca:
            log.info("Vigilancia: ninguna cerca del patrón")
            return

        cerca.sort(key=lambda t: (-t[1].cross_count, t[1].rsi))
        lineas = [f"👀 <b>Tocaron suelo — vigilando</b> ({len(cerca)})\n"]
        for sym, w in cerca[:12]:
            base = sym.split("-")[0]
            marca = "🟡" if w.cross_count >= config.TARGET_CROSS - 1 else "·"
            st = "ST alcista" if w.st_alcista else "ST bajista"
            lineas.append(
                f"{marca} <b>{base}</b>  RSI {w.rsi:.0f}/{w.rsi_sig:.0f}  "
                f"cruce {w.cross_count} de {config.TARGET_CROSS}  ·  {st}  ·  ATR {w.atr_pct:.2f}%"
            )
        lineas.append(
            f"\n🟡 a un cruce de disparar · · primer suelo hecho\n"
            f"<i>El aviso de señal llega cuando se completa el patrón.</i>"
        )
        await self.tg.send("\n".join(lineas))

    async def reconcile(self) -> None:
        """
        Detecta las posiciones que se cerraron EN EL EXCHANGE (stop o
        take profit) y que el bot no vio.

        Sin esto, cuando salta el SL la posición sigue "abierta" en el
        estado para siempre: bloquea el hueco de MAX_CONCURRENT y el
        circuit breaker no cuenta ni una pérdida. En SIGNAL no se nota;
        en LIVE el bot se queda mudo y bloqueado tras las primeras
        operaciones.
        """
        if not self.live:
            return
        abiertas = self.state.data.get("open", {})
        if not abiertas:
            return
        try:
            posiciones = await self.api.open_positions()
        except Exception as exc:  # noqa: BLE001
            log.warning("No se pudieron leer las posiciones: %s", exc)
            return
        vivos = {
            str(p.get("symbol", "")) for p in posiciones
            if float(p.get("positionAmt", 0) or 0) != 0
        }
        for symbol, pos in list(abiertas.items()):
            if symbol in vivos:
                continue
            velas = await self._velas(symbol)
            ultimo = velas[-1]["close"] if velas else float(pos["entry"])
            gano = ultimo > float(pos["entry"])
            riesgo = abs(float(pos["entry"]) - float(pos.get("sl_inicial", pos["sl"])))
            r_real = (ultimo - float(pos["entry"])) / riesgo if riesgo > 0 else 0.0
            minutos = int((time.time() - float(pos.get("opened_at", time.time()))) / 60)
            self.journal.cerrar(symbol, "sl/tp", ultimo, r_real, minutos)
            self.sumar_r_dia(r_real)
            await self.tg.send(
                f"{'✅' if gano else '🛑'} <b>{symbol.split('-')[0]}</b> cerrada en el exchange\n"
                f"Entrada {pos['entry']:.8g} → {ultimo:.8g}  ({r_real:+.2f} R)"
            )
            self.register_close(symbol, gano)

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
                riesgo = abs(float(pos["entry"]) - float(pos.get("sl_inicial", pos["sl"])))
                r_real = (precio - float(pos["entry"])) / riesgo if riesgo > 0 else 0.0
                minutos = int((time.time() - float(pos.get("opened_at", time.time()))) / 60)
                self.journal.cerrar(symbol, "supertrend", precio, r_real, minutos)
                self.sumar_r_dia(r_real)
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
