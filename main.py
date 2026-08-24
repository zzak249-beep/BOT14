"""
Bot de reversión por sobreextensión — BingX USDT-M, 5 minutos.

Escanea, filtra por amplitud, busca el estirón y avisa (o ejecuta si
LIVE está confirmado dos veces).

Lo que este bot NO hace, a propósito:
  · No promedia a la baja.
  · No reentra tras un stop en el mismo símbolo hasta pasado el enfriamiento.
  · No opera sin stop: el SL viaja en la misma orden que la entrada.
  · No opera símbolos sin amplitud, por bonito que sea el patrón.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import logging
import time

import httpx

import config
import scanner
import xsection
import strategy
from bingx import BingX, BingXError
from notify import State, Telegram

logging.basicConfig(
    level=getattr(logging, getattr(config, "LOG_LEVEL", "INFO"), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# httpx registra en INFO CADA petición. Con ~940 símbolos por ciclo y un
# ciclo cada pocos minutos, son decenas de miles de líneas al día que
# entierran lo único que importa leer: los ciclos, las señales y los
# errores. Se silencia salvo que se pida DEBUG a propósito.
if getattr(config, "LOG_LEVEL", "INFO") != "DEBUG":
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

log = logging.getLogger("bot")



# ══════════════════════════════════════════════════════════════════════
# COMPATIBILIDAD DE CONFIGURACIÓN
# Un bot con dinero real no puede morir porque el config.py del
# repositorio sea más antiguo que main.py. Aquí se comprueba que existan
# todos los ajustes que este archivo usa y, si falta alguno, se INYECTA
# su valor por defecto y se avisa — en vez de reventar con un
# AttributeError que además deja la posición sin vigilancia.
# ══════════════════════════════════════════════════════════════════════
_DEFAULTS = {
    "MIN_ATR_PCT": 4.0, "MIN_COST_COVER": 30.0, "COST_ROUNDTRIP_PCT": 0.25,
    "MAX_ER_LONG": 0.35, "MIN_QUOTE_VOLUME_24H": 2_000_000.0,
    "MA_LEN": 20, "ATR_LEN": 14, "STRETCH_ATR": 2.5, "MAX_BARS_STRETCH": 6,
    "SL_ATR": 1.0, "TP_MODE": "MEAN", "RR_FIXED": 1.5, "MIN_RR": 1.0,
    "RISK_PCT": 0.5, "MAX_CONCURRENT": 2, "LEVERAGE": 3,
    "MAX_CONSECUTIVE_LOSSES": 3, "COOLDOWN_MINUTES": 120,
    "MAX_TRADE_BARS": 12, "USE_TIME_EXIT": True, "TIME_EXIT_ONLY_LOSING": True,
    "ENTRY_TYPE": "LIMIT", "LIMIT_OFFSET_PCT": 0.05,
    "TIMEFRAME": "5m", "SCAN_INTERVAL_SEC": 60, "MAX_SYMBOLS": 200,
    "SCAN_ALL": True, "RANK_INTERVAL_MIN": 15, "RANK_TOP_N": 12,
    "RANK_MODE": "top_siempre", "RANK_ONLY_WHEN_CANDIDATES": False,
    "SCAN_CONCURRENCY": 8, "RANGE_LEN": 20, "ER_SHORT": 30, "ER_LONG": 180,
    "ER_TREND": 0.40, "EXCLUDE_PREFIXES": ["NC"], "SYMBOL_WHITELIST": [],
    "DAILY_SUMMARY": True, "DAILY_SUMMARY_HOUR_UTC": 7, "HEARTBEAT_HOURS": 12,
    "IDLE_ALERT_DAYS": 5, "WATCH_ALERTS": True, "WATCH_COOLDOWN_MIN": 60,
    "WATCH_NEAR_ATR": 0.5, "XSECTION_ENABLED": True, "XSECTION_HOUR_UTC": 0,
    "XSECTION_N": 5, "XSECTION_MIN_VOL": 500_000.0,
    "STATE_PATH": "/data/state.json", "LOG_LEVEL": "INFO",
}


def ensure_config() -> list[str]:
    """Rellena lo que falte y devuelve la lista de lo inyectado."""
    faltan = []
    for nombre, valor in _DEFAULTS.items():
        if not hasattr(config, nombre):
            setattr(config, nombre, valor)
            faltan.append(nombre)
    return faltan


def fmt_signal(sig: strategy.Signal, live: bool) -> str:
    lado = "LARGO" if sig.side == "BUY" else "CORTO"
    cabecera = "🟢 EJECUTADO" if live else "🔔 SEÑAL"
    return (
        f"{cabecera} · {lado} <b>{sig.symbol}</b>\n"
        f"Entrada <code>{sig.entry:.8g}</code>\n"
        f"SL <code>{sig.sl:.8g}</code>  ·  TP <code>{sig.tp:.8g}</code>\n"
        f"R:R {sig.rr:.2f}  ·  estirón {sig.stretch:+.2f} ATR\n"
        f"ATR {sig.atr_pct:.2f}%  ·  {sig.cost_cover:.0f}× el coste"
    )


class Bot:
    def __init__(self) -> None:
        self.state = State(config.STATE_PATH)
        self.client = httpx.AsyncClient()
        self.api = BingX(self.client)
        self.tg = Telegram(self.client)
        self.symbols: list[str] = []
        self.live = config.is_live()
        self.scanner = scanner.Scanner(self.api)
        self.last_rank = 0.0
        self.volumes: dict[str, float] = {}
        self.had_candidates = False
        self.last_rows: list = []
        self.last_heartbeat = time.time()

    async def start(self) -> None:
        faltan = ensure_config()
        if faltan:
            log.error("config.py desactualizado, faltaban: %s", ", ".join(faltan))
            await self.tg.send(
                "⚠️ <b>config.py desactualizado</b>\n"
                f"Faltaban {len(faltan)} ajustes; se han usado los valores por defecto "
                f"para no parar el bot:\n<code>{', '.join(faltan[:12])}</code>"
                + ("…" if len(faltan) > 12 else "")
                + "\n\nSube el config.py actualizado cuando puedas."
            )
        log.info("Modo: %s", config.describe())
        await self.tg.send(
            f"🤖 <b>Bot de reversión iniciado</b>\n"
            f"{config.describe()}\n"
            f"Filtro de amplitud: ATR ≥ {config.MIN_ATR_PCT}% y ≥ {config.MIN_COST_COVER:.0f}× el coste\n"
            f"Riesgo por operación: {config.RISK_PCT}%"
        )
        # Si un despliegue anterior dejó órdenes pendientes, se resuelven
        # antes de nada: al reiniciar, lo primero es saber qué hay vivo.
        huerfanas = list(self.state.data.get("pending", {}).keys())
        if huerfanas:
            await self.tg.send(
                f"⚠️ Al arrancar quedaban {len(huerfanas)} órdenes pendientes de antes: "
                f"{', '.join(huerfanas)}.\nSe resolverán en el primer ciclo."
            )

        await self.refresh_symbols()
        while True:
            try:
                await self.resolve_pending()
                await self.maybe_xsection()
                await self.reconcile()
                await self.maybe_daily_summary()
                await self.maybe_heartbeat()
                await self.maybe_idle_warning()
                await self.check_time_exits()
                await self.maybe_rank()
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

        # Filtro de liquidez: filtrar por amplitud sin mirar el volumen es
        # cazar justo las monedas donde el libro es un colador.
        try:
            self.volumes = await self.api.tickers_24h()
            antes = len(syms)
            syms = [s for s in syms if self.volumes.get(s, 0.0) >= config.MIN_QUOTE_VOLUME_24H]
            log.info(
                "Liquidez: %d de %d símbolos superan %.0f USDT de volumen 24h",
                len(syms), antes, config.MIN_QUOTE_VOLUME_24H,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("No se pudo filtrar por liquidez (%s): se sigue sin ese filtro", exc)
        self.symbols = syms if config.SCAN_ALL else syms[: config.MAX_SYMBOLS]
        log.info("Universo: %d símbolos", len(self.symbols))

    def stats_text(self) -> str:
        d = self.state.data
        cerradas = d.get("closed_trades", 0)
        wins = d.get("wins", 0)
        wr = (wins / cerradas * 100.0) if cerradas else 0.0
        abiertas = len(d.get("open", {}))
        enfr = ""
        if self.in_cooldown():
            mins = int((float(d["cooldown_until"]) - time.time()) / 60)
            enfr = f"\n⏸️ En enfriamiento {mins} min"
        return (
            f"Cerradas: <b>{cerradas}</b>  ·  aciertos {wins} ({wr:.0f}%)\n"
            f"Abiertas: {abiertas}  ·  racha de pérdidas: {d.get('consecutive_losses', 0)}"
            f"{enfr}"
        )

    async def maybe_daily_summary(self) -> None:
        """Un resumen al día. Sin esto solo te enteras cuando hay señal,
        y el silencio no distingue entre 'no hay nada' y 'está caído'."""
        if not config.DAILY_SUMMARY:
            return
        ahora = dt.datetime.now(dt.timezone.utc)
        hoy = ahora.strftime("%Y-%m-%d")
        if ahora.hour != config.DAILY_SUMMARY_HOUR_UTC:
            return
        if self.state.data.get("last_summary") == hoy:
            return
        self.state.data["last_summary"] = hoy
        self.state.save()
        entregado = await self.tg.send(
            f"📊 <b>Resumen diario</b> · {hoy}\n"
            f"{config.describe()}\n\n"
            f"{self.stats_text()}\n"
            f"Universo: {len(self.symbols)} símbolos"
        )
        if not entregado:
            log.error("El resumen diario NO se entregó: revisa las credenciales de Telegram")

    async def maybe_idle_warning(self) -> None:
        """
        Aviso de INACTIVIDAD ECONÓMICA.

        El fallo más traicionero de un bot en producción es el silencioso:
        la infraestructura sigue viva, los logs no dan errores y el
        exchange responde, pero hace semanas que no se opera. Vigilar la
        salud del proceso no detecta eso — hay que vigilar el resultado.
        """
        if not self.live or config.IDLE_ALERT_DAYS <= 0:
            return
        ultima = float(self.state.data.get("last_trade_ts", 0) or 0)
        if ultima <= 0:
            ultima = float(self.state.data.get("started_ts", time.time()))
            self.state.data.setdefault("started_ts", ultima)
        dias = (time.time() - ultima) / 86400.0
        if dias < config.IDLE_ALERT_DAYS:
            return
        if self.state.data.get("idle_warned_at", 0) and time.time() - float(self.state.data["idle_warned_at"]) < 86400:
            return
        self.state.data["idle_warned_at"] = time.time()
        self.state.save()
        await self.tg.send(
            f"🔇 <b>{dias:.0f} días sin una sola operación</b> estando en LIVE.\n"
            f"Puede ser el mercado (filtros exigentes) o puede ser un fallo silencioso.\n"
            f"Comprueba: ¿el ranking sigue llegando? ¿el saldo es correcto?\n"
            f"{self.stats_text()}"
        )

    async def maybe_heartbeat(self) -> None:
        """Señal de vida periódica: si deja de llegar, el bot está caído."""
        if config.HEARTBEAT_HOURS <= 0:
            return
        if time.time() - self.last_heartbeat < config.HEARTBEAT_HOURS * 3600:
            return
        self.last_heartbeat = time.time()
        await self.tg.send(f"💓 Vivo · {self.stats_text()}")

    async def _closes_24h(self) -> dict[str, tuple[float, float]]:
        """Precio de hace 24 h y actual, por símbolo."""
        out: dict[str, tuple[float, float]] = {}
        velas_dia = int(24 * 60 / {"5m": 5, "15m": 15, "30m": 30, "1h": 60}.get(config.TIMEFRAME, 5))
        for sym in self.symbols:
            if self.volumes.get(sym, 0.0) < config.XSECTION_MIN_VOL:
                continue
            try:
                velas = await self.api.klines(sym, config.TIMEFRAME, limit=velas_dia + 5)
            except Exception:  # noqa: BLE001
                continue
            if len(velas) < velas_dia + 1:
                continue
            out[sym] = (velas[-velas_dia - 1]["close"], velas[-1]["close"])
        return out

    async def maybe_xsection(self) -> None:
        """Una vez al día: evalúa lo de ayer y registra lo de hoy."""
        if not config.XSECTION_ENABLED:
            return
        ahora = dt.datetime.now(dt.timezone.utc)
        hoy = ahora.strftime("%Y-%m-%d")
        if ahora.hour != config.XSECTION_HOUR_UTC:
            return
        if self.state.data.get("xs_last_day") == hoy:
            return

        closes = await self._closes_24h()
        if len(closes) < config.XSECTION_N * 3:
            log.warning("Sección cruzada: solo %d símbolos con datos, se salta", len(closes))
            return

        # 1. Evaluar el ranking de ayer con los precios de hoy.
        anterior = self.state.data.get("xs_pending")
        texto, resumen = xsection.evaluate_previous(anterior, closes)
        if texto and resumen:
            hist = self.state.data.setdefault("xs_history", [])
            hist.append(resumen)
            await self.tg.send(texto + "\n\n" + xsection.format_history(hist))

        # 2. Registrar el de hoy.
        ranking = xsection.build_ranking(self.last_rows, self.volumes, closes)
        largos, cortos = xsection.pick_sides(ranking, config.XSECTION_N)
        if not largos:
            return
        self.state.data["xs_pending"] = {
            "fecha": hoy,
            "largos": [{"symbol": r.symbol, "price": r.price, "ret24": r.ret24} for r in largos],
            "cortos": [{"symbol": r.symbol, "price": r.price, "ret24": r.ret24} for r in cortos],
        }
        self.state.data["xs_last_day"] = hoy
        self.state.save()
        await self.tg.send(xsection.format_signal(largos, cortos))

    async def resolve_pending(self) -> None:
        """
        Cierra el ciclo de vida de las órdenes limitadas: o se ejecutaron
        (pasan a posición) o caducan (se cancelan y se olvidan).

        Sin esto, una orden que no entra se queda flotando en el exchange
        y puede ejecutarse horas después, cuando la señal que la justificó
        ya no existe. Es la forma más silenciosa de acabar con una
        posición que nadie decidió abrir.
        """
        if not self.live:
            return
        pendientes = self.state.data.get("pending", {})
        if not pendientes:
            return
        try:
            posiciones = await self.api.open_positions()
        except Exception as exc:  # noqa: BLE001
            log.warning("No se pudieron leer posiciones para las pendientes: %s", exc)
            return

        vivas = {
            str(p.get("symbol", "")): float(p.get("positionAmt", 0) or 0)
            for p in posiciones
            if float(p.get("positionAmt", 0) or 0) != 0
        }
        ahora = time.time()
        for symbol, pend in list(pendientes.items()):
            if symbol in vivas:
                # Se ejecutó: ahora sí es una posición.
                pend["opened_at"] = ahora
                self.state.data.setdefault("open", {})[symbol] = pend
                self.state.data["pending"].pop(symbol, None)
                self.state.save()
                await self.tg.send(f"✅ <b>{symbol}</b> ejecutada · la orden limitada entró")
                continue

            edad_min = (ahora - float(pend.get("opened_at", ahora))) / 60.0
            if edad_min >= config.LIMIT_TTL_MIN:
                try:
                    await self.api.cancel_open_orders(symbol)
                except Exception as exc:  # noqa: BLE001
                    log.warning("No se pudo cancelar la orden de %s: %s", symbol, exc)
                self.state.data["pending"].pop(symbol, None)
                self.state.save()
                await self.tg.send(
                    f"🚫 <b>{symbol}</b> cancelada · no se ejecutó en {config.LIMIT_TTL_MIN} min.\n"
                    f"No entrar es una decisión, no un fallo: el precio ya no es el de la señal."
                )

    async def reconcile(self) -> None:
        """
        BUG QUE ESTO ARREGLA: el bot abría posiciones y las guardaba en
        el estado, pero NADIE registraba los cierres. Cuando saltaba el
        SL o el TP en el exchange, el bot no se enteraba: la posición
        seguía "abierta" para siempre, bloqueando el hueco de
        MAX_CONCURRENT y dejando el circuit breaker sin contar ni una
        pérdida. En SIGNAL no se nota; en LIVE el bot se habría quedado
        mudo y bloqueado tras las primeras operaciones.

        El resultado (ganada/perdida) se estima comparando el último
        precio con la entrada. Es una APROXIMACIÓN — el fill real puede
        diferir — y sirve para el circuit breaker, no para contabilidad.
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

        vivos = {str(p.get("symbol", "")) for p in posiciones if float(p.get("positionAmt", 0) or 0) != 0}
        for symbol, pos in list(abiertas.items()):
            if symbol in vivos:
                continue
            # Ya no está en el exchange: se cerró por SL o por TP.
            try:
                velas = await self.api.klines(symbol, config.TIMEFRAME, limit=2)
                ultimo = velas[-1]["close"] if velas else pos["entry"]
            except Exception:  # noqa: BLE001
                ultimo = pos["entry"]
            if pos["side"] == "BUY":
                gano = ultimo > pos["entry"]
            else:
                gano = ultimo < pos["entry"]
            log.info("%s cerrada fuera del bot (%s)", symbol, "ganada" if gano else "perdida")
            await self.tg.send(
                f"{'✅' if gano else '🛑'} <b>{symbol}</b> cerrada · "
                f"{'objetivo' if gano else 'stop'}\n{self.stats_text()}"
            )
            self.register_close(symbol, gano)

    async def check_time_exits(self) -> None:
        """
        Cierra las posiciones que llevan demasiado tiempo abiertas.
        El SL y el TP los vigila el exchange; el reloj lo vigila el bot,
        porque el exchange no sabe nada de la ventana en la que la
        estrategia tiene sentido.
        """
        if not config.USE_TIME_EXIT:
            return
        limite = config.max_trade_seconds()
        ahora = time.time()
        for symbol, pos in list(self.state.data.get("open", {}).items()):
            edad = ahora - float(pos.get("opened_at", ahora))
            if edad < limite:
                continue
            if config.TIME_EXIT_ONLY_LOSING:
                try:
                    velas = await self.api.klines(symbol, config.TIMEFRAME, limit=2)
                    ultimo = velas[-1]["close"] if velas else None
                except Exception:  # noqa: BLE001
                    ultimo = None
                if ultimo is not None:
                    entrada = float(pos.get("entry", ultimo))
                    a_favor = ultimo > entrada if pos["side"] == "BUY" else ultimo < entrada
                    if a_favor:
                        log.info("%s pasa del límite pero va a favor: se deja correr", symbol)
                        continue
            log.info("%s lleva %.0f min abierta: cierre por tiempo", symbol, edad / 60)
            if self.live:
                try:
                    qty = float(pos.get("qty", 0))
                    if qty > 0:
                        await self.api.close_position(symbol, pos["side"], qty)
                except Exception as exc:  # noqa: BLE001
                    await self.tg.send(f"⚠️ No se pudo cerrar {symbol} por tiempo: {exc}")
                    continue
            self.state.data["open"].pop(symbol, None)
            self.state.save()
            await self.tg.send(
                f"⏱️ <b>{symbol}</b> cerrada por tiempo tras {edad / 60:.0f} min "
                f"(límite {limite // 60} min)\nLa vuelta no llegó: fuera de la ventana útil."
            )

    async def maybe_rank(self) -> None:
        """Ranking del universo completo, cada RANK_INTERVAL_MIN."""
        if not config.SCAN_ALL:
            return
        if time.time() - self.last_rank < config.RANK_INTERVAL_MIN * 60:
            return
        self.last_rank = time.time()
        rows = await self.scanner.run(self.symbols)
        self.last_rows = rows
        con_amplitud = [r for r in rows if r.verdict != "sin amplitud"]

        if not config.RANK_ONLY_WHEN_CANDIDATES and not con_amplitud:
            # Modo "top_siempre": aunque nadie sea operable, se manda la
            # lista de vigilancia con las mejores situadas.
            self.had_candidates = False
            await self.tg.send(scanner.format_watchlist(rows, config.RANK_TOP_N))
            return

        if config.RANK_ONLY_WHEN_CANDIDATES and not con_amplitud:
            # Nada que contar. Solo se avisa del cambio de estado: cuando
            # se pasa de tener candidatos a no tener ninguno.
            if self.had_candidates:
                await self.tg.send(
                    f"😴 Se acabaron los candidatos · {len(rows)} símbolos, "
                    f"ninguno con amplitud.\nSiguiente aviso cuando aparezca alguno."
                )
            self.had_candidates = False
            log.info("Ranking omitido: sin candidatos (evitando aviso repetido)")
            return

        self.had_candidates = bool(con_amplitud)
        await self.tg.send(scanner.format_ranking(rows, config.RANK_TOP_N))

    def in_cooldown(self) -> bool:
        return time.time() < float(self.state.data.get("cooldown_until", 0))

    async def scan_once(self) -> None:
        if self.in_cooldown():
            restante = int(float(self.state.data["cooldown_until"]) - time.time()) // 60
            log.info("En enfriamiento, %d min restantes", restante)
            return

        abiertas = len(self.state.data.get("open", {})) + len(self.state.data.get("pending", {}))
        if abiertas >= config.MAX_CONCURRENT:
            log.info("Límite de posiciones alcanzado (%d)", abiertas)
            return

        con_amplitud = 0
        for sym in self.symbols:
            try:
                velas = await self.api.klines(sym, config.TIMEFRAME, limit=300)
            except Exception as exc:  # noqa: BLE001
                log.debug("%s: sin velas (%s)", sym, exc)
                continue

            # Aviso de vigilancia: va ANTES de evaluar la señal, porque el
            # objetivo es avisar mientras el precio se estira — no cuando
            # ya se ha girado y la operación ha pasado.
            await self.maybe_watch(sym, velas)

            sig, motivo = strategy.evaluate(sym, velas)
            if not motivo.startswith("sin amplitud"):
                con_amplitud += 1
            if sig is None:
                log.debug("%s: %s", sym, motivo)
                continue
            if sym in self.state.data.get("open", {}) or sym in self.state.data.get("pending", {}):
                continue

            await self.handle_signal(sig)
            abiertas += 1
            if abiertas >= config.MAX_CONCURRENT:
                break
            await asyncio.sleep(0.2)  # respiro entre llamadas

        log.info(
            "Ciclo completo · %d símbolos, %d con amplitud suficiente",
            len(self.symbols),
            con_amplitud,
        )

    async def maybe_watch(self, symbol: str, velas: list[dict]) -> None:
        """Avisa de los símbolos que están A PUNTO, con enfriamiento."""
        if not config.WATCH_ALERTS:
            return
        if symbol in self.state.data.get("open", {}) or symbol in self.state.data.get("pending", {}):
            return

        # DEFENSIVO: si el repositorio tiene una versión de strategy.py
        # más antigua que main.py, esto reventaba el ciclo entero con
        # AttributeError. Un bot con dinero real NO puede caerse por un
        # desajuste de versiones entre dos archivos: se desactiva el
        # aviso, se avisa una vez, y lo demás sigue operando.
        fn = getattr(strategy, "watch_status", None)
        if fn is None:
            if not self.state.data.get("warned_watch_missing"):
                self.state.data["warned_watch_missing"] = True
                self.state.save()
                log.error("strategy.py desactualizado: falta watch_status()")
                await self.tg.send(
                    "⚠️ <b>Archivos descoordinados</b>\n"
                    "El <code>strategy.py</code> del repositorio es más antiguo que "
                    "<code>main.py</code> (falta <code>watch_status</code>).\n"
                    "Los avisos de vigilancia quedan desactivados; el resto sigue "
                    "funcionando. Sube el strategy.py actualizado."
                )
            return
        estado = fn(velas)
        if not estado:
            return
        cerca, stretch, atr_pct, lado = estado
        if not cerca:
            return

        ultimo = float(self.state.data.setdefault("watch", {}).get(symbol, 0))
        if time.time() - ultimo < config.WATCH_COOLDOWN_MIN * 60:
            return
        self.state.data["watch"][symbol] = time.time()
        self.state.save()

        base = symbol.split("-")[0]
        falta = config.STRETCH_ATR - abs(stretch)
        await self.tg.send(
            f"👀 <b>{base}</b> se está estirando\n"
            f"Estiramiento <b>{stretch:+.2f}</b> de {config.STRETCH_ATR:.1f} ATR "
            f"(faltan {falta:.2f})\n"
            f"ATR {atr_pct:.2f}%  ·  dirección probable: <b>{lado}</b>\n"
            f"<i>Aún no hay señal. Buen momento para abrirlo en TradingView "
            f"y pasarle el script.</i>"
        )

    async def handle_signal(self, sig: strategy.Signal) -> None:
        log.info(
            "SEÑAL %s %s entrada=%.8g sl=%.8g tp=%.8g rr=%.2f atr=%.2f%%",
            sig.side, sig.symbol, sig.entry, sig.sl, sig.tp, sig.rr, sig.atr_pct,
        )

        if not self.live:
            await self.tg.send(fmt_signal(sig, live=False))
            self.state.data.setdefault("last_signal", {})[sig.symbol] = time.time()
            self.state.save()
            return

        try:
            equity = await self.api.balance_usdt()
            if equity <= 0:
                await self.tg.send(f"⚠️ Señal en {sig.symbol} sin ejecutar: saldo 0")
                return
            qty = strategy.position_size(equity, sig.entry, sig.sl)
            qty = self.api.round_qty(sig.symbol, qty)
            minimo = self.api.min_qty(sig.symbol)
            if qty <= 0 or (minimo > 0 and qty < minimo):
                await self.tg.send(
                    f"⚠️ Señal en {sig.symbol} sin ejecutar: tamaño {qty} "
                    f"por debajo del mínimo del contrato ({minimo}).\n"
                    f"Con {config.RISK_PCT}% de riesgo y este stop no da para el lote mínimo."
                )
                return

            # Segunda comprobación, contra el EXCHANGE y no contra el
            # estado propio: si una posición se abrió fuera del bot o el
            # estado se perdió, abrir otra sería doblar el riesgo sin
            # enterarse.
            try:
                vivas = await self.api.open_positions()
                if any(
                    str(p.get("symbol")) == sig.symbol
                    and float(p.get("positionAmt", 0) or 0) != 0
                    for p in vivas
                ):
                    log.warning("%s ya tiene posición en el exchange: no se abre otra", sig.symbol)
                    return
            except Exception as exc:  # noqa: BLE001
                log.warning("No se pudo comprobar posiciones de %s: %s", sig.symbol, exc)
                return
            await self.api.set_leverage(sig.symbol, "LONG" if sig.side == "BUY" else "SHORT", config.LEVERAGE)
            # Identificador propio para poder comprobar después si la
            # orden existe cuando la respuesta se pierda por el camino.
            client_id = f"rev{sig.symbol.split('-')[0][:6]}{int(time.time())}"
            if config.ENTRY_TYPE == "LIMIT":
                # Se pide un pelín MEJOR que el precio actual: en un libro
                # fino no entrar es mejor que entrar a cualquier precio.
                ajuste = 1 + config.LIMIT_OFFSET_PCT / 100.0
                precio = self.api.round_price(sig.symbol, sig.entry * (ajuste if sig.side == "SELL" else 2 - ajuste))
                sl_r = self.api.round_price(sig.symbol, sig.sl)
                tp_r = self.api.round_price(sig.symbol, sig.tp)
                await self.api.limit_order(sig.symbol, sig.side, qty, precio, sl_r, tp_r, client_id)
            else:
                sl_r = self.api.round_price(sig.symbol, sig.sl)
                tp_r = self.api.round_price(sig.symbol, sig.tp)
                await self.api.market_order(sig.symbol, sig.side, qty, sl_r, tp_r, client_id)
        except BingXError as exc:
            await self.tg.send(f"❌ BingX rechazó la orden en {sig.symbol}: {exc}")
            return
        except Exception as exc:  # noqa: BLE001
            # LA RESPUESTA SE PERDIÓ, PERO LA ORDEN PUEDE EXISTIR.
            # Es el fallo clásico de los bots en producción: la petición
            # llega al exchange y la respuesta no vuelve. Darla por
            # fallida sin comprobar lleva a abrir una segunda encima.
            existe = await self.api.order_exists(sig.symbol, client_id)
            if existe:
                await self.tg.send(
                    f"⚠️ <b>{sig.symbol}</b>: fallo de red al enviar, pero la orden SÍ existe "
                    f"en el exchange.\nSe registra como abierta.\n<code>{exc}</code>"
                )
                log.warning("%s: respuesta perdida pero la orden existe", sig.symbol)
            else:
                await self.tg.send(
                    f"❌ Error al ejecutar {sig.symbol} y la orden NO existe "
                    f"en el exchange: {exc}"
                )
                return

        # BUG QUE ESTO ARREGLA: con ENTRY_TYPE=LIMIT la orden PUEDE NO
        # EJECUTARSE, y el bot la daba por abierta igualmente. Consecuencias
        # en real: el hueco de MAX_CONCURRENT bloqueado por una posición que
        # no existe, y reconcile() contando después un cierre inventado —
        # con su ganancia o pérdida ficticia alimentando el circuit breaker.
        # Ahora una orden limitada entra como PENDIENTE y solo pasa a
        # posición cuando el exchange confirma que hay algo abierto.
        es_limite = config.ENTRY_TYPE == "LIMIT"
        registro = {
            "side": sig.side,
            "entry": sig.entry,
            "sl": sig.sl,
            "tp": sig.tp,
            "qty": qty,
            "opened_at": time.time(),
        }
        if es_limite:
            self.state.data.setdefault("pending", {})[sig.symbol] = registro
        else:
            self.state.data.setdefault("open", {})[sig.symbol] = registro
        self.state.save()
        await self.tg.send(
            fmt_signal(sig, live=True)
            + (f"\n⏳ Orden limitada enviada · caduca en {config.LIMIT_TTL_MIN} min" if es_limite else "")
        )

    def register_close(self, symbol: str, won: bool) -> None:
        """
        Lo llama el reconciliador cuando detecta una posición cerrada.
        El circuit breaker cuenta RACHAS, no dinero: el bot no lleva la
        contabilidad en euros del exchange y fingir un drawdown en % con
        datos que no tiene sería inventarse una cifra.
        """
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
                asyncio.create_task(
                    self.tg.send(
                        f"⏸️ <b>Circuit breaker</b>\n"
                        f"{config.MAX_CONSECUTIVE_LOSSES} pérdidas seguidas · "
                        f"pausa de {config.COOLDOWN_MINUTES} min.\n"
                        f"No es un fallo: es el bot dejando de insistir."
                    )
                )
        d.get("open", {}).pop(symbol, None)
        self.state.save()


async def main() -> None:
    bot = Bot()
    try:
        await bot.start()
    finally:
        await bot.client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
