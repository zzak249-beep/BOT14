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
import liquidations
import rsi_confirm
import score as scoring
import stats
from bingx import BingX, BingXError
from notify import State, Telegram

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# httpx registra en INFO CADA petición. Con ~940 símbolos por ciclo y un
# ciclo cada pocos minutos, son decenas de miles de líneas al día que
# entierran lo único que importa leer: los ciclos, las señales y los
# errores. Se silencia salvo que se pida DEBUG a propósito.
if config.LOG_LEVEL != "DEBUG":
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

log = logging.getLogger("bot")


def fmt_signal(
    sig: strategy.Signal,
    live: bool,
    cascade: dict | None = None,
    rsi_result: "rsi_confirm.RsiConfirm | None" = None,
    bias30m: str | None = None,
    score: "scoring.EntryScore | None" = None,
) -> str:
    lado = "LARGO" if sig.side == "BUY" else "CORTO"
    cabecera = "🟢 EJECUTADO" if live else "🔔 SEÑAL"
    texto = (
        f"{cabecera} · {lado} <b>{sig.symbol}</b>\n"
        f"Entrada <code>{sig.entry:.8g}</code>\n"
        f"SL <code>{sig.sl:.8g}</code>  ·  TP <code>{sig.tp:.8g}</code>\n"
        f"R:R {sig.rr:.2f}  ·  estirón {sig.stretch:+.2f} ATR\n"
        f"ATR {sig.atr_pct:.2f}%  ·  {sig.cost_cover:.0f}× el coste"
    )
    if cascade and cascade["activa"] and liquidations.cascade_confirms(sig.side, cascade["lado"]):
        texto += (
            f"\n🔥 Confirmada por cascada: {cascade['multiplicador']:.1f}× lo normal, "
            f"{cascade['n_eventos']} liquidaciones de {cascade['lado'].lower()}"
        )
    if rsi_result is not None:
        if rsi_confirm.confirms(sig.side, rsi_result):
            texto += (
                f"\n📈 RSI confirma: doble "
                f"{'suelo' if sig.side == 'BUY' else 'techo'} hace "
                f"{rsi_result.velas_desde_señal} vela(s) (RSI {rsi_result.rsi_actual:.0f})"
            )
        else:
            texto += f"\n📉 RSI sin confirmar (RSI {rsi_result.rsi_actual:.0f}) — solo informativo"
    if bias30m and bias30m != "NEUTRAL":
        texto += f"\n🧭 A favor de la tendencia de 30m ({bias30m.lower()})"
    if score is not None:
        texto += f"\n🎯 {scoring.format_breakdown(score)}"
    return texto


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
        self.liq = liquidations.LiquidationTracker() if config.LIQUIDATIONS_ENABLED else None
        self.radar30 = scanner.Scanner(self.api, timeframe=config.RADAR30M_TIMEFRAME) if config.RADAR30M_ENABLED else None
        self.bias30m: dict[str, str] = {}
        self.last_radar30 = 0.0

    async def start(self) -> None:
        log.info("Modo: %s", config.describe())
        await self.tg.send(
            f"🤖 <b>Bot de reversión iniciado</b>\n"
            f"{config.describe()}\n"
            f"Filtro de amplitud: ATR ≥ {config.MIN_ATR_PCT}% y ≥ {config.MIN_COST_COVER:.0f}× el coste\n"
            f"Riesgo por operación: {config.RISK_PCT}%\n"
            f"Cascadas de liquidación: {'activadas (Binance + Bybit)' if self.liq else 'desactivadas'}\n"
            f"RSI doble cruce: {'EXIGIDO' if (config.RSI_CONFIRM_ENABLED and config.RSI_REQUIRE) else ('informativo' if config.RSI_CONFIRM_ENABLED else 'desactivado')}\n"
            f"Radar 30m (contra-tendencia): {'activado' if self.radar30 else 'desactivado'}"
        )
        await self.refresh_symbols()
        if self.liq:
            self.liq.set_symbols(self.symbols)
            self.liq.start()
        while True:
            try:
                await self.maybe_xsection()
                await self.reconcile()
                await self.reconcile_signal()
                await self.maybe_daily_summary()
                await self.maybe_heartbeat()
                await self.check_time_exits()
                await self.maybe_radar30m()
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
            + (f"\nLiquidaciones: {self.liq.status}" if self.liq else "")
        )
        if not entregado:
            log.error("El resumen diario NO se entregó: revisa las credenciales de Telegram")

        informe = self.build_expectancy_report()
        if informe:
            await self.tg.send(informe)

        franjas = stats.buckets_por_score(self.state.data.get("trades", []))
        if franjas:
            await self.tg.send(franjas)

    def build_expectancy_report(self) -> str | None:
        """
        Arma {'SIGNAL': [...], 'LIVE': [...]} a partir del historial
        guardado y delega en stats.format_report(). Separado por modo
        a propósito: SIGNAL no paga slippage ni comisión real, LIVE sí
        — mezclarlos escondería justo la diferencia que el README
        avisa que va a doler.
        """
        trades = self.state.data.get("trades", [])
        if not trades:
            return None
        rs_por_modo: dict[str, list[float]] = {}
        for t in trades:
            modo = t.get("mode", "LIVE")
            rs_por_modo.setdefault(modo, []).append(float(t["r"]))
        return stats.format_report(rs_por_modo)

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
            r = stats.compute_r(pos["entry"], pos["sl"], pos["side"], ultimo)
            gano = r > 0
            log.info("%s cerrada fuera del bot (%s, %+.2fR)", symbol, "ganada" if gano else "perdida", r)
            await self.tg.send(
                f"{'✅' if gano else '🛑'} <b>{symbol}</b> cerrada · "
                f"{'objetivo' if gano else 'stop'} · {r:+.2f}R\n{self.stats_text()}"
            )
            self.register_close_r(symbol, pos, ultimo, reason=("objetivo" if gano else "stop"), mode=pos.get("mode", "LIVE"))

    async def check_time_exits(self) -> None:
        """
        Cierra las posiciones que llevan demasiado tiempo abiertas.
        El SL y el TP los vigila el exchange; el reloj lo vigila el bot,
        porque el exchange no sabe nada de la ventana en la que la
        estrategia tiene sentido.

        ANTES esto hacía pop() del estado sin llamar a
        register_close_r(): la operación desaparecía sin dejar rastro
        en wins/losses ni en el historial de R. Con MAX_TRADE_BARS de
        por medio, buena parte de los cierres pasan por aquí — sin
        registrarlos, las estadísticas de rentabilidad estaban
        incompletas desde el principio.
        """
        if not config.USE_TIME_EXIT:
            return
        limite = config.max_trade_seconds()
        ahora = time.time()
        for symbol, pos in list(self.state.data.get("open", {}).items()):
            edad = ahora - float(pos.get("opened_at", ahora))
            if edad < limite:
                continue

            try:
                velas = await self.api.klines(symbol, config.TIMEFRAME, limit=2)
                ultimo = velas[-1]["close"] if velas else None
            except Exception:  # noqa: BLE001
                ultimo = None

            if config.TIME_EXIT_ONLY_LOSING and ultimo is not None:
                entrada = float(pos.get("entry", ultimo))
                a_favor = ultimo > entrada if pos["side"] == "BUY" else ultimo < entrada
                if a_favor:
                    log.info("%s pasa del límite pero va a favor: se deja correr", symbol)
                    continue

            log.info("%s lleva %.0f min abierta: cierre por tiempo", symbol, edad / 60)
            if self.live and pos.get("mode") != "SIGNAL":
                try:
                    qty = float(pos.get("qty", 0))
                    if qty > 0:
                        await self.api.close_position(symbol, pos["side"], qty)
                except Exception as exc:  # noqa: BLE001
                    await self.tg.send(f"⚠️ No se pudo cerrar {symbol} por tiempo: {exc}")
                    continue

            exit_price = ultimo if ultimo is not None else pos["entry"]
            r = stats.compute_r(pos["entry"], pos["sl"], pos["side"], exit_price)
            await self.tg.send(
                f"⏱️ <b>{symbol}</b> cerrada por tiempo tras {edad / 60:.0f} min "
                f"(límite {limite // 60} min) · {r:+.2f}R\n"
                f"La vuelta no llegó: fuera de la ventana útil."
            )
            self.register_close_r(symbol, pos, exit_price, reason="tiempo", mode=pos.get("mode", "LIVE"))

    async def reconcile_signal(self) -> None:
        """
        En SIGNAL no hay posición real en el exchange que vigilar — el
        SL y el TP son solo dos números guardados en el estado. SIN
        ESTE MÉTODO, una señal en SIGNAL no se cerraba NUNCA salvo por
        el límite de tiempo: si el precio tocaba el stop a los cinco
        minutos, el bot no se enteraba hasta MAX_TRADE_BARS después (o
        nunca, si iba a favor). Wins/losses se quedaban a cero para
        siempre y era imposible saber si el sistema es rentable — que
        es exactamente el problema que esto soluciona.
        """
        abiertas = self.state.data.get("open", {})
        objetivo = [(sym, pos) for sym, pos in abiertas.items() if pos.get("mode") == "SIGNAL"]
        for symbol, pos in objetivo:
            try:
                velas = await self.api.klines(symbol, config.TIMEFRAME, limit=3)
            except Exception as exc:  # noqa: BLE001
                log.debug("%s: no se pudo comprobar SL/TP en SIGNAL (%s)", symbol, exc)
                continue
            if len(velas) < 2:
                continue
            # Se ignora la vela en curso — igual que en strategy.py, solo
            # cuentan velas cerradas. Se miran las 2 últimas cerradas por
            # si el ciclo se saltó una vela entre comprobaciones.
            cerradas = velas[:-1][-2:]
            entry, sl, tp, side = pos["entry"], pos["sl"], pos["tp"], pos["side"]
            for vela in cerradas:
                if side == "BUY":
                    tp_hit = vela["high"] >= tp
                    sl_hit = vela["low"] <= sl
                else:
                    tp_hit = vela["low"] <= tp
                    sl_hit = vela["high"] >= sl
                if not tp_hit and not sl_hit:
                    continue
                # Si la misma vela toca los dos niveles, se asume el
                # PEOR caso (SL primero): es la convención conservadora
                # — sin datos de tick no hay forma de saber el orden
                # real dentro de la vela.
                if sl_hit:
                    exit_price, reason = sl, "stop"
                else:
                    exit_price, reason = tp, "objetivo"
                r = stats.compute_r(entry, sl, side, exit_price)
                await self.tg.send(
                    f"{'✅' if r > 0 else '🛑'} <b>{symbol}</b> "
                    f"{'objetivo' if r > 0 else 'stop'} (SIGNAL) · {r:+.2f}R\n{self.stats_text()}"
                )
                self.register_close_r(symbol, pos, exit_price, reason=reason, mode="SIGNAL")
                break

    async def maybe_radar30m(self) -> None:
        """
        Escaneo aparte, en 30m, EXCLUSIVAMENTE para saber si hay
        tendencia de fondo. No genera señales ni avisos propios — solo
        actualiza self.bias30m, que scan_once() consulta para bloquear
        entradas de 5m a contra-tendencia.
        """
        if not self.radar30:
            return
        if time.time() - self.last_radar30 < config.RADAR30M_INTERVAL_MIN * 60:
            return
        self.last_radar30 = time.time()
        try:
            rows = await self.radar30.run(self.symbols)
        except Exception as exc:  # noqa: BLE001
            log.warning("Radar 30m falló (%s): se mantiene el sesgo anterior", exc)
            return
        nuevo_bias = {r.symbol: scanner.bias_from_row(r) for r in rows}
        con_tendencia = sum(1 for v in nuevo_bias.values() if v != "NEUTRAL")
        log.info("Radar 30m: %d símbolos, %d con tendencia de fondo marcada", len(rows), con_tendencia)
        self.bias30m = nuevo_bias

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

        # Las favoritas: no es lo mismo que el ranking. El ranking ordena
        # por amplitud aunque falte la vela de agotamiento; esto es
        # exactamente lo que strategy.evaluate() aceptaría abrir AHORA.
        favoritas = scanner.format_favorites(
            rows, config.RANK_TOP_N,
            cascade_lookup=self.liq.cascade_status if self.liq else None,
        )
        if favoritas:
            await self.tg.send(favoritas)

    def in_cooldown(self) -> bool:
        return time.time() < float(self.state.data.get("cooldown_until", 0))

    def _priority_order(self) -> list[str]:
        """
        Antes se recorría self.symbols en el orden que devuelve la API
        de BingX — arbitrario para lo que importa aquí. Con
        MAX_CONCURRENT limitado y corte en cuanto se llenan los
        huecos, ese orden decidía en la práctica qué señal se tomaba:
        la primera que apareciera, no la mejor.

        Se reordena usando la amplitud (cover) del último ranking del
        universo (self.last_rows, actualizado cada RANK_INTERVAL_MIN) —
        dato que YA se calculó, sin llamadas extra a la API. Los
        símbolos sin dato (antes del primer ranking, o que fallaron esa
        vez) van al final, en su orden original, para no perderlos.
        """
        cover = {r.symbol: r.cover for r in self.last_rows}
        con_dato = sorted((s for s in self.symbols if s in cover), key=lambda s: cover[s], reverse=True)
        sin_dato = [s for s in self.symbols if s not in cover]
        return con_dato + sin_dato

    async def scan_once(self) -> None:
        if self.in_cooldown():
            restante = int(float(self.state.data["cooldown_until"]) - time.time()) // 60
            log.info("En enfriamiento, %d min restantes", restante)
            return

        abiertas = len(self.state.data.get("open", {}))
        if abiertas >= config.MAX_CONCURRENT:
            log.info("Límite de posiciones alcanzado (%d)", abiertas)
            return

        con_amplitud = 0
        for sym in self._priority_order():
            try:
                velas = await self.api.klines(sym, config.TIMEFRAME, limit=300)
            except Exception as exc:  # noqa: BLE001
                log.debug("%s: sin velas (%s)", sym, exc)
                continue

            sig, motivo = strategy.evaluate(sym, velas)
            if not motivo.startswith("sin amplitud"):
                con_amplitud += 1
            if sig is None:
                log.debug("%s: %s", sym, motivo)
                continue
            if sym in self.state.data.get("open", {}):
                continue

            # Filtro de contra-tendencia (radar 30m). Solo bloquea
            # cuando hay un sesgo CLARO — símbolo sin dato o en rango
            # en 30m pasa igual, para no bloquear todo por falta de
            # información. Es el mismo patrón que ya se midió como
            # principal fuente de pérdidas: largos a contra-tendencia
            # en mercado bajista. Esto sigue siendo un bloqueo DURO,
            # no algo que el score pueda compensar sumando puntos.
            bias = self.bias30m.get(sym, "NEUTRAL")
            if (bias == "BAJISTA" and sig.side == "BUY") or (bias == "ALCISTA" and sig.side == "SELL"):
                log.info("%s: señal %s bloqueada — contra-tendencia de 30m (%s)", sym, sig.side, bias)
                continue

            # Confirmación por RSI de doble cruce, sobre las MISMAS
            # velas de 5m — sin llamadas extra a la API.
            rsi_result = rsi_confirm.evaluate(
                velas,
                length=config.RSI_LENGTH,
                sig_length=config.RSI_SIGNAL_LENGTH,
                trigger=config.RSI_TRIGGER,
                target_count=config.RSI_TARGET_CROSSES,
                ventana=config.RSI_CONFIRM_BARS,
            ) if config.RSI_CONFIRM_ENABLED else None

            if config.RSI_CONFIRM_ENABLED and config.RSI_REQUIRE:
                if not rsi_confirm.confirms(sig.side, rsi_result):
                    log.info("%s: señal %s sin confirmar por RSI — descartada", sym, sig.side)
                    continue

            cascade = self.liq.cascade_status(sym) if self.liq else None

            score_result = scoring.compute(sig, rsi_result, cascade, bias) if config.SCORE_ENABLED else None
            if score_result and config.SCORE_MIN > 0 and score_result.total < config.SCORE_MIN:
                log.info(
                    "%s: score %.0f por debajo del mínimo (%.0f) — descartada",
                    sym, score_result.total, config.SCORE_MIN,
                )
                continue

            await self.handle_signal(sig, rsi_result=rsi_result, bias30m=bias, cascade=cascade, score=score_result)
            abiertas += 1
            if abiertas >= config.MAX_CONCURRENT:
                break
            await asyncio.sleep(0.2)  # respiro entre llamadas

        log.info(
            "Ciclo completo · %d símbolos, %d con amplitud suficiente",
            len(self.symbols),
            con_amplitud,
        )

    async def handle_signal(
        self,
        sig: strategy.Signal,
        rsi_result: "rsi_confirm.RsiConfirm | None" = None,
        bias30m: str | None = None,
        cascade: dict | None = None,
        score: "scoring.EntryScore | None" = None,
    ) -> None:
        log.info(
            "SEÑAL %s %s entrada=%.8g sl=%.8g tp=%.8g rr=%.2f atr=%.2f%%%s",
            sig.side, sig.symbol, sig.entry, sig.sl, sig.tp, sig.rr, sig.atr_pct,
            f" score={score.total:.0f}" if score else "",
        )

        if not self.live:
            await self.tg.send(
                fmt_signal(sig, live=False, cascade=cascade, rsi_result=rsi_result, bias30m=bias30m, score=score)
            )
            self.state.data.setdefault("open", {})[sig.symbol] = {
                "mode": "SIGNAL",
                "side": sig.side,
                "entry": sig.entry,
                "sl": sig.sl,
                "tp": sig.tp,
                "qty": 0,
                "opened_at": time.time(),
                "score": score.total if score else None,
            }
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
            if config.ENTRY_TYPE == "LIMIT":
                # Se pide un pelín MEJOR que el precio actual: en un libro
                # fino no entrar es mejor que entrar a cualquier precio.
                ajuste = 1 + config.LIMIT_OFFSET_PCT / 100.0
                precio = self.api.round_price(sig.symbol, sig.entry * (ajuste if sig.side == "SELL" else 2 - ajuste))
                sl_r = self.api.round_price(sig.symbol, sig.sl)
                tp_r = self.api.round_price(sig.symbol, sig.tp)
                await self.api.limit_order(sig.symbol, sig.side, qty, precio, sl_r, tp_r)
            else:
                sl_r = self.api.round_price(sig.symbol, sig.sl)
                tp_r = self.api.round_price(sig.symbol, sig.tp)
                await self.api.market_order(sig.symbol, sig.side, qty, sl_r, tp_r)
        except BingXError as exc:
            await self.tg.send(f"❌ BingX rechazó la orden en {sig.symbol}: {exc}")
            return
        except Exception as exc:  # noqa: BLE001
            await self.tg.send(f"❌ Error al ejecutar {sig.symbol}: {exc}")
            return

        self.state.data.setdefault("open", {})[sig.symbol] = {
            "mode": "LIVE",
            "side": sig.side,
            "entry": sig.entry,
            "sl": sig.sl,
            "tp": sig.tp,
            "qty": qty,
            "opened_at": time.time(),
            "score": score.total if score else None,
        }
        self.state.save()
        await self.tg.send(fmt_signal(sig, live=True, cascade=cascade, rsi_result=rsi_result, bias30m=bias30m, score=score))

    def register_close_r(self, symbol: str, pos: dict, exit_price: float, reason: str, mode: str) -> None:
        """
        Registra el cierre con el R conseguido, no solo si ganó o
        perdió. El circuit breaker sigue contando RACHAS igual que
        antes — el bot no lleva contabilidad en euros del exchange y
        fingir un drawdown en % con datos que no tiene sería
        inventarse una cifra. Lo nuevo es el historial en R
        (state.data['trades']), que es lo único que permite calcular
        expectativa y profit factor de verdad — ver stats.py.
        """
        r = stats.compute_r(pos["entry"], pos["sl"], pos["side"], exit_price)
        won = r > 0
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

        # Historial en R, acotado a las últimas 1000 para que
        # state.json no crezca sin límite con el tiempo.
        historial = d.setdefault("trades", [])
        historial.append(
            {
                "symbol": symbol,
                "side": pos["side"],
                "r": round(r, 4),
                "reason": reason,
                "mode": mode,
                "score": pos.get("score"),
                "closed_at": time.time(),
            }
        )
        if len(historial) > 1000:
            del historial[: len(historial) - 1000]

        self.state.save()


async def main() -> None:
    bot = Bot()
    try:
        await bot.start()
    finally:
        if bot.liq:
            await bot.liq.stop()
        await bot.client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
