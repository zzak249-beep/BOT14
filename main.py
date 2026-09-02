"""
Bot Wavelet MRA — BingX.

Descomposición multiescala causal + cruce sobre la aproximación, con el
filtro de régimen corregido (energía normalizada por escala).

Hereda toda la infraestructura ya probada en producción: margen
aislado, límite global de posiciones, pérdida diaria máxima,
reconciliación contra el exchange, diario de operaciones reales,
verificación tras respuesta perdida y redondeo a la precisión del
contrato.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import logging
import os
import time

import httpx

import config
import funding
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
    "LOOKBACK_ENERGY": 40, "APPROX_LEN": 8, "ATR_LEN": 14,
    "NORMALIZE_SCALES": True, "DOMINANCE_THRESHOLD": 1.30,
    "ALLOW_LONG": True, "ALLOW_SHORT": True,
    "USE_VOL_FILTER": True, "VOL_LEN": 20, "VOL_MULT": 1.2,
    "USE_HTF_FILTER": True, "HTF_MA_LEN": 200,
    "USE_TRAILING": False, "TRAIL_ATR": 2.0, "TRAIL_START_R": 1.0,
    "SL_ATR": 1.5, "TP_ATR": 2.5, "MAX_TRADE_MINUTES": 120,
    "USE_TIME_EXIT": True, "TIME_EXIT_ONLY_LOSING": True,
    "COST_ROUNDTRIP_PCT": 0.25, "MIN_ATR_PCT": 0.5, "MIN_COST_COVER": 6.0,
    "MAX_COST_IN_R": 0.20, "MAX_RISK_PCT": 4.0,
    "MIN_QUOTE_VOLUME_24H": 2_000_000.0, "TIMEFRAME": "5m",
    "SCAN_INTERVAL_SEC": 60, "MAX_SYMBOLS": 400, "SCAN_CONCURRENCY": 8,
    "SYMBOL_WHITELIST": [], "EXCLUDE_PREFIXES": ["NC"],
    "RISK_PCT": 0.5, "MAX_CONCURRENT": 1, "MAX_TOTAL_POSITIONS": 3,
    "LEVERAGE": 2, "MARGIN_MODE": "ISOLATED",
    "MAX_CONSECUTIVE_LOSSES": 3, "COOLDOWN_MINUTES": 120,
    "MAX_DAILY_LOSS_R": 3.0, "COOLDOWN_BARS": 4,
    "ENTRY_TYPE": "LIMIT", "LIMIT_OFFSET_PCT": 0.05, "LIMIT_TTL_MIN": 10,
    "SIGNAL_COOLDOWN_MIN": 60, "WATCHLIST_MIN": 30,
    "DAILY_SUMMARY": True, "DAILY_SUMMARY_HOUR_UTC": 7,
    "HEARTBEAT_HOURS": 12, "IDLE_ALERT_DAYS": 5, "ZOMBIE_ALERT_HOURS": 6, "BTC_CONTEXT": True, "FUNDING_ALERTS": True, "FUNDING_EXTREMO": 0.05,
    "FUNDING_ALERT_MIN": 120, "CARRY_MAX_DIAS_COBERTURA": 3.0, "SALDO_ESTIMADO": 135.0, "BTC_FILTER": False, "BTC_MIN_24H": -3.0,
    "TIMEFRAMES": ["5m"], "MIN_RISK_PCT": 0.0,
    "STATE_PATH": "/data/state_wavelet.json", "LOG_LEVEL": "INFO",
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
    lado = "LARGO" if sig.side == "BUY" else "CORTO"
    nombre = sig.symbol.split("-")[0]
    partes = [
        f"{cabecera} · {lado} <b>{nombre}</b>  (wavelet MRA)",
        f"Entrada <code>{sig.entry:.8g}</code>",
        f"SL <code>{sig.sl:.8g}</code>  ·  TP <code>{sig.tp:.8g}</code>",
        f"Riesgo {sig.riesgo_pct:.2f}%  ·  coste {sig.coste_r:.2f} R",
        f"Dominancia {sig.ratio:.2f} (umbral {sig.umbral:.2f}) · h8 {sig.h8:+.4f}",
        f"ATR {sig.atr_pct:.2f}% · {getattr(sig, 'timeframe', config.TIMEFRAME)}"
        + (f" · BTC {sig.btc_24h:+.1f}% 24h" if getattr(sig, "btc_24h", None) is not None else ""),
    ]
    if getattr(sig, "funding", None) is not None:
        partes.append(
            f"Funding {sig.funding:+.4f}%/8h — {funding.sesgo(sig.funding)}"
        )
    return chr(10).join(partes)


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
        self.funding: dict[str, float] = {}
        self._pendientes_aviso: list = []
        self.last_funding_alert = 0.0
        self.btc_ts = 0.0
        self.sem = asyncio.Semaphore(config.SCAN_CONCURRENCY)
        self.journal = journal.Journal(
            os.path.join(os.path.dirname(config.STATE_PATH) or "/data", "operaciones_wavelet.csv")
        )

    async def start(self) -> None:
        faltan = ensure_config()
        if faltan:
            log.error("config.py desactualizado, faltaban: %s", ", ".join(faltan))
        log.info("Modo: %s", config.describe())
        await self.tg.send(
            "🤖 <b>Bot Wavelet MRA iniciado</b>" + chr(10)
            + config.describe() + chr(10)
            + f"Dominancia ≥{config.DOMINANCE_THRESHOLD} "
            + ("(normalizada por escala)" if config.NORMALIZE_SCALES else "(SIN normalizar — modo original)") + chr(10)
            + f"Cruce sobre SMA({config.APPROX_LEN}) con la escala gruesa a favor" + chr(10)
            + f"Timeframe {config.TIMEFRAME} · riesgo {config.RISK_PCT}% · "
            + f"SL {config.SL_ATR} ATR / TP {config.TP_ATR} ATR"
        )
        await self.refresh_symbols()
        while True:
            try:
                await self.reconcile()
                await self.maybe_watchlist()
                await self.maybe_funding()
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
            sig.funding = self.funding.get(sym)
            ejecutada = await self.handle_signal(sig, hay_hueco)
            if ejecutada:
                abiertas += 1
                hay_hueco = abiertas < config.MAX_CONCURRENT

        await self.volcar_avisos()
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
        log.info("SEÑAL %s %s entrada=%.8g sl=%.8g ratio=%.2f", sig.symbol, sig.side, sig.entry, sig.sl, sig.ratio)

        # AVISO — siempre, con enfriamiento para no repetir cada ciclo.
        ultimos = self.state.data.setdefault("last_signal", {})
        previo = float(ultimos.get(sig.symbol, 0) or 0)
        if time.time() - previo >= config.SIGNAL_COOLDOWN_MIN * 60:
            ultimos[sig.symbol] = time.time()
            self.state.save()
            # Las señales que NO se pueden ejecutar se agrupan en un
            # solo mensaje por ciclo. Cinco avisos idénticos en el mismo
            # minuto (pasó: 1920 mensajes sin leer) no informan cinco
            # veces, enseñan a ignorar el chat.
            if not hay_hueco or not self.live:
                self._pendientes_aviso.append(sig)
                return False

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

        client_id = f"wav{sig.symbol.split('-')[0][:6]}{int(time.time())}"
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
            # Límite global: posiciones de TODA la cuenta, no solo de este bot.
            # En un desplome las alts se mueven juntas, así que lo que importa
            # es la exposición total y no cuántas abrió cada bot por su lado.
            n_total = sum(1 for p in vivas if float(p.get("positionAmt", 0) or 0) != 0)
            if n_total >= config.MAX_TOTAL_POSITIONS:
                await self.tg.send(
                    f"⚠️ <b>{sig.symbol}</b> sin abrir: ya hay <b>{n_total}</b> posiciones "
                    f"en la cuenta (límite global {config.MAX_TOTAL_POSITIONS}).\n"
                    f"<i>Puede haberlas abierto otro bot. En una caída las alts se mueven "
                    f"juntas, así que el riesgo se suma aunque los símbolos difieran.</i>"
                )
                return False
            if any(str(p.get("symbol")) == sig.symbol and float(p.get("positionAmt", 0) or 0) != 0 for p in vivas):
                return False

            await self.api.set_margin_mode(sig.symbol, config.MARGIN_MODE)

            await self.api.set_leverage(sig.symbol, "LONG" if sig.side == "BUY" else "SHORT", config.LEVERAGE)
            sl_r = self.api.round_price(sig.symbol, sig.sl)
            tp_r = self.api.round_price(sig.symbol, sig.tp) if sig.tp else sl_r * 100
            if config.ENTRY_TYPE == "LIMIT":
                ajuste = config.LIMIT_OFFSET_PCT / 100.0
                precio = self.api.round_price(sig.symbol, sig.entry * ((1 + ajuste) if sig.side == "SELL" else (1 - ajuste)))
                await self.api.limit_order(sig.symbol, sig.side, qty, precio, sl_r, tp_r, client_id)
            else:
                await self.api.market_order(sig.symbol, sig.side, qty, sl_r, tp_r, client_id)
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
            "side": sig.side, "entry": sig.entry, "sl": sig.sl, "sl_inicial": sig.sl, "qty": qty,
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
            if not w or not w["dominante"]:
                continue
            if w["atr_pct"] >= config.MIN_ATR_PCT:
                cerca.append((sym, w))

        if not cerca:
            log.info("Vigilancia: ningún símbolo en régimen dominante")
            return

        # Ordenadas por cercanía al cruce: el precio pegado a su
        # aproximación es el que está a punto de cruzarla.
        cerca.sort(key=lambda t: abs(t[1]["dist_aprox"]))
        lineas = ["👀 <b>En régimen dominante — vigilando</b> (" + str(len(cerca)) + ")" + chr(10)]
        for sym, w in cerca[:12]:
            base = sym.split("-")[0]
            marca = "🟡" if abs(w["dist_aprox"]) < 0.3 else "·"
            direccion = "▲" if w["h8"] > 0 else "▼"
            lineas.append(
                marca + " <b>" + base + "</b>  "
                + f"dominancia {w['ratio']:.2f}  {direccion}  "
                + f"a {w['dist_aprox']:+.2f} ATR de la aproximación  ·  "
                + f"ATR {w['atr_pct']:.2f}%"
            )
        lineas.append("")
        lineas.append("🟡 pegado a la aproximación: el cruce puede llegar en cualquier vela")
        await self.tg.send(chr(10).join(lineas))

    async def maybe_funding(self) -> None:
        """
        Avisa del funding extremo. NO monta carry: con este saldo no
        compensa (dos céntimos al día a tasa normal, trece días para
        cubrir la comisión). Avisa cuando sí compensaría y, sobre todo,
        señala dónde hay posicionamiento amontonado — que es donde una
        cascada encuentra combustible.
        """
        if not config.FUNDING_ALERTS:
            return
        if time.time() - self.last_funding_alert < config.FUNDING_ALERT_MIN * 60:
            return
        try:
            tasas = await self.api.funding_rates()
        except Exception as exc:  # noqa: BLE001
            log.warning("Funding no disponible: %s", exc)
            return
        if not tasas:
            return
        self.funding = tasas
        self.last_funding_alert = time.time()

        saldo = 0.0
        if self.live:
            try:
                saldo = await self.api.balance_usdt()
            except Exception:  # noqa: BLE001
                saldo = 0.0
        if saldo <= 0:
            saldo = config.SALDO_ESTIMADO

        extremos = [
            funding.evaluar(s, r, saldo)
            for s, r in tasas.items()
            if abs(r) >= config.FUNDING_EXTREMO and s in set(self.symbols)
        ]
        texto = funding.format_extremos(extremos, saldo)
        if texto:
            await self.tg.send(texto)
        else:
            log.info("Funding: ningún símbolo por encima de %.3f%%", config.FUNDING_EXTREMO)

    def aviso_en_frio(self, symbol: str, clave: str) -> bool:
        """
        ¿Toca avisar de esto, o ya se avisó hace poco?

        Sin enfriamiento, una señal que no se puede ejecutar se vuelve a
        detectar en cada ciclo y manda el mismo mensaje cada minuto.
        """
        avisos = self.state.data.setdefault("avisos", {})
        k = symbol + ":" + clave
        previo = float(avisos.get(k, 0) or 0)
        if time.time() - previo < config.SIGNAL_COOLDOWN_MIN * 60:
            return False
        avisos[k] = time.time()
        self.state.save()
        return True

    async def volcar_avisos(self) -> None:
        """
        Un solo mensaje con todas las señales del ciclo que no se
        ejecutaron, en vez de uno por cada una.
        """
        pend = self._pendientes_aviso
        self._pendientes_aviso = []
        if not pend:
            return
        # Enfriamiento por símbolo: si ya se avisó de este hace poco, no
        # se repite aunque vuelva a aparecer en el ciclo siguiente.
        nuevos = [s for s in pend if self.aviso_en_frio(s.symbol, "senal")]
        if not nuevos:
            return

        motivo = ("modo SIGNAL" if not self.live else
                  f"hueco lleno ({config.MAX_CONCURRENT} máx.)")
        lineas = [f"🔔 <b>{len(nuevos)} señal(es)</b> — no ejecutadas: {motivo}", ""]
        for sig in nuevos[:12]:
            lado = "🟢 LARGO" if sig.side == "BUY" else "🔴 CORTO"
            base = sig.symbol.split("-")[0]
            lineas.append(
                f"{lado} <b>{base}</b>  entrada <code>{sig.entry:.8g}</code>  "
                + f"SL <code>{sig.sl:.8g}</code>  TP <code>{sig.tp:.8g}</code>"
            )
            lineas.append(
                f"    riesgo {sig.riesgo_pct:.2f}% · coste {sig.coste_r:.2f}R · "
                + f"dominancia {sig.ratio:.2f}"
                + (f" · funding {sig.funding:+.3f}%" if getattr(sig, "funding", None) is not None else "")
            )
        if len(nuevos) > 12:
            lineas.append(f"… y {len(nuevos) - 12} más")
        await self.tg.send(chr(10).join(lineas))

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

        # Posición ZOMBI: sigue viva en el exchange mucho después de lo
        # que la estrategia contempla. No se cierra sola porque eso es
        # una decisión del usuario, pero avisar es obligatorio: una
        # posición olvidada es la forma más silenciosa de perder dinero.
        limite = config.ZOMBIE_ALERT_HOURS * 3600
        for symbol, pos in list(self.state.data.get("open", {}).items()):
            edad = time.time() - float(pos.get("opened_at", time.time()))
            if edad < limite:
                continue
            if self.aviso_en_frio(symbol, "zombi"):
                await self.tg.send(
                    f"🧟 <b>{symbol.split(chr(45))[0]}</b> lleva {edad/3600:.1f} h abierta." + chr(10)
                    + f"La estrategia contempla como máximo {config.MAX_TRADE_MINUTES} min." + chr(10)
                    + "<i>Comprueba en BingX si el SL y el TP siguen ahí.</i>"
                )

    async def manage_open(self) -> None:
        """
        SL y TP viven en el exchange desde la propia orden de entrada.
        Aquí solo se vigila el reloj: una señal de cruce que a las N
        velas no ha ido a ninguna parte ya no es la señal que se operó.
        Y solo se corta lo que NO va a favor — cortar las ganadoras por
        tiempo fue un error medido en el bot de reversión.
        """
        if not config.USE_TIME_EXIT:
            return
        abiertas = self.state.data.get("open", {})
        if not abiertas:
            return
        limite = config.max_trade_seconds()
        ahora = time.time()
        for symbol, pos in list(abiertas.items()):
            edad = ahora - float(pos.get("opened_at", ahora))
            if edad < limite:
                continue
            velas = await self._velas(symbol)
            if not velas:
                continue
            precio = velas[-1]["close"]
            entrada = float(pos["entry"])
            a_favor = precio > entrada if pos["side"] == "BUY" else precio < entrada
            if config.TIME_EXIT_ONLY_LOSING and a_favor:
                log.info("%s pasa del límite pero va a favor: se deja correr", symbol)
                continue
            if self.live:
                try:
                    await self.api.close_position(symbol, pos["side"], float(pos.get("qty", 0)))
                except Exception as exc:  # noqa: BLE001
                    await self.tg.send(f"⚠️ No se pudo cerrar {symbol} por tiempo: {exc}")
                    continue
            riesgo = abs(entrada - float(pos.get("sl_inicial", pos["sl"])))
            bruto = (precio - entrada) if pos["side"] == "BUY" else (entrada - precio)
            r_real = bruto / riesgo if riesgo > 0 else 0.0
            minutos = int(edad / 60)
            self.journal.cerrar(symbol, "tiempo", precio, r_real, minutos)
            self.sumar_r_dia(r_real)
            self.register_close(symbol, r_real > 0)
            await self.tg.send(
                f"⏱️ <b>{symbol.split(chr(45))[0]}</b> cerrada por tiempo tras {minutos} min "
                f"({r_real:+.2f} R)"
            )

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
