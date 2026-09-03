"""
Bot Wavelet MRA — BingX.

═══════════════════════════════════════════════════════════════════════
REPARACIONES DE ESTA VERSIÓN (wavelet-2.0)
═══════════════════════════════════════════════════════════════════════
1. NO ABRÍA NADA. El filtro de amplitud exigía ATR >= 1.50% en velas de
   5 minutos (MIN_COST_COVER=6 x coste 0.25%). 316 de 326 símbolos
   morían ahí. Recalibrado en config.py, y ahora el umbral efectivo se
   anuncia al arrancar y se contrasta cada ciclo contra la mediana real
   del universo: un filtro imposible ya no puede pasar desapercibido.

2. ÓRDENES LÍMITE FANTASMA. Con ENTRY_TYPE=LIMIT el bot daba la
   posición por abierta al enviar la orden. Si no se llenaba, ocupaba el
   único hueco de MAX_CONCURRENT para siempre y la reconciliación
   registraba un cierre inventado. Ahora hay estado "pendiente" real:
   se confirma contra el exchange con el precio de entrada REAL, o se
   cancela al vencer el TTL.

3. CORTOS MAL CONTABILIZADOS. La reconciliación calculaba R como
   (precio - entrada) también para los cortos: cada corto ganador se
   apuntaba como pérdida y alimentaba el circuit breaker al revés.

4. DIMENSIONADO. Se usaba el margen disponible como capital y se
   descartaba la señal si el tamaño caía bajo el lote mínimo. Ahora el
   capital dimensiona, el margen disponible se comprueba aparte, y el
   tamaño sube al mínimo del contrato mientras el riesgo resultante no
   pase de MAX_RISK_PER_TRADE_PCT.

5. ESCANEO. 326 símbolos de uno en uno tardaban ~160 s por ciclo con
   SCAN_INTERVAL_SEC=60, y la lista de vigilancia volvía a descargarlo
   todo. Ahora una sola pasada en paralelo sirve para ambas cosas.
"""
from __future__ import annotations

import asyncio
import dataclasses
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

CODE_VERSION = getattr(config, "CODE_VERSION", "wavelet-2.0")


_DEFAULTS = {
    "LOOKBACK_ENERGY": 40, "APPROX_LEN": 8, "ATR_LEN": 14,
    "NORMALIZE_SCALES": True, "DOMINANCE_THRESHOLD": 1.30,
    "ALLOW_LONG": True, "ALLOW_SHORT": True,
    "USE_VOL_FILTER": True, "VOL_LEN": 20, "VOL_MULT": 1.2,
    "USE_HTF_FILTER": True, "HTF_MA_LEN": 200,
    "USE_TRAILING": False, "TRAIL_ATR": 2.0, "TRAIL_START_R": 1.0,
    "SL_ATR": 1.5, "TP_ATR": 2.5, "MAX_TRADE_MINUTES": 120,
    "USE_TIME_EXIT": True, "TIME_EXIT_ONLY_LOSING": True,
    "COST_ROUNDTRIP_PCT": 0.15, "MIN_ATR_PCT": 0.30, "MIN_COST_COVER": 2.0,
    "MAX_COST_IN_R": 0.25, "MAX_RISK_PCT": 4.0, "MIN_RISK_PCT": 0.0,
    "MIN_QUOTE_VOLUME_24H": 2_000_000.0, "TIMEFRAME": "5m", "TIMEFRAMES": ["5m"],
    "SCAN_INTERVAL_SEC": 60, "MAX_SYMBOLS": 400, "SCAN_CONCURRENCY": 8,
    "SYMBOLS_REFRESH_HOURS": 6.0, "SCAN_DIAG": True,
    "SYMBOL_WHITELIST": [], "EXCLUDE_PREFIXES": ["NC"],
    "RISK_PCT": 0.5, "MAX_CONCURRENT": 1, "MAX_TOTAL_POSITIONS": 3,
    "LEVERAGE": 2, "MARGIN_MODE": "ISOLATED", "POSITION_MODE": "AUTO",
    "RECV_WINDOW": 10000,
    "MAX_CONSECUTIVE_LOSSES": 3, "COOLDOWN_MINUTES": 120,
    "MAX_DAILY_LOSS_R": 3.0, "COOLDOWN_BARS": 4,
    "ENTRY_TYPE": "MARKET", "LIMIT_OFFSET_PCT": 0.05, "LIMIT_TTL_MIN": 10,
    "ALLOW_MIN_QTY_BUMP": True, "MAX_RISK_PER_TRADE_PCT": 1.5,
    "MIN_NOTIONAL_USDT": 2.0, "MARGIN_USE_MAX_PCT": 90.0,
    "SIGNAL_COOLDOWN_MIN": 60, "WATCHLIST_MIN": 30,
    "DAILY_SUMMARY": True, "DAILY_SUMMARY_HOUR_UTC": 7,
    "HEARTBEAT_HOURS": 12, "IDLE_ALERT_DAYS": 5, "ZOMBIE_ALERT_HOURS": 6.0,
    "BTC_CONTEXT": True, "BTC_FILTER": False, "BTC_MIN_24H": -3.0,
    "FUNDING_ALERTS": True, "FUNDING_EXTREMO": 0.05, "FUNDING_ALERT_MIN": 120,
    "CARRY_MAX_DIAS_COBERTURA": 3.0, "FUNDING_MIN_USDT_DIA": 0.10,
    "SALDO_ESTIMADO": 135.0,
    "STATE_PATH": "/data/state_wavelet.json", "LOG_LEVEL": "INFO",
}


def ensure_config() -> list[str]:
    """Un config.py antiguo no puede tumbar un bot con dinero real."""
    faltan = []
    for nombre, valor in _DEFAULTS.items():
        if not hasattr(config, nombre):
            setattr(config, nombre, valor)
            faltan.append(nombre)
    if not hasattr(config, "atr_minimo_efectivo"):
        config.atr_minimo_efectivo = lambda: max(
            config.MIN_ATR_PCT,
            config.COST_ROUNDTRIP_PCT * config.MIN_COST_COVER,
            config.COST_ROUNDTRIP_PCT / (config.MAX_COST_IN_R * config.SL_ATR)
            if config.MAX_COST_IN_R > 0 and config.SL_ATR > 0 else 0.0,
        )
        faltan.append("atr_minimo_efectivo()")
    return faltan


def percentil(valores: list[float], p: float) -> float:
    if not valores:
        return 0.0
    v = sorted(valores)
    i = min(len(v) - 1, max(0, int(round((len(v) - 1) * p))))
    return v[i]


def fmt_signal(sig: strategy.Signal, live: bool, extra: str = "") -> str:
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
        partes.append(f"Funding {sig.funding:+.4f}%/8h — {funding.sesgo(sig.funding)}")
    if extra:
        partes.append(extra)
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
        self.last_symbols = 0.0
        self.btc_24h: float | None = None
        self.funding: dict[str, float] = {}
        self._pendientes_aviso: list = []
        self._vigilancia: list = []
        self.last_funding_alert = 0.0
        self.btc_ts = 0.0
        self.sem = asyncio.Semaphore(config.SCAN_CONCURRENCY)
        self.journal = journal.Journal(
            os.path.join(os.path.dirname(config.STATE_PATH) or "/data", "operaciones_wavelet.csv")
        )

    # ── arranque ──────────────────────────────────────────────────────
    async def start(self) -> None:
        faltan = ensure_config()
        if faltan:
            log.error("config.py desactualizado, faltaban: %s", ", ".join(faltan))
        log.info("Versión de código: %s", CODE_VERSION)
        log.info("Modo: %s", config.describe())

        minimo = config.atr_minimo_efectivo()
        aviso_modo = ""
        if self.live:
            try:
                dual = await self.api.dual_mode()
                aviso_modo = ("Posiciones en modo cobertura" if dual
                              else "Posiciones en modo unidireccional")
            except Exception as exc:  # noqa: BLE001
                aviso_modo = f"No se pudo leer el modo de posición ({exc})"

        await self.tg.send(
            f"🤖 <b>Bot Wavelet MRA iniciado</b> · <code>{CODE_VERSION}</code>" + chr(10)
            + config.describe() + chr(10)
            + f"Dominancia ≥{config.DOMINANCE_THRESHOLD} "
            + ("(normalizada por escala)" if config.NORMALIZE_SCALES else "(SIN normalizar)") + chr(10)
            + f"Cruce sobre SMA({config.APPROX_LEN}) con la escala gruesa a favor" + chr(10)
            + f"Timeframe {config.TIMEFRAME} · riesgo {config.RISK_PCT}% · "
            + f"SL {config.SL_ATR} ATR / TP {config.TP_ATR} ATR" + chr(10)
            + f"Entrada {config.ENTRY_TYPE} · apalancamiento x{config.LEVERAGE} · {config.MARGIN_MODE}" + chr(10)
            + f"<b>ATR mínimo efectivo: {minimo:.2f}%</b> "
            + f"(coste {config.COST_ROUNDTRIP_PCT}% · cobertura {config.MIN_COST_COVER}× · "
            + f"máx {config.MAX_COST_IN_R}R)"
            + (chr(10) + aviso_modo if aviso_modo else "")
        )
        await self.refresh_symbols()

        while True:
            try:
                await self.refresh_symbols_si_toca()
                await self.reconcile()
                await self.manage_pending()
                await self.manage_open()
                await self.maybe_funding()
                await self.scan_once()
                await self.maybe_watchlist()
                await self.maybe_daily_summary()
                await self.maybe_heartbeat()
            except Exception as exc:  # noqa: BLE001
                log.exception("Fallo en el ciclo: %s", exc)
            await asyncio.sleep(config.SCAN_INTERVAL_SEC)

    async def refresh_symbols_si_toca(self) -> None:
        if time.time() - self.last_symbols < config.SYMBOLS_REFRESH_HOURS * 3600:
            return
        await self.refresh_symbols()

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
        self.last_symbols = time.time()
        log.info("Universo: %d símbolos", len(self.symbols))

    # ── utilidades de estado ──────────────────────────────────────────
    def in_cooldown(self) -> bool:
        return time.time() < float(self.state.data.get("cooldown_until", 0))

    def dia_actual(self) -> str:
        return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")

    def abiertas_y_pendientes(self) -> int:
        return len(self.state.data.get("open", {})) + len(self.state.data.get("pending", {}))

    def en_enfriamiento_simbolo(self, symbol: str) -> bool:
        """Tras cerrar, N velas sin volver a entrar en el mismo símbolo."""
        if config.COOLDOWN_BARS <= 0:
            return False
        hasta = float(self.state.data.get("sym_cooldown", {}).get(symbol, 0) or 0)
        return time.time() < hasta

    def marcar_enfriamiento(self, symbol: str) -> None:
        if config.COOLDOWN_BARS <= 0:
            return
        minutos = config.COOLDOWN_BARS * config._tf_min(config.TIMEFRAME)
        self.state.data.setdefault("sym_cooldown", {})[symbol] = time.time() + minutos * 60
        self.state.save()

    def limite_diario_alcanzado(self) -> bool:
        """
        Stop de pérdida DIARIA. Distinto del circuit breaker por rachas:
        seis pérdidas alternadas con dos ganancias no disparan una racha
        de tres y el día acaba igual de mal. Se reinicia al cambiar de
        día UTC.
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

    async def contexto_btc(self) -> float | None:
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

    async def _velas(self, sym: str, tf: str | None = None) -> list[dict] | None:
        async with self.sem:
            try:
                return await self.api.klines(sym, tf or config.TIMEFRAME, limit=400)
            except Exception:  # noqa: BLE001
                return None

    async def _analizar(self, sym: str) -> strategy.Analisis:
        """Una sola lectura por símbolo y ciclo, reutilizada por la señal,
        la vigilancia y el diagnóstico del embudo."""
        peor = strategy.Analisis(symbol=sym)
        for tf in config.TIMEFRAMES:
            velas = await self._velas(sym, tf)
            if not velas:
                continue
            a = strategy.analizar(sym, velas, tf)
            if a.signal is not None:
                return a
            peor = a
        return peor

    # ── escaneo ───────────────────────────────────────────────────────
    async def scan_once(self) -> None:
        if not self.symbols:
            return
        if self.in_cooldown():
            log.info("En enfriamiento por rachas: no se escanea para abrir")

        btc = await self.contexto_btc()
        bloqueo_btc = config.BTC_FILTER and btc is not None and btc < config.BTC_MIN_24H
        if bloqueo_btc:
            log.info("BTC %.1f%% en 24h: por debajo del mínimo, no se abre", btc)

        if self.limite_diario_alcanzado() and self.state.data.get("aviso_dia") != self.dia_actual():
            self.state.data["aviso_dia"] = self.dia_actual()
            self.state.save()
            await self.tg.send(
                f"🛑 <b>Límite de pérdida diaria alcanzado</b>\n"
                f"{float(self.state.data.get('r_hoy', 0)):.2f} R hoy "
                f"(límite {config.MAX_DAILY_LOSS_R} R).\n"
                f"No se abren más posiciones hasta mañana (UTC)."
            )

        ocupados = set(self.state.data.get("open", {})) | set(self.state.data.get("pending", {}))
        objetivos = [s for s in self.symbols if s not in ocupados]

        t0 = time.time()
        resultados = await asyncio.gather(
            *[self._analizar(s) for s in objetivos], return_exceptions=True
        )

        motivos: dict[str, int] = {}
        atrs: list[float] = []
        señales: list[strategy.Signal] = []
        self._vigilancia = []

        for a in resultados:
            if isinstance(a, BaseException) or a is None:
                motivos["error de lectura"] = motivos.get("error de lectura", 0) + 1
                continue
            if a.atr_pct > 0:
                atrs.append(a.atr_pct)
            if a.dominante and a.atr_pct > 0:
                self._vigilancia.append(a)
            if a.signal is None:
                clave = a.motivo.split("(")[0].strip()
                motivos[clave] = motivos.get(clave, 0) + 1
                continue
            señales.append(a.signal)

        # Ejecución en serie: mandar órdenes en paralelo es cómo se
        # sobrepasa MAX_CONCURRENT sin darse cuenta.
        puede_abrir = (self.live and not self.in_cooldown()
                       and not bloqueo_btc and not self.limite_diario_alcanzado())
        for sig in señales:
            sig.btc_24h = btc
            sig.funding = self.funding.get(sig.symbol)
            hay_hueco = puede_abrir and self.abiertas_y_pendientes() < config.MAX_CONCURRENT
            await self.handle_signal(sig, hay_hueco)

        await self.volcar_avisos()

        detalle = " · ".join(f"{k}: {v}" for k, v in sorted(motivos.items(), key=lambda x: -x[1])[:5])
        diag = ""
        if config.SCAN_DIAG and atrs:
            minimo = config.atr_minimo_efectivo()
            pasan = sum(1 for x in atrs if x >= minimo)
            diag = (f" | ATR% med {percentil(atrs, 0.5):.2f} p90 {percentil(atrs, 0.9):.2f} "
                    f"máx {max(atrs):.2f} · umbral {minimo:.2f} → pasan {pasan}")
        log.info("Ciclo %.0fs · %d símbolos · %d señales | %s%s",
                 time.time() - t0, len(objetivos), len(señales), detalle, diag)

        await self.aviso_embudo(motivos, atrs, len(objetivos))

    async def aviso_embudo(self, motivos: dict[str, int], atrs: list[float], total: int) -> None:
        """
        Si el embudo se corta SIEMPRE en el mismo sitio, eso no es el
        mercado: es un filtro mal calibrado. Este aviso es exactamente lo
        que faltaba cuando 316 de 326 símbolos morían en "sin amplitud".
        """
        if not motivos or total <= 0:
            return
        top = max(motivos.items(), key=lambda x: x[1])
        if top[1] < total * 0.9:
            return
        hoy = self.dia_actual()
        if self.state.data.get("warned_funnel") == hoy:
            return
        self.state.data["warned_funnel"] = hoy
        self.state.save()

        extra = ""
        if "amplitud" in top[0] and atrs:
            minimo = config.atr_minimo_efectivo()
            extra = (f"\n\nEl ATR mediano del universo es <b>{percentil(atrs, 0.5):.2f}%</b> y el "
                     f"máximo <b>{max(atrs):.2f}%</b>, contra un umbral de <b>{minimo:.2f}%</b>.\n"
                     f"Ese umbral sale de <code>MIN_ATR_PCT</code>, "
                     f"<code>COST_ROUNDTRIP_PCT×MIN_COST_COVER</code> y "
                     f"<code>COST_ROUNDTRIP_PCT/(MAX_COST_IN_R×SL_ATR)</code>: manda el mayor "
                     f"de los tres. Si ninguno pasa, el filtro no está estricto, está cerrado.")
        await self.tg.send(
            f"🔻 <b>El embudo se corta siempre en el mismo punto</b>\n"
            f"<code>{top[0]}</code> descarta {top[1]} de {total} símbolos.{extra}"
        )

    # ── ejecución ─────────────────────────────────────────────────────
    async def handle_signal(self, sig: strategy.Signal, hay_hueco: bool = True) -> bool:
        log.info("SEÑAL %s %s entrada=%.8g sl=%.8g ratio=%.2f atr=%.2f%%",
                 sig.symbol, sig.side, sig.entry, sig.sl, sig.ratio, sig.atr_pct)

        ultimos = self.state.data.setdefault("last_signal", {})
        previo = float(ultimos.get(sig.symbol, 0) or 0)
        avisable = time.time() - previo >= config.SIGNAL_COOLDOWN_MIN * 60
        if avisable:
            ultimos[sig.symbol] = time.time()
            self.state.save()

        if not self.live or not hay_hueco or self.en_enfriamiento_simbolo(sig.symbol):
            if avisable:
                self._pendientes_aviso.append(sig)
            return False

        return await self.abrir(sig)

    async def abrir(self, sig: strategy.Signal) -> bool:
        client_id = f"wav{sig.symbol.split('-')[0][:6]}{int(time.time())}"
        try:
            saldo = await self.api.balance()
            equity = saldo["equity"]
            disponible = saldo["available"]
            if equity <= 0:
                await self.tg.send(
                    f"⚠️ <b>{sig.symbol}</b> sin ejecutar: el saldo leído es 0.\n"
                    f"<i>Comprueba que la API key tiene permiso de futuros y que la cuenta "
                    f"consultada es la que tiene el capital.</i>"
                )
                return False

            qty_ideal = strategy.position_size(equity, sig.entry, sig.sl)
            qty = self.api.floor_qty(sig.symbol, qty_ideal)

            # Mínimos del contrato: con cuentas pequeñas el tamaño teórico
            # cae por debajo y antes eso descartaba la señal entera.
            min_q = self.api.min_qty(sig.symbol)
            min_notional = self.api.min_notional(sig.symbol)
            necesaria = max(min_q, (min_notional / sig.entry) if sig.entry > 0 else 0.0)
            subido = False
            if qty < necesaria:
                if not config.ALLOW_MIN_QTY_BUMP:
                    await self.tg.send(
                        f"⚠️ <b>{sig.symbol}</b> sin ejecutar: {qty:.10g} bajo el mínimo "
                        f"({necesaria:.10g})."
                    )
                    return False
                qty = self.api.ceil_qty(sig.symbol, necesaria)
                subido = True

            if qty <= 0:
                await self.tg.send(f"⚠️ <b>{sig.symbol}</b> sin ejecutar: tamaño 0 tras redondeo.")
                return False

            riesgo_real_pct = qty * abs(sig.entry - sig.sl) / equity * 100.0
            if riesgo_real_pct > config.MAX_RISK_PER_TRADE_PCT:
                await self.tg.send(
                    f"⚠️ <b>{sig.symbol}</b> sin ejecutar: el lote mínimo del contrato obliga a "
                    f"arriesgar <b>{riesgo_real_pct:.2f}%</b> del capital "
                    f"(tope {config.MAX_RISK_PER_TRADE_PCT}%, objetivo {config.RISK_PCT}%).\n"
                    f"<i>Con {equity:.0f} USDT este símbolo es demasiado grande. No es un fallo: "
                    f"es el suelo del contrato.</i>"
                )
                return False

            nominal = qty * sig.entry
            margen = nominal / max(1, config.LEVERAGE)
            if margen > disponible * config.MARGIN_USE_MAX_PCT / 100.0:
                await self.tg.send(
                    f"⚠️ <b>{sig.symbol}</b> sin ejecutar: harían falta <b>{margen:.2f} USDT</b> "
                    f"de margen y hay <b>{disponible:.2f}</b> disponibles "
                    f"(capital {equity:.2f}).\n"
                    f"<i>Si el capital está pero el margen no, hay posiciones abiertas "
                    f"—posiblemente de otro bot— ocupándolo.</i>"
                )
                return False

            vivas = await self.api.open_positions()
            n_total = sum(1 for p in vivas if self.api.cantidad_posicion(p) != 0)
            if config.MAX_TOTAL_POSITIONS > 0 and n_total >= config.MAX_TOTAL_POSITIONS:
                await self.tg.send(
                    f"⚠️ <b>{sig.symbol}</b> sin abrir: ya hay <b>{n_total}</b> posiciones en la "
                    f"cuenta (límite global {config.MAX_TOTAL_POSITIONS}).\n"
                    f"<i>Puede haberlas abierto otro bot. En una caída las alts se mueven juntas, "
                    f"así que el riesgo se suma aunque los símbolos difieran.</i>"
                )
                return False
            if any(str(p.get("symbol")) == sig.symbol and self.api.cantidad_posicion(p) != 0
                   for p in vivas):
                return False

            await self.api.set_margin_mode(sig.symbol, config.MARGIN_MODE)
            await self.api.set_leverage(sig.symbol, sig.side, config.LEVERAGE)

            sl_r = self.api.round_price(sig.symbol, sig.sl)
            tp_r = self.api.round_price(sig.symbol, sig.tp)
            if config.ENTRY_TYPE == "LIMIT":
                ajuste = config.LIMIT_OFFSET_PCT / 100.0
                precio = self.api.round_price(
                    sig.symbol, sig.entry * ((1 + ajuste) if sig.side == "SELL" else (1 - ajuste))
                )
                await self.api.limit_order(sig.symbol, sig.side, qty, precio, sl_r, tp_r, client_id)
            else:
                await self.api.market_order(sig.symbol, sig.side, qty, sl_r, tp_r, client_id)

        except BingXError as exc:
            await self.tg.send(f"❌ BingX rechazó <b>{sig.symbol}</b>: {exc}")
            return False
        except Exception as exc:  # noqa: BLE001
            if await self.api.order_exists(sig.symbol, client_id):
                await self.tg.send(
                    f"⚠️ <b>{sig.symbol}</b>: fallo de red pero la orden SÍ existe. Se registra."
                )
            else:
                await self.tg.send(f"❌ Error en <b>{sig.symbol}</b>: {exc}")
                return False

        # PENDIENTE, no abierta: no hay posición hasta que el exchange la
        # confirme. Darla por hecha aquí fue el fallo que dejaba el hueco
        # bloqueado con órdenes límite que nunca se llenaban.
        self.state.data.setdefault("pending", {})[sig.symbol] = {
            "sig": dataclasses.asdict(sig),
            "qty": qty,
            "sent_at": time.time(),
            "client_id": client_id,
            "tipo": config.ENTRY_TYPE,
            "riesgo_pct_real": riesgo_real_pct,
            "subido_a_minimo": subido,
        }
        self.state.save()

        extra = ""
        if subido:
            extra = (f"<i>Tamaño subido al lote mínimo: riesgo real "
                     f"{riesgo_real_pct:.2f}% en vez de {config.RISK_PCT}%.</i>")
        await self.tg.send(
            fmt_signal(sig, live=True, extra=extra)
            + chr(10) + f"Cantidad {qty:.10g} · nominal {nominal:.2f} USDT · "
            + f"margen {margen:.2f} USDT"
            + chr(10) + f"<i>Enviada ({config.ENTRY_TYPE}). Se confirmará contra el exchange.</i>"
        )
        return True

    async def manage_pending(self) -> None:
        """
        Confirma o cancela lo enviado. Una orden enviada no es una
        posición: hasta que el exchange no la reconoce, el bot no puede
        contarla como abierta ni como cerrada.
        """
        pend = self.state.data.get("pending", {})
        if not pend or not self.live:
            return
        try:
            posiciones = await self.api.open_positions()
        except Exception as exc:  # noqa: BLE001
            log.warning("No se pudieron leer posiciones para confirmar pendientes: %s", exc)
            return
        vivas = {str(p.get("symbol")): p for p in posiciones if self.api.cantidad_posicion(p) != 0}

        for symbol, p in list(pend.items()):
            d = dict(p.get("sig", {}))
            sig = strategy.Signal(**{k: v for k, v in d.items()
                                     if k in strategy.Signal.__dataclass_fields__})
            pos = vivas.get(symbol)
            if pos:
                entrada_real = self.api.precio_entrada(pos) or sig.entry
                qty_real = abs(self.api.cantidad_posicion(pos)) or float(p.get("qty", 0))
                self.state.data.setdefault("open", {})[symbol] = {
                    "side": sig.side, "entry": entrada_real, "entrada_teorica": sig.entry,
                    "sl": sig.sl, "sl_inicial": sig.sl, "tp": sig.tp,
                    "qty": qty_real, "opened_at": time.time(),
                }
                self.journal.abrir(sig, qty_real, "LIVE", entrada_real)
                self.state.data["last_trade_ts"] = time.time()
                pend.pop(symbol, None)
                self.state.save()
                desliz = ((entrada_real - sig.entry) / sig.entry * 100.0) if sig.entry else 0.0
                await self.tg.send(
                    f"✅ <b>{symbol.split('-')[0]}</b> abierta y confirmada\n"
                    f"Entrada real <code>{entrada_real:.8g}</code> "
                    f"(esperada {sig.entry:.8g} · deslizamiento {desliz:+.3f}%)\n"
                    f"Cantidad {qty_real:.10g} · SL {sig.sl:.8g} · TP {sig.tp:.8g}"
                )
                continue

            edad = time.time() - float(p.get("sent_at", time.time()))
            ttl = config.LIMIT_TTL_MIN * 60 if p.get("tipo") == "LIMIT" else 120
            if edad < ttl:
                continue
            try:
                await self.api.cancel_open_orders(symbol)
            except Exception as exc:  # noqa: BLE001
                log.warning("No se pudo cancelar %s: %s", symbol, exc)
            pend.pop(symbol, None)
            self.state.save()
            await self.tg.send(
                f"🚫 <b>{symbol.split('-')[0]}</b>: la orden {p.get('tipo')} no se llenó en "
                f"{edad/60:.0f} min. Cancelada, el hueco queda libre.\n"
                f"<i>No entrar es una salida válida: en un libro fino, entrar al precio que "
                f"quede es peor que no entrar.</i>"
            )

    # ── gestión de lo abierto ─────────────────────────────────────────
    async def reconcile(self) -> None:
        """
        Detecta las posiciones cerradas EN EL EXCHANGE (stop o take
        profit) que el bot no vio. Sin esto, el hueco queda bloqueado
        para siempre y el circuit breaker no cuenta ni una pérdida.
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
        vivos = {str(p.get("symbol", "")) for p in posiciones
                 if self.api.cantidad_posicion(p) != 0}

        for symbol, pos in list(abiertas.items()):
            if symbol in vivos:
                continue
            velas = await self._velas(symbol)
            entrada = float(pos["entry"])
            ultimo = velas[-1]["close"] if velas else entrada
            largo = pos.get("side", "BUY") == "BUY"
            sl = float(pos.get("sl_inicial", pos.get("sl", entrada)))
            tp = float(pos.get("tp", 0) or 0)
            riesgo = abs(entrada - sl)

            # El precio actual puede estar lejos del punto real de salida.
            # Si ya cruzó el stop o el objetivo, se usa ESE precio: es la
            # estimación honesta, no la optimista.
            if (largo and ultimo <= sl) or ((not largo) and ultimo >= sl):
                salida, motivo = sl, "stop"
            elif tp and ((largo and ultimo >= tp) or ((not largo) and ultimo <= tp)):
                salida, motivo = tp, "objetivo"
            else:
                salida, motivo = ultimo, "cerrada fuera del bot"

            bruto = (salida - entrada) if largo else (entrada - salida)
            r_real = bruto / riesgo if riesgo > 0 else 0.0
            gano = bruto > 0
            minutos = int((time.time() - float(pos.get("opened_at", time.time()))) / 60)

            self.journal.cerrar(symbol, motivo, salida, r_real, minutos)
            self.sumar_r_dia(r_real)
            await self.tg.send(
                f"{'✅' if gano else '🛑'} <b>{symbol.split('-')[0]}</b> cerrada en el exchange "
                f"({motivo})\n"
                f"Entrada {entrada:.8g} → {salida:.8g}  ({r_real:+.2f} R) · {minutos} min"
            )
            self.register_close(symbol, gano)

        # Posición ZOMBI: viva mucho después de lo que la estrategia
        # contempla. No se cierra sola —esa decisión es tuya— pero una
        # posición olvidada es la forma más silenciosa de perder dinero.
        limite = config.ZOMBIE_ALERT_HOURS * 3600
        for symbol, pos in list(self.state.data.get("open", {}).items()):
            edad = time.time() - float(pos.get("opened_at", time.time()))
            if edad < limite:
                continue
            if self.aviso_en_frio(symbol, "zombi"):
                await self.tg.send(
                    f"🧟 <b>{symbol.split('-')[0]}</b> lleva {edad/3600:.1f} h abierta.\n"
                    f"La estrategia contempla como máximo {config.MAX_TRADE_MINUTES} min.\n"
                    f"<i>Comprueba en BingX si el SL y el TP siguen ahí.</i>"
                )

    async def manage_open(self) -> None:
        """
        SL y TP viven en el exchange desde la propia orden de entrada.
        Aquí solo se vigila el reloj, y solo se corta lo que NO va a
        favor: cortar las ganadoras por tiempo fue un error medido.
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
            largo = pos.get("side", "BUY") == "BUY"
            a_favor = precio > entrada if largo else precio < entrada
            if config.TIME_EXIT_ONLY_LOSING and a_favor:
                log.info("%s pasa del límite pero va a favor: se deja correr", symbol)
                continue
            if self.live:
                try:
                    await self.api.close_position(symbol, pos["side"], float(pos.get("qty", 0)))
                    await self.api.cancel_open_orders(symbol)
                except Exception as exc:  # noqa: BLE001
                    await self.tg.send(f"⚠️ No se pudo cerrar {symbol} por tiempo: {exc}")
                    continue
            riesgo = abs(entrada - float(pos.get("sl_inicial", pos["sl"])))
            bruto = (precio - entrada) if largo else (entrada - precio)
            r_real = bruto / riesgo if riesgo > 0 else 0.0
            minutos = int(edad / 60)
            self.journal.cerrar(symbol, "tiempo", precio, r_real, minutos)
            self.sumar_r_dia(r_real)
            self.register_close(symbol, r_real > 0)
            await self.tg.send(
                f"⏱️ <b>{symbol.split('-')[0]}</b> cerrada por tiempo tras {minutos} min "
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
                    f"⏸️ <b>Circuit breaker</b> · {config.MAX_CONSECUTIVE_LOSSES} pérdidas "
                    f"seguidas, pausa de {config.COOLDOWN_MINUTES} min."
                ))
        d.get("open", {}).pop(symbol, None)
        self.state.save()
        self.marcar_enfriamiento(symbol)

    # ── avisos ────────────────────────────────────────────────────────
    def aviso_en_frio(self, symbol: str, clave: str) -> bool:
        avisos = self.state.data.setdefault("avisos", {})
        k = f"{symbol}:{clave}"
        previo = float(avisos.get(k, 0) or 0)
        if time.time() - previo < config.SIGNAL_COOLDOWN_MIN * 60:
            return False
        avisos[k] = time.time()
        if len(avisos) > 500:  # el estado no es un archivo histórico
            corte = time.time() - 86400
            for kk, vv in list(avisos.items()):
                if float(vv or 0) < corte:
                    avisos.pop(kk, None)
        self.state.save()
        return True

    async def volcar_avisos(self) -> None:
        """Un solo mensaje con las señales no ejecutadas del ciclo. Cinco
        avisos idénticos en el mismo minuto no informan cinco veces:
        enseñan a ignorar el chat."""
        pend = self._pendientes_aviso
        self._pendientes_aviso = []
        if not pend:
            return
        nuevos = [s for s in pend if self.aviso_en_frio(s.symbol, "senal")]
        if not nuevos:
            return
        if not self.live:
            motivo = "modo SIGNAL"
        elif self.in_cooldown():
            motivo = "enfriamiento por rachas"
        elif self.limite_diario_alcanzado():
            motivo = "límite de pérdida diaria"
        else:
            motivo = f"hueco lleno ({config.MAX_CONCURRENT} máx.)"

        lineas = [f"🔔 <b>{len(nuevos)} señal(es)</b> — no ejecutadas: {motivo}", ""]
        for sig in nuevos[:12]:
            lado = "🟢 LARGO" if sig.side == "BUY" else "🔴 CORTO"
            base = sig.symbol.split("-")[0]
            lineas.append(
                f"{lado} <b>{base}</b>  entrada <code>{sig.entry:.8g}</code>  "
                f"SL <code>{sig.sl:.8g}</code>  TP <code>{sig.tp:.8g}</code>"
            )
            lineas.append(
                f"    riesgo {sig.riesgo_pct:.2f}% · coste {sig.coste_r:.2f}R · "
                f"dominancia {sig.ratio:.2f} · ATR {sig.atr_pct:.2f}%"
                + (f" · funding {sig.funding:+.3f}%" if getattr(sig, "funding", None) is not None else "")
            )
        if len(nuevos) > 12:
            lineas.append(f"… y {len(nuevos) - 12} más")
        await self.tg.send(chr(10).join(lineas))

    async def maybe_watchlist(self) -> None:
        """Lo que viene, no lo que ya pasó: los que están en régimen
        dominante y pegados a la aproximación están a un cruce de
        disparar. Sale de la misma pasada del escáner, sin releer nada."""
        if config.WATCHLIST_MIN <= 0:
            return
        if time.time() - self.last_watchlist < config.WATCHLIST_MIN * 60:
            return
        self.last_watchlist = time.time()

        minimo = config.atr_minimo_efectivo()
        cerca = [a for a in self._vigilancia if a.atr_pct >= minimo]
        if not cerca:
            log.info("Vigilancia: ningún símbolo en régimen dominante con amplitud")
            return
        cerca.sort(key=lambda a: abs(a.dist_aprox))
        lineas = [f"👀 <b>En régimen dominante — vigilando</b> ({len(cerca)})", ""]
        for a in cerca[:12]:
            marca = "🟡" if abs(a.dist_aprox) < 0.3 else "·"
            direccion = "▲" if a.h8 > 0 else "▼"
            lineas.append(
                f"{marca} <b>{a.symbol.split('-')[0]}</b>  dominancia {a.ratio:.2f}  "
                f"{direccion}  a {a.dist_aprox:+.2f} ATR de la aproximación  ·  "
                f"ATR {a.atr_pct:.2f}%"
            )
        lineas.append("")
        lineas.append("🟡 pegado a la aproximación: el cruce puede llegar en cualquier vela")
        await self.tg.send(chr(10).join(lineas))

    async def maybe_funding(self) -> None:
        """Avisa del funding extremo con el cálculo hecho sobre tu saldo
        real. NO monta carry: con esta base no compensa."""
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
                saldo = (await self.api.balance())["equity"]
            except Exception:  # noqa: BLE001
                saldo = 0.0
        if saldo <= 0:
            saldo = config.SALDO_ESTIMADO

        universo = set(self.symbols)
        extremos = [funding.evaluar(s, r, saldo) for s, r in tasas.items()
                    if abs(r) >= config.FUNDING_EXTREMO and s in universo]
        texto = funding.format_extremos(extremos, saldo)
        if texto:
            await self.tg.send(texto)
        else:
            log.info("Funding: ningún símbolo por encima de %.3f%%", config.FUNDING_EXTREMO)

    def stats_text(self) -> str:
        d = self.state.data
        n = d.get("closed_trades", 0)
        w = d.get("wins", 0)
        wr = (w / n * 100.0) if n else 0.0
        return (
            f"Cerradas: <b>{n}</b> · aciertos {w} ({wr:.0f}%)\n"
            f"Abiertas: {len(d.get('open', {}))} · pendientes: {len(d.get('pending', {}))} · "
            f"racha: {d.get('consecutive_losses', 0)} · hoy {float(d.get('r_hoy', 0)):+.2f} R"
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
            f"📊 <b>Resumen diario — Wavelet MRA</b> · {hoy}\n{config.describe()}\n\n"
            f"{self.stats_text()}\nUniverso: {len(self.symbols)} símbolos · "
            f"ATR mínimo {config.atr_minimo_efectivo():.2f}%\n\n{self.journal.resumen()}"
        )

    async def maybe_heartbeat(self) -> None:
        if config.HEARTBEAT_HOURS <= 0:
            return
        if time.time() - self.last_heartbeat < config.HEARTBEAT_HOURS * 3600:
            return
        self.last_heartbeat = time.time()
        await self.tg.send(f"💓 Vivo (wavelet MRA · {CODE_VERSION})\n{self.stats_text()}")


async def main() -> None:
    bot = Bot()
    try:
        await bot.start()
    finally:
        await bot.client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
