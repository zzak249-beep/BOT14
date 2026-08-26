"""
Detector de cascadas de liquidación en tiempo real.

Usa los DOS streams públicos y gratuitos que existen para esto — no
hace falta pagar Coinglass, que por dentro no es más que un
envoltorio de este mismo dato con un modelo encima:

  · Binance Futures: wss://fstream.binance.com/ws/!forceOrder@arr
    Un solo stream, TODOS los símbolos a la vez, sin autenticación,
    sin límite de peticiones — es literalmente un feed de mercado.
  · Bybit: canal público 'allLiquidation', también sin autenticar,
    pero hay que suscribirse símbolo a símbolo, así que aquí solo se
    suscribe a los símbolos que el bot tiene en su universo ahora
    mismo (se resincroniza si el universo cambia).

POR QUÉ DOS EXCHANGES Y NO UNO: BingX no publica liquidaciones. Los
símbolos del radar cotizan casi siempre también en Binance y/o
Bybit. Sumar ambos da más cobertura justo en las monedas finas de
poca capitalización, que es donde la cascada existe de verdad y
donde un solo exchange se queda corto de actividad para medirla.

QUÉ CUENTA COMO CASCADA AQUÍ (y qué no):
El único backtest de esto con walk-forward que sobrevivió (SOL/ETH
con PF>2.5 en varias ventanas fuera de muestra; BTC descartado por
tener el libro demasiado profundo) encontró que lo que separa una
cascada real de una liquidación suelta es VELOCIDAD + VOLUMEN:
actividad sostenida y muy por encima de lo normal, no un importe
absoluto grande de una vez. Aquí se traduce así: notional liquidado
en la ventana corta (LIQ_SHORT_WINDOW_SEC) muy por encima de la
media reciente de ESE símbolo (LIQ_MULTIPLIER), con varios eventos
distintos (LIQ_MIN_EVENTS) — no una liquidación grande y sola, que
puede ser ruido de una sola cuenta y no un mecanismo de mercado.

QUÉ NO ES: una señal de entrada por sí sola. Es una CONFIRMACIÓN —
el dato que separa "este estirón vino de un flujo forzado, mecánico,
candidato real a reversión" de "este movimiento vino de otro sitio y
la reversión aquí es solo una esperanza". strategy.evaluate() decide
exactamente igual que antes; esto solo añade información a la
notificación de la señal.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass

import config

log = logging.getLogger("liquidations")

BINANCE_WS = "wss://fstream.binance.com/ws/!forceOrder@arr"
BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"


@dataclass
class LiqEvent:
    symbol: str      # formato Binance/Bybit, SIN guion: 'CATEUSDT'
    side: str        # "BUY" = se liquidó un CORTO (compra forzada) | "SELL" = se liquidó un LARGO
    notional: float
    ts: float


def cascade_confirms(signal_side: str, lado_liquidado: str) -> bool:
    """
    ¿La cascada confirma la señal, o va en contra?

    Señal SELL = se apuesta a que un estirón AL ALZA revierte. Eso se
    confirma si lo que empujó hacia arriba fueron CORTOS liquidados
    (compra forzada). Señal BUY = estirón A LA BAJA; se confirma si
    lo que empujó hacia abajo fueron LARGOS liquidados (venta forzada).
    """
    if signal_side == "SELL":
        return lado_liquidado == "CORTOS"
    if signal_side == "BUY":
        return lado_liquidado == "LARGOS"
    return False


class LiquidationTracker:
    """
    Corre en segundo plano, no bloquea el bucle principal del bot.
    Cada exchange tiene su propia tarea con reconexión y backoff — un
    stream público se corta de vez en cuando, y sin reconexión el
    detector se queda mudo para siempre tras el primer corte sin que
    nadie se entere, el mismo tipo de fallo silencioso que ya se
    corrigió antes en Telegram y en el resto del proyecto.
    """

    def __init__(self) -> None:
        self._events: dict[str, deque[LiqEvent]] = {}
        self._binance_task: asyncio.Task | None = None
        self._bybit_task: asyncio.Task | None = None
        self._binance_connected = False
        self._bybit_connected = False
        self._bybit_symbols: list[str] = []
        self._bybit_resubscribe = asyncio.Event()

    # ── ciclo de vida ────────────────────────────────────────────────
    def start(self) -> None:
        self._binance_task = asyncio.create_task(self._run_binance())
        self._bybit_task = asyncio.create_task(self._run_bybit())
        log.info("Liquidaciones: arrancando (Binance + Bybit)")

    async def stop(self) -> None:
        for t in (self._binance_task, self._bybit_task):
            if t:
                t.cancel()

    @property
    def status(self) -> str:
        b = "Binance ✓" if self._binance_connected else "Binance ✗"
        y = "Bybit ✓" if self._bybit_connected else "Bybit ✗"
        return f"{b} · {y}"

    def set_symbols(self, symbols_bingx: list[str]) -> None:
        """
        Llamar cuando cambie el universo escaneado. Binance no lo
        necesita (ya manda todos los símbolos por el mismo stream);
        Bybit sí, porque exige suscripción símbolo a símbolo.
        """
        limpios = sorted({s.replace("-", "") for s in symbols_bingx})
        if limpios != self._bybit_symbols:
            self._bybit_symbols = limpios
            self._bybit_resubscribe.set()

    # ── almacenamiento ───────────────────────────────────────────────
    def _prune(self, symbol: str) -> None:
        dq = self._events.get(symbol)
        if not dq:
            return
        limite = time.time() - config.LIQ_BASELINE_MIN * 60
        while dq and dq[0].ts < limite:
            dq.popleft()

    def _add(self, ev: LiqEvent) -> None:
        dq = self._events.setdefault(ev.symbol, deque())
        dq.append(ev)
        self._prune(ev.symbol)

    # ── Binance: un solo stream, todos los símbolos ─────────────────
    async def _run_binance(self) -> None:
        backoff = 2
        while True:
            try:
                await self._listen_binance()
                backoff = 2  # conexión sana: resetea el backoff
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._binance_connected = False
                log.warning("Liquidaciones Binance: conexión perdida (%s), reintento en %ds", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _listen_binance(self) -> None:
        import websockets  # import perezoso: solo hace falta si LIQUIDATIONS_ENABLED

        async with websockets.connect(BINANCE_WS, ping_interval=20, ping_timeout=20) as ws:
            self._binance_connected = True
            log.info("Liquidaciones: conectado a Binance forceOrder")
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    o = msg.get("o", {})
                    symbol = str(o.get("s", ""))
                    side = str(o.get("S", ""))
                    qty = float(o.get("q", 0) or 0)
                    price = float(o.get("ap", 0) or o.get("p", 0) or 0)
                    if not symbol or qty <= 0 or price <= 0:
                        continue
                    self._add(LiqEvent(symbol, side, qty * price, time.time()))
                except Exception as exc:  # noqa: BLE001
                    log.debug("Liquidaciones Binance: mensaje descartado (%s)", exc)

    # ── Bybit: hay que suscribirse símbolo a símbolo ────────────────
    async def _run_bybit(self) -> None:
        backoff = 2
        while True:
            try:
                await self._listen_bybit()
                backoff = 2
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._bybit_connected = False
                log.warning("Liquidaciones Bybit: conexión perdida (%s), reintento en %ds", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _listen_bybit(self) -> None:
        import websockets

        async with websockets.connect(BYBIT_WS, ping_interval=20, ping_timeout=20) as ws:
            self._bybit_connected = True
            self._bybit_resubscribe.set()  # al reconectar hay que volver a suscribir todo
            log.info("Liquidaciones: conectado a Bybit")

            async def _subscriber() -> None:
                while True:
                    await self._bybit_resubscribe.wait()
                    self._bybit_resubscribe.clear()
                    symbols = self._bybit_symbols
                    if not symbols:
                        continue
                    # Se trocea en lotes de 50: Bybit admite varios args por
                    # mensaje, pero mejor no acercarse al límite de caracteres
                    # con un universo de cientos de símbolos.
                    for i in range(0, len(symbols), 50):
                        lote = symbols[i : i + 50]
                        args = [f"allLiquidation.{s}" for s in lote]
                        await ws.send(json.dumps({"op": "subscribe", "args": args}))
                    log.info("Liquidaciones Bybit: suscrito a %d símbolos", len(symbols))

            sub_task = asyncio.create_task(_subscriber())
            try:
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        topic = msg.get("topic", "")
                        if not topic.startswith("allLiquidation."):
                            continue
                        for ev in msg.get("data", []):
                            symbol = str(ev.get("s", ""))
                            side = "SELL" if str(ev.get("S", "")).upper() == "SELL" else "BUY"
                            qty = float(ev.get("v", 0) or 0)
                            price = float(ev.get("p", 0) or 0)
                            if not symbol or qty <= 0 or price <= 0:
                                continue
                            self._add(LiqEvent(symbol, side, qty * price, time.time()))
                    except Exception as exc:  # noqa: BLE001
                        log.debug("Liquidaciones Bybit: mensaje descartado (%s)", exc)
            finally:
                sub_task.cancel()

    # ── lectura ──────────────────────────────────────────────────────
    def cascade_status(self, symbol_bingx: str) -> dict | None:
        """
        None si no hay datos suficientes para opinar. Si los hay,
        siempre devuelve el diccionario — 'activa' dice si AHORA
        MISMO cumple velocidad + volumen, no solo si hubo alguna
        liquidación suelta en algún momento.
        """
        symbol = symbol_bingx.replace("-", "")
        self._prune(symbol)
        dq = self._events.get(symbol)
        if not dq or len(dq) < config.LIQ_MIN_EVENTS:
            return None

        ahora = time.time()
        ventana = config.LIQ_SHORT_WINDOW_SEC
        recientes = [e for e in dq if ahora - e.ts <= ventana]
        if len(recientes) < config.LIQ_MIN_EVENTS:
            return None

        total_baseline = sum(e.notional for e in dq)
        minutos_baseline = max(config.LIQ_BASELINE_MIN, 1)
        media_por_min = total_baseline / minutos_baseline
        media_en_ventana = media_por_min * (ventana / 60.0)

        notional_corto = sum(e.notional for e in recientes)
        largos = sum(e.notional for e in recientes if e.side == "SELL")  # SELL forzado = se liquidó un largo
        cortos = sum(e.notional for e in recientes if e.side == "BUY")   # BUY forzado = se liquidó un corto

        multiplicador = notional_corto / media_en_ventana if media_en_ventana > 0 else 0.0
        activa = (
            notional_corto >= config.LIQ_MIN_USD
            and multiplicador >= config.LIQ_MULTIPLIER
            and len(recientes) >= config.LIQ_MIN_EVENTS
        )

        return {
            "activa": activa,
            "notional_corto_usd": notional_corto,
            "multiplicador": multiplicador,
            "n_eventos": len(recientes),
            "lado": "LARGOS" if largos > cortos else "CORTOS",  # el lado que se está liquidando
            "largos_usd": largos,
            "cortos_usd": cortos,
            "ultimo_hace_seg": ahora - recientes[-1].ts,
        }

    def active_cascades(self, symbols_bingx: list[str]) -> list[tuple[str, dict]]:
        out: list[tuple[str, dict]] = []
        for sym in symbols_bingx:
            s = self.cascade_status(sym)
            if s and s["activa"]:
                out.append((sym, s))
        out.sort(key=lambda t: t[1]["multiplicador"], reverse=True)
        return out
