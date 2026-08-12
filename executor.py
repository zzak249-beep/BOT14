"""
Traduce una Signal en accion real (o de papel, en MODE=SIGNAL).

Lecciones de bots anteriores aplicadas aqui a proposito:
  - El tamaño de posicion se calcula por RIESGO, no por "% equity * leverage".
    qty = (equity * riesgo%) / distancia_al_SL. El leverage NO entra en esta
    formula -- solo afecta el margen que el exchange retiene, no el qty.
    Multiplicar por leverage aqui fue exactamente el bug que inflaba el PnL
    10-36x en un bot anterior.
  - El PnL se calcula en moneda absoluta (exit-entry)*qty*signo, nunca
    aplicando el leverage como multiplicador de un porcentaje -- eso es
    lo que disparaba circuit breakers falsos.
  - Tras colocar la orden de entrada, se relee la posicion real en BingX
    (positionAmt) antes de colocar SL/TP, en vez de asumir que se lleno
    exactamente el qty solicitado.
  - Hedge vs One-way: en HEDGE, side+positionSide (LONG/SHORT) definen la
    direccion, sin reduceOnly. En ONEWAY, positionSide=BOTH y reduceOnly
    para cerrar/reducir.
"""
import logging
import time
from typing import Optional

import config as cfg
from bingx_client import BingXClient, BingXError, normalize_symbol
from state import StateManager
from strategy import Signal
from telegram_notifier import TelegramNotifier, format_signal, format_position_closed

log = logging.getLogger("executor")


def _tier_of(symbol: str) -> str:
    """major/altcoin -- valida (o descarta) con datos reales del bot la
    hipotesis del backtest de Pine: sweeps de liquidez parecen funcionar
    en altcoins de menor capitalizacion y no en majors de alta liquidez."""
    norm = normalize_symbol(symbol, cfg.QUOTE_ASSET)
    return "major" if norm in cfg.MAJOR_SYMBOLS else "altcoin"


def _entry_side(direction: str) -> tuple:
    """(side, positionSide) para ABRIR una posicion."""
    if cfg.POSITION_MODE == "HEDGE":
        return ("BUY", "LONG") if direction == "LONG" else ("SELL", "SHORT")
    return ("BUY", "BOTH") if direction == "LONG" else ("SELL", "BOTH")


def _exit_side(direction: str) -> tuple:
    """(side, positionSide) para CERRAR/REDUCIR una posicion existente."""
    if cfg.POSITION_MODE == "HEDGE":
        return ("SELL", "LONG") if direction == "LONG" else ("BUY", "SHORT")
    return ("SELL", "BOTH") if direction == "LONG" else ("BUY", "BOTH")


async def _get_equity(client: BingXClient) -> float:
    try:
        bal = await client.get_balance()
        if isinstance(bal, dict):
            for key in ("balance", "equity", "availableMargin"):
                if key in bal:
                    return float(bal[key])
            # algunas cuentas devuelven una lista bajo 'balance' con un dict por moneda
            inner = bal.get("balance")
            if isinstance(inner, dict) and "equity" in inner:
                return float(inner["equity"])
        if isinstance(bal, list) and bal:
            return float(bal[0].get("equity", bal[0].get("balance", 0)))
    except (BingXError, TypeError, ValueError) as e:
        log.error("No se pudo leer el balance: %s", e)
    return 0.0


async def handle_signal(sig: Signal, client: BingXClient, state: StateManager, notifier: TelegramNotifier) -> None:
    if sig.symbol in state.positions:
        return  # ya hay una posicion abierta en este simbolo, no se apila

    if state.open_position_count() >= cfg.MAX_CONCURRENT_POSITIONS:
        log.info("%s: señal descartada, limite de posiciones concurrentes alcanzado.", sig.symbol)
        return

    if state.trades_today() >= cfg.MAX_TRADES_PER_DAY:
        log.info("%s: señal descartada, limite diario de operaciones alcanzado.", sig.symbol)
        return

    await notifier.send(format_signal(sig, cfg.MODE))

    if cfg.MODE == "SIGNAL":
        state.register_trade_opened()
        state.open_position(sig.symbol, {
            "direction": sig.direction, "entry": sig.entry, "sl": sig.sl,
            "tp1": sig.tp1, "tp2": sig.tp2, "qty": 0.0, "paper": True,
            "partial_done": False, "kill_zone": sig.kill_zone, "path": sig.path, "tier": _tier_of(sig.symbol),
            "opened_at": int(time.time() * 1000),
        })
        return

    # ── MODE=LIVE a partir de aqui ──
    equity = await _get_equity(client)
    if equity <= 0:
        log.error("%s: equity=0 o no disponible, no se abre posicion.", sig.symbol)
        await notifier.send(f"⚠️ {sig.symbol}: no se pudo leer equity, señal ignorada.")
        return

    r_dist = abs(sig.entry - sig.sl)
    if r_dist <= 0:
        return
    raw_qty = (equity * cfg.RISK_PCT / 100.0) / r_dist
    qty = client.round_qty(sig.symbol, raw_qty)
    if qty <= 0:
        log.warning("%s: qty calculado es 0 tras redondear, se omite.", sig.symbol)
        return

    side, pos_side = _entry_side(sig.direction)

    try:
        await client.set_leverage(sig.symbol, pos_side, cfg.LEVERAGE)
    except BingXError as e:
        log.warning("%s: no se pudo fijar leverage (%s), se continua con el valor actual.", sig.symbol, e)

    try:
        await client.place_order(sig.symbol, side, pos_side, "MARKET", quantity=qty)
    except BingXError as e:
        log.error("%s: fallo al abrir posicion: %s", sig.symbol, e)
        await notifier.send(f"❌ {sig.symbol}: fallo al abrir — {e.msg}")
        return

    # Releer la posicion real (positionAmt) en vez de asumir que se lleno `qty`.
    real_qty, real_entry = qty, sig.entry
    try:
        positions = await client.get_positions(sig.symbol)
        for p in positions:
            amt = float(p.get("positionAmt", p.get("positionAmt", 0)) or 0)
            if abs(amt) > 0:
                real_qty = abs(amt)
                real_entry = float(p.get("avgPrice", p.get("entryPrice", sig.entry)) or sig.entry)
                break
    except (BingXError, TypeError, ValueError) as e:
        log.warning("%s: no se pudo releer la posicion real, se usan valores calculados (%s).", sig.symbol, e)

    sl_px = client.round_price(sig.symbol, sig.sl)
    tp1_px = client.round_price(sig.symbol, sig.tp1)
    tp2_px = client.round_price(sig.symbol, sig.tp2)
    exit_side, exit_pos_side = _exit_side(sig.direction)
    reduce_only = None if cfg.POSITION_MODE == "HEDGE" else True

    tp1_qty = client.round_qty(sig.symbol, real_qty * cfg.PARTIAL_TP_PCT / 100.0) if cfg.USE_PARTIAL_TP else 0.0
    tp2_qty = client.round_qty(sig.symbol, real_qty - tp1_qty) if cfg.USE_PARTIAL_TP else real_qty

    try:
        await client.place_order(sig.symbol, exit_side, exit_pos_side, "STOP_MARKET",
                                  quantity=real_qty, stop_price=sl_px, reduce_only=reduce_only)
        if cfg.USE_PARTIAL_TP and tp1_qty > 0:
            await client.place_order(sig.symbol, exit_side, exit_pos_side, "TAKE_PROFIT_MARKET",
                                      quantity=tp1_qty, stop_price=tp1_px, reduce_only=reduce_only)
        if tp2_qty > 0:
            await client.place_order(sig.symbol, exit_side, exit_pos_side, "TAKE_PROFIT_MARKET",
                                      quantity=tp2_qty, stop_price=tp2_px, reduce_only=reduce_only)
    except BingXError as e:
        log.error("%s: entrada abierta pero fallo al colocar SL/TP: %s -- REVISAR MANUALMENTE.", sig.symbol, e)
        await notifier.send(f"🚨 {sig.symbol}: posicion abierta SIN SL/TP completo ({e.msg}). Revisa manualmente.")

    state.register_trade_opened()
    state.open_position(sig.symbol, {
        "direction": sig.direction, "entry": real_entry, "sl": sig.sl,
        "tp1": sig.tp1, "tp2": sig.tp2, "qty": real_qty, "paper": False,
        "partial_done": False, "kill_zone": sig.kill_zone, "path": sig.path, "tier": _tier_of(sig.symbol),
        "equity_at_entry": equity,
    })


async def manage_open_positions(client: BingXClient, state: StateManager, notifier: TelegramNotifier) -> None:
    """Revisa posiciones que el bot tiene registradas: detecta cierre (SL/TP
    tocado) y, tras el TP1 parcial, mueve el SL al break-even."""
    if not state.positions:
        return

    if cfg.MODE == "SIGNAL":
        return  # sin posiciones reales que reconciliar contra el exchange

    try:
        live_positions = await client.get_positions()
    except BingXError as e:
        log.error("No se pudieron leer posiciones para gestion: %s", e)
        return

    live_by_symbol = {}
    for p in live_positions:
        amt = float(p.get("positionAmt", 0) or 0)
        if abs(amt) > 0:
            live_by_symbol[p.get("symbol")] = p

    for symbol in list(state.positions.keys()):
        pos = state.positions[symbol]
        live = live_by_symbol.get(symbol)

        if live is None:
            # La posicion ya no existe en BingX -> se cerro (SL, TP o manual)
            entry = pos["entry"]
            sign = 1 if pos["direction"] == "LONG" else -1
            # No conocemos el precio exacto de cierre sin consultar el historial de
            # ordenes; usamos el SL/TP mas probable solo para el signo del resultado.
            approx_exit = pos.get("tp2", entry)
            win = (approx_exit - entry) * sign > 0
            state.close_position(symbol, win=win, kill_zone=pos.get("kill_zone"), path=pos.get("path"), tier=pos.get("tier"))
            await notifier.send(format_position_closed(symbol, "SL/TP", None))
            continue

        if not pos.get("partial_done") and cfg.USE_PARTIAL_TP:
            live_qty = abs(float(live.get("positionAmt", 0)))
            original_qty = pos.get("qty", live_qty)
            if original_qty > 0 and live_qty < original_qty * 0.97:
                # El TP1 ya redujo la posicion -> mover el SL restante a break-even
                direction = pos["direction"]
                entry = pos["entry"]
                r = abs(entry - pos["sl"])
                be = entry + cfg.BE_OFFSET_R * r if direction == "LONG" else entry - cfg.BE_OFFSET_R * r
                exit_side, exit_pos_side = _exit_side(direction)
                reduce_only = None if cfg.POSITION_MODE == "HEDGE" else True
                try:
                    for o in await client.get_open_orders(symbol):
                        if o.get("type") == "STOP_MARKET":
                            await client.cancel_order(symbol, o.get("orderId"))
                    await client.place_order(symbol, exit_side, exit_pos_side, "STOP_MARKET",
                                              quantity=live_qty, stop_price=client.round_price(symbol, be),
                                              reduce_only=reduce_only)
                    pos["partial_done"] = True
                    pos["qty"] = live_qty
                    await notifier.send(f"🔒 {symbol}: TP1 tocado, SL movido a break-even.")
                except BingXError as e:
                    log.error("%s: fallo moviendo SL a BE: %s", symbol, e)


async def manage_paper_positions(client: BingXClient, state: StateManager, notifier: TelegramNotifier) -> None:
    """Equivalente a manage_open_positions() pero para MODE=SIGNAL, donde
    no hay posicion real en BingX que consultar. Sin esto, una señal de
    papel se queda en state.positions PARA SIEMPRE: nunca se sabe si
    hubiera ganado o perdido (cero tracking de rentabilidad), y en cuanto
    se acumulan MAX_CONCURRENT_POSITIONS de estas, el bot deja de poder
    registrar señales nuevas -- confirmado en produccion: 5 posiciones
    de papel atascadas durante varios ciclos seguidos, justo en el limite."""
    if cfg.MODE != "SIGNAL" or not state.positions:
        return

    for symbol in list(state.positions.keys()):
        pos = state.positions[symbol]
        if not pos.get("paper"):
            continue
        try:
            candles = await client.get_klines(symbol, cfg.TIMEFRAME, 2)
        except BingXError as e:
            log.debug("%s: fallo trayendo precio para posicion de papel (%s)", symbol, e)
            continue
        if not candles:
            continue
        price = candles[-1].close
        direction = pos.get("direction")
        sl, tp2 = pos.get("sl"), pos.get("tp2")
        if sl is None or tp2 is None:
            continue

        hit_tp = price >= tp2 if direction == "LONG" else price <= tp2
        hit_sl = price <= sl if direction == "LONG" else price >= sl
        if not (hit_tp or hit_sl):
            continue

        win = hit_tp and not hit_sl  # si ambos se tocaron en el mismo hueco de vela, se cuenta como perdedor (conservador)
        state.close_position(symbol, win=win, kill_zone=pos.get("kill_zone"), path=pos.get("path"), tier=pos.get("tier"))
        elapsed_min = (int(time.time() * 1000) - pos.get("opened_at", 0)) / 60000 if pos.get("opened_at") else None
        extra = f" ({elapsed_min:.0f} min)" if elapsed_min is not None else ""
        log.info("%s: posicion de papel cerrada por %s%s", symbol, "TP" if win else "SL", extra)
        icon = "✅" if win else "❌"
        await notifier.send(f"{icon} <b>[Papel] Cierre {'TP' if win else 'SL'}</b> — {symbol}{extra}")


async def reconcile_on_startup(client: BingXClient, state: StateManager, notifier: TelegramNotifier) -> None:
    """Se corre UNA VEZ al arrancar. Compara el estado guardado contra lo
    que realmente hay en BingX y avisa de discrepancias en vez de
    dejar ordenes huerfanas acumulandose entre redeploys."""
    if cfg.MODE == "SIGNAL":
        return
    log.info("Reconciliando estado contra BingX...")
    try:
        live_positions = await client.get_positions()
    except BingXError as e:
        log.error("No se pudo reconciliar (fallo al leer posiciones): %s", e)
        return

    live_symbols = {p.get("symbol") for p in live_positions if abs(float(p.get("positionAmt", 0) or 0)) > 0}

    for symbol in list(state.positions.keys()):
        if symbol not in live_symbols:
            log.warning("%s: el estado dice que hay posicion abierta pero BingX no la tiene. Se limpia del estado.", symbol)
            state.close_position(symbol)

    huerfanas = live_symbols - set(state.positions.keys())
    if huerfanas:
        log.warning("Posiciones abiertas en BingX que el bot NO reconoce: %s -- no se tocan automaticamente.", huerfanas)
        await notifier.send(f"⚠️ Posiciones no rastreadas por el bot: {', '.join(sorted(huerfanas))}")

    state.save()
    log.info("Reconciliacion completa.")
