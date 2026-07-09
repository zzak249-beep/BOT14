"""
Position Monitor — detecta cierres de posiciones y retroalimenta el sistema
================================================================================
Sondea periódicamente las posiciones abiertas en BingX. Cuando detecta que
una posición que estaba abierta ya no lo está, la considera cerrada, obtiene
el PnL realizado y:
  1. Registra el resultado en el journal
  2. Actualiza el circuit breaker diario (risk_manager)
  3. Actualiza la memoria estadística del setup (setup_memory)
  4. Libera la exposición correlacionada (correlation_manager)
  5. Libera el riesgo abierto reservado (risk_manager)
"""
import logging
import time

log = logging.getLogger("position_monitor")


class PositionMonitor:
    def __init__(self, client, journal, risk_mgr, setup_memory, corr_mgr, recently_closed=None):
        self.client = client
        self.journal = journal
        self.risk_mgr = risk_mgr
        self.setup_memory = setup_memory
        self.corr_mgr = corr_mgr
        # symbol -> metadata registrada al abrir (setup_key, risk_pct, etc.)
        self.tracked = {}
        # symbol -> timestamp_ms del último CIERRE. Compartido con
        # execute_signal para el cooldown post-cierre: se observó en real
        # (UNI-USDT) que el bot cerraba una posición y reabría la misma
        # moneda 1 minuto después, 3 veces en el día — el dedup de apertura
        # no cubre este caso porque la posición anterior ya no existe.
        self.recently_closed = recently_closed if recently_closed is not None else {}

    def register_open(self, symbol, setup_key, risk_pct, opened_at_ms, side=None,
                       sl_price=None, tp_price=None, sl_placed=True, tp_placed=True):
        self.tracked[symbol] = {"setup_key": setup_key, "risk_pct": risk_pct,
                                "opened_at_ms": opened_at_ms, "side": side,
                                "sl_price": sl_price, "tp_price": tp_price,
                                "needs_sl": not sl_placed, "needs_tp": not tp_placed}

    async def check_closures(self, balance):
        if not self.tracked:
            return

        open_positions = await self.client.get_open_positions()
        open_symbols = {p["symbol"] for p in open_positions}

        closed_symbols = [s for s in self.tracked if s not in open_symbols]
        for symbol in closed_symbols:
            meta = self.tracked.pop(symbol)
            await self._handle_closure(symbol, meta, balance)

        # AUTO-REPARACIÓN de SL/TP: si una posición quedó abierta con el SL
        # o el TP sin colocar, reintentarlos en cada ciclo hasta que entren —
        # antes solo se logueaba el 🚨 y la posición quedaba desprotegida
        # esperando intervención manual. El SL va primero: es la protección;
        # el TP es la toma de ganancia, importante pero no crítico.
        for symbol, meta in self.tracked.items():
            if symbol not in open_symbols:
                continue
            if meta.get("needs_sl") and meta.get("sl_price"):
                try:
                    result = await self.client._place_stop(
                        symbol, meta.get("side") or "LONG", "STOP_MARKET",
                        meta["sl_price"], None)
                    if result.get("code") == 0:
                        meta["needs_sl"] = False
                        log.info("✅ [%s] SL auto-reparado: colocado en %s tras fallo inicial",
                                  symbol, meta["sl_price"])
                    else:
                        log.warning("🚨 [%s] Auto-reparación de SL sigue fallando: %s — se reintenta el próximo ciclo",
                                     symbol, result)
                except Exception as e:
                    log.warning("🚨 [%s] Auto-reparación de SL falló con excepción: %s", symbol, e)
            if meta.get("needs_tp") and meta.get("tp_price"):
                try:
                    result = await self.client._place_stop(
                        symbol, meta.get("side") or "LONG", "TAKE_PROFIT_MARKET",
                        meta["tp_price"], None)
                    if result.get("code") == 0:
                        meta["needs_tp"] = False
                        log.info("✅ [%s] TP auto-reparado: colocado en %s tras fallo inicial",
                                  symbol, meta["tp_price"])
                    else:
                        log.warning("[%s] Auto-reparación de TP sigue fallando: %s — se reintenta el próximo ciclo",
                                     symbol, result)
                except Exception as e:
                    log.warning("[%s] Auto-reparación de TP falló con excepción: %s", symbol, e)

    async def _handle_closure(self, symbol, meta, balance):
        # FIX: income_history traía las últimas 5 entradas del símbolo sin
        # filtrar por fecha — si el bot ya había operado ese símbolo antes,
        # PnL de trades viejos se sumaba al del trade que se acaba de cerrar.
        # Ahora solo cuenta lo posterior a opened_at_ms.
        try:
            income = await self.client.get_income_history(symbol, limit=10)
        except Exception as e:
            log.warning("[%s] No se pudo obtener income history al cerrar: %s", symbol, e)
            income = []

        opened_at_ms = meta.get("opened_at_ms", 0)
        income_this_trade = [i for i in income if i.get("time", 0) >= opened_at_ms]
        if len(income) > len(income_this_trade):
            log.debug("[%s] %d entradas de income descartadas por ser anteriores a la apertura",
                      symbol, len(income) - len(income_this_trade))

        pnl = sum(i["income"] for i in income_this_trade) if income_this_trade else 0.0
        is_win = pnl > 0

        self.risk_mgr.register_realized_pnl(pnl, balance)
        self.risk_mgr.release_open_risk(meta["risk_pct"])
        self.corr_mgr.register_close(symbol)
        self.setup_memory.record_outcome(meta["setup_key"], is_win)

        self.recently_closed[symbol] = int(time.time() * 1000)
        self.journal.record({
            "symbol": symbol, "event": "position_closed", "side": meta.get("side"),
            "pnl": pnl, "is_win": is_win, "setup_key": meta["setup_key"],
        })
        log.info("[%s] Posición cerrada | PnL=%.4f | %s | setup=%s",
                  symbol, pnl, "WIN" if is_win else "LOSS", meta["setup_key"])
