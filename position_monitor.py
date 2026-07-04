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

log = logging.getLogger("position_monitor")


class PositionMonitor:
    def __init__(self, client, journal, risk_mgr, setup_memory, corr_mgr):
        self.client = client
        self.journal = journal
        self.risk_mgr = risk_mgr
        self.setup_memory = setup_memory
        self.corr_mgr = corr_mgr
        # symbol -> metadata registrada al abrir (setup_key, risk_pct, etc.)
        self.tracked = {}

    def register_open(self, symbol, setup_key, risk_pct, opened_at_ms):
        self.tracked[symbol] = {"setup_key": setup_key, "risk_pct": risk_pct,
                                "opened_at_ms": opened_at_ms}

    async def check_closures(self, balance):
        if not self.tracked:
            return

        open_positions = await self.client.get_open_positions()
        open_symbols = {p["symbol"] for p in open_positions}

        closed_symbols = [s for s in self.tracked if s not in open_symbols]
        for symbol in closed_symbols:
            meta = self.tracked.pop(symbol)
            await self._handle_closure(symbol, meta, balance)

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

        self.journal.record({
            "symbol": symbol, "event": "position_closed",
            "pnl": pnl, "is_win": is_win, "setup_key": meta["setup_key"],
        })
        log.info("[%s] Posición cerrada | PnL=%.4f | %s | setup=%s",
                  symbol, pnl, "WIN" if is_win else "LOSS", meta["setup_key"])
