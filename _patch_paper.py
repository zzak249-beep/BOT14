import io

# ══ config.py: DRY_RUN_BALANCE + version ══
c = io.open('config.py', encoding='utf-8').read()
anchor = 'DRY_RUN = _b("DRY_RUN", True)  # True = solo loguea señales, no envía órdenes'
assert anchor in c
c = c.replace(anchor, anchor + '''
DRY_RUN_BALANCE = _f("DRY_RUN_BALANCE", 125.0)  # balance simulado en DRY_RUN
# cuando la cuenta real esta vacia: el bot observa, dimensiona y paper-tradea
# igual. Al fondear y pasar DRY_RUN=False, esto queda inerte.''')
assert 'CODE_VERSION = "2026-07-12-liquidity-floor"' in c
c = c.replace('CODE_VERSION = "2026-07-12-liquidity-floor"',
              'CODE_VERSION = "2026-07-18-paper-observer"')
io.open('config.py', 'w', encoding='utf-8').write(c)

# ══ main.py: balance simulado (solo DRY_RUN) + meta extendida al registrar ══
m = io.open('main.py', encoding='utf-8').read()
old_bal = '''    balance = await client.get_balance_usdt()
    if balance <= 0:
        log.warning("[%s] Balance no disponible o cero, se omite ciclo", tag)
        return'''
assert old_bal in m
m = m.replace(old_bal, '''    balance = await client.get_balance_usdt()
    if balance <= 0:
        if config.DRY_RUN:
            # Observacion sin fondos: balance simulado para sizing y journal.
            if not getattr(run_cycle, "_warned_balance", False):
                log.warning("[%s] Balance real invalido (%s) — DRY_RUN activo: "
                            "sigo con balance simulado de %.2f USDT "
                            "(DRY_RUN_BALANCE). Con fondos + DRY_RUN=False "
                            "vuelve a real.", tag, balance,
                            config.DRY_RUN_BALANCE)
                run_cycle._warned_balance = True
            balance = config.DRY_RUN_BALANCE
        else:
            log.warning("[%s] Balance no disponible o cero, se omite ciclo", tag)
            return''')

# register_open: pasar entry/qty/engine para el paper close
old_reg = 'pos_monitor.register_open(symbol, setup_key, risk_pct, opened_at_ms'
assert old_reg in m
i = m.index(old_reg)
j = m.index(')', i)
call = m[i:j]
assert 'entry=' not in call
m = m[:j] + ', entry=entry, qty=qty, engine=sig.get("engine")' + m[j:]
io.open('main.py', 'w', encoding='utf-8').write(m)

# ══ position_monitor.py: paper trading en DRY_RUN ══
p = io.open('position_monitor.py', encoding='utf-8').read()
if 'import config' not in p.split('class ')[0]:
    p = p.replace('import logging', 'import logging\n\nimport config', 1)

old_sig = '''    def register_open(self, symbol, setup_key, risk_pct, opened_at_ms, side=None,
                       sl_price=None, tp_price=None, sl_placed=True, tp_placed=True):
        self.tracked[symbol] = {"setup_key": setup_key, "risk_pct": risk_pct,
                                "opened_at_ms": opened_at_ms, "side": side,
                                "sl_price": sl_price, "tp_price": tp_price,
                                "needs_sl": not sl_placed, "needs_tp": not tp_placed}'''
assert old_sig in p
p = p.replace(old_sig, '''    def register_open(self, symbol, setup_key, risk_pct, opened_at_ms, side=None,
                       sl_price=None, tp_price=None, sl_placed=True, tp_placed=True,
                       entry=None, qty=None, engine=None):
        self.tracked[symbol] = {"setup_key": setup_key, "risk_pct": risk_pct,
                                "opened_at_ms": opened_at_ms, "side": side,
                                "sl_price": sl_price, "tp_price": tp_price,
                                "needs_sl": not sl_placed, "needs_tp": not tp_placed,
                                "entry": entry, "qty": qty, "engine": engine}''')

old_chk = '''    async def check_closures(self, balance):
        if not self.tracked:
            return

        open_positions = await self.client.get_open_positions()'''
assert old_chk in p
p = p.replace(old_chk, '''    async def check_closures(self, balance):
        if not self.tracked:
            return

        # ── PAPER TRADING en DRY_RUN ──
        # Las posiciones secas no existen en BingX: consultar el exchange las
        # daria por "cerradas" al instante con PnL basura. Se cierran contra
        # velas reales (SL/TP tocados; misma vela ambos = SL, conservador),
        # se journalean como simulated=True y alimentan riesgo y dedupe —
        # el DRY_RUN se comporta como se comportaria en vivo.
        if config.DRY_RUN:
            await self._paper_check_closures(balance)
            return

        open_positions = await self.client.get_open_positions()''')

old_handle = '    async def _handle_closure(self, symbol, meta, balance):'
assert old_handle in p
paper = '''    async def _paper_check_closures(self, balance):
        import time as _t
        for symbol in list(self.tracked.keys()):
            meta = self.tracked[symbol]
            entry = meta.get("entry")
            qty = meta.get("qty") or 0.0
            sl = meta.get("sl_price")
            tp = meta.get("tp_price")
            side = (meta.get("side") or "LONG").upper()
            if entry is None or sl is None or tp is None:
                continue
            try:
                kl = await self.client.get_klines(symbol, config.ENTRY_TF,
                                                  limit=250)
            except Exception as e:
                log.warning("[%s] paper: sin velas para evaluar cierre (%s)",
                            symbol, e)
                continue
            exit_price, result = None, None
            for c in kl[:-1]:  # solo velas cerradas
                if (c.get("time") or 0) < meta.get("opened_at_ms", 0):
                    continue
                if side == "LONG":
                    if c["low"] <= sl:
                        exit_price, result = sl, "sl"
                        break
                    if c["high"] >= tp:
                        exit_price, result = tp, "tp"
                        break
                else:
                    if c["high"] >= sl:
                        exit_price, result = sl, "sl"
                        break
                    if c["low"] <= tp:
                        exit_price, result = tp, "tp"
                        break
            if exit_price is None:
                continue
            self.tracked.pop(symbol)
            pnl = ((exit_price - entry) if side == "LONG"
                   else (entry - exit_price)) * qty
            self.risk_mgr.release_open_risk(meta.get("risk_pct", 0))
            self.risk_mgr.register_realized_pnl(pnl, balance)
            if self.recently_closed is not None:
                self.recently_closed[symbol] = int(_t.time() * 1000)
            self.journal.record({
                "symbol": symbol, "event": "position_closed",
                "side": side, "simulated": True, "result": result,
                "pnl": round(pnl, 4), "exit": exit_price,
                "entry": entry, "qty": qty,
                "setup_key": meta.get("setup_key"),
                "engine": meta.get("engine"),
                "is_win": pnl > 0,
                "held_min": round((_t.time() * 1000
                                   - meta.get("opened_at_ms", 0)) / 60000),
                "ts": int(_t.time() * 1000),
            })
            log.info("[%s] PAPER cerrada en %s (%s) | PnL simulado=%.4f USDT",
                     symbol, result.upper(), side, pnl)

    async def _handle_closure(self, symbol, meta, balance):'''
p = p.replace(old_handle, paper)
io.open('position_monitor.py', 'w', encoding='utf-8').write(p)
print("patch renewed-love aplicado")
