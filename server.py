"""dashboard/server.py — FastAPI + WebSocket live dashboard.

Serves a lightweight HTML dashboard on cfg.effective_port.
The healthcheck GET / is always available.
"""
from __future__ import annotations
import asyncio
import json
import time
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import uvicorn
from loguru import logger

app = FastAPI(title="UltraBot Dashboard")

# ── Shared state (written by bot, read by WS clients) ────────────────────────
_state: dict[str, Any] = {
    "status": "starting",
    "balance": 0.0,
    "positions": {},
    "scan_stats": {},
    "risk": {},
    "perf": {},
    "last_signals": [],
    "trade_metrics": {},
    "updated_at": time.time(),
}
_clients: list[WebSocket] = []


def update_state(**kwargs: Any) -> None:
    """Called from bot loops to push fresh state."""
    _state.update(kwargs)
    _state["updated_at"] = time.time()


# ── Routes ────────────────────────────────────────────────────────────────────

_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>⚡ UltraBot v3</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #0d1117; color: #c9d1d9; font-family: 'Segoe UI', monospace; font-size: 14px; }
  header { background: #161b22; padding: 12px 20px; border-bottom: 1px solid #30363d;
           display: flex; align-items: center; gap: 12px; }
  header h1 { font-size: 18px; color: #58a6ff; }
  .badge { padding: 3px 8px; border-radius: 12px; font-size: 12px; font-weight: bold; }
  .badge.running { background: #1f6feb; color: #fff; }
  .badge.halted  { background: #da3633; color: #fff; }
  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
          gap: 12px; padding: 16px; }
  .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 14px; }
  .card .label { color: #8b949e; font-size: 11px; text-transform: uppercase; letter-spacing: 1px; }
  .card .value { font-size: 22px; font-weight: bold; margin-top: 4px; color: #e6edf3; }
  .card .sub   { font-size: 12px; color: #8b949e; margin-top: 2px; }
  .green { color: #3fb950 !important; }
  .red   { color: #f85149 !important; }
  table { width: 100%; border-collapse: collapse; }
  th, td { padding: 8px 12px; text-align: left; border-bottom: 1px solid #21262d; }
  th { color: #8b949e; font-size: 11px; text-transform: uppercase; }
  section { margin: 0 16px 16px; background: #161b22; border: 1px solid #30363d; border-radius: 8px; }
  section h2 { padding: 12px 16px; font-size: 14px; border-bottom: 1px solid #30363d; color: #8b949e; }
  #status-dot { width: 10px; height: 10px; border-radius: 50%; background: #3fb950;
                box-shadow: 0 0 6px #3fb950; }
</style>
</head>
<body>
<header>
  <div id="status-dot"></div>
  <h1>⚡ UltraBot v3</h1>
  <span id="badge" class="badge running">RUNNING</span>
  <span id="updated" style="margin-left:auto;color:#8b949e;font-size:12px;"></span>
</header>

<div class="grid" id="metrics"></div>

<section>
  <h2>Open Positions</h2>
  <table><thead>
    <tr><th>Symbol</th><th>Side</th><th>Entry</th><th>Mark</th><th>PnL</th><th>Conf%</th><th>ADX</th></tr>
  </thead><tbody id="positions-body"></tbody></table>
</section>

<section>
  <h2>Latest Signals</h2>
  <table><thead>
    <tr><th>Symbol</th><th>Signal</th><th>Conf%</th><th>ADX</th><th>RSI</th><th>ATR%</th></tr>
  </thead><tbody id="signals-body"></tbody></table>
</section>

<script>
const ws = new WebSocket((location.protocol==='https:'?'wss':'ws') + '://' + location.host + '/ws');

ws.onmessage = e => {
  const d = JSON.parse(e.data);
  render(d);
};

ws.onclose = () => {
  document.getElementById('status-dot').style.background = '#f85149';
  setTimeout(() => location.reload(), 4000);
};

function pnlColor(v) { return parseFloat(v) >= 0 ? 'green' : 'red'; }

function render(d) {
  const r = d.risk || {};
  const s = d.scan_stats || {};
  const p = d.perf || {};
  document.getElementById('updated').textContent =
    'Updated ' + new Date(d.updated_at * 1000).toLocaleTimeString();

  const badge = document.getElementById('badge');
  badge.textContent = r.halted ? 'HALTED' : 'RUNNING';
  badge.className = 'badge ' + (r.halted ? 'halted' : 'running');

  const metrics = [
    { label: 'Balance', value: '$' + (d.balance||0).toFixed(2), sub: 'USDT' },
    { label: 'Day PnL', value: (r.daily_pnl_usdt>=0?'+':'')+((r.daily_pnl_usdt)||0).toFixed(2),
      cls: pnlColor(r.daily_pnl_usdt), sub: 'USDT' },
    { label: 'Win Rate', value: (r.win_rate||0) + '%', sub: r.wins+'W / '+r.losses+'L' },
    { label: 'Open Trades', value: Object.keys(d.positions||{}).length, sub: 'of ' + (r.open_count||0) },
    { label: 'Scan Speed', value: (s.last_ms||0).toFixed(0) + 'ms', sub: (s.n_scanned||0) + ' symbols' },
    { label: 'Signals', value: '🟢' + (s.n_buy||0) + ' 🔴' + (s.n_sell||0), sub: 'this scan' },
    { label: 'Total PnL', value: (r.total_pnl>=0?'+':'')+(r.total_pnl||0).toFixed(2), cls: pnlColor(r.total_pnl), sub: 'all time' },
    { label: 'Total Trades', value: p.total_trades||0, sub: 'closed' },
  ];

  document.getElementById('metrics').innerHTML = metrics.map(m =>
    `<div class="card"><div class="label">${m.label}</div>
     <div class="value ${m.cls||''}">${m.value}</div>
     <div class="sub">${m.sub||''}</div></div>`
  ).join('');

  const positions = d.positions || {};
  const posRows = Object.entries(positions).map(([sym, pos]) => {
    const pnl = parseFloat(pos.unrealizedProfit||0);
    const side = parseFloat(pos.positionAmt||0) > 0 ? '🟢 LONG' : '🔴 SHORT';
    const m = (d.trade_metrics||{})[sym] || {};
    return `<tr>
      <td>${sym}</td><td>${side}</td>
      <td>${parseFloat(pos.entryPrice||0).toFixed(4)}</td>
      <td>${parseFloat(pos.markPrice||pos.entryPrice||0).toFixed(4)}</td>
      <td class="${pnlColor(pnl)}">${pnl>=0?'+':''}${pnl.toFixed(2)}</td>
      <td>${(m.confidence||0).toFixed(0)}%</td>
      <td>${(m.adx||0).toFixed(1)}</td>
    </tr>`;
  }).join('') || '<tr><td colspan="7" style="color:#8b949e;text-align:center">No open positions</td></tr>';
  document.getElementById('positions-body').innerHTML = posRows;

  const signals = (d.last_signals||[]).slice(0,15);
  const sigRows = signals.map(s =>
    `<tr>
      <td>${s.symbol}</td>
      <td class="${s.signal==='BUY'?'green':'red'}">${s.signal==='BUY'?'🟢 BUY':'🔴 SELL'}</td>
      <td>${(s.confidence||0).toFixed(0)}%</td>
      <td>${(s.adx||0).toFixed(1)}</td>
      <td>${(s.rsi||0).toFixed(1)}</td>
      <td>${(s.atr_pct||0).toFixed(2)}%</td>
    </tr>`
  ).join('') || '<tr><td colspan="6" style="color:#8b949e;text-align:center">No signals yet</td></tr>';
  document.getElementById('signals-body').innerHTML = sigRows;
}
</script>
</body>
</html>"""


@app.get("/")
async def index() -> HTMLResponse:
    return HTMLResponse(_HTML)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "updated_at": _state["updated_at"]}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    _clients.append(ws)
    try:
        # Send current state immediately on connect
        await ws.send_text(json.dumps(_state, default=str))
        while True:
            # Push updates every 2 seconds
            await asyncio.sleep(2)
            await ws.send_text(json.dumps(_state, default=str))
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        _clients.discard(ws) if hasattr(_clients, "discard") else None
        if ws in _clients:
            _clients.remove(ws)


# ── Start ─────────────────────────────────────────────────────────────────────

async def start_dashboard() -> None:
    from core.config import cfg
    port = cfg.effective_port
    config = uvicorn.Config(
        app, host="0.0.0.0", port=port,
        log_level="warning", access_log=False
    )
    server = uvicorn.Server(config)
    asyncio.create_task(server.serve())
    logger.info(f"Dashboard running on http://0.0.0.0:{port}")
