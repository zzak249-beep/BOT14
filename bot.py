"""
UltraBot v3 — Single-file version.
All modules (config, risk, database, exchange, indicators, telegram, dashboard)
are embedded here so no subpackage folders are needed.
"""
from __future__ import annotations
import asyncio, hashlib, hmac, json, math, os, sys, time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print("✅ uvloop active")
except ImportError:
    print("ℹ️  uvloop not available")

import aiohttp
import aiosqlite
import numpy as np
from loguru import logger
from rich.console import Console
from rich.table import Table
from rich import box as rbox

try:
    from numba import njit  # type: ignore
except ImportError:
    def njit(*a, **kw):
        def d(f): return f
        return d

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
def _env(key, default=""):
    return os.environ.get(key, default)

def _envf(key, default):
    try: return float(os.environ.get(key, default))
    except: return float(default)

def _envi(key, default):
    try: return int(os.environ.get(key, default))
    except: return int(default)

def _envb(key, default):
    return os.environ.get(key, str(default)).lower() in ("1", "true", "yes")

class _Cfg:
    bingx_api_key    = _env("BINGX_API_KEY")
    bingx_secret_key = _env("BINGX_SECRET_KEY")
    telegram_token   = _env("TELEGRAM_TOKEN")
    telegram_chat_id = _env("TELEGRAM_CHAT_ID")
    timeframe        = _env("TIMEFRAME", "15m")
    confirm_tf       = _env("CONFIRM_TF", "1h")
    trend_tf         = _env("TREND_TF",   "4h")
    period           = _envi("PERIOD",     25)
    adx_len          = _envi("ADX_LEN",    14)
    adx_thresh       = _envf("ADX_THRESH", 28.0)
    rsi_len          = _envi("RSI_LEN",    14)
    rsi_ob           = _envf("RSI_OB",     72.0)
    rsi_os           = _envf("RSI_OS",     28.0)
    vol_spike_mult   = _envf("VOL_SPIKE_MULT", 1.8)
    min_confidence   = _envf("MIN_CONFIDENCE", 55.0)
    min_volume_usdt  = _envf("MIN_VOLUME_USDT", 3_000_000)
    top_n_symbols    = _envi("TOP_N_SYMBOLS",  60)
    blacklist        = set(s.strip() for s in _env("BLACKLIST","").split(",") if s.strip())
    leverage         = _envi("LEVERAGE",   5)
    risk_pct         = _envf("RISK_PCT",   1.0)
    max_open_trades  = _envi("MAX_OPEN_TRADES", 5)
    sl_pct           = _envf("SL_PCT",     2.0)
    tp_pct           = _envf("TP_PCT",     4.0)
    trailing_sl      = _envb("TRAILING_SL", True)
    max_drawdown_pct = _envf("MAX_DRAWDOWN_PCT", 8.0)
    daily_loss_limit = _envf("DAILY_LOSS_LIMIT", 4.0)
    max_consecutive_losses = _envi("MAX_CONSECUTIVE_LOSSES", 5)
    cooldown_after_loss    = _envi("COOLDOWN_AFTER_LOSS", 300)
    scan_interval    = _envi("SCAN_INTERVAL",  8)
    http_timeout     = _envi("HTTP_TIMEOUT",   6)
    dashboard_enabled= _envb("DASHBOARD_ENABLED", True)
    dashboard_port   = _envi("DASHBOARD_PORT", 8080)
    candles_needed   = 80

    @property
    def effective_port(self):
        return int(os.environ.get("PORT", self.dashboard_port))

cfg = _Cfg()

# ══════════════════════════════════════════════════════════════════════════════
#  RISK MANAGER
# ══════════════════════════════════════════════════════════════════════════════
class RiskManager:
    def __init__(self):
        self._balance = self._peak = self._day_start = 0.0
        self._day_ts = time.time()
        self._daily_pnl = self._total_pnl = 0.0
        self._consec_loss = self._win_count = self._loss_count = 0
        self._cooldown_until = 0.0
        self._halted = False
        self._halt_reason = ""
        self._open_symbols: set[str] = set()

    def set_balance(self, b: float):
        if self._peak == 0: self._peak = b
        if self._day_start == 0: self._day_start = b
        self._balance = b
        if time.time() - self._day_ts > 86400:
            self._day_ts = time.time(); self._day_start = b; self._daily_pnl = 0
        if b > self._peak: self._peak = b
        dd = (self._peak - b) / self._peak * 100 if self._peak else 0
        if dd >= cfg.max_drawdown_pct and not self._halted:
            self._halt(f"Max drawdown {dd:.1f}%")

    def can_trade(self, balance: float):
        if self._halted: return False, f"Halted: {self._halt_reason}"
        if time.time() < self._cooldown_until:
            return False, f"Cooldown ({int(self._cooldown_until-time.time())}s)"
        if len(self._open_symbols) >= cfg.max_open_trades:
            return False, "Max trades"
        if self._day_start > 0:
            dloss = -self._daily_pnl / self._day_start * 100
            if dloss >= cfg.daily_loss_limit: return False, f"Daily limit {dloss:.1f}%"
        return True, "ok"

    def correlation_ok(self, sym: str):
        base = sym.split("-")[0]
        return not any(s.split("-")[0] == base for s in self._open_symbols if s != sym)

    def position_size(self, balance, n_open, confidence, atr_pct):
        base = balance * cfg.risk_pct / 100
        cs = 0.7 + (confidence / 100) * 0.6
        ss = 0.7 ** n_open if n_open else 1.0
        vs = min(1.0, 1.5 / (atr_pct + 0.5)) if atr_pct > 0 else 1.0
        return round(max(5.0, min(base * cs * ss * vs, balance * 0.20)), 2)

    def dynamic_sl_tp(self, price, side, atr):
        if atr > 0 and price > 0:
            atr_pct = atr / price * 100
            sl_pct = max(cfg.sl_pct, min(atr_pct * 1.5, cfg.sl_pct * 2))
            tp_pct = sl_pct * (cfg.tp_pct / cfg.sl_pct)
        else:
            sl_pct, tp_pct = cfg.sl_pct, cfg.tp_pct
        if side == "BUY":
            sl = round(price * (1 - sl_pct / 100), 8)
            tp = round(price * (1 + tp_pct / 100), 8)
        else:
            sl = round(price * (1 + sl_pct / 100), 8)
            tp = round(price * (1 - tp_pct / 100), 8)
        return sl, tp, round(sl_pct, 2), round(tp_pct, 2)

    def record_open(self, sym): self._open_symbols.add(sym)
    def record_close(self, sym, pnl, balance):
        self._open_symbols.discard(sym)
        self._daily_pnl += pnl; self._total_pnl += pnl
        self.set_balance(balance)
        if pnl >= 0: self._win_count += 1; self._consec_loss = 0
        else:
            self._loss_count += 1; self._consec_loss += 1
            if self._consec_loss >= cfg.max_consecutive_losses:
                self._cooldown_until = time.time() + cfg.cooldown_after_loss

    def _halt(self, reason):
        self._halted = True; self._halt_reason = reason
        logger.critical(f"HALTED: {reason}")

    @property
    def is_halted(self): return self._halted

    def summary(self):
        tt = self._win_count + self._loss_count
        return {
            "balance": round(self._balance, 2), "peak": round(self._peak, 2),
            "daily_pnl_usdt": round(self._daily_pnl, 2),
            "total_pnl": round(self._total_pnl, 2),
            "win_rate": round(self._win_count / tt * 100, 1) if tt else 0.0,
            "wins": self._win_count, "losses": self._loss_count,
            "consec_losses": self._consec_loss, "open_count": len(self._open_symbols),
            "halted": self._halted, "halt_reason": self._halt_reason,
            "cooldown": max(0, int(self._cooldown_until - time.time())),
        }

# ══════════════════════════════════════════════════════════════════════════════
#  DATABASE
# ══════════════════════════════════════════════════════════════════════════════
DB_PATH = "data/ultrabot.db"

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, side TEXT, entry_price REAL, exit_price REAL,
            qty REAL, size_usdt REAL, sl REAL, tp REAL,
            pnl REAL, pnl_pct REAL, reason TEXT, metrics TEXT,
            opened_at TEXT, closed_at TEXT, duration_s INTEGER
        );
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, signal TEXT, metrics TEXT, executed INTEGER, ts TEXT
        );
        """)
        await db.commit()
    logger.info("DB initialised")

async def save_trade_open(symbol, side, entry, qty, size_usdt, sl, tp, metrics):
    oa = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO trades (symbol,side,entry_price,qty,size_usdt,sl,tp,metrics,opened_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (symbol, side, entry, qty, size_usdt, sl, tp, json.dumps(metrics), oa))
        await db.commit(); return cur.lastrowid

async def save_trade_close(trade_id, exit_price, pnl, pnl_pct, reason, entry_price, opened_at):
    ca = datetime.now(timezone.utc).isoformat()
    dur = 0
    if opened_at:
        try: dur = int((datetime.now(timezone.utc) - datetime.fromisoformat(opened_at)).total_seconds())
        except: pass
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE trades SET exit_price=?,pnl=?,pnl_pct=?,reason=?,closed_at=?,duration_s=? WHERE id=?",
            (exit_price, pnl, pnl_pct, reason, ca, dur, trade_id))
        await db.commit()

async def save_signal(symbol, signal, metrics, executed):
    ts = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO signals (symbol,signal,metrics,executed,ts) VALUES (?,?,?,?,?)",
                         (symbol, signal, json.dumps(metrics), int(executed), ts))
        await db.commit()

async def get_performance_stats():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT pnl,pnl_pct,duration_s FROM trades WHERE closed_at IS NOT NULL")
        rows = await cur.fetchall()
    if not rows:
        return {"total_trades":0,"wins":0,"losses":0,"win_rate":0.0,
                "total_pnl":0.0,"avg_win":0.0,"avg_loss":0.0,"best_trade":0.0,
                "worst_trade":0.0,"avg_duration_m":0.0}
    pnls  = [r["pnl"] for r in rows if r["pnl"] is not None]
    wins  = [p for p in pnls if p > 0]; losses = [p for p in pnls if p <= 0]
    durs  = [r["duration_s"] for r in rows if r["duration_s"]]
    return {"total_trades":len(pnls),"wins":len(wins),"losses":len(losses),
            "win_rate":round(len(wins)/len(pnls)*100,1) if pnls else 0.0,
            "total_pnl":round(sum(pnls),2),
            "avg_win":round(sum(wins)/len(wins),2) if wins else 0.0,
            "avg_loss":round(sum(losses)/len(losses),2) if losses else 0.0,
            "best_trade":round(max(pnls),2) if pnls else 0.0,
            "worst_trade":round(min(pnls),2) if pnls else 0.0,
            "avg_duration_m":round(sum(durs)/len(durs)/60,1) if durs else 0.0}

async def get_recent_trades(limit=20):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,))
        return [dict(r) for r in await cur.fetchall()]

# ══════════════════════════════════════════════════════════════════════════════
#  EXCHANGE CLIENT (BingX)
# ══════════════════════════════════════════════════════════════════════════════
_BASE = "https://open-api.bingx.com"
_session: aiohttp.ClientSession | None = None
_ws_prices: dict[str, float] = {}

def _get_session():
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=200, ttl_dns_cache=300, ssl=False),
            timeout=aiohttp.ClientTimeout(total=10))
    return _session

async def close_session():
    global _session
    if _session and not _session.closed: await _session.close()

def _sign(params, secret):
    qs = urlencode(sorted(params.items()))
    return hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()

def _auth(params=None):
    p = dict(params or {})
    p["timestamp"] = int(time.time() * 1000)
    p["signature"] = _sign(p, cfg.bingx_secret_key)
    return p

def _hdr(): return {"X-BX-APIKEY": cfg.bingx_api_key}

async def _get(path, params=None, auth=False):
    s = _get_session()
    p = _auth(params) if auth else (params or {})
    try:
        async with s.get(_BASE + path, params=p, headers=_hdr() if auth else {}) as r:
            return await r.json(content_type=None)
    except Exception as e:
        logger.warning(f"GET {path}: {e}"); return {}

async def _post(path, params=None):
    s = _get_session()
    try:
        async with s.post(_BASE + path, params=_auth(params), headers=_hdr()) as r:
            return await r.json(content_type=None)
    except Exception as e:
        logger.warning(f"POST {path}: {e}"); return {}

async def _delete(path, params=None):
    s = _get_session()
    try:
        async with s.delete(_BASE + path, params=_auth(params), headers=_hdr()) as r:
            return await r.json(content_type=None)
    except Exception as e:
        logger.warning(f"DELETE {path}: {e}"); return {}

async def fetch_all_tickers():
    r = await _get("/openApi/swap/v2/quote/ticker")
    d = r.get("data", r) if isinstance(r, dict) else r
    return d if isinstance(d, list) else []

async def _fetch_ohlcv(symbol, tf):
    r = await _get("/openApi/swap/v3/quote/klines", {"symbol": symbol, "interval": tf, "limit": 200})
    raw = r.get("data", []) if isinstance(r, dict) else []
    if len(raw) < 50: return None
    try:
        return {
            "open":   np.array([float(c[1]) for c in raw], dtype=np.float64),
            "high":   np.array([float(c[2]) for c in raw], dtype=np.float64),
            "low":    np.array([float(c[3]) for c in raw], dtype=np.float64),
            "close":  np.array([float(c[4]) for c in raw], dtype=np.float64),
            "volume": np.array([float(c[5]) for c in raw], dtype=np.float64),
        }
    except: return None

async def fetch_universe_concurrent(symbols):
    async def _one(sym):
        p, h, t = await asyncio.gather(
            _fetch_ohlcv(sym, cfg.timeframe),
            _fetch_ohlcv(sym, cfg.confirm_tf),
            _fetch_ohlcv(sym, cfg.trend_tf))
        return sym, p, h, t
    results = await asyncio.gather(*[asyncio.create_task(_one(s)) for s in symbols], return_exceptions=True)
    return {sym: {"p": p, "h": h, "t": t}
            for r in results if not isinstance(r, Exception)
            for sym, p, h, t in [r] if p is not None}

async def get_balance():
    r = await _get("/openApi/swap/v2/user/balance", auth=True)
    try:
        d = r.get("data", {})
        if isinstance(d, dict):
            b = d.get("balance", {})
            if isinstance(b, dict):
                return float(b.get("availableMargin", b.get("balance", 0)))
            return float(d.get("availableMargin", d.get("equity", 0)))
    except Exception as e: logger.warning(f"get_balance: {e}")
    return 0.0

async def get_all_positions():
    r = await _get("/openApi/swap/v2/user/positions", auth=True)
    try:
        d = r.get("data", [])
        if isinstance(d, list):
            return {p["symbol"]: p for p in d if abs(float(p.get("positionAmt", 0))) > 1e-9}
    except Exception as e: logger.warning(f"get_positions: {e}")
    return {}

async def set_leverage(symbol, leverage):
    return await _post("/openApi/swap/v2/trade/leverage", {"symbol": symbol, "side": "LONG", "leverage": leverage})

async def place_market_order(symbol, side, size_usdt, sl, tp):
    r = await _post("/openApi/swap/v2/trade/order", {
        "symbol": symbol, "side": side,
        "positionSide": "LONG" if side == "BUY" else "SHORT",
        "type": "MARKET", "quoteOrderQty": size_usdt,
        "stopLoss": str(sl), "takeProfit": str(tp)})
    return r if isinstance(r, dict) else {}

async def close_position(symbol, position):
    amt = float(position.get("positionAmt", 0))
    return await _post("/openApi/swap/v2/trade/closePosition", {
        "symbol": symbol, "positionSide": "LONG" if amt > 0 else "SHORT"})

async def cancel_all_orders(symbol):
    return await _delete("/openApi/swap/v2/trade/allOpenOrders", {"symbol": symbol})

async def get_price(symbol):
    if symbol in _ws_prices: return _ws_prices[symbol]
    r = await _get("/openApi/swap/v2/quote/price", {"symbol": symbol})
    try: return float(r.get("data", {}).get("price", 0))
    except: return 0.0

async def ws_price_stream(symbols):
    import gzip, websockets  # type: ignore
    streams = "/".join(f"{s.replace('-','').lower()}@markPrice" for s in symbols)
    url = f"wss://open-api-ws.bingx.com/market?streams={streams}"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                async for msg in ws:
                    try:
                        if isinstance(msg, bytes): msg = gzip.decompress(msg).decode()
                        d = json.loads(msg).get("data", {})
                        sym = d.get("s", ""); p = d.get("p", d.get("mp", 0))
                        if sym and p:
                            if "-" not in sym and sym.endswith("USDT"):
                                sym = sym[:-4] + "-USDT"
                            _ws_prices[sym] = float(p)
                    except: pass
        except Exception as e:
            logger.debug(f"WS reconnect: {e}"); await asyncio.sleep(5)

# ══════════════════════════════════════════════════════════════════════════════
#  INDICATORS
# ══════════════════════════════════════════════════════════════════════════════
@njit(cache=True)
def _rsi(close, period):
    n = len(close); out = np.full(n, np.nan)
    if n < period + 1: return out
    gains = np.zeros(n); losses = np.zeros(n)
    for i in range(1, n):
        d = close[i] - close[i-1]
        if d > 0: gains[i] = d
        else: losses[i] = -d
    ag = np.mean(gains[1:period+1]); al = np.mean(losses[1:period+1])
    for i in range(period, n):
        if i > period:
            ag = (ag*(period-1) + gains[i]) / period
            al = (al*(period-1) + losses[i]) / period
        out[i] = 100.0 if al == 0 else 100.0 - 100.0/(1.0 + ag/al)
    return out

@njit(cache=True)
def _atr(high, low, close, period):
    n = len(close); out = np.zeros(n); tr = np.zeros(n)
    for i in range(1, n):
        tr[i] = max(high[i]-low[i], abs(high[i]-close[i-1]), abs(low[i]-close[i-1]))
    s = np.sum(tr[1:period+1]) / period; out[period] = s
    for i in range(period+1, n):
        out[i] = (out[i-1]*(period-1) + tr[i]) / period
    return out

@njit(cache=True)
def _adx_di(high, low, close, period):
    n = len(close); adx = np.zeros(n); pdi = np.zeros(n); mdi = np.zeros(n)
    if n < period*2+1: return adx, pdi, mdi
    tr = np.zeros(n); pdm = np.zeros(n); mdm = np.zeros(n)
    for i in range(1, n):
        tr[i] = max(high[i]-low[i], abs(high[i]-close[i-1]), abs(low[i]-close[i-1]))
        up = high[i]-high[i-1]; dn = low[i-1]-low[i]
        pdm[i] = up if up > dn and up > 0 else 0.0
        mdm[i] = dn if dn > up and dn > 0 else 0.0
    str_ = np.sum(tr[1:period+1])
    spdm = np.sum(pdm[1:period+1]); smdm = np.sum(mdm[1:period+1])
    for i in range(period, n):
        if i > period:
            str_ = str_ - str_/period + tr[i]
            spdm = spdm - spdm/period + pdm[i]
            smdm = smdm - smdm/period + mdm[i]
        if str_ == 0: continue
        pdi[i] = 100.0*spdm/str_; mdi[i] = 100.0*smdm/str_
        dx = abs(pdi[i]-mdi[i])/(pdi[i]+mdi[i]+1e-10)*100.0
        adx[i] = dx if i == period else (adx[i-1]*(period-1)+dx)/period
    return adx, pdi, mdi

def generate_signal(high, low, close, open_, volume,
                    h_high, h_low, h_close, h_open, h_volume,
                    t_high, t_low, t_close, cfg):
    metrics = {}
    try:
        adx_a, pdi_a, mdi_a = _adx_di(high, low, close, cfg.adx_len)
        rsi_a = _rsi(close, cfg.rsi_len)
        atr_a = _atr(high, low, close, cfg.adx_len)
        adx = float(adx_a[-1]); pdi = float(pdi_a[-1]); mdi = float(mdi_a[-1])
        rsi = float(rsi_a[-1]); atr = float(atr_a[-1]); price = float(close[-1])
        atr_pct = atr/price*100 if price > 0 else 0.0

        dv = np.where(close >= open_, volume, -volume)
        p = cfg.period
        if len(dv) < p*3: return None, metrics
        d1 = float(np.sum(dv[-p:]))
        d2 = float(np.sum(dv[-p*2:-p]))
        d3 = float(np.sum(dv[-p*3:-p*2]))
        bull = sum(1 for d in [d1,d2,d3] if d > 0)
        bear = sum(1 for d in [d1,d2,d3] if d < 0)
        avg_vol = float(np.mean(volume[-p:])) if len(volume) >= p else float(np.mean(volume))
        vol_spike = float(volume[-1]) > avg_vol * cfg.vol_spike_mult
        confidence = max(bull,bear)/3*33 + max(0.0, adx-cfg.adx_thresh) + (10.0 if vol_spike else 0.0)
        metrics = {"adx":round(adx,2),"plus_di":round(pdi,2),"minus_di":round(mdi,2),
                   "rsi":round(rsi,2),"atr":round(atr,8),"atr_pct":round(atr_pct,4),
                   "delta1":round(d1,2),"delta2":round(d2,2),"delta3":round(d3,2),
                   "bull_steps":bull,"bear_steps":bear,"vol_spike":vol_spike,
                   "confidence":round(confidence,1)}
        if adx < cfg.adx_thresh: return None, metrics
        if not ((bull>=2 and pdi>mdi and rsi<cfg.rsi_ob) or
                (bear>=2 and mdi>pdi and rsi>cfg.rsi_os)): return None, metrics
        sig = "BUY" if bull>=2 and pdi>mdi and rsi<cfg.rsi_ob else "SELL"

        # HTF confirmation
        if h_close is not None and len(h_close) >= cfg.adx_len*2+5:
            ha, hp, hm = _adx_di(h_high, h_low, h_close, cfg.adx_len)
            if sig=="BUY"  and float(hm[-1]) > float(hp[-1])*1.2: return None, metrics
            if sig=="SELL" and float(hp[-1]) > float(hm[-1])*1.2: return None, metrics
        if t_close is not None and len(t_close) >= cfg.adx_len*2+5:
            ta, tp2, tm = _adx_di(t_high, t_low, t_close, cfg.adx_len)
            if sig=="BUY"  and float(tm[-1]) > float(tp2[-1])*1.5: return None, metrics
            if sig=="SELL" and float(tp2[-1]) > float(tm[-1])*1.5: return None, metrics
        if confidence < cfg.min_confidence: return None, metrics
        return sig, metrics
    except Exception as e:
        logger.debug(f"Indicator error: {e}"); return None, metrics

# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════════════════════════════════════
_tg_queue: asyncio.Queue = asyncio.Queue(maxsize=200)
_tg_last = 0.0

def start_sender(): asyncio.create_task(_tg_loop())

async def _tg_loop():
    global _tg_last
    while True:
        msg, silent = await _tg_queue.get()
        gap = time.time() - _tg_last
        if gap < 0.5: await asyncio.sleep(0.5-gap)
        try:
            url = f"https://api.telegram.org/bot{cfg.telegram_token}/sendMessage"
            async with aiohttp.ClientSession() as s:
                await s.post(url, json={"chat_id": cfg.telegram_chat_id, "text": msg[:4096],
                             "parse_mode": "HTML", "disable_notification": silent},
                             timeout=aiohttp.ClientTimeout(total=8))
        except Exception as e: logger.debug(f"TG: {e}")
        finally: _tg_last = time.time(); _tg_queue.task_done()

async def send(msg, silent=False):
    try: _tg_queue.put_nowait((msg, silent))
    except asyncio.QueueFull: pass

async def send_now(msg): await send(msg)

def _bar(c): return "█"*int(c/10) + "░"*(10-int(c/10))

def msg_start(n):
    return (f"⚡ <b>UltraBot v3 — Online</b>\n\n"
            f"📊 Universe: <b>{n} symbols</b>\n"
            f"⏱ {cfg.timeframe}/{cfg.confirm_tf}/{cfg.trend_tf} | "
            f"ADX≥{cfg.adx_thresh} | RSI {cfg.rsi_ob}/{cfg.rsi_os}\n"
            f"⚖️ {cfg.leverage}x | Risk {cfg.risk_pct}% | Max {cfg.max_open_trades} trades\n"
            f"🛡 SL {cfg.sl_pct}% | TP {cfg.tp_pct}% | Trailing: {cfg.trailing_sl}")

def msg_entry(sym, side, price, size, sl, tp, sl_pct, tp_pct, m):
    emoji = "🟢 LONG" if side=="BUY" else "🔴 SHORT"
    c = m.get("confidence",0)
    return (f"{emoji} <b>{sym}</b>\n\n"
            f"💰 Entry: <code>{price:.6g}</code>\n"
            f"🎯 TP: <code>{tp:.6g}</code> (+{tp_pct:.1f}%)\n"
            f"🛡 SL: <code>{sl:.6g}</code> (-{sl_pct:.1f}%)\n"
            f"📦 Size: <b>{size:.1f} USDT</b>\n\n"
            f"ADX:{m.get('adx',0):.1f} RSI:{m.get('rsi',0):.1f} ATR:{m.get('atr_pct',0):.2f}%\n"
            f"Δ1:{m.get('delta1',0):+.0f} Δ2:{m.get('delta2',0):+.0f} Δ3:{m.get('delta3',0):+.0f}\n"
            f"⚡ Conf: {c:.0f}% {_bar(c)}")

def msg_close(sym, side, pnl, pnl_pct, reason, dur_s):
    e = "💚" if pnl>=0 else "🔴"
    return (f"{e} <b>{sym}</b> {'🟢' if side=='LONG' else '🔴'} {side} — <b>{reason}</b>\n"
            f"PnL: <b>{pnl:+.2f} USDT</b> ({pnl_pct:+.2f}%) | {dur_s//60}m")

def msg_performance(perf, risk):
    return (f"📊 <b>Performance</b>\n"
            f"Trades:{perf.get('total_trades',0)} WR:{perf.get('win_rate',0):.1f}%\n"
            f"PnL:<b>{perf.get('total_pnl',0):+.2f}</b> | Day:{risk.get('daily_pnl_usdt',0):+.2f}")

def msg_halt(reason): return f"🚨 <b>HALTED</b>: {reason}"
def msg_cooldown(s, n): return f"⏸ Cooldown {s}s ({n} losses)"
def msg_error(e): return f"⚠️ <code>{e[:300]}</code>"

# ══════════════════════════════════════════════════════════════════════════════
#  DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════
_dash_state: dict = {"status":"starting","balance":0.0,"positions":{},
                     "scan_stats":{},"risk":{},"perf":{},"last_signals":[],
                     "trade_metrics":{},"updated_at":time.time()}

def update_state(**kw):
    _dash_state.update(kw); _dash_state["updated_at"] = time.time()

async def start_dashboard():
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect
    from fastapi.responses import HTMLResponse
    import uvicorn

    app = FastAPI()
    _HTML = """<!DOCTYPE html><html><head><meta charset="UTF-8">
<title>⚡ UltraBot v3</title>
<style>*{box-sizing:border-box;margin:0;padding:0}
body{background:#0d1117;color:#c9d1d9;font-family:monospace;font-size:14px}
header{background:#161b22;padding:12px 20px;border-bottom:1px solid #30363d;display:flex;align-items:center;gap:12px}
h1{font-size:18px;color:#58a6ff}.badge{padding:3px 8px;border-radius:12px;font-size:12px;font-weight:bold}
.running{background:#1f6feb;color:#fff}.halted{background:#da3633;color:#fff}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:12px;padding:16px}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px}
.label{color:#8b949e;font-size:11px;text-transform:uppercase}.value{font-size:20px;font-weight:bold;margin-top:4px}
.sub{font-size:12px;color:#8b949e;margin-top:2px}.green{color:#3fb950!important}.red{color:#f85149!important}
table{width:100%;border-collapse:collapse}th,td{padding:8px 12px;text-align:left;border-bottom:1px solid #21262d}
th{color:#8b949e;font-size:11px;text-transform:uppercase}section{margin:0 16px 16px;background:#161b22;border:1px solid #30363d;border-radius:8px}
section h2{padding:12px 16px;font-size:13px;border-bottom:1px solid #30363d;color:#8b949e}</style></head>
<body><header><h1>⚡ UltraBot v3</h1><span id="badge" class="badge running">RUNNING</span>
<span id="upd" style="margin-left:auto;color:#8b949e;font-size:12px"></span></header>
<div class="grid" id="metrics"></div>
<section><h2>Open Positions</h2><table><thead><tr><th>Symbol</th><th>Side</th><th>Entry</th><th>Mark</th><th>PnL</th><th>Conf%</th></tr></thead><tbody id="pos"></tbody></table></section>
<section><h2>Latest Signals</h2><table><thead><tr><th>Symbol</th><th>Signal</th><th>Conf%</th><th>ADX</th><th>RSI</th></tr></thead><tbody id="sig"></tbody></table></section>
<script>const ws=new WebSocket((location.protocol==='https:'?'wss':'ws')+'://'+location.host+'/ws');
ws.onmessage=e=>{const d=JSON.parse(e.data);const r=d.risk||{};const s=d.scan_stats||{};const p=d.perf||{};
document.getElementById('upd').textContent='Updated '+new Date(d.updated_at*1000).toLocaleTimeString();
const b=document.getElementById('badge');b.textContent=r.halted?'HALTED':'RUNNING';b.className='badge '+(r.halted?'halted':'running');
const mx=[{l:'Balance',v:'$'+(d.balance||0).toFixed(2)},{l:'Day PnL',v:(r.daily_pnl_usdt>=0?'+':'')+((r.daily_pnl_usdt)||0).toFixed(2),c:(r.daily_pnl_usdt>=0?'green':'red')},
{l:'Win Rate',v:(r.win_rate||0)+'%',s:r.wins+'W/'+r.losses+'L'},{l:'Open',v:Object.keys(d.positions||{}).length},
{l:'Scan',v:(s.last_ms||0).toFixed(0)+'ms',s:(s.n_scanned||0)+' syms'},{l:'Signals',v:'🟢'+(s.n_buy||0)+' 🔴'+(s.n_sell||0)},
{l:'Total PnL',v:(r.total_pnl>=0?'+':'')+(r.total_pnl||0).toFixed(2),c:(r.total_pnl>=0?'green':'red')},{l:'Trades',v:p.total_trades||0}];
document.getElementById('metrics').innerHTML=mx.map(m=>`<div class="card"><div class="label">${m.l}</div><div class="value ${m.c||''}">${m.v}</div><div class="sub">${m.s||''}</div></div>`).join('');
const pos=d.positions||{};
document.getElementById('pos').innerHTML=Object.entries(pos).map(([sym,p])=>{const pnl=parseFloat(p.unrealizedProfit||0);const side=parseFloat(p.positionAmt||0)>0?'🟢 LONG':'🔴 SHORT';const m=(d.trade_metrics||{})[sym]||{};
return `<tr><td>${sym}</td><td>${side}</td><td>${parseFloat(p.entryPrice||0).toFixed(4)}</td><td>${parseFloat(p.markPrice||p.entryPrice||0).toFixed(4)}</td><td class="${pnl>=0?'green':'red'}">${pnl>=0?'+':''}${pnl.toFixed(2)}</td><td>${(m.confidence||0).toFixed(0)}%</td></tr>`;}).join('')||'<tr><td colspan="6" style="color:#8b949e;text-align:center">No open positions</td></tr>';
document.getElementById('sig').innerHTML=(d.last_signals||[]).slice(0,15).map(s=>`<tr><td>${s.symbol}</td><td class="${s.signal==='BUY'?'green':'red'}">${s.signal==='BUY'?'🟢 BUY':'🔴 SELL'}</td><td>${(s.confidence||0).toFixed(0)}%</td><td>${(s.adx||0).toFixed(1)}</td><td>${(s.rsi||0).toFixed(1)}</td></tr>`).join('')||'<tr><td colspan="5" style="color:#8b949e;text-align:center">No signals yet</td></tr>';};
ws.onclose=()=>setTimeout(()=>location.reload(),4000);</script></body></html>"""

    @app.get("/")
    async def index(): return HTMLResponse(_HTML)

    @app.get("/health")
    async def health(): return {"status": "ok"}

    @app.websocket("/ws")
    async def ws_ep(ws: WebSocket):
        await ws.accept()
        try:
            await ws.send_text(json.dumps(_dash_state, default=str))
            while True:
                await asyncio.sleep(2)
                await ws.send_text(json.dumps(_dash_state, default=str))
        except: pass

    port = cfg.effective_port
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="warning", access_log=False)
    server = uvicorn.Server(config)
    asyncio.create_task(server.serve())
    logger.info(f"Dashboard on http://0.0.0.0:{port}")

# ══════════════════════════════════════════════════════════════════════════════
#  BOT ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════
console  = Console()
risk     = RiskManager()
executor = ThreadPoolExecutor(max_workers=12, thread_name_prefix="indicator")

open_trades:    dict[str, dict]  = {}
trailing_peaks: dict[str, float] = {}
last_signals:   dict[str, str]   = {}
scan_stats:     dict             = {"last_ms":0,"n_buy":0,"n_sell":0,"n_scanned":0}
trade_metrics:  dict[str, dict]  = {}


async def get_universe():
    tickers = await fetch_all_tickers()
    cands = []
    for t in tickers:
        sym = t.get("symbol","")
        if not sym.endswith("-USDT") or sym in cfg.blacklist: continue
        try:
            vol = float(t.get("quoteVolume") or t.get("volume") or 0)
        except: continue
        if vol >= cfg.min_volume_usdt: cands.append((sym, vol))
    cands.sort(key=lambda x: x[1], reverse=True)
    syms = [s[0] for s in cands[:cfg.top_n_symbols]]
    logger.info(f"Universe: {len(syms)} symbols")
    return syms


def _run_indicators(symbol, p, h, t):
    try:
        sig, m = generate_signal(
            p["high"],p["low"],p["close"],p["open"],p["volume"],
            h["high"] if h else None, h["low"] if h else None,
            h["close"] if h else None, h["open"] if h else None,
            h["volume"] if h else None,
            t["high"] if t else None, t["low"] if t else None,
            t["close"] if t else None, cfg)
    except Exception as e:
        return symbol, None, {"error": str(e)}
    return symbol, sig, m


async def execute_entry(symbol, sig, metrics, balance, n_open):
    can, reason = risk.can_trade(balance)
    if not can: logger.debug(f"Risk blocked {symbol}: {reason}"); return False
    if not risk.correlation_ok(symbol): return False
    size = risk.position_size(balance, n_open, metrics.get("confidence",0), metrics.get("atr_pct",0))
    if size < 5: return False
    price = await get_price(symbol)
    if price == 0: return False
    sl, tp, sl_pct, tp_pct = risk.dynamic_sl_tp(price, sig, metrics.get("atr",0))
    try:
        await set_leverage(symbol, cfg.leverage)
        resp = await place_market_order(symbol, sig, size, sl, tp)
        if resp.get("code") and int(resp.get("code",0)) != 0:
            logger.warning(f"Order error {symbol}: {resp}"); return False
    except Exception as e:
        logger.error(f"Order failed {symbol}: {e}"); return False
    db_id = await save_trade_open(symbol, sig, price, size*cfg.leverage/price, size, sl, tp, metrics)
    risk.record_open(symbol)
    open_trades[symbol] = {"side":sig,"entry":price,"sl":sl,"tp":tp,"size":size,
                            "db_id":db_id,"opened_at":datetime.now(timezone.utc).isoformat(),
                            "metrics":metrics,"sl_pct":sl_pct,"tp_pct":tp_pct}
    trailing_peaks[symbol] = price; last_signals[symbol] = sig; trade_metrics[symbol] = metrics
    await send(msg_entry(symbol,sig,price,size,sl,tp,sl_pct,tp_pct,metrics))
    logger.success(f"OPENED {sig} {symbol} @ {price:.6g} conf={metrics.get('confidence',0):.0f}%")
    return True


async def execute_close(symbol, position, reason):
    try:
        await close_position(symbol, position); await cancel_all_orders(symbol)
    except Exception as e:
        logger.error(f"Close failed {symbol}: {e}"); return
    pnl = float(position.get("unrealizedProfit",0))
    balance = await get_balance()
    side = "LONG" if float(position.get("positionAmt",0)) > 0 else "SHORT"
    entry = float(position.get("entryPrice",0))
    mark  = float(position.get("markPrice",entry))
    pnl_pct = (mark-entry)/entry*100*(1 if side=="LONG" else -1)*cfg.leverage
    trade = open_trades.pop(symbol, {})
    risk.record_close(symbol, pnl, balance)
    if trade.get("db_id"):
        await save_trade_close(trade["db_id"],mark,pnl,pnl_pct,reason,entry,trade.get("opened_at",""))
    trailing_peaks.pop(symbol,None); last_signals.pop(symbol,None)
    opened = trade.get("opened_at",""); dur = 0
    if opened:
        try: dur = int((datetime.now(timezone.utc)-datetime.fromisoformat(opened)).total_seconds())
        except: pass
    await send(msg_close(symbol,side,pnl,pnl_pct,reason,dur))
    logger.info(f"CLOSED {side} {symbol} — {reason} | PnL {pnl:+.2f}")


async def scan_loop():
    symbols = []; universe_refresh = 0
    while True:
        t0 = time.perf_counter()
        try:
            if time.time()-universe_refresh > 300:
                symbols = await get_universe(); universe_refresh = time.time()
                asyncio.create_task(ws_price_stream(symbols[:30]))
            universe_data = await fetch_universe_concurrent(symbols)
            loop = asyncio.get_event_loop()
            tasks = [loop.run_in_executor(executor, _run_indicators, sym,
                     d["p"], d.get("h"), d.get("t"))
                     for sym, d in universe_data.items()
                     if d["p"] is not None and len(d["p"].get("close",[])) >= cfg.candles_needed]
            results = list(await asyncio.gather(*tasks))
            signals = [(sym,sig,m) for sym,sig,m in results
                       if sig and m.get("confidence",0) >= cfg.min_confidence]
            signals.sort(key=lambda x: x[2].get("confidence",0), reverse=True)
            n_buy  = sum(1 for _,s,_ in signals if s=="BUY")
            n_sell = sum(1 for _,s,_ in signals if s=="SELL")
            elapsed_ms = (time.perf_counter()-t0)*1000
            scan_stats.update({"last_ms":elapsed_ms,"n_buy":n_buy,"n_sell":n_sell,
                                "n_scanned":len(results),"max_open":cfg.max_open_trades})
            for sym,sig,m in signals[:10]:
                asyncio.create_task(save_signal(sym,sig,m,False))
            logger.info(f"Scan {elapsed_ms:.0f}ms | {len(results)} syms | 🟢{n_buy} 🔴{n_sell}")
            balance   = await get_balance()
            positions = await get_all_positions()
            risk.set_balance(balance); n_open = len(positions)
            if risk.is_halted:
                await asyncio.sleep(cfg.scan_interval); continue
            for sym,sig,metrics in signals:
                if n_open >= cfg.max_open_trades: break
                if sym in positions:
                    pos = positions[sym]
                    is_long = float(pos["positionAmt"]) > 0
                    if (sig=="SELL" and is_long) or (sig=="BUY" and not is_long):
                        await execute_close(sym,pos,"FLIP")
                        positions = await get_all_positions(); n_open = len(positions)
                    continue
                ok = await execute_entry(sym,sig,metrics,balance,n_open)
                if ok: n_open+=1; balance = await get_balance(); asyncio.create_task(save_signal(sym,sig,metrics,True))
            positions = await get_all_positions()
            perf = await get_performance_stats()
            update_state(status="running",balance=balance,positions=positions,scan_stats=scan_stats,
                         risk=risk.summary(),perf=perf,
                         last_signals=[{"symbol":s,"signal":sig,**m} for s,sig,m in signals[:15]],
                         trade_metrics=trade_metrics)
        except Exception as e:
            logger.error(f"Scan error: {e}", exc_info=True); await send(msg_error(str(e)),silent=True)
        await asyncio.sleep(cfg.scan_interval)


async def position_monitor():
    while True:
        await asyncio.sleep(5)
        try:
            positions = await get_all_positions()
            for sym,pos in positions.items():
                amt  = float(pos.get("positionAmt",0)); mark = float(pos.get("markPrice") or pos.get("entryPrice",0) or 0)
                side = "LONG" if amt>0 else "SHORT"
                if mark == 0: continue
                if cfg.trailing_sl and sym in trailing_peaks:
                    peak = trailing_peaks[sym]
                    if side=="LONG" and mark>peak:  trailing_peaks[sym]=mark
                    elif side=="SHORT" and mark<peak: trailing_peaks[sym]=mark
                    peak_now = trailing_peaks[sym]
                    trade    = open_trades.get(sym,{})
                    sl_pct   = trade.get("sl_pct", cfg.sl_pct)
                    if ((side=="LONG"  and mark < peak_now*(1-sl_pct/100)) or
                        (side=="SHORT" and mark > peak_now*(1+sl_pct/100))):
                        logger.info(f"Trailing SL: {side} {sym}")
                        await execute_close(sym,pos,"Trailing SL")
        except Exception as e: logger.error(f"Monitor: {e}")


async def performance_loop():
    await asyncio.sleep(60)
    while True:
        try:
            perf = await get_performance_stats()
            await send(msg_performance(perf, risk.summary()), silent=True)
        except Exception as e: logger.debug(f"Perf: {e}")
        await asyncio.sleep(4*3600)


async def terminal_loop():
    while True:
        await asyncio.sleep(15)
        try:
            positions = await get_all_positions(); balance = await get_balance()
            t = Table(title="⚡ UltraBot v3", box=rbox.ROUNDED, show_lines=True)
            for col,sty,jus in [("Symbol","cyan","left"),("Side","","left"),("Entry","","right"),("Mark","","right"),("PnL","","right"),("Conf%","","right")]:
                t.add_column(col,style=sty,justify=jus)
            for sym,pos in positions.items():
                pnl = float(pos.get("unrealizedProfit",0))
                side = "🟢 LONG" if float(pos["positionAmt"])>0 else "🔴 SHORT"
                m = trade_metrics.get(sym,{})
                t.add_row(sym,side,f"{float(pos.get('entryPrice',0)):.4f}",
                          f"{float(pos.get('markPrice',pos.get('entryPrice',0))):.4f}",
                          f"[{'green' if pnl>=0 else 'red'}]{pnl:+.2f}[/]",
                          f"{m.get('confidence',0):.0f}%")
            console.print(t)
            rs = risk.summary()
            console.print(f"💰 {balance:.2f} | Scan:{scan_stats.get('last_ms',0):.0f}ms | "
                          f"🟢{scan_stats.get('n_buy',0)} 🔴{scan_stats.get('n_sell',0)} | "
                          f"Day:{rs['daily_pnl_usdt']:+.2f} | WR:{rs['win_rate']}% | "
                          f"{'[red]HALTED[/]' if rs['halted'] else '[green]RUNNING[/]'}")
        except: pass


async def main():
    logger.remove()
    logger.add(sys.stderr, level="INFO",
               format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
    os.makedirs("data", exist_ok=True)
    logger.add("data/ultrabot.log", rotation="1 day", retention="14 days", level="DEBUG")

    console.print("[bold cyan]⚡ UltraBot v3 initializing...[/bold cyan]")
    await init_db()
    balance = await get_balance()
    risk.set_balance(balance)
    console.print(f"💰 Balance: {balance:.2f} USDT")
    symbols = await get_universe()
    start_sender()
    await send(msg_start(len(symbols)))
    if cfg.dashboard_enabled:
        await start_dashboard()
    console.print(f"[green]✅ Ready — scanning {len(symbols)} symbols[/green]")
    await asyncio.gather(scan_loop(), position_monitor(), performance_loop(), terminal_loop())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[bold red]Stopped[/bold red]")
    finally:
        asyncio.run(close_session())
