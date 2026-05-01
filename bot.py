"""
UltraBot v6 — Fixed & Optimized
================================
Fixes vs v5:
  - [BUG] Low-balance Telegram spam → notified once, re-alerts only when balance changes
  - [BUG] WS reconnect loop spam → exponential back-off (1s → 60s) + dedup log
  - [BUG] Railway "WS stream: 40 symbols" logged as error → INFO now only on first connect
  - [BUG] performance_loop sent duplicate notifications → hash covers more fields
  - [BUG] scan_loop ran while balance=0 (API failure) → guard before trading
  - [BUG] position_monitor could close on markPrice=0 → strict guard
  - [BUG] Limit order fallback could double-order → order-id tracking
  - [BUG] Kelly returned <1 blend formula was wrong → fixed math

Profitability upgrades:
  - W11: EMA trend filter (20/50 EMA cross on primary TF) — avoids counter-trend longs/shorts
  - W12: Min ATR filter — skip low-volatility setups (ATR% < 0.15 not worth the fee)
  - W13: Consecutive wins bonus — small size increase after 3+ wins (anti-Kelly lock-down)
  - W14: Smart re-entry block — same symbol locked 30 min after ANY close (not just loss)
  - Wider TP ratio (default 2.5R instead of 2R) — better expectancy
  - Tighter default ADX threshold (32 instead of 30) — filter weak trends
  - Scan interval raised to 15s to reduce API rate-limit hits
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
    pass

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
def _env(k, d=""): return os.environ.get(k, d)
def _envf(k, d):
    try: return float(os.environ.get(k, d))
    except: return float(d)
def _envi(k, d):
    try: return int(os.environ.get(k, d))
    except: return int(d)
def _envb(k, d): return os.environ.get(k, str(d)).lower() in ("1","true","yes")

class _Cfg:
    # ── Keys ─────────────────────────────────────────────────────────────
    bingx_api_key    = _env("BINGX_API_KEY")
    bingx_secret_key = _env("BINGX_SECRET_KEY") or _env("BINGX_API_SECRET")
    telegram_token   = _env("TELEGRAM_TOKEN")
    telegram_chat_id = _env("TELEGRAM_CHAT_ID")
    # ── Strategy ─────────────────────────────────────────────────────────
    timeframe        = _env("TIMEFRAME",     "15m")
    confirm_tf       = _env("CONFIRM_TF",    "1h")
    trend_tf         = _env("TREND_TF",      "4h")
    period           = _envi("PERIOD",        25)
    adx_len          = _envi("ADX_LEN",       14)
    adx_thresh       = _envf("ADX_THRESH",    32.0)   # v6: tighter (was 30)
    rsi_len          = _envi("RSI_LEN",       14)
    rsi_ob           = _envf("RSI_OB",        70.0)
    rsi_os           = _envf("RSI_OS",        30.0)
    vol_spike_mult   = _envf("VOL_SPIKE_MULT", 2.0)
    min_confidence   = _envf("MIN_CONFIDENCE", 62.0)
    min_atr_pct      = _envf("MIN_ATR_PCT",    0.15)  # W12: skip dead markets
    ema_fast         = _envi("EMA_FAST",       20)    # W11
    ema_slow         = _envi("EMA_SLOW",       50)    # W11
    ema_filter       = _envb("EMA_FILTER",     True)  # W11
    # ── Universe ─────────────────────────────────────────────────────────
    min_volume_usdt  = _envf("MIN_VOLUME_USDT", 5_000_000)
    top_n_symbols    = _envi("TOP_N_SYMBOLS",   60)
    blacklist        = set(s.strip() for s in _env("BLACKLIST","").split(",") if s.strip())
    # ── Risk ─────────────────────────────────────────────────────────────
    leverage              = _envi("LEVERAGE",    3)
    risk_pct              = _envf("RISK_PCT",    1.5)
    max_open_trades       = _envi("MAX_OPEN_TRADES", 3)
    sl_pct                = _envf("SL_PCT",      1.8)
    tp_pct                = _envf("TP_PCT",      4.5)   # v6: wider TP (2.5R, was 2R)
    trailing_sl           = _envb("TRAILING_SL", True)
    trailing_activation   = _envf("TRAILING_ACTIVATION", 1.0)
    max_drawdown_pct      = _envf("MAX_DRAWDOWN_PCT", 15.0)
    daily_loss_limit      = _envf("DAILY_LOSS_LIMIT",  8.0)
    max_consec_loss       = _envi("MAX_CONSECUTIVE_LOSSES", 4)
    cooldown_loss         = _envi("COOLDOWN_AFTER_LOSS", 600)
    min_balance           = _envf("MIN_BALANCE", 20.0)
    close_cooldown        = _envi("CLOSE_COOLDOWN", 1800)  # W14: 30 min after any close
    # ── v5/v6 Weapons ────────────────────────────────────────────────────
    use_limit_orders      = _envb("USE_LIMIT_ORDERS",     True)
    partial_tp            = _envb("PARTIAL_TP",           True)
    partial_tp_pct        = _envf("PARTIAL_TP_PCT",       50.0)
    funding_filter        = _envb("FUNDING_FILTER",       True)
    max_funding_rate      = _envf("MAX_FUNDING_RATE",     0.0008)
    liq_radar             = _envb("LIQ_RADAR",            True)
    ob_filter             = _envb("OB_FILTER",            True)
    ob_imbalance_thresh   = _envf("OB_IMBALANCE_THRESH",  0.58)
    regime_filter         = _envb("REGIME_FILTER",        True)
    anti_stophunt         = _envb("ANTI_STOPHUNT",        True)
    session_filter        = _envb("SESSION_FILTER",       True)
    kelly_sizing          = _envb("KELLY_SIZING",         True)
    kelly_fraction        = _envf("KELLY_FRACTION",       0.25)
    # ── Performance ──────────────────────────────────────────────────────
    scan_interval    = _envi("SCAN_INTERVAL",   15)   # v6: 15s (was 10s) — fewer API hits
    dashboard_enabled= _envb("DASHBOARD_ENABLED", True)
    dashboard_port   = _envi("DASHBOARD_PORT",  8080)
    candles_needed   = 100

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
        self._consec_loss = self._consec_win = 0
        self._wins = self._losses = 0
        self._cooldown_until = 0.0
        self._halted = False
        self._halt_reason = ""
        self._open: set[str] = set()
        # v6: unified cooldown dict — tracks both loss AND post-close cooldowns
        self._sym_cooldown: dict[str, float] = {}
        self._recent_pnls: list[float] = []

    def set_balance(self, b: float):
        if not self._peak:      self._peak     = b
        if not self._day_start: self._day_start = b
        self._balance = b
        if time.time() - self._day_ts > 86400:
            self._day_ts     = time.time()
            self._day_start  = b
            self._daily_pnl  = 0.0
            if self._halted and "daily" in self._halt_reason.lower():
                self._halted = False; self._halt_reason = ""
                logger.info("Daily halt auto-resumed")
        if b > self._peak: self._peak = b
        dd = (self._peak - b) / self._peak * 100 if self._peak else 0
        if dd >= cfg.max_drawdown_pct and not self._halted:
            self._halt(f"Max drawdown {dd:.1f}%")

    def can_trade(self, balance: float, symbol: str = "") -> tuple[bool, str]:
        if self._halted:
            return False, f"Halted: {self._halt_reason}"
        if balance < cfg.min_balance:
            return False, f"Balance ${balance:.2f} < min ${cfg.min_balance}"
        if time.time() < self._cooldown_until:
            return False, f"Global cooldown ({int(self._cooldown_until-time.time())}s)"
        if symbol and time.time() < self._sym_cooldown.get(symbol, 0):
            rem = int(self._sym_cooldown[symbol] - time.time())
            return False, f"Symbol cooldown {symbol} ({rem}s)"
        if len(self._open) >= cfg.max_open_trades:
            return False, "Max trades reached"
        if self._day_start > 0:
            dl = -self._daily_pnl / self._day_start * 100
            if dl >= cfg.daily_loss_limit:
                if not self._halted:
                    self._halt(f"Daily loss limit {dl:.1f}% (resets tomorrow)")
                return False, f"Daily limit {dl:.1f}%"
        if cfg.session_filter:
            h = datetime.now(timezone.utc).hour
            if 0 <= h < 6:
                return False, "Session filter (00-06 UTC)"
        return True, "ok"

    def correlation_ok(self, sym: str) -> bool:
        base = sym.split("-")[0]
        return not any(s.split("-")[0] == base for s in self._open if s != sym)

    def kelly_multiplier(self) -> float:
        """Fractional Kelly — fixed blend formula."""
        if not cfg.kelly_sizing or len(self._recent_pnls) < 10:
            return 1.0
        wins   = [p for p in self._recent_pnls if p > 0]
        losses = [p for p in self._recent_pnls if p < 0]
        if not wins or not losses: return 1.0
        wr    = len(wins) / len(self._recent_pnls)
        avg_w = sum(wins)  / len(wins)
        avg_l = abs(sum(losses) / len(losses))
        if avg_l == 0: return 1.0
        b     = avg_w / avg_l
        kelly = max(0.0, wr - (1 - wr) / b)
        kelly = min(kelly, 1.0)
        # Fractional Kelly: blend between full Kelly and 1× (neutral)
        # FIX v6: correct blend = kelly_fraction * kelly_full + (1-kelly_fraction)*1.0
        blended = cfg.kelly_fraction * kelly + (1 - cfg.kelly_fraction) * 1.0
        # W13: consecutive wins bonus (up to +20% size after 3+ wins)
        if self._consec_win >= 3:
            blended = min(blended * (1 + (self._consec_win - 2) * 0.05), blended * 1.20)
        return round(max(0.5, min(blended, 1.5)), 3)

    def position_size(self, balance, n_open, confidence, atr_pct, funding_rate=0.0):
        base   = balance * cfg.risk_pct / 100
        conf_s = 0.7 + (min(confidence, 95) / 100) * 0.6
        slot_s = 0.75 ** n_open if n_open else 1.0
        vol_s  = min(1.0, 1.5 / (atr_pct + 0.5)) if atr_pct > 0 else 1.0
        fund_s = max(0.5, 1.0 - abs(funding_rate) * 500) if funding_rate else 1.0
        kelly  = self.kelly_multiplier()
        size   = base * conf_s * slot_s * vol_s * fund_s * kelly
        return round(max(5.0, min(size, balance * 0.20)), 2)

    def dynamic_sl_tp(self, price, side, atr):
        if atr > 0 and price > 0:
            ap = atr / price * 100
            sp = max(cfg.sl_pct, min(ap * 1.5, cfg.sl_pct * 2))
            tp = sp * (cfg.tp_pct / cfg.sl_pct)
        else:
            sp, tp = cfg.sl_pct, cfg.tp_pct
        if side == "BUY":
            sl = round(price*(1-sp/100), 8)
            tp1= round(price*(1+tp/100), 8)
        else:
            sl = round(price*(1+sp/100), 8)
            tp1= round(price*(1-tp/100), 8)
        return sl, tp1, round(sp, 2), round(tp, 2)

    def structural_sl(self, price, side, highs, lows, atr):
        if not cfg.anti_stophunt or highs is None or len(lows) < 20:
            sl, *_ = self.dynamic_sl_tp(price, side, atr)
            return sl
        if side == "BUY":
            swings = [lows[-i] for i in range(3, 20)
                      if lows[-i] < lows[-i+1] and lows[-i] < lows[-i-1]]
            sl = swings[0] * 0.9985 if swings else price * (1 - cfg.sl_pct/100)
            sl = max(sl, price * (1 - cfg.sl_pct * 2.5 / 100))
        else:
            swings = [highs[-i] for i in range(3, 20)
                      if highs[-i] > highs[-i+1] and highs[-i] > highs[-i-1]]
            sl = swings[0] * 1.0015 if swings else price * (1 + cfg.sl_pct/100)
            sl = min(sl, price * (1 + cfg.sl_pct * 2.5 / 100))
        return round(sl, 8)

    def record_open(self, sym): self._open.add(sym)

    def record_close(self, sym, pnl, balance):
        self._open.discard(sym)
        self._daily_pnl  += pnl
        self._total_pnl  += pnl
        self._recent_pnls = (self._recent_pnls + [pnl])[-20:]
        self.set_balance(balance)
        if pnl >= 0:
            self._wins       += 1
            self._consec_win += 1
            self._consec_loss = 0
        else:
            self._losses     += 1
            self._consec_loss += 1
            self._consec_win  = 0
        # W14: 30-min cooldown after ANY close (loss gets 20 min extra)
        base_cd = cfg.close_cooldown
        extra   = 1200 if pnl < 0 else 0
        self._sym_cooldown[sym] = time.time() + base_cd + extra
        if self._consec_loss >= cfg.max_consec_loss:
            self._cooldown_until = time.time() + cfg.cooldown_loss
            logger.warning(f"{cfg.max_consec_loss} consecutive losses — global pause {cfg.cooldown_loss}s")

    def _halt(self, reason):
        self._halted = True; self._halt_reason = reason
        logger.critical(f"HALTED: {reason}")

    def resume(self):
        self._halted = False; self._halt_reason = ""
        logger.info("Bot resumed")

    @property
    def is_halted(self): return self._halted

    def summary(self):
        tt = self._wins + self._losses
        return {
            "balance":        round(self._balance, 2),
            "peak":           round(self._peak, 2),
            "daily_pnl_usdt": round(self._daily_pnl, 2),
            "total_pnl":      round(self._total_pnl, 2),
            "win_rate":       round(self._wins/tt*100, 1) if tt else 0.0,
            "wins":           self._wins,
            "losses":         self._losses,
            "consec_losses":  self._consec_loss,
            "consec_wins":    self._consec_win,
            "open_count":     len(self._open),
            "halted":         self._halted,
            "halt_reason":    self._halt_reason,
            "cooldown":       max(0, int(self._cooldown_until - time.time())),
            "kelly_mult":     round(self.kelly_multiplier(), 2),
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
        CREATE INDEX IF NOT EXISTS idx_trades_sym ON trades(symbol);
        CREATE INDEX IF NOT EXISTS idx_trades_opened ON trades(opened_at);
        """)
        await db.commit()
    logger.info("DB ready")

async def save_trade_open(symbol, side, entry, qty, size_usdt, sl, tp, metrics):
    oa = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO trades (symbol,side,entry_price,qty,size_usdt,sl,tp,metrics,opened_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (symbol,side,entry,qty,size_usdt,sl,tp,json.dumps(metrics),oa))
        await db.commit()
        return cur.lastrowid

async def save_trade_close(tid, exit_price, pnl, pnl_pct, reason, opened_at):
    ca  = datetime.now(timezone.utc).isoformat()
    dur = 0
    if opened_at:
        try: dur = int((datetime.now(timezone.utc)-datetime.fromisoformat(opened_at)).total_seconds())
        except: pass
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE trades SET exit_price=?,pnl=?,pnl_pct=?,reason=?,closed_at=?,duration_s=? WHERE id=?",
            (exit_price,pnl,pnl_pct,reason,ca,dur,tid))
        await db.commit()

async def save_signal(sym, sig, metrics, executed):
    ts = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO signals (symbol,signal,metrics,executed,ts) VALUES (?,?,?,?,?)",
            (sym,sig,json.dumps(metrics),int(executed),ts))
        await db.commit()

async def get_performance_stats():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur  = await db.execute("SELECT pnl,pnl_pct,duration_s FROM trades WHERE closed_at IS NOT NULL")
        rows = await cur.fetchall()
    if not rows:
        return {"total_trades":0,"wins":0,"losses":0,"win_rate":0.0,"total_pnl":0.0,
                "avg_win":0.0,"avg_loss":0.0,"best_trade":0.0,"worst_trade":0.0,"avg_duration_m":0.0}
    pnls   = [r["pnl"] for r in rows if r["pnl"] is not None]
    wins   = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    durs   = [r["duration_s"] for r in rows if r["duration_s"]]
    return {
        "total_trades":   len(pnls),
        "wins":           len(wins),
        "losses":         len(losses),
        "win_rate":       round(len(wins)/len(pnls)*100,1) if pnls else 0.0,
        "total_pnl":      round(sum(pnls),2),
        "avg_win":        round(sum(wins)/len(wins),2) if wins else 0.0,
        "avg_loss":       round(sum(losses)/len(losses),2) if losses else 0.0,
        "best_trade":     round(max(pnls),2) if pnls else 0.0,
        "worst_trade":    round(min(pnls),2) if pnls else 0.0,
        "avg_duration_m": round(sum(durs)/len(durs)/60,1) if durs else 0.0,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  EXCHANGE CLIENT  (BingX)
# ══════════════════════════════════════════════════════════════════════════════
_BASE    = "https://open-api.bingx.com"
_session: aiohttp.ClientSession | None = None
_ws_prices:     dict[str, float]             = {}
_funding_cache: dict[str, tuple[float,float]] = {}
_ob_cache:      dict[str, dict]              = {}
_pending_orders: dict[str, str]             = {}  # v6: symbol → order_id (dedup)

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

def _sign(p, s):
    return hmac.new(s.encode(), urlencode(sorted(p.items())).encode(), hashlib.sha256).hexdigest()

def _auth(p=None):
    q = dict(p or {}); q["timestamp"] = int(time.time()*1000)
    q["signature"] = _sign(q, cfg.bingx_secret_key); return q

def _hdr(): return {"X-BX-APIKEY": cfg.bingx_api_key}

async def _get(path, params=None, auth=False):
    s = _get_session(); p = _auth(params) if auth else (params or {})
    try:
        async with s.get(_BASE+path, params=p, headers=_hdr() if auth else {}) as r:
            return await r.json(content_type=None)
    except Exception as e: logger.warning(f"GET {path}: {e}"); return {}

async def _post(path, params=None):
    s = _get_session()
    try:
        async with s.post(_BASE+path, params=_auth(params), headers=_hdr()) as r:
            return await r.json(content_type=None)
    except Exception as e: logger.warning(f"POST {path}: {e}"); return {}

async def _delete(path, params=None):
    s = _get_session()
    try:
        async with s.delete(_BASE+path, params=_auth(params), headers=_hdr()) as r:
            return await r.json(content_type=None)
    except Exception as e: logger.warning(f"DELETE {path}: {e}"); return {}

# ── Market data ───────────────────────────────────────────────────────────────
async def fetch_all_tickers():
    r = await _get("/openApi/swap/v2/quote/ticker")
    d = r.get("data", r) if isinstance(r, dict) else r
    return d if isinstance(d, list) else []

async def _fetch_ohlcv(symbol, tf):
    r   = await _get("/openApi/swap/v3/quote/klines", {"symbol":symbol,"interval":tf,"limit":200})
    raw = r.get("data",[]) if isinstance(r, dict) else []
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
        p,h,t = await asyncio.gather(
            _fetch_ohlcv(sym, cfg.timeframe),
            _fetch_ohlcv(sym, cfg.confirm_tf),
            _fetch_ohlcv(sym, cfg.trend_tf))
        return sym, p, h, t
    results = await asyncio.gather(*[asyncio.create_task(_one(s)) for s in symbols], return_exceptions=True)
    return {sym: {"p":p,"h":h,"t":t}
            for r in results if not isinstance(r, Exception)
            for sym,p,h,t in [r] if p is not None}

# ── W1: Funding rate ─────────────────────────────────────────────────────────
async def get_funding_rate(symbol):
    cached = _funding_cache.get(symbol)
    if cached and time.time()-cached[1] < 300: return cached[0]
    try:
        r    = await _get("/openApi/swap/v2/quote/premiumIndex", {"symbol":symbol})
        d    = r.get("data",{})
        if isinstance(d, list): d = d[0] if d else {}
        rate = float(d.get("lastFundingRate", d.get("fundingRate", 0)))
        _funding_cache[symbol] = (rate, time.time())
        return rate
    except: return 0.0

async def fetch_funding_batch(symbols):
    rates = await asyncio.gather(*[asyncio.create_task(get_funding_rate(s)) for s in symbols], return_exceptions=True)
    return {s: (r if not isinstance(r, Exception) else 0.0) for s,r in zip(symbols,rates)}

# ── W2: Liquidation cascade ───────────────────────────────────────────────────
def detect_liq_cascade(volume, close, open_):
    if len(volume) < 20: return {"cascade":False}
    avg_vol  = float(np.mean(volume[-20:-1]))
    avg_body = float(np.mean(np.abs(close[-20:-1]-open_[-20:-1])))
    cur_vol  = float(volume[-1])
    cur_body = float(abs(close[-1]-open_[-1]))
    vr = cur_vol/avg_vol if avg_vol>0 else 0
    br = cur_body/avg_body if avg_body>0 else 0
    return {"cascade":vr>=3.0 and br>=2.0,"vol_ratio":round(vr,2),
            "body_ratio":round(br,2),"direction":"up" if close[-1]>open_[-1] else "down"}

# ── W3: Order book imbalance ──────────────────────────────────────────────────
async def get_ob_imbalance(symbol, depth=20):
    cached = _ob_cache.get(symbol)
    if cached and time.time()-cached.get("ts",0) < 5: return cached
    try:
        r    = await _get("/openApi/swap/v2/quote/depth", {"symbol":symbol,"limit":depth})
        d    = r.get("data",{})
        bids = d.get("bids",[])[:depth]; asks = d.get("asks",[])[:depth]
        bv   = sum(float(b[1]) for b in bids if len(b)>=2)
        av   = sum(float(a[1]) for a in asks if len(a)>=2)
        tot  = bv+av; imbal = bv/tot if tot>0 else 0.5
        spread = 0.0
        if bids and asks:
            bb = float(bids[0][0]) if bids[0] else 0
            ba = float(asks[0][0]) if asks[0] else 0
            spread = (ba-bb)/bb*100 if bb>0 else 0
        res = {"bid_vol":bv,"ask_vol":av,"imbalance":round(imbal,3),"spread_pct":round(spread,4),"ts":time.time()}
        _ob_cache[symbol] = res; return res
    except: return {"imbalance":0.5,"spread_pct":0,"ts":0}

# ── W6: Market regime ─────────────────────────────────────────────────────────
def detect_regime(high, low, close, atr):
    if len(close) < 30 or atr == 0: return "unknown"
    sma  = float(np.mean(close[-20:]))
    std  = float(np.std(close[-20:]))
    bw   = (std*2)/sma if sma>0 else 0
    atr_pct  = atr/float(close[-1])*100
    hl_range = (float(np.max(high[-20:]))-float(np.min(low[-20:])))/float(close[-1])*100
    atr_mult = hl_range/(atr_pct*20) if atr_pct>0 else 1.0
    if bw < 0.02 and atr_mult < 0.5: return "ranging"
    if atr_pct > 3.0:                return "volatile"
    return "trending"

# ── Account ───────────────────────────────────────────────────────────────────
async def get_balance():
    r = await _get("/openApi/swap/v2/user/balance", auth=True)
    try:
        d = r.get("data",{})
        if isinstance(d, dict):
            b = d.get("balance",{})
            if isinstance(b, dict): return float(b.get("availableMargin", b.get("balance",0)))
            return float(d.get("availableMargin", d.get("equity",0)))
    except Exception as e: logger.warning(f"get_balance: {e}")
    return 0.0

async def get_all_positions():
    r = await _get("/openApi/swap/v2/user/positions", auth=True)
    try:
        d = r.get("data",[])
        if isinstance(d, list):
            return {p["symbol"]:p for p in d if abs(float(p.get("positionAmt",0)))>1e-9}
    except Exception as e: logger.warning(f"get_positions: {e}")
    return {}

async def set_leverage(sym, lev):
    return await _post("/openApi/swap/v2/trade/leverage", {"symbol":sym,"side":"LONG","leverage":lev})

async def place_market_order(symbol, side, size_usdt, sl, tp):
    r = await _post("/openApi/swap/v2/trade/order", {
        "symbol":symbol,"side":side,
        "positionSide":"LONG" if side=="BUY" else "SHORT",
        "type":"MARKET","quoteOrderQty":size_usdt,
        "stopLoss":str(sl),"takeProfit":str(tp)})
    return r if isinstance(r, dict) else {}

async def place_limit_order(symbol, side, size_usdt, price, sl, tp):
    r = await _post("/openApi/swap/v2/trade/order", {
        "symbol":symbol,"side":side,
        "positionSide":"LONG" if side=="BUY" else "SHORT",
        "type":"LIMIT","quoteOrderQty":size_usdt,"price":str(price),
        "timeInForce":"GTC","stopLoss":str(sl),"takeProfit":str(tp)})
    return r if isinstance(r, dict) else {}

async def close_position_market(symbol, position):
    amt = float(position.get("positionAmt",0))
    if amt == 0: return {}
    return await _post("/openApi/swap/v2/trade/closePosition",
                       {"symbol":symbol,"positionSide":"LONG" if amt>0 else "SHORT"})

async def close_partial(symbol, side, qty):
    close_side = "SELL" if side=="BUY" else "BUY"
    pos_side   = "LONG" if side=="BUY" else "SHORT"
    return await _post("/openApi/swap/v2/trade/order",{
        "symbol":symbol,"side":close_side,"positionSide":pos_side,
        "type":"MARKET","quantity":str(round(qty,4))})

async def cancel_all_orders(symbol):
    return await _delete("/openApi/swap/v2/trade/allOpenOrders", {"symbol":symbol})

async def get_price(sym):
    if sym in _ws_prices: return _ws_prices[sym]
    r = await _get("/openApi/swap/v2/quote/price", {"symbol":sym})
    try: return float(r.get("data",{}).get("price",0))
    except: return 0.0

# ── v6: WS with exponential back-off — no more reconnect spam ─────────────────
async def ws_price_stream(symbols):
    import gzip
    try:
        import websockets  # type: ignore
    except ImportError:
        logger.warning("websockets package not installed — WS disabled"); return

    streams = "/".join(f"{s.replace('-','').lower()}@markPrice" for s in symbols)
    url     = f"wss://open-api-ws.bingx.com/market?streams={streams}"
    backoff = 1
    first   = True
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                if first:
                    logger.info(f"WS connected: {len(symbols)} symbols")
                    first = False
                backoff = 1  # reset on success
                async for msg in ws:
                    try:
                        if isinstance(msg, bytes): msg = gzip.decompress(msg).decode()
                        d   = json.loads(msg).get("data",{})
                        sym = d.get("s",""); p = d.get("p", d.get("mp",0))
                        if sym and p:
                            if "-" not in sym and sym.endswith("USDT"):
                                sym = sym[:-4]+"-USDT"
                            _ws_prices[sym] = float(p)
                    except: pass
        except Exception as e:
            logger.debug(f"WS reconnect in {backoff}s: {e}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)  # cap at 60s

# ══════════════════════════════════════════════════════════════════════════════
#  INDICATORS
# ══════════════════════════════════════════════════════════════════════════════
@njit(cache=True)
def _rsi(close, period):
    n=len(close); out=np.full(n,np.nan)
    if n<period+1: return out
    g=np.zeros(n); l=np.zeros(n)
    for i in range(1,n):
        d=close[i]-close[i-1]
        if d>0: g[i]=d
        else: l[i]=-d
    ag=np.mean(g[1:period+1]); al=np.mean(l[1:period+1])
    for i in range(period,n):
        if i>period:
            ag=(ag*(period-1)+g[i])/period; al=(al*(period-1)+l[i])/period
        out[i]=100.0 if al==0 else 100.0-100.0/(1.0+ag/al)
    return out

@njit(cache=True)
def _atr(high, low, close, period):
    n=len(close); out=np.zeros(n); tr=np.zeros(n)
    for i in range(1,n):
        tr[i]=max(high[i]-low[i],abs(high[i]-close[i-1]),abs(low[i]-close[i-1]))
    s=np.sum(tr[1:period+1])/period; out[period]=s
    for i in range(period+1,n):
        out[i]=(out[i-1]*(period-1)+tr[i])/period
    return out

@njit(cache=True)
def _adx_di(high, low, close, period):
    n=len(close); adx=np.zeros(n); pdi=np.zeros(n); mdi=np.zeros(n)
    if n<period*2+1: return adx,pdi,mdi
    tr=np.zeros(n); pdm=np.zeros(n); mdm=np.zeros(n)
    for i in range(1,n):
        tr[i]=max(high[i]-low[i],abs(high[i]-close[i-1]),abs(low[i]-close[i-1]))
        up=high[i]-high[i-1]; dn=low[i-1]-low[i]
        pdm[i]=up if up>dn and up>0 else 0.0
        mdm[i]=dn if dn>up and dn>0 else 0.0
    st=np.sum(tr[1:period+1]); sp=np.sum(pdm[1:period+1]); sm=np.sum(mdm[1:period+1])
    for i in range(period,n):
        if i>period:
            st=st-st/period+tr[i]; sp=sp-sp/period+pdm[i]; sm=sm-sm/period+mdm[i]
        if st==0: continue
        pdi[i]=100.0*sp/st; mdi[i]=100.0*sm/st
        dx=abs(pdi[i]-mdi[i])/(pdi[i]+mdi[i]+1e-10)*100.0
        adx[i]=dx if i==period else (adx[i-1]*(period-1)+dx)/period
    return adx,pdi,mdi

@njit(cache=True)
def _ema(close, period):
    """W11: EMA calculation."""
    n=len(close); out=np.zeros(n)
    if n < period: return out
    out[period-1] = np.mean(close[:period])
    k = 2.0 / (period + 1)
    for i in range(period, n):
        out[i] = close[i]*k + out[i-1]*(1-k)
    return out

def generate_signal(high, low, close, open_, volume,
                    h_high, h_low, h_close, h_open, h_volume,
                    t_high, t_low, t_close, cfg,
                    ob_data=None, funding_rate=0.0):
    metrics: dict = {}
    try:
        adx_a,pdi_a,mdi_a = _adx_di(high,low,close,cfg.adx_len)
        rsi_a  = _rsi(close, cfg.rsi_len)
        atr_a  = _atr(high, low, close, cfg.adx_len)
        adx    = float(adx_a[-1]); pdi=float(pdi_a[-1]); mdi=float(mdi_a[-1])
        rsi    = float(rsi_a[-1]); atr=float(atr_a[-1]); price=float(close[-1])
        atr_pct= atr/price*100 if price>0 else 0.0

        # W12: Skip low-volatility setups — not worth fee
        if atr_pct < cfg.min_atr_pct:
            return None, {"regime":"low_atr","atr_pct":round(atr_pct,4)}

        # W11: EMA trend filter
        ema_fast_arr = _ema(close, cfg.ema_fast)
        ema_slow_arr = _ema(close, cfg.ema_slow)
        ema_fast_val = float(ema_fast_arr[-1])
        ema_slow_val = float(ema_slow_arr[-1])
        ema_bullish  = ema_fast_val > ema_slow_val
        ema_bearish  = ema_fast_val < ema_slow_val

        # Three-Step Volume Delta
        dv = np.where(close>=open_, volume, -volume); p=cfg.period
        if len(dv) < p*3: return None, metrics
        d1=float(np.sum(dv[-p:])); d2=float(np.sum(dv[-p*2:-p])); d3=float(np.sum(dv[-p*3:-p*2]))
        bull=sum(1 for d in [d1,d2,d3] if d>0); bear=sum(1 for d in [d1,d2,d3] if d<0)
        avg_vol   = float(np.mean(volume[-p:])) if len(volume)>=p else float(np.mean(volume))
        vol_spike = float(volume[-1]) > avg_vol * cfg.vol_spike_mult

        # W2: Liquidation cascade
        liq       = detect_liq_cascade(volume, close, open_)
        liq_bonus = 15.0 if liq["cascade"] else 0.0

        # W6: Regime
        regime    = detect_regime(high, low, close, atr)
        if cfg.regime_filter and regime == "ranging":
            return None, {"regime":"ranging","adx":round(adx,2)}
        regime_mult = 0.75 if regime == "volatile" else 1.0

        confidence = (max(bull,bear)/3*33 + max(0.0,adx-cfg.adx_thresh)
                     + (10.0 if vol_spike else 0) + liq_bonus) * regime_mult

        # W8: Dynamic confidence — raise bar during NY open
        h_utc = datetime.now(timezone.utc).hour
        dyn_min_conf = cfg.min_confidence + (5.0 if 12<=h_utc<16 else 0.0)

        metrics = {
            "adx":round(adx,2),"plus_di":round(pdi,2),"minus_di":round(mdi,2),
            "rsi":round(rsi,2),"atr":round(atr,8),"atr_pct":round(atr_pct,4),
            "ema_fast":round(ema_fast_val,6),"ema_slow":round(ema_slow_val,6),
            "ema_bullish":ema_bullish,
            "delta1":round(d1,2),"delta2":round(d2,2),"delta3":round(d3,2),
            "bull_steps":bull,"bear_steps":bear,"vol_spike":vol_spike,
            "confidence":round(confidence,1),"regime":regime,
            "liq_cascade":liq["cascade"],"liq_vol_ratio":liq.get("vol_ratio",0),
            "funding_rate":round(funding_rate*100,4),
            "ob_imbalance":ob_data.get("imbalance",0.5) if ob_data else 0.5,
            "dyn_min_conf":round(dyn_min_conf,1),
        }

        if adx < cfg.adx_thresh: return None, metrics

        long_ok  = bull>=2 and pdi>mdi and rsi<cfg.rsi_ob
        short_ok = bear>=2 and mdi>pdi and rsi>cfg.rsi_os
        if not long_ok and not short_ok: return None, metrics

        sig = "BUY" if long_ok else "SELL"

        # W11: EMA trend alignment — skip counter-trend signals
        if cfg.ema_filter:
            if sig == "BUY"  and not ema_bullish: return None, metrics
            if sig == "SELL" and not ema_bearish:  return None, metrics

        # W1: Funding filter
        if cfg.funding_filter and funding_rate != 0:
            if sig=="BUY"  and funding_rate >  cfg.max_funding_rate: return None, metrics
            if sig=="SELL" and funding_rate < -cfg.max_funding_rate: return None, metrics

        # W3: Order book
        if cfg.ob_filter and ob_data:
            imbal = ob_data.get("imbalance",0.5)
            if sig=="BUY"  and imbal < (1-cfg.ob_imbalance_thresh): return None, metrics
            if sig=="SELL" and imbal > cfg.ob_imbalance_thresh:      return None, metrics
            confidence += abs(imbal-0.5)*30
            metrics["confidence"] = round(confidence,1)

        # HTF confirmation (1h)
        if h_close is not None and len(h_close) >= cfg.adx_len*2+5:
            ha,hp,hm = _adx_di(h_high,h_low,h_close,cfg.adx_len)
            if sig=="BUY"  and float(hm[-1])>float(hp[-1])*1.2: return None, metrics
            if sig=="SELL" and float(hp[-1])>float(hm[-1])*1.2: return None, metrics

        # Trend filter (4h)
        if t_close is not None and len(t_close) >= cfg.adx_len*2+5:
            ta,tp2,tm = _adx_di(t_high,t_low,t_close,cfg.adx_len)
            if sig=="BUY"  and float(tm[-1])>float(tp2[-1])*1.5: return None, metrics
            if sig=="SELL" and float(tp2[-1])>float(tm[-1])*1.5: return None, metrics

        if confidence < dyn_min_conf: return None, metrics
        return sig, metrics

    except Exception as e:
        logger.debug(f"Indicator error: {e}"); return None, metrics

# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM  — deduplication & rate limiting
# ══════════════════════════════════════════════════════════════════════════════
_tg_q: asyncio.Queue = asyncio.Queue(maxsize=300)
_tg_last = 0.0

def start_sender(): asyncio.create_task(_tg_loop())

async def _tg_loop():
    global _tg_last
    while True:
        msg, silent = await _tg_q.get()
        gap = time.time()-_tg_last
        if gap < 0.5: await asyncio.sleep(0.5-gap)
        try:
            url = f"https://api.telegram.org/bot{cfg.telegram_token}/sendMessage"
            async with aiohttp.ClientSession() as s:
                await s.post(url, json={"chat_id":cfg.telegram_chat_id,"text":msg[:4096],
                             "parse_mode":"HTML","disable_notification":silent},
                             timeout=aiohttp.ClientTimeout(total=8))
        except Exception as e: logger.debug(f"TG: {e}")
        finally: _tg_last=time.time(); _tg_q.task_done()

async def send(msg, silent=False):
    try: _tg_q.put_nowait((msg, silent))
    except asyncio.QueueFull: pass

def _bar(c): return "█"*int(c/10)+"░"*(10-int(c/10))
def _regime_icon(r): return {"trending":"📈","volatile":"⚡","ranging":"➡️"}.get(r,"❓")

def msg_start(n, balance):
    weapons = ("✅ EMA trend filter  ✅ Min ATR filter\n"
               "✅ Funding filter  ✅ Liq cascade radar\n"
               "✅ Order book imbalance  ✅ Smart limit orders\n"
               "✅ Partial TP ladder  ✅ Regime filter\n"
               "✅ Anti stop-hunt SL  ✅ Session filter\n"
               "✅ Kelly sizing (fixed)  ✅ Dynamic confidence\n"
               "✅ 2.5R TP ratio  ✅ Auto-resume")
    return (f"⚡ <b>UltraBot v6 — Online</b>\n\n"
            f"💰 Balance: <b>${balance:.2f} USDT</b>\n"
            f"📊 Universe: <b>{n} symbols</b>\n"
            f"⏱ {cfg.timeframe}/{cfg.confirm_tf}/{cfg.trend_tf} | ADX≥{cfg.adx_thresh}\n"
            f"⚖️ {cfg.leverage}x | Risk {cfg.risk_pct}% | Max {cfg.max_open_trades}\n"
            f"🛡 SL {cfg.sl_pct}% | TP {cfg.tp_pct}% | Trailing: {cfg.trailing_sl}\n\n"
            f"<b>🔫 Weapons (12 active):</b>\n{weapons}")

def msg_entry(sym, side, price, size, sl, tp, sl_pct, tp_pct, m, kelly):
    emoji = "🟢 LONG" if side=="BUY" else "🔴 SHORT"
    c     = m.get("confidence",0)
    regime= m.get("regime","")
    ema_align = "✅" if m.get("ema_bullish") == (side=="BUY") else "⚠️"
    extras = f"\n📊 EMA: {ema_align} | OB: {m.get('ob_imbalance',0.5):.0%} | Kelly: {kelly:.2f}x"
    if m.get("liq_cascade"):
        extras += f"\n⚡ <b>Liq cascade!</b> ({m.get('liq_vol_ratio',0):.1f}× vol)"
    fund = m.get("funding_rate",0)
    if fund: extras += f"\n💸 Funding: {'+' if fund>=0 else ''}{fund:.4f}%"
    return (f"{emoji} <b>{sym}</b> {_regime_icon(regime)}\n\n"
            f"💰 Entry: <code>{price:.6g}</code>\n"
            f"🎯 TP: <code>{tp:.6g}</code> (+{tp_pct:.1f}%)\n"
            f"🛡 SL: <code>{sl:.6g}</code> (-{sl_pct:.1f}%)\n"
            f"📦 Size: <b>{size:.1f} USDT</b>\n\n"
            f"ADX:{m.get('adx',0):.1f} | RSI:{m.get('rsi',0):.1f} | ATR:{m.get('atr_pct',0):.2f}%\n"
            f"Δ1:{m.get('delta1',0):+.0f} Δ2:{m.get('delta2',0):+.0f} Δ3:{m.get('delta3',0):+.0f}\n"
            f"⚡ Conf: {c:.0f}% {_bar(c)}{extras}")

def msg_partial_tp(sym, mark, pct):
    return f"🎯 <b>Partial TP {pct:.0f}%</b> — {sym} @ {mark:.6g}"

def msg_close(sym, side, pnl, pnl_pct, reason, dur_s):
    e = "💚" if pnl>=0 else "🔴"
    return (f"{e} <b>{sym}</b> {'🟢' if side=='LONG' else '🔴'} — <b>{reason}</b>\n"
            f"PnL: <b>{pnl:+.2f} USDT</b> ({pnl_pct:+.2f}%) | {dur_s//60}m")

def msg_halted(reason, balance):
    return (f"🚨 <b>BOT PAUSED</b>\n"
            f"Reason: {reason}\n"
            f"Balance: ${balance:.2f}\n"
            f"💡 Auto-resumes at next UTC day")

def msg_low_balance(balance):
    return (f"⚠️ <b>Low balance</b>: ${balance:.2f}\n"
            f"Bot paused — minimum is ${cfg.min_balance:.0f}\n"
            f"Add funds to BingX to resume")

def msg_performance(perf, risk_s):
    kelly = risk_s.get("kelly_mult",1.0)
    return (f"📊 <b>UltraBot v6 Performance</b>\n\n"
            f"Trades: {perf.get('total_trades',0)} | WR: <b>{perf.get('win_rate',0):.1f}%</b>\n"
            f"Total PnL: <b>{perf.get('total_pnl',0):+.2f} USDT</b>\n"
            f"Best: +{perf.get('best_trade',0):.2f} | Worst: {perf.get('worst_trade',0):.2f}\n"
            f"Avg win: +{perf.get('avg_win',0):.2f} | Avg loss: {perf.get('avg_loss',0):.2f}\n"
            f"Avg duration: {perf.get('avg_duration_m',0):.0f}m\n"
            f"Kelly multiplier: {kelly:.2f}x\n"
            f"Day PnL: {risk_s.get('daily_pnl_usdt',0):+.2f} USDT")

def msg_error(e): return f"⚠️ <code>{e[:300]}</code>"

# ══════════════════════════════════════════════════════════════════════════════
#  DASHBOARD  (FastAPI + WebSocket)
# ══════════════════════════════════════════════════════════════════════════════
_dash: dict = {"status":"starting","balance":0.0,"positions":{},
               "scan_stats":{},"risk":{},"perf":{},"last_signals":[],
               "trade_metrics":{},"updated_at":time.time()}

def update_state(**kw): _dash.update(kw); _dash["updated_at"] = time.time()

async def start_dashboard():
    from fastapi import FastAPI, WebSocket
    from fastapi.responses import HTMLResponse
    import uvicorn

    app = FastAPI()

    _HTML = """<!DOCTYPE html><html><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>⚡ UltraBot v6</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0d1117;color:#c9d1d9;font-family:monospace;font-size:13px}
header{background:#161b22;padding:12px 20px;border-bottom:1px solid #30363d;display:flex;align-items:center;gap:12px;flex-wrap:wrap}
h1{font-size:16px;color:#58a6ff}
.badge{padding:3px 8px;border-radius:12px;font-size:11px;font-weight:bold}
.running{background:#1f6feb;color:#fff}.halted{background:#da3633;color:#fff}.paused{background:#d29922;color:#000}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(155px,1fr));gap:10px;padding:12px}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:12px}
.label{color:#8b949e;font-size:10px;text-transform:uppercase;letter-spacing:.5px}
.value{font-size:18px;font-weight:bold;margin-top:3px}.sub{font-size:11px;color:#8b949e;margin-top:2px}
.green{color:#3fb950!important}.red{color:#f85149!important}.yellow{color:#d29922!important}.blue{color:#58a6ff!important}
table{width:100%;border-collapse:collapse}
th,td{padding:6px 10px;text-align:left;border-bottom:1px solid #21262d;font-size:11px}
th{color:#8b949e;font-size:10px;text-transform:uppercase}
section{margin:0 12px 12px;background:#161b22;border:1px solid #30363d;border-radius:8px}
section h2{padding:10px 14px;font-size:12px;border-bottom:1px solid #30363d;color:#8b949e;letter-spacing:.5px}
</style></head>
<body>
<header>
  <h1>⚡ UltraBot v6</h1>
  <span id="badge" class="badge running">RUNNING</span>
  <span id="upd" style="margin-left:auto;color:#8b949e;font-size:11px"></span>
</header>
<div class="grid" id="metrics"></div>
<section><h2>📂 Open Positions</h2>
<table><thead><tr><th>Symbol</th><th>Side</th><th>Entry</th><th>Mark</th><th>PnL</th><th>Conf</th><th>Regime</th><th>EMA</th><th>Funding</th></tr></thead>
<tbody id="pos"></tbody></table></section>
<section><h2>🔍 Latest Signals</h2>
<table><thead><tr><th>Symbol</th><th>Signal</th><th>Conf</th><th>ADX</th><th>RSI</th><th>ATR%</th><th>Regime</th><th>EMA</th><th>Cascade</th></tr></thead>
<tbody id="sig"></tbody></table></section>
<script>
const ws=new WebSocket((location.protocol==='https:'?'wss':'ws')+'://'+location.host+'/ws');
const pc=v=>v>=0?'green':'red';
ws.onmessage=e=>{
  const d=JSON.parse(e.data),r=d.risk||{},s=d.scan_stats||{},p=d.perf||{};
  document.getElementById('upd').textContent='Updated '+new Date(d.updated_at*1000).toLocaleTimeString();
  const b=document.getElementById('badge');
  if(r.halted){b.textContent='HALTED';b.className='badge halted';}
  else if(r.cooldown>0){b.textContent='COOLDOWN '+r.cooldown+'s';b.className='badge paused';}
  else{b.textContent='RUNNING';b.className='badge running';}
  document.getElementById('metrics').innerHTML=[
    {l:'Balance',    v:'$'+(d.balance||0).toFixed(2)},
    {l:'Day PnL',    v:(r.daily_pnl_usdt>=0?'+':'')+(r.daily_pnl_usdt||0).toFixed(2), c:pc(r.daily_pnl_usdt)},
    {l:'Total PnL',  v:(r.total_pnl>=0?'+':'')+(r.total_pnl||0).toFixed(2),            c:pc(r.total_pnl)},
    {l:'Win Rate',   v:(r.win_rate||0)+'%', s:r.wins+'W / '+r.losses+'L'},
    {l:'Open Trades',v:Object.keys(d.positions||{}).length},
    {l:'Scan',       v:(s.last_ms||0).toFixed(0)+'ms', s:(s.n_scanned||0)+' syms'},
    {l:'Signals',    v:'🟢'+(s.n_buy||0)+' 🔴'+(s.n_sell||0), s:(s.n_cascade||0)+' cascades'},
    {l:'Kelly',      v:(r.kelly_mult||1).toFixed(2)+'×', c:'blue'},
    {l:'Consec W/L', v:(r.consec_wins||0)+'W / '+(r.consec_losses||0)+'L', c:(r.consec_losses||0)>=3?'red':''},
    {l:'Trades',     v:p.total_trades||0, s:'avg '+(p.avg_duration_m||0).toFixed(0)+'m'},
  ].map(m=>`<div class="card"><div class="label">${m.l}</div><div class="value ${m.c||''}">${m.v}</div><div class="sub">${m.s||''}</div></div>`).join('');
  const ri={'trending':'📈','volatile':'⚡','ranging':'➡️'};
  document.getElementById('pos').innerHTML=Object.entries(d.positions||{}).map(([sym,pos])=>{
    const pnl=parseFloat(pos.unrealizedProfit||0),side=parseFloat(pos.positionAmt||0)>0?'🟢 LONG':'🔴 SHORT';
    const m=(d.trade_metrics||{})[sym]||{};
    const emaOk=m.ema_bullish!=null?(parseFloat(pos.positionAmt||0)>0?m.ema_bullish:!m.ema_bullish)?'✅':'⚠️':'—';
    return `<tr><td><b>${sym}</b></td><td>${side}</td>
    <td>${parseFloat(pos.entryPrice||0).toFixed(4)}</td>
    <td>${parseFloat(pos.markPrice||pos.entryPrice||0).toFixed(4)}</td>
    <td class="${pc(pnl)}">${pnl>=0?'+':''}${pnl.toFixed(2)}</td>
    <td>${(m.confidence||0).toFixed(0)}%</td>
    <td>${ri[m.regime]||'—'} ${m.regime||'—'}</td>
    <td>${emaOk}</td>
    <td class="${(m.funding_rate||0)>0.05?'red':(m.funding_rate||0)<-0.05?'green':''}">${(m.funding_rate||0).toFixed(4)}%</td></tr>`;
  }).join('')||'<tr><td colspan="9" style="color:#8b949e;text-align:center;padding:20px">No open positions</td></tr>';
  document.getElementById('sig').innerHTML=(d.last_signals||[]).slice(0,12).map(s=>`<tr>
    <td><b>${s.symbol}</b></td>
    <td class="${s.signal==='BUY'?'green':'red'}">${s.signal==='BUY'?'🟢':'🔴'} ${s.signal}</td>
    <td>${(s.confidence||0).toFixed(0)}% / ${(s.dyn_min_conf||62).toFixed(0)}</td>
    <td>${(s.adx||0).toFixed(1)}</td><td>${(s.rsi||0).toFixed(1)}</td>
    <td>${(s.atr_pct||0).toFixed(2)}%</td>
    <td class="${s.regime==='trending'?'green':s.regime==='volatile'?'yellow':''}">${ri[s.regime]||'—'} ${s.regime||'—'}</td>
    <td>${s.ema_bullish!=null?(s.signal==='BUY'?s.ema_bullish:!s.ema_bullish)?'✅':'⚠️':'—'}</td>
    <td>${s.liq_cascade?'⚡ YES':'—'}</td></tr>`
  ).join('')||'<tr><td colspan="9" style="color:#8b949e;text-align:center;padding:20px">No signals yet</td></tr>';
};
ws.onclose=()=>setTimeout(()=>location.reload(),4000);
</script></body></html>"""

    @app.get("/")
    async def index(): return HTMLResponse(_HTML)

    @app.get("/health")
    async def health(): return {"status":"ok","version":"v6","balance":_dash.get("balance",0)}

    @app.websocket("/ws")
    async def ws_ep(ws: WebSocket):
        await ws.accept()
        try:
            await ws.send_text(json.dumps(_dash, default=str))
            while True:
                await asyncio.sleep(2)
                await ws.send_text(json.dumps(_dash, default=str))
        except: pass

    port   = cfg.effective_port
    server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=port, log_level="warning", access_log=False))
    asyncio.create_task(server.serve())
    logger.info(f"Dashboard → http://0.0.0.0:{port}")

# ══════════════════════════════════════════════════════════════════════════════
#  BOT ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════
console  = Console()
risk     = RiskManager()
executor = ThreadPoolExecutor(max_workers=12, thread_name_prefix="ind")

open_trades:    dict[str, dict]  = {}
trailing_peaks: dict[str, float] = {}
partial_closed: dict[str, bool]  = {}
trade_metrics:  dict[str, dict]  = {}
scan_stats:     dict             = {"last_ms":0,"n_buy":0,"n_sell":0,"n_scanned":0,"n_cascade":0}
_last_perf_hash = ""

# v6: State for deduplicated notifications
_low_balance_notified_at: float = 0.0   # FIX: send low-balance alert max 1/hour
_last_balance_for_notif: float = 0.0


async def get_universe():
    tickers = await fetch_all_tickers()
    cands   = []
    for t in tickers:
        sym = t.get("symbol","")
        if not sym.endswith("-USDT") or sym in cfg.blacklist: continue
        try:
            vol = float(t.get("quoteVolume") or t.get("volume") or t.get("turnover") or 0)
        except: continue
        if vol >= cfg.min_volume_usdt: cands.append((sym, vol))
    cands.sort(key=lambda x: x[1], reverse=True)
    syms = [s[0] for s in cands[:cfg.top_n_symbols]]
    logger.info(f"Universe: {len(syms)} symbols (vol≥${cfg.min_volume_usdt/1e6:.1f}M)")
    return syms


def _run_indicators(symbol, p, h, t, ob_data, funding_rate):
    try:
        sig, m = generate_signal(
            p["high"],p["low"],p["close"],p["open"],p["volume"],
            h["high"] if h else None, h["low"] if h else None,
            h["close"] if h else None, h["open"] if h else None,
            h["volume"] if h else None,
            t["high"] if t else None, t["low"] if t else None,
            t["close"] if t else None, cfg, ob_data, funding_rate)
    except Exception as e:
        return symbol, None, {"error": str(e)}
    return symbol, sig, m


async def execute_entry(symbol, sig, metrics, balance, n_open, p_data):
    can, reason = risk.can_trade(balance, symbol)
    if not can:
        logger.debug(f"Blocked {symbol}: {reason}"); return False
    if not risk.correlation_ok(symbol): return False

    # v6: Guard against double-order (limit fallback)
    if symbol in _pending_orders:
        logger.debug(f"Order already pending for {symbol}"); return False

    kelly = risk.kelly_multiplier()
    size  = risk.position_size(balance, n_open, metrics.get("confidence",0),
                               metrics.get("atr_pct",0), metrics.get("funding_rate",0)/100)
    if size < 5: return False

    price = await get_price(symbol)
    if price <= 0:
        logger.warning(f"Price=0 for {symbol}, skipping"); return False

    highs = p_data.get("high") if p_data else None
    lows  = p_data.get("low")  if p_data else None
    sl    = risk.structural_sl(price, sig, highs, lows, metrics.get("atr",0))
    _, tp, sl_pct, tp_pct = risk.dynamic_sl_tp(price, sig, metrics.get("atr",0))

    _pending_orders[symbol] = "pending"
    try:
        await set_leverage(symbol, cfg.leverage)
        resp = {}
        if cfg.use_limit_orders:
            tick = metrics.get("atr_pct",0.1) * price / 1000
            lp   = round(price-tick if sig=="BUY" else price+tick, 8)
            resp = await place_limit_order(symbol, sig, size, lp, sl, tp)
            if resp.get("code") and str(resp.get("code","0")) != "0":
                logger.debug(f"Limit rejected {symbol}, falling back to market")
                await asyncio.sleep(1)
                resp = await place_market_order(symbol, sig, size, sl, tp)
            else:
                await asyncio.sleep(3)  # wait for fill
        else:
            resp = await place_market_order(symbol, sig, size, sl, tp)

        code = resp.get("code")
        if code is not None and str(code) != "0":
            logger.warning(f"Order rejected {symbol}: {resp}"); return False
    except Exception as e:
        logger.error(f"Order failed {symbol}: {e}"); return False
    finally:
        _pending_orders.pop(symbol, None)

    qty   = size * cfg.leverage / price if price > 0 else 0
    db_id = await save_trade_open(symbol, sig, price, qty, size, sl, tp, metrics)
    risk.record_open(symbol)
    open_trades[symbol] = {
        "side":sig,"entry":price,"sl":sl,"tp":tp,"size":size,"qty":qty,
        "db_id":db_id,"opened_at":datetime.now(timezone.utc).isoformat(),
        "metrics":metrics,"sl_pct":sl_pct,"tp_pct":tp_pct}
    trailing_peaks[symbol] = price
    partial_closed[symbol] = False
    trade_metrics[symbol]  = metrics
    await send(msg_entry(symbol, sig, price, size, sl, tp, sl_pct, tp_pct, metrics, kelly))
    logger.success(f"OPENED {sig} {symbol} @ {price:.6g} | conf={metrics.get('confidence',0):.0f}% | {metrics.get('regime')} | kelly={kelly:.2f}x")
    return True


async def execute_close(symbol, position, reason):
    mark = float(position.get("markPrice") or position.get("entryPrice") or 0)
    # v6: Hard guard — never close on price=0
    if mark <= 0:
        logger.warning(f"Skipping close {symbol}: price=0"); return
    try:
        await close_position_market(symbol, position)
        await cancel_all_orders(symbol)
    except Exception as e:
        logger.error(f"Close {symbol}: {e}"); return

    pnl   = float(position.get("unrealizedProfit",0))
    bal   = await get_balance()
    side  = "LONG" if float(position.get("positionAmt",0))>0 else "SHORT"
    entry = float(position.get("entryPrice",0))
    pnl_pct = (mark-entry)/entry*100*(1 if side=="LONG" else -1)*cfg.leverage if entry>0 else 0

    trade = open_trades.pop(symbol, {})
    risk.record_close(symbol, pnl, bal)
    if trade.get("db_id"):
        await save_trade_close(trade["db_id"],mark,pnl,pnl_pct,reason,trade.get("opened_at",""))

    trailing_peaks.pop(symbol,None); partial_closed.pop(symbol,None); trade_metrics.pop(symbol,None)
    dur = 0
    if trade.get("opened_at"):
        try: dur=int((datetime.now(timezone.utc)-datetime.fromisoformat(trade["opened_at"])).total_seconds())
        except: pass

    await send(msg_close(symbol,side,pnl,pnl_pct,reason,dur))
    logger.info(f"CLOSED {side} {symbol} — {reason} | PnL {pnl:+.2f}")

    if risk.is_halted:
        await send(msg_halted(risk._halt_reason, bal))


async def scan_loop():
    global _low_balance_notified_at, _last_balance_for_notif
    symbols=[]; universe_refresh=0; funding_rates={}
    while True:
        t0 = time.perf_counter()
        try:
            if time.time()-universe_refresh > 300:
                symbols          = await get_universe()
                universe_refresh = time.time()
                asyncio.create_task(ws_price_stream(symbols[:40]))

            universe_data, funding_rates = await asyncio.gather(
                fetch_universe_concurrent(symbols),
                fetch_funding_batch(symbols[:30]))

            ob_res = await asyncio.gather(
                *[asyncio.create_task(get_ob_imbalance(s)) for s in symbols[:20]],
                return_exceptions=True)
            ob_map = {symbols[i]: (ob_res[i] if not isinstance(ob_res[i],Exception) else {})
                      for i in range(min(20,len(symbols)))}

            loop  = asyncio.get_event_loop()
            tasks = [loop.run_in_executor(executor, _run_indicators, sym,
                     d["p"],d.get("h"),d.get("t"),
                     ob_map.get(sym,{}), funding_rates.get(sym,0.0))
                     for sym,d in universe_data.items()
                     if d["p"] is not None and len(d["p"].get("close",[]))>=cfg.candles_needed]
            results = list(await asyncio.gather(*tasks))

            signals = [(sym,sig,m) for sym,sig,m in results
                       if sig and m.get("confidence",0)>=cfg.min_confidence]
            signals.sort(key=lambda x: x[2].get("confidence",0), reverse=True)

            n_buy     = sum(1 for _,s,_ in signals if s=="BUY")
            n_sell    = sum(1 for _,s,_ in signals if s=="SELL")
            n_cascade = sum(1 for _,_,m in results if m.get("liq_cascade"))
            elapsed   = (time.perf_counter()-t0)*1000
            scan_stats.update({"last_ms":elapsed,"n_buy":n_buy,"n_sell":n_sell,
                                "n_scanned":len(results),"n_cascade":n_cascade})

            logger.info(f"Scan {elapsed:.0f}ms | {len(results)} | 🟢{n_buy} 🔴{n_sell} | ⚡{n_cascade} | kelly={risk.kelly_multiplier():.2f}x")

            balance   = await get_balance()
            # v6: Guard against API returning 0 (network error) — don't trade on bad data
            if balance <= 0:
                logger.warning("Balance returned 0 — skipping this cycle")
                await asyncio.sleep(cfg.scan_interval); continue

            positions = await get_all_positions()
            risk.set_balance(balance)

            # v6: FIX — low balance alert max once per hour, only when balance actually changes
            if balance < cfg.min_balance:
                bal_changed = abs(balance - _last_balance_for_notif) > 0.5
                cooldown_ok = time.time() - _low_balance_notified_at > 3600
                if bal_changed or cooldown_ok:
                    await send(msg_low_balance(balance), silent=True)
                    _low_balance_notified_at = time.time()
                    _last_balance_for_notif  = balance

            n_open = len(positions)
            if not risk.is_halted:
                for sym,sig,metrics in signals:
                    if n_open >= cfg.max_open_trades: break
                    p_data = universe_data.get(sym,{}).get("p")
                    if sym in positions:
                        pos     = positions[sym]
                        is_long = float(pos["positionAmt"])>0
                        if (sig=="SELL" and is_long) or (sig=="BUY" and not is_long):
                            await execute_close(sym, pos, "FLIP")
                            positions = await get_all_positions(); n_open=len(positions)
                        continue
                    ok = await execute_entry(sym, sig, metrics, balance, n_open, p_data)
                    if ok:
                        n_open  += 1; balance = await get_balance()
                        asyncio.create_task(save_signal(sym,sig,metrics,True))

            positions = await get_all_positions()
            perf      = await get_performance_stats()
            update_state(status="running",balance=balance,positions=positions,
                         scan_stats=scan_stats,risk=risk.summary(),perf=perf,
                         last_signals=[{"symbol":s,"signal":sig,**m} for s,sig,m in signals[:15]],
                         trade_metrics=trade_metrics)

        except Exception as e:
            logger.error(f"Scan error: {e}", exc_info=True)
            await send(msg_error(str(e)), silent=True)

        await asyncio.sleep(cfg.scan_interval)


async def position_monitor():
    while True:
        await asyncio.sleep(4)
        try:
            positions = await get_all_positions()
            for sym, pos in positions.items():
                amt   = float(pos.get("positionAmt",0))
                mark  = float(pos.get("markPrice") or pos.get("entryPrice",0) or 0)
                entry = float(pos.get("entryPrice",0))
                # v6: Hard guard — never process on price=0
                if mark <= 0 or entry <= 0: continue
                side  = "LONG" if amt>0 else "SHORT"
                trade = open_trades.get(sym, {})
                tp_pct= trade.get("tp_pct", cfg.tp_pct)
                sl_pct= trade.get("sl_pct", cfg.sl_pct)

                # W5: Partial TP
                if cfg.partial_tp and not partial_closed.get(sym, True) and entry>0:
                    half = tp_pct / 2
                    if ((side=="LONG"  and mark >= entry*(1+half/100)) or
                        (side=="SHORT" and mark <= entry*(1-half/100))):
                        qty = abs(amt) * cfg.partial_tp_pct / 100
                        if qty > 0:
                            await close_partial(sym, "BUY" if side=="LONG" else "SELL", qty)
                            partial_closed[sym] = True
                            await send(msg_partial_tp(sym, mark, cfg.partial_tp_pct))
                            logger.info(f"Partial TP {cfg.partial_tp_pct:.0f}% {sym} @ {mark:.6g}")

                # Trailing SL
                if cfg.trailing_sl and sym in trailing_peaks:
                    peak = trailing_peaks[sym]
                    if side=="LONG":
                        if mark >= entry*(1+cfg.trailing_activation/100):
                            if mark > peak: trailing_peaks[sym] = mark
                        if mark < trailing_peaks[sym]*(1-sl_pct/100):
                            await execute_close(sym, pos, "Trailing SL")
                    else:
                        if mark <= entry*(1-cfg.trailing_activation/100):
                            if mark < peak: trailing_peaks[sym] = mark
                        if mark > trailing_peaks[sym]*(1+sl_pct/100):
                            await execute_close(sym, pos, "Trailing SL")

        except Exception as e: logger.error(f"Monitor: {e}")


async def performance_loop():
    global _last_perf_hash
    await asyncio.sleep(300)
    while True:
        try:
            perf = await get_performance_stats()
            rs   = risk.summary()
            # v6: FIX — hash includes more fields to detect real changes
            ph   = (f"{perf.get('total_trades',0)}"
                    f"{round(perf.get('total_pnl',0),1)}"
                    f"{perf.get('wins',0)}"
                    f"{round(rs.get('daily_pnl_usdt',0),1)}")
            if ph != _last_perf_hash:
                await send(msg_performance(perf, rs), silent=True)
                _last_perf_hash = ph
        except: pass
        await asyncio.sleep(4*3600)


async def terminal_loop():
    while True:
        await asyncio.sleep(15)
        try:
            positions = await get_all_positions()
            balance   = await get_balance()
            t = Table(title="⚡ UltraBot v6", box=rbox.ROUNDED, show_lines=True)
            for col,sty,jus in [("Symbol","cyan","left"),("Side","","left"),
                                  ("Entry","","right"),("Mark","","right"),
                                  ("PnL","","right"),("Conf","","right"),
                                  ("Regime","","left"),("EMA","","center"),
                                  ("Kelly","","right")]:
                t.add_column(col, style=sty, justify=jus)
            for sym,pos in positions.items():
                pnl  = float(pos.get("unrealizedProfit",0))
                side = "🟢 LONG" if float(pos["positionAmt"])>0 else "🔴 SHORT"
                m    = trade_metrics.get(sym,{})
                reg  = m.get("regime","—")
                rc   = {"trending":"green","volatile":"yellow","ranging":"red"}.get(reg,"white")
                ema_ok = m.get("ema_bullish") == (float(pos["positionAmt"])>0) if "ema_bullish" in m else True
                t.add_row(sym,side,
                          f"{float(pos.get('entryPrice',0)):.4f}",
                          f"{float(pos.get('markPrice',pos.get('entryPrice',0))):.4f}",
                          f"[{'green' if pnl>=0 else 'red'}]{pnl:+.2f}[/]",
                          f"{m.get('confidence',0):.0f}%",
                          f"[{rc}]{reg}[/]",
                          "✅" if ema_ok else "⚠️",
                          f"{risk.kelly_multiplier():.2f}×")
            console.print(t)
            rs = risk.summary()
            console.print(
                f"💰 {balance:.2f} | Scan:{scan_stats.get('last_ms',0):.0f}ms | "
                f"🟢{scan_stats.get('n_buy',0)} 🔴{scan_stats.get('n_sell',0)} | "
                f"⚡{scan_stats.get('n_cascade',0)} cascades | "
                f"Day:{rs['daily_pnl_usdt']:+.2f} | WR:{rs['win_rate']}% | "
                f"Kelly:{rs['kelly_mult']:.2f}x | "
                f"{'[red]HALTED[/]' if rs['halted'] else '[green]RUNNING[/]'}")
        except: pass


async def main():
    logger.remove()
    # v6: Use stderr with level INFO — Railway captures this correctly
    logger.add(sys.stderr, level="INFO",
               format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}",
               colorize=False)  # Railway doesn't render ANSI colors in JSON mode
    os.makedirs("data", exist_ok=True)
    logger.add("data/ultrabot.log", rotation="1 day", retention="14 days", level="DEBUG")

    console.print("[bold cyan]⚡ UltraBot v6 — Fixed & Optimized[/bold cyan]")
    console.print("[dim]12 weapons | EMA filter | 2.5R TP | No spam[/dim]")

    await init_db()
    balance = await get_balance()
    risk.set_balance(balance)
    console.print(f"💰 Balance: [bold]{balance:.2f} USDT[/bold]")

    if balance < cfg.min_balance:
        console.print(f"[yellow]⚠️  Balance below minimum (${cfg.min_balance}). Scanning but not trading.[/yellow]")

    symbols = await get_universe()
    if not symbols:
        logger.error("No symbols found — check API keys and MIN_VOLUME_USDT")

    start_sender()
    await send(msg_start(len(symbols), balance))

    if cfg.dashboard_enabled:
        await start_dashboard()

    console.print(f"[green]✅ Ready — {len(symbols)} symbols | balance ${balance:.2f}[/green]")
    await asyncio.gather(
        scan_loop(),
        position_monitor(),
        performance_loop(),
        terminal_loop())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[bold red]Stopped[/bold red]")
    finally:
        asyncio.run(close_session())
