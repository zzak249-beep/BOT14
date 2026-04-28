"""
MACD Divergencia + TD Sequence Bot para BingX Perpetual Futures
Multi-símbolo: escanea todos los pares líquidos automáticamente

CAMBIOS vs versión anterior:
  - Escanea TODOS los pares USDT de BingX (filtra por volumen mínimo)
  - Gestiona posiciones múltiples simultáneas (MAX_OPEN_POSITIONS)
  - Fix 'open_time not in index' — parser robusto de klines
  - Fix 'single positional indexer out-of-bounds' — validación df vacío
  - Cooldown por símbolo tras SL para evitar re-entrar inmediatamente
  - Estado guardado por símbolo en bot_state.json
"""

import os, sys, time, json, hmac, hashlib, logging, requests
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass, field, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────
API_KEY    = os.getenv("BINGX_API_KEY", "")
API_SECRET = os.getenv("BINGX_SECRET_KEY", os.getenv("BINGX_API_SECRET", ""))
TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", os.getenv("TELEGRAM_TOKEN", ""))
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
BASE_URL   = "https://open-api.bingx.com"

# ── Filtro de símbolos ───────────────────────
MIN_VOLUME_USDT    = float(os.getenv("MIN_VOLUME_USDT", "5000000"))   # 5M USDT 24h mínimo
TOP_N_SYMBOLS      = int(os.getenv("TOP_N_SYMBOLS", "40"))            # Top 40 por volumen
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "3"))        # Máx posiciones simultáneas
SYMBOL_REFRESH_SEC = int(os.getenv("SYMBOL_REFRESH_SEC", "3600"))     # Refrescar lista cada 1h

BLACKLIST = {"USDC-USDT", "BUSD-USDT", "TUSD-USDT", "DAI-USDT", "FDUSD-USDT", "USDP-USDT"}

# ── Riesgo y órdenes ─────────────────────────
_order_size        = float(os.getenv("ORDER_SIZE", "10"))
MAX_POSITION_USDT  = _order_size
MIN_POSITION_USDT  = max(5.0, _order_size * 0.5)
PAPER_MODE         = os.getenv("MODE", os.getenv("PAPER", "true")).lower() in ("true", "1")
RISK_PER_TRADE     = float(os.getenv("RISK_PCT", "0.02"))
MAX_LEVERAGE       = float(os.getenv("LEVERAGE", "10"))

# ── Estrategia ───────────────────────────────
MIN_DIV_STRENGTH   = float(os.getenv("MIN_DIV", "0.25"))
ATR_STOP_MULT      = float(os.getenv("ATR_STOP", "1.5"))
TRAILING_ATR       = float(os.getenv("TRAIL_ATR", "2.0"))
TRAILING_PCT       = float(os.getenv("TRAIL_PCT", "0.05"))
MIN_PROFIT_TRAIL   = float(os.getenv("MIN_PROFIT_TRAIL", "0.01"))
ENABLE_BUY_FILTER  = os.getenv("BUY_FILTER", "true").lower() == "true"
BUY_VOL_RATIO      = float(os.getenv("VOL_RATIO", "0.8"))
ENABLE_30M_CLEAR   = os.getenv("CLEAR_30M", "true").lower() == "true"
MIN_BARS_PROTECTION= int(os.getenv("MIN_BARS", "1"))
INITIAL_ADD_SIZE   = float(os.getenv("INITIAL_ADD", "0.3"))
CHECK_INTERVAL     = int(os.getenv("CHECK_SEC", "60"))
KLINES_LIMIT       = int(os.getenv("KLINES_LIMIT", "300"))
COOLDOWN_AFTER_SL  = int(os.getenv("COOLDOWN_SL_MIN", "60"))  # minutos

MIN_DF_ROWS = 60
TP_RATIOS   = {"1m": 0.25, "3m": 0.20, "5m": 0.25}
STATE_FILE  = "bot_state.json"

# ─────────────────────────────────────────────
# LOGGER
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger("MACD_TD_BOT")

# ─────────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────────
def tg(msg: str):
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        log.warning(f"Telegram: {e}")

# ─────────────────────────────────────────────
# BINGX API
# ─────────────────────────────────────────────
def _sign(params: dict) -> str:
    query = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    return hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()

def _req(method: str, path: str, params: dict = None) -> Optional[dict]:
    params = dict(params or {})
    params["timestamp"] = int(time.time() * 1000)
    params["signature"] = _sign(params)
    headers = {"X-BX-APIKEY": API_KEY}
    url = BASE_URL + path
    try:
        if method == "GET":
            r = requests.get(url, params=params, headers=headers, timeout=10)
        else:
            r = requests.post(url, params=params, headers=headers, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.debug(f"API {method} {path}: {e}")
        return None

def get_balance() -> float:
    r = _req("GET", "/openApi/swap/v2/user/balance")
    if not r or r.get("code") != 0:
        return 0.0
    bal = r.get("data", {}).get("balance", {})
    if isinstance(bal, list):
        for a in bal:
            if a.get("asset") == "USDT":
                return float(a.get("availableMargin", 0))
    elif isinstance(bal, dict):
        return float(bal.get("availableMargin", bal.get("balance", 0)))
    return 0.0

def get_mark_price(symbol: str) -> float:
    r = _req("GET", "/openApi/swap/v2/quote/price", {"symbol": symbol})
    if r and r.get("code") == 0:
        d = r.get("data", {})
        if isinstance(d, dict):
            return float(d.get("price", 0))
        if isinstance(d, list) and d:
            return float(d[0].get("price", 0))
    return 0.0

def get_all_tickers() -> List[dict]:
    """Devuelve todos los tickers de futuros perpetuos."""
    r = _req("GET", "/openApi/swap/v2/quote/ticker")
    if r and r.get("code") == 0:
        data = r.get("data", [])
        return data if isinstance(data, list) else []
    return []

def get_klines(symbol: str, interval: str, limit: int = 300) -> Optional[pd.DataFrame]:
    """
    Descarga klines y devuelve DataFrame con columnas estándar.
    Fix robusto para ambos formatos (lista de listas y lista de dicts).
    """
    r = _req("GET", "/openApi/swap/v3/quote/klines",
             {"symbol": symbol, "interval": interval, "limit": limit})
    if not r or r.get("code") != 0:
        return None
    raw = r.get("data") or r.get("result")
    if not raw or len(raw) == 0:
        return None

    try:
        if isinstance(raw[0], dict):
            df = pd.DataFrame(raw)
            # Mapear todas las variantes de nombres de columnas que usa BingX
            rename = {
                "t": "time", "T": "time", "openTime": "time", "open_time": "time",
                "o": "open", "O": "open",
                "h": "high", "H": "high",
                "l": "low",  "L": "low",
                "c": "close","C": "close",
                "v": "volume","V": "volume", "q": "volume",
            }
            df.rename(columns=rename, inplace=True)
        else:
            # Lista de listas: [ts, open, high, low, close, volume, ...]
            df = pd.DataFrame(raw)
            n = len(df.columns)
            base = ["time", "open", "high", "low", "close", "volume"]
            extra = [f"_x{i}" for i in range(n - len(base))]
            df.columns = base + extra

        # Verificar columnas requeridas
        required = ["time", "open", "high", "low", "close", "volume"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            log.debug(f"Klines {symbol} {interval}: columnas faltantes {missing}. Disponibles: {list(df.columns)}")
            return None

        df = df[required].copy()
        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df["time"] = pd.to_datetime(
            pd.to_numeric(df["time"], errors="coerce"), unit="ms", utc=True
        )
        df.dropna(subset=["time", "close"], inplace=True)
        df = df[df["close"] > 0].copy()
        df.sort_values("time", inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df if len(df) >= 20 else None

    except Exception as e:
        log.debug(f"Klines parse {symbol} {interval}: {e}")
        return None

def set_leverage_sym(symbol: str, leverage: int):
    for side in ["LONG", "SHORT"]:
        _req("POST", "/openApi/swap/v2/trade/leverage",
             {"symbol": symbol, "side": side, "leverage": leverage})

def place_order_sym(symbol: str, side: str, usdt_size: float,
                    reduce_only: bool = False) -> Optional[dict]:
    price = get_mark_price(symbol)
    if price <= 0:
        return None
    if PAPER_MODE:
        qty = round(usdt_size / price, 4)
        log.info(f"[PAPER] {symbol} {side} {qty} @ ~{price:.4f} (${usdt_size:.2f})")
        return {"qty": qty, "price": price, "usdt": qty * price}
    qty = round(usdt_size / price, 4)
    if qty <= 0:
        return None
    pos_side = "LONG" if side == "BUY" else "SHORT"
    params = {
        "symbol": symbol, "side": side,
        "positionSide": pos_side,
        "type": "MARKET", "quantity": qty,
    }
    if reduce_only:
        params["reduceOnly"] = "true"
        params["positionSide"] = "LONG" if side == "SELL" else "SHORT"
    r = _req("POST", "/openApi/swap/v2/trade/order", params)
    if r and r.get("code") == 0:
        return {"qty": qty, "price": price, "usdt": qty * price}
    log.error(f"Order FAILED {symbol}: {r}")
    return None

def close_position_sym(symbol: str, pos_side: str) -> bool:
    r = _req("POST", "/openApi/swap/v2/trade/closeAllPositions",
             {"symbol": symbol, "positionSide": pos_side})
    return bool(r and r.get("code") == 0)

# ─────────────────────────────────────────────
# INDICADORES
# ─────────────────────────────────────────────
def calc_indicators(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    WARMUP = 30
    if df is None or len(df) < WARMUP + 20:
        return None

    df = df.copy()
    df.dropna(subset=["open", "high", "low", "close", "volume"], inplace=True)
    df = df[df["close"] > 0].copy()
    df.reset_index(drop=True, inplace=True)

    if len(df) < WARMUP + 20:
        return None

    c, h, lo, v = df["close"], df["high"], df["low"], df["volume"]

    ema12 = c.ewm(span=12, adjust=False).mean()
    ema26 = c.ewm(span=26, adjust=False).mean()
    macd  = ema12 - ema26
    sig9  = macd.ewm(span=9, adjust=False).mean()

    ema8      = c.ewm(span=8,  adjust=False).mean()
    ema17     = c.ewm(span=17, adjust=False).mean()
    macd_fast = ema8 - ema17
    sig6      = macd_fast.ewm(span=6, adjust=False).mean()

    prev_c = c.shift(1)
    tr     = pd.concat([(h - lo), (h - prev_c).abs(), (lo - prev_c).abs()], axis=1).max(axis=1)
    atr    = tr.rolling(14).mean()

    ema20 = c.ewm(span=20, adjust=False).mean()
    ema60 = c.ewm(span=60, adjust=False).mean()

    delta    = c.diff()
    avg_gain = delta.clip(lower=0).rolling(14).mean()
    avg_loss = (-delta).clip(lower=0).rolling(14).mean()
    rs       = avg_gain / avg_loss.replace(0, np.nan)
    rsi      = 100 - (100 / (1 + rs))

    vol_ma    = v.rolling(20).mean()
    vol_ratio = (v / vol_ma.replace(0, np.nan)).fillna(1.0)

    df["macd"]      = macd
    df["macd_sig"]  = sig9
    df["macd_hist"] = macd - sig9
    df["macd_fast"] = macd_fast - sig6
    df["atr"]       = atr
    df["ema20"]     = ema20
    df["ema60"]     = ema60
    df["rsi"]       = rsi
    df["vol_ratio"] = vol_ratio

    df = df.iloc[WARMUP:].copy()
    df.reset_index(drop=True, inplace=True)

    key_cols = ["macd", "macd_sig", "atr", "ema20", "rsi"]
    df.dropna(subset=key_cols, inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df if len(df) >= MIN_DF_ROWS else None

# ─────────────────────────────────────────────
# DIVERGENCIA
# ─────────────────────────────────────────────
def find_extremes(data: np.ndarray, window: int = 3):
    peaks, troughs = [], []
    for i in range(window, len(data) - window):
        if all(data[i] > data[i-j] and data[i] > data[i+j] for j in range(1, window+1)):
            peaks.append((i, data[i]))
        if all(data[i] < data[i-j] and data[i] < data[i+j] for j in range(1, window+1)):
            troughs.append((i, data[i]))
    return peaks, troughs

def bullish_divergence(price_tr, macd_tr, fast_tr=None):
    if len(price_tr) < 2 or len(macd_tr) < 2:
        return False, 0, None
    p1, p2 = price_tr[-2], price_tr[-1]
    m1, m2 = macd_tr[-2], macd_tr[-1]
    if p2[1] < p1[1] and m2[1] > m1[1]:
        s = min(1.0, abs((p2[1]-p1[1])/p1[1])*50 +
                ((m2[1]-m1[1])/abs(m1[1]) if m1[1]!=0 else 1)*50)
        return True, s, {"price_low": p2[1], "index": p2[0], "strength": s}
    if fast_tr and len(fast_tr) >= 2:
        f1, f2 = fast_tr[-2], fast_tr[-1]
        if p2[1] < p1[1] and f2[1] > f1[1]:
            s = min(0.8, abs((p2[1]-p1[1])/p1[1])*30 +
                    ((f2[1]-f1[1])/abs(f1[1]) if f1[1]!=0 else 1)*30)
            return True, s, {"price_low": p2[1], "index": p2[0], "strength": s}
    return False, 0, None

def bearish_divergence(price_pk, macd_pk, fast_pk=None):
    if len(price_pk) < 2 or len(macd_pk) < 2:
        return False, 0, None
    p1, p2 = price_pk[-2], price_pk[-1]
    m1, m2 = macd_pk[-2], macd_pk[-1]
    if p2[1] > p1[1] and m2[1] < m1[1]:
        s = min(1.0, abs((p2[1]-p1[1])/p1[1])*50 +
                abs((m2[1]-m1[1])/abs(m1[1]) if m1[1]!=0 else 1)*50)
        return True, s, {"price_high": p2[1], "index": p2[0], "strength": s}
    if fast_pk and len(fast_pk) >= 2:
        f1, f2 = fast_pk[-2], fast_pk[-1]
        if p2[1] > p1[1] and f2[1] < f1[1]:
            s = min(0.8, abs((p2[1]-p1[1])/p1[1])*30 +
                    abs((f2[1]-f1[1])/abs(f1[1]) if f1[1]!=0 else 1)*30)
            return True, s, {"price_high": p2[1], "index": p2[0], "strength": s}
    return False, 0, None

# ─────────────────────────────────────────────
# TD SETUP
# ─────────────────────────────────────────────
def td_setup(df: pd.DataFrame, period: int = 9) -> int:
    if df is None or len(df) < period + 4:
        return 0
    closes = df["close"].values
    buy  = all(closes[-i] <= closes[-i-4] for i in range(1, period+1))
    sell = all(closes[-i] >= closes[-i-4] for i in range(1, period+1))
    return 1 if buy else (-1 if sell else 0)

# ─────────────────────────────────────────────
# ESTADO
# ─────────────────────────────────────────────
@dataclass
class Position:
    symbol:       str
    side:         str
    entry_price:  float
    entry_time:   str
    size_usdt:    float
    remain_usdt:  float
    stop_loss:    float
    highest:      float
    lowest:       float
    tp_done:      List[str] = field(default_factory=list)
    initial_added:bool = False
    bar_count:    int  = 0

@dataclass
class BotState:
    balance:       float = 0.0
    positions:     Dict[str, dict] = field(default_factory=dict)  # symbol → Position dict
    last_bull_idx: Dict[str, int]  = field(default_factory=dict)
    last_bear_idx: Dict[str, int]  = field(default_factory=dict)
    cooldowns:     Dict[str, str]  = field(default_factory=dict)  # symbol → iso timestamp
    trades:        List[dict]      = field(default_factory=list)

def load_state() -> BotState:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                d = json.load(f)
            return BotState(**d)
        except Exception as e:
            log.warning(f"State load: {e}")
    return BotState()

def save_state(st: BotState):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(asdict(st), f, indent=2, default=str)
    except Exception as e:
        log.error(f"State save: {e}")

# ─────────────────────────────────────────────
# SYMBOL SCANNER
# ─────────────────────────────────────────────
class SymbolScanner:
    def __init__(self):
        self._symbols: List[str] = []
        self._last_refresh: float = 0.0

    def get_symbols(self) -> List[str]:
        if time.time() - self._last_refresh > SYMBOL_REFRESH_SEC or not self._symbols:
            self._refresh()
        return list(self._symbols)

    def _refresh(self):
        tickers = get_all_tickers()
        if not tickers:
            log.warning("Sin tickers de BingX, manteniendo lista anterior")
            return

        candidates = []
        for t in tickers:
            sym = t.get("symbol", "")
            if not sym.endswith("-USDT") or sym in BLACKLIST:
                continue
            try:
                vol = float(t.get("quoteVolume") or t.get("turnover") or
                            t.get("volume") or 0)
                if vol >= MIN_VOLUME_USDT:
                    candidates.append((sym, vol))
            except (ValueError, TypeError):
                continue

        if not candidates:
            log.warning(f"0 símbolos superaron MIN_VOLUME_USDT={MIN_VOLUME_USDT:,.0f}")
            # Fallback: top 20 sin filtro
            candidates = []
            for t in tickers:
                sym = t.get("symbol", "")
                if sym.endswith("-USDT") and sym not in BLACKLIST:
                    try:
                        vol = float(t.get("quoteVolume") or t.get("volume") or 0)
                        candidates.append((sym, vol))
                    except Exception:
                        pass

        candidates.sort(key=lambda x: x[1], reverse=True)
        self._symbols = [s for s, _ in candidates[:TOP_N_SYMBOLS]]
        self._last_refresh = time.time()
        log.info(f"🔍 Escaneando {len(self._symbols)} símbolos: "
                 f"{', '.join(self._symbols[:8])}{'...' if len(self._symbols)>8 else ''}")

# ─────────────────────────────────────────────
# BOT PRINCIPAL
# ─────────────────────────────────────────────
class MACDTDBot:
    def __init__(self):
        self.state   = load_state()
        self.scanner = SymbolScanner()
        # Reconstruir posiciones activas
        self.positions: Dict[str, Position] = {}
        for sym, pd_ in self.state.positions.items():
            try:
                self.positions[sym] = Position(**pd_)
            except Exception:
                pass

        mode = "📋 PAPER" if PAPER_MODE else "💰 REAL"
        tg(f"🤖 <b>MACD+TD MultiSymbol Bot</b>\n"
           f"Modo: {mode}\n"
           f"Top {TOP_N_SYMBOLS} pares por volumen\n"
           f"Vol mínimo: ${MIN_VOLUME_USDT/1e6:.0f}M USDT/24h\n"
           f"Máx posiciones: {MAX_OPEN_POSITIONS}\n"
           f"Tamaño orden: ${MAX_POSITION_USDT} USDT\n"
           f"Apalancamiento: {int(MAX_LEVERAGE)}x")
        log.info(f"Bot iniciado — {len(self.positions)} posiciones activas")

    def _in_cooldown(self, symbol: str) -> bool:
        ts_str = self.state.cooldowns.get(symbol)
        if not ts_str:
            return False
        try:
            ts = datetime.fromisoformat(ts_str)
            remaining = (ts - datetime.now(timezone.utc)).total_seconds()
            return remaining > 0
        except Exception:
            return False

    def _set_cooldown(self, symbol: str):
        from datetime import timedelta
        expiry = datetime.now(timezone.utc) + timedelta(minutes=COOLDOWN_AFTER_SL)
        self.state.cooldowns[symbol] = expiry.isoformat()

    def fetch_symbol(self, symbol: str) -> Optional[dict]:
        """Descarga klines multi-timeframe para un símbolo."""
        dfs = {}
        for tf in ["1m", "3m", "5m", "15m", "30m"]:
            raw = get_klines(symbol, tf, KLINES_LIMIT)
            if raw is None or len(raw) < 50:
                return None
            df = calc_indicators(raw)
            if df is None or len(df) == 0:
                return None
            dfs[tf] = df
        return dfs

    def buy_filter_ok(self, df15: pd.DataFrame) -> Tuple[bool, str]:
        if not ENABLE_BUY_FILTER:
            return True, "sin filtro"
        if df15 is None or len(df15) == 0:
            return False, "df vacío"
        row = df15.iloc[-1]
        conds = []
        if row["rsi"] < 65:
            conds.append(f"RSI={row['rsi']:.0f}")
        if len(df15) >= 2 and df15["macd_hist"].iloc[-1] > df15["macd_hist"].iloc[-2]:
            conds.append("MACD↑")
        if row["vol_ratio"] > BUY_VOL_RATIO:
            conds.append(f"vol×{row['vol_ratio']:.1f}")
        dist = (row["close"] - row["ema20"]) / row["ema20"]
        if dist > -0.05:
            conds.append(f"EMA20 OK")
        ok = len(conds) >= 2
        return ok, ("✅ " if ok else "❌ ") + " ".join(conds)

    def sell_filter_ok(self, df15: pd.DataFrame) -> Tuple[bool, str]:
        if not ENABLE_BUY_FILTER:
            return True, "sin filtro"
        if df15 is None or len(df15) == 0:
            return False, "df vacío"
        row = df15.iloc[-1]
        conds = []
        if row["rsi"] > 35:
            conds.append(f"RSI={row['rsi']:.0f}")
        if len(df15) >= 2 and df15["macd_hist"].iloc[-1] < df15["macd_hist"].iloc[-2]:
            conds.append("MACD↓")
        if row["vol_ratio"] > BUY_VOL_RATIO:
            conds.append(f"vol×{row['vol_ratio']:.1f}")
        dist = (row["close"] - row["ema20"]) / row["ema20"]
        if dist < 0.05:
            conds.append("EMA20 OK")
        ok = len(conds) >= 2
        return ok, ("✅ " if ok else "❌ ") + " ".join(conds)

    def calc_size(self, symbol: str, price: float, stop: float, strength: float) -> float:
        bal = get_balance()
        if bal <= 0:
            return 0.0
        risk = bal * RISK_PER_TRADE
        dist = abs(price - stop)
        if dist <= 0:
            dist = price * 0.01
        mult = 0.5 + strength
        size = risk * mult / (dist / price)
        size = min(size, MAX_POSITION_USDT, bal * MAX_LEVERAGE)
        size = max(size, MIN_POSITION_USDT)
        return round(size, 2)

    def open_long(self, symbol: str, price: float, stop: float,
                  strength: float, reason: str):
        size = self.calc_size(symbol, price, stop, strength)
        if size <= 0:
            return
        set_leverage_sym(symbol, int(MAX_LEVERAGE))
        res = place_order_sym(symbol, "BUY", size)
        if not res:
            return
        pos = Position(
            symbol=symbol, side="long",
            entry_price=res["price"], entry_time=str(datetime.now(timezone.utc)),
            size_usdt=res["usdt"], remain_usdt=res["usdt"],
            stop_loss=stop, highest=res["price"], lowest=res["price"]
        )
        self.positions[symbol] = pos
        self.state.positions[symbol] = asdict(pos)
        save_state(self.state)
        tg(f"📈 <b>LONG {symbol}</b>\n"
           f"Precio: ${res['price']:.4f}\n"
           f"Tamaño: ${res['usdt']:.2f}\n"
           f"Stop: ${stop:.4f}\n"
           f"Fuerza: {strength:.2f}\n"
           f"{reason}")

    def open_short(self, symbol: str, price: float, stop: float,
                   strength: float, reason: str):
        size = self.calc_size(symbol, price, stop, strength)
        if size <= 0:
            return
        set_leverage_sym(symbol, int(MAX_LEVERAGE))
        res = place_order_sym(symbol, "SELL", size)
        if not res:
            return
        pos = Position(
            symbol=symbol, side="short",
            entry_price=res["price"], entry_time=str(datetime.now(timezone.utc)),
            size_usdt=res["usdt"], remain_usdt=res["usdt"],
            stop_loss=stop, highest=res["price"], lowest=res["price"]
        )
        self.positions[symbol] = pos
        self.state.positions[symbol] = asdict(pos)
        save_state(self.state)
        tg(f"📉 <b>SHORT {symbol}</b>\n"
           f"Precio: ${res['price']:.4f}\n"
           f"Tamaño: ${res['usdt']:.2f}\n"
           f"Stop: ${stop:.4f}\n"
           f"Fuerza: {strength:.2f}\n"
           f"{reason}")

    def close_all_sym(self, symbol: str, price: float, reason: str):
        pos = self.positions.get(symbol)
        if not pos:
            return
        pos_side = "LONG" if pos.side == "long" else "SHORT"
        close_position_sym(symbol, pos_side)
        pnl = ((price - pos.entry_price) if pos.side == "long"
               else (pos.entry_price - price))
        pnl_usdt = pnl / pos.entry_price * pos.remain_usdt * MAX_LEVERAGE
        self.state.trades.append({
            "symbol": symbol, "side": pos.side,
            "entry": pos.entry_price, "exit": price,
            "pnl_usdt": round(pnl_usdt, 4), "reason": reason,
            "time": str(datetime.now(timezone.utc))
        })
        del self.positions[symbol]
        del self.state.positions[symbol]
        if reason == "Stop Loss":
            self._set_cooldown(symbol)
        save_state(self.state)
        emoji = "🟢" if pnl_usdt > 0 else "🔴"
        tg(f"{emoji} <b>CERRADO {symbol}</b> ({reason})\n"
           f"PnL≈ ${pnl_usdt:+.2f} USDT")

    def update_trailing(self, pos: Position, price: float, atr: float) -> bool:
        profit_pct = ((price - pos.entry_price) / pos.entry_price
                      if pos.side == "long"
                      else (pos.entry_price - price) / pos.entry_price)
        if profit_pct < MIN_PROFIT_TRAIL:
            return False
        updated = False
        if pos.side == "long":
            pos.highest = max(pos.highest, price)
            new_sl = max(pos.highest - atr * TRAILING_ATR,
                         pos.highest * (1 - TRAILING_PCT))
            if new_sl > pos.stop_loss:
                pos.stop_loss = new_sl
                updated = True
        else:
            pos.lowest = min(pos.lowest, price)
            new_sl = min(pos.lowest + atr * TRAILING_ATR,
                         pos.lowest * (1 + TRAILING_PCT))
            if new_sl < pos.stop_loss:
                pos.stop_loss = new_sl
                updated = True
        return updated

    def process_symbol(self, symbol: str):
        """Lógica completa para un símbolo."""
        try:
            # ── Con posición abierta ──────────────────
            if symbol in self.positions:
                pos = self.positions[symbol]
                price = get_mark_price(symbol)
                if price <= 0:
                    return

                dfs = self.fetch_symbol(symbol)
                if not dfs:
                    return

                df15  = dfs["15m"]
                atr_s = df15["atr"].dropna()
                if len(atr_s) == 0:
                    return
                atr = float(atr_s.iloc[-1])

                td = {tf: td_setup(dfs[tf]) for tf in ["1m", "3m", "5m", "15m", "30m"]}

                pos.bar_count += 1
                in_protection = pos.bar_count < MIN_BARS_PROTECTION
                in_profit = (price > pos.entry_price if pos.side == "long"
                             else price < pos.entry_price)

                # Trailing stop
                if in_profit and not in_protection:
                    self.update_trailing(pos, price, atr)

                # Stop Loss
                sl_hit = ((price <= pos.stop_loss) if pos.side == "long"
                          else (price >= pos.stop_loss))
                if sl_hit:
                    self.close_all_sym(symbol, price, "Stop Loss")
                    return

                if not in_protection and in_profit:
                    # Take profit parciales por TD
                    for level, pct in TP_RATIOS.items():
                        if level in pos.tp_done:
                            continue
                        td_val = td.get(level, 0)
                        if (pos.side == "long" and td_val == -1) or \
                           (pos.side == "short" and td_val == 1):
                            close_usdt = pos.size_usdt * pct
                            if close_usdt >= MIN_POSITION_USDT:
                                side = "SELL" if pos.side == "long" else "BUY"
                                place_order_sym(symbol, side, close_usdt, reduce_only=True)
                                pos.remain_usdt -= close_usdt
                                pos.tp_done.append(level)
                                tg(f"🎯 Reducción {level} {symbol}\nPrecio: ${price:.4f}")

                    # TD9 15m/30m → cerrar todo
                    td15, td30 = td.get("15m", 0), td.get("30m", 0)
                    clear = False
                    if pos.side == "long":
                        if td15 == -1 or (ENABLE_30M_CLEAR and td30 == -1):
                            clear = True
                    else:
                        if td15 == 1 or (ENABLE_30M_CLEAR and td30 == 1):
                            clear = True
                    if clear:
                        self.close_all_sym(symbol, price, "TD9")

                # Actualizar estado
                self.state.positions[symbol] = asdict(pos)

            # ── Sin posición: buscar entrada ──────────
            elif len(self.positions) < MAX_OPEN_POSITIONS and not self._in_cooldown(symbol):
                dfs = self.fetch_symbol(symbol)
                if not dfs:
                    return

                df15 = dfs["15m"]
                price = get_mark_price(symbol)
                if price <= 0 or len(df15) == 0:
                    return

                atr_s = df15["atr"].dropna()
                if len(atr_s) == 0:
                    return
                atr = float(atr_s.iloc[-1])

                window = df15.tail(200)
                if len(window) < 20:
                    return

                cl = window["close"].values
                mc = window["macd"].values
                mf = window["macd_fast"].values

                pk_cl, tr_cl = find_extremes(cl)
                pk_mc, tr_mc = find_extremes(mc)
                pk_mf, tr_mf = find_extremes(mf)

                bull, bull_str, bull_info = bullish_divergence(tr_cl, tr_mc, tr_mf)
                bear, bear_str, bear_info = bearish_divergence(pk_cl, pk_mc, pk_mf)

                if bull and bull_str >= MIN_DIV_STRENGTH:
                    idx = bull_info["index"]
                    last = self.state.last_bull_idx.get(symbol, -1)
                    if idx != last:
                        ok, fmsg = self.buy_filter_ok(df15)
                        if ok:
                            stop = bull_info["price_low"] - atr * ATR_STOP_MULT
                            self.open_long(symbol, price, stop, bull_str, fmsg)
                            self.state.last_bull_idx[symbol] = idx
                        else:
                            log.debug(f"LONG {symbol} filtrado: {fmsg}")

                elif bear and bear_str >= MIN_DIV_STRENGTH:
                    idx = bear_info["index"]
                    last = self.state.last_bear_idx.get(symbol, -1)
                    if idx != last:
                        ok, fmsg = self.sell_filter_ok(df15)
                        if ok:
                            stop = bear_info["price_high"] + atr * ATR_STOP_MULT
                            self.open_short(symbol, price, stop, bear_str, fmsg)
                            self.state.last_bear_idx[symbol] = idx
                        else:
                            log.debug(f"SHORT {symbol} filtrado: {fmsg}")

        except Exception as e:
            log.debug(f"process_symbol {symbol}: {e}")

    def run_once(self):
        symbols = self.scanner.get_symbols()

        # Procesar en paralelo con límite de workers
        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = {ex.submit(self.process_symbol, s): s for s in symbols}
            for fut in as_completed(futures):
                try:
                    fut.result()
                except Exception as e:
                    sym = futures[fut]
                    log.debug(f"Error {sym}: {e}")

        save_state(self.state)
        log.info(f"Ciclo completo — {len(symbols)} símbolos | "
                 f"{len(self.positions)} posiciones abiertas")

    def start(self):
        log.info("=" * 60)
        log.info(f"MACD+TD MultiSymbol Bot arrancando")
        log.info(f"Top {TOP_N_SYMBOLS} pares | Max {MAX_OPEN_POSITIONS} posiciones")
        log.info(f"Modo: {'PAPER' if PAPER_MODE else 'REAL'}")
        log.info("=" * 60)
        cycle = 0
        while True:
            try:
                cycle += 1
                log.info(f"── Ciclo #{cycle} ──")
                self.run_once()
                # Heartbeat cada 30 ciclos
                if cycle % 30 == 0:
                    bal = get_balance()
                    tg(f"💤 <b>Heartbeat #{cycle}</b>\n"
                       f"Balance: ${bal:.2f} USDT\n"
                       f"Posiciones: {len(self.positions)}/{MAX_OPEN_POSITIONS}\n"
                       f"Símbolos escaneados: {len(self.scanner.get_symbols())}")
            except Exception as e:
                log.exception(f"Error ciclo: {e}")
                tg(f"⚠️ Error ciclo: {e}")
            time.sleep(CHECK_INTERVAL)


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    bot = MACDTDBot()
    bot.start()
