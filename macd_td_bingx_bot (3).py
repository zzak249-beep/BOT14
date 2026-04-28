"""
MACD Divergencia + TD Sequence Bot para BingX Perpetual Futures
Estrategia:
  1. Entrada en divergencia MACD (bullish/bearish)
  2. TD9 multi-timeframe gestiona adds/reduces de posición
  3. Trailing stop protege ganancias
  4. Notificaciones Telegram
"""

import os
import sys
import time
import json
import hmac
import hashlib
import logging
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass, field, asdict

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────
SYMBOL          = os.getenv("SYMBOL", "ETH-USDT")
API_KEY         = os.getenv("BINGX_API_KEY", "")
API_SECRET      = os.getenv("BINGX_SECRET_KEY", "")        # Railway: BINGX_SECRET_KEY
TG_TOKEN        = os.getenv("TELEGRAM_BOT_TOKEN", "")      # Railway: TELEGRAM_BOT_TOKEN
TG_CHAT_ID      = os.getenv("TELEGRAM_CHAT_ID", "")
BASE_URL        = "https://open-api.bingx.com"

# ORDER_SIZE=10 → tamaño de orden en USDT (Railway: ORDER_SIZE)
_order_size       = float(os.getenv("ORDER_SIZE", "10"))
MAX_POSITION_USDT = _order_size
MIN_POSITION_USDT = max(5.0, _order_size * 0.5)

# MODE=false → paper trading desactivado (órdenes reales)
PAPER_MODE      = os.getenv("MODE", "false").lower() == "true"

# Parámetros estrategia
RISK_PER_TRADE          = float(os.getenv("RISK_PCT", "0.02"))
MIN_DIV_STRENGTH        = float(os.getenv("MIN_DIV", "0.25"))
ATR_STOP_MULT           = float(os.getenv("ATR_STOP", "1.5"))
TRAILING_ATR            = float(os.getenv("TRAIL_ATR", "2.0"))
TRAILING_PCT            = float(os.getenv("TRAIL_PCT", "0.05"))
MIN_PROFIT_TRAIL        = float(os.getenv("MIN_PROFIT_TRAIL", "0.01"))
ENABLE_BUY_FILTER       = os.getenv("BUY_FILTER", "true").lower() == "true"
BUY_RSI_THRESHOLD       = float(os.getenv("RSI_THRESH", "40"))
BUY_VOL_RATIO           = float(os.getenv("VOL_RATIO", "0.8"))
ENABLE_30M_CLEAR        = os.getenv("CLEAR_30M", "true").lower() == "true"
MIN_BARS_PROTECTION     = int(os.getenv("MIN_BARS", "1"))
INITIAL_ADD_SIZE        = float(os.getenv("INITIAL_ADD", "0.3"))
MAX_LEVERAGE            = float(os.getenv("LEVERAGE", "10"))   # Railway: LEVERAGE
CHECK_INTERVAL          = int(os.getenv("CHECK_SEC", "60"))
KLINES_LIMIT            = int(os.getenv("KLINES_LIMIT", "500"))

# Mínimo de filas útiles después de calc_indicators
# EMA60 necesita 60 filas + 9 de warmup + margen = 80 mínimo
MIN_DF_ROWS = 80

# Ratios de reducción por nivel TD
TP_RATIOS = {"1m": 0.25, "3m": 0.20, "5m": 0.25}

STATE_FILE = "bot_state.json"

# ─────────────────────────────────────────────
# LOGGER
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("bot.log")]
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
        log.warning(f"Telegram error: {e}")

# ─────────────────────────────────────────────
# BINGX API
# ─────────────────────────────────────────────
def _sign(params: dict) -> str:
    query = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    return hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()

def _req(method: str, path: str, params: dict = None, data: dict = None) -> Optional[dict]:
    params = params or {}
    params["timestamp"] = int(time.time() * 1000)
    params["signature"] = _sign(params)
    headers = {"X-BX-APIKEY": API_KEY}
    url = BASE_URL + path
    try:
        if method == "GET":
            r = requests.get(url, params=params, headers=headers, timeout=10)
        else:
            r = requests.post(url, params=params, json=data, headers=headers, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.error(f"API {method} {path} error: {e}")
        return None

def get_balance() -> float:
    r = _req("GET", "/openApi/swap/v2/user/balance")
    if r and r.get("code") == 0:
        for asset in r["data"]["balance"]:
            if asset.get("asset") == "USDT":
                return float(asset.get("availableMargin", 0))
    log.error(f"Balance error: {r}")
    return 0.0

def get_mark_price() -> float:
    r = _req("GET", "/openApi/swap/v2/quote/price", {"symbol": SYMBOL})
    if r and r.get("code") == 0:
        return float(r["data"]["price"])
    return 0.0

def get_klines(interval: str, limit: int = 500) -> Optional[pd.DataFrame]:
    """Descarga klines y devuelve DataFrame con OHLCV, o None si hay error."""
    r = _req("GET", "/openApi/swap/v3/quote/klines",
             {"symbol": SYMBOL, "interval": interval, "limit": limit})
    if not r or r.get("code") != 0:
        log.error(f"Klines {interval} error: {r}")
        return None
    raw = r.get("data")
    if not raw or len(raw) == 0:
        log.error(f"Klines {interval}: respuesta vacía")
        return None

    # ── FIX: la API puede devolver lista de listas o lista de dicts ──
    try:
        if isinstance(raw[0], dict):
            df = pd.DataFrame(raw)
            # Renombrar columnas comunes de BingX
            col_map = {
                "t": "time", "o": "open", "h": "high",
                "l": "low", "c": "close", "v": "volume"
            }
            df.rename(columns=col_map, inplace=True)
            # Columnas alternativas también aceptadas
            if "openTime" in df.columns:
                df.rename(columns={"openTime": "time"}, inplace=True)
        else:
            # lista de listas: [time, open, high, low, close, volume, ...]
            df = pd.DataFrame(raw)
            # Puede venir con 7 u 8 columnas según versión de la API
            n = len(df.columns)
            base_cols = ["time", "open", "high", "low", "close", "volume"]
            extra = [f"_x{i}" for i in range(n - len(base_cols))]
            df.columns = base_cols + extra

        required = ["time", "open", "high", "low", "close", "volume"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            log.error(f"Klines {interval}: columnas faltantes {missing}. Cols disponibles: {list(df.columns)}")
            return None

        df = df[required].copy()
        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df["time"] = pd.to_datetime(
            pd.to_numeric(df["time"], errors="coerce"), unit="ms", utc=True
        )
        df.dropna(subset=["time", "close"], inplace=True)
        df.sort_values("time", inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    except Exception as e:
        log.error(f"Klines {interval} parse error: {e}")
        return None

def set_leverage(leverage: int):
    _req("POST", "/openApi/swap/v2/trade/leverage",
         {"symbol": SYMBOL, "side": "LONG", "leverage": leverage})
    _req("POST", "/openApi/swap/v2/trade/leverage",
         {"symbol": SYMBOL, "side": "SHORT", "leverage": leverage})

def place_order(side: str, usdt_size: float, reduce_only: bool = False) -> Optional[dict]:
    price = get_mark_price()
    if price <= 0:
        return None
    # PAPER MODE: simular orden sin enviar a BingX
    if PAPER_MODE:
        qty = round(usdt_size / price, 4)
        log.info(f"[PAPER] Orden simulada: {side} {qty} @ ~{price:.2f} (${usdt_size:.2f} USDT)")
        return {"qty": qty, "price": price, "usdt": qty * price}
    qty = round(usdt_size / price, 4)
    if qty <= 0:
        return None
    params = {
        "symbol": SYMBOL,
        "side": side,
        "positionSide": "LONG" if side == "BUY" else "SHORT",
        "type": "MARKET",
        "quantity": qty,
    }
    if reduce_only:
        params["reduceOnly"] = "true"
        params["positionSide"] = "LONG" if side == "SELL" else "SHORT"
    r = _req("POST", "/openApi/swap/v2/trade/order", params)
    if r and r.get("code") == 0:
        log.info(f"Order OK: {side} {qty} @ ~{price:.2f}")
        return {"qty": qty, "price": price, "usdt": qty * price}
    log.error(f"Order FAILED: {r}")
    return None

def close_position_all(pos_side: str) -> bool:
    r = _req("POST", "/openApi/swap/v2/trade/closeAllPositions",
             {"symbol": SYMBOL, "positionSide": pos_side})
    return r and r.get("code") == 0

def get_open_position() -> Optional[dict]:
    r = _req("GET", "/openApi/swap/v2/user/positions", {"symbol": SYMBOL})
    if r and r.get("code") == 0:
        for p in r.get("data", []):
            if float(p.get("positionAmt", 0)) != 0:
                return p
    return None

# ─────────────────────────────────────────────
# INDICADORES
# ─────────────────────────────────────────────
def calc_indicators(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Calcula indicadores usando pandas EWM/rolling.
    Sin funciones numpy propias para evitar problemas de longitud de arrays.
    """
    WARMUP = 40  # EWM converge rápido, 40 filas de margen es suficiente

    if df is None or len(df) < WARMUP + 20:
        log.warning(f"DataFrame entrada pequeño: {len(df) if df is not None else 0} filas")
        return None

    df = df.copy()
    df.dropna(subset=["open", "high", "low", "close", "volume"], inplace=True)
    df = df[df["close"] > 0].copy()
    df.reset_index(drop=True, inplace=True)

    if len(df) < WARMUP + 20:
        log.warning(f"Solo {len(df)} filas OHLCV limpias, insuficiente")
        return None

    c  = df["close"]
    h  = df["high"]
    lo = df["low"]
    v  = df["volume"]

    # MACD 12/26/9
    ema12 = c.ewm(span=12, adjust=False).mean()
    ema26 = c.ewm(span=26, adjust=False).mean()
    macd  = ema12 - ema26
    sig9  = macd.ewm(span=9, adjust=False).mean()

    # MACD rápido 8/17/6
    ema8      = c.ewm(span=8,  adjust=False).mean()
    ema17     = c.ewm(span=17, adjust=False).mean()
    macd_fast = ema8 - ema17
    sig6      = macd_fast.ewm(span=6, adjust=False).mean()

    # ATR 14
    prev_c = c.shift(1)
    tr     = pd.concat([(h - lo), (h - prev_c).abs(), (lo - prev_c).abs()], axis=1).max(axis=1)
    atr    = tr.rolling(14).mean()

    # EMA 20/60
    ema20 = c.ewm(span=20, adjust=False).mean()
    ema60 = c.ewm(span=60, adjust=False).mean()

    # RSI 14
    delta    = c.diff()
    avg_gain = delta.clip(lower=0).rolling(14).mean()
    avg_loss = (-delta).clip(lower=0).rolling(14).mean()
    rs       = avg_gain / avg_loss.replace(0, np.nan)
    rsi      = 100 - (100 / (1 + rs))

    # Vol ratio
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

    # Descartar filas de warmup y NaN residuales
    df = df.iloc[WARMUP:].copy()
    df.reset_index(drop=True, inplace=True)

    key_cols = ["macd", "macd_sig", "atr", "ema20", "ema60", "rsi"]
    nan_info = {col: int(df[col].isna().sum()) for col in key_cols}
    if any(n > 0 for n in nan_info.values()):
        log.warning(f"NaN residuales tras warmup: {nan_info}")

    before = len(df)
    df.dropna(subset=key_cols, inplace=True)
    df.reset_index(drop=True, inplace=True)

    log.info(f"calc_indicators OK: {before}→{len(df)} filas útiles")

    if len(df) < MIN_DF_ROWS:
        log.warning(f"Solo {len(df)} filas útiles, mínimo={MIN_DF_ROWS}")
        return None

    return df

def _ema(data: np.ndarray, period: int) -> np.ndarray:
    result = np.full(len(data), np.nan)
    if len(data) < period:
        return result
    k = 2 / (period + 1)
    result[period - 1] = np.mean(data[:period])
    for i in range(period, len(data)):
        result[i] = data[i] * k + result[i - 1] * (1 - k)
    return result

def _ema_of_nan(data: np.ndarray, period: int) -> np.ndarray:
    """EMA que ignora los NaN iniciales y arranca desde el primer valor válido."""
    result = np.full(len(data), np.nan)
    k = 2 / (period + 1)
    # Buscar primer índice no-NaN
    start = -1
    for i in range(len(data)):
        if not np.isnan(data[i]):
            start = i
            break
    if start < 0 or len(data) - start < period:
        return result
    result[start + period - 1] = np.nanmean(data[start:start + period])
    for i in range(start + period, len(data)):
        if not np.isnan(data[i]):
            result[i] = data[i] * k + result[i - 1] * (1 - k)
        else:
            result[i] = result[i - 1]
    return result

def _sma(data: np.ndarray, period: int) -> np.ndarray:
    result = np.full(len(data), np.nan)
    for i in range(period - 1, len(data)):
        result[i] = np.mean(data[i - period + 1:i + 1])
    return result

def _atr(high, low, close, period: int) -> np.ndarray:
    tr = np.maximum(high[1:] - low[1:],
         np.maximum(abs(high[1:] - close[:-1]),
                    abs(low[1:] - close[:-1])))
    tr = np.concatenate([[high[0] - low[0]], tr])
    return _sma(tr, period)

def _rsi(close: np.ndarray, period: int) -> np.ndarray:
    """RSI con output del mismo tamaño que el input."""
    result = np.full(len(close), np.nan)
    if len(close) < period + 1:
        return result
    delta    = np.diff(close)                              # len N-1
    gain     = np.where(delta > 0, delta, 0.0)
    loss     = np.where(delta < 0, -delta, 0.0)
    # Calcular SMA(period) sobre gain/loss — arrays de len N-1
    # El primer valor de RSI corresponde al índice `period` del close original
    for i in range(period - 1, len(delta)):
        avg_g = np.mean(gain[i - period + 1:i + 1])
        avg_l = np.mean(loss[i - period + 1:i + 1])
        if avg_l == 0:
            result[i + 1] = 100.0
        else:
            rs = avg_g / avg_l
            result[i + 1] = 100.0 - (100.0 / (1.0 + rs))
    return result

# ─────────────────────────────────────────────
# DIVERGENCIA MACD
# ─────────────────────────────────────────────
def find_extremes(data: np.ndarray, window: int = 3) -> Tuple[List, List]:
    peaks, troughs = [], []
    for i in range(window, len(data) - window):
        if all(data[i] > data[i-j] and data[i] > data[i+j] for j in range(1, window+1)):
            peaks.append((i, data[i]))
        if all(data[i] < data[i-j] and data[i] < data[i+j] for j in range(1, window+1)):
            troughs.append((i, data[i]))
    return peaks, troughs

def bullish_divergence(price_tr, macd_tr, fast_tr=None) -> Tuple[bool, float, Optional[dict]]:
    if len(price_tr) < 2 or len(macd_tr) < 2:
        return False, 0, None
    p1, p2 = price_tr[-2], price_tr[-1]
    m1, m2 = macd_tr[-2], macd_tr[-1]
    if p2[1] < p1[1] and m2[1] > m1[1]:
        strength = min(1.0,
            abs((p2[1]-p1[1])/p1[1]) * 50 +
            ((m2[1]-m1[1])/abs(m1[1]) if m1[1] != 0 else 1) * 50)
        return True, strength, {"price_low": p2[1], "index": p2[0], "strength": strength}
    if fast_tr and len(fast_tr) >= 2:
        f1, f2 = fast_tr[-2], fast_tr[-1]
        if p2[1] < p1[1] and f2[1] > f1[1]:
            strength = min(0.8,
                abs((p2[1]-p1[1])/p1[1]) * 30 +
                ((f2[1]-f1[1])/abs(f1[1]) if f1[1] != 0 else 1) * 30)
            return True, strength, {"price_low": p2[1], "index": p2[0], "strength": strength, "fast": True}
    return False, 0, None

def bearish_divergence(price_pk, macd_pk, fast_pk=None) -> Tuple[bool, float, Optional[dict]]:
    if len(price_pk) < 2 or len(macd_pk) < 2:
        return False, 0, None
    p1, p2 = price_pk[-2], price_pk[-1]
    m1, m2 = macd_pk[-2], macd_pk[-1]
    if p2[1] > p1[1] and m2[1] < m1[1]:
        strength = min(1.0,
            abs((p2[1]-p1[1])/p1[1]) * 50 +
            abs((m2[1]-m1[1])/abs(m1[1]) if m1[1] != 0 else 1) * 50)
        return True, strength, {"price_high": p2[1], "index": p2[0], "strength": strength}
    if fast_pk and len(fast_pk) >= 2:
        f1, f2 = fast_pk[-2], fast_pk[-1]
        if p2[1] > p1[1] and f2[1] < f1[1]:
            strength = min(0.8,
                abs((p2[1]-p1[1])/p1[1]) * 30 +
                abs((f2[1]-f1[1])/abs(f1[1]) if f1[1] != 0 else 1) * 30)
            return True, strength, {"price_high": p2[1], "index": p2[0], "strength": strength, "fast": True}
    return False, 0, None

# ─────────────────────────────────────────────
# TD SETUP
# ─────────────────────────────────────────────
def td_setup(df: pd.DataFrame, period: int = 9) -> int:
    closes = df["close"].values
    if len(closes) < period + 4:
        return 0
    buy  = all(closes[-i] <= closes[-i-4] for i in range(1, period+1))
    sell = all(closes[-i] >= closes[-i-4] for i in range(1, period+1))
    return 1 if buy else (-1 if sell else 0)

def get_td_signals(dfs: dict) -> dict:
    return {tf: td_setup(df) for tf, df in dfs.items()}

# ─────────────────────────────────────────────
# ESTADO
# ─────────────────────────────────────────────
@dataclass
class Position:
    side: str
    entry_price: float
    entry_time: str
    size_usdt: float
    remain_usdt: float
    stop_loss: float
    highest: float
    lowest: float
    tp_done: List[str] = field(default_factory=list)
    initial_added: bool = False
    entry_bar: int = 0

@dataclass
class BotState:
    balance: float = 0.0
    position: Optional[dict] = None
    last_bull_idx: int = -1
    last_bear_idx: int = -1
    trades: List[dict] = field(default_factory=list)

def load_state() -> BotState:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                d = json.load(f)
            return BotState(**d)
        except Exception as e:
            log.warning(f"State load error: {e}")
    return BotState()

def save_state(st: BotState):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(asdict(st), f, indent=2, default=str)
    except Exception as e:
        log.error(f"State save error: {e}")

# ─────────────────────────────────────────────
# LÓGICA PRINCIPAL
# ─────────────────────────────────────────────
class MACDTDBot:
    def __init__(self):
        self.state = load_state()
        self.pos: Optional[Position] = None
        if self.state.position:
            self.pos = Position(**self.state.position)
        log.info(f"Bot iniciado. Posición: {self.pos}")
        mode_str = "📋 PAPER" if PAPER_MODE else "💰 REAL"
        tg(f"🤖 <b>MACD+TD Bot iniciado</b>\nSímbolo: {SYMBOL}\nRiesgo: {RISK_PER_TRADE*100:.0f}%\nModo: {mode_str}\nTamaño orden: ${MAX_POSITION_USDT} USDT\nApalancamiento: {int(MAX_LEVERAGE)}x")
        set_leverage(int(MAX_LEVERAGE))

    # ── datos ──────────────────────────────────
    def fetch_all(self) -> Optional[dict]:
        # Necesitamos al menos WARMUP(70) + MIN_DF_ROWS(80) = 150 velas crudas
        MIN_RAW_ROWS = 150
        dfs = {}
        for tf in ["1m", "3m", "5m", "15m", "30m"]:
            raw = get_klines(tf, KLINES_LIMIT)
            if raw is None:
                log.error(f"Sin datos crudos para {tf}")
                return None
            log.info(f"Klines {tf}: {len(raw)} velas crudas recibidas")
            if len(raw) < MIN_RAW_ROWS:
                log.error(f"Velas insuficientes para {tf}: {len(raw)} < {MIN_RAW_ROWS}")
                return None
            df = calc_indicators(raw)
            if df is None:
                log.error(f"calc_indicators devolvió None para {tf}")
                return None
            log.info(f"Klines {tf}: {len(df)} filas útiles tras indicadores")
            dfs[tf] = df
        return dfs

    # ── filtro compra ───────────────────────────
    def buy_filter_ok(self, df15: pd.DataFrame) -> Tuple[bool, str]:
        """
        Filtro de entrada LONG: necesita 2 de 4 condiciones.
        Lógica: en divergencia alcista el precio YA está bajo,
        no exigir close<EMA60 como condición obligatoria.
        """
        if not ENABLE_BUY_FILTER:
            return True, "sin filtro"
        if df15 is None or len(df15) == 0:
            return False, "❌ DataFrame vacío en filtro"
        row = df15.iloc[-1]
        conds = []
        # RSI no sobrecomprado (< 65 es suficiente para long)
        if row["rsi"] < 65:
            conds.append(f"RSI={row['rsi']:.1f}")
        # MACD histograma subiendo (momentum positivo)
        if len(df15) >= 2 and df15["macd_hist"].iloc[-1] > df15["macd_hist"].iloc[-2]:
            conds.append("MACD↑")
        # Volumen por encima de media
        if row["vol_ratio"] > BUY_VOL_RATIO:
            conds.append(f"vol×{row['vol_ratio']:.2f}")
        # No en tendencia bajista fuerte (precio no muy lejos de EMA20)
        dist_ema20 = (row["close"] - row["ema20"]) / row["ema20"]
        if dist_ema20 > -0.05:  # no más del 5% bajo EMA20
            conds.append(f"dist_EMA20={dist_ema20*100:.1f}%")
        ok = len(conds) >= 2
        return ok, ("✅ " if ok else "❌ ") + ", ".join(conds) if conds else "❌ sin condiciones"

    def sell_filter_ok(self, df15: pd.DataFrame) -> Tuple[bool, str]:
        """Filtro de entrada SHORT: simétrico al de long."""
        if not ENABLE_BUY_FILTER:
            return True, "sin filtro"
        if df15 is None or len(df15) == 0:
            return False, "❌ DataFrame vacío en filtro"
        row = df15.iloc[-1]
        conds = []
        if row["rsi"] > 35:
            conds.append(f"RSI={row['rsi']:.1f}")
        if len(df15) >= 2 and df15["macd_hist"].iloc[-1] < df15["macd_hist"].iloc[-2]:
            conds.append("MACD↓")
        if row["vol_ratio"] > BUY_VOL_RATIO:
            conds.append(f"vol×{row['vol_ratio']:.2f}")
        dist_ema20 = (row["close"] - row["ema20"]) / row["ema20"]
        if dist_ema20 < 0.05:
            conds.append(f"dist_EMA20={dist_ema20*100:.1f}%")
        ok = len(conds) >= 2
        return ok, ("✅ " if ok else "❌ ") + ", ".join(conds) if conds else "❌ sin condiciones"

    # ── tamaño posición ─────────────────────────
    def calc_size(self, price: float, stop: float, strength: float) -> float:
        bal = get_balance()
        if bal <= 0:
            return 0.0
        self.state.balance = bal
        risk = bal * RISK_PER_TRADE
        dist = abs(price - stop)
        if dist <= 0:
            dist = price * 0.01
        mult = 0.5 + strength
        size = risk * mult / (dist / price)
        size = min(size, MAX_POSITION_USDT, bal * MAX_LEVERAGE)
        size = max(size, MIN_POSITION_USDT)
        return round(size, 2)

    # ── trailing stop ───────────────────────────
    def update_trailing(self, price: float, atr: float) -> bool:
        if not self.pos:
            return False
        profit_pct = ((price - self.pos.entry_price) / self.pos.entry_price
                      if self.pos.side == "long"
                      else (self.pos.entry_price - price) / self.pos.entry_price)
        if profit_pct < MIN_PROFIT_TRAIL:
            return False
        updated = False
        if self.pos.side == "long":
            if price > self.pos.highest:
                self.pos.highest = price
            new_sl = max(self.pos.highest - atr * TRAILING_ATR,
                         self.pos.highest * (1 - TRAILING_PCT))
            if new_sl > self.pos.stop_loss:
                self.pos.stop_loss = new_sl
                updated = True
        else:
            if price < self.pos.lowest:
                self.pos.lowest = price
            new_sl = min(self.pos.lowest + atr * TRAILING_ATR,
                         self.pos.lowest * (1 + TRAILING_PCT))
            if new_sl < self.pos.stop_loss:
                self.pos.stop_loss = new_sl
                updated = True
        return updated

    # ── abrir posición ──────────────────────────
    def open_long(self, price: float, stop: float, strength: float, reason: str):
        size = self.calc_size(price, stop, strength)
        if size <= 0:
            return
        res = place_order("BUY", size)
        if not res:
            tg("❌ Error abriendo LONG")
            return
        self.pos = Position(
            side="long", entry_price=res["price"],
            entry_time=str(datetime.now(timezone.utc)),
            size_usdt=res["usdt"], remain_usdt=res["usdt"],
            stop_loss=stop, highest=res["price"], lowest=res["price"],
            entry_bar=0
        )
        self._sync_entry_bar()
        self.state.position = asdict(self.pos)
        save_state(self.state)
        msg = (f"📈 <b>LONG abierto</b>\n"
               f"Precio: ${res['price']:.2f}\n"
               f"Tamaño: ${res['usdt']:.2f} USDT\n"
               f"Stop: ${stop:.2f}\n"
               f"Fuerza div.: {strength:.2f}\n"
               f"Razón: {reason}")
        log.info(msg); tg(msg)

    def open_short(self, price: float, stop: float, strength: float, reason: str = ""):
        size = self.calc_size(price, stop, strength)
        if size <= 0:
            return
        res = place_order("SELL", size)
        if not res:
            tg("❌ Error abriendo SHORT")
            return
        self.pos = Position(
            side="short", entry_price=res["price"],
            entry_time=str(datetime.now(timezone.utc)),
            size_usdt=res["usdt"], remain_usdt=res["usdt"],
            stop_loss=stop, highest=res["price"], lowest=res["price"],
            entry_bar=0
        )
        self._sync_entry_bar()
        self.state.position = asdict(self.pos)
        save_state(self.state)
        msg = (f"📉 <b>SHORT abierto</b>\n"
               f"Precio: ${res['price']:.2f}\n"
               f"Tamaño: ${res['usdt']:.2f} USDT\n"
               f"Stop: ${stop:.2f}\n"
               f"Fuerza div.: {strength:.2f}\n"
               f"Razón: {reason}")
        log.info(msg); tg(msg)

    def _sync_entry_bar(self):
        self._entry_bar_count = 0

    # ── cerrar parcial ──────────────────────────
    def close_partial(self, price: float, pct: float, level: str):
        if not self.pos:
            return
        close_usdt = self.pos.size_usdt * pct
        if close_usdt < MIN_POSITION_USDT or close_usdt > self.pos.remain_usdt:
            return
        side = "SELL" if self.pos.side == "long" else "BUY"
        res = place_order(side, close_usdt, reduce_only=True)
        if not res:
            return
        pnl = ((price - self.pos.entry_price) if self.pos.side == "long"
               else (self.pos.entry_price - price)) * res["qty"]
        self.pos.remain_usdt -= close_usdt
        self.pos.tp_done.append(level)
        self.state.position = asdict(self.pos)
        save_state(self.state)
        msg = (f"🎯 Reducción <b>{level}</b> ({pct*100:.0f}%)\n"
               f"Precio: ${price:.2f}\n"
               f"PnL: ${pnl:+.2f}\n"
               f"Resto: ${self.pos.remain_usdt:.2f} USDT")
        log.info(msg); tg(msg)

    # ── cerrar total ────────────────────────────
    def close_all(self, price: float, reason: str):
        if not self.pos:
            return
        close_position_all("LONG" if self.pos.side == "long" else "SHORT")
        pnl = ((price - self.pos.entry_price) if self.pos.side == "long"
               else (self.pos.entry_price - price)) * (self.pos.remain_usdt / self.pos.entry_price)
        self.state.trades.append({
            "side": self.pos.side, "entry": self.pos.entry_price,
            "exit": price, "pnl": round(pnl, 4), "reason": reason,
            "time": str(datetime.now(timezone.utc))
        })
        self.pos = None
        self.state.position = None
        save_state(self.state)
        emoji = "🟢" if pnl > 0 else "🔴"
        msg = (f"{emoji} <b>CERRADO</b> ({reason})\n"
               f"Precio: ${price:.2f}\n"
               f"PnL: ${pnl:+.2f}")
        log.info(msg); tg(msg)

    # ── ciclo principal ─────────────────────────
    def run_once(self):
        dfs = self.fetch_all()
        if not dfs:
            log.warning("fetch_all sin datos, saltando ciclo")
            return

        df15 = dfs.get("15m")
        if df15 is None or len(df15) == 0:
            log.warning("df15 vacío, saltando ciclo")
            return

        price = get_mark_price()
        if price <= 0:
            log.warning("Precio inválido, saltando ciclo")
            return

        # ── ATR con fallback seguro ─────────────
        atr_series = df15["atr"].dropna()
        if len(atr_series) == 0:
            log.warning("ATR vacío, saltando ciclo")
            return
        atr = float(atr_series.iloc[-1])
        if atr <= 0:
            log.warning(f"ATR inválido ({atr}), saltando ciclo")
            return

        td = get_td_signals({tf: dfs[tf] for tf in ["1m", "3m", "5m", "15m", "30m"]})

        # ── sin posición: buscar entrada ────────
        if self.pos is None:
            # Heartbeat cada 60 ciclos cuando no hay posición
            self._idle_count = getattr(self, "_idle_count", 0) + 1
            if self._idle_count >= 60:
                self._idle_count = 0
                bal = get_balance()
                tg(f"💤 <b>Bot activo — sin posición</b>\n"
                   f"Par: {SYMBOL} | Precio: ${price:.2f}\n"
                   f"Balance: ${bal:.2f} USDT\n"
                   f"Buscando divergencias...")

            window = df15.tail(200)
            if len(window) < 20:
                log.warning("Ventana de divergencia demasiado pequeña")
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
                if idx != self.state.last_bull_idx:
                    ok, fmsg = self.buy_filter_ok(df15)
                    if ok:
                        stop = bull_info["price_low"] - atr * ATR_STOP_MULT
                        self.open_long(price, stop, bull_str, fmsg)
                        self.state.last_bull_idx = idx
                    else:
                        log.info(f"Divergencia alcista filtrada: {fmsg}")
                        tg(f"👀 <b>Señal LONG filtrada</b>\n"
                           f"Par: {SYMBOL}\n"
                           f"Precio: ${price:.2f}\n"
                           f"Fuerza div.: {bull_str:.2f}\n"
                           f"Filtro: {fmsg}")

            elif bear and bear_str >= MIN_DIV_STRENGTH:
                idx = bear_info["index"]
                if idx != self.state.last_bear_idx:
                    ok_s, fmsg_s = self.sell_filter_ok(df15)
                    if ok_s:
                        stop = bear_info["price_high"] + atr * ATR_STOP_MULT
                        self.open_short(price, stop, bear_str, fmsg_s)
                        self.state.last_bear_idx = idx
                    else:
                        log.info(f"Divergencia bajista filtrada: {fmsg_s}")
                        tg(f"👀 <b>Señal SHORT filtrada</b>\n"
                           f"Par: {SYMBOL}\n"
                           f"Precio: ${price:.2f}\n"
                           f"Fuerza div.: {bear_str:.2f}\n"
                           f"Filtro: {fmsg_s}")

        # ── con posición: gestionar ─────────────
        else:
            self._entry_bar_count = getattr(self, "_entry_bar_count", 0) + 1
            in_protection = self._entry_bar_count < MIN_BARS_PROTECTION

            in_profit = ((price > self.pos.entry_price) if self.pos.side == "long"
                         else (price < self.pos.entry_price))

            if in_profit and not in_protection:
                upd = self.update_trailing(price, atr)
                if upd:
                    log.info(f"Trailing stop → ${self.pos.stop_loss:.2f}")
                    # Notificar solo cada 10 actualizaciones para no spamear
                    self._trail_notif = getattr(self, "_trail_notif", 0) + 1
                    if self._trail_notif % 10 == 1:
                        profit_pct = abs(price - self.pos.entry_price) / self.pos.entry_price * 100
                        tg(f"🛡 <b>Trailing stop actualizado</b>\n"
                           f"Nuevo SL: ${self.pos.stop_loss:.2f}\n"
                           f"Precio: ${price:.2f} (+{profit_pct:.1f}%)")

            sl_hit = ((price <= self.pos.stop_loss) if self.pos.side == "long"
                      else (price >= self.pos.stop_loss))
            if sl_hit:
                self.close_all(price, "Stop Loss")
                return

            if not in_protection:
                if in_profit:
                    for level, pct in TP_RATIOS.items():
                        if level in self.pos.tp_done:
                            continue
                        td_val = td.get(level, 0)
                        trigger = (td_val == -1 if self.pos.side == "long" else td_val == 1)
                        if trigger:
                            self.close_partial(price, pct, level)

                if in_profit and not self.pos.initial_added:
                    td15 = td.get("15m", 0)
                    add_ok = (td15 == 1 if self.pos.side == "long" else td15 == -1)
                    if add_ok:
                        add_usdt = self.pos.size_usdt * INITIAL_ADD_SIZE
                        if add_usdt >= MIN_POSITION_USDT:
                            side_add = "BUY" if self.pos.side == "long" else "SELL"
                            res = place_order(side_add, add_usdt)
                            if res:
                                self.pos.remain_usdt += res["usdt"]
                                self.pos.initial_added = True
                                self.state.position = asdict(self.pos)
                                save_state(self.state)
                                msg = (f"➕ <b>Add inicial</b>\n"
                                       f"USD añadido: ${res['usdt']:.2f}\n"
                                       f"Trigger: 15m TD9\n"
                                       f"Total: ${self.pos.remain_usdt:.2f} USDT")
                                log.info(msg); tg(msg)

                if self.pos.tp_done and in_profit:
                    td3, td5 = td.get("3m", 0), td.get("5m", 0)
                    readd_tf = None
                    if self.pos.side == "long":
                        if td3 == 1: readd_tf = "3m"
                        elif td5 == 1: readd_tf = "5m"
                    else:
                        if td3 == -1: readd_tf = "3m"
                        elif td5 == -1: readd_tf = "5m"
                    if readd_tf:
                        closed_pct = sum(TP_RATIOS.get(s, 0) for s in self.pos.tp_done)
                        target = self.pos.size_usdt * (1 - closed_pct)
                        if self.pos.remain_usdt < target:
                            add_usdt = min(target - self.pos.remain_usdt, self.pos.size_usdt * 0.3)
                            if add_usdt >= MIN_POSITION_USDT:
                                side_add = "BUY" if self.pos.side == "long" else "SELL"
                                res = place_order(side_add, add_usdt)
                                if res:
                                    self.pos.remain_usdt += res["usdt"]
                                    self.pos.tp_done = []
                                    self.state.position = asdict(self.pos)
                                    save_state(self.state)
                                    msg = (f"🔄 <b>Re-add tras reducción</b> ({readd_tf})\n"
                                           f"USD añadido: ${res['usdt']:.2f}")
                                    log.info(msg); tg(msg)

                td15, td30 = td.get("15m", 0), td.get("30m", 0)
                clear, clear_src = False, ""
                if self.pos.side == "long":
                    if td15 == -1: clear, clear_src = True, "15m"
                    elif ENABLE_30M_CLEAR and td30 == -1: clear, clear_src = True, "30m"
                else:
                    if td15 == 1: clear, clear_src = True, "15m"
                    elif ENABLE_30M_CLEAR and td30 == 1: clear, clear_src = True, "30m"
                if clear:
                    self.close_all(price, f"TD9 {clear_src}")
                    return

            pnl_unreal = ((price - self.pos.entry_price) if self.pos.side == "long"
                          else (self.pos.entry_price - price))
            pnl_usdt = pnl_unreal / self.pos.entry_price * self.pos.remain_usdt * MAX_LEVERAGE
            log.info(f"Posición: {self.pos.side.upper()} | Precio: ${price:.2f} | "
                     f"Entrada: ${self.pos.entry_price:.2f} | PnL≈${pnl_usdt:+.2f} | "
                     f"SL: ${self.pos.stop_loss:.2f} | TD: {td}")
            # Heartbeat Telegram cada ~60 ciclos (~1h con CHECK_INTERVAL=60s)
            self._hb_count = getattr(self, "_hb_count", 0) + 1
            if self._hb_count >= 60:
                self._hb_count = 0
                emoji = "📈" if self.pos.side == "long" else "📉"
                tg(f"{emoji} <b>Estado posición</b>\n"
                   f"Par: {SYMBOL} | {self.pos.side.upper()}\n"
                   f"Entrada: ${self.pos.entry_price:.2f}\n"
                   f"Precio actual: ${price:.2f}\n"
                   f"PnL≈: ${pnl_usdt:+.2f} USDT\n"
                   f"SL: ${self.pos.stop_loss:.2f}\n"
                   f"Restante: ${self.pos.remain_usdt:.2f} USDT")

        save_state(self.state)

    def start(self):
        log.info("=" * 60)
        log.info(f"BOT MACD+TD arrancando — {SYMBOL}")
        log.info(f"Riesgo: {RISK_PER_TRADE*100:.0f}% | Trailing: {TRAILING_PCT*100:.0f}%/{TRAILING_ATR}×ATR")
        log.info(f"Intervalo: {CHECK_INTERVAL}s")
        log.info("=" * 60)
        while True:
            try:
                self.run_once()
            except Exception as e:
                log.exception(f"Error en ciclo: {e}")
                tg(f"⚠️ Error ciclo: {e}")
            time.sleep(CHECK_INTERVAL)


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    bot = MACDTDBot()
    bot.start()
