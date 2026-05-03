"""
╔══════════════════════════════════════════════════════════════════════╗
║   PHANTOM EDGE BOT v7.0 — ZigZag + HMA + Future-Trend              ║
║   FIXES vs v6:                                                       ║
║   · Eliminado httpx → 100% aiohttp (ya instalado)                   ║
║   · place_order corregido: quoteOrderQty + SL/TP embebidos          ║
║     (mismo formato que client.py probado)                            ║
║   · Sin positionSide (modo ONE-WAY, compatible BingX por defecto)   ║
║   · AUTO_TRADING_ENABLED se loguea claramente al arrancar            ║
║   · Señales: crossover relaxado + score mínimo 3/6                  ║
║   · Warmup más rápido: 50 concurrentes en prio                      ║
╚══════════════════════════════════════════════════════════════════════╝
"""
from __future__ import annotations
import asyncio, hashlib, hmac, logging, math, os, signal, time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlencode

import aiohttp
import numpy as np

# ─────────────────────────────────────────────────────────────────────
#  CONFIGURACIÓN — todas vía env vars en Railway
# ─────────────────────────────────────────────────────────────────────
API_KEY         = os.getenv("BINGX_API_KEY", "")
API_SECRET      = os.getenv("BINGX_SECRET_KEY", os.getenv("BINGX_API_SECRET", ""))
TELEGRAM_TOKEN  = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT   = os.getenv("TELEGRAM_CHAT_ID", "")

AUTO_TRADING    = os.getenv("AUTO_TRADING_ENABLED", "false").lower() == "true"
LEVERAGE        = int(os.getenv("LEVERAGE", "10"))
TIMEFRAME       = os.getenv("TIMEFRAME", "5m")
TIMEFRAME_SLOW  = os.getenv("TIMEFRAME_SLOW", "15m")

# Parámetros del indicador (idénticos al Pine Script original)
PIVOT_LEN   = int(os.getenv("PIVOT_LEN",   "5"))    # ZigZag lookback
HMA_LEN     = int(os.getenv("HMA_LEN",    "50"))    # Hull MA length
FT_PERIOD   = int(os.getenv("FT_PERIOD",  "25"))    # Future-Trend period

# TP/SL dinámico (ATR) — mejora vs pips fijos del Pine
USE_ATR_TP  = os.getenv("USE_ATR_TP", "true").lower() == "true"
ATR_SL      = float(os.getenv("ATR_SL",  "1.5"))
ATR_TP1     = float(os.getenv("ATR_TP1", "1.5"))
ATR_TP2     = float(os.getenv("ATR_TP2", "3.0"))
TP_PIPS     = float(os.getenv("TP_PIPS", "45.0"))
SL_PIPS     = float(os.getenv("SL_PIPS", "30.0"))

MIN_SCORE       = int(os.getenv("MIN_SCORE", "3"))     # mín: ZZ+HMA+FT (=señal Pine)
MIN_ATR_PCT     = float(os.getenv("MIN_ATR_PCT", "0.08"))
MIN_VOL_MULT    = float(os.getenv("MIN_VOL_MULT", "0.5"))

TRADE_USDT      = float(os.getenv("TRADE_USDT", "9"))
MAX_POSITIONS   = int(os.getenv("MAX_POSITIONS", "5"))
SCAN_INTERVAL   = int(os.getenv("SCAN_INTERVAL", "30"))
MAX_CONCURRENT  = int(os.getenv("MAX_CONCURRENT", "40"))
MAX_DAILY_LOSS  = float(os.getenv("MAX_DAILY_LOSS", "5.0"))
PORT            = int(os.getenv("PORT", "8080"))
TP1_FRAC        = float(os.getenv("TP1_SIZE", "0.35"))  # fracción qty en TP1
TP2_FRAC        = float(os.getenv("TP2_SIZE", "0.35"))  # fracción qty en TP2

PRIORITY_RAW = os.getenv("PRIORITY_SYMBOLS",
    "BTC-USDT,ETH-USDT,SOL-USDT,XRP-USDT,DOGE-USDT,"
    "PNUT-USDT,PEPE-USDT,WIF-USDT,BONK-USDT,FLOKI-USDT,"
    "TURBO-USDT,HYPE-USDT,ARB-USDT,SHIB-USDT,AVAX-USDT,"
    "LINK-USDT,BNB-USDT,ADA-USDT,MATIC-USDT,SUI-USDT"
)
PRIORITY = [s.strip() for s in PRIORITY_RAW.split(",") if s.strip()]

BINGX_BASE = "https://open-api.bingx.com"
MIN_BALANCE = max(TRADE_USDT * 1.5, 5.0)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("PE7")


# ══════════════════════════════════════════════════════════════════════
#  INDICADORES — traducción Pine → Python
# ══════════════════════════════════════════════════════════════════════

def _f(a) -> np.ndarray:
    return np.nan_to_num(np.asarray(a, dtype=float), nan=0.0, posinf=0.0, neginf=0.0)


# ── ZigZag: ta.pivothigh / ta.pivotlow ──────────────────────────────

def _pivot_highs(h: np.ndarray, n: int) -> np.ndarray:
    """ta.pivothigh(high, n, n) — pivot confirmado n velas a la derecha."""
    result = np.full(len(h), np.nan)
    for i in range(n, len(h) - n):
        window = h[i - n: i + n + 1]
        if h[i] >= np.max(window):
            result[i] = h[i]
    return result


def _pivot_lows(l: np.ndarray, n: int) -> np.ndarray:
    """ta.pivotlow(low, n, n)"""
    result = np.full(len(l), np.nan)
    for i in range(n, len(l) - n):
        window = l[i - n: i + n + 1]
        if l[i] <= np.min(window):
            result[i] = l[i]
    return result


def peak_series(h: np.ndarray, n: int) -> np.ndarray:
    """var float peak = na; if not na(ph): peak := ph — forward-fill."""
    ph = _pivot_highs(h, n)
    out = np.full(len(h), np.nan)
    cur = np.nan
    for i in range(len(h)):
        if not np.isnan(ph[i]):
            cur = ph[i]
        out[i] = cur
    return out


def valley_series(l: np.ndarray, n: int) -> np.ndarray:
    """var float valley = na; forward-fill."""
    pl = _pivot_lows(l, n)
    out = np.full(len(l), np.nan)
    cur = np.nan
    for i in range(len(l)):
        if not np.isnan(pl[i]):
            cur = pl[i]
        out[i] = cur
    return out


def crossover(series: np.ndarray, level: np.ndarray) -> bool:
    """ta.crossover: prev ≤ level AND curr > level."""
    if len(series) < 2 or np.isnan(level[-1]):
        return False
    return bool(series[-2] <= level[-2] and series[-1] > level[-1])


def crossunder(series: np.ndarray, level: np.ndarray) -> bool:
    """ta.crossunder: prev ≥ level AND curr < level."""
    if len(series) < 2 or np.isnan(level[-1]):
        return False
    return bool(series[-2] >= level[-2] and series[-1] < level[-1])


# ── HMA: ta.hma(close, len) ─────────────────────────────────────────

def calc_hma(c: np.ndarray, n: int) -> np.ndarray:
    """HMA = WMA(2×WMA(n/2) − WMA(n), sqrt(n))"""
    c = _f(c)
    if len(c) < n:
        return np.full(len(c), c[-1] if len(c) else 0.0)

    def wma(arr, period):
        w = np.arange(1, period + 1, dtype=float)
        total = w.sum()
        out = np.zeros(len(arr))
        for i in range(period - 1, len(arr)):
            out[i] = np.dot(arr[i - period + 1: i + 1], w) / total
        return out

    half_n = max(1, n // 2)
    sqrt_n = max(1, int(math.sqrt(n)))
    raw = 2.0 * wma(c, half_n) - wma(c, n)
    return wma(raw, sqrt_n)


def hma_direction(hma_vals: np.ndarray, close_vals: np.ndarray):
    """Pine: hma_alcista = close > hma and hma > hma[1]"""
    if len(hma_vals) < 2:
        return False, False
    h0, h1, cl = hma_vals[-1], hma_vals[-2], close_vals[-1]
    return bool(cl > h0 and h0 > h1), bool(cl < h0 and h0 < h1)


# ── Future-Trend: Volume Delta × 3 períodos ─────────────────────────

def calc_future_trend(o: np.ndarray, c: np.ndarray, v: np.ndarray,
                       ft_period: int) -> float:
    """
    Pine Script original:
      delta_vol = close>open ? volume : close<open ? -volume : 0
      for i=0 to ft_period-1
          avg_delta = math.avg(delta_vol[i], delta_vol[i+ft], delta_vol[i+ft*2])
          vol_delta_sum += avg_delta
      vol_delta_avg = vol_delta_sum / ft_period
    """
    o, c, v = _f(o), _f(c), _f(v)
    n = len(c)
    if n < ft_period * 3 + 1:
        return 0.0

    delta = np.where(c > o, v, np.where(c < o, -v, 0.0))

    total = 0.0
    for i in range(ft_period):
        i0 = n - 1 - i
        i1 = n - 1 - i - ft_period
        i2 = n - 1 - i - ft_period * 2
        if i2 >= 0:
            total += (delta[i0] + delta[i1] + delta[i2]) / 3.0

    return total / ft_period


# ── ATR (Wilder) ────────────────────────────────────────────────────

def calc_atr(h, l, c, p=14) -> float:
    h, l, c = _f(h), _f(l), _f(c)
    if len(c) < p + 1:
        return max(float(np.mean(h - l)), 1e-12)
    tr = np.maximum(h[1:] - l[1:],
                    np.maximum(np.abs(h[1:] - c[:-1]),
                               np.abs(l[1:] - c[:-1])))
    tr = np.r_[h[0] - l[0], tr]
    a = np.zeros(len(tr))
    a[p - 1] = np.mean(tr[:p])
    for i in range(p, len(tr)):
        a[i] = (a[i - 1] * (p - 1) + tr[i]) / p
    return max(float(a[-1]), 1e-12)


def _pip_size(price: float) -> float:
    """Tamaño de pip para crypto (equivale a syminfo.mintick en Pine)."""
    if price > 10000: return 0.01
    if price > 100:   return 0.001
    if price > 1:     return 0.0001
    if price > 0.01:  return 0.00001
    return 0.000001


# ══════════════════════════════════════════════════════════════════════
#  MOTOR DE SEÑALES v7
# ══════════════════════════════════════════════════════════════════════

def analyze(c5: list[dict], c15: list[dict]) -> Optional[dict]:
    """
    LONG:  crossover(close, peak)   + HMA alcista + FutureTrend > 0
    SHORT: crossunder(close, valley) + HMA bajista + FutureTrend < 0

    Score 0-6:
      +2 Ruptura ZigZag (condición principal Pine)
      +1 HMA dirección (filtro rápido Pine)
      +1 FutureTrend volume delta (Pine)
      +1 Confirmación 15m (multi-TF mejora)
      +1 Volumen elevado (mejora)
    """
    need_5m  = max(HMA_LEN + 10, FT_PERIOD * 3 + 10, PIVOT_LEN * 4 + 5)
    need_15m = max(HMA_LEN + 5,  FT_PERIOD * 2 + 5)

    if len(c5) < need_5m or len(c15) < need_15m:
        return None

    # Arrays
    h5  = _f([x["h"] for x in c5])
    l5  = _f([x["l"] for x in c5])
    c5a = _f([x["c"] for x in c5])
    o5  = _f([x["o"] for x in c5])
    v5  = _f([x["v"] for x in c5])

    h15  = _f([x["h"] for x in c15])
    l15  = _f([x["l"] for x in c15])
    c15a = _f([x["c"] for x in c15])
    o15  = _f([x["o"] for x in c15])
    v15  = _f([x["v"] for x in c15])

    close = float(c5a[-1])
    if close <= 0:
        return None

    # Filtro mercado muerto
    atr14   = calc_atr(h5, l5, c5a, 14)
    atr_pct = atr14 / close * 100
    if atr_pct < MIN_ATR_PCT:
        return None

    vol20 = float(np.mean(v5[-20:])) if len(v5) >= 20 else 1.0
    if vol20 <= 0 or float(v5[-2]) < vol20 * MIN_VOL_MULT:
        return None

    # ── Componentes ──────────────────────────────────────────────────

    # 1. ZigZag (Pine: crossover(close, peak) / crossunder(close, valley))
    ps  = peak_series(h5, PIVOT_LEN)
    vs  = valley_series(l5, PIVOT_LEN)
    long_zz  = crossover(c5a, ps)
    short_zz = crossunder(c5a, vs)

    # 2. HMA 5m
    hma5 = calc_hma(c5a, HMA_LEN)
    hma_bull, hma_bear = hma_direction(hma5, c5a)

    # 3. Future-Trend 5m
    ft5  = calc_future_trend(o5, c5a, v5, FT_PERIOD)
    ft_bull, ft_bear = ft5 > 0, ft5 < 0

    # 4. Multi-TF 15m
    hma15 = calc_hma(c15a, HMA_LEN)
    hma15_bull, hma15_bear = hma_direction(hma15, c15a)
    ft15  = calc_future_trend(o15, c15a, v15, FT_PERIOD)

    # 5. Volumen
    vol_ok = float(v5[-2]) > vol20 * 1.2

    # ── Score LONG ───────────────────────────────────────────────────
    ls, lr = 0, []
    if long_zz:
        ls += 2; lr.append(f"ZZ↑{ps[-1]:.5g}")
    if hma_bull:
        ls += 1; lr.append(f"HMA↑")
    if ft_bull:
        ls += 1; lr.append(f"FT+{ft5:.0f}")
    if hma15_bull and ft15 > 0:
        ls += 1; lr.append("MTF▲")
    if vol_ok:
        ls += 1; lr.append("VOL↑")

    # ── Score SHORT ──────────────────────────────────────────────────
    ss, sr = 0, []
    if short_zz:
        ss += 2; sr.append(f"ZZ↓{vs[-1]:.5g}")
    if hma_bear:
        ss += 1; sr.append(f"HMA↓")
    if ft_bear:
        ss += 1; sr.append(f"FT{ft5:.0f}")
    if hma15_bear and ft15 < 0:
        ss += 1; sr.append("MTF▼")
    if vol_ok:
        ss += 1; sr.append("VOL↑")

    # ── SL / TP ──────────────────────────────────────────────────────
    if USE_ATR_TP:
        sl_d  = atr14 * ATR_SL
        tp1_d = atr14 * ATR_TP1
        tp2_d = atr14 * ATR_TP2
    else:
        pip   = _pip_size(close)
        sl_d  = SL_PIPS * pip
        tp1_d = TP_PIPS * pip
        tp2_d = TP_PIPS * 2 * pip

    # SL mínimo: 0.05% del precio (evita fill inmediato)
    sl_min = close * 0.0005
    sl_d   = max(sl_d, sl_min)

    # ── Señal ────────────────────────────────────────────────────────
    if ls >= MIN_SCORE and ls > ss:
        return {
            "side": "BUY", "score": ls, "reasons": lr,
            "entry": close,
            "sl":    round(close - sl_d, 8),
            "tp1":   round(close + tp1_d, 8),
            "tp2":   round(close + tp2_d, 8),
            "atr": atr14, "atr_pct": atr_pct,
            "ft": ft5, "hma": float(hma5[-1]),
            "zz": long_zz,
            "peak": float(ps[-1]) if not np.isnan(ps[-1]) else close,
        }

    if ss >= MIN_SCORE and ss > ls:
        return {
            "side": "SELL", "score": ss, "reasons": sr,
            "entry": close,
            "sl":    round(close + sl_d, 8),
            "tp1":   round(close - tp1_d, 8),
            "tp2":   round(close - tp2_d, 8),
            "atr": atr14, "atr_pct": atr_pct,
            "ft": ft5, "hma": float(hma5[-1]),
            "zz": short_zz,
            "valley": float(vs[-1]) if not np.isnan(vs[-1]) else close,
        }

    return None


# ══════════════════════════════════════════════════════════════════════
#  BINGX CLIENT — 100% aiohttp, formato probado (igual que client.py)
# ══════════════════════════════════════════════════════════════════════

class BingXClient:
    """
    Cliente aiohttp con:
    · Pool persistente 300 conexiones, keepalive 60s
    · Firma HMAC-SHA256 correcta (sorted urlencode)
    · place_order: quoteOrderQty + SL/TP embebidos (sin positionSide)
    · Leverage paralelo (LONG+SHORT simultáneo)
    """

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        # Public endpoints (klines/tickers) get more slots than auth endpoints
        self._sem_pub  = asyncio.Semaphore(25)  # klines, tickers
        self._sem_auth = asyncio.Semaphore(8)   # balance, orders, positions
        self._fail: dict[str, int] = defaultdict(int)
        self._lev_done: set        = set()

    def _sess(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            conn = aiohttp.TCPConnector(
                limit=300, limit_per_host=100,
                ttl_dns_cache=600, keepalive_timeout=60,
                ssl=False, force_close=False,
            )
            self._session = aiohttp.ClientSession(
                connector=conn,
                timeout=aiohttp.ClientTimeout(total=12, connect=4),
            )
        return self._session

    def _sign(self, params: dict) -> str:
        qs = urlencode(sorted(params.items()))
        return hmac.new(API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()

    def _auth(self, extra: dict = None) -> dict:
        p = dict(extra or {})
        p["timestamp"] = int(time.time() * 1000)
        p["signature"] = self._sign(p)
        return p

    def _hdrs(self) -> dict:
        return {"X-BX-APIKEY": API_KEY}

    async def _get(self, path: str, params: dict = None, auth=False) -> dict:
        async with (self._sem_auth if auth else self._sem_pub):
            p = self._auth(params) if auth else (params or {})
            for attempt in range(3):
                try:
                    async with self._sess().get(
                        BINGX_BASE + path, params=p, headers=self._hdrs()
                    ) as r:
                        return await r.json(content_type=None)
                except asyncio.TimeoutError:
                    if attempt < 2:
                        await asyncio.sleep(1.5 ** attempt)
                except Exception as e:
                    log.debug(f"GET {path}: {e}")
                    return {}
        return {}

    async def _post(self, path: str, params: dict = None) -> dict:
        p = self._auth(params)
        async with self._sem_auth:
            for attempt in range(3):
                try:
                    async with self._sess().post(
                        BINGX_BASE + path, params=p, headers=self._hdrs()
                    ) as r:
                        return await r.json(content_type=None)
                except asyncio.TimeoutError:
                    if attempt < 2:
                        await asyncio.sleep(1.5 ** attempt)
                except Exception as e:
                    log.debug(f"POST {path}: {e}")
                    return {}
        return {}

    async def get_balance(self) -> float:
        resp = await self._get("/openApi/swap/v2/user/balance", auth=True)
        try:
            if not isinstance(resp, dict) or resp.get("code", -1) not in (0, 200):
                return 0.0
            data = resp.get("data", {})
            if isinstance(data, dict):
                bal = data.get("balance", {})
                if isinstance(bal, dict):
                    for k in ("availableMargin", "available", "equity", "balance"):
                        v = bal.get(k)
                        if v is not None:
                            f = float(v)
                            if f > 0:
                                return f
                for k in ("availableMargin", "available", "equity"):
                    v = data.get(k)
                    if v is not None and float(v) > 0:
                        return float(v)
        except Exception as e:
            log.warning(f"get_balance parse: {e}")

        # Override manual (útil si API no retorna balance correctamente)
        ov = float(os.getenv("BALANCE_OVERRIDE", "0"))
        if ov > 0:
            log.warning(f"⚠️  BALANCE_OVERRIDE={ov} USDT")
            return ov
        return 0.0

    async def get_symbols(self) -> list[str]:
        resp = await self._get("/openApi/swap/v2/quote/contracts")
        try:
            out = []
            for item in resp.get("data", []):
                sym = item.get("symbol", "")
                if not sym.endswith("-USDT"):
                    continue
                if str(item.get("status", "1")) not in ("1", "TRADING"):
                    continue
                if any(x in sym for x in ("1000", "DEFI", "INDEX", "BEAR", "BULL")):
                    continue
                out.append(sym)
            return sorted(out)
        except Exception:
            return []

    async def klines(self, symbol: str, interval: str, limit=250,
                     is_warmup: bool = False) -> list[dict]:
        # During warmup allow more retries; normal ops: block after 5 fails
        fail_limit = 20 if is_warmup else 5
        if self._fail[symbol] >= fail_limit:
            return []
        try:
            resp = await asyncio.wait_for(
                self._get("/openApi/swap/v3/quote/klines", {
                    "symbol": symbol, "interval": interval, "limit": limit,
                }),
                timeout=15.0,
            )
        except asyncio.TimeoutError:
            self._fail[symbol] += 1
            log.debug(f"klines timeout: {symbol} {interval}")
            return []
        except Exception as e:
            self._fail[symbol] += 1
            log.debug(f"klines error {symbol}: {e}")
            return []

        if not isinstance(resp, dict):
            self._fail[symbol] += 1
            return []

        code = resp.get("code", -1)
        if code not in (0, 200, None):
            # Log first failure per symbol to help diagnose
            if self._fail[symbol] == 0:
                log.debug(f"klines {symbol} code={code} msg={resp.get('msg','')}")
            self._fail[symbol] += 1
            return []

        self._fail[symbol] = 0
        out = []
        for k in (resp.get("data") or []):
            try:
                out.append({
                    "t": int(k[0]),
                    "o": float(k[1]), "h": float(k[2]),
                    "l": float(k[3]), "c": float(k[4]),
                    "v": float(k[5]),
                })
            except Exception:
                continue
        return sorted(out, key=lambda x: x["t"])

    async def get_positions(self) -> dict[str, dict]:
        resp = await self._get("/openApi/swap/v2/user/positions", auth=True)
        try:
            data = resp.get("data", [])
            if isinstance(data, list):
                return {p["symbol"]: p for p in data
                        if abs(float(p.get("positionAmt", 0))) > 1e-9}
        except Exception:
            pass
        return {}

    async def set_leverage(self, symbol: str) -> None:
        """LONG+SHORT en paralelo, cacheado (igual que client.py)."""
        if symbol in self._lev_done:
            return
        await asyncio.gather(
            self._post("/openApi/swap/v2/trade/leverage",
                       {"symbol": symbol, "side": "LONG",  "leverage": LEVERAGE}),
            self._post("/openApi/swap/v2/trade/leverage",
                       {"symbol": symbol, "side": "SHORT", "leverage": LEVERAGE}),
        )
        self._lev_done.add(symbol)

    async def place_order(self, symbol: str, side: str, size_usdt: float,
                          sl: float, tp: float) -> dict:
        """
        Formato probado (idéntico a client.py que funciona):
        · quoteOrderQty = USDT a usar
        · stopLoss / takeProfit embebidos
        · Sin positionSide (modo one-way, default en BingX)
        """
        await self.set_leverage(symbol)
        resp = await self._post("/openApi/swap/v2/trade/order", {
            "symbol":        symbol,
            "side":          side,
            "type":          "MARKET",
            "quoteOrderQty": size_usdt,
            "stopLoss":      str(round(sl, 8)),
            "takeProfit":    str(round(tp, 8)),
        })
        return resp if isinstance(resp, dict) else {}

    async def close_position(self, symbol: str) -> dict:
        resp = await self._post("/openApi/swap/v2/trade/closePosition",
                                {"symbol": symbol})
        return resp if isinstance(resp, dict) else {}

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()


# ══════════════════════════════════════════════════════════════════════
#  TELEGRAM (aiohttp, sin httpx)
# ══════════════════════════════════════════════════════════════════════

async def tg(msg: str, client: BingXClient = None) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    # Reusar sesión del cliente si se pasa, si no crear una temporal
    sess = client._sess() if client else None
    try:
        if sess:
            async with sess.post(url, json={
                "chat_id": TELEGRAM_CHAT,
                "text": msg,
                "parse_mode": "HTML"
            }) as r:
                await r.read()
        else:
            conn = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=conn) as s:
                async with s.post(url, json={
                    "chat_id": TELEGRAM_CHAT,
                    "text": msg,
                    "parse_mode": "HTML"
                }) as r:
                    await r.read()
    except Exception as e:
        log.debug(f"TG: {e}")


# ══════════════════════════════════════════════════════════════════════
#  CACHÉ OHLCV incremental
# ══════════════════════════════════════════════════════════════════════

class KlineCache:
    def __init__(self, max_size: int = 300):
        self.data: list[dict] = []
        self.max_size = max_size
        self.warm = False

    def store(self, raw: list[dict]) -> bool:
        if len(raw) < 50:
            return False
        self.data = raw[-self.max_size:]
        self.warm = True
        return True

    def update(self, new: list[dict]) -> bool:
        if not self.warm or not new:
            return self.warm
        last_t = self.data[-1]["t"] if self.data else 0
        added = [x for x in new if x["t"] > last_t]
        if added:
            self.data.extend(added)
            self.data = self.data[-self.max_size:]
        elif new:
            # actualizar última vela (en construcción)
            self.data[-1] = new[-1]
        return True


# ══════════════════════════════════════════════════════════════════════
#  BOT PRINCIPAL
# ══════════════════════════════════════════════════════════════════════

class PhantomEdge:

    def __init__(self):
        self.api       = BingXClient()
        self.cache5:  dict[str, KlineCache] = {}
        self.cache15: dict[str, KlineCache] = {}
        self.symbols:  list[str] = []
        self.open_pos: dict[str, dict] = {}
        self.balance   = 0.0
        self.cycle     = 0
        self.daily_trades = 0
        self.daily_loss   = 0.0
        self.last_day  = datetime.now(timezone.utc).date()
        self.t0        = time.time()
        self.kill      = False
        self.total_sig = 0
        self.stop_evt  = asyncio.Event()

    def _reset_daily(self):
        today = datetime.now(timezone.utc).date()
        if today != self.last_day:
            self.daily_trades = 0
            self.daily_loss   = 0.0
            self.last_day     = today
            self.kill         = False
            log.info("📅 Contadores diarios reseteados")

    def _can_trade(self) -> tuple[bool, str]:
        if self.kill:
            return False, "KILL switch activo"
        if len(self.open_pos) >= MAX_POSITIONS:
            return False, f"MAX_POSITIONS={MAX_POSITIONS}"
        if self.daily_loss >= MAX_DAILY_LOSS:
            self.kill = True
            return False, f"MAX_DAILY_LOSS={MAX_DAILY_LOSS}% → KILL"
        if self.balance < MIN_BALANCE:
            return False, (
                f"Balance {self.balance:.2f} < {MIN_BALANCE:.2f} USDT mínimo. "
                f"Transfiere USDT a Futuros Perpetuos en BingX o pon "
                f"BALANCE_OVERRIDE=<saldo> en env vars de Railway."
            )
        return True, "OK"

    # ── Warmup ───────────────────────────────────────────────────────

    async def _warmup_one(self, sym: str) -> bool:
        need5  = max(HMA_LEN + 20, FT_PERIOD * 3 + 20, PIVOT_LEN * 6)
        need15 = max(HMA_LEN + 10, FT_PERIOD * 2 + 10)
        try:
            k5, k15 = await asyncio.gather(
                self.api.klines(sym, TIMEFRAME, max(300, need5 + 50), is_warmup=True),
                self.api.klines(sym, TIMEFRAME_SLOW, max(150, need15 + 30), is_warmup=True),
                return_exceptions=True,
            )
            c5  = self.cache5.setdefault(sym, KlineCache(300))
            c15 = self.cache15.setdefault(sym, KlineCache(200))
            if isinstance(k5, list) and isinstance(k15, list):
                ok = c5.store(k5) and c15.store(k15)
                if not ok and (isinstance(k5, list) and isinstance(k15, list)):
                    log.debug(f"warmup store fail {sym}: len5={len(k5)} len15={len(k15)}")
                return ok
        except Exception as e:
            log.debug(f"warmup {sym}: {e}")
        return False

    async def _warmup_all_background(self, syms: list[str]):
        """
        Calienta TODOS los símbolos en background sin bloquear el scanner.
        Usa concurrencia muy baja (8) para convivir con las actualizaciones
        incrementales del ciclo de escaneo.
        """
        total = len(syms)
        log.info(f"[BG-WARMUP] Iniciando {total} pares...")
        for i in range(0, total, 8):
            if self.stop_evt.is_set():
                break
            batch = syms[i:i + 8]
            res = await asyncio.gather(
                *[self._warmup_one(s) for s in batch],
                return_exceptions=True,
            )
            done_bg = sum(1 for r in res if r is True)
            warm_now = sum(1 for s in self.symbols
                           if self.cache5.get(s, KlineCache()).warm)
            if (i // 8) % 10 == 0:   # log cada 80 símbolos
                log.info(
                    f"[BG-WARMUP] {i+len(batch)}/{total} procesados | "
                    f"Total listos: {warm_now}/{len(self.symbols)}"
                )
            await asyncio.sleep(0.4)   # pausa entre batches para no saturar
        warm_final = sum(1 for s in self.symbols
                         if self.cache5.get(s, KlineCache()).warm)
        log.info(f"[BG-WARMUP] ✅ Completado — {warm_final}/{len(self.symbols)} pares listos")

    async def _refresh_symbols(self):
        """Recarga la lista de símbolos de BingX (nuevas monedas se añaden cada semana)."""
        new_syms = await self.api.get_symbols()
        if not new_syms:
            return
        added = [s for s in new_syms if s not in self.symbols]
        if added:
            log.info(f"[SYMBOLS] {len(added)} nuevos pares detectados: {added[:5]}...")
            self.symbols = new_syms
            # Calentar los nuevos inmediatamente
            asyncio.create_task(self._warmup_batch(added, conc=8))
        else:
            self.symbols = new_syms

    async def _warmup_batch(self, syms: list[str], conc: int = 12):
        done = 0
        # Reset fail counters so previous errors don't permanently block symbols
        for s in syms:
            self.api._fail[s] = 0

        for i in range(0, len(syms), conc):
            batch = syms[i:i + conc]
            res = await asyncio.gather(
                *[self._warmup_one(s) for s in batch],
                return_exceptions=True,
            )
            done += sum(1 for r in res if r is True)
            pct = done / len(syms) * 100
            log.info(f"  WarmUp {done}/{len(syms)} ({pct:.0f}%) listos")
            # Small pause between batches to avoid rate-limiting
            await asyncio.sleep(0.3)

    # ── Actualización incremental ─────────────────────────────────────

    async def _update(self, sym: str):
        c5  = self.cache5.get(sym)
        c15 = self.cache15.get(sym)
        if not c5 or not c15 or not c5.warm or not c15.warm:
            return

        k5, k15 = await asyncio.gather(
            self.api.klines(sym, TIMEFRAME, 3),
            self.api.klines(sym, TIMEFRAME_SLOW, 3),
            return_exceptions=True,
        )
        if isinstance(k5, list):
            c5.update(k5)
        if isinstance(k15, list):
            c15.update(k15)

    # ── Ciclo de escaneo ──────────────────────────────────────────────

    async def scan(self):
        self.cycle += 1
        self._reset_daily()

        # Refrescar lista de símbolos cada 60 ciclos (~30 min con interval=30s)
        if self.cycle % 60 == 0:
            await self._refresh_symbols()

        self.balance, pos_raw = await asyncio.gather(
            self.api.get_balance(),
            self.api.get_positions(),
        )
        self.open_pos = pos_raw

        warm_count = sum(
            1 for s in self.symbols
            if self.cache5.get(s, KlineCache()).warm
        )


        log.info(
            f"[C{self.cycle:04d}] Bal:{self.balance:.2f}U | "
            f"Pos:{len(self.open_pos)}/{MAX_POSITIONS} | "
            f"Warm:{warm_count}/{len(self.symbols)} | "
            f"Señales:{self.total_sig} | "
            f"{'AUTO ✅' if AUTO_TRADING else 'SIM 🟡'}"
        )

        ok, reason = self._can_trade()
        if not ok:
            log.warning(f"  ⛔ {reason}")
            return

        # Candidatos: cacheados y sin posición abierta
        warm = {s for s in self.symbols
                if self.cache5.get(s, KlineCache()).warm}
        prio  = [s for s in PRIORITY if s in warm and s not in self.open_pos]
        resto = [s for s in warm if s not in self.open_pos and s not in prio]
        cands = prio + resto

        if not cands:
            log.info("  Sin candidatos (todos con posición o sin warmup)")
            return

        # Actualizar en paralelo (máx 20 simultáneos — evita rate-limit)
        for i in range(0, len(cands), 20):
            await asyncio.gather(
                *[self._update(s) for s in cands[i:i + 20]],
                return_exceptions=True,
            )

        signals_found = 0
        for sym in cands:
            if len(self.open_pos) >= MAX_POSITIONS:
                break
            if sym in self.open_pos:
                continue

            c5  = self.cache5.get(sym)
            c15 = self.cache15.get(sym)
            if not c5 or not c15:
                continue

            sig = analyze(c5.data, c15.data)
            if sig is None:
                continue

            signals_found += 1
            self.total_sig += 1

            e    = "🟢" if sig["side"] == "BUY" else "🔴"
            zzt  = "⚡ZZ!" if sig.get("zz") else ""
            pt   = "⭐" if sym in PRIORITY else ""
            log.info(
                f"  {e}{pt}{zzt} {sym} {sig['side']} "
                f"Score:{sig['score']}/6 | {' '.join(sig['reasons'])} | "
                f"FT:{sig['ft']:.0f} HMA:{sig['hma']:.5g} "
                f"ATR:{sig['atr_pct']:.2f}%"
            )

            if AUTO_TRADING:
                entry  = sig["entry"]
                sl_d   = abs(entry - sig["sl"])
                sl_pct = sl_d / entry * 100

                if sl_pct < 0.05:
                    log.warning(f"  [SKIP] {sym} SL muy ajustado: {sl_pct:.3f}%")
                    continue

                t_start = time.time()
                resp = await self.api.place_order(
                    sym, sig["side"],
                    TRADE_USDT,
                    sig["sl"], sig["tp1"],   # TP1 como take-profit principal
                )
                ms = int((time.time() - t_start) * 1000)

                code = resp.get("code", -1)
                if code not in (0, 200, None):
                    log.error(
                        f"  ❌ {sym} order FAIL code={code} "
                        f"msg={resp.get('msg', '')} [{ms}ms]"
                    )
                    continue

                self.open_pos[sym] = {"symbol": sym, "side": sig["side"]}
                tp1_pct = abs(sig["tp1"] - entry) / entry * 100

                log.info(
                    f"  ✅ ENTRADA {sym} {sig['side']} "
                    f"entry={entry:.6g} SL={sig['sl']:.6g}(-{sl_pct:.2f}%) "
                    f"TP={sig['tp1']:.6g}(+{tp1_pct:.2f}%) [{ms}ms]"
                )

                await tg(
                    f"{e} <b>{sym}</b>{pt} — "
                    f"{'LONG' if sig['side']=='BUY' else 'SHORT'}\n"
                    f"📊 Score: {sig['score']}/6"
                    f"{' | ⚡ ZigZag Breakout!' if sig.get('zz') else ''}\n"
                    f"📍 Entry: {entry:.6g}\n"
                    f"🛡 SL:   {sig['sl']:.6g}  (-{sl_pct:.2f}%)\n"
                    f"🎯 TP1:  {sig['tp1']:.6g}  (+{tp1_pct:.2f}%)\n"
                    f"🎯 TP2:  {sig['tp2']:.6g}  (ref)\n"
                    f"🌊 FT:{sig['ft']:+.0f} | HMA:{sig['hma']:.5g} | "
                    f"ATR:{sig['atr_pct']:.2f}%\n"
                    f"✨ {' · '.join(sig['reasons'])}\n"
                    f"💰 Bal:{self.balance:.2f}U | ⚡{ms}ms",
                    self.api,
                )

            else:
                # Modo simulación — sólo notifica, no opera
                entry   = sig["entry"]
                sl_pct  = abs(entry - sig["sl"]) / entry * 100
                tp1_pct = abs(sig["tp1"] - entry) / entry * 100
                await tg(
                    f"🔔 [SIM] <b>{sym}</b>{pt} — "
                    f"{'LONG' if sig['side']=='BUY' else 'SHORT'} Score:{sig['score']}/6\n"
                    f"{zzt} Entry:{entry:.6g} | SL:{sig['sl']:.6g}(-{sl_pct:.2f}%)\n"
                    f"TP1:{sig['tp1']:.6g}(+{tp1_pct:.2f}%) | TP2:{sig['tp2']:.6g}\n"
                    f"FT:{sig['ft']:+.0f} HMA:{sig['hma']:.5g} ATR:{sig['atr_pct']:.2f}%\n"
                    f"✨ {' · '.join(sig['reasons'])}",
                    self.api,
                )

        log.info(
            f"  Cands:{len(cands)} | Señales:{signals_found} | "
            f"Pos:{len(self.open_pos)}/{MAX_POSITIONS}"
        )

    # ── Main loop ─────────────────────────────────────────────────────

    async def run(self):
        log.info("═" * 65)
        log.info("  PHANTOM EDGE v7.0 — ZigZag + HMA + Future-Trend")
        log.info(f"  Pivot:{PIVOT_LEN} | HMA:{HMA_LEN} | FT:{FT_PERIOD} | "
                 f"Score≥{MIN_SCORE}/6")
        log.info(f"  SL:{ATR_SL}R | TP1:{ATR_TP1}R | TP2:{ATR_TP2}R | Lev:x{LEVERAGE}")
        log.info(f"  Trade:{TRADE_USDT}$ | MaxPos:{MAX_POSITIONS} | "
                 f"Interval:{SCAN_INTERVAL}s")
        log.info(f"  Mode: {'🟢 AUTO-TRADING ACTIVO' if AUTO_TRADING else '🟡 SIMULACIÓN (pon AUTO_TRADING_ENABLED=true en Railway)'}")
        if not AUTO_TRADING:
            log.warning("  ⚠️  Para operar real: Railway → Variables → "
                        "AUTO_TRADING_ENABLED = true")
        log.info("═" * 65)

        self.balance = await self.api.get_balance()
        log.info(f"💰 Balance: {self.balance:.4f} USDT")
        if self.balance == 0 and API_KEY:
            log.error(
                "❌ Balance=0. Opciones:\n"
                "   1. Transfiere USDT a Futuros Perpetuos en BingX\n"
                "   2. Añade BALANCE_OVERRIDE=<saldo> en Railway env vars\n"
                "   3. Verifica BINGX_API_KEY y BINGX_SECRET_KEY"
            )

        self.symbols = await self.api.get_symbols()
        log.info(f"📊 {len(self.symbols)} pares USDT-Perp cargados")
        if not self.symbols:
            log.error("❌ Sin símbolos — verifica API key")
            return

        # ── Diagnóstico rápido de API pública ────────────────────────
        test_sym = "BTC-USDT"
        test_k = await self.api.klines(test_sym, "5m", 5, is_warmup=True)
        if test_k:
            log.info(f"✅ API klines OK — BTC-USDT 5m: {len(test_k)} velas recibidas")
        else:
            log.error(
                "❌ klines API falló en BTC-USDT. "
                "Revisa conectividad de Railway hacia open-api.bingx.com. "
                "El bot continuará intentando en el warmup."
            )

        # Warmup progresivo: priority primero, luego TODO el universo en background
        prio_ok = [s for s in PRIORITY if s in self.symbols]
        resto   = [s for s in self.symbols if s not in prio_ok]

        log.info(f"🔥 WarmUp priority ({len(prio_ok)} pares, conc=10)...")
        await self._warmup_batch(prio_ok, conc=10)

        warm_prio = sum(1 for s in prio_ok if self.cache5.get(s, KlineCache()).warm)
        log.info(f"✅ {warm_prio}/{len(prio_ok)} priority listos — arrancando scanner")

        # Calentar el resto de símbolos EN BACKGROUND mientras el bot ya opera
        # Usa concurrencia baja (8) para no interferir con el scanner en vivo
        log.info(f"🔄 WarmUp background: {len(resto)} pares restantes (conc=8, sin bloquear)")
        asyncio.create_task(self._warmup_all_background(resto))

        await tg(
            f"🤖 <b>Phantom Edge v7.0</b>\n"
            f"📐 ZigZag({PIVOT_LEN}) + HMA({HMA_LEN}) + FutureTrend({FT_PERIOD})\n"
            f"💰 Balance: {self.balance:.2f} USDT\n"
            f"📊 {len(self.symbols)} pares | Score≥{MIN_SCORE}/6\n"
            f"🎯 TP:{ATR_TP1}R/{ATR_TP2}R | SL:{ATR_SL}R | Lev:x{LEVERAGE}\n"
            f"{'🟢 AUTO-TRADING ACTIVO' if AUTO_TRADING else '🟡 SIMULACIÓN'}",
            self.api,
        )

        # Loop principal
        while not self.stop_evt.is_set():
            try:
                t0 = time.time()
                await self.scan()
                elapsed  = time.time() - t0
                sleep_s  = max(5.0, SCAN_INTERVAL - elapsed)
                log.info(f"  ⏱ Ciclo:{elapsed:.1f}s | sleep:{sleep_s:.0f}s\n")
                try:
                    await asyncio.wait_for(self.stop_evt.wait(), timeout=sleep_s)
                except asyncio.TimeoutError:
                    pass
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"❌ Error en ciclo: {e}", exc_info=True)
                await asyncio.sleep(15)

        log.info("Bot detenido")
        await self.api.close()


# ══════════════════════════════════════════════════════════════════════
#  HEALTH CHECK SERVER
# ══════════════════════════════════════════════════════════════════════

async def health_server(bot: PhantomEdge) -> None:
    from aiohttp import web

    async def handler(req: web.Request) -> web.Response:
        ok, reason = bot._can_trade()
        warm = sum(1 for s in bot.symbols
                   if bot.cache5.get(s, KlineCache()).warm)
        return web.json_response({
            "version":       "7.0",
            "strategy":      "ZigZag+HMA+FutureTrend",
            "status":        "kill" if bot.kill else ("trading" if ok else "blocked"),
            "block_reason":  None if ok else reason,
            "auto_trading":  AUTO_TRADING,
            "uptime_min":    round((time.time() - bot.t0) / 60, 1),
            "cycle":         bot.cycle,
            "balance_usdt":  round(bot.balance, 2),
            "warm_symbols":  warm,
            "total_symbols": len(bot.symbols),
            "open_positions": len(bot.open_pos),
            "open_symbols":  list(bot.open_pos.keys()),
            "daily_trades":  bot.daily_trades,
            "total_signals": bot.total_sig,
            "params": {
                "pivot_len":  PIVOT_LEN,
                "hma_len":    HMA_LEN,
                "ft_period":  FT_PERIOD,
                "min_score":  f"{MIN_SCORE}/6",
                "atr_sl":     ATR_SL,
                "atr_tp1":    ATR_TP1,
                "atr_tp2":    ATR_TP2,
                "leverage":   LEVERAGE,
                "trade_usdt": TRADE_USDT,
            },
        })

    app = web.Application()
    app.router.add_get("/", handler)
    app.router.add_get("/health", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info(f"🌐 Health server: http://0.0.0.0:{PORT}")


# ══════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════

async def main():
    bot = PhantomEdge()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, bot.stop_evt.set)

    await asyncio.gather(
        health_server(bot),
        bot.run(),
    )


if __name__ == "__main__":
    asyncio.run(main())
