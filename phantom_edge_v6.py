"""
╔══════════════════════════════════════════════════════════════════════╗
║     PHANTOM EDGE BOT v6.0 — ZigZag + HMA + Future-Trend            ║
║     Traducción EXACTA del Pine Script al motor de señales           ║
║                                                                      ║
║  LÓGICA ORIGINAL (Pine Script):                                      ║
║  ─────────────────────────────                                       ║
║  LONG:  crossover(close, peak) + HMA alcista + VolDelta > 0         ║
║  SHORT: crossunder(close, valley) + HMA bajista + VolDelta < 0      ║
║                                                                      ║
║  MEJORAS vs Pine original:                                           ║
║  · Confirmación multi-TF: señal en 5m validada con 15m              ║
║  · TP dinámico: ATR×2.0 en vez de pips fijos (adapta a crypto)     ║
║  · SL en swing previo en vez de pips fijos                          ║
║  · Filtro de volumen: solo si vol > media (evita señales falsas)    ║
║  · Score de calidad: cuenta cuántos componentes están alineados      ║
╚══════════════════════════════════════════════════════════════════════╝

TRADUCCIÓN Pine → Python:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ta.pivothigh(high, n, n)     →  pivot_high(h, n)
  ta.pivotlow(low, n, n)       →  pivot_low(l, n)
  ta.hma(close, len)           →  hma(c, len)   [WMA(2×WMA(n/2) - WMA(n), sqrt(n))]
  delta_vol = close>open?vol:  →  calc_volume_delta(o,c,v)
  vol_delta_sum loop           →  future_trend(delta, ft_period)
  ta.crossover(close, peak)    →  crossover(c, peak_series)
  ta.crossunder(close, valley) →  crossunder(c, valley_series)

SEÑAL MEJORADA (score 0-6):
  +2  Ruptura ZigZag confirmada (cruce de peak/valley)
  +1  HMA dirección correcta (slope alcista/bajista)
  +1  Future-Trend (Volume Delta) a favor
  +1  Confirmación multi-TF 15m (misma dirección)
  +1  Volumen actual > media 20 períodos
  MIN_SCORE = 3 (mínimo: ruptura + HMA + FutureTrend = señal original)
  IDEAL    = 5+ (señal fuerte con confirmación)
"""

import os, asyncio, logging, time, hmac, hashlib, json, math
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional
import numpy as np
import httpx

# ─────────────────────────────────────────────────────────────────────
#  CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────
API_KEY        = os.getenv("BINGX_API_KEY", "")
API_SECRET     = os.getenv("BINGX_API_SECRET", "")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")

AUTO_TRADING     = os.getenv("AUTO_TRADING_ENABLED", "false").lower() == "true"
LEVERAGE         = int(os.getenv("LEVERAGE", "10"))
TIMEFRAME        = os.getenv("TIMEFRAME", "5m")
TIMEFRAME_SLOW   = os.getenv("TIMEFRAME_SLOW", "15m")

# Parámetros del indicador (mismos que Pine Script)
PIVOT_LEN  = int(os.getenv("PIVOT_LEN", "5"))      # ZigZag lookback
HMA_LEN    = int(os.getenv("HMA_LEN", "50"))        # Hull MA length
FT_PERIOD  = int(os.getenv("FT_PERIOD", "25"))      # Future-Trend period
TP_PIPS    = float(os.getenv("TP_PIPS", "45.0"))    # TP original en pips
SL_PIPS    = float(os.getenv("SL_PIPS", "30.0"))    # SL original en pips
USE_ATR_TP = os.getenv("USE_ATR_TP", "true").lower() == "true"  # ATR dinámico
ATR_SL     = float(os.getenv("ATR_SL", "1.5"))      # SL = ATR × 1.5
ATR_TP1    = float(os.getenv("ATR_TP1", "1.5"))     # TP1 = ATR × 1.5 (1R)
ATR_TP2    = float(os.getenv("ATR_TP2", "3.0"))     # TP2 = ATR × 3.0 (2R)

MIN_SCORE        = int(os.getenv("MIN_SCORE", "3"))   # mínimo para señal
MIN_ATR_PCT      = float(os.getenv("MIN_ATR_PCT", "0.10"))
MIN_VOL_MULT     = float(os.getenv("MIN_VOL_MULT", "0.6"))

TRADE_USDT       = float(os.getenv("TRADE_USDT", "9"))
MAX_POSITIONS    = int(os.getenv("MAX_POSITIONS", "5"))
SCAN_INTERVAL    = int(os.getenv("SCAN_INTERVAL", "30"))
MAX_CONCURRENT   = int(os.getenv("MAX_CONCURRENT", "30"))
MAX_DAILY_TRADES = int(os.getenv("MAX_DAILY_TRADES", "40"))
MAX_DAILY_LOSS   = float(os.getenv("MAX_DAILY_LOSS", "5.0"))
PORT             = int(os.getenv("PORT", "8080"))
TP1_SIZE         = float(os.getenv("TP1_SIZE", "0.35"))
TP2_SIZE         = float(os.getenv("TP2_SIZE", "0.35"))

PRIORITY_RAW = os.getenv("PRIORITY_SYMBOLS",
    "BTC-USDT,ETH-USDT,SOL-USDT,XRP-USDT,DOGE-USDT,"
    "SKY-USDT,REZ-USDT,XNY-USDT,MAXXIN-USDT,ROBO-USDT,"
    "PNUT-USDT,TURBO-USDT,HYPE-USDT,RIVER-USDT,PEPE-USDT,"
    "WIF-USDT,BONK-USDT,FLOKI-USDT,ARB-USDT,SHIB-USDT"
)
PRIORITY = [s.strip() for s in PRIORITY_RAW.split(",") if s.strip()]

BINGX_BASE = "https://open-api.bingx.com"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("PE6")


# ══════════════════════════════════════════════════════════════════════
#  TRADUCCIÓN EXACTA: Pine Script → Python
# ══════════════════════════════════════════════════════════════════════

def _f(a) -> np.ndarray:
    """Sanitiza arrays — elimina NaN/Inf"""
    return np.nan_to_num(np.asarray(a, dtype=float), nan=0.0, posinf=0.0, neginf=0.0)


# ── ZigZag: ta.pivothigh / ta.pivotlow ──────────────────────────────
def pivot_highs(h: np.ndarray, n: int) -> np.ndarray:
    """
    Equivalente a ta.pivothigh(high, n, n) en Pine.
    Devuelve array donde cada posición tiene el valor del pivot
    o NaN si no es pivot. El pivot se confirma cuando han pasado
    n velas a la derecha (igual que Pine Script).
    """
    h = _f(h)
    result = np.full(len(h), np.nan)
    for i in range(n, len(h) - n):
        window = h[i-n : i+n+1]
        if h[i] == np.max(window):
            result[i] = h[i]
    return result


def pivot_lows(l: np.ndarray, n: int) -> np.ndarray:
    """Equivalente a ta.pivotlow(low, n, n) en Pine."""
    l = _f(l)
    result = np.full(len(l), np.nan)
    for i in range(n, len(l) - n):
        window = l[i-n : i+n+1]
        if l[i] == np.min(window):
            result[i] = l[i]
    return result


def last_peak(h: np.ndarray, n: int) -> float:
    """
    Equivalente a:
      var float peak = na
      if not na(ph): peak := ph
    → Devuelve el último peak confirmado (igual que la variable 'peak' en Pine)
    """
    ph = pivot_highs(h, n)
    # Buscar desde el final hacia atrás (excluir las últimas n velas — no confirmadas aún)
    for i in range(len(ph)-n-1, -1, -1):
        if not np.isnan(ph[i]):
            return float(ph[i])
    return float("nan")


def last_valley(l: np.ndarray, n: int) -> float:
    """Equivalente a la variable 'valley' en Pine."""
    pl = pivot_lows(l, n)
    for i in range(len(pl)-n-1, -1, -1):
        if not np.isnan(pl[i]):
            return float(pl[i])
    return float("nan")


def get_peak_series(h: np.ndarray, n: int) -> np.ndarray:
    """
    Reconstruye la serie 'peak' de Pine Script (forward-fill del último pivot).
    Necesaria para detectar el crossover correctamente.
    """
    ph = pivot_highs(h, n)
    result = np.full(len(h), np.nan)
    current = np.nan
    for i in range(len(h)):
        if not np.isnan(ph[i]):
            current = ph[i]
        result[i] = current
    return result


def get_valley_series(l: np.ndarray, n: int) -> np.ndarray:
    """Reconstruye la serie 'valley' de Pine Script (forward-fill)."""
    pl = pivot_lows(l, n)
    result = np.full(len(l), np.nan)
    current = np.nan
    for i in range(len(l)):
        if not np.isnan(pl[i]):
            current = pl[i]
        result[i] = current
    return result


# ── Crossover / Crossunder — ta.crossover / ta.crossunder ───────────
def crossover(series: np.ndarray, level: np.ndarray) -> bool:
    """
    ta.crossover(close, peak):
    La vela anterior estaba por DEBAJO de peak
    La vela actual está por ENCIMA de peak
    """
    if len(series) < 2 or len(level) < 2:
        return False
    prev_below = series[-2] <= level[-2]
    curr_above = series[-1] > level[-1]
    return bool(prev_below and curr_above and not np.isnan(level[-1]))


def crossunder(series: np.ndarray, level: np.ndarray) -> bool:
    """
    ta.crossunder(close, valley):
    La vela anterior estaba por ENCIMA de valley
    La vela actual está por DEBAJO de valley
    """
    if len(series) < 2 or len(level) < 2:
        return False
    prev_above = series[-2] >= level[-2]
    curr_below = series[-1] < level[-1]
    return bool(prev_above and curr_below and not np.isnan(level[-1]))


# ── HMA: ta.hma(close, len) ─────────────────────────────────────────
def calc_hma(c: np.ndarray, n: int) -> np.ndarray:
    """
    Hull Moving Average — Pine: ta.hma(close, len)
    HMA = WMA(2×WMA(n/2) − WMA(n), sqrt(n))
    Mucho más reactivo que EMA. Reacciona a cambios de tendencia ~mitad de velas.
    """
    c = _f(c)
    if len(c) < n:
        return np.full(len(c), c[-1] if len(c) > 0 else 0.0)

    def wma(arr, period):
        if len(arr) < period:
            return np.full(len(arr), arr[-1] if len(arr) > 0 else 0.0)
        weights = np.arange(1, period + 1, dtype=float)
        result  = np.zeros(len(arr))
        for i in range(period - 1, len(arr)):
            result[i] = np.dot(arr[i-period+1:i+1], weights) / weights.sum()
        return result

    half_n = max(1, n // 2)
    sqrt_n = max(1, int(math.sqrt(n)))

    wma_half = wma(c, half_n)
    wma_full = wma(c, n)

    # 2 × WMA(n/2) − WMA(n)
    raw = 2.0 * wma_half - wma_full

    # WMA de eso con periodo sqrt(n)
    hma_val = wma(raw, sqrt_n)
    return hma_val


def hma_direction(hma_vals: np.ndarray, close_vals: np.ndarray) -> tuple[bool, bool]:
    """
    Pine:
      hma_alcista = close > hma and hma > hma[1]
      hma_bajista = close < hma and hma < hma[1]
    """
    if len(hma_vals) < 2:
        return False, False
    hma_now  = hma_vals[-1]
    hma_prev = hma_vals[-2]
    close    = close_vals[-1]
    alcista  = bool(close > hma_now and hma_now > hma_prev)
    bajista  = bool(close < hma_now and hma_now < hma_prev)
    return alcista, bajista


# ── Future-Trend: Volume Delta × 3 períodos históricos ───────────────
def calc_future_trend(o: np.ndarray, c: np.ndarray, v: np.ndarray,
                      ft_period: int) -> float:
    """
    Traducción EXACTA del Pine Script:

      delta_vol = close > open ? volume : (close < open ? -volume : 0)

      for i = 0 to ft_period - 1
          if bar_index > (ft_period * 3)
              avg_delta = math.avg(delta_vol[i], delta_vol[i+ft_period], delta_vol[i+ft_period*2])
              vol_delta_sum += avg_delta

      vol_delta_avg = vol_delta_sum / ft_period

    Retorna: vol_delta_avg (>0 = bullish, <0 = bearish)
    """
    o, c, v = _f(o), _f(c), _f(v)
    n = len(c)

    if n < ft_period * 3 + 1:
        return 0.0

    # Delta de volumen por vela (igual que Pine)
    delta = np.where(c > o, v, np.where(c < o, -v, 0.0))

    # El loop de Pine itera desde i=0 hasta ft_period-1
    # usando los índices desde el final del array (como hace Pine con [i])
    vol_delta_sum = 0.0
    for i in range(ft_period):
        # Pine: delta_vol[i], delta_vol[i+ft_period], delta_vol[i+ft_period*2]
        # En Python (índice desde el final):
        idx0 = n - 1 - i
        idx1 = n - 1 - i - ft_period
        idx2 = n - 1 - i - ft_period * 2
        if idx2 >= 0:
            avg_d = (delta[idx0] + delta[idx1] + delta[idx2]) / 3.0
            vol_delta_sum += avg_d

    return vol_delta_sum / ft_period


# ── ATR (Wilder) ────────────────────────────────────────────────────
def calc_atr(h, l, c, p=14) -> float:
    h, l, c = _f(h), _f(l), _f(c)
    if len(c) < p + 1:
        return float(np.mean(h - l) + 1e-12)
    tr = np.maximum(h[1:]-l[1:], np.maximum(abs(h[1:]-c[:-1]), abs(l[1:]-c[:-1])))
    tr = np.r_[h[0]-l[0], tr]
    a  = np.zeros(len(tr))
    a[p-1] = np.mean(tr[:p])
    for i in range(p, len(tr)):
        a[i] = (a[i-1]*(p-1) + tr[i]) / p
    return max(float(a[-1]), 1e-12)


# ══════════════════════════════════════════════════════════════════════
#  MOTOR DE SEÑALES v6 — Traducción fiel + mejoras
# ══════════════════════════════════════════════════════════════════════
def analyze(c5: list[dict], c15: list[dict]) -> Optional[dict]:
    """
    Señal principal basada en:
      LONG:  crossover(close, peak)  + HMA alcista + FutureTrend > 0
      SHORT: crossunder(close, valley) + HMA bajista + FutureTrend < 0

    Mejoras:
      + Confirmación 15m (misma dirección HMA + FutureTrend)
      + Filtro de volumen mínimo
      + Score de calidad (0-6)
      + TP/SL dinámico basado en ATR en vez de pips fijos
    """
    # Mínimo de velas necesarias
    need_5m  = max(HMA_LEN + 10, FT_PERIOD * 3 + 10, PIVOT_LEN * 4)
    need_15m = max(HMA_LEN, FT_PERIOD * 2)

    if len(c5) < need_5m or len(c15) < need_15m:
        return None

    # ── Extraer arrays ───────────────────────────────────────
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

    # ── Filtro de mercado muerto ────────────────────────────
    atr14     = calc_atr(h5, l5, c5a, 14)
    atr_pct   = atr14 / close * 100
    if atr_pct < MIN_ATR_PCT:
        return None

    vol_ma20  = float(np.mean(v5[-20:])) if len(v5) >= 20 else 1.0
    if vol_ma20 <= 0 or float(v5[-1]) < vol_ma20 * MIN_VOL_MULT:
        return None

    # ════════════════════════════════════════════════════════
    #  COMPONENTE 1: ZigZag — Series de Peak / Valley
    #  (Igual que Pine Script: var float peak = na)
    # ════════════════════════════════════════════════════════
    peak_series   = get_peak_series(h5, PIVOT_LEN)
    valley_series = get_valley_series(l5, PIVOT_LEN)

    # Crossover / Crossunder (señal de entrada Pine original)
    long_zz  = crossover(c5a,  peak_series)
    short_zz = crossunder(c5a, valley_series)

    # ════════════════════════════════════════════════════════
    #  COMPONENTE 2: HMA — Filtro de tendencia rápida
    # ════════════════════════════════════════════════════════
    hma5_vals         = calc_hma(c5a, HMA_LEN)
    hma_bull, hma_bear = hma_direction(hma5_vals, c5a)

    # 15m HMA para confirmación multi-TF
    hma15_vals          = calc_hma(c15a, HMA_LEN)
    hma15_bull, hma15_bear = hma_direction(hma15_vals, c15a)

    # ════════════════════════════════════════════════════════
    #  COMPONENTE 3: Future-Trend (Volume Delta 3 períodos)
    # ════════════════════════════════════════════════════════
    ft_avg   = calc_future_trend(o5, c5a, v5, FT_PERIOD)
    ft_bull  = ft_avg > 0
    ft_bear  = ft_avg < 0

    # Future-Trend en 15m para confirmación
    ft15_avg  = calc_future_trend(o15, c15a, v15, FT_PERIOD)
    ft15_bull = ft15_avg > 0
    ft15_bear = ft15_avg < 0

    # ── Volumen ──────────────────────────────────────────────
    vol_ok = float(v5[-1]) > vol_ma20 * 1.2

    # ════════════════════════════════════════════════════════
    #  SEÑAL LONG (Pine original + mejoras)
    # ════════════════════════════════════════════════════════
    #  Pine original: longCond = crossover(close,peak) + hma_alcista + ft_bullish
    #  Aquí: Score 0-6
    long_score  = 0
    long_reasons = []

    # [2 pts] Ruptura ZigZag (condición principal Pine)
    if long_zz:
        long_score += 2
        pk = float(peak_series[-1]) if not np.isnan(peak_series[-1]) else 0
        long_reasons.append(f"ZZ_BRK↑{pk:.5g}")

    # [1 pt] HMA alcista 5m (condición Pine)
    if hma_bull:
        long_score += 1
        long_reasons.append(f"HMA↑{hma5_vals[-1]:.5g}")

    # [1 pt] Future-Trend bullish 5m (condición Pine)
    if ft_bull:
        long_score += 1
        long_reasons.append(f"FT+{ft_avg:.0f}")

    # [+1 pt] Confirmación 15m: HMA alcista Y FutureTrend bullish
    if hma15_bull and ft15_bull:
        long_score += 1
        long_reasons.append("MTF▲")

    # [+1 pt] Volumen elevado
    if vol_ok:
        long_score += 1
        long_reasons.append("VOL↑")

    # ════════════════════════════════════════════════════════
    #  SEÑAL SHORT
    # ════════════════════════════════════════════════════════
    short_score  = 0
    short_reasons = []

    if short_zz:
        short_score += 2
        vl = float(valley_series[-1]) if not np.isnan(valley_series[-1]) else 0
        short_reasons.append(f"ZZ_BRK↓{vl:.5g}")

    if hma_bear:
        short_score += 1
        short_reasons.append(f"HMA↓{hma5_vals[-1]:.5g}")

    if ft_bear:
        short_score += 1
        short_reasons.append(f"FT{ft_avg:.0f}")

    if hma15_bear and ft15_bear:
        short_score += 1
        short_reasons.append("MTF▼")

    if vol_ok:
        short_score += 1
        short_reasons.append("VOL↑")

    # ════════════════════════════════════════════════════════
    #  SL / TP — Dinámico (ATR) o Pips fijos (como Pine)
    # ════════════════════════════════════════════════════════
    if USE_ATR_TP:
        sl_dist  = atr14 * ATR_SL
        tp1_dist = atr14 * ATR_TP1
        tp2_dist = atr14 * ATR_TP2
    else:
        # Pips fijos como en Pine (mintick × pip_mult)
        # Para crypto, 1 pip = mintick (no ×10 como forex)
        pip = _get_pip_size(close)
        sl_dist  = SL_PIPS * pip
        tp1_dist = TP_PIPS * pip
        tp2_dist = TP_PIPS * 2 * pip   # TP2 = 2× TP original

    # ════════════════════════════════════════════════════════
    #  CONDICIÓN MÍNIMA: Igual que Pine Script
    #  Pine abre con score=3 (ZZ+HMA+FT). Permitimos score>=MIN_SCORE
    # ════════════════════════════════════════════════════════
    if long_score >= MIN_SCORE and long_score > short_score:
        return {
            "side": "BUY", "score": long_score, "max_score": 6,
            "reasons": long_reasons,
            "entry": close,
            "sl":    close - sl_dist,
            "tp1":   close + tp1_dist,
            "tp2":   close + tp2_dist,
            "atr": atr14, "atr_pct": atr_pct,
            "ft": ft_avg, "hma": float(hma5_vals[-1]),
            "zz_triggered": long_zz,
            "peak": float(peak_series[-1]) if not np.isnan(peak_series[-1]) else close,
        }

    if short_score >= MIN_SCORE and short_score > long_score:
        return {
            "side": "SELL", "score": short_score, "max_score": 6,
            "reasons": short_reasons,
            "entry": close,
            "sl":    close + sl_dist,
            "tp1":   close - tp1_dist,
            "tp2":   close - tp2_dist,
            "atr": atr14, "atr_pct": atr_pct,
            "ft": ft_avg, "hma": float(hma5_vals[-1]),
            "zz_triggered": short_zz,
            "valley": float(valley_series[-1]) if not np.isnan(valley_series[-1]) else close,
        }

    return None


def _get_pip_size(price: float) -> float:
    """
    Estima el tamaño de pip para crypto según el precio.
    Equivalente a syminfo.mintick en Pine.
    BTC ~50k → pip = 0.01 | DOGE ~0.1 → pip = 0.00001
    """
    if price > 10000: return 0.01
    if price > 100:   return 0.001
    if price > 1:     return 0.0001
    if price > 0.01:  return 0.00001
    return 0.000001


# ══════════════════════════════════════════════════════════════════════
#  BINGX CLIENT (igual que v5, probado)
# ══════════════════════════════════════════════════════════════════════
class BingXClient:

    def __init__(self):
        self._http: Optional[httpx.AsyncClient] = None
        self._sem  = asyncio.Semaphore(MAX_CONCURRENT)
        self._fail: dict[str, int] = defaultdict(int)
        self._lev_cache: set[str] = set()

    @property
    def http(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=12.0,
                limits=httpx.Limits(max_connections=60, max_keepalive_connections=30)
            )
        return self._http

    def _sign(self, p: dict) -> str:
        s = "&".join(f"{k}={v}" for k, v in sorted(p.items()))
        return hmac.new(API_SECRET.encode(), s.encode(), hashlib.sha256).hexdigest()

    def _ts(self) -> int:
        return int(time.time() * 1000)

    async def _get(self, path: str, params: dict = None, auth=False) -> dict:
        p = dict(params or {})
        hdrs = {}
        if auth:
            p["timestamp"] = self._ts()
            p["signature"] = self._sign(p)
            hdrs = {"X-BX-APIKEY": API_KEY}
        async with self._sem:
            try:
                r = await self.http.get(f"{BINGX_BASE}{path}", params=p, headers=hdrs)
                return r.json()
            except Exception as e:
                log.debug(f"GET {path}: {e}")
                return {}

    async def _post(self, path: str, params: dict = None) -> dict:
        p = dict(params or {})
        p["timestamp"] = self._ts()
        p["signature"] = self._sign(p)
        try:
            r = await self.http.post(
                f"{BINGX_BASE}{path}", data=p,
                headers={"X-BX-APIKEY": API_KEY,
                         "Content-Type": "application/x-www-form-urlencoded"}
            )
            return r.json()
        except Exception as e:
            log.debug(f"POST {path}: {e}")
            return {}

    # Balance — método probado (v5)
    async def get_balance(self) -> float:
        for path in ["/openApi/swap/v2/user/balance",
                     "/openApi/swap/v3/user/balance"]:
            data = await self._get(path, auth=True)
            if data.get("code") != 0:
                continue
            d = data.get("data", {})
            if isinstance(d, dict):
                bal = d.get("balance", {})
                if isinstance(bal, dict):
                    for f in ["availableMargin","available","equity","freeMargin"]:
                        v = bal.get(f)
                        if v is not None and float(v) > 0:
                            log.info(f"✅ Balance: {f} = {v}")
                            return float(v)
                for f in ["availableMargin","available","equity"]:
                    v = d.get(f)
                    if v is not None and float(v) > 0:
                        return float(v)
            if isinstance(d, list):
                for item in d:
                    if isinstance(item, dict) and item.get("asset","") in ("USDT",""):
                        for f in ["availableMargin","available"]:
                            v = item.get(f)
                            if v is not None and float(v) > 0:
                                return float(v)

        log.error("❌ Balance=0. Transfiere USDT a Futuros Perpetuos en BingX.")
        log.error("   O añade env var: BALANCE_OVERRIDE=<tu_saldo>")
        ov = float(os.getenv("BALANCE_OVERRIDE", "0"))
        if ov > 0:
            log.warning(f"⚠️  Usando BALANCE_OVERRIDE={ov}")
            return ov
        return 0.0

    async def get_symbols(self) -> list[str]:
        data = await self._get("/openApi/swap/v2/quote/contracts")
        if data.get("code") == 0:
            return [s["symbol"] for s in data.get("data",[])
                    if s.get("symbol","").endswith("-USDT") and s.get("status",0)==1]
        return []

    async def klines(self, symbol: str, interval: str, limit=250) -> list[dict]:
        if self._fail[symbol] >= 3:
            return []
        try:
            async with asyncio.timeout(8.0):
                data = await self._get("/openApi/swap/v3/quote/klines", {
                    "symbol": symbol, "interval": interval, "limit": limit,
                })
        except asyncio.TimeoutError:
            self._fail[symbol] += 1
            return []
        if data.get("code") != 0:
            self._fail[symbol] += 1
            return []
        self._fail[symbol] = 0
        out = []
        for k in data.get("data", []):
            try:
                out.append({"t":int(k[0]),"o":float(k[1]),"h":float(k[2]),
                            "l":float(k[3]),"c":float(k[4]),"v":float(k[5])})
            except Exception:
                continue
        return sorted(out, key=lambda x: x["t"])

    async def positions(self) -> list[dict]:
        data = await self._get("/openApi/swap/v2/user/positions", auth=True)
        if data.get("code") == 0:
            return [p for p in data.get("data",[]) if float(p.get("positionAmt",0))!=0]
        return []

    async def set_leverage(self, symbol: str):
        if symbol in self._lev_cache:
            return
        await asyncio.gather(
            self._post("/openApi/swap/v2/trade/leverage",
                       {"symbol":symbol,"side":"LONG","leverage":LEVERAGE}),
            self._post("/openApi/swap/v2/trade/leverage",
                       {"symbol":symbol,"side":"SHORT","leverage":LEVERAGE}),
        )
        self._lev_cache.add(symbol)

    async def place_order(self, symbol:str, side:str, qty:float,
                          sl:float, tp1:float, tp2:float) -> bool:
        pos  = "LONG"  if side=="BUY"  else "SHORT"
        clos = "SELL"  if side=="BUY"  else "BUY"
        await self.set_leverage(symbol)
        res = await self._post("/openApi/swap/v2/trade/order", {
            "symbol":symbol,"side":side,"positionSide":pos,
            "type":"MARKET","quantity":round(qty,4),
        })
        if res.get("code") != 0:
            log.error(f"Orden fallida {symbol}: {res.get('code')} {res.get('msg','')}")
            return False
        await asyncio.gather(
            self._post("/openApi/swap/v2/trade/order", {
                "symbol":symbol,"side":clos,"positionSide":pos,
                "type":"STOP_MARKET","stopPrice":round(sl,8),
                "closePosition":"true","workingType":"MARK_PRICE",
            }),
            self._post("/openApi/swap/v2/trade/order", {
                "symbol":symbol,"side":clos,"positionSide":pos,
                "type":"TAKE_PROFIT_MARKET","stopPrice":round(tp1,8),
                "quantity":round(qty*TP1_SIZE,4),"workingType":"MARK_PRICE",
            }),
            self._post("/openApi/swap/v2/trade/order", {
                "symbol":symbol,"side":clos,"positionSide":pos,
                "type":"TAKE_PROFIT_MARKET","stopPrice":round(tp2,8),
                "quantity":round(qty*TP2_SIZE,4),"workingType":"MARK_PRICE",
            }),
            return_exceptions=True,
        )
        return True

    async def close(self):
        if self._http and not self._http.is_closed:
            await self._http.aclose()


# ══════════════════════════════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════════════════════════════
async def tg(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT: return
    try:
        async with httpx.AsyncClient(timeout=8) as c:
            await c.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id":TELEGRAM_CHAT,"text":msg,"parse_mode":"HTML"},
            )
    except Exception as e:
        log.debug(f"TG: {e}")


# ══════════════════════════════════════════════════════════════════════
#  CACHE
# ══════════════════════════════════════════════════════════════════════
class Cache:
    def __init__(self, ttl=25):
        self.ttl=ttl; self._d:dict={}; self._t:dict={}
    def get(self, k): return self._d[k] if k in self._d and time.time()-self._t.get(k,0)<self.ttl else None
    def set(self, k, v): self._d[k]=v; self._t[k]=time.time()


# ══════════════════════════════════════════════════════════════════════
#  BOT PRINCIPAL v6
# ══════════════════════════════════════════════════════════════════════
class PhantomEdge:

    def __init__(self):
        self.api      = BingXClient()
        self.cache    = Cache(25)
        self.c5: dict = {}
        self.c15: dict= {}
        self.warm: set= set()
        self.symbols: list = []
        self.open_pos: dict= {}
        self.balance   = 0.0
        self.cycle     = 0
        self.daily_trades = 0
        self.daily_loss   = 0.0
        self.last_day  = datetime.now(timezone.utc).date()
        self.t0        = time.time()
        self.kill      = False
        self.total_sig = 0

    async def warmup_one(self, sym: str) -> bool:
        # Necesitamos más velas para HMA+FutureTrend (pine requiere ft_period×3 mínimo)
        min_5m  = max(HMA_LEN + 20, FT_PERIOD * 3 + 20, PIVOT_LEN * 6)
        min_15m = max(HMA_LEN + 10, FT_PERIOD * 2 + 10)
        try:
            k5, k15 = await asyncio.gather(
                self.api.klines(sym, TIMEFRAME, max(250, min_5m + 50)),
                self.api.klines(sym, TIMEFRAME_SLOW, max(150, min_15m + 30)),
                return_exceptions=True,
            )
            if (isinstance(k5, list) and isinstance(k15, list)
                    and len(k5) >= min_5m and len(k15) >= min_15m):
                self.c5[sym]  = k5
                self.c15[sym] = k15
                self.warm.add(sym)
                return True
        except Exception:
            pass
        return False

    async def warmup_batch(self, syms: list, conc=25):
        done=0; total=len(syms)
        for i in range(0, total, conc):
            res = await asyncio.gather(*[self.warmup_one(s) for s in syms[i:i+conc]], return_exceptions=True)
            done += sum(1 for r in res if r is True)
            log.info(f"  WarmUp {done}/{total}...")
            await asyncio.sleep(0.15)
        return done

    async def update(self, sym: str):
        k5key=f"{sym}:5"; k15key=f"{sym}:15"
        n5  = self.cache.get(k5key) is None
        n15 = self.cache.get(k15key) is None
        if not n5 and not n15: return

        tasks = []
        if n5:  tasks.append(self.api.klines(sym, TIMEFRAME, 3))
        if n15: tasks.append(self.api.klines(sym, TIMEFRAME_SLOW, 3))

        res = await asyncio.gather(*tasks, return_exceptions=True)
        idx = 0

        def mrg(ex, nc, mx):
            if not isinstance(nc,list) or not nc: return ex
            lt = ex[-1]["t"] if ex else 0
            for x in nc:
                if x["t"] > lt: ex.append(x)
                elif ex and x["t"] == ex[-1]["t"]: ex[-1] = x
            return ex[-mx:]

        if n5:
            r=res[idx]; idx+=1
            if isinstance(r,list):
                self.c5[sym] = mrg(self.c5.get(sym,[]), r, 300)
                self.cache.set(k5key, self.c5[sym])
        if n15:
            r=res[idx]
            if isinstance(r,list):
                self.c15[sym] = mrg(self.c15.get(sym,[]), r, 200)
                self.cache.set(k15key, self.c15[sym])

    def reset_daily(self):
        today = datetime.now(timezone.utc).date()
        if today != self.last_day:
            self.daily_trades=0; self.daily_loss=0.0
            self.last_day=today; self.kill=False
            log.info("📅 Reseteado")

    def can_trade(self) -> tuple[bool,str]:
        if self.kill: return False,"KILL"
        if len(self.open_pos) >= MAX_POSITIONS: return False,f"MAX_POS"
        if self.daily_trades >= MAX_DAILY_TRADES: return False,"MAX_TRADES"
        if self.daily_loss >= MAX_DAILY_LOSS: self.kill=True; return False,"MAX_LOSS→KILL"
        if self.balance < TRADE_USDT*0.5:
            return False,(f"BALANCE={self.balance:.2f}U mínimo={TRADE_USDT*0.5:.1f}U → "
                          f"Transfiere USDT a Futuros en BingX o usa BALANCE_OVERRIDE")
        return True,"OK"

    async def scan(self):
        self.cycle += 1
        self.reset_daily()

        self.balance, pos_raw = await asyncio.gather(
            self.api.get_balance(), self.api.positions()
        )
        self.open_pos = {p["symbol"]:p for p in pos_raw}

        log.info(
            f"[C{self.cycle:04d}] Bal:{self.balance:.2f}U | "
            f"Pos:{len(self.open_pos)}/{MAX_POSITIONS} | "
            f"Warm:{len(self.warm)}/{len(self.symbols)} | "
            f"SigTotal:{self.total_sig}"
        )

        ok, reason = self.can_trade()
        if not ok:
            log.info(f"  ⛔ {reason}")
            return

        prio  = [s for s in PRIORITY if s in self.warm and s not in self.open_pos]
        resto = [s for s in self.warm if s not in self.open_pos and s not in prio]
        cands = prio + resto
        if not cands: return

        for i in range(0, min(len(cands), 60), 30):
            await asyncio.gather(*[self.update(s) for s in cands[i:i+30]], return_exceptions=True)

        sigs = 0
        for sym in cands:
            if len(self.open_pos) >= MAX_POSITIONS: break

            sig = analyze(self.c5.get(sym,[]), self.c15.get(sym,[]))
            if sig is None: continue

            sigs += 1; self.total_sig += 1
            e = "🟢" if sig["side"]=="BUY" else "🔴"
            zt = "⚡ZZ!" if sig["zz_triggered"] else ""
            pt = "⭐" if sym in PRIORITY else ""
            log.info(
                f"  {e}{pt}{zt} {sym} {sig['side']} "
                f"Score:{sig['score']}/6 | {' '.join(sig['reasons'])} | "
                f"FT:{sig['ft']:.0f} HMA:{sig['hma']:.5g}"
            )

            if AUTO_TRADING:
                entry   = sig["entry"]
                sl_dist = abs(entry - sig["sl"])
                rp      = sl_dist / entry if entry > 0 else 0
                if rp < 0.0005: continue

                qty = round((TRADE_USDT / rp) / entry, 4)
                if qty <= 0: continue

                t0 = time.time()
                ok2 = await self.api.place_order(
                    sym, sig["side"], qty,
                    sig["sl"], sig["tp1"], sig["tp2"]
                )
                ms = int((time.time()-t0)*1000)

                if ok2:
                    self.daily_trades += 1
                    self.open_pos[sym] = {"symbol":sym,"side":sig["side"]}
                    slp  = sl_dist/entry*100
                    tp1p = abs(sig["tp1"]-entry)/entry*100
                    tp2p = abs(sig["tp2"]-entry)/entry*100
                    rr   = tp1p/slp if slp > 0 else 0
                    msg = (
                        f"{e} <b>{sym}</b>{pt} — "
                        f"{'LONG' if sig['side']=='BUY' else 'SHORT'}\n"
                        f"📊 Score: {sig['score']}/6"
                        f"{' | ⚡ ZigZag Breakout!' if sig['zz_triggered'] else ''}\n"
                        f"📍 Entry:  {entry:.6g}\n"
                        f"🛡 SL:    {sig['sl']:.6g}  (-{slp:.2f}%)\n"
                        f"🎯 TP1:   {sig['tp1']:.6g}  35%@1R (+{tp1p:.2f}%)\n"
                        f"🎯 TP2:   {sig['tp2']:.6g}  35%@2R (+{tp2p:.2f}%)\n"
                        f"🔄 Trail: 30% restante\n"
                        f"📐 RR:    1:{rr:.1f}\n"
                        f"🌊 FT: {sig['ft']:+.0f} | HMA: {sig['hma']:.5g} | "
                        f"ATR: {sig['atr_pct']:.2f}%\n"
                        f"✨ {' · '.join(sig['reasons'])}\n"
                        f"💰 Bal: {self.balance:.2f}U | ⚡ {ms}ms"
                    )
                    await tg(msg)
                    log.info(f"  ✅ {sym} qty={qty} {ms}ms")
            else:
                entry   = sig["entry"]
                sl_dist = abs(entry-sig["sl"])
                slp     = sl_dist/entry*100
                tp1p    = abs(sig["tp1"]-entry)/entry*100
                msg = (
                    f"🔔 <b>[SIM] {sym}</b>{pt} — "
                    f"{'LONG' if sig['side']=='BUY' else 'SHORT'}"
                    f" Score:{sig['score']}/6\n"
                    f"{'⚡ ZigZag Breakout confirmed!' if sig['zz_triggered'] else ''}\n"
                    f"Entry:{entry:.6g} | SL:{sig['sl']:.6g}(-{slp:.2f}%)\n"
                    f"TP1:{sig['tp1']:.6g}(+{tp1p:.2f}%) | TP2:{sig['tp2']:.6g}\n"
                    f"FT:{sig['ft']:+.0f} HMA:{sig['hma']:.5g} ATR:{sig['atr_pct']:.2f}%\n"
                    f"✨ {' · '.join(sig['reasons'])}"
                )
                await tg(msg)

        log.info(f"  Cands:{len(cands)} | Señales:{sigs} | Pos:{len(self.open_pos)}")

    async def run(self):
        log.info("═"*65)
        log.info("  PHANTOM EDGE v6.0 — ZigZag + HMA + Future-Trend")
        log.info(f"  Indicador base: Pine Script 'ZigZag+HMA Rápida+Future-Trend'")
        log.info(f"  Pivot:{PIVOT_LEN} | HMA:{HMA_LEN} | FT:{FT_PERIOD} | "
                 f"Score min:{MIN_SCORE}/6")
        log.info(f"  TP:{ATR_TP1}R/{ATR_TP2}R (ATR) | SL:{ATR_SL}R | Lev:x{LEVERAGE}")
        log.info(f"  Mode: {'AUTO-TRADING ✅' if AUTO_TRADING else 'SIMULACIÓN 🟡'}")
        log.info("═"*65)

        self.balance = await self.api.get_balance()
        log.info(f"💰 Balance: {self.balance:.2f} USDT")

        self.symbols = await self.api.get_symbols()
        log.info(f"📊 {len(self.symbols)} pares USDT-Perp")
        if not self.symbols:
            log.error("❌ Sin símbolos"); return

        await tg(
            f"🤖 <b>Phantom Edge v6.0</b>\n"
            f"📐 ZigZag(pivot={PIVOT_LEN}) + HMA({HMA_LEN}) + FutureTrend({FT_PERIOD})\n"
            f"💰 Balance: {self.balance:.2f} USDT\n"
            f"📊 {len(self.symbols)} pares | Score≥{MIN_SCORE}/6\n"
            f"🎯 TP: {ATR_TP1}R/{ATR_TP2}R | SL: {ATR_SL}R | Lev: x{LEVERAGE}\n"
            f"{'🟢 AUTO-TRADING' if AUTO_TRADING else '🟡 SIMULACIÓN'}"
            + (f"\n⚠️ BALANCE=0 → Transfiere USDT a Futuros en BingX"
               if self.balance==0 and API_KEY else "")
        )

        prio_ok = [s for s in PRIORITY if s in self.symbols]
        rest    = [s for s in self.symbols if s not in PRIORITY]

        log.info(f"🔥 WarmUp prio ({len(prio_ok)} pares)...")
        await self.warmup_batch(prio_ok, conc=min(len(prio_ok)+1, 25))
        log.info(f"🔥 WarmUp general...")
        await self.warmup_batch(rest[:200], conc=25)
        if len(rest) > 200:
            asyncio.create_task(self.warmup_batch(rest[200:], conc=15))

        log.info(f"✅ {len(self.warm)} pares listos")

        while True:
            try:
                t0=time.time()
                await self.scan()
                elapsed=time.time()-t0
                sl=max(5.0, SCAN_INTERVAL-elapsed)
                log.info(f"  ⏱ {elapsed:.1f}s | próximo:{sl:.0f}s\n")
                await asyncio.sleep(sl)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"❌ {e}", exc_info=True)
                await asyncio.sleep(10)

        await self.api.close()


# ══════════════════════════════════════════════════════════════════════
#  HEALTH CHECK
# ══════════════════════════════════════════════════════════════════════
async def health_server(bot: PhantomEdge, port: int):
    from aiohttp import web
    async def h(req):
        ok, reason = bot.can_trade()
        return web.json_response({
            "v": "6.0", "strategy": "ZigZag+HMA+FutureTrend",
            "status": "kill" if bot.kill else ("trading" if ok else "blocked"),
            "block_reason": None if ok else reason,
            "uptime_min": round((time.time()-bot.t0)/60,1),
            "cycle": bot.cycle, "balance_usdt": round(bot.balance,2),
            "warm": len(bot.warm), "symbols": len(bot.symbols),
            "open_pos": len(bot.open_pos),
            "daily_trades": bot.daily_trades,
            "total_signals": bot.total_sig,
            "auto_trading": AUTO_TRADING,
            "params": {
                "pivot_len": PIVOT_LEN, "hma_len": HMA_LEN,
                "ft_period": FT_PERIOD, "min_score": f"{MIN_SCORE}/6",
                "atr_sl": ATR_SL, "atr_tp1": ATR_TP1, "atr_tp2": ATR_TP2,
            }
        })
    app=web.Application()
    app.router.add_get("/",h); app.router.add_get("/health",h)
    runner=web.AppRunner(app); await runner.setup()
    await web.TCPSite(runner,"0.0.0.0",port).start()
    log.info(f"🌐 http://0.0.0.0:{port}")


async def main():
    bot = PhantomEdge()
    await asyncio.gather(health_server(bot,PORT), bot.run())

if __name__ == "__main__":
    asyncio.run(main())
