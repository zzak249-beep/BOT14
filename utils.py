"""
Utils — retry decorator, rate limiter thread-safe, filtros y formatters.
"""
from __future__ import annotations
import functools
import logging
import math
import threading
import time
from typing import Tuple, Type

logger = logging.getLogger(__name__)

# ── Retry con backoff exponencial ─────────────────────────────
def retry(
    attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exc: Tuple[Type[Exception], ...] = (Exception,),
):
    """
    Decora una función para reintentar N veces con backoff exponencial.
    Devuelve None si todos los intentos fallan (no lanza excepción).
    """
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            d = delay
            for attempt in range(1, attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exc as e:
                    if attempt == attempts:
                        logger.warning(f"{fn.__name__} failed after {attempts} attempts: {e}")
                        return None
                    logger.debug(f"{fn.__name__} attempt {attempt} error: {e} — retry in {d:.1f}s")
                    time.sleep(d)
                    d *= backoff
        return wrapper
    return deco


# ── Rate limiter token-bucket thread-safe ─────────────────────
class RateLimiter:
    """
    Permite `max_calls` llamadas cada `period` segundos.
    Hilo-seguro: puede usarse con ThreadPoolExecutor.
    """
    def __init__(self, max_calls: int, period: float):
        self._max  = max_calls
        self._per  = period
        self._log: list[float] = []
        self._lock = threading.Lock()

    def acquire(self):
        with self._lock:
            now = time.monotonic()
            self._log = [t for t in self._log if now - t < self._per]
            if len(self._log) >= self._max:
                wait = self._per - (now - self._log[0]) + 0.02
                if wait > 0:
                    time.sleep(wait)
                now = time.monotonic()
                self._log = [t for t in self._log if now - t < self._per]
            self._log.append(time.monotonic())


# Instancias globales (compartidas por todos los threads)
market_rl  = RateLimiter(max_calls=80, period=10.0)   # BingX: 100/10s market
trading_rl = RateLimiter(max_calls=15, period=10.0)   # BingX: 20/10s trading


# ── Filtros de símbolo ─────────────────────────────────────────
_BLACKLIST_SUFFIX = {"3L","3S","5L","5S","10L","10S","UP","DOWN","BEAR","BULL"}
_BLACKLIST_FULL   = {"BUSDUSDT","TUSDUSDT","USDCUSDT","DAIUSDT","FDUSDT","EURUSDT","GBPUSDT"}

def is_blacklisted(symbol: str) -> bool:
    """True si el símbolo es un token apalancado, estable, etc."""
    base = symbol.upper().replace("-USDT", "").replace("USDT", "")
    if symbol.upper().replace("-", "") in _BLACKLIST_FULL:
        return True
    return any(base.endswith(p) for p in _BLACKLIST_SUFFIX)


# ── Helpers de intervalo ───────────────────────────────────────
_UNIT_SECS = {"m": 60, "h": 3600, "d": 86400, "w": 604800}

def interval_to_seconds(iv: str) -> int:
    return int(iv[:-1]) * _UNIT_SECS.get(iv[-1].lower(), 3600)

def interval_to_ms(iv: str) -> int:
    return interval_to_seconds(iv) * 1000


# ── Formatters ─────────────────────────────────────────────────
def fmt_price(p: float) -> str:
    if p is None: return "—"
    if p >= 1000:  return f"{p:,.2f}"
    if p >= 1:     return f"{p:.4f}"
    if p >= 0.001: return f"{p:.6f}"
    return f"{p:.8f}"

def fmt_pct(a: float, b: float) -> str:
    if not a: return "—"
    return f"{(b - a) / a * 100:+.2f}%"

def fmt_usdt(v: float) -> str:
    return f"{v:+.2f} USDT"

def esc(text) -> str:
    """Escapa HTML para Telegram."""
    return str(text).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")


# ── Precisión de cantidad ──────────────────────────────────────
def qty_precision(step: float) -> int:
    """Decimales necesarios para un step dado."""
    if step <= 0:
        return 6
    s = f"{step:.10f}".rstrip("0")
    return len(s.split(".")[1]) if "." in s else 0

def floor_qty(qty: float, step: float) -> float:
    """Redondea qty al múltiplo inferior de step."""
    if step <= 0:
        return round(qty, 6)
    prec = qty_precision(step)
    factor = 10 ** prec
    return math.floor(qty * factor) / factor
