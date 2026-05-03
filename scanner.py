# -*- coding: utf-8 -*-
"""scanner.py -- Phantom Edge Bot v6.1 TURBO — Fixed Scanner.

ROOT CAUSE FIX: When 558 symbols all cold-fetch simultaneously,
BingX rate-limits and returns empty/error responses.
store() sees len(raw)<50 → returns False → cache never warms → infinite cold loop.

FIXES:
  1. Cold fetch: max_concurrent=8 (slow but reliable, runs once)
  2. Warm fetch: max_concurrent=30 (fast, only 2 bars each)
  3. fetch_klines retries 3x with backoff on empty response
  4. Debug logging shows actual BingX response on failure
  5. Separate warmup phase from live scan
"""
from __future__ import annotations
import asyncio
import numpy as np
from loguru import logger
from client import fetch_klines as _fetch_klines_base, _get


# ── fetch_klines with retry ───────────────────────────────────
async def fetch_klines_retry(symbol: str, interval: str,
                              limit: int = 200, retries: int = 3) -> list:
    """fetch_klines with retry + backoff. Logs on failure."""
    for attempt in range(retries):
        raw = await _fetch_klines_base(symbol, interval, limit)
        if len(raw) >= 10:
            return raw
        if attempt < retries - 1:
            wait = 1.0 * (attempt + 1)
            logger.debug(f"[RETRY] {symbol} {interval} got {len(raw)} bars, retry in {wait:.1f}s")
            await asyncio.sleep(wait)
        else:
            logger.debug(f"[FAIL] {symbol} {interval} returned {len(raw)} bars after {retries} tries")
    return []


# ── Symbol loader ─────────────────────────────────────────────
async def fetch_all_bingx_symbols() -> list[str]:
    resp = await _get("/openApi/swap/v2/quote/contracts")
    out  = []
    try:
        for item in resp.get("data", []):
            sym = item.get("symbol", "")
            if not sym.endswith("-USDT"): continue
            if str(item.get("status","1")) not in ("1","TRADING"): continue
            if any(x in sym for x in ("1000","DEFI","INDEX","BEAR","BULL")): continue
            out.append(sym)
    except Exception as e:
        logger.warning(f"[SCANNER] symbols error: {e}")
    logger.info(f"[SCANNER] {len(out)} pares USDT perpetuos en BingX")
    return sorted(out)


async def get_symbols(raw: str) -> list[str]:
    if raw.strip().upper() in ("ALL", ""):
        syms = await fetch_all_bingx_symbols()
        return syms or [
            "BTC-USDT","ETH-USDT","SOL-USDT","BNB-USDT","XRP-USDT",
            "DOGE-USDT","ADA-USDT","AVAX-USDT","LINK-USDT","ARB-USDT",
            "OP-USDT","NEAR-USDT","APT-USDT","SUI-USDT","PEPE-USDT",
        ]
    return [s.strip() for s in raw.split(",") if s.strip()]


# ── OHLCV Ring-Buffer Cache ───────────────────────────────────
class _SymCache:
    __slots__ = ("open","high","low","close","volume","last_ts","warm")
    SIZE = 220

    def __init__(self):
        self.open = self.high = self.low = self.close = self.volume = None
        self.last_ts: float = -1.
        self.warm: bool = False

    def store(self, raw: list) -> bool:
        if len(raw) < 50:
            return False
        try:
            r = raw[-self.SIZE:]
            self.open   = np.array([float(c[1]) for c in r], np.float64)
            self.high   = np.array([float(c[2]) for c in r], np.float64)
            self.low    = np.array([float(c[3]) for c in r], np.float64)
            self.close  = np.array([float(c[4]) for c in r], np.float64)
            self.volume = np.array([float(c[5]) for c in r], np.float64)
            self.last_ts = float(r[-1][0])
            self.warm = True
            return True
        except Exception as e:
            logger.debug(f"store() error: {e}")
            return False

    def update(self, raw: list) -> bool:
        if not self.warm or not raw:
            return self.store(raw)
        try:
            new = [c for c in raw if float(c[0]) > self.last_ts]
            if not new:
                return True
            k = len(new)
            def _app(arr, col):
                v = np.array([float(r[col]) for r in new], np.float64)
                return np.concatenate([arr[k:], v])
            self.open   = _app(self.open,   1)
            self.high   = _app(self.high,   2)
            self.low    = _app(self.low,    3)
            self.close  = _app(self.close,  4)
            self.volume = _app(self.volume, 5)
            self.last_ts = float(new[-1][0])
            return True
        except Exception:
            return self.store(raw)

    def to_dict(self) -> dict | None:
        if not self.warm: return None
        return {"open":self.open,"high":self.high,"low":self.low,
                "close":self.close,"volume":self.volume}

    def quick_vol_ratio(self) -> float:
        if not self.warm or self.volume is None or len(self.volume) < 22:
            return 1.
        avg = float(np.mean(self.volume[-22:-2]))
        return float(self.volume[-2]) / avg if avg > 0 else 0.


_cache: dict[tuple, _SymCache] = {}

def _get_cache(sym: str, tf: str) -> _SymCache:
    k = (sym, tf)
    if k not in _cache:
        _cache[k] = _SymCache()
    return _cache[k]


# ── Leverage cache ────────────────────────────────────────────
_lev_cache: dict[str, int] = {}

def leverage_already_set(sym: str, lev: int) -> bool:
    return _lev_cache.get(sym) == lev

def mark_leverage_set(sym: str, lev: int) -> None:
    _lev_cache[sym] = lev


# ── Warmup: cold-fetch all symbols (SLOW but reliable) ────────
async def warmup_all(
    symbols: list[str],
    tf_5m:   str = "5m",
    tf_15m:  str = "15m",
    batch:   int = 8,       # LOW concurrency for cold — avoids rate limits
) -> int:
    """
    Full 200-bar fetch for all symbols. Run once on startup.
    Uses low concurrency (8) to avoid BingX rate limits.
    Returns number of symbols successfully warmed.
    """
    total   = len(symbols)
    warmed  = 0
    sem     = asyncio.Semaphore(batch)

    async def _warm_one(sym: str) -> None:
        nonlocal warmed
        async with sem:
            c5  = _get_cache(sym, tf_5m)
            c15 = _get_cache(sym, tf_15m)
            if c5.warm and c15.warm:
                warmed += 1
                return
            raw5  = await fetch_klines_retry(sym, tf_5m,  220, retries=3)
            raw15 = await fetch_klines_retry(sym, tf_15m, 120, retries=3)
            ok5   = c5.store(raw5)
            ok15  = c15.store(raw15)
            if ok5 and ok15:
                warmed += 1

    # Process in chunks and log progress
    chunk = 50
    for i in range(0, total, chunk):
        batch_syms = symbols[i:i+chunk]
        await asyncio.gather(
            *[asyncio.create_task(_warm_one(s)) for s in batch_syms],
            return_exceptions=True,
        )
        logger.info(f"[WARMUP] {min(i+chunk, total)}/{total} procesados | {warmed} OK")
        await asyncio.sleep(0.5)   # brief pause between chunks

    logger.info(f"[WARMUP] Completo: {warmed}/{total} símbolos calentados")
    return warmed


# ── Live fetch: only new candles ──────────────────────────────
async def fetch_universe(
    symbols:        list[str],
    tf_5m:          str   = "5m",
    tf_15m:         str   = "15m",
    tf_1h:          str   = "1h",
    max_concurrent: int   = 30,    # HIGH concurrency for warm (2 bars only)
    min_vol_mult:   float = 0.6,
) -> dict[str, dict]:
    """
    Returns {symbol: {tf_5m: ohlcv, tf_15m: ohlcv}}

    WARM symbols: fetch only 2 new bars — fast path.
    COLD symbols: should be none after warmup_all() on startup.
                  If any remain (new symbols added), fetch full.
    """
    results: dict[str, dict] = {}
    sem  = asyncio.Semaphore(max_concurrent)
    stat = {"cold":0,"warm":0,"skip_vol":0,"ts_skip":0,"fail":0}

    async def _one(sym: str) -> None:
        async with sem:
            c5  = _get_cache(sym, tf_5m)
            c15 = _get_cache(sym, tf_15m)
            cold = not c5.warm or not c15.warm

            if cold:
                # Fallback cold fetch (should be rare after warmup)
                raw5  = await fetch_klines_retry(sym, tf_5m,  220, retries=2)
                raw15 = await fetch_klines_retry(sym, tf_15m, 120, retries=2)
                ok5   = c5.store(raw5)
                ok15  = c15.store(raw15)
                stat["cold"] += 1
                if not ok5 or not ok15:
                    stat["fail"] += 1
                    return
            else:
                # Fast path: only 2 new bars needed
                raw5 = await fetch_klines_retry(sym, tf_5m, 2, retries=2)
                if not raw5:
                    # Return cached data even if fetch failed
                    stat["ts_skip"] += 1
                    d5=c5.to_dict(); d15=c15.to_dict()
                    if d5 and d15: results[sym]={tf_5m:d5,tf_15m:d15}
                    return

                c5.update(raw5)
                stat["warm"] += 1

                # Volume pre-filter: skip 15m fetch for low-volume symbols
                vr = c5.quick_vol_ratio()
                if vr < min_vol_mult * 0.4:
                    stat["skip_vol"] += 1
                    d5=c5.to_dict(); d15=c15.to_dict()
                    if d5 and d15: results[sym]={tf_5m:d5,tf_15m:d15}
                    return

                raw15 = await fetch_klines_retry(sym, tf_15m, 2, retries=2)
                c15.update(raw15)

            d5=c5.to_dict(); d15=c15.to_dict()
            if d5 and d15:
                results[sym] = {tf_5m: d5, tf_15m: d15}

    await asyncio.gather(
        *[asyncio.create_task(_one(s)) for s in symbols],
        return_exceptions=True,
    )

    warm_count = sum(1 for s in symbols
                     if _get_cache(s, tf_5m).warm and _get_cache(s, tf_15m).warm)
    logger.info(
        f"[SCANNER] {len(results)}/{len(symbols)} map | "
        f"warm_total={warm_count} | "
        f"cold={stat['cold']} warm={stat['warm']} "
        f"vol_skip={stat['skip_vol']} ts_skip={stat['ts_skip']} fail={stat['fail']}"
    )
    return results
