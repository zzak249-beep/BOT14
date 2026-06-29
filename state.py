"""
Persistent bot state — survives Railway restarts.
Stores: entry_time, tp1_hit, trail_stop per symbol/side.

Root cause fix: entry_time stored in RAM was wiped on every Railway
redeploy → MAX_HOLD_MINUTES never triggered → positions open 20-26h.
"""

import json
import logging
import os
import time

log = logging.getLogger("state")

_STATE_FILE = os.getenv("STATE_FILE", "/tmp/bot_state.json")


# ── Internal I/O ──────────────────────────────────────────────

def _load() -> dict:
    try:
        with open(_STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def _save(data: dict):
    try:
        with open(_STATE_FILE, "w") as f:
            json.dump(data, f)
    except Exception as e:
        log.error(f"state write error: {e}")


def _key(symbol: str, side: str, field: str) -> str:
    return f"{symbol}_{side}_{field}"


# ── Entry time ────────────────────────────────────────────────

def save_entry(symbol: str, side: str, ts: float = None):
    d = _load()
    d[_key(symbol, side, "entry_ts")] = ts or time.time()
    _save(d)
    log.debug(f"state.save_entry {symbol} {side}")


def get_entry_ts(symbol: str, side: str) -> float | None:
    v = _load().get(_key(symbol, side, "entry_ts"))
    return float(v) if v is not None else None


def is_max_hold_expired(symbol: str, side: str, max_minutes: int) -> bool:
    ts = get_entry_ts(symbol, side)
    if ts is None:
        return False
    elapsed = (time.time() - ts) / 60.0
    if elapsed >= max_minutes:
        log.info(f"MAX_HOLD expired {symbol} {side} | elapsed={elapsed:.0f}m limit={max_minutes}m")
        return True
    return False


# ── TP1 hit flag ──────────────────────────────────────────────

def set_tp1_hit(symbol: str, side: str, hit: bool = True):
    d = _load()
    d[_key(symbol, side, "tp1_hit")] = hit
    _save(d)


def is_tp1_hit(symbol: str, side: str) -> bool:
    return bool(_load().get(_key(symbol, side, "tp1_hit"), False))


# ── Trail stop ────────────────────────────────────────────────

def save_trail(symbol: str, side: str, stop: float):
    d = _load()
    d[_key(symbol, side, "trail")] = stop
    _save(d)


def get_trail(symbol: str, side: str) -> float | None:
    v = _load().get(_key(symbol, side, "trail"))
    return float(v) if v is not None else None


# ── Breakeven flag ────────────────────────────────────────────

def set_be_moved(symbol: str, side: str, moved: bool = True):
    d = _load()
    d[_key(symbol, side, "be_moved")] = moved
    _save(d)


def is_be_moved(symbol: str, side: str) -> bool:
    return bool(_load().get(_key(symbol, side, "be_moved"), False))


# ── Clear all state for a position ───────────────────────────

def clear(symbol: str, side: str):
    d = _load()
    prefix = f"{symbol}_{side}_"
    keys_to_del = [k for k in d if k.startswith(prefix)]
    for k in keys_to_del:
        del d[k]
    _save(d)
    log.debug(f"state.clear {symbol} {side} ({len(keys_to_del)} keys removed)")


# ── Debug dump ────────────────────────────────────────────────

def dump() -> dict:
    return _load()
