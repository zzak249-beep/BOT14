"""utils.py — helpers para renewed-love EMA9×VWAP scanner."""

from datetime import datetime, timezone
import config


def _normalize(s: str) -> str:
    return (
        s.upper()
        .replace("(", "").replace(")", "")
        .replace("-", "").replace("_", "")
        .removesuffix("USDT")
    )


def is_blacklisted(symbol: str) -> bool:
    sym_base = _normalize(symbol)
    for bl in config.BLACKLIST:
        bl_base = _normalize(bl)
        if bl_base and (sym_base == bl_base or sym_base.startswith(bl_base)):
            return True
    return False


def in_trading_session() -> bool:
    now = datetime.now(tz=timezone.utc)
    h = now.hour + now.minute / 60.0
    return config.TRADE_START_UTC <= h < config.TRADE_END_UTC


def utc_hour() -> float:
    now = datetime.now(tz=timezone.utc)
    return now.hour + now.minute / 60.0


def base_symbol(symbol: str) -> str:
    return _normalize(symbol)


def count_direction(positions: list, side: str) -> int:
    return sum(1 for p in positions if p.get("positionSide") == side)


def format_price(price: float) -> str:
    if price >= 1000:
        return f"{price:.2f}"
    if price >= 1:
        return f"{price:.4f}"
    return f"{price:.6f}"
