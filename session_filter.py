"""
QF×JP Bot v6.0 — session_filter.py
Sesiones: NY (13-22 UTC), LDN (08-17 UTC), ASIA (00-09 UTC)
"""
from datetime import datetime, timezone
from config import cfg


SESSION_HOURS = {
    "NY":   (13, 22),
    "LDN":  (8,  17),
    "ASIA": (0,   9),
}


class SessionFilter:
    def is_tradeable(self) -> bool:
        if not cfg.ALLOWED_SESSIONS:
            return True
        h = datetime.now(timezone.utc).hour
        for sess in cfg.ALLOWED_SESSIONS:
            start, end = SESSION_HOURS.get(sess, (0, 24))
            if start <= h < end:
                return True
        return False

    def current_session(self) -> str:
        h = datetime.now(timezone.utc).hour
        for sess, (start, end) in SESSION_HOURS.items():
            if start <= h < end:
                return sess
        return "OFF"
