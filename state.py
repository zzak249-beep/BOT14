"""
State v2 — persistencia JSON atómica, historial de trades, stats diarias.
"""
from __future__ import annotations
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)
_STATE_FILE = os.getenv("STATE_FILE", "state.json")
_TMP_FILE   = _STATE_FILE + ".tmp"
_MAX_HISTORY = 200   # trades guardados en historial


class BotState:
    def __init__(self):
        self._d: Dict[str, Any] = {}
        self._load()

    # ── IO atómica ────────────────────────────────────────────
    def _load(self):
        for path in (_STATE_FILE, _TMP_FILE):
            if os.path.exists(path):
                try:
                    with open(path) as f:
                        self._d = json.load(f)
                    logger.info(f"State loaded from {path} ({len(self._d)} keys)")
                    return
                except Exception as e:
                    logger.warning(f"State load {path}: {e}")
        self._d = {}

    def save(self):
        try:
            with open(_TMP_FILE, "w") as f:
                json.dump(self._d, f, separators=(",", ":"))
            os.replace(_TMP_FILE, _STATE_FILE)
        except Exception as e:
            logger.error(f"State save: {e}")

    def get(self, key: str, default: Any = None) -> Any:
        return self._d.get(key, default)

    def set(self, key: str, value: Any):
        self._d[key] = value
        self.save()

    # ── Candle tracking ───────────────────────────────────────
    def get_last_candle_ts(self, symbol: str) -> int:
        return int(self._d.get(f"ct_{symbol}", 0))

    def set_last_candle_ts(self, symbol: str, ts: int):
        self._d[f"ct_{symbol}"] = ts
        self.save()

    def is_new_candle(self, symbol: str, ts: int) -> bool:
        return ts > self.get_last_candle_ts(symbol)

    # ── Signal tracking ───────────────────────────────────────
    def get_last_signal_ts(self, symbol: str) -> int:
        return int(self._d.get(f"sig_ts_{symbol}", 0))

    def get_last_signal_action(self, symbol: str) -> Optional[str]:
        return self._d.get(f"sig_action_{symbol}")

    def set_last_signal(self, symbol: str, action: str, ts: int, price: float):
        self._d[f"sig_ts_{symbol}"]     = ts
        self._d[f"sig_action_{symbol}"] = action
        self._d[f"sig_price_{symbol}"]  = price
        self.save()

    # ── Active position tracking ──────────────────────────────
    def get_active_positions(self) -> Dict[str, dict]:
        return dict(self._d.get("active_positions", {}))

    def add_active_position(self, symbol: str, data: dict):
        ap = self._d.setdefault("active_positions", {})
        ap[symbol] = {**data, "open_ts": int(time.time())}
        self.save()

    def remove_active_position(self, symbol: str) -> Optional[dict]:
        ap   = self._d.get("active_positions", {})
        data = ap.pop(symbol, None)
        self._d["active_positions"] = ap
        self.save()
        return data

    def get_active_position(self, symbol: str) -> Optional[dict]:
        return self._d.get("active_positions", {}).get(symbol)

    # ── Trade history ─────────────────────────────────────────
    def add_trade(self, trade: dict):
        """Agrega un trade al historial circular (max _MAX_HISTORY)."""
        hist = self._d.setdefault("trade_history", [])
        hist.append({**trade, "ts": int(time.time())})
        if len(hist) > _MAX_HISTORY:
            hist[:] = hist[-_MAX_HISTORY:]
        self.save()

    def get_trade_history(self) -> List[dict]:
        return list(self._d.get("trade_history", []))

    def get_recent_trades(self, n: int = 10) -> List[dict]:
        return self.get_trade_history()[-n:]

    # ── Daily stats ───────────────────────────────────────────
    def _today(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def get_daily_stats(self) -> dict:
        return dict(self._d.get(f"daily_{self._today()}", {
            "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0,
            "start_balance": 0.0,
        }))

    def update_daily_result(self, pnl: float, won: bool, start_balance: float = 0.0):
        key   = f"daily_{self._today()}"
        stats = self._d.setdefault(key, {
            "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0,
            "start_balance": start_balance,
        })
        stats["trades"] += 1
        stats["pnl"]    += pnl
        if won: stats["wins"]   += 1
        else:   stats["losses"] += 1
        self.save()

    def get_daily_pnl(self) -> float:
        return float(self.get_daily_stats().get("pnl", 0.0))

    def get_last_summary_date(self) -> str:
        return str(self._d.get("last_summary_date", ""))

    def set_last_summary_date(self, date: str):
        self.set("last_summary_date", date)

    # ── Paper positions (dry run) ─────────────────────────────
    def get_paper_positions(self) -> Dict[str, dict]:
        return dict(self._d.get("paper_positions", {}))

    def add_paper_position(self, symbol: str, data: dict):
        pp = self._d.setdefault("paper_positions", {})
        pp[symbol] = {**data, "open_ts": int(time.time())}
        self.save()

    def remove_paper_position(self, symbol: str) -> Optional[dict]:
        pp   = self._d.get("paper_positions", {})
        data = pp.pop(symbol, None)
        self._d["paper_positions"] = pp
        self.save()
        return data

    def get_paper_position(self, symbol: str) -> Optional[dict]:
        return self._d.get("paper_positions", {}).get(symbol)

    # ── Re-entry cooldown ─────────────────────────────────────
    def get_last_exit_ts(self, symbol: str) -> int:
        return int(self._d.get(f"exit_ts_{symbol}", 0))

    def set_last_exit(self, symbol: str):
        self.set(f"exit_ts_{symbol}", int(time.time()))


state = BotState()
