"""core/config.py — All configuration via environment variables."""
from __future__ import annotations
import os
from typing import List, Set
from pydantic import field_validator
from pydantic_settings import BaseSettings


class Config(BaseSettings):
    # ── Exchange ──────────────────────────────────────────────────────────
    bingx_api_key:    str = ""
    bingx_secret_key: str = ""

    # ── Telegram ──────────────────────────────────────────────────────────
    telegram_token:   str = ""
    telegram_chat_id: str = ""

    # ── Strategy ──────────────────────────────────────────────────────────
    timeframe:       str   = "15m"
    confirm_tf:      str   = "1h"
    trend_tf:        str   = "4h"
    period:          int   = 25
    adx_len:         int   = 14
    di_len:          int   = 14
    adx_thresh:      float = 28.0
    rsi_len:         int   = 14
    rsi_ob:          float = 72.0
    rsi_os:          float = 28.0
    vol_spike_mult:  float = 1.8
    min_confidence:  float = 55.0

    # ── Universe ──────────────────────────────────────────────────────────
    min_volume_usdt: float      = 3_000_000.0
    top_n_symbols:   int        = 60
    # Parsed from comma-separated env var, e.g. "LUNA-USDT,FTT-USDT"
    blacklist:       List[str]  = []

    # ── Risk ─────────────────────────────────────────────────────────────
    leverage:              int   = 5
    risk_pct:              float = 1.0
    max_open_trades:       int   = 5
    sl_pct:                float = 2.0
    tp_pct:                float = 4.0
    trailing_sl:           bool  = True
    max_drawdown_pct:      float = 8.0
    daily_loss_limit:      float = 4.0
    max_consecutive_losses:int   = 5
    cooldown_after_loss:   int   = 300

    # ── Performance ───────────────────────────────────────────────────────
    scan_interval:  int = 8
    max_concurrent: int = 30
    http_timeout:   int = 6

    # ── Dashboard ─────────────────────────────────────────────────────────
    dashboard_enabled: bool = True
    dashboard_port:    int  = 8080

    model_config = {"env_file": ".env", "case_sensitive": False, "extra": "ignore"}

    @field_validator("blacklist", mode="before")
    @classmethod
    def _parse_blacklist(cls, v: object) -> List[str]:
        """Accept CSV string (from env) or a real list."""
        if isinstance(v, str):
            return [s.strip() for s in v.split(",") if s.strip()]
        if isinstance(v, (list, set, tuple)):
            return [str(x).strip() for x in v if str(x).strip()]
        return []

    @property
    def effective_port(self) -> int:
        """Railway injects PORT; fall back to dashboard_port."""
        return int(os.environ.get("PORT", self.dashboard_port))


cfg = Config()
