"""
config.py

All runtime configuration comes from environment variables so the bot needs
zero code changes between local testing and Railway (Railway injects env
vars directly; python-dotenv is used only so a local .env file works the
same way when running outside Railway).
"""

import os

from dotenv import load_dotenv

load_dotenv()


def _get_bool(name, default):
    val = os.getenv(name)
    if val is None or val == "":
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


def _get_float(name, default):
    val = os.getenv(name)
    return float(val) if val not in (None, "") else default


def _get_int(name, default):
    val = os.getenv(name)
    return int(val) if val not in (None, "") else default


class Config:
    def __init__(self):
        # --- BingX ---
        self.api_key = os.getenv("BINGX_API_KEY", "")
        self.api_secret = os.getenv("BINGX_API_SECRET", "")
        self.base_url = os.getenv("BINGX_BASE_URL", "https://open-api.bingx.com")

        # --- Market / strategy (defaults mirror the Pine script inputs) ---
        self.symbols = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTC-USDT").split(",") if s.strip()]
        self.timeframe = os.getenv("TIMEFRAME", "15m")

        self.rsi_length = _get_int("RSI_LENGTH", 10)
        self.rsi_signal_length = _get_int("RSI_SIGNAL_LENGTH", 10)
        self.trigger_level = _get_float("RSI_TRIGGER_LEVEL", 50.0)
        self.target_cross_count = _get_int("TARGET_CROSS_COUNT", 2)

        self.st_atr_period = _get_int("SUPERTREND_ATR_PERIOD", 10)
        self.st_factor = _get_float("SUPERTREND_FACTOR", 2.5)

        # --- Risk / sizing (the Pine backtest used 100% of equity per trade -
        # NOT safe for live leveraged capital, so this defaults to a
        # risk-based size instead; see README) ---
        self.leverage = _get_int("LEVERAGE", 5)
        self.margin_mode = os.getenv("MARGIN_MODE", "ISOLATED").upper()
        self.position_sizing_mode = os.getenv("POSITION_SIZING_MODE", "RISK_PERCENT").upper()
        self.risk_percent_equity = _get_float("RISK_PERCENT_EQUITY", 2.0)
        self.fixed_margin_usdt = _get_float("FIXED_MARGIN_USDT", 50.0)
        self.stop_loss_pct = _get_float("STOP_LOSS_PCT", 0.0)  # 0 disables the safety stop
        self.quantity_precision = _get_int("QUANTITY_PRECISION", 3)
        self.price_precision = _get_int("PRICE_PRECISION", 2)

        # --- Runtime ---
        self.poll_interval_seconds = _get_int("POLL_INTERVAL_SECONDS", 30)
        self.klines_lookback = _get_int("KLINES_LOOKBACK", 500)
        self.recv_window_ms = _get_int("RECV_WINDOW_MS", 5000)
        self.dry_run = _get_bool("DRY_RUN", True)
        self.state_file_path = os.getenv("STATE_FILE_PATH", "./data/state.json")
        self.log_level = os.getenv("LOG_LEVEL", "INFO").upper()

        # --- Telegram ---
        self.telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")

    def validate(self):
        errors = []
        if not self.dry_run:
            if not self.api_key:
                errors.append("BINGX_API_KEY is required when DRY_RUN=false")
            if not self.api_secret:
                errors.append("BINGX_API_SECRET is required when DRY_RUN=false")
        if not self.symbols:
            errors.append("SYMBOLS must contain at least one symbol (e.g. BTC-USDT)")
        if self.position_sizing_mode not in ("RISK_PERCENT", "FIXED_MARGIN"):
            errors.append("POSITION_SIZING_MODE must be RISK_PERCENT or FIXED_MARGIN")
        if self.target_cross_count < 1:
            errors.append("TARGET_CROSS_COUNT must be >= 1")
        if errors:
            raise ValueError("Configuration error(s):\n- " + "\n- ".join(errors))
