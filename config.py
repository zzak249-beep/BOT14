import os
from dotenv import load_dotenv
load_dotenv()

# ── BingX ──────────────────────────────────────────────────────
BINGX_API_KEY    = os.getenv("BINGX_API_KEY", "")
BINGX_SECRET_KEY = os.getenv("BINGX_SECRET_KEY", "")

# ── Telegram ───────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# ── Estrategia ─────────────────────────────────────────────────
TIMEFRAME        = os.getenv("TIMEFRAME", "1h")
SWING_LEN        = int(os.getenv("SWING_LEN", "10"))
COOLDOWN_BARS    = int(os.getenv("COOLDOWN_BARS", "5"))
MIN_CONFLUENCE   = float(os.getenv("MIN_CONFLUENCE", "0.0"))
ATR_FILTER       = os.getenv("ATR_FILTER", "true").lower() == "true"
ATR_MULT         = float(os.getenv("ATR_MULT", "0.5"))
EQ_TOL           = float(os.getenv("EQ_TOL", "0.1"))
CONFLUENCE_TOL   = float(os.getenv("CONFLUENCE_TOL", "0.3"))
STRICT_ENGULF    = os.getenv("STRICT_ENGULF", "true").lower() == "true"
VOLUME_CONFIRM   = os.getenv("VOLUME_CONFIRM", "false").lower() == "true"
CANDLES_LIMIT    = int(os.getenv("CANDLES_LIMIT", "200"))

# ── Risk management ────────────────────────────────────────────
SL_METHOD           = os.getenv("SL_METHOD", "STRUCTURE")   # ATR | STRUCTURE
TP_METHOD           = os.getenv("TP_METHOD", "FIB_TARGET")  # ATR | FIB_TARGET | FIB_HALF
SL_ATR_MULT         = float(os.getenv("SL_ATR_MULT", "1.5"))
TP_ATR_MULT         = float(os.getenv("TP_ATR_MULT", "2.5"))
RISK_PCT            = float(os.getenv("RISK_PCT", "1.0"))
MAX_POSITIONS       = int(os.getenv("MAX_POSITIONS", "3"))
LEVERAGE            = int(os.getenv("LEVERAGE", "5"))
MARGIN_TYPE         = os.getenv("MARGIN_TYPE", "ISOLATED")
MIN_RR              = float(os.getenv("MIN_RR", "1.5"))          # mínimo R:R para ejecutar
MAX_POSITION_PCT    = float(os.getenv("MAX_POSITION_PCT", "20.0"))  # máx % del balance en un trade
MAX_ENTRY_ATR_DIST  = float(os.getenv("MAX_ENTRY_ATR_DIST", "1.5")) # máx ATR entre señal y precio actual

# ── Protección de capital ──────────────────────────────────────
DAILY_LOSS_LIMIT_PCT   = float(os.getenv("DAILY_LOSS_LIMIT_PCT", "5.0"))  # % max pérdida diaria
REENTRY_COOLDOWN_BARS  = int(os.getenv("REENTRY_COOLDOWN_BARS", "20"))    # barras mínimas para re-entrar mismo par

# ── Scanner ────────────────────────────────────────────────────
MIN_VOLUME_USDT  = float(os.getenv("MIN_VOLUME_USDT", "5000000"))
MAX_SYMBOLS      = int(os.getenv("MAX_SYMBOLS", "80"))
SCAN_INTERVAL    = int(os.getenv("SCAN_INTERVAL", "60"))
FETCH_WORKERS    = int(os.getenv("FETCH_WORKERS", "10"))   # threads paralelos para fetch
REQUEST_DELAY    = float(os.getenv("REQUEST_DELAY", "0.15"))
BLACKLIST        = [s.strip().upper() for s in os.getenv("BLACKLIST", "").split(",") if s.strip()]

# ── Ejecución ──────────────────────────────────────────────────
DRY_RUN        = os.getenv("DRY_RUN", "true").lower() == "true"
LONG_ENABLED   = os.getenv("LONG_ENABLED", "true").lower() == "true"
SHORT_ENABLED  = os.getenv("SHORT_ENABLED", "true").lower() == "true"

# ── Misc ───────────────────────────────────────────────────────
LOG_LEVEL      = os.getenv("LOG_LEVEL", "INFO")
SUMMARY_HOUR   = int(os.getenv("SUMMARY_HOUR", "0"))   # hora UTC para resumen diario
HEALTH_PORT    = int(os.getenv("HEALTH_PORT", "8080"))  # puerto health check HTTP
