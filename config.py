import os


def _bool(k, d="false"):
    return os.getenv(k, d).strip().split("#")[0].strip().lower() in ("1", "true", "yes")

def _float(k, d):
    try:
        return float(os.getenv(k, str(d)).strip().split("#")[0].strip())
    except:
        return d

def _int(k, d):
    try:
        return int(os.getenv(k, str(d)).strip().split("#")[0].strip())
    except:
        return d

def _str(k, d=""):
    return os.getenv(k, d).strip().split("#")[0].strip()

def _list(k, d=""):
    v = _str(k, d)
    return [x.strip() for x in v.split(",") if x.strip()] if v else []


# ── Identity
BOT_NAME   = _str("BOT_NAME", "renewed-love")
SHORT_ONLY = _bool("SHORT_ONLY", "false")  # renewed-love trades LONG+SHORT

# ── BingX
API_KEY    = _str("BINGX_API_KEY")
SECRET_KEY = _str("BINGX_SECRET_KEY")
BASE_URL   = "https://open-api.bingx.com"

# ── Trading universe
TOP_N_SYMBOLS    = _int("TOP_N_SYMBOLS", 500)
BLACKLIST        = _list("BLACKLIST", "ESPORTS,STABLEUSDT,EURUSD,SILVER,SILVERXAG,OILWTI,OILBRENT,PAXG,CUSDT,SYN,GOLD,GOLDXAU,XAU,GASOLINE")
MIN_VOLUME_USDT  = _float("MIN_VOLUME_USDT", 50_000)

# ── Execution
LEVERAGE          = _int("LEVERAGE", 7)
FIXED_NOTIONAL_USDT  = _float("FIXED_NOTIONAL_USDT", 15.0)
MIN_NOTIONAL_USDT    = _float("MIN_NOTIONAL_USDT", 12.0)
MAX_NOTIONAL_USDT    = _float("MAX_NOTIONAL_USDT", 30.0)
MAX_OPEN_TRADES      = _int("MAX_OPEN_TRADES", 5)
MAX_DAILY_TRADES     = _int("MAX_DAILY_TRADES", 15)
DAILY_LOSS_PCT       = _float("DAILY_LOSS_PCT", 5.0)
CAPITAL              = _float("CAPITAL", 125.0)
MIN_MARGIN_USDT      = _float("MIN_MARGIN_USDT", 1.0)
LIMIT_ORDERS_ENABLED = _bool("LIMIT_ORDERS_ENABLED", "true")
LIMIT_TIMEOUT_SECS   = _int("LIMIT_TIMEOUT_SECS", 15)

# ── Timeframes
TIMEFRAME      = _str("TIMEFRAME", "3m")
HTF_TIMEFRAME  = _str("HTF_TIMEFRAME", "15m")
HTF2_TIMEFRAME = _str("HTF2_TIMEFRAME", "1h")
HTF5_TIMEFRAME = _str("HTF5_TIMEFRAME", "4h")

# ── Indicator periods
ATR_LEN    = _int("ATR_LEN", 14)
ADX_LEN    = _int("ADX_LEN", 14)
EMA9_PERIOD  = _int("EMA9_PERIOD", 9)
EMA21_PERIOD = _int("EMA21_PERIOD", 21)
MACD_FAST    = _int("MACD_FAST", 12)
MACD_SLOW    = _int("MACD_SLOW", 26)
MACD_SIGNAL  = _int("MACD_SIGNAL", 9)
RSI_PERIOD   = _int("RSI_PERIOD", 14)
RSI_MID      = _int("RSI_MID", 50)
RSI_OB       = _int("RSI_OB", 70)
RSI_OS       = _int("RSI_OS", 30)
VOL_MA_PERIOD = _int("VOL_MA_PERIOD", 20)

# ── Entry filters
CROSS_LOOKBACK       = _int("CROSS_LOOKBACK", 1)
ADX_MIN              = _int("ADX_MIN", 15)
ADX_TREND            = _int("ADX_TREND", 25)
VWAP_SLOPE_MIN_PCT   = _float("VWAP_SLOPE_MIN_PCT", 0.008)
MACD_REQUIRED        = _bool("MACD_REQUIRED", "true")
RSI_REQUIRED         = _bool("RSI_REQUIRED", "true")
VOL_REQUIRED         = _bool("VOL_REQUIRED", "true")
EMA21_REQUIRED       = _bool("EMA21_REQUIRED", "true")
HTF_FILTER_ENABLED   = _bool("HTF_FILTER_ENABLED", "true")
HTF_MIN_ALIGNED      = _int("HTF_MIN_ALIGNED", 1)
VOL_MIN_MULT         = _float("VOL_MIN_MULT", 1.1)
MIN_TIER             = _str("MIN_TIER", "STD")
REQUIRE_TL_BREAK     = _bool("REQUIRE_TL_BREAK", "false")
SLOPE_FILTER_ENABLED = _bool("SLOPE_FILTER_ENABLED", "true")

# ── Scoring
MIN_SCORE    = _int("MIN_SCORE", 42)
FUEL_SCORE   = _int("FUEL_SCORE", 70)
SUP_SCORE    = _int("SUP_SCORE", 85)
PRED_THR_STD  = _int("PRED_THR_STD", 55)
PRED_THR_FUEL = _int("PRED_THR_FUEL", 68)
PRED_THR_SUP  = _int("PRED_THR_SUP", 80)
COUNTER_TREND_PENALTY = _float("COUNTER_TREND_PENALTY", 12.0)

# ── TP / SL / Trail  ← UPDATED VALUES
SL_ATR_MULT         = _float("SL_ATR_MULT", 1.8)
BREAKEVEN_ATR_MULT  = _float("BREAKEVEN_ATR_MULT", 1.0)    # era 1.5 → más rápido a BE
TP1_ATR_MULT        = _float("TP1_ATR_MULT", 2.5)          # era 3.0 → TP1 más cercano
TP2_ATR_MULT        = _float("TP2_ATR_MULT", 5.0)
TRAIL_DISTANCE_ATR          = _float("TRAIL_DISTANCE_ATR", 2.0)          # era 2.5
TRAIL_DISTANCE_ATR_POST_TP1 = _float("TRAIL_DISTANCE_ATR_POST_TP1", 1.0) # NEW — post-TP1 tight
ATR_TRAIL_MULT      = _float("ATR_TRAIL_MULT", 2.5)         # legacy alias
CB_ATR_MULT         = _float("CB_ATR_MULT", 4.0)
CB_BARS             = _int("CB_BARS", 5)

# ── Time controls
MAX_HOLD_MINUTES     = _int("MAX_HOLD_MINUTES", 90)   # era 120 → más agresivo en 3m TF
TRADE_START_UTC      = _int("TRADE_START_UTC", 7)
TRADE_END_UTC        = _int("TRADE_END_UTC", 21)
SCAN_INTERVAL        = _int("SCAN_INTERVAL", 60)
POSITION_CHECK_INTERVAL = _int("POSITION_CHECK_INTERVAL", 30)

# ── Kelly / sizing
RISK_PCT           = _float("RISK_PCT", 1.5)
KELLY_WIN_RATE     = _float("KELLY_WIN_RATE", 0.60)
KELLY_RR           = _float("KELLY_RR", 1.5)
KELLY_FRACTION     = _float("KELLY_FRACTION", 0.25)
PRED_KELLY_FRAC    = _float("PRED_KELLY_FRAC", 0.25)
PRED_KELLY_RR      = _float("PRED_KELLY_RR", 1.8)

# ── TP prediction
PRED_TP1_RR        = _float("PRED_TP1_RR", 1.5)
PRED_TP2_RR        = _float("PRED_TP2_RR", 3.0)
PRED_SLD_MIN_ATR   = _float("PRED_SLD_MIN_ATR", 1.0)
PRED_OB_MIN_SAMPLES = _int("PRED_OB_MIN_SAMPLES", 5)
PRED_FIB_ENABLED   = _bool("PRED_FIB_ENABLED", "true")

# ── Optional modules
BTC_CORR_ENABLED    = _bool("BTC_CORR_ENABLED", "true")
BTC_CORR_THRESHOLD  = _float("BTC_CORR_THRESHOLD", 0.5)
BTC_CORR_MAX_SAME   = _int("BTC_CORR_MAX_SAME", 3)
BTC_CORR_WINDOW_SEC = _int("BTC_CORR_WINDOW_SEC", 1800)
CORRELATION_WINDOW_SEC = _int("CORRELATION_WINDOW_SEC", 900)
MAX_SAME_DIRECTION  = _int("MAX_SAME_DIRECTION", 2)
OI_FILTER_ENABLED   = _bool("OI_FILTER_ENABLED", "true")
OI_CASCADE_ENABLED  = _bool("OI_CASCADE_ENABLED", "true")
FR_REGIME_ENABLED   = _bool("FR_REGIME_ENABLED", "false")
VOL_REGIME_ENABLED  = _bool("VOL_REGIME_ENABLED", "false")
CANDLE_TURN_ENABLED = _bool("CANDLE_TURN_ENABLED", "false")
CB_ENABLED          = _bool("CB_ENABLED", "false")
COMPLEMENT_MODE     = _str("COMPLEMENT_MODE", "DISABLED")
HARVEST_ENABLED     = _bool("HARVEST_ENABLED", "false")
HARVEST_FR_THR      = _float("HARVEST_FR_THR", 0.0010)
FR_EXTREME_THR      = _float("FR_EXTREME_THR", 0.0005)
RECONCILE_ON_STARTUP = _bool("RECONCILE_ON_STARTUP", "false")

# ── Exit rules
EMA_EXIT_ENABLED       = _bool("EMA_EXIT_ENABLED", "true")
EMA_EXIT_PERIOD        = _int("EMA_EXIT_PERIOD", 9)
EMA_EXIT_MIN_HOLD_MIN  = _int("EMA_EXIT_MIN_HOLD_MIN", 30)
SESSION_START          = _int("SESSION_START", 0)
SESSION_END            = _int("SESSION_END", 24)
MOMENTUM_EXIT_ENABLED  = _bool("MOMENTUM_EXIT_ENABLED", "true")
MOMENTUM_EXIT_OB       = _int("MOMENTUM_EXIT_OB", 70)
MOMENTUM_EXIT_OS       = _int("MOMENTUM_EXIT_OS", 30)

# ── Infrastructure
PORT       = _int("PORT", 8080)
WS_ENABLED = _bool("WS_ENABLED", "false")
STATE_FILE = _str("STATE_FILE", "/tmp/bot_state.json")
MASTER_URL = _str("MASTER_URL", "")

# ── Telegram
TELEGRAM_TOKEN = _str("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT  = _str("TELEGRAM_CHAT_ID")

# ── EMA9 Rally filter (crossover bot — rally disabled, uses crossover instead)
EMA9_RALLY_ENABLED   = _bool("EMA9_RALLY_ENABLED",   "false")
EMA9_NEAR_PCT        = _float("EMA9_NEAR_PCT",        1.0)
EMA9_VOL_HIGH_MULT   = _float("EMA9_VOL_HIGH_MULT",   1.3)

# ── Short-specific filters
IBS_PULLBACK_ENABLED  = _bool("IBS_PULLBACK_ENABLED",  "false")
BB_SHORT_ENABLED      = _bool("BB_SHORT_ENABLED",      "false")
BB_SHORT_VETO_LONG    = _bool("BB_SHORT_VETO_LONG",    "false")
EMA9_VWAP_ENABLED     = _bool("EMA9_VWAP_ENABLED",     "false")
EMA9_VWAP_VETO_LONG   = _bool("EMA9_VWAP_VETO_LONG",   "false")
EMA9_VWAP_BOOST       = _float("EMA9_VWAP_BOOST",      0.0)
EMA55_BOOST_ENABLED   = _bool("EMA55_BOOST_ENABLED",   "false")
RSI15M_FILTER_ENABLED = _bool("RSI15M_FILTER_ENABLED", "false")
RSI15M_SHORT_MAX      = _float("RSI15M_SHORT_MAX",     70.0)
RSI15M_REQUIRED       = _bool("RSI15M_REQUIRED",       "false")

# ── Scoring
SUP_SCORE    = _int("SUP_SCORE",   85)
FUEL_SCORE   = _int("FUEL_SCORE",  70)
MIN_SCORE    = _int("MIN_SCORE", 42)
COUNTER_TREND_PENALTY = _float("COUNTER_TREND_PENALTY", 12.0)

# ── Market structure / BTC regime
MS_ENABLED          = _bool("MS_ENABLED",          "true")
MS_LEN              = _int("MS_LEN",               10)
BTC_REGIME_ENABLED  = _bool("BTC_REGIME_ENABLED",  "true")
BTC_CORR_ENABLED    = _bool("BTC_CORR_ENABLED",    "true")
BTC_CORR_THRESHOLD  = _float("BTC_CORR_THRESHOLD", 0.5)
BTC_CORR_MAX_SAME   = _int("BTC_CORR_MAX_SAME",    3)
MAX_SAME_DIRECTION  = _int("MAX_SAME_DIRECTION",   2)
CORRELATION_WINDOW_SEC = _int("CORRELATION_WINDOW_SEC", 900)
BTC_CORR_WINDOW_SEC = _int("BTC_CORR_WINDOW_SEC",  1800)

# ── Misc entry params
REQUIRE_TL_BREAK    = _bool("REQUIRE_TL_BREAK",    "false")
LATERAL_ADX_MAX     = _float("LATERAL_ADX_MAX",    25.0)
MAX_ADAPTIVE_OFFSET = _int("MAX_ADAPTIVE_OFFSET",  6)
COPY_MIN_SCORE      = _int("COPY_MIN_SCORE", 50)
COPY_SIZE_MULT      = _float("COPY_SIZE_MULT",      0.4)
EXCLUSIVE_TOP_N     = _int("EXCLUSIVE_TOP_N",       30)
COPY_MAX_ADVERSE_PCT = _float("COPY_MAX_ADVERSE_PCT", -0.3)
HEDGE_LOSS_COUNT    = _int("HEDGE_LOSS_COUNT",      3)
MASTER_URL          = _str("MASTER_URL",            "")
WS_ENABLED          = _bool("WS_ENABLED",          "false")
PORT                = _int("PORT",                  8080)

ADX_LATERAL = _float("ADX_LATERAL", 20.0)

# ── Unicorn Model (multi-symbol scan)
UNICORN_TOP_N    = _int("UNICORN_TOP_N", 150)   # símbolos para Unicorn scan
UNICORN_SCAN_SEC = _int("UNICORN_SCAN_SEC", 300) # cada 5 min
UNICORN_PIVOT_LEN   = _int("UNICORN_PIVOT_LEN", 5)
UNICORN_SWEEP_LB    = _int("UNICORN_SWEEP_LB", 30)
UNICORN_REQUIRE_FVG = _bool("UNICORN_REQUIRE_FVG", "true")
UNICORN_RR          = _float("UNICORN_RR", 2.0)

# ── Cooldown
TRADE_COOLDOWN_SEC = _int("TRADE_COOLDOWN_SEC", 180)

DIRECTION           = _str("DIRECTION", "BOTH")  # LONG | SHORT | BOTH
