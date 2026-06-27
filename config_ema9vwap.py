"""
EMA9×VWAP Bot — config.py
════════════════════════════════════════════════════════════════
Estrategia: EMA9 crossover/crossunder VWAP con confirmaciones
  - Señal principal: EMA9 cruza VWAP (Pine "EMA 9 + VWAP Strategy")
  - Confirmación 1: MACD dirección + posición vs zero line
  - Confirmación 2: RSI > 50 (LONG) / RSI < 50 (SHORT)
  - Confirmación 3: Volumen > media × mult en la vela del cruce
  - Confirmación 4: EMA21 alineada (opcional)
  - Exit: ATR trailing stop dinámico
════════════════════════════════════════════════════════════════
"""
import os
from dotenv import load_dotenv
load_dotenv()

def _bool(k, d): return os.getenv(k, str(d)).strip().lower() in ("true","1","yes")
def _float(k, d):
    try: return float(os.getenv(k, str(d)).strip().split()[0])
    except: return d
def _int(k, d):
    try: return int(os.getenv(k, str(d)).strip().split()[0])
    except: return d
def _list(k, d):
    r = os.getenv(k, d).strip()
    return [x.strip() for x in r.split(",") if x.strip()] if r else []

# ── BingX ─────────────────────────────────────────────────────────────────────
BINGX_API_KEY    = os.getenv("BINGX_API_KEY", "").strip()
BINGX_SECRET_KEY = os.getenv("BINGX_SECRET_KEY", "").strip()
BINGX_BASE_URL   = "https://open-api.bingx.com"

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# ── Modo ──────────────────────────────────────────────────────────────────────
MODE = os.getenv("MODE", "SIGNAL").upper()

# ── Capital y riesgo ──────────────────────────────────────────────────────────
CAPITAL           = _float("CAPITAL",           400.0)
RISK_PCT          = _float("RISK_PCT",            1.0)
LEVERAGE          = _int("LEVERAGE",               5)
MAX_OPEN_TRADES   = _int("MAX_OPEN_TRADES",        4)
MAX_DAILY_TRADES  = _int("MAX_DAILY_TRADES",      20)
DAILY_LOSS_PCT    = _float("DAILY_LOSS_PCT",       5.0)
MAX_NOTIONAL_USDT = _float("MAX_NOTIONAL_USDT",   60.0)
MIN_NOTIONAL_USDT = _float("MIN_NOTIONAL_USDT",   10.0)
FIXED_NOTIONAL_USDT = _float("FIXED_NOTIONAL_USDT", 0.0)
MIN_MARGIN_USDT   = _float("MIN_MARGIN_USDT",      1.0)

# ── Timeframe ─────────────────────────────────────────────────────────────────
# EMA9×VWAP funciona mejor en 3m para scalping rápido
# o en 5m para señales más limpias con menos ruido
TIMEFRAME = os.getenv("TIMEFRAME", "3m")

# ── Indicadores principales ───────────────────────────────────────────────────
# EMA9 × VWAP (estrategia base del Pine)
EMA9_PERIOD   = _int("EMA9_PERIOD",   9)
EMA21_PERIOD  = _int("EMA21_PERIOD",  21)  # confirmación opcional

# ATR trailing stop (igual que el Pine original)
ATR_LEN       = _int("ATR_LEN",       14)
ATR_TRAIL_MULT = _float("ATR_TRAIL_MULT", 2.0)  # Pine default: 2× ATR
SL_ATR_MULT   = _float("SL_ATR_MULT",    2.0)   # SL inicial = ATR × 2

# ── Confirmación: MACD ────────────────────────────────────────────────────────
# MACD (12,26,9) — confirmación de momentum
# LONG: MACD line > Signal line (histograma positivo)
# SHORT: MACD line < Signal line (histograma negativo)
MACD_FAST     = _int("MACD_FAST",   12)
MACD_SLOW     = _int("MACD_SLOW",   26)
MACD_SIGNAL   = _int("MACD_SIGNAL",  9)
MACD_REQUIRED = _bool("MACD_REQUIRED", True)   # True = requisito, False = solo boost

# ── Confirmación: RSI ─────────────────────────────────────────────────────────
# RSI > 50 para LONG, RSI < 50 para SHORT = tendencia confirmada
# Basado en "Sols Day Trading Signals" (TradingView)
RSI_PERIOD    = _int("RSI_PERIOD",   14)
RSI_MID       = _float("RSI_MID",   50.0)   # umbral medio
RSI_REQUIRED  = _bool("RSI_REQUIRED", True)
RSI_OB        = _float("RSI_OB",    70.0)   # overbought — evitar LONG aquí
RSI_OS        = _float("RSI_OS",    30.0)   # oversold — evitar SHORT aquí

# ── Confirmación: Volumen ─────────────────────────────────────────────────────
# Volumen en la vela del cruce > media × mult = cruce con convicción
VOL_MA_PERIOD = _int("VOL_MA_PERIOD",   20)
VOL_MIN_MULT  = _float("VOL_MIN_MULT",   1.3)  # volumen mínimo = 1.3× media
VOL_REQUIRED  = _bool("VOL_REQUIRED",   False) # False por defecto — más señales

# ── Confirmación: EMA21 ───────────────────────────────────────────────────────
# EMA9 > EMA21 para LONG, EMA9 < EMA21 para SHORT = doble confirmación EMA
EMA21_REQUIRED = _bool("EMA21_REQUIRED", False) # False — señal adicional, no requisito

# ── Crossunder lookback ───────────────────────────────────────────────────────
# Barras atrás donde buscar el cruce (scanner con polling 60s puede perder cruces)
CROSS_LOOKBACK = _int("CROSS_LOOKBACK", 3)   # buscar cruce en últimas 3 barras

# ── TP / SL ───────────────────────────────────────────────────────────────────
TP1_ATR_MULT  = _float("TP1_ATR_MULT",  2.0)
TP2_ATR_MULT  = _float("TP2_ATR_MULT",  4.0)

# ── Trailing Stop ─────────────────────────────────────────────────────────────
BREAKEVEN_ATR_MULT = _float("BREAKEVEN_ATR_MULT", 1.0)
TRAIL_DISTANCE_ATR = _float("TRAIL_DISTANCE_ATR",  2.0)  # Pine usa 2× ATR
MAX_HOLD_MINUTES   = _int("MAX_HOLD_MINUTES",      120)

# ── Scanner ───────────────────────────────────────────────────────────────────
SCAN_INTERVAL   = _int("SCAN_INTERVAL",    60)
TOP_N_SYMBOLS   = _int("TOP_N_SYMBOLS",   150)
MIN_VOLUME_USDT = _float("MIN_VOLUME_USDT", 5_000_000.0)
BLACKLIST = set(_list("BLACKLIST",
    "ESPORTS,STABLEUSDT,EURUSD,SILVER,SILVERXAG,OILWTI,OILBRENT,PAXG,CUSDT,SYN,GOLD,GASOLINE"))

# ── Risk management ───────────────────────────────────────────────────────────
CORRELATION_WINDOW_SEC = _int("CORRELATION_WINDOW_SEC", 900)
MAX_SAME_DIRECTION     = _int("MAX_SAME_DIRECTION",       2)
RECONCILE_ON_STARTUP   = _bool("RECONCILE_ON_STARTUP",  False)
POSITION_CHECK_INTERVAL = _int("POSITION_CHECK_INTERVAL", 30)

# ── Limit orders ─────────────────────────────────────────────────────────────
LIMIT_ORDERS_ENABLED = _bool("LIMIT_ORDERS_ENABLED", True)
LIMIT_TIMEOUT_SECS   = _int("LIMIT_TIMEOUT_SECS",    15)

# ── EMA Exit ─────────────────────────────────────────────────────────────────
EMA_EXIT_ENABLED = _bool("EMA_EXIT_ENABLED", True)
EMA_EXIT_PERIOD  = _int("EMA_EXIT_PERIOD",    9)

# ── Momentum exit ─────────────────────────────────────────────────────────────
MOMENTUM_EXIT_ENABLED = _bool("MOMENTUM_EXIT_ENABLED", True)
MOMENTUM_EXIT_OB      = _float("MOMENTUM_EXIT_OB", 70.0)
MOMENTUM_EXIT_OS      = _float("MOMENTUM_EXIT_OS", 30.0)

# ── BTC Correlation Guard ─────────────────────────────────────────────────────
BTC_CORR_ENABLED    = _bool("BTC_CORR_ENABLED",    True)
BTC_CORR_THRESHOLD  = _float("BTC_CORR_THRESHOLD", 0.5)
BTC_CORR_MAX_SAME   = _int("BTC_CORR_MAX_SAME",    3)
BTC_CORR_WINDOW_SEC = _int("BTC_CORR_WINDOW_SEC",  1800)

# ── Volatility regime ─────────────────────────────────────────────────────────
VOL_REGIME_ENABLED = _bool("VOL_REGIME_ENABLED", True)

# ── Funding rate ──────────────────────────────────────────────────────────────
FR_EXTREME_THR = _float("FR_EXTREME_THR", 0.0005)

# ── CB ────────────────────────────────────────────────────────────────────────
CB_ENABLED  = _bool("CB_ENABLED",   False)
CB_ATR_MULT = _float("CB_ATR_MULT", 3.0)
CB_BARS     = _int("CB_BARS",       10)

# ── Kelly ─────────────────────────────────────────────────────────────────────
KELLY_WIN_RATE = _float("KELLY_WIN_RATE", 0.55)
KELLY_RR       = _float("KELLY_RR",       2.0)
KELLY_FRACTION = _float("KELLY_FRACTION", 0.15)

# ── Misc ──────────────────────────────────────────────────────────────────────
PORT             = _int("PORT", 8080)
WS_ENABLED       = _bool("WS_ENABLED", False)
MIN_TIER         = os.getenv("MIN_TIER", "STD").upper()
MIN_SCORE        = _float("MIN_SCORE",  55.0)
FUEL_SCORE       = _float("FUEL_SCORE", 65.0)
SUP_SCORE        = _float("SUP_SCORE",  80.0)
COMPLEMENT_MODE  = "DISABLED"
MASTER_URL       = ""

# ── Indicadores compatibilidad (necesarios para RiskManager/PositionManager) ──
ADX_LEN          = _int("ADX_LEN", 14)
ADX_TREND        = _float("ADX_TREND",   25.0)
ADX_LATERAL      = _float("ADX_LATERAL", 20.0)
CVD_ROLL_WINDOW  = _int("CVD_ROLL_WINDOW", 60)
EQL_LEN          = _int("EQL_LEN", 20)
EQL_TOL          = _float("EQL_TOL", 0.15)
OBP2_DIST        = _float("OBP2_DIST", 1.5)
PRE_SCORE        = _float("PRE_SCORE", 45.0)
REQUIRE_TL_BREAK = _bool("REQUIRE_TL_BREAK", False)
HTF_MIN_ALIGNED  = _int("HTF_MIN_ALIGNED", 1)
OI_FILTER_ENABLED = _bool("OI_FILTER_ENABLED", False)
FR_REGIME_ENABLED = _bool("FR_REGIME_ENABLED", False)
HARVEST_ENABLED   = _bool("HARVEST_ENABLED",   False)
HARVEST_FR_THR    = _float("HARVEST_FR_THR",   0.0010)
SLOPE_FILTER_ENABLED = _bool("SLOPE_FILTER_ENABLED", False)
CANDLE_TURN_ENABLED  = _bool("CANDLE_TURN_ENABLED",  False)
EXPLOSION_ENABLED    = _bool("EXPLOSION_ENABLED",    False)
DAILY_LOSS_PCT       = _float("DAILY_LOSS_PCT",       5.0)
TIME_STOP_MIN_PROGRESS_ATR = _float("TIME_STOP_MIN_PROGRESS_ATR", 0.5)
KOTE_SCAN_INTERVAL   = _int("KOTE_SCAN_INTERVAL", 900)
KOTE_SYMBOLS_LIST    = _list("KOTE_SYMBOLS_LIST", "")
