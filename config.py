"""
config.py — Configuración central del QF×JP v3.4 Bot
Lee todas las variables desde Railway Environment Variables / .env
"""
import os, sys

def _e(k, d=None, req=False):
    v = os.getenv(k, d)
    if req and not v:
        print(f"❌ FATAL: '{k}' no definida. Añádela en Railway → Variables.")
        sys.exit(1)
    return str(v).strip() if v is not None else v

# ── BINGX ─────────────────────────────────────────────────────────────────────
BINGX_API_KEY    = _e("BINGX_API_KEY",    req=True)
BINGX_SECRET_KEY = _e("BINGX_SECRET_KEY", req=True)
BINGX_BASE_URL   = _e("BINGX_BASE_URL",   "https://open-api.bingx.com")

# ── TELEGRAM ──────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = _e("TELEGRAM_TOKEN",   req=True)
TELEGRAM_CHAT_ID = _e("TELEGRAM_CHAT_ID", req=True)

# ── SÍMBOLOS ──────────────────────────────────────────────────────────────────
SYMBOLS = [s.strip() for s in _e("SYMBOLS", "BTC-USDT,ETH-USDT,SOL-USDT").split(",")]

# ── TRADING ───────────────────────────────────────────────────────────────────
LEVERAGE          = int(_e("LEVERAGE",         "5"))
RISK_PER_TRADE    = float(_e("RISK_PER_TRADE", "0.01"))   # 1% balance
MAX_OPEN_TRADES   = int(_e("MAX_OPEN_TRADES",  "3"))

# ── SCORE UMBRALES (del Pine Script) ─────────────────────────────────────────
SCORE_STD         = int(_e("SCORE_STD",   "55"))   # señal estándar
SCORE_FUEL        = int(_e("SCORE_FUEL",  "68"))   # señal fuel
SCORE_SUP         = int(_e("SCORE_SUP",   "80"))   # señal suprema
HTF_MIN_ALIGNED   = int(_e("HTF_MIN",     "2"))    # TFs alineados mínimo

# ── INDICADORES ───────────────────────────────────────────────────────────────
ATR_LEN           = int(_e("ATR_LEN",    "10"))
ATR_SL_MULT       = float(_e("ATR_SL",  "1.0"))    # SL dinámico × ATR
ATR_TP1_MULT      = float(_e("ATR_TP1", "1.5"))
ATR_TP2_MULT      = float(_e("ATR_TP2", "3.0"))
ATR_PTP_MULT      = float(_e("ATR_PTP", "0.5"))    # partial TP en 0.5×ATR
MOM_LEN           = int(_e("MOM_LEN",   "20"))
REV_LEN           = int(_e("REV_LEN",   "8"))
VOL_LEN           = int(_e("VOL_LEN",   "14"))
CVD_LEN           = int(_e("CVD_LEN",   "20"))
CVD_ROLL          = int(_e("CVD_ROLL",  "100"))
DECAY_LEN         = int(_e("DECAY_LEN", "40"))
ADX_LEN           = int(_e("ADX_LEN",   "14"))
ADX_TREND         = int(_e("ADX_TREND", "25"))
ADX_LATERAL       = int(_e("ADX_LAT",   "20"))
SQ_LEN            = int(_e("SQ_LEN",    "20"))
RSI_LEN           = int(_e("RSI_LEN",   "14"))
VP_LEN            = int(_e("VP_LEN",    "100"))
VP_BINS           = int(_e("VP_BINS",   "24"))
OB_BARS           = int(_e("OB_BARS",   "50"))
OB_IMP_MULT       = float(_e("OB_IMP", "1.5"))
FVG_MIN_MULT      = float(_e("FVG_MIN","0.3"))
FVG_BARS          = int(_e("FVG_BARS",  "40"))
FVG_MAX           = int(_e("FVG_MAX",   "5"))
SWING_BARS        = int(_e("SWING_BARS","40"))
OI_LEN            = int(_e("OI_LEN",    "20"))
LS_LEN            = int(_e("LS_LEN",    "20"))
TL_LOOKBACK       = int(_e("TL_LB",     "30"))
HL_WINDOW         = int(_e("HL_WIN",    "40"))

# ── KELLY ─────────────────────────────────────────────────────────────────────
KELLY_ENABLED     = _e("KELLY",       "true").lower() == "true"
KELLY_FRACTION    = float(_e("KELLY_FRAC", "0.25"))
KELLY_WIN_RATE    = float(_e("KELLY_WR",   "0.55"))
KELLY_RR          = float(_e("KELLY_RR",   "1.8"))

# ── CIRCUIT BREAKER ───────────────────────────────────────────────────────────
CB_ENABLED        = _e("CB_ON",       "true").lower() == "true"
CB_BARS           = int(_e("CB_BARS",  "10"))
CB_MULT           = float(_e("CB_MULT","3.0"))
DAILY_LOSS_LIMIT  = float(_e("DAILY_LOSS", "0.05"))   # 5%

# ── PARTIAL TP ────────────────────────────────────────────────────────────────
PTP_ENABLED       = _e("PTP_ON",  "true").lower() == "true"
PTP_PCT           = float(_e("PTP_PCT", "0.25"))      # cerrar 25% en TP0.5

# ── CANDLES ───────────────────────────────────────────────────────────────────
CANDLE_TF         = _e("CANDLE_TF",    "3m")
CANDLE_LIMIT      = int(_e("CANDLE_LIMIT", "300"))
HTF_15M_TF        = _e("HTF_15M", "15m")
HTF_1H_TF         = _e("HTF_1H",  "1h")
HTF_W_TF          = _e("HTF_W",   "1w")
HTF_1M_TF         = _e("HTF_1M",  "1m")

# ── FILTROS ───────────────────────────────────────────────────────────────────
VOL_FILTER_ON     = _e("VOL_FILTER", "true").lower() == "true"
VOL_FILTER_THR    = float(_e("VOL_THR", "0.70"))
EXEC_BPT          = float(_e("EXEC_BPT","0.18"))      # umbral drenaje spread %
OVERLAP_BOOST     = int(_e("OVL_BOOST", "3"))          # pts extra en LDN/NY overlap
SESSION_ASIA      = _e("SES_ASIA", "false").lower() == "true"  # incluir Asia en señales

# ── HEALTH CHECK ──────────────────────────────────────────────────────────────
PORT              = int(_e("PORT", "8080"))
LOG_LEVEL         = _e("LOG_LEVEL", "INFO")
CYCLE_MINUTES     = int(_e("CYCLE_MINUTES", "3"))
