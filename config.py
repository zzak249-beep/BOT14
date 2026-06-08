"""
GUA-USDT Bot v2 — Configuración
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── BingX API ──────────────────────────────────────────────────────────────────
BINGX_API_KEY  = os.getenv("BINGX_API_KEY", "")
BINGX_SECRET   = os.getenv("BINGX_SECRET", "")
BASE_URL       = "https://open-api.bingx.com"

# ── Símbolo y temporalidades ───────────────────────────────────────────────────
SYMBOL           = os.getenv("SYMBOL", "GUA-USDT")
INTERVAL         = os.getenv("INTERVAL", "3m")
INTERVAL_TREND   = os.getenv("INTERVAL_TREND", "15m")
INTERVAL_MACRO   = os.getenv("INTERVAL_MACRO", "1h")
LOOKBACK         = 150
LOOKBACK_TREND   = 100
LOOKBACK_MACRO   = 72

# ── Telegram ───────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# ── Modo ── CAMBIA A LIVE EN RAILWAY ──────────────────────────────────────────
#   SIGNAL → solo Telegram, sin órdenes reales
#   LIVE   → ejecuta órdenes en BingX
MODE = os.getenv("MODE", "SIGNAL")

# ── Capital ────────────────────────────────────────────────────────────────────
LEVERAGE         = int(os.getenv("LEVERAGE",        "5"))
RISK_PCT         = float(os.getenv("RISK_PCT",      "0.02"))
MAX_OPEN_TRADES  = int(os.getenv("MAX_OPEN_TRADES", "1"))

# ── ATR dinámico ──────────────────────────────────────────────────────────────
ATR_SL_MULT      = float(os.getenv("ATR_SL_MULT",      "1.5"))
ATR_TP1_MULT     = float(os.getenv("ATR_TP1_MULT",     "2.0"))
ATR_TP2_MULT     = float(os.getenv("ATR_TP2_MULT",     "4.0"))
ATR_TRAIL_MULT   = float(os.getenv("ATR_TRAIL_MULT",   "1.0"))
ATR_HIGHVOL_MULT = float(os.getenv("ATR_HIGHVOL_MULT", "2.0"))

# ── Indicadores ────────────────────────────────────────────────────────────────
RSI_PERIOD       = 14
RSI_OB           = float(os.getenv("RSI_OB",  "63"))
RSI_OS           = float(os.getenv("RSI_OS",  "37"))
EMA_FAST         = 9
EMA_SLOW         = 21
EMA_TREND        = 50
EMA_MACRO        = 200
ADX_PERIOD       = 14
ADX_MIN          = float(os.getenv("ADX_MIN", "18"))

# ── TTM Squeeze ────────────────────────────────────────────────────────────────
BB_PERIOD        = 20
BB_MULT          = 2.0
KC_PERIOD        = 20
KC_MULT          = 1.5
MOM_PERIOD       = 12

# ── VWAP ──────────────────────────────────────────────────────────────────────
VWAP_PERIOD      = 60
VWAP_BAND_MULT   = 1.5

# ── RVOL ──────────────────────────────────────────────────────────────────────
RVOL_PERIOD      = 20
RVOL_MIN         = float(os.getenv("RVOL_MIN", "1.0"))  # bajado de 1.3 → más permisivo

# ── CVD ────────────────────────────────────────────────────────────────────────
CVD_LB           = 20
CVD_DIV_LB       = 10

# ── FVG ────────────────────────────────────────────────────────────────────────
FVG_LOOKBACK     = 30
FVG_MIN_SIZE     = float(os.getenv("FVG_MIN_SIZE", "0.002"))  # bajado 0.3→0.2%

# ── Order Blocks ───────────────────────────────────────────────────────────────
OB_LOOKBACK      = 40
OB_IMPULSE_BARS  = 3

# ── Liquidity Sweeps ───────────────────────────────────────────────────────────
LIQ_LOOKBACK     = 25
LIQ_TOLERANCE    = float(os.getenv("LIQ_TOLERANCE", "0.003"))  # subido 0.2→0.3%

# ── ATR Percentil ──────────────────────────────────────────────────────────────
ATR_PERCENTILE_LB = 50

# ── Funding ────────────────────────────────────────────────────────────────────
FUNDING_EXTREME_LONG  = float(os.getenv("FUNDING_EXTREME_LONG",  "0.0003"))
FUNDING_EXTREME_SHORT = float(os.getenv("FUNDING_EXTREME_SHORT", "-0.0003"))

# ── OI ─────────────────────────────────────────────────────────────────────────
OI_HISTORY_LEN   = 5

# ── Señal ──────────────────────────────────────────────────────────────────────
SCORE_THR        = float(os.getenv("SCORE_THR", "0.55"))  # bajado 0.58→0.55

# ── Cooldown ───────────────────────────────────────────────────────────────────
COOLDOWN_MIN     = int(os.getenv("COOLDOWN_MIN", "15"))

# ── Sesiones ── GUA es cripto 24h, filtro OFF por defecto ─────────────────────
SESSION_FILTER   = os.getenv("SESSION_FILTER", "false").lower() == "true"
SESSION_HOURS    = [(0, 24)]   # 24h — sobreescribir con SESSION_FILTER=true si se quiere limitar

# ── Order Book Imbalance ───────────────────────────────────────────────────────
# GUA tiene volumen bajo → libro naturalmente desequilibrado → umbral alto
OB_IMBALANCE_THR = float(os.getenv("OB_IMBALANCE_THR", "0.60"))  # subido 0.50→0.60

# ── Health ─────────────────────────────────────────────────────────────────────
PORT = int(os.getenv("PORT", "8080"))
