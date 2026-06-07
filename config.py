"""
GUA-USDT Bot v2 — Configuración
Técnicas modernas: SMC, Squeeze, RVOL, VWAP, OI Delta, FVG, Order Blocks.
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
INTERVAL         = os.getenv("INTERVAL", "3m")        # entrada
INTERVAL_TREND   = os.getenv("INTERVAL_TREND", "15m")  # sesgo tendencial
INTERVAL_MACRO   = os.getenv("INTERVAL_MACRO", "1h")   # estructura macro
LOOKBACK         = 150   # velas 3m
LOOKBACK_TREND   = 100   # velas 15m
LOOKBACK_MACRO   = 72    # velas 1h

# ── Telegram ───────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# ── Modo ───────────────────────────────────────────────────────────────────────
MODE             = os.getenv("MODE", "SIGNAL")  # SIGNAL | LIVE

# ── Capital ────────────────────────────────────────────────────────────────────
LEVERAGE         = int(os.getenv("LEVERAGE",        "5"))
RISK_PCT         = float(os.getenv("RISK_PCT",      "0.02"))
MAX_OPEN_TRADES  = int(os.getenv("MAX_OPEN_TRADES", "1"))

# ── ATR dinámico ──────────────────────────────────────────────────────────────
ATR_SL_MULT      = float(os.getenv("ATR_SL_MULT",    "1.5"))
ATR_TP1_MULT     = float(os.getenv("ATR_TP1_MULT",   "2.0"))
ATR_TP2_MULT     = float(os.getenv("ATR_TP2_MULT",   "4.0"))
ATR_TRAIL_MULT   = float(os.getenv("ATR_TRAIL_MULT",  "1.0"))
# En régimen de alta volatilidad (ATR percentil > 75) el SL se amplía
ATR_HIGHVOL_MULT = float(os.getenv("ATR_HIGHVOL_MULT", "2.0"))

# ── Indicadores clásicos ───────────────────────────────────────────────────────
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
VWAP_PERIOD      = 60    # velas (3h en 3m chart)
VWAP_BAND_MULT   = 1.5   # bandas de desviación

# ── RVOL ──────────────────────────────────────────────────────────────────────
RVOL_PERIOD      = 20
RVOL_MIN         = float(os.getenv("RVOL_MIN", "1.3"))  # mínimo para confirmar

# ── CVD Divergencia ────────────────────────────────────────────────────────────
CVD_LB           = 20
CVD_DIV_LB       = 10   # velas para detectar divergencia

# ── FVG (Fair Value Gaps) ──────────────────────────────────────────────────────
FVG_LOOKBACK     = 30   # buscar FVGs en las últimas N velas
FVG_MIN_SIZE     = float(os.getenv("FVG_MIN_SIZE", "0.003"))  # 0.3% mínimo

# ── Order Blocks ───────────────────────────────────────────────────────────────
OB_LOOKBACK      = 40
OB_IMPULSE_BARS  = 3    # velas de impulso para validar OB

# ── Liquidity Sweeps ───────────────────────────────────────────────────────────
LIQ_LOOKBACK     = 25
LIQ_TOLERANCE    = float(os.getenv("LIQ_TOLERANCE", "0.002"))  # 0.2%

# ── ATR Percentil (régimen de volatilidad) ─────────────────────────────────────
ATR_PERCENTILE_LB = 50   # ventana para calcular percentil ATR

# ── Funding rate ───────────────────────────────────────────────────────────────
FUNDING_EXTREME_LONG  = float(os.getenv("FUNDING_EXTREME_LONG",  "0.0003"))  # longs pagando fuerte → SHORT
FUNDING_EXTREME_SHORT = float(os.getenv("FUNDING_EXTREME_SHORT", "-0.0003")) # shorts pagando fuerte → LONG

# ── OI Delta ──────────────────────────────────────────────────────────────────
OI_HISTORY_LEN   = 5    # guardar historial de OI para calcular delta

# ── Señal ──────────────────────────────────────────────────────────────────────
SCORE_THR        = float(os.getenv("SCORE_THR", "0.58"))

# ── Cooldown ───────────────────────────────────────────────────────────────────
COOLDOWN_MIN     = int(os.getenv("COOLDOWN_MIN", "15"))

# ── Sesiones de trading (UTC) ──────────────────────────────────────────────────
# Solo operar en London Open + NY Open (más liquidez en cripto)
SESSION_FILTER   = os.getenv("SESSION_FILTER", "true").lower() == "true"
SESSION_HOURS    = [(7, 12), (13, 18)]  # London 7-12 UTC, NY 13-18 UTC

# ── Health server ──────────────────────────────────────────────────────────────
PORT             = int(os.getenv("PORT", "8080"))
