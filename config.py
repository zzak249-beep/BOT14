"""
GUA-USDT Bot v3 — Configuración
Fixes: LOOKBACK=250 para EMA200 válida · OI_HISTORY_LEN=20 · tolerancia dinámica
Nuevo: MFI · Compresión · Funding predictivo · Fortaleza relativa BTC · Walk-forward
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── BingX API ─────────────────────────────────────────────────────────────────
BINGX_API_KEY  = os.getenv("BINGX_API_KEY", "")
BINGX_SECRET   = os.getenv("BINGX_SECRET", "")
BASE_URL       = "https://open-api.bingx.com"

# ── Símbolo y temporalidades ─────────────────────────────────────────────────
SYMBOL           = os.getenv("SYMBOL", "GUA-USDT")
INTERVAL         = os.getenv("INTERVAL",       "3m")
INTERVAL_TREND   = os.getenv("INTERVAL_TREND", "15m")
INTERVAL_MACRO   = os.getenv("INTERVAL_MACRO", "1h")
INTERVAL_ENTRY   = os.getenv("INTERVAL_ENTRY", "1m")   # entry refinement

# FIX: LOOKBACK mínimo 250 para que EMA200 sea matemáticamente válida
LOOKBACK         = 260    # era 150 → EMA200 con 150 velas = cálculo inválido
LOOKBACK_TREND   = 100
LOOKBACK_MACRO   = 72

# ── Telegram ─────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# ── Modo ─────────────────────────────────────────────────────────────────────
MODE             = os.getenv("MODE", "SIGNAL")   # SIGNAL | LIVE

# ── Capital y riesgo ─────────────────────────────────────────────────────────
LEVERAGE         = int(os.getenv("LEVERAGE",        "5"))
RISK_PCT         = float(os.getenv("RISK_PCT",      "0.02"))
MAX_OPEN_TRADES  = int(os.getenv("MAX_OPEN_TRADES", "1"))

# ── ATR dinámico ─────────────────────────────────────────────────────────────
ATR_SL_MULT      = float(os.getenv("ATR_SL_MULT",      "1.5"))
ATR_TP1_MULT     = float(os.getenv("ATR_TP1_MULT",     "2.0"))
ATR_TP2_MULT     = float(os.getenv("ATR_TP2_MULT",     "4.0"))
ATR_TRAIL_MULT   = float(os.getenv("ATR_TRAIL_MULT",   "1.0"))
ATR_HIGHVOL_MULT = float(os.getenv("ATR_HIGHVOL_MULT", "2.0"))

# ── Indicadores clásicos ─────────────────────────────────────────────────────
RSI_PERIOD       = 14
RSI_OB           = float(os.getenv("RSI_OB",  "63"))
RSI_OS           = float(os.getenv("RSI_OS",  "42"))   # era 37 → demasiado restrictivo
EMA_FAST         = 9
EMA_SLOW         = 21
EMA_TREND        = 50
EMA_MACRO        = 200
ADX_PERIOD       = 14
ADX_MIN          = float(os.getenv("ADX_MIN", "18"))

# ── TTM Squeeze ───────────────────────────────────────────────────────────────
BB_PERIOD        = 20
BB_MULT          = 2.0
KC_PERIOD        = 20
KC_MULT          = 1.5
MOM_PERIOD       = 12

# ── VWAP ─────────────────────────────────────────────────────────────────────
VWAP_PERIOD      = 60
VWAP_BAND_MULT   = 1.5

# ── RVOL ─────────────────────────────────────────────────────────────────────
RVOL_PERIOD      = 20
RVOL_MIN         = float(os.getenv("RVOL_MIN", "1.2"))

# ── MFI — Money Flow Index (nuevo v3) ────────────────────────────────────────
MFI_PERIOD       = 14
MFI_OB           = 75    # sobrecompra
MFI_OS           = 25    # sobreventa

# ── CVD ──────────────────────────────────────────────────────────────────────
CVD_LB           = 20
CVD_DIV_LB       = 10

# ── FVG ──────────────────────────────────────────────────────────────────────
FVG_LOOKBACK     = 30
FVG_MIN_SIZE     = float(os.getenv("FVG_MIN_SIZE", "0.003"))

# ── Order Blocks ─────────────────────────────────────────────────────────────
OB_LOOKBACK      = 40
OB_IMPULSE_BARS  = 3

# ── Liquidity Sweeps ─────────────────────────────────────────────────────────
LIQ_LOOKBACK     = 25
LIQ_TOLERANCE    = float(os.getenv("LIQ_TOLERANCE", "0.002"))
# v3: tolerancia dinámica basada en ATR (override si ATR disponible)
LIQ_ATR_FACTOR   = 0.5   # 0.5 × (ATR/price) como tolerancia dinámica

# ── ATR Percentil ────────────────────────────────────────────────────────────
ATR_PERCENTILE_LB = 50

# ── Compresión pre-breakout (nuevo v3) ───────────────────────────────────────
COMPRESSION_BARS      = 8      # ventana de comparación
COMPRESSION_MIN_SCORE = 0.30   # score mínimo para considerar compresión

# ── Funding rate ─────────────────────────────────────────────────────────────
FUNDING_EXTREME_LONG  = float(os.getenv("FUNDING_EXTREME_LONG",  "0.0003"))
FUNDING_EXTREME_SHORT = float(os.getenv("FUNDING_EXTREME_SHORT", "-0.0003"))
FUNDING_HOURS_UTC     = (0, 8, 16)          # horas de pago BingX
FUNDING_PRE_MINUTES   = 45                  # ventana anticipatoria antes del pago

# ── OI Delta ─────────────────────────────────────────────────────────────────
# FIX: era 5 (15 min de historia) → 20 (1h de contexto)
OI_HISTORY_LEN   = 20

# ── Fortaleza relativa BTC (nuevo v3) ────────────────────────────────────────
BTC_SYMBOL           = "BTC-USDT"
REL_STRENGTH_LB      = 4      # velas de comparación (12 min en 3m)
REL_STRENGTH_THR     = 0.003  # 0.3% diferencia mínima para ser significativo

# ── Walk-forward calibración (nuevo v3) ──────────────────────────────────────
WF_WINDOW        = 50    # trades en memoria para win rate
WF_MIN_WR        = 0.45  # por debajo → subir umbral automáticamente
WF_MAX_WR        = 0.62  # por encima → bajar umbral automáticamente
WF_THR_ADJUST    = 0.04  # cuánto ajustar el umbral

# ── Señal ────────────────────────────────────────────────────────────────────
SCORE_THR        = float(os.getenv("SCORE_THR", "0.58"))

# ── Cooldown dinámico (nuevo v3: diferente tras win/loss) ────────────────────
COOLDOWN_MIN         = int(os.getenv("COOLDOWN_MIN",      "15"))
COOLDOWN_MIN_LOSS    = int(os.getenv("COOLDOWN_MIN_LOSS", "30"))  # más tiempo tras SL
COOLDOWN_HIGHVOL_MULT= 1.5   # multiplicador en alta volatilidad

# ── Sesiones ─────────────────────────────────────────────────────────────────
SESSION_FILTER   = os.getenv("SESSION_FILTER", "true").lower() == "true"
SESSION_HOURS    = [(7, 12), (13, 18)]

# ── Trade logging CSV (nuevo v3) ─────────────────────────────────────────────
TRADE_LOG_FILE   = os.getenv("TRADE_LOG_FILE", "trades.csv")
TRADE_LOG_ENABLED= os.getenv("TRADE_LOG_ENABLED", "true").lower() == "true"

# ── Health server ────────────────────────────────────────────────────────────
PORT             = int(os.getenv("PORT", "8080"))
