"""
QF×JP Bot v6.5 — Config ANTI-LIQUIDACIÓN
Fixes críticos basados en análisis de pérdidas:
  - Notional cap 200 USDT (era ilimitado → liquidaciones de -85, -104, -278 USDT)
  - SL_ATR_MULT 2.0 (era 1.2 → stop hunts en VANA, ILV, BANANA)
  - Daily loss limit 2% (era 5% → hyper perdió 278 USDT en un día)
  - MAX_OPEN_TRADES 3 (era 5-6 → acumula posiciones perdedoras)
"""
import os
from dotenv import load_dotenv
load_dotenv()

def _bool(k, d): return os.getenv(k, str(d)).strip().lower() in ("true","1","yes")
def _float(k, d):
    try: return float(os.getenv(k, str(d)))
    except: return d
def _int(k, d):
    try: return int(os.getenv(k, str(d)))
    except: return d
def _list(k, d):
    r = os.getenv(k, d).strip()
    return [x.strip() for x in r.split(",") if x.strip()] if r else []

# ── BingX ─────────────────────────────────────────────────────────────────────
BINGX_API_KEY    = os.getenv("BINGX_API_KEY", "")
BINGX_SECRET_KEY = os.getenv("BINGX_SECRET_KEY", "")
BINGX_BASE_URL   = "https://open-api.bingx.com"

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# ── Modo ──────────────────────────────────────────────────────────────────────
MODE = os.getenv("MODE", "SIGNAL").upper()

# ── Capital y riesgo ──────────────────────────────────────────────────────────
CAPITAL          = _float("CAPITAL", 879.0)      # saldo actual visto en screenshots
RISK_PCT         = _float("RISK_PCT", 0.4)       # bajado a 0.4% — liquidaciones destruyen ganancias
LEVERAGE         = _int("LEVERAGE", 10)
MAX_OPEN_TRADES  = _int("MAX_OPEN_TRADES", 3)    # DURO: máx 3 simultáneos
MAX_DAILY_TRADES = _int("MAX_DAILY_TRADES", 8)   # bajado a 8 — calidad > cantidad

# ── Umbrales de señal ─────────────────────────────────────────────────────────
MIN_SCORE  = _float("MIN_SCORE",  62.0)   # subido: menos trades, mejores entradas
FUEL_SCORE = _float("FUEL_SCORE", 68.0)   # subido
SUP_SCORE  = _float("SUP_SCORE",  82.0)
MIN_TIER   = os.getenv("MIN_TIER", "FUEL").upper()

# ── Entrada ───────────────────────────────────────────────────────────────────
REQUIRE_TL_BREAK = _bool("REQUIRE_TL_BREAK", True)
HTF_MIN_ALIGNED  = _int("HTF_MIN_ALIGNED", 2)    # era 1 → 2 TFs confirmados

# ── Scanner ───────────────────────────────────────────────────────────────────
SCAN_INTERVAL   = _int("SCAN_INTERVAL", 60)
TOP_N_SYMBOLS   = _int("TOP_N_SYMBOLS", 0)
BLACKLIST       = set(_list("BLACKLIST", ""))
MIN_VOLUME_USDT = _float("MIN_VOLUME_USDT", 5_000_000.0)

# ── Timeframes ────────────────────────────────────────────────────────────────
TIMEFRAME      = os.getenv("TIMEFRAME",      "3m")
HTF_TIMEFRAME  = os.getenv("HTF_TIMEFRAME",  "15m")
HTF2_TIMEFRAME = os.getenv("HTF2_TIMEFRAME", "1h")
HTF5_TIMEFRAME = os.getenv("HTF5_TIMEFRAME", "4h")

# ── ATR / SL / TP ─────────────────────────────────────────────────────────────
ATR_LEN      = _int("ATR_LEN",       10)
SL_ATR_MULT  = _float("SL_ATR_MULT",  1.8)   # 1.8: evita hunts pero no demasiado lejos
TP1_ATR_MULT = _float("TP1_ATR_MULT", 1.5)   # TP1 rápido: cerrar 50% antes
TP2_ATR_MULT = _float("TP2_ATR_MULT", 3.5)   # TP2 con R:R 1:2

# ── ADX ───────────────────────────────────────────────────────────────────────
ADX_LEN     = _int("ADX_LEN", 14)
ADX_TREND   = _float("ADX_TREND",   25.0)
ADX_LATERAL = _float("ADX_LATERAL", 20.0)

# ── Kelly ─────────────────────────────────────────────────────────────────────
KELLY_WIN_RATE = _float("KELLY_WIN_RATE", 0.55)
KELLY_RR       = _float("KELLY_RR",       1.5)
KELLY_FRACTION = _float("KELLY_FRACTION", 0.15)  # era 0.25 → reducido

# ── Circuit Breaker ───────────────────────────────────────────────────────────
CB_ENABLED  = _bool("CB_ENABLED",   True)
CB_ATR_MULT = _float("CB_ATR_MULT", 3.0)
CB_BARS     = _int("CB_BARS",       10)

# ── Gestión de posiciones ─────────────────────────────────────────────────────
POSITION_CHECK_INTERVAL = _int("POSITION_CHECK_INTERVAL", 30)
# BREAKEVEN_ATR_MULT movido abajo con SL_MIN/MAX_PCT

# ── Límite de pérdida diaria ──────────────────────────────────────────────────
DAILY_LOSS_PCT = _float("DAILY_LOSS_PCT", 1.5)  # bajado a 1.5%: parar antes

# ── Notional máximo por trade ─────────────────────────────────────────────────
MAX_NOTIONAL_USDT = _float("MAX_NOTIONAL_USDT", 150.0)  # bajado: PIUSDT liquidó con 499 USDT

# ── SL mínimo obligatorio (% del entry) ───────────────────────────────────────
# Si ATR produce un SL demasiado cercano, usar este mínimo
SL_MIN_PCT = _float("SL_MIN_PCT", 0.8)   # mínimo 0.8% de distancia al entry
SL_MAX_PCT = _float("SL_MAX_PCT", 3.5)   # máximo 3.5%: SL muy lejano = pérdida enorme

# ── Breakeven más agresivo ─────────────────────────────────────────────────────
BREAKEVEN_ATR_MULT = _float("BREAKEVEN_ATR_MULT", 1.0)  # mover BE antes (era 1.5)

# ── Puerto ────────────────────────────────────────────────────────────────────
PORT = _int("PORT", 8080)
