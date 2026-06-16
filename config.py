"""
QF×JP Bot v7.2 — Config FIXES "0 TRADES"
═══════════════════════════════════════════════════════════════════════════════
FIXES vs v7.1 (diagnóstico: ningún bot abría trades):

  FIX 1 — MIN_TIER="STD" (era "FUEL"):
    MIN_TIER=FUEL requiere score ≥65, pero con mercados volátiles los scores
    reales oscilan entre 55-65. El bot analizaba 683 símbolos y descartaba
    todos por tier_ok(). Bajando a STD (score ≥58) el bot puede ejecutar.

  FIX 2 — CB_ATR_MULT=4.0 (era 3.0):
    Con CB_ATR_MULT=3.0 y mercado volátil los circuit breakers disparaban
    en casi todos los pares (logs muestran 15+ CB seguidos). Con 4.0 solo
    activa en velas verdaderamente extremas.

  FIX 3 — CB_BARS=5 (era 10):
    Solo mira las últimas 5 velas para el circuit breaker. Con 10 velas
    cualquier movimiento reciente de volatilidad bloqueaba el símbolo.

  FIX 4 — REQUIRE_TL_BREAK=False (era True):
    La condición de ruptura de tendencia filtrada junto con HTF_MIN_ALIGNED=2
    era demasiado restrictiva. Se desactiva para que la señal base del
    analyze() sea suficiente.

  FIX 5 — HTF_MIN_ALIGNED=1 (era 2):
    Requería alineación en 2 timeframes HTF simultáneamente. Con solo 3m
    de timeframe base en mercados laterales esto es casi imposible.

  FIX 6 — MIN_SCORE=55.0 (era 58.0):
    Umbral base más accesible. El tier STD ya filtra lo suficiente.

  FIX 7 — CB_COOLDOWN=300s (era 600s en scanner):
    El cooldown del circuit breaker de 10min era demasiado largo.
    Se reduce a 5min vía variable de entorno CB_COOLDOWN.

  FIX 8 — TOP_N_SYMBOLS=200 para renewed-love:
    En vez de los 683 símbolos (muchos micro-cap sin liquidez), limitar
    a top-200 por volumen mejora la calidad de señales y reduce ruido.
    joyful-art sigue con sus top-50 exclusivos.

  RESTO sin cambios (trailing stop, daily loss real, kelly, etc.)
═══════════════════════════════════════════════════════════════════════════════
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
CAPITAL          = _float("CAPITAL", 700.0)
RISK_PCT         = _float("RISK_PCT", 0.5)
LEVERAGE         = _int("LEVERAGE", 10)
MAX_OPEN_TRADES  = _int("MAX_OPEN_TRADES", 3)
MAX_DAILY_TRADES = _int("MAX_DAILY_TRADES", 10)

# ── Umbrales de señal ─────────────────────────────────────────────────────────
# FIX v7.2: MIN_SCORE 58→55, MIN_TIER FUEL→STD
# Con FUEL (≥65) el mercado actual no generaba ningún trade.
# STD (≥55) permite operar con señales de calidad razonable.
MIN_SCORE  = _float("MIN_SCORE",  55.0)    # era 58.0
FUEL_SCORE = _float("FUEL_SCORE", 65.0)
SUP_SCORE  = _float("SUP_SCORE",  80.0)
MIN_TIER   = os.getenv("MIN_TIER", "STD").upper()   # era "FUEL"

# ── Entrada ───────────────────────────────────────────────────────────────────
# FIX v7.2: REQUIRE_TL_BREAK False, HTF_MIN_ALIGNED 2→1
REQUIRE_TL_BREAK = _bool("REQUIRE_TL_BREAK", False)   # era True
HTF_MIN_ALIGNED  = _int("HTF_MIN_ALIGNED", 1)          # era 2

# ── Scanner ───────────────────────────────────────────────────────────────────
SCAN_INTERVAL   = _int("SCAN_INTERVAL", 60)
# FIX v7.2: TOP_N_SYMBOLS 0→200 para renewed-love
# 0 = todos los símbolos (683), pero muchos micro-cap sin señal real.
# 200 = top por volumen, mayor calidad de señal.
# joyful-art usa sus 50 exclusivos via complement_engine.
TOP_N_SYMBOLS   = _int("TOP_N_SYMBOLS", 200)    # era 0
BLACKLIST       = set(_list("BLACKLIST", ""))
MIN_VOLUME_USDT = _float("MIN_VOLUME_USDT", 5_000_000.0)

# ── Timeframes ────────────────────────────────────────────────────────────────
TIMEFRAME      = os.getenv("TIMEFRAME",      "3m")
HTF_TIMEFRAME  = os.getenv("HTF_TIMEFRAME",  "15m")
HTF2_TIMEFRAME = os.getenv("HTF2_TIMEFRAME", "1h")
HTF5_TIMEFRAME = os.getenv("HTF5_TIMEFRAME", "4h")

# ── ATR / SL / TP ─────────────────────────────────────────────────────────────
ATR_LEN      = _int("ATR_LEN",       10)
SL_ATR_MULT  = _float("SL_ATR_MULT",  2.0)
TP1_ATR_MULT = _float("TP1_ATR_MULT", 2.0)
TP2_ATR_MULT = _float("TP2_ATR_MULT", 4.0)

# ── ADX ───────────────────────────────────────────────────────────────────────
ADX_LEN     = _int("ADX_LEN", 14)
ADX_TREND   = _float("ADX_TREND",   25.0)
ADX_LATERAL = _float("ADX_LATERAL", 20.0)

# ── Kelly ─────────────────────────────────────────────────────────────────────
KELLY_WIN_RATE = _float("KELLY_WIN_RATE", 0.55)
KELLY_RR       = _float("KELLY_RR",       1.5)
KELLY_FRACTION = _float("KELLY_FRACTION", 0.15)

# ── Circuit Breaker ───────────────────────────────────────────────────────────
# FIX v7.2: CB_ATR_MULT 3.0→4.0, CB_BARS 10→5
# Con 3.0 y mercado volátil se disparaba en casi todos los pares.
# Con 4.0 + 5 barras solo activa en velas verdaderamente extremas.
CB_ENABLED  = _bool("CB_ENABLED",   True)
CB_ATR_MULT = _float("CB_ATR_MULT", 4.0)   # era 3.0
CB_BARS     = _int("CB_BARS",       5)      # era 10

# ── Gestión de posiciones ─────────────────────────────────────────────────────
POSITION_CHECK_INTERVAL = _int("POSITION_CHECK_INTERVAL", 30)

# ── Trailing Stop Dinámico ────────────────────────────────────────────────────
BREAKEVEN_ATR_MULT = _float("BREAKEVEN_ATR_MULT", 1.0)
TRAIL_DISTANCE_ATR = _float("TRAIL_DISTANCE_ATR", 1.5)

# ── Límite de pérdida diaria ──────────────────────────────────────────────────
DAILY_LOSS_PCT = _float("DAILY_LOSS_PCT", 2.0)

# ── Notional máximo por trade ─────────────────────────────────────────────────
MAX_NOTIONAL_USDT = _float("MAX_NOTIONAL_USDT", 200.0)

# ── Puerto ────────────────────────────────────────────────────────────────────
PORT = _int("PORT", 8080)
