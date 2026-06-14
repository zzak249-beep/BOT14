"""
QF×JP Bot v6.5 — Config ANTI-LIQUIDACIÓN
Fixes críticos basados en análisis de pérdidas:
  - Notional cap 200 USDT (era ilimitado → liquidaciones de -85, -104, -278 USDT)
  - SL_ATR_MULT 2.0 (era 1.2 → stop hunts en VANA, ILV, BANANA)
  - Daily loss limit 2% (era 5% → hyper perdió 278 USDT en un día)
  - MAX_OPEN_TRADES 3 (era 5-6 → acumula posiciones perdedoras)

PARSING DEFENSIVO:
  Todas las funciones usan .strip().split()[0] para ignorar comentarios inline.
  Ejemplo: CAPITAL="700  ← saldo real" → lee "700" correctamente.
"""
import os
from dotenv import load_dotenv
load_dotenv()


def _raw(k, d):
    """Obtiene el valor crudo y elimina comentarios inline (todo tras el primer espacio)."""
    v = os.getenv(k, str(d)).strip()
    return v.split()[0] if v else str(d)


def _bool(k, d):
    return _raw(k, d).lower() in ("true", "1", "yes")


def _float(k, d):
    try:
        return float(_raw(k, d))
    except Exception:
        return d


def _int(k, d):
    try:
        return int(_raw(k, d))
    except Exception:
        return d


def _list(k, d):
    r = os.getenv(k, d).strip()
    # Los comentarios en listas no aplican (valores separados por coma)
    return [x.strip() for x in r.split(",") if x.strip()] if r else []


# ── BingX ─────────────────────────────────────────────────────────────────────
BINGX_API_KEY    = os.getenv("BINGX_API_KEY", "")
BINGX_SECRET_KEY = os.getenv("BINGX_SECRET_KEY", "")
BINGX_BASE_URL   = "https://open-api.bingx.com"

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# ── Modo ──────────────────────────────────────────────────────────────────────
MODE = _raw("MODE", "SIGNAL").upper()

# ── Capital y riesgo ──────────────────────────────────────────────────────────
CAPITAL          = _float("CAPITAL",         700.0)
RISK_PCT         = _float("RISK_PCT",          0.5)
LEVERAGE         = _int("LEVERAGE",             10)
MAX_OPEN_TRADES  = _int("MAX_OPEN_TRADES",       3)
MAX_DAILY_TRADES = _int("MAX_DAILY_TRADES",     20)

# ── Umbrales de señal ─────────────────────────────────────────────────────────
MIN_SCORE  = _float("MIN_SCORE",  55.0)
FUEL_SCORE = _float("FUEL_SCORE", 62.0)
SUP_SCORE  = _float("SUP_SCORE",  78.0)
MIN_TIER   = _raw("MIN_TIER", "FUEL").upper()

# ── Entrada ───────────────────────────────────────────────────────────────────
REQUIRE_TL_BREAK = _bool("REQUIRE_TL_BREAK", True)
HTF_MIN_ALIGNED  = _int("HTF_MIN_ALIGNED", 2)

# ── Scanner ───────────────────────────────────────────────────────────────────
SCAN_INTERVAL   = _int("SCAN_INTERVAL",       60)
TOP_N_SYMBOLS   = _int("TOP_N_SYMBOLS",         0)
BLACKLIST       = set(_list("BLACKLIST",       ""))
MIN_VOLUME_USDT = _float("MIN_VOLUME_USDT", 5_000_000.0)

# ── Timeframes ────────────────────────────────────────────────────────────────
TIMEFRAME      = os.getenv("TIMEFRAME",      "3m")
HTF_TIMEFRAME  = os.getenv("HTF_TIMEFRAME",  "15m")
HTF2_TIMEFRAME = os.getenv("HTF2_TIMEFRAME", "1h")
HTF5_TIMEFRAME = os.getenv("HTF5_TIMEFRAME", "4h")

# ── ATR / SL / TP ─────────────────────────────────────────────────────────────
ATR_LEN      = _int("ATR_LEN",        10)
SL_ATR_MULT  = _float("SL_ATR_MULT",  2.0)
TP1_ATR_MULT = _float("TP1_ATR_MULT", 1.5)
TP2_ATR_MULT = _float("TP2_ATR_MULT", 2.5)

# ── ADX ───────────────────────────────────────────────────────────────────────
ADX_LEN     = _int("ADX_LEN", 14)
ADX_TREND   = _float("ADX_TREND",   25.0)
ADX_LATERAL = _float("ADX_LATERAL", 20.0)

# ── Kelly ─────────────────────────────────────────────────────────────────────
KELLY_WIN_RATE = _float("KELLY_WIN_RATE", 0.60)
KELLY_RR       = _float("KELLY_RR",        1.5)
KELLY_FRACTION = _float("KELLY_FRACTION",  0.25)

# ── Circuit Breaker ───────────────────────────────────────────────────────────
CB_ENABLED  = _bool("CB_ENABLED",   True)
CB_ATR_MULT = _float("CB_ATR_MULT", 3.0)
CB_BARS     = _int("CB_BARS",       10)

# ── Gestión de posiciones ─────────────────────────────────────────────────────
POSITION_CHECK_INTERVAL = _int("POSITION_CHECK_INTERVAL",   30)
BREAKEVEN_ATR_MULT      = _float("BREAKEVEN_ATR_MULT",       1.0)

# ── Límite de pérdida diaria ──────────────────────────────────────────────────
DAILY_LOSS_PCT = _float("DAILY_LOSS_PCT", 2.0)

# ── Notional máximo por trade ─────────────────────────────────────────────────
MAX_NOTIONAL_USDT = _float("MAX_NOTIONAL_USDT", 200.0)

# ── Puerto ────────────────────────────────────────────────────────────────────
PORT = _int("PORT", 8080)
