"""
Configuración del bot RSI doble suelo + SuperTrend.

MODE=SIGNAL por defecto. Esta estrategia no tiene ni una operación
medida: los parámetros del Pine original (RSI 10 en vez de 14,
multiplicador 2.5) son ajustes que su autor hizo para dar más señales y
salidas más rentables, o sea que vienen ya optimizados sobre algún
histórico que no es el tuyo. Mídela antes de ponerle dinero.
"""
import os


def _bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "si", "sí")


def _float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


MODE = os.getenv("MODE", "SIGNAL").strip().upper()
LIVE_CONFIRMED = _bool("LIVE_CONFIRMED", False)

BINGX_API_KEY = os.getenv("BINGX_API_KEY", "").strip()
BINGX_API_SECRET = os.getenv("BINGX_API_SECRET", "").strip()
BINGX_BASE_URL = os.getenv("BINGX_BASE_URL", "https://open-api.bingx.com").strip()

TELEGRAM_TOKEN = (os.getenv("TELEGRAM_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or os.getenv("CHAT_ID") or "").strip()

# ── Estrategia (mismos valores que el Pine original) ──────────────────
RSI_LEN = _int("RSI_LEN", 10)
SIG_LEN = _int("SIG_LEN", 10)
TRIGGER_LEVEL = _float("TRIGGER_LEVEL", 50.0)
# 2 = doble suelo (W). Con 1 se opera el primer intento, que es
# justamente el que el autor considera que falla.
TARGET_CROSS = _int("TARGET_CROSS", 2)
ST_PERIOD = _int("ST_PERIOD", 10)
ST_FACTOR = _float("ST_FACTOR", 2.5)
# TENSIÓN REAL DEL PINE ORIGINAL, descubierta al probar el motor:
# un doble suelo ocurre POR DEFINICIÓN durante una caída, así que en el
# momento de la señal el SuperTrend casi siempre está bajista y su línea
# queda POR ENCIMA del precio — no sirve como stop. El Pine no lo nota
# porque su salida solo se dispara en el INSTANTE del giro a bajista:
# si ya estaba bajista, la posición se queda abierta sin protección
# hasta el siguiente giro, que puede tardar días.
#   true  = exigir SuperTrend ya alcista. Menos señales, stop coherente.
#   false = fiel al original. Entra igual, y el stop lo pone el mínimo
#           reciente porque el SuperTrend no puede.
REQUIRE_ST_BULL = _bool("REQUIRE_ST_BULL", True)
SL_SWING_ATR = _float("SL_SWING_ATR", 0.5)
SL_SWING_LOOKBACK = _int("SL_SWING_LOOKBACK", 20)

# ── Objetivo ──────────────────────────────────────────────────────────
# El Pine original NO tiene objetivo: sale solo cuando gira el
# SuperTrend. Aquí es opcional para poder comparar las dos variantes.
USE_TP = _bool("USE_TP", False)
RR_TARGET = _float("RR_TARGET", 2.0)

# ── Universo y filtros ────────────────────────────────────────────────
TIMEFRAME = os.getenv("TIMEFRAME", "15m").strip()
SCAN_INTERVAL_SEC = _int("SCAN_INTERVAL_SEC", 90)
MAX_SYMBOLS = _int("MAX_SYMBOLS", 400)
SYMBOL_WHITELIST = [s.strip().upper() for s in os.getenv("SYMBOL_WHITELIST", "").split(",") if s.strip()]
EXCLUDE_PREFIXES = [p.strip().upper() for p in os.getenv("EXCLUDE_PREFIXES", "NC").split(",") if p.strip()]
MIN_QUOTE_VOLUME_24H = _float("MIN_QUOTE_VOLUME_24H", 3_000_000.0)
# El stop lo pone el SuperTrend, que puede quedar lejísimos. Sin este
# tope, una sola operación puede llevarse un múltiplo del riesgo previsto.
MIN_ATR_PCT = _float("MIN_ATR_PCT", 0.5)
MAX_RISK_PCT = _float("MAX_RISK_PCT", 4.0)
# EL FILTRO QUE FALTABA. El stop lo pone el SuperTrend o el mínimo
# reciente, y a veces queda MUY cerca: un riesgo del 0.69% con 0.25% de
# coste significa que la operación empieza perdiendo 0.36R antes de que
# el precio se mueva. Es el mismo error que ya se corrigió en el otro
# bot: no basta con acotar el stop por arriba, hay que acotarlo también
# por ABAJO en relación al coste.
MIN_RISK_PCT = _float("MIN_RISK_PCT", 1.5)
MAX_COST_IN_R = _float("MAX_COST_IN_R", 0.20)
SCAN_CONCURRENCY = _int("SCAN_CONCURRENCY", 8)

# ── Riesgo ────────────────────────────────────────────────────────────
RISK_PCT = _float("RISK_PCT", 0.25)
MAX_CONCURRENT = _int("MAX_CONCURRENT", 2)
LEVERAGE = _int("LEVERAGE", 2)
MAX_CONSECUTIVE_LOSSES = _int("MAX_CONSECUTIVE_LOSSES", 3)
COOLDOWN_MINUTES = _int("COOLDOWN_MINUTES", 180)
ENTRY_TYPE = os.getenv("ENTRY_TYPE", "LIMIT").strip().upper()
LIMIT_OFFSET_PCT = _float("LIMIT_OFFSET_PCT", 0.05)

# ── Avisos ────────────────────────────────────────────────────────────
# Enfriamiento por símbolo en modo SIGNAL: sin esto la misma señal se
# repite en cada ciclo mientras no cambie la vela.
SIGNAL_COOLDOWN_MIN = _int("SIGNAL_COOLDOWN_MIN", 60)

DAILY_SUMMARY = _bool("DAILY_SUMMARY", True)
DAILY_SUMMARY_HOUR_UTC = _int("DAILY_SUMMARY_HOUR_UTC", 7)
HEARTBEAT_HOURS = _int("HEARTBEAT_HOURS", 12)
IDLE_ALERT_DAYS = _int("IDLE_ALERT_DAYS", 5)

STATE_PATH = os.getenv("STATE_PATH", "/data/state_rsi.json")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").strip().upper()

COST_ROUNDTRIP_PCT = _float("COST_ROUNDTRIP_PCT", 0.25)


def is_live() -> bool:
    return MODE == "LIVE" and LIVE_CONFIRMED and bool(BINGX_API_KEY) and bool(BINGX_API_SECRET)


def describe() -> str:
    if is_live():
        return "LIVE — enviando órdenes reales a BingX"
    if MODE == "LIVE":
        return "LIVE pedido pero SIN confirmar (falta LIVE_CONFIRMED o claves) — sigue en SIGNAL"
    return "SIGNAL — solo avisos, no toca el exchange"
