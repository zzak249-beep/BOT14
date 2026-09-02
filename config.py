"""
Configuración del bot Wavelet MRA.

MODE=SIGNAL por defecto. Esta estrategia tiene CERO operaciones medidas
y su filtro central estaba mal calibrado en el original, así que los
números del hilo de partida (71%, Sharpe 2.44) no son una referencia
válida: describen una versión con el filtro encendido el 92% del
tiempo. Mide antes con backtest.py.
"""
import os


def _bool(n, d=False):
    return os.getenv(n, str(d)).strip().lower() in ("1", "true", "yes", "si", "sí")


def _float(n, d):
    try:
        return float(os.getenv(n, d))
    except (TypeError, ValueError):
        return d


def _int(n, d):
    try:
        return int(os.getenv(n, d))
    except (TypeError, ValueError):
        return d


MODE = os.getenv("MODE", "SIGNAL").strip().upper()
LIVE_CONFIRMED = _bool("LIVE_CONFIRMED", False)

BINGX_API_KEY = os.getenv("BINGX_API_KEY", "").strip()
BINGX_API_SECRET = os.getenv("BINGX_API_SECRET", "").strip()
BINGX_BASE_URL = os.getenv("BINGX_BASE_URL", "https://open-api.bingx.com").strip()

TELEGRAM_TOKEN = (os.getenv("TELEGRAM_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or os.getenv("CHAT_ID") or "").strip()

# ── Motor wavelet ─────────────────────────────────────────────────────
TIMEFRAME = os.getenv("TIMEFRAME", "5m").strip()
# Varios timeframes a la vez, separados por comas. Por defecto solo uno.
TIMEFRAMES = [t.strip() for t in os.getenv("TIMEFRAMES", "").split(",") if t.strip()] or [TIMEFRAME]
LOOKBACK_ENERGY = _int("LOOKBACK_ENERGY", 40)
APPROX_LEN = _int("APPROX_LEN", 8)
ATR_LEN = _int("ATR_LEN", 14)

# LA CORRECCIÓN CENTRAL. Con normalización por escala, el ratio en ruido
# puro tiene mediana 0.75 y percentil 75 en 1.00, así que 1.30 deja
# pasar aproximadamente el cuartil superior. Sin normalizar (modo
# original) el ruido puro ya da mediana 3.04 y habría que poner el
# umbral en 4.0 para filtrar algo — con 1.5 se enciende el 92% del
# tiempo y no filtra nada.
NORMALIZE_SCALES = _bool("NORMALIZE_SCALES", True)
DOMINANCE_THRESHOLD = _float("DOMINANCE_THRESHOLD", 1.30)

ALLOW_LONG = _bool("ALLOW_LONG", True)
ALLOW_SHORT = _bool("ALLOW_SHORT", True)

# ── Las tres correcciones del cruce de medias ─────────────────────────
# Un cruce sin filtros dispara 30-50 veces por trimestre con 60-65% de
# perdedoras y factor de ganancias ~1.0: breakeven menos comisiones. La
# literatura coincide en tres arreglos, y aquí están los tres.
#
# (1) RÉGIMEN — ya lo cubre DOMINANCE_THRESHOLD, que es el equivalente
#     al filtro de ADX que recomiendan: no operar cruces en mercado
#     plano.
#
# (2) VOLUMEN en la vela del cruce. Activado por defecto: las fuentes
#     dicen que "este filtro por sí solo elimina una porción
#     significativa de los whipsaws", porque los cruces con poco volumen
#     en mercado fino se giran casi siempre.
USE_VOL_FILTER = _bool("USE_VOL_FILTER", True)
VOL_LEN = _int("VOL_LEN", 20)
VOL_MULT = _float("VOL_MULT", 1.2)

# (3) TENDENCIA DEL TIMEFRAME SUPERIOR. Es la corrección que más
#     recortaba señales en los estudios ("elimina la mayoría de los
#     fallos a contratendencia"). Solo largos si el precio está sobre su
#     media larga, solo cortos si está por debajo. Reduce las señales
#     aproximadamente a la mitad — esa es la idea.
USE_HTF_FILTER = _bool("USE_HTF_FILTER", True)
HTF_MA_LEN = _int("HTF_MA_LEN", 200)

# ── Salidas ───────────────────────────────────────────────────────────
SL_ATR = _float("SL_ATR", 1.5)
TP_ATR = _float("TP_ATR", 2.5)
# "Los trailing stops típicamente superan a las salidas por cruce
# contrario porque capturan la continuación después de ganar el edge
# inicial." Disponible, apagado: cámbialo y compara en el backtester en
# vez de creértelo.
USE_TRAILING = _bool("USE_TRAILING", False)
TRAIL_ATR = _float("TRAIL_ATR", 2.0)
TRAIL_START_R = _float("TRAIL_START_R", 1.0)
MAX_TRADE_MINUTES = _int("MAX_TRADE_MINUTES", 120)
USE_TIME_EXIT = _bool("USE_TIME_EXIT", True)
TIME_EXIT_ONLY_LOSING = _bool("TIME_EXIT_ONLY_LOSING", True)

# ── Coste y liquidez ──────────────────────────────────────────────────
COST_ROUNDTRIP_PCT = _float("COST_ROUNDTRIP_PCT", 0.25)
MIN_ATR_PCT = _float("MIN_ATR_PCT", 0.5)
MIN_COST_COVER = _float("MIN_COST_COVER", 6.0)
MAX_COST_IN_R = _float("MAX_COST_IN_R", 0.20)
MAX_RISK_PCT = _float("MAX_RISK_PCT", 4.0)
# Suelo de riesgo: si el stop queda demasiado cerca, el coste pesa
# demasiado. MAX_COST_IN_R ya lo cubre, pero el backtester heredado lo
# consulta por separado.
MIN_RISK_PCT = _float("MIN_RISK_PCT", 0.0)
MIN_QUOTE_VOLUME_24H = _float("MIN_QUOTE_VOLUME_24H", 2_000_000.0)

# ── Universo ──────────────────────────────────────────────────────────
SCAN_INTERVAL_SEC = _int("SCAN_INTERVAL_SEC", 60)
MAX_SYMBOLS = _int("MAX_SYMBOLS", 400)
SCAN_CONCURRENCY = _int("SCAN_CONCURRENCY", 8)
SYMBOL_WHITELIST = [s.strip().upper() for s in os.getenv("SYMBOL_WHITELIST", "").split(",") if s.strip()]
EXCLUDE_PREFIXES = [p.strip().upper() for p in os.getenv("EXCLUDE_PREFIXES", "NC").split(",") if p.strip()]

# ── Riesgo ────────────────────────────────────────────────────────────
RISK_PCT = _float("RISK_PCT", 0.5)
MAX_CONCURRENT = _int("MAX_CONCURRENT", 1)
MAX_TOTAL_POSITIONS = _int("MAX_TOTAL_POSITIONS", 3)
LEVERAGE = _int("LEVERAGE", 2)
MARGIN_MODE = os.getenv("MARGIN_MODE", "ISOLATED").strip().upper()
MAX_CONSECUTIVE_LOSSES = _int("MAX_CONSECUTIVE_LOSSES", 3)
COOLDOWN_MINUTES = _int("COOLDOWN_MINUTES", 120)
MAX_DAILY_LOSS_R = _float("MAX_DAILY_LOSS_R", 3.0)
COOLDOWN_BARS = _int("COOLDOWN_BARS", 4)
ENTRY_TYPE = os.getenv("ENTRY_TYPE", "LIMIT").strip().upper()
LIMIT_OFFSET_PCT = _float("LIMIT_OFFSET_PCT", 0.05)
LIMIT_TTL_MIN = _int("LIMIT_TTL_MIN", 10)

# ── Avisos ────────────────────────────────────────────────────────────
SIGNAL_COOLDOWN_MIN = _int("SIGNAL_COOLDOWN_MIN", 60)
WATCHLIST_MIN = _int("WATCHLIST_MIN", 30)
DAILY_SUMMARY = _bool("DAILY_SUMMARY", True)
DAILY_SUMMARY_HOUR_UTC = _int("DAILY_SUMMARY_HOUR_UTC", 7)
HEARTBEAT_HOURS = _int("HEARTBEAT_HOURS", 12)
IDLE_ALERT_DAYS = _int("IDLE_ALERT_DAYS", 5)
# Horas tras las que una posición abierta se considera olvidada. No se
# cierra sola —esa decisión es del usuario— pero avisar es obligatorio.
ZOMBIE_ALERT_HOURS = _float("ZOMBIE_ALERT_HOURS", 6.0)
BTC_CONTEXT = _bool("BTC_CONTEXT", True)

# ── Funding ───────────────────────────────────────────────────────────
# El carry (comprar spot + vender perp) es edge ESTRUCTURAL, no
# estadístico: los fondos que lo hacen reportan drawdowns bajo el 1%.
# Pero con 135 USDT da 2 céntimos al día a funding normal y tarda 13
# días en cubrir la comisión de abrir. Por eso el bot NO monta carry:
# solo avisa cuando el funding está tan alto que sí compensaría, con el
# cálculo hecho sobre el saldo real.
FUNDING_ALERTS = _bool("FUNDING_ALERTS", True)
FUNDING_EXTREMO = _float("FUNDING_EXTREMO", 0.05)
FUNDING_ALERT_MIN = _int("FUNDING_ALERT_MIN", 120)
CARRY_MAX_DIAS_COBERTURA = _float("CARRY_MAX_DIAS_COBERTURA", 3.0)
# Saldo de referencia para el cálculo del carry cuando el bot está en
# SIGNAL y no puede consultar el balance real.
SALDO_ESTIMADO = _float("SALDO_ESTIMADO", 135.0)
# Filtro opcional y APAGADO: no abrir largos si BTC cae fuerte, porque
# las alts caen más. Sin datos propios que lo respalden, activarlo sería
# añadir una creencia al sistema.
BTC_FILTER = _bool("BTC_FILTER", False)
BTC_MIN_24H = _float("BTC_MIN_24H", -3.0)

STATE_PATH = os.getenv("STATE_PATH", "/data/state_wavelet.json")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").strip().upper()


def _tf_min() -> int:
    return {"1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240}.get(TIMEFRAME, 5)


def max_trade_seconds() -> int:
    return MAX_TRADE_MINUTES * 60


def is_live() -> bool:
    return MODE == "LIVE" and LIVE_CONFIRMED and bool(BINGX_API_KEY) and bool(BINGX_API_SECRET)


def describe() -> str:
    if is_live():
        return "LIVE — enviando órdenes reales a BingX"
    if MODE == "LIVE":
        return "LIVE pedido pero SIN confirmar — sigue en SIGNAL"
    return "SIGNAL — solo avisos, no toca el exchange"
