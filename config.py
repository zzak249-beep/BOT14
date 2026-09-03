"""
Configuración del bot Wavelet MRA.

═══════════════════════════════════════════════════════════════════════
POR QUÉ NO ABRÍA NADA (medido en los logs del 03-09-2026)
═══════════════════════════════════════════════════════════════════════
    Ciclo completo · 326 símbolos · 0 señales
    sin amplitud: 316 · sin dominancia: 10 · sin cruce: 1

316 de 326 morían en el PRIMER filtro. La causa es aritmética:

    cover = ATR% / COST_ROUNDTRIP_PCT   ->   0.25% de coste
    cover >= MIN_COST_COVER (6.0)       ->   exige ATR >= 1.50%

Un ATR del 1,5% EN UNA VELA DE 5 MINUTOS casi no existe: la lista de
vigilancia del propio bot mostraba 0.62%, 0.68%, 1.03% como los más
volátiles del universo. El filtro no estaba estricto, estaba imposible.

Y había un segundo cerrojo redundante detrás: MAX_COST_IN_R=0.20 con
SL_ATR=1.5 exige ATR >= 0.25/(0.20*1.5) = 0.83%. Dos puertas midiendo
lo mismo con números distintos.

Ahora hay UNA sola idea —que el coste no se coma la operación— y el
umbral efectivo se imprime al arrancar en vez de quedarse implícito:

    ATR mínimo efectivo = max(MIN_ATR_PCT,
                              COST*MIN_COST_COVER,
                              COST/(MAX_COST_IN_R*SL_ATR))

Con los valores de abajo: 0.40% en 5m. Alcanzable a diario por un
subconjunto real del universo.
"""
import os

CODE_VERSION = "wavelet-2.0"


def _bool(n, d=False):
    return os.getenv(n, str(d)).strip().strip('"').strip("'").lower() in ("1", "true", "yes", "si", "sí")


def _float(n, d):
    v = os.getenv(n)
    if v is None:
        return float(d)
    try:
        return float(v.strip().strip('"').strip("'"))
    except (TypeError, ValueError):
        return float(d)


def _int(n, d):
    v = os.getenv(n)
    if v is None:
        return int(d)
    try:
        return int(float(v.strip().strip('"').strip("'")))
    except (TypeError, ValueError):
        return int(d)


def _str(n, d=""):
    return (os.getenv(n, d) or "").strip().strip('"').strip("'")


MODE = _str("MODE", "SIGNAL").upper()
LIVE_CONFIRMED = _bool("LIVE_CONFIRMED", False)

BINGX_API_KEY = _str("BINGX_API_KEY")
BINGX_API_SECRET = _str("BINGX_API_SECRET")
BINGX_BASE_URL = _str("BINGX_BASE_URL", "https://open-api.bingx.com")
RECV_WINDOW = _int("RECV_WINDOW", 10000)
# AUTO detecta si la cuenta está en cobertura (hedge) o unidireccional.
# Mandar positionSide=LONG en una cuenta unidireccional hace que BingX
# rechace TODAS las órdenes. Es el fallo más caro de diagnosticar
# porque el bot parece funcionar hasta la primera entrada real.
POSITION_MODE = _str("POSITION_MODE", "AUTO").upper()

TELEGRAM_TOKEN = (_str("TELEGRAM_TOKEN") or _str("TELEGRAM_BOT_TOKEN"))
TELEGRAM_CHAT_ID = (_str("TELEGRAM_CHAT_ID") or _str("CHAT_ID"))

# ── Motor wavelet ─────────────────────────────────────────────────────
TIMEFRAME = _str("TIMEFRAME", "5m")
TIMEFRAMES = [t.strip() for t in _str("TIMEFRAMES").split(",") if t.strip()] or [TIMEFRAME]
LOOKBACK_ENERGY = _int("LOOKBACK_ENERGY", 40)
APPROX_LEN = _int("APPROX_LEN", 8)
ATR_LEN = _int("ATR_LEN", 14)

NORMALIZE_SCALES = _bool("NORMALIZE_SCALES", True)
DOMINANCE_THRESHOLD = _float("DOMINANCE_THRESHOLD", 1.30)

ALLOW_LONG = _bool("ALLOW_LONG", True)
ALLOW_SHORT = _bool("ALLOW_SHORT", True)

USE_VOL_FILTER = _bool("USE_VOL_FILTER", True)
VOL_LEN = _int("VOL_LEN", 20)
VOL_MULT = _float("VOL_MULT", 1.2)

USE_HTF_FILTER = _bool("USE_HTF_FILTER", True)
HTF_MA_LEN = _int("HTF_MA_LEN", 200)

# ── Salidas ───────────────────────────────────────────────────────────
SL_ATR = _float("SL_ATR", 1.5)
TP_ATR = _float("TP_ATR", 2.5)
USE_TRAILING = _bool("USE_TRAILING", False)
TRAIL_ATR = _float("TRAIL_ATR", 2.0)
TRAIL_START_R = _float("TRAIL_START_R", 1.0)
MAX_TRADE_MINUTES = _int("MAX_TRADE_MINUTES", 120)
USE_TIME_EXIT = _bool("USE_TIME_EXIT", True)
TIME_EXIT_ONLY_LOSING = _bool("TIME_EXIT_ONLY_LOSING", True)

# ── Coste y liquidez (RECALIBRADO) ────────────────────────────────────
# 0.15% = taker 0.05% x2 + deslizamiento estimado. El 0.25% anterior era
# una estimación pesimista que, multiplicada por MIN_COST_COVER=6,
# cerraba el bot entero.
COST_ROUNDTRIP_PCT = _float("COST_ROUNDTRIP_PCT", 0.15)
MIN_ATR_PCT = _float("MIN_ATR_PCT", 0.30)
MIN_COST_COVER = _float("MIN_COST_COVER", 2.0)
MAX_COST_IN_R = _float("MAX_COST_IN_R", 0.25)
MAX_RISK_PCT = _float("MAX_RISK_PCT", 4.0)
MIN_RISK_PCT = _float("MIN_RISK_PCT", 0.0)
MIN_QUOTE_VOLUME_24H = _float("MIN_QUOTE_VOLUME_24H", 2_000_000.0)

# ── Universo ──────────────────────────────────────────────────────────
SCAN_INTERVAL_SEC = _int("SCAN_INTERVAL_SEC", 60)
MAX_SYMBOLS = _int("MAX_SYMBOLS", 400)
SCAN_CONCURRENCY = _int("SCAN_CONCURRENCY", 8)
SYMBOLS_REFRESH_HOURS = _float("SYMBOLS_REFRESH_HOURS", 6.0)
SYMBOL_WHITELIST = [s.strip().upper() for s in _str("SYMBOL_WHITELIST").split(",") if s.strip()]
EXCLUDE_PREFIXES = [p.strip().upper() for p in _str("EXCLUDE_PREFIXES", "NC").split(",") if p.strip()]

# ── Riesgo ────────────────────────────────────────────────────────────
RISK_PCT = _float("RISK_PCT", 0.5)
MAX_CONCURRENT = _int("MAX_CONCURRENT", 1)
# Límite de posiciones de TODA la cuenta. 0 = desactivado. Si otro bot
# de la flota comparte cuenta y ya tiene 3 abiertas, este bot no abriría
# NINGUNA — y con el mensaje de aviso ahora se ve en Telegram.
MAX_TOTAL_POSITIONS = _int("MAX_TOTAL_POSITIONS", 3)
LEVERAGE = _int("LEVERAGE", 2)
MARGIN_MODE = _str("MARGIN_MODE", "ISOLATED").upper()
MAX_CONSECUTIVE_LOSSES = _int("MAX_CONSECUTIVE_LOSSES", 3)
COOLDOWN_MINUTES = _int("COOLDOWN_MINUTES", 120)
MAX_DAILY_LOSS_R = _float("MAX_DAILY_LOSS_R", 3.0)
COOLDOWN_BARS = _int("COOLDOWN_BARS", 4)

# MARKET por defecto. Con LIMIT el bot mandaba la orden y daba la
# posición por abierta sin comprobar el llenado: si no se llenaba,
# ocupaba el único hueco de MAX_CONCURRENT para siempre. Ahora hay
# gestión real de pendientes, pero MARKET es lo que de verdad entra.
ENTRY_TYPE = _str("ENTRY_TYPE", "MARKET").upper()
LIMIT_OFFSET_PCT = _float("LIMIT_OFFSET_PCT", 0.05)
LIMIT_TTL_MIN = _int("LIMIT_TTL_MIN", 10)

# Lote mínimo del contrato: con 135 USDT y 0.5% de riesgo, el tamaño
# calculado cae por debajo del mínimo en muchos símbolos. Antes se
# descartaba la señal; ahora se sube al mínimo SIEMPRE QUE el riesgo
# resultante no supere este tope.
ALLOW_MIN_QTY_BUMP = _bool("ALLOW_MIN_QTY_BUMP", True)
MAX_RISK_PER_TRADE_PCT = _float("MAX_RISK_PER_TRADE_PCT", 1.5)
MIN_NOTIONAL_USDT = _float("MIN_NOTIONAL_USDT", 2.0)
MARGIN_USE_MAX_PCT = _float("MARGIN_USE_MAX_PCT", 90.0)

# ── Avisos ────────────────────────────────────────────────────────────
SIGNAL_COOLDOWN_MIN = _int("SIGNAL_COOLDOWN_MIN", 60)
WATCHLIST_MIN = _int("WATCHLIST_MIN", 30)
DAILY_SUMMARY = _bool("DAILY_SUMMARY", True)
DAILY_SUMMARY_HOUR_UTC = _int("DAILY_SUMMARY_HOUR_UTC", 7)
HEARTBEAT_HOURS = _int("HEARTBEAT_HOURS", 12)
IDLE_ALERT_DAYS = _int("IDLE_ALERT_DAYS", 5)
ZOMBIE_ALERT_HOURS = _float("ZOMBIE_ALERT_HOURS", 6.0)
BTC_CONTEXT = _bool("BTC_CONTEXT", True)
# Diagnóstico del embudo en cada ciclo: mediana y p90 del ATR% del
# universo frente al umbral. Es lo que habría enseñado el fallo en el
# primer ciclo en vez de en el tercer día.
SCAN_DIAG = _bool("SCAN_DIAG", True)

# ── Funding ───────────────────────────────────────────────────────────
FUNDING_ALERTS = _bool("FUNDING_ALERTS", True)
FUNDING_EXTREMO = _float("FUNDING_EXTREMO", 0.05)
FUNDING_ALERT_MIN = _int("FUNDING_ALERT_MIN", 120)
CARRY_MAX_DIAS_COBERTURA = _float("CARRY_MAX_DIAS_COBERTURA", 3.0)
FUNDING_MIN_USDT_DIA = _float("FUNDING_MIN_USDT_DIA", 0.10)
SALDO_ESTIMADO = _float("SALDO_ESTIMADO", 135.0)
BTC_FILTER = _bool("BTC_FILTER", False)
BTC_MIN_24H = _float("BTC_MIN_24H", -3.0)

STATE_PATH = _str("STATE_PATH", "/data/state_wavelet.json")
LOG_LEVEL = _str("LOG_LEVEL", "INFO").upper()


def atr_minimo_efectivo() -> float:
    """
    El ATR% mínimo que un símbolo necesita para pasar los filtros de
    coste. Se calcula, no se declara: así no vuelve a haber dos puertas
    con números distintos y una de ellas cerrada.
    """
    gates = [MIN_ATR_PCT, COST_ROUNDTRIP_PCT * MIN_COST_COVER]
    if MAX_COST_IN_R > 0 and SL_ATR > 0:
        gates.append(COST_ROUNDTRIP_PCT / (MAX_COST_IN_R * SL_ATR))
    return max(gates)


def _tf_min(tf: str | None = None) -> int:
    return {"1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
            "1h": 60, "2h": 120, "4h": 240}.get(tf or TIMEFRAME, 5)


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
