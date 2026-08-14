"""
Configuracion central. Todo se lee de variables de entorno.

Railway: pega esto en el editor RAW de variables (una linea KEY=VALUE
por fila, SIN comillas). Las comillas se cuelan como parte del valor
literal y pueden dejar un booleano como el string 'true' con comillas,
lo que algunos parsers tratan como truthy pase lo que pase.
"""
import os
import sys
import logging
from typing import Optional

CODE_VERSION = "bingx-ict-scanner v1.7.0"


def _clean(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    v = v.strip()
    if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
        v = v[1:-1]
    return v


def _bool(name: str, default: bool) -> bool:
    v = _clean(os.getenv(name))
    if not v:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def _int(name: str, default: int) -> int:
    v = _clean(os.getenv(name))
    try:
        return int(v) if v else default
    except ValueError:
        return default


def _float(name: str, default: float) -> float:
    v = _clean(os.getenv(name))
    try:
        return float(v) if v else default
    except ValueError:
        return default


def _set(name: str, default: str) -> set:
    v = _clean(os.getenv(name)) or default
    return {s.strip().upper() for s in v.split(",") if s.strip()}


# ── Credenciales ──
BINGX_API_KEY = _clean(os.getenv("BINGX_API_KEY"))
BINGX_API_SECRET = _clean(os.getenv("BINGX_API_SECRET"))
BINGX_BASE_URL = _clean(os.getenv("BINGX_BASE_URL")) or "https://open-api.bingx.com"

TELEGRAM_BOT_TOKEN = _clean(os.getenv("TELEGRAM_BOT_TOKEN"))
TELEGRAM_CHAT_ID = _clean(os.getenv("TELEGRAM_CHAT_ID"))

# ── Modo ──
# SIGNAL = escanea, notifica por Telegram, guarda estadisticas en papel. No coloca ordenes.
# LIVE   = coloca ordenes reales en BingX.
MODE = (_clean(os.getenv("MODE")) or "SIGNAL").upper()
if MODE not in ("SIGNAL", "LIVE"):
    MODE = "SIGNAL"

# Modo de posicion de la CUENTA BingX. Debes saberlo de antemano:
# app BingX > Futuros > Preferencias > Modo de posicion.
# HEDGE  = posiciones LONG y SHORT independientes (positionSide LONG/SHORT)
# ONEWAY = una sola posicion neta (positionSide BOTH)
POSITION_MODE = (_clean(os.getenv("POSITION_MODE")) or "HEDGE").upper()

# ── Universo de simbolos ──
QUOTE_ASSET = _clean(os.getenv("QUOTE_ASSET")) or "USDT"
SYMBOL_BLACKLIST = _set("SYMBOL_BLACKLIST", "")
SYMBOL_WHITELIST = _set("SYMBOL_WHITELIST", "")
MIN_24H_VOLUME_USDT = _float("MIN_24H_VOLUME_USDT", 0.0)
SYMBOL_REFRESH_MIN = _int("SYMBOL_REFRESH_MIN", 60)

# ── Majors vs altcoins -- hipotesis sin confirmar todavia, en backtest de
# Pine se probo UN major (XRP: 0 operaciones en 3.5 semanas) mientras que
# 9 altcoins de menor capitalizacion si generaron señales, algunas rentables
# y otras no. La teoria: sweeps de liquidez son mechas de caza de stops,
# mas frecuentes donde hay mas retail apalancado y menos profundidad --
# eso describe altcoins de menor capitalizacion, no majors de alta liquidez.
# Sin confirmar en BTC/ETH todavia. Por eso: EXCLUDE_MAJORS empieza en
# false (no se excluye nada por defecto), pero el tracking por categoria
# (tier_stats) esta SIEMPRE activo -- para validar o descartar la hipotesis
# con datos reales del bot, no solo con el backtest de Pine.
MAJOR_SYMBOLS = _set("MAJOR_SYMBOLS", "BTC-USDT,ETH-USDT,XRP-USDT,BNB-USDT,SOL-USDT")
EXCLUDE_MAJORS = _bool("EXCLUDE_MAJORS", False)

# ── Timeframes ──
TIMEFRAME = _clean(os.getenv("TIMEFRAME")) or "5m"
HTF_TIMEFRAME = _clean(os.getenv("HTF_TIMEFRAME")) or "1h"
USE_HTF_BIAS = _bool("USE_HTF_BIAS", True)
HTF_EMA_LEN = _int("HTF_EMA_LEN", 50)

# ── Kill zones (hora de Nueva York, DST-aware via zoneinfo) ──
USE_KILL_ZONES = _bool("USE_KILL_ZONES", True)
KZ_LONDON = _bool("KZ_LONDON", True)
KZ_NY_AM = _bool("KZ_NY_AM", True)
KZ_NY_PM = _bool("KZ_NY_PM", False)
KZ_ASIA = _bool("KZ_ASIA", False)
KZ_ONLY_ENTRY = _bool("KZ_ONLY_ENTRY", False)

REFERENCE_RANGE = _clean(os.getenv("REFERENCE_RANGE")) or "0000-0830"  # HHMM-HHMM, hora NY

# ── Calidad del setup (equivalente a ict_killzone_v2.pine) ──
MIN_GAP_ATR = _float("MIN_GAP_ATR", 0.25)
DISPLACEMENT_ATR = _float("DISPLACEMENT_ATR", 0.80)
ENTRY_MODE = (_clean(os.getenv("ENTRY_MODE")) or "CONFIRMATION").upper()  # CONFIRMATION | CE
CE_EXPIRY_BARS = _int("CE_EXPIRY_BARS", 12)
FVG_EXPIRY_BARS = _int("FVG_EXPIRY_BARS", 30)
SWEEP_EXPIRY_BARS = _int("SWEEP_EXPIRY_BARS", 20)
USE_EQ = _bool("USE_EQ", True)

# ── Calidad del sweep: profundidad de mecha ──
# Idea de Liquidity Reaper [JOAT]: exige que la mecha mas alla del nivel
# barrido sea al menos este % del rango total de la vela -- distingue
# un rechazo fuerte de un toque debil. 0 = sin filtro (comportamiento
# anterior). No crea señales nuevas, solo descarta sweeps mecha-debiles.
SWEEP_MIN_WICK_PCT = _float("SWEEP_MIN_WICK_PCT", 0.0)

# ── Deteccion de sweep intra-vela ──
# Por defecto, el sweep solo se evalua una vez que la vela cierra (hasta
# 1 TIMEFRAME completo de retraso desde que ocurre hasta que el bot se
# entera). Con esto activo, el sweep tambien se comprueba contra la vela
# AUN EN FORMACION -- se entera en el siguiente ciclo de escaneo
# (SCAN_INTERVAL_SEC) en vez de esperar a que cierre la vela entera.
# Riesgo real, no cosmetico: un sweep visto a mitad de vela es
# provisional -- el precio puede seguir moviendose y invalidarlo antes
# del cierre real. Por eso se re-valida en cuanto la vela cierra de
# verdad (ver evaluate_symbol) y se invalida si ya no se sostiene.
# Solo afecta al SWEEP -- la geometria de la FVG sigue exigiendo velas
# cerradas, no tiene sentido evaluarla a medio formar.
INTRA_CANDLE_SWEEP = _bool("INTRA_CANDLE_SWEEP", False)

# ── Lead-lag entre simbolos ──
# BTC suele barrer un nivel primero; los alts correlacionados lo siguen
# segundos-minutos despues. Puramente informativo (Signal.lead_confirmed):
# no filtra nada, solo etiqueta si el simbolo lider se movio en la misma
# direccion recientemente. Off por defecto.
USE_LEAD_LAG = _bool("USE_LEAD_LAG", False)
LEAD_SYMBOL = _clean(os.getenv("LEAD_SYMBOL")) or "BTC-USDT"
LEAD_LAG_WINDOW_MIN = _int("LEAD_LAG_WINDOW_MIN", 15)
EQ_PIVOT_LEN = _int("EQ_PIVOT_LEN", 15)
EQ_TOL_ATR = _float("EQ_TOL_ATR", 0.10)
USE_PREMIUM_DISCOUNT = _bool("USE_PREMIUM_DISCOUNT", False)
DIRECTION = (_clean(os.getenv("DIRECTION")) or "BOTH").upper()  # BOTH | LONG | SHORT

# ── Ruta A: Reversion (sweep + FVG) -- la que ya existia, ahora toggleable ──
USE_PATH_A = _bool("USE_PATH_A", True)

# ── Ruta B: Continuacion (CHoCH + retroceso a golden zone) ──
# Complementa la Ruta A (sweep+FVG, ya existente) igual que en
# fibstruct_ict_confluence.pine: dos rutas separadas y etiquetadas,
# no fusionadas. Si ambas disparan direcciones contrarias en el mismo
# ciclo, se anulan las dos (mismo criterio que el resto del embudo).
USE_PATH_B = _bool("USE_PATH_B", True)
SWING_LEN = _int("SWING_LEN", 10)
GZ_LOW = _float("GZ_LOW", 0.5)     # borde cercano de la golden zone (fraccion fib)
GZ_HIGH = _float("GZ_HIGH", 0.618)  # borde lejano
CONT_CONFIRM = (_clean(os.getenv("CONT_CONFIRM")) or "ENGULF").upper()  # ENGULF | TOUCH

# ── Funding rate / Open Interest (opcionales, apagados por defecto) ──
# Filtros nuevos sin validar en vivo todavia -- pruebalos en SIGNAL
# comparando el desglose de resultados con y sin ellos antes de fiarte.
USE_FUNDING_FILTER = _bool("USE_FUNDING_FILTER", False)
# LONG exige funding <= -FUNDING_MIN_ABS (cortos pagando largos = posicionamiento
# cargado a corto, favorece el rebote). SHORT exige funding >= +FUNDING_MIN_ABS.
FUNDING_MIN_ABS = _float("FUNDING_MIN_ABS", 0.0001)  # 0.01%

USE_OI_FILTER = _bool("USE_OI_FILTER", False)
# Si el interes abierto sube mas de este % entre el barrido y la confirmacion,
# se descarta la señal: sugiere entrada de posicion nueva contra la reversion,
# no cierre de posiciones (flush) -- la lectura clasica de OI+precio en reversiones.
OI_MAX_INCREASE_PCT = _float("OI_MAX_INCREASE_PCT", 3.0)

# ── Riesgo ──
MIN_RR = _float("MIN_RR", 1.5)
SL_BUFFER_ATR = _float("SL_BUFFER_ATR", 0.5)
RR_FIXED_FALLBACK = _float("RR_FIXED_FALLBACK", 2.5)
USE_RANGE_TP = _bool("USE_RANGE_TP", True)
RISK_PCT = _float("RISK_PCT", 1.0)
LEVERAGE = _int("LEVERAGE", 5)
USE_PARTIAL_TP = _bool("USE_PARTIAL_TP", True)
PARTIAL_TP_R = _float("PARTIAL_TP_R", 1.0)
PARTIAL_TP_PCT = _int("PARTIAL_TP_PCT", 50)
BE_OFFSET_R = _float("BE_OFFSET_R", 0.05)
MAX_TRADES_PER_DAY = _int("MAX_TRADES_PER_DAY", 8)
MAX_CONCURRENT_POSITIONS = _int("MAX_CONCURRENT_POSITIONS", 5)
MAX_BARS_IN_TRADE = _int("MAX_BARS_IN_TRADE", 0)

# ── Escaneo ──
SCAN_INTERVAL_SEC = _int("SCAN_INTERVAL_SEC", 60)
KLINES_LOOKBACK = _int("KLINES_LOOKBACK", 300)
MAX_CONCURRENT_REQUESTS = _int("MAX_CONCURRENT_REQUESTS", 12)

# ── Estado / logs ──
STATE_FILE = _clean(os.getenv("STATE_FILE")) or "/data/state.json"
LOG_LEVEL = (_clean(os.getenv("LOG_LEVEL")) or "INFO").upper()
HEALTHCHECK_PORT = _int("PORT", 8080)  # Railway inyecta PORT si el servicio es de tipo "web"


def validate() -> None:
    if MODE == "LIVE" and (not BINGX_API_KEY or not BINGX_API_SECRET):
        logging.error("MODE=LIVE requiere BINGX_API_KEY y BINGX_API_SECRET. Abortando.")
        sys.exit(1)
    if not BINGX_API_KEY or not BINGX_API_SECRET:
        logging.warning("Sin credenciales BingX: solo se podran leer datos publicos.")
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("Sin TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID: el bot correra sin notificaciones.")
    if POSITION_MODE not in ("HEDGE", "ONEWAY"):
        logging.warning("POSITION_MODE=%s no reconocido, se usara HEDGE.", POSITION_MODE)


def setup_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s | %(levelname)-7s | %(name)-10s | %(message)s",
        stream=sys.stdout,
    )
    log = logging.getLogger("config")
    log.info("=" * 64)
    log.info("%s", CODE_VERSION)
    log.info("MODE=%s  POSITION_MODE=%s  TF=%s  HTF=%s", MODE, POSITION_MODE, TIMEFRAME, HTF_TIMEFRAME)
    log.info(
        "Riesgo=%.2f%%  MinRR=%.2f  MaxTrades/dia=%d  MaxPosicionesAbiertas=%d",
        RISK_PCT, MIN_RR, MAX_TRADES_PER_DAY, MAX_CONCURRENT_POSITIONS,
    )
    log.info("KillZones: London=%s NYam=%s NYpm=%s Asia=%s (uso=%s)",
              KZ_LONDON, KZ_NY_AM, KZ_NY_PM, KZ_ASIA, USE_KILL_ZONES)
    if MODE == "LIVE":
        log.warning("MODE=LIVE -> el bot va a colocar ordenes reales en BingX.")
    else:
        log.info("MODE=SIGNAL -> solo notificaciones, ninguna orden real.")
    log.info("=" * 64)
