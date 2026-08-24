"""
Configuración del bot. Todo por variables de entorno (Railway).

MODE=SIGNAL es el valor por defecto Y la recomendación. La estrategia
tiene 35 operaciones medidas: eso no basta para poner dinero. SIGNAL
manda avisos a Telegram y no toca el exchange.
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


# ── Modo ──────────────────────────────────────────────────────────────
# SIGNAL: solo avisa.  LIVE: envía órdenes reales a BingX.
MODE = os.getenv("MODE", "SIGNAL").strip().upper()

# Segundo cerrojo para LIVE: hay que ponerlo a mano, aparte del MODE.
# Dos interruptores para operar de verdad no es paranoia: es que el coste
# de un despliegue equivocado es dinero, y el de un cerrojo extra es un
# minuto de tu tiempo.
LIVE_CONFIRMED = _bool("LIVE_CONFIRMED", False)

# ── BingX ─────────────────────────────────────────────────────────────
BINGX_API_KEY = os.getenv("BINGX_API_KEY", "").strip()
BINGX_API_SECRET = os.getenv("BINGX_API_SECRET", "").strip()
BINGX_BASE_URL = os.getenv("BINGX_BASE_URL", "https://open-api.bingx.com").strip()

# ── Telegram ──────────────────────────────────────────────────────────
# Se aceptan los dos nombres: los bots antiguos del proyecto usan
# TELEGRAM_BOT_TOKEN y este empezó con TELEGRAM_TOKEN. Reutilizar el
# servicio de Railway con las variables de otro bot es lo normal, y que
# el bot se quede mudo por el nombre de una variable es un fallo tonto
# que solo se descubre leyendo los logs con lupa.
TELEGRAM_TOKEN = (
    os.getenv("TELEGRAM_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or ""
).strip()
TELEGRAM_CHAT_ID = (
    os.getenv("TELEGRAM_CHAT_ID") or os.getenv("CHAT_ID") or ""
).strip()

# ── Universo y escaneo ────────────────────────────────────────────────
TIMEFRAME = os.getenv("TIMEFRAME", "5m").strip()
SCAN_INTERVAL_SEC = _int("SCAN_INTERVAL_SEC", 60)
MAX_SYMBOLS = _int("MAX_SYMBOLS", 200)
SYMBOL_WHITELIST = [s.strip().upper() for s in os.getenv("SYMBOL_WHITELIST", "").split(",") if s.strip()]
# Excluye los tokenizados sintéticos de BingX (acciones, materias primas,
# forex). Mismo filtro que el resto de bots del proyecto.
EXCLUDE_PREFIXES = [p.strip().upper() for p in os.getenv("EXCLUDE_PREFIXES", "NC").split(",") if p.strip()]

# ── EL FILTRO QUE MANDA ───────────────────────────────────────────────
# Los backtests dicen: donde la reversión funcionó había ~40x el coste
# de operar (ATR 5-6%); donde no hubo negocio, 6-13x. Este umbral es el
# hallazgo principal de todo el trabajo, no un parámetro más.
MIN_ATR_PCT = _float("MIN_ATR_PCT", 4.0)
# 0.25% y no 0.14%: la comisión es lo de menos. En pares finos, una orden
# a mercado paga entre 0.1% y 0.5% de más POR OPERACIÓN, y en perpetuos
# las cascadas de liquidación amplifican eso justo cuando esta estrategia
# entra — tras un movimiento violento. El 0.14% de antes era la comisión
# sola, que es la parte que no duele.
COST_ROUNDTRIP_PCT = _float("COST_ROUNDTRIP_PCT", 0.25)
MIN_COST_COVER = _float("MIN_COST_COVER", 30.0)

# ── Escáner de universo completo ──────────────────────────────────────
# SCAN_ALL=true recorre TODOS los perpetuos y publica un ranking por
# Telegram cada RANK_INTERVAL_MIN. Sustituye al radar manual de
# TradingView, que solo admite diez símbolos escritos a mano.
SCAN_ALL = _bool("SCAN_ALL", True)
RANK_INTERVAL_MIN = _int("RANK_INTERVAL_MIN", 15)
RANK_TOP_N = _int("RANK_TOP_N", 12)
# Avisar SOLO cuando hay algo que decir. Un mensaje idéntico cada 15
# minutos diciendo "no hay nada" son 96 al día: dejas de mirarlos, y el
# día que llegue una señal de verdad la vas a pasar por alto igual que
# las otras 95. El "no hay nada" ya lo cubren el latido y el resumen.
# "solo_candidatos": avisa únicamente cuando alguno pasa TODOS los filtros.
# "top_siempre":     manda el top por amplitud aunque ninguno sea operable,
#                    marcando cuál pasa y cuál no. Cada mensaje es distinto
#                    (símbolos y cifras cambian), así que no cae en la
#                    fatiga de alertas del mensaje idéntico repetido.
RANK_MODE = os.getenv("RANK_MODE", "top_siempre").strip().lower()
RANK_ONLY_WHEN_CANDIDATES = RANK_MODE == "solo_candidatos"
# Mil símbolos son mil llamadas: el semáforo evita que BingX responda 429.
SCAN_CONCURRENCY = _int("SCAN_CONCURRENCY", 8)
RANGE_LEN = _int("RANGE_LEN", 20)
ER_SHORT = _int("ER_SHORT", 30)
ER_LONG = _int("ER_LONG", 180)
ER_TREND = _float("ER_TREND", 0.40)

# ── Liquidez ──────────────────────────────────────────────────────────
# Filtrar por amplitud sin filtrar por liquidez es cazar justo las
# monedas donde el libro es un colador. Volumen de 24h en USDT.
MIN_QUOTE_VOLUME_24H = _float("MIN_QUOTE_VOLUME_24H", 2_000_000.0)

# ── Filtro de eficiencia: descarta los verticales ─────────────────────
# El hallazgo que faltaba por implementar. Medido: la reversión gana en
# pumps que van y VUELVEN (JIMOTHY +0.22R, CATE PF 1.6) y pierde en los
# que solo van (INDEXUS -0.56R con 21 operaciones, y su ruptura -0.32R
# con 159: ahí no gana nadie). La diferencia se ve en el ratio de
# eficiencia: un movimiento en línea recta da ER alto; uno que sube y
# baja, ER bajo. Sin este filtro, el agregado de todo lo medido sale
# NEGATIVO porque el vertical se come lo que ganan los demás.
MAX_ER_LONG = _float("MAX_ER_LONG", 0.35)

# ── Ejecución ─────────────────────────────────────────────────────────
# LIMIT por defecto: la recomendación unánime para pares finos es no
# cruzar el spread con órdenes a mercado. Se paga con fills perdidos,
# que es mejor que pagar con precio.
ENTRY_TYPE = os.getenv("ENTRY_TYPE", "LIMIT").strip().upper()
LIMIT_OFFSET_PCT = _float("LIMIT_OFFSET_PCT", 0.05)
# Minutos que se le dan a una orden limitada para ejecutarse. Pasado ese
# plazo se cancela. Una orden colgada no es "una entrada que aún puede
# salir bien": es una entrada calculada para un contexto que ya no
# existe, esperando a ejecutarse en otro. Esta estrategia entra en
# agotamientos que duran minutos; una orden de hace dos horas no tiene
# nada que ver con la señal que la creó.
LIMIT_TTL_MIN = _int("LIMIT_TTL_MIN", 10)

# ── Estrategia (idéntica a reversion_5m.pine) ─────────────────────────
MA_LEN = _int("MA_LEN", 20)
ATR_LEN = _int("ATR_LEN", 14)
STRETCH_ATR = _float("STRETCH_ATR", 2.5)
MAX_BARS_STRETCH = _int("MAX_BARS_STRETCH", 6)
SL_ATR = _float("SL_ATR", 1.0)
MIN_RR = _float("MIN_RR", 1.0)
TP_MODE = os.getenv("TP_MODE", "MEAN").strip().upper()  # MEAN | FIXED_R
RR_FIXED = _float("RR_FIXED", 1.5)

# ── Tiempo máximo por operación ───────────────────────────────────────
# La reversión intradía vive en la ventana de minutos a una hora. A
# horizonte de un día varios estudios encuentran lo contrario: momentum
# tras retornos anormales. Si la vuelta no llega pronto, ya no estás en
# el fenómeno que querías operar — estás en el que va en tu contra.
# 12 velas de 5m = 60 minutos. Mismo valor que reversion_5m.pine.
MAX_TRADE_BARS = _int("MAX_TRADE_BARS", 12)
USE_TIME_EXIT = _bool("USE_TIME_EXIT", True)
# Corrección a partir de datos reales: en el histórico medido, varias de
# las MEJORES ganadoras duraron 75, 100 y 105 minutos. Cortarlas a los 60
# habría matado justo las que pagaban. El reloj solo se aplica a lo que
# NO va a favor: se corta lo muerto y se deja correr lo que funciona.
TIME_EXIT_ONLY_LOSING = _bool("TIME_EXIT_ONLY_LOSING", True)


def max_trade_seconds() -> int:
    minutos_por_vela = {"1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30, "1h": 60}
    return MAX_TRADE_BARS * minutos_por_vela.get(TIMEFRAME, 5) * 60


# ── Riesgo ────────────────────────────────────────────────────────────
RISK_PCT = _float("RISK_PCT", 0.5)
MAX_CONCURRENT = _int("MAX_CONCURRENT", 2)
LEVERAGE = _int("LEVERAGE", 3)

# ── Circuit breaker ───────────────────────────────────────────────────
MAX_CONSECUTIVE_LOSSES = _int("MAX_CONSECUTIVE_LOSSES", 3)
COOLDOWN_MINUTES = _int("COOLDOWN_MINUTES", 120)

# ── Sección cruzada (retorno de 24 h) ─────────────────────────────────
# Sistema RELATIVO: siempre hay un "peor 1%", así que opera todos los
# días — a diferencia de la reversión, cuyo filtro es absoluto y deja
# días enteros sin candidatos.
# Arranca en modo REGISTRO: apunta el ranking y lo evalúa al día
# siguiente con el coste descontado. No manda órdenes.
XSECTION_ENABLED = _bool("XSECTION_ENABLED", True)
XSECTION_HOUR_UTC = _int("XSECTION_HOUR_UTC", 0)
XSECTION_N = _int("XSECTION_N", 5)
# El paper atribuye el efecto a la iliquidez, y las monedas más líquidas
# muestran lo contrario (momentum). Este mínimo es más bajo que el de la
# otra estrategia a propósito: si se filtra igual, se corta justo donde
# el efecto es más fuerte. El coste dirá si compensa.
XSECTION_MIN_VOL = _float("XSECTION_MIN_VOL", 500_000.0)

# ── Avisos de vigilancia ──────────────────────────────────────────────
# Aviso ANTES de que dispare: cuando un símbolo pasa todos los filtros y
# está cerca del umbral de estiramiento, pero aún le falta la vela de
# agotamiento. Da tiempo a abrir el gráfico y pasarlo por el script
# antes de que la señal exista — que es justo lo que no pasó con AIINU,
# donde el aviso llegó cuando el precio ya había vuelto a su media.
WATCH_ALERTS = _bool("WATCH_ALERTS", True)
WATCH_STRETCH_PCT = _float("WATCH_STRETCH_PCT", 75.0)
# Sin esto, un símbolo que ronda el umbral avisaría cada minuto.
WATCH_COOLDOWN_MIN = _int("WATCH_COOLDOWN_MIN", 45)

# ── Avisos ────────────────────────────────────────────────────────────
DAILY_SUMMARY = _bool("DAILY_SUMMARY", True)
DAILY_SUMMARY_HOUR_UTC = _int("DAILY_SUMMARY_HOUR_UTC", 7)
HEARTBEAT_HOURS = _int("HEARTBEAT_HOURS", 12)
# Días sin operar en LIVE que disparan aviso. El fallo más traicionero
# es el silencioso: todo "funciona" y hace semanas que no se opera.
IDLE_ALERT_DAYS = _int("IDLE_ALERT_DAYS", 5)

# ── Estado ────────────────────────────────────────────────────────────
STATE_PATH = os.getenv("STATE_PATH", "/data/state.json")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").strip().upper()


def is_live() -> bool:
    """LIVE exige los DOS interruptores y credenciales de verdad."""
    return MODE == "LIVE" and LIVE_CONFIRMED and bool(BINGX_API_KEY) and bool(BINGX_API_SECRET)


def describe() -> str:
    if is_live():
        return "LIVE — enviando órdenes reales a BingX"
    if MODE == "LIVE":
        return "LIVE pedido pero SIN confirmar (falta LIVE_CONFIRMED o claves) — sigue en SIGNAL"
    return "SIGNAL — solo avisos, no toca el exchange"
