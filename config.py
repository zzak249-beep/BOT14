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
RANK_ONLY_WHEN_CANDIDATES = _bool("RANK_ONLY_WHEN_CANDIDATES", True)
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

# ── Avisos ────────────────────────────────────────────────────────────
DAILY_SUMMARY = _bool("DAILY_SUMMARY", True)
DAILY_SUMMARY_HOUR_UTC = _int("DAILY_SUMMARY_HOUR_UTC", 7)
HEARTBEAT_HOURS = _int("HEARTBEAT_HOURS", 12)

# ── Cascadas de liquidación (confirmación, no sustituto) ───────────────
# Gratis: streams públicos de Binance y Bybit — BingX no publica esto,
# y Coinglass ya no tiene tier gratuito (29$/mes mínimo). Esto SOLO
# añade una línea de confirmación a la señal que strategy.evaluate() ya
# decidió disparar; no cambia el criterio de entrada.
LIQUIDATIONS_ENABLED = _bool("LIQUIDATIONS_ENABLED", True)
# Minutos de historial usados para calcular la actividad "normal" de
# cada símbolo — sin esto no hay con qué comparar si la ventana corta
# está o no muy por encima de lo normal.
LIQ_BASELINE_MIN = _int("LIQ_BASELINE_MIN", 30)
# Ventana en la que se mide si hay cascada AHORA MISMO.
LIQ_SHORT_WINDOW_SEC = _int("LIQ_SHORT_WINDOW_SEC", 90)
# Una liquidación suelta no es cascada — puede ser ruido de una cuenta.
LIQ_MIN_EVENTS = _int("LIQ_MIN_EVENTS", 3)
# Mismo umbral (3×) que usó el único backtest de esto que sobrevivió
# walk-forward con velocidad+volumen como filtro (SOL/ETH PF>2.5).
LIQ_MULTIPLIER = _float("LIQ_MULTIPLIER", 3.0)
# Piso absoluto en USD: sin esto, un símbolo casi sin actividad de
# liquidaciones da falsos "3×" sobre una base casi nula.
LIQ_MIN_USD = _float("LIQ_MIN_USD", 5_000.0)

# ── RSI de doble cruce (confirmación de entrada, 5m) ───────────────────
# Traducción del script "ProBorsa: RSI & SuperTrend" — cuenta cruces del
# RSI sobre su propia media mientras sigue en zona débil, y dispara en
# el 2º cruce (doble suelo/techo visto en el RSI). Aquí se hizo
# simétrico: el original solo detectaba el lado alcista.
RSI_CONFIRM_ENABLED = _bool("RSI_CONFIRM_ENABLED", True)
RSI_LENGTH = _int("RSI_LENGTH", 10)
RSI_SIGNAL_LENGTH = _int("RSI_SIGNAL_LENGTH", 10)
RSI_TRIGGER = _float("RSI_TRIGGER", 50.0)
RSI_TARGET_CROSSES = _int("RSI_TARGET_CROSSES", 2)
# Cuántas velas de 5m hacia atrás cuentan como "reciente" — el cruce no
# tiene por qué caer EXACTAMENTE en la misma vela que la de agotamiento.
RSI_CONFIRM_BARS = _int("RSI_CONFIRM_BARS", 3)
# Si True, una señal SIN confirmación del RSI no se envía ni se abre —
# es un filtro real, no solo informativo. Empieza en True porque es
# literalmente lo que se pidió: combinar el RSI con las entradas.
# Se puede aflojar a False para verlo solo como información mientras
# se mide si ayuda o solo recorta señales buenas.
RSI_REQUIRE = _bool("RSI_REQUIRE", True)

# ── Radar de 30m (sesgo de tendencia, filtra contra-tendencia) ─────────
# Un segundo escaneo del universo, en 30m, EXCLUSIVAMENTE para decidir
# si hay una tendencia de fondo clara. Si la hay, bloquea las señales
# de 5m que apuesten EN CONTRA de ella — el patrón que el propio
# histórico del proyecto señaló como el principal origen de pérdidas
# (largos a contra-tendencia en mercado bajista, ~43% de acierto).
RADAR30M_ENABLED = _bool("RADAR30M_ENABLED", True)
RADAR30M_TIMEFRAME = os.getenv("RADAR30M_TIMEFRAME", "30m").strip()
RADAR30M_INTERVAL_MIN = _int("RADAR30M_INTERVAL_MIN", 30)

# ── Puntuación de confianza de entrada (score.py) ──────────────────────
# Combina en un solo número lo que antes se mostraba disperso (RSI,
# cascada, margen sobre los mínimos de R:R/cobertura). Se usa para
# ordenar el universo por calidad antes de escanear (ver
# Bot._priority_order) y se guarda junto a cada operación cerrada, para
# poder comprobar con datos propios si predice algo — ver
# stats.buckets_por_score(). No sustituye ningún bloqueo existente: el
# filtro de contra-tendencia de 30m sigue siendo un bloqueo duro.
SCORE_ENABLED = _bool("SCORE_ENABLED", True)
# 0 = desactivado, no bloquea nada por score. Ejemplo: 55 exige señales
# de calidad media-alta (aprox. base + un par de confirmaciones).
SCORE_MIN = _float("SCORE_MIN", 0.0)

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
