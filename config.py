"""
Configuración — Bot Supertrend + Unicorn Model (standalone)
=============================================================
Todo se lee de variables de entorno (Railway-friendly). Los defaults
son razonables para arrancar pero DEBEN revisarse antes de operar real.
"""
import os


def _f(name, default):
    return float(os.getenv(name, default))


def _i(name, default):
    return int(os.getenv(name, default))


def _b(name, default):
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


# ── Credenciales BingX ────────────────────────────────────────────────
# .strip() es a propósito: un espacio o salto de línea invisible al pegar
# la variable en Railway hace que la firma HMAC nunca coincida, con el
# mismo síntoma exacto que un secret genuinamente incorrecto ("Signature
# verification failed") — y es mucho más común de lo que parece.
BINGX_API_KEY = os.getenv("BINGX_API_KEY", "").strip()
BINGX_API_SECRET = os.getenv("BINGX_API_SECRET", "").strip()
BINGX_DEMO_MODE = _b("BINGX_DEMO_MODE", False)  # True = usa el dominio Demo/VST de BingX
                                                  # (open-api-vst.bingx.com) en vez del real.
                                                  # Si tu API key se generó estando en modo
                                                  # "Demo Trading" (VST) de BingX, es probable
                                                  # que NO sea válida contra el dominio real —
                                                  # mismo síntoma que un secret incorrecto
                                                  # (100001 Signature verification failed).
_DEFAULT_BASE_URL = "https://open-api-vst.bingx.com" if BINGX_DEMO_MODE else "https://open-api.bingx.com"
BINGX_BASE_URL = os.getenv("BINGX_BASE_URL", _DEFAULT_BASE_URL)
DRY_RUN = _b("DRY_RUN", True)  # True = solo loguea señales, no envía órdenes

# ── Scanner ────────────────────────────────────────────────────────────
SCAN_CONCURRENCY = _i("SCAN_CONCURRENCY", 5)        # bajado de 8: el rate limit 100410 seguía en
                                                     # loop porque cada reintento durante el bloqueo
                                                     # parece extenderlo — ver exchange_client.py.
                                                     # Menos concurrencia = ráfaga inicial más chica
                                                     # antes de que el cooldown compartido se conozca.
SCAN_INTERVAL_SEC = _i("SCAN_INTERVAL_SEC", 420)    # 45s (default viejo) era irreal con
                                                     # SCAN_ALL_SYMBOLS=True: ~694 símbolos x 5
                                                     # klines c/u ≈ 3470 requests/ciclo, y con el
                                                     # espaciado de exchange_client.py (~8 req/s)
                                                     # un ciclo completo tarda ~7 min de por sí
MIN_24H_VOLUME_USDT = _f("MIN_24H_VOLUME_USDT", 3_000_000)  # filtra símbolos ilíquidos (solo si SCAN_ALL_SYMBOLS=False)
SCAN_ALL_SYMBOLS = _b("SCAN_ALL_SYMBOLS", True)  # True = ignora MIN_24H_VOLUME_USDT, escanea TODO BingX (menos no-cripto)
NON_CRYPTO_PREFIXES = [  # instrumentos no-cripto que BingX a veces lista
    "XAU", "XAG", "US30", "US100", "US500", "GER40", "UK100", "JP225",
    "OIL", "WTI", "BRENT", "EURUSD", "GBPUSD", "USDJPY", "AUDUSD",
    "NZDUSD", "USDCAD", "USDCHF", "EURGBP", "EURJPY", "GBPJPY",
    # Acciones/ETFs tokenizados (BingX "TradFi" — NO son cripto, confirmado
    # tras ver NCSKMSFT2USD y NCSKQQQ2USD colarse con SCAN_ALL_SYMBOLS=True).
    # "NCSK" es el prefijo del proveedor institucional; el resto son tickers
    # directos o con sufijo de tokenización (ON=Ondo, X=xStocks) que BingX
    # también lista. Lista best-effort, no exhaustiva — BingX sigue agregando
    # acciones tokenizadas, revisar periódicamente igual que con forex/índices.
    # "NCS" y "NCC" cubren toda la familia de acciones/ETFs/índices tokenizados
    # de este proveedor institucional (ya vimos NCSK-MSFT, NCSI-NASDAQ, NCCO-GOLD,
    # NCSK-QQQ, NCSK-SPCX...) — prefijo de 3 letras en vez de perseguir cada
    # variante nueva una por una.
    # "NC" (2 letras) cubre TODA la familia de productos tokenizados de este
    # proveedor institucional — ya van 3 sub-familias confirmadas por
    # separado: NCS* (acciones/ETFs: MSFT, QQQ, SPCX), NCC* (commodities:
    # GOLD), NCFX* (pares forex: GBP2USD, USD2TRY). En vez de seguir
    # agregando cada variante nueva que aparece, se corta por la base.
    "NC", "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "NVDA", "TSLA",
    "PLTR", "HOOD", "MSTR", "CRCL", "QQQ", "SPY",
]
REQUIRE_USDT_QUOTE = _b("REQUIRE_USDT_QUOTE", True)  # excluye pares que no cotizan en USDT
                                                       # (ej. KAITO-USDC) — todo el risk_manager
                                                       # asume balance y PnL en USDT; mezclar
                                                       # otras monedas de cotización sin verificar
                                                       # el margen real es un riesgo aparte, no
                                                       # solo un tema de "no es cripto"

# ── Loop rápido (símbolos de mayor volumen, más frecuente) ───────────────
# Corre EN PARALELO al barrido completo (lento), sobre el mismo BingXClient
# -> comparte el pacing/cooldown de rate limit, no suma presión extra "gratis".
# Un asyncio.Lock protege la sección de apertura de posición para que los dos
# loops no abran más posiciones que MAX_ACTIVE_POSITIONS por una condición de
# carrera (ver main.py).
ENABLE_FAST_SCAN = _b("ENABLE_FAST_SCAN", True)
FAST_SCAN_TOP_N = _i("FAST_SCAN_TOP_N", 60)
FAST_SCAN_INTERVAL_SEC = _i("FAST_SCAN_INTERVAL_SEC", 60)
MAX_ACTIVE_POSITIONS = _i("MAX_ACTIVE_POSITIONS", 6)

# ── Timeframes ───────────────────────────────────────────────────────
ENTRY_TF = os.getenv("ENTRY_TF", "3m")       # Unicorn Model — timing de entrada
BIAS_TF = os.getenv("BIAS_TF", "1h")         # Supertrend — bias macro (BingX exige minúscula: 1h, no 1H)
HTF_A_TF = os.getenv("HTF_A_TF", "15m")      # fuente liquidez A del Unicorn Model
HTF_B_TF = os.getenv("HTF_B_TF", "30m")      # fuente liquidez B
HTF_C_TF = os.getenv("HTF_C_TF", "1h")       # fuente liquidez C (BingX exige minúscula: 1h, no 1H)
OB_TF = os.getenv("OB_TF", "15m")            # Order Block Engine (BigBeluga) — su propio timeframe

# ── Supertrend (BigBeluga custom) ─────────────────────────────────────
ST_LEN = _i("ST_LEN", 50)
ST_MULT = _f("ST_MULT", 3.5)

# ── Unicorn Model ──────────────────────────────────────────────────────
UNICORN_SWEEP_LB = _i("UNICORN_SWEEP_LB", 30)     # lookback de velas para sweep
UNICORN_REQUIRE_FVG = _b("UNICORN_REQUIRE_FVG", True)  # Unicorn Mode ON
UNICORN_RR = _f("UNICORN_RR", 2.0)                # risk:reward del TP (subido de 1.5: TP más largo pedido)
UNICORN_SL_ATR_BUFFER = _f("UNICORN_SL_ATR_BUFFER", 0.2)  # colchón extra en SL
DIRECTION = os.getenv("DIRECTION", "BOTH")        # LONG | SHORT | BOTH

# Filtro de tamaño de breaker (en múltiplos de ATR) — descarta breakers
# demasiado chicos (ruido) o demasiado grandes (movimiento ya agotado)
BREAKER_MIN_ATR = _f("BREAKER_MIN_ATR", 0.3)
BREAKER_MAX_ATR = _f("BREAKER_MAX_ATR", 3.0)

# ── Order Block Engine (BigBeluga) — segundo motor de entrada, en paralelo ─
# al Unicorn Model: si Unicorn no confirma, se intenta este. Usa el MISMO
# ST_LEN/ST_MULT de arriba para su propia tendencia interna (así ambos
# motores están de acuerdo en qué es "tendencia" si corrieran sobre las
# mismas velas). Solo entra si además coincide con el Supertrend de BIAS_TF.
ENABLE_OB_ENGINE = _b("ENABLE_OB_ENGINE", True)
OB_PIVOT_LEN = _i("OB_PIVOT_LEN", 7)              # barras a cada lado para confirmar un pivote
OB_MIN_BUY_PCT = _f("OB_MIN_BUY_PCT", 50.0)       # % mínimo de volumen comprador para aceptar retest LONG
OB_MIN_SELL_PCT = _f("OB_MIN_SELL_PCT", 50.0)     # % mínimo de volumen vendedor para aceptar retest SHORT
OB_DELETE_ON_BREAK = _b("OB_DELETE_ON_BREAK", True)  # invalida el Order Block si el precio lo rompe del todo
OB_RR = _f("OB_RR", 2.0)                          # risk:reward del TP (subido de 1.5, igual que UNICORN_RR)
OB_SL_ATR_BUFFER = _f("OB_SL_ATR_BUFFER", 0.2)    # colchón ATR extra en el SL

# ── CVD Filter (Cumulative Volume Delta, sin llamada extra a la API) ─────
# Inspirado en tu Confluence Gate de TradingView. Reusa candles_entry
# (ENTRY_TF) como timeframe fino para aproximar volumen intrabar — no pide
# velas 1m aparte. Off por defecto hasta validar en DRY_RUN, mismo criterio
# que Order Flow / Funding-OI.
ENABLE_CVD_FILTER = _b("ENABLE_CVD_FILTER", False)
CVD_LOOKBACK = _i("CVD_LOOKBACK", 20)              # velas finas hacia atrás (mismo default que cvd_len en Pine)

# ── Order Book Imbalance (OBI) — confirmación final, libro de órdenes en vivo ──
# Distinto de todo lo demás: no mira velas ni trades ejecutados, mira lo que
# está PARADO en el libro ahora mismo. Off por defecto hasta validar contra
# BingX real — mismo criterio que Order Flow/Funding-OI.
ENABLE_OBI_FILTER = _b("ENABLE_OBI_FILTER", False)
OBI_LEVELS = _i("OBI_LEVELS", 20)                  # niveles de profundidad a considerar
OBI_THRESHOLD = _f("OBI_THRESHOLD", 0.15)          # desequilibrio mínimo (-1 a 1) para confirmar

# ── Deduplicación de señales ──────────────────────────────────────────────
# El loop rápido y el lento pueden evaluar el mismo símbolo en ventanas
# superpuestas y encontrar la MISMA señal (mismas velas, mismo resultado).
# El chequeo de "ya hay posición abierta" no alcanza para evitar esto en
# DRY_RUN (las posiciones simuladas nunca aparecen en BingX real) — este
# cooldown lo cubre en los dos modos, independiente de si hay posición real.
DEDUP_COOLDOWN_SEC = _i("DEDUP_COOLDOWN_SEC", 300)
POST_CLOSE_COOLDOWN_SEC = _i("POST_CLOSE_COOLDOWN_SEC", 900)  # tras cerrar una posición,
                                                               # no reabrir el mismo símbolo
                                                               # por este tiempo (observado en
                                                               # real: UNI reabrió 1 min después
                                                               # de cerrar, 3 trades en el día)

# ── RSI Filter (momento actual, no congelado) ────────────────────────────
ENABLE_RSI_FILTER = _b("ENABLE_RSI_FILTER", False)
RSI_LENGTH = _i("RSI_LENGTH", 14)

# ── VWAP Filter (precio ponderado por volumen) ───────────────────────────
ENABLE_VWAP_FILTER = _b("ENABLE_VWAP_FILTER", False)

# ── Order Flow / Absorción (confirmación final, post Supertrend+Unicorn) ──
ENABLE_ORDER_FLOW_FILTER = _b("ENABLE_ORDER_FLOW_FILTER", False)  # off por defecto
ORDER_FLOW_TRADES_LIMIT = _i("ORDER_FLOW_TRADES_LIMIT", 1000)     # trades recientes a pedir
ORDER_FLOW_MIN_ABSORPTION_RATIO = _f("ORDER_FLOW_MIN_ABSORPTION_RATIO", 0.55)
ORDER_FLOW_MIN_VOLUME = _f("ORDER_FLOW_MIN_VOLUME", 0)  # volumen mínimo en la vela sweep (unidades base)

# ── Funding Rate + Open Interest ─────────────────────────────────────────
ENABLE_FUNDING_OI_FILTER = _b("ENABLE_FUNDING_OI_FILTER", False)
FUNDING_OI_MODE = os.getenv("FUNDING_OI_MODE", "inform")  # "inform" | "confirm"
FUNDING_EXTREME_THRESHOLD = _f("FUNDING_EXTREME_THRESHOLD", 0.0005)
OI_MIN_CHANGE_PCT = _f("OI_MIN_CHANGE_PCT", 1.0)

# ── Regime Filter (Choppiness Index) ────────────────────────────────────
ENABLE_REGIME_FILTER = _b("ENABLE_REGIME_FILTER", True)
REGIME_CHOP_LENGTH = _i("REGIME_CHOP_LENGTH", 14)
REGIME_CHOP_THRESHOLD = _f("REGIME_CHOP_THRESHOLD", 61.8)

# ── Correlation Manager ───────────────────────────────────────────────────
ENABLE_CORRELATION_FILTER = _b("ENABLE_CORRELATION_FILTER", True)
CORR_THRESHOLD = _f("CORR_THRESHOLD", 0.75)
CORR_LOOKBACK = _i("CORR_LOOKBACK", 30)
MAX_CORRELATED_POSITIONS = _i("MAX_CORRELATED_POSITIONS", 2)

# ── Persistencia (Railway Volume) ───────────────────────────────────────
DATA_DIR = os.getenv("DATA_DIR", "/data")

# ── Setup Memory (aprendizaje adaptativo) ────────────────────────────────
ENABLE_SETUP_MEMORY_FILTER = _b("ENABLE_SETUP_MEMORY_FILTER", True)
SETUP_MEMORY_MIN_SAMPLES = _i("SETUP_MEMORY_MIN_SAMPLES", 15)
SETUP_MEMORY_MIN_WIN_RATE = _f("SETUP_MEMORY_MIN_WIN_RATE", 0.35)
SETUP_MEMORY_FILE = os.path.join(DATA_DIR, "unicorn_st_setup_memory.json")

# ── Gestión de riesgo ───────────────────────────────────────────────────
RISK_PCT_PER_TRADE = _f("RISK_PCT_PER_TRADE", 0.5)   # % del balance por operación

# ── Notional mínimo por trade ────────────────────────────────────────────
# El sizing por riesgo produce notionals minúsculos cuando el SL queda lejos
# (visto en real: LDO-USDT con 4 USDT de notional porque el SL estaba ~11%
# abajo). Si el notional calculado queda por debajo de este mínimo, se INFLA
# la cantidad hasta alcanzarlo — PERO inflar un trade de SL lejano multiplica
# su riesgo real, así que hay un tope: si el riesgo efectivo tras inflar
# supera MIN_NOTIONAL_MAX_RISK_PCT, el trade se descarta (queda en el journal
# como min_notional_riesgo_excesivo). 0 = desactivado.
MIN_NOTIONAL_USDT = _f("MIN_NOTIONAL_USDT", 10.0)
MIN_NOTIONAL_MAX_RISK_PCT = _f("MIN_NOTIONAL_MAX_RISK_PCT", 1.5)
LEVERAGE = _i("LEVERAGE", 10)
DAILY_MAX_LOSS_PCT = _f("DAILY_MAX_LOSS_PCT", 5.0)   # circuit breaker diario
MAX_CONCURRENT_RISK_PCT = _f("MAX_CONCURRENT_RISK_PCT", 3.0)  # riesgo total abierto

# ── Persistencia adicional (mismo Railway Volume) ───────────────────────
JOURNAL_FILE = os.path.join(DATA_DIR, "unicorn_st_journal.json")
STATE_FILE = os.path.join(DATA_DIR, "unicorn_st_state.json")

# ── Logging ─────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
