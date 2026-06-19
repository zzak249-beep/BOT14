"""
QF×JP Bot v7.6 — Config RENEWED-LOVE (CAZADOR)
═══════════════════════════════════════════════════════════════════════════════
Bot MASTER — escanea todos los símbolos (683+), opera de forma autónoma.
NO copia de nadie. COMPLEMENT_MODE=DISABLED.
Los otros bots (joyful-art, zesty-reverence) leen su /status como fuente.

RENOMBRAR A config.py antes de subir al repo de renewed-love.
═══════════════════════════════════════════════════════════════════════════════
"""
import os
from dotenv import load_dotenv
load_dotenv()

def _bool(k, d): return os.getenv(k, str(d)).strip().lower() in ("true","1","yes")
def _float(k, d):
    try: return float(os.getenv(k, str(d)).strip().split()[0])
    except: return d
def _int(k, d):
    try: return int(os.getenv(k, str(d)).strip().split()[0])
    except: return d
def _list(k, d):
    r = os.getenv(k, d).strip()
    return [x.strip() for x in r.split(",") if x.strip()] if r else []

# ── BingX (claves PROPIAS de renewed-love, distintas de los otros bots) ───────
BINGX_API_KEY    = os.getenv("BINGX_API_KEY", "").strip()
BINGX_SECRET_KEY = os.getenv("BINGX_SECRET_KEY", "").strip()
BINGX_BASE_URL   = "https://open-api.bingx.com"

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# ── Modo ──────────────────────────────────────────────────────────────────────
MODE = os.getenv("MODE", "SIGNAL").upper()

# ── Capital y riesgo ──────────────────────────────────────────────────────────
# Actualizar CAPITAL con el saldo real de esta cuenta en Railway → Variables
CAPITAL          = _float("CAPITAL", 870.0)
RISK_PCT         = _float("RISK_PCT", 0.5)
LEVERAGE         = _int("LEVERAGE", 10)
MAX_OPEN_TRADES  = _int("MAX_OPEN_TRADES", 5)    # era 30 → demasiado, reducido a 5
MAX_DAILY_TRADES = _int("MAX_DAILY_TRADES", 20)

# ── Umbrales de señal ─────────────────────────────────────────────────────────
MIN_SCORE  = _float("MIN_SCORE",  58.0)
FUEL_SCORE = _float("FUEL_SCORE", 65.0)
SUP_SCORE  = _float("SUP_SCORE",  80.0)
MIN_TIER   = os.getenv("MIN_TIER", "FUEL").upper()

# ── Entrada ───────────────────────────────────────────────────────────────────
REQUIRE_TL_BREAK = _bool("REQUIRE_TL_BREAK", True)
HTF_MIN_ALIGNED  = _int("HTF_MIN_ALIGNED", 2)

# ── Scanner — CAZADOR: escanea TODOS los símbolos ─────────────────────────────
SCAN_INTERVAL   = _int("SCAN_INTERVAL", 60)
TOP_N_SYMBOLS   = _int("TOP_N_SYMBOLS", 0)     # 0 = sin límite (todos)
BLACKLIST       = set(_list("BLACKLIST", "ESPORTS,STABLE,EURUSD,SILVER,PAXG,CUSDT,SYN,FONK,FOLK,NCS"))
MIN_VOLUME_USDT = _float("MIN_VOLUME_USDT", 5_000_000.0)

# ── Complement Engine — DESACTIVADO en el bot master ─────────────────────────
# renewed-love ES el master. No copia de nadie.
# COMPLEMENT_MODE=DISABLED se pone en Railway → Variables
# MASTER_URL no necesario (este bot no lee a ningún otro)

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
CB_ENABLED  = _bool("CB_ENABLED",   False)
CB_ATR_MULT = _float("CB_ATR_MULT", 3.0)
CB_BARS     = _int("CB_BARS",       10)

# ── Gestión de posiciones ─────────────────────────────────────────────────────
POSITION_CHECK_INTERVAL = _int("POSITION_CHECK_INTERVAL", 30)

# ── Trailing Stop ─────────────────────────────────────────────────────────────
BREAKEVEN_ATR_MULT = _float("BREAKEVEN_ATR_MULT", 1.0)
TRAIL_DISTANCE_ATR = _float("TRAIL_DISTANCE_ATR", 1.5)

# ── Time Stop ─────────────────────────────────────────────────────────────────
MAX_HOLD_MINUTES           = _int("MAX_HOLD_MINUTES", 60)
TIME_STOP_MIN_PROGRESS_ATR = _float("TIME_STOP_MIN_PROGRESS_ATR", 0.5)

# ── Correlation Guard ─────────────────────────────────────────────────────────
CORRELATION_WINDOW_SEC = _int("CORRELATION_WINDOW_SEC", 900)
MAX_SAME_DIRECTION     = _int("MAX_SAME_DIRECTION", 2)

# ── Límite de pérdida diaria ──────────────────────────────────────────────────
DAILY_LOSS_PCT = _float("DAILY_LOSS_PCT", 2.0)

# ── Notional máximo por trade ─────────────────────────────────────────────────
MAX_NOTIONAL_USDT = _float("MAX_NOTIONAL_USDT", 200.0)


# ── Session filter ────────────────────────────────────────────────────────────
# Evita operar en horas de bajo volumen donde se concentran las pérdidas.
# Horas UTC: 0=medianoche, 8=08:00 UTC (10:00 Madrid verano)
# Para operar 24h poner TRADE_START_UTC=0 y TRADE_END_UTC=24
TRADE_START_UTC = _int("TRADE_START_UTC", 8)    # desde las 08:00 UTC
TRADE_END_UTC   = _int("TRADE_END_UTC",   20)   # hasta las 20:00 UTC

# ── Funding Rate extremo como señal contraria ─────────────────────────────────
# FR > FR_EXTREME_THR → longs sobrecomprados → bloquea LONG, boosta SHORT +8pts
# FR < -FR_EXTREME_THR → shorts sobrecomprados → bloquea SHORT, boosta LONG +8pts
# 0.0005 = 0.05% por cada 8h (umbral típico de sobrecompra en altcoins)
# Poner a 0 para desactivar el filtro
FR_EXTREME_THR = _float("FR_EXTREME_THR", 0.0005)

# ── Open Interest como filtro de confirmación ─────────────────────────────────
# Si OI decrece >5% entre lecturas → señal descartada (posiciones cerrándose)
# Si OI crece → boost de confirmación +3pts al score
# Requiere llamada extra a BingX por símbolo analizado — impacto en velocidad
OI_FILTER_ENABLED = _bool("OI_FILTER_ENABLED", True)

# ── Limit orders con fallback a market ───────────────────────────────────────
# Taker fee 0.05% → Maker fee 0.02% = ahorro del 60% en comisiones
# Si la orden no se llena en LIMIT_TIMEOUT_SECS → cancela y usa market
LIMIT_ORDERS_ENABLED = _bool("LIMIT_ORDERS_ENABLED", True)   # True = fee maker 0.02%
LIMIT_TIMEOUT_SECS   = _int("LIMIT_TIMEOUT_SECS", 25)

# ── Time Stop ─────────────────────────────────────────────────────────────────

# ── Correlation Guard ─────────────────────────────────────────────────────────


# ── Funding Regime Engine ─────────────────────────────────────────────────────
# Detecta régimen de funding con anticipación pre-pago: el edge profesional.
# CARRY/SQUEEZE/EXTREME/STRESS — cada régimen ajusta el score de señales.
# La ventana pre-funding (2h antes del pago) da máxima convicción contraria.
FR_REGIME_ENABLED   = _bool("FR_REGIME_ENABLED",   True)

# Funding Harvest: abre posición pequeña (25% notional) aprovechando que
# los apalancados cierran antes del pago → movimiento predecible.
# Condición: FR > HARVEST_FR_THR (0.10%/8h) en ventana pre-funding.
HARVEST_ENABLED     = _bool("HARVEST_ENABLED",     True)
HARVEST_FR_THR      = _float("HARVEST_FR_THR",     0.0010)  # 0.10%/8h mínimo


# ── Volatility Regime Engine ──────────────────────────────────────────────────
# Detecta el régimen de volatilidad de CADA símbolo contra su propia historia
# (percentil de ATR%, no umbral fijo). Ajusta sizing y SL/TP dinámicamente:
#   COMPRESSED (pctl<20%): +15% size, SL ajustado, TP más lejano (rupturas
#               tras compresión suelen ser explosivas)
#   EXPANDED   (pctl>70%): -30% size, SL más ancho (evita whipsaw)
#   EXTREME    (pctl>90%): -60% size o bloqueo — riesgo de cascada
VOL_REGIME_ENABLED = _bool("VOL_REGIME_ENABLED", True)


# ── Turn-of-Candle (timing boost conservador, solo LONG) ──────────────────────
# Shanaev et al. 2023 (Heliyon): retornos positivos concentrados en los
# minutos 0/15/30/45 de cada hora (giros de vela 15m), probado en BTC.
# ⚠️ Generalización a altcoins NO probada en literatura — por eso el boost
# es pequeño (+3 pts) y solo actúa como confluencia, nunca como trigger.
CANDLE_TURN_ENABLED        = _bool("CANDLE_TURN_ENABLED", False)  # ← desactivado: backtest propio (60 días BTC-USDT BingX) NO mostró efecto significativo (p=0.389, ni siquiera los minutos 0/15/30/45 del paper original)
CANDLE_TURN_BOOST          = _float("CANDLE_TURN_BOOST", 3.0)
CANDLE_TURN_TOLERANCE_MIN  = _int("CANDLE_TURN_TOLERANCE_MIN", 1)

# ── Slope Multi-Timeframe (confluencia de tendencia + anti-whipsaw) ──────────
# Pendiente de regresión lineal en 15m/1h/4h (klines ya descargados, sin
# coste de API extra). 3/3 timeframes alineados = +10pts ("respaldo
# institucional" cruzando horizontes). 2+/3 en contra con fuerza STRONG =
# bloquea la señal — firma clásica de entrar contra tendencia establecida.
SLOPE_FILTER_ENABLED       = _bool("SLOPE_FILTER_ENABLED", True)


# ── BTC Correlation Guard ──────────────────────────────────────────────────────
# Top-30 coins suelen correlacionar 0.6-0.95 con BTC — abrir varios símbolos
# "distintos" en la misma dirección puede ser la MISMA apuesta repetida sobre
# BTC. Caso real prevenido: SXT+LDO+FHE, 3 LONG correlacionados cerrados
# juntos con -23.47 USDT. 1 llamada extra a la API por iteración (BTC-USDT),
# reutilizada para todos los símbolos — sin coste adicional relevante.
BTC_CORR_ENABLED       = _bool("BTC_CORR_ENABLED", True)
BTC_CORR_THRESHOLD     = _float("BTC_CORR_THRESHOLD", 0.5)
BTC_CORR_MAX_SAME      = _int("BTC_CORR_MAX_SAME", 3)
BTC_CORR_WINDOW_SEC    = _int("BTC_CORR_WINDOW_SEC", 1800)


# ── WebSocket Market Data (OPCIONAL, desactivado por defecto) ────────────────
# Reduce latencia de hasta 60s (polling REST) a near-instant para el
# timeframe principal. Diseñado para NUNCA poder tumbar el bot: si falla
# o no está activado, cae automáticamente a REST sin cambio de comportamiento.
# Activar solo después de confirmar que los 3 bots están estables.
WS_ENABLED = _bool("WS_ENABLED", False)

# ── Puerto ────────────────────────────────────────────────────────────────────
PORT = _int("PORT", 8080)

# ── Indicadores v3.6 ─────────────────────────────────────────────────────────
CVD_ROLL_WINDOW = _int("CVD_ROLL_WINDOW", 60)
EQL_LEN         = _int("EQL_LEN", 20)
EQL_TOL         = _float("EQL_TOL", 0.15)
OBP2_DIST       = _float("OBP2_DIST", 1.5)
PRE_SCORE       = _float("PRE_SCORE", 45.0)
