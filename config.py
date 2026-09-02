"""
config.py — Carga de configuración desde variables de entorno.

Todos los parámetros replican 1:1 los `input.*()` del script Pine
"Wavelet MRA Haar 5m — BingX". Los valores por defecto son los mismos
que trae el script original.

El parseo numérico es defensivo (strip + separación de comentarios en
línea) porque en Railway es fácil que una variable quede con espacios,
comillas o un `# comentario` pegado al valor y rompa `int()`/`float()`.
"""

import logging
import os

logger = logging.getLogger("wavelet_bot.config")


def _clean(raw: str) -> str:
    # Corta cualquier comentario tipo "10  # diez por ciento" y quita
    # espacios/comillas sueltas antes de intentar convertir el valor.
    value = raw.split("#", 1)[0].strip()
    return value.strip("'").strip('"')


def _get_str(key: str, default: str) -> str:
    raw = os.environ.get(key)
    if raw is None or raw.strip() == "":
        return default
    return _clean(raw)


def _get_bool(key: str, default: bool) -> bool:
    raw = os.environ.get(key)
    if raw is None or raw.strip() == "":
        return default
    return _clean(raw).lower() in ("1", "true", "yes", "on", "si", "sí")


def _get_int(key: str, default: int) -> int:
    raw = os.environ.get(key)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(float(_clean(raw)))
    except ValueError:
        logger.warning("No se pudo parsear %s=%r como int, uso default=%s", key, raw, default)
        return default


def _get_float(key: str, default: float) -> float:
    raw = os.environ.get(key)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(_clean(raw))
    except ValueError:
        logger.warning("No se pudo parsear %s=%r como float, uso default=%s", key, raw, default)
        return default


class Config:
    # ── Credenciales BingX ──────────────────────────────────────────
    # Nombres fijos y sin alias para evitar el mismatch
    # BINGX_SECRET_KEY / BINGX_API_SECRET que ya ha dado problemas antes.
    BINGX_API_KEY = _get_str("BINGX_API_KEY", "")
    BINGX_API_SECRET = _get_str("BINGX_API_SECRET", "")
    BINGX_BASE_URL = _get_str("BINGX_BASE_URL", "https://open-api.bingx.com")
    BINGX_RECV_WINDOW_MS = _get_int("BINGX_RECV_WINDOW_MS", 5000)

    # Si es True, todos los símbolos se piden como BASE-VST en vez de
    # BASE-USDT: BingX ofrece el mismo API con saldo de práctica (VST)
    # sobre exactamente la misma infraestructura, ideal para probar el
    # bot antes de arriesgar capital real. No cambia ninguna otra lógica.
    DEMO_MODE = _get_bool("DEMO_MODE", False)

    # ── Telegram ─────────────────────────────────────────────────────
    TELEGRAM_BOT_TOKEN = _get_str("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = _get_str("TELEGRAM_CHAT_ID", "")

    # ── Interruptor maestro ─────────────────────────────────────────
    # Si es False, el bot calcula señales, las manda por Telegram y las
    # loguea, pero NUNCA manda órdenes reales a BingX. Útil para correr
    # el bot en paralelo un tiempo antes de activarlo con dinero real.
    LIVE_TRADING = _get_bool("LIVE_TRADING", True)

    # ── Universo de símbolos ────────────────────────────────────────
    # "ALL" -> escanea todos los perpetuos USDT-M activos en BingX.
    # o lista separada por comas, ej: "BTC-USDT,ETH-USDT,SOL-USDT"
    SYMBOLS = _get_str("SYMBOLS", "ALL")
    TIMEFRAME = _get_str("TIMEFRAME", "5m")  # debe existir como intervalo válido de BingX

    POLL_INTERVAL_SECONDS = _get_int("POLL_INTERVAL_SECONDS", 30)
    SYMBOL_BATCH_SIZE = _get_int("SYMBOL_BATCH_SIZE", 5)  # símbolos procesados en paralelo por tanda
    SYMBOL_BATCH_DELAY_SECONDS = _get_float("SYMBOL_BATCH_DELAY_SECONDS", 1.0)

    # ── Parámetros Wavelet (idénticos al script Pine) ───────────────
    LOOKBACK_ENERGY = _get_int("LOOKBACK_ENERGY", 40)
    K_DOMINANCE = _get_float("K_DOMINANCE", 1.5)
    COOLDOWN_BARS = _get_int("COOLDOWN_BARS", 4)

    USE_VOL_FILTER = _get_bool("USE_VOL_FILTER", False)
    VOL_LEN = _get_int("VOL_LEN", 20)
    VOL_MULT = _get_float("VOL_MULT", 1.2)

    # ── Riesgo (idéntico al script Pine) ────────────────────────────
    QTY_PCT = _get_float("QTY_PCT", 10.0)  # % del equity en NOCIONAL por operación
    LEVERAGE = _get_int("LEVERAGE", 10)

    USE_ATR_SL = _get_bool("USE_ATR_SL", True)
    ATR_LENGTH = _get_int("ATR_LENGTH", 14)
    ATR_MULT_SL = _get_float("ATR_MULT_SL", 1.5)
    ATR_MULT_TP = _get_float("ATR_MULT_TP", 2.5)
    SL_PERCENT = _get_float("SL_PERCENT", 1.0) / 100
    TP_PERCENT = _get_float("TP_PERCENT", 2.0) / 100

    # ── Salvaguardas propias del bot (no están en el Pine original) ─
    # Límite de posiciones simultáneas abiertas POR ESTE BOT. Con 10%
    # de nocional por operación y varios símbolos pudiendo señalar a la
    # vez, esto evita sobreexponer la cuenta si escaneas "ALL".
    MAX_CONCURRENT_POSITIONS = _get_int("MAX_CONCURRENT_POSITIONS", 5)

    # Si ya existe una posición abierta en ese símbolo (de este bot o de
    # cualquier otro proceso en la misma cuenta BingX), no se vuelve a
    # entrar. Coordina de forma segura con tus otros bots en la misma cuenta.
    SKIP_IF_SYMBOL_HAS_POSITION = _get_bool("SKIP_IF_SYMBOL_HAS_POSITION", True)

    MIN_BALANCE_USDT = _get_float("MIN_BALANCE_USDT", 0.0)  # 0 = sin mínimo

    # ── Servidor de salud (para Railway healthcheck / monitoreo) ────
    HEALTH_PORT = _get_int("PORT", _get_int("HEALTH_PORT", 8080))

    LOG_LEVEL = _get_str("LOG_LEVEL", "INFO")

    @classmethod
    def validate(cls):
        """Falla rápido y con mensaje claro en vez de un 100001 críptico."""
        missing = []
        if not cls.BINGX_API_KEY:
            missing.append("BINGX_API_KEY")
        if not cls.BINGX_API_SECRET:
            missing.append("BINGX_API_SECRET")
        if not cls.TELEGRAM_BOT_TOKEN:
            missing.append("TELEGRAM_BOT_TOKEN")
        if not cls.TELEGRAM_CHAT_ID:
            missing.append("TELEGRAM_CHAT_ID")
        if missing:
            raise RuntimeError(
                "Faltan variables de entorno obligatorias: " + ", ".join(missing)
            )

    @classmethod
    def summary(cls) -> str:
        modo = "DEMO (VST)" if cls.DEMO_MODE else "REAL"
        trading = "ACTIVO (envía órdenes reales)" if cls.LIVE_TRADING else "DESACTIVADO (solo señales)"
        return (
            f"Wavelet MRA Haar 5m — BingX\n"
            f"Modo cuenta: {modo} | Trading: {trading}\n"
            f"Símbolos: {cls.SYMBOLS} | Timeframe: {cls.TIMEFRAME}\n"
            f"qty_pct={cls.QTY_PCT}% | leverage={cls.LEVERAGE}x | "
            f"max_posiciones_simultaneas={cls.MAX_CONCURRENT_POSITIONS}\n"
            f"k_dominance={cls.K_DOMINANCE} | lookback_energy={cls.LOOKBACK_ENERGY} | "
            f"cooldown_bars={cls.COOLDOWN_BARS}\n"
            f"SL/TP: {'ATR x' + str(cls.ATR_MULT_SL) + '/' + str(cls.ATR_MULT_TP) if cls.USE_ATR_SL else str(cls.SL_PERCENT*100) + '%/' + str(cls.TP_PERCENT*100) + '%'}"
        )
