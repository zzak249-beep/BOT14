"""
Configuración central — QF×JP Bot v3.0
Todos los parámetros se leen de variables de entorno.
"""
import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    # ── API Keys ────────────────────────────────────────────
    BINGX_API_KEY : str = os.getenv("BINGX_API_KEY", "")
    BINGX_SECRET  : str = os.getenv("BINGX_SECRET", "")
    TG_TOKEN      : str = os.getenv("TG_TOKEN", "")
    TG_CHAT_ID    : str = os.getenv("TG_CHAT_ID", "")

    # ── Símbolos a operar ──────────────────────────────────
    # Formato BingX: "BTC-USDT", "ETH-USDT", "SOL-USDT"
    SYMBOLS: list[str] = field(default_factory=lambda: [
        s.strip() for s in os.getenv("SYMBOLS", "BTC-USDT,ETH-USDT").split(",")
    ])

    # ── Modo de operación ──────────────────────────────────
    # LIVE   = opera con dinero real
    # SIGNAL = solo envía señales por Telegram, no ejecuta
    MODE: str = os.getenv("MODE", "SIGNAL")

    # ── Riesgo ────────────────────────────────────────────
    LEVERAGE          : int   = int(os.getenv("LEVERAGE", "10"))
    RISK_PER_TRADE_PCT: float = float(os.getenv("RISK_PCT", "1.0"))  # % del balance
    MAX_DAILY_DD_PCT  : float = float(os.getenv("MAX_DD_PCT", "5.0"))
    TP_RR             : float = float(os.getenv("TP_RR", "2.0"))     # R:R para TP

    # ── Filtros de señal ──────────────────────────────────
    MIN_CONV_STD  : int = int(os.getenv("MIN_CONV_STD",  "5"))
    MIN_CONV_FUEL : int = int(os.getenv("MIN_CONV_FUEL", "7"))
    MIN_CONV_SUP  : int = int(os.getenv("MIN_CONV_SUP",  "8"))

    # ── Sesiones permitidas ───────────────────────────────
    # Vacío = todas. Opciones: "NY", "LDN", "ASIA"
    ALLOWED_SESSIONS: list[str] = field(default_factory=lambda: [
        s.strip() for s in os.getenv("SESSIONS", "NY,LDN").split(",") if s.strip()
    ])

    # ── Loop ──────────────────────────────────────────────
    LOOP_INTERVAL: int = int(os.getenv("LOOP_INTERVAL", "30"))  # segundos

    # ── L2 Factores ───────────────────────────────────────
    MOM_LEN : int   = 20
    REV_LEN : int   = 8
    VOL_LEN : int   = 14
    ATR_LEN : int   = 10
    W_MOM   : float = 0.40
    W_REV   : float = 0.30
    W_VOL   : float = 0.30
    SMO_LEN : int   = 3

    # ── L3 Decaimiento ────────────────────────────────────
    DECAY_LEN: int   = 40
    DECAY_THR: float = 0.50

    # ── L4 Dark Pool ──────────────────────────────────────
    DP_MULT  : float = 2.5
    DP_BASE  : int   = 20
    SPL_LEN  : int   = 5

    # ── L5 Ejecución ──────────────────────────────────────
    BP_THR   : float = 0.18

    # ── L6 Asimetría ──────────────────────────────────────
    ASY_LEN  : int   = 10
    ARR      : float = 1.40
    ABR      : float = 1.40

    # ── L7 Trendline ──────────────────────────────────────
    TL_LOOKBACK: int   = 30
    TL_LEFT    : int   = 5
    TL_RIGHT   : int   = 3
    TL_BUF     : float = 0.15

    # ── L8 Swing ──────────────────────────────────────────
    PL_LEFT  : int = 5
    PL_RIGHT : int = 3
    PH_LEFT  : int = 5
    PH_RIGHT : int = 3
    HL_COUNT : int = 2
    HH_COUNT : int = 2
    HL_WINDOW: int = 40

    # ── L9 FVG ────────────────────────────────────────────
    FVG_MIN  : float = 0.3
    FVG_BARS : int   = 40
    FVG_MITI : bool  = True

    # ── L10 Order Blocks ──────────────────────────────────
    OB_IMP   : float = 1.5
    OB_BARS  : int   = 50

    # ── L11 CVD ───────────────────────────────────────────
    CVD_LEN  : int = 20
    CVD_DIV  : int = 5

    # ── L12 Squeeze ───────────────────────────────────────
    SQ_LEN   : int   = 20
    SQ_BBM   : float = 2.0
    SQ_KCM   : float = 1.5


cfg = Config()
