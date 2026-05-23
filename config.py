"""
Configuración Central — QF Machine × JP Fusion Bot v3
Todos los parámetros del indicador y del bot en un solo lugar.
"""

# ── Temporalidades ────────────────────────────────────────────
HTF = "15m"   # Higher timeframe para confirmación de régimen

# ── Símbolos a operar ─────────────────────────────────────────
# Añade o quita según tu capital. Más símbolos = más oportunidades
# pero más margen utilizado. Empieza con 1-2 en paper.
SYMBOLS = [
    "BTC-USDT",
    "ETH-USDT",
]

# ── Señal: parámetros del indicador (espejo del Pine Script) ──
SIGNAL_CFG = {
    # L1 / General
    "smo":      3,      # Suavizado de señal
    "atr_len":  10,     # Longitud ATR

    # L2 Factores
    "mom":      20,     # Lookback momentum
    "rev":      8,      # Lookback media-reversión
    "vol_len":  14,     # Longitud volumen OBV
    "w1":       0.40,   # Peso momentum
    "w2":       0.30,   # Peso media-rev
    "w3":       0.30,   # Peso volumen

    # L3 Decaimiento
    "dlen":     40,     # Ventana decaimiento
    "dthr":     0.50,   # Umbral vida media

    # L4 Dark Pool
    "dpm":      2.5,    # Multiplicador volumen spike
    "dpb":      20,     # Baseline dark pool (barras)
    "spl":      5,      # Longitud spread

    # L5 Ejecución
    "exl":      12,     # Baseline ejecución
    "bpt":      0.18,   # Umbral drenaje BP (%)

    # L6 Asimetría
    "asl":      10,     # Ventana asimetría
    "arr":      1.40,   # Ratio alcista/bajista
    "abr":      1.40,   # Ratio bajista/alcista

    # L7 Trendline
    "tlb":      30,     # Lookback TL pivotes
    "tll":      5,      # Pivote barras izq
    "tlr":      3,      # Pivote barras der
    "tlm":      0.15,   # Buffer ruptura (ATR)

    # L8 Swing Analysis
    "pll":      5,      # Swing low izq
    "plr":      3,      # Swing low der
    "phl":      5,      # Swing high izq
    "phr":      3,      # Swing high der
    "hlc":      2,      # Min HL ascendentes
    "hhc":      2,      # Min LH descendentes
    "hlw":      40,     # Ventana análisis

    # L9 FVG
    "fvg_min":  0.3,    # Tamaño mínimo (× ATR)
    "fvg_bars": 40,     # Validez FVG (barras)

    # L10 Order Blocks
    "ob_imp":   1.5,    # Impulso mínimo (× ATR)
    "ob_bars":  50,     # Validez OB (barras)

    # L11 CVD Delta
    "cvd_len":  20,     # EMA CVD
    "cvd_div":  5,      # Ventana divergencia

    # L12 Squeeze
    "sq_len":   20,     # Longitud squeeze
    "sq_bbm":   2.0,    # Multiplicador BB
    "sq_kcm":   1.5,    # Multiplicador KC
}

# ── Riesgo ────────────────────────────────────────────────────
RISK_CFG = {
    # Capital inicial (sincronizar con equity real de BingX)
    "initial_equity": 500.0,   # USDT

    # Leverage (BingX soporta hasta 100x; usa poco mientras validas)
    "leverage": 5,

    # Riesgo por operación (% del equity) según tier
    "risk_pct_suprema": 1.5,   # Señal más fuerte
    "risk_pct_fuel":    1.0,
    "risk_pct_std":     0.5,
    "risk_pct_max":     2.0,   # Techo absoluto, nunca superado

    # Ratio Reward:Risk según tier
    "rr_suprema": 3.0,
    "rr_fuel":    2.5,
    "rr_std":     2.0,

    # Circuit Breakers
    "max_daily_loss_pct":      3.0,   # Pérdida máxima diaria (% equity)
    "max_drawdown_pct":       15.0,   # DD máximo desde pico (para siempre)
    "max_consecutive_losses":   4,    # Pérdidas seguidas antes de pausa
    "max_daily_trades":        10,    # Máx operaciones por día
}
