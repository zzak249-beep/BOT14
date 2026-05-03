# -*- coding: utf-8 -*-
"""config.py -- Phantom Edge Bot v6: ZigZag + HMA + FutureTrend."""
import os
from dataclasses import dataclass


@dataclass
class Config:
    bingx_api_key:    str   = ""
    bingx_secret_key: str   = ""
    telegram_token:   str   = ""
    telegram_chat_id: str   = ""

    trade_usdt:  float = 9.0
    leverage:    int   = 7      # FIX: 10→7 — reduce exposición en 5m

    # ── Estrategia v6 ─────────────────────────────────────────
    pivot_len:   int   = 5      # Pine: ta.pivothigh(high, 5, 5)
    hma_len:     int   = 50     # Pine: ta.hma(close, 50)
    ft_period:   int   = 25     # FutureTrend period
    atr_period:  int   = 14
    atr_mult:    float = 2.0    # FIX: 1.5→2.0 — más espacio en 5m, evita SL hunting
    rr:          float = 2.5    # TP = SL × 2.5

    # ── Filtros ───────────────────────────────────────────────
    min_atr_pct:  float = 0.15  # FIX: 0.10→0.15 — evitar pares planos en 5m
    min_vol_mult: float = 0.6
    min_score:    int   = 5     # FIX: 3→5 — exige confirmación 15m o volumen

    # ── Timeframes ────────────────────────────────────────────
    timeframe:      str = "5m"
    timeframe_slow: str = "15m"
    timeframe_1h:   str = "1h"   # compat, no usado

    # ── Gestión posiciones ────────────────────────────────────
    max_positions:   int   = 5
    breakeven_r:     float = 1.0
    partial_pct:     float = 0.35   # 35% cierre en TP1 (como Pine v6)
    max_trade_hours: float = 8.0
    min_r_time_exit: float = 0.5

    # ── Riesgo diario ─────────────────────────────────────────
    max_daily_trades:   int   = 40
    max_daily_loss_pct: float = 5.0
    min_balance_usdt:   float = 9.0

    # ── Scanning ──────────────────────────────────────────────
    symbols_raw:    str = "ALL"
    scan_interval:  int = 30    # 30s = más reactivo en 5m
    max_concurrent: int = 30

    # ── Compat (ignorados en v6 strategy) ────────────────────
    zz_deviation:   float = 0.5
    zz15_deviation: float = 0.8
    st_period:      int   = 10
    st_mult:        float = 3.0
    rsi_period:     int   = 14
    adx_period:     int   = 14
    adx_min:        float = 0.0

    http_timeout: int = 12
    health_port:  int = 8080

    @property
    def symbols(self) -> list[str]:
        if self.symbols_raw.strip().upper() == "ALL":
            return []
        return [s.strip() for s in self.symbols_raw.split(",") if s.strip()]

    def __post_init__(self) -> None:
        e = lambda k, d: os.getenv(k, d)
        self.bingx_api_key      = e("BINGX_API_KEY",      self.bingx_api_key)
        self.bingx_secret_key   = e("BINGX_SECRET_KEY",   self.bingx_secret_key)
        self.telegram_token     = e("TELEGRAM_TOKEN",     self.telegram_token)
        self.telegram_chat_id   = e("TELEGRAM_CHAT_ID",   self.telegram_chat_id)
        self.trade_usdt         = max(9.0, float(e("TRADE_USDT",       str(self.trade_usdt))))
        self.leverage           = int(e("LEVERAGE",            str(self.leverage)))
        self.pivot_len          = int(e("PIVOT_LEN",           str(self.pivot_len)))
        self.hma_len            = int(e("HMA_LEN",             str(self.hma_len)))
        self.ft_period          = int(e("FT_PERIOD",           str(self.ft_period)))
        self.atr_period         = int(e("ATR_PERIOD",          str(self.atr_period)))
        self.atr_mult           = float(e("ATR_MULT",          str(self.atr_mult)))
        self.rr                 = float(e("RR",                str(self.rr)))
        self.min_atr_pct        = float(e("MIN_ATR_PCT",       str(self.min_atr_pct)))
        self.min_vol_mult       = float(e("MIN_VOL_MULT",      str(self.min_vol_mult)))
        self.min_score          = int(e("MIN_SCORE",           str(self.min_score)))
        self.timeframe          = e("TIMEFRAME",               self.timeframe)
        self.timeframe_slow     = e("TIMEFRAME_SLOW",          self.timeframe_slow)
        self.max_positions      = int(e("MAX_POSITIONS",       str(self.max_positions)))
        self.scan_interval      = int(e("SCAN_INTERVAL",       str(self.scan_interval)))
        self.max_concurrent     = int(e("MAX_CONCURRENT",      str(self.max_concurrent)))
        self.symbols_raw        = e("SYMBOLS",                 self.symbols_raw)
        self.health_port        = int(e("PORT",                str(self.health_port)))
        self.max_daily_loss_pct = float(e("MAX_DAILY_LOSS",    str(self.max_daily_loss_pct)))
        self.max_daily_trades   = int(e("MAX_DAILY_TRADES",    str(self.max_daily_trades)))
        self.max_trade_hours    = float(e("MAX_TRADE_HOURS",   str(self.max_trade_hours)))

        if not self.bingx_api_key or not self.bingx_secret_key:
            import sys
            print("FATAL: BINGX_API_KEY / BINGX_SECRET_KEY no configurados", flush=True)
            sys.exit(1)


cfg = Config()
