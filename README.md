# QF×JP v3.4 Bot — BingX Perpetuals

Port completo del indicador QF Machine × JP Fusion v3.4 en Python.
Desplegado en Railway, opera en BingX Perpetuals en temporalidad 3m.

## Arquitectura

```
qfxjp_bot/
├── main.py                       # Orquestador + health server + loop
├── config.py                     # Variables de entorno
├── requirements.txt
├── railway.toml
├── Procfile
├── .env.example
├── bingx/
│   └── client.py                 # API BingX (HMAC-SHA256)
├── strategy/
│   ├── indicators.py             # TODOS los indicadores v3.4 portados
│   └── qfxjp_signal.py           # Score compuesto + señales STD/FUEL/SUP
├── trader/
│   ├── position_manager.py       # Abrir/cerrar con Partial TP → BE
│   └── risk_manager.py           # Daily loss, drawdown, circuit breaker
├── notifications/
│   └── telegram_notifier.py      # Mensajes ricos Telegram
└── market_mechanics/
    ├── session_manager.py        # Asia/London/NY + Judas Swing
    ├── funding_monitor.py        # Funding rate veto
    ├── oi_tracker.py             # Open Interest señales
    ├── liquidation_estimator.py  # Mapa de liquidaciones
    └── market_context.py         # Orquestador
```

## Indicadores portados del Pine Script v3.4

| Módulo  | Qué hace |
|---------|----------|
| L2      | Factores Momentum / Mean-Rev / Volume con pesos ADX dinámicos |
| L3      | Decay adaptativo (IC rolling correlation) |
| L4      | Dark Pool (volumen alto + rango estrecho) |
| L6      | Asimetría de momentum alcista/bajista |
| L7      | Ruptura de trendlines (pivotes) |
| L8      | Swing HL/LH (sell_exhausted / buy_exhausted) |
| L9      | Fair Value Gaps tracking múltiple |
| L10     | Order Blocks + Breaker Blocks |
| L11     | CVD Delta rolling + divergencias |
| L12     | Squeeze Momentum (BB vs KC) |
| L13     | CHoCH / BoS (cambio de estructura) |
| L14     | Liquidity Sweeps |
| L15     | Volume Profile (POC / VAH / VAL) |
| L16     | OI Delta sintético (conf long/short/squeeze) |
| L17     | LS Ratio sentiment contrarian |
| [REG]   | Pesos dinámicos por régimen (TEND/LATERAL/NEUTRAL) |
| [ENT]   | Entry Refinement 1m (wick rechazo) |
| [SLD]   | SL Dinámico ATR × estructura |
| [PTP]   | Partial TP 25% en TP0.5 → SL a breakeven |
| [CB]    | Circuit Breaker (vela gigante = news filter) |
| [KEL]   | Kelly Criterion sizing con walk-forward WR |
| [HTF4]  | 3 TFs alineados (15m + 1h) + semanal macro |

## Señales

- **STD** (score ≥ 55): señal estándar con exhaustion
- **FUEL** (score ≥ 68): STD + categoría fuel (TL break / SQ / FVG / OB / Sweep / CHoCH)
- **SUP** (score ≥ 80): FUEL + divergencias DP/CVD/RSI + sin OI squeeze

## Deploy en Railway

1. Push a GitHub
2. Conectar repo en Railway → New Project from GitHub
3. Añadir variables de entorno desde `.env.example`
4. Railway detecta `railway.toml` → deploy automático
5. Variables mínimas requeridas:
   - `BINGX_API_KEY`, `BINGX_SECRET_KEY`
   - `TELEGRAM_TOKEN`, `TELEGRAM_CHAT_ID`

## Test local

```bash
pip install -r requirements.txt
cp .env.example .env   # editar con tus claves
python main.py
```
