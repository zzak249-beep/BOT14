# GUA-USDT Bot v2 — SMC Edition 📉🤖

Bot de trading automático para **GUA-USDT perpetuos en BingX**.
Estrategia multi-técnica: SMC/ICT + TTM Squeeze + RVOL + VWAP + OI Delta.

---

## Técnicas implementadas

| Categoría | Técnicas |
|---|---|
| SMC / ICT | FVG · Order Blocks · Liquidity Sweeps · BOS / CHoCH |
| Momentum | TTM Squeeze · MACD histogram |
| Volumen | CVD divergencia · RVOL (Relative Volume) |
| Precio | VWAP con bandas de desviación |
| Derivados | Funding Rate extremo · OI Delta |
| Régimen | ATR Percentil (ajusta SL y tamaño automáticamente) |
| Multi-TF | 3m entrada · 15m sesgo · 1h estructura macro |
| Sesiones | London (7–12 UTC) · NY (13–18 UTC) |
| Order Flow | Order Book Imbalance (filtro final pre-entrada) |

---

## Archivos

```
├── main.py              # Orquestador · APScheduler · 3 TFs en paralelo
├── config.py            # Todas las variables de entorno con defaults
├── exchange.py          # Cliente BingX · HMAC-SHA256 · One-Way mode
├── indicators.py        # EMA · RSI · ATR · ADX · CVD · Squeeze · RVOL
│                        # VWAP · FVG · Order Blocks · Liq Sweeps · BOS/CHoCH
├── strategy.py          # Motor de señales con scoring SHORT + LONG
├── position_manager.py  # TP1 parcial → BE → Trailing → TP2 / SL
├── notifier.py          # Telegram con contexto SMC por señal
├── health.py            # Health server /health para Railway
├── requirements.txt
├── Procfile
├── railway.toml
└── .env.example
```

---

## Lógica de señal SHORT (bias principal en GUA)

```
[OBLIGATORIO] EMA9 < EMA21

+ EMA21 < EMA50           +0.07
+ Precio bajo EMA200      +0.05
+ RSI 53–63               +0.12
+ Liquidity Sweep highs   +0.14  ← señal ICT más potente
+ En FVG bajista          +0.10
+ En Order Block bajista  +0.08
+ BOS bajista             +0.07
+ Squeeze libera bajista  +0.10
+ CVD divergencia bajista +0.08
+ Precio sobre VWAP sup   +0.06
+ RVOL > 1.3x             +0.05
+ Funding extremo +        +0.05
+ OI expandiéndose        +0.04
+ Bias 15m bajista        +0.08
─────────────────────────────────
Score mínimo requerido: 0.58
```

## Lógica de señal LONG (conservador, counter-trend)

```
[OBLIGATORIO] RSI < 37

+ Liquidity Sweep lows    +0.16  ← imprescindible para LONG en GUA
+ En FVG alcista          +0.10
+ Squeeze libera alcista  +0.10
+ CHoCH alcista           +0.08
+ CVD divergencia alcista +0.08
+ Funding extremo -        +0.07
+ Precio bajo VWAP inf    +0.06

- Bias 15m bajista        -0.12  ← GUA ahora bajista → LONGs raros
- Macro 1h bajista        -0.10
─────────────────────────────────
Score mínimo requerido: 0.58
```

---

## Gestión de posición

```
Apertura
  │
  ├─ SL = entry ± ATR × 1.5  (× 2.0 si ATR percentil ≥ 75)
  │
  ├─ TP1 (50%) = ATR × 2.0
  │     └─ SL → Breakeven
  │     └─ Trailing SL activado (precio ± ATR × 1.0)
  │
  └─ TP2 (50%) = ATR × 4.0
        o cierre por Trailing SL si el precio retrocede
```

**Ratio R:R:** mínimo 1:1.33 (TP1) · hasta 1:2.67 (TP2 completo)

### Tamaño dinámico

```python
vol_factor = 0.8  # si ATR pct >= 75 (alta volatilidad → -20% tamaño)
risk_usd   = balance × RISK_PCT × LEVERAGE × vol_factor
qty        = risk_usd / (ATR × SL_MULT)
```

---

## Filtros de entrada (cascada)

1. **Sesión** — solo London 7–12 UTC y NY 13–18 UTC
2. **Cooldown** — 15 min tras cerrar posición
3. **Order Book Imbalance** — si el libro contradice la señal (> ±0.3) cancela
4. **ATR percentil bajo** — si volatilidad demasiado baja no entra
5. **1 trade máximo** — sin posiciones simultáneas

---

## Deploy en Railway

```bash
# 1. Subir archivos a GitHub (raíz plana, sin subcarpetas)
# 2. Conectar repo en Railway
# 3. Variables de entorno (copiar .env.example)
# 4. Railway detecta Procfile → python main.py
```

### Variables obligatorias

```env
BINGX_API_KEY=...
BINGX_SECRET=...
TELEGRAM_TOKEN=...
TELEGRAM_CHAT_ID=...
MODE=SIGNAL
```

### Flujo recomendado

| Fase | Duración | Acción |
|---|---|---|
| Paper trading | 1–2 semanas | `MODE=SIGNAL`, anotar señales |
| Validación | 20+ señales | Win rate ≥ 50% con R:R > 1.5 |
| Live conservador | 2 semanas | `MODE=LIVE`, `RISK_PCT=0.01` |
| Optimización | Continuo | Ajustar `SCORE_THR`, `ATR_*` |

---

## Configuración recomendada para GUA

```env
LEVERAGE=5              # No más — GUA muy volátil
RISK_PCT=0.02           # 2% por trade
SCORE_THR=0.58          # Más permisivo por más condiciones disponibles
ATR_SL_MULT=1.5         # SL estándar
ATR_HIGHVOL_MULT=2.0    # SL ampliado en días explosivos
RVOL_MIN=1.3            # Exige volumen mínimo
SESSION_FILTER=true     # Solo London + NY
COOLDOWN_MIN=15         # Anti-overtrading
```

---

## Health endpoints

```
GET /         →  "GUA-USDT Bot v2 running ✅"
GET /health   →  JSON {status, uptime, ticks, signals, mode, symbol}
```

---

## Notas técnicas

- **Firma BingX**: `HMAC-SHA256(urlencode(sorted(params.items())))`
- **One-Way mode**: `positionSide` NO se envía en ninguna orden
- **Vela [-2]**: siempre se usa la penúltima para evitar vela incompleta
- **Fetch paralelo**: `asyncio.gather()` para los 6 endpoints simultáneos
- **APScheduler**: `cron minute=*/3, second=5` — 5s post-cierre de vela

---

## ⚠️ Riesgos específicos de GUA

- **Unlocks masivos pendientes**: solo 5.5% del supply circula → riesgo de dump por tokenomics
- **Volumen bajo (~$4M/día)**: slippage real, no usar qty > 5–10% del volumen diario
- **Narrativa especulativa**: precio sensible a FUD/noticias sin fundamento técnico
- **No operar con funding < −0.05%** (shorts en peligro de squeeze)

---

*Bot para BingX perpetuos · DYOR · No es asesoramiento financiero*
