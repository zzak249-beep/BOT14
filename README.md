# QF Machine × JP Fusion Bot v3 🤖

Bot de trading algorítmico para criptomonedas. Porta la lógica del indicador Pine Script (12 capas) a Python para operar en BingX con señales en Telegram.

> ⚠️ **PAPER MODE activo por defecto.** El bot NO opera con dinero real hasta que cambies `PAPER_MODE=false` explícitamente.

---

## 🏗 Arquitectura

```
src/
├── main.py          ← Orquestador principal (loop de trading)
├── signals.py       ← Motor de señales (12 capas, port del Pine Script)
├── exchange.py      ← Conector BingX REST API
├── risk.py          ← Gestión de riesgo y circuit breakers
├── positions.py     ← Tracker de posiciones abiertas
├── telegram_bot.py  ← Bot de Telegram (señales + comandos)
├── config.py        ← Todos los parámetros configurables
└── backtest.py      ← Backtester sobre datos históricos CSV
```

---

## ⚡ Setup rápido (Railway)

### 1. Fork / sube a GitHub

```bash
git init
git add .
git commit -m "QF Bot v3 initial"
git remote add origin https://github.com/TU_USUARIO/qf-bot.git
git push -u origin main
```

### 2. Crea proyecto en Railway

1. railway.app → New Project → Deploy from GitHub
2. Selecciona tu repo
3. Railway detecta el `Dockerfile` automáticamente

### 3. Variables de entorno en Railway

En tu proyecto → **Variables** → añade:

| Variable | Valor |
|---|---|
| `BINGX_API_KEY` | Tu API key de BingX |
| `BINGX_SECRET` | Tu secret de BingX |
| `TELEGRAM_TOKEN` | Token de @BotFather |
| `TELEGRAM_CHAT_ID` | Tu Chat ID (ver abajo) |
| `PAPER_MODE` | `true` (¡no cambies hasta validar!) |
| `MIN_CONVICTION` | `6` |
| `LOOP_SECONDS` | `30` |
| `TRAIL_ATR` | `1.5` |

### 4. Obtener API Keys de BingX

1. BingX → Cuenta → Gestión de API
2. Crear API → habilitar **Futuros Perpetuos**
3. Habilitar: Lectura ✅ | Trading ✅ | Retiro ❌ (NUNCA)
4. IP whitelist: añade la IP de tu servidor Railway (opcional pero recomendado)

### 5. Obtener Telegram Token y Chat ID

```bash
# 1. Habla con @BotFather → /newbot → sigue instrucciones → guarda el token

# 2. Obtén tu Chat ID:
curl "https://api.telegram.org/bot<TOKEN>/getUpdates"
# Manda un mensaje a tu bot y busca "chat":{"id": XXXX}
```

---

## 🧪 Backtest antes de operar

```bash
# Instalar dependencias
pip install -r requirements.txt

# Descargar datos históricos de BingX (CSV)
# o exportar desde TradingView: Símbolo → Exportar datos CSV

# Ejecutar backtest
python src/backtest.py \
  --file3m  datos/BTCUSDT_3m.csv \
  --conviction 6 \
  --out logs/bt_result.json

# Resultado:
# ══════════════════════════════════════════
#   total_trades     : 87
#   win_rate_pct     : 58.6
#   total_pnl        : +234.50
#   max_drawdown_pct : 8.2
#   profit_factor    : 1.74
# ══════════════════════════════════════════
```

**Criterios mínimos para pasar a paper trading:**
- Win rate > 50%
- Profit factor > 1.3
- Max drawdown < 15%

**Criterios mínimos para pasar a live:**
- Paper trading rentable durante ≥ 3 semanas
- Al menos 30 operaciones en paper
- Win rate estable > 52%

---

## 📱 Comandos Telegram

| Comando | Función |
|---|---|
| `/start` | Panel principal con botones |
| `/status` | Equity, PnL, drawdown, circuit breaker |
| `/pause` | Detiene nuevas entradas (posiciones abiertas siguen) |
| `/resume` | Reanuda el bot |
| `/reset` | Desbloquea circuit breaker manualmente |
| `/mode` | Muestra modo paper vs live |
| `/help` | Lista de comandos |

---

## 🛡 Gestión de Riesgo (config.py)

| Parámetro | Default | Descripción |
|---|---|---|
| `leverage` | 5x | Apalancamiento (empieza con 3-5x) |
| `risk_pct_suprema` | 1.5% | Riesgo por op. señal SUPREMA |
| `risk_pct_fuel` | 1.0% | Riesgo por op. señal FUEL |
| `risk_pct_std` | 0.5% | Riesgo por op. señal STD |
| `max_daily_loss_pct` | 3% | Circuit breaker diario |
| `max_drawdown_pct` | 15% | Circuit breaker permanente |
| `max_consecutive_losses` | 4 | Pause tras N pérdidas seguidas |
| `max_daily_trades` | 10 | Máximo trades por día |

---

## 📊 Señales — Niveles de calidad

| Tier | Condición | Emoji Telegram |
|---|---|---|
| **SUPREMA** | FUEL + Dark Pool ó CVD div. | ⭐⭐⭐ |
| **FUEL** | STD + TL break ó Squeeze ó FVG/OB + CVD | 🔥 |
| **STD** | 6 capas base alineadas | ▶️ |

Con `MIN_CONVICTION=6` sólo se ejecutan señales con ≥6/10 filtros activos.

---

## 🔧 Ajustes recomendados por capital

| Capital | Leverage | Risk/op | Símbolos |
|---|---|---|---|
| < $500 | 3x | 0.5% / 0.8% / 1.2% | 1 (BTC) |
| $500-2K | 5x | 0.5% / 1.0% / 1.5% | 1-2 |
| $2K-10K | 5-10x | 0.3% / 0.8% / 1.2% | 2-3 |
| > $10K | Consult. prof. | < 0.5% | 2-4 |

---

## ⚠️ Disclaimer

Este software es una herramienta de automatización. El trading de futuros de criptomonedas conlleva riesgo de pérdida total del capital. Valida siempre en paper trading antes de usar dinero real. El autor no es responsable de pérdidas.
