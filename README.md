# 🤖 MZ SAMA Trading Bot

**Slope Adaptive Moving Average** — Bot automático para BingX Futures  
Deploy en Railway · Alertas en Telegram · Gestión de riesgo integrada

---

## ¿Es rentable esta estrategia?

### Análisis de la estrategia MZ SAMA

| Factor | Evaluación |
|---|---|
| **Tipo** | Trend-following adaptativo |
| **Señales** | Pocas pero de alta calidad (no es scalping) |
| **Mejor mercado** | Tendencias claras (BTC bull/bear runs) |
| **Peor mercado** | Rangos laterales prolongados (chop) |
| **Filtro de chop** | ✅ Sí — el parámetro `flat` filtra consolidaciones |
| **Lag** | Alto (length=200) — entrada tardía, salida tardía |
| **Win rate estimado** | 40–55% (típico de trend-following) |
| **R:R por defecto** | 1:2 (SL 1% / TP 2%) |

### Expectativa matemática

```
EV = (winRate × TP%) - (lossRate × SL%)
EV = (0.45 × 2%) - (0.55 × 1%)  =  0.90% - 0.55%  =  +0.35% por trade
```
Con apalancamiento 5x → **+1.75% por señal (bruto, antes de fees)**

BingX cobra ~0.045% maker / 0.075% taker × 2 lados = **~0.15% por operación**  
Beneficio neto estimado: **+1.60% por señal válida**

### Recomendaciones para maximizar rentabilidad

| Parámetro | Por defecto | Recomendado | Motivo |
|---|---|---|---|
| `INTERVAL` | 1h | **4h o 1d** | Menos ruido, señales más fiables |
| `LEVERAGE` | 5x | **3–5x** | Evitar liquidación en chop |
| `RISK_PCT` | 1% | **0.5–1%** | Kelly conservador |
| `TP_PCT` | 2% | **3–4%** | En 4h/1d los movimientos son mayores |
| `SL_PCT` | 1% | **1.5%** | Evitar stop hunting |
| `FLAT` | 17 | **20–25** | Más estricto = menos señales falsas |

> ⚠️ **Importante**: Corre siempre con `DRY_RUN=true` al menos 2–4 semanas antes de capital real.

---

## Estructura del proyecto

```
sama-bot/
├── src/
│   ├── bot.js        ← Orquestador principal
│   ├── strategy.js   ← Lógica SAMA (port de Pine Script)
│   ├── bingx.js      ← Cliente API BingX Futures
│   ├── telegram.js   ← Notificaciones Telegram
│   ├── risk.js       ← Gestión de riesgo / tamaño de posición
│   └── logger.js     ← Logger con archivo
├── logs/             ← Logs locales
├── .env.example      ← Variables de entorno (plantilla)
├── .gitignore
├── package.json
├── railway.toml      ← Config Railway
└── README.md
```

---

## Setup paso a paso

### 1. BingX — Crear API Key

1. Entra en [BingX](https://bingx.com) → tu cuenta → **API Management**
2. Crea una API Key con permisos: **Futures Trading** ✅, **Read** ✅
3. Añade tu IP de Railway (o deja vacío para cualquier IP)
4. Guarda `API_KEY` y `SECRET_KEY`

### 2. Telegram — Crear bot

```bash
# 1. Habla con @BotFather en Telegram
# 2. Envía: /newbot
# 3. Pon un nombre → te da el TOKEN

# 4. Para obtener tu CHAT_ID:
#    Habla con @userinfobot → te dice tu ID
#    O envía un mensaje a tu bot y visita:
#    https://api.telegram.org/bot<TOKEN>/getUpdates
```

### 3. Instalación local

```bash
git clone https://github.com/TU_USUARIO/sama-bot.git
cd sama-bot
npm install
cp .env.example .env
# Edita .env con tus claves
node src/bot.js
```

### 4. Deploy en Railway

#### Opción A — Railway CLI
```bash
npm install -g @railway/cli
railway login
railway init          # en la carpeta del proyecto
railway up
```

#### Opción B — GitHub + Railway web

1. Sube el proyecto a GitHub:
```bash
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/TU_USUARIO/sama-bot.git
git push -u origin main
```

2. En [railway.app](https://railway.app):
   - New Project → Deploy from GitHub repo
   - Selecciona tu repo `sama-bot`
   - Railway detecta Node.js automáticamente

3. Variables de entorno en Railway:
   - Ve a tu proyecto → **Variables**
   - Añade todas las variables de `.env.example` con tus valores reales

4. Railway arranca automáticamente con `node src/bot.js`

---

## Variables de entorno (referencia completa)

| Variable | Descripción | Ejemplo |
|---|---|---|
| `BINGX_API_KEY` | Clave API BingX | `abc123...` |
| `BINGX_SECRET_KEY` | Secret BingX | `xyz789...` |
| `TELEGRAM_TOKEN` | Token del bot Telegram | `1234567890:ABC...` |
| `TELEGRAM_CHAT_ID` | Tu chat ID | `987654321` |
| `SYMBOL` | Par de trading | `BTC-USDT` |
| `INTERVAL` | Temporalidad | `1h`, `4h`, `1d` |
| `SAMA_LENGTH` | Longitud AMA | `200` |
| `MAJ_LENGTH` | Alpha mayor | `14` |
| `MIN_LENGTH` | Alpha menor | `6` |
| `SLOPE_PERIOD` | Período del slope | `34` |
| `SLOPE_RANGE` | Rango del slope | `25` |
| `FLAT` | Umbral consolidación | `17` |
| `LEVERAGE` | Apalancamiento | `5` |
| `RISK_PCT` | % balance por trade | `1` |
| `TP_PCT` | % take profit | `2` |
| `SL_PCT` | % stop loss | `1` |
| `MIN_QTY` | Cantidad mínima | `0.001` |
| `QTY_STEP` | Paso de cantidad | `0.001` |
| `STATUS_EVERY` | Ciclos entre status | `24` |
| `DRY_RUN` | Modo papel | `true` / `false` |

---

## Cómo funciona el bot

```
┌─────────────────────────────────────────────────────┐
│  Cada vela cerrada (INTERVAL)                       │
│                                                     │
│  1. Descarga últimas N velas de BingX               │
│  2. Recalcula SAMA + Slope completo                 │
│  3. Detecta señal (BUY / SELL / CHOP)              │
│                                                     │
│  BUY signal:                                        │
│    → Cierra SHORT si existe                         │
│    → Abre LONG con TP/SL automático                │
│    → Notifica Telegram                              │
│                                                     │
│  SELL signal:                                       │
│    → Cierra LONG si existe                          │
│    → Abre SHORT con TP/SL automático               │
│    → Notifica Telegram                              │
│                                                     │
│  CHOP: no hace nada (filtrado por slope < flat)    │
└─────────────────────────────────────────────────────┘
```

---

## Mensajes Telegram

| Evento | Mensaje |
|---|---|
| Bot arranca | 🚀 Config completa |
| Señal LONG | 🟢 Entry, Qty, TP, SL, Slope |
| Señal SHORT | 🔴 Entry, Qty, TP, SL, Slope |
| Posición cerrada | ✅/❌ PnL en USDT |
| Chop detectado | ⚠️ Sin operación |
| Error | 🚨 Contexto + mensaje |
| Status periódico | 📊 Balance + posición actual |

---

## Seguridad

- Nunca subas `.env` a GitHub (está en `.gitignore`)
- Usa variables de entorno de Railway, nunca hardcodees claves
- Activa la restricción de IP en BingX si Railway tiene IP fija
- Empieza con `DRY_RUN=true` siempre

---

## Disclaimer

> Este bot es software educativo. El trading con apalancamiento conlleva riesgo  
> de pérdida total del capital. El autor no se responsabiliza de pérdidas.  
> **Siempre prueba en paper trading antes de usar capital real.**
