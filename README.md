# QF Machine × JP Fusion — Bot v3.0

Bot de trading algorítmico para BingX Perpetual Futures con señales por Telegram.  
Traduce fielmente el indicador Pine Script QF×JP v3 (12 capas) a Python asyncio.

---

## ⚠️ AVISO DE RIESGO

Trading con apalancamiento puede resultar en pérdida total del capital.  
**Empieza SIEMPRE en modo `MODE=SIGNAL` y valida al menos 2 semanas antes de pasar a LIVE.**

---

## Arquitectura

```
qf-jp-bot/
├── bot/
│   ├── main.py              # Loop principal asyncio
│   ├── engine.py            # Motor QF×JP (L1–L12 en Python/NumPy)
│   ├── bingx_client.py      # API BingX Futures (USDT-M)
│   ├── telegram_client.py   # Notificaciones Telegram
│   ├── risk_manager.py      # Tamaño de posición + drawdown
│   └── session_filter.py    # Filtro Asia/Londres/NY
├── config.py                # Todos los parámetros desde .env
├── requirements.txt
├── Dockerfile
├── railway.toml
└── .env.example
```

---

## Capas del motor de señal

| Capa | Nombre | Función |
|------|--------|---------|
| L1 | Microestructura | ATR, spread bid/ask estimado |
| L2 | Motor de Factores | Momentum + Mean-Reversion + OBV (ponderados) |
| L3 | Decaimiento de Señal | IC rolling — la señal se "apaga" si pierde predictibilidad |
| L4 | Dark Pool | Volumen spike + rango estrecho → bloque institucional |
| L5 | Ejecución | Filtro de spread destructivo del alfa |
| L6 | Asimetría Momentum | Velas alcistas vs bajistas (ratio rango) |
| L7 | Ruptura Trendline | Pivots automáticos + break con buffer ATR |
| L8 | Swing Analysis | HL↑ consecutivos = agotamiento vendedor |
| L9 | Fair Value Gaps | Imbalances ICT — precio retestando zona |
| L10 | Order Blocks | Última vela opuesta antes de impulso fuerte |
| L11 | CVD Delta | Buy/sell pressure proxy — divergencias ocultas |
| L12 | Squeeze Momentum | BB dentro de KC → compresión → explosión |

### Tiers de señal

| Tier | Condición | Convicción mínima recomendada |
|------|-----------|-------------------------------|
| STD | L1-L8 alineadas | 5/10 |
| FUEL | STD + TL break ó Squeeze ó FVG/OB con CVD | 7/10 |
| SUP ⭐ | FUEL + Dark Pool ó Divergencia CVD | 8/10 |

---

## Instalación local

```bash
git clone https://github.com/TU_USUARIO/qf-jp-bot.git
cd qf-jp-bot
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
# Edita .env con tus credenciales
python -m bot.main
```

---

## Despliegue en Railway (recomendado)

### 1. Crear bot de Telegram

1. Habla con `@BotFather` → `/newbot`
2. Guarda el token
3. Crea un grupo o canal, añade el bot como admin
4. Obtén el chat_id: envía un mensaje y visita  
   `https://api.telegram.org/bot<TOKEN>/getUpdates`

### 2. Credenciales BingX

1. Entra en BingX → **API Management**
2. Crea clave con permisos: **Read + Trade** (NO Withdraw)
3. Whitelist tu IP de Railway (o déjala abierta solo si usas Railway con IP fija)

### 3. Subir a GitHub

```bash
git init
git add .
git commit -m "QF×JP Bot v3.0"
git remote add origin https://github.com/TU_USUARIO/qf-jp-bot.git
git push -u origin main
```

### 4. Crear proyecto en Railway

1. [railway.app](https://railway.app) → **New Project → Deploy from GitHub**
2. Selecciona tu repositorio
3. Ve a **Variables** y añade una por una:

```
BINGX_API_KEY     = tu_key
BINGX_SECRET      = tu_secret
TG_TOKEN          = 123456:ABC...
TG_CHAT_ID        = -100123456789
MODE              = SIGNAL          ← empieza aquí
SYMBOLS           = BTC-USDT,ETH-USDT,SOL-USDT
LEVERAGE          = 10
RISK_PCT          = 1.0
MAX_DD_PCT        = 5.0
TP_RR             = 2.0
MIN_CONV_STD      = 5
MIN_CONV_FUEL     = 7
MIN_CONV_SUP      = 8
SESSIONS          = NY,LDN
LOOP_INTERVAL     = 30
```

4. Railway detecta el `Dockerfile` y despliega automáticamente
5. Verifica en **Logs** que aparece:
   ```
   QF × JP Bot v3 iniciado
   Balance USDT: xxx.xx
   ```

---

## Flujo de operación

```
Cada 30s por símbolo:
  1. ¿Sesión permitida? (NY/LDN/ASIA)
  2. Descarga 250 velas 3min + 100 velas 15min
  3. Calcula L1→L12 en NumPy
  4. Evalúa señal: STD / FUEL / SUP
  5. Si posición abierta → revisa SL dinámico y señal contraria
  6. Si señal nueva → calcula tamaño (Kelly fraccionado)
  7. SIGNAL: Telegram | LIVE: orden market BingX + SL integrado
```

---

## Gestión de riesgo

- **1% del balance** por trade (ajustable con `RISK_PCT`)
- **SL automático** basado en último swing low/high del indicador
- **TP automático** en R:R 2.0 (ajustable con `TP_RR`)
- **Drawdown diario** máx 5% → bot se detiene automáticamente
- **Apalancamiento** 10× máximo recomendado (usa 5× para empezar)
- Nunca más del 80% del margen disponible en una sola posición

---

## Protocolo de arranque (obligatorio)

```
Semana 1-2 → MODE=SIGNAL   Observa señales, valida calidad
Semana 3   → MODE=LIVE      RISK_PCT=0.5, LEVERAGE=5
Semana 4+  → MODE=LIVE      RISK_PCT=1.0, LEVERAGE=10
```

---

## Símbolos recomendados (BingX)

```
BTC-USDT   ETH-USDT   SOL-USDT
BNB-USDT   XRP-USDT   DOGE-USDT
```

Formato exacto requerido: `BTC-USDT` (con guion, en mayúsculas).

---

## Actualizar el bot

```bash
git add .
git commit -m "update"
git push
```
Railway redespliega automáticamente en segundos.

---

## Licencia

Uso personal. No redistribuir sin autorización.
