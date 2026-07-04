# Bot Supertrend + Unicorn Model (standalone)

Bot independiente para BingX Perpetual Futures — no comparte código ni
estado con `renewed-love` (CAZADOR) ni `joyful-art` (COMPLEMENTO).
Scanner amplio sobre 500+ símbolos.

## Estrategia — cascada completa de filtros

```
1. Regime Filter (Choppiness Index, 1H)   → bloquea mercados en rango
2. Supertrend custom (BigBeluga, 1H)       → bias macro direccional
3. Unicorn Model (3m)                      → sweep + breaker + FVG (timing)
4. Order Flow / Absorción                  → confirma el sweep con trades reales
5. Funding Rate + Open Interest            → confirma "combustible" del movimiento
6. Correlation Manager                     → evita exposición oculta a BTC
7. Setup Memory                            → aprende de setups históricos propios
```

Cada filtro solo se evalúa si el anterior confirma — pensado para no
malgastar rate limit consultando datos pesados (trades, funding, OI) sobre
500+ símbolos en cada ciclo. Los filtros 4-7 se activan/desactivan
independientemente vía variables de entorno.

## Estructura del repositorio

```
.
├── main.py                    # Orquestador principal (loop de scan + ejecución)
├── config.py                  # Toda la configuración vía variables de entorno
├── unicorn_model.py           # Motor de entrada: sweep + breaker + FVG
├── supertrend_engine.py       # Motor de bias: custom Supertrend (BigBeluga)
├── combined_engine.py         # Combina Supertrend + Unicorn + Regime Filter
├── order_flow.py              # Confirmación: absorción de volumen (trades reales)
├── funding_oi_filter.py       # Confirmación: funding rate + open interest
├── regime_filter.py           # Choppiness Index (detección de rango vs tendencia)
├── correlation_manager.py     # Límite de exposición correlacionada a BTC
├── setup_memory.py            # Aprendizaje adaptativo por tipo de setup
├── position_monitor.py        # Detecta cierres reales y retroalimenta el sistema
├── exchange_client.py         # Cliente async BingX (klines, trades, órdenes, etc.)
├── risk_manager.py            # Sizing, circuit breaker diario, límite de riesgo
├── journal.py                 # Persistencia JSON de señales/operaciones
├── tests/                     # Suite de tests (datos sintéticos, sin red)
│   ├── test_unicorn_model.py
│   ├── test_order_flow.py
│   ├── test_confluence_filters.py
│   └── run_all.py
├── requirements.txt
├── .env.example
├── .gitignore
├── Procfile                   # Para Railway (worker)
└── railway.json                # Config de despliegue Railway
```

## Cómo correrlo localmente

```bash
git clone <tu-repo>
cd unicorn_supertrend_bot
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # completar BINGX_API_KEY / SECRET
export $(cat .env | xargs)
python3 main.py
```

Por defecto `DRY_RUN=True` — el bot solo loguea las señales que encontraría,
sin enviar órdenes reales.

## Correr los tests

```bash
python3 tests/run_all.py
```

Todos los tests usan datos sintéticos (sin llamadas de red), validan la
lógica pura de cada motor/filtro: detección de sweep, breaker, FVG,
absorción de order flow, régimen de mercado, correlación y memoria de
setups.

## Subir a GitHub

```bash
cd unicorn_supertrend_bot
git init
git add .
git commit -m "Bot Supertrend + Unicorn Model standalone"
git branch -M main
git remote add origin https://github.com/<tu-usuario>/<tu-repo>.git
git push -u origin main
```

El `.gitignore` ya excluye `.env`, `__pycache__/`, archivos `.json` locales
(journal/state — estos viven en el Volume de Railway en producción, no en
el repo) y entornos virtuales.

## Despliegue en Railway

1. Crear un nuevo servicio en Railway apuntando a este repo
2. Railway detecta `railway.json` / `Procfile` automáticamente
3. Configurar todas las variables de `.env.example` en el panel de Railway
4. Montar un **Volume** en `/data` para persistir journal, setup memory y state
5. `DRY_RUN=False` solo cuando estés conforme con el comportamiento en dry-run

## Variables de entorno clave

| Variable | Default | Descripción |
|---|---|---|
| `DRY_RUN` | `True` | Si `False`, envía órdenes reales |
| `ENTRY_TF` | `3m` | Timeframe de timing del Unicorn Model |
| `BIAS_TF` | `1H` | Timeframe del Supertrend / régimen |
| `ENABLE_ORDER_FLOW_FILTER` | `False` | Confirmación por trades reales |
| `ENABLE_FUNDING_OI_FILTER` | `False` | Confirmación por funding/OI |
| `ENABLE_REGIME_FILTER` | `True` | Bloquea mercados en rango |
| `ENABLE_CORRELATION_FILTER` | `True` | Limita exposición correlacionada a BTC |
| `ENABLE_SETUP_MEMORY_FILTER` | `True` | Aprendizaje adaptativo por setup |
| `RISK_PCT_PER_TRADE` | `0.5` | % de riesgo por operación |
| `DAILY_MAX_LOSS_PCT` | `5.0` | Circuit breaker diario |

Ver `.env.example` para la lista completa.

## Notas de diseño y decisiones tomadas

- **Sizing por riesgo fijo**, no Kelly — simple a propósito; portar el
  sizing por tiers (SUP/FUEL/STD) de tus otros bots es un cambio acotado
  a `risk_manager.py`
- **`NON_CRYPTO_PREFIXES`** en `config.py` tiene una lista base — si tu
  CAZADOR ya tiene la lista ampliada de 34 prefijos, conviene copiarla acá
- **Order Flow y Funding/OI empiezan desactivados** (`False`) porque sus
  endpoints en `exchange_client.py` no están verificados contra la
  documentación vigente de BingX (sin acceso de red a BingX desde el
  entorno donde se generó este código) — activarlos solo tras confirmar
  los endpoints y correr un tiempo en `DRY_RUN=True`
- **Regime Filter, Correlation Manager y Setup Memory empiezan activados**
  (`True`) porque su lógica es autocontenida (no dependen de endpoints
  no verificados) y actúan de forma conservadora (con muestra insuficiente,
  siempre permiten operar — no penalizan setups nuevos)
- El **position_monitor** detecta cierres comparando posiciones abiertas
  entre ciclos; usa `get_income_history` para el PnL realizado — verificar
  también este endpoint contra la documentación vigente antes de operar real

## Validación realizada

Suite completa en `tests/` (14 tests), todos ejecutables sin red:
- Sweep de liquidez, formación de breaker, filtro de tamaño ATR, FVG sin
  mitigar, confirmación de cierre, cálculo de SL/TP coherente
- Filtro de confluencia direccional (Supertrend rechaza señales contra-tendencia)
- Absorción de order flow (confirma/rechaza según ratio comprador/vendedor real)
- Choppiness Index (distingue tendencia vs rango)
- Correlación con BTC (limita exposición correlacionada duplicada)
- Memoria de setups (aprende de historial propio, permisivo sin muestra)
- Funding rate + OI (confluencia direccional)

**Lo que NO fue validado** (requiere acceso a BingX real):
1. Nombres exactos de los endpoints en `exchange_client.py`
2. Formato real de campos de la API (`buyerMaker` vs `isBuyerMaker`, etc.)
3. Rendimiento histórico real de la estrategia (backtesting con datos reales)

## Pendiente antes de operar en real

1. Confirmar todos los endpoints de BingX contra su documentación vigente
2. Ampliar `NON_CRYPTO_PREFIXES` con tu lista completa de CAZADOR
3. Backtesting con datos históricos reales de BingX
4. Ajustar `MIN_24H_VOLUME_USDT` según tu tolerancia real de liquidez
5. Correr un período largo en `DRY_RUN=True` revisando el journal y los
   logs de cada filtro antes de activar órdenes reales
