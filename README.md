# Bot Supertrend + Unicorn Model (standalone)

Bot independiente para BingX Perpetual Futures — no comparte código ni
estado con `renewed-love` (CAZADOR) ni `joyful-art` (COMPLEMENTO).
Scanner amplio sobre 500+ símbolos.

## Estrategia — cascada completa de filtros

```
1. Regime Filter (Choppiness Index, 1H)   → bloquea mercados en rango
2. Supertrend custom (BigBeluga, 1H)       → bias macro direccional (gobierna 3a y 3b)
3a. Unicorn Model (3m)                     → sweep + breaker + FVG (timing)
3b. Order Block Engine (BigBeluga, 15m)    → si 3a no confirma: pivote + Order
                                              Block + retest, exigido a superar
                                              un ratio mínimo de volumen
3.5. CVD Filter (opcional)                 → si está activo, exige que el CVD de
                                              candles_entry también apunte en esa
                                              dirección — si no, se prueba el
                                              siguiente motor en vez de cortar
4. Order Flow / Absorción                  → confirma el sweep con trades reales
5. Funding Rate + Open Interest            → confirma "combustible" del movimiento
6. Correlation Manager                     → evita exposición oculta a BTC
7. Setup Memory                            → aprende de setups históricos propios
8. Order Book Imbalance (opcional)         → justo antes de ejecutar: confirma con
                                              el libro de órdenes EN VIVO (no velas,
                                              no trades — lo que está parado ahora
                                              mismo en bids/asks)
```

Cada filtro solo se evalúa si el anterior confirma — pensado para no
malgastar rate limit consultando datos pesados (trades, funding, OI) sobre
500+ símbolos en cada ciclo. Los filtros 4-7 se activan/desactivan
independientemente vía variables de entorno.

**3a/3b son dos motores de entrada en PARALELO, no un AND-gate**: se intenta
primero Unicorn Model; si no confirma, se intenta el Order Block Engine.
Pedir que ambos coincidan en la misma vela sería casi imposible
estadísticamente (dos eventos raros e independientes). El Order Block
Engine trae su "confirmación por volumen" incorporada: el retest solo
dispara si el ratio comprador/vendedor de esa zona supera `OB_MIN_BUY_PCT`
/ `OB_MIN_SELL_PCT`. Nota: el filtro 4 (Order Flow) usa el timestamp del
sweep del Unicorn Model, por lo que actualmente solo aplica a señales de 3a
— las señales de 3b lo saltan (ver `order_block_engine.py`).

## Estructura del repositorio

```
.
├── main.py                    # Orquestador principal (loop de scan + ejecución)
├── config.py                  # Toda la configuración vía variables de entorno
├── unicorn_model.py           # Motor de entrada 3a: sweep + breaker + FVG
├── order_block_engine.py      # Motor de entrada 3b: Order Block + volumen (BigBeluga)
├── cvd_filter.py               # Filtro opcional: Cumulative Volume Delta (sin llamada extra)
├── order_book_imbalance.py     # Filtro opcional: Order Book Imbalance (libro en vivo, confirmación final)
├── supertrend_engine.py       # Motor de bias: custom Supertrend (BigBeluga)
├── combined_engine.py         # Combina Supertrend + Unicorn + Order Block + Regime
├── order_flow.py              # Confirmación: absorción de volumen (trades reales)
├── funding_oi_filter.py       # Confirmación: funding rate + open interest
├── regime_filter.py           # Choppiness Index (detección de rango vs tendencia)
├── correlation_manager.py     # Límite de exposición correlacionada a BTC
├── setup_memory.py            # Aprendizaje adaptativo por tipo de setup
├── position_monitor.py        # Detecta cierres reales y retroalimenta el sistema
├── exchange_client.py         # Cliente async BingX (klines, trades, órdenes, etc.)
├── risk_manager.py            # Sizing, circuit breaker diario, límite de riesgo
├── journal.py                 # Persistencia JSON de señales/operaciones
├── test_order_block_engine.py # Tests sintéticos del Order Block Engine (7 tests)
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
| `SCAN_ALL_SYMBOLS` | `True` | `True` = escanea TODO BingX, ignora `MIN_24H_VOLUME_USDT` |
| `MIN_24H_VOLUME_USDT` | `3000000` | Filtro de liquidez — solo aplica si `SCAN_ALL_SYMBOLS=False` |
| `ENABLE_OB_ENGINE` | `True` | Activa el motor Order Block + Volumen (BigBeluga) |
| `OB_TF` | `15m` | Timeframe propio del Order Block Engine |
| `OB_PIVOT_LEN` | `7` | Barras a cada lado para confirmar un pivote |
| `OB_MIN_BUY_PCT` / `OB_MIN_SELL_PCT` | `50.0` | % mínimo de volumen para confirmar el retest |
| `ENABLE_CVD_FILTER` | `False` | Exige que el CVD (de `candles_entry`) confirme la dirección |
| `CVD_LOOKBACK` | `20` | Velas finas hacia atrás para el cálculo de CVD |
| `ENABLE_OBI_FILTER` | `False` | Confirma con el Order Book Imbalance justo antes de ejecutar |
| `OBI_THRESHOLD` | `0.15` | Desequilibrio mínimo del libro (-1 a 1) para confirmar |
| `ENABLE_ORDER_FLOW_FILTER` | `False` | Confirmación por trades reales (solo aplica a señales del Unicorn Model) |
| `ENABLE_FUNDING_OI_FILTER` | `False` | Confirmación por funding/OI |
| `ENABLE_REGIME_FILTER` | `True` | Bloquea mercados en rango |
| `ENABLE_CORRELATION_FILTER` | `True` | Limita exposición correlacionada a BTC |
| `ENABLE_SETUP_MEMORY_FILTER` | `True` | Aprendizaje adaptativo por setup |
| `RISK_PCT_PER_TRADE` | `0.5` | % de riesgo por operación |
| `DAILY_MAX_LOSS_PCT` | `5.0` | Circuit breaker diario |

Ver `.env.example` para la lista completa (recordá añadir las nuevas
variables del Order Block Engine y `SCAN_ALL_SYMBOLS` a tu propio
`.env.example`, ese archivo no se incluyó en la subida original).

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
- **Order Block Engine como motor paralelo, no como filtro AND** sobre el
  Unicorn Model: exigir que ambos coincidan en la misma vela sería
  prácticamente imposible (dos eventos raros e independientes). Cada uno
  trae su propia confirmación — el volumen ya está incorporado en el
  retest del Order Block Engine, no hace falta un filtro extra
- **`OB_TF` es un fetch de klines independiente** (no reutiliza `HTF_A_TF`
  aunque comparten default `15m`) — con `SCAN_ALL_SYMBOLS=True` esto suma
  una llamada más por símbolo (6 en total) sobre potencialmente 800+
  símbolos; vigilar `SCAN_INTERVAL_SEC` y `SCAN_CONCURRENCY` si el ciclo
  empieza a tardar más de lo esperado
- **`SCAN_ALL_SYMBOLS=True` incluye símbolos muy ilíquidos** — el sizing
  por `RISK_PCT_PER_TRADE` no ajusta por liquidez/slippage esperado; en
  monedas de bajo volumen el fill real puede diferir bastante del precio
  de la señal. Si eso se vuelve un problema, lo más simple es volver a
  `SCAN_ALL_SYMBOLS=False` con un `MIN_24H_VOLUME_USDT` bajo (en vez de 0
  total) más que tocar el sizing
- El Order Block Engine reusa `ST_LEN`/`ST_MULT` (el mismo Supertrend
  custom) para su propia tendencia interna — si tenés pensado tunear esos
  dos valores, afecta a ambos motores por igual
- **Aproximación conocida**: `supertrend_engine.py` calcula el rango medio
  (ATR custom) con la ventana `candles[i-st_len:i]`, que EXCLUYE la vela
  actual; el Pine original (`ta.sma`) la incluye. `order_block_engine.py`
  sí la incluye (fiel al Pine). Es una discrepancia menor preexistente
  entre ambos módulos — no se tocó `supertrend_engine.py` porque no fue
  parte de este pedido, pero como ambos alimentan el mismo bias direccional
  conviene decidir si conviene unificarlos

## Validación realizada

Suite completa en `tests/` (14 tests) + `test_order_block_engine.py` (7 tests
nuevos), todos ejecutables sin red:
- Sweep de liquidez, formación de breaker, filtro de tamaño ATR, FVG sin
  mitigar, confirmación de cierre, cálculo de SL/TP coherente
- Filtro de confluencia direccional (Supertrend rechaza señales contra-tendencia)
- Absorción de order flow (confirma/rechaza según ratio comprador/vendedor real)
- Choppiness Index (distingue tendencia vs rango)
- Correlación con BTC (limita exposición correlacionada duplicada)
- Memoria de setups (aprende de historial propio, permisivo sin muestra)
- Funding rate + OI (confluencia direccional)
- **Order Block Engine**: flip de tendencia, detección de pivote, ratio de
  volumen, lógica booleana de retest (cruce + umbral + anti-repintado +
  supresión por cambio de tendencia), invalidación por ruptura completa,
  y dos end-to-end (confirma LONG con volumen alto, rechaza con volumen bajo)
- **Order Book Imbalance**: cálculo de desequilibrio, respeto del parámetro
  de niveles de profundidad, libro vacío no bloquea, confirma/rechaza según
  umbral, y un end-to-end dentro de `execute_signal` real (libro a favor abre,
  libro en contra no)

**Ya validado en producción (no solo en teoría):**
- **Firma HMAC-SHA256**: el primer POST real de apertura de posición falló
  con "Signature verification failed" — causa encontrada y arreglada
  (`_request` ahora firma y envía la MISMA query string, ver
  `exchange_client.py`). Confirmado con la URL real parseada por yarl.

**Lo que NO fue validado** (requiere acceso a BingX real):
1. Nombres exactos de los endpoints en `exchange_client.py` — el de
   `get_order_book()` es el menos confirmado de todos, se infirió de
   wrappers de terceros, no de tráfico real contra BingX
2. Formato real de campos de la API (`buyerMaker` vs `isBuyerMaker`, etc.)
3. Rendimiento histórico real de la estrategia (backtesting con datos reales)
4. Si el research de OBI (arXiv 2602.00776, validado sobre Binance Futures)
   se sostiene igual en la liquidez y microestructura específicas de BingX

## Pendiente antes de operar en real

1. Confirmar todos los endpoints de BingX contra su documentación vigente
2. Ampliar `NON_CRYPTO_PREFIXES` con tu lista completa de CAZADOR
3. Backtesting con datos históricos reales de BingX (particularmente
   importante para el Order Block Engine — el ratio de volumen y el
   umbral de retest no fueron backtesteados contra datos reales, solo
   validados con velas sintéticas)
4. Con `SCAN_ALL_SYMBOLS=True`, observar en `DRY_RUN` cuántas señales caen
   en símbolos de muy bajo volumen y decidir si conviene volver a
   `SCAN_ALL_SYMBOLS=False` con un `MIN_24H_VOLUME_USDT` bajo en vez de 0
5. Observar la frecuencia real de señales del Order Block Engine en
   `DRY_RUN` — si `OB_TF=15m` resulta demasiado lento/rápido para tu
   gusto, es la variable a tunear primero (junto con `OB_PIVOT_LEN`)
6. Correr un período largo en `DRY_RUN=True` revisando el journal y los
   logs de cada filtro antes de activar órdenes reales — presta atención
   al campo `engine` en el journal para ver el split unicorn vs order_block
