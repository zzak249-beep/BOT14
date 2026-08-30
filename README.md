# RSI + SuperTrend — Bot BingX Futures (v2: Doble Suelo Real)

Port a Python del script Pine v6 **"ProBorsa: RSI & SuperTrend Özel Dip Stratejisi"**.
Solo LONG. Sale cuando el SuperTrend gira de alcista a bajista.

**v2 cambia por completo cómo se detecta la entrada** — ver sección 6.

## Estructura

```
rsi-supertrend-bot/
├── main.py                                    # loop principal (asyncio)
├── config.py                                   # variables de entorno
├── bingx_client.py                             # cliente BingX Perpetual Futures v2 (HMAC-SHA256)
├── indicators.py                               # RSI, SuperTrend, detector de doble suelo (puro Python)
├── telegram_notifier.py                        # notificaciones Telegram
├── state_manager.py                            # persistencia JSON con escritura atómica
├── tests/test_indicators.py                    # 18 tests unitarios
├── ProBorsa_RSI_SuperTrend_CiftDip_v2.pine     # indicador/estrategia Pine v6 actualizado (para TradingView)
├── requirements.txt
├── .env.example
├── .gitignore
├── Dockerfile
└── railway.json
```

## 1. Setup local

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env    # completar claves/params
python -m unittest discover -s tests -v   # valida el detector de doble suelo
python main.py
```

Con `DRY_RUN=true` (default) el bot calcula señales, loguea y manda Telegram
pero **no envía ninguna orden**. Déjalo correr así primero y confirmá que
las señales llegan cuando esperás que lleguen antes de tocar `DRY_RUN=false`.

## 2. Claves de BingX

1. BingX → API Management → crear API Key con permisos de **Perpetual
   Futures** (lectura + trading). No actives withdrawals.
2. El bot asume **Hedge Mode** en la cuenta (igual que tus otros bots) —
   confirmalo en BingX antes de operar en real; si tu cuenta está en
   One-way mode hay que cambiar `positionSide` de `LONG` a `BOTH` en
   `bingx_client.py`.
3. Para probar sin arriesgar capital real, BingX tiene un modo demo con
   token **VST** en Perpetual Futures.

## 3. Telegram

1. Hablale a `@BotFather` → `/newbot` → copiá el token → `TELEGRAM_BOT_TOKEN`.
2. Escribile algo a tu bot nuevo, después abrí
   `https://api.telegram.org/bot<TOKEN>/getUpdates` y copiá el `chat.id` →
   `TELEGRAM_CHAT_ID`.

## 4. Deploy en Railway

1. Subí esta carpeta a un repo de GitHub.
2. Railway → New Project → Deploy from GitHub repo.
3. Railway detecta el `Dockerfile` (vía `railway.json`, builder forzado a
   `DOCKERFILE` para evitar los fallos de build de Nixpacks/Metal).
4. Variables → cargar todo lo de `.env.example` con tus valores reales.
5. Si querés que el estado (`STATE_FILE_PATH`) sobreviva a un redeploy,
   montá un **Volume** en `/app/data`; si no, el bot igual reconcilia la
   posición real contra BingX al arrancar, así que nunca queda "ciego".
6. Deploy. Revisá los logs y el mensaje de arranque en Telegram.

## 5. Variables clave

| Variable | Qué hace |
|---|---|
| `SYMBOLS` | uno o varios símbolos separados por coma (`BTC-USDT,ORDI-USDT`) |
| `TIMEFRAME` | `15m` o `30m` — ver sección 7 antes de elegir |
| `PIVOT_LEFT_BARS` / `PIVOT_RIGHT_BARS` | bars a cada lado para confirmar un pivote de precio como dip (default 5/5) |
| `MAX_BOTTOM_DIFF_PCT` | qué tan parecidos en precio deben ser los dos suelos (default 2%) |
| `MIN_BARS_BETWEEN_LOWS` / `MAX_BARS_BETWEEN_LOWS` | separación mínima/máxima entre los dos suelos |
| `MIN_NECKLINE_BOUNCE_PCT` | cuánto debe subir el rebote entre los dos suelos (el "neckline") |
| `REQUIRE_RSI_DIVERGENCE` | exige que el RSI en el 2º suelo sea mayor que en el 1º (divergencia alcista real) |
| `MAX_WAIT_BARS` | cuántas velas espera la ruptura del neckline antes de descartar la señal |
| `USE_HTF_TREND_FILTER` | si está en `true`, no compra cuando la tendencia de `HTF_TIMEFRAME` está cayendo fuerte |
| `HTF_TIMEFRAME` / `HTF_EMA_LENGTH` | temporalidad y período de la EMA de referencia superior (default 4h / 100) |
| `HTF_EMA_SLOPE_LOOKBACK` / `HTF_MAX_DOWN_SLOPE_PCT` | cuántas velas mira hacia atrás la EMA y cuánto puede caer (%) antes de bloquear entradas |
| `POSITION_SIZING_MODE` | `RISK_PERCENT` (% del equity × leverage) o `FIXED_MARGIN` (margen fijo en USDT) |
| `STOP_LOSS_PCT` | 0 = desactivado. Si lo activás, el bot coloca una orden `STOP_MARKET reduceOnly` real en BingX (no un chequeo local) |
| `QUANTITY_PRECISION` / `PRICE_PRECISION` | decimales que exige BingX para el símbolo elegido — confirmalo antes de ir a real |

## 6. Qué cambió en v2 (y por qué)

Mirando el gráfico que mandaste: el v1 contaba cuántas veces el RSI cruzaba
hacia arriba su propia media estando bajo 50 — un proxy de momentum, no una
detección real de "doble suelo". Por eso podía entrar (y entró, en el
último "Long Giriş" del gráfico) mientras el precio todavía estaba haciendo
un mínimo más bajo, justo antes de la vela roja fuerte. El propio script
original ya traía una sección de divergencia RSI real (`bullCond`), pero
estaba desconectada de la entrada — solo era un dibujo.

v2 detecta un "W" de verdad:
1. Busca dos pivotes de mínimo en **precio** (no en RSI).
2. Exige que estén a un precio parecido, con separación razonable en velas,
   y que el rebote entre ambos (el "neckline") sea significativo.
3. Exige que el RSI del 2º suelo sea mayor que el del 1º — divergencia
   alcista real, confirmando que el momentum mejora aunque el precio
   repita el nivel.
4. Recién ahí arma una "señal pendiente", y **solo entra cuando el precio
   rompe el neckline hacia arriba** — no en el instante del cruce de RSI.
   Esto es lo que resuelve el problema de "comprar el cuchillo cayendo".

La salida por SuperTrend no cambió.

Además, ahora hay un **filtro de tendencia de timeframe superior** (lo que
quedaba pendiente): si `USE_HTF_TREND_FILTER=true` (default), antes de
entrar el bot chequea la EMA de `HTF_TIMEFRAME` (4h por defecto) — si cayó
más de `HTF_MAX_DOWN_SLOPE_PCT`% en las últimas `HTF_EMA_SLOPE_LOOKBACK`
velas, la señal se descarta. No se pierde: si el doble suelo es real pero
la tendencia mayor está débil, Telegram manda un aviso "⛔ FILTRADO" aparte
con los mismos datos del setup, para que decidas vos si entrar a mano.
Si falla la consulta de la temporalidad superior, el bot **no entra** ese
ciclo (falla cerrado, no abierto) — misma filosofía de cautela que el
resto del bot. El mismo filtro está en el `.pine` vía `request.security`
(con `close[1]` + `lookahead_off` para no repintar), así el backtest en
TradingView refleja exactamente lo que hace el bot en vivo.

## 7. 15m vs 30m — cómo decidir (no a ojo)

Con pocas señales visibles en el gráfico no alcanza para concluir que 30m
es mejor — podés estar viendo un tramo con suerte. La forma correcta de
decidir:

1. Subí `ProBorsa_RSI_SuperTrend_CiftDip_v2.pine` a TradingView (Pine
   Editor → pegar → Add to chart) sobre el símbolo que te interesa.
2. Abrí el **Strategy Tester** y compará, en la misma ventana de fechas,
   15m vs 30m: cantidad de operaciones, win rate, profit factor, y sobre
   todo el **max drawdown** — timeframes más altos suelen dar señales más
   limpias (menos ruido) pero más lentas y más espaciadas; timeframes
   bajos dan más señales pero más falsas rupturas de neckline.
3. Con esos números elegís `TIMEFRAME` en `.env` — es una sola variable,
   no requiere tocar código.
4. Si el resultado es parejo, un punto medio razonable es correr la
   detección en 30m (estructura más limpia) y no perder velocidad de
   reacción porque `POLL_INTERVAL_SECONDS` ya revisa la vela cerrada más
   reciente en cuanto BingX la publica.

Como mejora futura (no incluida todavía): escalar el tamaño de posición
según la fuerza de la tendencia superior, o exigir divergencia también en
la temporalidad superior. Avisame si querés que lo sume.

## 8. Señales en Telegram

Cada aviso de entrada trae: precio, los dos suelos con su RSI respectivo,
el neckline roto, si hubo divergencia alcista, nivel de SuperTrend (la
referencia de salida), estado de la tendencia superior si el filtro está
activo, stop sugerido si activaste `STOP_LOSS_PCT`, qty y leverage
sugeridos, y timestamp — pensado para operar a mano sin abrir el gráfico.
Las señales que el bot descarta por el filtro de tendencia superior
también avisan (⛔ FILTRADO), con motivo y niveles, para que decidas vos.
Con `DRY_RUN=true` el bot nunca manda una orden real: podés dejarlo así de
forma permanente y ejecutar vos mismo.

## 9. Notas importantes

- El Pine original usa `default_qty_value=100` (100% del equity) — válido
  para backtest, no para real con leverage; el bot usa sizing basado en
  `RISK_PERCENT_EQUITY` por defecto.
- Los endpoints de BingX (`/openApi/swap/v2/...`) están verificados contra
  documentación pública y ejemplos en producción, pero BingX los revisa
  de tanto en tanto — confirmá los paths actuales en
  https://bingx-api.github.io/docs/#/swapV2/introduce antes de ir a real.
- Este documento y el código son soporte técnico, no asesoramiento
  financiero — la estrategia, el apalancamiento y el sizing son decisión
  tuya.
