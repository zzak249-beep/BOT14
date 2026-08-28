# RSI Doble Dip + SuperTrend — Bot BingX Futures

Port a Python del script Pine v6 **"ProBorsa: RSI & SuperTrend Özel Dip Stratejisi"**.
Solo LONG. Entra en el 2º cruce alcista de RSI sobre su media (por debajo de
nivel 50) dentro de una ventana — el patrón "doble dip / W" — y sale cuando
el SuperTrend gira de alcista a bajista.

## Estructura

```
rsi-supertrend-bot/
├── main.py                # loop principal (asyncio)
├── config.py               # variables de entorno
├── bingx_client.py         # cliente BingX Perpetual Futures v2 (HMAC-SHA256)
├── indicators.py           # RSI, SuperTrend y el contador de cruces (puro Python)
├── telegram_notifier.py    # notificaciones Telegram
├── state_manager.py        # persistencia JSON con escritura atómica
├── tests/test_indicators.py
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
python -m unittest discover -s tests -v   # valida el port del indicador
python main.py
```

Con `DRY_RUN=true` (default) el bot calcula señales, loguea y manda Telegram
pero **no envía ninguna orden**. Déjalo correr así primero y confirma que
las señales llegan cuando esperás que lleguen antes de tocar `DRY_RUN=false`.

## 2. Claves de BingX

1. BingX → API Management → crear API Key con permisos de **Perpetual
   Futures** (lectura + trading). No actives withdrawals.
2. El bot asume **Hedge Mode** en la cuenta (igual que tus otros bots) —
   confirmalo en BingX antes de operar en real; si tu cuenta está en
   One-way mode hay que cambiar `positionSide` de `LONG` a `BOTH` en
   `bingx_client.py`.
3. Para probar sin arriesgar capital real, BingX tiene un modo demo con
   token **VST** en Perpetual Futures — útil para el primer test en vivo
   antes de pasar a USDT real.

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
| `SYMBOLS` | uno o varios símbolos separados por coma (`BTC-USDT,ETH-USDT`) |
| `TIMEFRAME` | `15m` por defecto, pedido en el prompt |
| `TARGET_CROSS_COUNT` | 2 = doble dip (W), como en el Pine original |
| `POSITION_SIZING_MODE` | `RISK_PERCENT` (% del equity × leverage) o `FIXED_MARGIN` (margen fijo en USDT) |
| `STOP_LOSS_PCT` | 0 = desactivado. El Pine original no trae SL fijo — si lo activás, el bot coloca una orden `STOP_MARKET reduceOnly` real en BingX (no un chequeo local), así que protege la posición aunque el bot esté caído |
| `QUANTITY_PRECISION` / `PRICE_PRECISION` | decimales que exige BingX para el símbolo elegido — confirmalo en las specs del contrato antes de ir a real |

## 6. Señales en Telegram

Cada aviso (entrada y cierre) trae precio, RSI, nivel de SuperTrend (la
referencia de salida real de la estrategia), stop sugerido si activaste
`STOP_LOSS_PCT`, qty y leverage sugeridos, y timestamp de la vela — pensado
para operar a mano desde el celular sin abrir el gráfico. Con
`DRY_RUN=true` el bot nunca manda una orden: solo calcula y avisa, así que
podés dejarlo corriendo en modo "solo señales" de forma permanente si
preferís ejecutar vos mismo.

## 7. Notas importantes

- El Pine original usa `default_qty_value=100` (100% del equity) — eso es
  válido para backtest pero no para real con leverage, así que el bot usa
  sizing basado en `RISK_PERCENT_EQUITY` por defecto. Ajustalo a tu gusto.
- Los endpoints de BingX (`/openApi/swap/v2/...`) están verificados contra
  la documentación pública y ejemplos en producción, pero BingX los
  revisa de tanto en tanto — antes de operar en real confirmá los paths
  actuales en https://bingx-api.github.io/docs/#/swapV2/introduce
- Este documento y el código son soporte técnico, no asesoramiento
  financiero — la estrategia, el apalancamiento y el sizing son decisión
  tuya.
