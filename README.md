# Wavelet MRA Haar 5m — Bot BingX

Bot standalone en Python que replica la lógica del script Pine
`Wavelet MRA Haar 5m — BingX` (filtro de régimen tendencia/ruido por
energía multiescala tipo Haar + cruce sobre SMA8). No depende de
TradingView ni de webhooks: recalcula la señal directamente desde las
velas de BingX, escanea todos los perpetuos USDT-M, opera de forma
automática y avisa cada señal por Telegram para poder operar en
paralelo a mano si quieres.

## Arquitectura

```
main.py              orquesta el bucle de escaneo
bingx_client.py       cliente REST BingX (firma HMAC-SHA256, klines, órdenes)
wavelet_engine.py      puerto 1:1 de la lógica Pine (haar_detail, energías, señal)
risk_manager.py         tamaño de posición y cálculo de SL/TP
telegram_notifier.py    notificaciones
state_manager.py        cooldown + reconciliación de posiciones abiertas
config.py                carga de variables de entorno
```

Las posiciones abiertas se leen siempre directo de BingX en cada
ciclo (nunca de un archivo local), porque Railway puede borrar el
filesystem al redeployar. Al reiniciar el bot no se pierde de vista
ninguna posición real, como mucho se reinicia el cooldown.

## 1. Crear la API Key en BingX

1. BingX → Perfil → Gestión de API → Crear nueva API Key.
2. Marca el permiso de **Trading de Futuros**. No actives retiros.
3. Restringe por IP si tu plan de Railway te da una IP fija; si no,
   déjala sin restricción de IP pero nunca actives permiso de retiro.
4. Guarda `API Key` y `Secret Key`.
5. Comprueba en BingX que tu cuenta de Futuros USDT-M está en **modo
   Hedge** (Position Mode). El bot asume Hedge (usa `positionSide`
   LONG/SHORT explícito), igual que el resto de tus bots en esta cuenta.

## 2. Crear el bot de Telegram

1. Habla con [@BotFather](https://t.me/BotFather) → `/newbot` → copia el token.
2. Escríbele algo a tu bot nuevo, luego abre en el navegador:
   `https://api.telegram.org/bot<TOKEN>/getUpdates` y copia el
   `chat.id` que aparece (o usa [@userinfobot](https://t.me/userinfobot)
   para tu chat_id personal).

## 3. Configurar variables de entorno

Copia `.env.example` a `.env` y rellena tus credenciales. Todas las
variables están documentadas ahí mismo con sus valores por defecto
(idénticos a los `input.*()` del script Pine original).

## 4. Probar antes de arriesgar capital

Recomendado, en este orden:

1. **`LIVE_TRADING=False`** — el bot calcula señales reales sobre
   mercado real y las manda por Telegram, pero nunca manda órdenes.
   Déjalo correr así un tiempo para verificar que las señales tienen
   sentido y la frecuencia es la esperada.
2. Cuando quieras dar el salto, cambia a `LIVE_TRADING=True` y
   considera bajar `QTY_PCT` los primeros días.
3. Opcional: `DEMO_MODE=True` usa el saldo de práctica VST de BingX
   sobre el mismo API. Es experimental en este bot (no se ha podido
   verificar en profundidad el listado de contratos VST) — trátalo
   como una opción a probar, no como garantía; el punto 1 es la forma
   fiable de validar sin arriesgar nada.

El propio script Pine ya avisaba de esto: el 71% de aciertos / Sharpe
2.44 del hilo original no está validado de forma independiente — haz
tu propio backtest/forward test antes de asignarle capital real.

## 5. Subir a GitHub

```bash
cd wavelet-mra-haar-bot
git init
git add .
git commit -m "Wavelet MRA Haar 5m bot"
git branch -M main
git remote add origin https://github.com/<tu-usuario>/<tu-repo>.git
git push -u origin main
```

`.env` está en `.gitignore` — nunca subas tus claves reales al repo.

## 6. Desplegar en Railway

1. Railway → New Project → Deploy from GitHub repo → selecciona el repo.
2. Railway detecta Python automáticamente (`requirements.txt`) y usa
   el `Procfile` (`worker: python main.py`) como comando de arranque.
3. Settings → Variables → pega todas las variables de tu `.env` (con
   tus valores reales, no lo dejes vacío).
4. Deploy. En Logs deberías ver el resumen de configuración al
   arrancar y un mensaje de Telegram "Bot iniciado".

No hace falta exponer un dominio público: es un worker, no un
servicio web. El bot igualmente levanta un pequeño servidor de salud
en el puerto que Railway indique en `PORT` (`/` responde `200 ok`) por
si quieres usarlo como healthcheck.

## Notas sobre los parámetros propios del bot (no están en el Pine original)

- `MAX_CONCURRENT_POSITIONS`: cuenta **todas** las posiciones abiertas
  en la cuenta (de este bot o de cualquier otro proceso), para no
  sobreexponer el capital total si varios símbolos señalan a la vez.
- `SKIP_IF_SYMBOL_HAS_POSITION`: si un símbolo ya tiene posición
  abierta (de este bot o de otro), no vuelve a entrar en él.
- `SYMBOL_BATCH_SIZE` / `SYMBOL_BATCH_DELAY_SECONDS`: controla cuántos
  símbolos se procesan en paralelo y la pausa entre tandas, para no
  saturar el rate limit de BingX al escanear "ALL".
