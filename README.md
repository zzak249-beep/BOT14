# BingX ICT Scanner

Bot 24/7 que escanea **todos los perpetuos USDT-M de BingX**, detecta
setups de barrido de liquidez + FVG dentro de kill zones (puerto de
`ict_killzone_v2.pine`), y opcionalmente ejecuta las entradas en BingX.

## Arquitectura

```
main.py         -> arranca todo, healthcheck, reconciliación, loop principal
scanner.py      -> universo de símbolos + ciclo de escaneo concurrente
strategy.py     -> motor de señales (puerto del Pine, funciones puras)
executor.py     -> sizing, órdenes, gestión de SL/TP/BE
bingx_client.py -> cliente firmado de la API BingX (único archivo que
                    habla con la API — si algo cambia en BingX, es aquí)
state.py        -> persistencia en JSON (atómica)
telegram_notifier.py -> notificaciones con límite de velocidad
healthcheck.py  -> servidor HTTP mínimo para Railway
config.py       -> toda la configuración vía variables de entorno
```

## Antes de nada

Este bot puede colocar órdenes reales con dinero real. Dos reglas que
vienen de bugs ya vividos:

1. **Sub-cuenta dedicada.** Nunca compartas la API key con otro bot.
   Interferencia cruzada y comportamiento no determinista garantizados.
2. **Empieza en `MODE=SIGNAL`.** Deja que corra un par de días, revisa
   las señales en Telegram, y SOLO entonces cambia a `MODE=LIVE`.

## Despliegue en Railway

1. Sube este repo a GitHub (ya viene con un commit inicial listo):
   ```
   git remote add origin https://github.com/TU_USUARIO/TU_REPO.git
   git push -u origin main
   ```
2. En Railway: **New Project → Deploy from GitHub repo**, elige este repo.
3. **Añade un volumen** y móntalo en `/data` (Settings → Volumes). Sin
   esto, el bot pierde el estado —posiciones abiertas, contadores del
   día, progreso de cada setup— en cada redeploy.
4. Pega el contenido de `.env.example` en el editor RAW de Variables,
   rellena `BINGX_API_KEY`, `BINGX_API_SECRET`, `TELEGRAM_BOT_TOKEN`,
   `TELEGRAM_CHAT_ID`. Sin comillas alrededor de los valores.
5. Deploy. En los logs deberías ver `Conectividad OK` y el recuento de
   contratos. Revisa `POSITION_MODE` si el log avisa de un mismatch
   contra lo que reporta BingX.

El `Procfile` lo despliega como `worker` (sin tráfico HTTP entrante
necesario). `healthcheck.py` igualmente expone `PORT` por si prefieres
configurarlo como servicio `web` en Railway para monitorearlo con una URL.

## Variables clave

| Variable | Qué hace |
|---|---|
| `MODE` | `SIGNAL` (solo avisa) o `LIVE` (opera) |
| `POSITION_MODE` | `HEDGE` u `ONEWAY`, según tu cuenta BingX |
| `MIN_RR` | R:R mínimo para aceptar una señal — el filtro que faltaba en el script original |
| `RISK_PCT` | % de equity arriesgado por operación (define el tamaño vía distancia al SL, no vía leverage) |
| `MAX_CONCURRENT_POSITIONS` / `MAX_TRADES_PER_DAY` | frenos de exposición |
| `USE_KILL_ZONES` | pon en `false` para comparar si las kill zones realmente aportan algo en tu universo |
| `ENTRY_MODE` | `CONFIRMATION` (espera cierre confirmado) o `CE` (entra al 50% del FVG) |
| `SYMBOL_BLACKLIST` / `SYMBOL_WHITELIST` | acepta cualquier formato de símbolo, se normaliza solo |

Lista completa y valores por defecto en `.env.example`.

## Verificar antes de MODE=LIVE

Los nombres de endpoint en `bingx_client.py` siguen la documentación
pública de BingX Swap V2, pero **la única forma de confirmar al 100%**
que los parámetros (`positionSide`, `workingType`, precisión de
qty/precio) son correctos para tu cuenta es viéndolo funcionar:

1. Corre en `MODE=SIGNAL` unos días.
2. Cuando pases a `LIVE`, hazlo primero con `MAX_CONCURRENT_POSITIONS=1`
   y `RISK_PCT` bajo, y mira el primer trade de principio a fin en los
   logs y en la app de BingX.
3. Si un endpoint devuelve error, el log muestra el código y mensaje
   originales de BingX — con eso se ubica en su documentación:
   https://bingx-api.github.io/docs/#/en-us/swapV2/

## Diferencias respecto al Pine original

- **Kill zones con `zoneinfo`**: maneja el cambio de horario de verano
  de Nueva York automáticamente; el Pine dependía de offsets fijos.
- **R:R mínimo obligatorio** (`MIN_RR`): el mayor destructor de payoff
  del script original no existía como filtro.
- **Reconciliación al arrancar**: compara el estado guardado contra las
  posiciones reales en BingX y avisa de discrepancias en vez de dejar
  que se acumulen órdenes huérfanas entre redeploys.
- **PnL y sizing calculados en moneda absoluta**, nunca multiplicando
  por el leverage directamente — la causa de un bug real que infló
  valores 10-36x en un bot anterior.

## v1.1.0 — Funding rate y Open Interest como filtros

Dos filtros nuevos, **apagados por defecto**:

- `USE_FUNDING_FILTER`: exige que el funding rate esté a favor de la
  reversión (funding negativo extremo para LONG, positivo extremo para
  SHORT — cortos/largos pagando la otra punta es posicionamiento
  cargado, favorece el rebote).
- `USE_OI_FILTER`: descarta la señal si el interés abierto sube más de
  `OI_MAX_INCREASE_PCT` entre el barrido y la confirmación. OI subiendo
  durante el barrido sugiere posición nueva en contra de la reversión
  (no un flush de liquidaciones), lo que debilita la lectura del setup.

**Aviso de honestidad:** a diferencia de `contracts` y `klines` (usados
y confirmados desde v1.0.0), los endpoints `openInterest` y
`premiumIndex` en `bingx_client.py` **no están verificados contra la
API en vivo** — los trianguleé de varios clientes no oficiales de
BingX y del patrón `/openApi/swap/v2/quote/<nombre>` ya confirmado,
pero no tengo forma de probarlos desde aquí. Por diseño fallan en
silencio (devuelven `None`, no crashean el ciclo) y ambos filtros están
apagados por defecto por esto exacto. Actívalos, mira los logs — si
`get_open_interest`/`get_funding_rate` devuelven error, ahí sale el
código real de BingX para ajustar el endpoint en ese archivo.

Igual que con las kill zones: actívalos uno a la vez y compara el
desglose de resultados antes de asumir que ayudan.

## v1.0.2 — el build seguía fallando: Railpack, no Nixpacks

El log de **Build Logs** (no Details) mostró la causa real:

```
- Staticfile
- Shell
The app contents that Railpack analyzed contains:
./
railpack process exited with an error
```

Dos cosas corregidas:

1. **Railway ya no usa Nixpacks.** Está deprecado/legacy; el builder
   por defecto ahora es **Railpack**. `railway.toml` tenía
   `builder = "NIXPACKS"`, que Railway ignora — de ahí que cayera a
   detectores genéricos (Staticfile, Shell) en vez de reconocer
   Python. Se quitó esa línea para que use el builder actual. La
   variable de version tambien cambio: **`RAILPACK_PYTHON_VERSION`**,
   no `NIXPACKS_PYTHON_VERSION` (la de la v1.0.1 no hacia nada en tu
   builder real).

2. **Sospecha fuerte, sin confirmar:** el log dice que Railpack
   analizó `./` y no encontró nada reconocible — ni `requirements.txt`
   ni `main.py`. Railpack SÍ busca `main.py` en la raíz (confirmado en
   su documentación), así que si no lo encontró, lo más probable es
   que en tu repo de GitHub los archivos estén **un nivel más adentro**
   de lo que Railway está mirando: una carpeta `bingx-ict-scanner/`
   dentro del repo en vez de los archivos sueltos en la raíz. Pasa
   fácilmente si se arrastra la carpeta completa al subidor web de
   GitHub en vez de su contenido.

   **Cómo comprobarlo en 10 segundos:** abre tu repo en GitHub. Si en
   la página principal ves `config.py`, `main.py`, `requirements.txt`
   directamente, la raíz está bien. Si en cambio ves una sola carpeta
   `bingx-ict-scanner` que hay que abrir para llegar a esos archivos,
   ahí está el problema. Dos formas de arreglarlo, la primera es más
   rápida:
   - Railway → tu servicio → Settings → **Root Directory** → pon
     `/bingx-ict-scanner`. No hay que tocar el repo.
   - O mueve los archivos a la raíz del repo (arrastrando en GitHub o
     con `git mv bingx-ict-scanner/* . && git commit`).



## Logs a vigilar

- `Ciclo completo: N símbolos, M señales, ...` — si el tiempo de ciclo
  supera `SCAN_INTERVAL_SEC`, sube el intervalo o reduce el universo.
- `Se filtró más del 90% del universo` — probablemente el campo de
  estado del contrato no es el esperado; revisa `_is_tradable()`.
- `Posiciones abiertas en BingX que el bot NO reconoce` — el bot nunca
  las toca automáticamente, pero avisa por si son de otro bot con la
  misma API key (no debería pasar si sigues la regla de sub-cuenta
  dedicada).
