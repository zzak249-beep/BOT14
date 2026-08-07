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

## v1.5.0 — sweep intra-vela + lead-lag entre símbolos

Pregunta: cómo anticipan los bots de verdad. Respuesta honesta primero:
ningún bot predice precio — reacciona más rápido a información que ya
existe. Dos formas reales de recortar esa distancia, no cosméticas:

**1. `INTRA_CANDLE_SWEEP`** (off por defecto). Antes, el sweep solo se
evaluaba al cerrar la vela — hasta `TIMEFRAME` completo de retraso desde
que ocurre hasta que el bot se entera. Verificado primero contra la
referencia oficial de BingX que cada vela trae su propio `closeTime`, en
vez de asumir si el endpoint incluye la vela en formación o no. Con esto
activo, el sweep se detecta contra la vela aún formándose y se marca
`provisional` — en cuanto cierra de verdad, se revalida: si el cierre
final ya no lo sostiene, se invalida en vez de quedar con un dato
obsoleto. Solo afecta al sweep — la geometría de la FVG y la confirmación
de entrada siguen exigiendo vela cerrada, no tiene sentido evaluarlas a
medio formar.

**2. `USE_LEAD_LAG`** (off por defecto). BTC suele barrer un nivel
primero; los alts correlacionados le siguen segundos-minutos después.
Cuando `LEAD_SYMBOL` (BTC-USDT por defecto) barre, queda registrado en
un estado compartido entre todos los símbolos del escaneo. Cualquier
señal en otro símbolo que coincida en dirección dentro de
`LEAD_LAG_WINDOW_MIN` se etiqueta `lead_confirmed` — puramente
informativo, no filtra nada, viaja hasta Telegram.

Verificado con tests: el modo intra-vela ignora la vela en formación
cuando está apagado, la detecta como provisional cuando está encendido,
y la invalida correctamente si el cierre real ya no la sostiene. El
lead-lag confirma cuando coincide en dirección y ventana, no confirma
fuera de ventana ni en símbolos que no son el líder, y la etiqueta llega
intacta hasta la señal real generada por el pipeline completo.

## v1.4.1 — filtro de profundidad de mecha (idea de Liquidity Reaper [JOAT])

El usuario compartió dos indicadores de terceros para evaluar si servían
a la estrategia. La mayoría de cada uno duplicaba o iba por debajo de lo
que ya existía (confirmación sin FVG, TP1/2/3 fijo) — pero uno tenía una
idea genuinamente nueva: exigir que la mecha del sweep sea al menos un
% del rango total de la vela, no solo "atravesó y volvió".

`SWEEP_MIN_WICK_PCT` (0 = sin filtro, comportamiento idéntico al
anterior). Verificado con test: una mecha del 40% del rango sigue
pasando con el filtro en 25%, una del 5% se rechaza. No crea señales
nuevas — solo descarta sweeps con rechazo débil, lo cual se puede medir
directamente contra el desglose del embudo ya existente (`sweeps=N` en
el log de cada ciclo) antes/después de subir el umbral.

Descartadas del mismo par de indicadores por ahora: pool de varios
niveles sin barrer (amplía el universo de sweeps antes de haber
confirmado que la calidad actual es suficiente) y confluencia de
volumen vía z-score (los dos indicadores tienen filosofías opuestas de
volumen — uno premia expansión, el otro premia escasez — y no hay
todavía una base para decidir cuál aporta sin antes tener el filtro de
mecha como referencia).

## v1.4.0 — Ruta B: Continuación (CHoCH + golden zone)

Pedido: llevar `fibstruct_ict_confluence.pine` (el combinado de dos rutas)
a este bot. La Ruta A (sweep+FVG) ya existía desde v1.0 — le faltaba la
Ruta B (estructura + retroceso a fib), así que se añadió aquí en vez de
crear un tercer bot con infraestructura duplicada.

- `USE_PATH_A` (nuevo, faltaba): apaga/enciende la Ruta A por separado.
  El Pine original ya lo tenía, el puerto no.
- `USE_PATH_B`: BOS/CHoCH sobre pivotes propios (`SWING_LEN`, separado
  de `EQ_PIVOT_LEN`), ancla golden zone (`GZ_LOW`-`GZ_HIGH`) al CHoCH,
  confirma con envolvente (`CONT_CONFIRM=ENGULF`) o solo toque de zona.
- Si las dos rutas disparan direcciones contrarias en el mismo ciclo,
  se anulan ambas — mismo criterio que el resto del embudo.
- `Signal.path` ("REV"/"CONT") viaja hasta Telegram y hasta el desglose
  W/L por ruta en el resumen de cada ciclo (`state.path_stats`, mismo
  patrón que `kz_stats`).

**Bug real encontrado construyendo esto, no solo en el test:** el
ancla del fib se fijaba una vez en el CHoCH y ahí se quedaba. Si el
precio seguía el impulso más allá de esa primera vela (algo normal,
no una rareza), la golden zone quedaba calculada sobre un rango
demasiado corto — zona equivocada. Arreglado con seguimiento en vivo:
el ancla se extiende con cada nuevo máximo/mínimo hasta que se
consume la Ruta B o llega un nuevo CHoCH. Verificado con test: mismo
escenario que antes no generaba señal, ahora sí, con la zona en el
rango correcto.

## v1.3.1 — log visible en Railway al cerrar posiciones de papel

`manage_paper_positions()` y el resumen de ciclo solo notificaban por
Telegram al cerrar una posición de papel — invisible en los logs de
Railway que se revisan todo el rato. Añadido `log.info()` en el cierre
y una línea de resumen W/L acumulado en cada ciclo.

## v1.3.0 — MODE=SIGNAL no cerraba nunca sus propias posiciones de papel

Reportado como "revisa rentabilidad" — no había nada que revisar, y el
motivo era peor que "falta un panel": **no existía ningún mecanismo
para saber si una señal de papel habría ganado o perdido.**

`manage_open_positions()` se sale en la primera línea si `MODE=SIGNAL`
("sin posiciones reales que reconciliar contra el exchange" — cierto,
pero la consecuencia no estaba cubierta). Una posición de papel se
queda en `state.positions` para siempre. Dos efectos, confirmados en
producción con una captura real: **5 posiciones de papel atascadas**,
exactamente en `MAX_CONCURRENT_POSITIONS`, durante varios ciclos:

1. Cero tracking de resultado — nunca se sabe si el SL o el TP se
   habrían tocado.
2. En cuanto se acumulan `MAX_CONCURRENT_POSITIONS` de estas, el bot
   deja de poder registrar señales nuevas — no porque el mercado no
   dé setups, sino porque el hueco nunca se libera.

Añadida `manage_paper_positions()`: compara el precio actual contra el
SL/TP guardado de cada posición de papel y la cierra cuando corresponde,
alimentando el mismo `kz_stats` (W/L por kill zone) que ya usa el panel
para posiciones reales. Si SL y TP se tocan en el mismo hueco de vela,
se cuenta como perdedor (conservador). Cada posición de papel guarda
ahora también `opened_at`, así el aviso de cierre dice cuánto tardó.

Verificado con test: TP cierra como ganador, SL como perdedor, una
posición que sigue dentro de rango no se toca, y el límite de 5 deja de
ser un tope permanente.

## v1.2.0 — el sweep nunca comprobaba el swing más reciente

Reportado como "900 símbolos, 0 señales" durante varios ciclos seguidos.
Verificado con cálculo exacto (no a ojo) que la kill zone SÍ estaba
activa en ese horario — no era eso.

**Causa real, en `detect_sweep()`:** los niveles de referencia eran
`[pdh, rango_sesión, EQH/EQL]` — el swing high/low más reciente
(`swHigh1`/`swLow1`) nunca estaba en la lista, pese a que mi propia
explicación anterior de "cómo funciona" decía que sí. Con PDH/PDL
actualizando una vez al día, el rango de sesión solo disponible tras
sellar (ventana limitada) y EQH/EQL exigiendo dos pivotes casi iguales
(poco frecuente), en la práctica los tres podían estar vacíos a la vez
para un símbolo dado durante la mayor parte del día — el swing corriente
era, con diferencia, la referencia de liquidez más frecuente, y era
justo la que faltaba. Añadido: se reutilizan los mismos pivotes ya
calculados para EQH/EQL, se toma el más reciente de cada lado, y se
suma a la lista de niveles que puede barrer una vela.

Verificado con un escenario sintético donde antes daba 0 sweeps y ahora
detecta 1, llega a FVG, y confirma correctamente.

**Además, diagnóstico nuevo para no tener que adivinar la próxima vez:**
cada ciclo ahora loguea el embudo completo:
```
Embudo: sweeps=N fvgs=N confirmaciones=N | rechazadas por RR=N direccion=N
kz_only=N htf=N premium/discount=N funding=N oi=N | señales=N
```
Si vuelve a haber una sequía de señales, esta línea dice exactamente en
qué etapa se están cayendo — sweeps que nunca llegan a FVG, FVGs que no
confirman, o confirmaciones que mueren en el filtro de R:R o de sesgo
HTF — en vez de tener que especular.

## v1.1.2 — POST en body (no en URL) + diagnóstico de saldo por cuenta

Dos cosas, encontradas en `github.com/BingX-API/api-ai-skills` — el
repo de referencia oficial de BingX para agentes de IA:

**1. Bug latente, no disparado todavía.** Para `POST` (usado por
`place_order` y `set_leverage`), BingX espera la query firmada en el
**body** (`application/x-www-form-urlencoded`), no en la URL. Mi código
mandaba todo por la URL, igual que en `GET`. Como `MODE=SIGNAL` nunca
llega a llamar `place_order`, no se había manifestado — habría fallado
en el primer intento de `MODE=LIVE`. `GET`/`DELETE` sí van en la URL,
confirmado por la misma referencia y por `get_balance()` funcionando
en v1.1.1.

**2. Diagnóstico nuevo: saldo por tipo de cuenta.** Si el balance de
Futuros USDT-M sale en 0 pero crees que hay fondos, `check_connectivity()`
ahora también llama a `/openApi/account/v1/allAccountBalance` (saldo
consolidado por `accountType`) y lo loguea al arrancar. Si aparece
saldo bajo `sopt` (spot/fondos) u otro tipo que no sea el de futuros,
el dinero está ahí — hay que transferirlo dentro de la app de BingX
(Activos → Transferir), no tocar el bot.

**Aparte, algo que solo tú puedes comprobar:** BingX tiene un entorno
de trading simulado (VST) completamente separado, con su propio
dominio (`open-api-vst.bingx.com`, no `open-api.bingx.com`). El bot
solo habla con el dominio real. Si en algún momento activaste o
depositaste en el modo demo/VST de la app, ese saldo no existe para
la API real — no es que el bot no lo vea, es que están en sistemas
distintos.

## v1.1.1 — bug crítico de firma: todo endpoint firmado fallaba, siempre

Primer log real con `bingx-ict-scanner v1.1.0` corriendo confirmó tres
cosas a la vez:

**1. Firma HMAC rota (bug real, no de credenciales).** `_sign()` firmaba
`urlencode(sorted(params))`, pero `_request()` enviaba el dict crudo vía
`params=` de `aiohttp`, que lo reserializa en **orden de inserción** —
una string distinta a la firmada. Confirmado reproduciendo ambas
strings fuera de línea: no coincidían nunca, con cualquier credencial.
`get_balance()`, `get_position_mode()`, y cualquier futura orden en
`MODE=LIVE` fallaban con `error 100001: Signature verification failed`
por esto, no por las API keys. Arreglado: la query string se construye
una sola vez (`_build_query`) y esa misma variable es la que se firma
y la que se envía — ya no hay un segundo punto de serialización que
pueda desincronizarse. Verificado con test que reconstruye la firma
del lado "servidor" y confirma que coincide.

**2. `/openApi/swap/v2/user/positionSide/dual` no existe** (BingX
respondió `error 100400: this api is not exist`). Ese endpoint era mi
mejor suposición para detectar Hedge vs One-Way automáticamente — está
mal, y no tengo un reemplazo verificado. **No bloquea nada**: ya caía
en gracia al valor de `POSITION_MODE` en tus variables, que sigues
teniendo que fijar tú mismo según tu cuenta real.

**3. `TELEGRAM_TOKEN` ≠ `TELEGRAM_BOT_TOKEN`.** El bot esperaba esta
segunda; si tienes la variable puesta con el primer nombre (típico
resto de otro bot), Telegram queda deshabilitado en silencio — no es
un bug, es la variable con el nombre equivocado. Revísala en Railway.

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
