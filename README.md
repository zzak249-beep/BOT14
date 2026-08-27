# Bot de Reversión por Sobreextensión — BingX 5m

Ejecuta en BingX (USDT-M perpetuos) la misma lógica que
`reversion_5m.pine`: cuando el precio se aleja rápido de su media,
entra en contra buscando la vuelta.

**Arranca en modo SIGNAL.** Avisa por Telegram y no toca el exchange.

---

## Lo primero, porque importa más que el código

Esta estrategia tiene **35 operaciones medidas** repartidas en dos
símbolos, y ambos estaban en un pump del +80/+100% ese día. Eso es
una pista prometedora, no una ventaja demostrada.

Lo que **sí** está demostrado con 323 operaciones es que la estrategia
contraria (ruptura de rango) pierde en ese tipo de símbolo.

Traducido a lo práctico: deja el bot en `SIGNAL` unas semanas, acumula
señales, comprueba si se cumplen, y solo entonces plantéate `LIVE`.
Poner dinero con 35 operaciones es apostar, no operar.

---

## El filtro que manda

`MIN_ATR_PCT` y `MIN_COST_COVER` son el hallazgo principal de todo el
trabajo previo, no dos parámetros más:

| Símbolo | ATR | × coste | Resultado |
|---------|-----|---------|-----------|
| CATE    | 5,89% | 42× | 13 operaciones, PF 1,615 |
| JIMOTHY | 5,76% | 41× | 22 operaciones, +0,13R |
| MERL    | 1,49% | 11× | 2 operaciones, −0,15R |
| SEI     | ~0,9% | 6×  | **0 operaciones** |

Donde no hay recorrido no hay negocio: el coste de entrar y salir se
come el movimiento entero. El bot descarta esos símbolos antes de
mirar el patrón.

---

## Escáner del universo completo

Con `SCAN_ALL=true` (por defecto) el bot recorre **todos** los perpetuos
de BingX cada `RANK_INTERVAL_MIN` y manda a Telegram un ranking ordenado
por amplitud:

```
📡 Escaneo BingX — 987 símbolos, 14 con amplitud

🏆 CATE      5.89% (42×)  ER 0.31/0.22  estirado +2.8
🔶 BASECAT  10.43% (74×)  ER 0.03/0.17  estirado +2.7
🟩 CASHCAT   5.49% (39×)  ER 0.25/0.16  fuera +0.4
·  MERL      4.48% (32×)  ER 0.27/0.10  en rango +0.3
```

🏆 lista para operar YA · 🔶 estirada, falta la vela de agotamiento ·
🟩 ruptura (informativo, la estrategia no la opera) · ▫ vertical,
descartada por el filtro de ER · `·` con amplitud, en espera.

El veredicto **no es un criterio aparte del que abre la operación**:
el escáner llama a la misma `strategy.evaluate()` que decide si se
abre o no, así que 🏆 en el ranking y 🔔 SEÑAL / 🟢 EJECUTADO son la
misma cosa vista dos veces, no dos criterios que puedan discrepar.

Justo después del ranking llega un segundo aviso, solo con lo que está
LISTO ahora mismo (o, si no hay nada listo, con lo más cerca de estarlo):

```
🏆 Favoritas de este escaneo — 1 lista(s) para operar

🔴 CATE CORTO — entrada 0.1234  SL 0.135  TP 0.115
   R:R 1.80 · ATR 5.89% (42×) · estirón +2.80 ATR
   🔥 Confirmada por cascada de liquidaciones: 17.8× lo normal,
   5 liquidaciones de cortos en los últimos 1 min
```

La línea 🔥 solo aparece cuando hay una cascada de liquidación real
(Binance + Bybit, gratis, ver más abajo) confirmando la dirección de
la señal — es información añadida, no cambia si el bot abre o no.

Esto sustituye al radar de TradingView, que solo admite diez símbolos
escritos a mano. Aquí no eliges: se miran todos y suben los que valen.

**Sobre el límite de peticiones:** mil símbolos son mil llamadas. Van con
un semáforo (`SCAN_CONCURRENCY=8`). Si BingX devuelve 429, baja ese
número antes que subir el intervalo — es la palanca que no te cuesta
frescura en los datos.

Un ciclo completo tarda del orden de dos a cuatro minutos. Por eso el
ranking va cada 15 y no cada minuto.

---

## Si no da señales: sube de timeframe, no bajes el listón

La volatilidad escala con la **raíz cuadrada del tiempo**: el ATR de 15m
es ≈1.73× el de 5m, y el de 30m ≈2.45×. Pero el coste de operar **no
escala** — la comisión y el spread son los mismos entres en la vela que
entres.

Consecuencia práctica: subir de timeframe da más candidatos **sin
rebajar el filtro**. No es aflojar nada; es que el mismo movimiento pesa
más frente al mismo coste.

Cuando el escaneo no encuentra a nadie, el aviso incluye cuántos
pasarían el MISMO listón en 15m y en 30m. Si ahí hay números, cambia
`TIMEFRAME` y vuelve a medir en TradingView antes de operarlo: la
estrategia se validó en 5m y el comportamiento en 30m hay que
comprobarlo, no suponerlo.

Lo que NO conviene: bajar `MIN_COST_COVER`. Ese umbral no es un gusto
personal, sale de los datos — donde la estrategia funcionó había 40× el
coste y donde no hubo negocio, 6-13×.

---

## Puntuación de confianza de entrada (ordena y mide, no adivina)

`score.py` combina en un solo número (0-100) lo que hasta ahora se
mostraba disperso — margen sobre los mínimos de R:R y cobertura de
coste, confirmación del RSI, cascada de liquidación. Se usa para dos
cosas concretas:

**1. Decidir cuál señal se opera primero.** Antes, con `MAX_CONCURRENT`
limitado, el bot recorría el universo en el orden que devuelve la API
de BingX — arbitrario para lo que importa. Ahora usa la amplitud
(`cover`) del último ranking (`self.last_rows`, ya calculado, sin
llamadas extra) para escanear primero los símbolos con mejor amplitud.
No es un rediseño completo — sigue cortando en cuanto se llenan los
huecos, no escanea el universo entero cada ciclo — pero sube mucho la
probabilidad de que el hueco libre lo llene la mejor oportunidad del
momento, no la primera que aparece.

**2. Quedar registrado para comprobarlo con datos.** Cada operación
cerrada guarda su score junto al R conseguido. El resumen diario
compara la expectativa real por franja de score:

```
🎯 ¿El score predice algo? (comparar franjas)

Score <40 · n=15 · media -0.36R · PF 0.27
Score 80-100 · n=15 · media +0.08R · PF 1.14
```

Si las franjas altas no rinden mejor que las bajas con muestra
suficiente, el score no está aportando nada — y hay que decirlo, no
seguir usándolo por inercia. Es la misma disciplina que ya aplica
`stats.py` al resto del sistema: medir en vez de asumir.

**Qué NO hace:** no sustituye ningún bloqueo existente. El filtro de
contra-tendencia de 30m sigue siendo un bloqueo duro — no se diluye
sumando o restando puntos. `SCORE_MIN` (0 por defecto, desactivado)
puede usarse como un umbral adicional más graduado que un simple sí/no,
pero nadie lo activa por ti: hay que ponerlo a mano.

---

## RSI de doble cruce + radar de 30m (confirmación de entrada)

Dos filtros nuevos sobre las señales de 5m, pensados para trabajar
juntos:

**RSI de doble cruce** (`rsi_confirm.py`) — traducción del script Pine
"ProBorsa: RSI & SuperTrend". No es un cruce de RSI cualquiera: cuenta
cuántas veces el RSI cruza por encima de su propia media móvil
MIENTRAS sigue por debajo de 50, y solo confirma en el 2º cruce desde
la última vez que superó 50 — un doble suelo visto en el RSI en vez de
en el precio. El script original solo detectaba el lado alcista; aquí
se añadió el espejo exacto para el lado corto (doble techo), porque el
bot opera los dos lados.

```
📈 RSI confirma: doble techo hace 1 vela(s) (RSI 68)
```

Con `RSI_REQUIRE=true` (por defecto) es un filtro real: una señal sin
confirmar por RSI **no se envía ni se abre**. Con `RSI_REQUIRE=false`
pasa a ser solo informativo — recomendado si quieres medir primero
cuánto recorta antes de dejarlo bloquear entradas.

**Radar de 30m** (`RADAR30M_ENABLED`) — un segundo escaneo del universo
completo, en 30m, exclusivamente para detectar tendencia de fondo. Si
un símbolo está en RUPTURA clara en 30m, bloquea las señales de 5m que
apuesten EN CONTRA de esa tendencia. No es un filtro cualquiera: ataca
directamente el patrón que el propio histórico de este proyecto
identificó como la principal fuente de pérdidas — largos a
contra-tendencia en mercado bajista (~43% de acierto frente a ~79% en
cortos). Un símbolo sin tendencia clara en 30m (en rango o estirado) no
bloquea nada — se prefiere dejar pasar una señal con contexto
desconocido antes que bloquear todo por falta de dato.

```
🧭 A favor de la tendencia de 30m (bajista)
```

---

## Cascadas de liquidación (confirmación, gratis)

Un módulo aparte, `liquidations.py`, escucha dos streams públicos y
gratuitos — Binance Futures (`!forceOrder@arr`, todos los símbolos de
golpe) y Bybit (`allLiquidation`, símbolo a símbolo) — sin API key ni
cuenta en ninguno de los dos. BingX no publica liquidaciones, y
Coinglass ya no tiene tier gratuito (29$/mes mínimo), así que esto es
la fuente primaria sin intermediario de pago.

**Qué mide:** no basta con que haya habido una liquidación grande —
una liquidación suelta puede ser una sola cuenta, no un mecanismo de
mercado. Se marca cascada cuando, en los últimos `LIQ_SHORT_WINDOW_SEC`
(90s por defecto), hay al menos `LIQ_MIN_EVENTS` (3) liquidaciones
distintas sumando `LIQ_MULTIPLIER` (3×) la actividad normal de ESE
símbolo en los últimos `LIQ_BASELINE_MIN` (30 min), con un piso de
`LIQ_MIN_USD` (5.000$) para que un símbolo casi sin actividad no dé un
falso "3×" sobre una base casi nula. Ese umbral de velocidad+volumen es
el mismo que usó el único backtest de esto con walk-forward que
sobrevivió (PF>2.5 en SOL/ETH; BTC descartado por libro demasiado
profundo para que el sobreimpulso sea operable).

**Qué NO hace:** no decide si se abre una operación. `strategy.evaluate()`
sigue siendo el único criterio de entrada, exactamente igual que antes.
La cascada solo añade una línea 🔥 a la señal cuando la dirección
coincide — largos liquidados (venta forzada) confirma una señal BUY
(fade de bajada); cortos liquidados (compra forzada) confirma una señal
SELL (fade de subida). Se puede desactivar con `LIQUIDATIONS_ENABLED=false`
sin que cambie nada más del bot.

Requiere `websockets` (ya en `requirements.txt`) y salida de red hacia
`fstream.binance.com` y `stream.bybit.com` — en Railway funciona sin
configuración aparte. El estado de conexión de ambos streams
(`Binance ✓ · Bybit ✓`) sale en el resumen diario, por la misma razón
que el latido de Telegram: si un stream se cae, hay que enterarse por
el aviso, no descubrirlo semanas después.

---

## ¿Cómo sé si es rentable?

El bot guarda el resultado real (en R, múltiplos de lo arriesgado) de
cada operación cerrada — no solo si ganó o perdió. Cada resumen diario
incluye el informe de expectancy:

```
📈 Rentabilidad — expectancy en R

SIGNAL · n=40
Media: +0.11R  ·  IC95%: [-0.16, +0.39]
Win rate: 55%  ·  Profit factor: 1.37
Drawdown máx: 4.76R
⚠️ muestra insuficiente — el intervalo cruza cero, podría ser azar
```

**El win rate solo no basta.** Dos sistemas con el mismo % de aciertos
pueden ser uno ganador y otro perdedor según el tamaño de los ganadores
frente a los perdedores. Lo que decide si hay ventaja es la
**expectativa** (media de R por operación).

**Y la expectativa sola tampoco basta con pocas operaciones.** Con la
varianza típica de una reversión (se gana 1-1.5R, se pierde 1R, y suele
haber más pérdidas que aciertos grandes), hacen falta del orden de
100-150 operaciones para que el intervalo de confianza al 95% deje de
tocar cero. Por debajo de eso, un +0.3R de media puede ser tan real
como pura suerte — es la misma cautela que ya pedía este README a mano
("35 operaciones... es apostar, no operar"), aquí convertida en número
exacto: si el IC95% toca cero, con esos mismos datos una estrategia SIN
ventaja real podría dar ese mismo promedio solo por azar.

El informe separa **SIGNAL de LIVE** a propósito: SIGNAL no paga
slippage ni comisión real, LIVE sí — mezclarlos escondería justo la
diferencia que la sección "Riesgo que conviene tener presente" de más
abajo avisa que va a doler.

**Detalle técnico importante:** en modo SIGNAL no hay posición real que
el exchange cierre solo — el bot comprueba cada ciclo si el precio tocó
el SL o el TP contra las velas reales (`reconcile_signal()` en
`main.py`). Sin esto, una señal en SIGNAL no se cerraría nunca salvo por
el límite de tiempo, y las estadísticas de rentabilidad estarían
incompletas desde el principio.

---

## Sección cruzada (opera todos los días)

Segundo sistema, independiente del anterior y con una diferencia clave:
su criterio es **relativo**, no absoluto. Siempre existe un "peor 1%",
así que siempre hay candidatos — justo lo que le falta a la reversión,
cuyo filtro de amplitud deja días enteros sin nada.

**La idea, con respaldo:** un estudio sobre más de 3.600 monedas
encuentra que las cripto con el retorno más bajo del último día superan
sistemáticamente a las de retorno más alto. Cada día a la hora fijada,
el bot ordena el universo por retorno de 24 h y apunta: largo en las N
peores, corto en las N mejores.

**Arranca en modo REGISTRO y no manda órdenes.** Guarda el ranking, y
al día siguiente evalúa qué habría pasado **descontando el coste de ida
y vuelta**. Te llega el resultado del día y el acumulado.

Con 20 días tendrás una respuesta propia a la única pregunta que
importa: ¿queda diferencial después de costes?

**El conflicto que hay que vigilar:** los autores atribuyen el efecto a
la iliquidez, y señalan que las monedas más grandes muestran momentum
diario en vez de reversión — el efecto contrario. O sea que el edge vive
donde más caro es operar. Por eso `XSECTION_MIN_VOL` es más bajo que el
filtro de la otra estrategia: si se filtra igual, se corta justo donde
el efecto es más fuerte. El coste dirá si compensa.

---

## Termómetro del mercado

El resumen diario incluye la temperatura del universo: ATR mediano,
percentil 90 y cuántos símbolos están a media distancia del umbral.

Responde a una pregunta distinta de "¿hay candidatos hoy?": **¿se está
calentando el mercado?** Con la mediana subiendo, van a aparecer
candidatos pronto aunque hoy no haya ninguno. Con la mediana plana,
puedes pasarte semanas sin una sola señal — y eso también es una
respuesta, no un fallo.

Contexto que conviene tener presente: la frecuencia de oportunidades de
esta estrategia depende del RÉGIMEN del mercado. Cuando la dominancia de
Bitcoin es alta y las alts están estancadas, hay poco que filtrar. En
fases de rotación hacia alts, los movimientos del 10-20% diario son
frecuentes y el radar se llena.

---

## Despliegue en Railway

**1. Sube el repositorio a GitHub** con estos archivos en la raíz.

**2. En Railway:** New Project → Deploy from GitHub repo.

**3. Monta un volumen en `/data`.** Sin él, el estado y el circuit
breaker se reinician en cada despliegue. Ya pasó una vez en este
proyecto y se perdió un historial entero.

**4. Variables de entorno:** copia las de `.env.example`. Las mínimas
para SIGNAL son `TELEGRAM_TOKEN` y `TELEGRAM_CHAT_ID`.

**5. Comprueba los logs.** Al arrancar dice el modo y cuántos símbolos
tiene en el universo, y cada ciclo informa de cuántos pasaron el filtro
de amplitud. Si ese número es 0 durante horas, el mercado está tranquilo
— no está roto.

---

## Pasar a LIVE

Hacen falta **dos** interruptores, no uno:

```
MODE=LIVE
LIVE_CONFIRMED=true
BINGX_API_KEY=...
BINGX_API_SECRET=...
```

Si falta alguno, el bot se queda en SIGNAL y lo dice en el log. Dos
cerrojos no es paranoia: el coste de un despliegue equivocado es
dinero, el de un cerrojo extra es un minuto.

Recomendado para el primer LIVE: `RISK_PCT=0.25`, `MAX_CONCURRENT=1`,
`SYMBOL_WHITELIST` con dos o tres símbolos que ya hayas medido.

---

## Detalles de ejecución que evitan rechazos

- Las cantidades y los precios se **redondean a la precisión del
  contrato** que publica BingX. Sin esto, la primera orden en LIVE se
  rechaza por enviar 13847.293847 donde solo se aceptan enteros, y el
  bot parece roto sin estarlo.
- Si el tamaño calculado queda por debajo del **lote mínimo** del
  contrato, la señal se descarta con un aviso explicando por qué: con
  un riesgo del 0.25% y un stop ancho, en algunos símbolos simplemente
  no da para el mínimo.
- Antes de abrir se comprueba si ya hay posición **en el exchange**, no
  solo en el estado propio. Si una posición se abrió fuera del bot o el
  estado se perdió, abrir otra sería doblar el riesgo sin enterarse.

---

## Lo que el bot no hace, a propósito

- No promedia a la baja.
- No reentra tras un stop hasta pasado el enfriamiento.
- No abre una posición sin stop: el SL viaja en la misma orden que la
  entrada, para que una desconexión no deje nada desprotegido.
- No opera símbolos sin amplitud, por bonito que sea el patrón.
- No calcula el drawdown en euros: el circuit breaker cuenta **rachas**
  de pérdidas, porque el bot no lleva la contabilidad del exchange y
  fingir un porcentaje con datos que no tiene sería inventárselo.

---

## Riesgo que conviene tener presente

Ponerse corto contra una moneda que acaba de subir un 100% es de lo
más peligroso que existe: short squeeze, iliquidez, huecos de precio.
El deslizamiento de 2 ticks que asume el backtest es **optimista** justo
en esas condiciones. Si el bot pasa a LIVE, espera resultados peores que
los del Strategy Tester, no iguales.

---

## Archivos

| Archivo | Qué hace |
|---------|----------|
| `main.py` | Bucle de escaneo, señales y ejecución |
| `strategy.py` | Motor — traducción literal del Pine |
| `scanner.py` | Escaneo del universo completo, ranking y favoritas |
| `stats.py` | Expectancy en R, IC95%, profit factor — ¿es rentable o es ruido? |
| `liquidations.py` | Cascadas de liquidación (Binance + Bybit, gratis) — confirmación, no criterio de entrada |
| `rsi_confirm.py` | RSI de doble cruce (traducido de ProBorsa) — confirmación de entrada en 5m |
| `score.py` | Puntuación de confianza 0-100 — ordena el universo y se mide contra el R real |
| `xsection.py` | Sección cruzada (retorno de 24h) |
| `bingx.py` | Cliente de la API (velas, saldo, órdenes) |
| `notify.py` | Telegram y estado en disco |
| `config.py` | Variables de entorno |

Si algún día cambias el Pine, cambia también `strategy.py`. Un bot que
opera algo distinto de lo que backtesteaste no es un bot: es una
sorpresa.

---

## Avisos de Telegram

Comprueba las credenciales **antes** de desplegar:

```bash
python test_telegram.py
```

Un bot que no puede avisarte es un bot mudo, y lo peor es que parece
que funciona: los logs dicen "señal detectada" y a ti no te llega nada.

Qué te va a llegar:

| Aviso | Cuándo |
|-------|--------|
| 🤖 Arranque | Al iniciar, con el modo y los filtros activos |
| 📡 Ranking | Cada `RANK_INTERVAL_MIN`, con el top por amplitud |
| 🏆 Favoritas | Justo después del ranking, solo lo que está LISTO ahora (o lo más cerca) |
| 🔔 Señal | Cuando un símbolo cumple las condiciones (con 🔥 si hay cascada confirmando) |
| 🟢 Ejecutado | Solo en LIVE, al abrir posición |
| ⏱️ Cierre por tiempo | Si la vuelta no llega en `MAX_TRADE_BARS` |
| ⏸️ Circuit breaker | Tras `MAX_CONSECUTIVE_LOSSES` pérdidas seguidas |
| 📊 Resumen diario | A la hora que fijes en `DAILY_SUMMARY_HOUR_UTC` |
| 💓 Latido | Cada `HEARTBEAT_HOURS` |

El resumen diario y el latido existen por una razón concreta: **el
silencio no distingue entre "no hay nada que operar" y "el bot está
caído"**. Si un día no llega el latido, es lo segundo.

---

## Archivos del repositorio

```
main.py                    Bucle: escaneo, señales, salidas por tiempo, avisos
strategy.py                Motor — traducción literal de reversion_5m.pine
scanner.py                 Escaneo del universo completo, ranking y favoritas
stats.py                   Expectancy en R, IC95%, profit factor
liquidations.py            Cascadas de liquidación (Binance + Bybit, gratis)
rsi_confirm.py             RSI de doble cruce — confirmación de entrada
score.py                   Puntuación de confianza 0-100 — ordena y mide
xsection.py                Sección cruzada (retorno de 24h)
bingx.py                   Cliente de la API
notify.py                  Telegram y estado en disco
config.py                  Variables de entorno
test_telegram.py           Comprobación previa de credenciales
requirements.txt           httpx, websockets
Procfile / railway.json    Arranque en Railway
.env.example               Todas las variables documentadas
railway_vars_SIGNAL.txt    Pegar en el raw editor de Railway
railway_vars_LIVE.txt      Ídem, para cuando pases a real
.gitignore
```
