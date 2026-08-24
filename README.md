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

🔶 BASECAT  10.43% (74×)  ER 0.03/0.17  estirado +2.7
🟩 CASHCAT   5.49% (39×)  ER 0.25/0.16  fuera +0.4
·  CATE      4.48% (32×)  ER 0.27/0.10  en rango +0.3
```

🔶 reversión lista · 🟩 ruptura lista · `·` con amplitud, esperando.

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

## Si no entra nunca: mira el embudo

El resumen diario incluye cuántos símbolos caen en cada filtro:

```
Universo: 536
→ liquidez ≥2.0M: 325
→ ATR ≥7.5%: 7
→ no vertical (ER ≤0.35): 5
→ estirado ≥2.5 ATR: 0
```

**Ojo a una interacción fácil de no ver:** el umbral de ATR que se
aplica de verdad NO es `MIN_ATR_PCT`, sino el mayor de los dos —
`MIN_COST_COVER` × `COST_ROUNDTRIP_PCT`. Con 30× y 0.25% de coste
estás pidiendo **7.5% de ATR**, no 4%.

Y ese umbral se solapa con el de eficiencia: un ATR del 7.5% en velas
de 5m lo tienen casi solo las monedas en pump, pero `MAX_ER_LONG`
descarta precisamente los movimientos verticales. La intersección
existe —JIMOTHY y CATE lo eran— pero es estrecha por construcción.

Si quieres más entradas, la palanca honesta es **bajar
`MIN_COST_COVER` a 20** (ATR efectivo 5%) sabiendo que el coste pesará
más, o **subir el timeframe**, que mejora la cobertura sin tocar el
listón.

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

## Ciclo de vida de una orden limitada

Con `ENTRY_TYPE=LIMIT` la orden **puede no ejecutarse**, y eso está
contemplado:

1. Se envía y queda como **pendiente** — no como posición.
2. Cada ciclo se comprueba contra el exchange. Si hay posición abierta,
   pasa a posición y te avisa.
3. Si a los `LIMIT_TTL_MIN` minutos no ha entrado, **se cancela**.

Una orden colgada no es "una entrada que aún puede salir bien": es una
entrada calculada para un contexto que ya no existe, esperando a
ejecutarse en otro. Esta estrategia entra en agotamientos que duran
minutos; una orden de hace dos horas no tiene nada que ver con la señal
que la creó.

Las pendientes ocupan hueco en `MAX_CONCURRENT` mientras viven, y si un
despliegue deja alguna sin resolver, el bot avisa al arrancar.

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
| `bingx.py` | Cliente de la API (velas, saldo, órdenes) |
| `notify.py` | Telegram y estado en disco |
| `config.py` | Variables de entorno |

Si algún día cambias el Pine, cambia también `strategy.py`. Un bot que
opera algo distinto de lo que backtesteaste no es un bot: es una
sorpresa.

---

## Avisos de Telegram

`RANK_MODE` decide cuánto te habla el bot:

- **`top_siempre`** (por defecto): cada `RANK_INTERVAL_MIN` te manda las
  mejores situadas del mercado, pasen o no el listón, con una marca de
  en qué estado está cada una. Cada mensaje es distinto —cambian los
  símbolos y las cifras—, así que sigues mirándolos.
- **`solo_candidatos`**: solo habla cuando alguno cumple TODOS los
  filtros. Silencio casi absoluto, y el día que suena es importante.


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
| 👀 Vigilancia | Un símbolo pasa todos los filtros y se acerca al umbral, pero aún no ha girado |
| 🔔 Señal | Cuando un símbolo cumple las condiciones |
| 🟢 Ejecutado | Solo en LIVE, al abrir posición |
| ⏱️ Cierre por tiempo | Si la vuelta no llega en `MAX_TRADE_BARS` |
| ⏸️ Circuit breaker | Tras `MAX_CONSECUTIVE_LOSSES` pérdidas seguidas |
| 📊 Resumen diario | A la hora que fijes en `DAILY_SUMMARY_HOUR_UTC` |
| 💓 Latido | Cada `HEARTBEAT_HOURS` |

El aviso de **vigilancia** llega ANTES de que exista la señal: mientras
el precio se estira, no cuando ya se ha girado. Es el que da tiempo a
abrir el gráfico y pasarle el script. Con un enfriamiento por símbolo
para que uno que ronde el umbral no avise cada minuto.

El resumen diario y el latido existen por una razón concreta: **el
silencio no distingue entre "no hay nada que operar" y "el bot está
caído"**. Si un día no llega el latido, es lo segundo.

---

## Archivos del repositorio

```
main.py                    Bucle: escaneo, señales, salidas por tiempo, avisos
strategy.py                Motor — traducción literal de reversion_5m.pine
scanner.py                 Escaneo del universo completo y ranking
bingx.py                   Cliente de la API
notify.py                  Telegram y estado en disco
config.py                  Variables de entorno
test_telegram.py           Comprobación previa de credenciales
requirements.txt           httpx
Procfile / railway.json    Arranque en Railway
.env.example               Todas las variables documentadas
railway_vars_SIGNAL.txt    Pegar en el raw editor de Railway
railway_vars_LIVE.txt      Ídem, para cuando pases a real
.gitignore
```
