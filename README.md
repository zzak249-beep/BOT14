# Bot Wavelet MRA — BingX

Descomposición multiescala causal + cruce sobre la aproximación, con el
filtro de régimen **corregido**.

---

## Por qué no abría ni una operación (`wavelet-2.0`)

Logs del 03-09-2026, ciclo tras ciclo:

```
326 símbolos · 0 señales | sin amplitud: 316 · sin dominancia: 10 · sin cruce: 1
```

El primer filtro mataba el 97% del universo, y por aritmética:

| Puerta | Fórmula | ATR exigido en 5m |
|---|---|---|
| `MIN_COST_COVER=6` × coste 0,25% | ATR ≥ coste × cobertura | **1,50%** |
| `MAX_COST_IN_R=0,20` con SL 1,5 ATR | ATR ≥ coste/(máx·SL) | 0,83% |
| `MIN_ATR_PCT` | — | 0,50% |

Un ATR del **1,5% en una vela de 5 minutos** casi no existe: la propia
lista de vigilancia del bot mostraba 0,62% / 0,68% / 1,03% como los más
volátiles de 326 símbolos. El filtro no estaba estricto: estaba cerrado.
Y había dos puertas midiendo lo mismo con números incompatibles.

**Ahora hay un solo umbral, calculado y anunciado:**

```
ATR mínimo efectivo = max(MIN_ATR_PCT,
                          COST_ROUNDTRIP_PCT × MIN_COST_COVER,
                          COST_ROUNDTRIP_PCT / (MAX_COST_IN_R × SL_ATR))
```

Con los valores nuevos (coste 0,15% · cobertura 2× · máx 0,25 R) sale
**0,40%**, alcanzable a diario por parte del universo. El número se
manda por Telegram al arrancar y cada ciclo se registra la mediana y el
p90 del ATR real frente a él:

```
Ciclo 34s · 318 símbolos · 2 señales | sin dominancia: 210 · sin cruce: 96
 | ATR% med 0.41 p90 0.88 máx 1.640 · umbral 0.40 → pasan 163
```

Si el embudo vuelve a cortarse siempre en el mismo sitio, llega un aviso
a Telegram con los números. Eso es lo que faltaba.

### Lo demás que estaba roto en la ruta LIVE

| Fallo | Consecuencia | Arreglo |
|---|---|---|
| Órdenes LÍMITE dadas por llenadas | El único hueco quedaba bloqueado para siempre y la reconciliación inventaba un cierre | Estado `pending` real: se confirma con el precio de entrada del exchange o se cancela al vencer el TTL |
| R de los cortos con el signo del largo | Cada corto ganador se apuntaba como pérdida y alimentaba el circuit breaker al revés | Signo por lado, y salida estimada en SL/TP si el precio ya los cruzó |
| Firma HMAC sobre una cadena distinta de la enviada | Rechazos en las órdenes con SL/TP (JSON con llaves y comillas) | La cadena se construye una vez, se firma y se envía esa misma |
| `positionSide=LONG` en cuenta unidireccional | BingX rechaza **todas** las órdenes | Detección del modo, `BOTH` cuando toca |
| Capital = margen disponible | "saldo 0" con la cuenta llena, porque otro bot ocupaba el margen | Capital y disponible separados, con aviso explícito |
| Tamaño bajo el lote mínimo → señal descartada | Con 135 USDT casi ningún símbolo entraba | Se sube al mínimo del contrato mientras el riesgo no pase de `MAX_RISK_PER_TRADE_PCT` |
| 326 símbolos leídos de uno en uno, y otra vez para la vigilancia | Ciclos de ~160 s con `SCAN_INTERVAL_SEC=60` | Una pasada en paralelo que sirve para señal, vigilancia y diagnóstico |

---

## El fallo del script original, medido

El script de partida compara la energía de las escalas gruesas (4 y 8
barras) contra las finas (1 y 2), y llama "tendencia" a que ese ratio
supere 1,5.

Lo simulé sobre un **paseo aleatorio puro** — ruido sin ninguna
tendencia:

| Serie | Ratio mediano | % del tiempo con ratio > 1,5 |
|---|---|---|
| **Ruido puro** | 3,04 | **92,6%** |
| Con tendencia | 5,75 | 99,5% |
| Oscilante | 1,21 | 30,8% |

El filtro se enciende el 92,6% del tiempo **en ruido puro**. No
distingue tendencia de aleatoriedad; solo descarta mercados
fuertemente oscilantes.

**La causa es matemática:** la diferencia entre dos medias de 8 barras
tiene mucha más varianza que entre dos de 1 barra, así que el
numerador arranca inflado. En un análisis wavelet serio la energía se
normaliza por escala antes de compararla.

**La corrección:** dividir la energía de cada escala por su longitud.
Con eso el mismo ruido puro da mediana **0,75**, y un umbral de 1,30
significa algo.

### Lo que cambia en la práctica

Backtest sintético, mismas series, SL 1,5 ATR / TP 2,5 ATR:

| Modo | Serie | Ops | Expectativa |
|---|---|---|---|
| **Corregido** | tendencia | 57 | **+1,076 R** |
| **Corregido** | ruido | 11 | −0,076 R |
| **Corregido** | oscilante | **0** | — |
| Original | tendencia | 136 | +0,825 R |
| Original | ruido | **146** | +0,215 R |
| Original | oscilante | **54** | **−0,898 R** |

El original opera 146 veces en ruido y 54 en mercado oscilante, donde
pierde casi 0,9 R por operación. El corregido evita ese terreno casi
por completo.

Puedes comparar los dos con `NORMALIZE_SCALES=false` y
`DOMINANCE_THRESHOLD=1.5` — no hace falta que me creas.

---

## Sobre el 71% y el Sharpe 2,44 del hilo original

No los tomes como referencia. Describen una versión con el filtro
encendido el 92% del tiempo — es decir, un cruce de precio sobre SMA(8)
sin filtro efectivo. Cualquier parecido con esta versión es casual.

**Esta estrategia tiene cero operaciones medidas en datos reales.**

---

## Lo que sí hereda: la infraestructura

Todo lo que costó encontrar en producción con los bots anteriores:

- **Margen aislado** por símbolo. Con cruzado, una cascada puede
  liquidar la cuenta entera antes de que salte el stop.
- **Límite global de posiciones** contando las de TODA la cuenta, no
  solo las de este bot. En un desplome las alts se mueven juntas.
- **Pérdida diaria máxima** en R. El circuit breaker por rachas no
  cubre seis pérdidas alternadas con dos ganancias.
- **Reconciliación** contra el exchange: detecta los cierres por SL/TP
  que el bot no vio.
- **Verificación tras respuesta perdida**: si el envío da timeout pero
  la orden existe, no se abre otra encima.
- **Redondeo a la precisión del contrato**, o BingX rechaza la orden.
- **Diario de operaciones reales** en CSV con deslizamiento y R real.
- **Salida por tiempo que solo corta lo que no va a favor.**

---

## Antes de operarlo

```
python backtest.py BTC-USDT 5m 240 --mensual
python backtest.py BTC-USDT,ETH-USDT,SOL-USDT 5m 240
python sweep.py BTC-USDT,ETH-USDT,SOL-USDT 5m 180
```

El primero mide un símbolo con desglose mensual. El segundo, varios. El
tercero prueba combinaciones de umbral y salidas mostrando **las dos
mitades del histórico**: la que solo funciona en la primera es
sobreajuste y lo marca.

Usan el mismo `strategy.py` que ejecuta el bot, así que miden
exactamente lo que operarías.

---

## Lo que esta estrategia es, sin el envoltorio

Quitando la capa wavelet, el disparador es **un cruce de precio sobre
SMA(8)**. Y sobre eso hay evidencia concreta:

> *"En mercados en rango —que son la mayoría, la mayor parte del
> tiempo— cada cruce parece una ruptura y cada uno se gira a los pocos
> bares."*

Un cruce sin filtros dispara 30-50 operaciones por trimestre, **60-65%
son perdedoras**, y el factor de ganancias queda en **~1,0**: breakeven
menos comisiones. Y sobre 5 minutos en particular: *"disparan demasiado
y hacen más whipsaw; se pueden operar, pero es mucho más difícil
sacarles beneficio"*.

Las fuentes coinciden en tres correcciones, y las tres están puestas:

| Corrección | Cómo se implementa | Estado |
|---|---|---|
| **Régimen** (ADX > 20-25) | `DOMINANCE_THRESHOLD` normalizado | activo |
| **Volumen** en la vela del cruce | `USE_VOL_FILTER`, 1,2× la media | **activo** |
| **Tendencia de fondo** | `USE_HTF_FILTER`, SMA(200) | **activo** |

El de volumen es el que las fuentes destacan: *"este filtro por sí solo
elimina una porción significativa de los whipsaws"*.

### Lo que cuesta cada filtro, medido

| Configuración | Ops (tendencia) | Ops (ruido) |
|---|---|---|
| Solo régimen | 89 | 18 |
| + volumen | 18 | 4 |
| + volumen + tendencia | 15 | 4 |

Los filtros recortan las señales a una sexta parte. **Eso es lo
esperado y lo que buscas**: de las 89 originales, la mayoría eran
whipsaws que el propio estudio predice.

También hay `USE_TRAILING` disponible y apagado — las fuentes dicen que
un trailing supera a la salida fija porque captura la continuación,
pero eso es una hipótesis que hay que medir, no una certeza.

---

## El sesgo que arruina los escáneres, y cómo se controla aquí

Este bot mira **327 símbolos**. Eso no es una ventaja gratis: es hacer
327 apuestas a la vez. Por puro azar, algunas van a salir bien.

La literatura sobre *data snooping* es contundente. Un estudio sobre
447 "anomalías" publicadas encontró que **el 85% no explicaba nada**, y
que del resto **el 93% no sobrevivía** a exigir un t-estadístico ≥ 3 en
vez del 2 habitual. Harvey, Liu y Zhu recomiendan ese umbral
precisamente cuando se ha buscado mucho.

Por eso `backtest.py` ya no se limita a darte la expectativa: calcula
el **t-estadístico** y lo compara con tres umbrales.

```
t-estadístico: +4.74   (n=300)
  umbral clásico 2.00 · data snooping 3.00 · Bonferroni 300 símbolos: 3.76
  -> PASA incluso ajustando por multiplicidad
```

Con una expectativa de +0,15 R y 300 operaciones, pasa. Sin edge real,
sale "no distinguible de cero" aunque el total sea positivo.

**Y una trampa que conviene evitar:** si backtesteas 50 símbolos y te
quedas con los 5 mejores, eso es data snooping puro — el sesgo de
elegir los k mejores de n es casi tan grande como elegir el mejor de
n^k. Mira **el agregado**, no los ganadores.

---

## Funding: por qué el carry no es para esta cuenta, y qué sí

El **carry** (comprar spot y vender el perpetuo) es la única estrategia
del proyecto con edge **estructural** en vez de estadístico: cobras el
funding sin apostar a la dirección. Los fondos que lo hacen reportan
drawdowns por debajo del 1% y rendimientos del 10-30% anual.

Con 135 USDT, repartidos en dos piernas de 67,5:

| Funding | Ingreso diario | Anualizado | Días para cubrir la comisión |
|---|---|---|---|
| 0,01%/8h (normal) | **0,020 USDT** | 11% | **16,7** |
| 0,03%/8h | 0,061 USDT | 33% | 5,6 |
| 0,05%/8h | 0,101 USDT | 55% | 3,3 |
| 0,08%/8h | 0,162 USDT | 88% | **2,1** |

Con funding normal son **dos céntimos al día** y más de dos semanas
recuperando el coste de abrir. Las fuentes citan 2.000 USD como mínimo
razonable — tienes el 7%.

**No es un problema de optimización: el porcentaje es correcto y la
base es demasiado pequeña.** Por eso el bot no monta carry.

### Lo que sí hace

**Avisa del funding extremo**, con el cálculo hecho sobre tu saldo real:

```
💸 Funding extremo — 3 símbolo(s)

✅ XYZ  +0.0912%/8h  (100% anual)  largos pagan  ·  cubre coste en 1.8 días
·  ABC  +0.0540%/8h  (59% anual)   largos pagan  ·  cubre coste en 3.1 días
```

**Y añade el posicionamiento a cada señal:**

```
Funding +0.0912%/8h — largos amontonados
```

Eso importa porque conecta con dos cosas ya investigadas aquí: el
funding extremo sostenido ha precedido a reversiones fuertes, y donde
hay posicionamiento amontonado hay combustible para una cascada de
liquidaciones. Un corto con funding muy positivo va **a favor** del
desequilibrio; un largo con ese mismo funding va en contra.

Va al diario en su propia columna. Dentro de un mes podrás comprobar si
tus ganadoras se concentran donde el funding era extremo — que es la
única forma de saberlo.

---

## Despliegue

Servicio **aparte**, volumen en `/data`, `railway_vars_SIGNAL.txt` en el
Raw Editor y `python test_telegram.py` antes de nada.

Para LIVE hacen falta los dos cerrojos: `MODE=LIVE` **y**
`LIVE_CONFIRMED=true`, más claves con permiso de futuros y **sin
retirada**.
