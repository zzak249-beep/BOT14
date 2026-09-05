# Bot Wavelet MRA — BingX

Descomposición **Haar à trous** causal + cruce sobre la tendencia, con
el régimen medido en dos componentes: energía normalizada por escala y
eficiencia de la tendencia.

**Arranca en SIGNAL.** Lee lo que viene antes de cambiarlo.

---

## Lo que cambió en esta versión

### 1. El motor ahora es à trous de verdad

Antes: el "detalle a escala n" era la diferencia entre dos medias de n
barras separadas n barras. Causal, pero no reconstruye la serie y la
normalización por n no correspondía a la longitud real del filtro.

Ahora, la recursión de Renaud, Starck & Murtagh:

```
S_0(t)     = close(t)
S_{j+1}(t) = [S_j(t) + S_j(t - 2^j)] / 2
w_{j+1}(t) = S_j(t) - S_{j+1}(t)
```

Verificado: reconstrucción exacta (error 0.0) y el valor de la barra t
no cambia 25 barras después.

### 2. El ratio de energía NO distingue tendencia de oscilación

Medido sobre 400 series sintéticas por régimen, con la normalización
correcta:

| Régimen | Mediana del ratio | Pasa 1.30 |
|---|---|---|
| Ruido puro | 0.70 | 6% |
| Tendencia moderada | 1.12 | 36% |
| Tendencia fuerte | 1.94 | 94% |
| **Oscilante** | **1.44** | **64%** |

La oscilante puntúa MÁS ALTO que la tendencia moderada. Es lógico: una
oscilación de amplitud grande también concentra energía en las escalas
gruesas. El ratio mide **tamaño** por escala, no **dirección**.

### 3. La corrección: eficiencia de la tendencia

Kaufman ER calculado sobre S_J (no sobre el precio, que en 5m es
ruidosísimo):

| Régimen | Mediana del ER | Filtro combinado |
|---|---|---|
| Ruido puro | 0.57 | 6% → **4%** |
| Tendencia moderada | 1.00 | 36% → **36%** |
| Tendencia fuerte | 1.00 | 94% → **94%** |
| Oscilante | 0.26 | 64% → **6%** |

Las oscilantes caen del 64% al 6% **sin recortar ni una** de las de
tendencia. `MIN_PERSISTENCE=0.60`, apagable con `USE_PERSISTENCE`.

---

## Cuatro fallos vivos que se han arreglado

1. **`reconcile()` salía antes si no estaba en LIVE.** Un `state.json`
   heredado, o el paso de LIVE a SIGNAL, dejaba posiciones "abiertas"
   eternamente bloqueando el hueco. Ahora corre siempre y limpia el
   estado avisando una vez.

2. **La R se calculaba sin mirar el lado.** `(ultimo - entrada) /
   riesgo` para todo, así que **en los cortos salía invertida**: una
   ganancia se contaba como pérdida. Corrompía `wins/losses`, el
   circuit breaker y el acumulador de pérdida diaria a la vez.

3. **`pending` existía en el estado y nadie lo usaba.** Con
   `ENTRY_TYPE=LIMIT` por defecto, una orden que no se ejecutaba se daba
   por abierta igual: hueco bloqueado, cierre inventado en `reconcile`,
   y la orden viva en el exchange llenándose horas después.
   `LIMIT_TTL_MIN` estaba definido y no se leía en ningún sitio.

4. **La firma no se construía una sola vez.** Se firmaba `urlencode(p)`
   y luego httpx reserializaba el dict. Funcionaba por orden de
   inserción, pero cualquier cambio de versión lo rompía en silencio.
   Ahora la cadena que se firma es literalmente la que viaja, con
   `recvWindow`.

Además: salida por **cruce contrario** (antes solo existía el reloj),
`IDLE_ALERT_DAYS` conectado, Bonferroni con suelo en 3.0 (con un solo
símbolo daba 1.96, menos que el umbral clásico), y los textos heredados
del bot RSI corregidos.

---

## Sobre el 71% y el Sharpe 2.44 del hilo original

No los tomes como referencia. Describen una versión con el filtro
encendido el 92% del tiempo — es decir, un cruce sin filtro efectivo.

**Esta estrategia sigue teniendo cero operaciones medidas en real.**

---

## Lo que hereda

Margen aislado por símbolo · límite global contando toda la cuenta ·
pérdida diaria máxima en R · reconciliación · verificación tras
respuesta perdida · redondeo a la precisión del contrato · diario de
operaciones reales · salida por tiempo que solo corta lo que no va a
favor · avisos agrupados con enfriamiento · watchlist · funding como
contexto · `ensure_config`.

---

## Antes de operarlo

```
python test_telegram.py
python backtest.py BTC-USDT 5m 240 --mensual
python backtest.py BTC-USDT,ETH-USDT,SOL-USDT 5m 240
python sweep.py BTC-USDT,ETH-USDT,SOL-USDT 5m 180
```

Compara `CROSS_SOURCE=trend` contra `price` y `USE_PERSISTENCE=true`
contra `false`. Mira **el agregado**, no el mejor símbolo: elegir los k
mejores de n sesga casi tanto como elegir el mejor de n^k.

Servicio aparte, volumen en `/data`, y para LIVE los dos cerrojos:
`MODE=LIVE` **y** `LIVE_CONFIRMED=true`.


---

## v2 — las cinco mejoras

### 1. Post-only (`POST_ONLY=true`)
Una limitada sin `postOnly` que cruza el spread se ejecuta como **taker**
y paga la tarifa alta sin avisar. Ahora se rechaza en vez de cruzar.
Comisión ida y vuelta: **0.070%** con post-only (0.02 maker entrada +
0.05 taker salida) contra **0.100%** sin él. El rechazo no es un error:
llega a Telegram con la sugerencia de subir `LIMIT_OFFSET_PCT` si pasa
muy a menudo.

### 2. Ranking de candidatos (`RANK_CANDIDATES=true`)
Con 400 símbolos y un hueco, ejecutar la primera que dispara hacía que
el **orden del universo** decidiera qué operas. Ahora se recogen todas
las del ciclo y se ordenan por coste en R ascendente, luego
persistencia y dominancia. Verificado: entre dos con el mismo coste,
gana la de más persistencia.

### 3. TCA por símbolo (`tca.py`, `USE_TCA=true`)
`COST_ROUNDTRIP_PCT` era **una constante para los 400 símbolos**. Ahora
se mide desde el diario:

```
coste = comisión_entrada + comisión_salida + 2 x deslizamiento_mediano
```

El deslizamiento se mide contra el **arrival price** — el precio del
momento de la señal, que es el que supone el backtest. Con menos de
`MIN_TCA_SAMPLES` (10) operaciones se usa la estimación. Los símbolos
cuyo coste medido supere `TCA_BLACKLIST_MULT` x la estimación se
descartan solos, con el motivo en el embudo.

Probado con diario sintético: un símbolo con 0.02% de deslizamiento da
0.110% de coste; uno con 0.25% da **0.603%** y queda descartado.
El informe va en el resumen diario.

### 4. Límite diario de CUENTA (`ACCOUNT_DAILY_LOSS=true`)
`MAX_DAILY_LOSS_R` era por bot. Con dos bots en real sobre la misma
cuenta, eso permite perder **el doble** de lo declarado sin que ninguno
se pare. Ahora se lee el PnL realizado de la cuenta desde las 00:00 UTC
(`/user/income`) y se convierte a R con el riesgo de referencia.

Es aproximado — si los bots usan `RISK_PCT` distintos, la conversión
desvía. Si el endpoint no responde, cae al contador propio en vez de
quedarse sin freno.

### 5. Freno de drawdown (`USE_DD_BRAKE=true`)
`RISK_PCT` es un porcentaje del saldo, así que en drawdown seguías
arriesgando lo mismo de un capital menor. Al caer un 10% desde el pico,
el riesgo se multiplica por 0.5 y **no se restaura hasta recuperar
hasta el 5%**. La histéresis evita que el factor parpadee en el umbral.

Verificado: 135→120 (dd 11.1%) frena; 120→129 (dd 4.4%) libera. Sizing
0.3375 normal, 0.1688 frenado.

---

## Lo que NO se implementó, y por qué

- **Smart order routing multi-venue**: reduce slippage pero añade
  complejidad operativa y exposición a contraparte. Con este tamaño el
  coste operativo supera el ahorro.
- **Volatility targeting a nivel cartera**: la estimación de
  volatilidad mira hacia atrás, así que reduce exposición *después* de
  que suba, y es procíclica. Con un hueco simultáneo no aporta.
- **Modelos de impacto de mercado**: tu orden no mueve el libro.

`LOOKBACK_ENERGY` sube de 40 a **160** en las plantillas: con 40, el
percentil 95 del ratio en ruido puro era 1.41, por encima del umbral de
1.30, y dos ventanas discrepaban en la decisión el 16% de las veces.
