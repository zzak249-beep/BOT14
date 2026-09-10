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
