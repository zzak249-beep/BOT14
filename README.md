# Bot RSI Doble Suelo + SuperTrend — BingX

Traducción a Python de la estrategia *ProBorsa: RSI & SuperTrend Özel
Dip Stratejisi*, para correr en Railway sobre perpetuos de BingX.

**Arranca en modo SIGNAL.** Sigue leyendo antes de cambiarlo.

---

## La estrategia

El RSI cruza al alza su propia media móvil. Si eso pasa **por debajo de
50**, se cuenta como un intento. El primero suele fallar; el **segundo**
es el que se opera — eso es lo que dibuja la figura de doble suelo (W).

El contador se reinicia en cuanto el RSI sube de 50: si el mercado ya se
recuperó, el intento anterior dejó de contar.

Salida: cuando el SuperTrend gira a bajista.

---

## Lo que hay que saber antes de ponerle dinero

**Cero operaciones medidas.** Esta estrategia no se ha probado en este
proyecto. Ni una.

**Los parámetros vienen preoptimizados.** El autor del Pine documenta
que bajó el RSI de 14 a 10 *"para aumentar la frecuencia de señales"* y
subió el multiplicador a 2.5 *"para que las salidas sean más
rentables"*. Son ajustes hechos mirando un histórico que no es el tuyo,
en un mercado que probablemente tampoco lo es.

**Cómo medirla:** el Pine original ya es una `strategy()`. Cárgalo en
TradingView sobre los símbolos que operas, con el timeframe que vayas a
usar, y mira operaciones y factor de ganancias antes de nada.

---

## Una tensión del diseño original, resuelta aquí

Al portar la lógica apareció algo que el Pine no controla:

Un doble suelo ocurre **por definición durante una caída**. En el
momento de la señal, el SuperTrend casi siempre está bajista, y su línea
queda **por encima del precio** — así que no sirve como stop.

El Pine no lo nota porque su salida solo se dispara en el **instante**
del giro a bajista. Si ya estaba bajista, la posición se queda abierta
**sin protección** hasta el siguiente giro, que puede tardar días.

Dos opciones, y las dos están:

| `REQUIRE_ST_BULL` | Qué hace |
|---|---|
| `true` (por defecto) | Exige SuperTrend ya alcista. Menos señales, stop coherente. |
| `false` | Fiel al original: entra igual, pero el stop lo pone el mínimo reciente porque el SuperTrend no puede. |

Prueba las dos en TradingView y quédate con la que mida mejor.

---

## Diferencias deliberadas con el Pine

- **Stop siempre presente.** El original no tiene ninguno; aquí el SL
  viaja en la misma orden que la entrada. Sin eso, un bot que se cae
  deja una posición desnuda.
- **Tope de riesgo** (`MAX_RISK_PCT`): si el stop queda a más de un 4%,
  la señal se descarta. El SuperTrend puede quedar lejísimos y una sola
  operación se llevaría varias veces el riesgo previsto.
- **Filtro de liquidez y de amplitud mínima**, para no operar donde el
  coste se come el movimiento.
- **El contador se recalcula entero** en cada evaluación, en vez de
  guardarse entre ciclos: así el estado del bot no puede
  desincronizarse del gráfico si se reinicia.
- **Timeframe 15m por defecto**, no 5m: con salida por SuperTrend las
  operaciones duran más, y en 5m el coste pesaría demasiado.

---

## Despliegue

1. Repo en GitHub con estos archivos en la raíz.
2. `python test_telegram.py` en local.
3. Railway → Deploy from GitHub. **Volumen montado en `/data`.**
4. Pega `railway_vars_SIGNAL.txt` en el Raw Editor.

**Servicio APARTE del bot de reversión.** Comparten formato de estado
pero no el archivo (`state_rsi.json` frente a `state.json`). Dos bots
escribiendo el mismo estado se pisan y acabas sin saber qué operó qué.

Para LIVE hacen falta los dos cerrojos: `MODE=LIVE` **y**
`LIVE_CONFIRMED=true`, más las claves de API con permiso de futuros y
**sin permiso de retirada**.

---

## Archivos

| Archivo | Qué hace |
|---|---|
| `main.py` | Bucle: escaneo, entradas, gestión y cierre por SuperTrend |
| `strategy.py` | Motor — RSI, SMA, ATR, SuperTrend y el contador |
| `bingx.py` | Cliente de la API (compartido con el otro bot) |
| `notify.py` | Telegram y estado en disco |
| `config.py` | Variables de entorno |
| `test_telegram.py` | Comprobación previa |
| `railway_vars_*.txt` | Para pegar en el Raw Editor |
