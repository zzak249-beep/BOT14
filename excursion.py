"""
excursion.py — ¿dónde debería estar de verdad el stop y el objetivo?

    python excursion.py crowding_ops.csv
    python excursion.py operaciones_wavelet.csv --tf 5m
    python excursion.py senales_todas.csv --barrido

═══════════════════════════════════════════════════════════════════════
POR QUÉ ESTO Y NO OTRO FILTRO DE ENTRADA
═══════════════════════════════════════════════════════════════════════
"Más precisión" suena a añadir una condición más. Pero una operación
solo puede fallar de cuatro formas, y cada una pide un arreglo
DISTINTO:

  1. El precio nunca fue a favor            -> la señal no vale
  2. Fue a favor y volvió a matar el stop   -> el stop está mal puesto
  3. Llegó cerca del objetivo y no lo tocó  -> el objetivo está lejos
  4. Se quedó parado hasta que venció       -> sobra el reloj o falta trailing

Desde fuera las cuatro son "una perdedora". Y si son la 2 o la 3, meter
otro filtro de entrada NO arregla nada: estarías tirando señales buenas
por un problema de salida.

Lo que las separa es la EXCURSIÓN: cuánto llegó a ir en contra (MAE) y
cuánto llegó a ir a favor (MFE) cada operación, en R. Es el único
análisis que responde "dónde se pierde la precisión" con datos en vez
de con opinión.

La lectura que decide todo:

    MAE de las GANADORAS. Si ninguna ganadora pasó nunca de -0.6 R
    antes de girarse, el stop a -1.0 R está regalando 0.4 R de riesgo
    en cada operación sin comprar nada. Si en cambio las ganadoras
    llegan rutinariamente a -0.9 R, el stop está donde debe y
    apretarlo mataría justo las que pagan.

═══════════════════════════════════════════════════════════════════════
LO QUE NO PUEDE HACER — LÉELO ANTES DE TOCAR NADA
═══════════════════════════════════════════════════════════════════════
El barrido de stops (--barrido) es optimización EN MUESTRA. Con 52
operaciones, el mejor stop del barrido casi siempre es ruido: probar 12
stops sobre las mismas 52 operaciones es hacer 12 apuestas y quedarse
con la que salió. Por eso el barrido sale APAGADO, imprime el umbral
corregido, y el script te dice cuánta muestra haría falta.

La regla honesta: el barrido sirve para DESCARTAR (si ningún stop
convierte el conjunto en positivo, el problema no es el stop), no para
elegir (si uno sale bien, es una hipótesis para el periodo siguiente).

Y la excursión se mide con velas cerradas, así que dentro de la vela no
se sabe el orden real. Cuando el máximo y el mínimo de una vela tocan
los dos, se supone el PEOR caso, igual que backtest.py.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import math
import statistics as st
import sys
import time

import requests

BASE = "https://open-api.bingx.com"
MS = {"1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
      "30m": 1_800_000, "1h": 3_600_000, "4h": 14_400_000}
PACING = 0.12


# ── datos ─────────────────────────────────────────────────────────────
def velas(symbol: str, desde_ms: int, hasta_ms: int, interval: str) -> list[dict]:
    paso = MS.get(interval, 900_000)
    n = int((hasta_ms - desde_ms) / paso) + 3
    try:
        r = requests.get(f"{BASE}/openApi/swap/v3/quote/klines", timeout=20,
                         params={"symbol": symbol, "interval": interval,
                                 "startTime": desde_ms, "limit": min(max(n, 2), 1000)})
        if r.status_code != 200:
            return []
        d = r.json()
        if d.get("code") not in (0, None):
            return []
        filas = d.get("data", [])
    except requests.RequestException:
        return []
    out = []
    for k in filas:
        try:
            if isinstance(k, dict):
                out.append({"t": int(k.get("time", 0)), "h": float(k["high"]),
                            "l": float(k["low"]), "c": float(k["close"])})
            else:
                out.append({"t": int(k[0]), "h": float(k[2]),
                            "l": float(k[3]), "c": float(k[4])})
        except (KeyError, ValueError, TypeError, IndexError):
            continue
    out.sort(key=lambda x: x["t"])
    return [v for v in out if desde_ms <= v["t"] <= hasta_ms]


def _ts(x: str) -> int | None:
    try:
        return int(dt.datetime.fromisoformat(x.replace("Z", "+00:00")).timestamp() * 1000)
    except (ValueError, AttributeError):
        return None


def _f(x, d=None):
    try:
        return float(x)
    except (TypeError, ValueError):
        return d


def leer(ruta: str) -> list[dict]:
    """
    Acepta los tres CSV de la flota. Lo único obligatorio es: símbolo,
    lado, momento de apertura, entrada y stop — el riesgo en precio sale
    de ahí y es el denominador de todo lo demás.
    """
    ops = []
    for enc in ("utf-8-sig", "utf-8"):
        try:
            with open(ruta, newline="", encoding=enc) as f:
                for x in csv.DictReader(f):
                    ab = (x.get("abierta_utc") or x.get("fecha_utc") or "").strip()
                    ce = (x.get("cerrada_utc") or "").strip()
                    t0 = _ts(ab) or _f(x.get("ts_señal"))
                    if not t0:
                        continue
                    entrada = _f(x.get("entrada") or x.get("entrada_real")
                                 or x.get("entrada_esperada") or x.get("price"))
                    sl = _f(x.get("sl"))
                    if not entrada or not sl or entrada <= 0:
                        continue
                    lado = (x.get("lado") or x.get("side") or "").upper()
                    ops.append({
                        "symbol": x.get("symbol", "?"),
                        "largo": lado in ("LONG", "BUY"),
                        "t0": int(t0),
                        "t1": _ts(ce),
                        "entrada": entrada,
                        "sl": sl,
                        "tp": _f(x.get("tp")),
                        "motivo": x.get("motivo", ""),
                        "r_real": _f(x.get("r_neto") or x.get("r_real")),
                        "coste_r": _f(x.get("coste_r"), 0.0) or 0.0,
                        "barras": x.get("barras"),
                        # senales_todas.csv trae el timeframe de cada señal:
                        # con TIMEFRAMES múltiples, resolverlas todas en 15m
                        # mediría otra cosa.
                        "tf": (x.get("timeframe") or "").strip(),
                    })
            break
        except UnicodeDecodeError:
            ops = []
    return ops


# ── excursión ─────────────────────────────────────────────────────────
def excursion(op: dict, tf: str, horas: float) -> dict | None:
    """
    MAE y MFE en R, más la barra en la que se alcanzó cada una.

    La barra del máximo favorable es la que contesta si el reloj corta
    pronto o tarde: si el MFE se alcanza en la barra 3 y el límite son
    16, las trece restantes solo sirven para devolverlo.
    """
    paso = MS.get(tf, 900_000)
    fin = op["t1"] or (op["t0"] + int(horas * 3_600_000))
    v = velas(op["symbol"], op["t0"] + paso, fin + paso, tf)
    if not v:
        return None
    riesgo = abs(op["entrada"] - op["sl"])
    if riesgo <= 0:
        return None

    mae = 0.0
    mfe = 0.0
    bar_mae = 0
    bar_mfe = 0
    for i, k in enumerate(v):
        if op["largo"]:
            adv = (op["entrada"] - k["l"]) / riesgo
            fav = (k["h"] - op["entrada"]) / riesgo
        else:
            adv = (k["h"] - op["entrada"]) / riesgo
            fav = (op["entrada"] - k["l"]) / riesgo
        if adv > mae:
            mae, bar_mae = adv, i + 1
        if fav > mfe:
            mfe, bar_mfe = fav, i + 1
    return {"mae": mae, "mfe": mfe, "bar_mae": bar_mae, "bar_mfe": bar_mfe,
            "n_velas": len(v)}


def simular(op: dict, stop_r: float, tp_r: float, max_barras: int | None) -> float | None:
    """
    Re-simula la operación con OTRO stop y OTRO objetivo, vela a vela.

    No vale reescalar el MAE: una operación que con stop -1.0 murió en
    la barra 4 pudo, con stop -1.5, seguir viva y tocar el objetivo en
    la 9. Hay que recorrer las velas otra vez. Si en la misma vela se
    tocan los dos, manda el STOP.
    """
    if op.get("_v") is None:
        return None
    riesgo = abs(op["entrada"] - op["sl"])
    if riesgo <= 0:
        return None
    for i, k in enumerate(op["_v"]):
        if op["largo"]:
            adv = (op["entrada"] - k["l"]) / riesgo
            fav = (k["h"] - op["entrada"]) / riesgo
        else:
            adv = (k["h"] - op["entrada"]) / riesgo
            fav = (op["entrada"] - k["l"]) / riesgo
        if adv >= stop_r:
            return -stop_r - op["coste_r"]
        if fav >= tp_r:
            return tp_r - op["coste_r"]
        if max_barras and i + 1 >= max_barras:
            c = k["c"]
            bruto = (c - op["entrada"]) if op["largo"] else (op["entrada"] - c)
            return bruto / riesgo - op["coste_r"]
    if op["_v"]:
        c = op["_v"][-1]["c"]
        bruto = (c - op["entrada"]) if op["largo"] else (op["entrada"] - c)
        return bruto / riesgo - op["coste_r"]
    return None


# ── informe ───────────────────────────────────────────────────────────
def pct(xs: list[float], p: float) -> float:
    if not xs:
        return 0.0
    s = sorted(xs)
    i = min(int(p / 100.0 * (len(s) - 1)), len(s) - 1)
    return s[i]


def z_bonferroni(k: int) -> float:
    p = 0.05 / (2.0 * max(k, 1))
    t = math.sqrt(-2.0 * math.log(p))
    return t - ((2.515517 + 0.802853 * t + 0.010328 * t * t) /
                (1 + 1.432788 * t + 0.189269 * t * t + 0.001308 * t ** 3))


def informe(ops: list[dict], stop_actual: float, tp_actual: float) -> None:
    gan = [o for o in ops if (o["r_real"] or 0) > 0]
    per = [o for o in ops if (o["r_real"] or 0) <= 0]

    print(f"\n{'=' * 72}\n1 · MAE — ¿DÓNDE DEBERÍA ESTAR EL STOP?\n{'=' * 72}")
    print("Cuánto llegó a ir EN CONTRA cada operación antes de resolverse.\n")
    print(f"{'grupo':<22} {'n':>4} {'mediana':>9} {'p75':>8} {'p90':>8} {'máx':>8}")
    for nombre, g in (("GANADORAS", gan), ("perdedoras", per), ("todas", ops)):
        xs = [o["mae"] for o in g]
        if not xs:
            continue
        print(f"{nombre:<22} {len(xs):>4} {st.median(xs):>9.2f} {pct(xs,75):>8.2f} "
              f"{pct(xs,90):>8.2f} {max(xs):>8.2f}")

    if gan:
        p90 = pct([o["mae"] for o in gan], 90)
        print(f"\nStop actual: {stop_actual:.2f} R")
        print(f"El p90 del MAE de las GANADORAS es {p90:.2f} R.")
        if p90 < stop_actual * 0.7:
            ahorro = stop_actual - p90
            print(f"  -> LECTURA: el 90% de las que acabaron ganando nunca pasó de "
                  f"{p90:.2f} R en contra.\n"
                  f"     Un stop en {p90:.2f} R habría matado como mucho 1 de cada 10 "
                  f"ganadoras\n"
                  f"     y liberado {ahorro:.2f} R de riesgo en TODAS las perdedoras.\n"
                  f"     Compruébalo con --barrido antes de tocarlo.")
        elif p90 > stop_actual * 0.95:
            print("  -> LECTURA: las ganadoras rozan el stop actual. Apretarlo mata "
                  "justo las que pagan.\n     Si algo sobra, no es el riesgo.")
        else:
            print("  -> LECTURA: el stop está en una zona razonable. El problema, si "
                  "lo hay, está en la salida.")

    print(f"\n{'=' * 72}\n2 · MFE — ¿ESTÁ EL OBJETIVO DONDE DEBE?\n{'=' * 72}")
    print("Cuánto llegó a ir A FAVOR. Lo importante son las PERDEDORAS:\n"
          "si llegaron lejos antes de morir, el problema es de salida, no de entrada.\n")
    print(f"{'grupo':<22} {'n':>4} {'mediana':>9} {'p75':>8} {'p90':>8} {'máx':>8}")
    for nombre, g in (("ganadoras", gan), ("PERDEDORAS", per), ("todas", ops)):
        xs = [o["mfe"] for o in g]
        if not xs:
            continue
        print(f"{nombre:<22} {len(xs):>4} {st.median(xs):>9.2f} {pct(xs,75):>8.2f} "
              f"{pct(xs,90):>8.2f} {max(xs):>8.2f}")

    if per:
        mfe_per = [o["mfe"] for o in per]
        med = st.median(mfe_per)
        n_1r = sum(1 for x in mfe_per if x >= 1.0)
        print(f"\nObjetivo actual: {tp_actual:.2f} R")
        print(f"Las perdedoras llegaron a {med:.2f} R a favor (mediana). "
              f"{n_1r} de {len(per)} ({n_1r/len(per)*100:.0f}%) pasaron de +1.0 R "
              f"y AUN ASÍ perdieron.")
        if n_1r / len(per) > 0.35:
            print("  -> LECTURA: más de un tercio de las perdedoras estuvo en beneficio "
                  "claro.\n     Eso NO se arregla filtrando entradas. Es salida: "
                  "objetivo más cerca,\n     toma parcial, o trailing. Mídelo con "
                  "--barrido, no lo supongas.")
        else:
            print("  -> LECTURA: las perdedoras apenas fueron a favor. Es un problema "
                  "de SEÑAL,\n     no de salida: el precio no hizo lo que la señal decía.")

    # ── el reloj ──
    barras = [o["bar_mfe"] for o in ops if o["mfe"] > 0.2]
    if barras:
        print(f"\n{'=' * 72}\n3 · EL RELOJ — ¿CUÁNDO SE AGOTA EL MOVIMIENTO?\n{'=' * 72}")
        print(f"Barra en la que se alcanza el máximo favorable "
              f"(solo las que fueron a favor >0.2 R):")
        print(f"  mediana {st.median(barras):.0f} · p75 {pct(barras,75):.0f} · "
              f"p90 {pct(barras,90):.0f} · máx {max(barras):.0f}")
        print("  -> Si el p90 está muy por debajo de tu límite de barras, las últimas "
              "solo\n     sirven para devolver lo ganado. Si está pegado al límite, "
              "estás cortando\n     movimientos vivos.")

    # ── las cuatro formas de fallar ──
    print(f"\n{'=' * 72}\n4 · POR QUÉ FALLA CADA PERDEDORA\n{'=' * 72}")
    nunca = sum(1 for o in per if o["mfe"] < 0.25)
    devuelta = sum(1 for o in per if o["mfe"] >= 1.0)
    cerca = sum(1 for o in per if 1.0 > o["mfe"] >= tp_actual * 0.6)
    parada = len(per) - nunca - devuelta - cerca
    total = max(len(per), 1)
    print(f"  nunca fue a favor (<0.25 R)      {nunca:3}  {nunca/total*100:4.0f}%   "
          f"-> la señal no valía")
    print(f"  llegó a +1 R o más y murió       {devuelta:3}  {devuelta/total*100:4.0f}%   "
          f"-> problema de SALIDA")
    print(f"  se quedó cerca del objetivo      {cerca:3}  {cerca/total*100:4.0f}%   "
          f"-> objetivo demasiado lejos")
    print(f"  se quedó parada                  {parada:3}  {parada/total*100:4.0f}%   "
          f"-> sobra reloj o falta filtro de amplitud")
    print("\nEste reparto es la respuesta a '¿mejoro la entrada o la salida?'.")


def barrido(ops: list[dict], stop_actual: float, tp_actual: float,
            max_barras: int | None) -> None:
    stops = [0.4, 0.5, 0.6, 0.7, 0.8, 1.0, 1.2, 1.5, 2.0]
    tps = [1.0, 1.5, 2.0, 2.5, 3.0]
    k = len(stops) * len(tps)
    umbral = z_bonferroni(k)

    print(f"\n{'=' * 72}")
    print(f"5 · BARRIDO — {k} combinaciones sobre las MISMAS {len(ops)} operaciones")
    print(f"{'=' * 72}")
    print(f"Esto es optimización EN MUESTRA. Probar {k} combinaciones y quedarte con")
    print(f"la mejor es hacer {k} apuestas: por azar alguna sale bien. El |t| exigido")
    print(f"sube de 2.00 a {umbral:.2f} y aun así el número que veas está sesgado al alza.")
    print(f"Úsalo para DESCARTAR (¿hay ALGUNA combinación positiva?), no para elegir.\n")
    print(f"{'stop':>6} " + " ".join(f"{t:>9.1f}R" for t in tps))

    mejor = None
    for s in stops:
        fila = [f"{s:>6.1f}"]
        for t in tps:
            rs = [r for r in (simular(o, s, t, max_barras) for o in ops) if r is not None]
            if not rs:
                fila.append(f"{'—':>10}")
                continue
            m = st.fmean(rs)
            fila.append(f"{m:>+10.3f}")
            sd = st.stdev(rs) if len(rs) > 1 else 0.0
            tt = m / (sd / math.sqrt(len(rs))) if sd > 0 else 0.0
            if mejor is None or m > mejor[0]:
                mejor = (m, s, t, tt, len(rs))
        print(" ".join(fila))

    actual = [r for r in (simular(o, stop_actual, tp_actual, max_barras) for o in ops)
              if r is not None]
    if actual:
        print(f"\nConfiguración ACTUAL ({stop_actual:.1f}R / {tp_actual:.1f}R): "
              f"{st.fmean(actual):+.3f} R por operación")
    if mejor:
        m, s, t, tt, n = mejor
        print(f"Mejor del barrido: stop {s:.1f}R / objetivo {t:.1f}R -> "
              f"{m:+.3f} R  (t={tt:+.2f}, n={n})")
        if m <= 0:
            print("\n  -> CONCLUSIÓN FUERTE: NINGUNA combinación de stop y objetivo")
            print("     convierte este conjunto en positivo. El problema no es dónde")
            print("     pones las salidas: es la señal. Ajustar stops aquí es mover")
            print("     los muebles.")
        elif abs(tt) < umbral:
            print(f"\n  -> El mejor no llega al |t| {umbral:.2f} que exige haber probado")
            print(f"     {k} combinaciones. Es un candidato, no un resultado. Vuelve a")
            print("     comprobarlo en el periodo SIGUIENTE, con operaciones que no")
            print("     hayan participado en este barrido.")
        else:
            print(f"\n  -> Pasa el umbral corregido. Sigue siendo EN MUESTRA: la prueba")
            print("     de verdad es el periodo siguiente.")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("csv")
    ap.add_argument("--tf", default="15m", help="timeframe de las velas (por defecto 15m)")
    ap.add_argument("--horas", type=float, default=6.0,
                    help="seguimiento máximo si la operación no tiene cierre")
    ap.add_argument("--stop", type=float, default=1.0, help="stop actual en R")
    ap.add_argument("--tp", type=float, default=2.0, help="objetivo actual en R")
    ap.add_argument("--barras", type=int, default=0, help="límite de barras (0 = sin límite)")
    ap.add_argument("--barrido", action="store_true",
                    help="barrido de stop x objetivo. Optimización en muestra: léelo antes.")
    a = ap.parse_args()

    ops = leer(a.csv)
    if not ops:
        print("Sin operaciones legibles. Se esperan columnas symbol/lado/entrada/sl "
              "y una fecha de apertura.")
        return 1

    print(f"{len(ops)} operaciones · velas de {a.tf} (o las del CSV) · "
          f"descargando de BingX...")
    ok = []
    for i, o in enumerate(ops, 1):
        tf = o.get("tf") or a.tf
        paso = MS.get(tf, 900_000)
        fin = o["t1"] or (o["t0"] + int(a.horas * 3_600_000))
        v = velas(o["symbol"], o["t0"] + paso, fin + paso, tf)
        time.sleep(PACING)
        if not v:
            continue
        o["_v"] = v
        riesgo = abs(o["entrada"] - o["sl"])
        if riesgo <= 0:
            continue
        mae = mfe = 0.0
        bmae = bmfe = 0
        for j, k in enumerate(v):
            adv = ((o["entrada"] - k["l"]) if o["largo"] else (k["h"] - o["entrada"])) / riesgo
            fav = ((k["h"] - o["entrada"]) if o["largo"] else (o["entrada"] - k["l"])) / riesgo
            if adv > mae:
                mae, bmae = adv, j + 1
            if fav > mfe:
                mfe, bmfe = fav, j + 1
        o.update({"mae": mae, "mfe": mfe, "bar_mae": bmae, "bar_mfe": bmfe})

        # SI EL CSV NO TRAE RESULTADO, se resuelve aquí con el stop y el
        # objetivo de la propia señal. Es el caso de senales_todas.csv: en
        # MODE=SIGNAL no hay r_real porque no se abrió nada, y sin separar
        # ganadoras de perdedoras el informe pierde su lectura principal
        # (el MAE de las que acabaron ganando). Queda marcado como
        # SIMULADO: no lleva deslizamiento y es optimista.
        if o["r_real"] is None:
            o["simulada"] = True
            tp_r = (abs(o["tp"] - o["entrada"]) / riesgo) if o["tp"] else 2.0
            o["r_real"] = simular(o, 1.0, tp_r, a.barras or None)
            if o["r_real"] is None:
                continue
        ok.append(o)
        if i % 20 == 0:
            print(f"  ... {i}/{len(ops)}", flush=True)

    if len(ok) < 10:
        print(f"\nSolo {len(ok)} operaciones con velas. Sin muestra para excursión.")
        return 0
    print(f"{len(ok)} con velas · {len(ops) - len(ok)} sin datos (símbolo retirado "
          f"o fuera del histórico)")
    n_sim = sum(1 for o in ok if o.get("simulada"))
    if n_sim:
        print(f"\n⚠️  {n_sim} de {len(ok)} SIN resultado real en el CSV: resueltas "
              f"contra las velas\n    con su propio stop y objetivo. Sin "
              f"deslizamiento y suponiendo que la\n    limitada se habría "
              f"llenado al precio de la señal — o sea, OPTIMISTA.")

    informe(ok, a.stop, a.tp)
    if a.barrido:
        barrido(ok, a.stop, a.tp, a.barras or None)

    print(f"\n{'=' * 72}\nADVERTENCIA DE MUESTRA\n{'=' * 72}")
    if len(ok) < 30:
        print(f"{len(ok)} operaciones. Los percentiles p75 y p90 con esta n se mueven")
        print("mucho al añadir una sola operación. Míralo como un dibujo, no como una")
        print("medida.")
    else:
        print(f"{len(ok)} operaciones. Suficiente para ver la FORMA de la distribución,")
        print("no para afinar un umbral al segundo decimal.")
    print("\nY la regla que no cambia: un cambio de stop encontrado aquí hay que")
    print("comprobarlo en el periodo siguiente. Si no se confirma, era ruido.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
