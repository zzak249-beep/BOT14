"""
estacionalidad.py — ¿tus señales funcionan mejor a ciertas horas o días?

    python estacionalidad.py senales_todas.csv
    python estacionalidad.py operaciones_wavelet.csv --horas 24

═══════════════════════════════════════════════════════════════════════
POR QUÉ NO TROCEA POR HORA, AUNQUE ES LO QUE TODO EL MUNDO HACE
═══════════════════════════════════════════════════════════════════════
Con 1.9 señales al día, repartirlas en 24 cubos deja 0.079 por cubo y
día. Para juntar 31 operaciones EN CADA HORA harían falta 392 días. Y 31
solo detecta ventajas de 0.50 R por operación, que no existen.

    por hora           24 cubos ->  392 días para 31 ops por cubo
    por sesión de 6h    4 cubos ->   65 días
    por día de semana   7 cubos ->  114 días
    laborable/finde     2 cubos ->   33 días

Y hay un segundo problema, peor: probar 24 horas x 2 direcciones son 48
hipótesis. Con 48 pruebas, algo sale "significativo" por puro azar. El
umbral sube de |t|>=1.96 a |t|>=3.28, y con 31 operaciones por cubo eso
solo detectaría 0.59 R por operación.

Por eso aquí NO hay barrido horario. Hay cuatro troceados
PRE-REGISTRADOS, elegidos porque tienen un mecanismo detrás y no porque
salgan bien:

  1. SESIÓN (4 cubos de 6 h). Asia, Europa, EEUU, madrugada. La
     liquidez y el spread cambian de verdad entre ellas.
  2. VENTANA DE FUNDING (2 cubos). Los pagos son a las 00, 08 y 16 UTC.
     Alrededor hay ajuste de posiciones documentado, y es mecánico: no
     depende de que el mercado "se comporte".
  3. DÍA DE LA SEMANA (7 cubos). El más caro de medir; se incluye
     porque el fin de semana la liquidez cae y eso sí es estructural.
  4. LABORABLE vs FIN DE SEMANA (2 cubos). La versión barata del
     anterior, y la primera que va a tener muestra.

Cada uno se corrige por el número de cubos que se está mirando, y el
script DICE cuántos días faltan cuando no llega.

═══════════════════════════════════════════════════════════════════════
LO QUE NO PUEDE
═══════════════════════════════════════════════════════════════════════
- Si le pasas --horas, hace el barrido horario. Está ahí porque vas a
  quererlo ver, pero sale con el umbral de 48 hipótesis y con el aviso
  de cuántos días faltan. No es una recomendación.
- Las señales no ejecutadas se simulan al precio de cierre, sin
  deslizamiento. Optimista para ellas.
- Encontrar un cubo bueno NO es una estrategia. Es una hipótesis que
  hay que volver a comprobar en el periodo siguiente, con datos que no
  se hayan usado para encontrarla.
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
COSTE_PCT = 0.10
HORAS_SEGUIMIENTO = 24.0
PACING = 0.12

SESIONES = {0: "madrugada 00-06", 1: "Asia 06-12", 2: "Europa 12-18", 3: "EEUU 18-24"}
DIAS = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]


def z_bonferroni(k: int) -> float:
    """|t| exigido para alpha=0.05 repartido entre k cubos."""
    p = 0.05 / (2.0 * max(k, 1))
    t = math.sqrt(-2.0 * math.log(p))
    return t - ((2.515517 + 0.802853 * t + 0.010328 * t * t) /
                (1 + 1.432788 * t + 0.189269 * t * t + 0.001308 * t ** 3))


def velas(symbol: str, desde_ms: int, barras: int, interval: str):
    try:
        r = requests.get(f"{BASE}/openApi/swap/v3/quote/klines", timeout=20,
                         params={"symbol": symbol, "interval": interval,
                                 "startTime": desde_ms, "limit": min(barras, 1000)})
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
    return [v for v in out if v["t"] >= desde_ms]


def resolver(s: dict, horas: float, coste_pct: float):
    """R neta. Si se tocan stop y objetivo en la misma vela, manda el STOP."""
    interval = s.get("timeframe") or "15m"
    barra = MS.get(interval, 900_000)
    n = max(int(horas * 3_600_000 / barra), 2)
    v = velas(s["symbol"], s["ts"] + barra, n, interval)
    if not v:
        return None
    largo = s["side"] == "LONG"
    riesgo = abs(s["price"] - s["sl"])
    if riesgo <= 0:
        return None
    coste_r = (coste_pct / 100.0 * s["price"]) / riesgo
    for k in v:
        if (k["l"] <= s["sl"]) if largo else (k["h"] >= s["sl"]):
            return -1.0 - coste_r
        if (k["h"] >= s["tp"]) if largo else (k["l"] <= s["tp"]):
            bruto = (s["tp"] - s["price"]) if largo else (s["price"] - s["tp"])
            return bruto / riesgo - coste_r
    ult = v[-1]["c"]
    bruto = (ult - s["price"]) if largo else (s["price"] - ult)
    return bruto / riesgo - coste_r


def bloque(titulo: str, grupos: dict, dias_datos: float, tasa: float) -> None:
    k = len(grupos)
    umbral = z_bonferroni(k)
    print(f"\n{'=' * 72}\n{titulo}   ({k} cubos · |t| exigido {umbral:.2f})\n{'=' * 72}")
    print(f"{'cubo':<20} {'n':>5} {'media R':>9} {'t':>7}   veredicto")

    algo = False
    for nombre in sorted(grupos):
        rs = grupos[nombre]
        n = len(rs)
        if n < 5:
            print(f"{nombre:<20} {n:>5} {'—':>9} {'—':>7}   sin muestra")
            continue
        m = st.fmean(rs)
        sd = st.pstdev(rs) if n > 1 else 1.0
        t = m * math.sqrt(n) / sd if sd > 1e-9 else 0.0
        # Ventaja MÍNIMA detectable con esta n y este umbral. Es lo que
        # convierte un "no significativo" en información: dice si el
        # cubo está vacío de señal o solo vacío de datos.
        detectable = umbral * sd / math.sqrt(n)
        if abs(t) >= umbral:
            ver = "DESTACA" if m > 0 else "DESTACA en contra"
            algo = True
        else:
            ver = f"nada (detectaría ≥{detectable:.2f} R)"
        print(f"{nombre:<20} {n:>5} {m:>+9.3f} {t:>+7.2f}   {ver}")

    # Cuántos días faltan para que el cubo medio llegue a 31 y a 87.
    por_cubo_dia = tasa / k if k else 0
    if por_cubo_dia > 0:
        n_medio = st.fmean([len(v) for v in grupos.values()]) if grupos else 0
        for meta in (31, 87):
            if n_medio < meta:
                faltan = (meta - n_medio) / por_cubo_dia
                print(f"  · para {meta} ops por cubo faltan ~{faltan:.0f} días")
    if not algo:
        print("  · ningún cubo destaca. Con esta muestra eso significa "
              "'todavía no se sabe', no 'no hay efecto'.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv")
    ap.add_argument("--coste", type=float, default=COSTE_PCT)
    ap.add_argument("--seguimiento", type=float, default=HORAS_SEGUIMIENTO)
    ap.add_argument("--horas", action="store_true",
                    help="añade el barrido por hora. Sale con el umbral de "
                         "48 hipótesis. No es una recomendación.")
    a = ap.parse_args()

    filas = []
    for enc in ("utf-8-sig", "utf-8"):
        try:
            with open(a.csv, newline="", encoding=enc) as f:
                for x in csv.DictReader(f):
                    try:
                        ts = int(float(x.get("ts_señal") or x.get("ts") or 0))
                        if ts <= 0:
                            continue
                        filas.append({
                            "symbol": x["symbol"],
                            "side": (x.get("side") or x.get("lado") or "").upper(),
                            "ts": ts,
                            "timeframe": (x.get("timeframe") or "15m").strip(),
                            "price": float(x.get("price") or x.get("entrada_esperada")),
                            "sl": float(x["sl"]), "tp": float(x["tp"]),
                            "r_guardada": float(x["r_real"]) if x.get("r_real") else None,
                        })
                    except (KeyError, ValueError, TypeError):
                        continue
            break
        except UnicodeDecodeError:
            filas = []
    if not filas:
        print("Sin señales legibles. Se esperan columnas ts_señal/symbol/side/price/sl/tp.")
        return 1

    t0 = min(x["ts"] for x in filas) / 1000
    t1 = max(x["ts"] for x in filas) / 1000
    dias = max((t1 - t0) / 86400.0, 0.5)
    tasa = len(filas) / dias

    print(f"{len(filas)} señales · {dias:.1f} días · {tasa:.2f} señales/día")
    print(f"Coste {a.coste}% · seguimiento {a.seguimiento:.0f} h\n")

    pendientes = [x for x in filas if x["r_guardada"] is None]
    if pendientes:
        print(f"Resolviendo {len(pendientes)} contra velas de BingX...")
    for i, s in enumerate(filas, 1):
        s["r"] = s["r_guardada"] if s["r_guardada"] is not None else \
            resolver(s, a.seguimiento, a.coste)
        if s["r_guardada"] is None:
            time.sleep(PACING)
            if i % 25 == 0:
                print(f"  ... {i}/{len(filas)}", flush=True)

    con = [x for x in filas if x["r"] is not None]
    if len(con) < 10:
        print(f"\nSolo {len(con)} operaciones resueltas. No hay nada que trocear.")
        return 0
    print(f"\n{len(con)} operaciones resueltas · media global "
          f"{st.fmean([x['r'] for x in con]):+.3f} R")

    for x in con:
        d = dt.datetime.fromtimestamp(x["ts"] / 1000, dt.timezone.utc)
        x["hora"] = d.hour
        x["dow"] = d.weekday()

    # 1 · Sesión
    g = {}
    for x in con:
        g.setdefault(SESIONES[x["hora"] // 6], []).append(x["r"])
    bloque("1 · POR SESIÓN (liquidez y spread cambian de verdad entre ellas)",
           g, dias, tasa)

    # 2 · Ventana de funding: los pagos son a las 00, 08 y 16 UTC.
    g = {}
    for x in con:
        cerca = min((x["hora"] - h) % 24 for h in (0, 8, 16)) <= 1 or \
                min((h - x["hora"]) % 24 for h in (0, 8, 16)) <= 1
        g.setdefault("±1h del funding" if cerca else "lejos del funding", []).append(x["r"])
    bloque("2 · VENTANA DE FUNDING (00, 08 y 16 UTC — mecánico, no conductual)",
           g, dias, tasa)

    # 3 · Laborable vs fin de semana
    g = {}
    for x in con:
        g.setdefault("fin de semana" if x["dow"] >= 5 else "laborable", []).append(x["r"])
    bloque("3 · LABORABLE vs FIN DE SEMANA (la liquidez cae, y eso es estructural)",
           g, dias, tasa)

    # 4 · Día de la semana
    g = {}
    for x in con:
        g.setdefault(f"{x['dow']} {DIAS[x['dow']]}", []).append(x["r"])
    bloque("4 · POR DÍA DE LA SEMANA (el más caro de medir)", g, dias, tasa)

    if a.horas:
        g = {}
        for x in con:
            g.setdefault(f"{x['hora']:02d}:00 UTC", []).append(x["r"])
        bloque("5 · POR HORA — 24 cubos, umbral altísimo. MIRAR, NO DECIDIR",
               g, dias, tasa)

    print(f"\n{'=' * 72}")
    print("CÓMO LEER ESTO")
    print("=" * 72)
    print("Un cubo que DESTACA no es una estrategia: es una hipótesis.")
    print("Para que valga hay que volver a comprobarla en el periodo")
    print("SIGUIENTE, con datos que no se hayan usado para encontrarla.")
    print("Si no se confirma, era ruido — y con muchos cubos siempre hay")
    print("alguno que parece bueno.")
    print("\nY lo contrario también: 'nada' con la ventaja detectable al")
    print("lado no dice que el cubo sea inútil. Dice que con esta muestra")
    print("no se vería ni un efecto de ese tamaño.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
