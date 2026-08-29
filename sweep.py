"""
Barrido de umbrales con validación fuera de muestra.

    python sweep.py CATE-USDT,JIMOTHY-USDT,BTR-USDT 5m 120

RESPONDE A: "el bot no abre nunca, ¿qué aflojo?"

Y lo hace sin la trampa habitual. Probar veinte combinaciones y quedarse
con la que mejor sale es EXACTAMENTE lo que produce que el 90% de los
backtests fallen en real: con suficientes intentos, siempre hay una
combinación que parece buena por casualidad. Bastan tres pruebas para
sacar un resultado aparentemente significativo que no lo es.

Por eso aquí:
  · El histórico se parte en dos mitades: PRIMERA (donde se elige) y
    SEGUNDA (donde se comprueba, sin haberla mirado).
  · Se muestran las dos columnas siempre, juntas.
  · La única combinación que vale es la que funciona en LAS DOS. Si una
    brilla en la primera y se hunde en la segunda, eso NO es un
    hallazgo: es la firma exacta del sobreajuste, y se marca en rojo.
  · También se muestra cuántas operaciones genera cada combinación,
    porque una que da 3 operaciones con expectativa +0.8R no es mejor
    que otra con 60 y +0.15R — es solo más ruidosa.

LO QUE NO PUEDE HACER: inventar una ventaja que no existe. Si todas las
combinaciones salen negativas en la segunda mitad, la respuesta honesta
es que la estrategia no funciona en esos símbolos, no que hay que
seguir buscando umbrales.
"""
from __future__ import annotations

import asyncio
import importlib
import os
import sys

import httpx

import backtest
import config
import strategy


# Combinaciones a probar. Pocas y con sentido, no una rejilla enorme:
# cuantas más pruebas, más fácil encontrar un ganador por azar.
CLAVES = ("MIN_RISK_PCT", "TARGET_CROSS", "MAX_COST_IN_R")
COMBINACIONES = [(1.5, 2, 0.20), (1.0, 2, 0.20), (1.5, 1, 0.20), (2.0, 2, 0.25), (1.0, 1, 0.30), (2.5, 2, 0.15)]


def evaluar(velas_por_symbol: dict[str, list[dict]], mitad: str) -> tuple[float, int]:
    """Expectativa media en R y número de operaciones en esa mitad."""
    total_r = 0.0
    total_n = 0
    for sym, velas in velas_por_symbol.items():
        corte = len(velas) // 2
        trozo = velas[:corte] if mitad == "primera" else velas[corte:]
        if len(trozo) < 500:
            continue
        res = backtest.simulate(sym, trozo)
        total_r += sum(t.r for t in res.trades)
        total_n += len(res.trades)
    return (total_r / total_n if total_n else 0.0), total_n


async def main() -> int:
    if len(sys.argv) < 3:
        print(__doc__)
        return 1
    symbols = [s.strip().upper() for s in sys.argv[1].split(",")]
    interval = sys.argv[2]
    days = int(sys.argv[3]) if len(sys.argv) > 3 else 120

    print(f"Descargando {days} días en {interval}...")
    velas_por_symbol: dict[str, list[dict]] = {}
    async with httpx.AsyncClient() as client:
        for sym in symbols:
            try:
                v = await backtest.download(client, sym, interval, days)
                if len(v) >= 1000:
                    velas_por_symbol[sym] = v
                    print(f"  {sym}: {len(v)} velas")
                else:
                    print(f"  {sym}: solo {len(v)} velas, se salta")
            except Exception as exc:  # noqa: BLE001
                print(f"  {sym}: {exc}")

    if not velas_por_symbol:
        print("Sin datos.")
        return 1

    print(f"\n{CLAVES[0][:6]:>6} {CLAVES[1][:5]:>5} {CLAVES[2][:8]:>8} │ {'PRIMERA mitad':>22} │ {'SEGUNDA mitad':>22} │")
    print(f"{'':>6} {'':>5} {'':>8} │ {'(se elige aquí)':>22} │ {'(se comprueba aquí)':>22} │")
    print("─" * 78)

    filas = []
    for cover, er, stretch in COMBINACIONES:
        for clave, valor in zip(CLAVES, (cover, er, stretch)):
            os.environ[clave] = str(valor)
        importlib.reload(config)
        importlib.reload(strategy)
        importlib.reload(backtest)

        e1, n1 = evaluar(velas_por_symbol, "primera")
        e2, n2 = evaluar(velas_por_symbol, "segunda")

        if n1 < 10 or n2 < 10:
            marca = "pocas ops"
        elif e1 > 0 and e2 > 0:
            marca = "✓ AGUANTA"
        elif e1 > 0 and e2 <= 0:
            marca = "✗ sobreajuste"
        else:
            marca = "· negativa"

        filas.append((cover, er, stretch, e1, n1, e2, n2, marca))
        print(
            f"{cover:>6} {er:>5.2f} {stretch:>8.1f} │ "
            f"{e1:+7.3f} R  ({n1:>4} ops) │ "
            f"{e2:+7.3f} R  ({n2:>4} ops) │  {marca}"
        )

    print("─" * 78)
    aguantan = [f for f in filas if f[7] == "✓ AGUANTA"]
    if not aguantan:
        print("\nNINGUNA combinación aguanta las dos mitades.")
        print("La respuesta honesta no es seguir bajando umbrales: es que")
        print("en estos símbolos y este periodo la estrategia no tiene")
        print("ventaja. Aflojar solo produciría más operaciones perdedoras.")
    else:
        # Se elige la de MÁS OPERACIONES entre las que aguantan, no la de
        # mejor expectativa: con muestras pequeñas la mejor expectativa
        # suele ser la más afortunada.
        mejor = max(aguantan, key=lambda f: f[4] + f[6])
        print(f"\nLa que aguanta con más muestra: "
              + " ".join(f"{k}={v}" for k, v in zip(CLAVES, mejor[:3])))
        print(f"  primera {mejor[3]:+.3f} R ({mejor[4]} ops) · "
              f"segunda {mejor[5]:+.3f} R ({mejor[6]} ops)")
        print("\nAntes de aplicarlo: repítelo con OTROS símbolos. Si la misma")
        print("combinación aguanta en dos grupos distintos, ya no es suerte.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
