"""
Backtest local sobre histórico descargado del exchange.

    python backtest.py ZEC-USDT 5m 240
    python backtest.py ZEC-USDT 15m 240 --mensual
    python backtest.py ZEC-USDT,PUMP-USDT,LDO-USDT 30m 240

POR QUÉ EXISTE
El plan gratuito de TradingView limita las barras del histórico: en 5m
llegas a unas tres semanas, en 15m a un par de meses, en 30m a varios.
Eso hace IMPOSIBLE comparar timeframes de forma justa — cada uno mira
un periodo distinto, y entonces no estás midiendo el timeframe, estás
midiendo qué meses te tocaron. Aquí se descargan los días que pidas,
iguales para todos los timeframes.

LA VENTAJA QUE NO TIENE TRADINGVIEW
Usa el MISMO strategy.py que ejecuta el bot. Los backtests de Pine
miden una estrategia parecida pero no idéntica — sin el filtro de
coste, sin REQUIRE_ST_BULL, sin el tope de riesgo. Aquí no hay esa
distancia: lo que mides es exactamente lo que opera.

DE DÓNDE SALEN LOS DATOS
Binance Futures, endpoint público de klines. Sin API key, sin cuenta,
sin límite práctico de histórico. La mayoría de perpetuos de BingX
cotizan también allí con el mismo nombre sin guion (ZEC-USDT →
ZECUSDT). Si un símbolo no existe en Binance, se avisa y se salta.

LO QUE ESTO NO ARREGLA
El deslizamiento sigue siendo una estimación, y el backtest supone que
entras al cierre de la vela de señal. En pares finos eso es optimista.
Los resultados de aquí son un techo, no una promesa.
"""
from __future__ import annotations

import asyncio
import sys
import time
from dataclasses import dataclass, field

import httpx

import config
import strategy

BINANCE_KLINES = "https://fapi.binance.com/fapi/v1/klines"
MS = {"1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
      "30m": 1_800_000, "1h": 3_600_000, "4h": 14_400_000}


@dataclass
class Trade:
    symbol: str
    entry_ts: int
    entry: float
    sl: float
    tp: float = 0.0
    side: str = "BUY"
    exit_ts: int = 0
    exit: float = 0.0
    r: float = 0.0
    motivo: str = ""


@dataclass
class Result:
    symbol: str
    trades: list[Trade] = field(default_factory=list)
    descartes: dict[str, int] = field(default_factory=dict)


async def download(client: httpx.AsyncClient, symbol: str, interval: str, days: int) -> list[dict]:
    """Descarga paginando hacia atrás. Binance da 1500 velas por llamada."""
    binance_sym = symbol.replace("-", "").upper()
    paso = MS.get(interval, 300_000)
    total = int(days * 24 * 60 * 60 * 1000 / paso)
    fin = int(time.time() * 1000)
    velas: list[dict] = []

    while len(velas) < total:
        faltan = min(1500, total - len(velas))
        inicio = fin - faltan * paso
        r = await client.get(
            BINANCE_KLINES,
            params={"symbol": binance_sym, "interval": interval,
                    "startTime": inicio, "endTime": fin, "limit": faltan},
            timeout=30,
        )
        if r.status_code != 200:
            if not velas:
                raise RuntimeError(f"{binance_sym}: {r.status_code} {r.text[:120]}")
            break
        datos = r.json()
        if not datos:
            break
        lote = [{"time": int(k[0]), "open": float(k[1]), "high": float(k[2]),
                 "low": float(k[3]), "close": float(k[4]), "volume": float(k[5])}
                for k in datos]
        velas = lote + velas
        fin = lote[0]["time"] - 1
        await asyncio.sleep(0.15)  # cortesía con el endpoint público

    velas.sort(key=lambda v: v["time"])
    return velas


def simulate(symbol: str, velas: list[dict]) -> Result:
    """
    Recorre el histórico vela a vela llamando a strategy.evaluate() con
    la ventana visible en cada momento — igual que hace el bot en vivo.
    Nunca ve el futuro: esa es toda la diferencia entre un backtest y
    un dibujo bonito.
    """
    res = Result(symbol)
    ventana = 400
    abierta: Trade | None = None
    i = ventana

    while i < len(velas):
        vela = velas[i]

        if abierta:
            riesgo0 = abs(abierta.entry - abierta.sl)
            largo = abierta.side == "BUY"
            toca_sl = vela["low"] <= abierta.sl if largo else vela["high"] >= abierta.sl
            toca_tp = vela["high"] >= abierta.tp if largo else vela["low"] <= abierta.tp
            venc = (vela["time"] - abierta.entry_ts) / 60000 >= config.MAX_TRADE_MINUTES

            # Si en la misma vela se tocan SL y TP, se supone el PEOR
            # caso. Suponer el mejor es la forma más común de inflar un
            # backtest sin darse cuenta.
            if toca_sl:
                abierta.exit = abierta.sl
                abierta.motivo = "stop"
                abierta.r = -1.0 - config.COST_ROUNDTRIP_PCT / (riesgo0 / abierta.entry * 100.0)
            elif toca_tp:
                abierta.exit = abierta.tp
                abierta.motivo = "objetivo"
                bruto = (abierta.tp - abierta.entry) if largo else (abierta.entry - abierta.tp)
                abierta.r = bruto / riesgo0 - config.COST_ROUNDTRIP_PCT / (riesgo0 / abierta.entry * 100.0)
            elif venc:
                precio = vela["close"]
                bruto = (precio - abierta.entry) if largo else (abierta.entry - precio)
                a_favor = bruto > 0
                if config.TIME_EXIT_ONLY_LOSING and a_favor:
                    i += 1
                    continue
                abierta.exit = precio
                abierta.motivo = "tiempo"
                abierta.r = bruto / riesgo0 - config.COST_ROUNDTRIP_PCT / (riesgo0 / abierta.entry * 100.0)
            else:
                i += 1
                continue

            abierta.exit_ts = vela["time"]
            res.trades.append(abierta)
            abierta = None
            i += 1
            continue

        sig, motivo = strategy.evaluate(symbol, velas[max(0, i - ventana) : i + 1])
        if sig is None:
            clave = motivo.split("(")[0].strip()
            res.descartes[clave] = res.descartes.get(clave, 0) + 1
        else:
            abierta = Trade(symbol, vela["time"], sig.entry, sig.sl, sig.tp, sig.side)
        i += 1

    return res


def report(res: Result, mensual: bool = False) -> str:
    t = res.trades
    if not t:
        top = sorted(res.descartes.items(), key=lambda x: -x[1])[:3]
        return (f"\n{res.symbol}: SIN OPERACIONES\n  " +
                " · ".join(f"{k}: {v}" for k, v in top))

    ganadoras = [x for x in t if x.r > 0]
    perdedoras = [x for x in t if x.r <= 0]
    suma_g = sum(x.r for x in ganadoras)
    suma_p = abs(sum(x.r for x in perdedoras))
    pf = suma_g / suma_p if suma_p > 0 else float("inf")
    exp = sum(x.r for x in t) / len(t)

    # Drawdown en R sobre la curva acumulada.
    acum = 0.0
    pico = 0.0
    dd = 0.0
    for x in t:
        acum += x.r
        pico = max(pico, acum)
        dd = min(dd, acum - pico)

    out = [
        f"\n{'='*58}",
        f"{res.symbol}",
        f"{'='*58}",
        f"Operaciones      {len(t)}",
        f"Acierto          {len(ganadoras)/len(t)*100:.1f}%",
        f"Factor ganancias {pf:.3f}",
        f"Expectativa      {exp:+.3f} R por operación",
        f"Total            {sum(x.r for x in t):+.1f} R",
        f"Peor racha       {dd:.1f} R",
    ]

    if mensual:
        import datetime as dt
        meses: dict[str, list[float]] = {}
        for x in t:
            k = dt.datetime.utcfromtimestamp(x.entry_ts / 1000).strftime("%Y-%m")
            meses.setdefault(k, []).append(x.r)
        out.append("\nPor mes (el reparto es lo que revela si depende del régimen):")
        for k in sorted(meses):
            rs = meses[k]
            marca = "✓" if sum(rs) > 0 else "✗"
            out.append(f"  {k}  {marca}  {len(rs):3d} ops  {sum(rs):+7.1f} R")

    return "\n".join(out)


def significancia(rs: list[float], n_simbolos: int) -> str:
    """
    ¿La expectativa observada es distinguible de cero, teniendo en
    cuenta cuántos símbolos se han probado?

    Probar 300 símbolos es hacer 300 apuestas: por puro azar, algunos
    van a salir bien. La literatura sobre data snooping (Harvey, Liu y
    Zhu) recomienda exigir t >= 3.0 en vez del 2.0 habitual cuando se
    ha buscado mucho. Y con un universo grande, ni siquiera 3.0 basta:
    aquí se calcula además el umbral de Bonferroni para el número de
    símbolos realmente probados.

    Un estudio sobre 447 anomalías publicadas encontró que el 85% no
    explicaba nada, y que el 93% de las que sí lo hacían no sobrevivían
    al umbral de t >= 3.
    """
    n = len(rs)
    if n < 10:
        return "Muestra demasiado corta para hablar de significancia."
    media = sum(rs) / n
    var = sum((r - media) ** 2 for r in rs) / (n - 1)
    sd = var ** 0.5
    if sd == 0:
        return "Sin dispersión: revisa los datos."
    t = media / (sd / (n ** 0.5))

    # Bonferroni aproximado: z necesario para alpha=0.05 repartido
    # entre los símbolos probados.
    import math
    alpha = 0.05 / max(1, n_simbolos)
    # aproximación de la inversa normal (Beasley-Springer-Moro simplificada)
    z = math.sqrt(2.0) * _erfinv(1 - alpha)

    veredicto = (
        "PASA incluso ajustando por multiplicidad" if abs(t) >= z else
        "pasa t>=3 (umbral de data snooping) pero NO Bonferroni" if abs(t) >= 3.0 else
        "pasa el t>=2 clásico, NO el t>=3 de data snooping" if abs(t) >= 2.0 else
        "no distinguible de cero"
    )
    return (
        f"t-estadístico: {t:+.2f}   (n={n})\n"
        f"  umbral clásico 2.00 · data snooping 3.00 · "
        f"Bonferroni {n_simbolos} símbolos: {z:.2f}\n"
        f"  -> {veredicto}"
    )


def _erfinv(y: float) -> float:
    """Inversa de la función error, aproximación suficiente aquí."""
    a = 0.147
    ln = __import__("math").log(1 - y * y)
    t1 = 2 / (__import__("math").pi * a) + ln / 2
    return (1 if y >= 0 else -1) * (((t1 * t1 - ln / a) ** 0.5) - t1) ** 0.5


async def main() -> int:
    if len(sys.argv) < 3:
        print(__doc__)
        return 1
    symbols = [s.strip().upper() for s in sys.argv[1].split(",")]
    interval = sys.argv[2]
    days = int(sys.argv[3]) if len(sys.argv) > 3 else 180
    mensual = "--mensual" in sys.argv

    print(f"Descargando {days} días en {interval} para {len(symbols)} símbolo(s)...")
    print(f"Filtros activos: coste {config.COST_ROUNDTRIP_PCT}% · "
          f"riesgo {config.MIN_RISK_PCT}-{config.MAX_RISK_PCT}%")

    total_r = 0.0
    total_ops = 0
    todas_las_r: list[float] = []
    async with httpx.AsyncClient() as client:
        for sym in symbols:
            try:
                velas = await download(client, sym, interval, days)
            except Exception as exc:  # noqa: BLE001
                print(f"\n{sym}: no se pudo descargar ({exc})")
                continue
            if len(velas) < 500:
                print(f"\n{sym}: solo {len(velas)} velas, insuficiente")
                continue
            res = simulate(sym, velas)
            print(report(res, mensual))
            total_r += sum(x.r for x in res.trades)
            total_ops += len(res.trades)
            todas_las_r.extend(x.r for x in res.trades)

    if total_ops:
        print(f"\n{'='*58}")
        print(f"AGREGADO: {total_ops} operaciones · {total_r:+.1f} R · "
              f"{total_r/total_ops:+.3f} R por operación")
        print(significancia(todas_las_r, len(symbols)))
        print()
        print("Con menos de 100 operaciones repartidas en varios meses,")
        print("esto sigue siendo una pista, no una conclusión.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
