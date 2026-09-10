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
BingX, endpoint público de klines. Sin API key, sin cuenta.

ANTES ERA BINANCE, y estaba mal por dos motivos:

  1. Binance devuelve HTTP 451 desde los servidores de Railway
     ("Service unavailable from a restricted location"), así que el
     backtest no se podía correr donde vive el bot.

  2. Y es un fallo de método aunque funcionara: tú operas en BingX.
     Medir con precios de Binance mete basis y, sobre todo, HUECOS —
     los perpetuos que solo cotizan en BingX se descartaban en silencio
     como "sin datos", y son justo los de cola larga que el universo
     ALL incluye. Eso sesga el resultado hacia las monedas grandes.

El precio a pagar: BingX da menos histórico que Binance. Si pides 180
días y solo devuelve 60, el script lo dice en vez de callarse.

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

BINGX_KLINES = "https://open-api.bingx.com/openApi/swap/v3/quote/klines"
# BingX sirve como mucho 1440 velas por llamada.
MAX_POR_LLAMADA = 1440
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


def _fila(k) -> dict | None:
    """BingX devuelve dicts o listas según versión; se aceptan las dos.
    OJO: en la respuesta real 'close' va ANTES que 'high', así que se
    parsea por NOMBRE y nunca por posición."""
    try:
        if isinstance(k, dict):
            return {"time": int(k.get("time", k.get("open_time", 0))),
                    "open": float(k["open"]), "high": float(k["high"]),
                    "low": float(k["low"]), "close": float(k["close"]),
                    "volume": float(k.get("volume", 0) or 0)}
        return {"time": int(k[0]), "open": float(k[1]), "high": float(k[2]),
                "low": float(k[3]), "close": float(k[4]),
                "volume": float(k[5]) if len(k) > 5 else 0.0}
    except (KeyError, IndexError, TypeError, ValueError):
        return None


async def download(client: httpx.AsyncClient, symbol: str, interval: str, days: int) -> list[dict]:
    """
    Descarga paginando hacia atrás desde BingX, el MISMO exchange donde
    opera el bot. Sin claves: es el endpoint público.

    Se para sola si BingX deja de devolver velas más antiguas, y avisa
    de cuántos días ha conseguido de verdad — callarlo haría comparar
    periodos distintos entre símbolos sin que se note.
    """
    paso = MS.get(interval, 300_000)
    total = int(days * 24 * 60 * 60 * 1000 / paso)
    fin = int(time.time() * 1000)
    velas: list[dict] = []
    vacios = 0

    while len(velas) < total:
        faltan = min(MAX_POR_LLAMADA, total - len(velas))
        inicio = fin - faltan * paso
        try:
            r = await client.get(
                BINGX_KLINES,
                params={"symbol": symbol, "interval": interval,
                        "startTime": inicio, "endTime": fin, "limit": faltan},
                timeout=30,
            )
        except Exception as exc:  # noqa: BLE001
            if not velas:
                raise RuntimeError(f"{symbol}: {exc}") from exc
            break
        if r.status_code != 200:
            if not velas:
                raise RuntimeError(f"{symbol}: HTTP {r.status_code} {r.text[:120]}")
            break
        cuerpo = r.json()
        if isinstance(cuerpo, dict) and str(cuerpo.get("code", 0)) not in ("0", "None"):
            if not velas:
                raise RuntimeError(f"{symbol}: code={cuerpo.get('code')} {cuerpo.get('msg')}")
            break
        datos = cuerpo.get("data") if isinstance(cuerpo, dict) else cuerpo
        lote = [x for x in (_fila(k) for k in (datos or [])) if x]
        if not lote:
            vacios += 1
            if vacios >= 2:      # BingX ya no tiene más histórico
                break
            fin = inicio - 1
            await asyncio.sleep(0.2)
            continue
        vacios = 0
        lote.sort(key=lambda v: v["time"])
        # Sin deduplicar, un solape entre páginas mete la misma vela dos
        # veces y el motor la evalúa como si fueran dos barras distintas.
        conocidos = {v["time"] for v in velas}
        velas = [x for x in lote if x["time"] not in conocidos] + velas
        fin = lote[0]["time"] - 1
        await asyncio.sleep(0.2)   # cortesía con el endpoint público

    velas.sort(key=lambda v: v["time"])
    if velas:
        dias_reales = (velas[-1]["time"] - velas[0]["time"]) / 86_400_000
        if dias_reales < days * 0.8:
            print(f"  {symbol}: BingX solo dio {dias_reales:.0f} días de los "
                  f"{days} pedidos ({len(velas)} velas)")
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
            elif strategy.exit_cross(velas[max(0, i - ventana): i + 1], abierta.side):
                precio = vela["close"]
                bruto = (precio - abierta.entry) if largo else (abierta.entry - precio)
                abierta.exit = precio
                abierta.motivo = "cruce"
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
    # Con un solo símbolo, Bonferroni da 1.96 — MENOS que el umbral
    # clásico de 2.0. Nunca puede ser el listón más bajo de los tres.
    z = max(z, 3.0)

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

    print(f"Descargando {days} días en {interval} para {len(symbols)} símbolo(s) "
          f"desde BingX (el mismo exchange donde opera el bot)...")
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
