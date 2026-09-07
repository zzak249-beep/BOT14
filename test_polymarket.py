"""
test_polymarket.py — prueba de humo. EJECÚTALO EN RAILWAY, NO EN TU PC.

Desde España el dominio está bloqueado a nivel de ISP: en local verás un
timeout o un 403 y pensarás que el código está roto. Railway no está en
España.

Qué hace: llama a la API, vuelca los CAMPOS CRUDOS de tres mercados y
luego prueba el parseo completo. Lo que se verifica es si Gamma llama a
sus campos como supone polymarket.py (question / outcomes /
outcomePrices / liquidityNum / endDate).

Si "peldanos" sale 0, el fallo está ahí: pega el bloque MERCADO CRUDO.

Uso: cambia temporalmente el start command del servicio a
    python test_polymarket.py
"""
import asyncio
import json

import httpx

import polymarket as pm

GAMMA = "https://gamma-api.polymarket.com"


class Cfg:
    PM_ENABLED = True
    PM_BASE = "BTC"
    PM_MIN_LIQUIDITY = 5000.0
    PM_MIN_VOLUME = 20000.0
    PM_TIMEOUT = 15


def seccion(t):
    print("\n" + "=" * 68 + "\n" + t + "\n" + "=" * 68)


async def main():
    cfg = Cfg()

    seccion("1 · SALIDA A LA API")
    try:
        async with httpx.AsyncClient(timeout=15, headers=pm.UA) as c:
            r = await c.get(f"{GAMMA}/markets",
                            params={"closed": "false", "limit": 3,
                                    "order": "volumeNum", "ascending": "false"})
        print("HTTP", r.status_code)
        data = r.json()
        if isinstance(data, dict):
            print("Respuesta dict. Claves:", list(data.keys())[:12])
            data = data.get("data") or data.get("markets") or []
        print("Mercados recibidos:", len(data))
    except Exception as e:
        print("FALLO:", type(e).__name__, e)
        print("Si estás en local y en España, es el bloqueo del ISP.")
        return

    seccion("2 · MERCADO CRUDO  (pega ESTO si algo falla)")
    for m in (data or [])[:3]:
        campos = {k: m.get(k) for k in
                  ("question", "slug", "outcomes", "outcomePrices",
                   "liquidityNum", "liquidity", "volumeNum", "volume",
                   "endDate", "end_date_iso", "closed", "active")}
        print(json.dumps(campos, indent=2, ensure_ascii=False)[:1200])
        print("-" * 60)

    seccion("3 · ESCALERA DE BTC")
    mercados = await asyncio.to_thread(pm._mercados, cfg)
    esc = pm.escalera(cfg, mercados, "BTC")
    print(f"{len(mercados)} mercados leídos · {len(esc)} peldaños")
    for p in esc[:15]:
        print(f"  ${p.strike:>12,.0f}  P(encima)={p.p_encima:5.3f}  "
              f"liq={p.liquidez:>10,.0f}  vence={p.vence.date()}  {p.pregunta[:55]}")
    if not esc:
        print("  VACÍO. Causas por orden de probabilidad:")
        print("   a) los campos se llaman distinto -> mira el bloque 2")
        print("   b) PM_MIN_LIQUIDITY / PM_MIN_VOLUME demasiado altos")
        print("   c) las preguntas no contienen above/reach/hit/below")

    seccion("4 · PARSEO DE STRIKES (control)")
    for q in ["Will Bitcoin reach $150,000 by December 31?",
              "Bitcoin above $120k on September 30?",
              "Will BTC dip to $60,000?",
              "Ethereum above $5,000 in 2026?"]:
        print(f"  {q[:52]:<54} -> {pm.parse_strike(q)}")

    seccion("5 · INSTANTÁNEA COMPLETA")
    spot = await precio_btc()
    snap = await pm.snapshot(cfg, spot, [spot] * 350, "5m")
    print(json.dumps(snap, indent=2, ensure_ascii=False))
    print()
    print(pm.format_telegram(snap))
    print()
    print("OJO: la σ realizada de arriba es FALSA (serie plana). En el bot")
    print("sale de las velas reales de BTC, así que el VRP aquí no se lee.")


async def precio_btc():
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get("https://api.binance.com/api/v3/ticker/price",
                            params={"symbol": "BTCUSDT"})
        p = float(r.json()["price"])
        print("Spot BTC:", p)
        return p
    except Exception:
        print("Sin spot; uso 79000 de referencia.")
        return 79000.0


if __name__ == "__main__":
    asyncio.run(main())
