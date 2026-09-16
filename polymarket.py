"""
Polymarket como FUENTE DE DATOS de la flota, no como mesa de trading.

═══════════════════════════════════════════════════════════════════════
POR QUÉ NO SE OPERA ALLÍ (las tres razones, con números)
═══════════════════════════════════════════════════════════════════════
1. España: la DGOJ abrió expediente a Polymarket y Kalshi el 26/05/2026 y
   ordenó a los ISP bloquear el acceso. Es bloqueo del proveedor, no
   geobloqueo de la plataforma. Verifica el estado antes de dar nada por
   hecho — se describió como temporal, 3-4 meses pendiente de revisión.

2. Coste: la comisión taker en la categoría cripto es del 1,80%. Tu ida y
   vuelta en perpetuos es 0,25%. Siete veces peor. Es el mismo muro que
   el carry de funding: el porcentaje es correcto y la base es pequeña.

3. Información: el oráculo de precio va 2-5 segundos por detrás del dato
   real del exchange, y la ventana mediana de arbitraje bajó a ~2,7 s.
   Los libros BTC UP/DOWN de 15m y 1h son un DERIVADO del precio que ya
   tienes en BingX. No hay nada ahí que no tengas antes.

═══════════════════════════════════════════════════════════════════════
LO QUE SÍ VALE, Y ES GRATIS
═══════════════════════════════════════════════════════════════════════
Gamma y Data API son públicas y sin autenticación. El bot corre en
Railway, no en España, así que el bloqueo del ISP no le afecta.

Los mercados con vencimiento largo forman una ESCALERA de probabilidades
sobre el precio de BTC: una densidad implícita que sólo tendrías de otra
forma pagando datos de opciones de Deribit. De ahí salen tres cosas:

  A) SIGMA IMPLÍCITA a horizonte, contra tu volatilidad realizada.
     Ratio alto = el mercado paga por movimiento que aún no está en tu
     ventana (terreno de ruptura). Ratio bajo = complacencia (terreno de
     reversión). HIPÓTESIS MÍA, no un resultado medido.

  B) MEDIANA, P(sube) y ASIMETRÍA implícitas, como contexto del diario —
     exactamente igual que BTC_CONTEXT y el funding.

  C) VENTANAS DE EVENTO: binarios macro fechados que resuelven en horas.
     Es lo que le faltó a todos el 10-11 oct 2025, cuando un titular de
     aranceles liquidó ~19.000 M en OI. Un bot que sabe que hay un reloj
     corriendo puede no abrir.

═══════════════════════════════════════════════════════════════════════
CALIBRACIÓN — DÓNDE CREER Y DÓNDE NO
═══════════════════════════════════════════════════════════════════════
Sobre 28.407 mercados resueltos (ene 2024 - may 2026) el error medio
absoluto de calibración fue de 2,1 puntos: un contrato a 40c resolvió SÍ
el 41% de las veces. PERO el sesgo favorito-longshot está documentado en
los DOS extremos: los contratos a 90c resuelven menos del 90% de las
veces, los de 10c más del 10%, y a horizontes largos los precios se
comprimen hacia el 50%.

Por eso sólo se usa la banda 0,15-0,85 para ajustar la distribución. La
cola es justo donde el dato miente, así que se descarta a propósito.

ESTADO: cero uso en producción, y el parseo de campos de Gamma está
escrito contra la documentación, no contra una respuesta real.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from statistics import NormalDist, pstdev
from typing import Any

import httpx

log = logging.getLogger("polymarket")

GAMMA = "https://gamma-api.polymarket.com"
UA = {"User-Agent": "wavelet-bot/1.0"}

# Se replican aquí para que el módulo funcione aunque main.py no los inyecte.
DEFAULTS = {
    "PM_ENABLED": True,
    "PM_BASE": "BTC",
    "PM_MIN_LIQUIDITY": 5000.0,
    "PM_MIN_VOLUME": 20000.0,
    "PM_TRUST_LO": 0.15,
    "PM_TRUST_HI": 0.85,
    "PM_MIN_LADDER": 4,
    "PM_EVENT_HOURS": 6.0,
    "PM_EVENT_LO": 0.20,
    "PM_EVENT_HI": 0.80,
    "PM_VRP_HIGH": 1.25,
    "PM_VRP_LOW": 0.80,
    "PM_TIMEOUT": 12,
    # Gamma devuelve como mucho 100 por página: pedir 500 no da 500, da 100.
    # Hay que paginar, y hay que hacerlo con criterio porque ordenado por
    # volumen las primeras páginas son política y deportes.
    "PM_PAGINAS": 12,            # 12 x 100 = 1200 mercados
    "PM_TAGS": "bitcoin,crypto", # etiquetas de /events que se piden primero
}


def cfg(config: Any, key: str):
    return getattr(config, key, DEFAULTS[key])


# ─────────────────────────────────────────────────────────── utilidades
def barras_por_dia(tf: str) -> float:
    """'5m' -> 288, '15m' -> 96, '30m' -> 48, '1h' -> 24, '4h' -> 6, '1d' -> 1."""
    try:
        s = str(tf).strip().lower()
        n = float(re.sub(r"[^0-9.]", "", s) or 1)
        if n <= 0:
            return 288.0
        if s.endswith("m"):
            return 1440.0 / n
        if s.endswith("h"):
            return 24.0 / n
        if s.endswith("d"):
            return 1.0 / n
        if s.endswith("w"):
            return 1.0 / (7.0 * n)
    except Exception:  # noqa: BLE001
        pass
    return 288.0


def sigma_por_vela(cierres, n: int = 300) -> float:
    """
    Desviación típica de los retornos LOGARÍTMICOS por vela.

    OJO: esto NO es ATR%. Son cosas distintas, y si se le pasa ATR% el
    VRP sale desplazado por un factor constante sin que nada avise. Esta
    función existe justo para que no haya forma de equivocarse.
    """
    try:
        c = [float(x) for x in cierres if x and float(x) > 0][-(n + 1):]
        if len(c) < 30:
            return 0.0
        rets = [math.log(c[i] / c[i - 1]) for i in range(1, len(c))]
        s = pstdev(rets) if len(rets) > 1 else 0.0
        return float(s) if s > 0 else 0.0
    except Exception:  # noqa: BLE001
        return 0.0


def cierres_de(velas) -> list[float]:
    """Extrae los cierres de la lista de velas del bot (dicts con 'close')."""
    try:
        return [float(v["close"]) for v in velas if v and v.get("close")]
    except Exception:  # noqa: BLE001
        return []


# ───────────────────────────────────────────────────────────── modelos
@dataclass
class Peldano:
    strike: float
    p_encima: float
    vence: datetime
    pregunta: str
    liquidez: float
    volumen: float


@dataclass
class Implicita:
    ok: bool = False
    motivo: str = "sin datos"
    n: int = 0
    horizonte_dias: float = 0.0
    mediana: float = 0.0
    sigma: float = 0.0        # logarítmica AL HORIZONTE, no anualizada
    asimetria: float = 0.0
    p_sube: float = 0.0
    peldanos: list = field(default_factory=list)


# ─────────────────────────────────────────────────────────────── fetch
def _get(url: str, params: dict, timeout: int) -> Any:
    with httpx.Client(timeout=timeout, headers=UA) as c:
        r = c.get(url, params=params)
        r.raise_for_status()
        return r.json()


def _lista(raw) -> list:
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:  # noqa: BLE001
            return []
    return []


_STRIKE_RE = re.compile(r"\$\s*([\d][\d,\.]*)\s*([kKmM])?")
_ENCIMA = ("above", "reach", "hit", "exceed", "or more", "greater", "over")
_DEBAJO = ("below", "under", "dip to", "or less", "fall to", "drop to")


def parse_strike(pregunta: str) -> float | None:
    m = _STRIKE_RE.search(pregunta or "")
    if not m:
        return None
    try:
        v = float(m.group(1).replace(",", ""))
    except ValueError:
        return None
    suf = (m.group(2) or "").lower()
    if suf == "k":
        v *= 1_000
    elif suf == "m":
        v *= 1_000_000
    return v if v > 0 else None


# Cómo se consiguieron los mercados en la última llamada. Se enseña en el
# aviso: si la escalera falla, lo primero que hay que saber es si el fallo
# está en la descarga o en el filtrado.
ORIGEN: dict[str, int] = {}


def _norm(raw) -> list:
    if isinstance(raw, dict):
        return raw.get("data") or raw.get("markets") or raw.get("events") or []
    return raw or []


def _por_etiquetas(config: Any, timeout: int) -> list:
    """
    Los peldaños de BTC viven dentro de un EVENTO ("What price will Bitcoin
    hit..."), no sueltos en el ranking global de volumen. Pedirlos por
    etiqueta los trae directamente, sin depender de que asomen entre los
    mercados más negociados del sitio.
    """
    out: list = []
    tags = [t.strip() for t in str(cfg(config, "PM_TAGS")).split(",") if t.strip()]
    for tag in tags:
        try:
            data = _norm(_get(f"{GAMMA}/events",
                              {"active": "true", "closed": "false",
                               "tag_slug": tag, "limit": 100}, timeout))
        except Exception as exc:  # noqa: BLE001
            log.debug("etiqueta %s falló: %s", tag, exc)
            continue
        n = 0
        for ev in data:
            for m in (ev.get("markets") or []):
                out.append(m)
                n += 1
        ORIGEN[f"tag:{tag}"] = n
    return out


def _paginado(config: Any, timeout: int) -> list:
    """Barrido general. 100 por página, que es el tope real de Gamma."""
    out: list = []
    paginas = int(cfg(config, "PM_PAGINAS"))
    for i in range(max(paginas, 1)):
        try:
            lote = _norm(_get(f"{GAMMA}/markets",
                              {"active": "true", "closed": "false",
                               "limit": 100, "offset": i * 100,
                               "order": "volumeNum", "ascending": "false"}, timeout))
        except Exception as exc:  # noqa: BLE001
            log.debug("página %d falló: %s", i, exc)
            break
        if not lote:
            break
        out.extend(lote)
        if len(lote) < 100:
            break
    ORIGEN["paginado"] = len(out)
    return out


def _mercados(config: Any) -> list:
    """
    Primero por etiqueta (dirigido), luego el barrido paginado. Se juntan y
    se quitan duplicados por id: un mercado puede venir por los dos caminos.
    """
    timeout = int(cfg(config, "PM_TIMEOUT"))
    ORIGEN.clear()
    bruto = _por_etiquetas(config, timeout) + _paginado(config, timeout)
    vistos: set = set()
    out: list = []
    for m in bruto:
        if not isinstance(m, dict):
            continue
        clave = m.get("id") or m.get("conditionId") or m.get("slug") or m.get("question")
        if clave in vistos:
            continue
        vistos.add(clave)
        out.append(m)
    ORIGEN["únicos"] = len(out)
    return out


# Por qué se cayó cada mercado. Sin esto, "sin escalera" no distingue entre
# "no hay mercados de BTC", "los umbrales de liquidez se los comen" y "las
# preguntas están redactadas de otra forma". Son tres arreglos distintos.
EMBUDO: dict[str, int] = {}


def escalera(config: Any, mercados: list, base: str = "BTC") -> list[Peldano]:
    nombre = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana"}.get(
        base.upper(), base.lower())
    minliq = float(cfg(config, "PM_MIN_LIQUIDITY"))
    minvol = float(cfg(config, "PM_MIN_VOLUME"))
    EMBUDO.clear()
    EMBUDO["leídos"] = len(mercados)

    def cae(motivo):
        EMBUDO[motivo] = EMBUDO.get(motivo, 0) + 1

    out: list[Peldano] = []
    for m in mercados:
        q = m.get("question") or m.get("title") or ""
        ql = q.lower()
        if nombre not in ql and base.lower() not in ql:
            continue                       # ni se cuenta: no es del subyacente
        EMBUDO["del subyacente"] = EMBUDO.get("del subyacente", 0) + 1

        strike = parse_strike(q)
        if strike is None:
            cae("sin strike en la pregunta")
            continue
        if not any(w in ql for w in _ENCIMA) and not any(w in ql for w in _DEBAJO):
            cae("sin arriba/abajo")
            continue

        liq = float(m.get("liquidityNum") or m.get("liquidity") or 0)
        vol = float(m.get("volumeNum") or m.get("volume") or 0)
        if liq < minliq:
            cae("poca liquidez")
            continue
        if vol < minvol:
            cae("poco volumen")
            continue

        outs = [str(o).lower() for o in _lista(m.get("outcomes"))]
        precios = [float(p) for p in _lista(m.get("outcomePrices")) if p not in (None, "")]
        if len(precios) < 2 or len(outs) < 2:
            cae("sin precios")
            continue
        try:
            p_si = precios[outs.index("yes")]
        except ValueError:
            p_si = precios[0]
        p_encima = p_si if any(w in ql for w in _ENCIMA) else 1.0 - p_si

        bruto = m.get("endDate") or m.get("end_date_iso") or m.get("endDateIso")
        if not bruto:
            cae("sin fecha de vencimiento")
            continue
        try:
            vence = datetime.fromisoformat(str(bruto).replace("Z", "+00:00"))
        except ValueError:
            cae("fecha ilegible")
            continue
        out.append(Peldano(strike, p_encima, vence, q, liq, vol))

    EMBUDO["peldaños"] = len(out)
    out.sort(key=lambda p: p.strike)
    return out


def diagnostico() -> str:
    """Una línea con de dónde salieron los mercados y dónde se cayeron."""
    org = " · ".join(f"{k}: {v}" for k, v in ORIGEN.items()) or "sin datos"
    emb = " · ".join(f"{k}: {v}" for k, v in EMBUDO.items() if v) or "sin datos"
    return f"origen → {org}\nembudo → {emb}"


# ──────────────────────────────────────────── ajuste de la distribución
def ajustar(config: Any, spot: float, peldanos: list[Peldano]) -> Implicita:
    """
    Regresión sobre TODA la escalera. Para una lognormal,
        ln(K/S0) = mu + sigma * Phi^-1(1 - p)
    así que mu y sigma salen de una recta, no de dos puntos elegidos a mano.

    sigma sale AL HORIZONTE MEDIO de los mercados usados, no anualizada:
    para compararla con la realizada hay que escalar ésta por la raíz del
    mismo número de velas.
    """
    r = Implicita()
    if not peldanos or spot <= 0:
        r.motivo = "sin escalera"
        return r

    lo = float(cfg(config, "PM_TRUST_LO"))
    hi = float(cfg(config, "PM_TRUST_HI"))
    usa = [p for p in peldanos if lo <= p.p_encima <= hi]
    r.n = len(usa)
    if len(usa) < int(cfg(config, "PM_MIN_LADDER")):
        r.motivo = f"solo {len(usa)} peldanos creibles (colas descartadas a proposito)"
        r.peldanos = peldanos
        return r

    nd = NormalDist()
    xs, ys = [], []
    for p in usa:
        try:
            ys.append(nd.inv_cdf(1.0 - p.p_encima))
            xs.append(math.log(p.strike / spot))
        except Exception:  # noqa: BLE001
            continue
    if len(xs) < 3:
        r.motivo = "peldanos insuficientes tras limpiar"
        return r

    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    den = sum((y - my) ** 2 for y in ys)
    if den <= 1e-12:
        r.motivo = "escalera degenerada"
        return r
    sigma = sum((xs[i] - mx) * (ys[i] - my) for i in range(n)) / den
    mu = mx - sigma * my
    if sigma <= 0:
        r.motivo = "sigma no positiva: escalera incoherente"
        return r

    ahora = datetime.now(timezone.utc)
    r.horizonte_dias = max(
        sum((p.vence - ahora).total_seconds() for p in usa) / len(usa) / 86400.0, 0.0)
    r.ok = True
    r.motivo = "ok"
    r.sigma = sigma
    r.mediana = spot * math.exp(mu)
    r.p_sube = 1.0 - nd.cdf((0.0 - mu) / sigma)
    try:
        k25 = spot * math.exp(mu + sigma * nd.inv_cdf(0.75))
        k75 = spot * math.exp(mu + sigma * nd.inv_cdf(0.25))
        arriba, abajo = k25 - r.mediana, r.mediana - k75
        r.asimetria = (arriba - abajo) / max(arriba + abajo, 1e-9)
    except Exception:  # noqa: BLE001
        r.asimetria = 0.0
    r.peldanos = peldanos
    return r


def vrp(config: Any, imp: Implicita, sigma_vela: float,
        barras_dia: float) -> tuple[float, str]:
    """Implicita al horizonte contra realizada escalada por raiz del tiempo."""
    if not imp.ok or sigma_vela <= 0 or imp.horizonte_dias <= 0:
        return 0.0, "sin datos"
    barras = max(imp.horizonte_dias * barras_dia, 1.0)
    realizada = sigma_vela * math.sqrt(barras)
    if realizada <= 0:
        return 0.0, "sin datos"
    ratio = imp.sigma / realizada
    if ratio >= float(cfg(config, "PM_VRP_HIGH")):
        return ratio, "implicita muy por encima: expansion esperada"
    if ratio <= float(cfg(config, "PM_VRP_LOW")):
        return ratio, "complacencia: rango esperado"
    return ratio, "sin desajuste claro"


# ──────────────────────────────────────────────────── ventana de evento
_TERMINOS = ("fed", "cpi", "inflation", "rate cut", "rate hike", "tariff",
             "fomc", "jobs", "unemployment", "sec ", "etf", "shutdown",
             "election", "powell")


def evento(config: Any, mercados: list) -> dict:
    """
    Binario macro fechado que resuelve dentro de N horas y sigue indeciso.
    No predice nada: avisa de que hay un reloj corriendo.
    """
    ahora = datetime.now(timezone.utc)
    hmax = float(cfg(config, "PM_EVENT_HOURS"))
    plo, phi = float(cfg(config, "PM_EVENT_LO")), float(cfg(config, "PM_EVENT_HI"))
    minvol = float(cfg(config, "PM_MIN_VOLUME"))
    mejor = None
    for m in mercados:
        q = m.get("question") or ""
        if not any(t in q.lower() for t in _TERMINOS):
            continue
        if float(m.get("volumeNum") or m.get("volume") or 0) < minvol:
            continue
        bruto = m.get("endDate") or m.get("end_date_iso")
        if not bruto:
            continue
        try:
            vence = datetime.fromisoformat(str(bruto).replace("Z", "+00:00"))
        except ValueError:
            continue
        horas = (vence - ahora).total_seconds() / 3600.0
        if not (0 < horas <= hmax):
            continue
        precios = [float(p) for p in _lista(m.get("outcomePrices")) if p not in (None, "")]
        if not precios or not (plo <= precios[0] <= phi):
            continue
        if mejor is None or horas < mejor["horas"]:
            mejor = {"horas": horas, "pregunta": q, "precio": precios[0]}
    return mejor or {}


# ──────────────────────────────────────────────────── punto de entrada
def _snapshot_sync(config: Any, spot: float, sigma_vela: float,
                   barras_dia: float, base: str) -> dict:
    mercados = _mercados(config)
    esc = escalera(config, mercados, base)
    imp = ajustar(config, spot, esc)
    ratio, lectura = vrp(config, imp, sigma_vela, barras_dia)
    ev = evento(config, mercados)
    return {
        "ok": True,
        "motivo": imp.motivo,
        "mercados": len(mercados),
        "diagnostico": diagnostico(),
        "peldanos": imp.n,
        "horizonte_dias": round(imp.horizonte_dias, 1),
        "mediana": round(imp.mediana, 8) if imp.ok else None,
        "sigma_implicita": round(imp.sigma, 4) if imp.ok else None,
        "sigma_realizada": round(sigma_vela, 6),
        "p_sube": round(imp.p_sube, 3) if imp.ok else None,
        "asimetria": round(imp.asimetria, 3) if imp.ok else None,
        "vrp": round(ratio, 2),
        "vrp_lectura": lectura,
        "evento_activo": bool(ev),
        "evento_horas": round(ev["horas"], 1) if ev else None,
        "evento": ev.get("pregunta") if ev else None,
        "evento_precio": ev.get("precio") if ev else None,
    }


async def snapshot(config: Any, spot: float, cierres, timeframe: str,
                   base: str | None = None) -> dict:
    """
    Instantánea completa. NUNCA lanza y NUNCA bloquea el bucle: la parte
    de red va en un hilo, porque httpx.Client es sincrono y una llamada de
    12 s congelaria el reconcile, el heartbeat y la gestion de abiertas.
    """
    base = (base or cfg(config, "PM_BASE")).upper()
    if not cfg(config, "PM_ENABLED"):
        return {"ok": False, "motivo": "desactivado"}
    try:
        sigma_vela = sigma_por_vela(cierres)
        barras_dia = barras_por_dia(timeframe)
        return await asyncio.to_thread(
            _snapshot_sync, config, float(spot), sigma_vela, barras_dia, base)
    except Exception as exc:  # noqa: BLE001
        log.warning("Polymarket no disponible: %s", exc)
        return {"ok": False, "motivo": f"{type(exc).__name__}: {exc}"}


def format_telegram(snap: dict, base: str = "BTC") -> str:
    if not snap.get("ok"):
        return f"🔮 <b>Polymarket · {base}</b> — sin datos ({snap.get('motivo')})"
    L = [f"🔮 <b>Polymarket · {base}</b>"]
    if snap.get("mediana"):
        L.append(f"Mediana implícita <code>{snap['mediana']:.8g}</code> · "
                 f"P(sube) {snap['p_sube']:.0%}")
        L.append(f"σ implícita {snap['sigma_implicita']:.1%} a "
                 f"{snap['horizonte_dias']:.0f} d ({snap['peldanos']} peldaños)")
        sk = snap.get("asimetria") or 0.0
        cara = "alcista" if sk > 0.1 else "bajista" if sk < -0.1 else "plana"
        L.append(f"Asimetría {cara} ({sk:+.2f}) · σ realizada por vela "
                 f"{snap.get('sigma_realizada', 0):.4f}")
        L.append(f"VRP {snap['vrp']:.2f} — {snap['vrp_lectura']}")
    else:
        L.append(f"Escalera no utilizable: {snap['motivo']} "
                 f"({snap.get('mercados', 0)} mercados leídos)")
        # El diagnóstico va en el propio aviso: si no, hay que entrar en los
        # logs de Railway para saber si el fallo es de descarga o de filtro.
        if snap.get("diagnostico"):
            L.append(f"<code>{snap['diagnostico']}</code>")
    if snap.get("evento_activo"):
        L.append("")
        L.append(f"⏳ <b>Evento macro en {snap['evento_horas']:.1f} h</b>")
        L.append(f"<i>{str(snap.get('evento'))[:110]}</i> · a "
                 f"{snap.get('evento_precio', 0):.2f}")
    L.append("")
    L.append("<i>Contexto para el diario. El VRP es hipótesis sin medir.</i>")
    return "\n".join(L)


def sesgo(snap: dict) -> str:
    """Lectura corta para acompañar una señal, al estilo de funding.sesgo()."""
    if not snap or not snap.get("ok") or not snap.get("vrp"):
        return "sin contexto"
    v = snap["vrp"]
    if v >= 1.25:
        return "expansión implícita"
    if v <= 0.80:
        return "complacencia implícita"
    return "sin desajuste"


if __name__ == "__main__":
    class _C:
        pass

    async def _demo():
        snap = await snapshot(_C(), 79000.0, [79000.0] * 350, "5m")
        print(json.dumps(snap, indent=2, ensure_ascii=False))
        print()
        print(format_telegram(snap))

    asyncio.run(_demo())
