"""
guardas.py — lo último que se mira antes de mandar una orden.

Tres comprobaciones, las tres nacidas de fallos REALES vistos en los logs
de joyful-art del 08/09/2026 06:36 UTC:

1) ORIENTACIÓN DE SL/TP.  LYN-USDT SHORT salió con sl=0.035412 (POR DEBAJO
   de la entrada 0.03559) y tp=0.035698 (POR ENCIMA). En un corto eso es
   exactamente al revés: el stop iba donde está tu beneficio y el objetivo
   donde está tu pérdida.
   La prueba de que están intercambiados y no mal calculados: el ratio
   tp/sl de LYN es 0.61 mientras el del LONG de SOLV es 1.66; si se
   intercambian los campos de LYN sale 1.64, o sea el mismo ratio. Y el
   "riesgo 0.22%" del log solo cuadra usando la distancia del campo 'tp'
   (0.22% exacto; con la del campo 'sl' daría 0.37%). El bot sabe cuál es
   el stop: lo asigna al campo equivocado al construir la orden.
   Por defecto aquí se BLOQUEA, no se autocorrige. Corregir en silencio
   deja el bug vivo en el generador de señales y te olvidas de él.

2) MARGEN ANTES DE PEDIRLO.  Las dos entradas murieron con
   'Insufficient margin' (código 101204). El error del exchange es
   críptico y llega tarde; aquí se calcula antes y se dice cuánto falta.
   Con margen fijo de 10 USDT x10 cada entrada pide 10 USDT libres, y la
   cuenta ronda los 135 repartidos entre varios bots.

3) RIESGO REAL DE LA OPERACIÓN.  SOLV arriesgaba 2.42% del equity y LYN
   0.22%: once veces de diferencia entre dos entradas consecutivas. Con
   margen fijo el riesgo no lo decides tú, lo decide la distancia al stop.
   Se avisa por encima de un techo, y hay una función para dimensionar por
   riesgo en vez de por margen si algún día quieres cambiarlo.

Ninguna función lanza. Si algo va mal devuelven "no operar" con motivo,
que es el fallo seguro.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger("guardas")

DEFAULTS = {
    "GUARD_ENABLED": True,
    "GUARD_BLOQUEAR_SLTP": True,      # False = solo avisa (NO recomendado)
    "GUARD_COLCHON": 0.95,            # fracción del margen libre utilizable
    "GUARD_MAX_RIESGO_PCT": 3.0,      # techo de riesgo por operación
    "GUARD_MIN_RATIO": 1.0,           # tp/sl mínimo aceptable
    "GUARD_AVISO_MIN": 30,            # minutos de enfriamiento por aviso
}


def cfg(config: Any, key: str):
    return getattr(config, key, DEFAULTS[key])


@dataclass
class Veredicto:
    ok: bool = False
    motivo: str = ""
    detalle: str = ""
    margen_necesario: float = 0.0
    margen_libre: float = 0.0
    riesgo_usdt: float = 0.0
    riesgo_pct: float = 0.0
    ratio: float = 0.0
    sl_corregido: float | None = None
    tp_corregido: float | None = None
    avisos: list = field(default_factory=list)


def _es_largo(side: str, position_side: str = "") -> bool:
    s = (side or "").strip().lower()
    p = (position_side or "").strip().upper()
    if p in ("LONG", "SHORT"):
        return p == "LONG"
    return s in ("buy", "long", "b")


# ───────────────────────────────────────── 1 · orientación de SL y TP
def revisar_sl_tp(side: str, position_side: str, entry: float,
                  sl: float, tp: float) -> tuple[bool, str, float, float]:
    """
    Devuelve (correcto, motivo, sl_ok, tp_ok).

    LONG  -> sl POR DEBAJO de la entrada, tp POR ENCIMA.
    SHORT -> sl POR ENCIMA, tp POR DEBAJO.
    """
    try:
        entry = float(entry)
        sl = float(sl)
        tp = float(tp)
    except (TypeError, ValueError):
        return False, "precios no numéricos", 0.0, 0.0
    if entry <= 0 or sl <= 0 or tp <= 0:
        return False, "algún precio es cero o negativo", sl, tp

    largo = _es_largo(side, position_side)
    lado = "LONG" if largo else "SHORT"

    if largo:
        bien = sl < entry < tp
    else:
        bien = tp < entry < sl
    if bien:
        return True, "", sl, tp

    # ¿Se arregla intercambiándolos? Entonces es el bug de los campos.
    if largo:
        swap_ok = tp < entry < sl
    else:
        swap_ok = sl < entry < tp
    if swap_ok:
        return (False,
                f"{lado}: SL y TP están INTERCAMBIADOS "
                f"(sl={sl:.8g}, tp={tp:.8g}, entrada={entry:.8g})",
                tp, sl)

    return (False,
            f"{lado}: SL/TP incoherentes con la entrada "
            f"(sl={sl:.8g}, tp={tp:.8g}, entrada={entry:.8g})",
            sl, tp)


# ─────────────────────────────────────────────────── 2 · margen
def margen_necesario(qty: float, price: float, leverage: float) -> float:
    try:
        lev = max(float(leverage), 1.0)
        return abs(float(qty)) * float(price) / lev
    except (TypeError, ValueError, ZeroDivisionError):
        return float("inf")


async def leer_cuenta(api: Any) -> dict:
    """
    Lector defensivo: no sé cómo se llama el método en este repo, así que
    prueba los nombres habituales y las claves habituales. Si no encuentra
    nada devuelve ceros, y con ceros la guarda bloquea.
    """
    datos = None
    for nombre in ("cuenta", "balance", "get_balance", "account",
                   "get_account", "fetch_balance"):
        fn = getattr(api, nombre, None)
        if fn is None:
            continue
        try:
            r = fn()
            datos = await r if hasattr(r, "__await__") else r
            if datos:
                break
        except Exception as exc:  # noqa: BLE001
            log.debug("guardas: %s() falló: %s", nombre, exc)
    if not isinstance(datos, dict):
        return {"disponible": 0.0, "equity": 0.0, "leido": False}

    def sacar(claves, por_defecto=0.0):
        for k in claves:
            if k in datos:
                try:
                    return float(datos[k])
                except (TypeError, ValueError):
                    pass
            d = datos.get("data") if isinstance(datos.get("data"), dict) else {}
            if k in d:
                try:
                    return float(d[k])
                except (TypeError, ValueError):
                    pass
        return por_defecto

    return {
        "disponible": sacar(("disponible", "availableMargin", "available",
                             "availableBalance", "free")),
        "equity": sacar(("equity", "balance", "totalWalletBalance", "total")),
        "leido": True,
    }


# ────────────────────────────────────────────────── 3 · riesgo real
def riesgo_de(qty: float, entry: float, sl: float, equity: float) -> tuple[float, float]:
    """Devuelve (riesgo en USDT, % del equity). El stop define el riesgo."""
    try:
        r = abs(float(qty)) * abs(float(entry) - float(sl))
        pct = (r / float(equity) * 100.0) if float(equity) > 0 else 0.0
        return r, pct
    except (TypeError, ValueError, ZeroDivisionError):
        return 0.0, 0.0


def qty_por_riesgo(equity: float, entry: float, sl: float,
                   riesgo_pct_objetivo: float) -> float:
    """
    Dimensionado por RIESGO en vez de por margen fijo. Con margen fijo el
    riesgo lo decide la distancia al stop y sale lo que salga: 2.42% en una
    entrada y 0.22% en la siguiente. Con esto, todas arriesgan lo mismo.
    """
    try:
        dist = abs(float(entry) - float(sl))
        if dist <= 0 or float(equity) <= 0:
            return 0.0
        return float(equity) * float(riesgo_pct_objetivo) / 100.0 / dist
    except (TypeError, ValueError, ZeroDivisionError):
        return 0.0


# ──────────────────────────────────────────── enfriamiento de avisos
_ultimo: dict = {}


def debe_avisar(clave: str, minutos: float = 30.0) -> bool:
    ahora = time.time()
    if ahora - float(_ultimo.get(clave, 0)) < minutos * 60.0:
        return False
    _ultimo[clave] = ahora
    return True


# ─────────────────────────────────────────────── punto de entrada
def revisar(config: Any, symbol: str, side: str, position_side: str,
            qty: float, entry: float, sl: float, tp: float,
            leverage: float, disponible: float, equity: float) -> Veredicto:
    """
    Una llamada, justo antes de mandar la orden. Nunca lanza.
    Si devuelve ok=False, NO se manda nada.
    """
    v = Veredicto()
    try:
        if not cfg(config, "GUARD_ENABLED"):
            v.ok = True
            v.motivo = "guardas desactivadas"
            return v

        # 1 · orientación
        bien, motivo, sl_ok, tp_ok = revisar_sl_tp(side, position_side, entry, sl, tp)
        if not bien:
            v.sl_corregido = sl_ok
            v.tp_corregido = tp_ok
            v.motivo = motivo
            v.detalle = ("Corregido sería sl=" + f"{sl_ok:.8g}" + " tp=" + f"{tp_ok:.8g}"
                         + ". No se autocorrige a propósito: arréglalo en el generador "
                           "de señales o el bug sigue vivo.")
            if cfg(config, "GUARD_BLOQUEAR_SLTP"):
                return v
            v.avisos.append(motivo)

        # 2 · margen
        nec = margen_necesario(qty, entry, leverage)
        colchon = float(cfg(config, "GUARD_COLCHON"))
        v.margen_necesario = nec
        v.margen_libre = float(disponible or 0.0)
        if nec > v.margen_libre * colchon:
            v.motivo = (f"margen insuficiente: pide {nec:.2f} USDT y hay "
                        f"{v.margen_libre:.2f} libres")
            v.detalle = (f"Patrimonio {float(equity or 0):.2f}. La diferencia está "
                         f"bloqueada en posiciones abiertas, tuyas o de otro bot "
                         f"de la misma cuenta.")
            return v

        # 3 · riesgo real
        # equity 0 no es "riesgo cero": es que no se pudo leer la cuenta. Sin
        # ese número el techo de riesgo NO puede comprobarse, y un límite que
        # no puede dispararse es peor que no tenerlo porque crees que está.
        # Es el mismo fallo silencioso del "equity=0.0000" de zesty-reverence.
        if float(equity or 0.0) <= 0.0:
            v.motivo = "equity 0 o ilegible: el techo de riesgo no se puede comprobar"
            v.detalle = ("Sin patrimonio no hay control de riesgo. Revisa la lectura "
                         "de cuenta antes de operar; no se manda nada a ciegas.")
            return v

        sl_real = v.sl_corregido if v.sl_corregido is not None else sl
        v.riesgo_usdt, v.riesgo_pct = riesgo_de(qty, entry, sl_real, equity)
        try:
            dsl = abs(float(entry) - float(sl_real))
            dtp = abs(float(entry) - float(tp if v.tp_corregido is None else v.tp_corregido))
            v.ratio = dtp / dsl if dsl > 0 else 0.0
        except Exception:  # noqa: BLE001
            v.ratio = 0.0

        maxr = float(cfg(config, "GUARD_MAX_RIESGO_PCT"))
        if v.riesgo_pct > maxr:
            v.motivo = f"riesgo {v.riesgo_pct:.2f}% supera el techo {maxr:.2f}%"
            v.detalle = ("Con margen fijo el riesgo lo decide la distancia al stop. "
                         "Si esto pasa mucho, dimensiona por riesgo con qty_por_riesgo().")
            return v

        minratio = float(cfg(config, "GUARD_MIN_RATIO"))
        if v.ratio > 0 and v.ratio < minratio:
            v.avisos.append(f"ratio tp/sl {v.ratio:.2f} por debajo de {minratio:.2f}")

        v.ok = True
        v.motivo = "ok"
        return v
    except Exception as exc:  # noqa: BLE001
        v.ok = False
        v.motivo = f"guarda falló ({type(exc).__name__}): no se opera"
        log.exception("guardas.revisar: %s", exc)
        return v


def formato_telegram(symbol: str, side: str, v: Veredicto) -> str:
    base = str(symbol).split("-")[0]
    if v.ok:
        L = [f"✅ <b>{base}</b> pasa las guardas",
             f"Riesgo {v.riesgo_usdt:.2f} USDT ({v.riesgo_pct:.2f}%) · ratio {v.ratio:.2f}",
             f"Margen {v.margen_necesario:.2f} de {v.margen_libre:.2f} libres"]
        for a in v.avisos:
            L.append(f"⚠️ {a}")
        return "\n".join(L)
    L = [f"🚫 <b>{base}</b> {str(side).upper()} NO enviada",
         v.motivo]
    if v.detalle:
        L.append(f"<i>{v.detalle}</i>")
    return "\n".join(L)
