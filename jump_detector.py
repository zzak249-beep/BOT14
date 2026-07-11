"""
jump_detector.py — Detección de saltos Lee-Mykland / Bipower Variation.

Mismo módulo que se fusionó al superscript de TradingView, portado al bot.

Base (Barndorff-Nielsen & Shephard 2004/2006; Lee & Mykland 2008):
- La varianza realizada (RV, media de retornos²) contiene deriva + saltos.
- La bipower variation (BV, media de |r_i|·|r_{i-1}| escalada por pi/2) es
  robusta a saltos: estima solo la parte CONTINUA.
- Estadístico por vela: L = |retorno| / sqrt(BV local). Bajo movimiento
  browniano puro L ~ normal -> L >= umbral (default 4) = salto
  estadísticamente real, no ruido.

Uso en el bot (anti-chase): si en las últimas JUMP_COOLDOWN_BARS velas
cerradas hubo un salto EN LA DIRECCIÓN de la señal, la entrada está
persiguiendo la detonación (comprar el pico del slippage). Un salto EN
CONTRA es el sweep mismo -> no bloquea, es parte del setup.

Es el detector que usa el mismo estudio RIBAF Vol. 81 que valida el VPIN:
VPIN predice el salto (combustible), esto lo detecta (detonación).

Modos (config.JUMP_GUARD_MODE):
- "off":   ni calcula.
- "log":   calcula y anota en journal/señal, NUNCA bloquea (observación,
           mismo patrón de rollout que ⚠struct en el superscript).
- "block": rechaza señales que persiguen un salto reciente a favor.
"""

import math

MU1_SQ_INV = math.pi / 2.0  # 1/mu1^2 con mu1 = E|Z| = sqrt(2/pi)


def confirms_direction(candles, direction, config):
    """Devuelve dict con el mismo contrato que cvd/rsi/vwap filters:
    confirms: None (sin datos / modo log), True (sin chase), False (chase).
    Además: L_last, jump_recent, jump_dir, jump_share, reason."""
    mode = str(getattr(config, "JUMP_GUARD_MODE", "log")).lower()
    win = int(getattr(config, "JUMP_WIN", 50))
    thresh = float(getattr(config, "JUMP_THRESH", 4.0))
    cooldown = int(getattr(config, "JUMP_COOLDOWN_BARS", 2))

    out = {"confirms": None, "reason": None, "L_last": None,
           "jump_recent": False, "jump_dir": None, "jump_share": None,
           "mode": mode}

    closes = [float(c["close"]) for c in candles if float(c.get("close", 0)) > 0]
    # win retornos para la vol local + cooldown+1 retornos a testear
    need = win + cooldown + 2
    if len(closes) < need:
        out["reason"] = f"datos_insuficientes ({len(closes)}/{need})"
        return out

    rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]

    # Ventana de calibración: retornos ANTERIORES a los testeados, para que
    # el propio salto no infle la vol local que lo mide (Lee-Mykland).
    test_n = cooldown + 1
    calib = rets[-(win + test_n):-test_n]
    bv = MU1_SQ_INV * sum(abs(calib[i]) * abs(calib[i - 1])
                          for i in range(1, len(calib))) / (len(calib) - 1)
    rv = sum(r * r for r in calib) / len(calib)
    out["jump_share"] = max(rv - bv, 0.0) / rv if rv > 0 else None

    if bv <= 0:
        out["reason"] = "bv_cero (sin volatilidad medible)"
        return out
    sigma_local = math.sqrt(bv)

    # Testear las últimas test_n velas cerradas (la señal llega en la vela
    # que acaba de cerrar; el chase relevante es esa y las cooldown previas)
    sig_dir = 1 if direction == "LONG" else -1
    chase = False
    chase_L = None
    for r in rets[-test_n:]:
        L = abs(r) / sigma_local
        if L >= thresh:
            j_dir = 1 if r > 0 else -1
            out["jump_recent"] = True
            out["jump_dir"] = j_dir
            if j_dir == sig_dir:
                chase = True
                chase_L = L
    out["L_last"] = abs(rets[-1]) / sigma_local

    if mode == "log":
        # Observación: anota todo pero nunca bloquea
        out["confirms"] = None
        out["reason"] = ("chase_detectado (solo log)" if chase
                         else "sin_chase (solo log)")
        return out

    if chase:
        out["confirms"] = False
        out["reason"] = (f"jump_chase: salto {'alcista' if sig_dir == 1 else 'bajista'} "
                         f"L={chase_L:.1f} en las últimas {test_n} velas — "
                         f"la entrada persigue la detonación")
    else:
        out["confirms"] = True
        out["reason"] = "sin salto reciente a favor"
    return out
