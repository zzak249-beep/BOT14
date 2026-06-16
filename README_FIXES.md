# QF×JP Bot v7.2 — FIXES "0 TRADES"

## Diagnóstico: por qué ambos bots abrían 0 trades

### renewed-love (683 símbolos, 0 señales)
1. **`MIN_TIER=FUEL`** requería score ≥65. El mercado actual genera scores entre 55-65 → todo descartado silenciosamente.
2. **`CB_ATR_MULT=3.0`** demasiado sensible → 15+ circuit breakers en cascada → símbolos bloqueados 10 min.
3. **`CB_BARS=10`** miraba demasiadas velas hacia atrás → activaba CB con volatilidad antigua.
4. **`REQUIRE_TL_BREAK=True`** + **`HTF_MIN_ALIGNED=2`** → doble filtro HTF casi imposible de cumplir en 3m.
5. **683 símbolos** incluye micro-caps sin volumen real → ruido, no señal.

### joyful-art (50 símbolos, 0 señales)
1. Solo scaneaba **top-50 por volumen** = BTC/ETH/BNB... los más eficientes, señal casi imposible.
2. **COPY mode** depende de trades SUP del master que nunca abría (bucle vicioso).
3. **`COPY_MAX_ADVERSE_PCT=0.0%`** rechazaba cualquier trade del master con -0.01% PnL.

---

## Archivos modificados

| Archivo | Cambios |
|---|---|
| `config.py` | MIN_TIER, MIN_SCORE, CB_ATR_MULT, CB_BARS, REQUIRE_TL_BREAK, HTF_MIN_ALIGNED, TOP_N_SYMBOLS |
| `scanner.py` | CB_COOLDOWN 600→300s, TOP_N_SYMBOLS respetado, OBI umbral 0.1→0.15, log diagnóstico |
| `complement_engine.py` | EXCLUSIVE_TOP_N 50→30, COPY_MAX_ADVERSE_PCT -0.3, unrealized_pnl en can_trade |
| `main.py` | Version 7.2, log de config extendido para diagnóstico |

---

## Variables Railway a actualizar

### renewed-love
```
MIN_TIER=STD
MIN_SCORE=55
CB_ATR_MULT=4.0
CB_BARS=5
REQUIRE_TL_BREAK=false
HTF_MIN_ALIGNED=1
TOP_N_SYMBOLS=200
```

### joyful-art
```
MIN_TIER=STD
MIN_SCORE=55
CB_ATR_MULT=4.0
CB_BARS=5
REQUIRE_TL_BREAK=false
HTF_MIN_ALIGNED=1
EXCLUSIVE_TOP_N=30
COPY_MAX_ADVERSE_PCT=-0.3
```

---

## Flujo esperado tras el fix

```
renewed-love:
  683 sym → top-200 por volumen
  analyze() → score 55-100
  tier_ok(STD) → PASA (score ≥55)
  → TRADE

joyful-art:
  top-30 exclusivos
  analyze() → score 55-100
  tier_ok(STD) → PASA
  → TRADE autónomo

  Si MASTER_URL configurado y master tiene trades SUP:
  → COPY 40% size
```

---

## Lo que NO cambió (mantenido de v7.1)
- Trailing stop dinámico (BREAKEVEN_ATR_MULT=1.0, TRAIL_DISTANCE_ATR=1.5)
- Daily loss real con unrealized_pnl
- Kelly Criterion sizing con cap MAX_NOTIONAL_USDT=200 USDT
- Cooldown 2h por símbolo tras pérdida
- reconcile_on_startup
- Hedge macro BTC (modo 4)
- Guardian CVD (modo 3)
