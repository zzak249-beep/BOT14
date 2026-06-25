"""
BTC Regime Engine v1.0 — Clasificador Unificado de Régimen de Mercado
══════════════════════════════════════════════════════════════════════════════
Usa BTC como indicador líder para clasificar el régimen del mercado en 5 estados.

Score de -100 a +100 compuesto de 4 señales independientes:

  1. Estructura de MAs (40% del score)
     MA10 > MA20 > MA50 diaria → bull fuerte
     MA10 < MA20 < MA50 diaria → bear fuerte

  2. Velocidad del slope (25%)
     Cuánto cambió MA10 en las últimas 5 barras → momentum del trend

  3. RSI diario de BTC (20%)
     RSI > 60 = bull   |   RSI < 40 = bear

  4. Trend de funding rate (15%)
     FR positivo y creciente → posiciones LONG dominan el mercado
     FR negativo y decreciente → shorts dominan

Estados:
  STRONG_BULL ( +70 a +100): solo LONGs, zesty activo, sizing máximo
  BULL        ( +30 a  +70): preferir LONGs, SHORTs con penalización
  NEUTRAL     ( -30 a  +30): ambas direcciones, comportamiento normal
  BEAR        ( -30 a  -70): preferir SHORTs, LONGs con penalización
  STRONG_BEAR ( -70 a -100): solo SHORTs, zesty pausado, sizing reducido

Integración:
  # En scan_loop, UNA VEZ por iteración:
  from btc_regime import btc_regime_engine
  regime = await btc_regime_engine.compute(client)

  # Para joyful-art (scanner.py):
  long_penalty  = regime.long_penalty   # pts a añadir al threshold para LONGs
  short_penalty = regime.short_penalty  # pts a añadir al threshold para SHORTs

  # Para zesty-reverence (kotegawa_scanner.py):
  if regime.state == "STRONG_BEAR" and require_regime:
      return None, "bear_regime"

  # En diag:
  diag["counts"][f"regime_{regime.state}"] += 1
══════════════════════════════════════════════════════════════════════════════
"""
import logging
import time
from dataclasses import dataclass

log = logging.getLogger("btc_regime")


# ── Constantes de régimen ─────────────────────────────────────────────────────

STRONG_BULL_THR =  70.0
BULL_THR        =  30.0
BEAR_THR        = -30.0
STRONG_BEAR_THR = -70.0


# ── Helpers ───────────────────────────────────────────────────────────────────

def _sma(values: list, period: int) -> float:
    if len(values) < period:
        return values[-1] if values else 0.0
    return sum(values[-period:]) / period


def _ema(values: list, period: int) -> list:
    if not values:
        return []
    k = 2.0 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(out[-1] + k * (v - out[-1]))
    return out


def _rma(values: list, period: int) -> list:
    n = len(values)
    out = [0.0] * n
    alpha = 1.0 / period
    for i in range(n):
        out[i] = (sum(values[:i+1]) / (i+1)) if i < period else \
                 (out[i-1] + alpha * (values[i] - out[i-1]))
    return out


def _rsi(closes: list, period: int = 14) -> float:
    if len(closes) < period + 2:
        return 50.0
    gains  = [max(closes[i] - closes[i-1], 0) for i in range(1, len(closes))]
    losses = [max(closes[i-1] - closes[i], 0) for i in range(1, len(closes))]
    up = _rma(gains, period)[-1]
    dn = _rma(losses, period)[-1]
    if dn < 1e-12: return 100.0
    if up < 1e-12: return 0.0
    return 100.0 - 100.0 / (1.0 + up / dn)


# ── Resultado del régimen ─────────────────────────────────────────────────────

@dataclass
class RegimeResult:
    """Resultado completo del clasificador de régimen."""
    score:          float   # -100 a +100
    state:          str     # STRONG_BULL / BULL / NEUTRAL / BEAR / STRONG_BEAR

    # Penalizaciones para el scanner (puntos extra al effective_min)
    long_penalty:   float   # cuántos pts adicionales necesita un LONG para pasar
    short_penalty:  float   # cuántos pts adicionales necesita un SHORT para pasar

    # Modificador de sizing
    size_mult:      float   # 1.0 normal | >1 bull | <1 bear

    # Detalles para logs
    ma_score:       float
    slope_score:    float
    rsi_score:      float
    fr_score:       float
    btc_price:      float
    btc_ma10:       float
    btc_ma50:       float
    btc_rsi:        float
    label:          str     # descripción para logs

    def __str__(self):
        return (
            f"BTC_REGIME={self.state} score={self.score:+.0f} "
            f"(ma={self.ma_score:+.0f} slope={self.slope_score:+.0f} "
            f"rsi={self.rsi_score:+.0f} fr={self.fr_score:+.0f}) "
            f"LONG_pen={self.long_penalty:+.0f} SHORT_pen={self.short_penalty:+.0f} "
            f"size_mult={self.size_mult:.2f}"
        )


def _score_to_state(score: float) -> str:
    if score >= STRONG_BULL_THR:
        return "STRONG_BULL"
    if score >= BULL_THR:
        return "BULL"
    if score >= BEAR_THR:
        return "NEUTRAL"
    if score >= STRONG_BEAR_THR:
        return "BEAR"
    return "STRONG_BEAR"


def _state_to_penalties(state: str) -> tuple:
    """
    Retorna (long_penalty, short_penalty) — puntos extra al MIN_SCORE.

    En STRONG_BEAR, un LONG necesita 20 pts extra para pasar el filtro.
    En STRONG_BULL, un SHORT necesita 20 pts extra.
    En NEUTRAL: sin penalización, ambas direcciones en igualdad.
    """
    penalties = {
        "STRONG_BULL": (0.0,  20.0),   # LONGs libres, SHORTs muy penalizados
        "BULL":        (0.0,  10.0),   # LONGs libres, SHORTs con barrera extra
        "NEUTRAL":     (0.0,   0.0),   # sin sesgo
        "BEAR":        (10.0,  0.0),   # LONGs con barrera extra, SHORTs libres
        "STRONG_BEAR": (20.0,  0.0),   # LONGs muy penalizados, SHORTs libres
    }
    return penalties.get(state, (0.0, 0.0))


def _state_to_size_mult(state: str) -> float:
    """Multiplicador de sizing según régimen."""
    return {
        "STRONG_BULL": 1.20,
        "BULL":        1.10,
        "NEUTRAL":     1.00,
        "BEAR":        0.85,
        "STRONG_BEAR": 0.70,
    }.get(state, 1.0)


# ── Motor principal ───────────────────────────────────────────────────────────

class BTCRegimeEngine:
    """
    Clasificador de régimen de mercado basado en BTC.

    Se instancia como singleton (btc_regime_engine) y se llama
    una vez por iteración del scan_loop.

    Cachea el resultado durante cache_ttl segundos para no fetchear
    BTC en cada símbolo del batch.
    """

    def __init__(self, cache_ttl: float = 60.0):
        self._cache_ts:  float         = 0.0
        self._cache_res: RegimeResult  = None
        self._cache_ttl: float         = cache_ttl

    async def compute(self, client, force: bool = False) -> RegimeResult:
        """
        Calcula el régimen actual de BTC.

        Args:
            client:  BingXClient (para fetchear klines y funding rate)
            force:   ignorar caché y recalcular

        Returns:
            RegimeResult con score, estado y penalizaciones
        """
        now = time.time()
        if not force and self._cache_res and (now - self._cache_ts) < self._cache_ttl:
            return self._cache_res

        try:
            result = await self._compute_internal(client)
        except Exception as e:
            log.warning("BTCRegimeEngine error: %s — usando NEUTRAL", e)
            result = self._neutral_result()

        self._cache_ts  = now
        self._cache_res = result
        return result

    async def _compute_internal(self, client) -> RegimeResult:
        import asyncio

        # Fetchear klines diarias y funding rate de BTC en paralelo
        klines_d, klines_h4, fr = await asyncio.gather(
            client.get_klines("BTC-USDT", "1d", 60),
            client.get_klines("BTC-USDT", "4h", 30),
            client.get_funding_rate("BTC-USDT"),
            return_exceptions=True,
        )

        if isinstance(klines_d, Exception) or len(klines_d) < 20:
            return self._neutral_result()

        closes_d = [k[4] for k in klines_d]
        btc_price = closes_d[-1]

        # ── 1. Estructura de MAs (40 pts max) ────────────────────────────────
        ma10  = _sma(closes_d, 10)
        ma20  = _sma(closes_d, 20)
        ma50  = _sma(closes_d, 50)

        if ma10 > ma20 > ma50:
            ma_score = 40.0   # bull perfecto: escalera alcista
        elif ma10 > ma20 and ma20 < ma50:
            ma_score = 20.0   # girando alcista desde bear
        elif ma10 > ma20:
            ma_score = 10.0   # corto plazo alcista, largo dudoso
        elif ma10 < ma20 and ma20 > ma50:
            ma_score = -20.0  # girando bajista desde bull
        elif ma10 < ma20 < ma50:
            ma_score = -40.0  # bear perfecto: escalera bajista
        else:
            ma_score = -10.0  # ambiguo bajista

        # ── 2. Velocidad del slope de MA10 (25 pts max) ──────────────────────
        # Cuánto ha cambiado MA10 en las últimas 5 barras relativo al precio
        if len(closes_d) >= 15:
            ma10_5d_ago = _sma(closes_d[:-5], 10)
            slope_pct   = (ma10 - ma10_5d_ago) / ma10_5d_ago * 100 if ma10_5d_ago > 0 else 0
            # ±2% de slope en 5 días = máxima puntuación
            slope_score = max(-25.0, min(25.0, slope_pct * 12.5))
        else:
            slope_score = 0.0

        # ── 3. RSI diario de BTC (20 pts max) ────────────────────────────────
        rsi_val = _rsi(closes_d, 14)
        if rsi_val >= 60:
            rsi_score = 20.0
        elif rsi_val >= 50:
            rsi_score = 10.0
        elif rsi_val >= 40:
            rsi_score = -10.0
        else:
            rsi_score = -20.0

        # ── 4. Funding rate de BTC (15 pts max) ──────────────────────────────
        fr_val = fr if isinstance(fr, float) else 0.0
        if fr_val > 0.0003:      # FR > 0.03% → bulls dominan fuertemente
            fr_score = 15.0
        elif fr_val > 0.0001:    # FR > 0.01% → bulls ligera ventaja
            fr_score = 7.0
        elif fr_val < -0.0003:   # FR muy negativo → shorts dominan
            fr_score = -15.0
        elif fr_val < -0.0001:
            fr_score = -7.0
        else:
            fr_score = 0.0

        # ── Score total ───────────────────────────────────────────────────────
        score = ma_score + slope_score + rsi_score + fr_score
        score = max(-100.0, min(100.0, score))

        state = _score_to_state(score)
        long_pen, short_pen = _state_to_penalties(state)
        size_mult = _state_to_size_mult(state)

        label = (
            f"BTC={btc_price:.0f} "
            f"MA10={ma10:.0f}|MA20={ma20:.0f}|MA50={ma50:.0f} "
            f"RSI={rsi_val:.0f} FR={fr_val*100:.3f}%"
        )

        result = RegimeResult(
            score=round(score, 1), state=state,
            long_penalty=long_pen, short_penalty=short_pen,
            size_mult=size_mult,
            ma_score=ma_score, slope_score=round(slope_score, 1),
            rsi_score=rsi_score, fr_score=fr_score,
            btc_price=btc_price, btc_ma10=round(ma10, 0),
            btc_ma50=round(ma50, 0), btc_rsi=round(rsi_val, 1),
            label=label,
        )

        log.info("📊 %s", result)
        return result

    @staticmethod
    def _neutral_result() -> RegimeResult:
        return RegimeResult(
            score=0.0, state="NEUTRAL",
            long_penalty=0.0, short_penalty=0.0, size_mult=1.0,
            ma_score=0.0, slope_score=0.0, rsi_score=0.0, fr_score=0.0,
            btc_price=0.0, btc_ma10=0.0, btc_ma50=0.0, btc_rsi=50.0,
            label="neutral_fallback",
        )

    def get_cached(self) -> RegimeResult:
        """Retorna el resultado cacheado sin llamar a la API."""
        return self._cache_res or self._neutral_result()


# Singleton global — importar en scanner.py y kotegawa_scanner.py
btc_regime_engine = BTCRegimeEngine(cache_ttl=120.0)


# ── Test ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Simular distintos escenarios con datos sintéticos
    scenarios = [
        {"name": "STRONG_BULL", "closes": [100+i*2 for i in range(60)], "fr": 0.0004},
        {"name": "STRONG_BEAR", "closes": [200-i*2 for i in range(60)], "fr": -0.0004},
        {"name": "NEUTRAL",     "closes": [150+(-1)**i*2 for i in range(60)], "fr": 0.0},
    ]

    for s in scenarios:
        closes = s["closes"]
        ma10  = _sma(closes, 10)
        ma20  = _sma(closes, 20)
        ma50  = _sma(closes, 50)
        rsi   = _rsi(closes, 14)
        fr    = s["fr"]

        if ma10 > ma20 > ma50: ma_s = 40.0
        elif ma10 < ma20 < ma50: ma_s = -40.0
        else: ma_s = 0.0

        ma10_5d = _sma(closes[:-5], 10)
        sl_pct = (ma10 - ma10_5d) / ma10_5d * 100 if ma10_5d > 0 else 0
        sl_s = max(-25.0, min(25.0, sl_pct * 12.5))

        rsi_s = 20 if rsi >= 60 else 10 if rsi >= 50 else -10 if rsi >= 40 else -20
        fr_s = 15 if fr > 0.0003 else 7 if fr > 0.0001 else -15 if fr < -0.0003 else -7 if fr < -0.0001 else 0

        score = max(-100, min(100, ma_s + sl_s + rsi_s + fr_s))
        state = _score_to_state(score)
        lp, sp = _state_to_penalties(state)
        sm = _state_to_size_mult(state)

        print(f"\n{s['name']} → score={score:+.0f} state={state}")
        print(f"  LONG_pen={lp:+.0f} SHORT_pen={sp:+.0f} size_mult={sm:.2f}")
        print(f"  RSI={rsi:.0f} FR={fr*100:.3f}%")
