"""
Correlation Manager — evita exposición oculta concentrada
=============================================================
Un scanner de 500+ símbolos puede terminar con 6 posiciones "diversificadas"
que en realidad son la misma apuesta 6 veces (todas correlacionadas a BTC).
Este módulo calcula correlación de retornos vs BTC y limita cuántas
posiciones altamente correlacionadas en la MISMA dirección pueden estar
abiertas simultáneamente.
"""
import logging
import statistics

log = logging.getLogger("correlation_manager")


def _pct_returns(candles):
    closes = [c["close"] for c in candles]
    return [(closes[i] - closes[i - 1]) / closes[i - 1] for i in range(1, len(closes)) if closes[i - 1] != 0]


def pearson_correlation(candles_a, candles_b, lookback=30):
    """Correlación de Pearson entre los retornos porcentuales de dos series de velas."""
    ra = _pct_returns(candles_a)[-lookback:]
    rb = _pct_returns(candles_b)[-lookback:]
    n = min(len(ra), len(rb))
    if n < 10:
        return None
    ra, rb = ra[-n:], rb[-n:]
    mean_a, mean_b = statistics.mean(ra), statistics.mean(rb)
    cov = sum((a - mean_a) * (b - mean_b) for a, b in zip(ra, rb))
    var_a = sum((a - mean_a) ** 2 for a in ra)
    var_b = sum((b - mean_b) ** 2 for b in rb)
    denom = (var_a * var_b) ** 0.5
    if denom == 0:
        return None
    return cov / denom


class CorrelationManager:
    def __init__(self, config):
        self.config = config
        self.open_exposure = {}  # symbol -> {"direction": str, "corr_btc": float}

    def evaluate(self, symbol, direction, candles_symbol, candles_btc):
        """
        Devuelve (can_open: bool, corr: float|None, reason: str)
        """
        threshold = getattr(self.config, "CORR_THRESHOLD", 0.75)
        max_correlated = getattr(self.config, "MAX_CORRELATED_POSITIONS", 2)
        lookback = getattr(self.config, "CORR_LOOKBACK", 30)

        corr = pearson_correlation(candles_symbol, candles_btc, lookback)
        if corr is None:
            return True, None, "datos_insuficientes_para_correlacion"

        if abs(corr) < threshold:
            return True, corr, f"correlación baja ({corr:.2f}), sin restricción"

        same_dir_correlated = sum(
            1 for v in self.open_exposure.values()
            if v["direction"] == direction and abs(v["corr_btc"]) >= threshold
        )
        if same_dir_correlated >= max_correlated:
            return False, corr, (
                f"correlación alta ({corr:.2f}) con BTC y ya hay {same_dir_correlated} "
                f"posiciones {direction} correlacionadas (límite {max_correlated})"
            )
        return True, corr, f"correlación alta ({corr:.2f}) pero dentro del límite permitido"

    def register_open(self, symbol, direction, corr):
        self.open_exposure[symbol] = {"direction": direction, "corr_btc": corr if corr is not None else 0.0}

    def register_close(self, symbol):
        self.open_exposure.pop(symbol, None)
