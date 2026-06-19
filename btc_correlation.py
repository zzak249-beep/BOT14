"""
QF×JP Bot — BTC Correlation Guard v1.0
═══════════════════════════════════════════════════════════════════════════════
EL PROBLEMA QUE RESUELVE:

El correlation_guard existente (risk_manager.direction_allowed) cuenta
trades por DIRECCIÓN (LONG/SHORT) pero no sabe si dos símbolos distintos
son en realidad la MISMA apuesta. Datos reales del portfolio: top-30 coins
suelen correlacionar 0.6-0.95 con Bitcoin — en la práctica, abrir LONG en
WAL-USDT y LONG en PNUT-USDT no son dos apuestas independientes, son la
misma apuesta repetida sobre la dirección de BTC, con volatilidad extra.

Caso real que esto previene: SXT-USDT, LDO-USDT y FHE-USDT — tres símbolos
"distintos" abiertos LONG en horas diferentes, cerrados los tres juntos a
las 20:45 con -23.47 USDT combinados. Si los tres tenían correlación alta
con BTC, eran la misma apuesta tres veces — el correlation_guard de
dirección no lo detectó porque las aperturas estaban espaciadas en tiempo
(fuera de CORRELATION_WINDOW_SEC=900s cada vez).

CÓMO FUNCIONA:
  1. Cada scan, se descarga BTC-USDT klines UNA VEZ (no por símbolo) —
     coste de 1 llamada extra a la API cada 60s, no por símbolo.
  2. Para cada señal candidata, se calcula la correlación de Pearson entre
     los retornos del símbolo y los de BTC en la misma ventana.
  3. Si |correlación| >= BTC_CORR_THRESHOLD, el trade se clasifica según
     su "dirección neta respecto a BTC":
       - correlación positiva + LONG  → apuesta ALCISTA sobre BTC
       - correlación positiva + SHORT → apuesta BAJISTA sobre BTC
       - correlación negativa + LONG  → apuesta BAJISTA sobre BTC (inverso)
       - correlación negativa + SHORT → apuesta ALCISTA sobre BTC (inverso)
  4. Si ya hay MAX_BTC_CORRELATED_SAME_DIRECTION trades abiertos con la
     MISMA dirección neta sobre BTC (sin importar el símbolo), se bloquea
     la nueva entrada — está apilando la misma apuesta sistémica.

Símbolos con correlación baja a BTC (narrativa propia, ej. una memecoin en
pump específico) NO cuentan para este guard — tienen riesgo idiosincrático
real, no son solo "BTC con volatilidad extra".
═══════════════════════════════════════════════════════════════════════════════
"""
import logging
import time
from collections import deque
from dataclasses import dataclass, field

import numpy as np

log = logging.getLogger("btc_guard")


def compute_correlation(symbol_klines: list, btc_klines: list, lookback: int = 60) -> float:
    """
    Correlación de Pearson entre los retornos del símbolo y los de BTC,
    usando las últimas `lookback` velas (mismo timeframe en ambos).

    Retorna 0.0 si no hay suficientes datos o si alguna serie es constante
    (std=0, ej. símbolo sin movimiento en la ventana).
    """
    if len(symbol_klines) < lookback + 1 or len(btc_klines) < lookback + 1:
        return 0.0

    sym_closes = np.array([c[4] for c in symbol_klines[-(lookback + 1):]], dtype=float)
    btc_closes = np.array([c[4] for c in btc_klines[-(lookback + 1):]], dtype=float)

    if np.any(sym_closes <= 0) or np.any(btc_closes <= 0):
        return 0.0

    sym_ret = np.diff(sym_closes) / sym_closes[:-1]
    btc_ret = np.diff(btc_closes) / btc_closes[:-1]

    if sym_ret.std() < 1e-12 or btc_ret.std() < 1e-12:
        return 0.0

    try:
        corr = np.corrcoef(sym_ret, btc_ret)[0, 1]
    except Exception:
        return 0.0

    return float(corr) if not np.isnan(corr) else 0.0


def btc_net_direction(direction: str, correlation: float) -> str:
    """
    Traduce una señal (dirección + correlación con BTC) a la apuesta neta
    que realmente representa sobre BTC.

    Correlación positiva: LONG=apuesta alcista, SHORT=apuesta bajista (igual)
    Correlación negativa: LONG=apuesta bajista, SHORT=apuesta alcista (invertido)
    """
    if correlation >= 0:
        return direction
    return "SHORT" if direction == "LONG" else "LONG"


@dataclass
class BTCCorrelationGuard:
    """
    Guard de exposición agregada a BTC — complementa (no reemplaza) al
    correlation_guard de dirección existente en risk_manager.
    """
    threshold:        float = 0.5
    window_sec:       int   = 1800   # 30 min — más amplio que el guard de dirección
    max_same:         int   = 3
    _btc_direction_ts: dict = field(default_factory=lambda: {"LONG": [], "SHORT": []})

    def allowed(self, direction: str, correlation: float) -> tuple[bool, str]:
        """
        Chequea Y RESERVA atómicamente si una nueva señal puede abrirse.

        FIX v1.1: misma race condition que risk_manager.can_trade() y
        direction_allowed() — antes el chequeo y el register() ocurrían
        en momentos distintos, separados por el round-trip de red al
        abrir la orden. Ahora reserva de inmediato si pasa, evitando que
        varios símbolos correlacionados a BTC abran a la vez en el mismo
        batch concurrente del scanner.

        Si el trade finalmente no se concreta, llamar a
        release(direction, correlation) para liberar el cupo.
        """
        if abs(correlation) < self.threshold:
            return True, ""  # correlación insuficiente — riesgo idiosincrático real

        net_dir = btc_net_direction(direction, correlation)
        now = time.time()
        ts_list = [t for t in self._btc_direction_ts.get(net_dir, []) if now - t < self.window_sec]

        if len(ts_list) >= self.max_same:
            self._btc_direction_ts[net_dir] = ts_list
            mins = int(self.window_sec / 60)
            return False, (
                f"btc_correlation_guard(btc_dir={net_dir},"
                f"{len(ts_list)}/{self.max_same} en {mins}min,corr={correlation:+.2f})"
            )

        # FIX: reservar de inmediato, no esperar a register()
        ts_list.append(now)
        self._btc_direction_ts[net_dir] = ts_list
        return True, ""

    def register(self, direction: str, correlation: float):
        """
        OBSOLETO tras el fix de reserva atómica — allowed() ya reserva.
        Se mantiene como no-op por compatibilidad, no duplica el registro.
        """
        pass

    def release(self, direction: str, correlation: float):
        """Libera una reserva cuando el trade finalmente no se concreta."""
        if abs(correlation) < self.threshold:
            return
        net_dir = btc_net_direction(direction, correlation)
        ts_list = self._btc_direction_ts.get(net_dir, [])
        if ts_list:
            ts_list.pop()
            self._btc_direction_ts[net_dir] = ts_list


# ── Singleton global (importado por scanner.py / risk_manager.py) ─────────────
btc_guard = BTCCorrelationGuard()
