"""
state_manager.py — Estado en memoria del bot.

Deliberadamente NO persiste en disco: Railway puede redeployar y
borrar el filesystem en cualquier momento. En vez de depender de un
archivo de estado que puede desincronizarse, las posiciones abiertas
se reconstruyen SIEMPRE desde BingX (fuente de verdad) en cada ciclo.
Perder el cooldown al reiniciar no es grave (como mucho se procesa una
señal de más tras un reinicio); perder de vista una posición abierta sí
lo sería, y por eso esa parte nunca vive solo en memoria local.
"""

import time
from dataclasses import dataclass, field


@dataclass
class StateManager:
    # symbol -> timestamp (ms) de la última vela que generó señal
    last_signal_time: dict = field(default_factory=dict)
    # symbol -> True si ya se configuró el leverage en esta sesión
    leverage_set: set = field(default_factory=set)
    # symbol -> positionSide conocido en el ciclo anterior (para detectar cierres)
    known_positions: dict = field(default_factory=dict)

    def can_signal(self, symbol: str, candle_time_ms: int, cooldown_bars: int, timeframe_ms: int) -> bool:
        last = self.last_signal_time.get(symbol)
        if last is None:
            return True
        elapsed_bars = (candle_time_ms - last) / timeframe_ms
        return elapsed_bars >= cooldown_bars

    def mark_signal(self, symbol: str, candle_time_ms: int) -> None:
        self.last_signal_time[symbol] = candle_time_ms

    def leverage_already_set(self, symbol: str) -> bool:
        return symbol in self.leverage_set

    def mark_leverage_set(self, symbol: str) -> None:
        self.leverage_set.add(symbol)


def timeframe_to_ms(timeframe: str) -> int:
    unit = timeframe[-1].lower()
    value = int(timeframe[:-1])
    mult = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}.get(unit)
    if mult is None:
        raise ValueError(f"Timeframe no soportado: {timeframe}")
    return value * mult
