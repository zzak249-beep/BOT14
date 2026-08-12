"""
Estado persistente en JSON. Escritura atomica (tmp + rename) para no
corromper el archivo si Railway mata el proceso a mitad de escritura.

IMPORTANTE: monta un volumen de Railway en el directorio de STATE_FILE
(por defecto /data). Sin volumen, el filesystem se resetea en cada
deploy y el bot "olvida" posiciones abiertas, contadores del dia y
el progreso de cada setup -- exactamente el bug de "contadores solo
en RAM" que aparecio en bots anteriores.
"""
import json
import logging
import os
import tempfile
import threading
from datetime import datetime, timezone
from typing import Optional

import config as cfg
from strategy import SymbolState

log = logging.getLogger("state")


class StateManager:
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self.symbol_states: dict = {}          # symbol -> SymbolState
        self.positions: dict = {}              # symbol -> dict (posicion abierta por el bot)
        self.trades_today_date: str = ""
        self.trades_today_count: int = 0
        self.kz_stats: dict = {}               # "LON" -> {"w": int, "l": int}, etc.
        self.path_stats: dict = {}             # "REV"/"CONT" -> {"w": int, "l": int}
        self.tier_stats: dict = {}             # "major"/"altcoin" -> {"w": int, "l": int} -- valida o descarta
                                                # la hipotesis de que sweeps funcionan en altcoins y no en majors
        self.active_days: set = set()          # "YYYY-MM-DD" (UTC) con al menos una apertura -- ver si el
                                                # numero de trades crece con dias reales o con pocos dias replicados
        self.equity_peak: float = 0.0
        self.contract_meta_synced_at: float = 0.0
        self._load()

    # ── Carga / guardado ──
    def _load(self) -> None:
        if not os.path.exists(self.path):
            log.info("Sin estado previo en %s, arrancando en limpio.", self.path)
            return
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            log.error("No se pudo leer %s (%s). Arrancando en limpio.", self.path, e)
            return

        self.symbol_states = {
            sym: SymbolState.from_dict(d) for sym, d in raw.get("symbol_states", {}).items()
        }
        self.positions = raw.get("positions", {})
        self.trades_today_date = raw.get("trades_today_date", "")
        self.trades_today_count = raw.get("trades_today_count", 0)
        self.kz_stats = raw.get("kz_stats", {})
        self.path_stats = raw.get("path_stats", {})
        self.tier_stats = raw.get("tier_stats", {})
        self.active_days = set(raw.get("active_days", []))
        self.equity_peak = raw.get("equity_peak", 0.0)
        log.info(
            "Estado cargado: %d simbolos rastreados, %d posiciones abiertas, %d trades hoy.",
            len(self.symbol_states), len(self.positions), self.trades_today_count,
        )

    def save(self) -> None:
        with self._lock:
            data = {
                "symbol_states": {sym: s.to_dict() for sym, s in self.symbol_states.items()},
                "positions": self.positions,
                "trades_today_date": self.trades_today_date,
                "trades_today_count": self.trades_today_count,
                "kz_stats": self.kz_stats,
                "path_stats": self.path_stats,
                "tier_stats": self.tier_stats,
                "active_days": sorted(self.active_days),
                "equity_peak": self.equity_peak,
                "saved_at": datetime.now(timezone.utc).isoformat(),
            }
            dirpath = os.path.dirname(self.path) or "."
            os.makedirs(dirpath, exist_ok=True)
            try:
                fd, tmp_path = tempfile.mkstemp(dir=dirpath, prefix=".state-", suffix=".tmp")
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(data, f)
                os.replace(tmp_path, self.path)
            except OSError as e:
                log.error("No se pudo guardar el estado en %s: %s", self.path, e)

    # ── Estado por simbolo ──
    def get_symbol_state(self, symbol: str) -> SymbolState:
        if symbol not in self.symbol_states:
            self.symbol_states[symbol] = SymbolState()
        return self.symbol_states[symbol]

    # ── Contador diario ──
    def _today(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def trades_today(self) -> int:
        if self.trades_today_date != self._today():
            self.trades_today_date = self._today()
            self.trades_today_count = 0
        return self.trades_today_count

    def register_trade_opened(self) -> None:
        self.trades_today()  # fuerza el reset si cambio el dia
        self.trades_today_count += 1

    # ── Posiciones que gestiona el bot ──
    def open_position(self, symbol: str, pos: dict) -> None:
        self.positions[symbol] = pos
        self.active_days.add(datetime.now(timezone.utc).strftime("%Y-%m-%d"))

    def close_position(self, symbol: str, win: Optional[bool] = None, kill_zone: Optional[str] = None, path: Optional[str] = None, tier: Optional[str] = None) -> None:
        self.positions.pop(symbol, None)
        if win is not None and kill_zone:
            bucket = self.kz_stats.setdefault(kill_zone, {"w": 0, "l": 0})
            bucket["w" if win else "l"] += 1
        if win is not None and path:
            bucket = self.path_stats.setdefault(path, {"w": 0, "l": 0})
            bucket["w" if win else "l"] += 1
        if win is not None and tier:
            bucket = self.tier_stats.setdefault(tier, {"w": 0, "l": 0})
            bucket["w" if win else "l"] += 1

    def open_position_count(self) -> int:
        return len(self.positions)
