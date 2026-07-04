"""
Setup Memory — aprendizaje adaptativo por tipo de setup
===========================================================
Registra el resultado (ganador/perdedor) de cada operación cerrada,
agrupado por "firma" del setup (fuente HTF, tipo de nivel, si tuvo FVG,
dirección). Con suficiente muestra, deprioriza automáticamente las
combinaciones con win rate históricamente pobre — sin tocar la lógica
de detección, solo actuando como filtro final aprendido de la propia
operativa del bot.
"""
import json
import logging
import os
import threading

log = logging.getLogger("setup_memory")


def setup_key(htf_source, level_type, has_fvg, direction):
    return f"{htf_source}|{level_type}|fvg={has_fvg}|{direction}"


class SetupMemory:
    def __init__(self, filepath):
        self.filepath = filepath
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        if not os.path.exists(filepath):
            self._write({})

    def _read(self):
        try:
            with open(self.filepath, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _write(self, data):
        tmp = self.filepath + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, self.filepath)

    def record_outcome(self, key, is_win):
        with self._lock:
            data = self._read()
            stats = data.get(key, {"wins": 0, "losses": 0})
            if is_win:
                stats["wins"] += 1
            else:
                stats["losses"] += 1
            data[key] = stats
            self._write(data)

    def get_stats(self, key):
        with self._lock:
            data = self._read()
        return data.get(key, {"wins": 0, "losses": 0})

    def win_rate(self, key):
        stats = self.get_stats(key)
        total = stats["wins"] + stats["losses"]
        if total == 0:
            return None, 0
        return stats["wins"] / total, total

    def should_allow(self, key, config):
        """
        Devuelve (allow: bool, reason: str).
        Solo bloquea si hay muestra suficiente Y el win rate es pobre.
        Con muestra insuficiente, siempre permite (no penaliza setups nuevos).
        """
        min_samples = getattr(config, "SETUP_MEMORY_MIN_SAMPLES", 15)
        min_win_rate = getattr(config, "SETUP_MEMORY_MIN_WIN_RATE", 0.35)

        rate, total = self.win_rate(key)
        if rate is None or total < min_samples:
            return True, f"muestra insuficiente ({total}/{min_samples}), se permite"

        if rate < min_win_rate:
            return False, f"win rate histórico {rate:.0%} < {min_win_rate:.0%} sobre {total} trades"

        return True, f"win rate histórico {rate:.0%} sobre {total} trades"
