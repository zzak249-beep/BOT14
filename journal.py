"""
Trade Journal — persistencia simple en JSON sobre Railway Volume.
Registra cada señal y operación para auditoría y análisis posterior.
"""
import json
import logging
import os
import threading
from datetime import datetime, timezone

log = logging.getLogger("journal")


class TradeJournal:
    def __init__(self, filepath):
        self.filepath = filepath
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        if not os.path.exists(filepath):
            self._write([])

    def _read(self):
        try:
            with open(self.filepath, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return []

    def _write(self, data):
        tmp = self.filepath + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2, default=str)
        os.replace(tmp, self.filepath)

    def record(self, entry: dict):
        entry = dict(entry)
        entry["timestamp"] = datetime.now(timezone.utc).isoformat()
        with self._lock:
            data = self._read()
            data.append(entry)
            # Mantener journal acotado (últimas 5000 entradas)
            if len(data) > 5000:
                data = data[-5000:]
            self._write(data)

    def recent(self, n=50):
        with self._lock:
            return self._read()[-n:]
