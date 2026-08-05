"""
Servidor HTTP minimo para que Railway (o cualquier monitor) sepa que
el proceso sigue vivo. Corre en un hilo aparte, no bloquea el loop
asyncio principal.
"""
import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

log = logging.getLogger("healthcheck")

_last_cycle_ts = time.time()
_started_ts = time.time()


def mark_cycle_ok() -> None:
    global _last_cycle_ts
    _last_cycle_ts = time.time()


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 (nombre requerido por BaseHTTPRequestHandler)
        if self.path in ("/", "/health", "/healthz"):
            since_last = time.time() - _last_cycle_ts
            body = json.dumps({
                "status": "ok",
                "uptime_sec": round(time.time() - _started_ts),
                "seconds_since_last_cycle": round(since_last),
            }).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):  # silencia el log por-request por defecto
        pass


def start(port: int) -> None:
    def _run():
        try:
            HTTPServer(("0.0.0.0", port), _Handler).serve_forever()
        except OSError as e:
            log.error("No se pudo levantar el healthcheck en el puerto %d: %s", port, e)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    log.info("Healthcheck escuchando en 0.0.0.0:%d", port)
