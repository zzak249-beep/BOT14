"""
Entry point — configura logging, health check HTTP y arranca el bot.
"""
import logging
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
import config as cfg


def setup_logging():
    level = getattr(logging, cfg.LOG_LEVEL.upper(), logging.INFO)
    logging.basicConfig(
        level   = level,
        format  = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S",
        handlers= [logging.StreamHandler(sys.stdout)],
    )
    for lib in ("urllib3", "requests", "concurrent.futures"):
        logging.getLogger(lib).setLevel(logging.WARNING)


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, *_):
        pass  # silenciar logs del HTTP server


def start_health_server(port: int = 8080):
    try:
        srv = HTTPServer(("0.0.0.0", port), _HealthHandler)
        t   = threading.Thread(target=srv.serve_forever, daemon=True, name="health-http")
        t.start()
        logging.getLogger(__name__).info(f"Health server on :{port}")
    except Exception as e:
        logging.getLogger(__name__).warning(f"Health server failed: {e}")


if __name__ == "__main__":
    setup_logging()
    log = logging.getLogger(__name__)
    log.info(
        f"FibStruct Bot v2 | TF={cfg.TIMEFRAME} | "
        f"DRY={cfg.DRY_RUN} | MAX_POS={cfg.MAX_POSITIONS} | "
        f"RISK={cfg.RISK_PCT}% | LEV={cfg.LEVERAGE}x | "
        f"MIN_RR={cfg.MIN_RR} | WORKERS={cfg.FETCH_WORKERS}"
    )
    start_health_server(cfg.HEALTH_PORT)
    from bot import FibStructBot
    FibStructBot().run()
