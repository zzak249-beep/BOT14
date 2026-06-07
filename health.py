"""
GUA-USDT Bot — Health Server (Railway)
Endpoint /health para mantener el servicio vivo en Railway.
"""

from __future__ import annotations
import asyncio
import logging
import time
from typing import Optional

from aiohttp import web

import config

log = logging.getLogger("health")

_start_time  = time.time()
_last_tick   = 0.0
_tick_count  = 0
_signal_count = 0


def register_tick() -> None:
    global _last_tick, _tick_count
    _last_tick  = time.time()
    _tick_count += 1


def register_signal() -> None:
    global _signal_count
    _signal_count += 1


async def _health(request: web.Request) -> web.Response:
    uptime   = int(time.time() - _start_time)
    h, rem   = divmod(uptime, 3600)
    m, s     = divmod(rem, 60)
    last_ago = int(time.time() - _last_tick) if _last_tick else -1
    return web.json_response(
        {
            "status":        "ok",
            "uptime":        f"{h:02d}:{m:02d}:{s:02d}",
            "ticks":         _tick_count,
            "signals":       _signal_count,
            "last_tick_ago": last_ago,
            "mode":          config.MODE,
            "symbol":        config.SYMBOL,
        }
    )


async def _root(request: web.Request) -> web.Response:
    return web.Response(text="GUA-USDT Bot running ✅")


async def start_health_server() -> None:
    """Arranca el servidor HTTP en segundo plano."""
    app    = web.Application()
    app.router.add_get("/",       _root)
    app.router.add_get("/health", _health)
    runner = web.AppRunner(app)
    await runner.setup()
    site   = web.TCPSite(runner, "0.0.0.0", config.PORT)
    await site.start()
    log.info("Health server en http://0.0.0.0:%d", config.PORT)
