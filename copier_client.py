"""
QF×JP — Copier Client
Lee posiciones y señales del bot MASTER (renewed-love) via su API /status y /positions.
"""
import asyncio
import logging
import os
import aiohttp

log = logging.getLogger("copier_client")

MASTER_URL = os.getenv("MASTER_URL", "")  # ej: https://renewed-love.up.railway.app


class MasterClient:
    def __init__(self):
        self._session = None

    async def _get_session(self):
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=8)
            )
        return self._session

    async def close(self):
        if self._session:
            await self._session.close()

    async def get_master_status(self) -> dict:
        """Obtiene estado completo del bot master."""
        if not MASTER_URL:
            return {}
        try:
            s = await self._get_session()
            async with s.get(f"{MASTER_URL}/status") as r:
                if r.status == 200:
                    return await r.json()
        except Exception as e:
            log.debug("master status error: %s", e)
        return {}

    async def get_master_trades(self) -> dict:
        """
        Retorna trades abiertos del master con todos sus datos:
        symbol → {direction, entry, sl, tp1, tp2, qty, score, tier, be_moved}
        """
        status = await self.get_master_status()
        return status.get("trades", {})

    async def get_master_risk(self) -> dict:
        status = await self.get_master_status()
        return status.get("risk", {})
