"""
QF×JP Bot v6.0 — scanner.py
Filtra pares por volumen (50M–600M USDT) y excluye stablecoins/memecoins extremas.
"""
import logging
from config import cfg

log = logging.getLogger("SCANNER")

# Pares siempre excluidos
BLACKLIST = {
    "BTC-USDT", "ETH-USDT",           # demasiado eficientes para este edge
    "USDC-USDT", "BUSD-USDT", "DAI-USDT",  # stablecoins
}

# Pares de alta convicción para seed inicial
SEED_SYMBOLS = [
    "SOL-USDT", "AVAX-USDT", "NEAR-USDT", "APT-USDT", "SUI-USDT",
    "INJ-USDT", "SEI-USDT", "ARB-USDT", "OP-USDT", "LINK-USDT",
    "AERO-USDT", "BANANA-USDT", "TIA-USDT", "JUP-USDT", "WIF-USDT",
]


class MarketScanner:
    def __init__(self, exchange):
        self.exchange = exchange
        self._cache:      list  = []
        self._cache_time: float = 0.0

    async def get_tradeable_symbols(self) -> list:
        import time
        # Re-escanear cada hora
        if time.time() - self._cache_time < 3600 and self._cache:
            return self._cache

        try:
            all_sym = await self.exchange.get_all_symbols()
        except Exception as e:
            log.error(f"Scanner error: {e}")
            return self._cache or SEED_SYMBOLS[:cfg.MAX_SYMBOLS]

        filtered = []
        for s in all_sym:
            sym = s["symbol"]
            vol = s["volume"]
            if sym in BLACKLIST:
                continue
            if vol < cfg.MIN_VOLUME_USDT:
                continue
            if vol > cfg.MAX_VOLUME_USDT:
                continue
            filtered.append(sym)

        # Ordenar por volumen descendente y limitar
        all_sym_dict = {s["symbol"]: s["volume"] for s in all_sym}
        filtered.sort(key=lambda x: all_sym_dict.get(x, 0), reverse=True)
        filtered = filtered[:cfg.MAX_SYMBOLS]

        if not filtered:
            log.warning("Scanner vacío — usando SEED_SYMBOLS")
            filtered = [s for s in SEED_SYMBOLS if s not in BLACKLIST][:cfg.MAX_SYMBOLS]

        import time as _t
        self._cache      = filtered
        self._cache_time = _t.time()
        log.info(f"Scanner: {len(filtered)} pares activos | "
                 f"Vol {cfg.MIN_VOLUME_USDT/1e6:.0f}M–{cfg.MAX_VOLUME_USDT/1e6:.0f}M USDT")
        return filtered
