"""
QF×JP Bot v6.0 — scanner.py
Filtra pares por volumen (50M–600M USDT).
Excluye: stablecoins, sintéticos BingX (NCC*, NCSI*, índices, materias primas),
         BTC/ETH (demasiado eficientes), y pares con nombre sospechoso.
Fix: no duplica tasks.
"""
import logging
import re
import time as _t
from config import cfg

log = logging.getLogger("SCANNER")

# ── Prefijos de sintéticos BingX a excluir ────────────────
# NCC = BingX Crypto Composite, NCSI = BingX Stock Index
_SYNTHETIC_PREFIXES = ("NCC", "NCSI", "NCCOX", "NCC0")

# ── Palabras clave de activos no-crypto ──────────────────
_NON_CRYPTO_KEYWORDS = (
    "NASDAQ", "SP500", "GOLD", "SILVER", "OIL", "WTI",
    "XAU", "XAG", "BRENT", "CRUDE", "DOW", "NDX",
)

# ── Blacklist explícita ───────────────────────────────────
BLACKLIST = {
    "BTC-USDT", "ETH-USDT",
    "USDC-USDT", "BUSD-USDT", "DAI-USDT", "TUSD-USDT", "FDUSD-USDT",
}

# ── Seed (fallback si el scanner falla) ──────────────────
SEED_SYMBOLS = [
    "SOL-USDT", "AVAX-USDT", "NEAR-USDT", "APT-USDT", "SUI-USDT",
    "INJ-USDT", "SEI-USDT", "ARB-USDT", "OP-USDT", "LINK-USDT",
    "AERO-USDT", "TIA-USDT", "JUP-USDT", "WIF-USDT", "HYPE-USDT",
]


def _is_valid_crypto(sym: str) -> bool:
    """Retorna True solo si es un par crypto-USDT legítimo."""
    if sym in BLACKLIST:
        return False
    # Debe terminar en -USDT
    if not sym.endswith("-USDT"):
        return False
    base = sym.replace("-USDT", "")
    # Excluir sintéticos por prefijo
    for pfx in _SYNTHETIC_PREFIXES:
        if base.upper().startswith(pfx):
            return False
    # Excluir por keyword de activo tradicional
    for kw in _NON_CRYPTO_KEYWORDS:
        if kw in base.upper():
            return False
    # Excluir si el nombre es demasiado largo (sintéticos suelen ser así)
    if len(base) > 12:
        return False
    # Debe contener solo letras, números, y algunos símbolos crypto válidos
    if not re.match(r'^[A-Z0-9]{1,12}$', base):
        return False
    return True


class MarketScanner:
    def __init__(self, exchange):
        self.exchange     = exchange
        self._cache:      list  = []
        self._cache_time: float = 0.0

    async def get_tradeable_symbols(self) -> list:
        # Cache de 1 hora
        if _t.time() - self._cache_time < 3600 and self._cache:
            return self._cache

        try:
            all_sym = await self.exchange.get_all_symbols()
        except Exception as e:
            log.error(f"Scanner error: {e}")
            return self._cache or SEED_SYMBOLS[:cfg.MAX_SYMBOLS]

        filtered = []
        rejected_synthetic = []
        for s in all_sym:
            sym = s["symbol"]
            vol = s["volume"]

            if not _is_valid_crypto(sym):
                if any(sym.startswith(p) for p in _SYNTHETIC_PREFIXES):
                    rejected_synthetic.append(sym)
                continue
            if vol < cfg.MIN_VOLUME_USDT:
                continue
            if vol > cfg.MAX_VOLUME_USDT:
                continue
            filtered.append(sym)

        if rejected_synthetic:
            log.info(f"Scanner: {len(rejected_synthetic)} sintéticos excluidos: "
                     f"{rejected_synthetic[:5]}{'...' if len(rejected_synthetic)>5 else ''}")

        # Ordenar por volumen desc y limitar
        vol_map = {s["symbol"]: s["volume"] for s in all_sym}
        filtered.sort(key=lambda x: vol_map.get(x, 0), reverse=True)
        filtered = filtered[:cfg.MAX_SYMBOLS]

        if not filtered:
            log.warning("Scanner vacío — usando SEED_SYMBOLS")
            filtered = [s for s in SEED_SYMBOLS if _is_valid_crypto(s)][:cfg.MAX_SYMBOLS]

        self._cache      = filtered
        self._cache_time = _t.time()
        log.info(f"Scanner: {len(filtered)} pares crypto válidos | "
                 f"{cfg.MIN_VOLUME_USDT/1e6:.0f}M–{cfg.MAX_VOLUME_USDT/1e6:.0f}M USDT")
        for sym in filtered:
            log.info(f"  ✓ {sym}  vol={vol_map.get(sym,0)/1e6:.1f}M")
        return filtered
