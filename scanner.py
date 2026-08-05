"""
Scanner v2 — fetch paralelo con ThreadPoolExecutor + blacklist + caché.
"""
from __future__ import annotations
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional
import pandas as pd
from bingx_api import BingXAPI, BingXError
from utils import is_blacklisted, interval_to_ms
import config as cfg

logger = logging.getLogger(__name__)


def get_active_symbols(api: BingXAPI) -> List[dict]:
    """
    Devuelve top MAX_SYMBOLS contratos USDT-perp activos, ordenados por volumen 24h.
    Excluye tokens apalancados, estables y la BLACKLIST de config.
    """
    try:
        contracts = api.get_contracts()
    except Exception as e:
        logger.error(f"get_contracts: {e}")
        return []

    usdt = [
        c for c in contracts
        if str(c.get("currency", "")).upper() == "USDT"
        and int(c.get("status", 0)) == 1
        and c.get("apiStateBuy", True)
        and c.get("apiStateSell", True)
        and not is_blacklisted(c["symbol"])
        and c["symbol"].upper() not in [b.upper() for b in cfg.BLACKLIST]
    ]

    if not usdt:
        logger.warning("No USDT contracts found after filter")
        return []

    # Volumen 24h
    time.sleep(0.3)
    try:
        tickers = {t["symbol"]: t for t in api.get_tickers()}
    except Exception as e:
        logger.error(f"get_tickers: {e}")
        tickers = {}

    enriched = []
    for c in usdt:
        sym  = c["symbol"]
        tick = tickers.get(sym, {})
        vol  = float(tick.get("quoteVolume", tick.get("volume", 0)))
        if vol >= cfg.MIN_VOLUME_USDT:
            c["volume24h"] = vol
            c["lastPrice"] = float(tick.get("lastPrice", 0))
            enriched.append(c)

    enriched.sort(key=lambda x: x["volume24h"], reverse=True)
    top = enriched[: cfg.MAX_SYMBOLS]
    logger.info(f"Symbols: {len(contracts)} total → {len(usdt)} USDT active → {len(enriched)} vol≥{cfg.MIN_VOLUME_USDT/1e6:.0f}M → top {len(top)}")
    return top


def fetch_candles(api: BingXAPI, symbol: str) -> Optional[pd.DataFrame]:
    """
    Fetch + parse velas. Descarta la vela en formación.
    Retorna None si datos insuficientes.
    """
    try:
        raw = api.get_klines(symbol, cfg.TIMEFRAME, cfg.CANDLES_LIMIT)
    except Exception as e:
        logger.debug(f"klines {symbol}: {e}")
        return None

    if not raw or len(raw) < 60:
        return None

    df = pd.DataFrame(raw)[["time","open","high","low","close","volume"]].copy()
    df = df.astype({"time":"int64","open":"float64","high":"float64",
                    "low":"float64","close":"float64","volume":"float64"})
    df.sort_values("time", inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Descartar vela abierta (aún no cerrada)
    bar_ms  = interval_to_ms(cfg.TIMEFRAME)
    now_ms  = int(time.time() * 1000)
    if now_ms < df["time"].iloc[-1] + bar_ms:
        df = df.iloc[:-1].reset_index(drop=True)

    return df if len(df) >= 60 else None


def fetch_all_candles_parallel(
    api: BingXAPI,
    symbols: List[dict],
) -> Dict[str, pd.DataFrame]:
    """
    Fetch paralelo de velas para todos los símbolos.
    Usa ThreadPoolExecutor con FETCH_WORKERS threads.
    Retorna dict {symbol: df}.
    """
    results: Dict[str, pd.DataFrame] = {}

    with ThreadPoolExecutor(max_workers=cfg.FETCH_WORKERS) as ex:
        futures = {
            ex.submit(fetch_candles, api, c["symbol"]): c["symbol"]
            for c in symbols
        }
        for future in as_completed(futures):
            sym = futures[future]
            try:
                df = future.result(timeout=20)
                if df is not None:
                    results[sym] = df
            except Exception as e:
                logger.debug(f"parallel fetch {sym}: {e}")

    logger.info(f"Fetched {len(results)}/{len(symbols)} symbol candles")
    return results


def build_contract_map(contracts: List[dict]) -> Dict[str, dict]:
    return {c["symbol"]: c for c in contracts}
