"""
Scanner — recorre todos los símbolos perpetuos de BingX (filtrados por
liquidez y excluyendo no-cripto), descarga velas multi-timeframe y
evalúa la señal combinada Supertrend + Unicorn Model.
"""
import asyncio
import logging

from combined_engine import evaluate_symbol
from order_flow import confirm_with_order_flow
from funding_oi_filter import confirm_with_funding_oi

log = logging.getLogger("scanner")


def _is_valid_symbol(symbol, non_crypto_prefixes, require_usdt_quote=True):
    base = symbol.split("-")[0] if "-" in symbol else symbol.replace("USDT", "")
    if any(base.startswith(p) for p in non_crypto_prefixes):
        return False
    if require_usdt_quote:
        quote = symbol.split("-")[1] if "-" in symbol else "USDT"
        if quote != "USDT":
            return False
    return True


async def get_symbol_universe(client, config):
    """
    Lista de símbolos filtrados por tipo (cripto), moneda de cotización y,
    opcionalmente, por liquidez. Con SCAN_ALL_SYMBOLS=True (default) se
    analizan TODAS las monedas de BingX, sin filtro de volumen — solo se
    sigue excluyendo lo que no es cripto (forex/índices/commodities/acciones
    tokenizadas) y, por defecto, lo que no cotiza en USDT.
    """
    all_symbols = await client.get_all_symbols_with_volume()
    scan_all = getattr(config, "SCAN_ALL_SYMBOLS", True)
    require_usdt_quote = getattr(config, "REQUIRE_USDT_QUOTE", True)
    min_vol = 0 if scan_all else config.MIN_24H_VOLUME_USDT
    filtered = [
        s["symbol"] for s in all_symbols
        if s["volume_24h_usdt"] >= min_vol
        and _is_valid_symbol(s["symbol"], config.NON_CRYPTO_PREFIXES, require_usdt_quote)
    ]
    log.info(
        "Universo de símbolos tras filtro: %d (SCAN_ALL_SYMBOLS=%s, min_vol=%s, require_usdt_quote=%s)",
        len(filtered), scan_all, min_vol, require_usdt_quote,
    )
    return filtered


async def get_top_n_symbols(client, config, n):
    """
    Subconjunto de mayor volumen 24h, para el loop RÁPIDO (más frecuente que
    el barrido completo). Mismos filtros de tipo/cotización que
    get_symbol_universe, pero ignora SCAN_ALL_SYMBOLS/MIN_24H_VOLUME_USDT
    a propósito — acá el orden por volumen ES el filtro.
    """
    all_symbols = await client.get_all_symbols_with_volume()
    require_usdt_quote = getattr(config, "REQUIRE_USDT_QUOTE", True)
    valid = [
        s for s in all_symbols
        if _is_valid_symbol(s["symbol"], config.NON_CRYPTO_PREFIXES, require_usdt_quote)
    ]
    valid.sort(key=lambda s: s["volume_24h_usdt"], reverse=True)
    top = [s["symbol"] for s in valid[:n]]
    log.info("Top %d símbolos por volumen para el loop rápido: %s...", n, top[:5])
    return top


async def _evaluate_one(client, symbol, config, semaphore):
    async with semaphore:
        try:
            ob_tf = getattr(config, "OB_TF", "15m")
            ob_engine_on = getattr(config, "ENABLE_OB_ENGINE", True)
            ob_reuses_htf_a = ob_engine_on and ob_tf == config.HTF_A_TF

            # Las 5 llamadas de un mismo símbolo no dependen entre sí -> se piden
            # en paralelo con gather en vez de una-tras-otra. El pacing/cooldown
            # compartido de exchange_client.py sigue gobernando la tasa REAL de
            # envío (esto no aumenta la carga total sobre BingX, solo evita que
            # una espere a la otra sin necesidad dentro de un mismo símbolo).
            htf_a_limit = 250 if ob_reuses_htf_a else 60
            fetches = {
                "entry": client.get_klines(symbol, config.ENTRY_TF, limit=150),
                "bias": client.get_klines(symbol, config.BIAS_TF, limit=120),
                "1h": client.get_klines(symbol, config.HTF_C_TF, limit=60),
                "15m": client.get_klines(symbol, config.HTF_A_TF, limit=htf_a_limit),
                "30m": client.get_klines(symbol, config.HTF_B_TF, limit=60),
            }
            if ob_engine_on and not ob_reuses_htf_a:
                fetches["ob"] = client.get_klines(symbol, ob_tf, limit=250)

            results = await asyncio.gather(*fetches.values())
            candles = dict(zip(fetches.keys(), results))

            candles_entry = candles["entry"]
            candles_bias = candles["bias"]
            candles_1h = candles["1h"]
            candles_15m = candles["15m"]
            candles_30m = candles["30m"]
            candles_ob = candles_15m if ob_reuses_htf_a else candles.get("ob")

            if len(candles_entry) < 80 or len(candles_bias) < 55:
                return None

            sig = evaluate_symbol(
                symbol, candles_entry, candles_bias, candles_1h,
                config, candles_15m, candles_30m, candles_ob,
            )

            # Cascada de confirmaciones finales: solo se consulta la API cuando
            # ya hay señal válida, para no gastar rate limit en 500+ símbolos.
            if sig.get("signal") is not None:
                sig = await confirm_with_order_flow(client, symbol, sig, config)
            if sig.get("signal") is not None:
                sig = await confirm_with_funding_oi(client, symbol, sig, config)

            return sig
        except Exception as e:
            log.error("Error evaluando %s: %s", symbol, e)
            return None


async def scan_universe(client, symbols, config):
    """Evalúa todos los símbolos con concurrencia acotada. Devuelve señales válidas."""
    semaphore = asyncio.Semaphore(config.SCAN_CONCURRENCY)
    tasks = [_evaluate_one(client, s, config, semaphore) for s in symbols]
    results = await asyncio.gather(*tasks)

    signals = [r for r in results if r and r.get("signal")]
    log.info("Ciclo de scan completo: %d símbolos evaluados, %d señales válidas",
              len(symbols), len(signals))
    return signals
