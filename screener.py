"""
screener.py
-----------
Escanea TODOS los pares de BingX Spot contra una moneda de cotización
(por defecto USDT) con la MISMA estrategia RSI + SuperTrend que usa el bot
en vivo, para encontrar:
  1) qué monedas tienen la señal de COMPRA activa justo en la última vela
     cerrada, y
  2) cómo se habría comportado la estrategia en cada una recientemente
     (ventana corta: una sola llamada de velas por moneda, no un backtest
     profundo — para eso usa backtest.py sobre la moneda que te interese).

No necesita API keys: usa solo datos públicos de mercado.

Uso:
    python screener.py
    python screener.py --quote USDT --min-volume-usdt 500000 --candles 500
    python screener.py --top 20 --output screener_resultados.csv
    python screener.py --limit-symbols 15   # prueba rápida con pocas monedas
"""

import argparse
import sys

import pandas as pd
from dotenv import load_dotenv

from backtest import simulate_trades
from exchange_client import ExchangeClient
from strategy import compute_signals

load_dotenv()


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--quote", default="USDT", help="Moneda de cotización a analizar (por defecto USDT)")
    p.add_argument("--timeframe", default="15m")
    p.add_argument("--candles", type=int, default=500,
                    help="Velas históricas por moneda en UNA sola llamada (500x15m ≈ 5.2 días). Por defecto 500 (valor conservador y seguro para cualquier exchange).")
    p.add_argument("--min-volume-usdt", type=float, default=200000,
                    help="Descarta monedas con menos de este volumen en 24h (evita pares ilíquidos/ruidosos). 0 = sin filtro.")
    p.add_argument("--trade-amount", type=float, default=100.0)
    p.add_argument("--fee-pct", type=float, default=0.1)
    p.add_argument("--rsi-length", type=int, default=10)
    p.add_argument("--signal-length", type=int, default=10)
    p.add_argument("--trigger-level", type=float, default=50)
    p.add_argument("--target-cross-count", type=int, default=2)
    p.add_argument("--atr-period", type=int, default=10)
    p.add_argument("--st-factor", type=float, default=2.5)
    p.add_argument("--top", type=int, default=30, help="Cuántas filas mostrar en el resumen final por pantalla")
    p.add_argument("--output", default=None, help="Ruta .csv donde guardar TODOS los resultados (no solo el top)")
    p.add_argument("--limit-symbols", type=int, default=None, help="Analiza como mucho N monedas (útil para probar rápido)")
    p.add_argument("--quiet", action="store_true", help="No imprime el progreso símbolo a símbolo")
    return p.parse_args()


def get_candidate_symbols(exchange_client: ExchangeClient, quote: str, min_volume_usdt: float):
    print("Cargando lista de mercados de BingX...")
    exchange_client.ensure_markets_loaded()
    markets = exchange_client.exchange.markets
    symbols = [
        s for s, m in markets.items()
        if m.get("spot") and m.get("active", True) is not False and m.get("quote") == quote
    ]
    print(f"  -> {len(symbols)} pares spot cotizados en {quote}")

    if min_volume_usdt > 0 and symbols:
        print(f"Consultando volumen 24h para descartar pares con menos de {min_volume_usdt:,.0f} {quote}...")
        tickers = exchange_client._with_retries(exchange_client.exchange.fetch_tickers, symbols)
        filtered = [s for s in symbols if (tickers.get(s, {}).get("quoteVolume") or 0) >= min_volume_usdt]
        print(f"  -> {len(filtered)} pares superan el volumen mínimo")
        symbols = filtered

    return sorted(symbols)


def analyze_symbol(exchange_client: ExchangeClient, symbol: str, args):
    df = exchange_client.fetch_ohlcv_df(symbol, args.timeframe, args.candles)
    min_bars = max(args.rsi_length + args.signal_length, args.atr_period) + 10
    if len(df) < min_bars:
        return None

    signals = compute_signals(
        df,
        rsi_length=args.rsi_length,
        signal_length=args.signal_length,
        trigger_level=args.trigger_level,
        target_cross_count=args.target_cross_count,
        atr_period=args.atr_period,
        st_factor=args.st_factor,
    )
    last = signals.iloc[-1]

    trades, open_position = simulate_trades(df, signals, args.trade_amount, args.fee_pct)
    total_pnl = sum(t["pnl_usdt"] for t in trades)
    wins = [t for t in trades if t["pnl_usdt"] > 0]
    win_rate = (len(wins) / len(trades) * 100) if trades else None

    return {
        "symbol": symbol,
        "close": float(last["close"]),
        "rsi": round(float(last["rsi"]), 1),
        "senal_compra_ahora": bool(last["special_buy"]),
        "tendencia": "alcista" if bool(last["trend_up"]) else "bajista",
        "operaciones_periodo": len(trades),
        "win_rate_pct": round(win_rate, 1) if win_rate is not None else None,
        "resultado_neto_pct": round(total_pnl / args.trade_amount * 100, 2) if args.trade_amount else None,
        "posicion_abierta_ahora": open_position is not None,
    }


def main():
    args = parse_args()
    exchange_client = ExchangeClient("", "", demo=False)  # datos públicos, sin API keys

    symbols = get_candidate_symbols(exchange_client, args.quote, args.min_volume_usdt)
    if args.limit_symbols:
        symbols = symbols[: args.limit_symbols]
    if not symbols:
        print("No se encontró ninguna moneda que cumpla los filtros.")
        return

    results = []
    errors = 0
    print(f"\nAnalizando {len(symbols)} monedas ({args.candles} velas de {args.timeframe} cada una, una sola llamada por moneda)...\n")
    for i, symbol in enumerate(symbols, 1):
        status = "OK"
        try:
            r = analyze_symbol(exchange_client, symbol, args)
            if r:
                results.append(r)
            else:
                status = "sin histórico suficiente"
        except Exception as exc:
            errors += 1
            status = f"error: {exc}"
        if not args.quiet:
            print(f"  [{i}/{len(symbols)}] {symbol:<15} {status}")

    if not results:
        print("\nNo se pudo analizar ninguna moneda con éxito.")
        return

    df_out = pd.DataFrame(results)
    df_out.sort_values(["senal_compra_ahora", "resultado_neto_pct"], ascending=[False, False], inplace=True)

    print("\n" + "=" * 92)
    print(f"RESUMEN — {len(results)} monedas analizadas ({errors} con error) — ventana de {args.candles} velas de {args.timeframe}")
    print("=" * 92)

    activos = df_out[df_out["senal_compra_ahora"]]
    if len(activos):
        print(f"\n🟢 SEÑAL DE COMPRA ACTIVA EN LA ÚLTIMA VELA CERRADA ({len(activos)} moneda(s)):")
        for _, row in activos.iterrows():
            print(f"   {row['symbol']:<15} precio={row['close']:<14.6f} RSI={row['rsi']}")
    else:
        print("\n(Ninguna moneda tiene la señal de compra activa justo en la última vela cerrada ahora mismo)")

    print(f"\nTop {min(args.top, len(df_out))} por resultado neto (%) en la ventana analizada:\n")
    cols = ["symbol", "close", "rsi", "tendencia", "operaciones_periodo", "win_rate_pct", "resultado_neto_pct", "posicion_abierta_ahora"]
    print(df_out[cols].head(args.top).to_string(index=False))

    if args.output:
        df_out.to_csv(args.output, index=False)
        print(f"\nResultados completos ({len(df_out)} monedas) guardados en: {args.output}")

    print("\n" + "=" * 92)
    print("Esto es un escaneo corto de muchas monedas a la vez, no un backtest profundo,")
    print("y no garantiza resultados futuros. Antes de operar en real una moneda que te")
    print("interese, valídala a fondo con: python backtest.py --symbol <PAR> --days 60")
    print("=" * 92)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nError: {exc}", file=sys.stderr)
        sys.exit(1)
