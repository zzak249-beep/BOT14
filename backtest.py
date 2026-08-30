"""
backtest.py
-----------
Backtesting histórico de la estrategia RSI + SuperTrend contra datos REALES
de BingX, usando la MISMA función `strategy.compute_signals` que usa el bot
en vivo (así el backtest no se puede desincronizar de lo que realmente
operaría el bot).

No necesita API keys: las velas históricas son un endpoint público.

Uso:
    python backtest.py --symbol BTC/USDT --timeframe 15m --days 60
    python backtest.py --symbol ETH/USDT --timeframe 15m --days 90 --trade-amount 200 --output resultados.csv

Todos los parámetros de la estrategia (--rsi-length, --st-factor, etc.) son
ajustables por si quieres explorar variantes antes de decidir la configuración
final del bot en vivo.
"""

import argparse
import os
import sys

import pandas as pd
from dotenv import load_dotenv

from exchange_client import ExchangeClient
from strategy import compute_signals

load_dotenv()


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--symbol", default=os.getenv("SYMBOL", "BTC/USDT"))
    p.add_argument("--timeframe", default=os.getenv("TIMEFRAME", "15m"))
    p.add_argument("--days", type=int, default=60, help="Días de histórico a descargar (por defecto 60)")
    p.add_argument("--trade-amount", type=float, default=float(os.getenv("TRADE_AMOUNT_USDT", 100)),
                    help="USDT simulados por operación")
    p.add_argument("--fee-pct", type=float, default=0.1,
                    help="Comisión por operación en %% (se aplica en la entrada y en la salida). BingX spot suele rondar 0.1%%.")
    p.add_argument("--rsi-length", type=int, default=int(os.getenv("RSI_LENGTH", 10)))
    p.add_argument("--signal-length", type=int, default=int(os.getenv("SIGNAL_LENGTH", 10)))
    p.add_argument("--trigger-level", type=float, default=float(os.getenv("TRIGGER_LEVEL", 50)))
    p.add_argument("--target-cross-count", type=int, default=int(os.getenv("TARGET_CROSS_COUNT", 2)))
    p.add_argument("--atr-period", type=int, default=int(os.getenv("ATR_PERIOD", 10)))
    p.add_argument("--st-factor", type=float, default=float(os.getenv("ST_FACTOR", 2.5)))
    p.add_argument("--output", default=None, help="Ruta de un .csv donde guardar el detalle de cada operación")
    return p.parse_args()


def fetch_history(symbol: str, timeframe: str, days: int) -> pd.DataFrame:
    exchange = ExchangeClient("", "", demo=False)  # datos públicos: no requiere API keys
    tf_ms = exchange.exchange.parse_timeframe(timeframe) * 1000
    since = exchange.exchange.milliseconds() - days * 86400 * 1000

    all_rows = []
    seen = set()
    print(f"Descargando histórico de {symbol} ({timeframe}, últimos {days} días) desde BingX...")
    for _ in range(200):  # límite de seguridad de páginas
        batch = exchange.fetch_ohlcv_raw(symbol, timeframe, since=since, limit=500)
        if not batch:
            break
        new_rows = [r for r in batch if r[0] not in seen]
        if not new_rows:
            break
        all_rows.extend(new_rows)
        seen.update(r[0] for r in new_rows)
        since = batch[-1][0] + tf_ms
        if len(batch) < 500 or since >= exchange.exchange.milliseconds():
            break

    if not all_rows:
        raise RuntimeError(f"No se recibieron velas para {symbol} en {timeframe}. ¿Símbolo o timeframe correctos?")

    all_rows.sort(key=lambda r: r[0])
    df = pd.DataFrame(all_rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    print(f"  -> {len(df)} velas descargadas ({df['datetime'].iloc[0]} a {df['datetime'].iloc[-1]})\n")
    return df


def simulate_trades(df: pd.DataFrame, signals: pd.DataFrame, trade_amount_usdt: float, fee_pct: float):
    """Replica exactamente la decisión del bot en vivo: compra si no hay
    posición y aparece special_buy; vende si hay posición y aparece sell_signal."""
    trades = []
    in_position = False
    entry_price = None
    entry_time = None

    for i in range(len(signals)):
        row = signals.iloc[i]
        ts = df["datetime"].iloc[i]
        if not in_position and bool(row["special_buy"]):
            in_position = True
            entry_price = float(row["close"])
            entry_time = ts
        elif in_position and bool(row["sell_signal"]):
            exit_price = float(row["close"])
            gross_return = exit_price / entry_price - 1
            net_return = gross_return - 2 * (fee_pct / 100)  # comisión de entrada + salida
            pnl_usdt = trade_amount_usdt * net_return
            trades.append(
                {
                    "entry_time": entry_time,
                    "exit_time": ts,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "return_pct": net_return * 100,
                    "pnl_usdt": pnl_usdt,
                }
            )
            in_position = False
            entry_price = None
            entry_time = None

    open_position = {"entry_time": entry_time, "entry_price": entry_price} if in_position else None
    return trades, open_position


def print_report(trades, open_position, trade_amount_usdt, symbol, timeframe, days):
    print("=" * 66)
    print(f"BACKTEST: {symbol} | {timeframe} | últimos {days} días | {trade_amount_usdt} USDT/operación")
    print("=" * 66)

    if not trades:
        print("\nNo se generó ninguna operación completa en el periodo analizado.")
    else:
        n = len(trades)
        wins = [t for t in trades if t["pnl_usdt"] > 0]
        losses = [t for t in trades if t["pnl_usdt"] <= 0]
        total_pnl = sum(t["pnl_usdt"] for t in trades)
        win_rate = len(wins) / n * 100

        equity, peak, max_dd = 0.0, 0.0, 0.0
        for t in trades:
            equity += t["pnl_usdt"]
            peak = max(peak, equity)
            max_dd = max(max_dd, peak - equity)

        print(f"\nOperaciones completas: {n}")
        print(f"Ganadoras: {len(wins)} ({win_rate:.1f}%)  |  Perdedoras: {len(losses)}")
        print(f"Resultado neto total: {total_pnl:+.2f} USDT  ({total_pnl / trade_amount_usdt * 100:+.2f}% acumulado sobre {trade_amount_usdt} USDT por operación)")
        if wins:
            print(f"Ganancia media: {sum(t['pnl_usdt'] for t in wins) / len(wins):+.2f} USDT")
        if losses:
            print(f"Pérdida media: {sum(t['pnl_usdt'] for t in losses) / len(losses):+.2f} USDT")
        print(f"Máximo drawdown (sobre la curva de resultados acumulados): {max_dd:.2f} USDT")

        print(f"\n{'Entrada (UTC)':<18}{'Salida (UTC)':<18}{'Precio in':<12}{'Precio out':<12}{'%':<9}{'USDT'}")
        for t in trades:
            print(
                f"{str(t['entry_time'])[:16]:<18}{str(t['exit_time'])[:16]:<18}"
                f"{t['entry_price']:<12.4f}{t['exit_price']:<12.4f}{t['return_pct']:+.2f}%   {t['pnl_usdt']:+.2f}"
            )

    if open_position:
        print(
            f"\n⚠️  Posición todavía abierta al final del periodo "
            f"(entrada: {open_position['entry_price']} el {open_position['entry_time']}) "
            f"— no se cuenta en las estadísticas anteriores."
        )

    print("\n" + "=" * 66)
    print("Esto es una simulación histórica sobre datos pasados. NO garantiza")
    print("resultados futuros ni tiene en cuenta slippage, huecos de liquidez")
    print("o rechazos de órdenes que sí pueden ocurrir en real.")
    print("=" * 66)


def main():
    args = parse_args()
    df = fetch_history(args.symbol, args.timeframe, args.days)
    signals = compute_signals(
        df,
        rsi_length=args.rsi_length,
        signal_length=args.signal_length,
        trigger_level=args.trigger_level,
        target_cross_count=args.target_cross_count,
        atr_period=args.atr_period,
        st_factor=args.st_factor,
    )
    trades, open_position = simulate_trades(df, signals, args.trade_amount, args.fee_pct)
    print_report(trades, open_position, args.trade_amount, args.symbol, args.timeframe, args.days)

    if args.output and trades:
        pd.DataFrame(trades).to_csv(args.output, index=False)
        print(f"\nDetalle de operaciones guardado en: {args.output}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nError: {exc}", file=sys.stderr)
        sys.exit(1)
