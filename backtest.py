"""
Backtester — QF Machine × JP Fusion Bot
Valida la estrategia en datos históricos antes de operar en vivo.
Uso: python backtest.py --symbol BTC-USDT --days 30
"""
import argparse
import asyncio
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from signals import QFSignalEngine
from config  import SIGNAL_CFG, RISK_CFG

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("Backtest")


class Backtester:
    def __init__(self, df_3m: pd.DataFrame, df_htf: pd.DataFrame,
                 min_conviction: int = 6):
        self.df_3m  = df_3m.reset_index(drop=True)
        self.df_htf = df_htf.reset_index(drop=True)
        self.engine = QFSignalEngine(SIGNAL_CFG)
        self.min_conv = min_conviction

    def run(self) -> dict:
        """
        Simula barra a barra: en cada barra calcula señal,
        si hay señal abre posición virtual y la gestiona.
        """
        trades   = []
        equity   = RISK_CFG["initial_equity"]
        peak_eq  = equity
        max_dd   = 0.0
        position = None  # dict con info de posición abierta

        WARMUP = 120   # barras mínimas para cálculos

        for i in range(WARMUP, len(self.df_3m) - 1):
            df_slice   = self.df_3m.iloc[:i + 1]
            price_next = float(self.df_3m.iloc[i + 1]["close"])

            # ── Gestionar posición abierta ──────────────────────
            if position is not None:
                hi  = float(self.df_3m.iloc[i + 1]["high"])
                lo  = float(self.df_3m.iloc[i + 1]["low"])
                exited, exit_price, reason = self._check_exit(position, hi, lo, price_next)
                if exited:
                    if position["dir"] == "LONG":
                        pnl = (exit_price - position["entry"]) * position["qty"]
                    else:
                        pnl = (position["entry"] - exit_price) * position["qty"]

                    equity += pnl
                    peak_eq = max(peak_eq, equity)
                    dd = (peak_eq - equity) / peak_eq * 100
                    max_dd = max(max_dd, dd)

                    trades.append({
                        "bar":       i,
                        "direction": position["dir"],
                        "tier":      position["tier"],
                        "conviction":position["conv"],
                        "entry":     position["entry"],
                        "exit":      exit_price,
                        "reason":    reason,
                        "pnl":       round(pnl, 4),
                        "equity":    round(equity, 4),
                    })
                    position = None
                else:
                    # Trailing stop
                    atr_val = self._atr(df_slice, 10)
                    position["sl"] = self._trail(position, price_next, atr_val)
                continue   # si hay posición abierta no buscar nueva señal

            # ── Calcular señal ───────────────────────────────────
            try:
                htf_slice = self._resample_htf(df_slice)
                if len(htf_slice) < 20:
                    continue
                sig = self.engine.compute(df_slice, htf_slice)
            except Exception:
                continue

            if sig.direction == "FLAT" or sig.tier == "NONE":
                continue
            if sig.conviction < self.min_conv:
                continue

            # Sizing (sin leverage para backtest conservador)
            sl_dist = abs(sig.entry_price - sig.sl_price)
            if sl_dist <= 0:
                continue

            risk_map = {"SUPREMA": 1.5, "FUEL": 1.0, "STD": 0.5}
            risk_pct = min(risk_map.get(sig.tier, 0.5), 2.0) / 100
            risk_usdt= equity * risk_pct
            qty      = risk_usdt / sl_dist

            # R:R para TP
            rr_map = {"SUPREMA": 3.0, "FUEL": 2.5, "STD": 2.0}
            rr     = rr_map.get(sig.tier, 2.0)
            if sig.direction == "LONG":
                tp = sig.entry_price + sl_dist * rr
            else:
                tp = sig.entry_price - sl_dist * rr

            position = {
                "dir":   sig.direction,
                "tier":  sig.tier,
                "conv":  sig.conviction,
                "entry": sig.entry_price,
                "sl":    sig.sl_price,
                "tp":    tp,
                "qty":   qty,
            }

        return self._summary(trades, equity, peak_eq, max_dd)

    def _check_exit(self, pos: dict, hi: float, lo: float, close: float):
        if pos["dir"] == "LONG":
            if lo <= pos["sl"]:
                return True, pos["sl"], "stop_loss"
            if hi >= pos["tp"]:
                return True, pos["tp"], "take_profit"
        else:
            if hi >= pos["sl"]:
                return True, pos["sl"], "stop_loss"
            if lo <= pos["tp"]:
                return True, pos["tp"], "take_profit"
        return False, close, ""

    def _trail(self, pos: dict, price: float, atr: float, mult: float = 1.5):
        if pos["dir"] == "LONG":
            proposed = price - atr * mult
            return max(proposed, pos["sl"])
        else:
            proposed = price + atr * mult
            return min(proposed, pos["sl"])

    @staticmethod
    def _atr(df: pd.DataFrame, n: int) -> float:
        hl = df["high"] - df["low"]
        return float(hl.rolling(n).mean().iloc[-1]) if len(df) >= n else float(hl.mean())

    @staticmethod
    def _resample_htf(df: pd.DataFrame, factor: int = 5) -> pd.DataFrame:
        """Resamplea 3m → 15m aproximando (cada 5 velas)"""
        rows = []
        for start in range(0, len(df) - factor + 1, factor):
            chunk = df.iloc[start:start + factor]
            rows.append({
                "open":   float(chunk["open"].iloc[0]),
                "high":   float(chunk["high"].max()),
                "low":    float(chunk["low"].min()),
                "close":  float(chunk["close"].iloc[-1]),
                "volume": float(chunk["volume"].sum()),
            })
        return pd.DataFrame(rows)

    def _summary(self, trades: list, final_eq: float, peak_eq: float, max_dd: float) -> dict:
        if not trades:
            return {"error": "Sin operaciones en el período"}

        df = pd.DataFrame(trades)
        wins   = df[df["pnl"] > 0]
        losses = df[df["pnl"] <= 0]
        total  = len(df)
        win_r  = len(wins) / total * 100

        avg_win  = float(wins["pnl"].mean()) if len(wins) > 0 else 0
        avg_loss = float(losses["pnl"].mean()) if len(losses) > 0 else 0
        profit_factor = (wins["pnl"].sum() / abs(losses["pnl"].sum())
                         if losses["pnl"].sum() != 0 else float("inf"))

        # Por tier
        tier_stats = {}
        for tier in ["SUPREMA", "FUEL", "STD"]:
            sub = df[df["tier"] == tier]
            if len(sub):
                w = sub[sub["pnl"] > 0]
                tier_stats[tier] = {
                    "trades":   len(sub),
                    "win_rate": round(len(w) / len(sub) * 100, 1),
                    "total_pnl":round(float(sub["pnl"].sum()), 2),
                }

        return {
            "total_trades":     total,
            "win_rate_pct":     round(win_r, 1),
            "total_pnl":        round(float(df["pnl"].sum()), 2),
            "final_equity":     round(final_eq, 2),
            "peak_equity":      round(peak_eq, 2),
            "max_drawdown_pct": round(max_dd, 2),
            "profit_factor":    round(profit_factor, 2),
            "avg_win":          round(avg_win, 2),
            "avg_loss":         round(avg_loss, 2),
            "expectancy":       round(float(df["pnl"].mean()), 4),
            "by_tier":          tier_stats,
        }


# ── CSV loader para datos históricos ─────────────────────────
def load_csv(path: str) -> pd.DataFrame:
    """
    Acepta CSV con columnas: open_time,open,high,low,close,volume
    open_time en milliseconds o formato ISO.
    """
    df = pd.read_csv(path)
    df.columns = [c.lower() for c in df.columns]
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col])
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="QF Bot Backtester")
    parser.add_argument("--file3m",  required=True, help="CSV 3m datos históricos")
    parser.add_argument("--file15m", required=False, help="CSV 15m (opcional, se resamplea si no se da)")
    parser.add_argument("--conviction", type=int, default=6, help="Convicción mínima (default 6)")
    parser.add_argument("--out", default="logs/backtest_result.json", help="Archivo resultado")
    args = parser.parse_args()

    df3  = load_csv(args.file3m)
    df15 = load_csv(args.file15m) if args.file15m else None

    bt  = Backtester(df3, df15 if df15 is not None else df3, min_conviction=args.conviction)
    res = bt.run()

    print("\n" + "="*50)
    print("  QF BOT — RESULTADO BACKTEST")
    print("="*50)
    for k, v in res.items():
        if k != "by_tier":
            print(f"  {k:25s}: {v}")
    if "by_tier" in res:
        print("\n  Por Tier:")
        for tier, stats in res["by_tier"].items():
            print(f"    {tier}: {stats}")
    print("="*50)

    Path(args.out).parent.mkdir(exist_ok=True)
    Path(args.out).write_text(json.dumps(res, indent=2))
    print(f"\n  Guardado en: {args.out}\n")
