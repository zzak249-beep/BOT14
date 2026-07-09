"""
Analizador de rentabilidad — lee el journal JSON del bot y calcula las
métricas que importan para decidir qué tunear: win rate, expectancy,
profit factor, split por engine (unicorn vs order_block), por setup_key,
por símbolo y por hora del día, más el desglose de señales rechazadas.

USO (dos opciones):
  1. Local, con el journal bajado del Volume de Railway:
       python3 analyze_journal.py unicorn_st_journal.json
  2. Dentro del contenedor de Railway (shell del servicio):
       python3 analyze_journal.py /data/unicorn_st_journal.json

Solo stdlib, sin dependencias.
"""
import json
import sys
from collections import defaultdict
from datetime import datetime


def load(path):
    with open(path, "r") as f:
        data = json.load(f)
    return data if isinstance(data, list) else []


def fmt_pnl(x):
    return f"{x:+.4f}"


def analyze(entries):
    closed = [e for e in entries if e.get("event") == "position_closed"]
    opened = [e for e in entries if e.get("event") == "position_opened"]
    rejected = [e for e in entries if e.get("event") == "signal_rejected"]

    print("=" * 72)
    print(f"JOURNAL: {len(entries)} entradas | {len(opened)} aperturas | "
          f"{len(closed)} cierres | {len(rejected)} señales rechazadas")
    print("=" * 72)

    if not closed:
        print("\nSin trades cerrados todavía — nada que medir.")
        _rejections(rejected)
        return

    wins = [e for e in closed if e.get("is_win")]
    losses = [e for e in closed if not e.get("is_win")]
    pnls = [float(e.get("pnl", 0)) for e in closed]
    total = sum(pnls)
    gross_win = sum(p for p in pnls if p > 0)
    gross_loss = -sum(p for p in pnls if p < 0)
    wr = len(wins) / len(closed)
    avg_win = gross_win / len(wins) if wins else 0.0
    avg_loss = gross_loss / len(losses) if losses else 0.0
    expectancy = total / len(closed)
    pf = gross_win / gross_loss if gross_loss > 0 else float("inf")
    # WR mínimo para breakeven dado el ratio realizado avg_win/avg_loss
    be_wr = avg_loss / (avg_win + avg_loss) if (avg_win + avg_loss) > 0 else None

    print(f"""
GLOBAL
  Trades cerrados:   {len(closed)}  ({len(wins)}W / {len(losses)}L)
  Win rate:          {wr:.1%}
  PnL total:         {fmt_pnl(total)} USDT
  Expectancy/trade:  {fmt_pnl(expectancy)} USDT
  Avg win / loss:    {fmt_pnl(avg_win)} / {fmt_pnl(-avg_loss)}
  Profit factor:     {pf:.2f}""")
    if be_wr is not None:
        print(f"  WR de breakeven:   {be_wr:.1%}  (con el ratio win/loss REALIZADO — "
              f"{'por ENCIMA' if wr > be_wr else 'por DEBAJO'} ahora)")
    if len(closed) < 30:
        print(f"  ⚠️  Muestra chica ({len(closed)} trades) — nada de esto es "
              f"concluyente antes de ~30-50 trades.")

    # ── Por engine ────────────────────────────────────────────────────
    # El engine no viaja en position_closed: se resuelve cruzando con la
    # apertura del mismo símbolo/setup_key más cercana anterior.
    open_by_key = defaultdict(list)
    for o in opened:
        open_by_key[(o.get("symbol"), o.get("setup_key"))].append(o)

    def engine_of(c):
        cands = open_by_key.get((c.get("symbol"), c.get("setup_key")))
        if cands:
            return cands[-1].get("engine") or "desconocido"
        return "desconocido"

    _group("POR ENGINE", closed, engine_of)
    _group("POR SETUP", closed, lambda c: c.get("setup_key", "desconocido"))
    _group("POR SÍMBOLO (top 15 por nº de trades)", closed,
           lambda c: c.get("symbol", "?"), top=15)
    _group("POR HORA UTC de cierre", closed, _hour_of, sort_key=lambda k: k)
    _group("POR LADO", closed, lambda c: c.get("side") or "?")

    _rejections(rejected)


def _hour_of(c):
    ts = c.get("timestamp", "")
    try:
        return f"{datetime.fromisoformat(ts).hour:02d}h"
    except ValueError:
        return "??"


def _group(title, closed, keyfn, top=None, sort_key=None):
    groups = defaultdict(list)
    for c in closed:
        groups[keyfn(c)].append(float(c.get("pnl", 0)))

    rows = []
    for k, pnls in groups.items():
        n = len(pnls)
        w = sum(1 for p in pnls if p > 0)
        rows.append((k, n, w / n, sum(pnls)))

    if sort_key:
        rows.sort(key=lambda r: sort_key(r[0]))
    else:
        rows.sort(key=lambda r: r[3], reverse=True)
    if top:
        rows = rows[:top]

    print(f"\n{title}")
    for k, n, wr, pnl in rows:
        flag = " ←💀" if n >= 10 and wr < 0.35 else ""
        print(f"  {str(k)[:52]:<52} n={n:<4} WR={wr:>5.1%}  PnL={fmt_pnl(pnl)}{flag}")


def _rejections(rejected):
    if not rejected:
        return
    counts = defaultdict(int)
    for r in rejected:
        reason = str(r.get("reason", "?"))
        # agrupar por prefijo (antes de ':') para no fragmentar
        counts[reason.split(":")[0].strip()] += 1
    print("\nSEÑALES RECHAZADAS (por qué se pierden trades)")
    for reason, n in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {reason:<40} {n}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python3 analyze_journal.py <ruta_al_journal.json>")
        sys.exit(1)
    analyze(load(sys.argv[1]))
