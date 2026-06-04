"""
notifications/telegram_notifier.py — Notificaciones Telegram QF×JP v3.4
=========================================================================
Mensajes ricos con todo el dashboard del bot.
"""
from __future__ import annotations
import logging, asyncio
from typing import Optional
import requests

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token   = token
        self.chat_id = chat_id

    def send(self, text: str, parse_mode: str = "Markdown") -> bool:
        url = TELEGRAM_API.format(token=self.token)
        try:
            r = requests.post(url, json={
                "chat_id":    self.chat_id,
                "text":       text[:4096],
                "parse_mode": parse_mode,
                "disable_web_page_preview": True,
            }, timeout=10)
            return r.status_code == 200
        except Exception as e:
            logger.error(f"[TG] Error: {e}")
            return False

    # ─── MENSAJES ESPECÍFICOS ─────────────────────────────────────────────────

    def signal_entry(self, signal, market_ctx=None) -> str:
        from strategy.qfxjp_signal import SignalResult
        s: SignalResult = signal
        r = s.ind

        icons = {"SUP": "⭐", "FUEL": "🔥", "STD": "📊"}
        icon  = icons.get(s.level, "📊")
        dir_icon = "🟢" if s.direction == "LONG" else "🔴"

        lines = [
            f"{icon} *{s.level} {s.direction}* — `{s.symbol}`",
            f"{dir_icon} Score: `{s.score}/100`  |  R:R `{s.rr1:.1f}`",
            f"",
            f"💰 Entry: `{s.price:.4f}`",
            f"🛑 SL:    `{s.sl_price:.4f}`",
            f"🎯 TP0.5: `{s.tp0_price:.4f}` _(25% parcial)_",
            f"🎯 TP1:   `{s.tp1_price:.4f}`",
            f"",
            f"📐 ATR: `{s.atr:.4f}` | Kelly: `{s.kelly_f*100:.1f}%` | Qty: `{s.quantity}`",
            f"📈 Régimen: `{s.regime}`",
            f"",
        ]

        if r:
            lines += [
                f"*── INDICADORES ──*",
                f"CVD: `{'▲' if r.cvd_rising else '▼'}{r.cvd:.0f}`  |  RSI: `{r.rsi_val:.0f}`  |  ADX: `{r.adx:.0f}`",
                f"VWAP: `{'SOBRE' if r.above_vwap else 'BAJO'}`  |  Vol: `{r.vol_pct}%`",
                f"Sesión: `{r.ses_label}`  |  AMD: `{'MANIP' if not r.circuit_ok else 'OK'}`",
            ]
            active = []
            if r.choch_bull or r.choch_bear: active.append("CHoCH")
            if r.bos_bull or r.bos_bear:     active.append("BoS")
            if r.liq_bull_sweep or r.liq_bear_sweep: active.append("Sweep")
            if r.sq_bull or r.sq_bear:       active.append("SQ🔥")
            if r.in_bull_fvg or r.in_bear_fvg: active.append("FVG")
            if r.in_bull_ob or r.in_bear_ob: active.append("OB")
            if r.dp_buy or r.dp_sell:        active.append("DarkPool")
            if r.oi_conf_long or r.oi_conf_short: active.append("OI✓")
            if active:
                lines.append(f"🔔 Activos: `{' | '.join(active)}`")

            if r.poc:
                lines.append(f"📊 VP: POC=`{r.poc:.4f}` VAH=`{r.vah:.4f}` VAL=`{r.val:.4f}`")

        htf_l = s.ind.__dict__.get("htf_score_long",0) if s.ind else 0
        htf_s = s.ind.__dict__.get("htf_score_short",0) if s.ind else 0
        lines.append(f"🕐 HTF: `{htf_l}/3▲` `{htf_s}/3▼`  |  Conv: `{s.conv_long}/12▲` `{s.conv_short}/12▼`")

        if market_ctx:
            lines += ["", f"_Ctx: {market_ctx.summary}_"]

        msg = "\n".join(lines)
        self.send(msg)
        return msg

    def signal_none(self, symbol: str, sc_l: int, sc_s: int, reason: str):
        if sc_l >= 45 or sc_s >= 45:
            self.send(f"📉 `{symbol}` Sin señal — L:`{sc_l}` S:`{sc_s}`\n_{reason}_")

    def position_closed(self, symbol: str, direction: str, pnl_pct: float,
                        reason: str = "TP/SL"):
        icon = "✅" if pnl_pct > 0 else "❌"
        self.send(
            f"{icon} *Posición cerrada* — `{symbol}`\n"
            f"Dir: `{direction}` | PnL: `{pnl_pct:+.2f}%`\n"
            f"Razón: _{reason}_"
        )

    def partial_tp(self, symbol: str, direction: str, tp_price: float):
        self.send(
            f"🎯 *Partial TP* — `{symbol}`\n"
            f"Dir: `{direction}` | TP0.5 @ `{tp_price:.4f}` → SL a *Breakeven*"
        )

    def risk_alert(self, reason: str):
        self.send(f"⚠️ *RIESGO ALERTA*\n{reason}")

    def circuit_breaker(self, reason: str):
        self.send(f"🔴 *CIRCUIT BREAKER*\n{reason}")

    def daily_report(self, stats: dict, positions: list):
        lines = [
            "📊 *REPORTE DIARIO — QF×JP v3.4*",
            f"📅 Fecha: `{stats.get('date','?')}`",
            f"💰 Balance: `{stats.get('balance','?')} USDT`",
            f"📈 PnL hoy: `{stats.get('total_pnl',0):+.2f}%`",
            f"🎯 Trades: `{stats.get('trades',0)}` | W:`{stats.get('wins',0)}` L:`{stats.get('losses',0)}`",
            f"📉 Win rate: `{stats.get('win_rate',0):.1f}%`",
            f"📉 Drawdown: `{stats.get('dd_pct',0):.2f}%`",
        ]
        if positions:
            lines += ["", "*── POSICIONES ABIERTAS ──*"]
            for p in positions:
                pnl_icon = "✅" if p["pnl_pct"] > 0 else "🔴"
                lines.append(
                    f"{pnl_icon} `{p['symbol']}` {p['dir']} [{p['level']}] "
                    f"PnL=`{p['pnl_pct']:+.1f}%` {p['age_min']:.0f}min"
                )
        self.send("\n".join(lines))

    def bot_started(self, symbols: list, cfg):
        self.send(
            f"🚀 *QF×JP v3.4 Bot INICIADO*\n"
            f"Símbolos: `{', '.join(symbols)}`\n"
            f"Score min: `STD={cfg.SCORE_STD} FUEL={cfg.SCORE_FUEL} SUP={cfg.SCORE_SUP}`\n"
            f"Leverage: `{cfg.LEVERAGE}x` | Risk: `{cfg.RISK_PER_TRADE*100:.1f}%`\n"
            f"Max trades: `{cfg.MAX_OPEN_TRADES}`\n"
            f"Railway: `ONLINE ✅`"
        )

    def scan_summary(self, results: list[dict]):
        """Envía un resumen rápido del scan de todos los símbolos."""
        if not results: return
        lines = ["🔍 *SCAN* QF×JP v3.4"]
        for r in results:
            icon = "⭐" if r.get("level")=="SUP" else "🔥" if r.get("level")=="FUEL" else "📊" if r.get("level")=="STD" else "•"
            lines.append(
                f"{icon} `{r['symbol']:12}` "
                f"L:`{r.get('sc_l',0):3d}` S:`{r.get('sc_s',0):3d}` "
                f"`{r.get('ses','?')}` `{r.get('regime','?')}`"
            )
        self.send("\n".join(lines))
