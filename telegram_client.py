"""
QF×JP Bot v6.0 — telegram_client.py
Notificaciones ricas con composite score breakdown.
"""
import asyncio
import logging
from typing import Optional

import aiohttp

log = logging.getLogger("TELEGRAM")


class TelegramClient:
    def __init__(self, token: str, chat_id: str):
        self._token   = token
        self._chat_id = chat_id
        self._base    = f"https://api.telegram.org/bot{token}"
        self._session: Optional[aiohttp.ClientSession] = None

    async def _sess(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            )
        return self._session

    async def send_message(self, text: str):
        if not self._token or not self._chat_id:
            return
        try:
            s = await self._sess()
            async with s.post(
                f"{self._base}/sendMessage",
                json={"chat_id": self._chat_id, "text": text,
                      "parse_mode": "Markdown", "disable_web_page_preview": True}
            ) as r:
                if r.status != 200:
                    log.error(f"TG error {r.status}: {await r.text()}")
        except Exception as e:
            log.error(f"TG send error: {e}")

    async def send_entry(
        self, symbol: str, sig: dict, price: float,
        size: float, order_id: str, mctx: dict
    ):
        d    = sig["direction"]
        emoj = "🟢" if d == "LONG" else "🔴"
        tier_emoj = {"SUP": "⚡", "FUEL": "🔥", "STD": "✅"}.get(sig["tier"], "✅")

        comp = sig["comp_bull"] if d == "LONG" else sig["comp_bear"]
        final = sig.get("final_score", comp)

        bar_final = self._bar(final)
        bar_score = self._bar(sig["norm_score"])
        bar_cvd   = self._bar((sig["cvd_norm"] + 1) / 2)
        bar_mom   = self._bar((sig["momentum"] + 200) / 400)
        bar_decay = self._bar(sig["decay_ratio"])
        bar_edge  = self._bar((sig.get("edge_score", 0) + 1) / 2)

        # Edge detail
        ed = sig.get("edge_detail", {})
        edge_lines = []
        for k, label in [
            ("fvg","FVG"), ("ob","OB"), ("bos","BOS"), ("choch","CHoCH"),
            ("liq","LIQ SWEEP"), ("cvd_div","CVD DIV"), ("d_exh","DELTA EXH"),
            ("dark","DARK POOL"), ("pattern","PATRÓN"),
        ]:
            val = ed.get(k, "")
            if val:
                edge_lines.append(f"    ▸ {label}: `{val}`")
        edge_str = "\n".join(edge_lines) if edge_lines else "    ▸ Sin señales edge activas"

        text = (
            f"{emoj} *{d} {symbol}* {tier_emoj}{sig['tier']}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 Entrada: `{price:.6f}` | Tamaño: `{size:.4f}`\n"
            f"🛑 SL: `{sig['sl']:.6f}` | 🎯 TP: `{sig['tp']:.6f}`\n"
            f"🏅 Conv: `{sig['conviction']}/10`\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 *SCORE FINAL*: {bar_final} `{final:.2%}`\n"
            f"  📊 Composite:  {bar_score} `{comp:.2%}`\n"
            f"  📉 CVD:        {bar_cvd} `{sig['cvd_bias']}` ({sig['cvd_norm']:+.2f})\n"
            f"  ⚡ Momentum:   {bar_mom} `{sig['momentum']:+.0f}`\n"
            f"  ⏱ Decay:      {bar_decay} `{sig['decay_ratio']:.2%}`\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🔬 *EDGE INSTITUCIONAL*: {bar_edge} `{sig.get('edge_score',0):+.2f}`\n"
            f"  Señales: 🟢`{sig.get('edge_signals_bull',0)}` 🔴`{sig.get('edge_signals_bear',0)}`\n"
            f"{edge_str}\n"
            f"  VPOC: `{ed.get('vpoc','-')}` | VWAP: `{ed.get('vwap','-')}`\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🏦 OFI: `{sig['ofi']:+.3f}` | FR: `{sig['funding_rate']:.4%}`\n"
            f"📦 OI Δ: `{sig['oi_delta']:+.3%}` | Vol: `{sig['vol_regime']}`\n"
            f"🌐 TF: Bull`{sig['tf_bull']:.0%}` Bear`{sig['tf_bear']:.0%}`\n"
            f"🔑 `{order_id}`"
        )
        await self.send_message(text)

    async def send_close(
        self, symbol: str, side: str, entry: float, exit_p: float,
        pnl: float, reason: str, trail_was_active: bool = False
    ):
        emoj = "✅" if pnl > 0 else "❌"
        trail = " 🔁trail" if trail_was_active else ""
        text = (
            f"{emoj} *CIERRE {side} {symbol}*\n"
            f"Razón: `{reason}`{trail}\n"
            f"Entrada: `{entry:.6f}` → Salida: `{exit_p:.6f}`\n"
            f"PnL: `{pnl:+.2f}%`"
        )
        await self.send_message(text)

    async def send_status(self, balance: float, positions: dict, gs: dict):
        pos_str = "\n".join(
            f"  • {sym}: {v['side']} conv={v['conv']}"
            for sym, v in positions.items()
        ) or "  Sin posiciones"
        gs_str = (
            f"WR={gs['win_rate']:.0%} PF={gs['profit_factor']:.2f} "
            f"avgPnL={gs['avg_pnl']:+.2f}%"
            if gs else "Sin datos"
        )
        text = (
            f"📊 *Reporte QF×JP v6*\n"
            f"💰 Balance: `{balance:.2f} USDT`\n"
            f"📌 Posiciones:\n{pos_str}\n"
            f"📈 Stats: {gs_str}"
        )
        await self.send_message(text)

    async def send_error(self, msg: str):
        await self.send_message(f"⚠️ *ERROR*: `{msg}`")

    @staticmethod
    def _bar(v: float, width: int = 8) -> str:
        v   = max(0.0, min(1.0, v))
        n   = round(v * width)
        return "█" * n + "░" * (width - n)
