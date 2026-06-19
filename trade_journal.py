"""
QF×JP Bot v7.7 — Trade Journal
═══════════════════════════════════════════════════════════════════════════════
Registra cada trade con todos sus componentes de señal y resultado final.
Tras N trades, calcula estadísticas reales:
  - Win rate por tier (STD/FUEL/SUP)
  - Win rate por hora UTC
  - Score mínimo óptimo empírico
  - Symbols con mejor/peor performance

Esta información alimenta al RiskManager para ajustar MIN_SCORE_EFFECTIVE
automáticamente según el rendimiento real del bot en tiempo real.

Almacenamiento: en memoria (ephemeral en Railway). Los reports se envían
a Telegram cada JOURNAL_REPORT_INTERVAL iteraciones del scanner, así quedan
en el historial de Telegram aunque Railway redeploye.
═══════════════════════════════════════════════════════════════════════════════
"""
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

import telegram_client as tg

log = logging.getLogger("journal")


@dataclass
class TradeRecord:
    # Identidad
    symbol:    str
    direction: str
    tier:      str
    # Señal
    score:     float
    fr:        float   # funding rate al entrar
    obi:       float   # order book imbalance
    oi_delta:  float   # cambio de OI normalizado (>0 crece, <0 decrece)
    htf_score: float
    adx:       float
    # Timing
    hour_utc:  int     # hora UTC de apertura
    opened_at: float   # timestamp
    # Resultado (se completa al cerrar)
    closed_at: Optional[float] = None
    pnl:       Optional[float] = None
    won:       Optional[bool]  = None
    reason:    str             = ""


class TradeJournal:
    """
    Registro de trades en memoria con análisis automático.
    El singleton se instancia en main.py y se pasa a scanner y position_manager.
    """

    def __init__(self):
        self._open:   dict[str, TradeRecord] = {}   # symbol → TradeRecord abierto
        self._closed: list[TradeRecord]      = []   # trades cerrados (histórico)
        # Umbrales adaptativos
        self._recent_wins:  list[bool]  = []        # últimas N (W/L) — global
        self._adaptive_min_score: float = 0.0       # 0 = usar config, >0 = override

        # ── Auto-blacklist por símbolo (aprendido de pérdidas reales) ────────────
        # El caso SYN/ESPORTS demostró que el BLACKLIST manual siempre va un
        # paso por detrás: hace falta perder dinero primero para añadir un
        # símbolo. Esto detecta símbolos tóxicos automáticamente con datos
        # propios, sin esperar a que alguien los añada a mano.
        self._symbol_pnl:     dict[str, float] = {}   # PnL acumulado por símbolo
        self._symbol_losses:  dict[str, int]   = {}   # pérdidas consecutivas por símbolo
        self._auto_blacklist: dict[str, float] = {}   # symbol → timestamp de bloqueo

        # ── Circuit breaker por racha de pérdidas (independiente del $ diario) ──
        # El límite de pérdida diaria en USDT puede tardar en activarse si las
        # pérdidas son pequeñas pero consecutivas — una racha de 5+ pérdidas
        # seguidas suele indicar que el régimen de mercado actual no encaja
        # con la estrategia, incluso si el total en USDT aún no es grande.
        self._consecutive_losses: int   = 0
        self._streak_pause_until: float = 0.0

        log.info("TradeJournal iniciado")

    # ── Apertura ──────────────────────────────────────────────────────────────

    def on_open(
        self,
        symbol:    str,
        direction: str,
        tier:      str,
        score:     float,
        fr:        float   = 0.0,
        obi:       float   = 0.0,
        oi_delta:  float   = 0.0,
        htf_score: float   = 0.0,
        adx:       float   = 0.0,
    ):
        rec = TradeRecord(
            symbol=symbol, direction=direction, tier=tier,
            score=score, fr=fr, obi=obi, oi_delta=oi_delta,
            htf_score=htf_score, adx=adx,
            hour_utc=time.gmtime().tm_hour,
            opened_at=time.time(),
        )
        self._open[symbol] = rec
        log.debug("[journal] abierto: %s %s score=%.1f", symbol, direction, score)

    # ── Cierre ────────────────────────────────────────────────────────────────

    # ── Configuración del auto-blacklist y circuit breaker de racha ─────────────
    AUTO_BLACKLIST_MIN_TRADES   = 3      # mínimo de trades antes de evaluar un símbolo
    AUTO_BLACKLIST_LOSS_STREAK  = 3      # 3 pérdidas consecutivas en el MISMO símbolo
    AUTO_BLACKLIST_DURATION_S   = 86400  # 24h de bloqueo automático
    STREAK_BREAKER_THRESHOLD    = 5      # 5 pérdidas consecutivas GLOBALES
    STREAK_BREAKER_PAUSE_S      = 3600   # pausa 1h tras racha mala

    async def on_close(self, symbol: str, pnl: float, reason: str = ""):
        rec = self._open.pop(symbol, None)
        if rec is None:
            return
        rec.closed_at = time.time()
        rec.pnl       = pnl
        rec.won       = pnl > 0
        rec.reason    = reason
        self._closed.append(rec)

        # Actualizar lista de resultados recientes para umbral adaptativo
        self._recent_wins.append(rec.won)
        if len(self._recent_wins) > 20:
            self._recent_wins.pop(0)

        # ── Auto-blacklist por símbolo ────────────────────────────────────────
        self._symbol_pnl[symbol] = self._symbol_pnl.get(symbol, 0.0) + pnl
        if rec.won:
            self._symbol_losses[symbol] = 0
        else:
            self._symbol_losses[symbol] = self._symbol_losses.get(symbol, 0) + 1
            n_trades_symbol = sum(1 for t in self._closed if t.symbol == symbol)
            if (n_trades_symbol >= self.AUTO_BLACKLIST_MIN_TRADES and
                    self._symbol_losses[symbol] >= self.AUTO_BLACKLIST_LOSS_STREAK and
                    symbol not in self._auto_blacklist):
                self._auto_blacklist[symbol] = time.time()
                log.warning(
                    "[journal] 🚫 AUTO-BLACKLIST: %s — %d pérdidas consecutivas "
                    "(PnL acumulado símbolo: %.4f) — bloqueado %dh",
                    symbol, self._symbol_losses[symbol], self._symbol_pnl[symbol],
                    self.AUTO_BLACKLIST_DURATION_S // 3600,
                )
                try:
                    await tg.notify_auto_blacklist(
                        symbol, self._symbol_losses[symbol], self._symbol_pnl[symbol],
                        self.AUTO_BLACKLIST_DURATION_S // 3600,
                    )
                except Exception:
                    pass

        # ── Circuit breaker por racha de pérdidas GLOBAL ──────────────────────
        if rec.won:
            self._consecutive_losses = 0
        else:
            self._consecutive_losses += 1
            if self._consecutive_losses >= self.STREAK_BREAKER_THRESHOLD:
                already_paused = time.time() < self._streak_pause_until
                self._streak_pause_until = time.time() + self.STREAK_BREAKER_PAUSE_S
                log.warning(
                    "[journal] ⏸️ STREAK BREAKER: %d pérdidas consecutivas — "
                    "pausa de %dmin (independiente del límite $ diario)",
                    self._consecutive_losses, self.STREAK_BREAKER_PAUSE_S // 60,
                )
                if not already_paused:
                    try:
                        await tg.notify_streak_breaker(
                            self._consecutive_losses, self.STREAK_BREAKER_PAUSE_S // 60,
                        )
                    except Exception:
                        pass

        # Recalcular umbral adaptativo
        self._recalculate_adaptive()
        log.info("[journal] cerrado: %s pnl=%.4f won=%s (total=%d)",
                 symbol, pnl, rec.won, len(self._closed))

    def is_symbol_auto_blacklisted(self, symbol: str) -> tuple[bool, str]:
        """
        Chequea si un símbolo está auto-bloqueado por pérdidas consecutivas
        recientes. Expira solo tras AUTO_BLACKLIST_DURATION_S — dale al
        símbolo una segunda oportunidad cuando el régimen de mercado cambie.
        """
        ts = self._auto_blacklist.get(symbol)
        if ts is None:
            return False, ""
        elapsed = time.time() - ts
        if elapsed > self.AUTO_BLACKLIST_DURATION_S:
            del self._auto_blacklist[symbol]
            self._symbol_losses[symbol] = 0  # reset tras expirar
            return False, ""
        remaining_h = (self.AUTO_BLACKLIST_DURATION_S - elapsed) / 3600
        return True, f"auto_blacklist({symbol}, {remaining_h:.1f}h restantes)"

    def is_streak_paused(self) -> tuple[bool, str]:
        """Chequea si el circuit breaker de racha global está activo."""
        if time.time() < self._streak_pause_until:
            remaining_min = (self._streak_pause_until - time.time()) / 60
            return True, f"streak_breaker({self._consecutive_losses} pérdidas, {remaining_min:.0f}min restantes)"
        return False, ""

    # ── Umbral adaptativo ─────────────────────────────────────────────────────

    def _recalculate_adaptive(self):
        """
        Ajusta MIN_SCORE_EFFECTIVE según win rate reciente.
        Con <10 trades no actúa (no hay datos suficientes).
        Con win rate <40%: sube umbral (+8 pts) — mercado desfavorable
        Con win rate >65%: baja umbral (-5 pts) — mercado favorable
        Con win rate 40-65%: usa el valor de config
        """
        if len(self._recent_wins) < 10:
            self._adaptive_min_score = 0.0
            return
        wr = sum(1 for w in self._recent_wins if w) / len(self._recent_wins)
        if wr < 0.40:
            self._adaptive_min_score = 8.0   # será sumado al MIN_SCORE base
            log.info("[journal] wr=%.0f%% → MIN_SCORE +8 (mercado desfavorable)", wr*100)
        elif wr > 0.65:
            self._adaptive_min_score = -5.0  # será restado al MIN_SCORE base
            log.info("[journal] wr=%.0f%% → MIN_SCORE -5 (mercado favorable)", wr*100)
        else:
            self._adaptive_min_score = 0.0
            log.debug("[journal] wr=%.0f%% → MIN_SCORE normal", wr*100)

    def get_adaptive_offset(self) -> float:
        """Retorna el offset a aplicar sobre MIN_SCORE. 0 = sin cambio."""
        return self._adaptive_min_score

    # ── Estadísticas ──────────────────────────────────────────────────────────

    def stats(self) -> dict:
        """Calcula estadísticas completas sobre los trades cerrados."""
        closed = self._closed
        n = len(closed)
        if n == 0:
            return {"total": 0}

        wins   = sum(1 for t in closed if t.won)
        losses = n - wins
        wr     = wins / n
        total_pnl = sum(t.pnl for t in closed if t.pnl is not None)

        # Por tier
        by_tier: dict[str, dict] = defaultdict(lambda: {"w": 0, "l": 0, "pnl": 0.0})
        for t in closed:
            k = t.tier
            if t.won: by_tier[k]["w"] += 1
            else:     by_tier[k]["l"] += 1
            by_tier[k]["pnl"] += t.pnl or 0

        # Por hora UTC (0-23)
        by_hour: dict[int, dict] = defaultdict(lambda: {"w": 0, "l": 0})
        for t in closed:
            h = t.hour_utc
            if t.won: by_hour[h]["w"] += 1
            else:     by_hour[h]["l"] += 1

        # Mejores horas (mínimo 2 trades)
        def _wr(h): 
            d = by_hour[h]
            tot = d["w"] + d["l"]
            return d["w"] / tot if tot >= 2 else -1
        best_hours = sorted(
            [h for h in by_hour if by_hour[h]["w"] + by_hour[h]["l"] >= 2],
            key=_wr, reverse=True
        )[:3]

        # Por símbolo (top 5 mejores, top 5 peores por PnL)
        by_sym: dict[str, float] = defaultdict(float)
        for t in closed:
            by_sym[t.symbol] += t.pnl or 0
        sym_sorted = sorted(by_sym.items(), key=lambda x: x[1], reverse=True)

        # Score mínimo óptimo empírico (score medio de trades ganadores)
        winning_scores = [t.score for t in closed if t.won]
        opt_score      = sum(winning_scores) / len(winning_scores) if winning_scores else 0

        # Win rate de las últimas N operaciones
        recent_wr = (
            sum(1 for w in self._recent_wins if w) / len(self._recent_wins)
            if self._recent_wins else 0
        )

        return {
            "total":          n,
            "wins":           wins,
            "losses":         losses,
            "win_rate":       round(wr * 100, 1),
            "recent_wr":      round(recent_wr * 100, 1),
            "total_pnl":      round(total_pnl, 4),
            "opt_score":      round(opt_score, 1),
            "adaptive_offset": self._adaptive_min_score,
            "by_tier":        {
                k: {
                    "wr":  round(d["w"] / (d["w"] + d["l"]) * 100, 1) if (d["w"] + d["l"]) > 0 else 0,
                    "pnl": round(d["pnl"], 4),
                    "n":   d["w"] + d["l"],
                }
                for k, d in by_tier.items()
            },
            "best_hours_utc": best_hours,
            "top5_symbols":   sym_sorted[:5],
            "bot5_symbols":   sym_sorted[-5:][::-1],
        }

    def recent_win_rate(self) -> float:
        if not self._recent_wins:
            return 0.5
        return sum(1 for w in self._recent_wins if w) / len(self._recent_wins)

    def open_count(self) -> int:
        return len(self._open)

    def total_closed(self) -> int:
        return len(self._closed)
