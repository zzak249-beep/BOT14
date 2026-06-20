"""
QF×JP Bot v7.9 — Trade Journal (FIX Deflated Sharpe Ratio por filtro)
═══════════════════════════════════════════════════════════════════════════════
FIX v7.9 — Deflated Sharpe Ratio (Bailey & López de Prado) por filtro:
  El win rate "confirmado vs no confirmado" de v7.8 es un buen primer
  indicador, pero no corrige por "multiple testing": con varios filtros
  corriendo a la vez (STC+Asimetría, STC+Volumen+Slope, Price Action), es
  esperable que AL MENOS UNO se vea bien solo por azar, aunque ninguno
  tenga edge real — cuantas más cosas pruebas, más probable que la mejor
  parezca buena por pura suerte.

  El Deflated Sharpe corrige exactamente por esto: calcula el Sharpe que
  se ESPERARÍA por puro azar dado cuántos filtros se están comparando
  (n_trials), y solo da por bueno un filtro si su Sharpe real supera ese
  umbral con un p-value > 0.95. Por debajo, "se ve bien" no es lo mismo
  que "hay evidencia de que aporte".

  Adaptado a trades discretos (no a un equity curve continuo de barras de
  tiempo regular): cada trade cerrado es una observación, sin anualizar
  (no hay frecuencia de calendario fija que anualizar). Requiere mínimo
  30 trades en el bucket "confirmado" — con menos, los momentos de orden
  3-4 (skew/kurtosis) que usa la fórmula son demasiado ruidosos para
  decir nada.

  Sin scipy: la CDF normal se calcula con math.erf() de la librería
  estándar (exacta, no una aproximación) — cero dependencias nuevas.

NUEVO en v7.8:
  ✅ filter_tags en TradeRecord — cuando un filtro de confirmación
     (stc_asym, stc_vol_slope, price_action) boostea una señal, se
     etiqueta el trade. _filter_breakdown() compara win rate de
     confirmado vs no confirmado por cada filtro.

Registra cada trade con todos sus componentes de señal y resultado final.
Tras N trades, calcula estadísticas reales:
  - Win rate por tier (STD/FUEL/SUP)
  - Win rate por hora UTC
  - Win rate y Deflated Sharpe por filtro de confirmación
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
import math
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

import telegram_client as tg

log = logging.getLogger("journal")


def _norm_cdf(x: float) -> float:
    """CDF normal estándar, exacta vía math.erf — sin scipy."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2)))


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
    # ── FIX v7.8: qué filtros de confirmación confirmaron esta señal ────────
    filter_tags: dict = field(default_factory=dict)
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
        self._symbol_pnl:     dict[str, float] = {}   # PnL acumulado por símbolo
        self._symbol_losses:  dict[str, int]   = {}   # pérdidas consecutivas por símbolo
        self._auto_blacklist: dict[str, float] = {}   # symbol → timestamp de bloqueo

        # ── Circuit breaker por racha de pérdidas (independiente del $ diario) ──
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
        filter_tags: Optional[dict] = None,
    ):
        rec = TradeRecord(
            symbol=symbol, direction=direction, tier=tier,
            score=score, fr=fr, obi=obi, oi_delta=oi_delta,
            htf_score=htf_score, adx=adx,
            hour_utc=time.gmtime().tm_hour,
            opened_at=time.time(),
            filter_tags=dict(filter_tags) if filter_tags else {},
        )
        self._open[symbol] = rec
        log.debug("[journal] abierto: %s %s score=%.1f filtros=%s",
                  symbol, direction, score, list(rec.filter_tags.keys()))

    # ── Cierre ────────────────────────────────────────────────────────────────

    AUTO_BLACKLIST_MIN_TRADES   = 3
    AUTO_BLACKLIST_LOSS_STREAK  = 3
    AUTO_BLACKLIST_DURATION_S   = 86400
    STREAK_BREAKER_THRESHOLD    = 5
    STREAK_BREAKER_PAUSE_S      = 3600

    # ── FIX v7.8: mínimo de trades por bucket antes de fiarse del win rate ──
    MIN_TRADES_PER_FILTER_BUCKET = 8
    # ── FIX v7.9: el Deflated Sharpe necesita más muestra que el win rate
    # crudo — con menos de 30, skew/kurtosis (momentos de orden 3-4) son
    # demasiado ruidosos para que la fórmula diga algo fiable.
    MIN_TRADES_FOR_DSR = 30

    async def on_close(self, symbol: str, pnl: float, reason: str = ""):
        rec = self._open.pop(symbol, None)
        if rec is None:
            return
        rec.closed_at = time.time()
        rec.pnl       = pnl
        rec.won       = pnl > 0
        rec.reason    = reason
        self._closed.append(rec)

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

        self._recalculate_adaptive()
        log.info("[journal] cerrado: %s pnl=%.4f won=%s filtros=%s (total=%d)",
                 symbol, pnl, rec.won, list(rec.filter_tags.keys()), len(self._closed))

    def is_symbol_auto_blacklisted(self, symbol: str) -> tuple[bool, str]:
        ts = self._auto_blacklist.get(symbol)
        if ts is None:
            return False, ""
        elapsed = time.time() - ts
        if elapsed > self.AUTO_BLACKLIST_DURATION_S:
            del self._auto_blacklist[symbol]
            self._symbol_losses[symbol] = 0
            return False, ""
        remaining_h = (self.AUTO_BLACKLIST_DURATION_S - elapsed) / 3600
        return True, f"auto_blacklist({symbol}, {remaining_h:.1f}h restantes)"

    def is_streak_paused(self) -> tuple[bool, str]:
        if time.time() < self._streak_pause_until:
            remaining_min = (self._streak_pause_until - time.time()) / 60
            return True, f"streak_breaker({self._consecutive_losses} pérdidas, {remaining_min:.0f}min restantes)"
        return False, ""

    # ── Umbral adaptativo ─────────────────────────────────────────────────────

    def _recalculate_adaptive(self):
        if len(self._recent_wins) < 10:
            self._adaptive_min_score = 0.0
            return
        wr = sum(1 for w in self._recent_wins if w) / len(self._recent_wins)
        if wr < 0.40:
            self._adaptive_min_score = 8.0
            log.info("[journal] wr=%.0f%% → MIN_SCORE +8 (mercado desfavorable)", wr*100)
        elif wr > 0.65:
            self._adaptive_min_score = -5.0
            log.info("[journal] wr=%.0f%% → MIN_SCORE -5 (mercado favorable)", wr*100)
        else:
            self._adaptive_min_score = 0.0
            log.debug("[journal] wr=%.0f%% → MIN_SCORE normal", wr*100)

    def get_adaptive_offset(self) -> float:
        return self._adaptive_min_score

    # ── Deflated Sharpe Ratio (FIX v7.9) ────────────────────────────────────

    def _deflated_sharpe(self, pnls: list[float], n_trials: int) -> dict:
        """
        Deflated Sharpe Ratio (Bailey & López de Prado) sobre una lista de
        PnL por trade. n_trials = cuántos filtros se están comparando a la
        vez — corrige el umbral por "si pruebo varias cosas, una va a
        parecer buena por azar aunque ninguna tenga edge real".

        Sin anualizar: cada trade es una observación discreta, no hay
        frecuencia de calendario fija que anualizar (a diferencia de un
        equity curve de barras diarias).

        dsr_pvalue > 0.95 → el Sharpe observado supera lo esperable por
        puro azar dado n_trials, con 95% de confianza. Por debajo, no hay
        evidencia estadística suficiente — "se ve bien" no es lo mismo que
        "hay evidencia real".
        """
        n = len(pnls)
        if n < self.MIN_TRADES_FOR_DSR:
            return {
                "dsr_pvalue": None,
                "reason": f"necesita {self.MIN_TRADES_FOR_DSR - n} trades más "
                          f"(mínimo {self.MIN_TRADES_FOR_DSR} para que skew/kurtosis no sean ruido)",
            }

        mu = sum(pnls) / n
        var = sum((x - mu) ** 2 for x in pnls) / n
        sigma = math.sqrt(var)
        if sigma <= 1e-12:
            return {"dsr_pvalue": None, "reason": "sin varianza en los resultados (todos iguales)"}

        sharpe = mu / sigma
        skew = (sum((x - mu) ** 3 for x in pnls) / n) / sigma ** 3
        kurt = (sum((x - mu) ** 4 for x in pnls) / n) / sigma ** 4

        gamma = 0.5772156649  # constante de Euler-Mascheroni
        log_n = math.log(max(n_trials, 2))
        # Sharpe esperado por azar como el MÁXIMO de n_trials variables
        # normales estándar — aproximación asintótica de valores extremos.
        sr0 = math.sqrt(2 * log_n) - gamma / math.sqrt(2 * log_n)

        num = (sharpe - sr0) * math.sqrt(n - 1)
        den = math.sqrt(max(1 - skew * sharpe + (kurt - 1) / 4 * sharpe ** 2, 1e-9))
        dsr = _norm_cdf(num / den)

        return {
            "sharpe_observado":         round(sharpe, 3),
            "sharpe_esperado_por_azar": round(sr0, 3),
            "dsr_pvalue":               round(dsr, 3),
            "veredicto":                "supera_azar" if dsr > 0.95 else "no_concluyente",
            "n":                        n,
            "n_trials_usados":          n_trials,
        }

    # ── Win rate + DSR por filtro (FIX v7.8 + v7.9) ─────────────────────────

    def _filter_breakdown(self, closed: list[TradeRecord]) -> dict:
        """
        Para cada filtro visto en algún filter_tags, compara win rate de
        trades CONFIRMADOS por ese filtro vs NO confirmados (ausencia de
        la clave — incluye "corrió y no encontró nada" y "desactivado"),
        y calcula el Deflated Sharpe del bucket confirmado corrigiendo por
        cuántos filtros se están comparando a la vez (n_trials).
        """
        all_filter_names: set[str] = set()
        for t in closed:
            all_filter_names.update(t.filter_tags.keys())

        # FIX v7.9: n_trials = cuántos filtros distintos se están evaluando
        # en paralelo — esa es la corrección de multiple-testing relevante
        # aquí, no el número total de trades ni de cambios hechos en toda
        # la sesión.
        n_trials = max(len(all_filter_names), 2)

        out: dict[str, dict] = {}
        for fname in all_filter_names:
            confirmados    = [t for t in closed if fname in t.filter_tags]
            no_confirmados = [t for t in closed if fname not in t.filter_tags]

            def _bucket(group: list[TradeRecord]) -> dict:
                n = len(group)
                if n == 0:
                    return {"n": 0, "wr": None, "pnl": 0.0, "suficiente": False}
                w = sum(1 for t in group if t.won)
                pnl = sum(t.pnl or 0 for t in group)
                return {
                    "n":   n,
                    "wr":  round(w / n * 100, 1),
                    "pnl": round(pnl, 4),
                    "suficiente": n >= self.MIN_TRADES_PER_FILTER_BUCKET,
                }

            b_conf   = _bucket(confirmados)
            b_noconf = _bucket(no_confirmados)
            veredicto = "datos_insuficientes"
            if b_conf["suficiente"] and b_noconf["suficiente"]:
                diff = (b_conf["wr"] or 0) - (b_noconf["wr"] or 0)
                if diff >= 10:
                    veredicto = "aporta"
                elif diff <= -10:
                    veredicto = "perjudica"
                else:
                    veredicto = "sin_diferencia_clara"

            pnls_confirmados = [t.pnl or 0 for t in confirmados]
            dsr = self._deflated_sharpe(pnls_confirmados, n_trials)

            out[fname] = {
                "confirmado":      b_conf,
                "no_confirmado":   b_noconf,
                "veredicto_winrate": veredicto,
                "deflated_sharpe":   dsr,
            }
        return out

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

        by_tier: dict[str, dict] = defaultdict(lambda: {"w": 0, "l": 0, "pnl": 0.0})
        for t in closed:
            k = t.tier
            if t.won: by_tier[k]["w"] += 1
            else:     by_tier[k]["l"] += 1
            by_tier[k]["pnl"] += t.pnl or 0

        by_hour: dict[int, dict] = defaultdict(lambda: {"w": 0, "l": 0})
        for t in closed:
            h = t.hour_utc
            if t.won: by_hour[h]["w"] += 1
            else:     by_hour[h]["l"] += 1

        def _wr(h):
            d = by_hour[h]
            tot = d["w"] + d["l"]
            return d["w"] / tot if tot >= 2 else -1
        best_hours = sorted(
            [h for h in by_hour if by_hour[h]["w"] + by_hour[h]["l"] >= 2],
            key=_wr, reverse=True
        )[:3]

        by_sym: dict[str, float] = defaultdict(float)
        for t in closed:
            by_sym[t.symbol] += t.pnl or 0
        sym_sorted = sorted(by_sym.items(), key=lambda x: x[1], reverse=True)

        winning_scores = [t.score for t in closed if t.won]
        opt_score      = sum(winning_scores) / len(winning_scores) if winning_scores else 0

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
            "by_filter":      self._filter_breakdown(closed),
        }

    def recent_win_rate(self) -> float:
        if not self._recent_wins:
            return 0.5
        return sum(1 for w in self._recent_wins if w) / len(self._recent_wins)

    def open_count(self) -> int:
        return len(self._open)

    def total_closed(self) -> int:
        return len(self._closed)
