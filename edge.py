"""
QF×JP Bot v6.0 — edge.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
VENTAJAS EXCLUSIVAS sobre bots y traders en 3m:

1.  FVG  — Fair Value Gap (desequilibrio institucional)
2.  OB   — Order Block (último bloque antes del impulso)
3.  BOS  — Break of Structure (cambio de estructura)
4.  CHoCH — Change of Character (giro de mercado)
5.  LIQ  — Liquidity Sweep (caza de stops real)
6.  CVD_DIV — Divergencia CVD vs precio (trampa)
7.  DELTA_EXH — Delta exhaustion (presión agotada)
8.  VPOC — Volume Point of Control (precio más activo)
9.  VWAP_ANCHOR — VWAP anclado a sesión
10. DARK_POOL — Proxy absorción institucional
11. FR_SQUEEZE — Funding Rate squeeze contrarian
12. CANDLE_PATTERN — Engulfing / Hammer en zonas clave
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
import numpy as np
from dataclasses import dataclass, field
from typing import Optional


def _safe_div(a, b, fill=0.0):
    with np.errstate(divide="ignore", invalid="ignore"):
        r = np.where(np.abs(b) > 1e-12, a / b, fill)
    return np.nan_to_num(r, nan=fill, posinf=fill, neginf=fill)


# ═══════════════════════════════════════════════════════════
# RESULTADO DE EDGE
# ═══════════════════════════════════════════════════════════
@dataclass
class EdgeResult:
    # Puntuación total [-1, 1]  (>0 = bullish, <0 = bearish)
    edge_score:   float = 0.0
    edge_dir:     Optional[str] = None   # "LONG" | "SHORT" | None

    # Señales individuales
    fvg_bull:     bool  = False   # Fair Value Gap alcista presente
    fvg_bear:     bool  = False
    ob_bull:      bool  = False   # Order Block alcista activo
    ob_bear:      bool  = False
    bos_bull:     bool  = False   # Break of Structure alcista
    bos_bear:     bool  = False
    choch_bull:   bool  = False   # Change of Character alcista
    choch_bear:   bool  = False
    liq_sweep_bull: bool = False  # Liquidity sweep → probable reversal al alza
    liq_sweep_bear: bool = False
    cvd_div_bull: bool  = False   # CVD divergencia alcista (precio cae, CVD sube)
    cvd_div_bear: bool  = False
    delta_exh_bull: bool = False  # Delta exhaustion alcista
    delta_exh_bear: bool = False
    vpoc_above:   bool  = False   # Precio por encima del VPOC (bullish bias)
    vpoc_below:   bool  = False
    vwap_bull:    bool  = False   # Precio sobre VWAP sesión
    vwap_bear:    bool  = False
    dark_pool:    bool  = False   # Absorción institucional detectada
    fr_squeeze_bull: bool = False # FR extremo corto → long squeeze
    fr_squeeze_bear: bool = False
    engulfing_bull: bool = False  # Patrón engulfing alcista en zona
    engulfing_bear: bool = False
    hammer:       bool  = False   # Hammer / inverted hammer
    shooting_star: bool = False

    # Zonas clave
    fvg_bull_zone: Optional[tuple] = None  # (low, high)
    fvg_bear_zone: Optional[tuple] = None
    ob_bull_level: Optional[float] = None
    ob_bear_level: Optional[float] = None
    vpoc_price:    Optional[float] = None
    vwap_price:    Optional[float] = None

    # Diagnóstico
    signals_count_bull: int = 0
    signals_count_bear: int = 0
    detail: dict = field(default_factory=dict)


# ═══════════════════════════════════════════════════════════
# MOTOR DE VENTAJAS
# ═══════════════════════════════════════════════════════════
class EdgeEngine:
    """
    Analiza microestructura e institucional para encontrar
    ventaja real sobre el resto de bots en timeframe 3m.
    """

    def compute(
        self,
        klines:   list,   # 3m, mínimo 100 velas
        klines_1m: list,  # 1m, mínimo 30 velas
        funding_rate: float = 0.0,
        ofi:      float = 0.0,
    ) -> EdgeResult:

        res = EdgeResult()
        if len(klines) < 50:
            return res

        o = np.array([k["o"] for k in klines], dtype=float)
        h = np.array([k["h"] for k in klines], dtype=float)
        l = np.array([k["l"] for k in klines], dtype=float)
        c = np.array([k["c"] for k in klines], dtype=float)
        v = np.array([k["v"] for k in klines], dtype=float)

        price = float(c[-1])

        # ── 1. FVG — Fair Value Gap ──────────────────────
        self._fvg(o, h, l, c, price, res)

        # ── 2. Order Block ───────────────────────────────
        self._order_block(o, h, l, c, v, price, res)

        # ── 3. BOS / CHoCH ───────────────────────────────
        self._structure(h, l, c, res)

        # ── 4. Liquidity Sweep ───────────────────────────
        self._liq_sweep(h, l, c, res)

        # ── 5. CVD Divergencia ───────────────────────────
        self._cvd_divergence(o, h, l, c, v, res)

        # ── 6. Delta Exhaustion ──────────────────────────
        self._delta_exhaustion(o, h, l, c, v, res)

        # ── 7. VPOC ──────────────────────────────────────
        self._vpoc(h, l, c, v, price, res)

        # ── 8. VWAP anclado a sesión ─────────────────────
        self._session_vwap(o, h, l, c, v, klines, price, res)

        # ── 9. Dark Pool proxy ───────────────────────────
        self._dark_pool(o, h, l, c, v, res)

        # ── 10. FR Squeeze ───────────────────────────────
        self._fr_squeeze(funding_rate, res)

        # ── 11. Candle Patterns en zonas ─────────────────
        self._candle_patterns(o, h, l, c, res)

        # ── Score compuesto ───────────────────────────────
        self._score(res, ofi)

        return res

    # ────────────────────────────────────────────────────────
    # 1. FAIR VALUE GAP
    # Vela 1 alta, vela 2 impulso, vela 3 → gap entre v1.high y v3.low (bull)
    # ────────────────────────────────────────────────────────
    def _fvg(self, o, h, l, c, price, res: EdgeResult):
        for i in range(len(c) - 15, len(c) - 1):
            if i < 2:
                continue
            # Bull FVG: h[i-2] < l[i] → gap entre vela i-2 y vela i
            if h[i - 2] < l[i]:
                gap_low  = h[i - 2]
                gap_high = l[i]
                # Precio dentro o tocando el FVG
                if gap_low <= price <= gap_high * 1.005:
                    res.fvg_bull      = True
                    res.fvg_bull_zone = (gap_low, gap_high)
                    break

            # Bear FVG: l[i-2] > h[i] → gap entre vela i-2 y vela i
            if l[i - 2] > h[i]:
                gap_high = l[i - 2]
                gap_low  = h[i]
                if gap_low * 0.995 <= price <= gap_high:
                    res.fvg_bear      = True
                    res.fvg_bear_zone = (gap_low, gap_high)
                    break

    # ────────────────────────────────────────────────────────
    # 2. ORDER BLOCK
    # Última vela bajista antes de un movimiento alcista fuerte (y viceversa)
    # ────────────────────────────────────────────────────────
    def _order_block(self, o, h, l, c, v, price, res: EdgeResult):
        lookback = min(30, len(c) - 2)
        vol_mean = float(np.mean(v[-50:])) if len(v) >= 50 else float(np.mean(v))

        for i in range(len(c) - 2, len(c) - lookback - 1, -1):
            # Bull OB: vela bajista (c < o) seguida de impulso alcista
            if c[i] < o[i] and v[i] > vol_mean * 1.2:
                # Verificar que el siguiente movimiento fue alcista
                subsequent_high = float(np.max(h[i + 1:i + 5])) if i + 5 <= len(h) else h[-1]
                if subsequent_high > h[i] * 1.002:
                    ob_level = float((o[i] + c[i]) / 2)
                    # Precio regresando al OB
                    if l[i] <= price <= h[i] * 1.003:
                        res.ob_bull       = True
                        res.ob_bull_level = ob_level
                        break

            # Bear OB: vela alcista seguida de impulso bajista
            if c[i] > o[i] and v[i] > vol_mean * 1.2:
                subsequent_low = float(np.min(l[i + 1:i + 5])) if i + 5 <= len(l) else l[-1]
                if subsequent_low < l[i] * 0.998:
                    ob_level = float((o[i] + c[i]) / 2)
                    if l[i] * 0.997 <= price <= h[i]:
                        res.ob_bear       = True
                        res.ob_bear_level = ob_level
                        break

    # ────────────────────────────────────────────────────────
    # 3. BOS / CHoCH — Estructura de mercado
    # BOS: ruptura de estructura en dirección de tendencia
    # CHoCH: primera ruptura contraria (posible giro)
    # ────────────────────────────────────────────────────────
    def _structure(self, h, l, c, res: EdgeResult):
        n = len(c)
        if n < 20:
            return

        # Últimos 3 swing highs/lows simples (max/min de ventanas de 5)
        sh, sl_arr = [], []
        for i in range(5, n - 5, 3):
            if h[i] == np.max(h[i-5:i+5]):
                sh.append((i, h[i]))
            if l[i] == np.min(l[i-5:i+5]):
                sl_arr.append((i, l[i]))

        if len(sh) >= 2 and len(sl_arr) >= 2:
            # BOS Bull: precio supera el último swing high con cierre
            last_sh = sh[-1][1]
            prev_sh = sh[-2][1]
            if c[-1] > last_sh and last_sh > prev_sh:
                res.bos_bull = True
            # BOS Bear: precio rompe por debajo del último swing low
            last_sl = sl_arr[-1][1]
            prev_sl = sl_arr[-2][1]
            if c[-1] < last_sl and last_sl < prev_sl:
                res.bos_bear = True

            # CHoCH Bull: tendencia bajista (HH→HL→LL→LH) pero precio rompe por encima del HH previo
            if not res.bos_bull and len(sh) >= 2:
                if sh[-1][1] < sh[-2][1] and c[-1] > sh[-1][1]:
                    res.choch_bull = True
            # CHoCH Bear
            if not res.bos_bear and len(sl_arr) >= 2:
                if sl_arr[-1][1] > sl_arr[-2][1] and c[-1] < sl_arr[-1][1]:
                    res.choch_bear = True

    # ────────────────────────────────────────────────────────
    # 4. LIQUIDITY SWEEP
    # Precio hace nuevo high/low y cierra de vuelta → stop hunt
    # ────────────────────────────────────────────────────────
    def _liq_sweep(self, h, l, c, res: EdgeResult):
        n = len(c)
        if n < 15:
            return

        # Ventana de referencia (swing de las 10 velas previas)
        ref_high = float(np.max(h[-15:-3]))
        ref_low  = float(np.min(l[-15:-3]))
        price    = float(c[-1])

        last_h   = float(h[-1])
        last_l   = float(l[-1])
        last_c   = float(c[-1])

        # Sweep alcista: mecha baja por debajo del ref_low pero cierra por encima
        if last_l < ref_low and last_c > ref_low:
            res.liq_sweep_bull = True   # stops cazados abajo → reversal al alza

        # Sweep bajista: mecha sube por encima del ref_high pero cierra por debajo
        if last_h > ref_high and last_c < ref_high:
            res.liq_sweep_bear = True   # stops cazados arriba → reversal a la baja

    # ────────────────────────────────────────────────────────
    # 5. CVD DIVERGENCE
    # Precio hace nuevo high pero CVD no (presión real es bearish)
    # ────────────────────────────────────────────────────────
    def _cvd_divergence(self, o, h, l, c, v, res: EdgeResult):
        n = len(c)
        if n < 20:
            return

        hl_r = h - l
        bvol = np.where(hl_r > 1e-12, ((c - l) / np.where(hl_r > 1e-12, hl_r, 1)) * v, v * 0.5)
        svol = np.where(hl_r > 1e-12, ((h - c) / np.where(hl_r > 1e-12, hl_r, 1)) * v, v * 0.5)
        delta = bvol - svol

        # Dividir en dos mitades
        mid      = n // 2
        cvd_1st  = float(np.sum(delta[:mid]))
        cvd_2nd  = float(np.sum(delta[mid:]))
        price_1h = float(c[mid])
        price_2h = float(c[-1])

        # Divergencia bearish: precio sube pero CVD cae (baja presión compradora)
        if price_2h > price_1h * 1.002 and cvd_2nd < cvd_1st * 0.85:
            res.cvd_div_bear = True

        # Divergencia bullish: precio cae pero CVD sube (absorción)
        if price_2h < price_1h * 0.998 and cvd_2nd > cvd_1st * 1.15:
            res.cvd_div_bull = True

    # ────────────────────────────────────────────────────────
    # 6. DELTA EXHAUSTION
    # Delta extremo pero precio apenas se mueve → agotamiento
    # ────────────────────────────────────────────────────────
    def _delta_exhaustion(self, o, h, l, c, v, res: EdgeResult):
        n = len(c)
        if n < 10:
            return

        hl_r = h[-10:] - l[-10:]
        bvol = np.where(hl_r > 1e-12,
                        ((c[-10:] - l[-10:]) / np.where(hl_r > 1e-12, hl_r, 1)) * v[-10:],
                        v[-10:] * 0.5)
        svol = np.where(hl_r > 1e-12,
                        ((h[-10:] - c[-10:]) / np.where(hl_r > 1e-12, hl_r, 1)) * v[-10:],
                        v[-10:] * 0.5)
        delta_last = float(np.sum(bvol - svol))
        vol_total  = float(np.sum(v[-10:])) or 1.0
        delta_ratio = delta_last / vol_total   # [-1, 1]

        # Rango de precio en las últimas 10 velas (normalizado)
        price_range = float(np.max(h[-10:]) - np.min(l[-10:]))
        atr_proxy   = float(np.mean(h[-20:] - l[-20:])) or 1e-12
        range_ratio = price_range / atr_proxy

        # Agotamiento bearish: delta muy positivo (compradores fuertes)
        # pero precio no sube (range estrecho)
        if delta_ratio > 0.35 and range_ratio < 0.6:
            res.delta_exh_bear = True   # compras no mueven precio → vender

        # Agotamiento bullish: delta muy negativo pero precio no cae
        if delta_ratio < -0.35 and range_ratio < 0.6:
            res.delta_exh_bull = True   # ventas no mueven precio → comprar

    # ────────────────────────────────────────────────────────
    # 7. VPOC — Volume Point of Control
    # Precio más negociado: soporte/resistencia más fiable
    # ────────────────────────────────────────────────────────
    def _vpoc(self, h, l, c, v, price, res: EdgeResult):
        n = min(len(c), 100)
        # Discretizar precio en 50 buckets
        p_min = float(np.min(l[-n:]))
        p_max = float(np.max(h[-n:]))
        if p_max <= p_min:
            return
        buckets = 50
        bucket_size = (p_max - p_min) / buckets
        vol_profile = np.zeros(buckets)
        for i in range(-n, 0):
            mid_p = (h[i] + l[i]) / 2
            idx   = int((mid_p - p_min) / bucket_size)
            idx   = min(idx, buckets - 1)
            vol_profile[idx] += v[i]

        vpoc_idx   = int(np.argmax(vol_profile))
        vpoc_price = p_min + (vpoc_idx + 0.5) * bucket_size

        res.vpoc_price  = vpoc_price
        res.vpoc_above  = price > vpoc_price   # bullish bias sobre VPOC
        res.vpoc_below  = price < vpoc_price

    # ────────────────────────────────────────────────────────
    # 8. VWAP ANCLADO A SESIÓN
    # VWAP desde primera vela de la sesión NY/LDN
    # ────────────────────────────────────────────────────────
    def _session_vwap(self, o, h, l, c, v, klines, price, res: EdgeResult):
        import datetime, math
        # Usar las últimas N velas como proxy de sesión (aprox 5h en 3m = 100 velas)
        n = min(100, len(c))
        tp  = (h[-n:] + l[-n:] + c[-n:]) / 3
        cv  = np.cumsum(tp * v[-n:])
        cv2 = np.cumsum(v[-n:])
        vwap = float(_safe_div(np.array([cv[-1]]), np.array([cv2[-1]]), fill=price)[0])

        res.vwap_price = vwap
        res.vwap_bull  = price > vwap
        res.vwap_bear  = price < vwap

    # ────────────────────────────────────────────────────────
    # 9. DARK POOL PROXY
    # Velas con volumen muy alto pero rango muy pequeño → absorción
    # ────────────────────────────────────────────────────────
    def _dark_pool(self, o, h, l, c, v, res: EdgeResult):
        n = min(50, len(c))
        vol_mean  = float(np.mean(v[-n:]))
        range_arr = h[-n:] - l[-n:]
        range_mean = float(np.mean(range_arr))

        # Última vela
        last_vol   = float(v[-1])
        last_range = float(h[-1] - l[-1])

        # Volumen 2× superior a media pero rango < 0.5× media → absorción
        if last_vol > vol_mean * 2.0 and last_range < range_mean * 0.5:
            res.dark_pool = True

    # ────────────────────────────────────────────────────────
    # 10. FUNDING RATE SQUEEZE
    # FR extremo → comerciantes atrapados → reversal inminente
    # ────────────────────────────────────────────────────────
    def _fr_squeeze(self, fr: float, res: EdgeResult):
        if fr > 0.003:     # longs muy cargados → squeeze → SHORT
            res.fr_squeeze_bear = True
        elif fr < -0.003:  # shorts muy cargados → squeeze → LONG
            res.fr_squeeze_bull = True

    # ────────────────────────────────────────────────────────
    # 11. CANDLE PATTERNS
    # Engulfing / Hammer en zonas de estructura
    # ────────────────────────────────────────────────────────
    def _candle_patterns(self, o, h, l, c, res: EdgeResult):
        if len(c) < 3:
            return

        # Engulfing alcista: vela anterior bajista, actual alcista que la engloba
        prev_bull = c[-2] > o[-2]
        prev_bear = c[-2] < o[-2]
        curr_bull = c[-1] > o[-1]
        curr_bear = c[-1] < o[-1]
        prev_body = abs(c[-2] - o[-2])
        curr_body = abs(c[-1] - o[-1])

        if prev_bear and curr_bull and curr_body > prev_body * 1.1:
            if c[-1] > o[-2] and o[-1] < c[-2]:
                res.engulfing_bull = True

        if prev_bull and curr_bear and curr_body > prev_body * 1.1:
            if c[-1] < o[-2] and o[-1] > c[-2]:
                res.engulfing_bear = True

        # Hammer: cuerpo pequeño, mecha inferior larga (reversal alcista)
        body       = abs(c[-1] - o[-1])
        lower_wick = min(c[-1], o[-1]) - l[-1]
        upper_wick = h[-1] - max(c[-1], o[-1])
        candle_range = h[-1] - l[-1]
        if candle_range > 1e-12:
            if lower_wick > body * 2 and lower_wick > upper_wick * 2:
                res.hammer = True
            if upper_wick > body * 2 and upper_wick > lower_wick * 2:
                res.shooting_star = True

    # ────────────────────────────────────────────────────────
    # SCORE FINAL
    # ────────────────────────────────────────────────────────
    def _score(self, res: EdgeResult, ofi: float):
        """
        Ponderación de señales edge.
        Cada señal aporta entre 0.05 y 0.20 al score.
        Score final en [-1, +1].
        """
        bull = 0.0
        bear = 0.0

        # Señales de alta ponderación (0.15-0.20)
        if res.fvg_bull:       bull += 0.18
        if res.fvg_bear:       bear += 0.18
        if res.ob_bull:        bull += 0.17
        if res.ob_bear:        bear += 0.17
        if res.liq_sweep_bull: bull += 0.20   # más potente: stop hunt confirmado
        if res.liq_sweep_bear: bear += 0.20
        if res.bos_bull:       bull += 0.15
        if res.bos_bear:       bear += 0.15
        if res.choch_bull:     bull += 0.12   # cambio de carácter (menos certeza)
        if res.choch_bear:     bear += 0.12

        # Señales de ponderación media (0.10-0.12)
        if res.cvd_div_bull:   bull += 0.12
        if res.cvd_div_bear:   bear += 0.12
        if res.delta_exh_bull: bull += 0.10
        if res.delta_exh_bear: bear += 0.10
        if res.dark_pool:                     # dark pool neutral → confirma el lado OFI
            if ofi > 0: bull += 0.10
            else:        bear += 0.10

        # Señales de baja ponderación (0.06-0.08)
        if res.vpoc_above:     bull += 0.08
        if res.vpoc_below:     bear += 0.08
        if res.vwap_bull:      bull += 0.07
        if res.vwap_bear:      bear += 0.07
        if res.fr_squeeze_bull: bull += 0.10
        if res.fr_squeeze_bear: bear += 0.10
        if res.engulfing_bull: bull += 0.08
        if res.engulfing_bear: bear += 0.08
        if res.hammer:         bull += 0.06
        if res.shooting_star:  bear += 0.06

        # Contar señales
        res.signals_count_bull = sum([
            res.fvg_bull, res.ob_bull, res.liq_sweep_bull, res.bos_bull,
            res.choch_bull, res.cvd_div_bull, res.delta_exh_bull,
            res.vpoc_above, res.vwap_bull, res.fr_squeeze_bull,
            res.engulfing_bull, res.hammer,
        ])
        res.signals_count_bear = sum([
            res.fvg_bear, res.ob_bear, res.liq_sweep_bear, res.bos_bear,
            res.choch_bear, res.cvd_div_bear, res.delta_exh_bear,
            res.vpoc_below, res.vwap_bear, res.fr_squeeze_bear,
            res.engulfing_bear, res.shooting_star,
        ])

        # Score normalizado
        net = bull - bear
        res.edge_score = float(np.clip(net, -1.0, 1.0))

        if res.edge_score > 0.15 and res.signals_count_bull >= 2:
            res.edge_dir = "LONG"
        elif res.edge_score < -0.15 and res.signals_count_bear >= 2:
            res.edge_dir = "SHORT"

        res.detail = {
            "fvg":     ("BULL" if res.fvg_bull else "") or ("BEAR" if res.fvg_bear else ""),
            "ob":      ("BULL" if res.ob_bull  else "") or ("BEAR" if res.ob_bear  else ""),
            "bos":     ("BULL" if res.bos_bull else "") or ("BEAR" if res.bos_bear else ""),
            "choch":   ("BULL" if res.choch_bull else "") or ("BEAR" if res.choch_bear else ""),
            "liq":     ("BULL" if res.liq_sweep_bull else "") or ("BEAR" if res.liq_sweep_bear else ""),
            "cvd_div": ("BULL" if res.cvd_div_bull else "") or ("BEAR" if res.cvd_div_bear else ""),
            "d_exh":   ("BULL" if res.delta_exh_bull else "") or ("BEAR" if res.delta_exh_bear else ""),
            "dark":    "✓" if res.dark_pool else "",
            "vpoc":    f"{res.vpoc_price:.4f}" if res.vpoc_price else "",
            "vwap":    f"{res.vwap_price:.4f}" if res.vwap_price else "",
            "pattern": (
                "ENG↑" if res.engulfing_bull else
                "ENG↓" if res.engulfing_bear else
                "HAMM" if res.hammer else
                "STAR" if res.shooting_star else ""
            ),
        }
