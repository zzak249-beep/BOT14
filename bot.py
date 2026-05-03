"""
╔══════════════════════════════════════════════════════════════════╗
║          PHANTOM EDGE BOT — ULTIMATE EDITION                     ║
║   ZigZag++ 15m · HMA · Volume Delta · ATR · BingX Futures       ║
║                                                                  ║
║   Estrategia: Ruptura ZigZag++ (pico/valle) en 15m              ║
║   TP: 45 pips   SL: 30 pips   RR: 1.5                          ║
║   Filtros: HMA slope · Volume Delta · ATR mín                   ║
╚══════════════════════════════════════════════════════════════════╝
"""

# ─────────────────────────────────────────────────────────────
#  IMPORTS
# ─────────────────────────────────────────────────────────────
import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import numpy as np

# ─────────────────────────────────────────────────────────────
#  CONFIGURACIÓN — 100% desde variables de entorno
# ─────────────────────────────────────────────────────────────
API_KEY      = os.getenv("BINGX_API_KEY", "")
API_SECRET   = os.getenv("BINGX_API_SECRET", "")
TG_TOKEN     = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT      = os.getenv("TELEGRAM_CHAT_ID", "")

AUTO_TRADE   = os.getenv("AUTO_TRADING_ENABLED", "false").lower() == "true"
LEVERAGE     = int(os.getenv("LEVERAGE", "10"))
TRADE_USDT   = float(os.getenv("TRADE_AMOUNT_USDT", "10"))
MAX_POS      = int(os.getenv("MAX_OPEN_TRADES", "5"))
SCAN_SEC     = int(os.getenv("SCAN_INTERVAL_SECONDS", "60"))
BATCH_SIZE   = int(os.getenv("SCAN_BATCH_SIZE", "10"))
COOLDOWN_SEC = int(os.getenv("COOLDOWN_SECONDS", "300"))
MIN_VOL_24H  = float(os.getenv("MIN_VOLUME_USDT", "1000000"))
PORT         = int(os.getenv("PORT", "8080"))

# ── Parámetros de estrategia ZigZag++ ─────────────────────────
PIVOT_LEN    = int(os.getenv("PIVOT_LEN", "5"))       # longitud pivot high/low
HMA_LEN      = int(os.getenv("HMA_LEN", "50"))        # HMA rápida
FT_PERIOD    = int(os.getenv("FT_PERIOD", "25"))       # Future-Trend delta ventana
ATR_LEN      = int(os.getenv("ATR_LEN", "14"))         # ATR para pip proxy
TP_PIPS      = int(os.getenv("TP_PIPS", "45"))         # pips objetivo TP
SL_PIPS      = int(os.getenv("SL_PIPS", "30"))         # pips stop loss
MIN_ATR_PCT  = float(os.getenv("MIN_ATR_PCT", "0.08")) # ATR % mínimo para operar
VOL_MULT     = float(os.getenv("VOL_MULT", "1.2"))     # multiplicador volumen spike
TIMEFRAME    = "15m"                                   # fijo según estrategia

BINGX_BASE   = "https://open-api.bingx.com"

# ─────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-5s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("PhantomEdge")


# ══════════════════════════════════════════════════════════════
#  BINGX REST CLIENT
#  Fixes aplicados:
#   • Firma HMAC correcta (params ordenados)
#   • Balance: prueba v3→v2, campo availableMargin
#   • Klines: endpoint v3, parseo array plano [ts,o,h,l,c,v]
#   • Órdenes SL+TP lanzadas en asyncio.gather (paralelo)
# ══════════════════════════════════════════════════════════════
class BingXClient:
    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(limit=60, ttl_dns_cache=300)
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=15),
            )
        return self._session

    def _sign(self, params: dict) -> str:
        payload = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        return hmac.new(API_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()

    async def _get(self, path: str, params: dict = None, auth: bool = False) -> dict:
        p = dict(params or {})
        headers = {}
        if auth:
            p["timestamp"] = int(time.time() * 1000)
            p["signature"] = self._sign(p)
            headers["X-BX-APIKEY"] = API_KEY
        try:
            async with self.session.get(f"{BINGX_BASE}{path}", params=p, headers=headers) as r:
                return await r.json()
        except Exception as e:
            log.debug(f"GET {path}: {e}")
            return {}

    async def _post(self, path: str, params: dict = None) -> dict:
        p = dict(params or {})
        p["timestamp"] = int(time.time() * 1000)
        p["signature"] = self._sign(p)
        headers = {"X-BX-APIKEY": API_KEY, "Content-Type": "application/x-www-form-urlencoded"}
        try:
            async with self.session.post(f"{BINGX_BASE}{path}", data=p, headers=headers) as r:
                return await r.json()
        except Exception as e:
            log.debug(f"POST {path}: {e}")
            return {}

    # ── Balance USDT disponible ───────────────────────────────
    async def get_balance(self) -> float:
        for ep in ("/openApi/swap/v3/user/balance", "/openApi/swap/v2/user/balance"):
            d = await self._get(ep, auth=True)
            if d.get("code") == 0:
                b = d.get("data", {}).get("balance", {})
                if isinstance(b, dict):
                    v = b.get("availableMargin") or b.get("available") or 0
                    return float(v)
                if isinstance(b, list):
                    for item in b:
                        if item.get("asset") == "USDT":
                            return float(item.get("availableMargin", 0))
        return 0.0

    # ── Lista de símbolos activos ─────────────────────────────
    async def get_symbols(self) -> list[dict]:
        d = await self._get("/openApi/swap/v2/quote/contracts")
        if d.get("code") == 0:
            return [
                s for s in d.get("data", [])
                if s.get("symbol", "").endswith("-USDT")
                and s.get("status", 0) == 1
            ]
        return []

    # ── Klines — formato v3: array plano ─────────────────────
    async def get_klines(self, symbol: str, interval: str = "15m", limit: int = 150) -> list[dict]:
        d = await self._get("/openApi/swap/v3/quote/klines", {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        })
        if d.get("code") != 0:
            return []
        candles = []
        for k in d.get("data", []):
            try:
                candles.append({
                    "t": int(k[0]), "o": float(k[1]), "h": float(k[2]),
                    "l": float(k[3]), "c": float(k[4]), "v": float(k[5]),
                })
            except Exception:
                continue
        return sorted(candles, key=lambda x: x["t"])

    # ── Ticker 24h (volumen) ──────────────────────────────────
    async def get_ticker(self, symbol: str) -> dict:
        d = await self._get("/openApi/swap/v2/quote/ticker", {"symbol": symbol})
        if d.get("code") == 0:
            return d.get("data", {})
        return {}

    # ── Posiciones abiertas ───────────────────────────────────
    async def get_positions(self) -> list[dict]:
        d = await self._get("/openApi/swap/v2/user/positions", auth=True)
        if d.get("code") == 0:
            return [p for p in d.get("data", []) if float(p.get("positionAmt", 0)) != 0]
        return []

    # ── Set leverage ─────────────────────────────────────────
    async def set_leverage(self, symbol: str):
        await asyncio.gather(
            self._post("/openApi/swap/v2/trade/leverage",
                       {"symbol": symbol, "side": "LONG",  "leverage": LEVERAGE}),
            self._post("/openApi/swap/v2/trade/leverage",
                       {"symbol": symbol, "side": "SHORT", "leverage": LEVERAGE}),
        )

    # ── Colocar orden completa (market + SL + TP en paralelo) ─
    async def place_order(
        self, symbol: str, side: str, qty: float,
        sl: float, tp: float, entry: float
    ) -> bool:
        pos_side  = "LONG"  if side == "BUY"  else "SHORT"
        close_side = "SELL" if side == "BUY"  else "BUY"

        await self.set_leverage(symbol)

        # Orden de mercado
        res = await self._post("/openApi/swap/v2/trade/order", {
            "symbol": symbol, "side": side, "positionSide": pos_side,
            "type": "MARKET", "quantity": round(qty, 4),
        })
        if res.get("code") != 0:
            log.error(f"❌ Orden fallida {symbol}: {res.get('msg')}")
            return False

        # SL + TP en paralelo ← velocidad clave
        await asyncio.gather(
            self._post("/openApi/swap/v2/trade/order", {
                "symbol": symbol, "side": close_side, "positionSide": pos_side,
                "type": "STOP_MARKET", "stopPrice": round(sl, 6),
                "closePosition": "true", "workingType": "MARK_PRICE",
            }),
            self._post("/openApi/swap/v2/trade/order", {
                "symbol": symbol, "side": close_side, "positionSide": pos_side,
                "type": "TAKE_PROFIT_MARKET", "stopPrice": round(tp, 6),
                "closePosition": "true", "workingType": "MARK_PRICE",
            }),
        )
        return True

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()


# ══════════════════════════════════════════════════════════════
#  INDICADORES TÉCNICOS — numpy vectorizado
# ══════════════════════════════════════════════════════════════

def _wma(arr: np.ndarray, n: int) -> np.ndarray:
    """Weighted Moving Average"""
    if len(arr) < n:
        return np.full(len(arr), arr[-1] if len(arr) else 0.0)
    w = np.arange(1, n + 1, dtype=np.float64)
    conv = np.convolve(arr, w[::-1] / w.sum(), mode="valid")
    return np.concatenate([np.full(n - 1, conv[0]), conv])


def calc_hma(c: np.ndarray, n: int = 50) -> np.ndarray:
    """
    Hull Moving Average — 3× más reactivo que EMA
    HMA(n) = WMA(2·WMA(n/2) − WMA(n), √n)
    """
    half = max(2, n // 2)
    sq   = max(2, int(np.sqrt(n)))
    return _wma(2 * _wma(c, half) - _wma(c, n), sq)


def calc_atr(h: np.ndarray, l: np.ndarray, c: np.ndarray, n: int = 14) -> float:
    """ATR de Wilder"""
    if len(c) < n + 1:
        return float(np.mean(h - l))
    tr = np.maximum(
        h[1:] - l[1:],
        np.maximum(np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1]))
    )
    tr = np.concatenate([[h[0] - l[0]], tr])
    atr = np.zeros(len(tr))
    atr[n - 1] = np.mean(tr[:n])
    for i in range(n, len(tr)):
        atr[i] = (atr[i - 1] * (n - 1) + tr[i]) / n
    return float(atr[-1])


def calc_pivots(h: np.ndarray, l: np.ndarray, pivot_len: int = 5) -> tuple[float, float]:
    """
    Pivot High / Pivot Low (equivalente al ZigZag++ de TradingView).
    Retorna (último pico, último valle).
    Un pivot high = máximo > todos sus vecinos dentro de ±pivot_len velas.
    """
    n = len(h)
    if n < 2 * pivot_len + 1:
        return np.nan, np.nan

    last_peak   = np.nan
    last_valley = np.nan

    # Buscamos de más reciente a más antiguo para coger el último
    for i in range(n - pivot_len - 1, pivot_len - 1, -1):
        window_h = h[i - pivot_len: i + pivot_len + 1]
        if h[i] == window_h.max() and np.isnan(last_peak):
            last_peak = float(h[i])

        window_l = l[i - pivot_len: i + pivot_len + 1]
        if l[i] == window_l.min() and np.isnan(last_valley):
            last_valley = float(l[i])

        if not np.isnan(last_peak) and not np.isnan(last_valley):
            break

    return last_peak, last_valley


def calc_volume_delta(c: np.ndarray, o: np.ndarray, v: np.ndarray,
                      period: int = 25) -> float:
    """
    Future-Trend Volume Delta (replicado del Pine Script).
    Promedia el delta de volumen en 3 ventanas desplazadas.
    Positivo = presión compradora · Negativo = presión vendedora
    """
    delta = np.where(c > o, v, np.where(c < o, -v, 0.0))
    n = len(delta)
    if n < period * 3:
        return 0.0

    total = 0.0
    for i in range(period):
        d0 = delta[n - 1 - i]
        d1 = delta[n - 1 - i - period]      if n - 1 - i - period >= 0      else 0.0
        d2 = delta[n - 1 - i - period * 2]  if n - 1 - i - period * 2 >= 0 else 0.0
        total += (d0 + d1 + d2) / 3.0

    return total / period


# ══════════════════════════════════════════════════════════════
#  MOTOR DE SEÑALES — ZigZag++ 15m
#
#  LONG  → close cruza por encima del último pico (ruptura)
#          + HMA alcista (slope positivo, precio > HMA)
#          + Volume Delta positivo (presión compradora)
#          + Volumen de la vela > media×1.2 (anti-fakeout)
#
#  SHORT → close cruza por debajo del último valle (ruptura)
#          + HMA bajista
#          + Volume Delta negativo
#          + Volumen spike
#
#  TP = entry ± TP_PIPS × pip_value
#  SL = entry ∓ SL_PIPS × pip_value
#  pip_value = ATR / ATR_LEN  (proxy de 1 pip adaptativo al activo)
# ══════════════════════════════════════════════════════════════
def analyze(candles: list[dict]) -> Optional[dict]:
    if len(candles) < 120:
        return None

    h = np.array([c["h"] for c in candles])
    l = np.array([c["l"] for c in candles])
    c = np.array([c["c"] for c in candles])
    o = np.array([c["o"] for c in candles])
    v = np.array([c["v"] for c in candles])

    close = float(c[-1])
    prev  = float(c[-2])

    # ── Filtro ATR mínimo (evita activos sin movimiento) ──────
    atr = calc_atr(h, l, c, ATR_LEN)
    if close <= 0 or (atr / close * 100) < MIN_ATR_PCT:
        return None

    # ── pip_value = precio de 1 pip adaptativo ───────────────
    # ATR / ATR_LEN da la volatilidad media por vela / ATR_LEN
    # Escalamos TP y SL con esa unidad
    pip_val = atr / ATR_LEN

    # ── ZigZag++ pivots ───────────────────────────────────────
    peak, valley = calc_pivots(h, l, PIVOT_LEN)
    if np.isnan(peak) or np.isnan(valley):
        return None

    # ── HMA ───────────────────────────────────────────────────
    hma = calc_hma(c, HMA_LEN)
    hma_bullish = close > hma[-1] and hma[-1] > hma[-2]
    hma_bearish = close < hma[-1] and hma[-1] < hma[-2]

    # ── Volume Delta (Future-Trend) ───────────────────────────
    vdelta      = calc_volume_delta(c, o, v, FT_PERIOD)
    flow_bull   = vdelta > 0
    flow_bear   = vdelta < 0

    # ── Volumen spike anti-fakeout ────────────────────────────
    vol_ma      = float(np.mean(v[-20:])) if len(v) >= 20 else 1.0
    vol_spike   = float(v[-1]) > vol_ma * VOL_MULT

    # ── Precio mínimo por encima de breakout (sin gap enorme) ─
    max_gap_mult = 1.5  # la ruptura no puede ser > 1.5×ATR del nivel
    within_long  = (close - peak)   < atr * max_gap_mult
    within_short = (valley - close) < atr * max_gap_mult

    # ─────────────────────────────────────────────────────────
    #  LONG: ruptura confirmada del pico
    # ─────────────────────────────────────────────────────────
    long_break  = prev <= peak < close   # crossover exacto
    long_cond   = (long_break and hma_bullish and flow_bull
                   and vol_spike and within_long)

    # ─────────────────────────────────────────────────────────
    #  SHORT: ruptura confirmada del valle
    # ─────────────────────────────────────────────────────────
    short_break = prev >= valley > close  # crossunder exacto
    short_cond  = (short_break and hma_bearish and flow_bear
                   and vol_spike and within_short)

    # ─────────────────────────────────────────────────────────
    #  Calcular TP y SL en unidades de precio (pips→precio)
    # ─────────────────────────────────────────────────────────
    if long_cond:
        tp_price = close + TP_PIPS * pip_val
        sl_price = close - SL_PIPS * pip_val
        return {
            "side":     "BUY",
            "entry":    close,
            "tp":       round(tp_price, 6),
            "sl":       round(sl_price, 6),
            "pip_val":  pip_val,
            "atr":      atr,
            "peak":     peak,
            "valley":   valley,
            "vdelta":   vdelta,
            "hma":      float(hma[-1]),
            "reasons":  ["ZZ++_break_peak", "HMA_bull", f"VDelta+{vdelta:.0f}", "VolSpike"],
        }

    if short_cond:
        tp_price = close - TP_PIPS * pip_val
        sl_price = close + SL_PIPS * pip_val
        return {
            "side":     "SELL",
            "entry":    close,
            "tp":       round(tp_price, 6),
            "sl":       round(sl_price, 6),
            "pip_val":  pip_val,
            "atr":      atr,
            "peak":     peak,
            "valley":   valley,
            "vdelta":   vdelta,
            "hma":      float(hma[-1]),
            "reasons":  ["ZZ++_break_valley", "HMA_bear", f"VDelta{vdelta:.0f}", "VolSpike"],
        }

    return None


# ══════════════════════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════════════════════
async def tg(session: aiohttp.ClientSession, msg: str):
    if not TG_TOKEN or not TG_CHAT:
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        await session.post(url, json={
            "chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"
        })
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════
#  BOT PRINCIPAL
# ══════════════════════════════════════════════════════════════
class PhantomEdgeBot:
    def __init__(self):
        self.bx          = BingXClient()
        self.symbols:    list[str]           = []
        self.candles:    dict[str, list]     = {}      # symbol → candles 15m
        self.warm:       set[str]            = set()
        self.open_pos:   dict[str, dict]     = {}
        self.cooldown:   dict[str, float]    = defaultdict(float)
        self.balance:    float               = 0.0
        self.cycle:      int                 = 0
        self.start_t:    float               = time.time()
        self.daily_trades: int               = 0
        self.last_day    = datetime.now(timezone.utc).date()
        self._tg_sess:   Optional[aiohttp.ClientSession] = None

    @property
    def tg_session(self) -> aiohttp.ClientSession:
        if self._tg_sess is None or self._tg_sess.closed:
            self._tg_sess = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8))
        return self._tg_sess

    # ── Telegram helper ───────────────────────────────────────
    async def notify(self, msg: str):
        await tg(self.tg_session, msg)

    # ── Warm-up de un símbolo ─────────────────────────────────
    async def warmup_sym(self, sym: str) -> bool:
        try:
            klines = await self.bx.get_klines(sym, TIMEFRAME, 150)
            if len(klines) >= 100:
                self.candles[sym] = klines
                self.warm.add(sym)
                return True
        except Exception:
            pass
        return False

    async def warmup_batch(self, syms: list[str], batch: int = 10):
        done = 0
        for i in range(0, len(syms), batch):
            group = syms[i: i + batch]
            res = await asyncio.gather(
                *[self.warmup_sym(s) for s in group], return_exceptions=True
            )
            done += sum(1 for r in res if r is True)
            log.info(f"  WarmUp {done}/{len(syms)}")
            await asyncio.sleep(0.3)
        return done

    # ── Actualización incremental de velas ────────────────────
    async def update_sym(self, sym: str):
        new = await self.bx.get_klines(sym, TIMEFRAME, 3)
        if not new:
            return
        existing = self.candles.get(sym, [])
        last_t = existing[-1]["t"] if existing else 0
        for k in new:
            if k["t"] > last_t:
                existing.append(k)
            elif k["t"] == last_t and existing:
                existing[-1] = k  # actualiza vela actual
        self.candles[sym] = existing[-150:]  # mantener ventana

    # ── Control diario ────────────────────────────────────────
    def _reset_daily(self):
        today = datetime.now(timezone.utc).date()
        if today != self.last_day:
            self.daily_trades = 0
            self.last_day = today
            log.info("📅 Contador diario reseteado")

    # ── Ciclo principal de escaneo ────────────────────────────
    async def scan(self):
        self.cycle += 1
        self._reset_daily()

        # Estado del mercado
        self.balance, pos_raw = await asyncio.gather(
            self.bx.get_balance(),
            self.bx.get_positions(),
        )
        self.open_pos = {p["symbol"]: p for p in pos_raw}

        now = time.time()
        log.info(
            f"[{self.cycle:04d}] BAL:{self.balance:.2f}U │ "
            f"POS:{len(self.open_pos)}/{MAX_POS} │ "
            f"WARM:{len(self.warm)} │ "
            f"{'AUTO-ON' if AUTO_TRADE else 'SIM'}"
        )

        # Guard: máximo de posiciones
        if len(self.open_pos) >= MAX_POS:
            log.info("  ⛔ Máximo de posiciones alcanzado")
            return

        # Candidatos = calientes, sin posición, sin cooldown
        candidates = [
            s for s in self.warm
            if s not in self.open_pos
            and now - self.cooldown[s] > COOLDOWN_SEC
        ]

        if not candidates:
            log.info("  ℹ️  Sin candidatos disponibles")
            return

        # Actualizar velas en lotes paralelos
        for i in range(0, len(candidates), BATCH_SIZE):
            await asyncio.gather(
                *[self.update_sym(s) for s in candidates[i: i + BATCH_SIZE]],
                return_exceptions=True,
            )

        signals = 0
        for sym in candidates:
            if len(self.open_pos) >= MAX_POS:
                break

            klines = self.candles.get(sym, [])
            sig = analyze(klines)
            if not sig:
                continue

            signals += 1
            self.cooldown[sym] = now

            side_emoji = "🟢" if sig["side"] == "BUY" else "🔴"
            direction  = "LONG" if sig["side"] == "BUY" else "SHORT"

            # Calcular % de movimiento con apalancamiento
            entry    = sig["entry"]
            tp_move  = abs(entry - sig["tp"]) / entry * 100
            sl_move  = abs(entry - sig["sl"]) / entry * 100
            roi_lev  = tp_move * LEVERAGE
            loss_lev = sl_move * LEVERAGE

            log.info(
                f"  {side_emoji} {sym} {direction} │ "
                f"Entry:{entry:.4f} TP:{sig['tp']:.4f}(+{tp_move:.2f}%) "
                f"SL:{sig['sl']:.4f}(-{sl_move:.2f}%) │ "
                f"{' · '.join(sig['reasons'])}"
            )

            if AUTO_TRADE:
                # Tamaño de posición por riesgo fijo
                sl_dist  = abs(entry - sig["sl"])
                risk_pct = sl_dist / entry
                if risk_pct < 0.0001:
                    continue
                qty = round((TRADE_USDT / risk_pct) / entry, 4)
                if qty <= 0:
                    continue

                ok = await self.bx.place_order(
                    sym, sig["side"], qty,
                    sig["sl"], sig["tp"], entry,
                )
                if ok:
                    self.daily_trades += 1
                    self.open_pos[sym] = {"symbol": sym, "side": sig["side"]}
                    gain_usdt = TRADE_USDT * risk_pct * LEVERAGE * (TP_PIPS / SL_PIPS)
                    loss_usdt = TRADE_USDT * risk_pct * LEVERAGE

                    await self.notify(
                        f"{side_emoji} <b>{sym}</b> — {direction}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📍 Entry : <code>{entry:.4f}</code>\n"
                        f"🎯 TP    : <code>{sig['tp']:.4f}</code>  +{TP_PIPS}pips  "
                        f"<b>+{roi_lev:.1f}%</b>  (+${gain_usdt:.2f})\n"
                        f"🛡 SL    : <code>{sig['sl']:.4f}</code>  -{SL_PIPS}pips  "
                        f"-{loss_lev:.1f}%  (-${loss_usdt:.2f})\n"
                        f"━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📊 RR: 1:{TP_PIPS/SL_PIPS:.1f} │ x{LEVERAGE} │ qty={qty}\n"
                        f"📈 ATR: {sig['atr']:.4f} │ pip_val: {sig['pip_val']:.6f}\n"
                        f"🔑 Nivel: {'Pico' if sig['side']=='BUY' else 'Valle'}: "
                        f"{sig['peak']:.4f} / {sig['valley']:.4f}\n"
                        f"✨ {' · '.join(sig['reasons'])}\n"
                        f"💰 Balance: {self.balance:.2f} USDT"
                    )
            else:
                gain_usdt = TRADE_USDT * (TP_PIPS / SL_PIPS)
                await self.notify(
                    f"🔔 <b>[SIM] {sym}</b> — {direction}\n"
                    f"Entry:{entry:.4f} │ TP:{sig['tp']:.4f} │ SL:{sig['sl']:.4f}\n"
                    f"RR:1:{TP_PIPS/SL_PIPS:.1f} │ ROI≈+{roi_lev:.1f}% │ "
                    f"SIM_Gain≈+${gain_usdt:.2f}\n"
                    f"✨ {' · '.join(sig['reasons'])}"
                )

        log.info(f"  SCAN: {len(candidates)} candidatos │ {signals} señales")

    # ── Loop principal ────────────────────────────────────────
    async def run(self):
        log.info("═" * 58)
        log.info("  PHANTOM EDGE BOT — ULTIMATE EDITION")
        log.info(f"  Estrategia: ZigZag++ 15m  TP:{TP_PIPS}pip  SL:{SL_PIPS}pip")
        log.info(f"  RR: 1:{TP_PIPS/SL_PIPS:.1f} │ Lev:x{LEVERAGE} │ "
                 f"{'AUTO-TRADE' if AUTO_TRADE else 'SIMULACION'}")
        log.info("═" * 58)

        self.balance = await self.bx.get_balance()
        log.info(f"💰 Balance: {self.balance:.2f} USDT")

        if self.balance == 0.0 and API_KEY:
            log.warning("⚠️  Balance=0. Verifica BINGX_API_KEY y permisos Futures.")

        # Filtrar símbolos por volumen mínimo 24h
        raw_syms = await self.bx.get_symbols()
        log.info(f"📊 Cargando {len(raw_syms)} símbolos...")

        # Filtro de volumen 24h en paralelo (lotes de 20)
        filtered: list[str] = []
        for i in range(0, min(len(raw_syms), 300), 20):
            batch = raw_syms[i: i + 20]
            tickers = await asyncio.gather(
                *[self.bx.get_ticker(s["symbol"]) for s in batch],
                return_exceptions=True,
            )
            for s, t in zip(batch, tickers):
                if isinstance(t, dict):
                    vol = float(t.get("quoteVolume", 0) or t.get("volume", 0) or 0)
                    if vol >= MIN_VOL_24H:
                        filtered.append(s["symbol"])

        self.symbols = filtered
        log.info(f"✅ {len(self.symbols)} pares con volumen ≥ ${MIN_VOL_24H:,.0f}")

        if not self.symbols:
            log.error("❌ Sin símbolos. Revisa MIN_VOLUME_USDT.")
            return

        await self.notify(
            f"🤖 <b>Phantom Edge Bot — Ultimate</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 Balance: {self.balance:.2f} USDT\n"
            f"📊 Pares activos: {len(self.symbols)}\n"
            f"⏱ Timeframe: {TIMEFRAME}\n"
            f"🎯 TP:{TP_PIPS}pip  🛡SL:{SL_PIPS}pip  RR:1:{TP_PIPS/SL_PIPS:.1f}\n"
            f"⚙️ Lev:x{LEVERAGE}  MaxPos:{MAX_POS}\n"
            f"{'🟢 AUTO-TRADE ACTIVO' if AUTO_TRADE else '🟡 MODO SIMULACIÓN'}"
        )

        # Warm-up inicial
        log.info(f"🔥 WarmUp de {len(self.symbols)} pares en 15m...")
        await self.warmup_batch(self.symbols, BATCH_SIZE)
        log.info(f"✅ {len(self.warm)} pares calientes. Iniciando ciclos...\n")

        # Loop infinito de trading
        while True:
            try:
                t0 = time.time()
                await self.scan()
                elapsed = time.time() - t0
                sleep   = max(5.0, SCAN_SEC - elapsed)
                log.info(f"  ⏱ {elapsed:.1f}s │ próximo:{sleep:.0f}s\n")
                await asyncio.sleep(sleep)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"❌ Error ciclo: {e}", exc_info=False)
                await asyncio.sleep(20)

        await self.bx.close()
        if self._tg_sess and not self._tg_sess.closed:
            await self._tg_sess.close()
        log.info("Bot detenido.")


# ══════════════════════════════════════════════════════════════
#  HEALTH CHECK SERVER (Railway lo necesita para el healthcheck)
# ══════════════════════════════════════════════════════════════
async def health_server(bot: PhantomEdgeBot):
    from aiohttp import web

    async def handle(req: web.Request):
        uptime = round((time.time() - bot.start_t) / 60, 1)
        return web.json_response({
            "status":       "running",
            "uptime_min":   uptime,
            "cycle":        bot.cycle,
            "balance_usdt": round(bot.balance, 2),
            "warm_symbols": len(bot.warm),
            "total_symbols": len(bot.symbols),
            "open_positions": len(bot.open_pos),
            "daily_trades": bot.daily_trades,
            "auto_trade":   AUTO_TRADE,
            "strategy":     f"ZigZag++ {TIMEFRAME} TP{TP_PIPS}pip SL{SL_PIPS}pip",
        })

    app = web.Application()
    app.router.add_get("/",       handle)
    app.router.add_get("/health", handle)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info(f"🌐 Health → http://0.0.0.0:{PORT}/health")


# ══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════
async def main():
    bot = PhantomEdgeBot()
    await asyncio.gather(
        health_server(bot),
        bot.run(),
    )


if __name__ == "__main__":
    asyncio.run(main())
