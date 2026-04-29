# ⚡ UltraBot v2 — Multi-Symbol Async Trading Bot

High-performance automated trading bot for **BingX Perpetual Futures**.
Scans **50+ symbols simultaneously**, executes in milliseconds.

---

## 🏗 Architecture

```
ultrabot/
├── bot.py                    ← Async orchestrator (main entry)
├── core/
│   ├── config.py             ← All config from env vars
│   └── risk.py               ← Risk engine (sizing, drawdown, daily limits)
├── exchange/
│   └── client.py             ← Async BingX client (aiohttp + connection pool)
├── strategies/
│   └── indicators.py         ← JIT-compiled indicators (NumPy + Numba)
└── notifications/
    └── telegram.py           ← Async Telegram with rate limiting
```

---

## ⚡ Speed Optimizations

| Feature | Tech | Benefit |
|---|---|---|
| Async event loop | `uvloop` | 2× faster than stdlib asyncio |
| HTTP connection pool | `aiohttp` 200 connections | No TCP handshake overhead |
| Concurrent kline fetch | `asyncio.gather` | 50 symbols fetched simultaneously |
| JIT indicators | `numba @njit` | Compiled to native machine code |
| Fast JSON | `orjson` | 10× faster than stdlib json |
| Parallel CPU work | `ThreadPoolExecutor` | Indicators across all symbols in parallel |
| DNS cache | `aiohttp` | No DNS lookup per request |

**Typical scan time: 200–600ms for 50 symbols**

---

## 🧠 Strategy: Three-Step Future-Trend + ADX + Multi-TF

### Signal Generation

```
PRIMARY (15m):
  Three-Step Volume Delta:
    δ1 = Σ(delta_vol, period)          ← Most recent window
    δ2 = Σ(delta_vol, 2×period) - δ1  ← Mid window
    δ3 = Σ(delta_vol, 3×period) - δ1 - δ2  ← Oldest window

  LONG when:  ≥2 bullish deltas + ADX ≥ 30 + +DI > -DI + RSI < 75
  SHORT when: ≥2 bearish deltas + ADX ≥ 30 + -DI > +DI + RSI > 25

CONFIRMATION (1h):
  Higher TF must NOT disagree with primary signal
  → Filters false breakouts dramatically

CONFIDENCE SCORE:
  = (delta_agreement × 33) + ADX_excess + volume_spike_bonus
  → Signals ranked → best confidence executed first
```

### Risk Management

- **Per-trade size**: `balance × RISK_PCT% × confidence_scale`
- **Stop Loss**: Auto SL/TP placed immediately after entry (parallel)
- **Trailing Stop**: Locks in profit as price moves in favor
- **Max drawdown**: Halts bot if portfolio drops > 10% from peak
- **Daily loss limit**: Stops trading if -5% on the day
- **Max concurrent trades**: 5 (configurable)
- **Slot scaling**: Each open trade reduces next size by 30%

---

## 🚀 Deploy on Railway

### 1. Prepare repo

```bash
git clone https://github.com/YOUR/ultrabot.git
cd ultrabot
cp .env.example .env
# Edit .env with your keys
```

### 2. Get BingX API Keys
1. [BingX](https://bingx.com) → Account → API Management
2. Enable **Futures trading** (NO withdrawal permission!)
3. Whitelist Railway's IP or leave unrestricted

### 3. Get Telegram credentials
1. `/newbot` → [@BotFather](https://t.me/BotFather) → copy token
2. [@userinfobot](https://t.me/userinfobot) → copy your chat ID
3. Send `/start` to your new bot

### 4. Deploy

```bash
# Via Railway CLI
npm i -g @railway/cli
railway login && railway init && railway up

# Add all variables:
railway variables set \
  BINGX_API_KEY=xxx BINGX_SECRET_KEY=xxx \
  TELEGRAM_TOKEN=xxx TELEGRAM_CHAT_ID=xxx \
  TIMEFRAME=15m CONFIRM_TF=1h \
  PERIOD=25 ADX_THRESH=30 \
  MAX_OPEN_TRADES=5 LEVERAGE=5 RISK_PCT=1 \
  SL_PCT=2 TP_PCT=4 TRAILING_SL=true \
  SCAN_INTERVAL=10 TOP_N_SYMBOLS=50
```

Or push to GitHub and connect at [railway.app](https://railway.app).

---

## ⚙️ Configuration Reference

| Variable | Default | Description |
|---|---|---|
| `TIMEFRAME` | `15m` | Primary signal timeframe |
| `CONFIRM_TF` | `1h` | HTF confirmation (must agree) |
| `PERIOD` | `25` | Three-Step window size |
| `ADX_THRESH` | `30` | Min ADX for valid trend |
| `MIN_VOLUME_USDT` | `5000000` | Min 24h vol to include symbol |
| `TOP_N_SYMBOLS` | `50` | Symbols to scan each cycle |
| `MAX_OPEN_TRADES` | `5` | Max concurrent positions |
| `LEVERAGE` | `5` | Futures leverage |
| `RISK_PCT` | `1.0` | % balance per trade |
| `SL_PCT` | `2.0` | Stop loss % |
| `TP_PCT` | `4.0` | Take profit % |
| `TRAILING_SL` | `true` | Trailing stop |
| `SCAN_INTERVAL` | `10` | Seconds between scans |
| `TOP_N_SYMBOLS` | `50` | Universe size |

---

## 📲 Telegram Messages

| Event | Message |
|---|---|
| Start | Bot online, universe size, config summary |
| LONG opened | Symbol, entry, SL/TP, size, ADX, RSI, delta, confidence bar |
| SHORT opened | Same as above |
| Position closed | Symbol, PnL, reason (TP/SL/Trailing/Flip) |
| Scan summary | Symbols scanned, signals found, scan time |
| Error | Truncated error for debugging |
| Halt | Reason for emergency stop |

---

## ⚠️ Risk Disclaimer

Trading perpetual futures with leverage involves **substantial risk of loss**.
- Start with **LEVERAGE=2** and **RISK_PCT=0.5** for at least 1 week
- Monitor Telegram notifications actively
- Keep bot balance separate from your main funds
- Never trade money you cannot afford to lose
- Past backtested performance does not guarantee live results
