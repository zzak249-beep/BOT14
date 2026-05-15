/**
 * MZ SAMA Multi-Symbol Trading Bot
 * Scans ALL BingX USDT perpetual futures every candle close
 * Opens/closes positions automatically with Telegram alerts
 */

require('dotenv').config();

const MultiScanner      = require('./scanner');
const PositionManager   = require('./positions');
const TelegramNotifier  = require('./telegram');
const BingXClient       = require('./bingx');
const RiskManager       = require('./risk');
const logger            = require('./logger');

// ── Config ────────────────────────────────────────────────────────────────────

const CONFIG = {
  // BingX credentials
  bingxApiKey:    process.env.BINGX_API_KEY,
  bingxSecretKey: process.env.BINGX_SECRET_KEY,

  // Candle timeframe
  interval:       process.env.INTERVAL       || '1h',

  // SAMA parameters (same as Pine Script defaults)
  length:         parseInt(process.env.SAMA_LENGTH    || '200'),
  majLength:      parseInt(process.env.MAJ_LENGTH     || '14'),
  minLength:      parseInt(process.env.MIN_LENGTH     || '6'),
  slopePeriod:    parseInt(process.env.SLOPE_PERIOD   || '34'),
  slopeInRange:   parseInt(process.env.SLOPE_RANGE    || '25'),
  flat:           parseInt(process.env.FLAT           || '17'),

  // Scanner filters
  maxSymbols:     parseInt(process.env.MAX_SYMBOLS    || '80'),   // top N by volume
  minVolume24h:   parseFloat(process.env.MIN_VOL      || '1000000'), // min $1M daily vol

  // Position management
  maxPositions:   parseInt(process.env.MAX_POSITIONS  || '5'),    // max concurrent trades
  leverage:       parseInt(process.env.LEVERAGE       || '5'),
  riskPct:        parseFloat(process.env.RISK_PCT     || '1'),    // % balance per trade
  tpPct:          parseFloat(process.env.TP_PCT       || '2'),
  slPct:          parseFloat(process.env.SL_PCT       || '1'),
  minQty:         parseFloat(process.env.MIN_QTY      || '0.001'),
  qtyStep:        parseFloat(process.env.QTY_STEP     || '0.001'),

  // Operational
  scanEvery:      parseInt(process.env.SCAN_EVERY     || '1'),    // scan every N candles
  summaryEvery:   parseInt(process.env.SUMMARY_EVERY  || '6'),    // send summary every N scans
  dryRun:         process.env.DRY_RUN !== 'false',                // default TRUE for safety
};

CONFIG.warmupCandles = CONFIG.length + 60;

// ── Clients ───────────────────────────────────────────────────────────────────

const bingx     = new BingXClient(CONFIG.bingxApiKey, CONFIG.bingxSecretKey);
const telegram  = new TelegramNotifier(process.env.TELEGRAM_TOKEN, process.env.TELEGRAM_CHAT_ID);
const scanner   = new MultiScanner(CONFIG);
const positions = new PositionManager(CONFIG);
const risk      = new RiskManager(CONFIG);

let scanCount = 0;

// ── Helpers ───────────────────────────────────────────────────────────────────

function intervalToMs(iv) {
  const map = { '1m':60e3,'3m':180e3,'5m':300e3,'15m':900e3,'30m':1800e3,
                '1h':3600e3,'2h':7200e3,'4h':14400e3,'6h':21600e3,'1d':86400e3 };
  return map[iv] || 3600e3;
}

async function getBalance() {
  if (CONFIG.dryRun) return 1000; // paper balance
  const res  = await bingx.getBalance();
  const usdt = (res.data?.balance || []).find(b => b.asset === 'USDT');
  return parseFloat(usdt?.availableMargin || usdt?.balance || '0');
}

// ── Trade execution ───────────────────────────────────────────────────────────

async function openLong(scanResult) {
  const { symbol, price, sama, slope } = scanResult;
  const balance    = await getBalance();
  const perTrade   = balance / CONFIG.maxPositions;
  const qty        = risk.calcQuantity(perTrade, price);
  const { tp, sl } = risk.calcTPSL(price, 'LONG');

  if (!CONFIG.dryRun) {
    await bingx.setLeverage(symbol, CONFIG.leverage, 'LONG');
    await bingx.marketOrder(symbol, 'BUY', 'LONG', qty);
    await bingx.setTPSL(symbol, 'LONG', tp, sl);
  }

  positions.open(symbol, { side: 'LONG', entry: price, qty, tp, sl });
  await telegram.sendBuy({ symbol, price, quantity: qty, leverage: CONFIG.leverage, tp, sl, sama, slope });
}

async function openShort(scanResult) {
  const { symbol, price, sama, slope } = scanResult;
  const balance    = await getBalance();
  const perTrade   = balance / CONFIG.maxPositions;
  const qty        = risk.calcQuantity(perTrade, price);
  const { tp, sl } = risk.calcTPSL(price, 'SHORT');

  if (!CONFIG.dryRun) {
    await bingx.setLeverage(symbol, CONFIG.leverage, 'SHORT');
    await bingx.marketOrder(symbol, 'SELL', 'SHORT', qty);
    await bingx.setTPSL(symbol, 'SHORT', tp, sl);
  }

  positions.open(symbol, { side: 'SHORT', entry: price, qty, tp, sl });
  await telegram.sendSell({ symbol, price, quantity: qty, leverage: CONFIG.leverage, tp, sl, sama, slope });
}

async function closePosition(symbol, currentPrice) {
  const pos = positions.get(symbol);
  if (!pos) return;

  const { side, entry, qty } = pos;
  const pnl = side === 'LONG'
    ? (currentPrice - entry) * qty * CONFIG.leverage
    : (entry - currentPrice) * qty * CONFIG.leverage;

  if (!CONFIG.dryRun) {
    await bingx.cancelAllOrders(symbol);
    await bingx.closePosition(symbol, side);
  }

  positions.close(symbol);
  await telegram.sendClose({ symbol, side, pnl, price: currentPrice });
}

// ── Main scan loop ────────────────────────────────────────────────────────────

async function tick() {
  scanCount++;
  logger.info(`━━━ Scan #${scanCount} ━━━  Positions: ${positions.openCount}/${CONFIG.maxPositions}`);

  try {
    // 1. Scan all symbols
    const allResults = await scanner.scanAll(CONFIG.warmupCandles);

    // 2. Get new signals only (color transitions)
    const newSignals = scanner.getNewSignals(allResults);

    // 3. Handle existing positions — check for reverse signals
    for (const pos of positions.all) {
      const symbolResult = allResults.find(r => r.symbol === pos.symbol);
      if (!symbolResult) continue;

      // Close on reverse signal
      if (pos.side === 'LONG'  && symbolResult.signal === 'SELL') {
        logger.info(`Reversing LONG → SHORT on ${pos.symbol}`);
        await closePosition(pos.symbol, symbolResult.price);
      }
      if (pos.side === 'SHORT' && symbolResult.signal === 'BUY') {
        logger.info(`Reversing SHORT → LONG on ${pos.symbol}`);
        await closePosition(pos.symbol, symbolResult.price);
      }
    }

    // 4. Notify new signals (even if we can't trade them all)
    if (newSignals.length > 0) {
      logger.info(`New signals: ${newSignals.map(s => `${s.symbol}:${s.signal}`).join(', ')}`);
      await telegram.sendMultiSignal(newSignals);
    }

    // 5. Open positions for actionable signals (within max capacity)
    const tradeable = newSignals.filter(s => !positions.has(s.symbol));

    for (const sig of tradeable) {
      if (!positions.hasCapacity) {
        logger.info(`Max positions (${CONFIG.maxPositions}) reached, skipping ${sig.symbol}`);
        break;
      }

      try {
        if (sig.signal === 'BUY')  await openLong(sig);
        if (sig.signal === 'SELL') await openShort(sig);
        // Small delay between orders
        await new Promise(r => setTimeout(r, 200));
      } catch (err) {
        logger.error(`Failed to open position on ${sig.symbol}: ${err.message}`);
        await telegram.sendError(`open position ${sig.symbol}`, err.message);
      }
    }

    // 6. Periodic market summary
    if (scanCount % CONFIG.summaryEvery === 0) {
      const overview = scanner.getMarketOverview(allResults);
      const topBull  = allResults.filter(r => r.color === 'bull').sort((a,b) => b.slope - a.slope);
      const topBear  = allResults.filter(r => r.color === 'bear').sort((a,b) => a.slope - b.slope);
      const balance  = await getBalance();

      await telegram.sendScanSummary({
        ...overview,
        interval: CONFIG.interval,
        topBull,
        topBear
      });
      await telegram.sendPositionsSummary(positions.all, balance.toFixed(2));
    }

  } catch (err) {
    logger.error(`Tick #${scanCount} error: ${err.message}`);
    await telegram.sendError(`tick #${scanCount}`, err.message);
  }
}

// ── Entry point ───────────────────────────────────────────────────────────────

async function main() {
  logger.info('╔═══════════════════════════════════════╗');
  logger.info('║  MZ SAMA Multi-Symbol Bot — Starting  ║');
  logger.info('╚═══════════════════════════════════════╝');
  logger.info(`Interval: ${CONFIG.interval} | MaxSymbols: ${CONFIG.maxSymbols} | MaxPositions: ${CONFIG.maxPositions} | DryRun: ${CONFIG.dryRun}`);

  if (!CONFIG.bingxApiKey || !CONFIG.bingxSecretKey) {
    logger.error('BINGX_API_KEY and BINGX_SECRET_KEY are required!');
    process.exit(1);
  }

  await telegram.sendStart(CONFIG);

  // First tick immediately
  await tick();

  // Then every candle interval
  const ms = intervalToMs(CONFIG.interval);
  logger.info(`Next scan in ${ms / 1000}s`);
  setInterval(tick, ms);
}

main().catch(async err => {
  logger.error('Fatal: ' + err.message);
  await telegram.sendError('main()', err.message);
  process.exit(1);
});
