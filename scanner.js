/**
 * Multi-Symbol Scanner
 * Fetches all USDT perpetual futures from BingX and filters by volume/signal
 *
 * FIXES APPLIED:
 *  1. Candles reversed (BingX returns newest-first, strategy needs oldest-first)
 *  2. Signal detection tracks color changes between scans (not within a single batch)
 */

const BingXClient  = require('./bingx');
const SAMAStrategy = require('./strategy');
const logger       = require('./logger');

class MultiScanner {
  constructor(config) {
    this.config   = config;
    this.bingx    = new BingXClient(config.bingxApiKey, config.bingxSecretKey);
    this.strategies  = new Map(); // symbol → SAMAStrategy instance
    this.lastColors  = new Map(); // symbol → last known color ('bull'|'bear'|'chop')

    // Filter thresholds
    this.minVolume24h = config.minVolume24h || 500_000;
    this.maxSymbols   = config.maxSymbols   || 50;
    this.minPrice     = config.minPrice     || 0.000001;
  }

  // ── Fetch 24h tickers for all symbols and rank by volume ───────────────

  async getRankedSymbols() {
    try {
      const res     = await this.bingx._get('/openApi/swap/v2/quote/ticker');
      const tickers = Array.isArray(res.data) ? res.data : [];

      const filtered = tickers
        .filter(t =>
          t.symbol.endsWith('-USDT') &&
          parseFloat(t.quoteVolume || t.volume || 0) >= this.minVolume24h
        )
        .sort((a, b) =>
          parseFloat(b.quoteVolume || b.volume || 0) -
          parseFloat(a.quoteVolume || a.volume || 0)
        )
        .slice(0, this.maxSymbols)
        .map(t => ({
          symbol: t.symbol,
          price:  parseFloat(t.lastPrice),
          vol24h: parseFloat(t.quoteVolume || t.volume || 0),
          change: parseFloat(t.priceChangePercent || 0)
        }));

      logger.info(`Ranked symbols: ${filtered.length} pairs (min vol $${this.minVolume24h.toLocaleString()})`);
      return filtered;
    } catch (err) {
      logger.error(`getRankedSymbols error: ${err.message}`);
      return [];
    }
  }

  // ── Fetch candles and run SAMA for one symbol ──────────────────────────

  async scanSymbol(symbol, warmupCandles) {
    try {
      const res = await this.bingx.getKlines(symbol, this.config.interval, warmupCandles + 10);
      if (!res.data || res.data.length < 50) return null;

      // FIX 1: BingX returns candles newest-first → reverse to oldest-first
      const candles = res.data
        .map(k => ({
          time:  k[0],
          open:  parseFloat(k[1]),
          high:  parseFloat(k[2]),
          low:   parseFloat(k[3]),
          close: parseFloat(k[4]),
          vol:   parseFloat(k[5])
        }))
        .reverse(); // ← crítico: estrategia necesita orden cronológico ascendente

      // Get or create strategy instance for this symbol
      if (!this.strategies.has(symbol)) {
        this.strategies.set(symbol, new SAMAStrategy(this.config));
      }
      const strat = this.strategies.get(symbol);
      strat.reset();

      let result = null;
      for (const candle of candles) {
        result = strat.update(candle);
      }

      if (!result || result.sama === null) return null;

      // Use last CLOSED candle (second to last) for price, last result for signal
      const lastClosedCandle = candles[candles.length - 2];
      return {
        symbol,
        price:  lastClosedCandle.close,
        vol24h: candles.slice(-24).reduce((s, c) => s + c.vol, 0),
        sama:   result.sama,
        slope:  result.slope,
        color:  result.color,
        signal: null // signal is determined by getNewSignals() across scans
      };
    } catch (err) {
      logger.warn(`scanSymbol(${symbol}): ${err.message}`);
      return null;
    }
  }

  // ── Scan all symbols, return results with color state ─────────────────

  async scanAll(warmupCandles) {
    const ranked = await this.getRankedSymbols();
    if (ranked.length === 0) return [];

    logger.info(`Scanning ${ranked.length} symbols...`);

    const results   = [];
    const batchSize = 10;

    for (let i = 0; i < ranked.length; i += batchSize) {
      const batch        = ranked.slice(i, i + batchSize);
      const batchResults = await Promise.all(
        batch.map(t => this.scanSymbol(t.symbol, warmupCandles))
      );
      results.push(...batchResults.filter(Boolean));

      if (i + batchSize < ranked.length) {
        await new Promise(r => setTimeout(r, 300));
      }
    }

    logger.info(`Scan complete: ${results.length} symbols with data`);
    return results;
  }

  // ── Detect NEW signals by comparing color to PREVIOUS scan ────────────
  //
  // FIX 2: Previously relied on strategy._calcSignal() which only fired on
  // color transitions within a single 270-candle batch (almost never the
  // last candle). Now we track color per symbol across scans and emit a
  // signal whenever the color changes between two consecutive scans.

  getNewSignals(scanResults) {
    const newSignals = [];

    for (const r of scanResults) {
      if (!r.color) continue; // not enough candles yet

      const prevColor = this.lastColors.get(r.symbol);

      if (prevColor === undefined) {
        // First time we see this symbol: store baseline, no signal
        this.lastColors.set(r.symbol, r.color);
        continue;
      }

      // Emit signal only on actual color transitions between scans
      if (r.color === 'bull' && prevColor !== 'bull') {
        newSignals.push({ ...r, signal: 'BUY' });
        logger.info(`Signal BUY  ${r.symbol} (${prevColor} → bull) slope=${r.slope}`);
      } else if (r.color === 'bear' && prevColor !== 'bear') {
        newSignals.push({ ...r, signal: 'SELL' });
        logger.info(`Signal SELL ${r.symbol} (${prevColor} → bear) slope=${r.slope}`);
      }

      // Always update known color
      this.lastColors.set(r.symbol, r.color);
    }

    return newSignals;
  }

  // ── Market overview stats ──────────────────────────────────────────────

  getMarketOverview(scanResults) {
    const bull  = scanResults.filter(r => r.color === 'bull').length;
    const bear  = scanResults.filter(r => r.color === 'bear').length;
    const chop  = scanResults.filter(r => r.color === 'chop').length;
    const buys  = scanResults.filter(r => r.signal === 'BUY').length;
    const sells = scanResults.filter(r => r.signal === 'SELL').length;
    const total = scanResults.length;

    return { bull, bear, chop, buys, sells, total };
  }
}

module.exports = MultiScanner;
