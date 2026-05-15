/**
 * Multi-Symbol Scanner
 * Fetches all USDT perpetual futures from BingX and filters by volume/signal
 */

const BingXClient = require('./bingx');
const SAMAStrategy = require('./strategy');
const logger = require('./logger');

class MultiScanner {
  constructor(config) {
    this.config   = config;
    this.bingx    = new BingXClient(config.bingxApiKey, config.bingxSecretKey);
    this.strategies = new Map(); // symbol → SAMAStrategy instance
    this.lastSignals = new Map(); // symbol → last signal state

    // Filter thresholds
    this.minVolume24h   = config.minVolume24h   || 500_000;   // min USDT 24h volume
    this.maxSymbols     = config.maxSymbols     || 50;        // cap concurrent symbols
    this.minPrice       = config.minPrice       || 0.000001;  // filter dust
  }

  // ── Fetch all tradeable USDT-perpetual symbols ──────────────────────────

  async getAllSymbols() {
    try {
      const res = await this.bingx._get('/openApi/swap/v2/quote/contracts');
      const contracts = res.data || [];

      return contracts
        .filter(c =>
          c.symbol.endsWith('-USDT') &&
          c.status === 1 &&                     // active
          parseFloat(c.lastPrice || 0) > this.minPrice
        )
        .map(c => c.symbol);
    } catch (err) {
      logger.error(`getAllSymbols error: ${err.message}`);
      return [];
    }
  }

  // ── Fetch 24h tickers for all symbols and rank by volume ───────────────

  async getRankedSymbols() {
    try {
      const res = await this.bingx._get('/openApi/swap/v2/quote/ticker');
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

      const candles = res.data.map(k => ({
        time:  k[0],
        open:  parseFloat(k[1]),
        high:  parseFloat(k[2]),
        low:   parseFloat(k[3]),
        close: parseFloat(k[4]),
        vol:   parseFloat(k[5])
      }));

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

      const lastCandle = candles[candles.length - 2]; // last closed candle
      return {
        symbol,
        price:  lastCandle.close,
        vol24h: candles.slice(-24).reduce((s, c) => s + c.vol, 0),
        ...result
      };
    } catch (err) {
      // Silently skip symbols with errors (delisted, no data, etc.)
      logger.warn(`scanSymbol(${symbol}): ${err.message}`);
      return null;
    }
  }

  // ── Scan all symbols, return results with signals ─────────────────────

  async scanAll(warmupCandles) {
    const ranked = await this.getRankedSymbols();
    if (ranked.length === 0) return [];

    logger.info(`Scanning ${ranked.length} symbols...`);

    // Batch requests to avoid rate limiting (10 at a time)
    const results = [];
    const batchSize = 10;
    for (let i = 0; i < ranked.length; i += batchSize) {
      const batch = ranked.slice(i, i + batchSize);
      const batchResults = await Promise.all(
        batch.map(t => this.scanSymbol(t.symbol, warmupCandles))
      );
      results.push(...batchResults.filter(Boolean));

      // Small delay between batches to respect rate limits
      if (i + batchSize < ranked.length) {
        await new Promise(r => setTimeout(r, 300));
      }
    }

    logger.info(`Scan complete: ${results.length} symbols with data`);
    return results;
  }

  // ── Detect NEW signals (color change) compared to previous scan ────────

  getNewSignals(scanResults) {
    const newSignals = [];

    for (const r of scanResults) {
      const prev = this.lastSignals.get(r.symbol);
      const isNew =
        (r.signal === 'BUY'  && prev !== 'BUY')  ||
        (r.signal === 'SELL' && prev !== 'SELL');

      if (isNew) {
        newSignals.push(r);
        this.lastSignals.set(r.symbol, r.signal);
      } else if (!r.signal) {
        // Update chop state
        this.lastSignals.set(r.symbol, r.color);
      }
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
