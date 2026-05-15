/**
 * Position Manager
 * Tracks open positions across all symbols, enforces max concurrent trades
 */

const logger = require('./logger');

class PositionManager {
  constructor(config) {
    this.maxPositions  = config.maxPositions  || 5;    // max concurrent open trades
    this.positions     = new Map();                    // symbol → position object
  }

  get openCount()  { return this.positions.size; }
  get hasCapacity(){ return this.positions.size < this.maxPositions; }
  get all()        { return Array.from(this.positions.values()); }

  has(symbol)  { return this.positions.has(symbol); }
  get(symbol)  { return this.positions.get(symbol); }

  open(symbol, { side, entry, qty, tp, sl }) {
    this.positions.set(symbol, { symbol, side, entry, qty, tp, sl, openedAt: Date.now() });
    logger.info(`Position opened: ${symbol} ${side} entry=${entry} qty=${qty}`);
  }

  close(symbol) {
    const pos = this.positions.get(symbol);
    this.positions.delete(symbol);
    logger.info(`Position closed: ${symbol}`);
    return pos;
  }

  summary() {
    if (this.positions.size === 0) return 'No open positions';
    return Array.from(this.positions.values())
      .map(p => `${p.symbol} ${p.side}@${p.entry}`)
      .join(' | ');
  }
}

module.exports = PositionManager;
