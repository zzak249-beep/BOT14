/**
 * Risk Management
 * Calculates position size, TP/SL based on account balance and config
 */

class RiskManager {
  constructor(config) {
    this.leverage  = config.leverage  || 5;
    this.riskPct   = config.riskPct   || 1;    // % of balance risked per trade
    this.tpPct     = config.tpPct     || 2;    // % TP from entry
    this.slPct     = config.slPct     || 1;    // % SL from entry
    this.minQty    = config.minQty    || 0.001; // minimum order size
    this.qtyStep   = config.qtyStep   || 0.001; // BingX qty precision step
  }

  /**
   * Calculate position size in base asset
   * @param {number} balance     USDT available
   * @param {number} entryPrice  current price
   * @returns {number} quantity
   */
  calcQuantity(balance, entryPrice) {
    const riskUsdt    = balance * (this.riskPct / 100);
    const notional    = riskUsdt * this.leverage;
    const rawQty      = notional / entryPrice;

    // Round down to qtyStep
    const qty = Math.floor(rawQty / this.qtyStep) * this.qtyStep;
    return Math.max(qty, this.minQty);
  }

  /**
   * Calculate TP and SL prices
   */
  calcTPSL(entryPrice, side) {
    const tpMult = this.tpPct / 100;
    const slMult = this.slPct / 100;

    if (side === 'LONG') {
      return {
        tp: parseFloat((entryPrice * (1 + tpMult)).toFixed(4)),
        sl: parseFloat((entryPrice * (1 - slMult)).toFixed(4))
      };
    } else {
      return {
        tp: parseFloat((entryPrice * (1 - tpMult)).toFixed(4)),
        sl: parseFloat((entryPrice * (1 + slMult)).toFixed(4))
      };
    }
  }

  /** R:R ratio */
  get rrRatio() {
    return this.tpPct / this.slPct;
  }
}

module.exports = RiskManager;
