/**
 * MZ SAMA Strategy Implementation
 * Slope Adaptive Moving Average
 *
 * FIX APPLIED:
 *  - Removed _calcSignal() and _lastSignal — signal detection is now handled
 *    by MultiScanner.getNewSignals() which compares color across consecutive scans.
 *    The old approach only fired on color transitions within a single candle batch,
 *    which almost never coincided with the very last candle.
 */

class SAMAStrategy {
  constructor(config = {}) {
    this.length      = config.length      || 200;
    this.majLength   = config.majLength   || 14;
    this.minLength   = config.minLength   || 6;
    this.slopePeriod = config.slopePeriod || 34;
    this.slopeInRange= config.slopeInRange|| 25;
    this.flat        = config.flat        || 17;

    // Internal state
    this._ama    = null;
    this._prices = [];
    this._amas   = [];
  }

  /**
   * Feed one candle and get the current SAMA state.
   * Returns { sama, slope, color, signal } — signal is always null here;
   * actual BUY/SELL signals are emitted by MultiScanner.getNewSignals().
   */
  update(candle) {
    const src = candle.close;
    this._prices.push(candle);

    // Need at minimum length+1 candles to compute AMA
    if (this._prices.length < this.length + 1) {
      this._amas.push(null);
      return { sama: null, slope: null, color: null, signal: null };
    }

    const sama  = this._calcAMA(src);
    this._amas.push(sama);
    const slope = this._calcSlope(sama, src);
    const color = this._dynColor(slope);

    return { sama, slope, color, signal: null };
  }

  _calcAMA(src) {
    const prices = this._prices;
    const n      = prices.length;
    const window = prices.slice(n - (this.length + 1));

    const highs = window.map(c => c.high);
    const lows  = window.map(c => c.low);
    const hh    = Math.max(...highs);
    const ll    = Math.min(...lows);

    const minAlpha = 2 / (this.minLength  + 1);
    const majAlpha = 2 / (this.majLength  + 1);

    let mult = 0;
    if (hh - ll !== 0) {
      mult = Math.abs(2 * src - ll - hh) / (hh - ll);
    }

    const final      = mult * (minAlpha - majAlpha) + majAlpha;
    const finalAlpha = Math.pow(final, 2);
    const prevAma    = this._ama !== null ? this._ama : src;

    this._ama = (src - prevAma) * finalAlpha + prevAma;
    return this._ama;
  }

  _calcSlope(ma, src) {
    if (this._amas.length < this.slopePeriod) return 0;

    const pi           = Math.PI;
    const recentPrices = this._prices.slice(-this.slopePeriod);
    const highs        = recentPrices.map(c => c.high);
    const lows         = recentPrices.map(c => c.low);
    const highestHigh  = Math.max(...highs);
    const lowestLow    = Math.min(...lows);

    if (highestHigh === lowestLow) return 0;

    const slopeRange = this.slopeInRange / (highestHigh - lowestLow) * lowestLow;
    const amas       = this._amas.filter(v => v !== null);
    const ma2        = amas.length >= 3 ? amas[amas.length - 3] : ma;

    const dt     = (ma2 - ma) / src * slopeRange;
    const c      = Math.sqrt(1 + dt * dt);
    const xAngle = Math.round(180 * Math.acos(1 / c) / pi);

    return dt > 0 ? -xAngle : xAngle;
  }

  _dynColor(slope) {
    if (slope > this.flat)   return 'bull';
    if (slope <= -this.flat) return 'bear';
    return 'chop';
  }

  reset() {
    this._ama    = null;
    this._prices = [];
    this._amas   = [];
  }
}

module.exports = SAMAStrategy;
