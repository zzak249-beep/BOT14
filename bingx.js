/**
 * BingX Futures API Client
 * Docs: https://bingx-api.github.io/docs/
 */

const crypto = require('crypto');
const axios  = require('axios');

const BASE_URL = 'https://open-api.bingx.com';

class BingXClient {
  constructor(apiKey, secretKey) {
    this.apiKey    = apiKey;
    this.secretKey = secretKey;
  }

  _sign(params) {
    const qs = Object.keys(params).sort().map(k => `${k}=${params[k]}`).join('&');
    return crypto.createHmac('sha256', this.secretKey).update(qs).digest('hex');
  }

  _buildParams(params = {}) {
    const ts = Date.now();
    const p  = { ...params, timestamp: ts };
    p.signature = this._sign(p);
    return p;
  }

  async _get(path, params = {}) {
    const p = this._buildParams(params);
    const res = await axios.get(`${BASE_URL}${path}`, {
      params: p,
      headers: { 'X-BX-APIKEY': this.apiKey }
    });
    return res.data;
  }

  async _post(path, params = {}) {
    const p = this._buildParams(params);
    const qs = new URLSearchParams(p).toString();
    const res = await axios.post(`${BASE_URL}${path}?${qs}`, null, {
      headers: { 'X-BX-APIKEY': this.apiKey, 'Content-Type': 'application/json' }
    });
    return res.data;
  }

  // ── Market data ──────────────────────────────────────────────────────────

  /** Get recent K-lines (OHLCV) */
  async getKlines(symbol, interval, limit = 300) {
    // interval examples: '1m','5m','15m','1h','4h','1d'
    return this._get('/openApi/swap/v2/quote/klines', { symbol, interval, limit });
  }

  /** Get ticker / latest price */
  async getTicker(symbol) {
    return this._get('/openApi/swap/v2/quote/ticker', { symbol });
  }

  // ── Account ──────────────────────────────────────────────────────────────

  async getBalance() {
    return this._get('/openApi/swap/v2/user/balance');
  }

  async getPositions(symbol) {
    return this._get('/openApi/swap/v2/user/positions', { symbol });
  }

  // ── Leverage & margin ────────────────────────────────────────────────────

  async setLeverage(symbol, leverage, side = 'LONG') {
    return this._post('/openApi/swap/v2/trade/leverage', { symbol, leverage, side });
  }

  async setMarginType(symbol, marginType = 'ISOLATED') {
    // marginType: 'ISOLATED' | 'CROSSED'
    return this._post('/openApi/swap/v2/trade/marginType', { symbol, marginType });
  }

  // ── Orders ───────────────────────────────────────────────────────────────

  /**
   * Place a market order
   * @param {string} symbol  e.g. 'BTC-USDT'
   * @param {string} side    'BUY' | 'SELL'
   * @param {string} positionSide 'LONG' | 'SHORT'
   * @param {number} quantity  in contracts / base asset
   */
  async marketOrder(symbol, side, positionSide, quantity) {
    return this._post('/openApi/swap/v2/trade/order', {
      symbol,
      side,
      positionSide,
      type: 'MARKET',
      quantity: quantity.toString()
    });
  }

  /**
   * Close a position fully via market order
   */
  async closePosition(symbol, positionSide) {
    const side = positionSide === 'LONG' ? 'SELL' : 'BUY';
    const positions = await this.getPositions(symbol);
    const pos = (positions.data || []).find(p => p.positionSide === positionSide);
    if (!pos || parseFloat(pos.positionAmt) === 0) return null;
    const qty = Math.abs(parseFloat(pos.positionAmt));
    return this.marketOrder(symbol, side, positionSide, qty);
  }

  /**
   * Set TP / SL on open position
   */
  async setTPSL(symbol, positionSide, takeProfitPrice, stopLossPrice) {
    const side = positionSide === 'LONG' ? 'SELL' : 'BUY';
    const orders = [];

    if (takeProfitPrice) {
      orders.push(this._post('/openApi/swap/v2/trade/order', {
        symbol, side, positionSide,
        type: 'TAKE_PROFIT_MARKET',
        stopPrice: takeProfitPrice.toString(),
        closePosition: 'true',
        workingType: 'MARK_PRICE'
      }));
    }
    if (stopLossPrice) {
      orders.push(this._post('/openApi/swap/v2/trade/order', {
        symbol, side, positionSide,
        type: 'STOP_MARKET',
        stopPrice: stopLossPrice.toString(),
        closePosition: 'true',
        workingType: 'MARK_PRICE'
      }));
    }
    return Promise.all(orders);
  }

  async cancelAllOrders(symbol) {
    return this._post('/openApi/swap/v2/trade/allOpenOrders', { symbol });
  }

  async getOpenOrders(symbol) {
    return this._get('/openApi/swap/v2/trade/openOrders', { symbol });
  }
}

module.exports = BingXClient;
