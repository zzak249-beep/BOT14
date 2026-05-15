/**
 * Telegram Bot Notifications — Multi-Symbol Edition
 */

const axios = require('axios');

class TelegramNotifier {
  constructor(token, chatId) {
    this.token  = token;
    this.chatId = chatId;
    this.base   = `https://api.telegram.org/bot${token}`;
  }

  async send(text, parseMode = 'HTML') {
    try {
      await axios.post(`${this.base}/sendMessage`, {
        chat_id: this.chatId,
        text,
        parse_mode: parseMode,
        disable_web_page_preview: true
      });
    } catch (err) {
      console.error('[Telegram] Error:', err.message);
    }
  }

  async sendBuy({ symbol, price, quantity, leverage, tp, sl, sama, slope }) {
    await this.send(
`🟢 <b>LONG — ${symbol}</b>
📍 Entry: <code>${price}</code>  📦 Qty: <code>${quantity}</code>  ⚡ <code>${leverage}x</code>
🎯 TP: <code>${tp}</code>  🛑 SL: <code>${sl}</code>
📐 Slope: <code>${slope}°</code>  SAMA: <code>${typeof sama === 'number' ? sama.toFixed(6) : sama}</code>
⏰ ${new Date().toUTCString()}`);
  }

  async sendSell({ symbol, price, quantity, leverage, tp, sl, sama, slope }) {
    await this.send(
`🔴 <b>SHORT — ${symbol}</b>
📍 Entry: <code>${price}</code>  📦 Qty: <code>${quantity}</code>  ⚡ <code>${leverage}x</code>
🎯 TP: <code>${tp}</code>  🛑 SL: <code>${sl}</code>
📐 Slope: <code>${slope}°</code>  SAMA: <code>${typeof sama === 'number' ? sama.toFixed(6) : sama}</code>
⏰ ${new Date().toUTCString()}`);
  }

  async sendClose({ symbol, side, pnl, price }) {
    const e = pnl >= 0 ? '✅' : '❌';
    await this.send(
`${e} <b>CLOSED — ${symbol}</b>
📌 ${side}  💰 PnL: <code>${pnl >= 0 ? '+' : ''}${pnl.toFixed(2)} USDT</code>
📍 Exit: <code>${price}</code>  ⏰ ${new Date().toUTCString()}`);
  }

  async sendError(context, error) {
    await this.send(
`🚨 <b>ERROR — ${context}</b>
<code>${String(error).slice(0, 300)}</code>`);
  }

  async sendScanSummary({ total, bull, bear, chop, buys, sells, interval, topBull, topBear }) {
    const bullPct = total ? Math.round(bull / total * 100) : 0;
    const bearPct = total ? Math.round(bear / total * 100) : 0;
    const bullBar = '🟢'.repeat(Math.round(bullPct/10)) + '⬜'.repeat(10 - Math.round(bullPct/10));
    const bearBar = '🔴'.repeat(Math.round(bearPct/10)) + '⬜'.repeat(10 - Math.round(bearPct/10));

    const fmtList = list => list.slice(0,5)
      .map(r => `  • <code>${r.symbol.padEnd(14)}</code> slope <code>${r.slope}°</code>`)
      .join('\n') || '  —';

    await this.send(
`📊 <b>MARKET SCAN — ${interval}</b>
━━━━━━━━━━━━━━━━━━━━
🔍 Pairs: <b>${total}</b>
🟢 Bull ${bull} (${bullPct}%) ${bullBar}
🔴 Bear ${bear} (${bearPct}%) ${bearBar}
🟡 Chop: <b>${chop}</b>
🆕 Signals → BUY: <b>${buys}</b>  SELL: <b>${sells}</b>
━━━━━━━━━━━━━━━━━━━━
<b>Top Bulls:</b>
${fmtList(topBull)}
<b>Top Bears:</b>
${fmtList(topBear)}
⏰ ${new Date().toUTCString()}`);
  }

  async sendMultiSignal(signals) {
    if (!signals.length) return;
    const buys  = signals.filter(s => s.signal === 'BUY');
    const sells = signals.filter(s => s.signal === 'SELL');
    const fmt   = (list, e) => list
      .map(s => `${e} <code>${s.symbol.padEnd(14)}</code> @ <code>${s.price}</code>  slope <code>${s.slope}°</code>`)
      .join('\n');

    let msg = `🚨 <b>NEW SIGNALS (${signals.length})</b>\n━━━━━━━━━━━━━━━━━━━━\n`;
    if (buys.length)  msg += `<b>LONGS:</b>\n${fmt(buys,  '🟢')}\n`;
    if (sells.length) msg += `<b>SHORTS:</b>\n${fmt(sells, '🔴')}\n`;
    msg += `⏰ ${new Date().toUTCString()}`;
    await this.send(msg);
  }

  async sendPositionsSummary(positions, balance) {
    const list = positions.length
      ? positions.map(p => `  ${p.side==='LONG'?'🟢':'🔴'} <code>${p.symbol}</code> @ <code>${p.entry}</code>`).join('\n')
      : '  —';
    await this.send(
`📋 <b>POSITIONS (${positions.length})</b>
${list}
💼 Balance: <code>${balance} USDT</code>
⏰ ${new Date().toUTCString()}`);
  }

  async sendStart(config) {
    await this.send(
`🚀 <b>MZ SAMA MULTI-SCANNER</b>
━━━━━━━━━━━━━━━━━━━━
⏱ Interval: <code>${config.interval}</code>
🔍 Max pairs: <code>${config.maxSymbols}</code>
💹 Min vol 24h: <code>$${Number(config.minVolume24h).toLocaleString()}</code>
📂 Max positions: <code>${config.maxPositions}</code>
⚡ Leverage: <code>${config.leverage}x</code>
💰 Risk/trade: <code>${config.riskPct}%</code>
🎯 TP: <code>${config.tpPct}%</code>  🛑 SL: <code>${config.slPct}%</code>
🧪 DryRun: <code>${config.dryRun}</code>
⏰ ${new Date().toUTCString()}`);
  }
}

module.exports = TelegramNotifier;
