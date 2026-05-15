const fs   = require('fs');
const path = require('path');

const LOG_DIR  = path.join(__dirname, '..', 'logs');
const LOG_FILE = path.join(LOG_DIR, 'bot.log');

if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });

function ts() { return new Date().toISOString(); }

function write(level, msg) {
  const line = `[${ts()}] [${level}] ${msg}`;
  console.log(line);
  fs.appendFileSync(LOG_FILE, line + '\n');
}

module.exports = {
  info:  msg => write('INFO',  msg),
  warn:  msg => write('WARN',  msg),
  error: msg => write('ERROR', msg),
};
