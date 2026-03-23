const fs = require('node:fs/promises');
const https = require('node:https');
const path = require('node:path');

require('dotenv').config();

const axios = require('axios');
const { Pool } = require('pg');
const TelegramBot = require('node-telegram-bot-api');

const TOKEN = process.env.TOKEN;
const DATABASE_URL = process.env.DATABASE_URL;
const DATABASE_SSL =
  process.env.DATABASE_SSL === 'true' || process.env.PGSSLMODE === 'require';

if (!TOKEN) {
  console.error('Missing required environment variable: TOKEN');
  process.exit(1);
}

const YAHOO_FINANCE_BASE_URL = 'https://query1.finance.yahoo.com/v8/finance/chart';
const LAKUEMAS_PRICES_URLS = ['https://www.lakuemas.com/', 'https://lakuemas.com/'];
const USERS_FILE = path.join(__dirname, 'users.json');
const POLL_INTERVAL_MS = 30_000;
const RETAIL_CACHE_MS = 30_000;
const TROY_OUNCE_TO_GRAMS = 31.1034768;
const FALLBACK_USD_TO_IDR_RATE = 15_500;
const FX_CACHE_MS = 30_000;
const DISPLAY_TIME_ZONE = process.env.DISPLAY_TIME_ZONE || 'Asia/Makassar';
const DISPLAY_TIME_LABEL = process.env.DISPLAY_TIME_LABEL || 'WITA';
const DEFAULT_RETAIL_SPREAD_RATE = 0.0341;
const DEFAULT_RETAIL_MID_MARKUP = 1.1058;
const DEFAULT_RETAIL_ROUND_TO = 1_000;
const RETAIL_MID_MARKUP = DEFAULT_RETAIL_MID_MARKUP;
const RETAIL_SPREAD_RATE = DEFAULT_RETAIL_SPREAD_RATE;
const RETAIL_ROUND_TO = DEFAULT_RETAIL_ROUND_TO;
const BUTTONS = {
  price: '💰 Cek Harga',
  buyAbove: '🔔 Beli Naik',
  buyBelow: '🔔 Beli Turun',
  sellAbove: '🔔 Jual Naik',
  sellBelow: '🔔 Jual Turun',
  alerts: '📌 Alert Saya',
  clear: '🧹 Hapus Semua',
  cancel: '⬅️ Batal',
};
const ALERT_DEFINITIONS = {
  buyAbove: {
    label: 'harga beli',
    shortLabel: 'harga beli',
    conditionText: 'naik ke atau melewati',
    valueKey: 'buyIdrPerGram',
    compare: (current, target) => current >= target,
    icon: '🚀',
  },
  buyBelow: {
    label: 'harga beli',
    shortLabel: 'harga beli',
    conditionText: 'turun ke atau di bawah',
    valueKey: 'buyIdrPerGram',
    compare: (current, target) => current <= target,
    icon: '📉',
  },
  sellAbove: {
    label: 'harga jual',
    shortLabel: 'harga jual',
    conditionText: 'naik ke atau melewati',
    valueKey: 'sellIdrPerGram',
    compare: (current, target) => current >= target,
    icon: '🚀',
  },
  sellBelow: {
    label: 'harga jual',
    shortLabel: 'harga jual',
    conditionText: 'turun ke atau di bawah',
    valueKey: 'sellIdrPerGram',
    compare: (current, target) => current <= target,
    icon: '📉',
  },
};
const BUTTON_TO_ALERT_TYPE = {
  [BUTTONS.buyAbove]: 'buyAbove',
  [BUTTONS.buyBelow]: 'buyBelow',
  [BUTTONS.sellAbove]: 'sellAbove',
  [BUTTONS.sellBelow]: 'sellBelow',
};

const bot = new TelegramBot(TOKEN, { polling: true });

let users = {};
let lastPrice = null;
let isCheckingPrice = false;
let saveQueue = Promise.resolve();
let pollTimer = null;
let activeYahooSymbol = 'XAUUSD=X';
let lastUsdToIdrRate = FALLBACK_USD_TO_IDR_RATE;
let lastFxFetchAt = 0;
let lastRetailQuote = null;
let lastRetailFetchAt = 0;
let lastRetailSignature = null;
let lastMarketCheckAt = null;
let lastRetailChangedAt = null;
let dbPool = null;

const PRICE_SYMBOLS = ['XAUUSD=X', 'GC=F'];
const FX_SYMBOLS = ['USDIDR=X'];

const usdFormatter = new Intl.NumberFormat('en-US', {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

const idrFormatter = new Intl.NumberFormat('id-ID', {
  maximumFractionDigits: 0,
});
const dateTimeFormatter = new Intl.DateTimeFormat('id-ID', {
  day: '2-digit',
  month: 'short',
  year: 'numeric',
  hour: '2-digit',
  minute: '2-digit',
  hour12: false,
  timeZone: DISPLAY_TIME_ZONE,
});

function normalizeUser(user = {}) {
  return {
    alertAbove: Number.isFinite(user.alertAbove) ? user.alertAbove : null,
    alertBelow: Number.isFinite(user.alertBelow) ? user.alertBelow : null,
    buyAbove: Number.isFinite(user.buyAbove) ? user.buyAbove : null,
    buyBelow: Number.isFinite(user.buyBelow) ? user.buyBelow : null,
    sellAbove: Number.isFinite(user.sellAbove) ? user.sellAbove : null,
    sellBelow: Number.isFinite(user.sellBelow) ? user.sellBelow : null,
    pendingAction:
      typeof user.pendingAction === 'string' && ALERT_DEFINITIONS[user.pendingAction]
        ? user.pendingAction
        : null,
  };
}

function isDatabaseStorageEnabled() {
  return Boolean(DATABASE_URL);
}

function getDbPool() {
  if (!isDatabaseStorageEnabled()) {
    return null;
  }

  if (!dbPool) {
    dbPool = new Pool({
      connectionString: DATABASE_URL,
      ssl: DATABASE_SSL ? { rejectUnauthorized: false } : undefined,
    });
  }

  return dbPool;
}

async function ensureUsersFile() {
  try {
    await fs.access(USERS_FILE);
  } catch (error) {
    if (error.code !== 'ENOENT') {
      throw error;
    }

    await fs.writeFile(USERS_FILE, '{}\n', 'utf8');
  }
}

async function loadUsers() {
  if (isDatabaseStorageEnabled()) {
    try {
      const pool = getDbPool();
      await pool.query(`
        CREATE TABLE IF NOT EXISTS bot_users (
          chat_id TEXT PRIMARY KEY,
          data JSONB NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `);

      const result = await pool.query('SELECT chat_id, data FROM bot_users');
      users = Object.fromEntries(
        result.rows.map((row) => [row.chat_id, normalizeUser(row.data || {})])
      );
      return;
    } catch (error) {
      console.error('Failed to load users from database:', error.message);
      users = {};
      return;
    }
  }

  await ensureUsersFile();

  try {
    const raw = await fs.readFile(USERS_FILE, 'utf8');
    const parsed = raw.trim() ? JSON.parse(raw) : {};

    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error('users.json must contain an object');
    }

    users = Object.fromEntries(
      Object.entries(parsed).map(([chatId, user]) => [chatId, normalizeUser(user)])
    );
  } catch (error) {
    console.error('Failed to load users.json:', error.message);
    users = {};
  }
}

function saveUsers() {
  saveQueue = saveQueue
    .catch(() => undefined)
    .then(async () => {
      if (isDatabaseStorageEnabled()) {
        const pool = getDbPool();
        const entries = Object.entries(users).map(([chatId, user]) => [
          chatId,
          JSON.stringify(normalizeUser(user)),
        ]);

        const client = await pool.connect();

        try {
          await client.query('BEGIN');
          await client.query('DELETE FROM bot_users');

          for (const [chatId, payload] of entries) {
            await client.query(
              `
                INSERT INTO bot_users (chat_id, data, updated_at)
                VALUES ($1, $2::jsonb, NOW())
              `,
              [chatId, payload]
            );
          }

          await client.query('COMMIT');
        } catch (error) {
          await client.query('ROLLBACK');
          throw error;
        } finally {
          client.release();
        }

        return;
      }

      const payload = JSON.stringify(users, null, 2) + '\n';
      await fs.writeFile(USERS_FILE, payload, 'utf8');
    })
    .catch((error) => {
      console.error(
        `Failed to save user storage (${isDatabaseStorageEnabled() ? 'database' : 'file'}):`,
        error.message
      );
    });

  return saveQueue;
}

function ensureUser(chatId) {
  const key = String(chatId);

  if (!users[key]) {
    users[key] = normalizeUser();
    void saveUsers();
  }

  return users[key];
}

function getMainKeyboard() {
  return {
    keyboard: [
      [BUTTONS.price, BUTTONS.alerts],
      [BUTTONS.buyAbove, BUTTONS.buyBelow],
      [BUTTONS.sellAbove, BUTTONS.sellBelow],
      [BUTTONS.clear],
    ],
    resize_keyboard: true,
  };
}

function getPromptKeyboard() {
  return {
    keyboard: [[BUTTONS.cancel]],
    resize_keyboard: true,
    one_time_keyboard: true,
  };
}

function formatUsd(value) {
  return `$${usdFormatter.format(value)}`;
}

function formatIdr(value) {
  return `Rp${idrFormatter.format(value)}`;
}

function getRetailSourceLabel(source) {
  return source === 'lakuemas' ? 'Lakuemas resmi' : 'Cadangan estimasi';
}

function formatSignedIdr(value) {
  const sign = value > 0 ? '+' : value < 0 ? '-' : '';
  return `${sign}Rp${idrFormatter.format(Math.abs(value))}`;
}

function formatDisplayDateTime(value) {
  if (!value) {
    return null;
  }

  return `${dateTimeFormatter.format(value)} ${DISPLAY_TIME_LABEL}`;
}

function convertUsdOunceToIdrGram(usdPerOunce, usdToIdrRate = lastUsdToIdrRate) {
  return (usdPerOunce * usdToIdrRate) / TROY_OUNCE_TO_GRAMS;
}

function roundToNearest(value, increment) {
  return Math.round(value / increment) * increment;
}

function parseIdrNumber(rawValue) {
  const digitsOnly = String(rawValue || '').replace(/[^\d]/g, '');
  const value = Number.parseInt(digitsOnly, 10);
  return Number.isFinite(value) ? value : null;
}

function parseUsdTarget(rawValue) {
  const normalized = String(rawValue || '').replace(/[$,\s]/g, '');
  const value = Number.parseFloat(normalized);
  return Number.isFinite(value) && value > 0 ? value : null;
}

function parseIdrTarget(rawValue) {
  const value = parseIdrNumber(rawValue);
  return Number.isFinite(value) && value > 0 ? value : null;
}

function getRetailSignature(retailQuote) {
  return [
    retailQuote?.buyIdrPerGram ?? 'na',
    retailQuote?.sellIdrPerGram ?? 'na',
    retailQuote?.spreadIdr ?? 'na',
  ].join(':');
}

function calculateRetailSpreadIdr(spotIdrPerGram) {
  return roundToNearest(spotIdrPerGram * RETAIL_SPREAD_RATE, RETAIL_ROUND_TO);
}

function buildEstimatedRetailQuote(usdPerOunce, usdToIdrRate = lastUsdToIdrRate) {
  const spotIdrPerGram = convertUsdOunceToIdrGram(usdPerOunce, usdToIdrRate);
  const retailMid = spotIdrPerGram * RETAIL_MID_MARKUP;
  const halfSpread = calculateRetailSpreadIdr(spotIdrPerGram) / 2;
  const buyIdrPerGram = roundToNearest(retailMid + halfSpread, RETAIL_ROUND_TO);
  const sellIdrPerGram = roundToNearest(Math.max(retailMid - halfSpread, 0), RETAIL_ROUND_TO);
  const spreadIdr = Math.max(buyIdrPerGram - sellIdrPerGram, 0);
  const spreadPct = sellIdrPerGram > 0 ? (spreadIdr / sellIdrPerGram) * 100 : 0;

  return {
    spotIdrPerGram,
    buyIdrPerGram,
    sellIdrPerGram,
    spreadIdr,
    spreadPct,
    source: 'estimate',
  };
}

function parseLakuemasRetailQuote(html, fallbackSpotIdrPerGram) {
  const compactHtml = String(html || '').replace(/\s+/g, ' ');
  const buyMatch = compactHtml.match(/HARGA BELI EMAS HARI INI.*?IDR\s*([0-9.,]+)/i);
  const sellMatch = compactHtml.match(/HARGA JUAL EMAS HARI INI.*?IDR\s*([0-9.,]+)/i);
  const buyIdrPerGram = parseIdrNumber(buyMatch?.[1]);
  const sellIdrPerGram = parseIdrNumber(sellMatch?.[1]);

  if (!Number.isFinite(buyIdrPerGram) || !Number.isFinite(sellIdrPerGram)) {
    throw new Error('Unable to parse Lakuemas buy/sell prices');
  }

  const spreadIdr = Math.max(buyIdrPerGram - sellIdrPerGram, 0);
  const spreadPct = sellIdrPerGram > 0 ? (spreadIdr / sellIdrPerGram) * 100 : 0;

  return {
    spotIdrPerGram: fallbackSpotIdrPerGram,
    buyIdrPerGram,
    sellIdrPerGram,
    spreadIdr,
    spreadPct,
    source: 'lakuemas',
  };
}

async function fetchRetailQuote(usdPerOunce, usdToIdrRate, { forceRefresh = false } = {}) {
  const now = Date.now();
  const fallbackQuote = buildEstimatedRetailQuote(usdPerOunce, usdToIdrRate);
  const cachedRetailQuote = lastRetailQuote
    ? {
        ...lastRetailQuote,
        spotIdrPerGram: fallbackQuote.spotIdrPerGram,
      }
    : null;

  if (!forceRefresh && cachedRetailQuote && now - lastRetailFetchAt < RETAIL_CACHE_MS) {
    return cachedRetailQuote;
  }

  let lastError = null;

  for (const url of LAKUEMAS_PRICES_URLS) {
    try {
      const response = await axios.get(url, {
        timeout: 15_000,
        httpsAgent: new https.Agent({ rejectUnauthorized: false }),
        headers: {
          'User-Agent': 'bot-gold/1.0',
        },
      });

      const retailQuote = parseLakuemasRetailQuote(response.data, fallbackQuote.spotIdrPerGram);
      lastRetailQuote = retailQuote;
      lastRetailFetchAt = now;
      return retailQuote;
    } catch (error) {
      lastError = error;
    }
  }

  if (cachedRetailQuote) {
    console.error(
      `Lakuemas retail quote fetch failed, using cached official quote: ${
        lastError?.message || 'Unknown error'
      }`
    );
    return cachedRetailQuote;
  }

  console.error(
    `Lakuemas retail quote fetch failed, using estimate fallback: ${
      lastError?.message || 'Unknown error'
    }`
  );
  return fallbackQuote;
}

function formatIdrPerGramFromUsdOunce(usdPerOunce, usdToIdrRate = lastUsdToIdrRate) {
  return `Rp${idrFormatter.format(convertUsdOunceToIdrGram(usdPerOunce, usdToIdrRate))}`;
}

function formatPriceMessage(
  price,
  usdToIdrRate = lastUsdToIdrRate,
  retailQuote,
  { checkedAt = lastMarketCheckAt, changedAt = lastRetailChangedAt } = {}
) {
  const quote = retailQuote || buildEstimatedRetailQuote(price, usdToIdrRate);
  const sourceLabel = getRetailSourceLabel(quote.source);
  const checkedAtLabel = formatDisplayDateTime(checkedAt);
  const changedAtLabel = formatDisplayDateTime(changedAt);
  const footerLines = [
    checkedAtLabel ? `Terakhir dicek: ${checkedAtLabel}` : null,
    changedAtLabel
      ? `Terakhir berubah: ${changedAtLabel}`
      : 'Perubahan harga belum terdeteksi sejak bot aktif.',
  ].filter(Boolean);

  if (quote.source === 'lakuemas') {
    return [
      '🪙 Harga Emas Hari Ini',
      '',
      `Harga beli: ${formatIdr(quote.buyIdrPerGram)}/gr`,
      `Harga jual: ${formatIdr(quote.sellIdrPerGram)}/gr`,
      `Selisih beli-jual: ${formatIdr(quote.spreadIdr)} (${quote.spreadPct.toFixed(2)}%)`,
      '',
      'Info pasar global:',
      `Harga emas dunia: ${formatUsd(price)}/oz`,
      `Kurs USD/IDR live: ${formatIdr(usdToIdrRate)}/USD`,
      `Spot global setara: ${formatIdrPerGramFromUsdOunce(price, usdToIdrRate)}/gr`,
      '',
      `Sumber harga: ${sourceLabel}`,
      'Harga beli dan jual diambil langsung dari Lakuemas.',
      '',
      ...footerLines,
    ].join('\n');
  }

  return [
    '🪙 Harga Emas Hari Ini',
    '',
    `Harga beli: ${formatIdr(quote.buyIdrPerGram)}/gr`,
    `Harga jual: ${formatIdr(quote.sellIdrPerGram)}/gr`,
    `Selisih beli-jual: ${formatIdr(quote.spreadIdr)} (${quote.spreadPct.toFixed(2)}%)`,
    '',
    'Info tambahan:',
    `Patokan emas dunia: ${formatUsd(price)}/oz`,
    `Perkiraan harga dasar: ${formatIdrPerGramFromUsdOunce(price, usdToIdrRate)}/gr`,
    `Kurs acuan: ${formatIdr(usdToIdrRate)}/USD`,
    `Sumber harga: ${sourceLabel}`,
    '',
    ...footerLines,
  ].join('\n');
}

function extractLatestClose(closes = []) {
  for (let index = closes.length - 1; index >= 0; index -= 1) {
    const price = closes[index];

    if (Number.isFinite(price)) {
      return price;
    }
  }

  return null;
}

async function fetchGoldPrice() {
  let lastError = null;

  for (const symbol of PRICE_SYMBOLS) {
    try {
      const response = await axios.get(`${YAHOO_FINANCE_BASE_URL}/${encodeURIComponent(symbol)}`, {
        timeout: 10_000,
        headers: {
          'User-Agent': 'bot-gold/1.0',
        },
        params: {
          interval: '1m',
          range: '1d',
        },
      });

      const result = response.data?.chart?.result?.[0];
      const regularMarketPrice = result?.meta?.regularMarketPrice;
      const latestClose = extractLatestClose(result?.indicators?.quote?.[0]?.close);
      const price = Number.isFinite(regularMarketPrice) ? regularMarketPrice : latestClose;

      if (!Number.isFinite(price)) {
        throw new Error(`Unable to read price for ${symbol}`);
      }

      if (activeYahooSymbol !== symbol) {
        if (symbol === 'GC=F') {
          console.warn('Using Yahoo fallback symbol GC=F because XAUUSD=X is unavailable.');
        } else {
          console.info('Yahoo primary symbol XAUUSD=X is available again.');
        }

        activeYahooSymbol = symbol;
      }

      return price;
    } catch (error) {
      lastError = error;
    }
  }

  throw new Error(
    `Unable to fetch gold price from Yahoo Finance. Last error: ${lastError?.message || 'Unknown error'}`
  );
}

async function fetchUsdToIdrRate({ forceRefresh = false } = {}) {
  const now = Date.now();

  if (!forceRefresh && lastFxFetchAt > 0 && now - lastFxFetchAt < FX_CACHE_MS) {
    return lastUsdToIdrRate;
  }

  let lastError = null;

  for (const symbol of FX_SYMBOLS) {
    try {
      const response = await axios.get(`${YAHOO_FINANCE_BASE_URL}/${encodeURIComponent(symbol)}`, {
        timeout: 10_000,
        headers: {
          'User-Agent': 'bot-gold/1.0',
        },
        params: {
          interval: '1m',
          range: '1d',
        },
      });

      const result = response.data?.chart?.result?.[0];
      const regularMarketPrice = result?.meta?.regularMarketPrice;
      const latestClose = extractLatestClose(result?.indicators?.quote?.[0]?.close);
      const rate = Number.isFinite(regularMarketPrice) ? regularMarketPrice : latestClose;

      if (!Number.isFinite(rate)) {
        throw new Error(`Unable to read FX rate for ${symbol}`);
      }

      lastUsdToIdrRate = rate;
      lastFxFetchAt = now;
      return lastUsdToIdrRate;
    } catch (error) {
      lastError = error;
    }
  }

  console.error(
    `USD/IDR fetch failed, using fallback rate Rp${idrFormatter.format(lastUsdToIdrRate)}: ${
      lastError?.message || 'Unknown error'
    }`
  );

  return lastUsdToIdrRate || FALLBACK_USD_TO_IDR_RATE;
}

async function fetchMarketSnapshot({ forceFxRefresh = false } = {}) {
  const [goldPrice, usdToIdrRate] = await Promise.all([
    fetchGoldPrice(),
    fetchUsdToIdrRate({ forceRefresh: forceFxRefresh }),
  ]);
  const retailQuote = await fetchRetailQuote(goldPrice, usdToIdrRate, {
    forceRefresh: forceFxRefresh,
  });

  return {
    checkedAt: Date.now(),
    goldPrice,
    retailQuote,
    usdToIdrRate,
  };
}

async function sendMessage(chatId, text, options = {}) {
  try {
    await bot.sendMessage(chatId, text, options);
    return true;
  } catch (error) {
    const description =
      error.response?.body?.description || error.message || 'Unknown Telegram error';
    console.error(`Failed to send Telegram message to ${chatId}:`, description);
    return false;
  }
}

function formatActiveAlerts(user) {
  const lines = [];

  if (user.buyAbove !== null) {
    lines.push(`Harga beli naik ke ${formatIdr(user.buyAbove)}/gr`);
  }

  if (user.buyBelow !== null) {
    lines.push(`Harga beli turun ke ${formatIdr(user.buyBelow)}/gr`);
  }

  if (user.sellAbove !== null) {
    lines.push(`Harga jual naik ke ${formatIdr(user.sellAbove)}/gr`);
  }

  if (user.sellBelow !== null) {
    lines.push(`Harga jual turun ke ${formatIdr(user.sellBelow)}/gr`);
  }

  if (user.alertAbove !== null) {
    lines.push(`Harga emas dunia naik ke ${formatUsd(user.alertAbove)}/oz`);
  }

  if (user.alertBelow !== null) {
    lines.push(`Harga emas dunia turun ke ${formatUsd(user.alertBelow)}/oz`);
  }

  return lines.length > 0 ? lines.map((line) => `- ${line}`).join('\n') : 'Belum ada alert aktif.';
}

function clearAllAlerts(user) {
  user.alertAbove = null;
  user.alertBelow = null;
  user.buyAbove = null;
  user.buyBelow = null;
  user.sellAbove = null;
  user.sellBelow = null;
  user.pendingAction = null;
}

function getAlertPromptText(alertType) {
  const config = ALERT_DEFINITIONS[alertType];
  return [
    `Saya akan pantau ${config.label} Lakuemas.`,
    `Kirim target saat ${config.label} ${config.conditionText}.`,
    'Contoh: 2388000',
    'Bisa juga: Rp2.388.000',
  ].join('\n');
}

async function promptRetailAlertInput(chatId, user, alertType) {
  user.pendingAction = alertType;
  await saveUsers();

  await sendMessage(chatId, getAlertPromptText(alertType), {
    reply_markup: getPromptKeyboard(),
  });
}

async function setRetailAlert(chatId, user, alertType, rawValue) {
  const target = parseIdrTarget(rawValue);

  if (target === null) {
    await sendMessage(chatId, 'Format harga belum benar.\nContoh: 2388000 atau Rp2.388.000', {
      reply_markup: getPromptKeyboard(),
    });
    return false;
  }

  user[alertType] = target;
  user.pendingAction = null;
  await saveUsers();

  await sendMessage(
    chatId,
    `✅ Siap. Saya akan beri tahu saat ${ALERT_DEFINITIONS[alertType].label} ${ALERT_DEFINITIONS[alertType].conditionText} ${formatIdr(target)}/gr.`,
    { reply_markup: getMainKeyboard() }
  );
  await maybeTriggerImmediateAlert(chatId);
  return true;
}

async function sendCurrentPrice(chatId) {
  ensureUser(chatId);

  try {
    const { checkedAt, goldPrice, retailQuote, usdToIdrRate } = await fetchMarketSnapshot({
      forceFxRefresh: true,
    });
    await sendMessage(
      chatId,
      formatPriceMessage(goldPrice, usdToIdrRate, retailQuote, {
        checkedAt,
        changedAt: lastRetailChangedAt,
      }),
      {
      reply_markup: getMainKeyboard(),
      }
    );
  } catch (error) {
    console.error(`/price failed for ${chatId}:`, error.message);
    await sendMessage(chatId, '⚠️ Failed to fetch the latest gold price. Please try again.', {
      reply_markup: getMainKeyboard(),
    });
  }
}

async function triggerPriorityAlerts(chatId, user, currentPrice, retailQuote) {
  let matched = false;
  let hasStateChanges = false;

  for (const [alertKey, config] of Object.entries(ALERT_DEFINITIONS)) {
    const target = user[alertKey];
    const currentRetailPrice = retailQuote?.[config.valueKey];

    if (target === null || !Number.isFinite(currentRetailPrice)) {
      continue;
    }

    if (!config.compare(currentRetailPrice, target)) {
      continue;
    }

    matched = true;
    const delivered = await sendMessage(
      chatId,
      [
        `${config.icon} Target tercapai`,
        `Pantauan: ${config.label} Lakuemas`,
        `Target: ${formatIdr(target)}/gr`,
        `Harga sekarang: ${formatIdr(currentRetailPrice)}/gr`,
      ].join('\n')
    );

    if (delivered) {
      user[alertKey] = null;
      hasStateChanges = true;
    }
  }

  if (user.alertAbove !== null && currentPrice >= user.alertAbove) {
    matched = true;
    const target = user.alertAbove;
    const delivered = await sendMessage(
      chatId,
      [
        '🚀 Target tercapai',
        'Pantauan: harga emas dunia',
        `Target: ${formatUsd(target)}/oz`,
        `Harga sekarang: ${formatUsd(currentPrice)}/oz`,
      ].join('\n')
    );

    if (delivered) {
      user.alertAbove = null;
      hasStateChanges = true;
    }
  }

  if (user.alertBelow !== null && currentPrice <= user.alertBelow) {
    matched = true;
    const target = user.alertBelow;
    const delivered = await sendMessage(
      chatId,
      [
        '📉 Target tercapai',
        'Pantauan: harga emas dunia',
        `Target: ${formatUsd(target)}/oz`,
        `Harga sekarang: ${formatUsd(currentPrice)}/oz`,
      ].join('\n')
    );

    if (delivered) {
      user.alertBelow = null;
      hasStateChanges = true;
    }
  }

  if (hasStateChanges) {
    await saveUsers();
  }

  return matched;
}

async function maybeTriggerImmediateAlert(chatId) {
  const user = ensureUser(chatId);

  try {
    const marketSnapshot = await fetchMarketSnapshot({ forceFxRefresh: true });
    await triggerPriorityAlerts(chatId, user, marketSnapshot.goldPrice, marketSnapshot.retailQuote);
  } catch (error) {
    console.error(`Immediate alert check failed for ${chatId}:`, error.message);
  }
}

async function handleNormalNotifications(currentPrice, previousPrice, marketSnapshot, previousRetailQuote) {
  const usdToIdrRate = marketSnapshot?.usdToIdrRate ?? (await fetchUsdToIdrRate());
  const retailQuote =
    marketSnapshot?.retailQuote ?? (await fetchRetailQuote(currentPrice, usdToIdrRate));
  const previousQuote = previousRetailQuote ?? retailQuote;
  const buyChange = retailQuote.buyIdrPerGram - previousQuote.buyIdrPerGram;
  const sellChange = retailQuote.sellIdrPerGram - previousQuote.sellIdrPerGram;
  const averageChange = (buyChange + sellChange) / 2;
  const spotChange = currentPrice - previousPrice;
  const direction = averageChange > 0 ? 'up' : averageChange < 0 ? 'down' : 'changed';
  const icon = averageChange > 0 ? '📈' : averageChange < 0 ? '📉' : '🔄';
  let hasChanges = false;

  for (const [chatId, user] of Object.entries(users)) {
    const hasPriorityAlert = await triggerPriorityAlerts(chatId, user, currentPrice, retailQuote);

    if (hasPriorityAlert) {
      hasChanges = true;
      continue;
    }

    const delivered = await sendMessage(
      chatId,
      [
        `${icon} Harga Lakuemas ${direction === 'up' ? 'naik' : direction === 'down' ? 'turun' : 'berubah'}`,
        '',
        `Harga beli sekarang: ${formatIdr(retailQuote.buyIdrPerGram)}/gr (${formatSignedIdr(buyChange)})`,
        `Harga jual sekarang: ${formatIdr(retailQuote.sellIdrPerGram)}/gr (${formatSignedIdr(sellChange)})`,
        `Selisih beli-jual: ${formatIdr(retailQuote.spreadIdr)} (${retailQuote.spreadPct.toFixed(2)}%)`,
        '',
        retailQuote.source === 'lakuemas'
          ? `Sumber harga: ${getRetailSourceLabel(retailQuote.source)}`
          : 'Info tambahan:',
        ...(retailQuote.source === 'lakuemas'
          ? []
          : [
              `Perubahan emas dunia: ${formatUsd(spotChange)}/oz`,
              `Kurs acuan: ${formatIdr(usdToIdrRate)}/USD`,
              `Sumber harga: ${getRetailSourceLabel(retailQuote.source)}`,
            ]),
      ].join('\n')
    );

    if (delivered) {
      hasChanges = true;
    }
  }

  if (hasChanges) {
    await saveUsers();
  }
}

async function checkPrice() {
  if (isCheckingPrice) {
    return;
  }

  isCheckingPrice = true;

  try {
    const marketSnapshot = await fetchMarketSnapshot();
    const { checkedAt, goldPrice: currentPrice, retailQuote } = marketSnapshot;
    const retailSignature = getRetailSignature(retailQuote);

    if (lastPrice === null) {
      for (const [chatId, user] of Object.entries(users)) {
        await triggerPriorityAlerts(chatId, user, currentPrice, retailQuote);
      }

      lastPrice = currentPrice;
      lastMarketCheckAt = checkedAt;
      lastRetailSignature = retailSignature;
      lastRetailQuote = retailQuote;
      return;
    }

    if (retailSignature !== lastRetailSignature) {
      lastRetailChangedAt = checkedAt;
      await handleNormalNotifications(currentPrice, lastPrice, marketSnapshot, lastRetailQuote);
    } else {
      for (const [chatId, user] of Object.entries(users)) {
        await triggerPriorityAlerts(chatId, user, currentPrice, retailQuote);
      }
    }

    lastPrice = currentPrice;
    lastMarketCheckAt = checkedAt;
    lastRetailSignature = retailSignature;
    lastRetailQuote = retailQuote;
  } catch (error) {
    console.error('Price monitor failed:', error.message);
  } finally {
    isCheckingPrice = false;
  }
}

bot.onText(/^\/start(?:@\w+)?$/, async (msg) => {
  const chatId = msg.chat.id;
  const user = ensureUser(chatId);
  user.pendingAction = null;
  await saveUsers();

  await sendMessage(
    chatId,
    [
      '👋 Bot pemantau harga emas siap digunakan.',
      '',
      'Gunakan tombol di bawah untuk:',
      `- cek harga beli dan harga jual terbaru`,
      `- pasang alert saat harga beli atau harga jual naik/turun ke target Anda`,
      `- lihat atau hapus alert yang sedang aktif`,
      '',
      'Perintah tambahan:',
      '/price - cek harga sekarang',
      '/alerts - lihat alert aktif',
      '/clear - hapus semua alert',
    ].join('\n')
    ,
    { reply_markup: getMainKeyboard() }
  );
});

bot.onText(/^\/price(?:@\w+)?$/, async (msg) => {
  await sendCurrentPrice(msg.chat.id);
});

bot.onText(/^\/alerts(?:@\w+)?$/, async (msg) => {
  const chatId = msg.chat.id;
  const user = ensureUser(chatId);
  user.pendingAction = null;
  await saveUsers();

  await sendMessage(chatId, `📌 Alert yang sedang aktif\n${formatActiveAlerts(user)}`, {
    reply_markup: getMainKeyboard(),
  });
});

bot.onText(/^\/setbuyabove(?:@\w+)?(?:\s+(.+))?$/, async (msg, match) => {
  const chatId = msg.chat.id;
  const user = ensureUser(chatId);

  if (!match?.[1]) {
    await promptRetailAlertInput(chatId, user, 'buyAbove');
    return;
  }

  await setRetailAlert(chatId, user, 'buyAbove', match[1]);
});

bot.onText(/^\/setbuybelow(?:@\w+)?(?:\s+(.+))?$/, async (msg, match) => {
  const chatId = msg.chat.id;
  const user = ensureUser(chatId);

  if (!match?.[1]) {
    await promptRetailAlertInput(chatId, user, 'buyBelow');
    return;
  }

  await setRetailAlert(chatId, user, 'buyBelow', match[1]);
});

bot.onText(/^\/setsellabove(?:@\w+)?(?:\s+(.+))?$/, async (msg, match) => {
  const chatId = msg.chat.id;
  const user = ensureUser(chatId);

  if (!match?.[1]) {
    await promptRetailAlertInput(chatId, user, 'sellAbove');
    return;
  }

  await setRetailAlert(chatId, user, 'sellAbove', match[1]);
});

bot.onText(/^\/setsellbelow(?:@\w+)?(?:\s+(.+))?$/, async (msg, match) => {
  const chatId = msg.chat.id;
  const user = ensureUser(chatId);

  if (!match?.[1]) {
    await promptRetailAlertInput(chatId, user, 'sellBelow');
    return;
  }

  await setRetailAlert(chatId, user, 'sellBelow', match[1]);
});

bot.onText(/^\/setabove(?:@\w+)?(?:\s+(.+))?$/, async (msg, match) => {
  const chatId = msg.chat.id;
  const user = ensureUser(chatId);
  const target = parseUsdTarget(match?.[1] || '');

  if (target === null) {
    await sendMessage(chatId, 'Format target belum benar.\nContoh: /setabove 4300', {
      reply_markup: getMainKeyboard(),
    });
    return;
  }

  user.alertAbove = target;
  user.pendingAction = null;
  await saveUsers();

  await sendMessage(chatId, `✅ Siap. Saya akan beri tahu saat harga emas dunia naik ke ${formatUsd(target)}/oz.`, {
    reply_markup: getMainKeyboard(),
  });
  await maybeTriggerImmediateAlert(chatId);
});

bot.onText(/^\/setbelow(?:@\w+)?(?:\s+(.+))?$/, async (msg, match) => {
  const chatId = msg.chat.id;
  const user = ensureUser(chatId);
  const target = parseUsdTarget(match?.[1] || '');

  if (target === null) {
    await sendMessage(chatId, 'Format target belum benar.\nContoh: /setbelow 4200', {
      reply_markup: getMainKeyboard(),
    });
    return;
  }

  user.alertBelow = target;
  user.pendingAction = null;
  await saveUsers();

  await sendMessage(chatId, `✅ Siap. Saya akan beri tahu saat harga emas dunia turun ke ${formatUsd(target)}/oz.`, {
    reply_markup: getMainKeyboard(),
  });
  await maybeTriggerImmediateAlert(chatId);
});

bot.onText(/^\/clear(?:@\w+)?$/, async (msg) => {
  const chatId = msg.chat.id;
  const user = ensureUser(chatId);

  clearAllAlerts(user);
  await saveUsers();

  await sendMessage(chatId, '🧹 Semua alert berhasil dihapus.', {
    reply_markup: getMainKeyboard(),
  });
});

bot.on('message', async (msg) => {
  const text = msg.text?.trim();

  if (!text || text.startsWith('/')) {
    return;
  }

  const chatId = msg.chat.id;
  const user = ensureUser(chatId);

  if (text === BUTTONS.price) {
    await sendCurrentPrice(chatId);
    return;
  }

  if (text === BUTTONS.alerts) {
    user.pendingAction = null;
    await saveUsers();
    await sendMessage(chatId, `📌 Alert yang sedang aktif\n${formatActiveAlerts(user)}`, {
      reply_markup: getMainKeyboard(),
    });
    return;
  }

  if (text === BUTTONS.clear) {
    clearAllAlerts(user);
    await saveUsers();
    await sendMessage(chatId, '🧹 Semua alert berhasil dihapus.', {
      reply_markup: getMainKeyboard(),
    });
    return;
  }

  if (text === BUTTONS.cancel) {
    user.pendingAction = null;
    await saveUsers();
    await sendMessage(chatId, 'Oke, dibatalkan.', {
      reply_markup: getMainKeyboard(),
    });
    return;
  }

  const buttonAlertType = BUTTON_TO_ALERT_TYPE[text];

  if (buttonAlertType) {
    await promptRetailAlertInput(chatId, user, buttonAlertType);
    return;
  }

  if (user.pendingAction) {
    await setRetailAlert(chatId, user, user.pendingAction, text);
  }
});

bot.on('polling_error', (error) => {
  console.error('Telegram polling error:', error.message);
});

async function start() {
  await loadUsers();
  await checkPrice();
  pollTimer = setInterval(checkPrice, POLL_INTERVAL_MS);
  console.log('Gold Telegram bot is running.');
}

async function shutdown(signal) {
  if (pollTimer) {
    clearInterval(pollTimer);
  }

  console.log(`Received ${signal}. Shutting down...`);

  try {
    await bot.stopPolling();
    if (dbPool) {
      await dbPool.end();
    }
  } catch (error) {
    console.error('Failed to stop Telegram polling:', error.message);
  } finally {
    process.exit(0);
  }
}

process.on('SIGINT', () => {
  void shutdown('SIGINT');
});

process.on('SIGTERM', () => {
  void shutdown('SIGTERM');
});

process.on('unhandledRejection', (error) => {
  console.error('Unhandled rejection:', error);
});

process.on('uncaughtException', (error) => {
  console.error('Uncaught exception:', error);
});

start().catch((error) => {
  console.error('Bot failed to start:', error);
  process.exit(1);
});
