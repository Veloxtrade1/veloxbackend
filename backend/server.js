/**
 * VELOX TRADING PLATFORM — PRODUCTION BACKEND v2
 * MongoDB · TradingView WebSocket · Alpha Vantage Prices · Bot Order Book
 */

require('dotenv').config();
const express   = require('express');
const http      = require('http');
const WebSocket = require('ws');
const cors      = require('cors');
const bcrypt    = require('bcryptjs');
const jwt       = require('jsonwebtoken');
const stripe    = require('stripe')(process.env.STRIPE_SECRET_KEY || 'sk_test_placeholder');
const crypto    = require('crypto');
const https     = require('https');

// ── MongoDB ──────────────────────────────────────────────────
let mongoose, User, Account, Trade, Transaction;
const MONGO_URL = process.env.MONGODB_URI || process.env.DATABASE_URL || '';

async function connectMongo() {
  if (!MONGO_URL) {
    console.log('⚠️  No MONGODB_URI — using in-memory fallback');
    return false;
  }
  try {
    mongoose = require('mongoose');
    await mongoose.connect(MONGO_URL, { serverSelectionTimeoutMS: 5000 });
    console.log('✅ MongoDB connected');

    const UserSchema = new mongoose.Schema({
      firstName: String, lastName: String,
      email: { type: String, unique: true, lowercase: true },
      password: String, phone: String, country: String,
      kycStatus: { type: String, default: 'pending' },
      emailVerified: { type: Boolean, default: false },
      createdAt: { type: Date, default: Date.now }
    });
    const AccountSchema = new mongoose.Schema({
      userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
      accountNumber: String, currency: { type: String, default: 'USD' },
      balance: { type: Number, default: 0 },
      equity: { type: Number, default: 0 },
      margin: { type: Number, default: 0 },
      freeMargin: { type: Number, default: 0 },
      leverage: { type: Number, default: 500 },
      type: { type: String, default: 'standard' },
      isDemo: { type: Boolean, default: false },
      createdAt: { type: Date, default: Date.now }
    });
    const TradeSchema = new mongoose.Schema({
      userId: mongoose.Schema.Types.ObjectId,
      accountId: mongoose.Schema.Types.ObjectId,
      symbol: String, type: String, lots: Number,
      openPrice: Number, closePrice: Number,
      currentPrice: Number, sl: Number, tp: Number,
      margin: Number, commission: Number,
      swap: { type: Number, default: 0 },
      profit: { type: Number, default: 0 },
      status: { type: String, default: 'open' },
      openTime: { type: Date, default: Date.now },
      closeTime: Date
    });
    const TransactionSchema = new mongoose.Schema({
      userId: mongoose.Schema.Types.ObjectId,
      accountId: mongoose.Schema.Types.ObjectId,
      type: String, amount: Number,
      method: String, status: String,
      stripeIntentId: String,
      bankDetails: String,
      createdAt: { type: Date, default: Date.now }
    });

    User        = mongoose.models.User        || mongoose.model('User', UserSchema);
    Account     = mongoose.models.Account     || mongoose.model('Account', AccountSchema);
    Trade       = mongoose.models.Trade       || mongoose.model('Trade', TradeSchema);
    Transaction = mongoose.models.Transaction || mongoose.model('Transaction', TransactionSchema);
    return true;
  } catch (err) {
    console.log('⚠️  MongoDB failed:', err.message, '— using in-memory fallback');
    return false;
  }
}

// ── In-memory fallback ───────────────────────────────────────
const mem = { users: new Map(), accounts: new Map(), trades: new Map(), transactions: new Map() };
let useMongo = false;

// ── DB abstraction layer (works with both Mongo and in-memory) ──
const db = {
  async createUser(data) {
    if (useMongo) { const u = new User(data); return await u.save(); }
    mem.users.set(data._id, data); return data;
  },
  async findUserByEmail(email) {
    if (useMongo) return await User.findOne({ email: email.toLowerCase() });
    return [...mem.users.values()].find(u => u.email === email.toLowerCase());
  },
  async findUserById(id) {
    if (useMongo) return await User.findById(id);
    return mem.users.get(id?.toString());
  },
  async createAccount(data) {
    if (useMongo) { const a = new Account(data); return await a.save(); }
    mem.accounts.set(data._id, data); return data;
  },
  async findAccountsByUser(userId) {
    if (useMongo) return await Account.find({ userId });
    return [...mem.accounts.values()].filter(a => a.userId?.toString() === userId?.toString());
  },
  async findAccountById(id) {
    if (useMongo) return await Account.findById(id);
    return mem.accounts.get(id?.toString());
  },
  async updateAccount(id, data) {
    if (useMongo) return await Account.findByIdAndUpdate(id, data, { new: true });
    const a = mem.accounts.get(id?.toString());
    if (a) { Object.assign(a, data); return a; }
  },
  async createTrade(data) {
    if (useMongo) { const t = new Trade(data); return await t.save(); }
    mem.trades.set(data._id, data); return data;
  },
  async findOpenTrades(userId) {
    if (useMongo) return await Trade.find({ userId, status: 'open' });
    return [...mem.trades.values()].filter(t => t.userId?.toString() === userId?.toString() && t.status === 'open');
  },
  async findTradeById(id) {
    if (useMongo) return await Trade.findById(id);
    return mem.trades.get(id?.toString());
  },
  async closeTrade(id, data) {
    if (useMongo) return await Trade.findByIdAndUpdate(id, data, { new: true });
    const t = mem.trades.get(id?.toString());
    if (t) { Object.assign(t, data); return t; }
  },
  async findTradeHistory(userId) {
    if (useMongo) return await Trade.find({ userId, status: 'closed' }).sort({ closeTime: -1 }).limit(100);
    return [...mem.trades.values()].filter(t => t.userId?.toString() === userId?.toString() && t.status === 'closed').slice(-100).reverse();
  },
  async createTransaction(data) {
    if (useMongo) { const tx = new Transaction(data); return await tx.save(); }
    mem.transactions.set(data._id, data); return data;
  },
  async findTransactions(userId) {
    if (useMongo) return await Transaction.find({ userId }).sort({ createdAt: -1 });
    return [...mem.transactions.values()].filter(t => t.userId?.toString() === userId?.toString()).reverse();
  }
};

// ── ALPHA VANTAGE PRICE FEED ─────────────────────────────────
const AV_KEY = process.env.ALPHA_VANTAGE_KEY || 'demo';
const QUANDL_KEY = process.env.QUANDL_KEY || '';

// Real prices from Alpha Vantage (refreshed every 60s)
let avPrices = {};
let avLastFetch = 0;
const AV_SYMBOLS = ['EURUSD', 'GBPUSD', 'USDJPY', 'XAUUSD', 'BTCUSD'];

function fetchAVPrice(fromSym, toSym) {
  return new Promise((resolve) => {
    const path = fromSym === 'BTC'
      ? `/query?function=CURRENCY_EXCHANGE_RATE&from_currency=BTC&to_currency=USD&apikey=${AV_KEY}`
      : `/query?function=CURRENCY_EXCHANGE_RATE&from_currency=${fromSym}&to_currency=${toSym}&apikey=${AV_KEY}`;
    const req = https.get({ hostname: 'www.alphavantage.co', path, timeout: 5000 }, (res) => {
      let data = '';
      res.on('data', d => data += d);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          const rate = json['Realtime Currency Exchange Rate'];
          if (rate) resolve(parseFloat(rate['5. Exchange Rate']));
          else resolve(null);
        } catch { resolve(null); }
      });
    });
    req.on('error', () => resolve(null));
    req.on('timeout', () => { req.destroy(); resolve(null); });
  });
}

async function refreshAVPrices() {
  const now = Date.now();
  if (now - avLastFetch < 60000) return; // max 1 per minute
  avLastFetch = now;
  try {
    const pairs = [['EUR','USD'],['GBP','USD'],['USD','JPY'],['XAU','USD'],['BTC','USD']];
    const symbols = ['EURUSD','GBPUSD','USDJPY','XAUUSD','BTCUSD'];
    const results = await Promise.allSettled(pairs.map(([f,t]) => fetchAVPrice(f,t)));
    results.forEach((r, i) => {
      if (r.status === 'fulfilled' && r.value) {
        const sym = symbols[i];
        avPrices[sym] = r.value;
        if (prices[sym]) prices[sym] = r.value; // sync to main prices
        console.log(`📡 AV: ${sym} = ${r.value}`);
      }
    });
  } catch(e) { console.log('AV fetch error:', e.message); }
}

// Initial fetch
setTimeout(refreshAVPrices, 3000);
setInterval(refreshAVPrices, 60000);

// ── SIMULATED PRICES (seeded from AV when available) ─────────
const prices = {
  EURUSD:1.0843, GBPUSD:1.2678, USDJPY:149.23, USDCHF:0.9012,
  AUDUSD:0.6534, USDCAD:1.3642, NZDUSD:0.5923, EURGBP:0.8562,
  EURJPY:161.82, GBPJPY:189.40,
  XAUUSD:2314.5, XAGUSD:27.34, WTIUSD:78.42, NGAS:2.84,
  BTCUSD:67842, ETHUSD:3428.1, SOLUSD:142.3, BNBUSD:432.1,
  XRPUSD:0.5234, ADAUSD:0.4521, DOTUSD:7.82, MATICUSD:0.88,
  US30:38420, SPX500:5124, NAS100:17834, DAX40:18234, FTSE100:7834,
  NIKKEI:38920, HANGSENG:17430,
  AAPL:192.62, TSLA:213.06, NVDA:874.5, AMZN:185.4,
  MSFT:415.3, GOOGL:174.2, META:493.5, BABA:74.2,
};

// Volatility per symbol
const VOL = {
  BTCUSD:0.003, ETHUSD:0.0028, SOLUSD:0.004, BNBUSD:0.003,
  XRPUSD:0.004, ADAUSD:0.004, DOTUSD:0.004, MATICUSD:0.005,
  XAUUSD:0.0015, WTIUSD:0.002, NGAS:0.003, XAGUSD:0.002,
  US30:0.001, SPX500:0.001, NAS100:0.0012, DAX40:0.001,
  AAPL:0.002, TSLA:0.003, NVDA:0.0025, AMZN:0.002,
  EURUSD:0.0004, GBPUSD:0.0005, USDJPY:0.0004,
};

// ── BOT ORDER BOOK ────────────────────────────────────────────
// Simulates real market depth with bot traders
const orderBook = {};
const BOT_NAMES = [
  'AlgoBot_FX01','QuantTrader_Pro','MarketMaker_X','HFT_Delta',
  'ArbitrageBot','TrendFollower','ScalpBot_v2','GridTrader',
  'NewsBot_Alpha','LiquidityBot'
];

function initOrderBook(symbol) {
  if (orderBook[symbol]) return;
  const mid = prices[symbol] || 1;
  const spread = mid * 0.0003;
  orderBook[symbol] = {
    bids: [], // buy orders below mid
    asks: [], // sell orders above mid
    lastTrades: [],
    volume24h: Math.floor(Math.random() * 1000000 + 500000)
  };
  // Populate initial depth
  for (let i = 0; i < 10; i++) {
    const priceDelta = spread * (i + 1) * (1 + Math.random() * 0.5);
    orderBook[symbol].bids.push({
      price: parseFloat((mid - priceDelta).toFixed(5)),
      size: parseFloat((Math.random() * 2 + 0.1).toFixed(2)),
      bot: BOT_NAMES[Math.floor(Math.random() * BOT_NAMES.length)],
      time: Date.now()
    });
    orderBook[symbol].asks.push({
      price: parseFloat((mid + priceDelta).toFixed(5)),
      size: parseFloat((Math.random() * 2 + 0.1).toFixed(2)),
      bot: BOT_NAMES[Math.floor(Math.random() * BOT_NAMES.length)],
      time: Date.now()
    });
  }
}

function updateOrderBook(symbol) {
  const mid = prices[symbol];
  if (!mid) return;
  initOrderBook(symbol);
  const ob = orderBook[symbol];
  const spread = mid * 0.0003;

  // Bots add/remove/modify orders randomly
  const action = Math.random();

  if (action < 0.3 && ob.bids.length < 15) {
    // Add new bid
    const depth = Math.floor(Math.random() * 8) + 1;
    ob.bids.push({
      price: parseFloat((mid - spread * depth).toFixed(5)),
      size: parseFloat((Math.random() * 3 + 0.1).toFixed(2)),
      bot: BOT_NAMES[Math.floor(Math.random() * BOT_NAMES.length)],
      time: Date.now()
    });
  } else if (action < 0.6 && ob.asks.length < 15) {
    ob.asks.push({
      price: parseFloat((mid + spread * (Math.floor(Math.random() * 8) + 1)).toFixed(5)),
      size: parseFloat((Math.random() * 3 + 0.1).toFixed(2)),
      bot: BOT_NAMES[Math.floor(Math.random() * BOT_NAMES.length)],
      time: Date.now()
    });
  } else if (action < 0.7 && ob.bids.length > 3) {
    // Remove a bid (filled)
    const idx = Math.floor(Math.random() * ob.bids.length);
    const filled = ob.bids.splice(idx, 1)[0];
    ob.lastTrades.unshift({ side:'buy', price:filled.price, size:filled.size, time:Date.now(), bot:filled.bot });
    ob.volume24h += filled.size * filled.price;
  } else if (ob.asks.length > 3) {
    const idx = Math.floor(Math.random() * ob.asks.length);
    const filled = ob.asks.splice(idx, 1)[0];
    ob.lastTrades.unshift({ side:'sell', price:filled.price, size:filled.size, time:Date.now(), bot:filled.bot });
    ob.volume24h += filled.size * filled.price;
  }

  // Keep lastTrades to 20
  if (ob.lastTrades.length > 20) ob.lastTrades.length = 20;

  // Sort order book
  ob.bids.sort((a,b) => b.price - a.price);
  ob.asks.sort((a,b) => a.price - b.price);

  // Update spread
  ob.spread = ob.asks[0] && ob.bids[0]
    ? parseFloat((ob.asks[0].price - ob.bids[0].price).toFixed(5)) : spread * 2;
}

// Init order books for major symbols
['EURUSD','GBPUSD','XAUUSD','BTCUSD','US30','ETHUSD'].forEach(initOrderBook);

// ── PRICE ENGINE ─────────────────────────────────────────────
// Realistic price movement with mean reversion + momentum
const priceState = {};
Object.keys(prices).forEach(sym => {
  priceState[sym] = { momentum: 0, trend: (Math.random() - 0.5) * 0.0001 };
});

function tickPrices() {
  for (const sym of Object.keys(prices)) {
    const vol = VOL[sym] || 0.0005;
    const state = priceState[sym];

    // Mean reversion + momentum
    const noise = (Math.random() - 0.499) * vol;
    state.momentum = state.momentum * 0.85 + noise * 0.15;
    if (Math.random() < 0.02) state.trend = (Math.random() - 0.5) * 0.0001; // trend shift

    prices[sym] = prices[sym] * (1 + state.momentum + state.trend);

    // Hard bounds to prevent drift
    const base = { EURUSD:1.08, GBPUSD:1.26, USDJPY:149, XAUUSD:2300, BTCUSD:67000,
                   US30:38000, SPX500:5000, NAS100:17000 };
    if (base[sym]) {
      const drift = (prices[sym] - base[sym]) / base[sym];
      if (Math.abs(drift) > 0.08) prices[sym] = base[sym] * (1 + drift * 0.5); // pull back
    }

    // Update order book for active symbols
    if (orderBook[sym]) updateOrderBook(sym);
  }

  // Broadcast to all WS clients
  const payload = JSON.stringify({ type:'PRICES', prices, ts:Date.now() });
  wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(payload); });
}
setInterval(tickPrices, 500);

// ── SERVER SETUP ─────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

const PORT       = process.env.PORT || 4000;
const JWT_SECRET = process.env.JWT_SECRET || 'velox_dev_secret_change_in_production';

// CORS — allow all Netlify deployments + localhost
app.use((req, res, next) => {
  const origin = req.headers.origin;
  // Allow any netlify.app subdomain, localhost, or no origin (server-to-server)
  if (!origin || origin.includes('netlify.app') || origin.includes('localhost') || origin.includes('railway.app')) {
    res.setHeader('Access-Control-Allow-Origin', origin || '*');
    res.setHeader('Access-Control-Allow-Credentials', 'true');
    res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS,PATCH');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization,Accept,Origin,X-Requested-With');
  }
  if (req.method === 'OPTIONS') return res.status(200).end();
  next();
});
app.use(cors({ origin: true, credentials: true }));
app.use(express.json({ limit: '10mb' }));

// ── AUTH MIDDLEWARE ──────────────────────────────────────────
function auth(req, res, next) {
  const token = req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'No token' });
  try { req.user = jwt.verify(token, JWT_SECRET); next(); }
  catch { res.status(401).json({ error: 'Invalid token' }); }
}

// ── HEALTH ───────────────────────────────────────────────────
app.get('/', (req, res) => res.json({
  status: 'VELOX API Running', version: '2.0.0',
  db: useMongo ? 'MongoDB' : 'Memory',
  prices: Object.keys(prices).length,
  orderBooks: Object.keys(orderBook).length,
  timestamp: new Date().toISOString()
}));
app.get('/health', (req, res) => res.json({ healthy: true, uptime: process.uptime() }));

// ── AUTH ROUTES ──────────────────────────────────────────────
app.post('/api/auth/register', async (req, res) => {
  try {
    const { firstName, lastName, email, password, phone, country, currency='USD' } = req.body;
    if (!firstName || !email || !password) return res.status(400).json({ error: 'Required fields missing' });
    if (password.length < 8) return res.status(400).json({ error: 'Password min 8 characters' });

    const existing = await db.findUserByEmail(email);
    if (existing) return res.status(409).json({ error: 'Email already registered' });

    const hashedPw = await bcrypt.hash(password, 12);
    const userId = new (require('mongoose') ? mongoose.Types.ObjectId : Object)() || crypto.randomUUID();
    const uid = useMongo ? new mongoose.Types.ObjectId() : crypto.randomUUID();

    const user = await db.createUser({
      _id: uid, firstName, lastName,
      email: email.toLowerCase(), password: hashedPw,
      phone, country, kycStatus:'pending'
    });

    // Live account
    const liveAcct = await db.createAccount({
      _id: useMongo ? new mongoose.Types.ObjectId() : crypto.randomUUID(),
      userId: uid, currency, balance:0, equity:0, margin:0,
      freeMargin:0, leverage:500, type:'standard', isDemo:false,
      accountNumber:`VLX${Math.floor(100000+Math.random()*900000)}`
    });

    // Demo account $10,000
    const demoAcct = await db.createAccount({
      _id: useMongo ? new mongoose.Types.ObjectId() : crypto.randomUUID(),
      userId: uid, currency, balance:10000, equity:10000, margin:0,
      freeMargin:10000, leverage:500, type:'demo', isDemo:true,
      accountNumber:`DEMO${Math.floor(100000+Math.random()*900000)}`
    });

    const token = jwt.sign({ userId: uid.toString(), email: email.toLowerCase() }, JWT_SECRET, { expiresIn:'7d' });
    const { password:_, ...safeUser } = user.toObject ? user.toObject() : user;
    res.json({ success:true, token, user:safeUser, accounts:[liveAcct, demoAcct] });
  } catch(e) {
    console.error('Register error:', e);
    res.status(500).json({ error: 'Registration failed' });
  }
});

app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    const user = await db.findUserByEmail(email);
    if (!user) return res.status(401).json({ error: 'Invalid credentials' });

    const valid = await bcrypt.compare(password, user.password);
    if (!valid) return res.status(401).json({ error: 'Invalid credentials' });

    const accounts = await db.findAccountsByUser(user._id);
    const token = jwt.sign({ userId: user._id.toString(), email: user.email }, JWT_SECRET, { expiresIn:'7d' });
    const { password:_, ...safeUser } = user.toObject ? user.toObject() : user;
    res.json({ success:true, token, user:safeUser, accounts });
  } catch(e) {
    res.status(500).json({ error: 'Login failed' });
  }
});

app.get('/api/auth/me', auth, async (req, res) => {
  try {
    const user = await db.findUserById(req.user.userId);
    if (!user) return res.status(404).json({ error: 'Not found' });
    const accounts = await db.findAccountsByUser(user._id);
    const { password:_, ...safeUser } = user.toObject ? user.toObject() : user;
    res.json({ user:safeUser, accounts });
  } catch(e) { res.status(500).json({ error: 'Error' }); }
});

// ── ACCOUNTS ─────────────────────────────────────────────────
app.get('/api/accounts', auth, async (req, res) => {
  const accounts = await db.findAccountsByUser(req.user.userId);
  res.json({ accounts });
});

// ── DEPOSITS ─────────────────────────────────────────────────
app.post('/api/deposits/stripe/intent', auth, async (req, res) => {
  try {
    const { amount, currency='usd', accountId } = req.body;
    if (!amount || amount < 10) return res.status(400).json({ error: 'Minimum $10' });
    const pi = await stripe.paymentIntents.create({
      amount: Math.round(amount * 100), currency: currency.toLowerCase(),
      metadata: { userId: req.user.userId, accountId }
    });
    res.json({ clientSecret: pi.client_secret });
  } catch(e) {
    res.status(500).json({ error: 'Stripe error: ' + e.message });
  }
});

app.post('/api/deposits/confirm', auth, async (req, res) => {
  try {
    const { accountId, amount, method='card' } = req.body;
    const acct = await db.findAccountById(accountId);
    if (!acct) return res.status(404).json({ error: 'Account not found' });

    const newBal = (acct.balance || 0) + parseFloat(amount);
    const updated = await db.updateAccount(accountId, {
      balance: newBal, equity: newBal + (acct.margin || 0),
      freeMargin: newBal - (acct.margin || 0)
    });

    await db.createTransaction({
      _id: useMongo ? new mongoose.Types.ObjectId() : crypto.randomUUID(),
      userId: acct.userId, accountId, type:'deposit',
      amount, method, status:'completed'
    });

    broadcastToUser(acct.userId.toString(), { type:'BALANCE_UPDATE', account: updated });
    res.json({ success:true, account: updated });
  } catch(e) { res.status(500).json({ error: 'Deposit failed' }); }
});

app.post('/api/webhooks/stripe', express.raw({ type:'application/json' }), async (req, res) => {
  try {
    const event = stripe.webhooks.constructEvent(
      req.body, req.headers['stripe-signature'], process.env.STRIPE_WEBHOOK_SECRET || ''
    );
    if (event.type === 'payment_intent.succeeded') {
      const pi = event.data.object;
      const { accountId, userId } = pi.metadata;
      const amount = pi.amount / 100;
      const acct = await db.findAccountById(accountId);
      if (acct) {
        const newBal = acct.balance + amount;
        await db.updateAccount(accountId, { balance:newBal, equity:newBal, freeMargin:newBal });
        await db.createTransaction({
          _id: useMongo ? new mongoose.Types.ObjectId() : crypto.randomUUID(),
          userId, accountId, type:'deposit', amount, method:'stripe', status:'completed'
        });
        broadcastToUser(userId, { type:'BALANCE_UPDATE' });
      }
    }
    res.json({ received:true });
  } catch(e) { res.status(400).json({ error: e.message }); }
});

// ── WITHDRAWALS ──────────────────────────────────────────────
app.post('/api/withdrawals/request', auth, async (req, res) => {
  try {
    const { accountId, amount, method, bankDetails } = req.body;
    const acct = await db.findAccountById(accountId);
    if (!acct) return res.status(404).json({ error: 'Account not found' });
    if (amount < 10) return res.status(400).json({ error: 'Minimum $10' });
    if (amount > acct.freeMargin) return res.status(400).json({ error: 'Insufficient free margin' });

    const newBal = acct.balance - parseFloat(amount);
    const updated = await db.updateAccount(accountId, {
      balance: newBal, equity: newBal, freeMargin: newBal - (acct.margin || 0)
    });

    await db.createTransaction({
      _id: useMongo ? new mongoose.Types.ObjectId() : crypto.randomUUID(),
      userId: acct.userId, accountId, type:'withdrawal',
      amount, method, status:'processing',
      bankDetails: JSON.stringify(bankDetails || {})
    });

    broadcastToUser(acct.userId.toString(), { type:'BALANCE_UPDATE', account: updated });
    res.json({ success:true, message:'Withdrawal submitted', account: updated });
  } catch(e) { res.status(500).json({ error: 'Withdrawal failed' }); }
});

// ── TRADING ──────────────────────────────────────────────────
app.post('/api/trades/open', auth, async (req, res) => {
  try {
    const { accountId, symbol, type, lots, sl, tp } = req.body;
    const acct = await db.findAccountById(accountId);
    if (!acct) return res.status(404).json({ error: 'Account not found' });

    const price = prices[symbol] || 1;
    const spread = price * 0.00005;
    const openPrice = type === 'buy' ? price + spread : price - spread;
    const requiredMargin = (price * lots * 100000) / acct.leverage;
    const commission = lots * 3.5;

    if (requiredMargin > acct.freeMargin) return res.status(400).json({ error: 'Insufficient margin' });

    const newMargin = (acct.margin || 0) + requiredMargin;
    const newBal = acct.balance - commission;
    const updated = await db.updateAccount(accountId, {
      margin: newMargin,
      balance: newBal,
      freeMargin: acct.equity - newMargin
    });

    const trade = await db.createTrade({
      _id: useMongo ? new mongoose.Types.ObjectId() : crypto.randomUUID(),
      userId: acct.userId, accountId, symbol, type, lots,
      openPrice: parseFloat(openPrice.toFixed(5)),
      currentPrice: price, margin: requiredMargin,
      commission, sl: sl || null, tp: tp || null, profit: 0
    });

    broadcastToUser(acct.userId.toString(), { type:'TRADE_OPENED', trade, account: updated });
    res.json({ success:true, trade, account: updated });
  } catch(e) {
    console.error(e);
    res.status(500).json({ error: 'Trade failed' });
  }
});

app.post('/api/trades/close/:id', auth, async (req, res) => {
  try {
    const trade = await db.findTradeById(req.params.id);
    if (!trade) return res.status(404).json({ error: 'Trade not found' });

    const closePrice = prices[trade.symbol] || trade.openPrice;
    const priceDiff = trade.type === 'buy'
      ? closePrice - trade.openPrice
      : trade.openPrice - closePrice;
    const profit = priceDiff * trade.lots * 100000;

    const acct = await db.findAccountById(trade.accountId);
    const newMargin = Math.max(0, (acct.margin || 0) - trade.margin);
    const newBal = acct.balance + profit;
    const updated = await db.updateAccount(trade.accountId, {
      margin: newMargin, balance: newBal, equity: newBal,
      freeMargin: newBal - newMargin
    });

    const closed = await db.closeTrade(trade._id, {
      status:'closed', closePrice, profit, closeTime: new Date()
    });

    broadcastToUser(acct.userId.toString(), { type:'TRADE_CLOSED', trade:closed, account:updated, profit });
    res.json({ success:true, trade:closed, account:updated, profit });
  } catch(e) { res.status(500).json({ error: 'Close failed' }); }
});

app.get('/api/trades/open', auth, async (req, res) => {
  const trades = await db.findOpenTrades(req.user.userId);
  res.json({ trades });
});

app.get('/api/trades/history', auth, async (req, res) => {
  const trades = await db.findTradeHistory(req.user.userId);
  res.json({ trades });
});

app.get('/api/transactions', auth, async (req, res) => {
  const transactions = await db.findTransactions(req.user.userId);
  res.json({ transactions });
});

// ── MARKET DATA ──────────────────────────────────────────────
app.get('/api/markets/prices', (req, res) => res.json({ prices, timestamp: Date.now() }));

app.get('/api/markets/orderbook/:symbol', (req, res) => {
  const { symbol } = req.params;
  initOrderBook(symbol);
  res.json({ symbol, orderBook: orderBook[symbol], timestamp: Date.now() });
});

app.get('/api/markets/candles/:symbol', (req, res) => {
  const { symbol } = req.params;
  const { timeframe='60', count='200' } = req.query;
  const base = prices[symbol] || 1;
  const candles = [];
  let p = base * 0.97;
  const now = Math.floor(Date.now() / 1000);
  const interval = parseInt(timeframe) * 60;

  for (let i = parseInt(count); i >= 0; i--) {
    const vol = VOL[symbol] || 0.0005;
    const o=p, h=o*(1+Math.random()*vol*3), l=o*(1-Math.random()*vol*3);
    const c=l+Math.random()*(h-l);
    candles.push({
      time: now - i*interval,
      open: parseFloat(o.toFixed(5)), high: parseFloat(h.toFixed(5)),
      low: parseFloat(l.toFixed(5)), close: parseFloat(c.toFixed(5)),
      volume: Math.floor(Math.random()*50000+10000)
    });
    p = c;
  }
  res.json({ symbol, timeframe, candles });
});

app.get('/api/markets/symbols', (req, res) => {
  res.json({ symbols: Object.keys(prices).map(s => ({
    symbol: s, price: prices[s],
    spread: orderBook[s]?.spread || prices[s] * 0.0001
  }))});
});

// ── WEBSOCKET ────────────────────────────────────────────────
const userSockets = new Map();

function broadcastToUser(userId, data) {
  const clients = userSockets.get(userId);
  if (!clients) return;
  const msg = JSON.stringify(data);
  clients.forEach(ws => { if (ws.readyState === WebSocket.OPEN) ws.send(msg); });
}

wss.on('connection', (ws) => {
  let userId = null;

  ws.on('message', (msg) => {
    try {
      const data = JSON.parse(msg);
      if (data.type === 'AUTH') {
        try {
          const decoded = jwt.verify(data.token, JWT_SECRET);
          userId = decoded.userId;
          if (!userSockets.has(userId)) userSockets.set(userId, new Set());
          userSockets.get(userId).add(ws);
          ws.send(JSON.stringify({ type:'AUTH_OK', userId }));
        } catch { ws.send(JSON.stringify({ type:'AUTH_FAIL' })); }
      }
      if (data.type === 'SUBSCRIBE_ORDERBOOK') {
        const sym = data.symbol;
        initOrderBook(sym);
        ws.send(JSON.stringify({ type:'ORDERBOOK', symbol:sym, data:orderBook[sym] }));
      }
    } catch {}
  });

  ws.on('close', () => {
    if (userId && userSockets.has(userId)) {
      userSockets.get(userId).delete(ws);
      if (userSockets.get(userId).size === 0) userSockets.delete(userId);
    }
  });

  // Send initial prices immediately
  ws.send(JSON.stringify({ type:'PRICES', prices, ts: Date.now() }));
});

// ── START ────────────────────────────────────────────────────
connectMongo().then(connected => {
  useMongo = connected;
  server.listen(PORT, () => {
    console.log(`\n🚀 VELOX API v2 running on port ${PORT}`);
    console.log(`   DB: ${useMongo ? '✅ MongoDB' : '⚡ In-Memory (add MONGODB_URI for persistence)'}`);
    console.log(`   Alpha Vantage: ${AV_KEY !== 'demo' ? '✅ Live prices' : '⚡ Demo key (add ALPHA_VANTAGE_KEY)'}`);
    console.log(`   WebSocket: ws://localhost:${PORT}`);
    console.log(`   Symbols: ${Object.keys(prices).length}`);
    console.log(`   Order Books: ${Object.keys(orderBook).length}\n`);
  });
});
