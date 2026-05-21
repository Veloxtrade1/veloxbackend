/**
 * VELOX TRADING PLATFORM — COMPLETE BACKEND SERVER
 * Node.js + Express + WebSockets + JWT + Stripe
 * 
 * Deploy: node server.js (or pm2 start server.js)
 */

require('dotenv').config();
const express     = require('express');
const http        = require('http');
const WebSocket   = require('ws');
const cors        = require('cors');
const bcrypt      = require('bcryptjs');
const jwt         = require('jsonwebtoken');
const stripe      = require('stripe')(process.env.STRIPE_SECRET_KEY || 'sk_test_REPLACE_WITH_REAL_KEY');
const { Pool }    = require('pg');
// path not needed (no static file serving)
const crypto      = require('crypto');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

// ── CONFIG ──────────────────────────────────────────────────
const PORT       = process.env.PORT || 4000;
const JWT_SECRET = process.env.JWT_SECRET || 'velox_jwt_secret_CHANGE_IN_PRODUCTION';
const FRONTEND   = process.env.FRONTEND_URL || 'http://localhost:3000';

// ── DATABASE ─────────────────────────────────────────────────
// Production: PostgreSQL
// For quick testing, this uses an in-memory store. Replace with real DB in prod.
const db = {
  users: new Map(),
  accounts: new Map(),
  transactions: new Map(),
  trades: new Map(),
  deposits: new Map(),
};

// ── MIDDLEWARE ───────────────────────────────────────────────
app.use(cors({ origin: ['https://veloxtrade.netlify.app', 'http://localhost:3000', 'http://localhost:4000', /.netlify.app$/, /.railway.app$/], credentials: true }));
app.use(express.json({ limit: "10mb" }));
// API health endpoints
app.get('/', (req, res) => res.json({ status: 'VELOX API Running', version: '1.0.0', docs: '/api', timestamp: new Date().toISOString() }));
app.get('/health', (req, res) => res.json({ healthy: true }));

// ── AUTH MIDDLEWARE ──────────────────────────────────────────
function authMiddleware(req, res, next) {
  const token = req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'No token provided' });
  try {
    req.user = jwt.verify(token, JWT_SECRET);
    next();
  } catch {
    res.status(401).json({ error: 'Invalid token' });
  }
}

// ══════════════════════════════════════════════════════════════
// AUTH ROUTES
// ══════════════════════════════════════════════════════════════

// Register
app.post('/api/auth/register', async (req, res) => {
  try {
    const { firstName, lastName, email, password, phone, country, currency = 'USD' } = req.body;

    if (!firstName || !lastName || !email || !password)
      return res.status(400).json({ error: 'All fields are required' });

    if (password.length < 8)
      return res.status(400).json({ error: 'Password must be at least 8 characters' });

    if ([...db.users.values()].find(u => u.email === email))
      return res.status(409).json({ error: 'Email already registered' });

    const hashedPassword = await bcrypt.hash(password, 12);
    const userId = crypto.randomUUID();

    const user = {
      id: userId,
      firstName, lastName, email, phone, country,
      password: hashedPassword,
      createdAt: new Date().toISOString(),
      kycStatus: 'pending',
      emailVerified: false,
    };

    // Create trading account
    const accountId = crypto.randomUUID();
    const account = {
      id: accountId,
      userId,
      currency,
      balance: 0,
      equity: 0,
      margin: 0,
      freeMargin: 0,
      marginLevel: 0,
      openPL: 0,
      accountNumber: `VLX${Math.floor(100000 + Math.random() * 900000)}`,
      leverage: 500,
      type: 'standard',
      createdAt: new Date().toISOString(),
    };

    // Demo account with $10,000 virtual funds
    const demoAccountId = crypto.randomUUID();
    const demoAccount = {
      ...account,
      id: demoAccountId,
      balance: 10000,
      equity: 10000,
      freeMargin: 10000,
      accountNumber: `DEMO${Math.floor(100000 + Math.random() * 900000)}`,
      type: 'demo',
      isDemo: true,
    };

    db.users.set(userId, user);
    db.accounts.set(accountId, account);
    db.accounts.set(demoAccountId, demoAccount);

    const token = jwt.sign({ userId, email }, JWT_SECRET, { expiresIn: '7d' });

    res.json({
      success: true,
      token,
      user: { id: userId, firstName, lastName, email, phone, country, kycStatus: 'pending' },
      accounts: [account, demoAccount],
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Registration failed' });
  }
});

// Login
app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    const user = [...db.users.values()].find(u => u.email === email);
    if (!user) return res.status(401).json({ error: 'Invalid credentials' });

    const valid = await bcrypt.compare(password, user.password);
    if (!valid) return res.status(401).json({ error: 'Invalid credentials' });

    const accounts = [...db.accounts.values()].filter(a => a.userId === user.id);
    const token = jwt.sign({ userId: user.id, email }, JWT_SECRET, { expiresIn: '7d' });

    const { password: _, ...safeUser } = user;
    res.json({ success: true, token, user: safeUser, accounts });
  } catch (err) {
    res.status(500).json({ error: 'Login failed' });
  }
});

// Get profile
app.get('/api/auth/me', authMiddleware, (req, res) => {
  const user = db.users.get(req.user.userId);
  if (!user) return res.status(404).json({ error: 'User not found' });
  const { password: _, ...safeUser } = user;
  const accounts = [...db.accounts.values()].filter(a => a.userId === user.id);
  res.json({ user: safeUser, accounts });
});

// ══════════════════════════════════════════════════════════════
// ACCOUNT ROUTES
// ══════════════════════════════════════════════════════════════

app.get('/api/accounts', authMiddleware, (req, res) => {
  const accounts = [...db.accounts.values()].filter(a => a.userId === req.user.userId);
  res.json({ accounts });
});

app.get('/api/accounts/:id', authMiddleware, (req, res) => {
  const account = db.accounts.get(req.params.id);
  if (!account || account.userId !== req.user.userId)
    return res.status(404).json({ error: 'Account not found' });
  res.json({ account });
});

// ══════════════════════════════════════════════════════════════
// DEPOSIT — STRIPE
// ══════════════════════════════════════════════════════════════

// Create Stripe Payment Intent
app.post('/api/deposits/stripe/intent', authMiddleware, async (req, res) => {
  try {
    const { amount, currency = 'usd', accountId } = req.body;
    if (!amount || amount < 10) return res.status(400).json({ error: 'Minimum deposit is $10' });

    const account = db.accounts.get(accountId);
    if (!account || account.userId !== req.user.userId)
      return res.status(404).json({ error: 'Account not found' });

    const paymentIntent = await stripe.paymentIntents.create({
      amount: Math.round(amount * 100), // cents
      currency: currency.toLowerCase(),
      metadata: {
        userId: req.user.userId,
        accountId,
        platform: 'velox',
      },
    });

    // Record pending deposit
    const depositId = crypto.randomUUID();
    db.deposits.set(depositId, {
      id: depositId,
      userId: req.user.userId,
      accountId,
      amount,
      currency: currency.toUpperCase(),
      method: 'card',
      status: 'pending',
      stripeIntentId: paymentIntent.id,
      createdAt: new Date().toISOString(),
    });

    res.json({
      clientSecret: paymentIntent.client_secret,
      depositId,
    });
  } catch (err) {
    console.error('Stripe error:', err);
    res.status(500).json({ error: 'Payment setup failed. Check Stripe API key.' });
  }
});

// Stripe Webhook — confirm deposit after payment
app.post('/api/webhooks/stripe', express.raw({ type: 'application/json' }), async (req, res) => {
  const sig = req.headers['stripe-signature'];
  let event;
  try {
    event = stripe.webhooks.constructEvent(req.body, sig, process.env.STRIPE_WEBHOOK_SECRET);
  } catch (err) {
    return res.status(400).json({ error: `Webhook error: ${err.message}` });
  }

  if (event.type === 'payment_intent.succeeded') {
    const pi = event.data.object;
    const { accountId, userId } = pi.metadata;
    const amount = pi.amount / 100;

    // Credit account
    const account = db.accounts.get(accountId);
    if (account) {
      account.balance += amount;
      account.equity  += amount;
      account.freeMargin += amount;
    }

    // Update deposit record
    const deposit = [...db.deposits.values()].find(d => d.stripeIntentId === pi.id);
    if (deposit) deposit.status = 'completed';

    // Broadcast balance update to connected user
    broadcastToUser(userId, { type: 'BALANCE_UPDATE', accountId, balance: account?.balance });
  }

  res.json({ received: true });
});

// Manual deposit confirm (for demo/testing without real Stripe)
app.post('/api/deposits/confirm', authMiddleware, async (req, res) => {
  try {
    const { accountId, amount, method = 'demo' } = req.body;
    if (amount < 10) return res.status(400).json({ error: 'Minimum $10' });

    const account = db.accounts.get(accountId);
    if (!account || account.userId !== req.user.userId)
      return res.status(404).json({ error: 'Account not found' });

    account.balance    += parseFloat(amount);
    account.equity     += parseFloat(amount);
    account.freeMargin += parseFloat(amount);

    const txId = crypto.randomUUID();
    db.transactions.set(txId, {
      id: txId, userId: req.user.userId, accountId,
      type: 'deposit', amount, method, status: 'completed',
      createdAt: new Date().toISOString(),
    });

    broadcastToUser(req.user.userId, { type: 'BALANCE_UPDATE', account });
    res.json({ success: true, account, txId });
  } catch (err) {
    res.status(500).json({ error: 'Deposit failed' });
  }
});

// ══════════════════════════════════════════════════════════════
// WITHDRAWAL ROUTES
// ══════════════════════════════════════════════════════════════

app.post('/api/withdrawals/request', authMiddleware, async (req, res) => {
  try {
    const { accountId, amount, method, bankDetails } = req.body;

    const account = db.accounts.get(accountId);
    if (!account || account.userId !== req.user.userId)
      return res.status(404).json({ error: 'Account not found' });

    if (amount < 10) return res.status(400).json({ error: 'Minimum withdrawal is $10' });
    if (amount > account.balance) return res.status(400).json({ error: 'Insufficient balance' });
    if (account.margin > 0 && amount > account.freeMargin)
      return res.status(400).json({ error: 'Cannot withdraw funds used as margin' });

    // Reserve funds
    account.balance    -= parseFloat(amount);
    account.equity     -= parseFloat(amount);
    account.freeMargin -= parseFloat(amount);

    const wdId = crypto.randomUUID();
    db.transactions.set(wdId, {
      id: wdId, userId: req.user.userId, accountId,
      type: 'withdrawal', amount, method,
      bankDetails: JSON.stringify(bankDetails || {}),
      status: 'processing',
      createdAt: new Date().toISOString(),
      estimatedArrival: method === 'bank' ? '1-3 business days' : 'Within 24 hours',
    });

    broadcastToUser(req.user.userId, { type: 'BALANCE_UPDATE', account });
    res.json({
      success: true,
      message: 'Withdrawal request submitted. Processing within 24 hours.',
      withdrawalId: wdId,
      account,
    });
  } catch (err) {
    res.status(500).json({ error: 'Withdrawal failed' });
  }
});

// ══════════════════════════════════════════════════════════════
// TRADING ROUTES
// ══════════════════════════════════════════════════════════════

// Open trade
app.post('/api/trades/open', authMiddleware, async (req, res) => {
  try {
    const { accountId, symbol, type, lots, sl, tp } = req.body;

    const account = db.accounts.get(accountId);
    if (!account || account.userId !== req.user.userId)
      return res.status(404).json({ error: 'Account not found' });

    // Get current market price (simulated)
    const price = simulatedPrices[symbol] || 1.0;
    const pipValue = 10 * lots;
    const requiredMargin = price * lots * 100000 / account.leverage;

    if (requiredMargin > account.freeMargin)
      return res.status(400).json({ error: 'Insufficient free margin' });

    // Lock margin
    account.margin     += requiredMargin;
    account.freeMargin -= requiredMargin;

    const tradeId = crypto.randomUUID();
    const trade = {
      id: tradeId, userId: req.user.userId, accountId,
      symbol, type, lots,
      openPrice: type === 'buy' ? price * 1.00005 : price * 0.99995,
      currentPrice: price,
      sl: sl || null, tp: tp || null,
      margin: requiredMargin,
      swap: 0, commission: lots * 3.5,
      profit: 0, status: 'open',
      openTime: new Date().toISOString(),
    };
    db.trades.set(tradeId, trade);

    // Deduct commission from balance
    account.balance -= trade.commission;

    broadcastToUser(req.user.userId, { type: 'TRADE_OPENED', trade, account });
    res.json({ success: true, trade, account });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Trade failed' });
  }
});

// Close trade
app.post('/api/trades/close/:id', authMiddleware, async (req, res) => {
  try {
    const trade = db.trades.get(req.params.id);
    if (!trade || trade.userId !== req.user.userId)
      return res.status(404).json({ error: 'Trade not found' });

    const account = db.accounts.get(trade.accountId);
    const closePrice = simulatedPrices[trade.symbol] || trade.openPrice;

    const priceDiff = trade.type === 'buy'
      ? closePrice - trade.openPrice
      : trade.openPrice - closePrice;

    const profit = priceDiff * trade.lots * 100000;

    // Release margin + credit profit
    account.margin     -= trade.margin;
    account.freeMargin += trade.margin + profit;
    account.balance    += profit;
    account.equity      = account.balance + calcOpenPL(account.id);

    trade.status      = 'closed';
    trade.closePrice  = closePrice;
    trade.profit      = profit;
    trade.closeTime   = new Date().toISOString();

    broadcastToUser(req.user.userId, { type: 'TRADE_CLOSED', trade, account });
    res.json({ success: true, trade, account, profit });
  } catch (err) {
    res.status(500).json({ error: 'Close failed' });
  }
});

// Get open trades
app.get('/api/trades/open', authMiddleware, (req, res) => {
  const trades = [...db.trades.values()]
    .filter(t => t.userId === req.user.userId && t.status === 'open');
  res.json({ trades });
});

// Get trade history
app.get('/api/trades/history', authMiddleware, (req, res) => {
  const trades = [...db.trades.values()]
    .filter(t => t.userId === req.user.userId && t.status === 'closed')
    .sort((a,b) => new Date(b.closeTime) - new Date(a.closeTime))
    .slice(0, 100);
  res.json({ trades });
});

// Get transactions
app.get('/api/transactions', authMiddleware, (req, res) => {
  const txs = [...db.transactions.values()]
    .filter(t => t.userId === req.user.userId)
    .sort((a,b) => new Date(b.createdAt) - new Date(a.createdAt));
  res.json({ transactions: txs });
});

// ══════════════════════════════════════════════════════════════
// MARKET DATA
// ══════════════════════════════════════════════════════════════

const simulatedPrices = {
  'EURUSD': 1.08432, 'GBPUSD': 1.26784, 'USDJPY': 149.234,
  'USDCHF': 0.90124, 'AUDUSD': 0.65342, 'USDCAD': 1.36421,
  'NZDUSD': 0.59234, 'EURGBP': 0.85621, 'EURJPY': 161.82,
  'XAUUSD': 2314.50, 'XAGUSD': 27.34,   'WTIUSD': 78.42,
  'BTCUSD': 67842,   'ETHUSD': 3428.10,  'SOLUSD': 142.30,
  'BNBUSD': 432.10,  'XRPUSD': 0.5234,   'ADAUSD': 0.4521,
  'US30':   38420,   'SPX500': 5124,      'NAS100': 17834,
  'DAX40':  18234,   'FTSE100': 7834,     'AAPL':   192.62,
  'TSLA':   213.06,  'NVDA':   874.50,    'AMZN':   185.40,
};

// Simulate price movement
function tickPrices() {
  for (const [sym, price] of Object.entries(simulatedPrices)) {
    const volatility = sym.includes('BTC') ? 0.003 : sym.includes('ETH') ? 0.0025 :
                       sym.includes('XAU') ? 0.002 : sym === 'US30' ? 0.001 : 0.0005;
    simulatedPrices[sym] = price * (1 + (Math.random() - 0.499) * volatility);
  }
  // Broadcast to all connected clients
  const data = JSON.stringify({ type: 'PRICES', prices: simulatedPrices, ts: Date.now() });
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) client.send(data);
  });
}
setInterval(tickPrices, 500); // 500ms tick

app.get('/api/markets/prices', (req, res) => {
  res.json({ prices: simulatedPrices, timestamp: Date.now() });
});

app.get('/api/markets/symbols', (req, res) => {
  const symbols = [
    { symbol:'EURUSD', name:'Euro / US Dollar', category:'forex', pip:0.00001 },
    { symbol:'GBPUSD', name:'British Pound / US Dollar', category:'forex', pip:0.00001 },
    { symbol:'USDJPY', name:'US Dollar / Japanese Yen', category:'forex', pip:0.001 },
    { symbol:'XAUUSD', name:'Gold / US Dollar', category:'commodities', pip:0.01 },
    { symbol:'XAGUSD', name:'Silver / US Dollar', category:'commodities', pip:0.001 },
    { symbol:'WTIUSD', name:'WTI Crude Oil', category:'commodities', pip:0.01 },
    { symbol:'BTCUSD', name:'Bitcoin / US Dollar', category:'crypto', pip:1 },
    { symbol:'ETHUSD', name:'Ethereum / US Dollar', category:'crypto', pip:0.01 },
    { symbol:'SOLUSD', name:'Solana / US Dollar', category:'crypto', pip:0.01 },
    { symbol:'US30',   name:'Dow Jones 30', category:'indices', pip:1 },
    { symbol:'SPX500', name:'S&P 500', category:'indices', pip:0.1 },
    { symbol:'NAS100', name:'Nasdaq 100', category:'indices', pip:0.1 },
    { symbol:'AAPL',   name:'Apple Inc.', category:'stocks', pip:0.01 },
    { symbol:'TSLA',   name:'Tesla Inc.', category:'stocks', pip:0.01 },
    { symbol:'NVDA',   name:'NVIDIA Corp.', category:'stocks', pip:0.01 },
  ];
  res.json({ symbols });
});

// OHLCV candle data (simulated history)
app.get('/api/markets/candles/:symbol', (req, res) => {
  const { symbol } = req.params;
  const { timeframe = '1H', count = 200 } = req.query;
  const basePrice = simulatedPrices[symbol] || 1.0;
  const candles = [];
  let price = basePrice * 0.95;
  const now = Date.now();
  const tfMs = { '1M':60000,'5M':300000,'15M':900000,'1H':3600000,'4H':14400000,'1D':86400000 };
  const interval = tfMs[timeframe] || 3600000;

  for (let i = parseInt(count); i >= 0; i--) {
    const open  = price;
    const high  = open * (1 + Math.random() * 0.008);
    const low   = open * (1 - Math.random() * 0.008);
    const close = low + Math.random() * (high - low);
    const vol   = Math.floor(Math.random() * 50000 + 10000);
    candles.push({ time: Math.floor((now - i * interval) / 1000), open, high, low, close, volume: vol });
    price = close;
  }
  res.json({ symbol, timeframe, candles });
});

// ── UTILITIES ────────────────────────────────────────────────
const userSockets = new Map(); // userId -> Set of WebSocket clients

function broadcastToUser(userId, data) {
  const clients = userSockets.get(userId);
  if (!clients) return;
  const msg = JSON.stringify(data);
  clients.forEach(ws => { if (ws.readyState === WebSocket.OPEN) ws.send(msg); });
}

function calcOpenPL(accountId) {
  let pl = 0;
  [...db.trades.values()].filter(t => t.accountId === accountId && t.status === 'open').forEach(t => {
    const cp = simulatedPrices[t.symbol] || t.openPrice;
    const diff = t.type === 'buy' ? cp - t.openPrice : t.openPrice - cp;
    pl += diff * t.lots * 100000;
  });
  return pl;
}

// ── WEBSOCKET ────────────────────────────────────────────────
wss.on('connection', (ws, req) => {
  console.log('WS client connected');
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
          ws.send(JSON.stringify({ type: 'AUTH_OK', userId }));
        } catch { ws.send(JSON.stringify({ type: 'AUTH_FAIL' })); }
      }
    } catch {}
  });

  ws.on('close', () => {
    if (userId && userSockets.has(userId)) {
      userSockets.get(userId).delete(ws);
    }
  });

  // Send initial prices
  ws.send(JSON.stringify({ type: 'PRICES', prices: simulatedPrices, ts: Date.now() }));
});

// ── START ────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`\n🚀 VELOX API Server running`);
  console.log(`   WebSocket: ws://localhost:${PORT}`);
  console.log(`   API:       http://localhost:${PORT}/api`);
  console.log(`   Frontend:  http://localhost:${PORT}\n`);
});

module.exports = app;
