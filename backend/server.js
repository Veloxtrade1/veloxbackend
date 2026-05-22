/**
 * VELOX BACKEND v3 - Render.com optimized
 * Handles CORS properly for all origins
 */
require('dotenv').config();
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');
const https = require('https');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 4000;
const JWT_SECRET = process.env.JWT_SECRET || 'velox_change_in_production_' + crypto.randomBytes(16).toString('hex');

// ── CORS — MUST be first middleware, handles ALL origins ──────
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', req.headers.origin || '*');
  res.setHeader('Access-Control-Allow-Credentials', 'true');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS,PATCH');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization,Accept,Origin,X-Requested-With');
  res.setHeader('Access-Control-Max-Age', '86400');
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }
  next();
});

app.use(express.json({ limit: '10mb' }));

// ── MongoDB (optional) ────────────────────────────────────────
let db_connected = false;
let User, Account, Trade, Transaction;
const mem = { users: new Map(), accounts: new Map(), trades: new Map(), txs: new Map() };

async function tryMongo() {
  const uri = process.env.MONGODB_URI;
  if (!uri) { console.log('⚡ No MONGODB_URI — using memory storage'); return; }
  try {
    const mongoose = require('mongoose');
    await mongoose.connect(uri, { serverSelectionTimeoutMS: 5000 });

    User = mongoose.model('User', new mongoose.Schema({
      firstName:String, lastName:String, email:{type:String,unique:true,lowercase:true},
      password:String, phone:String, country:String,
      kycStatus:{type:String,default:'pending'}, createdAt:{type:Date,default:Date.now}
    }));
    Account = mongoose.model('Account', new mongoose.Schema({
      userId:mongoose.Schema.Types.ObjectId, accountNumber:String, currency:{type:String,default:'USD'},
      balance:{type:Number,default:0}, equity:{type:Number,default:0},
      margin:{type:Number,default:0}, freeMargin:{type:Number,default:0},
      leverage:{type:Number,default:500}, type:{type:String,default:'standard'},
      isDemo:{type:Boolean,default:false}, createdAt:{type:Date,default:Date.now}
    }));
    Trade = mongoose.model('Trade', new mongoose.Schema({
      userId:mongoose.Schema.Types.ObjectId, accountId:mongoose.Schema.Types.ObjectId,
      symbol:String, type:String, lots:Number, openPrice:Number, closePrice:Number,
      sl:Number, tp:Number, margin:Number, commission:Number,
      swap:{type:Number,default:0}, profit:{type:Number,default:0},
      status:{type:String,default:'open'}, openTime:{type:Date,default:Date.now}, closeTime:Date
    }));
    Transaction = mongoose.model('Transaction', new mongoose.Schema({
      userId:mongoose.Schema.Types.ObjectId, accountId:mongoose.Schema.Types.ObjectId,
      type:String, amount:Number, method:String, status:String,
      createdAt:{type:Date,default:Date.now}
    }));

    db_connected = true;
    console.log('✅ MongoDB connected');
  } catch(e) {
    console.log('⚡ MongoDB failed, using memory:', e.message);
  }
}

// ── DB helpers ────────────────────────────────────────────────
const DB = {
  async findUserByEmail(email) {
    if (db_connected) return await User.findOne({ email: email.toLowerCase() });
    return [...mem.users.values()].find(u => u.email === email.toLowerCase());
  },
  async createUser(data) {
    if (db_connected) { const u = new User(data); await u.save(); return u; }
    const u = { _id: crypto.randomUUID(), ...data };
    mem.users.set(u._id, u); return u;
  },
  async createAccount(data) {
    if (db_connected) { const a = new Account(data); await a.save(); return a; }
    const a = { _id: crypto.randomUUID(), ...data };
    mem.accounts.set(a._id, a); return a;
  },
  async findAccountsByUser(userId) {
    if (db_connected) return await Account.find({ userId });
    return [...mem.accounts.values()].filter(a => String(a.userId) === String(userId));
  },
  async findAccountById(id) {
    if (db_connected) return await Account.findById(id);
    return mem.accounts.get(String(id));
  },
  async updateAccount(id, data) {
    if (db_connected) return await Account.findByIdAndUpdate(id, data, { new: true });
    const a = mem.accounts.get(String(id));
    if (a) { Object.assign(a, data); return a; }
  },
  async createTrade(data) {
    if (db_connected) { const t = new Trade(data); await t.save(); return t; }
    const t = { _id: crypto.randomUUID(), ...data };
    mem.trades.set(t._id, t); return t;
  },
  async findOpenTrades(userId) {
    if (db_connected) return await Trade.find({ userId, status:'open' });
    return [...mem.trades.values()].filter(t => String(t.userId) === String(userId) && t.status === 'open');
  },
  async findTradeById(id) {
    if (db_connected) return await Trade.findById(id);
    return mem.trades.get(String(id));
  },
  async closeTrade(id, data) {
    if (db_connected) return await Trade.findByIdAndUpdate(id, data, { new: true });
    const t = mem.trades.get(String(id));
    if (t) { Object.assign(t, data); return t; }
  },
  async findTradeHistory(userId) {
    if (db_connected) return await Trade.find({ userId, status:'closed' }).sort({ closeTime:-1 }).limit(100);
    return [...mem.trades.values()].filter(t => String(t.userId) === String(userId) && t.status === 'closed').reverse().slice(0,100);
  },
  async createTx(data) {
    if (db_connected) { const t = new Transaction(data); await t.save(); return t; }
    const t = { _id: crypto.randomUUID(), ...data };
    mem.txs.set(t._id, t); return t;
  },
  async findTxs(userId) {
    if (db_connected) return await Transaction.find({ userId }).sort({ createdAt:-1 });
    return [...mem.txs.values()].filter(t => String(t.userId) === String(userId)).reverse();
  }
};

// ── PRICES + ORDER BOOK ───────────────────────────────────────
const prices = {
  EURUSD:1.0843,GBPUSD:1.2678,USDJPY:149.23,USDCHF:0.9012,AUDUSD:0.6534,
  USDCAD:1.3642,NZDUSD:0.5923,EURGBP:0.8562,EURJPY:161.82,GBPJPY:189.40,
  XAUUSD:2314.5,XAGUSD:27.34,WTIUSD:78.42,
  BTCUSD:67842,ETHUSD:3428.1,SOLUSD:142.3,BNBUSD:432.1,XRPUSD:0.5234,
  US30:38420,SPX500:5124,NAS100:17834,DAX40:18234,FTSE100:7834,
  AAPL:192.62,TSLA:213.06,NVDA:874.5,AMZN:185.4,MSFT:415.3,GOOGL:174.2,META:493.5,
};
const VOL = { BTCUSD:0.003,ETHUSD:0.003,SOLUSD:0.004,XAUUSD:0.0015,WTIUSD:0.002,
  US30:0.001,SPX500:0.001,NAS100:0.0012,EURUSD:0.0004,GBPUSD:0.0005,USDJPY:0.0004 };
const momentum = {};
Object.keys(prices).forEach(s => momentum[s] = 0);

// Bot order book
const bots = ['AlgoBot_FX','QuantPro','MarketMaker','HFT_Delta','TrendBot','ScalpBot','GridTrader','NewsBot'];
const orderBooks = {};
function getOB(sym) {
  if (!orderBooks[sym]) {
    const mid = prices[sym]||1, sp = mid*0.0003;
    orderBooks[sym] = {
      bids: Array.from({length:8},(_,i)=>({price:parseFloat((mid-sp*(i+1)).toFixed(5)),size:parseFloat((Math.random()*2+0.1).toFixed(2)),bot:bots[i%bots.length]})),
      asks: Array.from({length:8},(_,i)=>({price:parseFloat((mid+sp*(i+1)).toFixed(5)),size:parseFloat((Math.random()*2+0.1).toFixed(2)),bot:bots[(i+4)%bots.length]})),
      trades:[], volume:Math.floor(Math.random()*1000000+200000)
    };
  }
  return orderBooks[sym];
}

// Alpha Vantage price refresh
const AV_KEY = process.env.ALPHA_VANTAGE_KEY;
function fetchAV(from, to, sym) {
  if (!AV_KEY || AV_KEY === 'demo') return;
  const path = `/query?function=CURRENCY_EXCHANGE_RATE&from_currency=${from}&to_currency=${to}&apikey=${AV_KEY}`;
  const req = https.get({hostname:'www.alphavantage.co',path,timeout:5000}, res => {
    let d=''; res.on('data',c=>d+=c);
    res.on('end',()=>{
      try {
        const rate = JSON.parse(d)['Realtime Currency Exchange Rate'];
        if (rate) { prices[sym] = parseFloat(rate['5. Exchange Rate']); }
      } catch {}
    });
  });
  req.on('error', function(e){ }); req.on('timeout', function(){ req.destroy(); });
}

// Refresh AV every 60s
setInterval(()=>{
  [['EUR','USD','EURUSD'],['GBP','USD','GBPUSD'],['XAU','USD','XAUUSD'],['BTC','USD','BTCUSD'],['USD','JPY','USDJPY']]
    .forEach(([f,t,s])=>fetchAV(f,t,s));
}, 60000);

// Price tick
setInterval(()=>{
  for (const sym in prices) {
    const v = VOL[sym]||0.0005;
    const noise = (Math.random()-.499)*v;
    momentum[sym] = momentum[sym]*0.9 + noise*0.1;
    prices[sym] *= 1 + momentum[sym];
  }
  // Update order books
  for (const sym in orderBooks) {
    const ob = orderBooks[sym]; const mid = prices[sym]; const sp = mid*0.0003;
    if (Math.random()<0.3 && ob.bids.length < 12)
      ob.bids.push({price:parseFloat((mid-sp*(Math.random()*5+1)).toFixed(5)),size:parseFloat((Math.random()*2+0.1).toFixed(2)),bot:bots[Math.floor(Math.random()*bots.length)]});
    if (Math.random()<0.3 && ob.asks.length < 12)
      ob.asks.push({price:parseFloat((mid+sp*(Math.random()*5+1)).toFixed(5)),size:parseFloat((Math.random()*2+0.1).toFixed(2)),bot:bots[Math.floor(Math.random()*bots.length)]});
    if (Math.random()<0.25 && ob.bids.length>3) { const f=ob.bids.splice(Math.floor(Math.random()*ob.bids.length),1)[0]; ob.trades.unshift({side:'buy',...f,time:Date.now()}); }
    if (Math.random()<0.25 && ob.asks.length>3) { const f=ob.asks.splice(Math.floor(Math.random()*ob.asks.length),1)[0]; ob.trades.unshift({side:'sell',...f,time:Date.now()}); }
    ob.bids.sort((a,b)=>b.price-a.price); ob.asks.sort((a,b)=>a.price-b.price);
    if (ob.trades.length>20) ob.trades.length=20;
  }
  const msg = JSON.stringify({type:'PRICES',prices,ts:Date.now()});
  wss.clients.forEach(c=>{ if(c.readyState===WebSocket.OPEN) c.send(msg); });
}, 500);

// ── AUTH ──────────────────────────────────────────────────────
function auth(req,res,next){
  const token = req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({error:'No token'});
  try { req.user = jwt.verify(token, JWT_SECRET); next(); }
  catch { res.status(401).json({error:'Invalid token'}); }
}

// ── ROUTES ────────────────────────────────────────────────────
app.get('/', (req,res) => res.json({
  status:'VELOX API Running', version:'3.0.0',
  db: db_connected?'MongoDB':'Memory',
  symbols: Object.keys(prices).length,
  timestamp: new Date().toISOString()
}));
app.get('/health', (req,res) => res.json({healthy:true,uptime:process.uptime()}));

// Register
app.post('/api/auth/register', async (req,res) => {
  try {
    const {firstName,lastName,email,password,phone,country,currency='USD'} = req.body;
    if (!firstName||!email||!password) return res.status(400).json({error:'Required fields missing'});
    if (password.length<8) return res.status(400).json({error:'Password min 8 characters'});
    if (await DB.findUserByEmail(email)) return res.status(409).json({error:'Email already registered'});

    const pw = await bcrypt.hash(password,12);
    const uid = crypto.randomUUID();
    const user = await DB.createUser({_id:uid,firstName,lastName,email:email.toLowerCase(),password:pw,phone,country,kycStatus:'pending'});
    const liveAcct = await DB.createAccount({userId:uid,currency,balance:0,equity:0,margin:0,freeMargin:0,leverage:500,type:'standard',isDemo:false,accountNumber:`VLX${Math.floor(100000+Math.random()*900000)}`});
    const demoAcct = await DB.createAccount({userId:uid,currency,balance:10000,equity:10000,margin:0,freeMargin:10000,leverage:500,type:'demo',isDemo:true,accountNumber:`DEMO${Math.floor(100000+Math.random()*900000)}`});

    const token = jwt.sign({userId:uid,email:email.toLowerCase()},JWT_SECRET,{expiresIn:'7d'});
    const {password:_,...safeUser} = user.toObject?user.toObject():user;
    res.json({success:true,token,user:safeUser,accounts:[liveAcct,demoAcct]});
  } catch(e) { console.error('Register error:',e); res.status(500).json({error:'Registration failed: '+e.message}); }
});

// Login
app.post('/api/auth/login', async (req,res) => {
  try {
    const {email,password} = req.body;
    const user = await DB.findUserByEmail(email);
    if (!user) return res.status(401).json({error:'Invalid credentials'});
    const valid = await bcrypt.compare(password, user.password);
    if (!valid) return res.status(401).json({error:'Invalid credentials'});
    const accounts = await DB.findAccountsByUser(user._id);
    const token = jwt.sign({userId:String(user._id),email:user.email},JWT_SECRET,{expiresIn:'7d'});
    const {password:_,...safeUser} = user.toObject?user.toObject():user;
    res.json({success:true,token,user:safeUser,accounts});
  } catch(e) { res.status(500).json({error:'Login failed'}); }
});

// Profile
app.get('/api/auth/me', auth, async (req,res) => {
  try {
    const user = await DB.findUserByEmail(req.user.email);
    if (!user) return res.status(404).json({error:'Not found'});
    const accounts = await DB.findAccountsByUser(user._id);
    const {password:_,...safeUser} = user.toObject?user.toObject():user;
    res.json({user:safeUser,accounts});
  } catch(e) { res.status(500).json({error:'Error'}); }
});

// Accounts
app.get('/api/accounts', auth, async (req,res) => {
  const accounts = await DB.findAccountsByUser(req.user.userId);
  res.json({accounts});
});

// Deposit
app.post('/api/deposits/confirm', auth, async (req,res) => {
  try {
    const {accountId,amount,method='card'} = req.body;
    const a = await DB.findAccountById(accountId);
    if (!a) return res.status(404).json({error:'Account not found'});
    const nb = (a.balance||0)+parseFloat(amount);
    const upd = await DB.updateAccount(accountId,{balance:nb,equity:nb,freeMargin:nb-(a.margin||0)});
    await DB.createTx({userId:a.userId,accountId,type:'deposit',amount,method,status:'completed'});
    broadcastToUser(String(a.userId),{type:'BALANCE_UPDATE',account:upd});
    res.json({success:true,account:upd});
  } catch(e) { res.status(500).json({error:'Deposit failed'}); }
});

// Withdrawal
app.post('/api/withdrawals/request', auth, async (req,res) => {
  try {
    const {accountId,amount,method,bankDetails} = req.body;
    const a = await DB.findAccountById(accountId);
    if (!a) return res.status(404).json({error:'Account not found'});
    if (amount>a.freeMargin) return res.status(400).json({error:'Insufficient free margin'});
    const nb = a.balance-parseFloat(amount);
    const upd = await DB.updateAccount(accountId,{balance:nb,equity:nb,freeMargin:nb-(a.margin||0)});
    await DB.createTx({userId:a.userId,accountId,type:'withdrawal',amount,method,status:'processing'});
    broadcastToUser(String(a.userId),{type:'BALANCE_UPDATE',account:upd});
    res.json({success:true,account:upd});
  } catch(e) { res.status(500).json({error:'Withdrawal failed'}); }
});

// Trades
app.post('/api/trades/open', auth, async (req,res) => {
  try {
    const {accountId,symbol,type,lots,sl,tp} = req.body;
    const a = await DB.findAccountById(accountId);
    if (!a) return res.status(404).json({error:'Account not found'});
    const price = prices[symbol]||1;
    const spread = price*0.00005;
    const openPrice = type==='buy'?price+spread:price-spread;
    const margin = (price*lots*100000)/a.leverage;
    const commission = lots*3.5;
    if (margin>a.freeMargin) return res.status(400).json({error:'Insufficient margin'});
    const upd = await DB.updateAccount(accountId,{margin:(a.margin||0)+margin,balance:a.balance-commission,freeMargin:a.equity-((a.margin||0)+margin)});
    const trade = await DB.createTrade({userId:a.userId,accountId,symbol,type,lots,openPrice:parseFloat(openPrice.toFixed(5)),margin,commission,sl:sl||null,tp:tp||null,profit:0});
    broadcastToUser(String(a.userId),{type:'TRADE_OPENED',trade,account:upd});
    res.json({success:true,trade,account:upd});
  } catch(e) { console.error(e); res.status(500).json({error:'Trade failed'}); }
});

app.post('/api/trades/close/:id', auth, async (req,res) => {
  try {
    const trade = await DB.findTradeById(req.params.id);
    if (!trade) return res.status(404).json({error:'Not found'});
    const closePrice = prices[trade.symbol]||trade.openPrice;
    const diff = trade.type==='buy'?closePrice-trade.openPrice:trade.openPrice-closePrice;
    const profit = diff*trade.lots*100000;
    const a = await DB.findAccountById(trade.accountId);
    const nm = Math.max(0,(a.margin||0)-trade.margin);
    const nb = a.balance+profit;
    const upd = await DB.updateAccount(trade.accountId,{margin:nm,balance:nb,equity:nb,freeMargin:nb-nm});
    const closed = await DB.closeTrade(trade._id||trade.id,{status:'closed',closePrice,profit,closeTime:new Date()});
    broadcastToUser(String(a.userId),{type:'TRADE_CLOSED',trade:closed,account:upd,profit});
    res.json({success:true,trade:closed,account:upd,profit});
  } catch(e) { res.status(500).json({error:'Close failed'}); }
});

app.get('/api/trades/open', auth, async (req,res) => res.json({trades:await DB.findOpenTrades(req.user.userId)}));
app.get('/api/trades/history', auth, async (req,res) => res.json({trades:await DB.findTradeHistory(req.user.userId)}));
app.get('/api/transactions', auth, async (req,res) => res.json({transactions:await DB.findTxs(req.user.userId)}));

// Market data
app.get('/api/markets/prices', (req,res) => res.json({prices,timestamp:Date.now()}));
app.get('/api/markets/orderbook/:symbol', (req,res) => res.json({symbol:req.params.symbol,orderBook:getOB(req.params.symbol),timestamp:Date.now()}));
app.get('/api/markets/candles/:symbol', (req,res) => {
  const {symbol} = req.params; const {timeframe='60',count='200'} = req.query;
  const base=prices[symbol]||1; const candles=[]; let p=base*0.97;
  const now=Math.floor(Date.now()/1000); const interval=parseInt(timeframe)*60;
  for (let i=parseInt(count);i>=0;i--) {
    const v=VOL[symbol]||0.0005;
    const o=p,h=o*(1+Math.random()*v*3),l=o*(1-Math.random()*v*3),c=l+Math.random()*(h-l);
    candles.push({time:now-i*interval,open:parseFloat(o.toFixed(5)),high:parseFloat(h.toFixed(5)),low:parseFloat(l.toFixed(5)),close:parseFloat(c.toFixed(5)),volume:Math.floor(Math.random()*50000+10000)});
    p=c;
  }
  res.json({symbol,timeframe,candles});
});

// ── WEBSOCKET ─────────────────────────────────────────────────
const userSockets = new Map();
function broadcastToUser(userId, data) {
  const clients = userSockets.get(userId);
  if (!clients) return;
  const msg = JSON.stringify(data);
  clients.forEach(ws=>{ if(ws.readyState===WebSocket.OPEN) ws.send(msg); });
}

wss.on('connection', (ws) => {
  let userId = null;
  ws.on('message', msg => {
    try {
      const d = JSON.parse(msg);
      if (d.type==='AUTH') {
        try {
          const dec = jwt.verify(d.token, JWT_SECRET);
          userId = dec.userId;
          if (!userSockets.has(userId)) userSockets.set(userId, new Set());
          userSockets.get(userId).add(ws);
          ws.send(JSON.stringify({type:'AUTH_OK',userId}));
        } catch { ws.send(JSON.stringify({type:'AUTH_FAIL'})); }
      }
    } catch {}
  });
  ws.on('close', () => {
    if (userId && userSockets.has(userId)) {
      userSockets.get(userId).delete(ws);
      if (!userSockets.get(userId).size) userSockets.delete(userId);
    }
  });
  ws.send(JSON.stringify({type:'PRICES',prices,ts:Date.now()}));
});

// ── START ─────────────────────────────────────────────────────
tryMongo().then(() => {
  server.listen(PORT, () => {
    console.log(`\n🚀 VELOX API v3 — port ${PORT}`);
    console.log(`   DB: ${db_connected?'✅ MongoDB':'⚡ Memory'}`);
    console.log(`   AV: ${AV_KEY?'✅ Live prices':'⚡ Simulation'}`);
    console.log(`   Symbols: ${Object.keys(prices).length}\n`);
  });
});
