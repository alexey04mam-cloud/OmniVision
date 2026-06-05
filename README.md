# OmniVision — Professional Crypto Analytics Platform

> All-in-one crypto market intelligence dashboard with 32 tools, AI assistant, Telegram bot, and built-in monetization. Ready to deploy and earn.

**Live Demo:** [dependable-tranquility-production-d86f.up.railway.app](https://dependable-tranquility-production-d86f.up.railway.app)

---

## Why This Is Worth $5,000+

This isn't a template or a tutorial project. OmniVision is a **production-ready SaaS platform** with 11,000+ lines of hand-written code, 100+ API endpoints, a working payment system, and a Telegram bot — all built to generate revenue from day one.

**Comparable platforms charge $29-99/month** for similar features. With just 50 paying users at $9.99/month, this generates **$6,000/year** in recurring revenue.

---

## What You Get

### 32 Professional Tools (each tab in the dashboard)

| Category | Tools |
|----------|-------|
| **Scanners** | Multi-market radar (crypto/stocks/commodities), Auto-hunter with intervals |
| **Analytics** | Deep Analytics (5 timeframes), Market Screener (250 coins, filters, column sorting), Technical Indicators (RSI, SMA, EMA, MACD with BUY/SELL/HOLD signals) |
| **Unique Tools** | Liquidation Map (leverage zones 2x-125x), Correlation Matrix (Pearson, 12 coins), DCA Calculator (historical backtest vs lump sum), Token Unlocks tracker |
| **Market Data** | Heatmap (top 50 treemap), Market Dominance (donut chart), Funding Rates, Gas Tracker (7 chains), Fear & Greed Index |
| **Social** | Trending Coins + NFTs, Social Sentiment (Twitter/Reddit/Telegram metrics), Whale Alerts |
| **DeFi** | On-Chain Analytics (DeFiLlama TVL, protocols, stablecoins), DEX Converter (DexScreener), Exchange Aggregator (BestChange) |
| **Trading** | Token Compare (side-by-side), Risk Calculator, Crypto-Fiat Converter (15 coins x 6 fiat), Multi-Chart (2/4 layout, candlestick/line toggle) |
| **Portfolio** | Portfolio tracker with P&L, Watchlist with groups, Price Alerts via Telegram, CSV Export |
| **AI** | AI Chat Assistant (Groq LLaMA 3.3 70B + OpenRouter fallback + rule-based fallback) |

### Built-In Monetization

- **3-tier subscription:** Free / Pro ($9.99/mo) / VIP ($29.99/mo)
- **Payment processing:** CryptoBot (Visa/Mastercard/Apple Pay/Google Pay/crypto) + Telegram Stars
- **Webhook auto-confirmation** — payment comes in, tier activates automatically
- **Freemium enforcement** — AI limits, export gates, feature locks with upgrade prompts
- **Referral system** — affiliate links for Bybit, BingX, OKX, Binance (passive income)

### Telegram Bot (@omnivision_alerts_bot)

- Price alerts with push notifications
- Market signals (auto-generated BUY/SELL)
- Whale tracking alerts
- `/digest` — one-command market summary
- `/post` — auto-publish to Telegram channel
- Premium subscription management
- Stars payment integration

### UX That Converts

- **Dark/Light theme** with localStorage persistence
- **Ctrl+K Command Palette** — instant search across all 32 tools
- **Searchable coin picker** — 100 coins with images, ranks, keyboard navigation
- **Mobile-first** — bottom navigation, PWA install prompt, touch-optimized
- **Onboarding** — first-time user tooltip
- **3 languages** — Ukrainian, English, Russian

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Backend | Python 3.11, FastAPI, SQLAlchemy ORM, SQLite |
| Frontend | Vanilla HTML/CSS/JS (no framework dependencies), Chart.js, lightweight-charts |
| AI | Groq API (LLaMA 3.3 70B), OpenRouter fallback |
| Bot | Python, httpx, Telegram Bot API |
| Payments | CryptoBot Crypto Pay API, Telegram Stars |
| APIs | CoinGecko, DexScreener, BestChange, Alternative.me, DeFiLlama |
| Deploy | Railway (one-click), Docker-ready |

**Zero framework lock-in.** No React, no Next.js, no npm dependencies on the frontend. The entire dashboard is a single HTML file — fast, portable, easy to customize.

---

## Deployment (5 minutes)

```bash
# 1. Clone
git clone https://github.com/YOUR_USERNAME/OmniVision.git
cd OmniVision

# 2. Install
pip install -r requirements.txt

# 3. Set environment variables
cp .env.example .env
# Edit .env with your API keys

# 4. Run
uvicorn main:app --reload --port 8000
```

### Railway (recommended)
1. Fork this repo
2. Connect to Railway
3. Add environment variables
4. Auto-deploys on every push

### Docker
```bash
docker build -t omnivision .
docker run -p 8000:8000 --env-file .env omnivision
```

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SECRET_KEY` | Yes | Session encryption key |
| `GROQ_API_KEY` | Yes | AI chat (free at groq.com) |
| `TELEGRAM_BOT_TOKEN` | Yes | From @BotFather |
| `ADMIN_CHAT_ID` | Yes | Your Telegram user ID |
| `CRYPTOBOT_TOKEN` | For payments | From @CryptoBot Crypto Pay |
| `OPENROUTER_API_KEY` | Optional | AI fallback |
| `TELEGRAM_CHANNEL_ID` | Optional | Auto-posting channel |
| `REF_BYBIT` | Optional | Referral link |

---

## Revenue Streams

1. **Subscriptions** — Pro $9.99/mo, VIP $29.99/mo (CryptoBot + Stars)
2. **Referral commissions** — Bybit, BingX, OKX, Binance affiliate links
3. **Telegram channel** — monetize audience with ads/signals
4. **White-label** — resell customized versions

---

## Code Quality

- **11,296 lines** of production code (main.py + dashboard.html + telegram_bot.py)
- **101 API endpoints** covering every feature
- **34 UI panels** — each with loading states, error handling, responsive design
- **0 npm dependencies** on frontend — no build step, no node_modules
- **Validated** — all JS passes `node --check`, all Python passes `py_compile`

---

## What Makes This Different From Free Templates

| Feature | Free Templates | OmniVision |
|---------|---------------|------------|
| Working payment system | No | CryptoBot + Stars, auto-confirm |
| AI assistant | No | Groq + OpenRouter + fallback |
| Telegram bot | No | Full bot with alerts, signals, payments |
| Revenue from day 1 | No | Subscriptions + referrals |
| 32 unique tools | 3-5 basic pages | Every tool built and working |
| Mobile PWA | No | Install prompt, bottom nav |
| Freemium enforcement | No | Real tier gates + upgrade prompts |

---

## License

Private / Commercial. Full source code included. No recurring fees. Deploy anywhere.

---

*Built with FastAPI, Chart.js, lightweight-charts, and CoinGecko API.*
