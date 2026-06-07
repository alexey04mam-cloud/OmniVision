# OmniVision — Professional Crypto Analytics Platform

> All-in-one crypto market intelligence dashboard with **35 tools**, AI-powered smart alerts, Telegram bot, and built-in monetization. Production-ready SaaS — deploy and earn.

**Live Demo:** [dependable-tranquility-production-d86f.up.railway.app](https://dependable-tranquility-production-d86f.up.railway.app)

---

## Why Buy This

This isn't a template or tutorial project. OmniVision is a **production-ready SaaS platform** with 14,000+ lines of hand-written code, 110+ API endpoints, a working payment system, Telegram bot, and AI integration — all built to generate revenue from day one.

**Comparable platforms charge $29–99/month** for similar features. With just 50 paying users at $9.99/month, this generates **$6,000/year** in recurring revenue.

**Asking price: $1,000** — a fraction of what it cost to build.

---

## What You Get

### 35 Professional Tools

| Category | Tools |
|----------|-------|
| **Scanners** | Multi-market radar (crypto/stocks/commodities), Auto-hunter with intervals |
| **Analytics** | Deep Analytics (5 timeframes), Market Screener (250 coins, filters, sparklines, sorting), Technical Indicators (RSI, SMA, EMA, MACD with BUY/SELL/HOLD) |
| **AI** | AI Chat Assistant (Groq LLaMA 3.3 70B + OpenRouter fallback), **AI Smart Alerts** (auto-detects volume spikes, breakouts, sentiment shifts, trend reversals) |
| **Unique Tools** | Liquidation Map (2x–125x leverage zones), Correlation Matrix (Pearson, 12 coins), DCA Calculator (backtest vs lump sum), Token Unlocks tracker |
| **Market Data** | Heatmap (top 50 treemap), Market Dominance (donut chart), Funding Rates (Binance real data), Gas Tracker (7 chains), Fear & Greed Index |
| **Social** | Trending Coins + NFTs, Social Sentiment (Twitter/Reddit/Telegram metrics), Whale Alerts (anomalous volume detection) |
| **DeFi** | On-Chain Analytics (DeFiLlama TVL, protocols, stablecoins), DEX Converter (DexScreener), Exchange Aggregator (BestChange) |
| **Trading** | Token Compare (side-by-side), Risk/Reward Calculator, Crypto-Fiat Converter (15 coins × 6 fiat), Multi-Chart (2/4 layout, candlestick/line) |
| **Portfolio** | Portfolio tracker with P&L + charts, Watchlist with groups, Price Alerts via Telegram, CSV Export |

### AI Smart Alerts (Unique Feature)

Background AI scans the market every 15 minutes and generates intelligent alerts:
- **Volume Spikes** — abnormal vol/mcap ratio detection
- **Price Breakouts** — >10% in 1h or >20% in 24h
- **Trend Reversals** — opposing 1h vs 7d movements
- **Sentiment Shifts** — extreme Fear & Greed (<15 or >85)
- **AI Analysis** — Groq generates human-readable explanation for each alert

No competitor offers this at this price point.

### Built-In Monetization

- **3-tier subscription:** Free / Pro ($9.99/mo) / VIP ($29.99/mo)
- **Payment processing:** CryptoBot (Visa/Mastercard/Apple Pay/Google Pay/crypto) + Telegram Stars
- **Webhook auto-confirmation** — payment arrives → tier activates instantly
- **Promo code system** — create, manage, track usage via admin panel
- **Freemium enforcement** — AI limits, export gates, feature locks with upgrade prompts
- **Referral system** — affiliate links for Bybit, BingX, OKX, Binance (passive income)

### Telegram Bot

- Price alerts with push notifications
- Market signals (auto-generated BUY/SELL)
- `/digest` — one-command market summary
- `/post` — auto-publish to channel
- **6 rotating auto-post types** every 6 hours (trending, movers, F&G, DeFi, promos)
- **Referral system** with tracking, notifications, bonus tiers (3/5/10 referrals)
- Premium subscription management via bot
- Stars payment integration

### UX That Converts

- **Real-time prices** — Binance WebSocket for 15 coins, flash animations
- **Dark/Light theme** with localStorage persistence
- **Ctrl+K Command Palette** — instant search across all 35 tools
- **Searchable coin picker** — 100 coins with images, ranks, keyboard nav
- **Mobile-first** — bottom navigation, PWA install prompt, touch-optimized
- **Guided onboarding** — 7-step tour for first-time users
- **Splash screen** with animations
- **i18n** — English + Ukrainian (easy to add more)
- **API health monitor** — real-time green/red dot in header
- **Auto-refresh panels** — sentiment, whales, trending, screener, gas

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Backend | Python 3.11, FastAPI, SQLAlchemy ORM, SQLite |
| Frontend | Vanilla HTML/CSS/JS (zero framework dependencies), Chart.js, lightweight-charts |
| Real-time | Binance WebSocket (wss://stream.binance.com) |
| AI | Groq API (LLaMA 3.3 70B) + OpenRouter fallback + rule-based fallback |
| Bot | Python, httpx, Telegram Bot API |
| Payments | CryptoBot Crypto Pay API + Telegram Stars |
| APIs | CoinGecko, Binance, DexScreener, BestChange, Alternative.me, DeFiLlama, Etherscan |
| Deploy | Railway (one-click), Docker-ready |

**Zero framework lock-in.** No React, no Next.js, no npm. The entire dashboard is a single HTML file — fast, portable, easy to customize.

---

## Deployment (5 minutes)

```bash
# 1. Clone
git clone https://github.com/alexey04mam-cloud/OmniVision.git
cd OmniVision

# 2. Install
pip install -r requirements.txt

# 3. Configure
cp .env.example .env
# Edit .env with your API keys

# 4. Run
uvicorn main:app --reload --port 8000
```

### Railway (Recommended)
1. Fork this repo
2. Connect to Railway
3. Add environment variables from `.env.example`
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
| `CRYPTOBOT_TOKEN` | For payments | From @CryptoBot |
| `OPENROUTER_API_KEY` | Optional | AI fallback |
| `TELEGRAM_CHANNEL_ID` | Optional | Auto-posting channel |
| `REF_BYBIT` | Optional | Referral link |

Full list in `.env.example`.

---

## Revenue Streams

1. **Subscriptions** — Pro $9.99/mo, VIP $29.99/mo (CryptoBot + Stars)
2. **Referral commissions** — Bybit, BingX, OKX, Binance affiliate links
3. **Telegram channel** — monetize audience with signals/ads
4. **White-label** — resell customized versions
5. **Promo codes** — run campaigns, track conversions

---

## Codebase

- **14,055 lines** of production code
- **110+ API endpoints** covering every feature
- **35 UI panels** — each with loading states, error handling, responsive design
- **0 npm dependencies** on frontend — no build step, no node_modules
- **Validated** — all JS passes `node --check`, all Python passes `py_compile`
- **Well-structured** — FastAPI backend, single-file frontend, separate Telegram bot

---

## What's Included

- Full source code (no obfuscation)
- Working deployment on Railway
- Telegram bot with all commands
- Landing page (SEO-optimized)
- `.env.example` with all config keys
- `deploy.bat` — one-click deploy script
- `dev.bat` — local development server
- `MEMORY.md` — detailed project documentation
- This README

---

## Contact

**Price: $1,000**

Interested? Reach out:
- **Telegram:** [@alexey04mam](https://t.me/alexey04mam)
- **Email:** alexey04mam@gmail.com

---

*Built with FastAPI, Chart.js, lightweight-charts, Binance WebSocket, and CoinGecko API.*
