# OmniVision — Project Memory

## What Is This
Crypto market analytics platform. FastAPI backend + single-file HTML dashboard. Goal: the most powerful crypto analytics tool — everything competitors have + unique features nobody else offers.

## Owner
- User: AlexeyMam (alexey04mam@gmail.com)
- GitHub: alexey04mam-cloud/OmniVision
- Admin user in system: `boss`

## Architecture
- **Backend**: FastAPI + SQLAlchemy ORM + SQLite (~4147 lines, 76 API endpoints)
- **Frontend**: Single file `dashboard.html` (~5476 lines, HTML/CSS/JS, 32 nav tabs)
- **Telegram Bot**: `telegram_bot.py` (~961 lines), httpx-based
- **Deployment**: Railway (https://dependable-tranquility-production-d86f.up.railway.app)
- **AI**: Groq API (llama-3.3-70b-versatile) + OpenRouter fallback + rule-based fallback
- **APIs**: CoinGecko, DexScreener, BestChange, Alternative.me, DeFiLlama

## Critical Development Rules
1. **NEVER use Edit tool on dashboard.html or main.py** — truncates large files. Always use Python patch scripts via bash.
2. **After every patch**: validate JS with `node --check`, Python with `py_compile.compile()`.
3. **After Edit tool**: always check `tail` of file to confirm it wasn't truncated.
4. **Ukrainian apostrophe** in JS: use `&#39;`, NOT `'`.
5. **Template system**: main.py serves dashboard.html with `{{PLACEHOLDER}}` replacements.

## Features Built (Complete List)
### Core
- Multi-radar scanner (crypto, stocks, commodities)
- Auto-hunter with interval scanning
- Deep Analytics with multi-timeframe (1h/4h/24h/7d/30d)
- AI Chat assistant (Groq + fallback chain)
- Premium tiers (Free/Pro/VIP) + payments (TON, CryptoBot)
- Push notifications + Telegram bot alerts

### Analytics & Tools
- Market Heatmap (treemap top-50 by mcap, 1h/24h/7d toggle)
- Correlation Matrix (12 coins, Pearson, color heatmap)
- Technical Indicators (RSI, SMA, EMA, MACD + BUY/SELL/HOLD)
- Liquidation Map (zones by leverage 2x-125x, support/resistance)
- Market Screener (filters: price, volume, mcap, change + sorting + pagination)
- Whale Alerts (anomalous volume/price detection, severity levels)
- Token Compare (side-by-side CoinGecko)
- Risk/Reward Calculator (leverage, liquidation, position sizing)
- DCA Calculator (historical backtest, DCA vs Lump Sum comparison, chart)
- Gas Tracker (7 chains: ETH, BSC, Polygon, Arbitrum, Optimism, Avalanche, Solana)
- On-Chain DeFi Analytics (DeFiLlama TVL, top protocols, chain TVLs, stablecoins)
- Funding Rates (15 coins, rate/predicted/annual, simulated from market data)
- Trending Coins (CoinGecko trending + NFTs, score, rank)
- Market Dominance (donut chart, total MCap, volume, BTC/ETH/ALT %)
- Token Unlocks (12 tokens, days until unlock, value, impact level, % supply)
- Social Sentiment (10 coins, Twitter/Reddit/Telegram/Watchlist metrics, bullish %)
- Portfolio Optimization (health score, diversification analysis, suggestions)
- Freemium Enforcement (AI daily limits, Deep Analytics gate, Export gate, upgrade prompts)

### Trading & Exchange
- DEX converter (DexScreener API)
- Exchange aggregator (BestChange public ZIP)
- Crypto ↔ Fiat Converter (15 cryptos × 6 fiats)
- Referral trade buttons (Bybit, BingX, OKX, Binance)
- Exchange banner on main page

### Visualization
- Multi-chart view (2 or 4 charts, candlestick via lightweight-charts)
- Live widgets: Fear & Greed gauge, BTC/ETH dominance, Top Movers, BTC sparkline
- Portfolio charts (line + pie + summary)

### User Features
- Portfolio tracker with P&L + history
- Watchlist with group filtering (DeFi, Layer1, Meme, Other)
- Price alert system with Telegram notifications
- Global token search with CoinGecko autocomplete

### UX / Usability
- Light/Dark theme toggle (topbar button, saved to localStorage)
- Ctrl+K Command Palette (search all 30 tabs, keyboard navigation)
- Sidebar search filter (instant tab filtering by name/description)
- Collapsible sidebar groups (Огляд, Ринки, Інструменти, Спостереження)
- Quick Actions panel on main page (8 popular features in one click)
- Tooltips on nav items (hover to see description)
- Sidebar group state saved to localStorage

### Telegram Bot
- /digest — market summary post
- /post — auto-publish to channel (admin only)
- /prices, /signals, /news, /alert, /premium

## Sidebar Navigation (32 tabs)
hunted, analytics, deepanalytics, advisor, crypto, stocks, commodities, dex, flow, sentiment, whales, onchain, compare, calculator, heatmap, converter, correlation, indicators, liquidation, screener, multichart, dca, gas, funding, trending, dominance, unlocks, social, portfolio-charts, watchlist, wallets, plans, news

## Environment Variables (Railway)
- GROQ_API_KEY, OPENROUTER_API_KEY — AI
- TELEGRAM_BOT_TOKEN, ADMIN_CHAT_ID, TELEGRAM_CHANNEL_ID — Bot
- CRYPTOBOT_TOKEN — Payments
- REF_BYBIT, REF_BINGX, REF_OKX, REF_BINANCE — Referrals
- DEFAULT_EXCHANGE, SITE_URL, BOT_USERNAME — Config

## TON Wallet
`UQAHQhdeLLuZerZxlVPiB-PVAFPhEzbvTX69qrpr_bT8TmV-`

## Dev Tools
- `deploy.bat` — one-click deploy (validates → commits → pushes)
- `dev.bat` — local server (uvicorn --reload)
- `validate.bat` — syntax check all .py files
- `.vscode/` — settings, tasks (Ctrl+Shift+B), launch (F5), extensions

## Last Updated
2026-06-01
