# OmniVision — Project Memory

## What Is This
Crypto market analytics platform. FastAPI backend + single-file HTML dashboard. Goal: the most powerful crypto analytics tool — everything competitors have + unique features nobody else offers.

## Owner
- User: AlexeyMam (alexey04mam@gmail.com)
- GitHub: alexey04mam-cloud/OmniVision
- Admin user in system: `boss`

## Architecture
- **Backend**: FastAPI + SQLAlchemy ORM + SQLite (~4318 lines, 108 API endpoints)
- **Frontend**: Single file `dashboard.html` (~6017 lines, HTML/CSS/JS, 32 nav tabs, v2.0 PRO)
- **Landing**: `landing.html` (266 lines, hero + features + pricing + CTA)
- **Total codebase**: ~11,561 lines
- **Telegram Bot**: `telegram_bot.py` (~961 lines), httpx-based
- **Deployment**: Railway (https://dependable-tranquility-production-d86f.up.railway.app)
- **AI**: Groq API (llama-3.3-70b-versatile) + OpenRouter fallback + rule-based fallback
- **APIs**: CoinGecko, DexScreener, BestChange, Alternative.me, DeFiLlama, toncenter.com
- **Payments**: CryptoBot (cards/Apple Pay/crypto) + Telegram Stars. Webhook auto-confirm.

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
- CSV Export (portfolio, watchlist, screener, hunted — Pro/VIP only)
- PWA Install Prompt (beforeinstallprompt, banner after 10s)
- Mobile Bottom Navigation (5 tabs: Головна, Тренди, Screener, AI, Меню)
- Touch-friendly mobile improvements (larger buttons, adaptive grid)
- First-time onboarding tooltip (Ctrl+K hint, dismissed to localStorage)
- News panel with market analysis banner (bullish/bearish/neutral)
- Version badge v2.0 NEW in sidebar
- Universal Coin Picker (searchable dropdown, 100 coins, images, rank, keyboard nav)
- Chart Type Toggle (candlestick/line) on Multi-Chart, Crypto panels
- Coin Picker on: Indicators, DCA, Liquidation panels (replaces static selects)

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

## Selling / Monetization
- README.md — professional buyer-facing documentation
- .env.example — complete config reference
- SEO: Open Graph, Twitter Cards, meta description, favicon, manifest.json
- robots.txt + sitemap.xml auto-generated
- Sell on: Flippa, MicroAcquire, Acquire.com, GitHub Marketplace, direct

## Real APIs (not simulated)
- Funding Rates: Binance fapi/v1/premiumIndex (real funding rates)
- Gas Tracker: Etherscan gastracker API (real ETH gas)
- Token Unlocks: DeFiLlama protocols API
- Fear & Greed: Alternative.me direct
- On-Chain BTC: blockchain.info/stats direct
- All other: CoinGecko, DexScreener, BestChange, DeFiLlama

## Session 2 Improvements (2026-06-06)
- **Binance WebSocket**: Real-time price updates via wss://stream.binance.com for 15 coins (BTC, ETH, SOL, BNB, XRP, DOGE, ADA, AVAX, DOT, MATIC, LINK, LTC, ATOM, UNI, NEAR). Flash animations on price change.
- **Global Market Summary Bar**: Total market cap, 24h volume, BTC/ETH dominance, active coins count — shown on homepage.
- **Quick Watchlist Bar**: 10 top coins with live WebSocket prices, instant update every second, mini-icons.
- **Enhanced Alerts**: Web Audio API sound notifications, browser Notification API, alert history (50 items), badge counter in header, removeAlert function.
- **Screener Sparklines**: 7-day mini charts for each coin in screener, Vol/MCap ratio column, clickable rows to asset page. Sparkline data from CoinGecko API.
- **Live indicator**: Pulsing green dot in header showing WebSocket connection status.
- **File ending fix**: Restored truncated Command Palette + closing HTML tags.
- **Total codebase**: ~11,896 lines (was 11,561)

## Last Updated
2026-06-06
