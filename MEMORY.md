# OmniVision — Project Memory

## What Is This
Crypto market analytics platform. FastAPI backend + single-file HTML dashboard. Goal: the most powerful crypto analytics tool — everything competitors have + unique features nobody else offers.

## Owner
- User: AlexeyMam (alexey04mam@gmail.com)
- GitHub: alexey04mam-cloud/OmniVision
- Admin user in system: `boss`

## Architecture
- **Backend**: FastAPI + SQLAlchemy ORM + SQLite
- **Frontend**: Single file `dashboard.html` (~3100 lines, HTML/CSS/JS)
- **Telegram Bot**: `telegram_bot.py` (~825 lines), httpx-based (no python-telegram-bot)
- **Deployment**: Railway (https://dependable-tranquility-production-d86f.up.railway.app)
- **AI**: Groq API (llama-3.3-70b-versatile) + OpenRouter fallback + rule-based fallback

## Critical Development Rules
1. **NEVER use Edit tool on dashboard.html** — it truncates large files. Always use Python patch scripts via bash.
2. **Bash sandbox does NOT sync to user disk** — only Read/Write/Edit tools modify user files. Use bash to create patches, then apply via file tools, or use `cp` within the mounted path.
3. **Template system**: main.py serves dashboard.html with `{{PLACEHOLDER}}` replacements.
4. **After every patch**: validate JS with `node --check`, Python with `py_compile.compile()`.
5. **Ukrainian apostrophe** in JS strings: use `&#39;` (HTML entity), NOT `'` — causes SyntaxError.
6. **main.py is also large (~2086 lines)** — Edit tool can truncate it too. For large edits, use patch scripts.

## Key Files
- `main.py` — Backend (routes, models, AI chat, payments, analytics)
- `dashboard.html` — Frontend (all tabs, CSS, JS in one file)
- `telegram_bot.py` — Telegram bot with commands, payments, alerts
- `.env` — Local env vars (BOSS_KEY, ADMIN_USER, ADMIN_PASS, SECRET_KEY, PORT)
- Railway Variables — GROQ_API_KEY, TELEGRAM_BOT_TOKEN, CRYPTOBOT_TOKEN, etc.

## Features Built
- Multi-radar scanner (crypto, stocks, commodities)
- Auto-hunter with interval scanning
- Portfolio tracker with P&L
- Deep Analytics (gainers/losers/volume/timeline)
- Market Pulse widget (fear & greed, sentiment)
- AI Chat assistant (full crypto Q&A, real-time data)
- Premium tiers (Free/Pro/VIP) with tier limits
- Payment system (TON wallet, Telegram CryptoBot)
- Push notifications (service worker)
- News & Trends aggregation
- Flow alerts (cross-market correlation detection)
- Telegram bot with alerts, premium, payment commands

## TON Wallet
`UQAHQhdeLLuZerZxlVPiB-PVAFPhEzbvTX69qrpr_bT8TmV-`

## API Keys (Railway Variables)
- GROQ_API_KEY — for AI chat (Groq/Llama)
- OPENROUTER_API_KEY — fallback AI
- TELEGRAM_BOT_TOKEN — Telegram bot
- CRYPTOBOT_TOKEN — CryptoBot payments
- ADMIN_CHAT_ID — admin Telegram notifications

## Known Issues / TODO
- Card/Apple Pay/Google Pay — needs real payment processor (Stripe or CryptoBot with card support)
- BestChange API — user needs to register and get API key, then add BESTCHANGE_API_KEY to Railway
- AI chat — fixed (missing JS functions added), Groq model fallback chain added, needs production testing
- Git lock files in sandbox — commit/push must be done from user's terminal

## Recently Completed
- DEX converter (DexScreener API) — search, quote, trending, pair details
- Exchange aggregator (BestChange ready) — demo mode without key, real with key
- AI chat fix — aiSend/aiAsk JS functions were missing, now added
- AI error handling — fallback model chain, detailed logging, /api/ai/test diagnostic

## Development Roadmap (Priority Order)
1. ~~DEX converters + exchange aggregators~~ ✅ DONE
2. Real card/Apple Pay/Google Pay payments
3. Advanced AI agents (multi-model analysis)
4. AWS migration (when scale requires)
5. On-chain analytics (whale tracking, smart money)
6. Unique features nobody else has

## Last Updated
2026-05-22
