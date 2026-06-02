"""
Omni-Vision Telegram Bot — Price Alerts, Signals & Whale Tracking
Lightweight implementation using httpx (no extra deps needed)
"""

import os
import asyncio
import logging
import time
import json
from datetime import datetime, timezone
from typing import Optional

import httpx

log = logging.getLogger("omni-tg-bot")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TON_WALLET = os.getenv("TON_WALLET", "UQAHQhdeLLuZerZxlVPiB-PVAFPhEzbvTX69qrpr_bT8TmV-")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "")
CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "")  # @YourChannel or -100xxxxx
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"


# ══════════════════════════════════════════
# STORAGE — subscribers & alerts
# ══════════════════════════════════════════

# In-memory store (persisted to JSON file)
_data_file = "tg_subscribers.json"
_subscribers = {}  # chat_id -> {username, alerts_enabled, language, joined_at, alerts: [{symbol, target, direction}]}
_alert_history = []  # [{chat_id, symbol, message, sent_at}]

def _load_data():
    global _subscribers
    try:
        if os.path.exists(_data_file):
            with open(_data_file, "r") as f:
                _subscribers = json.load(f)
                log.info(f"[TG] Loaded {len(_subscribers)} subscribers")
    except Exception as e:
        log.error(f"[TG] Failed to load data: {e}")

def _save_data():
    try:
        with open(_data_file, "w") as f:
            json.dump(_subscribers, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.error(f"[TG] Failed to save data: {e}")


# ══════════════════════════════════════════
# TELEGRAM API HELPERS
# ══════════════════════════════════════════

async def tg_request(method: str, data: dict = None) -> dict:
    """Make a request to Telegram Bot API"""
    if not TELEGRAM_TOKEN:
        return {"ok": False, "description": "No token"}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(f"{TELEGRAM_API}/{method}", json=data or {})
            return resp.json()
    except Exception as e:
        log.error(f"[TG] API error: {e}")
        return {"ok": False, "description": str(e)}


async def send_message(chat_id, text: str, parse_mode: str = "HTML", reply_markup: dict = None):
    """Send a message to a Telegram chat"""
    data = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
    if reply_markup:
        data["reply_markup"] = reply_markup
    return await tg_request("sendMessage", data)


async def answer_callback(callback_query_id: str, text: str = ""):
    return await tg_request("answerCallbackQuery", {
        "callback_query_id": callback_query_id, "text": text
    })


# ══════════════════════════════════════════
# COMMAND HANDLERS
# ══════════════════════════════════════════

async def cmd_start(chat_id, username=""):
    """Handle /start command"""
    cid = str(chat_id)
    if cid not in _subscribers:
        _subscribers[cid] = {
            "username": username,
            "alerts_enabled": True,
            "language": "ukr",
            "joined_at": datetime.now(timezone.utc).isoformat(),
            "price_alerts": [],
            "notify_whales": True,
            "notify_signals": True,
            "notify_hunt": False,
        }
        _save_data()

    keyboard = {
        "inline_keyboard": [
            [{"text": "📊 Ціни", "callback_data": "prices"},
             {"text": "🎯 Сигнали", "callback_data": "signals"}],
            [{"text": "🐋 Кити", "callback_data": "whales"},
             {"text": "📰 Новини", "callback_data": "news"}],
            [{"text": "🔔 Мої алерти", "callback_data": "my_alerts"},
             {"text": "⚙️ Налаштування", "callback_data": "settings"}],
            [{"text": "👑 Premium", "callback_data": "premium"}],
        ]
    }

    await send_message(chat_id,
        "🔭 <b>Omni-Vision Bot</b>\n\n"
        "Вітаю! Я відстежую ринки 24/7 і відправлю тобі:\n\n"
        "📈 <b>Сигнали входу/виходу</b> — коли вигідно купити або продати\n"
        "🐋 <b>Whale алерти</b> — великі рухи китів\n"
        "🔔 <b>Цінові алерти</b> — коли монета досягне твоєї ціни\n"
        "📰 <b>Новини</b> — з аналізом сентименту\n\n"
        "Обери що цікавить:",
        reply_markup=keyboard
    )


async def cmd_prices(chat_id, db_session_factory):
    """Show top prices"""
    try:
        db = db_session_factory()
        from sqlalchemy import desc
        assets = db.query_market_assets(limit=10)  # Will be connected to actual DB
        db.close()
    except:
        assets = []

    if not assets:
        # Fallback: fetch from CoinGecko
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get("https://api.coingecko.com/api/v3/simple/price",
                    params={"ids": "bitcoin,ethereum,solana,ripple,cardano,dogecoin,polkadot,avalanche-2,chainlink,uniswap",
                            "vs_currencies": "usd", "include_24hr_change": "true"})
                data = r.json()
        except:
            await send_message(chat_id, "❌ Не вдалося отримати ціни. Спробуйте пізніше.")
            return

        lines = ["📊 <b>Топ-10 криптовалют</b>\n"]
        names = {"bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL", "ripple": "XRP",
                 "cardano": "ADA", "dogecoin": "DOGE", "polkadot": "DOT",
                 "avalanche-2": "AVAX", "chainlink": "LINK", "uniswap": "UNI"}

        for cg_id, symbol in names.items():
            if cg_id in data:
                price = data[cg_id].get("usd", 0)
                change = data[cg_id].get("usd_24h_change", 0)
                emoji = "🟢" if change >= 0 else "🔴"
                sign = "+" if change >= 0 else ""
                if price >= 100:
                    p_str = f"${price:,.2f}"
                elif price >= 1:
                    p_str = f"${price:.4f}"
                else:
                    p_str = f"${price:.6f}"
                lines.append(f"{emoji} <b>{symbol}</b>  {p_str}  ({sign}{change:.1f}%)")

        lines.append(f"\n🕐 {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
        await send_message(chat_id, "\n".join(lines))
        return

    # From DB
    lines = ["📊 <b>Вполювані активи</b>\n"]
    for a in assets[:10]:
        ch = a.get("change_pct", 0) or 0
        emoji = "🟢" if ch >= 0 else "🔴"
        sign = "+" if ch >= 0 else ""
        price = a.get("price_usd", 0)
        lines.append(f"{emoji} <b>{a['symbol']}</b>  ${price:,.2f}  ({sign}{ch:.1f}%)")
    await send_message(chat_id, "\n".join(lines))


async def cmd_signals(chat_id, db_session_factory=None):
    """Generate and send investment signals"""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get("https://api.coingecko.com/api/v3/simple/price",
                params={"ids": "bitcoin,ethereum,solana,ripple,cardano,dogecoin,polkadot,avalanche-2,chainlink,uniswap,matic-network,near,cosmos,litecoin",
                        "vs_currencies": "usd", "include_24hr_change": "true",
                        "include_24hr_vol": "true"})
            data = r.json()
    except:
        await send_message(chat_id, "❌ Помилка отримання даних")
        return

    names = {"bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL", "ripple": "XRP",
             "cardano": "ADA", "dogecoin": "DOGE", "polkadot": "DOT",
             "avalanche-2": "AVAX", "chainlink": "LINK", "uniswap": "UNI",
             "matic-network": "MATIC", "near": "NEAR", "cosmos": "ATOM", "litecoin": "LTC"}

    buy_signals = []
    sell_signals = []
    hold_signals = []

    for cg_id, symbol in names.items():
        if cg_id not in data:
            continue
        ch = data[cg_id].get("usd_24h_change", 0) or 0
        price = data[cg_id].get("usd", 0)

        score = 0
        reason = ""

        if ch < -8:
            score += 3
            reason = f"Сильне падіння {ch:.1f}% — можлива корекція вгору"
        elif ch < -3:
            score += 1
            reason = f"Помірне падіння {ch:.1f}% — спостерігайте"
        elif ch > 8:
            score -= 2
            reason = f"Різкий ріст {ch:.1f}% — ризик відкату"
        elif ch > 3:
            score += 1
            reason = f"Стабільний ріст {ch:.1f}%"
        else:
            reason = f"Бічний рух ({ch:+.1f}%)"

        if price >= 100:
            p_str = f"${price:,.2f}"
        elif price >= 1:
            p_str = f"${price:.4f}"
        else:
            p_str = f"${price:.6f}"

        entry = f"<b>{symbol}</b> {p_str}\n   └ {reason}"

        if score >= 2:
            buy_signals.append(entry)
        elif score <= -2:
            sell_signals.append(entry)
        else:
            hold_signals.append(entry)

    lines = ["🎯 <b>Інвестиційні сигнали</b>\n"]

    if buy_signals:
        lines.append("📈 <b>ВХІД (BUY):</b>")
        lines.extend(buy_signals)
        lines.append("")

    if sell_signals:
        lines.append("📉 <b>ВИХІД (SELL):</b>")
        lines.extend(sell_signals)
        lines.append("")

    if hold_signals:
        lines.append("⏳ <b>ОЧІКУВАННЯ (HOLD):</b>")
        lines.extend(hold_signals)

    lines.append(f"\n🕐 {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
    lines.append("\n⚠️ <i>Це не фінансова порада. DYOR.</i>")

    await send_message(chat_id, "\n".join(lines))


async def cmd_set_alert(chat_id, args: str):
    """Set a price alert: /alert BTC 100000 above"""
    parts = args.strip().split()
    if len(parts) < 2:
        await send_message(chat_id,
            "🔔 <b>Як встановити алерт:</b>\n\n"
            "<code>/alert BTC 100000</code> — коли BTC досягне $100k\n"
            "<code>/alert ETH 3000 below</code> — коли ETH впаде нижче $3k\n"
            "<code>/alert SOL 200 above</code> — коли SOL підніметься вище $200")
        return

    symbol = parts[0].upper()
    try:
        target = float(parts[1])
    except ValueError:
        await send_message(chat_id, "❌ Невірна ціна. Приклад: <code>/alert BTC 100000</code>")
        return

    direction = parts[2].lower() if len(parts) > 2 else "above"
    if direction not in ("above", "below"):
        direction = "above"

    cid = str(chat_id)
    if cid not in _subscribers:
        _subscribers[cid] = {"price_alerts": [], "alerts_enabled": True,
                             "joined_at": datetime.now(timezone.utc).isoformat()}

    alerts = _subscribers[cid].get("price_alerts", [])
    if len(alerts) >= 20:
        await send_message(chat_id, "❌ Максимум 20 алертів. Видаліть старі через /my_alerts")
        return

    alerts.append({
        "symbol": symbol,
        "target": target,
        "direction": direction,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "triggered": False
    })
    _subscribers[cid]["price_alerts"] = alerts
    _save_data()

    dir_text = "вище" if direction == "above" else "нижче"
    await send_message(chat_id,
        f"✅ Алерт встановлено!\n\n"
        f"🔔 <b>{symbol}</b> — коли ціна буде {dir_text} <b>${target:,.2f}</b>\n\n"
        f"Всього активних алертів: {len([a for a in alerts if not a.get('triggered')])}")


async def cmd_my_alerts(chat_id):
    """Show user's alerts"""
    cid = str(chat_id)
    alerts = _subscribers.get(cid, {}).get("price_alerts", [])
    active = [a for a in alerts if not a.get("triggered")]

    if not active:
        await send_message(chat_id,
            "🔕 У вас немає активних алертів.\n\n"
            "Встановіть: <code>/alert BTC 100000</code>")
        return

    lines = ["🔔 <b>Ваші алерти:</b>\n"]
    for i, a in enumerate(active):
        dir_emoji = "📈" if a["direction"] == "above" else "📉"
        dir_text = "вище" if a["direction"] == "above" else "нижче"
        lines.append(f"{i+1}. {dir_emoji} <b>{a['symbol']}</b> — {dir_text} ${a['target']:,.2f}")

    lines.append(f"\nВсього: {len(active)}/20")
    lines.append("\nВидалити: <code>/delalert 1</code>")
    await send_message(chat_id, "\n".join(lines))


async def cmd_del_alert(chat_id, args: str):
    """Delete an alert by number"""
    cid = str(chat_id)
    alerts = _subscribers.get(cid, {}).get("price_alerts", [])
    active = [a for a in alerts if not a.get("triggered")]

    try:
        idx = int(args.strip()) - 1
        if 0 <= idx < len(active):
            removed = active[idx]
            alerts.remove(removed)
            _save_data()
            await send_message(chat_id, f"✅ Алерт <b>{removed['symbol']}</b> ${removed['target']:,.2f} видалено")
        else:
            await send_message(chat_id, "❌ Невірний номер")
    except:
        await send_message(chat_id, "❌ Вкажіть номер: <code>/delalert 1</code>")


async def cmd_news(chat_id):
    """Fetch and send latest crypto news with sentiment"""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get("https://min-api.cryptocompare.com/data/v2/news/",
                params={"lang": "EN", "sortOrder": "popular"})
            data = r.json()
            articles = data.get("Data", [])[:5]
    except:
        await send_message(chat_id, "❌ Помилка завантаження новин")
        return

    if not articles:
        await send_message(chat_id, "📰 Немає свіжих новин")
        return

    lines = ["📰 <b>Топ-5 крипто новин</b>\n"]
    for a in articles:
        title = a.get("title", "")[:80]
        source = a.get("source", "")
        url = a.get("url", "")
        lines.append(f"• <b>{title}</b>\n   <i>{source}</i> — <a href=\"{url}\">читати</a>\n")

    lines.append(f"🕐 {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
    await send_message(chat_id, "\n".join(lines), parse_mode="HTML")


async def cmd_premium(chat_id):
    """Show premium plans"""
    keyboard = {
        "inline_keyboard": [
            [{"text": "⭐ Pro — $9.99/міс", "callback_data": "buy_pro"}],
            [{"text": "👑 VIP — $29.99/міс", "callback_data": "buy_vip"}],
            [{"text": "◀️ Назад", "callback_data": "back_menu"}],
        ]
    }
    await send_message(chat_id,
        "👑 <b>Omni-Vision Premium</b>\n\n"
        "🆓 <b>Free</b> — Базовий доступ\n"
        "├ 3 гаманці, 10 watchlist\n"
        "├ 3 AI запити/день\n"
        "└ Без експорту\n\n"
        "⭐ <b>Pro — $9.99/міс</b>\n"
        "├ 20 гаманців, 100 watchlist\n"
        "├ 50 AI запитів/день\n"
        "├ CSV експорт\n"
        "└ Deep Analytics\n\n"
        "👑 <b>VIP — $29.99/міс</b>\n"
        "├ Без обмежень\n"
        "├ Необмежено AI запитів\n"
        "├ Пріоритетна підтримка\n"
        "└ Всі функції\n\n"
        "Оберіть план:",
        reply_markup=keyboard
    )


async def cmd_buy_tier(chat_id, tier, username=""):
    """Show payment options: Stars or site"""
    prices = {"pro": 9.99, "vip": 29.99}
    stars_prices = {"pro": 250, "vip": 750}  # ~$5 per 100 Stars
    price_usd = prices.get(tier, 9.99)
    stars = stars_prices.get(tier, 250)

    keyboard = {
        "inline_keyboard": [
            [{"text": f"\u2b50 \u041e\u043f\u043b\u0430\u0442\u0438\u0442\u0438 {stars} Stars", "callback_data": f"stars_{tier}"}],
            [{"text": "\U0001f4b3 \u041a\u0430\u0440\u0442\u0430 / Apple Pay (\u0441\u0430\u0439\u0442)", "url": SITE_URL + "/?tab=plans"}],
            [{"text": "\u25c0\ufe0f \u041d\u0430\u0437\u0430\u0434", "callback_data": "back_premium"}],
        ]
    }

    await send_message(chat_id,
        f"\U0001f451 <b>\u041e\u043f\u043b\u0430\u0442\u0430 {tier.upper()}</b>\n\n"
        f"\U0001f4b5 \u0421\u0443\u043c\u0430: <b>${price_usd}/\u043c\u0456\u0441</b>\n\n"
        f"<b>\u0421\u043f\u043e\u0441\u043e\u0431\u0438 \u043e\u043f\u043b\u0430\u0442\u0438:</b>\n\n"
        f"\u2b50 <b>Telegram Stars</b> \u2014 {stars} Stars\n"
        f"   \u041e\u043f\u043b\u0430\u0442\u0430 \u0447\u0435\u0440\u0435\u0437 App Store / Google Play\n\n"
        f"\U0001f4b3 <b>\u041a\u0430\u0440\u0442\u0430 / Apple Pay / Google Pay</b>\n"
        f"   \u041e\u043f\u043b\u0430\u0442\u0430 \u043d\u0430 \u0441\u0430\u0439\u0442\u0456 \u0447\u0435\u0440\u0435\u0437 CryptoBot\n\n"
        f"\u041e\u0431\u0435\u0440\u0456\u0442\u044c \u0441\u043f\u043e\u0441\u0456\u0431:",
        reply_markup=keyboard
    )
async def _fetch_market_data():
    """Fetch market summary for digest"""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # BTC price
            r = await client.get("https://api.coingecko.com/api/v3/simple/price",
                params={"ids": "bitcoin,ethereum", "vs_currencies": "usd", "include_24hr_change": "true"})
            prices = r.json() if r.status_code == 200 else {}

            # Top gainers/losers
            r2 = await client.get("https://api.coingecko.com/api/v3/coins/markets",
                params={"vs_currency": "usd", "order": "market_cap_desc", "per_page": 50, "page": 1,
                         "sparkline": "false", "price_change_percentage": "24h"})
            coins = r2.json() if r2.status_code == 200 else []

            # Fear & Greed
            r3 = await client.get("https://api.alternative.me/fng/?limit=1")
            fng = r3.json().get("data", [{}])[0] if r3.status_code == 200 else {}

        return {"prices": prices, "coins": coins, "fng": fng}
    except Exception as e:
        log.error(f"[TG] Market data fetch error: {e}")
        return {"prices": {}, "coins": [], "fng": {}}


async def _generate_digest():
    """Generate formatted digest post"""
    data = await _fetch_market_data()
    prices = data["prices"]
    coins = data["coins"]
    fng = data["fng"]

    btc = prices.get("bitcoin", {})
    eth = prices.get("ethereum", {})
    btc_price = btc.get("usd", 0)
    btc_change = btc.get("usd_24h_change", 0)
    eth_price = eth.get("usd", 0)
    eth_change = eth.get("usd_24h_change", 0)

    # Fear & Greed
    fg_val = fng.get("value", "?")
    fg_label = fng.get("value_classification", "?")
    fg_int = int(fg_val) if str(fg_val).isdigit() else 50
    if fg_int <= 25: fg_emoji = "\U0001f534"
    elif fg_int <= 45: fg_emoji = "\U0001f7e0"
    elif fg_int <= 55: fg_emoji = "\U0001f7e1"
    elif fg_int <= 75: fg_emoji = "\U0001f7e2"
    else: fg_emoji = "\U0001f7e2\U0001f7e2"

    # Sentiment
    with_change = [c for c in coins if c.get("price_change_percentage_24h") is not None]
    bullish = sum(1 for c in with_change if c["price_change_percentage_24h"] > 0)
    bearish = sum(1 for c in with_change if c["price_change_percentage_24h"] < 0)
    total = len(with_change) or 1
    sentiment = "BULLISH \U0001f7e2" if bullish > bearish else "BEARISH \U0001f534" if bearish > bullish else "NEUTRAL \U0001f7e1"
    bull_pct = round(bullish / total * 100, 1)

    # Top gainers & losers
    sorted_up = sorted(with_change, key=lambda x: x["price_change_percentage_24h"], reverse=True)[:3]
    sorted_dn = sorted(with_change, key=lambda x: x["price_change_percentage_24h"])[:3]

    # Format
    now = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")

    text = f"\U0001f4ca <b>Omni-Vision | Market Digest</b>\n"
    text += f"\U0001f4c5 {now}\n\n"

    text += f"{fg_emoji} <b>Fear & Greed:</b> {fg_val} ({fg_label})\n"
    text += f"\U0001f4e2 <b>\u041d\u0430\u0441\u0442\u0440\u0456\u0439:</b> {sentiment} ({bull_pct}% bullish)\n\n"

    btc_arrow = "\u25B2" if btc_change >= 0 else "\u25BC"
    eth_arrow = "\u25B2" if eth_change >= 0 else "\u25BC"
    text += f"\u20BF <b>BTC:</b> ${btc_price:,.0f} {btc_arrow} {btc_change:+.2f}%\n"
    text += f"\u039E <b>ETH:</b> ${eth_price:,.0f} {eth_arrow} {eth_change:+.2f}%\n\n"

    text += "\U0001f525 <b>\u0422\u043E\u043F \u0437\u0440\u043E\u0441\u0442\u0430\u043D\u043D\u044F:</b>\n"
    for c in sorted_up:
        sym = (c.get("symbol") or "").upper()
        ch = c["price_change_percentage_24h"]
        text += f"  \u25B2 {sym} <b>+{ch:.2f}%</b>\n"

    text += "\n\u2744\uFE0F <b>\u0422\u043E\u043F \u043F\u0430\u0434\u0456\u043D\u043D\u044F:</b>\n"
    for c in sorted_dn:
        sym = (c.get("symbol") or "").upper()
        ch = c["price_change_percentage_24h"]
        text += f"  \u25BC {sym} <b>{ch:.2f}%</b>\n"

    ref = REFERRAL_LINKS_BOT.get("bybit", "")
    text += f"\n\U0001f680 <a href=\"{ref}\">\u0422\u043E\u0440\u0433\u0443\u0432\u0430\u0442\u0438 \u043D\u0430 Bybit</a>\n"
    text += f"\U0001f4f1 <a href=\"{SITE_URL}\">Dashboard</a> | <a href=\"https://t.me/{BOT_USERNAME}\">Bot</a> | <a href=\"https://t.me/+fmJn1JHeb6NmZDRi\">\U0001f4e2 Канал</a>"

    return text




async def send_stars_invoice(chat_id, tier):
    """Send Telegram Stars invoice"""
    stars_prices = {"pro": 250, "vip": 750}
    stars = stars_prices.get(tier, 250)
    prices_usd = {"pro": 9.99, "vip": 29.99}

    payload = f"OV-STARS-{chat_id}-{tier}-{int(time.time())}"

    data = {
        "chat_id": chat_id,
        "title": f"Omni-Vision {tier.upper()} \u2014 30 \u0434\u043d\u0456\u0432",
        "description": f"\u041f\u0456\u0434\u043f\u0438\u0441\u043a\u0430 {tier.upper()} \u043d\u0430 Omni-Vision. Deep Analytics, AI \u0437\u0430\u043f\u0438\u0442\u0438, CSV \u0435\u043a\u0441\u043f\u043e\u0440\u0442.",
        "payload": payload,
        "currency": "XTR",  # Telegram Stars
        "prices": [{"label": f"{tier.upper()} Plan", "amount": stars}],
    }

    result = await tg_request("sendInvoice", data)
    if not result.get("ok"):
        await send_message(chat_id, f"\u274c \u041f\u043e\u043c\u0438\u043b\u043a\u0430 \u0441\u0442\u0432\u043e\u0440\u0435\u043d\u043d\u044f \u0456\u043d\u0432\u043e\u0439\u0441\u0443: {result.get('description', 'unknown')}")
    return result

async def cmd_digest(chat_id):
    """Generate and send market digest"""
    await send_message(chat_id, "\u23F3 \u0413\u0435\u043D\u0435\u0440\u0443\u044E \u0434\u0430\u0439\u0434\u0436\u0435\u0441\u0442...")
    text = await _generate_digest()
    await send_message(chat_id, text)


async def cmd_post_channel(chat_id):
    """Post digest to the configured channel"""
    if str(chat_id) != str(ADMIN_CHAT_ID):
        await send_message(chat_id, "\u26D4 \u0422\u0456\u043B\u044C\u043A\u0438 \u0430\u0434\u043C\u0456\u043D \u043C\u043E\u0436\u0435 \u043F\u043E\u0441\u0442\u0438\u0442\u0438 \u0432 \u043A\u0430\u043D\u0430\u043B")
        return
    if not CHANNEL_ID:
        await send_message(chat_id, "\u26A0 TELEGRAM_CHANNEL_ID \u043D\u0435 \u043D\u0430\u043B\u0430\u0448\u0442\u043E\u0432\u0430\u043D\u043E. \u0414\u043E\u0434\u0430\u0439 \u0432 Railway Variables.")
        return
    text = await _generate_digest()
    try:
        await send_message(CHANNEL_ID, text)
        await send_message(chat_id, "\u2705 \u041E\u043F\u0443\u0431\u043B\u0456\u043A\u043E\u0432\u0430\u043D\u043E \u0432 \u043A\u0430\u043D\u0430\u043B!")
    except Exception as e:
        await send_message(chat_id, f"\u274C \u041F\u043E\u043C\u0438\u043B\u043A\u0430: {e}")


async def cmd_settings(chat_id):
    """Show settings"""
    cid = str(chat_id)
    sub = _subscribers.get(cid, {})

    keyboard = {
        "inline_keyboard": [
            [{"text": ("✅" if sub.get("notify_signals", True) else "❌") + " Сигнали",
              "callback_data": "toggle_signals"},
             {"text": ("✅" if sub.get("notify_whales", True) else "❌") + " Кити",
              "callback_data": "toggle_whales"}],
            [{"text": ("✅" if sub.get("notify_hunt", False) else "❌") + " Результати полювання",
              "callback_data": "toggle_hunt"},
             {"text": ("✅" if sub.get("alerts_enabled", True) else "❌") + " Цінові алерти",
              "callback_data": "toggle_alerts"}],
            [{"text": "◀️ Назад", "callback_data": "back_menu"}],
        ]
    }

    await send_message(chat_id,
        "⚙️ <b>Налаштування сповіщень</b>\n\n"
        "Натисніть щоб увімкнути/вимкнути:",
        reply_markup=keyboard
    )


# ══════════════════════════════════════════
# CALLBACK HANDLER
# ══════════════════════════════════════════

async def handle_callback(callback_query, db_session_factory=None):
    """Handle inline button clicks"""
    data = callback_query.get("data", "")
    chat_id = callback_query["message"]["chat"]["id"]
    cb_id = callback_query["id"]

    if data == "prices":
        await answer_callback(cb_id, "Завантажую ціни...")
        await cmd_prices(chat_id, db_session_factory)
    elif data == "signals":
        await answer_callback(cb_id, "Аналізую ринок...")
        await cmd_signals(chat_id)
    elif data == "whales":
        await answer_callback(cb_id, "Шукаю китів...")
        await cmd_prices(chat_id, db_session_factory)  # Reuse for now
    elif data == "news":
        await answer_callback(cb_id, "Завантажую новини...")
        await cmd_news(chat_id)
    elif data == "my_alerts":
        await answer_callback(cb_id)
        await cmd_my_alerts(chat_id)
    elif data == "settings":
        await answer_callback(cb_id)
        await cmd_settings(chat_id)
    elif data == "premium":
        await answer_callback(cb_id)
        await cmd_premium(chat_id)
    elif data == "back_menu":
        await answer_callback(cb_id)
        await cmd_start(chat_id)
    elif data == "buy_pro":
        await answer_callback(cb_id, "Pro план...")
        username = callback_query.get("from", {}).get("username", "")
        await cmd_buy_tier(chat_id, "pro", username)
    elif data == "buy_vip":
        await answer_callback(cb_id, "VIP план...")
        username = callback_query.get("from", {}).get("username", "")
        await cmd_buy_tier(chat_id, "vip", username)
    elif data == "back_premium":
        await answer_callback(cb_id)
        await cmd_premium(chat_id)
    elif data.startswith("paid_"):
        await answer_callback(cb_id, "Дякуємо! Перевіряємо...")
        parts = data.split("_", 2)
        tier_name = parts[1] if len(parts) > 1 else "pro"
        await send_message(chat_id,
            "⏳ <b>Дякуємо!</b>\n\n"
            "Ваш платіж перевіряється. Адмін отримав сповіщення "
            "і підтвердить оплату найближчим часом.\n\n"
            "Зазвичай це займає кілька хвилин. ⚡"
        )
    elif data.startswith("admin_confirm_"):
        # Admin confirms payment
        parts = data.replace("admin_confirm_", "").split("_", 1)
        target_chat = parts[0] if parts else ""
        tier_name = parts[1] if len(parts) > 1 else "pro"
        await answer_callback(cb_id, f"Підтверджено {tier_name.upper()}!")
        if target_chat:
            try:
                await send_message(int(target_chat),
                    f"✅ <b>Оплату підтверджено!</b>\n\n"
                    f"🎉 План <b>{tier_name.upper()}</b> активовано на 30 днів!\n"
                    f"Дякуємо за підтримку Omni-Vision! 🚀"
                )
                await send_message(chat_id,
                    f"✅ Підтверджено {tier_name.upper()} для chat {target_chat}"
                )
            except Exception as e:
                await send_message(chat_id, f"Помилка: {e}")
    elif data.startswith("toggle_"):
        cid = str(chat_id)
        key = data.replace("toggle_", "notify_")
        if key == "notify_alerts":
            key = "alerts_enabled"
        if cid in _subscribers:
            current = _subscribers[cid].get(key, True)
            _subscribers[cid][key] = not current
            _save_data()
            status = "увімкнено ✅" if not current else "вимкнено ❌"
            await answer_callback(cb_id, f"{status}")
            await cmd_settings(chat_id)
    else:
        await answer_callback(cb_id, "🤔")


# ══════════════════════════════════════════
# UPDATE PROCESSOR
# ══════════════════════════════════════════

async def process_update(update: dict, db_session_factory=None):
    """Process a single Telegram update"""
    if "message" in update:
        msg = update["message"]
        chat_id = msg["chat"]["id"]
        text = msg.get("text", "")
        username = msg.get("from", {}).get("username", "")

        if text.startswith("/start stars_"):
            tier = text.replace("/start stars_", "").strip()
            if tier in ("pro", "vip"):
                await cmd_start(chat_id, username)
                await send_stars_invoice(chat_id, tier)
            else:
                await cmd_start(chat_id, username)
        elif text.startswith("/start pay_"):
            tier = text.replace("/start pay_", "").strip()
            if tier in ("pro", "vip"):
                await cmd_start(chat_id, username)
                await cmd_buy_tier(chat_id, tier, username)
            else:
                await cmd_start(chat_id, username)
        elif text.startswith("/start"):
            await cmd_start(chat_id, username)
        elif text.startswith("/prices") or text.startswith("/p"):
            await cmd_prices(chat_id, db_session_factory)
        elif text.startswith("/signals") or text.startswith("/s"):
            await cmd_signals(chat_id)
        elif text.startswith("/news") or text.startswith("/n"):
            await cmd_news(chat_id)
        elif text.startswith("/alert "):
            await cmd_set_alert(chat_id, text[7:])
        elif text.startswith("/my_alerts") or text.startswith("/alerts"):
            await cmd_my_alerts(chat_id)
        elif text.startswith("/delalert "):
            await cmd_del_alert(chat_id, text[10:])
        elif text.startswith("/settings"):
            await cmd_settings(chat_id)
        elif text.startswith("/digest"):
            await cmd_digest(chat_id)
        elif text.startswith("/post_channel") or text.startswith("/post"):
            await cmd_post_channel(chat_id)
        elif text.startswith("/premium"):
            await cmd_premium(chat_id)
        elif text.startswith("/help"):
            await send_message(chat_id,
                "📋 <b>Команди:</b>\n\n"
                "/prices — Топ криптовалют\n"
                "/signals — Інвестиційні сигнали\n"
                "/news — Останні новини\n"
                "/alert BTC 100000 — Ціновий алерт\n"
                "/my_alerts — Мої алерти\n"
                "/delalert 1 — Видалити алерт\n"
                "/settings — Налаштування\n"
                "/premium — Преміум плани\n"
                "/digest — Дайджест ринку\n"
                "/post — Опублікувати в канал (admin)\n"
                "/help — Ця довідка")
        else:
            # Unknown command — show menu
            await cmd_start(chat_id, username)

    elif "callback_query" in update:
        await handle_callback(update["callback_query"], db_session_factory)


# ══════════════════════════════════════════
# ALERT CHECKER (runs in background)
# ══════════════════════════════════════════

async def check_price_alerts():
    """Check all price alerts against current prices"""
    if not _subscribers:
        return

    # Gather all unique symbols needed
    symbols_needed = set()
    cg_map = {
        "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "XRP": "ripple",
        "ADA": "cardano", "DOGE": "dogecoin", "DOT": "polkadot", "AVAX": "avalanche-2",
        "LINK": "chainlink", "UNI": "uniswap", "MATIC": "matic-network",
        "NEAR": "near", "ATOM": "cosmos", "LTC": "litecoin", "BNB": "binancecoin",
        "SHIB": "shiba-inu", "APT": "aptos", "ARB": "arbitrum", "OP": "optimism",
        "SUI": "sui", "SEI": "sei-network", "TIA": "celestia", "INJ": "injective-protocol",
        "FET": "fetch-ai", "RENDER": "render-token", "WIF": "dogwifcoin",
        "PEPE": "pepe", "FLOKI": "floki"
    }

    for cid, sub in _subscribers.items():
        for alert in sub.get("price_alerts", []):
            if not alert.get("triggered"):
                symbols_needed.add(alert["symbol"])

    if not symbols_needed:
        return

    # Map to CoinGecko IDs
    cg_ids = [cg_map.get(s, s.lower()) for s in symbols_needed]

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get("https://api.coingecko.com/api/v3/simple/price",
                params={"ids": ",".join(cg_ids), "vs_currencies": "usd"})
            prices = r.json()
    except:
        return

    # Reverse map: cg_id -> symbol
    rev_map = {v: k for k, v in cg_map.items()}
    symbol_prices = {}
    for cg_id, pdata in prices.items():
        sym = rev_map.get(cg_id, cg_id.upper())
        symbol_prices[sym] = pdata.get("usd", 0)

    # Check alerts
    for cid, sub in _subscribers.items():
        if not sub.get("alerts_enabled", True):
            continue
        for alert in sub.get("price_alerts", []):
            if alert.get("triggered"):
                continue
            sym = alert["symbol"]
            current = symbol_prices.get(sym, 0)
            if not current:
                continue
            target = alert["target"]
            direction = alert.get("direction", "above")

            triggered = False
            if direction == "above" and current >= target:
                triggered = True
            elif direction == "below" and current <= target:
                triggered = True

            if triggered:
                alert["triggered"] = True
                dir_text = "піднялась вище" if direction == "above" else "впала нижче"
                await send_message(int(cid),
                    f"🔔🔔🔔 <b>АЛЕРТ СПРАЦЮВАВ!</b>\n\n"
                    f"<b>{sym}</b> {dir_text} <b>${target:,.2f}</b>\n"
                    f"Поточна ціна: <b>${current:,.2f}</b>\n\n"
                    f"🕐 {datetime.now(timezone.utc).strftime('%H:%M UTC')}")

    _save_data()


# ══════════════════════════════════════════
# BROADCAST — send signals to all subscribers
# ══════════════════════════════════════════

async def broadcast_hunt_results(hunt_result: dict):
    """Send hunt results to subscribers who opted in"""
    count = hunt_result.get("hunted_count", 0)
    if count == 0:
        return

    for cid, sub in _subscribers.items():
        if not sub.get("notify_hunt", False):
            continue
        try:
            await send_message(int(cid),
                f"🔭 <b>Полювання завершено!</b>\n\n"
                f"Вполювано: <b>{count}</b> активів\n"
                f"🕐 {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
        except:
            pass


async def broadcast_signal(symbol: str, signal_type: str, reason: str, price: float):
    """Send a buy/sell signal to all subscribers"""
    emoji = "📈" if signal_type == "buy" else "📉" if signal_type == "sell" else "⏳"
    label = "ВХІД" if signal_type == "buy" else "ВИХІД" if signal_type == "sell" else "ОЧІКУВАННЯ"

    for cid, sub in _subscribers.items():
        if not sub.get("notify_signals", True):
            continue
        try:
            await send_message(int(cid),
                f"{emoji} <b>СИГНАЛ: {label}</b>\n\n"
                f"<b>{symbol}</b> — ${price:,.2f}\n"
                f"└ {reason}\n\n"
                f"⚠️ <i>Не фінансова порада</i>")
        except:
            pass


# ══════════════════════════════════════════
# POLLING LOOP
# ══════════════════════════════════════════

_polling_offset = 0

async def poll_updates(db_session_factory=None):
    """Long-poll for Telegram updates"""
    global _polling_offset
    try:
        result = await tg_request("getUpdates", {
            "offset": _polling_offset,
            "timeout": 30,
            "allowed_updates": ["message", "callback_query"]
        })
        if result.get("ok"):
            for update in result.get("result", []):
                _polling_offset = update["update_id"] + 1
                try:
                    await process_update(update, db_session_factory)
                except Exception as e:
                    log.error(f"[TG] Update processing error: {e}")
    except Exception as e:
        log.error(f"[TG] Polling error: {e}")
        await asyncio.sleep(5)


async def run_bot(db_session_factory=None):
    """Main bot loop — polling + alert checking"""
    if not TELEGRAM_TOKEN:
        log.warning("[TG] TELEGRAM_BOT_TOKEN not set — bot disabled")
        return

    _load_data()
    log.info("[TG] Bot started (polling mode)")

    # Set bot commands
    await tg_request("setMyCommands", {"commands": [
        {"command": "start", "description": "Головне меню"},
        {"command": "prices", "description": "Топ криптовалют"},
        {"command": "signals", "description": "Інвестиційні сигнали"},
        {"command": "news", "description": "Крипто новини"},
        {"command": "alert", "description": "Ціновий алерт (BTC 100000)"},
        {"command": "my_alerts", "description": "Мої алерти"},
        {"command": "settings", "description": "Налаштування"},
        {"command": "help", "description": "Довідка"},
    ]})

    alert_check_interval = 60  # Check alerts every 60 seconds
    last_alert_check = 0

    while True:
        await poll_updates(db_session_factory)

        # Periodic alert check
        now = time.time()
        if now - last_alert_check > alert_check_interval:
            await check_price_alerts()
            last_alert_chec