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
    """Show payment instructions for tier"""
    prices = {"pro": 9.99, "vip": 29.99}
    price_usd = prices.get(tier, 9.99)

    # Get TON price
    ton_price = 3.0
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get("https://api.coingecko.com/api/v3/simple/price",
                                 params={"ids": "the-open-network", "vs_currencies": "usd"})
            data = r.json()
            ton_price = data.get("the-open-network", {}).get("usd", 3.0)
    except:
        pass

    amount_ton = round(price_usd / ton_price, 4)
    order_comment = f"OV-TG-{chat_id}-{tier}-{int(time.time())}"

    keyboard = {
        "inline_keyboard": [
            [{"text": "💎 Відкрити Tonkeeper", "url": f"https://app.tonkeeper.com/transfer/{TON_WALLET}?amount={int(amount_ton * 1e9)}&text={order_comment}"}],
            [{"text": "✅ Я оплатив", "callback_data": f"paid_{tier}_{order_comment}"}],
            [{"text": "🌐 Оплатити на сайті", "url": "https://dependable-tranquility-production-d86f.up.railway.app/?tab=plans"}],
            [{"text": "◀️ Назад", "callback_data": "back_premium"}],
        ]
    }

    await send_message(chat_id,
        f"💎 <b>Оплата {tier.upper()}</b>\n\n"
        f"💵 Сума: <b>${price_usd}</b>\n"
        f"💎 В TON: <b>{amount_ton} TON</b>\n"
        f"📊 Курс: 1 TON = ${ton_price:.2f}\n\n"
        f"📋 <b>Адреса:</b>\n"
        f"<code>{TON_WALLET}</code>\n\n"
        f"📝 <b>Коментар (обов'язково!):</b>\n"
        f"<code>{order_comment}</code>\n\n"
        f"⚠️ <b>Важливо:</b> додайте коментар при переказі!\n\n"
        f"Або оплатіть зручно на сайті (карта, Apple/Google Pay).",
        reply_markup=keyboard
    )

    # Notify admin
    if ADMIN_CHAT_ID:
        try:
            confirm_kb = {
                "inline_keyboard": [
                    [{"text": f"✅ Підтвердити {tier.upper()} для @{username}", "callback_data": f"admin_confirm_{chat_id}_{tier}"}],
                ]
            }
            await send_message(ADMIN_CHAT_ID,
                f"💰 <b>Запит на оплату!</b>\n\n"
                f"👤 @{username} (chat: {chat_id})\n"
                f"📋 План: {tier.upper()}\n"
                f"💵 ${price_usd} (~{amount_ton} TON)\n"
                f"🔑 <code>{order_comment}</code>",
                reply_markup=confirm_kb
            )
        except:
            pass


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

        if text.startswith("/start"):
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
            last_alert_check = now
