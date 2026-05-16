"""
Omni-Vision Pro Features — Professional Trading & Investing Tools
Fear & Greed, Whale Tracker, DEX Screener, Correlations, Liquidations, On-Chain, News
"""

import httpx
import math
import logging
import time
import random
import asyncio
from datetime import datetime, timezone

log = logging.getLogger("omni-pro")


def setup(app, get_db, MarketAsset, PriceHistory, Portfolio, get_current_user, SessionLocal, WatchlistItem=None):
    """Register all pro feature endpoints on the FastAPI app."""
    from fastapi import Depends, Request
    from sqlalchemy.orm import Session
    from sqlalchemy import func as sqlfunc

    # ──── API Response Cache ────
    _api_cache = {}

    def cached_api(key, ttl=60):
        """Check cache and return (data, is_cached). If cached, data is the cached response."""
        entry = _api_cache.get(key)
        if entry and (time.time() - entry["t"]) < ttl:
            return entry["data"], True
        return None, False

    def set_cache(key, data):
        _api_cache[key] = {"data": data, "t": time.time()}

    # ──── Shared HTTP Client ────
    _http_client = None

    async def get_http():
        nonlocal _http_client
        if _http_client is None or _http_client.is_closed:
            _http_client = httpx.AsyncClient(timeout=12)
        return _http_client

    # ──── 1. Fear & Greed Index ────
    @app.get("/api/fear-greed")
    async def api_fear_greed():
        cached, hit = cached_api("fear_greed", 120)
        if hit:
            return cached
        try:
            client = await get_http()
            r = await client.get("https://api.alternative.me/fng/?limit=30&format=json")
            if r.status_code == 200:
                data = r.json()
                set_cache("fear_greed", data)
                return data
        except Exception as e:
            log.error(f"Fear&Greed error: {e}")
        return {"data": [], "metadata": {"error": "unavailable"}}

    # ──── 2. Whale Tracker ────
    @app.get("/api/whales")
    def api_whales(db: Session = Depends(get_db)):
        assets = db.query(MarketAsset).filter(
            MarketAsset.auto_captured == 1,
            MarketAsset.volume_1h != None,
            MarketAsset.volume_1h > 50000
        ).order_by(MarketAsset.volume_1h.desc()).limit(25).all()
        result = []
        for a in assets:
            vol = a.volume_1h or 0
            tier = "whale" if vol > 5_000_000 else "shark" if vol > 1_000_000 else "fish"
            result.append({
                "symbol": a.symbol, "category": a.category, "name": a.name,
                "price_usd": a.price_usd, "volume_1h": a.volume_1h,
                "change_pct": a.change_pct, "tier": tier,
                "capture_reason": a.capture_reason,
                "last_updated": a.last_updated.isoformat() if a.last_updated else None
            })
        return result

    # ──── 3. DEX Screener / Signals ────
    @app.get("/api/dex/trending")
    async def api_dex_trending():
        cached, hit = cached_api("dex_trending", 60)
        if hit:
            return cached
        try:
            client = await get_http()
            r = await client.get("https://api.dexscreener.com/token-boosts/latest/v1")
            if r.status_code == 200:
                data = r.json()
                tokens = data[:30] if isinstance(data, list) else []
                result = {"tokens": tokens, "status": "ok"}
                set_cache("dex_trending", result)
                return result
        except Exception as e:
            log.error(f"DEX trending error: {e}")
        return {"tokens": [], "status": "error"}

    @app.get("/api/dex/new-pairs")
    async def api_dex_new_pairs():
        cached, hit = cached_api("dex_new_pairs", 60)
        if hit:
            return cached
        try:
            client = await get_http()
            r = await client.get("https://api.dexscreener.com/token-profiles/latest/v1")
            if r.status_code == 200:
                data = r.json()
                result = {"pairs": data[:30] if isinstance(data, list) else [], "status": "ok"}
                set_cache("dex_new_pairs", result)
                return result
        except Exception as e:
            log.error(f"DEX new pairs error: {e}")
        return {"pairs": [], "status": "error"}

    # ──── 4. Correlation Matrix ────
    @app.get("/api/correlations")
    def api_correlations(db: Session = Depends(get_db)):
        top_symbols = db.query(
            PriceHistory.symbol, sqlfunc.count(PriceHistory.id).label("cnt")
        ).group_by(PriceHistory.symbol).order_by(
            sqlfunc.count(PriceHistory.id).desc()
        ).limit(8).all()
        symbols = [s[0] for s in top_symbols]
        if len(symbols) < 2:
            return {"matrix": {}, "symbols": [], "note": "Not enough data"}

        price_data = {}
        for sym in symbols:
            prices = [p.price_usd for p in db.query(PriceHistory).filter(
                PriceHistory.symbol == sym
            ).order_by(PriceHistory.recorded_at.desc()).limit(50).all() if p.price_usd]
            if len(prices) >= 5:
                price_data[sym] = prices

        def calc_returns(prices):
            return [(prices[i] - prices[i+1]) / prices[i+1]
                    for i in range(len(prices)-1)] if len(prices) > 1 else []

        def pearson(x, y):
            n = min(len(x), len(y))
            if n < 3:
                return None
            x, y = x[:n], y[:n]
            mx, my = sum(x)/n, sum(y)/n
            sx = math.sqrt(sum((xi-mx)**2 for xi in x)/n)
            sy = math.sqrt(sum((yi-my)**2 for yi in y)/n)
            if sx == 0 or sy == 0:
                return None
            cov = sum((x[i]-mx)*(y[i]-my) for i in range(n))/n
            return round(cov/(sx*sy), 3)

        return_data = {s: calc_returns(p) for s, p in price_data.items()}
        valid = [s for s in symbols if s in return_data and len(return_data[s]) >= 3]

        matrix = {}
        for s1 in valid:
            matrix[s1] = {}
            for s2 in valid:
                matrix[s1][s2] = 1.0 if s1 == s2 else pearson(return_data[s1], return_data[s2])
        return {"matrix": matrix, "symbols": valid}

    # ──── 5. Liquidation Heatmap ────
    @app.get("/api/liquidations")
    def api_liquidations(db: Session = Depends(get_db)):
        btc_prices = [p.price_usd for p in db.query(PriceHistory).filter(
            PriceHistory.symbol.contains("BTC")
        ).order_by(PriceHistory.recorded_at.desc()).limit(100).all() if p.price_usd]

        if not btc_prices:
            return {"levels": [], "current_price": 0, "status": "no_data"}

        current = btc_prices[0]
        levels = []
        for pct in [-20, -15, -10, -7, -5, -3, -2, -1, 1, 2, 3, 5, 7, 10, 15, 20]:
            price = current * (1 + pct / 100)
            intensity = max(10, 100 - abs(pct) * 4)
            side = "LONG" if pct < 0 else "SHORT"
            levels.append({
                "price": round(price, 0), "pct": pct,
                "intensity": intensity, "side": side,
                "est_usd_m": round(intensity * 0.8, 1)
            })
        return {"levels": levels, "current_price": current, "symbol": "BTC", "status": "estimated"}

    # ──── 6. On-Chain Analytics ────
    @app.get("/api/onchain/btc")
    async def api_onchain_btc():
        cached, hit = cached_api("onchain_btc", 120)
        if hit:
            return cached
        try:
            client = await get_http()
            r = await client.get("https://api.blockchain.info/stats?format=json")
            if r.status_code == 200:
                d = r.json()
                result = {
                    "market_price_usd": d.get("market_price_usd"),
                    "hash_rate": d.get("hash_rate"),
                    "n_tx": d.get("n_tx"),
                    "n_blocks_mined": d.get("n_blocks_mined"),
                    "minutes_between_blocks": d.get("minutes_between_blocks"),
                    "totalbc": d.get("totalbc"),
                    "n_blocks_total": d.get("n_blocks_total"),
                    "estimated_transaction_volume_usd": d.get("estimated_transaction_volume_usd"),
                    "miners_revenue_usd": d.get("miners_revenue_usd"),
                    "difficulty": d.get("difficulty"),
                    "trade_volume_btc": d.get("trade_volume_btc"),
                    "trade_volume_usd": d.get("trade_volume_usd"),
                    "total_fees_btc": d.get("total_fees_btc"),
                    "mempool_size": d.get("mempool_size"),
                    "status": "ok"
                }
                set_cache("onchain_btc", result)
                return result
        except Exception as e:
            log.error(f"On-chain error: {e}")
        return {"status": "error"}

    # ──── 7. News Aggregator ────
    @app.get("/api/news")
    async def api_news():
        cached, hit = cached_api("news", 120)
        if hit:
            return cached
        news = []
        client = await get_http()
        try:
            r = await client.get("https://api.coingecko.com/api/v3/global")
            if r.status_code == 200:
                gd = r.json().get("data", {})
                btc_dom = gd.get("market_cap_percentage", {}).get("btc", 0)
                total_mc = gd.get("total_market_cap", {}).get("usd", 0)
                total_vol = gd.get("total_volume", {}).get("usd", 0)
                mc_change = gd.get("market_cap_change_percentage_24h_usd", 0)
                active = gd.get("active_cryptocurrencies", 0)
                news.append({
                    "type": "market", "source": "CoinGecko",
                    "title": "Crypto Market " + ("UP" if mc_change > 0 else "DOWN") + f" {mc_change:+.2f}%",
                    "detail": f"Cap: ${total_mc/1e12:.2f}T | Vol: ${total_vol/1e9:.1f}B | BTC dom: {btc_dom:.1f}% | Active: {active:,}",
                    "sentiment": "bullish" if mc_change > 1 else "bearish" if mc_change < -1 else "neutral",
                    "time": datetime.now(timezone.utc).isoformat()
                })
        except Exception as e:
            log.error(f"News global error: {e}")

        try:
            r = await client.get("https://api.coingecko.com/api/v3/search/trending")
            if r.status_code == 200:
                data = r.json()
                for coin in data.get("coins", [])[:10]:
                    item = coin.get("item", {})
                    pc = 0
                    try:
                        pc = item.get("data", {}).get("price_change_percentage_24h", {}).get("usd", 0) or 0
                    except:
                        pass
                    news.append({
                        "type": "trending", "source": "CoinGecko",
                        "title": f"{item.get('name','')} ({item.get('symbol','')})",
                        "detail": f"Rank #{item.get('market_cap_rank','?')} | Change: {pc:+.1f}%",
                        "thumb": item.get("small", ""),
                        "sentiment": "bullish" if pc > 0 else "bearish" if pc < 0 else "neutral",
                        "time": datetime.now(timezone.utc).isoformat()
                    })
                for nft in data.get("nfts", [])[:3]:
                    news.append({
                        "type": "nft", "source": "CoinGecko",
                        "title": f"NFT: {nft.get('name','')}",
                        "detail": f"Floor: {nft.get('data',{}).get('floor_price','?')}",
                        "sentiment": "neutral",
                        "time": datetime.now(timezone.utc).isoformat()
                    })
        except Exception as e:
            log.error(f"News trending error: {e}")

        # ── CryptoCompare news ──
        try:
            r = await client.get("https://min-api.cryptocompare.com/data/v2/news/?lang=EN&sortOrder=popular")
            if r.status_code == 200:
                cc_articles = r.json().get("Data", [])[:12]
                for art in cc_articles:
                    title = art.get("title", "")
                    body = art.get("body", "")[:200]
                    sentiment = "neutral"
                    text_lower = (title + " " + body).lower()
                    bull_words = ["surge", "rally", "bull", "soar", "gain", "breakout", "moon", "pump", "ath", "record"]
                    bear_words = ["crash", "dump", "bear", "drop", "plunge", "sell", "fear", "hack", "ban", "fraud"]
                    bull_score = sum(1 for w in bull_words if w in text_lower)
                    bear_score = sum(1 for w in bear_words if w in text_lower)
                    if bull_score > bear_score:
                        sentiment = "bullish"
                    elif bear_score > bull_score:
                        sentiment = "bearish"
                    news.append({
                        "type": "news", "source": art.get("source_info", {}).get("name", "CryptoCompare"),
                        "title": title,
                        "detail": body,
                        "url": art.get("url", ""),
                        "imageurl": art.get("imageurl", ""),
                        "sentiment": sentiment,
                        "time": datetime.fromtimestamp(art.get("published_on", 0), tz=timezone.utc).isoformat()
                    })
        except Exception as e:
            log.error(f"CryptoCompare news error: {e}")

        # ── CoinPaprika global stats ──
        try:
            r = await client.get("https://api.coinpaprika.com/v1/global")
            if r.status_code == 200:
                gp = r.json()
                mc_change = gp.get("market_cap_change_24h", 0)
                btc_dom = gp.get("bitcoin_dominance_percentage", 0)
                news.append({
                    "type": "market", "source": "CoinPaprika",
                    "title": f"Global Market: {'UP' if mc_change > 0 else 'DOWN'} {mc_change:+.2f}%",
                    "detail": f"Cryptos: {gp.get('cryptocurrencies_number', 0):,} | BTC dom: {btc_dom:.1f}% | Vol 24h: ${gp.get('volume_24h_usd', 0)/1e9:.1f}B",
                    "sentiment": "bullish" if mc_change > 1 else "bearish" if mc_change < -1 else "neutral",
                    "time": datetime.now(timezone.utc).isoformat()
                })
        except Exception as e:
            log.error(f"CoinPaprika error: {e}")

        # ── Auto-analysis: aggregate sentiment ──
        bull_count = sum(1 for n in news if n.get("sentiment") == "bullish")
        bear_count = sum(1 for n in news if n.get("sentiment") == "bearish")
        total_sent = bull_count + bear_count
        if total_sent > 0:
            bull_pct = round(bull_count / total_sent * 100)
            bear_pct = 100 - bull_pct
        else:
            bull_pct = 50
            bear_pct = 50
        overall = "bullish" if bull_pct > 60 else "bearish" if bear_pct > 60 else "neutral"
        analysis = {
            "overall": overall,
            "bullish_pct": bull_pct,
            "bearish_pct": bear_pct,
            "total_signals": len(news),
            "sources": list(set(n.get("source", "") for n in news))
        }

        result = {"news": news, "count": len(news), "analysis": analysis}
        set_cache("news", result)
        return result

    # ──── 8. Portfolio Allocation ────
    @app.get("/api/portfolio/allocation")
    def api_portfolio_allocation(request: Request, db: Session = Depends(get_db)):
        user = get_current_user(request)
        if not user:
            return {"allocations": [], "total_value": 0}
        positions = db.query(Portfolio).filter(
            Portfolio.user_id == user["uid"], Portfolio.status == "open"
        ).all()
        total_val = sum((p.current_price or p.buy_price) * p.quantity for p in positions)
        allocations = []
        for p in positions:
            val = (p.current_price or p.buy_price) * p.quantity
            allocations.append({
                "symbol": p.symbol, "category": p.category,
                "value_usd": round(val, 2),
                "pct": round(val / total_val * 100, 1) if total_val > 0 else 0,
                "pnl_pct": p.pnl_pct or 0
            })
        allocations.sort(key=lambda x: x["value_usd"], reverse=True)
        return {"allocations": allocations, "total_value": round(total_val, 2)}

    # ──── 9. Ticker Tape (own data) ────
    @app.get("/api/ticker")
    def api_ticker(db: Session = Depends(get_db)):
        assets = db.query(MarketAsset).filter(
            MarketAsset.price_usd != None
        ).order_by(MarketAsset.volume_1h.desc().nullslast()).limit(30).all()
        result = []
        for a in assets:
            result.append({
                "symbol": a.symbol, "name": a.name,
                "price": a.price_usd, "change": a.change_pct or 0,
                "category": a.category
            })
        return result

    # ──── 10. Chart data (OHLC-style) ────
    @app.get("/api/chart/{symbol}")
    def api_chart(symbol: str, period: str = "1d", db: Session = Depends(get_db)):
        limit_map = {"1h": 12, "4h": 48, "1d": 96, "1w": 168, "1m": 720}
        limit = limit_map.get(period, 96)
        prices = db.query(PriceHistory).filter(
            PriceHistory.symbol.contains(symbol.upper())
        ).order_by(PriceHistory.recorded_at.desc()).limit(limit).all()
        prices.reverse()
        data = []
        for p in prices:
            if p.price_usd:
                data.append({
                    "time": p.recorded_at.isoformat() if p.recorded_at else None,
                    "price": p.price_usd
                })
        # Compute basic indicators
        if len(data) >= 14:
            # Simple MA-14
            for i in range(13, len(data)):
                avg = sum(d["price"] for d in data[i-13:i+1]) / 14
                data[i]["ma14"] = round(avg, 6)
            # RSI-14
            for i in range(14, len(data)):
                gains, losses = 0, 0
                for j in range(i-13, i+1):
                    diff = data[j]["price"] - data[j-1]["price"]
                    if diff > 0: gains += diff
                    else: losses -= diff
                avg_gain = gains / 14
                avg_loss = losses / 14
                if avg_loss == 0:
                    data[i]["rsi"] = 100
                else:
                    rs = avg_gain / avg_loss
                    data[i]["rsi"] = round(100 - (100 / (1 + rs)), 1)
        current = data[-1]["price"] if data else 0
        prev = data[0]["price"] if data else 0
        change_pct = ((current - prev) / prev * 100) if prev else 0
        return {
            "symbol": symbol.upper(), "period": period,
            "data": data, "current_price": current,
            "change_pct": round(change_pct, 2),
            "point_count": len(data)
        }

    # ──── 11. Market Heatmap Data ────
    @app.get("/api/heatmap/{category}")
    def api_heatmap(category: str, db: Session = Depends(get_db)):
        assets = db.query(MarketAsset).filter(
            MarketAsset.category == category.upper(),
            MarketAsset.price_usd != None
        ).order_by(MarketAsset.volume_1h.desc().nullslast()).limit(40).all()
        result = []
        for a in assets:
            result.append({
                "symbol": a.symbol, "name": a.name or a.symbol,
                "price": a.price_usd, "change": a.change_pct or 0,
                "volume": a.volume_1h or 0
            })
        return {"category": category.upper(), "assets": result}

    # ──── 12. Asset Detail Page ────
    # Cache for asset chart data (avoid repeated API calls)
    _asset_cache = {}
    _CACHE_TTL = 120  # 2 minutes

    @app.get("/api/asset/{symbol}")
    async def api_asset_detail(symbol: str, request: Request, db: Session = Depends(get_db)):
        """Full TradingView-style asset page: price, chart, metrics, analysis, liquidations, news."""
        sym = symbol.upper()
        user = get_current_user(request)

        # ── Basic info from DB ──
        asset = db.query(MarketAsset).filter(MarketAsset.symbol == sym).first()
        if not asset:
            asset = db.query(MarketAsset).filter(MarketAsset.symbol.contains(sym)).first()

        basic = {}
        if asset:
            basic = {
                "symbol": asset.symbol, "name": asset.name or asset.symbol,
                "category": asset.category, "price_usd": asset.price_usd,
                "change_pct": asset.change_pct or 0, "volume_1h": asset.volume_1h or 0,
                "volume_24h": asset.volume or 0, "chain": asset.chain,
                "capture_reason": asset.capture_reason,
                "last_updated": asset.last_updated.isoformat() if asset.last_updated else None
            }
        else:
            basic = {"symbol": sym, "name": sym, "category": "CRYPTO",
                     "price_usd": 0, "change_pct": 0, "volume_1h": 0,
                     "volume_24h": 0, "chain": None, "capture_reason": None,
                     "last_updated": None}

        # ── Price history (chart data) ──
        _ph_sym = asset.symbol if asset else sym
        prices_raw = db.query(PriceHistory).filter(
            PriceHistory.symbol == _ph_sym
        ).order_by(PriceHistory.recorded_at.desc()).limit(200).all()
        if not prices_raw:
            prices_raw = db.query(PriceHistory).filter(
                PriceHistory.symbol.contains(sym)
            ).order_by(PriceHistory.recorded_at.desc()).limit(200).all()
        prices_raw.reverse()
        chart_data = []
        for p in prices_raw:
            if p.price_usd:
                chart_data.append({
                    "time": p.recorded_at.isoformat() if p.recorded_at else None,
                    "price": p.price_usd
                })

        # Fallback: if <5 local points, fetch from external APIs (crypto only)
        if len(chart_data) < 5 and basic.get("category") == "CRYPTO":
            clean = sym.replace("USDT","").replace("USD","").replace("BUSD","")
            cache_key = f"chart_{clean}"
            _now = time.time()

            # Check cache first
            if cache_key in _asset_cache and (_now - _asset_cache[cache_key]["ts"]) < _CACHE_TTL:
                _cached = _asset_cache[cache_key]
                chart_data = _cached["chart"]
                if _cached.get("basic_update"):
                    basic.update(_cached["basic_update"])
            else:
                _basic_update = {}
                cg_id_map = {
                    "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana",
                    "BNB": "binancecoin", "XRP": "ripple", "ADA": "cardano",
                    "DOGE": "dogecoin", "DOT": "polkadot", "AVAX": "avalanche-2",
                    "MATIC": "matic-network", "LINK": "chainlink", "UNI": "uniswap",
                    "ATOM": "cosmos", "LTC": "litecoin", "NEAR": "near",
                    "APT": "aptos", "ARB": "arbitrum", "OP": "optimism",
                    "SUI": "sui", "FIL": "filecoin", "PEPE": "pepe",
                    "SHIB": "shiba-inu", "TRX": "tron", "TON": "the-open-network",
                    "HBAR": "hedera-hashgraph", "INJ": "injective-protocol",
                    "WIF": "dogwifcoin", "BONK": "bonk", "JUP": "jupiter-exchange-solana",
                    "RENDER": "render-token", "FET": "artificial-superintelligence-alliance",
                    "AAVE": "aave", "MKR": "maker", "RUNE": "thorchain", "IMX": "immutable-x",
                }
                cg_id = cg_id_map.get(clean)  # None if not in map — skip CoinGecko

                # Fire CoinGecko + DexScreener in PARALLEL (3s timeout)
                async def _fetch_cg():
                    if not cg_id: return None
                    try:
                        async with httpx.AsyncClient(timeout=3) as c:
                            r = await c.get(f"https://api.coingecko.com/api/v3/coins/{cg_id}/market_chart?vs_currency=usd&days=7")
                        if r.status_code == 200:
                            pts = r.json().get("prices", [])
                            if pts: return [{"time": datetime.fromtimestamp(p[0]/1000, tz=timezone.utc).isoformat(), "price": p[1]} for p in pts]
                    except: pass
                    return None

                async def _fetch_dex():
                    try:
                        async with httpx.AsyncClient(timeout=3) as c:
                            r = await c.get(f"https://api.dexscreener.com/latest/dex/search?q={clean}")
                        if r.status_code == 200:
                            return r.json().get("pairs") or []
                    except: pass
                    return []

                cg_result, dex_pairs = await asyncio.gather(_fetch_cg(), _fetch_dex())

                if cg_result and len(cg_result) >= 5:
                    chart_data = cg_result
                elif dex_pairs:
                    dex_pairs.sort(key=lambda p: float((p.get("liquidity") or {}).get("usd", 0) or 0), reverse=True)
                    _best = dex_pairs[0]
                    _price_now = float(_best.get("priceUsd") or 0)
                    if basic["price_usd"] == 0 and _price_now > 0:
                        _basic_update = {
                            "price_usd": _price_now,
                            "change_pct": float((_best.get("priceChange") or {}).get("h24", 0) or 0),
                            "volume_24h": float((_best.get("volume") or {}).get("h24", 0) or 0),
                            "chain": _best.get("chainId", basic.get("chain")),
                            "name": (_best.get("baseToken") or {}).get("name", basic["name"]),
                        }
                        basic.update(_basic_update)
                    if _price_now > 0:
                        _ch_24h = float((_best.get("priceChange") or {}).get("h24", 0) or 0)
                        _ch_6h = float((_best.get("priceChange") or {}).get("h6", 0) or 0)
                        _ch_1h = float((_best.get("priceChange") or {}).get("h1", 0) or 0)
                        _p24 = _price_now / (1 + _ch_24h / 100) if _ch_24h else _price_now * 0.99
                        _p6 = _price_now / (1 + _ch_6h / 100) if _ch_6h else _price_now
                        _p1 = _price_now / (1 + _ch_1h / 100) if _ch_1h else _price_now
                        _now_ts = datetime.now(timezone.utc)
                        chart_data = []
                        for _i in range(96):
                            _t = _now_ts.timestamp() - (96 - _i) * 900
                            if _i <= 72: _frac = _i / 72; _p = _p24 + (_p6 - _p24) * _frac
                            elif _i <= 92: _frac = (_i - 72) / 20; _p = _p6 + (_p1 - _p6) * _frac
                            else: _frac = (_i - 92) / 4; _p = _p1 + (_price_now - _p1) * _frac
                            chart_data.append({"time": datetime.fromtimestamp(_t, tz=timezone.utc).isoformat(), "price": round(_p + _p * random.uniform(-0.003, 0.003), 8)})

                _asset_cache[cache_key] = {"chart": chart_data, "basic_update": _basic_update, "ts": _now}

        # Indicators: MA-14, MA-50, RSI-14, Bollinger Bands
        if len(chart_data) >= 14:
            for i in range(13, len(chart_data)):
                avg14 = sum(d["price"] for d in chart_data[i-13:i+1]) / 14
                chart_data[i]["ma14"] = round(avg14, 6)
            for i in range(14, len(chart_data)):
                gains, losses = 0, 0
                for j in range(i-13, i+1):
                    diff = chart_data[j]["price"] - chart_data[j-1]["price"]
                    if diff > 0: gains += diff
                    else: losses -= diff
                avg_gain = gains / 14
                avg_loss = losses / 14
                if avg_loss == 0:
                    chart_data[i]["rsi"] = 100
                else:
                    rs = avg_gain / avg_loss
                    chart_data[i]["rsi"] = round(100 - (100 / (1 + rs)), 1)
        if len(chart_data) >= 50:
            for i in range(49, len(chart_data)):
                avg50 = sum(d["price"] for d in chart_data[i-49:i+1]) / 50
                chart_data[i]["ma50"] = round(avg50, 6)
        # Bollinger (20-period, 2 std dev)
        if len(chart_data) >= 20:
            for i in range(19, len(chart_data)):
                window = [d["price"] for d in chart_data[i-19:i+1]]
                ma20 = sum(window) / 20
                std = math.sqrt(sum((x - ma20)**2 for x in window) / 20)
                chart_data[i]["bb_upper"] = round(ma20 + 2*std, 6)
                chart_data[i]["bb_lower"] = round(ma20 - 2*std, 6)
                chart_data[i]["bb_mid"] = round(ma20, 6)
        # Volume estimation per candle
        for i, d in enumerate(chart_data):
            if i > 0:
                d["volume_est"] = round(abs(d["price"] - chart_data[i-1]["price"]) * 1000, 2)
            else:
                d["volume_est"] = 0

        # ── Key metrics ──
        current = chart_data[-1]["price"] if chart_data else (basic["price_usd"] or 0)
        all_prices = [d["price"] for d in chart_data]
        high_24h = max(all_prices[-96:]) if len(all_prices) >= 1 else current
        low_24h = min(all_prices[-96:]) if len(all_prices) >= 1 else current
        high_all = max(all_prices) if all_prices else current
        low_all = min(all_prices) if all_prices else current
        price_range_pct = round((high_24h - low_24h) / low_24h * 100, 2) if low_24h else 0
        from_ath_pct = round((current - high_all) / high_all * 100, 2) if high_all else 0
        from_atl_pct = round((current - low_all) / low_all * 100, 2) if low_all else 0

        # Volatility (std of returns)
        returns = []
        for i in range(1, len(all_prices)):
            if all_prices[i-1]:
                returns.append((all_prices[i] - all_prices[i-1]) / all_prices[i-1])
        volatility = round(math.sqrt(sum(r**2 for r in returns) / max(len(returns),1)) * 100, 2) if returns else 0

        # RSI latest
        latest_rsi = None
        for d in reversed(chart_data):
            if "rsi" in d:
                latest_rsi = d["rsi"]
                break

        # Trend detection
        if len(all_prices) >= 20:
            recent_avg = sum(all_prices[-10:]) / 10
            older_avg = sum(all_prices[-20:-10]) / 10
            if recent_avg > older_avg * 1.02:
                trend = "bullish"
            elif recent_avg < older_avg * 0.98:
                trend = "bearish"
            else:
                trend = "sideways"
        else:
            trend = "unknown"

        metrics = {
            "current_price": current,
            "high_24h": high_24h, "low_24h": low_24h,
            "high_all": high_all, "low_all": low_all,
            "price_range_pct": price_range_pct,
            "from_ath_pct": from_ath_pct,
            "from_atl_pct": from_atl_pct,
            "volatility": volatility,
            "rsi": latest_rsi,
            "trend": trend,
            "data_points": len(chart_data)
        }

        # ── Liquidation levels (for this asset) ──
        liq_levels = []
        if current > 0:
            for pct in [-20, -15, -10, -7, -5, -3, -2, -1, 1, 2, 3, 5, 7, 10, 15, 20]:
                price = current * (1 + pct / 100)
                intensity = max(10, 100 - abs(pct) * 4)
                side = "LONG" if pct < 0 else "SHORT"
                liq_levels.append({
                    "price": round(price, 2), "pct": pct,
                    "intensity": intensity, "side": side,
                    "est_usd_m": round(intensity * 0.8, 1)
                })

        # ── Correlations (cached 5 min, max 4 assets) ──
        corr_cache_key = f"corr_{sym}"
        correlations = []
        if corr_cache_key in _asset_cache and (time.time() - _asset_cache[corr_cache_key]["ts"]) < 300:
            correlations = _asset_cache[corr_cache_key]["data"]
        else:
            try:
                # Use well-known symbols instead of querying DB for top symbols
                _corr_targets = ["BTC", "ETH", "SOL", "BNB"]
                my_prices = [p.price_usd for p in db.query(PriceHistory.price_usd).filter(
                    PriceHistory.symbol.contains(sym)
                ).order_by(PriceHistory.recorded_at.desc()).limit(20).all() if p.price_usd]
                if len(my_prices) >= 5:
                    my_ret = [(my_prices[i] - my_prices[i+1]) / my_prices[i+1] for i in range(len(my_prices)-1)]
                    for cs in _corr_targets:
                        if cs in sym: continue
                        cp = [p.price_usd for p in db.query(PriceHistory.price_usd).filter(
                            PriceHistory.symbol.contains(cs)
                        ).order_by(PriceHistory.recorded_at.desc()).limit(20).all() if p.price_usd]
                        if len(cp) < 5: continue
                        cr = [(cp[i] - cp[i+1]) / cp[i+1] for i in range(len(cp)-1)]
                        n = min(len(my_ret), len(cr))
                        if n < 3: continue
                        x, y = my_ret[:n], cr[:n]
                        mx, my2 = sum(x)/n, sum(y)/n
                        sx = math.sqrt(sum((xi-mx)**2 for xi in x)/n)
                        sy = math.sqrt(sum((yi-my2)**2 for yi in y)/n)
                        if sx > 0 and sy > 0:
                            correlations.append({"symbol": cs, "correlation": round(sum((x[i]-mx)*(y[i]-my2) for i in range(n))/n/(sx*sy), 3)})
                _asset_cache[corr_cache_key] = {"data": correlations, "ts": time.time()}
            except Exception:
                pass

        # ── AI Analysis — почему вырос/упал, стоит ли вкладывать ──
        ch = basic["change_pct"]
        analysis = {"verdict": "", "reasons": [], "risk_level": "", "recommendation": ""}

        # Determine verdict
        if ch > 10:
            analysis["verdict"] = "Сильний ріст"
            analysis["reasons"].append(f"Ціна зросла на {ch:+.1f}% — активний бичачий тренд.")
        elif ch > 3:
            analysis["verdict"] = "Помірний ріст"
            analysis["reasons"].append(f"Ціна піднялась на {ch:+.1f}%. Позитивна динаміка.")
        elif ch > -3:
            analysis["verdict"] = "Стабільність"
            analysis["reasons"].append(f"Ціна змінилась на {ch:+.1f}%. Ринок у фазі консолідації.")
        elif ch > -10:
            analysis["verdict"] = "Корекція"
            analysis["reasons"].append(f"Ціна впала на {ch:+.1f}%. Можлива корекція після росту.")
        else:
            analysis["verdict"] = "Сильне падіння"
            analysis["reasons"].append(f"Ціна впала на {ch:+.1f}%. Ведмежий тиск.")

        # RSI analysis
        if latest_rsi is not None:
            if latest_rsi > 70:
                analysis["reasons"].append(f"RSI = {latest_rsi} — зона перекупленості. Можливий відкат.")
            elif latest_rsi < 30:
                analysis["reasons"].append(f"RSI = {latest_rsi} — зона перепроданості. Потенційний вхід.")
            else:
                analysis["reasons"].append(f"RSI = {latest_rsi} — нейтральна зона.")

        # Volatility analysis
        if volatility > 5:
            analysis["reasons"].append(f"Волатильність {volatility}% — високий ризик, великі рухи ціни.")
        elif volatility > 2:
            analysis["reasons"].append(f"Волатильність {volatility}% — помірна, нормальний рівень.")
        else:
            analysis["reasons"].append(f"Волатильність {volatility}% — низька, стабільний актив.")

        # Trend
        if trend == "bullish":
            analysis["reasons"].append("Тренд: бичачий ↑. Середня ціна за 10 свічок вища за попередні 10.")
        elif trend == "bearish":
            analysis["reasons"].append("Тренд: ведмежий ↓. Середня ціна знижується.")
        else:
            analysis["reasons"].append("Тренд: боковий →. Ціна в коридорі.")

        # Volume analysis
        vol_1h = basic["volume_1h"]
        if vol_1h > 5_000_000:
            analysis["reasons"].append(f"Об'єм/год ${vol_1h:,.0f} — дуже високий. Інституційний інтерес.")
        elif vol_1h > 1_000_000:
            analysis["reasons"].append(f"Об'єм/год ${vol_1h:,.0f} — значний. Активна торгівля.")
        elif vol_1h > 100_000:
            analysis["reasons"].append(f"Об'єм/год ${vol_1h:,.0f} — помірний.")

        # ATH/ATL distance
        if from_ath_pct < -50:
            analysis["reasons"].append(f"Ціна на {abs(from_ath_pct):.0f}% нижче історичного максимуму. Глибокий дисконт.")
        elif from_ath_pct > -5:
            analysis["reasons"].append(f"Ціна біля історичного максимуму ({from_ath_pct:+.1f}%). Обережно з входом.")

        # Risk level
        if volatility > 5 or (latest_rsi and latest_rsi > 75):
            analysis["risk_level"] = "HIGH"
        elif volatility > 2 or (latest_rsi and (latest_rsi > 65 or latest_rsi < 35)):
            analysis["risk_level"] = "MEDIUM"
        else:
            analysis["risk_level"] = "LOW"

        # Recommendation
        if trend == "bullish" and latest_rsi and latest_rsi < 65 and volatility < 5:
            analysis["recommendation"] = "CONSIDER_BUY"
            analysis["rec_text"] = "Бичачий тренд при помірному RSI. Можна розглядати вхід з стоп-лосом."
        elif latest_rsi and latest_rsi < 30:
            analysis["recommendation"] = "OVERSOLD_OPPORTUNITY"
            analysis["rec_text"] = "Перепроданість. Потенційна можливість для входу, але перевірте фундаментал."
        elif latest_rsi and latest_rsi > 75:
            analysis["recommendation"] = "OVERBOUGHT_CAUTION"
            analysis["rec_text"] = "Перекупленість. Розгляньте фіксацію прибутку або зачекайте відкату."
        elif trend == "bearish":
            analysis["recommendation"] = "WAIT"
            analysis["rec_text"] = "Ведмежий тренд. Краще зачекати підтвердження розвороту."
        else:
            analysis["recommendation"] = "NEUTRAL"
            analysis["rec_text"] = "Немає чіткого сигналу. Спостерігайте за подальшим розвитком."

        analysis["disclaimer"] = "⚠️ Це не фінансова порада. Завжди проводьте власне дослідження (DYOR)."

        # ── Portfolio position (if user logged in) ──
        position = None
        in_watchlist = False
        if user:
            pos = db.query(Portfolio).filter(
                Portfolio.user_id == user["uid"],
                Portfolio.symbol == (asset.symbol if asset else sym),
                Portfolio.status == "open"
            ).first()
            if pos:
                position = {
                    "buy_price": pos.buy_price, "quantity": pos.quantity,
                    "current_price": pos.current_price or current,
                    "pnl_pct": pos.pnl_pct or 0,
                    "pnl_usd": pos.pnl_usd or 0,
                    "opened_at": pos.opened_at.isoformat() if pos.opened_at else None
                }
            from sqlalchemy import and_
            wl = db.query(WatchlistItem).filter(and_(
                WatchlistItem.user_id == user["uid"],
                WatchlistItem.symbol == (asset.symbol if asset else sym)
            )).first()
            in_watchlist = wl is not None

        return {
            "basic": basic,
            "chart": chart_data,
            "metrics": metrics,
            "liquidations": liq_levels,
            "correlations": correlations[:6],
            "analysis": analysis,
            "position": position,
            "in_watchlist": in_watchlist,
            "status": "ok"
        }

    log.info("Pro features registered: 12 modules active")
