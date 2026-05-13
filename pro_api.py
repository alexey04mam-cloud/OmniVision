"""
Omni-Vision Pro Features — Professional Trading & Investing Tools
Fear & Greed, Whale Tracker, DEX Screener, Correlations, Liquidations, On-Chain, News
"""

import httpx
import math
import logging
from datetime import datetime, timezone

log = logging.getLogger("omni-pro")


def setup(app, get_db, MarketAsset, PriceHistory, Portfolio, get_current_user, SessionLocal):
    """Register all pro feature endpoints on the FastAPI app."""
    from fastapi import Depends, Request
    from sqlalchemy.orm import Session
    from sqlalchemy import func as sqlfunc

    # ──── 1. Fear & Greed Index ────
    @app.get("/api/fear-greed")
    async def api_fear_greed():
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get("https://api.alternative.me/fng/?limit=30&format=json")
                if r.status_code == 200:
                    return r.json()
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
        try:
            async with httpx.AsyncClient(timeout=12) as client:
                r = await client.get("https://api.dexscreener.com/token-boosts/latest/v1")
                if r.status_code == 200:
                    data = r.json()
                    tokens = data[:30] if isinstance(data, list) else []
                    return {"tokens": tokens, "status": "ok"}
        except Exception as e:
            log.error(f"DEX trending error: {e}")
        return {"tokens": [], "status": "error"}

    @app.get("/api/dex/new-pairs")
    async def api_dex_new_pairs():
        try:
            async with httpx.AsyncClient(timeout=12) as client:
                r = await client.get("https://api.dexscreener.com/token-profiles/latest/v1")
                if r.status_code == 200:
                    data = r.json()
                    return {"pairs": data[:30] if isinstance(data, list) else [], "status": "ok"}
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
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get("https://api.blockchain.info/stats?format=json")
                if r.status_code == 200:
                    d = r.json()
                    return {
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
        except Exception as e:
            log.error(f"On-chain error: {e}")
        return {"status": "error"}

    # ──── 7. News Aggregator ────
    @app.get("/api/news")
    async def api_news():
        news = []
        try:
            async with httpx.AsyncClient(timeout=10) as client:
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
            async with httpx.AsyncClient(timeout=10) as client:
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

        return {"news": news, "count": len(news)}

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

    log.info("Pro features registered: 8 modules active")
