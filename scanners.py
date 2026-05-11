"""
Omni-Vision v0.5 — Промисловий Глобальний Сканер Ринків
Архітектура: asyncio.gather для паралельних запитів
Крипто: DexScreener + GeckoTerminal (нові пари, ліквідність, honeypot-фільтр)
Акції: yfinance S&P500 + Trending (RVOL аномалії)
Логування: rich (кольоровий вивід)
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
import yfinance as yf

# Спроба підключити rich для кольорового логування
try:
    from rich.logging import RichHandler
    from rich.console import Console
    console = Console()
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[RichHandler(console=console, rich_tracebacks=True)]
    )
except ImportError:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(name)s: %(message)s")

log = logging.getLogger("omni-scanner")

# ═══════════════════════════════════════════════════
# КОНФІГУРАЦІЯ
# ═══════════════════════════════════════════════════

MIN_LIQUIDITY_USD = 50_000       # Мінімум ліквідності для крипто-пар
MIN_VOLUME_24H = 10_000          # Мінімум 24г об'єму
VOLUME_CAPTURE_THRESHOLD = 100_000  # Поріг для автозахоплення ($100k/год)
RVOL_THRESHOLD = 2.5             # Relative Volume поріг для акцій (2.5x від середнього)
HTTP_TIMEOUT = 12.0              # Таймаут мережевих запитів
MAX_CONCURRENT = 10              # Макс. паралельних запитів

# Honeypot ознаки (підозрілі характеристики)
HONEYPOT_INDICATORS = {
    "buy_tax_threshold": 10,     # >10% податок на купівлю
    "sell_tax_threshold": 10,    # >10% податок на продаж
    "min_holders": 50,           # Менше 50 холдерів = підозріло
}

# Мережі для крипто-сканування
CRYPTO_CHAINS = ["solana", "eth", "base", "bsc", "arbitrum", "polygon", "avalanche"]

# S&P 500 основні + технологічні + крипто-суміжні (розширений список)
SP500_CORE = [
    "AAPL","MSFT","NVDA","GOOGL","AMZN","META","TSLA","BRK-B","JPM","V",
    "UNH","XOM","JNJ","WMT","MA","PG","HD","CVX","MRK","ABBV",
    "LLY","AVGO","PEP","KO","COST","TMO","MCD","CSCO","ACN","ABT",
    "DHR","NEE","TXN","PM","UPS","MS","RTX","AMGN","HON","LOW",
    "UNP","IBM","GE","CAT","BA","AXP","GS","BLK","SCHW","AMD",
    "NFLX","CRM","ORCL","INTC","QCOM","AMAT","ADM","MU","LRCX","KLAC",
    "COIN","MARA","RIOT","MSTR","PLTR","SOFI","SQ","PYPL","SHOP","UBER",
    "ABNB","SNOW","NET","CRWD","DDOG","ZS","OKTA","MDB","PANW","FTNT",
    "TSM","BABA","NIO","SE","GRAB","MELI","NU","PDD","JD","TCEHY",
    "ARM","SMCI","ON","DELL","HPE","SNPS","CDNS","ANSS","TER","MPWR",
]

# ═══════════════════════════════════════════════════
# КРИПТО СКАНЕР (DexScreener + GeckoTerminal)
# ═══════════════════════════════════════════════════

class CryptoDeepScanner:
    """
    Глибокий сканер крипто-ринку.
    - Пошук нових лістингів (New Pairs)
    - Топ-гейнери по всіх мережах
    - Фільтрація: ліквідність, об'єм, honeypot
    """

    DEX_API = "https://api.dexscreener.com"
    GECKO_API = "https://api.geckoterminal.com/api/v2"

    async def _fetch_json(self, client: httpx.AsyncClient, url: str, label: str = "") -> dict:
        """Безпечний GET-запит з логуванням."""
        try:
            resp = await client.get(url, headers={"Accept": "application/json"})
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            log.warning(f"[Крипто] HTTP помилка {e.response.status_code} для {label or url}")
            return {}
        except httpx.RequestError as e:
            log.warning(f"[Крипто] Мережева помилка для {label}: {e}")
            return {}
        except Exception as e:
            log.error(f"[Крипто] Невідома помилка для {label}: {e}")
            return {}

    def _is_honeypot(self, pair: dict) -> bool:
        """Перевірка на ознаки honeypot/скаму."""
        liq = float(pair.get("liquidity", {}).get("usd", 0) or 0)
        if liq < MIN_LIQUIDITY_USD:
            return True
        txns = pair.get("txns", {}).get("h24", {})
        buys = int(txns.get("buys", 0) or 0)
        sells = int(txns.get("sells", 0) or 0)
        if buys + sells < 20:
            return True
        if sells > 0 and buys / max(sells, 1) > 20:
            return True
        return False

    def _parse_dex_pair(self, pair: dict) -> Optional[dict]:
        """Парсинг пари з DexScreener у стандартний формат."""
        liq = float(pair.get("liquidity", {}).get("usd", 0) or 0)
        vol_24h = float(pair.get("volume", {}).get("h24", 0) or 0)
        if liq < MIN_LIQUIDITY_USD or vol_24h < MIN_VOLUME_24H:
            return None
        if self._is_honeypot(pair):
            return None
        price_change = pair.get("priceChange", {})
        return {
            "category": "CRYPTO",
            "symbol": pair.get("baseToken", {}).get("symbol", "???"),
            "name": pair.get("baseToken", {}).get("name", ""),
            "address": pair.get("baseToken", {}).get("address", ""),
            "price_usd": pair.get("priceUsd"),
            "change_5m": float(price_change.get("m5", 0) or 0),
            "change_1h": float(price_change.get("h1", 0) or 0),
            "change_24h": float(price_change.get("h24", 0) or 0),
            "volume_24h": vol_24h,
            "volume_1h": vol_24h / 24,
            "liquidity_usd": liq,
            "chain": pair.get("chainId", ""),
            "dex": pair.get("dexId", ""),
            "pair_created_at": pair.get("pairCreatedAt"),
            "pair_url": pair.get("url", ""),
            "honeypot": False,
            "source": "dexscreener",
            "found": True,
        }

    async def scan_new_pairs(self, client: httpx.AsyncClient) -> list:
        """Сканування нових лістингів через DexScreener."""
        log.info("[Крипто] Сканування нових пар...")
        results = []
        data = await self._fetch_json(
            client, f"{self.DEX_API}/token-profiles/latest/v1", "new_profiles")
        profiles = data if isinstance(data, list) else data.get("data", [])
        addresses_to_check = []
        for profile in profiles[:30]:
            addr = profile.get("tokenAddress") or profile.get("address")
            if addr:
                addresses_to_check.append(addr)
        if addresses_to_check:
            batch = ",".join(addresses_to_check[:30])
            pair_data = await self._fetch_json(
                client, f"{self.DEX_API}/latest/dex/tokens/{batch}", "batch_tokens")
            pairs = pair_data.get("pairs", [])
            for pair in pairs:
                parsed = self._parse_dex_pair(pair)
                if parsed:
                    parsed["is_new_listing"] = True
                    results.append(parsed)
        log.info(f"[Крипто] Нових пар після фільтрації: {len(results)}")
        return results

    async def scan_trending_chain(self, client: httpx.AsyncClient, chain: str) -> list:
        """Сканування трендових пулів на конкретній мережі через GeckoTerminal."""
        results = []
        data = await self._fetch_json(
            client, f"{self.GECKO_API}/networks/{chain}/trending_pools", f"trending_{chain}")
        pools = data.get("data", [])
        for pool in pools:
            attrs = pool.get("attributes", {})
            price_ch = attrs.get("price_change_percentage", {})
            vol_24h = float(attrs.get("volume_usd", {}).get("h24", 0) or 0)
            reserve = float(attrs.get("reserve_in_usd", 0) or 0)
            if reserve < MIN_LIQUIDITY_USD or vol_24h < MIN_VOLUME_24H:
                continue
            price = attrs.get("base_token_price_usd")
            name = attrs.get("name", "")
            results.append({
                "category": "CRYPTO",
                "symbol": name.split("/")[0].strip() if "/" in name else name[:20],
                "name": name,
                "address": attrs.get("address", ""),
                "price_usd": float(price) if price else None,
                "change_5m": float(price_ch.get("m5", 0) or 0),
                "change_1h": float(price_ch.get("h1", 0) or 0),
                "change_24h": float(price_ch.get("h24", 0) or 0),
                "volume_24h": vol_24h,
                "volume_1h": vol_24h / 24,
                "liquidity_usd": reserve,
                "chain": chain,
                "dex": attrs.get("dex_id", ""),
                "honeypot": False,
                "source": "geckoterminal",
                "found": True,
            })
        return results

    async def scan_top_gainers(self, client: httpx.AsyncClient) -> list:
        """Паралельне сканування топ-гейнерів по всіх мережах."""
        log.info(f"[Крипто] Сканування {len(CRYPTO_CHAINS)} мереж паралельно...")
        tasks = [self.scan_trending_chain(client, chain) for chain in CRYPTO_CHAINS]
        chain_results = await asyncio.gather(*tasks, return_exceptions=True)
        all_tokens = []
        for i, result in enumerate(chain_results):
            if isinstance(result, Exception):
                log.warning(f"[Крипто] Помилка мережі {CRYPTO_CHAINS[i]}: {result}")
                continue
            all_tokens.extend(result)
        all_tokens.sort(key=lambda x: x.get("change_24h", 0), reverse=True)
        log.info(f"[Крипто] Знайдено {len(all_tokens)} валідних токенів")
        return all_tokens[:50]

    async def get_token_price(self, token_address: str) -> dict:
        """Отримати ціну конкретного токена за адресою."""
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                data = await self._fetch_json(
                    client, f"{self.DEX_API}/latest/dex/tokens/{token_address}", "single_token")
                pairs = data.get("pairs", [])
                if not pairs:
                    return {"token_address": token_address, "found": False,
                            "error": "Токен не знайдено на DEX."}
                valid_pairs = [p for p in pairs if not self._is_honeypot(p)]
                if not valid_pairs:
                    return {"token_address": token_address, "found": False,
                            "error": "Токен виявлено, але всі пари мають ознаки honeypot."}
                best = max(valid_pairs, key=lambda p: float(
                    p.get("liquidity", {}).get("usd", 0) or 0))
                parsed = self._parse_dex_pair(best)
                if parsed:
                    parsed["token_address"] = token_address
                    return parsed
                return {"token_address": token_address, "found": False,
                        "error": "Не пройшов фільтрацію (ліквідність/об'єм)."}
        except Exception as e:
            return {"token_address": token_address, "found": False, "error": str(e)}

    async def full_scan(self, client: httpx.AsyncClient) -> list:
        """Повне сканування: нові пари + топ-гейнери."""
        new_pairs_task = self.scan_new_pairs(client)
        gainers_task = self.scan_top_gainers(client)
        new_pairs, gainers = await asyncio.gather(
            new_pairs_task, gainers_task, return_exceptions=True)
        results = []
        if isinstance(new_pairs, list):
            results.extend(new_pairs)
        if isinstance(gainers, list):
            results.extend(gainers)
        seen = set()
        unique = []
        for token in results:
            key = f"{token['symbol']}_{token.get('chain','')}_{token.get('address','')[:10]}"
            if key not in seen:
                seen.add(key)
                unique.append(token)
        return unique


# ═══════════════════════════════════════════════════
# ФОНДОВИЙ СКАНЕР (yfinance — S&P 500 + Trending)
# ═══════════════════════════════════════════════════

class GlobalStocksScanner:
    """
    Промисловий сканер акцій.
    - S&P 500 core (100 тікерів)
    - Relative Volume (RVOL) аномалії
    - Інсайдерські сигнали
    """

    def _scan_batch(self, tickers: list) -> list:
        """Сканування батчу тікерів через yfinance."""
        results = []
        for sym in tickers:
            try:
                stock = yf.Ticker(sym)
                info = stock.info or {}
                hist = stock.history(period="10d")

                price = info.get("currentPrice") or info.get("regularMarketPrice")
                prev_close = info.get("previousClose") or info.get("regularMarketPreviousClose")

                if not price:
                    continue

                change_pct = round((price - prev_close) / prev_close * 100, 2) \
                    if price and prev_close and prev_close > 0 else 0

                # RVOL — Relative Volume (поточний об'єм / середній за 10 днів)
                rvol = None
                vol_spike = False
                current_vol = info.get("volume") or 0
                if not hist.empty and len(hist) >= 3:
                    avg_vol = hist["Volume"].iloc[:-1].mean()
                    if avg_vol > 0:
                        rvol = round(current_vol / avg_vol, 2)
                        if rvol >= RVOL_THRESHOLD:
                            vol_spike = True

                # Інсайдерські транзакції
                insider_trades = []
                try:
                    ins = stock.insider_transactions
                    if ins is not None and not ins.empty:
                        for _, row in ins.head(5).iterrows():
                            insider_trades.append({
                                "insider": str(row.get("Insider", "")),
                                "action": str(row.get("Transaction", "")),
                                "shares": str(row.get("Shares", "")),
                                "date": str(row.get("Start Date", "")),
                            })
                except Exception:
                    pass

                volume_1h = current_vol / 6.5 if current_vol else 0

                results.append({
                    "symbol": sym,
                    "category": "STOCKS",
                    "found": True,
                    "name": info.get("shortName", sym),
                    "price_usd": price,
                    "change_pct": change_pct,
                    "market_cap": info.get("marketCap"),
                    "volume": current_vol,
                    "volume_1h": volume_1h,
                    "rvol": rvol,
                    "volume_spike": vol_spike,
                    "insider_trades": insider_trades,
                    "sector": info.get("sector"),
                    "source": "yfinance",
                })
            except Exception as e:
                log.debug(f"[Акції] Помилка {sym}: {e}")
                continue
        return results

    def hunt_stocks(self, ticker: str = None) -> dict:
        """Сканування одного тікера або повного списку."""
        tickers = [ticker] if ticker else SP500_CORE
        log.info(f"[Акції] Сканування {len(tickers)} тікерів...")
        results = self._scan_batch(tickers)
        log.info(f"[Акції] Успішно: {len(results)}/{len(tickers)}")
        return {"stocks": results, "total_scanned": len(tickers),
                "scanned_at": datetime.now(timezone.utc).isoformat()}

    def hunt_trending(self) -> dict:
        """Топ-гейнери та топ-лузери."""
        data = self.hunt_stocks()
        found = [s for s in data["stocks"] if s.get("found")]
        gainers = sorted(found, key=lambda x: x.get("change_pct", 0) or 0, reverse=True)
        losers = sorted(found, key=lambda x: x.get("change_pct", 0) or 0)
        anomalies = [s for s in found if s.get("volume_spike")]
        return {
            "top_gainers": gainers[:15],
            "top_losers": losers[:15],
            "volume_anomalies": anomalies,
            "total_scanned": len(found),
            "scanned_at": data["scanned_at"],
        }

    def get_stock(self, ticker: str) -> dict:
        data = self.hunt_stocks(ticker)
        return data["stocks"][0] if data["stocks"] else {"found": False}


# ═══════════════════════════════════════════════════
# КОПАЛИНИ (yfinance Futures)
# ═══════════════════════════════════════════════════

class CommoditiesScanner:
    COMMODITIES = {
        "GC=F": "Золото", "SI=F": "Срібло",
        "CL=F": "Нафта WTI", "BZ=F": "Нафта Brent",
        "NG=F": "Газ", "HG=F": "Мідь",
        "PL=F": "Платина", "ZW=F": "Пшениця",
        "ZC=F": "Кукурудза", "ZS=F": "Соя",
    }

    def hunt_commodities(self, symbol: str = None) -> dict:
        targets = {symbol: self.COMMODITIES.get(symbol, symbol)} if symbol \
            else self.COMMODITIES
        results = []
        log.info(f"[Копалини] Сканування {len(targets)} інструментів...")
        for sym, name in targets.items():
            try:
                hist = yf.Ticker(sym).history(period="5d")
                if hist.empty:
                    continue
                price = round(float(hist["Close"].iloc[-1]), 2)
                prev = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else price
                change_pct = round((price - prev) / prev * 100, 2) if prev > 0 else 0
                week_start = float(hist["Close"].iloc[0])
                week_change = round((price - week_start) / week_start * 100, 2) \
                    if week_start > 0 else 0
                results.append({
                    "symbol": sym, "category": "COMMODITIES", "found": True,
                    "name": name, "price_usd": price,
                    "change_pct": change_pct, "week_change_pct": week_change,
                    "source": "yfinance",
                })
            except Exception as e:
                log.debug(f"[Копалини] Помилка {sym}: {e}")
                continue
        return {"commodities": results, "scanned_at": datetime.now(timezone.utc).isoformat()}


# ═══════════════════════════════════════════════════
# ДЕТЕКТОР РИНКОВИХ ПЕРЕЛИВІВ
# ═══════════════════════════════════════════════════

class FlowDetector:
    """Виявлення міжринкових потоків капіталу."""

    def detect_flows(self, stocks_data: dict, commodities_data: dict,
                     crypto_change: Optional[float] = None) -> list:
        alerts = []
        now = datetime.now(timezone.utc).isoformat()

        stock_dumps = stock_pumps = vol_anomalies = 0
        for s in stocks_data.get("stocks", []):
            if not s.get("found"):
                continue
            ch = s.get("change_pct") or 0
            if ch < -3: stock_dumps += 1
            elif ch > 3: stock_pumps += 1
            if s.get("volume_spike"): vol_anomalies += 1

        gold_ch = oil_ch = 0
        for c in commodities_data.get("commodities", []):
            if not c.get("found"):
                continue
            if "GC=F" in c["symbol"]: gold_ch = c.get("change_pct", 0)
            if "CL=F" in c["symbol"]: oil_ch = c.get("change_pct", 0)

        if stock_dumps >= 5 and crypto_change and crypto_change > 5:
            alerts.append({"type": "MARKET_OVERFLOW", "severity": "HIGH",
                "message": f"РИНКОВИЙ ПЕРЕЛИВ: {stock_dumps} акцій падають, крипта +{crypto_change:.1f}%. Капітал тече в крипту.",
                "detected_at": now})
        if gold_ch > 2 and stock_dumps >= 3:
            alerts.append({"type": "FEAR_ROTATION", "severity": "HIGH",
                "message": f"СТРАХОВИЙ РЕЖИМ: Золото +{gold_ch:.1f}%, {stock_dumps} акцій падають.",
                "detected_at": now})
        if oil_ch > 4:
            alerts.append({"type": "INFLATION_SIGNAL", "severity": "MEDIUM",
                "message": f"СИГНАЛ ІНФЛЯЦІЇ: Нафта +{oil_ch:.1f}%.",
                "detected_at": now})
        if vol_anomalies >= 5:
            alerts.append({"type": "MASS_VOLUME_SPIKE", "severity": "HIGH",
                "message": f"МАСОВИЙ СПЛЕСК ОБ'ЄМУ: {vol_anomalies} акцій з RVOL > {RVOL_THRESHOLD}x. Щось великe відбувається.",
                "detected_at": now})
        if stock_pumps >= 8 and gold_ch > 1 and (crypto_change or 0) > 3:
            alerts.append({"type": "EUPHORIA_WARNING", "severity": "MEDIUM",
                "message": "ЕЙФОРІЯ: Все росте одночасно — сигнал корекції.",
                "detected_at": now})
        if not alerts:
            alerts.append({"type": "STABLE", "severity": "LOW",
                "message": "Ринки стабільні. Значних переливів не виявлено.",
                "detected_at": now})
        return alerts


# ═══════════════════════════════════════════════════
# ГЛОБАЛЬНИЙ МИСЛИВЕЦЬ (об'єднує все)
# ═══════════════════════════════════════════════════

class GlobalHunter:
    """
    Автономний мисливець. Паралельно сканує всі ринки,
    автоматично захоплює активи з об'ємом > $100k/год.
    """

    def __init__(self, crypto: CryptoDeepScanner, stocks: GlobalStocksScanner,
                 commodities: CommoditiesScanner):
        self.crypto = crypto
        self.stocks = stocks
        self.commodities = commodities
        self.last_hunt = None
        self.total_hunted = 0

    async def hunt_all(self) -> dict:
        """Повне паралельне сканування всіх ринків."""
        log.info("=" * 50)
        log.info("[Мисливець] ПОЧАТОК ГЛОБАЛЬНОГО СКАНУВАННЯ")
        log.info("=" * 50)
        hunted = []
        start = datetime.now(timezone.utc)

        # Крипто — асинхронно
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                crypto_results = await self.crypto.full_scan(client)
                for token in crypto_results:
                    vol_1h = token.get("volume_1h", 0) or 0
                    if vol_1h >= VOLUME_CAPTURE_THRESHOLD:
                        token["auto_captured"] = True
                        token["capture_reason"] = (
                            f"Об'єм ${vol_1h:,.0f}/год > поріг ${VOLUME_CAPTURE_THRESHOLD:,.0f}"
                        )
                        if token.get("is_new_listing"):
                            token["capture_reason"] += " + НОВИЙ ЛІСТИНГ"
                        hunted.append(token)
        except Exception as e:
            log.error(f"[Мисливець] Крипто-скан помилка: {e}")

        # Акції — синхронно (yfinance не підтримує async)
        try:
            stocks_data = self.stocks.hunt_stocks()
            for stock in stocks_data.get("stocks", []):
                if not stock.get("found"):
                    continue
                vol_1h = stock.get("volume_1h", 0) or 0
                if vol_1h >= VOLUME_CAPTURE_THRESHOLD:
                    stock["auto_captured"] = True
                    reason = f"Об'єм ${vol_1h:,.0f}/год > поріг"
                    if stock.get("volume_spike"):
                        reason += f" + RVOL {stock.get('rvol', 0)}x"
                    if stock.get("insider_trades"):
                        reason += f" + {len(stock['insider_trades'])} інсайд. угод"
                    stock["capture_reason"] = reason
                    hunted.append(stock)
        except Exception as e:
            log.error(f"[Мисливець] Акції-скан помилка: {e}")

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        self.last_hunt = datetime.now(timezone.utc)
        self.total_hunted += len(hunted)

        log.info(f"[Мисливець] ЗАВЕРШЕНО за {elapsed:.1f}с")
        log.info(f"[Мисливець] Вполювано: {len(hunted)} активів")
        log.info("=" * 50)

        return {
            "hunted_count": len(hunted),
            "hunted": hunted,
            "threshold_usd_1h": VOLUME_CAPTURE_THRESHOLD,
            "scan_duration_sec": round(elapsed, 1),
            "scanned_at": self.last_hunt.isoformat(),
        }


# ═══════════════════════════════════════════════════
# СІНГЛТОНИ (імпортуються в main.py)
# ═══════════════════════════════════════════════════

crypto_scanner = CryptoDeepScanner()
stocks_scanner = GlobalStocksScanner()
commodities_scanner = CommoditiesScanner()
flow_detector = FlowDetector()
global_hunter = GlobalHunter(crypto_scanner, stocks_scanner, commodities_scanner)
