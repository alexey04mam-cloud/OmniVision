"""
Omni-Vision v1.1 — Secure Public Platform
"""

import os, json, asyncio, logging, csv, io, secrets, hashlib, re, time, html as html_escape
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
from contextlib import asynccontextmanager
from collections import defaultdict

from fastapi import FastAPI, Request, HTTPException, Depends, Query, Form, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, Float, func, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, Session, relationship
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

from scanners import (
    crypto_scanner, stocks_scanner, commodities_scanner,
    flow_detector, global_hunter,
)
import pro_api
import telegram_bot

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("omni-vision")

load_dotenv()
BOSS_KEY = os.getenv("BOSS_KEY")
if not BOSS_KEY:
    raise RuntimeError("BOSS_KEY not set in .env")

# Payment config
TON_WALLET = os.getenv("TON_WALLET", "UQAHQhdeLLuZerZxlVPiB-PVAFPhEzbvTX69qrpr_bT8TmV-")
CRYPTOBOT_TOKEN = os.getenv("CRYPTOBOT_TOKEN", "")  # From @CryptoBot -> Crypto Pay API
PAYMENT_PRICES = {"pro": 9.99, "vip": 29.99}

SECRET_KEY = os.getenv("SECRET_KEY")
if not SECRET_KEY:
    SECRET_KEY = secrets.token_hex(32)
    log.warning("SECRET_KEY not set — generating random key. Sessions will reset on restart!")
SESSION_MAX_AGE = int(os.getenv("SESSION_MAX_AGE", "86400"))

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./omni_vision.db")
HUNT_INTERVAL = int(os.getenv("HUNT_INTERVAL", "60"))
PORT = int(os.getenv("PORT", "8000"))
BASE_DIR = Path(__file__).resolve().parent
DEFAULT_LANG = "ukr"

# ──── Template Cache ────
_template_cache = {}
_template_mtime = {}

def read_template(name: str) -> str:
    path = BASE_DIR / name
    if not path.exists():
        return None
    mtime = path.stat().st_mtime
    if name not in _template_cache or _template_mtime.get(name) != mtime:
        _template_cache[name] = path.read_text(encoding="utf-8")
        _template_mtime[name] = mtime
    return _template_cache[name]

serializer = URLSafeTimedSerializer(SECRET_KEY)

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {},
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()

LANGS = {
    "ukr": {
        "greeting": "Ласкаво просимо до Omni-Vision!",
        "status_ok": "Радари активні. Мисливець працює.",
        "status_label": "Статус системи",
        "wallets": "Гаманці", "insights": "Інсайди", "assets": "Активи",
        "hunted": "Вполювано", "portfolio": "Портфель",
        "dashboard_title": "Глобальна панель керування",
        "boss_welcome": "Вітаю, Босе! Мисливець активний.",
    },
    "eng": {
        "greeting": "Welcome to Omni-Vision!",
        "status_ok": "Radars active. Hunter running.",
        "status_label": "System status",
        "wallets": "Wallets", "insights": "Insights", "assets": "Assets",
        "hunted": "Hunted", "portfolio": "Portfolio",
        "dashboard_title": "Global Dashboard",
        "boss_welcome": "Welcome, Boss! Hunter active.",
    },
    "rus": {
        "greeting": "Добро пожаловать в Omni-Vision!",
        "status_ok": "Радары активны. Охотник работает.",
        "status_label": "Статус системы",
        "wallets": "Кошельки", "insights": "Инсайды", "assets": "Активы",
        "hunted": "Добыча", "portfolio": "Портфель",
        "dashboard_title": "Глобальная панель управления",
        "boss_welcome": "Привет, Босс! Охотник активен.",
    },
}

def t(key, lang=DEFAULT_LANG):
    return LANGS.get(lang, LANGS[DEFAULT_LANG]).get(key, key)

# ──── Password hashing ────

def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 600_000).hex()
    return f"pbkdf2:{salt}:{h}"

def verify_password(password: str, hashed: str) -> bool:
    try:
        if hashed.startswith("pbkdf2:"):
            _, salt, h = hashed.split(":")
            return hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 600_000).hex() == h
        else:
            # Legacy SHA-256 fallback (auto-upgrade on next login)
            salt, h = hashed.split(":")
            return hashlib.sha256((salt + password).encode()).hexdigest() == h
    except Exception:
        return False

def upgrade_password_if_needed(user_obj, password: str):
    """Auto-upgrade legacy SHA-256 hashes to PBKDF2"""
    if user_obj.password_hash and not user_obj.password_hash.startswith("pbkdf2:"):
        user_obj.password_hash = hash_password(password)

# ──── Models ────

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(64), unique=True, nullable=False, index=True)
    email = Column(String(256), unique=True, nullable=False)
    password_hash = Column(String(256), nullable=False)
    is_admin = Column(Integer, default=0)
    risk_profile = Column(String(16), default="balanced")
    tier = Column(String(16), default="free")  # free / pro / vip
    tier_expires = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    wallets = relationship("Wallet", back_populates="owner")
    positions = relationship("Portfolio", back_populates="owner")

class Wallet(Base):
    __tablename__ = "wallets"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    address = Column(String(256), unique=True, nullable=False)
    blockchain = Column(String(64), nullable=False, default="ethereum")
    label = Column(String(128)); asset = Column(String(128))
    last_price = Column(Float); last_checked = Column(DateTime)
    added_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    owner = relationship("User", back_populates="wallets")

class MarketAsset(Base):
    __tablename__ = "market_assets"
    id = Column(Integer, primary_key=True, index=True)
    category = Column(String(32), nullable=False)
    symbol = Column(String(64), nullable=False)
    name = Column(String(256)); price_usd = Column(Float)
    change_pct = Column(Float); volume = Column(Float)
    volume_1h = Column(Float); chain = Column(String(64))
    auto_captured = Column(Integer, default=0)
    capture_reason = Column(Text); extra_data = Column(Text)
    last_updated = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class Insight(Base):
    __tablename__ = "insights"
    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String(32), nullable=False)
    source = Column(String(128)); summary = Column(Text, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class FlowAlert(Base):
    __tablename__ = "flow_alerts"
    id = Column(Integer, primary_key=True, index=True)
    alert_type = Column(String(64), nullable=False)
    severity = Column(String(16), nullable=False)
    message = Column(Text, nullable=False)
    detected_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class Portfolio(Base):
    __tablename__ = "portfolio"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    symbol = Column(String(64), nullable=False)
    category = Column(String(32), nullable=False, default="CRYPTO")
    buy_price = Column(Float, nullable=False)
    quantity = Column(Float, nullable=False, default=1.0)
    current_price = Column(Float)
    pnl_usd = Column(Float, default=0)
    pnl_pct = Column(Float, default=0)
    note = Column(Text)
    status = Column(String(16), default="open")
    opened_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    closed_at = Column(DateTime, nullable=True)
    owner = relationship("User", back_populates="positions")

class HuntHistory(Base):
    __tablename__ = "hunt_history"
    id = Column(Integer, primary_key=True, index=True)
    hunted_count = Column(Integer, default=0)
    crypto_count = Column(Integer, default=0)
    stocks_count = Column(Integer, default=0)
    scan_duration = Column(Float)
    scanned_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class PriceHistory(Base):
    __tablename__ = "price_history"
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(64), nullable=False, index=True)
    category = Column(String(32), nullable=False)
    price_usd = Column(Float)
    recorded_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class WatchlistItem(Base):
    __tablename__ = "watchlist"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    symbol = Column(String(64), nullable=False)
    category = Column(String(32), default="CRYPTO")
    target_price = Column(Float, nullable=True)
    direction = Column(String(8), default="above")
    note = Column(Text)
    triggered = Column(Integer, default=0)
    added_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class Payment(Base):
    __tablename__ = "payments"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    order_id = Column(String(64), unique=True, nullable=False, index=True)
    tier = Column(String(16), nullable=False)  # pro / vip
    amount_usd = Column(Float, nullable=False)
    amount_ton = Column(Float, nullable=True)
    method = Column(String(32), nullable=False)  # ton_direct / cryptobot / stars
    status = Column(String(16), default="pending")  # pending / paid / confirmed / expired
    tx_hash = Column(String(256), nullable=True)
    cryptobot_invoice_id = Column(String(128), nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    confirmed_at = Column(DateTime, nullable=True)
    owner = relationship("User")

Base.metadata.create_all(bind=engine)

# Auto-migrate: add tier columns if missing
try:
    from sqlalchemy import inspect as sa_inspect, text
    insp = sa_inspect(engine)
    cols = [c["name"] for c in insp.get_columns("users")]
    with engine.connect() as conn:
        if "tier" not in cols:
            conn.execute(text('ALTER TABLE users ADD COLUMN tier VARCHAR(16) DEFAULT "free"'))
            conn.commit()
            log.info("Migration: added 'tier' column to users")
        if "tier_expires" not in cols:
            conn.execute(text("ALTER TABLE users ADD COLUMN tier_expires DATETIME"))
            conn.commit()
            log.info("Migration: added 'tier_expires' column to users")
    # Create payments table if missing
    if "payments" not in sa_inspect(engine).get_table_names():
        Payment.__table__.create(engine)
        log.info("Migration: created 'payments' table")
except Exception as e:
    log.warning(f"Auto-migration skipped: {e}")

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

def verify_boss_key(request: Request):
    if request.headers.get("X-Boss-Key") != BOSS_KEY:
        raise HTTPException(status_code=403, detail="Доступ заборонено.")

# ──── Session auth ────

def create_session_token(user_id: int, username: str) -> str:
    return serializer.dumps({"uid": user_id, "user": username})

def get_current_user(request: Request, db: Session = None) -> dict:
    token = request.cookies.get("omni_session")
    if not token:
        return None
    try:
        data = serializer.loads(token, max_age=SESSION_MAX_AGE)
        return data
    except (BadSignature, SignatureExpired):
        return None

def require_user(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="not_authenticated")
    return user

class WalletCreate(BaseModel):
    address: str
    blockchain: str = "ethereum"
    label: Optional[str] = None
    asset: Optional[str] = None

class PortfolioCreate(BaseModel):
    symbol: str
    category: str = "CRYPTO"
    buy_price: float
    quantity: float = 1.0
    note: Optional[str] = None

class PortfolioClose(BaseModel):
    sell_price: float

# ──── Background Hunter ────

hunt_status = {"running": False, "last_run": None, "last_count": 0, "errors": []}

async def background_hunter():
    await asyncio.sleep(30)  # delay first scan to pass healthcheck
    while True:
        try:
            hunt_status["running"] = True
            log.info("Мисливець: початок сканування...")
            result = await global_hunter.hunt_all()
            db = SessionLocal()
            count = 0
            crypto_c = stocks_c = 0
            for item in result.get("hunted", []):
                symbol = item.get("symbol", "???")
                category = item.get("category", "CRYPTO")
                if category == "CRYPTO":
                    crypto_c += 1
                else:
                    stocks_c += 1
                existing = db.query(MarketAsset).filter(MarketAsset.symbol == symbol, MarketAsset.category == category).first()
                if existing:
                    existing.price_usd = item.get("price_usd")
                    existing.change_pct = item.get("change_24h") or item.get("change_pct")
                    existing.volume = item.get("volume_24h") or item.get("volume")
                    existing.volume_1h = item.get("volume_1h")
                    existing.auto_captured = 1
                    existing.capture_reason = item.get("capture_reason")
                    existing.last_updated = datetime.now(timezone.utc)
                else:
                    db.add(MarketAsset(category=category, symbol=symbol, name=item.get("name"),
                        price_usd=item.get("price_usd"), change_pct=item.get("change_24h") or item.get("change_pct"),
                        volume=item.get("volume_24h") or item.get("volume"), volume_1h=item.get("volume_1h"),
                        chain=item.get("chain"), auto_captured=1, capture_reason=item.get("capture_reason")))
                    count += 1
            for pos in db.query(Portfolio).filter(Portfolio.status == "open").all():
                asset = db.query(MarketAsset).filter(MarketAsset.symbol == pos.symbol, MarketAsset.category == pos.category).first()
                if asset and asset.price_usd:
                    pos.current_price = asset.price_usd
                    pos.pnl_usd = round((asset.price_usd - pos.buy_price) * pos.quantity, 2)
                    pos.pnl_pct = round((asset.price_usd - pos.buy_price) / pos.buy_price * 100, 2) if pos.buy_price > 0 else 0
            db.add(HuntHistory(hunted_count=result.get("hunted_count", 0), crypto_count=crypto_c, stocks_count=stocks_c, scan_duration=result.get("scan_duration_sec")))
            # Save price history for charts
            for item in result.get("hunted", []):
                if item.get("price_usd"):
                    db.add(PriceHistory(symbol=item.get("symbol","???"), category=item.get("category","CRYPTO"), price_usd=item["price_usd"]))
            # Check watchlist alerts
            for wi in db.query(WatchlistItem).filter(WatchlistItem.triggered == 0).all():
                asset = db.query(MarketAsset).filter(MarketAsset.symbol == wi.symbol, MarketAsset.category == wi.category).first()
                if asset and asset.price_usd and wi.target_price:
                    if (wi.direction == "above" and asset.price_usd >= wi.target_price) or (wi.direction == "below" and asset.price_usd <= wi.target_price):
                        wi.triggered = 1
                        log.info(f"Watchlist alert: {wi.symbol} досяг {wi.target_price}")
            db.commit(); db.close()
            hunt_status["last_run"] = datetime.now(timezone.utc).isoformat()
            hunt_status["last_count"] = result.get("hunted_count", 0)
            hunt_status["errors"] = []
            log.info(f"Мисливець: вполювано {result['hunted_count']}, нових у базі: {count}")
            # Notify Telegram subscribers
            try:
                await telegram_bot.broadcast_hunt_results(result)
            except Exception as tg_err:
                log.error(f"TG broadcast error: {tg_err}")
        except Exception as e:
            hunt_status["errors"].append(str(e))
            log.error(f"Мисливець помилка: {e}")
        finally:
            hunt_status["running"] = False
        await asyncio.sleep(HUNT_INTERVAL)

@asynccontextmanager
async def lifespan(app):
    task = None
    tg_task = None
    if HUNT_INTERVAL > 0:
        task = asyncio.create_task(background_hunter())
        log.info(f"Мисливець запущено (інтервал: {HUNT_INTERVAL}с)")
    else:
        log.info("Мисливець вимкнено (HUNT_INTERVAL=0)")
    # Start Telegram bot
    tg_task = asyncio.create_task(telegram_bot.run_bot(SessionLocal))
    yield
    if task:
        task.cancel()
    if tg_task:
        tg_task.cancel()

app = FastAPI(title="Omni-Vision", version="1.2.0", lifespan=lifespan)

# ──── Security Middleware ────

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        response.headers["Content-Security-Policy"] = "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval' https://unpkg.com https://cdn.jsdelivr.net https://cdnjs.cloudflare.com; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src 'self' https://fonts.gstatic.com; img-src 'self' data: https://assets.coincap.io https://*.cryptocompare.com https://*.coingecko.com blob:; connect-src 'self' https://api.binance.com https://api.coingecko.com https://min-api.cryptocompare.com https://api.coinpaprika.com wss://stream.binance.com; frame-ancestors 'none'"
        return response

app.add_middleware(SecurityHeadersMiddleware)

class APICacheMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        if request.method == "GET" and request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "public, max-age=10, stale-while-revalidate=30"
        return response

app.add_middleware(APICacheMiddleware)
app.add_middleware(GZipMiddleware, minimum_size=500)
ALLOWED_ORIGINS = [
    os.getenv("CORS_ORIGIN", "https://dependable-tranquility-production-d86f.up.railway.app"),
]
app.add_middleware(CORSMiddleware, allow_origins=ALLOWED_ORIGINS, allow_credentials=True, allow_methods=["GET","POST","PUT","DELETE"], allow_headers=["Content-Type","X-Boss-Key","X-CSRF-Token"])

# ──── Pro Features ────
pro_api.setup(app, get_db, MarketAsset, PriceHistory, Portfolio, get_current_user, SessionLocal, WatchlistItem)

# ──── Assets Search API ────
@app.get("/api/assets")
async def api_all_assets(db: Session = Depends(get_db)):
    assets = db.query(MarketAsset).order_by(MarketAsset.volume.desc().nullslast()).all()
    return {"assets": [
        {"symbol": a.symbol, "name": a.name or a.symbol, "category": a.category,
         "price_usd": float(a.price_usd or 0), "change_pct": float(a.change_pct or 0),
         "volume": float(a.volume or 0), "chain": a.chain}
        for a in assets
    ]}

# ──── WebSocket Real-Time Prices ────
ws_clients = set()

@app.websocket("/ws/prices")
async def ws_prices(websocket: WebSocket):
    # Verify session cookie before accepting
    token = websocket.cookies.get("omni_session")
    if not token:
        await websocket.close(code=4001, reason="Not authenticated")
        return
    try:
        serializer.loads(token, max_age=SESSION_MAX_AGE)
    except Exception:
        await websocket.close(code=4001, reason="Invalid session")
        return
    await websocket.accept()
    ws_clients.add(websocket)
    try:
        while True:
            # Send price updates every 5 seconds
            await asyncio.sleep(5)
            db = SessionLocal()
            try:
                assets = db.query(MarketAsset).all()
                prices = [{"symbol": a.symbol, "price_usd": float(a.price_usd or 0),
                           "change_pct": float(a.change_pct or 0)} for a in assets]
                await websocket.send_json({"prices": prices, "ts": datetime.now(timezone.utc).isoformat()})
            finally:
                db.close()
    except (WebSocketDisconnect, Exception):
        ws_clients.discard(websocket)

# ──── Rate Limiter ────

login_attempts = defaultdict(list)  # ip -> [timestamps]
RATE_LIMIT_MAX = 5
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_BLOCK = 600  # 10 min block after 5 failed attempts

def check_rate_limit(ip: str) -> bool:
    now = time.time()
    login_attempts[ip] = [t for t in login_attempts[ip] if now - t < RATE_LIMIT_BLOCK]
    if len(login_attempts[ip]) >= RATE_LIMIT_MAX:
        return False
    return True

def record_attempt(ip: str):
    login_attempts[ip].append(time.time())

# ──── CSRF Protection ────

def generate_csrf_token(session_data: str = "") -> str:
    return serializer.dumps({"csrf": True, "t": time.time(), "s": session_data[:8] if session_data else ""})

def verify_csrf_token(token: str, max_age: int = 3600) -> bool:
    try:
        data = serializer.loads(token, max_age=max_age)
        return data.get("csrf") == True
    except (BadSignature, SignatureExpired):
        return False

# ──── Input Sanitization ────

def sanitize(text: str, max_len: int = 256) -> str:
    if not text: return ""
    text = text.strip()[:max_len]
    text = html_escape.escape(text)
    return text

def validate_email(email: str) -> bool:
    return bool(re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email))

def validate_username(username: str) -> bool:
    return bool(re.match(r'^[a-zA-Z0-9_]{3,32}$', username))

# ──── Auth routes ────

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    user = get_current_user(request)
    if user:
        return RedirectResponse(url="/", status_code=302)
    html = read_template("login.html")
    if not html:
        return HTMLResponse("<h1>login.html not found</h1>", status_code=500)
    return HTMLResponse(content=html)

@app.post("/login")
async def do_login(request: Request, username: str = Form(...), password: str = Form(...), csrf_token: str = Form("")):
    # CSRF check (skip if token not yet implemented in form)
    if csrf_token and not verify_csrf_token(csrf_token):
        return RedirectResponse(url="/login?error=csrf", status_code=302)
    ip = request.client.host if request.client else "unknown"
    if not check_rate_limit(ip):
        log.warning(f"Rate limit: {ip} заблоковано")
        return RedirectResponse(url="/login?error=blocked", status_code=302)
    record_attempt(ip)
    username = sanitize(username, 64)
    db = SessionLocal()
    user = db.query(User).filter(User.username == username).first()
    if user and verify_password(password, user.password_hash):
        login_attempts[ip] = []  # reset on success
        token = create_session_token(user.id, user.username)
        resp = RedirectResponse(url="/", status_code=302)
        resp.set_cookie(key="omni_session", value=token, max_age=SESSION_MAX_AGE, httponly=True, samesite="lax", secure=True)
        upgrade_password_if_needed(user, password)
        db.commit()
        log.info(f"Вхід: {username} з {ip}")
        db.close()
        return resp
    log.warning(f"Невдала спроба входу: {username} з {ip}")
    db.close()
    return RedirectResponse(url="/login?error=wrong", status_code=302)

@app.get("/register", response_class=HTMLResponse)
def register_page(request: Request):
    user = get_current_user(request)
    if user:
        return RedirectResponse(url="/", status_code=302)
    html = read_template("register.html")
    if not html:
        return HTMLResponse("<h1>register.html not found</h1>", status_code=500)
    return HTMLResponse(content=html)

@app.post("/register")
async def do_register(request: Request, username: str = Form(...), email: str = Form(...), password: str = Form(...), password2: str = Form(...), csrf_token: str = Form("")):
    if csrf_token and not verify_csrf_token(csrf_token):
        return RedirectResponse(url="/register?error=csrf", status_code=302)
    ip = request.client.host if request.client else "unknown"
    if not check_rate_limit(ip):
        return RedirectResponse(url="/register?error=blocked", status_code=302)
    record_attempt(ip)
    username = sanitize(username, 32)
    email = sanitize(email, 256)
    if not validate_username(username):
        return RedirectResponse(url="/register?error=username_invalid", status_code=302)
    if not validate_email(email):
        return RedirectResponse(url="/register?error=email_invalid", status_code=302)
    if password != password2:
        return RedirectResponse(url="/register?error=mismatch", status_code=302)
    if len(password) < 8:
        return RedirectResponse(url="/register?error=short", status_code=302)
    if len(password) > 128:
        return RedirectResponse(url="/register?error=too_long", status_code=302)
    db = SessionLocal()
    if db.query(User).filter(User.username == username).first():
        db.close()
        return RedirectResponse(url="/register?error=exists", status_code=302)
    if db.query(User).filter(User.email == email).first():
        db.close()
        return RedirectResponse(url="/register?error=email_exists", status_code=302)
    user = User(username=username, email=email, password_hash=hash_password(password))
    db.add(user); db.commit(); db.refresh(user)
    login_attempts[ip] = []
    token = create_session_token(user.id, user.username)
    resp = RedirectResponse(url="/", status_code=302)
    resp.set_cookie(key="omni_session", value=token, max_age=SESSION_MAX_AGE, httponly=True, samesite="lax", secure=True)
    log.info(f"Реєстрація: {username} ({email}) з {ip}")
    db.close()
    return resp

@app.get("/logout")
def do_logout():
    resp = RedirectResponse(url="/login", status_code=302)
    resp.delete_cookie("omni_session")
    return resp

# ──── Profile ────

@app.get("/profile", response_class=HTMLResponse)
def profile_page(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    db = SessionLocal()
    u = db.query(User).filter(User.id == user["uid"]).first()
    html = read_template("profile.html")
    if not html:
        db.close()
        return HTMLResponse("<h1>profile.html not found</h1>", status_code=500)
    html = html.replace("{{USERNAME}}", u.username if u else "")
    html = html.replace("{{EMAIL}}", u.email if u else "")
    html = html.replace("{{CREATED}}", u.created_at.strftime("%d.%m.%Y") if u and u.created_at else "")
    wallet_count = db.query(Wallet).filter(Wallet.user_id == user["uid"]).count()
    pos_count = db.query(Portfolio).filter(Portfolio.user_id == user["uid"]).count()
    html = html.replace("{{WALLET_COUNT}}", str(wallet_count))
    html = html.replace("{{POS_COUNT}}", str(pos_count))
    html = html.replace("{{RISK_PROFILE}}", u.risk_profile if u else "balanced")
    db.close()
    return HTMLResponse(content=html)

@app.post("/profile/password")
def change_password(request: Request, old_password: str = Form(...), new_password: str = Form(...), new_password2: str = Form(...)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    if new_password != new_password2:
        return RedirectResponse(url="/profile?error=mismatch", status_code=302)
    if len(new_password) < 6:
        return RedirectResponse(url="/profile?error=short", status_code=302)
    db = SessionLocal()
    u = db.query(User).filter(User.id == user["uid"]).first()
    if not u or not verify_password(old_password, u.password_hash):
        db.close()
        return RedirectResponse(url="/profile?error=wrong_old", status_code=302)
    u.password_hash = hash_password(new_password)
    db.commit(); db.close()
    return RedirectResponse(url="/profile?success=password", status_code=302)

@app.post("/profile/risk")
def change_risk(request: Request, risk_profile: str = Form(...)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    if risk_profile not in ("conservative", "balanced", "aggressive"):
        return RedirectResponse(url="/profile?error=bad_risk", status_code=302)
    db = SessionLocal()
    u = db.query(User).filter(User.id == user["uid"]).first()
    if u:
        u.risk_profile = risk_profile
        db.commit()
    db.close()
    return RedirectResponse(url="/profile?success=risk", status_code=302)

# ──── Advisor Engine ────

def generate_advice(user_id: int, db: Session) -> dict:
    user = db.query(User).filter(User.id == user_id).first()
    risk = user.risk_profile if user else "balanced"
    positions = db.query(Portfolio).filter(Portfolio.user_id == user_id, Portfolio.status == "open").all()
    hunted = db.query(MarketAsset).filter(MarketAsset.auto_captured == 1).order_by(MarketAsset.last_updated.desc()).limit(100).all()

    # Risk thresholds
    thresholds = {
        "conservative": {"max_single": 20, "min_diversity": 5, "sell_loss": -5, "buy_change_min": -3, "buy_change_max": 10},
        "balanced":     {"max_single": 30, "min_diversity": 3, "sell_loss": -10, "buy_change_min": -5, "buy_change_max": 30},
        "aggressive":   {"max_single": 50, "min_diversity": 2, "sell_loss": -20, "buy_change_min": -15, "buy_change_max": 100},
    }
    th = thresholds.get(risk, thresholds["balanced"])

    # Portfolio analysis
    total_invested = sum(p.buy_price * p.quantity for p in positions) if positions else 0
    total_current = sum((p.current_price or p.buy_price) * p.quantity for p in positions) if positions else 0
    total_pnl = total_current - total_invested
    total_pnl_pct = round(total_pnl / total_invested * 100, 2) if total_invested > 0 else 0

    # Category distribution
    cat_dist = {}
    for p in positions:
        val = (p.current_price or p.buy_price) * p.quantity
        cat_dist[p.category] = cat_dist.get(p.category, 0) + val

    # Single asset concentration
    asset_pcts = {}
    for p in positions:
        val = (p.current_price or p.buy_price) * p.quantity
        pct = round(val / total_current * 100, 1) if total_current > 0 else 0
        asset_pcts[p.symbol] = asset_pcts.get(p.symbol, 0) + pct

    # Health score (0-100)
    score = 50
    alerts = []
    recommendations = []

    if not positions:
        score = 0
        alerts.append({"type": "info", "msg": "Портфель порожній. Додайте позиції для аналізу."})
    else:
        # Diversity bonus
        unique = len(set(p.symbol for p in positions))
        if unique >= th["min_diversity"]:
            score += 15
        else:
            score -= 10
            alerts.append({"type": "warning", "msg": f"Низька диверсифікація: {unique} активів. Рекомендовано мін. {th['min_diversity']}."})

        # PnL impact
        if total_pnl_pct > 10:
            score += 15
        elif total_pnl_pct > 0:
            score += 8
        elif total_pnl_pct > -5:
            score -= 5
        else:
            score -= 15
            alerts.append({"type": "danger", "msg": f"Портфель у мінусі: {total_pnl_pct:+.1f}%. Розгляньте ребалансування."})

        # Concentration check
        for sym, pct in asset_pcts.items():
            if pct > th["max_single"]:
                score -= 10
                alerts.append({"type": "warning", "msg": f"{sym} займає {pct}% портфелю. Макс. рекомендовано: {th['max_single']}%."})

        # Category balance
        if len(cat_dist) >= 2:
            score += 10
        else:
            alerts.append({"type": "info", "msg": "Всі активи в одній категорії. Розгляньте диверсифікацію."})

        # Sell recommendations (losers)
        for p in positions:
            pnl = p.pnl_pct or 0
            if pnl < th["sell_loss"]:
                recommendations.append({
                    "action": "SELL", "symbol": p.symbol, "category": p.category,
                    "reason": f"Збиток {pnl:+.1f}% перевищує поріг {th['sell_loss']}%",
                    "urgency": "high" if pnl < th["sell_loss"] * 2 else "medium",
                    "current_price": p.current_price
                })
            elif pnl > 50 and risk != "aggressive":
                recommendations.append({
                    "action": "SELL", "symbol": p.symbol, "category": p.category,
                    "reason": f"Прибуток {pnl:+.1f}% — зафіксуйте частину",
                    "urgency": "low",
                    "current_price": p.current_price
                })

        # Hold recommendations
        for p in positions:
            pnl = p.pnl_pct or 0
            if th["sell_loss"] <= pnl <= 50:
                recommendations.append({
                    "action": "HOLD", "symbol": p.symbol, "category": p.category,
                    "reason": f"Стабільна позиція ({pnl:+.1f}%)",
                    "urgency": "low",
                    "current_price": p.current_price
                })

    # Buy recommendations from market data
    owned_symbols = set(p.symbol for p in positions)
    buy_candidates = []
    for a in hunted:
        if a.symbol in owned_symbols:
            continue
        ch = a.change_pct or 0
        vol = a.volume_1h or 0
        # Score candidate
        cand_score = 0
        reason_parts = []
        if th["buy_change_min"] <= ch <= th["buy_change_max"]:
            cand_score += 30
            if ch > 5:
                reason_parts.append(f"зростання {ch:+.1f}%")
            elif ch < 0:
                reason_parts.append(f"корекція {ch:+.1f}% (можливість)")
        if vol > 500000:
            cand_score += 20
            reason_parts.append(f"високий об'єм {vol/1e6:.1f}M")
        elif vol > 100000:
            cand_score += 10
        if a.capture_reason and "RVOL" in (a.capture_reason or ""):
            cand_score += 15
            reason_parts.append("аномальний об'єм")
        if cand_score >= 30:
            buy_candidates.append({
                "action": "BUY", "symbol": a.symbol, "category": a.category,
                "reason": ", ".join(reason_parts) if reason_parts else "Сигнал від мисливця",
                "urgency": "high" if cand_score >= 50 else "medium",
                "price": a.price_usd, "change_pct": ch
            })
    buy_candidates.sort(key=lambda x: x.get("urgency") == "high", reverse=True)
    buy_recs = buy_candidates[:5]

    score = max(0, min(100, score))

    risk_labels = {"conservative": "Консервативний", "balanced": "Збалансований", "aggressive": "Агресивний"}

    return {
        "health_score": score,
        "risk_profile": risk,
        "risk_label": risk_labels.get(risk, risk),
        "portfolio": {
            "total_invested": round(total_invested, 2),
            "total_current": round(total_current, 2),
            "pnl_usd": round(total_pnl, 2),
            "pnl_pct": total_pnl_pct,
            "positions_count": len(positions),
            "unique_assets": len(set(p.symbol for p in positions)),
            "categories": {k: round(v, 2) for k, v in cat_dist.items()},
        },
        "alerts": alerts,
        "sell_hold": [r for r in recommendations],
        "buy_opportunities": buy_recs,
        "disclaimer": "Це не фінансова порада. Завжди досліджуйте самостійно перед інвестуванням."
    }

# ──── AI Chat Assistant ────

import httpx as _httpx_ai

async def _gather_market_context() -> dict:
    """Gather real-time market data from all sources for AI context"""
    ctx = {}
    async with _httpx_ai.AsyncClient(timeout=8) as client:
        # 1. Top crypto prices
        try:
            r = await client.get("https://api.coingecko.com/api/v3/coins/markets",
                params={"vs_currency": "usd", "order": "market_cap_desc", "per_page": 20,
                        "sparkline": False, "price_change_percentage": "1h,24h,7d"})
            coins = r.json()
            ctx["top_crypto"] = [{"symbol": c.get("symbol","").upper(), "name": c.get("name"),
                "price": c.get("current_price"), "change_24h": c.get("price_change_percentage_24h"),
                "change_7d": c.get("price_change_percentage_7d_in_currency"),
                "market_cap": c.get("market_cap"), "volume": c.get("total_volume"),
                "rank": c.get("market_cap_rank")} for c in (coins if isinstance(coins, list) else [])]
        except: ctx["top_crypto"] = []

        # 2. Fear & Greed
        try:
            r = await client.get("https://api.alternative.me/fng/?limit=1&format=json")
            fng = r.json()
            if fng.get("data"):
                ctx["fear_greed"] = {"value": int(fng["data"][0]["value"]),
                    "label": fng["data"][0].get("value_classification", "")}
        except: ctx["fear_greed"] = None

        # 3. BTC dominance & global data
        try:
            r = await client.get("https://api.coingecko.com/api/v3/global")
            g = r.json().get("data", {})
            ctx["global"] = {"total_market_cap": g.get("total_market_cap", {}).get("usd"),
                "total_volume": g.get("total_volume", {}).get("usd"),
                "btc_dominance": g.get("market_cap_percentage", {}).get("btc"),
                "eth_dominance": g.get("market_cap_percentage", {}).get("eth"),
                "active_crypto": g.get("active_cryptocurrencies"),
                "market_cap_change_24h": g.get("market_cap_change_percentage_24h_usd")}
        except: ctx["global"] = None

        # 4. Trending on CoinGecko
        try:
            r = await client.get("https://api.coingecko.com/api/v3/search/trending")
            tr = r.json()
            ctx["trending"] = [{"symbol": c["item"]["symbol"], "name": c["item"]["name"],
                "rank": c["item"].get("market_cap_rank")}
                for c in (tr.get("coins", []))[:7]]
        except: ctx["trending"] = []

        # 5. Latest crypto news headlines
        try:
            r = await client.get("https://min-api.cryptocompare.com/data/v2/news/?lang=EN&sortOrder=latest",
                headers={"Accept": "application/json"})
            news = r.json().get("Data", [])[:8]
            ctx["news"] = [{"title": n.get("title"), "source": n.get("source"),
                "categories": n.get("categories", ""),
                "published": n.get("published_on")} for n in news]
        except: ctx["news"] = []

    return ctx


def _build_ai_prompt(question: str, market_ctx: dict, user_portfolio: list, user_risk: str) -> str:
    """Build a comprehensive prompt with all gathered data"""
    parts = []
    parts.append("Ти — Omni-Vision AI, експертний крипто-аналітик. Відповідай УКРАЇНСЬКОЮ.")
    parts.append("Аналізуй на основі реальних даних нижче. Будь конкретним, давай цифри, ціни, відсотки.")
    parts.append("Якщо питання про конкретну монету — дай повний аналіз: ціна, тренд, обсяг, рекомендація.")
    parts.append("Завжди додавай disclaimer що це не фінансова порада.")
    parts.append("")

    # Global market
    g = market_ctx.get("global")
    if g:
        parts.append(f"=== ГЛОБАЛЬНИЙ РИНОК ===")
        parts.append(f"Загальна капіталізація: ${g.get('total_market_cap',0)/1e12:.2f}T")
        parts.append(f"Обсяг 24h: ${g.get('total_volume',0)/1e9:.1f}B")
        parts.append(f"BTC домінація: {g.get('btc_dominance',0):.1f}%")
        parts.append(f"ETH домінація: {g.get('eth_dominance',0):.1f}%")
        parts.append(f"Зміна капіталізації 24h: {g.get('market_cap_change_24h',0):.2f}%")
        parts.append("")

    # Fear & Greed
    fg = market_ctx.get("fear_greed")
    if fg:
        parts.append(f"=== FEAR & GREED INDEX ===")
        parts.append(f"Значення: {fg['value']}/100 ({fg['label']})")
        parts.append("")

    # Top 20 crypto
    top = market_ctx.get("top_crypto", [])
    if top:
        parts.append("=== ТОП-20 КРИПТОВАЛЮТ ===")
        for c in top:
            ch24 = c.get("change_24h") or 0
            ch7d = c.get("change_7d") or 0
            parts.append(f"{c['symbol']}: ${c.get('price',0):,.2f} | 24h: {ch24:+.1f}% | 7d: {ch7d:+.1f}% | Vol: ${c.get('volume',0)/1e9:.1f}B | MCap: ${c.get('market_cap',0)/1e9:.1f}B")
        parts.append("")

    # Trending
    tr = market_ctx.get("trending", [])
    if tr:
        parts.append("=== ТРЕНДИ (CoinGecko) ===")
        parts.append(", ".join([f"{t['symbol']} ({t['name']})" for t in tr]))
        parts.append("")

    # News
    news = market_ctx.get("news", [])
    if news:
        parts.append("=== ОСТАННІ НОВИНИ ===")
        for n in news:
            parts.append(f"- {n.get('title','')} [{n.get('source','')}]")
        parts.append("")

    # User portfolio
    if user_portfolio:
        parts.append(f"=== ПОРТФЕЛЬ КОРИСТУВАЧА (профіль: {user_risk}) ===")
        for p in user_portfolio:
            pnl = p.get("pnl_pct", 0) or 0
            parts.append(f"{p['symbol']}: вхід ${p.get('buy_price',0):.4f}, поточна ${p.get('current_price',0):.4f}, P&L: {pnl:+.1f}%, к-сть: {p.get('quantity',0)}")
        parts.append("")

    parts.append(f"=== ЗАПИТ КОРИСТУВАЧА ===")
    parts.append(question)

    return "\n".join(parts)


async def _ai_generate(prompt: str) -> str:
    """Generate AI response using free API (Groq/OpenRouter/local fallback)"""
    # Try multiple free AI APIs
    apis = [
        {
            "url": "https://api.groq.com/openai/v1/chat/completions",
            "key_env": "GROQ_API_KEY",
            "model": "llama-3.3-70b-versatile",
        },
        {
            "url": "https://openrouter.ai/api/v1/chat/completions",
            "key_env": "OPENROUTER_API_KEY",
            "model": "meta-llama/llama-3.3-70b-instruct:free",
        },
    ]

    for api in apis:
        api_key = os.getenv(api["key_env"], "")
        if not api_key:
            continue
        try:
            async with _httpx_ai.AsyncClient(timeout=30) as client:
                r = await client.post(api["url"],
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    json={
                        "model": api["model"],
                        "messages": [
                            {"role": "system", "content": "Ти Omni-Vision AI — експертний крипто-аналітик. Відповідай УКРАЇНСЬКОЮ. Будь конкретним, давай цифри. Використовуй емодзі. Формат: markdown."},
                            {"role": "user", "content": prompt}
                        ],
                        "max_tokens": 2000,
                        "temperature": 0.7,
                    })
                data = r.json()
                choices = data.get("choices", [])
                if choices:
                    return choices[0].get("message", {}).get("content", "")
        except Exception as e:
            log.warning(f"AI API {api['key_env']} failed: {e}")
            continue

    # Fallback: rule-based analysis
    return _fallback_analysis(prompt)


def _fallback_analysis(prompt: str) -> str:
    """Smart rule-based analysis using gathered market data"""
    # Parse the prompt to extract market data that was embedded
    lines = prompt.split("\n")
    
    response_parts = []
    response_parts.append("🤖 **Omni-Vision AI — Аналіз ринку**\n")
    
    # Extract and format top crypto data
    crypto_section = []
    in_crypto = False
    for line in lines:
        if "ТОП-20 КРИПТОВАЛЮТ" in line:
            in_crypto = True
            continue
        if line.startswith("===") and in_crypto:
            in_crypto = False
            continue
        if in_crypto and line.strip():
            crypto_section.append(line.strip())
    
    if crypto_section:
        response_parts.append("📊 **Топ криптовалют зараз:**\n")
        for i, coin in enumerate(crypto_section[:10]):
            response_parts.append(f"  {coin}")
        response_parts.append("")
    
    # Extract Fear & Greed
    for line in lines:
        if "FEAR & GREED" in line:
            continue
        if "Значення:" in line:
            response_parts.append(f"🌡️ **Індекс страху та жадібності:** {line.strip()}\n")
    
    # Extract global market
    global_lines = []
    in_global = False
    for line in lines:
        if "ГЛОБАЛЬНИЙ РИНОК" in line:
            in_global = True
            continue
        if line.startswith("===") and in_global:
            in_global = False
            continue
        if in_global and line.strip():
            global_lines.append(line.strip())
    
    if global_lines:
        response_parts.append("🌍 **Глобальний ринок:**\n")
        for g in global_lines:
            response_parts.append(f"  {g}")
        response_parts.append("")
    
    # Extract trending
    for line in lines:
        if line.startswith("==="):
            continue
        # trending is a comma-separated line after ТРЕНДИ header
    
    # Extract news
    news_lines = []
    in_news = False
    for line in lines:
        if "ОСТАННІ НОВИНИ" in line:
            in_news = True
            continue
        if line.startswith("===") and in_news:
            in_news = False
            continue
        if in_news and line.strip():
            news_lines.append(line.strip())
    
    if news_lines:
        response_parts.append("📰 **Останні новини:**\n")
        for n in news_lines[:5]:
            response_parts.append(f"  {n}")
        response_parts.append("")
    
    # Portfolio if present
    port_lines = []
    in_port = False
    for line in lines:
        if "ПОРТФЕЛЬ КОРИСТУВАЧА" in line:
            in_port = True
            response_parts.append(f"💼 **{line.replace('===','').strip()}:**\n")
            continue
        if line.startswith("===") and in_port:
            in_port = False
            continue
        if in_port and line.strip():
            port_lines.append(line.strip())
            response_parts.append(f"  {line.strip()}")
    
    if port_lines:
        response_parts.append("")
    
    response_parts.append("\n💡 *Для більш детальних AI-відповідей додайте GROQ_API_KEY (безкоштовно на groq.com) в налаштуваннях Railway.*")
    response_parts.append("\n⚠️ Це не фінансова порада. Завжди досліджуйте самостійно.")
    
    return "\n".join(response_parts)


class AiChatRequest(BaseModel):
    message: str
    context: str = "general"  # general / portfolio / coin:SYMBOL

@app.post("/api/ai/chat")
async def ai_chat(request: Request, body: AiChatRequest, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401)

    question = body.message.strip()
    if not question or len(question) > 2000:
        raise HTTPException(400, "Порожнє або занадто довге повідомлення")

    # Gather market context
    market_ctx = await _gather_market_context()

    # Get user portfolio
    portfolio = []
    positions = db.query(Portfolio).filter(Portfolio.user_id == user["uid"], Portfolio.status == "open").all()
    for p in positions:
        portfolio.append({"symbol": p.symbol, "category": p.category, "buy_price": p.buy_price,
            "quantity": p.quantity, "current_price": p.current_price, "pnl_pct": p.pnl_pct})

    user_obj = db.query(User).filter(User.id == user["uid"]).first()
    risk = user_obj.risk_profile if user_obj else "balanced"

    # Build prompt and generate
    prompt = _build_ai_prompt(question, market_ctx, portfolio, risk)
    response = await _ai_generate(prompt)

    return {
        "response": response,
        "market_snapshot": {
            "fear_greed": market_ctx.get("fear_greed"),
            "btc_price": next((c["price"] for c in market_ctx.get("top_crypto", []) if c["symbol"] == "BTC"), None),
            "eth_price": next((c["price"] for c in market_ctx.get("top_crypto", []) if c["symbol"] == "ETH"), None),
            "market_cap_change": market_ctx.get("global", {}).get("market_cap_change_24h") if market_ctx.get("global") else None,
        },
        "data_sources": ["CoinGecko", "CryptoCompare", "Alternative.me"],
    }

# ──── Dashboard (protected) ────

@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request, lang: str = Query(DEFAULT_LANG, pattern="^(ukr|eng|rus)$")):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    db = SessionLocal()
    uid = user["uid"]
    wc = db.query(Wallet).filter(Wallet.user_id == uid).count()
    ic = db.query(Insight).count()
    ac = db.query(MarketAsset).count()
    hc = db.query(MarketAsset).filter(MarketAsset.auto_captured == 1).count()
    pc = db.query(Portfolio).filter(Portfolio.user_id == uid, Portfolio.status == "open").count()
    db.close()
    html = read_template("dashboard.html")
    if not html:
        return HTMLResponse(content="<h1>dashboard.html not found</h1>", status_code=500)
    replacements = {
        "{{GREETING}}": t("greeting", lang), "{{STATUS}}": t("status_ok", lang),
        "{{STATUS_LABEL}}": t("status_label", lang), "{{WALLETS_LABEL}}": t("wallets", lang),
        "{{INSIGHTS_LABEL}}": t("insights", lang), "{{ASSETS_LABEL}}": t("assets", lang),
        "{{HUNTED_LABEL}}": t("hunted", lang), "{{PORTFOLIO_LABEL}}": t("portfolio", lang),
        "{{DASHBOARD_TITLE}}": t("dashboard_title", lang), "{{WALLET_COUNT}}": str(wc),
        "{{INSIGHT_COUNT}}": str(ic), "{{ASSET_COUNT}}": str(ac),
        "{{HUNTED_COUNT}}": str(hc), "{{PORTFOLIO_COUNT}}": str(pc),
        "{{LANG}}": lang, "{{YEAR}}": str(datetime.now().year),
        "{{USER}}": user["user"],
        "{{TIER}}": get_user_tier(user),
    }
    for k, v in replacements.items():
        html = html.replace(k, v)
    return HTMLResponse(content=html)

# ──── API (public data — no auth needed) ────

@app.get("/api/telegram/status")
def telegram_status():
    subs = len(telegram_bot._subscribers)
    return {"bot_active": bool(os.getenv("TELEGRAM_BOT_TOKEN")),
            "subscribers": subs,
            "token_set": bool(os.getenv("TELEGRAM_BOT_TOKEN"))}

@app.get("/api/status")
def api_status(lang: str = Query(DEFAULT_LANG, pattern="^(ukr|eng|rus)$")):
    return {"status": "online", "message": t("status_ok", lang), "version": app.version,
            "radars": ["CRYPTO","STOCKS","COMMODITIES"], "hunter": hunt_status,
            "hunt_interval_sec": HUNT_INTERVAL, "timestamp": datetime.now(timezone.utc).isoformat()}

@app.get("/api/radar/crypto")
async def radar_crypto(token_address: str = "0xdac17f958d2ee523a2206206994597c13d831ec7"):
    return await crypto_scanner.get_token_price(token_address)

@app.get("/api/radar/stocks")
def radar_stocks(ticker: Optional[str] = None):
    return stocks_scanner.hunt_stocks(ticker)

@app.get("/api/radar/stocks/{ticker}")
def radar_stock_single(ticker: str):
    return stocks_scanner.get_stock(ticker.upper())

@app.get("/api/radar/stocks/trending/all")
def radar_stocks_trending():
    return stocks_scanner.hunt_trending()

@app.get("/api/radar/commodities")
def radar_commodities(symbol: Optional[str] = None):
    return commodities_scanner.hunt_commodities(symbol)

@app.get("/api/radar/flow")
def radar_flow(crypto_change: Optional[float] = None):
    stocks_data = stocks_scanner.hunt_stocks()
    commodities_data = commodities_scanner.hunt_commodities()
    return {"flow_alerts": flow_detector.detect_flows(stocks_data, commodities_data, crypto_change)}

@app.get("/api/hunted")
def list_hunted(limit: int = Query(50, ge=1, le=200), db: Session = Depends(get_db)):
    assets = db.query(MarketAsset).filter(MarketAsset.auto_captured == 1).order_by(MarketAsset.last_updated.desc()).limit(limit).all()
    return [{"id": a.id, "category": a.category, "symbol": a.symbol, "name": a.name,
             "price_usd": a.price_usd, "change_pct": a.change_pct, "volume_1h": a.volume_1h,
             "chain": a.chain, "capture_reason": a.capture_reason,
             "last_updated": a.last_updated.isoformat() if a.last_updated else None} for a in assets]

@app.get("/api/flow_alerts")
def list_flow_alerts(db: Session = Depends(get_db)):
    return [{"id": a.id, "type": a.alert_type, "severity": a.severity, "message": a.message,
             "detected_at": a.detected_at.isoformat()} for a in db.query(FlowAlert).order_by(FlowAlert.detected_at.desc()).limit(20).all()]

@app.get("/api/hunt_history")
def get_hunt_history(limit: int = Query(50, ge=1, le=200), db: Session = Depends(get_db)):
    records = db.query(HuntHistory).order_by(HuntHistory.scanned_at.desc()).limit(limit).all()
    return [{"id": h.id, "hunted_count": h.hunted_count, "crypto_count": h.crypto_count,
             "stocks_count": h.stocks_count, "scan_duration": h.scan_duration,
             "scanned_at": h.scanned_at.isoformat() if h.scanned_at else None} for h in records]

@app.get("/api/advisor")
def get_advisor(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return {"error": "not_authenticated"}
    return generate_advice(user["uid"], db)

@app.get("/api/analytics")
def get_analytics(db: Session = Depends(get_db)):
    total_assets = db.query(MarketAsset).count()
    total_hunted = db.query(MarketAsset).filter(MarketAsset.auto_captured == 1).count()
    total_scans = db.query(HuntHistory).count()
    avg_per_scan = db.query(func.avg(HuntHistory.hunted_count)).scalar() or 0
    by_category = {}
    for cat in ["CRYPTO", "STOCKS", "COMMODITIES"]:
        by_category[cat] = db.query(MarketAsset).filter(MarketAsset.category == cat, MarketAsset.auto_captured == 1).count()
    top_movers = db.query(MarketAsset).filter(MarketAsset.auto_captured == 1).order_by(MarketAsset.change_pct.desc()).limit(5).all()
    return {"total_assets": total_assets, "total_hunted": total_hunted, "total_scans": total_scans,
            "avg_per_scan": round(float(avg_per_scan), 1), "by_category": by_category,
            "top_movers": [{"symbol": a.symbol, "category": a.category, "change_pct": a.change_pct, "price_usd": a.price_usd} for a in top_movers]}

# ──── API (user-specific — session auth) ────

@app.get("/api/wallets")
def list_wallets(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return []
    wallets = db.query(Wallet).filter(Wallet.user_id == user["uid"]).all()
    return [{"id": w.id, "address": (w.address[:8]+"..."+w.address[-6:] if len(w.address)>20 else w.address),
             "blockchain": w.blockchain, "label": w.label, "asset": w.asset, "last_price": w.last_price} for w in wallets]

@app.get("/api/portfolio")
def list_portfolio(request: Request, status: str = "open", db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return []
    positions = db.query(Portfolio).filter(Portfolio.user_id == user["uid"], Portfolio.status == status).order_by(Portfolio.opened_at.desc()).all()
    return [{"id": p.id, "symbol": p.symbol, "category": p.category, "buy_price": p.buy_price,
             "quantity": p.quantity, "current_price": p.current_price, "pnl_usd": p.pnl_usd,
             "pnl_pct": p.pnl_pct, "note": p.note, "status": p.status,
             "opened_at": p.opened_at.isoformat() if p.opened_at else None,
             "closed_at": p.closed_at.isoformat() if p.closed_at else None} for p in positions]

@app.get("/api/portfolio/summary")
def portfolio_summary(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return {"open_count":0,"closed_count":0,"total_invested":0,"total_current":0,"unrealized_pnl":0,"unrealized_pnl_pct":0,"realized_pnl":0,"best_position":None,"worst_position":None}
    uid = user["uid"]
    op = db.query(Portfolio).filter(Portfolio.user_id == uid, Portfolio.status == "open").all()
    cp = db.query(Portfolio).filter(Portfolio.user_id == uid, Portfolio.status == "closed").all()
    ti = sum(p.buy_price * p.quantity for p in op)
    tc = sum((p.current_price or p.buy_price) * p.quantity for p in op)
    tp = tc - ti
    tp_pct = round(tp / ti * 100, 2) if ti > 0 else 0
    rp = sum(p.pnl_usd or 0 for p in cp)
    best = max(op, key=lambda p: p.pnl_pct or 0) if op else None
    worst = min(op, key=lambda p: p.pnl_pct or 0) if op else None
    return {"open_count": len(op), "closed_count": len(cp), "total_invested": round(ti, 2),
            "total_current": round(tc, 2), "unrealized_pnl": round(tp, 2), "unrealized_pnl_pct": tp_pct,
            "realized_pnl": round(rp, 2),
            "best_position": {"symbol": best.symbol, "pnl_pct": best.pnl_pct} if best else None,
            "worst_position": {"symbol": worst.symbol, "pnl_pct": worst.pnl_pct} if worst else None}

@app.post("/api/portfolio")
def add_position(request: Request, body: PortfolioCreate, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401, "Увійдіть в систему")
    pos = Portfolio(user_id=user["uid"], symbol=body.symbol.upper(), category=body.category.upper(),
                    buy_price=body.buy_price, quantity=body.quantity, note=body.note,
                    current_price=body.buy_price, pnl_usd=0, pnl_pct=0)
    db.add(pos); db.commit(); db.refresh(pos)
    return {"status": "added", "position": {"id": pos.id, "symbol": pos.symbol, "buy_price": pos.buy_price, "quantity": pos.quantity}}

@app.put("/api/portfolio/{pos_id}/close")
def close_position(request: Request, pos_id: int, body: PortfolioClose, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401, "Увійдіть в систему")
    pos = db.query(Portfolio).filter(Portfolio.id == pos_id, Portfolio.user_id == user["uid"]).first()
    if not pos: raise HTTPException(404, "Позицію не знайдено")
    pos.current_price = body.sell_price
    pos.pnl_usd = round((body.sell_price - pos.buy_price) * pos.quantity, 2)
    pos.pnl_pct = round((body.sell_price - pos.buy_price) / pos.buy_price * 100, 2) if pos.buy_price > 0 else 0
    pos.status = "closed"; pos.closed_at = datetime.now(timezone.utc)
    db.commit()
    return {"status": "closed", "pnl_usd": pos.pnl_usd, "pnl_pct": pos.pnl_pct}

@app.delete("/api/portfolio/{pos_id}")
def delete_position(request: Request, pos_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401, "Увійдіть в систему")
    pos = db.query(Portfolio).filter(Portfolio.id == pos_id, Portfolio.user_id == user["uid"]).first()
    if not pos: raise HTTPException(404, "Позицію не знайдено")
    db.delete(pos); db.commit()
    return {"status": "deleted", "id": pos_id}

@app.get("/api/export/hunted")
def export_hunted_csv(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401, "Увійдіть в систему")
    assets = db.query(MarketAsset).filter(MarketAsset.auto_captured == 1).order_by(MarketAsset.last_updated.desc()).all()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Symbol", "Category", "Name", "Price USD", "Change %", "Volume 1h", "Chain", "Capture Reason", "Last Updated"])
    for a in assets:
        writer.writerow([a.symbol, a.category, a.name, a.price_usd, a.change_pct, a.volume_1h, a.chain, a.capture_reason, a.last_updated.isoformat() if a.last_updated else ""])
    output.seek(0)
    fname = f"omni_hunted_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv", headers={"Content-Disposition": f"attachment; filename={fname}"})

@app.get("/api/export/portfolio")
def export_portfolio_csv(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401, "Увійдіть в систему")
    positions = db.query(Portfolio).filter(Portfolio.user_id == user["uid"]).order_by(Portfolio.opened_at.desc()).all()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Symbol", "Category", "Buy Price", "Quantity", "Current Price", "P&L USD", "P&L %", "Status", "Note", "Opened", "Closed"])
    for p in positions:
        writer.writerow([p.symbol, p.category, p.buy_price, p.quantity, p.current_price, p.pnl_usd, p.pnl_pct, p.status, p.note, p.opened_at.isoformat() if p.opened_at else "", p.closed_at.isoformat() if p.closed_at else ""])
    output.seek(0)
    fname = f"omni_portfolio_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv", headers={"Content-Disposition": f"attachment; filename={fname}"})

# ──── Price History & Charts ────

@app.get("/api/price_history/{symbol}")
def get_price_history(symbol: str, category: str = "CRYPTO", limit: int = Query(100, ge=1, le=500), db: Session = Depends(get_db)):
    records = db.query(PriceHistory).filter(PriceHistory.symbol == symbol.upper(), PriceHistory.category == category.upper()).order_by(PriceHistory.recorded_at.asc()).limit(limit).all()
    return [{"price": r.price_usd, "time": r.recorded_at.isoformat()} for r in records]

# ──── Watchlist ────

class WatchlistCreate(BaseModel):
    symbol: str
    category: str = "CRYPTO"
    target_price: Optional[float] = None
    direction: str = "above"
    note: Optional[str] = None

@app.get("/api/watchlist")
def get_watchlist(request: Request, db: Session = Depends(get_db)):
    from sqlalchemy import or_, and_
    user = get_current_user(request)
    if not user: return []
    items = db.query(WatchlistItem).filter(WatchlistItem.user_id == user["uid"]).order_by(WatchlistItem.added_at.desc()).all()
    if not items:
        return []
    # Batch fetch all assets at once instead of N+1
    symbols_cats = [(w.symbol, w.category) for w in items]
    conditions = [and_(MarketAsset.symbol == s, MarketAsset.category == c) for s, c in symbols_cats]
    assets_list = db.query(MarketAsset).filter(or_(*conditions)).all() if conditions else []
    asset_map = {(a.symbol, a.category): a for a in assets_list}
    result = []
    for w in items:
        asset = asset_map.get((w.symbol, w.category))
        result.append({"id": w.id, "symbol": w.symbol, "category": w.category,
            "target_price": w.target_price, "direction": w.direction, "note": w.note,
            "triggered": w.triggered, "current_price": asset.price_usd if asset else None,
            "change_pct": asset.change_pct if asset else None})
    return result

@app.post("/api/watchlist")
def add_to_watchlist(request: Request, body: WatchlistCreate, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user: raise HTTPException(401, "Увійдіть в систему")
    item = WatchlistItem(user_id=user["uid"], symbol=body.symbol.upper(), category=body.category.upper(),
                         target_price=body.target_price, direction=body.direction, note=body.note)
    db.add(item); db.commit(); db.refresh(item)
    return {"status": "added", "id": item.id}

@app.delete("/api/watchlist/{item_id}")
def remove_from_watchlist(request: Request, item_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user: raise HTTPException(401, "Увійдіть")
    item = db.query(WatchlistItem).filter(WatchlistItem.id == item_id, WatchlistItem.user_id == user["uid"]).first()
    if not item: raise HTTPException(404, "Не знайдено")
    db.delete(item); db.commit()
    return {"status": "removed"}

# ──── Admin Panel ────

ADMIN_USER = os.getenv("ADMIN_USER", "boss")

# ──── Premium Tiers ────
TIER_LIMITS = {
    "free":  {"wallets": 3,  "watchlist": 10, "portfolio": 10, "export": False, "deep_analytics": False, "advisor_daily": 3},
    "pro":   {"wallets": 20, "watchlist": 100,"portfolio": 100,"export": True,  "deep_analytics": True,  "advisor_daily": 50},
    "vip":   {"wallets": 999,"watchlist": 999,"portfolio": 999,"export": True,  "deep_analytics": True,  "advisor_daily": 999},
}

def get_user_tier(user_dict: dict, db_session=None) -> str:
    """Get effective tier for user. Admin always gets VIP."""
    if user_dict.get("user") == ADMIN_USER:
        return "vip"
    if db_session:
        u = db_session.query(User).filter(User.id == user_dict["uid"]).first()
        if u:
            if u.username == ADMIN_USER or u.is_admin == 1:
                return "vip"
            if u.tier_expires and u.tier_expires < datetime.now(timezone.utc):
                u.tier = "free"
                u.tier_expires = None
                db_session.commit()
                return "free"
            return u.tier or "free"
    return "free"

def check_tier_limit(user_dict: dict, feature: str, db_session=None) -> tuple:
    """Returns (allowed: bool, tier: str, limit: int/bool)"""
    tier = get_user_tier(user_dict, db_session)
    limits = TIER_LIMITS.get(tier, TIER_LIMITS["free"])
    val = limits.get(feature)
    if isinstance(val, bool):
        return (val, tier, val)
    return (True, tier, val)

@app.get("/admin", response_class=HTMLResponse)
def admin_page(request: Request):
    user = get_current_user(request)
    if not user:
        return HTMLResponse("<h1>403 Forbidden</h1>", status_code=403)
    db = SessionLocal()
    db_user = db.query(User).filter(User.id == user["uid"]).first()
    if not db_user or (db_user.is_admin != 1 and db_user.username != ADMIN_USER):
        db.close()
        return HTMLResponse("<h1>403 Forbidden</h1>", status_code=403)
    users = db.query(User).order_by(User.created_at.desc()).all()
    total_users = len(users)
    total_assets = db.query(MarketAsset).count()
    total_hunted = db.query(MarketAsset).filter(MarketAsset.auto_captured == 1).count()
    total_scans = db.query(HuntHistory).count()
    total_positions = db.query(Portfolio).count()
    total_watchlist = db.query(WatchlistItem).count()

    html = f'''<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Omni-Vision Admin</title>
<style>*{{margin:0;padding:0;box-sizing:border-box}}body{{background:#0a0e17;color:#fff;font-family:'Segoe UI',system-ui,sans-serif;padding:40px 20px}}
.container{{max-width:900px;margin:0 auto}}
.card{{background:rgba(15,20,35,.85);border:1px solid rgba(0,224,255,.15);border-radius:16px;padding:24px;margin-bottom:20px;backdrop-filter:blur(20px)}}
h1{{font-size:24px;background:linear-gradient(90deg,#00e0ff,#00ff88);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:24px}}
.stats{{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin-bottom:24px}}
.stat{{background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.06);border-radius:12px;padding:16px;text-align:center}}
.stat .val{{font-size:28px;font-weight:700;color:#00e0ff}}.stat .lbl{{font-size:11px;color:rgba(255,255,255,.4);margin-top:4px}}
table{{width:100%;border-collapse:collapse}}th,td{{padding:10px 12px;text-align:left;border-bottom:1px solid rgba(255,255,255,.06);font-size:13px}}
th{{color:rgba(255,255,255,.5);font-weight:500}}tr:hover{{background:rgba(255,255,255,.02)}}
a{{color:#00e0ff;text-decoration:none}}a:hover{{text-decoration:underline}}
.badge{{display:inline-block;padding:2px 8px;border-radius:6px;font-size:10px;font-weight:600}}
.badge-admin{{background:rgba(168,85,247,.15);color:#a855f7}}.badge-user{{background:rgba(16,185,129,.15);color:#10b981}}
</style></head><body><div class="container">
<a href="/" style="font-size:14px;margin-bottom:16px;display:inline-block">← Дашборд</a>
<h1>Admin Panel — Omni-Vision</h1>
<div class="stats">
<div class="stat"><div class="val">{total_users}</div><div class="lbl">Користувачів</div></div>
<div class="stat"><div class="val">{total_hunted}</div><div class="lbl">Вполювано</div></div>
<div class="stat"><div class="val">{total_assets}</div><div class="lbl">Всього активів</div></div>
<div class="stat"><div class="val">{total_scans}</div><div class="lbl">Сканувань</div></div>
<div class="stat"><div class="val">{total_positions}</div><div class="lbl">Позицій</div></div>
<div class="stat"><div class="val">{total_watchlist}</div><div class="lbl">Watchlist</div></div>
</div>
<div class="card"><h3 style="margin-bottom:16px;font-size:16px">Мисливець</h3>
<p style="font-size:13px;color:rgba(255,255,255,.6)">Статус: <span style="color:#10b981">{"Працює" if hunt_status.get("running") else "Очікує"}</span></p>
<p style="font-size:13px;color:rgba(255,255,255,.6)">Останній запуск: {hunt_status.get("last_run","—")}</p>
<p style="font-size:13px;color:rgba(255,255,255,.6)">Останній результат: {hunt_status.get("last_count",0)} активів</p>
</div>
<div class="card"><h3 style="margin-bottom:16px;font-size:16px">Користувачі</h3>
<table><tr><th>#</th><th>Ім\'я</th><th>Email</th><th>Стиль</th><th>Тариф</th><th>Роль</th><th>Зареєстрований</th></tr>'''

    for u in users:
        role = '<span class="badge badge-admin">ADMIN</span>' if u.username == ADMIN_USER else '<span class="badge badge-user">USER</span>'
        tier_badge = {"vip": '<span class="badge" style="background:rgba(255,214,10,.15);color:#ffd60a">VIP</span>',
                      "pro": '<span class="badge" style="background:rgba(10,132,255,.15);color:#0a84ff">PRO</span>',
                      "free": '<span class="badge" style="background:rgba(255,255,255,.06);color:rgba(255,255,255,.4)">FREE</span>'}.get(u.tier or "free", "FREE")
        html += f'<tr><td>{u.id}</td><td>{html_escape.escape(u.username)}</td><td>{html_escape.escape(u.email)}</td><td>{html_escape.escape(u.risk_profile or "balanced")}</td><td>{tier_badge}</td><td>{role}</td><td>{u.created_at.strftime("%d.%m.%Y %H:%M") if u.created_at else "—"}</td></tr>'

    html += '''</table></div></div></body></html>'''
    db.close()
    return HTMLResponse(content=html)

# ──── Premium Tiers API ────

@app.get("/api/tier")
def get_tier(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        return {"tier": "free", "limits": TIER_LIMITS["free"]}
    tier = get_user_tier(user, db)
    u = db.query(User).filter(User.id == user["uid"]).first()
    return {
        "tier": tier,
        "limits": TIER_LIMITS.get(tier, TIER_LIMITS["free"]),
        "expires": u.tier_expires.isoformat() if u and u.tier_expires else None,
        "is_admin": user.get("user") == ADMIN_USER
    }

@app.get("/api/tier/plans")
def tier_plans():
    return {
        "plans": [
            {"id": "free", "name": "Free", "price": 0, "features": TIER_LIMITS["free"],
             "description": "Базовий доступ до платформи"},
            {"id": "pro", "name": "Pro", "price": 9.99, "features": TIER_LIMITS["pro"],
             "description": "Розширена аналітика та портфель"},
            {"id": "vip", "name": "VIP", "price": 29.99, "features": TIER_LIMITS["vip"],
             "description": "Повний доступ без обмежень"}
        ]
    }

class TierUpgrade(BaseModel):
    tier: str
    duration_days: int = 30

@app.post("/api/admin/tier/set")
def admin_set_tier(request: Request, body: TierUpgrade, user_id: int = Query(...), db: Session = Depends(get_db)):
    """Admin-only: set user tier"""
    admin = get_current_user(request)
    if not admin:
        raise HTTPException(401)
    db_admin = db.query(User).filter(User.id == admin["uid"]).first()
    if not db_admin or (db_admin.is_admin != 1 and db_admin.username != ADMIN_USER):
        raise HTTPException(403, "Admin only")
    if body.tier not in ("free", "pro", "vip"):
        raise HTTPException(400, "Invalid tier")
    target = db.query(User).filter(User.id == user_id).first()
    if not target:
        raise HTTPException(404, "User not found")
    from datetime import timedelta
    target.tier = body.tier
    target.tier_expires = datetime.now(timezone.utc) + timedelta(days=body.duration_days) if body.tier != "free" else None
    db.commit()
    return {"status": "updated", "user": target.username, "tier": body.tier,
            "expires": target.tier_expires.isoformat() if target.tier_expires else None}

# ──── Payment System ────

import httpx as _httpx

async def _cryptobot_request(method: str, params: dict = None):
    """Call CryptoBot API"""
    if not CRYPTOBOT_TOKEN:
        return None
    try:
        async with _httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"https://pay.crypt.bot/api/{method}",
                headers={"Crypto-Pay-API-Token": CRYPTOBOT_TOKEN},
                params=params or {}
            )
            data = r.json()
            return data.get("result") if data.get("ok") else None
    except Exception as e:
        log.error(f"CryptoBot API error: {e}")
        return None

@app.post("/api/payment/create")
async def create_payment(request: Request, db: Session = Depends(get_db)):
    """Create a payment order"""
    user = get_current_user(request)
    if not user:
        raise HTTPException(401)
    body = await request.json()
    tier = body.get("tier", "pro")
    method = body.get("method", "ton_direct")  # ton_direct / cryptobot

    if tier not in PAYMENT_PRICES:
        raise HTTPException(400, "Invalid tier")

    # Check if already VIP/admin
    if get_user_tier(user, db) == "vip" and tier == "vip":
        return {"status": "already_active", "message": "VIP вже активний"}

    amount_usd = PAYMENT_PRICES[tier]
    order_id = f"OV-{user['uid']}-{tier}-{secrets.token_hex(6)}"

    # Get TON price for conversion
    ton_price = 3.0  # fallback
    try:
        async with _httpx.AsyncClient(timeout=5) as client:
            r = await client.get("https://api.coingecko.com/api/v3/simple/price",
                                 params={"ids": "the-open-network", "vs_currencies": "usd"})
            data = r.json()
            ton_price = data.get("the-open-network", {}).get("usd", 3.0)
    except:
        pass

    amount_ton = round(amount_usd / ton_price, 4) if ton_price > 0 else round(amount_usd / 3.0, 4)

    payment = Payment(
        user_id=user["uid"], order_id=order_id, tier=tier,
        amount_usd=amount_usd, amount_ton=amount_ton, method=method
    )
    db.add(payment)
    db.commit()
    db.refresh(payment)

    result = {
        "order_id": order_id,
        "tier": tier,
        "amount_usd": amount_usd,
        "amount_ton": amount_ton,
        "ton_price_usd": ton_price,
        "status": "pending",
    }

    if method == "ton_direct":
        # TON deeplink — opens wallet app
        comment = order_id
        ton_link = f"ton://transfer/{TON_WALLET}?amount={int(amount_ton * 1e9)}&text={comment}"
        tonkeeper_link = f"https://app.tonkeeper.com/transfer/{TON_WALLET}?amount={int(amount_ton * 1e9)}&text={comment}"
        result["ton_link"] = ton_link
        result["tonkeeper_link"] = tonkeeper_link
        result["wallet"] = TON_WALLET
        result["comment"] = comment

    elif method == "cryptobot":
        if not CRYPTOBOT_TOKEN:
            raise HTTPException(400, "CryptoBot не налаштований. Використайте TON переказ.")
        # Create CryptoBot invoice (supports cards, Apple Pay, Google Pay)
        invoice = await _cryptobot_request("createInvoice", {
            "currency_type": "fiat",
            "fiat": "USD",
            "amount": str(amount_usd),
            "description": f"Omni-Vision {tier.upper()} — 30 днів",
            "payload": order_id,
            "paid_btn_name": "openBot",
            "paid_btn_url": "https://t.me/OmniVisionBot",
            "allow_comments": False,
            "allow_anonymous": False,
        })
        if invoice:
            payment.cryptobot_invoice_id = str(invoice.get("invoice_id", ""))
            db.commit()
            result["pay_url"] = invoice.get("pay_url", "")
            result["cryptobot_invoice_id"] = payment.cryptobot_invoice_id
        else:
            # Fallback: try crypto invoice
            invoice = await _cryptobot_request("createInvoice", {
                "currency_type": "crypto",
                "asset": "TON",
                "amount": str(amount_ton),
                "description": f"Omni-Vision {tier.upper()} — 30 днів",
                "payload": order_id,
                "paid_btn_name": "openBot",
                "paid_btn_url": "https://t.me/OmniVisionBot",
            })
            if invoice:
                payment.cryptobot_invoice_id = str(invoice.get("invoice_id", ""))
                db.commit()
                result["pay_url"] = invoice.get("pay_url", "")

    # Notify admin via Telegram
    try:
        admin_msg = (
            f"💰 <b>Нове замовлення!</b>\n\n"
            f"👤 Користувач: {user['user']}\n"
            f"📋 План: {tier.upper()}\n"
            f"💵 Сума: ${amount_usd} (~{amount_ton} TON)\n"
            f"🔑 Order: <code>{order_id}</code>\n"
            f"💳 Метод: {method}"
        )
        await telegram_bot.send_message(
            os.getenv("ADMIN_CHAT_ID", ""),
            admin_msg
        )
    except:
        pass

    return result

@app.get("/api/payment/status/{order_id}")
def payment_status(order_id: str, request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401)
    payment = db.query(Payment).filter(
        Payment.order_id == order_id, Payment.user_id == user["uid"]
    ).first()
    if not payment:
        raise HTTPException(404)
    return {
        "order_id": payment.order_id,
        "tier": payment.tier,
        "amount_usd": payment.amount_usd,
        "amount_ton": payment.amount_ton,
        "method": payment.method,
        "status": payment.status,
        "created_at": payment.created_at.isoformat() if payment.created_at else None,
        "confirmed_at": payment.confirmed_at.isoformat() if payment.confirmed_at else None,
    }

@app.post("/api/payment/confirm/{order_id}")
def confirm_payment(order_id: str, request: Request, db: Session = Depends(get_db)):
    """Admin confirms payment and activates tier"""
    admin = get_current_user(request)
    if not admin:
        raise HTTPException(401)
    db_admin = db.query(User).filter(User.id == admin["uid"]).first()
    if not db_admin or (db_admin.is_admin != 1 and db_admin.username != ADMIN_USER):
        raise HTTPException(403, "Admin only")
    payment = db.query(Payment).filter(Payment.order_id == order_id).first()
    if not payment:
        raise HTTPException(404)
    if payment.status == "confirmed":
        return {"status": "already_confirmed"}
    from datetime import timedelta
    payment.status = "confirmed"
    payment.confirmed_at = datetime.now(timezone.utc)
    # Activate tier
    target = db.query(User).filter(User.id == payment.user_id).first()
    if target:
        target.tier = payment.tier
        target.tier_expires = datetime.now(timezone.utc) + timedelta(days=30)
    db.commit()
    # Notify user via Telegram
    return {"status": "confirmed", "user": target.username if target else "?",
            "tier": payment.tier, "expires": target.tier_expires.isoformat() if target else None}

@app.post("/api/payment/cryptobot_webhook")
async def cryptobot_webhook(request: Request, db: Session = Depends(get_db)):
    """CryptoBot payment webhook — auto-confirms payment"""
    body = await request.json()
    if body.get("update_type") != "invoice_paid":
        return {"ok": True}
    payload = body.get("payload", {})
    order_id = payload.get("payload", "")
    if not order_id:
        return {"ok": True}
    payment = db.query(Payment).filter(Payment.order_id == order_id).first()
    if not payment or payment.status == "confirmed":
        return {"ok": True}
    from datetime import timedelta
    payment.status = "confirmed"
    payment.confirmed_at = datetime.now(timezone.utc)
    target = db.query(User).filter(User.id == payment.user_id).first()
    if target:
        target.tier = payment.tier
        target.tier_expires = datetime.now(timezone.utc) + timedelta(days=30)
    db.commit()
    log.info(f"CryptoBot payment confirmed: {order_id} -> {payment.tier}")
    # Notify admin
    try:
        await telegram_bot.send_message(
            os.getenv("ADMIN_CHAT_ID", ""),
            f"✅ <b>Оплата підтверджена!</b>\nOrder: {order_id}\nПлан: {payment.tier.upper()}"
        )
    except:
        pass
    return {"ok": True}

@app.get("/api/payment/methods")
def payment_methods():
    """Available payment methods"""
    methods = [
        {"id": "ton_direct", "name": "TON Переказ", "icon": "💎",
         "description": "Пряма оплата в TON. Відкриється гаманець.",
         "supports": ["crypto"], "available": True},
    ]
    if CRYPTOBOT_TOKEN:
        methods.append({
            "id": "cryptobot", "name": "CryptoBot", "icon": "🤖",
            "description": "Крипто, карта, Apple Pay, Google Pay через @CryptoBot",
            "supports": ["crypto", "card", "apple_pay", "google_pay"],
            "available": True
        })
    return {"methods": methods, "wallet": TON_WALLET}

@app.get("/api/payment/history")
def payment_history(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401)
    payments = db.query(Payment).filter(Payment.user_id == user["uid"]).order_by(Payment.created_at.desc()).limit(20).all()
    return [{"order_id": p.order_id, "tier": p.tier, "amount_usd": p.amount_usd,
             "method": p.method, "status": p.status,
             "created_at": p.created_at.isoformat() if p.created_at else None} for p in payments]

# ──── Boss API (admin only via header) ────

@app.get("/boss_panel", dependencies=[Depends(verify_boss_key)])
def boss_panel(lang: str = Query(DEFAULT_LANG, pattern="^(ukr|eng|rus)$"), db: Session = Depends(get_db)):
    return {"message": t("boss_welcome", lang), "hunter_status": hunt_status,
            "users_count": db.query(User).count(),
            "hunted_assets": [{"id":a.id,"category":a.category,"symbol":a.symbol,"price_usd":a.price_usd,"change_pct":a.change_pct,"capture_reason":a.capture_reason} for a in db.query(MarketAsset).filter(MarketAsset.auto_captured==1).order_by(MarketAsset.last_updated.desc()).limit(50).all()],
            "flow_alerts": [{"type":a.alert_type,"severity":a.severity,"message":a.message} for a in db.query(FlowAlert).order_by(FlowAlert.detected_at.desc()).limit(10).all()]}

@app.post("/add_wallet")
async def add_wallet(request: Request, body: WalletCreate, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401, "Увійдіть в систему")
    uid = user["uid"]
    if db.query(Wallet).filter(Wallet.address == body.address, Wallet.user_id == uid).first():
        raise HTTPException(status_code=409, detail="Гаманець вже відстежується.")
    # Tier limit check
    allowed, tier, limit = check_tier_limit(user, "wallets", db)
    wallet_count = db.query(Wallet).filter(Wallet.user_id == uid).count()
    if wallet_count >= limit:
        raise HTTPException(status_code=403, detail=f"Ліміт гаманців ({limit}) для плану {tier.upper()}. Оновіть план!")
    price_data = await crypto_scanner.get_token_price(body.address)
    wallet = Wallet(user_id=uid, address=body.address, blockchain=body.blockchain, label=body.label,
                    asset=body.asset or (price_data.get("symbol") if price_data.get("found") else None))
    if price_data.get("found") and price_data.get("price_usd"):
        wallet.last_price = float(price_data["price_usd"])
        wallet.last_checked = datetime.now(timezone.utc)
    db.add(wallet); db.commit(); db.refresh(wallet)
    return {"status":"added","wallet":{"id":wallet.id,"address":wallet.address,"label":wallet.label,"asset":wallet.asset,"last_price":wallet.last_price},
            "token_info": price_data if price_data.get("found") else None}

@app.delete("/remove_wallet/{wallet_id}")
def remove_wallet(request: Request, wallet_id: int, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401, "Увійдіть в систему")
    wallet = db.query(Wallet).filter(Wallet.id == wallet_id, Wallet.user_id == user["uid"]).first()
    if not wallet: raise HTTPException(status_code=404, detail="Гаманець не знайдено.")
    db.delete(wallet); db.commit()
    return {"status":"removed","id":wallet_id}

@app.post("/boss_panel/hunt_now", dependencies=[Depends(verify_boss_key)])
async def boss_hunt_now():
    result = await global_hunter.hunt_all()
    db = SessionLocal()
    count = 0
    for item in result.get("hunted", []):
        symbol = item.get("symbol","???"); category = item.get("category","CRYPTO")
        existing = db.query(MarketAsset).filter(MarketAsset.symbol==symbol, MarketAsset.category==category).first()
        if not existing:
            db.add(MarketAsset(category=category, symbol=symbol, name=item.get("name"), price_usd=item.get("price_usd"),
                change_pct=item.get("change_24h") or item.get("change_pct"), volume=item.get("volume_24h") or item.get("volume"),
                volume_1h=item.get("volume_1h"), chain=item.get("chain"), auto_captured=1, capture_reason=item.get("capture_reason")))
            count += 1
        else:
            existing.price_usd=item.get("price_usd"); existing.change_pct=item.get("change_24h") or item.get("change_pct")
            existing.volume_1h=item.get("volume_1h"); existing.auto_captured=1
            existing.capture_reason=item.get("capture_reason"); existing.last_updated=datetime.now(timezone.utc)
    db.commit(); db.close()
    return {"status":"hunt_complete","hunted":result["hunted_count"],"new_in_db":count}

@app.post("/boss_panel/insight", dependencies=[Depends(verify_boss_key)])
def add_insight(ticker: str, summary: str, source: Optional[str] = None, db: Session = Depends(get_db)):
    ins = Insight(ticker=ticker, summary=summary, source=source)
    db.add(ins); db.commit(); db.refresh(ins)
    return {"added":ins.ticker,"id":ins.id}


# ──── Push Notifications ────
_push_subscribers = {}  # user_id -> {endpoint, keys, subscribed_at}
_push_file = "push_subs.json"

def _load_push_subs():
    global _push_subscribers
    try:
        if os.path.exists(_push_file):
            import json as _json
            with open(_push_file) as f:
                _push_subscribers = _json.load(f)
    except: pass

def _save_push_subs():
    try:
        import json as _json
        with open(_push_file, "w") as f:
            _json.dump(_push_subscribers, f)
    except: pass

_load_push_subs()

@app.get("/sw.js")
def serve_sw():
    sw_path = BASE_DIR / "sw.js"
    if sw_path.exists():
        return Response(content=sw_path.read_text(), media_type="application/javascript",
                       headers={"Cache-Control": "no-cache", "Service-Worker-Allowed": "/"})
    raise HTTPException(404)

@app.post("/api/push/subscribe")
def push_subscribe(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401)
    import json as _json
    body = asyncio.get_event_loop().run_until_complete(request.json()) if hasattr(request, 'json') else {}
    # Use sync approach
    return {"status": "ok"}

@app.post("/api/push/subscribe_sync")
async def push_subscribe_sync(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401)
    body = await request.json()
    uid = str(user["uid"])
    _push_subscribers[uid] = {
        "endpoint": body.get("endpoint"),
        "keys": body.get("keys"),
        "subscribed_at": datetime.now(timezone.utc).isoformat()
    }
    _save_push_subs()
    return {"status": "subscribed", "user": user["user"]}

@app.delete("/api/push/unsubscribe")
async def push_unsubscribe(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401)
    uid = str(user["uid"])
    _push_subscribers.pop(uid, None)
    _save_push_subs()
    return {"status": "unsubscribed"}

@app.get("/api/push/status")
def push_status(request: Request):
    user = get_current_user(request)
    if not user:
        return {"subscribed": False}
    uid = str(user["uid"])
    return {"subscribed": uid in _push_subscribers, "total_subscribers": len(_push_subscribers)}



# ──── Deep Analytics ────

@app.get("/api/analytics/timeline")
def analytics_timeline(days: int = Query(7, ge=1, le=90), db: Session = Depends(get_db)):
    """Price history timeline for charts"""
    from datetime import timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    records = db.query(HuntHistory).filter(HuntHistory.scanned_at >= cutoff).order_by(HuntHistory.scanned_at.asc()).all()
    return [{"count": h.hunted_count, "crypto": h.crypto_count, "stocks": h.stocks_count,
             "duration": h.scan_duration, "time": h.scanned_at.isoformat() if h.scanned_at else None} for h in records]

@app.get("/api/analytics/top_gainers")
def top_gainers(limit: int = Query(10, ge=1, le=50), db: Session = Depends(get_db)):
    assets = db.query(MarketAsset).filter(MarketAsset.auto_captured == 1, MarketAsset.change_pct != None).order_by(MarketAsset.change_pct.desc()).limit(limit).all()
    return [{"symbol": a.symbol, "category": a.category, "price": a.price_usd,
             "change": a.change_pct, "volume_1h": a.volume_1h} for a in assets]

@app.get("/api/analytics/top_losers")
def top_losers(limit: int = Query(10, ge=1, le=50), db: Session = Depends(get_db)):
    assets = db.query(MarketAsset).filter(MarketAsset.auto_captured == 1, MarketAsset.change_pct != None).order_by(MarketAsset.change_pct.asc()).limit(limit).all()
    return [{"symbol": a.symbol, "category": a.category, "price": a.price_usd,
             "change": a.change_pct, "volume_1h": a.volume_1h} for a in assets]

@app.get("/api/analytics/volume_leaders")
def volume_leaders(limit: int = Query(10, ge=1, le=50), db: Session = Depends(get_db)):
    assets = db.query(MarketAsset).filter(MarketAsset.auto_captured == 1, MarketAsset.volume_1h != None).order_by(MarketAsset.volume_1h.desc()).limit(limit).all()
    return [{"symbol": a.symbol, "category": a.category, "price": a.price_usd,
             "change": a.change_pct, "volume_1h": a.volume_1h} for a in assets]

@app.get("/api/analytics/market_summary")
def market_summary(db: Session = Depends(get_db)):
    """Overall market summary"""
    assets = db.query(MarketAsset).filter(MarketAsset.auto_captured == 1).all()
    if not assets:
        return {"total": 0, "bullish": 0, "bearish": 0, "neutral": 0, "avg_change": 0}
    bullish = len([a for a in assets if (a.change_pct or 0) > 1])
    bearish = len([a for a in assets if (a.change_pct or 0) < -1])
    neutral = len(assets) - bullish - bearish
    changes = [a.change_pct for a in assets if a.change_pct is not None]
    avg_change = round(sum(changes) / len(changes), 2) if changes else 0
    total_vol = sum(a.volume_1h or 0 for a in assets)
    return {"total": len(assets), "bullish": bullish, "bearish": bearish, "neutral": neutral,
            "avg_change": avg_change, "total_volume_1h": total_vol,
            "bullish_pct": round(bullish / len(assets) * 100, 1) if assets else 0,
            "bearish_pct": round(bearish / len(assets) * 100, 1) if assets else 0}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)