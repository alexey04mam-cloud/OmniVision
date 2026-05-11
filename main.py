"""
Omni-Vision v1.1 — Secure Public Platform
"""

import os, json, asyncio, logging, csv, io, secrets, hashlib, re, time, html as html_escape
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
from contextlib import asynccontextmanager
from collections import defaultdict

from fastapi import FastAPI, Request, HTTPException, Depends, Query, Form, Response
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, Float, func, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, Session, relationship
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

from scanners import (
    crypto_scanner, stocks_scanner, commodities_scanner,
    flow_detector, global_hunter,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("omni-vision")

load_dotenv()
BOSS_KEY = os.getenv("BOSS_KEY")
if not BOSS_KEY:
    raise RuntimeError("BOSS_KEY not set in .env")

SECRET_KEY = os.getenv("SECRET_KEY") or secrets.token_hex(32)
SESSION_MAX_AGE = int(os.getenv("SESSION_MAX_AGE", "86400"))

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./omni_vision.db")
HUNT_INTERVAL = int(os.getenv("HUNT_INTERVAL", "60"))
PORT = int(os.getenv("PORT", "8000"))
BASE_DIR = Path(__file__).resolve().parent
DEFAULT_LANG = "ukr"

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
    h = hashlib.sha256((salt + password).encode()).hexdigest()
    return f"{salt}:{h}"

def verify_password(password: str, hashed: str) -> bool:
    try:
        salt, h = hashed.split(":")
        return hashlib.sha256((salt + password).encode()).hexdigest() == h
    except Exception:
        return False

# ──── Models ────

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(64), unique=True, nullable=False, index=True)
    email = Column(String(256), unique=True, nullable=False)
    password_hash = Column(String(256), nullable=False)
    is_admin = Column(Integer, default=0)
    risk_profile = Column(String(16), default="balanced")
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

Base.metadata.create_all(bind=engine)

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
        except Exception as e:
            hunt_status["errors"].append(str(e))
            log.error(f"Мисливець помилка: {e}")
        finally:
            hunt_status["running"] = False
        await asyncio.sleep(HUNT_INTERVAL)

@asynccontextmanager
async def lifespan(app):
    task = None
    if HUNT_INTERVAL > 0:
        task = asyncio.create_task(background_hunter())
        log.info(f"Мисливець запущено (інтервал: {HUNT_INTERVAL}с)")
    else:
        log.info("Мисливець вимкнено (HUNT_INTERVAL=0)")
    yield
    if task:
        task.cancel()

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
        return response

app.add_middleware(SecurityHeadersMiddleware)

# ──── Rate Limiter ────

login_attempts = defaultdict(list)  # ip -> [timestamps]
RATE_LIMIT_MAX = 5
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_BLOCK = 300  # 5 min block

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
    tpl = BASE_DIR / "login.html"
    if not tpl.exists():
        return HTMLResponse("<h1>login.html not found</h1>", status_code=500)
    html = tpl.read_text(encoding="utf-8")
    return HTMLResponse(content=html)

@app.post("/login")
def do_login(request: Request, username: str = Form(...), password: str = Form(...)):
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
        resp.set_cookie(key="omni_session", value=token, max_age=SESSION_MAX_AGE, httponly=True, samesite="lax")
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
    tpl = BASE_DIR / "register.html"
    if not tpl.exists():
        return HTMLResponse("<h1>register.html not found</h1>", status_code=500)
    html = tpl.read_text(encoding="utf-8")
    return HTMLResponse(content=html)

@app.post("/register")
def do_register(request: Request, username: str = Form(...), email: str = Form(...), password: str = Form(...), password2: str = Form(...)):
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
    if len(password) < 6:
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
    resp.set_cookie(key="omni_session", value=token, max_age=SESSION_MAX_AGE, httponly=True, samesite="lax")
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
    tpl = BASE_DIR / "profile.html"
    if not tpl.exists():
        db.close()
        return HTMLResponse("<h1>profile.html not found</h1>", status_code=500)
    html = tpl.read_text(encoding="utf-8")
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
    tpl = BASE_DIR / "dashboard.html"
    if not tpl.exists():
        return HTMLResponse(content="<h1>dashboard.html not found</h1>", status_code=500)
    html = tpl.read_text(encoding="utf-8")
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
    }
    for k, v in replacements.items():
        html = html.replace(k, v)
    return HTMLResponse(content=html)

# ──── API (public data — no auth needed) ────

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
def list_hunted(limit: int = 50, db: Session = Depends(get_db)):
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
def get_hunt_history(limit: int = 50, db: Session = Depends(get_db)):
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
def export_hunted_csv(db: Session = Depends(get_db)):
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
def get_price_history(symbol: str, category: str = "CRYPTO", limit: int = 100, db: Session = Depends(get_db)):
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
    user = get_current_user(request)
    if not user: return []
    items = db.query(WatchlistItem).filter(WatchlistItem.user_id == user["uid"]).order_by(WatchlistItem.added_at.desc()).all()
    result = []
    for w in items:
        asset = db.query(MarketAsset).filter(MarketAsset.symbol == w.symbol, MarketAsset.category == w.category).first()
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

@app.get("/admin", response_class=HTMLResponse)
def admin_page(request: Request):
    user = get_current_user(request)
    if not user or user["user"] != ADMIN_USER:
        return HTMLResponse("<h1>403 Forbidden</h1>", status_code=403)
    db = SessionLocal()
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
<table><tr><th>#</th><th>Ім\'я</th><th>Email</th><th>Стиль</th><th>Роль</th><th>Зареєстрований</th></tr>'''

    for u in users:
        role = '<span class="badge badge-admin">ADMIN</span>' if u.username == ADMIN_USER else '<span class="badge badge-user">USER</span>'
        html += f'<tr><td>{u.id}</td><td>{u.username}</td><td>{u.email}</td><td>{u.risk_profile or "balanced"}</td><td>{role}</td><td>{u.created_at.strftime("%d.%m.%Y %H:%M") if u.created_at else "—"}</td></tr>'

    html += '''</table></div></div></body></html>'''
    db.close()
    return HTMLResponse(content=html)

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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
