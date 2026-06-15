#!/usr/bin/env python3
"""Prediction Market — local virtual betting for friends. Polymarket-backed odds + resolution."""

import asyncio, json, os, re, sqlite3, subprocess, threading, time, urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

APP_DIR = Path(__file__).parent
STATIC_DIR = APP_DIR / "static"
DB_PATH = APP_DIR / "pm.db"
PORT = int(os.environ.get("PORT", 18081))

app = FastAPI(title="PredictionMarket")

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# ── DB ──────────────────────────────────────────────────────────────────────

def get_db():
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")
    return db

def init_db():
    db = get_db()
    db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE,
            password_hash TEXT,
            is_admin INTEGER NOT NULL DEFAULT 0,
            points REAL NOT NULL DEFAULT 1000,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            expires_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);
        CREATE TABLE IF NOT EXISTS markets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            polymarket_condition_id TEXT UNIQUE,
            question TEXT NOT NULL,
            outcomes TEXT NOT NULL,  -- JSON array
            outcome_prices TEXT,      -- JSON array, current Polymarket odds
            volume REAL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'active',  -- active|closed|resolved
            winner_idx INTEGER,       -- NULL until resolved
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            resolved_at TEXT
        );
        CREATE TABLE IF NOT EXISTS bets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            market_id INTEGER NOT NULL REFERENCES markets(id),
            side TEXT NOT NULL,        -- 'yes' or 'no' (maps to outcome index 0 or 1)
            amount REAL NOT NULL,
            odds_at_bet REAL NOT NULL, -- probability at bet time (0-1)
            placed_at TEXT NOT NULL DEFAULT (datetime('now')),
            won INTEGER,               -- NULL=pending, 0=lost, 1=won
            payout REAL                -- NULL until resolved
        );
        CREATE INDEX IF NOT EXISTS idx_bets_user ON bets(user_id, placed_at DESC);
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_name TEXT NOT NULL,
            team_a TEXT NOT NULL,
            team_b TEXT NOT NULL,
            matchday INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'scheduled',
            score_a INTEGER,
            score_b INTEGER,
            winner TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            start_date TEXT,
            outcome_prices TEXT
        );
        CREATE TABLE IF NOT EXISTS match_bets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            match_id INTEGER NOT NULL REFERENCES matches(id),
            pick TEXT NOT NULL,  -- 'team_a', 'draw', 'team_b'
            amount REAL NOT NULL,
            odds REAL NOT NULL,
            placed_at TEXT NOT NULL DEFAULT (datetime('now')),
            won INTEGER,
            payout REAL
        );
        CREATE INDEX IF NOT EXISTS idx_match_bets_user ON match_bets(user_id, placed_at DESC);
        CREATE INDEX IF NOT EXISTS idx_match_bets_match ON match_bets(match_id);
        CREATE TABLE IF NOT EXISTS group_standings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_name TEXT NOT NULL,
            team_name TEXT NOT NULL,
            played INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            draws INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            goals_for INTEGER DEFAULT 0,
            goals_against INTEGER DEFAULT 0,
            goal_diff INTEGER DEFAULT 0,
            points INTEGER DEFAULT 0,
            UNIQUE(group_name, team_name)
        );
        CREATE TABLE IF NOT EXISTS knockout_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            round TEXT NOT NULL,  -- 'R32', 'R16', 'QF', 'SF', '3rd', 'Final'
            slot TEXT NOT NULL,   -- e.g., 'R32-1', 'QF-3'
            team_a TEXT,
            team_b TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            score_a INTEGER,
            score_b INTEGER,
            winner TEXT,
            next_match_id INTEGER REFERENCES knockout_matches(id),
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            outcome_prices TEXT
        );
        CREATE TABLE IF NOT EXISTS knockout_bets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            knockout_match_id INTEGER NOT NULL REFERENCES knockout_matches(id),
            pick TEXT NOT NULL,  -- 'team_a' or 'team_b' (no draws in KO)
            amount REAL NOT NULL,
            odds REAL NOT NULL,
            placed_at TEXT NOT NULL DEFAULT (datetime('now')),
            won INTEGER,
            payout REAL
        );
        CREATE INDEX IF NOT EXISTS idx_ko_bets_user ON knockout_bets(user_id, placed_at DESC);
        CREATE INDEX IF NOT EXISTS idx_ko_bets_match ON knockout_bets(knockout_match_id);
    """)
    db.commit()
    db.close()

    # Migration: add columns to existing users table (CREATE TABLE IF NOT EXISTS
    # doesn't alter pre-existing schemas). SQLite rejects UNIQUE constraints on
    # ALTER TABLE ADD COLUMN, so we add the column plain and build the index
    # separately.
    db = get_db()
    cur_cols = {row["name"] for row in db.execute("PRAGMA table_info(users)").fetchall()}
    plain_migrations = [
        ("email", "ALTER TABLE users ADD COLUMN email TEXT"),
        ("password_hash", "ALTER TABLE users ADD COLUMN password_hash TEXT"),
        ("is_admin", "ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0"),
    ]
    for col, sql in plain_migrations:
        if col not in cur_cols:
            db.execute(sql)
    # Unique index on email (idempotent via IF NOT EXISTS)
    db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON users(email) WHERE email IS NOT NULL")
    db.commit()
    db.close()

init_db()

# ── Background sync (in-process, every 5 min) ─────────────────────────────
# Runs on startup + every 5 min as a daemon thread. Belt-and-suspenders to the
# pm-sync.sh cron — if the cron disappears, the server still keeps itself
# up-to-date. Also gives the UI a "Last sync: X ago" indicator.

import threading, time as _time

_sync_state = {
    "last_run": None,         # datetime ISO
    "last_ok": None,          # bool
    "last_summary": "",       # last line of stdout
    "in_progress": False,
    "next_run": None,         # datetime ISO
    "history": [],             # last 5 runs for display
    "started_at": datetime.now(timezone.utc).isoformat(),
}
_sync_lock = threading.Lock()


def _run_sync_once():
    """Spawn sync.py as subprocess, capture output, update state."""
    with _sync_lock:
        if _sync_state["in_progress"]:
            return  # skip if already running
        _sync_state["in_progress"] = True

    started = datetime.now(timezone.utc)
    try:
        proc = subprocess.run(
            ["python3", str(APP_DIR / "sync.py")],
            capture_output=True, text=True, timeout=240,
            cwd=str(APP_DIR),
        )
        ok = proc.returncode == 0
        # Surface last non-empty line
        summary = ""
        for line in reversed(proc.stdout.splitlines()):
            line = line.strip()
            if line:
                summary = line
                break
        if not ok and proc.stderr:
            summary = (proc.stderr.strip().splitlines() or [""])[-1]
    except Exception as e:
        ok = False
        summary = f"exception: {e}"
    finally:
        with _sync_lock:
            _sync_state["in_progress"] = False
            _sync_state["last_run"] = started.isoformat()
            _sync_state["last_ok"] = ok
            _sync_state["last_summary"] = summary[:200]
            _sync_state["next_run"] = (started + timedelta(minutes=5)).isoformat()
            _sync_state["history"].append({
                "at": started.isoformat(),
                "ok": ok,
                "summary": summary[:200],
            })
            _sync_state["history"] = _sync_state["history"][-5:]


def _sync_loop():
    """DEPRECATED — replaced by launchd jobs:
        com.prediction-market.sync  (full sync every 30s)
        com.prediction-market.live  (live-tick every 60s)
    This loop used to spawn sync.py in-process every 5min but conflicted
    with the launchd jobs on DB writes. Kept as a no-op so the
    on_event("startup") hook doesn't error if it's still wired up.
    """
    while True:
        _time.sleep(3600)  # sleep an hour, do nothing


def get_sync_state():
    with _sync_lock:
        return dict(_sync_state)


# ── Auth helpers ────────────────────────────────────────────────────────────

import hashlib
import hmac
import secrets

SESSION_TTL_DAYS = 30


def _hash_password(password: str, salt: bytes = None) -> str:
    """PBKDF2-HMAC-SHA256. Returns 'salt$hash' hex string. ~50ms at 200k iter."""
    if salt is None:
        salt = secrets.token_bytes(16)
    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
    return f"{salt.hex()}${derived.hex()}"


def _verify_password(password: str, stored: str) -> bool:
    try:
        salt_hex, hash_hex = stored.split("$", 1)
        derived = hashlib.pbkdf2_hmac(
            "sha256", password.encode("utf-8"), bytes.fromhex(salt_hex), 200_000
        )
        return hmac.compare_digest(derived.hex(), hash_hex)
    except (ValueError, AttributeError):
        return False


def _new_token() -> str:
    return secrets.token_urlsafe(32)


def _create_session(db, user_id: int) -> str:
    token = _new_token()
    expires = datetime.now(timezone.utc) + timedelta(days=SESSION_TTL_DAYS)
    db.execute(
        "INSERT INTO sessions (token, user_id, expires_at) VALUES (?,?,?)",
        (token, user_id, expires.isoformat()),
    )
    db.commit()
    return token


def _resolve_token(db, token: str) -> dict | None:
    """Look up a valid (non-expired) session. Returns user row or None."""
    if not token:
        return None
    row = db.execute(
        """SELECT u.* FROM sessions s
           JOIN users u ON u.id = s.user_id
           WHERE s.token=? AND datetime(s.expires_at) > datetime('now')""",
        (token,),
    ).fetchone()
    return dict(row) if row else None


def _extract_token(req: Request) -> str | None:
    """Pull token from Authorization: Bearer <token> or X-Auth-Token header."""
    auth = req.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    return req.headers.get("x-auth-token")


def _current_user(req: Request) -> dict | None:
    """Returns logged-in user dict or None. Caches on request.state."""
    if hasattr(req.state, "user"):
        return req.state.user
    token = _extract_token(req)
    if not token:
        return None
    db = get_db()
    user = _resolve_token(db, token)
    db.close()
    req.state.user = user
    return user


def require_user(req: Request) -> dict:
    """Dependency: 401 if not logged in."""
    user = _current_user(req)
    if not user:
        raise HTTPException(401, "Login required")
    return user


def require_admin(req: Request) -> dict:
    """Dependency: 401/403 for non-admins."""
    user = require_user(req)
    if not user.get("is_admin"):
        raise HTTPException(403, "Admin only")
    return user

# ── Polymarket API helpers ─────────────────────────────────────────────────

POLYMARKET_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

def poly_fetch(url, timeout=10):
    req = urllib.request.Request(url, headers={"User-Agent": POLYMARKET_UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())

def poly_search(query, limit=20):
    """Search Polymarket markets."""
    q = urllib.parse.quote(query)
    data = poly_fetch(f"https://gamma-api.polymarket.com/public-search?q={q}")
    results = []
    for ev in data.get("events", [])[:limit]:
        for m in ev.get("markets", []):
            try:
                prices = json.loads(m.get("outcomePrices", "[]"))
                outcomes = json.loads(m.get("outcomes", "[]"))
            except (json.JSONDecodeError, TypeError):
                continue
            status = "closed" if m.get("closed") else "active"
            winner = None
            if status == "closed" and len(prices) >= 2:
                if prices[0] == "1" and prices[1] == "0":
                    winner = 0
                elif prices[0] == "0" and prices[1] == "1":
                    winner = 1
            results.append({
                "condition_id": m.get("conditionId", ""),
                "question": m.get("question", ""),
                "outcomes": outcomes,
                "prices": [float(p) for p in prices],
                "volume": float(m.get("volume", 0)),
                "status": "resolved" if winner is not None else status,
                "winner_idx": winner,
            })
    return results

def poly_market_by_id(condition_id):
    """Get a single Polymarket market by condition ID for resolution check."""
    encoded = urllib.parse.quote(condition_id)
    data = poly_fetch(f"https://gamma-api.polymarket.com/markets?slug=&limit=5")
    # Not ideal — Polymarket doesn't have a direct condition_id lookup in Gamma.
    # Use the CLOB API instead.
    return None

def check_resolution(condition_id):
    """Check if a Polymarket market has resolved. Returns winner_idx or None."""
    try:
        encoded = urllib.parse.quote(condition_id)
        data = poly_fetch(
            f"https://gamma-api.polymarket.com/markets?closed=true&limit=100"
        )
        for m in data:
            if m.get("conditionId") == condition_id:
                prices = json.loads(m.get("outcomePrices", "[]"))
                if len(prices) >= 2:
                    if prices[0] == "1" and prices[1] == "0":
                        return 0
                    elif prices[0] == "0" and prices[1] == "1":
                        return 1
                return None
        return None
    except Exception:
        return None

# ── API Routes ──────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    from fastapi.responses import RedirectResponse
    return RedirectResponse("/worldcup")

# ── Users ───────────────────────────────────────────────────────────────────

@app.get("/api/users")
def list_users():
    db = get_db()
    users = [dict(r) for r in db.execute("SELECT * FROM users ORDER BY points DESC").fetchall()]
    db.close()
    return users

@app.post("/api/users")
async def create_user(req: Request):
    body = await req.json()
    name = body.get("name", "").strip()
    if not name:
        raise HTTPException(400, "Name required")
    db = get_db()
    try:
        db.execute("INSERT INTO users (name) VALUES (?)", (name,))
        db.commit()
        uid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        user = dict(db.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone())
        db.close()
        return user
    except sqlite3.IntegrityError:
        db.close()
        raise HTTPException(409, "Name taken")

# ── Auth ────────────────────────────────────────────────────────────────────

# Backwards-compat: returns the resolved current user (or the user from query)
# so legacy endpoints can still accept ?user_id=X for admin scripts.
# In the bet/place paths the body is overridden by require_user().

@app.post("/api/auth/register")
async def register(req: Request):
    _rate_limit_auth(req)
    body = await req.json()
    name = (body.get("name") or "").strip()
    password = body.get("password") or ""
    email = (body.get("email") or "").strip().lower() or None

    if len(name) < 2 or len(name) > 32:
        raise HTTPException(400, "Name must be 2-32 chars")
    if not (4 <= len(password) <= 128):
        raise HTTPException(400, "Password must be 4-128 chars")
    if email and ("@" not in email or "." not in email.split("@")[-1]):
        raise HTTPException(400, "Invalid email")

    db = get_db()
    # First registered user becomes admin
    user_count = db.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    is_admin = 1 if user_count == 0 else 0

    try:
        cur = db.execute(
            "INSERT INTO users (name, email, password_hash, is_admin) VALUES (?,?,?,?)",
            (name, email, _hash_password(password), is_admin),
        )
        db.commit()
        uid = cur.lastrowid
    except sqlite3.IntegrityError as e:
        db.close()
        if "users.name" in str(e) or "name" in str(e).lower():
            raise HTTPException(409, "Name already taken")
        if "users.email" in str(e) or "email" in str(e).lower():
            raise HTTPException(409, "Email already registered")
        raise HTTPException(409, "Conflict")

    token = _create_session(db, uid)
    user = dict(db.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone())
    db.close()
    user.pop("password_hash", None)
    return {"token": token, "user": user}


@app.post("/api/auth/login")
async def login(req: Request):
    _rate_limit_auth(req)
    body = await req.json()
    name = (body.get("name") or body.get("email") or "").strip()
    password = body.get("password") or ""
    if not name or not password:
        raise HTTPException(400, "Name and password required")

    db = get_db()
    row = db.execute(
        "SELECT * FROM users WHERE LOWER(name)=LOWER(?) OR LOWER(email)=LOWER(?)",
        (name, name),
    ).fetchone()
    db.close()
    if not row or not row["password_hash"]:
        # Same generic error to avoid user-enumeration
        raise HTTPException(401, "Invalid credentials")
    if not _verify_password(password, row["password_hash"]):
        raise HTTPException(401, "Invalid credentials")

    db = get_db()
    token = _create_session(db, row["id"])
    db.close()
    user = dict(row)
    user.pop("password_hash", None)
    return {"token": token, "user": user}


@app.post("/api/auth/logout")
async def logout(req: Request):
    token = _extract_token(req)
    if token:
        db = get_db()
        db.execute("DELETE FROM sessions WHERE token=?", (token,))
        db.commit()
        db.close()
    return {"ok": True}


@app.get("/api/auth/me")
async def me(req: Request):
    user = _current_user(req)
    if not user:
        raise HTTPException(401, "Not logged in")
    user.pop("password_hash", None)
    return user


# Lightweight in-process rate limit for auth endpoints. Backs off bursts from
# a single IP — sufficient for a friend-LAN app, not a substitute for a real
# limit at the edge if exposed publicly.
_auth_attempts: dict[str, list[float]] = {}
AUTH_RATE_LIMIT = 10  # requests
AUTH_RATE_WINDOW = 60  # seconds


def _rate_limit_auth(req: Request):
    # Skip rate limit in test mode (set DISABLE_AUTH_RATE_LIMIT=1) so the
    # test suite can register/login many users rapidly.
    if os.environ.get("DISABLE_AUTH_RATE_LIMIT") == "1":
        return
    ip = (req.client.host if req.client else "unknown")
    now = time.time()
    attempts = _auth_attempts.setdefault(ip, [])
    # Drop expired
    attempts[:] = [t for t in attempts if now - t < AUTH_RATE_WINDOW]
    if len(attempts) >= AUTH_RATE_LIMIT:
        raise HTTPException(429, "Too many auth attempts, slow down")
    attempts.append(now)

# ── Markets ─────────────────────────────────────────────────────────────────

@app.get("/api/markets")
def list_markets(status: str = None):
    db = get_db()
    if status:
        rows = db.execute(
            "SELECT * FROM markets WHERE status=? ORDER BY created_at DESC", (status,)
        ).fetchall()
    else:
        rows = db.execute("SELECT * FROM markets ORDER BY created_at DESC").fetchall()
    markets = []
    for r in rows:
        m = dict(r)
        m["outcomes"] = json.loads(m["outcomes"])
        m["outcome_prices"] = json.loads(m["outcome_prices"]) if m["outcome_prices"] else []
        # Count bets
        bet_count = db.execute(
            "SELECT COUNT(*) FROM bets WHERE market_id=?", (m["id"],)
        ).fetchone()[0]
        m["bet_count"] = bet_count
        markets.append(m)
    db.close()
    return markets

@app.post("/api/markets/search")
async def search_markets(req: Request):
    body = await req.json()
    query = body.get("query", "").strip()
    if not query:
        raise HTTPException(400, "Query required")
    results = poly_search(query)
    return results

@app.post("/api/markets/import")
async def import_market(req: Request):
    """Import a Polymarket market into local DB for betting."""
    body = await req.json()
    condition_id = body.get("condition_id")
    question = body.get("question")
    outcomes = body.get("outcomes", ["Yes", "No"])
    prices = body.get("prices", [0.5, 0.5])
    volume = body.get("volume", 0)
    status = body.get("status", "active")
    winner_idx = body.get("winner_idx")

    if not condition_id:
        raise HTTPException(400, "condition_id required")

    db = get_db()
    existing = db.execute(
        "SELECT id FROM markets WHERE polymarket_condition_id=?", (condition_id,)
    ).fetchone()

    if existing:
        # Update prices and status
        db.execute(
            "UPDATE markets SET outcome_prices=?, volume=?, status=?, winner_idx=? WHERE id=?",
            (json.dumps(prices), volume, status, winner_idx, existing["id"]),
        )
        db.commit()
        m = dict(db.execute("SELECT * FROM markets WHERE id=?", (existing["id"],)).fetchone())
        db.close()
        m["outcomes"] = json.loads(m["outcomes"])
        m["outcome_prices"] = json.loads(m["outcome_prices"]) if m["outcome_prices"] else []
        return m

    db.execute(
        "INSERT INTO markets (polymarket_condition_id, question, outcomes, outcome_prices, volume, status, winner_idx) VALUES (?,?,?,?,?,?,?)",
        (condition_id, question, json.dumps(outcomes), json.dumps(prices), volume, status, winner_idx),
    )
    db.commit()
    mid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
    m = dict(db.execute("SELECT * FROM markets WHERE id=?", (mid,)).fetchone())
    db.close()
    m["outcomes"] = json.loads(m["outcomes"])
    m["outcome_prices"] = json.loads(m["outcome_prices"]) if m["outcome_prices"] else []
    return m

@app.post("/api/markets/refresh")
async def refresh_market(req: Request):
    """Refresh a single market's odds from Polymarket."""
    body = await req.json()
    market_id = body.get("market_id")
    db = get_db()
    m = db.execute("SELECT * FROM markets WHERE id=?", (market_id,)).fetchone()
    if not m:
        db.close()
        raise HTTPException(404, "Market not found")

    cid = m["polymarket_condition_id"]
    results = poly_search(m["question"], limit=1)

    updated = False
    for r in results:
        if r["condition_id"] == cid:
            db.execute(
                "UPDATE markets SET outcome_prices=?, volume=?, status=?, winner_idx=? WHERE id=?",
                (json.dumps(r["prices"]), r["volume"], r["status"], r["winner_idx"], market_id),
            )
            db.commit()
            updated = True
            break

    if not updated:
        # Fallback: check closed markets
        winner = check_resolution(cid)
        if winner is not None:
            db.execute(
                "UPDATE markets SET status='resolved', winner_idx=?, resolved_at=datetime('now') WHERE id=?",
                (winner, market_id),
            )
            # Settle pending bets for this market (was missing — bets stayed
            # un-settled while market flipped to resolved, leaving P&L dangling).
            for b in db.execute(
                "SELECT * FROM bets WHERE market_id=? AND won IS NULL", (market_id,)
            ).fetchall():
                bet_winner_idx = 0 if b["side"] == "yes" else 1
                won = 1 if bet_winner_idx == winner else 0
                payout = (b["amount"] / b["odds_at_bet"]) if won and b["odds_at_bet"] > 0 else 0.0
                db.execute(
                    "UPDATE bets SET won=?, payout=? WHERE id=?", (won, payout, b["id"])
                )
                if payout > 0:
                    db.execute(
                        "UPDATE users SET points = points + ? WHERE id=?", (payout, b["user_id"])
                    )
            db.commit()
            updated = True

    m = dict(db.execute("SELECT * FROM markets WHERE id=?", (market_id,)).fetchone())
    db.close()
    m["outcomes"] = json.loads(m["outcomes"])
    m["outcome_prices"] = json.loads(m["outcome_prices"]) if m["outcome_prices"] else []
    return m

# ── Bets ────────────────────────────────────────────────────────────────────

@app.post("/api/bets")
async def place_bet(req: Request):
    me = require_user(req)
    body = await req.json()
    # user_id from auth, not body. Admins can override via body.user_id.
    user_id = me["id"]
    if body.get("user_id") and body["user_id"] != me["id"] and not me.get("is_admin"):
        raise HTTPException(403, "Cannot bet on another user's behalf")
    if me.get("is_admin") and body.get("user_id"):
        user_id = int(body["user_id"])

    market_id = body.get("market_id")
    side = body.get("side")  # "yes" or "no"
    amount = float(body.get("amount", 0))

    if not all([market_id, side, amount]):
        raise HTTPException(400, "market_id, side, amount required")
    if side not in ("yes", "no"):
        raise HTTPException(400, "side must be yes or no")
    if amount <= 0:
        raise HTTPException(400, "amount must be positive")
    if amount > 1_000_000:
        raise HTTPException(400, "amount too large (max 1M)")

    db = get_db()
    # Transaction with row-locked points check — prevents concurrent
    # overdraw where two bets both pass the SELECT and then both UPDATE.
    db.execute("BEGIN IMMEDIATE")
    try:
        market = db.execute("SELECT * FROM markets WHERE id=?", (market_id,)).fetchone()
        if not market:
            db.execute("ROLLBACK"); db.close()
            raise HTTPException(404, "Market not found")
        if market["status"] != "active":
            db.execute("ROLLBACK"); db.close()
            raise HTTPException(400, f"Market is {market['status']}")
        prices = json.loads(market["outcome_prices"]) if market["outcome_prices"] else [0.5, 0.5]
        odds = prices[0] if side == "yes" else prices[1]
        if odds <= 0:
            odds = 0.01

        cur = db.execute(
            "UPDATE users SET points = points - ? WHERE id=? AND points >= ?",
            (amount, user_id, amount),
        )
        if cur.rowcount == 0:
            db.execute("ROLLBACK"); db.close()
            raise HTTPException(400, "Insufficient points")

        db.execute(
            "INSERT INTO bets (user_id, market_id, side, amount, odds_at_bet) VALUES (?,?,?,?,?)",
            (user_id, market_id, side, amount, odds),
        )
        db.commit()
        bid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        bet = dict(db.execute("SELECT * FROM bets WHERE id=?", (bid,)).fetchone())
        user = dict(db.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone())
    except HTTPException:
        raise
    except Exception as e:
        db.execute("ROLLBACK"); db.close()
        raise HTTPException(500, f"Bet failed: {e}")
    db.close()
    return {"bet": bet, "user": user}

@app.get("/api/bets")
def list_bets(user_id: int = None, market_id: int = None):
    db = get_db()
    q = "SELECT b.*, u.name as user_name, m.question as market_question FROM bets b JOIN users u ON b.user_id = u.id JOIN markets m ON b.market_id = m.id"
    conds = []
    params = []
    if user_id:
        conds.append("b.user_id=?")
        params.append(user_id)
    if market_id:
        conds.append("b.market_id=?")
        params.append(market_id)
    if conds:
        q += " WHERE " + " AND ".join(conds)
    q += " ORDER BY b.placed_at DESC"
    rows = db.execute(q, params).fetchall()
    db.close()
    return [dict(r) for r in rows]

# ── Resolution ──────────────────────────────────────────────────────────────

@app.post("/api/admin/resolve-all")
async def resolve_all():
    """Check all active markets for resolution, settle bets."""
    db = get_db()
    active = db.execute(
        "SELECT * FROM markets WHERE status='active' OR status='closed'"
    ).fetchall()

    resolved_count = 0
    for m in active:
        cid = m["polymarket_condition_id"]
        winner = check_resolution(cid)
        if winner is not None:
            # Resolve this market
            db.execute(
                "UPDATE markets SET status='resolved', winner_idx=?, resolved_at=datetime('now') WHERE id=?",
                (winner, m["id"]),
            )
            # Settle all bets for this market
            bets = db.execute(
                "SELECT * FROM bets WHERE market_id=? AND won IS NULL", (m["id"],)
            ).fetchall()
            for b in bets:
                bet_winner_idx = 0 if b["side"] == "yes" else 1
                won = 1 if bet_winner_idx == winner else 0
                payout = 0.0
                if won:
                    # Payout = amount / odds_at_bet
                    odds = b["odds_at_bet"]
                    if odds > 0:
                        payout = b["amount"] / odds
                db.execute(
                    "UPDATE bets SET won=?, payout=? WHERE id=?",
                    (won, payout, b["id"]),
                )
                if payout > 0:
                    db.execute(
                        "UPDATE users SET points = points + ? WHERE id=?",
                        (payout, b["user_id"]),
                    )
            resolved_count += 1

    db.commit()
    db.close()
    return {"resolved": resolved_count}

# ── Leaderboard ─────────────────────────────────────────────────────────────

@app.get("/api/leaderboard")
def leaderboard():
    db = get_db()
    users = db.execute("""
        SELECT u.*,
            COUNT(b.id) as total_bets,
            COALESCE(SUM(CASE WHEN b.won=1 THEN 1 ELSE 0 END), 0) as wins,
            COALESCE(SUM(CASE WHEN b.won=0 THEN 1 ELSE 0 END), 0) as losses,
            COALESCE(SUM(CASE WHEN b.won IS NULL THEN 1 ELSE 0 END), 0) as pending
        FROM users u
        LEFT JOIN bets b ON u.id = b.user_id
        GROUP BY u.id
        ORDER BY u.points DESC
    """).fetchall()
    db.close()
    return [dict(r) for r in users]

# ── World Cup 2026 ───────────────────────────────────────────────────────────

WC_GROUPS = {
    "A": ["Mexico", "South Korea", "South Africa", "Czechia"],
    "B": ["Switzerland", "Canada", "Bosnia-Herzegovina", "Qatar"],
    "C": ["Brazil", "Morocco", "Scotland", "Haiti"],
    "D": ["USA", "Türkiye", "Paraguay", "Australia"],
    "E": ["Germany", "Ecuador", "Ivory Coast", "Curaçao"],
    "F": ["Netherlands", "Japan", "Sweden", "Tunisia"],
    "G": ["Belgium", "Iran", "Egypt", "New Zealand"],
    "H": ["Spain", "Uruguay", "Saudi Arabia", "Cape Verde"],
    "I": ["France", "Norway", "Senegal", "Iraq"],
    "J": ["Argentina", "Austria", "Algeria", "Jordan"],
    "K": ["Portugal", "Colombia", "DR Congo", "Uzbekistan"],
    "L": ["England", "Croatia", "Ghana", "Panama"],
}

@app.get("/worldcup", response_class=HTMLResponse)
async def worldcup_page():
    # Aggressive no-cache so the browserbase (and any other) client always
    # picks up the latest JS. Without this, browserbase's persistent cache
    # held onto an old version of WORLDCUP_HTML (where `renderAdvance` was
    # undefined) even after the server was restarted, making the Advance
    # tab show "Loading..." forever.
    headers = {
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
        "Expires": "0",
    }
    return HTMLResponse(WORLDCUP_HTML, headers=headers)

@app.get("/api/worldcup/groups")
def worldcup_groups():
    """Return groups with team odds from Polymarket markets + OKX tournament
    win % for top teams. The DB stores both signals so the UI can show the
    per-match odds (Polymarket-derived) alongside the team's tournament win
    % (OKX) when available — making it clear they're different metrics."""
    db = get_db()
    okx_metrics = {r['team_name']: r['okx_tournament_win_pct'] for r in db.execute("SELECT team_name, okx_tournament_win_pct FROM team_metrics").fetchall()}
    # Polymarket uses different spellings for a few teams. Map the
    # canonical WC_GROUPS name to the alternates the search LIKE pattern
    # should also try.
    TEAM_ALIASES = {
        'Bosnia-Herzegovina': ['Bosnia and Herzegovina'],
        'Türkiye':            ['Turkiye'],
        'Curaçao':            ['Curacao'],
    }
    def _search(category, team, anchor):
        """Find an active Polymarket market whose question contains `team`
        (or one of its aliases) and `anchor` (e.g. 'win Group A')."""
        names = [team] + TEAM_ALIASES.get(team, [])
        for n in names:
            r = db.execute(
                "SELECT outcome_prices FROM markets WHERE category=? AND question LIKE ? AND status='active' LIMIT 1",
                (category, f"%{n}{anchor}%"),
            ).fetchone()
            if r: return r
        return None
    result = {}
    for g_name, teams in WC_GROUPS.items():
        group_data = {"name": g_name, "teams": []}
        for team in teams:
            # Polymarket titles:
            #   group:  "Will {team} win Group {g_name} in the 2026 FIFA World Cup?"
            #   advance:"Will {team} reach the Round of 16 at the 2026 FIFA World Cup?"
            # Use the `category` column (set by sync) instead of LIKE-on-question
            # so the question text format (which uses "reach", not "advance")
            # doesn't matter.
            win_m  = _search('group_winner',  team, f" win Group {g_name}")
            adv_m  = _search('advance_r32',   team, '')
            win_odds = json.loads(win_m["outcome_prices"])[0] if win_m else 0
            adv_odds = json.loads(adv_m["outcome_prices"])[0] if adv_m else 0
            group_data["teams"].append({
                "name": team,
                "win_group_pct": round(win_odds * 100, 1),
                "advance_pct": round(adv_odds * 100, 1),
                "okx_tournament_win_pct": round(okx_metrics.get(team, 0) * 100, 1) if okx_metrics.get(team) else None,
            })
        result[g_name] = group_data
    db.close()
    return result

@app.get("/api/worldcup/matches")
def worldcup_matches(group: str = None, status: str = None):
    db = get_db()
    where, params = [], []
    if group: where.append("group_name=?"); params.append(group)
    if status: where.append("status=?"); params.append(status)
    q = "SELECT * FROM matches"
    if where: q += " WHERE " + " AND ".join(where)
    q += " ORDER BY group_name, matchday, id"
    rows = db.execute(q, params).fetchall()
    matches = []
    for r in rows:
        m = dict(r)
        # Count bets
        bc = db.execute("SELECT COUNT(*) FROM match_bets WHERE match_id=?", (m["id"],)).fetchone()[0]
        m["bet_count"] = bc
        matches.append(m)
    db.close()
    return matches

@app.post("/api/worldcup/bet")
async def worldcup_place_bet(req: Request):
    me = require_user(req)
    body = await req.json()
    user_id = me["id"]
    if body.get("user_id") and body["user_id"] != me["id"] and not me.get("is_admin"):
        raise HTTPException(403, "Cannot bet on another user's behalf")
    if me.get("is_admin") and body.get("user_id"):
        user_id = int(body["user_id"])

    match_id = body.get("match_id")
    pick = body.get("pick")  # 'team_a', 'draw', 'team_b'
    side = body.get("side", "yes")  # 'yes' (this outcome happens) or 'no' (this outcome doesn't)
    try:
        amount = float(body.get("amount", 0))
    except (TypeError, ValueError):
        raise HTTPException(400, "amount must be a number")
    if amount != amount or amount in (float("inf"), float("-inf")):
        raise HTTPException(400, "amount must be a finite number")

    if not all([match_id, pick, amount]) or amount <= 0:
        raise HTTPException(400, "match_id, pick, amount required")
    if pick not in ("team_a", "draw", "team_b"):
        raise HTTPException(400, "pick must be team_a, draw, or team_b")
    if side not in ("yes", "no"):
        raise HTTPException(400, "side must be 'yes' or 'no'")
    if amount > 1_000_000:
        raise HTTPException(400, "amount too large (max 1M)")

    db = get_db()
    # Transaction with row-locked points check (see /api/bets for rationale).
    db.execute("BEGIN IMMEDIATE")
    try:
        match = db.execute("SELECT * FROM matches WHERE id=?", (match_id,)).fetchone()
        if not match:
            db.execute("ROLLBACK"); db.close()
            raise HTTPException(404, "Match not found")
        if match["status"] != "scheduled":
            db.execute("ROLLBACK"); db.close()
            raise HTTPException(400, f"Match is {match['status']}")

        # Refuse to bet without Polymarket-synced odds — no fake/estimated odds.
        # The UI already blocks this; the API must too (otherwise direct calls bypass
        # the "no fake odds" rule). Sync populates outcome_prices from OKX/Polymarket
        # within 5 min of match creation.
        if not match["outcome_prices"]:
            db.execute("ROLLBACK"); db.close()
            raise HTTPException(
                409,
                "No Polymarket odds for this match yet. Wait for next sync (every 5 min).",
            )

        # Use per-match Polymarket odds if already synced (live order book).
        # Otherwise compute fair odds from group win probabilities.
        # (match is a sqlite3.Row, not a dict — use bracket access.)
        pm_stored = match["outcome_prices"]
        if pm_stored:
            try:
                pm = json.loads(pm_stored)
                a_prob = float(pm.get("moneyline_home", 0))
                draw_prob = float(pm.get("draw", 0))
                away_total = float(pm.get("moneyline_away", 0))
                b_prob = away_total - draw_prob
                if a_prob > 0 and draw_prob > 0 and b_prob > 0:
                    odds_map = {"team_a": a_prob, "draw": draw_prob, "team_b": b_prob}
                    yes_odds = odds_map.get(pick, 0.33)
                    if yes_odds <= 0:
                        yes_odds = 0.01
                else:
                    pm_stored = None
            except (json.JSONDecodeError, TypeError, ValueError):
                pm_stored = None

        if not pm_stored:
            team_a_odds = _get_team_odds(db, match["team_a"], match["group_name"])
            team_b_odds = _get_team_odds(db, match["team_b"], match["group_name"])
            total = team_a_odds + team_b_odds
            if total == 0:
                total = 1
            a_share = team_a_odds / total
            b_share = team_b_odds / total
            diff = abs(a_share - b_share)
            draw_prob = max(0.05, min(0.40, 0.30 - 0.20 * diff))
            win_share = 1.0 - draw_prob
            a_prob = a_share * win_share
            b_prob = b_share * win_share
            odds_map = {"team_a": a_prob, "draw": draw_prob, "team_b": b_prob}
            yes_odds = odds_map.get(pick, 0.33)
            if yes_odds <= 0:
                yes_odds = 0.01

        # Bet model: `amount` (input) is the WAGER — the points the user pays
        # upfront. If the bet wins, the payout is amount / odds (YES) or
        # amount / (1 - odds) (NO). This matches the user's intuition:
        # "what I pay" + "what I get back" + "profit". Equivalent to Polymarket
        # where amount = number-of-shares and cost = amount × price.
        wager = round(float(amount), 2)  # wager is exactly the user-typed amount
        stored_odds = round(float(yes_odds), 4)  # canonical YES price; used by sync for settlement
        # Calculate the implied payout if this bet wins (informational only;
        # settlement recomputes from stored wager + odds at sync time).
        if side == "yes":
            payout_if_win = round(wager / yes_odds, 2) if yes_odds > 0 else 0
        else:
            payout_if_win = round(wager / (1.0 - yes_odds), 2) if yes_odds < 1 else 0
        # Store the YES price as the canonical odds (for transparency +
        # settlement). `wager` is stored in the `amount` column (legacy name).

        cur = db.execute(
            "UPDATE users SET points = points - ? WHERE id=? AND points >= ?",
            (wager, user_id, wager),
        )
        if cur.rowcount == 0:
            db.execute("ROLLBACK"); db.close()
            raise HTTPException(400, "Insufficient points")

        db.execute(
            "INSERT INTO match_bets (user_id, match_id, pick, amount, odds, side) VALUES (?,?,?,?,?,?)",
            (user_id, match_id, pick, wager, stored_odds, side),
        )
        db.commit()
        bid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        bet = dict(db.execute("SELECT * FROM match_bets WHERE id=?", (bid,)).fetchone())
        user = dict(db.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone())
    except HTTPException:
        raise
    except Exception as e:
        db.execute("ROLLBACK"); db.close()
        raise HTTPException(500, f"Bet failed: {e}")
    db.close()
    return {"bet": bet, "user": user, "wager": wager, "payout_if_win": payout_if_win, "side": side, "pays_on": "hit" if side == "yes" else "miss"}

@app.get("/api/worldcup/advance-markets")
def worldcup_advance_markets():
    """Return per-round advance markets for the 48 nations, grouped by
    team. Categories: advance_r32 (R16 from group), advance_r16 (QF),
    advance_qf (SF), advance_final (champion). All markets are YES/NO
    binary — the YES price is `outcome_prices[0]`. The user can bet on
    any team advancing (or NOT advancing) to the next round.

    Example response:
    {
      "Brazil": [
        {"id": 1, "category": "advance_r32", "question": "...", "yes_price": 0.95, "volume": 1000},
        ...
      ],
      ...
    }"""
    db = get_db()
    rows = db.execute("""
        SELECT id, polymarket_condition_id, question, outcome_prices, volume,
               category, team_name, status
        FROM markets
        WHERE status='active' AND category LIKE 'advance%'
        ORDER BY team_name, category
    """).fetchall()
    db.close()
    grouped = {}
    for r in rows:
        try:
            prices = json.loads(r["outcome_prices"] or "[]")
        except Exception:
            prices = []
        yes_price = float(prices[0]) if len(prices) > 0 else 0
        no_price = float(prices[1]) if len(prices) > 1 else (1 - yes_price)
        team = r["team_name"] or "?"
        grouped.setdefault(team, []).append({
            "id": r["id"],
            "polymarket_condition_id": r["polymarket_condition_id"],
            "category": r["category"],
            "question": r["question"],
            "yes_price": yes_price,
            "no_price": no_price,
            "volume": r["volume"] or 0,
        })
    return grouped


# Stage → category map for settlement
STAGE_TO_CATEGORY = {
    "R32": "advance_r16",   # winning R32 means team advanced to R16
    "R16": "advance_qf",    # winning R16 means team advanced to QF
    "QF":  "advance_sf",    # winning QF means team advanced to SF
    "SF":  "advance_final", # winning SF means team advanced to Final
    "Final": "advance_final",  # winning Final means tournament winner
}


@app.get("/api/worldcup/leaderboard")
def worldcup_leaderboard():
    db = get_db()
    users = db.execute("""
        SELECT u.*,
            COUNT(mb.id) as total_match_bets,
            COALESCE(SUM(CASE WHEN mb.won=1 THEN 1 ELSE 0 END), 0) as wins,
            COALESCE(SUM(CASE WHEN mb.won=0 THEN 1 ELSE 0 END), 0) as losses
        FROM users u
        LEFT JOIN match_bets mb ON u.id = mb.user_id
        GROUP BY u.id
        ORDER BY u.points DESC
    """).fetchall()
    db.close()
    return [dict(r) for r in users]


@app.get("/api/history/matches")
def match_history(limit: int = 30):
    """Completed match history with results."""
    db = get_db()
    rows = db.execute("""
        SELECT m.*,
            COUNT(mb.id) as total_bets,
            COALESCE(SUM(CASE WHEN mb.won=1 THEN 1 ELSE 0 END), 0) as winning_bets
        FROM matches m
        LEFT JOIN match_bets mb ON m.id = mb.match_id
        WHERE m.status = 'completed'
        GROUP BY m.id
        ORDER BY m.start_date DESC, m.id DESC
        LIMIT ?
    """, (limit,)).fetchall()
    db.close()
    return [dict(r) for r in rows]


# ── Combined history per user ───────────────────────────────────────────────

def _pick_label(team_a: str, team_b: str, pick: str, side: str, allow_draw: bool = True) -> str:
    """Build the human-readable bet label from (team_a, team_b, pick, side).
    pick ∈ {'team_a','draw','team_b'}; side ∈ {'yes','no'}. When side='no',
    the label flips to indicate the user is betting the outcome does NOT
    happen — otherwise the history would show e.g. 'Germany Win' for a
    bet that actually wins when Germany doesn't win.
    """
    base = {
        'team_a': team_a + " Win",
        'draw':   "Draw",
        'team_b': team_b + " Win",
    }.get(pick, "?")
    if side == "yes":
        return base
    # side == 'no' → negate. For match bets (allow_draw) a team NOT-winning
    # means the OTHER team wins OR a draw; for knockout there is no draw so
    # team NOT-winning means the other team wins.
    if pick == 'team_a': return team_a + " NOT Win"
    if pick == 'team_b': return team_b + " NOT Win"
    if pick == 'draw':   return "NOT Draw"
    return base

@app.get("/api/users/{user_id}/history")
def user_history(user_id: int, limit: int = 50, offset: int = 0, scope: str = "all"):
    """Per-user bet history across all bet types. PUBLIC — friends can see
    each other's history for transparency. scope=match|market|knockout|all.
    """
    limit = max(1, min(limit, 200))
    offset = max(0, offset)
    db = get_db()
    out = {"market_bets": [], "match_bets": [], "knockout_bets": []}
    totals = {"wagered": 0.0, "returned": 0.0, "net": 0.0, "wins": 0, "losses": 0, "pending": 0}

    if scope in ("all", "market"):
        mb = db.execute("""
            SELECT b.*, m.question as market_question
            FROM bets b JOIN markets m ON m.id = b.market_id
            WHERE b.user_id=?
            ORDER BY b.placed_at DESC LIMIT ? OFFSET ?
        """, (user_id, limit, offset)).fetchall()
        for r in mb:
            d = dict(r)
            totals["wagered"] += d["amount"]
            totals["returned"] += (d.get("payout") or 0)
            if d["won"] == 1:
                d["result"] = f"Won +{(d.get('payout') or 0):.0f}"; totals["wins"] += 1
            elif d["won"] == 0:
                d["result"] = f"Lost -{d['amount']:.0f}"; totals["losses"] += 1
            else:
                d["result"] = "Pending"; totals["pending"] += 1
            out["market_bets"].append(d)

    if scope in ("all", "match"):
        mb2 = db.execute("""
            SELECT mb.*, m.team_a, m.team_b, m.group_name, m.start_date, m.status as match_status,
                   m.score_a, m.score_b, m.winner as match_winner
            FROM match_bets mb JOIN matches m ON m.id = mb.match_id
            WHERE mb.user_id=?
            ORDER BY mb.placed_at DESC LIMIT ? OFFSET ?
        """, (user_id, limit, offset)).fetchall()
        for r in mb2:
            d = dict(r)
            totals["wagered"] += d["amount"]
            totals["returned"] += (d.get("payout") or 0)
            # side may be missing on legacy rows (pre-Nov 2024) — treat as 'yes'.
            side = d.get("side") or "yes"
            pick_label = _pick_label(d["team_a"], d["team_b"], d["pick"], side, allow_draw=True)
            if d["won"] == 1:
                d["result"] = f"Won +{(d.get('payout') or 0):.0f}"; totals["wins"] += 1
            elif d["won"] == 0:
                d["result"] = f"Lost -{d['amount']:.0f}"; totals["losses"] += 1
            else:
                d["result"] = "Pending"; totals["pending"] += 1
            d["match_label"] = f"{d['team_a']} vs {d['team_b']} (Group {d['group_name']})"
            d["pick_label"] = pick_label
            out["match_bets"].append(d)

    if scope in ("all", "knockout"):
        kb = db.execute("""
            SELECT kb.*, km.round, km.slot, km.team_a, km.team_b, km.status as match_status,
                   km.winner as match_winner
            FROM knockout_bets kb JOIN knockout_matches km ON km.id = kb.knockout_match_id
            WHERE kb.user_id=?
            ORDER BY kb.placed_at DESC LIMIT ? OFFSET ?
        """, (user_id, limit, offset)).fetchall()
        for r in kb:
            d = dict(r)
            totals["wagered"] += d["amount"]
            totals["returned"] += (d.get("payout") or 0)
            side = d.get("side") or "yes"
            pick_label = _pick_label(d["team_a"] or "?", d["team_b"] or "?", d["pick"], side, allow_draw=False)
            if d["won"] == 1:
                d["result"] = f"Won +{(d.get('payout') or 0):.0f}"; totals["wins"] += 1
            elif d["won"] == 0:
                d["result"] = f"Lost -{d['amount']:.0f}"; totals["losses"] += 1
            else:
                d["result"] = "Pending"; totals["pending"] += 1
            d["match_label"] = f"{d['team_a']} vs {d['team_b']} ({d['round']})"
            d["pick_label"] = pick_label
            out["knockout_bets"].append(d)

    totals["net"] = totals["returned"] - totals["wagered"]
    db.close()
    return {"totals": totals, **out}


# ── Active bets (pending only) for the dashboard tab ──────────────────────

@app.get("/api/users/{user_id}/profile")
def user_profile(user_id: int):
    """Public user profile — name, points, join date, is_admin (so users
    can tell who's running the show). Same as /api/auth/me but for any user."""
    db = get_db()
    u = db.execute("SELECT id, name, points, is_admin, created_at FROM users WHERE id=?", (user_id,)).fetchone()
    db.close()
    if not u:
        raise HTTPException(404, "User not found")
    return dict(u)


@app.get("/api/activity")
def activity_feed(limit: int = 50):
    """Public activity feed — recent bets placed + matches resolved across
    all users. Chronological, newest first. Powers the Activity tab."""
    limit = max(1, min(limit, 200))
    db = get_db()
    feed = []

    # Recent match bets (group stage)
    for r in db.execute("""
        SELECT mb.id, mb.placed_at, mb.pick, mb.amount, mb.odds, mb.won, mb.payout,
               u.id as user_id, u.name as user_name,
               m.id as match_id, m.team_a, m.team_b, m.group_name, m.status as match_status,
               m.score_a, m.score_b, m.winner as match_winner, m.start_date
        FROM match_bets mb
        JOIN users u ON mb.user_id = u.id
        JOIN matches m ON mb.match_id = m.id
        ORDER BY mb.placed_at DESC LIMIT ?
    """, (limit,)).fetchall():
        d = dict(r)
        d["kind"] = "match_bet"
        d["label"] = f"{d['team_a']} vs {d['team_b']}"
        pick_label = (d["team_a"] + " Win" if d["pick"] == "team_a"
                      else ("Draw" if d["pick"] == "draw" else d["team_b"] + " Win"))
        d["summary"] = f"{d['user_name']} bet {d['amount']} on {pick_label} · odds {(d['odds']*100):.0f}%"
        if d["match_status"] == "completed":
            d["result"] = ("Won +" + f"{d['payout']:.0f}" if d["won"] == 1 else (f"Lost -{d['amount']:.0f}" if d["won"] == 0 else "Pending"))
        feed.append(d)

    # Recent knockout bets
    for r in db.execute("""
        SELECT kb.id, kb.placed_at, kb.pick, kb.amount, kb.odds, kb.won, kb.payout,
               u.id as user_id, u.name as user_name,
               km.id as match_id, km.round, km.slot, km.team_a, km.team_b, km.status as match_status, km.winner as match_winner
        FROM knockout_bets kb
        JOIN users u ON kb.user_id = u.id
        JOIN knockout_matches km ON km.id = kb.knockout_match_id
        ORDER BY kb.placed_at DESC LIMIT ?
    """, (limit,)).fetchall():
        d = dict(r)
        d["kind"] = "ko_bet"
        d["label"] = f"{d['team_a'] or 'TBD'} vs {d['team_b'] or 'TBD'}"
        pick_label = ((d["team_a"] or "?") + " Win" if d["pick"] == "team_a" else (d["team_b"] or "?") + " Win")
        d["summary"] = f"{d['user_name']} bet {d['amount']} on {pick_label} · KO {d['round']} · odds {(d['odds']*100):.0f}%"
        if d["match_status"] == "completed":
            d["result"] = ("Won +" + f"{d['payout']:.0f}" if d["won"] == 1 else (f"Lost -{d['amount']:.0f}" if d["won"] == 0 else "Pending"))
        feed.append(d)

    # Recent market bets
    for r in db.execute("""
        SELECT b.id, b.placed_at, b.side, b.amount, b.odds_at_bet as odds, b.won, b.payout,
               u.id as user_id, u.name as user_name,
               m.id as match_id, m.question, m.status as market_status
        FROM bets b
        JOIN users u ON b.user_id = u.id
        JOIN markets m ON m.id = b.market_id
        ORDER BY b.placed_at DESC LIMIT ?
    """, (limit,)).fetchall():
        d = dict(r)
        d["kind"] = "market_bet"
        d["label"] = d["question"]
        d["summary"] = f"{d['user_name']} bet {d['amount']} on {d['side'].upper()} · market · odds {(d['odds']*100):.0f}%"
        if d["market_status"] == "resolved":
            d["result"] = ("Won +" + f"{d['payout']:.0f}" if d["won"] == 1 else (f"Lost -{d['amount']:.0f}" if d["won"] == 0 else "Pending"))
        feed.append(d)

    # Sort merged feed by placed_at DESC, then trim to limit
    feed.sort(key=lambda x: x.get("placed_at") or "", reverse=True)
    feed = feed[:limit]

    db.close()
    return {"items": feed, "count": len(feed)}


@app.get("/api/users/{user_id}/active-bets")
def user_active_bets(user_id: int):
    """Pending bets only — across group matches + knockout. Used by the
    'My Bets' dashboard tab. PUBLIC — friends can see each other's active bets."""
    db = get_db()
    out = {"match_bets": [], "knockout_bets": [], "totals": {"wagered": 0.0, "potential": 0.0, "count": 0}}

    # Group-stage match bets (pending)
    for r in db.execute("""
        SELECT mb.id, mb.pick, mb.amount, mb.odds, mb.placed_at,
               m.id as match_id, m.team_a, m.team_b, m.group_name, m.start_date,
               m.outcome_prices
        FROM match_bets mb JOIN matches m ON m.id = mb.match_id
        WHERE mb.user_id=? AND mb.won IS NULL AND m.status='scheduled'
        ORDER BY m.start_date ASC
    """, (user_id,)).fetchall():
        d = dict(r)
        d["type"] = "match"
        d["potential_payout"] = round(d["amount"] / d["odds"], 2) if d["odds"] > 0 else 0
        side = d.get("side") or "yes"
        d["pick_label"] = _pick_label(d["team_a"], d["team_b"], d["pick"], side, allow_draw=True)
        d["label"] = f"{d['team_a']} vs {d['team_b']} (Group {d['group_name']})"
        out["match_bets"].append(d)
        out["totals"]["wagered"] += d["amount"]
        out["totals"]["potential"] += d["potential_payout"]
        out["totals"]["count"] += 1

    # Knockout bets (pending) — start_date not stored on knockout_matches; only
    # R32+ have deterministic dates once the group stage finishes. The dashboard
    # shows TBD until then.
    for r in db.execute("""
        SELECT kb.id, kb.pick, kb.amount, kb.odds, kb.placed_at,
               km.id as match_id, km.round, km.slot, km.team_a, km.team_b
        FROM knockout_bets kb JOIN knockout_matches km ON km.id = kb.knockout_match_id
        WHERE kb.user_id=? AND kb.won IS NULL
        ORDER BY km.round, km.slot
    """, (user_id,)).fetchall():
        d = dict(r)
        d["type"] = "knockout"
        d["potential_payout"] = round(d["amount"] / d["odds"], 2) if d["odds"] > 0 else 0
        side = d.get("side") or "yes"
        d["pick_label"] = _pick_label(d["team_a"] or "?", d["team_b"] or "?", d["pick"], side, allow_draw=False)
        d["label"] = f"{d['team_a'] or 'TBD'} vs {d['team_b'] or 'TBD'} ({d['round']})"
        out["knockout_bets"].append(d)
        out["totals"]["wagered"] += d["amount"]
        out["totals"]["potential"] += d["potential_payout"]
        out["totals"]["count"] += 1

    db.close()
    return out


@app.get("/api/users/{user_id}/stats")
def user_stats(user_id: int):
    """P&L, win rate, current balance. PUBLIC — friends can see each other's stats."""
    db = get_db()
    u = db.execute("SELECT id, name, points, created_at FROM users WHERE id=?", (user_id,)).fetchone()
    if not u:
        db.close(); raise HTTPException(404, "User not found")
    user = dict(u)

    def agg(table, won_col="won"):
        r = db.execute(f"""
            SELECT
              COUNT(*) as total,
              COALESCE(SUM(CASE WHEN {won_col}=1 THEN 1 ELSE 0 END), 0) as wins,
              COALESCE(SUM(CASE WHEN {won_col}=0 THEN 1 ELSE 0 END), 0) as losses,
              COALESCE(SUM(CASE WHEN {won_col} IS NULL THEN 1 ELSE 0 END), 0) as pending,
              COALESCE(SUM(amount), 0) as wagered,
              COALESCE(SUM(payout), 0) as returned
            FROM {table} WHERE user_id=?
        """, (user_id,)).fetchone()
        return dict(r) if r else {"total":0,"wins":0,"losses":0,"pending":0,"wagered":0,"returned":0}

    user["markets"] = agg("bets")
    user["matches"] = agg("match_bets")
    user["knockouts"] = agg("knockout_bets")
    user["total_wagered"] = (user["markets"]["wagered"] or 0) + (user["matches"]["wagered"] or 0) + (user["knockouts"]["wagered"] or 0)
    user["total_returned"] = (user["markets"]["returned"] or 0) + (user["matches"]["returned"] or 0) + (user["knockouts"]["returned"] or 0)
    user["net_pnl"] = user["total_returned"] - user["total_wagered"]
    user["total_bets"] = (user["markets"]["total"] or 0) + (user["matches"]["total"] or 0) + (user["knockouts"]["total"] or 0)
    user["wins"] = (user["markets"]["wins"] or 0) + (user["matches"]["wins"] or 0) + (user["knockouts"]["wins"] or 0)
    user["win_rate"] = (user["wins"] / user["total_bets"]) if user["total_bets"] else 0
    db.close()
    return user


@app.post("/api/admin/reset")
async def reset_data(req: Request):
    """Reset betting data — keep match schedule and odds. Admin only; requires
    explicit confirm token in body to prevent footgun clicks."""
    require_admin(req)
    body = await req.json() if (await req.body()) else {}
    if not body.get("confirm"):
        raise HTTPException(400, "Pass {\"confirm\": true} to reset")
    db = get_db()
    db.execute("DELETE FROM match_bets")
    db.execute("DELETE FROM bets")
    db.execute("DELETE FROM group_standings")
    # Only reset matches that don't have confirmed scores
    db.execute("UPDATE matches SET status='scheduled', winner=NULL, score_a=NULL, score_b=NULL WHERE score_a IS NULL")
    db.execute("UPDATE knockout_matches SET status='pending', winner=NULL, team_a=NULL, team_b=NULL, score_a=NULL, score_b=NULL, outcome_prices=NULL")
    db.execute("UPDATE users SET points=1000")
    db.commit()
    db.close()
    return {"ok": True, "message": "Bets cleared, points reset. Match schedule and odds preserved."}


@app.post("/api/admin/recompute-balances")
async def recompute_balances(req: Request, body: dict = None):
    """Recompute users.points from the bet ledger. Use this to recover from
    a sync crash that left bet='won' but didn't add the payout. The bet
    ledger (match_bets / knockout_bets / bets) is the source of truth.
    Admin only; requires explicit confirm."""
    require_admin(req)
    payload = await req.json() if (await req.body()) else {}
    if not payload.get("confirm"):
        raise HTTPException(400, "Pass {\"confirm\": true} to rebuild balances")
    db = get_db()
    # Recompute each user's balance = starting_points + sum(payouts) - sum(amounts not pending)
    # Simplest model: 1000 starting + sum of all settled bet payouts.
    # We treat pending bets as already-deducted-from-balance (correct since
    # they were debited when placed) and settled bets as adding payout on top.
    # The current users.points column is whatever it is — we OVERWRITE it.
    summary = []
    for u in db.execute("SELECT id, name, points FROM users").fetchall():
        # Sum of payouts from all settled bets (won=1)
        payouts = db.execute("""
            SELECT
              (SELECT COALESCE(SUM(payout), 0) FROM match_bets WHERE user_id=? AND won=1) +
              (SELECT COALESCE(SUM(payout), 0) FROM knockout_bets WHERE user_id=? AND won=1) +
              (SELECT COALESCE(SUM(payout), 0) FROM bets WHERE user_id=? AND won=1) AS total_payout
        """, (u['id'], u['id'], u['id'])).fetchone()['total_payout']
        # New balance: 1000 (starting) + all settled payouts.
        # (Note: bets already debited their amount at placement, so we only
        # need to ADD the payouts on top of the starting balance.)
        new_balance = 1000 + payouts
        delta = new_balance - u['points']
        db.execute("UPDATE users SET points=? WHERE id=?", (new_balance, u['id']))
        summary.append({"id": u['id'], "name": u['name'], "old": round(u['points'], 2), "new": round(new_balance, 2), "delta": round(delta, 2)})
    db.commit()
    db.close()
    return {"ok": True, "message": f"Rebuilt {len(summary)} user balances from the bet ledger.", "summary": summary}

@app.post("/api/worldcup/resolve")
async def worldcup_resolve(req: Request):
    require_admin(req)
    body = await req.json()
    match_id = body.get("match_id")
    winner = body.get("winner")  # 'team_a', 'draw', 'team_b'
    score_a = body.get("score_a")  # optional score
    score_b = body.get("score_b")
    force = bool(body.get("force", False))  # explicit override for re-resolve

    if not match_id or winner not in ("team_a", "draw", "team_b"):
        raise HTTPException(400, "match_id and winner (team_a/draw/team_b) required")

    db = get_db()
    match = db.execute("SELECT * FROM matches WHERE id=?", (match_id,)).fetchone()
    if not match:
        db.close(); raise HTTPException(404, "Match not found")

    # Guard: refuse to re-resolve an already-completed match unless explicitly forced.
    # Re-resolving would change the match winner but NOT re-settle already-settled
    # bets (the settle loop filters won IS NULL), leaving the DB in an inconsistent
    # state where match.winner != bet.won rows.
    if match["status"] == "completed" and not force:
        db.close()
        raise HTTPException(
            409,
            f"Match {match_id} already resolved as {match['winner']} ({match['score_a']}-{match['score_b']}). Pass force=true to override."
        )

    db.execute(
        "UPDATE matches SET status='completed', winner=?, score_a=?, score_b=? WHERE id=?",
        (winner, score_a, score_b, match_id),
    )
    
    # Update group standings if scores provided
    if score_a is not None and score_b is not None:
        _update_standings(db, match["group_name"], match["team_a"], match["team_b"], score_a, score_b)

    # Settle bets using the same Polymarket-style YES/NO logic as sync.py.
    # (Pick: team_a / team_b / draw. Side: yes / no. Resolution: winner.)
    bets = db.execute("SELECT * FROM match_bets WHERE match_id=? AND won IS NULL", (match_id,)).fetchall()
    for b in bets:
        side = (b["side"] if "side" in b.keys() else "yes") or "yes"  # legacy rows
        pick_hit = 1 if b["pick"] == winner else 0
        bet_won = 1 if (side == "yes" and pick_hit == 1) or (side == "no" and pick_hit == 0) else 0
        payout = 0.0
        if bet_won and b["odds"] > 0:
            if side == "yes":
                payout = b["amount"] / b["odds"]
            else:
                no_odds = 1.0 - b["odds"]
                payout = b["amount"] / no_odds if no_odds > 0 else b["amount"]
            payout = round(payout, 2)
        db.execute("UPDATE match_bets SET won=?, payout=? WHERE id=?", (bet_won, payout, b["id"]))
        if payout > 0:
            db.execute("UPDATE users SET points = points + ? WHERE id=?", (payout, b["user_id"]))
    db.commit()
    db.close()
    return {"resolved": match_id, "winner": winner, "bets_settled": len(bets)}

def _update_standings(db, group_name, team_a, team_b, score_a, score_b):
    """Update group standings after a match result."""
    # Team A result
    _upsert_team_result(db, group_name, team_a, score_a, score_b)
    # Team B result
    _upsert_team_result(db, group_name, team_b, score_b, score_a)

def _upsert_team_result(db, group, team, gf, ga):
    existing = db.execute(
        "SELECT * FROM group_standings WHERE group_name=? AND team_name=?", (group, team)
    ).fetchone()
    
    if gf > ga:
        pts, w, d, l = 3, 1, 0, 0
    elif gf == ga:
        pts, w, d, l = 1, 0, 1, 0
    else:
        pts, w, d, l = 0, 0, 0, 1

    if existing:
        db.execute("""UPDATE group_standings SET 
            played=played+1, wins=wins+?, draws=draws+?, losses=losses+?,
            goals_for=goals_for+?, goals_against=goals_against+?,
            goal_diff=goals_for+?-goals_against-?, points=points+?
            WHERE group_name=? AND team_name=?""",
            (w, d, l, gf, ga, gf, ga, pts, group, team))
    else:
        db.execute("""INSERT INTO group_standings 
            (group_name, team_name, played, wins, draws, losses, goals_for, goals_against, goal_diff, points)
            VALUES (?,?,1,?,?,?,?,?,?,?)""",
            (group, team, w, d, l, gf, ga, gf-ga, pts))

@app.get("/api/worldcup/standings")
def worldcup_standings():
    """Get group standings computed from match results."""
    db = get_db()
    rows = db.execute(
        "SELECT * FROM group_standings ORDER BY group_name, points DESC, goal_diff DESC, goals_for DESC"
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]

@app.get("/api/worldcup/knockout")
def worldcup_knockout():
    """Get knockout bracket matches."""
    db = get_db()
    rows = db.execute(
        "SELECT * FROM knockout_matches ORDER BY CASE round WHEN 'R32' THEN 1 WHEN 'R16' THEN 2 WHEN 'QF' THEN 3 WHEN 'SF' THEN 4 WHEN '3rd' THEN 5 WHEN 'Final' THEN 6 END, slot"
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]

@app.post("/api/worldcup/knockout/set-team")
async def knockout_set_team(req: Request):
    """Set a team in a knockout slot."""
    require_admin(req)
    body = await req.json()
    match_id = body.get("match_id")
    side = body.get("side")  # 'team_a' or 'team_b'
    team = body.get("team", "").strip()

    if not match_id or side not in ("team_a", "team_b"):
        raise HTTPException(400, "match_id and side required")

    # Validate team exists in WC_GROUPS — prevents arbitrary strings / typos
    # from poisoning the bracket.
    valid_teams = {t for teams in WC_GROUPS.values() for t in teams}
    if team and team not in valid_teams:
        raise HTTPException(400, f"Unknown team '{team}'. Must be one of WC2026 participants.")

    db = get_db()
    if side == "team_a":
        db.execute("UPDATE knockout_matches SET team_a=? WHERE id=?", (team, match_id))
    else:
        db.execute("UPDATE knockout_matches SET team_b=? WHERE id=?", (team, match_id))
    db.commit()
    db.close()
    return {"ok": True}


@app.post("/api/worldcup/knockout/bet")
async def knockout_place_bet(req: Request):
    """Place a bet on a knockout match. team_a vs team_b only (no draws)."""
    me = require_user(req)
    body = await req.json()
    user_id = me["id"]
    if body.get("user_id") and body["user_id"] != me["id"] and not me.get("is_admin"):
        raise HTTPException(403, "Cannot bet on another user's behalf")
    if me.get("is_admin") and body.get("user_id"):
        user_id = int(body["user_id"])

    match_id = body.get("match_id")
    pick = body.get("pick")  # 'team_a' or 'team_b'
    side = body.get("side", "yes")  # 'yes' = this team wins, 'no' = this team doesn't
    try:
        amount = float(body.get("amount", 0))
    except (TypeError, ValueError):
        raise HTTPException(400, "amount must be a number")
    if amount != amount or amount in (float("inf"), float("-inf")):
        raise HTTPException(400, "amount must be a finite number")

    if not all([match_id, pick, amount]) or amount <= 0:
        raise HTTPException(400, "match_id, pick, amount required")
    if pick not in ("team_a", "team_b"):
        raise HTTPException(400, "pick must be team_a or team_b")
    if side not in ("yes", "no"):
        raise HTTPException(400, "side must be 'yes' or 'no'")
    if amount > 1_000_000:
        raise HTTPException(400, "amount too large (max 1M)")

    db = get_db()
    db.execute("BEGIN IMMEDIATE")
    try:
        match = db.execute("SELECT * FROM knockout_matches WHERE id=?", (match_id,)).fetchone()
        if not match:
            db.execute("ROLLBACK"); db.close()
            raise HTTPException(404, "Knockout match not found")
        if match["status"] != "pending":
            db.execute("ROLLBACK"); db.close()
            raise HTTPException(400, f"Match is {match['status']}")
        if not match["team_a"] or not match["team_b"]:
            db.execute("ROLLBACK"); db.close()
            raise HTTPException(409, "Both teams must be set before betting")
        if not match["outcome_prices"]:
            db.execute("ROLLBACK"); db.close()
            raise HTTPException(409, "No odds for this match yet")

        op = json.loads(match["outcome_prices"])
        # Same convention as group matches: moneyline_home = team_a, moneyline_away = team_b
        yes_odds = float(op.get("moneyline_home" if pick == "team_a" else "moneyline_away", 0))
        if yes_odds <= 0:
            yes_odds = 0.5  # Even money fallback

        # KO bet model (same as WC): `amount` is the WAGER (what the user
        # pays upfront). Payout if win = wager / yes_odds (YES) or
        # wager / (1-yes_odds) (NO). Settlement recomputes from stored
        # wager + odds at sync time.
        wager = round(float(amount), 2)
        stored_odds = round(float(yes_odds), 4)
        if side == "yes":
            payout_if_win = round(wager / yes_odds, 2) if yes_odds > 0 else 0
        else:
            payout_if_win = round(wager / (1.0 - yes_odds), 2) if yes_odds < 1 else 0

        cur = db.execute(
            "UPDATE users SET points = points - ? WHERE id=? AND points >= ?",
            (wager, user_id, wager),
        )
        if cur.rowcount == 0:
            db.execute("ROLLBACK"); db.close()
            raise HTTPException(400, "Insufficient points")

        db.execute(
            "INSERT INTO knockout_bets (user_id, knockout_match_id, pick, amount, odds, side) VALUES (?,?,?,?,?,?)",
            (user_id, match_id, pick, wager, yes_odds, side),
        )
        db.commit()
        bid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        bet = dict(db.execute("SELECT * FROM knockout_bets WHERE id=?", (bid,)).fetchone())
        user = dict(db.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone())
    except HTTPException:
        raise
    except Exception as e:
        db.execute("ROLLBACK"); db.close()
        raise HTTPException(500, f"Bet failed: {e}")
    db.close()
    return {"bet": bet, "user": user, "wager": wager, "payout_if_win": payout_if_win, "side": side, "pays_on": "hit" if side == "yes" else "miss"}


@app.post("/api/worldcup/knockout/resolve")
async def knockout_resolve(req: Request):
    """Resolve a knockout match, advance winner, and settle bets."""
    require_admin(req)
    body = await req.json()
    match_id = body.get("match_id")
    winner_side = body.get("winner")  # 'team_a' or 'team_b'
    
    if not match_id or winner_side not in ("team_a", "team_b"):
        raise HTTPException(400, "match_id and winner required")
    
    db = get_db()
    match = db.execute("SELECT * FROM knockout_matches WHERE id=?", (match_id,)).fetchone()
    if not match:
        db.close(); raise HTTPException(404, "Match not found")
    
    winner_team = match["team_a"] if winner_side == "team_a" else match["team_b"]
    db.execute(
        "UPDATE knockout_matches SET status='completed', winner=? WHERE id=?",
        (winner_team, match_id)
    )
    
    # Advance winner to next match — but only if a slot is actually free,
    # otherwise we'd silently overwrite a manually-set team.
    if match["next_match_id"]:
        next_match = db.execute("SELECT * FROM knockout_matches WHERE id=?", (match["next_match_id"],)).fetchone()
        if next_match:
            if not next_match["team_a"]:
                db.execute("UPDATE knockout_matches SET team_a=? WHERE id=?", (winner_team, match["next_match_id"]))
            elif not next_match["team_b"]:
                db.execute("UPDATE knockout_matches SET team_b=? WHERE id=?", (winner_team, match["next_match_id"]))

    # Also handle 3rd place match: if this is SF and loser should go to 3rd place
    if match["round"] == "SF":
        loser_team = match["team_b"] if winner_side == "team_a" else match["team_a"]
        third_id = db.execute("SELECT id FROM knockout_matches WHERE round='3rd'").fetchone()
        if third_id:
            t = db.execute("SELECT * FROM knockout_matches WHERE id=?", (third_id["id"],)).fetchone()
            if not t["team_a"]:
                db.execute("UPDATE knockout_matches SET team_a=? WHERE id=?", (loser_team, third_id["id"]))
            elif not t["team_b"]:
                db.execute("UPDATE knockout_matches SET team_b=? WHERE id=?", (loser_team, third_id["id"]))

    # Settle knockout bets using the same Polymarket-style YES/NO logic as sync.py.
    # (Pick: team_a / team_b. Side: yes / no. Resolution: winner_side.)
    bets_settled = 0
    for b in db.execute(
        "SELECT * FROM knockout_bets WHERE knockout_match_id=? AND won IS NULL", (match_id,)
    ).fetchall():
        side = (b["side"] if "side" in b.keys() else "yes") or "yes"
        pick_hit = 1 if b["pick"] == winner_side else 0
        bet_won = 1 if (side == "yes" and pick_hit == 1) or (side == "no" and pick_hit == 0) else 0
        payout = 0.0
        if bet_won and b["odds"] > 0:
            if side == "yes":
                payout = b["amount"] / b["odds"]
            else:
                no_odds = 1.0 - b["odds"]
                payout = b["amount"] / no_odds if no_odds > 0 else b["amount"]
            payout = round(payout, 2)
            db.execute("UPDATE users SET points = points + ? WHERE id=?", (payout, b["user_id"]))
        db.execute("UPDATE knockout_bets SET won=?, payout=? WHERE id=?", (bet_won, payout, b["id"]))
        bets_settled += 1

    # Settle the per-round ADVANCE markets (Polymarket) for the teams in
    # this match. When a team wins a KO round, the corresponding "advance
    # to next round" market resolves YES; the loser resolves NO. We also
    # immediately settle any bets on those markets so P&L is up to date.
    adv_n, adv_payout = _settle_advance_markets_for_match(db, match, winner_team)

    db.commit()
    db.close()
    return {"ok": True, "winner": winner_team, "advanced_to": match["next_match_id"],
            "bets_settled": bets_settled, "advance_markets_settled": adv_n,
            "advance_payouts_credited": round(adv_payout, 2)}


def _settle_market_bets(db, market_id, winner_idx):
    """Settle all pending `bets` for a market after it resolves. Used by
    both the Polymarket refresh path (when check_resolution returns a
    winner) and by the knockout_resolve path (which manually resolves
    advance markets like 'Will Brazil reach the QF'). Returns the
    number of bets settled and the total payout credited."""
    settled = 0
    total_payout = 0.0
    for b in db.execute(
        "SELECT * FROM bets WHERE market_id=? AND won IS NULL", (market_id,)
    ).fetchall():
        bet_winner_idx = 0 if b["side"] == "yes" else 1
        won = 1 if bet_winner_idx == winner_idx else 0
        payout = 0.0
        if won:
            odds = b["odds_at_bet"] if "odds_at_bet" in b.keys() else b["odds"]
            if odds > 0:
                payout = b["amount"] / odds
                payout = round(payout, 2)
        db.execute("UPDATE bets SET won=?, payout=? WHERE id=?", (won, payout, b["id"]))
        if payout > 0:
            db.execute("UPDATE users SET points = points + ? WHERE id=?", (payout, b["user_id"]))
            total_payout += payout
        settled += 1
    return settled, total_payout


def _settle_advance_markets_for_match(db, match, winner_team):
    """Settle the per-round Polymarket advance markets for the two teams
    in a KO match, based on the round (R32/R16/QF/SF/Final) and the
    winning team. Returns the number of markets settled.

    The map is:
      R32 match  → settle "advance_r16"  markets (advancing from R32 to R16)
      R16 match  → settle "advance_qf"   markets
      QF match   → settle "advance_sf"   markets
      SF match   → settle "advance_final" (= reach Final)
      Final      → settle "advance_final" (winner = champion)
    """
    round_name = (match.get("round") or "").strip()
    category = STAGE_TO_CATEGORY.get(round_name)
    if not category:
        return 0, 0
    settled = 0
    total_payout = 0.0
    for team_name, did_advance in [
        (match.get("team_a"), 1 if winner_team == match.get("team_a") else 0),
        (match.get("team_b"), 1 if winner_team == match.get("team_b") else 0),
    ]:
        if not team_name:
            continue
        # Find the market for this team + this advance category
        m = db.execute("""
            SELECT id, polymarket_condition_id FROM markets
            WHERE team_name=? AND category=? AND status='active'
            LIMIT 1
        """, (team_name, category)).fetchone()
        if not m:
            continue
        # Resolve the market: YES (idx 0) if team advanced, NO (idx 1) if not
        winner_idx = 0 if did_advance else 1
        db.execute("""
            UPDATE markets SET status='resolved', winner_idx=?, resolved_at=datetime('now')
            WHERE id=?
        """, (winner_idx, m["id"]))
        # Settle any pending bets on this market so P&L is correct immediately
        # (rather than waiting for the next refresh_market call).
        bet_n, bet_pay = _settle_market_bets(db, m["id"], winner_idx)
        total_payout += bet_pay
        settled += 1
    return settled, total_payout

def _get_team_odds(db, team_name, group_name):
    """Get team's implied strength from Polymarket group win odds."""
    m = db.execute(
        "SELECT outcome_prices FROM markets WHERE question LIKE ? AND status='active' LIMIT 1",
        (f"%{team_name} win Group {group_name}%",)
    ).fetchone()
    if m:
        prices = json.loads(m["outcome_prices"])
        return float(prices[0]) if prices else 0
    return 0

@app.post("/api/sync")
async def sync_polymarket():
    """Manual sync trigger — runs sync.py in a worker thread, returns immediately.
    Use /api/sync/last to check status."""
    import threading
    threading.Thread(target=_run_sync_once, daemon=True).start()
    return {"started": True, "note": "running in background; poll /api/sync/last"}


@app.get("/api/sync/last")
def sync_last():
    """Last sync status — drives the 'Last sync: X ago' indicator in the admin tab."""
    s = get_sync_state()
    last = s.get("last_run")
    if last:
        try:
            dt = datetime.fromisoformat(last)
            now = datetime.now(timezone.utc)
            minutes_ago = round((now - dt).total_seconds() / 60, 1)
        except Exception:
            minutes_ago = None
    else:
        minutes_ago = None
    return {
        "last_run": last,
        "minutes_ago": minutes_ago,
        "last_ok": s.get("last_ok"),
        "last_summary": s.get("last_summary", ""),
        "in_progress": s.get("in_progress", False),
        "next_run": s.get("next_run"),
        "history": s.get("history", []),
    }


# Start the background sync thread on app startup
@app.on_event("startup")
def _start_background_sync():
    t = threading.Thread(target=_sync_loop, daemon=True, name="pm-bg-sync")
    t.start()

# ── HTML Template ───────────────────────────────────────────────────────────

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Prediction Market</title>
<style>
:root {
  --bg: #0a0a0f;
  --card-bg: rgba(20, 20, 35, 0.7);
  --card-border: rgba(255, 255, 255, 0.06);
  --text: #e0e0e0;
  --text-dim: #888;
  --accent: #7c5cff;
  --green: #00e676;
  --red: #ff5252;
  --gold: #ffd740;
  --radius: 12px;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Display', 'Segoe UI', sans-serif;
  background: var(--bg);
  color: var(--text);
  min-height: 100vh;
  background-image:
    radial-gradient(ellipse at 20% 20%, rgba(124, 92, 255, 0.08) 0%, transparent 50%),
    radial-gradient(ellipse at 80% 80%, rgba(0, 230, 118, 0.05) 0%, transparent 50%);
}
.container { max-width: 900px; margin: 0 auto; padding: 24px 16px; }
header {
  display: flex; justify-content: space-between; align-items: center;
  margin-bottom: 24px; flex-wrap: wrap; gap: 12px;
}
h1 {
  font-size: 24px; font-weight: 700;
  background: linear-gradient(135deg, var(--accent), var(--green));
  -webkit-background-clip: text; -webkit-text-fill-color: transparent;
}
.user-bar {
  display: flex; gap: 8px; align-items: center;
}
select, input, button {
  background: var(--card-bg); border: 1px solid var(--card-border);
  color: var(--text); border-radius: 8px; padding: 8px 12px;
  font-size: 14px; font-family: inherit;
}
button {
  cursor: pointer; transition: all .15s;
  background: var(--accent); border-color: var(--accent);
  font-weight: 600;
}
button:hover { opacity: 0.85; transform: translateY(-1px); }
button:active { transform: translateY(0); }
button.outline {
  background: transparent; border-color: var(--card-border);
}
button.outline:hover { border-color: var(--accent); }
button.danger { background: var(--red); border-color: var(--red); }

.tabs {
  display: flex; gap: 4px; margin-bottom: 20px;
  background: var(--card-bg); border-radius: var(--radius);
  padding: 4px; backdrop-filter: blur(20px);
}
.tab {
  flex: 1; text-align: center; padding: 10px;
  border-radius: 8px; cursor: pointer; font-weight: 600;
  font-size: 14px; color: var(--text-dim); transition: all .2s;
}
.tab.active { background: var(--accent); color: #fff; }
.tab:hover:not(.active) { color: var(--text); }

.card {
  background: var(--card-bg); border: 1px solid var(--card-border);
  border-radius: var(--radius); padding: 16px; margin-bottom: 12px;
  backdrop-filter: blur(20px); transition: all .2s;
}
.card:hover { border-color: rgba(124, 92, 255, 0.3); }

.market-header { display: flex; justify-content: space-between; align-items: flex-start; gap: 8px; }
.market-q { font-size: 15px; font-weight: 600; line-height: 1.4; flex: 1; }
.market-meta { font-size: 12px; color: var(--text-dim); white-space: nowrap; }
.odds-bar {
  display: flex; margin-top: 12px; height: 32px; border-radius: 6px;
  overflow: hidden; background: rgba(255,255,255,0.04);
  position: relative; border: 1px solid rgba(255,255,255,0.08);
}
.odds-yes {
  background: rgba(0, 230, 118, 0.2);
  display: flex; align-items: center; padding-left: 10px;
  font-size: 13px; font-weight: 700; color: var(--green);
  transition: width 0.5s ease;
}
.odds-no {
  background: rgba(255, 82, 82, 0.15);
  display: flex; align-items: center; justify-content: flex-end;
  padding-right: 10px; font-size: 13px; font-weight: 700;
  color: var(--red); transition: width 0.5s ease;
}
.market-actions {
  display: flex; gap: 8px; margin-top: 10px;
}
.market-actions button { flex: 1; font-size: 13px; padding: 6px 12px; }
.status-badge {
  display: inline-block; padding: 2px 8px; border-radius: 4px;
  font-size: 11px; font-weight: 700; text-transform: uppercase;
}
.status-active { background: rgba(0,230,118,0.15); color: var(--green); }
.status-closed { background: rgba(255,215,64,0.15); color: var(--gold); }
.status-resolved { background: rgba(124,92,255,0.15); color: var(--accent); }

.leaderboard-table { width: 100%; border-collapse: collapse; }
.leaderboard-table th, .leaderboard-table td {
  padding: 10px 12px; text-align: left; font-size: 14px;
  border-bottom: 1px solid var(--card-border);
}
.leaderboard-table th { font-size: 11px; text-transform: uppercase; color: var(--text-dim); }
.rank { font-weight: 700; font-size: 18px; }
.rank-1 { color: var(--gold); }
.rank-2 { color: #c0c0c0; }
.rank-3 { color: #cd7f32; }
.points { font-weight: 700; font-variant-numeric: tabular-nums; }
.points-pos { color: var(--green); }
.points-neg { color: var(--red); }

.modal-overlay {
  display: none; position: fixed; top:0; left:0; right:0; bottom:0;
  background: rgba(0,0,0,0.7); z-index: 100;
  justify-content: center; align-items: center;
}
.modal-overlay.show { display: flex; }
.modal {
  background: #1a1a2e; border: 1px solid var(--card-border);
  border-radius: var(--radius); padding: 24px; max-width: 420px;
  width: 90%; backdrop-filter: blur(20px);
}
.modal h3 { margin-bottom: 16px; font-size: 16px; }
.modal .field { margin-bottom: 14px; }
.modal .field label { display: block; font-size: 12px; color: var(--text-dim); margin-bottom: 4px; }
.modal .field input, .modal .field select { width: 100%; }

/* Polymarket-style per-market row inside the bet modal.
   Each row is a "market" with its own YES / NO pair of buttons. */
.pmarket-row { display: flex; align-items: center; gap: 8px; padding: 6px 8px; background: rgba(255,255,255,0.02); border: 1px solid var(--card-border); border-radius: 6px; }
.pmarket-name { font-size: 13px; font-weight: 600; min-width: 90px; flex-shrink: 0; }
.pmarket-buttons { display: flex; gap: 6px; flex: 1; }
.pmarket-buttons button { flex: 1; padding: 6px 4px; border-radius: 6px; border: 1px solid; cursor: pointer; font-family: inherit; color: var(--text); display: flex; flex-direction: column; align-items: center; gap: 1px; }
.pmarket-buttons button.yes { background: rgba(0,230,118,0.05); border-color: rgba(0,230,118,0.3); }
.pmarket-buttons button.yes:hover { background: rgba(0,230,118,0.18); border-color: var(--green); }
.pmarket-buttons button.no { background: rgba(255,82,82,0.05); border-color: rgba(255,82,82,0.3); }
.pmarket-buttons button.no:hover { background: rgba(255,82,82,0.18); border-color: var(--red); }
.pmarket-side { font-size: 11px; font-weight: 700; letter-spacing: 0.4px; }
.pmarket-buttons button.yes .pmarket-side { color: var(--green); }
.pmarket-buttons button.no .pmarket-side { color: var(--red); }
.pmarket-price { font-size: 14px; font-weight: 800; line-height: 1.1; }
.pmarket-wager { font-size: 9px; color: var(--text-dim); }
.modal .actions { display: flex; gap: 8px; margin-top: 20px; }
.modal .actions button { flex: 1; }

.search-box { display: flex; gap: 8px; margin-bottom: 16px; }
.search-box input { flex: 1; }
.search-results { margin-top: 12px; }
.search-result {
  padding: 12px; margin-bottom: 8px; border-radius: 8px;
  background: rgba(255,255,255,0.03); border: 1px solid var(--card-border);
  cursor: pointer; transition: all .15s;
}
.search-result:hover { border-color: var(--accent); }

.toast {
  position: fixed; bottom: 20px; right: 20px; padding: 12px 20px;
  border-radius: 8px; font-size: 14px; font-weight: 600;
  z-index: 200; animation: slideIn 0.3s ease;
  display: none;
}
.toast.show { display: block; }
.toast.success { background: var(--green); color: #000; }
.toast.error { background: var(--red); color: #fff; }
@keyframes slideIn { from { transform: translateY(20px); opacity: 0; } to { transform: translateY(0); opacity: 1; } }

.admin-bar { margin-top: 20px; padding-top: 16px; border-top: 1px solid var(--card-border); display: flex; gap: 8px; }
.empty { text-align: center; padding: 40px; color: var(--text-dim); font-size: 14px; }
.refresh-hint { font-size: 11px; color: var(--text-dim); margin-top: 4px; }
.pnl { font-size: 12px; font-weight: 600; }
.pnl-pos { color: var(--green); }
.pnl-neg { color: var(--red); }
</style>
<script src="/static/app.js"></script>
</head>
<body>
<div class="container">
  <header>
    <h1>📊 Prediction Market</h1>
    <div class="user-bar" id="pm-user-bar"></div>
  </header>

  <div class="tabs">
    <div class="tab active" onclick="switchTab('markets', this)">🎯 Markets</div>
    <div class="tab" onclick="switchTab('mybets', this)">📋 My Bets</div>
    <div class="tab" onclick="switchTab('leaderboard', this)">🏆 Leaderboard</div>
    <div class="tab" onclick="switchTab('import', this)">🔍 Import</div>
    <div class="tab" onclick="location.href='/worldcup'" style="background:linear-gradient(135deg,#ffd740,#ff9100);color:#000">🏆 World Cup</div>
  </div>

  <!-- MARKETS TAB -->
  <div id="tab-markets">
    <div style="display:flex;gap:8px;margin-bottom:14px;">
      <button onclick="loadMarkets('active')" id="filter-active" style="background:var(--accent)">Active</button>
      <button onclick="loadMarkets('closed')" id="filter-closed" class="outline">Closed</button>
      <button onclick="loadMarkets('resolved')" id="filter-resolved" class="outline">Resolved</button>
    </div>
    <div id="markets-list"><div class="empty">No markets yet. Import some!</div></div>
  </div>

  <!-- MY BETS TAB -->
  <div id="tab-mybets" style="display:none;">
    <div id="mybets-list"><div class="empty">Select a user first</div></div>
  </div>

  <!-- LEADERBOARD TAB -->
  <div id="tab-leaderboard" style="display:none;">
    <div id="leaderboard-content"></div>
  </div>

  <!-- IMPORT TAB -->
  <div id="tab-import" style="display:none;">
    <div class="search-box">
      <input type="text" id="search-query" placeholder="Search Polymarket (e.g. 'BTC', 'election', 'Super Bowl')" onkeydown="if(event.key==='Enter')searchPoly()">
      <button onclick="searchPoly()">Search</button>
    </div>
    <div id="search-results" class="search-results"></div>
    <div class="admin-bar">
      <button onclick="refreshAll()" class="outline">🔄 Refresh All Odds</button>
      <button onclick="resolveAll()" class="outline">✅ Check Resolutions</button>
    </div>
  </div>
</div>

<!-- Bet Modal -->
<div class="modal-overlay" id="bet-modal">
  <div class="modal">
    <h3>Place Bet</h3>
    <div id="bet-market-q" style="font-size:14px;margin-bottom:12px;color:var(--text-dim)"></div>
    <div class="field">
      <label>Side</label>
      <select id="bet-side">
        <option value="yes">Yes</option>
        <option value="no">No</option>
      </select>
    </div>
    <div class="field">
      <label>Amount (points) — you have <span id="bet-balance" style="color:var(--gold);font-weight:700">…</span> pts</label>
      <input type="number" id="bet-amount" min="1" placeholder="100" oninput="updateBetPreview()">
      <div style="display:flex;gap:6px;margin-top:6px;flex-wrap:wrap">
        <button class="sm outline" onclick="setBetAmount(10)" style="font-size:11px">10</button>
        <button class="sm outline" onclick="setBetAmount(50)" style="font-size:11px">50</button>
        <button class="sm outline" onclick="setBetAmount(100)" style="font-size:11px">100</button>
        <button class="sm outline" onclick="setBetAmount(250)" style="font-size:11px">250</button>
        <button class="sm outline" onclick="setBetAmount(500)" style="font-size:11px">500</button>
        <button class="sm outline gold" onclick="setBetAmount('max')" style="font-size:11px">MAX</button>
      </div>
    </div>
    <div class="field">
      <label>Current odds</label>
      <div id="bet-odds" style="font-size:13px"></div>
    </div>
    <div class="field">
      <label>Potential payout</label>
      <div id="bet-payout" style="font-size:16px;font-weight:700;color:var(--green)"></div>
    </div>
    <div class="actions">
      <button onclick="closeModal('bet-modal')" class="outline">Cancel</button>
      <button onclick="submitBet()">Place Bet</button>
    </div>
  </div>
</div>

<!-- Add User Modal -->
<div class="modal-overlay" id="user-modal">
  <div class="modal">
    <h3>Add Friend</h3>
    <div class="field">
      <label>Name</label>
      <input type="text" id="new-user-name" placeholder="Your name">
    </div>
    <div class="actions">
      <button onclick="closeModal('user-modal')" class="outline">Cancel</button>
      <button onclick="addUser()">Join</button>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
// ── State ──
let currentMarketId = null;
let currentTab = 'markets';
let allMarkets = [];
let polymarketResults = [];

// ── Init ──
async function init() {
  // PM.refreshMe() already ran in app.js. Render the user bar from there.
  PM.renderUserBar(document.getElementById('pm-user-bar'));
  PM.onAuthChange((u) => PM.renderUserBar(document.getElementById('pm-user-bar')));
  // Auth state lives in PM.getUser(); no need for a local currentUser.
  await loadMarkets('active');
}
init();

// ── API helpers ──
// Use PM.api() (in /static/app.js) — it auto-injects the auth token and
// handles 401 by clearing the session.
const api = PM.api;

function switchUser() {
  // Legacy no-op. The user-bar in this app is now auth-driven via PM.
}

function showAddUser() { PM.showRegister(); }

async function addUser() {
  const name = document.getElementById('new-user-name').value.trim();
  if (!name) return toast('Enter a name', 'error');
  try {
    await api('/api/users', {method:'POST', body: JSON.stringify({name})});
    closeModal('user-modal');
    document.getElementById('new-user-name').value = '';
    await loadUsers();
    toast(name + ' joined!', 'success');
  } catch(e) { toast(e.message, 'error'); }
}

// ── Tabs ──
function switchTab(t, el) {
  currentTab = t;
  document.querySelectorAll('.tab').forEach(e => e.classList.remove('active'));
  document.querySelectorAll('[id^="tab-"]').forEach(e => e.style.display = 'none');
  document.getElementById('tab-'+t).style.display = 'block';
  el.classList.add('active');
  if (t === 'markets') loadMarkets('active');
  else if (t === 'mybets') loadMyBets();
  else if (t === 'leaderboard') loadLeaderboard();
}

// ── Markets ──
async function loadMarkets(status) {
  document.querySelectorAll('#tab-markets button[id^="filter-"]').forEach(b => b.className='outline');
  document.getElementById('filter-'+status).style.background = 'var(--accent)';

  allMarkets = await api('/api/markets?status='+status);
  renderMarkets(allMarkets);
}

function renderMarkets(markets) {
  const el = document.getElementById('markets-list');
  if (!markets.length) { el.innerHTML = '<div class="empty">No markets</div>'; return; }
  el.innerHTML = markets.map(m => {
    const prices = m.outcome_prices || [0.5, 0.5];
    const yesPct = (prices[0] * 100).toFixed(1);
    const noPct = (prices[1] * 100).toFixed(1);
    const badgestyle = 'status-'+m.status;

    let actions = '';
    if (m.status === 'active') {
      actions = `<button onclick="openBet(${m.id})">Bet Yes</button>` +
                `<button onclick="openBet(${m.id},'no')" class="outline">Bet No</button>`;
    }
    if (m.status === 'resolved' && m.winner_idx !== null) {
      const outcomes = m.outcomes || ['Yes','No'];
      const winner = outcomes[m.winner_idx] || '?';
      actions = '<span style="font-size:13px;color:var(--accent);font-weight:600">✅ Resolved: '+winner+'</span>';
    }

    return '<div class="card">' +
      '<div class="market-header">' +
        '<div class="market-q">'+esc(m.question)+'</div>' +
        '<div class="market-meta">' +
          '<span class="status-badge '+badgestyle+'">'+m.status+'</span>' +
          '<div style="margin-top:4px">'+m.bet_count+' bets</div>' +
        '</div>' +
      '</div>' +
      '<div class="odds-bar">' +
        '<div class="odds-yes" style="width:'+yesPct+'%">Yes '+yesPct+'%</div>' +
        '<div class="odds-no" style="width:'+noPct+'%">'+noPct+'% No</div>' +
      '</div>' +
      '<div class="market-actions">'+actions+'</div>' +
    '</div>';
  }).join('');
}

// ── Betting ──
function openBet(marketId, side='yes') {
  if (!PM.getUser()) return toast('Please log in first', 'error');
  currentMarketId = marketId;
  const m = allMarkets.find(x => x.id === marketId);
  if (!m) return;
  const prices = m.outcome_prices || [0.5, 0.5];
  document.getElementById('bet-market-q').textContent = m.question;
  document.getElementById('bet-side').value = side;
  document.getElementById('bet-amount').value = '';
  document.getElementById('bet-odds').textContent = (prices[side==='yes'?0:1] * 100).toFixed(1) + '%';
  document.getElementById('bet-payout').textContent = '';
  document.getElementById('bet-balance').textContent = PM.getUser().points.toFixed(0);
  document.getElementById('bet-modal').classList.add('show');
}

// Set the bet amount via quick chips. 'max' uses the user's full balance.
function setBetAmount(v) {
  const el = document.getElementById('bet-amount');
  if (v === 'max') {
    el.value = PM.getUser()?.points?.toFixed(0) || 0;
  } else {
    el.value = v;
  }
}

// Set the KO bet amount via quick chips (uses KO bet amount field).
function setKOBetAmount(v) {
  const el = document.getElementById('ko-bet-amount');
  if (v === 'max') {
    el.value = PM.getUser()?.points?.toFixed(0) || 0;
  } else {
    el.value = v;
  }
}

// Open the Polymarket bet modal for a market (used by the markets tab).
function openBetModal(m) {
  if (!PM.getUser()) return toast('Please log in first', 'error');
  currentMarketId = m.id;
  const prices = m.outcome_prices || [0.5, 0.5];
  document.getElementById('bet-market-q').textContent = m.question;
  document.getElementById('bet-side').value = 'yes';
  document.getElementById('bet-amount').value = '';
  document.getElementById('bet-odds').textContent = (prices[0] * 100).toFixed(1) + '%';
  document.getElementById('bet-payout').textContent = '';
  document.getElementById('bet-balance').textContent = PM.getUser().points.toFixed(0);
  document.getElementById('bet-modal').dataset.prices = JSON.stringify(prices);
  document.getElementById('bet-modal').classList.add('show');
}

function updateBetPreview() {
  const amount = parseFloat(document.getElementById('bet-amount').value) || 0;
  const modal = document.getElementById('bet-modal');
  const prices = JSON.parse(modal.dataset.prices || '[0.5,0.5]');
  const side = document.getElementById('bet-side').value;
  const odds = prices[side==='yes'?0:1];
  if (amount > 0 && odds > 0) {
    const payout = amount / odds;
    document.getElementById('bet-payout').textContent = payout.toFixed(0) + ' pts';
  } else {
    document.getElementById('bet-payout').textContent = '—';
  }
}

async function submitBet() {
  if (!PM.getUser()) return toast('Please log in first', 'error');
  const amount = parseInt(document.getElementById('bet-amount').value);
  if (!amount || amount < 1) return toast('Enter valid amount', 'error');
  try {
    // api = PM.api, which injects the Authorization header. Server reads
    // user_id from auth context — not from body. Don't pass user_id here.
    const r = await api('/api/bets', {
      method: 'POST',
      body: JSON.stringify({
        market_id: currentMarketId,
        side: document.getElementById('bet-side').value,
        amount: amount,
      })
    });
    closeModal('bet-modal');
    await PM.refreshMe();
    loadMarkets(document.querySelector('#tab-markets button:not(.outline)')?.id?.replace('filter-','') || 'active');
    toast('Bet placed!', 'success');
  } catch (e) { toast(e.message, 'error'); }
}

// ── My Bets (markets page) ──
async function loadMyBets() {
  const el = document.getElementById('mybets-list');
  if (!PM.getUser()) { el.innerHTML = '<div class="empty">Please log in first</div>'; return; }
  // New per-user history endpoint — returns market/match/KO bets + totals
  const data = await api(`/api/users/${PM.getUser().id}/history?limit=100&scope=all`);
  const bets = data.market_bets || [];
  if (!bets.length) { el.innerHTML = '<div class="empty">No market bets yet</div>'; return; }

  el.innerHTML = bets.map(b => {
    let statusHtml = '';
    if (b.won === null) statusHtml = '<span class="status-badge status-active">PENDING</span>';
    else if (b.won) statusHtml = '<span class="status-badge status-resolved" style="background:rgba(0,230,118,0.15);color:var(--green)">WON +'+(b.payout ?? 0).toFixed(0)+'</span>';
    else statusHtml = '<span class="status-badge" style="background:rgba(255,82,82,0.15);color:var(--red)">LOST</span>';

    const sideHtml = b.side === 'yes' ?
      '<span style="color:var(--green)">YES</span>' :
      '<span style="color:var(--red)">NO</span>';

    return '<div class="card">' +
      '<div style="font-size:14px;font-weight:600;margin-bottom:6px">'+esc(b.market_question)+'</div>' +
      '<div style="display:flex;justify-content:space-between;align-items:center">' +
        '<div><span style="font-size:13px;color:var(--text-dim)">'+sideHtml+' • '+b.amount+' pts @ '+(b.odds*100).toFixed(0)+'%</span></div>' +
        '<div>'+statusHtml+'</div>' +
      '</div>' +
      '<div style="font-size:11px;color:var(--text-dim);margin-top:4px">'+new Date(b.placed_at+'Z').toLocaleString()+'</div>' +
    '</div>';
  }).join('');
}

// ── Leaderboard ──
async function loadLeaderboard() {
  const data = await api('/api/leaderboard');
  const el = document.getElementById('leaderboard-content');
  if (!data.length) { el.innerHTML = '<div class="empty">No users yet</div>'; return; }

  let html = '<table class="leaderboard-table"><thead><tr><th>#</th><th>Name</th><th>Points</th><th>Bets</th><th>W</th><th>L</th><th>Pending</th></tr></thead><tbody>';
  data.forEach((u, i) => {
    const rankClass = i < 3 ? ' rank-'+(i+1) : '';
    const pnlClass = u.points >= 1000 ? 'points-pos' : 'points-neg';
    html += '<tr>' +
      '<td class="rank'+rankClass+'">'+(i+1)+'</td>' +
      '<td style="font-weight:600">'+esc(u.name)+'</td>' +
      '<td class="points '+pnlClass+'">'+u.points.toFixed(0)+'</td>' +
      '<td>'+u.total_bets+'</td>' +
      '<td style="color:var(--green)">'+u.wins+'</td>' +
      '<td style="color:var(--red)">'+u.losses+'</td>' +
      '<td>'+u.pending+'</td>' +
    '</tr>';
  });
  html += '</tbody></table>';
  el.innerHTML = html;
}

// ── Import / Search ──
async function searchPoly() {
  const q = document.getElementById('search-query').value.trim();
  if (!q) return;
  const el = document.getElementById('search-results');
  el.innerHTML = '<div style="color:var(--text-dim);padding:12px">Searching Polymarket...</div>';
  try {
    polymarketResults = await api('/api/markets/search', {
      method: 'POST', body: JSON.stringify({query: q})
    });
  } catch (e) { toast(e.message, 'error'); return; }
  if (!polymarketResults.length) {
    el.innerHTML = '<div class="empty">No results</div>';
    return;
  }
  el.innerHTML = polymarketResults.map((m, i) => {
    const pct = (m.prices[0] * 100).toFixed(1);
    const badge = m.status === 'resolved' ? 'Resolved' : m.status === 'closed' ? 'Closed' : 'Active';
    const bClass = 'status-'+m.status;
    const importBtn = m.status === 'resolved' ? '' :
      '<button onclick="importMarket('+i+')" style="font-size:12px;padding:4px 10px">+ Import</button>';
    return '<div class="search-result">' +
      '<div style="display:flex;justify-content:space-between;align-items:flex-start">' +
        '<div style="flex:1;font-size:14px;font-weight:600">'+esc(m.question)+'</div>' +
        '<div style="display:flex;gap:6px;align-items:center">' +
          '<span class="status-badge '+bClass+'">'+badge+'</span>' +
          importBtn +
        '</div>' +
      '</div>' +
      '<div style="font-size:12px;color:var(--text-dim);margin-top:4px">' +
        'Yes '+pct+'% • Vol $'+(m.volume||0).toLocaleString() +
      '</div>' +
    '</div>';
  }).join('');
}

async function importMarket(idx) {
  const m = polymarketResults[idx];
  if (!m) return;
  try {
    await api('/api/markets/import', {
      method: 'POST', body: JSON.stringify(m)
    });
    toast('Imported!', 'success');
    await loadMarkets('active');
  } catch(e) { toast(e.message, 'error'); }
}

async function refreshAll() {
  toast('Refreshing all markets...', 'success');
  for (const m of allMarkets) {
    if (m.status !== 'resolved') {
      try { await api('/api/markets/refresh', {method:'POST', body: JSON.stringify({market_id: m.id})}); }
      catch(e) { console.warn('Refresh failed for', m.id); }
    }
  }
  await loadMarkets('active');
  toast('Refreshed!', 'success');
}

async function resolveAll() {
  toast('Checking resolutions...', 'success');
  const r = await api('/api/admin/resolve-all', {method:'POST'});
  toast('Resolved '+r.resolved+' markets!', 'success');
  await loadMarkets('active');
  await loadUsers();
}

// ── Utilities ──
function closeModal(id) { document.getElementById(id).classList.remove('show'); }
function esc(s) {
  const d = document.createElement('div'); d.textContent = s; return d.innerHTML;
}
function toast(msg, type) {
  const t = document.getElementById('toast');
  t.textContent = msg; t.className = 'toast '+type+' show';
  setTimeout(() => t.classList.remove('show'), 3000);
}
</script>
</body>
</html>"""

WORLDCUP_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>FIFA World Cup 2026 — Prediction Market</title>
<style>
:root {
  --bg: #0a0a0f; --card-bg: rgba(20,20,35,0.7); --card-border: rgba(255,255,255,0.06);
  --text: #e0e0e0; --text-dim: #888; --accent: #7c5cff; --green: #00e676;
  --red: #ff5252; --gold: #ffd740; --radius: 12px;
}
* { margin:0; padding:0; box-sizing:border-box; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Display', sans-serif;
  background: var(--bg); color: var(--text); min-height: 100vh;
  background-image: radial-gradient(ellipse at 20% 20%, rgba(124,92,255,0.08) 0%, transparent 50%),
                    radial-gradient(ellipse at 80% 80%, rgba(255,215,64,0.05) 0%, transparent 50%);
}
.container { max-width: 1300px; margin: 0 auto; padding: 24px 16px; }
header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; flex-wrap: wrap; gap: 12px; }
h1 { font-size: 26px; font-weight: 800; background: linear-gradient(135deg, #ffd740, #ff9100, var(--accent)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
.user-bar { display: flex; gap: 8px; align-items: center; }
select, input, button { background: var(--card-bg); border: 1px solid var(--card-border); color: var(--text); border-radius: 8px; padding: 8px 14px; font-size: 14px; font-family: inherit; }
button { cursor: pointer; transition: all .15s; background: var(--accent); border-color: var(--accent); font-weight: 600; }
button:hover { opacity: 0.85; }
button.outline { background: transparent; border-color: var(--card-border); }
button.outline:hover { border-color: var(--accent); }
button.gold { background: linear-gradient(135deg,#ffd740,#ff9100); border-color: #ffd740; color:#000; }
button.sm { font-size: 11px; padding: 4px 10px; }

.tabs { display: flex; gap: 6px; margin-bottom: 20px; flex-wrap: wrap; }
.tab { padding: 10px 18px; border-radius: 8px; cursor: pointer; font-weight: 700; font-size: 13px; background: var(--card-bg); border: 1px solid var(--card-border); color: var(--text-dim); transition: all .2s; text-transform: uppercase; letter-spacing: 0.5px; }
.tab.active { background: var(--accent); color: #fff; border-color: var(--accent); }
.tab:hover:not(.active) { border-color: var(--accent); color: var(--text); }

.groups-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 16px; margin-bottom: 24px; }
.group-card { background: var(--card-bg); border: 1px solid var(--card-border); border-radius: var(--radius); overflow: hidden; backdrop-filter: blur(20px); }
.group-card:hover { border-color: rgba(124,92,255,0.3); }
.group-header { padding: 10px 16px; font-weight: 800; font-size: 15px; background: linear-gradient(135deg, rgba(124,92,255,0.2), rgba(255,215,64,0.1)); border-bottom: 1px solid var(--card-border); display: flex; justify-content: space-between; align-items: center; }

/* Standings table */
.standings-table { width: 100%; border-collapse: collapse; font-size: 13px; }
.standings-table th { font-size: 10px; text-transform: uppercase; color: var(--text-dim); padding: 6px 8px; text-align: center; border-bottom: 1px solid var(--card-border); }
.standings-table th:first-child { text-align: left; padding-left: 16px; }
.standings-table td { padding: 6px 8px; text-align: center; border-bottom: 1px solid rgba(255,255,255,0.03); }
.standings-table td:first-child { text-align: left; padding-left: 16px; font-weight: 600; }
.standings-table tr:hover td { background: rgba(255,255,255,0.02); }
.standings-pts { font-weight: 800; color: var(--gold); }

/* Team odds display */
.team-odds { display: flex; gap: 8px; font-size: 11px; justify-content: center; }
.team-odds span { display: flex; flex-direction: column; align-items: center; }
.odds-label { color: var(--text-dim); font-size: 9px; }
.odds-val { font-weight: 700; }
.odds-high { color: var(--green); } .odds-mid { color: var(--gold); } .odds-low { color: var(--red); }

/* Match cards */
.match-card { background: rgba(255,255,255,0.02); border: 1px solid var(--card-border); border-radius: 8px; padding: 10px 14px; margin-bottom: 6px; display: flex; align-items: center; gap: 10px; transition: all .15s; font-size: 13px; }
.match-card:hover { border-color: rgba(255,215,64,0.3); }
.match-day { background: var(--accent); color: #fff; border-radius: 4px; padding: 2px 6px; font-size: 10px; font-weight: 700; white-space: nowrap; }
.match-teams { flex: 1; font-weight: 600; }
.match-teams .vs { color: var(--text-dim); margin: 0 4px; }
.match-score { font-weight: 800; font-size: 14px; color: var(--gold); min-width: 30px; text-align: center; }
.match-status { font-size: 10px; font-weight: 700; padding: 2px 6px; border-radius: 4px; white-space: nowrap; }
.status-scheduled { background: rgba(124,92,255,0.15); color: var(--accent); }
.status-completed { background: rgba(0,230,118,0.15); color: var(--green); }

/* Knockout bracket */
.bracket-round { margin-bottom: 24px; }
.bracket-round h3 { font-size: 15px; color: var(--gold); margin-bottom: 10px; padding: 6px 12px; background: rgba(255,215,64,0.08); border-radius: 6px; display: inline-block; }
.bracket-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 10px; }
.knockout-card { background: var(--card-bg); border: 1px solid var(--card-border); border-radius: 8px; padding: 12px 14px; display: flex; align-items: center; gap: 10px; }
.knockout-card.live { border-color: rgba(124,92,255,0.4); }
.knockout-slot { font-size: 10px; color: var(--text-dim); min-width: 50px; }
.knockout-teams { flex: 1; }
.knockout-team { padding: 3px 0; font-size: 13px; font-weight: 600; }
.knockout-team.tbd { color: var(--text-dim); font-style: italic; font-weight: 400; }
.knockout-winner { font-size: 10px; }
.knockout-winner span { color: var(--green); font-weight: 700; }
.knockout-actions { display: flex; gap: 4px; }

/* Leaderboard */
.leaderboard-table { width: 100%; border-collapse: collapse; }
.leaderboard-table th, .leaderboard-table td { padding: 10px 14px; text-align: left; font-size: 14px; border-bottom: 1px solid var(--card-border); }
.leaderboard-table th { font-size: 11px; text-transform: uppercase; color: var(--text-dim); }
.rank { font-weight: 800; font-size: 18px; }
.rank-1 { color: var(--gold); } .rank-2 { color: #c0c0c0; } .rank-3 { color: #cd7f32; }
.points { font-weight: 700; font-variant-numeric: tabular-nums; }

/* Modal */
.modal-overlay { display: none; position: fixed; top:0; left:0; right:0; bottom:0; background: rgba(0,0,0,0.75); z-index: 100; justify-content: center; align-items: center; }
.modal-overlay.show { display: flex; }
.modal { background: #1a1a2e; border: 1px solid var(--card-border); border-radius: var(--radius); padding: 24px; max-width: 440px; width: 90%; backdrop-filter: blur(20px); }
.modal h3 { margin-bottom: 16px; font-size: 16px; }
.modal .field { margin-bottom: 14px; }
.modal .field label { display: block; font-size: 12px; color: var(--text-dim); margin-bottom: 4px; }
.modal .field select, .modal .field input { width: 100%; }
.modal .score-row { display: flex; gap: 8px; }
.modal .score-row input { width: 60px; text-align: center; font-size: 18px; font-weight: 700; }
.modal .actions { display: flex; gap: 8px; margin-top: 20px; }
.modal .actions button { flex: 1; }

.toast { position: fixed; bottom: 20px; right: 20px; padding: 12px 20px; border-radius: 8px; font-size: 14px; font-weight: 600; z-index: 200; display: none; animation: slideIn 0.3s ease; }
.toast.show { display: block; }
.toast.success { background: var(--green); color: #000; }
.toast.error { background: var(--red); color: #fff; }
@keyframes slideIn { from { transform: translateY(20px); opacity: 0; } to { transform: translateY(0); opacity: 1; } }

.empty { text-align: center; padding: 40px; color: var(--text-dim); }
.tag { display: inline-block; padding: 2px 6px; border-radius: 4px; font-size: 10px; font-weight: 700; margin-left: 4px; }
.tag-win { background: rgba(0,230,118,0.15); color: var(--green); }
.tag-draw { background: rgba(255,215,64,0.15); color: var(--gold); }
.tag-loss { background: rgba(255,82,82,0.15); color: var(--red); }

/* Stats grid + bet cards */
.stats-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-bottom: 18px; }
@media (max-width: 720px) { .stats-grid { grid-template-columns: repeat(2, 1fr); } }
.stat-card { background: var(--card-bg); border: 1px solid var(--card-border); border-radius: 10px; padding: 12px 14px; backdrop-filter: blur(20px); }
.stat-label { font-size: 10px; color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px; }
.stat-value { font-size: 22px; font-weight: 800; color: var(--text); }
.stat-value .stat-unit { font-size: 11px; color: var(--text-dim); font-weight: 400; margin-left: 3px; }

.section-h { font-size: 11px; color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.5px; margin: 14px 0 8px; padding-left: 4px; }

.bet-card { padding: 12px 14px; }
.bet-card.group-bet { background: rgba(124,92,255,0.06); border-color: rgba(124,92,255,0.25); }
.bet-card.ko-bet { background: rgba(255,215,64,0.06); border-color: rgba(255,215,64,0.25); }
.bet-card:hover { transform: translateY(-1px); transition: transform 0.15s; }

/* Empty + loading states */
.spinner { display: inline-block; width: 16px; height: 16px; border: 2px solid var(--card-border); border-top-color: var(--accent); border-radius: 50%; animation: spin 0.7s linear infinite; vertical-align: middle; }
@keyframes spin { to { transform: rotate(360deg); } }

/* Match history (used in History tab) */
.history-row { display: flex; align-items: center; gap: 10px; padding: 8px 12px; border-radius: 8px; border: 1px solid var(--card-border); margin-bottom: 6px; }
.history-row .score-pill { background: var(--card-bg); padding: 2px 8px; border-radius: 6px; font-weight: 700; font-size: 13px; color: var(--gold); }
.history-row .winner-tag { padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 700; }
.history-row .winner-tag.win { background: rgba(0,230,118,0.15); color: var(--green); }
.history-row .winner-tag.lose { background: rgba(255,82,82,0.15); color: var(--red); }
.history-row .winner-tag.pending { background: rgba(255,215,64,0.15); color: var(--gold); }

/* Live refresh indicator */
.last-updated-bar { display: flex; align-items: center; gap: 8px; margin-bottom: 10px; font-size: 11px; color: var(--text-dim); }
.last-updated { font-variant-numeric: tabular-nums; }
.last-updated.stale { color: var(--gold); font-weight: 700; }

/* Live match (in-progress) — pulsing dot */
.live-pulse { display: inline-block; width: 8px; height: 8px; background: var(--red); border-radius: 50%; margin-right: 6px; animation: live-pulse 1.4s ease-in-out infinite; }
@keyframes live-pulse { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.4; transform: scale(1.4); } }

/* Score-flash animation — fires when live score changes between refreshes */
@keyframes score-flash { 0% { background: var(--red); transform: scale(1.4); color: #fff; }
  50% { background: var(--red); color: #fff; }
  100% { background: transparent; transform: scale(1); color: var(--red); } }
.score-flash { animation: score-flash 1.5s ease-out; padding: 2px 6px; border-radius: 4px; }

/* In-progress match — slight red border, full opacity */
.match-card.status-in-progress { border-color: rgba(255,82,82,0.4); background: rgba(255,82,82,0.04); }
.status-in-progress { color: var(--red); }
</style>
</head>
<script src="/static/app.js"></script>
<body>
<div class="container">
  <header>
    <h1>⚽ FIFA World Cup 2026</h1>
    <div class="user-bar" id="pm-user-bar"></div>
  </header>

  <!-- Pinned LIVE banner — always visible at top when a match is in progress -->
  <div id="live-banner" style="display:none;background:linear-gradient(90deg, rgba(255,82,82,0.15), rgba(255,82,82,0.05));border:1px solid rgba(255,82,82,0.4);border-radius:8px;padding:10px 14px;margin:0 0 14px 0;align-items:center;gap:14px;cursor:pointer;backdrop-filter:blur(20px);transition:transform 0.15s" onclick="openLiveMatch()">
    <div style="display:flex;align-items:center;gap:8px;flex-shrink:0">
      <span class="live-pulse"></span><span style="font-size:11px;font-weight:700;color:var(--red);letter-spacing:0.5px">LIVE NOW</span>
    </div>
    <div id="live-banner-content" style="flex:1;display:flex;align-items:center;gap:14px;flex-wrap:wrap;min-width:0"></div>
    <span style="font-size:10px;color:var(--text-dim);flex-shrink:0">View match →</span>
  </div>

  <div class="tabs">
    <div class="tab active" onclick="switchView('groups', this)">📊 Groups</div>
    <div class="tab" onclick="switchView('mybets', this)">🎯 My Bets</div>
    <div class="tab" onclick="switchView('advance', this)">🚀 Advance</div>
    <div class="tab" onclick="switchView('knockout', this)">🏟️ Knockout</div>
    <div class="tab" onclick="switchView('history', this)">📋 History</div>
    <div class="tab" onclick="switchView('activity', this)">📡 Activity</div>
    <div class="tab" onclick="switchView('leaderboard', this)">🏆 Leaderboard</div>
    <div class="tab" onclick="switchView('admin', this)">⚙️ Admin</div>
  </div>

  <!-- GROUPS -->
  <div id="view-groups">
    <div class="live-bar-slot" data-view="groups"></div>
    <div style="margin-bottom:14px;display:flex;gap:6px;flex-wrap:wrap" id="group-filters"></div>
    <div class="groups-grid" id="groups-grid"><div class="empty">Loading...</div></div>
  </div>

  <!-- MY BETS (active only) -->
  <div id="view-mybets" style="display:none">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
      <h3 style="margin:0">🎯 My Active Bets</h3>
      <span id="mybets-count" style="font-size:11px;color:var(--text-dim)"></span>
    </div>
    <div class="live-bar-slot" data-view="mybets"></div>
    <p style="color:var(--text-dim);font-size:12px;margin:0 0 12px">Pending wagers on group + knockout matches. Resolved bets move to <b>History</b>.</p>
    <div id="mybets-content"><div class="empty">Loading...</div></div>
  </div>

  <!-- ADVANCE (per-round Polymarket markets) -->
  <div id="view-advance" style="display:none">
    <div class="live-bar-slot" data-view="advance"></div>
    <div id="advance-grid"><div class="empty">Loading advance markets...</div></div>
  </div>

  <!-- KNOCKOUT -->
  <div id="view-knockout" style="display:none">
    <div class="live-bar-slot" data-view="knockout"></div>
    <div id="knockout-content"><div class="empty">Loading...</div></div>
  </div>

  <!-- HISTORY -->
  <div id="view-history" style="display:none">
    <div class="live-bar-slot" data-view="history"></div>
    <div style="margin-bottom:14px">
      <h3 style="margin:0 0 8px 0">📋 Bet History</h3>
    </div>
    <div id="history-content"><div class="empty">Loading...</div></div>
  </div>

  <!-- ACTIVITY (public feed) -->
  <div id="view-activity" style="display:none">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
      <h3 style="margin:0">📡 Live Activity</h3>
      <span id="activity-count" style="font-size:11px;color:var(--text-dim)"></span>
    </div>
    <div class="live-bar-slot" data-view="activity"></div>
    <p style="color:var(--text-dim);font-size:12px;margin:0 0 12px">Every bet placed across all users — public for transparency.</p>
    <div id="activity-content"><div class="empty">Loading...</div></div>
  </div>

  <!-- LEADERBOARD -->
  <div id="view-leaderboard" style="display:none">
    <div class="live-bar-slot" data-view="leaderboard"></div>
    <div id="leaderboard-content"></div>
  </div>

  <!-- ADMIN -->
  <div id="view-admin" style="display:none">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
      <h3 style="margin:0">⚙️ Match Status</h3>
      <div style="display:flex;gap:8px;align-items:center">
        <span id="sync-status" style="font-size:11px;color:var(--text-dim)"></span>
        <button class="sm outline" onclick="syncNow()">🔄 Sync Now</button>
      </div>
    </div>
    <p style="color:var(--text-dim);font-size:13px;margin-bottom:12px">Auto-resolved from ESPN + Polymarket + OKX every 5 min. Matches below show real-time status.</p>
    <div id="admin-matches-list"></div>

    <h3 style="margin:24px 0 8px">🏟️ Knockout Bracket</h3>
    <p style="color:var(--text-dim);font-size:13px;margin-bottom:8px">Set teams in R32 slots, then advance winners after each round.</p>
    <div id="admin-ko-list"></div>

    <h3 style="margin:24px 0 8px">⚠️ Danger Zone</h3>
    <div style="background:rgba(255,82,82,0.08);border:1px solid rgba(255,82,82,0.3);border-radius:8px;padding:14px">
      <p style="font-size:12px;color:var(--text-dim);margin-bottom:10px">Reset clears all bets, standings, and resets points to 1000. Match schedule + odds preserved. Cannot undo.</p>
      <button onclick="resetAll()" style="background:var(--red);border-color:var(--red);font-size:12px">⚠️ Reset All Data</button>
    </div>
  </div>
</div>

<!-- BET MODAL -->
<div class="modal-overlay" id="bet-modal">
  <div class="modal">
    <h3>Place Bet</h3>
    <div id="bet-match-info" style="font-size:14px;margin-bottom:12px;color:var(--text-dim)"></div>

    <div class="field">
      <label>Your Pick (3 options)</label>
      <input type="hidden" id="bet-pick" value="team_a">
      <div id="bet-pick-buttons" style="display:flex;gap:6px;flex-wrap:wrap;margin-top:4px">
        <button type="button" class="pick-card" data-pick="team_a" onclick="selectPick(this)" style="flex:1;min-width:90px;padding:8px 6px;border-radius:8px;border:2px solid var(--accent);background:rgba(122,162,247,0.15);text-align:center;cursor:pointer;color:var(--text);font-size:12px">
          <div id="pick-team_a-name" style="font-weight:700;font-size:13px">Home</div>
          <div id="pick-team_a-pct" style="font-size:10px;color:var(--gold);margin-top:3px">—</div>
        </button>
        <button type="button" class="pick-card" data-pick="draw" onclick="selectPick(this)" style="flex:1;min-width:90px;padding:8px 6px;border-radius:8px;border:2px solid var(--card-border);background:transparent;text-align:center;cursor:pointer;color:var(--text);font-size:12px">
          <div style="font-weight:700;font-size:13px">Draw</div>
          <div id="pick-draw-pct" style="font-size:10px;color:var(--gold);margin-top:3px">—</div>
        </button>
        <button type="button" class="pick-card" data-pick="team_b" onclick="selectPick(this)" style="flex:1;min-width:90px;padding:8px 6px;border-radius:8px;border:2px solid var(--card-border);background:transparent;text-align:center;cursor:pointer;color:var(--text);font-size:12px">
          <div id="pick-team_b-name" style="font-weight:700;font-size:13px">Away</div>
          <div id="pick-team_b-pct" style="font-size:10px;color:var(--gold);margin-top:3px">—</div>
        </button>
      </div>
    </div>

    <!-- Polymarket-style YES/NO toggle. Default YES. NO bets against the
         picked outcome (i.e., bet that this outcome DOESN'T happen). -->
    <div class="field">
      <label>Bet on this outcome</label>
      <div style="display:flex;gap:6px;margin-top:4px">
        <button type="button" id="bet-side-yes" onclick="selectBetSide('yes')" style="flex:1;padding:6px;border-radius:6px;border:2px solid var(--green);background:rgba(0,230,118,0.15);color:var(--text);font-family:inherit;cursor:pointer">
          <div style="font-weight:700;font-size:13px">YES</div>
          <div style="font-size:10px;color:var(--green)">bet this happens</div>
        </button>
        <button type="button" id="bet-side-no" onclick="selectBetSide('no')" style="flex:1;padding:6px;border-radius:6px;border:2px solid var(--card-border);background:transparent;color:var(--text);font-family:inherit;cursor:pointer">
          <div style="font-weight:700;font-size:13px">NO</div>
          <div style="font-size:10px;color:var(--red)">bet this doesn't</div>
        </button>
      </div>
    </div>

    <div class="field">
      <label>Amount (points) — you have <span id="bet-balance" style="color:var(--gold);font-weight:700">…</span> pts</label>
      <input type="number" id="bet-amount" min="1" placeholder="100" oninput="updateBetSummary()">
      <div style="display:flex;gap:6px;margin-top:6px;flex-wrap:wrap">
        <button class="sm outline" onclick="setBetAmount(10);updateBetSummary()" style="font-size:11px">10</button>
        <button class="sm outline" onclick="setBetAmount(50);updateBetSummary()" style="font-size:11px">50</button>
        <button class="sm outline" onclick="setBetAmount(100);updateBetSummary()" style="font-size:11px">100</button>
        <button class="sm outline" onclick="setBetAmount(250);updateBetSummary()" style="font-size:11px">250</button>
        <button class="sm outline" onclick="setBetAmount(500);updateBetSummary()" style="font-size:11px">500</button>
        <button class="sm outline gold" onclick="setBetAmount('max');updateBetSummary()" style="font-size:11px">MAX</button>
      </div>
    </div>

    <div class="field">
      <label>Summary</label>
      <div id="bet-summary" style="font-size:13px;line-height:1.6"></div>
    </div>

    <div class="actions">
      <button onclick="closeModal('bet-modal')" class="outline">Cancel</button>
      <button onclick="submitMatchBet()" class="gold">Place Bet</button>
    </div>
  </div>
</div>

<!-- KO BET MODAL — 2-pick + YES/NO toggle pattern (no draws in KO) -->
<div class="modal-overlay" id="ko-bet-modal">
  <div class="modal">
    <h3>Place KO Bet</h3>
    <div id="ko-bet-info" style="font-size:14px;margin-bottom:12px;color:var(--text-dim)"></div>
    <div class="field">
      <label>Your Pick (2 options — no draws in knockout)</label>
      <input type="hidden" id="ko-bet-pick" value="team_a">
      <div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:4px">
        <button type="button" class="pick-card" data-pick="team_a" onclick="selectKOPick(this)" style="flex:1;min-width:90px;padding:8px 6px;border-radius:8px;border:2px solid var(--accent);background:rgba(122,162,247,0.15);text-align:center;cursor:pointer;color:var(--text);font-size:12px">
          <div id="ko-pick-team_a-name" style="font-weight:700;font-size:13px">Home</div>
          <div id="ko-pick-team_a-pct" style="font-size:10px;color:var(--gold);margin-top:3px">—</div>
        </button>
        <button type="button" class="pick-card" data-pick="team_b" onclick="selectKOPick(this)" style="flex:1;min-width:90px;padding:8px 6px;border-radius:8px;border:2px solid var(--card-border);background:transparent;text-align:center;cursor:pointer;color:var(--text);font-size:12px">
          <div id="ko-pick-team_b-name" style="font-weight:700;font-size:13px">Away</div>
          <div id="ko-pick-team_b-pct" style="font-size:10px;color:var(--gold);margin-top:3px">—</div>
        </button>
      </div>
    </div>

    <!-- YES/NO toggle. Default YES. NO bets against the picked team winning. -->
    <div class="field">
      <label>Bet on this outcome</label>
      <div style="display:flex;gap:6px;margin-top:4px">
        <button type="button" id="ko-bet-side-yes" onclick="selectKOBetSide('yes')" style="flex:1;padding:6px;border-radius:6px;border:2px solid var(--green);background:rgba(0,230,118,0.15);color:var(--text);font-family:inherit;cursor:pointer">
          <div style="font-weight:700;font-size:13px">YES</div>
          <div style="font-size:10px;color:var(--green)">bet this team wins</div>
        </button>
        <button type="button" id="ko-bet-side-no" onclick="selectKOBetSide('no')" style="flex:1;padding:6px;border-radius:6px;border:2px solid var(--card-border);background:transparent;color:var(--text);font-family:inherit;cursor:pointer">
          <div style="font-weight:700;font-size:13px">NO</div>
          <div style="font-size:10px;color:var(--red)">bet this team doesn't</div>
        </button>
      </div>
    </div>

    <div class="field">
      <label>Amount (points) — you have <span id="ko-bet-balance" style="color:var(--gold);font-weight:700">…</span> pts</label>
      <input type="number" id="ko-bet-amount" min="1" placeholder="100" oninput="updateKOBetSummary()">
      <div style="display:flex;gap:6px;margin-top:6px;flex-wrap:wrap">
        <button class="sm outline" onclick="setKOBetAmount(10);updateKOBetSummary()" style="font-size:11px">10</button>
        <button class="sm outline" onclick="setKOBetAmount(50);updateKOBetSummary()" style="font-size:11px">50</button>
        <button class="sm outline" onclick="setKOBetAmount(100);updateKOBetSummary()" style="font-size:11px">100</button>
        <button class="sm outline" onclick="setKOBetAmount(250);updateKOBetSummary()" style="font-size:11px">250</button>
        <button class="sm outline" onclick="setKOBetAmount(500);updateKOBetSummary()" style="font-size:11px">500</button>
        <button class="sm outline gold" onclick="setKOBetAmount('max');updateKOBetSummary()" style="font-size:11px">MAX</button>
      </div>
    </div>

    <div class="field">
      <label>Summary</label>
      <div id="ko-bet-summary" style="font-size:13px;line-height:1.6"></div>
    </div>

    <div class="actions">
      <button onclick="closeKOBet()" class="outline">Cancel</button>
      <button onclick="submitKOBet()" class="gold">Place Bet</button>
    </div>
  </div>
</div>

<!-- RESOLVE MODAL -->
<div class="modal-overlay" id="resolve-modal">
  <div class="modal">
    <h3>Resolve Match</h3>
    <div id="resolve-match-info" style="font-size:14px;margin-bottom:12px;color:var(--text-dim)"></div>
    <div class="field">
      <label>Scores</label>
      <div class="score-row">
        <div style="flex:1;text-align:center">
          <div style="font-size:12px;color:var(--text-dim);margin-bottom:4px" id="res-team-a-label">Home</div>
          <input type="number" id="res-score-a" min="0" value="0" onchange="autoPickWinner()">
        </div>
        <div style="display:flex;align-items:center;font-weight:700;color:var(--text-dim)">—</div>
        <div style="flex:1;text-align:center">
          <div style="font-size:12px;color:var(--text-dim);margin-bottom:4px" id="res-team-b-label">Away</div>
          <input type="number" id="res-score-b" min="0" value="0" onchange="autoPickWinner()">
        </div>
      </div>
    </div>
    <div class="field">
      <label>Winner</label>
      <select id="resolve-winner">
        <option value="team_a" id="res-team-a">Home</option>
        <option value="draw">Draw</option>
        <option value="team_b" id="res-team-b">Away</option>
      </select>
    </div>
    <div class="actions">
      <button onclick="closeModal('resolve-modal')" class="outline">Cancel</button>
      <button onclick="submitResolve()" style="background:var(--gold);border-color:var(--gold);color:#000">Confirm Result</button>
    </div>
  </div>
</div>

<!-- USER PROFILE MODAL (public — clickable from leaderboard) -->
<div class="modal-overlay" id="user-profile-modal" onclick="if(event.target===this)closeUserProfile()">
  <div class="modal" style="max-width:520px">
    <div id="user-profile-body"><div class="spinner"></div> Loading...</div>
    <div class="actions" style="margin-top:16px">
      <button onclick="closeUserProfile()" class="outline" style="flex:1">Close</button>
    </div>
  </div>
</div>
<div class="toast" id="toast"></div>
<script>
// ── State ──
// State — currentUser comes from PM.getUser() at use sites.
let currentView = 'groups', currentMatchId = null;
let groupsData = {}, matchesData = [], standingsData = [], knockoutData = [];
let currentMatchOdds = {};

// ── Country flags from flagcdn.com ──
const FLAGS = {
  'Algeria': 'https://flagcdn.com/w40/dz.png',
  'Argentina': 'https://flagcdn.com/w40/ar.png',
  'Australia': 'https://flagcdn.com/w40/au.png',
  'Austria': 'https://flagcdn.com/w40/at.png',
  'Belgium': 'https://flagcdn.com/w40/be.png',
  'Bosnia-Herzegovina': 'https://flagcdn.com/w40/ba.png',
  'Brazil': 'https://flagcdn.com/w40/br.png',
  'Canada': 'https://flagcdn.com/w40/ca.png',
  'Cape Verde': 'https://flagcdn.com/w40/cv.png',
  'Colombia': 'https://flagcdn.com/w40/co.png',
  'Congo DR': 'https://flagcdn.com/w40/cd.png',
  'Croatia': 'https://flagcdn.com/w40/hr.png',
  'Curaçao': 'https://flagcdn.com/w40/cw.png',
  'Czechia': 'https://flagcdn.com/w40/cz.png',
  'DR Congo': 'https://flagcdn.com/w40/cd.png',
  'Ecuador': 'https://flagcdn.com/w40/ec.png',
  'Egypt': 'https://flagcdn.com/w40/eg.png',
  'England': 'https://flagcdn.com/w40/gb-eng.png',
  'France': 'https://flagcdn.com/w40/fr.png',
  'Germany': 'https://flagcdn.com/w40/de.png',
  'Ghana': 'https://flagcdn.com/w40/gh.png',
  'Haiti': 'https://flagcdn.com/w40/ht.png',
  'Iran': 'https://flagcdn.com/w40/ir.png',
  'Iraq': 'https://flagcdn.com/w40/iq.png',
  'Ivory Coast': 'https://flagcdn.com/w40/ci.png',
  'Japan': 'https://flagcdn.com/w40/jp.png',
  'Jordan': 'https://flagcdn.com/w40/jo.png',
  'Mexico': 'https://flagcdn.com/w40/mx.png',
  'Morocco': 'https://flagcdn.com/w40/ma.png',
  'Netherlands': 'https://flagcdn.com/w40/nl.png',
  'New Zealand': 'https://flagcdn.com/w40/nz.png',
  'Norway': 'https://flagcdn.com/w40/no.png',
  'Panama': 'https://flagcdn.com/w40/pa.png',
  'Paraguay': 'https://flagcdn.com/w40/py.png',
  'Portugal': 'https://flagcdn.com/w40/pt.png',
  'Qatar': 'https://flagcdn.com/w40/qa.png',
  'Saudi Arabia': 'https://flagcdn.com/w40/sa.png',
  'Scotland': 'https://flagcdn.com/w40/gb-sct.png',
  'Senegal': 'https://flagcdn.com/w40/sn.png',
  'South Africa': 'https://flagcdn.com/w40/za.png',
  'South Korea': 'https://flagcdn.com/w40/kr.png',
  'Spain': 'https://flagcdn.com/w40/es.png',
  'Sweden': 'https://flagcdn.com/w40/se.png',
  'Switzerland': 'https://flagcdn.com/w40/ch.png',
  'Tunisia': 'https://flagcdn.com/w40/tn.png',
  'Türkiye': 'https://flagcdn.com/w40/tr.png',
  'USA': 'https://flagcdn.com/w40/us.png',
  'Uruguay': 'https://flagcdn.com/w40/uy.png',
  'Uzbekistan': 'https://flagcdn.com/w40/uz.png',
};
function flagImg(name) {
  const url = FLAGS[name];
  return url ? `<img src="${url}" style="width:20px;height:14px;vertical-align:middle;margin-right:6px;border-radius:2px" onerror="this.style.display='none'">` : '';
}
// OKX World Cup Winner probabilities (synced live)
const OKX_TEAMS = {
  'France': 0.17, 'Spain': 0.17, 'Portugal': 0.11, 'England': 0.10,
  'Argentina': 0.09, 'Germany': 0.07, 'Brazil': 0.06, 'Netherlands': 0.05,
};
function toHCMC(utcStr) {
  // Convert UTC ISO string to Ho Chi Minh time (UTC+7) — manual offset
  const d = new Date(utcStr);
  const hcmc = new Date(d.getTime() + 7 * 3600000); // Add 7 hours
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  const days = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
  return {
    date: days[hcmc.getUTCDay()] + ', ' + months[hcmc.getUTCMonth()] + ' ' + hcmc.getUTCDate(),
    time: String(hcmc.getUTCHours()).padStart(2,'0') + ':' + String(hcmc.getUTCMinutes()).padStart(2,'0'),
  };
}

async function init() {
  // PM.refreshMe() already ran in app.js. Render the user bar from there.
  PM.renderUserBar(document.getElementById('pm-user-bar'));
  PM.onAuthChange((u) => PM.renderUserBar(document.getElementById('pm-user-bar')));
  // Auth state lives in PM.getUser(); no need for a local currentUser.
  await loadAll();
}
async function loadAll() {
  // Use PM.api to get auth-aware fetches.
  const [groups, matches, standings, knockout] = await Promise.all([
    PM.api('/api/worldcup/groups'),
    PM.api('/api/worldcup/matches'),
    PM.api('/api/worldcup/standings'),
    PM.api('/api/worldcup/knockout'),
  ]);
  groupsData = groups; matchesData = matches; standingsData = standings; knockoutData = knockout;
  if (currentView === 'groups') renderGroups();
  else if (currentView === 'knockout') renderKnockout();
  else if (currentView === 'advance') renderAdvance();
  else if (currentView === 'admin') renderAdmin();
}
init();

function switchUser() {
  // Legacy no-op. The user-bar in this app is now auth-driven via PM.
}

// ── Live refresh ──────────────────────────────────────────────────────────
// Polls the current view every 30s so scores/odds/bets stay current.
// Pauses when the tab is hidden to save resources.

let _liveTimer = null;
let _lastUpdated = 0;
let _liveEls = []; // [{getEl(), loader}] — each tab registers itself

function liveRender(loader) {
  return async () => {
    try { await loader(); } catch (e) { console.warn('live refresh error:', e); }
    _lastUpdated = Date.now();
    document.querySelectorAll('.last-updated').forEach(el => {
      el.textContent = formatLastUpdated();
      el.classList.remove('stale');
    });
  };
}

function formatLastUpdated() {
  if (!_lastUpdated) return '—';
  const sec = Math.floor((Date.now() - _lastUpdated) / 1000);
  if (sec < 5) return 'just now';
  if (sec < 60) return `${sec}s ago`;
  const min = Math.floor(sec / 60);
  return `${min}m ago`;
}

function lastUpdatedBar(viewName, refreshFn) {
  // Returns HTML for a small "Last updated Xs ago · 🔄 Refresh" bar.
  // Registers `refreshFn` to be polled every 30s.
  const id = `lub-${viewName}`;
  _liveEls.push({ id, refresh: refreshFn });
  return `<div class="last-updated-bar">
    <span class="last-updated" id="${id}">—</span>
    <button class="sm outline" style="font-size:10px;padding:2px 8px" onclick="liveRefreshByView('${viewName}')">🔄 Refresh</button>
  </div>`;
}

function liveRefreshByView(viewName) {
  const entry = _liveEls.find(e => e.id === `lub-${viewName}`);
  if (entry) entry.refresh();
}

function startLiveUpdates() {
  stopLiveUpdates();
  _lastUpdated = Date.now();
  document.querySelectorAll('.last-updated').forEach(el => el.textContent = formatLastUpdated());
  _liveTimer = setInterval(() => {
    if (document.hidden) return; // pause when tab is in background
    // Find current view and refresh
    const visibleView = Array.from(document.querySelectorAll('[id^="view-"]')).find(v => v.style.display !== 'none');
    if (!visibleView) return;
    const viewName = visibleView.id.replace('view-', '');
    const entry = _liveEls.find(e => e.id === `lub-${viewName}`);
    if (entry) entry.refresh();
  }, 15000);
}

function stopLiveUpdates() {
  if (_liveTimer) { clearInterval(_liveTimer); _liveTimer = null; }
}

// Refresh the "X ago" label every 10s even when no data changes.
setInterval(() => {
  document.querySelectorAll('.last-updated').forEach(el => {
    const txt = formatLastUpdated();
    if (el.textContent !== txt) {
      el.textContent = txt;
      // Mark stale if > 90s
      const sec = _lastUpdated ? Math.floor((Date.now() - _lastUpdated) / 1000) : 0;
      if (sec > 90) el.classList.add('stale');
    }
  });
}, 10000);

function switchView(v, el) {
  currentView = v;
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('[id^="view-"]').forEach(d => d.style.display = 'none');
  document.getElementById('view-'+v).style.display = 'block';
  el.classList.add('active');
  if (v === 'groups') renderGroups();
  else if (v === 'mybets') renderMyBets();
  else if (v === 'advance') {
    if (typeof renderAdvance === 'function') renderAdvance();
    else document.getElementById('advance-grid').innerHTML = '<div style="color:var(--red);padding:20px">⚠️ renderAdvance() not loaded — please hard-refresh (Cmd+Shift+R)</div>';
  }
  else if (v === 'knockout') renderKnockout();
  else if (v === 'history') renderHistory();
  else if (v === 'activity') renderActivity();
  else if (v === 'leaderboard') renderLeaderboard();
  else if (v === 'admin') renderAdmin();
  startLiveUpdates(); // reset the live polling on the new view
}

// Mount the "Last updated Xs ago" bars into each view's slot. Called once
// on init (the slot is empty until the JS runs, so we can't use template
// interpolation for these since the HTML is in a Python string).
const _LIVE_REFRESH = {
  groups:    () => loadAll().then(() => { renderGroups(); _lastUpdated = Date.now(); }),
  mybets:    () => renderMyBets(),
  advance:   () => renderAdvance(),
  knockout:  () => loadAll().then(renderKnockout),
  history:   () => renderHistory(),
  activity:  () => renderActivity(),
  leaderboard: () => renderLeaderboard(),
};

function mountLiveBars() {
  document.querySelectorAll('.live-bar-slot').forEach(slot => {
    const view = slot.getAttribute('data-view');
    if (!view || !_LIVE_REFRESH[view]) return;
    const fn = liveRender(_LIVE_REFRESH[view]);
    slot.outerHTML = lastUpdatedBar(view, fn);
  });
}

// Pinned LIVE banner — shows any in-progress match at the very top of the
// page so the live score is impossible to miss. Auto-updates with the same
// 60s tick as the rest of the live system.
let _liveMatch = null;
// Track last seen score so we can flash the banner when it changes.
let _lastLiveScore = '';

// Live minute ticker. ESPN's `match_clock` is a static snapshot
// ("1'", "45+2'", "HT") that only refreshes on each ~5min sync. Between
// syncs the display would stay frozen — so we parse the base minute, add
// the seconds elapsed since the server snapshot, and re-tick every second.
// Re-anchors automatically on each renderLiveBanner() / renderMatchRow()
// pass (so the next sync corrects any drift within 5min).

function liveClockStr(clock, anchorMs) {
  if (!clock) return '';
  // HT / FT / ET / P (penalties) — don't tick, just show
  if (/^(HT|FT|ET|P)$/i.test(clock.trim())) return clock;
  // "45+2'" → base 45, added 2 (don't add to the added-minutes, those are stoppage)
  const m = clock.match(/^(\d+)(?:\+(\d+))?'/);
  if (!m) return clock;
  const base = parseInt(m[1], 10);
  const added = m[2] ? parseInt(m[2], 10) : 0;
  const elapsedMin = Math.floor((Date.now() - anchorMs) / 60000);
  // Show as e.g. "47'" once we cross into the added-time range; otherwise
  // just the bumped base minute with the original "+(stoppage)" preserved.
  const newBase = base + elapsedMin;
  // Hard cap at 120 — a match can't run past 120'+stoppage and we don't
  // want the ticker to drift to 999' if the user keeps the tab open.
  const capped = Math.min(newBase, 120);
  return added > 0 ? `${capped}+${added}'` : `${capped}'`;
}

function tickLiveClocks() {
  // Find every .live-clock element and update its text from its data-anchor.
  document.querySelectorAll('.live-clock').forEach(el => {
    const anchor = parseInt(el.dataset.anchor || '0', 10);
    if (!anchor) return;
    const clock = el.dataset.clock || '';
    el.textContent = liveClockStr(clock, anchor);
  });
}
setInterval(tickLiveClocks, 1000);

async function renderLiveBanner() {
  const banner = document.getElementById('live-banner');
  const content = document.getElementById('live-banner-content');
  // Always re-fetch the live list to be safe (cheap query)
  let liveMatches = [];
  try {
    liveMatches = await PM.api('/api/worldcup/matches?status=in_progress') || [];
  } catch (e) { return; }
  if (!liveMatches.length) {
    banner.style.display = 'none';
    _liveMatch = null;
    return;
  }
  _liveMatch = liveMatches[0];
  const m = _liveMatch;
  const sa = m.score_a ?? 0;
  const sb = m.score_b ?? 0;
  const clock = m.match_clock || '';
  // Anchor the ticker to NOW (when this server snapshot was rendered).
  // The next 1Hz tick will add the elapsed ms on top of the stored minute.
  const anchor = Date.now();
  // Detect score change → flash + toast
  const scoreKey = `${m.id}-${sa}-${sb}`;
  if (_lastLiveScore && _lastLiveScore !== scoreKey) {
    const [prevA, prevB] = _lastLiveScore.split('-').slice(1).map(Number);
    if (prevA !== sa || prevB !== sb) {
      const scorer = sa > prevA ? m.team_a : m.team_b;
      toast(`⚽ GOAL! ${scorer} scores! (${sa}-${sb})`, 'success');
    }
  }
  _lastLiveScore = scoreKey;
  // Build a compact row: Team A [flag] score - score [flag] Team B · minute
  content.innerHTML = `
    <div style="display:flex;align-items:center;gap:8px">
      ${flagImg(m.team_a)}
      <span style="font-weight:700;color:var(--text)">${PM.esc(m.team_a)}</span>
    </div>
    <div class="live-score-block" style="display:flex;align-items:center;gap:6px;font-size:20px;font-weight:800;color:var(--red)">
      <span class="live-score-a">${sa}</span>
      <span style="color:var(--text-dim);font-weight:400">-</span>
      <span class="live-score-b">${sb}</span>
    </div>
    <div style="display:flex;align-items:center;gap:8px">
      <span style="font-weight:700;color:var(--text)">${PM.esc(m.team_b)}</span>
      ${flagImg(m.team_b)}
    </div>
    ${clock ? `<span class="live-clock" data-clock="${PM.esc(clock)}" data-anchor="${anchor}" style="font-size:11px;color:var(--red);font-weight:700;border-left:1px solid rgba(255,82,82,0.3);padding-left:14px">${PM.esc(liveClockStr(clock, anchor))}</span>` : ''}
  `;
  banner.style.display = 'flex';
}

function openLiveMatch() {
  // Switch to Groups tab + scroll to the live match's group filter.
  if (!_liveMatch) return;
  const tab = document.querySelectorAll('.tab')[0];
  if (tab) switchView('groups', tab);
  // After render, click the matching group filter button
  setTimeout(() => {
    const g = _liveMatch.group_name;
    const btn = Array.from(document.querySelectorAll('#group-filters button'))
      .find(b => b.textContent.trim() === 'Group ' + g);
    if (btn) btn.click();
    // Then scroll to the live match card
    setTimeout(() => {
      const live = document.querySelector('.match-card.status-in-progress');
      if (live) live.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }, 100);
  }, 100);
}

// Call mountLiveBars after _LIVE_REFRESH is defined (TDZ-safe).
mountLiveBars();
renderLiveBanner(); // also render the pinned LIVE banner on init

// Quick-bet amount chips (also defined in the markets page script; mirrored
// here so the WC page bet modal can call them).
function setBetAmount(v) {
  const el = document.getElementById('bet-amount');
  if (v === 'max') el.value = PM.getUser()?.points?.toFixed(0) || 0;
  else el.value = v;
  if (typeof updateBetOdds === 'function') updateBetOdds();
}
function setKOBetAmount(v) {
  const el = document.getElementById('ko-bet-amount');
  if (v === 'max') el.value = PM.getUser()?.points?.toFixed(0) || 0;
  else el.value = v;
  if (typeof updateKOBetPreview === 'function') updateKOBetPreview();
}

// Refresh the banner every 15s (faster than the rest — score changes are
// the most time-sensitive data) regardless of which view is visible.
setInterval(renderLiveBanner, 15000);

// ── GROUPS ──
function renderGroups() {
  const grid = document.getElementById('groups-grid');
  const filters = document.getElementById('group-filters');
  filters.innerHTML = '<button class="outline" onclick="renderGroups()" style="background:var(--accent);border-color:var(--accent)">All</button>';
  Object.keys(groupsData).sort().forEach(g => {
    filters.innerHTML += `<button class="outline" onclick="renderGroup('${g}')">Group ${g}</button>`;
  });

  let html = '';
  for (const [g, data] of Object.entries(groupsData).sort()) {
    html += renderGroupCard(g, data);
  }
  grid.innerHTML = html || '<div class="empty">Loading group data...</div>';
}

function renderGroup(g) {
  const el = document.getElementById('groups-grid');
  if (!el) return;
  const data = groupsData[g];
  if (!data) return;
  el.innerHTML = renderGroupCard(g, data);

  // Update filter button states
  document.querySelectorAll('#group-filters button').forEach(b => {
    b.style.background = b.textContent === 'Group ' + g ? 'var(--accent)' : 'transparent';
    b.style.borderColor = b.textContent === 'Group ' + g ? 'var(--accent)' : 'var(--card-border)';
  });
}

function renderGroupCard(g, data) {
  // Standings for this group — always show all 4 teams
  const gs = standingsData.filter(s => s.group_name === g);
  const hasGames = gs.length > 0;

  let standingsHtml = `<table class="standings-table"><thead><tr>
    <th>Team</th><th>P</th><th>W</th><th>D</th><th>L</th><th>GF</th><th>GA</th><th>GD</th><th>Pts</th><th>Win%</th><th>Adv%</th></tr></thead><tbody>`;

  // Sort: teams with standings first (by points), then by odds
  const sortedTeams = [...data.teams].sort((a, b) => {
    const sa = gs.find(s => s.team_name === a.name);
    const sb = gs.find(s => s.team_name === b.name);
    if (sa && sb) return sb.points - sa.points || sb.goal_diff - sa.goal_diff;
    if (sa) return -1;
    if (sb) return 1;
    return b.win_group_pct - a.win_group_pct;
  });

  sortedTeams.forEach(t => {
    const s = gs.find(x => x.team_name === t.name);
    const wc = t.win_group_pct > 40 ? 'odds-high' : t.win_group_pct > 15 ? 'odds-mid' : 'odds-low';
    const ac = t.advance_pct > 70 ? 'odds-high' : t.advance_pct > 30 ? 'odds-mid' : 'odds-low';

    standingsHtml += `<tr>
      <td>${flagImg(t.name)}${esc(t.name)}</td>
      <td>${s ? s.played : 0}</td><td>${s ? s.wins : 0}</td><td>${s ? s.draws : 0}</td><td>${s ? s.losses : 0}</td>
      <td>${s ? s.goals_for : 0}</td><td>${s ? s.goals_against : 0}</td><td>${s ? (s.goal_diff > 0 ? '+' : '') + s.goal_diff : 0}</td>
      <td class="standings-pts">${s ? s.points : 0}</td>
      <td><span class="odds-val ${wc}">${t.win_group_pct}%</span></td>
      <td><span class="odds-val ${ac}">${t.advance_pct}%</span></td>
    </tr>`;
  });
  standingsHtml += '</tbody></table>';

  // Show ALL matches — sorted by date
  const groupMatches = matchesData.filter(m => m.group_name === g);
  groupMatches.sort((a, b) => {
    if (a.start_date && b.start_date) return a.start_date.localeCompare(b.start_date);
    if (a.start_date) return -1;
    if (b.start_date) return 1;
    return 0;
  });
  let matchHtml = '';
  if (groupMatches.length > 0) {
    matchHtml = '<div style="padding:8px 12px;background:rgba(0,0,0,0.2);border-top:1px solid var(--card-border)">' +
      groupMatches.map(m => renderMatchRow(m)).join('') + '</div>';
  }

  return `<div class="group-card">
    <div class="group-header">🏆 Group ${g}</div>
    ${standingsHtml}
    ${matchHtml}
  </div>`;
}

function renderMatchRow(m) {
  const isLive = m.status === 'in_progress';
  const sc = m.status === 'completed' ? 'status-completed'
           : isLive ? 'status-in-progress'
           : 'status-scheduled';
  let scoreHtml = '';
  let actions = '';
  let dateHtml = '';

  // Show real date + countdown (UTC+7 / Ho Chi Minh) for NOT-YET-STARTED matches.
  // In-progress and completed matches don't need a countdown.
  if (m.start_date && !isLive && m.status !== 'completed') {
    dateHtml = `<span style="font-size:10px;color:var(--text-dim);min-width:160px">${toCountdown(m.start_date)}</span>`;
  } else if (isLive) {
    // In-progress: show pulsing LIVE + minute counter (from ESPN clock if present)
    const clock = m.match_clock || '';
    // Anchor the ticker to render time. The match row is re-rendered on each
    // live refresh (15s) so the anchor stays close to the server snapshot.
    const _anchor = Date.now();
    const liveStr = clock ? liveClockStr(clock, _anchor) : '';
    dateHtml = `<span style="font-size:10px;color:var(--red);min-width:160px;font-weight:700"><span class="live-pulse"></span>LIVE${liveStr ? ` · <span class="live-clock" data-clock="${PM.esc(clock)}" data-anchor="${_anchor}">${PM.esc(liveStr)}</span>` : ''}</span>`;
  }

  // Show Polymarket-derived odds if available (only for upcoming matches).
  // Rounded to whole percent for readability.
  let oddsHtml = '';
  if (m.outcome_prices && m.status === 'scheduled') {
    try {
      const op = JSON.parse(m.outcome_prices);
      if (op.moneyline_home != null && op.moneyline_away != null) {
        // Polymarket-style: 1-decimal-place probability on each YES. The
        // implied NO price is just 1 - YES.
        const h_pct = (op.moneyline_home * 100).toFixed(1);
        const d_pct = op.draw != null ? (op.draw * 100).toFixed(1) : null;
        const awayRaw = op.draw != null ? (op.moneyline_away - op.draw) : op.moneyline_away;
        const a_pct = Math.max(0, awayRaw * 100).toFixed(1);
        const src = op._source || '?';
        const srcLabel = src === 'polymarket_per_match' ? 'via Polymarket (live order book)' :
                         src === 'polymarket_group_odds_derived' ? 'derived from group odds (fallback)' : src;
        oddsHtml = `<span style="font-size:10px;color:var(--gold);min-width:110px;text-align:right" title="Source: ${esc(srcLabel)}">${h_pct} / ${d_pct != null ? d_pct : '—'} / ${a_pct}%</span>`;
      }
    } catch(e) {}
  }

  if (m.status === 'completed') {
    scoreHtml = `<span class="match-score">${m.score_a ?? '-'} - ${m.score_b ?? '-'}</span>`;
    const w = m.winner === 'team_a' ? m.team_a : m.winner === 'team_b' ? m.team_b : 'Draw';
    scoreHtml += `<span style="font-size:10px;color:var(--gold)">${w}</span>`;
  } else if (isLive) {
    // In-progress: show current score prominently, flash on change
    const sa = m.score_a ?? 0;
    const sb = m.score_b ?? 0;
    // Score-change detection: compare to last-seen score for this match
    const matchScoreKey = `mc-${m.id}-${sa}-${sb}`;
    const prevKey = window._lastMatchScores?.[m.id];
    const scoreChanged = prevKey && prevKey !== matchScoreKey;
    window._lastMatchScores = window._lastMatchScores || {};
    window._lastMatchScores[m.id] = matchScoreKey;
    const flashClass = scoreChanged ? ' score-flash' : '';
    if (scoreChanged) console.log('FLASH m='+m.id, prevKey, '→', matchScoreKey);
    scoreHtml = `<span class="match-score${flashClass}" style="color:var(--red);font-weight:800">${sa} - ${sb}</span>`;
  } else if (m.outcome_prices) {
    actions = `<button class="sm" onclick="openMatchBet(${m.id})">Bet</button>`;
  } else {
    actions = `<span style="font-size:10px;color:var(--text-dim)">No odds</span>`;
  }
  return `<div class="match-card ${sc}">
    <div class="match-teams">${flagImg(m.team_a)}${esc(m.team_a)} <span class="vs">vs</span> ${flagImg(m.team_b)}${esc(m.team_b)}</div>
    ${scoreHtml}
    ${dateHtml}
    ${oddsHtml}
    <div style="display:flex;gap:4px">${actions}</div>
  </div>`;
}

// ── MY BETS (active) ──
function toCountdown(utcStr) {
  // Returns "Fri, Jun 19 08:00 (5d 18h)" — date + time, plus a parenthesized
  // countdown. "🔴 LIVE" while the match is in progress.
  if (!utcStr) return '—';
  const h = toHCMC(utcStr);
  const diffMs = new Date(utcStr).getTime() - Date.now();
  if (diffMs < 0 && diffMs > -3*3600000) return '<span class="live-pulse"></span>LIVE';
  if (diffMs <= 0) return h.date + ' ' + h.time;
  const totalMin = Math.floor(diffMs / 60000);
  const totalHrs = Math.floor(diffMs / 3600000);
  const days = Math.floor(totalHrs / 24);
  const hrs = totalHrs % 24;
  const mins = totalMin % 60;
  let cd = '';
  if (days > 0) cd = `${days}d ${hrs}h`;
  else if (totalHrs > 0) cd = mins > 0 ? `${totalHrs}h ${mins}m` : `${totalHrs}h`;
  else cd = `${totalMin}m`;
  return cd;
}

async function renderMyBets() {
  const el = document.getElementById('mybets-content');
  const cnt = document.getElementById('mybets-count');
  if (!el) return;
  if (!PM.getUser()) {
    el.innerHTML = '<div class="empty">Login to see your active bets</div>';
    cnt.textContent = '';
    return;
  }
  try {
    // Ensure matchesData is loaded so we can show live scores on bet cards.
    if (!matchesData || !matchesData.length) {
      try { matchesData = await PM.api('/api/worldcup/matches'); } catch (e) {}
    }
    // Fetch stats + active bets in parallel.
    const [data, stats] = await Promise.all([
      PM.api(`/api/users/${PM.getUser().id}/active-bets`),
      PM.api(`/api/users/${PM.getUser().id}/stats`),
    ]);

    // Settlement toast: compare with last-known state, toast for any newly-settled bets.
    const lastKnownRaw = localStorage.getItem('pm-known-settled');
    const lastKnown = lastKnownRaw ? new Set(JSON.parse(lastKnownRaw)) : new Set();
    if (lastKnownRaw) {
      // Pull from history — settled bets since last visit show toasts.
      try {
        const hist = await PM.api(`/api/users/${PM.getUser().id}/history?limit=50`);
        for (const b of [...(hist.match_bets||[]), ...(hist.knockout_bets||[])]) {
          if (b.won != null && !lastKnown.has(b.id)) {
            if (b.won === 1) {
              toast(`🎉 Won +${(b.payout||0).toFixed(0)} pts on ${b.match_label || b.label}`, 'success');
            } else {
              toast(`❌ Lost -${b.amount.toFixed(0)} pts on ${b.match_label || b.label}`, 'error');
            }
          }
        }
        // Update known set with all currently-settled bet IDs.
        const allSettled = new Set([
          ...(hist.match_bets||[]).filter(b => b.won != null).map(b => b.id),
          ...(hist.knockout_bets||[]).filter(b => b.won != null).map(b => b.id),
        ]);
        localStorage.setItem('pm-known-settled', JSON.stringify([...allSettled]));
      } catch (e) { /* ignore */ }
    } else {
      // First visit — seed the known set without toasting.
      try {
        const hist = await PM.api(`/api/users/${PM.getUser().id}/history?limit=50`);
        const allSettled = new Set([
          ...(hist.match_bets||[]).filter(b => b.won != null).map(b => b.id),
          ...(hist.knockout_bets||[]).filter(b => b.won != null).map(b => b.id),
        ]);
        localStorage.setItem('pm-known-settled', JSON.stringify([...allSettled]));
      } catch (e) { /* ignore */ }
    }
    cnt.textContent = `${data.totals.count} active · ${data.totals.wagered.toFixed(0)} pts at risk · potential ${data.totals.potential.toFixed(0)} pts`;

    // Stats card at top
    const pnl = stats.net_pnl || 0;
    const pnlColor = pnl > 0 ? 'var(--green)' : (pnl < 0 ? 'var(--red)' : 'var(--text)');
    const wr = (stats.win_rate * 100).toFixed(0);
    const wrColor = wr >= 50 ? 'var(--green)' : 'var(--text-dim)';
    let html = `
      <div class="stats-grid" style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:18px">
        <div class="stat-card"><div class="stat-label">Balance</div><div class="stat-value">${stats.points.toFixed(0)}<span class="stat-unit">pts</span></div></div>
        <div class="stat-card"><div class="stat-label">Net P&L</div><div class="stat-value" style="color:${pnlColor}">${pnl > 0 ? '+' : ''}${pnl.toFixed(0)}<span class="stat-unit">pts</span></div></div>
        <div class="stat-card"><div class="stat-label">Win Rate</div><div class="stat-value" style="color:${wrColor}">${wr}<span class="stat-unit">%</span></div><div style="font-size:10px;color:var(--text-dim)">${stats.wins}W / ${(stats.total_bets - stats.wins - (stats.matches?.pending||0) - (stats.knockouts?.pending||0) - (stats.markets?.pending||0))}L</div></div>
        <div class="stat-card"><div class="stat-label">Wagered</div><div class="stat-value">${stats.total_wagered.toFixed(0)}<span class="stat-unit">pts</span></div></div>
      </div>
    `;

    if (data.totals.count === 0) {
      html += '<div class="empty">No active bets. Place one from a Group or Knockout match.</div>';
    } else {
      if (data.match_bets.length) {
        html += '<h4 class="section-h">Group Stage</h4>';
        html += data.match_bets.map(b => {
          // Look up the current match in matchesData to show live score.
          const m = (matchesData || []).find(x => x.id === b.match_id);
          const scoreChip = m && m.status === 'in_progress' && m.score_a != null
            ? `<span style="background:var(--red);color:#000;padding:2px 8px;border-radius:4px;font-weight:800;font-size:13px">${m.score_a}-${m.score_b}${m.match_clock ? ' ' + PM.esc(m.match_clock) : ''}</span>`
            : '';
          return `<div class="match-card bet-card group-bet">
            <div class="match-teams" style="flex:1;min-width:0">
              <b style="color:var(--accent)">${PM.esc(b.pick_label)}</b> · ${PM.esc(b.label)}
              ${scoreChip ? `<div style="margin-top:4px">${scoreChip}</div>` : ''}
            </div>
            <div style="font-size:11px;color:var(--text-dim);text-align:right;min-width:130px;flex-shrink:0">
              ${m && m.status === 'in_progress' ? '<span style="color:var(--red);font-weight:700">LIVE NOW</span>' : toCountdown(b.start_date)}<br>
              <span style="color:var(--gold)">${b.amount} pts @ ${Math.round(b.odds*100)}%</span>
              <br><span style="color:var(--green)">→ ${b.potential_payout.toFixed(0)} pts</span>
            </div>
          </div>`;
        }).join('');
      }

      if (data.knockout_bets.length) {
        html += '<h4 class="section-h">Knockout</h4>';
        html += data.knockout_bets.map(b => {
          const km = (knockoutData || []).find(x => x.id === b.match_id);
          return `<div class="match-card bet-card ko-bet">
            <div class="match-teams" style="flex:1;min-width:0">
              <b style="color:var(--gold)">${PM.esc(b.pick_label)}</b> · ${PM.esc(b.label)}
            </div>
            <div style="font-size:11px;color:var(--text-dim);text-align:right;min-width:130px;flex-shrink:0">
              ${b.start_date ? toCountdown(b.start_date) : 'TBD'}<br>
              <span style="color:var(--gold)">${b.amount} pts @ ${Math.round(b.odds*100)}%</span>
              <br><span style="color:var(--green)">→ ${b.potential_payout.toFixed(0)} pts</span>
            </div>
          </div>`;
        }).join('');
      }
    }
    el.innerHTML = html;
  } catch (e) {
    el.innerHTML = '<div class="empty">Error: ' + PM.esc(e.message) + '</div>';
  }
}

// ── KNOCKOUT ──
const ROUND_NAMES = {'R32':'Round of 32','R16':'Round of 16','QF':'Quarter-finals','SF':'Semi-finals','3rd':'3rd Place','Final':'🏆 Final'};
const ROUND_ORDER = ['R32','R16','QF','SF','3rd','Final'];

function projectKnockout() {
  // Project teams into knockout slots based on advance probabilities + OKX data
  const projected = [];
  for (const [g, data] of Object.entries(groupsData)) {
    // Sort by OKX win probability if available, else advance %
    const teams = [...data.teams].sort((a, b) => {
      const aOkx = OKX_TEAMS[a.name] || 0;
      const bOkx = OKX_TEAMS[b.name] || 0;
      if (aOkx > 0 || bOkx > 0) return bOkx - aOkx;
      return b.advance_pct - a.advance_pct;
    });
    projected.push({ group: g, winner: teams[0], runner_up: teams[1], third: teams[2] });
  }
  projected.sort((a, b) => a.group.localeCompare(b.group));
  return projected;
}

function renderKnockout() {
  const el = document.getElementById('knockout-content');
  if (!el) return;
  let html = '';

  for (const round of ROUND_ORDER) {
    const matches = knockoutData.filter(m => m.round === round);
    if (!matches.length) continue;
    html += `<div class="bracket-round"><h3>${ROUND_NAMES[round]}</h3><div class="bracket-grid">`;

    matches.forEach(m => {
      let teamA = m.team_a;
      let teamB = m.team_b;

      const cls = m.status === 'completed' ? '' : (teamA && teamB ? 'live' : '');
      const canBet = m.status === 'pending' && teamA && teamB && PM.getUser();
      const betBtn = canBet
        ? `<button class="sm" style="margin-left:6px" onclick="openKOBet(${m.id})">Bet</button>`
        : '';
      html += `<div class="knockout-card ${cls}">
        <div class="knockout-slot">${m.slot}</div>
        <div class="knockout-teams">
          <div class="knockout-team${teamA ? '' : ' tbd'}">${flagImg(teamA || '')}${teamA || 'TBD'}</div>
          <div class="knockout-team${teamB ? '' : ' tbd'}">${flagImg(teamB || '')}${teamB || 'TBD'}</div>
        </div>
        <div style="display:flex;flex-direction:column;align-items:flex-end;gap:4px">
          ${betBtn}
        </div>`;
      if (m.status === 'completed') {
        const scoreStr = (m.score_a != null && m.score_b != null) ? `${m.score_a}-${m.score_b}` : '';
        html += `<div class="knockout-winner" style="text-align:right"><div>${scoreStr}</div><span>${m.winner}</span></div>`;
      }
      html += '</div>';
    });
    html += '</div></div>';
  }
  el.innerHTML = html || '<div class="empty">No knockout matches</div>';
}

// ── KO bet modal (reuses bet-modal but with a 2-pick layout: team_a / team_b only) ──
let koMatchId = null;

function openKOBet(matchId) {
  if (!PM.getUser()) return toast('Please log in first', 'error');
  const m = knockoutData.find(x => x.id === matchId);
  if (!m) return;
  if (!m.team_a || !m.team_b) return toast('Both teams must be set', 'error');
  if (m.status !== 'pending') return toast('Match not pending', 'error');
  koMatchId = matchId;
  document.getElementById('ko-bet-info').textContent = `${m.round} · ${m.team_a} vs ${m.team_b}`;
  // Set team names on the pick cards
  document.getElementById('ko-pick-team_a-name').textContent = m.team_a;
  document.getElementById('ko-pick-team_b-name').textContent = m.team_b;
  // Default pick = team_a (highlighted) and side = yes (highlighted)
  selectKOPickByValue('team_a');
  selectKOBetSide('yes');
  document.getElementById('ko-bet-amount').value = '';
  document.getElementById('ko-bet-balance').textContent = PM.getUser().points.toFixed(0);
  // Reset odds display
  if (m.outcome_prices) {
    try {
      const op = JSON.parse(m.outcome_prices);
      const h = op.moneyline_home || 0;
      const a = op.moneyline_away || 0;
      // Populate pick-card percentages (1-decimal precision)
      const setPct = (elId, p) => {
        const el = document.getElementById(elId);
        if (el) el.textContent = (p * 100).toFixed(1) + '%';
      };
      setPct('ko-pick-team_a-pct', h);
      setPct('ko-pick-team_b-pct', a);
      // Stash odds for submission
      window._koOdds = { team_a: h || 0.5, team_b: a || 0.5 };
    } catch (_) { window._koOdds = { team_a: 0.5, team_b: 0.5 }; }
  } else {
    ['ko-pick-team_a-pct', 'ko-pick-team_b-pct'].forEach(elId => {
      const el = document.getElementById(elId);
      if (el) el.textContent = '—';
    });
    window._koOdds = { team_a: 0.5, team_b: 0.5 };
  }
  updateKOBetSummary();
  document.getElementById('ko-bet-modal').classList.add('show');
}

function closeKOBet() { document.getElementById('ko-bet-modal').classList.remove('show'); }

// KO pick card selection. Highlights the chosen card, un-highlights others.
function selectKOPick(btn) {
  const value = btn.getAttribute('data-pick');
  selectKOPickByValue(value);
}
function selectKOPickByValue(value) {
  document.getElementById('ko-bet-pick').value = value;
  document.querySelectorAll('#ko-bet-modal .pick-card').forEach(c => {
    if (c.getAttribute('data-pick') === value) {
      c.style.background = 'rgba(122,162,247,0.15)';
      c.style.borderColor = 'var(--accent)';
    } else {
      c.style.background = 'transparent';
      c.style.borderColor = 'var(--card-border)';
    }
  });
  updateKOBetSummary();
}

// KO YES/NO side toggle. Default YES. NO = bet against this team winning.
function selectKOBetSide(side) {
  document.getElementById('ko-bet-pick').dataset.side = side;
  const yesBtn = document.getElementById('ko-bet-side-yes');
  const noBtn  = document.getElementById('ko-bet-side-no');
  if (side === 'yes') {
    yesBtn.style.background = 'rgba(0,230,118,0.15)';
    yesBtn.style.borderColor = 'var(--green)';
    noBtn.style.background = 'transparent';
    noBtn.style.borderColor = 'var(--card-border)';
  } else {
    noBtn.style.background = 'rgba(255,82,82,0.15)';
    noBtn.style.borderColor = 'var(--red)';
    yesBtn.style.background = 'transparent';
    yesBtn.style.borderColor = 'var(--card-border)';
  }
  updateKOBetSummary();
}

// Re-render the bottom summary line in the KO bet modal.
function updateKOBetSummary() {
  const el = document.getElementById('ko-bet-summary');
  if (!el) return;
  const pick = document.getElementById('ko-bet-pick').value;
  const side = document.getElementById('ko-bet-pick').dataset.side || 'yes';
  const amount = parseFloat(document.getElementById('ko-bet-amount').value) || 0;
  const yesProb = (window._koOdds || {})[pick] || 0.5;
  // KO bet model: `amount` is the WAGER. Payout = wager / odds.
  let payout = 0, profit = 0;
  if (amount > 0) {
    if (side === 'yes') {
      payout = yesProb > 0 ? amount / yesProb : 0;
    } else {
      const noProb = 1 - yesProb;
      payout = noProb > 0 ? amount / noProb : 0;
    }
    profit = payout - amount;
  }
  const teamLabel = pick === 'team_a' ? (document.getElementById('ko-pick-team_a-name').textContent || 'Home')
                  : (document.getElementById('ko-pick-team_b-name').textContent || 'Away');
  const sideLabel = side === 'yes' ? 'YES' : 'NO';
  const sideColor = side === 'yes' ? 'var(--green)' : 'var(--red)';
  const yesProbPct = (yesProb * 100).toFixed(1);
  const noProbPct = ((1 - yesProb) * 100).toFixed(1);
  if (amount <= 0) {
    el.innerHTML = `<span style="color:var(--text-dim)">Pick a team, choose YES/NO, set amount, then Place Bet.</span>`;
  } else {
    el.innerHTML = `
      <div>You bet <span style="color:${sideColor};font-weight:700">${sideLabel}</span> on
           <span style="font-weight:700">${teamLabel}</span> for
           <span style="color:var(--gold);font-weight:700">${amount.toFixed(0)} pts</span></div>
      <div style="font-size:11px;color:var(--text-dim);margin-top:3px">
        ${side === 'yes'
          ? `Pays back <b style="color:var(--green)">${payout.toFixed(0)} pts</b> if ${teamLabel} wins (YES ${yesProbPct}% chance) — profit <b style="color:var(--green)">${profit.toFixed(0)} pts</b>`
          : `Pays back <b style="color:var(--green)">${payout.toFixed(0)} pts</b> if ${teamLabel} doesn't win (NO ${noProbPct}% chance) — profit <b style="color:var(--green)">${profit.toFixed(0)} pts</b>`}
      </div>
    `;
  }
}

// Place the KO bet. Reads pick + side + amount (wager), sends to backend.
async function submitKOBet() {
  if (!PM.getUser()) return toast('Please log in first', 'error');
  const amount = parseFloat(document.getElementById('ko-bet-amount').value);
  if (!amount || amount < 1) return toast('Enter a valid amount (min 1)', 'error');
  const pick = document.getElementById('ko-bet-pick').value;
  const side = document.getElementById('ko-bet-pick').dataset.side || 'yes';
  if (!['team_a','team_b'].includes(pick)) return toast('Pick a team', 'error');
  if (amount > PM.getUser().points) {
    return toast(`Insufficient points (have ${PM.getUser().points.toFixed(0)}, bet ${amount.toFixed(0)})`, 'error');
  }
  try {
    const r = await PM.api('/api/worldcup/knockout/bet', {
      method: 'POST',
      body: JSON.stringify({ match_id: koMatchId, pick, side, amount }),
    });
    const teamLabel = pick === 'team_a' ? (document.getElementById('ko-pick-team_a-name').textContent || 'Home')
                    : (document.getElementById('ko-pick-team_b-name').textContent || 'Away');
    const verb = side === 'yes' ? 'YES' : 'NO';
    const payout = r.payout_if_win != null ? r.payout_if_win.toFixed(0) : '?';
    const profit = r.payout_if_win != null ? (r.payout_if_win - amount).toFixed(0) : '?';
    toast(`✅ KO ${verb} on ${teamLabel} — wagered ${amount.toFixed(0)} pts · pays back ${payout} pts if hit (+${profit} profit)`, 'success');
    PM.getUser().points = r.user.points;
    document.getElementById('ko-bet-balance').textContent = PM.getUser().points.toFixed(0);
    updateKOBetSummary();
  } catch (e) {
    toast(e.message || 'KO bet failed', 'error');
  }
}

// Backward-compat shim: updateKOBetPreview used to do the per-button wager
// preview; now it just refreshes the new summary block.
function updateKOBetPreview() {
  return projected;
}

async function knockoutSetTeam(matchId, side) {
  const team = prompt('Enter team name for ' + side + ':');
  if (!team) return;
  try {
    await PM.api('/api/worldcup/knockout/set-team', {
      method: 'POST',
      body: JSON.stringify({match_id: matchId, side: side, team: team}),
    });
  } catch (e) { toast(e.message, 'error'); return; }
  await loadAll();
}

async function knockoutResolve(matchId) {
  const m = knockoutData.find(x => x.id === matchId);
  if (!m) return;
  const w = confirm(m.team_a + ' wins? (OK=' + m.team_a + ', Cancel=' + m.team_b + ')');
  const winner = w ? 'team_a' : 'team_b';
  await fetch('/api/worldcup/knockout/resolve', {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify({match_id: matchId, winner: winner}),
  });
  await loadAll();
  toast('Knockout resolved! Winner advanced.', 'success');
}

// ── BETTING ──
async function openMatchBet(matchId) {
  if (!PM.getUser()) return toast('Please log in first', 'error');
  currentMatchId = matchId;
  const m = matchesData.find(x => x.id === matchId);
  if (!m) return;
  document.getElementById('bet-match-info').textContent = `Group ${m.group_name} · ${m.team_a} vs ${m.team_b}`;
  // Set team names on the 3 pick cards
  document.getElementById('pick-team_a-name').textContent = m.team_a;
  document.getElementById('pick-team_b-name').textContent = m.team_b;
  // Default pick = team_a (highlighted) and side = yes (highlighted)
  selectPickByValue('team_a');
  selectBetSide('yes');
  document.getElementById('bet-amount').value = '';
  document.getElementById('bet-balance').textContent = PM.getUser().points.toFixed(0);
  document.getElementById('bet-modal').classList.add('show');

  // Use Polymarket per-match odds if available
  if (m.outcome_prices) {
    try {
      const op = JSON.parse(m.outcome_prices);
      if (op.moneyline_home != null && op.moneyline_away != null) {
        const home = op.moneyline_home;
        const draw = op.draw != null ? op.draw : 0.15;
        const away = op.draw != null ? (op.moneyline_away - op.draw) : op.moneyline_away;
        currentMatchOdds = { team_a: Math.max(0.01, home), draw: Math.max(0.01, draw), team_b: Math.max(0.01, away) };
        // Populate pick-card percentages (1-decimal precision)
        const setPct = (elId, p) => {
          const el = document.getElementById(elId);
          if (el) el.textContent = (p * 100).toFixed(1) + '%';
        };
        setPct('pick-team_a-pct', home);
        setPct('pick-draw-pct',   draw);
        setPct('pick-team_b-pct', away);
        updateBetSummary();
        return;
      }
    } catch(e) {}
  }

  // No Polymarket odds — show empty
  ['pick-team_a-pct', 'pick-draw-pct', 'pick-team_b-pct'].forEach(elId => {
    const el = document.getElementById(elId);
    if (el) el.textContent = '—';
  });
  updateBetSummary();
}

// Programmatic pick selection (used by openMatchBet to set the default
// pick without needing a click). Just updates the highlighted card.
function selectPickByValue(value) {
  document.getElementById('bet-pick').value = value;
  document.querySelectorAll('#bet-modal .pick-card').forEach(c => {
    if (c.getAttribute('data-pick') === value) {
      c.style.background = 'rgba(122,162,247,0.15)';
      c.style.borderColor = 'var(--accent)';
    } else {
      c.style.background = 'transparent';
      c.style.borderColor = 'var(--card-border)';
    }
  });
}

// 3-pick card selection. Highlights the chosen card, un-highlights others.
// The first card (team_a / "Home") starts selected so the user always has
// a valid pick. Updates the bet summary preview.
function selectPick(btn) {
  const value = btn.getAttribute('data-pick');
  document.getElementById('bet-pick').value = value;
  document.querySelectorAll('#bet-modal .pick-card').forEach(c => {
    if (c.getAttribute('data-pick') === value) {
      c.style.background = 'rgba(122,162,247,0.15)';
      c.style.borderColor = 'var(--accent)';
    } else {
      c.style.background = 'transparent';
      c.style.borderColor = 'var(--card-border)';
    }
  });
  updateBetSummary();
}

// YES/NO side toggle. Default is YES (highlighted green). NO means "bet
// against the picked outcome" (i.e., the picked outcome does NOT happen).
function selectBetSide(side) {
  document.getElementById('bet-pick').dataset.side = side;
  const yesBtn = document.getElementById('bet-side-yes');
  const noBtn  = document.getElementById('bet-side-no');
  if (side === 'yes') {
    yesBtn.style.background = 'rgba(0,230,118,0.15)';
    yesBtn.style.borderColor = 'var(--green)';
    noBtn.style.background = 'transparent';
    noBtn.style.borderColor = 'var(--card-border)';
  } else {
    noBtn.style.background = 'rgba(255,82,82,0.15)';
    noBtn.style.borderColor = 'var(--red)';
    yesBtn.style.background = 'transparent';
    yesBtn.style.borderColor = 'var(--card-border)';
  }
  updateBetSummary();
}

// Re-render the bottom summary line ("YOU BET X YES/NO on Home Win for
// Y pts, pays back Z pts (profit W pts) if Home Win") whenever the user
// changes the pick, side, or amount.
function updateBetSummary() {
  const el = document.getElementById('bet-summary');
  if (!el) return;
  const pick = document.getElementById('bet-pick').value;
  const side = document.getElementById('bet-pick').dataset.side || 'yes';
  const amount = parseFloat(document.getElementById('bet-amount').value) || 0;
  const yesProb = (currentMatchOdds || {})[pick] || 0.5;
  // New model: `amount` is the WAGER. Payout = wager / odds.
  // For YES at probability p, payout = wager / p (heavier favorite → smaller payout)
  // For NO  at (1-p),        payout = wager / (1-p) (heavier favorite → bigger payout)
  let payout = 0, profit = 0;
  if (amount > 0) {
    if (side === 'yes') {
      payout = yesProb > 0 ? amount / yesProb : 0;
    } else {
      const noProb = 1 - yesProb;
      payout = noProb > 0 ? amount / noProb : 0;
    }
    profit = payout - amount;
  }
  const teamLabel = pick === 'team_a' ? (document.getElementById('pick-team_a-name').textContent || 'Home')
                  : pick === 'team_b' ? (document.getElementById('pick-team_b-name').textContent || 'Away')
                  : 'Draw';
  const sideLabel = side === 'yes' ? 'YES' : 'NO';
  const sideColor = side === 'yes' ? 'var(--green)' : 'var(--red)';
  const yesProbPct = (yesProb * 100).toFixed(1);
  const noProbPct = ((1 - yesProb) * 100).toFixed(1);
  if (amount <= 0) {
    el.innerHTML = `<span style="color:var(--text-dim)">Pick a market, choose YES/NO, set amount, then Place Bet.</span>`;
  } else {
    el.innerHTML = `
      <div>You bet <span style="color:${sideColor};font-weight:700">${sideLabel}</span> on
           <span style="font-weight:700">${teamLabel}</span> for
           <span style="color:var(--gold);font-weight:700">${amount.toFixed(0)} pts</span></div>
      <div style="font-size:11px;color:var(--text-dim);margin-top:3px">
        ${side === 'yes'
          ? `Pays back <b style="color:var(--green)">${payout.toFixed(0)} pts</b> if ${teamLabel} (YES ${yesProbPct}% chance) — profit <b style="color:var(--green)">${profit.toFixed(0)} pts</b>`
          : `Pays back <b style="color:var(--green)">${payout.toFixed(0)} pts</b> if NOT ${teamLabel} (NO ${noProbPct}% chance) — profit <b style="color:var(--green)">${profit.toFixed(0)} pts</b>`}
      </div>
    `;
  }
}

// Place the bet. Reads pick (Home/Draw/Away) + side (YES/NO default) +
// amount from the modal, sends to the backend, updates the UI.
async function submitMatchBet() {
  if (!PM.getUser()) return toast('Please log in first', 'error');
  // New model: `amount` is the WAGER (what the user pays upfront). The
  // backend stores amount = wager, computes payout = wager / odds at
  // settlement time, and credits the user the full payout if they win.
  const amount = parseFloat(document.getElementById('bet-amount').value);
  if (!amount || amount < 1) return toast('Enter a valid amount (min 1)', 'error');
  const pick = document.getElementById('bet-pick').value;
  const side = document.getElementById('bet-pick').dataset.side || 'yes';
  if (!['team_a','draw','team_b'].includes(pick)) return toast('Pick a market', 'error');
  if (amount > PM.getUser().points) {
    return toast(`Insufficient points (have ${PM.getUser().points.toFixed(0)}, bet ${amount.toFixed(0)})`, 'error');
  }
  try {
    const r = await PM.api('/api/worldcup/bet', {
      method: 'POST',
      body: JSON.stringify({ match_id: currentMatchId, pick, side, amount }),
    });
    const teamLabel = pick === 'team_a' ? (document.getElementById('pick-team_a-name').textContent || 'Home')
                    : pick === 'team_b' ? (document.getElementById('pick-team_b-name').textContent || 'Away')
                    : 'Draw';
    const verb = side === 'yes' ? 'YES' : 'NO';
    const payout = r.payout_if_win != null ? r.payout_if_win.toFixed(0) : '?';
    const profit = r.payout_if_win != null ? (r.payout_if_win - amount).toFixed(0) : '?';
    toast(`✅ ${verb} on ${teamLabel} — wagered ${amount.toFixed(0)} pts · pays back ${payout} pts if hit (+${profit} profit)`, 'success');
    PM.getUser().points = r.user.points;
    document.getElementById('bet-balance').textContent = PM.getUser().points.toFixed(0);
    updateBetSummary();
    if (typeof renderMyBets === 'function') renderMyBets();
  } catch (e) {
    toast(e.message || 'Bet failed', 'error');
  }
}

// ── RESOLVE ──
function autoPickWinner() {
  const sa = parseInt(document.getElementById('res-score-a').value) || 0;
  const sb = parseInt(document.getElementById('res-score-b').value) || 0;
  const sel = document.getElementById('resolve-winner');
  if (sa > sb) sel.value = 'team_a';
  else if (sb > sa) sel.value = 'team_b';
  return projected;
}

// ============================================================
// Per-round ADVANCE MARKETS (Polymarket). One per (team, round).
// Renders the "Advance" tab: 48 teams × 4 rounds = 192 markets.
// ============================================================
let _advanceData = null;
async function renderAdvance() {
  console.log('[renderAdvance] called, grid=', !!document.getElementById('advance-grid'),
              'view-advance display=', document.getElementById('view-advance')?.style.display,
              'cached _advanceData=', !!_advanceData);
  const grid = document.getElementById('advance-grid');
  if (!grid) { console.warn('[renderAdvance] no #advance-grid — view not mounted?'); return; }
  if (!_advanceData) {
    try {
      console.log('[renderAdvance] fetching /api/worldcup/advance-markets');
      const ctrl = new AbortController();
      const tid = setTimeout(() => ctrl.abort(), 8000);
      const r = await fetch('/api/worldcup/advance-markets', { signal: ctrl.signal });
      clearTimeout(tid);
      console.log('[renderAdvance] fetch status:', r.status, 'ok=', r.ok);
      if (!r.ok) { grid.innerHTML = `<div style="color:var(--red);padding:20px">⚠️ Server returned ${r.status}</div>`; return; }
      _advanceData = await r.json();
      console.log('[renderAdvance] got', Object.keys(_advanceData).length, 'teams');
    } catch (e) {
      console.error('[renderAdvance] fetch failed:', e);
      grid.innerHTML = `<div style="color:var(--red);padding:20px">⚠️ Failed to load: ${esc(String(e))}</div>`;
      return;
    }
  }
  // Category label map
  const catLabels = {
    advance_r32:   'R32 (advance from group)',
    advance_r16:   'QF (advance past R32)',
    advance_qf:    'SF (advance past QF)',
    advance_sf:    'Final (advance past SF)',
    advance_final: '🏆 Champion (win the tournament)',
  };
  // Sort teams by their highest advance probability (favorites first)
  const teams = Object.entries(_advanceData).sort((a, b) => {
    const aMax = Math.max(...a[1].map(m => m.yes_price));
    const bMax = Math.max(...b[1].map(m => m.yes_price));
    return bMax - aMax;
  });
  // Stats summary
  const total = teams.reduce((s, [, ms]) => s + ms.length, 0);
  grid.innerHTML = `
    <div style="background:rgba(255,255,255,0.04);border:1px solid var(--card-border);border-radius:8px;padding:12px 16px;margin-bottom:12px;font-size:13px">
      <b style="color:var(--gold)">🚀 Per-round advance markets</b> —
      pulled from Polymarket's order book. Each market is YES/NO:
      bet YES if the team advances that far, NO if they don't.
      <span style="color:var(--text-dim)">· ${teams.length} teams · ${total} markets</span>
    </div>
  `;
  teams.forEach(([team, markets]) => {
    const card = document.createElement('div');
    card.className = 'advance-card';
    card.style.cssText = 'background:rgba(255,255,255,0.04);border:1px solid var(--card-border);border-radius:8px;padding:12px;margin-bottom:10px';
    const teamNameEsc = esc(team);
    const rows = markets
      .sort((a, b) => catLabels[a.category].localeCompare(catLabels[b.category]))
      .map(m => {
        const yesProb = (m.yes_price * 100).toFixed(1);
        const noProb = (m.no_price * 100).toFixed(1);
        return `
          <div class="adv-row" data-market-id="${m.id}" data-team="${teamNameEsc}" data-category="${m.category}"
               style="display:flex;align-items:center;gap:8px;padding:6px 4px;border-bottom:1px solid rgba(255,255,255,0.04)">
            <div style="flex:1;font-size:12px;color:var(--text-dim)">${catLabels[m.category] || m.category}</div>
            <div style="display:flex;gap:4px">
              <button onclick="openAdvanceBet(${m.id}, 'yes')"
                      style="background:rgba(0,230,118,0.1);border:1px solid var(--green);color:var(--text);padding:4px 8px;border-radius:4px;cursor:pointer;font-size:11px">
                YES <b>${yesProb}%</b>
              </button>
              <button onclick="openAdvanceBet(${m.id}, 'no')"
                      style="background:rgba(255,82,82,0.1);border:1px solid var(--red);color:var(--text);padding:4px 8px;border-radius:4px;cursor:pointer;font-size:11px">
                NO <b>${noProb}%</b>
              </button>
            </div>
          </div>
        `;
      }).join('');
    card.innerHTML = `
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
        <div style="font-weight:700;font-size:14px">${teamNameEsc}</div>
        <div style="margin-left:auto;font-size:11px;color:var(--text-dim)">${markets.length} markets</div>
      </div>
      ${rows}
    `;
    grid.appendChild(card);
  });
}

// Open the existing markets bet modal pre-filled with the chosen
// advance market + side.
async function openAdvanceBet(marketId, side) {
  if (!PM.getUser()) return toast('Please log in first', 'error');
  try {
    const m = await fetch(`/api/markets/${marketId}`).then(r => r.json());
    // Switch to the markets tab
    const tab = Array.from(document.querySelectorAll('.tab')).find(t => t.textContent.includes('Markets'));
    if (tab) tab.click();
    setTimeout(() => {
      openBetModal(m);
      if (side) {
        setTimeout(() => {
          const yesBtn = document.getElementById('bet-side-yes');
          const noBtn  = document.getElementById('bet-side-no');
          if (yesBtn && noBtn) {
            if (side === 'yes') { yesBtn.click(); }
            else { noBtn.click(); }
          }
        }, 50);
      }
    }, 100);
  } catch (e) {
    toast('Failed to open bet modal: ' + e, 'error');
  }
}

function openResolve(matchId) {
  const m = matchesData.find(x => x.id === matchId);
  if (!m) return;
  currentMatchId = matchId;
  document.getElementById('resolve-match-info').textContent = `Group ${m.group_name} · ${m.team_a} vs ${m.team_b}`;
  document.getElementById('res-team-a-label').textContent = m.team_a;
  document.getElementById('res-team-b-label').textContent = m.team_b;
  document.getElementById('res-team-a').textContent = m.team_a + ' Win';
  document.getElementById('res-team-b').textContent = m.team_b + ' Win';
  document.getElementById('res-score-a').value = 0;
  document.getElementById('res-score-b').value = 0;
  document.getElementById('resolve-winner').value = 'draw';
  document.getElementById('resolve-modal').classList.add('show');
}

async function submitResolve() {
  const sa = parseInt(document.getElementById('res-score-a').value) || 0;
  const sb = parseInt(document.getElementById('res-score-b').value) || 0;
  try {
    await PM.api('/api/worldcup/resolve', {
      method: 'POST',
      body: JSON.stringify({
        match_id: currentMatchId,
        winner: document.getElementById('resolve-winner').value,
        score_a: sa, score_b: sb,
      }),
    });
    closeModal('resolve-modal');
    await loadAll(); await loadUsers();
    toast('Resolved! Standings updated.', 'success');
  } catch (e) { toast(e.message, 'error'); }
}

// ── ADMIN ──
function renderAdmin() {
  if (!PM.getUser()) {
    document.getElementById('admin-matches-list').innerHTML = '<div class="empty">Login to view admin</div>';
    const koEl = document.getElementById('admin-ko-list');
    if (koEl) koEl.innerHTML = '<div class="empty">Login to view admin</div>';
    return;
  }
  const el = document.getElementById('admin-matches-list');

  // Get all matches sorted by date
  const allMatches = [...matchesData].sort((a, b) => {
    if (a.start_date && b.start_date) return a.start_date.localeCompare(b.start_date);
    if (a.start_date) return -1;
    if (b.start_date) return 1;
    return (a.group_name + a.matchday).localeCompare(b.group_name + b.matchday);
  });
  
  const syncedCount = matchesData.filter(m => m.start_date).length;
  document.getElementById('sync-status').textContent = `${syncedCount}/${matchesData.length} synced`;
  
  let html = '';
  let currentDate = '';
  
  allMatches.forEach(m => {
    let dateStr = 'TBD';
    let timeStr = '';
    let statusIcon = '';
    
    if (m.start_date) {
      const h = toHCMC(m.start_date);
      dateStr = h.date;
      timeStr = h.time;
      
      const diffMs = new Date(m.start_date).getTime() - Date.now();
      if (m.status === 'completed') {
        statusIcon = '✅';
      } else if (diffMs < 0 && diffMs > -3*3600000) {
        statusIcon = '🔴';
        dateStr = 'LIVE';
      } else if (diffMs <= 6*3600000 && diffMs > 0) {
        statusIcon = '⏳';
      }
    } else if (m.status === 'completed') {
      statusIcon = '✅';
    }
    
    if (dateStr !== currentDate) {
      currentDate = dateStr;
      const label = dateStr === 'LIVE' ? '🔴 LIVE NOW' : dateStr === 'TBD' ? '📅 TBD' : `📅 ${dateStr}`;
      html += `<div style="color:var(--gold);font-size:12px;font-weight:700;margin:14px 0 6px;padding:4px 10px;background:rgba(255,215,64,0.08);border-radius:4px">${label}</div>`;
    }
    
    // Match row
    let scoreDisplay = '';
    let rightInfo = '';
    if (m.status === 'completed') {
      scoreDisplay = (m.score_a != null && m.score_b != null) ? `${m.score_a} - ${m.score_b}` : '?';
      rightInfo = `<span style="font-size:11px;color:var(--green);min-width:80px;text-align:right">${m.winner === 'team_a' ? m.team_a : m.winner === 'team_b' ? m.team_b : 'Draw'}</span>`;
    } else if (m.outcome_prices) {
      try {
        const op = JSON.parse(m.outcome_prices);
        const h = (op.moneyline_home * 100).toFixed(0);
        const d = op.draw ? (op.draw * 100).toFixed(0) : '?';
        const a = op.draw ? ((op.moneyline_away - op.draw) * 100).toFixed(0) : '?';
        rightInfo = `<span style="font-size:10px;color:var(--text-dim);min-width:90px;text-align:right">${h}% / ${d}% / ${a}%</span>`;
      } catch(e) {}
    }
    
    html += `<div class="match-card" style="${m.status === 'completed' ? 'opacity:0.7' : ''}">
      <span style="font-size:10px;color:var(--text-dim);min-width:45px">${statusIcon} ${timeStr}</span>
      <div class="match-teams" style="flex:1;font-size:13px">${flagImg(m.team_a)}${esc(m.team_a)} <span class="vs">vs</span> ${flagImg(m.team_b)}${esc(m.team_b)}</div>
      <span style="font-size:12px;font-weight:700;color:var(--gold);min-width:35px;text-align:center">${scoreDisplay}</span>
      <span style="font-size:10px;color:var(--text-dim);min-width:45px;text-align:center">G${m.group_name}</span>
      ${rightInfo}
      <button class="sm outline" style="min-width:auto" onclick="openResolve(${m.id})" title="Set winner + score">⚙️</button>
    </div>`;
  });

  el.innerHTML = html || '<div class="empty">No matches</div>';

  // KO admin: show all slots, with set-team + resolve buttons for admins
  const koEl = document.getElementById('admin-ko-list');
  if (koEl) {
    const isAdmin = PM.isAdmin();
    let koHtml = '';
    for (const round of ROUND_ORDER) {
      const matches = knockoutData.filter(m => m.round === round);
      if (!matches.length) continue;
      koHtml += `<div class="bracket-round"><h3>${ROUND_NAMES[round]}</h3><div class="bracket-grid">`;
      matches.forEach(m => {
        const teamA = m.team_a, teamB = m.team_b;
        const cls = m.status === 'completed' ? '' : (teamA && teamB ? 'live' : '');
        const setA = isAdmin && !teamA ? `<button class="sm outline" onclick="knockoutSetTeam(${m.id},'team_a')">+</button>` : '';
        const setB = isAdmin && !teamB ? `<button class="sm outline" onclick="knockoutSetTeam(${m.id},'team_b')">+</button>` : '';
        const resolveBtn = isAdmin && teamA && teamB && m.status === 'pending' ? `<button class="sm gold" onclick="knockoutResolve(${m.id})">Resolve</button>` : '';
        const scoreStr = (m.score_a != null && m.score_b != null) ? `${m.score_a}-${m.score_b}` : '';
        koHtml += `<div class="knockout-card ${cls}">
          <div class="knockout-slot">${m.slot}</div>
          <div class="knockout-teams">
            <div class="knockout-team${teamA ? '' : ' tbd'}">${flagImg(teamA || '')}${teamA || 'TBD'}${setA}</div>
            <div class="knockout-team${teamB ? '' : ' tbd'}">${flagImg(teamB || '')}${teamB || 'TBD'}${setB}</div>
          </div>
          <div style="display:flex;flex-direction:column;align-items:flex-end;gap:4px">
            ${m.status === 'completed' ? `<div class="score-pill">${scoreStr}</div><div class="winner-tag win">${m.winner}</div>` : ''}
            ${resolveBtn}
          </div>
        </div>`;
      });
      koHtml += '</div></div>';
    }
    if (!isAdmin) {
      koHtml = '<div class="empty" style="font-size:12px">Admin only — log in as the first registered user to manage the bracket.</div>';
    } else if (!koHtml) {
      koHtml = '<div class="empty">No knockout slots</div>';
    }
    koEl.innerHTML = koHtml;
  }
}

async function syncNow() {
  const btn = event.target;
  btn.textContent = '⏳ Syncing...';
  btn.disabled = true;
  try {
    const r = await fetch('/api/sync', {method:'POST'});
    const data = await r.json();
    await loadAll();
    toast('Synced! ' + (data.output.match(/Updated: \d+/) || [''])[0], 'success');
  } catch(e) {
    toast('Sync failed', 'error');
  }
  btn.textContent = '🔄 Sync Now';
  btn.disabled = false;
}

// ── HISTORY ──
async function renderHistory() {
  const el = document.getElementById('history-content');
  const me = PM.getUser();
  // Load: per-user bet history + completed match history (shared, all users)
  const [myBets, completed] = await Promise.all([
    me ? PM.api(`/api/users/${me.id}/history?limit=100`).catch(() => ({match_bets:[],knockout_bets:[],market_bets:[],totals:{count:0}})) : Promise.resolve(null),
    PM.api('/api/history/matches?limit=20'),
  ]);
  const esc = PM.esc;
  let html = '';

  // Section 1: Recent completed matches (shared)
  if (completed && completed.length) {
    html += '<h4 class="section-h">Recent Completed Matches</h4>';
    html += completed.slice(0, 10).map(m => {
      const scoreStr = (m.score_a != null && m.score_b != null) ? `${m.score_a}-${m.score_b}` : '?';
      const w = m.winner === 'team_a' ? m.team_a : m.winner === 'team_b' ? m.team_b : 'Draw';
      const date = m.start_date ? m.start_date.slice(0, 10) : '';
      return `<div class="history-row">
        <span style="font-size:11px;color:var(--text-dim);min-width:60px">${esc(date)}</span>
        <div style="flex:1;min-width:0">
          <div style="font-size:12px">${esc(m.team_a)} <span style="color:var(--text-dim)">vs</span> ${esc(m.team_b)}</div>
          <div style="font-size:10px;color:var(--text-dim)">Group ${esc(m.group_name)} · ${m.total_bets} bets · ${m.winning_bets} won</div>
        </div>
        <span class="score-pill">${scoreStr}</span>
        <span class="winner-tag win" style="min-width:60px;text-align:center">${esc(w)}</span>
      </div>`;
    }).join('');
  }

  // Section 2: My bets (per-user, all bet types)
  if (me && myBets) {
    const totalRows = (myBets.match_bets || []).length + (myBets.knockout_bets || []).length + (myBets.market_bets || []).length;
    if (totalRows > 0) {
      html += `<h4 class="section-h">Your Bet History (${myBets.totals.wins}W / ${myBets.totals.losses}L · net ${myBets.totals.net > 0 ? '+' : ''}${myBets.totals.net.toFixed(0)} pts)</h4>`;
      const allBets = [
        ...(myBets.match_bets || []).map(b => ({...b, _type: 'Group'})),
        ...(myBets.knockout_bets || []).map(b => ({...b, _type: 'KO'})),
        ...(myBets.market_bets || []).map(b => ({...b, _type: 'Market'})),
      ].sort((a, b) => (b.placed_at || '').localeCompare(a.placed_at || ''));
      html += `<table class="leaderboard-table"><thead><tr>
        <th>Time</th><th>Type</th><th>Match</th><th>Pick</th><th>Amount</th><th>Odds</th><th>Result</th>
      </tr></thead><tbody>`;
      for (const b of allBets.slice(0, 100)) {
        const timeStr = b.placed_at ? new Date(b.placed_at + 'Z').toLocaleString('en-US', {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit'}) : '';
        const scoreStr = (b.score_a != null && b.score_b != null) ? ` (${b.score_a}-${b.score_b})` : ' (?)';
        const label = b.match_label || b.label || b.market_question || '';
        const tag = b.won === 1 ? 'win' : (b.won === 0 ? 'lose' : 'pending');
        html += `<tr>
          <td style="font-size:11px;color:var(--text-dim)">${esc(timeStr)}</td>
          <td><span class="tag tag-${tag === 'win' ? 'win' : tag === 'lose' ? 'loss' : 'draw'}">${esc(b._type)}</span></td>
          <td style="font-size:12px">${esc(label)}${scoreStr}</td>
          <td>${esc(b.pick_label || b.side || '')}</td>
          <td>${b.amount.toFixed(0)}</td>
          <td>${(b.odds*100).toFixed(0)}%</td>
          <td style="font-weight:700;color:${b.won === 1 ? 'var(--green)' : b.won === 0 ? 'var(--red)' : 'var(--gold)'}">${esc(b.result)}</td>
        </tr>`;
      }
      html += '</tbody></table>';
    } else {
      html += '<h4 class="section-h">Your Bet History</h4><div class="empty">No bets yet — place one from a Group or Knockout match.</div>';
    }
  } else {
    html += '<div class="empty" style="margin-top:18px">Login to see your personal bet history</div>';
  }

  if (!html) html = '<div class="empty">No history yet</div>';
  el.innerHTML = html;
}

async function resetAll() {
  if (!PM.isAdmin()) return toast('Admin only', 'error');
  if (!confirm('⚠️ Reset ALL data? This clears bets, standings, and resets points to 1000. Cannot undo.')) return;
  if (!confirm('Are you ABSOLUTELY sure? Type confirm in the next dialog.')) return;
  const reason = prompt('Reason for reset (logged in pm.log):');
  try {
    await PM.api('/api/admin/reset', {method:'POST', body: JSON.stringify({confirm: true, reason: reason || ''})});
    await loadAll();
    await loadUsers();
    toast('All data reset!', 'success');
  } catch (e) { toast(e.message, 'error'); }
}

// ── LEADERBOARD ──
async function renderLeaderboard() {
  const data = await fetch('/api/worldcup/leaderboard').then(r => r.json());
  const el = document.getElementById('leaderboard-content');
  if (!data.length) { el.innerHTML = '<div class="empty">No users</div>'; return; }
  let html = '<table class="leaderboard-table"><thead><tr><th>#</th><th>Name</th><th>Points</th><th>Bets</th><th>W</th><th>L</th></tr></thead><tbody>';
  data.forEach((u, i) => {
    const rc = i < 3 ? ' rank-'+(i+1) : '';
    const adminBadge = u.is_admin ? ' <span style="font-size:9px;color:var(--gold);font-weight:700">[admin]</span>' : '';
    html += `<tr style="cursor:pointer" onclick="openUserProfile(${u.id})" title="Click to view profile">
      <td class="rank${rc}">${i+1}</td>
      <td style="font-weight:600">${esc(u.name)}${adminBadge}</td>
      <td class="points">${u.points.toFixed(0)}</td>
      <td>${u.total_match_bets}</td>
      <td style="color:var(--green)">${u.wins}</td>
      <td style="color:var(--red)">${u.losses}</td>
    </tr>`;
  });
  html += '</tbody></table>';
  el.innerHTML = html;
}

// ── Public activity feed ─────────────────────────────────────────────────
async function renderActivity() {
  const el = document.getElementById('activity-content');
  const cnt = document.getElementById('activity-count');
  try {
    const data = await PM.api('/api/activity?limit=80');
    cnt.textContent = `${data.count} events`;
    if (!data.items.length) {
      el.innerHTML = '<div class="empty">No bets yet — be the first!</div>';
      return;
    }
    const esc = PM.esc;
    const kindLabel = { match_bet: 'Group', ko_bet: 'KO', market_bet: 'Market' };
    const kindColor = { match_bet: 'var(--accent)', ko_bet: 'var(--gold)', market_bet: 'var(--green)' };
    let html = '';
    for (const a of data.items) {
      const timeStr = a.placed_at ? new Date(a.placed_at + 'Z').toLocaleString('en-US', {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit'}) : '';
      const resultBadge = a.result
        ? (a.result.startsWith('Won') ? `<span class="winner-tag win">${esc(a.result)}</span>`
          : a.result === 'Lost' ? `<span class="winner-tag lose">${esc(a.result)}</span>`
          : `<span class="winner-tag pending">${esc(a.result)}</span>`)
        : '';
      html += `<div class="history-row">
        <span style="font-size:10px;color:var(--text-dim);min-width:60px">${esc(timeStr)}</span>
        <span class="tag" style="background:${kindColor[a.kind] || 'var(--text-dim)'}22;color:${kindColor[a.kind] || 'var(--text-dim)'};min-width:60px;text-align:center;font-size:10px">${kindLabel[a.kind] || a.kind}</span>
        <div style="flex:1;min-width:0">
          <div style="font-size:12px">${esc(a.summary)}</div>
          <div style="font-size:10px;color:var(--text-dim)">${esc(a.label || '')}</div>
        </div>
        ${resultBadge}
      </div>`;
    }
    el.innerHTML = html;
  } catch (e) {
    el.innerHTML = '<div class="empty">Error: ' + PM.esc(e.message) + '</div>';
  }
}

// ── User profile modal (clickable from leaderboard) ─────────────────────
let _profileCache = {};

async function openUserProfile(userId) {
  const modal = document.getElementById('user-profile-modal');
  const body = document.getElementById('user-profile-body');
  body.innerHTML = '<div class="spinner"></div> Loading...';
  modal.classList.add('show');
  try {
    const [profile, stats, active, history] = await Promise.all([
      PM.api(`/api/users/${userId}/profile`),
      PM.api(`/api/users/${userId}/stats`),
      PM.api(`/api/users/${userId}/active-bets`),
      PM.api(`/api/users/${userId}/history?limit=20&scope=all`),
    ]);
    const pnl = stats.net_pnl || 0;
    const pnlColor = pnl > 0 ? 'var(--green)' : (pnl < 0 ? 'var(--red)' : 'var(--text)');
    const wr = (stats.win_rate * 100).toFixed(0);
    const adminTag = profile.is_admin ? '<span style="background:var(--gold);color:#000;padding:2px 6px;border-radius:4px;font-size:10px;font-weight:700;margin-left:6px">ADMIN</span>' : '';
    const totalActive = (active.match_bets || []).length + (active.knockout_bets || []).length + (active.market_bets || []).length;
    let html = `
      <div style="display:flex;align-items:center;margin-bottom:14px">
        <h3 style="margin:0">${PM.esc(profile.name)}</h3>${adminTag}
        <span style="margin-left:auto;font-size:11px;color:var(--text-dim)">Joined ${profile.created_at?.slice(0, 10) || '?'}</span>
      </div>
      <div class="stats-grid" style="margin-bottom:14px">
        <div class="stat-card"><div class="stat-label">Balance</div><div class="stat-value">${stats.points.toFixed(0)}<span class="stat-unit">pts</span></div></div>
        <div class="stat-card"><div class="stat-label">Net P&L</div><div class="stat-value" style="color:${pnlColor}">${pnl > 0 ? '+' : ''}${pnl.toFixed(0)}<span class="stat-unit">pts</span></div></div>
        <div class="stat-card"><div class="stat-label">Win Rate</div><div class="stat-value">${wr}<span class="stat-unit">%</span></div><div style="font-size:10px;color:var(--text-dim)">${stats.wins}W / ${stats.total_bets - stats.wins}L</div></div>
        <div class="stat-card"><div class="stat-label">Wagered</div><div class="stat-value">${stats.total_wagered.toFixed(0)}<span class="stat-unit">pts</span></div></div>
      </div>
      <h4 class="section-h">Active Bets (${totalActive})</h4>
    `;
    if (totalActive === 0) {
      html += '<div class="empty" style="padding:14px">No active bets</div>';
    } else {
      for (const b of (active.match_bets || [])) {
        html += `<div class="history-row">
          <span class="tag" style="background:var(--accent)22;color:var(--accent);min-width:50px;text-align:center;font-size:10px">Group</span>
          <div style="flex:1"><b>${PM.esc(b.pick_label)}</b> · ${PM.esc(b.label)}</div>
          <span style="font-size:11px;color:var(--text-dim)">${b.amount} pts @ ${Math.round(b.odds*100)}%</span>
        </div>`;
      }
      for (const b of (active.knockout_bets || [])) {
        html += `<div class="history-row">
          <span class="tag" style="background:var(--gold)22;color:var(--gold);min-width:50px;text-align:center;font-size:10px">KO</span>
          <div style="flex:1"><b>${PM.esc(b.pick_label)}</b> · ${PM.esc(b.label)}</div>
          <span style="font-size:11px;color:var(--text-dim)">${b.amount} pts @ ${Math.round(b.odds*100)}%</span>
        </div>`;
      }
    }
    html += `<h4 class="section-h">Recent Bets</h4>`;
    const allBets = [
      ...(history.match_bets || []).map(b => ({...b, _type: 'Group'})),
      ...(history.knockout_bets || []).map(b => ({...b, _type: 'KO'})),
      ...(history.market_bets || []).map(b => ({...b, _type: 'Market'})),
    ].slice(0, 10);
    if (!allBets.length) {
      html += '<div class="empty" style="padding:14px">No bets yet</div>';
    } else {
      for (const b of allBets) {
        const tag = b.won === 1 ? 'win' : (b.won === 0 ? 'lose' : 'pending');
        const label = b.match_label || b.label || b.market_question || '';
        html += `<div class="history-row">
          <span class="tag" style="min-width:50px;text-align:center;font-size:10px">${b._type}</span>
          <div style="flex:1">${PM.esc(label)} <span style="color:var(--text-dim)">·</span> <b>${PM.esc(b.pick_label || b.side || '')}</b> · ${b.amount}pts</div>
          <span class="winner-tag ${tag}">${PM.esc(b.result || 'Pending')}</span>
        </div>`;
      }
    }
    body.innerHTML = html;
  } catch (e) {
    body.innerHTML = '<div class="empty">Error: ' + PM.esc(e.message) + '</div>';
  }
}

function closeUserProfile() {
  document.getElementById('user-profile-modal').classList.remove('show');
}

function closeModal(id) { document.getElementById(id).classList.remove('show'); }
function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
function toast(msg, type) { const t = document.getElementById('toast'); t.textContent = msg; t.className = 'toast '+type+' show'; setTimeout(() => t.classList.remove('show'), 3000); }
</script>
</body>
</html>"""

# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    print(f"Prediction Market → http://localhost:{PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
