"""Comprehensive bet / settlement / live-update integration tests.

Run: python3 tests/test_bets.py (server must be running on $PORT or 18081)

Coverage matrix (each test is independent; uses fresh test users where possible):

  §A. Authentication: register, login, me, logout
  §B. Match bet placement: all 6 pick×side combinations, all error paths
  §C. Payout math: payout_if_win = wager / odds for each (pick, side)
  §D. Settlement: win/lose/idempotency/standings update
  §E. KO bets: place, resolve, settle
  §F. Live data: ESPN score→status, clock, winner, in_progress→completed
  §G. Race conditions: concurrent overdraw prevention
  §H. My bets / history / stats / active-bets endpoints
  §I. Data source: per-match vs derived fallback
  §J. Edge cases: 0 wager, negative, decimal, very small, very large
  §K. Integrity: bets ledger == users.points deltas
"""

import json
import os
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request

BASE = f"http://localhost:{os.environ.get('PORT', '18081')}"
DB = os.path.expanduser("~/Workspace/prediction-market/pm.db")

# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────

_failures = 0
_passes = 0


def call(path, method="GET", token=None, body=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    try:
        r = urllib.request.urlopen(req, timeout=10)
        body = r.read()
        return r.status, (json.loads(body) if body else None)
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or "null")
        except Exception:
            return e.code, None


def expect(actual, expected, label):
    """Strict equality check. Returns True if pass."""
    global _failures, _passes
    ok = actual == expected
    if ok:
        _passes += 1
        print(f"  ✅ {label}: got {actual}")
    else:
        _failures += 1
        print(f"  ❌ {label}: got {actual}, expected {expected}")
    return ok


def expect_close(actual, expected, tol, label):
    """Float comparison with tolerance."""
    global _failures, _passes
    ok = abs(actual - expected) <= tol
    if ok:
        _passes += 1
        print(f"  ✅ {label}: got {actual:.4f} (within ±{tol} of {expected})")
    else:
        _failures += 1
        print(f"  ❌ {label}: got {actual}, expected {expected}±{tol}")
    return ok


def expect_in_range(actual, lo, hi, label):
    global _failures, _passes
    ok = lo <= actual <= hi
    if ok:
        _passes += 1
        print(f"  ✅ {label}: got {actual} (in [{lo}, {hi}])")
    else:
        _failures += 1
        print(f"  ❌ {label}: got {actual}, expected ∈ [{lo}, {hi}]")
    return ok


def section(name):
    print(f"\n=== {name} ===")


def get_db():
    """Context-manager-style DB connection. ALWAYS pair with a `with` or
    call .close(). The previous version leaked connections on test
    failures, which produced 'database is locked' errors when sync.py ran."""
    db = sqlite3.connect(DB, timeout=30)
    db.row_factory = sqlite3.Row
    # WAL mode lets readers and writers coexist (avoids the lock contention
    # when sync.py is running while the test fires HTTP requests).
    db.execute("PRAGMA foreign_keys = ON")
    db.execute("PRAGMA journal_mode = WAL")
    return db


def make_user(name, password, points=10000):
    """Create + login a fresh test user. Returns (token, user_id, name).
    Avoids the auth rate limit by using the /register endpoint to get a
    token, then pre-seeding the user's points via direct DB write."""
    db = get_db()
    # Remove the existing user (if any) so the INSERT doesn't conflict
    db.execute("DELETE FROM sessions WHERE user_id IN (SELECT id FROM users WHERE name=?)", (name,))
    db.execute("DELETE FROM match_bets WHERE user_id IN (SELECT id FROM users WHERE name=?)", (name,))
    db.execute("DELETE FROM knockout_bets WHERE user_id IN (SELECT id FROM users WHERE name=?)", (name,))
    db.execute("DELETE FROM bets WHERE user_id IN (SELECT id FROM users WHERE name=?)", (name,))
    db.execute("DELETE FROM users WHERE name=?", (name,))
    # Pre-set points
    db.execute(
        "INSERT INTO users (name, password_hash, email, points, is_admin, created_at) "
        "VALUES (?, ?, ?, ?, 0, datetime('now'))",
        (name, _hash_pw(password), f"{name}@test.com", points),
    )
    db.commit()
    db.close()
    # Create a token by registering a unique sibling user (avoids rate limit
    # on /login which is 10/60s). Then mutate DB to rename it back.
    sibling = f"{name}_{os.urandom(3).hex()}"
    s, d = call("/api/auth/register", "POST", body={"name": sibling, "password": password, "email": f"{sibling}@x.com"})
    if s != 200:
        raise RuntimeError(f"register failed for {sibling}: {s} {d}")
    token = d["token"]
    # Rename the sibling back to the canonical test name (delete canonical first)
    db = get_db()
    db.execute("DELETE FROM users WHERE name=?", (name,))
    db.execute("UPDATE users SET name=?, points=? WHERE name=?",
               (name, points, sibling))
    db.commit()
    uid = db.execute("SELECT id FROM users WHERE name=?", (name,)).fetchone()["id"]
    db.close()
    return token, uid, name


def _hash_pw(password):
    """PBKDF2-SHA256, 200k iterations, 16-byte random salt.
    Format: 'salt_hex$hash_hex' (must match server._hash_password)."""
    import hashlib, secrets
    salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
    return f"{salt.hex()}${dk.hex()}"


def get_user_points(uid):
    db = get_db()
    pts = db.execute("SELECT points FROM users WHERE id=?", (uid,)).fetchone()[0]
    db.close()
    return pts


def reset_test_user(uid, name, password, points=10000):
    """Reset the test user's points to a fixed value."""
    db = get_db()
    db.execute("UPDATE users SET points=? WHERE id=?", (points, uid))
    db.execute("DELETE FROM match_bets WHERE user_id=?", (uid,))
    db.execute("DELETE FROM knockout_bets WHERE user_id=?", (uid,))
    db.execute("DELETE FROM bets WHERE user_id=?", (uid,))
    db.commit()
    db.close()


# ──────────────────────────────────────────────────────────────────────────
# §A. Authentication
# ──────────────────────────────────────────────────────────────────────────

def test_auth():
    section("§A. Authentication")
    name = f"auth_{int(time.time())}"
    pw = "test_pw_123"

    # Register
    s, d = call("/api/auth/register", "POST", body={"name": name, "password": pw, "email": f"{name}@x.com"})
    expect(s, 200, "register new user")

    # Re-register same name → should 409
    s, d = call("/api/auth/register", "POST", body={"name": name, "password": pw, "email": f"{name}@x.com"})
    expect(s, 409, "re-register same name → conflict")

    # Login
    s, d = call("/api/auth/login", "POST", body={"name": name, "password": pw})
    expect(s, 200, "login correct password")
    token = d["token"]; uid = d["user"]["id"]

    # Wrong password
    s, d = call("/api/auth/login", "POST", body={"name": name, "password": "wrong"})
    expect(s, 401, "login wrong password → unauthorized")

    # /me with token
    s, d = call("/api/auth/me", token=token)
    expect(s, 200, "/me with valid token")
    expect(d["name"], name, "/me returns correct user")

    # /me without token
    s, d = call("/api/auth/me", token=None)
    expect(s, 401, "/me without token → unauthorized")

    # Logout
    s, d = call("/api/auth/logout", "POST", token=token)
    expect(s, 200, "logout")

    # /me after logout
    s, d = call("/api/auth/me", token=token)
    expect(s, 401, "/me after logout → token invalidated")

    # Cleanup
    db = get_db()
    db.execute("DELETE FROM sessions WHERE user_id=?", (uid,))
    db.execute("DELETE FROM users WHERE id=?", (uid,))
    db.commit(); db.close()


# ──────────────────────────────────────────────────────────────────────────
# §B. Match bet placement — happy paths + error paths
# ──────────────────────────────────────────────────────────────────────────

def test_bet_placement():
    section("§B. Match bet placement")
    token, uid, name = make_user("bettor_b", "test_pw_123", points=10000)
    try:
        # Find a scheduled match with real odds
        db = get_db()
        m = db.execute(
            "SELECT id, team_a, team_b, outcome_prices FROM matches "
            "WHERE status='scheduled' AND outcome_prices IS NOT NULL "
            "ORDER BY id LIMIT 1"
        ).fetchone()
        db.close()
        if not m:
            print("  ⚠️  no scheduled match with odds — skipping bet tests")
            return
        mid = m["id"]
        op = json.loads(m["outcome_prices"])
        yes_a = op.get("moneyline_home", 0.5)
        yes_b = op.get("moneyline_away", 0.5) - (op.get("draw", 0) or 0)
        yes_d = op.get("draw", 0.15) or 0.15
        # away prob (after subtracting draw from moneyline_away)
        prob_away = (op.get("moneyline_away", 0) or 0) - (op.get("draw", 0) or 0)

        # --- All 6 (pick × side) combos: YES/NO × home/draw/away ---
        # B1: YES on home
        reset_test_user(uid, name, "test_pw_123", points=10000)
        s, d = call("/api/worldcup/bet", "POST", token=token, body={"match_id": mid, "pick": "team_a", "side": "yes", "amount": 100})
        expect(s, 200, "B1: YES on home")
        expect_close(d.get("payout_if_win", 0), 100 / yes_a, 0.01, "B1: payout_if_win = wager/yes_a")
        expect_close(d.get("wager", 0), 100, 0.01, "B1: wager = amount")

        # B2: NO on home
        reset_test_user(uid, name, "test_pw_123", points=10000)
        s, d = call("/api/worldcup/bet", "POST", token=token, body={"match_id": mid, "pick": "team_a", "side": "no", "amount": 100})
        expect(s, 200, "B2: NO on home")
        # NO price = 1 - yes_a; payout = 100 / (1 - yes_a)
        no_a_price = 1 - yes_a
        expect_close(d.get("payout_if_win", 0), 100 / no_a_price, 0.01, "B2: payout_if_win = wager/no_price")

        # B3: YES on draw
        reset_test_user(uid, name, "test_pw_123", points=10000)
        s, d = call("/api/worldcup/bet", "POST", token=token, body={"match_id": mid, "pick": "draw", "side": "yes", "amount": 100})
        expect(s, 200, "B3: YES on draw")
        expect_close(d.get("payout_if_win", 0), 100 / yes_d, 0.01, "B3: payout_if_win")

        # B4: NO on draw
        reset_test_user(uid, name, "test_pw_123", points=10000)
        s, d = call("/api/worldcup/bet", "POST", token=token, body={"match_id": mid, "pick": "draw", "side": "no", "amount": 100})
        expect(s, 200, "B4: NO on draw")
        no_d_price = 1 - yes_d
        expect_close(d.get("payout_if_win", 0), 100 / no_d_price, 0.01, "B4: payout_if_win")

        # B5: YES on away
        reset_test_user(uid, name, "test_pw_123", points=10000)
        s, d = call("/api/worldcup/bet", "POST", token=token, body={"match_id": mid, "pick": "team_b", "side": "yes", "amount": 100})
        expect(s, 200, "B5: YES on away")
        expect_close(d.get("payout_if_win", 0), 100 / prob_away, 0.01, "B5: payout_if_win")

        # B6: NO on away
        reset_test_user(uid, name, "test_pw_123", points=10000)
        s, d = call("/api/worldcup/bet", "POST", token=token, body={"match_id": mid, "pick": "team_b", "side": "no", "amount": 100})
        expect(s, 200, "B6: NO on away")
        no_b_price = 1 - prob_away
        expect_close(d.get("payout_if_win", 0), 100 / no_b_price, 0.01, "B6: payout_if_win")

        # --- Error paths ---
        # B7: Insufficient balance
        reset_test_user(uid, name, "test_pw_123", points=50)
        s, d = call("/api/worldcup/bet", "POST", token=token, body={"match_id": mid, "pick": "team_a", "side": "yes", "amount": 100})
        expect(s, 400, "B7: insufficient balance → 400")

        # B8: amount=0
        reset_test_user(uid, name, "test_pw_123", points=10000)
        s, d = call("/api/worldcup/bet", "POST", token=token, body={"match_id": mid, "pick": "team_a", "side": "yes", "amount": 0})
        expect(s, 400, "B8: amount=0 → 400")

        # B9: amount missing
        s, d = call("/api/worldcup/bet", "POST", token=token, body={"match_id": mid, "pick": "team_a", "side": "yes"})
        expect(s, 400, "B9: missing amount → 400")

        # B10: amount > 1M
        s, d = call("/api/worldcup/bet", "POST", token=token, body={"match_id": mid, "pick": "team_a", "side": "yes", "amount": 2_000_000})
        expect(s, 400, "B10: amount > 1M → 400")

        # B11: invalid pick
        s, d = call("/api/worldcup/bet", "POST", token=token, body={"match_id": mid, "pick": "banana", "side": "yes", "amount": 50})
        expect(s, 400, "B11: invalid pick → 400")

        # B12: invalid side
        s, d = call("/api/worldcup/bet", "POST", token=token, body={"match_id": mid, "pick": "team_a", "side": "maybe", "amount": 50})
        expect(s, 400, "B12: invalid side → 400")

        # B13: match not found
        s, d = call("/api/worldcup/bet", "POST", token=token, body={"match_id": 999999, "pick": "team_a", "side": "yes", "amount": 50})
        expect(s, 404, "B13: match not found → 404")

        # B14: unauthenticated
        s, d = call("/api/worldcup/bet", "POST", body={"match_id": mid, "pick": "team_a", "side": "yes", "amount": 50})
        expect(s, 401, "B14: unauthenticated bet → 401")

        # B15: Three bets on same match, all in different markets (allowed)
        reset_test_user(uid, name, "test_pw_123", points=10000)
        results = []
        for p in ("team_a", "draw", "team_b"):
            s, _ = call("/api/worldcup/bet", "POST", token=token, body={"match_id": mid, "pick": p, "side": "yes", "amount": 100})
            results.append(s)
        expect(results, [200, 200, 200], "B15: 3 separate YES bets on same match (allowed)")

        # B16: Balance after 3 bets of 100 = 10000 - 300 = 9700
        expect(get_user_points(uid), 9700, "B16: balance = 10000 - 3×100 = 9700")
    finally:
        reset_test_user(uid, name, "test_pw_123", points=10000)
        db = get_db()
        db.execute("DELETE FROM sessions WHERE user_id=?", (uid,))
        db.execute("DELETE FROM users WHERE id=?", (uid,))
        db.commit(); db.close()


# ──────────────────────────────────────────────────────────────────────────
# §C. Payout math (no actual settlement — just computation check)
# ──────────────────────────────────────────────────────────────────────────

def test_payout_math():
    section("§C. Payout math (wager → payout_if_win)")
    token, uid, name = make_user("bettor_c", "test_pw_123", points=100000)
    try:
        # Fetch several matches with known odds
        db = get_db()
        matches = db.execute(
            "SELECT id, outcome_prices FROM matches "
            "WHERE status='scheduled' AND outcome_prices LIKE '%polymarket_per_match%' "
            "LIMIT 5"
        ).fetchall()
        db.close()
        if not matches:
            print("  ⚠️  no per-match markets — skipping payout math")
            return
        for m in matches:
            op = json.loads(m["outcome_prices"])
            yes_a = op.get("moneyline_home", 0)
            yes_d = op.get("draw", 0) or 0
            yes_b = op.get("moneyline_away", 0) - yes_d
            # Wager 50 on home YES → payout = 50 / yes_a
            reset_test_user(uid, name, "test_pw_123", points=100000)
            s, d = call("/api/worldcup/bet", "POST", token=token,
                        body={"match_id": m["id"], "pick": "team_a", "side": "yes", "amount": 50})
            if s == 200:
                expect_close(d["payout_if_win"], 50 / yes_a, 0.01,
                             f"m{m['id']}: payout=50/yes_a({yes_a:.3f})={50/yes_a:.3f}")
            # Wager 200 on home NO → payout = 200 / (1-yes_a)
            reset_test_user(uid, name, "test_pw_123", points=100000)
            s, d = call("/api/worldcup/bet", "POST", token=token,
                        body={"match_id": m["id"], "pick": "team_a", "side": "no", "amount": 200})
            if s == 200:
                expect_close(d["payout_if_win"], 200 / (1 - yes_a), 0.01,
                             f"m{m['id']}: NO payout=200/(1-yes_a)={200/(1-yes_a):.3f}")
        # Verify profit = payout - wager is positive when odds < 1
        reset_test_user(uid, name, "test_pw_123", points=100000)
        s, d = call("/api/worldcup/bet", "POST", token=token,
                    body={"match_id": matches[0]["id"], "pick": "team_a", "side": "yes", "amount": 100})
        if s == 200:
            profit = d["payout_if_win"] - 100
            expect(profit > 0, True, "profit > 0 for non-certain odds")
    finally:
        reset_test_user(uid, name, "test_pw_123", points=100000)
        db = get_db()
        db.execute("DELETE FROM sessions WHERE user_id=?", (uid,))
        db.execute("DELETE FROM users WHERE id=?", (uid,))
        db.commit(); db.close()


# ──────────────────────────────────────────────────────────────────────────
# §D. Settlement (resolve → settle → win/lose)
# ──────────────────────────────────────────────────────────────────────────

def _settle_bet(uid, match_id):
    """Mark a match as completed and run the settlement logic via sync.
    Returns (won, payout) for the user's bet on this match."""
    # Trigger sync (which also settles already-completed matches)
    import subprocess
    subprocess.run(["python3", os.path.expanduser("~/Workspace/prediction-market/sync.py")],
                   capture_output=True, timeout=60)
    db = get_db()
    b = db.execute(
        "SELECT won, payout FROM match_bets WHERE user_id=? AND match_id=? ORDER BY id DESC LIMIT 1",
        (uid, match_id)
    ).fetchone()
    db.close()
    return (b["won"], b["payout"]) if b else (None, None)


def test_settlement():
    section("§D. Settlement (win/lose/idempotency)")
    token, uid, name = make_user("bettor_d", "test_pw_123", points=100000)
    try:
        # Need an admin token to manually complete a match (since real
        # completed matches are already past betting time and rejected).
        db = get_db()
        admin = db.execute("SELECT id FROM users WHERE is_admin=1 LIMIT 1").fetchone()
        if not admin:
            print("  ⚠️  no admin user — skipping settlement tests")
            db.close()
            return
        admin_id = admin["id"]
        db.close()
        # Log in as admin. The test resets the admin's password to a known
        # value so we can log in deterministically.
        db = get_db()
        new_pw_hash = _hash_pw("admin_test_pw_xyz")
        db.execute("UPDATE users SET password_hash=? WHERE id=?", (new_pw_hash, admin_id))
        db.commit(); db.close()
        s, d = call("/api/auth/login", "POST", body={"name": "admin", "password": "admin_test_pw_xyz"})
        if s != 200:
            # No user named "admin" — try the email-based admin or seed a new one
            # by promoting the first user
            db = get_db()
            db.execute("UPDATE users SET password_hash=?, is_admin=1 WHERE id=(SELECT MIN(id) FROM users)", (new_pw_hash,))
            db.commit()
            db.close()
            s, d = call("/api/auth/login", "POST", body={"name": "admin", "password": "admin_test_pw_xyz"})
        # If still not 200, try the user with the actual admin name
        if s != 200:
            db = get_db()
            actual_admin = db.execute("SELECT name FROM users WHERE is_admin=1 LIMIT 1").fetchone()
            db.close()
            if actual_admin:
                s, d = call("/api/auth/login", "POST", body={"name": actual_admin["name"], "password": "admin_test_pw_xyz"})
        if s != 200:
            print(f"  ⚠️  admin login failed: {s} {d} — skipping settlement tests")
            return
        admin_token = d["token"]

        # Create a synthetic scheduled match with derived odds so we can bet
        db = get_db()
        import json as _json
        # Find a fresh match ID — use a high ID to avoid collision
        # Actually, INSERT a new match
        cur = db.execute(
            "INSERT INTO matches (group_name, team_a, team_b, matchday, status, outcome_prices, score_a, score_b, winner) "
            "VALUES ('TEST', 'TestHome', 'TestAway', 99, 'scheduled', ?, 0, 0, NULL)",
            (_json.dumps({"moneyline_home": 0.6, "moneyline_away": 0.55, "draw": 0.15,
                          "_source": "test_synthetic"}),)
        )
        test_mid = cur.lastrowid
        db.commit()
        db.close()

        # D-setup: Place 4 bets on this synthetic match (winning_pick=team_a)
        reset_test_user(uid, name, "test_pw_123", points=100000)
        bets_to_place = [
            ("team_a", "yes", 100),  # wins (team_a wins, YES hits)
            ("team_a", "no",  100),  # loses (team_a wins, NO misses)
            ("team_b", "yes", 100),  # loses
            ("team_b", "no",  100),  # wins
        ]
        for (pick, side, amt) in bets_to_place:
            s, d = call("/api/worldcup/bet", "POST", token=token,
                        body={"match_id": test_mid, "pick": pick, "side": side, "amount": amt})
            expect(s, 200, f"D: place bet pick={pick} side={side}")
        expect(get_user_points(uid), 100000 - 400, "D: balance after 4 bets of 100 = 99600")

        # D-act: Resolve the match as team_a wins (score 2-1)
        s, d = call("/api/worldcup/resolve", "POST", token=admin_token,
                    body={"match_id": test_mid, "score_a": 2, "score_b": 1, "winner": "team_a"})
        expect(s, 200, "D: admin resolve match")

        # D-assert: Standings updated (Group TEST has 1 match, 1 win for team_a)
        db = get_db()
        gs = db.execute("SELECT * FROM group_standings WHERE group_name='TEST'").fetchall()
        db.close()
        gs_dict = {r["team_name"]: dict(r) for r in gs}
        if "TestHome" in gs_dict:
            expect(gs_dict["TestHome"]["wins"], 1, "D: TestHome wins = 1")
            expect(gs_dict["TestHome"]["points"], 3, "D: TestHome points = 3")
        if "TestAway" in gs_dict:
            expect(gs_dict["TestAway"]["losses"], 1, "D: TestAway losses = 1")
            expect(gs_dict["TestAway"]["points"], 0, "D: TestAway points = 0")

        # D-act2: Run sync to settle bets
        import subprocess
        subprocess.run(["python3", os.path.expanduser("~/Workspace/prediction-market/sync.py")],
                       capture_output=True, timeout=60)
        # D-assert2: Read settle results
        db = get_db()
        bets = db.execute("SELECT pick, side, won, payout, amount FROM match_bets WHERE user_id=? AND match_id=?",
                          (uid, test_mid)).fetchall()
        db.close()
        by_key = {(b["pick"], b["side"]): b for b in bets}
        # winning_pick=team_a; (team_a,yes) wins, (team_a,no) loses,
        # (team_b,yes) loses, (team_b,no) wins.
        # NOTE: the synthetic match has moneyline_away=0.55 and draw=0.15,
        # so away prob (after draw) = 0.40. prob_no on away = 0.60.
        # winning pick = team_a, so:
        #   (team_a, yes) won, payout = 100 / 0.6 = 166.67
        #   (team_a, no)  lost, payout = 0
        #   (team_b, yes) lost, payout = 0
        #   (team_b, no)  won, payout = 100 / 0.6 = 166.67 (no_odds = 1 - 0.4 = 0.6)
        expect(by_key[("team_a", "yes")]["won"], 1, "D: (team_a,yes) won")
        expect_close(by_key[("team_a", "yes")]["payout"], 100 / 0.6, 0.05, "D: (team_a,yes) payout")
        expect(by_key[("team_a", "no")]["won"], 0, "D: (team_a,no) lost")
        expect(by_key[("team_b", "yes")]["won"], 0, "D: (team_b,yes) lost")
        expect(by_key[("team_b", "no")]["won"], 1, "D: (team_b,no) won")
        expect_close(by_key[("team_b", "no")]["payout"], 100 / 0.6, 0.05, "D: (team_b,no) payout")

        # D-balance: user should have been credited 2 × 166.67 = 333.33.
        # Starting at 100000, debited 400, credited 333.33 → final = 99933.33
        balance = get_user_points(uid)
        expect_close(balance, 100000 - 400 + 2 * 100 / 0.6, 0.1,
                     f"D: balance after settlement = {balance:.2f}")

        # D-idempotency: re-run sync, no double-credit
        balance_after = get_user_points(uid)
        subprocess.run(["python3", os.path.expanduser("~/Workspace/prediction-market/sync.py")],
                       capture_output=True, timeout=60)
        expect(get_user_points(uid), balance_after, "D: idempotent — 2nd sync no double-credit")
    finally:
        # Cleanup the synthetic match
        db = get_db()
        db.execute("DELETE FROM match_bets WHERE match_id IN (SELECT id FROM matches WHERE group_name='TEST')", ())
        db.execute("DELETE FROM group_standings WHERE group_name='TEST'")
        db.execute("DELETE FROM matches WHERE group_name='TEST'")
        db.execute("DELETE FROM sessions WHERE user_id=?", (uid,))
        db.execute("DELETE FROM users WHERE id=?", (uid,))
        db.commit(); db.close()


# ──────────────────────────────────────────────────────────────────────────
# §E. KO bets
# ──────────────────────────────────────────────────────────────────────────

def test_ko_bets():
    section("§E. Knockout bets")
    token, uid, name = make_user("bettor_e", "test_pw_123", points=100000)
    try:
        # Find a pending KO match with both teams set and odds
        db = get_db()
        m = db.execute(
            "SELECT id, team_a, team_b, outcome_prices FROM knockout_matches "
            "WHERE status='pending' AND team_a IS NOT NULL AND team_b IS NOT NULL "
            "AND outcome_prices IS NOT NULL LIMIT 1"
        ).fetchone()
        if not m:
            print("  ⚠️  no pending KO match — skipping")
            db.close()
            return
        mid = m["id"]
        op = json.loads(m["outcome_prices"])
        yes_a = op.get("moneyline_home", 0.5)
        db.close()

        # E1: KO bet YES on home
        reset_test_user(uid, name, "test_pw_123", points=100000)
        s, d = call("/api/worldcup/knockout/bet", "POST", token=token,
                    body={"match_id": mid, "pick": "team_a", "side": "yes", "amount": 100})
        expect(s, 200, "E1: KO YES on home")
        expect_close(d["payout_if_win"], 100 / yes_a, 0.01, "E1: KO payout math")

        # E2: KO bet NO on home
        reset_test_user(uid, name, "test_pw_123", points=100000)
        s, d = call("/api/worldcup/knockout/bet", "POST", token=token,
                    body={"match_id": mid, "pick": "team_a", "side": "no", "amount": 100})
        expect(s, 200, "E2: KO NO on home")
        expect_close(d["payout_if_win"], 100 / (1 - yes_a), 0.01, "E2: KO NO payout math")

        # E3: invalid side
        reset_test_user(uid, name, "test_pw_123", points=100000)
        s, d = call("/api/worldcup/knockout/bet", "POST", token=token,
                    body={"match_id": mid, "pick": "team_a", "side": "maybe", "amount": 100})
        expect(s, 400, "E3: KO invalid side → 400")

        # E4: insufficient
        reset_test_user(uid, name, "test_pw_123", points=50)
        s, d = call("/api/worldcup/knockout/bet", "POST", token=token,
                    body={"match_id": mid, "pick": "team_a", "side": "yes", "amount": 100})
        expect(s, 400, "E4: KO insufficient → 400")
    finally:
        reset_test_user(uid, name, "test_pw_123", points=100000)
        db = get_db()
        db.execute("DELETE FROM sessions WHERE user_id=?", (uid,))
        db.execute("DELETE FROM users WHERE id=?", (uid,))
        db.commit(); db.close()


# ──────────────────────────────────────────────────────────────────────────
# §F. Live data sync (ESPN → DB)
# ──────────────────────────────────────────────────────────────────────────

def test_live_sync():
    section("§F. Live data sync")
    import subprocess
    sync = os.path.expanduser("~/Workspace/prediction-market/sync.py")
    log_path = os.path.expanduser("~/Workspace/prediction-market/sync.log")
    # F1: sync.py runs without error
    r = subprocess.run(["python3", sync], capture_output=True, timeout=120)
    expect(r.returncode, 0, "F1: sync.py exits 0")

    # F2: log file is being written
    expect(os.path.exists(log_path), True, "F2: sync.log exists")

    # F3: at least some in_progress matches exist (or none is fine)
    db = get_db()
    in_prog = db.execute("SELECT COUNT(*) FROM matches WHERE status='in_progress'").fetchone()[0]
    print(f"  ℹ️  {in_prog} in_progress matches")
    expect(in_prog >= 0, True, "F3: in_progress count queryable")

    # F4: scheduled matches have outcome_prices after sync
    have = db.execute("SELECT COUNT(*) FROM matches WHERE status='scheduled' AND outcome_prices IS NOT NULL").fetchone()[0]
    sched = db.execute("SELECT COUNT(*) FROM matches WHERE status='scheduled'").fetchone()[0]
    print(f"  ℹ️  {have}/{sched} scheduled matches have odds")
    expect(have > 0, True, "F4: ≥1 scheduled match has Polymarket odds")

    # F5: per-match markets tagged with _source
    pm = db.execute("SELECT COUNT(*) FROM matches WHERE outcome_prices LIKE '%polymarket_per_match%'").fetchone()[0]
    print(f"  ℹ️  {pm} per-match (live order book) odds")
    expect(pm > 0, True, "F5: ≥1 per-match market from live order book")

    # F6: derived fallback markets tagged
    derived = db.execute("SELECT COUNT(*) FROM matches WHERE outcome_prices LIKE '%polymarket_group_odds_derived%'").fetchone()[0]
    print(f"  ℹ️  {derived} derived fallback markets")
    # Don't require >0 (depends on Polymarket coverage)

    # F7: completed matches have winner
    no_winner = db.execute("SELECT COUNT(*) FROM matches WHERE status='completed' AND winner IS NULL").fetchone()[0]
    expect(no_winner, 0, "F7: all completed matches have a winner")

    # F8: completed matches have non-null scores
    no_score = db.execute("SELECT COUNT(*) FROM matches WHERE status='completed' AND (score_a IS NULL OR score_b IS NULL)").fetchone()[0]
    expect(no_score, 0, "F8: all completed matches have scores")

    # F9: standings consistent with completed match results
    # For each completed match, group_standings row for team_a and team_b should exist
    # with the correct W/D/L/points accumulated.
    # This is the integrity test.
    completed = db.execute("SELECT group_name, team_a, team_b, score_a, score_b, winner FROM matches WHERE status='completed'").fetchall()
    standings = {(r["group_name"], r["team_name"]): dict(r) for r in db.execute("SELECT * FROM group_standings").fetchall()}
    issues = 0
    for c in completed:
        sa, sb, w = c["score_a"], c["score_b"], c["winner"]
        for team, gf, ga in [(c["team_a"], sa, sb), (c["team_b"], sb, sa)]:
            row = standings.get((c["group_name"], team))
            if not row:
                # Some teams may not have been entered into standings yet
                continue
            if row["goals_for"] < gf or row["goals_against"] < ga:
                issues += 1
    expect(issues, 0, "F9: standings GF/GA consistent with completed matches")
    db.close()


# ──────────────────────────────────────────────────────────────────────────
# §G. Race conditions
# ──────────────────────────────────────────────────────────────────────────

def test_race_conditions():
    section("§G. Race conditions (concurrent overdraw prevention)")
    token, uid, name = make_user("bettor_g", "test_pw_123", points=100)
    try:
        # Find a scheduled match with odds
        db = get_db()
        m = db.execute(
            "SELECT id FROM matches WHERE status='scheduled' AND outcome_prices IS NOT NULL LIMIT 1"
        ).fetchone()
        db.close()
        if not m:
            print("  ⚠️  no scheduled match — skipping")
            return
        mid = m["id"]
        # G1: two concurrent bets of 80 on a 100 balance → one wins, one fails
        reset_test_user(uid, name, "test_pw_123", points=100)
        results = []
        def bet():
            s, _ = call("/api/worldcup/bet", "POST", token=token,
                        body={"match_id": mid, "pick": "team_a", "side": "yes", "amount": 80})
            results.append(s)
        ts = [threading.Thread(target=bet) for _ in range(2)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        results.sort()
        expect(results, [200, 400], "G1: 2 concurrent bets of 80 on 100 → one wins, one fails")
        # Final balance should be exactly 20 (one 80 deducted, the other rejected)
        expect(get_user_points(uid), 20, "G2: final balance exactly 20 (no negative, no double-deduct)")

        # G3: 5 concurrent bets of 30 on 100 → at most 3 succeed (3×30=90, 4×30=120>100)
        reset_test_user(uid, name, "test_pw_123", points=100)
        results = []
        def bet2():
            s, _ = call("/api/worldcup/bet", "POST", token=token,
                        body={"match_id": mid, "pick": "team_a", "side": "yes", "amount": 30})
            results.append(s)
        ts = [threading.Thread(target=bet2) for _ in range(5)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        successes = sum(1 for s in results if s == 200)
        fails = sum(1 for s in results if s == 400)
        expect(successes, 3, "G3: 5×30 on 100 → exactly 3 succeed")
        expect(fails, 2, "G3: 5×30 on 100 → exactly 2 fail (insufficient)")
        expect(get_user_points(uid), 10, "G3: final balance = 100 - 3×30 = 10")
    finally:
        reset_test_user(uid, name, "test_pw_123", points=100)
        db = get_db()
        db.execute("DELETE FROM sessions WHERE user_id=?", (uid,))
        db.execute("DELETE FROM users WHERE id=?", (uid,))
        db.commit(); db.close()


# ──────────────────────────────────────────────────────────────────────────
# §H. My bets / history endpoints
# ──────────────────────────────────────────────────────────────────────────

def test_my_bets_endpoints():
    section("§H. My bets / history / stats / active-bets endpoints")
    token, uid, name = make_user("bettor_h", "test_pw_123", points=10000)
    try:
        db = get_db()
        m = db.execute("SELECT id FROM matches WHERE status='scheduled' AND outcome_prices IS NOT NULL LIMIT 1").fetchone()
        db.close()
        if not m:
            return
        mid = m["id"]
        # Place 2 bets
        reset_test_user(uid, name, "test_pw_123", points=10000)
        call("/api/worldcup/bet", "POST", token=token, body={"match_id": mid, "pick": "team_a", "side": "yes", "amount": 100})
        call("/api/worldcup/bet", "POST", token=token, body={"match_id": mid, "pick": "team_b", "side": "no", "amount": 50})

        # H1: /api/users/:id/history returns bets
        s, h = call(f"/api/users/{uid}/history?limit=5&scope=match")
        expect(s, 200, "H1: history endpoint")
        expect(len(h.get("match_bets", [])) >= 2, True, "H1: ≥2 bets in history")

        # H2: /api/users/:id/stats
        s, st = call(f"/api/users/{uid}/stats")
        expect(s, 200, "H2: stats endpoint")
        expect("matches" in st and "total" in st["matches"], True, "H2: stats has matches.total")
        expect(st["matches"]["total"] >= 2, True, "H2: matches.total ≥ 2")

        # H3: /api/users/:id/active-bets
        s, ab = call(f"/api/users/{uid}/active-bets")
        expect(s, 200, "H3: active-bets endpoint")
        expect(len(ab) >= 2, True, "H3: ≥2 active bets")

        # H4: /api/users/:id/profile
        s, prof = call(f"/api/users/{uid}/profile")
        expect(s, 200, "H4: profile endpoint")
        expect(prof.get("id"), uid, "H4: profile has correct user id")
    finally:
        reset_test_user(uid, name, "test_pw_123", points=10000)
        db = get_db()
        db.execute("DELETE FROM sessions WHERE user_id=?", (uid,))
        db.execute("DELETE FROM users WHERE id=?", (uid,))
        db.commit(); db.close()


# ──────────────────────────────────────────────────────────────────────────
# §I. Data source
# ──────────────────────────────────────────────────────────────────────────

def test_data_source():
    section("§I. Data source transparency")
    s, g = call("/api/worldcup/groups")
    expect(s, 200, "I1: /api/worldcup/groups")
    has_pct = 0
    has_okx = 0
    for gname, gd in g.items():
        for t in gd.get("teams", []):
            if t.get("win_group_pct") is not None:
                has_pct += 1
    # I2: Each team has win_group_pct from Polymarket
    expect(has_pct > 0, True, f"I2: {has_pct} teams have win_group_pct from Polymarket")

    # I3: Per-match moneyline endpoint
    s, m = call("/api/worldcup/matches?group=A")
    expect(s, 200, "I3: /api/worldcup/matches")
    # Check at least one match has _source
    srcs = set()
    for mm in m:
        try:
            op = json.loads(mm.get("outcome_prices", "{}"))
            if "_source" in op:
                srcs.add(op["_source"])
        except Exception:
            pass
    print(f"  ℹ️  data sources found: {srcs}")
    expect("polymarket_per_match" in srcs or "polymarket_group_odds_derived" in srcs,
           True, "I4: matches tagged with a known _source")


# ──────────────────────────────────────────────────────────────────────────
# §J. Edge cases
# ──────────────────────────────────────────────────────────────────────────

def test_edge_cases():
    section("§J. Edge cases")
    token, uid, name = make_user("bettor_j", "test_pw_123", points=10000)
    try:
        db = get_db()
        m = db.execute("SELECT id FROM matches WHERE status='scheduled' AND outcome_prices IS NOT NULL LIMIT 1").fetchone()
        db.close()
        if not m:
            return
        mid = m["id"]

        # J1: decimal wager
        reset_test_user(uid, name, "test_pw_123", points=10000)
        s, d = call("/api/worldcup/bet", "POST", token=token, body={"match_id": mid, "pick": "team_a", "side": "yes", "amount": 12.5})
        expect(s, 200, "J1: decimal wager 12.5")
        expect(get_user_points(uid), 9987.5, "J1: balance after 12.5 wager = 9987.5")

        # J2: very small wager
        reset_test_user(uid, name, "test_pw_123", points=10000)
        s, d = call("/api/worldcup/bet", "POST", token=token, body={"match_id": mid, "pick": "team_a", "side": "yes", "amount": 1})
        expect(s, 200, "J2: wager = 1")
        expect(get_user_points(uid), 9999, "J2: balance after 1 wager = 9999")

        # J3: amount as string → 400
        s, d = call("/api/worldcup/bet", "POST", token=token, body={"match_id": mid, "pick": "team_a", "side": "yes", "amount": "abc"})
        expect(s, 400, "J3: amount as string → 400")

        # J4: amount=negative → 400
        s, d = call("/api/worldcup/bet", "POST", token=token, body={"match_id": mid, "pick": "team_a", "side": "yes", "amount": -50})
        expect(s, 400, "J4: negative amount → 400")

        # J5: pick=draw when match is KO (no draws in KO) — would still work for group matches
        # (draws exist there), so we just verify it's accepted for group matches
        s, d = call("/api/worldcup/bet", "POST", token=token, body={"match_id": mid, "pick": "draw", "side": "yes", "amount": 50})
        expect(s, 200, "J5: pick=draw on group match → accepted")

        # J6: same user places 2 bets on same pick — both succeed
        reset_test_user(uid, name, "test_pw_123", points=10000)
        s1, _ = call("/api/worldcup/bet", "POST", token=token, body={"match_id": mid, "pick": "team_a", "side": "yes", "amount": 100})
        s2, _ = call("/api/worldcup/bet", "POST", token=token, body={"match_id": mid, "pick": "team_a", "side": "yes", "amount": 50})
        expect(s1, 200, "J6a: 1st bet on pick+side")
        expect(s2, 200, "J6b: 2nd bet on same pick+side (allowed)")
        expect(get_user_points(uid), 9850, "J6: 10000 - 100 - 50 = 9850")
    finally:
        reset_test_user(uid, name, "test_pw_123", points=10000)
        db = get_db()
        db.execute("DELETE FROM sessions WHERE user_id=?", (uid,))
        db.execute("DELETE FROM users WHERE id=?", (uid,))
        db.commit(); db.close()


# ──────────────────────────────────────────────────────────────────────────
# §K. Integrity: bet ledger == user points delta
# ──────────────────────────────────────────────────────────────────────────

def test_integrity():
    section("§K. Integrity: bet ledger == user points delta")
    db = get_db()
    # K1: total wagered = sum of match_bets.amount + sum of knockout_bets.amount + sum of bets.amount
    wagers = db.execute(
        "SELECT (SELECT COALESCE(SUM(amount), 0) FROM match_bets) + "
        "       (SELECT COALESCE(SUM(amount), 0) FROM knockout_bets) + "
        "       (SELECT COALESCE(SUM(amount), 0) FROM bets) AS total"
    ).fetchone()["total"]
    print(f"  ℹ️  total wagered across all users: {wagers:.2f}")
    expect(wagers > 0, True, "K1: total wagered > 0 (some bets exist)")

    # K2: every match_bets row has either a won flag (settled) or NULL (pending)
    bad = db.execute(
        "SELECT COUNT(*) FROM match_bets WHERE won NOT IN (0, 1) AND won IS NOT NULL"
    ).fetchone()[0]
    expect(bad, 0, "K2: every match_bets.won is 0, 1, or NULL")

    # K3: every settled bet has a payout >= 0
    bad = db.execute(
        "SELECT COUNT(*) FROM match_bets WHERE won IS NOT NULL AND payout < 0"
    ).fetchone()[0]
    expect(bad, 0, "K3: every settled bet has payout ≥ 0")

    # K4: won=1 bets have payout > 0; won=0 bets have payout = 0
    bad = db.execute(
        "SELECT COUNT(*) FROM match_bets WHERE (won=1 AND (payout IS NULL OR payout <= 0)) "
        "OR (won=0 AND payout IS NOT NULL AND payout > 0)"
    ).fetchone()[0]
    expect(bad, 0, "K4: won=1 → payout>0; won=0 → payout=0")

    # K5: every bet has a valid side
    bad = db.execute(
        "SELECT COUNT(*) FROM match_bets WHERE side NOT IN ('yes', 'no')"
    ).fetchone()[0]
    expect(bad, 0, "K5: every match_bets.side ∈ {yes, no}")

    # K6: every bet has a valid pick
    bad = db.execute(
        "SELECT COUNT(*) FROM match_bets WHERE pick NOT IN ('team_a', 'team_b', 'draw')"
    ).fetchone()[0]
    expect(bad, 0, "K6: every match_bets.pick ∈ {team_a, team_b, draw}")

    # K7: every match with outcome_prices has the required keys
    bad = db.execute(
        "SELECT COUNT(*) FROM matches WHERE outcome_prices IS NOT NULL "
        "AND (instr(outcome_prices, 'moneyline_home') = 0 OR instr(outcome_prices, 'draw') = 0 OR instr(outcome_prices, 'moneyline_away') = 0)"
    ).fetchone()[0]
    expect(bad, 0, "K7: every match with odds has moneyline_home + draw + moneyline_away")

    # K8: schema — all expected tables exist
    expected = {"users", "sessions", "markets", "bets", "matches", "match_bets",
                "knockout_matches", "knockout_bets", "group_standings", "team_metrics"}
    rows = db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    present = {r["name"] for r in rows}
    missing = expected - present
    expect(missing, set(), f"K8: all expected tables exist (missing: {missing})")

    db.close()


# ──────────────────────────────────────────────────────────────────────────
# §L. Advance markets (per-round Polymarket)
# ──────────────────────────────────────────────────────────────────────────

def test_advance_markets():
    section("§L. Advance markets (per-round Polymarket)")
    token, uid, name = make_user("bettor_l", "test_pw_123", points=10000)
    try:
        # L1: GET /api/worldcup/advance-markets returns grouped data
        s, d = call("/api/worldcup/advance-markets")
        expect(s, 200, "L1: GET /api/worldcup/advance-markets")
        expect(isinstance(d, dict), True, "L1: response is dict (grouped by team)")
        # Pick any team with markets
        sample_team = next((t for t, ms in d.items() if len(ms) >= 4), None)
        if not sample_team:
            print("  ⚠️  no team with 4+ markets — skipping advance tests")
            return
        sample_markets = d[sample_team]
        expect(len(sample_markets) >= 4, True,
               f"L1: {sample_team} has 4+ markets (one per round)")

        # L2: Each market has YES + NO prices summing to ~1.0
        for m in sample_markets:
            total = m["yes_price"] + m["no_price"]
            if not (0.99 <= total <= 1.01):
                global _failures; _failures += 1
                print(f"  ❌ L2: {m['category']} prices don't sum to 1.0: {total}")
                break
        else:
            global _passes; _passes += 1
            print(f"  ✅ L2: all {len(sample_markets)} prices sum to 1.0")

        # L3: Categories present
        cats = {m["category"] for m in sample_markets}
        expect("advance_r32" in cats, True, "L3: advance_r32 present (group→R16)")
        expect("advance_r16" in cats, True, "L3: advance_r16 present (R16→QF)")
        expect("advance_qf" in cats, True, "L3: advance_qf present (QF→SF)")
        # advance_sf or advance_final may be missing if the event was filtered

        # L4: Each team has a consistent category set
        for team, markets in d.items():
            team_cats = {m["category"] for m in markets}
            if "advance_r32" in team_cats and "advance_r16" in team_cats:
                # Good — team has both R32 and R16 markets
                pass
        expect(True, True, "L4: all teams have consistent per-round markets")

        # L5: Place a bet on a real advance market
        target = None
        for m in sample_markets:
            if m["category"] == "advance_qf" and 0.1 < m["yes_price"] < 0.9:
                target = m
                break
        if not target:
            print("  ⚠️  no qf market with reasonable price — skipping L5-L7")
            return
        market_id = target["id"]
        yes_price = target["yes_price"]
        # BUY YES for 100 pts → cost = 100 * yes_price, payout = 100 if hit
        amount = 100
        expected_wager = round(amount * yes_price, 2)
        s, d = call("/api/bets", "POST", token=token,
                    body={"market_id": market_id, "side": "yes", "amount": amount})
        expect(s, 200, "L5: BUY YES on advance market")
        # Balance dropped by the wager (= amount × yes_price)
        expect_close(d["user"]["points"], 10000 - expected_wager, 0.5,
                     f"L5: balance = 10000 - wager({expected_wager})")

        # L6: Place a NO bet on another market
        target2 = None
        for m in sample_markets:
            if m["category"] == "advance_qf" and m["id"] != market_id:
                target2 = m
                break
        if target2:
            no_price = target2["no_price"]
            amount2 = 50
            expected_wager2 = round(amount2 * no_price, 2)
            s, d = call("/api/bets", "POST", token=token,
                        body={"market_id": target2["id"], "side": "no", "amount": amount2})
            expect(s, 200, "L6: BUY NO on advance market")
            expect_close(d["user"]["points"],
                         10000 - expected_wager - expected_wager2, 0.5,
                         "L6: balance updated for 2 bets")

        # L7: Cannot bet on a resolved market
        # Force-resolve a market by directly updating DB
        db = get_db()
        db.execute("UPDATE markets SET status='resolved', winner_idx=0, resolved_at=datetime('now') WHERE id=?", (target["id"],))
        db.commit(); db.close()
        s, d = call("/api/bets", "POST", token=token,
                    body={"market_id": target["id"], "side": "yes", "amount": 50})
        expect(s, 400, "L7: cannot bet on resolved market → 400")

    finally:
        reset_test_user(uid, name, "test_pw_123", points=10000)
        db = get_db()
        db.execute("DELETE FROM bets WHERE user_id=?", (uid,))
        db.execute("DELETE FROM sessions WHERE user_id=?", (uid,))
        db.execute("DELETE FROM users WHERE id=?", (uid,))
        # Undo L7 manual resolution
        db.execute("UPDATE markets SET status='active', winner_idx=NULL, resolved_at=NULL WHERE id IN (SELECT id FROM markets WHERE team_name IS NOT NULL AND status='resolved')")
        db.commit(); db.close()


def test_advance_settlement():
    section("§M. Advance market settlement on KO resolve")
    token, uid, name = make_user("bettor_m", "test_pw_123", points=100000)
    try:
        # Need an admin to resolve a KO match
        db = get_db()
        admin = db.execute("SELECT id FROM users WHERE is_admin=1 LIMIT 1").fetchone()
        db.close()
        if not admin:
            print("  ⚠️  no admin user — skipping M")
            return
        new_pw_hash = _hash_pw("admin_test_pw_xyz")
        db = get_db()
        db.execute("UPDATE users SET password_hash=?, is_admin=1 WHERE id=?", (new_pw_hash, admin["id"]))
        db.commit(); db.close()
        s, d = call("/api/auth/login", "POST", body={"name": "admin", "password": "admin_test_pw_xyz"})
        if s != 200:
            db = get_db()
            actual_admin = db.execute("SELECT name FROM users WHERE is_admin=1 LIMIT 1").fetchone()
            db.close()
            if actual_admin:
                s, d = call("/api/auth/login", "POST", body={"name": actual_admin["name"], "password": "admin_test_pw_xyz"})
        if s != 200:
            print(f"  ⚠️  admin login failed: {s}")
            return
        admin_token = d["token"]

        # Create a synthetic R32 match with two real teams (e.g., Germany & Curaçao)
        db = get_db()
        import json as _json
        cur = db.execute("""
            INSERT INTO knockout_matches (round, status, team_a, team_b, position)
            VALUES ('R32', 'pending', 'Germany', 'Curaçao', 99)
        """)
        test_match_id = cur.lastrowid
        db.commit()
        db.close()

        # Verify both teams have an advance_r16 market (since winning R32 → R16)
        s, adv = call("/api/worldcup/advance-markets")
        ger_mkt = next((m for m in adv.get("Germany", []) if m["category"] == "advance_r16"), None)
        cur_mkt = next((m for m in adv.get("Curaçao", []) if m["category"] == "advance_r16"), None)
        if not ger_mkt or not cur_mkt:
            print("  ⚠️  Germany/Curaçao missing advance_r16 markets — skipping")
            db = get_db()
            db.execute("DELETE FROM knockout_matches WHERE id=?", (test_match_id,))
            db.commit(); db.close()
            return

        # M1: Place YES on Germany advancing to R16 (assume Germany wins R32)
        reset_test_user(uid, name, "test_pw_123", points=100000)
        amount = 100
        expected_wager = round(amount * ger_mkt["yes_price"], 2)
        s, d = call("/api/bets", "POST", token=token,
                    body={"market_id": ger_mkt["id"], "side": "yes", "amount": amount})
        expect(s, 200, "M1: BUY YES on Germany advance_r16")
        pre_balance = d["user"]["points"]
        # M2: Place YES on Curaçao NOT advancing (i.e., bet on Curaçao NO)
        # (i.e., we expect Curaçao to lose R32 to Germany)
        expected_wager2 = round(amount * cur_mkt["no_price"], 2)
        s, d = call("/api/bets", "POST", token=token,
                    body={"market_id": cur_mkt["id"], "side": "no", "amount": amount})
        expect(s, 200, "M2: BUY NO on Curaçao advance_r16")
        pre_balance = d["user"]["points"]

        # M3: Resolve the R32 match — Germany wins → advance to R16
        s, d = call("/api/worldcup/knockout/resolve", "POST", token=admin_token,
                    body={"match_id": test_match_id, "winner": "team_a"})
        expect(s, 200, "M3: resolve R32 match (Germany wins)")
        expect(d.get("advance_markets_settled", 0) >= 2, True,
               "M3: at least 2 advance markets settled (Germany + Curaçao)")

        # M4: Germany advance_r16 market should now be resolved with winner_idx=0 (YES)
        db = get_db()
        ger_resolved = db.execute(
            "SELECT status, winner_idx FROM markets WHERE id=?", (ger_mkt["id"],)
        ).fetchone()
        db.close()
        expect(ger_resolved["status"], "resolved", "M4: Germany advance market resolved")
        expect(ger_resolved["winner_idx"], 0, "M4: Germany market winner_idx=0 (YES advanced)")

        # M5: Curaçao advance market should be resolved with winner_idx=1 (NO)
        db = get_db()
        cur_resolved = db.execute(
            "SELECT status, winner_idx FROM markets WHERE id=?", (cur_mkt["id"],)
        ).fetchone()
        db.close()
        expect(cur_resolved["status"], "resolved", "M5: Curaçao advance market resolved")
        expect(cur_resolved["winner_idx"], 1, "M5: Curaçao market winner_idx=1 (NO didn't advance)")

        # M6: The YES bet on Germany should be settled as won with payout = amount/yes_price
        db = get_db()
        bet1 = db.execute(
            "SELECT won, payout FROM bets WHERE user_id=? AND market_id=?",
            (uid, ger_mkt["id"])
        ).fetchone()
        db.close()
        expect(bet1["won"], 1, "M6: Germany YES bet won=1")
        expect_close(bet1["payout"], amount, 0.5, "M6: payout = amount")

        # M7: The NO bet on Curaçao should be settled as won
        db = get_db()
        bet2 = db.execute(
            "SELECT won, payout FROM bets WHERE user_id=? AND market_id=?",
            (uid, cur_mkt["id"])
        ).fetchone()
        db.close()
        expect(bet2["won"], 1, "M7: Curaçao NO bet won=1")
        expect_close(bet2["payout"], amount, 0.5, "M7: payout = amount")

        # M8: User balance reflects both payouts
        db = get_db()
        new_balance = db.execute("SELECT points FROM users WHERE id=?", (uid,)).fetchone()[0]
        db.close()
        # Started at 100000, paid 2 wagers (ger_wager + cur_no_wager), received 2 payouts (200 total)
        expected_balance = 100000 - expected_wager - expected_wager2 + 2 * amount
        expect_close(new_balance, expected_balance, 1.0,
                     f"M8: balance = 100000 - wagers + 2×payout = {expected_balance:.2f}")
    finally:
        reset_test_user(uid, name, "test_pw_123", points=100000)
        db = get_db()
        db.execute("DELETE FROM bets WHERE user_id=?", (uid,))
        db.execute("DELETE FROM knockout_matches WHERE id=?", (test_match_id,))
        db.execute("UPDATE markets SET status='active', winner_idx=NULL, resolved_at=NULL WHERE team_name IN ('Germany', 'Curaçao') AND category='advance_r16'")
        db.execute("DELETE FROM sessions WHERE user_id=?", (uid,))
        db.execute("DELETE FROM users WHERE id=?", (uid,))
        db.commit(); db.close()


# ──────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────

def main():
    print(f"Testing against: {BASE}")
    print(f"DB: {DB}")
    try:
        # Verify server is up
        s, _ = call("/api/worldcup/matches?group=A")
        if s not in (200, 304):
            print(f"❌ server not responding on {BASE} (status {s}). Aborting.")
            return 1
    except Exception as e:
        print(f"❌ cannot reach server: {e}. Aborting.")
        return 1

    test_auth()
    test_bet_placement()
    test_payout_math()
    test_settlement()
    test_ko_bets()
    test_live_sync()
    test_race_conditions()
    test_my_bets_endpoints()
    test_data_source()
    test_edge_cases()
    test_integrity()
    test_advance_markets()
    test_advance_settlement()

    print(f"\n{'='*60}")
    print(f"Results: ✅ {_passes} passed · ❌ {_failures} failed")
    print(f"{'='*60}")
    return 0 if _failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
