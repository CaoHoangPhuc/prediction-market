# Prediction Market — FIFA World Cup 2026

Local virtual betting app for friends. Polymarket-backed odds, OKX tournament
win %, ESPN live scores, per-user points ledger, atomic bet placement.

## Stack
- **Backend**: FastAPI + SQLite + uvicorn (single-file `server.py`, ~4600 lines)
- **Sync**: `sync.py` (full + live tick) + `pm-live-tick.py` (live only)
- **Frontend**: vanilla JS + Python-rendered HTML (single page, no framework)
- **Schedulers**: `launchd` jobs (full sync 30s, live tick 60s)
- **Auth**: PBKDF2-SHA256, server-side sessions, bearer tokens

## Endpoints (high level)
- `GET  /` — markets list (Polymarket-backed)
- `GET  /worldcup` — WC2026 groups + matches + knockout + leaderboard
- `POST /api/auth/{register,login,logout}` — auth
- `GET  /api/auth/me` — current user
- `POST /api/worldcup/bet` — place group-stage bet
- `POST /api/worldcup/knockout/{bet,set-team,resolve}` — KO stage
- `GET  /api/worldcup/{groups,matches,standings,knockout,leaderboard}`
- `GET  /api/users/{id}/{profile,active-bets,history,stats}`

## Run locally
```bash
# 1. Dependencies
pip install fastapi uvicorn

# 2. Start server (auto-reload off in production)
python3 server.py
# → http://localhost:18081

# 3. Sync (one-shot)
python3 sync.py

# 4. Live tick only (every 60s)
python3 pm-live-tick.py
```

## Deploy (macOS, persistent)
```bash
# Load launchd jobs
cp ~/Library/LaunchAgents/com.prediction-market.{dashboard,sync,live}.plist /tmp/  # not in repo
launchctl load ~/Library/LaunchAgents/com.prediction-market.dashboard.plist
launchctl load ~/Library/LaunchAgents/com.prediction-market.sync.plist
launchctl load ~/Library/LaunchAgents/com.prediction-market.live.plist
```

## Schema
See `server.py` line ~30-150 for `CREATE TABLE` statements. Key tables:
- `users` (id, name, password_hash, points, is_admin)
- `markets` (Polymarket-derived, `category` tag for group_winner/advance_r32/...)
- `bets`, `match_bets`, `knockout_bets` (with `side`='yes'/'no' + `pick`)
- `matches` (status: scheduled/in_progress/completed, score_a/score_b, match_clock)
- `team_metrics` (OKX tournament win %)
- `knockout_matches` (32 → 16 → QF → SF → Final)

## Invariants worth knowing
- `match_bets.side='no'` means the user bets the outcome does NOT happen.
  The API's `_pick_label()` flips the label accordingly.
- `adv_market` queries use the `category` column (not LIKE on `question`)
  because Polymarket titles use "reach", not "advance".
- Live match scores are kept fresh by `pm-live-tick.py` (60s); the launchd
  `com.prediction-market.sync` does everything else (30s).
