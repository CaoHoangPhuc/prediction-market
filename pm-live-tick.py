#!/usr/bin/env python3
"""Live score tick — runs every 60s via launchd.

Does ONE thing: fetch the ESPN scoreboard and update scores/clock for any
match currently in_progress. No Polymarket, no OKX, no knockout, no
resolution. Cheap enough to run constantly.

Why a separate process: the main pm-sync.py does ~7 different work items
(Polymarket search, advance markets, OKX, ESPN full + live + resolve,
knockout projection, etc). Each full run takes 5-15s. Running it every
30s means constant churn. This script is ~1 HTTP call + 1 SQLite UPDATE
loop — well under 2s.

Idempotent: if no matches are in_progress, the UPDATE statements are
no-ops. If ESPN is down, the script fails silently (no DB writes).

Run interval: 60s. Live scores change every ~minute in real football,
so this matches the natural refresh cadence and gives the UI (which
polls /api/worldcup/matches?status=in_progress every 15s) data that's
at most 60s stale.
"""
import json
import os
import sqlite3
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone

DB_PATH  = "/Users/phuccao/Workspace/prediction-market/pm.db"
ESPN_URL = "https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/scoreboard"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


def fetch(url, timeout=10):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def main():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    # Quick check: any live match? Skip the ESPN call entirely if not.
    # (Saves an API roundtrip on the 99% of minutes when no games are on.)
    live_count = db.execute(
        "SELECT COUNT(*) AS n FROM matches WHERE status='in_progress'"
    ).fetchone()["n"]
    if live_count == 0:
        # No live matches — nothing to do. Exit quietly.
        sys.exit(0)
    try:
        espn = fetch(ESPN_URL)
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as e:
        print(f"  ⚠️ ESPN fetch failed: {e}", file=sys.stderr)
        sys.exit(1)

    updated = 0
    for e in espn.get("events", []):
        st = e.get("status", {}).get("type", {})
        state = st.get("state", "")
        desc = st.get("description", "")
        if state != "in" and "In Progress" not in desc and "Halftime" not in desc:
            continue
        # Same parsing as the main sync — keep in sync with sync.py:425-444.
        name = e.get("name", "").replace(" at ", " vs ")
        parts = name.split(" vs ")
        if len(parts) != 2:
            continue
        away, home = parts[0].strip(), parts[1].strip()
        scores = {}
        for c in e.get("competitions", []):
            for comp in c.get("competitors", []):
                raw = comp.get("score", "0")
                scores[comp.get("homeAway")] = int(raw) if str(raw).isdigit() else 0
        if "home" not in scores or "away" not in scores:
            continue
        sa, sb = scores["home"], scores["away"]
        clock = (st.get("shortDetail") or st.get("detail") or "")
        for ta, tb in [(home, away), (away, home)]:
            ta_score = sa if ta == home else sb
            tb_score = sb if ta == home else sa
            cur = db.execute(
                "UPDATE matches SET status='in_progress', score_a=?, score_b=?, match_clock=? "
                "WHERE team_a=? AND team_b=? AND status IN ('scheduled','in_progress')",
                (ta_score, tb_score, clock, ta, tb),
            )
            if cur.rowcount > 0:
                updated += 1
                # Mark completed if ESPN says HT/FT (Halftime is a pause, not
                # a finish — only mark completed on Full Time). We let the
                # main sync handle the FT transition since it also does
                # settlement.
                break
    db.commit()
    db.close()
    if updated:
        print(f"  🔴 live-tick @ {datetime.now(timezone.utc).isoformat(timespec='seconds')}: "
              f"{updated} match(es) updated")


if __name__ == "__main__":
    main()
