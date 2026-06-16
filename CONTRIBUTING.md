# Contributing

Thanks for your interest in the prediction-market app! Here's how the
branch model works and how to get your changes in.

## Branches

| Branch | Purpose | Push policy |
|---|---|---|
| `main`  | Stable, deployed | **PR only.** Protected — no direct push, no force-push, no deletion. Requires 1 review. |
| `dev`   | Active development | **Open to collaborators** — direct push welcome. The integration branch before `main`. |

## Workflow

```
1.  Fork the repo (or be added as a collaborator for direct push).
2.  Create a feature branch off `dev`:
        git checkout dev
        git pull
        git checkout -b feat/your-feature
3.  Make your changes. Test locally:
        python3 -m compileall server.py sync.py pm-live-tick.py
        python3 server.py  # smoke-test on http://localhost:18081
4.  Push to your fork (or directly to `dev` if you're a collaborator):
        git push origin feat/your-feature
5.  Open a PR from your feature branch into `dev`.
6.  After review + merge into `dev`, the maintainer promotes `dev` → `main`
    via a separate PR once it's been tested in production.
```

## Code style

- Python 3.11+ (project uses `from __future__` patterns, no walrus operator abuse)
- No framework on the frontend — vanilla JS only. Single-page worldcup.html
  inlined into `server.py:WORLDCUP_HTML` (~73KB of HTML+JS).
- All bets are server-validated; never trust the client to compute payouts.
- DB writes for user-facing state go through `BEGIN IMMEDIATE` to prevent
  double-spend on points.
- Comments in English; docstrings in English.

## What to test before pushing

- [ ] `python3 -m compileall server.py sync.py pm-live-tick.py` passes
- [ ] App loads without "Loading..." stuck on any tab (Groups, My Bets, Advance, Knockout, History, Activity, Leaderboard, Admin)
- [ ] If you touched the bet placement path, place a YES bet AND a NO bet, then check `/api/users/{id}/history` shows the right `pick_label` (e.g. "Germany NOT Win" for side=no, pick=team_a)
- [ ] If you touched the sync, leave the page open for 5 min and verify the live score updates

## Reporting bugs / requesting features

Open an issue on the `dev` branch. Include:
- What you expected
- What happened
- Browser + OS (if UI-related)
- The relevant URL + timestamp (so we can correlate with server.log)

## Security

Never commit:
- `pm.db` (user data, bet history, password hashes)
- `*.bak.*` (backup files clutter the tree)
- `server.log` / `sync.log` / `live-tick.log` (runtime)
- Any `.env` / `credentials.json`

These are all in `.gitignore`. The CI check refuses a PR that adds them.
