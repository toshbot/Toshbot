# Tosh Bot — Phase 1: Live token detection

Detects new Pump.fun token launches via the PumpPortal WebSocket, validates and de-duplicates them,
stores them in **Firebase Firestore**, and serves them over a FastAPI REST API.
**No security analysis, scoring or trading exists yet.** Every stored token has status `INSUFFICIENT_DATA`
(fail closed) until Phase 2 builds the security engine.

## Run locally
```bash
python3.11+ -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # then edit it
uvicorn app.main:app --reload
```

### Firebase setup (needed for persistent storage)
1. Firebase console → create a project → Build → **Firestore Database** → create (production mode).
2. Project settings → Service accounts → **Generate new private key**. Save the JSON outside the repo.
3. In `.env` set `FIREBASE_PROJECT_ID` and `GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json`.
4. Never commit the key (`.gitignore` already excludes common names).

No credentials yet? Either use the Firestore emulator (`firebase emulators:start --only firestore`, then
`export FIRESTORE_EMULATOR_HOST=localhost:8080`) or explicitly set `STORAGE_BACKEND=memory` (not persistent).
The app refuses to start on Firestore without credentials; it never silently falls back.

## Verify
```bash
pytest -q                                  # unit tests (no network, synthetic fixtures)
curl localhost:8000/health
curl -H "X-API-Key: <API_KEY>" "localhost:8000/api/tokens/recent?limit=20"
curl -H "X-API-Key: <API_KEY>" localhost:8000/api/system/health
```
Live check: start the app, then watch the logs for `connected and subscribed` and `token detected`.
`/api/system/health` shows `pumpportal_ws` status, `metrics.parse_errors` (should stay near 0), queue depth and
database reachability. A feed that is connected but silent for `FEED_STALE_AFTER_SECONDS` shows as `stale`.

Optional emulator test: `FIRESTORE_EMULATOR_HOST=localhost:8080 pytest tests/test_firestore_integration.py`.

## Firestore collections
`tokens` (doc id = mint, created atomically so duplicates are impossible), `token_launch_events`
(raw events + receipt time), `analysis_runs` (one pending job per mint).
Tokens are ordered by `detected_at` (single-field index is automatic).

## Known limitations / to verify
- **Not yet tested against the live PumpPortal feed.** The message format (`txType: "create"`, `mint`,
  `traderPublicKey`, ...) and the `subscribeNewToken` call are from memory of their public docs. The spec links the
  *Bonk.fun* page; confirm the Pump.fun data-API page and check `parse_errors` on first run.
- Firestore is not relational and has no migrations. The spec's SQL design (joins, Alembic) is replaced by
  collections plus a `schema_version` field; the `Repository` interface keeps storage swappable.
- Firestore bills per read/write. Each launch costs about 3 writes; Pump.fun launch volume is high, so check the
  current free-tier quota and pricing before leaving it running.
- Queue overflow drops events (counted in `metrics.dropped_queue_full`, logged) rather than blocking the socket.
- Firebase Hosting / Cloud Functions cannot run a long-lived WebSocket listener. Host the backend on something
  that supports persistent processes (small VPS, Railway/Render/Fly paid tier). Free tiers may sleep.
- If the process is down, launches during the outage are missed (the feed has no replay).
