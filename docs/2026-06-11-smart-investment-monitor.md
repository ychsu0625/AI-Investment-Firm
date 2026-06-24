# Smart Investment Monitor — Session 2026-06-11

## Project Location
`C:\Users\ychsu\Documents\Claude_Files\smart-investment-monitor\ui\`

## How to Run
```bash
cd C:\Users\ychsu\Documents\Claude_Files\smart-investment-monitor\ui
python backend.py
# Open browser: http://localhost:8765  (NOT file:// — CORS restricted)
```

## Architecture
- **Backend**: `backend.py` — FastAPI + uvicorn, port 8765, bound to 127.0.0.1
- **Frontend**: `index.html` — SPA served by FastAPI `GET /`, TradingView Lightweight Charts
- **DB**: `monitor.db` (SQLite) — positions, watchlist, signals, risk_config, strategy_config, kbar_cache, chip_snapshot, news_cache
- **Market DB**: `market.db` — daily_kbar, backtest_result

## Security Constraints (MUST follow)
- NEVER hardcode Shioaji API keys — load from: env vars, `.env`, or `~/ai-investment-system/config.yaml`
- SIMULATION=True by default; live only when SJ_PRODUCTION=true
- EXIT_D cannot be disabled (force_enabled=True)
- Webhook URLs must be https:// (SSRF guard)

## Changes Made This Session

### 1. CRITICAL Security Hardening (Codex adversarial review fix)
- **CORS**: `allow_origins=["*"]` → `["http://localhost:8765", "http://127.0.0.1:8765"]`
- **Bind**: `0.0.0.0` → `127.0.0.1` (localhost only)
- **API Token**: Per-startup random 64-char token via `secrets.token_hex(32)`
  - `GET /api/auth/token` — frontend fetches token on load
  - `require_token` FastAPI Depends on all mutating endpoints:
    - `/api/positions` (POST/PUT/DELETE)
    - `/api/risk-config` (POST)
    - `/api/macro-lock/{state}` (POST)
    - `/api/auto-sell/execute` (POST)
    - `/api/auto-sell/toggle/{state}` (POST)
    - `/api/strategies/{sid}/toggle` (PUT)
    - `/api/strategies/{sid}/params` (PUT)
  - Frontend sends `X-API-Token` header via `authHeaders()` helper
- **`GET /`** serves `index.html` so frontend origin = `http://localhost:8765`
- `.api_token` and `.env` added to `.gitignore`

### 2. HIGH Auto-sell Idempotency Fix
- `_execute_sell_order(pos_id, code, shares, reason)` rewritten:
  - Marks position `pending_sell` BEFORE submitting order (atomic CAS: `WHERE id=? AND status='open'`)
  - Concurrent duplicate calls get `rowcount=0` → skip
  - Production: records `sell_order_id`, stays `pending_sell` awaiting fill callback
  - Simulation: directly sets `closed`
  - On failure: rolls back to `open`
- DB migration: `positions` table gained `sell_order_id TEXT`, `sell_reason TEXT`
- `auto_sell_execute` only scans `status='open'` (pending_sell excluded)

### 3. US Stock K-line Fix (from prior session, verified working)
- `us_kbars()` endpoint now uses shared `_build_kbar_response()` — same JSON shape as TW
- Search box auto-detects market: `[A-Z]{1,5}` → US, `\d{4,6}` → TW

### 4. Manual Scan + After-Hours Review
- **`GET /api/scan/after-hours`**: Runs signal engine on all watchlist+positions using closing snapshot, returns per-stock summary (close, MA5/MA20 position, MACD direction/cross, vol_ratio, PnL if held)
- **Frontend buttons** (home page, signal card area):
  - "手動掃描" — calls `/api/scan/signals` bypassing 09:00-13:35 time check
  - "盤後回顧" — calls after-hours endpoint, renders summary table
  - "補齊K線" — calls `/api/kbars/warm-up`
- `scanSignals()` refactored: time check in wrapper, actual logic in `_doScanSignals()`
- Empty signal message shows hint: "盤後可點手動掃描或盤後回顧"

### 5. K-bar Cache Warm-up
- **`POST /api/kbars/warm-up`**: Bulk-fetches daily K for all watchlist+positions (500 days), skips if already ≥240 bars cached
- Frontend "補齊K線" button with progress feedback

### 6. Strategy Persistence
- New DB table `strategy_config(strategy_id, enabled, params, updated_at)`
- `_load_strategy_config()` called at startup — merges saved enabled/params into STRATEGIES
- `toggle_strategy()` now writes to DB
- **New endpoint `PUT /api/strategies/{sid}/params`** — saves custom param values
- Frontend strategy detail shows current (not default) values, "儲存參數" button
- EXIT_D remains force_enabled regardless of DB state

### 7. Startup Encoding Fix (from prior session)
- UTF-8 stdout wrapper for Windows cp950 terminals

## Current Status
- Backend: running on port 8765, simulation mode
- All 4 watchlist stocks (2330, 2317, 2454, 2382) have full kbar cache (330+ bars)
- After-hours review working with MA/MACD/vol_ratio for all stocks
- Token auth verified: no-token → 403, with-token → 200

## Known Issues / TODO
- **Fill callback**: Production auto-sell marks `pending_sell` but no Shioaji order_deal_event callback wired yet to confirm fill → close position. Need to register callback in `get_api()`.
- **PATH**: `node`/`codex` not on Windows PATH — `/codex:` commands need full paths
- **Multiple Python processes**: User has stale http.server processes (PID 33956, 37496, 43964) — harmless but could be cleaned up
- Strategy params not yet wired into actual signal engine logic (engine still uses hardcoded values, not `p.get("value", p["default"])`)

## File Inventory
| File | Lines | Role |
|------|-------|------|
| `ui/backend.py` | ~3500 | FastAPI backend, all APIs |
| `ui/index.html` | ~2500 | SPA frontend |
| `ui/monitor.db` | — | SQLite main DB |
| `ui/market.db` | — | Backtest/daily kbar DB |
| `.gitignore` | 7 | Excludes .db, .env, .api_token |
