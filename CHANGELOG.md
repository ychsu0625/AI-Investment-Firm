# Changelog

## v2.0.0 — 2026-06-11

Initial release of Smart Investment Monitor v2.0.

**Features:**
- FastAPI backend (port 8765, localhost-only, token auth)
- Taiwan stock monitoring via Shioaji 1.5.2
- US/macro data via yfinance
- AI analysis via Claude claude-sonnet-4-6
- Info Center (IC) — macro, TW market, US market, AI recommendations
- BUY signal push notifications (Telegram / Email / Webhook)
- Background macro scheduler (840s interval)
- Recommendation history with P&L tracking
- GitHub Agent — Watch Mode + Push Mode

**Security fixes applied:**
- SQL injection: parameterized queries for user-supplied market param
- SSL: CERT_NONE downgrade only on ssl.SSLError fallback
- DB connection leak: validate before opening connection
- Concurrent race: `_ic_refresh_lock` guards archive-DELETE-INSERT
