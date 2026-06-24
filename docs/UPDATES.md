# Push History

## 2026-06-11 14:30
- **功能**：初始推送 master + feature/info-center
- **改動摘要**：
  - backend.py: 技術指標端點、watchlist 備援、signal name LEFT JOIN、auth token Depends、indicators batch
  - index.html: undo/redo (confirmEditPosition/confirmAddPosition)、watchlist column resize、null price handling
  - info_center.html: icFetch auth wrapper、XSS fix (envBannerHtml)、macro/signals/backtest endpoints
  - github_agent.py: Watch Mode + Push Mode、branch guard、_SAFE_STAGE patterns
  - version/changelog: v2.0 release
