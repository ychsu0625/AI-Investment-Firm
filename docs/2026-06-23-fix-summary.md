# SIM Tool Team 修復小結 — 2026-06-23

---

## P0 — backend 啟動後全部 API 卡死

**根因**：`get_api()` 非執行緒安全，多個背景/請求 thread 同時觸發 Shioaji login → 重複抓 contracts 5 次 → GIL 長期佔住 → uvicorn threadpool 耗盡，連 `/api/health` 都排不進去。

**修法**（三合一）：

| 修改 | 效果 |
|------|------|
| `get_api()` 加 `threading.Lock()` double-check locking | 消除競態，Shioaji 只 login 一次 |
| `if __name__` 啟動時預先呼叫 `get_api()` | uvicorn 開始接請求前 Shioaji 已 ready，首批 HTTP 不阻塞 |
| Token 改「有就沿用、沒有才產生」 | 重啟不換 token，前端/腳本不需重讀 |

**驗證**：重啟後 health 0.3 秒回 200。

---

## ^TWII 只有 30 根 K 線

**根因**：index 資料被預填到錯誤路徑（`smart-investment-monitor/market.db`），正確路徑是 `ui/data/market.db`。

**修法**：重新灌 10 年資料到正確 DB。

**結果**：`^TWII` 2539 根（2016~2026）、`^VIX` 2632 根、`^SOX/^GSPC/^IXIC` 各 2500+。

---

## kbars 預設輸出仍是 6 個月

**修法**：日 K 無帶參數時自動設 `effective_days=1825`（5 年），走 market.db 長期庫。

**結果**：`kbars/2330` 預設回 855 根（原 115）、`kbars/^TWII` 預設回 1813 根。

---

## 本輪已完成清單（含上一輪）

| 項目 | 狀態 |
|------|------|
| S-00b kbars `limit`/`start_date`/`end_date` | ✅ |
| S-00 K 線預設 5 年 | ✅ |
| H-01/H-02/H-03 指數歷史（^TWII/^VIX/^SOX/^GSPC/^IXIC） | ✅ 各 2500+ 根 |
| S-02 回測 CI（Bootstrap 95%） | ✅ `win_rate_ci95` / `avg_return_ci95` |
| U-01/U-02 Universe 清單 | ✅ TW 199 / US 269 |
| B-03 `_call_claude_analysis` 未定義 | ✅ |
| R-01 報告中心自動掃資料夾 | ✅ filesystem 為主、MANIFEST 為輔 |
| backend hang（本輪 P0） | ✅ 鎖 + 預登入 + token 沿用 |
