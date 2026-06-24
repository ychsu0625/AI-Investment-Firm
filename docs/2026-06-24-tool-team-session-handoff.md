---
aif_report: false
title: Tool Team Session Handoff（轉 session 用）
category: handoff
date: 2026-06-24
tags: [tool-team, handoff, session-transfer, S-15, S-16, base-rate, regime]
summary: 接手者必讀。本 session 完成 S-05/06/15/16 + base-rate 提速220x + 3個P0/P2修復。含後端啟動法、待辦、與一個要策略端定奪的框架門檻問題。
---

# Tool Team — Session Handoff（轉 session）

**身分**：你是 SIM（Smart Investment Monitor）的 **Tool Team**，負責後端 `backend.py`。AIF 策略團隊寫需求、你建工具。
**單一真實來源需求清單**：`C:\Users\ychsu\Documents\Claude_Files\ai-investment-firm\2026-06-23-tool-team-MASTER-requirements.md`
**最新交辦（開放項）**：`C:\Users\ychsu\Documents\Claude_Files\ai-investment-firm\2026-06-23-tool-team-handoff-open-items.md`

---

## 0. 後端基本資訊（先讀這段）

```
檔案：  C:\Users\ychsu\Documents\Claude_Files\smart-investment-monitor\ui\backend.py  (~13,300 行)
DB：    C:\Users\ychsu\Documents\Claude_Files\smart-investment-monitor\ui\data\market.db   ← 正確路徑！
        （注意：smart-investment-monitor\market.db 是【錯的】舊檔，別用）
埠：    127.0.0.1:8765
當前狀態：執行中（本 session 結束時 health 200）
```

**重啟流程（改完 backend.py 必做，且要 curl 驗證才算完成）**：
```bash
# 1) 殺舊 process（PowerShell）
$lines = netstat -ano | Select-String ":8765" | Select-String "LISTEN"; foreach ($l in $lines) { if ($l -match '(\d+)$') { Stop-Process -Id $Matches[1] -Force -ErrorAction SilentlyContinue } }

# 2) 啟動（Bash, 背景）
cd "C:\Users\ychsu\Documents\Claude_Files\smart-investment-monitor\ui" && python backend.py > /tmp/backend.log 2>&1 &
sleep 10

# 3) 驗證
curl -s -m 5 "http://localhost:8765/api/health"   # 要 {"ok":true}
```

---

## 1. 本 session 完成清單（全部已驗）

| 項目 | 內容 | 實測 |
|------|------|------|
| **P0 backend hang** | `get_api()` 加 `threading.Lock()`（double-check）+ 啟動預登入 Shioaji + token 沿用不輪替 | health 0.2~0.3s |
| **^TWII 30→2539 根** | index 資料原本灌到錯誤 market.db，重灌到 `ui/data/market.db` | ^TWII 2539、^VIX 2632、^SOX/^GSPC/^IXIC 各 2500+ |
| **kbars 預設 5 年** | 日K 無參數時 `effective_days=1825`，走 market.db 長期庫 | 2330 預設 855 根、^TWII 1813 根 |
| **S-02 回測 CI** | bootstrap 1000 次，`win_rate_ci95`/`avg_return_ci95`/`ci_n_trades` | — |
| **S-05 4-regime** | `GET /api/regime?market=TW&date=` 含完整 inputs + rule_version | TREND_UP/RISK_OFF/MEAN_REVERT/CRISIS |
| **S-06/S-16 regime 歷史** | `GET /api/regime/history` 每列 per-row inputs + summary(頻率/持續天數/切換次數/當前streak) + rule_version | 485天<1s |
| **S-15 base-rate** | `POST /api/backtest/base-rate`→job_id；`GET .../{id}`輪詢；`POST .../{id}/cancel` | 見下 |
| **D-S15-PERF2 提速** | trade-level outcome 表 + numpy 指標，compute-once | TW 199股×12策×5年 = **29秒**（原~108分），~220× |
| **D-S15-AVGRET** | summary 加 `avg_trade_return_pct`（扣成本每筆報酬%），點估計落 CI 內 | 全 12 列 point-in-CI ✓ |
| **D-S15-DEAD** | 4 死策略確認非 bug（引擎無觸發碼），從 base-rate 預設剔除 | 見下 |
| **D-S16-FIELDS** | history 補 inputs + `rule_version="4regime-v1"` | inputs 可 100% 重現 regime |

---

## 2. 關鍵技術設計（接手者要懂的）

### base-rate 快速引擎（核心，在 backend.py 約 line 12816+）
- **方法論改變**：base-rate 從「投組模擬」改成 **trade-level 事件研究**。理由：base rate 要的是 P(獲利|訊號)，投組模擬會被現金/部位污染（訊號觸發但沒現金→漏記）。
- **三個函式**：
  - `_compute_stock_features(code, all_dates, bar_data)` — 每股算一次：numpy 指標(MA/MACD/RSI/KD/量比/布林) + 「進場日→固定 EXIT_C+EXIT_D 結果」outcome 表（含扣成本）。
  - `_detect_triggers(sid, features)` — 12 個進場策略的觸發判斷，吃預算指標。
  - `_fast_base_rate_market(...)` — 每股算一次 features，各策略只查表統計。有 `progress_cb`/`should_cancel`。
- **指標連續計算**（非投組引擎的逐日視窗版），暖身期後差異可忽略；start 有 120 天 pre-buffer 吸收暖身。
- **報酬扣成本**：`_net_return_pct()` 台股手續費`TW_COMMISSION*TW_DISCOUNT`+證交稅`TW_TAX`，美股 0。台股代碼用 `_is_tw_code()`（首字為數字）判斷。
- **預設策略**：`_BASE_RATE_BUY_IMPL`（12 個有實作觸發碼的），出場固定 `["EXIT_C","EXIT_D"]`。

### 12 個有效進場策略（base-rate 預設）
`BUY_A, BUY_B, LOW_BUY, SQUEEZE_BREAK, KD_CROSS, MACD_CROSS, RSI_EXTREME, MA_ALIGN, DONCHIAN_BREAK, MA_PULLBACK, BB_SQUEEZE, VOL_BREAKOUT`

### 4 個死策略（已剔除，非 bug）
`LOCK_BUY, SQUEEZE_BUY, SENTIMENT_REVERSAL, WIFE_SIMPLE` — 回測引擎的 buy_signal 迴圈裡**根本沒有它們的觸發碼**。SQUEEZE_BUY 需融券、SENTIMENT_REVERSAL 需情緒數據（純 OHLCV 算不出），WIFE_SIMPLE 是 UI 便利策略。要接的話得另外實作觸發邏輯。

### regime 判斷樹（單一真實來源）
- `_classify_regime(vix, daily_change_pct, close, ma60, atr20, atr60, ma20)` → (regime, reason)
- S-05 當日 `_calc_regime()` 與 S-16 歷史 `_batch_calc_regime()` **都呼叫它**，確保一致。
- 版本字串 `_REGIME_RULE_VERSION = "4regime-v1"`，改規則時一併更新。
- 規則：VIX>35 或單日跌>3%→CRISIS；VIX>25 且 close<MA60→RISK_OFF；ATR20/ATR60<0.8 且 |close-MA20|/MA20<0.03→MEAN_REVERT；其餘→TREND_UP。

### 歷史陷阱（別重蹈）
- **yfinance 會卡死 uvicorn**：`yf.download()` 在 sync handler 裡會阻塞整個 event loop。指數補資料用背景 `threading.Thread(daemon=True)`，絕不 inline。
- **market.db 路徑**：`MARKET_DB_PATH = Path(__file__).parent / "data" / "market.db"` → 一定是 `ui/data/market.db`。

---

## 3. base-rate API 用法（給 AIF 或測試）

```bash
# 啟動（預設 TW+US、12策略、出場固定 EXIT_C+EXIT_D、近6年）
curl -s -X POST "http://localhost:8765/api/backtest/base-rate" \
  -H "Content-Type: application/json" -d '{"start":"2021-01-01"}'
# → {"ok":true,"job_id":"xxxx",...}

# 輪詢進度/結果
curl -s "http://localhost:8765/api/backtest/base-rate/{job_id}"
# running: {status,progress,total,current}
# done:    + rows[], thresholds, ...

# 取消
curl -s -X POST "http://localhost:8765/api/backtest/base-rate/{job_id}/cancel"
```
每列 row：`market, strategy, N, win_rate, win_rate_ci95, avg_return, avg_return_ci95, pl_ratio, max_dd, sharpe, pass, reason`
門檻 `_BASE_RATE_THRESHOLDS`：N≥30、勝率CI下界>50、盈虧比≥1.2。

---

## 4. ⚠️ 待策略端定奪（已回報，未決）— 重要

本 session 跑 TW base-rate，**12 策略全 FAIL**，全卡在同一條：**勝率95%CI下界<50%**。
但它們**盈虧比 ~2.0、扣成本平均每筆 +2~3%（正期望值）**——典型趨勢策略「大賺小賠」特徵。

現門檻「勝率CI下界>50%」會**判死所有趨勢型策略**，只放行高勝率（均值回歸型）。
**這是框架第一層的定義問題，不是 bug**。等策略端回覆方向：
1. 維持現狀（刻意只要高勝率訊號當 prior）；或
2. 加「期望值/盈虧比並行通過條件」雙軌（如 avg_return_ci 下界>0 或 pl_ratio≥1.5 且 N 夠大）。

**接手者**：策略端若回覆要雙軌，改 `_extract_row()` 的 `passed` 邏輯即可（在 base-rate 區塊）。

---

## 5. 待辦（下個 session 可接）

| 優先 | 項目 | 備註 |
|------|------|------|
| 等回覆 | base-rate 門檻雙軌（見 §4） | 策略端定奪後改 `_extract_row` |
| 未開始 | **Layer 4 Kelly 倉位規則庫**（S-11） | 本 session 問過老闆「要不要順手做」，未動工。不阻塞 base rate |
| 規劃中(P3) | S-07~S-14 Bayesian/LR/MonteCarlo | MASTER 清單標「先別動，等地基穩」 |
| P3 | R-01~R-03 報告中心 UI、V-series 呈現 | 等數據/計算到位 |
| 可選 | base-rate ④增量快取 | outcome 表天然 append-friendly，每月重跑可降到秒級 |

---

## 6. 工作紀律（從 memory，務必遵守）

- **改 backend.py → 必須重啟 + curl 驗證**，不要只改 code 就說完成。
- **驗證/測試任務一次做完**所有項目，不要做一半就報告。
- **改完 code 跑真實測試通過才回覆**，禁止 monkey patch 測試。
- **小改動不要每次跑 full test**，累積一批，且跑 full test 前先問老闆。
- **任何 GitHub push 前必須老闆批准**。
- 站 user 角度驗收（實際開瀏覽器用），不能只測 API；可寫 code 驗證但**不改 tool team 的 code**（不能球員兼裁判）。
- 給路徑/連結：單獨一行、code block 包起來可一鍵複製。
- 專家團隊主動執行，不要列選項問老闆；action item = 直接做。

---

## 7. 給接手者的第一步建議

1. 讀 MASTER 需求清單 + handoff-open-items（§開頭兩個路徑）看最新驗收狀態。
2. `curl health` 確認 backend 活著；沒活就用 §0 重啟。
3. 若策略端已回覆門檻方向 → 動 §4；否則等。
4. 若老闆要推進 → Layer 4 Kelly（§5）。
