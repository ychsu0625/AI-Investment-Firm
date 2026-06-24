# QA Bug 修復計畫 — 雙方案比較與最佳執行選擇

**日期**：2026-06-22
**依據**：Phase 0 QA Report v2（25 bugs）

---

## P0 安全漏洞

### SEC-01：risk-config 回傳明文密碼

**方案 A：回傳時遮蔽敏感欄位**
- 在 `get_risk_config()` 回傳前，對 `email_pass`、`telegram_bot_token`、`smtp_password` 等欄位值替換為 `"***"`
- 優點：最小改動（3 行），前端不需改
- 缺點：敏感 key 名稱靠硬編碼 list 維護

**方案 B：拆分為兩個 endpoint**
- `/api/risk-config`（公開）只回傳非敏感設定
- `/api/risk-config/secrets`（需 token + 額外驗證）回傳敏感欄位
- 優點：架構正確，零風險
- 缺點：改動大，前端也要配合

**✅ 選 A** — 現階段遮蔽即可，系統已有 token 保護（SEC-02 修完後），不需要拆 endpoint。

---

### SEC-02：risk-config 無需 auth

**方案 A：GET 也加 `Depends(require_token)`**
- `def get_risk_config(_: None = Depends(require_token)):`
- 1 行改動

**方案 B：用中間件對所有 `/api/` 路由強制 token**
- 全局中間件 + 白名單（`/api/auth/token` 等不需要的）
- 優點：杜絕未來遺漏
- 缺點：改動大，可能影響現有不需 auth 的 endpoint

**✅ 選 A** — 精準修復。全局中間件風險太大（可能影響 snapshot 等公開 API）。

---

## P0 核心功能

### BUG-01：台股回測 0 trades

**根因分析**：已確認不是資料問題。2330 股價 ~2510/股，`lot=1000`，一張 = 2,510,000。初始資金 1,000,000，連一張都買不起（`cash * 0.1 / (price * lot) = 0.039 → 0 shares`）。2317(~200/股) 和 2454(~2000/股) 同理，0.1 倉位限制加上 lot=1000 導致計算結果為 0。

**方案 A：改用「股」計算，最後再換算成「張」**
- 把 `shares = max(1, int(cash * 0.1 / (price * lot)))` 的 lot 移除
- 買賣仍用「股」為單位，但台股最小交易單位改為 1000 股（取整到千股）
- `shares = max(1000, int(cash * 0.1 / price) // 1000 * 1000)` 台股
- `shares = max(1, int(cash * 0.1 / price))` 美股
- 優點：統一用「股」，符合實際下單邏輯
- 缺點：需改買/賣/淨值/強平 4 處，零股交易不支援

**方案 B：倉位比例自適應 + 放寬到整張**
- 台股：`max_invest = cash * position_pct`，`張數 = max(1, int(max_invest / (price * 1000)))`
- 買不起一張時跳過（但 position_pct 從 10% 提高到 30%，或動態調整）
- 美股保持現有邏輯
- 優點：保留「張」概念，台灣投資者直覺
- 缺點：高價股（台積電）可能仍觸發不了

**✅ 選 A** — 統一用「股」計算更正確。台股零股交易已普及，且回測中用「股」更精確。key change：
- 台股：`shares = max(1000, (int(cash * 0.1 / price) // 1000) * 1000)`，最少 1000 股（一張），但不夠一張時用 `max(100, int(cash * 0.1 / price))` 零股模式
- 美股：`shares = max(1, int(cash * 0.1 / price))`
- 買賣成本計算全部改用 `shares * price`（不再乘 lot）

---

### BUG-02：TW/US 推薦列表完全相同

**根因**：`ic_get_recs()` 的 SQL `SELECT ... FROM ic_recommendations ORDER BY score DESC` 沒有 `WHERE market=?` 過濾，也沒接收 query parameter。

**方案 A：加 query parameter 過濾**
- `def ic_get_recs(market: str = ""):`
- SQL 加 `WHERE market=?` 當 market 非空

**方案 B：改為兩個獨立 endpoint**
- `/api/ic/recommendations/tw` 和 `/api/ic/recommendations/us`
- 優點：URL 語義明確
- 缺點：前端要改路由

**✅ 選 A** — 加 query param 最小改動，前端已用 `?market=TW` 呼叫。

---

### BUG-03：TW/US 持倉隔離失敗

**根因**：`GET /api/positions` 回傳所有 `status='open'` 的持倉，沒有 market 過濾。AAPL 的 market 欄位可能存為 TW（歷史資料問題）。

**方案 A：API 加 market filter + 清理髒資料**
- `get_positions(market: str = "")` 加 `WHERE market=?`
- 一次性 SQL 修正 AAPL 等明顯美股的 market 欄位

**方案 B：只清理資料，不改 API**
- 修正 DB 裡 AAPL 的 market='US'，刪除 ZZZZ 測試數據
- 優點：不改 code
- 缺點：未來仍可能再出現

**✅ 選 A** — 加 filter 才是根本解。髒資料也一起清。

---

### BUG-04：台股持倉 current_price 全 0

**根因**：`GET /api/positions` 回傳的資料沒有 `current_price` 欄位 — DB 裡 positions 表沒有這個欄位，需要即時查詢。

**方案 A：在 `get_positions()` 回傳時即時查價**
- 遍歷結果，用 `_get_ohlcv_from_cache` 或 snapshot 取最新收盤價
- 計算 `pnl_pct = (current_price - cost) / cost * 100`
- 優點：每次都是最新價
- 缺點：多支股票時較慢

**方案 B：背景定時更新 positions 表的 current_price 欄位**
- 加欄位 `current_price`，排程每 5 分鐘批次更新
- 優點：查詢快
- 缺點：需改 schema + 排程

**✅ 選 A** — 即時查價更準確，持倉數量通常 < 20，效能不是問題。

---

### BUG-05：美股持倉 avg_cost 全 0

**根因**：`us_positions()` 回傳 `SELECT * FROM positions WHERE market='US'`，cost 欄位在 DB 裡可能是 0（新增持倉時沒填 cost，或 migrate 時沒帶入）。

**方案 A：從 trade_records 反算 avg_cost**
- 若 positions.cost = 0，查 `trade_records WHERE code=? AND action='BUY'`，加權平均計算
- 回填到 positions.cost

**方案 B：在 API 層即時計算**
- `us_positions()` 中，對 cost=0 的持倉即時從 trade_records 算出
- 不改 DB

**✅ 選 A** — 一次性回填更乾淨。根本原因是資料寫入流程缺了回填，修復寫入邏輯 + 回填現有。

---

## P1 重要功能

### BUG-06：batch-score 回傳空

**根因**：`_ic_score_stock()` 需要 30 天以上的 OHLCV 資料。台股資料來自 `kbar_cache`（可能只有即時 K 線沒有日線），美股用 yfinance 即時抓。如果 `kbar_cache` 裡該股日線不足 30 根，回傳 `{}`，batch-score 就 skip。

**方案 A：fallback 到 daily_kbar（market.db）**
- `_get_ohlcv_from_cache` 台股路徑加 fallback：`kbar_cache` 沒有時查 `daily_kbar`
- 優點：複用已有的回測資料

**方案 B：batch-score 先自動補資料**
- 在評分前對每支股票呼叫 `_ensure_daily_data()`
- 優點：確保有資料
- 缺點：首次慢

**✅ 選 A** — fallback 不增加延遲，daily_kbar 資料已經很完整。

---

### BUG-07：Walk-Forward 全壞（folds/avg_sharpe 全 None）

**根因**：API 回傳的欄位是 `windows`/`avg_train_return`/`avg_test_return`/`consistency`/`overfit_ratio`，但 QA 查的是 `folds`/`avg_sharpe` — 欄位名不匹配。另外如果用台股且 0 trades，所有值都會是 0。

**方案 A：加上 QA 期望的欄位別名**
- 在 summary 裡加 `"folds": len(results)`, `"avg_sharpe": avg_test_sharpe`
- 保留原欄位向前相容

**方案 B：統一所有 API 的命名規範文件**
- 建立 API schema 文件，前端/QA 都依照
- 優點：長期正確
- 缺點：工程量大

**✅ 選 A** — 加別名最快，向前相容不破壞現有。同時加 `avg_sharpe` 計算（目前沒有）。

---

### BUG-08：回測歷史不存結果（20 筆全 None）

**根因**：`backtest_result` 表的存入邏輯在 `run_backtest()` 是正確的（存 config + summary + trades）。20 筆全 None 代表是**早期回測**（在修復存入邏輯之前跑的），DB 裡已有殼但欄位為空。

**方案 A：清理 None 記錄 + 加 NOT NULL 防護**
- `DELETE FROM backtest_result WHERE summary IS NULL`
- 回傳時過濾 None

**方案 B：回傳時做 null-safe 處理**
- 把 None 替換為空 `{}`/`[]`/`0`
- 不刪資料

**✅ 選 A** — 髒資料沒有保留價值，直接清掉。同時 API 加 null-safe。

---

### BUG-09：推薦歷史無價格（rec_price/current_price/pnl 全缺）

**根因**：`ic_rec_history` 的 `entry_price` 可能在早期寫入時為 NULL。`_ic_refresh` 裡的寫入邏輯只有在 `batch-score` 有結果時才帶 `entry_price`。

**方案 A：evaluate 時回填 entry_price**
- 若 `entry_price IS NULL`，用推薦建立日期的收盤價回填
- 優點：修復存量

**方案 B：寫入時確保 entry_price 非空**
- 在 `INSERT INTO ic_rec_history` 前強制查詢收盤價
- 優點：源頭修復
- 缺點：不修復存量

**✅ 選 A+B** — 兩者都做。寫入時確保有值（B），存量資料回填（A）。

---

### BUG-10：績效評估回 0 筆

**根因**：`evaluate` 需要 `outcome='PENDING' AND created_at < cutoff AND entry_price IS NOT NULL`。如果 entry_price 全是 NULL（BUG-09），就 0 筆符合條件。

**✅ 隨 BUG-09 修復自動解決** — entry_price 回填後 evaluate 就有資料了。

---

### BUG-11：交易分析全 None，commission 異常

**根因**：
1. analytics 回傳的欄位是 `total_records`/`total_commission`/`realized_pnl`/`stocks`，沒有 `win_rate`/`avg_pnl`/`total_trades`
2. commission 1,475,742 是歷史遺留（`shares×1000` bug 造成的舊資料）

**方案 A：加上 win_rate/avg_pnl 計算 + 清理舊資料**
- 在 analytics 加 `win_rate`、`avg_pnl`、`total_trades` 欄位
- 清理 commission 異常的歷史記錄

**方案 B：只加欄位不清資料**
- 讓 analytics 從 trade_records 即時算出
- 優點：不刪資料
- 缺點：異常 commission 會拖歪統計

**✅ 選 A** — 加欄位 + 清理歷史髒資料。

---

### BUG-12：Multi-factor 不回傳分數

**根因**：API 回傳 `composite`（非 `composite_score`）和 `factors`（`z_*` keys，非 `momentum`/`value`/`quality`）。QA 用了前端的欄位名去查 API。

**方案 A：加上 QA 期望的欄位別名**
- `"composite_score": r.get("composite", 0)`, `"momentum": z_mom_20d`, `"value": z_bias_20d`, `"quality": z_vol_price_corr_20d`

**方案 B：改前端用正確欄位名**
- 優點：不動後端
- 缺點：QA 和前端都要改

**✅ 選 A** — 後端加別名，前後端都適用。

---

### BUG-13：新聞標題/日期全空

**根因**：`_fetch_yahoo_tw_news` 用 regex 解析 Yahoo HTML，回傳 `headline`/`sentiment`/`source`。QA 期望 `title`/`date`。而且 Yahoo 改版後 regex 可能已經匹配不到。

**方案 A：修復 regex + 加 title/date 別名**
- 加 `"title": headline`（別名），加 `"date": today`
- 更新 regex 適配 Yahoo 新版 HTML

**方案 B：改用 yfinance news API**
- `yf.Ticker(code).news` 有結構化 title/date
- 優點：穩定的 API 而非 HTML scraping
- 缺點：yfinance 的 news 對台股不一定有

**✅ 選 A** — regex 修復 + 加欄位。yfinance news 對台股覆蓋不好。

---

### BUG-14：Factor IC 回 0 結果

**根因**：`factor_ic_check` 沒帶 codes 時從 watchlist 查，如果 watchlist 裡沒有 US 股票或台股 OHLCV 不足 60 天，`_calc_factor_ic` 全部失敗回 empty。

**方案 A：fallback 到 daily_kbar + 保底 codes**
- 無 watchlist 時用預設股票清單
- OHLCV fallback 到 market.db

**方案 B：回傳明確錯誤訊息**
- `{"results": [], "error": "watchlist 為空或資料不足"}`
- 優點：使用者知道原因
- 缺點：不修根本問題

**✅ 選 A** — fallback 讓功能可用，同時回傳診斷訊息。

---

### BUG-15：RISK_ON vs CAUTION 矛盾

**根因**：`macro/interpretation` 是 AI 判讀（看總經環境），`risk-level` 是規則引擎（VIX/DXY/US10Y 閥值）。兩者設計上獨立，但使用者困惑。

**方案 A：在 macro interpretation 加入 risk-level 作為參考欄位**
- 回傳裡加 `"system_risk_level": "CAUTION"` + `"note": "AI判讀與系統風控為獨立判斷"`
- 優點：資訊透明

**方案 B：強制一致**
- macro interpretation 的結論以 risk-level 為準
- 缺點：犧牲 AI 獨立判斷

**✅ 選 A** — 加參考欄位，不犧牲 AI 獨立性。

---

## P2 中等問題

### BUG-16：信號歷史異常價格（BUY_A@100 for 2330）

**方案 A：清理 DB 髒資料**
- `DELETE FROM signal_log WHERE code='2330' AND price < 500`
- 優點：直接解決

**方案 B：加價格合理性檢查**
- 寫入 signal_log 前驗證 price 在合理範圍
- 優點：防止未來再出現

**✅ 選 A+B** — 清理存量 + 防止增量。

---

### BUG-17：策略數量不一致（21 vs 29）

**方案 A：更新 handoff 文件**
- 把「29」改為「21」，說明正確數量

**方案 B：確認是否有策略被移除**
- 查 code 裡 STRATEGIES 陣列實際數量

**✅ 選 A** — 確認後更新文件。

---

### BUG-18：SF sessions 元數據全 None

**根因**：`_strategy_factory_controller` 完成時沒有回寫 session 的 `best_sharpe`/`strategies_tested` 等統計。

**方案 A：controller 結束時回寫**
- 完成後 `UPDATE sf_session SET best_sharpe=?, strategies_tested=? WHERE id=?`

**方案 B：API 即時計算**
- 在 `sf_session_detail` 裡從 `sf_strategy` 表 JOIN 計算
- 優點：不改 controller
- 缺點：每次查詢都要算

**✅ 選 A** — controller 結束時寫一次最正確。

---

### BUG-19：SF AI 策略重複命名

**方案 A：生成時加去重（名稱+hash）**
- 存入前檢查同名策略，有的話加序號 `-v2`

**方案 B：允許同名但加 unique constraint**
- code hash 去重，名稱可重複但加時間戳
- 優點：不影響創造力
- 缺點：使用者看到重複

**✅ 選 A** — 加序號去重，使用者體驗更好。

---

### BUG-20：法人排行混入 ETF

**方案 A：SQL 加 `WHERE code NOT LIKE '00%'`**
- 過濾掉 00 開頭的 ETF 代碼

**方案 B：維護 ETF 排除名單**
- 完整排除清單
- 優點：更精確
- 缺點：需維護

**✅ 選 A** — 台灣 ETF 都是 00 開頭，簡單 filter 就夠。

---

### BUG-21：US indices symbol 為 None

**根因**：`_yf_snapshot` 回傳 `"code"` 欄位，不是 `"symbol"`。QA 查的是 `symbol`。

**方案 A：加 `"symbol": symbol` 別名**
- 在 `_yf_snapshot` 回傳裡加一行

**方案 B：前端改用 `code`**
- 優點：不改後端

**✅ 選 A** — 加別名，前後端都相容。

---

### BUG-22：負數 capital 可回測

**方案 A：API 層 validation**
- `if capital <= 0: return JSONResponse({"error": "capital 必須大於 0"}, 400)`

**方案 B：Pydantic model validation**
- 用 `conint(gt=0)` 或 `confloat(gt=0)` 型別
- 優點：更嚴謹
- 缺點：需改 input model

**✅ 選 A** — 簡單有效，不需要為此新建 Pydantic model。

---

## P3 低優先

### BUG-23：黃金報價 4207

**判定**：GLD ETF（SPDR Gold Shares）每股 ~$230，不是現貨金 ~$2400。但報價 4207 可能是其他合約（GC=F 期貨）。

**✅ 不修** — 查明資料源後在 UI 加單位說明即可。

---

### BUG-24：6 個 orphan KB chunks

**方案 A：`DELETE FROM ic_kb_chunks WHERE source_id NOT IN (SELECT id FROM ic_news_sources)`**

**方案 B：保留但標記 orphan**

**✅ 選 A** — 直接清理。

---

### BUG-25：Sectors 數值微不一致

**判定**：兩個 endpoint 可能在不同時間抓取 yfinance，導致微小差異。

**✅ 不修** — 預期行為，不影響使用。

---

## 執行順序與預估

| 批次 | Bugs | 預估時間 |
|------|------|---------|
| **第一批** | SEC-01, SEC-02, BUG-01, BUG-02, BUG-03, BUG-04, BUG-05 | 30 分鐘 |
| **第二批** | BUG-06, BUG-07, BUG-08, BUG-09/10, BUG-11, BUG-12, BUG-13, BUG-14, BUG-15 | 30 分鐘 |
| **第三批** | BUG-16~22, BUG-24 | 15 分鐘 |
| **驗證** | curl 逐項 + E2E 23 項 | 15 分鐘 |

**每批修完立即跑 E2E 確認不回歸。**
