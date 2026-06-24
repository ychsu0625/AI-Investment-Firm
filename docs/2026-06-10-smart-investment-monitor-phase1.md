# 智慧投顧監控系統 — Phase 1 驗收報告

**日期**：2026-06-10  
**後端**：`ui/backend.py`（FastAPI + uvicorn, port 8765）  
**前端**：`ui/index.html`（TradingView Lightweight Charts 4.1.3 + Vanilla JS）  
**資料庫**：SQLite `monitor.db`  
**行情 API**：Shioaji 1.5.2（永豐金，模擬盤預設）  
**總經資料**：yfinance 1.4.1

---

## 1. 八大 Gap 實作狀態

| Gap | 功能 | 狀態 | 說明 |
|-----|------|------|------|
| G1 | 年線 240MA + LOW_BUY 訊號 | ✅ | K線圖顯示 MA240（灰色虛線）；`run_signal_engine` 偵測乖離年線 −15% 觸發 LOW_BUY |
| G2 | 五檔委託（BidAsk） | ✅ | HTML 表格 + JS `loadBidAsk()` 接 WebSocket `/ws/tick/{code}`，渲染買五/賣五即時報價 |
| G3 | 波段 / 當沖區分 | ✅ | 持倉 `trade_type` 欄位；波段單停損 EXIT_C（預設 −7%），當沖單 EXIT_D（預設 −3%）；K線面板右側切換按鈕 |
| G4 | 可調停損滑桿 | ✅ | K線面板右側停損滑桿 3–12%；成本為 0 時禁用並提示「需填成本」；個別持倉可覆蓋全域設定 |
| G5 | SQUEEZE_BREAK 籌碼擠壓 | ✅ | 突破近 20 日最高點且量比 ≥ 2x 觸發；盤後分析頁顯示候選清單 |
| G6 | 法人目標價 | ✅ | 新增持倉 modal 可填入目標價；盤後分析頁顯示現價 vs 目標價乖離空間 |
| G7 | NLP 盤中警示條 | ✅* | 盤中黃色警示條（`#nlp-strip`）在 EXIT_D / EXIT_C / MACRO_LOCK 觸發時自動顯示。*全文 NLP 新聞偵測為 Phase 2 項目 |
| G8 | MACRO_LOCK 全站鎖定 | ✅ | VIX>35 / DXY月漲>3% / US10Y>5% / 加權低月線5% 四條件；≥2 條自動建議鎖定；手動啟停；鎖定後禁用全站買入按鈕並顯示紅色橫幅 |

---

## 2. 訊號引擎

| 訊號 | 方向 | 觸發條件 | 去重 |
|------|------|----------|------|
| BUY_A | BUY | MACD 金叉 + 站上 MA20 + 量比 ≥ 1.5x | 每日一次 |
| BUY_B | BUY | MA5 上穿 MA10 + 量比 ≥ 1.2x | 每日一次 |
| LOW_BUY | BUY | 低於 MA240 −15%（G1 超跌低吸） | 每日一次 |
| EXIT_A | SELL | 跌破 MA20 | 每日一次 |
| EXIT_B | SELL | MACD 死叉 + 量縮 < 0.8x | 每日一次 |
| EXIT_C | SELL | 持倉高點回落超過停損%（G4 個別 > 全域） | 每日一次 |
| EXIT_D | SELL | 緊急停損：從成本跌幅 ≥ exit_d_threshold%（預設 5%）| 每日一次 |
| SQUEEZE_BREAK | BUY | 突破 20 日高 + 量比 ≥ 2x（G5） | 每日一次 |

掃描頻率：`scanSignals` 每 5 分鐘（盤中 09:00–13:35）；`scanExitD` 每 60 秒全天候。

---

## 3. K 線圖

| 項目 | 實作 |
|------|------|
| 時框 | 日K / 60 分 / 5 分（Shioaji 1-min bars → pandas resample） |
| MA 線 | MA5（藍）/ MA10（紫）/ MA20（橙）/ MA60（綠）/ MA240 年線（灰，G1） |
| MACD | DIF / MACD / 柱狀圖，與主圖 timescale 同步 |
| 成交量 | 紅漲綠跌半透明柱狀圖 |
| 台灣色規 | 紅漲綠跌（與台股慣例一致） |
| 訊號標記 | 金叉 ↑ 買、死叉 ↓ 賣自動顯示於 K 線上 |
| 載入提示 | 後端離線時顯示「後端未啟動，請執行 backend.py」覆蓋層 |

---

## 4. 後端架構

### 4.1 端點清單

| 方法 | 路徑 | 功能 |
|------|------|------|
| GET | `/api/kbars/{code}?tf=D\|60\|5` | K 線資料 + MA + MACD + 訊號 markers |
| GET | `/api/snapshot/{code}` | 單股即時快照 |
| GET | `/api/watchlist` | 自選股清單（含快照） |
| POST | `/api/watchlist/add/{code}` | 新增自選股 |
| DELETE | `/api/watchlist/remove/{code}` | 移除自選股 |
| GET | `/api/positions` | 所有持倉 |
| POST | `/api/positions` | 新增持倉 |
| PUT | `/api/positions/{id}` | 更新持倉 |
| DELETE | `/api/positions/{id}` | 刪除持倉 |
| GET | `/api/risk-config` | 讀取風控設定 |
| POST | `/api/risk-config` | 更新風控設定 |
| GET | `/api/scan/exitd` | 掃描 EXIT_D 持倉 |
| GET | `/api/scan/signals` | 掃描全倉訊號 |
| GET | `/api/macro` | VIX/DXY/US10Y/ES/TWII（yfinance，15 分鐘快取） |
| GET | `/api/macro-lock` | 取得 MACRO_LOCK 狀態 |
| POST | `/api/macro-lock/{state}` | 設定 MACRO_LOCK |
| GET | `/api/sparkline/{code}` | 30 日趨勢線資料 |
| WS | `/ws/tick/{code}` | 即時 Tick（Shioaji subscribe，非 polling） |

### 4.2 Rate Limiter（Token Bucket）

| 桶 | 容量 | 視窗 | 適用 |
|----|------|------|------|
| `_rl_data` | 50 tokens | 5 秒 | 所有行情查詢（snapshots / kbars） |
| `_rl_ticks` | 10 tokens | 5 秒 | Ticks 子桶（先扣子桶再扣全域） |

- 非流量限制錯誤立即 re-raise（不重試）
- 流量限制錯誤指數退避重試：0.5s / 1s / 2s / 4s / 8s（max 5 次）
- 即時行情用 `api.quote.subscribe()` push，不消耗查詢額度

### 4.3 K-bar 快取策略

| 時框 | 快取有效期 |
|------|-----------|
| 日K | 15:00 後更新一次，隔日 15:00 前有效 |
| 60分 / 5分 | 5 分鐘 TTL |

### 4.4 SQLite 資料表

- `kbar_cache`：K 線快取
- `positions`：持倉（含 trade_type G3、stop_loss G4、target_price G6）
- `signal_log`：訊號記錄
- `risk_config`：風控設定 + MACRO_LOCK 狀態 + macro_cache
- `watchlist`：自選股清單

---

## 5. 憑證安全

API Key 載入順序（不硬編碼）：
1. 環境變數 `SJ_API_KEY` / `SJ_SEC_KEY`
2. `ui/.env` 檔案
3. `~/ai-investment-system/config.yaml`

`SJ_PRODUCTION=true` 才切換正式盤，預設模擬盤。

---

## 6. 已知問題 / Phase 2 待辦

| 項目 | 說明 |
|------|------|
| G7 NLP 全文偵測 | 目前僅在訊號觸發時顯示警示條，無真正 NLP 新聞分析 |
| Tick 即時 K 線更新 | WebSocket 已接，但前端未將 tick 即時合併進 K 線圖 |
| 總經數據定時刷新 | 目前 15 分鐘快取 + 手動切頁觸發，未加定時輪詢 |
| Line Notify 推播 | Phase 2 |
| 自動下單（EXIT_D / EXIT_C） | Phase 3 |
