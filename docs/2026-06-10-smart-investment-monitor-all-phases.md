# 智慧投顧監控系統 — 全 Phase 驗收總覽

**日期**：2026-06-10
**後端**：`ui/backend.py`（FastAPI + uvicorn, port 8765）
**前端**：`ui/index.html`（TradingView Lightweight Charts 4.1.3 + Vanilla JS）
**資料庫**：SQLite `monitor.db`
**行情 API**：Shioaji 1.5.2（永豐金，模擬盤預設）
**總經資料**：yfinance
**推播**：Telegram Bot + Email SMTP + Webhook
**美股資料**：yfinance（K線、快照、指數）

---

## Phase 總覽

| Phase | 範圍 | 狀態 |
|-------|------|------|
| Phase 1 | 八大 Gap（G1-G8）+ 核心 K 線 / 訊號 / 風控 | ✅ 完成 |
| Phase 2 | Tick 即時 K 線、總經定時輪詢、推播通知 | ✅ 完成 |
| Phase 2.5 | highest_price 追蹤、移動止盈、VWAP、風控三級制 | ✅ 完成 |
| Phase 3 | 籌碼模組（三大法人、融資融券、投信鎖碼） | ✅ 完成 |
| Phase 4 | 進階訊號（Buy_A/B tick-level、LOCK_BUY、外盤/大單追蹤） | ✅ 完成 |
| Phase 5 | 自動停損下單（EXIT_D 市價送出 + 安全閥） | ✅ 完成 |
| Phase 6 | 通知改版（Telegram+Email）+ 資料來源管理頁 | ✅ 完成 |
| Phase 7 | 美股市場支援（yfinance K線/快照/訊號/自選股/持倉） | ✅ 完成 |
| Phase 8 | PRD缺失補齊：利多不漲NLP + 當沖比午盤防洗 + 融券軋空聯動 | ✅ 完成 |
| Phase 9 | 策略管理頁 + 回測系統 + market.db 資料分離 | ✅ 完成 |

---

## Phase 1：八大 Gap + 核心功能

### G1-G8 實作狀態

| Gap | 功能 | 說明 |
|-----|------|------|
| G1 | 年線 MA240 + LOW_BUY | K 線灰色虛線；乖離年線 −15% 觸發 LOW_BUY |
| G2 | 五檔委託 BidAsk | WebSocket `/ws/tick/{code}` 推送，前端渲染買五/賣五 |
| G3 | 波段 / 當沖區分 | `trade_type` 欄位；波段 EXIT_C / 當沖 EXIT_D 分別停損 |
| G4 | 可調停損滑桿 | 3–12% 滑桿；成本為 0 時禁用；個別覆蓋全域 |
| G5 | SQUEEZE_BREAK 籌碼擠壓 | 突破 20 日最高 + 量比 ≥ 2x 觸發 |
| G6 | 法人目標價 | 新增持倉可填目標價；盤後頁顯示乖離空間 |
| G7 | NLP 盤中警示條 | EXIT_D / EXIT_C / MACRO_LOCK 觸發時顯示黃色橫幅 |
| G8 | MACRO_LOCK 全站鎖定 | VIX>35 / DXY月漲>3% / US10Y>5% / 加權低月線5%；≥2 條鎖定 |

### 訊號引擎

| 訊號 | 方向 | 觸發條件 | 去重 |
|------|------|----------|------|
| BUY_A | BUY | MACD 金叉 + 站上 MA20 + 量比 ≥ 1.5x | 每日一次 |
| BUY_B | BUY | MA5 上穿 MA10 + 量比 ≥ 1.2x | 每日一次 |
| LOW_BUY | BUY | 低於 MA240 −15% 超跌低吸 | 每日一次 |
| EXIT_A | SELL | 跌破 VWAP 均價線 0.5%（Phase 2.5 改） | 每日一次 |
| EXIT_B | SELL | MACD 死叉 + 量縮 < 0.8x | 每日一次 |
| EXIT_C | SELL | 移動止盈：最高價回落（Phase 2.5 改） | 每日一次 |
| EXIT_D | SELL | 從成本跌幅 ≥ exit_d_threshold%（預設 5%） | 每日一次 |
| SQUEEZE_BREAK | BUY | 突破 20 日高 + 量比 ≥ 2x | 每日一次 |

掃描頻率：`scanSignals` 每 5 分鐘（盤中 09:00–13:35）；`scanExitD` 每 60 秒全天候。

### K 線圖

| 項目 | 實作 |
|------|------|
| 時框 | 日K / 60 分 / 5 分（Shioaji 1-min bars → pandas resample） |
| MA 線 | MA5（藍）/ MA10（紫）/ MA20（橙）/ MA60（綠）/ MA240 年線（灰） |
| MACD | DIF / MACD / 柱狀圖，與主圖 timescale 同步 |
| 成交量 | 紅漲綠跌半透明柱狀圖 |
| 台灣色規 | 紅漲綠跌 |
| 訊號標記 | 金叉 ↑ 買、死叉 ↓ 賣 |
| 離線提示 | 後端離線時覆蓋層顯示 |

### Rate Limiter（Token Bucket）

| 桶 | 容量 | 視窗 | 適用 |
|----|------|------|------|
| `_rl_data` | 50 tokens | 5 秒 | 所有行情查詢 |
| `_rl_ticks` | 10 tokens | 5 秒 | Ticks 子桶 |

非限速錯誤立即 raise；限速錯誤指數退避重試（0.5s–8s，max 5 次）。

### K-bar 快取策略

| 時框 | 快取有效期 |
|------|-----------|
| 日K | 15:00 後更新一次，隔日 15:00 前有效 |
| 60分 / 5分 | 5 分鐘 TTL |

### 憑證安全

載入順序：(1) 環境變數 `SJ_API_KEY`/`SJ_SEC_KEY` → (2) `ui/.env` → (3) `~/ai-investment-system/config.yaml`。`SJ_PRODUCTION=true` 才切正式盤。

---

## Phase 2：即時更新 + 推播

### Tick 即時 K 線更新

- K 線頁載入後自動訂閱 WebSocket tick
- 每個 tick 即時更新最後一根 K 棒的 OHLC（僅更新價格，不累加 volume 避免 double-counting）
- TF boundary guard：tick 超過當前 K 棒時間範圍時跳過
- 離開 K 線頁自動斷開 chart tick WS

### 總經定時輪詢

- 前端 `setInterval(loadMacro, 15 * 60 * 1000)` 每 15 分鐘自動刷新
- 後端 yfinance 15 分鐘快取

### LINE Messaging API / Webhook 推播

- 訊號觸發時透過 daemon thread 非同步推送
- 支援 LINE Messaging API（Channel Access Token + User ID）
- 支援 HTTPS Webhook URL（SSRF 防護：僅允許 https://）
- 設定頁可配置 Token / User ID / Webhook / 啟停
- `POST /api/notify/test` 測試推播

---

## Phase 2.5：核心邏輯補齊

### 1. highest_price 持倉最高價追蹤

- `positions` 表新增 `highest_price REAL DEFAULT 0` 欄位
- 每個 Shioaji tick 回調自動更新所有持倉的最高價（`WHERE highest_price < current_price`）
- 新增持倉時 `highest_price` 初始化為成本價
- 持倉頁顯示最高價追蹤值

### 2. Exit_C 移動止盈（重寫）

原邏輯：成本 × (1 − 停損%)，固定比例停損。

新邏輯（依原始規格）：

| 類型 | 啟動條件 | 觸發條件 |
|------|----------|----------|
| 波段 | 利潤達成本 +8% | 從最高價回落 2% |
| 當沖 | 利潤達成本 +3% | 從最高價回落 1% |

- 個別停損價（G4）仍優先檢查
- 觸發時記錄鎖住的利潤百分比

### 3. VWAP 均價線

- `_ws_route_tick` 即時累積 Σ(price × volume) / Σ(volume)，每日自動重置
- tick 推送新增 `vwap` 欄位
- K 線頁 price bar 顯示即時 VWAP
- `GET /api/vwap/{code}` 查詢端點
- Exit_A 改為跌破 VWAP 0.5% 觸發（取代原本的跌破 MA20）

### 4. 風控三級制

| 等級 | 觸發條件 | 部位縮放 | 買入按鈕 |
|------|----------|----------|----------|
| NORMAL | 0 項警報 | 100% | 正常 |
| CAUTION | 1 項警報 | 60% | 顯示警告 |
| ALERT | ≥2 項警報 | 30% | 禁用 |

警報條件（同 Phase 1 G8）：VIX>35 / DXY月漲>3% / US10Y>5% / 加權低月線5%

- Header 顯示彩色風控等級 badge（綠/黃/紅）
- 總經頁顯示等級 + 部位縮放比例
- `GET /api/risk-level` 查詢端點
- `/api/macro` 回傳新增 `risk_level`、`position_scale`、`alert_count`

---

## 後端 API 端點總覽（截至 Phase 2.5）

| 方法 | 路徑 | 功能 | Phase |
|------|------|------|-------|
| GET | `/api/info` | 系統資訊 + 模擬/正式模式 | 1 |
| GET | `/api/snapshot/{code}` | 單股即時快照 | 1 |
| GET | `/api/kbars/{code}?tf=D\|60\|5` | K 線 + MA + MACD + markers | 1 |
| GET | `/api/watchlist` | 自選股清單（含快照） | 1 |
| GET | `/api/watchlist/list` | 自選股列表 | 1 |
| POST | `/api/watchlist/add/{code}` | 新增自選股 | 1 |
| DELETE | `/api/watchlist/remove/{code}` | 移除自選股 | 1 |
| GET | `/api/sparkline/{code}` | 30 日趨勢線 | 1 |
| GET | `/api/positions` | 所有持倉（含 highest_price） | 1+2.5 |
| POST | `/api/positions` | 新增持倉 | 1 |
| PUT | `/api/positions/{id}` | 更新持倉 | 1 |
| DELETE | `/api/positions/{id}` | 刪除持倉 | 1 |
| GET | `/api/risk-config` | 風控設定 | 1 |
| POST | `/api/risk-config` | 更新風控設定 | 1 |
| GET | `/api/macro` | 總經數據 + 風控等級 | 1+2.5 |
| GET | `/api/macro-lock` | MACRO_LOCK 狀態 | 1 |
| POST | `/api/macro-lock/{state}` | 設定 MACRO_LOCK | 1 |
| GET | `/api/vwap/{code}` | 即時 VWAP | 2.5 |
| GET | `/api/risk-level` | 風控等級 + 部位縮放 | 2.5 |
| GET | `/api/signals` | 訊號記錄 | 1 |
| POST | `/api/signals` | 手動寫入訊號 | 1 |
| GET | `/api/scan/exitd` | EXIT_D 掃描 | 1 |
| GET | `/api/scan/signals` | 全倉訊號掃描 | 1 |
| POST | `/api/notify/test` | 測試推播 | 2 |
| WS | `/ws/tick/{code}` | 即時 Tick + BidAsk + VWAP | 1+2.5 |

---

## SQLite 資料表

| 表名 | 用途 | 欄位重點 |
|------|------|----------|
| `kbar_cache` | K 線快取 | code, tf, OHLCV, updated_at |
| `positions` | 持倉 | trade_type(G3), stop_loss(G4), target_price(G6), **highest_price**(2.5), **market**(TW/US), **status**(open/closed) |
| `signal_log` | 訊號記錄 | signal_type, direction, price, detail |
| `risk_config` | 風控設定 | key-value（含 macro_lock, **risk_level**, **position_scale**, LINE token） |
| `watchlist` | 自選股清單 | code, name, sort_order, **market**(TW/US) |

---

## 前端頁面（7 頁）

| 頁面 | 功能 |
|------|------|
| 總覽 | 總經指標 chips、今日訊號卡片、自選股表格（含 30 日趨勢線）、持倉快覽 |
| K 線 | 多時框 K 線圖 + MA/MACD/Volume + 五檔委託(G2) + 快速下單 + 停損滑桿(G4) + **即時 tick 更新** + **VWAP** |
| 持倉 | 持倉管理 CRUD + 停損調整 + **最高價追蹤** + **移動止盈規則顯示** |
| 盤後 | SQUEEZE_BREAK 候選(G5) + 法人目標價(G6) + 訊號歷史 |
| 總經 | VIX/DXY/US10Y/ES 儀表 + MACRO_LOCK 面板(G8) + **風控等級/部位縮放** |
| 設定 | 停損%(G4) + EXIT_D 滑桿 + 部位上限 + **Telegram/Email/Webhook 推播設定 + 測試** |
| 資料源 | 12 種資料來源狀態總覽（活躍/規劃中） |

---

## Phase 7：美股市場支援

### 後端 API

| 方法 | 路徑 | 功能 |
|------|------|------|
| GET | `/api/us/kbars/{symbol}?tf=D\|60\|5` | 美股 K 線（yfinance） |
| GET | `/api/us/snapshot/{symbol}` | 美股即時快照 |
| GET | `/api/us/watchlist` | 美股自選股清單（含快照） |
| POST | `/api/us/watchlist/add/{symbol}` | 新增美股自選股 |
| DELETE | `/api/us/watchlist/remove/{symbol}` | 移除美股自選股 |
| GET | `/api/us/indices` | 美股大盤指數（SPY/QQQ/DIA/^GSPC/^IXIC/^DJI/^SOX） |
| GET | `/api/us/positions` | 美股持倉 |
| POST | `/api/us/positions` | 新增美股持倉 |
| GET | `/api/us/scan/signals` | 掃描美股訊號（BUY_A/B, EXIT_B/C/D） |

### 訊號引擎（美股版）

| 訊號 | 觸發條件 |
|------|----------|
| BUY_A | MACD 金叉 + 站上 MA20 + 量比 ≥ 1.5x |
| BUY_B | MA5 上穿 MA10 + 量比 ≥ 1.2x |
| EXIT_B | MACD 死叉 + 量縮 < 0.8x |
| EXIT_C | 移動止盈（波段 8%→2% / 當沖 3%→1%） |
| EXIT_D | 從成本跌幅 ≥ threshold% |

### 前端

- Header TW/US 市場切換按鈕
- 美股首頁：大盤指數 chips + 訊號卡片 + 自選股表格 + 持倉快覽
- K 線圖自動偵測市場，US 使用 `/api/us/kbars/`，停用 Tick WebSocket
- DB 新增 `market` 欄位（watchlist + positions），預設 'TW'

---

## Phase 8：PRD 缺失補齊

### 8.1 利多不漲 NLP 情感逆向

| 項目 | 實作 |
|------|------|
| 資料源 | TWSE 公開資訊觀測站 (MOPS) 重大訊息 + Yahoo 台灣股市新聞 |
| 情感分析 | 關鍵字匹配（19 正面詞 / 13 負面詞），回傳 positive/negative/neutral |
| 訊號 | `NEWS_BEARISH`：正面新聞但收跌>1% + 量比>1.5x → 利多出盡賣出 |
| 端點 | `GET /api/news/{code}` — 個股新聞+情感 |
|  | `POST /api/news/fetch-material` — 抓取 MOPS 重大訊息 |
|  | `GET /api/news/bearish-reversal` — 利多不漲候選清單 |
| 前端 | 盤後分析頁「利多不漲偵測」卡片 + 抓取按鈕 |

### 8.2 當沖比過高午盤防洗

| 項目 | 實作 |
|------|------|
| 資料源 | TWSE 當日沖銷交易統計 (`TWTB4U`) |
| DB | `daytrade_snapshot` 表（code, date, daytrade_vol, total_vol, daytrade_ratio） |
| 訊號 | `DAYTRADE_WARN`：前日當沖比>70% + 12:30後跌破VWAP → 當沖客倒貨賣壓 |
| 端點 | `POST /api/chip/fetch-daytrade` — 抓取當沖比數據 |
|  | `GET /api/chip/daytrade/{code}` — 個股當沖比歷史 |
|  | `GET /api/chip/daytrade-warn` — 當沖比>50%高危名單 |
| 前端 | 盤後分析頁「當沖比警示（午盤防洗）」卡片 + 抓取按鈕 |

### 8.3 融券軋空 + 盤中突破前日高聯動

| 項目 | 實作 |
|------|------|
| 訊號 | `SQUEEZE_BUY`：券資比>30% + 盤中突破前日高 = 強力軋空買訊 |
| 端點 | `GET /api/chip/squeeze-breakout` — 融券軋空突破候選清單 |
| 前端 | 盤後分析頁「融券軋空盤中突破」卡片，顯示突破/未突破狀態 |
| 訊號引擎 | `run_signal_engine()` 新增 `SQUEEZE_BUY` 條件檢查 |

---

## 原始規格 vs 目前實作 — 殘留差距

### Phase 3–5 實作完成

| 項目 | 原始規格 | 狀態 | Phase |
|------|----------|------|-------|
| `chip_snapshot` 表 | 三大法人、融資融券、強制回補日 | ✅ | 3 |
| 台灣證交所公開資料接入 | HTTP GET 法人買賣超 / 融資融券 | ✅ | 3 |
| 融券軋空篩選 | 券資比>30% | ✅ `/api/chip/squeeze-candidates` | 3 |
| 投信鎖碼偵測 | 連買3天 | ✅ `/api/chip/itrust-lock` | 3 |
| 籌碼棄守訊號 | 投信連2日大賣超>500張 | ✅ `/api/chip/abandon` | 3 |
| Buy_A 原始邏輯 | 假跌破5MA → 15-30分拉回 + 大單/外盤 | ✅ tick-level + fallback MACD | 4 |
| Buy_B 原始邏輯 | 量比>2.5x + 連續外盤≥5 + 特大單 | ✅ tick-level + fallback MA交叉 | 4 |
| LOCK_BUY | 正乖離>15% 鎖定買進 | ✅ | 4 |
| 外盤/內盤計數 | tick_type 連續外盤追蹤 | ✅ `_tick_buf` + 前端顯示 | 4 |
| 特大單偵測 | ≥100 張 | ✅ 含內盤大單(砸盤)分類 | 4 |
| Exit_B 增強 | 高檔爆量出貨（內盤大單） | ✅ + fallback MACD死叉 | 4 |
| 自動停損下單 | EXIT_D 市價送出 | ✅ `/api/auto-sell/execute` | 5 |
| 下單安全閥 | auto_sell_enabled + 模擬/正式分離 | ✅ 預設關閉，需手動啟用 | 5 |
| 持倉狀態管理 | open/closed 狀態 | ✅ positions.status 欄位 | 5 |
| 利多不漲 NLP | 正面新聞+開高走低量大收黑 | ✅ 關鍵字情感+NEWS_BEARISH訊號 | 8 |
| 當沖比午盤防洗 | 當沖比>70%+12:30後跌破VWAP | ✅ DAYTRADE_WARN訊號 | 8 |
| 融券軋空盤中聯動 | 券資比>30%+突破前日高 | ✅ SQUEEZE_BUY訊號 | 8 |

### 仍為未來項目

| 項目 | 說明 |
|------|------|
| G7 NLP 深度模型 | 目前用關鍵字匹配，未來可接 LLM/BERT 做全文語意分析 |
| EXIT_C 自動下單 | 目前僅 EXIT_D 自動下單，EXIT_C 仍為警示 |

### 已與原始 PRD 完全對齊的項目

- ✅ 移動止盈（Exit_C）：波段 8%→回落2% / 當沖 3%→回落1%
- ✅ VWAP 均價線 + Exit_A 跌破 VWAP 3 分鐘確認
- ✅ 風控三級制：NORMAL(100%) / CAUTION(60%) / ALERT(30%)
- ✅ highest_price 追蹤
- ✅ Tick-level 外盤連續/特大單追蹤（Phase 4）
- ✅ 自動停損市價賣出 + 安全閥（Phase 5）
- ✅ Telegram + Email 推播通知（Phase 6）
- ✅ 美股市場支援（Phase 7）
- ✅ 利多不漲 NLP 情感逆向偵測（Phase 8）
- ✅ 當沖比過高午盤防洗（Phase 8）
- ✅ 融券軋空 + 盤中突破前日高聯動（Phase 8）

---

## 安全注意事項

1. **永遠先用 `simulation=True` 測試**（`SJ_PRODUCTION=true` 才切正式）
2. **API Key 不硬編碼**，僅從 env / .env / config.yaml 載入
3. **Webhook SSRF 防護**：僅允許 `https://` scheme
4. **Telegram Token / Email 密碼存於 SQLite**（明文），建議正式環境改用 OS Keychain
5. **EXIT_D 保命機制不可關閉**

---

*本文件由 Claude Code 自動生成，2026-06-10。*
