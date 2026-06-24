# Smart Investment Monitor — 完整專案 Handoff

**版本**：v3.0（2026-06-22）
**用途**：給新專案 / 新 Claude session 的完整上下文，讓接手者能立即理解並操作本系統

---

## 一、專案概觀

### 1.1 是什麼

一個全功能的**智慧投顧監控系統**，涵蓋台股（Shioaji）+ 美股（yfinance），提供：

- 即時行情 / K 線 / 五檔報價
- 28 種固定策略訊號（BUY_A~F, EXIT_A~D 等）
- 回測引擎（固定策略 + AI 動態策略）
- Walk-Forward 驗證（防過擬合）
- 策略實驗室（Grid Search → Bayesian → AI 三層迭代調參）
- 策略工廠（AI 自主產生全新策略程式碼）
- 專家委員會（多角色 AI 分析）
- 資訊中心（板塊輪動、多因子、AI 推薦、知識庫 RAG）
- 風控中心（MACRO_LOCK、三級風控、EXIT_D 保命）
- 持倉管理 + 交易紀錄
- 盤後分析（籌碼、融券、當沖比）

### 1.2 技術棧

| 項目 | 技術 |
|------|------|
| 後端 | FastAPI (Python), uvicorn |
| 前端 | 單頁 SPA (index.html), vanilla JS, CSS variables |
| 資料庫 | SQLite (monitor.db, market.db) |
| 台股行情 | 永豐金 Shioaji API |
| 美股行情 | yfinance |
| AI | Claude (subscription CLI 或 API) |
| 優化 | Optuna (Bayesian) |
| 圖表 | 原生 Canvas + SVG |

### 1.3 規模

| 指標 | 數量 |
|------|------|
| backend.py | 11,610 行 |
| index.html | 7,557 行 |
| API routes | 168 個 |
| DB tables | 33 張 |
| Functions | 312 個 |
| 固定策略 | 28 種 |
| 公式 | 38 條 |
| 資料管道 | 50+ |
| E2E 測試 | 23/23 通過 |

---

## 二、檔案結構

```
smart-investment-monitor/
├── ui/
│   ├── backend.py          # 主後端（所有 API + 邏輯）
│   ├── index.html          # 主前端 SPA
│   ├── info_center.html    # 資訊中心（獨立頁面，已整合入 index.html）
│   ├── manual.html         # HTML 使用手冊
│   ├── simple.html         # 簡化版頁面
│   ├── sf_agent.py         # 策略工廠 LLM Queue Agent（handoff 腳本）
│   ├── github_agent.py     # GitHub 自動推送 agent
│   ├── monitor.db          # 主資料庫（持倉、交易、設定、策略等）
│   ├── test_iter_ui.py     # 策略實驗室 E2E 測試（23 項）
│   ├── test_sf_ui.py       # 策略工廠 E2E 測試
│   ├── .api_token          # API token（每次重啟重新產生）
│   └── data/
│       ├── market.db       # 行情快取資料庫
│       └── investment.db   # 投資資料（舊版）
├── tests/
│   ├── test_full_io.py     # 141 endpoint 完整測試
│   ├── test_api_smoke.py   # API 煙霧測試
│   ├── test_e2e_user.py    # 使用者流程 E2E
│   ├── test_integration.py # 整合測試
│   └── test_ui_full.py     # UI 全功能測試
├── docs/
│   ├── 2026-06-22-project-handoff.md  # ← 本文件
│   ├── 2026-06-22-user-manual.md      # 使用手冊（最新版）
│   ├── UPDATES.md                      # Push 歷史
│   └── ...                             # 歷史文件
├── CHANGELOG.md
└── .claude/
    └── launch.json         # Claude Code 啟動設定
```

---

## 三、資料庫 Schema

### 3.1 monitor.db（主庫，33 張表）

#### 核心業務
| 表名 | 用途 |
|------|------|
| `positions` | 持倉（code, name, shares, cost, market, status） |
| `trade_records` | 交易紀錄（type, price, shares, commission, tax） |
| `watchlist` | 自選股清單 |
| `signal_log` | 訊號歷史紀錄 |
| `strategy_config` | 策略啟停與參數 |
| `risk_config` | 風控設定（停損%、部位上限、通知設定） |

#### 行情與籌碼
| 表名 | 用途 |
|------|------|
| `kbar_cache` | K 線快取（5min, 60min, daily） |
| `daily_kbar` | 日線資料（含美股） |
| `chip_snapshot` | 法人買賣超 |
| `daytrade_snapshot` | 當沖比率 |
| `news_cache` | 新聞快取 |

#### 策略實驗室
| 表名 | 用途 |
|------|------|
| `iteration_session` | 迭代 session（status, best_sharpe, rounds） |
| `iteration_round` | 每輪記錄（layer, params, sharpe, improvement） |
| `backtest_result` | 回測結果快取 |

#### 策略工廠（AI 自動生成策略）
| 表名 | 用途 |
|------|------|
| `sf_strategy` | AI 策略（code, performance, lifecycle） |
| `sf_backtest_run` | 工廠回測記錄 |
| `sf_session` | 工廠 session |
| `sf_knowledge` | 結構化知識（FTS5 全文搜索） |
| `sf_llm_queue` | LLM 呼叫佇列（handoff 機制核心） |

#### 資訊中心
| 表名 | 用途 |
|------|------|
| `ic_settings` | AI 設定（模型、來源） |
| `ic_recommendations` | AI 推薦紀錄 |
| `ic_rec_history` | 推薦歷史績效 |
| `ic_news_sources` | 自訂資料來源 |
| `ic_news_cache` | 新聞快取 |
| `ic_kb_chunks` | 知識庫 RAG chunks |
| `ic_token_usage` | Token 用量追蹤 |
| `ic_sentiment_history` | 情緒歷史 |
| `stock_names` | 股票名稱對照 |

#### 專家委員會
| 表名 | 用途 |
|------|------|
| `expert_sessions` | 分析 session |
| `expert_opinions` | 各專家意見 |
| `expert_schedules` | 排程設定 |
| `expert_config` | 專家 AI 設定 |

### 3.2 market.db（行情庫）

| 表名 | 用途 |
|------|------|
| `daily_kbar` | 日 K 線資料（台+美） |

---

## 四、API 架構

### 4.1 認證

所有 API 需 `X-API-Token` header。Token 每次重啟重新產生：

```bash
TOKEN=$(curl -s http://localhost:8765/api/auth/token | python -c "import sys,json;print(json.load(sys.stdin)['token'])")
```

### 4.2 主要 API 群組（168 routes）

| 群組 | prefix | routes 數 | 說明 |
|------|--------|-----------|------|
| 行情 | `/api/kbar`, `/api/market-data` | ~15 | K 線、快照、五檔 |
| 持倉 | `/api/positions`, `/api/us/positions` | ~12 | CRUD + 損益計算 |
| 交易 | `/api/trades` | ~8 | 買賣紀錄 + 分析 |
| 自選 | `/api/watchlist` | ~6 | 新增/移除/列表 |
| 訊號 | `/api/signals` | ~5 | 掃描 + 歷史 |
| 策略 | `/api/strategies` | ~5 | 列表/切換/參數 |
| 回測 | `/api/backtest` | ~5 | 回測 + Walk-Forward |
| 籌碼 | `/api/chip` | ~12 | 法人/融券/當沖/擠壓 |
| 風控 | `/api/risk-config`, `/api/macro-lock` | ~5 | 設定 + 鎖定 |
| 專家 | `/api/expert` | ~10 | Session + 意見 + 排程 |
| 資訊中心 | `/api/ic` | ~30 | 推薦/分析/板塊/量化/AI/知識庫 |
| 系統透視 | `/api/datasources`, `/api/formula-registry` | ~8 | 資料源/功能/公式 |
| 策略實驗室 | `/api/iteration` | ~10 | 迭代優化 |
| 策略工廠 | `/api/sf` | ~16 | AI 策略生成 |
| 盤後 | `/api/after-hours` | ~5 | 籌碼掃描 |
| 通知 | `/api/notify` | ~3 | Telegram/Email |
| 系統 | `/api/auth`, `/api/data`, `/api/auto-sell` | ~10 | 認證/備份/自動賣 |

### 4.3 策略工廠 API 詳情

```
POST /api/sf/session/start          啟動工廠 session
  body: {codes: ["NVDA"], market: "US", start: "2023-01-01", end: "2025-01-01", num_strategies: 3, mode: "explore"}

GET  /api/sf/session/{sid}/live     即時進度（status, logs, strategies_created）
GET  /api/sf/session/{sid}          詳情
POST /api/sf/session/{sid}/stop     停止
GET  /api/sf/sessions               列出所有

GET  /api/sf/strategies             AI 策略列表
GET  /api/sf/strategies/{id}        策略詳情（含程式碼）
POST /api/sf/strategies/{id}/promote   晉升策略
POST /api/sf/strategies/{id}/archive   歸檔
POST /api/sf/strategies/{id}/retest    重新回測
POST /api/sf/strategies/{id}/evolve    演化（AI 改良）

GET  /api/sf/knowledge              知識庫
POST /api/sf/knowledge/search       全文搜索
GET  /api/sf/leaderboard            排行榜

GET  /api/sf/llm-queue              待處理 LLM 佇列（handoff 用）
POST /api/sf/llm-queue/{id}/respond  回填 LLM 回應（handoff 用）
```

---

## 五、LLM Queue Handoff 機制

### 5.1 問題背景

`claude -p` (subscription CLI subprocess) 與活躍的 Claude Code session 衝突，永遠 120 秒 timeout。

### 5.2 解決方案

```
策略工廠 Controller
    │
    ├─ 嘗試 1: subscription (claude -p) ──→ timeout 120s
    ├─ 嘗試 2: API (Anthropic API key) ──→ 無 key 則失敗
    └─ 嘗試 3: Queue Handoff
         │
         ├─ INSERT INTO sf_llm_queue (prompt, status='pending')
         ├─ 每 5 秒 poll 等待 response（最多 30 分鐘）
         │
         └─ Agent（Claude Code 或 sf_agent.py）
              ├─ GET /api/sf/llm-queue → 讀取 prompt
              ├─ 生成策略碼 / 知識萃取 / 修復
              └─ POST /api/sf/llm-queue/{id}/respond → 回填
```

### 5.3 使用方式

#### 方式 A：Claude Code 對話中手動 handoff
```
你：「幫我跑策略工廠研究 NVDA」
我：啟動 session → 監控 queue → 自動生成策略 → 回填 → 回報結果
```

#### 方式 B：sf_agent.py 獨立腳本
```bash
cd ui/
# 先啟動 session
curl -X POST http://localhost:8765/api/sf/session/start \
  -H "Content-Type: application/json" \
  -H "X-API-Token: $TOKEN" \
  -d '{"codes":["NVDA"],"market":"US","start":"2023-01-01","end":"2025-01-01","num_strategies":3}'

# 再跑 agent（會自動監聽 queue 並回填）
python sf_agent.py <session_id>
```

### 5.4 sf_agent.py 擴展點

目前 `sf_agent.py` 內建 3 個策略模板。若要擴展：
1. 在 `STRATS` list 加入新策略（`evaluate(ctx)` 函數 + METADATA）
2. 或改為讀取 queue 裡的 prompt 後呼叫外部 LLM API 生成

---

## 六、核心運作邏輯

### 6.1 策略訊號流程

```
行情更新 → 計算指標（MA/RSI/KD/MACD/BB/ATR）
  → 28 種策略各自 evaluate → signal_log
  → EXIT_D 掃描（不可關閉） → 自動停損
  → 風控檢查（VIX/DXY/US10Y/指數偏離）→ MACRO_LOCK
```

### 6.2 回測引擎（固定策略）

```python
_run_backtest(config) → {summary, trades, equity_curve}
  config: {codes, market, start, end, capital, strategies, trade_type, commission_discount}
  summary: {total_return_pct, sharpe_ratio, win_rate, max_drawdown, profit_factor, total_trades}
```

**重要修復紀錄**：
- yfinance MultiIndex：`df.columns = df.columns.get_level_values(0)`
- 台美股 lot size：`lot = 1000 if mkt == "TW" else 1`（買賣/淨值/強平全部要套用）
- signal 型別相容：`int (1/-1/0)` 和 `str ("BUY"/"SELL"/"HOLD")` 都要支援

### 6.3 策略工廠（AI 動態策略）

```python
_strategy_factory_controller(session_id):
  1. 知識收集 → sf_knowledge FTS5 搜索
  2. LLM 生成策略碼 → _sf_llm_call() (直連/queue)
  3. 驗證 → compile() + dry run + 去重
  4. 動態回測 → _run_dynamic_backtest(config, strategy_code)
  5. Walk-Forward → _sf_walk_forward()
  6. 知識萃取 → LLM 提取策略洞察
  7. 存入 sf_strategy（lifecycle: draft → testing → validated → promoted）
```

### 6.4 策略實驗室（參數迭代）

```
Layer 1: Grid Search → 暴力搜索最佳參數
Layer 2: Bayesian (Optuna) → 精搜最佳值
Layer 3: AI 分析 → Claude 分析弱點並建議
收斂：連續 3 輪 Sharpe 改善 < 0.05 或 maxRound 20
```

### 6.5 資訊中心 AI 推薦

```
掃描自選股 → 計算 14 指標 → 100 分評分 → BUY/SELL/HOLD
  → AI 深度解讀（Claude） → 儲存推薦歷史 → 自動評估績效
TW/US 市場完全隔離，互不覆蓋
```

---

## 七、啟動與運行

### 7.1 環境需求

```bash
pip install fastapi uvicorn shioaji yfinance pandas numpy optuna
```

### 7.2 啟動

```bash
cd ui/
python backend.py          # localhost:8765
ngrok http 8765 --domain=your-domain.ngrok-free.dev   # 外部存取（選配）
```

### 7.3 取得 API Token

```bash
TOKEN=$(curl -s http://localhost:8765/api/auth/token | python -c "import sys,json;print(json.load(sys.stdin)['token'])")
```

### 7.4 驗證測試

```bash
# E2E 測試（23 項）
PYTHONIOENCODING=utf-8 python test_iter_ui.py

# 完整 API 測試（141 endpoint）
python ../tests/test_full_io.py "$TOKEN"
```

---

## 八、設定鍵值

### 8.1 ic_settings（資訊中心/AI 設定）

| key | 說明 | 預設值 |
|-----|------|--------|
| `source_stock_analyze` | AI 來源 | `subscription` |
| `model_stock_analyze` | AI 模型 | `claude-sonnet-4-6` |
| `api_key` | Anthropic API key | （空） |

### 8.2 risk_config

| key | 說明 | 預設值 |
|-----|------|--------|
| `swing_stop_loss` | 波段停損% | 5 |
| `daytrade_stop_loss` | 當沖停損% | 1 |
| `exitd_threshold` | EXIT_D 停損% | 5 |
| `max_position_pct` | 單檔上限% | 20 |
| `max_positions` | 最大持倉數 | 10 |
| `telegram_bot_token` | Telegram bot | （空） |
| `smtp_*` | Email SMTP | （空） |

---

## 九、前端頁面結構

index.html 的 12 個頁面（SPA, `showPage('xxx')` 切換）：

| ID | 頁面 | 圖示 |
|----|------|------|
| `home` | 首頁總覽 | 🏠 |
| `chart` | K 線圖表 | 📈 |
| `portfolio` | 持倉管理 | 💼 |
| `trades` | 交易紀錄 | ➕ |
| `after-hours` | 盤後分析 | 📊 |
| `risk` | 風控中心 | 🛡 |
| `system-view` | 系統透視 | 📦 |
| `strategies` | 策略管理 | ⭐ |
| `backtest` | 回測引擎 | ⏱ |
| `expert` | 專家委員會 | 🧠 |
| `info-center` | 資訊中心 | ❓ |
| `settings` | 系統設定 | ⚙️ |
| `iteration` | 策略實驗室 | 🔬 |
| `factory` | 策略工廠 | 🏭 |

---

## 十、已知 Bug 修復紀錄

這些 bug 已修復，但新專案接手時需注意是否有相似模式：

| Bug | 根因 | 修復位置 |
|-----|------|---------|
| yfinance 0 trades | MultiIndex columns `('Close','AAPL')` | `_fetch_us_daily`, `_fetch_tw_daily` |
| 美股 lot 計算錯 | 硬編碼 `*1000` | 買/賣/淨值/強平 4 處 |
| Grid Search OOM | `itertools.product` 14 維 = 6B | 超限改隨機抽樣 |
| WF 忽略最佳參數 | 未 apply best_params | `_apply_params_to_config` |
| signal int vs str | `evaluate()` 回傳 `1` 但 code 用 `.upper()` | `_run_dynamic_backtest` line 7169 |
| SF controller import | `re`, `math`, `numpy` 未 import | 函數頂部加入 |
| SF walk-forward np | `np.mean` 未 import | `_sf_walk_forward` 加 `import numpy as np` |
| CSS variables | `--sell`, `--accent` 未定義 | `:root` 加入 |
| Expert session stuck | 無 try/finally | 包 `try/except/finally` |
| IC 推薦互蓋 | TW/US 共用 key | 市場隔離 |

---

## 十一、給新專案的建議

### 11.1 如果要複用回測引擎

```python
from backend import _run_backtest, _run_dynamic_backtest

# 固定策略回測
result = _run_backtest({
    "codes": ["NVDA", "AAPL"],
    "market": "US",
    "start": "2023-01-01",
    "end": "2025-01-01",
    "capital": 1000000,
    "strategies": ["BUY_A", "EXIT_C"],
    "commission_discount": 0.6
})

# 動態策略回測（AI 生成的 code）
result = _run_dynamic_backtest(config, strategy_code_string)
```

### 11.2 如果要複用策略工廠

1. 確保 `sf_llm_queue` 表存在
2. 呼叫 `POST /api/sf/session/start` 啟動
3. 用 `sf_agent.py` 或自己的 agent 處理 queue
4. 用 `GET /api/sf/session/{sid}/live` 監控進度

### 11.3 如果要複用 AI 分析

```python
from backend import _ic_llm_call

response = _ic_llm_call(prompt, model="claude-sonnet-4-6", source="subscription", max_tokens=2000)
```

### 11.4 如果要拆分為微服務

建議拆法：
1. **行情服務**：kbar_cache, daily_kbar, market.db → 獨立 FastAPI
2. **交易服務**：positions, trade_records, risk_config → 獨立 FastAPI
3. **分析引擎**：backtest, iteration, strategy_factory → 獨立 worker
4. **AI 服務**：LLM 呼叫統一入口 + queue → 獨立 worker
5. **前端**：index.html → React/Vue SPA

---

## 十二、測試清單

| 測試 | 檔案 | 涵蓋 |
|------|------|------|
| E2E 策略實驗室 | `ui/test_iter_ui.py` | 23 項，全通過 |
| E2E 策略工廠 | `ui/test_sf_ui.py` | 工廠流程 |
| API 煙霧測試 | `tests/test_api_smoke.py` | 基本 API |
| 完整 IO 測試 | `tests/test_full_io.py` | 141 endpoints |
| 整合測試 | `tests/test_integration.py` | 跨功能整合 |
| UI 全功能 | `tests/test_ui_full.py` | 前端功能 |

---

## 十三、分析師團隊推薦標的

（來自 2026-06-22 分析師團隊報告，實際回測驗證）

### 核心持倉（低頻穩健）
AAPL, MSFT, GOOGL, 2330(台積電), 2881(富邦金)

### 波段交易（中頻）
NVDA, AVGO, META, AMZN, TSM, 2454(聯發科), 2382(廣達), 3711(日月光), 2308(台達電)

### 積極交易（高頻）
TSLA, AMD, 2317(鴻海), 2603(長榮)

### 應避免
UNH, 1301(台塑), JPM/V/MA/LLY 組合

---

*本文件為 Smart Investment Monitor 專案的完整 handoff 文件。新 Claude session 讀取本文件即可獲得完整上下文。最後更新：2026-06-22*
