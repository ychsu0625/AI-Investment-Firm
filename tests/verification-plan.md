# Smart Investment Monitor — 系統驗證計畫

> 設計目標：確認每條路徑暢通、每個公式產出符合預期、參數修改能正確傳播影響鏈  
> 總計 120+ API endpoints、12 頁面、38 條公式、50 個資料源

---

## 驗證架構：四層金字塔

```
        ╱ L4: 端到端場景 ╲        ← 模擬真實用戶操作流程
       ╱ L3: 資料流貫穿    ╲       ← 跨模組資料一致性
      ╱ L2: 公式正確性       ╲      ← 公式計算結果 vs 手動驗算
     ╱ L1: API 路徑連通        ╲     ← 每個 endpoint 回應正確
```

---

## L1：API 路徑連通（Smoke Test）

每個 endpoint 驗證：HTTP status 200、回傳格式正確、必要欄位存在。

### 執行方式
建立 `test_api_smoke.py`，用 `httpx` 打每個 GET endpoint，驗 status + schema。

### 分組檢測清單

#### 1.1 核心基礎（必須全過才繼續）
| # | Method | Endpoint | 驗證 |
|---|--------|----------|------|
| 1 | GET | `/api/health` | status=200, body 含 `ok` |
| 2 | GET | `/api/info` | 含 `version`, `market_status` |
| 3 | GET | `/api/auth/token` | 回傳 token 字串 |

#### 1.2 自選股 & 持倉
| # | Method | Endpoint | 驗證 |
|---|--------|----------|------|
| 4 | GET | `/api/watchlist` | array, 每項含 `code`,`name` |
| 5 | GET | `/api/watchlist/list` | array |
| 6 | GET | `/api/positions` | array, 每項含 `id`,`code`,`shares`,`cost` |
| 7 | GET | `/api/positions/{pid}/exit-check` | 含 `triggered`, `checks` |

#### 1.3 行情 & K線
| # | Method | Endpoint | 驗證 |
|---|--------|----------|------|
| 8 | GET | `/api/snapshot/{code}` | 含 `price` 或 error msg |
| 9 | GET | `/api/kbars/{code}?tf=D` | array, 每項含 `time`,`open`,`high`,`low`,`close`,`volume` |
| 10 | GET | `/api/kbars/{code}/indicators` | 含 MA/RSI 等 |
| 11 | GET | `/api/kbars/{code}/strategy-markers` | array |
| 12 | GET | `/api/vwap/{code}` | 含 `vwap` 數值 |
| 13 | GET | `/api/sparkline/{code}` | array of numbers |

#### 1.4 風控 & 總經
| # | Method | Endpoint | 驗證 |
|---|--------|----------|------|
| 14 | GET | `/api/macro` | 含 `VIX`,`DXY`,`US10Y` |
| 15 | GET | `/api/risk-level` | 含 `level` ∈ {NORMAL,CAUTION,ALERT} |
| 16 | GET | `/api/macro-lock` | 含 `locked` bool |
| 17 | GET | `/api/risk-config` | 含 config dict |

#### 1.5 訊號 & 掃描
| # | Method | Endpoint | 驗證 |
|---|--------|----------|------|
| 18 | GET | `/api/signals` | array |
| 19 | GET | `/api/scan/exitd` | array |
| 20 | GET | `/api/scan/signals` | array |
| 21 | GET | `/api/scan/after-hours` | 含結果 |

#### 1.6 籌碼分析
| # | Method | Endpoint | 驗證 |
|---|--------|----------|------|
| 22 | GET | `/api/chip/{code}` | 含 `foreign`,`itrust`,`dealer` |
| 23 | GET | `/api/chip/scheduler-status` | 含 `enabled` |
| 24 | GET | `/api/chip/squeeze-candidates` | array |
| 25 | GET | `/api/chip/itrust-lock` | array |
| 26 | GET | `/api/chip/abandon` | array |
| 27 | GET | `/api/chip/daytrade-warn` | array |
| 28 | GET | `/api/chip/squeeze-breakout` | array |

#### 1.7 新聞
| # | Method | Endpoint | 驗證 |
|---|--------|----------|------|
| 29 | GET | `/api/news/{code}` | array |
| 30 | GET | `/api/news/bearish-reversal` | array |

#### 1.8 美股
| # | Method | Endpoint | 驗證 |
|---|--------|----------|------|
| 31 | GET | `/api/us/watchlist` | array |
| 32 | GET | `/api/us/indices` | 含 `SPY`,`QQQ` 等 |
| 33 | GET | `/api/us/positions` | array |
| 34 | GET | `/api/us/kbars/{symbol}` | array |
| 35 | GET | `/api/us/scan/signals` | array |

#### 1.9 策略 & 回測
| # | Method | Endpoint | 驗證 |
|---|--------|----------|------|
| 36 | GET | `/api/strategies` | array, len≥12, 每項含 `id`,`name`,`enabled`,`params` |
| 37 | GET | `/api/backtest/history` | array |
| 38 | GET | `/api/market-data/status` | 含 status |

#### 1.10 資訊中心 (IC)
| # | Method | Endpoint | 驗證 |
|---|--------|----------|------|
| 39 | GET | `/api/ic/settings` | 含 `source_mode` |
| 40 | GET | `/api/ic/token-usage` | 含 `total_tokens` |
| 41 | GET | `/api/ic/macro` | 含 macro data |
| 42 | GET | `/api/ic/us/sectors` | array |
| 43 | GET | `/api/ic/sector-rotation` | array, 每項含 `symbol`,`sector`,`pct_1m`,`rank` |
| 44 | GET | `/api/ic/events/{code}` | 含 `tags`,`news` |
| 45 | GET | `/api/ic/factors/{code}` | 含 20 factor keys |
| 46 | GET | `/api/ic/social-sentiment/{code}` | 含 `score` |
| 47 | GET | `/api/ic/options/{code}` | 含 `put_call_ratio` 或 error |
| 48 | GET | `/api/ic/crypto/{symbol}` | 含 `price` |
| 49 | GET | `/api/ic/openbb/status` | 含 `installed` |
| 50 | GET | `/api/ic/sources` | 含 `system`,`user`,`embedding` |
| 51 | GET | `/api/ic/sources/news` | array |
| 52 | GET | `/api/ic/recommendations` | array |
| 53 | GET | `/api/ic/recommendations/history` | array |
| 54 | GET | `/api/ic/notify-config` | 含 config |
| 55 | GET | `/api/ic/sentiment-history/{code}` | array |
| 56 | GET | `/api/ic/kb/entities` | array |
| 57 | GET | `/api/ic/info_center` | HTML 或 JSON |

#### 1.11 系統透視（本次新增）
| # | Method | Endpoint | 驗證 |
|---|--------|----------|------|
| 58 | GET | `/api/datasources` | array len=50, 每項含 `id`,`status`,`market_scope`,`ic_used` |
| 59 | GET | `/api/feature-datasource-map` | array len=20, 每項含 `datasource_details` |
| 60 | GET | `/api/formula-registry` | array len=38, 每項含 `id`,`category`,`formula`,`params`,`external` |

#### 1.12 資料管理 & GitHub
| # | Method | Endpoint | 驗證 |
|---|--------|----------|------|
| 61 | GET | `/api/data/stats` | 含 `tables` |
| 62 | GET | `/api/data/integrity` | 含 `checks` |
| 63 | GET | `/api/github/status` | 回應 |

---

## L2：公式正確性（Unit Test）

用已知輸入驗算每條公式的輸出，確認程式碼邏輯與公式說明一致。

### 執行方式
建立 `test_formulas.py`，直接 import backend 函數或透過 API 傳入已知數據。

### 2.1 技術指標計算驗證

| # | 公式 | 測試方法 | 預期 |
|---|------|----------|------|
| F1 | KD(9) | 給定 9 日 H/L/C 序列 → 計算 RSV/K/D | K/D 值 ±0.01 |
| F2 | MACD(12,26,9) | 給定 30 日收盤價 → 計算 DIF/MACD | DIF/MACD 值 ±0.01 |
| F3 | RSI(14) | 給定 15 日收盤價 → 計算 RSI | RSI 值 ±0.1 |
| F4 | RVOL | 給定 25 日成交量 → 當日/MA20 比 | RVOL 值 ±0.01 |
| F5 | VWAP(20) | 給定 20 日 H/L/C/V → TP加權均價 | VWAP 值 ±0.01 |
| F6 | OBV | 給定 21 日 C/V → 累計 OBV trend | 方向正確 |
| F7 | MFI(14) | 給定 15 日 H/L/C/V → MFI | 值 ∈ [0,100] |

### 2.2 Alpha 因子驗證

| # | 因子 | 測試方法 | 預期 |
|---|------|----------|------|
| F8 | mom_20d | 收盤100→120 | mom_20d = 20.0 |
| F9 | vol_ratio_5_20 | 近5日量=200, 前20日均=100 | ratio = 2.0 |
| F10 | bias_20d | 收盤120, MA20=100 | bias = 20.0 |
| F11 | price_pos_60d | C=80, L60=60, H60=100 | pos = 0.5 |
| F12 | amplitude_5d | 5日平均(H-L)/C | 值 > 0 |

### 2.3 評分邏輯驗證

| # | 場景 | 輸入條件 | 預期分數影響 |
|---|------|----------|-------------|
| S1 | KD 金叉超賣區 | K從15→25穿D, K<80 | +20 |
| S2 | RSI 超買 | RSI=75 | -10 |
| S3 | 完美多頭排列 | P>MA5>MA10>MA20>MA60 | +25 |
| S4 | 放量突破 | RVOL=2.0 | +10 |
| S5 | 量價頂背離 | 價+5%, 量-20% | -8 |
| S6 | 情緒過熱反轉 | sentiment=90 | -4 |
| S7 | 情緒冰點反轉 | sentiment=10 | +4 |
| S8 | 低PE高息 | PE=12, DY=5% | +5+3 = +8 |
| S9 | 強勢板塊 | sector rank=2 | +5 |
| S10 | BUY 判定 | raw_score=25 → +40=65 | direction=BUY (≥62) |
| S11 | SELL 判定 | raw_score=-5 → +40=35 | direction=SELL (≤38) |
| S12 | HOLD 判定 | raw_score=10 → +40=50 | direction=HOLD |

### 2.4 風控閾值驗證

| # | 場景 | 輸入 | 預期 |
|---|------|------|------|
| R1 | VIX 警戒 | VIX=40 | alert_count += 1 |
| R2 | 雙警報 | VIX=40 + US10Y=5.5 | level=ALERT, scale=30% |
| R3 | 單警報 | 只有 DXY月漲4% | level=CAUTION, scale=60% |
| R4 | 正常 | 所有指標正常 | level=NORMAL, scale=100% |

### 2.5 出場邏輯驗證

| # | 場景 | 輸入 | 預期 |
|---|------|------|------|
| E1 | EXIT_D 停損 | cost=100, price=94 (PnL=-6%) | 觸發 EXIT_D |
| E2 | EXIT_C 波段 | highest=110, price=107.5 (drawdown=2.27%) | 觸發 EXIT_C |
| E3 | EXIT_C 未觸發 | highest=110, price=109 (drawdown=0.9%) | 不觸發 |
| E4 | EXIT_D 保命不可關 | 嘗試 disable EXIT_D | 應被拒絕 |

### 2.6 IC/多因子驗證

| # | 場景 | 測試 | 預期 |
|---|------|------|------|
| IC1 | Spearman IC | 完美正相關因子 | IC ≈ 1.0 |
| IC2 | Spearman IC | 隨機因子 | IC ≈ 0.0, significant=false |
| IC3 | Z-score | values=[1,2,3,4,5] | z=[−1.26, −0.63, 0, 0.63, 1.26] ±0.01 |
| IC4 | 負向因子反轉 | bias_20d z=0.8 | 反轉為 z=-0.8 |

### 2.7 AI 信心度驗證

| # | 場景 | 輸入 | 預期 |
|---|------|------|------|
| A1 | 高分 | tech=80, 3 confirmed src, VIX=20 | conf = 0.50 + (80-50)/50×0.28 + 0.15 = 0.818 → clamp 0.82 |
| A2 | 低分 | tech=20, 0 src, VIX=30 | conf = 0.50 + (20-50)/50×0.28 + 0 - 0.08 = 0.252 → clamp 0.28 |
| A3 | 推薦門檻 | conf=0.72 | 應推播通知 (≥0.70) |
| A4 | 不推薦 | conf=0.65 | 不推播 |

---

## L3：資料流貫穿（Integration Test）

驗證跨模組的資料一致性和連動。

### 3.1 系統透視三維度一致性

| # | 測試 | 方法 | 預期 |
|---|------|------|------|
| D1 | 資料源↔IC關聯 | `/api/datasources` 中 `ic_used=true` 的 id 集合 == `IC_SYSTEM_SOURCES` 中所有 `datasource_ids` 的聯集 | 完全一致 |
| D2 | 功能↔資料源 | `/api/feature-datasource-map` 每個 feature 的 `datasource_details` 都有 `name`,`status` | 無 null |
| D3 | 公式↔功能 | `/api/formula-registry` 每條的 `feature` 都存在於 feature-map 的 `id` 中 | 完全匹配 |
| D4 | 公式↔策略連動 | 有 `strategy_link` 的公式 → 該 strategy ID 存在於 `/api/strategies` | 全部找到 |

### 3.2 參數修改傳播鏈

| # | 測試 | 步驟 | 預期 |
|---|------|------|------|
| P1 | 公式參數修改 | POST `/api/formula-registry/params` 改 `rsi_oversold=25` → GET registry | value=25, default 仍=30 |
| P2 | 公式參數重置 | POST `/api/formula-registry/reset` → GET registry | value 回 30 |
| P3 | 策略參數修改 | PUT `/api/strategies/EXIT_C/params` 改 swing_profit → GET strategies | 參數已更新 |
| P4 | 參數不互相干擾 | 改公式參數 → 不影響策略頁的值（兩者目前獨立存儲） | 各自獨立 |

### 3.3 IC 分析完整性

| # | 測試 | 步驟 | 預期 |
|---|------|------|------|
| IC5 | 分析→評分→歷史 | POST `/api/ic/analyze` → 檢查回傳含 `score`,`direction`,`signals` | 結構完整 |
| IC6 | 評分含新指標 | 分析結果的 `detail` 含 `RS`,`OBV`,`MFI`,`SENTIMENT_COMPOSITE`,`SECTOR`,`ALPHA`,`EVENT` | 全部存在 |
| IC7 | 情緒歷史記錄 | 分析後 GET `/api/ic/sentiment-history/{code}` | 新增一筆記錄 |
| IC8 | 板塊輪動一致 | `/api/ic/sector-rotation` 回傳 11 個板塊，排名 1-11 不重複 | rank 唯一 |
| IC9 | 事件偵測 | `/api/ic/events/{code}` 含 `tags` array | 非 null |
| IC10 | 因子計算 | `/api/ic/factors/{code}` 含 20 個因子 key | 全部為數值 |

### 3.4 回測完整性

| # | 測試 | 步驟 | 預期 |
|---|------|------|------|
| BT1 | 基本回測 | POST `/api/backtest/run` → 檢查結果 | 含 `total_return_pct`,`sharpe`,`max_drawdown`,`trades` |
| BT2 | Walk-Forward | POST `/api/backtest/walk-forward` | 含 `windows` array, 每個含 train/test metrics |
| BT3 | 歷史查詢 | GET `/api/backtest/history` → GET `/api/backtest/{id}` | 可取回完整結果 |

---

## L4：端到端場景（E2E Scenario Test）

模擬分析師的真實操作流程，從頭到尾走一遍。

### 場景 A：新股票研究流程
```
1. GET /api/us/watchlist                        → 取得自選股列表
2. POST /api/us/watchlist/add/AAPL              → 加入 AAPL
3. GET /api/us/kbars/AAPL                       → 取得 K 線
4. POST /api/ic/analyze {code:"AAPL",market:"US"} → AI 分析
   驗證：回傳含 score, direction, signals, detail
   驗證：detail 含 KD/MACD/RSI/RS/OBV/MFI/VWAP/RVOL/FUND/SECTOR/EVENT/SENTIMENT_COMPOSITE/ALPHA
5. GET /api/ic/factors/AAPL                     → 查看 Alpha 因子
   驗證：20 個因子都有數值
6. GET /api/ic/events/AAPL                      → 查看事件
7. GET /api/ic/options/AAPL                     → 查看期權鏈
8. GET /api/ic/sector-rotation                  → 板塊排名
   驗證：AAPL 所屬板塊（XLK 科技）有排名
```

### 場景 B：參數調優→回測驗證流程
```
1. GET /api/formula-registry                    → 取得全部公式
2. POST /api/formula-registry/params            → 修改 RSI oversold=25, MACD fast=10
   {changes: {rsi_oversold: 25, macd_fast: 10}}
3. GET /api/formula-registry                    → 確認值已更新
4. POST /api/backtest/run                       → 用新參數回測
   {code:"2330", start:"2024-01-01", end:"2024-12-31"}
5. 記錄結果 A
6. POST /api/formula-registry/reset             → 重置參數
7. POST /api/backtest/run                       → 用預設參數回測（相同條件）
8. 記錄結果 B
9. 比較 A vs B                                  → 確認參數變更確實影響回測結果
```

### 場景 C：風控觸發→倉位管控流程
```
1. GET /api/macro                               → 取得當前總經數據
2. GET /api/risk-level                          → 檢查風險等級
3. 假設 VIX=40:
   → risk-level 應為 CAUTION 或 ALERT
   → position_scale < 100%
4. GET /api/scan/exitd                          → EXIT_D 掃描
   → 虧損超過 5% 的持倉應出現
5. GET /api/positions/{pid}/exit-check           → 單一持倉出場檢查
```

### 場景 D：系統透視三維度瀏覽
```
1. GET /api/datasources                         → 50 個資料源
   驗證：至少 9 個 ic_used=true
   驗證：每個含 market_scope ∈ {TW,US,ALL,CRYPTO}
2. GET /api/feature-datasource-map              → 20 個功能
   驗證：每個功能至少 1 個 datasource_detail
   驗證：每個 detail 含 status, configured
3. GET /api/formula-registry                    → 38 條公式
   驗證：每條含 category, formula (非空), external (dict)
   驗證：有 params 的公式，每個 param 含 key, value, default, min, max
   驗證：有 strategy_link 的公式 → link ID 存在於 /api/strategies
```

### 場景 E：知識庫 RAG 流程
```
1. POST /api/ic/sources {name:"test", type:"TEXT", content:"台積電法說會重點..."}
   → 新增來源
2. GET /api/ic/sources                          → 確認出現在 user sources
3. GET /api/ic/kb/search?q=台積電                → 搜尋知識庫
   驗證：回傳含剛入庫的 chunk
4. POST /api/ic/analyze {code:"2330"}           → 分析時引用知識
```

### 場景 F：多因子量化流程
```
1. POST /api/ic/multi-factor {codes:["AAPL","MSFT","GOOGL"], market:"US"}
   → 回傳 Z-score 排名
   驗證：每個股票含 composite, rank, factors
2. POST /api/ic/factor-ic {codes:["AAPL","MSFT","GOOGL"], factor_name:"mom_20d"}
   → 回傳 IC 值
   驗證：含 ic, p_value, significant, strength
3. POST /api/backtest/walk-forward {code:"AAPL", ...}
   → Walk-Forward 回測
   驗證：含 windows, overfit_ratio, consistency_score
```

---

## 執行腳本設計

### test_api_smoke.py — L1 自動化
```python
"""
L1 Smoke Test：打所有 GET endpoints，驗證 status=200 + 基本 schema。
用法：python test_api_smoke.py [--base http://localhost:8765]
輸出：PASS/FAIL 清單 + 總結
"""
# 結構：
# ENDPOINTS = [
#   ("GET", "/api/health", ["ok"]),
#   ("GET", "/api/datasources", None, lambda r: len(r)>=50),
#   ...
# ]
# for method, path, required_keys, validator in ENDPOINTS:
#     resp = httpx.request(method, BASE+path)
#     assert resp.status_code == 200
#     if required_keys: assert all(k in data for k in required_keys)
#     if validator: assert validator(data)
```

### test_formulas.py — L2 自動化
```python
"""
L2 Formula Test：用已知數據驗算公式輸出。
直接 import backend.py 中的計算函數，傳入固定輸入，比對預期輸出。
"""
# 結構：
# def test_rsi():
#     closes = [44,44.34,44.09,43.61,44.33,44.83,45.10,45.42,45.84,46.08,
#               45.89,46.03,45.61,46.28,46.28,46.00,46.03,46.41,46.22,45.64]
#     rsi = _calc_rsi(closes, 14)
#     assert abs(rsi - 70.46) < 0.5
```

### test_integration.py — L3 自動化
```python
"""
L3 Integration Test：驗證跨模組資料一致性。
"""
# 結構：
# def test_datasource_ic_consistency():
#     ds = GET("/api/datasources")
#     ic_used_ids = {d["id"] for d in ds if d["ic_used"]}
#     formulas = GET("/api/formula-registry")
#     fm = GET("/api/feature-datasource-map")
#     # 驗證三維度交叉一致
```

### test_e2e.py — L4 手動 + 半自動
```python
"""
L4 E2E Scenario：模擬完整用戶流程。
需要真實資料（yfinance），耗時較長，建議手動執行。
"""
```

---

## 前端頁面驗證清單

用瀏覽器逐頁操作確認。

| # | 頁面 | 操作 | 預期結果 |
|---|------|------|----------|
| U1 | 首頁 | 載入 | 自選股列表、持倉摘要、訊號顯示正常 |
| U2 | K線 | 點自選股 | 圖表渲染、指標疊加、策略標記正常 |
| U3 | 持倉 | 查看 | lifecycle badge 顯示、exit-check 可點 |
| U4 | 分析 | 載入 | 掃描結果正常 |
| U5 | 風控 | 載入 | VIX/DXY/US10Y 數據顯示、風險等級色塊 |
| U6 | 系統透視→按資料源 | 載入 | 50 張卡片、市場篩選、IC分析中 badge |
| U7 | 系統透視→按功能 | 切換 | 20 個功能、狀態指示燈、按頁面分組 |
| U8 | 系統透視→按公式 | 切換 | 38 條公式、手風琴展開、外部對照 badge |
| U9 | 按公式→調參數 | 改 RSI oversold | 黃色邊框、sticky toolbar 出現 |
| U10 | 按公式→儲存 | 點儲存 | toolbar 消失、重載後值保持 |
| U11 | 按公式→還原 | 點全部還原 | 所有值回 default |
| U12 | 按公式→送入回測 | 點送入回測 | 跳轉到回測頁 |
| U13 | 按公式→策略頁連動 | 點 EXIT_C 的「→策略頁」 | 跳轉到策略頁並選中 EXIT_C |
| U14 | 策略管理 | 載入 | 12 策略列表、點選顯示詳情、參數可調 |
| U15 | 回測 | 執行 | 結果含績效指標、權益曲線、月度報酬 |
| U16 | 資訊中心→分析 | 輸入股票代碼→分析 | 評分卡、12 指標面板、AI 報告 |
| U17 | IC→板塊輪動 | 切tab | 11 板塊排名、動量色塊 |
| U18 | IC→量化工具 | 切tab | 多因子/IC/Walk-Forward 工具 |
| U19 | IC→來源 | 切tab | 系統來源含「關聯資料源」欄、用戶來源 |
| U20 | IC→來源關聯 | 點系統來源的資料源 badge | 跳轉到系統透視→按資料源 |
| U21 | 設定 | 載入 | 通知設定、GitHub、資料管理 |

---

## 優先級排序

| 優先級 | 層級 | 測試數 | 建議時機 |
|--------|------|--------|----------|
| P0 | L1 核心 (#1-#3, #14-#17, #58-#60) | 9 | 每次部署前 |
| P1 | L1 完整 (全部 #1-#63) | 63 | 每週一次 |
| P2 | L2 公式 (F1-F12, S1-S12) | 24 | 改公式邏輯後 |
| P3 | L2 風控+出場 (R1-R4, E1-E4) | 8 | 改閾值後 |
| P4 | L3 一致性 (D1-D4, P1-P4) | 8 | 改系統透視後 |
| P5 | L3 IC+回測 (IC5-IC10, BT1-BT3) | 9 | 改分析邏輯後 |
| P6 | L4 場景 (A-F) | 6 | 重大功能更新後 |
| P7 | 前端 (U1-U21) | 21 | 改 UI 後 |

---

## 執行建議

1. **先跑 L1 Smoke Test** — 確認所有路徑通（約 2 分鐘）
2. **再跑 L2 公式驗算** — 確認計算正確（約 1 分鐘，純計算）
3. **L3 整合測試** — 確認資料流（約 3 分鐘，需真實 API）
4. **L4 場景按需** — 大改版後跑一輪（約 10 分鐘）
5. **前端 U1-U21** — 瀏覽器手動走一遍（約 15 分鐘）
