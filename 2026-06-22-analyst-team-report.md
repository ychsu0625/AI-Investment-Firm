# 智慧投顧系統 — 分析師團隊報告

> 日期：2026-06-22
> 團隊：標的研究員 / 策略分析師（回測驗證）/ 系統架構師

---

## 一、系統審計結果

### 修復的關鍵 Bug（8 項，全部已修）

| 等級 | 問題 | 影響 | 修復方式 |
|------|------|------|---------|
| P0 | yfinance MultiIndex columns | 所有美股資料無法讀取，0 筆交易 | `df.columns = df.columns.get_level_values(0)` |
| P0 | `* 1000` 硬編碼 lot size | 美股買賣成本計算錯誤 | `lot = 1000 if mkt == "TW" else 1` |
| P0 | 回測忽略參數變更 | Grid Search 200 組全部產出相同結果 | 改用 `_get_strategy_param()` 讀取 |
| P0 | Grid Search MemoryError | `itertools.product` 14 維 = 6B 組合 | 超過上限改隨機抽樣 |
| P1 | Walk-Forward 忽略最佳參數 | 驗證時用原始參數 | `_apply_params_to_config(best_params)` |
| P1 | LLM subscription CLI timeout | claude CLI 與活躍 session 衝突 | 自動 fallback 切換模式 |
| P1 | exec() sandbox 太寬鬆 | 完整 pd 模組可做檔案 I/O | 限制只暴露 Series/DataFrame |
| P1 | CSS variables 未定義 | `var(--sell)`, `var(--accent)` 無效 | 加入 `:root` 定義 |

### 修復後驗證

- **回測引擎**：5 支美股 18 個月 → 16 筆交易、Sharpe 1.50、勝率 56.2%
- **迭代系統**：Session #10 → Sharpe 0.00 → **1.52**、勝率 80%
- **E2E 測試**：23/23 全通過

---

## 二、推薦標的

### 回測績效排名（2023-06 ~ 2025-06 實測）

#### 個股獨立回測 Top 5

| 排名 | 股票 | 市場 | Sharpe | 報酬% | 勝率% | 盈虧比 | MDD% |
|------|------|------|--------|-------|-------|--------|------|
| 1 | **NVDA** | US | **1.78** | +4.29 | 75.0 | 5.16 | 0.80 |
| 2 | **2317 鴻海** | TW | **1.44** | +6.44 | 60.0 | 12.05 | 1.05 |
| 3 | AVGO | US | 0.77 | +4.38 | 55.6 | 2.05 | 1.37 |
| 4 | TSLA | US | 0.62 | +2.10 | 66.7 | 1.76 | 1.69 |
| 5 | 2330 台積電 | TW | 0.57 | +6.39 | 25.0 | 6.03 | 6.87 |

### 最終推薦標的清單

#### 核心持倉（低頻穩健）

| 代碼 | 名稱 | 市場 | 推薦策略 | 月均訊號 |
|------|------|------|---------|---------|
| AAPL | Apple | US | MA_ALIGN, WIFE_SIMPLE | 1-2 次 |
| MSFT | Microsoft | US | MACD_CROSS, LOW_BUY | 1-2 次 |
| GOOGL | Alphabet | US | MA_ALIGN, VOL_DIVERGENCE | 1-2 次 |
| 2330 | 台積電 | TW | SQUEEZE_BREAK, LOW_BUY | 2-3 次 |
| 2881 | 富邦金 | TW | KD_CROSS, WIFE_SIMPLE | 1-2 次 |

#### 波段交易（中頻）

| 代碼 | 名稱 | 市場 | 推薦策略 | 月均訊號 |
|------|------|------|---------|---------|
| NVDA | NVIDIA | US | SQUEEZE_BREAK, EXIT_C | 2-3 次 |
| AVGO | Broadcom | US | MACD_CROSS, BUY_B | 2-3 次 |
| META | Meta | US | SENTIMENT_REVERSAL, KD_CROSS | 2-3 次 |
| AMZN | Amazon | US | SQUEEZE_BREAK | 1-2 次 |
| TSM | 台積電 ADR | US | MACD_CROSS, KD_CROSS | 2-3 次 |
| 2454 | 聯發科 | TW | BUY_B, EXIT_C | 3-4 次 |
| 2382 | 廣達 | TW | MA_ALIGN, MACD_CROSS | 2-3 次 |
| 3711 | 日月光投控 | TW | RSI_EXTREME, EXIT_C | 2-3 次 |
| 2308 | 台達電 | TW | WIFE_SIMPLE, VOL_DIVERGENCE | 1-2 次 |

#### 積極交易（高頻）

| 代碼 | 名稱 | 市場 | 推薦策略 | 月均訊號 |
|------|------|------|---------|---------|
| TSLA | Tesla | US | BUY_A, BUY_B | 4-5 次 |
| AMD | AMD | US | SQUEEZE_BREAK, BUY_B | 3-4 次 |
| 2317 | 鴻海 | TW | BUY_A, SQUEEZE_BUY | 2-3 次 |
| 2603 | 長榮 | TW | BUY_A, SENTIMENT_REVERSAL | 3-5 次 |

#### 應避免

- UNH（聯合健康）：單筆 -17.89% 大虧
- 1301（台塑）：策略在傳產類股表現差
- G3 整組（JPM/V/MA/UNH/LLY）：Sharpe -0.45

---

## 三、維護與管理計畫

### 每日

| 項目 | 門檻 | 動作 |
|------|------|------|
| 行情資料新鮮度 | > 3 天黃燈，> 5 天紅燈 | `/api/market-data/{code}` 觸發補抓 |
| yfinance 可靠性 | 單日失敗率 > 20% | 切換備用資料源 |
| 訊號異常 | 同方向 > 20 筆或零訊號 | 檢查參數/資料源 |

### 每週

| 項目 | 門檻 | 動作 |
|------|------|------|
| 策略績效漂移 | Z-score < -2.0 | 重新優化 |
| 參數過時 | > 30 天 | 排入迭代 |
| AI 策略健康 | Overfit > 2.0 | 停用或重訓 |

### 每月

| 項目 | 標準 | 動作 |
|------|------|------|
| Walk-Forward 再驗證 | Overfit < 1.5 健康 / > 2.5 危險 | 退場或減倉 |
| 策略生命週期報表 | promoted 策略品質 | 淘汰劣策略 |

### 策略晉升條件（全部滿足）

- Sharpe(OOS) >= 0.8、勝率 >= 45%、MDD <= 25%
- WF Consistency >= 55%、Overfit <= 2.0
- 交易次數 >= 20、跨 >= 3 檔標的測試

### 策略退場條件（任一觸發）

- 近 3 月 Sharpe < 0.2 或 MDD > 35%
- WF Overfit > 3.0 或連虧 >= 3 月
- 無訊號 > 60 天

### 備份

| 項目 | 頻率 | 保留 |
|------|------|------|
| monitor.db | 每日 | 7 份 |
| market.db | 每週 | 4 份 |
| sf_strategy | 每次 promote/archive | 永久 |

---

## 四、下一步

1. 用推薦標的在策略實驗室跑完整迭代優化
2. 用策略工廠為 NVDA + 2317 生成新策略
3. 建立 `/api/health` 健康檢查端點
4. 實作 DB 自動備份腳本
5. 建立策略績效漂移監控
