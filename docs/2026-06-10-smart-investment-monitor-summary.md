# 智慧投顧監控系統 — Phase 1-8 完整摘要

**日期**：2026-06-10
**狀態**：PRD 五大區塊 100% 覆蓋

---

## 系統架構

```
後端：ui/backend.py（FastAPI + uvicorn, port 8765）
前端：ui/index.html（TradingView Lightweight Charts 4.1.3 + Vanilla JS）
資料庫：SQLite monitor.db（8 張表）
台股行情：Shioaji 1.5.2（永豐金，模擬盤預設）
美股行情：yfinance（K線、快照、指數）
總經資料：yfinance（VIX/DXY/US10Y/ES/TWII）
推播通知：Telegram Bot + Email SMTP + Webhook（三通道並行）
```

---

## Phase 總覽

| Phase | 範圍 | 狀態 |
|-------|------|------|
| 1 | 八大 Gap（G1-G8）+ 核心 K線 / 訊號 / 風控 | ✅ |
| 2 | Tick 即時 K線、總經定時輪詢、推播通知 | ✅ |
| 2.5 | highest_price 追蹤、移動止盈、VWAP、風控三級制 | ✅ |
| 3 | 籌碼模組（三大法人、融資融券、投信鎖碼） | ✅ |
| 4 | 進階訊號（Buy_A/B tick-level、LOCK_BUY、外盤/大單追蹤） | ✅ |
| 5 | 自動停損下單（EXIT_D 市價送出 + 安全閥） | ✅ |
| 6 | 通知改版（Telegram+Email）+ 資料來源管理頁 | ✅ |
| 7 | 美股市場支援（yfinance K線/快照/訊號/自選股/持倉） | ✅ |
| 8 | PRD缺失補齊：利多不漲NLP + 當沖比午盤防洗 + 融券軋空聯動 | ✅ |

---

## 訊號引擎（12 種訊號）

### 買進訊號（6 種）

| 訊號 | 觸發條件 |
|------|----------|
| **BUY_A** | 假跌破5MA → 15-30分拉回 + 大單/連續外盤（tick-level）；fallback: MACD金叉+站上MA20+量比≥1.5x |
| **BUY_B** | 量比>2.5x + 連續外盤≥5 + 特大單≥1（tick-level）；fallback: MA5上穿MA10+量比≥1.2x |
| **LOW_BUY** | 低於 MA240 年線 −15%，超跌左側低吸 |
| **LOCK_BUY** | 正乖離率>15%（MA5），強勢鎖定買進 |
| **SQUEEZE_BREAK** | 突破近20日最高點 + 量比≥2x |
| **SQUEEZE_BUY** | 券資比>30% + 盤中突破前日高 = 融券軋空強力買訊 |

### 賣出訊號（6 種）

| 訊號 | 觸發條件 |
|------|----------|
| **EXIT_A** | 跌破 VWAP 均價線，3 分鐘內無法站回 |
| **EXIT_B** | 高檔爆量出貨（內盤特大單砸盤）；fallback: MACD死叉+量縮<0.8x |
| **EXIT_C** | 移動止盈：波段利潤達8%後回落2% / 當沖利潤達3%後回落1% |
| **EXIT_D** | 絕對停損安全閥：從成本跌幅≥threshold%（預設5%），不可關閉 |
| **NEWS_BEARISH** | 利多不漲：正面新聞但收跌>1% + 量比>1.5x → 利多出盡 |
| **DAYTRADE_WARN** | 當沖比>70% + 12:30後跌破VWAP → 當沖客倒貨賣壓 |

### 風控守門

- ALERT 風控等級或 MACRO_LOCK 啟動時，所有 BUY 訊號被 `_block_buy` 阻斷
- 僅允許 EXIT 訊號執行

---

## 前端頁面（7 頁 + TW/US 雙市場）

| 頁面 | 功能 |
|------|------|
| 總覽 | TW/US 切換、總經指標 chips、今日訊號、自選股表格、持倉快覽、美股大盤指數 |
| K 線 | 多時框(5分/60分/日K) + MA/MACD/Volume + 五檔委託 + VWAP + tick更新 + 停損滑桿 |
| 持倉 | CRUD + 停損調整 + 最高價追蹤 + 移動止盈規則 |
| 盤後分析 | SQUEEZE候選 + 法人目標價 + 融券軋空 + 投信鎖碼 + 籌碼棄守 + **利多不漲** + **當沖比警示** + **融券突破** + 訊號歷史 |
| 總經 | VIX/DXY/US10Y/ES 儀表 + MACRO_LOCK + 風控等級 |
| 設定 | 停損% + EXIT_D + 通知設定(Telegram/Email/Webhook) + 自動停損開關 |
| 資料源 | 12 種資料來源狀態總覽 |

---

## 資料庫（8 張表）

| 表 | 用途 |
|----|------|
| `kbar_cache` | K 線快取 |
| `positions` | 持倉（trade_type, stop_loss, target_price, highest_price, market, status） |
| `signal_log` | 訊號記錄 |
| `risk_config` | 風控設定 key-value |
| `watchlist` | 自選股（market: TW/US） |
| `chip_snapshot` | 每日籌碼快照（法人/融資融券） |
| `daytrade_snapshot` | 當沖比快照 |
| `news_cache` | 新聞/重大訊息快取 |

---

## API 端點總數

- 台股核心：18 端點
- 籌碼模組：8 端點
- Phase 8 新增：7 端點（當沖比3 + 新聞3 + 融券突破1）
- 美股模組：9 端點
- **合計：42+ 端點**

---

## 安全規範

1. API Key 不硬編碼（env → .env → config.yaml）
2. 預設模擬盤（SJ_PRODUCTION=true 才切正式）
3. EXIT_D 保命機制不可關閉
4. Webhook SSRF 防護（僅 https://）
5. 自動下單預設關閉，啟用需二次確認
6. ALERT 風控自動阻斷所有買進

---

## 未來增強

| 項目 | 說明 |
|------|------|
| G7 NLP 深度模型 | 接 LLM/BERT 做全文語意分析（目前用關鍵字匹配） |
| EXIT_C 自動下單 | 目前僅 EXIT_D 自動，EXIT_C 仍為警示 |
| 回測系統 | 策略歷史驗證（規劃中） |
| 策略管理頁 | 獨立展示所有策略邏輯與參數 |

---

*本文件由 Claude Code 自動生成，2026-06-10。*
