# smart-investment-monitor — Session 接手包（handoff）

> 用法：新 session 開場貼「開場白」那段即可。本檔由 Claude Code 維護，接手後若有重大變更請更新此檔。
> 最後更新：2026-06-12

---

## 開場白（複製貼上）

```
延續 smart-investment-monitor（老婆的投資監控專案）。

先讀這份接手包：
C:\Users\ychsu\Documents\Claude_Files\smart-investment-monitor\docs\2026-06-12-smart-investment-monitor-handoff.md

再讀以下 memory：
- project_ic_knowledge_base（知識庫 RAG / hybrid 檢索 / 標籤對焦）
- project_ic_ai_dual_source（AI 雙來源：API / 訂閱 CLI）
- project_macro_riskcontrol_split（總經→風控架構）
- project_smart_investment_monitor、user_investment_profile

今天想做：<寫你要做的事>
```

只想先了解現況、別急著改：開場白最後一句改成
「先讀完上面資料跟我確認你掌握的現況，等我指示再動手。」

---

## 專案位置

- 程式碼：`C:\Users\ychsu\Documents\Claude_Files\smart-investment-monitor\ui\`
  - `backend.py`（FastAPI + SQLite，服務跑在 `localhost:8765`）
  - `index.html`（單檔 SPA，前端全部在這）
- 資料庫：`ui\monitor.db`（**不是** investment.db，那個是空的）
- 完整背景：`C:\Users\ychsu\ai-investment-system\docs\Personal AI Investment System.md`
  （看「十四、十五」是最近兩輪決策與踩坑）

---

## 接手必做檢查

1. **確認後端在跑**：開 `http://localhost:8765/`。沒服務就在 `ui\` 資料夾 `python backend.py`。
   - 重啟方式（PowerShell）：找 8765 listener PID → `Stop-Process` → 重開。
2. **改設定/DB 後務必先 F5 再按儲存**：UI 是整批覆蓋儲存，先重整拿最新值，否則舊值蓋回 DB。
3. **依賴**：本機已裝 `pypdf`、`fastembed`；換機器才需 `pip install pypdf fastembed`。

---

## 目前狀態（2026-06-12 完成）

### A. 資訊中心 AI 雙來源
每個 AI 功能可各自選來源：`api`（Anthropic API Key 計費）或 `subscription`（本機 `claude` CLI 吃訂閱、零 API 費用）。統一入口 `_ic_llm_call`。訂閱模式靠**後端那台機器**裝好並登入 Claude Code。

### B. 知識庫 RAG（Phase A）
本地 RAG，刻意不做知識圖譜（省 token）。表 `ic_kb_chunks`+`ic_kb_fts`(FTS5 trigram)。embedding 用 fastembed（CPU、延遲載入、裝不起來退回純 FTS5）。**hybrid 檢索 = 稀疏 + 稠密 + RRF**。手動餵料：純文字 / PDF 上傳 / 網址，入庫即切塊+向量化。分析師輸出逐點標 `[#n]` + 底部「本次知識庫參考」。

### C. 標籤對焦（Phase B）
來源掛標籤(ticker/產業)；分析某股時自動加權命中該標籤的來源（窮人版圖譜，零 token）。前端可按類型 + 標籤篩選，對焦命中標 🎯。

### 其他近期改動
- 總經→風控（risk control）重構：控制類放風控頁、純數據放資訊中心。
- K 線：MA5/10/20/60 toggle、持倉進場點、策略快選面板。
- Telegram/Email 多人訂閱（含老婆 Yasmine）。
- 損益單位：台股 `shares` 是實際股數（非張），`lotMul=1`。

---

## 未做（依使用者指示保留）
- 知識庫 Phase C：跨文件關係 full KG。真需要供應鏈推理才做。

---

## 規則提醒（來自全域設定）
- **任何 GitHub push 前一定要先取得使用者同意**；有 `docs/UPDATES.md` 就追加紀錄。
- Session 結束前提醒使用者把新決策/踩坑追加到 `Personal AI Investment System.md`。
- monitor.db 的 `risk_config` 內有真實憑證（telegram token、email app password），勿外洩。
