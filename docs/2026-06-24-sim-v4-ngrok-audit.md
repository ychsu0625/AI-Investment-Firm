# SIM v4 — 全站 ngrok 體檢誠實缺陷清單

**日期**：2026-06-24
**方法**：依鐵律第 14 條，從老闆真實入口 `https://hanky-doorway-constable.ngrok-free.dev` 用 Chrome 逐頁實際操作（非本機、非只測 API）。每頁抓畫面 + console error + 網路請求。
**結論**：8 區裡 **7 區正常、1 區（K線）真的壞**。另有 2 個次要問題。之前「Step 3 全完成」漏抓這些，因為只做了外觀 + 本機 + API，沒從 ngrok 觸發即時資料。

---

## ❌ P1 — K線頁整頁壞（老闆截圖那頁，根因已鎖定）

### BUG-1：策略標記路由被貪婪路由遮蔽 → K線圖掛掉
- **現象**：K線圖卡「載入中…」→「後端未啟動，請執行 backend.py」；開/高/低/量/VWAP 全 `--`。
- **真根因（非後端沒開，是誤導訊息）**：
  - `GET /api/kbars/2330?tf=D` → **200**（K線資料其實有抓到）
  - `GET /api/kbars/2330/strategy-markers?...` → **404** 回 `{"error":"no bars"}`
  - console：`TypeError: stratMarkers is not iterable at reloadChartMarkers (:3930) at async loadKbars (:3702)`
  - 元兇：`backend.py:898` `@app.get("/api/kbars/{code:path}")` 的 **`{code:path}` 貪婪吃斜線**，把 `2330/strategy-markers` 整串當 code，先匹配到、回 404。導致定義在後面的三條子路由全失效：
    - `/api/kbars/{code}/strategy-markers`（:1100）
    - `/api/kbars/{code}/indicators`（:1129）
    - `/api/kbars/{code}/indicators/history`（:12290）
  - 404 非陣列 → 前端 `[...stratMarkers]` 炸 → 整個 loadKbars async 拋錯 → catch 顯示「後端未啟動」。
- **修法（後端，二擇一）**：
  1. `backend.py:898` `{code:path}` → `{code}`（股票代號不含斜線，最小改動）；或
  2. 把三條具體子路由註冊到 `{code:path}` 之前。
- **前端防呆（建議併做，index.html:3918）**：`if(!Array.isArray(stratMarkers)) stratMarkers=[];` 讓單一子請求失敗不炸整張圖。

### BUG-2：五檔委託「訂閱失敗」（WebSocket 寫死位址）
- **現象**：五檔委託顯示「訂閱失敗」、外盤連/大單 0。
- **根因**：`index.html:3737` 與 `:4739` 寫死 `new WebSocket('ws://localhost:8765/ws/tick/'+code)`：
  1. 埠錯（8765=舊 v3，現行 v4=8766）
  2. 寫死 `localhost` → ngrok 下指向 viewer 自己的電腦
  3. `ws://` 對 https 頁面被瀏覽器擋（mixed content）
- **修法（前端兩處）**：
  ```js
  const wsProto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const ws = new WebSocket(`${wsProto}//${location.host}/ws/tick/${code}`);
  ```
- **但書**：修完盤後仍只會顯示「等待行情…」（Shioaji 盤後不推 tick），盤中才有跳動數字——但不會再「訂閱失敗」。

---

## ⚠️ P2 — 持倉「集中度」bar 無填色
- **現象**：持倉頁部位集中度表，「集中度」欄所有 bar 都是同寬灰條，不依佔比、無超標配色（8299 31.81% 超標 與 3042 2.80% bar 一樣長）。
- **影響**：純視覺。佔比 % 與狀態文字（⚠️超過20%上限／✓正常）都正確。
- **與 backlog 落差**：backlog 記「G-07 集中度 bar 比例+配色正確」，但 ngrok 實看是空條 → 待查渲染（fill 元素寬度/顏色沒套上）。

## ⚠️ P3 — 設定頁公開暴露 PII
- **現象**：ngrok 公開下，設定頁**不需登入**即顯示 email 帳號（`setitallfree0625@gmail.com`）與 Telegram 訂閱者 Chat ID（Token 已遮罩 `***`）。
- **風險**：app 對外公開＝任何拿到網址者可開。建議把設定頁這類 PII 顯示加 token gate，或遮罩。

---

## ✅ 正常（ngrok 實走確認）
| 區 | 狀態 |
|---|---|
| 駕駛艙/總覽 | ✅ regime 雙 TREND_UP、今日訊號卡真資料、持倉快覽損益%正確（華邦電+73.68%）、macro strip 全有值、無 console error |
| 盤後 | ✅ 法人目標價表現價正確、籌碼擠壓 graceful、input 寬度正常 |
| 資訊中心（總覽） | ✅ 總經快覽 8 卡全有值、今日推薦標的卡技術+基本面豐富、regime banner |
| 決策引擎/專家 | ✅ 策略專家委員會、分析紀錄 SNDK 完成卡 |
| 驗證台/回測 | ✅ 設定表單完整、16買進+12賣出訊號、歷史紀錄 CAGR/勝率/MDD |
| 投組/持倉 | ✅ KPI 4卡、21檔表、未實現+38.8%（除集中度 bar 見 P2） |
| 系統/設定 | ✅ 全 input 深色（Z6 白底修法成立，ngrok 下確認）（除 PII 見 P3） |

---

## 給 tool team / cockpit 的修復序
1. **BUG-1**（後端 1 行 `:path`→`{code}` + 前端防呆）— 解 K線整頁
2. **BUG-2**（前端 2 行 WS 位址）— 解五檔
3. P2 集中度 bar（前端 fill 渲染）
4. P3 設定頁 PII gate（安全）

> 修完一律**回 ngrok 複驗**才算完成（鐵律第 14 條）。
</content>
