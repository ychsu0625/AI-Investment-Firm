# Smart Investment Monitor — 測試計畫 (Test Plan)

**版本**: v2.0  
**建立日期**: 2026-06-11  
**涵蓋範圍**: backend.py + index.html + info_center.html + github_agent.py  
**目的**: 預防回歸問題，累積可重複執行的測試案例

---

## 測試層次

| 層次 | 方式 | 執行時機 |
|------|------|---------|
| T1 — API 煙霧測試 | curl / fetch | 每次後端啟動後 |
| T2 — 訊號引擎邏輯 | Python 腳本 | 每次修改 backend.py |
| T3 — 前端功能 | 手動瀏覽器 | 每次修改 HTML/JS |
| T4 — 安全測試 | curl / 手動 | 每次修改 auth/SQL |
| T5 — 並發壓力 | Python threading | 修改鎖定/DB 區段後 |

---

## T1 — API 煙霧測試 (Smoke Tests)

### 取得 Token（所有後續測試的前提）
```bash
TOKEN=$(curl -s http://localhost:8765/api/auth/token | python -c "import sys,json; print(json.load(sys.stdin)['token'])")
```

### T1-01: 系統資訊
```bash
# 預期: {"simulation": true, "version": "2.0", ...}
curl -s http://localhost:8765/api/info | python -m json.tool
```

### T1-02: 總經數據
```bash
# 預期: {vix, dxy, us10y, es_futures_chg, risk_level, position_scale}
curl -s -H "X-API-Token: $TOKEN" http://localhost:8765/api/macro | python -m json.tool
```

### T1-03: 風控等級
```bash
# 預期: {risk_level: "NORMAL"|"CAUTION"|"ALERT", position_scale: float}
curl -s http://localhost:8765/api/risk-level | python -m json.tool
```

### T1-04: 自選股清單
```bash
# 預期: 陣列 [{code, name, market, ...}]
curl -s -H "X-API-Token: $TOKEN" http://localhost:8765/api/watchlist | python -m json.tool
```

### T1-05: 新增自選股（TW）
```bash
# 預期: {"ok": true}
curl -s -X POST -H "X-API-Token: $TOKEN" http://localhost:8765/api/watchlist/add/2330
```

### T1-06: 刪除自選股（TW）
```bash
# 預期: {"ok": true}
curl -s -X DELETE -H "X-API-Token: $TOKEN" http://localhost:8765/api/watchlist/remove/2330
```

### T1-07: K線資料（台股）
```bash
# 預期: {kbars: [...], mas: {...}, macd: {...}}
curl -s "http://localhost:8765/api/kbars/2330?tf=D" | python -c "import sys,json; d=json.load(sys.stdin); print('kbars:', len(d.get('kbars',[])), 'MA keys:', list(d.get('mas',{}).keys()))"
```

### T1-08: K線資料（美股）
```bash
# 預期: 與台股相同結構
curl -s "http://localhost:8765/api/us/kbars/AAPL?tf=D" | python -c "import sys,json; d=json.load(sys.stdin); print('kbars:', len(d.get('kbars',[])))"
```

### T1-09: 美股指數
```bash
# 預期: [{symbol, name, price, change_pct}, ...]
curl -s http://localhost:8765/api/us/indices | python -m json.tool
```

### T1-10: 訊號記錄
```bash
# 預期: [{code, signal_type, direction, price, detail, created_at}, ...]
curl -s http://localhost:8765/api/signals | python -m json.tool
```

### T1-11: 持倉清單
```bash
# 預期: [{id, code, name, cost, shares, trade_type, ...}]
curl -s -H "X-API-Token: $TOKEN" http://localhost:8765/api/positions | python -m json.tool
```

### T1-12: 盤後掃描
```bash
# 預期: {results: [{code, name, close, ma5_pos, macd_dir, ...}], count: N}
curl -s -H "X-API-Token: $TOKEN" http://localhost:8765/api/scan/after-hours | python -c "import sys,json; d=json.load(sys.stdin); print('count:', d.get('count'))"
```

### T1-13: IC 推薦清單
```bash
# 預期: [{code, name, market, direction, score, confidence, ...}]
curl -s http://localhost:8765/api/ic/recommendations | python -m json.tool
```

### T1-14: IC 宏觀數據
```bash
# 預期: {vix, dxy, us10y, ...}
curl -s http://localhost:8765/api/ic/macro | python -m json.tool
```

### T1-15: GitHub 狀態
```bash
# 預期: {current_version, latest_tag, modified_files, ...}
curl -s -H "X-API-Token: $TOKEN" http://localhost:8765/api/github/status | python -m json.tool
```

---

## T2 — 訊號引擎邏輯測試

### T2-01: BUY_A 假跌破觸發
```python
# 條件: 現價剛回站 MA5 + 有大單 + 15-30 分鐘內
# 驗證: signal_log 有新增 BUY_A 紀錄
import sqlite3, json
con = sqlite3.connect("monitor.db")
cur = con.cursor()
cur.execute("SELECT signal_type, direction FROM signal_log WHERE signal_type='BUY_A' ORDER BY id DESC LIMIT 1")
row = cur.fetchone()
print("BUY_A:", row)
con.close()
```

### T2-02: EXIT_D 停損掃描
```bash
# 手動觸發 EXIT_D 掃描
curl -s http://localhost:8765/api/scan/exitd | python -m json.tool
```

### T2-03: MACRO_LOCK 在 ALERT 時阻斷買進
```bash
# Step 1: 設定 MACRO_LOCK ON
curl -s -X POST -H "X-API-Token: $TOKEN" http://localhost:8765/api/macro-lock/on
# Step 2: 執行訊號掃描
curl -s http://localhost:8765/api/scan/signals
# Step 3: 確認結果不含 BUY 訊號
# Step 4: 關回 MACRO_LOCK
curl -s -X POST -H "X-API-Token: $TOKEN" http://localhost:8765/api/macro-lock/off
```

### T2-04: 策略停用後不觸發
```bash
# Step 1: 停用 BUY_A
curl -s -X PUT -H "X-API-Token: $TOKEN" http://localhost:8765/api/strategies/BUY_A/toggle
# Step 2: 確認 enabled=false
curl -s http://localhost:8765/api/strategies | python -c "import sys,json; [print(s['id'],s['enabled']) for s in json.load(sys.stdin)]"
# Step 3: 執行掃描，確認無 BUY_A
curl -s http://localhost:8765/api/scan/signals
# Step 4: 重新啟用
curl -s -X PUT -H "X-API-Token: $TOKEN" http://localhost:8765/api/strategies/BUY_A/toggle
```

### T2-05: 策略參數更新生效
```bash
# Step 1: 更新 BUY_A breach_min 為 20
curl -s -X PUT -H "X-API-Token: $TOKEN" -H "Content-Type: application/json" \
  -d '{"breach_min": 20}' \
  http://localhost:8765/api/strategies/BUY_A/params
# Step 2: 確認回傳 params.breach_min = 20
```

---

## T3 — 前端功能測試（手動）

### T3-01: 頁面導覽
| 步驟 | 操作 | 預期 |
|------|------|------|
| 1 | 開啟 http://localhost:8765 | 首頁載入，總覽頁顯示 |
| 2 | 點擊 K線 | K線頁顯示，圖表載入 |
| 3 | 點擊 持倉 | 持倉頁顯示，持倉表格顯示 |
| 4 | 點擊 盤後 | 盤後分析頁顯示 |
| 5 | 點擊 總經 | 總經頁顯示 VIX/DXY/US10Y |
| 6 | 點擊 設定 | 設定頁顯示 |
| 7 | 點擊 資料源 | 資料來源頁顯示 12 個卡片 |
| 8 | 點擊 回測 | 回測頁顯示 |
| 9 | 點擊 資訊中心 | IC 頁面顯示 |

### T3-02: TW/US 市場切換
| 步驟 | 操作 | 預期 |
|------|------|------|
| 1 | 點擊 Header 的 US 按鈕 | 首頁切換為美股模式 |
| 2 | 確認自選股表格顯示美股代碼格式 | 代碼全大寫字母 |
| 3 | 點擊 TW 按鈕 | 切回台股模式 |

### T3-03: 新增/刪除自選股
| 步驟 | 操作 | 預期 |
|------|------|------|
| 1 | 在搜尋框輸入 2330 | 下拉出現台積電 |
| 2 | 點擊新增 | 自選股表格出現 2330 |
| 3 | 點擊刪除 | 2330 從表格移除 |

### T3-04: K線頁功能
| 步驟 | 操作 | 預期 |
|------|------|------|
| 1 | 點一支自選股進入K線頁 | 日K圖表顯示，MA5/10/20/60/240 |
| 2 | 切換 60分 | 60分 K線圖顯示 |
| 3 | 切換 5分 | 5分 K線圖顯示 |
| 4 | 拖動停損滑桿 | 滑桿移動，數值更新 |
| 5 | 開啟 MACD 核選框 | MACD 圖表顯示在下方 |

### T3-05: 持倉 P&L 計算驗證
| 步驟 | 操作 | 預期 |
|------|------|------|
| 1 | 新增 TW 持倉：2330，成本100，1張 | 持倉出現 |
| 2 | 確認市值和損益欄位 | 市值 = 現價 × 1000（NT$單位）|
| 3 | 新增 US 持倉：AAPL，成本200，10股 | 持倉出現 |
| 4 | 確認市值和損益欄位 | 市值 = 現價 × 10（US$單位）|

### T3-06: 資訊中心 (IC) 功能
| 步驟 | 操作 | 預期 |
|------|------|------|
| 1 | 進入資訊中心 | 概覽頁顯示 |
| 2 | 點擊側欄「總經分析」| 總經頁顯示 VIX/DXY |
| 3 | 點擊「AI 建議」| 推薦頁顯示（可能空白等待） |
| 4 | 點擊「重新分析」| 觸發掃描，結果顯示 |
| 5 | 進入 IC 設定 | 通知開關與門檻滑桿顯示 |
| 6 | 切換通知開關 | 保存成功 |

### T3-07: 盤後分析頁「籌碼/新聞資料管理」置頂
| 步驟 | 操作 | 預期 |
|------|------|------|
| 1 | 進入盤後分析頁 | 資料管理卡片在最上方可見 |
| 2 | 點擊「抓取今日籌碼」| 觸發抓取，顯示 loading |

---

## T4 — 安全測試

### T4-01: 無 Token 拒絕寫入
```bash
# 所有 POST/PUT/DELETE 在無 token 時應回 401/403
# 預期: 401 或 403
curl -s -o /dev/null -w "%{http_code}" -X POST \
  -H "Content-Type: application/json" \
  -d '{"code": "2330"}' \
  http://localhost:8765/api/positions
# 預期 code: 401 或 403

curl -s -o /dev/null -w "%{http_code}" -X POST \
  http://localhost:8765/api/us/positions \
  -H "Content-Type: application/json" \
  -d '{"code": "AAPL", "cost": 1}'
# 預期: 401 (Fixed C2)

curl -s -o /dev/null -w "%{http_code}" -X POST \
  http://localhost:8765/api/signals \
  -H "Content-Type: application/json" \
  -d '{"code": "2330", "signal_type": "BUY_A", "direction": "BUY", "price": 100}'
# 預期: 401 (Fixed C2)
```

### T4-02: 錯誤 Token 拒絕
```bash
curl -s -o /dev/null -w "%{http_code}" -X POST \
  -H "X-API-Token: invalid-token" \
  -H "Content-Type: application/json" \
  -d '{"code": "2330"}' \
  http://localhost:8765/api/positions
# 預期: 401 或 403
```

### T4-03: SQL Injection 防護
```bash
# market 參數注入嘗試
curl -s "http://localhost:8765/api/watchlist?market=TW%27%20OR%201%3D1--" | python -m json.tool
# 預期: 正常回應（空清單）或 400，不應 500

# 自選股代碼注入
curl -s -X POST -H "X-API-Token: $TOKEN" \
  "http://localhost:8765/api/watchlist/add/2330%27%3BDROP%20TABLE%20watchlist--"
# 預期: 400 或 正常拒絕，watchlist 表仍存在
```

### T4-04: IC Settings API Key 不外洩
```bash
# 預期: claude_api_key = "***" 或 ""，不顯示實際金鑰 (Fixed L3)
curl -s http://localhost:8765/api/ic/settings | python -c "import sys,json; d=json.load(sys.stdin); print('api_key:', d.get('claude_api_key'))"
```

### T4-05: XSS 防護（手動）
```python
# 插入含 XSS payload 的 signal
import sqlite3
con = sqlite3.connect("monitor.db")
con.execute("INSERT INTO signal_log(code,signal_type,direction,price,detail) VALUES(?,?,?,?,?)",
    ("2330", "BUY_A", "BUY", 100.0, "<img src=x onerror=alert('XSS')>"))
con.commit(); con.close()
# 然後在瀏覽器首頁查看訊號卡片
# 預期: 顯示文字 <img src=x onerror=alert('XSS')>，不執行 JS
```

---

## T5 — 並發壓力測試

### T5-01: 並發訊號掃描（測試鎖定）
```python
import threading, requests

TOKEN = "YOUR_TOKEN"
headers = {"X-API-Token": TOKEN}
results = []

def scan():
    r = requests.get("http://localhost:8765/api/scan/signals")
    results.append(r.status_code)

threads = [threading.Thread(target=scan) for _ in range(10)]
for t in threads: t.start()
for t in threads: t.join()

print("All status codes:", results)
# 預期: 全部 200，無 500
```

### T5-02: 並發策略切換 + 掃描
```python
import threading, requests

TOKEN = "YOUR_TOKEN"
headers = {"X-API-Token": TOKEN}

def toggle():
    requests.put("http://localhost:8765/api/strategies/BUY_A/toggle", headers=headers)

def scan():
    requests.get("http://localhost:8765/api/scan/signals")

threads = [threading.Thread(target=toggle if i%2==0 else scan) for i in range(10)]
for t in threads: t.start()
for t in threads: t.join()
# 預期: 無例外，策略狀態一致
```

### T5-03: 並發推薦刷新（測試 _ic_refresh_lock）
```python
import threading, requests

TOKEN = "YOUR_TOKEN"
headers = {"X-API-Token": TOKEN, "Content-Type": "application/json"}
results = []

def refresh():
    r = requests.post("http://localhost:8765/api/ic/recommendations/refresh",
                      json={"use_ai": False}, headers=headers)
    results.append(r.status_code)

threads = [threading.Thread(target=refresh) for _ in range(5)]
for t in threads: t.start()
for t in threads: t.join()

print("Status codes:", results)
# 預期: 全部 200，DB 無重複資料
```

---

## T6 — 回歸測試清單（每次版本更新執行）

| 測試ID | 測試項目 | 重要性 | 上次失敗 |
|--------|---------|--------|---------|
| R-01 | 系統啟動無錯誤 | 🔴 Critical | — |
| R-02 | Token Auth 保護所有寫入端點 | 🔴 Critical | — |
| R-03 | SQL Injection 防護 (market 參數) | 🔴 Critical | — |
| R-04 | EXIT_D 掃描不崩潰 | 🔴 Critical | 2026-06-11 (foreign_buy_sell) |
| R-05 | TW K線 (2330, D 時框) | 🟠 High | — |
| R-06 | US K線 (AAPL, D 時框) | 🟠 High | — |
| R-07 | 總經數據回傳含 risk_level | 🟠 High | — |
| R-08 | 訊號掃描不崩潰 | 🟠 High | — |
| R-09 | IC 推薦刷新 (無 AI) | 🟠 High | — |
| R-10 | 持倉 P&L 計算正確（TW x1000） | 🟠 High | 2026-06-11 (M5) |
| R-11 | IC Settings API Key 被遮罩 | 🟠 High | 2026-06-11 (L3) |
| R-12 | 策略停用後不觸發訊號 | 🟡 Medium | 2026-06-11 (H5) |
| R-13 | GitHub Push Mode 成功提交 | 🟡 Medium | — |
| R-14 | GitHub Watch Check 不崩潰 | 🟡 Medium | — |
| R-15 | IC 買進/賣出顏色一致 (台股慣例) | 🟡 Medium | 2026-06-11 (UI) |
| R-16 | 無 Token 操作美股持倉回 401 | 🔴 Critical | 2026-06-11 (C2) |
| R-17 | 無 Token 寫入 signals 回 401 | 🔴 Critical | 2026-06-11 (C2) |
| R-18 | 盤後掃描不崩潰 | 🟠 High | — |
| R-19 | MACRO_LOCK 阻斷買進 | 🔴 Critical | — |
| R-20 | WAL 模式啟用 (monitor.db) | 🟠 High | 2026-06-11 (C4) |

---

## 已知 Bug 追蹤

| Bug ID | 描述 | 狀態 | 修復日期 |
|--------|------|------|---------|
| BUG-01 | `cs.foreign_buy_sell` 欄位不存在 → ic_tw_institutional_top 500 | 需確認 | — |
| BUG-02 | EXIT_C 僅警示，無自動下單 | 已知限制 | — |
| BUG-03 | Production 訂單 fill callback 未完整連線 | 已知限制 | — |
| BUG-04 | Strategy param 更新需重啟才生效（已部分修改） | 持續追蹤 | — |
| BUG-05 | TW 持倉 P&L 少乘 1000 | Fixed | 2026-06-11 |
| BUG-06 | XSS in signal card / position name | Fixed | 2026-06-11 |
| BUG-07 | POST /api/us/positions 無 token 驗證 | Fixed | 2026-06-11 |
| BUG-08 | NEWS_BEARISH 邏輯：前5筆無條件納入 | Fixed | 2026-06-11 |
| BUG-09 | _breach_times dict 非 thread-safe | Fixed | 2026-06-11 |
| BUG-10 | monitor.db 未啟用 WAL mode | Fixed | 2026-06-11 |

---

## 快速執行腳本

儲存以下腳本為 `run_smoke_tests.sh`（或手動執行）：

```bash
#!/bin/bash
BASE=http://localhost:8765
TOKEN=$(curl -s $BASE/api/auth/token | python -c "import sys,json; print(json.load(sys.stdin)['token'])" 2>/dev/null)

echo "=== Smart Monitor Smoke Tests ==="
echo "Token: ${TOKEN:0:16}..."

pass=0; fail=0

check() {
  local name=$1; local url=$2; local expected=$3
  local result=$(curl -s -H "X-API-Token: $TOKEN" "$url" | python -c "import sys,json; d=json.load(sys.stdin); print('ok')" 2>/dev/null)
  if [ "$result" = "ok" ]; then
    echo "✅ $name"; ((pass++))
  else
    echo "❌ $name ($url)"; ((fail++))
  fi
}

check_code() {
  local name=$1; local url=$2; local method=$3; local expected_code=$4
  local code=$(curl -s -o /dev/null -w "%{http_code}" -X $method "$url")
  if [ "$code" = "$expected_code" ]; then
    echo "✅ $name (HTTP $code)"; ((pass++))
  else
    echo "❌ $name (expected $expected_code, got $code)"; ((fail++))
  fi
}

check "T1-01 系統資訊" "$BASE/api/info"
check "T1-02 總經數據" "$BASE/api/macro"
check "T1-10 訊號記錄" "$BASE/api/signals"
check "T1-13 IC 推薦" "$BASE/api/ic/recommendations"
check_code "T4-01 無Token拒絕新增持倉" "$BASE/api/positions" "POST" "401"
check_code "T4-01 無Token拒絕美股持倉" "$BASE/api/us/positions" "POST" "401"
check_code "T4-01 無Token拒絕寫入訊號" "$BASE/api/signals" "POST" "401"

echo ""
echo "Results: $pass passed, $fail failed"
```

---

*本文件隨每次修復自動更新，Bug 修復後在 Bug 追蹤表更新狀態與日期。*
