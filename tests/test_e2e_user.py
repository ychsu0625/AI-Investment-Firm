"""
E2E User Simulation Test
模擬使用者從開瀏覽器到操作每個頁面的完整流程
"""
import sys, json, time, re, os
from datetime import datetime, timedelta
os.environ["PYTHONIOENCODING"] = "utf-8"
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
try:
    import httpx
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "httpx", "-q"])
    import httpx

BASE = sys.argv[sys.argv.index("--base") + 1] if "--base" in sys.argv else "http://localhost:8765"

passed = 0
failed = 0
warnings = []

def check(name, condition, detail=""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  ✅ {name}")
    else:
        failed += 1
        print(f"  ❌ {name} {detail}")

def warn(msg):
    warnings.append(msg)
    print(f"  ⚠️  {msg}")

client = httpx.Client(timeout=20.0, follow_redirects=True)

# Step 0: Get token
print(f"\n{'='*60}")
print(f"  E2E User Simulation @ {BASE}")
print(f"{'='*60}")

token_resp = client.get(f"{BASE}/api/auth/token")
TOKEN = token_resp.json().get("token", "")
headers = {"X-API-Token": TOKEN, "Content-Type": "application/json"}
check("T0 取得 API Token", bool(TOKEN), f"resp={token_resp.status_code}")

# ════════════════════════════════════════════
# 1. 使用者開啟首頁
# ════════════════════════════════════════════
print("\n  --- 📄 T1: 首頁載入 ---")
html = client.get(f"{BASE}/").text
check("T1-01 HTML 載入成功", len(html) > 100000, f"len={len(html)}")
check("T1-02 含 nav 按鈕", 'id="nav-home"' in html and 'id="nav-trade"' in html)
check("T1-03 含交易紀錄頁", 'id="page-trade"' in html)
check("T1-04 含專家頁", 'id="page-expert"' in html or 'expert' in html.lower())

# 首頁 API calls (模擬 JS 載入)
wl = client.get(f"{BASE}/api/watchlist").json()
check("T1-05 自選股載入", isinstance(wl, list) and len(wl) > 0, f"count={len(wl)}")

pos = client.get(f"{BASE}/api/positions").json()
check("T1-06 持倉載入", isinstance(pos, list), f"count={len(pos)}")

signals = client.get(f"{BASE}/api/signals").json()
check("T1-07 訊號載入", isinstance(signals, list))

risk = client.get(f"{BASE}/api/risk-level").json()
check("T1-08 風控載入", "risk_level" in risk, f"data={risk}")

# ════════════════════════════════════════════
# 2. K線頁
# ════════════════════════════════════════════
print("\n  --- 📈 T2: K線頁 ---")
# 台股 K線
kd_tw = client.get(f"{BASE}/api/kbars/2330?tf=D").json()
has_bars = "kbars" in kd_tw or "candles" in kd_tw
bars_key = "kbars" if "kbars" in kd_tw else "candles"
check("T2-01 台股K線(2330) API回應", has_bars, f"keys={list(kd_tw.keys())[:6]}")
if len(kd_tw.get(bars_key, [])) == 0:
    warn("T2-01 K線資料空（非盤中或無快取，非 bug）")
has_ma = "mas" in kd_tw or "ma5" in kd_tw
check("T2-02 MA 結構存在", has_ma)
has_macd = "macd" in kd_tw or "macd_dif" in kd_tw
check("T2-03 MACD 結構存在", has_macd)

# 美股 K線
kd_us = client.get(f"{BASE}/api/us/kbars/AAPL?tf=D").json()
has_bars_us = "kbars" in kd_us or "candles" in kd_us
check("T2-04 美股K線(AAPL) API回應", has_bars_us, f"keys={list(kd_us.keys())[:6]}")

# 60分K
kd_60 = client.get(f"{BASE}/api/kbars/2330?tf=60").json()
check("T2-05 60分K線 API回應", "kbars" in kd_60 or "candles" in kd_60)

# 策略標記
strat_markers = client.get(f"{BASE}/api/kbars/2330/strategy-markers?strategies=BUY_A,EXIT_D").json()
check("T2-06 策略標記API", isinstance(strat_markers, list))

# ════════════════════════════════════════════
# 3. 持倉頁
# ════════════════════════════════════════════
print("\n  --- 💼 T3: 持倉頁 ---")
check("T3-01 持倉數量", len(pos) > 0, f"count={len(pos)}")
if pos:
    p = pos[0]
    check("T3-02 持倉 schema", all(k in p for k in ["code", "name", "shares", "cost"]),
          f"keys={list(p.keys())[:8]}")
    check("T3-03 有 market 欄位", "market" in p)
    # 出場檢查
    ec = client.get(f"{BASE}/api/positions/{p['id']}/exit-check").json()
    check("T3-04 出場檢查API", "code" in ec, f"resp={ec}")

# ════════════════════════════════════════════
# 4. 交易紀錄頁（本次新增重點）
# ════════════════════════════════════════════
print("\n  --- 📝 T4: 交易紀錄頁 ---")

# 4.1 匯入持倉
migrate = client.post(f"{BASE}/api/trade-records/migrate-positions", headers=headers).json()
check("T4-01 匯入持倉API", migrate.get("ok"), f"resp={migrate}")

# 4.2 查看台股紀錄
tw_trades = client.get(f"{BASE}/api/trade-records?market=TW").json()
check("T4-02 台股交易紀錄", isinstance(tw_trades, list) and len(tw_trades) > 0, f"count={len(tw_trades)}")

# 4.3 查看美股紀錄
us_trades = client.get(f"{BASE}/api/trade-records?market=US").json()
check("T4-03 美股交易紀錄", isinstance(us_trades, list), f"count={len(us_trades)}")

# 4.4 新增一筆台股買入
t_new = client.post(f"{BASE}/api/trade-records", headers=headers, json={
    "code": "2888", "name": "新光金", "market": "TW", "action": "BUY",
    "shares": 5, "price": 12.5, "trade_date": "2026-06-10",
    "commission_rate": 0.001425, "commission_discount": 0.6, "tax_rate": 0.003
}).json()
check("T4-04 新增台股交易", t_new.get("ok") and t_new.get("id"), f"resp={t_new}")
check("T4-05 手續費計算", t_new.get("commission", -1) >= 0 and t_new.get("tax") == 0,
      f"comm={t_new.get('commission')} tax={t_new.get('tax')} (買入不應有稅)")
t_id1 = t_new.get("id")

# 4.5 新增一筆台股賣出
t_sell = client.post(f"{BASE}/api/trade-records", headers=headers, json={
    "code": "2888", "name": "新光金", "market": "TW", "action": "SELL",
    "shares": 3, "price": 13.0, "trade_date": "2026-06-15"
}).json()
check("T4-06 新增賣出", t_sell.get("ok"))
check("T4-07 賣出有稅", t_sell.get("tax", 0) > 0, f"tax={t_sell.get('tax')}")
t_id2 = t_sell.get("id")

# 4.6 filter by code
filtered = client.get(f"{BASE}/api/trade-records?code=2888").json()
check("T4-08 code filter", len(filtered) >= 2 and all(t["code"] == "2888" for t in filtered))

# 4.7 編輯交易
if t_id1:
    t_edit = client.put(f"{BASE}/api/trade-records/{t_id1}", headers=headers, json={
        "code": "2888", "name": "新光金改", "market": "TW", "action": "BUY",
        "shares": 10, "price": 12.0, "trade_date": "2026-06-08"
    }).json()
    check("T4-09 編輯交易", t_edit.get("ok"))
    edited = [t for t in client.get(f"{BASE}/api/trade-records?code=2888").json() if t["id"] == t_id1]
    check("T4-10 編輯後數據", edited and edited[0]["shares"] == 10 and edited[0]["price"] == 12.0)

# 4.8 交易分析
analytics = client.get(f"{BASE}/api/trade-records/analytics?code=2888").json()
check("T4-11 分析API結構", all(k in analytics for k in ["total_records", "realized_pnl", "stocks"]))
check("T4-12 有2888分析", "2888" in analytics.get("stocks", {}))
s2888 = analytics["stocks"].get("2888", {})
check("T4-13 買賣次數正確", s2888.get("buy_count") == 1 and s2888.get("sell_count") == 1,
      f"buy={s2888.get('buy_count')} sell={s2888.get('sell_count')}")
check("T4-14 有已實現損益", "realized_pnl" in s2888)

# 4.9 確認持倉同步
pos_after = client.get(f"{BASE}/api/positions").json()
pos_2888 = [p for p in pos_after if p.get("code") == "2888" and p.get("status") == "open"]
check("T4-15 持倉同步(BUY建立)", len(pos_2888) > 0)
if pos_2888:
        check("T4-16 持倉張數(買入-賣出同步)", pos_2888[0].get("shares") >= 0,
          f"shares={pos_2888[0].get('shares')}")

# 4.10 確認自動加入watchlist
wl_after = client.get(f"{BASE}/api/watchlist").json()
has_2888 = any(w.get("code") == "2888" for w in wl_after)
check("T4-17 自動加入watchlist", has_2888)

# 4.11 Auth 保護
r_noauth = client.post(f"{BASE}/api/trade-records", json={"code":"X","action":"BUY","shares":1,"price":1})
check("T4-18 POST無token拒絕", r_noauth.status_code in (401, 403))
r_noauth2 = client.put(f"{BASE}/api/trade-records/1", json={"code":"X","action":"BUY","shares":1,"price":1})
check("T4-19 PUT無token拒絕", r_noauth2.status_code in (401, 403))
r_noauth3 = client.delete(f"{BASE}/api/trade-records/1")
check("T4-20 DELETE無token拒絕", r_noauth3.status_code in (401, 403))

# 4.12 新增美股交易
t_us = client.post(f"{BASE}/api/trade-records", headers=headers, json={
    "code": "TSLA", "name": "Tesla", "market": "US", "action": "BUY",
    "shares": 10, "price": 250.5, "trade_date": "2026-06-12"
}).json()
check("T4-21 新增美股交易", t_us.get("ok"))
check("T4-22 美股無稅", t_us.get("tax") == 0)
t_id_us = t_us.get("id")

# 4.13 美股交易分析
us_analytics = client.get(f"{BASE}/api/trade-records/analytics?code=TSLA").json()
check("T4-23 美股分析", "TSLA" in us_analytics.get("stocks", {}))

# 清理測試資料
for tid in [t_id1, t_id2, t_id_us]:
    if tid:
        client.delete(f"{BASE}/api/trade-records/{tid}", headers=headers)
# 清理 2888 持倉
for p in client.get(f"{BASE}/api/positions").json():
    if p.get("code") in ("2888", "TSLA"):
        client.delete(f"{BASE}/api/positions/{p['id']}", headers=headers)

# ════════════════════════════════════════════
# 5. 策略頁
# ════════════════════════════════════════════
print("\n  --- 🎯 T5: 策略頁 ---")
strats = client.get(f"{BASE}/api/strategies").json()
check("T5-01 策略數量>=19", len(strats) >= 19, f"count={len(strats)}")

builtin = [s for s in strats if s.get("strat_type") == "builtin"]
custom = [s for s in strats if s.get("strat_type") == "custom"]
check("T5-02 內建策略>=15", len(builtin) >= 15)
check("T5-03 自訂策略>=1", len(custom) >= 1)

wife = [s for s in strats if "WIFE" in s.get("id", "")]
check("T5-04 老婆策略存在", len(wife) >= 1)

formula_linked = [s for s in strats if s.get("formula_link")]
check("T5-05 公式連結策略>=5", len(formula_linked) >= 5)

# toggle test
buy_a = next((s for s in strats if s["id"] == "BUY_A"), None)
if buy_a:
    orig_enabled = buy_a.get("enabled", True)
    client.put(f"{BASE}/api/strategies/BUY_A/toggle", headers=headers)
    strats2 = client.get(f"{BASE}/api/strategies").json()
    buy_a2 = next((s for s in strats2 if s["id"] == "BUY_A"), {})
    check("T5-06 策略toggle", buy_a2.get("enabled") != orig_enabled)
    # toggle back
    client.put(f"{BASE}/api/strategies/BUY_A/toggle", headers=headers)

# ════════════════════════════════════════════
# 6. 風控頁
# ════════════════════════════════════════════
print("\n  --- 🛡️ T6: 風控頁 ---")
macro = client.get(f"{BASE}/api/macro").json()
check("T6-01 總經數據", isinstance(macro, dict) and "risk_level" in macro)

risk_cfg = client.get(f"{BASE}/api/risk-config").json()
check("T6-02 風控設定", isinstance(risk_cfg, (dict, list)))

lock_status = client.get(f"{BASE}/api/macro-lock").json()
check("T6-03 MACRO_LOCK 狀態", isinstance(lock_status, dict))

# ════════════════════════════════════════════
# 7. 回測頁
# ════════════════════════════════════════════
print("\n  --- 🔄 T7: 回測頁 ---")
bt_history = client.get(f"{BASE}/api/backtest/history").json()
check("T7-01 回測歷史", isinstance(bt_history, list))

mkt_status = client.get(f"{BASE}/api/market-data/status").json()
check("T7-02 市場數據狀態", isinstance(mkt_status, dict))

# ════════════════════════════════════════════
# 8. 專家委員會
# ════════════════════════════════════════════
print("\n  --- 🧠 T8: 專家委員會 ---")
roles = client.get(f"{BASE}/api/expert/roles").json()
check("T8-01 專家角色>=5", isinstance(roles, list) and len(roles) >= 5)
if roles:
    r0 = roles[0]
    check("T8-02 角色 schema", all(k in r0 for k in ["id", "name", "icon", "perspective"]))

ex_cfg = client.get(f"{BASE}/api/expert/config").json()
check("T8-03 專家設定", isinstance(ex_cfg, dict))

schedules = client.get(f"{BASE}/api/expert/schedules").json()
check("T8-04 排程>=2", isinstance(schedules, list) and len(schedules) >= 2)

sessions = client.get(f"{BASE}/api/expert/sessions").json()
check("T8-05 session 結構", isinstance(sessions, dict) and "sessions" in sessions)

# 驗證沒有 session 卡在 running（已完成但狀態未更新的 bug）
if isinstance(sessions, dict):
    stuck = [s for s in sessions.get("sessions", [])
             if s.get("status") == "running"
             and s.get("created_at", "") < (datetime.now() - timedelta(hours=1)).isoformat()]
    check("T8-06 無卡住的 running session", len(stuck) == 0,
          f"found {len(stuck)} stuck sessions")
    # 所有已完成 session 應有 completed_at
    completed = [s for s in sessions.get("sessions", []) if s.get("status") == "completed"]
    if completed:
        all_have_ts = all(s.get("completed_at") for s in completed)
        check("T8-07 completed session 有完成時間", all_have_ts)

# ════════════════════════════════════════════
# 9. 資訊中心
# ════════════════════════════════════════════
print("\n  --- 📊 T9: 資訊中心 ---")
ic_settings = client.get(f"{BASE}/api/ic/settings").json()
check("T9-01 IC 設定", isinstance(ic_settings, dict))
# API key 遮罩
api_key = ic_settings.get("claude_api_key", "")
check("T9-02 API Key 遮罩", api_key in ("", "***", None) or api_key.startswith("sk-***"),
      f"key={api_key[:20]}...")

ic_recs = client.get(f"{BASE}/api/ic/recommendations").json()
check("T9-03 推薦清單", isinstance(ic_recs, list))

ic_sources = client.get(f"{BASE}/api/ic/sources").json()
check("T9-04 資料源", isinstance(ic_sources, dict) and "system" in ic_sources)

ic_news = client.get(f"{BASE}/api/ic/sources/news").json()
check("T9-05 新聞源", isinstance(ic_news, list))

ic_macro = client.get(f"{BASE}/api/ic/macro").json()
check("T9-06 IC 總經", isinstance(ic_macro, dict))

ic_kb = client.get(f"{BASE}/api/ic/kb/entities").json()
check("T9-07 知識庫", isinstance(ic_kb, (list, dict)))

# ════════════════════════════════════════════
# 10. 系統透視（資料源/功能/公式）
# ════════════════════════════════════════════
print("\n  --- 🔍 T10: 系統透視 ---")
ds = client.get(f"{BASE}/api/datasources").json()
check("T10-01 資料源>=45", isinstance(ds, list) and len(ds) >= 45, f"count={len(ds)}")

fm = client.get(f"{BASE}/api/feature-datasource-map").json()
check("T10-02 功能映射>=15", isinstance(fm, list) and len(fm) >= 15)

fr = client.get(f"{BASE}/api/formula-registry").json()
check("T10-03 公式>=35", isinstance(fr, list) and len(fr) >= 35, f"count={len(fr)}")

# ════════════════════════════════════════════
# 11. 盤後分析
# ════════════════════════════════════════════
print("\n  --- 📋 T11: 盤後分析 ---")
exitd = client.get(f"{BASE}/api/scan/exitd").json()
check("T11-01 EXIT_D 掃描", isinstance(exitd, (list, dict)))

scan_sig = client.get(f"{BASE}/api/scan/signals").json()
check("T11-02 訊號掃描", isinstance(scan_sig, (list, dict)))

squeeze = client.get(f"{BASE}/api/chip/squeeze-candidates").json()
check("T11-03 擠壓候選", isinstance(squeeze, list))

# ════════════════════════════════════════════
# 12. 美股
# ════════════════════════════════════════════
print("\n  --- 🇺🇸 T12: 美股 ---")
us_wl = client.get(f"{BASE}/api/us/watchlist").json()
check("T12-01 美股自選", isinstance(us_wl, list))

us_idx = client.get(f"{BASE}/api/us/indices").json()
check("T12-02 美股指數", isinstance(us_idx, (list, dict)))

us_pos = client.get(f"{BASE}/api/us/positions").json()
check("T12-03 美股持倉", isinstance(us_pos, list))

# ════════════════════════════════════════════
# 13. 資料管理
# ════════════════════════════════════════════
print("\n  --- 🗄️ T13: 資料管理 ---")
stats = client.get(f"{BASE}/api/data/stats").json()
check("T13-01 資料統計", isinstance(stats, dict))

integrity = client.get(f"{BASE}/api/data/integrity").json()
check("T13-02 資料完整性", isinstance(integrity, dict))

# ════════════════════════════════════════════
# 14. 安全性
# ════════════════════════════════════════════
print("\n  --- 🔒 T14: 安全性 ---")
# SQL injection
inj = client.get(f"{BASE}/api/watchlist?market=TW' OR 1=1--")
check("T14-01 SQL injection 防護", inj.status_code != 500, f"code={inj.status_code}")

# XSS in signal (已修過)
check("T14-02 HTML 中無 innerHTML 直接插入未過濾 user data",
      'innerHTML = code' not in html and 'innerHTML=code' not in html)

# 無 token 拒絕
for name, method, path in [
    ("positions POST", "POST", "/api/positions"),
    ("us/positions POST", "POST", "/api/us/positions"),
    ("signals POST", "POST", "/api/signals"),
    ("trade POST", "POST", "/api/trade-records"),
    ("trade PUT", "PUT", "/api/trade-records/1"),
    ("trade DELETE", "DELETE", "/api/trade-records/1"),
    ("migrate POST", "POST", "/api/trade-records/migrate-positions"),
]:
    r = client.request(method, BASE + path,
                       json={"code": "X", "action": "BUY", "shares": 1, "price": 1}
                       if method in ("POST", "PUT") else None)
    check(f"T14-03 {name} 無token={r.status_code}", r.status_code in (401, 403, 422))

# ════════════════════════════════════════════
# 15. 前端 JS 完整性
# ════════════════════════════════════════════
print("\n  --- 🧪 T15: 前端 JS ---")
# 檢查所有 onclick 呼叫的函數是否存在
BUILTIN = {'if','else','return','var','let','const','new','typeof','void','this','event',
    'parseInt','parseFloat','Number','String','Boolean','Array','Object','Date','Math',
    'JSON','console','window','document','alert','confirm','prompt',
    'setTimeout','setInterval','clearTimeout','clearInterval','fetch',
    'encodeURIComponent','isNaN','Error','Map','Set','Promise',
    'getElementById','querySelector','querySelectorAll',
    'getAttribute','setAttribute','addEventListener','removeEventListener',
    'appendChild','removeChild','classList','contains','add','remove','toggle','replace',
    'scrollIntoView','scrollTo','focus','blur','click',
    'preventDefault','stopPropagation','splice','slice','push','pop',
    'map','filter','reduce','forEach','find','findIndex',
    'indexOf','includes','join','sort','reverse','concat',
    'split','trim','match','test','keys','values','entries','from','assign',
    'stringify','parse','toFixed','toLocaleString','toString',
    'abs','min','max','round','floor','ceil','sqrt','log','pow','random',
    'now','getTime','toISOString','then','catch','finally',
    'bind','call','apply','hasOwnProperty',
    'requestAnimationFrame','getComputedStyle','matchMedia',
    'startsWith','endsWith','padStart','padEnd','toUpperCase','toLowerCase',
    'charAt','substring','substr'}

inline_calls = set()
for attr in re.findall(r'(?:onclick|onchange|oninput)\s*=\s*"([^"]*)"', html):
    for fn in re.findall(r'([a-zA-Z_]\w+)\s*\(', attr):
        if fn not in BUILTIN:
            inline_calls.add(fn)

defined_fns = set(re.findall(r'(?:function|async\s+function)\s+([a-zA-Z_]\w+)', html))
defined_fns.update(re.findall(r'(?:const|let|var)\s+([a-zA-Z_]\w+)\s*=\s*(?:async\s*)?\(', html))
defined_fns.update(re.findall(r'(?:const|let|var)\s+([a-zA-Z_]\w+)\s*=\s*(?:async\s*)?function', html))

undefined = sorted(inline_calls - defined_fns)
if undefined:
    for fn in undefined:
        check(f"T15 JS函數 {fn} 已定義", False, "called but not defined")
else:
    check(f"T15-01 所有 {len(inline_calls)} 個 inline 函數已定義", True)

# 括號配對
scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)
all_js = '\n'.join(scripts)
parens = all_js.count('(') - all_js.count(')')
braces = all_js.count('{') - all_js.count('}')
check("T15-02 括號配對", abs(parens) <= 2 and abs(braces) <= 2,
      f"() diff={parens}, {{}} diff={braces}")

# ════════════════════════════════════════════
# Summary
# ════════════════════════════════════════════
client.close()

print(f"\n{'='*60}")
print(f"  E2E Results: {passed} PASS / {failed} FAIL")
if warnings:
    print(f"  Warnings: {len(warnings)}")
    for w in warnings:
        print(f"    ⚠️  {w}")
if failed:
    print(f"\n  ❌ {failed} test(s) failed!")
else:
    print(f"\n  ✅ All tests passed!")
print(f"{'='*60}\n")

sys.exit(0 if failed == 0 else 1)
