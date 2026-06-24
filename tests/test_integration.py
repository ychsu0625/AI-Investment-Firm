"""
L3 Integration Test: 驗證跨模組資料一致性 — 系統透視三維度 + 參數傳播
用法: python test_integration.py [--base http://localhost:8765]
"""
import sys, json
try:
    import httpx
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "httpx", "-q"])
    import httpx

BASE = sys.argv[sys.argv.index("--base") + 1] if "--base" in sys.argv else "http://localhost:8765"

passed, failed = 0, 0

def check(name, condition, detail=""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  PASS  {name}")
    else:
        failed += 1
        print(f"  FAIL  {name} — {detail}")

def GET(path):
    return httpx.get(BASE + path, timeout=15).json()

def run():
    global passed, failed
    print(f"\n{'='*60}")
    print(f"  L3 Integration Test @ {BASE}")
    print(f"{'='*60}\n")

    # ── D1: 資料源 ic_used 與 IC_SYSTEM_SOURCES 一致 ──
    ds = GET("/api/datasources")
    ic_used_ids = {d["id"] for d in ds if d.get("ic_used")}
    check("D1 ic_used 有標記",
          len(ic_used_ids) >= 5,
          f"only {len(ic_used_ids)} ic_used sources")

    # ── D2: 功能→資料源 完整性 ──
    fm = GET("/api/feature-datasource-map")
    check("D2 feature-map 數量",
          len(fm) >= 15,
          f"only {len(fm)} features")
    bad_fm = [f["id"] for f in fm if not f.get("datasource_details")]
    check("D2 每功能有 datasource_details",
          len(bad_fm) == 0,
          f"missing: {bad_fm}")
    for f in fm:
        for dd in f.get("datasource_details", []):
            if "name" not in dd or "status" not in dd:
                check(f"D2 {f['id']} detail schema", False, f"missing name/status in {dd}")
                break

    # ── D3: 公式 feature 對應 feature-map ──
    formulas = GET("/api/formula-registry")
    fm_ids = {f["id"] for f in fm}
    formula_features = {f.get("feature") for f in formulas if f.get("feature")}
    unmatched = formula_features - fm_ids
    check("D3 公式 feature 全匹配 feature-map",
          len(unmatched) == 0,
          f"unmatched: {unmatched}")

    # ── D4: strategy_link 存在於 strategies ──
    strategies = GET("/api/strategies")
    strat_ids = {s["id"] for s in strategies}
    for f in formulas:
        sl = f.get("strategy_link")
        if sl:
            check(f"D4 strategy_link {sl} exists",
                  sl in strat_ids,
                  f"{sl} not in strategies")

    # ── 公式結構驗證 ──
    check("D5 公式數量>=35",
          len(formulas) >= 35,
          f"only {len(formulas)}")
    for f in formulas:
        check(f"D6 {f['id']} has formula",
              bool(f.get("formula")),
              "empty formula")
        check(f"D6 {f['id']} has category",
              bool(f.get("category")),
              "empty category")
        check(f"D6 {f['id']} has external dict",
              isinstance(f.get("external"), dict),
              f"external={f.get('external')}")
        for p in f.get("params", []):
            if p.get("min") is not None:
                check(f"D7 {f['id']}.{p['key']} param schema",
                      all(k in p for k in ["key","value","default","min","max","step"]),
                      f"missing fields in {p}")

    # ── P1: 參數修改 ──
    print("\n  --- 參數修改傳播 ---")
    token = GET("/api/auth/token")
    headers = {"Content-Type": "application/json"}
    if isinstance(token, str):
        headers["X-API-Token"] = token
    elif isinstance(token, dict) and "token" in token:
        headers["X-API-Token"] = token["token"]

    r = httpx.post(BASE + "/api/formula-registry/params",
                   json={"changes": {"rsi_oversold": 25}},
                   headers=headers, timeout=10).json()
    check("P1 參數修改成功", r.get("ok") and r.get("applied", {}).get("rsi_oversold") == 25)

    reg = GET("/api/formula-registry")
    rsi_formula = [f for f in reg if f["id"] == "tech_rsi"][0]
    rsi_param = [p for p in rsi_formula["params"] if p["key"] == "rsi_oversold"][0]
    check("P1 修改後值正確", rsi_param["value"] == 25, f"got {rsi_param['value']}")
    check("P1 default 未變", rsi_param["default"] == 30, f"got {rsi_param['default']}")

    # ── P2: 參數重置 ──
    r = httpx.post(BASE + "/api/formula-registry/reset",
                   headers=headers, timeout=10).json()
    check("P2 重置成功", r.get("ok"))

    reg = GET("/api/formula-registry")
    rsi_formula = [f for f in reg if f["id"] == "tech_rsi"][0]
    rsi_param = [p for p in rsi_formula["params"] if p["key"] == "rsi_oversold"][0]
    check("P2 重置後值回 default", rsi_param["value"] == 30, f"got {rsi_param['value']}")

    # ── IC 分析完整性 ──
    print("\n  --- IC 分析完整性 ---")
    ic_sources = GET("/api/ic/sources")
    check("IC sources has system", isinstance(ic_sources.get("system"), list))
    check("IC sources has user", isinstance(ic_sources.get("user"), list))
    sys_srcs = ic_sources.get("system", [])
    has_linked = any(s.get("linked_datasources") for s in sys_srcs)
    check("IC system sources have linked_datasources", has_linked)

    sectors = GET("/api/ic/sector-rotation")
    if isinstance(sectors, list) and len(sectors) > 0:
        ranks = [s.get("rank") for s in sectors]
        check("IC sector rotation 排名不重複",
              len(ranks) == len(set(ranks)),
              f"ranks: {ranks}")
    else:
        check("IC sector rotation 有資料", False, "empty or error")

    # ── 資料源 schema 驗證 ──
    print("\n  --- 資料源 schema ---")
    check("DS 總數>=45", len(ds) >= 45, f"only {len(ds)}")
    scopes = {d.get("market_scope") for d in ds}
    check("DS 含四種市場", scopes >= {"TW", "US", "ALL"}, f"scopes: {scopes}")
    for d in ds:
        needed = ["id", "name", "status", "category", "market_scope", "ic_used", "configured"]
        missing = [k for k in needed if k not in d]
        if missing:
            check(f"DS {d.get('id','?')} schema", False, f"missing: {missing}")
            break

    # ── 交易紀錄 CRUD ──
    print("\n  --- 交易紀錄 CRUD ---")
    trades_before = GET("/api/trade-records")
    check("TR1 trade-records is list", isinstance(trades_before, list))

    # 新增
    tr = httpx.post(BASE + "/api/trade-records",
                    json={"code": "9999", "name": "測試股", "action": "BUY",
                          "shares": 1, "price": 50.0, "trade_date": "2026-06-01"},
                    headers=headers, timeout=10).json()
    check("TR2 新增交易成功", tr.get("ok") and "id" in tr)
    tr_id = tr.get("id")

    # 查詢
    trades = GET("/api/trade-records?code=9999")
    check("TR3 filter by code", len(trades) >= 1 and trades[0]["code"] == "9999")
    check("TR3 cost fields present",
          all(k in trades[0] for k in ["commission", "tax", "total_cost", "net_amount"]))

    # 編輯
    if tr_id:
        tr2 = httpx.put(BASE + f"/api/trade-records/{tr_id}",
                        json={"code": "9999", "name": "測試股改", "action": "BUY",
                              "shares": 2, "price": 55.0, "trade_date": "2026-06-02"},
                        headers=headers, timeout=10).json()
        check("TR4 編輯交易成功", tr2.get("ok"))
        trades2 = GET("/api/trade-records?code=9999")
        updated = [t for t in trades2 if t["id"] == tr_id]
        check("TR4 編輯後數據正確",
              updated and updated[0]["shares"] == 2 and updated[0]["price"] == 55.0)

    # 分析
    analytics = GET("/api/trade-records/analytics?code=9999")
    check("TR5 analytics 結構正確",
          "realized_pnl" in analytics and "stocks" in analytics and "9999" in analytics.get("stocks", {}))

    # 清理
    if tr_id:
        dr = httpx.delete(BASE + f"/api/trade-records/{tr_id}", headers=headers, timeout=10).json()
        check("TR6 刪除交易成功", dr.get("ok"))

    # 清理持倉中自動建立的 9999
    pos_all = httpx.get(BASE + "/api/positions", timeout=10).json()
    for p in pos_all:
        if p.get("code") == "9999":
            httpx.delete(BASE + f"/api/positions/{p['id']}", headers=headers, timeout=10)

    # ── 專家委員會 ──
    print("\n  --- 專家委員會 ---")
    roles = GET("/api/expert/roles")
    check("EX1 roles >= 5", isinstance(roles, list) and len(roles) >= 5)
    check("EX1 role schema",
          all(k in roles[0] for k in ["id", "name", "icon", "perspective"]))

    ex_config = GET("/api/expert/config")
    check("EX2 config is dict", isinstance(ex_config, dict))

    schedules = GET("/api/expert/schedules")
    check("EX3 schedules >= 2", isinstance(schedules, list) and len(schedules) >= 2)
    check("EX3 schedule schema",
          all(k in schedules[0] for k in ["id", "name", "enabled"]))

    sessions = GET("/api/expert/sessions")
    check("EX4 sessions has data", isinstance(sessions, dict) and "sessions" in sessions)

    # ── 策略分類 ──
    print("\n  --- 策略分類 ---")
    strats = GET("/api/strategies")
    check("ST1 strategies >= 19", len(strats) >= 19)
    builtin = [s for s in strats if s.get("strat_type") == "builtin"]
    custom = [s for s in strats if s.get("strat_type") == "custom"]
    check("ST2 有 builtin 策略", len(builtin) >= 15)
    check("ST3 有 custom 策略", len(custom) >= 1)
    formula_linked = [s for s in strats if s.get("formula_link")]
    check("ST4 有 formula_link 策略", len(formula_linked) >= 5)

    # ── 交易紀錄 Auth 保護 ──
    print("\n  --- 交易紀錄 Auth ---")
    r_noauth = httpx.post(BASE + "/api/trade-records",
                          json={"code": "2330", "action": "BUY", "shares": 1, "price": 100},
                          timeout=10)
    check("AUTH1 POST trade-records 無token拒絕", r_noauth.status_code in (401, 403))

    r_noauth2 = httpx.put(BASE + "/api/trade-records/1",
                          json={"code": "2330", "action": "BUY", "shares": 1, "price": 100},
                          timeout=10)
    check("AUTH2 PUT trade-records 無token拒絕", r_noauth2.status_code in (401, 403))

    r_noauth3 = httpx.delete(BASE + "/api/trade-records/1", timeout=10)
    check("AUTH3 DELETE trade-records 無token拒絕", r_noauth3.status_code in (401, 403))

    r_noauth4 = httpx.post(BASE + "/api/trade-records/migrate-positions", timeout=10)
    check("AUTH4 migrate-positions 無token拒絕", r_noauth4.status_code in (401, 403))

    # ── 總結 ──
    print(f"\n{'='*60}")
    print(f"  Results: {passed} PASS / {failed} FAIL")
    print(f"{'='*60}\n")
    return 0 if failed == 0 else 1

if __name__ == "__main__":
    sys.exit(run())
