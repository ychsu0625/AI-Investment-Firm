# -*- coding: utf-8 -*-
"""隔離測：ops/forward_daily_cron.py（半自動 forward cron 編排）。

全程不碰 :8766 / 不碰 live 計分（spec §6）：用一個 in-process mock HTTP server 模擬
5 個 endpoint，run-log 寫 temp sqlite，token 寫 temp 檔。涵蓋：
  1. dry_run 編排測：auto-roll dry_run:true、search/run/update 跳過、序列/落地/守門正確。
  2. 冪等測：同 run_date 連跑兩次第二次 skip；--force 時不被守門擋。
  3. 失敗注入測：某步非 200 → status partial、error 入表、後續步驟仍跑、摘要紅字。
  4. token 從檔讀：mock server 收到正確 X-API-Token header；token 不外洩進 log/摘要/run-log/argv。
  5. health-fail abort：/api/health ok:false → status error、不打後續步驟。
  6. 零回歸感：腳本 enable_replace 永遠送 False（寫死，body 不開覆寫）。

可直接 `python tests/test_forward_cron.py` 跑（不依賴 pytest）。
"""
import importlib.util
import json
import sqlite3
import sys
import tempfile
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# ── 載入待測模組（ops/forward_daily_cron.py）─────────────────────────────────
_REPO = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("forward_daily_cron", _REPO / "ops" / "forward_daily_cron.py")
cron = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cron)

TOKEN = "x" * 40   # ≥32 chars


# ══════════════════════════════════════════════════════════════════════════════
# Mock backend server
# ══════════════════════════════════════════════════════════════════════════════
class MockState:
    def __init__(self):
        self.calls = []                 # (method, path, query, body, headers)
        self.fail = {}                  # path -> (status, body)  注入失敗
        self.health_ok = True
        self.search_status_seq = ["done"]   # status poll 回傳序列
        self._search_i = 0


def make_handler(state: MockState):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):      # 靜音
            pass

        def _send(self, code, obj):
            body = json.dumps(obj).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_body(self):
            n = int(self.headers.get("Content-Length", 0) or 0)
            if n:
                try:
                    return json.loads(self.rfile.read(n).decode("utf-8"))
                except Exception:
                    return None
            return None

        def _record(self, method):
            from urllib.parse import urlsplit, parse_qs
            u = urlsplit(self.path)
            body = self._read_body() if method == "POST" else None
            state.calls.append({
                "method": method, "path": u.path, "query": parse_qs(u.query),
                "body": body, "token": self.headers.get("X-API-Token"),
            })
            return u.path, parse_qs(u.query), body

        def _maybe_fail(self, path):
            if path in state.fail:
                code, msg = state.fail[path]
                self._send(code, {"ok": False, "error": msg})
                return True
            return False

        def do_GET(self):
            path, query, _ = self._record("GET")
            if path == "/api/health":
                self._send(200, {"ok": bool(state.health_ok)})
                return
            if self._maybe_fail(path):
                return
            if path == "/api/search/auto/status":
                i = min(state._search_i, len(state.search_status_seq) - 1)
                st = state.search_status_seq[i]
                state._search_i += 1
                self._send(200, {"ok": True, "run": {"run_id": query.get("run_id", [""])[0], "status": st}})
                return
            if path == "/api/forward/track":
                self._send(200, {"ok": True, "as_of": "2026-06-26", "strategies": [
                    {"name": "near_grad_strat", "strategy_id": 1, "status": "active",
                     "closed": 12, "live_avg_excess": 1.8, "closed_avg_excess": 2.0,
                     "win_rate_pct": 60.0, "roll_dsr": 0.92, "days_since_start": 40, "decay": None},
                    {"name": "young_strat", "strategy_id": 2, "status": "active",
                     "closed": 2, "live_avg_excess": 0.5, "closed_avg_excess": None,
                     "win_rate_pct": None, "roll_dsr": 0.91, "days_since_start": 5, "decay": None},
                ]})
                return
            self._send(404, {"ok": False, "error": "not found"})

        def do_POST(self):
            path, query, body = self._record("POST")
            if self._maybe_fail(path):
                return
            if path == "/api/search/auto/start":
                self._send(200, {"ok": True, "run_id": "deadbeefcafe0001"})
                return
            if path == "/api/forward/auto-roll":
                self._send(200, {"ok": True, "dry_run": (body or {}).get("dry_run"),
                                 "rolled": [{"name": "a"}], "rejected_collinear": [],
                                 "rejected_gate": [{"name": "b"}], "replaced": [],
                                 "summary": {"rolled": 1, "rejected_collinear": 0,
                                             "rejected_gate": 1, "replaced": 0}})
                return
            if path == "/api/forward/run":
                self._send(200, {"ok": True, "requested_date": query.get("date", [""])[0],
                                 "results": [{"strategy_id": 1, "name": "s1", "n_picks": 3},
                                             {"strategy_id": 2, "name": "s2", "skipped": True}]})
                return
            if path == "/api/forward/update":
                self._send(200, {"ok": True, "entered": 2, "marked": 5, "settled": 1})
                return
            self._send(404, {"ok": False, "error": "not found"})
    return H


def start_server(state):
    srv = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(state))
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}"


# ── 測試腳手架 ────────────────────────────────────────────────────────────────
def _mk_opts(tmp, base, **over):
    db_path = str(Path(tmp) / "monitor.db")
    state_dir = str(Path(tmp) / "state"); Path(state_dir).mkdir(exist_ok=True)
    opts = {
        "base_url": base, "token": TOKEN, "db_path": db_path,
        "market": "TW", "regime": "TREND_UP", "budget": 120,
        "run_date": "2026-06-26", "force": False, "dry_run": False,
        "search_timeout": 30, "step_timeout": 10, "total_timeout": 60, "search_poll": 0,
        "state_dir": state_dir,
    }
    opts.update(over)
    return opts


def _paths(method=None, path=None, state=None):
    return [c for c in state.calls if (method is None or c["method"] == method)
            and (path is None or c["path"] == path)]


# ══════════════════════════════════════════════════════════════════════════════
# 測試
# ══════════════════════════════════════════════════════════════════════════════
def test_dry_run_orchestration():
    """dry-run：auto-roll dry_run:true、search/run/update 跳過、status=dryrun、摘要落地、不計 done。"""
    state = MockState(); srv, base = start_server(state)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            opts = _mk_opts(tmp, base, dry_run=True)
            res = cron.run_cron(opts)
            assert res["status"] == "dryrun", res["status"]
            # auto-roll 有打且 dry_run:true、enable_replace:false
            ar = _paths("POST", "/api/forward/auto-roll", state)
            assert len(ar) == 1, ar
            assert ar[0]["body"]["dry_run"] is True
            assert ar[0]["body"]["enable_replace"] is False
            # search/run/update 不打
            assert not _paths("POST", "/api/search/auto/start", state)
            assert not _paths("POST", "/api/forward/run", state)
            assert not _paths("POST", "/api/forward/update", state)
            # health + track 有打
            assert _paths("GET", "/api/health", state)
            assert _paths("GET", "/api/forward/track", state)
            # run-log 寫成 dryrun（不擋真跑）
            row = cron._get_run(opts["db_path"], "2026-06-26")
            assert row and row["status"] == "dryrun", row
            # 摘要 md 存在
            assert Path(res["summary_path"]).exists()
    finally:
        srv.shutdown()
    print("PASS test_dry_run_orchestration")


def test_idempotency_skip_and_force():
    """同 run_date 連跑：第一次 done；第二次無 force → skip（不打步驟）；--force → 重打。"""
    state = MockState(); srv, base = start_server(state)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            opts = _mk_opts(tmp, base)
            r1 = cron.run_cron(opts)
            assert r1["status"] == "done", r1["status"]
            n_after_first = len(state.calls)
            # 第二次無 force → skip，不應再打任何 endpoint
            r2 = cron.run_cron(opts)
            assert r2.get("skipped") is True and r2["status"] == "skipped", r2
            assert len(state.calls) == n_after_first, "skip 不應再打 endpoint"
            # --force → 重跑（會再打）
            r3 = cron.run_cron({**opts, "force": True})
            assert r3["status"] == "done", r3["status"]
            assert len(state.calls) > n_after_first, "force 應重打 endpoint"
    finally:
        srv.shutdown()
    print("PASS test_idempotency_skip_and_force")


def test_failure_injection_partial():
    """auto-roll 注入 500 → status=partial、error 入表、後續 run/update/track 仍跑、摘要紅字。"""
    state = MockState(); srv, base = start_server(state)
    state.fail["/api/forward/auto-roll"] = (500, "boom")
    try:
        with tempfile.TemporaryDirectory() as tmp:
            opts = _mk_opts(tmp, base)
            res = cron.run_cron(opts)
            assert res["status"] == "partial", res["status"]
            # 後續步驟未被污染中斷：run/update/track 都有打
            assert _paths("POST", "/api/forward/run", state)
            assert _paths("POST", "/api/forward/update", state)
            assert _paths("GET", "/api/forward/track", state)
            row = cron._get_run(opts["db_path"], "2026-06-26")
            assert row["status"] == "partial" and row["error"] and "auto_roll" in row["error"], row
            # 摘要含紅字
            md = Path(res["summary_path"]).read_text(encoding="utf-8")
            assert "🟥" in md and "ERROR" in md, md[:300]
    finally:
        srv.shutdown()
    print("PASS test_failure_injection_partial")


def test_token_from_file_not_leaked():
    """token 經 build_opts 從檔讀 → 注入 header；不出現在 argv / log / 摘要 / run-log。"""
    state = MockState(); srv, base = start_server(state)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            tok_file = Path(tmp) / ".api_token"
            tok_file.write_text(TOKEN, encoding="utf-8")
            db_path = str(Path(tmp) / "monitor.db")
            state_dir = str(Path(tmp) / "state"); Path(state_dir).mkdir()
            argv = ["--base-url", base, "--token-file", str(tok_file),
                    "--db-path", db_path, "--state-dir", state_dir,
                    "--run-date", "2026-06-26", "--search-poll", "0"]
            # main 跑完整 CLI 路徑
            code = cron.main(argv)
            assert code == 0, code
            # 每個 call 都帶正確 token header
            assert state.calls, "應有呼叫"
            assert all(c["token"] == TOKEN for c in state.calls), \
                [c["path"] for c in state.calls if c["token"] != TOKEN]
            # token 不在 argv
            assert TOKEN not in " ".join(argv)
            # token 不在摘要 / run-log
            md = (Path(state_dir) / "_forward_cron_2026-06-26.md").read_text(encoding="utf-8")
            assert TOKEN not in md
            row = cron._get_run(db_path, "2026-06-26")
            assert TOKEN not in json.dumps(row, ensure_ascii=False)
            # _safe_url 去 query（即使 query 含敏感值也不入 log）
            assert cron._safe_url("http://h/x?a=secret") == "http://h/x"
    finally:
        srv.shutdown()
    print("PASS test_token_from_file_not_leaked")


def test_health_fail_aborts():
    """health ok:false → status=error、不打任何後續步驟（不盲打 prod）。"""
    state = MockState(); state.health_ok = False
    srv, base = start_server(state)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            opts = _mk_opts(tmp, base)
            res = cron.run_cron(opts)
            assert res["status"] == "error", res["status"]
            # 只打了 health，沒打任何 POST 步驟
            assert _paths("GET", "/api/health", state)
            assert not _paths("POST", "/api/search/auto/start", state)
            assert not _paths("POST", "/api/forward/auto-roll", state)
            assert not _paths("POST", "/api/forward/run", state)
            row = cron._get_run(opts["db_path"], "2026-06-26")
            assert row["status"] == "error" and "health" in (row["error"] or ""), row
    finally:
        srv.shutdown()
    print("PASS test_health_fail_aborts")


def test_enable_replace_hardcoded_false():
    """硬不變式：正常實滾時 auto-roll body 永遠 enable_replace:false、search 帶 sell_strategies。"""
    state = MockState(); srv, base = start_server(state)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            opts = _mk_opts(tmp, base)
            cron.run_cron(opts)
            ar = _paths("POST", "/api/forward/auto-roll", state)[0]
            assert ar["body"]["enable_replace"] is False
            ss = _paths("POST", "/api/search/auto/start", state)[0]
            assert ss["body"]["sell_strategies"] == ["EXIT_C", "EXIT_D"], ss["body"]
            assert ss["body"]["market"] == "TW"
            assert ss["body"]["budget"] == 120
    finally:
        srv.shutdown()
    print("PASS test_enable_replace_hardcoded_false")


def test_search_timeout_partial():
    """搜尋 poll 永遠 running 至 step timeout → 該步 partial、不中斷、整體往下滾既有候選。"""
    state = MockState(); state.search_status_seq = ["running"]   # 永不終態
    srv, base = start_server(state)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            opts = _mk_opts(tmp, base, search_timeout=1, search_poll=0)
            res = cron.run_cron(opts)
            assert res["status"] == "partial", res["status"]
            search_step = [s for s in res["steps"] if s["step"] == "1_search"][0]
            assert search_step["status"] == "partial", search_step
            # 後續 auto-roll 仍跑
            assert _paths("POST", "/api/forward/auto-roll", state)
    finally:
        srv.shutdown()
    print("PASS test_search_timeout_partial")


def _run_all():
    tests = [test_dry_run_orchestration, test_idempotency_skip_and_force,
             test_failure_injection_partial, test_token_from_file_not_leaked,
             test_health_fail_aborts, test_enable_replace_hardcoded_false,
             test_search_timeout_partial]
    failed = 0
    for t in tests:
        try:
            t()
        except AssertionError as e:
            failed += 1; print(f"FAIL {t.__name__}: {e}")
        except Exception as e:
            failed += 1; print(f"ERROR {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return failed


if __name__ == "__main__":
    sys.exit(1 if _run_all() else 0)
