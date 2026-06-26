#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""半自動 Forward Daily Cron — standalone 編排腳本（SIM tool team / coder）

工單：ai-investment-firm/2026-06-26-semiauto-forward-cron-requirements.md
設計：ai-investment-firm/2026-06-26-forward-loop-design.md（閘的不對稱：ROLL 低門檻 / GRADUATION 嚴）

把已閉環的「daemon 搜 → auto-roll 過閘滾入 → run/update 推進計分」接成每天自動跑一次的
**半自動**迴圈，讓借券 sbl_short 與所有 active 策略每日自動累積紙上 OOS 戰績。

每日序列（spec §2，按序、每步 timeout + 結果落地）：
  1. daemon 搜新組合     POST /api/search/auto/start  → poll GET /api/search/auto/status
  2. 過閘滾入 forward     POST /api/forward/auto-roll  (enable_replace=false 寫死)
  3. 今日選股            POST /api/forward/run?date=<today>
  4. 結算/計分推進        POST /api/forward/update
  5. 畢業閘＝只報不動      GET  /api/forward/track  （唯讀彙整「接近畢業」清單）

硬不變式（spec §1/§5，違反=退件）：
  * **半自動**：cron 只做搜/滾/選股/結算/彙整 5 步。**畢業、真錢一律人工。**
    本腳本不 import 任何下單 / live-execution / promote code path（只打上面 5 個 HTTP endpoint）。
  * enable_replace=false 寫死；market 預設僅 ["TW"]；budget 有界（預設 120）。
  * 冪等 + 今日已跑守門（同 run_date 已 done 就 skip，除非 --force）；跑前 /api/health 檢查。
  * token 不上命令列：從 ui/.api_token 讀檔注入 X-API-Token header（不寫進 log / argv）。

部署：Windows Task Scheduler 每日觸發（schtasks 指令見 ops/forward_daily_cron_schtasks.md）。
      **實際註冊到系統由 cockpit/老闆執行；本腳本不擅自註冊排程。**

設計刻意不 import backend.py（spec §3 鐵律#2：不碰 prod backend、零重啟風險）。
只用 stdlib（urllib / sqlite3 / json / argparse），run-log 直接寫 monitor.db 的 forward_cron_runs 表
（建表冪等；backend.py 亦於 module-load 建同表 + 啟動對帳，兩邊各自冪等不衝突）。
"""

import argparse
import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

# ── 預設常數（硬不變式：可被 CLI 覆寫的留參數，寫死的不開 flag）──────────────────────────
DEFAULT_BASE_URL = "http://127.0.0.1:8766"        # 老婆 production；cron 只打 HTTP，不重啟它
DEFAULT_MARKET = "TW"                              # spec §2 v1 僅 TW；US 暫不納入 auto-roll
DEFAULT_REGIME = "TREND_UP"
DEFAULT_BUDGET = 120                               # 有界 budget（別無限跑吃資源）
BUDGET_HARD_CAP = 500                              # 保險絲：CLI 給太大也夾住
DEFAULT_SEARCH_TIMEOUT = 1800                      # 搜尋步上限 30 min（spec §2）
DEFAULT_STEP_TIMEOUT = 300                         # 其餘步驟單次 HTTP 上限 5 min
DEFAULT_SEARCH_POLL = 10                           # 搜尋 status poll 間隔（秒）
DEFAULT_TOTAL_TIMEOUT = 3600                       # 總 run 硬上限 60 min

ENABLE_REPLACE = False                             # ★寫死（鐵律#1，取代依賴 holdout=R-FWD-9 未做）

# repo 內路徑推導（ops/ 在 repo 根；ui/ 在隔壁）
_OPS_DIR = Path(__file__).resolve().parent
_REPO_DIR = _OPS_DIR.parent
DEFAULT_TOKEN_FILE = _REPO_DIR / "ui" / ".api_token"
DEFAULT_DB_PATH = _REPO_DIR / "ui" / "monitor.db"
# 摘要寫檔目錄（spec §4.2）：n8n-claude/state/，與 repo 同層父目錄下；不存在則退回 ops/_state
_CANDIDATE_STATE_DIRS = [
    _REPO_DIR.parent / "n8n-claude" / "state",
    Path.home() / "Documents" / "Claude_Files" / "n8n-claude" / "state",
]


# ══════════════════════════════════════════════════════════════════════════════
# HTTP helpers（urllib；token 走 header，不入 argv/log）
# ══════════════════════════════════════════════════════════════════════════════
class HttpError(Exception):
    """非 2xx 或傳輸層失敗。帶 status（None=傳輸/逾時）與 body 片段供 log。"""
    def __init__(self, msg, status=None, body=None):
        super().__init__(msg)
        self.status = status
        self.body = body


def _request(method, url, token, body=None, timeout=DEFAULT_STEP_TIMEOUT):
    data = None
    headers = {"Accept": "application/json"}
    if token:
        headers["X-API-Token"] = token            # ★token 只在 header，永不進 URL/argv/log
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                raise HttpError(f"{method} {_safe_url(url)} 回傳非 JSON", status=resp.status, body=raw[:500])
    except urllib.error.HTTPError as e:
        raw = ""
        try:
            raw = e.read().decode("utf-8", "replace")
        except Exception:
            pass
        raise HttpError(f"{method} {_safe_url(url)} → HTTP {e.code}", status=e.code, body=raw[:500])
    except urllib.error.URLError as e:
        raise HttpError(f"{method} {_safe_url(url)} 連線失敗: {e.reason}", status=None, body=None)
    except (TimeoutError, OSError) as e:
        raise HttpError(f"{method} {_safe_url(url)} 逾時/IO: {e}", status=None, body=None)


def _safe_url(url):
    """log 用：去掉 query 內任何敏感值（本腳本不放 token 進 query，仍保險去 query）。"""
    p = urllib.parse.urlsplit(url)
    return urllib.parse.urlunsplit((p.scheme, p.netloc, p.path, "", ""))


def _post(base, path, token, body=None, timeout=DEFAULT_STEP_TIMEOUT):
    return _request("POST", base.rstrip("/") + path, token, body=body, timeout=timeout)


def _get(base, path, token, params=None, timeout=DEFAULT_STEP_TIMEOUT):
    url = base.rstrip("/") + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    return _request("GET", url, token, body=None, timeout=timeout)


# ══════════════════════════════════════════════════════════════════════════════
# 持久化：forward_cron_runs（monitor.db）；同 run_date INSERT OR REPLACE 冪等
# ══════════════════════════════════════════════════════════════════════════════
def _ensure_table(db_path):
    con = sqlite3.connect(str(db_path))
    con.execute("""CREATE TABLE IF NOT EXISTS forward_cron_runs(
        run_date TEXT PRIMARY KEY, started_at TEXT, completed_at TEXT,
        status TEXT, market TEXT, search_run_id TEXT,
        rolled INTEGER DEFAULT 0, rejected_collinear INTEGER DEFAULT 0,
        rejected_gate INTEGER DEFAULT 0, replaced INTEGER DEFAULT 0,
        picks_total INTEGER DEFAULT 0, settled INTEGER DEFAULT 0,
        near_graduation_json TEXT, error TEXT, summary_json TEXT, updated_at TEXT
    )""")
    con.commit(); con.close()


def _get_run(db_path, run_date):
    con = sqlite3.connect(str(db_path)); con.row_factory = sqlite3.Row
    cur = con.execute("SELECT * FROM forward_cron_runs WHERE run_date=?", (run_date,))
    row = cur.fetchone(); con.close()
    return dict(row) if row else None


def _persist(db_path, rec):
    """落地一筆 run（INSERT OR REPLACE by run_date）。失敗印警告但不掩蓋主流程結果。"""
    rec = dict(rec)
    rec["updated_at"] = datetime.now().isoformat()
    cols = ["run_date", "started_at", "completed_at", "status", "market", "search_run_id",
            "rolled", "rejected_collinear", "rejected_gate", "replaced",
            "picks_total", "settled", "near_graduation_json", "error", "summary_json", "updated_at"]
    vals = [rec.get(c) for c in cols]
    try:
        con = sqlite3.connect(str(db_path))
        con.execute(f"INSERT OR REPLACE INTO forward_cron_runs({','.join(cols)}) "
                    f"VALUES({','.join('?' * len(cols))})", vals)
        con.commit(); con.close()
    except Exception as e:
        print(f"[forward-cron] run-log 持久化失敗: {e}", file=sys.stderr)


# ══════════════════════════════════════════════════════════════════════════════
# token / state-dir
# ══════════════════════════════════════════════════════════════════════════════
def _read_token(token_file):
    p = Path(token_file)
    tok = p.read_text(encoding="utf-8").strip()
    if len(tok) < 32:
        raise ValueError(f"token 檔內容過短（<32 chars）：{p}")
    return tok


def _resolve_state_dir(explicit):
    if explicit:
        d = Path(explicit); d.mkdir(parents=True, exist_ok=True); return d
    for d in _CANDIDATE_STATE_DIRS:
        try:
            if d.parent.exists():
                d.mkdir(parents=True, exist_ok=True); return d
        except Exception:
            continue
    fallback = _OPS_DIR / "_state"; fallback.mkdir(parents=True, exist_ok=True)
    return fallback


# ══════════════════════════════════════════════════════════════════════════════
# 接近畢業彙整（spec §2.5 / §8：只報不動，絕不 promote）
# ══════════════════════════════════════════════════════════════════════════════
_NEAR_GRAD_MIN_CLOSED = 8          # 標「接近畢業」啟示性門檻：closed ≥ 此數才看（只供人工審，非真畢業閘）
_NEAR_GRAD_MIN_EXCESS = 0.0        # live 累積超額 > 0 才提示

def _summarize_near_graduation(track):
    """從 /api/forward/track 唯讀彙整「接近畢業條件」的 active 策略供人工審。
    **不做任何 promote 判斷**；真畢業閘 R-FWD-9 是另案、一律人工（spec §8）。"""
    out = []
    for s in (track or {}).get("strategies", []):
        if s.get("status") != "active":
            continue
        n_closed = s.get("closed") or 0
        live = s.get("live_avg_excess")
        decay = (s.get("decay") or {}).get("decay_flag") if s.get("decay") else None
        flags = []
        if n_closed >= _NEAR_GRAD_MIN_CLOSED:
            flags.append(f"closed≥{_NEAR_GRAD_MIN_CLOSED}")
        if live is not None and live > _NEAR_GRAD_MIN_EXCESS:
            flags.append("live_excess>0")
        if decay == "KILL_CANDIDATE":
            flags.append("⚠DECAY_KILL_CANDIDATE")
        near = (n_closed >= _NEAR_GRAD_MIN_CLOSED and live is not None and live > _NEAR_GRAD_MIN_EXCESS)
        if near or decay == "KILL_CANDIDATE":
            out.append({"name": s.get("name"), "strategy_id": s.get("strategy_id"),
                        "closed": n_closed, "live_avg_excess": live,
                        "closed_avg_excess": s.get("closed_avg_excess"),
                        "win_rate_pct": s.get("win_rate_pct"), "roll_dsr": s.get("roll_dsr"),
                        "days_since_start": s.get("days_since_start"),
                        "decay_flag": decay, "flags": flags,
                        "note": "接近畢業條件→供人工審；cron 永不自動 promote(spec §1/§8)"})
    return out


# ══════════════════════════════════════════════════════════════════════════════
# 摘要寫檔（spec §4.2，human-readable，cockpit/老闆 3 秒掃）
# ══════════════════════════════════════════════════════════════════════════════
def _write_summary_md(state_dir, run_date, rec, steps, near_grad, dry_run):
    path = Path(state_dir) / f"_forward_cron_{run_date}.md"
    L = []
    status = rec.get("status")
    head = "🟥" if status in ("error", "partial") else ("🟦 DRY-RUN" if dry_run else "🟩")
    L.append(f"# Forward Daily Cron — {run_date}  {head} status={status}")
    L.append("")
    L.append(f"- market={rec.get('market')}  started={rec.get('started_at')}  completed={rec.get('completed_at')}")
    if dry_run:
        L.append("- **DRY-RUN 稽核模式**：auto-roll dry_run:true、search/run/update 跳過（不寫資料）；今日守門不計 done。")
    if rec.get("error"):
        L.append("")
        L.append(f"> 🟥 **ERROR**: {rec.get('error')}")
    L.append("")
    L.append("## 五步結果")
    for st in steps:
        icon = {"done": "✅", "skipped": "⏭️", "error": "❌", "partial": "⚠️", "timeout": "⏱️"}.get(st["status"], "•")
        L.append(f"- {icon} **{st['step']}** [{st['status']}] {st.get('detail', '')}")
    L.append("")
    L.append("## 接近畢業（只報不動，人工審；cron 永不自動 promote）")
    if near_grad:
        for g in near_grad:
            L.append(f"- `{g['name']}` closed={g['closed']} live_excess={g['live_avg_excess']} "
                     f"win%={g['win_rate_pct']} dsr={g['roll_dsr']} flags={','.join(g['flags'])}")
    else:
        L.append("- （無策略達接近畢業啟示門檻）")
    L.append("")
    L.append("---")
    L.append(f"_持久化：forward_cron_runs[{run_date}] @ monitor.db；唯讀查 GET /api/forward/cron/status_")
    try:
        path.write_text("\n".join(L), encoding="utf-8")
    except Exception as e:
        print(f"[forward-cron] 摘要寫檔失敗: {e}", file=sys.stderr)
    return path


# ══════════════════════════════════════════════════════════════════════════════
# 主編排
# ══════════════════════════════════════════════════════════════════════════════
def run_cron(opts):
    """執行一次每日 cron。回 result dict（含 status / steps / 落地路徑）。
    opts: dict（見 build_opts）。純函式風格：所有外部依賴（base_url/token/db_path/state_dir）由 opts 注入，
    方便隔離測（mock server + temp db）。"""
    base = opts["base_url"]
    token = opts["token"]
    db_path = opts["db_path"]
    market = opts["market"]
    regime = opts["regime"]
    budget = min(max(int(opts["budget"]), 1), BUDGET_HARD_CAP)
    run_date = opts["run_date"]
    force = opts["force"]
    dry_run = opts["dry_run"]
    search_timeout = opts["search_timeout"]
    step_timeout = opts["step_timeout"]
    total_timeout = opts["total_timeout"]
    poll_interval = opts["search_poll"]
    state_dir = opts["state_dir"]

    t0 = time.monotonic()
    started_at = datetime.now().isoformat()
    _ensure_table(db_path)

    steps = []
    def add_step(step, status, detail="", **extra):
        s = {"step": step, "status": status, "detail": detail}
        s.update(extra)
        steps.append(s)
        return s

    rec = {"run_date": run_date, "started_at": started_at, "completed_at": None,
           "status": "running", "market": market, "search_run_id": None,
           "rolled": 0, "rejected_collinear": 0, "rejected_gate": 0, "replaced": 0,
           "picks_total": 0, "settled": 0, "near_graduation_json": None,
           "error": None, "summary_json": None}

    # ── 今日已跑守門（spec §5）──────────────────────────────────────────────
    existing = _get_run(db_path, run_date)
    if existing and existing.get("status") == "done" and not force:
        add_step("guard", "skipped", f"{run_date} 已 done → skip（--force 可覆寫）")
        rec["status"] = "skipped"
        rec["completed_at"] = datetime.now().isoformat()
        # 不覆寫已 done 的歷史紀錄；僅回報 skipped
        near = json.loads(existing["near_graduation_json"]) if existing.get("near_graduation_json") else []
        md = _write_summary_md(state_dir, run_date, rec, steps, near, dry_run)
        return {"status": "skipped", "run_date": run_date, "steps": steps,
                "summary_path": str(md), "record": existing, "skipped": True}

    rec["status"] = "running"
    _persist(db_path, rec)

    errors = []

    def over_budget_time():
        return (time.monotonic() - t0) > total_timeout

    # ── 跑前 prod 存活檢查（spec §5）────────────────────────────────────────
    try:
        h = _get(base, "/api/health", token, timeout=min(step_timeout, 30))
        if not (isinstance(h, dict) and h.get("ok")):
            raise HttpError("/api/health ok!=true", status=200, body=str(h)[:200])
        add_step("health", "done", "ok=true")
    except HttpError as e:
        add_step("health", "error", str(e))
        rec["status"] = "error"; rec["error"] = f"health 檢查失敗，未開始：{e}"
        rec["completed_at"] = datetime.now().isoformat()
        _persist(db_path, rec)
        md = _write_summary_md(state_dir, run_date, rec, steps, [], dry_run)
        return {"status": "error", "run_date": run_date, "steps": steps,
                "summary_path": str(md), "record": rec}

    # ── STEP 1：daemon 搜新組合 ────────────────────────────────────────────
    if dry_run:
        add_step("1_search", "skipped", "dry-run：跳過搜尋（避免寫 composite_candidates）")
    elif over_budget_time():
        add_step("1_search", "timeout", "總 timeout 已到，跳過搜尋")
        errors.append("total_timeout_before_search")
    else:
        try:
            body = {"market": market, "regime": regime,
                    "start": "2020-01-01", "end": run_date,
                    "benchmark": "equal_weight", "budget": budget,
                    # base-rate/composite 評估帶賣出策略，避免出場污染（spec §1 鐵律#5）
                    "sell_strategies": ["EXIT_C", "EXIT_D"]}
            r = _post(base, "/api/search/auto/start", token, body=body, timeout=step_timeout)
            run_id = r.get("run_id")
            rec["search_run_id"] = run_id
            if not run_id:
                add_step("1_search", "error", f"start 未回 run_id：{str(r)[:200]}")
                errors.append("search_no_run_id")
            else:
                # poll 到終態或 step timeout
                deadline = time.monotonic() + search_timeout
                final = None
                while True:
                    st = _get(base, "/api/search/auto/status", token,
                              params={"run_id": run_id}, timeout=min(step_timeout, 60))
                    run_view = st.get("run") or st
                    sstatus = (run_view or {}).get("status")
                    if sstatus in ("done", "error", "stopped"):
                        final = sstatus; break
                    if time.monotonic() > deadline or over_budget_time():
                        final = "timeout"; break
                    time.sleep(poll_interval)
                if final == "done":
                    add_step("1_search", "done", f"run_id={run_id} status=done")
                elif final == "timeout":
                    add_step("1_search", "partial",
                             f"run_id={run_id} 逾時（已寫入 candidates 仍可滾）")
                    errors.append("search_timeout")
                else:
                    add_step("1_search", "partial", f"run_id={run_id} status={final}（續往下滾既有候選）")
                    if final == "error":
                        errors.append("search_error")
        except HttpError as e:
            add_step("1_search", "error", str(e))
            errors.append(f"search:{e}")

    # ── STEP 2：過閘滾入 forward（enable_replace=false 寫死）──────────────────
    if over_budget_time():
        add_step("2_auto_roll", "timeout", "總 timeout 已到，跳過 auto-roll")
        errors.append("total_timeout_before_roll")
    else:
        try:
            body = {"market": market, "regime": regime,
                    "dry_run": bool(dry_run),       # 實滾=false；--dry-run 稽核=true
                    "enable_replace": ENABLE_REPLACE}   # ★寫死 False，body 不開覆寫
            r = _post(base, "/api/forward/auto-roll", token, body=body, timeout=step_timeout)
            summ = r.get("summary") or {}
            rec["rolled"] = summ.get("rolled", len(r.get("rolled") or []))
            rec["rejected_collinear"] = summ.get("rejected_collinear", len(r.get("rejected_collinear") or []))
            rec["rejected_gate"] = summ.get("rejected_gate", len(r.get("rejected_gate") or []))
            rec["replaced"] = summ.get("replaced", len(r.get("replaced") or []))
            add_step("2_auto_roll", "done",
                     f"rolled={rec['rolled']} rej_collinear={rec['rejected_collinear']} "
                     f"rej_gate={rec['rejected_gate']} replaced={rec['replaced']} "
                     f"(dry_run={dry_run}, enable_replace={ENABLE_REPLACE})",
                     audit={"rolled": r.get("rolled"), "rejected_collinear": r.get("rejected_collinear"),
                            "rejected_gate": r.get("rejected_gate"), "replaced": r.get("replaced")})
        except HttpError as e:
            add_step("2_auto_roll", "error", str(e))
            errors.append(f"auto_roll:{e}")

    # ── STEP 3：今日選股（date 為 query param）──────────────────────────────
    if dry_run:
        add_step("3_run_picks", "skipped", "dry-run：跳過選股（forward/run 無 dry 模式、會寫 ft_picks）")
    elif over_budget_time():
        add_step("3_run_picks", "timeout", "總 timeout 已到，跳過選股")
        errors.append("total_timeout_before_run")
    else:
        try:
            r = _request("POST",
                         base.rstrip("/") + "/api/forward/run?" + urllib.parse.urlencode({"date": run_date}),
                         token, body=None, timeout=step_timeout)
            results = r.get("results") or []
            picks_total = sum((x.get("n_picks") or 0) for x in results)
            rec["picks_total"] = picks_total
            n_skip = sum(1 for x in results if x.get("skipped"))
            add_step("3_run_picks", "done",
                     f"strategies={len(results)} picks_total={picks_total} skipped={n_skip}")
        except HttpError as e:
            add_step("3_run_picks", "error", str(e))
            errors.append(f"run:{e}")

    # ── STEP 4：結算/計分推進 ──────────────────────────────────────────────
    if dry_run:
        add_step("4_update", "skipped", "dry-run：跳過結算（forward/update 無 dry 模式、會寫 ft_picks）")
    elif over_budget_time():
        add_step("4_update", "timeout", "總 timeout 已到，跳過結算")
        errors.append("total_timeout_before_update")
    else:
        try:
            r = _post(base, "/api/forward/update", token, body=None, timeout=step_timeout)
            rec["settled"] = r.get("settled", 0)
            add_step("4_update", "done",
                     f"entered={r.get('entered')} marked={r.get('marked')} settled={r.get('settled')}")
        except HttpError as e:
            add_step("4_update", "error", str(e))
            errors.append(f"update:{e}")

    # ── STEP 5：畢業閘＝只報不動（唯讀彙整）─────────────────────────────────
    near_grad = []
    try:
        track = _get(base, "/api/forward/track", token, timeout=step_timeout)
        near_grad = _summarize_near_graduation(track)
        rec["near_graduation_json"] = json.dumps(near_grad, ensure_ascii=False)
        add_step("5_graduation_report", "done",
                 f"near_graduation={len(near_grad)}（只報不動，人工審）")
    except HttpError as e:
        add_step("5_graduation_report", "error", str(e))
        errors.append(f"track:{e}")

    # ── 收尾：狀態判定（誠實 > 好看）─────────────────────────────────────────
    step_statuses = [s["status"] for s in steps]
    if errors and any(st == "error" for st in step_statuses):
        # 有硬 error → 看是否全毀或部分成功
        if all(st in ("error", "timeout") for st in step_statuses if st not in ("done", "skipped")):
            rec["status"] = "error" if not any(st == "done" for st in step_statuses) else "partial"
        else:
            rec["status"] = "partial"
    elif any(st in ("partial", "timeout") for st in step_statuses):
        rec["status"] = "partial"
    elif dry_run:
        rec["status"] = "dryrun"          # 稽核模式：不算 done，今日守門不擋真跑
    else:
        rec["status"] = "done"

    if errors:
        rec["error"] = " | ".join(errors)[:1000]
    rec["completed_at"] = datetime.now().isoformat()
    rec["summary_json"] = json.dumps({"steps": steps}, ensure_ascii=False)

    # dry-run 不覆寫已存在的真跑紀錄
    if dry_run and existing and existing.get("status") == "done":
        pass  # 保留 done 歷史，dry-run 只寫摘要 md
    else:
        _persist(db_path, rec)

    md = _write_summary_md(state_dir, run_date, rec, steps, near_grad, dry_run)
    return {"status": rec["status"], "run_date": run_date, "steps": steps,
            "summary_path": str(md), "record": rec, "near_graduation": near_grad,
            "errors": errors}


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════
def build_opts(args):
    token = _read_token(args.token_file)          # 從檔讀；絕不上 argv
    state_dir = _resolve_state_dir(args.state_dir)
    run_date = args.run_date or datetime.now().strftime("%Y-%m-%d")
    # 驗 run_date 格式
    datetime.strptime(run_date, "%Y-%m-%d")
    market = (args.market or DEFAULT_MARKET).upper()
    if market != "TW" and not args.allow_non_tw:
        raise SystemExit(f"market={market} 非 TW：v1 僅 TW（spec §2）。要跑須顯式 --allow-non-tw（US 須先 US-SI 真回補+F1 驗）。")
    return {
        "base_url": args.base_url,
        "token": token,
        "db_path": args.db_path,
        "market": market,
        "regime": (args.regime or DEFAULT_REGIME).upper(),
        "budget": args.budget,
        "run_date": run_date,
        "force": args.force,
        "dry_run": args.dry_run,
        "search_timeout": args.search_timeout,
        "step_timeout": args.step_timeout,
        "total_timeout": args.total_timeout,
        "search_poll": args.search_poll,
        "state_dir": state_dir,
    }


def main(argv=None):
    p = argparse.ArgumentParser(
        description="半自動 Forward Daily Cron（standalone 編排；不 import backend、不碰真錢）")
    p.add_argument("--base-url", default=DEFAULT_BASE_URL, help=f"backend base URL（預設 {DEFAULT_BASE_URL}）")
    p.add_argument("--token-file", default=str(DEFAULT_TOKEN_FILE),
                   help="API token 檔（預設 ui/.api_token；token 永不上命令列）")
    p.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="monitor.db 路徑（run-log 落地）")
    p.add_argument("--state-dir", default="", help="摘要 md 寫檔目錄（預設 n8n-claude/state）")
    p.add_argument("--market", default=DEFAULT_MARKET, help="目標市場（預設 TW；v1 僅 TW）")
    p.add_argument("--allow-non-tw", action="store_true", help="顯式允許非 TW market（防呆，預設禁）")
    p.add_argument("--regime", default=DEFAULT_REGIME, help="目標 regime 切片（預設 TREND_UP）")
    p.add_argument("--budget", type=int, default=DEFAULT_BUDGET,
                   help=f"搜尋 budget（有界，預設 {DEFAULT_BUDGET}，硬上限 {BUDGET_HARD_CAP}）")
    p.add_argument("--run-date", default="", help="run 日期 YYYY-MM-DD（預設今天）")
    p.add_argument("--force", action="store_true", help="覆寫『今日已 done』守門，強制重跑")
    p.add_argument("--dry-run", action="store_true",
                   help="稽核模式：auto-roll dry_run:true、search/run/update 跳過（不寫資料、不計 done）")
    p.add_argument("--search-timeout", type=int, default=DEFAULT_SEARCH_TIMEOUT, help="搜尋步 poll 上限秒")
    p.add_argument("--step-timeout", type=int, default=DEFAULT_STEP_TIMEOUT, help="單次 HTTP 上限秒")
    p.add_argument("--total-timeout", type=int, default=DEFAULT_TOTAL_TIMEOUT, help="總 run 硬上限秒")
    p.add_argument("--search-poll", type=int, default=DEFAULT_SEARCH_POLL, help="搜尋 status poll 間隔秒")
    args = p.parse_args(argv)

    opts = build_opts(args)
    print(f"[forward-cron] start run_date={opts['run_date']} market={opts['market']} "
          f"dry_run={opts['dry_run']} base={opts['base_url']}")
    result = run_cron(opts)
    print(f"[forward-cron] done status={result['status']} summary={result['summary_path']}")
    if result.get("errors"):
        print(f"[forward-cron] errors: {result['errors']}", file=sys.stderr)
    # exit code：done/skipped/dryrun=0；partial=1；error=2（給 Task Scheduler 判讀）
    code = {"done": 0, "skipped": 0, "dryrun": 0, "partial": 1, "error": 2}.get(result["status"], 1)
    return code


if __name__ == "__main__":
    sys.exit(main())
