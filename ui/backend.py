"""
智慧投顧監控系統 — FastAPI 後端 v2.0
端口：8765

設定方式（任一）：
  1. 環境變數：set SJ_API_KEY=xxx  SJ_SEC_KEY=yyy
  2. 同目錄建立 .env 檔：SJ_API_KEY=xxx\nSJ_SEC_KEY=yyy
  3. 指向 ai-investment-system 的 config.yaml（見 CONFIG_YAML_PATH）

安裝：pip install fastapi uvicorn shioaji pandas numpy python-dotenv pyyaml
啟動：python backend.py
"""
import asyncio
import math
import os
import sqlite3
import json
import threading
import time
import smtplib
import urllib.request
import urllib.parse
import urllib.error
import ssl
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional, List

import pandas as pd
import uvicorn
import secrets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, Header, UploadFile, File, Form, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel

# ── 讀取憑證（依優先順序）─────────────────────────
def _load_credentials():
    key = os.environ.get("SJ_API_KEY", "")
    sec = os.environ.get("SJ_SEC_KEY", "")
    if key and sec:
        return key, sec

    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        from dotenv import load_dotenv
        load_dotenv(env_path)
        key = os.environ.get("SJ_API_KEY", "")
        sec = os.environ.get("SJ_SEC_KEY", "")
        if key and sec:
            return key, sec

    config_yaml = Path.home() / "ai-investment-system" / "config.yaml"
    if config_yaml.exists():
        import yaml
        with open(config_yaml, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        api_keys = cfg.get("api_keys", {})
        key = api_keys.get("shioaji_api_key", "")
        sec = api_keys.get("shioaji_secret_key", "")
        if key and sec:
            return key, sec

    raise RuntimeError(
        "找不到 Shioaji API Key！\n"
        "請在 ui/.env 建立：\nSJ_API_KEY=your_key\nSJ_SEC_KEY=your_secret"
    )

API_KEY, SECRET_KEY = _load_credentials()
SIMULATION = os.environ.get("SJ_PRODUCTION", "").lower() not in ("1", "true")

# ── DB 路徑 ────────────────────────────────────────
DB_PATH = Path(__file__).parent / "monitor.db"

# ── 本機 API Token（防止惡意網頁 CSRF）───────────────
_TOKEN_FILE = Path(__file__).parent / ".api_token"

def _load_or_create_token() -> str:
    """沿用既有 token；不存在才產生新的"""
    try:
        existing = _TOKEN_FILE.read_text(encoding="utf-8").strip()
        if len(existing) >= 32:
            return existing
    except Exception:
        pass
    tok = secrets.token_hex(32)
    try:
        _TOKEN_FILE.write_text(tok, encoding="utf-8")
    except Exception:
        pass
    return tok

_API_TOKEN: str = _load_or_create_token()   # 模組載入時即產生，確保全域一致

def require_token(x_api_token: str = Header(None, alias="X-API-Token")):
    """FastAPI 依賴：驗證 X-API-Token，用於高危端點（自動停損、風控設定等）"""
    if x_api_token != _API_TOKEN:
        raise HTTPException(status_code=403, detail="缺少或無效的 API Token（X-API-Token）")

# ── App ───────────────────────────────────────────
_LOCALHOST_ORIGINS = [
    "http://localhost:8765",
    "http://127.0.0.1:8765",
    "https://hanky-doorway-constable.ngrok-free.dev",
]

app = FastAPI(title="智慧投顧 API v2")
app.add_middleware(
    CORSMiddleware,
    allow_origins=_LOCALHOST_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── 前端入口 & Token 端點 ──────────────────────────
@app.get("/", include_in_schema=False)
def serve_index():
    """提供 index.html（讓前端 origin 固定為 http://localhost:8765）"""
    p = Path(__file__).parent / "index.html"
    if p.exists():
        return FileResponse(str(p), media_type="text/html")
    return JSONResponse({"error": "index.html not found"}, status_code=404)

@app.get("/manual.html", include_in_schema=False)
def serve_manual():
    p = Path(__file__).parent / "manual.html"
    if p.exists():
        return FileResponse(str(p), media_type="text/html")
    return JSONResponse({"error": "manual.html not found"}, status_code=404)

@app.get("/doc/{fname}", include_in_schema=False)
def serve_doc(fname: str):
    """服務 ui/ 下的 .html 報告文件（給手機/ngrok 閱讀）。路徑安全：僅 basename、僅 .html。"""
    if (not fname.endswith(".html")) or ("/" in fname) or ("\\" in fname) or (".." in fname):
        return JSONResponse({"error": "invalid"}, status_code=400)
    p = Path(__file__).parent / fname
    if p.is_file():
        return FileResponse(str(p), media_type="text/html")
    return JSONResponse({"error": "not found"}, status_code=404)

@app.get("/api/auth/token")
def get_api_token():
    """回傳本次啟動的 API Token（僅限 localhost 存取，用於前端高危操作）"""
    return {"token": _API_TOKEN}

# ── SQLite 初始化 ─────────────────────────────────

def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL")
    cur = con.cursor()

    # K-bar 日線快取（15:00 後更新一次）
    cur.execute("""
        CREATE TABLE IF NOT EXISTS kbar_cache (
            code        TEXT NOT NULL,
            tf          TEXT NOT NULL,   -- D / 60 / 5
            date_key    TEXT NOT NULL,   -- YYYY-MM-DD（日K）或 YYYY-MM-DD HH:MM（分K）
            open        REAL,
            high        REAL,
            low         REAL,
            close       REAL,
            volume      INTEGER,
            updated_at  TEXT,
            PRIMARY KEY (code, tf, date_key)
        )
    """)

    # 持倉表（含波段/當沖區分 G3）
    cur.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            code         TEXT NOT NULL,
            name         TEXT,
            trade_type   TEXT DEFAULT '波段',  -- 波段 / 當沖
            shares       INTEGER DEFAULT 0,
            cost         REAL DEFAULT 0,
            stop_loss    REAL DEFAULT 0,       -- 個別停損價（G4）
            target_price REAL DEFAULT 0,       -- 法人目標價（G6）
            highest_price REAL DEFAULT 0,      -- 移動止盈追蹤最高價
            entry_date   TEXT,
            signal_type  TEXT,                 -- 進場訊號
            note         TEXT,
            status       TEXT DEFAULT 'open',  -- open / closed
            updated_at   TEXT
        )
    """)
    try:
        cur.execute("ALTER TABLE positions ADD COLUMN status TEXT DEFAULT 'open'")
    except Exception:
        pass
    # 既有 DB 遷移：加 highest_price 欄位
    try:
        cur.execute("ALTER TABLE positions ADD COLUMN highest_price REAL DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    # 既有 DB 遷移：加 sell_order_id（記錄券商訂單 ID，用於 fill 確認）
    try:
        cur.execute("ALTER TABLE positions ADD COLUMN sell_order_id TEXT DEFAULT NULL")
    except sqlite3.OperationalError:
        pass
    # 既有 DB 遷移：加 sell_reason（記錄停損原因，防重複下單）
    try:
        cur.execute("ALTER TABLE positions ADD COLUMN sell_reason TEXT DEFAULT NULL")
    except sqlite3.OperationalError:
        pass
    # P7: lifecycle_stage + exit_conditions
    try:
        cur.execute("ALTER TABLE positions ADD COLUMN lifecycle_stage TEXT DEFAULT 'holding'")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE positions ADD COLUMN exit_conditions TEXT DEFAULT NULL")
    except sqlite3.OperationalError:
        pass

    # 訊號記錄
    cur.execute("""
        CREATE TABLE IF NOT EXISTS signal_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            code        TEXT,
            signal_type TEXT,
            direction   TEXT,  -- BUY / SELL / WARN
            price       REAL,
            detail      TEXT,
            created_at  TEXT DEFAULT (datetime('now','localtime'))
        )
    """)

    # 風控設定
    cur.execute("""
        CREATE TABLE IF NOT EXISTS risk_config (
            key         TEXT PRIMARY KEY,
            value       TEXT,
            updated_at  TEXT
        )
    """)
    # 預設風控值
    defaults = [
        ("stop_loss_wave",   "7"),    # 波段停損 %（G4）
        ("stop_loss_day",    "3"),    # 當沖停損 %
        ("exit_d_threshold", "5"),    # EXIT_D 緊急停損 %
        ("macro_lock",       "0"),    # G8: 1=全面鎖單
        ("max_single_stock", "20"),   # 單檔持倉上限 %
        ("max_positions",    "5"),    # 同時持倉數上限
        ("line_channel_token", ""),   # LINE (deprecated, kept for migration)
        ("line_user_id",       ""),   # LINE (deprecated)
        ("telegram_bot_token", ""),   # Telegram Bot Token (@BotFather)
        ("telegram_chat_id",   ""),   # Telegram Chat ID (comma-separated for multi-subscriber)
        ("telegram_chat_names",""),   # Telegram 訂閱者暱稱 (comma-separated, matches chat_id order)
        ("email_smtp_host",    ""),   # SMTP server (e.g. smtp.gmail.com)
        ("email_smtp_port",    "587"),# SMTP port
        ("email_user",         ""),   # SMTP 帳號
        ("email_pass",         ""),   # SMTP 密碼 / App Password
        ("email_to",           ""),   # 收件人
        ("webhook_url",        ""),   # 通用 Webhook URL (POST JSON)
        ("notify_enabled",     "1"),  # 1=啟用推播
        ("risk_level",         "NORMAL"),  # NORMAL / CAUTION / ALERT
        ("position_scale",     "100"),     # 部位縮放 %
        ("auto_sell_enabled",       "0"),  # Phase 5: 1=啟用 EXIT_D 自動停損賣出（預設關閉）
        ("auto_sell_exitc_enabled", "0"),  # Phase 5b: 1=啟用 EXIT_C 自動移動止盈（預設關閉）
        ("chip_auto_fetch_enabled", "1"),  # 1=啟用收盤後自動抓取法人籌碼（預設開啟）
        ("ic_notify_enabled",       "1"),  # 1=AI建議完成後推播高信心度 BUY 訊號
        ("ic_notify_threshold",  "0.70"),  # 推播最低信心度門檻（0~1）
    ]
    for k, v in defaults:
        cur.execute(
            "INSERT OR IGNORE INTO risk_config(key,value,updated_at) VALUES(?,?,?)",
            (k, v, datetime.now().isoformat())
        )

    # 每日籌碼快照（Phase 3）
    cur.execute("""
        CREATE TABLE IF NOT EXISTS chip_snapshot (
            code                TEXT NOT NULL,
            date                TEXT NOT NULL,
            foreign_buy         INTEGER DEFAULT 0,  -- 外資買賣超（張）
            itrust_buy          INTEGER DEFAULT 0,  -- 投信買賣超（張）
            dealer_buy          INTEGER DEFAULT 0,  -- 自營商買賣超（張）
            itrust_hold_ratio   REAL DEFAULT 0,     -- 投信持股比例
            margin_buy          INTEGER DEFAULT 0,  -- 融資買進
            margin_sell         INTEGER DEFAULT 0,  -- 融資賣出
            margin_balance      INTEGER DEFAULT 0,  -- 融資餘額
            short_buy           INTEGER DEFAULT 0,  -- 融券買進
            short_sell          INTEGER DEFAULT 0,  -- 融券賣出
            short_balance       INTEGER DEFAULT 0,  -- 融券餘額
            margin_short_ratio  REAL DEFAULT 0,     -- 券資比
            forced_buyback_date TEXT,                -- 融券強制回補日
            PRIMARY KEY (code, date)
        )
    """)
    # TW-SBL：借券賣出（SBL, 外資/機構 informed short）3 欄——容錯 ALTER，鏡像 watchlist :308 模式，
    # 既有列預設 0（零回歸：舊路徑 INSERT/SELECT 皆顯式列名，新欄不影響）。單位＝股（TWT93U hints=單位:股，
    # 與融資券 MI_MARGN 的「張」不同→ turnover=餘額÷ADV 為橫斷面 rank、單位均勻縮放於排名抵消）。
    for _sbl_col in ("sbl_short_balance", "sbl_short_sell", "sbl_return"):
        try:
            cur.execute(f"ALTER TABLE chip_snapshot ADD COLUMN {_sbl_col} INTEGER DEFAULT 0")
        except Exception:
            pass

    # 自選股清單
    cur.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            code        TEXT PRIMARY KEY,
            name        TEXT,
            market      TEXT DEFAULT 'TW',  -- TW / US
            sort_order  INTEGER DEFAULT 0,
            added_at    TEXT DEFAULT (datetime('now','localtime'))
        )
    """)
    try:
        cur.execute("ALTER TABLE watchlist ADD COLUMN market TEXT DEFAULT 'TW'")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE positions ADD COLUMN market TEXT DEFAULT 'TW'")
    except Exception:
        pass
    # Phase 8: 當沖比快照
    cur.execute("""
        CREATE TABLE IF NOT EXISTS daytrade_snapshot (
            code        TEXT NOT NULL,
            date        TEXT NOT NULL,
            daytrade_vol INTEGER DEFAULT 0,
            total_vol    INTEGER DEFAULT 0,
            daytrade_ratio REAL DEFAULT 0,
            PRIMARY KEY (code, date)
        )
    """)

    # Phase 8: 新聞/重大訊息快取
    cur.execute("""
        CREATE TABLE IF NOT EXISTS news_cache (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            code        TEXT NOT NULL,
            date        TEXT NOT NULL,
            headline    TEXT,
            sentiment   TEXT DEFAULT 'neutral',
            source      TEXT,
            fetched_at  TEXT
        )
    """)

    # 策略設定持久化
    cur.execute("""
        CREATE TABLE IF NOT EXISTS strategy_config (
            strategy_id TEXT PRIMARY KEY,
            enabled     INTEGER DEFAULT 1,
            params      TEXT DEFAULT '{}',
            updated_at  TEXT
        )
    """)

    # 交易紀錄表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trade_records (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            code         TEXT NOT NULL,
            name         TEXT,
            market       TEXT DEFAULT 'TW',
            action       TEXT NOT NULL,       -- BUY / SELL
            shares       INTEGER NOT NULL,
            price        REAL NOT NULL,
            trade_date   TEXT NOT NULL,
            commission_rate REAL DEFAULT 0.001425,
            commission_discount REAL DEFAULT 0.6,
            tax_rate     REAL DEFAULT 0.003,
            commission   REAL DEFAULT 0,
            tax          REAL DEFAULT 0,
            total_cost   REAL DEFAULT 0,
            net_amount   REAL DEFAULT 0,      -- 實收/實付金額
            position_id  INTEGER,             -- 關聯持倉
            note         TEXT,
            created_at   TEXT DEFAULT (datetime('now','localtime'))
        )
    """)

    # 預設自選股
    defaults_wl = [("2330","台積電"),("2317","鴻海"),("2454","聯發科"),("2382","廣達")]
    for code, name in defaults_wl:
        cur.execute(
            "INSERT OR IGNORE INTO watchlist(code,name) VALUES(?,?)", (code, name)
        )

    con.commit()
    con.close()

init_db()

# ── Shioaji 單例 ──────────────────────────────────
_api = None

def _on_order_fill(stat):
    """Shioaji 訂單狀態回調：成交後將 pending_sell 持倉標記為 closed"""
    try:
        status_str = str(getattr(stat, "status", "")).lower()
        if "filled" not in status_str:
            return
        # 優先用 trade id，fallback 到 ordno
        order_id = (getattr(stat, "id", None)
                    or getattr(stat, "ordno", None)
                    or getattr(stat, "seqno", None))
        if not order_id:
            return
        con = db()
        cur = con.cursor()
        cur.execute(
            "SELECT id, code FROM positions WHERE sell_order_id=? AND status='pending_sell'",
            (str(order_id),)
        )
        row = cur.fetchone()
        if not row:
            con.close()
            return
        pos_id, code = row
        cur.execute(
            "UPDATE positions SET status='closed', updated_at=? WHERE id=?",
            (datetime.now().isoformat(), pos_id)
        )
        con.commit()
        con.close()
        threading.Thread(
            target=_send_notification,
            args=(f"✅ 停損成交確認 [{code}]\n訂單 {order_id} 已成交，持倉已關閉",),
            daemon=True,
        ).start()
    except Exception as e:
        print(f"[order_fill callback error] {e}", flush=True)


_api_lock = threading.Lock()

def get_api():
    global _api
    if _api is not None:
        return _api
    with _api_lock:
        if _api is not None:
            return _api
        import shioaji as sj
        _api = sj.Shioaji(simulation=SIMULATION)
        _api.login(api_key=API_KEY, secret_key=SECRET_KEY, contracts_timeout=10000)
        _api.set_on_tick_stk_v1_callback(_ws_route_tick)
        _api.set_on_bidask_stk_v1_callback(_ws_route_bidask)
        if not SIMULATION:
            try:
                _api.set_order_status_callback(_on_order_fill)
            except Exception:
                pass
    return _api

# ── WebSocket subscriber registry ─────────────────
# key: stock code, value: list of (loop, queue) for active WS connections
_ws_subs: dict = {}
_ws_subs_lock = threading.Lock()

# ── VWAP 即時累積狀態 ────────────────────────────
# key: stock code, value: {"cum_pv": float, "cum_vol": int, "date": str}
_vwap_state: dict = {}
_vwap_lock = threading.Lock()

# ── Phase 4: Tick-level 追蹤（外盤連續、特大單、假跌破時間）──
# key: stock code, value: dict with tracking fields
_tick_buf: dict = {}
_tick_buf_lock = threading.Lock()
# key: stock code, value: datetime when price breached below MA5
_breach_times: dict = {}
_breach_times_lock = threading.Lock()
_ic_refresh_lock = threading.Lock()
# key: stock code, value: datetime when price first dropped below VWAP
_vwap_breach_times: dict = {}
_vwap_breach_times_lock = threading.Lock()
VWAP_FAIL_MINUTES = 3

def _ws_route_tick(tick):
    code = str(tick.code)
    price = float(tick.close)
    vol = int(tick.volume)

    # VWAP 累積（每日重置）
    today = datetime.now().strftime("%Y-%m-%d")
    with _vwap_lock:
        st = _vwap_state.get(code)
        if st is None or st["date"] != today:
            st = {"cum_pv": 0.0, "cum_vol": 0, "date": today}
            _vwap_state[code] = st
        st["cum_pv"] += price * vol
        st["cum_vol"] += vol
        vwap = st["cum_pv"] / st["cum_vol"] if st["cum_vol"] > 0 else price

    # Phase 4: tick-level 追蹤
    tick_type = int(tick.tick_type)
    with _tick_buf_lock:
        buf = _tick_buf.get(code)
        if buf is None or buf["date"] != today:
            buf = {"date": today, "outside_bid_count": 0, "large_order_count": 0,
                   "large_sell_count": 0, "tick_count": 0, "total_vol": 0}
            _tick_buf[code] = buf
        buf["tick_count"] += 1
        buf["total_vol"] += vol
        if tick_type == 1:  # 外盤（主動買）
            buf["outside_bid_count"] += 1
        else:
            buf["outside_bid_count"] = 0  # 連續中斷歸零
        if vol >= 100:  # 特大單 ≥100張
            buf["large_order_count"] += 1
            if tick_type == 2:  # 內盤大單（砸盤）
                buf["large_sell_count"] += 1

    # 更新持倉 highest_price（非阻塞，用 try 防止 DB 鎖）
    try:
        con = sqlite3.connect(DB_PATH, timeout=1)
        con.execute(
            "UPDATE positions SET highest_price = ? WHERE code = ? AND highest_price < ?",
            (price, code, price)
        )
        con.commit()
        con.close()
    except Exception:
        pass

    with _ws_subs_lock:
        entries = list(_ws_subs.get(code, []))
    payload = {
        "type":      "tick",
        "code":      code,
        "price":     price,
        "volume":    vol,
        "tick_type": int(tick.tick_type),
        "vwap":      round(vwap, 2),
        "ts":        str(tick.datetime),
    }
    for loop, q in entries:
        loop.call_soon_threadsafe(q.put_nowait, payload)

def _ws_route_bidask(bidask):
    code = str(bidask.code)
    with _ws_subs_lock:
        entries = list(_ws_subs.get(code, []))
    payload = {
        "type":       "bidask",
        "code":       code,
        "bid_price":  [float(p) for p in bidask.bid_price],
        "bid_volume": list(bidask.bid_volume),
        "ask_price":  [float(p) for p in bidask.ask_price],
        "ask_volume": list(bidask.ask_volume),
    }
    for loop, q in entries:
        loop.call_soon_threadsafe(q.put_nowait, payload)

# ── 工具函式 ──────────────────────────────────────

def calc_ma(closes: list, period: int):
    s = pd.Series(closes)
    return s.rolling(period).mean().tolist()

def calc_macd(closes: list, fast=12, slow=26, signal=9):
    s = pd.Series(closes)
    ema_f = s.ewm(span=fast).mean()
    ema_s = s.ewm(span=slow).mean()
    dif   = ema_f - ema_s
    macd  = dif.ewm(span=signal).mean()
    hist  = dif - macd
    return dif.tolist(), macd.tolist(), hist.tolist()

def safe(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    return round(float(v), 4)

def db():
    return sqlite3.connect(DB_PATH)

# ── Rate Limiter ──────────────────────────────────

class _TokenBucket:
    """Thread-safe token bucket. capacity tokens refilled every window_sec."""
    def __init__(self, capacity: int, window_sec: float):
        self._capacity   = capacity
        self._window_sec = window_sec
        self._tokens     = float(capacity)
        self._last       = time.monotonic()
        self._lock       = threading.Lock()

    def consume(self, n: int = 1) -> float:
        """Consume n tokens. Returns seconds to wait (0 if ok)."""
        with self._lock:
            now  = time.monotonic()
            refill = (now - self._last) / self._window_sec * self._capacity
            self._tokens = min(self._capacity, self._tokens + refill)
            self._last   = now
            if self._tokens >= n:
                self._tokens -= n
                return 0.0
            # calculate wait time for n tokens to be available
            needed = n - self._tokens
            return needed / self._capacity * self._window_sec

# 行情查詢：5秒 50次總量；Ticks 子桶：5秒 10次
_rl_data  = _TokenBucket(capacity=50, window_sec=5.0)
_rl_ticks = _TokenBucket(capacity=10, window_sec=5.0)

def _api_call_with_backoff(fn, *args, is_ticks: bool = False, **kwargs):
    """
    Call fn(*args, **kwargs) respecting rate limits.
    Blocks until tokens are available; retries on exception with exponential backoff.
    """
    max_retries = 5
    for attempt in range(max_retries):
        # acquire token — sub-bucket first (ticks), then global
        if is_ticks:
            wait = _rl_ticks.consume(1)
            if wait > 0:
                time.sleep(wait)
        wait = _rl_data.consume(1)
        if wait > 0:
            time.sleep(wait)

        try:
            return fn(*args, **kwargs)
        except Exception as e:
            msg = str(e).lower()
            rate_limited = any(k in msg for k in ("rate limit", "too many", "exceed", "throttl", "quota"))
            if not rate_limited:
                raise  # non-rate-limit errors propagate immediately
            backoff = min(2 ** attempt * 0.5, 30.0)
            print(f"[rate-limit] attempt {attempt+1} failed ({e}), retry in {backoff:.1f}s")
            time.sleep(backoff)
    raise RuntimeError("API call failed after max retries")

# ── K-bar 快取 ────────────────────────────────────

def _cache_key_fresh(code: str, tf: str) -> bool:
    """日K：15:00 後到隔日 15:00 前算新鮮；分K：5 分鐘有效"""
    con = db()
    cur = con.cursor()
    now = datetime.now()
    if tf == "D":
        today = now.strftime("%Y-%m-%d")
        cutoff = now.replace(hour=15, minute=0, second=0, microsecond=0)
        cur.execute(
            "SELECT updated_at FROM kbar_cache WHERE code=? AND tf='D' ORDER BY date_key DESC LIMIT 1",
            (code,)
        )
        row = cur.fetchone()
        con.close()
        if not row:
            return False
        upd = datetime.fromisoformat(row[0])
        return upd >= cutoff if now >= cutoff else (now - upd).total_seconds() < 86400
    else:
        # 分K 5分鐘快取
        cur.execute(
            "SELECT updated_at FROM kbar_cache WHERE code=? AND tf=? ORDER BY date_key DESC LIMIT 1",
            (code, tf)
        )
        row = cur.fetchone()
        con.close()
        if not row:
            return False
        upd = datetime.fromisoformat(row[0])
        return (now - upd).total_seconds() < 300

def _save_kbars(code: str, tf: str, df: pd.DataFrame):
    if "ts" not in df.columns:
        df = df.reset_index()
    con = db()
    cur = con.cursor()
    now_iso = datetime.now().isoformat()
    for _, row in df.iterrows():
        ts = pd.to_datetime(row["ts"])
        key = ts.strftime("%Y-%m-%d") if tf == "D" else ts.strftime("%Y-%m-%d %H:%M")
        cur.execute("""
            INSERT OR REPLACE INTO kbar_cache(code,tf,date_key,open,high,low,close,volume,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?)
        """, (code, tf, key,
              safe(row["Open"]), safe(row["High"]), safe(row["Low"]), safe(row["Close"]),
              int(row["Volume"]), now_iso))
    con.commit()
    con.close()

def _load_kbars_from_cache(code: str, tf: str) -> list:
    con = db()
    cur = con.cursor()
    cur.execute(
        "SELECT date_key,open,high,low,close,volume FROM kbar_cache WHERE code=? AND tf=? ORDER BY date_key",
        (code, tf)
    )
    rows = cur.fetchall()
    con.close()
    result = []
    for r in rows:
        try:
            ts = datetime.fromisoformat(r[0]) if " " in r[0] else datetime.strptime(r[0], "%Y-%m-%d")
            result.append({
                "time": int(ts.timestamp()),
                "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5] or 0
            })
        except Exception:
            pass
    return result

def _fetch_kbars_from_api(code: str, tf: str) -> pd.DataFrame:
    api = get_api()
    contract = api.Contracts.Stocks.get(code)
    if contract is None:
        return pd.DataFrame()

    today = datetime.now().strftime("%Y-%m-%d")
    # Shioaji 1.5 kbars() always returns 1-min bars; resample to target timeframe
    if tf == "D":
        start = (datetime.now() - timedelta(days=1100)).strftime("%Y-%m-%d")
    elif tf == "60":
        start = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
    else:  # 5-min
        start = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")

    bars = _api_call_with_backoff(api.kbars, contract, start=start, end=today)
    df = pd.DataFrame({**bars})
    if df.empty:
        return df

    first_ts = pd.to_numeric(df["ts"].iloc[0], errors="coerce")
    if pd.isna(first_ts):
        return pd.DataFrame()
    # Shioaji 時間為台灣 UTC+8，明確 localize 避免系統時區影響 Unix timestamp 轉換
    tz_tw = "Asia/Taipei"
    df["ts"] = (
        pd.to_datetime(df["ts"], unit="ns" if first_ts > 1e15 else "s")
        .dt.tz_localize(tz_tw)
    )
    df = df.set_index("ts").sort_index()

    if tf == "D":
        rule = "D"
    elif tf == "60":
        rule = "60min"
    else:
        rule = "5min"

    if tf != "D":
        # 只保留交易時段 09:00–13:30
        df = df.between_time("09:00", "13:30")

    agg = df.resample(rule, closed="left", label="left").agg(
        Open=("Open", "first"),
        High=("High", "max"),
        Low=("Low", "min"),
        Close=("Close", "last"),
        Volume=("Volume", "sum"),
    ).dropna(subset=["Open"])
    agg = agg.reset_index().rename(columns={"ts": "ts"})
    return agg

def _build_kbar_response(code: str, tf: str, candles: list) -> dict:
    closes = [c["close"] for c in candles]
    times  = [c["time"]  for c in candles]
    vols   = [c["volume"] for c in candles]

    def ma_pts(period):
        vals = calc_ma(closes, period)
        return [{"time": t, "value": safe(v)}
                for t, v in zip(times, vals) if v is not None and not math.isnan(v)]

    dif, macd_line, hist = calc_macd(closes)

    markers = []
    for i in range(1, len(dif)):
        d_prev, d_cur = dif[i-1], dif[i]
        m_prev, m_cur = macd_line[i-1], macd_line[i]
        if d_prev < m_prev and d_cur >= m_cur:
            markers.append({"time": times[i], "position": "belowBar",
                            "color": "#3fb950", "shape": "arrowUp", "text": "買"})
        elif d_prev > m_prev and d_cur <= m_cur:
            markers.append({"time": times[i], "position": "aboveBar",
                            "color": "#f85149", "shape": "arrowDown", "text": "賣"})

    # G1: 240MA 年線
    ma240 = ma_pts(240)

    # 持倉進場標記
    entry_markers = []
    entry_price = None
    try:
        con_p = db()
        cur_p = con_p.cursor()
        cur_p.execute(
            "SELECT entry_date, cost FROM positions WHERE code=? AND (status='open' OR status IS NULL)",
            (code,))
        pos_rows = cur_p.fetchall()
        con_p.close()
        for entry_date, cost in pos_rows:
            if not cost:
                continue
            entry_price = cost
            if entry_date and str(entry_date).strip():
                try:
                    ed = str(entry_date).strip()
                    if 'T' in ed:
                        et = int(datetime.fromisoformat(ed).timestamp())
                    else:
                        et = int(datetime.strptime(ed[:10], "%Y-%m-%d").timestamp())
                    entry_markers.append({
                        "time": et, "position": "belowBar",
                        "color": "#f0b429", "shape": "arrowUp",
                        "text": f"進場 ${cost}"
                    })
                except Exception:
                    pass
    except Exception:
        pass

    return {
        "code":    code,
        "tf":      tf,
        "candles": candles,
        "ma5":     ma_pts(5),
        "ma10":    ma_pts(10),
        "ma20":    ma_pts(20),
        "ma60":    ma_pts(60),
        "ma240":   ma240,
        "volume":  [{"time": t, "value": v,
                     "color": "rgba(248,81,73,0.5)" if closes[i] >= closes[i-1] else "rgba(63,185,80,0.5)"}
                    for i, (t, v) in enumerate(zip(times, vols))],
        "macd": {
            "dif":  [{"time": t, "value": safe(v)} for t, v in zip(times, dif)],
            "macd": [{"time": t, "value": safe(v)} for t, v in zip(times, macd_line)],
            "hist": [{"time": t, "value": safe(v),
                      "color": "rgba(248,81,73,0.8)" if v >= 0 else "rgba(63,185,80,0.8)"}
                     for t, v in zip(times, hist)],
        },
        "markers": markers[-30:],
        "entry_markers": entry_markers,
        "entry_price": entry_price,
    }

# ── Endpoints ─────────────────────────────────────

@app.get("/api/health")
def health():
    return {"ok": True}

@app.get("/api/info")
def info():
    return {"simulation": SIMULATION, "time": datetime.now().isoformat(), "version": "2.0"}

@app.get("/api/snapshot/{code}")
def snapshot(code: str):
    try:
        api = get_api()
        contract = api.Contracts.Stocks.get(code)
        if contract is not None:
            snaps = _api_call_with_backoff(api.snapshots, [contract])
            if snaps:
                s = snaps[0]
                close_val = safe(s.close)
                # Patch 0-close from kbar_cache
                if not close_val:
                    con = db(); cur = con.cursor()
                    cur.execute("SELECT close FROM kbar_cache WHERE code=? AND tf='D' ORDER BY date_key DESC LIMIT 1", (code,))
                    row = cur.fetchone(); con.close()
                    close_val = row[0] if row else None
                return {
                    "code":         str(s.code),
                    "name":         getattr(contract, "name", code),
                    "close":        close_val,
                    "open":         safe(s.open),
                    "high":         safe(s.high),
                    "low":          safe(s.low),
                    "change_price": safe(s.change_price),
                    "change_rate":  safe(s.change_rate),
                    "total_volume": int(s.total_volume),
                    "average_price":safe(s.average_price),
                    "buy_price":    safe(s.buy_price),
                    "sell_price":   safe(s.sell_price),
                }
    except Exception:
        pass
    # Shioaji unavailable — return kbar close
    con = db(); cur = con.cursor()
    cur.execute("SELECT close FROM kbar_cache WHERE code=? AND tf='D' ORDER BY date_key DESC LIMIT 1", (code,))
    row = cur.fetchone(); con.close()
    if row:
        return {"code": code, "name": code, "close": row[0], "change_price": None, "change_rate": None}
    return JSONResponse({"error": f"找不到 {code}"}, status_code=404)

@app.post("/api/kbars/warm-up")
def kbars_warm_up():
    """
    一鍵補齊：對所有自選股+持倉+市場精選抓日K並寫入快取。
    Shioaji 優先，失敗改用 yfinance。
    """
    con = db()
    cur = con.cursor()
    cur.execute("SELECT DISTINCT code, name, market FROM watchlist")
    wl = cur.fetchall()
    cur.execute("SELECT DISTINCT code, name, market FROM positions WHERE status='open' OR status IS NULL")
    pos = cur.fetchall()
    con.close()
    seen = {}
    for c, n, m in (list(wl) + list(pos)):
        seen[c] = (c, n, _detect_market(c))
    for mkt_key, stocks in _MARKET_UNIVERSE.items():
        for code, name in stocks:
            if code not in seen:
                seen[code] = (code, name, mkt_key)
    all_cands = list(seen.values())
    tw_cands = [c for c in all_cands if c[2] == "TW"]
    _warm_up_kbars_for_market("TW", tw_cands)
    return {"total": len(tw_cands), "data": tw_cands, "results": tw_cands, "message": f"K線補齊完成（{len(tw_cands)} 檔台股）"}

@app.get("/api/kbars/{code}")
def kbars(code: str, tf: str = "D", days: int = 0, limit: int = 0, start_date: str = "", end_date: str = ""):
    """
    tf: "5" 5分K / "60" 60分K / "D" 日K
    days: >0 拉指定天數日K（從 market.db 補充）
    limit: 限制回傳筆數
    start_date/end_date: 指定日期範圍（YYYY-MM-DD）
    支援指數代碼：^TWII, ^VIX, ^SOX, ^GSPC, ^IXIC 等
    """
    # 指數代碼（^開頭）或長期日K請求 → 走 market.db
    is_index = code.startswith("^") or code.startswith("%5E")
    if is_index:
        code = code.replace("%5E", "^")
    effective_days = days or (limit if limit else 0)
    # 日K 預設拉 5 年長期資料（不再只吐 6 個月快取）
    if tf == "D" and effective_days == 0 and not start_date:
        effective_days = 1825
    use_market_db = (effective_days > 180 or is_index or start_date) and tf == "D"

    if use_market_db:
        mkt = "INDEX" if is_index else ("US" if not code.isdigit() else "TW")
        if start_date:
            sd = start_date
        elif effective_days > 0:
            sd = (datetime.now() - timedelta(days=int(effective_days * 1.5))).strftime("%Y-%m-%d")
        else:
            sd = (datetime.now() - timedelta(days=3650)).strftime("%Y-%m-%d")
        ed = end_date or datetime.now().strftime("%Y-%m-%d")

        # 指數用 yfinance 直抓存入 daily_kbar
        if is_index:
            _ensure_index_data(code, sd, ed)
        else:
            _ensure_daily_data(code, mkt, sd, ed)

        con = market_db(); cur = con.cursor()
        q = "SELECT date, open, high, low, close, volume FROM daily_kbar WHERE code=? AND date BETWEEN ? AND ? ORDER BY date"
        cur.execute(q, (code, sd, ed))
        rows = cur.fetchall(); con.close()
        if limit > 0:
            rows = rows[-limit:]
        if rows:
            candles = []
            for r in rows:
                dt = datetime.strptime(r[0], "%Y-%m-%d")
                candles.append({"time": int(dt.timestamp()), "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5] or 0})
            return _build_kbar_response(code, tf, candles)

    # 嘗試快取
    if _cache_key_fresh(code, tf):
        candles = _load_kbars_from_cache(code, tf)
        if candles:
            return _build_kbar_response(code, tf, candles)

    # 從 API 抓取
    df = _fetch_kbars_from_api(code, tf)
    if df.empty:
        # 退而求其次用舊快取
        candles = _load_kbars_from_cache(code, tf)
        if candles:
            return _build_kbar_response(code, tf, candles)
        return JSONResponse({"error": "no bars"}, status_code=404)

    _save_kbars(code, tf, df)

    if "ts" not in df.columns:
        df = df.reset_index()
    df["ts"] = pd.to_datetime(df["ts"])
    candles = []
    for _, row in df.iterrows():
        candles.append({
            "time":   int(row["ts"].timestamp()),
            "open":   safe(row["Open"]),
            "high":   safe(row["High"]),
            "low":    safe(row["Low"]),
            "close":  safe(row["Close"]),
            "volume": int(row["Volume"]),
        })

    return _build_kbar_response(code, tf, candles)


def _calc_indicators(code: str, market: str = "TW") -> dict:
    """Compute technical indicators from kbar_cache for a single stock."""
    con = db()
    cur = con.cursor()
    cur.execute(
        "SELECT date_key, open, high, low, close, volume FROM kbar_cache WHERE code=? AND tf='D' ORDER BY date_key",
        (code,)
    )
    rows = cur.fetchall()
    con.close()
    if len(rows) < 5:
        return {"code": code}

    closes = [r[4] for r in rows if r[4] is not None]
    highs  = [r[2] for r in rows if r[2] is not None]
    lows   = [r[3] for r in rows if r[3] is not None]
    volumes= [r[5] or 0 for r in rows]
    n = len(closes)
    result = {"code": code, "close": closes[-1] if closes else None}

    # --- MA ---
    for period in (5, 10, 20, 60, 120, 240):
        key = f"ma{period}"
        if n >= period:
            result[key] = round(sum(closes[-period:]) / period, 2)

    # --- RSI(14) ---
    rsi_period = 14
    if n > rsi_period:
        gains, losses = [], []
        for i in range(n - rsi_period, n):
            diff = closes[i] - closes[i - 1]
            gains.append(diff if diff > 0 else 0)
            losses.append(-diff if diff < 0 else 0)
        avg_gain = sum(gains) / rsi_period
        avg_loss = sum(losses) / rsi_period
        if avg_loss == 0:
            result["rsi"] = 100.0
        else:
            rs = avg_gain / avg_loss
            result["rsi"] = round(100 - 100 / (1 + rs), 2)

    # --- MACD (12, 26, 9) ---
    if n >= 26:
        def ema(data, period):
            k = 2 / (period + 1)
            e = [data[0]]
            for d in data[1:]:
                e.append(d * k + e[-1] * (1 - k))
            return e
        ema12 = ema(closes, 12)
        ema26 = ema(closes, 26)
        dif_line = [ema12[i] - ema26[i] for i in range(len(closes))]
        dea_line = ema(dif_line, 9)
        result["macd_dif"] = round(dif_line[-1], 2)
        result["macd_dea"] = round(dea_line[-1], 2)
        result["macd_hist"] = round(2 * (dif_line[-1] - dea_line[-1]), 2)

    # --- KD (9, 3, 3) ---
    kd_period = 9
    if n >= kd_period:
        recent_c = closes[-kd_period:]
        recent_h = highs[-kd_period:] if len(highs) >= kd_period else highs
        recent_l = lows[-kd_period:] if len(lows) >= kd_period else lows
        hh = max(highs[-kd_period:])
        ll = min(lows[-kd_period:])
        rsv = (closes[-1] - ll) / (hh - ll) * 100 if hh != ll else 50
        # Simple approx: K = 2/3*prev_K + 1/3*RSV, start at 50
        k_val = 50
        d_val = 50
        # Iterate last few RSV values for smoother result
        for i in range(max(0, n - 20), n):
            h_slice = highs[max(0, i - kd_period + 1):i + 1]
            l_slice = lows[max(0, i - kd_period + 1):i + 1]
            if h_slice and l_slice:
                hh_i = max(h_slice)
                ll_i = min(l_slice)
                rsv_i = (closes[i] - ll_i) / (hh_i - ll_i) * 100 if hh_i != ll_i else 50
                k_val = 2 / 3 * k_val + 1 / 3 * rsv_i
                d_val = 2 / 3 * d_val + 1 / 3 * k_val
        result["k_val"] = round(k_val, 2)
        result["d_val"] = round(d_val, 2)

    # --- Bollinger Bands (20, 2) ---
    bb_period = 20
    if n >= bb_period:
        bb_closes = closes[-bb_period:]
        bb_ma = sum(bb_closes) / bb_period
        bb_std = (sum((c - bb_ma) ** 2 for c in bb_closes) / bb_period) ** 0.5
        result["boll_upper"] = round(bb_ma + 2 * bb_std, 2)
        result["boll_mid"] = round(bb_ma, 2)
        result["boll_lower"] = round(bb_ma - 2 * bb_std, 2)

    # --- ATR(14) ---
    atr_period = 14
    if n >= atr_period + 1:
        trs = []
        for i in range(n - atr_period, n):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1])
            )
            trs.append(tr)
        result["atr"] = round(sum(trs) / atr_period, 2)

    # --- 52-week high/low ---
    w52 = min(252, n)
    result["high52"] = max(highs[-w52:])
    result["low52"] = min(lows[-w52:])

    # --- Volume MA ---
    if n >= 20:
        result["vol_ma5"] = round(sum(volumes[-5:]) / 5)
        result["vol_ma20"] = round(sum(volumes[-20:]) / 20)
        result["volume"] = volumes[-1]

    return result


@app.get("/api/kbars/{code}/strategy-markers")
def kbar_strategy_markers(code: str, strategies: str = ""):
    """Return signal_log entries as LightweightCharts markers for given strategies."""
    if not strategies:
        return []
    sids = [s.strip() for s in strategies.split(",") if s.strip()]
    con = db()
    cur = con.cursor()
    placeholders = ",".join("?" * len(sids))
    cur.execute(
        f"SELECT signal_type, direction, price, created_at FROM signal_log "
        f"WHERE code=? AND signal_type IN ({placeholders}) ORDER BY created_at",
        [code] + sids)
    rows = cur.fetchall()
    con.close()
    markers = []
    for sig_type, direction, price, created_at in rows:
        try:
            ts = int(datetime.fromisoformat(str(created_at)).timestamp())
        except Exception:
            continue
        if direction == "BUY":
            markers.append({"time": ts, "position": "belowBar",
                            "color": "#f85149", "shape": "circle", "text": sig_type})
        else:
            markers.append({"time": ts, "position": "aboveBar",
                            "color": "#3fb950", "shape": "circle", "text": sig_type})
    return markers

@app.get("/api/kbars/{code}/indicators")
def kbar_indicators(code: str):
    """Return technical indicators for a single stock."""
    return _calc_indicators(code)


@app.get("/api/indicators/batch")
def batch_indicators(codes: str = ""):
    """Return indicators for multiple stocks. codes=2330,2317,AAPL"""
    if not codes:
        return []
    code_list = [c.strip() for c in codes.split(",") if c.strip()]
    return [_calc_indicators(c) for c in code_list]


@app.get("/api/watchlist")
def watchlist_snap(codes: str = "", market: str = "TW"):
    """若 codes 為空，從 DB watchlist 取；否則用傳入的逗號清單"""
    con = db()
    cur = con.cursor()
    if not codes:
        cur.execute("SELECT code FROM watchlist WHERE market=? ORDER BY sort_order, added_at", (market,))
        code_list = [r[0] for r in cur.fetchall()]
    else:
        code_list = [c.strip() for c in codes.split(",") if c.strip()]
    con.close()

    if not code_list:
        return []

    # Get names from DB for fallback
    con2 = db()
    cur2 = con2.cursor()
    cur2.execute("SELECT code, name FROM watchlist")
    name_map = {r[0]: r[1] for r in cur2.fetchall()}
    con2.close()

    result = []
    snap_map = {}
    try:
        api = get_api()
        contracts = [api.Contracts.Stocks[c] for c in code_list
                     if api.Contracts.Stocks.get(c) is not None]
        if contracts:
            try:
                snaps = _api_call_with_backoff(api.snapshots, contracts)
                for s in snaps:
                    ctr = api.Contracts.Stocks[str(s.code)]
                    snap_map[str(s.code)] = {
                        "code":         str(s.code),
                        "name":         getattr(ctr, "name", str(s.code)),
                        "close":        safe(s.close),
                        "change_price": safe(s.change_price),
                        "change_rate":  safe(s.change_rate),
                        "volume_ratio": safe(getattr(s, "volume_ratio", 0)),
                        "average_price":safe(s.average_price),
                    }
            except Exception:
                pass
    except Exception:
        pass  # Shioaji unavailable — will fall back to kbar_cache below

    # kbar_cache fallback: last known close per code
    con3 = db()
    cur3 = con3.cursor()
    cur3.execute("""
        SELECT code, close FROM kbar_cache
        WHERE tf='D' AND (code, date_key) IN (
            SELECT code, MAX(date_key) FROM kbar_cache WHERE tf='D' GROUP BY code
        )
    """)
    kbar_close = {r[0]: r[1] for r in cur3.fetchall()}
    con3.close()

    # Return results for all codes, with fallback for missing snapshots
    for c in code_list:
        if c in snap_map:
            row = snap_map[c]
            # If snapshot returned 0/None close, patch from kbar
            if not row.get("close"):
                row["close"] = kbar_close.get(c)
            result.append(row)
        else:
            result.append({
                "code": c,
                "name": name_map.get(c, c),
                "close": kbar_close.get(c),
                "change_price": None, "change_rate": None,
                "volume_ratio": None, "average_price": None,
            })
    return result

@app.get("/api/sparkline/{code}")
def sparkline(code: str):
    api = get_api()
    contract = api.Contracts.Stocks.get(code)
    if contract is not None:
        try:
            start = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
            end   = datetime.now().strftime("%Y-%m-%d")
            bars  = _api_call_with_backoff(api.kbars, contract, start=start, end=end)
            if bars is not None:
                df = pd.DataFrame({**bars})
                if not df.empty:
                    return [round(float(v), 2) for v in df["Close"].tail(30).tolist()]
        except Exception:
            pass  # live API 失敗 → fall through 到 cache
    mkt = "US" if not code.isdigit() else "TW"
    ohlcv = _get_ohlcv_from_cache(code, 30, mkt)
    if ohlcv and ohlcv.get("closes"):
        return [round(float(v), 2) for v in ohlcv["closes"][-30:]]
    return []

# ── 自選股 CRUD ───────────────────────────────────

@app.get("/api/watchlist/list")
def get_watchlist():
    con = db()
    cur = con.cursor()
    cur.execute("SELECT code, name, sort_order FROM watchlist ORDER BY sort_order, added_at")
    rows = cur.fetchall()
    con.close()
    return [{"code": r[0], "name": r[1], "sort_order": r[2]} for r in rows]

@app.post("/api/watchlist/add/{code}")
def add_to_watchlist(code: str):
    api = get_api()
    contract = api.Contracts.Stocks.get(code)
    name = getattr(contract, "name", code) if contract else code
    con = db()
    cur = con.cursor()
    cur.execute("SELECT MAX(sort_order) FROM watchlist")
    max_sort = (cur.fetchone()[0] or 0) + 1
    cur.execute(
        "INSERT OR IGNORE INTO watchlist(code,name,sort_order,market) VALUES(?,?,?,?)",
        (code, name, max_sort, "TW")
    )
    con.commit()
    con.close()
    return {"ok": True, "code": code, "name": name}

@app.delete("/api/watchlist/remove/{code}")
def remove_from_watchlist(code: str):
    con = db()
    cur = con.cursor()
    cur.execute("DELETE FROM watchlist WHERE code=? AND market='TW'", (code,))
    con.commit()
    con.close()
    return {"ok": True}

# ── 持倉 CRUD ─────────────────────────────────────

def _ensure_watchlist(code: str, name: str, market: str):
    """持倉新增時自動同步到 watchlist（單向：有持倉就要有自選）"""
    con = db()
    exists = con.execute("SELECT 1 FROM watchlist WHERE code=?", (code,)).fetchone()
    if not exists:
        max_sort = con.execute("SELECT COALESCE(MAX(sort_order),0) FROM watchlist WHERE market=?", (market,)).fetchone()[0]
        try:
            con.execute("INSERT OR IGNORE INTO watchlist(code, name, market, sort_order) VALUES(?,?,?,?)",
                        (code, name or code, market, max_sort + 1))
            con.commit()
        except Exception:
            pass
    con.close()

class PositionIn(BaseModel):
    code:         str
    name:         Optional[str] = ""
    trade_type:   Optional[str] = "波段"
    shares:       Optional[int] = 0
    cost:         Optional[float] = 0
    stop_loss:    Optional[float] = 0
    target_price: Optional[float] = 0
    highest_price: Optional[float] = 0
    entry_date:   Optional[str] = ""
    signal_type:  Optional[str] = ""
    note:         Optional[str] = ""

@app.get("/api/positions")
def get_positions(market: str = ""):
    con = db()
    cur = con.cursor()
    if market:
        cur.execute("""
            SELECT id,code,name,trade_type,shares,cost,stop_loss,target_price,
                   highest_price,entry_date,signal_type,note,updated_at,market,status
            FROM positions WHERE status='open' AND market=? ORDER BY updated_at DESC
        """, (market.upper(),))
    else:
        cur.execute("""
            SELECT id,code,name,trade_type,shares,cost,stop_loss,target_price,
                   highest_price,entry_date,signal_type,note,updated_at,market,status
            FROM positions WHERE status='open' ORDER BY updated_at DESC
        """)
    rows = cur.fetchall()
    con.close()
    cols = ["id","code","name","trade_type","shares","cost","stop_loss","target_price",
            "highest_price","entry_date","signal_type","note","updated_at","market","status"]
    results = [dict(zip(cols, r)) for r in rows]
    for p in results:
        try:
            ohlcv = _get_ohlcv_from_cache(p["code"], 5, p.get("market", "TW"))
            p["current_price"] = round(ohlcv["closes"][-1], 2) if ohlcv and ohlcv.get("closes") else 0
        except Exception:
            p["current_price"] = 0
        cost = p.get("cost", 0)
        p["pnl_pct"] = round((p["current_price"] / cost - 1) * 100, 2) if cost and p["current_price"] else 0
    return results

@app.post("/api/positions")
def add_position(p: PositionIn, _: None = Depends(require_token)):
    con = db()
    cur = con.cursor()
    now = datetime.now().isoformat()
    highest = p.highest_price if p.highest_price else p.cost
    market = _detect_market(p.code)
    cur.execute("""
        INSERT INTO positions(code,name,trade_type,shares,cost,stop_loss,target_price,
                              highest_price,entry_date,signal_type,note,updated_at,market)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (p.code, p.name, p.trade_type, p.shares, p.cost, p.stop_loss,
          p.target_price, highest, p.entry_date, p.signal_type, p.note, now, market))
    pid = cur.lastrowid
    con.commit()
    con.close()
    _ensure_watchlist(p.code, p.name or p.code, market)
    return {"ok": True, "id": pid}

@app.put("/api/positions/{pid}")
def update_position(pid: int, p: PositionIn, _: None = Depends(require_token)):
    con = db()
    cur = con.cursor()
    now = datetime.now().isoformat()
    cur.execute("""
        UPDATE positions SET code=?,name=?,trade_type=?,shares=?,cost=?,
               stop_loss=?,target_price=?,highest_price=?,entry_date=?,signal_type=?,note=?,updated_at=?
        WHERE id=?
    """, (p.code, p.name, p.trade_type, p.shares, p.cost, p.stop_loss,
          p.target_price, p.highest_price, p.entry_date, p.signal_type, p.note, now, pid))
    con.commit()
    con.close()
    return {"ok": True}

@app.delete("/api/positions/{pid}")
def delete_position(pid: int, _: None = Depends(require_token)):
    con = db()
    cur = con.cursor()
    cur.execute("DELETE FROM positions WHERE id=?", (pid,))
    con.commit()
    con.close()
    return {"ok": True}

# P7: Lifecycle stage + exit conditions
@app.put("/api/positions/{pid}/lifecycle")
def update_lifecycle(pid: int, body: dict, _: None = Depends(require_token)):
    con = db()
    stage = body.get("stage", "holding")
    exit_cond = body.get("exit_conditions", "")
    con.execute("UPDATE positions SET lifecycle_stage=?, exit_conditions=?, updated_at=? WHERE id=?",
                (stage, exit_cond, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), pid))
    con.commit()
    con.close()
    return {"ok": True}

@app.get("/api/positions/{pid}/exit-check")
def check_exit_conditions(pid: int):
    """自動檢查持倉的出場條件"""
    con = db()
    row = con.execute("SELECT code, name, cost, stop_loss, target_price, shares, lifecycle_stage, exit_conditions FROM positions WHERE id=?", (pid,)).fetchone()
    con.close()
    if not row:
        return {"error": "not found"}
    code, name, cost, sl, tp, shares, stage, exit_cond = row
    market = "US" if not code.isdigit() else "TW"
    alerts = []
    try:
        tech = _ic_score_stock(code, market)
        price = tech.get("price", 0)
        if sl and price and price <= sl:
            alerts.append({"type": "stop_loss", "msg": f"已觸及停損 {sl}", "severity": "high"})
        if tp and price and price >= tp:
            alerts.append({"type": "target_hit", "msg": f"已達目標價 {tp}", "severity": "medium"})
        if cost and price:
            pnl_pct = (price / cost - 1) * 100
            if pnl_pct <= -10:
                alerts.append({"type": "loss_warning", "msg": f"虧損{pnl_pct:.1f}%", "severity": "high"})
            elif pnl_pct >= 30:
                alerts.append({"type": "profit_lock", "msg": f"獲利{pnl_pct:.1f}%，考慮鎖利", "severity": "low"})
        direction = tech.get("direction", "HOLD")
        if direction == "SELL":
            alerts.append({"type": "tech_sell", "msg": f"技術面轉空 {tech.get('score',0)}/100", "severity": "medium"})
        return {"code": code, "name": name, "price": price, "stage": stage, "alerts": alerts, "score": tech.get("score")}
    except Exception as e:
        return {"code": code, "alerts": [], "error": str(e)}

# ── 交易紀錄 ──────────────────────────────────────

class TradeRecordIn(BaseModel):
    code: str
    name: Optional[str] = ""
    market: Optional[str] = "TW"
    action: str  # BUY / SELL
    shares: int
    price: float
    trade_date: Optional[str] = ""
    commission_rate: Optional[float] = 0.001425
    commission_discount: Optional[float] = 0.6
    tax_rate: Optional[float] = 0.003
    note: Optional[str] = ""
    position_id: Optional[int] = None

def _calc_trade_costs(action: str, price: float, shares: int, market: str,
                      commission_rate: float, commission_discount: float, tax_rate: float):
    trade_value = price * shares
    if market == "TW":
        trade_value *= 1000  # 台股 1張=1000股
    commission = trade_value * commission_rate * (1 - commission_discount / 100.0 if commission_discount > 1 else 1 - commission_discount)
    tax = trade_value * tax_rate if action == "SELL" else 0
    total_cost = commission + tax
    if action == "BUY":
        net_amount = -(trade_value + total_cost)
    else:
        net_amount = trade_value - total_cost
    return round(commission, 2), round(tax, 2), round(total_cost, 2), round(net_amount, 2)

@app.get("/api/trade-records")
def get_trade_records(code: str = "", market: str = ""):
    con = db()
    sql = "SELECT * FROM trade_records"
    params = []
    wheres = []
    if code:
        wheres.append("code=?")
        params.append(code)
    if market:
        wheres.append("market=?")
        params.append(market)
    if wheres:
        sql += " WHERE " + " AND ".join(wheres)
    sql += " ORDER BY trade_date DESC, id DESC"
    rows = con.execute(sql, params).fetchall()
    cols = [d[0] for d in con.execute("PRAGMA table_info(trade_records)").fetchall()]
    col_names = [c[1] for c in con.execute("PRAGMA table_info(trade_records)").fetchall()]
    con.close()
    return [dict(zip(col_names, r)) for r in rows]

@app.post("/api/trade-records")
def add_trade_record(t: TradeRecordIn, _: None = Depends(require_token)):
    trade_date = t.trade_date or datetime.now().strftime("%Y-%m-%d")
    market = t.market or ("US" if t.code.isalpha() else "TW")
    commission, tax, total_cost, net_amount = _calc_trade_costs(
        t.action, t.price, t.shares, market,
        t.commission_rate, t.commission_discount, t.tax_rate)
    con = db()
    cur = con.cursor()
    cur.execute("""
        INSERT INTO trade_records(code, name, market, action, shares, price, trade_date,
            commission_rate, commission_discount, tax_rate, commission, tax, total_cost,
            net_amount, position_id, note)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (t.code, t.name, market, t.action, t.shares, t.price, trade_date,
          t.commission_rate, t.commission_discount, t.tax_rate,
          commission, tax, total_cost, net_amount, t.position_id, t.note))
    rec_id = cur.lastrowid
    con.commit()
    con.close()
    _trade_sync_position(t.code, t.name, market, t.action, t.shares, t.price, trade_date, rec_id)
    _ensure_watchlist(t.code, t.name or t.code, market)
    return {"ok": True, "id": rec_id, "commission": commission, "tax": tax,
            "total_cost": total_cost, "net_amount": net_amount}

def _trade_sync_position(code, name, market, action, shares, price, trade_date, rec_id):
    """BUY→建立/增加持倉, SELL→減少/關閉持倉"""
    con = db()
    cur = con.cursor()
    now = datetime.now().isoformat()
    existing = cur.execute(
        "SELECT id, shares, cost FROM positions WHERE code=? AND status='open' ORDER BY id DESC LIMIT 1",
        (code,)).fetchone()
    if action == "BUY":
        if existing:
            pid, old_shares, old_cost = existing
            new_shares = old_shares + shares
            new_cost = (old_cost * old_shares + price * shares) / new_shares if new_shares else price
            cur.execute("UPDATE positions SET shares=?, cost=?, updated_at=? WHERE id=?",
                        (new_shares, round(new_cost, 4), now, pid))
        else:
            cur.execute("""
                INSERT INTO positions(code, name, trade_type, shares, cost, entry_date, signal_type, note, updated_at, market)
                VALUES(?,?,?,?,?,?,?,?,?,?)
            """, (code, name, "波段", shares, price, trade_date, "手動買入", f"交易紀錄#{rec_id}", now, _detect_market(code)))
    elif action == "SELL":
        if existing:
            pid, old_shares, old_cost = existing
            remain = old_shares - shares
            if remain <= 0:
                cur.execute("UPDATE positions SET shares=0, status='closed', updated_at=? WHERE id=?", (now, pid))
            else:
                cur.execute("UPDATE positions SET shares=?, updated_at=? WHERE id=?", (remain, now, pid))
    con.commit()
    con.close()

@app.put("/api/trade-records/{rid}")
def update_trade_record(rid: int, t: TradeRecordIn, _: None = Depends(require_token)):
    trade_date = t.trade_date or datetime.now().strftime("%Y-%m-%d")
    market = t.market or ("US" if t.code.isalpha() else "TW")
    commission, tax, total_cost, net_amount = _calc_trade_costs(
        t.action, t.price, t.shares, market,
        t.commission_rate, t.commission_discount, t.tax_rate)
    con = db()
    con.execute("""
        UPDATE trade_records SET code=?, name=?, market=?, action=?, shares=?, price=?, trade_date=?,
            commission_rate=?, commission_discount=?, tax_rate=?, commission=?, tax=?, total_cost=?,
            net_amount=?, note=? WHERE id=?
    """, (t.code, t.name, market, t.action, t.shares, t.price, trade_date,
          t.commission_rate, t.commission_discount, t.tax_rate,
          commission, tax, total_cost, net_amount, t.note, rid))
    con.commit()
    con.close()
    return {"ok": True, "commission": commission, "tax": tax, "total_cost": total_cost, "net_amount": net_amount}

@app.delete("/api/trade-records/{rid}")
def delete_trade_record(rid: int, _: None = Depends(require_token)):
    con = db()
    con.execute("DELETE FROM trade_records WHERE id=?", (rid,))
    con.commit()
    con.close()
    return {"ok": True}

@app.get("/api/trade-records/analytics")
def trade_analytics(code: str = ""):
    con = db()
    params = []
    where = ""
    if code:
        where = " WHERE code=?"
        params = [code]
    rows = con.execute(f"SELECT * FROM trade_records{where} ORDER BY trade_date, id", params).fetchall()
    col_names = [c[1] for c in con.execute("PRAGMA table_info(trade_records)").fetchall()]
    con.close()
    records = [dict(zip(col_names, r)) for r in rows]
    by_code = {}
    for r in records:
        by_code.setdefault(r["code"], []).append(r)
    analytics = {"total_records": len(records), "total_commission": 0, "total_tax": 0,
                 "realized_pnl": 0, "stocks": {}}
    for c, trades in by_code.items():
        buys, sells = [], []
        for t in trades:
            analytics["total_commission"] += t.get("commission", 0)
            analytics["total_tax"] += t.get("tax", 0)
            if t["action"] == "BUY":
                buys.append(t)
            else:
                sells.append(t)
        total_buy_val = sum(b["price"] * b["shares"] for b in buys)
        total_buy_shares = sum(b["shares"] for b in buys)
        avg_cost = total_buy_val / total_buy_shares if total_buy_shares else 0
        total_sell_val = sum(s["price"] * s["shares"] for s in sells)
        total_sell_shares = sum(s["shares"] for s in sells)
        realized = total_sell_val - avg_cost * total_sell_shares if total_sell_shares else 0
        analytics["realized_pnl"] += realized
        analytics["stocks"][c] = {
            "name": trades[0].get("name", c), "buy_count": len(buys), "sell_count": len(sells),
            "avg_cost": round(avg_cost, 2), "total_buy_shares": total_buy_shares,
            "total_sell_shares": total_sell_shares, "realized_pnl": round(realized, 2),
            "total_cost": round(sum(t.get("total_cost", 0) for t in trades), 2)
        }
    analytics["total_commission"] = round(analytics["total_commission"], 2)
    analytics["total_tax"] = round(analytics["total_tax"], 2)
    analytics["realized_pnl"] = round(analytics["realized_pnl"], 2)
    analytics["total_pnl"] = analytics["realized_pnl"]
    sell_trades = sum(s.get("sell_count", 0) for s in analytics["stocks"].values())
    wins = sum(1 for s in analytics["stocks"].values() if s.get("realized_pnl", 0) > 0 and s.get("sell_count", 0) > 0)
    analytics["total_trades"] = analytics["total_records"]
    analytics["closed_trades"] = sell_trades
    sold_stocks = [s for s in analytics["stocks"].values() if s.get("sell_count", 0) > 0]
    analytics["win_rate"] = round(wins / len(sold_stocks) * 100, 1) if sold_stocks else 0
    analytics["avg_pnl"] = round(analytics["realized_pnl"] / sell_trades, 2) if sell_trades else 0
    total_wins = sum(s["realized_pnl"] for s in sold_stocks if s["realized_pnl"] > 0)
    total_losses = abs(sum(s["realized_pnl"] for s in sold_stocks if s["realized_pnl"] < 0))
    analytics["profit_factor"] = round(total_wins / total_losses, 2) if total_losses > 0 else (999 if total_wins > 0 else 0)
    return analytics

@app.post("/api/trade-records/migrate-positions")
def migrate_positions_to_trades(_: None = Depends(require_token)):
    """將現有持倉轉為交易紀錄（回溯至專案開始日）"""
    PROJECT_START = "2026-06-01"
    con = db()
    positions = con.execute(
        "SELECT id, code, name, shares, cost, entry_date, market FROM positions WHERE status='open'"
    ).fetchall()
    existing = con.execute("SELECT DISTINCT position_id FROM trade_records WHERE position_id IS NOT NULL").fetchall()
    existing_pids = {r[0] for r in existing}
    created = 0
    for pid, code, name, shares, cost, entry_date, pos_market in positions:
        if pid in existing_pids:
            continue
        market = pos_market or ("US" if code.isalpha() else "TW")
        date = entry_date if entry_date else PROJECT_START
        # positions.shares 可能是「股」(舊資料) 或「張」(新交易同步)，migrate 不重新計算費用
        trade_shares = shares
        if market == "TW" and shares >= 1000:
            trade_shares = shares // 1000
        commission, tax, total_cost, net_amount = _calc_trade_costs(
            "BUY", cost, trade_shares, market, 0.001425, 0.6, 0.003)
        con.execute("""
            INSERT INTO trade_records(code, name, market, action, shares, price, trade_date,
                commission_rate, commission_discount, tax_rate, commission, tax, total_cost,
                net_amount, position_id, note)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (code, name, market, "BUY", shares, cost, date,
              0.001425, 0.6, 0.003, commission, tax, total_cost, net_amount, pid,
              "從既有持倉匯入"))
        created += 1
    con.commit()
    con.close()
    return {"ok": True, "migrated": created}

# ── 風控設定 ──────────────────────────────────────

@app.get("/api/risk-config")
def get_risk_config(_: None = Depends(require_token)):
    _SENSITIVE_KEYS = {"email_pass", "smtp_password", "telegram_bot_token", "api_key", "shioaji_api_key", "shioaji_secret"}
    con = db()
    cur = con.cursor()
    cur.execute("SELECT key, value FROM risk_config")
    rows = cur.fetchall()
    con.close()
    return {r[0]: ("***" if r[0] in _SENSITIVE_KEYS and r[1] else r[1]) for r in rows}

@app.post("/api/risk-config")
def set_risk_config(data: dict, _: None = Depends(require_token)):
    con = db()
    cur = con.cursor()
    now = datetime.now().isoformat()
    for k, v in data.items():
        cur.execute(
            "INSERT OR REPLACE INTO risk_config(key,value,updated_at) VALUES(?,?,?)",
            (k, str(v), now)
        )
    con.commit()
    con.close()
    return {"ok": True}

@app.get("/api/macro")
def macro_data():
    """
    抓 VIX / DXY / US10Y / ES期貨 / 加權指數 並判斷 MACRO_LOCK 條件。
    用 yfinance，結果快取 15 分鐘。
    """
    try:
        import yfinance as yf
    except ImportError:
        return JSONResponse({"error": "pip install yfinance"}, status_code=500)

    cache_key = "macro_cache"
    con = db(); cur = con.cursor()
    cur.execute("SELECT value, updated_at FROM risk_config WHERE key=?", (cache_key,))
    row = cur.fetchone(); con.close()
    if row:
        upd = datetime.fromisoformat(row[1])
        if (datetime.now() - upd).total_seconds() < 900:  # 15-min cache
            return json.loads(row[0])

    def _fetch(ticker, period="5d"):
        try:
            hist = yf.Ticker(ticker).history(period=period)
            if hist.empty:
                return None, None
            last  = float(hist["Close"].iloc[-1])
            prev  = float(hist["Close"].iloc[-2]) if len(hist) > 1 else last
            return last, prev
        except Exception:
            return None, None

    vix,  vix_prev  = _fetch("^VIX")
    us10y,_         = _fetch("^TNX")
    es,   es_prev   = _fetch("ES=F")

    # DXY: one fetch for both current value and monthly change
    try:
        dxy_hist = yf.Ticker("DX-Y.NYB").history(period="35d")["Close"].dropna()
        dxy      = float(dxy_hist.iloc[-1]) if len(dxy_hist) >= 1 else None
        dxy_month_chg = (float(dxy_hist.iloc[-1]) / float(dxy_hist.iloc[0]) - 1) * 100 if len(dxy_hist) >= 10 else 0
    except Exception:
        dxy, dxy_month_chg = None, 0

    # TWII: one fetch for value, prev, and MA
    try:
        twii_hist = yf.Ticker("^TWII").history(period="35d")["Close"].dropna()
        twii      = float(twii_hist.iloc[-1]) if len(twii_hist) >= 1 else None
        twii_prev = float(twii_hist.iloc[-2]) if len(twii_hist) >= 2 else twii
        twii_ma   = float(twii_hist.mean()) if len(twii_hist) >= 5 else twii
        twii_ma_pct = (twii / twii_ma - 1) * 100 if twii and twii_ma else 0
    except Exception:
        twii, twii_prev, twii_ma_pct = None, None, 0

    result = {
        "vix":           safe(vix),
        "vix_alert":     vix is not None and vix > 35,
        "dxy":           safe(dxy),
        "dxy_month_chg": safe(dxy_month_chg),
        "dxy_alert":     dxy_month_chg > 3,
        "us10y":         safe(us10y),
        "us10y_alert":   us10y is not None and us10y > 5,
        "es":            safe(es),
        "es_chg":        safe((es / es_prev - 1) * 100) if es and es_prev else None,
        "twii":          safe(twii),
        "twii_chg":      safe((twii / twii_prev - 1) * 100) if twii and twii_prev else None,
        "twii_ma_pct":   safe(twii_ma_pct),
        "twii_alert":    twii_ma_pct < -5,
    }

    # 評估風控等級（三級制）
    alerts = [result["vix_alert"], result["dxy_alert"], result["us10y_alert"], result["twii_alert"]]
    alert_count = sum(alerts)
    if alert_count >= 2:
        result["risk_level"] = "ALERT"
        result["position_scale"] = 30
    elif alert_count == 1:
        result["risk_level"] = "CAUTION"
        result["position_scale"] = 60
    else:
        result["risk_level"] = "NORMAL"
        result["position_scale"] = 100
    result["macro_lock_suggested"] = alert_count >= 2
    result["alert_count"] = alert_count

    # 快取 + 持久化風控等級
    now_iso = datetime.now().isoformat()
    con = db(); cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO risk_config(key,value,updated_at) VALUES(?,?,?)",
                (cache_key, json.dumps(result), now_iso))
    cur.execute("INSERT OR REPLACE INTO risk_config(key,value,updated_at) VALUES(?,?,?)",
                ("risk_level", result["risk_level"], now_iso))
    cur.execute("INSERT OR REPLACE INTO risk_config(key,value,updated_at) VALUES(?,?,?)",
                ("position_scale", str(result["position_scale"]), now_iso))
    con.commit(); con.close()

    return result

@app.get("/api/vwap/{code}")
def get_vwap(code: str):
    with _vwap_lock:
        st = _vwap_state.get(code)
    if not st or st["cum_vol"] == 0:
        return {"code": code, "vwap": None, "cum_vol": 0}
    return {
        "code": code,
        "vwap": round(st["cum_pv"] / st["cum_vol"], 2),
        "cum_vol": st["cum_vol"],
    }

@app.get("/api/risk-level")
def get_risk_level():
    con = db(); cur = con.cursor()
    cur.execute("SELECT key, value FROM risk_config WHERE key IN ('risk_level','position_scale','macro_lock')")
    rows = cur.fetchall(); con.close()
    d = {r[0]: r[1] for r in rows}
    return {
        "risk_level": d.get("risk_level", "NORMAL"),
        "position_scale": int(d.get("position_scale", "100")),
        "macro_lock": d.get("macro_lock", "0") == "1",
    }

@app.get("/api/macro-lock")
def get_macro_lock():
    con = db()
    cur = con.cursor()
    cur.execute("SELECT value FROM risk_config WHERE key='macro_lock'")
    row = cur.fetchone()
    con.close()
    locked = bool(int(row[0])) if row else False
    return {"locked": locked}

@app.post("/api/macro-lock/{state}")
def set_macro_lock(state: str, _: None = Depends(require_token)):
    val = "1" if state in ("1", "on", "true") else "0"
    con = db()
    cur = con.cursor()
    cur.execute("UPDATE risk_config SET value=?, updated_at=? WHERE key='macro_lock'",
                (val, datetime.now().isoformat()))
    con.commit()
    con.close()
    return {"ok": True, "locked": val == "1"}

# ── 訊號記錄 ──────────────────────────────────────

@app.get("/api/signals")
def get_signals(limit: int = 50, code: str = ""):
    con = db()
    cur = con.cursor()
    if code:
        cur.execute("""
            SELECT s.id, s.code, s.signal_type, s.direction, s.price, s.detail, s.created_at,
                   COALESCE(NULLIF(p.name,''), NULLIF(w.name,''), '') as name
            FROM signal_log s
            LEFT JOIN positions p ON s.code = p.code AND p.status = 'open'
            LEFT JOIN watchlist w ON s.code = w.code
            WHERE s.code=?
            GROUP BY s.id
            ORDER BY s.id DESC LIMIT ?
        """, (code, limit))
    else:
        cur.execute("""
            SELECT s.id, s.code, s.signal_type, s.direction, s.price, s.detail, s.created_at,
                   COALESCE(NULLIF(p.name,''), NULLIF(w.name,''), '') as name
            FROM signal_log s
            LEFT JOIN positions p ON s.code = p.code AND p.status = 'open'
            LEFT JOIN watchlist w ON s.code = w.code
            GROUP BY s.id
            ORDER BY s.id DESC LIMIT ?
        """, (limit,))
    rows = cur.fetchall()
    con.close()
    cols = ["id","code","signal_type","direction","price","detail","created_at","name"]
    results = []
    for r in rows:
        d = dict(zip(cols, r))
        d["market"] = _detect_market(d["code"])
        results.append(d)
    return results

class SignalIn(BaseModel):
    code:        str
    signal_type: str
    direction:   str = "BUY"
    price:       float = 0
    detail:      str = ""

@app.post("/api/signals")
def log_signal(s: SignalIn, _: None = Depends(require_token)):
    con = db()
    cur = con.cursor()
    cur.execute("""
        INSERT INTO signal_log(code,signal_type,direction,price,detail)
        VALUES(?,?,?,?,?)
    """, (s.code, s.signal_type, s.direction, s.price, s.detail))
    con.commit()
    con.close()
    return {"ok": True}

# ── 訊號引擎 ──────────────────────────────────────

def _get_closes_from_cache(code: str, tf: str = "D", n: int = 300) -> list:
    """從 kbar_cache 取最近 n 根收盤價"""
    con = db()
    cur = con.cursor()
    cur.execute(
        "SELECT close FROM kbar_cache WHERE code=? AND tf=? ORDER BY date_key DESC LIMIT ?",
        (code, tf, n)
    )
    rows = cur.fetchall()
    con.close()
    closes = [r[0] for r in reversed(rows) if r[0] is not None]
    return closes

def _signal_exists_today(code: str, signal_type: str) -> bool:
    """避免同一天重複觸發相同訊號"""
    today = datetime.now().strftime("%Y-%m-%d")
    con = db()
    cur = con.cursor()
    cur.execute(
        "SELECT 1 FROM signal_log WHERE code=? AND signal_type=? AND created_at >= ? LIMIT 1",
        (code, signal_type, today + " 00:00:00")
    )
    found = cur.fetchone() is not None
    con.close()
    return found

def _write_signal(code: str, signal_type: str, direction: str, price: float, detail: str):
    con = db()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO signal_log(code,signal_type,direction,price,detail) VALUES(?,?,?,?,?)",
        (code, signal_type, direction, price, detail)
    )
    con.commit()
    con.close()
    # Phase 2: 推播通知
    icon = "🔴" if direction == "SELL" else "🟢" if direction == "BUY" else "⚠️"
    threading.Thread(
        target=_send_notification,
        args=(f"{icon} {signal_type} | {code}\n💰 ${price}\n📝 {detail}",),
        daemon=True,
    ).start()

_strategies_lock = threading.Lock()

def _get_strategy_param(sid: str, key: str, default):
    """從 STRATEGIES 取出使用者設定的參數值（已透過 _load_strategy_config 合併自 DB）"""
    for s in STRATEGIES:
        if s["id"] == sid:
            for p in s.get("params", []):
                if p["key"] == key:
                    return p.get("value", p["default"])
    return default

def _get_strategy_enabled(sid: str) -> bool:
    """回傳策略是否啟用（DB 狀態已於 _load_strategy_config 合併）"""
    for s in STRATEGIES:
        if s["id"] == sid:
            return bool(s.get("enabled", True))
    return True


def run_signal_engine(code: str, current_price: float) -> list:
    """
    對單支股票執行所有訊號掃描，回傳本次新觸發的訊號清單。
    訊號類型：BUY_A, BUY_B, LOW_BUY, LOCK_BUY, EXIT_A, EXIT_B, EXIT_C, EXIT_D, SQUEEZE_BREAK,
              SQUEEZE_BUY, NEWS_BEARISH, DAYTRADE_WARN
    """
    closes = _get_closes_from_cache(code, "D", 300)
    if len(closes) < 30:
        return []

    # 已啟用策略集合（DB 狀態於 _load_strategy_config 合併，讀取需加鎖）
    with _strategies_lock:
        _enabled = {s["id"] for s in STRATEGIES if s.get("enabled", True)}

    # 風控等級：ALERT 時不產生 BUY 訊號（僅允許 EXIT 訊號）
    con_rc = db()
    cur_rc = con_rc.cursor()
    cur_rc.execute("SELECT value FROM risk_config WHERE key='risk_level'")
    rl_row = cur_rc.fetchone()
    cur_rc.execute("SELECT value FROM risk_config WHERE key='macro_lock'")
    ml_row = cur_rc.fetchone()
    con_rc.close()
    _current_risk = rl_row[0] if rl_row else "NORMAL"
    _macro_locked = ml_row and ml_row[0] == "1"
    _block_buy = _current_risk == "ALERT" or _macro_locked

    triggered = []
    s = pd.Series(closes + [current_price])

    # MA 計算
    ma5   = s.rolling(5).mean().iloc[-1]
    ma10  = s.rolling(10).mean().iloc[-1]
    ma20  = s.rolling(20).mean().iloc[-1]
    ma60  = s.rolling(60).mean().iloc[-1] if len(s) >= 60 else None
    ma240 = s.rolling(240).mean().iloc[-1] if len(s) >= 240 else None

    prev5  = s.rolling(5).mean().iloc[-2]
    prev10 = s.rolling(10).mean().iloc[-2]
    prev_close = s.iloc[-2]

    # MACD
    dif_s, macd_s, _ = calc_macd(s.tolist())
    dif_cur, macd_cur = dif_s[-1], macd_s[-1]
    dif_prev, macd_prev = dif_s[-2], macd_s[-2]

    # 量比（與20日平均量比）
    vols = _get_vol_from_cache(code)
    vol_ratio = 1.0
    if vols and len(vols) >= 20:
        avg20 = sum(vols[-20:]) / 20
        vol_ratio = vols[-1] / avg20 if avg20 > 0 else 1.0

    # 取得 tick buffer 狀態（Phase 4）
    with _tick_buf_lock:
        buf = _tick_buf.get(code, {})
        outside_count = buf.get("outside_bid_count", 0)
        large_count = buf.get("large_order_count", 0)
        large_sell = buf.get("large_sell_count", 0)

    now = datetime.now()

    # ── BUY_A: 假跌破5MA→拉回 + tick-level 大單/連續外盤 ──
    with _breach_times_lock:
        if current_price < ma5 and code not in _breach_times:
            _breach_times[code] = now
        elif current_price >= ma5 and code in _breach_times:
            breach_t = _breach_times[code]
            elapsed_min = (now - breach_t).total_seconds() / 60
            _ba_min = _get_strategy_param("BUY_A", "breach_min", 15)
            _ba_max = _get_strategy_param("BUY_A", "breach_max", 30)
            _ba_ob  = _get_strategy_param("BUY_A", "outside_bid_min", 5)
            has_tick_confirm = outside_count >= _ba_ob or large_count >= 1
            if (not _block_buy and _ba_min <= elapsed_min <= _ba_max and has_tick_confirm
                    and not _signal_exists_today(code, "BUY_A")):
                _write_signal(code, "BUY_A", "BUY", current_price,
                              f"假跌破5MA後{elapsed_min:.0f}分拉回，外盤連{outside_count}筆/大單{large_count}")
                triggered.append("BUY_A")
            _breach_times.pop(code, None)
        if code in _breach_times:
            elapsed = (now - _breach_times[code]).total_seconds() / 60
            if elapsed > 35:
                _breach_times.pop(code, None)

    # ── BUY_A fallback: MACD 金叉 + 站上 MA20 + 量比 > 1.5（原邏輯保留）──
    if (not _block_buy and "BUY_A" not in triggered
            and dif_prev < macd_prev and dif_cur >= macd_cur
            and current_price > ma20
            and vol_ratio >= 1.5
            and not _signal_exists_today(code, "BUY_A")):
        _write_signal(code, "BUY_A", "BUY", current_price,
                      f"MACD金叉+站上MA20，量比{vol_ratio:.1f}x")
        triggered.append("BUY_A")

    # ── BUY_B: 量比>2.5x + 連續外盤≥5 + 特大單（tick-level 原始邏輯）──
    if "BUY_B" in _enabled:
        _bb_vol  = _get_strategy_param("BUY_B", "vol_ratio_min", 2.5)
        _bb_ob   = _get_strategy_param("BUY_B", "outside_count", 5)
        if (not _block_buy and vol_ratio >= _bb_vol and outside_count >= _bb_ob and large_count >= 1
                and not _signal_exists_today(code, "BUY_B")):
            _write_signal(code, "BUY_B", "BUY", current_price,
                          f"量價突破：量比{vol_ratio:.1f}x，外盤連{outside_count}筆，大單{large_count}")
            triggered.append("BUY_B")

        # ── BUY_B fallback: MA5 上穿 MA10 + 放量（原邏輯保留）──
        if (not _block_buy and "BUY_B" not in triggered
                and prev5 < prev10 and ma5 >= ma10
                and vol_ratio >= 1.2
                and not _signal_exists_today(code, "BUY_B")):
            _write_signal(code, "BUY_B", "BUY", current_price,
                          f"MA5上穿MA10，量比{vol_ratio:.1f}x")
            triggered.append("BUY_B")

    # ── LOW_BUY (G1): 乖離 MA240 超過 -15%，超跌左側低吸 ──
    if "LOW_BUY" in _enabled:
        _lb_bias = _get_strategy_param("LOW_BUY", "ma240_bias", -15)
        if (not _block_buy and ma240 is not None
                and current_price < ma240 * (1 + _lb_bias / 100)
                and not _signal_exists_today(code, "LOW_BUY")):
            pct = (current_price / ma240 - 1) * 100
            _write_signal(code, "LOW_BUY", "BUY", current_price,
                          f"低於年線{abs(pct):.1f}%，超跌低吸區")
            triggered.append("LOW_BUY")

    # ── EXIT_A: 跌破 VWAP 均價線 N 分鐘無法站回 ──
    _ea_min = _get_strategy_param("EXIT_A", "vwap_fail_min", VWAP_FAIL_MINUTES)
    with _vwap_lock:
        vst = _vwap_state.get(code)
        vwap = vst["cum_pv"] / vst["cum_vol"] if vst and vst["cum_vol"] > 0 else None
    if vwap:
        with _vwap_breach_times_lock:
            if current_price < vwap:
                if code not in _vwap_breach_times:
                    _vwap_breach_times[code] = now
                else:
                    minutes_below = (now - _vwap_breach_times[code]).total_seconds() / 60
                    if (minutes_below >= _ea_min
                            and not _signal_exists_today(code, "EXIT_A")):
                        pct_below = (1 - current_price / vwap) * 100
                        _write_signal(code, "EXIT_A", "SELL", current_price,
                                      f"跌破VWAP均價線${vwap:.1f}持續{minutes_below:.0f}分鐘（低{pct_below:.1f}%），減碼")
                        triggered.append("EXIT_A")
            else:
                _vwap_breach_times.pop(code, None)

    # ── LOCK_BUY: 正乖離 > N%（MA5），鎖定買進 ──
    if "LOCK_BUY" in _enabled:
        _lk_bias = _get_strategy_param("LOCK_BUY", "bias_threshold", 15)
        if (not _block_buy and ma5 > 0
                and ((current_price / ma5) - 1) * 100 > _lk_bias
                and not _signal_exists_today(code, "LOCK_BUY")):
            bias = ((current_price / ma5) - 1) * 100
            _write_signal(code, "LOCK_BUY", "BUY", current_price,
                          f"正乖離{bias:.1f}%超過15%，強勢鎖定買進")
            triggered.append("LOCK_BUY")

    # ── EXIT_B: 高檔爆量出貨（特大單砸內盤） ──
    _eb_lsmin = _get_strategy_param("EXIT_B", "large_sell_min", 1)
    if (large_sell >= _eb_lsmin and vol_ratio >= 2.0
            and not _signal_exists_today(code, "EXIT_B")):
        _write_signal(code, "EXIT_B", "SELL", current_price,
                      f"高檔爆量出貨：內盤大單{large_sell}筆，量比{vol_ratio:.1f}x")
        triggered.append("EXIT_B")

    # ── EXIT_B fallback: MACD 死叉 + 量縮 ──
    if ("EXIT_B" not in triggered
            and dif_prev > macd_prev and dif_cur <= macd_cur
            and vol_ratio < 0.8
            and not _signal_exists_today(code, "EXIT_B")):
        _write_signal(code, "EXIT_B", "SELL", current_price,
                      f"MACD死叉+量縮{vol_ratio:.1f}x，考慮出場")
        triggered.append("EXIT_B")

    # ── EXIT_C: 持倉高點回落超過停損% ──
    _check_exit_c(code, current_price, triggered)

    # ── SQUEEZE_BREAK (G5): 突破近 N 日最高點 + 量比 > 門檻 ──
    if "SQUEEZE_BREAK" in _enabled:
        _sb_days = int(_get_strategy_param("SQUEEZE_BREAK", "high_days", 20))
        _sb_vol  = _get_strategy_param("SQUEEZE_BREAK", "vol_ratio", 2.0)
        highs_20 = _get_highs_from_cache(code, _sb_days)
        if highs_20:
            prev_high = max(highs_20[:-1]) if len(highs_20) > 1 else highs_20[0]
            if (not _block_buy and current_price > prev_high
                    and vol_ratio >= _sb_vol
                    and not _signal_exists_today(code, "SQUEEZE_BREAK")):
                _write_signal(code, "SQUEEZE_BREAK", "BUY", current_price,
                              f"突破{prev_high}前高，量比{vol_ratio:.1f}x")
                triggered.append("SQUEEZE_BREAK")

    # ── Phase 8: SQUEEZE_BUY — 融券軋空 + 突破前日高聯動 ──
    if "SQUEEZE_BUY" in _enabled and not _block_buy and not _signal_exists_today(code, "SQUEEZE_BUY"):
        con_sq = db()
        cur_sq = con_sq.cursor()
        cur_sq.execute("""
            SELECT margin_short_ratio, short_balance, forced_buyback_date
            FROM chip_snapshot WHERE code=?
            ORDER BY date DESC LIMIT 1
        """, (code,))
        sq_row = cur_sq.fetchone()
        con_sq.close()
        _sqb_thr = _get_strategy_param("SQUEEZE_BUY", "msr_threshold", 30)
        if sq_row and sq_row[0] > _sqb_thr and sq_row[1] > 0:
            highs_2 = _get_highs_from_cache(code, 2)
            if highs_2 and len(highs_2) >= 2:
                prev_day_high = highs_2[-2]
                if current_price > prev_day_high:
                    fbd = sq_row[2] or "未知"
                    _write_signal(code, "SQUEEZE_BUY", "BUY", current_price,
                                  f"融券軋空突破！券資比{sq_row[0]:.1f}%，突破前日高{prev_day_high}，回補日{fbd}")
                    triggered.append("SQUEEZE_BUY")

    # ── Phase 8: NEWS_BEARISH — 利多不漲偵測 ──
    if not _signal_exists_today(code, "NEWS_BEARISH"):
        today_str = now.strftime("%Y-%m-%d")
        con_nw = db()
        cur_nw = con_nw.cursor()
        cur_nw.execute("SELECT COUNT(*) FROM news_cache WHERE code=? AND date=? AND sentiment='positive'", (code, today_str))
        pos_count = cur_nw.fetchone()[0]
        con_nw.close()
        if pos_count > 0 and len(closes) >= 2:
            change_pct = (current_price - closes[-1]) / closes[-1] * 100
            _nb_drop = _get_strategy_param("NEWS_BEARISH", "news_drop_pct", 1.0)
            _nb_vol  = _get_strategy_param("NEWS_BEARISH", "news_vol_ratio", 1.5)
            if change_pct < -_nb_drop and vol_ratio > _nb_vol:
                _write_signal(code, "NEWS_BEARISH", "SELL", current_price,
                              f"利多不漲！{pos_count}則正面新聞但收跌{abs(change_pct):.1f}%，量比{vol_ratio:.1f}x，利多出盡")
                triggered.append("NEWS_BEARISH")

    # ── Phase 8: DAYTRADE_WARN — 當沖比過高午盤防洗 ──
    _dt_thr  = _get_strategy_param("DAYTRADE_WARN", "dt_ratio_threshold", 70)
    _dt_hour = _get_strategy_param("DAYTRADE_WARN", "dt_block_hour", 12.5)
    _dt_h, _dt_m = int(_dt_hour), int((_dt_hour % 1) * 60)
    if now.hour > _dt_h or (now.hour == _dt_h and now.minute >= _dt_m):
        con_dt = db()
        cur_dt = con_dt.cursor()
        cur_dt.execute("""
            SELECT daytrade_ratio FROM daytrade_snapshot
            WHERE code=? ORDER BY date DESC LIMIT 1
        """, (code,))
        dt_row = cur_dt.fetchone()
        con_dt.close()
        if dt_row and dt_row[0] > _dt_thr:
            dt_ratio = dt_row[0]
            # 午盤12:30後，當沖比>70% → 阻斷追高 + 跌破VWAP觸發賣出
            if not _signal_exists_today(code, "DAYTRADE_WARN"):
                with _vwap_lock:
                    vst2 = _vwap_state.get(code)
                    vwap2 = vst2["cum_pv"] / vst2["cum_vol"] if vst2 and vst2["cum_vol"] > 0 else None
                if vwap2 and current_price < vwap2:
                    _write_signal(code, "DAYTRADE_WARN", "SELL", current_price,
                                  f"當沖比{dt_ratio:.0f}%過高+午盤跌破VWAP${vwap2:.1f}，當沖客倒貨賣壓")
                    triggered.append("DAYTRADE_WARN")

    return triggered

def _get_vol_from_cache(code: str, n: int = 60) -> list:
    con = db()
    cur = con.cursor()
    cur.execute(
        "SELECT volume FROM kbar_cache WHERE code=? AND tf='D' ORDER BY date_key DESC LIMIT ?",
        (code, n)
    )
    rows = cur.fetchall()
    con.close()
    return [r[0] for r in reversed(rows) if r[0] is not None]

def _get_highs_from_cache(code: str, n: int = 20) -> list:
    con = db()
    cur = con.cursor()
    cur.execute(
        "SELECT high FROM kbar_cache WHERE code=? AND tf='D' ORDER BY date_key DESC LIMIT ?",
        (code, n)
    )
    rows = cur.fetchall()
    con.close()
    return [r[0] for r in reversed(rows) if r[0] is not None]

def _get_ohlcv_from_cache(code: str, n: int = 300, market: str = "TW") -> dict:
    """取最近 n 根完整 OHLCV，回傳 {dates, opens, highs, lows, closes, volumes}"""
    if market == "US":
        try:
            import yfinance as yf
            hist = yf.Ticker(code).history(period="1y", interval="1d")
            if hist.empty:
                return {}
            return {
                "dates":   list(hist.index.strftime("%Y-%m-%d")),
                "opens":   list(hist["Open"]),
                "highs":   list(hist["High"]),
                "lows":    list(hist["Low"]),
                "closes":  list(hist["Close"]),
                "volumes": list(hist["Volume"]),
            }
        except Exception:
            return {}
    con = db()
    cur = con.cursor()
    cur.execute(
        "SELECT date_key, open, high, low, close, volume FROM kbar_cache "
        "WHERE code=? AND tf='D' ORDER BY date_key DESC LIMIT ?",
        (code, n)
    )
    rows = cur.fetchall()
    con.close()
    if not rows:
        try:
            mcon = market_db()
            rows = mcon.execute(
                "SELECT date, open, high, low, close, volume FROM daily_kbar "
                "WHERE code=? AND market=? ORDER BY date DESC LIMIT ?",
                (code, market, n)
            ).fetchall()
            mcon.close()
        except Exception:
            rows = []
    if not rows:
        return {}
    rows = list(reversed(rows))
    return {
        "dates":   [r[0] for r in rows],
        "opens":   [r[1] for r in rows],
        "highs":   [r[2] for r in rows],
        "lows":    [r[3] for r in rows],
        "closes":  [r[4] for r in rows],
        "volumes": [r[5] for r in rows],
    }

def _check_exit_c(code: str, current_price: float, triggered: list):
    """
    Exit_C：移動止盈（追蹤最高價回落）
    波段：利潤達 8% 後，從最高價回落 2% 觸發
    當沖：利潤達 3% 後，從最高價回落 1% 觸發
    若有個別停損價，也同時檢查絕對停損線
    """
    con = db()
    cur = con.cursor()
    cur.execute(
        "SELECT id, code, cost, stop_loss, highest_price, trade_type FROM positions WHERE code=? AND (status='open' OR status IS NULL)",
        (code,)
    )
    positions = cur.fetchall()
    con.close()

    for pos in positions:
        pid, pcode, cost, custom_sl, highest, trade_type = pos
        if not cost or cost <= 0:
            continue
        highest = max(highest or cost, current_price)

        # 更新 highest_price
        if current_price > (highest or 0):
            try:
                c2 = db()
                c2.execute("UPDATE positions SET highest_price=? WHERE id=?", (current_price, pid))
                c2.commit(); c2.close()
            except Exception:
                pass

        # 絕對停損線（G4 個別設定）
        if custom_sl and custom_sl > 0 and current_price <= custom_sl:
            if not _signal_exists_today(pcode, "EXIT_C"):
                _write_signal(pcode, "EXIT_C", "SELL", current_price,
                              f"觸及停損線${custom_sl:.1f}（成本${cost}）")
                triggered.append("EXIT_C")
            continue

        # 移動止盈（使用策略設定參數）
        if trade_type == "波段":
            profit_trigger  = _get_strategy_param("EXIT_C", "swing_profit",   8) / 100
            drawdown_trigger = _get_strategy_param("EXIT_C", "swing_drawdown", 2) / 100
        else:
            profit_trigger  = _get_strategy_param("EXIT_C", "day_profit",   3) / 100
            drawdown_trigger = _get_strategy_param("EXIT_C", "day_drawdown", 1) / 100

        max_profit_pct = (highest - cost) / cost
        if max_profit_pct < profit_trigger:
            continue

        drawdown_from_high = (highest - current_price) / highest
        if (drawdown_from_high >= drawdown_trigger
                and not _signal_exists_today(pcode, "EXIT_C")):
            locked = (current_price - cost) / cost * 100
            _write_signal(pcode, "EXIT_C", "SELL", current_price,
                          f"移動止盈：最高${highest:.1f}(+{max_profit_pct*100:.1f}%) "
                          f"回落{drawdown_from_high*100:.1f}% 鎖住{locked:.1f}%")
            triggered.append("EXIT_C")

# ── EXIT_D 偵測端點 ──────────────────────────────

@app.get("/api/scan/exitd")
def scan_exitd():
    """
    掃描所有持倉，回傳觸發 EXIT_D（緊急停損）的清單。
    EXIT_D：跌幅 ≥ exit_d_threshold%（預設 5%）
    """
    con = db()
    cur = con.cursor()
    cur.execute("SELECT value FROM risk_config WHERE key='exit_d_threshold'")
    row = cur.fetchone()
    threshold = float(row[0]) / 100 if row else 0.05

    cur.execute("SELECT code, name, cost, shares FROM positions WHERE cost > 0 AND (status='open' OR status IS NULL)")
    positions = cur.fetchall()
    con.close()

    if not positions:
        return {"alerts": []}

    api = get_api()
    alerts = []
    for code, name, cost, shares in positions:
        contract = api.Contracts.Stocks.get(code)
        if contract is None:
            continue
        try:
            snaps = _api_call_with_backoff(api.snapshots, [contract])
            if not snaps:
                continue
            price = float(snaps[0].close)
            pnl_pct = (price - cost) / cost
            if pnl_pct <= -threshold:
                alerts.append({
                    "code":    code,
                    "name":    name or code,
                    "cost":    cost,
                    "price":   price,
                    "pnl_pct": round(pnl_pct * 100, 2),
                    "shares":  shares,
                })
                if not _signal_exists_today(code, "EXIT_D"):
                    _write_signal(code, "EXIT_D", "SELL", price,
                                  f"緊急停損觸發：跌幅{abs(pnl_pct*100):.1f}%（成本${cost}）")
        except Exception:
            pass

    return {"alerts": alerts, "threshold_pct": round(threshold * 100, 1)}

# ── 全倉訊號掃描端點 ─────────────────────────────

@app.get("/api/scan/signals")
def scan_all_signals():
    """
    對自選股 + 持倉全部執行訊號引擎，回傳本次新觸發訊號。
    前端可每隔 5 分鐘呼叫一次（盤中）。
    """
    con = db()
    cur = con.cursor()
    cur.execute("SELECT DISTINCT code FROM watchlist UNION SELECT DISTINCT code FROM positions")
    codes = [r[0] for r in cur.fetchall()]
    con.close()

    if not codes:
        return {"triggered": [], "scanned": 0}

    api = get_api()
    contracts = [api.Contracts.Stocks[c] for c in codes if api.Contracts.Stocks.get(c)]
    if not contracts:
        return {"triggered": [], "scanned": 0}

    snaps = _api_call_with_backoff(api.snapshots, contracts)
    snap_map = {str(s.code): float(s.close) for s in snaps}

    all_triggered = []
    for code in codes:
        price = snap_map.get(code)
        if price:
            signals = run_signal_engine(code, price)
            for sig in signals:
                all_triggered.append({"code": code, "signal": sig, "price": price})

    return {"triggered": all_triggered, "scanned": len(codes)}

@app.get("/api/scan/after-hours")
def scan_after_hours():
    """
    盤後回顧模式：用收盤快照對全部自選股+持倉跑訊號引擎。
    不受盤中時間限制，隨時可呼叫。
    回傳本次新觸發的訊號 + 每檔股票的盤後摘要（收盤價、MA位置、MACD方向、量比）。
    """
    con = db()
    cur = con.cursor()
    cur.execute("SELECT DISTINCT code FROM watchlist WHERE market='TW' OR market IS NULL UNION SELECT DISTINCT code FROM positions WHERE (market='TW' OR market IS NULL) AND status='open'")
    codes = [r[0] for r in cur.fetchall()]
    con.close()

    if not codes:
        return {"triggered": [], "summaries": [], "scanned": 0}

    # Try Shioaji snapshot first
    snap_map = {}
    snap_detail = {}
    try:
        api = get_api()
        contracts = [api.Contracts.Stocks[c] for c in codes if api.Contracts.Stocks.get(c)]
        if contracts:
            snaps = _api_call_with_backoff(api.snapshots, contracts)
            for s in snaps:
                close_val = safe(s.close) or 0
                if close_val > 0:
                    snap_map[str(s.code)] = close_val
                    snap_detail[str(s.code)] = {
                        "close": close_val,
                        "change": safe(s.change_price) or 0,
                        "change_pct": safe(s.change_rate) or 0,
                        "volume": int(getattr(s, "total_volume", 0) or 0),
                    }
    except Exception:
        pass

    # Fallback: use kbar_cache last close for codes missing from snapshot
    for code in codes:
        if code not in snap_map:
            closes = _get_closes_from_cache(code, "D", 5)
            if closes:
                snap_map[code] = closes[-1]
                snap_detail[code] = {"close": closes[-1], "change": 0, "change_pct": 0, "volume": 0}

    all_triggered = []
    summaries = []
    for code in codes:
        price = snap_map.get(code)
        if not price:
            continue
        try:
            # 跑訊號引擎
            signals = run_signal_engine(code, price)
            for sig in signals:
                all_triggered.append({"code": code, "signal": sig, "price": price})

            # 產生盤後摘要
            closes = _get_closes_from_cache(code, "D", 300)
            vols = _get_vol_from_cache(code)
            summary = {"code": code, **snap_detail.get(code, {})}

            if len(closes) >= 20:
                s_arr = pd.Series(closes + [price])
                ma5  = round(float(s_arr.rolling(5).mean().iloc[-1]), 2)
                ma20 = round(float(s_arr.rolling(20).mean().iloc[-1]), 2)
                summary["ma5"] = ma5
                summary["ma20"] = ma20
                summary["above_ma5"] = price >= ma5
                summary["above_ma20"] = price >= ma20

                dif_s, macd_s, _ = calc_macd(s_arr.tolist())
                summary["macd_direction"] = "bull" if dif_s[-1] > macd_s[-1] else "bear"
                summary["macd_cross"] = (
                    "golden" if dif_s[-2] <= macd_s[-2] and dif_s[-1] > macd_s[-1]
                    else "death" if dif_s[-2] >= macd_s[-2] and dif_s[-1] < macd_s[-1]
                    else "none"
                )

            if vols and len(vols) >= 20:
                avg20 = sum(vols[-20:]) / 20
                summary["vol_ratio"] = round(vols[-1] / avg20, 2) if avg20 > 0 else 1.0

            # 檢查持倉狀態
            con2 = db()
            cur2 = con2.cursor()
            cur2.execute("SELECT cost, shares, stop_loss FROM positions WHERE code=? AND status='open'", (code,))
            pos_row = cur2.fetchone()
            con2.close()
            if pos_row:
                cost, shares, sl = pos_row
                if cost > 0:
                    summary["pnl_pct"] = round((price - cost) / cost * 100, 2)
                    summary["cost"] = cost
                    summary["shares"] = shares
                    if sl and sl > 0:
                        summary["stop_loss_dist"] = round((price - sl) / price * 100, 2)

            summaries.append(summary)
        except Exception as e:
            summaries.append({"code": code, "error": str(e)})

    return {
        "triggered": all_triggered,
        "summaries": summaries,
        "scanned": len(codes),
        "mode": "after-hours",
    }

# ── Phase 3: 籌碼模組 ────────────────────────────

# ── TWSE 抓取共用層：明確跟隨 307/308 轉址 + 把「節流型 307」標為可重試 ───────────────
# 根因（2026-06-25 cockpit 全量回補 2020 大量 HTTP 307）：TWSE WAF/CDN 在高併發全區間回補時，
# 對該 IP 丟出『307 Temporary Redirect 但不帶 Location』的暫時性節流訊號。Python urllib 的
# HTTPRedirectHandler 只有在帶 Location 時才跟隨；無 Location → 落到 http_error_default →
# 原樣上拋 HTTPError 307。隔離複驗(Py3.12)：2020 日期單打/40 併發 hammer 皆 200，證實 307 是
# 「負載觸發的暫時節流」而非舊日期端點搬移（端點未變）。修法：①帶 Location 的 307/308 照樣跟隨
# 到目標 URL；②無 Location 的節流 307/308 與 403/429/5xx 一律轉 _TwseThrottle → 由 guard 退避重試。
class _TwseThrottle(Exception):
    """TWSE 暫時性節流/轉址訊號（307/308 無 Location、403、429、5xx）。
    獨立型別 → _twse_fetch_with_guard 視為「該退避重試」而非永久失敗。"""
    pass

class _TwseRedirectHandler(urllib.request.HTTPRedirectHandler):
    """帶 Location 的 307/308 → 沿用 urllib 既有邏輯跟隨到目標 URL（GET 無 body，安全）；
    無 Location 的 307/308（TWSE 節流）→ 上拋 _TwseThrottle。只作用於 TWSE opener，不動全域 urllib。"""
    def http_error_307(self, req, fp, code, msg, headers):
        if "location" not in headers and "uri" not in headers:
            raise _TwseThrottle(f"HTTP {code} {msg}: no Location（TWSE 節流）")
        return urllib.request.HTTPRedirectHandler.http_error_302(
            self, req, fp, code, msg, headers)
    http_error_308 = http_error_307

_twse_opener = urllib.request.build_opener(_TwseRedirectHandler)

def _twse_urlopen(url: str, timeout: int = 15) -> dict:
    """TWSE 抓取單一入口：帶 UA、跟隨 307/308 至目標、把節流類 HTTP 錯誤統一轉 _TwseThrottle
    供 guard 退避重試；回傳已 decode 的 JSON dict。非節流錯誤（如 404）原樣上拋（不浪費重試）。"""
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        resp = _twse_opener.open(req, timeout=timeout)
        return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code in (307, 308, 403, 429, 500, 502, 503, 504):
            raise _TwseThrottle(f"HTTP {e.code} {e.reason}（TWSE 節流/轉址）") from e
        raise

def _fetch_twse_institutional(date_str: str, raise_on_error: bool = False) -> list:
    """從台灣證交所抓三大法人買賣超（個股）。
    raise_on_error=True 時，網路/限流等例外會上拋（供歷史回補管線區分「該重試的網路錯誤」
    vs「該跳過的非交易日/未發布」）；stat!=OK（節假日/無資料）仍回 []（不上拋，不該重試）。"""
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date_str}&selectType=ALLBUT0999&response=json"
    try:
        data = _twse_urlopen(url, timeout=15)
        if data.get("stat") != "OK" or "data" not in data:
            return []
        results = []
        for row in data["data"]:
            # 跳過 TWSE T86 回應的「合計/總計」等彙總列：個股代號為 ASCII 英數
            #（純數字，特別股帶英文後綴如 2891B），彙總列代號是中文字（如「合計」），
            # 直接丟給 int() 會炸（invalid literal '合計'）→ 每日 daemon 靜默吞錯、
            # 歷史回補整天 raise。明確比對代號 + try 跳列，雙重保險不讓單列毀掉整天。
            try:
                code = row[0].strip()
                if not (code and code.isascii() and code.isalnum()):
                    continue
                def _int(s): return int(s.replace(",", "").replace(" ", "") or "0")
                # ── 法人籌碼聚合（台股標準慣例，老闆 2026-06-25 核定）──────────
                # 現行 T86 ALLBUT0999 為 19 欄，全部取「淨買賣超」(=買進−賣出，TWSE 已算好)：
                #   [4] 外陸資買賣超(不含外資自營商)   [7] 外資自營商買賣超
                #   [10] 投信買賣超
                #   [14] 自營商買賣超(自行買賣)        [17] 自營商買賣超(避險)
                #   [11] 自營商買賣超合計  [18] 三大法人買賣超合計（皆供健全性檢查用）
                # 外資 = 外資(不含自營商) + 外資自營商；自營商 = 自行買賣 + 避險；投信 = 投信。
                # 健全性：foreign+trust+dealer 應等於 row[18] 三大法人合計。
                foreign_net = _int(row[4]) + _int(row[7])
                trust_net   = _int(row[10])
                dealer_net  = _int(row[14]) + _int(row[17])
                results.append({
                    "code": code,
                    "foreign_buy": foreign_net,   # schema: foreign_buy = 外資買賣超(淨)
                    "itrust_buy": trust_net,      # schema: itrust_buy  = 投信買賣超(淨)
                    "dealer_buy": dealer_net,     # schema: dealer_buy  = 自營商買賣超(淨)
                })
            except (ValueError, IndexError, AttributeError):
                # 個別列格式異常（彙總列/欄位數不符）→ 跳過，零回歸保護
                continue
        return results
    except Exception as e:
        print(f"[籌碼] 法人資料抓取失敗: {e}")
        if raise_on_error:
            raise
        return []

def _fetch_twse_margin(date_str: str, raise_on_error: bool = False) -> list:
    """從台灣證交所抓融資融券餘額。raise_on_error 語意同 _fetch_twse_institutional。"""
    url = f"https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date={date_str}&selectType=STOCK&response=json"
    try:
        data = _twse_urlopen(url, timeout=15)
        if data.get("stat") != "OK":
            return []
        tables = data.get("tables", [])
        if len(tables) < 2:
            return []
        rows = tables[1].get("data", [])
        results = []
        for row in rows:
            # 跳過 MI_MARGN「合計」彙總列：真實回應首列為 ['　','合計',...]，代號欄是
            # 全形空格（strip 後為空）、名稱欄「合計」會被 int() 炸（invalid literal
            # '合計'）→ 回補整天 raise、每日 daemon 靜默吞錯（chip_snapshot 只剩 2 天真因）。
            # 明確比對代號（純數字/英數）+ try 跳列，雙重保險不讓單列毀掉整天。
            try:
                code = row[0].strip()
                if not (code and code.isascii() and code.isalnum()):
                    continue
                def _int(s): return int(s.replace(",", "").replace(" ", "") or "0")
                # ── 融資券欄位對位（現行 MI_MARGN STOCK 個股表為 16 欄，老闆 2026-06-25 核定）──
                #   [1]=名稱（非數字！舊版誤讀 row[1] 致 int() 炸→整日跳光→margin 回 []）
                #   融資: [2]買進 [3]賣出 [4]現金償還 [5]前日餘額 [6]今日餘額 [7]限額
                #   融券: [8]買進 [9]賣出 [10]現券償還 [11]前日餘額 [12]今日餘額 [13]限額
                #   [14]資券互抵 [15]註記。餘額皆取「今日餘額」；券資比 = 融券餘額 / 融資餘額。
                margin_buy     = _int(row[2])
                margin_sell    = _int(row[3])
                margin_balance = _int(row[6])    # 融資今日餘額
                short_buy      = _int(row[8])
                short_sell     = _int(row[9])
                short_balance  = _int(row[12])   # 融券今日餘額
                margin_short_ratio = round(short_balance / margin_balance * 100, 2) if margin_balance > 0 else 0
                results.append({
                    "code": code,
                    "margin_buy": margin_buy,
                    "margin_sell": margin_sell,
                    "margin_balance": margin_balance,
                    "short_buy": short_buy,
                    "short_sell": short_sell,
                    "short_balance": short_balance,
                    "margin_short_ratio": margin_short_ratio,
                })
            except (ValueError, IndexError, AttributeError):
                # 個別列格式異常（彙總列/欄位數不符）→ 跳過，零回歸保護
                continue
        return results
    except Exception as e:
        print(f"[籌碼] 融資融券資料抓取失敗: {e}")
        if raise_on_error:
            raise
        return []

def _fetch_twse_sbl(date_str: str, raise_on_error: bool = False) -> list:
    """從台灣證交所抓借券賣出（SBL）餘額。raise_on_error 語意同 _fetch_twse_margin。

    來源：TWT93U「信用額度總量管制餘額表」——同一張日報表同時揭露融券(欄2-7)與借券(欄8-14)，
    我們只取借券(SBL)三欄。鏡像 _fetch_twse_margin 的 requests/_twse_urlopen 模式，零 OpenBB 依賴。
    歷史深度：自 101(2012)-03-19 新公式起結構穩定(15 欄)，可逐日 date= 回補。盤後揭露 → PIT 對齊次一交易日。"""
    url = f"https://www.twse.com.tw/rwd/zh/marginTrading/TWT93U?date={date_str}&selectType=&response=json"
    try:
        data = _twse_urlopen(url, timeout=15)
        if data.get("stat") != "OK":
            return []
        rows = data.get("data", [])
        results = []
        for row in rows:
            # 跳過「合計/彙總」列：個股代號為 ASCII 英數（純數字 2330 或 ETF 如 00400A），
            # 彙總列代號是中文字會被 int() 炸 → 比對代號 + try 跳列，雙重保險不讓單列毀整天
            #（鏡像 _fetch_twse_margin :2630/:2657）。
            try:
                code = row[0].strip()
                if not (code and code.isascii() and code.isalnum()):
                    continue
                def _int(s): return int(s.replace(",", "").replace(" ", "") or "0")
                # ── TWT93U 欄位對位（15 欄，2026-06-26 接源以實際 JSON pin 死；單位＝股）──────────
                #   融券段（已由 MI_MARGN 取，此處不重複）：[2]前日餘額 [3]賣出 [4]買進 [5]現券
                #                                          [6]今日餘額 [7]次一營業日限額
                #   借券段（SBL，本函式取）：[8]前日餘額 [9]當日賣出 [10]當日還券 [11]當日調整
                #                            [12]當日餘額 [13]次一營業日可限額 [14]備註
                #   驗算：[12]餘額 = [8]前日 + [9]賣出 − [10]還券 + [11]調整（2330 已對帳）。
                sbl_short_sell    = _int(row[9])    # 當日借券賣出（量）
                sbl_return        = _int(row[10])   # 當日還券（量）
                sbl_short_balance = _int(row[12])   # 借券賣出餘額（股）← 主因子來源
                results.append({
                    "code": code,
                    "sbl_short_balance": sbl_short_balance,
                    "sbl_short_sell": sbl_short_sell,
                    "sbl_return": sbl_return,
                })
            except (ValueError, IndexError, AttributeError):
                # 個別列格式異常（彙總列/欄位數不符）→ 跳過，零回歸保護
                continue
        return results
    except Exception as e:
        print(f"[籌碼] 借券賣出(SBL)資料抓取失敗: {e}")
        if raise_on_error:
            raise
        return []

def _upsert_chip_snapshot(date_key: str, inst_list: list, margin_list: list,
                          sbl_list: list = None) -> int:
    """把單日 T86 法人 + 融資券資料 upsert 進 chip_snapshot。
    以 (code, date) 為主鍵 INSERT OR REPLACE → 同日重跑冪等覆寫、不會長出重複列。
    供 _do_fetch_chip（每日排程）與歷史回補管線共用 —— 單日 upsert 邏輯的單一真實來源。
    date_key 須為 'YYYY-MM-DD'。回傳寫入筆數。"""
    inst_map   = {r["code"]: r for r in inst_list}
    margin_map = {r["code"]: r for r in margin_list}
    sbl_map    = {r["code"]: r for r in (sbl_list or [])}   # TW-SBL：借券賣出（可選，None→寫 0）
    all_codes  = set(inst_map.keys()) | set(margin_map.keys()) | set(sbl_map.keys())

    con = db(); cur = con.cursor()
    count = 0
    for code in all_codes:
        inst   = inst_map.get(code, {})
        margin = margin_map.get(code, {})
        sbl    = sbl_map.get(code, {})
        cur.execute("""
            INSERT OR REPLACE INTO chip_snapshot(
                code, date, foreign_buy, itrust_buy, dealer_buy,
                margin_buy, margin_sell, margin_balance,
                short_buy, short_sell, short_balance, margin_short_ratio,
                sbl_short_balance, sbl_short_sell, sbl_return
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            code, date_key,
            inst.get("foreign_buy", 0), inst.get("itrust_buy", 0), inst.get("dealer_buy", 0),
            margin.get("margin_buy", 0), margin.get("margin_sell", 0), margin.get("margin_balance", 0),
            margin.get("short_buy", 0), margin.get("short_sell", 0), margin.get("short_balance", 0),
            margin.get("margin_short_ratio", 0),
            sbl.get("sbl_short_balance", 0), sbl.get("sbl_short_sell", 0), sbl.get("sbl_return", 0),
        ))
        count += 1
    con.commit(); con.close()
    return count

def _do_fetch_chip(date_str: str) -> dict:
    """核心抓取邏輯（供端點與排程共用）。"""
    inst_list   = _fetch_twse_institutional(date_str)
    margin_list = _fetch_twse_margin(date_str)
    sbl_list    = _fetch_twse_sbl(date_str)        # TW-SBL：借券賣出餘額（同盤後揭露，與融資券/法人同寫一筆）

    if not inst_list and not margin_list and not sbl_list:
        return {"ok": False, "message": f"無法取得 {date_str} 資料（可能非交易日或資料未發布）", "count": 0}

    date_key = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    count = _upsert_chip_snapshot(date_key, inst_list, margin_list, sbl_list)
    # 記錄最後自動抓取時間
    try:
        c2 = db()
        c2.execute("INSERT OR REPLACE INTO risk_config(key,value,updated_at) VALUES(?,?,?)",
                   ("chip_last_auto_fetch", f"{date_key} count={count}", datetime.now().isoformat()))
        c2.commit(); c2.close()
    except Exception:
        pass
    return {"ok": True, "date": date_key, "count": count}

@app.post("/api/chip/fetch")
def fetch_chip_data(date_str: str = "", _: None = Depends(require_token)):
    """手動觸發抓取指定日期的籌碼資料（預設今天）"""
    if not date_str:
        date_str = datetime.now().strftime("%Y%m%d")
    return _do_fetch_chip(date_str)

@app.post("/api/tdcc/backfill")
def tdcc_holder_backfill(_: None = Depends(require_token)):
    """D1：手動/週 cron 觸發抓取 TDCC 最新一週集保大戶結構並 upsert tdcc_holder（冪等）。
    ⚠️ TDCC OpenAPI 僅最新一週可得 → 每週呼叫一次向前累積歷史（誠實累積中）。供 cockpit 在 :8766 觸發。"""
    return _backfill_tdcc_holder()

# ── 法人籌碼自動排程 ──────────────────────────────────

_chip_scheduler_next: str = ""   # 下次排程時間（字串，供狀態端點顯示）

def _chip_scheduler_loop():
    """背景執行緒：每個交易日 14:30 自動抓取法人籌碼。"""
    global _chip_scheduler_next
    while True:
        try:
            now    = datetime.now()
            # 計算今日 14:30
            target = now.replace(hour=14, minute=30, second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)
            # 跳過週末
            while target.weekday() >= 5:
                target += timedelta(days=1)
            _chip_scheduler_next = target.strftime("%Y-%m-%d %H:%M")
            sleep_secs = max(1, (target - datetime.now()).total_seconds())
            time.sleep(sleep_secs)
        except Exception:
            time.sleep(60)
            continue

        # 檢查開關
        try:
            con = db(); cur = con.cursor()
            cur.execute("SELECT value FROM risk_config WHERE key='chip_auto_fetch_enabled'")
            row = cur.fetchone(); con.close()
            if row and row[0] == "0":
                print("[自動排程] 法人籌碼自動抓取已停用，跳過")
                continue
        except Exception:
            continue

        now = datetime.now()
        if now.weekday() >= 5:
            continue  # 週末不抓

        date_str = now.strftime("%Y%m%d")
        date_key = now.strftime("%Y-%m-%d")

        # 若今日資料已存在則跳過
        try:
            con = db(); cur = con.cursor()
            cur.execute("SELECT COUNT(*) FROM chip_snapshot WHERE date=?", (date_key,))
            existing = cur.fetchone()[0]; con.close()
            if existing > 0:
                print(f"[自動排程] {date_key} 籌碼已存在（{existing} 筆），跳過")
                continue
        except Exception:
            pass

        print(f"[自動排程] 開始抓取 {date_key} 法人籌碼…")
        try:
            result = _do_fetch_chip(date_str)
            print(f"[自動排程] 完成：{result}")
            if result.get("ok"):
                threading.Thread(
                    target=_send_notification,
                    args=(f"法人籌碼自動更新\n日期：{date_key}\n筆數：{result['count']}",),
                    daemon=True,
                ).start()
        except Exception as e:
            print(f"[自動排程] 抓取失敗: {e}")

threading.Thread(target=_chip_scheduler_loop, daemon=True, name="chip-scheduler").start()

# ── T86 籌碼歷史回補管線（P0-1① · R-CHIP-F 前置）──────────────────
# 目的：把 TWSE 三大法人(T86) + 融資券(MI_MARGN) 歷史灌進 chip_snapshot，
# 讓籌碼因子(inst_net_buy/trust_net_buy/short_squeeze)有長序列可回測（回補後適配器零改動自動生效）。
# 做法：複用單日抓取 _fetch_twse_institutional/_fetch_twse_margin + _upsert_chip_snapshot，
#       只加「迴圈交易日批次 + 護欄(限流/退避重試/增量跳過/冪等/可中途停/進度持久化)」。
# 不重寫單日抓取；不動既有 chip daemon / IC / base-rate。

# TWSE 個股「三大法人買賣超日報表(T86)」最早資料日（個股拆三法人格式自此起）。
# 早於此只有融資券(MI_MARGN, ~2001 起)，但法人欄會是空 → 以 T86 為可回測綁定下限。
_T86_EARLIEST = "2012-05-02"

# 保守限流：≤4 req / 5s（寧可慢不可被封）。所有 backfill 對 TWSE 的請求都先過這個桶。
_rl_twse = _TokenBucket(capacity=4, window_sec=5.0)

_CHIP_BACKFILL_JOBS: dict = {}          # job_id -> live state（in-memory）
_CHIP_BACKFILL_LOCK = threading.Lock()  # 保護 job 計數器的並行更新

def _ensure_chip_backfill_table():
    """進度持久化表（重啟後可查狀態 / 憑增量跳過續跑）。"""
    try:
        con = db(); cur = con.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS chip_backfill_jobs (
                job_id       TEXT PRIMARY KEY,
                start        TEXT,
                end          TEXT,
                market       TEXT,
                status       TEXT,
                total        INTEGER DEFAULT 0,
                done         INTEGER DEFAULT 0,
                filled       INTEGER DEFAULT 0,
                skipped      INTEGER DEFAULT 0,
                no_data      INTEGER DEFAULT 0,
                rows_written INTEGER DEFAULT 0,
                failed_json  TEXT,
                started_at   TEXT,
                completed_at TEXT,
                updated_at   TEXT
            )
        """)
        con.commit(); con.close()
    except Exception as e:
        print(f"[T86回補] 建表失敗: {e}")

def _persist_backfill_job(job_id: str):
    """把 in-memory job 狀態落地（週期性 + 結束時呼叫）。失敗不影響回補本身。"""
    job = _CHIP_BACKFILL_JOBS.get(job_id)
    if not job:
        return
    try:
        con = db(); cur = con.cursor()
        cur.execute("""
            INSERT OR REPLACE INTO chip_backfill_jobs(
                job_id, start, end, market, status, total, done, filled,
                skipped, no_data, rows_written, failed_json, started_at, completed_at, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            job_id, job["start"], job["end"], job["market"], job["status"],
            job["total"], job["done"], job["filled"], job["skipped"], job["no_data"],
            job["rows"], json.dumps(job["failed_dates"], ensure_ascii=False),
            job["started_at"], job.get("completed_at"), datetime.now().isoformat(),
        ))
        con.commit(); con.close()
    except Exception as e:
        print(f"[T86回補] 進度持久化失敗: {e}")

def _reconcile_chip_backfill_on_startup():
    """啟動時把上次「進行中」的 job 標 interrupted（避免重啟後狀態卡 running、且不偷偷 hammer TWSE）。
    要續跑：重新 POST 同一區間即可——增量跳過會略過已回補日，成本極低。"""
    try:
        _ensure_chip_backfill_table()
        con = db(); cur = con.cursor()
        cur.execute(
            "UPDATE chip_backfill_jobs SET status='interrupted', updated_at=? "
            "WHERE status IN ('running','starting')",
            (datetime.now().isoformat(),)
        )
        con.commit(); con.close()
    except Exception as e:
        print(f"[T86回補] 啟動對帳失敗: {e}")

_reconcile_chip_backfill_on_startup()

def _twse_fetch_with_guard(fn, date_str: str, max_retries: int = 6):
    """對單一 TWSE 抓取套護欄：token-bucket 限流 + 指數退避(+jitter)重試。
    只對「網路/限流例外」重試（fn 以 raise_on_error=True 上拋，含 _TwseThrottle：307/308/429/5xx）；
    genuine no-data（節假日/未發布）由 fn 回 [] → 不重試。
    退避：1.5/3/6/12/24s（上限 45s）+ 0~1.5s 抖動 —— 抖動化解 thread pool 重試對齊的
    thundering-herd（cockpit 高併發全區間回補觸發 TWSE 307 暫時節流時，足以撐過節流窗）。重試耗盡才上拋。"""
    last_err = None
    for attempt in range(max_retries):
        wait = _rl_twse.consume(1)
        if wait > 0:
            time.sleep(wait)
        try:
            return fn(date_str, raise_on_error=True)
        except Exception as e:
            last_err = e
            if attempt < max_retries - 1:
                backoff = min(2 ** attempt * 1.5, 45.0) + random.uniform(0, 1.5)
                print(f"[T86回補] {date_str} 抓取第{attempt+1}次失敗({e})，{backoff:.1f}s 後重試")
                time.sleep(backoff)
    raise RuntimeError(f"TWSE fetch failed after {max_retries} retries: {last_err}")

def _chip_date_has_data(date_key: str) -> bool:
    """增量判斷：該日 chip_snapshot 已有資料 → 跳過（不重抓、不覆寫已對的）。"""
    con = db(); cur = con.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM chip_snapshot WHERE date=?", (date_key,))
        return cur.fetchone()[0] > 0
    finally:
        con.close()

def _chip_backfill_one_day(date_str: str):
    """回補單一交易日：抓 T86 法人 + 融資券 → upsert chip_snapshot。
    回 ('ok', n)：寫入 n 檔；('no_data', 0)：節假日/未發布（不算失敗、不重試）。
    網路/限流錯誤經護欄重試後仍失敗 → 上拋（由呼叫端記 failed，整批不中斷）。"""
    inst_list   = _twse_fetch_with_guard(_fetch_twse_institutional, date_str)
    margin_list = _twse_fetch_with_guard(_fetch_twse_margin, date_str)
    sbl_list    = _twse_fetch_with_guard(_fetch_twse_sbl, date_str)   # TW-SBL：與法人/融資券同寫一筆 snapshot
    if not inst_list and not margin_list and not sbl_list:
        return ("no_data", 0)
    date_key = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    n = _upsert_chip_snapshot(date_key, inst_list, margin_list, sbl_list)
    return ("ok", n)

def _chip_backfill_worker(job_id: str, start: str, end: str, market: str,
                          workers: int, force: bool):
    """背景執行緒：迴圈交易日 → 增量跳過 → 抓+upsert（thread pool 並行，限流封頂）。
    單日失敗不中斷整批（記 failed_dates，可日後重試）；可中途 stop；進度週期性持久化。"""
    job = _CHIP_BACKFILL_JOBS[job_id]
    try:
        # 1) 產生交易日清單（跳週末；節假日由 no_data 自然落單，不需本地行事曆）
        dates = []
        d = datetime.strptime(start, "%Y-%m-%d")
        e = datetime.strptime(end, "%Y-%m-%d")
        while d <= e:
            if d.weekday() < 5:                      # 0-4 = 週一~週五
                dates.append(d.strftime("%Y%m%d"))
            d += timedelta(days=1)
        job["total"] = len(dates)
        job["status"] = "running"
        _persist_backfill_job(job_id)

        if not dates:
            job["status"] = "done"
            job["completed_at"] = datetime.now().isoformat()
            _persist_backfill_job(job_id)
            return

        def _process(date_str: str):
            if job.get("stop"):
                return ("stopped", date_str, 0)
            date_key = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            # 增量：已有資料 → 跳過（force=True 才重抓覆寫）
            if not force and _chip_date_has_data(date_key):
                return ("skipped", date_str, 0)
            try:
                status, n = _chip_backfill_one_day(date_str)
                return (status, date_str, n)
            except Exception as ex:
                return ("failed", date_str, str(ex))

        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(_process, ds): ds for ds in dates}
            for f in as_completed(futs):
                kind, ds, payload = f.result()
                with _CHIP_BACKFILL_LOCK:
                    job["done"] += 1
                    if kind == "ok":
                        job["filled"] += 1
                        job["rows"]   += payload
                    elif kind == "skipped":
                        job["skipped"] += 1
                    elif kind == "no_data":
                        job["no_data"] += 1
                    elif kind == "failed":
                        job["failed_dates"].append({"date": ds, "error": payload})
                    # 'stopped' 不計數，只是略過
                    job["current"] = ds
                    done = job["done"]
                if done % 20 == 0:
                    _persist_backfill_job(job_id)

        job["status"] = "stopped" if job.get("stop") else "done"
        job["completed_at"] = datetime.now().isoformat()
        _persist_backfill_job(job_id)
        print(f"[T86回補] job {job_id} {job['status']}: "
              f"filled={job['filled']} skipped={job['skipped']} "
              f"no_data={job['no_data']} failed={len(job['failed_dates'])} rows={job['rows']}")
    except Exception as e:
        job["status"] = "error"
        job["error"] = str(e)
        job["completed_at"] = datetime.now().isoformat()
        _persist_backfill_job(job_id)
        print(f"[T86回補] job {job_id} 異常終止: {e}")

@app.post("/api/chip/backfill")
def chip_backfill_start(config: dict, _: None = Depends(require_token)):
    """啟動 T86/融資券歷史回補背景 job，回 job_id 供輪詢。
    body: {start:'YYYY-MM-DD', end:'YYYY-MM-DD', market:'TWSE', workers:1-4, force:false}
    - 增量：預設 force=false → 已有資料的日期自動跳過（冪等、可中途續跑）。
    - market：目前僅 TWSE（上市）；TPEX 上櫃為 P1（不靜默假成功，明確擋下）。"""
    start  = str(config.get("start", "2020-01-01"))
    end    = str(config.get("end", datetime.now().strftime("%Y-%m-%d")))
    market = str(config.get("market", "TWSE")).upper()
    force  = bool(config.get("force", False))
    try:
        workers = int(config.get("workers", 3))
    except (TypeError, ValueError):
        workers = 3
    workers = min(max(workers, 1), 4)   # 保守上限 4 緒，嚴守 TWSE rate limit（限流桶才是硬封頂）

    # 日期格式 / 區間驗證
    try:
        sdt = datetime.strptime(start, "%Y-%m-%d")
        edt = datetime.strptime(end, "%Y-%m-%d")
    except (TypeError, ValueError):
        return JSONResponse({"ok": False, "error": "start/end 須為 YYYY-MM-DD 格式"}, status_code=400)
    if sdt > edt:
        return JSONResponse({"ok": False, "error": "start 不可晚於 end"}, status_code=400)

    # 市場護欄：目前只支援 TWSE 上市
    if market not in ("TWSE", "TW"):
        return JSONResponse(
            {"ok": False, "error": f"market={market} 尚未支援（目前僅 TWSE 上市；TPEX 上櫃為 P1）"},
            status_code=400)
    market = "TWSE"

    # clamp 到 T86 最早可回補日（早於此法人欄無資料）
    earliest = datetime.strptime(_T86_EARLIEST, "%Y-%m-%d")
    clamped = False
    if sdt < earliest:
        start = _T86_EARLIEST
        clamped = True

    _ensure_chip_backfill_table()
    # job store 上限防呆：超過時淘汰最舊的已結束 job
    if len(_CHIP_BACKFILL_JOBS) >= 100:
        finished = [(jid, j.get("completed_at") or j.get("started_at") or "")
                    for jid, j in _CHIP_BACKFILL_JOBS.items()
                    if j.get("status") in ("done", "stopped", "error", "interrupted")]
        for jid, _ts in sorted(finished, key=lambda x: x[1])[:max(1, len(finished) // 2)]:
            _CHIP_BACKFILL_JOBS.pop(jid, None)

    job_id = secrets.token_hex(8)
    _CHIP_BACKFILL_JOBS[job_id] = {
        "start": start, "end": end, "market": market,
        "status": "starting", "total": 0, "done": 0, "filled": 0,
        "skipped": 0, "no_data": 0, "rows": 0, "current": "",
        "failed_dates": [], "stop": False, "error": None,
        "started_at": datetime.now().isoformat(), "completed_at": None,
    }
    _persist_backfill_job(job_id)
    threading.Thread(target=_chip_backfill_worker,
                     args=(job_id, start, end, market, workers, force),
                     daemon=True, name=f"chip-backfill-{job_id}").start()
    return {"ok": True, "job_id": job_id, "start": start, "end": end, "market": market,
            "workers": workers, "force": force, "clamped_to_earliest": clamped,
            "message": f"Chip backfill started. Poll GET /api/chip/backfill/{job_id}."}

@app.get("/api/chip/backfill/{job_id}")
def chip_backfill_status(job_id: str):
    """輪詢回補進度：已回補N天/總天數/跳過/無資料/失敗清單。
    重啟後 in-memory 丟失時，退回讀持久化表（status=interrupted）。"""
    if (not isinstance(job_id, str) or not (8 <= len(job_id) <= 64)
            or any(c not in "0123456789abcdef" for c in job_id)):
        return JSONResponse({"error": "job not found"}, status_code=404)
    job = _CHIP_BACKFILL_JOBS.get(job_id)
    if job:
        return {
            "job_id": job_id, "status": job["status"],
            "start": job["start"], "end": job["end"], "market": job["market"],
            "total": job["total"], "done": job["done"],
            "filled": job["filled"], "skipped": job["skipped"],
            "no_data": job["no_data"], "rows_written": job["rows"],
            "failed": len(job["failed_dates"]), "failed_dates": job["failed_dates"][:50],
            "current": job.get("current", ""),
            "started_at": job["started_at"], "completed_at": job.get("completed_at"),
            "error": job.get("error"),
        }
    # in-memory 沒有 → 讀持久化表
    try:
        con = db(); cur = con.cursor()
        cur.execute("""SELECT start,end,market,status,total,done,filled,skipped,no_data,
                       rows_written,failed_json,started_at,completed_at FROM chip_backfill_jobs
                       WHERE job_id=?""", (job_id,))
        r = cur.fetchone(); con.close()
        if not r:
            return JSONResponse({"error": "job not found"}, status_code=404)
        try:
            failed_dates = json.loads(r[10] or "[]")
        except Exception:
            failed_dates = []
        return {
            "job_id": job_id, "status": r[3], "start": r[0], "end": r[1], "market": r[2],
            "total": r[4], "done": r[5], "filled": r[6], "skipped": r[7], "no_data": r[8],
            "rows_written": r[9], "failed": len(failed_dates), "failed_dates": failed_dates[:50],
            "started_at": r[11], "completed_at": r[12], "persisted": True,
        }
    except Exception as e:
        return JSONResponse({"error": f"job lookup failed: {e}"}, status_code=500)

@app.post("/api/chip/backfill/{job_id}/stop")
def chip_backfill_stop(job_id: str, _: None = Depends(require_token)):
    """請求中途停止回補（in-flight 的日做完即收手）。"""
    job = _CHIP_BACKFILL_JOBS.get(job_id)
    if not job:
        return JSONResponse({"error": "job not found"}, status_code=404)
    if job["status"] in ("done", "stopped", "error", "interrupted"):
        return {"ok": False, "message": f"job already {job['status']}"}
    job["stop"] = True
    return {"ok": True, "message": "stop requested; will halt after in-flight days"}

# ── P0-2 R-FUND-F：MOPS 月營收歷史回補管線（基本面正交維度，可回補→即時有 base-rate N）──────
# 目的：把 MOPS 月營收(t21sc03 歷史檔) 灌進 revenue_monthly，讓基本面因子 revenue_mom 有長序列可回測
#       （回補後適配器零改動自動生效，與籌碼 P0-1 同精神）。
# 資料源（隔離測證 2026-06-25，cross-check 2330/2317 與官方一致）：
#   https://mopsov.twse.com.tw/nas/t21/{sii|otc}/t21sc03_{roc}_{month}_0.html （Big5；月份無前導零）
#   上市(sii)+上櫃(otc) 皆 2010-01(ROC99) 起；當月/上月/去年當月營收欄位位置跨年(2010→2024)穩定。
# 🔑PIT（最致命）：台股月營收『次月10日前』才公布 → announce_date 保守取「次月10日」，因子進場僅落
#   announce_date 之後交易日（嚴格 >），絕不在營收所屬月當下使用（嚴重未來函數）。見 _revenue_factor_series。
# 做法：複用 chip backfill 護欄模式（限流/退避重試/增量跳過/冪等/可中途停/進度持久化），不重寫單月抓取。

_MOPS_REV_EARLIEST_YM = "2010-01"     # t21sc03 歷史檔最早月份（ROC99；早於此 MOPS 無檔）—— 可回測下限
_rl_mops = _TokenBucket(capacity=3, window_sec=5.0)   # 保守限流：≤3 req/5s（mopsov 靜態檔，寧慢不被封）
_REV_BACKFILL_JOBS: dict = {}          # job_id -> live state（in-memory）
_REV_BACKFILL_LOCK = threading.Lock()  # 保護 job 計數器的並行更新

def _ensure_revenue_table():
    """月營收 PIT 落地表（冪等建表）。period_ym=營收所屬月 'YYYY-MM'；announce_date=保守公布日(次月10日)。
    revenue 單位＝千元(MOPS 原值)；yoy/mom 由原始當月/上月/去年當月營收算出（非檔內最後%欄＝累計YoY）。"""
    try:
        con = db()
        con.execute("""CREATE TABLE IF NOT EXISTS revenue_monthly(
            code          TEXT NOT NULL,
            market        TEXT NOT NULL DEFAULT 'TW',
            period_ym     TEXT NOT NULL,
            revenue       REAL,
            prev_revenue  REAL,
            yoy           REAL,
            mom           REAL,
            announce_date TEXT NOT NULL,
            src           TEXT DEFAULT 'mops_t21',
            updated_at    TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(code, market, period_ym)
        )""")
        con.execute("CREATE INDEX IF NOT EXISTS idx_rev_code ON revenue_monthly(code, market)")
        con.commit(); con.close()
    except Exception as e:
        print(f"[月營收] 建表失敗: {e}")

_ensure_revenue_table()


def _ensure_us_short_interest_table():
    """Type 2 US-SI：美股 FINRA 空單餘額 PIT 落地表（冪等建表，monitor.db，與 chip backfill 同庫慣例）。
    settlement_date=FINRA 申報 as-of 日（半月頻，每月約 2 筆）；publish_date=FINRA 公布日（settlement
    後約8營業日，保守取較晚日防 look-ahead，PIT 錨，存入時算好）。si_ratio=days_to_cover=short_shares/ADV。"""
    try:
        con = db()
        con.execute("""CREATE TABLE IF NOT EXISTS us_short_interest(
            code             TEXT NOT NULL,
            settlement_date  TEXT NOT NULL,
            publish_date     TEXT NOT NULL,
            short_shares     REAL,
            avg_daily_volume REAL,
            days_to_cover    REAL,
            short_pct_float  REAL,
            src              TEXT DEFAULT 'finra_openbb',
            updated_at       TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(code, settlement_date)
        )""")
        con.execute("CREATE INDEX IF NOT EXISTS idx_us_si_code ON us_short_interest(code)")
        con.commit(); con.close()
    except Exception as e:
        print(f"[US-SI] 建表失敗: {e}")

_ensure_us_short_interest_table()


# FINRA settlement → publish 約 8 營業日（保守，寧晚不早防 look-ahead；FINRA 官方對照表細節見報告風險未決）。
_US_SI_PUBLISH_LAG_BDAYS = 8


def _us_si_publish_date(settlement_date: str, lag_bdays: int = _US_SI_PUBLISH_LAG_BDAYS) -> str:
    """🔑PIT 公布日（保守）：FINRA settlement_date('YYYY-MM-DD') + 約 8 個營業日 → publish_date。
    鏡像 _announce_date_for_period 的「保守取較晚公布日」精神：因子進場僅落 publish_date 之後（嚴格 >）
    → 確保『進場 > FINRA 公布日』即使遇假日順延亦成立 → 永不 look-ahead（個股實際公布多早於此 → 只會更晚進場）。"""
    d = datetime.strptime(settlement_date[:10], "%Y-%m-%d").date()
    added = 0
    while added < lag_bdays:
        d += timedelta(days=1)
        if d.weekday() < 5:        # Mon-Fri（不扣美股假日 → 寧可更晚一兩日，PIT 安全方向）
            added += 1
    return d.strftime("%Y-%m-%d")


def _backfill_us_short_interest(codes: list, start: str = "2010-01-01") -> dict:
    """Type 2 US-SI 回補：嘗試用 OpenBB obb.equity.shorts.short_interest（FINRA，免費無 key）回補
    2010+ 美股 SI 到 us_short_interest（monitor.db）。OpenBB 未裝/抓不到 → 誠實回 ok=False（real backfill
    pending），不寫假資料。供 cockpit 在 :8766 沒人用時帶 universe codes 觸發；本函式只負責 fetch+落地。"""
    try:
        from openbb import obb              # noqa: F401（未裝則 ImportError → 誠實降級）
    except Exception as e:
        return {"ok": False, "added": 0, "reason": f"OpenBB 未安裝/不可用：{e}（real FINRA backfill pending）"}
    added = 0; errs = []
    con = db()
    for code in (codes or []):
        try:
            res = obb.equity.shorts.short_interest(symbol=code, provider="finra")
            recs = res.results if hasattr(res, "results") else res
            for row in recs:
                g = (lambda k: getattr(row, k, None) if not isinstance(row, dict) else row.get(k))
                settle = str(g("settlement_date") or g("settlementDate") or "")[:10]
                if not settle:
                    continue
                sh = g("short_volume") or g("current_short_position") or g("short_shares")
                adv = g("average_daily_volume") or g("avg_daily_volume")
                dtc = g("days_to_cover")
                pctf = g("short_percent_of_float") or g("short_pct_float")
                if dtc is None and sh and adv:
                    try: dtc = float(sh) / float(adv)
                    except Exception: dtc = None
                pub = _us_si_publish_date(settle)
                con.execute(
                    "INSERT OR REPLACE INTO us_short_interest"
                    "(code,settlement_date,publish_date,short_shares,avg_daily_volume,days_to_cover,short_pct_float)"
                    " VALUES(?,?,?,?,?,?,?)",
                    (code, settle, pub,
                     float(sh) if sh is not None else None,
                     float(adv) if adv is not None else None,
                     float(dtc) if dtc is not None else None,
                     float(pctf) if pctf is not None else None))
                added += 1
        except Exception as e:
            errs.append(f"{code}:{e}")
    con.commit(); con.close()
    return {"ok": added > 0, "added": added, "errs": errs[:10]}


# ── D1 TDCC：集保大戶持股結構（股權分散表）PIT 落地表 ─────────────────────────────────
# 擴池第一個「持股結構/level」訊號（現有籌碼全是 flow）。鏡像 us_short_interest（greenfield 表，
# settlement_date=每週結算日 + publish_date=公布日，PIT 錨；存入時算好 publish_date）。
# 資料源：TDCC OpenAPI https://openapi.tdcc.com.tw/v1/opendata/1-5（集保戶股權分散表，免費·每週）。
#   欄位（2026-06-27 實打驗證，BOM 注意 key '﻿資料日期'）：證券代號 / 資料日期 / 持股分級 / 股數 /
#   占集保庫存數比例% / 人數。持股分級 1-17：1-15=張數級距、16=差異數調整、17=合計(100%)。
#   級距定義（誠實標記，已對 2330 對帳）：11=200,001-400,000股(≤400張)、12=400,001-600,000、13=600,001-
#   800,000、14=800,001-1,000,000、15=1,000,001股以上(>1000張)。→ 大戶(>400張)=級距 12-15 占比合計；
#   千張大戶(>1000張)=級距 15 占比。
def _ensure_tdcc_holder_table():
    """D1：集保大戶持股結構 PIT 落地表（冪等建表，monitor.db，與 us_short_interest 同庫慣例）。
    settlement_date=股權分散表結算日（每週最後營業日，TDCC '資料日期'）；publish_date=公布日（結算後
    保守 N 營業日，PIT 錨，寧晚不早防 look-ahead）。conc_gt400=大戶(>400張，級距12-15)占集保比率%；
    conc_gt1000=千張大戶(>1000張，級距15)占比%；big_holders=大戶人數；total_holders=總戶數(級距17)。"""
    try:
        con = db()
        con.execute("""CREATE TABLE IF NOT EXISTS tdcc_holder(
            code             TEXT NOT NULL,
            settlement_date  TEXT NOT NULL,
            publish_date     TEXT NOT NULL,
            conc_gt400       REAL,
            conc_gt1000      REAL,
            big_holders      INTEGER,
            total_holders    INTEGER,
            src              TEXT DEFAULT 'tdcc_openapi',
            updated_at       TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(code, settlement_date)
        )""")
        con.execute("CREATE INDEX IF NOT EXISTS idx_tdcc_holder_code ON tdcc_holder(code)")
        con.commit(); con.close()
    except Exception as e:
        print(f"[TDCC] 建表失敗: {e}")

_ensure_tdcc_holder_table()


# TDCC 股權分散表結算後約 N 營業日公布（保守，寧晚不早防 look-ahead）。真實 cadence 約「次週末上線」
# (結算週五 → 約 +1 營業日次週一可見)；取 4 營業日為保守上界，足以覆蓋假日順延，又不犧牲整週新鮮度。
# ⚠️ 誠實標記：實際 TDCC 公布精確時點未逐週對照官方時戳；cockpit 可再校準。寧晚（少數天訊號變舊，PIT 安全）
# 不早（早一天即 look-ahead，致命）。
_TDCC_PUBLISH_LAG_BDAYS = 4

# 大戶級距合計（>400張＝級距 12-15）；千張大戶（>1000張＝級距 15）。級距 16=差異調整、17=合計（不計）。
_TDCC_BIG_LEVELS    = (12, 13, 14, 15)
_TDCC_GT1000_LEVELS = (15,)
_TDCC_OPENAPI_URL   = "https://openapi.tdcc.com.tw/v1/opendata/1-5"


def _tdcc_publish_date(settlement_date: str, lag_bdays: int = _TDCC_PUBLISH_LAG_BDAYS) -> str:
    """🔑PIT 公布日（保守）：settlement_date('YYYY-MM-DD') + 約 N 個營業日 → publish_date。
    鏡像 _us_si_publish_date：因子進場僅落 publish_date 之後（嚴格 >）→ 即使遇假日順延亦『進場 > 公布日』
    成立 → 永不 look-ahead（實際公布多早於此 → 只會更晚進場，PIT 安全方向）。"""
    d = datetime.strptime(settlement_date[:10], "%Y-%m-%d").date()
    added = 0
    while added < lag_bdays:
        d += timedelta(days=1)
        if d.weekday() < 5:        # Mon-Fri（不扣台股假日 → 寧可更晚一兩日，PIT 安全方向）
            added += 1
    return d.strftime("%Y-%m-%d")


def _fetch_tdcc_holder(raise_on_error: bool = False) -> tuple:
    """從 TDCC OpenAPI 抓集保戶股權分散表（全市場最新一週），聚合成每股大戶持股集中度。
    回 (settlement_date 'YYYY-MM-DD', [{code, conc_gt400, conc_gt1000, big_holders, total_holders}, ...])。
    ⚠️ 資料可得性（誠實標記）：openapi.tdcc.com.tw 僅提供『最新一週』（?date= 參數實測被忽略）→ 無免費
    官方 bulk 歷史回補；歷史深度靠每週 cron 向前累積（誠實累積，鏡像 forward monitor）。
    BOM 注意：key '﻿資料日期' 帶 \\ufeff，用 _date_of 容錯取。級距 pct 欄='占集保庫存數比例%'（已 ÷集保庫存）。"""
    try:
        req = urllib.request.Request(_TDCC_OPENAPI_URL, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            rows = json.loads(resp.read().decode("utf-8-sig"))
    except Exception as e:
        print(f"[TDCC] 股權分散表抓取失敗: {e}")
        if raise_on_error:
            raise
        return ("", [])

    def _date_of(r):
        for k, v in r.items():           # key 可能帶 BOM '﻿資料日期'
            if "資料日期" in k:
                return str(v).strip()
        return ""

    def _num(s):
        try: return float(str(s).replace(",", "").replace(" ", "") or "0")
        except (ValueError, TypeError): return 0.0

    def _intnum(s):
        try: return int(float(str(s).replace(",", "").replace(" ", "") or "0"))
        except (ValueError, TypeError): return 0

    settle_raw = ""
    agg = {}   # code -> {conc_gt400, conc_gt1000, big_holders, total_holders}
    for r in rows:
        try:
            code = str(r.get("證券代號", "")).strip()
            if not (code and code.isascii() and code.isalnum()):
                continue                  # 跳過彙總/異常列（雙重保險，鏡像 _fetch_twse_sbl）
            if not settle_raw:
                settle_raw = _date_of(r)
            lv = _intnum(r.get("持股分級"))
            pct = _num(r.get("占集保庫存數比例%"))
            holders = _intnum(r.get("人數"))
            a = agg.setdefault(code, {"conc_gt400": 0.0, "conc_gt1000": 0.0,
                                      "big_holders": 0, "total_holders": 0})
            if lv in _TDCC_BIG_LEVELS:
                a["conc_gt400"] += pct
                a["big_holders"] += holders
            if lv in _TDCC_GT1000_LEVELS:
                a["conc_gt1000"] += pct
            if lv == 17:                  # 級距 17=合計 → 總戶數
                a["total_holders"] = holders
        except (ValueError, AttributeError, TypeError):
            continue                      # 個別列格式異常 → 跳過，零回歸保護
    if not settle_raw or len(settle_raw) != 8:
        return ("", [])
    settlement = f"{settle_raw[:4]}-{settle_raw[4:6]}-{settle_raw[6:8]}"
    results = [{"code": c, **v} for c, v in agg.items()]
    return (settlement, results)


def _backfill_tdcc_holder(raise_on_error: bool = False) -> dict:
    """D1 回補/週更：抓 TDCC 最新一週股權分散表 → 聚合 → upsert tdcc_holder（PK code,settlement_date 冪等）。
    供 cockpit/週 cron 在 :8766 沒人用時觸發。⚠️ 僅最新一週可得（見 _fetch_tdcc_holder）→ 每週呼叫一次
    向前累積歷史（誠實累積中，鏡像 us-si 'real backfill pending'/forward monitor）。回寫入筆數+結算日。"""
    settlement, recs = _fetch_tdcc_holder(raise_on_error=raise_on_error)
    if not settlement or not recs:
        return {"ok": False, "added": 0, "reason": "TDCC 無資料（抓取失敗或格式異常）"}
    pub = _tdcc_publish_date(settlement)
    con = db(); added = 0
    for r in recs:
        try:
            con.execute(
                "INSERT OR REPLACE INTO tdcc_holder"
                "(code,settlement_date,publish_date,conc_gt400,conc_gt1000,big_holders,total_holders)"
                " VALUES(?,?,?,?,?,?,?)",
                (r["code"], settlement, pub,
                 float(r["conc_gt400"]), float(r["conc_gt1000"]),
                 int(r["big_holders"]), int(r["total_holders"])))
            added += 1
        except Exception:
            continue
    con.commit(); con.close()
    return {"ok": added > 0, "added": added, "settlement_date": settlement, "publish_date": pub}


def _ensure_revenue_backfill_table():
    """月營收回補進度持久化表（重啟後可查狀態 / 憑增量跳過續跑）。"""
    try:
        con = db()
        con.execute("""CREATE TABLE IF NOT EXISTS revenue_backfill_jobs(
            job_id TEXT PRIMARY KEY, start_ym TEXT, end_ym TEXT, market TEXT, status TEXT,
            total INTEGER DEFAULT 0, done INTEGER DEFAULT 0, filled INTEGER DEFAULT 0,
            skipped INTEGER DEFAULT 0, no_data INTEGER DEFAULT 0, rows_written INTEGER DEFAULT 0,
            failed_json TEXT, started_at TEXT, completed_at TEXT, updated_at TEXT
        )""")
        con.commit(); con.close()
    except Exception as e:
        print(f"[月營收回補] 建表失敗: {e}")

def _persist_rev_backfill_job(job_id: str):
    """把 in-memory job 狀態落地（週期性 + 結束時）。失敗不影響回補本身。"""
    job = _REV_BACKFILL_JOBS.get(job_id)
    if not job:
        return
    try:
        con = db(); cur = con.cursor()
        cur.execute("""INSERT OR REPLACE INTO revenue_backfill_jobs(
            job_id, start_ym, end_ym, market, status, total, done, filled,
            skipped, no_data, rows_written, failed_json, started_at, completed_at, updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
            job_id, job["start_ym"], job["end_ym"], job["market"], job["status"],
            job["total"], job["done"], job["filled"], job["skipped"], job["no_data"],
            job["rows"], json.dumps(job["failed_periods"], ensure_ascii=False),
            job["started_at"], job.get("completed_at"), datetime.now().isoformat(),
        ))
        con.commit(); con.close()
    except Exception as e:
        print(f"[月營收回補] 進度持久化失敗: {e}")

def _reconcile_revenue_backfill_on_startup():
    """啟動時把上次『進行中』的 job 標 interrupted（避免重啟後卡 running、且不偷偷 hammer MOPS）。
    要續跑：重新 POST 同一區間即可——增量跳過會略過已回補月，成本極低。"""
    try:
        _ensure_revenue_backfill_table()
        con = db(); cur = con.cursor()
        cur.execute("UPDATE revenue_backfill_jobs SET status='interrupted', updated_at=? "
                    "WHERE status IN ('running','starting')", (datetime.now().isoformat(),))
        con.commit(); con.close()
    except Exception as e:
        print(f"[月營收回補] 啟動對帳失敗: {e}")

_reconcile_revenue_backfill_on_startup()


# ── 半自動 Forward Daily Cron run-log（鏡像 revenue_backfill 持久化慣例）────────────────────
# 編排本體在 backend 之外的 ops/forward_daily_cron.py（standalone，不 import 本檔、不加常駐 thread）。
# 此處僅「純加法、唯讀方向」：建表冪等 + 啟動對帳 + 一支唯讀 status endpoint，供 cockpit 查 cron 落地。
# 對既有 forward/search/auto-roll 行為零影響（不被任何既有路徑呼叫）。
def _ensure_forward_cron_table():
    """半自動 forward cron 每日 run-log 持久化表（重啟後可查；同 run_date INSERT OR REPLACE 冪等）。
    放 monitor.db（與 forward ft_* 同庫）。欄位對齊 spec §4.1。"""
    try:
        con = db()
        con.execute("""CREATE TABLE IF NOT EXISTS forward_cron_runs(
            run_date TEXT PRIMARY KEY, started_at TEXT, completed_at TEXT,
            status TEXT, market TEXT, search_run_id TEXT,
            rolled INTEGER DEFAULT 0, rejected_collinear INTEGER DEFAULT 0,
            rejected_gate INTEGER DEFAULT 0, replaced INTEGER DEFAULT 0,
            picks_total INTEGER DEFAULT 0, settled INTEGER DEFAULT 0,
            near_graduation_json TEXT, error TEXT, summary_json TEXT, updated_at TEXT
        )""")
        con.commit(); con.close()
    except Exception as e:
        print(f"[forward-cron] 建表失敗: {e}")

def _reconcile_forward_cron_on_startup():
    """啟動對帳：上次卡在 'running' 的 cron run（例如 prod 重啟或 cron 進程被砍）標 'interrupted'，
    避免重啟後永遠掛 running。要續跑：cron 隔日自然再跑，或人工帶 --force 重觸。"""
    try:
        _ensure_forward_cron_table()
        con = db(); cur = con.cursor()
        cur.execute("UPDATE forward_cron_runs SET status='interrupted', updated_at=? "
                    "WHERE status='running'", (datetime.now().isoformat(),))
        con.commit(); con.close()
    except Exception as e:
        print(f"[forward-cron] 啟動對帳失敗: {e}")

_reconcile_forward_cron_on_startup()


def _announce_date_for_period(period_ym: str) -> str:
    """🔑PIT 公布日（保守）：營收所屬月 period_ym('YYYY-MM') → 次月15日 'YYYY-MM-15'。
    台股月營收法定『次月10日前』公布，但10日遇假日順延至次一營業日（典型：1月營收次月10日撞春節，
    如 2024-02-10 為週六＋春節連假、市場 2/15 才開紅盤 → 法定截止日順延至 2/15）。若用名目「次月10日」，
    晚報股可能在順延後的法定日（如 2/15）才公布，而因子首個進場日也恰落該日 → 同日 look-ahead。
    改取『次月15日』為更保守的安全日（業界月營收 PIT 慣用），即使遇春節順延亦涵蓋，因子進場僅落此日
    之後（嚴格 >）→ 確保『進場日 > 法定(順延後)公布日』即使順延月仍成立 → 永不 look-ahead。
    （個股實際公布日多早於15日，用15日只會更晚進場 → PIT 安全方向；殘留邊界見報告風險未決。）"""
    y, m = int(period_ym[:4]), int(period_ym[5:7])
    ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
    return f"{ny:04d}-{nm:02d}-15"

_YM_RANGE_HARD_CAP = 6000   # ~500 年；遠超任何合理回補區間。保險絲硬上限（防無界成長）

def _ym_range(start_ym: str, end_ym: str) -> list:
    """產生 [start_ym .. end_ym] 的月份清單（含端點，'YYYY-MM'）。
    🔒保險絲(P1-001)：解析後檢查月份須 1..12，否則 raise。月份越界(如 '2024-13')會讓 `m` 永遠
    不等於 12 → 年永不進位 → `(y,m) <= (ey,em)` 在 end 為更晚年份時恆為真 → 無窮迴圈、out 無界成長
    → 吃滿 CPU/記憶體 → OOM kill 整個 process。雖呼叫端(revenue_backfill_start)已用收緊 regex 先擋下，
    此處再加月份檢查＋硬上限兩道保險絲，鎖死任何未來呼叫端再踩到無界迴圈。"""
    out = []
    y, m = int(start_ym[:4]), int(start_ym[5:7])
    ey, em = int(end_ym[:4]), int(end_ym[5:7])
    if not (1 <= m <= 12 and 1 <= em <= 12):
        raise ValueError(f"_ym_range 月份越界（須 01-12）: start={start_ym} end={end_ym}")
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        if len(out) > _YM_RANGE_HARD_CAP:
            raise RuntimeError(f"_ym_range 超過硬上限 {_YM_RANGE_HARD_CAP} 月（區間異常）: "
                               f"start={start_ym} end={end_ym}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out

def _fetch_mops_revenue_month(roc_year: int, month: int, seg: str,
                              raise_on_error: bool = False) -> list:
    """抓 MOPS 單月營收（seg: 'sii'=上市 / 'otc'=上櫃），回 [{code,revenue,prev_revenue,yoy,mom}, ...]。
    YoY/MoM 由原始 當月[2]/上月[3]/去年當月[4] 營收『自算單月值』（檔內最後%欄為累計YoY 非單月，故不採用；
    隔離測證 2330/2317 自算 YoY 與官方一致）。Big5 解碼；產業標題/合計列由『代號須數字開頭』過濾跳過。
    raise_on_error=True：網路/節流例外上拋（供回補區分『該重試』）；404/無資料回 []（不重試）。"""
    import re as _re
    url = f"https://mopsov.twse.com.tw/nas/t21/{seg}/t21sc03_{roc_year}_{month}_0.html"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = _twse_opener.open(req, timeout=30)   # 複用 307 節流防護 opener（無 Location 307 → _TwseThrottle）
        raw = resp.read()
        try:
            html = raw.decode("big5", errors="replace")
        except Exception:
            html = raw.decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []                          # 該月無檔（早於最早月/未來未公布）→ 無資料，不重試
        if raise_on_error:
            raise
        return []
    except Exception:                          # 含 _TwseThrottle（無 Location 307 節流）
        if raise_on_error:
            raise
        return []
    out = []
    for tr in _re.findall(r"<tr[^>]*>(.*?)</tr>", html, _re.S):
        cells = _re.findall(r"<td[^>]*>(.*?)</td>", tr, _re.S)
        if len(cells) < 5:
            continue
        vals = [_re.sub(r"<[^>]+>", "", c).replace("\xa0", " ").replace("&nbsp;", " ").strip()
                for c in cells]
        code = vals[0]
        if not (code and code.isascii() and code.isalnum() and code[0].isdigit()):
            continue                           # 跳過產業標題/合計列（代號非數字開頭）
        def _num(s):
            try:
                return float(s.replace(",", ""))
            except (ValueError, AttributeError):
                return None
        cur_v = _num(vals[2]); prev_v = _num(vals[3]); yb_v = _num(vals[4])
        if cur_v is None:
            continue
        yoy = round((cur_v - yb_v) / yb_v * 100, 2) if (yb_v and yb_v != 0) else None
        mom = round((cur_v - prev_v) / prev_v * 100, 2) if (prev_v and prev_v != 0) else None
        out.append({"code": code, "revenue": cur_v, "prev_revenue": prev_v, "yoy": yoy, "mom": mom})
    if not out and raise_on_error:
        # 200 卻解出 0 列：真無資料是 404（已於上方回 []），200-0列極可能是節流轉址到非 t21sc03 頁
        #（帶 Location 的 307 → opener 跟隨到非資料頁）→ 視為可重試（上拋給 guard 退避重試），
        # 不靜默當 no_data 吞掉造成資料缺口。
        raise _TwseThrottle(f"MOPS {roc_year}/{month} {seg}: HTTP 200 但解析 0 列（疑節流轉址非資料頁），視為可重試")
    return out

def _mops_fetch_with_guard(roc_year: int, month: int, seg: str, max_retries: int = 5):
    """對單月 MOPS 抓取套護欄：token-bucket 限流 + 指數退避(+jitter)重試。
    只對網路/節流例外重試（fn raise_on_error=True 上拋）；404/genuine no-data 由 fn 回 [] → 不重試。"""
    last_err = None
    for attempt in range(max_retries):
        wait = _rl_mops.consume(1)
        if wait > 0:
            time.sleep(wait)
        try:
            return _fetch_mops_revenue_month(roc_year, month, seg, raise_on_error=True)
        except Exception as e:
            last_err = e
            if attempt < max_retries - 1:
                backoff = min(2 ** attempt * 1.5, 30.0) + random.uniform(0, 1.0)
                print(f"[月營收回補] {roc_year}/{month} {seg} 第{attempt+1}次失敗({e})，{backoff:.1f}s 後重試")
                time.sleep(backoff)
    raise RuntimeError(f"MOPS fetch failed after {max_retries} retries: {last_err}")

def _upsert_revenue_monthly(period_ym: str, market: str, seg_rows: dict) -> int:
    """把單月（period_ym）多 seg 的營收列 upsert 進 revenue_monthly。冪等（PK=code,market,period_ym
    → 同月重跑覆寫、不長重複列）。seg_rows: {seg: [row,...]}。announce_date 由 period_ym 推得。回寫入筆數。"""
    announce = _announce_date_for_period(period_ym)
    now = datetime.now().isoformat()
    rows = []
    for seg, lst in seg_rows.items():
        for r in lst:
            rows.append((r["code"], market, period_ym, r["revenue"], r.get("prev_revenue"),
                         r.get("yoy"), r.get("mom"), announce, f"mops_{seg}", now))
    if not rows:
        return 0
    con = db(); cur = con.cursor()
    try:
        cur.executemany("""INSERT OR REPLACE INTO revenue_monthly(
            code, market, period_ym, revenue, prev_revenue, yoy, mom, announce_date, src, updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?)""", rows)
        con.commit()
    finally:
        con.close()
    return len(rows)

def _rev_period_has_data(period_ym: str, market: str = "TW") -> bool:
    """增量判斷：該月 revenue_monthly 已有資料 → 跳過（不重抓、不覆寫已對的）。"""
    con = db(); cur = con.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM revenue_monthly WHERE period_ym=? AND market=?",
                    (period_ym, market))
        return cur.fetchone()[0] > 0
    finally:
        con.close()

def _rev_backfill_one_period(period_ym: str) -> tuple:
    """回補單月：抓 上市(sii)+上櫃(otc) → upsert revenue_monthly。
    回 ('ok', n)：寫 n 檔；('no_data', 0)：未發布/無檔（不算失敗、不重試）。
    網路/節流錯誤經護欄重試後仍失敗 → 上拋（由呼叫端記 failed，整批不中斷）。"""
    y, m = int(period_ym[:4]), int(period_ym[5:7])
    roc = y - 1911
    seg_rows = {seg: _mops_fetch_with_guard(roc, m, seg) for seg in ("sii", "otc")}
    if not any(seg_rows.values()):
        return ("no_data", 0)
    n = _upsert_revenue_monthly(period_ym, "TW", seg_rows)
    return ("ok", n)

def _rev_backfill_worker(job_id: str, start_ym: str, end_ym: str, market: str,
                         workers: int, force: bool):
    """背景執行緒：迴圈月份 → 增量跳過 → 抓+upsert（thread pool 並行，限流封頂）。
    單月失敗不中斷整批（記 failed_periods，可日後重試）；可中途 stop；進度週期性持久化。"""
    job = _REV_BACKFILL_JOBS[job_id]
    try:
        periods = _ym_range(start_ym, end_ym)
        job["total"] = len(periods)
        job["status"] = "running"
        _persist_rev_backfill_job(job_id)
        if not periods:
            job["status"] = "done"; job["completed_at"] = datetime.now().isoformat()
            _persist_rev_backfill_job(job_id); return

        def _process(period_ym: str):
            if job.get("stop"):
                return ("stopped", period_ym, 0)
            if not force and _rev_period_has_data(period_ym, market):
                return ("skipped", period_ym, 0)
            try:
                status, n = _rev_backfill_one_period(period_ym)
                return (status, period_ym, n)
            except Exception as ex:
                return ("failed", period_ym, str(ex))

        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(_process, p): p for p in periods}
            for f in as_completed(futs):
                kind, p, payload = f.result()
                with _REV_BACKFILL_LOCK:
                    job["done"] += 1
                    if kind == "ok":
                        job["filled"] += 1; job["rows"] += payload
                    elif kind == "skipped":
                        job["skipped"] += 1
                    elif kind == "no_data":
                        job["no_data"] += 1
                    elif kind == "failed":
                        job["failed_periods"].append({"period": p, "error": payload})
                    job["current"] = p
                    done = job["done"]
                if done % 6 == 0:
                    _persist_rev_backfill_job(job_id)

        job["status"] = "stopped" if job.get("stop") else "done"
        job["completed_at"] = datetime.now().isoformat()
        _persist_rev_backfill_job(job_id)
        print(f"[月營收回補] job {job_id} {job['status']}: filled={job['filled']} "
              f"skipped={job['skipped']} no_data={job['no_data']} "
              f"failed={len(job['failed_periods'])} rows={job['rows']}")
    except Exception as e:
        job["status"] = "error"; job["error"] = str(e)
        job["completed_at"] = datetime.now().isoformat()
        _persist_rev_backfill_job(job_id)
        print(f"[月營收回補] job {job_id} 異常終止: {e}")

@app.post("/api/revenue/backfill")
def revenue_backfill_start(config: dict, _: None = Depends(require_token)):
    """啟動 MOPS 月營收歷史回補背景 job，回 job_id 供輪詢。
    body: {start_ym:'YYYY-MM', end_ym:'YYYY-MM', workers:1-3, force:false}
    - 增量：預設 force=false → 已有資料的月自動跳過（冪等、可中途續跑）。
    - 上市(sii)+上櫃(otc) 一起回補。clamp 到最早可回補月 2010-01。"""
    start_ym = str(config.get("start_ym", _MOPS_REV_EARLIEST_YM))
    end_ym   = str(config.get("end_ym", datetime.now().strftime("%Y-%m")))
    force    = bool(config.get("force", False))
    try:
        workers = int(config.get("workers", 2))
    except (TypeError, ValueError):
        workers = 2
    workers = min(max(workers, 1), 3)   # 保守上限 3 緒，嚴守 MOPS rate limit（限流桶才是硬封頂）
    import re as _re
    # 🔒P1-001：月份段限 01-12（舊 \d{2} 放行 13-99/00，會讓 _ym_range 年永不進位→無窮迴圈/OOM 打垮:8766）。
    if not (_re.match(r"^\d{4}-(0[1-9]|1[0-2])$", start_ym) and _re.match(r"^\d{4}-(0[1-9]|1[0-2])$", end_ym)):
        return JSONResponse({"ok": False, "error": "start_ym/end_ym 須為 YYYY-MM 格式（月份 01-12）"}, status_code=400)
    if start_ym > end_ym:
        return JSONResponse({"ok": False, "error": "start_ym 不可晚於 end_ym"}, status_code=400)
    clamped = False
    if start_ym < _MOPS_REV_EARLIEST_YM:
        start_ym = _MOPS_REV_EARLIEST_YM; clamped = True
    _ensure_revenue_table(); _ensure_revenue_backfill_table()
    if len(_REV_BACKFILL_JOBS) >= 100:
        finished = [(jid, j.get("completed_at") or j.get("started_at") or "")
                    for jid, j in _REV_BACKFILL_JOBS.items()
                    if j.get("status") in ("done", "stopped", "error", "interrupted")]
        for jid, _ts in sorted(finished, key=lambda x: x[1])[:max(1, len(finished) // 2)]:
            _REV_BACKFILL_JOBS.pop(jid, None)
    job_id = secrets.token_hex(8)
    _REV_BACKFILL_JOBS[job_id] = {
        "start_ym": start_ym, "end_ym": end_ym, "market": "TW",
        "status": "starting", "total": 0, "done": 0, "filled": 0,
        "skipped": 0, "no_data": 0, "rows": 0, "current": "",
        "failed_periods": [], "stop": False, "error": None,
        "started_at": datetime.now().isoformat(), "completed_at": None,
    }
    _persist_rev_backfill_job(job_id)
    threading.Thread(target=_rev_backfill_worker,
                     args=(job_id, start_ym, end_ym, "TW", workers, force),
                     daemon=True, name=f"rev-backfill-{job_id}").start()
    return {"ok": True, "job_id": job_id, "start_ym": start_ym, "end_ym": end_ym,
            "workers": workers, "force": force, "clamped_to_earliest": clamped,
            "message": f"Revenue backfill started. Poll GET /api/revenue/backfill/{job_id}."}

@app.get("/api/revenue/backfill/{job_id}")
def revenue_backfill_status(job_id: str):
    """輪詢回補進度：已回補N月/總月數/跳過/無資料/失敗清單。in-memory 丟失時退回讀持久化表。"""
    if (not isinstance(job_id, str) or not (8 <= len(job_id) <= 64)
            or any(c not in "0123456789abcdef" for c in job_id)):
        return JSONResponse({"error": "job not found"}, status_code=404)
    job = _REV_BACKFILL_JOBS.get(job_id)
    if job:
        return {
            "job_id": job_id, "status": job["status"],
            "start_ym": job["start_ym"], "end_ym": job["end_ym"], "market": job["market"],
            "total": job["total"], "done": job["done"], "filled": job["filled"],
            "skipped": job["skipped"], "no_data": job["no_data"], "rows_written": job["rows"],
            "failed": len(job["failed_periods"]), "failed_periods": job["failed_periods"][:50],
            "current": job.get("current", ""),
            "started_at": job["started_at"], "completed_at": job.get("completed_at"),
            "error": job.get("error"),
        }
    try:
        con = db(); cur = con.cursor()
        cur.execute("""SELECT start_ym,end_ym,market,status,total,done,filled,skipped,no_data,
                       rows_written,failed_json,started_at,completed_at FROM revenue_backfill_jobs
                       WHERE job_id=?""", (job_id,))
        r = cur.fetchone(); con.close()
        if not r:
            return JSONResponse({"error": "job not found"}, status_code=404)
        try:
            failed_periods = json.loads(r[10] or "[]")
        except Exception:
            failed_periods = []
        return {
            "job_id": job_id, "status": r[3], "start_ym": r[0], "end_ym": r[1], "market": r[2],
            "total": r[4], "done": r[5], "filled": r[6], "skipped": r[7], "no_data": r[8],
            "rows_written": r[9], "failed": len(failed_periods), "failed_periods": failed_periods[:50],
            "started_at": r[11], "completed_at": r[12], "persisted": True,
        }
    except Exception as e:
        return JSONResponse({"error": f"job lookup failed: {e}"}, status_code=500)

@app.post("/api/revenue/backfill/{job_id}/stop")
def revenue_backfill_stop(job_id: str, _: None = Depends(require_token)):
    """請求中途停止回補（in-flight 的月做完即收手）。"""
    job = _REV_BACKFILL_JOBS.get(job_id)
    if not job:
        return JSONResponse({"error": "job not found"}, status_code=404)
    if job["status"] in ("done", "stopped", "error", "interrupted"):
        return {"ok": False, "message": f"job already {job['status']}"}
    job["stop"] = True
    return {"ok": True, "message": "stop requested; will halt after in-flight months"}

@app.get("/api/revenue/{code}")
def revenue_series(code: str, limit: int = 36):
    """讀某股已落地的月營收序列（供 AIF cross-check / UI）。回最近 limit 月，含 PIT announce_date。"""
    if not (code and str(code).isalnum()):
        return JSONResponse({"error": "bad code"}, status_code=400)
    try:                                       # 🔒P2-003：clamp 上界（limit=-1 → SQLite LIMIT -1 = 回全部；超大正數 → 無界讀取）
        limit = min(max(int(limit), 1), 240)
    except (TypeError, ValueError):
        limit = 36
    try:
        con = db(); cur = con.cursor()
        rows = cur.execute(
            "SELECT period_ym, revenue, prev_revenue, yoy, mom, announce_date, src "
            "FROM revenue_monthly WHERE code=? AND market='TW' ORDER BY period_ym DESC LIMIT ?",
            (str(code), limit)).fetchall()
        con.close()
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    return {"code": code, "count": len(rows), "series": [
        {"period_ym": r[0], "revenue": r[1], "prev_revenue": r[2], "yoy": r[3],
         "mom": r[4], "announce_date": r[5], "src": r[6]} for r in rows]}

# ── 總經數據自動排程 ──────────────────────────────────

def _macro_scheduler_loop():
    """背景執行緒：每 14 分鐘主動刷新 IC 總經快取（低於 15-min TTL，確保數據常新）。"""
    time.sleep(60)  # 讓 yfinance 等模組先完成初始化
    while True:
        try:
            _fetch_macro_data(force=True)
        except Exception as e:
            print(f"[總經排程] 刷新失敗: {e}")
        time.sleep(840)  # 14 min

threading.Thread(target=_macro_scheduler_loop, daemon=True, name="macro-scheduler").start()

# ── 推薦標的自動排程 ──────────────────────────────────
_rec_scheduler_next: str = ""

def _warm_up_kbars_for_market(mkt: str, codes_with_names: list):
    """補齊指定市場所有股票的日 K 資料。TW: Shioaji→yfinance fallback，US: yfinance live 不需預抓。"""
    if mkt != "TW":
        return
    fetched, skipped, failed = 0, 0, 0
    for code, name, _ in codes_with_names:
        if _cache_key_fresh(code, "D"):
            skipped += 1
            continue
        ok = False
        try:
            df = _fetch_kbars_from_api(code, "D")
            if not df.empty:
                _save_kbars(code, "D", df)
                ok = True
        except Exception:
            pass
        if not ok:
            try:
                import yfinance as yf
                hist = yf.Ticker(f"{code}.TW").history(period="3y", interval="1d")
                if not hist.empty:
                    con2 = db(); cur2 = con2.cursor()
                    now_iso = datetime.now().isoformat()
                    for idx, row in hist.iterrows():
                        key = idx.strftime("%Y-%m-%d")
                        cur2.execute("INSERT OR REPLACE INTO kbar_cache(code,tf,date_key,open,high,low,close,volume,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                            (code, "D", key, float(row["Open"]), float(row["High"]), float(row["Low"]), float(row["Close"]), int(row["Volume"]), now_iso))
                    con2.commit(); con2.close()
                    ok = True
            except Exception:
                pass
        if ok:
            fetched += 1
        else:
            failed += 1
        time.sleep(0.3)
    print(f"[K線補齊] {mkt}: 更新 {fetched} 檔, 已有 {skipped} 檔, 失敗 {failed} 檔")

def _rec_scheduler_loop():
    """每個交易日自動掃描：TW 13:40（收盤後）、US 06:00（盤後）。先補齊 K 線再掃描推薦。"""
    global _rec_scheduler_next
    time.sleep(120)
    while True:
        try:
            con = db(); cur = con.cursor()
            cur.execute("SELECT value FROM risk_config WHERE key='rec_auto_scan_enabled'")
            row = cur.fetchone(); con.close()
            if row and row[0] == "0":
                time.sleep(600)
                continue
        except Exception:
            time.sleep(300)
            continue

        try:
            now = datetime.now()
            tw_target = now.replace(hour=13, minute=40, second=0, microsecond=0)
            us_target = now.replace(hour=6, minute=0, second=0, microsecond=0)

            next_runs = []
            if now < tw_target and now.weekday() < 5:
                next_runs.append(("TW", tw_target))
            if now < us_target and now.weekday() < 5:
                next_runs.append(("US", us_target))

            if not next_runs:
                tomorrow = now + timedelta(days=1)
                while tomorrow.weekday() >= 5:
                    tomorrow += timedelta(days=1)
                next_runs.append(("TW", tomorrow.replace(hour=6, minute=0, second=0)))

            next_runs.sort(key=lambda x: x[1])
            mkt, target = next_runs[0]
            _rec_scheduler_next = f"{target.strftime('%Y-%m-%d %H:%M')} ({mkt})"
            sleep_secs = max(1, (target - datetime.now()).total_seconds())
            time.sleep(sleep_secs)

            if datetime.now().weekday() >= 5:
                continue

            # Step 1: 收集所有需掃描的標的
            con2 = db(); cur2 = con2.cursor()
            cur2.execute("SELECT DISTINCT code, name, market FROM watchlist")
            wl = cur2.fetchall()
            cur2.execute("SELECT DISTINCT code, name, market FROM positions WHERE status='open'")
            pos = cur2.fetchall()
            con2.close()
            pos_fixed = [(c, n, _detect_market(c)) for c, n, m in pos]
            seen = {}
            for r in (wl + pos_fixed):
                seen[r[0]] = r
            for mkt_key, stocks in _MARKET_UNIVERSE.items():
                if mkt_key == mkt:
                    for code, name in stocks:
                        if code not in seen:
                            seen[code] = (code, name, mkt_key)
            cands = [c for c in seen.values() if c[2] == mkt]

            # Step 2: 補齊 K 線資料
            print(f"[推薦排程] 開始補齊 {mkt} K 線資料（{len(cands)} 檔）…")
            _warm_up_kbars_for_market(mkt, cands)

            # Step 3: 掃描推薦
            print(f"[推薦排程] 開始自動掃描 {mkt} 市場…")
            macro = _fetch_macro_data()
            scan_results = []
            now_ts = datetime.now().isoformat()
            for code, name, m in cands:
                try:
                    tech = _ic_score_stock(code, m)
                    if not tech:
                        continue
                    sources = _ic_detect_sources(code, m)
                    confidence = _ic_calc_confidence(tech, sources, macro)
                    scan_results.append({
                        "code": code, "name": name, "market": m,
                        "score": tech["score"], "direction": tech["direction"],
                        "signals": tech["signals"], "indicators": tech["indicators"],
                        "sources": sources, "confidence": confidence,
                        "ai_analysis": "", "disclaimer": "⚠ 以上分析僅供參考",
                        "entry_price": tech.get("price"), "created_at": now_ts,
                    })
                except Exception as e:
                    print(f"[推薦排程] {code} 掃描失敗: {e}")
            scan_results.sort(key=lambda x: (x["direction"]=="BUY", x["score"]), reverse=True)
            with _ic_refresh_lock:
                con3 = db()
                con3.execute(f"DELETE FROM ic_recommendations WHERE market=?", (mkt,))
                for r in scan_results:
                    con3.execute("""INSERT INTO ic_recommendations
                        (market,code,name,direction,score,reasons,indicators,
                         ai_analysis,sources_used,confidence,disclaimer,entry_price,created_at)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                        r["market"], r["code"], r["name"], r["direction"], r["score"],
                        json.dumps(r["signals"], ensure_ascii=False),
                        json.dumps(r["indicators"], ensure_ascii=False),
                        r["ai_analysis"], json.dumps(r["sources"], ensure_ascii=False),
                        r["confidence"], r["disclaimer"], r["entry_price"], r["created_at"],
                    ))
                con3.commit(); con3.close()
            count = len(scan_results)
            buy_count = sum(1 for r in scan_results if r["direction"] == "BUY")
            print(f"[推薦排程] {mkt} 完成：{count} 檔，BUY={buy_count}")
            if buy_count > 0:
                buys = [r for r in scan_results if r["direction"] == "BUY" and r.get("confidence",0) >= 0.6][:5]
                if buys:
                    lines = [f"📊 {mkt} 每日推薦掃描完成\n"]
                    for r in buys:
                        sigs = "、".join(r["signals"][:3]) if r["signals"] else "—"
                        lines.append(f"▶ {r['name']}({r['code']}) 評分{r['score']:.0f} 信心{r['confidence']*100:.0f}%\n  {sigs}")
                    _send_notification("\n".join(lines))

        except Exception as e:
            print(f"[推薦排程] 失敗: {e}")
            time.sleep(300)

threading.Thread(target=_rec_scheduler_loop, daemon=True, name="rec-scheduler").start()

@app.get("/api/ic/rec-scheduler-status")
def rec_scheduler_status():
    con = db(); cur = con.cursor()
    cur.execute("SELECT value FROM risk_config WHERE key='rec_auto_scan_enabled'")
    row = cur.fetchone(); con.close()
    return {
        "enabled": (row[0] if row else "1") == "1",
        "next_scan": _rec_scheduler_next,
    }

@app.post("/api/ic/rec-scheduler/toggle/{state}")
def rec_scheduler_toggle(state: str, _: None = Depends(require_token)):
    val = "1" if state == "on" else "0"
    con = db()
    con.execute("INSERT OR REPLACE INTO risk_config(key,value) VALUES('rec_auto_scan_enabled',?)", (val,))
    con.commit(); con.close()
    return {"ok": True, "enabled": val == "1"}

@app.get("/api/chip/scheduler-status")
def chip_scheduler_status():
    """回傳法人籌碼排程狀態。"""
    con = db(); cur = con.cursor()
    cur.execute("SELECT key, value FROM risk_config WHERE key IN ('chip_auto_fetch_enabled','chip_last_auto_fetch')")
    cfg = {r[0]: r[1] for r in cur.fetchall()}; con.close()
    return {
        "enabled":    cfg.get("chip_auto_fetch_enabled", "1") == "1",
        "next_fetch": _chip_scheduler_next,
        "last_fetch": cfg.get("chip_last_auto_fetch", "尚未執行"),
    }

@app.post("/api/chip/scheduler/toggle/{state}")
def chip_scheduler_toggle(state: str, _: None = Depends(require_token)):
    """開啟(1)或關閉(0)法人籌碼自動排程。"""
    if state not in ("0", "1"):
        raise HTTPException(400, "state 必須為 0 或 1")
    con = db()
    con.execute("INSERT OR REPLACE INTO risk_config(key,value,updated_at) VALUES(?,?,?)",
                ("chip_auto_fetch_enabled", state, datetime.now().isoformat()))
    con.commit(); con.close()
    return {"chip_auto_fetch_enabled": state == "1"}

@app.get("/api/chip/{code}")
def get_chip_history(code: str, days: int = 10):
    """取得個股近 N 日籌碼資料"""
    con = db()
    cur = con.cursor()
    cur.execute("""
        SELECT date, foreign_buy, itrust_buy, dealer_buy,
               margin_balance, short_balance, margin_short_ratio
        FROM chip_snapshot WHERE code=? ORDER BY date DESC LIMIT ?
    """, (code, days))
    rows = cur.fetchall()
    con.close()
    cols = ["date", "foreign_buy", "itrust_buy", "dealer_buy",
            "margin_balance", "short_balance", "margin_short_ratio"]
    return [dict(zip(cols, r)) for r in reversed(rows)]

@app.get("/api/chip/squeeze-candidates")
def squeeze_candidates():
    """融券軋空篩選：券資比>30%, 有融券餘額"""
    con = db()
    cur = con.cursor()
    cur.execute("""
        SELECT code, date, margin_short_ratio, short_balance, margin_balance,
               forced_buyback_date
        FROM chip_snapshot
        WHERE date = (SELECT MAX(date) FROM chip_snapshot)
          AND margin_short_ratio > 30
          AND short_balance > 0
        ORDER BY margin_short_ratio DESC
    """)
    rows = cur.fetchall()
    con.close()
    cols = ["code", "date", "margin_short_ratio", "short_balance",
            "margin_balance", "forced_buyback_date"]
    return [dict(zip(cols, r)) for r in rows]

@app.get("/api/chip/itrust-lock")
def itrust_lock_candidates():
    """投信鎖碼股：近 5 日中至少連買 3 天"""
    con = db()
    cur = con.cursor()
    cur.execute("""
        SELECT code,
               COUNT(*) AS buy_days,
               SUM(itrust_buy) AS total_buy
        FROM chip_snapshot
        WHERE date >= date('now', '+8 hours', '-7 days')
          AND itrust_buy > 0
        GROUP BY code
        HAVING buy_days >= 3
        ORDER BY total_buy DESC
    """)
    rows = cur.fetchall()
    con.close()
    return [{"code": r[0], "buy_days": r[1], "total_buy": r[2]} for r in rows]

@app.get("/api/chip/abandon")
def chip_abandon_signals():
    """籌碼棄守：投信連續 2 日大賣超（賣超 > 500 張）"""
    con = db()
    cur = con.cursor()
    cur.execute("""
        SELECT code, date, itrust_buy
        FROM chip_snapshot
        WHERE date >= date('now', '+8 hours', '-3 days')
        ORDER BY code, date DESC
    """)
    rows = cur.fetchall()
    con.close()

    from itertools import groupby
    results = []
    for code, group in groupby(rows, key=lambda r: r[0]):
        days = list(group)
        if len(days) >= 2 and days[0][2] < -500 and days[1][2] < -500:
            results.append({
                "code": code,
                "day1": {"date": days[0][1], "itrust_buy": days[0][2]},
                "day2": {"date": days[1][1], "itrust_buy": days[1][2]},
            })
    return results

# ── Phase 8: 當沖比數據（TWSE 每日沖銷交易統計）────────

def _fetch_twse_daytrade(date_str: str) -> list:
    """從台灣證交所抓取當日沖銷交易統計"""
    url = f"https://www.twse.com.tw/rwd/zh/trading/TWTB4U?date={date_str}&selectType=All&response=json"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urllib.request.urlopen(req, timeout=15)
        data = json.loads(resp.read().decode("utf-8"))
        if data.get("stat") != "OK" or "data" not in data:
            return []
        results = []
        for row in data["data"]:
            code = row[0].strip()
            def _int(s): return int(s.replace(",", "").replace(" ", "") or "0")
            dt_buy = _int(row[1])
            dt_sell = _int(row[2])
            dt_vol = max(dt_buy, dt_sell)
            total_vol = _int(row[3]) if len(row) > 3 else 0
            ratio = round(dt_vol / total_vol * 100, 2) if total_vol > 0 else 0
            results.append({
                "code": code,
                "daytrade_vol": dt_vol,
                "total_vol": total_vol,
                "daytrade_ratio": ratio,
            })
        return results
    except Exception as e:
        print(f"[Phase8] 當沖比資料抓取失敗: {e}")
        return []

@app.post("/api/chip/fetch-daytrade")
def fetch_daytrade_data(date_str: str = "", _: None = Depends(require_token)):
    """抓取當沖比數據並存入 DB"""
    if not date_str:
        date_str = datetime.now().strftime("%Y%m%d")
    dt_list = _fetch_twse_daytrade(date_str)
    if not dt_list:
        return {"ok": False, "message": f"無法取得 {date_str} 當沖比資料", "count": 0}
    date_key = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    con = db()
    count = 0
    for r in dt_list:
        con.execute("""
            INSERT OR REPLACE INTO daytrade_snapshot(code, date, daytrade_vol, total_vol, daytrade_ratio)
            VALUES(?,?,?,?,?)
        """, (r["code"], date_key, r["daytrade_vol"], r["total_vol"], r["daytrade_ratio"]))
        count += 1
    con.commit()
    con.close()
    return {"ok": True, "date": date_key, "count": count}

@app.get("/api/chip/daytrade/{code}")
def get_daytrade_history(code: str, days: int = 10):
    """取得個股近 N 日當沖比"""
    con = db()
    cur = con.cursor()
    cur.execute("""
        SELECT date, daytrade_vol, total_vol, daytrade_ratio
        FROM daytrade_snapshot WHERE code=? ORDER BY date DESC LIMIT ?
    """, (code, days))
    rows = cur.fetchall()
    con.close()
    cols = ["date", "daytrade_vol", "total_vol", "daytrade_ratio"]
    return [dict(zip(cols, r)) for r in reversed(rows)]

@app.get("/api/chip/daytrade-warn")
def daytrade_warn_candidates():
    """當沖比>50%高危名單（用於午盤防洗）"""
    con = db()
    cur = con.cursor()
    cur.execute("""
        SELECT code, daytrade_ratio, daytrade_vol, total_vol
        FROM daytrade_snapshot
        WHERE date = (SELECT MAX(date) FROM daytrade_snapshot)
          AND daytrade_ratio > 50
        ORDER BY daytrade_ratio DESC
    """)
    rows = cur.fetchall()
    con.close()
    return [{"code": r[0], "daytrade_ratio": r[1], "daytrade_vol": r[2], "total_vol": r[3]} for r in rows]


# ── Phase 8: 新聞/重大訊息偵測（關鍵字情感分析）────────

_POSITIVE_KEYWORDS = [
    "營收創新高", "獲利成長", "營收年增", "毛利率提升", "EPS創新高",
    "接獲大單", "法說會利多", "上修目標", "調升評等", "轉盈",
    "股利創高", "營收月增", "出貨暢旺", "產能滿載", "需求強勁",
    "突破新高", "漲停", "法人看好", "外資買超",
]

_NEGATIVE_KEYWORDS = [
    "營收衰退", "獲利下滑", "毛利率下降", "虧損擴大", "下修目標",
    "調降評等", "出貨遞延", "產能利用率下滑", "需求疲弱",
    "跌停", "警示股", "處置股", "違約交割",
]

def _analyze_sentiment(text: str) -> str:
    """簡易關鍵字情感分析"""
    pos = sum(1 for kw in _POSITIVE_KEYWORDS if kw in text)
    neg = sum(1 for kw in _NEGATIVE_KEYWORDS if kw in text)
    if pos > neg:
        return "positive"
    elif neg > pos:
        return "negative"
    return "neutral"

def _fetch_twse_material_info() -> list:
    """從公開資訊觀測站抓取重大訊息（MOPS 即時重大訊息）"""
    url = "https://mops.twse.com.tw/mops/web/ajax_t05st01"
    try:
        post_data = urllib.parse.urlencode({
            "encodeURIComponent": "1",
            "step": "1",
            "firstin": "1",
            "off": "1",
            "TYPEK": "all",
        }).encode("utf-8")
        req = urllib.request.Request(url, data=post_data, headers={
            "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/x-www-form-urlencoded",
        })
        resp = urllib.request.urlopen(req, timeout=15)
        html = resp.read().decode("utf-8")
        import re
        rows = re.findall(r'<td[^>]*>(\d{4})</td>\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>', html)
        results = []
        for code, name, headline in rows:
            headline = re.sub(r'<[^>]+>', '', headline).strip()
            if headline:
                results.append({
                    "code": code.strip(),
                    "name": name.strip(),
                    "headline": headline,
                    "sentiment": _analyze_sentiment(headline),
                })
        return results
    except Exception as e:
        print(f"[Phase8] 重大訊息抓取失敗: {e}")
        return []

def _fetch_yahoo_tw_news(code: str) -> list:
    """從 Yahoo 台灣股市抓取個股新聞標題"""
    url = f"https://tw.stock.yahoo.com/quote/{code}/news"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urllib.request.urlopen(req, timeout=10)
        html = resp.read().decode("utf-8")
        import re
        titles = re.findall(r'"title"\s*:\s*"([^"]{10,100})"', html)
        seen = set()
        results = []
        for t in titles[:10]:
            t = t.strip()
            if (t not in seen) and (code in t or len(results) < 5):
                seen.add(t)
                results.append({
                    "headline": t,
                    "title": t,
                    "date": datetime.now().strftime("%Y-%m-%d"),
                    "sentiment": _analyze_sentiment(t),
                    "source": "Yahoo",
                })
        return results
    except Exception:
        return []

@app.get("/api/news/{code}")
def get_news(code: str):
    """取得個股新聞 + 情感分析"""
    yahoo = _fetch_yahoo_tw_news(code)
    con = db()
    cur = con.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    cur.execute("SELECT headline, sentiment, source FROM news_cache WHERE code=? AND date=?", (code, today))
    cached = [{"headline": r[0], "title": r[0], "date": today, "sentiment": r[1], "source": r[2]} for r in cur.fetchall()]
    con.close()
    all_news = cached + yahoo
    positive_count = sum(1 for n in all_news if n["sentiment"] == "positive")
    negative_count = sum(1 for n in all_news if n["sentiment"] == "negative")
    return {
        "code": code,
        "news": all_news[:10],
        "positive_count": positive_count,
        "negative_count": negative_count,
        "overall": "positive" if positive_count > negative_count else "negative" if negative_count > positive_count else "neutral",
    }

@app.post("/api/news/fetch-material")
def fetch_material_info():
    """抓取公開資訊觀測站重大訊息並存入 DB"""
    items = _fetch_twse_material_info()
    if not items:
        return {"ok": False, "message": "無法取得重大訊息", "count": 0}
    today = datetime.now().strftime("%Y-%m-%d")
    now_str = datetime.now().isoformat()
    con = db()
    count = 0
    for item in items:
        con.execute("""
            INSERT OR IGNORE INTO news_cache(code, date, headline, sentiment, source, fetched_at)
            VALUES(?,?,?,?,?,?)
        """, (item["code"], today, item["headline"], item["sentiment"], "MOPS", now_str))
        count += 1
    con.commit()
    con.close()
    return {"ok": True, "count": count}

@app.get("/api/news/bearish-reversal")
def news_bearish_reversal():
    """利多不漲偵測：正面新聞 + 開高走低量大收黑"""
    today = datetime.now().strftime("%Y-%m-%d")
    con = db()
    cur = con.cursor()
    cur.execute("""
        SELECT DISTINCT code FROM news_cache
        WHERE date=? AND sentiment='positive'
    """, (today,))
    positive_codes = [r[0] for r in cur.fetchall()]
    con.close()

    results = []
    for code in positive_codes:
        closes = _get_closes_from_cache(code, "D", 5)
        if len(closes) < 2:
            continue
        vols = _get_vol_from_cache(code, 5)
        if not vols or len(vols) < 2:
            continue
        today_close = closes[-1]
        prev_close = closes[-2]
        change_pct = (today_close - prev_close) / prev_close * 100
        vol_ratio = vols[-1] / vols[-2] if vols[-2] > 0 else 1
        if change_pct < -1 and vol_ratio > 1.5:
            results.append({
                "code": code,
                "change_pct": round(change_pct, 2),
                "vol_ratio": round(vol_ratio, 2),
                "signal": "NEWS_BEARISH",
                "detail": f"利多不漲：正面新聞但收跌{abs(change_pct):.1f}%，量比{vol_ratio:.1f}x",
            })
    return results

# ── Phase 8: 融券軋空盤中聯動 ────────────────────────

@app.get("/api/chip/squeeze-breakout")
def squeeze_breakout_candidates():
    """融券軋空 + 盤中突破前日高 = 強力買訊"""
    con = db()
    cur = con.cursor()
    cur.execute("""
        SELECT code, margin_short_ratio, short_balance, forced_buyback_date
        FROM chip_snapshot
        WHERE date = (SELECT MAX(date) FROM chip_snapshot)
          AND margin_short_ratio > 30
          AND short_balance > 0
        ORDER BY margin_short_ratio DESC
    """)
    squeeze_list = cur.fetchall()
    con.close()

    results = []
    for code, msr, sb, fbd in squeeze_list:
        closes = _get_closes_from_cache(code, "D", 5)
        if len(closes) < 2:
            continue
        prev_high_list = _get_highs_from_cache(code, 2)
        if not prev_high_list or len(prev_high_list) < 2:
            continue
        prev_day_high = prev_high_list[-2]
        current = closes[-1]
        broke_out = current > prev_day_high
        results.append({
            "code": code,
            "margin_short_ratio": msr,
            "short_balance": sb,
            "forced_buyback_date": fbd,
            "prev_day_high": prev_day_high,
            "current_price": current,
            "broke_out": broke_out,
            "signal": "SQUEEZE_BUY" if broke_out else None,
        })
    return results


@app.get("/api/tick-stats/{code}")
def get_tick_stats(code: str):
    """Phase 4: 取得個股 tick-level 追蹤統計"""
    with _tick_buf_lock:
        buf = _tick_buf.get(code)
    if not buf:
        return {"code": code, "message": "尚無 tick 資料"}
    return {
        "code": code,
        "date": buf["date"],
        "outside_bid_count": buf["outside_bid_count"],
        "large_order_count": buf["large_order_count"],
        "large_sell_count": buf["large_sell_count"],
        "tick_count": buf["tick_count"],
        "total_vol": buf["total_vol"],
    }

# ── Phase 5: 自動停損下單 ────────────────────────────

def _execute_sell_order(pos_id: int, code: str, shares: int, reason: str,
                        config_key: str = "auto_sell_enabled") -> dict:
    """
    執行市價賣出，帶有冪等性保護：
    1. config_key（預設 auto_sell_enabled，EXIT_C 傳 auto_sell_exitc_enabled）必須為 1
    2. 在提交訂單前先將持倉標記為 pending_sell（DB 事務），防止並發重複下單
    3. SJ_PRODUCTION=true 時才會真正下單，並記錄 sell_order_id
    4. 模擬模式下直接關閉（無真實訂單需確認）；正式模式等 fill 後由回調關閉
    5. 下單後推播通知
    """
    con = db()
    cur = con.cursor()
    cur.execute("SELECT value FROM risk_config WHERE key=?", (config_key,))
    row = cur.fetchone()
    enabled = row and row[0] == "1"

    if not enabled:
        con.close()
        return {"executed": False, "reason": f"{config_key} 未開啟", "code": code}

    # 冪等性：若持倉已是 pending_sell / closed，跳過
    cur.execute("SELECT status FROM positions WHERE id=?", (pos_id,))
    pos_row = cur.fetchone()
    if not pos_row or pos_row[0] in ("pending_sell", "closed"):
        con.close()
        return {"executed": False, "reason": f"持倉 {pos_id} 已是 {pos_row[0] if pos_row else 'unknown'}，跳過", "code": code}

    # 在提交訂單前先鎖定持倉（pending_sell），防止並發重複下單
    cur.execute(
        "UPDATE positions SET status='pending_sell', sell_reason=?, updated_at=? WHERE id=? AND status='open'",
        (reason, datetime.now().isoformat(), pos_id)
    )
    if cur.rowcount == 0:
        con.close()
        return {"executed": False, "reason": f"持倉 {pos_id} 搶先被其他請求鎖定，跳過", "code": code}
    con.commit()
    con.close()

    is_production = os.getenv("SJ_PRODUCTION", "").lower() == "true"
    result = {"code": code, "shares": shares, "reason": reason, "production": is_production, "pos_id": pos_id}

    if is_production:
        try:
            import shioaji as sj
            api = get_api()
            contract = api.Contracts.Stocks.get(code)
            if contract is None:
                # 回滾 pending_sell → open（找不到合約，無法下單）
                con2 = db()
                con2.execute("UPDATE positions SET status='open', sell_reason=NULL, updated_at=? WHERE id=?",
                             (datetime.now().isoformat(), pos_id))
                con2.commit()
                con2.close()
                result["executed"] = False
                result["error"] = f"找不到合約 {code}"
                return result
            order = api.Order(
                price=0,
                quantity=shares,
                action=sj.constant.Action.Sell,
                price_type=sj.constant.StockPriceType.MKT,
                order_type=sj.constant.TFTOrderType.ROD,
            )
            trade = api.place_order(contract, order)
            order_id = trade.order.id if trade and trade.order else None
            trade_status = str(trade.status.status) if trade else "unknown"
            # 記錄訂單 ID；position 維持 pending_sell，等待 fill callback 真正關閉
            con2 = db()
            con2.execute(
                "UPDATE positions SET sell_order_id=?, updated_at=? WHERE id=?",
                (order_id, datetime.now().isoformat(), pos_id)
            )
            con2.commit()
            con2.close()
            result["executed"] = True
            result["order_id"] = order_id
            result["trade_status"] = trade_status
        except Exception as e:
            # 下單失敗，回滾 pending_sell → open
            try:
                con2 = db()
                con2.execute("UPDATE positions SET status='open', sell_reason=NULL, sell_order_id=NULL, updated_at=? WHERE id=?",
                             (datetime.now().isoformat(), pos_id))
                con2.commit()
                con2.close()
            except Exception:
                pass
            result["executed"] = False
            result["error"] = str(e)
    else:
        # 模擬模式：直接關閉（無真實訂單）
        con2 = db()
        con2.execute("UPDATE positions SET status='closed', updated_at=? WHERE id=?",
                     (datetime.now().isoformat(), pos_id))
        con2.commit()
        con2.close()
        result["executed"] = True
        result["simulated"] = True
        print(f"[模擬下單] 賣出 {code} x {shares}張 — {reason}")

    threading.Thread(
        target=_send_notification,
        args=(f"自動停損賣出\n股票：{code}\n張數：{shares}\n原因：{reason}\n{'正式（等待成交確認）' if is_production else '模擬'}",),
        daemon=True,
    ).start()

    return result

@app.post("/api/auto-sell/execute")
def auto_sell_execute(_: None = Depends(require_token)):
    """
    掃描所有 status='open' 持倉，對觸發 EXIT_D 的持倉自動送出市價賣單。
    安全閥：auto_sell_enabled 必須為 1；模擬環境下只記錄不下單。
    冪等性：已是 pending_sell/closed 的持倉自動跳過，防止並發重複下單。
    """
    con = db()
    cur = con.cursor()
    cur.execute("SELECT value FROM risk_config WHERE key='exit_d_threshold'")
    row = cur.fetchone()
    threshold = float(row[0]) / 100 if row else 0.05

    # 只取 status='open' 的持倉（pending_sell 已在處理中，不重複下單）
    cur.execute("SELECT id, code, name, cost, shares FROM positions WHERE cost > 0 AND status='open'")
    positions = cur.fetchall()
    con.close()

    if not positions:
        return {"executed": [], "message": "無持倉"}

    api = get_api()
    executed = []
    for pos_id, code, name, cost, shares in positions:
        contract = api.Contracts.Stocks.get(code)
        if contract is None:
            continue
        try:
            snaps = _api_call_with_backoff(api.snapshots, [contract])
            if not snaps:
                continue
            price = float(snaps[0].close)
            pnl_pct = (price - cost) / cost
            if pnl_pct <= -threshold:
                reason = f"EXIT_D 停損：跌幅{abs(pnl_pct*100):.1f}%（成本${cost}->現價${price}）"
                result = _execute_sell_order(pos_id, code, shares, reason)
                if result.get("executed") and not _signal_exists_today(code, "EXIT_D"):
                    _write_signal(code, "EXIT_D", "SELL", price, reason)
                executed.append(result)
        except Exception as e:
            executed.append({"code": code, "error": str(e), "executed": False})

    return {"executed": executed, "threshold_pct": round(threshold * 100, 1)}

@app.post("/api/auto-sell/exit-c")
def auto_sell_exit_c(_: None = Depends(require_token)):
    """
    掃描所有 status='open' 台股持倉，對觸發 EXIT_C（移動止盈）的持倉自動送出市價賣單。
    安全閥：auto_sell_exitc_enabled 必須為 1。
    冪等性：已是 pending_sell/closed 的持倉自動跳過。
    """
    con = db()
    cur = con.cursor()
    cur.execute("SELECT value FROM risk_config WHERE key='auto_sell_exitc_enabled'")
    row = cur.fetchone()
    if not row or row[0] != "1":
        con.close()
        return {"executed": [], "message": "EXIT_C 自動止盈未開啟"}

    cur.execute(
        "SELECT id, code, name, cost, shares, highest_price, trade_type "
        "FROM positions WHERE cost > 0 AND status='open'"
    )
    positions = cur.fetchall()
    con.close()

    if not positions:
        return {"executed": [], "message": "無持倉"}

    api = get_api()
    executed = []
    for pos_id, code, name, cost, shares, highest, trade_type in positions:
        contract = api.Contracts.Stocks.get(code)
        if contract is None:
            continue
        try:
            snaps = _api_call_with_backoff(api.snapshots, [contract])
            if not snaps:
                continue
            current_price = float(snaps[0].close)

            # 更新 highest_price
            effective_high = max(highest or cost, current_price)
            if current_price > (highest or 0):
                c2 = db()
                c2.execute("UPDATE positions SET highest_price=? WHERE id=?", (effective_high, pos_id))
                c2.commit(); c2.close()

            # EXIT_C 條件（與 _check_exit_c 相同邏輯）
            if trade_type == "波段":
                profit_trigger   = _get_strategy_param("EXIT_C", "swing_profit",   8) / 100
                drawdown_trigger = _get_strategy_param("EXIT_C", "swing_drawdown", 2) / 100
            else:
                profit_trigger   = _get_strategy_param("EXIT_C", "day_profit",   3) / 100
                drawdown_trigger = _get_strategy_param("EXIT_C", "day_drawdown", 1) / 100

            max_profit_pct    = (effective_high - cost) / cost
            if max_profit_pct < profit_trigger:
                continue

            drawdown_from_high = (effective_high - current_price) / effective_high
            if drawdown_from_high < drawdown_trigger:
                continue

            locked = (current_price - cost) / cost * 100
            reason = (f"EXIT_C 移動止盈：最高${effective_high:.1f}(+{max_profit_pct*100:.1f}%) "
                      f"回落{drawdown_from_high*100:.1f}% 鎖利{locked:.1f}%")
            result = _execute_sell_order(pos_id, code, shares, reason,
                                         config_key="auto_sell_exitc_enabled")
            if result.get("executed") and not _signal_exists_today(code, "EXIT_C"):
                _write_signal(code, "EXIT_C", "SELL", current_price, reason)
            executed.append(result)
        except Exception as e:
            executed.append({"code": code, "error": str(e), "executed": False})

    return {"executed": executed}

@app.get("/api/auto-sell/status")
def auto_sell_status():
    """查看自動停損/止盈開關狀態"""
    con = db()
    cur = con.cursor()
    cur.execute("SELECT key, value FROM risk_config WHERE key IN ('auto_sell_enabled','auto_sell_exitc_enabled')")
    rows = {r[0]: r[1] for r in cur.fetchall()}
    con.close()
    is_prod = os.getenv("SJ_PRODUCTION", "").lower() == "true"
    return {
        "auto_sell_enabled":       rows.get("auto_sell_enabled",       "0") == "1",
        "auto_sell_exitc_enabled": rows.get("auto_sell_exitc_enabled", "0") == "1",
        "production": is_prod,
    }

@app.post("/api/auto-sell/toggle/{state}")
def toggle_auto_sell(state: str, _: None = Depends(require_token)):
    """開啟(1)或關閉(0) EXIT_D 自動停損"""
    if state not in ("0", "1"):
        raise HTTPException(400, "state 必須為 0 或 1")
    con = db()
    con.execute("INSERT OR REPLACE INTO risk_config(key,value,updated_at) VALUES(?,?,?)",
                ("auto_sell_enabled", state, datetime.now().isoformat()))
    con.commit()
    con.close()
    return {"auto_sell_enabled": state == "1"}

@app.post("/api/auto-sell/toggle-exitc/{state}")
def toggle_auto_sell_exitc(state: str, _: None = Depends(require_token)):
    """開啟(1)或關閉(0) EXIT_C 自動移動止盈"""
    if state not in ("0", "1"):
        raise HTTPException(400, "state 必須為 0 或 1")
    con = db()
    con.execute("INSERT OR REPLACE INTO risk_config(key,value,updated_at) VALUES(?,?,?)",
                ("auto_sell_exitc_enabled", state, datetime.now().isoformat()))
    con.commit()
    con.close()
    return {"auto_sell_exitc_enabled": state == "1"}

# ── 美股模組 (yfinance) ──────────────────────────

def _yf_kbars(symbol: str, tf: str = "D") -> list:
    """用 yfinance 取得美股 K 線，回傳 list of dict"""
    try:
        import yfinance as yf
    except ImportError:
        return []
    period_map = {"D": ("6mo", "1d"), "60": ("1mo", "1h"), "5": ("5d", "5m")}
    period, interval = period_map.get(tf, ("6mo", "1d"))
    try:
        tk = yf.Ticker(symbol)
        df = tk.history(period=period, interval=interval)
        if df.empty:
            return []
        bars = []
        for idx, row in df.iterrows():
            ts = idx.timestamp() if hasattr(idx, 'timestamp') else 0
            bars.append({
                "time": int(ts),
                "date": idx.strftime("%Y-%m-%d %H:%M") if tf != "D" else idx.strftime("%Y-%m-%d"),
                "open": round(row["Open"], 2),
                "high": round(row["High"], 2),
                "low":  round(row["Low"], 2),
                "close": round(row["Close"], 2),
                "volume": int(row["Volume"]),
            })
        return bars
    except Exception as e:
        print(f"[US] yfinance kbars 失敗 {symbol}: {e}")
        return []

def _yf_snapshot(symbol: str) -> dict:
    """用 yfinance 取得美股即時快照"""
    try:
        import yfinance as yf
    except ImportError:
        return {}
    try:
        tk = yf.Ticker(symbol)
        info = tk.fast_info
        hist = tk.history(period="2d")
        if hist.empty:
            return {}
        last = hist.iloc[-1]
        prev_close = hist["Close"].iloc[-2] if len(hist) >= 2 else last["Close"]
        price = round(float(last["Close"]), 2)
        chg = round(price - prev_close, 2)
        chg_pct = round((chg / prev_close) * 100, 2) if prev_close else 0
        return {
            "code": symbol,
            "symbol": symbol,
            "name": getattr(info, "short_name", symbol) if hasattr(info, "short_name") else symbol,
            "price": price,
            "change": chg,
            "change_pct": chg_pct,
            "open": round(float(last["Open"]), 2),
            "high": round(float(last["High"]), 2),
            "low": round(float(last["Low"]), 2),
            "volume": int(last["Volume"]),
            "prev_close": round(float(prev_close), 2),
            "market": "US",
        }
    except Exception as e:
        print(f"[US] yfinance snapshot 失敗 {symbol}: {e}")
        return {}

@app.get("/api/us/kbars/{symbol}")
def us_kbars(symbol: str, tf: str = "D"):
    """美股 K 線"""
    tf_map = {"daily": "D", "60": "60", "5": "5"}
    tf_norm = tf_map.get(tf, tf)
    bars = _yf_kbars(symbol.upper(), tf_norm)
    if not bars:
        raise HTTPException(404, f"無法取得 {symbol} K 線資料")
    return _build_kbar_response(symbol.upper(), tf_norm, bars)

@app.get("/api/us/snapshot/{symbol}")
def us_snapshot(symbol: str):
    """美股即時快照"""
    data = _yf_snapshot(symbol.upper())
    if not data:
        raise HTTPException(404, f"無法取得 {symbol} 快照")
    return data

@app.get("/api/us/watchlist")
def us_watchlist():
    """美股自選股清單（含快照）"""
    con = db()
    cur = con.cursor()
    cur.execute("SELECT code, name FROM watchlist WHERE market='US' ORDER BY sort_order")
    rows = cur.fetchall()
    con.close()
    result = []
    for code, name in rows:
        snap = _yf_snapshot(code)
        if snap:
            result.append(snap)
        else:
            result.append({"code": code, "name": name or code, "market": "US", "price": None})
    return result

@app.post("/api/us/watchlist/add/{symbol}")
def us_watchlist_add(symbol: str, name: str = ""):
    sym = symbol.upper()
    if not name:
        try:
            import yfinance as yf
            tk = yf.Ticker(sym)
            name = getattr(tk.fast_info, "short_name", sym) if hasattr(tk.fast_info, "short_name") else sym
        except Exception:
            name = sym
    con = db()
    con.execute("INSERT OR IGNORE INTO watchlist(code, name, market) VALUES(?,?,?)", (sym, name, "US"))
    con.commit()
    con.close()
    return {"ok": True, "code": sym, "name": name}

@app.delete("/api/us/watchlist/remove/{symbol}")
def us_watchlist_remove(symbol: str):
    con = db()
    con.execute("DELETE FROM watchlist WHERE code=? AND market='US'", (symbol.upper(),))
    con.commit()
    con.close()
    return {"ok": True}

@app.get("/api/us/indices")
def us_indices():
    """美股大盤指數快照（SPY/QQQ/DIA + 主要指數）"""
    symbols = [
        ("SPY", "S&P 500 ETF"),
        ("QQQ", "Nasdaq 100 ETF"),
        ("DIA", "Dow Jones ETF"),
        ("^GSPC", "S&P 500"),
        ("^IXIC", "Nasdaq Composite"),
        ("^DJI", "Dow Jones"),
        ("^SOX", "費城半導體"),
    ]
    results = []
    for sym, label in symbols:
        snap = _yf_snapshot(sym)
        if snap:
            snap["label"] = label
            results.append(snap)
    return results

@app.get("/api/us/positions")
def us_positions():
    """美股持倉"""
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM positions WHERE market='US' AND (status='open' OR status IS NULL)")
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    con.close()
    for r in rows:
        if not r.get("cost") or r["cost"] == 0:
            con2 = db()
            buys = con2.execute("SELECT price, shares FROM trade_records WHERE code=? AND action='BUY'", (r["code"],)).fetchall()
            con2.close()
            if buys:
                total_val = sum(b[0] * b[1] for b in buys)
                total_sh = sum(b[1] for b in buys)
                r["cost"] = round(total_val / total_sh, 2) if total_sh else 0
        snap = _yf_snapshot(r["code"])
        if snap:
            r["current_price"] = snap["price"]
            r["change_pct"] = snap["change_pct"]
            if r.get("cost") and r["cost"] > 0:
                r["pnl_pct"] = round((snap["price"] - r["cost"]) / r["cost"] * 100, 2)
    return rows

@app.post("/api/us/positions")
def us_add_position(body: dict, _: None = Depends(require_token)):
    """新增美股持倉"""
    code = body.get("code", "").upper()
    con = db()
    con.execute("""INSERT INTO positions(code, name, cost, shares, trade_type, stop_loss,
                   target_price, entry_date, signal_type, note, market, status)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
        (code, body.get("name", code), body.get("cost", 0), body.get("shares", 0),
         body.get("trade_type", "波段"), body.get("stop_loss", 0), body.get("target_price", 0),
         body.get("entry_date", datetime.now().strftime("%Y-%m-%d")),
         body.get("signal_type", ""), body.get("note", ""), "US", "open"))
    con.commit()
    con.close()
    _ensure_watchlist(code, body.get("name", code), "US")
    return {"ok": True, "code": code}

def run_us_signal_engine(symbol: str, current_price: float) -> list:
    """美股訊號引擎（簡化版：MACD金叉/死叉 + MA交叉 + EXIT_C/D）"""
    bars = _yf_kbars(symbol, "D")
    if len(bars) < 30:
        return []

    closes = [b["close"] for b in bars]
    triggered = []
    s = pd.Series(closes)

    ma5  = s.rolling(5).mean().iloc[-1]
    ma10 = s.rolling(10).mean().iloc[-1]
    ma20 = s.rolling(20).mean().iloc[-1]
    prev5  = s.rolling(5).mean().iloc[-2]
    prev10 = s.rolling(10).mean().iloc[-2]

    dif_s, macd_s, _ = calc_macd(closes)
    dif_cur, macd_cur = dif_s[-1], macd_s[-1]
    dif_prev, macd_prev = dif_s[-2], macd_s[-2]

    vols = [b["volume"] for b in bars]
    vol_ratio = 1.0
    if len(vols) >= 20:
        avg20 = sum(vols[-20:]) / 20
        vol_ratio = vols[-1] / avg20 if avg20 > 0 else 1.0

    # BUY_A: MACD 金叉 + 站上 MA20 + 量比 > 1.5
    if (dif_prev < macd_prev and dif_cur >= macd_cur
            and current_price > ma20 and vol_ratio >= 1.5
            and not _signal_exists_today(symbol, "BUY_A")):
        _write_signal(symbol, "BUY_A", "BUY", current_price,
                      f"[US] MACD金叉+站上MA20，量比{vol_ratio:.1f}x")
        triggered.append("BUY_A")

    # BUY_B: MA5 上穿 MA10
    if (prev5 < prev10 and ma5 >= ma10 and vol_ratio >= 1.2
            and not _signal_exists_today(symbol, "BUY_B")):
        _write_signal(symbol, "BUY_B", "BUY", current_price,
                      f"[US] MA5上穿MA10，量比{vol_ratio:.1f}x")
        triggered.append("BUY_B")

    # EXIT_B: MACD 死叉 + 量縮
    if (dif_prev > macd_prev and dif_cur <= macd_cur and vol_ratio < 0.8
            and not _signal_exists_today(symbol, "EXIT_B")):
        _write_signal(symbol, "EXIT_B", "SELL", current_price,
                      f"[US] MACD死叉+量縮{vol_ratio:.1f}x")
        triggered.append("EXIT_B")

    # EXIT_C/D: 對美股持倉掃描停損
    con = db()
    cur = con.cursor()
    cur.execute("SELECT id, cost, highest_price, trade_type, stop_loss FROM positions WHERE code=? AND market='US' AND (status='open' OR status IS NULL)", (symbol,))
    for pos in cur.fetchall():
        pid, cost, highest, trade_type, custom_sl = pos
        if not cost or cost <= 0:
            continue
        highest = max(highest or cost, current_price)
        if current_price > (highest or 0):
            try:
                c2 = db()
                c2.execute("UPDATE positions SET highest_price=? WHERE id=?", (current_price, pid))
                c2.commit(); c2.close()
            except Exception:
                pass
        # EXIT_D
        cur2 = con.cursor()
        cur2.execute("SELECT value FROM risk_config WHERE key='exit_d_threshold'")
        r = cur2.fetchone()
        threshold = float(r[0]) / 100 if r else 0.05
        pnl = (current_price - cost) / cost
        if pnl <= -threshold and not _signal_exists_today(symbol, "EXIT_D"):
            _write_signal(symbol, "EXIT_D", "SELL", current_price,
                          f"[US] 停損觸發：跌幅{abs(pnl*100):.1f}%（成本${cost}）")
            triggered.append("EXIT_D")
        # EXIT_C
        if trade_type == "波段":
            pt, dt = 0.08, 0.02
        else:
            pt, dt = 0.03, 0.01
        max_p = (highest - cost) / cost
        if max_p >= pt:
            dd = (highest - current_price) / highest
            if dd >= dt and not _signal_exists_today(symbol, "EXIT_C"):
                locked = (current_price - cost) / cost * 100
                _write_signal(symbol, "EXIT_C", "SELL", current_price,
                              f"[US] 移動止盈：最高${highest:.1f} 回落{dd*100:.1f}% 鎖住{locked:.1f}%")
                triggered.append("EXIT_C")
    con.close()
    return triggered

@app.get("/api/us/scan/signals")
def us_scan_signals():
    """掃描美股自選股+持倉的訊號，並寫入 signal_log"""
    con = db()
    cur = con.cursor()
    cur.execute("SELECT DISTINCT code FROM watchlist WHERE market='US' UNION SELECT DISTINCT code FROM positions WHERE market='US'")
    codes = [r[0] for r in cur.fetchall()]
    con.close()
    all_triggered = []
    for code in codes:
        snap = _yf_snapshot(code)
        if snap and snap.get("price"):
            signals = run_us_signal_engine(code, snap["price"])
            for sig in signals:
                all_triggered.append({"code": code, "signal": sig, "price": snap["price"]})
                if not _signal_exists_today(code, sig):
                    direction = "BUY" if sig.startswith("BUY") or sig in ("LOW_BUY","SQUEEZE_BREAK","LOCK_BUY") else "SELL"
                    _log_signal(code, sig, direction, snap["price"], f"[US] {sig}")
    return {"triggered": all_triggered, "scanned": len(codes)}

@app.get("/api/us/scan/after-hours")
def us_scan_after_hours():
    """美股盤後回顧：用 yfinance 快照對美股自選+持倉跑訊號+摘要"""
    con = db()
    cur = con.cursor()
    cur.execute("SELECT DISTINCT code FROM watchlist WHERE market='US' UNION SELECT DISTINCT code FROM positions WHERE market='US' AND status='open'")
    codes = [r[0] for r in cur.fetchall()]
    con.close()
    if not codes:
        return {"triggered": [], "summaries": [], "scanned": 0}
    all_triggered = []
    summaries = []
    for code in codes:
        snap = _yf_snapshot(code)
        if not snap or not snap.get("price"):
            continue
        price = snap["price"]
        signals = run_us_signal_engine(code, price)
        for sig in signals:
            all_triggered.append({"code": code, "signal": sig, "price": price})
            if not _signal_exists_today(code, sig):
                direction = "BUY" if sig.startswith("BUY") or sig in ("LOW_BUY","SQUEEZE_BREAK","LOCK_BUY") else "SELL"
                _write_signal(code, sig, direction, price, f"[US] {sig}")
        ohlcv = _get_ohlcv_from_cache(code, market="US")
        summary = {"code": code, "close": price, "change_pct": snap.get("change_pct", 0)}
        if ohlcv and len(ohlcv.get("closes", [])) >= 20:
            closes = ohlcv["closes"]
            s_arr = pd.Series(closes + [price])
            ma5  = round(float(s_arr.rolling(5).mean().iloc[-1]), 2)
            ma20 = round(float(s_arr.rolling(20).mean().iloc[-1]), 2)
            summary["ma5"] = ma5; summary["ma20"] = ma20
            summary["above_ma5"] = price >= ma5; summary["above_ma20"] = price >= ma20
            dif_s, macd_s, _ = calc_macd(s_arr.tolist())
            summary["macd_direction"] = "bull" if dif_s[-1] > macd_s[-1] else "bear"
            summary["macd_cross"] = "golden" if dif_s[-2] <= macd_s[-2] and dif_s[-1] > macd_s[-1] else "death" if dif_s[-2] >= macd_s[-2] and dif_s[-1] < macd_s[-1] else "none"
        if ohlcv and len(ohlcv.get("volumes", [])) >= 20:
            vols = ohlcv["volumes"]
            avg20 = sum(vols[-20:]) / 20
            summary["vol_ratio"] = round(vols[-1] / avg20, 2) if avg20 > 0 else 1.0
        con2 = db(); cur2 = con2.cursor()
        cur2.execute("SELECT cost, shares, stop_loss FROM positions WHERE code=? AND status='open' AND market='US'", (code,))
        pos_row = cur2.fetchone(); con2.close()
        if pos_row and pos_row[0]:
            cost = pos_row[0]
            summary["pnl_pct"] = round((price - cost) / cost * 100, 2)
            if pos_row[2]: summary["stop_loss_dist"] = round((price - pos_row[2]) / price * 100, 1)
        summaries.append(summary)
    return {"triggered": all_triggered, "summaries": summaries, "scanned": len(codes)}

# ── 資料來源管理 ─────────────────────────────────

@app.get("/api/datasources")
def get_datasources():
    """回傳系統所有資料來源狀態"""
    sources = [
        {
            "id": "shioaji",
            "name": "永豐金 Shioaji API",
            "category": "行情",
            "usage": ["即時 Tick/BidAsk", "歷史 K 線", "快照報價", "下單執行"],
            "status": "active",
            "config": "SJ_API_KEY / SJ_SEC_KEY (env or .env)",
            "rate_limit": "50 req / 5s (查詢)、10 req / 5s (ticks)",
            "docs": "https://sinotrade.github.io/",
            "notes": "模擬盤預設；SJ_PRODUCTION=true 切正式",
        },
        {
            "id": "yfinance",
            "name": "Yahoo Finance (yfinance)",
            "category": "總經",
            "usage": ["VIX", "DXY 美元指數", "US10Y 美國十年期", "ES 美股期指", "TWII 加權指數"],
            "status": "active",
            "config": "免費，無需 API key",
            "rate_limit": "無明確限制，建議 15 分鐘快取",
            "docs": "https://pypi.org/project/yfinance/",
            "notes": "資料延遲約 15 分鐘；盤後數據可能隔日才更新",
        },
        {
            "id": "yfinance_us",
            "name": "Yahoo Finance (美股行情)",
            "category": "行情",
            "usage": ["美股日K線/OHLCV", "美股快照報價", "美股 Benchmark (SPY)"],
            "status": "active",
            "config": "免費，無需 API key",
            "rate_limit": "無明確限制，建議快取",
            "docs": "https://pypi.org/project/yfinance/",
            "notes": "美股持倉/自選股的價格來源；亦用於 RS 相對強弱計算",
        },
        {
            "id": "yfinance_fund",
            "name": "Yahoo Finance (基本面)",
            "category": "基本面",
            "usage": ["PE/PB/ROE", "EPS", "殖利率", "市值", "產業分類 (Sector/Industry)"],
            "status": "active",
            "config": "免費，無需 API key",
            "rate_limit": "無明確限制，快取 6 小時",
            "docs": "https://pypi.org/project/yfinance/",
            "notes": "台股用 {code}.TW 取得；資訊中心股票分析的基本面指標來源",
        },
        {
            "id": "claude_ai",
            "name": "Claude AI (LLM 分析)",
            "category": "AI",
            "usage": ["股票深度分析", "AI 推薦掃描", "總經 AI 解讀", "新聞情緒分析"],
            "status": "active",
            "config": "API Key 或本機 claude CLI 訂閱（設定頁切換）",
            "rate_limit": "依方案（API: token 計費 / 訂閱: 無額外費用）",
            "docs": "https://docs.anthropic.com/",
            "notes": "資訊中心各功能可獨立選擇 API 或訂閱模式",
        },
        {
            "id": "twse",
            "name": "台灣證交所公開資料",
            "category": "籌碼",
            "usage": ["三大法人買賣超 (T86)", "融資融券餘額 (MI_MARGN)"],
            "status": "active",
            "config": "免費公開 API，無需 key",
            "rate_limit": "建議每次抓取間隔 30 秒以上",
            "docs": "https://www.twse.com.tw/zh/trading/foreign/BFI82U.html",
            "notes": "僅上市股票；盤後約 16:00 更新；非交易日無資料",
        },
        {
            "id": "telegram",
            "name": "Telegram Bot API",
            "category": "通知",
            "usage": ["訊號推播", "停損警示", "系統通知"],
            "status": "active",
            "config": "telegram_bot_token + telegram_chat_id",
            "rate_limit": "30 msg/sec (per bot)",
            "docs": "https://core.telegram.org/bots/api",
            "notes": "免費；@BotFather 建立 bot 取得 token",
        },
        {
            "id": "email",
            "name": "Email SMTP",
            "category": "通知",
            "usage": ["訊號推播", "每日報告"],
            "status": "active",
            "config": "email_smtp_host / email_user / email_pass",
            "rate_limit": "依 SMTP 供應商（Gmail: 500/日）",
            "docs": "",
            "notes": "Gmail 需使用 App Password（非帳號密碼）",
        },
        {
            "id": "tpex",
            "name": "櫃買中心 (TPEX)",
            "category": "籌碼",
            "usage": ["上櫃三大法人", "上櫃融資融券"],
            "status": "planned",
            "config": "免費公開 API",
            "rate_limit": "同 TWSE",
            "docs": "https://www.tpex.org.tw/",
            "notes": "目前僅接上市(TWSE)，上櫃股需另接 TPEX API",
        },
        {
            "id": "goodinfo",
            "name": "Goodinfo 台灣股市資訊網",
            "category": "基本面",
            "usage": ["本益比/殖利率", "營收月報", "法人持股比例", "融券強制回補日"],
            "status": "planned",
            "config": "網頁爬蟲（需注意 rate limit）",
            "rate_limit": "建議 3-5 秒間隔",
            "docs": "https://goodinfo.tw/tw/index.asp",
            "notes": "資料豐富但無官方 API，需爬蟲；可補充投信持股比例和強制回補日",
        },
        {
            "id": "mops",
            "name": "公開資訊觀測站 (MOPS)",
            "category": "基本面",
            "usage": ["月營收", "財報", "重大訊息", "董監持股"],
            "status": "planned",
            "config": "免費公開 API",
            "rate_limit": "無明確限制",
            "docs": "https://mops.twse.com.tw/",
            "notes": "官方資料源，適合自動化月營收追蹤和重大訊息偵測（可替代部分 NLP）",
        },
        {
            "id": "finmind",
            "name": "FinMind 開源金融資料",
            "category": "行情/籌碼",
            "usage": ["歷史股價", "法人買賣超", "融資融券", "期貨大額交易人"],
            "status": "planned",
            "config": "免費 API key (finmindtrade.com)",
            "rate_limit": "免費版 600 req/hr",
            "docs": "https://finmindtrade.com/",
            "notes": "統一 API 取得多種資料，適合作為 TWSE 爬蟲的備援或補強",
        },
        {
            "id": "cnyes",
            "name": "鉅亨網 API",
            "category": "新聞/總經",
            "usage": ["即時新聞", "國際指數", "原物料報價"],
            "status": "planned",
            "config": "非官方 API（JSON endpoints）",
            "rate_limit": "未知，建議低頻",
            "docs": "https://www.cnyes.com/",
            "notes": "可用於 G7 NLP 新聞偵測的資料源；需爬蟲",
        },
        {
            "id": "fugle",
            "name": "Fugle API",
            "category": "行情",
            "usage": ["即時報價", "歷史 K 線", "基本面資料"],
            "status": "planned",
            "config": "API key (fugle.tw)",
            "rate_limit": "免費版 60 req/min",
            "docs": "https://developer.fugle.tw/",
            "notes": "可作為 Shioaji 行情的備援；提供 RESTful API 和 WebSocket",
        },
        {
            "id": "taifex",
            "name": "期交所公開資料",
            "category": "籌碼",
            "usage": ["期貨大額交易人", "選擇權 Put/Call ratio", "未平倉量"],
            "status": "planned",
            "config": "免費公開 API",
            "rate_limit": "同公開資料站",
            "docs": "https://www.taifex.com.tw/",
            "notes": "期貨籌碼可輔助判斷大盤多空方向，強化 MACRO_LOCK 判斷",
        },
        {
            "id": "finnhub",
            "name": "Finnhub",
            "category": "基本面/情緒",
            "usage": ["分析師評級/目標價", "EPS 預估", "公司新聞", "社群情緒"],
            "status": "planned",
            "config": "API key (finnhub.io)，免費版可用",
            "rate_limit": "免費 60 req/min",
            "docs": "https://finnhub.io/docs/api",
            "notes": "美股為主；可補強分析師共識和 EPS surprise 數據",
        },
        {
            "id": "newsapi",
            "name": "NewsAPI",
            "category": "新聞",
            "usage": ["全球新聞搜尋", "關鍵字即時新聞", "NLP 情緒來源"],
            "status": "planned",
            "config": "API key (newsapi.org)，免費版 100 req/日",
            "rate_limit": "免費 100 req/日；付費無限",
            "docs": "https://newsapi.org/docs",
            "notes": "可作為新聞情緒分析的輸入源；免費版僅回傳標題",
        },
        {
            "id": "reddit",
            "name": "Reddit API (社群情緒)",
            "category": "社群",
            "usage": ["WSB/investing 熱門討論", "社群情緒分析", "散戶動向"],
            "status": "planned",
            "config": "Reddit API credentials (免費)",
            "rate_limit": "60 req/min",
            "docs": "https://www.reddit.com/dev/api/",
            "notes": "追蹤 WallStreetBets 等社群的個股討論熱度和情緒",
        },
        {
            "id": "openbb",
            "name": "OpenBB Platform",
            "category": "多資產統一引擎",
            "usage": ["統一 API 取股票/ETF/期權/總經", "多 provider 切換", "標準化輸出"],
            "status": "planned",
            "config": "pip install openbb；各 provider 需個別 key",
            "rate_limit": "依底層 provider",
            "docs": "https://docs.openbb.co/",
            "notes": "可替代多個獨立 API，統一資料格式；適合未來整合",
        },
        {
            "id": "fred",
            "name": "FRED (聯準會經濟數據)",
            "category": "總經",
            "usage": ["GDP", "CPI/PPI", "失業率", "聯邦基金利率", "殖利率曲線"],
            "status": "planned",
            "config": "API key (fred.stlouisfed.org)，免費",
            "rate_limit": "120 req/min",
            "docs": "https://fred.stlouisfed.org/docs/api/",
            "notes": "官方總經數據源；可強化風控頁的總經指標",
        },
        {
            "id": "alpha_vantage",
            "name": "Alpha Vantage",
            "category": "行情/基本面",
            "usage": ["全球股價", "技術指標 API", "財報數據", "外匯/加密貨幣"],
            "status": "planned",
            "config": "API key (免費版 25 req/日)",
            "rate_limit": "免費 25 req/日；付費無限",
            "docs": "https://www.alphavantage.co/documentation/",
            "notes": "免費額度低但資料全面；適合低頻基本面查詢",
        },
        {
            "id": "yfinance_sector",
            "name": "Yahoo Finance (板塊輪動)",
            "category": "板塊",
            "usage": ["GICS 11大板塊 ETF", "板塊動量排名", "RS vs SPY"],
            "status": "active",
            "config": "免費，無需 API key",
            "rate_limit": "無明確限制，快取 1 小時",
            "docs": "https://pypi.org/project/yfinance/",
            "notes": "P3 板塊輪動功能的數據源",
        },
        {
            "id": "yfinance_events",
            "name": "Yahoo Finance (事件驅動)",
            "category": "事件",
            "usage": ["財報日期", "除息日", "新聞標題", "事件情緒判斷"],
            "status": "active",
            "config": "免費，無需 API key",
            "rate_limit": "無明確限制，快取 6 小時",
            "docs": "https://pypi.org/project/yfinance/",
            "notes": "P4 事件驅動功能的數據源",
        },
        {
            "id": "yfinance_options",
            "name": "Yahoo Finance (期權鏈)",
            "category": "衍生品",
            "usage": ["期權鏈", "Put/Call Ratio", "隱含波動率 IV", "Greeks"],
            "status": "active",
            "config": "免費，無需 API key",
            "rate_limit": "無明確限制",
            "docs": "https://pypi.org/project/yfinance/",
            "notes": "P15 期權數據功能；僅美股",
        },
        {
            "id": "yfinance_crypto",
            "name": "Yahoo Finance (加密貨幣)",
            "category": "加密貨幣",
            "usage": ["BTC/ETH 等主流幣價格", "歷史K線", "成交量"],
            "status": "active",
            "config": "免費，無需 API key",
            "rate_limit": "無明確限制",
            "docs": "https://pypi.org/project/yfinance/",
            "notes": "P16 Crypto 功能的數據源；鏈上數據規劃中",
        },
        {
            "id": "alpha_factors",
            "name": "內建 Alpha 因子庫",
            "category": "量化",
            "usage": ["Alpha158 風格因子", "動量/波動/量能/技術", "多因子組合排名", "IC/ICIR 驗證"],
            "status": "active",
            "config": "內建計算，無需外部 API",
            "rate_limit": "無限制",
            "docs": "",
            "notes": "P9-P11 因子庫 + 多因子組合 + IC驗證",
        },
        {
            "id": "twitter",
            "name": "Twitter/X API (社群情緒)",
            "category": "社群",
            "usage": ["$CASHTAG 股票討論", "情緒分析", "KOL 追蹤", "即時輿情"],
            "status": "planned",
            "config": "Twitter API v2 Bearer Token (付費)",
            "rate_limit": "Basic: 10k tweets/月；Pro: 1M/月",
            "docs": "https://developer.twitter.com/en/docs",
            "notes": "FinGPT 核心情緒來源；需付費 API；可用 Cashtag 搜尋個股討論",
        },
        {
            "id": "sec_edgar",
            "name": "SEC EDGAR (美股財報)",
            "category": "基本面",
            "usage": ["10-K/10-Q 年報季報", "8-K 重大事件", "13-F 機構持倉", "內部人交易"],
            "status": "planned",
            "config": "免費 API，需 User-Agent header",
            "rate_limit": "10 req/sec",
            "docs": "https://www.sec.gov/edgar/sec-api-documentation",
            "notes": "FinGPT/OpenAlice 共用；美股財報原始資料源；可用於事件驅動策略",
        },
        {
            "id": "stocktwits",
            "name": "Stocktwits (社群情緒)",
            "category": "社群",
            "usage": ["個股討論情緒", "Bull/Bear 比例", "熱門標的", "散戶動向"],
            "status": "planned",
            "config": "免費 API（有限制）",
            "rate_limit": "200 req/hr",
            "docs": "https://api.stocktwits.com/developers/docs",
            "notes": "OpenAlice 使用；專注股票社群；有內建 Bull/Bear 情緒標籤",
        },
        {
            "id": "polygon",
            "name": "Polygon.io (美股即時)",
            "category": "行情",
            "usage": ["即時報價 WebSocket", "歷史 Tick", "期權/外匯/加密", "企業事件"],
            "status": "planned",
            "config": "API key (polygon.io)，免費版延遲15分鐘",
            "rate_limit": "免費 5 req/min；付費無限",
            "docs": "https://polygon.io/docs",
            "notes": "OpenAlice 使用；可替代 yfinance 取得更即時的美股數據",
        },
        {
            "id": "ibkr",
            "name": "Interactive Brokers API",
            "category": "券商",
            "usage": ["美股/全球下單", "即時行情", "帳戶管理", "期權交易"],
            "status": "planned",
            "config": "IBKR 帳戶 + TWS/Gateway",
            "rate_limit": "50 msg/sec",
            "docs": "https://interactivebrokers.github.io/",
            "notes": "OpenAlice 全生命周期券商；可用 ib_insync Python lib",
        },
        {
            "id": "qlib",
            "name": "Qlib 數據集 (微軟)",
            "category": "量化",
            "usage": ["Alpha158/Alpha360 標準因子", "A股/美股歷史數據", "預處理管線"],
            "status": "planned",
            "config": "pip install qlib；需下載數據集",
            "rate_limit": "本地計算，無限制",
            "docs": "https://qlib.readthedocs.io/",
            "notes": "RD-Agent 核心；我們已用 alpha_factors 內建替代部分功能",
        },
        {
            "id": "adanos",
            "name": "Adanos 情緒 API (付費)",
            "category": "情緒",
            "usage": ["NLP 新聞情緒", "社群情緒指數", "情緒趨勢"],
            "status": "planned",
            "config": "付費 API key",
            "rate_limit": "依方案",
            "docs": "",
            "notes": "FinGPT 使用；專業金融情緒分析服務",
        },
        {
            "id": "benzinga",
            "name": "Benzinga",
            "category": "新聞",
            "usage": ["即時公司新聞", "財報日曆", "分析師評級", "IPO 追蹤"],
            "status": "planned",
            "config": "API key (benzinga.com)，付費",
            "rate_limit": "依方案",
            "docs": "https://docs.benzinga.io/",
            "notes": "OpenBB 核心新聞源；即時性高，有結構化事件標籤",
        },
        {
            "id": "biztoc",
            "name": "Biztoc (新聞聚合)",
            "category": "新聞",
            "usage": ["跨源新聞聚合", "即時財經頭條", "趨勢話題"],
            "status": "planned",
            "config": "免費 API (RapidAPI)",
            "rate_limit": "免費版有限",
            "docs": "https://biztoc.com/",
            "notes": "OpenBB 使用；聚合多家媒體新聞，適合快速掃描",
        },
        {
            "id": "seeking_alpha",
            "name": "Seeking Alpha",
            "category": "分析",
            "usage": ["分析師深度文章", "個股評級", "股利分析", "財報解讀"],
            "status": "planned",
            "config": "非官方 API / 爬蟲",
            "rate_limit": "需注意反爬",
            "docs": "https://seekingalpha.com/",
            "notes": "FinGPT 使用；散戶分析師觀點，可用於情緒對比",
        },
        {
            "id": "google_trends",
            "name": "Google Trends",
            "category": "另類數據",
            "usage": ["搜尋熱度趨勢", "個股關注度", "產業熱度比較", "地區分析"],
            "status": "planned",
            "config": "免費 (pytrends lib)",
            "rate_limit": "無官方限制，建議低頻",
            "docs": "https://pypi.org/project/pytrends/",
            "notes": "FinGPT 使用；搜尋量異常可作為另類訊號",
        },
        {
            "id": "intrinio",
            "name": "Intrinio",
            "category": "行情/基本面",
            "usage": ["即時報價", "歷史價格", "財務報表", "企業事件"],
            "status": "planned",
            "config": "API key (intrinio.com)，付費",
            "rate_limit": "依方案",
            "docs": "https://docs.intrinio.com/",
            "notes": "OpenBB provider；機構級數據品質",
        },
        {
            "id": "tiingo",
            "name": "Tiingo",
            "category": "行情",
            "usage": ["日線/日內數據", "IEX 即時報價", "加密貨幣", "企業行動"],
            "status": "planned",
            "config": "API key (tiingo.com)，免費版可用",
            "rate_limit": "免費 500 req/hr",
            "docs": "https://api.tiingo.com/documentation",
            "notes": "OpenBB provider；免費額度不錯，適合備援",
        },
        {
            "id": "fmp",
            "name": "Financial Modeling Prep (FMP)",
            "category": "基本面",
            "usage": ["財務比率", "成長指標", "DCF 估值", "ETF 持倉", "ESG 評分"],
            "status": "planned",
            "config": "API key (financialmodelingprep.com)，免費版可用",
            "rate_limit": "免費 250 req/日",
            "docs": "https://site.financialmodelingprep.com/developer/docs",
            "notes": "OpenBB provider；基本面數據豐富，有 DCF 和 ESG",
        },
        {
            "id": "finviz",
            "name": "Finviz (選股篩選器)",
            "category": "選股",
            "usage": ["多條件選股篩選", "熱力圖", "內部人交易", "技術指標篩選"],
            "status": "planned",
            "config": "付費 Elite 版 API / 免費版爬蟲 (finvizfinance lib)",
            "rate_limit": "免費版需注意頻率",
            "docs": "https://finviz.com/",
            "notes": "OpenBB provider；最受歡迎的選股工具之一",
        },
        {
            "id": "bls",
            "name": "BLS 勞工統計局",
            "category": "總經",
            "usage": ["非農就業", "失業率", "CPI/PPI 通膨", "薪資數據"],
            "status": "planned",
            "config": "免費 API key (bls.gov)",
            "rate_limit": "v2: 500 req/日",
            "docs": "https://www.bls.gov/developers/",
            "notes": "OpenBB provider；聯準會決策關鍵數據；與 FRED 互補",
        },
        {
            "id": "cftc",
            "name": "CFTC (期貨大戶部位)",
            "category": "籌碼",
            "usage": ["COT 報告", "期貨大額交易人部位", "商業/非商業持倉", "淨部位變化"],
            "status": "planned",
            "config": "免費公開資料",
            "rate_limit": "每週五更新",
            "docs": "https://www.cftc.gov/MarketReports/CommitmentsofTraders/",
            "notes": "OpenBB provider；期貨市場大戶方向判斷的關鍵數據",
        },
        {
            "id": "econdb",
            "name": "EconDB (全球總經)",
            "category": "總經",
            "usage": ["全球 GDP/CPI", "各國央行利率", "跨國經濟指標比較"],
            "status": "planned",
            "config": "免費 API",
            "rate_limit": "合理使用",
            "docs": "https://www.econdb.com/",
            "notes": "OpenBB provider；覆蓋全球各國總經數據",
        },
        {
            "id": "cboe",
            "name": "CBOE (選擇權交易所)",
            "category": "衍生品",
            "usage": ["VIX 期權", "SKEW 指數", "Put/Call 成交量", "波動率曲面"],
            "status": "planned",
            "config": "免費公開資料 + 付費 API",
            "rate_limit": "依方案",
            "docs": "https://www.cboe.com/market_data/",
            "notes": "OpenBB provider；恐慌指標和選擇權市場結構數據",
        },
        {
            "id": "finra",
            "name": "FINRA (空頭/監管數據)",
            "category": "籌碼",
            "usage": ["空頭餘額 (Short Interest)", "暗池成交", "場外交易量", "監管公告"],
            "status": "planned",
            "config": "免費公開資料",
            "rate_limit": "合理使用",
            "docs": "https://www.finra.org/finra-data",
            "notes": "OpenBB provider；空頭數據是軋空策略的關鍵",
        },
        {
            "id": "deribit",
            "name": "Deribit (加密衍生品)",
            "category": "加密貨幣",
            "usage": ["BTC/ETH 期權", "期貨", "隱含波動率", "資金費率"],
            "status": "planned",
            "config": "免費 API (deribit.com)",
            "rate_limit": "不驗證: 20 req/sec",
            "docs": "https://docs.deribit.com/",
            "notes": "OpenBB provider；加密貨幣衍生品的主要交易所",
        },
        {
            "id": "kaggle",
            "name": "Kaggle Datasets",
            "category": "量化",
            "usage": ["競賽數據集", "歷史股價CSV", "另類數據", "ML 訓練資料"],
            "status": "planned",
            "config": "免費帳戶 + kaggle API key",
            "rate_limit": "無明確限制",
            "docs": "https://www.kaggle.com/docs/api",
            "notes": "RD-Agent 使用；75+ 金融相關競賽數據集",
        },
        {
            "id": "line_notify",
            "name": "LINE Notify",
            "category": "通知",
            "usage": ["訊號推播", "停損警示", "每日報告", "群組通知"],
            "status": "planned",
            "config": "免費 Token (notify-bot.line.me)",
            "rate_limit": "1000 msg/hr",
            "docs": "https://notify-bot.line.me/",
            "notes": "台灣最常用通訊軟體；一行 code 即可推播",
        },
        {
            "id": "discord",
            "name": "Discord Webhook",
            "category": "通知",
            "usage": ["多頻道分類推播", "Embed 格式報告", "機器人互動"],
            "status": "planned",
            "config": "免費 Webhook URL",
            "rate_limit": "30 msg/min per webhook",
            "docs": "https://discord.com/developers/docs/resources/webhook",
            "notes": "免費，可建多頻道分類（停損/推薦/日報）",
        },
    ]

    # 加上 market_scope 分類
    _scope_map = {
        "shioaji": "TW", "twse": "TW", "tpex": "TW", "goodinfo": "TW", "mops": "TW",
        "finmind": "TW", "cnyes": "TW", "fugle": "TW", "taifex": "TW",
        "yfinance_us": "US", "yfinance_options": "US", "yfinance_sector": "US",
        "sec_edgar": "US", "stocktwits": "US", "polygon": "US", "ibkr": "US",
        "benzinga": "US", "intrinio": "US", "tiingo": "US", "fmp": "US",
        "finviz": "US", "finra": "US", "cboe": "US", "cftc": "US",
        "seeking_alpha": "US", "deribit": "CRYPTO",
        "yfinance_crypto": "CRYPTO",
    }
    for s in sources:
        s["market_scope"] = _scope_map.get(s["id"], "ALL")

    # 標記 IC 分析實際使用中的資料源
    for s in sources:
        s["ic_used"] = s["id"] in _IC_DATASOURCE_IDS

    # 即時檢查已啟用資料源的設定狀態
    cfg = _get_notify_config()
    for s in sources:
        if s["id"] == "telegram":
            s["configured"] = bool(cfg.get("telegram_bot_token") and cfg.get("telegram_chat_id"))
        elif s["id"] == "email":
            s["configured"] = bool(cfg.get("email_smtp_host") and cfg.get("email_user"))
        elif s["id"] == "shioaji":
            s["configured"] = bool(API_KEY and SECRET_KEY)
        elif s["id"] == "claude_ai":
            ic_settings = _ic_get_settings()
            has_api = bool(ic_settings.get("claude_api_key"))
            import shutil as _sh
            has_cli = bool(_sh.which(ic_settings.get("claude_cli_path", "claude") or "claude"))
            s["configured"] = has_api or has_cli
        elif s["id"] in ("yfinance", "yfinance_us", "yfinance_fund", "twse",
                         "yfinance_sector", "yfinance_events", "yfinance_options",
                         "yfinance_crypto", "alpha_factors"):
            s["configured"] = True
        else:
            s["configured"] = s["status"] == "active"

    return sources


FEATURE_DATASOURCE_MAP = [
    {"id": "stock_analysis", "name": "股票深度分析", "page": "資訊中心", "icon": "🔬",
     "datasources": ["yfinance", "yfinance_us", "yfinance_fund", "shioaji", "twse", "claude_ai"],
     "description": "技術面+基本面+籌碼+AI綜合評分"},
    {"id": "macro_risk", "name": "風控/總經監控", "page": "風控中心", "icon": "🌍",
     "datasources": ["yfinance"],
     "description": "VIX/DXY/US10Y/期指/加權指數即時監控"},
    {"id": "chip_analysis", "name": "籌碼分析", "page": "籌碼分析", "icon": "🏦",
     "datasources": ["twse", "shioaji"],
     "description": "三大法人/融資融券/當沖比/軋空偵測"},
    {"id": "sector_rotation", "name": "板塊輪動", "page": "資訊中心", "icon": "🔄",
     "datasources": ["yfinance_sector"],
     "description": "GICS 11大板塊動量排名與RS強弱"},
    {"id": "event_driven", "name": "事件驅動", "page": "資訊中心", "icon": "📅",
     "datasources": ["yfinance_events"],
     "description": "財報日/除息日/新聞事件偵測"},
    {"id": "sentiment_fusion", "name": "多源情緒融合", "page": "資訊中心", "icon": "💬",
     "datasources": ["claude_ai", "yfinance_events"],
     "description": "AI+新聞+技術面加權情緒合成"},
    {"id": "alpha_factors", "name": "量化因子庫", "page": "資訊中心", "icon": "📐",
     "datasources": ["yfinance_us", "shioaji", "alpha_factors"],
     "description": "Alpha158風格20核心因子計算"},
    {"id": "factor_ic", "name": "IC/ICIR驗證", "page": "資訊中心", "icon": "📊",
     "datasources": ["yfinance_us", "shioaji", "alpha_factors"],
     "description": "因子有效性Spearman相關性驗證"},
    {"id": "multi_factor", "name": "多因子組合", "page": "資訊中心", "icon": "⚖️",
     "datasources": ["yfinance_us", "shioaji", "alpha_factors"],
     "description": "Z-score正交化+等權複合排名"},
    {"id": "ai_factor_gen", "name": "AI因子生成", "page": "資訊中心", "icon": "🤖",
     "datasources": ["claude_ai", "alpha_factors"],
     "description": "AI分析IC結果建議新因子公式"},
    {"id": "auto_quant", "name": "Auto-Quant迭代", "page": "資訊中心", "icon": "🔁",
     "datasources": ["claude_ai", "yfinance_us", "shioaji"],
     "description": "AI分析回測結果自動迭代策略"},
    {"id": "backtest", "name": "回測引擎", "page": "資訊中心", "icon": "📈",
     "datasources": ["yfinance_us", "shioaji"],
     "description": "歷史回測+Walk-forward防過擬合"},
    {"id": "options_chain", "name": "期權數據", "page": "資訊中心", "icon": "📋",
     "datasources": ["yfinance_options"],
     "description": "期權鏈/Put-Call Ratio/IV/Greeks"},
    {"id": "crypto", "name": "加密貨幣", "page": "資訊中心", "icon": "₿",
     "datasources": ["yfinance_crypto"],
     "description": "BTC/ETH主流幣價格與技術分析"},
    {"id": "ai_recommend", "name": "AI推薦掃描", "page": "資訊中心", "icon": "⭐",
     "datasources": ["claude_ai", "yfinance_us", "yfinance_fund", "shioaji", "twse"],
     "description": "AI批量掃描自選股產生推薦"},
    {"id": "knowledge_base", "name": "知識庫RAG", "page": "資訊中心", "icon": "📚",
     "datasources": ["claude_ai"],
     "description": "本地RAG檢索用戶餵入的研究資料"},
    {"id": "notifications", "name": "訊號推播", "page": "設定", "icon": "🔔",
     "datasources": ["telegram", "email"],
     "description": "停損/推薦/日報通知推送"},
    {"id": "position_lifecycle", "name": "持倉生命周期", "page": "持倉管理", "icon": "🔄",
     "datasources": ["shioaji", "yfinance_us"],
     "description": "研究→建倉→持有→減碼→出場全流程"},
    {"id": "social_sentiment", "name": "社群情緒", "page": "資訊中心", "icon": "🗣️",
     "datasources": ["yfinance_events"],
     "description": "新聞代理的社群情緒快照"},
    {"id": "us_sectors", "name": "美股產業總覽", "page": "資訊中心", "icon": "🏢",
     "datasources": ["yfinance_us", "yfinance_fund"],
     "description": "美股11大產業持倉分佈與表現"},
]


@app.get("/api/feature-datasource-map")
def get_feature_datasource_map():
    """回傳功能→資料源依賴對照表"""
    ds_all = get_datasources()
    ds_map = {d["id"]: d for d in ds_all}
    result = []
    for feat in FEATURE_DATASOURCE_MAP:
        f = dict(feat)
        f["datasource_details"] = [{
            "id": did, "name": ds_map[did]["name"],
            "status": ds_map[did]["status"],
            "configured": ds_map[did].get("configured", False),
        } for did in f["datasources"] if did in ds_map]
        result.append(f)
    return result


# ── 公式註冊表 (Formula Registry) ─────────────────────
_formula_overrides: dict = {}

FORMULA_REGISTRY = [
    # ── A. 技術指標 (_ic_score_stock) ──
    {"id": "tech_kd", "category": "技術指標", "name": "KD 隨機指標", "feature": "stock_analysis",
     "formula": "RSV=(C-L9)/(H9-L9)×100, K=RSV.ewm(com=2), D=K.ewm(com=2)",
     "scoring": [
         {"condition": "KD金叉 且 K<80", "points": "+20"},
         {"condition": "K>80 超買", "points": "-10"},
         {"condition": "K<20 超賣", "points": "+10"},
     ],
     "params": [
         {"key": "kd_period", "label": "回看期", "value": 9, "default": 9, "min": 5, "max": 20, "step": 1},
         {"key": "kd_overbought", "label": "超買閾值", "value": 80, "default": 80, "min": 60, "max": 95, "step": 5},
         {"key": "kd_oversold", "label": "超賣閾值", "value": 20, "default": 20, "min": 5, "max": 40, "step": 5},
     ],
     "external": {"FinGPT": "標準KD", "RD-Agent": "不使用傳統技術指標", "OpenAlice": "整合TA-Lib"},
    },
    {"id": "tech_macd", "category": "技術指標", "name": "MACD 指數平滑異同", "feature": "stock_analysis",
     "formula": "DIF=EMA(12)-EMA(26), MACD=EMA(DIF,9)",
     "scoring": [
         {"condition": "MACD金叉", "points": "+20"},
         {"condition": "多方區(DIF>0且MACD>0)", "points": "+10"},
         {"condition": "MACD死叉", "points": "-20"},
     ],
     "params": [
         {"key": "macd_fast", "label": "快線EMA", "value": 12, "default": 12, "min": 5, "max": 20, "step": 1},
         {"key": "macd_slow", "label": "慢線EMA", "value": 26, "default": 26, "min": 15, "max": 40, "step": 1},
         {"key": "macd_signal", "label": "訊號線EMA", "value": 9, "default": 9, "min": 5, "max": 15, "step": 1},
     ],
     "external": {"FinGPT": "MACD+Volume Weighted MACD+Dual MACD", "RD-Agent": "Alpha因子替代", "OpenAlice": "標準MACD"},
    },
    {"id": "tech_ma", "category": "技術指標", "name": "均線排列", "feature": "stock_analysis",
     "formula": "MA5/MA10/MA20/MA60 多頭排列判斷",
     "scoring": [
         {"condition": "完美多頭排列 Price>MA5>MA10>MA20>MA60", "points": "+25"},
         {"condition": "價格站上MA20", "points": "+10"},
         {"condition": "價格跌破MA20", "points": "-10"},
     ],
     "params": [
         {"key": "ma_short", "label": "短均線", "value": 5, "default": 5, "min": 3, "max": 10, "step": 1},
         {"key": "ma_mid", "label": "中均線", "value": 20, "default": 20, "min": 10, "max": 30, "step": 1},
         {"key": "ma_long", "label": "長均線", "value": 60, "default": 60, "min": 40, "max": 120, "step": 5},
     ],
     "external": {"FinGPT": "MA交叉策略", "OpenAlice": "多均線系統"},
    },
    {"id": "tech_rvol", "category": "技術指標", "name": "相對量能 RVOL", "feature": "stock_analysis",
     "formula": "RVOL = 當日量 / MA20(量), RVOL5 = MA5(量) / MA20(量)",
     "scoring": [
         {"condition": "RVOL ≥ 1.5 放量", "points": "+10"},
         {"condition": "RVOL < 0.5 縮量", "points": "-5"},
     ],
     "params": [
         {"key": "rvol_surge", "label": "放量倍數", "value": 1.5, "default": 1.5, "min": 1.0, "max": 3.0, "step": 0.1},
         {"key": "rvol_dry", "label": "縮量閾值", "value": 0.5, "default": 0.5, "min": 0.2, "max": 0.8, "step": 0.1},
     ],
     "external": {"FinGPT": "成交量分析", "OpenAlice": "量能確認"},
    },
    {"id": "tech_vwap", "category": "技術指標", "name": "VWAP 量價加權均價", "feature": "stock_analysis",
     "formula": "TP=(H+L+C)/3, VWAP=Σ(TP×V)/Σ(V) [20日], Dist%=(C/VWAP-1)×100",
     "scoring": [
         {"condition": "Dist > +3% 強勢", "points": "+5"},
         {"condition": "Dist < -3% 弱勢", "points": "-5"},
     ],
     "params": [
         {"key": "vwap_period", "label": "計算天數", "value": 20, "default": 20, "min": 5, "max": 60, "step": 5},
         {"key": "vwap_threshold", "label": "偏離閾值%", "value": 3.0, "default": 3.0, "min": 1.0, "max": 10.0, "step": 0.5},
     ],
     "external": {},
    },
    {"id": "tech_rsi", "category": "技術指標", "name": "RSI 相對強弱指標", "feature": "stock_analysis",
     "formula": "RSI = 100 - 100/(1 + AvgGain/AvgLoss) [14期]",
     "scoring": [
         {"condition": "RSI < 30 超賣", "points": "+10"},
         {"condition": "RSI > 70 超買", "points": "-10"},
     ],
     "params": [
         {"key": "rsi_period", "label": "計算期數", "value": 14, "default": 14, "min": 5, "max": 30, "step": 1},
         {"key": "rsi_oversold", "label": "超賣閾值", "value": 30, "default": 30, "min": 15, "max": 40, "step": 5},
         {"key": "rsi_overbought", "label": "超買閾值", "value": 70, "default": 70, "min": 60, "max": 85, "step": 5},
     ],
     "external": {"FinGPT": "RSI(14)", "OpenAlice": "RSI標準"},
    },
    {"id": "tech_rs", "category": "技術指標", "name": "相對強弱 RS vs Benchmark", "feature": "stock_analysis",
     "formula": "RS = 個股報酬率 - 基準報酬率 [1W/1M/3M]",
     "scoring": [
         {"condition": "1M RS > +5% 強勢", "points": "+10"},
         {"condition": "1M RS < -5% 弱勢", "points": "-10"},
     ],
     "params": [
         {"key": "rs_threshold", "label": "強弱閾值%", "value": 5.0, "default": 5.0, "min": 2.0, "max": 15.0, "step": 1.0},
     ],
     "external": {"RD-Agent": "相對動量因子"},
    },
    {"id": "tech_obv", "category": "技術指標", "name": "OBV 能量潮", "feature": "stock_analysis",
     "formula": "C↑:OBV+=V, C↓:OBV-=V, 比較OBV今 vs OBV_20日前",
     "scoring": [
         {"condition": "OBV↑且分數>40 量能確認", "points": "+5"},
         {"condition": "OBV↓且分數<60 量能背離", "points": "-5"},
     ],
     "params": [
         {"key": "obv_lookback", "label": "回看天數", "value": 20, "default": 20, "min": 5, "max": 60, "step": 5},
     ],
     "external": {"FinGPT": "OBV分析"},
    },
    {"id": "tech_mfi", "category": "技術指標", "name": "MFI 資金流量指標", "feature": "stock_analysis",
     "formula": "TP=(H+L+C)/3, MFI=100-100/(1+PosMF/NegMF) [14期]",
     "scoring": [
         {"condition": "MFI > 80 超買", "points": "-5"},
         {"condition": "MFI < 20 超賣", "points": "+5"},
     ],
     "params": [
         {"key": "mfi_period", "label": "計算期數", "value": 14, "default": 14, "min": 5, "max": 30, "step": 1},
         {"key": "mfi_overbought", "label": "超買閾值", "value": 80, "default": 80, "min": 60, "max": 95, "step": 5},
         {"key": "mfi_oversold", "label": "超賣閾值", "value": 20, "default": 20, "min": 5, "max": 40, "step": 5},
     ],
     "external": {},
    },
    {"id": "tech_divergence", "category": "技術指標", "name": "量價背離偵測", "feature": "stock_analysis",
     "formula": "price_chg=C[-1]/C[-11]-1, vol_chg=MA10(V近)/MA10(V前)-1",
     "scoring": [
         {"condition": "價漲>3%且量跌>15% 頂背離", "points": "-8"},
         {"condition": "價跌>3%且量跌>15% 底背離", "points": "+5"},
     ],
     "params": [
         {"key": "div_price_pct", "label": "價格變動閾值%", "value": 3.0, "default": 3.0, "min": 1.0, "max": 10.0, "step": 0.5},
         {"key": "div_vol_pct", "label": "量能萎縮閾值%", "value": 15.0, "default": 15.0, "min": 5.0, "max": 30.0, "step": 5.0},
     ],
     "external": {},
    },
    {"id": "tech_fundamental", "category": "技術指標", "name": "基本面估值", "feature": "stock_analysis",
     "formula": "PE/PB/ROE/EPS/殖利率 from yfinance",
     "scoring": [
         {"condition": "0 < PE < 15 低估", "points": "+5"},
         {"condition": "PE > 40 高估", "points": "-5"},
         {"condition": "殖利率 > 4% 高息", "points": "+3"},
     ],
     "params": [
         {"key": "pe_low", "label": "PE低估上限", "value": 15, "default": 15, "min": 5, "max": 25, "step": 1},
         {"key": "pe_high", "label": "PE高估下限", "value": 40, "default": 40, "min": 25, "max": 80, "step": 5},
         {"key": "dy_threshold", "label": "高息殖利率%", "value": 4.0, "default": 4.0, "min": 2.0, "max": 8.0, "step": 0.5},
     ],
     "external": {"FinGPT": "財務比率分析", "RD-Agent": "基本面因子"},
    },
    # ── B. Alpha 因子庫 ──
    {"id": "alpha_momentum", "category": "Alpha因子", "name": "動量因子群", "feature": "alpha_factors",
     "formula": "mom_Nd = (C[-1]/C[-N-1] - 1)×100, N=5/10/20/60",
     "scoring": [],
     "params": [
         {"key": "mom_periods", "label": "動量期數(逗號分隔)", "value": "5,10,20,60", "default": "5,10,20,60", "min": None, "max": None, "step": None},
     ],
     "external": {"RD-Agent": "Alpha158動量因子, IC衰退→AI重新生成", "FinGPT": "動量策略"},
    },
    {"id": "alpha_volatility", "category": "Alpha因子", "name": "波動/振幅因子", "feature": "alpha_factors",
     "formula": "vol_Nd=std(returns[-N:])×100, amplitude_Nd=mean((H-L)/C)×100",
     "scoring": [],
     "params": [
         {"key": "vol_periods", "label": "波動期數", "value": "5,20", "default": "5,20", "min": None, "max": None, "step": None},
     ],
     "external": {"RD-Agent": "波動率因子"},
    },
    {"id": "alpha_volume", "category": "Alpha因子", "name": "量能比率因子", "feature": "alpha_factors",
     "formula": "vol_ratio_5_20=MA5(V)/MA20(V), vol_chg_5d=(V[-1]/V[-6]-1)×100",
     "scoring": [],
     "params": [],
     "external": {"RD-Agent": "量能因子"},
    },
    {"id": "alpha_bias", "category": "Alpha因子", "name": "乖離/位置因子", "feature": "alpha_factors",
     "formula": "bias_Nd=(C/MA_N-1)×100, price_pos_60d=(C-L60)/(H60-L60)",
     "scoring": [
         {"condition": "price_pos_60d > 0.9 (60日高檔)", "points": "-2"},
         {"condition": "price_pos_60d < 0.1 (60日低檔)", "points": "+2"},
     ],
     "params": [
         {"key": "pos_high", "label": "高檔警示位置", "value": 0.9, "default": 0.9, "min": 0.7, "max": 1.0, "step": 0.05},
         {"key": "pos_low", "label": "低檔機會位置", "value": 0.1, "default": 0.1, "min": 0.0, "max": 0.3, "step": 0.05},
     ],
     "external": {"RD-Agent": "Price Position因子"},
    },
    {"id": "alpha_candle", "category": "Alpha因子", "name": "K線形態/排名因子", "feature": "alpha_factors",
     "formula": "upper_shadow=(H-max(C,Cp))/C×100, lower_shadow=(min(C,Cp)-L)/C×100, close_rank_60d, volume_rank_60d",
     "scoring": [],
     "params": [],
     "external": {"OpenAlice": "Chart pattern recognition"},
    },
    # ── C. 情緒系統 ──
    {"id": "sentiment_fusion", "category": "情緒分析", "name": "多源情緒融合", "feature": "sentiment_fusion",
     "formula": "composite = Σ(score×weight)/Σ(weight), sources: AI/新聞/技術面",
     "scoring": [
         {"condition": "composite ≥ 75 偏多", "points": "標記"},
         {"condition": "composite ≤ 25 偏空", "points": "標記"},
     ],
     "params": [
         {"key": "sent_w_ai", "label": "AI情緒權重", "value": 0.5, "default": 0.5, "min": 0.0, "max": 1.0, "step": 0.1},
         {"key": "sent_w_news", "label": "新聞情緒權重", "value": 0.3, "default": 0.3, "min": 0.0, "max": 1.0, "step": 0.1},
         {"key": "sent_w_tech", "label": "技術面情緒權重", "value": 0.2, "default": 0.2, "min": 0.0, "max": 1.0, "step": 0.1},
     ],
     "external": {"FinGPT": "情緒60%+強度40%加權, LoRA fine-tune F1=87.6%", "RD-Agent": "不使用情緒分析"},
    },
    {"id": "sentiment_momentum", "category": "情緒分析", "name": "情緒動量", "feature": "sentiment_fusion",
     "formula": "delta=latest-oldest [7日], avg_delta=delta/(N-1)",
     "scoring": [
         {"condition": "avg_delta > 2 情緒改善", "points": "+5"},
         {"condition": "avg_delta < -2 情緒惡化", "points": "-5"},
     ],
     "params": [
         {"key": "sent_mom_lookback", "label": "回看天數", "value": 7, "default": 7, "min": 3, "max": 14, "step": 1},
         {"key": "sent_mom_threshold", "label": "趨勢閾值", "value": 2.0, "default": 2.0, "min": 0.5, "max": 5.0, "step": 0.5},
     ],
     "external": {},
    },
    {"id": "sentiment_reversal", "category": "情緒分析", "name": "極端情緒反轉", "feature": "sentiment_fusion",
     "formula": "score≥overbought → 逆向警示, score≤oversold → 逆向機會",
     "scoring": [
         {"condition": "情緒 ≥ 85 過熱", "points": "-4"},
         {"condition": "情緒 ≤ 15 冰點", "points": "+4"},
     ],
     "params": [
         {"key": "sent_overbought", "label": "過熱閾值", "value": 85, "default": 85, "min": 70, "max": 95, "step": 5},
         {"key": "sent_oversold", "label": "冰點閾值", "value": 15, "default": 15, "min": 5, "max": 30, "step": 5},
     ],
     "external": {"FinGPT": "情緒極端值逆向"},
    },
    # ── D. 板塊輪動 ──
    {"id": "sector_rotation", "category": "板塊輪動", "name": "GICS板塊動量排名", "feature": "sector_rotation",
     "formula": "rs_vs_spy = sector_1M_return - SPY_1M_return, 排名by 1M return",
     "scoring": [
         {"condition": "排名 ≤ 3 強勢板塊", "points": "+5"},
         {"condition": "排名 ≥ 9 弱勢板塊", "points": "-3"},
     ],
     "params": [
         {"key": "sector_strong", "label": "強勢RS閾值%", "value": 2.0, "default": 2.0, "min": 0.5, "max": 5.0, "step": 0.5},
         {"key": "sector_weak", "label": "弱勢RS閾值%", "value": -2.0, "default": -2.0, "min": -5.0, "max": -0.5, "step": 0.5},
         {"key": "sector_top_n", "label": "強勢前N名", "value": 3, "default": 3, "min": 1, "max": 5, "step": 1},
     ],
     "external": {"RD-Agent": "無板塊輪動", "OpenAlice": "多資產覆蓋"},
    },
    # ── E. 事件驅動 ──
    {"id": "event_driven", "category": "事件驅動", "name": "事件偵測與新聞情緒", "feature": "event_driven",
     "formula": "財報日/除息日偵測(14天窗口), 新聞正負面關鍵字比對",
     "scoring": [
         {"condition": "正面新聞匹配", "points": "+3"},
         {"condition": "負面新聞匹配", "points": "-3"},
     ],
     "params": [
         {"key": "event_window", "label": "事件偵測窗口(天)", "value": 14, "default": 14, "min": 7, "max": 30, "step": 1},
     ],
     "external": {"FinGPT": "新聞情緒NLP F1=87.6%", "RD-Agent": "事件驅動因子"},
    },
    # ── F. 風控/總經 ──
    {"id": "macro_vix", "category": "風控總經", "name": "VIX 恐慌指數警戒", "feature": "macro_risk",
     "formula": "VIX > threshold → alert+1",
     "scoring": [{"condition": "VIX > 35", "points": "觸發警戒"}],
     "params": [
         {"key": "macro_vix_alert", "label": "VIX警戒值", "value": 35, "default": 35, "min": 20, "max": 50, "step": 5},
     ],
     "external": {},
    },
    {"id": "macro_us10y", "category": "風控總經", "name": "US10Y 公債殖利率警戒", "feature": "macro_risk",
     "formula": "US10Y > threshold → alert+1",
     "scoring": [{"condition": "US10Y > 5%", "points": "觸發警戒"}],
     "params": [
         {"key": "macro_us10y_alert", "label": "US10Y警戒值%", "value": 5.0, "default": 5.0, "min": 3.0, "max": 7.0, "step": 0.5},
     ],
     "external": {},
    },
    {"id": "macro_dxy", "category": "風控總經", "name": "DXY 美元指數警戒", "feature": "macro_risk",
     "formula": "DXY月漲幅 > threshold → alert+1",
     "scoring": [{"condition": "月漲幅 > 3%", "points": "觸發警戒"}],
     "params": [
         {"key": "macro_dxy_alert", "label": "月漲幅閾值%", "value": 3.0, "default": 3.0, "min": 1.0, "max": 5.0, "step": 0.5},
     ],
     "external": {},
    },
    {"id": "macro_twii", "category": "風控總經", "name": "加權指數偏離警戒", "feature": "macro_risk",
     "formula": "TWII < MA20 × (1 + threshold) → alert+1",
     "scoring": [{"condition": "低於MA20超過5%", "points": "觸發警戒"}],
     "params": [
         {"key": "macro_twii_dev", "label": "偏離閾值%", "value": -5.0, "default": -5.0, "min": -10.0, "max": -2.0, "step": 0.5},
     ],
     "external": {},
    },
    {"id": "macro_risk_level", "category": "風控總經", "name": "風險等級判定", "feature": "macro_risk",
     "formula": "alert_count≥2→ALERT(30%), =1→CAUTION(60%), =0→NORMAL(100%)",
     "scoring": [
         {"condition": "ALERT(≥2警報) 倉位30%", "points": "封鎖買進"},
         {"condition": "CAUTION(1警報) 倉位60%", "points": "縮減"},
         {"condition": "NORMAL(0警報) 倉位100%", "points": "正常"},
     ],
     "params": [
         {"key": "risk_alert_count", "label": "ALERT觸發數", "value": 2, "default": 2, "min": 2, "max": 4, "step": 1},
         {"key": "risk_alert_scale", "label": "ALERT倉位%", "value": 30, "default": 30, "min": 10, "max": 50, "step": 5},
         {"key": "risk_caution_scale", "label": "CAUTION倉位%", "value": 60, "default": 60, "min": 30, "max": 80, "step": 5},
     ],
     "external": {},
    },
    # ── G. 出場規則 (策略頁連動) ──
    {"id": "exit_c", "category": "出場規則", "name": "EXIT_C 移動止盈", "feature": "position_lifecycle",
     "formula": "max_profit≥trigger → 回撤≥drawdown → 出場",
     "scoring": [],
     "params": [
         {"key": "exit_c_swing_profit", "label": "波段觸發利潤%", "value": 8.0, "default": 8.0, "min": 3.0, "max": 20.0, "step": 1.0},
         {"key": "exit_c_swing_drawdown", "label": "波段回撤%", "value": 2.0, "default": 2.0, "min": 0.5, "max": 5.0, "step": 0.5},
         {"key": "exit_c_day_profit", "label": "當沖觸發利潤%", "value": 3.0, "default": 3.0, "min": 1.0, "max": 10.0, "step": 0.5},
         {"key": "exit_c_day_drawdown", "label": "當沖回撤%", "value": 1.0, "default": 1.0, "min": 0.3, "max": 3.0, "step": 0.1},
     ],
     "external": {"OpenAlice": "版本歷史追蹤出場決策"},
     "strategy_link": "EXIT_C",
    },
    {"id": "exit_d", "category": "出場規則", "name": "EXIT_D 絕對停損 ⚠不可關閉", "feature": "position_lifecycle",
     "formula": "PnL ≤ -threshold% → 強制出場",
     "scoring": [],
     "params": [
         {"key": "exit_d_threshold", "label": "停損閾值%", "value": 5.0, "default": 5.0, "min": 3.0, "max": 10.0, "step": 0.5},
     ],
     "external": {},
     "strategy_link": "EXIT_D",
    },
    # ── H. 進場訊號 (策略頁連動) ──
    {"id": "buy_a", "category": "進場訊號", "name": "BUY_A 假跌破破底翻", "feature": "stock_analysis",
     "formula": "MACD金叉 + Price>MA20 + RVOL≥1.5",
     "scoring": [],
     "params": [
         {"key": "buy_a_breach_min", "label": "跌破最短(分)", "value": 15, "default": 15, "min": 5, "max": 60, "step": 5},
         {"key": "buy_a_breach_max", "label": "跌破最長(分)", "value": 30, "default": 30, "min": 10, "max": 90, "step": 5},
         {"key": "buy_a_outside_min", "label": "外盤最少筆", "value": 5, "default": 5, "min": 1, "max": 20, "step": 1},
     ],
     "external": {},
     "strategy_link": "BUY_A",
    },
    {"id": "buy_b", "category": "進場訊號", "name": "BUY_B 主力量價突破", "feature": "stock_analysis",
     "formula": "MA5↑穿MA10 + RVOL≥vol_ratio_min",
     "scoring": [],
     "params": [
         {"key": "buy_b_vol_ratio", "label": "量比門檻", "value": 2.5, "default": 2.5, "min": 1.0, "max": 5.0, "step": 0.5},
         {"key": "buy_b_outside", "label": "外盤連續", "value": 5, "default": 5, "min": 3, "max": 15, "step": 1},
         {"key": "buy_b_large", "label": "大單門檻(張)", "value": 100, "default": 100, "min": 50, "max": 500, "step": 50},
     ],
     "external": {},
     "strategy_link": "BUY_B",
    },
    {"id": "low_buy", "category": "進場訊號", "name": "LOW_BUY 年線超跌低吸", "feature": "stock_analysis",
     "formula": "Price < MA240 × (1 + bias) → 超跌",
     "scoring": [],
     "params": [
         {"key": "low_buy_bias", "label": "乖離閾值%", "value": -15, "default": -15, "min": -30, "max": -5, "step": 1},
     ],
     "external": {},
     "strategy_link": "LOW_BUY",
    },
    {"id": "squeeze_break", "category": "進場訊號", "name": "SQUEEZE_BREAK 籌碼擠壓突破", "feature": "stock_analysis",
     "formula": "突破N日高 + RVOL≥vol_ratio",
     "scoring": [],
     "params": [
         {"key": "sq_days", "label": "高點回看天數", "value": 20, "default": 20, "min": 5, "max": 60, "step": 5},
         {"key": "sq_vol_ratio", "label": "量比門檻", "value": 2.0, "default": 2.0, "min": 1.0, "max": 5.0, "step": 0.5},
     ],
     "external": {},
     "strategy_link": "SQUEEZE_BREAK",
    },
    # ── I. 回測引擎 ──
    {"id": "bt_walkforward", "category": "回測引擎", "name": "Walk-Forward 防過擬合", "feature": "backtest",
     "formula": "滑動窗口: train→test→slide, overfit_ratio=avg_train/avg_test",
     "scoring": [
         {"condition": "overfit_ratio < 2.0", "points": "穩健"},
         {"condition": "overfit_ratio ≥ 3.0", "points": "過擬合風險"},
     ],
     "params": [
         {"key": "wf_train_months", "label": "訓練期(月)", "value": 6, "default": 6, "min": 3, "max": 12, "step": 1},
         {"key": "wf_test_months", "label": "測試期(月)", "value": 2, "default": 2, "min": 1, "max": 6, "step": 1},
         {"key": "wf_overfit_warn", "label": "過擬合警示比", "value": 2.0, "default": 2.0, "min": 1.5, "max": 5.0, "step": 0.5},
         {"key": "wf_overfit_danger", "label": "過擬合危險比", "value": 3.0, "default": 3.0, "min": 2.0, "max": 10.0, "step": 0.5},
     ],
     "external": {"RD-Agent": "CSI300 train 2016-2020 / test 2022-2025, Sharpe 0.5968"},
    },
    {"id": "bt_cost", "category": "回測引擎", "name": "交易成本", "feature": "backtest",
     "formula": "total_cost = (commission×discount + tax) × trade_value",
     "scoring": [],
     "params": [
         {"key": "bt_commission", "label": "手續費率%", "value": 0.1425, "default": 0.1425, "min": 0.01, "max": 0.5, "step": 0.01},
         {"key": "bt_tax", "label": "交易稅率%", "value": 0.3, "default": 0.3, "min": 0.0, "max": 0.5, "step": 0.05},
         {"key": "bt_discount", "label": "手續費折扣%", "value": 60, "default": 60, "min": 20, "max": 100, "step": 5},
     ],
     "external": {},
    },
    # ── J. 因子驗證 ──
    {"id": "ic_validation", "category": "因子驗證", "name": "IC/ICIR 因子有效性", "feature": "factor_ic",
     "formula": "IC = Spearman(factor_values, forward_returns), ICIR = mean(IC)/std(IC)",
     "scoring": [
         {"condition": "|IC| > 0.1 強", "points": "有效"},
         {"condition": "|IC| > 0.05 中", "points": "參考"},
         {"condition": "|IC| ≤ 0.05 弱", "points": "無效"},
     ],
     "params": [
         {"key": "ic_forward_days", "label": "前瞻期(天)", "value": 20, "default": 20, "min": 5, "max": 60, "step": 5},
         {"key": "ic_strong", "label": "強因子IC閾值", "value": 0.1, "default": 0.1, "min": 0.05, "max": 0.2, "step": 0.01},
         {"key": "ic_medium", "label": "中因子IC閾值", "value": 0.05, "default": 0.05, "min": 0.02, "max": 0.1, "step": 0.01},
     ],
     "external": {"RD-Agent": "IC=0.0532, 自動生成因子超越Alpha158", "FinGPT": "情緒因子IC"},
    },
    {"id": "multifactor", "category": "因子驗證", "name": "多因子組合排名", "feature": "multi_factor",
     "formula": "Z = (factor - mean) / std, 負向因子取反, composite = mean(Z_all)",
     "scoring": [],
     "params": [
         {"key": "mf_factors", "label": "組合因子", "value": "mom_20d,vol_ratio_5_20,bias_20d,price_pos_60d,vol_price_corr_20d", "default": "mom_20d,vol_ratio_5_20,bias_20d,price_pos_60d,vol_price_corr_20d", "min": None, "max": None, "step": None},
     ],
     "external": {"RD-Agent": "因子+模型聯合優化, 用70%更少因子達2×報酬"},
    },
    # ── K. AI 評分 ──
    {"id": "ai_confidence", "category": "AI評分", "name": "AI信心度計算", "feature": "ai_recommend",
     "formula": "conf = 0.50 + base×0.28 + src_bonus - risk_penalty, clamp [0.28, 0.82]",
     "scoring": [
         {"condition": "conf ≥ 0.70 推薦通知", "points": "推播"},
         {"condition": "VIX > 25", "points": "-0.08 懲罰"},
     ],
     "params": [
         {"key": "ai_base_weight", "label": "技術分權重", "value": 0.28, "default": 0.28, "min": 0.1, "max": 0.5, "step": 0.02},
         {"key": "ai_src_bonus", "label": "確認源加分(每個)", "value": 0.05, "default": 0.05, "min": 0.01, "max": 0.1, "step": 0.01},
         {"key": "ai_vix_penalty", "label": "VIX>25懲罰", "value": 0.08, "default": 0.08, "min": 0.0, "max": 0.2, "step": 0.02},
         {"key": "ai_notify_threshold", "label": "推薦通知門檻", "value": 0.70, "default": 0.70, "min": 0.5, "max": 0.9, "step": 0.05},
         {"key": "ai_conf_min", "label": "最低信心度", "value": 0.28, "default": 0.28, "min": 0.1, "max": 0.4, "step": 0.02},
         {"key": "ai_conf_max", "label": "最高信心度", "value": 0.82, "default": 0.82, "min": 0.7, "max": 0.95, "step": 0.02},
     ],
     "external": {"FinGPT": "LoRA rank=8 alpha=32 fine-tune", "RD-Agent": "Co-STEER代碼生成"},
    },
    # ── L. 最終評分 ──
    {"id": "final_score", "category": "最終評分", "name": "綜合分數與方向判斷", "feature": "stock_analysis",
     "formula": "final = raw + 40, clamp [0, 100]",
     "scoring": [
         {"condition": "score ≥ 62", "points": "BUY"},
         {"condition": "score ≤ 38", "points": "SELL"},
         {"condition": "38 < score < 62", "points": "HOLD"},
     ],
     "params": [
         {"key": "score_offset", "label": "基準偏移", "value": 40, "default": 40, "min": 30, "max": 50, "step": 5},
         {"key": "score_buy", "label": "買進門檻", "value": 62, "default": 62, "min": 55, "max": 75, "step": 1},
         {"key": "score_sell", "label": "賣出門檻", "value": 38, "default": 38, "min": 25, "max": 45, "step": 1},
     ],
     "external": {},
    },
]


@app.get("/api/formula-registry")
def get_formula_registry():
    """回傳公式註冊表，含當前參數值（可能被 override）"""
    result = []
    for entry in FORMULA_REGISTRY:
        e = {k: v for k, v in entry.items()}
        if e.get("params"):
            e["params"] = []
            for p in entry["params"]:
                pp = dict(p)
                pp["value"] = _formula_overrides.get(pp["key"], pp["default"])
                e["params"].append(pp)
        result.append(e)
    return result


@app.post("/api/formula-registry/params")
def update_formula_params(data: dict, _: None = Depends(require_token)):
    """更新公式參數（記憶體內，重啟回預設）"""
    changes = data.get("changes", {})
    valid_keys = set()
    for entry in FORMULA_REGISTRY:
        for p in entry.get("params", []):
            valid_keys.add(p["key"])
    applied = {}
    for k, v in changes.items():
        if k not in valid_keys:
            continue
        _formula_overrides[k] = v
        applied[k] = v
    return {"ok": True, "applied": applied, "total_overrides": len(_formula_overrides)}


@app.post("/api/formula-registry/reset")
def reset_formula_params(_: None = Depends(require_token)):
    """重置所有參數為預設值"""
    _formula_overrides.clear()
    return {"ok": True, "message": "已重置為預設值"}


# ── WebSocket Tick ────────────────────────────────

@app.websocket("/ws/tick/{code}")
async def tick_ws(websocket: WebSocket, code: str):
    await websocket.accept()
    import shioaji as sj
    api = get_api()
    contract = api.Contracts.Stocks.get(code)
    if contract is None:
        await websocket.close(code=4004, reason=f"unknown code {code}")
        return

    q: asyncio.Queue = asyncio.Queue()
    loop = asyncio.get_running_loop()   # 修缺陷2: get_running_loop

    # 修缺陷1: 註冊到 subscriber registry，不覆寫全域 callback
    with _ws_subs_lock:
        first_sub = code not in _ws_subs or len(_ws_subs[code]) == 0
        _ws_subs.setdefault(code, []).append((loop, q))

    if first_sub:
        api.quote.subscribe(contract, quote_type=sj.constant.QuoteType.Tick,
                            version=sj.constant.QuoteVersion.v1)
        api.quote.subscribe(contract, quote_type=sj.constant.QuoteType.BidAsk,
                            version=sj.constant.QuoteVersion.v1)
    try:
        while True:
            try:
                data = await asyncio.wait_for(q.get(), timeout=30)
                await websocket.send_json(data)
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "ping"})
    except WebSocketDisconnect:
        pass
    finally:
        with _ws_subs_lock:
            subs = _ws_subs.get(code, [])
            _ws_subs[code] = [(l, x) for l, x in subs if x is not q]
            last_sub = len(_ws_subs[code]) == 0
        if last_sub:
            try:
                api.quote.unsubscribe(contract, quote_type=sj.constant.QuoteType.Tick,
                                      version=sj.constant.QuoteVersion.v1)
                api.quote.unsubscribe(contract, quote_type=sj.constant.QuoteType.BidAsk,
                                      version=sj.constant.QuoteVersion.v1)
            except Exception:
                pass

# ── 推播通知（LINE Messaging API / Webhook）──────
def _get_notify_config():
    con = db()
    cur = con.cursor()
    cur.execute("""SELECT key, value FROM risk_config WHERE key IN (
        'telegram_bot_token','telegram_chat_id','telegram_chat_names',
        'email_smtp_host','email_smtp_port','email_user','email_pass','email_to',
        'webhook_url','notify_enabled')""")
    rows = cur.fetchall()
    con.close()
    return {r[0]: r[1] for r in rows}

def _send_telegram(msg: str, cfg: dict) -> bool:
    """Send Telegram to multiple chat_ids (comma-separated)."""
    token = cfg.get("telegram_bot_token", "")
    raw_ids = cfg.get("telegram_chat_id", "")
    if not token or not raw_ids:
        return False
    chat_ids = [cid.strip() for cid in raw_ids.split(",") if cid.strip()]
    if not chat_ids:
        return False
    any_ok = False
    for cid in chat_ids:
        try:
            payload = json.dumps({"chat_id": cid, "text": f"📊 智慧投顧\n{msg}", "parse_mode": "HTML"}).encode("utf-8")
            req = urllib.request.Request(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data=payload,
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=10)
            any_ok = True
            print(f"[Telegram] 已推播至 {cid}")
        except Exception as e:
            print(f"[Telegram] 推播至 {cid} 失敗: {e}")
    return any_ok

def _send_email(msg: str, cfg: dict) -> bool:
    """Send email to multiple recipients (comma-separated)."""
    host = cfg.get("email_smtp_host", "")
    user = cfg.get("email_user", "")
    pwd = cfg.get("email_pass", "")
    raw_to = cfg.get("email_to", "")
    if not all([host, user, pwd, raw_to]):
        return False
    recipients = [addr.strip() for addr in raw_to.split(",") if addr.strip()]
    if not recipients:
        return False
    try:
        port = int(cfg.get("email_smtp_port", "587"))
        mime = MIMEMultipart("alternative")
        mime["From"] = user
        mime["To"] = ", ".join(recipients)
        mime["Subject"] = "📊 智慧投顧監控通知"
        mime.attach(MIMEText(msg, "plain", "utf-8"))
        with smtplib.SMTP(host, port, timeout=15) as s:
            s.starttls()
            s.login(user, pwd)
            s.sendmail(user, recipients, mime.as_string())
        print(f"[Email] 已推播至 {', '.join(recipients)}")
        return True
    except Exception as e:
        print(f"[Email] 推播失敗: {e}")
        return False

def _send_notification(msg: str):
    cfg = _get_notify_config()
    if cfg.get("notify_enabled", "1") != "1":
        return False
    sent = False
    # 1. Telegram Bot
    if _send_telegram(msg, cfg):
        sent = True
    # 2. Email SMTP
    if _send_email(msg, cfg):
        sent = True
    # 3. Webhook fallback (SSRF guard: https only)
    wh = cfg.get("webhook_url", "")
    if wh and wh.startswith("https://"):
        try:
            payload = json.dumps({"text": f"📊 智慧投顧\n{msg}", "source": "smart-investment-monitor"}).encode("utf-8")
            req = urllib.request.Request(wh, data=payload, headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=10)
            sent = True
        except Exception as e:
            print(f"[Webhook] 推播失敗: {e}")
    return sent

@app.post("/api/notify/test")
def test_notify():
    cfg = _get_notify_config()
    results = {}
    results["telegram"] = _send_telegram("🔔 測試推播成功！系統運作正常。", cfg)
    results["email"] = _send_email("🔔 測試推播成功！\n系統運作正常。", cfg)
    wh = cfg.get("webhook_url", "")
    if wh and wh.startswith("https://"):
        try:
            payload = json.dumps({"text": "🔔 測試推播成功！", "source": "smart-investment-monitor"}).encode("utf-8")
            req = urllib.request.Request(wh, data=payload, headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=10)
            results["webhook"] = True
        except Exception:
            results["webhook"] = False
    any_ok = any(results.values())
    channels = [k for k, v in results.items() if v]
    if any_ok:
        return {"ok": True, "message": f"已發送：{', '.join(channels)}", "detail": results}
    has_config = cfg.get("telegram_bot_token") or cfg.get("email_smtp_host") or wh
    if not has_config:
        return JSONResponse({"ok": False, "message": "尚未設定任何通知管道（Telegram / Email / Webhook）", "detail": results}, status_code=400)
    return JSONResponse({"ok": False, "message": "全部管道推播失敗，請檢查設定", "detail": results}, status_code=500)

# ══════════════════════════════════════════════════
# Phase 9: 策略管理 + 回測系統
# ══════════════════════════════════════════════════

MARKET_DB_PATH = Path(__file__).parent / "data" / "market.db"
MARKET_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def market_db():
    con = sqlite3.connect(str(MARKET_DB_PATH), check_same_thread=False, timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    return con

def _init_market_db():
    con = market_db()
    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_kbar (
            code    TEXT NOT NULL,
            market  TEXT NOT NULL DEFAULT 'TW',
            date    TEXT NOT NULL,
            open    REAL, high REAL, low REAL, close REAL,
            volume  INTEGER,
            PRIMARY KEY (code, market, date)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS backtest_result (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT,
            config      TEXT,
            summary     TEXT,
            trades      TEXT,
            equity_curve TEXT,
            created_at  TEXT
        )
    """)
    con.commit()
    con.close()

_init_market_db()

# ── 策略定義 ──────────────────────────────────────

STRATEGIES = [
    # ══ 系統內建策略 ══
    {
        "id": "BUY_A", "name": "假跌破破底翻", "direction": "BUY",
        "strat_type": "builtin", "formula_link": "buy_a",
        "description": "盤中跌破5MA後15-30分鐘內拉回，伴隨特大單或連續外盤敲進",
        "conditions": [
            "價格跌破 5日均線",
            "15-30 分鐘內拉回 5MA 之上",
            "拉回時：連續外盤 ≥5 筆 或 特大單 ≥1 筆",
        ],
        "fallback": "MACD金叉 + 站上MA20 + 量比≥1.5x",
        "params": [
            {"key": "breach_min", "label": "回站時間下限(分)", "default": 15, "min": 5, "max": 60},
            {"key": "breach_max", "label": "回站時間上限(分)", "default": 30, "min": 10, "max": 90},
            {"key": "outside_bid_min", "label": "連續外盤筆數", "default": 5, "min": 1, "max": 20},
            {"key": "vol_ratio_min", "label": "量比門檻(日K)", "default": 1.5, "min": 0.8, "max": 3.0},
        ],
        "timeframe": "5min / Tick",
        "enabled": True,
    },
    {
        "id": "BUY_B", "name": "主力量價突破", "direction": "BUY",
        "strat_type": "builtin", "formula_link": "buy_b",
        "description": "即時量比>2.5倍，連續5筆以上外盤成交，伴隨特大單",
        "conditions": [
            "量比(5min) > 2.5 倍",
            "連續外盤 ≥ 5 筆",
            "特大單(>100張) ≥ 1 筆",
        ],
        "fallback": "MA5 上穿 MA10 + 量比≥1.2x",
        "params": [
            {"key": "vol_ratio_min", "label": "量比門檻", "default": 2.5, "min": 1.0, "max": 5.0},
            {"key": "outside_count", "label": "外盤連續筆數", "default": 5, "min": 3, "max": 15},
            {"key": "large_lots", "label": "特大單(張)", "default": 100, "min": 50, "max": 500},
        ],
        "timeframe": "5min / Tick",
        "enabled": True,
    },
    {
        "id": "LOW_BUY", "name": "年線超跌低吸", "direction": "BUY",
        "strat_type": "builtin", "formula_link": "low_buy",
        "description": "股價低於240日均線(年線)15%以上，觸發左側低吸提示",
        "conditions": ["現價低於 MA240 × 0.85（乖離 -15%）"],
        "params": [
            {"key": "ma240_bias", "label": "年線乖離閾值(%)", "default": -15, "min": -30, "max": -5},
        ],
        "timeframe": "日K",
        "enabled": True,
    },
    {
        "id": "LOCK_BUY", "name": "正乖離鎖定買進", "direction": "BUY",
        "strat_type": "builtin",
        "description": "正乖離率>15%(MA5)，強勢不追高，鎖定買進權限",
        "conditions": ["(現價 - MA5) / MA5 × 100 > 15%"],
        "params": [
            {"key": "bias_threshold", "label": "乖離率閾值(%)", "default": 15, "min": 5, "max": 30},
        ],
        "timeframe": "日K",
        "enabled": True,
    },
    {
        "id": "SQUEEZE_BREAK", "name": "籌碼擠壓突破", "direction": "BUY",
        "strat_type": "builtin", "formula_link": "squeeze_break",
        "description": "突破近20日最高點，量比≥2倍",
        "conditions": ["現價突破近20日最高價", "量比 ≥ 2.0x"],
        "params": [
            {"key": "high_days", "label": "突破天數", "default": 20, "min": 5, "max": 60},
            {"key": "vol_ratio", "label": "量比門檻", "default": 2.0, "min": 1.0, "max": 5.0},
        ],
        "timeframe": "日K",
        "enabled": True,
    },
    {
        "id": "SQUEEZE_BUY", "name": "融券軋空突破", "direction": "BUY",
        "strat_type": "builtin",
        "description": "券資比>30%且突破前日高點，融券軋空強力買訊",
        "conditions": ["券資比 > 30%", "盤中突破前日最高價"],
        "params": [
            {"key": "msr_threshold", "label": "券資比門檻(%)", "default": 30, "min": 10, "max": 60},
        ],
        "timeframe": "日K + 籌碼",
        "enabled": True,
    },
    {
        "id": "EXIT_A", "name": "VWAP跌破賣出", "direction": "SELL",
        "strat_type": "builtin",
        "description": "跌破當日均價線(VWAP)，3分鐘內無法站回",
        "conditions": ["現價 < VWAP", "持續低於VWAP達3分鐘"],
        "params": [
            {"key": "vwap_fail_min", "label": "VWAP跌破確認(分)", "default": 3, "min": 1, "max": 10},
        ],
        "timeframe": "Tick",
        "enabled": True,
    },
    {
        "id": "EXIT_B", "name": "高檔爆量出貨", "direction": "SELL",
        "strat_type": "builtin",
        "description": "高檔區出現特大單砸向內盤，伴隨長上影線或黑K",
        "conditions": ["內盤特大單(>100張) ≥ 1 筆", "量比 ≥ 2.0x"],
        "fallback": "MACD死叉 + 量縮 < 0.8x",
        "params": [
            {"key": "large_sell_min", "label": "內盤大單筆數", "default": 1, "min": 1, "max": 5},
        ],
        "timeframe": "Tick / 1min K",
        "enabled": True,
    },
    {
        "id": "EXIT_C", "name": "移動止盈", "direction": "SELL",
        "strat_type": "builtin", "formula_link": "exit_c",
        "description": "波段：利潤達8%後回落2%；當沖：利潤達3%後回落1%",
        "conditions": [
            "波段：最高利潤 ≥ 8%，從最高回落 ≥ 2%",
            "當沖：最高利潤 ≥ 3%，從最高回落 ≥ 1%",
        ],
        "params": [
            {"key": "swing_profit", "label": "波段觸發利潤(%)", "default": 8, "min": 3, "max": 20},
            {"key": "swing_drawdown", "label": "波段回落閾值(%)", "default": 2, "min": 0.5, "max": 5},
            {"key": "day_profit", "label": "當沖觸發利潤(%)", "default": 3, "min": 1, "max": 10},
            {"key": "day_drawdown", "label": "當沖回落閾值(%)", "default": 1, "min": 0.3, "max": 3},
        ],
        "timeframe": "日K",
        "enabled": True,
    },
    {
        "id": "EXIT_D", "name": "絕對停損(保命鍵)", "direction": "SELL",
        "strat_type": "builtin", "formula_link": "exit_d",
        "description": "任何個股虧損達設定上限，強制市價停損，不可關閉",
        "conditions": ["帳面虧損 ≥ exit_d_threshold%（預設5%）"],
        "params": [
            {"key": "exit_d_threshold", "label": "停損閾值(%)", "default": 5, "min": 3, "max": 10},
        ],
        "timeframe": "即時",
        "enabled": True,
        "force_enabled": True,
    },
    {
        "id": "NEWS_BEARISH", "name": "利多不漲(NLP)", "direction": "SELL",
        "strat_type": "builtin",
        "description": "正面新聞但開高走低量大收黑，利多出盡防守賣出",
        "conditions": [
            "當日有正面新聞(關鍵字匹配)",
            "收盤跌 > 1%",
            "量比 > 1.5x",
        ],
        "params": [
            {"key": "news_drop_pct", "label": "跌幅閾值(%)", "default": 1.0, "min": 0.5, "max": 5.0},
            {"key": "news_vol_ratio", "label": "量比門檻", "default": 1.5, "min": 1.0, "max": 3.0},
        ],
        "timeframe": "日K + 新聞",
        "enabled": True,
    },
    {
        "id": "DAYTRADE_WARN", "name": "當沖比午盤防洗", "direction": "SELL",
        "strat_type": "builtin",
        "description": "當沖比>70%且12:30後跌破VWAP，當沖客倒貨賣壓",
        "conditions": [
            "前日當沖比 > 70%",
            "時間 ≥ 12:30",
            "現價 < VWAP 均價線",
        ],
        "params": [
            {"key": "dt_ratio_threshold", "label": "當沖比門檻(%)", "default": 70, "min": 40, "max": 90},
            {"key": "dt_block_hour", "label": "阻斷時間(時)", "default": 12.5, "min": 11, "max": 13},
        ],
        "timeframe": "日K + 當沖比",
        "enabled": True,
    },
    # ══ 公式衍生策略（技術指標→進出場訊號）══
    {
        "id": "KD_CROSS", "name": "KD黃金/死亡交叉", "direction": "BUY",
        "strat_type": "builtin", "formula_link": "tech_kd",
        "description": "K線上穿D線(金叉)且K<80買進；K線下穿D線(死叉)且K>20賣出",
        "conditions": [
            "KD金叉：K由下往上穿越D線",
            "K值 < 80（非超買區確認）",
            "反向：KD死叉 + K > 20 → 賣出提示",
        ],
        "params": [
            {"key": "kd_period", "label": "KD回看期", "default": 9, "min": 5, "max": 20},
            {"key": "kd_overbought", "label": "超買閾值", "default": 80, "min": 60, "max": 95},
            {"key": "kd_oversold", "label": "超賣閾值", "default": 20, "min": 5, "max": 40},
        ],
        "timeframe": "日K",
        "enabled": True,
    },
    {
        "id": "MACD_CROSS", "name": "MACD金叉/死叉", "direction": "BUY",
        "strat_type": "builtin", "formula_link": "tech_macd",
        "description": "DIF上穿MACD信號線(金叉)買進，死叉時賣出警示",
        "conditions": [
            "MACD金叉：DIF由下往上穿越信號線",
            "多方確認：DIF > 0 且 MACD > 0",
            "反向：MACD死叉 → 賣出提示",
        ],
        "params": [
            {"key": "macd_fast", "label": "快線EMA", "default": 12, "min": 5, "max": 20},
            {"key": "macd_slow", "label": "慢線EMA", "default": 26, "min": 15, "max": 40},
            {"key": "macd_signal", "label": "訊號線EMA", "default": 9, "min": 5, "max": 15},
        ],
        "timeframe": "日K",
        "enabled": True,
    },
    {
        "id": "RSI_EXTREME", "name": "RSI超賣反彈", "direction": "BUY",
        "strat_type": "builtin", "formula_link": "tech_rsi",
        "description": "RSI跌破超賣線後回升買進，突破超買線後回落賣出",
        "conditions": [
            "RSI < 30 進入超賣區",
            "RSI從超賣區回升突破30 → 買進",
            "反向：RSI > 70 超買回落 → 賣出提示",
        ],
        "params": [
            {"key": "rsi_period", "label": "計算期數", "default": 14, "min": 5, "max": 30},
            {"key": "rsi_oversold", "label": "超賣閾值", "default": 30, "min": 15, "max": 40},
            {"key": "rsi_overbought", "label": "超買閾值", "default": 70, "min": 60, "max": 85},
        ],
        "timeframe": "日K",
        "enabled": True,
    },
    {
        "id": "MA_ALIGN", "name": "均線多頭排列", "direction": "BUY",
        "strat_type": "builtin", "formula_link": "tech_ma",
        "description": "MA5>MA10>MA20>MA60完美多頭排列買進，跌破MA20警示",
        "conditions": [
            "Price > MA5 > MA10 > MA20 > MA60",
            "各均線斜率向上",
            "反向：價格跌破MA20 → 賣出警示",
        ],
        "params": [
            {"key": "ma_short", "label": "短均線", "default": 5, "min": 3, "max": 10},
            {"key": "ma_mid", "label": "中均線", "default": 20, "min": 10, "max": 30},
            {"key": "ma_long", "label": "長均線", "default": 60, "min": 40, "max": 120},
        ],
        "timeframe": "日K",
        "enabled": True,
    },
    {
        "id": "VOL_DIVERGENCE", "name": "量價背離警示", "direction": "SELL",
        "strat_type": "builtin", "formula_link": "tech_divergence",
        "description": "價格上漲但量能萎縮(頂背離)賣出，價跌量縮(底背離)為潛在買點",
        "conditions": [
            "頂背離：10日價漲>3% 且 量縮>15% → 賣出",
            "底背離：10日價跌>3% 且 量縮>15% → 買進機會",
        ],
        "params": [
            {"key": "div_price_pct", "label": "價格變動閾值%", "default": 3.0, "min": 1.0, "max": 10.0},
            {"key": "div_vol_pct", "label": "量能萎縮閾值%", "default": 15.0, "min": 5.0, "max": 30.0},
        ],
        "timeframe": "日K",
        "enabled": True,
    },
    {
        "id": "SENTIMENT_REVERSAL", "name": "極端情緒反轉", "direction": "BUY",
        "strat_type": "builtin", "formula_link": "sentiment_reversal",
        "description": "情緒冰點(<=15)逆向買進機會，情緒過熱(>=85)逆向賣出警示",
        "conditions": [
            "情緒分數 <= 15 (冰點) → 逆向買進",
            "情緒分數 >= 85 (過熱) → 逆向賣出",
        ],
        "params": [
            {"key": "sent_overbought", "label": "過熱閾值", "default": 85, "min": 70, "max": 95},
            {"key": "sent_oversold", "label": "冰點閾值", "default": 15, "min": 5, "max": 30},
        ],
        "timeframe": "日K + 情緒",
        "enabled": True,
    },
    {
        "id": "MACRO_RISK_BLOCK", "name": "總經風險封鎖", "direction": "SELL",
        "strat_type": "builtin", "formula_link": "macro_risk_level",
        "description": "VIX/US10Y/DXY/TWII多重警報觸發時，降低倉位或封鎖買進",
        "conditions": [
            "ALERT(>=2警報) → 倉位降至30%，封鎖買進",
            "CAUTION(1警報) → 倉位降至60%",
        ],
        "params": [
            {"key": "risk_alert_count", "label": "ALERT觸發數", "default": 2, "min": 2, "max": 4},
            {"key": "risk_alert_scale", "label": "ALERT倉位%", "default": 30, "min": 10, "max": 50},
            {"key": "risk_caution_scale", "label": "CAUTION倉位%", "default": 60, "min": 30, "max": 80},
        ],
        "timeframe": "即時",
        "enabled": True,
    },
    # ══ 日線級進場策略（Phase 1 新增）══
    {
        "id": "DONCHIAN_BREAK", "name": "唐奇安突破", "direction": "BUY",
        "strat_type": "builtin", "formula_link": "donchian_break",
        "description": "收盤突破N日最高價，經典趨勢跟蹤策略",
        "conditions": ["收盤價 > 過去N日最高價"],
        "params": [
            {"key": "donchian_period", "label": "突破天數", "default": 20, "min": 5, "max": 60},
        ],
        "timeframe": "日K",
        "enabled": True,
    },
    {
        "id": "MA_PULLBACK", "name": "均線回踩買入", "direction": "BUY",
        "strat_type": "builtin", "formula_link": "ma_pullback",
        "description": "價格回踩MA20且不破MA60，收紅K確認支撐",
        "conditions": [
            "最低價觸及或跌破 MA20",
            "收盤價 > MA60（長期趨勢完好）",
            "收紅K（收盤 > 開盤）",
        ],
        "params": [
            {"key": "pullback_ma_short", "label": "短均線", "default": 20, "min": 10, "max": 30},
            {"key": "pullback_ma_long", "label": "長均線", "default": 60, "min": 40, "max": 120},
        ],
        "timeframe": "日K",
        "enabled": True,
    },
    {
        "id": "BB_SQUEEZE", "name": "布林收縮突破", "direction": "BUY",
        "strat_type": "builtin", "formula_link": "bb_squeeze",
        "description": "布林帶寬收縮到近60日最低後突破上軌，波動爆發買入",
        "conditions": [
            "布林帶寬 < 近60日最低帶寬",
            "收盤突破布林上軌",
        ],
        "params": [
            {"key": "bb_period", "label": "布林週期", "default": 20, "min": 10, "max": 30},
            {"key": "bb_squeeze_lookback", "label": "收縮比較天數", "default": 60, "min": 20, "max": 120},
        ],
        "timeframe": "日K",
        "enabled": True,
    },
    {
        "id": "VOL_BREAKOUT", "name": "放量突破", "direction": "BUY",
        "strat_type": "builtin", "formula_link": "vol_breakout",
        "description": "成交量超過20日均量2倍且收盤漲幅>1%，放量突破確認",
        "conditions": [
            "成交量 > 20日均量 × 2",
            "收盤漲幅 > 1%",
        ],
        "params": [
            {"key": "vol_multi", "label": "量比倍數", "default": 2.0, "min": 1.5, "max": 5.0},
            {"key": "price_chg_min", "label": "最低漲幅(%)", "default": 1.0, "min": 0.5, "max": 3.0},
        ],
        "timeframe": "日K",
        "enabled": True,
    },
    # ══ 新出場策略（Phase 1 新增）══
    {
        "id": "EXIT_TRAIL", "name": "移動停利", "direction": "SELL",
        "strat_type": "builtin", "formula_link": "exit_trail",
        "description": "從持倉最高點回落X%時出場，保護獲利",
        "conditions": ["(持倉最高價 - 現價) / 持倉最高價 ≥ X%"],
        "params": [
            {"key": "trail_pct", "label": "回落比例(%)", "default": 5, "min": 2, "max": 15},
        ],
        "timeframe": "日K",
        "enabled": True,
    },
    {
        "id": "EXIT_NDAY", "name": "固定天數出場", "direction": "SELL",
        "strat_type": "builtin", "formula_link": "exit_nday",
        "description": "持倉滿N天強制出場，用於回測比較不同持有期",
        "conditions": ["持倉天數 ≥ N"],
        "params": [
            {"key": "hold_days", "label": "持倉天數", "default": 20, "min": 1, "max": 60},
        ],
        "timeframe": "日K",
        "enabled": True,
    },
    {
        "id": "EXIT_TARGET", "name": "固定停利", "direction": "SELL",
        "strat_type": "builtin", "formula_link": "exit_target",
        "description": "達到目標報酬X%時出場",
        "conditions": ["(現價 - 成本) / 成本 ≥ X%"],
        "params": [
            {"key": "target_pct", "label": "目標報酬(%)", "default": 10, "min": 3, "max": 30},
        ],
        "timeframe": "日K",
        "enabled": True,
    },
    # ══ 自訂策略 ══
    {
        "id": "WIFE_SIMPLE", "name": "老婆簡易策略", "direction": "BUY",
        "strat_type": "custom",
        "description": "簡化版進場：AI信心度>=70% + 風控正常 + KD未超買，適合不看盤操作",
        "conditions": [
            "AI推薦信心度 >= 70%",
            "風控等級 = NORMAL（無警報）",
            "KD K值 < 80（非超買區）",
            "個股評分 >= 62（BUY方向）",
        ],
        "params": [
            {"key": "wife_confidence", "label": "AI信心門檻%", "default": 70, "min": 50, "max": 90},
            {"key": "wife_score_min", "label": "最低評分", "default": 62, "min": 50, "max": 80},
        ],
        "timeframe": "日K + AI",
        "enabled": True,
    },
    {
        "id": "WIFE_EXIT", "name": "老婆簡易出場", "direction": "SELL",
        "strat_type": "custom",
        "description": "簡化版出場：虧損>=3%停損 或 獲利>=5%且KD>80止盈，不需盯盤",
        "conditions": [
            "停損：帳面虧損 >= 3%",
            "止盈：帳面獲利 >= 5% 且 KD K值 > 80",
            "風控：風險等級升至ALERT → 全部出場",
        ],
        "params": [
            {"key": "wife_stoploss", "label": "停損閾值%", "default": 3, "min": 2, "max": 8},
            {"key": "wife_takeprofit", "label": "止盈閾值%", "default": 5, "min": 3, "max": 15},
            {"key": "wife_kd_exit", "label": "KD止盈閾值", "default": 80, "min": 70, "max": 90},
        ],
        "timeframe": "日K",
        "enabled": True,
    },
]

def _load_strategy_config():
    """從 DB 載入策略啟用狀態和自訂參數，合併回 STRATEGIES"""
    con = db()
    cur = con.cursor()
    cur.execute("SELECT strategy_id, enabled, params FROM strategy_config")
    rows = cur.fetchall()
    con.close()
    saved = {r[0]: {"enabled": bool(r[1]), "params": json.loads(r[2]) if r[2] else {}} for r in rows}
    for s in STRATEGIES:
        if s["id"] in saved:
            if not s.get("force_enabled"):
                s["enabled"] = saved[s["id"]]["enabled"]
            # 合併自訂參數值
            custom = saved[s["id"]].get("params", {})
            if custom and s.get("params"):
                for p in s["params"]:
                    if p["key"] in custom:
                        p["value"] = custom[p["key"]]

def _save_strategy_config(sid: str, enabled: bool, params_dict: dict = None):
    """儲存單一策略的啟用狀態和參數到 DB"""
    con = db()
    cur = con.cursor()
    cur.execute("""
        INSERT INTO strategy_config(strategy_id, enabled, params, updated_at)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(strategy_id) DO UPDATE SET enabled=?, params=?, updated_at=?
    """, (sid, int(enabled), json.dumps(params_dict or {}), datetime.now().isoformat(),
          int(enabled), json.dumps(params_dict or {}), datetime.now().isoformat()))
    con.commit()
    con.close()

_load_strategy_config()

@app.get("/api/strategies")
def get_strategies():
    return STRATEGIES

@app.put("/api/strategies/{sid}/toggle")
def toggle_strategy(sid: str, _: None = Depends(require_token)):
    with _strategies_lock:
        for s in STRATEGIES:
            if s["id"] == sid:
                if s.get("force_enabled"):
                    return JSONResponse({"ok": False, "message": f"{sid} 為保命機制，不可關閉"}, status_code=400)
                s["enabled"] = not s["enabled"]
                params_dict = {}
                if s.get("params"):
                    for p in s["params"]:
                        params_dict[p["key"]] = p.get("value", p["default"])
                _save_strategy_config(sid, s["enabled"], params_dict)
                return {"ok": True, "id": sid, "enabled": s["enabled"]}
    return JSONResponse({"ok": False, "message": "策略不存在"}, status_code=404)

@app.put("/api/strategies/{sid}/params")
def update_strategy_params(sid: str, data: dict, _: None = Depends(require_token)):
    """更新策略參數並持久化"""
    with _strategies_lock:
        for s in STRATEGIES:
            if s["id"] == sid:
                if not s.get("params"):
                    return JSONResponse({"ok": False, "message": "此策略無可調參數"}, status_code=400)
                updated = {}
                for p in s["params"]:
                    if p["key"] in data:
                        val = data[p["key"]]
                        if "min" in p and val < p["min"]:
                            val = p["min"]
                        if "max" in p and val > p["max"]:
                            val = p["max"]
                        p["value"] = val
                    updated[p["key"]] = p.get("value", p["default"])
                _save_strategy_config(sid, s["enabled"], updated)
                return {"ok": True, "id": sid, "params": updated}
    return JSONResponse({"ok": False, "message": "策略不存在"}, status_code=404)


# ── 歷史行情管理（market.db auto-fetch）────────────

_INDEX_BG_FETCH: set = set()

def _bg_fetch_index(code: str, start_date: str, end_date: str):
    """背景下載指數資料（不阻塞請求）"""
    try:
        import yfinance as yf
        df = yf.download(code, start=start_date, end=end_date, progress=False)
        if df is None or df.empty:
            return
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        con = market_db()
        count = 0
        for idx, row in df.iterrows():
            d = idx.strftime("%Y-%m-%d") if hasattr(idx, 'strftime') else str(idx)[:10]
            try:
                c = float(row.get("Close", 0) or 0)
                if c == 0 or c != c: continue
                con.execute("INSERT OR IGNORE INTO daily_kbar(code,market,date,open,high,low,close,volume) VALUES(?,?,?,?,?,?,?,?)",
                    (code, "INDEX", d, round(float(row.get("Open",0) or 0),2), round(float(row.get("High",0) or 0),2),
                     round(float(row.get("Low",0) or 0),2), round(c,2), int(row.get("Volume",0) or 0)))
                count += 1
            except: pass
        con.commit(); con.close()
        print(f"[market.db] INDEX/{code}: 背景新增 {count} 筆 ({start_date}~{end_date})")
    except Exception as e:
        print(f"[market.db] 背景抓取失敗 {code}: {e}")
    finally:
        _INDEX_BG_FETCH.discard(code)

def _ensure_index_data(code: str, start_date: str, end_date: str) -> int:
    """查詢 market.db 是否有指數資料，無則啟動背景下載"""
    con = market_db(); cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM daily_kbar WHERE code=? AND date BETWEEN ? AND ?",
                (code, start_date, end_date))
    cnt = cur.fetchone()[0]; con.close()
    if cnt == 0 and code not in _INDEX_BG_FETCH:
        _INDEX_BG_FETCH.add(code)
        import threading
        threading.Thread(target=_bg_fetch_index, args=(code, start_date, end_date), daemon=True).start()
    return cnt

_daily_topup_tried: dict = {}   # {(code, market, today): True} 進程內節流：同日同碼只增量補抓一次（避免每次 _load_backtest_data 重打網路）

def _last_expected_trading_day(end_date: str) -> str:
    """end_date 起回退到最近一個工作日（週末跳過）。粗略：不含台股國定假日表 →
    假日當天最多多嘗試一次（由 _daily_topup_tried 節流，抓回空 INSERT OR IGNORE 無害），不影響正確性。"""
    d = datetime.strptime(end_date, "%Y-%m-%d")
    while d.weekday() >= 5:          # 5=Sat 6=Sun
        d -= timedelta(days=1)
    return d.strftime("%Y-%m-%d")

def _ensure_daily_data(code: str, market: str, start_date: str, end_date: str) -> int:
    """確保 market.db 有指定區間的日K資料；缺失則抓全段，落後最近交易日則增量補尾。
    純加法（INSERT OR IGNORE，不覆寫既有；PIT 安全：只補 ≤ end_date 的歷史/當日 bar）。
    修前 bug：『existing_count>0 即 return』＝抓一次後永不增量 → daily_kbar 凍結在各碼首載日，
    forward/回測讀不到新日線 → 部位無法進場/mark。"""
    con = market_db()
    cur = con.cursor()
    cur.execute("""
        SELECT MIN(date), MAX(date), COUNT(*) FROM daily_kbar
        WHERE code=? AND market=? AND date BETWEEN ? AND ?
    """, (code, market, start_date, end_date))
    row = cur.fetchone()
    existing_count = row[2] if row else 0
    max_date = row[1] if row else None

    # 抓取區間決策：無資料→抓全段；有資料但落後最近交易日→只抓缺的尾段
    fetch_start = start_date
    if existing_count > 0:
        if max_date and max_date >= _last_expected_trading_day(end_date):
            con.close()
            return existing_count                       # 已到最近交易日 → 不抓
        today = datetime.now().strftime("%Y-%m-%d")
        if _daily_topup_tried.get((code, market, today)):
            con.close()
            return existing_count                       # 今日已嘗試 → 進程內節流，免每次重打網路
        _daily_topup_tried[(code, market, today)] = True
        if max_date:
            fetch_start = (datetime.strptime(max_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    # 自動抓取（增量或全段）
    if market == "US":
        bars = _fetch_us_daily(code, fetch_start, end_date)
    else:
        bars = _fetch_tw_daily(code, fetch_start, end_date)

    added = 0
    for b in bars:
        cur.execute("""
            INSERT OR IGNORE INTO daily_kbar(code, market, date, open, high, low, close, volume)
            VALUES(?,?,?,?,?,?,?,?)
        """, (code, market, b["date"], b["open"], b["high"], b["low"], b["close"], b["volume"]))
        added += cur.rowcount
    con.commit()
    con.close()
    if added:
        print(f"[market.db] {market}/{code}: {'增量補' if existing_count > 0 else '新增'} {added} 筆日K ({fetch_start}~{end_date})")
    return existing_count + added

def _fetch_tw_daily(code: str, start_date: str, end_date: str) -> list:
    """用 Shioaji kbars 或 yfinance 抓台股日K"""
    try:
        import yfinance as yf
        symbol = f"{code}.TW"
        df = yf.download(symbol, start=start_date, end=end_date, progress=False)
        if df.empty:
            symbol = f"{code}.TWO"
            df = yf.download(symbol, start=start_date, end=end_date, progress=False)
        if df.empty:
            return []
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        bars = []
        for idx, row in df.iterrows():
            d = idx.strftime("%Y-%m-%d") if hasattr(idx, 'strftime') else str(idx)[:10]
            bars.append({
                "date": d,
                "open": round(float(row["Open"]), 2),
                "high": round(float(row["High"]), 2),
                "low": round(float(row["Low"]), 2),
                "close": round(float(row["Close"]), 2),
                "volume": int(row["Volume"]),
            })
        return bars
    except Exception as e:
        print(f"[market.db] 台股日K抓取失敗 {code}: {e}")
        return []

def _fetch_us_daily(code: str, start_date: str, end_date: str) -> list:
    """用 yfinance 抓美股日K"""
    try:
        import yfinance as yf
        df = yf.download(code, start=start_date, end=end_date, progress=False)
        if df.empty:
            return []
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        bars = []
        for idx, row in df.iterrows():
            d = idx.strftime("%Y-%m-%d") if hasattr(idx, 'strftime') else str(idx)[:10]
            bars.append({
                "date": d,
                "open": round(float(row["Open"]), 2),
                "high": round(float(row["High"]), 2),
                "low": round(float(row["Low"]), 2),
                "close": round(float(row["Close"]), 2),
                "volume": int(row["Volume"]),
            })
        return bars
    except Exception as e:
        print(f"[market.db] 美股日K抓取失敗 {code}: {e}")
        return []

@app.get("/api/market-data/{code}")
def get_market_data(code: str, market: str = "TW", start: str = "", end: str = ""):
    """取得/自動抓取歷史日K"""
    if not start:
        start = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
    if not end:
        end = datetime.now().strftime("%Y-%m-%d")
    count = _ensure_daily_data(code, market, start, end)
    con = market_db()
    cur = con.cursor()
    cur.execute("""
        SELECT date, open, high, low, close, volume FROM daily_kbar
        WHERE code=? AND market=? AND date BETWEEN ? AND ?
        ORDER BY date
    """, (code, market, start, end))
    rows = cur.fetchall()
    con.close()
    cols = ["date", "open", "high", "low", "close", "volume"]
    return {"code": code, "market": market, "count": len(rows),
            "bars": [dict(zip(cols, r)) for r in rows]}

@app.get("/api/market-data/status")
def market_data_status():
    """market.db 統計"""
    con = market_db()
    cur = con.cursor()
    cur.execute("SELECT market, COUNT(DISTINCT code), COUNT(*), MIN(date), MAX(date) FROM daily_kbar GROUP BY market")
    rows = cur.fetchall()
    con.close()
    return [{"market": r[0], "stocks": r[1], "bars": r[2], "from": r[3], "to": r[4]} for r in rows]


@app.post("/api/market-data/refresh-daily")
def market_data_refresh_daily(market: str = "TW", _: None = Depends(require_token)):
    """每交易日增量補 daily_kbar：對 forward/回測同源 universe 逐碼 top-up 至最近交易日。
    純加法（_ensure_daily_data INSERT OR IGNORE）。供 forward daily cron STEP 0 與 cockpit 手動觸發。
    根因：daily_kbar 無每日增量路徑（rec-scheduler 只餵 kbar_cache，forward 讀的是 daily_kbar）。"""
    market = (market or "TW").upper()
    codes = _ft_universe_codes(market)
    if not codes:
        return JSONResponse({"ok": False, "error": f"universe 空: {market}"}, status_code=400)
    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")
    con = market_db(); cur = con.cursor()
    cur.execute("SELECT MAX(date) FROM daily_kbar WHERE market=?", (market,))
    before = cur.fetchone()[0]; con.close()
    failed = 0
    for code in codes:
        try:
            _ensure_daily_data(code, market, start, end)
        except Exception:
            failed += 1
    con = market_db(); cur = con.cursor()
    cur.execute("SELECT MAX(date) FROM daily_kbar WHERE market=?", (market,))
    after = cur.fetchone()[0]; con.close()
    return {"ok": True, "market": market, "universe": len(codes), "failed": failed,
            "latest_before": before, "latest_date": after}


# ── 回測引擎 ──────────────────────────────────────

# 台股成本模型
TW_COMMISSION = 0.001425
TW_TAX = 0.003
TW_DISCOUNT = 0.6
US_SLIPPAGE_PCT = 0.0005

def _load_backtest_data(codes: list, mkt: str, start: str, end: str) -> tuple:
    """預載回測資料（download + DB load），回傳 (all_dates, bar_data)。可供多策略共用。"""
    for code in codes:
        _ensure_daily_data(code, mkt, start, end)

    con = market_db()
    cur = con.cursor()
    placeholders = ",".join("?" * len(codes))
    cur.execute(f"""
        SELECT DISTINCT date FROM daily_kbar
        WHERE code IN ({placeholders}) AND market=? AND date BETWEEN ? AND ?
        ORDER BY date
    """, codes + [mkt, start, end])
    all_dates = [r[0] for r in cur.fetchall()]

    bar_data = {}
    for code in codes:
        cur.execute("""
            SELECT date, open, high, low, close, volume FROM daily_kbar
            WHERE code=? AND market=? AND date BETWEEN ? AND ?
            ORDER BY date
        """, (code, mkt, start, end))
        for r in cur.fetchall():
            if r[4] is None or (isinstance(r[4], float) and r[4] != r[4]):
                continue
            bar_data[(code, r[0])] = {"open": r[1] or 0, "high": r[2] or 0, "low": r[3] or 0, "close": r[4], "volume": r[5] or 0}
    con.close()
    return all_dates, bar_data


def _run_backtest(config: dict, _preloaded: tuple = None) -> dict:
    """
    核心回測引擎。
    config: {codes, market, start, end, strategies, initial_capital, commission_discount, trade_type}
    _preloaded: optional (all_dates, bar_data) tuple to skip data loading (compute-once optimization)
    """
    codes = config.get("codes", [])
    mkt = config.get("market", "TW")
    start = config.get("start", "2021-01-01")
    end = config.get("end", datetime.now().strftime("%Y-%m-%d"))
    _all_enabled = [s["id"] for s in STRATEGIES if s["enabled"]]
    if config.get("buy_strategies") or config.get("sell_strategies"):
        selected = set(config.get("buy_strategies", []) + config.get("sell_strategies", []))
    else:
        selected = set(config.get("strategies", _all_enabled))
    capital = config.get("initial_capital", config.get("capital", 1000000))
    discount = config.get("commission_discount", TW_DISCOUNT)
    trade_type = config.get("trade_type", "波段")

    all_trades = []
    equity_points = []
    cash = capital
    holdings = {}  # code -> {shares, cost, highest, entry_date}

    if _preloaded:
        all_dates, bar_data = _preloaded
    else:
        all_dates, bar_data = _load_backtest_data(codes, mkt, start, end)

    # 回測參數 — 從 STRATEGIES 讀取（迭代系統透過 _apply_params_to_config 修改 default 值）
    exit_d_pct = _get_strategy_param("EXIT_D", "exit_d_threshold", 5) / 100
    swing_profit = _get_strategy_param("EXIT_C", "swing_profit", 8) / 100
    swing_dd = _get_strategy_param("EXIT_C", "swing_drawdown", 2) / 100
    day_profit = _get_strategy_param("EXIT_C", "day_profit", 3) / 100
    day_dd = _get_strategy_param("EXIT_C", "day_drawdown", 1) / 100

    for di, today in enumerate(all_dates):
        for code in codes:
            bar = bar_data.get((code, today))
            if not bar:
                continue
            price = bar["close"]
            if price is None or price != price:
                continue

            # 收集歷史收盤
            history = []
            for d in all_dates[max(0, di-250):di+1]:
                b = bar_data.get((code, d))
                if b:
                    history.append(b["close"])

            if len(history) < 30:
                continue

            s = pd.Series(history)
            ma5 = s.rolling(5).mean().iloc[-1]
            ma10 = s.rolling(10).mean().iloc[-1]
            ma20 = s.rolling(20).mean().iloc[-1]
            ma240 = s.rolling(240).mean().iloc[-1] if len(s) >= 240 else None

            prev5 = s.rolling(5).mean().iloc[-2] if len(s) > 5 else ma5
            prev10 = s.rolling(10).mean().iloc[-2] if len(s) > 10 else ma10

            dif_s, macd_s, _ = calc_macd(history)
            dif_cur, macd_cur = dif_s[-1], macd_s[-1]
            dif_prev = dif_s[-2] if len(dif_s) > 1 else dif_cur
            macd_prev = macd_s[-2] if len(macd_s) > 1 else macd_cur

            vols = []
            for d in all_dates[max(0, di-20):di+1]:
                b = bar_data.get((code, d))
                if b:
                    vols.append(b["volume"])
            vol_ratio = vols[-1] / (sum(vols[:-1]) / max(len(vols)-1, 1)) if len(vols) > 1 and sum(vols[:-1]) > 0 else 1.0

            # ── 持倉出場檢查 ──
            if code in holdings:
                h = holdings[code]
                h["highest"] = max(h["highest"] or price, bar["high"] or price)
                cost = h["cost"]
                pnl_pct = (price - cost) / cost

                sold = False
                sell_signal = ""

                # EXIT_D
                if "EXIT_D" in selected and pnl_pct <= -exit_d_pct:
                    sell_signal = "EXIT_D"
                    sold = True
                # EXIT_C
                elif "EXIT_C" in selected:
                    pt = swing_profit if trade_type == "波段" else day_profit
                    dt = swing_dd if trade_type == "波段" else day_dd
                    max_p = (h["highest"] - cost) / cost
                    if max_p >= pt:
                        dd = (h["highest"] - price) / h["highest"]
                        if dd >= dt:
                            sell_signal = "EXIT_C"
                            sold = True
                # EXIT_B (MACD死叉 + 量縮)
                if not sold and "EXIT_B" in selected:
                    if dif_prev > macd_prev and dif_cur <= macd_cur and vol_ratio < 0.8:
                        sell_signal = "EXIT_B"
                        sold = True
                # EXIT_A (簡化：跌破MA20)
                if not sold and "EXIT_A" in selected:
                    if price < ma20 and history[-2] >= s.rolling(20).mean().iloc[-2]:
                        sell_signal = "EXIT_A"
                        sold = True
                # NEWS_BEARISH (簡化：漲幅<-1% + 量比>1.5)
                if not sold and "NEWS_BEARISH" in selected:
                    day_chg = (price - history[-2]) / history[-2] if len(history) >= 2 else 0
                    if day_chg < -0.01 and vol_ratio > 1.5:
                        sell_signal = "NEWS_BEARISH"
                        sold = True
                # EXIT_TRAIL (移動停利：從最高回落X%)
                if not sold and "EXIT_TRAIL" in selected:
                    _trail_pct = _get_strategy_param("EXIT_TRAIL", "trail_pct", 5) / 100
                    _trail_dd = (h["highest"] - price) / h["highest"] if h["highest"] > 0 else 0
                    if _trail_dd >= _trail_pct:
                        sell_signal = "EXIT_TRAIL"
                        sold = True
                # EXIT_NDAY (持倉滿N天出場)
                if not sold and "EXIT_NDAY" in selected:
                    _hold_days = int(_get_strategy_param("EXIT_NDAY", "hold_days", 20))
                    _entry_idx = all_dates.index(h["entry_date"]) if h["entry_date"] in all_dates else 0
                    if di - _entry_idx >= _hold_days:
                        sell_signal = "EXIT_NDAY"
                        sold = True
                # EXIT_TARGET (固定停利：報酬達X%)
                if not sold and "EXIT_TARGET" in selected:
                    _target_pct = _get_strategy_param("EXIT_TARGET", "target_pct", 10) / 100
                    if pnl_pct >= _target_pct:
                        sell_signal = "EXIT_TARGET"
                        sold = True

                if sold:
                    sell_price = price
                    sell_value = sell_price * h["shares"]
                    if mkt == "TW":
                        commission = sell_value * TW_COMMISSION * discount
                        tax = sell_value * TW_TAX
                    else:
                        commission = 0
                        tax = 0
                    proceeds = sell_value - commission - tax
                    profit = proceeds - (cost * h["shares"])
                    cash += proceeds
                    all_trades.append({
                        "code": code, "signal": sell_signal, "action": "SELL",
                        "date": today, "price": sell_price,
                        "shares": h["shares"], "profit": round(profit, 0),
                        "profit_pct": round(pnl_pct * 100, 2),
                        "entry_date": h["entry_date"], "entry_price": cost,
                    })
                    del holdings[code]
                continue

            # ── 買進訊號（無持倉時）──
            if code in holdings:
                continue

            buy_signal = ""

            # BUY_A (MACD金叉 + 站上MA20 + 量比)
            if "BUY_A" in selected:
                buy_a_vol = _get_strategy_param("BUY_A", "vol_ratio_min", 1.5)
                if dif_prev < macd_prev and dif_cur >= macd_cur and price > ma20 and vol_ratio >= buy_a_vol:
                    buy_signal = "BUY_A"
            # BUY_B (MA5上穿MA10 + 量比)
            if not buy_signal and "BUY_B" in selected:
                buy_b_vol = _get_strategy_param("BUY_B", "vol_ratio_min", 1.2)
                if prev5 < prev10 and ma5 >= ma10 and vol_ratio >= buy_b_vol:
                    buy_signal = "BUY_B"
            # LOW_BUY
            if not buy_signal and "LOW_BUY" in selected:
                if ma240 and price < ma240 * 0.85:
                    buy_signal = "LOW_BUY"
            # SQUEEZE_BREAK
            if not buy_signal and "SQUEEZE_BREAK" in selected:
                highs = [bar_data.get((code, d), {}).get("high", 0) for d in all_dates[max(0, di-20):di]]
                if highs and price > max(highs) and vol_ratio >= 2.0:
                    buy_signal = "SQUEEZE_BREAK"
            # KD_CROSS — 用累積 EWM 計算 K/D，黃金交叉且 K < 50
            if not buy_signal and "KD_CROSS" in selected and len(history) >= 14:
                _k_val, _d_val = 50.0, 50.0
                _pk_val, _pd_val = 50.0, 50.0
                for j in range(max(0, len(history)-30), len(history)):
                    _sl = history[max(0,j-8):j+1]
                    _kd_low = min(_sl) if _sl else history[j]
                    _kd_high = max(_sl) if _sl else history[j]
                    _rsv = (history[j] - _kd_low) / (_kd_high - _kd_low) * 100 if _kd_high != _kd_low else 50
                    _pk_val, _pd_val = _k_val, _d_val
                    _k_val = _rsv * (1/3) + _k_val * (2/3)
                    _d_val = _k_val * (1/3) + _d_val * (2/3)
                if _pk_val < _pd_val and _k_val >= _d_val and _k_val < 50:
                    buy_signal = "KD_CROSS"
            # MACD_CROSS
            if not buy_signal and "MACD_CROSS" in selected:
                if dif_prev < macd_prev and dif_cur >= macd_cur:
                    buy_signal = "MACD_CROSS"
            # RSI_EXTREME
            if not buy_signal and "RSI_EXTREME" in selected and len(history) >= 14:
                _gains = [max(0, history[i] - history[i-1]) for i in range(1, len(history))]
                _losses = [max(0, history[i-1] - history[i]) for i in range(1, len(history))]
                _ag = sum(_gains[-14:]) / 14
                _al = sum(_losses[-14:]) / 14
                _rsi = 100 - 100 / (1 + _ag / _al) if _al > 0 else 100
                _ag_p = sum(_gains[-15:-1]) / 14 if len(_gains) >= 15 else _ag
                _al_p = sum(_losses[-15:-1]) / 14 if len(_losses) >= 15 else _al
                _rsi_p = 100 - 100 / (1 + _ag_p / _al_p) if _al_p > 0 else 100
                if _rsi_p < 30 and _rsi >= 30:
                    buy_signal = "RSI_EXTREME"
            # MA_ALIGN
            if not buy_signal and "MA_ALIGN" in selected and len(history) >= 60:
                ma60 = s.rolling(60).mean().iloc[-1]
                if price > ma5 > ma10 > ma20 > ma60:
                    buy_signal = "MA_ALIGN"
            # DONCHIAN_BREAK
            if not buy_signal and "DONCHIAN_BREAK" in selected and len(history) >= 5:
                _dc_n = int(_get_strategy_param("DONCHIAN_BREAK", "donchian_period", 20))
                _dc_highs = [bar_data.get((code, d), {}).get("high", 0) for d in all_dates[max(0, di-_dc_n):di]]
                if _dc_highs and price > max(_dc_highs):
                    buy_signal = "DONCHIAN_BREAK"
            # MA_PULLBACK
            if not buy_signal and "MA_PULLBACK" in selected and len(history) >= 60:
                _pb_ma_s = int(_get_strategy_param("MA_PULLBACK", "pullback_ma_short", 20))
                _pb_ma_l = int(_get_strategy_param("MA_PULLBACK", "pullback_ma_long", 60))
                _pb_ma_short = s.rolling(_pb_ma_s).mean().iloc[-1] if len(s) >= _pb_ma_s else ma20
                _pb_ma_long = s.rolling(_pb_ma_l).mean().iloc[-1] if len(s) >= _pb_ma_l else _pb_ma_short
                if bar["low"] <= _pb_ma_short and price > _pb_ma_long and price > bar["open"]:
                    buy_signal = "MA_PULLBACK"
            # BB_SQUEEZE
            if not buy_signal and "BB_SQUEEZE" in selected and len(history) >= 60:
                _bb_n = int(_get_strategy_param("BB_SQUEEZE", "bb_period", 20))
                _bb_lb = int(_get_strategy_param("BB_SQUEEZE", "bb_squeeze_lookback", 60))
                _bb_ma = s.rolling(_bb_n).mean().iloc[-1] if len(s) >= _bb_n else price
                _bb_std = s.rolling(_bb_n).std().iloc[-1] if len(s) >= _bb_n else 0
                _bb_upper = _bb_ma + 2 * _bb_std
                _bb_width = (4 * _bb_std / _bb_ma) if _bb_ma > 0 else 0
                _bb_widths = []
                for _wi in range(max(0, len(s)-_bb_lb), len(s)):
                    _w_slice = s.iloc[max(0,_wi-_bb_n+1):_wi+1]
                    if len(_w_slice) >= _bb_n:
                        _w_std = _w_slice.std()
                        _w_ma = _w_slice.mean()
                        if _w_ma > 0:
                            _bb_widths.append(4 * _w_std / _w_ma)
                if _bb_widths and _bb_width <= min(_bb_widths[:-1]) if len(_bb_widths) > 1 else True:
                    if price > _bb_upper:
                        buy_signal = "BB_SQUEEZE"
            # VOL_BREAKOUT
            if not buy_signal and "VOL_BREAKOUT" in selected and len(history) >= 2:
                _vb_multi = _get_strategy_param("VOL_BREAKOUT", "vol_multi", 2.0)
                _vb_chg = _get_strategy_param("VOL_BREAKOUT", "price_chg_min", 1.0) / 100
                _day_chg = (price - history[-2]) / history[-2] if history[-2] > 0 else 0
                if vol_ratio >= _vb_multi and _day_chg >= _vb_chg:
                    buy_signal = "VOL_BREAKOUT"

            if buy_signal:
                raw_shares = int(cash * 0.1 / price) if price > 0 else 0
                if mkt == "TW":
                    shares = (raw_shares // 1000) * 1000
                    if shares < 1000:
                        shares = min(raw_shares, 999) if raw_shares >= 1 else 0
                else:
                    shares = raw_shares
                if shares > 0 and cash >= shares * price:
                    cost_total = shares * price
                    commission = cost_total * TW_COMMISSION * discount if mkt == "TW" else 0
                    cash -= (cost_total + commission)
                    holdings[code] = {
                        "shares": shares, "cost": price,
                        "highest": bar["high"] or price, "entry_date": today,
                    }
                    all_trades.append({
                        "code": code, "signal": buy_signal, "action": "BUY",
                        "date": today, "price": price,
                        "shares": shares, "profit": 0, "profit_pct": 0,
                    })

        # 每日權益計算
        holdings_value = 0
        for code, h in holdings.items():
            bar = bar_data.get((code, today))
            if bar:
                holdings_value += bar["close"] * h["shares"]
        equity = cash + holdings_value
        equity_points.append({"date": today, "equity": round(equity, 0)})

    # 強制平倉剩餘持倉
    if holdings and all_dates:
        last_date = all_dates[-1]
        for code, h in list(holdings.items()):
            bar = bar_data.get((code, last_date))
            if bar:
                price = bar["close"]
                pnl_pct = (price - h["cost"]) / h["cost"]
                proceeds = price * h["shares"]
                profit = proceeds - h["cost"] * h["shares"]
                cash += proceeds
                all_trades.append({
                    "code": code, "signal": "FORCE_CLOSE", "action": "SELL",
                    "date": last_date, "price": price,
                    "shares": h["shares"], "profit": round(profit, 0),
                    "profit_pct": round(pnl_pct * 100, 2),
                    "entry_date": h["entry_date"], "entry_price": h["cost"],
                })
        holdings.clear()

    # 績效計算
    total_trades = len([t for t in all_trades if t["action"] == "SELL"])
    win_trades = len([t for t in all_trades if t["action"] == "SELL" and t["profit"] > 0])
    lose_trades = total_trades - win_trades
    wins = [t["profit"] for t in all_trades if t["action"] == "SELL" and t["profit"] > 0]
    losses = [abs(t["profit"]) for t in all_trades if t["action"] == "SELL" and t["profit"] <= 0]
    avg_win = sum(wins) / len(wins) if wins else 0
    avg_loss = sum(losses) / len(losses) if losses else 1

    final_equity = equity_points[-1]["equity"] if equity_points else capital
    total_return = (final_equity - capital) / capital * 100

    # 年化報酬
    days = (datetime.strptime(end, "%Y-%m-%d") - datetime.strptime(start, "%Y-%m-%d")).days
    years = max(days / 365.25, 0.01)
    cagr = ((final_equity / capital) ** (1 / years) - 1) * 100

    # 最大回撤
    max_dd = 0
    peak = capital
    for ep in equity_points:
        if ep["equity"] > peak:
            peak = ep["equity"]
        dd = (peak - ep["equity"]) / peak * 100
        if dd > max_dd:
            max_dd = dd

    # 夏普比率（簡化）
    if len(equity_points) > 1:
        returns = []
        for i in range(1, len(equity_points)):
            r = (equity_points[i]["equity"] - equity_points[i-1]["equity"]) / equity_points[i-1]["equity"]
            returns.append(r)
        import numpy as np
        ret_arr = np.array(returns)
        sharpe = (ret_arr.mean() / ret_arr.std() * (252 ** 0.5)) if ret_arr.std() > 0 else 0
    else:
        sharpe = 0

    # 月度報酬
    monthly = {}
    for ep in equity_points:
        ym = ep["date"][:7]
        monthly[ym] = ep["equity"]
    monthly_returns = {}
    prev_eq = capital
    for ym in sorted(monthly.keys()):
        mr = (monthly[ym] - prev_eq) / prev_eq * 100
        monthly_returns[ym] = round(mr, 2)
        prev_eq = monthly[ym]

    # 每筆交易平均報酬%（扣成本，與 avg_return_ci95 同單位）
    sell_pnls = [t.get("profit_pct", 0) for t in all_trades if t.get("action") == "SELL"]
    avg_trade_return = sum(sell_pnls) / len(sell_pnls) if sell_pnls else 0

    summary = {
        "initial_capital": capital,
        "final_equity": round(final_equity, 0),
        "total_return_pct": round(total_return, 2),
        "return_pct": round(total_return, 2),
        "cagr_pct": round(cagr, 2),
        "total_trades": total_trades,
        "total_pnl": round(final_equity - capital, 0),
        "win_trades": win_trades,
        "lose_trades": lose_trades,
        "win_rate_pct": round(win_trades / total_trades * 100, 1) if total_trades else 0,
        "avg_trade_return_pct": round(avg_trade_return, 2),
        "profit_loss_ratio": round(avg_win / avg_loss, 2) if avg_loss > 0 else 0,
        "max_drawdown_pct": round(max_dd, 2),
        "sharpe_ratio": round(sharpe, 2),
        "monthly_returns": monthly_returns,
        "codes": config.get("codes", []),
        "market": config.get("market", "TW"),
    }

    # Bootstrap 信賴區間（95% CI）
    if total_trades >= 5:
        import random
        trade_results = [1 if t.get("profit", 0) > 0 else 0 for t in all_trades if t.get("action") == "SELL"]
        trade_pnls = [t.get("profit_pct", 0) for t in all_trades if t.get("action") == "SELL"]
        n_boot = 1000
        boot_wr, boot_ret = [], []
        for _ in range(n_boot):
            sample_idx = [random.randint(0, len(trade_results)-1) for __ in range(len(trade_results))]
            if trade_results:
                boot_wr.append(sum(trade_results[i] for i in sample_idx) / len(sample_idx) * 100)
            if trade_pnls:
                boot_ret.append(sum(trade_pnls[i] for i in sample_idx) / len(sample_idx))
        boot_wr.sort(); boot_ret.sort()
        ci_lo, ci_hi = int(n_boot * 0.025), int(n_boot * 0.975)
        summary["win_rate_ci95"] = [round(boot_wr[ci_lo], 1), round(boot_wr[ci_hi], 1)] if boot_wr else None
        summary["avg_return_ci95"] = [round(boot_ret[ci_lo], 2), round(boot_ret[ci_hi], 2)] if boot_ret else None
        summary["ci_n_trades"] = len(trade_results)

    return {"summary": summary, "trades": all_trades, "equity_curve": equity_points}


@app.post("/api/backtest/run")
def run_backtest(config: dict):
    """執行回測"""
    capital = config.get("initial_capital", config.get("capital", 1000000))
    if not isinstance(capital, (int, float)) or capital <= 0:
        return JSONResponse({"error": "capital 必須大於 0"}, status_code=400)
    config["initial_capital"] = capital
    try:
        result = _run_backtest(config)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e), "traceback": traceback.format_exc()}, status_code=500)

    # 存入 market.db
    con = market_db()
    con.execute("""
        INSERT INTO backtest_result(name, config, summary, trades, equity_curve, created_at)
        VALUES(?,?,?,?,?,?)
    """, (
        config.get("name", f"回測 {datetime.now().strftime('%m/%d %H:%M')}"),
        json.dumps(config, ensure_ascii=False),
        json.dumps(result["summary"], ensure_ascii=False),
        json.dumps(result["trades"], ensure_ascii=False),
        json.dumps(result["equity_curve"], ensure_ascii=False),
        datetime.now().isoformat(),
    ))
    con.commit()
    con.close()
    return result

@app.get("/api/backtest/history")
def backtest_history():
    """回測歷史紀錄"""
    con = market_db()
    cur = con.cursor()
    cur.execute("SELECT id, name, summary, created_at FROM backtest_result WHERE summary IS NOT NULL ORDER BY id DESC LIMIT 20")
    rows = cur.fetchall()
    con.close()
    results = []
    for r in rows:
        s = json.loads(r[2]) if r[2] else {}
        results.append({
            "id": r[0], "name": r[1] or "",
            "codes": s.get("codes", []),
            "market": s.get("market", ""),
            "trades": s.get("total_trades", 0),
            "return": s.get("total_return_pct", 0),
            "sharpe": s.get("sharpe_ratio", 0),
            "summary": s,
            "created_at": r[3] or "",
        })
    return results

@app.get("/api/backtest/{bt_id}")
def get_backtest(bt_id: int):
    """取得單筆回測結果"""
    con = market_db()
    cur = con.cursor()
    cur.execute("SELECT name, config, summary, trades, equity_curve, created_at FROM backtest_result WHERE id=?", (bt_id,))
    r = cur.fetchone()
    con.close()
    if not r:
        return JSONResponse({"error": "not found"}, status_code=404)
    return {
        "name": r[0],
        "config": json.loads(r[1]) if r[1] else {},
        "summary": json.loads(r[2]) if r[2] else {},
        "trades": json.loads(r[3]) if r[3] else [],
        "equity_curve": json.loads(r[4]) if r[4] else [],
        "created_at": r[5],
    }


# ── P8: Walk-Forward 回測 ──────────────────────────

@app.post("/api/backtest/walk-forward")
def walk_forward_backtest(config: dict):
    """
    Walk-forward 回測：將期間切成多個 train/test 窗口，
    每個窗口用 train 段優化後在 test 段驗證，防止過擬合。
    """
    from dateutil.relativedelta import relativedelta
    codes = config.get("codes", [])
    if not codes:
        sym = config.get("symbols", "")
        if sym:
            codes = [s.strip() for s in sym.split(",") if s.strip()]
    mkt = config.get("market", "TW")
    start_str = config.get("start", "2023-01-01")
    end_str = config.get("end", datetime.now().strftime("%Y-%m-%d"))
    train_months = config.get("train_months", 6)
    test_months = config.get("test_months", 2)
    capital = config.get("initial_capital", config.get("capital", 1000000))

    start_dt = datetime.strptime(start_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_str, "%Y-%m-%d")

    windows = []
    cursor = start_dt
    while cursor + relativedelta(months=train_months + test_months) <= end_dt:
        train_start = cursor.strftime("%Y-%m-%d")
        train_end = (cursor + relativedelta(months=train_months)).strftime("%Y-%m-%d")
        test_start = train_end
        test_end = (cursor + relativedelta(months=train_months + test_months)).strftime("%Y-%m-%d")
        windows.append({"train_start": train_start, "train_end": train_end,
                         "test_start": test_start, "test_end": test_end})
        cursor += relativedelta(months=test_months)

    if not windows:
        return {"error": "期間太短，無法切分窗口", "windows": []}

    results = []
    cumulative_pnl = 0
    for w in windows:
        train_cfg = {**config, "start": w["train_start"], "end": w["train_end"], "initial_capital": capital}
        test_cfg = {**config, "start": w["test_start"], "end": w["test_end"], "initial_capital": capital}
        train_result = _run_backtest(train_cfg)
        test_result = _run_backtest(test_cfg)
        ts = train_result.get("summary", {})
        tt = test_result.get("summary", {})
        cumulative_pnl += tt.get("total_pnl", 0)
        results.append({
            "window": w,
            "train": {"return_pct": ts.get("return_pct", 0), "trades": ts.get("total_trades", 0),
                       "win_rate": ts.get("win_rate", 0), "sharpe": ts.get("sharpe_ratio", 0)},
            "test": {"return_pct": tt.get("return_pct", 0), "trades": tt.get("total_trades", 0),
                      "win_rate": tt.get("win_rate", 0), "sharpe": tt.get("sharpe_ratio", 0),
                      "pnl": tt.get("total_pnl", 0)},
        })

    avg_train_ret = sum(r["train"]["return_pct"] for r in results) / len(results) if results else 0
    avg_test_ret = sum(r["test"]["return_pct"] for r in results) / len(results) if results else 0
    overfit_ratio = avg_train_ret / avg_test_ret if avg_test_ret != 0 else 0

    avg_test_sharpe = sum(r["test"]["sharpe"] for r in results) / len(results) if results else 0
    consistency_pct = sum(1 for r in results if r["test"]["return_pct"] > 0) / len(results) * 100 if results else 0
    summary = {
        "windows": len(results),
        "folds": len(results),
        "avg_train_return": round(avg_train_ret, 2),
        "avg_test_return": round(avg_test_ret, 2),
        "avg_sharpe": round(avg_test_sharpe, 3),
        "overfit_ratio": round(overfit_ratio, 2),
        "cumulative_test_pnl": round(cumulative_pnl, 0),
        "consistency": consistency_pct,
        "verdict": "穩健" if overfit_ratio < 2 and avg_test_ret > 0 else ("過擬合風險" if overfit_ratio >= 3 else "需觀察"),
    }
    return {"summary": summary, "windows": results}


# ══════════════════════════════════════════════════
# Phase 10: 策略迭代實驗室 (Strategy Iteration Lab)
# ══════════════════════════════════════════════════

def _init_iteration_db():
    con = market_db()
    con.execute("""
        CREATE TABLE IF NOT EXISTS iteration_session (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT,
            target_strategies TEXT,
            codes           TEXT,
            market          TEXT DEFAULT 'TW',
            date_range      TEXT,
            layers          TEXT,
            convergence     TEXT,
            status          TEXT DEFAULT 'pending',
            current_round   INTEGER DEFAULT 0,
            best_sharpe     REAL DEFAULT 0,
            best_winrate    REAL DEFAULT 0,
            best_params     TEXT,
            total_runs      INTEGER DEFAULT 0,
            log             TEXT DEFAULT '[]',
            created_at      TEXT,
            completed_at    TEXT
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS iteration_round (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id      INTEGER,
            round_num       INTEGER,
            layer           TEXT,
            params_used     TEXT,
            backtest_summary TEXT,
            walkforward     TEXT,
            analysis        TEXT,
            param_changes   TEXT,
            sharpe          REAL,
            winrate         REAL,
            max_drawdown    REAL,
            improvement     REAL DEFAULT 0,
            created_at      TEXT,
            FOREIGN KEY (session_id) REFERENCES iteration_session(id)
        )
    """)
    con.commit()
    con.close()

_init_iteration_db()

_iteration_running = {}
_iteration_progress = {}

def _iter_log(sid, msg):
    ts = datetime.now().strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    con = market_db()
    cur = con.cursor()
    cur.execute("SELECT log FROM iteration_session WHERE id=?", (sid,))
    r = cur.fetchone()
    logs = json.loads(r[0]) if r and r[0] else []
    logs.append(entry)
    if len(logs) > 500:
        logs = logs[-500:]
    con.execute("UPDATE iteration_session SET log=? WHERE id=?", (json.dumps(logs, ensure_ascii=False), sid))
    con.commit()
    con.close()

def _get_strategy_params_space(strategy_ids: list) -> dict:
    space = {}
    for s in STRATEGIES:
        if s["id"] in strategy_ids and "params" in s:
            for p in s["params"]:
                key = f"{s['id']}.{p['key']}"
                space[key] = {
                    "min": p.get("min", p["default"] * 0.5),
                    "max": p.get("max", p["default"] * 2.0),
                    "default": p["default"],
                    "type": "float" if isinstance(p["default"], float) else "int",
                }
    return space

def _apply_params_to_config(params: dict):
    for full_key, val in params.items():
        parts = full_key.split(".", 1)
        if len(parts) != 2:
            continue
        sid, key = parts
        for s in STRATEGIES:
            if s["id"] == sid and "params" in s:
                for p in s["params"]:
                    if p["key"] == key:
                        p["value"] = val
                        break

def _read_current_params(strategy_ids: list) -> dict:
    params = {}
    for s in STRATEGIES:
        if s["id"] in strategy_ids and "params" in s:
            for p in s["params"]:
                params[f"{s['id']}.{p['key']}"] = p.get("value", p["default"])
    return params

def _run_backtest_with_params(params: dict, base_config: dict) -> dict:
    _apply_params_to_config(params)
    return _run_backtest(base_config)

# ── Layer 1: Grid Search ──────────────────────────

def _grid_search(sid: int, space: dict, base_config: dict, max_combos: int = 200) -> list:
    import itertools, random
    _iter_log(sid, f"Grid Search 開始，參數空間 {len(space)} 維")
    _iteration_progress[sid] = {"round": 0, "layer": "grid", "status": "running", "message": "生成參數網格..."}

    grid_points = {}
    for key, spec in space.items():
        steps = 5
        if spec["type"] == "int":
            vals = sorted(set(int(spec["min"] + (spec["max"] - spec["min"]) * i / (steps - 1)) for i in range(steps)))
        else:
            vals = [round(spec["min"] + (spec["max"] - spec["min"]) * i / (steps - 1), 4) for i in range(steps)]
        grid_points[key] = vals

    keys = list(grid_points.keys())
    total_combos = 1
    for k in keys:
        total_combos *= len(grid_points[k])
    if total_combos <= max_combos:
        all_combos = list(itertools.product(*(grid_points[k] for k in keys)))
    else:
        _iter_log(sid, f"組合數 {total_combos} 超過上限，隨機抽樣 {max_combos}")
        all_combos = []
        for _ in range(max_combos):
            combo = tuple(random.choice(grid_points[k]) for k in keys)
            all_combos.append(combo)

    results = []
    for i, combo in enumerate(all_combos):
        params = dict(zip(keys, combo))
        _iteration_progress[sid]["message"] = f"Grid {i+1}/{len(all_combos)}"
        try:
            bt = _run_backtest_with_params(params, base_config)
            s = bt.get("summary", {})
            results.append({
                "params": params,
                "sharpe": s.get("sharpe_ratio", 0),
                "winrate": s.get("win_rate_pct", 0),
                "max_dd": s.get("max_drawdown_pct", 0),
                "return_pct": s.get("total_return_pct", 0),
                "trades": s.get("total_trades", 0),
                "pnl_ratio": s.get("profit_loss_ratio", 0),
            })
        except Exception as e:
            _iter_log(sid, f"Grid combo {i+1} 失敗: {e}")

    results.sort(key=lambda x: x["sharpe"], reverse=True)
    _iter_log(sid, f"Grid Search 完成，{len(results)} 組，最佳 Sharpe={results[0]['sharpe'] if results else 0:.2f}")
    return results[:20]

# ── Layer 2: Bayesian Optimization ────────────────

def _bayesian_optimize(sid: int, space: dict, base_config: dict,
                       n_trials: int = 50, best_grid_params: dict = None) -> dict:
    try:
        import optuna
        optuna.logging.set_verbosity(optuna.logging.WARNING)
    except ImportError:
        _iter_log(sid, "Optuna 未安裝，跳過 Bayesian")
        return {"best_params": best_grid_params or {}, "trials": [], "skipped": True}

    _iter_log(sid, f"Bayesian Optimization 開始，{n_trials} trials")
    _iteration_progress[sid] = {"round": 0, "layer": "bayesian", "status": "running", "message": "Optuna 搜索中..."}
    trial_history = []

    def objective(trial):
        params = {}
        for key, spec in space.items():
            if spec["type"] == "int":
                params[key] = trial.suggest_int(key, int(spec["min"]), int(spec["max"]))
            else:
                params[key] = round(trial.suggest_float(key, spec["min"], spec["max"]), 4)
        bt = _run_backtest_with_params(params, base_config)
        s = bt.get("summary", {})
        sharpe = s.get("sharpe_ratio", 0)
        winrate = s.get("win_rate_pct", 0)
        max_dd = abs(s.get("max_drawdown_pct", 0))
        score = sharpe * 0.5 + (winrate / 100) * 0.3 - (max_dd / 100) * 0.2
        trial_history.append({"trial": trial.number, "params": params, "sharpe": sharpe,
                              "winrate": winrate, "max_dd": s.get("max_drawdown_pct", 0), "score": round(score, 4)})
        _iteration_progress[sid]["message"] = f"Bayesian {trial.number+1}/{n_trials}"
        return score

    study = optuna.create_study(direction="maximize")
    if best_grid_params:
        study.enqueue_trial(best_grid_params)
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
    _iter_log(sid, f"Bayesian 完成，最佳 score={study.best_value:.4f}")
    return {"best_params": study.best_params, "best_score": study.best_value, "trials": trial_history[-10:]}

# ── Layer 3: AI Strategy Analysis ─────────────────

def _ai_analyze_iteration(sid: int, bt_result: dict, params: dict,
                          round_history: list, strategy_ids: list) -> dict:
    _iter_log(sid, "AI 策略分析中...")
    _iteration_progress[sid] = {"round": 0, "layer": "ai", "status": "running", "message": "Claude 分析中..."}

    summary = bt_result.get("summary", {})
    trades = bt_result.get("trades", [])
    losing = [t for t in trades if t.get("pnl_pct", 0) < 0]
    winning = [t for t in trades if t.get("pnl_pct", 0) > 0]
    strat_info = {s["id"]: {"name": s["name"], "desc": s.get("description", "")}
                  for s in STRATEGIES if s["id"] in strategy_ids}

    prev_text = ""
    if round_history:
        prev_text = "\n前幾輪:\n" + "\n".join(
            f"- R{rh['round_num']}: Sharpe={rh.get('sharpe',0):.2f}, 勝率={rh.get('winrate',0):.1f}%, {rh.get('layer','')}"
            for rh in round_history[-3:]
        )

    prompt = f"""你是量化策略研究員。分析回測結果並提出改進。

## 績效
Sharpe: {summary.get('sharpe_ratio',0):.2f}, 勝率: {summary.get('win_rate_pct',0):.1f}%, 盈虧比: {summary.get('profit_loss_ratio',0):.2f}
最大回撤: {summary.get('max_drawdown_pct',0):.2f}%, 報酬: {summary.get('total_return_pct',0):.2f}%, 交易數: {summary.get('total_trades',0)}

## 策略與參數
{json.dumps(strat_info, ensure_ascii=False)}
參數: {json.dumps(params, ensure_ascii=False)}

## 虧損分析
虧損{len(losing)}筆 avg={sum(t.get('pnl_pct',0) for t in losing)/max(len(losing),1):.2f}%
獲利{len(winning)}筆 avg={sum(t.get('pnl_pct',0) for t in winning)/max(len(winning),1):.2f}%
最大虧損5筆: {json.dumps(sorted(losing, key=lambda x: x.get('pnl_pct',0))[:5], ensure_ascii=False) if losing else '無'}
{prev_text}

回覆嚴格 JSON（不要 markdown）：
{{"weakness_analysis":"弱點(100字內)","param_adjustments":{{"STRAT.key":value}},"logic_suggestions":["建議1"],"expected_improvement":"預期改善(50字內)","confidence":0.0-1.0}}"""

    settings = _ic_get_settings()
    model = settings.get("ai_model", "claude-sonnet-4-20250514")
    source = settings.get("ai_source", "subscription")
    text = _ic_llm_call(prompt, model, source, max_tokens=1000)
    _iter_log(sid, f"AI 回覆 {len(text)} 字元")

    import re
    try:
        t = text.strip()
        if t.startswith("```"):
            t = t.split("\n", 1)[1] if "\n" in t else t
            t = t.rsplit("```", 1)[0]
        m = re.search(r'\{[\s\S]*\}', t)
        if m:
            result = json.loads(m.group())
        else:
            raise ValueError("no JSON object found")
    except Exception as e:
        _iter_log(sid, f"AI JSON 解析失敗: {e}, text={text[:100]}")
        result = {"weakness_analysis": text[:200] if text else "解析失敗",
                  "param_adjustments": {}, "logic_suggestions": [], "expected_improvement": "", "confidence": 0.3}
    return result

# ── 迭代主控制器 ──────────────────────────────────

def _iteration_controller(sid: int):
    con = market_db()
    cur = con.cursor()
    cur.execute("SELECT target_strategies, codes, market, date_range, layers, convergence FROM iteration_session WHERE id=?", (sid,))
    row = cur.fetchone()
    con.close()
    if not row:
        return

    strategy_ids = json.loads(row[0])
    codes = json.loads(row[1])
    mkt = row[2]
    date_range = json.loads(row[3])
    layers = json.loads(row[4])
    convergence = json.loads(row[5])

    max_rounds = convergence.get("max_rounds", 20)
    min_improvement = convergence.get("min_improvement", 0.05)
    stale_rounds = convergence.get("stale_rounds", 3)
    max_overfit = convergence.get("max_overfit_ratio", 2.0)

    base_config = {
        "codes": codes, "market": mkt,
        "start": date_range.get("start", "2021-01-01"),
        "end": date_range.get("end", datetime.now().strftime("%Y-%m-%d")),
        "strategies": strategy_ids, "initial_capital": 1000000,
        "commission_discount": 0.6, "trade_type": "波段",
    }

    space = _get_strategy_params_space(strategy_ids)
    if not space:
        _iter_log(sid, "錯誤：所選策略沒有可調參數")
        con = market_db()
        con.execute("UPDATE iteration_session SET status='error' WHERE id=?", (sid,))
        con.commit(); con.close()
        return

    original_params = _read_current_params(strategy_ids)
    con = market_db()
    con.execute("UPDATE iteration_session SET status='running' WHERE id=?", (sid,))
    con.commit(); con.close()
    _iter_log(sid, f"迭代開始：{len(strategy_ids)} 策略，{len(codes)} 標的，{len(space)} 參數")

    best_sharpe = -999
    best_params = original_params.copy()
    stale_count = 0
    round_history = []
    total_runs = 0

    try:
        for round_num in range(1, max_rounds + 1):
            con = market_db()
            cur = con.cursor()
            cur.execute("SELECT status FROM iteration_session WHERE id=?", (sid,))
            s_row = cur.fetchone()
            con.close()
            if not s_row or s_row[0] == 'stopped':
                _iter_log(sid, "用戶手動停止")
                break

            _iter_log(sid, f"═══ Round {round_num}/{max_rounds} ═══")
            layer = "grid"
            round_result = {}

            if round_num == 1 and "grid" in layers:
                layer = "grid"
                grid_results = _grid_search(sid, space, base_config)
                total_runs += len(grid_results)
                if grid_results:
                    best_params = grid_results[0]["params"]
                    round_result = grid_results[0]

            elif round_num == 2 and "bayesian" in layers:
                layer = "bayesian"
                bay = _bayesian_optimize(sid, space, base_config, n_trials=50, best_grid_params=best_params)
                total_runs += len(bay.get("trials", []))
                if not bay.get("skipped"):
                    best_params = bay["best_params"]
                bt = _run_backtest_with_params(best_params, base_config)
                total_runs += 1
                s = bt.get("summary", {})
                round_result = {"sharpe": s.get("sharpe_ratio", 0), "winrate": s.get("win_rate_pct", 0),
                                "max_dd": s.get("max_drawdown_pct", 0), "return_pct": s.get("total_return_pct", 0),
                                "trades": s.get("total_trades", 0)}

            else:
                use_ai = "ai" in layers and (round_num % 2 == 1 or "bayesian" not in layers)
                if use_ai:
                    layer = "ai"
                    bt = _run_backtest_with_params(best_params, base_config)
                    total_runs += 1
                    ai_result = _ai_analyze_iteration(sid, bt, best_params, round_history, strategy_ids)
                    adj = ai_result.get("param_adjustments", {})
                    if adj:
                        for k, v in adj.items():
                            if k in space:
                                best_params[k] = max(space[k]["min"], min(space[k]["max"], v))
                        _iter_log(sid, f"AI 調整 {len(adj)} 個參數")
                    s = bt.get("summary", {})
                    round_result = {"sharpe": s.get("sharpe_ratio", 0), "winrate": s.get("win_rate_pct", 0),
                                    "max_dd": s.get("max_drawdown_pct", 0), "return_pct": s.get("total_return_pct", 0),
                                    "trades": s.get("total_trades", 0), "ai_analysis": ai_result}
                elif "bayesian" in layers:
                    layer = "bayesian"
                    narrowed = {}
                    for k, spec in space.items():
                        cur_val = best_params.get(k, spec["default"])
                        rng = (spec["max"] - spec["min"]) * 0.3
                        narrowed[k] = {"min": max(spec["min"], cur_val - rng), "max": min(spec["max"], cur_val + rng),
                                       "default": cur_val, "type": spec["type"]}
                    bay = _bayesian_optimize(sid, narrowed, base_config, n_trials=30, best_grid_params=best_params)
                    total_runs += len(bay.get("trials", []))
                    if not bay.get("skipped"):
                        best_params = bay["best_params"]
                    bt = _run_backtest_with_params(best_params, base_config)
                    total_runs += 1
                    s = bt.get("summary", {})
                    round_result = {"sharpe": s.get("sharpe_ratio", 0), "winrate": s.get("win_rate_pct", 0),
                                    "max_dd": s.get("max_drawdown_pct", 0), "return_pct": s.get("total_return_pct", 0),
                                    "trades": s.get("total_trades", 0)}
                elif "ai" in layers:
                    layer = "ai"
                    bt = _run_backtest_with_params(best_params, base_config)
                    total_runs += 1
                    ai_result = _ai_analyze_iteration(sid, bt, best_params, round_history, strategy_ids)
                    adj = ai_result.get("param_adjustments", {})
                    if adj:
                        for k, v in adj.items():
                            if k in space:
                                best_params[k] = max(space[k]["min"], min(space[k]["max"], v))
                    s = bt.get("summary", {})
                    round_result = {"sharpe": s.get("sharpe_ratio", 0), "winrate": s.get("win_rate_pct", 0),
                                    "max_dd": s.get("max_drawdown_pct", 0), "return_pct": s.get("total_return_pct", 0),
                                    "trades": s.get("total_trades", 0), "ai_analysis": ai_result}
                else:
                    # G-17: 沒有可再優化的層（如只選 grid），grid 跑完即收斂，不再無條件跑 AI 輪次
                    _iter_log(sid, f"layers={layers} 無更多可優化層，於 Round {round_num} 收斂結束")
                    break

            cur_sharpe = round_result.get("sharpe", 0)
            improvement = cur_sharpe - best_sharpe if best_sharpe > -999 else 0

            wf_result = {}
            try:
                if best_params:
                    _apply_params_to_config(best_params)
                wf = walk_forward_backtest({**base_config})
                wf_result = wf.get("summary", {})
                _iter_log(sid, f"Walk-Forward: overfit={wf_result.get('overfit_ratio',0):.2f}, consistency={wf_result.get('consistency',0):.0f}%")
            except Exception as e:
                _iter_log(sid, f"Walk-Forward 失敗: {e}")

            con = market_db()
            con.execute("""
                INSERT INTO iteration_round
                (session_id, round_num, layer, params_used, backtest_summary, walkforward, analysis, param_changes, sharpe, winrate, max_drawdown, improvement, created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (sid, round_num, layer, json.dumps(best_params, ensure_ascii=False),
                  json.dumps(round_result, ensure_ascii=False), json.dumps(wf_result, ensure_ascii=False),
                  json.dumps(round_result.get("ai_analysis", {}), ensure_ascii=False),
                  json.dumps(round_result.get("ai_analysis", {}).get("param_adjustments", {}), ensure_ascii=False),
                  cur_sharpe, round_result.get("winrate", 0), round_result.get("max_dd", 0),
                  improvement, datetime.now().isoformat()))
            con.commit(); con.close()

            round_history.append({"round_num": round_num, "layer": layer, "sharpe": cur_sharpe,
                                  "winrate": round_result.get("winrate", 0), "improvement": improvement})

            if cur_sharpe > best_sharpe:
                best_sharpe = cur_sharpe
                stale_count = 0
            else:
                stale_count += 1

            con = market_db()
            con.execute("""UPDATE iteration_session SET current_round=?, best_sharpe=?, best_winrate=?, best_params=?, total_runs=? WHERE id=?""",
                        (round_num, best_sharpe, round_result.get("winrate", 0), json.dumps(best_params, ensure_ascii=False), total_runs, sid))
            con.commit(); con.close()
            _iter_log(sid, f"Round {round_num}: Sharpe={cur_sharpe:.2f}, 改善={improvement:+.3f}, 停滯={stale_count}")

            if stale_count >= stale_rounds:
                _iter_log(sid, f"收斂：連續 {stale_rounds} 輪無改善")
                break
            if wf_result.get("overfit_ratio", 0) > max_overfit and round_num > 2:
                _iter_log(sid, f"停止：過擬合 (ratio={wf_result.get('overfit_ratio',0):.2f}>{max_overfit})")
                break

    except Exception as e:
        _iter_log(sid, f"迭代例外: {e}")
        import traceback
        _iter_log(sid, traceback.format_exc()[:500])
    finally:
        _apply_params_to_config(original_params)
        con = market_db()
        con.execute("UPDATE iteration_session SET status='converged', completed_at=? WHERE id=? AND status='running'",
                     (datetime.now().isoformat(), sid))
        con.commit(); con.close()
        _iter_log(sid, f"迭代結束，最佳 Sharpe={best_sharpe:.2f}，總回測={total_runs}")
        _iteration_running.pop(sid, None)
        _iteration_progress.pop(sid, None)

# ── 迭代 API Endpoints ───────────────────────────

@app.post("/api/iteration/start")
def iteration_start(config: dict, _: None = Depends(require_token)):
    strategies = config.get("strategies", [])
    if not strategies:
        return JSONResponse({"error": "請選擇至少一個策略"}, status_code=400)
    codes = config.get("codes", [])
    if not codes:
        return JSONResponse({"error": "請選擇至少一個標的"}, status_code=400)

    name = config.get("name", f"迭代實驗 {datetime.now().strftime('%m/%d %H:%M')}")
    mkt = config.get("market", "TW")
    layers = config.get("layers", ["grid", "bayesian", "ai"])
    date_range = {"start": config.get("start", "2021-01-01"),
                  "end": config.get("end", datetime.now().strftime("%Y-%m-%d"))}
    convergence = {"max_rounds": config.get("max_rounds", 20), "min_improvement": config.get("min_improvement", 0.05),
                   "stale_rounds": config.get("stale_rounds", 3), "max_overfit_ratio": config.get("max_overfit_ratio", 2.0)}

    con = market_db()
    cur = con.cursor()
    cur.execute("""INSERT INTO iteration_session (name,target_strategies,codes,market,date_range,layers,convergence,status,created_at) VALUES(?,?,?,?,?,?,?,?,?)""",
                (name, json.dumps(strategies), json.dumps(codes), mkt, json.dumps(date_range),
                 json.dumps(layers), json.dumps(convergence), "pending", datetime.now().isoformat()))
    sid = cur.lastrowid
    con.commit(); con.close()

    t = threading.Thread(target=_iteration_controller, args=(sid,), daemon=True)
    _iteration_running[sid] = t
    t.start()
    return {"session_id": sid, "status": "running", "name": name}

@app.get("/api/iteration/sessions")
def iteration_sessions():
    con = market_db()
    cur = con.cursor()
    cur.execute("SELECT id,name,target_strategies,codes,market,status,current_round,best_sharpe,best_winrate,total_runs,created_at,completed_at FROM iteration_session ORDER BY id DESC LIMIT 50")
    rows = cur.fetchall()
    con.close()
    return [{"id":r[0],"name":r[1],"strategies":json.loads(r[2]) if r[2] else [],"codes":json.loads(r[3]) if r[3] else [],
             "market":r[4],"status":r[5],"current_round":r[6],"best_sharpe":r[7],"best_winrate":r[8],"total_runs":r[9],
             "created_at":r[10],"completed_at":r[11]} for r in rows]

@app.get("/api/iteration/compare")
def iteration_compare(ids: str = ""):
    if not ids:
        return []
    id_list = [int(x.strip()) for x in ids.split(",") if x.strip().isdigit()]
    con = market_db()
    cur = con.cursor()
    results = []
    for sid_ in id_list:
        cur.execute("SELECT id,name,best_sharpe,best_winrate,total_runs,status,best_params FROM iteration_session WHERE id=?", (sid_,))
        r = cur.fetchone()
        if r:
            cur.execute("SELECT round_num,sharpe,winrate,max_drawdown,layer FROM iteration_round WHERE session_id=? ORDER BY round_num", (sid_,))
            rounds = [{"round":rr[0],"sharpe":rr[1],"winrate":rr[2],"max_dd":rr[3],"layer":rr[4]} for rr in cur.fetchall()]
            results.append({"id":r[0],"name":r[1],"best_sharpe":r[2],"best_winrate":r[3],"total_runs":r[4],
                           "status":r[5],"best_params":json.loads(r[6]) if r[6] else {},"rounds":rounds})
    con.close()
    return results

@app.get("/api/iteration/{sid}")
def iteration_detail(sid: int):
    con = market_db()
    cur = con.cursor()
    cur.execute("SELECT * FROM iteration_session WHERE id=?", (sid,))
    cols = [d[0] for d in cur.description]
    row = cur.fetchone()
    if not row:
        con.close()
        return JSONResponse({"error": "not found"}, status_code=404)
    session = dict(zip(cols, row))
    for k in ["target_strategies","codes","date_range","layers","convergence","best_params","log"]:
        if session.get(k):
            try: session[k] = json.loads(session[k])
            except Exception: pass

    cur.execute("SELECT id,round_num,layer,params_used,backtest_summary,walkforward,analysis,param_changes,sharpe,winrate,max_drawdown,improvement,created_at FROM iteration_round WHERE session_id=? ORDER BY round_num", (sid,))
    rounds = []
    for r in cur.fetchall():
        rd = {"id":r[0],"round_num":r[1],"layer":r[2],"sharpe":r[8],"winrate":r[9],"max_drawdown":r[10],"improvement":r[11],"created_at":r[12]}
        for i, k in enumerate(["params_used","backtest_summary","walkforward","analysis","param_changes"], 3):
            try: rd[k] = json.loads(r[i]) if r[i] else {}
            except Exception: rd[k] = {}
        rounds.append(rd)
    con.close()
    session["rounds"] = rounds
    return session

@app.get("/api/iteration/{sid}/live")
def iteration_live(sid: int):
    progress = _iteration_progress.get(sid, {})
    con = market_db()
    cur = con.cursor()
    cur.execute("SELECT status,current_round,best_sharpe,best_winrate,total_runs,log FROM iteration_session WHERE id=?", (sid,))
    row = cur.fetchone()
    con.close()
    if not row:
        return JSONResponse({"error": "not found"}, status_code=404)
    return {"status":row[0],"current_round":row[1],"best_sharpe":row[2],"best_winrate":row[3],
            "total_runs":row[4],"progress":progress,"recent_logs":json.loads(row[5])[-20:] if row[5] else []}

@app.post("/api/iteration/{sid}/stop")
def iteration_stop(sid: int, _: None = Depends(require_token)):
    con = market_db()
    con.execute("UPDATE iteration_session SET status='stopped' WHERE id=? AND status='running'", (sid,))
    con.commit(); con.close()
    _iter_log(sid, "用戶手動停止")
    return {"ok": True}

@app.get("/api/iteration/{sid}/best")
def iteration_best(sid: int):
    con = market_db()
    cur = con.cursor()
    cur.execute("SELECT best_params,best_sharpe,best_winrate FROM iteration_session WHERE id=?", (sid,))
    row = cur.fetchone()
    con.close()
    if not row:
        return JSONResponse({"error": "not found"}, status_code=404)
    return {"best_params":json.loads(row[0]) if row[0] else {},"best_sharpe":row[1],"best_winrate":row[2]}

@app.post("/api/iteration/{sid}/apply")
def iteration_apply(sid: int, _: None = Depends(require_token)):
    con = market_db()
    cur = con.cursor()
    cur.execute("SELECT best_params FROM iteration_session WHERE id=?", (sid,))
    row = cur.fetchone()
    if not row or not row[0]:
        con.close()
        return JSONResponse({"error": "無最佳參數"}, status_code=400)
    params = json.loads(row[0])
    con.close()
    _apply_params_to_config(params)
    # Group params by strategy
    by_strat = {}
    for full_key, val in params.items():
        parts = full_key.split(".", 1)
        if len(parts) == 2:
            by_strat.setdefault(parts[0], {})[parts[1]] = val
    con2 = sqlite3.connect(DB_PATH, check_same_thread=False)
    for sid_, p in by_strat.items():
        cur2 = con2.cursor()
        cur2.execute("SELECT params FROM strategy_config WHERE strategy_id=?", (sid_,))
        row2 = cur2.fetchone()
        existing = json.loads(row2[0]) if row2 and row2[0] else {}
        existing.update(p)
        con2.execute("INSERT OR REPLACE INTO strategy_config(strategy_id, enabled, params, updated_at) VALUES(?,1,?,?)",
                     (sid_, json.dumps(existing), datetime.now().isoformat()))
    con2.commit(); con2.close()
    _iter_log(sid, f"最佳參數已寫入正式策略設定（{len(params)} 個）")
    return {"ok": True, "applied_params": params}



# ══════════════════════════════════════════════════════
# ── Strategy Factory (SF) — AI 自主策略研發系統 ──────
# ══════════════════════════════════════════════════════

_sf_progress = {}

def _init_sf_db():
    con = market_db()
    cur = con.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS sf_strategy (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_id     TEXT UNIQUE NOT NULL,
            name            TEXT NOT NULL,
            description     TEXT,
            direction       TEXT DEFAULT 'BUY',
            category        TEXT DEFAULT 'technical',
            code            TEXT NOT NULL,
            code_hash       TEXT,
            params          TEXT DEFAULT '{}',
            signals_used    TEXT DEFAULT '[]',
            data_sources    TEXT DEFAULT '[]',
            version         INTEGER DEFAULT 1,
            parent_id       INTEGER,
            generation      INTEGER DEFAULT 1,
            status          TEXT DEFAULT 'draft',
            promotion_date  TEXT,
            archive_reason  TEXT,
            best_sharpe     REAL,
            best_winrate    REAL,
            best_return     REAL,
            best_max_dd     REAL,
            wf_consistency  REAL,
            wf_overfit      REAL,
            total_backtests INTEGER DEFAULT 0,
            created_at      TEXT,
            updated_at      TEXT
        );
        CREATE TABLE IF NOT EXISTS sf_backtest_run (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_id     TEXT,
            session_id      INTEGER,
            codes           TEXT,
            market          TEXT,
            date_range      TEXT,
            params_used     TEXT,
            sharpe          REAL,
            winrate         REAL,
            max_drawdown    REAL,
            total_return    REAL,
            total_trades    INTEGER,
            profit_loss_ratio REAL,
            equity_curve    TEXT,
            trades          TEXT,
            error           TEXT,
            created_at      TEXT
        );
        CREATE TABLE IF NOT EXISTS sf_session (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT,
            mode            TEXT DEFAULT 'explore',
            target_market   TEXT DEFAULT 'US',
            target_direction TEXT,
            target_category TEXT,
            codes           TEXT,
            date_range      TEXT,
            num_strategies  INTEGER DEFAULT 3,
            status          TEXT DEFAULT 'pending',
            strategies_created TEXT DEFAULT '[]',
            knowledge_context TEXT,
            log             TEXT DEFAULT '[]',
            created_at      TEXT,
            completed_at    TEXT
        );
        CREATE TABLE IF NOT EXISTS sf_knowledge (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            category        TEXT,
            title           TEXT,
            content         TEXT,
            market_regime   TEXT,
            confidence      REAL DEFAULT 0.5,
            evidence_count  INTEGER DEFAULT 1,
            source_strategies TEXT DEFAULT '[]',
            tags            TEXT DEFAULT '[]',
            embedding       BLOB,
            created_at      TEXT,
            updated_at      TEXT
        );
        CREATE TABLE IF NOT EXISTS sf_llm_queue (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id      INTEGER,
            purpose         TEXT DEFAULT 'generate',
            prompt          TEXT NOT NULL,
            response        TEXT,
            status          TEXT DEFAULT 'pending',
            created_at      TEXT,
            completed_at    TEXT
        );
    """)
    try:
        cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='sf_knowledge_fts'")
        if not cur.fetchone():
            cur.execute("CREATE VIRTUAL TABLE sf_knowledge_fts USING fts5(title, content, tags, tokenize='trigram')")
    except Exception:
        pass
    con.commit()
    con.close()

_init_sf_db()


def _sf_llm_call(session_id: int, prompt: str, purpose: str = "generate", max_tokens: int = 2000) -> str:
    """策略工廠 LLM 呼叫：直連 → queue fallback（等 agent 回填）"""
    ic_settings = {}
    try:
        scon = sqlite3.connect(DB_PATH, check_same_thread=False)
        scur = scon.cursor()
        scur.execute("SELECT key, value FROM ic_settings")
        ic_settings = {r[0]: r[1] for r in scur.fetchall()}
        scon.close()
    except Exception:
        pass

    source = ic_settings.get("source_stock_analyze", "subscription")
    model = ic_settings.get("model_stock_analyze", "claude-sonnet-4-6")

    response = _ic_llm_call(prompt, model=model, source=source, max_tokens=max_tokens)
    if response and not any(kw in response for kw in ("逾時", "失敗", "找不到", "例外")) and len(response) >= 50:
        return response

    alt_source = "api" if source == "subscription" else "subscription"
    _sf_log(session_id, f"LLM {source} 失敗，嘗試 {alt_source}...")
    response = _ic_llm_call(prompt, model=model, source=alt_source, max_tokens=max_tokens)
    if response and not any(kw in response for kw in ("逾時", "失敗", "找不到", "例外")) and len(response) >= 50:
        return response

    _sf_log(session_id, "直連 LLM 皆失敗，放入 queue 等待 agent 處理...")
    now = datetime.now().isoformat()
    con = market_db()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO sf_llm_queue (session_id, purpose, prompt, status, created_at) VALUES (?,?,?,?,?)",
        (session_id, purpose, prompt, "pending", now),
    )
    queue_id = cur.lastrowid
    con.commit()
    con.close()

    import time as _time
    for _ in range(360):
        _time.sleep(5)
        con2 = market_db()
        row = con2.execute("SELECT status, response FROM sf_llm_queue WHERE id=?", (queue_id,)).fetchone()
        srow = con2.execute("SELECT status FROM sf_session WHERE id=?", (session_id,)).fetchone()
        con2.close()
        if row and row[0] == "completed" and row[1]:
            _sf_log(session_id, f"Queue #{queue_id} 由 agent 完成（{len(row[1])} chars）")
            return row[1]
        if srow and srow[0] in ("stopped", "error"):
            return ""
    _sf_log(session_id, f"Queue #{queue_id} 逾時（30 分鐘），跳過")
    con3 = market_db()
    con3.execute("UPDATE sf_llm_queue SET status='timeout' WHERE id=?", (queue_id,))
    con3.commit()
    con3.close()
    return ""


def _sf_log(session_id, msg):
    ts = datetime.now().strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    con = market_db()
    cur = con.cursor()
    cur.execute("SELECT log FROM sf_session WHERE id=?", (session_id,))
    row = cur.fetchone()
    logs = json.loads(row[0]) if row and row[0] else []
    logs.append(entry)
    if len(logs) > 500:
        logs = logs[-500:]
    con.execute("UPDATE sf_session SET log=? WHERE id=?", (json.dumps(logs, ensure_ascii=False), session_id))
    con.commit()
    con.close()

def _next_sf_strategy_id():
    con = market_db()
    cur = con.cursor()
    cur.execute("SELECT MAX(CAST(SUBSTR(strategy_id,4) AS INTEGER)) FROM sf_strategy WHERE strategy_id LIKE 'SF_%'")
    row = cur.fetchone()
    n = (row[0] or 0) + 1
    con.close()
    return f"SF_{n:03d}"

# ── 技術指標計算 ──

def _calc_rsi(closes, period=14):
    import numpy as np
    c = np.array(closes, dtype=float)
    deltas = np.diff(c)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains[-period:])
    avg_loss = np.mean(losses[-period:])
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - 100 / (1 + rs), 2)

def _calc_kd(highs, lows, closes, k_period=9, d_period=3):
    import numpy as np
    if len(closes) < k_period:
        return 50.0, 50.0
    h = np.array(highs[-k_period:], dtype=float)
    l = np.array(lows[-k_period:], dtype=float)
    hh, ll = np.max(h), np.min(l)
    if hh == ll:
        return 50.0, 50.0
    rsv = (closes[-1] - ll) / (hh - ll) * 100
    k = rsv
    d = k
    return round(k, 2), round(d, 2)

def _calc_bbands(closes, period=20, num_std=2):
    import numpy as np
    c = np.array(closes, dtype=float)
    if len(c) < period:
        return c[-1], c[-1], c[-1]
    mid = float(np.mean(c[-period:]))
    std = float(np.std(c[-period:]))
    return round(mid + num_std * std, 4), round(mid, 4), round(mid - num_std * std, 4)

def _calc_atr(highs, lows, closes, period=14):
    import numpy as np
    if len(closes) < 2:
        return 0.0
    trs = []
    for i in range(-min(period, len(closes)-1), 0):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
        trs.append(tr)
    return round(float(np.mean(trs)), 4) if trs else 0.0

# ── 市場上下文建構器 ──

def _build_market_context(code, market, date, bar, history_bars, all_bar_data, all_dates, di, macro_data, chip_data):
    import numpy as np
    closes = [b["close"] for b in history_bars]
    highs = [b["high"] for b in history_bars]
    lows = [b["low"] for b in history_bars]
    volumes = [b["volume"] for b in history_bars]

    if len(closes) < 30:
        return None

    s = pd.Series(closes)
    ma5 = float(s.rolling(5).mean().iloc[-1])
    ma10 = float(s.rolling(10).mean().iloc[-1])
    ma20 = float(s.rolling(20).mean().iloc[-1])
    ma60 = float(s.rolling(60).mean().iloc[-1]) if len(s) >= 60 else ma20
    ma240 = float(s.rolling(240).mean().iloc[-1]) if len(s) >= 240 else None

    dif_list, macd_list, hist_list = calc_macd(closes)

    vol_mean = np.mean(volumes[-20:-1]) if len(volumes) > 20 else np.mean(volumes[:-1]) if len(volumes) > 1 else 1
    vol_ratio = round(volumes[-1] / vol_mean, 3) if vol_mean > 0 else 1.0

    indicators = {
        "ma5": round(ma5, 4), "ma10": round(ma10, 4), "ma20": round(ma20, 4),
        "ma60": round(ma60, 4), "ma240": round(ma240, 4) if ma240 else None,
        "rsi_14": _calc_rsi(closes),
        "kd_k": _calc_kd(highs, lows, closes)[0],
        "kd_d": _calc_kd(highs, lows, closes)[1],
        "macd_dif": round(dif_list[-1], 4), "macd_signal": round(macd_list[-1], 4),
        "macd_hist": round(hist_list[-1], 4),
        "vol_ratio": vol_ratio,
        "atr_14": _calc_atr(highs, lows, closes),
        "bbands_upper": _calc_bbands(closes)[0],
        "bbands_mid": _calc_bbands(closes)[1],
        "bbands_lower": _calc_bbands(closes)[2],
    }

    factors = _calc_alpha_factors(closes, highs, lows, volumes) if len(closes) >= 60 else {}

    macro = macro_data.get(date, {})
    chip = chip_data.get((code, date), {})

    ctx = {
        "price": bar["close"], "open": bar["open"], "high": bar["high"],
        "low": bar["low"], "close": bar["close"], "volume": bar["volume"],
        "history": history_bars[-250:],
        "indicators": indicators,
        "factors": factors,
        "macro": macro,
        "sentiment": {"news_score": 50, "news_count": 0, "market_fear_greed": 50},
        "institutional": chip,
        "holding": None,
        "params": {},
        "date": date,
    }
    return ctx

# ── 動態回測引擎 ──

def _run_dynamic_backtest(config: dict, strategy_code: str, strategy_params: dict = None) -> dict:
    import numpy as np
    codes = config.get("codes", [])
    mkt = config.get("market", "US")
    start = config.get("start", "2021-01-01")
    end = config.get("end", "2025-01-01")
    initial_cap = config.get("initial_capital", 1000000)
    trade_type = config.get("trade_type", "swing")

    for c in codes:
        _ensure_daily_data(c, mkt, start, end)

    macro_symbols = ["^VIX", "^DXY", "^TNX", "GC=F", "CL=F", "BTC-USD", "ETH-USD", "ES=F", "NQ=F", "^SOX"]
    def _fetch_macro_sym(sym):
        try:
            _ensure_daily_data(sym, "US", start, end)
        except Exception:
            pass
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=3) as pool:
        futs = {pool.submit(_fetch_macro_sym, ms): ms for ms in macro_symbols}
        for f in as_completed(futs, timeout=30):
            try:
                f.result()
            except Exception:
                pass

    con = market_db()
    cur = con.cursor()

    bar_data = {}
    cur.execute("SELECT code, date, open, high, low, close, volume FROM daily_kbar WHERE code IN ({}) AND market=? AND date BETWEEN ? AND ? ORDER BY date".format(
        ",".join(["?"]*len(codes))), (*codes, mkt, start, end))
    for r in cur.fetchall():
        bar_data[(r[0], r[1])] = {"open": float(r[2]), "high": float(r[3]), "low": float(r[4]), "close": float(r[5]), "volume": float(r[6]), "date": r[1]}

    macro_data = {}
    macro_map = {"^VIX": "vix", "^DXY": "dxy", "^TNX": "us10y", "GC=F": "gold", "CL=F": "crude_oil",
                 "BTC-USD": "btc", "ETH-USD": "eth", "ES=F": "es_futures", "NQ=F": "nq_futures", "^SOX": "sox"}
    for sym, key in macro_map.items():
        cur.execute("SELECT date, close FROM daily_kbar WHERE code=? AND market='US' AND date BETWEEN ? AND ?", (sym, start, end))
        for r in cur.fetchall():
            macro_data.setdefault(r[0], {})[key] = float(r[1]) if r[1] else None

    chip_data = {}
    try:
        cur2 = sqlite3.connect(DB_PATH, check_same_thread=False).cursor()
        cur2.execute("SELECT code, date, foreign_buy, itrust_buy, dealer_buy FROM chip_snapshot WHERE code IN ({}) AND date BETWEEN ? AND ?".format(
            ",".join(["?"]*len(codes))), (*codes, start, end))
        for r in cur2.fetchall():
            chip_data[(r[0], r[1])] = {"foreign_buy_sell": r[2] or 0, "trust_buy_sell": r[3] or 0, "dealer_buy_sell": r[4] or 0}
    except Exception:
        pass

    con.close()

    all_dates = sorted(set(d for (c, d) in bar_data.keys()))
    if not all_dates:
        return {"summary": {"initial_capital": initial_cap, "final_equity": initial_cap, "total_return_pct": 0,
                            "sharpe_ratio": 0, "total_trades": 0, "win_trades": 0, "win_rate_pct": 0,
                            "max_drawdown_pct": 0, "profit_loss_ratio": 0}, "trades": [], "equity_curve": []}

    safe_builtins = {"abs": abs, "min": min, "max": max, "round": round, "len": len, "sum": sum,
                     "sorted": sorted, "range": range, "enumerate": enumerate, "zip": zip,
                     "dict": dict, "list": list, "tuple": tuple, "set": set, "float": float, "int": int, "str": str,
                     "True": True, "False": False, "None": None, "bool": bool, "isinstance": isinstance,
                     "any": any, "all": all, "map": map, "filter": filter}
    import math as _math
    import types as _types
    _safe_pd = _types.ModuleType("pd")
    _safe_pd.Series = pd.Series
    _safe_pd.DataFrame = pd.DataFrame
    _safe_pd.isna = pd.isna
    _safe_pd.notna = pd.notna
    restricted_globals = {"__builtins__": safe_builtins, "math": _math, "np": np, "pd": _safe_pd}

    try:
        compiled = compile(strategy_code, "<ai_strategy>", "exec")
        exec(compiled, restricted_globals)
    except Exception as e:
        return {"summary": {"sharpe_ratio": 0, "total_trades": 0, "win_rate_pct": 0, "max_drawdown_pct": 0,
                            "total_return_pct": 0, "initial_capital": initial_cap, "final_equity": initial_cap,
                            "profit_loss_ratio": 0, "win_trades": 0},
                "trades": [], "equity_curve": [], "error": f"compile: {e}"}

    evaluate_fn = restricted_globals.get("evaluate")
    if not callable(evaluate_fn):
        return {"summary": {"sharpe_ratio": 0, "total_trades": 0, "win_rate_pct": 0, "max_drawdown_pct": 0,
                            "total_return_pct": 0, "initial_capital": initial_cap, "final_equity": initial_cap,
                            "profit_loss_ratio": 0, "win_trades": 0},
                "trades": [], "equity_curve": [], "error": "no evaluate() function"}

    capital = initial_cap
    holdings = {}
    trades = []
    equity_curve = []
    errors = 0
    max_errors = 5
    params = strategy_params or {}

    for di, today in enumerate(all_dates):
        for code in codes:
            bar = bar_data.get((code, today))
            if not bar:
                continue

            history_bars = []
            for d in all_dates[max(0, di-250):di+1]:
                b = bar_data.get((code, d))
                if b:
                    history_bars.append(b)

            ctx = _build_market_context(code, mkt, today, bar, history_bars, bar_data, all_dates, di, macro_data, chip_data)
            if not ctx:
                continue

            if code in holdings:
                h = holdings[code]
                h["highest"] = max(h["highest"] or price, bar["high"] or price)
                pnl_pct = (bar["close"] - h["cost"]) / h["cost"]
                ctx["holding"] = {"shares": h["shares"], "cost": h["cost"], "pnl_pct": round(pnl_pct, 4),
                                  "entry_date": h["entry_date"], "highest": h["highest"]}

            ctx["params"] = params

            try:
                result = evaluate_fn(ctx)
            except Exception:
                errors += 1
                if errors >= max_errors:
                    return {"summary": {"sharpe_ratio": 0, "total_trades": 0, "win_rate_pct": 0, "max_drawdown_pct": 0,
                                        "total_return_pct": 0, "initial_capital": initial_cap, "final_equity": capital,
                                        "profit_loss_ratio": 0, "win_trades": 0},
                            "trades": trades, "equity_curve": equity_curve, "error": f"too many errors ({errors})"}
                continue

            if not isinstance(result, dict) or "signal" not in result:
                continue
            raw_signal = result.get("signal", "HOLD")
            if isinstance(raw_signal, int):
                signal = {1: "BUY", -1: "SELL"}.get(raw_signal, "HOLD")
            else:
                signal = str(raw_signal).upper()

            if signal == "SELL" and code in holdings:
                h = holdings[code]
                sell_price = bar["close"]
                pnl = (sell_price - h["cost"]) * h["shares"]
                if mkt == "TW":
                    pnl -= sell_price * h["shares"] * TW_TAX
                    pnl -= sell_price * h["shares"] * TW_COMMISSION * TW_DISCOUNT
                else:
                    pnl -= sell_price * h["shares"] * US_SLIPPAGE_PCT
                capital += sell_price * h["shares"] + pnl
                trades.append({"code": code, "action": "SELL", "date": today, "price": sell_price,
                               "shares": h["shares"], "profit": round(pnl, 2),
                               "profit_pct": round((sell_price - h["cost"]) / h["cost"] * 100, 2),
                               "entry_date": h["entry_date"], "entry_price": h["cost"],
                               "signal": result.get("reason", "AI"), "strategy": "SF"})
                del holdings[code]

            elif signal == "BUY" and code not in holdings:
                alloc = capital * 0.1
                if alloc < bar["close"]:
                    continue
                shares = int(alloc / bar["close"])
                if mkt == "TW":
                    shares = (shares // 1000) * 1000
                    if shares <= 0:
                        shares = int(alloc / bar["close"])
                if shares <= 0:
                    continue
                cost = bar["close"] * shares
                if mkt == "TW":
                    cost += bar["close"] * shares * TW_COMMISSION * TW_DISCOUNT
                else:
                    cost += bar["close"] * shares * US_SLIPPAGE_PCT
                capital -= cost
                holdings[code] = {"shares": shares, "cost": bar["close"], "entry_date": today,
                                  "highest": bar["high"]}
                trades.append({"code": code, "action": "BUY", "date": today, "price": bar["close"],
                               "shares": shares, "profit": 0, "profit_pct": 0,
                               "signal": result.get("reason", "AI"), "strategy": "SF"})

        total_val = capital
        for c2, h2 in holdings.items():
            b2 = bar_data.get((c2, today))
            if b2:
                total_val += b2["close"] * h2["shares"]
        equity_curve.append({"date": today, "equity": round(total_val, 2)})

    for c2, h2 in list(holdings.items()):
        last_bar = bar_data.get((c2, all_dates[-1]))
        if last_bar:
            pnl = (last_bar["close"] - h2["cost"]) * h2["shares"]
            capital += last_bar["close"] * h2["shares"]
            trades.append({"code": c2, "action": "SELL", "date": all_dates[-1], "price": last_bar["close"],
                           "shares": h2["shares"], "profit": round(pnl, 2),
                           "profit_pct": round((last_bar["close"] - h2["cost"]) / h2["cost"] * 100, 2),
                           "entry_date": h2["entry_date"], "entry_price": h2["cost"],
                           "signal": "end_of_period", "strategy": "SF"})
    holdings.clear()

    final_equity = capital
    total_return = (final_equity - initial_cap) / initial_cap * 100
    buy_trades = [t for t in trades if t["action"] == "BUY"]
    sell_trades = [t for t in trades if t["action"] == "SELL"]
    wins = [t for t in sell_trades if t["profit"] > 0]
    losses = [t for t in sell_trades if t["profit"] <= 0]
    win_rate = len(wins) / len(sell_trades) * 100 if sell_trades else 0
    avg_win = np.mean([t["profit_pct"] for t in wins]) if wins else 0
    avg_loss = abs(np.mean([t["profit_pct"] for t in losses])) if losses else 1
    pl_ratio = round(avg_win / avg_loss, 2) if avg_loss > 0 else 0

    if len(equity_curve) > 1:
        eq = [e["equity"] for e in equity_curve]
        peak = eq[0]
        max_dd = 0
        for e in eq:
            peak = max(peak, e)
            dd = (peak - e) / peak * 100 if peak > 0 else 0
            max_dd = max(max_dd, dd)
    else:
        max_dd = 0

    if len(equity_curve) > 30:
        eq_arr = np.array([e["equity"] for e in equity_curve])
        daily_returns = np.diff(eq_arr) / eq_arr[:-1]
        sharpe = float(np.mean(daily_returns) / np.std(daily_returns) * np.sqrt(252)) if np.std(daily_returns) > 0 else 0
    else:
        sharpe = 0

    summary = {
        "initial_capital": initial_cap, "final_equity": round(final_equity, 2),
        "total_return_pct": round(total_return, 2),
        "total_trades": len(sell_trades), "win_trades": len(wins),
        "win_rate_pct": round(win_rate, 1), "profit_loss_ratio": pl_ratio,
        "max_drawdown_pct": round(max_dd, 2), "sharpe_ratio": round(sharpe, 3),
    }
    return {"summary": summary, "trades": trades, "equity_curve": equity_curve}

# ── Walk-Forward for dynamic strategies ──

def _sf_walk_forward(config, strategy_code, strategy_params):
    import numpy as np
    import copy
    start_dt = datetime.strptime(config["start"], "%Y-%m-%d")
    end_dt = datetime.strptime(config["end"], "%Y-%m-%d")
    total_days = (end_dt - start_dt).days
    if total_days < 180:
        return {"summary": {"windows": 0, "overfit_ratio": 0, "consistency": 0, "verdict": "data too short"}, "windows": []}

    train_months = 6
    test_months = 2
    windows = []
    cursor = start_dt

    while True:
        train_end = cursor + timedelta(days=train_months * 30)
        test_start = train_end + timedelta(days=1)
        test_end = test_start + timedelta(days=test_months * 30)
        if test_end > end_dt:
            break

        train_cfg = copy.deepcopy(config)
        train_cfg["start"] = cursor.strftime("%Y-%m-%d")
        train_cfg["end"] = train_end.strftime("%Y-%m-%d")
        train_result = _run_dynamic_backtest(train_cfg, strategy_code, strategy_params)

        test_cfg = copy.deepcopy(config)
        test_cfg["start"] = test_start.strftime("%Y-%m-%d")
        test_cfg["end"] = test_end.strftime("%Y-%m-%d")
        test_result = _run_dynamic_backtest(test_cfg, strategy_code, strategy_params)

        windows.append({
            "window": {"train_start": train_cfg["start"], "train_end": train_cfg["end"],
                       "test_start": test_cfg["start"], "test_end": test_cfg["end"]},
            "train": {"return_pct": train_result["summary"]["total_return_pct"],
                      "sharpe": train_result["summary"]["sharpe_ratio"],
                      "trades": train_result["summary"]["total_trades"]},
            "test": {"return_pct": test_result["summary"]["total_return_pct"],
                     "sharpe": test_result["summary"]["sharpe_ratio"],
                     "trades": test_result["summary"]["total_trades"]}
        })
        cursor += timedelta(days=test_months * 30)

    if not windows:
        return {"summary": {"windows": 0, "overfit_ratio": 0, "consistency": 0, "verdict": "no windows"}, "windows": []}

    avg_train = np.mean([w["train"]["return_pct"] for w in windows])
    avg_test = np.mean([w["test"]["return_pct"] for w in windows])
    overfit = round(avg_train / avg_test, 2) if avg_test != 0 else 99.0
    consistency = round(sum(1 for w in windows if w["test"]["return_pct"] > 0) / len(windows) * 100, 1)
    verdict = "robust" if overfit < 2.5 and consistency >= 60 else "overfit risk" if overfit >= 2.5 else "needs review"

    return {"summary": {"windows": len(windows), "avg_train_return": round(avg_train, 2),
                        "avg_test_return": round(avg_test, 2), "overfit_ratio": overfit,
                        "consistency": consistency, "verdict": verdict}, "windows": windows}

# ── 知識系統 ──

def _sf_knowledge_add(category, title, content, market_regime=None, confidence=0.5, source_strategies=None, tags=None):
    con = market_db()
    emb = None
    try:
        from fastembed import TextEmbedding
        model = TextEmbedding("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
        vecs = list(model.embed([title + " " + content]))
        import struct
        emb = struct.pack(f"{len(vecs[0])}f", *vecs[0])
    except Exception:
        pass
    now = datetime.now().isoformat()
    con.execute("INSERT INTO sf_knowledge(category,title,content,market_regime,confidence,source_strategies,tags,embedding,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (category, title, content, market_regime, confidence,
                 json.dumps(source_strategies or []), json.dumps(tags or []), emb, now, now))
    kid = con.execute("SELECT last_insert_rowid()").fetchone()[0]
    try:
        con.execute("INSERT INTO sf_knowledge_fts(rowid,title,content,tags) VALUES(?,?,?,?)",
                    (kid, title, content, json.dumps(tags or [])))
    except Exception:
        pass
    con.commit()
    con.close()
    return kid

def _sf_knowledge_search(query, limit=8):
    con = market_db()
    cur = con.cursor()
    results = []
    try:
        cur.execute("SELECT rowid, title, content, category, confidence, evidence_count FROM sf_knowledge_fts WHERE sf_knowledge_fts MATCH ? ORDER BY rank LIMIT ?",
                    (query, limit))
        for r in cur.fetchall():
            results.append({"id": r[0], "title": r[1], "content": r[2], "category": r[3], "confidence": r[4], "evidence_count": r[5]})
    except Exception:
        cur.execute("SELECT id, title, content, category, confidence, evidence_count FROM sf_knowledge ORDER BY updated_at DESC LIMIT ?", (limit,))
        for r in cur.fetchall():
            results.append({"id": r[0], "title": r[1], "content": r[2], "category": r[3], "confidence": r[4], "evidence_count": r[5]})
    con.close()
    return results

# ── 策略工廠控制器 ──

_SF_STRATEGY_INTERFACE = """
You must write a Python function with EXACTLY this signature:

def evaluate(ctx: dict) -> dict:
    # ctx contains:
    # ctx['price']       - current close price (float)
    # ctx['open'], ctx['high'], ctx['low'], ctx['close'], ctx['volume'] - today's bar
    # ctx['history']     - list of dicts [{date,open,high,low,close,volume}], last 250 bars
    # ctx['indicators']  - dict: ma5,ma10,ma20,ma60,ma240, rsi_14, kd_k,kd_d,
    #                      macd_dif,macd_signal,macd_hist, vol_ratio, atr_14,
    #                      bbands_upper,bbands_mid,bbands_lower
    # ctx['factors']     - dict: 20 alpha factors (mom_5d,mom_10d,mom_20d,mom_60d,
    #                      vol_5d,vol_20d, vol_ratio_5_20, bias_5d,bias_20d,bias_60d,
    #                      price_pos_60d, vol_price_corr_20d, amplitude_5d,amplitude_20d, etc.)
    # ctx['macro']       - dict: vix,dxy,us10y,gold,crude_oil,btc,eth,es_futures,nq_futures (may be None)
    # ctx['sentiment']   - dict: news_score (0-100)
    # ctx['institutional'] - dict: foreign_buy_sell, trust_buy_sell, dealer_buy_sell (may be empty)
    # ctx['holding']     - dict if holding position: {shares,cost,pnl_pct,entry_date,highest}, else None
    # ctx['params']      - dict of tunable parameters
    # ctx['date']        - str 'YYYY-MM-DD'
    #
    # You have access to: math, np (numpy), pd (pandas)
    # You must return: {"signal": "BUY" or "SELL" or "HOLD", "strength": 0.0-1.0, "reason": "short text"}
    pass

IMPORTANT RULES:
- DO NOT use import statements
- DO NOT use open(), exec(), eval(), __import__()
- Use only math, np, pd which are pre-loaded
- The function must handle None values gracefully (macro data may be None)
- Always return a dict with signal, strength, reason keys
- Return HOLD when unsure
"""

def _strategy_factory_controller(session_id):
    import re, hashlib, math as _math
    import numpy as _np
    con = market_db()
    cur = con.cursor()
    cur.execute("SELECT mode, target_market, target_direction, target_category, codes, date_range, num_strategies FROM sf_session WHERE id=?", (session_id,))
    row = cur.fetchone()
    con.close()
    if not row:
        return

    mode, target_market, target_dir, target_cat, codes_json, date_range_json, num_strategies = row
    codes = json.loads(codes_json) if codes_json else ["AAPL"]
    dr = json.loads(date_range_json) if date_range_json else {"start": "2021-01-01", "end": "2025-01-01"}

    con2 = market_db()
    con2.execute("UPDATE sf_session SET status='running' WHERE id=?", (session_id,))
    con2.commit()
    con2.close()

    _sf_progress[session_id] = {"status": "running", "current_strategy": 0, "total": num_strategies, "message": "starting"}
    _sf_log(session_id, f"Strategy Factory starting: mode={mode}, {num_strategies} strategies, codes={codes}")

    created_ids = []
    try:
        # Step 1: Knowledge gathering
        _sf_log(session_id, "Step 1: Gathering knowledge...")
        knowledge_entries = _sf_knowledge_search(f"{target_market} {target_dir or 'trading'} {target_cat or 'strategy'}", limit=8)
        knowledge_text = ""
        if knowledge_entries:
            knowledge_text = "\n\nKNOWLEDGE FROM PAST ITERATIONS:\n"
            for ke in knowledge_entries:
                knowledge_text += f"- [{ke['category']}] {ke['title']}: {ke['content'][:200]}\n"
        _sf_log(session_id, f"Found {len(knowledge_entries)} knowledge entries")

        existing_strategies_ref = ""
        sample_strategies = [s for s in STRATEGIES if s.get("direction") == (target_dir or "BUY")][:2]
        if sample_strategies:
            existing_strategies_ref = "\n\nEXISTING STRATEGIES FOR REFERENCE STYLE (do NOT copy these, create something NEW):\n"
            for ss in sample_strategies:
                existing_strategies_ref += f"- {ss['id']}: {ss['name']} - {ss.get('description','')[:100]}\n"
                existing_strategies_ref += f"  Conditions: {', '.join(ss.get('conditions',[])) }\n"

        for si in range(num_strategies):
            _sf_progress[session_id] = {"status": "running", "current_strategy": si + 1, "total": num_strategies,
                                        "message": f"Generating strategy {si+1}/{num_strategies}"}
            _sf_log(session_id, f"--- Strategy {si+1}/{num_strategies} ---")

            # Step 2: Generate strategy via LLM
            _sf_log(session_id, "Step 2: LLM generating strategy code...")
            direction_hint = f"Direction: {target_dir}" if target_dir else "Direction: BUY or SELL (your choice)"
            category_hint = f"Category focus: {target_cat}" if target_cat else "Category: any (technical, macro, sentiment, multi-factor, hybrid)"

            prompt = f"""You are a quantitative trading strategy developer. Generate a NOVEL trading strategy as a Python function.

{_SF_STRATEGY_INTERFACE}

{direction_hint}
{category_hint}
Market: {target_market}
Test codes: {', '.join(codes)}

{knowledge_text}
{existing_strategies_ref}

Generate a strategy that is DIFFERENT from basic MA crossover or simple RSI oversold/overbought.
Consider combining multiple signals: technical indicators, macro conditions, volume patterns, momentum factors.
Be creative but practical.

After the function, add a comment block with metadata:
# METADATA
# name: <strategy name in English>
# description: <one line description>
# direction: BUY or SELL
# category: technical or macro or sentiment or multi-factor or hybrid
# signals_used: <comma separated list of signals used>
"""
            try:
                response = _sf_llm_call(session_id, prompt, purpose="generate", max_tokens=2000)
            except Exception as e:
                _sf_log(session_id, f"LLM call failed: {e}")
                continue

            if not response or len(response) < 50:
                _sf_log(session_id, f"LLM response too short ({len(response) if response else 0} chars): {(response or '')[:100]}")
                _sf_log(session_id, "skipping this strategy")
                continue

            _sf_log(session_id, f"LLM response: {len(response)} chars")

            # Parse code from response
            code_match = re.search(r'```python\s*(.*?)```', response, re.DOTALL)
            if code_match:
                strategy_code = code_match.group(1).strip()
            else:
                code_match = re.search(r'(def evaluate\(ctx.*?\n(?:    .*\n)*)', response, re.MULTILINE)
                if code_match:
                    strategy_code = code_match.group(1).strip()
                else:
                    _sf_log(session_id, "Could not extract Python code from LLM response, skipping")
                    continue

            # Parse metadata
            meta_name = re.search(r'#\s*name:\s*(.+)', response)
            meta_desc = re.search(r'#\s*description:\s*(.+)', response)
            meta_dir = re.search(r'#\s*direction:\s*(\w+)', response)
            meta_cat = re.search(r'#\s*category:\s*([\w-]+)', response)
            meta_signals = re.search(r'#\s*signals_used:\s*(.+)', response)

            strat_name = meta_name.group(1).strip() if meta_name else f"AI Strategy {si+1}"
            strat_desc = meta_desc.group(1).strip() if meta_desc else ""
            strat_dir = (meta_dir.group(1).strip().upper() if meta_dir else (target_dir or "BUY"))
            strat_cat = meta_cat.group(1).strip() if meta_cat else (target_cat or "multi-factor")
            strat_signals = [s.strip() for s in meta_signals.group(1).split(",")] if meta_signals else []

            # Step 3: Validation
            _sf_log(session_id, "Step 3: Validating strategy code...")
            try:
                compiled = compile(strategy_code, "<ai_strategy>", "exec")
            except SyntaxError as e:
                _sf_log(session_id, f"Syntax error: {e}, attempting fix...")
                fix_prompt = f"Fix this Python syntax error in the strategy code:\nError: {e}\nCode:\n```python\n{strategy_code}\n```\nReturn ONLY the fixed Python code in a ```python block."
                try:
                    fix_resp = _sf_llm_call(session_id, fix_prompt, purpose="fix", max_tokens=2000)
                    fix_match = re.search(r'```python\s*(.*?)```', fix_resp, re.DOTALL)
                    if fix_match:
                        strategy_code = fix_match.group(1).strip()
                        compiled = compile(strategy_code, "<ai_strategy>", "exec")
                    else:
                        _sf_log(session_id, "Fix failed, skipping")
                        continue
                except Exception:
                    _sf_log(session_id, "Fix attempt failed, skipping")
                    continue

            # Check dedup
            import hashlib
            code_hash = hashlib.sha256(strategy_code.encode()).hexdigest()[:16]
            con_dup = market_db()
            dup = con_dup.execute("SELECT strategy_id FROM sf_strategy WHERE code_hash=?", (code_hash,)).fetchone()
            con_dup.close()
            if dup:
                _sf_log(session_id, f"Duplicate of {dup[0]}, skipping")
                continue

            # Dry run
            safe_builtins = {"abs": abs, "min": min, "max": max, "round": round, "len": len, "sum": sum,
                             "sorted": sorted, "range": range, "enumerate": enumerate, "zip": zip,
                             "dict": dict, "list": list, "tuple": tuple, "set": set, "float": float, "int": int,
                             "str": str, "True": True, "False": False, "None": None, "bool": bool,
                             "isinstance": isinstance, "any": any, "all": all, "map": map, "filter": filter}
            test_globals = {"__builtins__": safe_builtins, "math": _math, "np": _np, "pd": pd}
            try:
                exec(compiled, test_globals)
                test_fn = test_globals.get("evaluate")
                if not callable(test_fn):
                    _sf_log(session_id, "No evaluate() function found, skipping")
                    continue
                sample_ctx = {
                    "price": 100, "open": 99, "high": 101, "low": 98, "close": 100, "volume": 1000000,
                    "history": [{"date": "2021-01-01", "open": 99, "high": 101, "low": 98, "close": 100, "volume": 1000000}] * 60,
                    "indicators": {"ma5": 100, "ma10": 99, "ma20": 98, "ma60": 97, "ma240": None,
                                   "rsi_14": 55, "kd_k": 60, "kd_d": 55, "macd_dif": 0.5, "macd_signal": 0.3,
                                   "macd_hist": 0.2, "vol_ratio": 1.2, "atr_14": 2.5,
                                   "bbands_upper": 105, "bbands_mid": 100, "bbands_lower": 95},
                    "factors": {"mom_5d": 2, "mom_10d": 3, "mom_20d": 5, "vol_5d": 1.5, "bias_5d": 1.0,
                                "bias_20d": 2.0, "price_pos_60d": 0.7},
                    "macro": {"vix": 15, "dxy": 104, "us10y": 4.3, "btc": 60000, "eth": 3000,
                              "gold": 2300, "crude_oil": 75, "es_futures": 5500, "nq_futures": 19000},
                    "sentiment": {"news_score": 50}, "institutional": {},
                    "holding": None, "params": {}, "date": "2024-06-15"
                }
                test_result = test_fn(sample_ctx)
                if not isinstance(test_result, dict) or "signal" not in test_result:
                    _sf_log(session_id, f"Dry run returned invalid format: {test_result}, skipping")
                    continue
                _sf_log(session_id, f"Dry run OK: signal={test_result.get('signal')}")
            except Exception as e:
                _sf_log(session_id, f"Dry run failed: {e}, skipping")
                continue

            # Save strategy to DB — deduplicate name
            strategy_id = _next_sf_strategy_id()
            con_save = market_db()
            now = datetime.now().isoformat()
            dup = con_save.execute("SELECT COUNT(*) FROM sf_strategy WHERE name=?", (strat_name,)).fetchone()[0]
            if dup > 0:
                strat_name = f"{strat_name} v{dup + 1}"
            con_save.execute("""INSERT INTO sf_strategy(strategy_id,name,description,direction,category,code,code_hash,
                               signals_used,data_sources,status,created_at,updated_at)
                               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                             (strategy_id, strat_name, strat_desc, strat_dir, strat_cat, strategy_code, code_hash,
                              json.dumps(strat_signals), json.dumps(["daily_kbar", "macro"]), "testing", now, now))
            con_save.commit()
            con_save.close()
            _sf_log(session_id, f"Saved as {strategy_id}: {strat_name}")
            created_ids.append(strategy_id)

            # Step 4: Backtest
            _sf_log(session_id, "Step 4: Running backtest...")
            _sf_progress[session_id]["message"] = f"Backtesting {strategy_id}"
            bt_config = {"codes": codes, "market": target_market, "start": dr["start"], "end": dr["end"]}
            bt_result = _run_dynamic_backtest(bt_config, strategy_code)
            bt_summary = bt_result.get("summary", {})
            bt_error = bt_result.get("error")

            _sf_log(session_id, f"Backtest: sharpe={bt_summary.get('sharpe_ratio',0):.3f}, "
                    f"WR={bt_summary.get('win_rate_pct',0):.1f}%, trades={bt_summary.get('total_trades',0)}"
                    + (f", error={bt_error}" if bt_error else ""))

            # Save backtest run
            con_bt = market_db()
            con_bt.execute("""INSERT INTO sf_backtest_run(strategy_id,session_id,codes,market,date_range,
                              sharpe,winrate,max_drawdown,total_return,total_trades,profit_loss_ratio,
                              equity_curve,trades,error,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                           (strategy_id, session_id, json.dumps(codes), target_market, json.dumps(dr),
                            bt_summary.get("sharpe_ratio", 0), bt_summary.get("win_rate_pct", 0),
                            bt_summary.get("max_drawdown_pct", 0), bt_summary.get("total_return_pct", 0),
                            bt_summary.get("total_trades", 0), bt_summary.get("profit_loss_ratio", 0),
                            json.dumps(bt_result.get("equity_curve", [])[-50:]),
                            json.dumps(bt_result.get("trades", [])[-20:], ensure_ascii=False),
                            bt_error, now))
            con_bt.commit()

            status = "failed"
            if not bt_error and bt_summary.get("total_trades", 0) >= 5 and bt_summary.get("sharpe_ratio", 0) > 0:
                status = "testing"
            if bt_summary.get("sharpe_ratio", 0) >= 0.3 and bt_summary.get("win_rate_pct", 0) >= 40:
                status = "testing"

            # Step 5: Walk-Forward (only if passed backtest)
            wf_result = None
            if status == "testing":
                _sf_log(session_id, "Step 5: Walk-Forward validation...")
                _sf_progress[session_id]["message"] = f"Walk-Forward {strategy_id}"
                wf_result = _sf_walk_forward(bt_config, strategy_code, {})
                wf_summary = wf_result.get("summary", {})
                _sf_log(session_id, f"WF: overfit={wf_summary.get('overfit_ratio',0)}, consistency={wf_summary.get('consistency',0)}%")

                if (wf_summary.get("consistency", 0) >= 60 and wf_summary.get("overfit_ratio", 99) < 2.5
                    and bt_summary.get("sharpe_ratio", 0) >= 0.5 and bt_summary.get("win_rate_pct", 0) >= 45
                    and bt_summary.get("max_drawdown_pct", 100) <= 25 and bt_summary.get("total_trades", 0) >= 20):
                    status = "validated"
                    _sf_log(session_id, f"Strategy {strategy_id} VALIDATED!")
            else:
                _sf_log(session_id, "Step 5: Skipped Walk-Forward (backtest insufficient)")

            # Update strategy status
            con_bt.execute("""UPDATE sf_strategy SET status=?, best_sharpe=?, best_winrate=?, best_return=?,
                              best_max_dd=?, wf_consistency=?, wf_overfit=?, total_backtests=1, updated_at=? WHERE strategy_id=?""",
                           (status, bt_summary.get("sharpe_ratio", 0), bt_summary.get("win_rate_pct", 0),
                            bt_summary.get("total_return_pct", 0), bt_summary.get("max_drawdown_pct", 0),
                            wf_result["summary"]["consistency"] if wf_result else None,
                            wf_result["summary"]["overfit_ratio"] if wf_result else None,
                            now, strategy_id))
            con_bt.commit()
            con_bt.close()

            # Step 6: Knowledge extraction
            _sf_log(session_id, "Step 6: Extracting knowledge...")
            try:
                k_prompt = f"""Analyze this trading strategy's backtest results and extract ONE key insight.

Strategy: {strat_name}
Direction: {strat_dir}, Category: {strat_cat}
Sharpe: {bt_summary.get('sharpe_ratio',0):.3f}, Win Rate: {bt_summary.get('win_rate_pct',0):.1f}%
Max Drawdown: {bt_summary.get('max_drawdown_pct',0):.1f}%, Trades: {bt_summary.get('total_trades',0)}
Status: {status}

Respond in this EXACT JSON format (no other text):
{{"category": "pattern" or "failure" or "indicator_combo", "title": "short title", "content": "insight in 1-2 sentences", "tags": ["tag1", "tag2"]}}"""
                k_resp = _sf_llm_call(session_id, k_prompt, purpose="knowledge", max_tokens=300)
                k_json = re.search(r'\{[\s\S]*\}', k_resp)
                if k_json:
                    kd = json.loads(k_json.group())
                    _sf_knowledge_add(kd.get("category", "pattern"), kd.get("title", strat_name),
                                      kd.get("content", ""), source_strategies=[strategy_id],
                                      tags=kd.get("tags", []),
                                      confidence=0.7 if status == "validated" else 0.4 if status == "testing" else 0.2)
                    _sf_log(session_id, f"Knowledge saved: {kd.get('title','')}")
            except Exception as e:
                _sf_log(session_id, f"Knowledge extraction skipped: {e}")

    except Exception as e:
        import traceback
        _sf_log(session_id, f"Factory error: {e}")
        _sf_log(session_id, traceback.format_exc())
    finally:
        con_f = market_db()
        best_sharpe = 0
        try:
            rows = con_f.execute("SELECT sharpe FROM sf_strategy WHERE session_id=? AND sharpe IS NOT NULL", (session_id,)).fetchall()
            if rows:
                best_sharpe = max(r[0] for r in rows)
        except Exception:
            pass
        con_f.execute("UPDATE sf_session SET status='completed', strategies_created=?, completed_at=?, best_sharpe=?, strategies_tested=? WHERE id=?",
                      (json.dumps(created_ids), datetime.now().isoformat(), best_sharpe, num_strategies, session_id))
        con_f.commit()
        con_f.close()
        _sf_progress[session_id] = {"status": "completed", "current_strategy": num_strategies, "total": num_strategies,
                                     "message": f"Done. Created {len(created_ids)} strategies."}
        _sf_log(session_id, f"Factory complete. Created: {created_ids}")

# ── SF API Endpoints ──

@app.get("/api/sf/sessions")
def sf_sessions():
    con = market_db()
    cur = con.cursor()
    cur.execute("SELECT id,name,mode,target_market,status,strategies_created,num_strategies,created_at,completed_at FROM sf_session ORDER BY id DESC LIMIT 50")
    rows = cur.fetchall()
    con.close()
    return [{"id": r[0], "name": r[1], "mode": r[2], "market": r[3], "status": r[4],
             "strategies_created": json.loads(r[5]) if r[5] else [], "num_strategies": r[6],
             "created_at": r[7], "completed_at": r[8]} for r in rows]

@app.post("/api/sf/session/start")
def sf_session_start(body: dict = Body(...), _: None = Depends(require_token)):
    codes = body.get("codes", [])
    if isinstance(codes, str):
        codes = [c.strip() for c in codes.split(",") if c.strip()]
    if not codes:
        return {"error": "codes required"}
    mode = body.get("mode", "explore")
    market = body.get("market", "US")
    direction = body.get("direction")
    category = body.get("category")
    start = body.get("start", "2021-01-01")
    end = body.get("end", "2025-01-01")
    num = min(int(body.get("num_strategies", 3)), 10)

    con = market_db()
    now = datetime.now()
    name = f"Factory {now.strftime('%m/%d %H:%M')}"
    con.execute("""INSERT INTO sf_session(name,mode,target_market,target_direction,target_category,codes,date_range,
                   num_strategies,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (name, mode, market, direction, category, json.dumps(codes),
                 json.dumps({"start": start, "end": end}), num, "pending", now.isoformat()))
    sid = con.execute("SELECT last_insert_rowid()").fetchone()[0]
    con.commit()
    con.close()

    t = threading.Thread(target=_strategy_factory_controller, args=(sid,), daemon=True)
    t.start()
    return {"session_id": sid, "status": "running", "name": name}

@app.get("/api/sf/session/{sid}/live")
def sf_session_live(sid: int):
    con = market_db()
    cur = con.cursor()
    cur.execute("SELECT status, log, strategies_created FROM sf_session WHERE id=?", (sid,))
    row = cur.fetchone()
    con.close()
    if not row:
        return {"error": "not found"}
    logs = json.loads(row[1]) if row[1] else []
    progress = _sf_progress.get(sid, {})
    return {"status": row[0], "recent_logs": logs[-20:],
            "strategies_created": json.loads(row[2]) if row[2] else [],
            "current_strategy": progress.get("current_strategy", 0),
            "total": progress.get("total", 0), "message": progress.get("message", "")}

@app.get("/api/sf/session/{sid}")
def sf_session_detail(sid: int):
    con = market_db()
    cur = con.cursor()
    cur.execute("SELECT * FROM sf_session WHERE id=?", (sid,))
    row = cur.fetchone()
    if not row:
        con.close()
        return {"error": "not found"}
    cols = [d[0] for d in cur.description]
    session = dict(zip(cols, row))
    for k in ["codes", "date_range", "strategies_created", "log"]:
        if session.get(k):
            session[k] = json.loads(session[k])
    con.close()
    return session

@app.post("/api/sf/session/{sid}/stop")
def sf_session_stop(sid: int, _: None = Depends(require_token)):
    con = market_db()
    con.execute("UPDATE sf_session SET status='stopped' WHERE id=?", (sid,))
    con.commit()
    con.close()
    _sf_progress[sid] = {"status": "stopped", "message": "stopped by user"}
    return {"ok": True}

@app.get("/api/sf/strategies")
def sf_strategies(status: str = None, category: str = None, direction: str = None):
    con = market_db()
    cur = con.cursor()
    q = "SELECT strategy_id,name,description,direction,category,status,best_sharpe,best_winrate,best_return,best_max_dd,wf_consistency,wf_overfit,total_backtests,created_at FROM sf_strategy"
    conditions = []
    params = []
    if status:
        conditions.append("status=?")
        params.append(status)
    if category:
        conditions.append("category=?")
        params.append(category)
    if direction:
        conditions.append("direction=?")
        params.append(direction)
    if conditions:
        q += " WHERE " + " AND ".join(conditions)
    q += " ORDER BY created_at DESC LIMIT 100"
    cur.execute(q, params)
    rows = cur.fetchall()
    con.close()
    return [{"strategy_id": r[0], "name": r[1], "description": r[2], "direction": r[3], "category": r[4],
             "status": r[5], "best_sharpe": r[6], "best_winrate": r[7], "best_return": r[8], "best_max_dd": r[9],
             "wf_consistency": r[10], "wf_overfit": r[11], "total_backtests": r[12], "created_at": r[13]} for r in rows]

@app.get("/api/sf/strategies/{strategy_id}")
def sf_strategy_detail(strategy_id: str):
    con = market_db()
    cur = con.cursor()
    cur.execute("SELECT * FROM sf_strategy WHERE strategy_id=?", (strategy_id,))
    row = cur.fetchone()
    if not row:
        con.close()
        return {"error": "not found"}
    cols = [d[0] for d in cur.description]
    strat = dict(zip(cols, row))
    for k in ["params", "signals_used", "data_sources"]:
        if strat.get(k):
            strat[k] = json.loads(strat[k])
    cur.execute("SELECT id,codes,market,sharpe,winrate,max_drawdown,total_return,total_trades,error,created_at FROM sf_backtest_run WHERE strategy_id=? ORDER BY created_at DESC", (strategy_id,))
    strat["backtest_runs"] = [{"id": r[0], "codes": json.loads(r[1]) if r[1] else [], "market": r[2],
                               "sharpe": r[3], "winrate": r[4], "max_drawdown": r[5], "total_return": r[6],
                               "total_trades": r[7], "error": r[8], "created_at": r[9]} for r in cur.fetchall()]
    con.close()
    return strat

@app.post("/api/sf/strategies/{strategy_id}/promote")
def sf_strategy_promote(strategy_id: str, _: None = Depends(require_token)):
    con = market_db()
    con.execute("UPDATE sf_strategy SET status='promoted', promotion_date=? WHERE strategy_id=?",
                (datetime.now().isoformat(), strategy_id))
    con.commit()
    con.close()
    return {"ok": True}

@app.post("/api/sf/strategies/{strategy_id}/archive")
def sf_strategy_archive(strategy_id: str, body: dict = Body({}), _: None = Depends(require_token)):
    reason = body.get("reason", "manual archive")
    con = market_db()
    con.execute("UPDATE sf_strategy SET status='archived', archive_reason=? WHERE strategy_id=?",
                (reason, strategy_id))
    con.commit()
    con.close()
    return {"ok": True}

@app.post("/api/sf/strategies/{strategy_id}/retest")
def sf_strategy_retest(strategy_id: str, body: dict = Body(...), _: None = Depends(require_token)):
    con = market_db()
    cur = con.cursor()
    cur.execute("SELECT code, params FROM sf_strategy WHERE strategy_id=?", (strategy_id,))
    row = cur.fetchone()
    con.close()
    if not row:
        return {"error": "not found"}
    strategy_code = row[0]
    strategy_params = json.loads(row[1]) if row[1] else {}

    codes = body.get("codes", ["AAPL"])
    if isinstance(codes, str):
        codes = [c.strip() for c in codes.split(",") if c.strip()]
    config = {"codes": codes, "market": body.get("market", "US"),
              "start": body.get("start", "2021-01-01"), "end": body.get("end", "2025-01-01")}
    result = _run_dynamic_backtest(config, strategy_code, strategy_params)

    con2 = market_db()
    now = datetime.now().isoformat()
    s = result.get("summary", {})
    con2.execute("""INSERT INTO sf_backtest_run(strategy_id,codes,market,date_range,sharpe,winrate,max_drawdown,
                    total_return,total_trades,profit_loss_ratio,error,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                 (strategy_id, json.dumps(codes), config["market"],
                  json.dumps({"start": config["start"], "end": config["end"]}),
                  s.get("sharpe_ratio", 0), s.get("win_rate_pct", 0), s.get("max_drawdown_pct", 0),
                  s.get("total_return_pct", 0), s.get("total_trades", 0), s.get("profit_loss_ratio", 0),
                  result.get("error"), now))
    con2.execute("UPDATE sf_strategy SET total_backtests=total_backtests+1, updated_at=? WHERE strategy_id=?", (now, strategy_id))
    if s.get("sharpe_ratio", 0) > 0:
        con2.execute("UPDATE sf_strategy SET best_sharpe=MAX(COALESCE(best_sharpe,0),?), best_winrate=MAX(COALESCE(best_winrate,0),?) WHERE strategy_id=?",
                     (s.get("sharpe_ratio", 0), s.get("win_rate_pct", 0), strategy_id))
    con2.commit()
    con2.close()
    return {"ok": True, "summary": s, "error": result.get("error")}

@app.post("/api/sf/strategies/{strategy_id}/evolve")
def sf_strategy_evolve(strategy_id: str, _: None = Depends(require_token)):
    con = market_db()
    cur = con.cursor()
    cur.execute("SELECT code, name, direction, category, best_sharpe, best_winrate FROM sf_strategy WHERE strategy_id=?", (strategy_id,))
    row = cur.fetchone()
    con.close()
    if not row:
        return {"error": "not found"}
    return {"ok": True, "message": "Use factory session with mode=evolve to evolve strategies"}

@app.get("/api/sf/knowledge")
def sf_knowledge_list(category: str = None):
    con = market_db()
    cur = con.cursor()
    if category:
        cur.execute("SELECT id,category,title,content,market_regime,confidence,evidence_count,tags,created_at FROM sf_knowledge WHERE category=? ORDER BY updated_at DESC LIMIT 50", (category,))
    else:
        cur.execute("SELECT id,category,title,content,market_regime,confidence,evidence_count,tags,created_at FROM sf_knowledge ORDER BY updated_at DESC LIMIT 50")
    rows = cur.fetchall()
    con.close()
    return [{"id": r[0], "category": r[1], "title": r[2], "content": r[3], "market_regime": r[4],
             "confidence": r[5], "evidence_count": r[6], "tags": json.loads(r[7]) if r[7] else [],
             "created_at": r[8]} for r in rows]

@app.post("/api/sf/knowledge/search")
def sf_knowledge_search_api(body: dict = Body(...)):
    query = body.get("query", "")
    if not query:
        return []
    return _sf_knowledge_search(query, limit=body.get("limit", 10))

@app.get("/api/sf/llm-queue")
def sf_llm_queue_list(_: None = Depends(require_token)):
    """取得待處理的 LLM queue 項目（供 Claude Code agent 讀取）"""
    con = market_db()
    rows = con.execute(
        "SELECT id, session_id, purpose, prompt, status, created_at FROM sf_llm_queue WHERE status='pending' ORDER BY id"
    ).fetchall()
    con.close()
    return [{"id": r[0], "session_id": r[1], "purpose": r[2], "prompt": r[3], "status": r[4], "created_at": r[5]} for r in rows]


@app.post("/api/sf/llm-queue/{queue_id}/respond")
def sf_llm_queue_respond(queue_id: int, body: dict = Body(...), _: None = Depends(require_token)):
    """Agent 回填 LLM 回應"""
    response = body.get("response", "")
    if not response:
        raise HTTPException(400, "response is required")
    con = market_db()
    con.execute(
        "UPDATE sf_llm_queue SET status='completed', response=?, completed_at=? WHERE id=?",
        (response, datetime.now().isoformat(), queue_id),
    )
    con.commit()
    con.close()
    return {"ok": True, "queue_id": queue_id}


@app.get("/api/sf/leaderboard")
def sf_leaderboard():
    con = market_db()
    cur = con.cursor()
    cur.execute("""SELECT strategy_id, name, direction, category, status, best_sharpe, best_winrate,
                   best_return, best_max_dd, wf_consistency, total_backtests, created_at
                   FROM sf_strategy WHERE status IN ('testing','validated','promoted') AND best_sharpe IS NOT NULL
                   ORDER BY (COALESCE(best_sharpe,0)*0.4 + COALESCE(best_winrate,0)/100*0.3 + COALESCE(wf_consistency,0)/100*0.2 - COALESCE(best_max_dd,0)/100*0.1) DESC LIMIT 20""")
    rows = cur.fetchall()
    con.close()
    result = []
    for r in rows:
        score = (r[5] or 0)*0.4 + (r[6] or 0)/100*0.3 + (r[9] or 0)/100*0.2 - (r[8] or 0)/100*0.1
        result.append({"strategy_id": r[0], "name": r[1], "direction": r[2], "category": r[3], "status": r[4],
                       "sharpe": r[5], "winrate": r[6], "return": r[7], "max_dd": r[8],
                       "wf_consistency": r[9], "backtests": r[10], "created_at": r[11], "score": round(score, 3)})
    return result


# ── P9: Alpha 因子庫 ──────────────────────────────

def _calc_alpha_factors(closes: list, highs: list, lows: list, volumes: list) -> dict:
    """計算 Alpha158 風格因子（精選 20 個核心因子）"""
    if len(closes) < 60:
        return {}
    import numpy as np
    c = np.array(closes, dtype=float)
    h = np.array(highs, dtype=float)
    l = np.array(lows, dtype=float)
    v = np.array(volumes, dtype=float)

    def _ret(arr, n):
        return (arr[-1] / arr[-n-1] - 1) if len(arr) > n and arr[-n-1] != 0 else 0

    def _std(arr, n):
        return float(np.std(arr[-n:])) if len(arr) >= n else 0

    def _mean(arr, n):
        return float(np.mean(arr[-n:])) if len(arr) >= n else 0

    def _rank_pct(val, arr):
        return float(np.sum(arr <= val) / len(arr)) if len(arr) > 0 else 0.5

    factors = {}
    # 動量因子
    factors["mom_5d"] = round(_ret(c, 5) * 100, 3)
    factors["mom_10d"] = round(_ret(c, 10) * 100, 3)
    factors["mom_20d"] = round(_ret(c, 20) * 100, 3)
    factors["mom_60d"] = round(_ret(c, 60) * 100, 3)
    # 波動因子
    factors["vol_5d"] = round(_std(c[-5:] / c[-6:-1] - 1, 5) * 100, 3) if len(c) > 6 else 0
    factors["vol_20d"] = round(_std(c[-20:] / c[-21:-1] - 1, 20) * 100, 3) if len(c) > 21 else 0
    # 量能因子
    factors["vol_ratio_5_20"] = round(_mean(v, 5) / _mean(v, 20), 3) if _mean(v, 20) > 0 else 1
    factors["vol_chg_5d"] = round(_ret(v, 5) * 100, 3)
    # 技術因子
    ma5 = _mean(c, 5)
    ma20 = _mean(c, 20)
    ma60 = _mean(c, 60)
    factors["bias_5d"] = round((c[-1] / ma5 - 1) * 100, 3) if ma5 > 0 else 0
    factors["bias_20d"] = round((c[-1] / ma20 - 1) * 100, 3) if ma20 > 0 else 0
    factors["bias_60d"] = round((c[-1] / ma60 - 1) * 100, 3) if ma60 > 0 else 0
    # 價格位置因子
    high_60 = float(np.max(h[-60:]))
    low_60 = float(np.min(l[-60:]))
    factors["price_pos_60d"] = round((c[-1] - low_60) / (high_60 - low_60), 3) if high_60 > low_60 else 0.5
    # 量價相關性
    if len(c) >= 20:
        ret20 = np.diff(c[-21:]) / c[-21:-1]
        corr = float(np.corrcoef(ret20, v[-20:])[0, 1]) if np.std(ret20) > 0 and np.std(v[-20:]) > 0 else 0
        factors["vol_price_corr_20d"] = round(corr, 3)
    # 振幅因子
    factors["amplitude_5d"] = round(float(np.mean((h[-5:] - l[-5:]) / c[-5:])) * 100, 3)
    factors["amplitude_20d"] = round(float(np.mean((h[-20:] - l[-20:]) / c[-20:])) * 100, 3)
    # 上下影線比
    body = abs(c[-1] - c[-2]) if len(c) >= 2 else 1
    factors["upper_shadow"] = round((h[-1] - max(c[-1], c[-2] if len(c)>=2 else c[-1])) / c[-1] * 100, 3)
    factors["lower_shadow"] = round((min(c[-1], c[-2] if len(c)>=2 else c[-1]) - l[-1]) / c[-1] * 100, 3)
    # Rank 因子（在歷史中的分位數）
    factors["close_rank_60d"] = round(_rank_pct(c[-1], c[-60:]), 3)
    factors["volume_rank_60d"] = round(_rank_pct(v[-1], v[-60:]), 3)

    return factors

# ── P10: IC/ICIR 因子驗證 ──────────────────────────

def _calc_factor_ic(codes: list, market: str, factor_name: str, periods: int = 20) -> dict:
    """計算因子 IC（與未來 N 日報酬的 rank correlation）"""
    import numpy as np
    factor_vals = []
    fwd_rets = []
    for code in codes:
        try:
            ohlcv = _get_ohlcv_from_cache(code, 300, market)
            if not ohlcv or len(ohlcv["closes"]) < 40:
                continue
            factors = _calc_alpha_factors(ohlcv["closes"], ohlcv["highs"], ohlcv["lows"], ohlcv["volumes"])
            fv = factors.get(factor_name)
            if fv is None:
                continue
            closes = ohlcv["closes"]
            fwd_ret = (closes[-1] / closes[-periods-1] - 1) * 100 if len(closes) > periods else 0
            factor_vals.append(fv)
            fwd_rets.append(fwd_ret)
        except Exception:
            continue
    if len(factor_vals) < 3:
        return {"ic": 0, "samples": len(factor_vals), "msg": "樣本不足"}
    def _spearman(x, y):
        n = len(x)
        rx = np.argsort(np.argsort(x)).astype(float)
        ry = np.argsort(np.argsort(y)).astype(float)
        d = rx - ry
        rho = 1 - 6 * np.sum(d**2) / (n * (n**2 - 1))
        t = rho * np.sqrt((n - 2) / (1 - rho**2 + 1e-12))
        from math import erfc, sqrt
        p = erfc(abs(t) / sqrt(2))
        return float(rho), float(p)
    ic_val, p_val = _spearman(np.array(factor_vals), np.array(fwd_rets))
    return {
        "factor": factor_name,
        "ic": round(ic_val, 4),
        "p_value": round(p_val, 4),
        "samples": len(factor_vals),
        "significant": p_val < 0.05,
        "strength": "強" if abs(ic_val) > 0.1 else ("中" if abs(ic_val) > 0.05 else "弱"),
    }

@app.post("/api/ic/factor-ic")
def factor_ic_check(body: dict):
    """驗證因子有效性"""
    codes = body.get("codes", [])
    market = body.get("market", "TW")
    factors = body.get("factors", ["mom_20d", "vol_ratio_5_20", "bias_20d", "price_pos_60d"])
    periods = body.get("forward_days", 20)
    if not codes:
        con = db()
        rows = con.execute("SELECT code FROM watchlist WHERE market=?", (market,)).fetchall()
        con.close()
        codes = [r[0] for r in rows]
    if not codes:
        codes = ["2330","2317","2454","2881","2882","2891","3711","2308"] if market == "TW" else ["AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","TSM"]
    results = []
    for f in factors:
        r = _calc_factor_ic(codes, market, f, periods)
        results.append(r)
    results.sort(key=lambda x: abs(x.get("ic", 0)), reverse=True)
    return {"data": results, "results": results, "forward_days": periods}

# ── P11: 多因子組合 ──────────────────────────────

def _multi_factor_score(codes: list, market: str) -> list:
    """多因子組合評分：對所有股票計算因子 → 標準化 → 等權加總"""
    import numpy as np
    key_factors = ["mom_20d", "vol_ratio_5_20", "bias_20d", "price_pos_60d", "vol_price_corr_20d"]
    # 正向因子（值越大越好）vs 反向因子
    positive = {"mom_20d", "vol_ratio_5_20", "vol_price_corr_20d"}
    rows = []
    for code in codes:
        try:
            ohlcv = _get_ohlcv_from_cache(code, 120, market)
            if not ohlcv or len(ohlcv["closes"]) < 60:
                continue
            f = _calc_alpha_factors(ohlcv["closes"], ohlcv["highs"], ohlcv["lows"], ohlcv["volumes"])
            if f:
                rows.append({"code": code, "factors": f})
        except Exception:
            continue
    if len(rows) < 3:
        return rows
    # Z-score 標準化
    for fn in key_factors:
        vals = [r["factors"].get(fn, 0) for r in rows]
        mean = np.mean(vals)
        std = np.std(vals)
        if std < 1e-9:
            for r in rows:
                r["factors"][f"z_{fn}"] = 0
        else:
            for r, v in zip(rows, vals):
                z = (v - mean) / std
                r["factors"][f"z_{fn}"] = round(float(z if fn in positive else -z), 3)
    # 等權組合分
    for r in rows:
        zs = [r["factors"].get(f"z_{fn}", 0) for fn in key_factors]
        r["composite"] = round(sum(zs) / len(zs), 3)
        r["composite_score"] = r["composite"]
        r["momentum"] = r["factors"].get("z_mom_20d", 0)
        r["value"] = r["factors"].get("z_bias_20d", 0)
        r["quality"] = r["factors"].get("z_vol_price_corr_20d", 0)
    rows.sort(key=lambda x: x["composite"], reverse=True)
    for i, r in enumerate(rows):
        r["rank"] = i + 1
    return rows

@app.post("/api/ic/multi-factor")
def multi_factor_ranking(body: dict):
    codes = body.get("codes", [])
    market = body.get("market", "TW")
    if not codes:
        con = db()
        rows = con.execute("SELECT code FROM watchlist WHERE market=?", (market,)).fetchall()
        con.close()
        codes = [r[0] for r in rows]
    result = _multi_factor_score(codes, market)
    _ranked = [{"code": r["code"], "rank": r.get("rank", i+1),
                "composite": r.get("composite", 0), "composite_score": r.get("composite_score", r.get("composite", 0)),
                "momentum": r.get("momentum", 0), "value": r.get("value", 0), "quality": r.get("quality", 0),
                "factors": {k: v for k, v in r.get("factors", {}).items() if k.startswith("z_")}}
               for i, r in enumerate(result)]
    return {"data": _ranked, "rankings": _ranked}

# ── P12: 自動因子生成（AI 驅動）──────────────────

@app.post("/api/ic/factor-generate")
def generate_factors(body: dict):
    """用 AI 分析因子表現並建議新因子"""
    market = body.get("market", "TW")
    codes = body.get("codes", [])
    if not codes:
        con = db()
        rows = con.execute("SELECT code FROM watchlist WHERE market=? LIMIT 10", (market,)).fetchall()
        con.close()
        codes = [r[0] for r in rows]
    # 先計算現有因子 IC
    factors_to_test = ["mom_5d", "mom_20d", "vol_ratio_5_20", "bias_20d", "price_pos_60d", "vol_price_corr_20d", "amplitude_20d"]
    ic_results = []
    for f in factors_to_test:
        r = _calc_factor_ic(codes, market, f, 20)
        ic_results.append(r)
    ic_results.sort(key=lambda x: abs(x.get("ic", 0)), reverse=True)
    # 用 AI 分析
    prompt = f"""你是量化因子研究員。以下是 {market} 市場 {len(codes)} 檔股票的因子 IC 分析結果：

{json.dumps(ic_results, ensure_ascii=False, indent=2)}

請基於以上結果：
1. 評估哪些因子有效、哪些無效
2. 建議 3 個新的衍生因子公式（用 closes/highs/lows/volumes 陣列表達）
3. 解釋每個建議因子的邏輯

用繁體中文回覆，簡潔扼要。"""
    try:
        ai_text = _call_claude_analysis(prompt)
        return {"ic_analysis": ic_results, "ai_suggestions": ai_text}
    except Exception as e:
        return {"ic_analysis": ic_results, "ai_suggestions": f"AI 分析失敗: {e}"}

# ── P13: Auto-Quant AI 迭代策略 ──────────────────

@app.post("/api/ic/auto-quant")
def auto_quant_iterate(body: dict):
    """AI 分析策略績效並建議迭代改進"""
    code = body.get("code", "")
    market = body.get("market", "TW")
    # 取回測歷史
    con = market_db()
    rows = con.execute("SELECT name, summary FROM backtest_result ORDER BY id DESC LIMIT 5").fetchall()
    con.close()
    bt_summaries = [{"name": r[0], "summary": json.loads(r[1]) if r[1] else {}} for r in rows]
    # 取因子
    factors = {}
    if code:
        try:
            ohlcv = _get_ohlcv_from_cache(code, 120, market)
            if ohlcv and len(ohlcv["closes"]) >= 60:
                factors = _calc_alpha_factors(ohlcv["closes"], ohlcv["highs"], ohlcv["lows"], ohlcv["volumes"])
        except Exception:
            pass

    prompt = f"""你是量化策略研究員。請分析以下資訊並提出策略迭代建議：

## 最近回測結果
{json.dumps(bt_summaries, ensure_ascii=False, indent=2)}

## 當前因子值（{code} {market}）
{json.dumps(factors, ensure_ascii=False, indent=2) if factors else '無'}

請提出：
1. 當前策略的主要弱點（勝率/回撤/風險）
2. 3 個具體的策略改進假設
3. 每個假設的驗證方法

用繁體中文，簡潔格式。"""
    try:
        ai_text = _call_claude_analysis(prompt)
        return {"backtests": bt_summaries, "current_factors": factors, "ai_iteration": ai_text}
    except Exception as e:
        return {"backtests": bt_summaries, "ai_iteration": f"分析失敗: {e}"}

# ── P14: 社群情緒 Reddit/Twitter ──────────────────

@app.get("/api/ic/social-sentiment/{code}")
def social_sentiment(code: str, market: str = "US"):
    """社群情緒分析（Reddit/Twitter）— 目前使用 yfinance news 作為替代"""
    ev = _get_events(code.upper(), market.upper())
    news = ev.get("news", [])
    neg_kw = ["downgrade","lawsuit","recall","decline","cut","warning","sell","bearish","short","下修","裁員","虧損"]
    pos_kw = ["upgrade","beat","record","growth","approval","buy","bullish","上修","成長","突破","獲利"]
    pos_count = sum(1 for n in news if any(k in n["title"].lower() for k in pos_kw))
    neg_count = sum(1 for n in news if any(k in n["title"].lower() for k in neg_kw))
    total = max(len(news), 1)
    sentiment_score = round(50 + (pos_count - neg_count) / total * 30, 1)
    return {
        "code": code, "market": market,
        "sentiment_score": sentiment_score,
        "positive": pos_count, "negative": neg_count, "neutral": total - pos_count - neg_count,
        "news_count": len(news),
        "source": "yfinance_news (Reddit/Twitter API 規劃中)",
        "headlines": [n["title"] for n in news[:5]],
    }

# ── P15: Derivatives 期權鏈/Greeks ──────────────────

@app.get("/api/ic/options/{code}")
def options_chain(code: str, market: str = "US"):
    """期權鏈數據（使用 yfinance）"""
    try:
        import yfinance as yf
        tk = yf.Ticker(code.upper())
        expirations = tk.options
        if not expirations:
            return {"code": code, "options": [], "msg": "無期權數據"}
        exp = expirations[0]
        chain = tk.option_chain(exp)
        calls = chain.calls.head(10).to_dict("records") if hasattr(chain, 'calls') and len(chain.calls) > 0 else []
        puts = chain.puts.head(10).to_dict("records") if hasattr(chain, 'puts') and len(chain.puts) > 0 else []
        # 清理 NaN/Timestamp
        def clean(recs):
            import numpy as np
            result = []
            for r in recs:
                cleaned = {}
                for k, v in r.items():
                    if v is None or (isinstance(v, float) and v != v):
                        cleaned[k] = None
                    elif isinstance(v, (np.integer,)):
                        cleaned[k] = int(v)
                    elif isinstance(v, (np.floating,)):
                        cleaned[k] = None if np.isnan(v) else round(float(v), 4)
                    elif isinstance(v, np.bool_):
                        cleaned[k] = bool(v)
                    elif hasattr(v, 'isoformat'):
                        cleaned[k] = v.isoformat()
                    else:
                        cleaned[k] = v
                result.append(cleaned)
            return result
        calls = clean(calls)
        puts = clean(puts)
        # Put/Call ratio
        total_call_vol = sum(c.get("volume", 0) or 0 for c in calls)
        total_put_vol = sum(p.get("volume", 0) or 0 for p in puts)
        pcr = round(total_put_vol / total_call_vol, 3) if total_call_vol > 0 else 0
        return {
            "code": code, "expiration": exp,
            "expirations": list(expirations[:5]),
            "calls": calls, "puts": puts,
            "put_call_ratio": pcr,
            "signal": "偏空" if pcr > 1.2 else ("偏多" if pcr < 0.7 else "中性"),
        }
    except Exception as e:
        return {"code": code, "options": [], "error": str(e)}

# ── P16: Crypto 多幣種+鏈上 ──────────────────

@app.get("/api/ic/crypto/{symbol}")
def crypto_data(symbol: str = "BTC"):
    """加密貨幣數據（使用 yfinance）"""
    try:
        import yfinance as yf
        ticker = f"{symbol.upper()}-USD"
        tk = yf.Ticker(ticker)
        hist = tk.history(period="3mo", interval="1d")
        if hist.empty:
            return {"symbol": symbol, "error": "無數據"}
        closes = hist["Close"].tolist()
        volumes = hist["Volume"].tolist()
        price = round(closes[-1], 2)
        ret_7d = round((closes[-1] / closes[-7] - 1) * 100, 2) if len(closes) >= 7 else 0
        ret_30d = round((closes[-1] / closes[-30] - 1) * 100, 2) if len(closes) >= 30 else 0
        avg_vol = sum(volumes[-20:]) / 20 if len(volumes) >= 20 else volumes[-1]
        rvol = round(volumes[-1] / avg_vol, 2) if avg_vol > 0 else 1
        return {
            "symbol": symbol, "price": price,
            "ret_7d": ret_7d, "ret_30d": ret_30d,
            "volume_24h": round(volumes[-1], 0),
            "rvol": rvol,
            "source": "yfinance (鏈上數據規劃中)",
        }
    except Exception as e:
        return {"symbol": symbol, "error": str(e)}

# ── P17: OpenBB 統一引擎（規劃中）──────────────────

@app.get("/api/ic/openbb/status")
def openbb_status():
    """檢查 OpenBB SDK 是否可用"""
    try:
        import openbb
        return {"available": True, "version": getattr(openbb, '__version__', 'unknown')}
    except ImportError:
        return {"available": False, "msg": "OpenBB SDK 未安裝。安裝：pip install openbb"}

@app.get("/api/ic/factors/{code}")
def ic_alpha_factors(code: str, market: str = "TW"):
    """取個股 Alpha 因子"""
    try:
        ohlcv = _get_ohlcv_from_cache(code.upper(), 120, market.upper())
        if not ohlcv or len(ohlcv.get("closes", [])) < 60:
            return {"factors": {}, "msg": "數據不足"}
        factors = _calc_alpha_factors(ohlcv["closes"], ohlcv["highs"], ohlcv["lows"], ohlcv["volumes"])
        return {"code": code, "market": market, "factors": factors}
    except Exception as e:
        return {"factors": {}, "error": str(e)}


# ═══════════════════════════════════════════════════════════════
# ── 資訊中心 (Info Center)  /api/ic/  ──────────────────────────
# ═══════════════════════════════════════════════════════════════

def _ic_db_migrate():
    con = db()
    con.executescript("""
        CREATE TABLE IF NOT EXISTS ic_settings (
            key   TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS ic_recommendations (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            market       TEXT,
            code         TEXT,
            name         TEXT,
            direction    TEXT,
            score        REAL,
            reasons      TEXT,
            indicators   TEXT,
            ai_analysis  TEXT,
            sources_used TEXT,
            confidence   REAL DEFAULT 0.5,
            disclaimer   TEXT,
            created_at   TEXT
        );
        CREATE TABLE IF NOT EXISTS ic_news_sources (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT NOT NULL,
            url          TEXT DEFAULT '',
            type         TEXT DEFAULT 'HTML',
            market       TEXT DEFAULT 'ALL',
            source_type  TEXT DEFAULT 'user',
            description  TEXT DEFAULT '',
            active       INTEGER DEFAULT 1,
            reliability  TEXT DEFAULT 'reference',
            last_fetched TEXT
        );
        CREATE TABLE IF NOT EXISTS ic_news_cache (
            source_id   INTEGER PRIMARY KEY,
            content     TEXT,
            fetched_at  TEXT
        );
        CREATE TABLE IF NOT EXISTS ic_rec_history (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            market       TEXT,
            code         TEXT,
            name         TEXT,
            direction    TEXT,
            score        REAL,
            confidence   REAL DEFAULT 0.5,
            ai_analysis  TEXT,
            entry_price  REAL,
            created_at   TEXT,
            eval_price   REAL,
            eval_at      TEXT,
            pnl_pct      REAL,
            outcome      TEXT DEFAULT 'PENDING'
        );
        -- 知識庫切塊（RAG）：一份來源 → 多個 chunk，各帶向量
        CREATE TABLE IF NOT EXISTS ic_kb_chunks (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id   INTEGER,
            chunk_idx   INTEGER,
            text        TEXT,
            embedding   BLOB,
            created_at  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_kb_chunks_src ON ic_kb_chunks(source_id);
        -- trigram FTS5：中文子字串關鍵字檢索（稀疏）
        CREATE VIRTUAL TABLE IF NOT EXISTS ic_kb_fts USING fts5(
            text, content='ic_kb_chunks', content_rowid='id', tokenize='trigram'
        );
    """)
    # 補欄位（若舊表已存在缺少欄位）
    for col, typedef in [
        ("sources_used", "TEXT"),
        ("confidence",   "REAL DEFAULT 0.5"),
        ("disclaimer",   "TEXT"),
        ("entry_price",  "REAL"),
    ]:
        try:
            con.execute(f"ALTER TABLE ic_recommendations ADD COLUMN {col} {typedef}")
        except Exception:
            pass
    # ic_news_sources 補欄位：entities(關聯標籤 JSON) + content(手動貼上的純文字)
    for col, defval in [("entities", "''"), ("content", "''")]:
        try:
            con.execute(f"ALTER TABLE ic_news_sources ADD COLUMN {col} TEXT DEFAULT {defval}")
        except Exception:
            pass
    con.commit()
    con.close()

# ── 新聞來源抓取工具 ──────────────────────────────────

class _HtmlTextExtractor(HTMLParser):
    """Extract visible text from HTML, skipping script/style/nav."""
    SKIP = {"script","style","nav","header","footer","aside","noscript","form","button"}
    def __init__(self):
        super().__init__()
        self._depth = 0
        self._chunks = []
    def handle_starttag(self, tag, attrs):
        if tag.lower() in self.SKIP:
            self._depth += 1
    def handle_endtag(self, tag):
        if tag.lower() in self.SKIP and self._depth > 0:
            self._depth -= 1
    def handle_data(self, data):
        if self._depth == 0:
            s = data.strip()
            if len(s) > 4:
                self._chunks.append(s)
    def get_text(self, max_chars=800) -> str:
        return " ".join(self._chunks)[:max_chars]

def _ic_fetch_url(url: str, timeout: int = 12) -> bytes:
    """Fetch URL (max 100 KB). Uses proper SSL by default; falls back to
    no-verify only for non-public / self-signed hosts, with a warning."""
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; SmartInvestMonitor/1.0)",
        "Accept":     "text/html,application/xhtml+xml,application/xml,application/json;q=0.9,*/*;q=0.8",
    }
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read(100 * 1024)
    except ssl.SSLError:
        # Retry without verification only for SSL failures (e.g. self-signed cert).
        print(f"[IC fetch] SSL error for {url}, retrying without cert verify")
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode    = ssl.CERT_NONE
        req2 = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req2, timeout=timeout, context=ctx) as resp:
            return resp.read(100 * 1024)

def _ic_parse_rss(raw: bytes) -> str:
    """Parse RSS/Atom feed, return top-6 headlines."""
    try:
        text = raw.decode("utf-8", errors="replace")
        root = ET.fromstring(text)
    except Exception as e:
        return f"[RSS 解析錯誤: {e}]"
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    items = root.findall(".//item") or root.findall(".//atom:entry", ns)
    lines = []
    for item in items[:6]:
        title = (item.findtext("title") or
                 item.findtext("atom:title", namespaces=ns) or "").strip()
        desc  = (item.findtext("description") or
                 item.findtext("atom:summary", namespaces=ns) or "").strip()
        if desc:
            p = _HtmlTextExtractor()
            try: p.feed(desc)
            except Exception: pass
            desc = p.get_text(100)
        if title:
            lines.append(f"• {title}" + (f" — {desc}" if desc else ""))
    return "\n".join(lines)[:800] if lines else "[RSS 無內容]"

def _ic_parse_html(raw: bytes) -> str:
    """Extract readable text from an HTML page."""
    try:
        text = raw.decode("utf-8", errors="replace")
        p = _HtmlTextExtractor()
        p.feed(text)
        return p.get_text(800)
    except Exception as e:
        return f"[HTML 解析錯誤: {e}]"

def _ic_parse_json_feed(raw: bytes) -> str:
    """Convert a JSON API response to a short readable summary."""
    try:
        data = json.loads(raw.decode("utf-8", errors="replace"))
        if isinstance(data, list):
            lines = []
            for item in data[:5]:
                if isinstance(item, dict):
                    title = item.get("title") or item.get("headline") or item.get("name") or ""
                    desc  = item.get("description") or item.get("summary") or item.get("content") or ""
                    if title:
                        lines.append(f"• {str(title).strip()}: {str(desc).strip()[:100]}")
            return "\n".join(lines)[:800] if lines else json.dumps(data, ensure_ascii=False)[:400]
        return json.dumps(data, ensure_ascii=False)[:600]
    except Exception:
        return raw.decode("utf-8", errors="replace")[:600]

def _ic_fetch_source(source: dict) -> str:
    """Fetch and parse one user-defined source. Returns extracted text (≤800 chars)."""
    url = (source.get("url") or "").strip()
    if not url:
        return "[未設定 URL]"
    src_type = (source.get("type") or "HTML").upper()
    try:
        raw = _ic_fetch_url(url)
    except Exception as e:
        return f"[抓取失敗: {str(e)[:100]}]"
    # Auto-detect RSS from content signature
    peek = raw[:300].decode("utf-8", errors="replace").lower()
    if src_type == "RSS" or "<rss" in peek or "<feed" in peek:
        return _ic_parse_rss(raw)
    elif src_type == "API" or raw[:1].decode("utf-8", errors="replace").strip() in ("{", "["):
        return _ic_parse_json_feed(raw)
    else:
        return _ic_parse_html(raw)

def _ic_get_news_for_market(market: str) -> str:
    """
    Return cached news content from ic_news_cache for sources matching `market`.
    Used to inject user-defined source content into the AI prompt.
    """
    con = db()
    cur = con.cursor()
    cur.execute("""
        SELECT ns.name, ns.reliability, nc.content, nc.fetched_at
        FROM ic_news_sources ns
        JOIN ic_news_cache nc ON nc.source_id = ns.id
        WHERE ns.active = 1
          AND (ns.market = 'ALL' OR ns.market = ? OR ? = 'ALL')
          AND nc.content NOT LIKE '[%失敗%]'
          AND nc.content NOT LIKE '[未設定%]'
        ORDER BY nc.fetched_at DESC
        LIMIT 5
    """, (market, market))
    rows = cur.fetchall()
    con.close()
    if not rows:
        return ""
    parts = []
    for name, rel, content, fetched_at in rows:
        age_min = int((datetime.now() - datetime.fromisoformat(fetched_at)).total_seconds() / 60) if fetched_at else 999
        parts.append(f"[{name}|{rel}|{age_min}分鐘前]\n{(content or '')[:300]}")
    return "\n\n".join(parts)

# 系統內建來源定義（唯讀，不存 DB，前端靠此 list 顯示）
IC_SYSTEM_SOURCES = [
    {"id": "sys_yfinance",  "name": "yfinance",    "type": "API",    "market": "US/MACRO",
     "source_type": "system", "reliability": "reference",
     "description": "美股/總經價格數據，15分鐘延遲（雅虎財經）",
     "datasource_ids": ["yfinance", "yfinance_us", "yfinance_fund", "yfinance_sector", "yfinance_events", "yfinance_options", "yfinance_crypto"]},
    {"id": "sys_shioaji",   "name": "Shioaji K線",  "type": "API",    "market": "TW",
     "source_type": "system", "reliability": "confirmed",
     "description": "台股K線、即時報價（永豐金API）",
     "datasource_ids": ["shioaji"]},
    {"id": "sys_twse",      "name": "TWSE公開資料", "type": "API",    "market": "TW",
     "source_type": "system", "reliability": "confirmed",
     "description": "法人買賣超、融資融券、當沖比（台灣證交所）",
     "datasource_ids": ["twse"]},
    {"id": "sys_monitor_db","name": "本機持倉/籌碼","type": "BUILTIN","market": "TW",
     "source_type": "system", "reliability": "confirmed",
     "description": "持倉、自選股、歷史訊號、籌碼快照（monitor.db）",
     "datasource_ids": []},
]

_IC_DATASOURCE_IDS = set()
for _s in IC_SYSTEM_SOURCES:
    _IC_DATASOURCE_IDS.update(_s.get("datasource_ids", []))

_ic_db_migrate()

# ── 知識庫 RAG 引擎（hybrid: FTS5 trigram 稀疏 + fastembed 稠密 + RRF）────
import struct as _struct

_KB_EMBED_MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
_KB_EMBED = None          # None=未載入, False=不可用, 物件=可用
_KB_DIM = 384

def _kb_embedder():
    """延遲載入本地 embedding 模型；裝不起來回 None（自動退回純 FTS5）。"""
    global _KB_EMBED
    if _KB_EMBED is False:
        return None
    if _KB_EMBED is None:
        try:
            from fastembed import TextEmbedding
            _KB_EMBED = TextEmbedding(_KB_EMBED_MODEL_NAME)
        except Exception as e:
            print(f"[KB] fastembed 不可用，退回純關鍵字檢索：{e}")
            _KB_EMBED = False
            return None
    return _KB_EMBED

def _kb_embed_texts(texts: list) -> list:
    """回傳 list[bytes]（float32 packed）；模型不可用時回 [None,...]。"""
    m = _kb_embedder()
    if not m or not texts:
        return [None] * len(texts)
    import numpy as np
    out = []
    for v in m.embed(list(texts)):
        a = np.asarray(v, dtype=np.float32)
        n = np.linalg.norm(a)
        if n > 0:
            a = a / n               # 正規化 → 內積即 cosine
        out.append(a.astype(np.float32).tobytes())
    return out

def _kb_chunk_text(text: str, size: int = 480, overlap: int = 80) -> list:
    """按段落聚合、再以字元視窗切塊（中文友善）。"""
    text = (text or "").strip()
    if not text:
        return []
    paras = [p.strip() for p in text.replace("\r", "").split("\n") if p.strip()]
    chunks, buf = [], ""
    for p in paras:
        if len(buf) + len(p) + 1 <= size:
            buf = (buf + "\n" + p) if buf else p
        else:
            if buf:
                chunks.append(buf)
            if len(p) <= size:
                buf = p
            else:  # 單段過長：滑動視窗切
                i = 0
                while i < len(p):
                    chunks.append(p[i:i + size])
                    i += size - overlap
                buf = ""
    if buf:
        chunks.append(buf)
    return [c for c in chunks if len(c) >= 10]

def _kb_ingest(source_id: int, text: str):
    """把一份來源的文字切塊 + 向量化，覆寫該來源的所有 chunk。"""
    con = db()
    # 清舊 chunk（連帶 FTS）
    old = [r[0] for r in con.execute("SELECT id FROM ic_kb_chunks WHERE source_id=?", (source_id,)).fetchall()]
    for cid in old:
        con.execute("INSERT INTO ic_kb_fts(ic_kb_fts, rowid, text) VALUES('delete', ?, (SELECT text FROM ic_kb_chunks WHERE id=?))", (cid, cid))
    con.execute("DELETE FROM ic_kb_chunks WHERE source_id=?", (source_id,))
    chunks = _kb_chunk_text(text)
    embs = _kb_embed_texts(chunks)
    now = datetime.now().isoformat(timespec="seconds")
    for idx, (c, e) in enumerate(zip(chunks, embs)):
        cur = con.execute(
            "INSERT INTO ic_kb_chunks(source_id,chunk_idx,text,embedding,created_at) VALUES(?,?,?,?,?)",
            (source_id, idx, c, e, now))
        con.execute("INSERT INTO ic_kb_fts(rowid, text) VALUES(?,?)", (cur.lastrowid, c))
    con.commit()
    con.close()
    return len(chunks)

def _kb_rrf(rank_lists: list, k: int = 60) -> dict:
    """Reciprocal Rank Fusion：rank_lists=[[id按相關度排序], ...] → {id: score}。"""
    scores = {}
    for lst in rank_lists:
        for rank, cid in enumerate(lst):
            scores[cid] = scores.get(cid, 0.0) + 1.0 / (k + rank + 1)
    return scores

def _kb_parse_ent_field(raw) -> list:
    """ic_news_sources.entities 欄位 → list（相容 JSON 與逗號字串）。"""
    if not raw:
        return []
    raw = raw.strip()
    if raw.startswith("["):
        try:
            return [str(x).strip() for x in json.loads(raw) if str(x).strip()]
        except Exception:
            return []
    return [x.strip() for x in raw.replace("，", ",").split(",") if x.strip()]

def _kb_search(query: str, top_k: int = 6, types: list = None,
               boost_entities: list = None, filter_entities: list = None) -> list:
    """Hybrid 檢索（Phase B：標籤對焦）。回傳含 entities/matched 的命中清單。
    - types: 限定來源類型（PDF/TEXT/HTML/...）
    - boost_entities: 命中這些標籤的來源在排序上加權（窮人版圖譜，零 token）
    - filter_entities: 硬篩，只留標籤命中的來源"""
    query = (query or "").strip()
    if not query:
        return []
    def _norm(s): return str(s).strip().lower()
    boost_set  = {_norm(e) for e in (boost_entities or []) if str(e).strip()}
    filter_set = {_norm(e) for e in (filter_entities or []) if str(e).strip()}
    con = db()
    # 有效來源（active）對應表（含 entities）
    rows = con.execute("""SELECT id,name,type,reliability,entities FROM ic_news_sources WHERE active=1""").fetchall()
    src_meta = {r[0]: {"name": r[1], "type": (r[2] or "").upper(), "reliability": r[3] or "reference",
                       "entities": _kb_parse_ent_field(r[4])} for r in rows}
    if types:
        types_u = {t.upper() for t in types}
        src_meta = {sid: m for sid, m in src_meta.items() if m["type"] in types_u}
    if filter_set:
        src_meta = {sid: m for sid, m in src_meta.items()
                    if filter_set & {_norm(e) for e in m["entities"]}}
    if not src_meta:
        con.close(); return []
    allowed = set(src_meta.keys())

    # 稀疏：FTS5 trigram。把 query 拆成詞、以 OR 比對（單一長片語幾乎不會命中）。
    # 中文無空白時整串視為一詞；trigram 對 ≥3 字做子字串匹配。
    sparse_ids = []
    try:
        import re as _re
        terms = [t for t in _re.split(r"\s+", query.replace('"', " ").strip()) if len(t) >= 2]
        if terms:
            match_expr = " OR ".join('"%s"' % t for t in terms)
            frows = con.execute(
                "SELECT rowid FROM ic_kb_fts WHERE ic_kb_fts MATCH ? ORDER BY bm25(ic_kb_fts) LIMIT 40",
                (match_expr,)).fetchall()
            sparse_ids = [r[0] for r in frows]
    except Exception:
        sparse_ids = []

    # 稠密：cosine（模型可用時）
    dense_ids = []
    qv = _kb_embed_texts([query])[0]
    if qv is not None:
        import numpy as np
        qa = np.frombuffer(qv, dtype=np.float32)
        crows = con.execute("SELECT id, embedding FROM ic_kb_chunks WHERE embedding IS NOT NULL").fetchall()
        sims = []
        for cid, emb in crows:
            va = np.frombuffer(emb, dtype=np.float32)
            if va.shape == qa.shape:
                sims.append((cid, float(np.dot(qa, va))))
        sims.sort(key=lambda x: x[1], reverse=True)
        dense_ids = [cid for cid, _ in sims[:40]]

    rank_lists = [l for l in (dense_ids, sparse_ids) if l]
    if not rank_lists:
        con.close(); return []
    fused = _kb_rrf(rank_lists)

    # 先把候選 chunk 的 source/text 撈出來（只取進入 fused 的）
    cand = {}
    for cid in fused:
        row = con.execute("SELECT source_id, text FROM ic_kb_chunks WHERE id=?", (cid,)).fetchone()
        if row and row[0] in allowed:
            cand[cid] = row

    # Phase B：標籤對焦加權 — 來源 entities 命中 boost_entities 的 chunk 加分
    matched_map = {}
    if boost_set:
        for cid, (sid, _txt) in cand.items():
            ents = src_meta[sid]["entities"]
            hit = [e for e in ents if _norm(e) in boost_set]
            if hit:
                fused[cid] = fused.get(cid, 0.0) + 0.02 * len(hit)  # 約等於額外一筆 rank-1 命中
                matched_map[cid] = hit

    ids_sorted = sorted(cand.keys(), key=lambda c: fused.get(c, 0.0), reverse=True)
    out = []
    for cid in ids_sorted:
        sid, txt = cand[cid]
        m = src_meta[sid]
        out.append({"chunk_id": cid, "source_id": sid, "source_name": m["name"],
                    "type": m["type"], "reliability": m["reliability"],
                    "entities": m["entities"], "matched": matched_map.get(cid, []),
                    "text": txt, "score": round(fused.get(cid, 0.0), 4)})
        if len(out) >= top_k:
            break
    con.close()
    return out

def _kb_extract_pdf(raw: bytes) -> str:
    """從 PDF bytes 抽純文字。"""
    try:
        import io
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(raw))
        parts = []
        for pg in reader.pages:
            t = pg.extract_text() or ""
            if t.strip():
                parts.append(t)
        return "\n".join(parts).strip()
    except Exception as e:
        return f"[PDF 解析失敗：{str(e)[:120]}]"

# ── 投資風格設定 ──────────────────────────────────

_IC_DEFAULTS = {
    "preferred_indicators": json.dumps(["KD", "MACD", "MA", "VOL"]),
    "holding_period":       "波段",
    "risk_level":           "穩健",
    "tw_sectors_focus":     json.dumps([]),
    "us_sectors_focus":     json.dumps([]),
    "claude_api_key":       "",
    "auto_refresh_minutes": "30",
    "recommendation_count": "10",
    "custom_sources":       json.dumps([]),
    # per-function AI model selection
    "model_stock_analyze":  "claude-sonnet-4-6",   # 單支股票深度分析
    "model_rec_scan":       "claude-haiku-4-5-20251001",    # 批次推薦掃描
    "model_batch_score":    "claude-haiku-4-5-20251001",    # 批次評分
    "model_macro_ai":       "claude-haiku-4-5-20251001",    # 總經 AI 解讀
    "model_sentiment":      "claude-haiku-4-5-20251001",    # 新聞情緒分析
    # per-function AI 來源：'api'=Anthropic API Key 計費 / 'subscription'=本機 claude CLI 訂閱
    "source_stock_analyze": "api",
    "source_rec_scan":      "api",
    "source_batch_score":   "api",
    "source_macro_ai":      "api",
    "source_sentiment":     "api",
    "claude_cli_path":      "claude",   # 訂閱模式呼叫的 claude CLI 路徑
}

def _ic_get_settings() -> dict:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT key, value FROM ic_settings")
    rows = cur.fetchall()
    con.close()
    s = dict(_IC_DEFAULTS)
    for k, v in rows:
        s[k] = v
    return s

def _ic_save_settings(data: dict):
    con = db()
    for k, v in data.items():
        if k in _IC_DEFAULTS:
            con.execute(
                "INSERT INTO ic_settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=?",
                (k, str(v), str(v))
            )
    con.commit()
    con.close()

@app.get("/api/ic/settings")
def ic_get_settings_route():
    s = _ic_get_settings()
    if s.get("claude_api_key"):
        s["claude_api_key"] = "***"  # redact; use POST to update
    for k in ["preferred_indicators", "tw_sectors_focus", "us_sectors_focus", "custom_sources"]:
        try:
            s[k] = json.loads(s[k])
        except Exception:
            s[k] = []
    return s

@app.get("/api/ic/token-usage")
def ic_token_usage():
    con = db()
    con.execute("""CREATE TABLE IF NOT EXISTS ic_token_usage(
        date TEXT, model TEXT, tokens INTEGER, cost REAL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
    today = datetime.now().strftime('%Y-%m-%d')
    month = today[:7]
    row_today = con.execute("SELECT COALESCE(SUM(tokens),0), COALESCE(SUM(cost),0) FROM ic_token_usage WHERE date=?", (today,)).fetchone()
    row_month = con.execute("SELECT COALESCE(SUM(tokens),0), COALESCE(SUM(cost),0) FROM ic_token_usage WHERE date LIKE ?", (month+'%',)).fetchone()
    con.close()
    return {
        "today_tokens": row_today[0], "today_cost": row_today[1],
        "month_tokens": row_month[0], "month_cost": row_month[1],
    }

@app.post("/api/ic/settings")
def ic_save_settings_route(data: dict, _: None = Depends(require_token)):
    for k in ["preferred_indicators", "tw_sectors_focus", "us_sectors_focus", "custom_sources"]:
        if k in data and isinstance(data[k], list):
            data[k] = json.dumps(data[k])
    _ic_save_settings(data)
    return {"ok": True}

# ── 總經數據（含快取）────────────────────────────────

_IC_MACRO_CACHE: dict = {}
_IC_MACRO_TS: float = 0.0
_IC_MACRO_TTL = 900  # 15 min

_IC_MACRO_SYMBOLS = {
    "VIX":    "^VIX",
    "DXY":    "DX-Y.NYB",
    "US10Y":  "^TNX",
    "US2Y":   "^IRX",
    "WTI":    "CL=F",
    "BRENT":  "BZ=F",
    "GOLD":   "GC=F",
    "SILVER": "SI=F",
    "COPPER": "HG=F",
    "SPX":    "^GSPC",
    "NDX":    "^IXIC",
    "DJI":    "^DJI",
    "SOX":    "^SOX",
    "TWD":    "USDTWD=X",
    "JPY":    "USDJPY=X",
    "EUR":    "EURUSD=X",
    "CNY":    "USDCNY=X",
}

def _fetch_macro_data(force: bool = False) -> dict:
    global _IC_MACRO_CACHE, _IC_MACRO_TS
    if not force and time.time() - _IC_MACRO_TS < _IC_MACRO_TTL and _IC_MACRO_CACHE:
        return _IC_MACRO_CACHE
    try:
        import yfinance as yf
    except ImportError:
        return {}

    result = {}
    try:
        tickers = yf.Tickers(" ".join(_IC_MACRO_SYMBOLS.values()))
        for label, sym in _IC_MACRO_SYMBOLS.items():
            try:
                hist = tickers.tickers[sym].history(period="5d", interval="1d")
                if hist.empty:
                    continue
                closes = list(hist["Close"])
                price = closes[-1]
                prev  = closes[-2] if len(closes) >= 2 else price
                chg   = price - prev
                chg_pct = chg / prev * 100 if prev else 0
                _UNITS = {"GOLD": "USD/oz (期貨GC=F)", "SILVER": "USD/oz (期貨)", "WTI": "USD/bbl (期貨)", "BRENT": "USD/bbl (期貨)", "COPPER": "USD/lb (期貨)"}
                result[label] = {
                    "price":      round(price, 4),
                    "change":     round(chg, 4),
                    "change_pct": round(chg_pct, 2),
                    "symbol":     sym,
                    "unit":       _UNITS.get(label, ""),
                }
            except Exception:
                pass
    except Exception:
        pass

    _IC_MACRO_CACHE = result
    _IC_MACRO_TS = time.time()
    return result

@app.get("/api/ic/macro")
def ic_macro():
    return _fetch_macro_data()

@app.post("/api/ic/macro/refresh")
def ic_macro_refresh(_: None = Depends(require_token)):
    return _fetch_macro_data(force=True)

# ── 美股板塊 ETF ──────────────────────────────────

US_SECTOR_ETFS = {
    "科技":    "XLK",
    "金融":    "XLF",
    "能源":    "XLE",
    "醫療":    "XLV",
    "工業":    "XLI",
    "消費選擇": "XLY",
    "民生消費": "XLP",
    "原材料":  "XLB",
    "房地產":  "XLRE",
    "公用事業": "XLU",
    "通訊":    "XLC",
}

_IC_US_SECTOR_CACHE: list = []
_IC_US_SECTOR_TS: float = 0.0
_IC_US_SECTOR_TTL = 1800  # 30 min

def _fetch_us_sectors(force: bool = False) -> list:
    global _IC_US_SECTOR_CACHE, _IC_US_SECTOR_TS
    if not force and time.time() - _IC_US_SECTOR_TS < _IC_US_SECTOR_TTL and _IC_US_SECTOR_CACHE:
        return _IC_US_SECTOR_CACHE
    try:
        import yfinance as yf
    except ImportError:
        return []

    result = []
    try:
        tickers = yf.Tickers(" ".join(US_SECTOR_ETFS.values()))
        for sector, sym in US_SECTOR_ETFS.items():
            try:
                hist = tickers.tickers[sym].history(period="6mo", interval="1d")
                if hist.empty:
                    continue
                closes = list(hist["Close"])
                price  = closes[-1]
                prev   = closes[-2] if len(closes) >= 2 else price
                ma20   = sum(closes[-20:]) / 20 if len(closes) >= 20 else price
                result.append({
                    "sector":      sector,
                    "symbol":      sym,
                    "price":       round(price, 2),
                    "change_pct":  round((price - prev) / prev * 100 if prev else 0, 2),
                    "m1_pct":      round((price / closes[-22] - 1) * 100 if len(closes) >= 22 else 0, 2),
                    "m3_pct":      round((price / closes[-66] - 1) * 100 if len(closes) >= 66 else 0, 2),
                    "above_ma20":  price > ma20,
                })
            except Exception:
                pass
    except Exception:
        pass

    result.sort(key=lambda x: x.get("m1_pct", 0), reverse=True)
    _IC_US_SECTOR_CACHE = result
    _IC_US_SECTOR_TS = time.time()
    return result

@app.get("/api/ic/us/sectors")
def ic_us_sectors():
    return _fetch_us_sectors()

# ── Phase 1: 相對強弱 benchmark 快取 ─────────────────
_RS_BENCH_CACHE: dict = {}
_RS_BENCH_TS: float = 0.0
_RS_BENCH_TTL = 1800

def _get_benchmark_closes(market: str) -> list:
    """取 benchmark 收盤價（US=SPY, TW=^TWII），30min 快取"""
    global _RS_BENCH_CACHE, _RS_BENCH_TS
    key = market
    if time.time() - _RS_BENCH_TS < _RS_BENCH_TTL and key in _RS_BENCH_CACHE:
        return _RS_BENCH_CACHE[key]
    if market == "US":
        try:
            import yfinance as yf
            hist = yf.Ticker("SPY").history(period="1y", interval="1d")
            closes = list(hist["Close"]) if not hist.empty else []
        except Exception:
            closes = []
    else:
        closes = _get_closes_from_cache("TAIEX", "D", 300)
        if not closes:
            try:
                import yfinance as yf
                hist = yf.Ticker("^TWII").history(period="1y", interval="1d")
                closes = list(hist["Close"]) if not hist.empty else []
            except Exception:
                closes = []
    _RS_BENCH_CACHE[key] = closes
    _RS_BENCH_TS = time.time()
    return closes

# ── base-rate beta 濾網用：回測全窗 {date: close} 對照表 (R4) ──
# 公平基準（§三E）：逐筆超額可選 ^TWII(市值加權,預設) / 等權 universe / 0050，
# 脫離市值加權 ^TWII 的台積電(>30%權重)偏誤，回答「中小型策略是不是被偏誤尺冤殺」。
_BENCH_MAP_CACHE: dict = {}    # key = "<sym>:<start>:<end>" → {date: close}
_BENCH_MAP_TS: dict = {}
_BENCH_MAP_TTL = 1800  # 30min，沿用 RS benchmark 快取週期
_EW_MIN_STOCKS = 10    # 等權基準：當日有效股 < 此數則該日報酬視為 0（暖身/稀疏防呆，§三E）
_BENCHMARK_WHITELIST = ("twii", "equal_weight", "0050")  # run_base_rate benchmark 白名單

def _fetch_index_close_map(sym: str, start: str, end: str) -> dict:
    """抓任一指數/ETF 的 {date(YYYY-MM-DD): close} 字典（auto_adjust=False 取「價格收盤」，
    與個股價格報酬對齊、不混入指數股息）。30min 快取，key=sym:start:end。
    供公平基準 0050 與既有 ^TWII/SPY 共用同一條取數路徑。"""
    key = f"{sym}:{start}:{end}"
    now = time.time()
    if key in _BENCH_MAP_CACHE and now - _BENCH_MAP_TS.get(key, 0) < _BENCH_MAP_TTL:
        return _BENCH_MAP_CACHE[key]
    out = {}
    try:
        import yfinance as yf
        hist = yf.Ticker(sym).history(start=start, end=end, interval="1d", auto_adjust=False)
        if not hist.empty and "Close" in hist:
            for idx, cl in hist["Close"].items():
                if cl is None or cl != cl:
                    continue
                out[idx.strftime("%Y-%m-%d")] = float(cl)
    except Exception as e:
        print(f"[base-rate] index map fetch failed ({sym} {start}~{end}): {e}", flush=True)
        out = {}
    _BENCH_MAP_CACHE[key] = out
    _BENCH_MAP_TS[key] = now
    print(f"[base-rate] index map {sym}: {len(out)} days ({start}~{end})", flush=True)
    return out

def _get_benchmark_close_map(market: str, start: str, end: str) -> dict:
    """回測全窗的 {date(YYYY-MM-DD): close} 字典（US=SPY, TW=^TWII），供 base-rate
    逐筆超額對齊持有期 + 橫斷面相對/殘差因子。用「價格收盤」與個股價格報酬對齊（公平比
    曝險期間 alpha vs beta，不混入指數股息）。預設市值加權基準，行為與既往一致（零回歸）。"""
    sym = "SPY" if market == "US" else "^TWII"
    return _fetch_index_close_map(sym, start, end)

def _equal_weight_bench_map(codes: list, all_dates: list, bar_data: dict) -> dict:
    """等權 universe 公平基準（§三E，脫離市值加權 ^TWII 的台積電偏誤）：每個交易日對
    「當日與前一交易日皆有報價」的個股取『等權平均日報酬』（每日重平衡），累乘成淨值序列。
    回傳 {date: 累積淨值}，與 _get_benchmark_close_map 同口徑（價格報酬、gross，逐筆超額
    用 NAV[exit]/NAV[entry]-1）。零新數據源：純由既有載入的 bar_data 計算。
    防呆：close 為 None/NaN/≤0 不計入當日；當日有效股 < _EW_MIN_STOCKS 則該日報酬視為 0
    （淨值持平，不以稀疏暖身期污染基準）。"""
    if not all_dates:
        return {}
    level = 1.0
    nav = {all_dates[0]: level}
    prev = all_dates[0]
    for d in all_dates[1:]:
        rets = []
        for code in codes:
            b0 = bar_data.get((code, prev))
            b1 = bar_data.get((code, d))
            if not b0 or not b1:
                continue
            c0 = b0.get("close"); c1 = b1.get("close")
            if c0 is None or c1 is None or c0 != c0 or c1 != c1 or c0 <= 0 or c1 <= 0:
                continue
            rets.append(c1 / c0 - 1.0)
        if len(rets) >= _EW_MIN_STOCKS:
            level *= (1.0 + sum(rets) / len(rets))   # 等權日報酬累乘
        nav[d] = level
        prev = d
    return nav

def _benchmark_label(benchmark: str) -> str:
    """公平基準的顯示標籤（job 層級，metaBar/矩陣標題用）。"""
    return {
        "equal_weight": "等權 universe（每日重平衡）",
        "0050": "0050 ETF（台股；美股退回 SPY）",
    }.get((benchmark or "twii").lower(), "市值加權（^TWII / SPY）")

def _calc_relative_strength(stock_closes: list, bench_closes: list) -> dict:
    """多週期相對強弱：1W(5日)/1M(21日)/3M(63日)"""
    rs = {}
    n = min(len(stock_closes), len(bench_closes))
    if n < 6:
        return {}
    sc = stock_closes[-n:]
    bc = bench_closes[-n:]
    for label, days in [("1W", 5), ("1M", 21), ("3M", 63)]:
        if n > days:
            s_ret = (sc[-1] / sc[-days-1] - 1) * 100
            b_ret = (bc[-1] / bc[-days-1] - 1) * 100
            rs[label] = round(s_ret - b_ret, 2)
    return rs

# ── Phase 1: OBV / MFI / 量價背離 ────────────────────
def _calc_obv(closes: list, volumes: list) -> dict:
    """OBV + 20日 OBV 趨勢方向"""
    if len(closes) < 22 or len(volumes) < 22:
        return {}
    n = min(len(closes), len(volumes))
    c, v = closes[-n:], volumes[-n:]
    obv = [0]
    for i in range(1, len(c)):
        if c[i] > c[i-1]:
            obv.append(obv[-1] + v[i])
        elif c[i] < c[i-1]:
            obv.append(obv[-1] - v[i])
        else:
            obv.append(obv[-1])
    obv_now = obv[-1]
    obv_20  = obv[-21] if len(obv) > 20 else obv[0]
    trend   = "up" if obv_now > obv_20 else ("down" if obv_now < obv_20 else "flat")
    return {"obv": obv_now, "obv_trend": trend}

def _calc_mfi(highs: list, lows: list, closes: list, volumes: list, period: int = 14) -> dict:
    """Money Flow Index (0~100)"""
    n = min(len(highs), len(lows), len(closes), len(volumes))
    if n < period + 2:
        return {}
    tp = [(highs[-n+i] + lows[-n+i] + closes[-n+i]) / 3 for i in range(n)]
    pos_flow = 0.0
    neg_flow = 0.0
    for i in range(n - period, n):
        mf = tp[i] * volumes[-n+i]
        if tp[i] > tp[i-1]:
            pos_flow += mf
        elif tp[i] < tp[i-1]:
            neg_flow += mf
    if neg_flow == 0:
        mfi = 100.0
    else:
        mfi = 100 - 100 / (1 + pos_flow / neg_flow)
    return {"mfi": round(mfi, 1)}

def _detect_volume_price_divergence(closes: list, volumes: list, window: int = 10) -> str:
    """偵測量價背離：價漲量縮=頂背離, 價跌量縮=底背離"""
    if len(closes) < window + 1 or len(volumes) < window + 1:
        return ""
    price_chg = closes[-1] / closes[-window-1] - 1
    vol_avg_recent = sum(volumes[-window:]) / window
    vol_avg_prev   = sum(volumes[-2*window:-window]) / window if len(volumes) >= 2*window else vol_avg_recent
    vol_chg = vol_avg_recent / vol_avg_prev - 1 if vol_avg_prev > 0 else 0
    if price_chg > 0.03 and vol_chg < -0.15:
        return "top_divergence"
    if price_chg < -0.03 and vol_chg < -0.15:
        return "bottom_divergence"
    return ""

# ── Phase 1: 情緒動量追蹤 ────────────────────────────
def _ic_ensure_sentiment_table():
    con = db()
    con.execute("""CREATE TABLE IF NOT EXISTS ic_sentiment_history(
        code TEXT NOT NULL,
        market TEXT DEFAULT 'TW',
        date TEXT NOT NULL,
        score REAL,
        direction TEXT,
        confidence REAL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(code, date)
    )""")
    # R-SENT 歷史快照：補欄位（冪等，舊 DB 自動 migrate）
    cols = {r[1] for r in con.execute("PRAGMA table_info(ic_sentiment_history)").fetchall()}
    if "source" not in cols:
        con.execute("ALTER TABLE ic_sentiment_history ADD COLUMN source TEXT DEFAULT 'ic_scan'")
    if "trend" not in cols:
        con.execute("ALTER TABLE ic_sentiment_history ADD COLUMN trend TEXT")
    con.execute("CREATE INDEX IF NOT EXISTS idx_sent_hist_date ON ic_sentiment_history(date)")
    con.commit()
    con.close()

def _ic_record_sentiment(code: str, market: str, score: float, direction: str, confidence: float):
    _ic_ensure_sentiment_table()
    today = datetime.now().strftime("%Y-%m-%d")
    con = db()
    con.execute(
        "INSERT OR REPLACE INTO ic_sentiment_history(code,market,date,score,direction,confidence) VALUES(?,?,?,?,?,?)",
        (code, market, today, score, direction, confidence)
    )
    con.commit()
    con.close()

def _ic_get_sentiment_momentum(code: str, lookback: int = 7) -> dict:
    """取最近 N 次情緒紀錄，計算動量（趨勢變化率）"""
    _ic_ensure_sentiment_table()
    con = db()
    rows = con.execute(
        "SELECT date, score, direction, confidence FROM ic_sentiment_history "
        "WHERE code=? ORDER BY date DESC LIMIT ?",
        (code, lookback)
    ).fetchall()
    con.close()
    if len(rows) < 2:
        return {}
    rows = list(reversed(rows))
    scores = [r[1] for r in rows if r[1] is not None]
    if len(scores) < 2:
        return {}
    delta = scores[-1] - scores[0]
    avg_delta = delta / (len(scores) - 1)
    trend = "improving" if avg_delta > 2 else ("deteriorating" if avg_delta < -2 else "stable")
    return {
        "latest_score": round(scores[-1], 1),
        "prev_score": round(scores[0], 1),
        "delta": round(delta, 1),
        "avg_delta": round(avg_delta, 2),
        "trend": trend,
        "data_points": len(scores),
        "history": [{"date": r[0], "score": r[1], "direction": r[2]} for r in rows],
    }

# ── R-SENT：情緒分數每日落地（純資料管線，與策略脫鉤）──────────
def _snapshot_sentiment_daily() -> dict:
    """每日情緒快照：讀全 watchlist + 持倉個股的 live 情緒分數（_ic_score_stock），
    upsert 進 ic_sentiment_history（PK=(code,date)，同日重跑覆蓋當日，冪等）。
    回傳寫入統計。純資料：不碰 base-rate / 策略邏輯。
    A7 情緒逆向回測之前置——每晚落地，未來才有歷史時間序列可餵。"""
    _ic_ensure_sentiment_table()
    today = datetime.now().strftime("%Y-%m-%d")

    # 收集 watchlist + 持倉（去重；market 缺漏時用 _detect_market 推斷）
    con = db(); cur = con.cursor()
    cur.execute("SELECT DISTINCT code, name, market FROM watchlist")
    wl = cur.fetchall()
    cur.execute("SELECT DISTINCT code, name, market FROM positions WHERE status='open' OR status IS NULL")
    pos = cur.fetchall()
    con.close()

    seen = {}
    for c, n, m in wl:
        if c:
            seen[c] = (c, n, m or _detect_market(c))
    for c, n, m in pos:
        if c and c not in seen:
            seen[c] = (c, n, _detect_market(c))

    try:
        macro = _fetch_macro_data()
    except Exception:
        macro = {}

    written, skipped, failed = 0, 0, 0
    errors = []
    for code, name, mkt in seen.values():
        try:
            tech = _ic_score_stock(code, mkt)
            if not tech:
                skipped += 1            # 資料不足（<30 根 K）→ 略過，不寫空值
                continue
            score     = tech.get("score")
            direction = tech.get("direction", "")
            try:
                sources    = _ic_detect_sources(code, mkt)
                confidence = _ic_calc_confidence(tech, sources, macro)
            except Exception:
                confidence = None
            # trend：以既有歷史動量計（近似，含/不含今日皆可接受，僅為便利欄位）
            try:
                mom   = _ic_get_sentiment_momentum(code, lookback=7)
                trend = mom.get("trend", "") if mom else ""
            except Exception:
                trend = ""
            con2 = db()
            con2.execute(
                "INSERT OR REPLACE INTO ic_sentiment_history"
                "(code,market,date,score,direction,confidence,source,trend) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (code, mkt, today, score, direction, confidence, "daily_snapshot", trend),
            )
            con2.commit(); con2.close()
            written += 1
        except Exception as e:
            failed += 1
            if len(errors) < 10:
                errors.append(f"{code}: {e}")

    result = {"date": today, "total": len(seen),
              "written": written, "skipped": skipped, "failed": failed}
    if errors:
        result["errors"] = errors
    print(f"[情緒快照] {today} 寫入 {written}/{len(seen)} (skip {skipped}, fail {failed})")
    return result


_sentiment_snapshot_next: str = ""

def _sentiment_snapshot_scheduler_loop():
    """每交易日 14:30（TW 收盤後、rec 掃描完）落地全 watchlist+持倉的情緒分數歷史。
    可由 risk_config key='sentiment_snapshot_enabled' 設 '0' 關閉。"""
    global _sentiment_snapshot_next
    time.sleep(150)  # 啟動後稍候，待其他初始化完成
    while True:
        try:
            con = db(); cur = con.cursor()
            cur.execute("SELECT value FROM risk_config WHERE key='sentiment_snapshot_enabled'")
            row = cur.fetchone(); con.close()
            if row and row[0] == "0":
                time.sleep(600); continue
        except Exception:
            time.sleep(300); continue
        try:
            now = datetime.now()
            target = now.replace(hour=14, minute=30, second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)
            while target.weekday() >= 5:          # 跳過週六/日
                target += timedelta(days=1)
            _sentiment_snapshot_next = target.strftime("%Y-%m-%d %H:%M")
            sleep_secs = max(1, (target - datetime.now()).total_seconds())
            time.sleep(sleep_secs)
            if datetime.now().weekday() >= 5:
                continue
            res = _snapshot_sentiment_daily()
            print(f"[情緒快照排程] 完成：{res}")
        except Exception as e:
            print(f"[情緒快照排程] 失敗: {e}")
            time.sleep(300)

threading.Thread(target=_sentiment_snapshot_scheduler_loop,
                 daemon=True, name="sentiment-snapshot-scheduler").start()

@app.post("/api/sentiment/snapshot")
def sentiment_snapshot_manual(_: None = Depends(require_token)):
    """手動觸發一次每日情緒快照落地（require_token）。冪等：同日重跑覆蓋當日。
    供 cockpit 今晚立即落地一次驗證用。"""
    res = _snapshot_sentiment_daily()
    return {"ok": True, **res}

@app.get("/api/sentiment/snapshot-status")
def sentiment_snapshot_status():
    """情緒快照排程狀態 + 已落地統計（無需 token，供監看）。"""
    _ic_ensure_sentiment_table()
    con = db(); cur = con.cursor()
    cur.execute("SELECT value FROM risk_config WHERE key='sentiment_snapshot_enabled'")
    row = cur.fetchone()
    cur.execute("SELECT COUNT(*), COUNT(DISTINCT code), MIN(date), MAX(date) "
                "FROM ic_sentiment_history WHERE source='daily_snapshot'")
    cnt, codes, mn, mx = cur.fetchone()
    con.close()
    return {
        "enabled": (row[0] if row else "1") == "1",
        "next_run": _sentiment_snapshot_next,
        "snapshot_rows": cnt or 0,
        "distinct_codes": codes or 0,
        "earliest_date": mn,
        "latest_date": mx,
    }

# ── 基本面數據 ────────────────────────────────────

_fund_cache: dict = {}
_fund_cache_ts: dict = {}

def _get_fundamentals(code: str, market: str) -> dict:
    """取 PE/PB/ROE/EPS/殖利率，快取 6 小時"""
    cache_key = f"{code}_{market}"
    if cache_key in _fund_cache and (time.time() - _fund_cache_ts.get(cache_key, 0)) < 21600:
        return _fund_cache[cache_key]
    try:
        import yfinance as yf
        ticker = code if market == "US" else f"{code}.TW"
        info = yf.Ticker(ticker).info or {}
        fund = {}
        if info.get("trailingPE"):  fund["pe"] = round(info["trailingPE"], 2)
        if info.get("forwardPE"):   fund["fwd_pe"] = round(info["forwardPE"], 2)
        if info.get("priceToBook"): fund["pb"] = round(info["priceToBook"], 2)
        if info.get("returnOnEquity"): fund["roe"] = round(info["returnOnEquity"] * 100, 1)
        if info.get("trailingEps"): fund["eps"] = round(info["trailingEps"], 2)
        raw_dy = info.get("trailingAnnualDividendYield") or info.get("dividendYield")
        if raw_dy:
            dy_pct = raw_dy * 100 if raw_dy < 1 else raw_dy
            fund["dy"] = round(dy_pct, 2)
        if info.get("marketCap"):   fund["mkt_cap"] = info["marketCap"]
        if info.get("sector"):      fund["sector"] = info["sector"]
        if info.get("industry"):    fund["industry"] = info["industry"]
        _fund_cache[cache_key] = fund
        _fund_cache_ts[cache_key] = time.time()
        return fund
    except Exception:
        return {}

# ── P3: Sector Rotation 板塊輪動 ──────────────────

GICS_SECTORS = {
    "XLK": "科技", "XLF": "金融", "XLV": "醫療", "XLE": "能源",
    "XLI": "工業", "XLY": "非必需消費", "XLP": "必需消費", "XLU": "公用事業",
    "XLB": "原物料", "XLRE": "房地產", "XLC": "通訊",
}
_sector_cache: dict = {}
_sector_cache_ts: float = 0

def _get_sector_rotation() -> list:
    """取 GICS 11 大板塊 ETF 的 1W/1M/3M 表現，快取 1 小時"""
    global _sector_cache, _sector_cache_ts
    if _sector_cache and (time.time() - _sector_cache_ts) < 3600:
        return _sector_cache
    try:
        import yfinance as yf
        tickers = list(GICS_SECTORS.keys()) + ["SPY"]
        data = yf.download(tickers, period="4mo", interval="1d", group_by="ticker", progress=False, threads=True)
        spy_closes = data["SPY"]["Close"].dropna()
        result = []
        for etf, name_zh in GICS_SECTORS.items():
            try:
                closes = data[etf]["Close"].dropna()
                if len(closes) < 10:
                    continue
                cur = float(closes.iloc[-1])
                pct_1w = (cur / float(closes.iloc[-5]) - 1) * 100 if len(closes) >= 5 else 0
                pct_1m = (cur / float(closes.iloc[-21]) - 1) * 100 if len(closes) >= 21 else 0
                pct_3m = (cur / float(closes.iloc[-63]) - 1) * 100 if len(closes) >= 63 else 0
                # Relative vs SPY
                spy_1m = (float(spy_closes.iloc[-1]) / float(spy_closes.iloc[-21]) - 1) * 100 if len(spy_closes) >= 21 else 0
                rs_1m = pct_1m - spy_1m
                result.append({
                    "etf": etf, "name": name_zh, "price": round(cur, 2),
                    "pct_1w": round(pct_1w, 2), "pct_1m": round(pct_1m, 2), "pct_3m": round(pct_3m, 2),
                    "rs_vs_spy": round(rs_1m, 2),
                })
            except Exception:
                continue
        result.sort(key=lambda x: x["pct_1m"], reverse=True)
        for i, r in enumerate(result):
            r["rank"] = i + 1
            r["momentum"] = "強勢" if r["rs_vs_spy"] > 2 else ("弱勢" if r["rs_vs_spy"] < -2 else "中性")
        _sector_cache = result
        _sector_cache_ts = time.time()
        return result
    except Exception:
        return []

def _get_stock_sector(code: str, market: str) -> dict:
    """取個股所屬板塊及該板塊輪動排名"""
    fund = _get_fundamentals(code, market)
    sector_en = fund.get("sector", "")
    if not sector_en:
        return {}
    sector_map = {
        "Technology": "XLK", "Financial Services": "XLF", "Healthcare": "XLV",
        "Energy": "XLE", "Industrials": "XLI", "Consumer Cyclical": "XLY",
        "Consumer Defensive": "XLP", "Utilities": "XLU", "Basic Materials": "XLB",
        "Real Estate": "XLRE", "Communication Services": "XLC",
    }
    etf = sector_map.get(sector_en, "")
    rotation = _get_sector_rotation()
    matched = next((r for r in rotation if r["etf"] == etf), None)
    return {"sector": sector_en, "etf": etf, "rotation": matched} if matched else {"sector": sector_en}

@app.get("/api/ic/sector-rotation")
def ic_sector_rotation():
    return {"sectors": _get_sector_rotation()}

# ── P4: Event-Driven 事件驅動 ──────────────────

_event_cache: dict = {}
_event_cache_ts: dict = {}

def _get_events(code: str, market: str) -> dict:
    """取個股事件：財報日期、除息日、近期新聞標題。快取 6 小時"""
    cache_key = f"{code}_{market}"
    if cache_key in _event_cache and (time.time() - _event_cache_ts.get(cache_key, 0)) < 21600:
        return _event_cache[cache_key]
    events = {"earnings": None, "ex_dividend": None, "news": []}
    try:
        import yfinance as yf
        ticker_str = code if market == "US" else f"{code}.TW"
        tk = yf.Ticker(ticker_str)
        info = tk.info or {}
        # 財報日期
        from datetime import datetime, timedelta
        cal = None
        try:
            cal = tk.calendar
        except Exception:
            pass
        if cal is not None:
            if isinstance(cal, dict):
                ed = cal.get("Earnings Date")
                if ed:
                    if isinstance(ed, list) and len(ed) > 0:
                        events["earnings"] = str(ed[0])[:10]
                    elif hasattr(ed, 'strftime'):
                        events["earnings"] = ed.strftime("%Y-%m-%d")
                exd = cal.get("Ex-Dividend Date")
                if exd:
                    events["ex_dividend"] = str(exd)[:10] if not hasattr(exd, 'strftime') else exd.strftime("%Y-%m-%d")
            elif hasattr(cal, 'columns'):
                try:
                    if "Earnings Date" in cal.index:
                        ed_val = cal.loc["Earnings Date"]
                        if hasattr(ed_val, 'iloc'):
                            events["earnings"] = str(ed_val.iloc[0])[:10]
                        else:
                            events["earnings"] = str(ed_val)[:10]
                except Exception:
                    pass
        # 近期新聞
        try:
            news_list = tk.news or []
            now_ts = time.time()
            for n in news_list[:5]:
                title = n.get("title", "")
                pub = n.get("providerPublishTime", 0)
                age_days = (now_ts - pub) / 86400 if pub else 99
                if title and age_days < 14:
                    events["news"].append({
                        "title": title,
                        "publisher": n.get("publisher", ""),
                        "age_days": round(age_days, 1),
                        "link": n.get("link", ""),
                    })
        except Exception:
            pass
        # 事件標籤
        tags = []
        if events["earnings"]:
            try:
                ed = datetime.strptime(events["earnings"], "%Y-%m-%d")
                days_to = (ed - datetime.now()).days
                if 0 <= days_to <= 14:
                    tags.append(f"財報將於{days_to}天後公布")
                elif -3 <= days_to < 0:
                    tags.append("剛公布財報")
            except Exception:
                pass
        if events["ex_dividend"]:
            try:
                exd = datetime.strptime(events["ex_dividend"], "%Y-%m-%d")
                days_to = (exd - datetime.now()).days
                if 0 <= days_to <= 14:
                    tags.append(f"除息日{days_to}天後")
            except Exception:
                pass
        # 新聞情緒粗判
        neg_kw = ["downgrade", "lawsuit", "recall", "investigation", "fraud", "decline", "cut", "warning",
                   "下修", "訴訟", "召回", "調查", "下調", "警告", "虧損", "裁員"]
        pos_kw = ["upgrade", "beat", "record", "growth", "approval", "上修", "突破", "成長", "核准", "獲利"]
        for n in events.get("news", []):
            t_low = n["title"].lower()
            if any(k in t_low for k in neg_kw):
                tags.append(f"負面新聞: {n['title'][:30]}")
            elif any(k in t_low for k in pos_kw):
                tags.append(f"正面新聞: {n['title'][:30]}")
        events["tags"] = tags
        _event_cache[cache_key] = events
        _event_cache_ts[cache_key] = time.time()
    except Exception:
        pass
    return events

@app.get("/api/ic/events/{code}")
def ic_events(code: str, market: str = "US"):
    return _get_events(code.upper(), market.upper())

# ── 技術面評分引擎（Phase 1 強化版）──────────────────

def _ic_score_stock(code: str, market: str = "TW") -> dict:
    """計算技術面分數 0-100，含 RS/OBV/MFI/量價背離。"""
    ohlcv = _get_ohlcv_from_cache(code, 300, market)
    if not ohlcv or not ohlcv.get("closes") or len(ohlcv["closes"]) < 30:
        return {}

    closes  = ohlcv["closes"]
    volumes = ohlcv["volumes"]
    highs   = ohlcv["highs"]
    lows    = ohlcv["lows"]

    s      = pd.Series(closes)
    score  = 0
    sigs   = []
    detail = {}

    # KD (Stochastic 9)
    if len(closes) >= 12:
        low_n  = s.rolling(9).min()
        high_n = s.rolling(9).max()
        rsv = (s - low_n) / (high_n - low_n).replace(0, 1) * 100
        k = rsv.ewm(com=2).mean()
        d = k.ewm(com=2).mean()
        kv, dv = float(k.iloc[-1]), float(d.iloc[-1])
        kp, dp = float(k.iloc[-2]), float(d.iloc[-2])
        detail["KD"] = {"K": round(kv, 1), "D": round(dv, 1)}
        if kp < dp and kv > dv and kv < 80:
            score += 20; sigs.append("KD金叉")
        elif kv > 80:
            score -= 10; sigs.append("KD超買")
        elif kv < 20:
            score += 10; sigs.append("KD超賣低接")

    # MACD (12/26/9)
    if len(closes) >= 35:
        dif  = s.ewm(span=12).mean() - s.ewm(span=26).mean()
        macd = dif.ewm(span=9).mean()
        dv, mv = float(dif.iloc[-1]), float(macd.iloc[-1])
        dp, mp = float(dif.iloc[-2]), float(macd.iloc[-2])
        detail["MACD"] = {"DIF": round(dv, 3), "MACD": round(mv, 3), "above_zero": dv > 0}
        if dp < mp and dv >= mv:
            score += 20; sigs.append("MACD金叉")
        elif dv > 0 and mv > 0:
            score += 10; sigs.append("MACD多頭區")
        elif dp > mp and dv <= mv:
            score -= 20; sigs.append("MACD死叉")

    # 均線排列
    if len(closes) >= 60:
        ma5  = float(s.rolling(5).mean().iloc[-1])
        ma10 = float(s.rolling(10).mean().iloc[-1])
        ma20 = float(s.rolling(20).mean().iloc[-1])
        ma60 = float(s.rolling(60).mean().iloc[-1])
        price = closes[-1]
        detail["MA"] = {
            "MA5": round(ma5, 2), "MA10": round(ma10, 2),
            "MA20": round(ma20, 2), "MA60": round(ma60, 2),
        }
        if price > ma5 > ma10 > ma20 > ma60:
            score += 25; sigs.append("均線多頭排列")
        elif price > ma20:
            score += 10; sigs.append("站上MA20")
        else:
            score -= 10

    # 量比 (RVOL) + VWAP
    if volumes and len(volumes) >= 20:
        avg5  = sum(volumes[-5:]) / 5
        avg20 = sum(volumes[-20:]) / 20
        rvol  = volumes[-1] / avg20 if avg20 > 0 else 1.0
        rvol5 = avg5 / avg20 if avg20 > 0 else 1.0
        vol_detail = {"rvol": round(rvol, 2), "rvol5": round(rvol5, 2), "vol_today": volumes[-1], "vol_avg20": round(avg20, 0)}
        # VWAP（日K近似：典型價*量 / 累積量）
        tp = [(h + l + c) / 3 for h, l, c in zip(highs, lows, closes)]
        cum_tpv = sum(t * v for t, v in zip(tp[-20:], volumes[-20:]))
        cum_vol = sum(volumes[-20:])
        vwap_20 = round(cum_tpv / cum_vol, 2) if cum_vol > 0 else closes[-1]
        vwap_dist = round((closes[-1] / vwap_20 - 1) * 100, 2) if vwap_20 > 0 else 0
        vol_detail["vwap"] = vwap_20
        vol_detail["vwap_dist_pct"] = vwap_dist
        detail["VOL"] = vol_detail
        if rvol >= 1.5:
            score += 10; sigs.append(f"放量{rvol:.1f}x")
        elif rvol < 0.5:
            score -= 5;  sigs.append("量縮")
        if vwap_dist > 3:
            score += 5; sigs.append(f"價在VWAP上方+{vwap_dist:.1f}%")
        elif vwap_dist < -3:
            score -= 5; sigs.append(f"價在VWAP下方{vwap_dist:.1f}%")

    # RSI(14)
    if len(closes) >= 16:
        delta = s.diff()
        gain  = delta.clip(lower=0).rolling(14).mean()
        loss  = (-delta.clip(upper=0)).rolling(14).mean()
        rs    = gain / loss.replace(0, 0.001)
        rsi   = float((100 - 100 / (1 + rs)).iloc[-1])
        detail["RSI"] = {"value": round(rsi, 1)}
        if rsi < 30:
            score += 10; sigs.append(f"RSI超賣{rsi:.0f}")
        elif rsi > 70:
            score -= 10; sigs.append(f"RSI超買{rsi:.0f}")

    # ── 1A: 相對強弱 (Relative Strength vs Benchmark) ──
    bench = _get_benchmark_closes(market)
    rs_data = _calc_relative_strength(closes, bench) if bench else {}
    if rs_data:
        detail["RS"] = rs_data
        m1_rs = rs_data.get("1M", 0)
        if m1_rs > 5:
            score += 10; sigs.append(f"月強勢+{m1_rs:.1f}%")
        elif m1_rs < -5:
            score -= 10; sigs.append(f"月弱勢{m1_rs:.1f}%")

    # ── 1B: OBV 趨勢確認 ──
    obv_data = _calc_obv(closes, volumes)
    if obv_data:
        detail["OBV"] = obv_data
        if obv_data["obv_trend"] == "up" and score > 40:
            score += 5; sigs.append("OBV量能確認")
        elif obv_data["obv_trend"] == "down" and score < 60:
            score -= 5; sigs.append("OBV量能背離")

    # ── 1B: MFI 資金流量 ──
    mfi_data = _calc_mfi(highs, lows, closes, volumes)
    if mfi_data:
        detail["MFI"] = mfi_data
        mfi_val = mfi_data["mfi"]
        if mfi_val > 80:
            score -= 5; sigs.append(f"MFI超買{mfi_val:.0f}")
        elif mfi_val < 20:
            score += 5; sigs.append(f"MFI超賣{mfi_val:.0f}")

    # ── 1B: 量價背離偵測 ──
    div = _detect_volume_price_divergence(closes, volumes)
    if div:
        detail["divergence"] = div
        if div == "top_divergence":
            score -= 8; sigs.append("量價頂背離⚠")
        elif div == "bottom_divergence":
            score += 5; sigs.append("量價底背離")

    # ── 1C: 情緒動量 ──
    sent_m = _ic_get_sentiment_momentum(code)
    if sent_m:
        detail["sentiment_momentum"] = sent_m
        if sent_m["trend"] == "improving":
            score += 5; sigs.append(f"情緒改善Δ{sent_m['delta']:+.0f}")
        elif sent_m["trend"] == "deteriorating":
            score -= 5; sigs.append(f"情緒惡化Δ{sent_m['delta']:+.0f}")
        # P5: 情緒反轉 — 極端情緒逆向訊號
        ls = sent_m.get("latest_score")
        if ls is not None:
            if ls >= 85:
                score -= 4; sigs.append(f"⚠情緒過熱{ls:.0f}→逆向警示")
                detail["sentiment_reversal"] = {"type": "overbought", "score": ls}
            elif ls <= 15:
                score += 4; sigs.append(f"情緒冰點{ls:.0f}→逆向機會")
                detail["sentiment_reversal"] = {"type": "oversold", "score": ls}

    # ── P2: 基本面指標 (PE/PB/ROE/EPS/DY) ──
    fund = _get_fundamentals(code, market)
    if fund:
        detail["FUND"] = fund
        pe = fund.get("pe")
        if pe and 0 < pe < 15:
            score += 5; sigs.append(f"低PE{pe:.1f}")
        elif pe and pe > 40:
            score -= 5; sigs.append(f"高PE{pe:.0f}")
        dy = fund.get("dy")
        if dy and dy > 4:
            score += 3; sigs.append(f"高殖利率{dy:.1f}%")

    # P3: Sector Rotation
    sec_info = _get_stock_sector(code, market)
    if sec_info.get("rotation"):
        rot = sec_info["rotation"]
        detail["SECTOR"] = {"name": rot["name"], "etf": rot["etf"], "rank": rot["rank"],
                            "pct_1m": rot["pct_1m"], "rs_vs_spy": rot["rs_vs_spy"], "momentum": rot["momentum"]}
        if rot["rank"] <= 3:
            score += 5; sigs.append(f"板塊強勢#{rot['rank']}{rot['name']}")
        elif rot["rank"] >= 9:
            score -= 3; sigs.append(f"板塊弱勢#{rot['rank']}{rot['name']}")

    # P4: Event-Driven
    ev = _get_events(code, market)
    if ev and (ev.get("tags") or ev.get("news")):
        detail["EVENT"] = {"earnings": ev.get("earnings"), "ex_dividend": ev.get("ex_dividend"),
                           "tags": ev.get("tags", []), "news_count": len(ev.get("news", []))}
        for tag in ev.get("tags", []):
            if "正面新聞" in tag:
                score += 3; sigs.append(tag[:25])
            elif "負面新聞" in tag:
                score -= 3; sigs.append(tag[:25])
            elif "財報將於" in tag:
                sigs.append(tag)
            elif "除息日" in tag:
                sigs.append(tag)

    # P6: 多源情緒融合
    sentiment_sources = []
    sm = detail.get("sentiment_momentum", {})
    if sm and sm.get("latest_score") is not None:
        sentiment_sources.append(("AI情緒", sm["latest_score"], 0.5))
    ev_tags = detail.get("EVENT", {}).get("tags", [])
    news_score = 50
    for t in ev_tags:
        if "正面" in t: news_score += 15
        elif "負面" in t: news_score -= 15
    if news_score != 50:
        sentiment_sources.append(("新聞", max(0, min(100, news_score)), 0.3))
    tech_sent = 50 + (score - 0) * 0.5
    sentiment_sources.append(("技術面", max(0, min(100, tech_sent)), 0.2))
    if len(sentiment_sources) > 1:
        total_w = sum(w for _, _, w in sentiment_sources)
        composite = sum(s * w for _, s, w in sentiment_sources) / total_w if total_w > 0 else 50
        detail["SENTIMENT_COMPOSITE"] = {
            "score": round(composite, 1),
            "sources": [{"name": n, "score": round(s, 1), "weight": w} for n, s, w in sentiment_sources],
        }
        if composite >= 75:
            sigs.append(f"綜合情緒偏多{composite:.0f}")
        elif composite <= 25:
            sigs.append(f"綜合情緒偏空{composite:.0f}")

    # P9: Alpha 因子
    alpha = _calc_alpha_factors(closes, highs, lows, volumes)
    if alpha:
        detail["ALPHA"] = {k: v for k, v in alpha.items() if k in
            ("mom_5d","mom_20d","vol_ratio_5_20","bias_20d","price_pos_60d","vol_price_corr_20d","amplitude_20d")}
        pp = alpha.get("price_pos_60d", 0.5)
        if pp > 0.9:
            score -= 2; sigs.append(f"60日高檔{pp:.0%}")
        elif pp < 0.1:
            score += 2; sigs.append(f"60日低檔{pp:.0%}")

    score = max(0, min(100, score + 40))
    direction = "BUY" if score >= 62 else ("SELL" if score <= 38 else "HOLD")
    return {"score": score, "signals": sigs, "indicators": detail, "direction": direction,
            "price": round(closes[-1], 2)}

# ── Claude API 深度分析 ───────────────────────────

def _ic_detect_sources(code: str, market: str) -> list:
    """偵測本次分析實際用到哪些資料來源"""
    sources = []
    # 技術面來源
    if market == "US":
        sources.append({"id": "sys_yfinance", "name": "yfinance", "source_type": "system",
                        "detail": "美股日K線 / 成交量", "reliability": "reference"})
    else:
        has_kbar = bool(_get_closes_from_cache(code, "D", 5))
        sources.append({"id": "sys_shioaji" if has_kbar else "sys_yfinance",
                        "name": "Shioaji K線" if has_kbar else "yfinance",
                        "source_type": "system",
                        "detail": "台股日K線 / 成交量", "reliability": "confirmed" if has_kbar else "reference"})
    # 籌碼來源（TWSE）
    try:
        con = db(); cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM chip_snapshot WHERE code=?", (code,))
        if cur.fetchone()[0] > 0:
            sources.append({"id": "sys_twse", "name": "TWSE公開資料", "source_type": "system",
                            "detail": "法人買賣超 / 融資融券", "reliability": "confirmed"})
        con.close()
    except Exception:
        pass
    # 新聞來源
    try:
        con = db(); cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM news_cache WHERE code=?", (code,))
        if cur.fetchone()[0] > 0:
            sources.append({"id": "sys_news", "name": "新聞快取", "source_type": "system",
                            "detail": "個股相關新聞 / 情感", "reliability": "reference"})
        con.close()
    except Exception:
        pass
    # 使用者自訂來源（active）
    try:
        con = db(); cur = con.cursor()
        cur.execute(
            "SELECT id, name, url, description FROM ic_news_sources "
            "WHERE active=1 AND (market='ALL' OR market=?)",
            (market,),
        )
        for r in cur.fetchall():
            sources.append({"id": f"user_{r[0]}", "name": r[1], "source_type": "user",
                            "detail": r[3] or r[2], "reliability": "reference"})
        con.close()
    except Exception:
        pass
    return sources


def _ic_calc_confidence(tech: dict, sources: list, macro: dict, cal_row: dict = None) -> float:
    """計算 AI 信心度 0.0~0.82（永遠不會是 1.0）。

    cal_row=None → 沿用既有手調公式（零回歸：行為與改版前 byte-equal）。
    cal_row 提供（MVP 引擎複合桶命中）→ R-PROD-2：信心度 = 分條件回測勝率(地基)
      × 即時確認係數微調，套誠實上限（桶未過閘/N<30 → 上限 0.45）。"""
    score = tech.get("score", 50)
    n_confirmed = sum(1 for s in sources if s.get("reliability") == "confirmed")
    n_total     = max(len(sources), 1)
    base        = (score - 50) / 50           # -1.0 to +1.0
    src_bonus   = min(n_confirmed, 3) * 0.05  # 最多 +0.15
    vix         = (macro.get("VIX") or {}).get("price", 20)
    risk_pen    = 0.08 if vix > 25 else 0.0   # 高 VIX 降低信心
    conf = 0.50 + base * 0.28 + src_bonus - risk_pen
    conf = round(max(0.28, min(0.82, conf)), 2)
    if cal_row is None:
        return conf                            # ── 零回歸：未啟用 MVP 背書 ──
    # ── MVP 背書：勝率地基 × 即時確認係數（確認源最多 +9%），套誠實上限 ──
    wr = cal_row.get("win_rate")
    if wr is None:
        return conf
    backed = (wr / 100.0) * (1.0 + min(n_confirmed, 3) * 0.03)
    if (cal_row.get("n") or 0) < 30 or cal_row.get("status") == "FAIL":
        backed = min(backed, 0.45)             # 桶未過閘/資料不足 → 硬上限 0.45
    return round(max(0.28, min(0.82, backed)), 2)


def _call_claude_analysis(prompt: str, max_tokens: int = 1000) -> str:
    """IC AI 分析的便利入口，自動選擇 source"""
    settings = _ic_get_settings()
    source = settings.get("ai_source", "subscription")
    model = settings.get("ai_model", "claude-sonnet-4-20250514")
    return _ic_llm_call(prompt, model, source, max_tokens)

def _ic_llm_call(prompt: str, model: str, source: str, max_tokens: int = 550) -> str:
    """統一 LLM 呼叫入口。source='api' 走 Anthropic API Key（計費）；
    source='subscription' 走本機 claude CLI（訂閱，零 API 費用）。
    兩種模式都會記錄 token 用量（訂閱模式 cost 記 0）。"""
    settings = _ic_get_settings()
    source = (source or "api").lower()

    if source == "subscription":
        import subprocess, shutil
        cli = settings.get("claude_cli_path", "claude") or "claude"
        cli = shutil.which(cli) or (cli if os.path.isfile(cli) else None)
        if not cli:
            return "（找不到 claude CLI，請確認本機已安裝並登入 Claude Code，或在設定頁指定完整路徑）"
        try:
            proc = subprocess.run(
                [cli, "-p", "--model", model, "--output-format", "json"],
                input=prompt, capture_output=True, text=True,
                encoding="utf-8", timeout=120,
            )
            if proc.returncode != 0:
                return f"（訂閱呼叫失敗：{(proc.stderr or proc.stdout or '').strip()[:200]}）"
            data = json.loads(proc.stdout)
            text = data.get("result", "") or ""
            usage = data.get("usage", {}) or {}
            _ic_record_token_usage(model, usage.get("input_tokens", 0),
                                   usage.get("output_tokens", 0), cost_override=0.0)
            return text or "（訂閱模式無回傳內容）"
        except subprocess.TimeoutExpired:
            return "（訂閱呼叫逾時，請稍後再試）"
        except Exception as e:
            return f"（訂閱呼叫例外：{e}）"

    # 預設：API Key 模式
    api_key = settings.get("claude_api_key", "")
    if not api_key:
        return "（此功能來源設為 API，請在設定頁填入 Claude API Key，或將來源改為訂閱）"
    try:
        from anthropic import Anthropic
        client = Anthropic(api_key=api_key)
    except ImportError:
        return "（需安裝：pip install anthropic）"
    except Exception as e:
        return f"（初始化失敗：{e}）"
    try:
        msg = client.messages.create(
            model=model, max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        _ic_record_token_usage(model, msg.usage.input_tokens, msg.usage.output_tokens)
        return msg.content[0].text
    except Exception as e:
        return f"（分析失敗：{e}）"


def _ic_ai_analyze(code: str, name: str, market: str, tech: dict, macro: dict,
                   sources: list = None, model: str = None, source: str = None) -> str:
    settings = _ic_get_settings()
    if not model:
        model = settings.get("model_stock_analyze", "claude-sonnet-4-6")
    if not source:
        source = settings.get("source_stock_analyze", "api")

    holding  = settings.get("holding_period", "波段")
    risk     = settings.get("risk_level", "穩健")
    inds     = settings.get("preferred_indicators", "KD, MACD, MA")
    mkt_str  = "台股" if market == "TW" else "美股"
    sources  = sources or _ic_detect_sources(code, market)

    # 來源清單文字
    src_lines = "\n".join(
        f"  {'✦系統' if s['source_type']=='system' else '◈用戶'} {s['name']}：{s['detail']}（{s['reliability']}）"
        for s in sources
    ) or "  無額外來源"

    news_content = _ic_get_news_for_market(market)
    news_section = f"\n\n【用戶自定義新聞/資訊】（◈用戶來源，僅供參考）\n{news_content}" if news_content else ""

    # ── 知識庫 RAG 檢索（Phase B：以個股代號/名稱為標籤對焦）──
    kb_query = f"{name} {code} {mkt_str} {inds}"
    kb_hits = _kb_search(kb_query, top_k=6, boost_entities=[code, name])
    if kb_hits:
        kb_lines = "\n".join(
            f"[#{i+1}] 「{h['source_name']}」（{h['reliability']}{'｜🎯'+'/'.join(h['matched']) if h.get('matched') else ''}）：{h['text'][:280]}"
            for i, h in enumerate(kb_hits))
        kb_section = (f"\n\n【知識庫檢索片段】（用戶餵入的報告/文字，已依相關度排序，🎯=標籤對焦命中，引用時用 [#編號]）\n{kb_lines}")
        kb_rule = "\n- 若引用知識庫片段佐證，請在該點句末標注對應 [#編號]（可多個）。"
    else:
        kb_section, kb_rule = "", ""

    # ── Phase 1: 組裝強化指標區塊 ──
    inds_data = tech.get("indicators", {})
    extra_sections = []
    rs = inds_data.get("RS", {})
    if rs:
        rs_txt = " / ".join(f"{k}:{v:+.1f}%" for k, v in rs.items())
        extra_sections.append(f"相對強弱(vs大盤)：{rs_txt}")
    obv = inds_data.get("OBV", {})
    if obv:
        extra_sections.append(f"OBV趨勢：{obv.get('obv_trend','N/A')}")
    mfi = inds_data.get("MFI", {})
    if mfi:
        extra_sections.append(f"MFI資金流：{mfi.get('mfi','N/A')}")
    div = inds_data.get("divergence", "")
    if div:
        div_label = "量價頂背離（價漲量縮）" if div == "top_divergence" else "量價底背離（價跌量縮）"
        extra_sections.append(f"量價背離：{div_label}")
    sent_m = inds_data.get("sentiment_momentum", {})
    if sent_m:
        extra_sections.append(
            f"情緒動量：{sent_m.get('trend','N/A')} "
            f"(Δ{sent_m.get('delta',0):+.0f}, 共{sent_m.get('data_points',0)}筆歷史)")
    phase1_block = ("\n" + "\n".join(extra_sections)) if extra_sections else ""

    prompt = f"""你是一位謹慎、客觀的投資分析師。請分析 {mkt_str} {name}（{code}），
嚴格區分「確定事實」和「推論可能性」，不可過度自信。

【本次分析使用的資料來源】
{src_lines}

【技術面】（評分 {tech.get("score",50)}/100，偏向 {tech.get("direction","HOLD")}）
訊號：{', '.join(tech.get("signals",[]) or ['無明顯訊號'])}
指標：{json.dumps({k:v for k,v in inds_data.items() if k not in ("RS","OBV","MFI","divergence","sentiment_momentum")}, ensure_ascii=False)}

【量能與動量】{phase1_block if phase1_block else "（資料不足）"}

【總經環境】（來源：yfinance）
VIX={macro.get("VIX",{}).get("price","N/A")}  US10Y={macro.get("US10Y",{}).get("price","N/A")}%  DXY={macro.get("DXY",{}).get("price","N/A")}{news_section}{kb_section}

【投資人偏好】 週期：{holding}  風險：{risk}  指標：{inds}

請用繁體中文嚴格依照以下格式回答（每點 ≤25字）：

【資料確認】可從數據直接確認的事實（標注來源，最多3點）
【推論】基於以上數據的分析觀點，以「可能性約X%」表達（最多3點）
【方向】買進 / 觀察 / 減碼（一行）
【進場條件】具體且可驗證的條件（1~2點）
【失效條件】什麼情況下此判斷需重新評估（1~2點）
規則：每點 ≤25字。量能/動量/相對強弱若有明顯訊號請納入判斷。{kb_rule}

⚠ 本分析僅供參考，基於有限數據與AI推論，可能有誤，請獨立判斷後再操作。"""

    result = _ic_llm_call(prompt, model, source, max_tokens=600)
    # 底部附「本次知識庫參考」清單，讓使用者一眼看到論證根據
    if kb_hits:
        ref_list = "\n".join(
            f"  [#{i+1}] {h['source_name']}（{h['type']}/{h['reliability']}）"
            f"{'　🎯'+'/'.join(h['matched']) if h.get('matched') else ''}"
            for i, h in enumerate(kb_hits))
        result = f"{result}\n\n【本次知識庫參考】\n{ref_list}"
    return result


def _ic_record_token_usage(model: str, input_tokens: int, output_tokens: int, cost_override=None):
    if cost_override is not None:
        cost = cost_override   # 訂閱模式：零 API 費用
    else:
        cost_map = {
            'claude-haiku-4-5-20251001': (0.80, 4.00),
            'claude-sonnet-4-6':         (3.00, 15.00),
            'claude-opus-4-6':           (15.00, 75.00),
        }
        inp_rate, out_rate = cost_map.get(model, (3.00, 15.00))
        cost = (input_tokens * inp_rate + output_tokens * out_rate) / 1_000_000
    total_tokens = input_tokens + output_tokens
    today = datetime.now().strftime('%Y-%m-%d')
    con = db()
    con.execute("""CREATE TABLE IF NOT EXISTS ic_token_usage(
        date TEXT, model TEXT, tokens INTEGER, cost REAL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
    con.execute("INSERT INTO ic_token_usage(date,model,tokens,cost) VALUES(?,?,?,?)",
                (today, model, total_tokens, cost))
    con.commit()
    con.close()

# ── 推薦清單產生 ──────────────────────────────────

_MARKET_UNIVERSE = {
    "TW": [
        ("2330","台積電"),("2317","鴻海"),("2454","聯發科"),("2382","廣達"),("2308","台達電"),
        ("2881","富邦金"),("2882","國泰金"),("2891","中信金"),("2886","兆豐金"),("2884","玉山金"),
        ("2303","聯電"),("3711","日月光"),("2412","中華電"),("1301","台塑"),("1303","南亞"),
        ("2002","中鋼"),("3034","聯詠"),("2357","華碩"),("6505","台塑化"),("2327","國巨"),
        ("3037","欣興"),("3231","緯創"),("2345","智邦"),("4904","遠傳"),("2603","長榮"),
        ("3008","大立光"),("2379","瑞昱"),("6669","緯穎"),("8046","南電"),("5274","信驊"),
    ],
    "US": [
        ("AAPL","Apple"),("MSFT","Microsoft"),("GOOGL","Alphabet"),("AMZN","Amazon"),
        ("NVDA","NVIDIA"),("META","Meta"),("TSLA","Tesla"),("TSM","TSMC ADR"),
        ("AVGO","Broadcom"),("AMD","AMD"),("CRM","Salesforce"),("NFLX","Netflix"),
        ("ADBE","Adobe"),("COST","Costco"),("PEP","PepsiCo"),
        ("MU","Micron"),("QCOM","Qualcomm"),("INTC","Intel"),("AMAT","Applied Materials"),
        ("LRCX","Lam Research"),("KLAC","KLA"),("MRVL","Marvell"),("SNPS","Synopsys"),
        ("ARM","Arm Holdings"),("PLTR","Palantir"),("COIN","Coinbase"),("UBER","Uber"),
        ("SQ","Block"),("SHOP","Shopify"),("SNOW","Snowflake"),
    ],
}

def _detect_market(code: str) -> str:
    if code.isdigit() or (len(code) == 4 and code[0].isdigit()):
        return "TW"
    return "US"

# ══════════════════════════════════════════════════════════════════════════
# 量產引擎 MVP（production-engine-plan §七）— additive / 可關，預設不破壞既有輸出
#   R-PROD-1 regime 閘 + R-PROD-2 信心度 base-rate 背書 + R-PROD-3 A4 接 runtime
#   所有新邏輯僅在 mvp_engine=True 時啟動；off 時既有 IC 路徑 byte-equal。
# ══════════════════════════════════════════════════════════════════════════

# regime → 倉位係數（regime-rulebook §3，單一真實來源寫死在 code）
_REGIME_POS_COEF = {"TREND_UP": 1.0, "MEAN_REVERT": 0.8, "CRISIS": 0.3,
                    "RISK_OFF": 0.0, "UNKNOWN": 0.0}

def _regime_position_coef(regime: str) -> float:
    return _REGIME_POS_COEF.get((regime or "UNKNOWN").upper(), 0.0)


def _ensure_ic_calibration_table():
    """R-PROD-2/R-PROD-7：信心度校準表（regime×複合桶 → 勝率/超額/CI/N/status）。
    冪等建表 + 種入 MVP 硬寫的 A4 TREND_UP 桶當地基（夜間 base-rate job 之後會覆寫）。"""
    con = db()
    con.execute("""CREATE TABLE IF NOT EXISTS ic_confidence_calibration(
        regime     TEXT NOT NULL,
        bucket     TEXT NOT NULL,
        win_rate   REAL,           -- 條件切片勝率 %
        excess_avg REAL,           -- 對公平基準平均超額 %
        ci_low     REAL,           -- 超額 95% CI 下界
        ci_high    REAL,           -- 超額 95% CI 上界
        n          INTEGER,        -- 樣本筆數
        status     TEXT,           -- ALPHA_CANDIDATE / KEEPER / FAIL / HEURISTIC
        benchmark  TEXT,           -- 超額對照基準（equal_weight / twii / 0050）
        note       TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(regime, bucket)
    )""")
    seed = con.execute(
        "SELECT 1 FROM ic_confidence_calibration WHERE regime='TREND_UP' AND bucket='A4topsec'"
    ).fetchone()
    if not seed:
        con.execute("""INSERT INTO ic_confidence_calibration
            (regime,bucket,win_rate,excess_avg,ci_low,ci_high,n,status,benchmark,note,updated_at)
            VALUES('TREND_UP','A4topsec',58.0,0.83,0.29,1.43,2748,'ALPHA_CANDIDATE','equal_weight',?,?)""",
            ("MVP 硬寫地基：A4 板塊兩層複合 TREND_UP 條件切片，對等權公平基準超額（公司唯一過 ALPHA 閘的候選 alpha）",
             datetime.now().isoformat()))
    # ── 旗艦 A4∧月營收（EXP-2026-006）真驗校準 row：冪等 upsert（不存在才種，已存在不覆寫，夜間 job 可後續更新）──
    seed_rev = con.execute(
        "SELECT 1 FROM ic_confidence_calibration WHERE regime='TREND_UP' AND bucket='A4_revenue'"
    ).fetchone()
    if not seed_rev:
        # win_rate 留 NULL：研發 g1_row 勝率%未持久化（LEDGER 只記 excess/CI/N/DSR），誠實不臆造。
        #   → _ic_calc_confidence 對 win_rate=None 退回既有啟發式信心（不假裝校準背書）；
        #   badge/excess/CI/DSR 背書照常。待 AIF 補 g1_row 真值後夜間 job 覆寫即生效校準背書。
        con.execute("""INSERT INTO ic_confidence_calibration
            (regime,bucket,win_rate,excess_avg,ci_low,ci_high,n,status,benchmark,note,updated_at)
            VALUES('TREND_UP','A4_revenue',NULL,2.08,1.11,3.01,847,'ALPHA_CANDIDATE','equal_weight',?,?)""",
            ("A4∧月營收 旗艦·DSR跨閘·PBO假警報已解(6月OOS holdout不崩)·順勢型｜dsr=0.968｜EXP-2026-006"
             "｜win_rate 待補 g1_row 真值(未持久化→暫退啟發式信心，excess/CI/DSR 背書照常)",
             datetime.now().isoformat()))
    con.commit(); con.close()


def _ic_calibration_lookup(regime: str, bucket: str) -> dict | None:
    """查校準表；查無回 None（→ 信心度走既有手調公式）。"""
    if not regime or not bucket:
        return None
    try:
        _ensure_ic_calibration_table()
        con = db(); cur = con.cursor()
        cur.execute("""SELECT win_rate,excess_avg,ci_low,ci_high,n,status,benchmark,note
                       FROM ic_confidence_calibration WHERE regime=? AND bucket=?""",
                    (regime, bucket))
        r = cur.fetchone(); con.close()
    except Exception:
        return None
    if not r:
        return None
    return {"regime": regime, "bucket": bucket, "win_rate": r[0], "excess_avg": r[1],
            "ci_low": r[2], "ci_high": r[3], "n": r[4], "status": r[5],
            "benchmark": r[6], "note": r[7]}


def _a4_latest_from_smom(smom_by_code: dict, sector_of: dict) -> dict:
    """A4 runtime 抽取核心（純函式，無 DB/網路）：把各股 N 日報酬序列 + 產業映射
    丟進 base-rate 同一支 _compute_sector_factors，取『最新交易日』的 topsec/sector_rel，
    並在前段板塊成員內算橫斷面 rank_pct（與 base-rate 進場濾網排名同邏輯）。
    → runtime 與 base-rate 對同股同日 byte-equal 的單一真實來源。"""
    out = {"date": None, "topsec": {}, "sector_rel": {}, "rank_pct": {}}
    sec_fv = _compute_sector_factors(smom_by_code, sector_of,
                                     {"sector_rel", "sector_rel_topsec"})
    topsec = sec_fv.get("sector_rel_topsec", {})
    secrel = sec_fv.get("sector_rel", {})
    all_ds = sorted(set(topsec.keys()) | set(secrel.keys()))
    latest = all_ds[-1] if all_ds else None
    out["date"] = latest
    if latest:
        ts = dict(topsec.get(latest, {}))
        out["topsec"] = ts
        out["sector_rel"] = dict(secrel.get(latest, {}))
        if len(ts) >= _CS_MIN_XSECTION:                     # 同 base-rate 暖身樣本門檻
            items = sorted(ts.items(), key=lambda kv: kv[1])
            denom = (len(ts) - 1) if len(ts) > 1 else 1
            for ri, (cc, _v) in enumerate(items):
                out["rank_pct"][cc] = ri / denom
    return out


_A4_RUNTIME_CACHE = {}  # (market, asof_date) -> (ts, map)

def _a4_topsec_runtime_map(market: str, asof_date: str = "", _window_days: int = 220) -> dict:
    """R-PROD-3：把過閘的 A4 sector_rel_topsec 接成 runtime 個股組件分數。

    與 base-rate 引擎『同源同邏輯』：複用同一組純函式
      get_universe → _load_backtest_data → _compute_stock_features['c']
      → _nday_return_pct_series(_CS_REL_N) → _compute_sector_factors（板塊中位+前段板塊濾網）
    回傳最新交易日的 {code: topsec_rel} + 橫斷面 rank_pct（與 base-rate 進場濾網同排名）。
    只取最新日值（N 日窗只需 c[d] 與 c[d-N]，與 base-rate 該日值 byte-equal）。整 universe 算一次、快取。"""
    market = (market or "TW").upper()
    if not asof_date:
        asof_date = datetime.now().strftime("%Y-%m-%d")
    cache_key = (market, asof_date)
    cached = _A4_RUNTIME_CACHE.get(cache_key)
    if cached and time.time() - cached[0] < 3600:
        return cached[1]
    out = {"date": None, "topsec": {}, "sector_rel": {}, "rank_pct": {},
           "n_universe": 0, "n_sectored": 0, "market": market}
    try:
        uni = get_universe(market)
        if isinstance(uni, JSONResponse):
            _A4_RUNTIME_CACHE[cache_key] = (time.time(), out); return out
        raw = uni.get("data", uni.get("stocks", []))
        codes = [s if isinstance(s, str) else s.get("code", "") for s in raw]
        codes = [c for c in codes if c]
        if not codes:
            _A4_RUNTIME_CACHE[cache_key] = (time.time(), out); return out
        out["n_universe"] = len(codes)
        from datetime import timedelta as _td
        start = (datetime.strptime(asof_date, "%Y-%m-%d") - _td(days=_window_days)).strftime("%Y-%m-%d")
        all_dates, bar_data = _load_backtest_data(codes, market, start, asof_date)
        sector_of = _get_sector_map(market, codes)          # 同 base-rate 產業映射
        smom_by_code = {}
        for code in codes:
            if code not in sector_of:                       # 僅有 sector 的個股參與 A4（同 base-rate）
                continue
            f = _compute_stock_features(code, all_dates, bar_data)
            if not f:
                continue
            smom_by_code[code] = (f["dates"], _nday_return_pct_series(f["c"], _CS_REL_N))
        out["n_sectored"] = len(smom_by_code)
        ext = _a4_latest_from_smom(smom_by_code, sector_of)   # 與 base-rate 同源抽取核心
        out["date"] = ext["date"]
        out["topsec"] = ext["topsec"]
        out["sector_rel"] = ext["sector_rel"]
        out["rank_pct"] = ext["rank_pct"]
    except Exception as e:
        out["error"] = str(e)
    _A4_RUNTIME_CACHE[cache_key] = (time.time(), out)
    return out


_REVENUE_RUNTIME_CACHE = {}  # (market, asof_date) -> (ts, map)

def _revenue_mom_runtime_map(market: str, asof_date: str = "", _window_days: int = 220) -> dict:
    """旗艦 A4∧月營收 的 runtime 月營收 overlay（鏡像 _a4_topsec_runtime_map）。

    與回測『同源同邏輯』：複用同一組純函式
      get_universe → _load_backtest_data → _compute_stock_features
      → _compute_revenue_cs_factors（讀 revenue_monthly，PIT 對齊 announce_date<d 嚴格）
    回傳最新交易日的 {code: revenue_mom_raw} + 橫斷面 rank_pct（1=月營收動量最強，與回測 cs 排名同邏輯）。
    PIT 防洩漏由 _revenue_factor_series 保證（每交易日只納 announce_date 嚴格 < d 的已公布月營收）。

    【TW-gated】月營收僅 TW（revenue_monthly market='TW'）。mkt≠TW 回空 map → A4_revenue 桶在 US 自然不形成
    （A4_US 已證無 alpha，不借 TW 信心）。整 universe 算一次、快取（與 A4 runtime 同 TTL）。"""
    market = (market or "TW").upper()
    if not asof_date:
        asof_date = datetime.now().strftime("%Y-%m-%d")
    cache_key = (market, asof_date)
    cached = _REVENUE_RUNTIME_CACHE.get(cache_key)
    if cached and time.time() - cached[0] < 3600:
        return cached[1]
    out = {"date": None, "rank_pct": {}, "raw": {},
           "n_universe": 0, "n_revenue": 0, "market": market}
    if market != "TW":                                      # TW-gated：他市場不形成 A4_revenue 桶
        _REVENUE_RUNTIME_CACHE[cache_key] = (time.time(), out); return out
    try:
        uni = get_universe(market)
        if isinstance(uni, JSONResponse):
            _REVENUE_RUNTIME_CACHE[cache_key] = (time.time(), out); return out
        raw = uni.get("data", uni.get("stocks", []))
        codes = [s if isinstance(s, str) else s.get("code", "") for s in raw]
        codes = [c for c in codes if c]
        if not codes:
            _REVENUE_RUNTIME_CACHE[cache_key] = (time.time(), out); return out
        out["n_universe"] = len(codes)
        from datetime import timedelta as _td
        start = (datetime.strptime(asof_date, "%Y-%m-%d") - _td(days=_window_days)).strftime("%Y-%m-%d")
        all_dates, bar_data = _load_backtest_data(codes, market, start, asof_date)
        feats = {}
        for code in codes:
            f = _compute_stock_features(code, all_dates, bar_data)
            if f:
                feats[code] = f
        rev_fv = _compute_revenue_cs_factors(market, codes, feats, {"revenue_mom"})
        dmap = rev_fv.get("revenue_mom", {})               # {date: {code: raw_val}}
        # latest-rankable 選日（降冪掃最近交易日，取第一個橫斷面 ≥ _CS_MIN_XSECTION 的日子）。
        # 🐛 不可盲取 max(date)：月營收經 _revenue_factor_series fill-forward 是「逐股獨立」，只要任一檔
        # 日K較新鮮（資料新鮮度不齊，個別股多 1~2 個交易日）就在 dmap 生出一個僅含極少數股的最新日，整批
        # < 暖身門檻 → 不排名 → 回空（此即 8dab675 cockpit 真驗回空之根因；真驗 2026-06-24 僅 3 檔）。
        # A4 走 _compute_sector_factors 天生需板塊橫斷面，最新日自動收斂到稠密日，故無此症。降冪選第一個
        # 可排名日 → 與 forward _cs_rank_pct「每個達標日皆排名」逐字一致；PIT 不變（仍 announce_date<d）。
        ranked_dates = sorted(dmap.keys(), reverse=True)
        latest = next((d for d in ranked_dates if len(dmap[d]) >= _CS_MIN_XSECTION), None)
        if latest is None and ranked_dates:                # 暖身期：無任一日達門檻 → 誠實標最新日 raw、rank 留空
            latest = ranked_dates[0]
        out["date"] = latest
        if latest:
            cvals = dict(dmap[latest])
            out["raw"] = cvals
            out["n_revenue"] = len(cvals)
            if len(cvals) >= _CS_MIN_XSECTION:              # 同 base-rate 暖身樣本門檻才排名
                items = sorted(cvals.items(), key=lambda kv: kv[1])   # 升冪 → 高值 rank_pct 近 1
                denom = (len(cvals) - 1) if len(cvals) > 1 else 1
                for ri, (cc, _v) in enumerate(items):
                    out["rank_pct"][cc] = ri / denom
    except Exception as e:
        out["error"] = str(e)
    _REVENUE_RUNTIME_CACHE[cache_key] = (time.time(), out)
    return out


def _ic_chip_consecutive_buy(code: str, market: str = "TW") -> int:
    """法人（外資）連續買超天數（R-PROD-4 最小版，TW 限定，讀 chip_snapshot）。"""
    if (market or "TW").upper() != "TW":
        return 0
    try:
        con = db(); cur = con.cursor()
        cur.execute("SELECT foreign_buy FROM chip_snapshot WHERE code=? ORDER BY date DESC LIMIT 10",
                    (code,))
        rows = cur.fetchall(); con.close()
    except Exception:
        return 0
    consec = 0
    for (fb,) in rows:
        if (fb or 0) > 0:
            consec += 1
        else:
            break
    return consec


_A4_REVENUE_RANK_THR = 0.50   # A4_revenue 升級門檻＝研發旗艦 revenue_mom 層 top_k=0.50（rank_pct≥0.50），
                              # 與 EXP-2026-006 真驗策略逐字對齊（§3 校準數字背書的就是這個切片）。

def _ic_mvp_assess(code: str, name: str, market: str, tech: dict,
                   regime: str, a4_map: dict, revenue_map: dict = None) -> dict:
    """組裝個股 MVP 評估：複合桶判定 + A4 runtime 組件 + 倉位係數 + 信心度背書。
    純組裝，不改 tech['score']（技術分數零擾動 → 零回歸）。
    基礎複合桶＝ TREND_UP × A4topsec ∧ 技術多頭 ∧ (情緒不惡化 ∨ 法人連買)。
    更高階桶 A4_revenue＝ A4topsec ∧ 月營收動量 rank_pct≥_A4_REVENUE_RANK_THR（旗艦 A4∧月營收，TW-only）。"""
    regime = (regime or "UNKNOWN").upper()
    pos_coef = _regime_position_coef(regime)
    inds = tech.get("indicators", {}) or {}
    a4_map = a4_map or {}

    a4_rel    = (a4_map.get("topsec") or {}).get(code)        # 前段板塊 ∧ 板塊內相對
    a4_secrel = (a4_map.get("sector_rel") or {}).get(code)    # 板塊內相對（不論板塊排名）
    a4_rank   = (a4_map.get("rank_pct") or {}).get(code)
    in_top_sector = a4_rel is not None
    beats_sector  = in_top_sector and a4_rel > 0              # 屬前段強板塊 ∧ 贏過自己板塊
    a4_component = {
        "present": in_top_sector,
        "in_top_sector": in_top_sector,
        "topsec_rel": round(a4_rel, 4) if a4_rel is not None else None,
        "sector_rel": round(a4_secrel, 4) if a4_secrel is not None else None,
        "rank_pct": round(a4_rank, 4) if a4_rank is not None else None,
        "sub_score": round(a4_rank, 4) if a4_rank is not None else None,
        "beats_sector": beats_sector,
        "asof": a4_map.get("date"),
        "source": "runtime_sector_rel_topsec (= base-rate cs_overlay 同源)",
    }

    # 確認層
    tech_bullish = (tech.get("score", 50) >= 55)
    sm = inds.get("sentiment_momentum", {}) or {}
    sentiment_ok = sm.get("trend") != "deteriorating"
    consec_buy = _ic_chip_consecutive_buy(code, market)
    chip_confirm = consec_buy >= 2

    # 複合桶判定（基礎桶）
    bucket = None
    if regime == "TREND_UP" and beats_sector and tech_bullish and (sentiment_ok or chip_confirm):
        bucket = "A4topsec"

    # ── 更高階桶 A4_revenue：A4topsec ∧ 月營收動量強（旗艦 A4∧月營收，TW-only）。純加法升級 ──
    revenue_map = revenue_map or {}
    rev_rank = (revenue_map.get("rank_pct") or {}).get(code)
    rev_present = rev_rank is not None
    rev_strong = rev_present and rev_rank >= _A4_REVENUE_RANK_THR
    revenue_component = {
        "present": rev_present,
        "rank_pct": round(rev_rank, 4) if rev_rank is not None else None,
        "raw": (revenue_map.get("raw") or {}).get(code),
        "threshold": _A4_REVENUE_RANK_THR,
        "strong": rev_strong,
        "asof": revenue_map.get("date"),
        "source": "runtime revenue_mom cs (= 回測 _compute_revenue_cs_factors 同源，announce_date<d PIT)",
    }
    if bucket == "A4topsec" and rev_strong:
        bucket = "A4_revenue"                                # 升級：旗艦複合桶

    cal_row = _ic_calibration_lookup(regime, bucket) if bucket else None

    badge = "🧪 啟發式"
    if cal_row:
        st = cal_row.get("status")
        badge = "🎓 過閘keeper" if st == "KEEPER" else ("🎓 候選" if st == "ALPHA_CANDIDATE" else "🧪 啟發式")

    is_rev = (bucket == "A4_revenue")
    return {
        "regime": regime,
        "position_coef": pos_coef,
        "bucket": bucket,
        "bucket_desc": (
            "TREND_UP × A4topsec ∧ 月營收動量 rank_pct≥%.2f（旗艦 A4∧月營收·TW）" % _A4_REVENUE_RANK_THR
            if is_rev else
            ("TREND_UP × A4topsec ∧ 技術多頭 ∧ (情緒不惡化 ∨ 法人連買)" if bucket else None)),
        "bucket_factors": {
            "a4_topsec": beats_sector, "tech_bullish": tech_bullish,
            "sentiment_ok": sentiment_ok, "chip_consecutive_buy": consec_buy,
            "revenue_strong": rev_strong,
        },
        "a4": a4_component,
        "revenue": revenue_component,
        "confidence_backing": cal_row,
        "grad_badge": badge,
        "candidate_note": (
            ("A4∧月營收 旗艦 ALPHA 候選（DSR0.97·順勢型·單市場 TW·CI 邊際）→ 候選級信心，RISK_OFF 自動降曝險"
             if is_rev else
             "A4 為 ALPHA 候選（順勢型·2022 破功·單市場·CI 邊際）→ 候選級信心，RISK_OFF 自動降曝險")
            if bucket else None),
    }


@app.get("/api/ic/confidence-calibration")
def ic_confidence_calibration():
    """R-PROD-2：唯讀查信心度校準表（regime×複合桶 → 勝率/超額/CI/N/status）。"""
    _ensure_ic_calibration_table()
    con = db(); cur = con.cursor()
    cur.execute("""SELECT regime,bucket,win_rate,excess_avg,ci_low,ci_high,n,status,benchmark,note,updated_at
                   FROM ic_confidence_calibration ORDER BY regime,bucket""")
    rows = cur.fetchall(); con.close()
    return {"data": [{
        "regime": r[0], "bucket": r[1], "win_rate": r[2], "excess_avg": r[3],
        "ci_low": r[4], "ci_high": r[5], "n": r[6], "status": r[7],
        "benchmark": r[8], "note": r[9], "updated_at": r[10],
    } for r in rows]}


@app.post("/api/ic/recommendations/refresh")
def ic_refresh_recs(data: dict = {}, _: None = Depends(require_token)):
    """掃描自選股+持倉+市場精選，產生推薦清單。data: {use_ai, market, scope, mvp_engine}"""
    use_ai  = bool(data.get("use_ai", False))
    mkt_f   = data.get("market", "ALL")
    scope   = data.get("scope", "all")  # "watchlist", "universe", "all"
    macro   = _fetch_macro_data()

    # ── 量產引擎 MVP 開關（additive / 可關，預設 off → 既有路徑 byte-equal）──
    mvp_engine = bool(data.get("mvp_engine", False))
    _regime_cache: dict = {}
    _a4_cache: dict = {}
    _rev_cache: dict = {}
    def _regime_for(_m):
        if _m not in _regime_cache:
            try:
                _regime_cache[_m] = _calc_regime(_m)
            except Exception as e:
                _regime_cache[_m] = {"regime": "UNKNOWN", "reason": f"error: {e}"}
        return _regime_cache[_m]
    def _a4_for(_m):
        if _m not in _a4_cache:
            _a4_cache[_m] = _a4_topsec_runtime_map(_m) if mvp_engine else {}
        return _a4_cache[_m]
    def _rev_for(_m):   # 旗艦 A4∧月營收 runtime overlay（TW-gated；mvp_engine 關時零變動）
        if _m not in _rev_cache:
            _rev_cache[_m] = _revenue_mom_runtime_map(_m) if mvp_engine else {}
        return _rev_cache[_m]

    con = db()
    cur = con.cursor()
    cur.execute("SELECT DISTINCT code, name, market FROM watchlist")
    wl  = cur.fetchall()
    cur.execute("SELECT DISTINCT code, name, market FROM positions WHERE status='open'")
    pos = cur.fetchall()
    con.close()

    pos_fixed = []
    for code, name, mkt in pos:
        pos_fixed.append((code, name, _detect_market(code)))

    wl_codes = {r[0] for r in wl}
    pos_codes = {r[0] for r in pos_fixed}

    seen = {}
    src_map = {}
    if scope != "universe":
        for r in pos_fixed:
            seen[r[0]] = r
            src_map[r[0]] = "position"
        for r in wl:
            if r[0] not in seen:
                seen[r[0]] = r
            src_map[r[0]] = "position" if r[0] in pos_codes else "watchlist"
    if scope != "watchlist":
        for mkt_key, stocks in _MARKET_UNIVERSE.items():
            if mkt_f != "ALL" and mkt_key != mkt_f:
                continue
            for code, name in stocks:
                if code not in seen:
                    seen[code] = (code, name, mkt_key)
                    src_map[code] = "universe"

    candidates = list(seen.values())
    if mkt_f != "ALL":
        candidates = [c for c in candidates if c[2] == mkt_f]

    # Auto-fetch missing kbar data before scanning
    _warm_up_kbars_for_market(mkt_f if mkt_f != "ALL" else "TW", candidates)

    results = []
    now_ts  = datetime.now().isoformat()
    for code, name, mkt in candidates:
        tech    = _ic_score_stock(code, mkt)
        if not tech:
            continue
        sources    = _ic_detect_sources(code, mkt)
        direction  = tech["direction"]
        mvp = None; cal_row = None
        if mvp_engine:
            regime = _regime_for(mkt).get("regime", "UNKNOWN")
            mvp = _ic_mvp_assess(code, name, mkt, tech, regime, _a4_for(mkt), _rev_for(mkt))
            cal_row = mvp.get("confidence_backing")
            # R-PROD-1 regime 閘：非 TREND_UP 不輸出新 BUY（降為 HOLD，僅持倉管理）
            if mvp["regime"] != "TREND_UP" and direction == "BUY":
                direction = "HOLD"
                mvp["regime_gated"] = True
            tech["indicators"]["_mvp"] = mvp   # 隨 indicators 落地，重啟不丟、GET 可取回
        confidence = _ic_calc_confidence(tech, sources, macro, cal_row)
        _s = _ic_get_settings()
        ai_txt     = _ic_ai_analyze(code, name, mkt, tech, macro, sources, model=_s.get("model_rec_scan","claude-haiku-4-5-20251001"), source=_s.get("source_rec_scan","api")) if use_ai else ""
        _ic_record_sentiment(code, mkt, tech["score"], direction, confidence)
        disclaimer = "⚠ 以上分析僅供參考，基於有限數據與AI推論，可能有誤，請獨立判斷後再操作。"
        rec = {
            "code": code, "name": name, "market": mkt,
            "score": tech["score"], "direction": direction,
            "signals": tech["signals"], "indicators": tech["indicators"],
            "sources": sources, "confidence": confidence,
            "ai_analysis": ai_txt, "disclaimer": disclaimer,
            "entry_price": tech.get("price"),
            "created_at": now_ts,
            "rec_source": src_map.get(code, "universe"),
        }
        if mvp:
            rec["mvp"] = mvp
        results.append(rec)

    results.sort(key=lambda x: (x["direction"] == "BUY", x["score"]), reverse=True)

    with _ic_refresh_lock:
        con = db()
        cur = con.cursor()

        # 先把現有推薦存入歷史表（避免重複 archived）
        archive_where = "WHERE 1=1"
        if mkt_f != "ALL":
            archive_where += f" AND market='{mkt_f}'"
        cur.execute(f"""
            SELECT code, name, market, direction, score, confidence, ai_analysis, entry_price, created_at
            FROM ic_recommendations {archive_where}
        """)
        old_recs = cur.fetchall()
        for rec in old_recs:
            cur.execute("SELECT 1 FROM ic_rec_history WHERE code=? AND created_at=?",
                        (rec[0], rec[8]))
            if not cur.fetchone():
                cur.execute("""
                    INSERT INTO ic_rec_history
                      (code,name,market,direction,score,confidence,ai_analysis,entry_price,created_at,outcome)
                    VALUES (?,?,?,?,?,?,?,?,?,'PENDING')
                """, rec)

        if mkt_f != "ALL":
            con.execute("DELETE FROM ic_recommendations WHERE market=?", (mkt_f,))
        else:
            con.execute("DELETE FROM ic_recommendations")
        for r in results:
            con.execute("""
                INSERT INTO ic_recommendations
                  (market,code,name,direction,score,reasons,indicators,
                   ai_analysis,sources_used,confidence,disclaimer,entry_price,created_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                r["market"], r["code"], r["name"], r["direction"], r["score"],
                json.dumps(r["signals"],    ensure_ascii=False),
                json.dumps(r["indicators"], ensure_ascii=False),
                r["ai_analysis"],
                json.dumps(r["sources"],    ensure_ascii=False),
                r["confidence"], r["disclaimer"], r["entry_price"], r["created_at"],
            ))
        con.commit()
        con.close()

    # Item 7 — 推播高信心度 BUY 訊號
    def _notify_buys():
        try:
            c2 = db(); cu2 = c2.cursor()
            cu2.execute("SELECT value FROM risk_config WHERE key IN ('ic_notify_enabled','ic_notify_threshold')")
            cfg = {r[0]: r[1] for r in cu2.fetchall()}; c2.close()
            if cfg.get("ic_notify_enabled", "1") != "1":
                return
            threshold = float(cfg.get("ic_notify_threshold", "0.70"))
        except Exception:
            return  # Cannot read config; do not fire notifications

        buys = [r for r in results if r["direction"] == "BUY" and r.get("confidence", 0) >= threshold]
        if not buys:
            return
        lines = ["🔔 AI 建議推播 — 新一批高信心度 BUY 訊號\n"]
        for r in buys:
            sig_str = "、".join(r["signals"][:3]) if r["signals"] else "—"
            lines.append(
                f"▶ {r['name']}({r['code']}) [{r['market']}]\n"
                f"  評分 {r['score']:.0f} | 信心度 {r['confidence']*100:.0f}%\n"
                f"  訊號：{sig_str}"
            )
        _send_notification("\n".join(lines))

    threading.Thread(target=_notify_buys, daemon=True, name="ic-notify").start()

    regime_summary = {}
    if mvp_engine:
        for _m, _rv in _regime_cache.items():
            _rg = _rv.get("regime", "UNKNOWN")
            regime_summary[_m] = {
                "regime": _rg, "reason": _rv.get("reason"),
                "position_coef": _regime_position_coef(_rg),
                "date": _rv.get("date"),
                "tradeable": _rg == "TREND_UP",
                "a4_asof": (_a4_cache.get(_m) or {}).get("date"),
            }
    return {"ok": True, "count": len(results), "data": results, "results": results,
            "mvp_engine": mvp_engine, "regime": regime_summary}

@app.get("/api/ic/recommendations")
def ic_get_recs(market: str = ""):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT DISTINCT code FROM watchlist")
    wl_codes = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT DISTINCT code FROM positions WHERE status='open'")
    pos_codes = {r[0] for r in cur.fetchall()}
    if market:
        cur.execute("""
            SELECT id,market,code,name,direction,score,reasons,indicators,
                   ai_analysis,sources_used,confidence,disclaimer,created_at
            FROM ic_recommendations WHERE market=? ORDER BY score DESC
        """, (market.upper(),))
    else:
        cur.execute("""
            SELECT id,market,code,name,direction,score,reasons,indicators,
                   ai_analysis,sources_used,confidence,disclaimer,created_at
            FROM ic_recommendations ORDER BY score DESC
        """)
    rows = cur.fetchall()
    con.close()
    def _src(code):
        if code in pos_codes: return "position"
        if code in wl_codes: return "watchlist"
        return "universe"
    _recs = []
    for r in rows:
        ind = json.loads(r[7]) if r[7] else {}
        mvp = ind.pop("_mvp", None) if isinstance(ind, dict) else None  # 從 indicators 取回 MVP 區塊
        _recs.append({
            "id": r[0], "market": r[1], "code": r[2], "name": r[3],
            "direction": r[4], "score": r[5],
            "signals":    json.loads(r[6]) if r[6] else [],
            "indicators": ind,
            "ai_analysis": r[8] or "",
            "sources":    json.loads(r[9]) if r[9] else [],
            "confidence": r[10] or 0.5,
            "disclaimer": r[11] or "",
            "created_at": r[12] or "",
            "rec_source": _src(r[2]),
            "mvp": mvp,
        })
    # 若任一筆帶 MVP，附上各市場當前 regime 橫幅資料（reload 後 UI 仍能顯示）
    regime_summary = {}
    if any(x.get("mvp") for x in _recs):
        for _m in {x["market"] for x in _recs if x.get("mvp")}:
            try:
                _rv = _calc_regime(_m); _rg = _rv.get("regime", "UNKNOWN")
                regime_summary[_m] = {
                    "regime": _rg, "reason": _rv.get("reason"),
                    "position_coef": _regime_position_coef(_rg),
                    "tradeable": _rg == "TREND_UP",
                }
            except Exception:
                pass
    return {"data": _recs, "mvp_engine": bool(regime_summary), "regime": regime_summary}

def _ic_get_current_price(code: str, market: str) -> float | None:
    """Fetch latest close price for evaluation."""
    if market == "US":
        try:
            import yfinance as yf
            hist = yf.Ticker(code).history(period="3d", interval="1d")
            if not hist.empty:
                return round(float(hist["Close"].iloc[-1]), 2)
        except Exception:
            pass
        return None
    # TW — try Shioaji snapshot, fallback to K-bar cache
    try:
        api = get_api()
        contract = api.Contracts.Stocks.get(code)
        if contract:
            snaps = _api_call_with_backoff(api.snapshots, [contract])
            if snaps:
                return round(float(snaps[0].close), 2)
    except Exception:
        pass
    closes = _get_closes_from_cache(code, "D", 5)
    return round(closes[-1], 2) if closes else None

@app.get("/api/ic/notify-config")
def ic_notify_config_get():
    con = db(); cur = con.cursor()
    cur.execute("SELECT key, value FROM risk_config WHERE key IN ('ic_notify_enabled','ic_notify_threshold')")
    cfg = {r[0]: r[1] for r in cur.fetchall()}; con.close()
    return {
        "ic_notify_enabled":   cfg.get("ic_notify_enabled",   "1") == "1",
        "ic_notify_threshold": float(cfg.get("ic_notify_threshold", "0.70")),
    }

@app.post("/api/ic/notify-config")
def ic_notify_config_set(data: dict, _: None = Depends(require_token)):
    val = None
    if "ic_notify_threshold" in data:
        val = float(data["ic_notify_threshold"])
        if not (0.0 <= val <= 1.0):
            raise HTTPException(400, "threshold 必須介於 0 ~ 1")
    con = db(); ts = datetime.now().isoformat()
    if "ic_notify_enabled" in data:
        con.execute("INSERT OR REPLACE INTO risk_config(key,value,updated_at) VALUES(?,?,?)",
                    ("ic_notify_enabled", "1" if data["ic_notify_enabled"] else "0", ts))
    if val is not None:
        con.execute("INSERT OR REPLACE INTO risk_config(key,value,updated_at) VALUES(?,?,?)",
                    ("ic_notify_threshold", str(round(val, 2)), ts))
    con.commit(); con.close()
    return ic_notify_config_get()

@app.get("/api/ic/recommendations/history")
def ic_rec_history_list():
    """歷史推薦清單（最近 200 筆），含評估結果。"""
    con = db(); cur = con.cursor()
    cur.execute("""
        SELECT id, market, code, name, direction, score, confidence,
               entry_price, eval_price, pnl_pct, outcome, created_at, eval_at
        FROM ic_rec_history
        ORDER BY created_at DESC
        LIMIT 200
    """)
    rows = cur.fetchall(); con.close()
    _hist = [{
        "id":          r[0],
        "market":      r[1],
        "code":        r[2],
        "name":        r[3],
        "direction":   r[4],
        "score":       r[5],
        "confidence":  r[6] or 0.5,
        "entry_price": r[7],
        "eval_price":  r[8],
        "pnl_pct":     r[9],
        "outcome":     r[10] or "PENDING",
        "created_at":  r[11] or "",
        "eval_at":     r[12] or "",
    } for r in rows]
    return {"data": _hist}

@app.post("/api/ic/recommendations/evaluate")
def ic_evaluate_recs(_: None = Depends(require_token)):
    """
    對 PENDING 且建議時間 ≥ 3 天的歷史推薦進行評估：
    抓取現價計算損益，BUY+漲≥2%=WIN；SELL+跌≥2%=WIN；其餘 NEUTRAL。
    """
    cutoff = (datetime.now() - timedelta(days=3)).isoformat()
    con = db(); cur = con.cursor()
    cur.execute("""
        SELECT id, code, market, direction, entry_price, created_at
        FROM ic_rec_history
        WHERE outcome = 'PENDING' AND created_at < ?
    """, (cutoff,))
    pending = cur.fetchall(); con.close()

    backfilled = 0
    new_pending = []
    for hid, code, market, direction, entry_price, created_at in pending:
        if entry_price is None or entry_price == 0:
            ep = _ic_get_current_price(code, market)
            if ep:
                con2 = db()
                con2.execute("UPDATE ic_rec_history SET entry_price=? WHERE id=?", (ep, hid))
                con2.commit(); con2.close()
                entry_price = ep
                backfilled += 1
            else:
                continue
        new_pending.append((hid, code, market, direction, entry_price, created_at))
    pending = new_pending

    updated = []
    for hid, code, market, direction, entry_price, created_at in pending:
        current_price = _ic_get_current_price(code, market)
        if not current_price:
            continue
        pnl_pct = (current_price - entry_price) / entry_price * 100
        if direction == "BUY":
            outcome = "WIN" if pnl_pct > 2 else ("LOSS" if pnl_pct < -2 else "NEUTRAL")
        elif direction == "SELL":
            outcome = "WIN" if pnl_pct < -2 else ("LOSS" if pnl_pct > 2 else "NEUTRAL")
        else:
            outcome = "NEUTRAL"
        con = db()
        con.execute("""
            UPDATE ic_rec_history
            SET eval_price=?, eval_at=?, pnl_pct=?, outcome=?
            WHERE id=?
        """, (current_price, datetime.now().isoformat(), round(pnl_pct, 2), outcome, hid))
        con.commit(); con.close()
        updated.append({"id": hid, "code": code, "pnl_pct": round(pnl_pct, 2), "outcome": outcome})

    return {"data": updated, "updated": updated, "count": len(updated),
            "message": f"評估 {len(updated)} 筆，剩餘 {len(pending)-len(updated)} 筆無法取得價格"}

@app.post("/api/ic/analyze")
def ic_analyze_single(data: dict, _: None = Depends(require_token)):
    """單股深度分析。data: {code, market, use_ai}"""
    code   = data.get("code", "")
    market = data.get("market", "TW")
    use_ai = bool(data.get("use_ai", False))
    if not code:
        return JSONResponse({"error": "code required"}, status_code=400)

    name = code
    try:
        con = db(); cur = con.cursor()
        cur.execute("SELECT name FROM watchlist WHERE code=? LIMIT 1", (code,))
        row = cur.fetchone()
        if not row:
            cur.execute("SELECT name FROM positions WHERE code=? LIMIT 1", (code,))
            row = cur.fetchone()
        con.close()
        if row:
            name = row[0]
    except Exception:
        pass

    macro      = _fetch_macro_data()
    tech       = _ic_score_stock(code, market)
    sources    = _ic_detect_sources(code, market) if tech else []
    confidence = _ic_calc_confidence(tech, sources, macro) if tech else 0.5
    disclaimer = "⚠ 以上分析僅供參考，基於有限數據與AI推論，可能有誤，請獨立判斷後再操作。"
    _s = _ic_get_settings()
    ai_text    = _ic_ai_analyze(code, name, market, tech, macro, sources, model=_s.get("model_stock_analyze","claude-sonnet-4-6"), source=_s.get("source_stock_analyze","api")) if (use_ai and tech) else ""
    if tech:
        _ic_record_sentiment(code, market, tech.get("score", 50),
                             tech.get("direction", "HOLD"), confidence)
    return {
        "code": code, "name": name, "market": market,
        "tech": tech, "ai_analysis": ai_text,
        "sources": sources, "confidence": confidence, "disclaimer": disclaimer,
        "macro_context": {k: macro[k] for k in ("VIX","US10Y","DXY","SPX") if k in macro},
    }

# ── 總經環境解讀 ──────────────────────────────────

def _ic_interpret_macro() -> dict:
    macro = _fetch_macro_data()
    sigs, bearish, bullish = [], 0, 0

    def add(key, status, detail, color, bear=0, bull=0):
        nonlocal bearish, bullish
        sigs.append({"key": key, "status": status, "detail": detail, "color": color})
        bearish += bear; bullish += bull

    vix   = (macro.get("VIX")   or {}).get("price")
    us10y = (macro.get("US10Y") or {}).get("price")
    us2y  = (macro.get("US2Y")  or {}).get("price")
    dxy   = (macro.get("DXY")   or {}).get("price")
    wti   = (macro.get("WTI")   or {}).get("price")
    spx_c = (macro.get("SPX")   or {}).get("change_pct", 0)
    gold_c= (macro.get("GOLD")  or {}).get("change_pct", 0)

    if vix:
        if vix > 30:   add("VIX", "恐慌", f"VIX {vix:.1f} 極度恐慌", "red",   bear=3)
        elif vix > 20: add("VIX", "警戒", f"VIX {vix:.1f} 偏高", "yellow", bear=1)
        else:          add("VIX", "平靜", f"VIX {vix:.1f} 市場穩定", "green", bull=1)

    if us10y:
        if us10y > 5.0:   add("US10Y", "高壓", f"10年期 {us10y:.2f}% 利率高壓", "red",    bear=2)
        elif us10y > 4.5: add("US10Y", "偏高", f"10年期 {us10y:.2f}% 仍偏高",    "yellow", bear=1)
        else:             add("US10Y", "中性", f"10年期 {us10y:.2f}%",             "green")

    if us10y and us2y:
        sp = us10y - us2y
        if sp < 0:    add("殖利率曲線", "倒掛", f"10Y-2Y={sp:.2f}% 衰退警告",    "red",    bear=2)
        elif sp < 0.5:add("殖利率曲線", "平坦", f"10Y-2Y={sp:.2f}%",             "yellow", bear=1)
        else:         add("殖利率曲線", "正常", f"10Y-2Y={sp:.2f}% 健康",         "green",  bull=1)

    if dxy:
        if dxy > 107:  add("DXY", "強勢", f"美元 {dxy:.1f} 壓制新興市場", "yellow", bear=1)
        elif dxy < 100:add("DXY", "弱勢", f"美元 {dxy:.1f} 利多風險資產", "green",  bull=1)
        else:          add("DXY", "中性", f"美元 {dxy:.1f}",               "green")

    if wti and wti > 90:
        add("原油", "通脹", f"WTI ${wti:.1f} 通脹壓力", "yellow", bear=1)

    if gold_c and gold_c > 1.0:
        add("黃金", "避險", f"黃金 +{gold_c:.1f}% 資金避險中", "yellow", bear=1)

    if spx_c and spx_c < -1.5:
        add("SPX", "下跌", f"標普500 {spx_c:.1f}% 大幅下跌", "red", bear=1)
    elif spx_c and spx_c > 1.0:
        add("SPX", "上漲", f"標普500 +{spx_c:.1f}% 強勢上漲", "green", bull=1)

    if bearish >= 5:     env, color = "RISK_OFF", "red"
    elif bullish >= 3 and bearish <= 1: env, color = "RISK_ON", "green"
    else:                env, color = "NEUTRAL",  "yellow"

    advice_map = {
        "RISK_ON":  "總經偏多，可積極佈局成長型標的，科技/消費板塊優先",
        "NEUTRAL":  "環境中性，精選強勢個股，控制總倉位在60-70%",
        "RISK_OFF": "風險規避，建議降倉，偏好防禦型資產（公用/消費必需/黃金）",
    }
    label_map = {
        "RISK_ON": "做多環境 ✦", "NEUTRAL": "中性觀望 ～", "RISK_OFF": "風險規避 ▼"
    }
    try:
        rl = _get_risk_level()
        sys_risk = rl.get("risk_level", rl.get("level", "UNKNOWN")) if isinstance(rl, dict) else str(rl)
    except Exception:
        sys_risk = "UNKNOWN"
    return {
        "environment": env, "color": color,
        "label": label_map[env],
        "advice": advice_map[env],
        "bearish": bearish, "bullish": bullish,
        "signals": sigs,
        "system_risk_level": sys_risk,
        "note": "AI總經判讀與系統風控(risk-level)為獨立判斷，可能不一致",
        "updated_at": datetime.now().isoformat(),
    }

@app.get("/api/ic/macro/interpretation")
def ic_macro_interpretation():
    return _ic_interpret_macro()

# ── 台股法人買超排行 ───────────────────────────────

_tw_stock_names: dict = {}
_tw_stock_names_loaded = False

def _ensure_tw_stock_names():
    global _tw_stock_names, _tw_stock_names_loaded
    if _tw_stock_names_loaded:
        return
    _tw_stock_names_loaded = True
    con = db(); cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS stock_names (code TEXT PRIMARY KEY, name TEXT)")
    cur.execute("SELECT code, name FROM stock_names")
    _tw_stock_names = {r[0]: r[1] for r in cur.fetchall()}
    cur.execute("SELECT code, name FROM watchlist WHERE name != code")
    for code, name in cur.fetchall():
        if code not in _tw_stock_names:
            _tw_stock_names[code] = name
    cur.execute("SELECT code, name FROM positions WHERE name != code")
    for code, name in cur.fetchall():
        if code not in _tw_stock_names:
            _tw_stock_names[code] = name
    con.close()
    if len(_tw_stock_names) < 100:
        threading.Thread(target=_fetch_twse_stock_names, daemon=True).start()

def _fetch_twse_stock_names():
    global _tw_stock_names
    try:
        import urllib.request
        urls = [
            "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=json",
        ]
        req = urllib.request.Request(
            "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2",
            headers={"User-Agent": "Mozilla/5.0"}
        )
        resp = urllib.request.urlopen(req, timeout=15)
        html = resp.read().decode("big5", errors="ignore")
        import re
        rows = re.findall(r'<td[^>]*>(\d{4,6}[A-Z]?)\s*</td>\s*<td[^>]*>([^<]+)</td>', html)
        if not rows:
            rows = re.findall(r'>(\d{4,6}[A-Z]?)　([^<　]+)<', html)
        names = {}
        for code, name in rows:
            code = code.strip()
            name = name.strip()
            if name and name != code:
                names[code] = name
        if not names:
            well_known = {
                "0050":"元大台灣50","0056":"元大高股息","00878":"國泰永續高股息",
                "00881":"國泰台灣5G+","00891":"中信關鍵半導體","00892":"富邦台灣半導體",
                "00893":"國泰智能電動車","00900":"富邦特選高股息30",
                "00403A":"FH公司債A","00404A":"FH公司債B","00405A":"FH公司債C",
                "00401A":"FH美債A","00402A":"FH美債B","00400A":"FH美債",
                "00981A":"台新美債20年A",
                "2303":"聯電","2409":"友達","2344":"華邦電","2887":"台新金",
                "2892":"第一金","2324":"仁寶","2883":"開發金","9105":"泰金寶",
                "2801":"彰銀","2884":"玉山金","2303":"聯電",
                "1301":"台塑","1303":"南亞","1326":"台化","2002":"中鋼",
                "2308":"台達電","2317":"鴻海","2327":"國巨","2330":"台積電",
                "2345":"智邦","2357":"華碩","2379":"瑞昱","2382":"廣達",
                "2412":"中華電","2454":"聯發科","2603":"長榮","2880":"華南金",
                "2881":"富邦金","2882":"國泰金","2885":"元大金","2886":"兆豐金",
                "2888":"新光金","2889":"國票金","2890":"永豐金","2891":"中信金",
                "3008":"大立光","3034":"聯詠","3037":"欣興","3231":"緯創",
                "3443":"創意","3661":"世芯","3711":"日月光","5274":"信驊",
                "5876":"上海商銀","6505":"台塑化","6669":"緯穎","8046":"南電",
            }
            names = well_known
        _tw_stock_names.update(names)
        con = db()
        for code, name in names.items():
            con.execute("INSERT OR REPLACE INTO stock_names(code, name) VALUES(?,?)", (code, name))
        con.commit(); con.close()
        print(f"[股名快取] 載入 {len(names)} 檔股票名稱")
    except Exception as e:
        print(f"[股名快取] 抓取失敗: {e}")

def _get_stock_name(code: str) -> str:
    _ensure_tw_stock_names()
    return _tw_stock_names.get(code, code)

@app.get("/api/ic/tw/institutional-top")
def ic_tw_institutional_top():
    _ensure_tw_stock_names()
    con = db()
    cur = con.cursor()
    cur.execute("""
        SELECT cs.code,
               cs.foreign_buy,
               cs.itrust_buy,
               cs.dealer_buy,
               cs.margin_balance,
               cs.short_balance,
               cs.date
        FROM chip_snapshot cs
        WHERE cs.date = (SELECT MAX(date) FROM chip_snapshot WHERE code = cs.code)
        ORDER BY (COALESCE(cs.foreign_buy,0) + COALESCE(cs.itrust_buy,0)) DESC
        LIMIT 30
    """)
    rows = cur.fetchall()
    con.close()
    results = []
    for r in rows:
        code = r[0]
        if code.startswith("00"):
            continue
        results.append({
            "code":    code,
            "foreign": r[1] or 0,
            "trust":   r[2] or 0,
            "dealer":  r[3] or 0,
            "total":   (r[1] or 0) + (r[2] or 0) + (r[3] or 0),
            "margin":  r[4] or 0,
            "short":   r[5] or 0,
            "name":    _get_stock_name(code),
            "date":    r[6] or "",
        })
        if len(results) >= 20:
            break
    return results

# ── 批次評分 ──────────────────────────────────────

@app.post("/api/ic/batch-score")
def ic_batch_score(data: dict, _: None = Depends(require_token)):
    """data: {stocks:[{code,market},...], use_ai:bool}  或 {codes:[...],market:...}  最多20支"""
    stocks = data.get("stocks", [])[:20]
    if not stocks:
        codes = data.get("codes", [])[:20]
        mkt = data.get("market", "TW")
        stocks = [{"code": c, "market": mkt} for c in codes]
    use_ai = bool(data.get("use_ai", False))
    macro  = _fetch_macro_data()
    results = []
    for s in stocks:
        code   = (s.get("code") or "").strip().upper()
        market = s.get("market", "TW")
        if not code:
            continue
        name = code
        try:
            con = db(); cur = con.cursor()
            cur.execute("SELECT name FROM watchlist WHERE code=? LIMIT 1", (code,))
            row = cur.fetchone()
            con.close()
            if row: name = row[0]
        except Exception:
            pass
        tech = _ic_score_stock(code, market)
        if not tech:
            continue
        sources    = _ic_detect_sources(code, market)
        confidence = _ic_calc_confidence(tech, sources, macro)
        disclaimer = "⚠ 以上分析僅供參考，基於有限數據與AI推論，可能有誤，請獨立判斷後再操作。"
        _s = _ic_get_settings()
        ai_txt     = _ic_ai_analyze(code, name, market, tech, macro, sources, model=_s.get("model_batch_score","claude-haiku-4-5-20251001"), source=_s.get("source_batch_score","api")) if use_ai else ""
        _ic_record_sentiment(code, market, tech["score"], tech["direction"], confidence)
        results.append({
            "code": code, "name": name, "market": market,
            "score": tech["score"], "direction": tech["direction"],
            "signals": tech["signals"], "indicators": tech["indicators"],
            "sources": sources, "confidence": confidence,
            "ai_analysis": ai_txt, "disclaimer": disclaimer,
        })
    results.sort(key=lambda x: (x["direction"] == "BUY", x["score"]), reverse=True)
    return {"data": results, "results": results}

# ── 情緒歷史查詢 API ─────────────────────────────────
@app.get("/api/ic/sentiment-history/{code}")
def ic_sentiment_history(code: str):
    return _ic_get_sentiment_momentum(code.upper(), lookback=30)

# ── 回測預覽（資訊中心快速回測）──────────────────────

@app.post("/api/ic/backtest-preview")
def ic_backtest_preview(data: dict, _: None = Depends(require_token)):
    """
    快速回測單股。data: {code, market, direction, start}
    使用者偏好策略自動帶入。
    """
    code   = data.get("code", "")
    market = data.get("market", "TW")
    start  = data.get("start",  "2021-01-01")
    if not code:
        return JSONResponse({"error": "code required"}, status_code=400)

    settings  = _ic_get_settings()
    trade_type = settings.get("holding_period", "波段")
    buy_signals  = ["BUY_A", "BUY_B", "LOW_BUY", "SQUEEZE_BREAK"]
    sell_signals = ["EXIT_B", "EXIT_C", "EXIT_D"]

    config = {
        "name":   f"IC快速回測 {code}",
        "codes":  [code],
        "market": market,
        "start":  start,
        "end":    datetime.now().strftime("%Y-%m-%d"),
        "strategies": buy_signals + sell_signals,
        "initial_capital": 1000000,
        "trade_type": trade_type,
        "commission_discount": 0.6 if market == "TW" else 1.0,
    }
    return _run_backtest(config)

# ── 資料來源管理 + 抓取 ───────────────────────────────

@app.post("/api/ic/sources/{db_id}/fetch")
def ic_fetch_one_source(db_id: int, _: None = Depends(require_token)):
    """立即抓取並快取指定 user 來源的內容。"""
    con = db(); cur = con.cursor()
    cur.execute("SELECT id,name,url,type,market FROM ic_news_sources WHERE id=?", (db_id,))
    row = cur.fetchone(); con.close()
    if not row:
        return JSONResponse({"error": "來源不存在"}, status_code=404)
    source = {"id": row[0], "name": row[1], "url": row[2], "type": row[3], "market": row[4]}
    content = _ic_fetch_source(source)
    now = datetime.now().isoformat()
    con = db()
    con.execute("INSERT OR REPLACE INTO ic_news_cache(source_id,content,fetched_at) VALUES(?,?,?)",
                (db_id, content, now))
    con.execute("UPDATE ic_news_sources SET last_fetched=? WHERE id=?", (now, db_id))
    con.commit(); con.close()
    n_chunks = _kb_ingest(db_id, content)   # 入知識庫（切塊+向量）
    return {"ok": True, "source_id": db_id, "name": source["name"],
            "content": content, "fetched_at": now, "chunks": n_chunks}

@app.post("/api/ic/sources/fetch-all")
def ic_fetch_all_sources(_: None = Depends(require_token)):
    """抓取所有 active 用戶來源。超過 1 小時未更新才重抓（快取保護）。"""
    con = db(); cur = con.cursor()
    cur.execute("""
        SELECT ns.id, ns.name, ns.url, ns.type, ns.market, nc.fetched_at
        FROM ic_news_sources ns
        LEFT JOIN ic_news_cache nc ON nc.source_id = ns.id
        WHERE ns.active = 1 AND ns.url != ''
    """)
    sources = cur.fetchall(); con.close()

    results = []
    for sid, name, url, src_type, market, last_fetched in sources:
        if last_fetched:
            age = (datetime.now() - datetime.fromisoformat(last_fetched)).total_seconds()
            if age < 3600:
                results.append({"source_id": sid, "name": name, "cached": True,
                                 "fetched_at": last_fetched})
                continue
        content = _ic_fetch_source({"id": sid, "name": name, "url": url,
                                    "type": src_type, "market": market})
        now = datetime.now().isoformat()
        con = db()
        con.execute("INSERT OR REPLACE INTO ic_news_cache(source_id,content,fetched_at) VALUES(?,?,?)",
                    (sid, content, now))
        con.execute("UPDATE ic_news_sources SET last_fetched=? WHERE id=?", (now, sid))
        con.commit(); con.close()
        _kb_ingest(sid, content)   # 入知識庫
        results.append({"source_id": sid, "name": name,
                        "content": content[:200], "cached": False, "fetched_at": now})
    return {"data": results, "results": results, "count": len(results)}

@app.get("/api/ic/sources/news")
def ic_get_news_cache():
    """回傳所有已快取的新聞內容（含 fetched_at）。"""
    con = db(); cur = con.cursor()
    cur.execute("""
        SELECT ns.id, ns.name, ns.market, ns.type, ns.reliability,
               nc.content, nc.fetched_at
        FROM ic_news_sources ns
        JOIN ic_news_cache nc ON nc.source_id = ns.id
        WHERE ns.active = 1
        ORDER BY nc.fetched_at DESC
    """)
    rows = cur.fetchall(); con.close()
    return [{
        "source_id": r[0], "name": r[1], "market": r[2], "type": r[3],
        "reliability": r[4], "content": r[5] or "", "fetched_at": r[6] or "",
    } for r in rows]

@app.get("/api/ic/sources")
def ic_list_sources():
    """Return system built-in sources + user-defined sources from DB."""
    ds_all = get_datasources()
    ds_map = {d["id"]: d for d in ds_all}
    system = []
    for src in IC_SYSTEM_SOURCES:
        s = dict(src)
        linked = [ds_map[did] for did in s.get("datasource_ids", []) if did in ds_map]
        s["linked_datasources"] = [{
            "id": d["id"], "name": d["name"], "status": d["status"],
            "configured": d.get("configured", False), "market_scope": d.get("market_scope", "ALL"),
        } for d in linked]
        system.append(s)
    con = db(); cur = con.cursor()
    cur.execute("""
        SELECT id,name,url,type,market,source_type,description,active,reliability,last_fetched,entities
        FROM ic_news_sources ORDER BY id
    """)
    rows = cur.fetchall()
    # 每來源的 chunk 數
    cc = {r[0]: r[1] for r in cur.execute(
        "SELECT source_id, COUNT(*) FROM ic_kb_chunks GROUP BY source_id").fetchall()}
    user_sources = [{
        "id":          f"usr_{r[0]}",
        "db_id":       r[0],
        "name":        r[1],
        "url":         r[2] or "",
        "type":        r[3],
        "market":      r[4],
        "source_type": "user",
        "description": r[6] or "",
        "active":      bool(r[7]),
        "reliability": r[8] or "reference",
        "last_fetched":r[9] or "",
        "entities":    _kb_parse_ent_field(r[10]),
        "chunks":      cc.get(r[0], 0),
    } for r in rows]
    con.close()
    return {"system": system, "user": user_sources,
            "embedding": ("ready" if _KB_EMBED not in (None, False) else
                          ("disabled" if _KB_EMBED is False else "lazy"))}

def _kb_parse_entities(data: dict):
    ents = data.get("entities")
    if isinstance(ents, list):
        return json.dumps([str(x).strip() for x in ents if str(x).strip()], ensure_ascii=False)
    if isinstance(ents, str) and ents.strip():
        return json.dumps([x.strip() for x in ents.replace("，", ",").split(",") if x.strip()], ensure_ascii=False)
    return "[]"

@app.post("/api/ic/sources")
def ic_add_source(data: dict, _: None = Depends(require_token)):
    """Add a user-defined source. URL 類會立即抓取入庫；TEXT 類直接把 content 入庫。"""
    name = (data.get("name") or "").strip()
    if not name:
        return JSONResponse({"error": "name required"}, status_code=400)
    src_type = (data.get("type") or "HTML").upper()
    content  = (data.get("content") or "").strip()
    con = db()
    cur = con.cursor()
    cur.execute("""
        INSERT INTO ic_news_sources (name,url,type,market,source_type,description,active,reliability,entities,content)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (
        name,
        (data.get("url") or "").strip(),
        src_type,
        (data.get("market") or "ALL").upper(),
        "user",
        (data.get("description") or "").strip(),
        1,
        (data.get("reliability") or "reference"),
        _kb_parse_entities(data),
        content,
    ))
    new_id = cur.lastrowid
    con.commit(); con.close()
    n_chunks = 0
    now = datetime.now().isoformat()
    if src_type == "TEXT" and content:
        con = db()
        con.execute("INSERT OR REPLACE INTO ic_news_cache(source_id,content,fetched_at) VALUES(?,?,?)", (new_id, content, now))
        con.execute("UPDATE ic_news_sources SET last_fetched=? WHERE id=?", (now, new_id))
        con.commit(); con.close()
        n_chunks = _kb_ingest(new_id, content)
    elif (data.get("url") or "").strip():   # URL 類立即抓取入庫
        fetched = _ic_fetch_source({"id": new_id, "name": name, "url": data.get("url"), "type": src_type, "market": data.get("market")})
        con = db()
        con.execute("INSERT OR REPLACE INTO ic_news_cache(source_id,content,fetched_at) VALUES(?,?,?)", (new_id, fetched, now))
        con.execute("UPDATE ic_news_sources SET last_fetched=? WHERE id=?", (now, new_id))
        con.commit(); con.close()
        n_chunks = _kb_ingest(new_id, fetched)
    return {"ok": True, "id": f"usr_{new_id}", "db_id": new_id, "chunks": n_chunks}

@app.post("/api/ic/sources/text")
def ic_add_text_source(data: dict, _: None = Depends(require_token)):
    """貼上純文字 → 建立 TEXT 來源並入知識庫。"""
    data = dict(data or {})
    data["type"] = "TEXT"
    return ic_add_source(data, _)

@app.post("/api/ic/sources/upload")
async def ic_upload_pdf(file: UploadFile = File(...), name: str = Form(""),
                        market: str = Form("ALL"), reliability: str = Form("reference"),
                        entities: str = Form(""), description: str = Form(""),
                        _: None = Depends(require_token)):
    """上傳 PDF → 本機抽文字 → 建立 PDF 來源並入知識庫。"""
    raw = await file.read()
    if len(raw) > 25 * 1024 * 1024:
        return JSONResponse({"error": "PDF 超過 25MB"}, status_code=400)
    text = _kb_extract_pdf(raw)
    if text.startswith("[PDF 解析失敗"):
        return JSONResponse({"error": text}, status_code=400)
    disp = (name or file.filename or "PDF").strip()
    now = datetime.now().isoformat()
    con = db(); cur = con.cursor()
    cur.execute("""INSERT INTO ic_news_sources
        (name,url,type,market,source_type,description,active,reliability,entities,content,last_fetched)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (disp, "", "PDF", (market or "ALL").upper(), "user",
         (description or f"PDF：{file.filename}").strip(), 1, reliability or "reference",
         _kb_parse_entities({"entities": entities}), text[:200000], now))
    new_id = cur.lastrowid
    con.execute("INSERT OR REPLACE INTO ic_news_cache(source_id,content,fetched_at) VALUES(?,?,?)", (new_id, text, now))
    con.commit(); con.close()
    n_chunks = _kb_ingest(new_id, text)
    return {"ok": True, "db_id": new_id, "name": disp, "chars": len(text), "chunks": n_chunks}

@app.get("/api/ic/kb/search")
def ic_kb_search_route(q: str, k: int = 6, types: str = "", boost: str = "", only: str = ""):
    """知識庫檢索預覽。types=類型過濾, boost=對焦標籤(加權), only=硬篩標籤。"""
    tlist = [t.strip().upper() for t in types.split(",") if t.strip()] or None
    blist = [t.strip() for t in boost.split(",") if t.strip()] or None
    olist = [t.strip() for t in only.split(",") if t.strip()] or None
    return {"query": q, "results": _kb_search(q, top_k=k, types=tlist,
                                               boost_entities=blist, filter_entities=olist)}

@app.get("/api/ic/kb/entities")
def ic_kb_entities():
    """回傳所有 active 來源用過的標籤 + 出現次數（給前端篩選用）。"""
    con = db()
    rows = con.execute("SELECT entities FROM ic_news_sources WHERE active=1").fetchall()
    con.close()
    counts = {}
    for (raw,) in rows:
        for e in _kb_parse_ent_field(raw):
            counts[e] = counts.get(e, 0) + 1
    return {"entities": sorted(({"name": k, "count": v} for k, v in counts.items()),
                               key=lambda x: (-x["count"], x["name"]))}

@app.delete("/api/ic/sources/{db_id}")
def ic_delete_source(db_id: int, _: None = Depends(require_token)):
    """Delete a user-defined source (連帶清掉知識庫 chunk + 快取)。"""
    con = db()
    for cid in [r[0] for r in con.execute("SELECT id FROM ic_kb_chunks WHERE source_id=?", (db_id,)).fetchall()]:
        con.execute("INSERT INTO ic_kb_fts(ic_kb_fts, rowid, text) VALUES('delete', ?, (SELECT text FROM ic_kb_chunks WHERE id=?))", (cid, cid))
    con.execute("DELETE FROM ic_kb_chunks WHERE source_id=?", (db_id,))
    con.execute("DELETE FROM ic_news_cache WHERE source_id=?", (db_id,))
    con.execute("DELETE FROM ic_news_sources WHERE id=?", (db_id,))
    con.commit(); con.close()
    return {"ok": True}

@app.get("/api/ic/info_center")
def ic_page():
    """Serve the info center SPA"""
    p = Path(__file__).parent / "info_center.html"
    if p.exists():
        return FileResponse(str(p), media_type="text/html")
    return JSONResponse({"error": "info_center.html not found"}, status_code=404)

@app.get("/info_center.html", include_in_schema=False)
def serve_info_center_shortcut():
    return ic_page()

# ── GitHub Agent ──────────────────────────────────
try:
    import github_agent as _gh_agent
    _GH_AVAILABLE = True
except ImportError:
    _GH_AVAILABLE = False

def _require_gh():
    if not _GH_AVAILABLE:
        raise HTTPException(503, "github_agent.py not found")

@app.get("/api/github/status")
def github_status(_: None = Depends(require_token)):
    _require_gh()
    return _gh_agent.git_status()

@app.get("/api/github/watch")
def github_watch_list(_: None = Depends(require_token)):
    _require_gh()
    return _gh_agent.watch_list()

@app.post("/api/github/watch")
def github_watch_add(data: dict, _: None = Depends(require_token)):
    _require_gh()
    owner = data.get("owner", "").strip()
    repo  = data.get("repo", "").strip()
    if not owner or not repo:
        raise HTTPException(400, "owner and repo are required")
    return _gh_agent.watch_add(owner, repo, data.get("label", ""))

@app.delete("/api/github/watch/{owner}/{repo}")
def github_watch_remove(owner: str, repo: str, _: None = Depends(require_token)):
    _require_gh()
    return _gh_agent.watch_remove(owner, repo)

@app.post("/api/github/watch/check")
def github_watch_check(_: None = Depends(require_token)):
    """Manually trigger a check of all watched repos for new releases."""
    _require_gh()
    found = _gh_agent.watch_check(notify_fn=_send_notification)
    return {"ok": True, "new_releases": found, "count": len(found)}

@app.post("/api/github/push")
def github_push(data: dict, _: None = Depends(require_token)):
    """
    Push local changes to GitHub.
    body: { "message": str, "bump": "patch"|"minor"|"major", "include_docs": bool }
    """
    _require_gh()
    message = (data.get("message") or "").strip()
    if not message:
        raise HTTPException(400, "message is required")
    bump = data.get("bump", "patch")
    if bump not in ("patch", "minor", "major"):
        raise HTTPException(400, "bump must be patch, minor, or major")
    include_docs = bool(data.get("include_docs", True))
    result = _gh_agent.push_update(message, bump, include_docs)
    if not result["ok"]:
        raise HTTPException(500, result["message"])
    return result

# ── 資料管理 ──────────────────────────────────────

@app.get("/api/data/stats")
def data_stats():
    """各資料表的筆數、最早/最新日期、估算大小"""
    con = db()
    tables = [
        ("kbar_cache",       "date_key",   "code, tf, date_key"),
        ("chip_snapshot",    "date",        "code, date"),
        ("news_cache",       "date",        "code, date, headline"),
        ("signal_log",       "created_at",  "code, signal_type, created_at"),
        ("daytrade_snapshot","date",        "code, date"),
        ("ic_sentiment_history", "date",    "code, date, score"),
        ("ic_kb_chunks",     "created_at",  "source_id, chunk_idx"),
        ("ic_token_usage",   "date",        "date, model, tokens"),
        ("ic_news_cache",    "fetched_at",  "source_id, fetched_at"),
        ("ic_rec_history",   "archived_at", "code, archived_at"),
    ]
    result = []
    for tbl, date_col, cols in tables:
        try:
            row = con.execute(f"SELECT COUNT(*), MIN({date_col}), MAX({date_col}) FROM {tbl}").fetchone()
            page_count = con.execute(f"PRAGMA page_count").fetchone()[0]
            page_size  = con.execute(f"PRAGMA page_size").fetchone()[0]
            result.append({
                "table": tbl, "count": row[0],
                "oldest": row[1], "newest": row[2],
            })
        except Exception:
            pass
    db_size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
    con.close()
    return {"tables": result, "db_size_mb": round(db_size / 1048576, 2)}

@app.post("/api/data/cleanup")
def data_cleanup(data: dict = {}, _: None = Depends(require_token)):
    """
    清理過期資料。data: { retention_days: int (default 180) }
    保留最近 N 天的資料，刪除更舊的。
    """
    days = int(data.get("retention_days", 180))
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    con = db()
    deleted = {}
    cleanup_targets = [
        ("kbar_cache",        "date_key < ?"),
        ("chip_snapshot",     "date < ?"),
        ("news_cache",        "date < ?"),
        ("signal_log",        "created_at < ?"),
        ("daytrade_snapshot", "date < ?"),
        ("ic_sentiment_history", "date < ?"),
        ("ic_news_cache",     "fetched_at < ?"),
        ("ic_token_usage",    "date < ?"),
        ("ic_rec_history",    "archived_at < ?"),
    ]
    for tbl, where in cleanup_targets:
        try:
            before = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            con.execute(f"DELETE FROM {tbl} WHERE {where}", (cutoff,))
            after = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            removed = before - after
            if removed > 0:
                deleted[tbl] = removed
        except Exception:
            pass
    con.execute("PRAGMA optimize")
    con.commit()
    con.close()
    try:
        con2 = db()
        con2.execute("VACUUM")
        con2.close()
    except Exception:
        pass
    return {"cutoff_date": cutoff, "retention_days": days, "deleted": deleted}

@app.get("/api/data/integrity")
def data_integrity():
    """檢查資料完整性：缺失天數、孤立資料"""
    con = db()
    issues = []
    try:
        wl_codes = [r[0] for r in con.execute("SELECT code FROM watchlist").fetchall()]
        for code in wl_codes[:10]:
            cnt = con.execute(
                "SELECT COUNT(DISTINCT date_key) FROM kbar_cache WHERE code=? AND tf='D'", (code,)
            ).fetchone()[0]
            if cnt < 20:
                issues.append({"type": "sparse_kbar", "code": code, "days": cnt,
                               "msg": f"{code} 僅 {cnt} 天日K資料"})
        orphan_chunks = con.execute(
            "SELECT COUNT(*) FROM ic_kb_chunks WHERE source_id NOT IN (SELECT id FROM ic_news_sources)"
        ).fetchone()[0]
        if orphan_chunks > 0:
            issues.append({"type": "orphan_chunks", "count": orphan_chunks,
                           "msg": f"{orphan_chunks} 個知識庫 chunk 的來源已被刪除"})
    except Exception as e:
        issues.append({"type": "error", "msg": str(e)})
    con.close()
    return {"issues": issues, "checked_at": datetime.now().isoformat()}

# ═══════════════════════════════════════════════════════════════
# ── 策略專家委員會 (Strategy Expert Committee)  /api/expert/  ──
# ═══════════════════════════════════════════════════════════════

EXPERT_ROLES = [
    {"id": "quant",     "name": "量化分析師", "icon": "📊", "color": "#6c5ce7",
     "perspective": "統計模型、因子分析、回測驗證、過擬合檢測",
     "prompt_role": "你是一位嚴謹的量化分析師，專精統計套利、Alpha因子、IC/ICIR檢驗、Walk-forward回測。你重視數據證據，對任何沒有統計顯著性的論點保持懷疑。"},
    {"id": "technical", "name": "技術分析師", "icon": "📈", "color": "#00b894",
     "perspective": "技術指標、圖形型態、量價關係、週期分析",
     "prompt_role": "你是一位經驗豐富的技術分析師，專精KD/MACD/RSI/均線系統、量價分析、K線形態辨識。你相信市場價格已反映所有資訊，關注趨勢與轉折訊號。"},
    {"id": "macro",     "name": "總經策略師", "icon": "🏦", "color": "#0984e3",
     "perspective": "宏觀經濟、利率政策、匯率、板塊輪動",
     "prompt_role": "你是一位總經策略師，專精Fed政策分析、殖利率曲線、匯率走勢、GICS板塊輪動。你從Top-down角度判斷大環境，再決定配置方向。"},
    {"id": "math",      "name": "數理研究員", "icon": "🧮", "color": "#e17055",
     "perspective": "隨機過程、均值回歸、動量衰減模型、資訊熵",
     "prompt_role": "你是一位數理金融研究員，專精Black-Scholes、隨機微分方程、均值回歸速率估計、動量衰減半衰期、Shannon熵用於市場不確定性量化。你用數學模型驗證其他專家的直覺判斷。"},
    {"id": "sentiment", "name": "情緒分析師", "icon": "📰", "color": "#fdcb6e",
     "perspective": "新聞NLP情緒、社群輿情、極端值反轉、事件驅動",
     "prompt_role": "你是一位市場情緒分析師，專精NLP新聞情緒分析、社群媒體輿情監測、恐慌/貪婪指數、極端情緒反轉交易。你關注市場參與者的心理狀態變化。"},
]

EXPERT_SCHEDULES_DEFAULT = [
    {"id": "tw_close",  "name": "台股收盤後分析", "time": "14:30", "timezone": "Asia/Taipei",
     "description": "台股收盤後回顧當日表現，提出明日策略建議", "enabled": True,
     "market_focus": "TW", "horizon": "daily"},
    {"id": "us_close",  "name": "美股收盤後分析", "time": "06:00", "timezone": "Asia/Taipei",
     "description": "美股收盤後（台灣清晨），分析美股對台股隔日影響", "enabled": True,
     "market_focus": "US", "horizon": "daily"},
    {"id": "weekly",    "name": "每週策略會議", "time": "18:00", "timezone": "Asia/Taipei",
     "description": "每週日晚間，回顧一週績效，調整下週策略權重", "enabled": True,
     "market_focus": "ALL", "horizon": "weekly", "day_of_week": 6},
    {"id": "monthly",   "name": "月度策略大會", "time": "10:00", "timezone": "Asia/Taipei",
     "description": "每月第一個交易日，全面檢視資產配置與中期展望", "enabled": True,
     "market_focus": "ALL", "horizon": "monthly", "day_of_month": 1},
]

def _expert_db_migrate():
    con = db()
    con.executescript("""
        CREATE TABLE IF NOT EXISTS expert_sessions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            title       TEXT,
            market      TEXT DEFAULT 'ALL',
            targets     TEXT DEFAULT '[]',
            horizon     TEXT DEFAULT 'daily',
            experts     TEXT DEFAULT '[]',
            status      TEXT DEFAULT 'pending',
            rounds      INTEGER DEFAULT 1,
            trigger     TEXT DEFAULT 'manual',
            config      TEXT DEFAULT '{}',
            summary     TEXT,
            consensus   TEXT,
            created_at  TEXT,
            completed_at TEXT
        );
        CREATE TABLE IF NOT EXISTS expert_opinions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id  INTEGER,
            expert_id   TEXT,
            round_num   INTEGER DEFAULT 1,
            opinion     TEXT,
            direction   TEXT,
            confidence  REAL DEFAULT 0.5,
            key_points  TEXT DEFAULT '[]',
            created_at  TEXT,
            FOREIGN KEY(session_id) REFERENCES expert_sessions(id)
        );
        CREATE TABLE IF NOT EXISTS expert_schedules (
            id          TEXT PRIMARY KEY,
            name        TEXT,
            time        TEXT,
            timezone    TEXT DEFAULT 'Asia/Taipei',
            description TEXT,
            enabled     INTEGER DEFAULT 1,
            market_focus TEXT DEFAULT 'ALL',
            horizon     TEXT DEFAULT 'daily',
            day_of_week INTEGER,
            day_of_month INTEGER,
            experts     TEXT DEFAULT '[]',
            notify      TEXT DEFAULT '[]',
            updated_at  TEXT
        );
        CREATE TABLE IF NOT EXISTS expert_config (
            key   TEXT PRIMARY KEY,
            value TEXT
        );
    """)
    con.execute("UPDATE expert_sessions SET status='error', summary='中斷：伺服器重啟', completed_at=? WHERE status='running'",
                (datetime.now().isoformat(),))
    con.commit()
    con.close()
    _expert_init_schedules()

def _expert_init_schedules():
    con = db()
    cur = con.cursor()
    for sched in EXPERT_SCHEDULES_DEFAULT:
        cur.execute("SELECT id FROM expert_schedules WHERE id=?", (sched["id"],))
        if not cur.fetchone():
            experts_json = json.dumps([r["id"] for r in EXPERT_ROLES])
            cur.execute("""INSERT INTO expert_schedules(id,name,time,timezone,description,enabled,
                           market_focus,horizon,day_of_week,day_of_month,experts,updated_at)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (sched["id"], sched["name"], sched["time"], sched.get("timezone","Asia/Taipei"),
                         sched["description"], int(sched.get("enabled",True)),
                         sched.get("market_focus","ALL"), sched.get("horizon","daily"),
                         sched.get("day_of_week"), sched.get("day_of_month"),
                         experts_json, datetime.now().isoformat()))
    con.commit()
    con.close()

_expert_db_migrate()

def _expert_get_config():
    con = db()
    cur = con.cursor()
    cur.execute("SELECT key, value FROM expert_config")
    cfg = {r[0]: r[1] for r in cur.fetchall()}
    con.close()
    return {
        "default_rounds": int(cfg.get("default_rounds", "1")),
        "default_horizon": cfg.get("default_horizon", "daily"),
        "ai_source": cfg.get("ai_source", "subscription"),
        "ai_model": cfg.get("ai_model", "claude-sonnet-4-6"),
        "notify_on_complete": cfg.get("notify_on_complete", "true") == "true",
    }

def _expert_gather_market_data(market: str, targets: list):
    """Gather current market data for expert analysis."""
    data = {}
    try:
        data["macro"] = get_macro_data()
    except:
        data["macro"] = {}
    try:
        data["risk"] = get_risk_level()
    except:
        data["risk"] = {}
    try:
        if market in ("TW", "ALL"):
            wl = _get_watchlist()
            data["tw_watchlist"] = [w for w in wl if not w.get("market") or w.get("market") == "TW"][:20]
        if market in ("US", "ALL"):
            data["us_watchlist"] = _get_us_watchlist()[:20]
    except:
        pass
    try:
        data["strategies"] = [{"id": s["id"], "name": s["name"], "direction": s["direction"],
                               "enabled": s["enabled"], "strat_type": s.get("strat_type","builtin")}
                              for s in STRATEGIES]
    except:
        pass
    if targets:
        target_data = []
        for t in targets[:10]:
            try:
                code = t.get("code", t) if isinstance(t, dict) else str(t)
                mkt = t.get("market", market) if isinstance(t, dict) else market
                scored = _ic_score_stock(code, mkt)
                target_data.append({"code": code, "market": mkt, "score": scored})
            except:
                target_data.append({"code": str(t), "error": "scoring failed"})
        data["targets"] = target_data
    return data

def _expert_build_prompt(expert: dict, market_data: dict, session_info: dict,
                         prev_opinions: list = None):
    """Build prompt for a single expert."""
    horizon_labels = {"daily": "今日/明日", "weekly": "本週", "monthly": "本月", "yearly": "本年度"}
    horizon = horizon_labels.get(session_info.get("horizon", "daily"), "今日/明日")
    targets_str = ""
    for t in market_data.get("targets", []):
        if "score" in t and isinstance(t["score"], dict):
            s = t["score"]
            targets_str += f"\n  {t['code']}({t['market']}): 評分{s.get('score','-')}/100, 方向{s.get('direction','?')}, 訊號:{s.get('signals','')}"
        else:
            targets_str += f"\n  {t.get('code','?')}: 資料不足"

    macro = market_data.get("macro", {})
    macro_str = json.dumps(macro, ensure_ascii=False, default=str)[:800] if macro else "無資料"
    risk = market_data.get("risk", {})
    risk_str = f"風險等級: {risk.get('level','?')}, 警報數: {risk.get('alert_count',0)}" if risk else "無資料"

    strats = market_data.get("strategies", [])
    enabled_buy = [s["id"] for s in strats if s["enabled"] and s["direction"] == "BUY"]
    enabled_sell = [s["id"] for s in strats if s["enabled"] and s["direction"] == "SELL"]

    prev_text = ""
    if prev_opinions:
        prev_text = "\n\n【前輪其他專家觀點】\n"
        for op in prev_opinions:
            prev_text += f"- {op['expert_name']}({op['expert_id']}): {op.get('direction','?')} 信心{op.get('confidence',0):.0%}\n"
            for kp in op.get("key_points", [])[:3]:
                prev_text += f"  . {kp}\n"

    prompt = f"""{expert['prompt_role']}

你正在參加一場「策略專家委員會」會議。

【會議資訊】
- 分析範圍: {session_info.get('market','ALL')}
- 時間維度: {horizon}
- 分析標的: {targets_str or '全面分析（無指定標的）'}

【市場環境】
- 總經數據: {macro_str}
- {risk_str}
- 啟用中的買進策略: {', '.join(enabled_buy)}
- 啟用中的賣出策略: {', '.join(enabled_sell)}
{prev_text}

請從你的專業角度({expert['perspective']})提供分析，回覆格式如下（JSON）：
{{
  "direction": "BULLISH/BEARISH/NEUTRAL",
  "confidence": 0.0~1.0,
  "key_points": ["要點1", "要點2", "要點3"],
  "strategy_suggestions": ["建議啟用/停用/調整的策略"],
  "risk_warnings": ["風險警示"],
  "horizon_outlook": {{
    "short_term": "1-3天展望",
    "medium_term": "1-4週展望",
    "long_term": "1-3月展望"
  }}
}}

請務必：
1. 只從你的專業角度發言，不要越界到其他專家的領域
2. 明確區分事實與推論
3. 給出具體可操作的建議，不要空泛的結論
4. confidence 要反映你對自己判斷的真實信心度"""

    return prompt

def _expert_parse_opinion(text: str):
    """Parse expert response, extract JSON."""
    try:
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            return json.loads(text[start:end])
    except:
        pass
    return {"direction": "NEUTRAL", "confidence": 0.3,
            "key_points": [text[:200]], "raw": text}

def _expert_build_consensus(opinions: list, session_info: dict):
    """Build consensus from all expert opinions."""
    if not opinions:
        return {"direction": "NEUTRAL", "confidence": 0, "summary": "No opinions gathered"}

    direction_scores = {"BULLISH": 0, "BEARISH": 0, "NEUTRAL": 0}
    total_conf = 0
    all_points = []
    all_risks = []
    all_suggestions = []
    outlooks = {"short_term": [], "medium_term": [], "long_term": []}

    for op in opinions:
        d = op.get("direction", "NEUTRAL")
        c = op.get("confidence", 0.5)
        direction_scores[d] = direction_scores.get(d, 0) + c
        total_conf += c
        all_points.extend(op.get("key_points", [])[:3])
        all_risks.extend(op.get("risk_warnings", [])[:2])
        all_suggestions.extend(op.get("strategy_suggestions", [])[:2])
        ho = op.get("horizon_outlook", {})
        for k in outlooks:
            if ho.get(k):
                outlooks[k].append(ho[k])

    winner = max(direction_scores, key=direction_scores.get)
    avg_conf = total_conf / len(opinions) if opinions else 0
    agreement = direction_scores[winner] / total_conf if total_conf else 0

    return {
        "direction": winner,
        "confidence": round(avg_conf, 2),
        "agreement": round(agreement, 2),
        "vote_detail": direction_scores,
        "key_points": all_points,
        "risk_warnings": list(set(all_risks)),
        "strategy_suggestions": list(set(all_suggestions)),
        "horizon_outlook": {k: "; ".join(v) for k, v in outlooks.items()},
    }

_expert_running = {}

def _ic_llm_call_with_meta(prompt: str, model: str, source: str, max_tokens: int = 2000):
    """Wrapper that returns (text, {input_tokens, output_tokens, elapsed_ms, model, source})."""
    settings = _ic_get_settings()
    source = (source or "api").lower()
    t0 = time.time()

    if source == "subscription":
        import subprocess, shutil
        cli = settings.get("claude_cli_path", "claude") or "claude"
        cli = shutil.which(cli) or (cli if os.path.isfile(cli) else None)
        if not cli:
            return "（找不到 claude CLI）", {"input_tokens":0,"output_tokens":0,"elapsed_ms":0,"model":model,"source":source}
        try:
            proc = subprocess.run(
                [cli, "-p", "--model", model, "--output-format", "json"],
                input=prompt, capture_output=True, text=True, encoding="utf-8", timeout=120)
            elapsed = int((time.time()-t0)*1000)
            if proc.returncode != 0:
                return f"（訂閱呼叫失敗：{(proc.stderr or proc.stdout or '').strip()[:200]}）", {"input_tokens":0,"output_tokens":0,"elapsed_ms":elapsed,"model":model,"source":source}
            data = json.loads(proc.stdout)
            text = data.get("result","") or ""
            usage = data.get("usage",{}) or {}
            inp, out = usage.get("input_tokens",0), usage.get("output_tokens",0)
            _ic_record_token_usage(model, inp, out, cost_override=0.0)
            return text or "（無回傳內容）", {"input_tokens":inp,"output_tokens":out,"elapsed_ms":elapsed,"model":model,"source":source}
        except subprocess.TimeoutExpired:
            return "（逾時）", {"input_tokens":0,"output_tokens":0,"elapsed_ms":120000,"model":model,"source":source}
        except Exception as e:
            return f"（例外：{e}）", {"input_tokens":0,"output_tokens":0,"elapsed_ms":int((time.time()-t0)*1000),"model":model,"source":source}

    api_key = settings.get("claude_api_key","")
    if not api_key:
        return "（需 API Key）", {"input_tokens":0,"output_tokens":0,"elapsed_ms":0,"model":model,"source":source}
    try:
        from anthropic import Anthropic
        client = Anthropic(api_key=api_key)
        msg = client.messages.create(model=model, max_tokens=max_tokens,
                                     messages=[{"role":"user","content":prompt}])
        elapsed = int((time.time()-t0)*1000)
        inp, out = msg.usage.input_tokens, msg.usage.output_tokens
        _ic_record_token_usage(model, inp, out)
        return msg.content[0].text, {"input_tokens":inp,"output_tokens":out,"elapsed_ms":elapsed,"model":model,"source":source}
    except Exception as e:
        return f"（失敗：{e}）", {"input_tokens":0,"output_tokens":0,"elapsed_ms":int((time.time()-t0)*1000),"model":model,"source":source}


def _expert_check_resources():
    """Check if system has enough resources to run an AI call."""
    import psutil
    mem = psutil.virtual_memory()
    if mem.available < 500 * 1024 * 1024:  # < 500MB available
        return False, f"RAM不足: 僅剩 {mem.available // (1024*1024)}MB (需至少500MB)"
    return True, "ok"

def _expert_run_session(session_id: int):
    """Run expert session in background thread. Calls experts sequentially with resource checks."""
    session_start = time.time()
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM expert_sessions WHERE id=?", (session_id,))
    row = cur.fetchone()
    if not row:
        con.close()
        return
    cols = [d[0] for d in cur.description]
    session = dict(zip(cols, row))
    con.close()

    experts_ids = json.loads(session.get("experts", "[]")) or [r["id"] for r in EXPERT_ROLES]
    experts = [r for r in EXPERT_ROLES if r["id"] in experts_ids]
    targets = json.loads(session.get("targets", "[]"))
    market = session.get("market", "ALL")
    rounds = session.get("rounds", 1)
    config = json.loads(session.get("config", "{}"))

    con = db()
    con.execute("UPDATE expert_sessions SET status='running' WHERE id=?", (session_id,))
    con.commit()
    con.close()

    try:
        market_data = _expert_gather_market_data(market, targets)

        ai_source = config.get("ai_source") or _expert_get_config().get("ai_source", "subscription")
        ai_model = config.get("ai_model") or _expert_get_config().get("ai_model", "claude-sonnet-4-6")

        all_opinions = []
        total_input_tokens = 0
        total_output_tokens = 0
        skipped = []

        for round_num in range(1, rounds + 1):
            prev_opinions = all_opinions if round_num > 1 else None
            round_opinions = []

            for i, expert in enumerate(experts):
                # Resource gate: check before each AI call
                try:
                    ok, reason = _expert_check_resources()
                    if not ok:
                        parsed = {"direction": "NEUTRAL", "confidence": 0,
                                  "key_points": [f"跳過: {reason}"], "error": reason}
                        raw = f"(skipped: {reason})"
                        meta = {"input_tokens":0,"output_tokens":0,"elapsed_ms":0,
                                "model":ai_model,"source":ai_source,"skipped":True,"skip_reason":reason}
                        skipped.append(expert["name"])
                        con = db()
                        con.execute("""INSERT INTO expert_opinions(session_id, expert_id, round_num, opinion,
                                       direction, confidence, key_points, created_at)
                                       VALUES(?,?,?,?,?,?,?,?)""",
                                    (session_id, expert["id"], round_num,
                                     json.dumps({"text": raw, "meta": meta}, ensure_ascii=False),
                                     "NEUTRAL", 0,
                                     json.dumps(parsed["key_points"], ensure_ascii=False),
                                     datetime.now().isoformat()))
                        con.commit(); con.close()
                        round_opinions.append({"expert_id": expert["id"], "expert_name": expert["name"],
                                               "expert_icon": expert["icon"], **parsed})
                        continue
                except ImportError:
                    pass

                prompt = _expert_build_prompt(expert, market_data,
                                              {"market": market, "horizon": session.get("horizon","daily")},
                                              prev_opinions)
                try:
                    raw, meta = _ic_llm_call_with_meta(prompt, model=ai_model, source=ai_source, max_tokens=2000)
                    parsed = _expert_parse_opinion(raw)
                except Exception as e:
                    parsed = {"direction": "NEUTRAL", "confidence": 0, "key_points": [f"AI call failed: {e}"], "error": str(e)}
                    raw = str(e)
                    meta = {"input_tokens":0,"output_tokens":0,"elapsed_ms":0,"model":ai_model,"source":ai_source}

                total_input_tokens += meta.get("input_tokens", 0)
                total_output_tokens += meta.get("output_tokens", 0)

                opinion_record = {
                    "expert_id": expert["id"],
                    "expert_name": expert["name"],
                    "expert_icon": expert["icon"],
                    **parsed,
                }
                round_opinions.append(opinion_record)

                con = db()
                con.execute("""INSERT INTO expert_opinions(session_id, expert_id, round_num, opinion,
                               direction, confidence, key_points, created_at)
                               VALUES(?,?,?,?,?,?,?,?)""",
                            (session_id, expert["id"], round_num,
                             json.dumps({"text": raw, "meta": meta}, ensure_ascii=False),
                             parsed.get("direction","NEUTRAL"), parsed.get("confidence",0.5),
                             json.dumps(parsed.get("key_points",[]), ensure_ascii=False),
                             datetime.now().isoformat()))
                con.commit()
                con.close()

                if i < len(experts) - 1:
                    time.sleep(2)

            all_opinions = round_opinions

            if round_num < rounds:
                time.sleep(3)

        total_elapsed_ms = int((time.time() - session_start) * 1000)
        consensus = _expert_build_consensus(all_opinions, session)
        summary = f"{consensus['direction']} (信心{consensus['confidence']:.0%}, 共識{consensus['agreement']:.0%})"
        if skipped:
            summary += f" [跳過: {','.join(skipped)}]"

        usage_meta = {
            "model": ai_model,
            "source": ai_source,
            "total_input_tokens": total_input_tokens,
            "total_output_tokens": total_output_tokens,
            "total_tokens": total_input_tokens + total_output_tokens,
            "total_elapsed_ms": total_elapsed_ms,
            "expert_count": len(experts),
            "rounds": rounds,
            "skipped": skipped,
        }
        config["usage"] = usage_meta

        con = db()
        con.execute("""UPDATE expert_sessions SET status='completed', summary=?, consensus=?,
                       config=?, completed_at=? WHERE id=?""",
                    (summary, json.dumps(consensus, ensure_ascii=False),
                     json.dumps(config, ensure_ascii=False),
                     datetime.now().isoformat(), session_id))
        con.commit()
        con.close()

    except Exception as e:
        con = db()
        con.execute("UPDATE expert_sessions SET status='error', summary=?, completed_at=? WHERE id=?",
                    (f"分析失敗: {e}", datetime.now().isoformat(), session_id))
        con.commit(); con.close()

    finally:
        _expert_running.pop(session_id, None)


@app.get("/api/expert/roles")
def expert_get_roles():
    return EXPERT_ROLES

@app.get("/api/expert/config")
def expert_get_config():
    cfg = _expert_get_config()
    cfg["roles"] = EXPERT_ROLES
    return cfg

@app.post("/api/expert/config")
def expert_save_config(req: dict, _: None = Depends(require_token)):
    con = db()
    for k, v in req.items():
        if k in ("default_rounds", "default_horizon", "ai_source", "ai_model", "notify_on_complete"):
            con.execute("INSERT OR REPLACE INTO expert_config(key,value) VALUES(?,?)", (k, str(v)))
    con.commit()
    con.close()
    return {"ok": True}

@app.get("/api/expert/schedules")
def expert_get_schedules():
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM expert_schedules ORDER BY time")
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    con.close()
    for r in rows:
        r["enabled"] = bool(r.get("enabled"))
        r["experts"] = json.loads(r.get("experts","[]"))
        r["notify"] = json.loads(r.get("notify","[]"))
    return rows

@app.put("/api/expert/schedules/{sid}")
def expert_update_schedule(sid: str, req: dict, _: None = Depends(require_token)):
    con = db()
    fields = []
    vals = []
    for k in ("name","time","timezone","description","enabled","market_focus","horizon",
              "day_of_week","day_of_month","experts","notify"):
        if k in req:
            fields.append(f"{k}=?")
            v = req[k]
            if k in ("experts","notify"):
                v = json.dumps(v, ensure_ascii=False)
            elif k == "enabled":
                v = int(v)
            vals.append(v)
    if fields:
        fields.append("updated_at=?")
        vals.append(datetime.now().isoformat())
        vals.append(sid)
        con.execute(f"UPDATE expert_schedules SET {','.join(fields)} WHERE id=?", vals)
        con.commit()
    con.close()
    return {"ok": True}

@app.get("/api/expert/sessions")
def expert_list_sessions(limit: int = 20, offset: int = 0):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM expert_sessions ORDER BY id DESC LIMIT ? OFFSET ?", (limit, offset))
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    cur.execute("SELECT COUNT(*) FROM expert_sessions")
    total = cur.fetchone()[0]
    con.close()
    for r in rows:
        r["targets"] = json.loads(r.get("targets","[]"))
        r["experts"] = json.loads(r.get("experts","[]"))
        r["consensus"] = json.loads(r.get("consensus","null")) if r.get("consensus") else None
        r["config"] = json.loads(r.get("config","{}"))
    return {"sessions": rows, "total": total}

@app.get("/api/expert/sessions/{sid}")
def expert_get_session(sid: int):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM expert_sessions WHERE id=?", (sid,))
    row = cur.fetchone()
    if not row:
        con.close()
        return {"error": "not found"}
    cols = [d[0] for d in cur.description]
    session = dict(zip(cols, row))
    session["targets"] = json.loads(session.get("targets","[]"))
    session["experts"] = json.loads(session.get("experts","[]"))
    session["consensus"] = json.loads(session.get("consensus","null")) if session.get("consensus") else None
    session["config"] = json.loads(session.get("config","{}"))

    cur.execute("SELECT * FROM expert_opinions WHERE session_id=? ORDER BY round_num, id", (sid,))
    ocols = [d[0] for d in cur.description]
    opinions = [dict(zip(ocols, r)) for r in cur.fetchall()]
    con.close()
    for op in opinions:
        op["key_points"] = json.loads(op.get("key_points","[]"))
        expert_def = next((e for e in EXPERT_ROLES if e["id"]==op["expert_id"]), {})
        op["expert_name"] = expert_def.get("name", op["expert_id"])
        op["expert_icon"] = expert_def.get("icon", "?")
        op["expert_color"] = expert_def.get("color", "#888")
    session["opinions"] = opinions
    return session

@app.post("/api/expert/sessions")
def expert_create_session(req: dict, _: None = Depends(require_token)):
    """Create and start a new expert session."""
    cfg = _expert_get_config()
    market = req.get("market", "ALL")
    targets = req.get("targets", [])
    horizon = req.get("horizon", cfg.get("default_horizon", "daily"))
    experts = req.get("experts") or [r["id"] for r in EXPERT_ROLES]
    rounds = req.get("rounds", cfg.get("default_rounds", 1))
    title = req.get("title", "")
    trigger = req.get("trigger", "manual")

    horizon_labels = {"daily": "日", "weekly": "週", "monthly": "月", "yearly": "年"}
    if not title:
        t_str = ",".join(str(t.get("code",t) if isinstance(t,dict) else t) for t in targets[:3]) or market
        title = f"{horizon_labels.get(horizon,horizon)}度分析 — {t_str}"

    config = {
        "ai_source": req.get("ai_source", cfg.get("ai_source","subscription")),
        "ai_model": req.get("ai_model", cfg.get("ai_model","claude-sonnet-4-6")),
    }

    con = db()
    cur = con.cursor()
    cur.execute("""INSERT INTO expert_sessions(title, market, targets, horizon, experts, status,
                   rounds, trigger, config, created_at) VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (title, market, json.dumps(targets, ensure_ascii=False),
                 horizon, json.dumps(experts), "pending",
                 rounds, trigger, json.dumps(config), datetime.now().isoformat()))
    session_id = cur.lastrowid
    con.commit()
    con.close()

    t = threading.Thread(target=_expert_run_session, args=(session_id,), daemon=True)
    t.start()
    _expert_running[session_id] = t

    return {"ok": True, "session_id": session_id, "status": "running"}

@app.delete("/api/expert/sessions/{sid}")
def expert_delete_session(sid: int, _: None = Depends(require_token)):
    con = db()
    con.execute("DELETE FROM expert_opinions WHERE session_id=?", (sid,))
    con.execute("DELETE FROM expert_sessions WHERE id=?", (sid,))
    con.commit()
    con.close()
    return {"ok": True}


# ── 後端重啟 ─────────────────────────────────────

@app.post("/api/admin/restart")
def admin_restart(_: None = Depends(require_token)):
    """重啟後端：寫一個 bat 等舊進程退出後再啟動新的"""
    import subprocess, sys, tempfile
    script_path = os.path.abspath(__file__)
    py_path = sys.executable
    work_dir = os.path.dirname(script_path)
    bat = os.path.join(tempfile.gettempdir(), "_restart_backend.bat")
    with open(bat, "w") as f:
        f.write(f'@echo off\ntimeout /t 2 /nobreak >nul\ncd /d "{work_dir}"\nstart "" "{py_path}" "{script_path}"\n')
    subprocess.Popen(["cmd", "/c", bat], creationflags=subprocess.CREATE_NO_WINDOW)
    threading.Thread(target=lambda: (time.sleep(0.5), os._exit(0)), daemon=True).start()
    return {"ok": True, "msg": "後端重啟中，請稍候…"}

# ══════════════════════════════════════════════════
# Phase 2 + Phase 3 增強 API
# ══════════════════════════════════════════════════

# ── 跨市場金額換算：美元金額一律換成台幣再加總 ─────────
#  bug 根因：台股(台幣) 與 美股(美金) market_value 直接相加算 % → 佔比錯。
#  匯率取 macro 快取的 USDTWD（本地計算、零 LLM、不阻塞：只讀快取，無快取用保守 fallback）。
_USDTWD_FALLBACK = 31.8
def _get_usdtwd() -> float:
    try:
        c = _IC_MACRO_CACHE or {}
        v = (c.get("TWD") or {}).get("price")
        if v is not None and 20 < float(v) < 45:   # sanity bound，擋掉異常值
            return float(v)
    except Exception:
        pass
    return _USDTWD_FALLBACK

# ── 2.1 持倉集中度 ─────────────────────────────────
@app.get("/api/portfolio/concentration")
def portfolio_concentration():
    positions = get_positions()
    if isinstance(positions, dict):
        positions = positions.get("data", positions.get("results", []))
    fx = _get_usdtwd()  # 美元→台幣；跨市場金額一律換算成台幣再加總（修：原本台幣+美金直接相加）
    total_mv = 0
    items = []
    by_market = {}
    for p in positions:
        cp = p.get("current_price", 0) or 0
        shares = p.get("shares", 0) or 0
        mkt = p.get("market", "TW")
        mv_native = cp * shares
        mv = mv_native * fx if str(mkt).upper() == "US" else mv_native  # 統一台幣計價
        total_mv += mv
        by_market[mkt] = by_market.get(mkt, 0) + mv
        items.append({"code": p["code"], "name": p.get("name", ""), "market": mkt,
                      "market_value": round(mv, 0), "market_value_native": round(mv_native, 0),
                      "shares": shares, "current_price": cp})
    if total_mv == 0:
        return {"total_market_value": 0, "data": [], "by_market": {}, "max_single_stock": None,
                "currency": "TWD", "fx_usdtwd": fx}
    for it in items:
        it["weight_pct"] = round(it["market_value"] / total_mv * 100, 2)
        it["alert"] = f"超過 20% 上限" if it["weight_pct"] > 20 else None
    items.sort(key=lambda x: x["weight_pct"], reverse=True)
    by_market_pct = {k: round(v / total_mv * 100, 1) for k, v in by_market.items()}
    return {
        "total_market_value": round(total_mv, 0),
        "currency": "TWD",
        "fx_usdtwd": fx,
        "data": items,
        "by_market": by_market_pct,
        "max_single_stock": {"code": items[0]["code"], "weight_pct": items[0]["weight_pct"]} if items else None,
    }

# ── 2.2 籌碼歷史趨勢（已有 /api/chip/{code}?days=N，擴展 summary）──
@app.get("/api/chip/{code}/trend")
def chip_trend(code: str, days: int = 20):
    history = get_chip_history(code, days)
    if not history:
        return {"code": code, "history": [], "summary": {}}
    foreign_vals = [h.get("foreign_buy", 0) or 0 for h in history]
    trust_vals = [h.get("itrust_buy", 0) or 0 for h in history]
    consec_buy = 0
    for v in reversed(foreign_vals):
        if v > 0:
            consec_buy += 1
        else:
            break
    return {
        "code": code,
        "history": history,
        "summary": {
            "foreign_consecutive_buy_days": consec_buy,
            "foreign_5d_total": sum(foreign_vals[-5:]),
            "foreign_20d_total": sum(foreign_vals[-20:]),
            "trust_5d_total": sum(trust_vals[-5:]),
            "trust_20d_total": sum(trust_vals[-20:]),
        }
    }

# ── 2.3 經濟數據/財報日曆 ──────────────────────────
@app.get("/api/calendar/events")
def calendar_events(days: int = 14):
    events = []
    today = datetime.now()
    end = today + timedelta(days=days)
    con = db(); cur = con.cursor()
    cur.execute("SELECT DISTINCT code FROM watchlist")
    wl_codes = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT DISTINCT code FROM positions WHERE status='open'")
    pos_codes = [r[0] for r in cur.fetchall()]
    con.close()
    all_codes = list(set(wl_codes + pos_codes))
    us_codes = [c for c in all_codes if not c.isdigit()]
    for code in us_codes[:20]:
        try:
            import yfinance as yf
            tk = yf.Ticker(code)
            cal = tk.calendar
            if cal is not None:
                if isinstance(cal, pd.DataFrame):
                    if "Earnings Date" in cal.index:
                        ed = cal.loc["Earnings Date"]
                        for dt_val in ed:
                            if hasattr(dt_val, 'strftime'):
                                dt_str = dt_val.strftime("%Y-%m-%d")
                                if today.strftime("%Y-%m-%d") <= dt_str <= end.strftime("%Y-%m-%d"):
                                    events.append({"date": dt_str, "type": "earnings", "code": code, "name": f"{code} Earnings"})
                elif isinstance(cal, dict):
                    for ed in cal.get("Earnings Date", []):
                        if hasattr(ed, 'strftime'):
                            dt_str = ed.strftime("%Y-%m-%d")
                            if today.strftime("%Y-%m-%d") <= dt_str <= end.strftime("%Y-%m-%d"):
                                events.append({"date": dt_str, "type": "earnings", "code": code, "name": f"{code} Earnings"})
        except Exception:
            pass
    events.sort(key=lambda x: x["date"])
    return {"data": events, "range": {"start": today.strftime("%Y-%m-%d"), "end": end.strftime("%Y-%m-%d")}}

# ── 2.4 組合損益 Dashboard ─────────────────────────
@app.get("/api/portfolio/summary")
def portfolio_summary():
    positions = get_positions()
    if isinstance(positions, dict):
        positions = positions.get("data", positions.get("results", []))
    fx = _get_usdtwd()  # 跨市場金額換算成台幣再加總（修：原本台幣+美金直接相加）
    total_invested = 0
    total_mv = 0
    by_market = {}
    top_winner = None
    top_loser = None
    for p in positions:
        cp = p.get("current_price", 0) or 0
        shares = p.get("shares", 0) or 0
        cost = p.get("cost", 0) or 0
        rate = fx if str(p.get("market", "TW")).upper() == "US" else 1.0  # 美股換台幣
        mv = cp * shares * rate
        invested = cost * shares * rate
        total_mv += mv
        total_invested += invested
        mkt = p.get("market", "TW")
        if mkt not in by_market:
            by_market[mkt] = {"value": 0, "invested": 0}
        by_market[mkt]["value"] += mv
        by_market[mkt]["invested"] += invested
        pnl_pct = p.get("pnl_pct", 0)
        if top_winner is None or pnl_pct > top_winner.get("pnl_pct", -999):
            top_winner = {"code": p["code"], "name": p.get("name", ""), "pnl_pct": pnl_pct}
        if top_loser is None or pnl_pct < top_loser.get("pnl_pct", 999):
            top_loser = {"code": p["code"], "name": p.get("name", ""), "pnl_pct": pnl_pct}
    total_pnl = total_mv - total_invested
    total_pnl_pct = round(total_pnl / total_invested * 100, 2) if total_invested > 0 else 0
    by_market_out = {}
    for mkt, vals in by_market.items():
        pnl_pct_m = round((vals["value"] / vals["invested"] - 1) * 100, 2) if vals["invested"] > 0 else 0
        by_market_out[mkt.lower()] = {"value": round(vals["value"], 0), "pnl_pct": pnl_pct_m}
    return {
        "total_equity": round(total_mv, 0),
        "currency": "TWD",
        "fx_usdtwd": fx,
        "invested": round(total_invested, 0),
        "total_pnl": round(total_pnl, 0),
        "total_pnl_pct": total_pnl_pct,
        "positions_count": len(positions),
        "top_winner": top_winner,
        "top_loser": top_loser,
        **by_market_out,
    }

# ── 3.1 因子回測 ──────────────────────────────────
@app.post("/api/ic/factor-backtest")
def ic_factor_backtest(data: dict, _: None = Depends(require_token)):
    market = data.get("market", "TW")
    factor = data.get("factor", "momentum")
    top_pct = data.get("top_pct", 20) / 100
    rebalance = data.get("rebalance", "monthly")
    start = data.get("start", "2021-01-01")
    end = data.get("end", datetime.now().strftime("%Y-%m-%d"))
    capital = data.get("initial_capital", 1000000)

    con = db(); cur = con.cursor()
    if market == "TW":
        cur.execute("SELECT code FROM watchlist WHERE market='TW'")
    else:
        cur.execute("SELECT code FROM watchlist WHERE market='US'")
    codes = [r[0] for r in cur.fetchall()]
    con.close()
    if len(codes) < 5:
        return {"data": [], "summary": {"error": "需至少5支股票"}}

    result = _multi_factor_score(codes, market)
    n_top = max(1, int(len(result) * top_pct))
    long_codes = [r["code"] for r in result[:n_top]]
    short_codes = [r["code"] for r in result[-n_top:]]

    long_bt = _run_backtest({"codes": long_codes, "market": market, "start": start, "end": end,
                             "strategies": ["MACD_CROSS", "MA_ALIGN", "KD_CROSS", "EXIT_C", "EXIT_D"],
                             "initial_capital": capital // 2})
    short_bt = _run_backtest({"codes": short_codes, "market": market, "start": start, "end": end,
                              "strategies": ["MACD_CROSS", "MA_ALIGN", "KD_CROSS", "EXIT_C", "EXIT_D"],
                              "initial_capital": capital // 2})

    long_ret = long_bt.get("summary", {}).get("total_return_pct", 0)
    short_ret = short_bt.get("summary", {}).get("total_return_pct", 0)
    spread = round(long_ret - short_ret, 2)

    return {
        "data": {
            "long": {"codes": long_codes, "return_pct": long_ret,
                     "trades": long_bt.get("summary", {}).get("total_trades", 0),
                     "sharpe": long_bt.get("summary", {}).get("sharpe_ratio", 0)},
            "short": {"codes": short_codes, "return_pct": short_ret,
                      "trades": short_bt.get("summary", {}).get("total_trades", 0),
                      "sharpe": short_bt.get("summary", {}).get("sharpe_ratio", 0)},
            "spread_pct": spread,
        },
        "factor": factor, "market": market, "top_pct": data.get("top_pct", 20),
    }

# ── 3.2 歷史指標時序 ──────────────────────────────
@app.get("/api/kbars/{code}/indicators/history")
def kbar_indicators_history(code: str, days: int = 60, market: str = "TW"):
    con = db(); cur = con.cursor()
    cur.execute(
        "SELECT date_key, open, high, low, close, volume FROM kbar_cache WHERE code=? AND tf='D' ORDER BY date_key",
        (code,))
    rows = cur.fetchall(); con.close()
    if len(rows) < 14:
        return {"code": code, "data": []}

    closes = [r[4] for r in rows]
    highs = [r[2] for r in rows]
    lows = [r[3] for r in rows]
    dates = [r[0] for r in rows]
    n = len(closes)

    def ema_series(data, period):
        k = 2 / (period + 1)
        e = [data[0]]
        for d in data[1:]:
            e.append(d * k + e[-1] * (1 - k))
        return e

    ema12 = ema_series(closes, 12) if n >= 12 else [0]*n
    ema26 = ema_series(closes, 26) if n >= 26 else [0]*n
    dif_line = [ema12[i] - ema26[i] for i in range(n)] if n >= 26 else [0]*n
    dea_line = ema_series(dif_line, 9) if n >= 26 else [0]*n

    k_val, d_val = 50.0, 50.0
    k_series, d_series = [], []
    for i in range(n):
        h_sl = highs[max(0, i-8):i+1]
        l_sl = lows[max(0, i-8):i+1]
        hh = max(h_sl) if h_sl else closes[i]
        ll = min(l_sl) if l_sl else closes[i]
        rsv = (closes[i] - ll) / (hh - ll) * 100 if hh != ll else 50
        k_val = rsv * (1/3) + k_val * (2/3)
        d_val = k_val * (1/3) + d_val * (2/3)
        k_series.append(k_val)
        d_series.append(d_val)

    rsi_series = [50.0] * n
    for i in range(14, n):
        gains = [max(0, closes[j] - closes[j-1]) for j in range(i-13, i+1)]
        losses = [max(0, closes[j-1] - closes[j]) for j in range(i-13, i+1)]
        ag = sum(gains) / 14
        al = sum(losses) / 14
        rsi_series[i] = round(100 - 100 / (1 + ag / al), 2) if al > 0 else 100

    result = []
    start_idx = max(0, n - days)
    for i in range(start_idx, n):
        pt = {"date": dates[i], "close": closes[i]}
        pt["rsi"] = round(rsi_series[i], 2)
        pt["k_val"] = round(k_series[i], 2)
        pt["d_val"] = round(d_series[i], 2)
        if n >= 26:
            pt["macd_dif"] = round(dif_line[i], 2)
            pt["macd_dea"] = round(dea_line[i], 2)
            pt["macd_hist"] = round(2 * (dif_line[i] - dea_line[i]), 2)
        result.append(pt)

    return {"code": code, "data": result}

# ── 3.3 interpretation ↔ risk-level 文件化 ─────────
@app.get("/api/risk-level/detail")
def risk_level_detail():
    base = get_risk_level()
    macro = _ic_interpret_macro()
    interpretation = macro.get("interpretation", "UNKNOWN") if isinstance(macro, dict) else "UNKNOWN"
    mapping = {
        "RISK_ON": {"expected_level": "NORMAL", "expected_scale": 100, "description": "多頭友善，全額操作"},
        "NEUTRAL": {"expected_level": "NORMAL", "expected_scale": 100, "description": "中性觀望"},
        "CAUTIOUS": {"expected_level": "CAUTION", "expected_scale": 60, "description": "謹慎操作，倉位降至60%"},
        "RISK_OFF": {"expected_level": "ALERT", "expected_scale": 30, "description": "空頭防禦，倉位降至30%，封鎖買進"},
    }
    base["macro_interpretation"] = interpretation
    base["mapping"] = mapping
    base["current_mapping"] = mapping.get(interpretation, {"description": "未知狀態"})
    return base

# ── 3.4 清理測試資料 ──────────────────────────────
@app.post("/api/trade-records/cleanup-test")
def cleanup_test_records(_: None = Depends(require_token)):
    con = db(); cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM trade_records WHERE code IN ('TEST1','ZZZZ','TEST','TEST2')")
    count = cur.fetchone()[0]
    if count > 0:
        cur.execute("DELETE FROM trade_records WHERE code IN ('TEST1','ZZZZ','TEST','TEST2')")
        con.commit()
    con.close()
    return {"ok": True, "deleted": count, "message": f"清除 {count} 筆測試資料"}

# ══════════════════════════════════════════════════
# 研究報告中心 API
# ══════════════════════════════════════════════════

_AIF_ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "..", "ai-investment-firm")
if not os.path.isdir(_AIF_ROOT):
    _AIF_ROOT = r"C:\Users\ychsu\Documents\Claude_Files\ai-investment-firm"

_REPORT_CACHE: dict = {}
_REPORT_CACHE_TS: float = 0
_REPORT_CACHE_TTL = 300

def _parse_frontmatter(filepath: str) -> dict:
    """解析 .md 檔案的 YAML frontmatter"""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read(4096)
        if not content.startswith("---"):
            return {}
        end = content.find("---", 3)
        if end < 0:
            return {}
        fm_text = content[3:end].strip()
        fm = {}
        for line in fm_text.split("\n"):
            if ":" not in line:
                continue
            key, val = line.split(":", 1)
            key = key.strip(); val = val.strip()
            if val.startswith("[") and val.endswith("]"):
                val = [v.strip().strip("'\"") for v in val[1:-1].split(",") if v.strip()]
            elif val.lower() in ("true", "false"):
                val = val.lower() == "true"
            fm[key] = val
        return fm
    except Exception:
        return {}

def _dir_to_category(subdir: str) -> str:
    mapping = {"reports": "research", "methodology": "methodology", "predictions": "prediction",
               "rulebook": "rulebook", "qa": "qa", "weekly": "weekly"}
    return mapping.get(subdir, "charter")

def _scan_reports() -> list:
    global _REPORT_CACHE, _REPORT_CACHE_TS
    now = time.time()
    if _REPORT_CACHE and now - _REPORT_CACHE_TS < _REPORT_CACHE_TTL:
        return _REPORT_CACHE.get("reports", [])

    # 1. 載入 MANIFEST 作為覆寫層
    manifest_map = {}
    manifest_path = os.path.join(_AIF_ROOT, "MANIFEST.json")
    if os.path.isfile(manifest_path):
        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
            for r in manifest.get("reports", []):
                if r.get("id"):
                    manifest_map[r["id"]] = r
                if r.get("path"):
                    manifest_map["path:" + r["path"]] = r
        except Exception as e:
            print(f"[ReportHub] MANIFEST parse error: {e}")

    # 2. 遞迴掃描所有 .md
    reports = []
    seen_ids = set()
    for root, dirs, files in os.walk(_AIF_ROOT):
        dirs[:] = [d for d in dirs if d not in (".git", "__pycache__", "node_modules")]
        for fname in files:
            if not fname.endswith(".md"):
                continue
            full_path = os.path.join(root, fname)
            rel_path = os.path.relpath(full_path, _AIF_ROOT).replace("\\", "/")
            fm = _parse_frontmatter(full_path)
            if not fm.get("aif_report"):
                # 沒有 aif_report: true，但 MANIFEST 有列 → 也納入
                if "path:" + rel_path not in manifest_map:
                    continue

            # 從 frontmatter 建立基本 metadata
            rid = fm.get("id") or fname.replace(".md", "").lstrip("0123456789-")
            subdir = rel_path.split("/")[0] if "/" in rel_path else ""
            # 從檔名取日期
            import re
            date_match = re.match(r"(\d{4}-\d{2}-\d{2})", fname)
            file_date = date_match.group(1) if date_match else ""

            report = {
                "id": rid,
                "title": fm.get("title") or fname.replace(".md", ""),
                "category": fm.get("category") or _dir_to_category(subdir),
                "path": rel_path,
                "date": fm.get("date") or file_date or datetime.fromtimestamp(os.path.getmtime(full_path)).strftime("%Y-%m-%d"),
                "status": fm.get("status") or "active",
                "tags": fm.get("tags") if isinstance(fm.get("tags"), list) else [],
                "summary": fm.get("summary") or "",
                "supersedes": fm.get("supersedes"),
                "exists": True,
                "mtime": os.path.getmtime(full_path),
                "size": os.path.getsize(full_path),
            }

            # MANIFEST 覆寫（精確標題/摘要/supersedes 等）
            m_entry = manifest_map.get(rid) or manifest_map.get("path:" + rel_path)
            if m_entry:
                for k in ("title", "summary", "category", "tags", "supersedes", "status"):
                    if m_entry.get(k) is not None:
                        report[k] = m_entry[k]

            if rid not in seen_ids:
                reports.append(report)
                seen_ids.add(rid)

    reports.sort(key=lambda x: x.get("date", ""), reverse=True)
    _REPORT_CACHE = {"reports": reports}
    _REPORT_CACHE_TS = now
    return reports

@app.get("/api/reports")
def list_reports(category: str = "", status: str = "active", q: str = "", tag: str = ""):
    reports = _scan_reports()
    filtered = []
    for r in reports:
        if status and r.get("status") != status:
            continue
        if category and r.get("category") != category:
            continue
        if tag and tag not in r.get("tags", []):
            continue
        if q:
            q_lower = q.lower()
            searchable = f"{r.get('title','')} {r.get('summary','')} {' '.join(r.get('tags',[]))}".lower()
            if q_lower not in searchable:
                continue
        filtered.append(r)
    filtered.sort(key=lambda x: x.get("date", ""), reverse=True)
    cats = {}
    for r in _scan_reports():
        c = r.get("category", "other")
        cats[c] = cats.get(c, 0) + 1
    return {"data": filtered, "total": len(filtered), "categories": cats}

@app.get("/api/reports/{report_id}")
def get_report(report_id: str):
    reports = _scan_reports()
    for r in reports:
        if r["id"] == report_id:
            full_path = os.path.join(_AIF_ROOT, r["path"])
            if not os.path.isfile(full_path):
                return JSONResponse({"error": "檔案不存在"}, status_code=404)
            with open(full_path, "r", encoding="utf-8") as f:
                content = f.read()
            superseded_by = None
            if r.get("status") == "superseded":
                for r2 in reports:
                    if r2.get("supersedes") == report_id:
                        superseded_by = r2["id"]
                        break
            return {"report": r, "content": content, "superseded_by": superseded_by}
    return JSONResponse({"error": "報告不存在"}, status_code=404)

@app.get("/api/reports/search/fulltext")
def search_reports(q: str = ""):
    if not q:
        return {"data": [], "total": 0}
    reports = _scan_reports()
    results = []
    q_lower = q.lower()
    for r in reports:
        full_path = os.path.join(_AIF_ROOT, r["path"])
        if not os.path.isfile(full_path):
            continue
        try:
            with open(full_path, "r", encoding="utf-8") as f:
                content = f.read()
            if q_lower in content.lower():
                idx = content.lower().index(q_lower)
                snippet = content[max(0, idx-50):idx+len(q)+50]
                results.append({**r, "snippet": f"...{snippet}..."})
        except Exception:
            pass
    return {"data": results, "total": len(results)}

@app.post("/api/reports/rescan")
def rescan_reports():
    global _REPORT_CACHE_TS
    _REPORT_CACHE_TS = 0
    reports = _scan_reports()
    return {"ok": True, "count": len(reports)}

# ══════════════════════════════════════════════════
# S-05 / S-06 / S-16 — 4-Regime 環境分類
# ══════════════════════════════════════════════════

# 判斷樹版本：S-05(當日)與 S-16(歷史)必須共用同一版本字串，
# 改規則時一併更新，確保歷史與當日一致。
_REGIME_RULE_VERSION = "4regime-v1"


def _classify_regime(vix_val, daily_change_pct, latest_close, ma60, atr20, atr60, ma20):
    """4-regime 判斷樹（單一真實來源，S-05 當日與 S-16 歷史共用）。回傳 (regime, reason)。"""
    if (vix_val and vix_val > 35) or daily_change_pct < -3:
        reason = f"VIX={vix_val:.1f}>35" if (vix_val and vix_val > 35) else f"daily_change={daily_change_pct:.1f}%<-3%"
        return "CRISIS", reason
    elif vix_val and vix_val > 25 and latest_close < ma60:
        return "RISK_OFF", f"VIX={vix_val:.1f}>25 AND close<MA60({ma60:.0f})"
    elif atr60 and (atr20 / atr60 < 0.8) and ma20 and (abs(latest_close - ma20) / ma20 < 0.03):
        return "MEAN_REVERT", f"ATR20/ATR60={atr20/atr60:.2f}<0.8 AND |close-MA20|/MA20={abs(latest_close-ma20)/ma20:.3f}<0.03"
    else:
        return "TREND_UP", "default (no crisis/risk_off/mean_revert conditions met)"


def _calc_regime(market: str = "TW", target_date: str = "") -> dict:
    """計算指定日期的 4-regime 環境分類。"""
    index_code = "^TWII" if market.upper() == "TW" else "^GSPC"
    vix_code = "^VIX"
    if not target_date:
        target_date = datetime.now().strftime("%Y-%m-%d")

    con = market_db()
    cur = con.cursor()

    # 取目標日往前 80 個交易日的資料（算 MA60 + ATR60 需要）
    cur.execute("""
        SELECT date, open, high, low, close FROM daily_kbar
        WHERE code=? AND market='INDEX' AND date <= ?
        ORDER BY date DESC LIMIT 80
    """, (index_code, target_date))
    rows = cur.fetchall()

    cur.execute("""
        SELECT date, close FROM daily_kbar
        WHERE code=? AND market='INDEX' AND date <= ?
        ORDER BY date DESC LIMIT 5
    """, (vix_code, target_date))
    vix_rows = cur.fetchall()
    con.close()

    if len(rows) < 60:
        return {"regime": "UNKNOWN", "reason": f"insufficient data for {index_code} ({len(rows)} bars, need 60)", "date": target_date}

    rows.reverse()
    closes = [r[4] for r in rows]
    highs = [r[2] for r in rows]
    lows = [r[3] for r in rows]

    # MA
    ma20 = sum(closes[-20:]) / 20
    ma60 = sum(closes[-60:]) / 60

    # ATR (True Range)
    def calc_atr(n):
        trs = []
        for i in range(max(1, len(rows) - n), len(rows)):
            h, l, pc = highs[i], lows[i], closes[i - 1]
            trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        return sum(trs) / len(trs) if trs else 0

    atr20 = calc_atr(20)
    atr60 = calc_atr(60)

    # 大盤最新收盤 & 單日跌幅
    latest_close = closes[-1]
    prev_close = closes[-2] if len(closes) >= 2 else latest_close
    daily_change_pct = (latest_close - prev_close) / prev_close * 100 if prev_close else 0

    # VIX
    vix_val = vix_rows[0][1] if vix_rows else None

    # 4-regime 判斷
    inputs = {
        "index": index_code, "close": round(latest_close, 2),
        "ma20": round(ma20, 2), "ma60": round(ma60, 2),
        "atr20": round(atr20, 2), "atr60": round(atr60, 2),
        "atr_ratio": round(atr20 / atr60, 3) if atr60 else None,
        "daily_change_pct": round(daily_change_pct, 2),
        "deviation_from_ma20": round(abs(latest_close - ma20) / ma20, 4) if ma20 else None,
        "vix": round(vix_val, 2) if vix_val else None,
    }

    regime, reason = _classify_regime(vix_val, daily_change_pct, latest_close, ma60, atr20, atr60, ma20)

    return {"regime": regime, "reason": reason, "inputs": inputs,
            "date": target_date, "market": market.upper(), "rule_version": _REGIME_RULE_VERSION}


@app.get("/api/regime")
def get_regime(market: str = "TW", date: str = ""):
    try:
        return _calc_regime(market, date)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


def _batch_calc_regime(market: str, start: str, end: str) -> list:
    """批次算歷史 regime，一次載入全部資料，共用 _calc_regime 判斷樹。"""
    index_code = "^TWII" if market.upper() == "TW" else "^GSPC"
    vix_code = "^VIX"

    con = market_db()
    cur = con.cursor()

    # 往前多拉 120 天（MA60 + buffer）
    from datetime import timedelta as _td
    pre_start = (datetime.strptime(start, "%Y-%m-%d") - _td(days=120)).strftime("%Y-%m-%d")

    cur.execute("""
        SELECT date, open, high, low, close FROM daily_kbar
        WHERE code=? AND market='INDEX' AND date BETWEEN ? AND ?
        ORDER BY date
    """, (index_code, pre_start, end))
    idx_rows = cur.fetchall()

    cur.execute("""
        SELECT date, close FROM daily_kbar
        WHERE code=? AND market='INDEX' AND date BETWEEN ? AND ?
        ORDER BY date
    """, (vix_code, pre_start, end))
    vix_map = {r[0]: r[1] for r in cur.fetchall()}
    con.close()

    if len(idx_rows) < 60:
        return []

    results = []
    for i in range(60, len(idx_rows)):
        row = idx_rows[i]
        d = row[0]
        if d < start:
            continue

        closes = [r[4] for r in idx_rows[max(0, i-59):i+1]]
        highs = [r[2] for r in idx_rows[max(0, i-59):i+1]]
        lows = [r[3] for r in idx_rows[max(0, i-59):i+1]]

        ma20 = sum(closes[-20:]) / 20
        ma60 = sum(closes) / len(closes) if len(closes) >= 60 else sum(closes) / len(closes)

        # ATR
        def _atr(n, end_idx):
            trs = []
            src = idx_rows[max(0, end_idx - n):end_idx + 1]
            for j in range(1, len(src)):
                h, l, pc = src[j][2], src[j][3], src[j-1][4]
                trs.append(max(h - l, abs(h - pc), abs(l - pc)))
            return sum(trs) / len(trs) if trs else 0

        atr20 = _atr(20, i)
        atr60 = _atr(60, i)

        latest_close = closes[-1]
        prev_close = closes[-2] if len(closes) >= 2 else latest_close
        daily_change_pct = (latest_close - prev_close) / prev_close * 100 if prev_close else 0
        vix_val = vix_map.get(d)

        # 共用判斷樹（與 S-05 當日完全同源）
        regime, _ = _classify_regime(vix_val, daily_change_pct, latest_close, ma60, atr20, atr60, ma20)

        # D-S16-FIELDS: per-row inputs（讓 AIF 能獨立驗證分類、做門檻敏感度校準）
        results.append({
            "date": d, "regime": regime,
            "vix": round(vix_val, 2) if vix_val else None,
            "close": round(latest_close, 2),
            "ma20": round(ma20, 2), "ma60": round(ma60, 2),
            "atr_ratio": round(atr20 / atr60, 3) if atr60 else None,
            "deviation_from_ma20": round(abs(latest_close - ma20) / ma20, 4) if ma20 else None,
            "daily_change_pct": round(daily_change_pct, 2),
        })

    return results


@app.get("/api/regime/history")
def get_regime_history(market: str = "TW", start: str = "2020-01-01", end: str = ""):
    """S-16: 歷史 regime + summary 統計（頻率、平均持續天數、切換次數、當前已持續天數）"""
    if not end:
        end = datetime.now().strftime("%Y-%m-%d")

    history = _batch_calc_regime(market, start, end)
    total_days = len(history)

    # distribution
    regime_counts = {}
    for r in history:
        regime_counts[r["regime"]] = regime_counts.get(r["regime"], 0) + 1

    # frequency %
    frequency = {k: round(v / total_days * 100, 1) if total_days else 0 for k, v in regime_counts.items()}

    # streaks: 連續同 regime 的段
    streaks = []  # [(regime, length)]
    if history:
        cur_regime = history[0]["regime"]
        cur_len = 1
        for i in range(1, len(history)):
            if history[i]["regime"] == cur_regime:
                cur_len += 1
            else:
                streaks.append((cur_regime, cur_len))
                cur_regime = history[i]["regime"]
                cur_len = 1
        streaks.append((cur_regime, cur_len))

    # 切換次數
    transitions = len(streaks) - 1 if len(streaks) > 1 else 0

    # 各 regime 平均持續天數
    avg_duration = {}
    for reg in ["TREND_UP", "MEAN_REVERT", "RISK_OFF", "CRISIS"]:
        durations = [s[1] for s in streaks if s[0] == reg]
        avg_duration[reg] = round(sum(durations) / len(durations), 1) if durations else 0

    # 當前已持續天數
    current_regime = streaks[-1][0] if streaks else None
    current_streak_days = streaks[-1][1] if streaks else 0

    summary = {
        "total_days": total_days,
        "distribution": regime_counts,
        "frequency_pct": frequency,
        "avg_duration_days": avg_duration,
        "transitions": transitions,
        "current_regime": current_regime,
        "current_streak_days": current_streak_days,
    }

    return {
        "market": market.upper(), "start": start, "end": end,
        "rule_version": _REGIME_RULE_VERSION,
        "summary": summary,
        "history": history,
    }


# ══════════════════════════════════════════════════
# S-15 — Universe Base Rate 一鍵計算
# ══════════════════════════════════════════════════

_BASE_RATE_THRESHOLDS = {
    # 正期望值閘（取代舊勝率 CI>50 語義；勝率降為族別分類）
    "min_N": 30,            # 有理論
    "min_N_no_theory": 50,  # 無理論（本批 12 策略皆有理論，預設走 30）
    "avg_ret_ci_lower_gt": 0.0,   # 取代 ci_lower_gt:50；改用 avg_return 95%CI 下界 > 0
    "min_pl": 1.2,
    # beta 濾網（S-03+，ALPHA 升級關）
    "subperiod_pos_frac_min": 0.60,
    "subperiod_min_trades": 5,
    "subperiod_min_buckets": 3,
    "excess_ci_lower_gt": 0.0,
}

_BASE_RATE_JOBS: dict = {}  # job_id -> {status, progress, total, rows, errors, cancelled, ...}

# base rate 只測「回測引擎真的有實作觸發碼」的進場策略。
# 排除 LOCK_BUY/SQUEEZE_BUY/SENTIMENT_REVERSAL/WIFE_SIMPLE：
#   前兩者需融券/籌碼，SENTIMENT_REVERSAL 需情緒數據，WIFE_SIMPLE 為 UI 便利策略，
#   皆未接進 OHLCV 回測迴圈（N 恆 0，非 bug）。
_BASE_RATE_BUY_IMPL = [
    "BUY_A", "BUY_B", "LOW_BUY", "SQUEEZE_BREAK", "KD_CROSS", "MACD_CROSS",
    "RSI_EXTREME", "MA_ALIGN", "DONCHIAN_BREAK", "MA_PULLBACK", "BB_SQUEEZE", "VOL_BREAKOUT",
]

# ── R-CS 橫斷面排名（alpha agenda Tier 1 / H-A1 旗艦）─────────────────────
# A1 對照實驗策略：純橫斷面排名進場（signal = 當日對全 universe 的因子百分位 ∈ top K%），
# 沿用 EXIT_C/D 出場、淨成本、bootstrap CI、子期間/超額濾網。三檔構成 §5.2 殺手級對照：
#   A1_RAW  = 絕對動量（個股 N 日報酬）        → 高負載 β，預期僅 POSITIVE_EV
#   A1_REL  = 相對強度（個股 − 指數 N 日報酬）  → 剝除市場分量（快速版），預期升 ALPHA
#   A1_RESID= 殘差動量（對指數滾動回歸殘差 IR） → 完整 β 殘差化（嚴謹版），預期升 ALPHA
#
# ── H-A4 板塊中性化相對強弱（alpha agenda Tier 1，兩層）─────────────────────
#   A4_SECTOR_REL        = 板塊內相對強弱：個股 N 日報酬 − 同板塊中位數 N 日報酬 top K%
#                          → 中性化市場 + 板塊共同因子（半導體齊漲齊跌），剩「贏過自己板塊」= 選股 alpha
#   A4_SECTOR_REL_TOPSEC = 上述 ∧ 屬「前段板塊」（板塊等權籃子 N 日報酬 top _CS_SECTOR_TOP_FRAC）
#                          → 兩層：板塊動量超配 + 板塊內相對強弱（量化 RA-003 散熱回檔/金融過熱）
_BASE_RATE_CS_STRATS = {
    "A1_RAW":   "raw_mom",
    "A1_REL":   "rel_str",
    "A1_RESID": "resid_mom",
    "A4_SECTOR_REL":        "sector_rel",
    "A4_SECTOR_REL_TOPSEC": "sector_rel_topsec",
}
_CS_SECTOR_FACTORS = {"sector_rel", "sector_rel_topsec"}  # 需產業映射 + 跨股聚合（非逐股可算）
_CS_REL_N      = 60     # 相對強度 / 絕對動量 / 板塊相對 回看天數（agenda N 預設 60）
_CS_RESID_REG  = 120    # 殘差動量：beta 估計窗（60–120d）
_CS_RESID_ACC  = 60     # 殘差動量：殘差累積窗（短於估計窗，避免 OLS 殘差零和）
_CS_DEFAULT_K  = 0.20   # 進場 top K%（agenda 預設前 20%，可配）
_CS_MIN_XSECTION = 10   # 當日可排名所需的最少橫斷面樣本數（暖身期不足則不排名）
_CS_SECTOR_MIN_MEMBERS = 3    # 板塊當日成員數下限（中位數/籃子可靠性，過少則該板塊當日不計）
_CS_SECTOR_TOP_FRAC    = 0.50 # A4 TOPSEC：板塊籃子報酬前此比例 = 「前段板塊」（板塊動量層）

# ── P0-1 R-CHIP-F：籌碼橫斷面因子（讀 chip_snapshot，與價格族正交，攻 A4 美股破功命門）──
# 籌碼僅台股(TW)有資料；他市場回空集（複合自然縮小、誠實標記，與 _chip_streak_dates 同精神）。
# 【給 Reviewer：PIT】籌碼 d 日收盤(13:30)後才公布 → 因子值對齊「次一交易日」(chip series[k+1])
# 才可進場，與既有 _chip_streak_dates 的 PIT 修法逐字同精神（進場∩公布日=∅）。
_CHIP_CS_FACTORS = {"inst_net_buy", "trust_net_buy", "short_squeeze", "sbl_short"}
_CHIP_N_DEFAULT  = 5     # 三大法人/投信淨買累積窗（agenda N∈{3,5,10,20}，搜尋層暫用 default，param_space 供日後 Bayesian）
_CHIP_SQ_N       = 5     # short_squeeze：融券餘額變化回看窗
_CHIP_SBL_N      = 5     # sbl_short：借券賣出餘額 sbl_chg 模式的 N 日變化回看窗（turnover/ratio 模式不需窗）
_CHIP_SBL_FIELD_DEFAULT = "sbl_turnover"  # 借券賣出 default 排名欄：餘額÷ADV（days-to-cover 類比；÷ADV 控流動性）
_CHIP_ADV_WIN    = 20    # ADV（日均量）回看窗，供淨買標準化（÷ADV 控流動性/市值，避免因子退化成市值代理）

# 橫斷面因子家族（§2.4 parsimony 軟剪分族用；同族雙 cs = 換湯不換藥，跨族 chip∧price∧fundamental = 補正交維度）
_CS_FACTOR_FAMILY = {
    "raw_mom": "momentum", "rel_str": "momentum", "resid_mom": "momentum",
    "sector_rel": "sector", "sector_rel_topsec": "sector",
    "inst_net_buy": "chip", "trust_net_buy": "chip", "short_squeeze": "chip",
    "sbl_short": "chip",            # TW-SBL：借券賣出餘額（外資/機構 informed short；與融券 short_squeeze 同族，須驗報酬 corr 判真正交）
    "short_interest": "chip",       # Type 2 US-SI：美股 FINRA 空單餘額（chip 族，與價格族正交）
    "holder_concentration": "chip", # D1：集保大戶持股集中度 level（spec 標 chip；機制為 level/結構，與 flow 正交，見 _TDCC_CS_META flag）
    "holder_conc_delta": "chip",    # D1：大戶集中度週變化（吸籌/派發方向）
    "revenue_mom": "fundamental",   # P0-2：月營收動量（基本面族，與價格/籌碼正交）
    # Type 1a：價格族正交積木（純 OHLCV + 免費宏觀序列自算，零新管線）
    "low_beta": "style",            # BAB（低 β 風險調整後贏）
    "rate_sensitivity": "macro",    # 對 US10Y 久期/折現敏感
    "dollar_sensitivity": "macro",  # 對 DXY 出口商暴險
    "short_rev": "reversal",        # 短期過度反應反轉（DeBondt-Thaler）
}


def _cs_family(factor: str) -> str:
    """cs 因子所屬家族（軟剪相關性分族；未知 factor 自成一族不誤剪）。"""
    return _CS_FACTOR_FAMILY.get(factor, factor)


# 籌碼因子註冊 metadata（入庫三道資格 §3.4：①機制 ②PIT ③參數 metadata，param_space 對齊
# _get_strategy_params_space 契約 {min,max,default,type}；dir∈{+1,−1} 為 R1(a) per-layer 方向）。
_CHIP_CS_META = {
    "inst_net_buy": {
        "param_space": {"N": {"min": 3, "max": 20, "default": _CHIP_N_DEFAULT, "type": "int"},
                        "dir": {"choices": [1, -1], "default": 1, "type": "int"}},
        "pit_rule": "T86 隔日盤後公布 → 進場落次一交易日（只用 ≤T−1 資料）",
        "mechanism": "羊群/smart-money 確認：法人累積淨買超為價外資訊，過濾『漲但無人接』假突破",
        "family": "chip"},
    "trust_net_buy": {
        "param_space": {"N": {"min": 3, "max": 10, "default": _CHIP_N_DEFAULT, "type": "int"},
                        "dir": {"choices": [1, -1], "default": 1, "type": "int"}},
        "pit_rule": "T86 隔日盤後公布 → 進場落次一交易日",
        "mechanism": "投信黏性：季底作帳/連續性比外資強（台股 edge）；dir=−1 表投信倒貨之空方濾網",
        "family": "chip"},
    "short_squeeze": {
        "param_space": {"N": {"min": 3, "max": 10, "default": _CHIP_SQ_N, "type": "int"},
                        "dir": {"choices": [1, -1], "default": 1, "type": "int"}},
        "pit_rule": "TWSE 融資券隔日公布 → 進場落次一交易日",
        "mechanism": "高券資比 + 融券 N 日增 → 空頭回補擠壓（軋空）",
        "family": "chip"},
    "sbl_short": {
        # dir 預設 −1（informed-bear，學術主證據）；+1（借券回補擠壓/與融券軋空疊加）留 forward 翻，註冊時不賭方向。
        "param_space": {"N": {"min": 1, "max": 20, "default": _CHIP_SBL_N, "type": "int"},
                        "field": {"choices": ["sbl_turnover", "sbl_ratio", "sbl_chg"],
                                  "default": _CHIP_SBL_FIELD_DEFAULT, "type": "str"},
                        "dir": {"choices": [1, -1], "default": -1, "type": "int"}},
        "pit_rule": "TWSE 借券賣出餘額(TWT93U)盤後公布 → 進場落次一交易日（series[k] 值落 series[k+1] date，進場∩公布日=∅）",
        "mechanism": "外資/機構 informed short：透過借券系統放空者多為法人(資訊優勢)，高借券賣出餘額(÷ADV 標準化)個股後續 underperform(Boehmer 2008 台股對應)；dir=−1=避開/做空高 SBL。caveat：餘額含避險/套利/造市部位→方向性被稀釋，故 dir 不賭死、預設 −1 信學術主證據，+1 留 forward 發現。",
        "family": "chip"},
}

# ── P0-2 R-FUND-F：月營收動量 cs 因子（基本面族，與價格/籌碼正交）─────────────────────────
# 資料源 revenue_monthly（MOPS t21 月營收回補；2010-01 起，見 _fetch_mops_revenue_month）。
# 【給 Reviewer：PIT】announce_date=次月10日（_announce_date_for_period）→ 因子進場僅落 announce_date
# 之後交易日（嚴格 >，見 _revenue_factor_series）→「進場日 ∩ 營收所屬月 = ∅」「進場日 > 法定公布日」雙成立。
_REVENUE_CS_FACTORS = {"revenue_mom"}
_REV_WIN_DEFAULT   = 1          # 月營收動量回看窗（月數；agenda 窗∈{1,3,6}，搜尋層暫用 default，param_space 供日後 Bayesian）
_REV_FIELD_DEFAULT = "yoy"      # 動量欄位：'yoy'(年增率，台股最被重視) / 'mom'(月增率)

# 月營收因子註冊 metadata（入庫三道資格 §3.4：①機制 ②PIT ③參數 metadata；dir∈{+1,−1} 為 R1(a) per-layer 方向）。
_REVENUE_CS_META = {
    "revenue_mom": {
        "param_space": {"window": {"min": 1, "max": 6, "default": _REV_WIN_DEFAULT, "type": "int"},
                        "field": {"choices": ["yoy", "mom"], "default": _REV_FIELD_DEFAULT, "type": "str"},
                        "dir": {"choices": [1, -1], "default": 1, "type": "int"}},
        "pit_rule": "月營收次月10日前公布 → announce_date=次月10日，進場僅落其後交易日（嚴格 >，進場∩營收月=∅）",
        "mechanism": "基本面動量/反應不足：月營收 YoY 為慢變數，市場對基本面持續改善反應不足（台股月營收高頻獨有，與動量/籌碼正交）",
        "family": "fundamental"},
}

# ── Type 1a R-PROD-7f：價格族橫斷面因子（純 OHLCV + 免費宏觀序列自算，零新管線）──────────────
# 全用現有資料：low_beta/short_rev 僅需 market.db OHLCV（個股 + 市場指數，沿用 _get_benchmark_close_map）；
# rate_sensitivity/dollar_sensitivity 另需免費宏觀序列（^TNX=US10Y、DX-Y.NYB=DXY，沿用 _fetch_index_close_map
# 30min 快取，零新管線）。全市場可用（不像籌碼/月營收 TW-only）。複用 outcome 快取，不重算 K 線。
# 【給 Reviewer：🔑PIT（核心）】每因子在 index i 用「≤dts[i] 的資料」算出 stat，落點 entry=dts[i+1]（次一交易日）
# → 進場日的因子值只含 ≤T−1 資料，與既有 _chip_factor_series/_revenue_factor_series 的「次一交易日落點」
# PIT 修法逐字同精神（進場∩資訊可得當日=∅、進場>資訊可得日一個交易日）。暖身期/缺值=NaN（不排名）。
_PRICE_CS_FACTORS = {"low_beta", "rate_sensitivity", "dollar_sensitivity", "short_rev"}
_PRICE_BETA_WIN      = 120   # 滾動 β 回歸窗（agenda 60–120d，default 120；param_space 供日後 Bayesian）
_PRICE_BETA_MIN_FRAC = 0.5   # 回歸窗內有效（個股報酬∧回歸子變動皆非 NaN）樣本最低占比，不足則該日不算
_PRICE_REV_N         = 5     # short_rev 短期反轉回看天數（agenda 1–5d，default 5）

# 價格族因子註冊 metadata（入庫三道資格 §3.4：①機制 ②PIT ③參數 metadata；dir∈{+1,−1} 為 R1(a) per-layer 方向）。
# low_beta：raw=β，dir 預設 −1（BAB → 取 bottom K%＝低 β 股）；其餘 raw=β/−報酬，dir 由搜尋層決定。
_PRICE_CS_META = {
    "low_beta": {
        "param_space": {"beta_win": {"min": 60, "max": 250, "default": _PRICE_BETA_WIN, "type": "int"},
                        "dir": {"choices": [1, -1], "default": -1, "type": "int"}},
        "pit_rule": "個股對指數滾動 β 用 ≤T−1 報酬窗算 → 落次一交易日進場（只用 ≤T−1）",
        "mechanism": "BAB（Frazzini-Pedersen）：低 β 股風險調整後贏；dir=−1 取低 β（raw=β，反向 rank）",
        "family": "style"},
    "rate_sensitivity": {
        "param_space": {"beta_win": {"min": 60, "max": 250, "default": _PRICE_BETA_WIN, "type": "int"},
                        "dir": {"choices": [1, -1], "default": 1, "type": "int"}},
        "pit_rule": "個股對 US10Y(^TNX) yield 差分滾動 β 用 ≤T−1 窗算 → 落次一交易日進場",
        "mechanism": "久期/折現敏感：對長端利率變動的暴險（降息 regime 高 β 受惠），dir 由 regime 決定",
        "family": "macro"},
    "dollar_sensitivity": {
        "param_space": {"beta_win": {"min": 60, "max": 250, "default": _PRICE_BETA_WIN, "type": "int"},
                        "dir": {"choices": [1, -1], "default": 1, "type": "int"}},
        "pit_rule": "個股對 DXY(DX-Y.NYB) 報酬滾動 β 用 ≤T−1 窗算 → 落次一交易日進場",
        "mechanism": "匯率暴險：出口商 vs 內需對美元敏感度（弱勢美元利出口），dir 由 regime 決定",
        "family": "macro"},
    "short_rev": {
        "param_space": {"rev_n": {"min": 1, "max": 10, "default": _PRICE_REV_N, "type": "int"},
                        "dir": {"choices": [1, -1], "default": 1, "type": "int"}},
        "pit_rule": "−(個股 N 日報酬) 用 ≤T−1 收盤算 → 落次一交易日進場",
        "mechanism": "過度反應反轉（DeBondt-Thaler）：近 N 日大跌者 snap-back（raw=−報酬→高 rank=跌最多）",
        "family": "reversal"},
}

# ── Type 2 R-PROD US-SI：美股籌碼橫斷面因子（short_interest，讀 us_short_interest，US-gated）──
# 鏡像 _CHIP_CS_FACTORS（TW chip）→ 美股換源：FINRA 半月頻空單餘額（greenfield 新管線，monitor.db）。
# 【給 Reviewer：PIT】FINRA settlement 後約 8 營業日才 publish → 因子值只落 publish_date 之後交易日
# （嚴格 >，鏡像 _revenue_factor_series 的 announce_date）→「進場∩settlement期=∅」「進場>publish_date」雙成立。
# 本次只註冊 short_interest（inst_13f 為後續階段，留好 set 擴充；cot_net 走 regime 永不入此白名單）。
_US_CHIP_CS_FACTORS = {"short_interest"}
_US_SI_FIELD_DEFAULT = "si_ratio"   # 排名欄位：si_ratio(=days_to_cover) / days_to_cover / si_pct_float
_US_SI_WIN_DEFAULT   = 1            # 回看 N 個『已公布』申報取均值（window=1 即最近一筆已公布 SI）

# 美股籌碼因子註冊 metadata（鏡像 _CHIP_CS_META；dir 預設 −1=informed-bear，搜尋層可翻 +1 試擠空，
# 註冊時不賭方向，見 spec §2.1 警語）。param_space 對齊 _get_strategy_params_space 契約 {min,max,default,type}。
_US_CHIP_CS_META = {
    "short_interest": {
        "param_space": {"field": {"choices": ["si_ratio", "days_to_cover", "si_pct_float"],
                                  "default": _US_SI_FIELD_DEFAULT, "type": "str"},
                        "window": {"min": 1, "max": 3, "default": _US_SI_WIN_DEFAULT, "type": "int"},
                        "dir": {"choices": [1, -1], "default": -1, "type": "int"}},
        "pit_rule": "FINRA settlement 後約8營業日 publish → 因子僅落 publish_date 之後交易日（嚴格 >，進場∩settlement期=∅）",
        "mechanism": "空方資訊優勢（Boehmer-Jones-Zhang 2008）：高 short interest / 高 days-to-cover 個股後續 underperform，"
                     "dir=−1 避開/做空高 SI（主機制）；高 days-to-cover×擠空 regime 時搜尋層可翻 dir=+1（軋空 fuel，次案）",
        "family": "chip"},
}

# ── D1 R-CHIP-STRUCT：集保大戶持股結構 cs 因子（讀 tdcc_holder，TW-gated）──────────────────────
# 擴池第一個「持股結構/level」訊號——現有籌碼全是 flow（當期買賣超），完全沒有 level/結構。
# 大戶持股集中度＝informed 存量吸籌/派發，與 flow 天生正交（須驗 corr 證實，留 cockpit）。
# 【給 Reviewer：🔑PIT】週頻 + 公布有 lag。因子值只落 publish_date 之後交易日（嚴格 >，鏡像
# _short_interest_series 的 publish_date）→「進場∩結算週=∅」「進場>公布日」雙成立。禁用結算日當期=look-ahead。
# 【family 標記】spec §2 明訂 family="chip"（鏡像 sbl_short）；但機制為 level/結構非 flow → 與 flow chip
# 天生正交。此處遵 spec 標 chip，並於報告 flag：AIF 可考慮獨立 family "holder/structure" 讓 diversity-sort
# 獎勵 holder∧flow 跨族堆疊（純加法，不改既有因子）。dir 預設 +1（集中度高/吸籌看多），搜尋層可翻。
_TDCC_CS_FACTORS = {"holder_concentration", "holder_conc_delta"}
_TDCC_CONC_FIELD_DEFAULT = "conc_gt400"   # 大戶集中度排名欄：>400張(級距12-15)占比；可選 conc_gt1000(>1000張)
_TDCC_WIN_DEFAULT        = 1              # level：回看 N 筆『已公布』週取均值（1=最近一筆已公布）
_TDCC_DELTA_WIN_DEFAULT  = 1              # delta：對 N 個『已公布』週前的變化（1=週變化＝吸籌/派發方向）

# 集保大戶結構因子註冊 metadata（入庫三道資格 §3.4：①機制 ②PIT ③參數 metadata；dir∈{+1,−1} per-layer）。
_TDCC_CS_META = {
    "holder_concentration": {
        "param_space": {"field": {"choices": ["conc_gt400", "conc_gt1000"],
                                  "default": _TDCC_CONC_FIELD_DEFAULT, "type": "str"},
                        "window": {"min": 1, "max": 4, "default": _TDCC_WIN_DEFAULT, "type": "int"},
                        "dir": {"choices": [1, -1], "default": 1, "type": "int"}},
        "pit_rule": "TDCC 股權分散表每週結算、結算後約4營業日公布 → 因子僅落 publish_date 之後交易日（嚴格 >，進場∩結算週=∅）",
        "mechanism": "informed 存量結構：千張/大戶(>400張)持股集中度 level 上升＝法人/主力沉默吸籌(sticky 低噪音領先)；"
                     "散戶級距佔比升＝派發。與現有 flow 籌碼(inst/sbl)機制不同→天生正交。dir=+1 集中度高看多（須驗 corr/EV）",
        "family": "chip"},
    "holder_conc_delta": {
        "param_space": {"field": {"choices": ["conc_gt400", "conc_gt1000"],
                                  "default": _TDCC_CONC_FIELD_DEFAULT, "type": "str"},
                        "window": {"min": 1, "max": 4, "default": _TDCC_DELTA_WIN_DEFAULT, "type": "int"},
                        "dir": {"choices": [1, -1], "default": 1, "type": "int"}},
        "pit_rule": "同 holder_concentration（嚴格 > publish_date）；delta=已公布最新值 − N週前已公布值（皆 < 進場日）",
        "mechanism": "吸籌/派發方向：大戶集中度週變化為正＝大戶加碼吸籌(看多)，為負＝派發(看空)。dir=+1 吸籌看多（搜尋層可翻）",
        "family": "chip"},
}

_SECTOR_MAP_MEM: dict = {}    # in-process cache: f"{market}:{code}" -> sector_str（避免同跑重複查）

# ── H-A5 財報後漂移（PEAD，免共識代理版）─────────────────────────────────────
# 事件型 alpha：事件時間天然與日曆市場去相關，與順勢型 A1/A4 低相關（分散）。
#   A5_PEAD = 財報後正異常報酬（earnings momentum）→ 做多前段驚奇，持有沿用 EXIT_C/D。
# 驚奇代理（免共識，只需 財報日期 + OHLCV）＝「財報前一收盤 → 後 S 日收盤」的個股報酬
# 減同窗基準報酬 = 異常報酬；≥ 門檻 = 正驚奇。進場 = 驚奇窗末日收盤（已可完整觀測，無 look-ahead）。
_BASE_RATE_EVENT_STRATS = {"A5_PEAD"}   # 事件型策略：trigger 由 earnings_dates + 後驚奇代理算（非每日橫斷面）
_A5_SURPRISE_DAYS   = 2      # 驚奇窗長度（交易日）＝財報後 1–2 日異常報酬（agenda 免共識快速版）
_A5_SURPRISE_THRESH = 2.0    # 正驚奇門檻：財報後窗異常報酬 ≥ 此 %（跳漲>門檻才做多）
_A5_EARNINGS_FETCH_LIMIT = 48  # yfinance .earnings_dates 抓取上限（≈12 年季報，實際視覆蓋而定）
_EARNINGS_MAP_MEM: dict = {}   # in-process cache: f"{market}:{code}" -> [earnings_date str]（避免同跑重複查）


def _ensure_sector_table():
    """A4 產業映射落地表（可快取、可擴充；冪等建表）。sector 取自 yfinance .info（GICS 板塊，
    TW/.TW 與 US 皆有），一檔抓一次落地，之後讀表免再打 yfinance。"""
    con = db()
    con.execute("""CREATE TABLE IF NOT EXISTS stock_sector(
        code TEXT NOT NULL,
        market TEXT NOT NULL DEFAULT 'TW',
        sector TEXT,
        industry TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(code, market)
    )""")
    con.commit()
    con.close()


def _get_sector_map(market: str, codes: list) -> dict:
    """回傳 {code: sector_str}（A4 用）。先讀 stock_sector 落地表 → 缺的個股用
    _get_fundamentals(yfinance .info) 補抓並落地 → 回傳有 sector 的個股映射。
    純讀表時零外呼；首跑某 universe 會逐檔抓一次（之後快取）。無 sector 的個股不納入（A4 自然縮小到有分類者）。"""
    _ensure_sector_table()
    out = {}
    miss = []
    con = db()
    have = {}
    try:
        qs = ",".join("?" * len(codes))
        for c, sec in con.execute(
            f"SELECT code, sector FROM stock_sector WHERE market=? AND code IN ({qs})",
            [market, *codes]
        ).fetchall():
            have[c] = sec
    except Exception:
        have = {}
    con.close()
    for code in codes:
        mk = f"{market}:{code}"
        if mk in _SECTOR_MAP_MEM:
            sec = _SECTOR_MAP_MEM[mk]
        elif code in have:
            sec = have[code]
            _SECTOR_MAP_MEM[mk] = sec
        else:
            miss.append(code)
            continue
        if sec:
            out[code] = sec
    if miss:
        con = db()
        for code in miss:
            try:
                fund = _get_fundamentals(code, market)
                sec = fund.get("sector") or ""
                ind = fund.get("industry") or ""
            except Exception:
                sec, ind = "", ""
            try:
                con.execute(
                    "INSERT OR REPLACE INTO stock_sector(code, market, sector, industry, updated_at) "
                    "VALUES(?,?,?,?,?)",
                    (code, market, sec, ind, datetime.now().isoformat())
                )
            except Exception:
                pass
            _SECTOR_MAP_MEM[f"{market}:{code}"] = sec
            if sec:
                out[code] = sec
        con.commit()
        con.close()
        print(f"[base-rate] A4 sector map: {market} fetched {len(miss)} new, "
              f"{len(out)}/{len(codes)} have sector", flush=True)
    return out


def _ensure_earnings_table():
    """A5 財報日期落地表（可快取、可擴充；冪等建表，在 monitor.db）。日期取自 yfinance JSON 視覺化端點
    （_get_earnings_dates_using_screener，無 lxml 依賴；實測 US+TW 覆蓋皆佳，回 2013–2025 季報）。
    eps_estimate/reported_eps/surprise 一併存，供未來 SUE 嚴謹版升級。earnings_fetch_log 記「抓過的
    code（即使 0 筆）」，避免每跑重抓覆蓋失敗者。"""
    con = db()
    con.execute("""CREATE TABLE IF NOT EXISTS earnings_dates(
        code TEXT NOT NULL,
        market TEXT NOT NULL DEFAULT 'TW',
        earnings_date TEXT NOT NULL,
        eps_estimate REAL,
        reported_eps REAL,
        surprise_pct REAL,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(code, market, earnings_date)
    )""")
    con.execute("""CREATE TABLE IF NOT EXISTS earnings_fetch_log(
        code TEXT NOT NULL,
        market TEXT NOT NULL DEFAULT 'TW',
        fetched_at TEXT,
        n_dates INTEGER DEFAULT 0,
        PRIMARY KEY(code, market)
    )""")
    con.commit()
    con.close()


def _fetch_earnings_dates_yf(code: str, market: str) -> list:
    """抓歷史財報日期 → list[(date_str, eps_est, reported_eps, surprise_pct)]。

    主取數路徑＝yfinance JSON 視覺化端點 _get_earnings_dates_using_screener（**無 lxml 依賴**，
    US+TW 覆蓋皆佳，實測回 2013–2025 季報；本機環境無 lxml，故不能用 .earnings_dates HTML scrape）。
    無此法/失敗時才退回 .earnings_dates（HTML scrape，需 lxml；環境無 lxml 時自然失敗→回空，best-effort）。

    排除 Event Type='Meeting'（股東會，非財報）。注意：此端點時區為 America/New_York，TW(.TW) 的
    日期可能與台北時間差 1 日 → 但本驚奇窗為 2 日 [a-1, a+1] 已吸收此 ±1 日誤差。
    純取數，不落地；回空不視為錯。"""
    out = []
    try:
        import yfinance as yf
        ticker = code if market == "US" else f"{code}.TW"
        tk = yf.Ticker(ticker)
        df = None
        # 1) JSON 端點優先（無 lxml；US+TW 覆蓋佳）
        try:
            fn = getattr(tk, "_get_earnings_dates_using_screener", None)
            if fn is not None:
                df = fn(limit=_A5_EARNINGS_FETCH_LIMIT)
        except Exception:
            df = None
        # 2) 退回 HTML scrape（需 lxml；本機無 lxml 時失敗→回空）
        if df is None or len(df) == 0:
            try:
                df = tk.get_earnings_dates(limit=_A5_EARNINGS_FETCH_LIMIT)
            except Exception:
                try:
                    df = tk.earnings_dates
                except Exception:
                    df = None
        if df is None or len(df) == 0:
            return out
        cols = {str(col).lower(): col for col in df.columns}
        est_col = next((cols[k] for k in cols if "estimate" in k), None)
        rep_col = next((cols[k] for k in cols if "reported" in k), None)
        sur_col = next((cols[k] for k in cols if "surprise" in k), None)
        evt_col = next((cols[k] for k in cols if "event" in k and "type" in k), None)

        def _num(row, col):
            if not col:
                return None
            try:
                v = row[col]
                if v is None or (isinstance(v, float) and v != v):
                    return None
                return float(v)
            except Exception:
                return None

        for idx, row in df.iterrows():
            if evt_col is not None:
                try:
                    if str(row[evt_col]).strip().lower() == "meeting":
                        continue   # 股東會非財報事件，排除
                except Exception:
                    pass
            try:
                ds = idx.strftime("%Y-%m-%d")
            except Exception:
                ds = str(idx)[:10]
            if not ds or len(ds) < 10:
                continue
            out.append((ds, _num(row, est_col), _num(row, rep_col), _num(row, sur_col)))
    except Exception:
        return out
    return out


def _get_earnings_map(market: str, codes: list) -> dict:
    """回傳 {code: [sorted earnings_date str]}（A5 用）。先讀 earnings_dates 落地表 → 未抓過的 code
    用 yfinance 補抓並落地（含 0 筆者記 fetch_log，免重抓 best-effort 失敗者）→ 回傳有 ≥1 財報日期者。
    純讀表時零外呼；首跑某 universe 逐檔抓一次（之後快取）。覆蓋率低的市場（如 TW）會誠實縮小。"""
    _ensure_earnings_table()
    out = {}
    # 1) 記憶體快取直接用
    for c in codes:
        eds = _EARNINGS_MAP_MEM.get(f"{market}:{c}")
        if eds:
            out[c] = list(eds)
    need_db = [c for c in codes if f"{market}:{c}" not in _EARNINGS_MAP_MEM]
    # 2) 讀落地表（fetch_log 判斷誰抓過、earnings_dates 取日期）
    fetched = set()
    db_dates = {}
    if need_db:
        con = db()
        try:
            for (c,) in con.execute("SELECT code FROM earnings_fetch_log WHERE market=?", [market]).fetchall():
                fetched.add(c)
            qs = ",".join("?" * len(need_db))
            for code, ed in con.execute(
                f"SELECT code, earnings_date FROM earnings_dates WHERE market=? AND code IN ({qs})",
                [market, *need_db]
            ).fetchall():
                db_dates.setdefault(code, []).append(ed)
        except Exception:
            pass
        con.close()
    miss = []
    for c in need_db:
        if c in fetched:  # 抓過（即使 0 筆）→ 用表內日期，不再外呼
            eds = sorted(set(db_dates.get(c, [])))
            _EARNINGS_MAP_MEM[f"{market}:{c}"] = eds
            if eds:
                out[c] = eds
        else:
            miss.append(c)
    # 3) 未抓過 → yfinance 補抓 + 落地 + 記 fetch_log
    if miss:
        con = db()
        for code in miss:
            rows = _fetch_earnings_dates_yf(code, market)
            for ds, est, rep, sur in rows:
                try:
                    con.execute(
                        "INSERT OR REPLACE INTO earnings_dates(code, market, earnings_date, eps_estimate, reported_eps, surprise_pct, updated_at) "
                        "VALUES(?,?,?,?,?,?,?)",
                        (code, market, ds, est, rep, sur, datetime.now().isoformat())
                    )
                except Exception:
                    pass
            try:
                con.execute(
                    "INSERT OR REPLACE INTO earnings_fetch_log(code, market, fetched_at, n_dates) VALUES(?,?,?,?)",
                    (code, market, datetime.now().isoformat(), len(rows))
                )
            except Exception:
                pass
            eds = sorted({r[0] for r in rows})
            _EARNINGS_MAP_MEM[f"{market}:{code}"] = eds
            if eds:
                out[code] = eds
        con.commit()
        con.close()
        have = sum(1 for c in codes if out.get(c))
        print(f"[base-rate] A5 earnings: {market} fetched {len(miss)} new codes, "
              f"{have}/{len(codes)} have ≥1 earnings date", flush=True)
    return out


def _net_return_pct(buy_price: float, sell_price: float, mkt: str, discount: float) -> float:
    """單筆交易扣成本後報酬%（台股含手續費+證交稅；美股套買賣兩邊滑價 US_SLIPPAGE_PCT，
    與投組引擎 7608/7632 一致，否則正EV閘對美股失真 — R0）"""
    if mkt == "TW":
        cr = TW_COMMISSION * discount
        buy_cost = buy_price * (1 + cr)
        proceeds = sell_price * (1 - cr - TW_TAX)
    else:
        buy_cost = buy_price * (1 + US_SLIPPAGE_PCT)
        proceeds = sell_price * (1 - US_SLIPPAGE_PCT)
    return (proceeds - buy_cost) / buy_cost * 100 if buy_cost else 0.0


def _compute_stock_features(code: str, all_dates: list, bar_data: dict):
    """
    每股算一次：指標陣列 + 「進場日→固定出場(EXIT_C+EXIT_D)結果」outcome 表。
    回傳 dict（含 numpy 陣列），供所有進場策略共用。指標為連續計算（暖身期後與
    投組引擎的視窗版差異可忽略）。
    """
    import numpy as np
    seq = [(d, bar_data[(code, d)]) for d in all_dates if (code, d) in bar_data]
    if len(seq) < 60:
        return None

    o = np.array([x[1]["open"] for x in seq], dtype=float)
    h = np.array([x[1]["high"] for x in seq], dtype=float)
    l = np.array([x[1]["low"] for x in seq], dtype=float)
    c = np.array([x[1]["close"] for x in seq], dtype=float)
    v = np.array([x[1]["volume"] for x in seq], dtype=float)
    dates_c = [x[0] for x in seq]
    n = len(c)
    cs = pd.Series(c)

    def _rmean(w):
        return cs.rolling(w).mean().to_numpy()

    ma5, ma10, ma20, ma60, ma240 = _rmean(5), _rmean(10), _rmean(20), _rmean(60), _rmean(240)

    # MACD（沿用 calc_macd 的 EWM 公式，連續計算）
    dif_l, macd_l, _ = calc_macd(c.tolist())
    dif = np.array(dif_l, dtype=float)
    macd = np.array(macd_l, dtype=float)

    # RSI(14) — 簡單平均，對齊回測引擎
    diff = np.diff(c, prepend=c[0])
    gain = pd.Series(np.where(diff > 0, diff, 0.0))
    loss = pd.Series(np.where(diff < 0, -diff, 0.0))
    ag = gain.rolling(14).mean().to_numpy()
    al = loss.rolling(14).mean().to_numpy()
    with np.errstate(divide="ignore", invalid="ignore"):
        rsi = np.where(al > 0, 100 - 100 / (1 + ag / al), 100.0)

    # KD（連續 EWM，RSV 用收盤 9 日高低，對齊回測引擎）
    kd_k = np.full(n, 50.0)
    kd_d = np.full(n, 50.0)
    _k, _d = 50.0, 50.0
    for i in range(n):
        lo = c[max(0, i - 8):i + 1].min()
        hi = c[max(0, i - 8):i + 1].max()
        rsv = (c[i] - lo) / (hi - lo) * 100 if hi != lo else 50.0
        _k = rsv * (1 / 3) + _k * (2 / 3)
        _d = _k * (1 / 3) + _d * (2 / 3)
        kd_k[i], kd_d[i] = _k, _d

    # 量比：v[i] / 前 20 日均量（不含當日）
    vol_ratio = np.ones(n)
    for i in range(1, n):
        prev = v[max(0, i - 20):i]
        m = prev.mean() if len(prev) else 0
        vol_ratio[i] = v[i] / m if m > 0 else 1.0

    # 前 20 日最高（不含當日）— SQUEEZE_BREAK / DONCHIAN_BREAK
    prior_high20 = pd.Series(h).rolling(20).max().shift(1).to_numpy()

    # 布林帶寬（BB_SQUEEZE）
    bb_std = cs.rolling(20).std().to_numpy()
    bb_upper = ma20 + 2 * bb_std
    with np.errstate(divide="ignore", invalid="ignore"):
        bb_width = np.where(ma20 > 0, 4 * bb_std / ma20, np.nan)
    bb_min_prior = pd.Series(bb_width).rolling(60).min().shift(1).to_numpy()

    # ── outcome 表：進場 i（close[i]）→ 固定 EXIT_C+EXIT_D ──
    exit_d_pct = _get_strategy_param("EXIT_D", "exit_d_threshold", 5) / 100
    pt = _get_strategy_param("EXIT_C", "swing_profit", 8) / 100   # 波段預設
    dt = _get_strategy_param("EXIT_C", "swing_drawdown", 2) / 100
    discount = TW_DISCOUNT

    outcome_ret = np.full(n, np.nan)
    outcome_exit = np.full(n, -1, dtype=int)
    for i in range(n - 1):
        cost = c[i]
        if cost <= 0:
            continue
        peak = h[i]
        for j in range(i + 1, n):
            if h[j] > peak:
                peak = h[j]
            pnl = (c[j] - cost) / cost
            exit_here = False
            if pnl <= -exit_d_pct:               # EXIT_D 絕對停損優先
                exit_here = True
            elif peak > 0:                        # EXIT_C 移動止盈
                max_p = (peak - cost) / cost
                if max_p >= pt and (peak - c[j]) / peak >= dt:
                    exit_here = True
            if exit_here or j == n - 1:
                outcome_ret[i] = _net_return_pct(cost, c[j], "TW" if _is_tw_code(code) else "US", discount)
                outcome_exit[i] = j
                break

    return {
        "dates": dates_c, "n": n, "o": o, "h": h, "l": l, "c": c, "v": v,  # v：P0-1 籌碼因子 ÷ADV 用（加法，零回歸）
        "ma5": ma5, "ma10": ma10, "ma20": ma20, "ma60": ma60, "ma240": ma240,
        "dif": dif, "macd": macd, "rsi": rsi, "kd_k": kd_k, "kd_d": kd_d,
        "vol_ratio": vol_ratio, "prior_high20": prior_high20,
        "bb_upper": bb_upper, "bb_width": bb_width, "bb_min_prior": bb_min_prior,
        "outcome_ret": outcome_ret, "outcome_exit": outcome_exit,
    }


def _is_tw_code(code: str) -> bool:
    """台股代碼為純數字（含 4 位/上市櫃）；美股為英文。"""
    return code[:1].isdigit()


def _detect_triggers(sid: str, f: dict):
    """回傳 bool 陣列：該進場策略在每日是否觸發。用預算好的指標。"""
    import numpy as np
    n = f["n"]
    c, o, h, l = f["c"], f["o"], f["h"], f["l"]
    ma5, ma10, ma20, ma60, ma240 = f["ma5"], f["ma10"], f["ma20"], f["ma60"], f["ma240"]
    dif, macd, rsi = f["dif"], f["macd"], f["rsi"]
    kd_k, kd_d, volr = f["kd_k"], f["kd_d"], f["vol_ratio"]
    ph20, bb_upper, bb_width, bb_min = f["prior_high20"], f["bb_upper"], f["bb_width"], f["bb_min_prior"]
    trig = np.zeros(n, dtype=bool)

    def nz(x):
        return x is not None and x == x  # not NaN

    for i in range(1, n):
        price = c[i]
        if sid == "BUY_A":
            if dif[i-1] < macd[i-1] and dif[i] >= macd[i] and nz(ma20[i]) and price > ma20[i] and volr[i] >= 1.5:
                trig[i] = True
        elif sid == "BUY_B":
            if nz(ma5[i]) and nz(ma10[i]) and ma5[i-1] < ma10[i-1] and ma5[i] >= ma10[i] and volr[i] >= 1.2:
                trig[i] = True
        elif sid == "LOW_BUY":
            if nz(ma240[i]) and price < ma240[i] * 0.85:
                trig[i] = True
        elif sid == "SQUEEZE_BREAK":
            if nz(ph20[i]) and price > ph20[i] and volr[i] >= 2.0:
                trig[i] = True
        elif sid == "KD_CROSS":
            if kd_k[i-1] < kd_d[i-1] and kd_k[i] >= kd_d[i] and kd_k[i] < 50:
                trig[i] = True
        elif sid == "MACD_CROSS":
            if dif[i-1] < macd[i-1] and dif[i] >= macd[i]:
                trig[i] = True
        elif sid == "RSI_EXTREME":
            if nz(rsi[i-1]) and nz(rsi[i]) and rsi[i-1] < 30 and rsi[i] >= 30:
                trig[i] = True
        elif sid == "MA_ALIGN":
            if all(nz(x[i]) for x in (ma5, ma10, ma20, ma60)) and price > ma5[i] > ma10[i] > ma20[i] > ma60[i]:
                trig[i] = True
        elif sid == "DONCHIAN_BREAK":
            if nz(ph20[i]) and price > ph20[i]:
                trig[i] = True
        elif sid == "MA_PULLBACK":
            if nz(ma20[i]) and nz(ma60[i]) and l[i] <= ma20[i] and price > ma60[i] and price > o[i]:
                trig[i] = True
        elif sid == "BB_SQUEEZE":
            if nz(bb_width[i]) and nz(bb_min[i]) and bb_width[i] <= bb_min[i] and nz(bb_upper[i]) and price > bb_upper[i]:
                trig[i] = True
        elif sid == "VOL_BREAKOUT":
            day_chg = (c[i] - c[i-1]) / c[i-1] if c[i-1] > 0 else 0
            if volr[i] >= 2.0 and day_chg >= 0.01:
                trig[i] = True
    return trig


def _a5_pead_triggers(dates, c, bench_map, earnings_dates):
    """A5 PEAD（財報後漂移）事件型進場 trigger（免共識代理版，只需 財報日期 + OHLCV）。

    驚奇代理（earnings momentum）＝財報後異常報酬：以「財報前一收盤 → 後 S 日收盤」的個股報酬
    減同窗基準（^TWII/SPY）報酬 = 異常報酬。正驚奇（≥ _A5_SURPRISE_THRESH%）→ 在驚奇窗末日
    收盤進場（該日已完整觀測驚奇），持有沿用 EXIT_C/D outcome 表（_emit_trades 自動去重疊倉）。

    防 look-ahead：
      a    = 第一個 >= 財報日期 的交易日（涵蓋 BMO/AMC 公告時點不確定性，把反應落在 a 或 a+1 都涵蓋）。
      base = a-1（財報前一收盤，資訊釋出前基準）；entry = a-1+S（驚奇窗末日 = 進場日）。
      進場 index 即驚奇窗最後一日 → surprise 完全實現於該日收盤，絕不早於可觀測點。

    純計算（無 yfinance/DB），供隔離測試驗算。dates 須升冪（ISO 字串）；earnings_dates = list[YYYY-MM-DD]。
    回傳 bool 陣列（對齊 dates）。"""
    import numpy as np
    from bisect import bisect_left
    n = len(c)
    trig = np.zeros(n, dtype=bool)
    if not earnings_dates or n == 0:
        return trig
    S = _A5_SURPRISE_DAYS
    for ed in earnings_dates:
        if not ed:
            continue
        a = bisect_left(dates, ed)        # 公告交易日（第一個 >= 財報日期）
        if a >= n:
            continue
        base = a - 1                      # 財報前一收盤
        entry = a - 1 + S                 # 驚奇窗末日 = 進場日（T+S，相對前一收盤）
        if base < 0 or entry >= n:
            continue
        if c[base] <= 0 or c[entry] <= 0:
            continue
        r_stock = c[entry] / c[base] - 1
        b0 = bench_map.get(dates[base]); b1 = bench_map.get(dates[entry])
        if b0 is None or b1 is None or b0 <= 0:
            continue                      # 缺基準 → 無法算異常報酬，誠實跳過該事件
        r_bench = b1 / b0 - 1
        surprise = (r_stock - r_bench) * 100   # 財報後異常報酬%（驚奇代理）
        if surprise >= _A5_SURPRISE_THRESH:
            trig[entry] = True            # 正驚奇 → 該日進場
    return trig


def _compute_cs_factors(f: dict, bench_map: dict, factors_needed: set) -> dict:
    """為單一個股算「橫斷面因子的時間序列」（對齊 f['dates']），供之後跨 universe 排名。
       raw_mom   = 個股 N 日報酬%（絕對動量，§5.2 對照組，高負載 β）
       rel_str   = 個股 N 日報酬 − 指數 N 日報酬（相對強度，A1 快速版，剝市場分量）
       resid_mom = 對指數滾動回歸殘差的資訊比（近 acc 窗累積殘差 / 殘差波動，A1 嚴謹版）
    回傳 {factor: np.array(len=n)}，暖身期/缺指數為 NaN。純 OHLCV + market.db 指數，零新數據。"""
    import numpy as np
    dates_c = f["dates"]; c = f["c"]; n = f["n"]
    out = {}
    idx = np.array([bench_map.get(d, float("nan")) for d in dates_c], dtype=float)

    N = _CS_REL_N
    if "raw_mom" in factors_needed or "rel_str" in factors_needed:
        s_mom = np.full(n, np.nan)
        b_mom = np.full(n, np.nan)
        for i in range(N, n):
            if c[i - N] > 0:
                s_mom[i] = (c[i] / c[i - N] - 1) * 100
            if idx[i - N] == idx[i - N] and idx[i] == idx[i] and idx[i - N] > 0:
                b_mom[i] = (idx[i] / idx[i - N] - 1) * 100
        if "raw_mom" in factors_needed:
            out["raw_mom"] = s_mom
        if "rel_str" in factors_needed:
            out["rel_str"] = s_mom - b_mom  # NaN 傳染：任一缺 → 不排名

    if "resid_mom" in factors_needed:
        sr = np.full(n, np.nan); br = np.full(n, np.nan)
        for i in range(1, n):
            if c[i - 1] > 0:
                sr[i] = c[i] / c[i - 1] - 1
            if idx[i - 1] == idx[i - 1] and idx[i] == idx[i] and idx[i - 1] > 0:
                br[i] = idx[i] / idx[i - 1] - 1
        reg, acc = _CS_RESID_REG, _CS_RESID_ACC
        rir = np.full(n, np.nan)
        for i in range(reg, n):
            xs = br[i - reg + 1:i + 1]; ys = sr[i - reg + 1:i + 1]
            m = (xs == xs) & (ys == ys)
            if int(m.sum()) < reg * 0.6:
                continue
            x = xs[m]; y = ys[m]
            xm = x.mean(); sxx = float(((x - xm) ** 2).sum())
            if sxx <= 0:
                continue
            beta = float(((x - xm) * (y - y.mean())).sum() / sxx)
            xa = br[i - acc + 1:i + 1]; ya = sr[i - acc + 1:i + 1]
            ma = (xa == xa) & (ya == ya)
            if int(ma.sum()) < acc * 0.6:
                continue
            # 殘差報酬 = 個股報酬剝除「市場 beta 暴險」後的分量（保留特質性 alpha，
            # 不減回歸截距，否則持續性 alpha 會被截距吃掉 → 殘差動量抓不到 alpha）。
            # beta 估在長窗、殘差累在近 acc 窗 → 資訊比 = 近窗累積殘差 / 殘差波動。
            resid = ya[ma] - beta * xa[ma]
            sd = float(resid.std())
            if sd > 0:
                rir[i] = float(resid.sum()) / sd
        out["resid_mom"] = rir
    return out


def _nday_return_pct_series(c, N: int):
    """個股 N 日報酬%序列（對齊 dates，暖身期 NaN）。與 _compute_cs_factors 的 s_mom 同公式，
    供 A4 板塊聚合用（板塊相對強弱以「個股 N 日報酬 − 同板塊中位」定義）。"""
    import numpy as np
    n = len(c)
    out = np.full(n, np.nan)
    for i in range(N, n):
        if c[i - N] > 0:
            out[i] = (c[i] / c[i - N] - 1) * 100
    return out


def _compute_sector_factors(smom_by_code: dict, sector_of: dict, factors_needed: set) -> dict:
    """A4 板塊中性化相對強弱因子。輸入各股 N 日報酬序列 + 產業映射，回傳
        {factor: {date: {code: val}}}（與 _fast_base_rate_market 的 factor_vals 同形狀，可直接併入排名）。

      sector_rel        = 個股 N 日報酬 − 同板塊「當日成員中位數」N 日報酬
                          → 中性化市場 + 板塊共同因子，剩「贏過自己板塊」= 選股 alpha（天然正交）。
      sector_rel_topsec = 同上，但僅在「個股所屬板塊屬當日前段（等權籃子報酬 top _CS_SECTOR_TOP_FRAC）」時保留
                          → 兩層：板塊動量（籃子排名）∧ 板塊內相對強弱。

    純計算（無 yfinance/DB），供隔離測試驗算。
    smom_by_code: {code: (dates_list, np.array(N日報酬%, 暖身期/缺值=NaN))}；sector_of: {code: sector_str}。"""
    import numpy as np
    want_topsec = "sector_rel_topsec" in factors_needed
    # 1) 每 (date, sector) 收集成員 N 日報酬
    sector_day = {}  # date -> {sector -> [smom,...]}
    for code, (dts, smom) in smom_by_code.items():
        sec = sector_of.get(code)
        if not sec:
            continue
        for i in range(len(dts)):
            v = smom[i]
            if v == v:  # not NaN
                sector_day.setdefault(dts[i], {}).setdefault(sec, []).append(float(v))
    # 2) 每 (date, sector) 中位數（板塊內相對基準）+ 等權籃子均值 → 前段板塊（TOPSEC）
    sector_median = {}  # date -> {sector -> median}
    top_sectors = {}    # date -> set(sector) 前段
    for d, secs in sector_day.items():
        med = {}; basket = {}
        for sec, vals in secs.items():
            if len(vals) >= _CS_SECTOR_MIN_MEMBERS:
                med[sec] = float(np.median(vals))
                basket[sec] = float(np.mean(vals))  # 等權籃子 N 日報酬 = sector_mom
        sector_median[d] = med
        if want_topsec and basket:
            items = sorted(basket.items(), key=lambda kv: kv[1])  # 升冪
            m = len(items); denom = (m - 1) if m > 1 else 1
            top_sectors[d] = {sec for ri, (sec, _v) in enumerate(items)
                              if ri / denom >= (1.0 - _CS_SECTOR_TOP_FRAC)}
    # 3) 每股 sector_rel = N 日報酬 − 同板塊中位；TOPSEC 加前段板塊濾網
    out = {fac: {} for fac in factors_needed if fac in _CS_SECTOR_FACTORS}
    for code, (dts, smom) in smom_by_code.items():
        sec = sector_of.get(code)
        if not sec:
            continue
        for i in range(len(dts)):
            v = smom[i]
            if v != v:
                continue
            d = dts[i]
            med = sector_median.get(d, {}).get(sec)
            if med is None:
                continue
            rel = float(v) - med
            if "sector_rel" in out:
                out["sector_rel"].setdefault(d, {})[code] = rel
            if "sector_rel_topsec" in out and sec in top_sectors.get(d, set()):
                out["sector_rel_topsec"].setdefault(d, {})[code] = rel
    return out


# ── P0-1 R-CHIP-F：籌碼橫斷面因子（pure 計算，無 DB，可隔離測 PIT/算法）────────────
def _chip_factor_series(chip_series: list, adv_by_date: dict, factors_needed: set,
                        n: int = _CHIP_N_DEFAULT, sq_n: int = _CHIP_SQ_N,
                        sbl_n: int = _CHIP_SBL_N, sbl_field: str = _CHIP_SBL_FIELD_DEFAULT) -> dict:
    """單一個股的籌碼因子原始值時間序列（橫斷面排名前的 raw value）。純函式、無 DB、無 yfinance。

    輸入：
      chip_series : [(date, foreign, trust, dealer, short_bal, ms_ratio, sbl_bal), ...]，依 date 升冪（chip 公布日）。
                    （sbl_bal=借券賣出餘額；舊呼叫只給前 6 格時 want_sbl 分支自動略過，零回歸。）
      adv_by_date : {date: ADV}（該股各「交易日」的日均量，供 ÷ADV 標準化；缺/0 → 該日不算）。
      factors_needed ⊆ _CHIP_CS_FACTORS。
    回 {factor: {entry_date: raw_val}}。

    【給 Reviewer — PIT 防洩漏（核心）】chip date d 收盤後才公布 → 因子值對齊「次一交易日」
    才可進場。實作：用 chip_series[k] 算出的累積/變化值，key 落在 chip_series[k+1] 的 date
    （與既有 _chip_streak_dates 的 `series[k+1][0]` 逐字同精神）。故「進場日集合 ∩ 公布日(=每個
    series[k] 的 date 用於計算)」永不重疊 —— 進場永遠晚於資訊可得日一個交易日。

    因子定義：
      inst_net_buy  = N 日累積(外資+投信+自營)淨買(張) ÷ ADV（橫斷面 rank；smart-money 確認）
      trust_net_buy = N 日累積投信淨買(張) ÷ ADV（投信黏性，台股 edge）
      short_squeeze = 券資比(now) × max(0, 1 + 融券餘額 sq_n 日增幅)（高券資比+融券增=軋空壓力）
      sbl_short     = 借券賣出餘額 ÷ ADV（default sbl_turnover；informed-bear，dir 預設 −1）；
                      field=sbl_ratio（餘額÷流通股數，無股數來源→暫退回 turnover，標待補）；
                      field=sbl_chg（餘額 sbl_n 日變化率，機構新建空 flow）。
    """
    import numpy as np
    out = {fac: {} for fac in factors_needed if fac in _CHIP_CS_FACTORS}
    if not out:
        return out
    m = len(chip_series)
    if m < 2:
        return out  # 無「次一交易日」可落點 → PIT 下無可用值
    dates   = [r[0] for r in chip_series]
    foreign = np.array([(r[1] if r[1] is not None else 0.0) for r in chip_series], dtype=float)
    trust   = np.array([(r[2] if r[2] is not None else 0.0) for r in chip_series], dtype=float)
    dealer  = np.array([(r[3] if r[3] is not None else 0.0) for r in chip_series], dtype=float)
    short_b = np.array([(r[4] if r[4] is not None else np.nan) for r in chip_series], dtype=float)
    msr     = np.array([(r[5] if r[5] is not None else np.nan) for r in chip_series], dtype=float)
    # TW-SBL：借券賣出餘額（tuple 第 7 格；舊 6 格呼叫 → 全 NaN → want_sbl 分支自動略過，零回歸）。
    # 餘額==0 視為 NaN（無借券放空＝該日無方向性訊號，不應壓低排名為「最不看空」；亦規避歷史未回補列的偽 0）。
    sbl_b   = np.array([((r[6] if (len(r) > 6 and r[6] is not None) else np.nan)) for r in chip_series], dtype=float)
    inst    = foreign + trust + dealer
    want_inst  = "inst_net_buy"  in out
    want_trust = "trust_net_buy" in out
    want_sq    = "short_squeeze" in out
    want_sbl   = "sbl_short"     in out
    N  = max(int(n), 1)
    sN = max(int(sq_n), 1)
    sblN = max(int(sbl_n), 1)
    sbl_fld = str(sbl_field)
    for k in range(m - 1):                 # k=公布日索引；進場 = dates[k+1]（PIT：次一交易日）
        entry = dates[k + 1]
        if want_inst or want_trust:
            adv = adv_by_date.get(entry)   # ÷ADV 用進場日可得的日均量（PIT 安全）
            if adv and adv > 0 and k - N + 1 >= 0:
                if want_inst:
                    out["inst_net_buy"][entry] = float(inst[k - N + 1:k + 1].sum()) / adv
                if want_trust:
                    out["trust_net_buy"][entry] = float(trust[k - N + 1:k + 1].sum()) / adv
        if want_sq and k - sN >= 0:
            now = short_b[k]; base = short_b[k - sN]; r_now = msr[k]
            if r_now == r_now and now == now and base == base:   # 三者皆非 NaN
                if base > 0:
                    growth = (now - base) / base
                elif now > 0:
                    growth = 1.0                                  # 由 0 增為正 → 視為大增
                else:
                    growth = 0.0
                out["short_squeeze"][entry] = float(r_now) * max(0.0, 1.0 + growth)
        if want_sbl:
            # PIT：用 series[k] 的 SBL 餘額（公布日 k），值落 dates[k+1]（次一交易日）→ 進場∩公布日=∅，
            # 與 want_sq 逐字同精神。turnover/ratio 用單日餘額；sbl_chg 用 sblN 日變化率。
            bal_now = sbl_b[k]
            if sbl_fld == "sbl_chg":
                if k - sblN >= 0:
                    base = sbl_b[k - sblN]
                    if bal_now == bal_now and base == base:       # 皆非 NaN
                        if base > 0:
                            out["sbl_short"][entry] = (bal_now - base) / base
                        elif bal_now > 0:
                            out["sbl_short"][entry] = 1.0          # 由 0 增為正 → 視為大增
                        else:
                            out["sbl_short"][entry] = 0.0
            else:
                # sbl_turnover(default) = 餘額÷ADV；sbl_ratio 無流通股數來源 → 暫退回 turnover（標待補）。
                adv = adv_by_date.get(entry)
                if adv and adv > 0 and bal_now == bal_now:
                    out["sbl_short"][entry] = bal_now / adv
    return out


def _compute_chip_cs_factors(mkt: str, codes: list, feats: dict, factors_needed: set,
                             n: int = _CHIP_N_DEFAULT, sq_n: int = _CHIP_SQ_N,
                             adv_win: int = _CHIP_ADV_WIN,
                             sbl_n: int = _CHIP_SBL_N,
                             sbl_field: str = _CHIP_SBL_FIELD_DEFAULT) -> dict:
    """籌碼 cs 因子 orchestrator：批次讀 chip_snapshot（I/O）+ 由 feats 量能算 ADV → 呼叫 pure
    _chip_factor_series → 組成 {factor: {date: {code: raw_val}}}（與 _compute_sector_factors 同形狀，
    可直接餵 _cs_rank_pct 的排名迴圈）。籌碼僅 TW；他市場/無資料回空（複合自然縮小、誠實標記）。"""
    import numpy as np
    chip_needed = set(factors_needed) & _CHIP_CS_FACTORS
    out = {fac: {} for fac in chip_needed}
    if not chip_needed or not codes or str(mkt).upper() != "TW":
        return out
    # 一次性批次 DB 讀（主緒；與 _chip_streak_dates 同模式）
    try:
        con = db(); cur = con.cursor()
        qs = ",".join("?" * len(codes))
        rows = cur.execute(
            f"SELECT code, date, foreign_buy, itrust_buy, dealer_buy, short_balance, margin_short_ratio, "
            f"sbl_short_balance "
            f"FROM chip_snapshot WHERE code IN ({qs}) ORDER BY code, date", list(codes)
        ).fetchall()
        con.close()
    except Exception:
        return out
    by_code = {}
    for r in rows:
        # tuple 第 7 格 = sbl_short_balance（TW-SBL）；0→NaN（純函式內視 0 為無訊號，規避偽 0/未回補列）
        sbl_v = r[7] if (r[7] is not None and r[7] != 0) else None
        by_code.setdefault(r[0], []).append((r[1], r[2], r[3], r[4], r[5], r[6], sbl_v))
    aw = max(int(adv_win), 1)
    for code, series in by_code.items():
        f = feats.get(code)
        if not f:
            continue
        dts = f.get("dates"); vol = f.get("v")
        if not dts or vol is None:
            continue
        # ADV = 進場日(交易日)前 aw 日的日均量（同日收盤量於進場收盤已可觀測 → PIT 安全）
        adv_arr = pd.Series(np.asarray(vol, dtype=float)).rolling(
            aw, min_periods=max(5, aw // 2)).mean().to_numpy()
        adv_by_date = {dts[i]: float(adv_arr[i]) for i in range(len(dts))
                       if adv_arr[i] == adv_arr[i] and adv_arr[i] > 0}
        cf = _chip_factor_series(series, adv_by_date, chip_needed, n=n, sq_n=sq_n,
                                 sbl_n=sbl_n, sbl_field=sbl_field)
        for fac, dmap in cf.items():
            fv = out[fac]
            for d, val in dmap.items():
                if val == val:                       # not NaN
                    fv.setdefault(d, {})[code] = val
    return out


# ── P0-2 R-FUND-F：月營收動量 cs 因子（pure 計算，無 DB，可隔離測 PIT/算法）──────────────
def _revenue_factor_series(rev_records: list, trading_dates: list,
                           field: str = _REV_FIELD_DEFAULT, window: int = _REV_WIN_DEFAULT) -> dict:
    """單一個股的月營收動量原始值時間序列（橫斷面排名前的 raw value）。純函式、無 DB、無 yfinance。

    輸入：
      rev_records  : [(period_ym, yoy, mom, announce_date), ...]（月營收公布記錄；本函式內自行依 period_ym 排序）。
      trading_dates: 該股交易日清單（升冪，'YYYY-MM-DD'；來自 feats['dates']）。
      field        : 'yoy'(年增率) 或 'mom'(月增率)。
      window       : 回看 N 個『已公布』月份取均值（動量平滑；window=1 即最近一個已公布月）。
    回 {trading_date: raw_val}（fill-forward：每交易日用『當下已公布』的最近 window 月均值）。

    【給 Reviewer — 🔑PIT 防洩漏（核心，最致命）】每交易日 d 只納入 announce_date < d（嚴格小於）的
    營收記錄。announce_date=次月10日（_announce_date_for_period），故 d 嚴格晚於『營收所屬月的次月10日』
    → 進場日永不落在營收所屬月內、且嚴格晚於法定公布日 →「進場日 ∩ 營收所屬月 = ∅」「進場日 > 公布日」
    雙成立（隔離測 test_revenue_pit 已斷言）。fill-forward 僅用『已公布』記錄向後填值（基本面為慢變數，
    下一筆公布前持有同一動量），j 指標單調前移、不回看未來，不引入任何未來資訊。"""
    out = {}
    if not rev_records or not trading_dates:
        return out
    fld_idx = 1 if str(field) == "yoy" else 2   # record tuple: (period_ym, yoy, mom, announce_date)
    W = max(int(window), 1)
    recs = sorted(rev_records, key=lambda r: r[0])   # 依 period_ym 升冪（→ announce_date 亦升冪）
    n = len(recs)
    j = 0                                            # 指標：announce_date < d 的已公布記錄數（單調前移）
    for d in trading_dates:
        while j < n and recs[j][3] < d:             # 嚴格 < → PIT：法定公布日(次月10日)當天不進場
            j += 1
        if j == 0:
            continue                                 # 此交易日尚無任何已公布月營收
        lo = max(0, j - W)
        vals = [recs[k][fld_idx] for k in range(lo, j) if recs[k][fld_idx] is not None]
        if vals:
            out[d] = float(sum(vals)) / len(vals)
    return out


def _compute_revenue_cs_factors(mkt: str, codes: list, feats: dict, factors_needed: set,
                                window: int = _REV_WIN_DEFAULT, field: str = _REV_FIELD_DEFAULT) -> dict:
    """月營收動量 cs 因子 orchestrator：批次讀 revenue_monthly（I/O）→ 對每股呼叫 pure
    _revenue_factor_series → 組成 {factor: {date: {code: raw_val}}}（與 _compute_chip_cs_factors 同形狀，
    可直接餵 _cs_rank_pct 排名迴圈）。月營收僅 TW；他市場/無資料回空（複合自然縮小、誠實標記）。"""
    rev_needed = set(factors_needed) & _REVENUE_CS_FACTORS
    out = {fac: {} for fac in rev_needed}
    if not rev_needed or not codes or str(mkt).upper() != "TW":
        return out
    try:
        con = db(); cur = con.cursor()
        qs = ",".join("?" * len(codes))
        rows = cur.execute(
            f"SELECT code, period_ym, yoy, mom, announce_date FROM revenue_monthly "
            f"WHERE market='TW' AND code IN ({qs}) ORDER BY code, period_ym", list(codes)
        ).fetchall()
        con.close()
    except Exception:
        return out
    by_code = {}
    for r in rows:
        by_code.setdefault(r[0], []).append((r[1], r[2], r[3], r[4]))   # (period_ym, yoy, mom, announce_date)
    fv = out["revenue_mom"]
    for code, recs in by_code.items():
        f = feats.get(code)
        if not f:
            continue
        dts = f.get("dates")
        if not dts:
            continue
        series = _revenue_factor_series(recs, dts, field=field, window=window)
        for d, val in series.items():
            if val == val:                          # not NaN
                fv.setdefault(d, {})[code] = val
    return out


# ── Type 2 US-SI：美股 short_interest cs 因子（pure 計算，無 DB，可隔離測 PIT/算法）──────────
def _short_interest_series(si_records: list, trading_dates: list,
                           field: str = _US_SI_FIELD_DEFAULT, window: int = _US_SI_WIN_DEFAULT) -> dict:
    """單一美股 short interest 原始值時間序列（橫斷面排名前的 raw value）。純函式、無 DB、無 yfinance。

    輸入：
      si_records   : [(settlement_date, publish_date, si_ratio, days_to_cover, si_pct_float), ...]
                     （FINRA 半月頻 SI 申報；本函式內自行依 settlement_date 排序）。
      trading_dates: 該股交易日清單（升冪，'YYYY-MM-DD'；來自 feats['dates']）。
      field        : 'si_ratio'(=short_shares/ADV，即 days_to_cover) / 'days_to_cover' / 'si_pct_float'。
      window       : 回看 N 筆『已公布』申報取均值（window=1 即最近一筆已公布 SI）。
    回 {trading_date: raw_val}（fill-forward：每交易日用『publish_date < d（嚴格）』的最近 window 筆均值）。

    【給 Reviewer — 🔑PIT 防洩漏（核心，最致命）】每交易日 d 只納入 publish_date < d（嚴格小於）的
    SI 申報。FINRA settlement 後約8營業日才 publish_date 公布，故 d 嚴格晚於『FINRA 公布日』→ 進場日
    永不落在 settlement 期內、且嚴格晚於公布日 →「進場日 ∩ settlement 期 = ∅」「進場日 > 公布日」雙成立
    （隔離測 test_us_si_pit 已斷言）。fill-forward 僅用『已公布』記錄向後填值（SI 為半月慢變數，下一筆
    公布前持有同一值），j 指標單調前移、不回看未來，不引入任何未來資訊。鏡像 _revenue_factor_series。"""
    out = {}
    if not si_records or not trading_dates:
        return out
    # record tuple: (settlement_date, publish_date, si_ratio, days_to_cover, si_pct_float)
    fld_idx = {"si_ratio": 2, "days_to_cover": 3, "si_pct_float": 4}.get(str(field), 2)
    W = max(int(window), 1)
    recs = sorted(si_records, key=lambda r: r[0])    # 依 settlement_date 升冪（→ publish_date 亦升冪）
    n = len(recs)
    j = 0                                            # 指標：publish_date < d 的已公布記錄數（單調前移）
    for d in trading_dates:
        while j < n and recs[j][1] < d:             # 嚴格 < → PIT：FINRA 公布日當天不進場
            j += 1
        if j == 0:
            continue                                 # 此交易日尚無任何已公布 SI 申報
        lo = max(0, j - W)
        vals = [recs[k][fld_idx] for k in range(lo, j) if recs[k][fld_idx] is not None]
        if vals:
            out[d] = float(sum(vals)) / len(vals)
    return out


def _compute_us_chip_cs_factors(mkt: str, codes: list, feats: dict, factors_needed: set,
                                field: str = _US_SI_FIELD_DEFAULT, window: int = _US_SI_WIN_DEFAULT) -> dict:
    """美股籌碼 cs 因子 orchestrator：批次讀 us_short_interest（I/O）→ 對每股呼叫 pure
    _short_interest_series → 組成 {factor: {date: {code: raw_val}}}（與 _compute_chip_cs_factors 同形狀，
    可直接餵 _cs_rank_pct 排名迴圈）。美股籌碼僅 US；他市場/無資料回空（複合自然縮小、誠實標記）。
    【鏡像 _compute_chip_cs_factors，US-gated（!= "US" → 回空），與 TW chip gate 互斥、零交叉污染。】"""
    us_chip_needed = set(factors_needed) & _US_CHIP_CS_FACTORS
    out = {fac: {} for fac in us_chip_needed}
    if not us_chip_needed or not codes or str(mkt).upper() != "US":
        return out
    try:
        con = db(); cur = con.cursor()
        qs = ",".join("?" * len(codes))
        rows = cur.execute(
            f"SELECT code, settlement_date, publish_date, days_to_cover, short_pct_float "
            f"FROM us_short_interest WHERE code IN ({qs}) ORDER BY code, settlement_date", list(codes)
        ).fetchall()
        con.close()
    except Exception:
        return out
    by_code = {}
    for r in rows:
        # si_ratio == days_to_cover（short_shares/ADV）；tuple 對齊 _short_interest_series 期望順序
        # (settlement_date, publish_date, si_ratio, days_to_cover, si_pct_float)
        by_code.setdefault(r[0], []).append((r[1], r[2], r[3], r[3], r[4]))
    fv = out["short_interest"]
    for code, recs in by_code.items():
        f = feats.get(code)
        if not f:
            continue
        dts = f.get("dates")
        if not dts:
            continue
        series = _short_interest_series(recs, dts, field=field, window=window)
        for d, val in series.items():
            if val == val:                          # not NaN
                fv.setdefault(d, {})[code] = val
    return out


# ── D1 TDCC：集保大戶持股結構 cs 因子（pure 計算，無 DB，可隔離測 PIT/算法）──────────────────
def _holder_conc_series(holder_records: list, trading_dates: list, factor: str,
                        field: str = _TDCC_CONC_FIELD_DEFAULT, window: int = _TDCC_WIN_DEFAULT) -> dict:
    """單一個股集保大戶持股結構因子原始值時間序列（橫斷面排名前的 raw value）。純函式、無 DB、無 yfinance。

    輸入：
      holder_records: [(settlement_date, publish_date, conc_gt400, conc_gt1000), ...]
                      （TDCC 週頻股權分散表；本函式內自行依 settlement_date 排序）。
      trading_dates : 該股交易日清單（升冪，'YYYY-MM-DD'；來自 feats['dates']）。
      factor        : 'holder_concentration'（level）/ 'holder_conc_delta'（已公布週變化）。
      field         : 'conc_gt400'(>400張，級距12-15占比) / 'conc_gt1000'(>1000張，級距15占比)。
      window        : level→回看 N 筆已公布週取均值；delta→對 N 個已公布週前的變化。
    回 {trading_date: raw_val}（fill-forward：每交易日用『publish_date < d（嚴格）』的已公布記錄）。

    【給 Reviewer — 🔑PIT 防洩漏（核心，最致命）】每交易日 d 只納入 publish_date < d（嚴格小於）的週記錄。
    TDCC 結算後約4營業日才 publish_date 公布，故 d 嚴格晚於『公布日』→ 進場日永不落在結算週內、且嚴格晚於
    公布日 →「進場日 ∩ 結算週 = ∅」「進場日 > 公布日」雙成立（隔離測 test_tdcc_holder_pit 已斷言：特意
    讓結算日落在 T 內者驗未被用）。fill-forward 僅用已公布記錄向後填值（週慢變數，下一筆公布前持有同值），
    j 指標單調前移、不回看未來。delta=已公布最新值 − N 週前已公布值（皆嚴格 < 進場日）→ 不引入未來資訊。
    鏡像 _short_interest_series / _revenue_factor_series。"""
    out = {}
    if not holder_records or not trading_dates:
        return out
    # record tuple: (settlement_date, publish_date, conc_gt400, conc_gt1000)
    fld_idx = {"conc_gt400": 2, "conc_gt1000": 3}.get(str(field), 2)
    W = max(int(window), 1)
    is_delta = (str(factor) == "holder_conc_delta")
    recs = sorted(holder_records, key=lambda r: r[0])    # 依 settlement_date 升冪（→ publish_date 亦升冪）
    n = len(recs)
    j = 0                                                # 指標：publish_date < d 的已公布記錄數（單調前移）
    for d in trading_dates:
        while j < n and recs[j][1] < d:                  # 嚴格 < → PIT：公布日當天不進場
            j += 1
        if j == 0:
            continue                                     # 此交易日尚無任何已公布週記錄
        if is_delta:
            if j - 1 - W < 0:
                continue                                 # 不足 W+1 筆已公布 → 無法算變化
            now_v  = recs[j - 1][fld_idx]
            base_v = recs[j - 1 - W][fld_idx]
            if now_v is not None and base_v is not None:
                out[d] = float(now_v) - float(base_v)
        else:
            lo = max(0, j - W)
            vals = [recs[k][fld_idx] for k in range(lo, j) if recs[k][fld_idx] is not None]
            if vals:
                out[d] = float(sum(vals)) / len(vals)
    return out


def _compute_tdcc_cs_factors(mkt: str, codes: list, feats: dict, factors_needed: set,
                             field: str = _TDCC_CONC_FIELD_DEFAULT, window: int = _TDCC_WIN_DEFAULT) -> dict:
    """集保大戶結構 cs 因子 orchestrator：批次讀 tdcc_holder（I/O）→ 對每股呼叫 pure _holder_conc_series
    → 組成 {factor: {date: {code: raw_val}}}（與 _compute_chip_cs_factors 同形狀，可直接餵 _cs_rank_pct）。
    集保資料僅台股(TW)；他市場/無資料回空（複合自然縮小、誠實標記）。
    【鏡像 _compute_us_chip_cs_factors，TW-gated（!= "TW" → 回空），與美股 chip gate 互斥、零交叉污染。】"""
    tdcc_needed = set(factors_needed) & _TDCC_CS_FACTORS
    out = {fac: {} for fac in tdcc_needed}
    if not tdcc_needed or not codes or str(mkt).upper() != "TW":
        return out
    try:
        con = db(); cur = con.cursor()
        qs = ",".join("?" * len(codes))
        rows = cur.execute(
            f"SELECT code, settlement_date, publish_date, conc_gt400, conc_gt1000 "
            f"FROM tdcc_holder WHERE code IN ({qs}) ORDER BY code, settlement_date", list(codes)
        ).fetchall()
        con.close()
    except Exception:
        return out
    by_code = {}
    for r in rows:
        # tuple 對齊 _holder_conc_series 期望順序 (settlement_date, publish_date, conc_gt400, conc_gt1000)
        by_code.setdefault(r[0], []).append((r[1], r[2], r[3], r[4]))
    for code, recs in by_code.items():
        f = feats.get(code)
        if not f:
            continue
        dts = f.get("dates")
        if not dts:
            continue
        for fac in tdcc_needed:
            series = _holder_conc_series(recs, dts, fac, field=field, window=window)
            fv = out[fac]
            for d, val in series.items():
                if val == val:                          # not NaN
                    fv.setdefault(d, {})[code] = val
    return out


# ── Type 1a：價格族 cs 因子（pure 計算，無 DB/yfinance，可隔離測 PIT/算法）──────────────
def _price_cs_factor_series(dates: list, c, bench_map: dict, tnx_map: dict, dxy_map: dict,
                            factors_needed: set, beta_win: int = _PRICE_BETA_WIN,
                            rev_n: int = _PRICE_REV_N,
                            min_frac: float = _PRICE_BETA_MIN_FRAC) -> dict:
    """單一個股的價格族 cs 因子原始值時間序列（橫斷面排名前的 raw value）。純函式、無 DB、無 yfinance。

    輸入：
      dates    : 該股交易日清單（升冪，'YYYY-MM-DD'；來自 feats['dates']）。
      c        : 該股收盤序列（對齊 dates）。
      bench_map: {date: 指數收盤}（low_beta 回歸子；沿用 _get_benchmark_close_map，^TWII/SPY）。
      tnx_map  : {date: ^TNX 收盤(US10Y yield×10)}（rate_sensitivity 回歸子；None=不算該因子）。
      dxy_map  : {date: DXY 收盤}（dollar_sensitivity 回歸子；None=不算該因子）。
      factors_needed ⊆ _PRICE_CS_FACTORS。
    回 {factor: {entry_date: raw_val}}。

    因子定義（raw value，橫斷面 rank 在 _cs_rank_pct）：
      low_beta           = 個股日報酬對「指數日報酬」滾動 β（dir=−1 取低 β＝BAB）。
      rate_sensitivity   = 個股日報酬對「^TNX yield 差分(Δ)」滾動 β（久期敏感；yield 用差分非報酬）。
      dollar_sensitivity = 個股日報酬對「DXY 日報酬(pct)」滾動 β（匯率暴險）。
      short_rev          = −(個股 N 日報酬%)（近 N 日跌最多→高 rank→反轉做多）。

    【給 Reviewer — 🔑PIT 防洩漏（核心）】每因子在 index i 用「≤dts[i] 的資料」算出 stat（β 窗
    [i−W+1, i]、N 日報酬 c[i]/c[i−N]），key 落在 dts[i+1]（次一交易日）→ 進場日的因子值只含
    ≤T−1 資料，與 _chip_factor_series/_revenue_factor_series 的『次一交易日落點』PIT 修法逐字同精神
    （進場∩資訊可得當日=∅、進場>資訊可得日一個交易日）。回歸子序列以 dates 對齊，缺值(US/TW 假日
    不重疊)→該日變動 NaN→剔出回歸樣本（PIT 安全，不外插未來）。"""
    import numpy as np
    out = {fac: {} for fac in factors_needed if fac in _PRICE_CS_FACTORS}
    if not out:
        return out
    n = len(dates)
    if n < 2:
        return out  # 無「次一交易日」可落點 → PIT 下無可用值
    c = np.asarray(c, dtype=float)

    # 個股日報酬（簡單報酬；r_s[i] 為 dts[i−1]→dts[i] 的報酬）
    r_s = np.full(n, np.nan)
    for i in range(1, n):
        if c[i - 1] > 0:
            r_s[i] = c[i] / c[i - 1] - 1.0

    want_lowbeta = "low_beta" in out
    want_rate    = "rate_sensitivity" in out
    want_dollar  = "dollar_sensitivity" in out
    want_srev    = "short_rev" in out

    def _aligned(dmap):
        return np.array([(dmap.get(d, np.nan) if dmap else np.nan) for d in dates], dtype=float)

    def _ret_series(dmap):       # 價格型回歸子（指數/DXY）→ 日報酬(pct)
        arr = _aligned(dmap); o = np.full(n, np.nan)
        for i in range(1, n):
            if arr[i - 1] == arr[i - 1] and arr[i] == arr[i] and arr[i - 1] > 0:
                o[i] = arr[i] / arr[i - 1] - 1.0
        return o

    def _diff_series(dmap):      # yield 型回歸子(^TNX)→ 日差分(Δyield，避免近零 pct 爆量)
        arr = _aligned(dmap); o = np.full(n, np.nan)
        for i in range(1, n):
            if arr[i - 1] == arr[i - 1] and arr[i] == arr[i]:
                o[i] = arr[i] - arr[i - 1]
        return o

    r_idx = _ret_series(bench_map) if want_lowbeta else None
    r_tnx = _diff_series(tnx_map) if (want_rate and tnx_map) else None
    r_dxy = _ret_series(dxy_map) if (want_dollar and dxy_map) else None

    W = max(int(beta_win), 5)

    def _beta_at(i, rf):
        """r_s 對 rf 在窗 [i−W+1, i] 的 OLS β（資料 ≤dts[i]）；樣本不足/零變異 → NaN。"""
        ys = r_s[i - W + 1:i + 1]; xs = rf[i - W + 1:i + 1]
        m = (xs == xs) & (ys == ys)
        if int(m.sum()) < W * min_frac:
            return np.nan
        x = xs[m]; y = ys[m]
        xm = x.mean(); sxx = float(((x - xm) ** 2).sum())
        if sxx <= 0:
            return np.nan
        return float(((x - xm) * (y - y.mean())).sum() / sxx)

    Nr = max(int(rev_n), 1)
    for i in range(n - 1):                  # i=資訊可得日索引；進場 = dts[i+1]（PIT：次一交易日）
        entry = dates[i + 1]
        if i >= W - 1:
            if want_lowbeta:
                b = _beta_at(i, r_idx)
                if b == b:
                    out["low_beta"][entry] = b
            if r_tnx is not None:
                b = _beta_at(i, r_tnx)
                if b == b:
                    out["rate_sensitivity"][entry] = b
            if r_dxy is not None:
                b = _beta_at(i, r_dxy)
                if b == b:
                    out["dollar_sensitivity"][entry] = b
        if want_srev and i - Nr >= 0 and c[i - Nr] > 0:
            out["short_rev"][entry] = -((c[i] / c[i - Nr] - 1.0) * 100.0)
    return out


def _compute_price_cs_factors(mkt: str, codes: list, feats: dict, factors_needed: set,
                              bench_map: dict) -> dict:
    """價格族 cs 因子 orchestrator：rate/dollar 需要的免費宏觀序列一次抓齊（^TNX/DXY，30min 快取）→
    對每股呼叫 pure _price_cs_factor_series → 組成 {factor: {date: {code: raw_val}}}（與
    _compute_chip_cs_factors/_compute_revenue_cs_factors 同形狀，直接餵 _cs_rank_pct 排名迴圈）。
    宏觀序列抓取窗由 feats 日期範圍決定（PIT：只抓回測窗內，不引入未來；無資料→該因子自然縮小、誠實標）。"""
    price_needed = set(factors_needed) & _PRICE_CS_FACTORS
    out = {fac: {} for fac in price_needed}
    if not price_needed or not codes:
        return out
    tnx_map = dxy_map = None
    need_rate = "rate_sensitivity" in price_needed
    need_dollar = "dollar_sensitivity" in price_needed
    if need_rate or need_dollar:
        alld = set()
        for code in codes:
            f = feats.get(code)
            if f and f.get("dates"):
                alld.update(f["dates"])
        if alld:
            lo, hi = min(alld), max(alld)
            try:                              # yfinance end 為 exclusive → +1 日確保含 hi
                from datetime import datetime, timedelta
                hi_x = (datetime.strptime(hi, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            except Exception:
                hi_x = hi
            if need_rate:
                tnx_map = _fetch_index_close_map("^TNX", lo, hi_x)
            if need_dollar:
                dxy_map = _fetch_index_close_map("DX-Y.NYB", lo, hi_x)
    for code in codes:
        f = feats.get(code)
        if not f:
            continue
        dts = f.get("dates"); c = f.get("c")
        if not dts or c is None:
            continue
        cf = _price_cs_factor_series(dts, c, bench_map, tnx_map, dxy_map, price_needed)
        for fac, dmap in cf.items():
            fv = out[fac]
            for d, val in dmap.items():
                if val == val:                # not NaN
                    fv.setdefault(d, {})[code] = val
    return out


def _emit_trades(sid: str, trig, f: dict, rank_map, thresh: float, dst: list):
    """記錄某策略的逐筆 trade（entry_date, exit_date, net_ret%）到 dst。
       trig=None → 純橫斷面策略（每日皆候選，由 rank 決定）；trig 陣列 → 既有訊號策略。
       rank_map=None → 不套橫斷面濾網（預設行為，零回歸）；
       rank_map={date:pct} → 進場須 pct ≥ thresh（top K%）。open_until 自動去重疊倉。"""
    oexit, oret, dts = f["outcome_exit"], f["outcome_ret"], f["dates"]
    open_until = -1
    for i in range(f["n"]):
        if trig is not None and not trig[i]:
            continue
        if i > open_until and oexit[i] >= 0 and oret[i] == oret[i]:
            if rank_map is not None:
                pct = rank_map.get(dts[i])
                if pct is None or pct < thresh:
                    continue
            dst.append((dts[i], dts[oexit[i]], float(oret[i])))
            open_until = oexit[i]


def _fast_base_rate_market(mkt: str, codes: list, start: str, end: str,
                           buy_ids: list, preloaded: tuple,
                           progress_cb=None, should_cancel=None,
                           cs_top_k: float = _CS_DEFAULT_K, cs_overlay_factor: str = None,
                           benchmark: str = "twii") -> dict:
    """
    trade-level 事件研究：每股算一次 outcome 表+指標，各進場策略只查表統計。
    回傳 {strategy: summary_dict}，summary 欄位對齊 _extract_row 期望。

    R-CS（alpha agenda Tier 1）：buy_ids 含 A1_RAW/A1_REL/A1_RESID（純橫斷面策略），
    或 cs_overlay_factor 不為 None（對既有訊號加「∧ 橫斷面 rank ∈ top K%」濾網）時，
    先跑一趟「橫斷面 context pass」算全 universe 每日因子百分位，再用於進場濾網。
    無橫斷面需求時走原單趟路徑（零回歸、零額外記憶體）。
    """
    import numpy as np
    all_dates, bar_data = preloaded
    bench_map = _get_benchmark_close_map(mkt, start, end)  # CS 相對/殘差因子訊號用（^TWII/SPY，不變）

    # ── 公平基準（§三E）：逐筆超額（ALPHA 閘）的對照表，可選 twii/equal_weight/0050 ──
    # 注意：訊號定義(rel_str/resid_mom) 仍對 ^TWII，只有「ALPHA 閘的逐筆超額」換基準，
    # 以隔離測「閘基準是否偏誤」。benchmark='twii' 時 excess_bench_map 即 bench_map（零回歸）。
    _bench = (benchmark or "twii").lower()
    if _bench == "equal_weight":
        excess_bench_map = _equal_weight_bench_map(codes, all_dates, bar_data)
        print(f"[base-rate] {mkt}: excess benchmark = 等權 universe ({len(excess_bench_map)} days)", flush=True)
    elif _bench == "0050" and mkt == "TW":
        _m0050 = _fetch_index_close_map("0050.TW", start, end)
        excess_bench_map = _m0050 if _m0050 else bench_map
        print(f"[base-rate] {mkt}: excess benchmark = 0050.TW "
              f"({len(excess_bench_map)} days{'' if _m0050 else ', 抓取失敗→退回市值加權'})", flush=True)
    else:
        excess_bench_map = bench_map  # twii 預設（或 0050 在非 TW 市場 → 退回該市場指數）

    strat_trades = {sid: [] for sid in buy_ids}  # sid -> list of (entry_date, exit_date, net_ret%)
    cs_ids = [s for s in buy_ids if s in _BASE_RATE_CS_STRATS]
    event_ids = [s for s in buy_ids if s in _BASE_RATE_EVENT_STRATS]   # A5：事件型（財報後漂移），trigger 由 earnings_dates 算
    non_cs_ids = [s for s in buy_ids if s not in _BASE_RATE_CS_STRATS and s not in _BASE_RATE_EVENT_STRATS]
    # 每個 sid 用哪個因子排名：橫斷面策略用自身因子；既有訊號策略用 overlay 因子（None=不濾）；事件策略不套橫斷面
    sid_factor = {}
    for s in buy_ids:
        sid_factor[s] = _BASE_RATE_CS_STRATS.get(s) or (cs_overlay_factor if s in non_cs_ids else None)
    factors_needed = {fac for fac in sid_factor.values() if fac}
    thresh = 1.0 - float(cs_top_k)
    # A5 事件型：預載財報日期映射（首跑逐檔抓 yfinance .earnings_dates，之後讀表；覆蓋率低的市場誠實縮小）
    earnings_by_code = _get_earnings_map(mkt, codes) if event_ids else {}

    if not factors_needed:
        # ── 預設路徑（無橫斷面因子）：原單趟、低記憶體行為，逐字保留 ──
        for ci, code in enumerate(codes):
            if should_cancel and should_cancel():
                break
            if progress_cb and ci % 10 == 0:
                progress_cb(ci, len(codes))
            f = _compute_stock_features(code, all_dates, bar_data)
            if not f:
                continue
            for sid in buy_ids:
                if sid in _BASE_RATE_EVENT_STRATS:   # A5：財報後漂移事件 trigger（earnings_dates + 後驚奇代理）
                    etrig = _a5_pead_triggers(f["dates"], f["c"], bench_map, earnings_by_code.get(code, []))
                    _emit_trades(sid, etrig, f, None, thresh, strat_trades[sid])
                else:
                    _emit_trades(sid, _detect_triggers(sid, f), f, None, thresh, strat_trades[sid])
    else:
        # ── 橫斷面路徑：兩趟（context pass 算排名 → 進場 pass 套濾網） ──
        # context pass：先建每股 feats（outcome 表/指標）；橫斷面排名抽到 _cs_rank_pct
        # （單一真實來源，與複合搜尋引擎 _eval_composite_market 共用；排名邏輯與原內聯逐字等價，
        #  見隔離測 test_extract_equiv → 零回歸）。
        feats = {}
        for ci, code in enumerate(codes):
            if should_cancel and should_cancel():
                break
            if progress_cb and ci % 10 == 0:
                progress_cb(ci, len(codes) * 2)
            f = _compute_stock_features(code, all_dates, bar_data)
            if not f:
                continue
            feats[code] = f
        rank_pct = _cs_rank_pct(mkt, codes, feats, factors_needed, bench_map)
        # 進場 pass
        for ci, code in enumerate(codes):
            if should_cancel and should_cancel():
                break
            if progress_cb and ci % 10 == 0:
                progress_cb(len(codes) + ci, len(codes) * 2)
            f = feats.get(code)
            if not f:
                continue
            for sid in non_cs_ids:
                fac = sid_factor[sid]
                rmap = rank_pct.get(fac, {}).get(code) if fac else None
                _emit_trades(sid, _detect_triggers(sid, f), f, rmap, thresh, strat_trades[sid])
            for sid in cs_ids:
                rmap = rank_pct.get(sid_factor[sid], {}).get(code, {})
                _emit_trades(sid, None, f, rmap, thresh, strat_trades[sid])
            for sid in event_ids:   # A5：事件型 trigger（earnings_dates + 後驚奇代理），不套橫斷面濾網
                etrig = _a5_pead_triggers(f["dates"], f["c"], bench_map, earnings_by_code.get(code, []))
                _emit_trades(sid, etrig, f, None, thresh, strat_trades[sid])

    # 彙總（逐筆 trade → summary 抽到 _summarize_trades，單一真實來源，與複合搜尋引擎共用）
    out = {}
    for sid in buy_ids:
        out[sid] = _summarize_trades(strat_trades[sid], mkt, excess_bench_map,
                                     cs_factor=sid_factor.get(sid), cs_top_k=cs_top_k)
    return out


def _extract_row(s: dict, mkt: str, buy_strat: str) -> dict:
    """從 backtest summary 萃取 base-rate row。
    三級 status（由 AIF 拍板，R2）：
      FAIL         — 未過正期望值閘（N / 報酬CI下界 / P/L 任一不過）
      POSITIVE_EV  — 過正EV閘但 beta 濾網未過或無法評估（可交易，帶 beta 疑慮）
      ALPHA        — 過正EV閘 + 過 beta 濾網（真 alpha keeper）；pass==ALPHA。
    勝率不再決定通過，僅作族別分類（均值回歸型 / 趨勢型）。"""
    T = _BASE_RATE_THRESHOLDS
    n_trades = s.get("ci_n_trades", s.get("total_trades", 0))
    win_rate = s.get("win_rate_pct", s.get("win_rate", 0))
    wr_ci = s.get("win_rate_ci95", [None, None])
    avg_ret = s.get("avg_trade_return_pct", 0)
    ar_ci = s.get("avg_return_ci95", [None, None])
    pl_ratio = s.get("profit_loss_ratio", s.get("pl_ratio", 0))
    max_dd = s.get("max_drawdown_pct", s.get("max_drawdown", 0))
    sharpe = s.get("sharpe_ratio", s.get("sharpe", 0))
    # beta 濾網原料（來自引擎彙總）
    subperiod_pos     = s.get("subperiod_pos", "0/0")
    subperiod_frac    = s.get("subperiod_frac")
    subperiod_buckets = s.get("subperiod_buckets", 0)
    excess_avg        = s.get("excess_avg")
    excess_ci         = s.get("excess_ci95", [None, None]) or [None, None]
    excess_n          = s.get("excess_n", 0)
    # §5.3 補強欄位 + R-CS 因子標記
    info_ratio        = s.get("info_ratio")
    downside_avg      = s.get("downside_excess_avg")
    downside_n        = s.get("downside_excess_n", 0)
    cs_factor         = s.get("cs_factor")
    cs_top_k          = s.get("cs_top_k")

    # ── 族別分類（勝率 CI 下界 > 50 → 均值回歸型；否則趨勢型）；不決定通過 ──
    wr_lo = wr_ci[0] if wr_ci[0] is not None else 0
    family = "均值回歸型" if wr_lo > 50 else "趨勢型"

    # ── 第一關：正期望值閘（avg_return 95%CI 下界 > 0 且 P/L ≥ 1.2 且 N 足） ──
    ar_lo = ar_ci[0] if ar_ci[0] is not None else None
    ev_fail = []
    if n_trades < T["min_N"]:
        ev_fail.append(f"N={n_trades}<{T['min_N']}")
    if ar_lo is None or ar_lo <= T["avg_ret_ci_lower_gt"]:
        ev_fail.append(f"報酬CI下界{ar_lo if ar_lo is not None else 'NA'}≤{T['avg_ret_ci_lower_gt']}")
    if (pl_ratio or 0) < T["min_pl"]:
        ev_fail.append(f"PL={pl_ratio}<{T['min_pl']}")
    positive_ev = not ev_fail

    # ── 第二關：beta 濾網（子期間穩定 + 超額報酬，兩項皆過才 PASS） ──
    sub_insufficient = subperiod_buckets < T["subperiod_min_buckets"]
    exc_insufficient = excess_n < 5 or excess_ci[0] is None
    sub_ok = subperiod_frac is not None and subperiod_frac >= T["subperiod_pos_frac_min"]
    exc_ok = excess_ci[0] is not None and excess_ci[0] > T["excess_ci_lower_gt"]
    if sub_insufficient or exc_insufficient:
        beta_filter = "INSUFFICIENT"
    else:
        beta_filter = "PASS" if (sub_ok and exc_ok) else "FAIL"

    # ── 三級 status ──
    if not positive_ev:
        status = "FAIL"
    elif beta_filter == "PASS":
        status = "ALPHA"
    else:
        status = "POSITIVE_EV"
    passed = (status == "ALPHA")

    # ── reason ──
    if status == "FAIL":
        reason = "; ".join(ev_fail)
    elif status == "ALPHA":
        reason = "ALPHA：過正EV+beta濾網"
    else:  # POSITIVE_EV
        bp = []
        if beta_filter == "INSUFFICIENT":
            if sub_insufficient:
                bp.append(f"子期間合格桶{subperiod_buckets}<{T['subperiod_min_buckets']}")
            if exc_insufficient:
                bp.append(f"超額樣本{excess_n}<5")
        else:
            if not sub_ok:
                bp.append(f"子期間僅{subperiod_pos}正<{int(T['subperiod_pos_frac_min']*100)}%")
            if not exc_ok:
                bp.append(f"超額CI下界{excess_ci[0]}≤0")
        reason = "可交易(beta疑慮)：" + ("; ".join(bp) if bp else "未過beta濾網")
        # §5.3：多頭樣本偏誤下，防禦型 alpha 易被超額閘誤殺。若 2022 空頭桶逐筆超額為正，
        # 標記提示（不改判級），讓人不把「在空頭仍贏大盤」的防禦型訊號當純 beta 丟掉。
        if downside_avg is not None and downside_avg > 0 and downside_n >= 5:
            reason += f" ｜ 2022空頭超額+{downside_avg}%(防禦型候選)"

    return {
        "market": mkt, "strategy": buy_strat,
        "N": n_trades,
        "win_rate": round(win_rate, 1) if win_rate else 0,
        "win_rate_ci95": wr_ci,
        "avg_return": round(avg_ret, 2) if avg_ret else 0,
        "avg_return_ci95": ar_ci,
        "pl_ratio": round(pl_ratio, 2) if pl_ratio else 0,
        "max_dd": round(max_dd, 1) if max_dd else 0,
        "sharpe": round(sharpe, 2) if sharpe else 0,
        "family": family,
        "subperiod_pos": subperiod_pos,
        "subperiod_frac": subperiod_frac,
        "excess_avg": excess_avg,
        "excess_ci95": excess_ci,
        "info_ratio": info_ratio,
        "downside_excess_avg": downside_avg,
        "downside_excess_n": downside_n,
        "cs_factor": cs_factor,
        "cs_top_k": cs_top_k,
        "beta_filter": beta_filter,
        "status": status,
        "pass": passed,
        "reason": reason,
    }


# ── R-DOWNSIDE（gate-label-conditional-reframe F 節 R-DOWNSIDE + B.4 防禦型判準切換 + A③，2026-06-27）──
#   把 downside_excess 正式接成「防禦型」訊號的判準路徑：下行桶(RISK_OFF/CRISIS/2022)逐筆超額
#   bootstrap CI_low > 0 ＋ 全期 Sharpe/IR 為正 ＋ 與指定順勢 alpha（如 A4）逐筆報酬相關(要低/負)。
#
#   ★命門＝零回歸：只有被標/pre-register 為「defensive」的訊號才走此下行判準路徑；其餘訊號
#     一律走既有標準超額閘（_extract_row 原樣），輸出位元級不變。目前**無任何 defensive 訊號**
#     登記（_DEFENSIVE_SIGNAL_IDS 為空）→ 整條防禦型分支 latent，對所有現有訊號零行為改動。
#     未來 pre-register/R-PROD-LABEL-WIRE 標 defensive 才會啟動（屆時由其補 regime-bucket 切片與
#     對 A4 逐筆報酬相關係數的實算；本件提供判準計算與路由地基）。
_DEFENSIVE_SIGNAL_IDS: set = set()   # 被標/pre-register 為防禦型(做多低beta/低特質波動)的 signal_id；空＝R-DOWNSIDE latent
_DEFENSIVE_CORR_MAX = 0.30           # 與順勢 alpha 逐筆報酬相關上限（B.4(c)/C.3：要低/負 → 分散互補價值）


def _is_defensive_signal(signal_id) -> bool:
    """訊號是否被標/pre-register 為防禦型。目前恆 False（_DEFENSIVE_SIGNAL_IDS 空）→ 對所有現有
       訊號零行為改動。未來標 defensive 才走下行判準（B.4）。"""
    return (signal_id or "") in _DEFENSIVE_SIGNAL_IDS


def _bootstrap_ci_low(values, n_boot: int = 1000, seed: int = 0):
    """對 values 算 bootstrap 95% 平均數 CI。**用本地 random.Random(seed)**，零全域 RNG 干擾
       → 不影響 _summarize_trades 既有 wr/ar/excess bootstrap（命門：既有輸出位元級不變）。
       N<5 → (None,None)。回 (ci_low, ci_high)。"""
    import random as _r
    vals = [v for v in (values or []) if v is not None]
    n = len(vals)
    if n < 5:
        return (None, None)
    rng = _r.Random(seed)
    boot = []
    for _ in range(n_boot):
        boot.append(sum(vals[rng.randint(0, n - 1)] for __ in range(n)) / n)
    boot.sort()
    lo, hi = int(n_boot * 0.025), int(n_boot * 0.975)
    return (round(boot[lo], 2), round(boot[hi], 2))


def _defensive_gate_eval(downside_excess, *, sharpe=None, info_ratio=None,
                         corr_with_trend_alpha=None, boot_seed: int = 0) -> dict:
    """下行超額判準計算（B.4）。純函式、無副作用。
       downside_excess = 該訊號在下行桶(RISK_OFF/CRISIS/2022)的逐筆超額 list（vs 公平基準）。
       回 dict：下行桶 N / avg / bootstrap CI95 / CI_low ＋ 透傳全期 sharpe/info_ratio ＋
       與順勢 alpha 逐筆報酬相關係數。判定交給 _defensive_gate_decision。"""
    de = [x for x in (downside_excess or []) if x is not None]
    n = len(de)
    avg = round(sum(de) / n, 2) if n else None
    ci_low, ci_high = _bootstrap_ci_low(de, seed=boot_seed) if n >= 5 else (None, None)
    return {
        "downside_excess_n": n,
        "downside_excess_avg": avg,
        "downside_excess_ci95": [ci_low, ci_high],
        "downside_excess_ci_low": ci_low,
        "sharpe": sharpe,
        "info_ratio": info_ratio,
        "corr_with_trend_alpha": (round(float(corr_with_trend_alpha), 2)
                                  if corr_with_trend_alpha is not None else None),
    }


def _defensive_gate_decision(ev: dict) -> dict:
    """防禦型升格判定（A③ defensive-conditional 的閘，B.4）。命門三條：
       (a) 下行桶逐筆超額 bootstrap CI_low > 0（崩盤時仍贏大盤）；
       (b) 全期 Sharpe 或 IR 為正（風險調整後不虧）；
       (c) 與指定順勢 alpha（A4）逐筆報酬相關 ≤ _DEFENSIVE_CORR_MAX（低/負 → 分散互補）；
           未提供相關係數 → 不否決、標 caveat（分散價值待驗）。
       保守：下行桶 N<5 或 CI_low 不可得 → 一律不過（不臆斷防禦性）。
       回 {pass, beta_filter, status, reason, corr_caveat}。"""
    ci_low = ev.get("downside_excess_ci_low")
    n = ev.get("downside_excess_n", 0) or 0
    sharpe = ev.get("sharpe")
    ir = ev.get("info_ratio")
    corr = ev.get("corr_with_trend_alpha")
    fails = []
    # (a) 下行超額 CI_low > 0
    if n < 5 or ci_low is None:
        fails.append(f"下行桶樣本不足(N={n})或CI不可得")
    elif ci_low <= 0:
        fails.append(f"下行超額CI下界{ci_low}≤0(崩盤未保護)")
    # (b) 風險調整後為正
    risk_adj_pos = (sharpe is not None and sharpe > 0) or (ir is not None and ir > 0)
    if not risk_adj_pos:
        fails.append("Sharpe/IR 皆非正(風險調整後未顯防禦)")
    # (c) 與順勢 alpha 相關低/負（未提供→caveat 不否決）
    corr_caveat = None
    if corr is not None and corr > _DEFENSIVE_CORR_MAX:
        fails.append(f"與順勢alpha相關{corr}>{_DEFENSIVE_CORR_MAX}(共線、無分散價值)")
    elif corr is None:
        corr_caveat = "未提供與順勢alpha相關係數(分散互補價值待驗)"
    passed = not fails
    if passed:
        reason = (f"防禦型過閘：下行超額CI下界{ci_low}>0(崩盤保護)"
                  + (f"、與順勢alpha相關{corr}(低/負→分散)" if corr is not None else "")
                  + (f" ｜ {corr_caveat}" if corr_caveat else ""))
        return {"pass": True, "beta_filter": "DEFENSIVE_PASS",
                "status": "DEFENSIVE", "reason": reason, "corr_caveat": corr_caveat}
    return {"pass": False, "beta_filter": "DEFENSIVE_FAIL",
            "status": "DEFENSIVE_FAIL", "reason": "防禦型未過閘：" + "; ".join(fails),
            "corr_caveat": corr_caveat}


def _extract_row_routed(s: dict, mkt: str, buy_strat: str,
                        downside_excess=None, corr_with_trend_alpha=None) -> dict:
    """標籤路由（R-DOWNSIDE 核心接線）：
       • 非防禦型訊號 → return _extract_row(...) **原樣**（位元級不變·零回歸命門）。
       • 防禦型訊號（_is_defensive_signal=True，目前無）→ 走下行超額判準，覆寫 status/pass/
         reason/beta_filter，並附 defensive_eval / gate_path 欄（latent 分支，只在未來標 defensive 才走）。
       downside_excess 預設取 summary 的 downside_excess_returns（下行桶逐筆超額）；
       corr_with_trend_alpha 由呼叫方（標 defensive 時）提供與 A4 的逐筆報酬相關係數。"""
    if not _is_defensive_signal(buy_strat):
        return _extract_row(s, mkt, buy_strat)          # ← 既有標準超額閘路徑，原封不動
    # ── 防禦型路徑（latent；目前無 defensive 訊號）──────────────────────────────
    row = _extract_row(s, mkt, buy_strat)               # 先取標準 row（保留全部數值欄供呈現）
    de = downside_excess if downside_excess is not None else s.get("downside_excess_returns")
    ev = _defensive_gate_eval(de, sharpe=row.get("sharpe"),
                              info_ratio=row.get("info_ratio"),
                              corr_with_trend_alpha=corr_with_trend_alpha)
    dec = _defensive_gate_decision(ev)
    # 防禦型：判準改由下行超額閘決定（B.4「換對的尺」），覆寫判決欄
    row["gate_path"] = "defensive"
    row["defensive_eval"] = ev
    row["beta_filter"] = dec["beta_filter"]
    row["status"] = dec["status"]
    row["pass"] = dec["pass"]
    row["reason"] = dec["reason"]
    return row


def _base_rate_worker(job_id: str, markets: list, buy_ids: list, sell_ids: list, start: str, end: str,
                      cs_top_k: float = _CS_DEFAULT_K, cs_overlay_factor: str = None,
                      benchmark: str = "twii"):
    """compute-once: 每個市場只載入一次資料，所有策略共用同一份 bar_data"""
    job = _BASE_RATE_JOBS[job_id]
    rows = []
    errors = []
    total_tasks = 0

    # Phase 1: 收集各市場 universe
    market_info = []  # [(mkt, codes)]
    for mkt in markets:
        if job.get("cancelled"):
            break
        try:
            uni = get_universe(mkt)
            if isinstance(uni, JSONResponse):
                errors.append({"market": mkt, "error": "universe not available"})
                continue
            raw = uni.get("data", uni.get("stocks", []))
            codes = [s if isinstance(s, str) else s.get("code", "") for s in raw]
            codes = [c for c in codes if c]
            if not codes:
                errors.append({"market": mkt, "error": "empty universe"})
                continue
            market_info.append((mkt, codes))
            total_tasks += len(buy_ids)
        except Exception as e:
            errors.append({"market": mkt, "error": str(e)})

    job["total"] = total_tasks
    job["status"] = "running"
    progress = 0

    # Phase 2: 逐市場 compute-once
    for mkt, codes in market_info:
        if job.get("cancelled"):
            break

        # ── COMPUTE ONCE: 下載+載入此市場全部資料 ──
        job["current"] = f"{mkt}/loading {len(codes)} stocks..."
        t0 = time.time()
        try:
            preloaded = _load_backtest_data(codes, mkt, start, end)
        except Exception as e:
            errors.append({"market": mkt, "error": f"data load failed: {e}"})
            progress += len(buy_ids)
            job["progress"] = progress
            continue
        load_sec = time.time() - t0
        print(f"[base-rate] {mkt}: data loaded ({len(codes)} stocks, {load_sec:.1f}s)", flush=True)

        # ── trade-level 快速引擎：每股算一次 outcome 表+指標，所有策略共用 ──
        t1 = time.time()
        def _prog(done, tot, _mkt=mkt, _base=progress):
            job["current"] = f"{_mkt}/features {done}/{tot} stocks"
        try:
            summaries = _fast_base_rate_market(
                mkt, codes, start, end, buy_ids, preloaded,
                progress_cb=_prog, should_cancel=lambda: job.get("cancelled"),
                cs_top_k=cs_top_k, cs_overlay_factor=cs_overlay_factor,
                benchmark=benchmark,
            )
        except Exception as e:
            import traceback; traceback.print_exc()
            errors.append({"market": mkt, "error": f"compute failed: {e}"})
            progress += len(buy_ids)
            job["progress"] = progress
            continue
        compute_sec = time.time() - t1
        print(f"[base-rate] {mkt}: computed {len(buy_ids)} strategies in {compute_sec:.1f}s", flush=True)

        for buy_strat in buy_ids:
            s = summaries.get(buy_strat, {})
            # R-DOWNSIDE 路由：非防禦型 → _extract_row 原樣（位元級不變）；防禦型(目前無) → 走下行判準。
            row = _extract_row_routed(s, mkt, buy_strat)
            rows.append(row)
            progress += 1
            job["progress"] = progress
            print(f"[base-rate] {mkt}/{buy_strat}: N={row['N']} WR={row['win_rate']}% retCI={row['avg_return_ci95']} beta={row['beta_filter']} → {row['status']}", flush=True)

    job["progress"] = progress
    job["current"] = ""
    job["status"] = "cancelled" if job.get("cancelled") else "done"
    job["rows"] = rows
    job["errors"] = errors if errors else None
    job["completed_at"] = datetime.now().isoformat()
    print(f"[base-rate] Job {job_id} {'cancelled' if job.get('cancelled') else 'done'}: {len(rows)} rows", flush=True)


@app.post("/api/backtest/base-rate")
def run_base_rate(config: dict):
    """S-15: 啟動 base rate 背景計算，回傳 job_id 供輪詢"""
    markets = config.get("markets", ["TW", "US"])
    if isinstance(markets, str):
        markets = [markets]
    # 預設只測「回測引擎有實作觸發碼」的 12 個進場策略（排除 4 個死策略，見 _BASE_RATE_BUY_IMPL）
    buy_ids = config.get("buy_strategies", list(_BASE_RATE_BUY_IMPL))
    sell_ids = config.get("sell_strategies", ["EXIT_C", "EXIT_D"])
    start = config.get("start", "2020-01-01")
    end = config.get("end", datetime.now().strftime("%Y-%m-%d"))
    # R-CS（alpha agenda）：top K% 進場濾網 + 對既有訊號疊橫斷面因子的 overlay 模式
    try:
        cs_top_k = float(config.get("cs_top_k", _CS_DEFAULT_K))
    except (TypeError, ValueError):
        cs_top_k = _CS_DEFAULT_K
    cs_top_k = min(max(cs_top_k, 0.01), 1.0)
    cs_overlay_factor = config.get("cs_overlay_factor")  # None 或 raw_mom/rel_str/resid_mom/sector_rel/sector_rel_topsec
    if cs_overlay_factor not in (None, "raw_mom", "rel_str", "resid_mom", "sector_rel", "sector_rel_topsec"):
        cs_overlay_factor = None
    # 公平基準（§三E）：ALPHA 閘逐筆超額對照基準。白名單外一律退回 twii（市值加權，現況）。
    benchmark = str(config.get("benchmark", "twii")).lower()
    if benchmark not in _BENCHMARK_WHITELIST:
        benchmark = "twii"

    job_id = secrets.token_hex(8)
    _BASE_RATE_JOBS[job_id] = {
        "status": "starting", "progress": 0, "total": 0, "current": "",
        "rows": [], "errors": None, "cancelled": False,
        "config": {"markets": markets, "buy_strategies": buy_ids, "sell_strategies": sell_ids,
                   "start": start, "end": end, "cs_top_k": cs_top_k, "cs_overlay_factor": cs_overlay_factor,
                   "benchmark": benchmark},
        "started_at": datetime.now().isoformat(), "completed_at": None,
    }

    threading.Thread(target=_base_rate_worker,
                     args=(job_id, markets, buy_ids, sell_ids, start, end, cs_top_k, cs_overlay_factor, benchmark),
                     daemon=True).start()
    return {"ok": True, "job_id": job_id, "message": f"Base rate job started: {len(buy_ids)} strategies × {len(markets)} markets. Poll GET /api/backtest/base-rate/{job_id} for progress."}


@app.post("/api/backtest/base-rate/{job_id}/cancel")
def cancel_base_rate(job_id: str):
    """取消正在跑的 base-rate job"""
    job = _BASE_RATE_JOBS.get(job_id)
    if not job:
        return JSONResponse({"error": "job not found"}, status_code=404)
    if job["status"] in ("done", "cancelled"):
        return {"ok": False, "message": f"job already {job['status']}"}
    job["cancelled"] = True
    return {"ok": True, "message": "cancel requested, job will stop after current strategy completes"}


@app.get("/api/backtest/base-rate/{job_id}")
def get_base_rate_status(job_id: str):
    """查詢 base rate job 進度/結果"""
    job = _BASE_RATE_JOBS.get(job_id)
    if not job:
        return JSONResponse({"error": "job not found"}, status_code=404)
    resp = {
        "job_id": job_id,
        "status": job["status"],
        "progress": job["progress"],
        "total": job["total"],
        "current": job.get("current", ""),
    }
    if job["status"] in ("done", "cancelled"):
        resp["rows"] = job["rows"]
        resp["errors"] = job["errors"]
        resp["thresholds"] = _BASE_RATE_THRESHOLDS
        resp["sell_strategies"] = job["config"]["sell_strategies"]
        resp["run_at"] = job["completed_at"]
        resp["history_from"] = job["config"]["start"]
        resp["cs_top_k"] = job["config"].get("cs_top_k")
        resp["cs_overlay_factor"] = job["config"].get("cs_overlay_factor")
        resp["benchmark"] = job["config"].get("benchmark", "twii")
        resp["benchmark_label"] = _benchmark_label(resp["benchmark"])
    return resp


# ══════════════════════════════════════════════════════════════════════════
# 軌2 — 複合 alpha 搜尋/驗證引擎 MVP（R-PROD-6/7a/7c/7e）
#   規格：ai-investment-firm/2026-06-25-composite-search-engine.md（§三防過擬合/
#         §四 regime 切分/§九 MVP）。全複用既有 base-rate 引擎原語，不重造計算。
#   單一真實來源：_cs_rank_pct（橫斷面排名）/ _summarize_trades（逐筆→summary）
#                同時供 _fast_base_rate_market 與本搜尋引擎使用（零回歸：見 test_extract_equiv）。
# ══════════════════════════════════════════════════════════════════════════

# ── 共用原語①：橫斷面因子排名（從 _fast_base_rate_market 抽出，逐字等價）─────────
def _cs_rank_pct(mkt: str, codes: list, feats: dict, factors_needed: set, bench_map: dict) -> dict:
    """給定每股 feats，算每因子每日對全 universe 的百分位 rank ∈[0,1]（1=因子最高，動量最強）。
    複用 _compute_cs_factors（正交因子 raw_mom/rel_str/resid_mom）/ _compute_sector_factors
    （板塊因子 sector_rel/sector_rel_topsec=A4）。回 {factor: {code: {date: pct}}}。
    與 _fast_base_rate_market 原內聯兩趟邏輯逐字等價（隔離測 test_extract_equiv 已證）。"""
    factor_vals = {fac: {} for fac in factors_needed}  # fac -> {date -> {code -> val}}
    sector_needed = factors_needed & _CS_SECTOR_FACTORS
    chip_needed = factors_needed & _CHIP_CS_FACTORS          # P0-1：籌碼因子（讀 chip_snapshot，PIT 對齊次一交易日）
    us_chip_needed = factors_needed & _US_CHIP_CS_FACTORS    # Type 2 US-SI：美股籌碼（讀 us_short_interest，PIT 對齊 publish_date 後交易日）
    tdcc_needed = factors_needed & _TDCC_CS_FACTORS          # D1：集保大戶結構（讀 tdcc_holder，TW-gated，PIT 對齊 publish_date 後交易日）
    rev_needed = factors_needed & _REVENUE_CS_FACTORS        # P0-2：月營收動量（讀 revenue_monthly，PIT 對齊 announce_date 後交易日）
    price_needed = factors_needed & _PRICE_CS_FACTORS        # Type 1a：價格族（OHLCV±免費宏觀序列，PIT 對齊次一交易日）
    orth_needed = factors_needed - sector_needed - chip_needed - us_chip_needed - tdcc_needed - rev_needed - price_needed
    sector_of = _get_sector_map(mkt, codes) if sector_needed else {}
    smom_by_code = {}
    for code in codes:
        f = feats.get(code)
        if not f:
            continue
        dts = f["dates"]
        if orth_needed:
            cf = _compute_cs_factors(f, bench_map, orth_needed)
            for fac, arr in cf.items():
                fv = factor_vals[fac]
                for i in range(len(dts)):
                    val = arr[i]
                    if val == val:  # not NaN
                        d = dts[i]
                        bucket = fv.get(d)
                        if bucket is None:
                            bucket = fv[d] = {}
                        bucket[code] = float(val)
        if sector_needed and code in sector_of:
            smom_by_code[code] = (dts, _nday_return_pct_series(f["c"], _CS_REL_N))
    if sector_needed:
        sec_fv = _compute_sector_factors(smom_by_code, sector_of, sector_needed)
        for fac, dmap in sec_fv.items():
            factor_vals[fac] = dmap
    if chip_needed:                                          # P0-1：籌碼 cs 因子 → 同排名管線
        chip_fv = _compute_chip_cs_factors(mkt, codes, feats, chip_needed)
        for fac, dmap in chip_fv.items():
            factor_vals[fac] = dmap
    if us_chip_needed:                                       # Type 2 US-SI：美股籌碼 cs 因子（US-gated）→ 同排名管線
        us_chip_fv = _compute_us_chip_cs_factors(mkt, codes, feats, us_chip_needed)
        for fac, dmap in us_chip_fv.items():
            factor_vals[fac] = dmap
    if tdcc_needed:                                          # D1：集保大戶結構 cs 因子（TW-gated）→ 同排名管線
        tdcc_fv = _compute_tdcc_cs_factors(mkt, codes, feats, tdcc_needed)
        for fac, dmap in tdcc_fv.items():
            factor_vals[fac] = dmap
    if rev_needed:                                           # P0-2：月營收 cs 因子 → 同排名管線
        rev_fv = _compute_revenue_cs_factors(mkt, codes, feats, rev_needed)
        for fac, dmap in rev_fv.items():
            factor_vals[fac] = dmap
    if price_needed:                                         # Type 1a：價格族 cs 因子 → 同排名管線
        price_fv = _compute_price_cs_factors(mkt, codes, feats, price_needed, bench_map)
        for fac, dmap in price_fv.items():
            factor_vals[fac] = dmap
    rank_pct = {fac: {} for fac in factors_needed}
    for fac in factors_needed:
        fv = factor_vals[fac]; rp = rank_pct[fac]
        for d, cvals in fv.items():
            m = len(cvals)
            if m < _CS_MIN_XSECTION:
                continue
            items = sorted(cvals.items(), key=lambda kv: kv[1])
            denom = (m - 1) if m > 1 else 1
            for rank_i, (cc, _v) in enumerate(items):
                rp.setdefault(cc, {})[d] = rank_i / denom
    return rank_pct


# ── 共用原語②：逐筆 trade → base-rate summary（從 _fast_base_rate_market 抽出，逐字等價）──
def _summarize_trades(trades: list, mkt: str, excess_bench_map: dict,
                      cs_factor=None, cs_top_k=None) -> dict:
    """逐筆 trade list → base-rate summary（勝率/CI/P-L/Sharpe/MDD/子期間/超額CI/IR/下行超額）。
    單一真實來源：_fast_base_rate_market 與複合搜尋引擎共用（避免重寫計算）。
    trades: list of (entry_date, exit_date, net_ret%)。回傳欄位對齊 _extract_row 期望。"""
    import numpy as np
    rets = [t[2] for t in trades]
    n_tr = len(rets)
    summary = {"total_trades": n_tr, "ci_n_trades": n_tr, "market": mkt}
    if n_tr == 0:
        summary.update({"win_rate_pct": 0, "avg_trade_return_pct": 0,
                        "win_rate_ci95": [None, None], "avg_return_ci95": [None, None],
                        "profit_loss_ratio": 0, "sharpe_ratio": 0, "max_drawdown_pct": 0,
                        "subperiod_pos": "0/0", "subperiod_frac": None, "subperiod_buckets": 0,
                        "excess_avg": None, "excess_ci95": [None, None], "excess_n": 0,
                        "info_ratio": None, "downside_excess_avg": None, "downside_excess_n": 0,
                        "downside_excess_returns": [],
                        "cs_factor": cs_factor, "cs_top_k": (cs_top_k if cs_factor else None)})
        return summary

    wins = [r for r in rets if r > 0]
    losses = [-r for r in rets if r <= 0]
    win_rate = len(wins) / n_tr * 100
    avg_ret = sum(rets) / n_tr
    avg_win = sum(wins) / len(wins) if wins else 0
    avg_loss = sum(losses) / len(losses) if losses else 0
    pl_ratio = (avg_win / avg_loss) if avg_loss > 0 else 0
    arr = np.array(rets)
    sharpe = float(arr.mean() / arr.std()) if arr.std() > 0 else 0  # 每筆 sharpe（資訊比）

    # bootstrap 95% CI（1000 次，對齊投組引擎）
    wr_ci = ar_ci = [None, None]
    if n_tr >= 5:
        import random
        wins_ind = [1 if r > 0 else 0 for r in rets]
        n_boot = 1000
        bw, br = [], []
        for _ in range(n_boot):
            idx = [random.randint(0, n_tr - 1) for __ in range(n_tr)]
            bw.append(sum(wins_ind[k] for k in idx) / n_tr * 100)
            br.append(sum(rets[k] for k in idx) / n_tr)
        bw.sort(); br.sort()
        lo, hi = int(n_boot * 0.025), int(n_boot * 0.975)
        wr_ci = [round(bw[lo], 1), round(bw[hi], 1)]
        ar_ci = [round(br[lo], 2), round(br[hi], 2)]

    # max drawdown：依出場日序列等權連乘
    trades_sorted = sorted(trades, key=lambda t: t[1])  # 依 exit_date
    eq = 1.0; peak = 1.0; max_dd = 0.0
    for t in trades_sorted:
        eq *= (1 + t[2] / 100)
        peak = max(peak, eq)
        dd = (peak - eq) / peak * 100
        max_dd = max(max_dd, dd)

    # ── beta 濾網原料 (R3) ──
    min_bt = _BASE_RATE_THRESHOLDS["subperiod_min_trades"]
    year_rets: dict = {}
    for t in trades:
        year_rets.setdefault(t[0][:4], []).append(t[2])
    qualified = [rs for rs in year_rets.values() if len(rs) >= min_bt]
    n_qual = len(qualified)
    n_pos = sum(1 for rs in qualified if (sum(rs) / len(rs)) > 0)
    subperiod_pos = f"{n_pos}/{n_qual}"
    subperiod_frac = round(n_pos / n_qual, 2) if n_qual else None

    # (B) 逐筆對齊持有期超額 vs 基準
    excess = []
    down_excess = []
    for t in trades:
        be, bx = excess_bench_map.get(t[0]), excess_bench_map.get(t[1])
        if be and bx and be > 0:
            e = t[2] - (bx / be - 1) * 100
            excess.append(e)
            if t[0][:4] == "2022":
                down_excess.append(e)
    excess_n = len(excess)
    excess_avg = round(sum(excess) / excess_n, 2) if excess_n else None
    excess_ci = [None, None]
    if excess_n >= 5:
        import random
        be_boot = []
        for _ in range(1000):
            idx = [random.randint(0, excess_n - 1) for __ in range(excess_n)]
            be_boot.append(sum(excess[k] for k in idx) / excess_n)
        be_boot.sort()
        lo, hi = int(1000 * 0.025), int(1000 * 0.975)
        excess_ci = [round(be_boot[lo], 2), round(be_boot[hi], 2)]

    info_ratio = None
    if excess_n >= 5:
        ex_sd = float(np.std(excess))
        info_ratio = round(float(np.mean(excess)) / ex_sd, 2) if ex_sd > 0 else None
    down_n = len(down_excess)
    down_avg = round(sum(down_excess) / down_n, 2) if down_n else None

    summary.update({
        "win_rate_pct": round(win_rate, 1),
        "avg_trade_return_pct": round(avg_ret, 2),
        "win_rate_ci95": wr_ci,
        "avg_return_ci95": ar_ci,
        "profit_loss_ratio": round(pl_ratio, 2),
        "sharpe_ratio": round(sharpe, 2),
        "max_drawdown_pct": round(max_dd, 2),
        "subperiod_pos": subperiod_pos,
        "subperiod_frac": subperiod_frac,
        "subperiod_buckets": n_qual,
        "excess_avg": excess_avg,
        "excess_ci95": excess_ci,
        "excess_n": excess_n,
        "info_ratio": info_ratio,
        "downside_excess_avg": down_avg,
        "downside_excess_n": down_n,
        # R-DOWNSIDE（2026-06-27）：additive — 暴露下行桶逐筆超額 list（既有迴圈已建好、原本丟棄），
        #   供防禦型判準路徑（_defensive_gate_eval）lazily 算 bootstrap CI_low。零 RNG、零額外計算、
        #   既有 _extract_row/composite 持久化讀的是 explicit key → 此 additive key 對既有輸出位元級不變。
        "downside_excess_returns": list(down_excess),
        "cs_factor": cs_factor,
        "cs_top_k": (cs_top_k if cs_factor else None),
    })
    return summary


# ── §三 防過擬合：Deflated Sharpe Ratio（對試驗數 T 折減）─────────────────────
def _deflated_sharpe_ratio(returns, trial_sharpes, T):
    """Bailey & López de Prado (2014) Deflated Sharpe Ratio。
    returns       = 候選逐筆報酬序列（算 SR̂/n/skew/kurt）
    trial_sharpes = 本批 T 次試驗各自 Sharpe（算噪音期望最大 SR 的 Var({SRₙ})）
    T             = 試驗計數（多重檢定校正輸入；§3.0 落地）
    回 {sharpe, sr0, dsr, skew, kurt, n, T, var_sr}。DSR>0.95 = keeper 級（G2）。
    純數值（numpy+statistics），可用已知序列驗算（見 test_pure.py）。"""
    import numpy as np
    from statistics import NormalDist
    r = np.asarray([x for x in returns if x == x], dtype=float)
    n = int(r.size)
    if n < 2:
        return {"sharpe": None, "sr0": None, "dsr": None, "skew": None,
                "kurt": None, "n": n, "T": int(T), "var_sr": None}
    sd = float(r.std(ddof=1))
    sr_hat = float(r.mean() / sd) if sd > 0 else 0.0
    m = r - r.mean()
    s2 = float((m ** 2).mean())
    if s2 > 0:
        skew = float((m ** 3).mean() / s2 ** 1.5)
        kurt = float((m ** 4).mean() / s2 ** 2)   # raw kurtosis（常態=3）
    else:
        skew, kurt = 0.0, 3.0
    ts = np.asarray([x for x in (trial_sharpes or []) if x == x], dtype=float)
    var_sr = float(ts.var(ddof=1)) if ts.size >= 2 else 0.0
    Nd = NormalDist()
    gamma = 0.5772156649015329  # Euler-Mascheroni
    Te = max(int(T), 2)
    e = float(np.e)
    sr0 = (var_sr ** 0.5) * ((1.0 - gamma) * Nd.inv_cdf(1.0 - 1.0 / Te)
                             + gamma * Nd.inv_cdf(1.0 - 1.0 / (Te * e)))
    denom = 1.0 - skew * sr_hat + (kurt - 1.0) / 4.0 * sr_hat ** 2
    dsr = None
    if denom > 0:
        z = (sr_hat - sr0) * ((n - 1) ** 0.5) / (denom ** 0.5)
        dsr = float(Nd.cdf(z))
    return {"sharpe": round(sr_hat, 4), "sr0": round(sr0, 4),
            "dsr": (round(dsr, 4) if dsr is not None else None),
            "skew": round(skew, 4), "kurt": round(kurt, 4),
            "n": n, "T": int(T), "var_sr": round(var_sr, 6)}


# ── B.2 S5 誠實 T：多重檢定校正的試驗數 ──────────────────────────────────────
# 研究輪次的保守地板：人工/稀薄 cohort 也至少當作掃過 ~30 變體（18 因子詞彙 × top_k 網格），
# 避免「單筆評估 → T 小 → DSR 被高估、偏好看」。data-grounded，非憑空。
_HONEST_T_ORPHAN_FLOOR = 30
# 有效試驗門檻：n<此值的切片（空切片 sharpe=0.0、或 2~7 筆的退化回測）其 Sharpe 非可比的
# 「試驗結果」，既不該計入 T（無法被選為 best），更不該定義 null 的離散度（var_sr）。
# 實證：單一 n=2 garbage 試驗(Sharpe -2.93)會讓 var_sr 暴漲 50×(0.0012→0.062)、sr0 0.094→0.68、
# 把整批 DSR 壓成 0——那是退化雜訊不是嚴格，B&LdP 假設試驗 Sharpe 來自可比回測。門檻在 [20,100]
# 對 var_sr 穩定(皆 ≈0.0012)，取 30（Sharpe 至少 30 筆才有意義）。
_MIN_TRIAL_N = 30

def _honest_trial_T(cur, market, start, end, benchmark):
    """誠實 T（B.2 S5）：T = 該候選『所屬那次搜尋』累計試過的**有效**試驗數。

    定義＝共享同一 (market, start, end, benchmark) 配置、且 n≥_MIN_TRIAL_N 的候選數——即「同一
    回測目標下試過、且產出可比 Sharpe 的所有切片/變體」（含失敗但有效的切片、含同訊號歷次實驗，
    正是 S5 要的廣度）。**不**把無關全宇宙（其他市場/期間/基準）算進去 → 避免過度懲罰（task 點1）；
    亦不再只算單筆/小批 → 避免過鬆高估（task 病灶）；**亦排除退化切片**（空切片/2~7 筆 garbage，
    其 Sharpe 非可比試驗結果）。比舊制（whole-table running COUNT(*)）更誠實：
      ·舊制把不同市場/期間/基準的搜尋混成一個 T（過度懲罰）；
      ·舊制 trial_sharpes 取 insert 當下 whole-table → 同策略因「插入時表內有哪些退化試驗」而 DSR
        天差地別（實證：同 A4∧rate 一筆 0.9755、一筆 0.5068，純因 -2.93 退化試驗插入早晚）。非決定性。
    回 (T, trial_sharpes, source)：
      ·source='search-cohort'：有效 cohort≥地板 → T=有效試驗數（含本筆，若本筆 n≥門檻）。
      ·source='orphan-floor' ：有效 cohort<地板（人工單評估/稀薄）→ T=地板，誠實標記、不假裝硬化。
    trial_sharpes 與 T 同集（皆有效試驗），var_sr 描述同一次搜尋的真實試驗離散度。
    _deflated_sharpe_ratio 數學本身不動，只改餵進去的 T 與 trial 集。純讀本地、零 token。"""
    where = ("market=? AND start=? AND end=? "
             "AND IFNULL(benchmark,'')=IFNULL(?,'') "
             "AND sharpe IS NOT NULL AND IFNULL(n,0) >= ?")
    args = (market, start, end, benchmark, _MIN_TRIAL_N)
    sharpes = [r[0] for r in cur.execute(
        f"SELECT sharpe FROM composite_candidates WHERE {where}", args).fetchall()]
    valid = len(sharpes)
    if valid >= _HONEST_T_ORPHAN_FLOOR:
        return valid, sharpes, "search-cohort"
    return _HONEST_T_ORPHAN_FLOOR, sharpes, "orphan-floor"


# ── §三 防過擬合：時間分塊 P&L（PBO 矩陣的列）────────────────────────────────
def _bin_returns_to_blocks(trades, start, end, n_blocks=16):
    """逐筆 trade 依「出場日」均分到 n_blocks 個等寬時間塊，回每塊平均報酬%（空塊=None）。
    供 PBO（CSCV）建 P&L 矩陣（同 (start,end,n_blocks) 的候選天然對齊）。純數值。"""
    import numpy as np
    if n_blocks < 1 or not trades:
        return [None] * n_blocks
    try:
        t0 = datetime.strptime(start, "%Y-%m-%d").toordinal()
        t1 = datetime.strptime(end, "%Y-%m-%d").toordinal()
    except Exception:
        return [None] * n_blocks
    span = max(t1 - t0, 1)
    buckets = [[] for _ in range(n_blocks)]
    for (entry, exit_d, ret) in trades:
        try:
            o = datetime.strptime(exit_d, "%Y-%m-%d").toordinal()
        except Exception:
            continue
        b = int((o - t0) / span * n_blocks)
        b = min(max(b, 0), n_blocks - 1)
        buckets[b].append(ret)
    return [round(float(np.mean(b)), 4) if b else None for b in buckets]


def _cscv_sharpe_cols(M, idx, N):
    """子集 idx（時間塊）上每候選（行）的 Sharpe（mean/std）；純 numpy 向量化。"""
    import numpy as np
    sub = M[idx, :]
    if sub.shape[0] < 2:
        return np.full(N, np.nan)
    mu = np.nanmean(sub, axis=0)
    sd = np.nanstd(sub, axis=0, ddof=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(sd > 0, mu / sd, np.nan)


def _cscv_process_combos(combo_chunk, M, blocks, S, N):
    """處理一批 train 組合，回 (lambdas, degr)。純 numpy（GIL 釋放）→ 可多緒平行。"""
    import numpy as np
    all_blocks = set(range(S))
    lambdas, degr = [], []
    for train_sets in combo_chunk:
        train_idx = np.concatenate([blocks[b] for b in train_sets])
        test_idx = np.concatenate([blocks[b] for b in (all_blocks - set(train_sets))])
        is_perf = _cscv_sharpe_cols(M, train_idx, N)
        oos_perf = _cscv_sharpe_cols(M, test_idx, N)
        if np.all(np.isnan(is_perf)):
            continue
        n_star = int(np.nanargmax(is_perf))
        oos_star = oos_perf[n_star]
        finite = oos_perf[np.isfinite(oos_perf)]
        if finite.size < 2 or not np.isfinite(oos_star):
            continue
        rank = int((finite < oos_star).sum())            # OOS 打敗的候選數
        omega = (rank + 1.0) / (finite.size + 1.0)        # 相對排名 ∈(0,1)
        omega = min(max(omega, 1e-6), 1 - 1e-6)
        lambdas.append(float(np.log(omega / (1.0 - omega))))
        degr.append((float(is_perf[n_star]), float(oos_star)))
    return lambdas, degr


# ── §3.2 防過擬合：PBO（CSCV，Bailey-Borwein-LdP-Zhu 2017）──────────────────
def _pbo_cscv(pnl_matrix, S=16, max_workers=8):
    """Combinatorially Symmetric Cross-Validation 的回測過擬合機率。
    pnl_matrix: shape (T_obs, N)，列=時間塊、行=候選複合（本批所有試驗）。
    切 S 等長塊，列舉 C(S,S/2) 半訓半測；每組合取 IS-best 候選，算其 OOS 相對排名 ω、
    λ=ln(ω/(1−ω))；PBO = #{λ≤0}/#{組合}（OOS 低於中位的比例）。PBO<0.20 = keeper 級（G3）。
    combo 迴圈純 numpy → 用 ThreadPool 分塊平行（GIL 在 numpy 釋放）。"""
    import numpy as np
    from itertools import combinations
    from concurrent.futures import ThreadPoolExecutor
    M = np.asarray(pnl_matrix, dtype=float)
    if M.ndim != 2 or M.shape[1] < 2:
        return {"pbo": None, "reason": "need >=2 candidates", "n_combos": 0, "S": 0}
    T_obs, N = M.shape
    S = min(int(S), T_obs)
    if S % 2:
        S -= 1
    if S < 2:
        return {"pbo": None, "reason": f"too few rows ({T_obs}) for CSCV", "n_combos": 0, "S": S}
    bounds = np.linspace(0, T_obs, S + 1).astype(int)
    blocks = [np.arange(bounds[i], bounds[i + 1]) for i in range(S)]
    half = S // 2
    combos = list(combinations(range(S), half))
    # 分塊平行（chunk 數 = workers；每 chunk 純 numpy）
    workers = max(1, min(int(max_workers), len(combos)))
    chunk = (len(combos) + workers - 1) // workers
    chunks = [combos[i:i + chunk] for i in range(0, len(combos), chunk)]
    lambdas, degr = [], []
    if workers > 1 and len(chunks) > 1:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            for lam, dg in ex.map(lambda ck: _cscv_process_combos(ck, M, blocks, S, N), chunks):
                lambdas.extend(lam); degr.extend(dg)
    else:
        lam, dg = _cscv_process_combos(combos, M, blocks, S, N)
        lambdas.extend(lam); degr.extend(dg)
    if not lambdas:
        return {"pbo": None, "reason": "no valid CSCV combos", "n_combos": 0, "S": int(S)}
    lam = np.array(lambdas)
    pbo = float((lam <= 0).mean())
    deg = np.array(degr)
    perf_deg = None
    if deg.shape[0] >= 2 and np.std(deg[:, 0]) > 0:
        perf_deg = float(np.polyfit(deg[:, 0], deg[:, 1], 1)[0])   # OOS~IS 衰退斜率
    return {"pbo": round(pbo, 4), "n_combos": int(lam.size), "S": int(S),
            "lambda_median": round(float(np.median(lam)), 4),
            "perf_degradation_slope": (round(perf_deg, 4) if perf_deg is not None else None),
            "prob_oos_below_median": round(pbo, 4)}


# ── R-PROD-6：依進場日 regime 切片 ──────────────────────────────────────────
def _slice_trades_by_regime(trades, regime_map):
    """逐筆 trade 用「進場日」regime 分桶（複用 _batch_calc_regime → {date: regime}）。
    回 {regime: [trades]}。缺 regime 的進場日歸 UNKNOWN（誠實標記，不丟）。"""
    out = {}
    for t in trades:
        reg = regime_map.get(t[0]) or "UNKNOWN"
        out.setdefault(reg, []).append(t)
    return out


# ══════════════════════════════════════════════════════════════════════════════
# P0-3a — 市場情緒「regime / 條件化軸」（市場級，非 cs 選股因子）
# ──────────────────────────────────────────────────────────────────────────────
# VIX / F&G / AAII 一天一個值、全市場共用 → 不能橫斷面排名個股（不是 cs 選股因子），
# 而是「條件切片維度」。把 VIX 收盤 / F&G 指數轉成情緒桶（低波·貪婪 / 中性 / 高波·恐懼），
# 複用既有 _slice_trades_by_regime 對 trade「進場日」分桶 → 讓 base-rate / composite 能切
# 「A4 在 低VIX/貪婪 期 vs 高VIX/恐懼 期」找條件 alpha（與 A4 regime 條件 alpha 直接綜效）。
#
# ⚠ 設計鐵則：這是 regime 軸，「絕不」註冊進 _COMPOSITE_CS_FACTORS（市場級一天一值無法
#   排名個股；若誤當 cs 會壞掉橫斷面語意）。它走獨立的 sentiment_map → per_sentiment 路徑，
#   與既有 4-regime（per_regime）正交並存，互不影響。
#
# PIT：桶用「值[d]」標記進場日 d。VIX[d] 於 d 收盤已知（用於 d+1 進場不洩漏）、F&G[d] 為
#   當日盤後值，與既有 _batch_calc_regime 用 vix_map.get(d) 完全同一對齊慣例。非交易日對齊用
#   「最近一個 ≤ d 的值」前向填補（as-of，仍 PIT 安全：用昨日已知值，無 look-ahead）。
#   mode='fixed' 用絕對門檻（無 look-ahead，可當 live 濾網）；mode='tercile' 用窗內 33/67
#   分位（in-sample，僅供歷史樣本「條件化分析」分桶，精神同 per_regime，非 live 濾網）。
# ══════════════════════════════════════════════════════════════════════════════
_VIX_SENTIMENT_LABELS = ("VIX_LOW", "VIX_MID", "VIX_HIGH")     # 低波(自滿/貪婪)/中性/高波(恐懼)
_FNG_SENTIMENT_LABELS = ("FNG_FEAR", "FNG_NEUTRAL", "FNG_GREED")  # 低值=恐懼 / 中性 / 高值=貪婪
# 固定絕對門檻（PIT-clean，單一真實來源；VIX 25/35 與既有 4-regime 同尺，15 為常見低波界）
_VIX_FIXED_LO, _VIX_FIXED_HI = 15.0, 25.0
# F&G 0–100：恐懼 <25 / 中性 25–75 / 貪婪 >=75（CNN 常用 extreme 帶界，與老闆規格一致）
_FNG_FIXED_LO, _FNG_FIXED_HI = 25.0, 75.0


def _sentiment_buckets_from_series(rows, start, end, mode, fixed_lo, fixed_hi, labels):
    """共用核心：sorted [(date, value)] → 前向填補(as-of)的 {date: label} + thresholds/counts。
    labels=(low_label, mid_label, high_label)；value<lo→low / lo<=value<hi→mid / value>=hi→high。
    mode='fixed' 用 fixed_lo/hi（PIT-clean）；'tercile' 用窗 [start,end] 內 33/67 分位（in-sample）。
    前向填補：逐日曆日掃，缺值日沿用最近 ≤ d 的值（非交易日/休市對齊，PIT 安全）。"""
    from datetime import timedelta as _td
    low_l, mid_l, high_l = labels
    if not rows:
        return {"map": {}, "thresholds": None, "counts": {low_l: 0, mid_l: 0, high_l: 0}, "n_days": 0}
    in_window = [v for (d, v) in rows if start <= d <= end and v is not None]
    if mode == "tercile" and len(in_window) >= 3:
        srt = sorted(in_window)
        lo = srt[int(len(srt) * 0.33)]
        hi = srt[int(len(srt) * 0.67)]
        thr = {"lo": round(lo, 2), "hi": round(hi, 2), "basis": "in-sample 33/67 分位 [start,end]（非 live 濾網）"}
    else:
        lo, hi = fixed_lo, fixed_hi
        thr = {"lo": lo, "hi": hi, "basis": "fixed 絕對門檻（PIT-clean）"}
    smap = {}
    counts = {low_l: 0, mid_l: 0, high_l: 0}
    vi = 0
    last_v = None
    sdt = datetime.strptime(start, "%Y-%m-%d")
    edt = datetime.strptime(end, "%Y-%m-%d")
    d = sdt
    while d <= edt:
        ds = d.strftime("%Y-%m-%d")
        while vi < len(rows) and rows[vi][0] <= ds:    # 吃掉所有 ≤ 當日的值（含 pre-window，建 as-of 起點）
            if rows[vi][1] is not None:
                last_v = rows[vi][1]
            vi += 1
        if last_v is not None:
            lab = low_l if last_v < lo else (mid_l if last_v < hi else high_l)
            smap[ds] = lab
            counts[lab] += 1
        d += _td(days=1)
    return {"map": smap, "thresholds": thr, "counts": counts, "n_days": len(smap)}


def _vix_sentiment_map(start, end, mode="fixed"):
    """VIX 情緒條件軸 {date: VIX_LOW/VIX_MID/VIX_HIGH}（市場級，非 cs）。讀 daily_kbar ^VIX 收盤
    （market.db；與 chip backfill 的 monitor.db 不同庫，零鎖競爭）。多拉 20 日前置供 as-of 起點。"""
    from datetime import timedelta as _td
    pre = (datetime.strptime(start, "%Y-%m-%d") - _td(days=20)).strftime("%Y-%m-%d")
    con = market_db(); cur = con.cursor()
    cur.execute("""SELECT date, close FROM daily_kbar
                   WHERE code='^VIX' AND market='INDEX' AND date BETWEEN ? AND ?
                   ORDER BY date""", (pre, end))
    rows = cur.fetchall(); con.close()
    out = _sentiment_buckets_from_series(rows, start, end, mode, _VIX_FIXED_LO, _VIX_FIXED_HI, _VIX_SENTIMENT_LABELS)
    out.update({"axis": "vix", "mode": mode, "source": "daily_kbar ^VIX (market.db)"})
    return out


def _fng_sentiment_map(start, end, mode="fixed"):
    """CNN Fear&Greed 情緒條件軸 {date: FNG_FEAR/FNG_NEUTRAL/FNG_GREED}（市場級，非 cs）。
    讀 sentiment_daily(source='fng')（需先 _backfill_fng_history）。表缺/無資料 → 空 map（誠實，全 UNKNOWN）。"""
    from datetime import timedelta as _td
    pre = (datetime.strptime(start, "%Y-%m-%d") - _td(days=20)).strftime("%Y-%m-%d")
    rows = []
    try:
        con = market_db(); cur = con.cursor()
        cur.execute("""SELECT date, value FROM sentiment_daily
                       WHERE source='fng' AND date BETWEEN ? AND ? ORDER BY date""", (pre, end))
        rows = cur.fetchall(); con.close()
    except Exception:
        rows = []
    out = _sentiment_buckets_from_series(rows, start, end, mode, _FNG_FIXED_LO, _FNG_FIXED_HI, _FNG_SENTIMENT_LABELS)
    out.update({"axis": "fng", "mode": mode, "source": "sentiment_daily fng (market.db)"})
    return out


def _sentiment_regime_map(start, end, source="vix", mode="fixed"):
    """情緒條件軸 dispatcher（market-agnostic：^VIX/F&G 為全球市場級，TW/US 共用同軸）。
    source ∈ {'vix','fng'}（預設 vix）；回 {map, axis, mode, thresholds, counts, n_days, source}。"""
    return _fng_sentiment_map(start, end, mode) if str(source).lower() == "fng" \
        else _vix_sentiment_map(start, end, mode)


def _ensure_sentiment_table():
    """冪等建表：sentiment_daily（市場級日頻情緒，source 區分 fng/aaii…；market.db）。"""
    con = market_db()
    con.execute("""CREATE TABLE IF NOT EXISTS sentiment_daily(
        date   TEXT NOT NULL,
        source TEXT NOT NULL,
        value  REAL,
        rating TEXT,
        PRIMARY KEY(date, source))""")
    con.commit(); con.close()


def _backfill_vix_history(start="2010-01-01", end=None):
    """補 ^VIX 日線到 daily_kbar（market.db）。INSERT OR IGNORE → 不覆寫既有（純加法，零回歸）。
    複用 yfinance（同 _bg_fetch_index 路徑）。回 {ok, added, range}。"""
    end = end or datetime.now().strftime("%Y-%m-%d")
    try:
        import yfinance as yf
        df = yf.download("^VIX", start=start, end=end, progress=False)
    except Exception as e:
        return {"ok": False, "added": 0, "error": f"yfinance: {e}"}
    if df is None or df.empty:
        return {"ok": False, "added": 0, "error": "yfinance ^VIX empty"}
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    con = market_db(); added = 0
    for idx, row in df.iterrows():
        d = idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)[:10]
        try:
            c = float(row.get("Close", 0) or 0)
            if c == 0 or c != c:
                continue
            cur = con.execute(
                "INSERT OR IGNORE INTO daily_kbar(code,market,date,open,high,low,close,volume) VALUES(?,?,?,?,?,?,?,?)",
                ("^VIX", "INDEX", d, round(float(row.get("Open", 0) or 0), 2), round(float(row.get("High", 0) or 0), 2),
                 round(float(row.get("Low", 0) or 0), 2), round(c, 2), 0))
            added += cur.rowcount
        except Exception:
            pass
    con.commit()
    rng = con.execute("SELECT MIN(date),MAX(date),COUNT(*) FROM daily_kbar WHERE code='^VIX' AND market='INDEX'").fetchone()
    con.close()
    return {"ok": True, "added": added, "range": {"min": rng[0], "max": rng[1], "count": rng[2]}}


def _backfill_fng_history():
    """補 CNN Fear&Greed 日值到 sentiment_daily(source='fng')。免費 GitHub CSV（whit3rabbit）2011→今。
    INSERT OR IGNORE（純加法）。回 {ok, added, range}。"""
    _ensure_sentiment_table()
    import urllib.request, csv as _csv
    url = "https://raw.githubusercontent.com/whit3rabbit/fear-greed-data/main/fear-greed.csv"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        txt = urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "ignore").splitlines()
    except Exception as e:
        return {"ok": False, "added": 0, "error": f"fetch: {e}"}
    rdr = list(_csv.reader(txt))
    body = rdr[1:] if rdr else []
    con = market_db(); added = 0
    for r in body:
        if len(r) < 2:
            continue
        d = r[0][:10]
        try:
            v = float(r[1])
        except Exception:
            continue
        rating = r[2].strip() if len(r) > 2 else None
        cur = con.execute("INSERT OR IGNORE INTO sentiment_daily(date,source,value,rating) VALUES(?,?,?,?)",
                          (d, "fng", round(v, 2), rating))
        added += cur.rowcount
    con.commit()
    rng = con.execute("SELECT MIN(date),MAX(date),COUNT(*) FROM sentiment_daily WHERE source='fng'").fetchone()
    con.close()
    return {"ok": True, "added": added, "range": {"min": rng[0], "max": rng[1], "count": rng[2]}}


# ── R-PROD-7a：複合進場（AND-stack）emission（與 _emit_trades trig=None+rank 分支同邏輯）──
def _emit_composite_trades(f, eligible, dst):
    """複合進場：eligible = 該股「各層 AND 後」可進場日期集合。出場/淨成本/去重疊倉沿用
    outcome 表 + open_until（與 _emit_trades trig=None 分支逐字同邏輯 → 單層時 byte-level 等價，
    見 test_pure.py AND-stack 等價測）。"""
    oexit, oret, dts = f["outcome_exit"], f["outcome_ret"], f["dates"]
    open_until = -1
    for i in range(f["n"]):
        if i > open_until and oexit[i] >= 0 and oret[i] == oret[i]:
            if dts[i] in eligible:
                dst.append((dts[i], dts[oexit[i]], float(oret[i])))
                open_until = oexit[i]


# ── R-PROD-7a 籌碼層：法人連買（inst_net_buy）日期集合 ────────────────────────
def _chip_streak_dates(codes: list, window: int = 3, min_net: float = 0.0) -> dict:
    """法人連買：每檔在 chip_snapshot「連續 window 個交易日三大法人合計淨買超 > min_net(張)」的日期集合。
    net = foreign_buy + itrust_buy + dealer_buy。回 {code: set(date)}。
    僅 TW 有籌碼；無資料的市場/個股回空集（複合自然縮小、誠實標記）。一次性 DB 讀（I/O，主緒批次）。"""
    out = {c: set() for c in codes}
    if not codes:
        return out
    try:
        con = db(); cur = con.cursor()
        qs = ",".join("?" * len(codes))
        rows = cur.execute(
            f"SELECT code, date, (COALESCE(foreign_buy,0)+COALESCE(itrust_buy,0)+COALESCE(dealer_buy,0)) "
            f"FROM chip_snapshot WHERE code IN ({qs}) ORDER BY code, date", list(codes)
        ).fetchall()
        con.close()
    except Exception:
        return out
    by_code = {}
    for code, d, net in rows:
        by_code.setdefault(code, []).append((d, net))
    w = max(int(window), 1)
    for code, series in by_code.items():
        streak = 0
        elig = out.setdefault(code, set())
        for k, (d, net) in enumerate(series):
            if net is not None and net > min_net:
                streak += 1
            else:
                streak = 0
            # PIT 防洩漏：台股三大法人買賣超於 d 收盤(13:30)後才公布 → 連買 streak 在 d
            # 完成時，d 當下尚拿不到該資訊。進場改落在「資料可得的下一交易日」(series[k+1])，
            # 不再用 streak 完成當日 d，避免 look-ahead 製造假 alpha。
            if streak >= w and k + 1 < len(series):
                elig.add(series[k + 1][0])
    return out


# 可當「cs 層」的橫斷面因子白名單（A4 = sector_rel_topsec）。
# P0-1：加入籌碼因子 _CHIP_CS_FACTORS（inst_net_buy/trust_net_buy/short_squeeze）→ 搜尋引擎自動納入。
# P0-2：加入月營收因子 _REVENUE_CS_FACTORS（revenue_mom，基本面族）→ 搜尋引擎自動納入。
# Type 1a：加入價格族 _PRICE_CS_FACTORS（low_beta/rate_sensitivity/dollar_sensitivity/short_rev）→ 可手動 AND-stack。
_COMPOSITE_CS_FACTORS = {"raw_mom", "rel_str", "resid_mom",
                         "sector_rel", "sector_rel_topsec"} | _CHIP_CS_FACTORS | _REVENUE_CS_FACTORS \
                        | _PRICE_CS_FACTORS | _US_CHIP_CS_FACTORS | _TDCC_CS_FACTORS


def _normalize_layers(layers: list):
    """驗證/正規化複合層列表。回 (layers_norm, err)。
    支援三型可 AND：cs（{factor, top_k, dir}）/ pead（{type:'pead'}）/ chip（{type:'chip',window,min_net}）。
    R1：放開單一 cs 層限制 → 支援任意 k 層 cs∧cs AND-stack（R-PROD-7a）；per-layer dir∈{+1,−1}（R1(a)）。
    至少一層、總層數上限 8。
    【給 Reviewer — 零回歸】單一 cs 層且 dir=+1（預設）時，dir key 省略 → 正規化輸出與舊版逐字一致
    （layers_json/dedup canon 不變）；僅 dir=−1 才寫入 dir key。"""
    if not isinstance(layers, list) or not layers:
        return None, "layers 需為非空陣列"
    if len(layers) > 8:
        return None, "layers 上限為 8 層"
    norm = []
    for L in layers:
        if not isinstance(L, dict):
            return None, f"層格式錯誤：{L}"
        typ = L.get("type")
        if typ == "pead":
            norm.append({"type": "pead"})
        elif typ == "chip":
            norm.append({"type": "chip",
                         "window": int(L.get("window", 3)),
                         "min_net": float(L.get("min_net", 0.0))})
        elif L.get("factor") in _COMPOSITE_CS_FACTORS:
            tk = L.get("top_k", _CS_DEFAULT_K)
            try:
                tk = min(max(float(tk), 0.01), 1.0)
            except (TypeError, ValueError):
                tk = _CS_DEFAULT_K
            nl = {"factor": L["factor"], "top_k": tk}
            try:                                          # R1(a) per-layer dir∈{+1,−1}
                d = 1 if int(L.get("dir", 1)) >= 0 else -1
            except (TypeError, ValueError):
                d = 1
            if d == -1:                                   # 預設 +1 省略 → 零回歸
                nl["dir"] = -1
            norm.append(nl)
        else:
            return None, f"未知層：{L}（cs factor 須屬 {sorted(_COMPOSITE_CS_FACTORS)}，或 type=pead/chip）"
    return norm, None


def _cs_layer_eligible(rmap: dict, top_k: float, direction: int = 1) -> set:
    """單一 cs 層的可進場日期集合。rmap={date: pct∈[0,1]}（1=因子最高）。
    dir=+1 取 top K%（pct ≥ 1−K，多方：法人連買/動量強）；
    dir=−1 取 bottom K%（pct ≤ K，空方/反向：法人倒貨、高估值、過熱）。
    【給 Reviewer — 零回歸】dir=+1 時回 `{d for d,p if p>=1-top_k}`，與 _eval_composite_market
    舊版單 cs 內聯逐字等價。"""
    if direction >= 0:
        thr = 1.0 - top_k
        return {d for d, p in rmap.items() if p >= thr}
    return {d for d, p in rmap.items() if p <= top_k}


# ── R-PROD-7a/7b 核心：複合 AND-stack 評估（全複用既有引擎原語）──────────────
def _eval_composite_market(mkt, codes, start, end, layers, preloaded,
                           benchmark="equal_weight", regime_map=None,
                           progress_cb=None, should_cancel=None, sentiment_map=None):
    """複合 AND-stack 評估：進場＝各層條件同時成立（AND），出場/淨成本/CI/公平基準/ALPHA 閘
    沿用 base-rate 同尺。複用 _compute_stock_features / _cs_rank_pct / _a5_pead_triggers /
    _chip_streak_dates / _emit_composite_trades / _summarize_trades / _extract_row / _slice_trades_by_regime。
    回 {row(全期), trades, per_regime, cs_factor, cs_top_k, n_stocks, layer_kinds}。"""
    all_dates, bar_data = preloaded
    bench_map = _get_benchmark_close_map(mkt, start, end)
    # 公平基準（與 _fast_base_rate_market 同邏輯；MVP 預設等權公平基準）
    _bench = (benchmark or "equal_weight").lower()
    if _bench == "equal_weight":
        excess_bench_map = _equal_weight_bench_map(codes, all_dates, bar_data)
    elif _bench == "0050" and mkt == "TW":
        _m0050 = _fetch_index_close_map("0050.TW", start, end)
        excess_bench_map = _m0050 if _m0050 else bench_map
    else:
        excess_bench_map = bench_map

    # 解析層（R-PROD-7a：支援任意 k 個 cs 層 AND-stack + per-layer dir∈{+1,−1}）
    cs_layers = []; pead_layer = chip_layer = None
    for L in layers:
        if L.get("type") == "pead":
            pead_layer = L
        elif L.get("type") == "chip":
            chip_layer = L
        elif L.get("factor") in _COMPOSITE_CS_FACTORS:
            cs_layers.append(L)

    # context pass：每股 feats（outcome 表）
    feats = {}
    for ci, code in enumerate(codes):
        if should_cancel and should_cancel():
            break
        if progress_cb and ci % 10 == 0:
            progress_cb(ci, len(codes) * 2)
        f = _compute_stock_features(code, all_dates, bar_data)
        if not f:
            continue
        feats[code] = f

    # cs 層橫斷面排名（一次算齊所有 cs 層需要的因子；含籌碼）
    cs_factors_needed = {L["factor"] for L in cs_layers}
    rank_pct = _cs_rank_pct(mkt, codes, feats, cs_factors_needed, bench_map) if cs_factors_needed else {}
    # 報表主因子/K（單 cs 時即該層；多 cs 取首層 → 單層輸出與舊版逐字一致，零回歸）
    cs_factor = cs_layers[0]["factor"] if cs_layers else None
    cs_top_k = float(cs_layers[0].get("top_k", _CS_DEFAULT_K)) if cs_layers else None

    # 籌碼層（一次性 DB 讀）
    chip_elig = _chip_streak_dates(codes, chip_layer.get("window", 3),
                                   chip_layer.get("min_net", 0.0)) if chip_layer else {}
    # PEAD 事件
    earnings_by_code = _get_earnings_map(mkt, codes) if pead_layer else {}

    # 進場 pass：逐股組各層 eligible 日期集合 → AND → emit
    all_trades = []
    for ci, code in enumerate(codes):
        if should_cancel and should_cancel():
            break
        if progress_cb and ci % 10 == 0:
            progress_cb(len(codes) + ci, len(codes) * 2)
        f = feats.get(code)
        if not f:
            continue
        dts = f["dates"]
        layer_sets = []
        for L in cs_layers:                         # k 層 cs∧cs：每層各自取 top/bottom K%（dir）
            rmap = rank_pct.get(L["factor"], {}).get(code, {})
            layer_sets.append(_cs_layer_eligible(
                rmap, float(L.get("top_k", _CS_DEFAULT_K)), int(L.get("dir", 1))))
        if pead_layer:
            trig = _a5_pead_triggers(dts, f["c"], bench_map, earnings_by_code.get(code, []))
            layer_sets.append({dts[i] for i in range(len(dts)) if trig[i]})
        if chip_layer:
            layer_sets.append(chip_elig.get(code, set()))
        if not layer_sets:
            continue
        eligible = set.intersection(*layer_sets) if len(layer_sets) > 1 else layer_sets[0]
        if not eligible:
            continue
        _emit_composite_trades(f, eligible, all_trades)

    # 全期彙總 + ALPHA 閘
    summary = _summarize_trades(all_trades, mkt, excess_bench_map,
                                cs_factor=cs_factor, cs_top_k=cs_top_k)
    row = _extract_row(summary, mkt, "COMPOSITE")

    # R-PROD-6 regime 切片（per-regime G1 閘 → TREND_UP-only 條件 keeper）
    per_regime = {}
    if regime_map:
        for reg, trs in _slice_trades_by_regime(all_trades, regime_map).items():
            rs = _summarize_trades(trs, mkt, excess_bench_map,
                                   cs_factor=cs_factor, cs_top_k=cs_top_k)
            per_regime[reg] = _extract_row(rs, mkt, "COMPOSITE")

    # P0-3a 市場情緒「條件軸」切片（VIX/F&G 桶；與 4-regime 正交並存，純加法）。
    # 複用同一 _slice_trades_by_regime（按進場日分桶）→ 讓「A4 在 低VIX/貪婪 vs 高VIX/恐懼」可量測。
    per_sentiment = {}
    if sentiment_map:
        for sb, trs in _slice_trades_by_regime(all_trades, sentiment_map).items():
            rs = _summarize_trades(trs, mkt, excess_bench_map,
                                   cs_factor=cs_factor, cs_top_k=cs_top_k)
            per_sentiment[sb] = _extract_row(rs, mkt, "COMPOSITE")

    # layer_kinds：每 cs 層一筆（dir=−1 標 :dir-1）；單 cs dir+1 → "cs:<factor>"（與舊版逐字一致）
    cs_kinds = [("cs:" + L["factor"] + (":dir-1" if int(L.get("dir", 1)) < 0 else ""))
                for L in cs_layers]
    layer_kinds = cs_kinds + (["pead"] if pead_layer else []) + (["chip"] if chip_layer else [])
    return {"row": row, "trades": all_trades, "per_regime": per_regime,
            "per_sentiment": per_sentiment,
            "cs_factor": cs_factor, "cs_top_k": cs_top_k,
            "n_stocks": len(feats), "layer_kinds": layer_kinds}


# ══════════════════════════════════════════════════════════════════════════════
# R-LABEL（gate-label-conditional-reframe F 節，2026-06-27）：六標籤分級欄位。
#   全加法（additive）地基件：只「擴充欄位 + 保守落值」，不取代既有 status、不改任何
#   選股/閘/打分邏輯（那是後續 R-COND-SLICE / R-PROD-LABEL-WIRE，不在本工單）。
#   命門＝零回歸：① 新欄全部 ADD COLUMN（metadata-only，既有 explicit-column 的 INSERT/
#   UPDATE/SELECT 一律不列新欄 → 既有讀寫路徑位元級不變）；② backfill 只 UPDATE 新欄、
#   WHERE label IS NULL（冪等、重入安全、絕不覆寫已標、絕不亂升級）。
# ──────────────────────────────────────────────────────────────────────────────
# 六標籤（A 節）：ALPHA-keeper / conditional-ALPHA / defensive-conditional /
#   POSITIVE_EV-beta / composite-feature-only / parked-unproven
# R-LABEL 擴充欄（兩表共用；皆 nullable，預設 NULL → 未標 = parked 語義由讀方判定）
_RLABEL_COLS = (
    ("label",               "TEXT"),  # 六標籤 enum（A.1）
    ("permitted_use",       "TEXT"),  # enum{standalone,conditional,feature,beta-sleeve,none}
    ("confidence_cap",      "TEXT"),  # enum{keeper,candidate,feature,beta,none}（量產信心上限）
    ("gating_condition",    "TEXT"),  # 條件上閘（如 regime=TREND_UP / regime=RISK_OFF）；NULL=未限定
    ("label_evidence",      "TEXT"),  # JSON：初判依據（src/status/basis），後續接 G1–G5 逐項
    ("live_pending",        "INTEGER"),  # 1=待 live(G5) 確認，封在候選級（E.5 紅線#5）
    ("single_market",       "INTEGER"),  # 1=僅單市場驗過（待 G4 跨市場），封信心
    ("parked_subtype",      "TEXT"),  # enum{honest-negative,untested,inconclusive}；NULL=未知
    ("retrigger_condition", "TEXT"),  # parked 重啟條件
)


def _rlabel_migrate(cur, table):
    """冪等補 R-LABEL 欄到指定表（ADD COLUMN 為 metadata-only；duplicate-column 吞掉 → 重入安全）。"""
    for col, typ in _RLABEL_COLS:
        try:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")
        except Exception:
            pass


def _rlabel_initial(status, market, regime_gate=None):
    """從現有 status 保守對映初始標籤（不確定 → parked-unproven，絕不亂升級、絕不給 keeper）。
       回傳欄位 dict（鍵 = _RLABEL_COLS 名）。對映規則（task + A.1 表）：
         ALPHA            → conditional-ALPHA（候選級，封 live/single-market，不升 keeper）
         active(forward)  → conditional-ALPHA（已滾入紙上、受 regime_gate 條件約束）
         POSITIVE_EV      → POSITIVE_EV-beta（beta sleeve，不灌 alpha 信心）
         FAIL/retired/其他 → parked-unproven（量產不用；subtype 未知標 NULL，不臆斷 honest-negative）"""
    s = (status or "").strip().upper()
    single = 1 if (market or "").strip() else None
    if s == "ALPHA" or s == "ACTIVE":
        gating = f"regime={regime_gate}" if (regime_gate and str(regime_gate).strip()) else None
        return {
            "label": "conditional-ALPHA",
            "permitted_use": "conditional",
            "confidence_cap": "candidate",
            "gating_condition": gating,
            "label_evidence": json.dumps(
                {"src": "R-LABEL-init", "status": s,
                 "basis": "conservative status->label; no live(G5)/cross-market(G4) evidence"},
                ensure_ascii=False),
            "live_pending": 1,
            "single_market": single,
            "parked_subtype": None,
            "retrigger_condition": None,
        }
    if s == "POSITIVE_EV":
        return {
            "label": "POSITIVE_EV-beta",
            "permitted_use": "beta-sleeve",
            "confidence_cap": "beta",
            "gating_condition": None,
            "label_evidence": json.dumps(
                {"src": "R-LABEL-init", "status": s,
                 "basis": "positive-EV but excess vs benchmark not significant -> beta sleeve"},
                ensure_ascii=False),
            "live_pending": None,
            "single_market": single,
            "parked_subtype": None,
            "retrigger_condition": None,
        }
    # FAIL / retired / NULL / 未知 → 保守 parked-unproven（不臆斷 subtype）
    return {
        "label": "parked-unproven",
        "permitted_use": "none",
        "confidence_cap": "none",
        "gating_condition": None,
        "label_evidence": json.dumps(
            {"src": "R-LABEL-init", "status": s or None,
             "basis": "conservative default; not promoted"},
            ensure_ascii=False),
        "live_pending": None,
        "single_market": None,
        "parked_subtype": None,   # 未知（不臆斷 honest-negative vs untested）
        "retrigger_condition": "re-test under pre-registered sector x regime slice (R-COND-SLICE)",
    }


def _rlabel_backfill(cur, table, key_col, status_col, regime_col=None):
    """保守落初始標籤：只 UPDATE label IS NULL 的列（冪等、不覆寫已標）。
       只寫 R-LABEL 新欄，既有欄一律不碰 → 既有讀路徑位元級不變。"""
    sel = f"SELECT {key_col},{status_col}" + (f",{regime_col}" if regime_col else "") + \
          f",market FROM {table} WHERE label IS NULL"
    rows = cur.execute(sel).fetchall()
    set_cols = ",".join(f"{c}=?" for c, _ in _RLABEL_COLS)
    for r in rows:
        key = r[0]; status = r[1]
        regime = r[2] if regime_col else None
        market = r[-1]
        vals = _rlabel_initial(status, market, regime_gate=regime)
        params = [vals[c] for c, _ in _RLABEL_COLS] + [key]
        cur.execute(f"UPDATE {table} SET {set_cols} WHERE {key_col}=?", params)
    return len(rows)


# ── R-COND-SLICE-A：pre-registration 表 + 升格防火牆 ────────────────────────────
#   設計依據：2026-06-27-gate-label-conditional-reframe.md B 節（假設驅動條件重測協議）
#   + F 節 R-COND-SLICE。核心：要升 ② conditional-ALPHA（可單獨條件上），必須事前 pre-register
#   機制+切片（「先講 why 才准測」）；盲掃 daemon 無 prereg → 永遠升不了（B.5 防火牆）。
#   本件只做 prereg 表 + 升格牆（純新增、零回歸）；sector 切片引擎與 DSR-T 改造屬 R-COND-SLICE-B。

def _parse_ts(ts):
    """把 epoch 數字 / 數字字串 / ISO8601 字串統一解析成 epoch float；無法解析回 None。
       升格防火牆用它把 prereg.created_ts 與外部傳入的 backtest_run_ts 化到同一刻度比較。"""
    if ts is None:
        return None
    if isinstance(ts, bool):           # bool 是 int 子類，明確排除
        return None
    if isinstance(ts, (int, float)):
        return float(ts)
    s = str(ts).strip()
    if not s:
        return None
    try:
        return float(s)                # 純數字字串（epoch）
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(s).timestamp()   # ISO8601（與全檔 created_at 同款）
    except Exception:
        return None


def _ensure_preregistration_table():
    """冪等建表：preregistration（事前假設紀錄；monitor.db / db()）。
       每筆＝一次「先講機制+切片+預測符號 才准測」的 pre-register，帶不可事後改的 created_ts。
       欄位對齊 F 節 R-COND-SLICE：{id, signal_id, hypothesized_mechanism, target_slices(JSON),
       predicted_sign(JSON), author, created_ts(epoch timestamp), notes}；created_iso 為顯示便利欄。"""
    con = db()
    con.execute("""CREATE TABLE IF NOT EXISTS preregistration(
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id               TEXT,
        hypothesized_mechanism  TEXT,
        target_slices           TEXT,   -- JSON array（要測的 regime/sector 切片）
        predicted_sign          TEXT,   -- JSON（每切片預測符號，事前寫的）
        author                  TEXT,
        created_ts              REAL,   -- epoch 秒（升格防火牆比對基準，落地後不改）
        created_iso             TEXT,   -- 人讀時間戳（顯示用）
        notes                   TEXT
    )""")
    con.commit(); con.close()


def _preregister(signal_id, hypothesized_mechanism, target_slices, predicted_sign,
                 author=None, notes=None, created_ts=None):
    """落一筆 pre-registration。created_ts 不帶 → 現在 epoch（測試可注入以驗早/晚於回測）。
       target_slices / predicted_sign 接受 list/dict（自動 json.dumps）或已是 JSON 字串。回 inserted row dict。"""
    _ensure_preregistration_table()
    ts = float(created_ts) if created_ts is not None else time.time()
    iso = datetime.fromtimestamp(ts).isoformat()
    def _as_json(v):
        if v is None or isinstance(v, str):
            return v
        return json.dumps(v, ensure_ascii=False)
    con = db(); cur = con.cursor()
    cur.execute("""INSERT INTO preregistration
        (signal_id,hypothesized_mechanism,target_slices,predicted_sign,author,created_ts,created_iso,notes)
        VALUES(?,?,?,?,?,?,?,?)""",
        (signal_id, hypothesized_mechanism, _as_json(target_slices), _as_json(predicted_sign),
         author, ts, iso, notes))
    con.commit()
    rid = cur.lastrowid
    con.close()
    return {"id": rid, "signal_id": signal_id, "created_ts": ts, "created_iso": iso}


def _can_promote_to_conditional_alpha(signal_id, backtest_run_ts):
    """升格防火牆（B 節核心紅線）：要升 ② conditional-ALPHA（拿到「單獨條件上」權限），必須能連到
       一筆 created_ts 早於本次回測執行時刻(backtest_run_ts) 的 pre-registration（事前寫的機制+切片）。
       連不到 → 最高停在 candidate / composite-feature-only。盲掃(daemon)無 prereg → 永遠 False。
       回 (can_promote: bool, reason: str)。fail-closed：signal_id 空 / ts 無法解析 → False。"""
    _ensure_preregistration_table()
    sid = (signal_id or "").strip()
    if not sid:
        return False, "no signal_id → cannot bind pre-registration (blind-scan / unidentified); cap at candidate/composite-feature-only"
    bt = _parse_ts(backtest_run_ts)
    if bt is None:
        return False, f"backtest_run_ts unparseable ({backtest_run_ts!r}) → fail-closed, refuse promotion"
    con = db(); cur = con.cursor()
    rows = cur.execute(
        "SELECT id,created_ts,created_iso,hypothesized_mechanism FROM preregistration "
        "WHERE signal_id=? ORDER BY created_ts", (sid,)).fetchall()
    con.close()
    if not rows:
        return False, (f"no pre-registration for signal_id={sid} → blind-scan: "
                       f"cap at candidate/composite-feature-only, cannot promote to conditional-ALPHA")
    eligible = [r for r in rows if (r[1] is not None and float(r[1]) < bt)]
    if not eligible:
        earliest = min((r for r in rows if r[1] is not None),
                       key=lambda r: float(r[1]), default=None)
        et = earliest[1] if earliest else None
        return False, (f"pre-registration(s) exist for {sid} but none predate backtest_run_ts "
                       f"(earliest prereg ts={et} >= backtest ts={bt}) → post-hoc, refuse promotion")
    pre = eligible[0]
    return True, (f"pre-registered before backtest (prereg id={pre[0]} ts={pre[1]} "
                  f"mechanism='{pre[3]}') predates backtest ts={bt} → eligible for conditional-ALPHA")


# ── R-PROD-7e：試驗持久化（知識森林 store，重啟不丟、續搜基礎）─────────────────
def _ensure_composite_search_tables():
    """冪等建表：composite_candidates（每次複合評估 = 一筆試驗）。"""
    con = db()
    con.execute("""CREATE TABLE IF NOT EXISTS composite_candidates(
        cand_id        TEXT PRIMARY KEY,
        created_at     TEXT,
        parent_id      TEXT,
        market         TEXT, start TEXT, end TEXT, benchmark TEXT,
        layers_json    TEXT,
        depth          INTEGER,
        n              INTEGER,
        win_rate       REAL,
        avg_return     REAL,
        excess_avg     REAL,
        excess_ci_low  REAL,
        excess_ci_high REAL,
        sharpe         REAL,
        status         TEXT,
        dsr            REAL,
        pbo            REAL,
        trial_T        INTEGER,
        per_regime_json TEXT,
        returns_json    TEXT,
        pnl_blocks_json TEXT,
        notes          TEXT
    )""")
    # P0-3a 冪等遷移：補 per_sentiment_json 欄（舊 DB 無此欄；ADD COLUMN 為 metadata-only，
    # 既有 INSERT/UPDATE 不列此欄故零回歸；duplicate column 例外吞掉 → 重入安全）。
    try:
        con.execute("ALTER TABLE composite_candidates ADD COLUMN per_sentiment_json TEXT")
    except Exception:
        pass
    # R-LABEL（2026-06-27）：補六標籤欄 + 保守落初始標籤（冪等、零回歸；見 _RLABEL_COLS）。
    cur = con.cursor()
    _rlabel_migrate(cur, "composite_candidates")
    _rlabel_backfill(cur, "composite_candidates", "cand_id", "status")
    con.commit(); con.close()


_COMPOSITE_JOBS: dict = {}   # job_id -> {status, progress, total, current, result, error, ...}


def _composite_search_worker(job_id, market, codes, start, end, layers, benchmark, regime_slice, parent_id,
                             sentiment_slice=False, sentiment_source="vix", sentiment_mode="fixed"):
    """背景：載入資料 → _eval_composite_market → DSR（對全持久化試驗 T 折減）→ 落地 →
    PBO（CSCV，對本批 pnl_blocks）→ 更新該筆。全本地計算，零 token。
    P0-3a：sentiment_slice=True 時另建市場情緒條件軸（VIX/F&G 桶）切片 → per_sentiment（與 4-regime 正交）。"""
    import json
    job = _COMPOSITE_JOBS[job_id]
    try:
        _ensure_composite_search_tables()
        job["status"] = "running"; job["current"] = f"{market}/loading {len(codes)} stocks..."
        preloaded = _load_backtest_data(codes, market, start, end)
        # regime map（R-PROD-6；切片用）
        regime_map = {}
        if regime_slice:
            try:
                regime_map = {r["date"]: r["regime"] for r in _batch_calc_regime(market, start, end)}
            except Exception as e:
                print(f"[search] regime map failed: {e}", flush=True)
        # P0-3a 情緒條件軸 map（市場級 VIX/F&G 桶；預設關閉 → 零回歸）
        sentiment_map = {}; sentiment_meta = None
        if sentiment_slice:
            try:
                _sm = _sentiment_regime_map(start, end, source=sentiment_source, mode=sentiment_mode)
                sentiment_map = _sm.get("map", {})
                sentiment_meta = {k: v for k, v in _sm.items() if k != "map"}
            except Exception as e:
                print(f"[search] sentiment map failed: {e}", flush=True)

        def _prog(done, tot):
            job["progress"] = done; job["total"] = tot
            job["current"] = f"{market}/composite eval {done}/{tot}"

        res = _eval_composite_market(market, codes, start, end, layers, preloaded,
                                     benchmark=benchmark, regime_map=regime_map,
                                     progress_cb=_prog, should_cancel=lambda: job.get("cancelled"),
                                     sentiment_map=sentiment_map)
        row = res["row"]
        trades = res["trades"]
        returns = [t[2] for t in trades]
        pnl_blocks = _bin_returns_to_blocks(trades, start, end, 16)
        sharpe = row.get("sharpe") or 0.0
        cand_id = secrets.token_hex(8)
        excess_ci = row.get("excess_ci95") or [None, None]

        # ── 落地本筆（dsr/pbo 暫 NULL，下面回填）──
        con = db(); cur = con.cursor()
        cur.execute("""INSERT INTO composite_candidates
            (cand_id,created_at,parent_id,market,start,end,benchmark,layers_json,depth,
             n,win_rate,avg_return,excess_avg,excess_ci_low,excess_ci_high,sharpe,status,
             dsr,pbo,trial_T,per_regime_json,returns_json,pnl_blocks_json,notes)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
            cand_id, datetime.now().isoformat(), parent_id, market, start, end, benchmark,
            json.dumps(layers, ensure_ascii=False), len(layers),
            row.get("N"), row.get("win_rate"), row.get("avg_return"), row.get("excess_avg"),
            excess_ci[0], excess_ci[1], sharpe, row.get("status"),
            None, None, None,
            json.dumps(res["per_regime"], ensure_ascii=False),
            json.dumps(returns), json.dumps(pnl_blocks),
            f"layers={res['layer_kinds']} stocks={res['n_stocks']}"))
        con.commit()

        # ── §3.1 DSR（B.2 S5 誠實 T）：T = 該候選所屬搜尋(同 market/start/end/benchmark)累計試驗數，
        #    trial_sharpes 同步 scope 到該 cohort（不再混入無關全宇宙）。_deflated_sharpe_ratio 數學不變。 ──
        T, all_sharpes, _honest_src = _honest_trial_T(cur, market, start, end, benchmark)
        dsr_res = _deflated_sharpe_ratio(returns, all_sharpes, T)

        # ── §3.2 PBO：本批（同 market/start/end/n_blocks）所有試驗的 pnl_blocks 建矩陣 ──
        pbo_res = {"pbo": None, "reason": "need >=2 candidates"}
        batch = cur.execute(
            "SELECT pnl_blocks_json FROM composite_candidates WHERE market=? AND start=? AND end=?",
            (market, start, end)).fetchall()
        cols = []
        for (pj,) in batch:
            try:
                blk = json.loads(pj) if pj else None
            except Exception:
                blk = None
            if blk and len(blk) == 16:
                cols.append([(x if x is not None else float("nan")) for x in blk])
        if len(cols) >= 2:
            import numpy as np
            M = np.array(cols, dtype=float).T   # (16, N)
            pbo_res = _pbo_cscv(M, S=16)

        cur.execute("UPDATE composite_candidates SET dsr=?, pbo=?, trial_T=? WHERE cand_id=?",
                    (dsr_res.get("dsr"), pbo_res.get("pbo"), T, cand_id))
        # P0-3a：per_sentiment 落地（獨立 UPDATE → 既有 INSERT/UPDATE 逐字不變，零回歸）
        if res.get("per_sentiment"):
            cur.execute("UPDATE composite_candidates SET per_sentiment_json=? WHERE cand_id=?",
                        (json.dumps(res["per_sentiment"], ensure_ascii=False), cand_id))
        con.commit(); con.close()

        # ── 分級閘判讀（G1 既有 status；G2 DSR>0.95；G3 PBO<0.20）──
        g1 = row.get("status")
        g2 = (dsr_res.get("dsr") is not None and dsr_res["dsr"] > 0.95)
        g3 = (pbo_res.get("pbo") is not None and pbo_res["pbo"] < 0.20)
        if g1 == "ALPHA" and g2 and g3:
            grade = "穩健候選(過G1-G3，待US/live)"
        elif g1 == "ALPHA":
            grade = "ALPHA候選(過G1，DSR/PBO未達或不足)"
        else:
            grade = g1

        job["result"] = {
            "cand_id": cand_id, "market": market, "start": start, "end": end,
            "benchmark": benchmark, "layers": layers, "trial_T": T,
            "honest_t_source": _honest_src,
            "g1_status": g1, "gate_grade": grade,
            "g1_row": row, "dsr": dsr_res, "pbo": pbo_res,
            "per_regime": res["per_regime"], "n_stocks": res["n_stocks"],
            "per_sentiment": res.get("per_sentiment", {}), "sentiment_meta": sentiment_meta,
            "n_trades": row.get("N"), "cs_factor": res["cs_factor"], "cs_top_k": res["cs_top_k"],
        }
        job["status"] = "cancelled" if job.get("cancelled") else "done"
        job["completed_at"] = datetime.now().isoformat()
        print(f"[search] composite {cand_id} done: N={row.get('N')} status={g1} "
              f"DSR={dsr_res.get('dsr')} PBO={pbo_res.get('pbo')} T={T}", flush=True)
    except Exception as e:
        import traceback; traceback.print_exc()
        job["status"] = "error"; job["error"] = str(e)
        job["completed_at"] = datetime.now().isoformat()


@app.post("/api/search/composite")
def run_composite_search(config: dict, _: None = Depends(require_token)):
    """R-PROD-7d：LLM 零 token 觸發一次複合 AND-stack 評估（背景 job，回 job_id 輪詢）。
    body: {layers:[{factor,top_k}|{type:'pead'}|{type:'chip',window,min_net}], market, start, end,
           benchmark, regime_slice, parent_id}。複用 base-rate 引擎；結果含 G1/DSR/PBO/per-regime，落地持久化。"""
    layers_raw = config.get("layers")
    layers, err = _normalize_layers(layers_raw)
    if err:
        return JSONResponse({"ok": False, "error": err}, status_code=400)
    market = str(config.get("market", "TW")).upper()
    start = str(config.get("start", "2020-01-01"))
    end = str(config.get("end", datetime.now().strftime("%Y-%m-%d")))
    # 輸入驗證：start/end 須為合法 ISO 日期且 start<=end（壞日期不再靜默退化成空試驗）
    try:
        sdt = datetime.strptime(start, "%Y-%m-%d")
        edt = datetime.strptime(end, "%Y-%m-%d")
    except (TypeError, ValueError):
        return JSONResponse({"ok": False, "error": "start/end 須為 YYYY-MM-DD 格式"}, status_code=400)
    if sdt > edt:
        return JSONResponse({"ok": False, "error": "start 不可晚於 end"}, status_code=400)
    benchmark = str(config.get("benchmark", "equal_weight")).lower()
    if benchmark not in _BENCHMARK_WHITELIST:
        benchmark = "equal_weight"
    regime_slice = bool(config.get("regime_slice", True))
    # P0-3a 市場情緒條件軸切片（預設關閉 → 零回歸；開啟才另算 per_sentiment）
    sentiment_slice = bool(config.get("sentiment_slice", False))
    sentiment_source = str(config.get("sentiment_source", "vix")).lower()
    if sentiment_source not in ("vix", "fng"):
        sentiment_source = "vix"
    sentiment_mode = str(config.get("sentiment_mode", "fixed")).lower()
    if sentiment_mode not in ("fixed", "tercile"):
        sentiment_mode = "fixed"
    parent_id = config.get("parent_id")

    # universe（複用 get_universe）
    uni = get_universe(market)
    if isinstance(uni, JSONResponse):
        return JSONResponse({"ok": False, "error": f"universe not available for {market}"}, status_code=400)
    raw = uni.get("data", uni.get("stocks", []))
    codes = [s if isinstance(s, str) else s.get("code", "") for s in raw]
    codes = [c for c in codes if c]
    if not codes:
        return JSONResponse({"ok": False, "error": f"empty universe for {market}"}, status_code=400)

    # job store 上限防呆：超過上限時，淘汰最舊的「已結束(done/error)」job（保留進行中的）
    if len(_COMPOSITE_JOBS) >= 200:
        finished = [(jid, j.get("completed_at") or j.get("started_at") or "")
                    for jid, j in _COMPOSITE_JOBS.items() if j.get("status") in ("done", "error")]
        for jid, _ts in sorted(finished, key=lambda x: x[1])[:max(1, len(finished) // 2)]:
            _COMPOSITE_JOBS.pop(jid, None)

    job_id = secrets.token_hex(8)
    _COMPOSITE_JOBS[job_id] = {
        "status": "starting", "progress": 0, "total": 0, "current": "", "cancelled": False,
        "result": None, "error": None,
        "config": {"market": market, "start": start, "end": end, "benchmark": benchmark,
                   "layers": layers, "regime_slice": regime_slice,
                   "sentiment_slice": sentiment_slice, "sentiment_source": sentiment_source,
                   "sentiment_mode": sentiment_mode},
        "started_at": datetime.now().isoformat(), "completed_at": None,
    }
    threading.Thread(target=_composite_search_worker,
                     args=(job_id, market, codes, start, end, layers, benchmark, regime_slice, parent_id),
                     kwargs={"sentiment_slice": sentiment_slice, "sentiment_source": sentiment_source,
                             "sentiment_mode": sentiment_mode},
                     daemon=True).start()
    return {"ok": True, "job_id": job_id, "layers": layers, "market": market,
            "message": f"Composite search started ({len(codes)} stocks). "
                       f"Poll GET /api/search/composite/{job_id}."}


@app.get("/api/search/composite/{job_id}")
def get_composite_search(job_id: str):
    """輪詢複合搜尋 job：done → 回 G1/DSR/PBO/per-regime 摘要（zero-token 給 LLM 讀）。"""
    # job_id 格式防呆（token_hex(8) → 16 位 hex；非法格式直接 404，不必查 dict）
    if (not isinstance(job_id, str) or not (8 <= len(job_id) <= 64)
            or any(c not in "0123456789abcdef" for c in job_id)):
        return JSONResponse({"error": "job not found"}, status_code=404)
    job = _COMPOSITE_JOBS.get(job_id)
    if not job:
        return JSONResponse({"error": "job not found"}, status_code=404)
    resp = {"job_id": job_id, "status": job["status"], "progress": job["progress"],
            "total": job["total"], "current": job.get("current", "")}
    if job["status"] == "done":
        resp["result"] = job["result"]
    elif job["status"] == "error":
        resp["error"] = job.get("error")
    return resp


# ── P0-3a：市場情緒條件軸 inspect + 資料補檔 endpoints ─────────────────────────
@app.get("/api/sentiment/regime")
def get_sentiment_regime(start: str = "2020-01-01", end: str = "", source: str = "vix", mode: str = "fixed"):
    """市場情緒「條件軸」(VIX/F&G)分桶 inspect（市場級，非 cs 選股因子）。回 thresholds/counts/桶分佈
    + 最近樣本，供 AIF 設計「A4 在 低VIX/貪婪 vs 高VIX/恐懼」條件切片。零 token、純本地計算。"""
    if not end:
        end = datetime.now().strftime("%Y-%m-%d")
    source = str(source).lower(); mode = str(mode).lower()
    if source not in ("vix", "fng"):
        return JSONResponse({"error": "source 須為 vix 或 fng"}, status_code=400)
    if mode not in ("fixed", "tercile"):
        return JSONResponse({"error": "mode 須為 fixed 或 tercile"}, status_code=400)
    try:
        res = _sentiment_regime_map(start, end, source=source, mode=mode)
        smap = res.pop("map", {})
        res["recent_sample"] = dict(sorted(smap.items())[-15:])   # 不回整個 map（太大），回最近 15 日
        res["window"] = {"start": start, "end": end}
        return res
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/sentiment/backfill_vix")
def backfill_vix(config: dict = None, _: None = Depends(require_token)):
    """補 ^VIX 日線 2010→今 到 daily_kbar（market.db；INSERT OR IGNORE 純加法，與 chip backfill 的
    monitor.db 不同庫，零鎖競爭）。body 可選 {start,end}。零 token。"""
    config = config or {}
    start = str(config.get("start", "2010-01-01"))
    end = config.get("end") or None
    try:
        return _backfill_vix_history(start, end)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/sentiment/backfill_fng")
def backfill_fng(_: None = Depends(require_token)):
    """補 CNN Fear&Greed 日值 2011→今 到 sentiment_daily（免費 GitHub CSV；INSERT OR IGNORE 純加法）。零 token。"""
    try:
        return _backfill_fng_history()
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/search/overfit")
def run_overfit_check(config: dict, _: None = Depends(require_token)):
    """R-PROD-7c：對「本批試驗」算 PBO（CSCV）+ 各候選 DSR。
    body: {cand_ids:[...]} 或 {market,start,end}（同窗的全部試驗）。零 token、純讀持久化 + 本地計算。"""
    import json
    _ensure_composite_search_tables()
    con = db(); cur = con.cursor()
    cand_ids = config.get("cand_ids")
    if cand_ids:
        if not isinstance(cand_ids, list) or len(cand_ids) > 1000:
            con.close()
            return JSONResponse({"ok": False, "error": "cand_ids 須為陣列且≤1000"}, status_code=400)
        qs = ",".join("?" * len(cand_ids))
        rows = cur.execute(
            f"SELECT cand_id,layers_json,sharpe,returns_json,pnl_blocks_json,n FROM composite_candidates "
            f"WHERE cand_id IN ({qs})", list(cand_ids)).fetchall()
    else:
        market = str(config.get("market", "TW")).upper()
        start = str(config.get("start", "2020-01-01"))
        end = str(config.get("end", datetime.now().strftime("%Y-%m-%d")))
        try:
            datetime.strptime(start, "%Y-%m-%d"); datetime.strptime(end, "%Y-%m-%d")
        except (TypeError, ValueError):
            con.close()
            return JSONResponse({"ok": False, "error": "start/end 須為 YYYY-MM-DD 格式"}, status_code=400)
        rows = cur.execute(
            "SELECT cand_id,layers_json,sharpe,returns_json,pnl_blocks_json,n FROM composite_candidates "
            "WHERE market=? AND start=? AND end=?", (market, start, end)).fetchall()
    # B.2 S5 誠實 T：本批送進來的候選＝這次要一起檢定的試驗集；T = 該批**有效**試驗數（n≥門檻、
    #   地板保護），trial_sharpes 同集（排除退化切片，不混入無關全宇宙）。決定性、不依 insert 順序。math 不變。
    all_sharpes = [r[2] for r in rows if r[2] is not None and (r[5] or 0) >= _MIN_TRIAL_N]
    T = max(len(all_sharpes), _HONEST_T_ORPHAN_FLOOR)
    con.close()
    if not rows:
        return {"ok": False, "error": "no candidates matched", "trial_T": T}

    cols = []
    per_cand = []
    for cid, lj, sharpe, rj, pj, _n in rows:
        try:
            rets = json.loads(rj) if rj else []
        except Exception:
            rets = []
        dsr_res = _deflated_sharpe_ratio(rets, all_sharpes, T)
        per_cand.append({"cand_id": cid, "layers": (json.loads(lj) if lj else None),
                         "sharpe": sharpe, "dsr": dsr_res.get("dsr"), "n": dsr_res.get("n")})
        try:
            blk = json.loads(pj) if pj else None
        except Exception:
            blk = None
        if blk and len(blk) == 16:
            cols.append([(x if x is not None else float("nan")) for x in blk])
    pbo_res = {"pbo": None, "reason": "need >=2 candidates"}
    if len(cols) >= 2:
        import numpy as np
        pbo_res = _pbo_cscv(np.array(cols, dtype=float).T, S=16)
    return {"ok": True, "trial_T": T, "n_candidates": len(rows),
            "pbo": pbo_res, "per_candidate": per_cand,
            "gate": {"G2_DSR_gt_0.95": "keeper級", "G3_PBO_lt_0.20": "keeper級"}}


@app.get("/api/search/candidates")
def list_composite_candidates(market: str = "", limit: int = 100,
                              _: None = Depends(require_token)):
    """列出已持久化的複合試驗（續搜基礎；證明重啟不丟）。"""
    import json
    try:
        limit = min(max(int(limit), 1), 1000)   # clamp 防呆：避免一次拉爆全表
    except (TypeError, ValueError):
        limit = 100
    if market:
        market = str(market).strip().upper()
        if not market.isalnum() or len(market) > 8:
            return JSONResponse({"ok": False, "error": "invalid market"}, status_code=400)
    _ensure_composite_search_tables()
    con = db(); cur = con.cursor()
    if market:
        rows = cur.execute(
            "SELECT cand_id,created_at,market,start,end,layers_json,n,excess_avg,excess_ci_low,"
            "sharpe,status,dsr,pbo,trial_T FROM composite_candidates WHERE market=? "
            "ORDER BY created_at DESC LIMIT ?", (market.upper(), limit)).fetchall()
    else:
        rows = cur.execute(
            "SELECT cand_id,created_at,market,start,end,layers_json,n,excess_avg,excess_ci_low,"
            "sharpe,status,dsr,pbo,trial_T FROM composite_candidates "
            "ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
    total = cur.execute("SELECT COUNT(*) FROM composite_candidates").fetchone()[0]
    con.close()
    cols = ["cand_id", "created_at", "market", "start", "end", "layers", "n", "excess_avg",
            "excess_ci_low", "sharpe", "status", "dsr", "pbo", "trial_T"]
    out = []
    for r in rows:
        d = dict(zip(cols, r))
        try:
            d["layers"] = json.loads(d["layers"]) if d["layers"] else None
        except Exception:
            pass
        out.append(d)
    return {"ok": True, "trial_T_total": total, "count": len(out), "candidates": out}


# ══════════════════════════════════════════════════
# Track A — 自主前向貪婪搜尋 daemon（machine-time，零 LLM token）
# 規格：2026-06-25-composite-search-engine.md §2.2/§2.5/§六
# 全加法：複用 _eval_composite_market / DSR / PBO / 持久化，不重寫任何數值。
# ══════════════════════════════════════════════════

# ── 共享狀態（多執行緒，所有可變欄位走 _AUTO_SEARCH_LOCK）──────────────────────
_AUTO_SEARCH_RUNS: dict = {}            # run_id -> state dict
_AUTO_SEARCH_LOCK = threading.Lock()

# 可插拔搜尋空間（確定性、有界）
_AUTO_FACTOR_POOL = ("sector_rel_topsec", "sector_rel", "resid_mom", "rel_str", "raw_mom")
_AUTO_CHIP_CS_POOL = ("inst_net_buy", "trust_net_buy", "short_squeeze", "sbl_short")  # P0-1：可疊加的籌碼 cs 層（A4∧籌碼）；TW-SBL 借券賣出納入 daemon 自動列舉 add_cs:sbl_short
_AUTO_REVENUE_CS_POOL = ("revenue_mom",)  # P0-2：可疊加的月營收 cs 層（A4∧基本面正交複合）
# Type 2 US-SI：可疊加的美股籌碼 cs 層（daemon mkt="US" run 自動列舉 A4_US∧short_interest）。
# 本次只 short_interest；inst_13f 為後續階段（留好擴充）。US-gated orchestrator → TW run 此因子自然回空。
_AUTO_US_CHIP_CS_POOL = ("short_interest",)
# D1：可疊加的集保大戶結構 cs 層（daemon TW run 自動列舉 A4∧holder_concentration / A4∧holder_conc_delta）。
# TW-gated orchestrator → US run 此因子自然回空。補池子最大空白(level/結構)、與 flow chip 天生正交。
_AUTO_TDCC_CS_POOL = ("holder_concentration", "holder_conc_delta")
# R-PROD-7h-1：價格族 4 新因子入搜尋池（解鎖 style/macro/reversal 三族跨族正交堆疊）。純加法 add_cs move。
_AUTO_PRICE_CS_POOL = ("low_beta", "rate_sensitivity", "dollar_sensitivity", "short_rev")
_AUTO_TOPK_POOL   = (0.10, 0.20, 0.30)
_AUTO_MAX_DEPTH   = 4                   # §2.5：層上限（控過擬合/容量）；k 層 cs∧cs + pead/chip 合計 ≤ 此
_AUTO_REPORT_CORR_PRUNE = 0.85          # R-PROD-7h-2：add_cs 候選 pnl_blocks 對 frontier corr≥此 → 軟剪（換湯不換藥）
# lessons 剪枝鉤：已知失敗單因子（預設空，留給 lessons 注入；含此因子的 cs 層一律剪）
_AUTO_LESSONS_SKIP_FACTORS: set = set()

# 種子＝目前 SOTA：A4 = sector_rel_topsec @ TREND_UP（§九 MVP）
_AUTO_SEED_LAYERS = [{"factor": "sector_rel_topsec", "top_k": _CS_DEFAULT_K}]


# ── 純函式（無 DB / 無 eval，可單元隔離測）──────────────────────────────────────
def _layers_canon(layers: list) -> str:
    """layers 的確定性正規 key（dedup 用，順序無關）。"""
    parts = []
    for L in (layers or []):
        if L.get("type") == "pead":
            parts.append("pead")
        elif L.get("type") == "chip":
            parts.append(f"chip:{int(L.get('window', 3))}:{float(L.get('min_net', 0.0))}")
        elif L.get("factor") in _COMPOSITE_CS_FACTORS:
            try:
                suffix = ":d-1" if int(L.get("dir", 1)) < 0 else ""   # dir=+1 → 空 suffix（零回歸）
            except (TypeError, ValueError):
                suffix = ""
            parts.append(f"cs:{L['factor']}:{round(float(L.get('top_k', _CS_DEFAULT_K)), 4)}{suffix}")
    return "|".join(sorted(parts))


def _auto_replace_cs(current: list, factor=None, top_k=None) -> list:
    """回 current 的副本，僅替換其 cs 層的 factor/top_k（深拷貝每層 dict，不改原 list）。"""
    out = []
    for L in current:
        if L.get("factor") in _COMPOSITE_CS_FACTORS:
            nl = dict(L)
            if factor is not None:
                nl["factor"] = factor
            if top_k is not None:
                nl["top_k"] = float(top_k)
            out.append(nl)
        else:
            out.append(dict(L))
    return out


def _auto_price_cs_layer(cf: str) -> dict:
    """價格族 add_cs 候選層工廠：套 _PRICE_CS_META 的 default dir（low_beta=−1 BAB；其餘 +1）。
    （_normalize_layers：dir=+1 省略 key → 零回歸；僅 dir=−1 寫入。）"""
    d = ((_PRICE_CS_META.get(cf) or {}).get("param_space", {}).get("dir", {}) or {}).get("default", 1)
    layer = {"factor": cf, "top_k": _CS_DEFAULT_K}
    if d == -1:
        layer["dir"] = -1
    return layer


def _auto_chip_cs_layer(cf: str) -> dict:
    """籌碼族 add_cs 候選層工廠：套 _CHIP_CS_META 的 default dir。
    既有 inst_net_buy/trust_net_buy/short_squeeze default dir=+1 → dir 省略（_normalize_layers 零回歸）；
    TW-SBL sbl_short default dir=−1（informed-bear）→ 寫入 dir=−1（forward 仍可翻 +1 試擠空）。
    鏡像 _auto_price_cs_layer 的 META-default-dir 套用模式。"""
    d = ((_CHIP_CS_META.get(cf) or {}).get("param_space", {}).get("dir", {}) or {}).get("default", 1)
    layer = {"factor": cf, "top_k": _CS_DEFAULT_K}
    if d == -1:
        layer["dir"] = -1
    return layer


def _auto_tdcc_cs_layer(cf: str) -> dict:
    """集保大戶結構 add_cs 候選層工廠：套 _TDCC_CS_META 的 default dir（holder_concentration/
    holder_conc_delta 皆 +1=集中度高/吸籌看多 → dir 省略，_normalize_layers 零回歸）。
    鏡像 _auto_chip_cs_layer 的 META-default-dir 套用模式（forward 仍可翻 −1）。"""
    d = ((_TDCC_CS_META.get(cf) or {}).get("param_space", {}).get("dir", {}) or {}).get("default", 1)
    layer = {"factor": cf, "top_k": _CS_DEFAULT_K}
    if d == -1:
        layer["dir"] = -1
    return layer


def _auto_search_candidates(current: list,
                            factor_pool=_AUTO_FACTOR_POOL,
                            topk_pool=_AUTO_TOPK_POOL,
                            chip_pool=_AUTO_CHIP_CS_POOL,
                            rev_pool=_AUTO_REVENUE_CS_POOL,
                            price_pool=_AUTO_PRICE_CS_POOL,
                            us_chip_pool=_AUTO_US_CHIP_CS_POOL,
                            tdcc_pool=_AUTO_TDCC_CS_POOL) -> list:
    """前向貪婪：列舉「下一步」候選（確定性、有界）。回 [(label, layers)]。
    可插拔層 moves：①加 PEAD ②加 chip(法人連買 streak) ③加籌碼 cs 層(A4∧inst_net_buy，R-PROD-7a)
    ③b 加月營收 cs 層(A4∧revenue_mom，基本面正交) ④換 cs 因子(resid_mom/rel_str/…) ⑤換 top_k∈{.10,.20,.30}。
    【給 Reviewer — 零回歸】③為 P0-1、③b 為 P0-2 新增（讓 Track A 自動列舉 A4∧籌碼/基本面 正交複合）；
    ④⑤僅在『恰有單一 cs 層』時觸發 → 保 _auto_replace_cs（替換全部 cs 層）語義正確、且種子(單 cs)行為與舊版
    逐字一致（種子仍吐 add:pead/add:chip/swap_factor×N/top_k×N，新增 add_cs×(chip+revenue) 為加法）。"""
    has_pead = any(L.get("type") == "pead" for L in current)
    has_chip = any(L.get("type") == "chip" for L in current)
    cs_layers = [L for L in current if L.get("factor") in _COMPOSITE_CS_FACTORS]
    present_factors = {L["factor"] for L in cs_layers}
    cands = []
    if not has_pead:                                   # ① 加事件層（PEAD 財報正驚奇）
        cands.append(("add:pead", current + [{"type": "pead"}]))
    if not has_chip:                                   # ② 加籌碼 streak 層（法人連買 type:chip）
        cands.append(("add:chip", current + [{"type": "chip", "window": 3, "min_net": 0.0}]))
    for cf in chip_pool:                               # ③ 加籌碼 cs 層（k 層 cs∧cs：A4∧籌碼正交複合）
        if cf not in present_factors:
            cands.append((f"add_cs:{cf}", current + [_auto_chip_cs_layer(cf)]))
    for cf in rev_pool:                                # ③b 加月營收 cs 層（基本面正交：A4∧revenue_mom）
        if cf not in present_factors:
            cands.append((f"add_cs:{cf}", current + [{"factor": cf, "top_k": _CS_DEFAULT_K}]))
    for cf in us_chip_pool:                            # ③b2 加美股籌碼 cs 層（Type 2 US-SI：A4_US∧short_interest，US-gated）
        if cf not in present_factors:
            cands.append((f"add_cs:{cf}", current + [{"factor": cf, "top_k": _CS_DEFAULT_K}]))
    for cf in tdcc_pool:                               # ③b3 加集保大戶結構 cs 層（D1：A4∧holder_concentration，TW-gated，補 level/結構正交）
        if cf not in present_factors:
            cands.append((f"add_cs:{cf}", current + [_auto_tdcc_cs_layer(cf)]))
    for cf in price_pool:                              # ③c 加價格族 cs 層（R-PROD-7h：style/macro/reversal 跨族）
        if cf not in present_factors:
            cands.append((f"add_cs:{cf}", current + [_auto_price_cs_layer(cf)]))
    if len(cs_layers) == 1:                            # 換因子/換 top_k 僅單 cs 層時（保替換語義+零回歸）
        cs = cs_layers[0]
        cur_factor = cs.get("factor")
        cur_k = float(cs.get("top_k", _CS_DEFAULT_K))
        for f in factor_pool:                          # ④ 換 cs 因子族
            if f != cur_factor:
                cands.append((f"swap_factor:{f}", _auto_replace_cs(current, factor=f)))
        for k in topk_pool:                            # ⑤ 換 top_k
            if abs(float(k) - cur_k) > 1e-9:
                cands.append((f"top_k:{k}", _auto_replace_cs(current, top_k=k)))
    return cands


def _auto_prune_reason(current: list, cand: list, max_depth: int = _AUTO_MAX_DEPTH):
    """三層剪枝。回剪枝理由(str)或 None(保留)。
    硬剪：①depth>max_depth ②層內帶 twii 基準（twii 結構性永不進候選空間）。
    lessons：含 _AUTO_LESSONS_SKIP_FACTORS 的 cs 層。
    軟剪冗餘（§2.4 parsimony）：同『家族』雙 cs 層 = 換湯不換藥 → 剪；跨家族（chip∧price）為補
    正交維度的目的 → 保留。
    【給 Reviewer — 零回歸】既有候選皆單一 cs 層 → 家族數 ≤1，永不觸發新軟剪。"""
    if len(cand) > max_depth:
        return f"hard-prune:depth>{max_depth}"
    for L in cand:
        if L.get("factor") in _AUTO_LESSONS_SKIP_FACTORS:
            return f"lessons-skip:{L.get('factor')}"
        if str(L.get("benchmark", "")).lower() == "twii":
            return "hard-prune:twii-benchmark"
    fams = [_cs_family(L["factor"]) for L in cand if L.get("factor") in _COMPOSITE_CS_FACTORS]
    if len(fams) != len(set(fams)):
        return "soft-prune:same-family-cs-redundant"
    return None


def _auto_phi_g1(res: dict, regime: str):
    """從 eval 結果取目標 Φ（該 regime 超額 CI 下界）與 G1 是否過（status==ALPHA）。
    指定 regime 但無切片/無交易 → (None, False)（誠實：該 regime 無法推進）。
    regime 為 'ALL'/None → 用全期 row。"""
    per = res.get("per_regime") or {}
    if regime and regime != "ALL":
        row = per.get(regime)
        if not row:
            return None, False
    else:
        row = res.get("row") or {}
    ci = row.get("excess_ci95") or [None, None]
    return ci[0], (row.get("status") == "ALPHA")


def _auto_greedy_round_decision(base_phi, cand_results: list, min_improvement: float):
    """純函式：前向貪婪「一輪」決策（留改善層 / 剪退步層）。
    base_phi = 當前 frontier 的 Φ。cand_results = [{label,layers,phi,g1_pass,...}]。
    留下：g1_pass 且 phi 不為 None 且 (phi - base_phi) >= min_improvement。
    剪掉：未過 G1 或增益 < 門檻 或 phi/base_phi 無法量測。
    回 (best, kept, dropped)；best=增益最大的 kept（無則 None → 該輪無改善）。"""
    kept, dropped = [], []
    for c in cand_results:
        phi = c.get("phi")
        gain = (phi - base_phi) if (phi is not None and base_phi is not None) else None
        c2 = dict(c, gain=gain)
        if c.get("g1_pass") and gain is not None and gain >= min_improvement:
            kept.append(c2)
        else:
            dropped.append(c2)
    best = max(kept, key=lambda x: x["gain"]) if kept else None
    return best, kept, dropped


def _auto_should_stop(trials, max_trials, stale, stale_limit, stop_requested, depth, max_depth):
    """純函式：停止判斷（預算 / stale / 手動 / 深度 四保險）。回 (should_stop, reason)。"""
    if stop_requested:
        return True, "manual-stop"
    if trials >= max_trials:
        return True, "budget-exhausted"
    if stale >= stale_limit:
        return True, "stale-no-improvement"
    if depth >= max_depth:
        return True, "max-depth"
    return False, None


_AUTO_RESTART_UNION_POOL = (_AUTO_FACTOR_POOL + _AUTO_PRICE_CS_POOL
                            + _AUTO_CHIP_CS_POOL + _AUTO_REVENUE_CS_POOL + _AUTO_US_CHIP_CS_POOL)


def _auto_pick_restart_factor(used_factors, pool=_AUTO_RESTART_UNION_POOL):
    """換族 random-restart（R-PROD-7h-3）：從聯集池（價格+籌碼+營收+原 factor 池）抽尚未用過的 cs 因子，
    且偏好「尚未出現過的 family」（用 _cs_family；真跳出 A4/價格盆地，非近親）。耗盡 → None → 收斂停止。"""
    used_fams = {_cs_family(f) for f in used_factors}
    # 第一優先：未用過 + 其 family 也未出現過（跨族跳出）
    for f in pool:
        if f not in used_factors and _cs_family(f) not in used_fams:
            return f
    # 退而求其次：未用過的因子（同族不同因子）
    for f in pool:
        if f not in used_factors:
            return f
    return None


def _auto_family_diversity_sort(current: list, cands: list) -> list:
    """R-PROD-7h-3 family-diversity 排序：把「引入 current 尚未出現 family」的 add_cs 候選排最前
    （跨族 span 最大化）→ 在候選封頂(max_candidates_per_round)前優先保留跨族腿，不被同族擠掉。
    確定性穩定排序：(0=跨族新 family add_cs, 1=其他)；組內保原序。cands=[(label, layers)]。"""
    cur_fams = {_cs_family(L["factor"]) for L in current if L.get("factor") in _COMPOSITE_CS_FACTORS}

    def _key(item):
        label, layers = item
        if isinstance(label, str) and label.startswith("add_cs:"):
            cf = label.split(":", 1)[1]
            if cf in _COMPOSITE_CS_FACTORS and _cs_family(cf) not in cur_fams:
                return 0
        return 1
    return sorted(cands, key=_key)


def _auto_status_view(st: dict) -> dict:
    """小 JSON（零 token 給 cockpit/LLM 里程碑讀；不含逐筆 returns/pnl）。"""
    sota = st.get("sota") or {}
    return {
        "run_id": st.get("run_id"), "status": st.get("status"),
        "market": st.get("market"), "start": st.get("start"), "end": st.get("end"),
        "benchmark": st.get("benchmark"), "target_regime": st.get("target_regime"),
        "trials": st.get("trials"), "max_trials": st.get("max_trials"),
        "round": st.get("round"), "stale_rounds": st.get("stale_rounds"),
        "stale_limit": st.get("stale_limit"),
        "best_phi": st.get("best_phi"),
        "frontier": st.get("frontier"),
        "sota": {"layers": sota.get("layers"), "phi": sota.get("phi"), "g1": sota.get("g1"),
                 "cand_id": sota.get("cand_id"), "dsr": sota.get("dsr"), "pbo": sota.get("pbo")},
        "frontier_candidates": (st.get("frontier_candidates") or [])[:12],
        "used_factors": sorted(st.get("used_factors", [])),
        "current": st.get("current"), "stop_reason": st.get("stop_reason"),
        "started_at": st.get("started_at"), "updated_at": st.get("updated_at"),
        "completed_at": st.get("completed_at"),
    }


# ── 狀態存取器（thread-safe）────────────────────────────────────────────────
def _auto_touch(run_id, **kw):
    with _AUTO_SEARCH_LOCK:
        st = _AUTO_SEARCH_RUNS.get(run_id)
        if st is None:
            return
        st.update(kw)
        st["updated_at"] = datetime.now().isoformat()


def _auto_get_copy(run_id):
    with _AUTO_SEARCH_LOCK:
        st = _AUTO_SEARCH_RUNS.get(run_id)
        return dict(st) if st else None


def _auto_stop_requested(run_id) -> bool:
    with _AUTO_SEARCH_LOCK:
        st = _AUTO_SEARCH_RUNS.get(run_id)
        return bool(st and st.get("stop_requested"))


def _auto_universe_codes(market: str) -> list:
    """複用 get_universe 取碼表（與單發 search 同邏輯）。"""
    uni = get_universe(market)
    if isinstance(uni, JSONResponse):
        raise RuntimeError(f"universe not available for {market}")
    raw = uni.get("data", uni.get("stocks", []))
    codes = [s if isinstance(s, str) else s.get("code", "") for s in raw]
    codes = [c for c in codes if c]
    if not codes:
        raise RuntimeError(f"empty universe for {market}")
    return codes


# ── 持久化 + DSR + PBO（純複用既有原語；單發 worker 不動，故獨立此函式，零回歸）──
def _persist_composite_eval(res, market, start, end, benchmark, layers, parent_id=None):
    """落地一筆試驗 → composite_candidates，並算 DSR（對全 T 折減）+ PBO（CSCV，本批）。
    與 _composite_search_worker 同邏輯（複用 _deflated_sharpe_ratio/_pbo_cscv/_bin_returns_to_blocks），
    但不觸碰既有 worker（純加法）。回 {cand_id,g1_status,dsr,pbo,trial_T,sharpe,excess_ci_low}。"""
    import json
    row = res["row"]
    trades = res["trades"]
    returns = [t[2] for t in trades]
    pnl_blocks = _bin_returns_to_blocks(trades, start, end, 16)
    sharpe = row.get("sharpe") or 0.0
    excess_ci = row.get("excess_ci95") or [None, None]
    cand_id = secrets.token_hex(8)

    con = db(); cur = con.cursor()
    cur.execute("""INSERT INTO composite_candidates
        (cand_id,created_at,parent_id,market,start,end,benchmark,layers_json,depth,
         n,win_rate,avg_return,excess_avg,excess_ci_low,excess_ci_high,sharpe,status,
         dsr,pbo,trial_T,per_regime_json,returns_json,pnl_blocks_json,notes)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
        cand_id, datetime.now().isoformat(), parent_id, market, start, end, benchmark,
        json.dumps(layers, ensure_ascii=False), len(layers),
        row.get("N"), row.get("win_rate"), row.get("avg_return"), row.get("excess_avg"),
        excess_ci[0], excess_ci[1], sharpe, row.get("status"),
        None, None, None,
        json.dumps(res.get("per_regime", {}), ensure_ascii=False),
        json.dumps(returns), json.dumps(pnl_blocks),
        f"auto-search layers={res.get('layer_kinds')} stocks={res.get('n_stocks')}"))
    con.commit()

    # B.2 S5 誠實 T：同 _composite_search_worker——T = 該候選所屬搜尋(同配置)累計試驗數，
    #   trial_sharpes 同步 scope（不混入無關全宇宙）。_deflated_sharpe_ratio 數學不變。
    T, all_sharpes, _honest_src = _honest_trial_T(cur, market, start, end, benchmark)
    dsr_res = _deflated_sharpe_ratio(returns, all_sharpes, T)

    pbo_res = {"pbo": None, "reason": "need >=2 candidates"}
    batch = cur.execute(
        "SELECT pnl_blocks_json FROM composite_candidates WHERE market=? AND start=? AND end=?",
        (market, start, end)).fetchall()
    cols = []
    for (pj,) in batch:
        try:
            blk = json.loads(pj) if pj else None
        except Exception:
            blk = None
        if blk and len(blk) == 16:
            cols.append([(x if x is not None else float("nan")) for x in blk])
    if len(cols) >= 2:
        import numpy as np
        pbo_res = _pbo_cscv(np.array(cols, dtype=float).T, S=16)

    cur.execute("UPDATE composite_candidates SET dsr=?, pbo=?, trial_T=? WHERE cand_id=?",
                (dsr_res.get("dsr"), pbo_res.get("pbo"), T, cand_id))
    con.commit(); con.close()

    return {"cand_id": cand_id, "g1_status": row.get("status"),
            "dsr": dsr_res.get("dsr"), "pbo": pbo_res.get("pbo"), "trial_T": T,
            "honest_t_source": _honest_src,
            "sharpe": sharpe, "excess_ci_low": excess_ci[0]}


# ── 背景 thread daemon：前向貪婪迴圈（machine-time 持續跑，零 token）────────────
def _auto_search_worker(run_id):
    """前向貪婪自主搜尋：種子→列舉候選→eval→留改善剪退步→advance frontier→重複。
    護欄：①預算 max_trials ②stale 連續無改善 ③剪枝(lessons/twii/深度/dedup) ④候選每輪封頂
    ⑤手動 stop。每輪寫進度到 search-state（可查）。所有數值複用 _eval_composite_market/DSR/PBO。"""
    st0 = _auto_get_copy(run_id)
    if not st0:
        return
    market = st0["market"]; start = st0["start"]; end = st0["end"]
    benchmark = st0["benchmark"]; target_regime = st0["target_regime"]
    max_trials = st0["max_trials"]; stale_limit = st0["stale_limit"]
    max_cands = st0["max_candidates_per_round"]; min_improvement = st0["min_improvement"]
    enable_restart = st0["enable_restart"]
    try:
        _ensure_composite_search_tables()
        _auto_touch(run_id, status="running", current="loading universe + bar data")
        codes = _auto_universe_codes(market)
        preloaded = _load_backtest_data(codes, market, start, end)
        regime_map = {}
        try:
            regime_map = {r["date"]: r["regime"] for r in _batch_calc_regime(market, start, end)}
        except Exception as e:
            print(f"[auto-search] regime map failed: {e}", flush=True)

        def _eval(layers):
            res = _eval_composite_market(market, codes, start, end, layers, preloaded,
                                         benchmark=benchmark, regime_map=regime_map,
                                         should_cancel=lambda: _auto_stop_requested(run_id))
            persisted = _persist_composite_eval(res, market, start, end, benchmark, layers)
            phi, g1 = _auto_phi_g1(res, target_regime)
            blocks = _bin_returns_to_blocks(res.get("trades", []), start, end, 16)  # 7h-2 報酬-corr 軟剪基質
            return res, persisted, phi, g1, blocks

        # 本地計數（worker 私有，避免每筆鎖；里程碑同步進 state）
        trials = 0
        stale = 0
        rnd = 0
        evaluated = set()
        history = []
        cs_seed = next((L.get("factor") for L in st0["seed_layers"]
                        if L.get("factor") in _COMPOSITE_CS_FACTORS), None)
        used_factors = {cs_seed} if cs_seed else set()

        # ── 基準：先 eval 種子，建立 best_phi 起點 ──
        current = [dict(L) for L in st0["seed_layers"]]
        res0, p0, phi0, g10, frontier_blocks = _eval(current)   # frontier_blocks：7h-2 報酬-corr 軟剪比對基準
        trials += 1
        evaluated.add(_layers_canon(current))
        best_phi = phi0
        sota = {"layers": current, "phi": phi0, "g1": g10, "cand_id": p0["cand_id"],
                "dsr": p0["dsr"], "pbo": p0["pbo"]}
        history.append({"trial": trials, "label": "seed", "layers": current,
                        "phi": phi0, "g1": g10, "cand_id": p0["cand_id"]})
        _auto_touch(run_id, trials=trials, round=rnd, stale_rounds=stale,
                    best_phi=best_phi, frontier=current, current=current, sota=sota,
                    used_factors=list(used_factors), history=history[-50:])

        # ── 前向貪婪主迴圈 ──
        while True:
            depth = len(current)
            stop, reason = _auto_should_stop(trials, max_trials, stale, stale_limit,
                                             _auto_stop_requested(run_id), depth, _AUTO_MAX_DEPTH)
            if stop:
                _auto_touch(run_id, stop_reason=reason)
                break

            rnd += 1
            # 列舉 → 剪枝（lessons/twii/深度）→ dedup → family-diversity 排序 → 候選封頂（有界）
            raw_cands = _auto_search_candidates(current)
            cands = []
            for lbl, lay in raw_cands:
                if _layers_canon(lay) in evaluated:
                    continue
                pr = _auto_prune_reason(current, lay)
                if pr is not None:
                    continue
                cands.append((lbl, lay))
            cands = _auto_family_diversity_sort(current, cands)   # 7h-3：跨族 add_cs 排前，封頂前優先保留
            cands = cands[:max_cands]
            _auto_touch(run_id, round=rnd, current=current,
                        current_msg=f"round {rnd}: {len(cands)} candidates")

            if not cands:
                # 本層無新候選 → 視為無改善（走 stale / restart 分支）
                cand_results = []
            else:
                cand_results = []
                for lbl, lay in cands:
                    if _auto_stop_requested(run_id) or trials >= max_trials:
                        break
                    res, p, phi, g1, blk = _eval(lay)
                    trials += 1
                    evaluated.add(_layers_canon(lay))
                    # 7h-2 報酬-corr 軟剪：add_cs 候選 pnl_blocks 對 frontier corr≥0.85 = 換湯不換藥 → 剪
                    rc = _ft_block_pearson(blk, frontier_blocks) if str(lbl).startswith("add_cs:") else None
                    if rc is not None and rc >= _AUTO_REPORT_CORR_PRUNE:
                        history.append({"trial": trials, "label": lbl, "layers": lay, "phi": phi,
                                        "g1": g1, "cand_id": p["cand_id"],
                                        "pruned": f"report-corr {round(rc,3)}≥{_AUTO_REPORT_CORR_PRUNE}"})
                        _auto_touch(run_id, trials=trials, history=history[-50:],
                                    current_msg=f"round {rnd}: soft-prune {lbl} (報酬-corr {round(rc,3)})")
                        continue
                    cr = {"label": lbl, "layers": lay, "phi": phi, "g1_pass": g1,
                          "cand_id": p["cand_id"], "dsr": p["dsr"], "pbo": p["pbo"], "blocks": blk}
                    cand_results.append(cr)
                    history.append({"trial": trials, "label": lbl, "layers": lay,
                                    "phi": phi, "g1": g1, "cand_id": p["cand_id"]})
                    _auto_touch(run_id, trials=trials,
                                frontier_candidates=[{k: v for k, v in c.items() if k != "blocks"}
                                                     for c in cand_results],
                                history=history[-50:],
                                current_msg=f"round {rnd}: eval {lbl} → Φ={phi} G1={g1}")

            # ── 一輪決策（純函式）：留改善層 / 剪退步層 ──
            _fc_view = [{k: v for k, v in c.items() if k != "blocks"} for c in cand_results]
            best, kept, dropped = _auto_greedy_round_decision(best_phi, cand_results, min_improvement)
            if best is None:
                stale += 1
                _auto_touch(run_id, stale_rounds=stale, frontier_candidates=_fc_view)
                # stale 達上限 → 換族 random-restart（換因子族）或停止
                if stale >= stale_limit and enable_restart:
                    rf = _auto_pick_restart_factor(used_factors)
                    if rf is not None:
                        current = [{"factor": rf, "top_k": _CS_DEFAULT_K}]
                        used_factors.add(rf)
                        frontier_blocks = None      # 新族 frontier 尚未評估 → 暫停報酬-corr 軟剪至重建
                        stale = 0
                        _auto_touch(run_id, frontier=current, current=current,
                                    used_factors=list(used_factors), stale_rounds=stale,
                                    current_msg=f"random-restart → family {rf}")
                        continue
                # 無可換族 → 下一輪 _auto_should_stop 會以 stale 收斂停止
                continue
            else:
                # 貪婪：吃下最佳增益層，advance frontier
                current = best["layers"]
                best_phi = best["phi"]
                frontier_blocks = best.get("blocks")   # 7h-2：frontier 前進 → 更新報酬-corr 比對基準
                stale = 0
                cf = next((L.get("factor") for L in current
                           if L.get("factor") in _COMPOSITE_CS_FACTORS), None)
                if cf:
                    used_factors.add(cf)
                sota = {"layers": current, "phi": best_phi, "g1": True,
                        "cand_id": best.get("cand_id"), "dsr": best.get("dsr"),
                        "pbo": best.get("pbo")}
                _auto_touch(run_id, frontier=current, current=current, best_phi=best_phi,
                            stale_rounds=stale, sota=sota, used_factors=list(used_factors),
                            current_msg=f"advance frontier → Φ={best_phi} (+{best.get('gain')})")

        final = "stopped" if _auto_stop_requested(run_id) else "done"
        _auto_touch(run_id, status=final, completed_at=datetime.now().isoformat(),
                    current="", current_msg="finished")
        snap = _auto_get_copy(run_id) or {}
        print(f"[auto-search] {run_id} {final}: T={snap.get('trials')} "
              f"bestΦ={snap.get('best_phi')} reason={snap.get('stop_reason')} "
              f"SOTA={(snap.get('sota') or {}).get('layers')}", flush=True)
    except Exception as e:
        import traceback; traceback.print_exc()
        _auto_touch(run_id, status="error", error=str(e),
                    completed_at=datetime.now().isoformat())


@app.post("/api/search/auto/start")
def start_auto_search(config: dict, _: None = Depends(require_token)):
    """Track A：啟動自主前向貪婪搜尋 daemon（machine-time，零 token）。回 run_id。
    body(全可選)：{seed:[layers], budget|max_trials, markets|market, regimes|regime,
                   benchmark, start, end, stale_rounds, max_candidates_per_round,
                   min_improvement, enable_restart}。"""
    cfg = config or {}
    # 市場
    market = cfg.get("market") or (cfg.get("markets") or ["TW"])
    if isinstance(market, list):
        market = market[0] if market else "TW"
    market = str(market).upper()
    # regime（MVP 單一目標 regime）
    regime = cfg.get("regime") or cfg.get("target_regime") or cfg.get("regimes") or "TREND_UP"
    if isinstance(regime, list):
        regime = regime[0] if regime else "TREND_UP"
    target_regime = str(regime).upper()
    # 日期
    start = str(cfg.get("start", "2020-01-01"))
    end = str(cfg.get("end", datetime.now().strftime("%Y-%m-%d")))
    try:
        sdt = datetime.strptime(start, "%Y-%m-%d"); edt = datetime.strptime(end, "%Y-%m-%d")
    except (TypeError, ValueError):
        return JSONResponse({"ok": False, "error": "start/end 須為 YYYY-MM-DD 格式"}, status_code=400)
    if sdt > edt:
        return JSONResponse({"ok": False, "error": "start 不可晚於 end"}, status_code=400)
    # 基準（twii 結構性排除：硬剪規則 §2.4）
    benchmark = str(cfg.get("benchmark", "equal_weight")).lower()
    if benchmark == "twii" or benchmark not in _BENCHMARK_WHITELIST:
        benchmark = "equal_weight"
    # 種子 layers（預設 A4）
    seed_raw = cfg.get("seed") or _AUTO_SEED_LAYERS
    seed_layers, err = _normalize_layers(seed_raw)
    if err:
        return JSONResponse({"ok": False, "error": f"seed 不合法：{err}"}, status_code=400)
    # 護欄參數（clamp 防呆）
    def _clamp_int(v, lo, hi, default):
        try:
            return min(max(int(v), lo), hi)
        except (TypeError, ValueError):
            return default
    max_trials = _clamp_int(cfg.get("budget", cfg.get("max_trials", 200)), 1, 2000, 200)
    stale_limit = _clamp_int(cfg.get("stale_rounds", 3), 1, 50, 3)
    max_cands = _clamp_int(cfg.get("max_candidates_per_round", 12), 1, 50, 12)
    try:
        min_improvement = float(cfg.get("min_improvement", 0.10))
    except (TypeError, ValueError):
        min_improvement = 0.10
    enable_restart = bool(cfg.get("enable_restart", True))

    # run store 上限防呆：淘汰最舊的已結束 run
    with _AUTO_SEARCH_LOCK:
        if len(_AUTO_SEARCH_RUNS) >= 50:
            finished = [(rid, r.get("completed_at") or r.get("started_at") or "")
                        for rid, r in _AUTO_SEARCH_RUNS.items()
                        if r.get("status") in ("done", "error", "stopped")]
            for rid, _ts in sorted(finished, key=lambda x: x[1])[:max(1, len(finished) // 2)]:
                _AUTO_SEARCH_RUNS.pop(rid, None)
        run_id = secrets.token_hex(8)
        _AUTO_SEARCH_RUNS[run_id] = {
            "run_id": run_id, "status": "starting",
            "market": market, "start": start, "end": end, "benchmark": benchmark,
            "target_regime": target_regime,
            "seed_layers": seed_layers, "frontier": seed_layers, "current": seed_layers,
            "sota": None, "best_phi": None,
            "trials": 0, "round": 0, "stale_rounds": 0,
            "max_trials": max_trials, "stale_limit": stale_limit,
            "max_candidates_per_round": max_cands, "min_improvement": min_improvement,
            "enable_restart": enable_restart,
            "stop_requested": False, "stop_reason": None, "error": None,
            "frontier_candidates": [], "history": [], "used_factors": [],
            "current_msg": "", "started_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(), "completed_at": None,
        }
    threading.Thread(target=_auto_search_worker, args=(run_id,), daemon=True).start()
    return {"ok": True, "run_id": run_id, "market": market, "target_regime": target_regime,
            "seed": seed_layers, "max_trials": max_trials, "stale_limit": stale_limit,
            "max_candidates_per_round": max_cands, "min_improvement": min_improvement,
            "message": f"Auto forward-greedy search started. "
                       f"Poll GET /api/search/auto/status?run_id={run_id}, stop via POST /api/search/auto/stop."}


@app.get("/api/search/auto/status")
def get_auto_search_status(run_id: str = ""):
    """里程碑零 token 監看：帶 run_id → 該 run 小 JSON（frontier/SOTA/T/bestΦ/前緣候選/是否還在跑）；
    不帶 → 列出全部 run 摘要。"""
    if run_id:
        if (not isinstance(run_id, str) or not (8 <= len(run_id) <= 64)
                or any(c not in "0123456789abcdef" for c in run_id)):
            return JSONResponse({"ok": False, "error": "run not found"}, status_code=404)
        st = _auto_get_copy(run_id)
        if not st:
            return JSONResponse({"ok": False, "error": "run not found"}, status_code=404)
        return {"ok": True, "run": _auto_status_view(st)}
    with _AUTO_SEARCH_LOCK:
        runs = [{"run_id": r.get("run_id"), "status": r.get("status"),
                 "market": r.get("market"), "target_regime": r.get("target_regime"),
                 "trials": r.get("trials"), "max_trials": r.get("max_trials"),
                 "best_phi": r.get("best_phi"),
                 "sota_layers": (r.get("sota") or {}).get("layers"),
                 "started_at": r.get("started_at"), "completed_at": r.get("completed_at")}
                for r in _AUTO_SEARCH_RUNS.values()]
    runs.sort(key=lambda x: x.get("started_at") or "", reverse=True)
    return {"ok": True, "count": len(runs), "runs": runs}


@app.post("/api/search/auto/stop")
def stop_auto_search(config: dict, _: None = Depends(require_token)):
    """停止一個自主搜尋 run（協作式：worker 下一檢查點收手；不可無限燒）。"""
    run_id = (config or {}).get("run_id", "")
    if (not isinstance(run_id, str) or not (8 <= len(run_id) <= 64)
            or any(c not in "0123456789abcdef" for c in run_id)):
        return JSONResponse({"ok": False, "error": "run not found"}, status_code=404)
    with _AUTO_SEARCH_LOCK:
        st = _AUTO_SEARCH_RUNS.get(run_id)
        if not st:
            return JSONResponse({"ok": False, "error": "run not found"}, status_code=404)
        st["stop_requested"] = True
        st["updated_at"] = datetime.now().isoformat()
        status = st["status"]
    return {"ok": True, "run_id": run_id, "status": status,
            "message": "stop requested; worker will halt at next checkpoint."}


# ══════════════════════════════════════════════════
# Universe 清單 API
# ══════════════════════════════════════════════════

_UNIVERSE_CACHE: dict = {}
_UNIVERSE_CACHE_TS: float = 0

@app.get("/api/universe/{market}")
def get_universe(market: str, refresh: bool = False):
    global _UNIVERSE_CACHE, _UNIVERSE_CACHE_TS
    cache_key = market.upper()
    if not refresh and cache_key in _UNIVERSE_CACHE and time.time() - _UNIVERSE_CACHE_TS < 86400:
        return _UNIVERSE_CACHE[cache_key]

    if market.upper() == "TW":
        result = _fetch_tw_universe()
    elif market.upper() == "US":
        result = _fetch_us_universe()
    else:
        return JSONResponse({"error": f"不支援市場: {market}"}, status_code=400)

    _UNIVERSE_CACHE[cache_key] = result
    _UNIVERSE_CACHE_TS = time.time()
    return result

def _fetch_tw_universe() -> dict:
    """台股 Top 200：用 yfinance 抓 TWSE 大型股"""
    try:
        import yfinance as yf
        # 台股主要大型股代碼（市值排序 Top 200 常見）
        # 先用已知的主要成分股 + 動態擴充
        tw_major = [
            "2330","2317","2454","2881","2891","2882","2886","2884","2892","3711",
            "2303","2308","2382","2412","1301","1303","1326","2002","1101","1216",
            "2880","2883","2885","2887","2890","5880","5876","2801","2834","2823",
            "3008","2357","2379","3034","2395","2408","3231","6505","2327","3037",
            "2345","2301","2344","2383","2356","4904","3045","2912","9910","1402",
            "2207","2201","3443","6669","4938","2474","3661","2049","1590","2542",
            "5871","2609","2615","1605","2603","2618","9945","2105","1504","3702",
            "2347","6446","2324","3706","2352","5347","8069","2377","4958","6239",
            "6271","2353","2492","3532","6415","2376","6456","3529","3533","6531",
            "2727","2633","2637","5269","3044","3711","6770","3036","2312","5483",
            "2368","1477","4919","8046","2354","6547","3035","2458","1476","6592",
            "2449","3042","6442","1560","8299","5274","3017","6285","3563","2206",
            "1102","1210","1229","1314","1434","1440","1513","1536","1589","1722",
            "1802","2014","2015","2020","2027","2059","2101","2103","2106","2204",
            "2227","2313","2314","2328","2338","2342","2355","2360","2362","2371",
            "2373","2374","2375","2385","2392","2404","2409","2448","2451","2457",
            "2504","2511","2545","2548","2597","2606","2610","2634","2642","2707",
            "2809","2812","2816","2820","2836","2838","2845","2849","2855","2867",
            "3006","3019","3023","3029","3030","3041","3058","3189","3376","3406",
            "3481","3515","3528","3588","3665","3682","3714","4108","4137","4142",
        ]
        # 去重
        codes = list(dict.fromkeys(tw_major))[:200]
        return {"market": "TW", "count": len(codes), "data": codes, "source": "curated_top200", "updated": datetime.now().isoformat()}
    except Exception as e:
        return {"market": "TW", "count": 0, "data": [], "error": str(e)}

def _fetch_us_universe() -> dict:
    """美股 S&P500 + NQ100 去重"""
    try:
        import yfinance as yf
        # S&P 500 + Nasdaq 100 主要成分股
        sp500_core = [
            "AAPL","MSFT","AMZN","NVDA","GOOGL","GOOG","META","BRK-B","UNH","XOM",
            "JNJ","JPM","V","PG","MA","HD","CVX","MRK","ABBV","LLY",
            "PEP","KO","COST","AVGO","MCD","WMT","TMO","CSCO","ACN","ABT",
            "DHR","CRM","ADBE","TXN","NEE","AMD","NFLX","BMY","UPS","PM",
            "RTX","INTC","QCOM","INTU","AMAT","LOW","HON","UNP","DE","GS",
            "CAT","BLK","SYK","ISRG","ELV","ADP","MDLZ","AMGN","LMT","GILD",
            "ADI","REGN","BKNG","VRTX","MMC","CB","PLD","CI","SCHW","MO",
            "ZTS","PYPL","TMUS","DUK","SO","SHW","BSX","PGR","CME","ICE",
            "CL","BDX","NOC","MCK","EQIX","ITW","AON","CSX","EMR","WM",
            "FDX","GM","F","FCX","KLAC","MRVL","SNPS","CDNS","LRCX","PANW",
            "CRWD","DDOG","SNOW","ZS","OKTA","PLTR","NET","MDB","COIN","ABNB",
            "UBER","LYFT","SQ","SHOP","SE","MELI","NU","GRAB","TTD","ROKU",
            "PINS","SNAP","DASH","RBLX","U","RIVN","LCID","NIO","XPEV","LI",
            "TSM","ASML","ARM","SMCI","MU","LSCC","ON","MCHP","NXPI","SWKS",
            "TER","MPWR","ENTG","WOLF","ACLS","CRUS","SLAB","DIOD","RMBS","MTSI",
            "DELL","HPQ","HPE","WDC","STX","NTAP","PSTG","PURE","ZM","TWLO",
            "WDAY","VEEV","HUBS","DOCU","FIVN","GTLB","CFLT","ESTC","PATH","AI",
            "IONQ","RGTI","QUBT","ARQQ","QBTS","IRM","DLR","AMT","CCI","SBAC",
            "PSA","EXR","SPG","O","VICI","WPC","NNN","STAG","FR","COLD",
            "BAC","WFC","C","MS","AXP","USB","PNC","TFC","COF","BK",
            "STT","KEY","FITB","HBAN","RF","CFG","ALLY","SYF","DFS","NDAQ",
            "CMG","SBUX","YUM","DPZ","QSR","WEN","JACK","SHAK","WING","CAVA",
            "LULU","NKE","TJX","ROST","GPS","ANF","AEO","URBN","BURL","FIVE",
            "DIS","WBD","PARA","CMCSA","CHTR","FOXA","NWSA","LYV","IMAX","EDR",
            "PFE","MRNA","AZN","GSK","NVS","SNY","TAK","BNTX","REGN","BIIB",
            "BA","LMT","NOC","GD","RTX","HII","TDG","HWM","SPR","ERJ",
            "TSLA","GM","F","RIVN","LCID","LI","NIO","XPEV","PSNY","VFS",
            "COP","EOG","PXD","DVN","FANG","OXY","MPC","VLO","PSX","HES",
        ]
        codes = list(dict.fromkeys(sp500_core))
        return {"market": "US", "count": len(codes), "data": codes, "source": "sp500_nq100_curated", "updated": datetime.now().isoformat()}
    except Exception as e:
        return {"market": "US", "count": 0, "data": [], "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════════
# 前向模擬器 Phase 1 — live-forward 紙上實盤（多策略，每日循環）
# ──────────────────────────────────────────────────────────────────────────────
# 目的：把研發成果（過閘策略）變成每日循環 → 每日選股 → 模擬買進 → 累積真實 OOS 戰績。
#       前向不可能 in-sample 造假，直接驗證/打掉 PBO 過擬合疑慮（憲章 §一‧五）。
#
# 【🔑選股與回測「同源」(核心，給 Reviewer)】選股「逐字複用」回測引擎
#   _eval_composite_market 的同一批原語（單一真實來源，無另寫一套）：
#     _compute_stock_features  — 個股特徵/outcome 表（與回測同函式）
#     _cs_rank_pct             — 橫斷面因子百分位排名；內含 _compute_cs_factors /
#                                _compute_sector_factors / _compute_chip_cs_factors /
#                                _compute_revenue_cs_factors（與回測完全同一批因子函式）
#     _cs_layer_eligible       — 每 cs 層 top/bottom K% 可進場集合（同函式、同門檻語意）
#     _a5_pead_triggers / _chip_streak_dates — pead/chip 層（同函式）
#     set.intersection         — AND-stack 交集（與回測同一行邏輯）
#   差別僅：回測 _emit_composite_trades emit 整段交易（含 open_until 去重疊倉），
#   live 只問「pick_date 是否 ∈ eligible」——這與回測「某一交易日是否進場」是完全同一判定
#   （open_until 只關乎回測不重複堆疊倉位，與『今日這檔是否入選』無關）。
#   故 live 選股 ≡ backtest 選股（同因子值、同排名、同 K% 門檻、同 AND 交集）。
#
# 【🔑PIT 防洩漏】
#   (1) 選股只用 ≤ pick_date 的資料（pick_date = 「最新一個有足量橫斷面資料的交易日 ≤ 請求日」）。
#       因子值在 pick_date 只用該股 ≤pick_date 的收盤；籌碼/月營收因子本身已 PIT 對齊次一交易日 /
#       announce_date（見 _chip_factor_series / _revenue_factor_series docstring）。
#   (2) 進場 = pick_date 之「次一交易日」開盤（entry_date/entry_price 次日）。次日 bar 尚未到時
#       status='pending'，待 /api/forward/update 取得次日 bar 後才回填 entry → 對「未來」零洩漏，
#       OOS 純度更強（下單前進場 bar 根本還不存在）。
#
# 全加法：ft_strategies / ft_picks / ft_runs 為全新表，新 endpoint，零碰既有（零回歸）。
# ══════════════════════════════════════════════════════════════════════════════
_FT_DEFAULT_HORIZON     = 20
_FT_DEFAULT_REGIME_GATE = "TREND_UP"
_FT_WARMUP_DAYS         = 540   # 選股資料載入回看天數（足供 resid_mom reg120+acc60 / sector_rel N60 暖身）

# ── R-FWD-8 auto-roll ROLL 閘 config（單一真實來源；門檻寫死於 code，可由 endpoint body 覆寫）──
# ★sbl-forward-entry-decision(2026-06-26) category-error 修正：ROLL=「進紙上觀察」低門檻閘，
#   不該套畢業級的尺。DSR≥0.60（優於拋硬幣 0.50 底線）即可滾入紙上；DSR>0.90 是「畢業真錢」
#   級檢定（R-FWD-9 GRADUATION 前置，與 live N≥20 + walk-forward + 多 regime 同層），放在 ROLL =
#   category error（違反 operating model「ROLL 低門檻、GRADUATION 嚴」不對稱）。前瞻鎖定·對所有
#   未來候選一體適用·非為 sbl 客製。
_FT_ROLL_DSR_MIN       = 0.60    # E2：DSR≥0.60（ROLL 紙上入場底線；>0.90 已移至 GRADUATION 前置，見 _FT_GRADUATION_DSR_MIN）
_FT_GRADUATION_DSR_MIN = 0.90    # ★R-FWD-9 畢業關前置（尚未建關，stub）：DSR>0.90 屬「畢業真錢」級，非 ROLL 入場閘
_FT_DEDUP_CORR_MAX     = 0.70    # ★E4：16 塊 pnl 向量 max Pearson < 0.70 才滾入（老闆 #1 去重；唯一硬約束）
_FT_ROLL_CRISIS_FLOOR  = -5.0    # E3：CRISIS 超額 < 此 = 「危機翻大負」；Track A rel_str∧rev 危機 −2.9 非大負
_FT_ROLL_SCAN_LIMIT    = 300     # 每次 auto-roll 掃描 composite_candidates 上限（最新優先）
_FT_REGIME_KEYS        = ("TREND_UP", "MEAN_REVERT", "RISK_OFF", "CRISIS")


def _ensure_ft_tables():
    """冪等建表：ft_strategies（策略註冊表）/ ft_picks（每日選股）/ ft_runs（每次跑/跳過稽核）。"""
    con = db(); cur = con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS ft_strategies(
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        name         TEXT NOT NULL UNIQUE,
        market       TEXT NOT NULL DEFAULT 'TW',
        layers_json  TEXT NOT NULL,
        regime_gate  TEXT DEFAULT 'TREND_UP',
        vix_gate     TEXT,
        horizon_days INTEGER DEFAULT 20,
        benchmark    TEXT DEFAULT 'equal_weight',
        status       TEXT DEFAULT 'active',
        source       TEXT DEFAULT 'manual',
        created_at   TEXT
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS ft_picks(
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy_id     INTEGER NOT NULL,
        pick_date       TEXT NOT NULL,
        run_date        TEXT,
        ticker          TEXT NOT NULL,
        name            TEXT,
        rank_json       TEXT,
        ref_price       REAL,
        regime          TEXT,
        vix_bucket      TEXT,
        horizon_days    INTEGER,
        entry_date      TEXT,
        entry_price     REAL,
        mark_date       TEXT,
        mark_price      REAL,
        mark_return     REAL,
        mark_excess     REAL,
        exit_date       TEXT,
        exit_price      REAL,
        realized_return REAL,
        realized_excess REAL,
        status          TEXT DEFAULT 'pending',
        created_at      TEXT,
        updated_at      TEXT,
        UNIQUE(strategy_id, pick_date, ticker)
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS ft_runs(
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy_id  INTEGER NOT NULL,
        run_date     TEXT,
        pick_date    TEXT,
        regime       TEXT,
        vix_bucket   TEXT,
        n_picks      INTEGER,
        skipped      INTEGER DEFAULT 0,
        skip_reason  TEXT,
        created_at   TEXT,
        UNIQUE(strategy_id, pick_date)
    )""")
    # R-FWD-8b 冪等遷移：補 auto-roll 來源欄（16 塊向量 + roll 時的 G1/DSR 指標，作 E4 去重 corr 基質 +
    # 取代支配比較 + 稽核。ADD COLUMN 為 metadata-only，既有 INSERT/UPDATE 不列此欄故零回歸。）
    # sbl-forward-entry-decision(2026-06-26)：provenance（區隔 PROVISIONAL_BORDERLINE 與一般 active，
    #   供監控密度區分）+ notes（誠實標 E4 raw-pnl 例外/手動置入緣由）。ADD COLUMN metadata-only → 零回歸。
    for col, typ in (("pnl_blocks_json", "TEXT"), ("roll_cand_id", "TEXT"),
                     ("roll_dsr", "REAL"), ("roll_excess_ci_low", "REAL"),
                     ("roll_pbo", "REAL"), ("roll_regime", "TEXT"),
                     ("provenance", "TEXT"), ("notes", "TEXT")):
        try:
            cur.execute(f"ALTER TABLE ft_strategies ADD COLUMN {col} {typ}")
        except Exception:
            pass
    # R-LABEL（2026-06-27）：補六標籤欄 + 保守落初始標籤（active→conditional-ALPHA 受 regime_gate
    #   條件約束；retired→parked-unproven）。冪等、零回歸（只寫新欄、WHERE label IS NULL）。
    _rlabel_migrate(cur, "ft_strategies")
    _rlabel_backfill(cur, "ft_strategies", "id", "status", regime_col="regime_gate")
    con.commit(); con.close()


# ── R-FWD-8c 去重 corr（16 塊 pnl 向量逐對 Pearson；純函式，無 DB，可單元隔離測）──────────
def _ft_block_pearson(a: list, b: list):
    """兩個 16 塊 pnl 向量的 Pearson（None/NaN 塊成對丟棄）。配對 < 3 或任一邊零變異 → None。
    與 Track A 的 pnl_blocks corr 同源同算（PBO/CSCV 同基質）。"""
    if not a or not b:
        return None
    pts = []
    for x, y in zip(a, b):
        if x is None or y is None:
            continue
        try:
            xf = float(x); yf = float(y)
        except (TypeError, ValueError):
            continue
        if xf != xf or yf != yf:   # NaN
            continue
        pts.append((xf, yf))
    if len(pts) < 3:
        return None
    n = len(pts)
    mx = sum(p[0] for p in pts) / n
    my = sum(p[1] for p in pts) / n
    num = sum((p[0] - mx) * (p[1] - my) for p in pts)
    dx = sum((p[0] - mx) ** 2 for p in pts) ** 0.5
    dy = sum((p[1] - my) ** 2 for p in pts) ** 0.5
    if dx == 0 or dy == 0:
        return None
    return num / (dx * dy)


def _ft_max_corr_to_roster(blocks: list, roster: list):
    """blocks（16 塊）對 roster 每支策略的 16 塊逐對 Pearson → 回 (max_signed_corr, peer_sid)。
    roster = [{"sid","name","blocks",...}]；空/全不可算 → (None, None)。用 signed max：強負相關＝分散(好)。"""
    best_corr = None; best_sid = None
    for peer in roster:
        c = _ft_block_pearson(blocks, peer.get("blocks"))
        if c is None:
            continue
        if best_corr is None or c > best_corr:
            best_corr = c; best_sid = peer.get("sid")
    return best_corr, best_sid


def _layers_factor_set(layers: list) -> frozenset:
    """layers 的 factor/type 集合（忽略 top_k/dir；用於把 ft 策略 ↔ composite_candidate 對齊取 blocks）。"""
    out = []
    for L in (layers or []):
        if L.get("type"):
            out.append(L["type"])
        elif L.get("factor"):
            out.append(L["factor"])
    return frozenset(out)


def _ft_match_candidate(layers, market, cur):
    """用 factor-set 把 forward 策略 ↔ composite_candidate 對齊（同 market，取 excess_ci_low 最高且有 16 塊者）。
    回 {blocks, dsr, excess_ci_low} 或 None。讓 manual 策略也能拿到 blocks/DSR/ci 參與 E4 去重 + 取代支配，
    否則既有 5 支 manual 永無 corr/支配指標可比。
    ★F2 單窗對齊假設：match 鍵僅 factor-set + market（忽略 start/end）→ 隱含「對齊的 candidate 與被對齊策略
    的 16 塊來自同一回測窗」。同一 daemon run 用固定窗時成立；若 DB 混入多窗候選，回填的 blocks 可能跨窗錯位，
    使後續 E4 corr 失準。此為已知假設，非 bug——daemon 固定窗下安全。"""
    want = _layers_factor_set(layers)
    if not want:
        return None
    best = None; best_ci = None
    rows = cur.execute(
        "SELECT layers_json,pnl_blocks_json,excess_ci_low,dsr FROM composite_candidates WHERE market=?",
        (market,)).fetchall()
    for lj, pj, ci, dsr in rows:
        if not pj:
            continue
        try:
            if _layers_factor_set(json.loads(lj)) != want:
                continue
            blk = json.loads(pj)
        except Exception:
            continue
        if not blk or len(blk) != 16:
            continue
        civ = ci if ci is not None else -9e9
        if best is None or civ > best_ci:
            best = {"blocks": blk, "dsr": dsr, "excess_ci_low": ci}; best_ci = civ
    return best


def _ft_strategy_blocks(sid, layers, market, cur):
    """取某 forward 策略的 16 塊 pnl 向量（E4 去重基質）：① 優先 ft_strategies.pnl_blocks_json
    （auto-roll 落地的，§8b）；② 無則 factor-set 對齊 composite_candidate 回填。"""
    row = cur.execute("SELECT pnl_blocks_json FROM ft_strategies WHERE id=?", (sid,)).fetchone()
    if row and row[0]:
        try:
            blk = json.loads(row[0])
            if blk and len(blk) == 16:
                return blk
        except Exception:
            pass
    m = _ft_match_candidate(layers, market, cur)
    return m["blocks"] if m else None


def _ft_load_roster(cur, exclude_sid=None, market=None):
    """載入 active forward roster（含每支 16 塊 blocks，供 E4 去重 / 取代支配 / 8d 正交矩陣）。
    每支帶 roll_dsr / roll_excess_ci_low：auto 用落地欄；manual（欄為 None）回填自對齊 candidate（取代支配可比）。
    ★F1 跨市場隔離：market=None 載全市場；指定 market 則只載該市場 active。E4 去重/取代支配的 16 塊
    Pearson 隱含「同 (start,end) 對齊」前提——跨市場（甚至跨 daemon 窗）block i 對應不同日曆區間 →
    跨市場 corr 是錯位比較、語義無效，且 replace 路徑會誤退役他市場策略。故 auto-roll 須傳當前滾入候選
    的 market、forward_track 正交矩陣按 market 分組各自算。"""
    rows = cur.execute(
        "SELECT id,name,market,layers_json,roll_dsr,roll_excess_ci_low,pnl_blocks_json FROM ft_strategies "
        "WHERE status='active' AND (? IS NULL OR market=?)", (market, market)).fetchall()
    roster = []
    for sid, name, mkt, lj, rdsr, rci, pbj in rows:
        if exclude_sid is not None and sid == exclude_sid:
            continue
        try:
            layers = json.loads(lj)
        except Exception:
            layers = []
        blocks = None
        if pbj:
            try:
                b = json.loads(pbj)
                if b and len(b) == 16:
                    blocks = b
            except Exception:
                pass
        # blocks 或 roll 指標缺 → 回填自對齊 candidate（manual 補齊參與去重/支配）
        if blocks is None or rdsr is None or rci is None:
            m = _ft_match_candidate(layers, mkt, cur)
            if m:
                if blocks is None:
                    blocks = m["blocks"]
                if rdsr is None:
                    rdsr = m["dsr"]
                if rci is None:
                    rci = m["excess_ci_low"]
        roster.append({"sid": sid, "name": name, "market": mkt, "layers": layers,
                       "blocks": blocks, "roll_dsr": rdsr, "roll_excess_ci_low": rci})
    return roster


def _ft_eval_roll_gates(cand: dict, regime: str, roster: list, cfg: dict) -> dict:
    """套 §1.2 ROLL 閘 E1–E5 於單一 daemon 候選。純判讀（無 DB）。
    cand = {layers, status(全窗 G1), dsr, pbo, per_regime(dict), blocks(16)}。
    regime = 目標 regime（E1/E3 切片）。回 {decision, e1..e5, max_corr, peer_sid, dominates_sid, reasons}。
    decision ∈ {roll, reject_collinear, replace, reject_gate}。E5 PBO 僅記錄不否決（Track A 鐵證）。"""
    dsr_min   = cfg.get("dsr_min", _FT_ROLL_DSR_MIN)
    corr_max  = cfg.get("dedup_threshold", _FT_DEDUP_CORR_MAX)
    crisis_fl = cfg.get("crisis_floor", _FT_ROLL_CRISIS_FLOOR)
    enable_replace = cfg.get("enable_replace", False)
    per = cand.get("per_regime") or {}
    # ★F4：regime != ALL → 取該 regime 切片；ALL → 用全窗指標（by_set 帶 status + excess_avg），
    #   而非引用不存在的 cand["row"]（會使 target={} → E1=None → 恆 reject_gate 的靜默壞分支）。
    if regime and regime != "ALL":
        target = per.get(regime) or {}
    else:
        target = {"status": cand.get("status"), "excess_avg": cand.get("excess_avg")}
    reasons = []

    # E1 viable edge：目標 regime status==ALPHA（ci_lower>0，全窗）
    e1 = bool(target and target.get("status") == "ALPHA")
    if not e1:
        reasons.append(f"E1: 目標 regime {regime} status={target.get('status') if target else 'NA'}≠ALPHA")

    # E2 robustness：DSR≥0.60（ROLL 紙上入場底線，優於拋硬幣 0.50）。
    # ★DSR>0.90（_FT_GRADUATION_DSR_MIN）屬 GRADUATION（R-FWD-9）前置檢查，非 ROLL 入場閘——
    #   在此卡 0.90 是 category error（sbl-forward-entry-decision 2026-06-26 修正）。ROLL = E1 ALPHA
    #   ∧ E4 去重<0.70 ∧ E2 DSR≥0.60；DSR>0.90 + live N≥20 + walk-forward + 多 regime 留待畢業關。
    dsr = cand.get("dsr")
    e2 = (dsr is not None and dsr >= dsr_min)
    if not e2:
        reasons.append(f"E2: DSR={dsr}<{dsr_min}（ROLL 底線；>0.90 為畢業前置非入場閘）")

    # E3 regime 不崩：目標 regime 超額≥0；且(4 regime ≥2 正 或 CRISIS 不為大負)
    tgt_exc = (target or {}).get("excess_avg")
    n_pos = 0
    for rk in _FT_REGIME_KEYS:
        rr = per.get(rk) or {}
        if rr.get("excess_avg") is not None and rr.get("excess_avg") >= 0:
            n_pos += 1
    crisis = per.get("CRISIS") or {}
    crisis_exc = crisis.get("excess_avg")
    crisis_ok = (crisis_exc is None) or (crisis_exc > crisis_fl)
    e3 = ((tgt_exc is not None and tgt_exc >= 0) and (n_pos >= 2 or crisis_ok))
    if not e3:
        reasons.append(f"E3: 目標超額={tgt_exc} n_pos={n_pos}/4 crisis={crisis_exc}")

    # ★E4 去重/正交（唯一硬約束）：max signed Pearson < corr_max
    blocks = cand.get("blocks")
    max_corr, peer_sid = _ft_max_corr_to_roster(blocks, roster) if blocks else (None, None)
    e4 = (max_corr is None) or (max_corr < corr_max)
    dominates_sid = None
    if not e4:
        # E4 不過 → 看是否「嚴格支配」被撞的弱者（全窗 excess_ci_lower 且 dsr 皆更高）→ 取代而非拒滾
        peer = next((p for p in roster if p.get("sid") == peer_sid), None)
        if peer is not None:
            p_ci = peer.get("roll_excess_ci_low"); p_dsr = peer.get("roll_dsr")
            n_ci = cand.get("excess_ci_low")
            if (p_ci is not None and p_dsr is not None and n_ci is not None and dsr is not None
                    and n_ci > p_ci and dsr > p_dsr):
                dominates_sid = peer_sid
        # ⚠ 取代為破壞性（退役既有策略）+ spec 要求 holdout 優越（R-FWD-9 才有）→ 預設 enable_replace=False：
        #   只記「可取代」不執行，避免擅自退役 manual 策略；要啟用須顯式帶 enable_replace=True。
        will_replace = bool(dominates_sid is not None and enable_replace)
        if dominates_sid is not None and not enable_replace:
            reasons.append(f"E4: max_corr={round(max_corr,3)}≥{corr_max} (peer sid={peer_sid}) → "
                           f"全窗指標嚴格支配 sid={dominates_sid}，但 enable_replace=False（holdout 未驗）→ 拒滾(不取代)")
        else:
            reasons.append(f"E4: max_corr={round(max_corr,3)}≥{corr_max} (peer sid={peer_sid})"
                           + (f" → 嚴格支配 sid={dominates_sid}，取代" if will_replace else " → 共線雙胞胎拒滾"))
        if not will_replace:
            dominates_sid = None

    # E5 PBO：僅記錄，不否決
    pbo = cand.get("pbo")

    if not (e1 and e2 and e3):
        decision = "reject_gate"
    elif e4:
        decision = "roll"
    elif dominates_sid is not None:
        decision = "replace"
    else:
        decision = "reject_collinear"

    return {"decision": decision, "e1": e1, "e2": e2, "e3": e3, "e4": e4,
            "dsr": dsr, "pbo": pbo, "excess_ci_low": cand.get("excess_ci_low"),
            "target_excess": tgt_exc, "n_pos_regimes": n_pos, "crisis_excess": crisis_exc,
            "max_corr": (round(max_corr, 4) if max_corr is not None else None),
            "peer_sid": peer_sid, "dominates_sid": dominates_sid, "reasons": reasons}


def _ft_eval_graduation_gates(cand: dict) -> dict:
    """★R-FWD-9 GRADUATION（紙上→真錢）閘 — STUB（尚未建關）。
    sbl-forward-entry-decision(2026-06-26)：DSR>0.90 是「畢業真錢」級檢定，從 ROLL E2 移來此層。
    畢業前置（全部須過，不得在此前轉真錢）：
      ① DSR > _FT_GRADUATION_DSR_MIN(0.90)（隨資料加深升過）
      ② live N≥20 closed picks（真 OOS 戰績，非樣本內）
      ③ walk-forward 穩定（多窗 OOS 一致）
      ④ regime 廣度：不准只憑單一 TREND_UP 畢業（現 sbl 僅 2.5 年單一 regime）
      ⑤ 全史 corr 定讞（去重存活、非市場 beta 假象）
    TODO(R-FWD-9)：實作上述五關 + holdout 優越驗證後才開放 enable_replace / 真錢轉換。"""
    return {"implemented": False, "graduation_dsr_min": _FT_GRADUATION_DSR_MIN,
            "todo": "R-FWD-9 未建：DSR>0.90 + live N≥20 + walk-forward + 多 regime + 全史 corr"}


def _ft_universe_codes(market: str) -> list:
    """複用 get_universe 取碼表（與回測/搜尋引擎同一 universe → 同源）。"""
    uni = get_universe(market)
    if isinstance(uni, JSONResponse):
        return []
    raw = uni.get("data", uni.get("stocks", []))
    codes = [(s if isinstance(s, str) else s.get("code", "")) for s in raw]
    return [c for c in codes if c]


def _ft_resolve_asof(all_dates: list, codes: list, bar_data: dict, requested: str) -> str:
    """as-of 解析（PIT）：最新一個 ≤ requested 且有 ≥ _CS_MIN_XSECTION 檔 universe 報價的交易日。
    （資料落後請求日時，自動退回最近一個有橫斷面的收盤日 → live 在最近一收盤後選股。）"""
    for d in reversed(all_dates):
        if d > requested:
            continue
        n = 0
        for c in codes:
            if (c, d) in bar_data:
                n += 1
                if n >= _CS_MIN_XSECTION:
                    return d
    return None


def _ft_resolve_entry(code: str, pick_date: str, all_dates: list, bar_data: dict):
    """PIT 次一交易日進場：第一個 > pick_date 且該股有 bar 的交易日 → (entry_date, entry_open)。
    次日 bar 尚未到（today 之後）→ (None, None) → pending，待 update 回填。"""
    for d in all_dates:
        if d > pick_date and (code, d) in bar_data:
            b = bar_data[(code, d)]
            op = b.get("open") or b.get("close")
            return d, (float(op) if op else None)
    return None, None


def _ft_vix_bucket(asof: str) -> str:
    """as-of 當日 VIX 情緒桶（複用 _vix_sentiment_map 同門檻；缺值回 None）。"""
    try:
        return _vix_sentiment_map(asof, asof, "fixed").get("map", {}).get(asof)
    except Exception:
        return None


def _ft_compute_picks(mkt: str, codes: list, layers: list, pick_date: str, preloaded: tuple, bench_map: dict):
    """🔑 與回測同源的當日選股：逐字鏡像 _eval_composite_market 的進場 pass（同原語、同 AND 交集），
    但只回「pick_date 入選的個股」而非 emit 整段交易。回 (picks, n_ranked)。
    picks = [{code, ref_price(=pick_date close), rank{factor:pct}}]。"""
    all_dates, bar_data = preloaded
    cs_layers = []; pead_layer = chip_layer = None
    for L in layers:                                  # 解析層（與 _eval_composite_market 同）
        if L.get("type") == "pead":
            pead_layer = L
        elif L.get("type") == "chip":
            chip_layer = L
        elif L.get("factor") in _COMPOSITE_CS_FACTORS:
            cs_layers.append(L)
    feats = {}
    for code in codes:                                # 同回測 context pass
        f = _compute_stock_features(code, all_dates, bar_data)
        if f:
            feats[code] = f
    cs_needed = {L["factor"] for L in cs_layers}
    rank_pct = _cs_rank_pct(mkt, codes, feats, cs_needed, bench_map) if cs_needed else {}
    chip_elig = _chip_streak_dates(codes, chip_layer.get("window", 3),
                                   chip_layer.get("min_net", 0.0)) if chip_layer else {}
    earnings = _get_earnings_map(mkt, codes) if pead_layer else {}
    picks = []
    for code in codes:                                # 進場 pass（逐字鏡像回測，只查 pick_date）
        f = feats.get(code)
        if not f or (code, pick_date) not in bar_data:
            continue
        dts = f["dates"]
        layer_sets = []; rankinfo = {}
        for L in cs_layers:
            rmap = rank_pct.get(L["factor"], {}).get(code, {})
            layer_sets.append(_cs_layer_eligible(
                rmap, float(L.get("top_k", _CS_DEFAULT_K)), int(L.get("dir", 1))))
            rankinfo[L["factor"]] = (round(rmap[pick_date], 4) if pick_date in rmap else None)
        if pead_layer:
            trig = _a5_pead_triggers(dts, f["c"], bench_map, earnings.get(code, []))
            layer_sets.append({dts[i] for i in range(len(dts)) if trig[i]})
        if chip_layer:
            layer_sets.append(chip_elig.get(code, set()))
        if not layer_sets:
            continue
        eligible = set.intersection(*layer_sets) if len(layer_sets) > 1 else layer_sets[0]
        if pick_date in eligible:                     # 與回測「某日是否進場」同一判定
            picks.append({"code": code,
                          "ref_price": float(bar_data[(code, pick_date)]["close"]),
                          "rank": rankinfo})
    picks.sort(key=lambda p: p["code"])
    return picks, len(feats)


def _ft_record_run(sid, run_date, pick_date, regime, vbucket, n_picks, skipped, reason):
    """稽核：記錄某策略某 pick_date 的跑/跳過（冪等：同 strategy×pick_date 覆蓋）。"""
    con = db(); cur = con.cursor()
    cur.execute("""INSERT INTO ft_runs(strategy_id,run_date,pick_date,regime,vix_bucket,n_picks,skipped,skip_reason,created_at)
        VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(strategy_id,pick_date) DO UPDATE SET run_date=excluded.run_date,regime=excluded.regime,
            vix_bucket=excluded.vix_bucket,n_picks=excluded.n_picks,skipped=excluded.skipped,
            skip_reason=excluded.skip_reason,created_at=excluded.created_at""",
        (sid, run_date, pick_date, regime, vbucket, n_picks, skipped, reason, datetime.now().isoformat()))
    con.commit(); con.close()


def _ft_insert_picks(sid, pick_date, run_date, picks, regime, vbucket, horizon, all_dates, bar_data) -> int:
    """落地當日 picks（冪等：UNIQUE(strategy_id,pick_date,ticker) + INSERT OR IGNORE → 同日同策略不重複）。
    進場：PIT 次一交易日；次日 bar 已到 → entry_price+status='open'，否則 pending。"""
    con = db(); cur = con.cursor(); now = datetime.now().isoformat(); ins = 0
    for p in picks:
        code = p["code"]
        ed, ep = _ft_resolve_entry(code, pick_date, all_dates, bar_data)
        status = "open" if ep is not None else "pending"
        cur.execute("""INSERT OR IGNORE INTO ft_picks
            (strategy_id,pick_date,run_date,ticker,name,rank_json,ref_price,regime,vix_bucket,horizon_days,
             entry_date,entry_price,status,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (sid, pick_date, run_date, code, _get_stock_name(code),
             json.dumps(p["rank"], ensure_ascii=False), round(p["ref_price"], 2),
             regime, vbucket, horizon, ed, (round(ep, 2) if ep else None), status, now, now))
        ins += cur.rowcount
    con.commit(); con.close()
    return ins


@app.post("/api/forward/strategy")
def forward_register_strategy(config: dict, _: None = Depends(require_token)):
    """註冊/更新一個 live-forward 策略（冪等 by name）。
    body: {name, layers:[{factor,top_k,dir}|{type:'pead'}|{type:'chip',...}], market, regime_gate,
           vix_gate, horizon_days, benchmark, source}。layers 走 _normalize_layers（與回測同驗證）。"""
    _ensure_ft_tables()
    name = str(config.get("name", "")).strip()
    if not name:
        return JSONResponse({"ok": False, "error": "name 必填"}, status_code=400)
    layers, err = _normalize_layers(config.get("layers"))
    if err:
        return JSONResponse({"ok": False, "error": err}, status_code=400)
    market = str(config.get("market", "TW")).upper()
    regime_gate = config.get("regime_gate", _FT_DEFAULT_REGIME_GATE)
    if regime_gate in ("", None):
        regime_gate = None
    vix_gate = config.get("vix_gate") or None
    if vix_gate and vix_gate not in _VIX_SENTIMENT_LABELS:
        return JSONResponse({"ok": False, "error": f"vix_gate 須屬 {_VIX_SENTIMENT_LABELS} 或空"}, status_code=400)
    try:
        horizon = int(config.get("horizon_days", _FT_DEFAULT_HORIZON))
    except (TypeError, ValueError):
        horizon = _FT_DEFAULT_HORIZON
    benchmark = str(config.get("benchmark", "equal_weight")).lower()
    source = str(config.get("source", "manual"))
    # sbl-forward-entry-decision(2026-06-26)：可選 provenance/notes/roll_dsr（PROVISIONAL_BORDERLINE 入場用）。
    #   既有呼叫端不帶這些 key → 預設 None，INSERT/UPDATE 行為與舊版逐字等價（零回歸）。
    provenance = config.get("provenance") or None
    notes = config.get("notes") or None
    try:
        roll_dsr = float(config["roll_dsr"]) if config.get("roll_dsr") is not None else None
    except (TypeError, ValueError):
        roll_dsr = None
    con = db(); cur = con.cursor()
    cur.execute("""INSERT INTO ft_strategies(name,market,layers_json,regime_gate,vix_gate,horizon_days,benchmark,status,source,created_at,provenance,notes,roll_dsr)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(name) DO UPDATE SET layers_json=excluded.layers_json,regime_gate=excluded.regime_gate,
            vix_gate=excluded.vix_gate,horizon_days=excluded.horizon_days,benchmark=excluded.benchmark,
            market=excluded.market,source=excluded.source,status='active',
            provenance=COALESCE(excluded.provenance,ft_strategies.provenance),
            notes=COALESCE(excluded.notes,ft_strategies.notes),
            roll_dsr=COALESCE(excluded.roll_dsr,ft_strategies.roll_dsr)""",
        (name, market, json.dumps(layers, ensure_ascii=False), regime_gate, vix_gate, horizon,
         benchmark, "active", source, datetime.now().isoformat(), provenance, notes, roll_dsr))
    con.commit()
    sid = cur.execute("SELECT id FROM ft_strategies WHERE name=?", (name,)).fetchone()[0]
    con.close()
    return {"ok": True, "strategy_id": sid, "name": name, "layers": layers,
            "regime_gate": regime_gate, "vix_gate": vix_gate, "horizon_days": horizon, "market": market,
            "provenance": provenance, "roll_dsr": roll_dsr}


@app.get("/api/forward/strategies")
def forward_list_strategies():
    """列出已註冊策略。"""
    _ensure_ft_tables()
    con = db(); cur = con.cursor()
    rows = cur.execute("""SELECT id,name,market,layers_json,regime_gate,vix_gate,horizon_days,
        benchmark,status,source,created_at FROM ft_strategies ORDER BY id""").fetchall()
    con.close()
    cols = ["id", "name", "market", "layers_json", "regime_gate", "vix_gate", "horizon_days",
            "benchmark", "status", "source", "created_at"]
    out = []
    for r in rows:
        d = dict(zip(cols, r))
        try:
            d["layers"] = json.loads(d.pop("layers_json"))
        except Exception:
            d["layers"] = None; d.pop("layers_json", None)
        out.append(d)
    return {"ok": True, "count": len(out), "strategies": out}


@app.post("/api/reframe/preregister")
def reframe_preregister(body: dict = Body(...), _: None = Depends(require_token)):
    """R-COND-SLICE-A：落一筆事前 pre-registration（「先講 why 才准測」，B-S1/S2/S3）。
       body: {signal_id*, hypothesized_mechanism*, target_slices(JSON/list), predicted_sign(JSON/dict),
              author, notes, created_ts?(epoch；測試注入用)}。created_ts 不帶 → 現在 epoch。
       signal_id 與機制必填（沒有可命名機制 = 資料挖掘噪音，B-S1 直接拒）。"""
    sid = str(body.get("signal_id", "")).strip()
    mech = str(body.get("hypothesized_mechanism", "")).strip()
    if not sid:
        return JSONResponse({"ok": False, "error": "signal_id 必填"}, status_code=400)
    if not mech:
        return JSONResponse({"ok": False, "error": "hypothesized_mechanism 必填（沒有可命名機制=噪音，B-S1）"},
                            status_code=400)
    rec = _preregister(sid, mech, body.get("target_slices"), body.get("predicted_sign"),
                       body.get("author"), body.get("notes"), body.get("created_ts"))
    return {"ok": True, "preregistration": rec}


@app.get("/api/reframe/preregistrations")
def reframe_list_preregistrations(signal_id: str = None):
    """列出 pre-registrations（可選 signal_id 過濾），created_ts 新到舊。"""
    _ensure_preregistration_table()
    cols = ["id", "signal_id", "hypothesized_mechanism", "target_slices", "predicted_sign",
            "author", "created_ts", "created_iso", "notes"]
    sel = "SELECT " + ",".join(cols) + " FROM preregistration"
    con = db(); cur = con.cursor()
    if signal_id:
        rows = cur.execute(sel + " WHERE signal_id=? ORDER BY created_ts DESC", (signal_id,)).fetchall()
    else:
        rows = cur.execute(sel + " ORDER BY created_ts DESC").fetchall()
    con.close()
    out = []
    for r in rows:
        d = dict(zip(cols, r))
        for k in ("target_slices", "predicted_sign"):
            try:
                d[k] = json.loads(d[k]) if d[k] else None
            except Exception:
                pass   # 非 JSON（如手填字串）→ 原樣回傳
        out.append(d)
    return {"ok": True, "count": len(out), "preregistrations": out}


def _ft_auto_name(layers: list) -> str:
    """auto-roll 策略命名（冪等 by name 用；確定性、可讀）：auto:A4∧revenue_mom 形式。
    sector_rel_topsec → A4 別名（對齊既有 manual 命名習慣），其餘用 factor/type 原名，依字母排序。"""
    alias = {"sector_rel_topsec": "A4"}
    parts = []
    for L in (layers or []):
        if L.get("type"):
            parts.append(L["type"])
        elif L.get("factor"):
            f = L["factor"]; parts.append(alias.get(f, f))
    return "auto:" + "∧".join(sorted(parts))


@app.post("/api/forward/auto-roll")
def forward_auto_roll(config: dict = None, _: None = Depends(require_token)):
    """R-FWD-8a daemon→forward auto-roll 橋：掃 composite_candidates frontier → 套 §1.2 ROLL 閘
    E1–E5 → 過閘者以 source='auto'（冪等 by name）註冊進 ft_strategies，並存 16 塊 pnl_blocks（§8b，作
    E4 去重基質）。回 {rolled, rejected_collinear, replaced, rejected_gate} 稽核列。
    body(全可選)：{market='TW', regime='TREND_UP'(E1/E3 目標切片), dedup_threshold=0.70, dsr_min=0.90,
                   crisis_floor=-5.0, limit=300, dry_run=False, enable_replace=False}。
    enable_replace 預設 False：共線但全窗指標支配既有策略時「只記不取代」（取代屬破壞性 + spec 要求 holdout
    優越，holdout 屬 R-FWD-9）→ 要退役弱者換新須顯式帶 enable_replace=True。
    閘的不對稱：ROLL 低門檻（E1 全窗 ALPHA + E2 DSR>0.90 + E3 regime 不崩 + ★E4 去重<0.70；E5 PBO 僅記錄）。"""
    _ensure_ft_tables()
    cfg = config or {}
    market = str(cfg.get("market", "TW")).upper()
    regime = str(cfg.get("regime", _FT_DEFAULT_REGIME_GATE) or _FT_DEFAULT_REGIME_GATE).upper()
    dry_run = bool(cfg.get("dry_run", False))
    try:
        limit = min(max(int(cfg.get("limit", _FT_ROLL_SCAN_LIMIT)), 1), 2000)
    except (TypeError, ValueError):
        limit = _FT_ROLL_SCAN_LIMIT
    def _cfg_float(key, default):                # ★F5：body 傳非數字 → 回退預設，不 500（比照 limit 防護）
        try:
            return float(cfg.get(key, default))
        except (TypeError, ValueError):
            return default
    gate_cfg = {"dsr_min": _cfg_float("dsr_min", _FT_ROLL_DSR_MIN),
                "dedup_threshold": _cfg_float("dedup_threshold", _FT_DEDUP_CORR_MAX),
                "crisis_floor": _cfg_float("crisis_floor", _FT_ROLL_CRISIS_FLOOR),
                "enable_replace": bool(cfg.get("enable_replace", False))}

    con = db(); cur = con.cursor()
    rows = cur.execute(
        "SELECT cand_id,layers_json,status,excess_avg,dsr,pbo,excess_ci_low,per_regime_json,pnl_blocks_json,created_at "
        "FROM composite_candidates WHERE market=? ORDER BY created_at DESC LIMIT ?",
        (market, limit)).fetchall()

    # 同 factor-set 去冗：每個 factor-set 只取 excess_ci_low 最高且有 16 塊者（避免評估 19 筆 A4∧rev）。
    # ★F2：去冗鍵僅 factor-set（不含 start/end）→ 隱含「同一 daemon run 用固定回測窗」假設；
    #   若混入多窗候選，16 塊去重 corr 可能跨窗錯位。同 run 固定窗 → 通常 OK，此處明示假設。
    by_set = {}
    for cand_id, lj, status, exavg, dsr, pbo, ci, prj, pbj, _ts in rows:
        try:
            layers = json.loads(lj)
            blocks = json.loads(pbj) if pbj else None
            per = json.loads(prj) if prj else {}
        except Exception:
            continue
        if not blocks or len(blocks) != 16:
            continue
        fset = _layers_factor_set(layers)
        if not fset:
            continue
        prev = by_set.get(fset)
        civ = ci if ci is not None else -9e9
        if prev is None or civ > prev["_ci_sort"]:
            by_set[fset] = {"cand_id": cand_id, "layers": layers, "status": status,
                            "excess_avg": exavg, "dsr": dsr, "pbo": pbo, "excess_ci_low": ci,
                            "per_regime": per, "blocks": blocks, "_ci_sort": civ}
    cands = sorted(by_set.values(), key=lambda c: c["_ci_sort"], reverse=True)

    roster = _ft_load_roster(cur, market=market)   # ★F1 只載同 market roster：E4/取代支配僅對同市場算
    rolled, rejected_collinear, replaced, rejected_gate = [], [], [], []
    now = datetime.now().isoformat()
    dry_sid_seq = -1   # ★F3 dry_run roster 合成臨時 id（負數遞減）：同輪多支互相共線時不撞 sid=None → 支配 peer 查找正確、預覽分類與實滾一致

    for c in cands:
        name = _ft_auto_name(c["layers"])
        # 冪等：同名 active 已存在 → 跳過（避免重滾）。
        # ★F6 NIT（by-design，留標記）：同名 retired 者不在此 skip → 下方 ON CONFLICT DO UPDATE status='active'
        #   會「復活」之；理論上被取代/退役者日後仍可被滾回（churn）。當前 enable_replace 預設 False 不主動退役，
        #   風險低；若未來開啟取代且觀察到 churn，再於此補 retired 同名 skip。
        exist = cur.execute("SELECT id,status FROM ft_strategies WHERE name=?", (name,)).fetchone()
        if exist and exist[1] == "active":
            continue
        verdict = _ft_eval_roll_gates(c, regime, roster, gate_cfg)
        audit = {"name": name, "cand_id": c["cand_id"], "layers": c["layers"],
                 "dsr": verdict["dsr"], "pbo": verdict["pbo"], "excess_ci_low": verdict["excess_ci_low"],
                 "max_corr": verdict["max_corr"], "peer_sid": verdict["peer_sid"],
                 "target_excess": verdict["target_excess"], "n_pos_regimes": verdict["n_pos_regimes"],
                 "crisis_excess": verdict["crisis_excess"],
                 "gates": {"E1": verdict["e1"], "E2": verdict["e2"], "E3": verdict["e3"],
                           "E4": verdict["e4"], "E5_pbo_recorded_only": verdict["pbo"]},
                 "reasons": verdict["reasons"]}

        if verdict["decision"] == "reject_gate":
            rejected_gate.append(audit); continue
        if verdict["decision"] == "reject_collinear":
            rejected_collinear.append(audit); continue

        # roll 或 replace → 寫入（dry_run 只記稽核不寫，但仍模擬 roster 成長 → 預覽與實滾一致）
        replaced_sid = verdict["dominates_sid"]
        layers_norm, lerr = _normalize_layers(c["layers"])   # cs 層帶 top_k；type 層原樣
        if lerr:
            audit["reasons"].append(f"normalize 失敗：{lerr}")
            rejected_gate.append(audit); continue
        new_sid = None
        if not dry_run:
            if replaced_sid is not None:
                cur.execute("UPDATE ft_strategies SET status='retired' WHERE id=?", (replaced_sid,))
            cur.execute("""INSERT INTO ft_strategies
                (name,market,layers_json,regime_gate,vix_gate,horizon_days,benchmark,status,source,created_at,
                 pnl_blocks_json,roll_cand_id,roll_dsr,roll_excess_ci_low,roll_pbo,roll_regime)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(name) DO UPDATE SET layers_json=excluded.layers_json,status='active',
                    source='auto',pnl_blocks_json=excluded.pnl_blocks_json,roll_cand_id=excluded.roll_cand_id,
                    roll_dsr=excluded.roll_dsr,roll_excess_ci_low=excluded.roll_excess_ci_low,
                    roll_pbo=excluded.roll_pbo,roll_regime=excluded.roll_regime""",
                (name, market, json.dumps(layers_norm, ensure_ascii=False), regime, None,
                 _FT_DEFAULT_HORIZON, "equal_weight", "active", "auto", now,
                 json.dumps(c["blocks"]), c["cand_id"], c["dsr"], c["excess_ci_low"], c["pbo"], regime))
            con.commit()
            new_sid = cur.execute("SELECT id FROM ft_strategies WHERE name=?", (name,)).fetchone()[0]
            audit["strategy_id"] = new_sid
        else:
            new_sid = dry_sid_seq; dry_sid_seq -= 1   # ★F3 dry_run 合成臨時 id（負數，全程唯一）
        # in-memory roster 同步（dry/實滾皆做）：取代移除弱者、新支加入 → 同輪後續候選也對它去重
        if replaced_sid is not None:
            roster = [p for p in roster if p.get("sid") != replaced_sid]
        roster.append({"sid": new_sid, "name": name, "market": market, "layers": layers_norm,
                       "blocks": c["blocks"], "roll_dsr": c["dsr"], "roll_excess_ci_low": c["excess_ci_low"]})
        if replaced_sid is not None:
            audit["replaced_sid"] = replaced_sid
            replaced.append(audit)
        else:
            rolled.append(audit)

    con.close()
    return {"ok": True, "market": market, "regime": regime, "dry_run": dry_run,
            "config": gate_cfg, "scanned_factor_sets": len(cands),
            "rolled": rolled, "rejected_collinear": rejected_collinear,
            "replaced": replaced, "rejected_gate": rejected_gate,
            "summary": {"rolled": len(rolled), "rejected_collinear": len(rejected_collinear),
                        "replaced": len(replaced), "rejected_gate": len(rejected_gate)}}


@app.post("/api/forward/run")
def forward_run(date: str = "", strategy: int = 0, _: None = Depends(require_token)):
    """每日選股：對每個 active 策略——as-of 解析 → regime/vix gate（不符記 skipped）→
    與回測同源選股(_ft_compute_picks) → PIT 次日進場落地 ft_picks（冪等）。回各策略今日 picks。"""
    _ensure_ft_tables()
    requested = date or datetime.now().strftime("%Y-%m-%d")
    try:
        datetime.strptime(requested, "%Y-%m-%d")
    except (TypeError, ValueError):
        return JSONResponse({"ok": False, "error": "date 須 YYYY-MM-DD"}, status_code=400)
    con = db(); cur = con.cursor()
    q = ("SELECT id,name,market,layers_json,regime_gate,vix_gate,horizon_days,benchmark,status,source "
         "FROM ft_strategies WHERE status='active'")
    params = []
    if strategy:
        q += " AND id=?"; params = [strategy]
    strats = cur.execute(q, params).fetchall()
    con.close()
    if not strats:
        return {"ok": True, "requested_date": requested, "message": "no active strategies", "results": []}
    by_mkt = {}
    for s in strats:
        by_mkt.setdefault(s[2], []).append(s)
    results = []
    for mkt, slist in by_mkt.items():
        codes = _ft_universe_codes(mkt)
        if not codes:
            for s in slist:
                results.append({"strategy_id": s[0], "name": s[1], "skipped": True, "reason": "empty universe"})
            continue
        start = (datetime.strptime(requested, "%Y-%m-%d") - timedelta(days=_FT_WARMUP_DAYS)).strftime("%Y-%m-%d")
        preloaded = _load_backtest_data(codes, mkt, start, requested)
        all_dates, bar_data = preloaded
        bench_map = _get_benchmark_close_map(mkt, start, requested)
        asof = _ft_resolve_asof(all_dates, codes, bar_data, requested)
        if not asof:
            for s in slist:
                results.append({"strategy_id": s[0], "name": s[1], "skipped": True, "reason": "no data <= date"})
            continue
        reg = _calc_regime(mkt, asof).get("regime", "UNKNOWN")
        vbucket = _ft_vix_bucket(asof)
        for s in slist:
            sid, name, _m, layers_json, rgate, vgate, horizon, benchmark, status, source = s
            layers = json.loads(layers_json)
            if rgate and reg != rgate:
                _ft_record_run(sid, requested, asof, reg, vbucket, 0, 1, f"regime {reg}≠gate {rgate}")
                results.append({"strategy_id": sid, "name": name, "pick_date": asof, "regime": reg,
                                "skipped": True, "reason": f"regime {reg}≠{rgate}"})
                continue
            if vgate and vbucket != vgate:
                _ft_record_run(sid, requested, asof, reg, vbucket, 0, 1, f"vix {vbucket}≠gate {vgate}")
                results.append({"strategy_id": sid, "name": name, "pick_date": asof, "regime": reg,
                                "vix_bucket": vbucket, "skipped": True, "reason": f"vix {vbucket}≠{vgate}"})
                continue
            picks, n_ranked = _ft_compute_picks(mkt, codes, layers, asof, preloaded, bench_map)
            inserted = _ft_insert_picks(sid, asof, requested, picks, reg, vbucket, horizon, all_dates, bar_data)
            _ft_record_run(sid, requested, asof, reg, vbucket, len(picks), 0, None)
            results.append({"strategy_id": sid, "name": name, "pick_date": asof, "regime": reg,
                            "vix_bucket": vbucket, "n_picks": len(picks), "inserted": inserted,
                            "n_universe_ranked": n_ranked,
                            "picks": [{"ticker": p["code"], "name": _get_stock_name(p["code"]),
                                       "ref_price": round(p["ref_price"], 2), "rank": p["rank"]}
                                      for p in picks]})
    return {"ok": True, "requested_date": requested, "results": results}


@app.post("/api/forward/update")
def forward_update(_: None = Depends(require_token)):
    """每日結算：對所有 open/pending picks——pending 回填 PIT 次日進場；open 算至今報酬+對等權大盤
    超額(mark-to-market)；到 horizon_days 則結算 exit/realized/status=closed。超額 = 個股淨報酬 −
    等權 universe 同持有期報酬（複用 _equal_weight_bench_map，與回測 _summarize_trades 同口徑）。"""
    _ensure_ft_tables()
    con = db(); cur = con.cursor()
    open_rows = cur.execute("""SELECT id,strategy_id,pick_date,ticker,horizon_days,entry_date,entry_price,status
        FROM ft_picks WHERE status IN ('pending','open')""").fetchall()
    strat_mkt = {r[0]: r[1] for r in cur.execute("SELECT id,market FROM ft_strategies").fetchall()}
    con.close()
    if not open_rows:
        return {"ok": True, "entered": 0, "marked": 0, "settled": 0, "message": "no open/pending picks"}
    by_mkt = {}
    for r in open_rows:
        mkt = strat_mkt.get(r[1], "TW")
        by_mkt.setdefault(mkt, []).append(r)
    today = datetime.now().strftime("%Y-%m-%d")
    entered = marked = settled = 0
    for mkt, prs in by_mkt.items():
        codes = _ft_universe_codes(mkt)
        min_pick = min(p[2] for p in prs)
        start = (datetime.strptime(min_pick, "%Y-%m-%d") - timedelta(days=10)).strftime("%Y-%m-%d")
        preloaded = _load_backtest_data(codes, mkt, start, today)
        all_dates, bar_data = preloaded
        bench = _equal_weight_bench_map(codes, all_dates, bar_data)
        con = db(); cur = con.cursor(); now = datetime.now().isoformat()
        for p in prs:
            pid, sid, pick_date, ticker, horizon, entry_date, entry_price, status = p
            if entry_price is None:                         # pending → 回填 PIT 次日進場
                ed, ep = _ft_resolve_entry(ticker, pick_date, all_dates, bar_data)
                if ep is None:
                    continue                                # 次日 bar 仍未到 → 維持 pending
                entry_date, entry_price = ed, ep; entered += 1
                cur.execute("UPDATE ft_picks SET entry_date=?,entry_price=?,status='open',updated_at=? WHERE id=?",
                            (entry_date, round(entry_price, 2), now, pid))
            sdates = [d for d in all_dates if (ticker, d) in bar_data]   # 該股自身交易日序列
            if entry_date not in sdates:
                continue
            ei = sdates.index(entry_date)
            ti = ei + int(horizon or _FT_DEFAULT_HORIZON)
            if ti < len(sdates):                            # 到 horizon → 結算
                exit_date = sdates[ti]; exit_close = float(bar_data[(ticker, exit_date)]["close"])
                rret = _net_return_pct(entry_price, exit_close, mkt, TW_DISCOUNT)
                rexc = None; be, bx = bench.get(entry_date), bench.get(exit_date)
                if be and bx and be > 0:
                    rexc = round(rret - (bx / be - 1) * 100, 2)
                cur.execute("""UPDATE ft_picks SET exit_date=?,exit_price=?,realized_return=?,realized_excess=?,
                    mark_date=?,mark_price=?,mark_return=?,mark_excess=?,status='closed',updated_at=? WHERE id=?""",
                    (exit_date, round(exit_close, 2), round(rret, 2), rexc,
                     exit_date, round(exit_close, 2), round(rret, 2), rexc, now, pid))
                settled += 1
            else:                                           # 未到 horizon → mark-to-market 至最新
                mark_date = sdates[-1]; mark_close = float(bar_data[(ticker, mark_date)]["close"])
                mret = _net_return_pct(entry_price, mark_close, mkt, TW_DISCOUNT)
                mexc = None; be, bx = bench.get(entry_date), bench.get(mark_date)
                if be and bx and be > 0:
                    mexc = round(mret - (bx / be - 1) * 100, 2)
                cur.execute("""UPDATE ft_picks SET mark_date=?,mark_price=?,mark_return=?,mark_excess=?,updated_at=? WHERE id=?""",
                            (mark_date, round(mark_close, 2), round(mret, 2), mexc, now, pid))
                marked += 1
        con.commit(); con.close()
    return {"ok": True, "as_of": today, "entered": entered, "marked": marked, "settled": settled}


# ── sbl-forward-entry-decision(2026-06-26) 衰減閘 hook ────────────────────────────────────────
_FT_DECAY_MIN_CLOSED = 10        # 衰減閘最小觀測：closed picks ≥ 此數才評估 CI（不足 → 點估先頂著）

def _ft_excess_ci_lower(values: list, n_boot: int = 1000):
    """realized excess 的 95% bootstrap CI 下界（與 _summarize_trades excess_ci95 同源同算法）。
    純函式無 DB，可單元隔離測。N<5 → None（樣本不足，不下判斷）。"""
    n = len(values)
    if n < 5:
        return None
    import random
    boot = []
    for _ in range(n_boot):
        idx = [random.randint(0, n - 1) for __ in range(n)]
        boot.append(sum(values[k] for k in idx) / n)
    boot.sort()
    return round(boot[int(n_boot * 0.025)], 2)


def _ft_decay_flag(closed_excess: list):
    """PROVISIONAL_BORDERLINE 衰減閘：closed N≥_FT_DECAY_MIN_CLOSED 且 realized excess bootstrap CI
    下界<0 → 'KILL_CANDIDATE'（forward edge 消失，AIF 放行條件 2）。
    回 {decay_flag, ci_lower, n_closed, point_estimate}。只回 flag 給 cockpit 看，不自動 kill。
    N 不足 CI(<5) → 退回點估比較先頂著（TODO：N 達標前不具統計效力）。"""
    n = len(closed_excess)
    point = round(sum(closed_excess) / n, 2) if n else None
    if n < _FT_DECAY_MIN_CLOSED:
        return {"decay_flag": None, "ci_lower": None, "n_closed": n, "point_estimate": point,
                "note": f"closed N={n}<{_FT_DECAY_MIN_CLOSED} → 衰減閘未啟（觀測不足）"}
    ci_lo = _ft_excess_ci_lower(closed_excess)
    if ci_lo is None:                                   # N≥10 但 <5 不可能；防呆
        flag = "KILL_CANDIDATE" if (point is not None and point < 0) else None
        return {"decay_flag": flag, "ci_lower": None, "n_closed": n, "point_estimate": point,
                "note": "CI 不可得 → 點估頂著(TODO)"}
    flag = "KILL_CANDIDATE" if ci_lo < 0 else None
    return {"decay_flag": flag, "ci_lower": ci_lo, "n_closed": n, "point_estimate": point,
            "note": ("forward excess CI 下界<0 → edge 消失" if flag else "forward excess CI 下界≥0 → 存活")}


@app.get("/api/forward/track")
def forward_track():
    """戰績：每策略 + 整體 live-forward 記錄（picks 數、open/pending/closed、累積平均超額、勝率、
    起跑至今天數、最佳/最差）。closed 用 realized_excess；open 用 mark_excess（current standing）。"""
    _ensure_ft_tables()
    today = datetime.now().strftime("%Y-%m-%d")
    con = db(); cur = con.cursor()
    strs = cur.execute("""SELECT id,name,market,regime_gate,vix_gate,horizon_days,status,source,provenance,roll_dsr
        FROM ft_strategies ORDER BY id""").fetchall()
    out = []
    ov = {"picks": 0, "open": 0, "pending": 0, "closed": 0, "excess": [], "ret": []}
    for s in strs:
        sid = s[0]
        prows = cur.execute("""SELECT status,realized_return,realized_excess,mark_return,mark_excess,ticker,name
            FROM ft_picks WHERE strategy_id=?""", (sid,)).fetchall()
        n = len(prows)
        n_open = sum(1 for r in prows if r[0] == "open")
        n_pend = sum(1 for r in prows if r[0] == "pending")
        n_closed = sum(1 for r in prows if r[0] == "closed")
        closed_exc = [r[2] for r in prows if r[0] == "closed" and r[2] is not None]
        closed_ret = [r[1] for r in prows if r[0] == "closed" and r[1] is not None]
        live = [(r[2] if r[0] == "closed" else r[4]) for r in prows
                if r[0] in ("closed", "open") and (r[2] if r[0] == "closed" else r[4]) is not None]
        ranked = [((r[2] if r[0] == "closed" else r[4]), r[5], r[6], r[0]) for r in prows
                  if (r[2] if r[0] == "closed" else r[4]) is not None]
        best = max(ranked, key=lambda x: x[0]) if ranked else None
        worst = min(ranked, key=lambda x: x[0]) if ranked else None
        first = cur.execute("SELECT MIN(pick_date) FROM ft_picks WHERE strategy_id=?", (sid,)).fetchone()[0]
        days = ((datetime.strptime(today, "%Y-%m-%d") - datetime.strptime(first, "%Y-%m-%d")).days
                if first else 0)
        # sbl-forward-entry-decision(2026-06-26) 衰減閘 hook：僅 PROVISIONAL_BORDERLINE 算 decay_flag。
        provenance = s[8]
        decay = (_ft_decay_flag(closed_exc) if provenance == "provisional_borderline" else None)
        out.append({
            "strategy_id": sid, "name": s[1], "market": s[2], "regime_gate": s[3], "vix_gate": s[4],
            "horizon_days": s[5], "status": s[6], "source": s[7],
            "provenance": provenance, "roll_dsr": s[9], "decay": decay,
            "n_picks": n, "open": n_open, "pending": n_pend, "closed": n_closed,
            "closed_avg_excess": (round(sum(closed_exc) / len(closed_exc), 2) if closed_exc else None),
            "closed_avg_return": (round(sum(closed_ret) / len(closed_ret), 2) if closed_ret else None),
            "win_rate_pct": (round(sum(1 for e in closed_exc if e > 0) / len(closed_exc) * 100, 1) if closed_exc else None),
            "live_avg_excess": (round(sum(live) / len(live), 2) if live else None),
            "days_since_start": days, "start_date": first,
            "best": ({"excess": round(best[0], 2), "ticker": best[1], "name": best[2], "status": best[3]} if best else None),
            "worst": ({"excess": round(worst[0], 2), "ticker": worst[1], "name": worst[2], "status": worst[3]} if worst else None),
        })
        ov["picks"] += n; ov["open"] += n_open; ov["pending"] += n_pend; ov["closed"] += n_closed
        ov["excess"] += closed_exc; ov["ret"] += closed_ret

    # ── R-FWD-8d roster 正交稽核：每支 active 策略對其餘 roster 的 max 報酬-corr（16 塊 Pearson）──
    # ★F1：正交矩陣按 market 分組各自算——跨市場 16 塊對應不同日曆窗 → corr 錯位無效，only same-market。
    roster = _ft_load_roster(cur)              # 含 blocks（manual 經 factor-set 回填）；全市場載，下面按 market 過濾 peer
    by_sid = {p["sid"]: p for p in roster}
    n_pairs_over = 0
    for o in out:
        if o["status"] != "active":
            o["max_corr_to_roster"] = None; o["max_corr_peer"] = None; continue
        me = by_sid.get(o["strategy_id"])
        others = [p for p in roster if p["sid"] != o["strategy_id"] and p.get("market") == o["market"]]
        mc, peer = _ft_max_corr_to_roster(me.get("blocks") if me else None, others)
        o["max_corr_to_roster"] = (round(mc, 4) if mc is not None else None)
        o["max_corr_peer"] = (by_sid[peer]["name"] if peer in by_sid else None)
        if mc is not None and mc >= _FT_DEDUP_CORR_MAX:
            n_pairs_over += 1
        # sbl-forward-entry-decision(2026-06-26) 去重存活 hook：PROVISIONAL_BORDERLINE 多回對 short_squeeze
        #   的 max_corr（AIF 放行條件 3：forward 內 corr(sbl,squeeze)<0.70 持續；收斂則留較強者 sbl）。
        #   ⚠ 此為 raw-pnl corr（被市場 beta 主導、finding#1 已知量不出選股正交）→ 僅供觀察，非硬閘。
        if o.get("provenance") == "provisional_borderline":
            sq_peers = [p for p in others if "short_squeeze" in _layers_factor_set(p.get("layers"))]
            sq_mc, sq_peer = _ft_max_corr_to_roster(me.get("blocks") if me else None, sq_peers)
            o["corr_to_short_squeeze"] = (round(sq_mc, 4) if sq_mc is not None else None)
            o["short_squeeze_peer"] = (by_sid[sq_peer]["name"] if sq_peer in by_sid else None)
            o["corr_to_short_squeeze_note"] = ("raw-pnl corr 被市場 beta 主導(finding#1)→僅觀察；"
                                               "真去重看橫斷面因子 rank corr(-0.315) 非此值")
    con.close()
    overall = {
        "n_picks": ov["picks"], "open": ov["open"], "pending": ov["pending"], "closed": ov["closed"],
        "closed_avg_excess": (round(sum(ov["excess"]) / len(ov["excess"]), 2) if ov["excess"] else None),
        "closed_avg_return": (round(sum(ov["ret"]) / len(ov["ret"]), 2) if ov["ret"] else None),
        "win_rate_pct": (round(sum(1 for e in ov["excess"] if e > 0) / len(ov["excess"]) * 100, 1) if ov["excess"] else None),
    }
    orthogonality = {"dedup_threshold": _FT_DEDUP_CORR_MAX, "active_strategies": len(by_sid),
                     "strategies_over_threshold": n_pairs_over,
                     "note": "max_corr_to_roster = 該支 16 塊 pnl 向量對其餘 active roster 的最大 Pearson；"
                             "≥門檻 = 共線雙胞胎（E4 應已擋；既有 manual 可能殘留）"}
    # sbl-forward-entry-decision(2026-06-26) PROVISIONAL_BORDERLINE 監控總覽：
    #   ★方向性稀釋切片（AIF 放行條件 4，盯最大紅旗）= DEFER：借券含避險/做市/套利腿，須按
    #   「純方向放空 spike」vs「借券與避險/套利同升（期貨基差/ETF 申贖 proxy）」分層；edge 只集中
    #   套利重時段 → crowding proxy 非 informed-bear → kill。需新資料源（期貨基差/ETF 流）→ 無源硬做=造假，
    #   故本次只註記監控需求、不實作（誠實標 defer）。
    provisional_monitoring = {
        "decay_gate": {"min_closed": _FT_DECAY_MIN_CLOSED,
                       "rule": "closed N≥min 且 realized excess bootstrap CI 下界<0 → KILL_CANDIDATE（僅 flag，不自動 kill）"},
        "dedup_survival": "PROVISIONAL 策略多回 corr_to_short_squeeze（raw-pnl，僅觀察；真去重看因子 rank corr）",
        "directional_dilution_slice": {
            "status": "DEFER", "implemented": False,
            "needs_data": ["期貨基差 (futures basis)", "ETF 申贖/流向 (ETF creation/redemption flow)",
                           "做市借券 proxy (market-maker SBL)"],
            "rule": "edge 只集中套利重時段 → crowding/regime proxy 非 informed-bear → kill",
            "note": "AIF 放行條件 4 最大紅旗探針；無資料源 → 本次僅監控需求註記，硬做=造假故不實作(TODO)"},
        "graduation_precheck": _ft_eval_graduation_gates({}),
    }
    return {"ok": True, "as_of": today, "overall": overall,
            "roster_orthogonality": orthogonality,
            "provisional_monitoring": provisional_monitoring, "strategies": out}


# ── equity-curve(2026-06-27) 老闆「100萬→報酬%曲線」唯讀重建 ─────────────────────────────
def _ft_load_bars_readonly(codes: list, mkt: str, start: str, end: str) -> tuple:
    """純讀 daily_kbar 收盤（不觸發下載/寫入），回 (all_dates, bar_data)。equity-curve 唯讀用——
    cron 已每日 top-up daily_kbar(現到 06-26)，UI GET 不應觸網或寫表（與 _load_backtest_data 的
    差別＝省去 _ensure_daily_data 下載步驟；同一張 daily_kbar → bench 口徑與 track 一致）。"""
    if not codes:
        return [], {}
    con = market_db(); cur = con.cursor()
    ph = ",".join("?" * len(codes))
    cur.execute(f"""SELECT DISTINCT date FROM daily_kbar
        WHERE code IN ({ph}) AND market=? AND date BETWEEN ? AND ? ORDER BY date""",
        codes + [mkt, start, end])
    all_dates = [r[0] for r in cur.fetchall()]
    bar_data = {}
    cur.execute(f"""SELECT code,date,close FROM daily_kbar
        WHERE code IN ({ph}) AND market=? AND date BETWEEN ? AND ?""",
        codes + [mkt, start, end])
    for code, d, close in cur.fetchall():
        if close is None or (isinstance(close, float) and close != close) or close <= 0:
            continue
        bar_data[(code, d)] = {"close": float(close)}
    con.close()
    return all_dates, bar_data


@app.get("/api/forward/equity-curve")
def forward_equity_curve(strategy: int = 0):
    """唯讀：每 active 策略從 ft_picks(entry_date/entry_price) + daily_kbar(close) 重建每日權益序列，
    供 P2「報酬%(100萬)」欄 + 迷你 sparkline + L1 大曲線用。
      • raw_pct(d)  = 等權平均 net 報酬%（與 track mark_return 同口徑 _net_return_pct，含台股手續費+稅；
                      非 gross close/entry−1 → 才能與 forward/track 的 mark 數對得上，且誠實扣成本）。
      • excess_pct(d)= 逐 pick (raw_i − 同期等權大盤報酬) 的等權平均（複用 _equal_weight_bench_map，
                      與 forward/track live_avg_excess 同基準；逐 pick 算可容忍交錯進場日）。
    純讀不寫任何表、不觸網（daily_kbar 由 cron top-up）；PIT 用該日實際收盤、不用未來 bar。
    0 picks（如 sbl_short 尚未出手）→ 回空 points，前端顯示「未出手」。"""
    _ensure_ft_tables()
    today = datetime.now().strftime("%Y-%m-%d")
    con = db(); cur = con.cursor()
    q = "SELECT id,name,market,horizon_days,status FROM ft_strategies WHERE status='active'"
    p = []
    if strategy:
        q += " AND id=?"; p.append(strategy)
    q += " ORDER BY id"
    strs = cur.execute(q, p).fetchall()
    pmap = {}
    for sid, name, mkt, horizon, status in strs:
        pmap[sid] = cur.execute("""SELECT ticker,entry_date,entry_price,horizon_days,status,exit_date
            FROM ft_picks WHERE strategy_id=? AND status IN ('open','closed')""", (sid,)).fetchall()
    con.close()

    # 按 market 分組一次預載 universe bars + 等權大盤 nav（同 forward_update：同市場共用，省重載）
    mkts = {}
    for sid, name, mkt, horizon, status in strs:
        ents = [r[1] for r in pmap.get(sid, []) if r[1] and r[2]]
        if ents:
            mkts.setdefault(mkt, set()).add(min(ents))
    preload = {}
    for mkt, ents in mkts.items():
        codes = _ft_universe_codes(mkt)
        start = (datetime.strptime(min(ents), "%Y-%m-%d") - timedelta(days=12)).strftime("%Y-%m-%d")
        all_dates, bar_data = _ft_load_bars_readonly(codes, mkt, start, today)
        bench = _equal_weight_bench_map(codes, all_dates, bar_data)
        preload[mkt] = (all_dates, bar_data, bench)

    out = []
    for sid, name, mkt, horizon, status in strs:
        picks = [r for r in pmap.get(sid, []) if r[1] and r[2] is not None]  # 需 entry_date+entry_price
        if not picks or mkt not in preload:
            out.append({"strategy_id": sid, "name": name, "market": mkt, "points": [],
                        "current_raw_pct": None, "current_excess_pct": None,
                        "n_open": 0, "entry_date": None})
            continue
        all_dates, bar_data, bench = preload[mkt]
        min_entry = min(r[1] for r in picks)
        n_open = sum(1 for r in picks if r[4] == "open")
        points = []
        for d in all_dates:
            if d < min_entry or d > today:
                continue
            raws = []; excs = []
            for ticker, entry_date, entry_price, hd, st, exit_date in picks:
                if d < entry_date:                              # 尚未進場
                    continue
                if st == "closed" and exit_date and d > exit_date:   # 已出場 → 不再 mark
                    continue
                bar = bar_data.get((ticker, d))
                if not bar:                                     # 該股當日無收盤（停牌/缺資料）
                    continue
                rret = _net_return_pct(entry_price, bar["close"], mkt, TW_DISCOUNT)
                raws.append(rret)
                be, bx = bench.get(entry_date), bench.get(d)
                if be and bx and be > 0:
                    excs.append(rret - (bx / be - 1) * 100)
            if not raws:                                        # 非交易日 → 不落點
                continue
            points.append({"date": d,
                           "raw_pct": round(sum(raws) / len(raws), 2),
                           "excess_pct": (round(sum(excs) / len(excs), 2) if excs else None)})
        out.append({"strategy_id": sid, "name": name, "market": mkt, "points": points,
                    "current_raw_pct": (points[-1]["raw_pct"] if points else None),
                    "current_excess_pct": (points[-1]["excess_pct"] if points else None),
                    "n_open": n_open, "entry_date": min_entry})
    return {"ok": True, "as_of": today, "capital": 1000000, "strategies": out}


# ── real-curve(2026-06-27) 老闆「研究→🏭量產AI建議→👰老婆實際」閉環同頁戰績 ─────────────
#  與 forward/equity-curve 同模式（報酬%/超額/曲線），純讀不寫業務表、不觸網（daily_kbar 由 cron
#  top-up）。兩個追蹤組合：🤖 AI建議（每檔首次 BUY 為等權進場，每日 mark net 報酬%）、👰 老婆實際
#  （positions 成本基，報酬%/市值美股換匯 _get_usdtwd 與首頁同口徑）。誠實鐵律：資料不足就 from-today、
#  不偽造歷史曲線；mark = 未實現預估。
def _real_curve_ai_buy(today: str) -> dict:
    """🤖 AI建議組合：ic_rec_history 每檔『首次出現 BUY（含 entry_price>0）』為等權進場，
    每日從 daily_kbar 收盤 mark net 報酬%（_net_return_pct，與 forward equity-curve 同口徑·扣成本），
    excess 逐 pick 減同市場等權大盤（_equal_weight_bench_map）。自首個 BUY 日起追蹤（誠實標 start_date）。
    台美跨市場：各 pick 用自己市場的 daily_kbar / bench；美股 daily_kbar 較舊時其 mark 會略凍結（已標）。"""
    name = "🤖 AI建議組合"
    note_base = "跟著每日『今日該買』BUY 清單買的戰績（等權·每日 mark·扣手續費/稅 net）"
    con = db(); cur = con.cursor()
    try:
        rows = cur.execute("""
            SELECT code, MIN(created_at) AS first_seen
            FROM ic_rec_history
            WHERE direction='BUY' AND entry_price IS NOT NULL AND entry_price > 0
            GROUP BY code
        """).fetchall()
        picks = []
        for code, first_seen in rows:
            ep = cur.execute("""
                SELECT entry_price, market FROM ic_rec_history
                WHERE code=? AND direction='BUY' AND entry_price IS NOT NULL AND entry_price > 0
                ORDER BY created_at LIMIT 1
            """, (code,)).fetchone()
            if not ep or not ep[0] or ep[0] <= 0:
                continue
            mkt = (ep[1] or ("TW" if _is_tw_code(code) else "US")).upper()
            picks.append({"code": code, "market": mkt,
                          "entry_date": (first_seen or "")[:10], "entry_price": float(ep[0])})
    finally:
        con.close()
    picks = [p for p in picks if p["entry_date"]]
    if not picks:
        return {"key": "ai_buy", "name": name, "points": [], "current_raw_pct": None,
                "current_excess_pct": None, "n_holdings": 0, "start_date": None,
                "note": note_base + "；目前無 BUY 紀錄（誠實：待量產出建議後累積）",
                "from_today": True, "holdings": []}
    start_date = min(p["entry_date"] for p in picks)
    # 按市場預載 universe + pick bars（唯讀），各市場一份等權大盤 nav
    by_mkt = {}
    for p in picks:
        by_mkt.setdefault(p["market"], []).append(p)
    preload = {}
    for mkt, ps in by_mkt.items():
        uni = _ft_universe_codes(mkt)
        load_codes = list(set(uni) | {p["code"] for p in ps})
        mn = min(p["entry_date"] for p in ps)
        start = (datetime.strptime(mn, "%Y-%m-%d") - timedelta(days=12)).strftime("%Y-%m-%d")
        all_dates, bar_data = _ft_load_bars_readonly(load_codes, mkt, start, today)
        bench = _equal_weight_bench_map(uni, all_dates, bar_data)
        preload[mkt] = (all_dates, bar_data, bench)
    all_d = sorted({d for (ad, _bd, _bh) in preload.values() for d in ad})
    points = []
    for d in all_d:
        if d < start_date or d > today:
            continue
        raws = []; excs = []
        for p in picks:
            if d < p["entry_date"]:
                continue
            _ad, bar_data, bench = preload[p["market"]]
            bar = bar_data.get((p["code"], d))
            if not bar:
                continue
            rret = _net_return_pct(p["entry_price"], bar["close"], p["market"], TW_DISCOUNT)
            raws.append(rret)
            be, bx = bench.get(p["entry_date"]), bench.get(d)
            if be and bx and be > 0:
                excs.append(rret - (bx / be - 1) * 100)
        if not raws:
            continue
        points.append({"date": d,
                       "raw_pct": round(sum(raws) / len(raws), 2),
                       "excess_pct": (round(sum(excs) / len(excs), 2) if excs else None)})
    # 每 pick 最新 mark（最後一個有 bar 的交易日）→ L1 明細
    holdings = []
    for p in picks:
        _ad, bar_data, _bh = preload[p["market"]]
        mark_pct = None; mark_date = None
        for d in reversed(_ad):
            if d > today or d < p["entry_date"]:
                continue
            bar = bar_data.get((p["code"], d))
            if bar:
                mark_pct = round(_net_return_pct(p["entry_price"], bar["close"], p["market"], TW_DISCOUNT), 2)
                mark_date = d
                break
        holdings.append({"code": p["code"], "market": p["market"], "entry_date": p["entry_date"],
                         "entry_price": round(p["entry_price"], 2), "mark_pct": mark_pct,
                         "mark_date": mark_date})
    holdings.sort(key=lambda h: (h["mark_pct"] is None, -(h["mark_pct"] or 0)))
    n_us = sum(1 for p in picks if p["market"] == "US")
    note = note_base + "；自 %s 起追蹤（%d 檔，含美股 %d 檔·美股日線較舊時其 mark 會略凍結）" % (
        start_date, len(picks), n_us)
    return {"key": "ai_buy", "name": name, "points": points,
            "current_raw_pct": (points[-1]["raw_pct"] if points else None),
            "current_excess_pct": (points[-1]["excess_pct"] if points else None),
            "n_holdings": len(picks), "start_date": start_date, "note": note,
            "from_today": False, "holdings": holdings}


def _real_curve_wife(today: str) -> dict:
    """👰 老婆實際持倉組合：positions(open) 為持倉、成本為基。報酬%/市值『美股換匯』(_get_usdtwd)
    與首頁 portfolio_summary 同口徑（current_raw_pct 對得上首頁 total_pnl_pct）。
    誠實降級：positions 多數無建立日(entry_date 空)、美股 daily_kbar 零碎(部分僅到 04-30) → 不偽造歷史
    曲線，from-today 單錨點，每日累積。報酬%含換匯、為未實現預估。"""
    name = "👰 老婆實際持倉"
    positions = get_positions()
    if isinstance(positions, dict):
        positions = positions.get("data", positions.get("results", []))
    fx = _get_usdtwd()
    total_cost = 0.0; total_mv = 0.0
    holdings = []
    for p in positions:
        shares = p.get("shares", 0) or 0
        cost = p.get("cost", 0) or 0
        cp = p.get("current_price", 0) or 0
        if shares <= 0 or cost <= 0:
            continue
        mkt = str(p.get("market", "TW")).upper()
        rate = fx if mkt == "US" else 1.0
        cost_twd = cost * shares * rate
        mv_twd = cp * shares * rate
        total_cost += cost_twd
        total_mv += mv_twd
        holdings.append({"code": p.get("code", ""), "market": mkt, "shares": shares,
                         "cost": round(cost, 2), "current_price": round(cp, 2),
                         "pnl_pct": (round((cp / cost - 1) * 100, 2) if cost else None),
                         "mkt_value_twd": round(mv_twd, 0)})
    if total_cost <= 0:
        return {"key": "wife", "name": name, "points": [], "current_raw_pct": None,
                "current_excess_pct": None, "n_holdings": 0, "start_date": None,
                "note": "無持倉或成本資料", "from_today": True, "holdings": [],
                "fx_usdtwd": fx, "total_cost_twd": 0, "total_mv_twd": 0}
    cur_pct = round((total_mv / total_cost - 1) * 100, 2)
    holdings.sort(key=lambda h: -(h["mkt_value_twd"] or 0))
    n_us = sum(1 for h in holdings if h["market"] == "US")
    note = (f"實際持倉未實現報酬%（含美股換匯 USDTWD={fx:.2f}·與首頁持倉同口徑）；"
            f"持倉 {len(holdings)} 檔含美股 {n_us} 檔。多數無建立日、美股日線資料不齊 → "
            f"不偽造歷史曲線，自今日起每日累積（預估）")
    return {"key": "wife", "name": name,
            "points": [{"date": today, "raw_pct": cur_pct, "excess_pct": None}],
            "current_raw_pct": cur_pct, "current_excess_pct": None,
            "n_holdings": len(holdings), "start_date": today, "note": note,
            "from_today": True, "holdings": holdings,
            "fx_usdtwd": fx, "total_cost_twd": round(total_cost, 0),
            "total_mv_twd": round(total_mv, 0)}


@app.get("/api/portfolio/real-curve")
def portfolio_real_curve():
    """唯讀：把『🏭量產AI建議』與『👰老婆實際持倉』接進前向戰績頁，與 forward/equity-curve 同模式
    （報酬%/超額/曲線）。閉環最後一塊：研究策略 → AI建議（產品賺不賺）→ 老婆實際 並排同頁。
    純讀、不寫業務表；美股一律換匯成台幣再加總（_get_usdtwd）。資料不足誠實 from-today。"""
    today = datetime.now().strftime("%Y-%m-%d")
    combos = []
    for fn in (_real_curve_ai_buy, _real_curve_wife):
        try:
            combos.append(fn(today))
        except Exception as e:
            combos.append({"key": getattr(fn, "__name__", "?"), "name": "（載入失敗）",
                           "points": [], "current_raw_pct": None, "current_excess_pct": None,
                           "n_holdings": 0, "start_date": None, "from_today": True,
                           "holdings": [], "note": "計算失敗：%s" % e})
    return {"ok": True, "as_of": today, "capital": 1000000, "combos": combos}


@app.get("/api/forward/cron/status")
def forward_cron_status(limit: int = 30):
    """半自動 forward cron run-log 唯讀查詢（cockpit 查 cron 落地用）。純讀 forward_cron_runs，
    不觸發任何編排、不寫資料、不依賴 token。編排本體在 ops/forward_daily_cron.py（standalone）。"""
    _ensure_forward_cron_table()
    try:
        limit = min(max(int(limit), 1), 365)
    except (TypeError, ValueError):
        limit = 30
    con = db(); cur = con.cursor()
    cols = ["run_date", "started_at", "completed_at", "status", "market", "search_run_id",
            "rolled", "rejected_collinear", "rejected_gate", "replaced",
            "picks_total", "settled", "near_graduation_json", "error", "summary_json", "updated_at"]
    rows = cur.execute(
        f"SELECT {','.join(cols)} FROM forward_cron_runs ORDER BY run_date DESC LIMIT ?",
        (limit,)).fetchall()
    con.close()
    runs = []
    for r in rows:
        d = dict(zip(cols, r))
        for jk in ("near_graduation_json", "summary_json"):
            if d.get(jk):
                try:
                    d[jk[:-5]] = json.loads(d[jk])
                except Exception:
                    d[jk[:-5]] = None
            else:
                d[jk[:-5]] = None
            d.pop(jk, None)
        runs.append(d)
    last = runs[0] if runs else None
    return {"ok": True, "count": len(runs), "last": last, "runs": runs}


@app.get("/api/forward/picks")
def forward_picks(strategy: int = 0, date: str = ""):
    """看 picks 明細（可依 strategy / pick_date 過濾）。"""
    _ensure_ft_tables()
    con = db(); cur = con.cursor()
    cols = ["id", "strategy_id", "pick_date", "run_date", "ticker", "name", "ref_price", "regime",
            "vix_bucket", "horizon_days", "entry_date", "entry_price", "mark_date", "mark_price",
            "mark_return", "mark_excess", "exit_date", "exit_price", "realized_return", "realized_excess",
            "status", "rank_json"]
    q = f"SELECT {','.join(cols)} FROM ft_picks WHERE 1=1"
    p = []
    if strategy:
        q += " AND strategy_id=?"; p.append(strategy)
    if date:
        q += " AND pick_date=?"; p.append(date)
    q += " ORDER BY strategy_id,pick_date,ticker"
    rows = cur.execute(q, p).fetchall()
    con.close()
    out = []
    for r in rows:
        d = dict(zip(cols, r))
        try:
            d["rank"] = json.loads(d.pop("rank_json")) if d.get("rank_json") else None
        except Exception:
            d["rank"] = None; d.pop("rank_json", None)
        out.append(d)
    return {"ok": True, "count": len(out), "picks": out}


# 種子策略（老闆規格 §五）：①A4 k0.1 ②A4∧revenue k0.1×k0.5 ③同②+vix_gate=VIX_MID
#   ④A4∧inst_net_buy k0.1×k0.5（對照組，看籌碼 live 是否真沒用）。
_FT_SEED_STRATEGIES = [
    {"name": "A4_topsec_k10",
     "layers": [{"factor": "sector_rel_topsec", "top_k": 0.10}],
     "regime_gate": "TREND_UP", "vix_gate": None, "horizon_days": 20, "source": "manual"},
    {"name": "A4∧revenue_mom",
     "layers": [{"factor": "sector_rel_topsec", "top_k": 0.10}, {"factor": "revenue_mom", "top_k": 0.50}],
     "regime_gate": "TREND_UP", "vix_gate": None, "horizon_days": 20, "source": "manual"},
    {"name": "A4∧revenue_mom@VIX_MID",
     "layers": [{"factor": "sector_rel_topsec", "top_k": 0.10}, {"factor": "revenue_mom", "top_k": 0.50}],
     "regime_gate": "TREND_UP", "vix_gate": "VIX_MID", "horizon_days": 20, "source": "manual"},
    {"name": "A4∧inst_net_buy",
     "layers": [{"factor": "sector_rel_topsec", "top_k": 0.10}, {"factor": "inst_net_buy", "top_k": 0.50}],
     "regime_gate": "TREND_UP", "vix_gate": None, "horizon_days": 20, "source": "manual"},
]


@app.post("/api/forward/seed")
def forward_seed(date: str = "", run: bool = True, _: None = Depends(require_token)):
    """種子：註冊 4 個策略（冪等）並（可選）立即跑今日選股。"""
    _ensure_ft_tables()
    registered = [forward_register_strategy(dict(spec)) for spec in _FT_SEED_STRATEGIES]
    res = forward_run(date=date or datetime.now().strftime("%Y-%m-%d")) if run else None
    return {"ok": True, "registered": registered, "run": res}


# ── sbl-forward-entry-decision(2026-06-26)：A4∧sbl_short PROVISIONAL_BORDERLINE 入場 ─────────────
#   AIF 判決(B)：live :8766 真驗 excess +2.32%[1.12,3.66] DSR0.84（三 chip 因子最強）→ 放行進 forward
#   紙上實盤。DSR0.84 卡的是「畢業真錢」尺、不該擋紙上觀察（ROLL 閘已修正，見 _FT_ROLL_DSR_MIN）。
#   layers = sector_rel_topsec(A4, top_k0.1) ∧ sbl_short(top_k0.2, dir=-1：借券賣出餘額 informed-bear)。
_FT_PROVISIONAL_SBL = {
    "name": "A4∧sbl_short",
    "layers": [{"factor": "sector_rel_topsec", "top_k": 0.10},
               {"factor": "sbl_short", "top_k": 0.20, "dir": -1}],
    "market": "TW", "regime_gate": "TREND_UP", "vix_gate": None, "horizon_days": 20,
    "source": "aif_decision", "provenance": "provisional_borderline", "roll_dsr": 0.84,
    # ★E4 去重例外（誠實處理 finding #1）：A4∧sbl 與既有 A4-family 的 raw-pnl corr ≥0.70（共用 A4 腿、
    #   市場 beta 主導——cockpit 實證 A4∧sbl vs A4∧rev pnl≈0.97），但這是 E4 raw-pnl 度量已知缺陷
    #   （量不出選股正交）非真冗餘（因子 rank corr=-0.315 真不同訊號）。故本入場 = deliberate provisional
    #   override：直接註冊、不讓壞掉的 raw-pnl E4 擋它。
    "notes": "PROVISIONAL_BORDERLINE(DSR=0.84)。AIF 判決(B) 2026-06-26 手動置入觀察。"
             "E4 raw-pnl 去重對共用 A4 腿不可靠(finding#1：市場 beta 主導, A4∧sbl vs A4∧rev pnl≈0.97)，"
             "故繞過 auto-roll E4 直接註冊；真去重看橫斷面因子 rank corr(=-0.315 vs short_squeeze)。"
             "監控：衰減閘(closed N≥10 且 excess bootstrap CI 下界<0 → KILL_CANDIDATE)、"
             "去重存活(vs short_squeeze)。畢業前置見 R-FWD-9 stub。"
             "方向性稀釋切片(期貨基差/ETF 流)需新資料 → defer(TODO)。",
}


@app.post("/api/forward/register-sbl-provisional")
def forward_register_sbl_provisional(run: bool = False, date: str = "", _: None = Depends(require_token)):
    """註冊 A4∧sbl_short 為 PROVISIONAL_BORDERLINE forward 策略（AIF 判決(B) 2026-06-26）。
    冪等 by name。deliberate override：繞過 auto-roll 的 raw-pnl E4 去重（finding#1 已知缺陷）直接置入。
    可選 run=True 立即跑今日選股。"""
    _ensure_ft_tables()
    registered = forward_register_strategy(dict(_FT_PROVISIONAL_SBL))
    res = forward_run(date=date or datetime.now().strftime("%Y-%m-%d")) if run else None
    return {"ok": True, "registered": registered, "provisional": True, "run": res}


# ── 啟動 ──────────────────────────────────────────
if __name__ == "__main__":
    import sys, io
    if sys.stdout.encoding and sys.stdout.encoding.lower().startswith("cp"):
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    mode = "Simulation" if SIMULATION else "PRODUCTION"
    print(f"[Smart Monitor v4 (v2.0 base)] http://localhost:8766  [{mode}]")
    print(f"[Security] API Token: {_API_TOKEN}")
    print(f"[Security] Token file: {_TOKEN_FILE}")
    print(f"[Security] Bound to 127.0.0.1 (localhost only)")
    # 預先登入 Shioaji，避免首次請求時多 thread 競爭造成 hang
    try:
        print("[Startup] Shioaji login...", flush=True)
        get_api()
        print("[Startup] Shioaji ready.", flush=True)
    except Exception as e:
        print(f"[Startup] Shioaji login failed (non-fatal): {e}", flush=True)
    uvicorn.run(app, host="127.0.0.1", port=8766, reload=False)
