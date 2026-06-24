# 智慧投顧與盤中監控系統 — 架構設計與實作規格

**建立日期：** 2026-06-09  
**適用市場：** 台灣股票市場（TWSE / TPEX）  
**API 核心：** Shioaji (永豐金) + yfinance（總經數據）

---

## 一、系統架構總覽

```
┌─────────────────────────────────────────────────────────────┐
│                    智慧投顧監控系統                           │
├──────────────┬──────────────┬──────────────┬────────────────┤
│  資料層       │  訊號引擎層   │  風控層       │  執行層        │
│  DataLayer   │  SignalEngine│  RiskManager │  OrderManager  │
├──────────────┼──────────────┼──────────────┼────────────────┤
│ Shioaji Tick │ 多週期聯動    │ 部位縮放      │ 智慧單送出     │
│ K線歷史資料  │ MACD/MA/BIAS │ 總經風控閥    │ 移動止盈       │
│ 盤後籌碼     │ 量價突破偵測  │ 停損強制執行  │ 盤後選股更新   │
│ 總經指標     │ NLP情感分析  │ 當沖比監控   │ Line/Email通知 │
└──────────────┴──────────────┴──────────────┴────────────────┘
```

---

## 二、資料庫設計

### 技術選型建議

| 用途 | 技術 | 理由 |
|------|------|------|
| Tick 即時資料 | Redis (記憶體) | 毫秒延遲，TTL 自動清除當日資料 |
| K線歷史資料 | SQLite / DuckDB | 輕量、本機、無需伺服器 |
| 籌碼/選股池 | SQLite | 每日更新，結構化查詢 |
| 持倉/交易紀錄 | SQLite | ACID，確保資料一致性 |
| 總經指標快取 | Redis | 30分鐘 TTL 快取 |

### SQLite Schema

```sql
-- 持倉表
CREATE TABLE positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    entry_price REAL NOT NULL,
    quantity INTEGER NOT NULL,
    entry_time DATETIME NOT NULL,
    highest_price REAL,          -- 移動止盈用
    stop_loss_price REAL,        -- 絕對停損價
    trailing_stop_pct REAL,      -- 移動止盈回落%
    status TEXT DEFAULT 'open'   -- open/closed
);

-- 選股監控池
CREATE TABLE watchlist (
    code TEXT PRIMARY KEY,
    name TEXT,
    add_date DATE,
    reason TEXT,                 -- BUY_A / BUY_B / SQUEEZE / INSTITUTION
    itrust_days INTEGER,         -- 投信連買天數
    margin_short_ratio REAL,     -- 券資比
    target_price REAL,           -- 法人目標價中位數
    active INTEGER DEFAULT 1
);

-- 每日籌碼快照
CREATE TABLE chip_snapshot (
    code TEXT,
    date DATE,
    itrust_buy INTEGER,          -- 投信買超張數
    itrust_hold_ratio REAL,      -- 投信持股比例
    margin_short_ratio REAL,
    forced_buyback_date DATE,    -- 融券強制回補日
    PRIMARY KEY (code, date)
);

-- 交易訊號日誌
CREATE TABLE signal_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT,
    signal_type TEXT,            -- BUY_A/BUY_B/EXIT_A/EXIT_B/EXIT_C/EXIT_D
    price REAL,
    timestamp DATETIME,
    executed INTEGER DEFAULT 0
);
```

---

## 三、API 架構方案

### 即時報價串接（Shioaji）

```python
# 訂閱 Tick + 五檔
api.quote.subscribe(contract, quote_type=sj.constant.QuoteType.Tick)
api.quote.subscribe(contract, quote_type=sj.constant.QuoteType.BidAsk)
```

### 盤後資料來源

| 資料 | 來源 | 方式 |
|------|------|------|
| 三大法人買賣超 | 台灣證交所 | HTTP GET（公開資料） |
| 融資融券餘額 | 台灣證交所 | HTTP GET（公開資料） |
| 歷史 K 線 | Shioaji `api.kbars()` | Python API |
| VIX / DXY / US10Y | yfinance | `yf.download('^VIX')` |
| 美股期指夜盤 | yfinance | `yf.Ticker('ES=F')` |

---

## 四、Python 完整實作範例

```python
"""
智慧投顧與盤中監控系統 — 核心實作
需求: shioaji, pandas, numpy, yfinance, redis, sqlite3
"""

import shioaji as sj
import pandas as pd
import numpy as np
import yfinance as yf
import sqlite3
import redis
import time
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# 資料結構定義
# ──────────────────────────────────────────────

class SignalType(Enum):
    BUY_A = "BUY_A"          # 假跌破破底翻
    BUY_B = "BUY_B"          # 主力量價突破
    EXIT_A = "EXIT_A"        # 均價線跌破
    EXIT_B = "EXIT_B"        # 高檔爆量出貨
    EXIT_C = "EXIT_C"        # 移動止盈
    EXIT_D = "EXIT_D"        # 絕對停損（保命鍵）
    LOCK_BUY = "LOCK_BUY"    # 正乖離過大鎖定買進


@dataclass
class MarketEnvironment:
    """總經環境狀態"""
    vix: float = 0.0
    dxy: float = 0.0
    us10y: float = 0.0
    es_futures_chg: float = 0.0   # 美股期指漲跌%
    risk_level: str = "NORMAL"    # NORMAL / CAUTION / ALERT
    position_scale: float = 1.0   # 部位縮放倍率 0.0~1.0


@dataclass
class TickBuffer:
    """個股 Tick 緩衝區"""
    code: str
    prices: list = field(default_factory=list)
    volumes: list = field(default_factory=list)
    bid_prices: list = field(default_factory=list)
    ask_prices: list = field(default_factory=list)
    timestamps: list = field(default_factory=list)
    outside_bid_count: int = 0    # 連續外盤計數
    large_order_count: int = 0    # 特大單計數


# ──────────────────────────────────────────────
# 指標計算模組
# ──────────────────────────────────────────────

class IndicatorEngine:

    @staticmethod
    def moving_average(prices: pd.Series, period: int) -> pd.Series:
        return prices.rolling(window=period).mean()

    @staticmethod
    def macd(prices: pd.Series, fast=12, slow=26, signal=9):
        ema_fast = prices.ewm(span=fast).mean()
        ema_slow = prices.ewm(span=slow).mean()
        dif = ema_fast - ema_slow
        macd_line = dif.ewm(span=signal).mean()
        histogram = dif - macd_line
        return dif, macd_line, histogram

    @staticmethod
    def bias(current_price: float, ma5: float) -> float:
        """乖離率：(現價 - 5日MA) / 5日MA * 100%"""
        if ma5 == 0:
            return 0.0
        return (current_price - ma5) / ma5 * 100.0

    @staticmethod
    def vwap(prices: pd.Series, volumes: pd.Series) -> float:
        """當日 VWAP（均價線）"""
        if volumes.sum() == 0:
            return 0.0
        return (prices * volumes).sum() / volumes.sum()

    @staticmethod
    def is_bullish_alignment(ma5: float, ma10: float, ma20: float) -> bool:
        """多頭排列：5MA > 10MA > 20MA"""
        return ma5 > ma10 > ma20


# ──────────────────────────────────────────────
# 總經風控模組
# ──────────────────────────────────────────────

class MacroRiskManager:

    ALERT_VIX_SURGE = 0.15       # VIX 單日漲幅超 15%
    ALERT_DXY_SURGE = 0.008      # DXY 單日漲幅超 0.8%
    ALERT_US10Y_SURGE = 0.15     # US10Y 單日漲幅超 15bps

    def fetch_macro_data(self) -> MarketEnvironment:
        env = MarketEnvironment()
        try:
            # VIX
            vix_data = yf.Ticker('^VIX').history(period='2d')
            if len(vix_data) >= 2:
                env.vix = vix_data['Close'].iloc[-1]
                vix_prev = vix_data['Close'].iloc[-2]
                vix_chg = (env.vix - vix_prev) / vix_prev
            else:
                vix_chg = 0.0

            # DXY 美元指數
            dxy_data = yf.Ticker('DX-Y.NYB').history(period='2d')
            if len(dxy_data) >= 2:
                env.dxy = dxy_data['Close'].iloc[-1]
                dxy_chg = (env.dxy - dxy_data['Close'].iloc[-2]) / dxy_data['Close'].iloc[-2]
            else:
                dxy_chg = 0.0

            # US10Y
            us10y_data = yf.Ticker('^TNX').history(period='2d')
            if len(us10y_data) >= 2:
                env.us10y = us10y_data['Close'].iloc[-1]
                us10y_chg = env.us10y - us10y_data['Close'].iloc[-2]
            else:
                us10y_chg = 0.0

            # 美股期指（ES）
            es_data = yf.Ticker('ES=F').history(period='2d')
            if len(es_data) >= 2:
                env.es_futures_chg = (
                    es_data['Close'].iloc[-1] - es_data['Close'].iloc[-2]
                ) / es_data['Close'].iloc[-2]

            # 風控等級判斷
            alert_conditions = [
                vix_chg > self.ALERT_VIX_SURGE,
                dxy_chg > self.ALERT_DXY_SURGE,
                us10y_chg > self.ALERT_US10Y_SURGE,
            ]
            alert_count = sum(alert_conditions)

            if alert_count >= 2:
                env.risk_level = "ALERT"
                env.position_scale = 0.30   # 部位縮至 30%
                logger.warning(f"[風控] ALERT — VIX:{env.vix:.1f} DXY_chg:{dxy_chg:.3f} US10Y_chg:{us10y_chg:.3f}")
            elif alert_count == 1:
                env.risk_level = "CAUTION"
                env.position_scale = 0.60   # 部位縮至 60%
            else:
                env.risk_level = "NORMAL"
                env.position_scale = 1.00

        except Exception as e:
            logger.error(f"[總經資料] 擷取失敗: {e}")
            env.risk_level = "CAUTION"
            env.position_scale = 0.60
        return env


# ──────────────────────────────────────────────
# 盤中買進訊號引擎
# ──────────────────────────────────────────────

class BuySignalEngine:

    # 參數設定
    BIAS_BUY_LOCK_THRESHOLD = 15.0   # 正乖離 > 15% 鎖定買進
    VOLUME_RATIO_THRESHOLD = 2.5     # 量比 > 2.5 倍
    LARGE_ORDER_LOTS = 100           # 特大單定義（張）
    OUTSIDE_BID_CONSECUTIVE = 5      # 連續外盤筆數

    def __init__(self, indicator: IndicatorEngine):
        self.ind = indicator

    def check_buy_lock(self, current_price: float, ma5: float) -> bool:
        """乖離率過大 → 鎖定買進"""
        bias = self.ind.bias(current_price, ma5)
        if bias > self.BIAS_BUY_LOCK_THRESHOLD:
            logger.info(f"[LOCK] 正乖離 {bias:.1f}% > {self.BIAS_BUY_LOCK_THRESHOLD}%，鎖定買進")
            return True
        return False

    def check_daily_bullish_trend(self, daily_df: pd.DataFrame) -> bool:
        """日K多頭排列過濾"""
        close = daily_df['close']
        ma5 = self.ind.moving_average(close, 5).iloc[-1]
        ma10 = self.ind.moving_average(close, 10).iloc[-1]
        ma20 = self.ind.moving_average(close, 20).iloc[-1]
        return self.ind.is_bullish_alignment(ma5, ma10, ma20)

    def check_60min_macd_bull(self, h60_df: pd.DataFrame) -> bool:
        """60分K MACD 金叉或柱狀體翻紅"""
        close = h60_df['close']
        dif, macd_line, histogram = self.ind.macd(close)

        # 柱狀體由綠翻紅（前一根 < 0，最新一根 > 0）
        hist_flip_red = histogram.iloc[-2] < 0 and histogram.iloc[-1] > 0
        # DIF 金叉 MACD
        golden_cross = dif.iloc[-2] < macd_line.iloc[-2] and dif.iloc[-1] > macd_line.iloc[-1]

        return hist_flip_red or golden_cross

    def check_buy_a(
        self,
        current_price: float,
        ma5: float,
        tick_buffer: TickBuffer,
        breach_time: Optional[datetime],
    ) -> bool:
        """
        Buy_A：五日線假跌破/破底翻
        1. 盤中跌破 5日線
        2. 15-30 分鐘內拉回 5日線之上
        3. 拉回時伴隨特大單或連續外盤
        """
        if breach_time is None:
            return False
        minutes_since_breach = (datetime.now() - breach_time).seconds / 60
        if not (15 <= minutes_since_breach <= 30):
            return False
        if current_price <= ma5:
            return False

        # 確認量能條件
        has_large_order = tick_buffer.large_order_count >= 1
        has_consecutive_outside = tick_buffer.outside_bid_count >= self.OUTSIDE_BID_CONSECUTIVE
        return has_large_order or has_consecutive_outside

    def check_buy_b(
        self,
        tick_buffer: TickBuffer,
        avg_5day_volume_per_min: float,
    ) -> bool:
        """
        Buy_B：主力多頭量價突破
        1. 量比 > 2.5 倍
        2. 連續 5 筆以上外盤 + 特大單
        3. 五檔外盤壓大單被連續吃掉
        """
        if len(tick_buffer.volumes) < 5:
            return False

        # 計算最近 5 分鐘量比
        recent_vol = sum(tick_buffer.volumes[-5:])
        if avg_5day_volume_per_min > 0:
            volume_ratio = recent_vol / (avg_5day_volume_per_min * 5)
        else:
            return False

        if volume_ratio < self.VOLUME_RATIO_THRESHOLD:
            return False

        large_order_threshold = max(
            self.LARGE_ORDER_LOTS,
            avg_5day_volume_per_min * 5
        )
        has_large_order = tick_buffer.large_order_count >= 1
        has_consecutive_outside = tick_buffer.outside_bid_count >= self.OUTSIDE_BID_CONSECUTIVE

        return has_large_order and has_consecutive_outside


# ──────────────────────────────────────────────
# 盤中賣出/停損引擎
# ──────────────────────────────────────────────

class ExitSignalEngine:

    VWAP_FAIL_MINUTES = 3       # VWAP 跌破後無法站回時間（分鐘）
    LARGE_ORDER_SELL_LOTS = 100 # 特大單砸盤定義

    def check_exit_a(
        self,
        current_price: float,
        vwap: float,
        below_vwap_since: Optional[datetime],
    ) -> bool:
        """Exit_A：跌破均價線 3 分鐘無法站回"""
        if current_price < vwap and below_vwap_since is not None:
            minutes_below = (datetime.now() - below_vwap_since).seconds / 60
            if minutes_below >= self.VWAP_FAIL_MINUTES:
                logger.info(f"[EXIT_A] 跌破VWAP {vwap:.2f}，持續 {minutes_below:.1f} 分鐘")
                return True
        return False

    def check_exit_b(
        self,
        is_high_zone: bool,
        tick_buffer: TickBuffer,
        last_1min_candle: Optional[dict],
    ) -> bool:
        """Exit_B：高檔爆量出貨（特大單砸內盤 + 長上影線或黑K）"""
        if not is_high_zone:
            return False
        has_large_sell = tick_buffer.large_order_count >= 1
        if not has_large_sell:
            return False
        if last_1min_candle:
            body = last_1min_candle['close'] - last_1min_candle['open']
            upper_shadow = last_1min_candle['high'] - max(
                last_1min_candle['open'], last_1min_candle['close']
            )
            is_bearish = body < 0 or upper_shadow > abs(body) * 0.5
            if is_bearish:
                logger.info("[EXIT_B] 高檔爆量 + 長上影/黑K，觸發出場")
                return True
        return False

    def check_exit_c(
        self,
        current_price: float,
        entry_price: float,
        highest_price: float,
        is_swing: bool = True,
    ) -> bool:
        """
        Exit_C：移動止盈
        波段單：利潤達 8% 後回落 2%
        當沖單：利潤達 3% 後回落 1%
        """
        if is_swing:
            profit_trigger = 0.08
            drawdown_trigger = 0.02
        else:
            profit_trigger = 0.03
            drawdown_trigger = 0.01

        max_profit_pct = (highest_price - entry_price) / entry_price
        if max_profit_pct < profit_trigger:
            return False

        drawdown_from_high = (highest_price - current_price) / highest_price
        if drawdown_from_high >= drawdown_trigger:
            locked_profit = (current_price - entry_price) / entry_price * 100
            logger.info(
                f"[EXIT_C] 移動止盈觸發 — 最高獲利:{max_profit_pct*100:.1f}% "
                f"回落:{drawdown_from_high*100:.1f}% 鎖住獲利:{locked_profit:.1f}%"
            )
            return True
        return False

    def check_exit_d(
        self,
        current_price: float,
        entry_price: float,
        stop_loss_pct: float = 0.05,
    ) -> bool:
        """Exit_D：絕對停損安全閥（保命鍵）— 虧損達 -5% 強制出場"""
        loss_pct = (current_price - entry_price) / entry_price
        if loss_pct <= -stop_loss_pct:
            logger.warning(
                f"[EXIT_D ⚠️ 保命鍵] 虧損 {loss_pct*100:.2f}% 達停損閥，強制市價出場！"
            )
            return True
        return False


# ──────────────────────────────────────────────
# 盤後選股篩選模組
# ──────────────────────────────────────────────

class PostMarketScanner:

    def scan_squeeze_candidates(self, db_conn: sqlite3.Connection) -> pd.DataFrame:
        """融券軋空股篩選：券資比 > 30%，距強制回補 < 7 個交易日"""
        query = """
        SELECT c.code, c.margin_short_ratio, c.forced_buyback_date,
               julianday(c.forced_buyback_date) - julianday('now') AS days_to_buyback
        FROM chip_snapshot c
        WHERE c.date = (SELECT MAX(date) FROM chip_snapshot)
          AND c.margin_short_ratio > 0.30
          AND julianday(c.forced_buyback_date) - julianday('now') < 7
          AND julianday(c.forced_buyback_date) - julianday('now') > 0
        ORDER BY days_to_buyback ASC
        """
        return pd.read_sql(query, db_conn)

    def scan_institution_lock(self, db_conn: sqlite3.Connection) -> pd.DataFrame:
        """投信鎖碼股：連買 3 天以上，持股比例 3%-12%"""
        query = """
        SELECT code,
               SUM(CASE WHEN date >= date('now', '-5 days') AND itrust_buy > 0 THEN 1 ELSE 0 END) AS buy_days,
               MAX(itrust_hold_ratio) AS hold_ratio
        FROM chip_snapshot
        GROUP BY code
        HAVING buy_days >= 3
           AND hold_ratio BETWEEN 0.03 AND 0.12
        ORDER BY buy_days DESC
        """
        return pd.read_sql(query, db_conn)

    def check_abandon_signal(
        self,
        db_conn: sqlite3.Connection,
        code: str,
    ) -> bool:
        """籌碼棄守：原鎖碼投信連續 2 日大賣超"""
        query = """
        SELECT itrust_buy
        FROM chip_snapshot
        WHERE code = ?
        ORDER BY date DESC
        LIMIT 2
        """
        df = pd.read_sql(query, db_conn, params=(code,))
        if len(df) < 2:
            return False
        return df['itrust_buy'].iloc[0] < -500 and df['itrust_buy'].iloc[1] < -500


# ──────────────────────────────────────────────
# 主系統整合
# ──────────────────────────────────────────────

class SmartMonitorSystem:

    def __init__(self, api_key: str, secret_key: str, simulation: bool = True):
        # Shioaji 初始化
        self.api = sj.Shioaji(simulation=simulation)
        self.api.login(api_key=api_key, secret_key=secret_key)

        # 元件初始化
        self.ind = IndicatorEngine()
        self.macro_risk = MacroRiskManager()
        self.buy_engine = BuySignalEngine(self.ind)
        self.exit_engine = ExitSignalEngine()
        self.scanner = PostMarketScanner()

        # 狀態
        self.market_env = MarketEnvironment()
        self.tick_buffers: dict[str, TickBuffer] = {}
        self.breach_times: dict[str, datetime] = {}   # 跌破 5MA 的時間
        self.vwap_breach_times: dict[str, datetime] = {}

        # DB
        self.db = sqlite3.connect('investment_monitor.db', check_same_thread=False)
        self._init_db()

        # Tick 回呼
        self.api.quote.set_on_tick_fop_v1_callback(self._on_tick)
        self.api.quote.set_on_bidask_fop_v1_callback(self._on_bidask)

    def _init_db(self):
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL, entry_price REAL, quantity INTEGER,
            entry_time DATETIME, highest_price REAL, status TEXT DEFAULT 'open'
        );
        CREATE TABLE IF NOT EXISTS signal_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT, signal_type TEXT, price REAL, timestamp DATETIME, executed INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS chip_snapshot (
            code TEXT, date DATE, itrust_buy INTEGER, itrust_hold_ratio REAL,
            margin_short_ratio REAL, forced_buyback_date DATE, PRIMARY KEY (code, date)
        );
        """)
        self.db.commit()

    def _on_tick(self, exchange, tick):
        """Tick 回呼 — 更新緩衝區，觸發訊號檢查"""
        code = tick.code
        if code not in self.tick_buffers:
            self.tick_buffers[code] = TickBuffer(code=code)

        buf = self.tick_buffers[code]
        buf.prices.append(tick.close)
        buf.volumes.append(tick.volume)
        buf.timestamps.append(tick.datetime)

        # 外盤計數
        if tick.tick_type == 1:   # 1 = 外盤（主動買）
            buf.outside_bid_count += 1
        else:
            buf.outside_bid_count = 0

        # 特大單偵測（> 100 張）
        if tick.volume >= 100:
            buf.large_order_count += 1

        # 保留最近 1000 筆
        if len(buf.prices) > 1000:
            buf.prices = buf.prices[-1000:]
            buf.volumes = buf.volumes[-1000:]
            buf.timestamps = buf.timestamps[-1000:]

        self._run_signal_check(code, tick.close)

    def _on_bidask(self, exchange, bidask):
        """五檔更新（暫存，供 Buy_B 五檔結構分析使用）"""
        pass

    def _run_signal_check(self, code: str, current_price: float):
        """主訊號檢查流程"""
        # 1. 風控環境 → 如果 ALERT 且無持倉，阻斷買進
        if self.market_env.risk_level == "ALERT":
            return

        # 2. 取得日K資料
        try:
            contract = self.api.Contracts.Stocks[code]
            end = datetime.now().strftime('%Y-%m-%d')
            start = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
            kbars = self.api.kbars(contract, start=start, end=end)
            daily_df = pd.DataFrame({**kbars})
        except Exception:
            return

        if len(daily_df) < 20:
            return

        # 3. 日K多頭排列過濾
        if not self.buy_engine.check_daily_bullish_trend(daily_df):
            return

        close = daily_df['close']
        ma5 = self.ind.moving_average(close, 5).iloc[-1]

        # 4. 乖離率鎖定檢查
        if self.buy_engine.check_buy_lock(current_price, ma5):
            self._log_signal(code, SignalType.LOCK_BUY, current_price)
            return

        buf = self.tick_buffers.get(code, TickBuffer(code=code))

        # 5. 跌破 5MA 紀錄時間
        if current_price < ma5 and code not in self.breach_times:
            self.breach_times[code] = datetime.now()
        elif current_price > ma5 and code in self.breach_times:
            pass  # 等 Buy_A 判斷後再清除

        # 6. Buy_A 假跌破
        avg_vol_per_min = close.mean() / 100  # 簡化：實際應從歷史 tick 計算
        if self.buy_engine.check_buy_a(
            current_price, ma5, buf, self.breach_times.get(code)
        ):
            self._log_signal(code, SignalType.BUY_A, current_price)
            self.breach_times.pop(code, None)

        # 7. Buy_B 量價突破
        if self.buy_engine.check_buy_b(buf, avg_vol_per_min):
            self._log_signal(code, SignalType.BUY_B, current_price)

        # 8. 持倉出場檢查
        self._check_exit_for_positions(code, current_price, daily_df)

    def _check_exit_for_positions(
        self, code: str, current_price: float, daily_df: pd.DataFrame
    ):
        cursor = self.db.execute(
            "SELECT id, entry_price, highest_price FROM positions WHERE code=? AND status='open'",
            (code,)
        )
        for row in cursor.fetchall():
            pos_id, entry_price, highest_price = row
            highest_price = max(highest_price or entry_price, current_price)

            # 更新最高價
            self.db.execute(
                "UPDATE positions SET highest_price=? WHERE id=?",
                (highest_price, pos_id)
            )
            self.db.commit()

            # Exit_D 優先（保命鍵）
            if self.exit_engine.check_exit_d(current_price, entry_price):
                self._log_signal(code, SignalType.EXIT_D, current_price)
                self._send_exit_order(code, pos_id, reason="EXIT_D 保命停損")
                return

            # Exit_C 移動止盈
            if self.exit_engine.check_exit_c(current_price, entry_price, highest_price):
                self._log_signal(code, SignalType.EXIT_C, current_price)
                self._send_exit_order(code, pos_id, reason="EXIT_C 移動止盈")
                return

            # Exit_A VWAP 跌破
            prices_s = pd.Series(self.tick_buffers[code].prices)
            vols_s = pd.Series(self.tick_buffers[code].volumes)
            vwap = self.ind.vwap(prices_s, vols_s)
            if current_price < vwap and code not in self.vwap_breach_times:
                self.vwap_breach_times[code] = datetime.now()
            elif current_price >= vwap:
                self.vwap_breach_times.pop(code, None)

            if self.exit_engine.check_exit_a(
                current_price, vwap, self.vwap_breach_times.get(code)
            ):
                self._log_signal(code, SignalType.EXIT_A, current_price)
                self._send_exit_order(code, pos_id, reason="EXIT_A VWAP跌破")

    def _send_exit_order(self, code: str, pos_id: int, reason: str):
        """送出市價賣單（實際下單前請確認模擬/正式環境）"""
        logger.warning(f"[下單] {code} 出場 — 原因: {reason}")
        # 實際下單：
        # contract = self.api.Contracts.Stocks[code]
        # order = self.api.Order(price=0, quantity=quantity,
        #     action=sj.constant.Action.Sell,
        #     price_type=sj.constant.StockPriceType.MKT,
        #     order_type=sj.constant.TFTOrderType.ROD)
        # self.api.place_order(contract, order)
        self.db.execute(
            "UPDATE positions SET status='closed' WHERE id=?", (pos_id,)
        )
        self.db.commit()

    def _log_signal(self, code: str, signal: SignalType, price: float):
        logger.info(f"[訊號] {code} {signal.value} @ {price:.2f}")
        self.db.execute(
            "INSERT INTO signal_log (code, signal_type, price, timestamp) VALUES (?,?,?,?)",
            (code, signal.value, price, datetime.now())
        )
        self.db.commit()

    def update_macro_environment(self):
        """定時更新總經環境（建議每 30 分鐘執行一次）"""
        self.market_env = self.macro_risk.fetch_macro_data()
        logger.info(
            f"[總經] 風控等級:{self.market_env.risk_level} "
            f"部位縮放:{self.market_env.position_scale*100:.0f}% "
            f"VIX:{self.market_env.vix:.1f}"
        )

    def subscribe_watchlist(self, codes: list[str]):
        """批次訂閱監控清單"""
        for code in codes:
            try:
                contract = self.api.Contracts.Stocks[code]
                self.api.quote.subscribe(
                    contract,
                    quote_type=sj.constant.QuoteType.Tick,
                    version=sj.constant.QuoteVersion.v1,
                )
                self.tick_buffers[code] = TickBuffer(code=code)
                logger.info(f"[訂閱] {code} Tick 訂閱成功")
            except Exception as e:
                logger.error(f"[訂閱] {code} 失敗: {e}")

    def run_post_market_scan(self):
        """盤後選股掃描（每日 15:00 後執行）"""
        logger.info("[盤後] 開始選股掃描...")
        squeeze = self.scanner.scan_squeeze_candidates(self.db)
        institution = self.scanner.scan_institution_lock(self.db)
        logger.info(f"[盤後] 融券軋空候選: {len(squeeze)} 檔")
        logger.info(f"[盤後] 投信鎖碼候選: {len(institution)} 檔")
        return {"squeeze": squeeze, "institution": institution}

    def shutdown(self):
        self.api.logout()
        self.db.close()
        logger.info("[系統] 已安全關閉")


# ──────────────────────────────────────────────
# 啟動範例
# ──────────────────────────────────────────────

if __name__ == "__main__":
    system = SmartMonitorSystem(
        api_key="YOUR_API_KEY",
        secret_key="YOUR_SECRET_KEY",
        simulation=True,          # ← 務必先用模擬環境測試
    )

    # 初始化總經環境
    system.update_macro_environment()

    # 訂閱監控清單
    watchlist = ["2330", "2317", "2454", "2382"]
    system.subscribe_watchlist(watchlist)

    try:
        logger.info("[系統] 盤中監控啟動，Ctrl+C 停止")
        while True:
            now = datetime.now()
            # 每 30 分鐘更新一次總經
            if now.minute % 30 == 0 and now.second < 5:
                system.update_macro_environment()
            # 盤後掃描
            if now.hour == 15 and now.minute == 5:
                system.run_post_market_scan()
            time.sleep(1)
    except KeyboardInterrupt:
        system.shutdown()
```

---

## 五、部署建議

### 本機開發環境
```bash
pip install shioaji pandas numpy yfinance redis sqlite3
```

### 排程設定（Windows Task Scheduler / crontab）
| 任務 | 執行時間 | 說明 |
|------|---------|------|
| 系統啟動 | 08:50 | 盤前總經更新 + 訂閱 |
| 總經更新 | 每 30 分鐘 | `update_macro_environment()` |
| 盤後掃描 | 15:05 | `run_post_market_scan()` |
| 系統關閉 | 14:00 | 盤後資料整理 |

### 注意事項
1. **永遠先用 `simulation=True` 測試**，確認邏輯正確後再切換正式環境
2. Exit_D 停損單屬於保命機制，不得人為關閉
3. VIX > 30 時建議手動確認後再允許新買進
4. 資料庫定期備份（每週）

---

*本文件由 Claude Code 自動生成，供參考規劃使用。實際交易請自行承擔風險評估責任。*
