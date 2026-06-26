# -*- coding: utf-8 -*-
"""隔離測：_revenue_mom_runtime_map 的 latest-rankable 選日（8dab675 cockpit 真驗回空 fix）。

🐛 被修的 bug：旗艦 A4∧月營收 runtime overlay 原本 `latest = max(dmap.keys())`。月營收經
_revenue_factor_series fill-forward 是「逐股獨立」，只要 universe 內任一檔日K較新鮮（資料新鮮度不齊，
個別股多 1~2 個交易日），dmap 就會生出一個僅含極少數股的最新交易日；max() 盲取該稀疏日 → 整批
< _CS_MIN_XSECTION 暖身門檻 → 不排名 → 整個 rank_pct 回空。cockpit live :8766（regime=TREND_UP）真驗：
2026-06-24 僅 3 檔有日K，其餘 177 檔止於 06-22 → A4_revenue 桶＝0、2303/2327 revenue present=False。
（A4 走 _compute_sector_factors 天生需板塊橫斷面，最新日自動收斂到稠密日，故無此症 → 故純鏡像不夠。）

🔑 Reviewer 預警的 stub 盲點：均勻日期的純 stub「永遠不會」踩到稀疏最新日 → 過不了關也測不出 bug。
本檔兩道防線，皆刻意製造「資料新鮮度不齊」這個真實結構：
  test_uneven_freshness_picks_dense_day — 確定性（無 DB）：stub dmap = {稠密日:12檔, 更新的稀疏日:3檔}，
      斷言『選稠密日、排名非空』，且明證 OLD max()-選日邏輯會回空（守住 regression 的命門）。
  test_real_db_nonempty_2303_2327    — 對真 monitor.db+market.db（只讀，_ensure_daily_data no-op）：
      斷言回非空 rank_pct，且 forward id4 選中股 2303/2327 rank_pct≥0.50（與 forward 一致）。DB 缺則 skip。

可直接 `python tests/test_revenue_runtime_map.py` 跑（不依賴 pytest）。全程唯讀、不碰 :8766、不下載。
"""
import os
import sys
from pathlib import Path

_UI = Path(__file__).resolve().parents[1] / "ui"
sys.path.insert(0, str(_UI))
os.chdir(str(_UI))

import backend as B   # noqa: E402

# 全域唯讀化：runtime map 內 _load_backtest_data 會呼 _ensure_daily_data（可能下載/寫庫）→ no-op
B._ensure_daily_data = lambda code, market, start_date, end_date: 0


def _restore(snapshot):
    for name, fn in snapshot.items():
        setattr(B, name, fn)


# ── 防線 1：確定性 — uneven freshness 必選稠密日（精準守住被修的選日邏輯）────────────────
def test_uneven_freshness_picks_dense_day():
    B._REVENUE_RUNTIME_CACHE.clear()
    snap = {n: getattr(B, n) for n in
            ("get_universe", "_load_backtest_data", "_compute_stock_features",
             "_compute_revenue_cs_factors")}
    dense = "2026-06-22"     # 稠密交易日：12 檔（≥ _CS_MIN_XSECTION=10）→ 可排名
    sparse = "2026-06-24"    # 更新但稀疏：3 檔（< 門檻）→ 不可排名；max() 會盲取此日 → 舊邏輯回空
    dense_codes = [f"D{i:02d}" for i in range(12)]
    sparse_codes = ["D00", "D01", "D02"]   # 僅這 3 檔有較新日K（資料新鮮度不齊）
    all_codes = dense_codes
    # dmap[d][code] = raw_val；稠密日 raw 遞增 → 最高值股(D11) rank_pct 應≈1.0
    dmap = {
        dense: {c: float(i) for i, c in enumerate(dense_codes)},
        sparse: {c: 99.0 for c in sparse_codes},
    }
    try:
        B.get_universe = lambda market, refresh=False: {"data": all_codes}
        B._load_backtest_data = lambda codes, mkt, start, end: ([dense, sparse], {})
        B._compute_stock_features = lambda code, all_dates, bar_data: {"dates": [dense, sparse], "c": [1.0, 1.0]}
        B._compute_revenue_cs_factors = lambda mkt, codes, feats, need, **kw: {"revenue_mom": dmap}

        m = B._revenue_mom_runtime_map("TW", asof_date="2026-06-24")
    finally:
        _restore(snap)
        B._REVENUE_RUNTIME_CACHE.clear()

    # 明證 bug 結構存在：max(date) 是稀疏日，且其橫斷面 < 門檻（即舊 max()-邏輯必回空）
    assert max(dmap.keys()) == sparse, "前置：稀疏日必須是最新日才複現 bug 結構"
    assert len(dmap[sparse]) < B._CS_MIN_XSECTION, "前置：最新日橫斷面須 < 門檻"
    # fix 行為：選稠密日、非空排名
    assert m["date"] == dense, f"應選稠密可排名日 {dense}，實得 {m['date']}"
    assert len(m["rank_pct"]) == len(dense_codes) >= B._CS_MIN_XSECTION, \
        f"稠密日全員應排名，實得 {len(m['rank_pct'])}"
    assert m["rank_pct"]["D11"] == 1.0, "稠密日最高 raw 應 rank_pct=1.0"
    assert m["rank_pct"]["D00"] == 0.0, "稠密日最低 raw 應 rank_pct=0.0"
    print("PASS test_uneven_freshness_picks_dense_day "
          f"(date={m['date']}, ranked={len(m['rank_pct'])})")


def test_warmup_no_rankable_day_is_honest_empty():
    """暖身期：所有日皆 < 門檻 → date 標最新日（誠實）、rank_pct 留空（不造假）。"""
    B._REVENUE_RUNTIME_CACHE.clear()
    snap = {n: getattr(B, n) for n in
            ("get_universe", "_load_backtest_data", "_compute_stock_features",
             "_compute_revenue_cs_factors")}
    dmap = {"2026-06-20": {"D00": 1.0, "D01": 2.0},
            "2026-06-22": {"D00": 3.0}}     # 全部 < _CS_MIN_XSECTION
    try:
        B.get_universe = lambda market, refresh=False: {"data": ["D00", "D01"]}
        B._load_backtest_data = lambda codes, mkt, start, end: (["2026-06-20", "2026-06-22"], {})
        B._compute_stock_features = lambda code, all_dates, bar_data: {"dates": ["2026-06-20"], "c": [1.0]}
        B._compute_revenue_cs_factors = lambda mkt, codes, feats, need, **kw: {"revenue_mom": dmap}
        m = B._revenue_mom_runtime_map("TW", asof_date="2026-06-22")
    finally:
        _restore(snap)
        B._REVENUE_RUNTIME_CACHE.clear()
    assert m["date"] == "2026-06-22", "暖身期仍誠實標最新日"
    assert m["rank_pct"] == {}, "暖身樣本不足不得排名（不造假）"
    print("PASS test_warmup_no_rankable_day_is_honest_empty")


# ── 防線 2：對真 DB 跑出非空（補 stub 盲點：真實 universe×真實月營收×真實日K 新鮮度）──────────
def test_real_db_nonempty_2303_2327():
    if not (B.DB_PATH.exists() and B.MARKET_DB_PATH.exists()):
        print("SKIP test_real_db_nonempty_2303_2327 (DB 不存在)")
        return
    B._REVENUE_RUNTIME_CACHE.clear()
    m = B._revenue_mom_runtime_map("TW")   # asof=now，與 cockpit refresh 同呼法
    rp = m.get("rank_pct") or {}
    raw = m.get("raw") or {}
    if not raw:
        # 真 DB 可能在某些環境無月營收/日K → 不誤判，標記 skip（仍由防線 1 守 regression）
        print(f"SKIP test_real_db_nonempty_2303_2327 (真 DB 無月營收資料；n_universe={m.get('n_universe')})")
        return
    assert len(rp) >= B._CS_MIN_XSECTION, \
        f"真 DB 應回非空可排名 rank_pct，實得 {len(rp)}（date={m.get('date')}, n_rev={m.get('n_revenue')}）"
    # forward id4（A4∧revenue_mom）同日選中股應 rank_pct≥0.50（與 forward 一致；非造假門檻）
    for code in ("2303", "2327"):
        v = rp.get(code)
        assert v is not None and v >= B._A4_REVENUE_RANK_THR, \
            f"forward 選中股 {code} rank_pct 應≥{B._A4_REVENUE_RANK_THR}，實得 {v}（date={m.get('date')}）"
    print(f"PASS test_real_db_nonempty_2303_2327 "
          f"(date={m['date']}, ranked={len(rp)}, 2303={rp.get('2303'):.3f}, 2327={rp.get('2327'):.3f})")


def _run_all():
    tests = [test_uneven_freshness_picks_dense_day,
             test_warmup_no_rankable_day_is_honest_empty,
             test_real_db_nonempty_2303_2327]
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
