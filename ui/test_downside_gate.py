"""
R-DOWNSIDE 隔離測（gate-label-conditional-reframe F 節 R-DOWNSIDE + B.4 + A③）
- 不 import 整個 backend（避免 DB/網路/daemon 副作用、不碰 prod :8766）。
- 用 AST 只抽出純函式 + _BASE_RATE_THRESHOLDS，exec 進隔離 namespace 測真實源碼。
命門：①現有非防禦路徑位元級不變(零回歸) ②防禦分支下行判準算對。
用法: python ui/test_downside_gate.py
"""
import ast
import copy
import os
import random
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "backend.py")

WANT_FUNCS = {
    "_summarize_trades", "_extract_row", "_extract_row_routed",
    "_is_defensive_signal", "_bootstrap_ci_low",
    "_defensive_gate_eval", "_defensive_gate_decision",
}
WANT_ASSIGN = {"_BASE_RATE_THRESHOLDS", "_DEFENSIVE_SIGNAL_IDS", "_DEFENSIVE_CORR_MAX"}

with open(SRC, "r", encoding="utf-8") as f:
    tree = ast.parse(f.read(), filename=SRC)

ns = {"__name__": "backend_isolated", "__builtins__": __builtins__}
picked_funcs, picked_assign = set(), set()
mod = ast.Module(body=[], type_ignores=[])
for node in tree.body:
    if isinstance(node, ast.FunctionDef) and node.name in WANT_FUNCS:
        mod.body.append(node); picked_funcs.add(node.name)
    elif isinstance(node, ast.Assign):
        for t in node.targets:
            if isinstance(t, ast.Name) and t.id in WANT_ASSIGN:
                mod.body.append(node); picked_assign.add(t.id)
    elif isinstance(node, ast.AnnAssign):
        if isinstance(node.target, ast.Name) and node.target.id in WANT_ASSIGN:
            mod.body.append(node); picked_assign.add(node.target.id)

missing = (WANT_FUNCS - picked_funcs) | (WANT_ASSIGN - picked_assign)
assert not missing, f"AST 抽取缺漏: {missing}"
exec(compile(mod, SRC, "exec"), ns)

_summarize_trades = ns["_summarize_trades"]
_extract_row = ns["_extract_row"]
_extract_row_routed = ns["_extract_row_routed"]
_defensive_gate_eval = ns["_defensive_gate_eval"]
_defensive_gate_decision = ns["_defensive_gate_decision"]
_bootstrap_ci_low = ns["_bootstrap_ci_low"]

passed = failed = 0
def check(name, cond, detail=""):
    global passed, failed
    if cond:
        passed += 1; print(f"  PASS  {name}")
    else:
        failed += 1; print(f"  FAIL  {name} — {detail}")


# ── 合成資料：含 2022(下行桶) 與非 2022 的逐筆 trade ───────────────────────────
def make_trades(seed=1, n_2022=20, n_other=40):
    rng = random.Random(seed)
    trades = []
    for _ in range(n_other):
        y = rng.choice(["2020", "2021", "2023", "2024"])
        m = f"{rng.randint(1,12):02d}"; d = f"{rng.randint(1,28):02d}"
        ed = f"{y}-{m}-{d}"
        trades.append((ed, ed, round(rng.uniform(-4, 6), 2)))
    for _ in range(n_2022):
        m = f"{rng.randint(1,12):02d}"; d = f"{rng.randint(1,28):02d}"
        ed = f"2022-{m}-{d}"
        trades.append((ed, ed, round(rng.uniform(-3, 5), 2)))
    return trades

# 公平基準 map：每個出現過的日期給一個基準價（entry/exit 同日 → 超額=該筆報酬，簡化但確定）
def make_bench(trades, seed=2):
    rng = random.Random(seed)
    bench = {}
    for e, x, _ in trades:
        for dt in (e, x):
            bench.setdefault(dt, round(rng.uniform(90, 110), 2))
    return bench

trades = make_trades()
bench = make_bench(trades)

# ═══════════════════════════════════════════════════════════════════════════
# 測 1：命門·零回歸 — 非防禦路徑 _extract_row_routed == _extract_row（位元級）
# ═══════════════════════════════════════════════════════════════════════════
random.seed(99)
summ = _summarize_trades(copy.deepcopy(trades), "TW", bench)
row_std = _extract_row(copy.deepcopy(summ), "TW", "BUY_A")
row_routed = _extract_row_routed(copy.deepcopy(summ), "TW", "BUY_A")
check("[零回歸] 非防禦 routed == 標準 _extract_row（位元級）",
      row_routed == row_std, f"routed={row_routed}\nstd={row_std}")
check("[零回歸] 非防禦路徑不混入防禦欄(gate_path/defensive_eval)",
      "gate_path" not in row_routed and "defensive_eval" not in row_routed,
      f"keys={list(row_routed.keys())}")

# ═══════════════════════════════════════════════════════════════════════════
# 測 2：additive key — downside_excess_returns 存在、與既有 avg/n 一致，且既有欄未動
# ═══════════════════════════════════════════════════════════════════════════
de_list = summ.get("downside_excess_returns")
check("[additive] downside_excess_returns 為 list", isinstance(de_list, list), f"{type(de_list)}")
check("[additive] downside_excess_n == len(returns)",
      summ["downside_excess_n"] == len(de_list), f"n={summ['downside_excess_n']} len={len(de_list)}")
if de_list:
    exp_avg = round(sum(de_list)/len(de_list), 2)
    check("[additive] downside_excess_avg == mean(returns)（既有值未被擾動）",
          summ["downside_excess_avg"] == exp_avg, f"avg={summ['downside_excess_avg']} exp={exp_avg}")
# 既有 key 仍齊全（additive 不刪不改既有 schema）
EXISTING_KEYS = {"win_rate_pct","avg_trade_return_pct","win_rate_ci95","avg_return_ci95",
                 "profit_loss_ratio","sharpe_ratio","max_drawdown_pct","subperiod_pos",
                 "subperiod_frac","subperiod_buckets","excess_avg","excess_ci95","excess_n",
                 "info_ratio","downside_excess_avg","downside_excess_n","cs_factor","cs_top_k"}
check("[additive] 既有 summary keys 全保留", EXISTING_KEYS <= set(summ.keys()),
      f"missing={EXISTING_KEYS - set(summ.keys())}")

# 證明：summarize 兩次（同 seed）下行 list 一致，且未引入新 RNG 干擾既有 CI
random.seed(99); summ_a = _summarize_trades(copy.deepcopy(trades), "TW", bench)
random.seed(99); summ_b = _summarize_trades(copy.deepcopy(trades), "TW", bench)
check("[零回歸·RNG] 同 seed 兩次 summarize 既有 CI 一致(無新 RNG 擾動)",
      summ_a["avg_return_ci95"] == summ_b["avg_return_ci95"] and
      summ_a["excess_ci95"] == summ_b["excess_ci95"],
      f"a={summ_a['avg_return_ci95']}/{summ_a['excess_ci95']} b={summ_b['avg_return_ci95']}/{summ_b['excess_ci95']}")

# ═══════════════════════════════════════════════════════════════════════════
# 測 3：bootstrap CI_low 本地 RNG — 確定性 + 不碰全域 RNG
# ═══════════════════════════════════════════════════════════════════════════
vals = [1.2, 0.8, 1.5, 0.9, 2.1, 1.1, 1.8, 0.7, 1.3, 1.6]  # 全正
ci1 = _bootstrap_ci_low(vals, seed=42)
ci2 = _bootstrap_ci_low(vals, seed=42)
check("[CI] 同 seed 確定性", ci1 == ci2, f"{ci1} vs {ci2}")
check("[CI] 全正樣本 → CI_low > 0", ci1[0] is not None and ci1[0] > 0, f"{ci1}")
# 不碰全域 RNG：呼叫前後 random.random() 序列不被擾動
random.seed(7); seq_a = [random.random() for _ in range(3)]
random.seed(7); _bootstrap_ci_low(vals, seed=42); seq_b = [random.random() for _ in range(3)]
check("[CI] 不擾動全域 RNG（本地 Random）", seq_a == seq_b, f"{seq_a} vs {seq_b}")
check("[CI] N<5 → (None,None)", _bootstrap_ci_low([1,2,3], seed=1) == (None, None))

# ═══════════════════════════════════════════════════════════════════════════
# 測 4：防禦分支正確性 — 註冊合成 defensive 訊號，走下行判準
#   monkeypatch namespace 的 _DEFENSIVE_SIGNAL_IDS（_is_defensive_signal 閉包讀同 ns globals）
# ═══════════════════════════════════════════════════════════════════════════
ns["_DEFENSIVE_SIGNAL_IDS"].add("DEF_TEST")

# 4a：強防禦 — 下行桶全正超額、sharpe/IR 正、corr 負 → DEFENSIVE 過
summ_def = copy.deepcopy(summ)
summ_def["downside_excess_returns"] = [1.2, 0.8, 1.5, 0.9, 2.1, 1.1, 1.8, 1.3, 1.6, 1.4]
summ_def["sharpe_ratio"] = 0.9
summ_def["info_ratio"] = 0.5
row_def = _extract_row_routed(summ_def, "TW", "DEF_TEST", corr_with_trend_alpha=-0.12)
check("[防禦·過] status==DEFENSIVE", row_def["status"] == "DEFENSIVE", f"{row_def['status']} / {row_def['reason']}")
check("[防禦·過] pass==True", row_def["pass"] is True, f"{row_def['pass']}")
check("[防禦·過] gate_path==defensive", row_def.get("gate_path") == "defensive")
check("[防禦·過] beta_filter==DEFENSIVE_PASS", row_def.get("beta_filter") == "DEFENSIVE_PASS")
check("[防禦·過] defensive_eval.CI_low>0",
      row_def["defensive_eval"]["downside_excess_ci_low"] is not None and
      row_def["defensive_eval"]["downside_excess_ci_low"] > 0,
      f"{row_def['defensive_eval']}")
check("[防禦·過] corr 透傳 -0.12", row_def["defensive_eval"]["corr_with_trend_alpha"] == -0.12)

# 4b：下行桶全負超額 → CI_low<0 → DEFENSIVE_FAIL（崩盤未保護）
summ_neg = copy.deepcopy(summ)
summ_neg["downside_excess_returns"] = [-1.5, -0.8, -2.1, -1.2, -0.9, -1.7, -1.1, -1.4]
summ_neg["sharpe_ratio"] = 0.5; summ_neg["info_ratio"] = 0.3
row_neg = _extract_row_routed(summ_neg, "TW", "DEF_TEST", corr_with_trend_alpha=-0.1)
check("[防禦·否] 下行全負 → DEFENSIVE_FAIL", row_neg["status"] == "DEFENSIVE_FAIL", f"{row_neg['reason']}")
check("[防禦·否] pass==False", row_neg["pass"] is False)

# 4c：corr 過高（與順勢 alpha 共線）→ FAIL
row_corr = _extract_row_routed(summ_def, "TW", "DEF_TEST", corr_with_trend_alpha=0.85)
check("[防禦·否] corr 0.85>0.30 共線 → FAIL", row_corr["status"] == "DEFENSIVE_FAIL", f"{row_corr['reason']}")

# 4d：未提供 corr → 不否決、標 caveat
row_nocorr = _extract_row_routed(summ_def, "TW", "DEF_TEST")
check("[防禦·過] 未提供 corr 仍過(caveat)", row_nocorr["status"] == "DEFENSIVE",
      f"{row_nocorr['reason']}")
check("[防禦·過] caveat 標示分散價值待驗",
      row_nocorr["defensive_eval"]["corr_with_trend_alpha"] is None and
      "待驗" in row_nocorr["reason"], f"{row_nocorr['reason']}")

# 4e：下行桶 N<5 → 保守不過
summ_thin = copy.deepcopy(summ)
summ_thin["downside_excess_returns"] = [1.0, 2.0, 1.5]
summ_thin["sharpe_ratio"] = 1.0; summ_thin["info_ratio"] = 0.8
row_thin = _extract_row_routed(summ_thin, "TW", "DEF_TEST", corr_with_trend_alpha=-0.1)
check("[防禦·否] 下行桶 N<5 → 保守 FAIL", row_thin["status"] == "DEFENSIVE_FAIL", f"{row_thin['reason']}")

# 4f：Sharpe/IR 皆非正 → FAIL（風險調整後未顯防禦）
summ_nosharpe = copy.deepcopy(summ_def)
summ_nosharpe["sharpe_ratio"] = -0.2; summ_nosharpe["info_ratio"] = -0.1
row_ns = _extract_row_routed(summ_nosharpe, "TW", "DEF_TEST", corr_with_trend_alpha=-0.1)
check("[防禦·否] Sharpe/IR 皆非正 → FAIL", row_ns["status"] == "DEFENSIVE_FAIL", f"{row_ns['reason']}")

# ═══════════════════════════════════════════════════════════════════════════
# 測 5：命門再確認 — 即使有 defensive 註冊，非防禦訊號仍走標準路(位元級不變)
# ═══════════════════════════════════════════════════════════════════════════
row_std2 = _extract_row(copy.deepcopy(summ), "TW", "BUY_B")
row_routed2 = _extract_row_routed(copy.deepcopy(summ), "TW", "BUY_B")
check("[零回歸] DEF 註冊後，非防禦 BUY_B 仍位元級不變",
      row_routed2 == row_std2 and "gate_path" not in row_routed2,
      f"routed={row_routed2}")

print(f"\n{'='*60}\n  R-DOWNSIDE 隔離測: {passed} passed, {failed} failed\n{'='*60}")
sys.exit(1 if failed else 0)
