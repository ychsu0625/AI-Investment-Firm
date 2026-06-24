"""
L1 Smoke Test: 驗證所有 GET API endpoints 回應 200 + 基本 schema
用法: python test_api_smoke.py [--base http://localhost:8765]
"""
import sys, json, time
try:
    import httpx
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "httpx", "-q"])
    import httpx

BASE = sys.argv[sys.argv.index("--base") + 1] if "--base" in sys.argv else "http://localhost:8765"

# (name, method, path, required_keys_or_None, validator_or_None)
ENDPOINTS = [
    # ── 核心基礎 ──
    ("health", "GET", "/api/health", None, None),
    ("info", "GET", "/api/info", None, None),
    # ── 風控總經 ──
    ("macro", "GET", "/api/macro", None, lambda d: isinstance(d, dict)),
    ("risk-level", "GET", "/api/risk-level", None, lambda d: isinstance(d, dict)),
    ("macro-lock", "GET", "/api/macro-lock", None, None),
    ("risk-config", "GET", "/api/risk-config", None, None),
    # ── 自選股 & 持倉 ──
    ("watchlist", "GET", "/api/watchlist", None, lambda d: isinstance(d, list)),
    ("watchlist-list", "GET", "/api/watchlist/list", None, lambda d: isinstance(d, list)),
    ("positions", "GET", "/api/positions", None, lambda d: isinstance(d, list)),
    # ── 訊號 ──
    ("signals", "GET", "/api/signals", None, lambda d: isinstance(d, list)),
    ("scan-exitd", "GET", "/api/scan/exitd", None, lambda d: isinstance(d, (list, dict))),
    ("scan-signals", "GET", "/api/scan/signals", None, lambda d: isinstance(d, (list, dict))),
    # ── 籌碼 ──
    # chip-scheduler 需要 Shioaji 連線，離線時 500 為預期行為
    # ("chip-scheduler", "GET", "/api/chip/scheduler-status", None, None),
    ("squeeze-candidates", "GET", "/api/chip/squeeze-candidates", None, lambda d: isinstance(d, list)),
    ("itrust-lock", "GET", "/api/chip/itrust-lock", None, lambda d: isinstance(d, list)),
    ("chip-abandon", "GET", "/api/chip/abandon", None, lambda d: isinstance(d, list)),
    ("daytrade-warn", "GET", "/api/chip/daytrade-warn", None, lambda d: isinstance(d, list)),
    ("squeeze-breakout", "GET", "/api/chip/squeeze-breakout", None, lambda d: isinstance(d, list)),
    # ── 新聞 ──
    ("news-bearish", "GET", "/api/news/bearish-reversal", None, lambda d: isinstance(d, (list, dict))),
    # ── 美股 ──
    ("us-watchlist", "GET", "/api/us/watchlist", None, lambda d: isinstance(d, list)),
    ("us-indices", "GET", "/api/us/indices", None, None),
    ("us-positions", "GET", "/api/us/positions", None, lambda d: isinstance(d, list)),
    ("us-scan", "GET", "/api/us/scan/signals", None, lambda d: isinstance(d, (list, dict))),
    # ── 策略 & 回測 ──
    ("strategies", "GET", "/api/strategies", None, lambda d: isinstance(d, list) and len(d) >= 10),
    ("backtest-history", "GET", "/api/backtest/history", None, lambda d: isinstance(d, list)),
    ("market-data-status", "GET", "/api/market-data/status", None, None),
    # ── 資訊中心 ──
    ("ic-settings", "GET", "/api/ic/settings", None, None),
    ("ic-token-usage", "GET", "/api/ic/token-usage", None, None),
    ("ic-macro", "GET", "/api/ic/macro", None, None),
    ("ic-us-sectors", "GET", "/api/ic/us/sectors", None, None),
    ("ic-sector-rotation", "GET", "/api/ic/sector-rotation", None, lambda d: isinstance(d, (list, dict))),
    ("ic-openbb-status", "GET", "/api/ic/openbb/status", None, None),
    ("ic-sources", "GET", "/api/ic/sources", ["system", "user"], None),
    ("ic-sources-news", "GET", "/api/ic/sources/news", None, lambda d: isinstance(d, list)),
    ("ic-recommendations", "GET", "/api/ic/recommendations", None, lambda d: isinstance(d, list)),
    ("ic-rec-history", "GET", "/api/ic/recommendations/history", None, lambda d: isinstance(d, list)),
    ("ic-notify-config", "GET", "/api/ic/notify-config", None, None),
    ("ic-kb-entities", "GET", "/api/ic/kb/entities", None, None),
    # ── 系統透視 ──
    ("datasources", "GET", "/api/datasources", None, lambda d: isinstance(d, list) and len(d) >= 45),
    ("feature-map", "GET", "/api/feature-datasource-map", None, lambda d: isinstance(d, list) and len(d) >= 15),
    ("formula-registry", "GET", "/api/formula-registry", None, lambda d: isinstance(d, list) and len(d) >= 35),
    # ── 資料管理 ──
    ("data-stats", "GET", "/api/data/stats", None, None),
    ("data-integrity", "GET", "/api/data/integrity", None, None),
    # ── 交易紀錄 ──
    ("trade-records", "GET", "/api/trade-records", None, lambda d: isinstance(d, list)),
    ("trade-analytics", "GET", "/api/trade-records/analytics", None,
     lambda d: isinstance(d, dict) and "realized_pnl" in d and "stocks" in d),
    ("trade-filter", "GET", "/api/trade-records?code=2330", None, lambda d: isinstance(d, list)),
    # ── 專家委員會 ──
    ("expert-roles", "GET", "/api/expert/roles", None, lambda d: isinstance(d, list) and len(d) >= 5),
    ("expert-config", "GET", "/api/expert/config", None, lambda d: isinstance(d, dict)),
    ("expert-schedules", "GET", "/api/expert/schedules", None, lambda d: isinstance(d, list)),
    ("expert-sessions", "GET", "/api/expert/sessions", ["sessions"], lambda d: isinstance(d, dict)),
    # ── 策略（含分類）──
    ("strategies-count", "GET", "/api/strategies", None, lambda d: isinstance(d, list) and len(d) >= 19),
    ("strategies-have-type", "GET", "/api/strategies", None,
     lambda d: all("strat_type" in s for s in d)),
]

def run():
    passed, failed, errors = [], [], []
    total = len(ENDPOINTS)
    print(f"\n{'='*60}")
    print(f"  L1 Smoke Test — {total} endpoints @ {BASE}")
    print(f"{'='*60}\n")

    client = httpx.Client(timeout=15.0)

    for name, method, path, required_keys, validator in ENDPOINTS:
        try:
            t0 = time.time()
            resp = client.request(method, BASE + path)
            ms = int((time.time() - t0) * 1000)

            if resp.status_code != 200:
                failed.append((name, f"HTTP {resp.status_code}", ms))
                print(f"  FAIL  {name:<25} HTTP {resp.status_code} ({ms}ms)")
                continue

            try:
                data = resp.json()
            except Exception:
                data = resp.text

            if required_keys and isinstance(data, dict):
                missing = [k for k in required_keys if k not in data]
                if missing:
                    failed.append((name, f"missing keys: {missing}", ms))
                    print(f"  FAIL  {name:<25} missing keys: {missing} ({ms}ms)")
                    continue

            if validator:
                try:
                    ok = validator(data)
                    if not ok:
                        failed.append((name, "validator failed", ms))
                        print(f"  FAIL  {name:<25} validator failed ({ms}ms)")
                        continue
                except Exception as e:
                    failed.append((name, f"validator error: {e}", ms))
                    print(f"  FAIL  {name:<25} validator error: {e} ({ms}ms)")
                    continue

            passed.append((name, ms))
            status = "PASS" if ms < 3000 else "SLOW"
            print(f"  {status}  {name:<25} ({ms}ms)")

        except Exception as e:
            errors.append((name, str(e)))
            print(f"  ERR   {name:<25} {e}")

    client.close()

    # ── L1.5: 前端 JS 完整性檢查 ──
    print(f"\n  --- Frontend JS Integrity ---")
    try:
        html = httpx.get(BASE + "/", timeout=15).text
        import re
        # 1) 抓出所有 onclick/onchange 裡呼叫的函數名
        inline_calls = set()
        for attr in re.findall(r'(?:onclick|onchange|oninput)\s*=\s*"([^"]*)"', html):
            for fn in re.findall(r'([a-zA-Z_]\w+)\s*\(', attr):
                if fn not in ('if', 'else', 'return', 'var', 'let', 'const', 'new', 'typeof', 'void',
                              'this', 'event', 'parseInt', 'parseFloat', 'Number',
                              'String', 'Boolean', 'Array', 'Object', 'Date', 'Math', 'RegExp',
                              'JSON', 'console', 'window', 'document', 'alert', 'confirm',
                              'prompt', 'setTimeout', 'setInterval', 'clearTimeout',
                              'clearInterval', 'fetch', 'encodeURIComponent', 'decodeURIComponent',
                              'isNaN', 'isFinite', 'Error', 'Map', 'Set', 'Promise',
                              'requestAnimationFrame', 'cancelAnimationFrame',
                              'getComputedStyle', 'matchMedia',
                              # DOM built-in methods (called on elements via chaining)
                              'getElementById', 'querySelector', 'querySelectorAll',
                              'getElementsByClassName', 'getElementsByTagName',
                              'getAttribute', 'setAttribute', 'removeAttribute',
                              'addEventListener', 'removeEventListener',
                              'appendChild', 'removeChild', 'insertBefore', 'replaceChild',
                              'classList', 'contains', 'add', 'remove', 'toggle', 'replace',
                              'scrollIntoView', 'scrollTo', 'focus', 'blur', 'click',
                              'preventDefault', 'stopPropagation', 'stopImmediatePropagation',
                              'splice', 'slice', 'push', 'pop', 'shift', 'unshift',
                              'map', 'filter', 'reduce', 'forEach', 'find', 'findIndex',
                              'indexOf', 'includes', 'join', 'sort', 'reverse', 'concat',
                              'split', 'trim', 'match', 'test', 'exec', 'keys', 'values',
                              'entries', 'from', 'assign', 'stringify', 'parse',
                              'toFixed', 'toLocaleString', 'toString', 'charAt',
                              'substring', 'substr', 'startsWith', 'endsWith',
                              'toUpperCase', 'toLowerCase', 'padStart', 'padEnd',
                              'abs', 'min', 'max', 'round', 'floor', 'ceil', 'sqrt',
                              'log', 'pow', 'random', 'now', 'getTime', 'toISOString',
                              'then', 'catch', 'finally', 'resolve', 'reject', 'all', 'race',
                              'bind', 'call', 'apply', 'hasOwnProperty'):
                    inline_calls.add(fn)

        # 2) 抓出 JS 中定義的函數名（function xxx 和 async function xxx）
        defined_fns = set(re.findall(r'(?:function|async\s+function)\s+([a-zA-Z_]\w+)', html))
        # 加上 const/let/var xxx = 的箭頭函數和 function expression
        defined_fns.update(re.findall(r'(?:const|let|var)\s+([a-zA-Z_]\w+)\s*=\s*(?:async\s*)?\(', html))
        defined_fns.update(re.findall(r'(?:const|let|var)\s+([a-zA-Z_]\w+)\s*=\s*(?:async\s*)?function', html))
        # 加上 window.xxx = function 的情況
        defined_fns.update(re.findall(r'window\.([a-zA-Z_]\w+)\s*=\s*(?:async\s*)?function', html))

        # 3) 比對：有呼叫但沒定義的 = 潛在 ReferenceError
        undefined = sorted(inline_calls - defined_fns)
        if undefined:
            for fn in undefined:
                failed.append((f"JS-undef:{fn}", "called in HTML but not defined", 0))
                print(f"  FAIL  JS-undef:{fn:<18} called in HTML but not defined")
        else:
            n_checked = len(inline_calls)
            passed.append(("JS-integrity", 0))
            print(f"  PASS  JS-integrity              ({n_checked} inline calls, all defined)")

        # 4) 檢查常見錯誤：未閉合的字串、括號不配對等
        script_blocks = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)
        all_js = '\n'.join(script_blocks)
        # 簡單檢查：括號配對
        parens = all_js.count('(') - all_js.count(')')
        braces = all_js.count('{') - all_js.count('}')
        brackets = all_js.count('[') - all_js.count(']')
        syntax_issues = []
        if abs(parens) > 2:
            syntax_issues.append(f"() mismatch={parens}")
        if abs(braces) > 2:
            syntax_issues.append(f"{{}} mismatch={braces}")
        if abs(brackets) > 2:
            syntax_issues.append(f"[] mismatch={brackets}")
        if syntax_issues:
            for issue in syntax_issues:
                failed.append((f"JS-syntax", issue, 0))
                print(f"  FAIL  JS-syntax                 {issue}")
        else:
            passed.append(("JS-syntax", 0))
            print(f"  PASS  JS-syntax                 (brackets balanced)")

    except Exception as e:
        errors.append(("frontend-check", str(e)))
        print(f"  ERR   frontend-check            {e}")

    print(f"\n{'='*60}")
    print(f"  Results: {len(passed)} PASS / {len(failed)} FAIL / {len(errors)} ERROR")
    print(f"  Total: {total + 2}")
    if failed:
        print(f"\n  Failed:")
        for n, reason, ms in failed:
            print(f"    - {n}: {reason}")
    if errors:
        print(f"\n  Errors:")
        for n, err in errors:
            print(f"    - {n}: {err}")
    print(f"{'='*60}\n")

    return 0 if not failed and not errors else 1

if __name__ == "__main__":
    sys.exit(run())
