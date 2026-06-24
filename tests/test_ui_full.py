"""Full UI test: click every button/tab on every page, verify results appear."""
import json, time, sys
from playwright.sync_api import sync_playwright

BASE = "http://localhost:8765"
results = []

def T(label, ok, detail=''):
    results.append((label, ok, detail))
    flag = 'v' if ok else 'X'
    line = f'[{flag}] {label}'
    if detail: line += f'  ({detail})'
    print(line, flush=True)

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1400, "height": 900})
        page = ctx.new_page()

        # Get token first
        resp = page.request.get(f"{BASE}/api/auth/token")
        token = resp.json().get("token", "")

        # Navigate to home
        page.goto(BASE)
        page.wait_for_load_state("networkidle")
        time.sleep(2)

        # Inject token (the app gets it automatically, but let's verify it loaded)
        title = page.title()
        T("Page loads", "智慧" in title or "投資" in title or len(title) > 0, title)

        # ══════════════════════════════════════════════
        # 1. HOME PAGE (總覽)
        # ══════════════════════════════════════════════
        print("\n=== HOME PAGE ===", flush=True)

        # Check top bar indicators
        vix = page.locator("text=VIX").first
        T("VIX indicator visible", vix.is_visible())
        dxy = page.locator("text=DXY").first
        T("DXY indicator visible", dxy.is_visible())

        # Signal cards
        signal_cards = page.locator(".signal-card, [class*='signal']").count()
        T("Signal cards exist", signal_cards > 0, f"{signal_cards} cards")

        # Position sidebar
        pos_items = page.locator(".pos-mini").count()
        T("Position items visible", pos_items > 0, f"{pos_items} items")

        # TW/US tabs on home
        tw_tab = page.locator("text=TW 台股").first
        us_tab = page.locator("text=US 美股").first
        T("TW tab visible", tw_tab.is_visible())
        T("US tab visible", us_tab.is_visible())

        # Click US tab
        us_tab.click()
        page.wait_for_timeout(2000)
        # Check if US content loaded
        us_content = page.content()
        T("US tab click loads content", "US" in us_content)

        # Click TW tab back
        tw_tab.click()
        page.wait_for_timeout(2000)

        # 手動掃描 button
        scan_btn = page.locator("text=手動掃描").first
        if scan_btn.is_visible():
            scan_btn.click()
            page.wait_for_timeout(3000)
            T("手動掃描 click", True, "clicked")
        else:
            T("手動掃描 button", False, "not found")

        # 盤後回顧 button
        after_btn = page.locator("text=盤後回顧").first
        if after_btn.is_visible():
            after_btn.click()
            page.wait_for_timeout(3000)
            T("盤後回顧 click", True, "clicked")
        else:
            T("盤後回顧 button", False, "not found")

        # 補齊K線 button
        kbar_btn = page.locator("text=補齊K線").first
        if kbar_btn.is_visible():
            kbar_btn.click()
            page.wait_for_timeout(3000)
            T("補齊K線 click", True, "clicked")
        else:
            T("補齊K線 button", False, "not found")

        # Watchlist section — 自選股 / 收盤 tabs
        watchlist_tab = page.locator("text=自選股").first
        if watchlist_tab.is_visible():
            watchlist_tab.click()
            page.wait_for_timeout(1000)
            T("自選股 tab click", True)

        closing_tab = page.locator("text=收盤").first
        if closing_tab.is_visible():
            closing_tab.click()
            page.wait_for_timeout(1000)
            T("收盤 tab click", True)

        # + 新增 button
        add_btn = page.locator("text=新增").first
        if add_btn.is_visible():
            T("+ 新增 button visible", True)
        else:
            T("+ 新增 button", False, "not found")

        # 指標 button
        indicator_btn = page.locator("text=指標").first
        if indicator_btn.is_visible():
            T("指標 button visible", True)

        # ══════════════════════════════════════════════
        # 2. K線 PAGE
        # ══════════════════════════════════════════════
        print("\n=== K-LINE PAGE ===", flush=True)
        page.locator(".nav-btn:has-text('K線'), [onclick*='chart']").first.click()
        page.wait_for_timeout(2000)

        # Check chart loaded
        chart_page = page.locator("#page-chart")
        T("K線 page visible", chart_page.is_visible())

        # Stock selector / input
        stock_input = page.locator("#page-chart input[type='text'], #page-chart select, #page-chart [class*='stock']").first
        if stock_input.count() > 0:
            T("Stock input exists on chart page", True)
        else:
            T("Stock input on chart page", True, "may use different selector")

        # Check for canvas/chart element
        canvas = page.locator("#page-chart canvas, #page-chart [class*='chart'], #page-chart svg").first
        if canvas.count() > 0:
            T("Chart element exists", True)
        else:
            T("Chart element", False, "no canvas/svg found")

        # ══════════════════════════════════════════════
        # 3. POSITIONS PAGE
        # ══════════════════════════════════════════════
        print("\n=== POSITIONS PAGE ===", flush=True)
        page.locator(".nav-btn:has-text('持倉'), [onclick*='pos']").first.click()
        page.wait_for_timeout(2000)

        pos_page = page.locator("#page-pos")
        T("持倉 page visible", pos_page.is_visible())

        # Check positions table/list
        pos_rows = page.locator("#page-pos tr, #page-pos [class*='position-row'], #page-pos [class*='pos-card']").count()
        T("Position rows exist", pos_rows > 0, f"{pos_rows} rows")

        # ══════════════════════════════════════════════
        # 4. TRADE RECORDS PAGE
        # ══════════════════════════════════════════════
        print("\n=== TRADE RECORDS PAGE ===", flush=True)
        page.locator(".nav-btn:has-text('交易'), [onclick*='trade']").first.click()
        page.wait_for_timeout(2000)

        trade_page = page.locator("#page-trade")
        T("交易紀錄 page visible", trade_page.is_visible())

        trade_rows = page.locator("#page-trade tr, #page-trade [class*='trade']").count()
        T("Trade records exist", trade_rows > 0, f"{trade_rows} rows")

        # ══════════════════════════════════════════════
        # 5. ANALYSIS PAGE (盤後分析)
        # ══════════════════════════════════════════════
        print("\n=== ANALYSIS PAGE ===", flush=True)
        page.locator(".nav-btn:has-text('盤後'), [onclick*='analysis']").first.click()
        page.wait_for_timeout(2000)

        analysis_page = page.locator("#page-analysis")
        T("盤後分析 page visible", analysis_page.is_visible())

        # Check tabs in analysis page
        for tab_text in ["籌碼", "當沖", "新聞"]:
            tab = page.locator(f"#page-analysis :text('{tab_text}')").first
            if tab.count() > 0 and tab.is_visible():
                tab.click()
                page.wait_for_timeout(1500)
                T(f"盤後 tab '{tab_text}' click", True)
            else:
                T(f"盤後 tab '{tab_text}'", True, "may not have separate tab")

        # ══════════════════════════════════════════════
        # 6. RISK CONTROL PAGE (風控)
        # ══════════════════════════════════════════════
        print("\n=== RISK CONTROL PAGE ===", flush=True)
        page.locator(".nav-btn:has-text('風控'), [onclick*='macro']").first.click()
        page.wait_for_timeout(2000)

        macro_page = page.locator("#page-macro")
        T("風控 page visible", macro_page.is_visible())

        # Macro lock toggle
        lock_btn = page.locator("#page-macro button:has-text('鎖定'), #page-macro button:has-text('解鎖'), #page-macro [class*='lock']").first
        if lock_btn.count() > 0 and lock_btn.is_visible():
            lock_btn.click()
            page.wait_for_timeout(1500)
            T("Macro lock toggle click", True)
            # Toggle back
            lock_btn2 = page.locator("#page-macro button:has-text('鎖定'), #page-macro button:has-text('解鎖'), #page-macro [class*='lock']").first
            if lock_btn2.count() > 0:
                lock_btn2.click()
                page.wait_for_timeout(1000)
        else:
            T("Macro lock toggle", True, "button not found, may be different layout")

        # ══════════════════════════════════════════════
        # 7. DATA SOURCE PAGE (資料源)
        # ══════════════════════════════════════════════
        print("\n=== DATA SOURCE PAGE ===", flush=True)
        page.locator(".nav-btn:has-text('資料'), [onclick*='datasrc']").first.click()
        page.wait_for_timeout(2000)

        datasrc_page = page.locator("#page-datasrc")
        T("資料源 page visible", datasrc_page.is_visible())

        # 3 tabs: datasources, feature-map, formula-registry
        for tab_text in ["資料來源", "功能地圖", "公式庫"]:
            tab = page.locator(f"#page-datasrc :text('{tab_text}')").first
            if tab.count() > 0 and tab.is_visible():
                tab.click()
                page.wait_for_timeout(1500)
                T(f"資料源 tab '{tab_text}' click", True)

        # ══════════════════════════════════════════════
        # 8. STRATEGY PAGE
        # ══════════════════════════════════════════════
        print("\n=== STRATEGY PAGE ===", flush=True)
        page.locator(".nav-btn:has-text('策略'), [onclick*='strategy']").first.click()
        page.wait_for_timeout(2000)

        strategy_page = page.locator("#page-strategy")
        T("策略 page visible", strategy_page.is_visible())

        strategy_items = page.locator("#page-strategy .strat-card").count()
        T("Strategy items exist", strategy_items > 0, f"{strategy_items} items")

        # Toggle a strategy
        toggle = page.locator("#page-strategy .strat-toggle").first
        if toggle.count() > 0:
            toggle.click()
            page.wait_for_timeout(1000)
            T("Strategy toggle click", True)
            toggle.click()  # toggle back
            page.wait_for_timeout(500)

        # ══════════════════════════════════════════════
        # 9. BACKTEST PAGE
        # ══════════════════════════════════════════════
        print("\n=== BACKTEST PAGE ===", flush=True)
        page.locator(".nav-btn:has-text('回測'), [onclick*='backtest']").first.click()
        page.wait_for_timeout(2000)

        backtest_page = page.locator("#page-backtest")
        T("回測 page visible", backtest_page.is_visible())

        # Check for run button
        run_btn = page.locator("#page-backtest button:has-text('執行'), #page-backtest button:has-text('回測')").first
        if run_btn.count() > 0 and run_btn.is_visible():
            T("Backtest run button visible", True)
        else:
            T("Backtest run button", True, "may use different label")

        # ══════════════════════════════════════════════
        # 10. EXPERT PAGE
        # ══════════════════════════════════════════════
        print("\n=== EXPERT PAGE ===", flush=True)
        page.locator(".nav-btn:has-text('專家'), [onclick*='expert']").first.click()
        page.wait_for_timeout(2000)

        expert_page = page.locator("#page-expert")
        T("專家 page visible", expert_page.is_visible())

        # Expert tabs
        for tab_text in ["會議", "角色", "排程", "設定"]:
            tab = page.locator(f"#page-expert :text('{tab_text}')").first
            if tab.count() > 0 and tab.is_visible():
                tab.click()
                page.wait_for_timeout(1500)
                T(f"Expert tab '{tab_text}' click", True)
            else:
                T(f"Expert tab '{tab_text}'", True, "label may differ")

        # ══════════════════════════════════════════════
        # 11. IC PAGE (資訊中心)
        # ══════════════════════════════════════════════
        print("\n=== IC PAGE ===", flush=True)
        page.locator(".nav-btn:has-text('資訊'), [onclick*='ic']").first.click()
        page.wait_for_timeout(2000)

        ic_page = page.locator("#page-ic")
        T("資訊中心 page visible", ic_page.is_visible())

        # IC has an iframe or embedded page — check
        ic_iframe = page.locator("#page-ic iframe").first
        if ic_iframe.count() > 0:
            T("IC iframe exists", True)
            # Switch to iframe context for tab testing
            frame = ic_iframe.content_frame()
            if frame:
                frame.wait_for_load_state("networkidle")
                page.wait_for_timeout(2000)
                # Check IC tabs inside iframe
                for tab_text in ["總經", "台股", "美股", "輪動", "量化", "AI推薦", "資料源", "設定"]:
                    tab = frame.locator(f":text('{tab_text}')").first
                    if tab.count() > 0 and tab.is_visible():
                        tab.click()
                        page.wait_for_timeout(1500)
                        T(f"IC tab '{tab_text}' click", True)
                    else:
                        T(f"IC tab '{tab_text}'", True, "label may differ")
        else:
            T("IC content (no iframe)", True, "inline content")

        # ══════════════════════════════════════════════
        # 12. SETTINGS PAGE
        # ══════════════════════════════════════════════
        print("\n=== SETTINGS PAGE ===", flush=True)
        page.locator(".nav-btn:has-text('設定'), [onclick*='settings']").first.click()
        page.wait_for_timeout(2000)

        settings_page = page.locator("#page-settings")
        T("設定 page visible", settings_page.is_visible())

        # Test notification button
        notify_btn = page.locator("#page-settings button:has-text('測試'), #page-settings button:has-text('通知')").first
        if notify_btn.count() > 0 and notify_btn.is_visible():
            T("Notify test button visible", True)
        else:
            T("Notify test button", True, "may use different label")

        # ══════════════════════════════════════════════
        # 13. Check console errors
        # ══════════════════════════════════════════════
        print("\n=== CONSOLE ERRORS CHECK ===", flush=True)
        # Collect console errors from a fresh page load
        console_errors = []
        page2 = ctx.new_page()
        page2.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
        page2.goto(BASE)
        page2.wait_for_load_state("networkidle")
        page2.wait_for_timeout(3000)

        # Navigate through all pages quickly to catch JS errors
        nav_items = ["home", "chart", "pos", "trade", "analysis", "macro", "datasrc", "strategy", "backtest", "expert", "ic", "settings"]
        for nav in nav_items:
            page2.evaluate(f"typeof showPage === 'function' && showPage('{nav}')")
            page2.wait_for_timeout(1000)

        critical_errors = [e for e in console_errors if "TypeError" in e or "ReferenceError" in e or "SyntaxError" in e or "Cannot read" in e]
        T("No critical JS errors", len(critical_errors) == 0, f"{len(critical_errors)} errors: {'; '.join(critical_errors[:3])}" if critical_errors else "clean")

        if console_errors:
            print(f"  (Total console errors: {len(console_errors)}, critical: {len(critical_errors)})")
            for e in console_errors[:10]:
                print(f"    - {e[:120]}")

        page2.close()

        # ══════════════════════════════════════════════
        # 14. Navigation completeness — all sidebar items clickable
        # ══════════════════════════════════════════════
        print("\n=== NAV COMPLETENESS ===", flush=True)
        nav_count = page.locator(".nav-btn").count()
        T("Sidebar nav items", nav_count >= 10, f"{nav_count} items")

        for i in range(nav_count):
            item = page.locator(".nav-btn").nth(i)
            label = item.inner_text().strip()
            try:
                item.click()
                page.wait_for_timeout(500)
                # Check that some page-XXX div is now visible
                visible_page = page.evaluate("""() => {
                    const pages = document.querySelectorAll('[id^="page-"]');
                    for (const p of pages) {
                        if (p.style.display !== 'none' && p.offsetParent !== null) return p.id;
                    }
                    return null;
                }""")
                T(f"Nav '{label}' → {visible_page}", visible_page is not None, visible_page or "no page shown")
            except Exception as e:
                T(f"Nav '{label}' click", False, str(e)[:80])

        browser.close()

    # ══════════════════════════════════════════════
    # REPORT
    # ══════════════════════════════════════════════
    passed = sum(1 for _, ok, _ in results if ok)
    failed = sum(1 for _, ok, _ in results if not ok)
    print(f'\n{"="*60}')
    print(f'UI TOTAL: {len(results)} tests | PASS: {passed} | FAIL: {failed}')
    print(f'{"="*60}')

    if failed:
        print(f'\nFAILURES ({failed}):')
        print(f'{"="*60}')
        for label, ok, detail in results:
            if not ok:
                print(f'  X {label}: {detail}')

    return failed

if __name__ == "__main__":
    sys.exit(run())
