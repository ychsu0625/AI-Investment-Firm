"""策略實驗室 UI E2E test with Playwright"""
import sys, io, json, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
from playwright.sync_api import sync_playwright

BASE = "http://localhost:8765"
RESULTS = []

def log(name, ok, detail=""):
    status = "PASS" if ok else "FAIL"
    RESULTS.append((name, ok, detail))
    print(f"[{status}] {name}" + (f" - {detail}" if detail else ""))

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()

    # 1. Load page
    page.goto(BASE, wait_until="networkidle", timeout=15000)
    title = page.title()
    log("Page loads", "Smart" in title or len(title) > 0, f"title={title}")

    # 2. Click nav-iter button
    nav_btn = page.locator("#nav-iter")
    log("Nav button exists", nav_btn.count() > 0)
    nav_btn.click()
    time.sleep(0.5)

    # 3. Check page-iter is visible
    page_iter = page.locator("#page-iter")
    visible = page_iter.is_visible()
    log("Page-iter visible after nav click", visible)

    # 4. Check settings card elements
    codes_input = page.locator("#iter-codes")
    log("Codes input exists", codes_input.count() > 0)
    
    market_select = page.locator("#iter-market")
    log("Market select exists", market_select.count() > 0)
    
    start_input = page.locator("#iter-start")
    log("Start date input exists", start_input.count() > 0)
    
    end_input = page.locator("#iter-end")
    log("End date input exists", end_input.count() > 0)
    
    grid_cb = page.locator("#iter-layer-grid")
    log("Grid checkbox exists", grid_cb.count() > 0)
    
    bay_cb = page.locator("#iter-layer-bay")
    log("Bayesian checkbox exists", bay_cb.count() > 0)
    
    ai_cb = page.locator("#iter-layer-ai")
    log("AI checkbox exists", ai_cb.count() > 0)

    # 5. Check strategy checkboxes rendered
    strat_cbs = page.locator(".iter-strat-cb")
    strat_count = strat_cbs.count()
    log("Strategy checkboxes rendered", strat_count > 0, f"count={strat_count}")

    # 6. Check select all / deselect all buttons
    sel_all = page.locator("text=全選")
    log("Select all button", sel_all.count() > 0)

    # 7. Fill form and start iteration
    codes_input.fill("AAPL")
    market_select.select_option("US")
    start_input.fill("2024-06-01")
    end_input.fill("2024-12-01")
    
    # Uncheck bayesian and AI, keep only grid for speed
    if bay_cb.is_checked():
        bay_cb.uncheck()
    if ai_cb.is_checked():
        ai_cb.uncheck()
    if not grid_cb.is_checked():
        grid_cb.check()

    # Select only BUY_A and EXIT_C
    page.evaluate("""() => {
        document.querySelectorAll('.iter-strat-cb').forEach(cb => cb.checked = false);
        document.querySelectorAll('.iter-strat-cb').forEach(cb => {
            if (cb.value === 'BUY_A' || cb.value === 'EXIT_C') cb.checked = true;
        });
    }""")

    # Set max rounds to 1
    max_rounds = page.locator("#iter-max-rounds")
    if max_rounds.count() > 0:
        max_rounds.fill("1")

    # Click start button
    start_btn = page.locator("text=開始迭代")
    log("Start button exists", start_btn.count() > 0)
    start_btn.first.click()
    time.sleep(1)

    # 8. Check live progress panel appears
    status_el = page.locator("#iter-status")
    log("Status element updates", status_el.count() > 0, f"text={status_el.inner_text()[:50] if status_el.count() > 0 else 'N/A'}")

    # 9. Wait for completion (poll)
    for i in range(60):
        time.sleep(2)
        st = page.locator("#iter-status").inner_text() if page.locator("#iter-status").count() > 0 else ""
        if "converged" in st.lower() or "stopped" in st.lower() or "完成" in st or "收斂" in st:
            break
    
    final_status = page.locator("#iter-status").inner_text() if page.locator("#iter-status").count() > 0 else "unknown"
    log("Iteration completes", "running" not in final_status.lower(), f"status={final_status}")

    # 10. Check Sharpe chart has dots
    sharpe_dots = page.locator("#iter-sharpe-chart circle")
    log("Sharpe chart has data points", sharpe_dots.count() > 0, f"dots={sharpe_dots.count()}")

    # 11. Check rounds table has rows
    round_rows = page.locator("#iter-rounds-body tr")
    log("Rounds table has rows", round_rows.count() > 0, f"rows={round_rows.count()}")

    # 12. Check best params displayed
    best_params = page.locator("#iter-best-params")
    bp_text = best_params.inner_text() if best_params.count() > 0 else ""
    log("Best params displayed", len(bp_text) > 5, f"len={len(bp_text)}")

    # 13. Check log panel has content
    log_el = page.locator("#iter-logs")
    log_text = log_el.inner_text() if log_el.count() > 0 else ""
    log("Log panel has content", len(log_text) > 10, f"len={len(log_text)}")

    # 14. Test Apply button
    apply_btn = page.locator("#iter-apply-btn")
    log("Apply button exists", apply_btn.count() > 0)
    if apply_btn.count() > 0:
        apply_btn.first.click()
        time.sleep(1)
        # Check for success alert or message
        # The apply function shows alert, which we can't easily capture in headless
        log("Apply button clickable", True, "clicked without error")

    # 15. Check history section
    history_list = page.locator("#iter-history-list")
    hist_children = page.locator("#iter-history-list > *")
    log("History list has entries", hist_children.count() > 0, f"entries={hist_children.count()}")

    # 16. Test stop button exists
    stop_btn = page.locator("text=停止")
    log("Stop button exists", stop_btn.count() > 0)

    # Screenshot for reference
    page.screenshot(path="/c/Users/ychsu/Documents/Claude_Files/smart-investment-monitor/ui/test_iter_screenshot.png", full_page=False)

    browser.close()

# Summary
passed = sum(1 for _, ok, _ in RESULTS if ok)
total = len(RESULTS)
print(f"\n{'='*50}")
print(f"TOTAL: {passed}/{total} passed")
if passed < total:
    print("FAILURES:")
    for name, ok, detail in RESULTS:
        if not ok:
            print(f"  - {name}: {detail}")
