"""Strategy Factory UI E2E test with Playwright"""
import json, time
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
    log("Page loads", page.title() != "")

    # 2. Click nav-factory button
    nav_btn = page.locator("#nav-factory")
    log("Nav factory button exists", nav_btn.count() > 0)
    nav_btn.click()
    time.sleep(0.5)

    # 3. Check page-factory is visible
    page_factory = page.locator("#page-factory")
    log("Page-factory visible", page_factory.is_visible())

    # 4. Check 4 tabs exist
    tabs = page.locator("#sf-tabs .tf-btn")
    log("4 tabs exist", tabs.count() == 4, f"count={tabs.count()}")

    # 5. Check console tab elements
    codes_input = page.locator("#sf-codes")
    log("Codes input exists", codes_input.count() > 0)

    market_select = page.locator("#sf-market")
    log("Market select exists", market_select.count() > 0)

    start_input = page.locator("#sf-start")
    log("Start date input exists", start_input.count() > 0)

    end_input = page.locator("#sf-end")
    log("End date input exists", end_input.count() > 0)

    direction_select = page.locator("#sf-direction")
    log("Direction select exists", direction_select.count() > 0)

    category_select = page.locator("#sf-category")
    log("Category select exists", category_select.count() > 0)

    num_input = page.locator("#sf-num")
    log("Num strategies input exists", num_input.count() > 0)

    mode_select = page.locator("#sf-mode")
    log("Mode select exists", mode_select.count() > 0)

    start_btn = page.locator("#sf-start-btn")
    log("Start button exists", start_btn.count() > 0)

    # 6. Fill form
    codes_input.fill("AAPL")
    market_select.select_option("US")
    start_input.fill("2024-06-01")
    end_input.fill("2024-09-01")
    num_input.fill("1")
    log("Form filled", True)

    # 7. Click start button
    start_btn.click()
    time.sleep(2)

    # 8. Check progress card appears
    progress_card = page.locator("#sf-progress-card")
    log("Progress card visible", progress_card.is_visible())

    status_el = page.locator("#sf-status")
    status_text = status_el.inner_text() if status_el.count() > 0 else ""
    log("Status shows running", "啟動" in status_text or "執行" in status_text or "running" in status_text.lower(),
        f"status={status_text}")

    stop_btn = page.locator("#sf-stop-btn")
    log("Stop button visible", stop_btn.is_visible())

    # 9. Wait for completion (max 3 min)
    for i in range(90):
        time.sleep(2)
        st = status_el.inner_text() if status_el.count() > 0 else ""
        if "完成" in st or "停止" in st or "completed" in st.lower():
            break

    final_status = status_el.inner_text() if status_el.count() > 0 else "unknown"
    log("Factory completes", "完成" in final_status or "completed" in final_status.lower(),
        f"status={final_status}")

    # 10. Check logs have content
    logs_el = page.locator("#sf-logs")
    logs_text = logs_el.inner_text() if logs_el.count() > 0 else ""
    log("Logs have content", len(logs_text) > 20, f"len={len(logs_text)}")

    # 11. Switch to library tab
    page.evaluate("() => sfSwitchTab('library', document.querySelectorAll('#sf-tabs .tf-btn')[1])")
    time.sleep(1)
    lib_tab = page.locator("#sf-tab-library")
    log("Library tab visible", lib_tab.is_visible())

    # 12. Check strategies grid
    strat_grid = page.locator("#sf-strategies-grid")
    grid_text = strat_grid.inner_text() if strat_grid.count() > 0 else ""
    log("Strategy grid has content", len(grid_text) > 5, f"len={len(grid_text)}")

    # 13. Switch to knowledge tab
    page.evaluate("() => sfSwitchTab('knowledge', document.querySelectorAll('#sf-tabs .tf-btn')[2])")
    time.sleep(1)
    kb_tab = page.locator("#sf-tab-knowledge")
    log("Knowledge tab visible", kb_tab.is_visible())

    # 14. Switch to leaderboard tab
    page.evaluate("() => sfSwitchTab('leaderboard', document.querySelectorAll('#sf-tabs .tf-btn')[3])")
    time.sleep(1)
    lb_tab = page.locator("#sf-tab-leaderboard")
    log("Leaderboard tab visible", lb_tab.is_visible())

    # 15. Start button re-appears after completion
    start_btn_after = page.locator("#sf-start-btn")
    log("Start button re-appears", start_btn_after.is_visible())

    # 16. History section has entries
    history_list = page.locator("#sf-history-list")
    hist_text = history_list.inner_text() if history_list.count() > 0 else ""
    log("History has entries", "Factory" in hist_text or len(hist_text) > 10, f"len={len(hist_text)}")

    # Screenshot
    page.screenshot(path="/c/Users/ychsu/Documents/Claude_Files/smart-investment-monitor/ui/test_sf_screenshot.png", full_page=False)

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
