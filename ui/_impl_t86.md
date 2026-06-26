# T86 籌碼回補 — 實作日誌（tool team）

> 抓取/解析層：`_fetch_twse_institutional`(T86 三大法人) + `_fetch_twse_margin`(MI_MARGN 融資券)
> → `_twse_fetch_with_guard`(限流+退避) → `_chip_backfill_*`(回補框架) → `_upsert_chip_snapshot` → `chip_snapshot`。
> 前序輪（parser 19 欄對位、MI_MARGN 16 欄對位、彙總列跳列、2330 cross-check 18/18）記於 commit `abf3b54`。

---

## 307 轉址修復輪（2026-06-25）

### 案發
cockpit 真實跑全量回補(2020–2026)，**2020 年份大量失敗，錯誤＝`HTTP Error 307: Temporary Redirect`**；2024+ 近期日期正常。

### 根因（實測查清，非臆測）
- **不是 parser、不是舊日期端點搬移。** 直打 `https://www.twse.com.tw/rwd/zh/fund/T86?date=20200204&...`：
  - 單打 → **HTTP 200**；`curl -D-` 看 header → 200、無 Location；
  - **40 併發 hammer 2020 日期 → 全部 200**（無法在本機複現 307）。
  證明 307 是**負載觸發的暫時性節流**，端點/路徑/參數皆未變（http 已是 https）。
- **307 轉去哪？→ 哪兒都沒去。** Python 3.12 的 `urllib.request.HTTPRedirectHandler` **本來就會跟隨**帶 `Location` 的 307/308（已驗 `http_error_307`/`http_error_308` 皆存在）。urllib 之所以「上拋」而非「跟隨」，唯一可能是 **TWSE 的節流 307 不帶 `Location` header**（WAF/CDN 對高併發 IP 的暫時擋）：無 Location → redirect handler 回 None → 落到 `http_error_default` → 原樣丟 `HTTPError 307`，reason 仍為 "Temporary Redirect"（與案發訊息完全吻合，且無 "not allowed" 字樣＝排除「轉址被擋」）。
- **為何「2020 大量、2024 正常」**：thread pool 把 dates 由舊到新排入，2020 在隊首被最密集 hammer → 撞上節流窗；之後退避把有效速率壓低，2024 走在較慢節奏 → 正常。

### 修法（最小修，只動抓取/轉址層）
新增 TWSE 抓取共用層（`backend.py` Phase 3 開頭）：
1. `_TwseRedirectHandler`：**帶 `Location` 的 307/308 → 沿用 urllib 既有邏輯跟隨到目標 URL**（防 TWSE 日後真的搬端點）；**無 `Location` 的 307/308（節流）→ 上拋 `_TwseThrottle`**（可重試型別）。`http_error_308 = http_error_307` 別名。
2. `_twse_urlopen(url)`：單一入口，帶 UA、走上面 opener、把 `307/308/403/429/5xx` 統一轉 `_TwseThrottle`（非節流錯誤如 404 原樣拋，不浪費重試）。`_fetch_twse_institutional` / `_fetch_twse_margin` 改呼叫它（**僅換抓取 3 行，parser 對位完全不動**）。
3. `_twse_fetch_with_guard`：退避加 **jitter** + 重試 4→6 次、上限 30→45s（`1.5/3/6/12/24s + 0~1.5s 抖動`）。抖動化解 thread pool 重試對齊的 thundering-herd，足以撐過 TWSE 暫時節流窗。
   `_TwseThrottle` 經 `raise_on_error=True` 上拋 → guard 視為「該退避重試」；genuine no-data(節假日)仍回 `[]` 不重試。

未動：parser 欄位對位、彙總列跳列、回補框架(date loop / thread pool / upsert)、其他 TWSE 端點(TWTB4U)。零回歸。

### 真實驗證（直打真實 TWSE，非 mock；AST 抽出已修函式原始碼執行）
| 日期 | inst 檔數 | margin 檔數 | 307? |
|---|---|---|---|
| **2020-02-04**（案發年） | 1017 | 938 | ✅ 無 |
| **2022-05-04** | 1069 | 952 | ✅ 無 |
| **2024-06-03**（零回歸） | 1199 | 987 | ✅ 無 |

**2330 台積電 cross-check vs TWSE 官方 `三大法人合計` 欄(row[18])：**
- **2020-02-04**：外資 `6,327,466`（row[4] 6,327,466 + row[7] 0）、投信 `-306,000`、自營 `-1,557,994`（row[14] -711,000 + row[17] -846,994）→ 加總 `4,463,472` **== TWSE 官方合計 4,463,472 ✅**
- **2024-06-03**：外資 `7,151,154`、投信 `220,817`、自營 `1,050,307` → 加總 `8,422,278` **== TWSE 官方合計 8,422,278 ✅**

**handler 單元驗證**：①無 Location 的 307 → raise `_TwseThrottle`（會被 guard 重試）✅ ②`http_error_308 is http_error_307`（別名生效）✅ ③帶 Location 的 307 → 委派 stock `http_error_302` 跟隨（外部 httpbin 本機不可達，但此分支即 Python 原生未改邏輯）。

**語法雙驗**：`py_compile` + `ast.parse` 皆 OK。

### 交接給 cockpit
未重啟 :8766、未 commit/git（依約束）。cockpit 重啟後再真打複驗 + 重跑全量回補即生效。
