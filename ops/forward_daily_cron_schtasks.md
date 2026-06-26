# Forward Daily Cron — Windows Task Scheduler 註冊說明

> ⚠️ **本檔只交付指令，不代為註冊。** 實際註冊到系統 + 首次實滾由 cockpit/老闆挑 :8766 空窗執行（spec §3/§7）。
> 排程啟用前請先跑一輪 `--dry-run` 看稽核列，再實跑一輪，最後驗 `forward_cron_runs` 落地。

## 0. 前置
- backend :8766 須在排程觸發時為**已啟動**狀態（cron 跑前會 `GET /api/health`，不 ok 即記 error 退出，不盲打）。
- token 由腳本自 `ui/.api_token` 讀檔；**不要把 token 放進排程指令**。
- Python 路徑：請用實際 `python.exe` 絕對路徑（避免 Task Scheduler 環境找不到 PATH）。
  查路徑：PowerShell `(Get-Command python).Source`

## 1. 先手動驗一次（不註冊，cockpit 空窗做）

```powershell
# (a) 稽核 dry-run：auto-roll dry_run:true、search/run/update 跳過、不寫計分、不計 done
python C:\Users\ychsu\Documents\Claude_Files\smart-investment-monitor-v4\ops\forward_daily_cron.py --dry-run

# (b) 實跑一輪（會寫 ft_picks / 推進計分；確認上面稽核 OK 再做）
python C:\Users\ychsu\Documents\Claude_Files\smart-investment-monitor-v4\ops\forward_daily_cron.py

# (c) 驗落地
#   - 唯讀 API： curl http://127.0.0.1:8766/api/forward/cron/status
#   - 摘要 md：  n8n-claude\state\_forward_cron_<today>.md
```

## 2. 註冊每日排程（台股收盤 13:30 後，建議 14:00 TW）

> 用 `schtasks`。把 `PYTHON_EXE` 換成第 0 節查到的絕對路徑。
> 指令包了一層 cmd `/c` 讓 stdout/err 落 log 檔，方便事後查。

```bat
schtasks /Create /TN "SIM_ForwardDailyCron_TW" /SC DAILY /ST 14:00 /RL LIMITED /F ^
 /TR "cmd /c PYTHON_EXE \"C:\Users\ychsu\Documents\Claude_Files\smart-investment-monitor-v4\ops\forward_daily_cron.py\" >> \"C:\Users\ychsu\Documents\Claude_Files\smart-investment-monitor-v4\ops\_cron_run.log\" 2>&1"
```

PowerShell 版（等價，引號處理較清楚）：

```powershell
$py   = (Get-Command python).Source
$cron = "C:\Users\ychsu\Documents\Claude_Files\smart-investment-monitor-v4\ops\forward_daily_cron.py"
$log  = "C:\Users\ychsu\Documents\Claude_Files\smart-investment-monitor-v4\ops\_cron_run.log"
$tr   = "cmd /c `"$py`" `"$cron`" >> `"$log`" 2>&1"
schtasks /Create /TN "SIM_ForwardDailyCron_TW" /SC DAILY /ST 14:00 /RL LIMITED /F /TR $tr
```

## 3. 操作

```bat
schtasks /Run    /TN "SIM_ForwardDailyCron_TW"     :: 立即手動觸發一次
schtasks /Query  /TN "SIM_ForwardDailyCron_TW" /V /FO LIST   :: 查狀態/上次結果
schtasks /Change /TN "SIM_ForwardDailyCron_TW" /DISABLE      :: 一鍵停用（不刪）
schtasks /Change /TN "SIM_ForwardDailyCron_TW" /ENABLE       :: 重新啟用
schtasks /Delete /TN "SIM_ForwardDailyCron_TW" /F            :: 移除排程
```

## 4. exit code（Task Scheduler「上次執行結果」可判讀）
- `0` = done / skipped(今日已跑) / dryrun
- `1` = partial（部分步驟逾時或部分失敗；已記 run-log，可續觀察）
- `2` = error（health 失敗或全毀；查 `_cron_run.log` + 摘要 md 紅字）

## 5. 旗標速查
| 旗標 | 用途 |
|---|---|
| `--dry-run` | 稽核模式：auto-roll dry_run:true、search/run/update 跳過、不計 done |
| `--force` | 覆寫「今日已 done」守門，強制重跑 |
| `--budget N` | 搜尋 budget（有界，預設 120，硬上限 500） |
| `--run-date YYYY-MM-DD` | 指定 run 日期（補跑用；預設今天） |
| `--market TW` | 目標市場（v1 僅 TW；非 TW 須另帶 `--allow-non-tw`） |
| `--base-url` | backend URL（預設 http://127.0.0.1:8766） |

> US 市場暫不納入（spec §8：等 US-SI 真回補 + F1 跨市場隔離真驗後另開）。
