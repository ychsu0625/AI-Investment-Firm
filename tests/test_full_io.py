"""Complete I/O functional test for all endpoints and features."""
import json, urllib.request, sys, sqlite3

TOKEN = sys.argv[1] if len(sys.argv) > 1 else ''
DB = r'C:\Users\ychsu\Documents\Claude_Files\smart-investment-monitor\ui\monitor.db'
results = []

def req(method, path, body=None):
    try:
        url = f'http://localhost:8765{path}'
        if body:
            data = json.dumps(body).encode()
            rq = urllib.request.Request(url, data=data, method=method)
            rq.add_header('Content-Type','application/json')
        else:
            rq = urllib.request.Request(url, method=method)
        rq.add_header('X-API-Token', TOKEN)
        with urllib.request.urlopen(rq, timeout=120) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        try: return json.loads(e.read())
        except: return {'_http_error': e.code}
    except Exception as e:
        return {'_error': str(e)}

def T(label, ok, detail=''):
    results.append((label, ok, detail))

def is_ok(d):
    if isinstance(d, list): return True
    return '_http_error' not in d and '_error' not in d and d.get('detail') != 'Not Found'

# ══════════════════════════════════════════════════
# 1. SYSTEM
# ══════════════════════════════════════════════════
d = req('GET','/api/health'); T('health', d.get('ok')==True)
d = req('GET','/api/auth/token'); T('auth/token', bool(d.get('token')))
d = req('GET','/api/info'); T('info', is_ok(d))

# ══════════════════════════════════════════════════
# 2. HOME PAGE
# ══════════════════════════════════════════════════
# 2a. Watchlist CRUD
d = req('GET','/api/watchlist'); T('TW watchlist GET', isinstance(d,list) and len(d)>0, f'{len(d)} stocks')
d = req('GET','/api/watchlist/list'); T('TW watchlist/list', isinstance(d,list))
d = req('POST','/api/watchlist/add/0050'); T('TW watchlist add', d.get('ok')==True)
d = req('GET','/api/watchlist'); T('TW add verify', any(x.get('code')=='0050' for x in d))
d = req('DELETE','/api/watchlist/remove/0050'); T('TW watchlist remove', d.get('ok')==True)
d = req('GET','/api/watchlist'); T('TW remove verify', not any(x.get('code')=='0050' for x in d))

# US watchlist
d = req('GET','/api/us/watchlist'); T('US watchlist GET', isinstance(d,list) and len(d)>0, f'{len(d)} stocks')
d = req('POST','/api/us/watchlist/add/AAPL'); T('US watchlist add', d.get('ok')==True or 'already' in str(d).lower())
d = req('DELETE','/api/us/watchlist/remove/AAPL'); T('US watchlist remove', d.get('ok')==True)

# 2b. Signals
d = req('GET','/api/scan/signals'); T('scan/signals', 'scanned' in d, f'scanned={d.get("scanned")}')
d = req('GET','/api/scan/after-hours'); T('scan/after-hours', 'scanned' in d, f'scanned={d.get("scanned")}')
d = req('GET','/api/scan/exitd'); T('scan/exitd', 'alerts' in d, f'{len(d.get("alerts",[]))} alerts')
d = req('GET','/api/us/scan/signals'); T('US scan/signals', 'scanned' in d)
d = req('GET','/api/signals'); T('signal history', isinstance(d,list), f'{len(d)} entries')

# 2c. Macro indicators
d = req('GET','/api/macro'); T('macro indicators', 'vix' in d, f'vix={d.get("vix")}')
d = req('GET','/api/risk-level'); T('risk-level', 'risk_level' in d, d.get('risk_level'))
d = req('GET','/api/us/indices'); T('US indices', is_ok(d))

# 2d. K-bar warmup
d = req('POST','/api/kbars/warm-up'); T('kbars warmup', 'results' in d, f'{len(d.get("results",[]))} stocks')

# ══════════════════════════════════════════════════
# 3. K-LINE CHART PAGE
# ══════════════════════════════════════════════════
d = req('GET','/api/kbars/2330'); T('kbars/2330 TW daily', 'candles' in d and len(d['candles'])>0, f'{len(d.get("candles",[]))} bars')
d = req('GET','/api/kbars/2330/indicators'); T('kbars indicators', 'rsi' in d, f'rsi={d.get("rsi")}')
d = req('GET','/api/kbars/2330/strategy-markers'); T('strategy markers', isinstance(d, (list,dict)))
d = req('GET','/api/snapshot/2330'); T('snapshot/2330', 'close' in d, f'close={d.get("close")}')
d = req('GET','/api/sparkline/2330'); T('sparkline/2330', isinstance(d,list) and len(d)>0, f'{len(d)} pts')
d = req('GET','/api/vwap/2330'); T('vwap/2330', is_ok(d))
d = req('GET','/api/tick-stats/2330'); T('tick-stats/2330', is_ok(d))
d = req('GET','/api/indicators/batch'); T('indicators/batch', is_ok(d))
d = req('GET','/api/market-data/status'); T('market-data/status', is_ok(d))
d = req('GET','/api/market-data/2330'); T('market-data/2330', is_ok(d))
d = req('GET','/api/us/kbars/TSLA'); T('US kbars/TSLA', 'candles' in d and len(d['candles'])>0, f'{len(d.get("candles",[]))} bars')
d = req('GET','/api/us/snapshot/TSLA'); T('US snapshot/TSLA', 'close' in d or 'price' in d)

# ══════════════════════════════════════════════════
# 4. POSITIONS PAGE
# ══════════════════════════════════════════════════
d = req('POST','/api/positions', {"code":"TEST1","name":"TestPos","shares":5,"cost":100})
T('position create', d.get('ok')==True or d.get('id'))
pid = d.get('id','')
if not pid:
    dd = req('GET','/api/positions')
    pp = [x for x in dd if x.get('code')=='TEST1']
    if pp: pid = pp[0]['id']
d = req('PUT',f'/api/positions/{pid}', {"code":"TEST1","name":"TestEdited","shares":10,"cost":120})
T('position edit', d.get('ok')==True)
dd = req('GET','/api/positions')
pp = [x for x in dd if x.get('code')=='TEST1']
T('position edit verify', pp and pp[0]['shares']==10 and pp[0]['cost']==120)
d = req('GET',f'/api/positions/{pid}/exit-check'); T('position exit-check', is_ok(d))
d = req('PUT',f'/api/positions/{pid}/lifecycle', {"stage":"watching"}); T('position lifecycle', is_ok(d))
d = req('DELETE',f'/api/positions/{pid}'); T('position delete', d.get('ok')==True)
dd = req('GET','/api/positions')
T('position delete verify', not any(x.get('code')=='TEST1' for x in dd))

d = req('GET','/api/us/positions'); T('US positions GET', isinstance(d,(list,dict)))
d = req('POST','/api/us/positions', {"code":"ZZZZ","name":"TestUS","shares":1,"cost":10})
T('US position create', is_ok(d))

# ══════════════════════════════════════════════════
# 5. TRADE RECORDS PAGE
# ══════════════════════════════════════════════════
# BUY with fee verification
d = req('POST','/api/trade-records', {"code":"TEST1","name":"TestTrade","action":"BUY","date":"2026-06-21","shares":3,"price":100,"commission_rate":0.001425,"discount":0.6,"tax_rate":0.003,"market":"TW","note":"fulltest"})
T('trade BUY create', d.get('ok')==True)
T('trade BUY commission=171', d.get('commission')==171.0, f'got {d.get("commission")}')
T('trade BUY tax=0', d.get('tax')==0, f'got {d.get("tax")}')
T('trade BUY net=-300171', d.get('net_amount')==-300171.0, f'got {d.get("net_amount")}')
rid = d.get('id')

# SELL with fee verification
d = req('POST','/api/trade-records', {"code":"TEST1","name":"TestTrade","action":"SELL","date":"2026-06-21","shares":1,"price":110,"commission_rate":0.001425,"discount":0.6,"tax_rate":0.003,"market":"TW","note":"fulltest_sell"})
T('trade SELL create', d.get('ok')==True)
T('trade SELL commission=62.7', d.get('commission')==62.7, f'got {d.get("commission")}')
T('trade SELL tax=330', d.get('tax')==330.0, f'got {d.get("tax")}')
T('trade SELL net=109607.3', abs((d.get('net_amount') or 0)-109607.3)<0.1, f'got {d.get("net_amount")}')
rid2 = d.get('id')

# US trade
d = req('POST','/api/trade-records', {"code":"AAPL","name":"Apple","action":"BUY","date":"2026-06-21","shares":10,"price":200,"commission_rate":0.001425,"discount":0.6,"tax_rate":0,"market":"US","note":"fulltest_us"})
T('trade US BUY', d.get('ok')==True)
us_comm = 200*10*0.001425*0.4
T('trade US commission', d.get('commission')==round(us_comm,2), f'got {d.get("commission")} expect {round(us_comm,2)}')
rid3 = d.get('id')

# Edit trade
d = req('PUT',f'/api/trade-records/{rid}', {"code":"TEST1","name":"TestTrade","action":"BUY","date":"2026-06-20","shares":5,"price":90,"commission_rate":0.001425,"discount":0.6,"tax_rate":0.003,"market":"TW","note":"edited"})
T('trade edit', d.get('ok')==True)
T('trade edit commission=256.5', d.get('commission')==256.5, f'got {d.get("commission")}')

# Analytics
d = req('GET','/api/trade-records/analytics'); T('trade analytics', 'total_records' in d, f'{d.get("total_records")} records')

# Filter
d = req('GET','/api/trade-records?code=TEST1')
records = d if isinstance(d,list) else d.get('records',d.get('trades',[]))
T('trade filter by code', isinstance(records,list) and len(records)>=2, f'{len(records) if isinstance(records,list) else "?"} records')

# Delete
d = req('DELETE',f'/api/trade-records/{rid}'); T('trade delete 1', d.get('ok')==True)
d = req('DELETE',f'/api/trade-records/{rid2}'); T('trade delete 2', d.get('ok')==True)
d = req('DELETE',f'/api/trade-records/{rid3}'); T('trade delete US', d.get('ok')==True)

# Migrate
d = req('POST','/api/trade-records/migrate-positions'); T('trade migrate', d.get('ok')==True, f'migrated={d.get("migrated")}')

# ══════════════════════════════════════════════════
# 6. AFTER-HOURS ANALYSIS PAGE
# ══════════════════════════════════════════════════
d = req('GET','/api/chip/squeeze-candidates'); T('chip squeeze candidates', is_ok(d))
d = req('GET','/api/chip/squeeze-breakout'); T('chip squeeze breakout', is_ok(d))
d = req('GET','/api/chip/itrust-lock'); T('chip itrust lock', is_ok(d))
d = req('GET','/api/chip/abandon'); T('chip abandon', is_ok(d))
d = req('GET','/api/chip/2330'); T('chip/2330 detail', is_ok(d))
d = req('GET','/api/chip/daytrade-warn'); T('chip daytrade warn', is_ok(d))
d = req('GET','/api/chip/daytrade/2330'); T('chip daytrade/2330', is_ok(d))
d = req('POST','/api/chip/fetch'); T('chip fetch', is_ok(d))
d = req('POST','/api/chip/fetch-daytrade'); T('chip fetch-daytrade', is_ok(d))
d = req('GET','/api/chip/scheduler-status'); T('chip scheduler status', is_ok(d))
d = req('POST','/api/chip/scheduler/toggle/true'); T('chip scheduler on', is_ok(d))
d = req('POST','/api/chip/scheduler/toggle/false'); T('chip scheduler off', is_ok(d))

# News
d = req('GET','/api/news/2330'); T('news/2330', is_ok(d))
d = req('GET','/api/news/bearish-reversal'); T('news bearish-reversal', is_ok(d))
d = req('POST','/api/news/fetch-material'); T('news fetch-material', is_ok(d))

# ══════════════════════════════════════════════════
# 7. RISK CONTROL PAGE
# ══════════════════════════════════════════════════
d = req('GET','/api/macro-lock'); T('macro-lock status', 'locked' in d)
d = req('POST','/api/macro-lock/true'); T('macro-lock ON', d.get('ok')==True)
d = req('GET','/api/risk-level')
T('risk-level with lock', d.get('macro_lock')==True, f'macro_lock={d.get("macro_lock")}')
d = req('POST','/api/macro-lock/false'); T('macro-lock OFF', d.get('ok')==True)
d = req('GET','/api/risk-config'); T('risk-config GET', is_ok(d))
d = req('POST','/api/risk-config', {"key":"stop_loss_wave","value":"5"})
T('risk-config POST', is_ok(d))

# ══════════════════════════════════════════════════
# 8. SYSTEM INSIGHT PAGE (3 tabs)
# ══════════════════════════════════════════════════
d = req('GET','/api/datasources'); T('tab1: datasources', isinstance(d,list) and len(d)>0, f'{len(d)} sources')
d = req('GET','/api/feature-datasource-map'); T('tab2: feature map', isinstance(d,list) and len(d)>0, f'{len(d)} features')
d = req('GET','/api/formula-registry'); T('tab3: formula registry', isinstance(d,list) and len(d)>30, f'{len(d)} formulas')
d = req('POST','/api/formula-registry/params', {"id":"tech_kd","params":{"kd_period":9}})
T('formula params update', is_ok(d))
d = req('POST','/api/formula-registry/reset'); T('formula reset', is_ok(d))

# ══════════════════════════════════════════════════
# 9. STRATEGY PAGE
# ══════════════════════════════════════════════════
d = req('GET','/api/strategies'); T('strategies list', isinstance(d,list) and len(d)>10, f'{len(d)} strategies')
strats = d if isinstance(d,list) else []
if strats:
    sid = strats[0].get('id','BUY_A')
    d = req('PUT',f'/api/strategies/{sid}/toggle', {"enabled":False})
    T('strategy toggle off', d.get('ok')==True)
    d = req('GET','/api/strategies')
    s = [x for x in d if x['id']==sid]
    T('strategy toggle verify', s and s[0]['enabled']==False)
    d = req('PUT',f'/api/strategies/{sid}/toggle', {"enabled":True})
    T('strategy toggle back on', d.get('ok')==True)
    d = req('PUT',f'/api/strategies/{sid}/params', {"params":{"test_p":1}})
    T('strategy params update', d.get('ok')==True)

# ══════════════════════════════════════════════════
# 10. BACKTEST PAGE
# ══════════════════════════════════════════════════
d = req('POST','/api/backtest/run', {"symbols":"2330","market":"TW","start_date":"2026-03-01","end_date":"2026-06-15","initial_capital":1000000,"commission_discount":0.6,"strategies":["BUY_A","EXIT_D"]})
T('backtest run', 'summary' in d and 'trades' in d, f'trades={len(d.get("trades",[]))}')
d = req('GET','/api/backtest/history'); T('backtest history', isinstance(d,list), f'{len(d)} entries')
if isinstance(d,list) and d:
    bt_id = d[0].get('id',1)
    d2 = req('GET',f'/api/backtest/{bt_id}'); T('backtest detail', is_ok(d2))
d = req('POST','/api/backtest/walk-forward', {"symbols":"2330","market":"TW"})
T('backtest walk-forward', is_ok(d))

# ══════════════════════════════════════════════════
# 11. EXPERT PAGE (4 tabs)
# ══════════════════════════════════════════════════
d = req('GET','/api/expert/sessions'); T('tab1: expert sessions', isinstance(d.get('sessions',d),(list,dict)))
sessions = d.get('sessions',d) if isinstance(d,dict) else d
if isinstance(sessions,list) and sessions:
    esid = sessions[0].get('id',1)
    d2 = req('GET',f'/api/expert/sessions/{esid}')
    T('expert session detail', is_ok(d2))

d = req('GET','/api/expert/roles'); T('tab2: expert roles', isinstance(d,list) and len(d)>=5, f'{len(d)} roles')
d = req('GET','/api/expert/config'); T('tab4a: expert config GET', 'default_rounds' in d)
d = req('POST','/api/expert/config', {"ai_source":"subscription","ai_model":"claude-haiku-4-5-20251001"})
T('tab4b: expert config POST', is_ok(d))
d = req('GET','/api/expert/schedules'); T('tab3: expert schedules', isinstance(d,list), f'{len(d)} schedules')
if isinstance(d,list) and d:
    sch_id = d[0].get('id',1)
    d2 = req('PUT',f'/api/expert/schedules/{sch_id}', {"enabled":False})
    T('expert schedule toggle off', is_ok(d2))
    req('PUT',f'/api/expert/schedules/{sch_id}', {"enabled":True})

# ══════════════════════════════════════════════════
# 12. IC PAGE (9 tabs)
# ══════════════════════════════════════════════════
# tab: Overview
try:
    rq = urllib.request.Request('http://localhost:8765/api/ic/info_center')
    rq.add_header('X-API-Token', TOKEN)
    with urllib.request.urlopen(rq, timeout=10) as r:
        ct = r.headers.get('Content-Type','')
        T('IC tab-overview', 'text/html' in ct, f'serves HTML SPA ({ct})')
except Exception as e:
    T('IC tab-overview', False, str(e))

# tab: Macro
d = req('GET','/api/ic/macro'); T('IC tab-macro GET', is_ok(d))
d = req('GET','/api/ic/macro/interpretation'); T('IC macro interpretation', 'environment' in d or 'label' in d)
d = req('POST','/api/ic/macro/refresh'); T('IC macro refresh', is_ok(d))

# tab: TW
d = req('GET','/api/ic/tw/institutional-top'); T('IC tab-TW institutional', isinstance(d,list) and len(d)>0, f'{len(d)}')

# tab: US
d = req('GET','/api/ic/us/sectors'); T('IC tab-US sectors', is_ok(d))

# tab: Sector rotation
d = req('GET','/api/ic/sector-rotation'); T('IC tab-rotation', 'sectors' in d and len(d['sectors'])>0, f'{len(d.get("sectors",[]))}')

# tab: Quant tools
d = req('POST','/api/ic/multi-factor', {"codes":["2330"],"market":"TW"})
T('IC multi-factor', is_ok(d))
d = req('POST','/api/ic/factor-generate', {"code":"2330","market":"TW"})
T('IC factor-generate', is_ok(d))
d = req('POST','/api/ic/factor-ic', {"code":"2330","market":"TW"})
T('IC factor-ic', is_ok(d))
d = req('GET','/api/ic/factors/2330'); T('IC factors/2330', is_ok(d))
d = req('GET','/api/ic/options/TSLA'); T('IC options/TSLA', is_ok(d))
d = req('GET','/api/ic/crypto/BTC'); T('IC crypto/BTC', is_ok(d))
d = req('GET','/api/ic/events/2330'); T('IC events/2330', is_ok(d))
d = req('GET','/api/ic/social-sentiment/2330'); T('IC social-sentiment', is_ok(d))
d = req('POST','/api/ic/auto-quant', {}); T('IC auto-quant', is_ok(d))

# tab: AI Recommendations
d = req('GET','/api/ic/recommendations')
recs = d.get('data', d.get('recommendations',[])) if isinstance(d,dict) else d
tw_recs = [r for r in recs if r.get('market')=='TW']
us_recs = [r for r in recs if r.get('market')=='US']
T('IC recs GET', len(recs)>0, f'total={len(recs)}')
T('IC recs TW exist', len(tw_recs)>0, f'{len(tw_recs)} TW')
T('IC recs US exist', len(us_recs)>0, f'{len(us_recs)} US')

# Market isolation test
d = req('POST','/api/ic/recommendations/refresh', {"market":"TW"})
T('IC recs refresh TW', d.get('ok')==True, f'count={d.get("count")}')
d2 = req('GET','/api/ic/recommendations')
recs2 = d2.get('data', d2.get('recommendations',[])) if isinstance(d2,dict) else d2
us_after = [r for r in recs2 if r.get('market')=='US']
T('IC market isolation (US preserved)', len(us_after)>=len(us_recs), f'before={len(us_recs)} after={len(us_after)}')

# Batch score with indicator check
d = req('POST','/api/ic/batch-score', {"stocks":[{"code":"2330","market":"TW"}],"use_ai":False})
T('IC batch-score', len(d.get('results',[]))>0)
if d.get('results'):
    r = d['results'][0]
    T('IC score has KD', 'KD' in r.get('indicators',{}))
    T('IC score has MACD', 'MACD' in r.get('indicators',{}))
    T('IC score has RS', 'RS' in r.get('indicators',{}))
    T('IC score has OBV', 'OBV' in r.get('indicators',{}))
    T('IC score has MFI', 'MFI' in r.get('indicators',{}))
    T('IC score direction valid', r.get('direction') in ('BUY','SELL','HOLD'), r.get('direction'))
    T('IC score 0-100', 0<=r.get('score',0)<=100, f'score={r.get("score")}')
    T('IC confidence 0-1', 0<=r.get('confidence',0)<=1, f'conf={r.get("confidence")}')

# AI analyze (test endpoint responds)
d = req('POST','/api/ic/analyze', {"code":"2330","market":"TW","source":"subscription"})
T('IC analyze endpoint', is_ok(d))

# Backtest preview
d = req('POST','/api/ic/backtest-preview', {"code":"2330","market":"TW"})
T('IC backtest-preview', is_ok(d))

# History + evaluate
d = req('GET','/api/ic/recommendations/history')
_hist = d.get('data', d) if isinstance(d, dict) else d
T('IC rec history', isinstance(_hist, list) and len(_hist) > 0, f'{len(_hist)} entries')
if isinstance(_hist, list) and _hist:
    d = _hist
    T('IC history has market field', 'market' in d[0], d[0].get('market'))
    T('IC history has outcome field', 'outcome' in d[0], d[0].get('outcome'))
    T('IC history has entry_price', 'entry_price' in d[0])
d = req('POST','/api/ic/recommendations/evaluate')
T('IC evaluate', is_ok(d))

# Sentiment
d = req('GET','/api/ic/sentiment-history/2330'); T('IC sentiment history', is_ok(d))

# tab: Data sources
d = req('GET','/api/ic/sources'); T('IC sources GET', is_ok(d))
sys_src = d.get('system',[]) if isinstance(d,dict) else []
T('IC system sources >0', len(sys_src)>0, f'{len(sys_src)}')
d = req('POST','/api/ic/sources/text', {"name":"fulltest_src","content":"Test content","tags":["test"]})
T('IC add text source', d.get('ok')==True or d.get('id'))
d = req('POST','/api/ic/sources', {"name":"fulltest_custom","type":"rss","url":"https://example.com/feed","tags":["test"]})
T('IC add custom source', is_ok(d))
d = req('POST','/api/ic/sources/fetch-all'); T('IC sources fetch-all', is_ok(d))
d = req('GET','/api/ic/sources/news'); T('IC sources/news', is_ok(d))
d = req('GET','/api/ic/kb/search?q=test'); T('IC kb search', is_ok(d))
d = req('GET','/api/ic/kb/entities'); T('IC kb entities', is_ok(d))
d = req('GET','/api/ic/openbb/status'); T('IC openbb status', is_ok(d))

# tab: IC Settings
d = req('GET','/api/ic/settings'); T('IC settings GET', 'model_stock_analyze' in d)
d = req('POST','/api/ic/settings', {"recommendation_count":10}); T('IC settings POST', is_ok(d))
d = req('GET','/api/ic/notify-config'); T('IC notify-config GET', is_ok(d))
d = req('POST','/api/ic/notify-config', {"ic_notify_enabled":True,"ic_notify_threshold":0.7})
T('IC notify-config POST', is_ok(d))
d = req('GET','/api/ic/token-usage'); T('IC token-usage', 'today_tokens' in d)

# ══════════════════════════════════════════════════
# 13. SETTINGS PAGE
# ══════════════════════════════════════════════════
d = req('POST','/api/notify/test', {"channel":"telegram"}); T('notify test', is_ok(d))
d = req('GET','/api/auto-sell/status'); T('auto-sell status', is_ok(d))
d = req('POST','/api/auto-sell/toggle/true'); T('auto-sell toggle on', is_ok(d))
d = req('POST','/api/auto-sell/toggle/false'); T('auto-sell toggle off', is_ok(d))
d = req('POST','/api/auto-sell/toggle-exitc/true'); T('auto-sell exitc on', is_ok(d))
d = req('POST','/api/auto-sell/toggle-exitc/false'); T('auto-sell exitc off', is_ok(d))
d = req('POST','/api/auto-sell/execute'); T('auto-sell execute scan', is_ok(d))
d = req('POST','/api/auto-sell/exit-c'); T('auto-sell exit-c scan', is_ok(d))

# GitHub (read-only)
d = req('GET','/api/github/status'); T('github status', is_ok(d))
d = req('GET','/api/github/watch'); T('github watch list', is_ok(d))

# Data management
d = req('GET','/api/data/stats'); T('data stats', is_ok(d))
d = req('GET','/api/data/integrity'); T('data integrity', is_ok(d))
d = req('POST','/api/data/cleanup', {"dry_run":True}); T('data cleanup (dry)', is_ok(d))

# ══════════════════════════════════════════════════
# 14. FRONTEND PAGE IDs
# ══════════════════════════════════════════════════
try:
    with urllib.request.urlopen('http://localhost:8765/') as r:
        html = r.read().decode('utf-8', errors='replace')
    expected_pages = ['page-home','page-chart','page-pos','page-trade','page-analysis',
                      'page-macro','page-datasrc','page-strategy','page-backtest',
                      'page-expert','page-ic','page-settings']
    for pid in expected_pages:
        found = f'id="{pid}"' in html
        T(f'DOM {pid}', found)

    # Verify showPage calls match
    import re
    show_calls = set(re.findall(r"showPage\('([^']+)'\)", html))
    for call in show_calls:
        T(f'showPage({call}) has target', f'id="page-{call}"' in html, call)
except Exception as e:
    T('frontend HTML check', False, str(e))

# ══════════════════════════════════════════════════
# CLEANUP
# ══════════════════════════════════════════════════
try:
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("DELETE FROM trade_records WHERE note LIKE '%fulltest%'")
    cur.execute("DELETE FROM positions WHERE code IN ('TEST1','ZZZZ')")
    cur.execute("DELETE FROM ic_news_sources WHERE name LIKE 'fulltest%'")
    conn.commit()
    conn.close()
except: pass

# ══════════════════════════════════════════════════
# REPORT
# ══════════════════════════════════════════════════
passed = sum(1 for _,ok,_ in results if ok)
failed = sum(1 for _,ok,_ in results if not ok)
print(f'\n{"="*60}')
print(f'TOTAL: {len(results)} tests | PASS: {passed} | FAIL: {failed}')
print(f'{"="*60}\n')

for label, ok, detail in results:
    flag = 'v' if ok else 'X'
    line = f'[{flag}] {label}'
    if detail: line += f'  ({detail})'
    print(line)

if failed:
    print(f'\n{"="*60}')
    print(f'FAILURES ({failed}):')
    print(f'{"="*60}')
    for label, ok, detail in results:
        if not ok: print(f'  X {label}: {detail}')
