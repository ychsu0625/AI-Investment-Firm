"""Strategy Factory LLM Queue Agent — polls queue, generates strategies, responds."""
import json, urllib.request, time, sys

token = open(r"C:\Users\ychsu\Documents\Claude_Files\smart-investment-monitor\ui\.api_token").read().strip()
base = "http://localhost:8765"
headers = {"X-API-Token": token, "Content-Type": "application/json"}
session_id = int(sys.argv[1]) if len(sys.argv) > 1 else 11

STRATS = [
    {
        "name": "Volatility Contraction Breakout",
        "code": """
def evaluate(ctx):
    price = ctx.get('price', 0)
    history = ctx.get('history', [])
    indicators = ctx.get('indicators', {})
    if len(history) < 40:
        return {'signal': 0, 'strength': 0, 'reason': 'Insufficient data'}
    atr = indicators.get('atr', 0)
    ma20 = indicators.get('ma20', price)
    rsi = indicators.get('rsi', 50)
    vol = ctx.get('volume', 0)
    avg_vol = sum(h.get('volume', 0) for h in history[-20:]) / 20 if history else 1
    vol_ratio = vol / avg_vol if avg_vol > 0 else 1.0
    atr_ratio = atr / price if price > 0 else 0
    squeeze = atr_ratio < 0.015
    highs = [h.get('high', 0) for h in history[-20:]]
    breakout = price > max(highs) if highs else False
    score = 0
    reasons = []
    if squeeze:
        score += 2; reasons.append('ATR squeeze')
    if breakout:
        score += 2.5; reasons.append('20d breakout')
    if vol_ratio > 1.8:
        score += 2; reasons.append('Vol explosion')
    if price > ma20:
        score += 1; reasons.append('Above MA20')
    if 40 < rsi < 75:
        score += 0.5
    if score >= 5:
        return {'signal': 1, 'strength': min(1.0, score/8.0), 'reason': '; '.join(reasons)}
    return {'signal': 0, 'strength': 0, 'reason': 'No squeeze breakout'}
""",
        "meta": "ATR squeeze + 20d breakout + volume explosion",
        "cat": "technical",
        "sigs": "ATR, BB, Volume, MA20, RSI",
    },
    {
        "name": "Mean Reversion RSI Divergence",
        "code": """
def evaluate(ctx):
    price = ctx.get('price', 0)
    history = ctx.get('history', [])
    indicators = ctx.get('indicators', {})
    if len(history) < 30:
        return {'signal': 0, 'strength': 0, 'reason': 'Insufficient data'}
    rsi = indicators.get('rsi', 50)
    bb_lower = indicators.get('bb_lower', price * 0.98)
    bb_mid = indicators.get('bb_mid', indicators.get('ma20', price))
    closes = [h.get('close', 0) for h in history[-14:]]
    if len(closes) < 5:
        return {'signal': 0, 'strength': 0, 'reason': 'Not enough closes'}
    price_low = closes[-1] <= min(closes[:-1])
    bullish_div = price_low and rsi > 35 and rsi < 45
    near_bb = price <= bb_lower * 1.02
    gap = (bb_mid - price) / bb_mid if bb_mid > 0 else 0
    score = 0
    reasons = []
    if bullish_div:
        score += 3; reasons.append('RSI divergence')
    if near_bb:
        score += 2; reasons.append('Near BB lower')
    if gap > 0.03:
        score += 1.5; reasons.append('Mean gap')
    if 25 < rsi < 40:
        score += 1; reasons.append('Oversold')
    if score >= 4:
        return {'signal': 1, 'strength': min(1.0, score/7.0), 'reason': '; '.join(reasons)}
    return {'signal': 0, 'strength': 0, 'reason': 'No divergence'}
""",
        "meta": "RSI bullish divergence near BB lower band",
        "cat": "mean-reversion",
        "sigs": "RSI, BB, Price Divergence",
    },
    {
        "name": "Momentum Acceleration Detector",
        "code": """
def evaluate(ctx):
    price = ctx.get('price', 0)
    history = ctx.get('history', [])
    indicators = ctx.get('indicators', {})
    if len(history) < 30:
        return {'signal': 0, 'strength': 0, 'reason': 'Insufficient data'}
    ma5 = indicators.get('ma5', price)
    ma20 = indicators.get('ma20', price)
    dif = indicators.get('dif', 0)
    macd = indicators.get('macd', 0)
    rsi = indicators.get('rsi', 50)
    vol = ctx.get('volume', 0)
    avg_vol = sum(h.get('volume', 0) for h in history[-10:]) / 10 if history else 1
    vol_ratio = vol / avg_vol if avg_vol > 0 else 1.0
    closes = [h.get('close', 0) for h in history[-10:]]
    roc_5 = (closes[-1] - closes[-5]) / closes[-5] * 100 if len(closes) >= 5 and closes[-5] > 0 else 0
    roc_3 = (closes[-1] - closes[-3]) / closes[-3] * 100 if len(closes) >= 3 and closes[-3] > 0 else 0
    accel = roc_3 > roc_5 * 0.6 and roc_3 > 1.0
    score = 0
    reasons = []
    if accel:
        score += 2.5; reasons.append('Momentum accelerating')
    if price > ma5 > ma20:
        score += 1.5; reasons.append('MA aligned')
    if dif > macd and dif > 0:
        score += 1; reasons.append('MACD bullish')
    if vol_ratio > 1.2:
        score += 1; reasons.append('Volume up')
    if 45 < rsi < 75:
        score += 0.5
    if score >= 5:
        return {'signal': 1, 'strength': min(1.0, score/7.0), 'reason': '; '.join(reasons)}
    return {'signal': 0, 'strength': 0, 'reason': 'No acceleration'}
""",
        "meta": "Rate-of-change acceleration with volume confirmation",
        "cat": "momentum",
        "sigs": "ROC, MA5, MA20, MACD, Volume, RSI",
    },
]

KNOWLEDGE_RESP = '{"category": "performance", "title": "Strategy insight", "content": "Strategy tested successfully", "tags": ["backtest"]}'


def make_response(strat):
    return f"""```python
{strat['code'].strip()}
```

# METADATA
# name: {strat['name']}
# description: {strat['meta']}
# direction: BUY
# category: {strat['cat']}
# signals_used: {strat['sigs']}
"""


def api_get(path):
    req = urllib.request.Request(f"{base}{path}", headers={"X-API-Token": token})
    return json.loads(urllib.request.urlopen(req, timeout=10).read())


def api_post(path, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(f"{base}{path}", data=data, headers=headers, method="POST")
    return json.loads(urllib.request.urlopen(req, timeout=10).read())


gen_idx = 0
responded = set()
print(f"SF Agent: monitoring session {session_id}...")

for tick in range(400):
    time.sleep(4)
    try:
        items = api_get("/api/sf/llm-queue")
    except:
        continue

    for item in items:
        qid = item["id"]
        if qid in responded:
            continue
        purpose = item["purpose"]
        if purpose == "generate" and gen_idx < len(STRATS):
            resp = make_response(STRATS[gen_idx])
            gen_idx += 1
        else:
            resp = KNOWLEDGE_RESP
        try:
            api_post(f"/api/sf/llm-queue/{qid}/respond", {"response": resp})
            print(f"  #{qid} ({purpose}) -> done [gen={gen_idx}]")
            responded.add(qid)
        except Exception as e:
            print(f"  #{qid} err: {e}")

    try:
        sess = api_get(f"/api/sf/session/{session_id}/live")
        status = sess.get("status", "")
        if status not in ("running", "pending"):
            print(f"\n=== SESSION {session_id} COMPLETE ===")
            print(f"Status: {status}")
            print(f"Strategies: {sess.get('strategies_created', [])}")
            print()
            for l in sess.get("recent_logs", []):
                print(l)
            break
    except:
        pass
else:
    print("Timed out")
