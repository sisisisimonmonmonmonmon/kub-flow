#!/usr/bin/env python3
"""Daily KUB CEX flow snapshot (net buy/sell + order-book depth) -> flow_data.js.

Ports the old Vercel /api/ingest + /api/snapshot + /api/data into ONE
GitHub-Actions batch, so kub-flow runs 100% on GitHub Pages (no Vercel, no Blob).
Accumulates per-day CEX taker net (watermark dedup, same proven algorithm) plus
one order-book depth snapshot/day into flow_state.json, then emits aggregate-only
flow_data.js (window.FLOW). Today's CEX + all DEX stay live client-side in
kub-flow.html — this job only persists the day history. No per-wallet data.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone, timedelta

DIR = os.path.dirname(os.path.abspath(__file__))
STATE_F = os.path.join(DIR, "flow_state.json")
JS_F = os.path.join(DIR, "flow_data.js")
SYM = "KUB_THB"
HEADERS = {"User-Agent": "Mozilla/5.0 (bbt-bd-sosmart kub-flow)"}
KEEP_DAYS = 70


def bkk_day(ts):
    return (datetime.fromtimestamp(ts, timezone.utc) + timedelta(hours=7)).strftime("%Y-%m-%d")


def today_bkk():
    return (datetime.now(timezone.utc) + timedelta(hours=7)).strftime("%Y-%m-%d")


def week_start(dk):
    d = datetime.strptime(dk, "%Y-%m-%d")
    return (d - timedelta(days=d.weekday())).strftime("%Y-%m-%d")   # Monday-start


def get(url, retries=6):
    for i in range(retries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=HEADERS), timeout=30) as r:
                return json.load(r)
        except Exception as e:
            print(f"  retry {i+1}: {e}", flush=True)
            time.sleep(min(2 ** i, 30))
    raise RuntimeError("gave up: " + url)


def ingest_net(state):
    j = get(f"https://api.bitkub.com/api/v3/market/trades?sym={SYM}&lmt=1000")
    raw = j.get("result") or []
    wm = state.get("watermark", 0)
    boundary = set(state.get("boundary", []))
    days = state.setdefault("days", {})
    gap = set(state.get("gapDays", []))
    asc = list(reversed(raw))                      # oldest -> newest
    if asc:
        oldest = int(asc[0][0])
        if wm > 0 and oldest > wm:                 # >1000 trades since last run -> gap
            t, end = wm, bkk_day(oldest)
            for _ in range(800):
                if bkk_day(t) > end:
                    break
                gap.add(bkk_day(t)); t += 86400
            gap.add(end)
    added = 0
    for r in asc:
        ts = int(r[0]); price = float(r[1]); amt = float(r[2]); side = r[3]
        if ts < wm:
            continue
        key = f"{ts}|{r[1]}|{r[2]}|{side}"
        if ts == wm and key in boundary:
            continue
        d = days.setdefault(bkk_day(ts), {"bB": 0, "sB": 0, "bQ": 0, "sQ": 0, "bC": 0, "sC": 0})
        val = price * amt
        if side == "BUY":
            d["bB"] += amt; d["bQ"] += val; d["bC"] += 1
        else:
            d["sB"] += amt; d["sQ"] += val; d["sC"] += 1
        if ts > wm:
            wm = ts; boundary = {key}
        else:
            boundary.add(key)
        added += 1
    state["watermark"] = wm
    state["boundary"] = sorted(boundary)
    state["gapDays"] = sorted(gap)
    print(f"net: +{added} trades, watermark {wm}, {len(days)} days", flush=True)


def snapshot_depth(state):
    j = get(f"https://api.bitkub.com/api/v3/market/depth?sym={SYM}&lmt=1000")
    r = j.get("result") or {}
    bids, asks = r.get("bids") or [], r.get("asks") or []
    if not bids or not asks:
        return
    best_bid, best_ask = float(bids[0][0]), float(asks[0][0])
    mid = (best_bid + best_ask) / 2

    def agg(levels, is_ask):
        base = q = 0.0
        band = {2: 0.0, 5: 0.0, 10: 0.0}
        for p_, a_ in levels:
            p, a = float(p_), float(a_)
            base += a; q += p * a
            for pct in (2, 5, 10):
                lim = mid * (1 + pct / 100) if is_ask else mid * (1 - pct / 100)
                if (p <= lim) if is_ask else (p >= lim):
                    band[pct] += a
        return base, q, band

    bb, bq, bband = agg(bids, False)
    ab, aq, aband = agg(asks, True)
    state.setdefault("depthDays", {})[today_bkk()] = {
        "ts": int(time.time()), "mid": mid, "bestBid": best_bid, "bestAsk": best_ask,
        "bidBase": bb, "bidQuote": bq, "askBase": ab, "askQuote": aq,
        "bid2": bband[2], "bid5": bband[5], "bid10": bband[10],
        "ask2": aband[2], "ask5": aband[5], "ask10": aband[10],
    }


def build_js(state):
    gap = set(state.get("gapDays", []))
    days = state.get("days", {})
    daily = []
    for dk in sorted(days):
        b = days[dk]
        daily.append({
            "day": dk, "week": week_start(dk), "buy": b["bB"], "sell": b["sB"],
            "net": b["bB"] - b["sB"], "netQ": b["bQ"] - b["sQ"],
            "avgBuy": (b["bQ"] / b["bB"]) if b["bB"] > 0 else None,
            "avgSell": (b["sQ"] / b["sB"]) if b["sB"] > 0 else None,
            "trades": b["bC"] + b["sC"], "gap": dk in gap,
        })
    dd = state.get("depthDays", {})
    depth = [dict(day=dk, **dd[dk]) for dk in sorted(dd)]
    now_ms = int(time.time() * 1000)
    data = {
        "symbol": SYM, "today": today_bkk(), "kubThb": (depth[-1]["mid"] if depth else None),
        "dailyNet": daily, "depthDaily": depth, "gapDays": sorted(gap),
        "netUpdatedAt": now_ms, "depthUpdatedAt": now_ms,
    }
    with open(JS_F, "w") as f:
        f.write("window.FLOW = " + json.dumps(data, separators=(",", ":")) + ";\n")


def main():
    state = json.load(open(STATE_F)) if os.path.exists(STATE_F) else {}
    ingest_net(state)
    try:
        snapshot_depth(state)
    except Exception as e:
        print("depth snapshot failed:", e, flush=True)
    cutoff = (datetime.now(timezone.utc) + timedelta(hours=7) - timedelta(days=KEEP_DAYS)).strftime("%Y-%m-%d")
    for key in ("days", "depthDays"):
        m = state.get(key, {})
        for dk in [d for d in m if d < cutoff]:
            del m[dk]
    state["updated"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    json.dump(state, open(STATE_F, "w"), separators=(",", ":"))
    build_js(state)
    print(f"wrote flow_data.js · days={len(state.get('days', {}))} depthDays={len(state.get('depthDays', {}))}", flush=True)


if __name__ == "__main__":
    main()
