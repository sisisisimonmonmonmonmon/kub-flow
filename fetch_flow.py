#!/usr/bin/env python3
"""KUB daily flow snapshot -> flow_data.js (for GitHub Pages, no server/DB).

Three sources, all aggregate-only (no per-wallet data), accumulated per Asia/Bangkok day:
  1. CEX net  — Bitkub taker Buy - Sell (watermark dedup on last 1000 trades).
  2. Depth    — one order-book snapshot/day (bid/ask liquidity + ±2/5/10% bands).
  3. DEX net  — on-chain swap events from the 5 live KKUB/stable pools across all
                5 DEXes (KUBLERX/Udonswap/Ponder/Junoswap), read straight from the
                Bitkub Chain RPC. Incremental by block watermark.

State persists in flow_state.json (committed by the daily worker). The web page
reads flow_data.js and lets the user toggle "count DEX or not".
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
RPC = "https://rpc.bitkubchain.io"
KEEP_DAYS = 70

KKUB = "0x67ebd850304c70d983b2d1b93ea79c7cd6c3f6b5"
V2_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
V3_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
# The 5 pools that actually trade (13 others are dust). stableDec: USDT/USDC.e=6, KUSDT=18.
DEX_POOLS = [
    {"addr": "0x7b7c3ef4dd11b2c994e5d93468f1e9586713bf0b", "type": "v3", "stableDec": 6,  "dex": "KUBLERX",  "pair": "KKUB/USDT"},
    {"addr": "0x8d56d2fba2b43ce8ddc0863bd4eb3959b5799924", "type": "v3", "stableDec": 6,  "dex": "KUBLERX",  "pair": "KKUB/USDC.e"},
    {"addr": "0xa7906787409c60e165bf2e5bd819f7a15c8ae265", "type": "v2", "stableDec": 18, "dex": "Udonswap", "pair": "KKUB/KUSDT"},
    {"addr": "0xe9813764621855f9b630a3cf621b537d65f655f0", "type": "v2", "stableDec": 18, "dex": "Ponder",   "pair": "KKUB/KUSDT"},
    {"addr": "0xcf0c912a4efa1b12eab70f3ae701d6219103df0f", "type": "v3", "stableDec": 18, "dex": "Junoswap", "pair": "KKUB/KUSDT"},
]
SEED_BLOCKS = int(os.environ.get("DEX_SEED_BLOCKS", str(7 * 28800)))   # first run seeds ~7 days (~3s blocks)


def bkk_day(ts):
    return (datetime.fromtimestamp(ts, timezone.utc) + timedelta(hours=7)).strftime("%Y-%m-%d")


def today_bkk():
    return (datetime.now(timezone.utc) + timedelta(hours=7)).strftime("%Y-%m-%d")


def week_start(dk):
    d = datetime.strptime(dk, "%Y-%m-%d")
    return (d - timedelta(days=d.weekday())).strftime("%Y-%m-%d")


def http_json(url, retries=6):
    for i in range(retries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=HEADERS), timeout=30) as r:
                return json.load(r)
        except Exception as e:
            print(f"  retry {i+1}: {e}", flush=True); time.sleep(min(2 ** i, 30))
    raise RuntimeError("gave up: " + url)


def rpc(method, params, retries=4):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(RPC, data=body, headers={**HEADERS, "Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=45) as r:
                j = json.load(r)
            if "error" in j and j["error"]:
                raise RuntimeError(j["error"])
            return j["result"]
        except Exception as e:
            last = e; time.sleep(min(2 ** i, 15))
    raise RuntimeError(f"rpc {method} failed: {last}")


# ---------- CEX (Bitkub taker net) ----------
def ingest_net(state):
    j = http_json(f"https://api.bitkub.com/api/v3/market/trades?sym={SYM}&lmt=1000")
    raw = j.get("result") or []
    wm = state.get("watermark", 0)
    boundary = set(state.get("boundary", []))
    days = state.setdefault("days", {})
    gap = set(state.get("gapDays", []))
    asc = list(reversed(raw))
    if asc:
        oldest = int(asc[0][0])
        if wm > 0 and oldest > wm:
            t, end = wm, bkk_day(oldest)
            for _ in range(800):
                if bkk_day(t) > end:
                    break
                gap.add(bkk_day(t)); t += 86400
            gap.add(end)
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
    state["watermark"] = wm; state["boundary"] = sorted(boundary); state["gapDays"] = sorted(gap)


def snapshot_depth(state):
    j = http_json(f"https://api.bitkub.com/api/v3/market/depth?sym={SYM}&lmt=1000")
    r = j.get("result") or {}
    bids, asks = r.get("bids") or [], r.get("asks") or []
    if not bids or not asks:
        return
    bb, ba = float(bids[0][0]), float(asks[0][0])
    mid = (bb + ba) / 2

    def agg(levels, is_ask):
        base = q = 0.0; band = {2: 0.0, 5: 0.0, 10: 0.0}
        for p_, a_ in levels:
            p, a = float(p_), float(a_); base += a; q += p * a
            for pct in (2, 5, 10):
                lim = mid * (1 + pct / 100) if is_ask else mid * (1 - pct / 100)
                if (p <= lim) if is_ask else (p >= lim):
                    band[pct] += a
        return base, q, band
    b1, bq, bband = agg(bids, False); a1, aq, aband = agg(asks, True)
    state.setdefault("depthDays", {})[today_bkk()] = {
        "ts": int(time.time()), "mid": mid, "bestBid": bb, "bestAsk": ba,
        "bidBase": b1, "bidQuote": bq, "askBase": a1, "askQuote": aq,
        "bid2": bband[2], "bid5": bband[5], "bid10": bband[10],
        "ask2": aband[2], "ask5": aband[5], "ask10": aband[10],
    }


# ---------- DEX (on-chain swap events) ----------
def to_int256(word):
    v = int(word, 16)
    return v - (1 << 256) if v >= (1 << 255) else v


def get_logs(pool, topic0, from_b, to_b):
    out, step, b = [], 5000, from_b
    while b <= to_b:
        e = min(b + step - 1, to_b)
        try:
            logs = rpc("eth_getLogs", [{"address": pool, "topics": [topic0],
                                        "fromBlock": hex(b), "toBlock": hex(e)}], retries=2)
            out.extend(logs); b = e + 1
        except Exception:
            if step > 500:
                step //= 2                 # range/limit error -> narrow the window
            else:
                b = e + 1                  # give up on this tiny slice, move on
    return out


def ingest_dex(state, latest):
    last = state.get("dexLastBlock", 0)
    from_b = last + 1 if last > 0 else max(1, latest - SEED_BLOCKS)
    if from_b > latest:
        return
    dex_days = state.setdefault("dexDays", {})
    sidx_cache = state.setdefault("poolStableIdx", {})
    blk_ts = state.setdefault("blockTs", {})

    def block_time(bn):
        k = str(bn)
        if k not in blk_ts:
            blk_ts[k] = int(rpc("eth_getBlockByNumber", [hex(bn), False])["timestamp"], 16)
        return blk_ts[k]

    total = 0
    for p in DEX_POOLS:
        addr = p["addr"]
        if addr not in sidx_cache:                      # token0()==KKUB ? stable=1 : stable=0
            t0 = "0x" + rpc("eth_call", [{"to": addr, "data": "0x0dfe1681"}, "latest"])[-40:]
            sidx_cache[addr] = 1 if t0.lower() == KKUB.lower() else 0
        sidx = sidx_cache[addr]; kidx = 1 - sidx
        topic = V3_TOPIC if p["type"] == "v3" else V2_TOPIC
        logs = get_logs(addr, topic, from_b, latest)
        for lg in logs:
            h = lg["data"][2:] if lg["data"].startswith("0x") else lg["data"]
            w = [h[i:i + 64] for i in range(0, len(h), 64)]
            if p["type"] == "v2":
                a = [int(x, 16) for x in w[:4]]  # amount0In, amount1In, amount0Out, amount1Out
                sIn = a[1] if sidx == 1 else a[0]; sOut = a[3] if sidx == 1 else a[2]
                kIn = a[1] if kidx == 1 else a[0]; kOut = a[3] if kidx == 1 else a[2]
                if sIn > 0:
                    buy, kkub, usd = True, kOut, sIn / 10 ** p["stableDec"]
                elif sOut > 0:
                    buy, kkub, usd = False, kIn, sOut / 10 ** p["stableDec"]
                else:
                    continue
            else:
                a0, a1 = to_int256(w[0]), to_int256(w[1])
                sAmt = a1 if sidx == 1 else a0; kAmt = a1 if kidx == 1 else a0
                if sAmt > 0:
                    buy = True
                elif sAmt < 0:
                    buy = False
                else:
                    continue
                kkub, usd = abs(kAmt), abs(sAmt) / 10 ** p["stableDec"]
            kkub /= 1e18
            day = bkk_day(block_time(int(lg["blockNumber"], 16)))
            d = dex_days.setdefault(day, {"bK": 0, "sK": 0, "bU": 0, "sU": 0, "n": 0})
            if buy:
                d["bK"] += kkub; d["bU"] += usd
            else:
                d["sK"] += kkub; d["sU"] += usd
            d["n"] += 1; total += 1
    state["dexLastBlock"] = latest
    if len(blk_ts) > 4000:                              # keep the block-timestamp cache small
        for k in sorted(blk_ts, key=lambda x: int(x))[:len(blk_ts) - 3000]:
            del blk_ts[k]
    print(f"dex: +{total} swaps, blocks {from_b}->{latest}, {len(dex_days)} days", flush=True)


def build_js(state):
    gap = set(state.get("gapDays", []))
    days = state.get("days", {})
    dex_days = state.get("dexDays", {})
    dd = state.get("depthDays", {})
    depth = [dict(day=dk, **dd[dk]) for dk in sorted(dd)]
    kub_thb = depth[-1]["mid"] if depth else None

    out = []
    for dk in sorted(set(days) | set(dex_days)):
        c = days.get(dk, {"bB": 0, "sB": 0, "bQ": 0, "sQ": 0, "bC": 0, "sC": 0})
        x = dex_days.get(dk, {"bK": 0, "sK": 0, "bU": 0, "sU": 0, "n": 0})
        dex_net_kkub = x["bK"] - x["sK"]
        out.append({
            "day": dk, "week": week_start(dk),
            "cexBuy": c["bB"], "cexSell": c["sB"], "cexNet": c["bB"] - c["sB"], "cexNetQ": c["bQ"] - c["sQ"],
            "avgBuy": (c["bQ"] / c["bB"]) if c["bB"] > 0 else None,
            "avgSell": (c["sQ"] / c["sB"]) if c["sB"] > 0 else None,
            "cexTrades": c["bC"] + c["sC"],
            "dexBuy": x["bK"], "dexSell": x["sK"], "dexNet": dex_net_kkub,
            "dexNetQ": (dex_net_kkub * kub_thb) if kub_thb else 0,
            "dexUsd": x["bU"] - x["sU"], "dexSwaps": x["n"],
            "gap": dk in gap,
        })
    data = {
        "symbol": SYM, "today": today_bkk(), "kubThb": kub_thb,
        "dailyNet": out, "depthDaily": depth, "gapDays": sorted(gap),
        "netUpdatedAt": int(time.time() * 1000), "depthUpdatedAt": int(time.time() * 1000),
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
    try:
        latest = int(rpc("eth_blockNumber", []), 16)
        ingest_dex(state, latest)
    except Exception as e:
        print("dex scan failed:", e, flush=True)
    cutoff = (datetime.now(timezone.utc) + timedelta(hours=7) - timedelta(days=KEEP_DAYS)).strftime("%Y-%m-%d")
    for key in ("days", "depthDays", "dexDays"):
        m = state.get(key, {})
        for dk in [d for d in m if d < cutoff]:
            del m[dk]
    state["updated"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    json.dump(state, open(STATE_F, "w"), separators=(",", ":"))
    build_js(state)
    print(f"wrote flow_data.js · days={len(state.get('days', {}))} dexDays={len(state.get('dexDays', {}))}", flush=True)


if __name__ == "__main__":
    main()
