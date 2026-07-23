#!/usr/bin/env python3
"""KUB Exchange netflow — daily native-KUB inflow/outflow of the Bitkub Exchange
hot/cold wallets.

For every exchange wallet we page kubscan's normal transactions and classify each
native-KUB transfer (value > 0):
  - INFLOW   external -> exchange wallet  (deposits; usually sell-side pressure)
  - OUTFLOW  exchange wallet -> external  (withdrawals; usually accumulation)
Transfers BETWEEN two exchange wallets (hot<->cold shuffles) are excluded.

Aggregate-only (per-day sums, no counterparties) so it is safe to publish. State
is a plain per-day totals dict + a per-wallet block watermark, all committable —
no address sets, so (unlike activity) no private cache is needed.
"""
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta

DIR = os.path.dirname(os.path.abspath(__file__))
STATE_F = os.path.join(DIR, "hotflow_state.json")
JS_F = os.path.join(DIR, "hotflow_data.js")
HEADERS = {"User-Agent": "Mozilla/5.0 (bbt-bd-sosmart hotflow)"}
KEEP_DAYS = 90
SEED_HORIZON_DAYS = 45                       # first run per wallet seeds back this far
MAX_PAGES = int(os.environ.get("HOTFLOW_MAX_PAGES", "50"))   # per wallet per run (safety cap)

# Bitkub Exchange wallets (hot + cold), from the CEX exclusion set. Lower-cased.
HOT = frozenset(a.lower() for a in [
    "0xd4d189ae4c76dae3da202d285f86dced8dcf7f4a",
    "0x275f6bf5e301b56c5bdb756b0caa076fdcce3294",
    "0x593f38e99fe02600832f4905234dfeef76aa169c",
    "0x6ce8e06bf6c91417fd3d73fe5d835b343063b3e2",
    "0x71cacef3a37379abaee32a9cc2474176b7edef98",
    "0x7368735e1500ff6b63f0ba34cb436b8a0d9a38d6",
    "0x941a98bb32dce53bc8da4f453e662ffba3f8c91d",
    "0xa0cc55b76d53b9dc324006f8d57c4014e4fafcde",
    "0xae3217ec059bc2252f06fd757f4e593f25aeb5d8",
    "0xb9c3d85a1f9362c799216e608b2913510d5d184e",
    "0xbd8b12af54d1ffc09452a0b2a8ade0cc1d15fa66",
    "0xd8008a8c32950046a6a0e2e820adcd536dde695f",
    "0x96f00c26d3eb5865bd1a32eb1a67f65c3e1e2797",
    "0x08372d5b86aabc40ec86981e43e4786cc906114e",
    "0x545dec78b30f1ebfb62bdd38b304b87a82a87519",
    "0x5f001f54ec8fbd3c9f7d21c6a5a3302001144cc1",
    "0x7081aa230336d50b94909b617a236308c02d2d05",
    "0xfc78c1d8755aa70bc647700751938139f8708979",
    "0x08134b2966d7e5618d9b5a1f93ad8870bb087e0a",
    "0x2a9dd53872184b0b388871781e1c534b3f8a6f5c",
    "0x35fdd152014c308cc366e45196d5ca55eb9bf5aa",
    "0x5f6174de4883507914ecd147c00e35731963a287",
    "0x6066ea738a706304af4d190fc8148dfe2adc0451",
    "0xa8e15e6932ad47970d0c6cdd64b38409d15ab80a",
    "0xcae02305a617f02cfed1eee510e0aacba64dfd85",
    "0xd51977de87d61935f0a0ae0df21f0038ab5ba66b",
    "0xf772f8c33f4f771af1557989f4d5f14a066c6b37",
    "0xae2c036d891156c9423ebf40f696ca5f11f4a7cb",
])


def bkk_day(iso):
    return (datetime.fromisoformat(iso.replace("Z", "+00:00")) + timedelta(hours=7)).strftime("%Y-%m-%d")


def today_bkk():
    return (datetime.now(timezone.utc) + timedelta(hours=7)).strftime("%Y-%m-%d")


def get(url, retries=6):
    for i in range(retries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=HEADERS), timeout=30) as r:
                return json.load(r)
        except Exception as e:
            print(f"  retry {i+1}/{retries}: {e}", flush=True); time.sleep(min(2 ** i, 30))
    return None


def scan_wallet(addr, wlast, days, horizon):
    """Page one wallet newest->oldest; accumulate per-day inflow/outflow; return max block seen."""
    first = wlast.get(addr, 0) == 0
    base = f"https://www.kubscan.com/api/v2/addresses/{addr}/transactions"
    nxt, pages, newest = None, 0, wlast.get(addr, 0)
    while pages < MAX_PAGES:
        url = base + ("?" + urllib.parse.urlencode(nxt) if nxt else "")
        data = get(url)
        if not data:
            break
        items = data.get("items", [])
        if not items:
            break
        stop = False
        for t in items:
            bn = int(t.get("block") or t.get("block_number") or 0)
            if bn > newest:
                newest = bn
            if not first and bn <= wlast.get(addr, 0):
                stop = True
                break
            v = int(t.get("value") or 0) / 1e18
            if v <= 0:
                continue
            frm = ((t.get("from") or {}).get("hash") or "").lower()
            to = ((t.get("to") or {}).get("hash") or "").lower()
            in_hot, from_hot = to in HOT, frm in HOT
            if in_hot and from_hot:
                continue                      # internal exchange shuffle — not real flow
            ts = t.get("timestamp")
            if not ts:
                continue
            dk = bkk_day(ts)
            d = days.setdefault(dk, {"i": 0.0, "o": 0.0, "iT": 0, "oT": 0})
            if in_hot:
                d["i"] += v; d["iT"] += 1
            elif from_hot:
                d["o"] += v; d["oT"] += 1
        pages += 1
        nxt = data.get("next_page_params")
        if stop or not nxt:
            break
        if first and items and bkk_day(items[-1]["timestamp"]) < horizon:
            break                             # seeded enough history on first run
        time.sleep(0.05)
    return newest


def build_js(state):
    days = state.get("days", {})
    out = []
    for dk in sorted(days):
        d = days[dk]
        out.append({"day": dk, "inflow": round(d["i"], 2), "outflow": round(d["o"], 2),
                    "net": round(d["i"] - d["o"], 2), "inTx": d["iT"], "outTx": d["oT"]})
    data = {"updatedAt": int(time.time() * 1000), "wallets": len(HOT),
            "today": today_bkk(), "daily": out}
    with open(JS_F, "w") as f:
        f.write("window.HOTFLOW = " + json.dumps(data, separators=(",", ":")) + ";\n")


def main():
    state = json.load(open(STATE_F)) if os.path.exists(STATE_F) else {}
    days = state.setdefault("days", {})
    wlast = state.setdefault("wlast", {})
    horizon = (datetime.now(timezone.utc) + timedelta(hours=7) - timedelta(days=SEED_HORIZON_DAYS)).strftime("%Y-%m-%d")

    for addr in HOT:
        try:
            wlast[addr] = scan_wallet(addr, wlast, days, horizon)
        except Exception as e:
            print(f"  wallet {addr[:10]} failed: {e}", flush=True)

    cutoff = (datetime.now(timezone.utc) + timedelta(hours=7) - timedelta(days=KEEP_DAYS)).strftime("%Y-%m-%d")
    for dk in [d for d in days if d < cutoff]:
        del days[dk]

    state["updated"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    json.dump(state, open(STATE_F, "w"), separators=(",", ":"))
    build_js(state)
    tot_i = sum(d["i"] for d in days.values()); tot_o = sum(d["o"] for d in days.values())
    print(f"hotflow: {len(days)} days · in {tot_i:,.0f} KUB · out {tot_o:,.0f} KUB · net {tot_i-tot_o:,.0f}", flush=True)


if __name__ == "__main__":
    main()
