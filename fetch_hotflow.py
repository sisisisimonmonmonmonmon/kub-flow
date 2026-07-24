#!/usr/bin/env python3
"""KUB Exchange netflow — daily native-KUB inflow/outflow of the Bitkub Exchange
wallets, counting only flow with GENUINELY EXTERNAL addresses.

The naive "flow to/from a wallet set" is misleading because Bitkub shuffles huge
amounts between its own wallets (hot<->cold rebalancing, treasury/routing wallets).
Those internal moves must NOT count as deposits/withdrawals. We exclude a transfer
when the counterparty is:
  1. one of the known Bitkub Exchange wallets (EXCH seed set), OR
  2. tagged "Bitkub ..." on kubscan (resolved live for sizable counterparties —
     catches exchange wallets / Bitkub-Peg / etc. not in the seed set), OR
  3. a curated untagged Bitkub operational wallet (CONFIRMED_INTERNAL).

Aggregate-only (per-day sums, no counterparties) so state is committable — no
private cache. Sizable untagged counterparties are logged for review so the
CONFIRMED_INTERNAL list can be kept honest over time.
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
SEED_HORIZON_DAYS = 45
MAX_PAGES = int(os.environ.get("HOTFLOW_MAX_PAGES", "50"))
RESOLVE_MIN = 300        # resolve a counterparty's tag only when a single transfer is >= this (KUB)
AUTOCHECK_MIN = 50000    # for a big untagged counterparty, deep-check if it's a Bitkub-internal wallet
REVIEW_MIN = 20000       # log untagged "external" counterparties above this for curation review

# Known Bitkub Exchange wallets (hot + cold), lower-cased.
EXCH = frozenset(a.lower() for a in [
    "0xd4d189ae4c76dae3da202d285f86dced8dcf7f4a", "0x275f6bf5e301b56c5bdb756b0caa076fdcce3294",
    "0x593f38e99fe02600832f4905234dfeef76aa169c", "0x6ce8e06bf6c91417fd3d73fe5d835b343063b3e2",
    "0x71cacef3a37379abaee32a9cc2474176b7edef98", "0x7368735e1500ff6b63f0ba34cb436b8a0d9a38d6",
    "0x941a98bb32dce53bc8da4f453e662ffba3f8c91d", "0xa0cc55b76d53b9dc324006f8d57c4014e4fafcde",
    "0xae3217ec059bc2252f06fd757f4e593f25aeb5d8", "0xb9c3d85a1f9362c799216e608b2913510d5d184e",
    "0xbd8b12af54d1ffc09452a0b2a8ade0cc1d15fa66", "0xd8008a8c32950046a6a0e2e820adcd536dde695f",
    "0x96f00c26d3eb5865bd1a32eb1a67f65c3e1e2797", "0x08372d5b86aabc40ec86981e43e4786cc906114e",
    "0x545dec78b30f1ebfb62bdd38b304b87a82a87519", "0x5f001f54ec8fbd3c9f7d21c6a5a3302001144cc1",
    "0x7081aa230336d50b94909b617a236308c02d2d05", "0xfc78c1d8755aa70bc647700751938139f8708979",
    "0x08134b2966d7e5618d9b5a1f93ad8870bb087e0a", "0x2a9dd53872184b0b388871781e1c534b3f8a6f5c",
    "0x35fdd152014c308cc366e45196d5ca55eb9bf5aa", "0x5f6174de4883507914ecd147c00e35731963a287",
    "0x6066ea738a706304af4d190fc8148dfe2adc0451", "0xa8e15e6932ad47970d0c6cdd64b38409d15ab80a",
    "0xcae02305a617f02cfed1eee510e0aacba64dfd85", "0xd51977de87d61935f0a0ae0df21f0038ab5ba66b",
    "0xf772f8c33f4f771af1557989f4d5f14a066c6b37", "0xae2c036d891156c9423ebf40f696ca5f11f4a7cb",
])

# Untagged Bitkub operational wallets (verified: hold/route large KUB solely among
# Bitkub Exchange wallets). Not user flow — excluded. Extend as new ones surface.
CONFIRMED_INTERNAL = frozenset(a.lower() for a in [
    "0x7a1cf8ce543f4838c964fb14d403cc6ed0bdbacc",  # routing wallet, ~53.8M KUB pass-through, only-Bitkub counterparties
    "0x9201c44555bc2ae585c27e8811e53596bef1b965",  # holds ~720K KUB, only-Bitkub counterparties
    "0xc1864356c72baf98df2357b51a1c5a4c8c9eea05",  # ~20.4M KUB, touches multiple Bitkub exchange wallets
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


def touches_multi_exch(cp):
    """A Bitkub-internal wallet shuffles KUB among several exchange wallets; a real
    user typically uses just one. True if cp has >= 2 distinct exchange counterparties."""
    d = get(f"https://www.kubscan.com/api/v2/addresses/{cp}/transactions", retries=3) or {}
    seen = set()
    for t in d.get("items", []):
        for side in ("from", "to"):
            h = ((t.get(side) or {}).get("hash") or "").lower()
            if h in EXCH:
                seen.add(h)
    return len(seen) >= 2


def is_internal(cp, v, tags, autoint, review):
    """True if counterparty cp is Bitkub-internal (so this transfer is not real user flow)."""
    if cp in EXCH or cp in CONFIRMED_INTERNAL:
        return True
    if v < RESOLVE_MIN:
        return False                                  # small counterparty → assume a real external user
    if cp not in tags:                                # resolve + cache its kubscan tag
        j = get(f"https://www.kubscan.com/api/v2/addresses/{cp}", retries=3) or {}
        tg = [t.get("display_name") for t in (j.get("public_tags") or [])]
        tags[cp] = ((tg[0] if tg else (j.get("name") or "")) or "").lower()
        time.sleep(0.03)
    if "bitkub" in tags[cp]:
        return True
    if v >= AUTOCHECK_MIN:                             # big untagged — is it a Bitkub-internal wallet?
        if cp not in autoint:
            autoint[cp] = touches_multi_exch(cp)
            time.sleep(0.03)
        if autoint[cp]:
            return True
    if v >= REVIEW_MIN and not tags[cp]:
        review[cp] = review.get(cp, 0) + v            # big untagged "external" — flag for review
    return False


def scan_wallet(addr, wlast, days, tags, autoint, review, horizon):
    first = wlast.get(addr, 0) == 0
    base = f"https://www.kubscan.com/api/v2/addresses/{addr}/transactions"
    nxt, pages, newest = None, 0, wlast.get(addr, 0)
    while pages < MAX_PAGES:
        data = get(base + ("?" + urllib.parse.urlencode(nxt) if nxt else ""))
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
            if (t.get("result") or t.get("status")) not in ("success", "ok", None):
                continue                               # skip reverted/failed txns
            v = int(t.get("value") or 0) / 1e18
            if v <= 0:
                continue
            frm = ((t.get("from") or {}).get("hash") or "").lower()
            to = ((t.get("to") or {}).get("hash") or "").lower()
            if frm == addr:
                dirn, cp = "o", to        # exchange -> counterparty = outflow
            elif to == addr:
                dirn, cp = "i", frm       # counterparty -> exchange = inflow
            else:
                continue
            if is_internal(cp, v, tags, autoint, review):
                continue
            ts = t.get("timestamp")
            if not ts:
                continue
            dk = bkk_day(ts)
            d = days.setdefault(dk, {"i": 0.0, "o": 0.0, "iT": 0, "oT": 0})
            d[dirn] += v
            d["iT" if dirn == "i" else "oT"] += 1
        pages += 1
        nxt = data.get("next_page_params")
        if stop or not nxt:
            break
        if first and items and bkk_day(items[-1]["timestamp"]) < horizon:
            break
        time.sleep(0.05)
    return newest


def build_js(state):
    days = state.get("days", {})
    out = []
    for dk in sorted(days):
        d = days[dk]
        out.append({"day": dk, "inflow": round(d["i"], 2), "outflow": round(d["o"], 2),
                    "net": round(d["i"] - d["o"], 2), "inTx": d["iT"], "outTx": d["oT"]})
    data = {"updatedAt": int(time.time() * 1000), "wallets": len(EXCH),
            "today": today_bkk(), "daily": out}
    with open(JS_F, "w") as f:
        f.write("window.HOTFLOW = " + json.dumps(data, separators=(",", ":")) + ";\n")


def main():
    state = json.load(open(STATE_F)) if os.path.exists(STATE_F) else {}
    days = state.setdefault("days", {})
    wlast = state.setdefault("wlast", {})
    tags = state.setdefault("tags", {})
    autoint = state.setdefault("autoint", {})
    review = {}
    horizon = (datetime.now(timezone.utc) + timedelta(hours=7) - timedelta(days=SEED_HORIZON_DAYS)).strftime("%Y-%m-%d")

    for addr in EXCH:
        try:
            wlast[addr] = scan_wallet(addr, wlast, days, tags, autoint, review, horizon)
        except Exception as e:
            print(f"  wallet {addr[:10]} failed: {e}", flush=True)

    cutoff = (datetime.now(timezone.utc) + timedelta(hours=7) - timedelta(days=KEEP_DAYS)).strftime("%Y-%m-%d")
    for dk in [d for d in days if d < cutoff]:
        del days[dk]

    state["updated"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    json.dump(state, open(STATE_F, "w"), separators=(",", ":"))
    build_js(state)
    tot_i = sum(d["i"] for d in days.values()); tot_o = sum(d["o"] for d in days.values())
    print(f"hotflow: {len(days)} days · external in {tot_i:,.0f} · out {tot_o:,.0f} · net {tot_i-tot_o:,.0f} KUB", flush=True)
    if review:
        print("REVIEW — big untagged 'external' counterparties (curate into CONFIRMED_INTERNAL if Bitkub):", flush=True)
        for cp, v in sorted(review.items(), key=lambda x: -x[1])[:15]:
            print(f"    {cp}  {v:,.0f} KUB", flush=True)


if __name__ == "__main__":
    main()
