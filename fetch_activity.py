#!/usr/bin/env python3
"""Incremental scan of KUB Chain transactions -> per-day on-chain activity.

Each run pages kubscan /api/v2/transactions from newest back to the last block
already processed, buckets each tx by Asia/Bangkok day, and records per day:
  - active EOAs   (tx senders that are NOT contracts)
  - per contract touched: set of unique EOA users + tx count (+ label)
Writes one merged file per day under activity_days/ (union on re-touch).
build_activity.py unions the last 7 days into activity_data.js.

Incremental & idempotent: only blocks with number > state.last_block are
processed, so re-runs never double count. First run is capped by MAX_PAGES to
seed ~2 days; the 7-day window then fills over a week of daily runs.

Per-day files hold raw EOA addresses -> gitignored (never published); only the
aggregate activity_data.js is deployed.
"""
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta

DIR = os.path.dirname(os.path.abspath(__file__))
DAYS_DIR = os.path.join(DIR, "activity_days")
STATE_F = os.path.join(DIR, "activity_state.json")
HEADERS = {"User-Agent": "Mozilla/5.0 (bbt-bd-sosmart activity)"}
BASE = "https://www.kubscan.com/api/v2/transactions"
WINDOW_DAYS = 7
KEEP_DAYS = WINDOW_DAYS + 2
MAX_PAGES = int(os.environ.get("ACT_MAX_PAGES", "1400"))  # ~2 days; safety cap so a first run/gap can't run away


def bkk_day(iso):
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00")) + timedelta(hours=7)
    return dt.strftime("%Y-%m-%d")


def today_bkk():
    return (datetime.now(timezone.utc) + timedelta(hours=7)).strftime("%Y-%m-%d")


def get(url, retries=6):
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.load(r)
        except Exception as e:
            wait = min(2 ** i, 30)
            print(f"  retry {i+1}/{retries}: {e} (sleep {wait}s)", flush=True)
            time.sleep(wait)
    raise RuntimeError("gave up: " + url)


def load_day(date):
    p = os.path.join(DAYS_DIR, f"day_{date}.json")
    if os.path.exists(p):
        d = json.load(open(p))
        return {
            "active": set(d.get("active", [])),
            "contracts": {k: {"name": v.get("name"), "users": set(v.get("users", [])), "tx": v.get("tx", 0)}
                          for k, v in d.get("contracts", {}).items()},
        }
    return {"active": set(), "contracts": {}}


def save_day(date, day):
    os.makedirs(DAYS_DIR, exist_ok=True)
    out = {
        "date": date,
        "active": sorted(day["active"]),
        "contracts": {k: {"name": v["name"], "users": sorted(v["users"]), "tx": v["tx"]}
                      for k, v in day["contracts"].items()},
    }
    json.dump(out, open(os.path.join(DAYS_DIR, f"day_{date}.json"), "w"), separators=(",", ":"))


def main():
    state = json.load(open(STATE_F)) if os.path.exists(STATE_F) else {"last_block": 0}
    last_block = int(state.get("last_block", 0))
    first_run = last_block == 0
    horizon = (datetime.now(timezone.utc) + timedelta(hours=7) - timedelta(days=WINDOW_DAYS)).strftime("%Y-%m-%d")

    cache = {}                 # date -> day dict (lazy)
    newest_block = last_block
    nxt, pages, seen = None, 0, 0
    stop = False

    while pages < MAX_PAGES and not stop:
        url = BASE + ("?" + urllib.parse.urlencode(nxt) if nxt else "")
        data = get(url)
        items = data.get("items", [])
        if not items:
            break
        for t in items:
            bn = t.get("block_number")
            if bn is None:
                bn = t.get("block")
            if bn is None:
                continue
            bn = int(bn)
            if bn > newest_block:
                newest_block = bn
            if not first_run and bn <= last_block:
                stop = True
                break
            ts = t.get("timestamp")
            if not ts:
                continue
            date = bkk_day(ts)
            day = cache.get(date)
            if day is None:
                day = cache[date] = load_day(date)
            frm = t.get("from") or {}
            to = t.get("to") or {}
            fh = (frm.get("hash") or "").lower()
            f_eoa = fh and not frm.get("is_contract")
            if f_eoa:
                day["active"].add(fh)
            if to.get("hash") and to.get("is_contract"):
                ch = to["hash"].lower()
                c = day["contracts"].get(ch)
                if c is None:
                    c = day["contracts"][ch] = {"name": to.get("name"), "users": set(), "tx": 0}
                if to.get("name") and not c["name"]:
                    c["name"] = to.get("name")
                c["tx"] += 1
                if f_eoa:
                    c["users"].add(fh)
            seen += 1
        pages += 1
        nxt = data.get("next_page_params")
        if not nxt:
            break
        if first_run and bkk_day(items[-1]["timestamp"]) < horizon:
            stop = True   # seeded the whole window on first run
        time.sleep(0.05)

    for date, day in cache.items():
        save_day(date, day)

    # prune day files older than the window (+buffer)
    cutoff = (datetime.now(timezone.utc) + timedelta(hours=7) - timedelta(days=KEEP_DAYS)).strftime("%Y-%m-%d")
    if os.path.isdir(DAYS_DIR):
        for fn in os.listdir(DAYS_DIR):
            if fn.startswith("day_") and fn[4:14] < cutoff:
                os.remove(os.path.join(DAYS_DIR, fn))

    json.dump({"last_block": newest_block, "updated": datetime.now(timezone.utc).isoformat(timespec="seconds")},
              open(STATE_F, "w"))
    print(f"scanned {pages} pages, {seen} new txns · days touched {sorted(cache)} · block {last_block}->{newest_block}", flush=True)


if __name__ == "__main__":
    main()
