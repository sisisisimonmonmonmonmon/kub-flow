#!/usr/bin/env python3
"""Union the last 7 daily activity files -> activity_data.js (aggregate only).

Also reads the latest KUB EOA holder count from daily_summary.csv and the KKUB
holder count + chain stats from kubscan. No per-wallet data is emitted.
"""
import csv
import json
import os
import time
import urllib.request
from datetime import datetime, timezone, timedelta

DIR = os.path.dirname(os.path.abspath(__file__))
DAYS_DIR = os.path.join(DIR, "activity_days")
CSV_F = os.path.join(DIR, "daily_summary.csv")
JS_F = os.path.join(DIR, "activity_data.js")
KKUB = "0x67eBD850304c70d983B2d1b93ea79c7CD6c3F6b5"
HEADERS = {"User-Agent": "Mozilla/5.0 (bbt-bd-sosmart activity build)"}
WINDOW_DAYS = 7
TOP_N = 8


def get(url):
    for i in range(4):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.load(r)
        except Exception:
            time.sleep(2 * (i + 1))
    return None


def latest_holders():
    if not os.path.exists(CSV_F):
        return None
    last = None
    for r in csv.DictReader(open(CSV_F)):
        last = r
    return last


def main():
    today = datetime.strptime((datetime.now(timezone.utc) + timedelta(hours=7)).strftime("%Y-%m-%d"), "%Y-%m-%d")
    window = sorted((today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(WINDOW_DAYS))

    active = set()
    contracts = {}
    by_day = []
    for d in window:
        p = os.path.join(DAYS_DIR, f"day_{d}.json")
        if not os.path.exists(p):
            by_day.append({"date": d, "count": 0})
            continue
        day = json.load(open(p))
        a = day.get("active", [])
        active.update(a)
        by_day.append({"date": d, "count": len(a)})
        for ch, c in day.get("contracts", {}).items():
            cc = contracts.get(ch)
            if cc is None:
                cc = contracts[ch] = {"name": c.get("name"), "users": set(), "tx": 0}
            if c.get("name") and not cc["name"]:
                cc["name"] = c.get("name")
            cc["users"].update(c.get("users", []))
            cc["tx"] += c.get("tx", 0)

    top = sorted(contracts.items(), key=lambda kv: (-len(kv[1]["users"]), -kv[1]["tx"]))[:TOP_N]
    top_out = [{"addr": a, "name": v["name"], "users": len(v["users"]), "tx": v["tx"]} for a, v in top]

    hold = latest_holders()
    kk = get(f"https://www.kubscan.com/api/v2/tokens/{KKUB}/counters")
    stats = get("https://www.kubscan.com/api/v2/stats")

    data = {
        "generatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "windowDays": WINDOW_DAYS,
        "daysCovered": sum(1 for b in by_day if b["count"] > 0),
        "eoaHolders": int(hold["eoa_holders"]) if hold else None,
        "eoaHoldersDate": hold["date"] if hold else None,
        "totalKub": float(hold["total_kub"]) if hold else None,
        "kkubHolders": int(kk["token_holders_count"]) if kk and kk.get("token_holders_count") else None,
        "activeEoa7d": len(active),
        "activeByDay": by_day,
        "topContracts": top_out,
        "totalAddresses": int(stats["total_addresses"]) if stats and stats.get("total_addresses") else None,
        "txToday": int(stats["transactions_today"]) if stats and stats.get("transactions_today") else None,
    }
    with open(JS_F, "w") as f:
        f.write("window.ACTIVITY = " + json.dumps(data, separators=(",", ":")) + ";\n")
    print("activity_data.js:", json.dumps({k: data[k] for k in ["eoaHolders", "activeEoa7d", "daysCovered", "kkubHolders"]}))
    print("top4:", [(t["name"] or t["addr"][:10], t["users"], t["tx"]) for t in top_out[:4]])


if __name__ == "__main__":
    main()
