#!/usr/bin/env python3
"""
scan_dataset.py -- walk the train-major CSV tree and collect the facts needed to
describe it: per-file identity and coverage, and the global totals.

    python3 scan_dataset.py /Volumes/shit/trains --out stats.json

Per file it records train_no, the most common train_name/train_type, the number
of stop rows, the number of distinct runs (dates), the first and last run date,
the number of distinct stations, and how many rows carry an actual arrival or
departure time (the rows that make the delay columns usable).

Globals are accumulated the same way so the totals are exact rather than summed
from rounded per-file figures.
"""

import argparse
import csv
import json
import os
import sys
from collections import Counter
from multiprocessing import Pool

# Not every file carries a header: a minority start straight on a data row.
# They still have these 24 fields in this order, so the header is supplied.
HEADER = [
    "date", "train_no", "train_name", "train_type", "seq", "station_code",
    "halts_here", "sch_arrival", "sch_arrival_date", "sch_departure",
    "sch_departure_date", "act_arrival", "act_arrival_date", "act_departure",
    "act_departure_date", "delay_arrival_min", "delay_departure_min",
    "platform", "distance_km", "lat", "lng", "departed", "reported",
    "reported_at",
]


def scan_one(path):
    name = os.path.basename(path)
    rec = {
        "file": name,
        "bytes": os.path.getsize(path),
        "train_no": os.path.splitext(name)[0],
        "rows": 0,
        "runs": 0,
        "first": None,
        "last": None,
        "stations": 0,
        "with_actual": 0,
        "with_delay": 0,
        "with_latlng": 0,
        "name": "",
        "type": "",
        "headerless": False,
    }
    names, types, stations, dates = Counter(), Counter(), set(), set()
    try:
        with open(path, newline="", encoding="utf-8", errors="replace") as fh:
            first = fh.readline()
            fh.seek(0)
            if first.startswith("date,train_no"):
                r = csv.DictReader(fh)
            else:
                rec["headerless"] = True
                r = csv.DictReader(fh, fieldnames=HEADER)
            for row in r:
                rec["rows"] += 1
                d = row.get("date") or ""
                if d:
                    dates.add(d)
                if row.get("train_name"):
                    names[row["train_name"]] += 1
                if row.get("train_type"):
                    types[row["train_type"]] += 1
                if row.get("station_code"):
                    stations.add(row["station_code"])
                if row.get("act_arrival") or row.get("act_departure"):
                    rec["with_actual"] += 1
                if row.get("delay_arrival_min") or row.get("delay_departure_min"):
                    rec["with_delay"] += 1
                if row.get("lat") and row.get("lng"):
                    rec["with_latlng"] += 1
    except Exception as e:  # a truncated file should not stop the scan
        rec["error"] = "%s: %s" % (type(e).__name__, e)

    rec["runs"] = len(dates)
    rec["stations"] = len(stations)
    if dates:
        rec["first"], rec["last"] = min(dates), max(dates)
    if names:
        rec["name"] = names.most_common(1)[0][0]
    if types:
        rec["type"] = types.most_common(1)[0][0]
    rec["_stations"] = sorted(stations)
    rec["_types"] = dict(types)
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root")
    ap.add_argument("--out", default="stats.json")
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()

    paths = sorted(
        os.path.join(args.root, f)
        for f in os.listdir(args.root)
        if f.endswith(".csv") and not f.startswith(".")
    )
    print("%d files" % len(paths), file=sys.stderr)

    files, all_stations, all_types = [], set(), Counter()
    done = 0
    with Pool(args.workers) as pool:
        for rec in pool.imap_unordered(scan_one, paths, chunksize=8):
            all_stations.update(rec.pop("_stations"))
            all_types.update(rec.pop("_types"))
            files.append(rec)
            done += 1
            if done % 250 == 0:
                print("  %d/%d" % (done, len(paths)), file=sys.stderr)

    files.sort(key=lambda r: r["file"])
    firsts = [f["first"] for f in files if f["first"]]
    lasts = [f["last"] for f in files if f["last"]]
    summary = {
        "files": len(files),
        "bytes": sum(f["bytes"] for f in files),
        "rows": sum(f["rows"] for f in files),
        "runs": sum(f["runs"] for f in files),
        "with_actual": sum(f["with_actual"] for f in files),
        "with_delay": sum(f["with_delay"] for f in files),
        "with_latlng": sum(f["with_latlng"] for f in files),
        "stations": len(all_stations),
        "headerless": sum(1 for f in files if f.get("headerless")),
        "train_types": all_types.most_common(),
        "first": min(firsts) if firsts else None,
        "last": max(lasts) if lasts else None,
        "errors": [f for f in files if "error" in f],
    }
    json.dump({"summary": summary, "files": files}, open(args.out, "w"), indent=1)

    # The headerless files are a defect worth fixing at the source, so name them
    # rather than just counting them.
    hl = [f["file"] for f in files if f.get("headerless")]
    if hl:
        listing = os.path.splitext(args.out)[0] + ".headerless.txt"
        with open(listing, "w") as fh:
            fh.write("\n".join(hl) + "\n")
        print("%d files have no header row -> %s" % (len(hl), listing),
              file=sys.stderr)
    print(json.dumps(summary, indent=1)[:2000], file=sys.stderr)


if __name__ == "__main__":
    main()
