#!/usr/bin/env python3
"""
apply_patch.py -- point this at a Where Is My Train patch bundle and it does the
whole job: unpacks it, applies the SQL to whereismytrain.db, decodes the
`jingli` route blob, and merges everything into the CSVs in csv/.

    python3 apply_patch.py "../where_is_my_train/patch (2).zip"
    python3 apply_patch.py ../where_is_my_train/patch/          # already unpacked
    python3 apply_patch.py patch.zip --dry-run                  # inspect only

A bundle is nested two deep:

    patch.zip
      patch_wimt.zip
        jingli                        route + running-days blob
        coach_positions.txt           train_no|rake_type|stations|coach_order
        deleted.txt                   integer ids (see NOTE below)
        patch_src_station_index.bin   companion index, not needed here
      patch_db.sql.zip
        patch_db.sql                  REPLACE INTO train_info/station_info/...

Either the outer zip, either inner zip, or any directory holding these files
works -- the finder walks whatever it is given.

MERGE SEMANTICS. Patches are not always full snapshots (the older 1.2 MB bundle
carries 2,662 trains against the current one's 13,678), so nothing is ever
dropped wholesale. Rows are upserted per train_no: a train present in the patch
has its rows replaced, a train absent from it keeps whatever it already had.
Re-running the same patch is therefore idempotent, and applying an older bundle
after a newer one degrades only the trains that older bundle actually mentions.

The database is copied to whereismytrain.db.<timestamp>.bak before any write,
and patch_db.sql is applied in a single transaction -- if any statement fails
the whole thing rolls back and the CSVs are left untouched.

NOTE ON deleted.txt: these integers are read by the app into a HashSet that is
handed to the station-index reader alongside patch_src_station_index.bin, so
they address route segments, not train_info or station_info rows. Their range
(87 .. 594411) rules out train numbers. This script therefore reports the count
but deletes nothing -- guessing wrong here would silently corrupt good data.
"""

import argparse
import csv
import os
import shutil
import sqlite3
import sys
import tempfile
import zipfile
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import decode_jingli as dj

HERE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(HERE, "whereismytrain.db")
CSV_DIR = os.path.join(HERE, "csv")

WANTED = ("jingli", "patch_db.sql", "coach_positions.txt", "deleted.txt")

ROUTE_HEADER = [
    "train_no", "train_name", "seq", "station_code", "station_name", "arrival",
    "is_halt", "halt_min", "distance_km", "platform", "station_id",
    "route_inherited_from",
]
DAYS_HEADER = [
    "train_no", "train_name", "runs_on", "mon", "tue", "wed", "thu", "fri",
    "sat", "sun", "running_days_mask", "departure", "valid_from", "valid_to",
    "stops", "parent_train",
]
COACH_HEADER = ["train_no", "rake_type", "stations", "coach_order"]


def log(msg=""):
    print(msg, flush=True)


# ---------------------------------------------------------------- unpacking


def unpack(src, workdir):
    """Recursively expand zips under `src` and return {basename: path}."""
    staging = os.path.join(workdir, "unpacked")
    os.makedirs(staging, exist_ok=True)
    if os.path.isdir(src):
        shutil.copytree(src, staging, dirs_exist_ok=True)
    elif zipfile.is_zipfile(src):
        with zipfile.ZipFile(src) as z:
            z.extractall(staging)
    else:
        raise SystemExit("not a zip or directory: %s" % src)

    # Keep expanding nested zips until none are left.
    for _ in range(5):
        nested = [
            os.path.join(r, f)
            for r, _d, fs in os.walk(staging)
            for f in fs
            if f.lower().endswith(".zip")
        ]
        if not nested:
            break
        for z in nested:
            with zipfile.ZipFile(z) as zf:
                zf.extractall(os.path.dirname(z))
            os.remove(z)

    found = {}
    for root, _dirs, files in os.walk(staging):
        for f in files:
            if f in WANTED:
                found[f] = os.path.join(root, f)
    return found


# ---------------------------------------------------------------- database


def table_counts(con):
    out = {}
    for t in ("train_info", "station_info", "station_aka_info"):
        out[t] = con.execute("SELECT COUNT(*) FROM %s" % t).fetchone()[0]
    return out


def apply_sql(sql_path, backup):
    if not os.path.exists(DB):
        raise SystemExit("database not found: %s" % DB)
    shutil.copy2(DB, backup)
    log("  backup -> %s" % os.path.basename(backup))

    con = sqlite3.connect(DB)
    before = table_counts(con)
    sql = open(sql_path, encoding="utf-8").read()
    try:
        con.executescript("BEGIN;\n" + sql + "\nCOMMIT;")
    except Exception as exc:
        con.rollback()
        con.close()
        shutil.copy2(backup, DB)  # restore byte-for-byte
        raise SystemExit("SQL failed, database restored from backup: %s" % exc)
    after = table_counts(con)
    ok = con.execute("PRAGMA integrity_check").fetchone()[0]
    con.close()
    if ok != "ok":
        shutil.copy2(backup, DB)
        raise SystemExit("integrity check failed (%s), database restored" % ok)

    for t in before:
        delta = after[t] - before[t]
        log("  %-18s %6d -> %6d  (%+d)" % (t, before[t], after[t], delta))
    log("  integrity_check: ok")
    return before, after


def db_lookups():
    con = sqlite3.connect(DB)
    gid = {
        int(g): (c, t)
        for c, g, t in con.execute(
            "SELECT station_code, gid, title FROM station_info WHERE gid != ''"
        )
    }
    names = dict(con.execute("SELECT train_no, train_name FROM train_info"))
    con.close()
    return gid, names


# ---------------------------------------------------------------- csv merge


def read_csv(path, header):
    """Return (rows, header). Missing file -> empty, using the given header."""
    if not os.path.exists(path):
        return [], header
    with open(path, newline="", encoding="utf-8") as fh:
        rows = list(csv.reader(fh))
    if not rows:
        return [], header
    return rows[1:], rows[0]


def merge_by_train(path, header, fresh_rows, touched):
    """Replace every row whose train_no is in `touched`, keep the rest."""
    existing, old_header = read_csv(path, header)
    if old_header != header and existing:
        log("  ! %s has an older column layout; rewriting it" % os.path.basename(path))
        existing = []
    kept = [r for r in existing if r and r[0] not in touched]
    merged = kept + fresh_rows
    # train_no, then numeric seq where the file has one
    if "seq" in header:
        idx = header.index("seq")
        merged.sort(key=lambda r: (r[0], int(r[idx])))
    else:
        merged.sort(key=lambda r: r[0])
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(merged)
    return len(kept), len(fresh_rows), len(merged)


def build_rows(jingli_path, gid, names):
    """Decode jingli into (route rows, halt rows, day rows, touched train set)."""
    buf = open(jingli_path, "rb").read()
    recs, stop_size, total = dj.decode(jingli_path)
    log("  stop_size %d, %d records, %d bytes parsed exactly" % (stop_size, len(recs), total))

    by = {}
    for r in recs:
        by.setdefault(r["train_no"], r)

    def route_for(rec):
        if rec["has_own_route"]:
            return dj.decode_route(buf, rec), ""
        parent = by.get("%05d" % rec["parent_train"])
        if parent is None or not parent["has_own_route"]:
            return [], ""
        return (
            dj.decode_route(buf, parent, start_minute=rec["minute_offset"]),
            parent["train_no"],
        )

    routes, halts, days = [], [], []
    unresolved = derived = orphan = 0
    for train in sorted(by):
        rec = by[train]
        name = names.get(train, "")
        points, inherited = route_for(rec)
        if not rec["has_own_route"]:
            derived += 1
            if not points:
                orphan += 1
        for p in points:
            code, title = gid.get(p["station_id"], ("", ""))
            if not code:
                unresolved += 1
            row = [
                train, name, p["seq"], code, title, p["arr"], int(p["is_halt"]),
                p["halt_min"], "%.2f" % p["distance_km"], p["platform"],
                p["station_id"], inherited,
            ]
            routes.append(row)
            if p["is_halt"]:
                halts.append(row)
        mask = rec["running_days"]
        hh, mm = divmod(rec["minute_offset"], 60)
        days.append(
            [train, name, dj.days_str(mask)]
            + [(mask >> i) & 1 for i in range(7)]
            + [
                mask,
                "%02d:%02d" % (hh % 24, mm),
                dj.fmt_date(rec["valid_from"]),
                dj.fmt_date(rec["valid_to"]),
                rec["stops"],
                "%05d" % rec["parent_train"] if rec["parent_train"] else "",
            ]
        )

    log("  %d trains (%d derived, %d could not resolve a parent)" % (len(by), derived, orphan))
    if unresolved:
        log("  %d route points reference a station id absent from station_info" % unresolved)
    return routes, halts, days, set(by)


def build_coach_rows(path):
    rows, seen = [], set()
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("|")
            if len(parts) < 4:
                continue
            rows.append(parts[:4])
            seen.add(parts[0])
    return rows, seen


# ---------------------------------------------------------------- driver


def main():
    ap = argparse.ArgumentParser(
        description="Unpack a Where Is My Train patch bundle, apply its SQL to "
        "whereismytrain.db, decode the jingli route blob, and merge the "
        "results into the CSVs in csv/.",
        epilog='example: python3 apply_patch.py "../where_is_my_train/patch (2).zip"',
    )
    ap.add_argument("bundle", help="patch zip, inner zip, or unpacked directory")
    ap.add_argument("--dry-run", action="store_true",
                    help="report what the bundle holds; change nothing")
    ap.add_argument("--skip-sql", action="store_true",
                    help="decode jingli and rebuild CSVs without touching the database")
    args = ap.parse_args()

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    workdir = tempfile.mkdtemp(prefix="wimt-patch-")
    try:
        log("bundle: %s" % args.bundle)
        found = unpack(args.bundle, workdir)
        for w in WANTED:
            mark = "found" if w in found else "absent"
            size = os.path.getsize(found[w]) if w in found else 0
            log("  %-24s %-7s %s" % (w, mark, "{:,} bytes".format(size) if size else ""))
        if not found:
            raise SystemExit("no recognisable patch files inside %s" % args.bundle)

        if "deleted.txt" in found:
            n = sum(1 for line in open(found["deleted.txt"]) if line.strip())
            log("  deleted.txt lists %d ids (route-segment ids; not applied -- see docstring)" % n)

        if args.dry_run:
            log("\ndry run: nothing written")
            return

        os.makedirs(CSV_DIR, exist_ok=True)

        if "patch_db.sql" in found and not args.skip_sql:
            log("\napplying patch_db.sql")
            backup = os.path.join(HERE, "whereismytrain.db.%s.bak" % stamp)
            apply_sql(found["patch_db.sql"], backup)
        elif args.skip_sql:
            log("\nskipping SQL (--skip-sql)")

        gid, names = db_lookups()

        if "jingli" in found:
            log("\ndecoding jingli")
            routes, halts, days, touched = build_rows(found["jingli"], gid, names)
            log("\nmerging CSVs")
            for path, header, rows in (
                ("train_routes.csv", ROUTE_HEADER, routes),
                ("train_halts.csv", ROUTE_HEADER, halts),
                ("train_running_days.csv", DAYS_HEADER, days),
            ):
                kept, new, total = merge_by_train(
                    os.path.join(CSV_DIR, path), header, rows, touched
                )
                log("  %-24s %6d kept + %6d from patch = %6d rows" % (path, kept, new, total))

        if "coach_positions.txt" in found:
            rows, seen = build_coach_rows(found["coach_positions.txt"])
            kept, new, total = merge_by_train(
                os.path.join(CSV_DIR, "coach_positions.csv"), COACH_HEADER, rows, seen
            )
            log("  %-24s %6d kept + %6d from patch = %6d rows" % ("coach_positions.csv", kept, new, total))

        log("\ndone.")
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
