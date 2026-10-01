#!/usr/bin/env python3
"""
build_metadata.py -- turn scan_dataset.py's stats.json into everything Kaggle
asks for: dataset-metadata.json (title, subtitle, description, keywords, and a
per-file description plus a column schema for all 12,935 CSVs).

    python3 kaggle/build_metadata.py \
        --stats stats.json \
        --description kaggle/description.md \
        --out kaggle/dataset-metadata.json

The description template carries @@TOKENS@@ that get replaced with the real
totals, so no number in the dataset card is typed by hand.

--schema controls how much of the column documentation is emitted:

    all    (default) every file gets the full 24-column schema. This is what
           makes Kaggle's "Column Description" check pass for the whole dataset,
           and it is also what makes the file large. Expect tens of MB.
    first  only the first file carries the schema. Small output, useful for
           eyeballing the structure before generating the real thing.
"""

import argparse
import json
import os
import sys

DATASET_ID = "yadavsaurabh5517/indian-railways-dataset"
TITLE = "Indian Railways: stop-level delay history"
SUBTITLE = "Scheduled vs actual arrival, departure and delay at every stop"

# Kaggle rejects keywords that are not real tag slugs, so this list sticks to
# ones that exist on the site. Anything extra is easier to add in the UI picker.
KEYWORDS = [
    "india",
    "transportation",
    "time series analysis",
    "tabular",
    "travel",
]

# (name, kaggle type, description). The types are the ones Kaggle's schema
# vocabulary accepts: string / number / integer / datetime / date / boolean.
COLUMNS = [
    ("date", "date",
     "Journey start date at the origin (YYYY-MM-DD). Constant for every stop of "
     "one run; (train_no, date) identifies a journey."),
    ("train_no", "string",
     "Five digit Indian Railways train number, same as the filename. Read as a "
     "string: leading zeros are part of the number."),
    ("train_name", "string",
     "Name of the service, repeated on every row so one file stands alone. Can "
     "change across years if the number is reassigned."),
    ("train_type", "string",
     "Service class. 24 distinct values in two vocabularies that the upstream "
     "API used at different times: short codes (EMU, MEX, SUF, PAS, SUB, RAJ, "
     "SHT, JSH, DRNT, GBR, MEMU, DEMU, TOD) for almost all rows, and long "
     "labels (MAIL/EXPRESS, SUPERFAST, PASSENGER, AC EXPRESS) for under 0.4%. "
     "Normalise before grouping: SUF and SUPERFAST are the same class."),
    ("seq", "integer",
     "Position of this stop in the train's full route. Not contiguous, because "
     "the route contains points that are not listed as rows."),
    ("station_code", "string",
     "Indian Railways station code, e.g. NDLS, PUNE, MAS."),
    ("halts_here", "integer",
     "1 if the timetable gives this stop a scheduled halt, 0 if the train is "
     "only routed through it."),
    ("sch_arrival", "string",
     "Scheduled arrival time, HH:MM, 24 hour. Blank at the originating station."),
    ("sch_arrival_date", "string",
     "Calendar date of the scheduled arrival, 'DD Mon YYYY'. This is the column "
     "that advances when a run crosses midnight."),
    ("sch_departure", "string",
     "Scheduled departure time, HH:MM, 24 hour. Blank at the final station."),
    ("sch_departure_date", "string",
     "Calendar date of the scheduled departure, 'DD Mon YYYY'."),
    ("act_arrival", "string",
     "Observed arrival time, HH:MM. Blank where this stop never filed a status "
     "update (see the reported column)."),
    ("act_arrival_date", "string",
     "Calendar date of the observed arrival, 'DD Mon YYYY'."),
    ("act_departure", "string",
     "Observed departure time, HH:MM. Blank where this stop never reported."),
    ("act_departure_date", "string",
     "Calendar date of the observed departure, 'DD Mon YYYY'."),
    ("delay_arrival_min", "number",
     "Minutes late arriving, one decimal place. Never negative: the source "
     "reports 0 for both on-time and early, so this is a lower bound."),
    ("delay_departure_min", "number",
     "Minutes late departing, one decimal place. Same non-negative caveat as "
     "delay_arrival_min."),
    ("platform", "string",
     "Platform the train used. A string, not a number, because values like '2A' "
     "occur. Frequently blank."),
    ("distance_km", "number",
     "Cumulative distance from the originating station in kilometres, three "
     "decimal places."),
    ("lat", "number", "Station latitude in decimal degrees, six decimal places."),
    ("lng", "number", "Station longitude in decimal degrees, six decimal places."),
    ("departed", "integer",
     "1 if the train had already left this stop as of the last observation of "
     "the run, 0 otherwise."),
    ("reported", "integer",
     "1 if this stop ever filed a live status update, 0 if it never did. Filter "
     "on reported == 1 before computing anything about delays."),
    ("reported_at", "string",
     "Timestamp of that status update, ISO 8601 with a +05:30 offset. Blank "
     "when reported is 0."),
]

SCHEMA = {"fields": [{"name": n, "description": d, "type": t}
                     for n, t, d in COLUMNS]}


def commas(n):
    return "{:,}".format(n)


def file_description(rec):
    """One line describing a single train's file, built from its own contents."""
    name = rec.get("name") or "unnamed service"
    ttype = rec.get("type") or "unclassified"
    if not rec["rows"]:
        return ("Train %s: no usable rows. The file is present so the train "
                "number is accounted for." % rec["train_no"])
    span = ("%s to %s" % (rec["first"], rec["last"])) if rec["first"] else "no dated runs"
    pct = round(100.0 * rec["with_actual"] / rec["rows"]) if rec["rows"] else 0
    warn = ""
    if rec.get("headerless"):
        warn = (" NOTE: this file has no header row and starts on a data row. "
                "Read it with header=None and supply the column names, or "
                "pandas will turn its first record into the column headers.")
    return (
        "Train %s, %s (%s). %s stop records from %s runs at %s stations, %s. "
        "%d%% of rows carry an observed arrival or departure time. One row per "
        "scheduled stop; a single journey is (train_no, date).%s"
        % (rec["train_no"], name, ttype, commas(rec["rows"]), commas(rec["runs"]),
           commas(rec["stations"]), span, pct, warn)
    )


def fill(template, s):
    # The headerless-files caveat only belongs in the card while such files
    # exist. Once they are repaired the whole block comes out, rather than
    # printing "0 files have no header row" at a reader.
    headerless = s.get("headerless", 0)
    start, end = template.find("<!--HL_START-->"), template.find("<!--HL_END-->")
    if start != -1 and end != -1:
        if headerless:
            template = template.replace("<!--HL_START-->", "").replace(
                "<!--HL_END-->", "")
        else:
            template = template[:start] + template[end + len("<!--HL_END-->"):]
    if headerless:
        template = template.replace(
            "<!--HL_INTRO-->",
            "\n\nNote that @@HEADERLESS@@ of them are missing that header row; "
            "see the caveats before reading those.")
    else:
        template = template.replace("<!--HL_INTRO-->", "")

    pct = round(100.0 * s["with_actual"] / s["rows"], 1) if s["rows"] else 0
    repl = {
        "@@ROWS@@": commas(s["rows"]),
        "@@RUNS@@": commas(s["runs"]),
        "@@TRAINS@@": commas(s["files"]),
        "@@STATIONS@@": commas(s["stations"]),
        "@@FIRST@@": s["first"] or "?",
        "@@LAST@@": s["last"] or "?",
        "@@WITH_ACTUAL_PCT@@": ("%g" % pct),
        "@@SIZE_GB@@": "%.0f" % (s["bytes"] / 1024.0**3),
        "@@HEADERLESS@@": commas(s.get("headerless", 0)),
    }
    for k, v in repl.items():
        template = template.replace(k, v)
    left = [k for k in repl if k in template]
    if left:
        print("warning: unreplaced tokens %s" % left, file=sys.stderr)
    return template


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stats", default="stats.json")
    ap.add_argument("--description", default="kaggle/description.md")
    ap.add_argument("--out", default="kaggle/dataset-metadata.json")
    ap.add_argument("--schema", choices=("all", "first"), default="all")
    ap.add_argument("--id", default=DATASET_ID)
    ap.add_argument("--no-readme", dest="readme", action="store_false",
                    help="omit the README.md resource entry")
    ap.add_argument("--data-dir",
                    help="check every resource against this folder and drop "
                         "the ones with no file, which the uploader treats as "
                         "a fatal error")
    ap.add_argument("--prefix", default="trains/",
                    help="folder the files sit in inside the dataset. Kaggle "
                         "matches resources by exact path, so a name that "
                         "misses the prefix is silently ignored.")
    args = ap.parse_args()

    data = json.load(open(args.stats))
    summary, files = data["summary"], data["files"]
    description = fill(open(args.description).read(), summary)

    resources = []
    for i, rec in enumerate(files):
        r = {"path": args.prefix + rec["file"],
             "description": file_description(rec),
             "totalBytes": rec["bytes"]}
        if args.schema == "all" or i == 0:
            r["schema"] = SCHEMA
        resources.append(r)

    # README.md rides along in the dataset folder, so it needs a description
    # too or Kaggle counts it as an undocumented file.
    if args.readme:
        resources.append({
            "path": args.prefix + "README.md",
            "description": "Dataset overview: file layout, the meaning of every "
                           "column, and the three caveats that change how the "
                           "delay figures should be read (non-negative delays, "
                           "the 2020-21 gap, and stops that never report).",
        })

    if args.data_dir:
        missing = [r["path"] for r in resources
                   if not os.path.exists(os.path.join(args.data_dir,
                                                      r["path"]))]
        if missing:
            print("dropping %d resources with no file on disk: %s"
                  % (len(missing), missing[:3]), file=sys.stderr)
            gone = set(missing)
            resources = [r for r in resources if r["path"] not in gone]

    meta = {
        "id": args.id,
        "title": TITLE,
        "subtitle": SUBTITLE,
        "description": description,
        "licenses": [{"name": "GPL-3.0"}],
        "keywords": KEYWORDS,
        "resources": resources,
    }

    # Compact separators, because 12,936 resources x 24 fields is a lot of
    # whitespace: pretty-printing this costs roughly 40 MB and buys nothing,
    # since nobody reads it by hand. Use --schema first for a readable one.
    with open(args.out, "w") as fh:
        json.dump(meta, fh, separators=(",", ":"))

    # Next to the template, not next to --out: --out can point into the data
    # folder, and anything left there gets uploaded as a data file.
    filled = os.path.join(os.path.dirname(args.description) or ".",
                          "description.filled.md")
    with open(filled, "w") as fh:
        fh.write(description)
    print("wrote %s  (paste this into Kaggle's description box)" % filled)

    size = os.path.getsize(args.out)
    print("wrote %s  (%s resources, %.1f MB)"
          % (args.out, commas(len(resources)), size / 1024.0**2))
    print("subtitle is %d chars" % len(SUBTITLE))
    if size > 20 * 1024**2:
        print("note: that is large for a single metadata upload; --schema first "
              "produces a small version for review.", file=sys.stderr)


if __name__ == "__main__":
    main()
