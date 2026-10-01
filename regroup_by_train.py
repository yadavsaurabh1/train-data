#!/usr/bin/env python3
"""
regroup_by_train.py -- turn the day-major tree into a train-major one.

    livestatus/days/2020-01-05/12624.csv   (9.5M files, one per train-day)
        ->
    livestatus/trains/12624.csv            (~13.7k files, one per train)

Each train file holds every run of that train, oldest first, with the same
columns as before. Nothing needs adding: `date` is already the first column and
is constant within a train-day file -- it is the journey's start date, while the
per-stop dates (sch_arrival_date and friends) are what cross midnight. So this
is a pure concatenation, and a run is still identified by (train_no, date).

    python3 regroup_by_train.py                    # days/ -> trains/
    python3 regroup_by_train.py --gzip             # trains/12624.csv.gz
    python3 regroup_by_train.py --since 2023-01-01 # only these days

WHY 13.7k FILES IS WORTH IT. Same bytes, four hundred times fewer inodes. On
the exFAT drive, where every file burns a 256 KB allocation block regardless of
its size, 9.5M files need ~2.4 TB of slack and 13.7k need ~3.5 GB. It is also
simply the shape most analysis wants: one read gets a train's whole history.

HOW IT AVOIDS EATING MEMORY. Days are processed one at a time and buffered in
full -- a single day is only ~14 MB -- then appended to the per-train files and
flushed. Output handles are kept open across days so 9.5M appends do not become
9.5M opens; the process file limit is raised to make room, and if the OS will
not allow enough, the least recently used handles are closed and reopened on
demand.

RESUME. Completed days are recorded in trains/.regroup_state.json and skipped on
a later run. The state is written only after a day has been fully appended and
flushed, so an interrupt between days is clean. An interrupt *during* a day's
append is the one unsafe window -- rare, and small, but if it happens use
--restart rather than --resume, since appending that day again would duplicate
its rows.
"""

import argparse
import csv
import gzip
import io
import json
import os
import resource
import shutil
import sys
import time
from collections import OrderedDict

HERE = os.path.dirname(os.path.abspath(__file__))
DAYS = os.path.join(HERE, "livestatus", "days")
TRAINS = os.path.join(HERE, "livestatus", "trains")
STATE = ".regroup_state.json"
SUMMARY = "_summary.csv"


def human(n):
    for u in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return "%.1f %s" % (n, u)
        n /= 1024.0
    return "%.1f PB" % n


def clock(sec):
    if sec is None or sec != sec or sec == float("inf"):
        return "--:--"
    sec = int(sec)
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    return "%d:%02d:%02d" % (h, m, s) if h else "%d:%02d" % (m, s)


def raise_fd_limit(want):
    """Ask the OS for enough descriptors to keep every train file open."""
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target = min(max(want, soft), hard if hard > 0 else want)
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
        soft = target
    except (ValueError, OSError):
        pass
    return soft


class Handles:
    """Open output files, capped, evicting least-recently-used when full."""

    def __init__(self, out_dir, limit, use_gzip):
        self.dir = out_dir
        self.limit = max(16, limit)
        self.gzip = use_gzip
        self.open = OrderedDict()
        self.seen = set()          # trains whose file already has a header
        self.reopens = 0

    def path(self, train):
        return os.path.join(self.dir, "%s.csv%s" % (train, ".gz" if self.gzip else ""))

    def get(self, train):
        """Return (handle, needs_header)."""
        fh = self.open.get(train)
        if fh is not None:
            self.open.move_to_end(train)
            return fh, False
        while len(self.open) >= self.limit:
            _, victim = self.open.popitem(last=False)
            victim.close()
        p = self.path(train)
        fresh = train not in self.seen and not os.path.exists(p)
        if train in self.seen:
            self.reopens += 1
        if self.gzip:
            fh = gzip.open(p, "at", newline="", encoding="utf-8")
        else:
            fh = open(p, "a", newline="", encoding="utf-8")
        self.open[train] = fh
        self.seen.add(train)
        return fh, fresh

    def flush(self):
        for fh in self.open.values():
            fh.flush()

    def close(self):
        for fh in self.open.values():
            fh.close()
        self.open.clear()


def main():
    ap = argparse.ArgumentParser(
        description="Rewrite livestatus/days (one file per train-day) as "
                    "livestatus/trains (one file per train).")
    ap.add_argument("--days", default=DAYS, help="source day-major tree")
    ap.add_argument("--out", default=TRAINS, help="destination train-major tree")
    ap.add_argument("--since", metavar="DATE", help="earliest day folder")
    ap.add_argument("--until", metavar="DATE", help="latest day folder")
    ap.add_argument("--gzip", action="store_true",
                    help="write trains/12624.csv.gz (roughly 5x smaller)")
    ap.add_argument("--restart", action="store_true",
                    help="delete the destination and start over")
    ap.add_argument("--max-open", type=int, default=16384,
                    help="descriptors to keep for output files (default 16384)")
    ap.add_argument("--no-progress", action="store_true")
    args = ap.parse_args()

    src, dst = os.path.abspath(args.days), os.path.abspath(args.out)
    if not os.path.isdir(src):
        raise SystemExit("no such directory: %s" % src)
    if args.restart and os.path.isdir(dst):
        shutil.rmtree(dst)
    os.makedirs(dst, exist_ok=True)

    state_path = os.path.join(dst, STATE)
    done = set()
    if os.path.exists(state_path):
        try:
            with open(state_path) as fh:
                done = set(json.load(fh).get("done", []))
        except (OSError, ValueError):
            done = set()

    days = sorted(d for d in os.listdir(src) if os.path.isdir(os.path.join(src, d)))
    if args.since:
        days = [d for d in days if d >= args.since]
    if args.until:
        days = [d for d in days if d <= args.until]
    todo = [d for d in days if d not in done]

    print("  source : %s" % src)
    print("  dest   : %s" % dst)
    print("  days   : %d total, %d already done, %d to process"
          % (len(days), len(days) - len(todo), len(todo)))
    if not todo:
        print("  nothing to do")
        return

    soft = raise_fd_limit(args.max_open + 64)
    limit = max(64, min(args.max_open, soft - 64))
    print("  file descriptors: %d available, keeping up to %d output files open"
          % (soft, limit))

    handles = Handles(dst, limit, args.gzip)
    tty = (not args.no_progress) and sys.stdout.isatty()
    t0 = time.time()
    n_rows = n_files = n_bytes = 0
    last_draw = 0.0

    try:
        for i, day in enumerate(todo, 1):
            d = os.path.join(src, day)
            try:
                names = sorted(os.listdir(d))
            except OSError:
                continue

            # Buffer the whole day (~14 MB) so the append phase is short and a
            # clean interrupt point exists between days.
            buffered = []
            for fn in names:
                if not fn.endswith(".csv") or fn == SUMMARY:
                    continue
                train = fn[:-4]
                p = os.path.join(d, fn)
                try:
                    with open(p, "r", newline="", encoding="utf-8") as fh:
                        head = fh.readline()
                        body = fh.read()
                except OSError:
                    continue
                if not body:
                    continue
                buffered.append((train, head, body))
                n_files += 1
                n_bytes += len(body)

            for train, head, body in buffered:
                fh, fresh = handles.get(train)
                if fresh:
                    fh.write(head)
                fh.write(body)
                n_rows += body.count("\n")

            handles.flush()
            done.add(day)
            tmp = state_path + ".tmp"
            with open(tmp, "w") as fh:
                json.dump({"done": sorted(done)}, fh)
            os.replace(tmp, state_path)

            now = time.time()
            if tty and now - last_draw >= 0.25:
                last_draw = now
                el = max(now - t0, 1e-9)
                rate = i / el
                eta = (len(todo) - i) / rate if rate > 0 else None
                line = ("  %5.1f%%  day %d/%d  %s trains-days  %s  %s/s  eta %s"
                        % (100.0 * i / len(todo), i, len(todo),
                           format(n_files, ","), human(n_bytes),
                           human(n_bytes / el), clock(eta)))
                cols = shutil.get_terminal_size((100, 24)).columns
                sys.stdout.write("\r" + line[:cols - 1] + "\033[K")
                sys.stdout.flush()
    except KeyboardInterrupt:
        if tty:
            sys.stdout.write("\r\033[K")
        print("  interrupted after %d complete day(s)." % len(done))
        print("  rerun to resume. If it stopped mid-day, use --restart instead:")
        print("  re-appending a half-written day would duplicate its rows.")
        return
    finally:
        handles.close()

    if tty:
        sys.stdout.write("\r\033[K")
    el = max(time.time() - t0, 1e-9)
    try:
        out_files = sum(1 for e in os.scandir(dst) if e.is_file() and e.name != STATE)
    except OSError:
        out_files = 0
    print("  read   : %s train-days, %s rows, %s"
          % (format(n_files, ","), format(n_rows, ","), human(n_bytes)))
    print("  wrote  : %s train files in %s (%s/s)"
          % (format(out_files, ","), clock(el), human(n_bytes / el)))
    if handles.reopens:
        print("  note   : %s handle reopens -- raise --max-open to cut them"
              % format(handles.reopens, ","))
    print("  %s" % dst)


if __name__ == "__main__":
    main()
