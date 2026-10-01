#!/usr/bin/env python3
"""
fetch_live_status.py -- harvest historical running/delay data from
whereismytrain.in's live_status cache into a browsable CSV dataset.

    https://whereismytrain.in/cache/live_status?date=DD-MM-YYYY&train_no=NNNNN

It walks BACKWARDS from a start date (default today), one day at a time, for
every train you point it at, and stops when it stops finding data.

    # everything, from today backwards, until each train runs dry
    python3 fetch_live_status.py

    # the full archive, in two passes that step over the 2020-21 dead zone
    python3 fetch_live_status.py --workers 96 --until 2021-12-01
    python3 fetch_live_status.py --workers 96 --from 2020-03-20 --until 2020-01-01

    # one train, a year back
    python3 fetch_live_status.py --train 12624 --until 2025-08-18

    # (re)build the per-day summaries and the optional flat files
    python3 fetch_live_status.py --build-dataset

LAYOUT -- a folder per day, a CSV per train inside it

    livestatus/
      days/
        2026-08-17/
          _summary.csv        every train that ran that day, one row each
          12624.csv           that train's full run: every stop, with delays
          12625.csv
          ...
        2026-08-16/
          ...
      dataset/                optional flat files, only if --build-dataset
        stop_delays.csv       every train CSV concatenated
        train_runs.csv        every _summary.csv concatenated
      state.sqlite            fetch ledger (resume + skip)

    import pandas as pd
    pd.read_csv("livestatus/days/2026-08-17/12624.csv")     # one train, one day
    pd.read_csv("livestatus/days/2026-08-17/_summary.csv")  # that whole day

Each train CSV is one row per station on that day's run: scheduled and actual
arrival/departure, both delay figures, platform, distance, coordinates. Train
identity (name, type) repeats on every row so a single file stands alone.

Because each train-day is its own file, writing is naturally idempotent -- a
refetch overwrites exactly one file and can never duplicate or interleave rows.

RESUMING AND SKIPPING. THE CSV FILES ARE THE SOURCE OF TRUTH. On startup the
day folders are scanned, and any train-day with a CSV already on disk counts as
fetched no matter what the ledger says. state.sqlite is only an index -- it
makes restarts fast and additionally remembers the *negative* outcomes (no_data,
dead, srv_500) that leave no file behind. If the two disagree, the files win.

That matters because a ledger can be lost: killed before a flush, copied from
another machine, deleted. Recovering it costs one directory walk; refetching
those hours costs hours. --reindex rebuilds the ledger from the files outright;
--no-reconcile turns the startup scan off if you ever want the old behaviour.

Pass --refetch to ignore all of it, or --refetch-partial to re-pull today's rows
once the day has finished.

Only *today* is ever marked `partial` -- a train caught mid-journey has reported
just the stations it has passed. Past dates are recorded `ok` even when a few
stops never reported, because some stations simply never file updates: roughly
3 of 23 on the train used for testing. Treating that as incomplete would
refetch the entire archive on every run and never converge.

STOPPING. --until DATE is a hard floor (inclusive), defaulting to 2020-01-01 --
about as far back as the API holds data. Giving up early uses two thresholds:

  --give-up-after (30)   blank days tolerated BEFORE a train yields anything.
                         Cheap rejection of numbers that are not real trains.
  --gap-tolerance (700)  blank days tolerated AFTER it has yielded something.
                         Once we know a train is real, a long silence is a gap,
                         not an ending.

THE 2020-21 GAP MATTERS. Passenger services stopped in the pandemic, and for
train 12624 the hole runs from 2020-03-22 to somewhere in late 2021 -- about
650 consecutive dateless days, verified by bisecting the API. A single
30-day rule aborts the walk inside that hole and never reaches the 2020 data on
the far side, which is why the two thresholds exist.

Crossing it blindly still costs ~650 wasted requests per train (roughly 9M
across the full fleet). Cheaper to step over it with two passes, as above, or
with --skip-range 2020-03-22..2021-11-30. Do not make skipping the default: a
few specials did run during the shutdown, and blanket-skipping would silently
drop them.

WHICH DAYS. By default a train is only queried on days its timetable says it
runs, read from csv/train_running_days.csv (decoded by decode_jingli.py). That
skips roughly a third of all requests and costs nothing in coverage. Use
--all-days to ignore the timetable.

API QUIRKS worth knowing, all handled here:
  * Errors come back as HTTP 200 with an {"error": ...} body, not a 4xx.
      "not running on the specified date"  -> no data for this date
      "no longer running"                  -> dead train, abandon it entirely
  * Future dates return a *projected* timetable with no actuals. Anything after
    today is skipped; a response where no stop carries `updated_at` is recorded
    as unobserved rather than banked as a real observation.

SPEED. Each worker holds one keep-alive HTTPS connection, which is where
essentially all the throughput comes from -- measured against this API:

    fresh connection per request,  8 threads     11 req/s   (the old behaviour)
    keep-alive,                   48 threads     73 req/s
    keep-alive,                   96 threads    145 req/s
    keep-alive,                  250 threads    149-173 req/s   <- the plateau
    keep-alive,                  500 threads    119-137 req/s   <- worse

MORE WORKERS STOP HELPING AROUND 250, AND 500 IS SLOWER. It is not contention
between workers. The cleanest test: 10 separate OS processes x 50 threads,
sharing absolutely nothing -- no locks, no queue, no common database -- reached
195 req/s against 166 req/s for a single process of 250 threads. Removing all
communication bought ~17%. The ceiling is the server and the round-trip, not us.

What does degrade past ~250 is CPU spent per request, purely on scheduling more
OS threads than there is work for: 2.8 ms/req at 96 workers, 4.1 ms at 250,
6.8 ms at 500. So --workers 250 is the sweet spot; higher trades throughput for
heat. Sharding by date across processes would dodge the GIL but reintroduce a
worse cost: each shard would have to re-probe every train independently to
learn it is dead, since the give-up counters are per-walk.

Two things that WOULD have throttled a full-scale run were fixed instead:
  * The ledger no longer commits on the request path. One writer thread owns
    SQLite and drains a queue, so no worker ever waits on fsync.
  * Day summaries are maintained in memory, not rebuilt by rescanning the day
    folder. Rebuilding a full 13,678-train day by re-reading every file costs
    ~5.2 s and used to repeat every --summary-every seconds per active day;
    the incremental index flushes the same day in ~12 ms, about 450x faster.

The API returns a hard 500 for certain (train, date) pairs -- persistently, not
under load: replaying failures one at a time, 0.4s apart, reproduced every one.
Those are recorded as `srv_500` and skipped on later runs instead of being
retried forever; --retry-errors forces another attempt. 429 and 503 are treated
as real throttling and park every worker for the advertised interval.

SCALE, PLAINLY. 13,678 trains x ~2,400 days is tens of millions of requests and
the same number of small files. Even at 145/s that is days of continuous
running against someone else's server, so prefer a narrow --from/--until window
or a --trains-file subset over pointing it at everything at once. If you intend
to republish the result, check the site's terms and record the provenance in
the dataset card.
"""

import argparse
import csv
import http.client
import json
import os
import queue
import random
import shutil
import socket
import sqlite3
import ssl
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "livestatus")
DAYS = os.path.join(OUT, "days")
DATASET = os.path.join(OUT, "dataset")
STATE = os.path.join(OUT, "state.sqlite")
TRAIN_CSV = os.path.join(HERE, "csv", "train_running_days.csv")

API_HOST = "whereismytrain.in"
API_PATH = "/cache/live_status?date=%s&train_no=%s"
API = "https://" + API_HOST + API_PATH
UA = "train-data-research/1.0 (+historical schedule adherence dataset)"
SUMMARY_NAME = "_summary.csv"

STOP_HEADER = [
    "date", "train_no", "train_name", "train_type", "seq", "station_code",
    "halts_here", "sch_arrival", "sch_arrival_date", "sch_departure",
    "sch_departure_date", "act_arrival", "act_arrival_date", "act_departure",
    "act_departure_date", "delay_arrival_min", "delay_departure_min",
    "platform", "distance_km", "lat", "lng", "departed", "reported",
    "reported_at",
]
RUN_HEADER = [
    "date", "train_no", "train_name", "train_type", "source_station",
    "destination_station", "distance_km", "stops", "reported_stops",
    "complete", "max_delay_min", "final_delay_min", "status", "fetched_at",
]

# Status values written to the ledger.
OK = "ok"              # settled: past date, or every stop reported
PARTIAL = "partial"    # today's run, still unfolding
NO_DATA = "no_data"    # "not running on the specified date"
DEAD = "dead"          # "no longer running" -> stop trying this train
UNOBSERVED = "unobs"   # schedule returned, nothing actually reported
ERROR = "error"        # transient network/parse failure; retried on a later run
SRV_ERR = "srv_500"    # the API itself 500s for this (train, date) -- see below

_print_lock = threading.Lock()
_progress = None  # set to a Progress once a run starts


def log(msg):
    """Print a permanent line without leaving the live status bar smeared."""
    with _print_lock:
        if _progress is not None:
            _progress.erase()
        print(msg, flush=True)
        if _progress is not None:
            _progress.draw()


class Progress:
    """One self-rewriting status line, so a long run says what it is doing
    without scrolling thousands of lines past you.

    Falls back to an occasional plain line when stdout is not a terminal, so
    piping to a file or a log stays readable instead of filling with \\r."""

    SPIN = "|/-\\"

    def __init__(self, enabled, interval=0.25):
        self.enabled = enabled
        self.tty = enabled and sys.stdout.isatty()
        self.interval = interval
        self.lock = threading.Lock()
        self.counts = defaultdict(int)
        self.current = ""
        self.days = 0
        self.trains_left = 0
        self.started = time.time()
        self.frame = 0
        self.width = 0
        self._last_plain = time.time()

    # -- state updates, called from the workers -------------------------
    def set_current(self, day, train_no):
        with self.lock:
            self.current = "%s %s" % (train_no, day.isoformat())

    def tick(self, status):
        with self.lock:
            self.counts[status] += 1

    def set_context(self, trains_left=None, days=None):
        with self.lock:
            if trains_left is not None:
                self.trains_left = trains_left
            if days is not None:
                self.days = days

    # -- rendering ------------------------------------------------------
    def _line(self):
        with self.lock:
            total = sum(self.counts.values())
            counts = dict(self.counts)
            cur, days, left = self.current, self.days, self.trains_left
        el = time.time() - self.started
        rate = total / el if el else 0.0
        m, s = divmod(int(el), 60)
        h, m = divmod(m, 60)
        clock = ("%d:%02d:%02d" % (h, m, s)) if h else ("%d:%02d" % (m, s))
        # Order the tallies so the interesting ones lead.
        order = [OK, PARTIAL, UNOBSERVED, NO_DATA, DEAD, SRV_ERR, ERROR]
        tally = " ".join(
            "%s %d" % (k, counts[k]) for k in order if counts.get(k)
        ) or "starting"
        bits = [
            "%s %s" % (self.SPIN[self.frame % 4], clock),
            "%d req %.1f/s" % (total, rate),
            tally,
        ]
        if cur:
            bits.append("now %s" % cur)
        if days:
            bits.append("%d days" % days)
        if left:
            bits.append("%d trains queued" % left)
        return "  ".join(bits)

    def draw(self):
        if not self.tty:
            return
        line = self._line()
        cols = shutil.get_terminal_size((100, 24)).columns
        if len(line) > cols - 1:
            line = line[: cols - 2] + "…"
        self.width = len(line)
        sys.stdout.write("\r" + line + "\033[K")
        sys.stdout.flush()

    def erase(self):
        if not self.tty or not self.width:
            return
        sys.stdout.write("\r\033[K")
        sys.stdout.flush()
        self.width = 0

    def render_loop(self, stop_event):
        if not self.enabled:
            return
        while not stop_event.wait(self.interval):
            if self.tty:
                with _print_lock:
                    self.frame += 1
                    self.draw()
            else:
                # Not a terminal: emit a normal line now and then instead.
                now = time.time()
                if now - self._last_plain >= 30:
                    self._last_plain = now
                    with _print_lock:
                        print(self._line(), flush=True)

    def finish(self):
        with _print_lock:
            self.erase()


# ------------------------------------------------------------------ ledger


# fetch_log mirrors RUN_HEADER column-for-column, so a day's _summary.csv is a
# plain SELECT rather than anything held in memory. Negative outcomes
# (no_data / dead / srv_500) store date, train_no, status, fetched_at and NULLs.
LEDGER_COLS = list(RUN_HEADER)
_INSERT = ("INSERT OR REPLACE INTO fetch_log VALUES (" +
           ",".join("?" * len(RUN_HEADER)) + ")")


def _quarantine_corrupt_db():
    """Move a corrupted ledger aside so a fresh one can be built.

    This is survivable precisely because the ledger is only an index: the CSV
    files are the real data, and the startup scan re-derives 'already fetched'
    from them. Nothing is deleted -- the damaged file is kept in case it is ever
    worth salvaging with `sqlite3 old.sqlite .recover`."""
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    moved = []
    for suffix in ("", "-wal", "-shm"):
        p = STATE + suffix
        if os.path.exists(p):
            dst = "%s.corrupt-%s%s" % (STATE, stamp, suffix)
            try:
                os.replace(p, dst)
                moved.append(os.path.basename(dst))
            except OSError as exc:
                raise SystemExit(
                    "the ledger at %s is corrupt and could not be moved aside "
                    "(%s). Close anything using it, then delete %s* by hand and "
                    "rerun with --reindex." % (p, exc, os.path.basename(STATE)))
    log("!! state.sqlite was corrupt; moved aside as %s" % ", ".join(moved))
    log("!! rebuilding the index from the CSV files on disk -- no data is lost")


def open_state(_retry=True):
    os.makedirs(OUT, exist_ok=True)
    try:
        return _open_state_inner()
    except sqlite3.DatabaseError as exc:
        # "database disk image is malformed" and friends. A killed process or a
        # runaway WAL can leave the file unreadable; the fix is to rebuild it,
        # not to refetch 9.5M files.
        if not _retry:
            raise
        log("ledger unreadable: %s" % exc)
        _quarantine_corrupt_db()
        return _open_state_inner()


def _open_state_inner():
    con = sqlite3.connect(STATE, check_same_thread=False, timeout=60)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA busy_timeout=60000")
    # Without a size limit the -wal file only ever grows to its high-water
    # mark and never shrinks; a long run can leave tens of GB sitting next to
    # a 40 MB database. This caps it and lets checkpoints truncate it back.
    con.execute("PRAGMA journal_size_limit=67108864")   # 64 MB
    cols = [r[1] for r in con.execute("PRAGMA table_info(fetch_log)")]
    if cols:
        # quick_check reads the pages, not just the schema. Without it a
        # damaged interior page slips through here and blows up later, in the
        # middle of whatever we were doing -- including --reindex, which is the
        # very command meant to recover from this.
        verdict = con.execute("PRAGMA quick_check(1)").fetchone()[0]
        if verdict != "ok":
            raise sqlite3.DatabaseError("quick_check: %s" % verdict)
    if cols and cols != LEDGER_COLS:
        log("migrating state.sqlite to the wider ledger schema...")
        con.execute("ALTER TABLE fetch_log RENAME TO fetch_log_old")
        _create_ledger(con)
        con.execute(
            "INSERT OR REPLACE INTO fetch_log (date, train_no, status, stops, "
            "reported_stops, max_delay_min, fetched_at) "
            "SELECT date, train_no, status, stops, observed, max_delay, fetched_at "
            "FROM fetch_log_old")
        con.execute("DROP TABLE fetch_log_old")
        con.commit()
        log("  migrated %d rows" % con.execute(
            "SELECT COUNT(*) FROM fetch_log").fetchone()[0])
    elif not cols:
        _create_ledger(con)
        con.commit()
    return con


def _create_ledger(con):
    con.execute(
        """CREATE TABLE IF NOT EXISTS fetch_log (
               date               TEXT NOT NULL,
               train_no           TEXT NOT NULL,
               train_name         TEXT,
               train_type         TEXT,
               source_station     TEXT,
               destination_station TEXT,
               distance_km        REAL,
               stops              INTEGER,
               reported_stops     INTEGER,
               complete           INTEGER,
               max_delay_min      REAL,
               final_delay_min    REAL,
               status             TEXT NOT NULL,
               fetched_at         TEXT,
               PRIMARY KEY (date, train_no)
           )"""
    )
    con.execute("CREATE INDEX IF NOT EXISTS fetch_log_train ON fetch_log(train_no)")
    con.execute("CREATE INDEX IF NOT EXISTS fetch_log_status ON fetch_log(status)")
    con.execute("CREATE INDEX IF NOT EXISTS fetch_log_date ON fetch_log(date)")


def load_done(con, refetch_partial, retry_errors=False):
    """Return {(date, train)} for everything we should not fetch again."""
    skip = {ERROR}
    if retry_errors:
        skip.add(SRV_ERR)
    if refetch_partial:
        skip.add(PARTIAL)
    try:
        return {
            (d, t)
            for d, t, s in con.execute("SELECT date, train_no, status FROM fetch_log")
            if s not in skip
        }
    except sqlite3.DatabaseError as exc:
        # Corruption can sit in pages the schema check never reads. Losing the
        # index is not losing the work: the startup scan of days/ re-derives
        # what has already been fetched from the files themselves.
        log("!! could not read the ledger (%s); falling back to the CSV files" % exc)
        return set()


def scan_existing_files():
    """Every (date, train) that already has a CSV on disk.

    The ledger is only an index; the CSV files are the actual work. If the two
    ever disagree -- ledger lost, corrupted, copied from another machine, or
    never flushed because the run was killed -- the files win. Rediscovering
    them costs one directory walk; re-fetching them costs hours."""
    found = set()
    if not os.path.isdir(DAYS):
        return found
    with os.scandir(DAYS) as days:
        for d in days:
            if not d.is_dir():
                continue
            iso = d.name
            try:
                with os.scandir(d.path) as files:
                    for f in files:
                        n = f.name
                        if n.endswith(".csv") and n != SUMMARY_NAME:
                            found.add((iso, n[:-4]))
            except OSError:
                continue
    return found


def reindex_from_files(con):
    """Rebuild ledger rows for CSVs the ledger does not know about."""
    on_disk = scan_existing_files()
    known = {(d, t) for d, t in con.execute("SELECT date, train_no FROM fetch_log")}
    missing = sorted(on_disk - known)
    log("csv files on disk: %d   already in ledger: %d   to reindex: %d"
        % (len(on_disk), len(known), len(missing)))
    added = 0
    batch = []
    for iso, train_no in missing:
        row = summarise_train_file(iso, train_no)
        if row is None:
            continue
        batch.append(row)
        if len(batch) >= 500:
            con.executemany(_INSERT, batch)
            con.commit()
            added += len(batch)
            batch.clear()
    if batch:
        con.executemany(_INSERT, batch)
        con.commit()
        added += len(batch)
    log("reindexed %d train-days from disk" % added)
    return added


def summarise_train_file(iso, train_no):
    """Derive one ledger/summary row by reading a stored train CSV."""
    path = os.path.join(DAYS, iso, "%s.csv" % train_no)
    try:
        with open(path, newline="", encoding="utf-8") as fh:
            stops = list(csv.DictReader(fh))
    except OSError:
        return None
    if not stops:
        return None
    observed = [s for s in stops if s.get("reported") == "1"]
    delays = [float(v) for s in observed
              for v in (s.get("delay_arrival_min"), s.get("delay_departure_min")) if v]
    final = ""
    for s in reversed(observed):
        final = s.get("delay_arrival_min") or s.get("delay_departure_min")
        if final:
            break
    first = stops[0]
    return (
        iso, train_no, first.get("train_name") or "", first.get("train_type") or "",
        stops[0].get("station_code") or "", stops[-1].get("station_code") or "",
        float(stops[-1]["distance_km"]) if stops[-1].get("distance_km") else None,
        len(stops), len(observed), int(len(observed) == len(stops)),
        max(delays) if delays else None,
        float(final) if final else None,
        OK, first.get("reported_at") or "",
    )


def dead_trains(con):
    try:
        return {
            t for (t,) in con.execute(
                "SELECT DISTINCT train_no FROM fetch_log WHERE status = ?", (DEAD,)
            )
        }
    except sqlite3.DatabaseError as exc:
        log("!! could not read the dead-train list (%s); all trains will be tried" % exc)
        return set()


# ------------------------------------------------------------------ trains


def load_trains(args):
    if args.train:
        return [(t, 0x7F) for t in args.train]
    if args.trains_file:
        with open(args.trains_file) as fh:
            return [(ln.strip(), 0x7F) for ln in fh if ln.strip()]
    if not os.path.exists(TRAIN_CSV):
        raise SystemExit(
            "no train list: %s missing. Pass --train/--trains-file, or run "
            "apply_patch.py first." % TRAIN_CSV
        )
    out = []
    with open(TRAIN_CSV, newline="") as fh:
        for row in csv.DictReader(fh):
            try:
                mask = int(row["running_days_mask"])
            except (KeyError, ValueError):
                mask = 0x7F
            out.append((row["train_no"], mask))
    return out


def runs_on(mask, day):
    """decode_jingli bit convention: bit 0 == Monday, and so is weekday()."""
    return bool(mask >> day.weekday() & 1)


# ------------------------------------------------------------------ fetch


class ServerError(Exception):
    """A 5xx the API returns consistently for one (train, date).

    Verified by replaying failures one at a time, 0.4s apart: every single one
    still 500s. They are a property of the request, not of our concurrency, so
    retrying them is pure waste -- they are recorded once and skipped
    thereafter unless --retry-errors says otherwise."""


_local = threading.local()
_throttle = threading.Event()  # set == server asked us to ease off


def _conn(timeout):
    """One persistent HTTPS connection per worker thread.

    This is the single biggest speed lever. Opening a fresh TLS connection per
    request caps out around 11 req/s; reusing one per thread reaches ~98 req/s
    at 96 workers, measured against this API. Everything else here is noise by
    comparison."""
    c = getattr(_local, "conn", None)
    if c is None:
        c = http.client.HTTPSConnection(API_HOST, timeout=timeout,
                                        context=_local.__dict__.setdefault(
                                            "ctx", ssl.create_default_context()))
        _local.conn = c
    return c


def _drop_conn():
    c = getattr(_local, "conn", None)
    if c is not None:
        try:
            c.close()
        except Exception:
            pass
        _local.conn = None


def http_get(path, timeout, attempts, backoff):
    """GET one path over the thread's keep-alive connection.

    Honours 429/503 by parking every worker for the advertised interval --
    pushing harder when a server is already saying stop just turns into errors."""
    last = None
    for i in range(attempts):
        if _throttle.is_set():
            time.sleep(0.5)
        try:
            c = _conn(timeout)
            c.request("GET", path, headers={"User-Agent": UA,
                                            "Connection": "keep-alive",
                                            "Accept": "application/json"})
            r = c.getresponse()
            body = r.read()
            if r.status in (429, 503):
                wait = float(r.getheader("Retry-After") or backoff * (2**i) or 2)
                if not _throttle.is_set():
                    _throttle.set()
                    log("  server pushed back (HTTP %d); pausing %.0fs" % (r.status, wait))
                    time.sleep(min(wait, 60))
                    _throttle.clear()
                last = RuntimeError("HTTP %d" % r.status)
                continue
            if 500 <= r.status < 600:
                raise ServerError("HTTP %d" % r.status)
            if r.status != 200:
                raise RuntimeError("HTTP %d" % r.status)
            return json.loads(body.decode("utf-8", "replace"))
        except ServerError:
            _drop_conn()
            raise
        except Exception as exc:  # network, timeout, malformed JSON, stale socket
            last = exc
            _drop_conn()  # a reused socket may simply have gone stale
            if i < attempts - 1:
                time.sleep(backoff * (2**i) + random.uniform(0, 0.3))
    raise last


def num(v, nd=None):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return ""
    return round(f, nd) if nd is not None else f


def normalise(payload, train_no, day):
    """Flatten the API response into stop rows plus one summary row."""
    iso = day.isoformat()
    name = payload.get("train_name") or ""
    ttype = payload.get("train_type") or ""
    rows, observed, delays = [], 0, []

    for s in payload.get("days_schedule", []):
        seen = bool(s.get("updated_at"))
        if seen:
            observed += 1
        da, dd = num(s.get("delay_in_arrival"), 1), num(s.get("delay_in_departure"), 1)
        if seen:
            delays += [d for d in (da, dd) if d != ""]
        rows.append([
            iso, train_no, name, ttype, s.get("sno"), s.get("station_code") or "",
            int(bool(s.get("stops"))),
            s.get("sch_arrival_time") or "", s.get("sch_arrival_date") or "",
            s.get("sch_departure_time") or "", s.get("sch_departure_date") or "",
            s.get("actual_arrival_time") or "", s.get("actual_arrival_date") or "",
            s.get("actual_departure_time") or "", s.get("actual_departure_date") or "",
            da, dd, s.get("platform") or "", num(s.get("distance"), 3),
            num(s.get("lat"), 6), num(s.get("lng"), 6),
            int(bool(s.get("departed"))), int(seen), s.get("updated_at") or "",
        ])

    final = ""
    for r in reversed(rows):
        if r[STOP_HEADER.index("reported")]:
            final = (r[STOP_HEADER.index("delay_arrival_min")]
                     or r[STOP_HEADER.index("delay_departure_min")])
            break

    summary = [
        iso, train_no, name, ttype,
        payload.get("source_station") or "", payload.get("destination_station") or "",
        num(payload.get("distance"), 3), len(rows), observed,
        int(observed >= len(rows) and len(rows) > 0),
        max(delays) if delays else "", final, "",
        datetime.now().astimezone().isoformat(timespec="seconds"),
    ]
    return rows, summary, observed


# ------------------------------------------------------------------ storage


def day_dir(day):
    return os.path.join(DAYS, day.isoformat())


def train_path(day, train_no):
    return os.path.join(day_dir(day), "%s.csv" % train_no)


_known_dirs = set()
_dirs_lock = threading.Lock()


def write_train_csv(day, train_no, rows):
    """One file per train-day. Overwriting is the whole dedup story."""
    path = train_path(day, train_no)
    d = os.path.dirname(path)
    if d not in _known_dirs:            # makedirs is a syscall per write otherwise
        os.makedirs(d, exist_ok=True)
        with _dirs_lock:
            _known_dirs.add(d)
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(STOP_HEADER)
        w.writerows(rows)
    os.replace(tmp, path)


def fetch_one(day, train_no, args, today):
    """Return (status, stop_rows, summary_row, note).

    PARTIAL is reserved for a run that can still change -- i.e. today's, caught
    mid-journey. A past date is final even when some stops never reported: a
    handful of stations simply never file updates, so `complete` is routinely 0
    on perfectly good historical data and must not trigger a refetch."""
    path = API_PATH % (day.strftime("%d-%m-%Y"), train_no)
    try:
        payload = http_get(path, args.timeout, args.attempts, args.backoff)
    except ServerError as exc:
        return SRV_ERR, None, None, str(exc)
    except Exception as exc:
        detail = str(exc).strip()
        return ERROR, None, None, ("%s: %s" % (type(exc).__name__, detail))[:60] if detail else type(exc).__name__

    if isinstance(payload, dict) and payload.get("error"):
        msg = payload["error"].lower()
        if "no longer running" in msg:
            return DEAD, None, None, payload["error"]
        return NO_DATA, None, None, payload["error"]

    rows, summary, observed = normalise(payload, train_no, day)
    if not rows:
        return NO_DATA, None, None, "empty schedule"
    if observed == 0:
        return UNOBSERVED, None, summary, "schedule only, nothing reported"
    settled = day < today or summary[RUN_HEADER.index("complete")]
    return (OK if settled else PARTIAL), rows, summary, ""


# ------------------------------------------------------------------ driver


def daterange_back(start, until):
    d = start
    while d >= until:
        yield d
        d -= timedelta(days=1)


def run(args):
    global _progress
    con = open_state()
    done = set() if args.refetch else load_done(con, args.refetch_partial, args.retry_errors)
    if not args.refetch and not args.no_reconcile:
        on_disk = scan_existing_files()
        extra = on_disk - done
        done |= on_disk
        if extra:
            log("found %d train-day CSVs on disk the ledger did not know about "
                "(counting them as done; --reindex writes them into the ledger)"
                % len(extra))
    dead = dead_trains(con)

    trains = [(t, m) for t, m in load_trains(args) if t not in dead]
    today = date.today()
    start = min(args.start, today)
    log("trains: %d   dates: %s back to %s   already recorded: %d"
        % (len(trains), start, args.until, len(done)))
    if dead:
        log("skipping %d train(s) previously reported as no longer running" % len(dead))
    if start != args.start:
        log("note: --from clamped to today (%s); future dates carry no actuals" % today)

    work = queue.Queue()
    for item in trains:
        work.put(item)

    counts = defaultdict(int)
    err_kinds = defaultdict(int)
    _progress = Progress(not args.no_progress)
    _progress.set_context(trains_left=len(trains))
    budget = threading.Semaphore(args.max_requests) if args.max_requests else None
    stop = threading.Event()
    state_lock = threading.Lock()
    remaining = [len(trains)]
    dirty_seen = set()
    summaries = SummaryIndex()
    # Bounded on purpose: if the writer ever stalls, workers block briefly
    # instead of the queue swallowing memory until the machine dies.
    ledger_q = queue.Queue(maxsize=20000)

    def record(day, train_no, status, summary):
        iso = day.isoformat()
        # Workers never touch SQLite or the filesystem index directly: the row
        # goes on a queue and one writer thread owns the database. Committing
        # under a shared lock made every worker wait on fsync, which is what
        # made 500 workers slower than 250.
        if summary is not None:
            summary[RUN_HEADER.index("status")] = status
            row = tuple(v if v != "" else None for v in summary)
        else:
            row = (iso, train_no, None, None, None, None, None, None, None,
                   None, None, None, status,
                   datetime.now().astimezone().isoformat(timespec="seconds"))
        ledger_q.put(row)
        if summary is not None:
            summaries.mark(iso)
        with state_lock:
            counts[status] += 1
            if summary is not None:
                dirty_seen.add(iso)
        _progress.tick(status)
        if summary is not None:
            _progress.set_context(days=len(dirty_seen))

    ledger_failures = [0]

    def _commit(batch):
        """Never let a database hiccup kill this thread silently.

        The previous version called executemany/commit bare. One exception and
        the daemon thread vanished: nothing more was ever recorded, while the
        unbounded queue kept growing. A long run then ended with the CSVs on
        disk but an empty ledger -- every hour of it looking un-fetched."""
        for attempt in range(5):
            try:
                con.executemany(_INSERT, batch)
                con.commit()
                return True
            except Exception as exc:
                ledger_failures[0] += 1
                log("  ledger write failed (%s: %s); retry %d/5"
                    % (type(exc).__name__, exc, attempt + 1))
                time.sleep(1.0 * (attempt + 1))
        log("  LEDGER WRITE GAVE UP on %d rows -- the CSVs are still on disk and "
            "will be picked up by --reindex" % len(batch))
        return False

    def ledger_writer():
        batch = []
        last = time.time()
        last_ckpt = time.time()
        try:
            while True:
                try:
                    item = ledger_q.get(timeout=0.5)
                    if item is None:
                        break
                    batch.append(item)
                except queue.Empty:
                    pass
                now = time.time()
                if batch and (len(batch) >= 500 or now - last >= 2.0):
                    _commit(batch)
                    batch = []
                    last = now
                if now - last_ckpt >= 60.0:
                    last_ckpt = now
                    try:
                        con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    except Exception as exc:
                        log("  wal checkpoint skipped (%s)" % exc)
            if batch:
                _commit(batch)
        except Exception as exc:      # belt and braces: this thread must not die
            log("  ledger writer crashed (%s: %s) -- run --reindex afterwards"
                % (type(exc).__name__, exc))

    def worker():
        while not stop.is_set():
            try:
                train_no, mask = work.get_nowait()
            except queue.Empty:
                return
            misses = 0
            found_any = False
            for day in daterange_back(start, args.until):
                if stop.is_set():
                    return
                if any(lo <= day <= hi for lo, hi in args.skip_ranges):
                    continue
                if not args.all_days and not runs_on(mask, day):
                    continue
                if (day.isoformat(), train_no) in done:
                    continue
                if budget is not None and not budget.acquire(blocking=False):
                    stop.set()
                    log("request budget exhausted; stopping cleanly")
                    return

                _progress.set_current(day, train_no)
                status, rows, summary, note = fetch_one(day, train_no, args, today)
                if status in (ERROR, SRV_ERR):
                    with state_lock:
                        err_kinds[note] += 1
                if rows:
                    write_train_csv(day, train_no, rows)
                record(day, train_no, status, summary)

                if status == DEAD:
                    log("  %s: %s -- abandoning this train" % (train_no, note))
                    break
                if status in (NO_DATA, UNOBSERVED):
                    misses += 1
                    # Before this train has yielded anything, a short run of
                    # blanks means it probably does not exist -- bail cheaply.
                    # Once it HAS yielded data we know it is real, so tolerate a
                    # much longer silence: the 2020 pandemic shutdown stopped
                    # most passenger services for the better part of a year, and
                    # the old 30-day rule would abort the walk inside that gap
                    # and never reach the data on the far side.
                    limit = args.gap_tolerance if found_any else args.give_up_after
                    if limit and misses >= limit:
                        break
                elif status not in (ERROR, SRV_ERR):
                    misses = 0
                    found_any = True
                time.sleep(args.delay)
            with state_lock:
                remaining[0] -= 1
            _progress.set_context(trains_left=remaining[0])
            work.task_done()

    # The dataset is kept current as the fetch runs, so a plain
    # `python3 fetch_live_status.py` needs no follow-up command and an
    # interrupted run still leaves usable summaries behind.
    def flush_summaries(limit=None):
        return summaries.flush(limit)

    def summariser():
        # Runs the whole time the fetch runs, so the dataset on disk stays
        # current and a crash or kill never loses more than one tick.
        while not stop.wait(args.summary_every):
            flush_summaries(args.summary_batch)

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(args.workers)]
    ticker = threading.Thread(target=summariser, daemon=True)
    bar = threading.Thread(target=_progress.render_loop, args=(stop,), daemon=True)
    scribe = threading.Thread(target=ledger_writer, daemon=True)
    t0 = time.time()
    for t in threads:
        t.start()
    ticker.start()
    bar.start()
    scribe.start()
    try:
        while any(t.is_alive() for t in threads):
            time.sleep(0.5)
    except KeyboardInterrupt:
        stop.set()
        log("\ninterrupted -- progress is saved, re-run to resume")
    stop.set()
    # One shared deadline, not a timeout per thread. Joining 250 workers at
    # 5 s each, sequentially, could block for 20 minutes when they were all
    # sitting in a 30 s socket read -- which is what made Ctrl-C feel hung.
    # They are daemon threads and the ledger is already durable, so anything
    # still in flight can simply be abandoned.
    deadline = time.time() + args.shutdown_grace
    for t in threads + [ticker, bar]:
        left = deadline - time.time()
        if left <= 0:
            break
        t.join(timeout=left)
    stragglers = sum(1 for t in threads if t.is_alive())
    if stragglers:
        log("%d worker(s) still mid-request; abandoning them (their work is "
            "already on disk or will simply be refetched)" % stragglers)

    dt = time.time() - t0
    total = sum(counts.values())
    log("\n%d requests in %.1fs (%.1f/s)" % (total, dt, total / dt if dt else 0))
    for k in sorted(counts):
        log("  %-8s %d" % (k, counts[k]))
    if err_kinds:
        log("errors by cause (transient ones retry next run; srv_500 needs --retry-errors):")
        for k, v in sorted(err_kinds.items(), key=lambda kv: -kv[1]):
            log("  %-28s %d" % (k, v))

    ledger_q.put(None)
    scribe.join(timeout=30)

    n = flush_summaries(args.summary_batch * 4)
    if n:
        log("summaries refreshed for %d day(s)" % n)
    leftover = len(summaries.dirty)
    if leftover:
        log("%d day summaries not rewritten before exit -- they are derived data; "
            "`--build-dataset` regenerates them from the ledger" % leftover)
    if ledger_failures[0]:
        log("NOTE: %d ledger write failures during this run. The CSVs are safe; "
            "run --reindex to rebuild the ledger from them." % ledger_failures[0])
    summaries.close()
    try:
        con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except Exception:
        pass
    con.close()
    _progress.finish()


# ------------------------------------------------------------------ summaries


class SummaryIndex:
    """Tracks which days need their _summary.csv rewritten, and rewrites them
    straight out of SQLite.

    The previous version cached every row it had ever seen as
    {day: {train_no: row}} and never evicted. Measured at 86 bytes/row that is
    ~2.8 GB across a full 2,400-day x 13,678-train run -- which is exactly the
    unbounded growth seen over a 10-15 hour session. Workers walk train-major,
    so all 2,400 days are "active" at once and no LRU window helps.

    Now nothing is retained: the ledger already stores the full summary row, so
    a flush is one indexed SELECT per dirty day. Memory is O(dirty days)."""

    def __init__(self):
        self.dirty = set()
        self.lock = threading.Lock()
        self._con = None

    def mark(self, iso):
        with self.lock:
            self.dirty.add(iso)

    def _conn(self):
        if self._con is None:
            # Its own read connection: the writer thread owns the other one.
            self._con = sqlite3.connect(STATE, check_same_thread=False, timeout=60)
            self._con.execute("PRAGMA busy_timeout=60000")
            self._con.execute("PRAGMA journal_size_limit=67108864")
        return self._con

    def flush(self, limit=None):
        """Rewrite dirty days, at most `limit` per call.

        Capped because a busy minute can dirty every day in the range at once:
        workers walk train-major, so 250 of them touch 250 different dates
        concurrently. Rewriting all of them in one cycle turns a 20 s tick into
        a long stall. Whatever is left stays marked for the next tick."""
        with self.lock:
            pending = sorted(self.dirty)
            if limit is not None and len(pending) > limit:
                keep = pending[limit:]
                pending = pending[:limit]
                self.dirty = set(keep)
            else:
                self.dirty = set()
        if not pending:
            return 0
        con = self._conn()
        written = 0
        for iso in pending:
            try:
                rows = con.execute(
                    "SELECT " + ",".join(LEDGER_COLS) + " FROM fetch_log "
                    "WHERE date = ? AND stops IS NOT NULL ORDER BY train_no", (iso,)
                ).fetchall()
                if not rows:
                    continue
                d = os.path.join(DAYS, iso)
                os.makedirs(d, exist_ok=True)
                tmp = os.path.join(d, SUMMARY_NAME + ".tmp")
                with open(tmp, "w", newline="", encoding="utf-8") as fh:
                    w = csv.writer(fh)
                    w.writerow(RUN_HEADER)
                    for r in rows:
                        w.writerow(["" if v is None else v for v in r])
                os.replace(tmp, os.path.join(d, SUMMARY_NAME))
                written += 1
                # End the implicit read transaction. A reader holding an old
                # snapshot blocks WAL checkpointing, which is how the -wal file
                # runs away while the database itself stays small.
                con.rollback()
            except Exception as exc:
                with self.lock:
                    self.dirty.add(iso)
                log("  summary for %s deferred (%s: %s)" % (iso, type(exc).__name__, exc))
        return written

    def close(self):
        if self._con is not None:
            self._con.close()
            self._con = None


def write_day_summary(iso, con):
    """Rebuild one day's _summary.csv from the train CSVs actually on disk."""
    d = os.path.join(DAYS, iso)
    if not os.path.isdir(d):
        return 0
    ledger = {
        t: (s, f) for t, s, f in con.execute(
            "SELECT train_no, status, fetched_at FROM fetch_log WHERE date = ?", (iso,)
        )
    }
    rows = []
    for fn in sorted(os.listdir(d)):
        if not fn.endswith(".csv") or fn == SUMMARY_NAME:
            continue
        train_no = fn[:-4]
        with open(os.path.join(d, fn), newline="", encoding="utf-8") as fh:
            stops = list(csv.DictReader(fh))
        if not stops:
            continue
        observed = [s for s in stops if s["reported"] == "1"]
        delays = [
            float(v) for s in observed
            for v in (s["delay_arrival_min"], s["delay_departure_min"]) if v
        ]
        final = ""
        for s in reversed(observed):
            final = s["delay_arrival_min"] or s["delay_departure_min"]
            if final:
                break
        first = stops[0]
        status, fetched = ledger.get(train_no, ("", ""))
        rows.append([
            iso, train_no, first["train_name"], first["train_type"],
            stops[0]["station_code"], stops[-1]["station_code"],
            stops[-1]["distance_km"], len(stops), len(observed),
            int(len(observed) == len(stops)),
            max(delays) if delays else "", final, status, fetched,
        ])
    path = os.path.join(d, SUMMARY_NAME)
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(RUN_HEADER)
        w.writerows(rows)
    os.replace(tmp, path)
    return len(rows)


def build_dataset(flat):
    if not os.path.isdir(DAYS):
        raise SystemExit("nothing fetched yet: %s missing" % DAYS)
    con = open_state()
    reindex_from_files(con)
    days = sorted(d for d in os.listdir(DAYS) if os.path.isdir(os.path.join(DAYS, d)))
    idx = SummaryIndex()
    for iso in days:
        idx.mark(iso)
    total = idx.flush()
    idx.close()
    con.close()
    log("summaries: %d day folder(s), %d rewritten" % (len(days), total))

    if not flat:
        log("(pass --flat as well to also write the single-file dataset/ copies)")
        return

    os.makedirs(DATASET, exist_ok=True)
    stops_out = os.path.join(DATASET, "stop_delays.csv")
    runs_out = os.path.join(DATASET, "train_runs.csv")
    n_stop = n_run = 0
    with open(stops_out, "w", newline="", encoding="utf-8") as fs, \
         open(runs_out, "w", newline="", encoding="utf-8") as fr:
        ws, wr = csv.writer(fs), csv.writer(fr)
        ws.writerow(STOP_HEADER)
        wr.writerow(RUN_HEADER)
        for iso in days:
            d = os.path.join(DAYS, iso)
            for fn in sorted(os.listdir(d)):
                if not fn.endswith(".csv"):
                    continue
                with open(os.path.join(d, fn), newline="", encoding="utf-8") as fh:
                    r = csv.reader(fh)
                    head = next(r, None)
                    if fn == SUMMARY_NAME:
                        if head != RUN_HEADER:
                            continue
                        for row in r:
                            wr.writerow(row)
                            n_run += 1
                    else:
                        if head != STOP_HEADER:
                            continue
                        for row in r:
                            ws.writerow(row)
                            n_stop += 1
    log("flat files: %d stop rows -> %s" % (n_stop, stops_out))
    log("            %d run rows  -> %s" % (n_run, runs_out))


# ------------------------------------------------------------------ cli


def parse_day(s):
    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    raise argparse.ArgumentTypeError("use YYYY-MM-DD or DD-MM-YYYY: %s" % s)


def main():
    ap = argparse.ArgumentParser(
        description="Harvest historical train running/delay data into one folder per day, one CSV per train.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="examples:\n"
               "  python3 fetch_live_status.py --train 12624 --until 2026-08-01\n"
               "  python3 fetch_live_status.py --from 2026-08-18 --until 2026-06-01\n"
               "  python3 fetch_live_status.py --build-dataset --flat\n",
    )
    ap.add_argument("--from", dest="start", type=parse_day, default=date.today(),
                    metavar="DATE", help="start date, walked backwards (default: today)")
    ap.add_argument("--until", type=parse_day, default=date(2020, 1, 1), metavar="DATE",
                    help="stop at this date, inclusive (default: 2020-01-01, "
                         "about as far back as the API holds data)")
    ap.add_argument("--train", action="append", metavar="NO",
                    help="fetch only this train; repeatable")
    ap.add_argument("--trains-file", metavar="PATH",
                    help="file of train numbers, one per line")
    ap.add_argument("--skip-range", action="append", default=[], metavar="A..B",
                    help="skip this date window entirely; repeatable. Use it for "
                         "the 2020-2021 shutdown, e.g. "
                         "--skip-range 2020-03-22..2021-11-30")
    ap.add_argument("--all-days", action="store_true",
                    help="query every date, ignoring each train's timetable")
    ap.add_argument("--give-up-after", type=int, default=30, metavar="N",
                    help="abandon a train after N blank days IF it has produced "
                         "nothing at all yet (default: 30, 0 = never)")
    ap.add_argument("--gap-tolerance", type=int, default=700, metavar="N",
                    help="once a train HAS produced data, how many blank days to "
                         "cross before giving up (default: 700, enough to span "
                         "the ~650-day 2020-21 shutdown; 0 = never give up)")
    ap.add_argument("--workers", type=int, default=64,
                    help="parallel fetchers, each holding one keep-alive "
                         "connection (default: 64; ~250 is the measured peak, "
                         "beyond which throughput falls)")
    ap.add_argument("--delay", type=float, default=0.0, metavar="SEC",
                    help="extra pause per worker between requests (default: 0; "
                         "concurrency is the throttle, not this)")
    ap.add_argument("--timeout", type=float, default=30.0)
    ap.add_argument("--attempts", type=int, default=3, help="tries per request")
    ap.add_argument("--backoff", type=float, default=1.0, help="retry backoff base seconds")
    ap.add_argument("--no-progress", action="store_true",
                    help="disable the live status line (it already degrades to "
                         "an occasional plain line when not on a terminal)")
    ap.add_argument("--shutdown-grace", type=float, default=8.0, metavar="SEC",
                    help="total time to wait for in-flight workers on Ctrl-C "
                         "(default: 8; they are abandoned after that)")
    ap.add_argument("--summary-batch", type=int, default=250, metavar="N",
                    help="max day summaries rewritten per tick (default: 250)")
    ap.add_argument("--summary-every", type=float, default=20.0, metavar="SEC",
                    help="rebuild touched days' _summary.csv this often while fetching (default: 20)")
    ap.add_argument("--max-requests", type=int, default=0, metavar="N",
                    help="stop cleanly after N requests (0 = no cap)")
    ap.add_argument("--reindex", action="store_true",
                    help="rebuild the ledger from the CSV files already on disk, "
                         "then exit. Use after a run whose ledger was lost or "
                         "never flushed -- it recovers the work without refetching")
    ap.add_argument("--no-reconcile", action="store_true",
                    help="skip the startup scan that treats existing CSV files as "
                         "already fetched (the scan is what makes files, not the "
                         "database, the source of truth)")
    ap.add_argument("--retry-errors", action="store_true",
                    help="re-attempt (train, date) pairs the API answered with a "
                         "persistent 500; they are skipped by default")
    ap.add_argument("--refetch", action="store_true", help="ignore the ledger entirely")
    ap.add_argument("--refetch-partial", action="store_true",
                    help="re-pull days captured while the run was still in progress")
    ap.add_argument("--build-dataset", action="store_true",
                    help="rebuild every day's _summary.csv from the train CSVs on disk")
    ap.add_argument("--flat", action="store_true",
                    help="with --build-dataset, also write dataset/ single-file copies")
    args = ap.parse_args()

    ranges = []
    for spec in args.skip_range:
        try:
            a, b = spec.split("..")
        except ValueError:
            raise SystemExit("--skip-range wants A..B, got %r" % spec)
        lo, hi = parse_day(a), parse_day(b)
        ranges.append((min(lo, hi), max(lo, hi)))
    args.skip_ranges = ranges

    if args.reindex:
        con = open_state()
        reindex_from_files(con)
        idx = SummaryIndex()
        for iso in sorted(d for d in os.listdir(DAYS)
                          if os.path.isdir(os.path.join(DAYS, d))) if os.path.isdir(DAYS) else []:
            idx.mark(iso)
        log("rewrote %d day summaries" % idx.flush())
        idx.close()
        con.close()
        return

    if args.build_dataset:
        build_dataset(args.flat)
        return
    if args.workers > 300:
        log("note: measured throughput peaks near 250 workers and falls off "
            "above it -- the API, not this script, is the limit")
    if args.skip_ranges:
        for lo, hi in args.skip_ranges:
            log("skipping %s .. %s entirely" % (lo, hi))
    if args.until > args.start:
        raise SystemExit("--until (%s) is after --from (%s); this walks backwards"
                         % (args.until, args.start))
    run(args)


if __name__ == "__main__":
    main()
