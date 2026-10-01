#!/usr/bin/env python3
"""
sync_livestatus.py -- pull the livestatus/days tree from another machine that is
serving it over `python3 -m http.server`.

    python3 sync_livestatus.py http://10.5.87.57:8000/train_data/livestatus/days/
    python3 sync_livestatus.py <url> --from 2024-01-01 --until 2024-12-31
    python3 sync_livestatus.py <url> --dry-run

Resumable and idempotent: a file already present locally is skipped, so you can
stop and restart at will. Downloads land on a .part file and are renamed only
once complete, so an interrupted transfer never leaves a truncated CSV that a
later run would mistake for finished.

SPEED, MEASURED against the real server (which was busy fetching at the time):

    24 workers     51 files/s
    48 workers    331 files/s     <- default
    96 workers     44 files/s     <- worse; the source machine is saturated

Rates are noisy for exactly that reason, so treat 48 as a starting point. At
~330 files/s a full 4.4M-file tree still takes about 3.7 hours, because every
file is a separate HTTP request and `python -m http.server` speaks HTTP/1.0 --
no keep-alive, so each one costs a fresh TCP connection.

IF YOU CAN RUN ONE COMMAND ON THE OTHER MACHINE, DO THAT INSTEAD. One tar
stream is one connection and one sequential read, so it moves the same data at
link speed rather than paying 4.4 million round trips:

    # on the source machine
    cd /path/to/train_data/livestatus && tar czf - days | nc -l 9000
    # here
    nc 10.5.87.57 9000 | tar xzf -

    # progress + resume, one connection (see pull_tar.py):
    #   host:  cd .../livestatus && tar czf days.tar.gz days
    #   here:  python3 pull_tar.py http://10.5.87.57:8000/train_data/livestatus/days.tar.gz
    #
    # or, if ssh works at all, just
    rsync -a --info=progress2 user@10.5.87.57:/path/train_data/livestatus/days/ \\
        ./livestatus/days/

Afterwards, rebuild the index from the files you just copied:

    python3 fetch_live_status.py --reindex
"""

import argparse
import http.client
import os
import queue
import re
import shutil
import sys
import threading
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime

LINK = re.compile(r'<a href="([^"?][^"]*)"', re.I)
DAYDIR = re.compile(r"^\d{4}-\d{2}-\d{2}/$")

_print_lock = threading.Lock()


_TTY = sys.stdout.isatty()


def log(msg):
    with _print_lock:
        # Only emit the erase sequence on a real terminal, otherwise piping this
        # to a file fills it with literal [K.
        sys.stdout.write(("\r\033[K" if _TTY else "") + msg + "\n")
        sys.stdout.flush()


def listing(url, timeout, attempts=3):
    """Return the hrefs in one autoindex page."""
    last = None
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "livestatus-sync"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                html = r.read().decode("utf-8", "replace")
            return [urllib.parse.unquote(h) for h in LINK.findall(html)]
        except Exception as exc:
            last = exc
            time.sleep(1.5 * (i + 1))
    raise last


def fetch_file(url, dest, timeout, attempts=3):
    """Download one file atomically. Returns bytes written, or -1 on failure."""
    tmp = dest + ".part"
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "livestatus-sync"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                expected = r.getheader("Content-Length")
                body = r.read()
            if expected is not None and len(body) != int(expected):
                raise IOError("short read: %d of %s bytes" % (len(body), expected))
            with open(tmp, "wb") as fh:
                fh.write(body)
            os.replace(tmp, dest)          # only now does it count as present
            return len(body)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            if i < attempts - 1:
                time.sleep(1.0 * (i + 1))
    return -1


class Bar:
    SPIN = "|/-\\"

    def __init__(self, enabled):
        self.tty = enabled and sys.stdout.isatty()
        self.lock = threading.Lock()
        self.done = self.skipped = self.failed = self.bytes = 0
        self.day = ""
        self.days_done = 0
        self.days_total = 0
        self.started = time.time()
        self.frame = 0

    def tick(self, got=0, skipped=False, failed=False, nbytes=0):
        with self.lock:
            if failed:
                self.failed += 1
            elif skipped:
                self.skipped += 1
            else:
                self.done += 1
                self.bytes += nbytes

    def line(self):
        with self.lock:
            d, s, f, b = self.done, self.skipped, self.failed, self.bytes
            day, dd, dt = self.day, self.days_done, self.days_total
        el = max(time.time() - self.started, 1e-9)
        m, sec = divmod(int(el), 60)
        h, m = divmod(m, 60)
        clock = "%d:%02d:%02d" % (h, m, sec) if h else "%d:%02d" % (m, sec)
        bits = ["%s %s" % (self.SPIN[self.frame % 4], clock),
                "day %d/%d" % (dd, dt),
                "%d got %.1f/s" % (d, d / el),
                "%.1f MB" % (b / 1e6)]
        if s:
            bits.append("%d skipped" % s)
        if f:
            bits.append("%d failed" % f)
        if day:
            bits.append(day)
        return "  ".join(bits)

    def draw(self):
        if not self.tty:
            return
        line = self.line()
        cols = shutil.get_terminal_size((100, 24)).columns
        if len(line) > cols - 1:
            line = line[:cols - 2] + "…"
        with _print_lock:
            sys.stdout.write("\r" + line + "\033[K")
            sys.stdout.flush()

    def loop(self, stop):
        last_plain = time.time()
        while not stop.wait(0.25):
            if self.tty:
                self.frame += 1
                self.draw()
            elif time.time() - last_plain >= 30:
                last_plain = time.time()
                log(self.line())


def main():
    ap = argparse.ArgumentParser(
        description="Mirror a livestatus/days tree served over http.server.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("WHEN NOT TO USE")[0].strip().split("\n\n", 1)[1])
    ap.add_argument("url", help="e.g. http://10.5.87.57:8000/train_data/livestatus/days/")
    ap.add_argument("--dest", default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                   "livestatus", "days"),
                    help="local days/ directory (default: beside this script)")
    ap.add_argument("--from", dest="start", metavar="DATE", help="earliest day folder to pull")
    ap.add_argument("--until", metavar="DATE", help="latest day folder to pull")
    ap.add_argument("--workers", type=int, default=48,
                    help="parallel downloads (default: 48, the measured peak; "
                         "96 was 7x SLOWER because the source machine is also "
                         "busy running the fetcher)")
    ap.add_argument("--timeout", type=float, default=60.0)
    ap.add_argument("--dry-run", action="store_true", help="list what would be pulled")
    ap.add_argument("--no-progress", action="store_true")
    args = ap.parse_args()

    base = args.url if args.url.endswith("/") else args.url + "/"
    os.makedirs(args.dest, exist_ok=True)

    log("listing %s" % base)
    days = sorted(d for d in listing(base, args.timeout) if DAYDIR.match(d))
    if args.start:
        days = [d for d in days if d.strip("/") >= args.start]
    if args.until:
        days = [d for d in days if d.strip("/") <= args.until]
    log("%d day folders to consider (%s .. %s)"
        % (len(days), days[0].strip("/") if days else "-", days[-1].strip("/") if days else "-"))
    if not days:
        return

    bar = Bar(not args.no_progress)
    bar.days_total = len(days)
    stop = threading.Event()
    ticker = threading.Thread(target=bar.loop, args=(stop,), daemon=True)
    ticker.start()

    work = queue.Queue()
    for d in days:
        work.put(d)

    totals = defaultdict(int)
    tot_lock = threading.Lock()

    # Two stages. Listers walk the day folders; downloaders pull individual
    # files. The first version parallelised only across days, so a single day
    # folder was fetched by one thread, one file at a time -- 18 files/s against
    # the ~130 the server can actually serve. Files are the unit of work.
    fileq = queue.Queue(maxsize=5000)
    listing_done = threading.Event()

    def lister():
        while True:
            try:
                day = work.get_nowait()
            except queue.Empty:
                return
            iso = day.strip("/")
            local_day = os.path.join(args.dest, iso)
            try:
                names = [n for n in listing(base + day, args.timeout) if n.endswith(".csv")]
            except Exception as exc:
                log("  %s: listing failed (%s)" % (iso, exc))
                with tot_lock:
                    totals["listing_failed"] += 1
                with tot_lock:
                    bar.days_done += 1
                continue
            os.makedirs(local_day, exist_ok=True)
            try:
                existing = set(os.listdir(local_day))
            except OSError:
                existing = set()
            bar.day = iso
            for n in names:
                dest = os.path.join(local_day, n)
                if n in existing and os.path.getsize(dest) > 0:
                    bar.tick(skipped=True)
                    continue
                if args.dry_run:
                    with tot_lock:
                        totals["would_fetch"] += 1
                    continue
                fileq.put((base + day + urllib.parse.quote(n), dest))
            with tot_lock:
                bar.days_done += 1

    def downloader():
        while True:
            try:
                url, dest = fileq.get(timeout=0.5)
            except queue.Empty:
                if listing_done.is_set():
                    return
                continue
            got = fetch_file(url, dest, args.timeout)
            if got < 0:
                bar.tick(failed=True)
            else:
                bar.tick(nbytes=got)

    listers = [threading.Thread(target=lister, daemon=True)
               for _ in range(max(1, min(6, args.workers // 4)))]
    downloaders = [threading.Thread(target=downloader, daemon=True)
                   for _ in range(args.workers)]
    for t in listers:
        t.start()
    for t in downloaders:
        t.start()
    try:
        for t in listers:
            t.join()
        listing_done.set()
        for t in downloaders:
            t.join()
    except KeyboardInterrupt:
        log("interrupted -- rerun to resume, nothing partial was kept")
        listing_done.set()

    stop.set()
    ticker.join(timeout=2)
    if bar.tty:
        with _print_lock:
            sys.stdout.write("\r\033[K")

    el = time.time() - bar.started
    log("")
    log("downloaded : %d files, %.1f MB in %.0fs (%.1f files/s)"
        % (bar.done, bar.bytes / 1e6, el, bar.done / max(el, 1e-9)))
    log("skipped    : %d already present" % bar.skipped)
    if bar.failed:
        log("failed     : %d (rerun to retry them)" % bar.failed)
    if totals.get("listing_failed"):
        log("day listings that failed: %d" % totals["listing_failed"])
    if args.dry_run:
        log("dry run -- %d files would be fetched" % totals["would_fetch"])
    else:
        log("now run:  python3 fetch_live_status.py --reindex")


if __name__ == "__main__":
    main()
