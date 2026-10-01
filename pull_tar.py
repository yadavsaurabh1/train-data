#!/usr/bin/env python3
"""
pull_tar.py -- pull the dataset as one tar stream, with a progress line.

This is the fast path. sync_livestatus.py fetches ~4.4 million files over
HTTP/1.0 at roughly 330 files/s (about 3.7 hours); one tar stream is a single
connection and a single sequential read, so it moves at link speed instead of
paying 4.4 million round trips.

ON THE HOST, once:

    cd /path/to/train_data/livestatus
    tar czf days.tar.gz days          # gzip: smaller over the wire, some CPU
    # or, on a fast LAN where CPU is the limit rather than bandwidth:
    tar cf  days.tar    days          # no compression, usually faster on 1GbE

HERE:

    python3 pull_tar.py http://10.5.87.57:8000/train_data/livestatus/days.tar.gz
    python3 fetch_live_status.py --reindex

Progress is free: the bytes are already passing through this process, so
counting them costs nothing measurable. It comes from the response's
Content-Length, so the percentage and ETA are exact rather than guessed. One
self-rewriting line, nothing else -- no per-file listing.

If the transfer breaks, rerun with --resume: it sends a Range request for the
remaining bytes and appends, rather than starting over. That requires keeping
the partial file, so --resume implies --keep (below).

By default the archive is streamed straight into tar and never stored, so you
need room for the extracted tree but not for the tarball. --keep writes the
archive to disk first and extracts afterwards, which is what --resume needs.
"""

import argparse
import os
import shutil
import subprocess
import sys
import time
import urllib.request

CHUNK = 1 << 20  # 1 MiB reads; big enough that syscall overhead disappears


def human(n):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return "%.1f %s" % (n, unit)
        n /= 1024.0
    return "%.1f PB" % n


def clock(sec):
    if sec is None or sec != sec or sec in (float("inf"),):
        return "--:--"
    sec = int(sec)
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    return "%d:%02d:%02d" % (h, m, s) if h else "%d:%02d" % (m, s)


class Progress:
    """One rewriting line: percent, transferred, rate, ETA."""

    def __init__(self, total, enabled):
        self.total = total
        self.tty = enabled and sys.stdout.isatty()
        self.done = 0
        self.started = time.time()
        self.last_draw = 0.0

    def add(self, n):
        self.done += n
        now = time.time()
        if now - self.last_draw >= 0.25:      # redraw 4x/s, not per chunk
            self.last_draw = now
            self.draw()

    def draw(self):
        el = max(time.time() - self.started, 1e-9)
        rate = self.done / el
        if self.total:
            pct = 100.0 * self.done / self.total
            eta = (self.total - self.done) / rate if rate > 0 else None
            line = "  %5.1f%%  %s / %s  %s/s  eta %s" % (
                pct, human(self.done), human(self.total), human(rate), clock(eta))
        else:
            line = "  %s  %s/s  %s elapsed" % (
                human(self.done), human(rate), clock(el))
        if self.tty:
            cols = shutil.get_terminal_size((100, 24)).columns
            sys.stdout.write("\r" + line[:cols - 1] + "\033[K")
            sys.stdout.flush()

    def finish(self):
        el = max(time.time() - self.started, 1e-9)
        if self.tty:
            sys.stdout.write("\r\033[K")
            sys.stdout.flush()
        print("  transferred %s in %s (%s/s)"
              % (human(self.done), clock(el), human(self.done / el)))


def tar_flags(url, explicit):
    if explicit:
        return explicit
    low = url.lower()
    if low.endswith((".tar.gz", ".tgz")):
        return "xzf"
    if low.endswith((".tar.bz2", ".tbz")):
        return "xjf"
    if low.endswith((".tar.xz", ".txz")):
        return "xJf"
    return "xf"


def main():
    ap = argparse.ArgumentParser(
        description="Download a tar archive and extract it, with a single-line progress display.")
    ap.add_argument("url", help="URL of the .tar / .tar.gz on the other machine")
    ap.add_argument("--dest", default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "livestatus"),
        help="directory to extract into (default: ./livestatus beside this script)")
    ap.add_argument("--keep", metavar="PATH", nargs="?", const="",
                    help="save the archive to disk instead of streaming it; "
                         "extract afterwards")
    ap.add_argument("--resume", action="store_true",
                    help="continue a partial --keep download using an HTTP Range request")
    ap.add_argument("--tar-flags", help="override tar flags (default inferred from the URL)")
    ap.add_argument("--no-extract", action="store_true", help="download only")
    ap.add_argument("--no-progress", action="store_true")
    ap.add_argument("--timeout", type=float, default=60.0)
    args = ap.parse_args()

    os.makedirs(args.dest, exist_ok=True)
    flags = tar_flags(args.url, args.tar_flags)

    keep_path = None
    if args.keep is not None or args.resume:
        keep_path = args.keep or os.path.join(args.dest, os.path.basename(args.url))

    offset = 0
    if args.resume and keep_path and os.path.exists(keep_path):
        offset = os.path.getsize(keep_path)
        print("  resuming at %s" % human(offset))

    headers = {"User-Agent": "livestatus-pull"}
    if offset:
        headers["Range"] = "bytes=%d-" % offset

    req = urllib.request.Request(args.url, headers=headers)
    with urllib.request.urlopen(req, timeout=args.timeout) as resp:
        clen = resp.getheader("Content-Length")
        total = int(clen) + offset if clen else 0
        if offset and resp.status != 206:
            print("  server ignored Range; restarting from 0")
            offset = 0
            total = int(clen) if clen else 0
        print("  %s" % args.url)
        print("  %s to fetch%s" % (human(total) if total else "unknown size",
                                   ", extracting into %s" % args.dest
                                   if not args.no_extract and not keep_path else ""))

        bar = Progress(total, not args.no_progress)
        bar.done = offset

        if keep_path:
            mode = "ab" if offset else "wb"
            with open(keep_path, mode) as fh:
                while True:
                    buf = resp.read(CHUNK)
                    if not buf:
                        break
                    fh.write(buf)
                    bar.add(len(buf))
            bar.finish()
            if not args.no_extract:
                print("  extracting...")
                subprocess.run(["tar", flags, keep_path, "-C", args.dest], check=True)
                print("  extracted into %s" % args.dest)
        else:
            # Stream straight into tar: no space needed for the archive itself.
            proc = subprocess.Popen(["tar", flags, "-", "-C", args.dest],
                                    stdin=subprocess.PIPE)
            try:
                while True:
                    buf = resp.read(CHUNK)
                    if not buf:
                        break
                    proc.stdin.write(buf)
                    bar.add(len(buf))
                proc.stdin.close()
                # tar extracts while the download runs, so it is still writing
                # files after the last byte arrives. Without this the bar sits
                # at 100% looking hung -- extraction is the slow half (measured
                # ~2,550 files/s), not the transfer.
                if bar.tty:
                    sys.stdout.write("\r\033[K")
                    sys.stdout.flush()
                print("  transfer complete; tar is still extracting...")
            except BrokenPipeError:
                raise SystemExit("tar exited early -- is the archive format right? "
                                 "(tried flags %r)" % flags)
            rc = proc.wait()
            bar.finish()
            if rc != 0:
                raise SystemExit("tar exited with status %d" % rc)
            print("  extracted into %s" % args.dest)

    print("  now run:  python3 fetch_live_status.py --reindex")


if __name__ == "__main__":
    main()
