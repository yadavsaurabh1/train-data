#!/usr/bin/env python3
"""
make_archive.py -- run this ON THE HOST to pack livestatus/days into one file.

    python3 make_archive.py                      # -> days.tar next to days/
    python3 make_archive.py --since 2024-01-01   # only newer day folders
    python3 make_archive.py --out /tmp/days.tar

No compression by default. Packing 4.4M small text files with gzip costs real
CPU, and on a machine that is already busy fetching, that is the bottleneck --
not the network. Plain tar just concatenates, so it runs at disk speed. Pass
--gzip only if the link is slow enough to be worth paying for it.

Progress comes from polling the size of the output file, so nothing passes
through Python and the archiving itself is exactly as fast as bare tar. The
percentage is exact, not estimated: the scan pass computes the true tar size
up front (512-byte header per entry plus content padded to a 512-byte
boundary), which for millions of ~3 KB files is a real 15-30% on top of the
raw bytes -- worth knowing before you check free disk space.

Then serve it with the http.server you already have running, and pull it from
the other machine:

    python3 pull_tar.py http://<this-host>:8000/train_data/livestatus/days.tar
"""

import argparse
import os
import shutil
import subprocess
import sys
import time

BLOCK = 512


def human(n):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return "%.1f %s" % (n, unit)
        n /= 1024.0
    return "%.1f PB" % n


def clock(sec):
    if sec is None or sec != sec or sec == float("inf"):
        return "--:--"
    sec = int(sec)
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    return "%d:%02d:%02d" % (h, m, s) if h else "%d:%02d" % (m, s)


def draw(line, tty):
    if not tty:
        return
    cols = shutil.get_terminal_size((100, 24)).columns
    sys.stdout.write("\r" + line[:cols - 1] + "\033[K")
    sys.stdout.flush()


def clear(tty):
    if tty:
        sys.stdout.write("\r\033[K")
        sys.stdout.flush()


def scan(root, days, tty):
    """Count files and compute the exact size tar will produce."""
    total = files = dirs = 0
    t0 = time.time()
    last = 0.0
    for day in days:
        dirs += 1
        total += BLOCK                      # directory entry
        d = os.path.join(root, day)
        try:
            with os.scandir(d) as it:
                for e in it:
                    if not e.is_file():
                        continue
                    try:
                        sz = e.stat().st_size
                    except OSError:
                        continue
                    files += 1
                    # tar: one header block, then content padded up to 512
                    total += BLOCK + ((sz + BLOCK - 1) // BLOCK) * BLOCK
        except OSError:
            continue
        now = time.time()
        if now - last >= 0.25:
            last = now
            draw("  scanning  %d folders  %s files  %s so far"
                 % (dirs, format(files, ","), human(total)), tty)
    total += BLOCK * 2                      # two zero blocks terminate the archive
    clear(tty)
    print("  %s files in %d day folders, archive will be %s (%.0fs to scan)"
          % (format(files, ","), dirs, human(total), time.time() - t0))
    return total, files


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(
        description="Pack livestatus/days into a single tar for transfer.")
    ap.add_argument("--days", default=os.path.join(here, "livestatus", "days"),
                    help="the days/ directory (default: ./livestatus/days)")
    ap.add_argument("--out", help="output path (default: days.tar beside days/)")
    ap.add_argument("--since", metavar="DATE", help="only day folders >= this date")
    ap.add_argument("--until", metavar="DATE", help="only day folders <= this date")
    ap.add_argument("--gzip", action="store_true",
                    help="compress (costs CPU; skip it on a weak host)")
    ap.add_argument("--no-scan", action="store_true",
                    help="skip the sizing pass; show bytes/rate but no percentage")
    ap.add_argument("--no-progress", action="store_true")
    args = ap.parse_args()

    days_dir = os.path.abspath(args.days)
    if not os.path.isdir(days_dir):
        raise SystemExit("not a directory: %s" % days_dir)
    parent = os.path.dirname(days_dir)
    name = os.path.basename(days_dir)

    out = args.out or os.path.join(parent, "days.tar" + (".gz" if args.gzip else ""))
    tty = (not args.no_progress) and sys.stdout.isatty()

    days = sorted(d for d in os.listdir(days_dir)
                  if os.path.isdir(os.path.join(days_dir, d)))
    if args.since:
        days = [d for d in days if d >= args.since]
    if args.until:
        days = [d for d in days if d <= args.until]
    if not days:
        raise SystemExit("no day folders matched")
    print("  packing %s (%d day folders, %s .. %s)" % (days_dir, len(days), days[0], days[-1]))

    total = 0
    if not args.no_scan:
        total, _ = scan(days_dir, days, tty)
        free = shutil.disk_usage(parent).free
        if total > free:
            raise SystemExit("  not enough space: need %s, %s free on %s"
                             % (human(total), human(free), parent))

    # Feed tar an explicit member list so --since/--until work, and so tar never
    # walks the tree a second time.
    listing = out + ".filelist"
    with open(listing, "w", encoding="utf-8") as fh:
        for d in days:
            fh.write("%s/%s\n" % (name, d))

    # Pin ustar. bsdtar defaults to pax, which writes an extra extended header
    # per member -- about 2 KB on top of every 3 KB file, so the archive came
    # out 41% larger than predicted and the progress bar would sail past 100%.
    # ustar has no per-file extras; our paths are far inside its 255-char limit.
    # Flags stay separate: bundling them as "cf" ahead of --format makes bsdtar
    # complain it was given no mode.
    cmd = ["tar", "-c", "--format=ustar"]
    if args.gzip:
        cmd.append("-z")
    cmd += ["-f", out, "-C", parent, "-T", listing]
    t0 = time.time()
    proc = subprocess.Popen(cmd)

    # Poll the output file's size. Nothing is piped through this process, so
    # archiving runs at exactly bare-tar speed.
    try:
        while proc.poll() is None:
            time.sleep(0.25)
            try:
                done = os.path.getsize(out)
            except OSError:
                done = 0
            el = max(time.time() - t0, 1e-9)
            rate = done / el
            if total:
                pct = min(100.0, 100.0 * done / total)
                eta = (total - done) / rate if rate > 0 else None
                draw("  %5.1f%%  %s / %s  %s/s  eta %s"
                     % (pct, human(done), human(total), human(rate), clock(eta)), tty)
            else:
                draw("  %s  %s/s  %s elapsed" % (human(done), human(rate), clock(el)), tty)
    except KeyboardInterrupt:
        proc.terminate()
        clear(tty)
        try:
            os.unlink(listing)
        except OSError:
            pass
        raise SystemExit("  interrupted -- %s is incomplete, delete it and rerun" % out)

    clear(tty)
    try:
        os.unlink(listing)
    except OSError:
        pass
    if proc.returncode != 0:
        raise SystemExit("  tar exited with status %d" % proc.returncode)

    size = os.path.getsize(out)
    el = time.time() - t0
    print("  wrote %s in %s (%s/s)" % (human(size), clock(el), human(size / max(el, 1e-9))))
    print("  %s" % out)
    print()
    print("  serve it and pull from the other machine:")
    print("    python3 pull_tar.py http://<this-host>:8000/.../%s" % os.path.basename(out))


if __name__ == "__main__":
    main()
