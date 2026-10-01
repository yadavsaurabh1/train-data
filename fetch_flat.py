#!/usr/bin/env python3
"""
fetch_flat.py -- mirror one flat http.server directory to a local folder.

    python3 fetch_flat.py http://10.5.87.57:8000/train_data/livestatus/trains/ \
        --dest /Volumes/shit/trains

sync_livestatus.py walks the nested days/ tree; this one is for a single flat
directory such as the train-major output. Resumable: a file already present is
skipped, downloads land on .part and are renamed only when complete, so an
interrupted run never leaves a truncated CSV that a later run would trust.
"""

import argparse
import os
import queue
import re
import shutil
import sys
import threading
import time
import urllib.parse
import urllib.request

LINK = re.compile(r'<a href="([^"?][^"]*)"', re.I)
_print_lock = threading.Lock()
_TTY = sys.stdout.isatty()


def log(msg):
    with _print_lock:
        sys.stdout.write(("\r\033[K" if _TTY else "") + msg + "\n")
        sys.stdout.flush()


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


def main():
    ap = argparse.ArgumentParser(description="Mirror a flat http.server directory.")
    ap.add_argument("url")
    ap.add_argument("--dest", required=True)
    ap.add_argument("--workers", type=int, default=48)
    ap.add_argument("--timeout", type=float, default=120.0)
    ap.add_argument("--suffix", default=".csv",
                    help="only fetch names ending with this (default .csv)")
    ap.add_argument("--no-progress", action="store_true")
    args = ap.parse_args()

    base = args.url if args.url.endswith("/") else args.url + "/"
    os.makedirs(args.dest, exist_ok=True)

    log("listing %s" % base)
    req = urllib.request.Request(base, headers={"User-Agent": "flat-sync"})
    with urllib.request.urlopen(req, timeout=args.timeout) as r:
        html = r.read().decode("utf-8", "replace")
    names = [urllib.parse.unquote(h) for h in LINK.findall(html)]
    names = [n for n in names if n.endswith(args.suffix)]

    have = set(os.listdir(args.dest))
    todo = [n for n in names
            if n not in have or os.path.getsize(os.path.join(args.dest, n)) == 0]
    log("%d file(s) listed, %d already here, %d to fetch"
        % (len(names), len(names) - len(todo), len(todo)))
    if not todo:
        log("nothing to do")
        return

    work = queue.Queue()
    for n in todo:
        work.put(n)

    done = [0]
    failed = [0]
    nbytes = [0]
    lock = threading.Lock()
    started = time.time()
    stop = threading.Event()

    def worker():
        while not stop.is_set():
            try:
                name = work.get_nowait()
            except queue.Empty:
                return
            dest = os.path.join(args.dest, name)
            tmp = dest + ".part"
            ok = False
            for attempt in range(3):
                try:
                    rq = urllib.request.Request(base + urllib.parse.quote(name),
                                                headers={"User-Agent": "flat-sync"})
                    with urllib.request.urlopen(rq, timeout=args.timeout) as resp:
                        expected = resp.getheader("Content-Length")
                        body = resp.read()
                    if expected is not None and len(body) != int(expected):
                        raise IOError("short read")
                    with open(tmp, "wb") as fh:
                        fh.write(body)
                    os.replace(tmp, dest)
                    with lock:
                        done[0] += 1
                        nbytes[0] += len(body)
                    ok = True
                    break
                except Exception:
                    try:
                        os.unlink(tmp)
                    except OSError:
                        pass
                    time.sleep(1.0 * (attempt + 1))
            if not ok:
                with lock:
                    failed[0] += 1

    def bar():
        spin = "|/-\\"
        frame = 0
        last_plain = time.time()
        while not stop.wait(0.25):
            frame += 1
            with lock:
                d, f, b = done[0], failed[0], nbytes[0]
            el = max(time.time() - started, 1e-9)
            rate = b / el
            left = len(todo) - d - f
            eta = (left / (d / el)) if d else None
            line = ("%s %d/%d files  %s  %s/s  %.0f files/s  eta %s%s"
                    % (spin[frame % 4], d, len(todo), human(b), human(rate),
                       d / el, clock(eta), "  %d failed" % f if f else ""))
            if _TTY and not args.no_progress:
                cols = shutil.get_terminal_size((100, 24)).columns
                with _print_lock:
                    sys.stdout.write("\r" + line[:cols - 1] + "\033[K")
                    sys.stdout.flush()
            elif time.time() - last_plain >= 30:
                last_plain = time.time()
                log(line)

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(args.workers)]
    ticker = threading.Thread(target=bar, daemon=True)
    for t in threads:
        t.start()
    ticker.start()
    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        log("interrupted -- rerun to resume")
    stop.set()
    ticker.join(timeout=2)
    if _TTY and not args.no_progress:
        with _print_lock:
            sys.stdout.write("\r\033[K")
            sys.stdout.flush()

    el = max(time.time() - started, 1e-9)
    log("fetched %d files, %s in %s (%.0f files/s, %s/s)"
        % (done[0], human(nbytes[0]), clock(el), done[0] / el, human(nbytes[0] / el)))
    if failed[0]:
        log("%d failed -- rerun to retry them" % failed[0])


if __name__ == "__main__":
    main()
