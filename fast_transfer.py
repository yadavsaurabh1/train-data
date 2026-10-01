#!/usr/bin/env python3
"""
fast_transfer.py -- move the livestatus/days tree between two machines with
parallel reads on the sender, one TCP stream, and parallel writes on the
receiver. One file: copy it to both machines.

    # on the host (the one holding the data)
    python3 fast_transfer.py serve --days /path/train_data/livestatus/days --port 9000

    # here
    python3 fast_transfer.py pull --host 10.5.87.57 --port 9000 --dest ./livestatus/days
    python3 fetch_live_status.py --reindex

WHY NOT tar. On this dataset -- 9.5M files, 41.5 GB -- tar was going to take
68 hours, about 39 files/s. It reads members strictly one at a time, so every
file costs a full open+seek+read round trip and the disk is idle most of the
time. The giveaway was that merely *scanning* the same tree took 870 s: metadata
is fast, serial content reads are not. The same host served 331 files/s over
plain HTTP with 48 concurrent requests, so the disk is perfectly happy to do
~10x more work when asked for several files at once.

So: --readers threads pull file contents off the disk in parallel and feed one
socket; the receiver parses that stream and hands writes to --writers threads.
Both ends stay busy, and there is exactly one connection rather than 9.5M.

PROTOCOL (all integers big-endian, JSON control lines are UTF-8 + '\\n'):

    client -> {"since": null, "until": null}
    server -> {"days": [["2020-01-01", 1990, 6800000], ...]}   name, files, bytes
    client -> {"want": ["2020-01-01", ...]}
    server -> repeated records, then a single uint16 0 to finish:
                  uint16 path_len, path bytes ("2020-01-01/12624.csv"),
                  uint64 size, size bytes of content

RESUME is per day folder. The manifest carries each day's file count, the
client compares it against what is already on disk, and asks only for days that
are missing or short. Interrupt it and rerun: completed days are skipped, a
half-written day is simply re-requested. Files are written to .part and renamed,
so a partial file never counts as present.
"""

import argparse
import io
import json
import os
import queue
import socket
import struct
import sys
import tarfile
import threading
import time

MAGIC = b"LSX1"
CHUNK = 1 << 16

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


# ----------------------------------------------------------------- socket io


def send_all(sock, data):
    sock.sendall(data)


def recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(min(CHUNK, n - len(buf)))
        if not chunk:
            raise ConnectionError("peer closed after %d of %d bytes" % (len(buf), n))
        buf += chunk
    return bytes(buf)


def send_json(sock, obj):
    send_all(sock, (json.dumps(obj) + "\n").encode("utf-8"))


def recv_json(sock):
    buf = bytearray()
    while not buf.endswith(b"\n"):
        c = sock.recv(1)
        if not c:
            raise ConnectionError("peer closed during handshake")
        buf += c
    return json.loads(buf.decode("utf-8"))


# ----------------------------------------------------------------- serve


def build_manifest(days_dir, since, until):
    out = []
    for name in sorted(os.listdir(days_dir)):
        d = os.path.join(days_dir, name)
        if not os.path.isdir(d):
            continue
        if since and name < since:
            continue
        if until and name > until:
            continue
        n = nbytes = 0
        try:
            with os.scandir(d) as it:
                for e in it:
                    if e.is_file():
                        n += 1
                        try:
                            nbytes += e.stat().st_size
                        except OSError:
                            pass
        except OSError:
            continue
        out.append([name, n, nbytes])
    return out


def day_folders(days_dir, since, until):
    """Just the folder names -- one readdir, no stat of anything inside."""
    out = []
    for name in sorted(os.listdir(days_dir)):
        if not os.path.isdir(os.path.join(days_dir, name)):
            continue
        if since and name < since:
            continue
        if until and name > until:
            continue
        out.append(name)
    return out


def serve_one(conn, days_dir, readers):
    req = recv_json(conn)
    since, until = req.get("since"), req.get("until")

    if req.get("mode") == "stream":
        # Skip the sizing pass. Counting 9.5M files took 870 s on this dataset
        # and buys only a percentage and per-day resume -- not worth making the
        # transfer wait for it. Listing the day folders is one readdir.
        want = day_folders(days_dir, since, until)
        log("  stream mode: %d day folder(s), sending immediately" % len(want))
        if not want:
            log("  !! nothing to send -- --days is %s" % days_dir)
            log("  !! it must be the directory HOLDING the YYYY-MM-DD folders")
        send_json(conn, {"stream": True, "days": len(want)})
        stream_days(conn, days_dir, want, readers)
        return

    log("  client wants %s .. %s; building manifest"
        % (since or "start", until or "end"))
    t0 = time.time()
    manifest = build_manifest(days_dir, since, until)
    log("  manifest: %d day folders, %s files, %s (%.0fs)"
        % (len(manifest), format(sum(m[1] for m in manifest), ","),
           human(sum(m[2] for m in manifest)), time.time() - t0))
    if not manifest:
        log("  !! nothing to send -- --days is %s" % days_dir)
        log("  !! it must be the directory HOLDING the YYYY-MM-DD folders")
    send_json(conn, {"days": manifest})

    want = recv_json(conn).get("want", [])
    log("  sending %d day folder(s)" % len(want))
    stream_days(conn, days_dir, want, readers)


def stream_days(conn, days_dir, want, readers):
    # Readers pull file bodies off disk concurrently; one thread owns the socket.
    jobs = queue.Queue()
    bodies = queue.Queue(maxsize=2000)
    for day in want:
        jobs.put(day)

    stop = threading.Event()

    def reader():
        while not stop.is_set():
            try:
                day = jobs.get_nowait()
            except queue.Empty:
                return
            d = os.path.join(days_dir, day)
            try:
                names = sorted(os.listdir(d))
            except OSError:
                continue
            for n in names:
                p = os.path.join(d, n)
                try:
                    if not os.path.isfile(p):
                        continue
                    with open(p, "rb") as fh:
                        data = fh.read()
                except OSError:
                    continue
                bodies.put(("%s/%s" % (day, n), data))

    threads = [threading.Thread(target=reader, daemon=True) for _ in range(readers)]
    for t in threads:
        t.start()

    sent = nbytes = 0
    t0 = time.time()
    last = 0.0
    try:
        while True:
            if all(not t.is_alive() for t in threads) and bodies.empty():
                break
            try:
                path, data = bodies.get(timeout=0.2)
            except queue.Empty:
                continue
            pb = path.encode("utf-8")
            send_all(conn, struct.pack(">H", len(pb)) + pb +
                     struct.pack(">Q", len(data)) + data)
            sent += 1
            nbytes += len(data)
            now = time.time()
            if now - last >= 2.0:
                last = now
                el = max(now - t0, 1e-9)
                log("  sent %s files, %s (%.0f files/s)"
                    % (format(sent, ","), human(nbytes), sent / el))
        send_all(conn, struct.pack(">H", 0))
    finally:
        stop.set()
    el = max(time.time() - t0, 1e-9)
    log("  done: %s files, %s in %s (%.0f files/s, %s/s)"
        % (format(sent, ","), human(nbytes), clock(el), sent / el, human(nbytes / el)))


def cmd_serve(args):
    days_dir = os.path.abspath(args.days)
    if not os.path.isdir(days_dir):
        raise SystemExit("not a directory: %s" % days_dir)
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((args.bind, args.port))
    srv.listen(4)
    log("serving %s on %s:%d  (%d reader threads)"
        % (days_dir, args.bind, args.port, args.readers))
    log("waiting for a client... Ctrl-C to stop")
    try:
        while True:
            conn, addr = srv.accept()
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            log("connection from %s:%d" % addr)
            try:
                serve_one(conn, days_dir, args.readers)
            except Exception as exc:
                log("  client aborted (%s: %s)" % (type(exc).__name__, exc))
            finally:
                try:
                    conn.close()
                except OSError:
                    pass
            if args.once:
                return
    except KeyboardInterrupt:
        log("stopped")


# ----------------------------------------------------------------- pull


class Bar:
    SPIN = "|/-\\"

    def __init__(self, total_files, total_bytes, enabled):
        self.tf, self.tb = total_files, total_bytes
        self.tty = enabled and _TTY
        self.files = self.bytes = 0
        self.started = time.time()
        self.frame = 0
        self.lock = threading.Lock()

    def add(self, nbytes):
        with self.lock:
            self.files += 1
            self.bytes += nbytes

    def line(self):
        with self.lock:
            f, b = self.files, self.bytes
        el = max(time.time() - self.started, 1e-9)
        rate = b / el
        if not self.tb:
            # Stream mode: no sizing pass ran, so there is no honest denominator.
            # Show throughput rather than invent a percentage.
            return ("%s  %s files  %s  %s/s  %.0f files/s  %s elapsed"
                    % (self.SPIN[self.frame % 4], format(f, ","), human(b),
                       human(rate), f / el, clock(el)))
        pct = (100.0 * b / self.tb)
        eta = (self.tb - b) / rate if rate > 0 else None
        return ("%s %5.1f%%  %s/%s files  %s/%s  %s/s  eta %s"
                % (self.SPIN[self.frame % 4], pct, format(f, ","),
                   format(self.tf, ","), human(b), human(self.tb),
                   human(rate), clock(eta)))

    def draw(self):
        if not self.tty:
            return
        import shutil as _sh
        line = self.line()
        cols = _sh.get_terminal_size((100, 24)).columns
        with _print_lock:
            sys.stdout.write("\r" + line[:cols - 1] + "\033[K")
            sys.stdout.flush()

    def loop(self, stop):
        last_plain = time.time()
        while not stop.wait(0.25):
            self.frame += 1
            if self.tty:
                self.draw()
            elif time.time() - last_plain >= 30:
                last_plain = time.time()
                log(self.line())


def cmd_pull(args):
    dest = os.path.abspath(args.dest)
    os.makedirs(dest, exist_ok=True)
    s = socket.create_connection((args.host, args.port), timeout=args.timeout)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.settimeout(args.timeout)

    mode = "manifest" if args.manifest else "stream"
    send_json(s, {"since": args.since, "until": args.until, "mode": mode})

    if mode == "stream":
        hello = recv_json(s)
        ndays = hello.get("days", 0)
        log("stream mode: %d day folder(s), no sizing pass -- files start now"
            % ndays)
        if not ndays:
            log("")
            log("the server found no day folders. --days on the HOST is wrong:")
            log("    it must point at .../train_data/livestatus/days")
            log("    (the directory that HOLDS the YYYY-MM-DD folders)")
            return
        want_files = want_bytes = 0      # unknown up front; that is the trade
    else:
        manifest = recv_json(s)["days"]
        log("manifest: %d day folders, %s files, %s"
            % (len(manifest), format(sum(m[1] for m in manifest), ","),
               human(sum(m[2] for m in manifest))))
        if not manifest:
            send_json(s, {"want": []})
            log("")
            log("the server found no day folders. --days on the HOST is wrong:")
            log("    it must point at .../train_data/livestatus/days")
            return

        # Ask only for days we are missing or short on -- that is the resume.
        want, want_files, want_bytes = [], 0, 0
        for name, n, nbytes in manifest:
            local = os.path.join(dest, name)
            have = 0
            if args.archive:
                have = 0                  # an archive is written start to finish
            elif os.path.isdir(local):
                try:
                    have = sum(1 for e in os.scandir(local) if e.is_file()
                               and not e.name.endswith(".part"))
                except OSError:
                    have = 0
            if have < n:
                want.append(name)
                want_files += n - have
                want_bytes += int(nbytes * (1 - have / n)) if n else nbytes
        log("need %d day folder(s): about %s files, %s"
            % (len(want), format(want_files, ","), human(want_bytes)))
        send_json(s, {"want": want})
        if not want:
            log("every day folder is already complete locally -- nothing to do")
            return

    bar = Bar(want_files, want_bytes, not args.no_progress)
    stop = threading.Event()
    ticker = threading.Thread(target=bar.loop, args=(stop,), daemon=True)
    ticker.start()

    writeq = queue.Queue(maxsize=4000)
    known_dirs = set()
    dirs_lock = threading.Lock()
    failures = [0]

    # Writing 9.5M loose files onto exFAT is not possible: its allocation block
    # is 128 KB, so every 4 KB CSV consumes 128 KB and the tree needs ~1.2 TB.
    # A single streamed archive has no per-file slack, and gzip takes the ~49 GB
    # tar down to roughly 8 GB. One sequential write, which is also what that
    # filesystem is fastest at.
    archive = None
    if args.archive:
        os.makedirs(os.path.dirname(os.path.abspath(args.archive)) or ".", exist_ok=True)
        mode = "w|gz" if args.archive.endswith((".gz", ".tgz")) else "w|"
        archive = tarfile.open(args.archive, mode,
                               **({"compresslevel": args.level} if "gz" in mode else {}))
        log("writing archive %s (mode %s, level %d)" % (args.archive, mode, args.level))

    def archive_writer():
        while True:
            item = writeq.get()
            if item is None:
                return
            rel, data = item
            try:
                info = tarfile.TarInfo("days/" + rel)
                info.size = len(data)
                info.mtime = 0
                archive.addfile(info, io.BytesIO(data))
                bar.add(len(data))
            except Exception:
                failures[0] += 1

    def writer():
        while True:
            item = writeq.get()
            if item is None:
                return
            rel, data = item
            path = os.path.join(dest, rel)
            d = os.path.dirname(path)
            if d not in known_dirs:
                try:
                    os.makedirs(d, exist_ok=True)
                except OSError:
                    pass
                with dirs_lock:
                    known_dirs.add(d)
            tmp = path + ".part"
            try:
                with open(tmp, "wb") as fh:
                    fh.write(data)
                os.replace(tmp, path)
                bar.add(len(data))
            except OSError:
                failures[0] += 1
                try:
                    os.unlink(tmp)
                except OSError:
                    pass

    # tarfile is a single sequential stream, so exactly one writer in that mode.
    if archive is not None:
        writers = [threading.Thread(target=archive_writer, daemon=True)]
    else:
        writers = [threading.Thread(target=writer, daemon=True) for _ in range(args.writers)]
    for t in writers:
        t.start()

    t0 = time.time()
    try:
        while True:
            (plen,) = struct.unpack(">H", recv_exact(s, 2))
            if plen == 0:
                break
            rel = recv_exact(s, plen).decode("utf-8")
            (size,) = struct.unpack(">Q", recv_exact(s, 8))
            data = recv_exact(s, size) if size else b""
            writeq.put((rel, data))
    except KeyboardInterrupt:
        log("interrupted -- rerun to resume from the last complete day")
    except Exception as exc:
        log("transfer ended early (%s: %s) -- rerun to resume"
            % (type(exc).__name__, exc))
    finally:
        for _ in writers:
            writeq.put(None)
        for t in writers:
            t.join(timeout=300)
        if archive is not None:
            try:
                archive.close()
            except Exception as exc:
                log("closing the archive failed (%s)" % exc)
        stop.set()
        ticker.join(timeout=2)
        if bar.tty:
            with _print_lock:
                sys.stdout.write("\r\033[K")
                sys.stdout.flush()
        try:
            s.close()
        except OSError:
            pass

    el = max(time.time() - t0, 1e-9)
    log("received %s files, %s in %s (%.0f files/s, %s/s)"
        % (format(bar.files, ","), human(bar.bytes), clock(el),
           bar.files / el, human(bar.bytes / el)))
    if failures[0]:
        log("%d file(s) failed to write" % failures[0])
    if args.archive:
        try:
            log("archive: %s (%s)" % (args.archive, human(os.path.getsize(args.archive))))
        except OSError:
            pass
        log("extract later with:  tar xzf %s -C <dest>" % args.archive)
    else:
        log("now run:  python3 fetch_live_status.py --reindex")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="mode", required=True)

    sv = sub.add_parser("serve", help="run on the machine that HAS the data")
    sv.add_argument("--days", required=True, help="path to livestatus/days")
    sv.add_argument("--bind", default="0.0.0.0")
    sv.add_argument("--port", type=int, default=9000)
    sv.add_argument("--readers", type=int, default=64,
                    help="parallel disk readers (default: 64; this is the knob "
                         "that beats tar's serial walk)")
    sv.add_argument("--once", action="store_true", help="exit after one client")
    sv.set_defaults(func=cmd_serve)

    pl = sub.add_parser("pull", help="run on the machine that WANTS the data")
    pl.add_argument("--host", required=True)
    pl.add_argument("--port", type=int, default=9000)
    pl.add_argument("--dest", default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "livestatus", "days"))
    pl.add_argument("--since", help="earliest day folder")
    pl.add_argument("--until", help="latest day folder")
    pl.add_argument("--writers", type=int, default=32, help="parallel disk writers")
    pl.add_argument("--archive", metavar="PATH",
                    help="stream into one .tar.gz instead of writing loose files. "
                         "Required for exFAT/FAT targets, where 128 KB allocation "
                         "blocks would turn 9.5M small files into ~1.2 TB. No "
                         "resume in this mode -- slice with --since/--until "
                         "and write one archive per slice.")
    pl.add_argument("--level", type=int, default=6, metavar="N",
                    help="gzip level for --archive (default 6; 1 is much faster "
                         "and still compresses this text well)")
    pl.add_argument("--manifest", action="store_true",
                    help="ask the server to count everything first, for an exact "
                         "percentage and per-day resume. Costs a full walk of the "
                         "tree before a single byte moves -- 870s on 9.5M files. "
                         "Default is to stream straight away.")
    pl.add_argument("--timeout", type=float, default=3600.0,
                    help="socket timeout in seconds (default 3600; the old 120 "
                         "was shorter than the manifest pass and killed the "
                         "handshake before it could finish)")
    pl.add_argument("--no-progress", action="store_true")
    pl.set_defaults(func=cmd_pull)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
