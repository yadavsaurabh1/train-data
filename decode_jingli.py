#!/usr/bin/env python3
"""
Decode `jingli` -- the offline running-days / route-segment blob shipped with
the Where Is My Train app (it is the "ilgnij" entry inside patch.zip; the app
reverses the name via AppUtils.F before opening it).

Format recovered from smali_classes2/nqm.smali, method G()
("loadRouteSegmentsFromStream"). Big-endian, java.io.DataInputStream, records
stored back-to-back with no index -- the app scans sequentially until it has
found every train number it was asked for.

Per record:
    int32  train_no                     rendered "%05d"
    int16  valid_from  packed date      see unpack_date(); 0 => absent
    int16  valid_to    packed date
    uint8  running_days BITMASK         bit i set => train runs on day i
    int32  packed      parent_train = v >>> 11, minute_offset = v & 0x7ff
    uint8  n_master
      n_master x { int32 train_no, int16 stn_id_a, int16 stn_id_b }  (8 bytes)
    int16  stop_count
      stop_count x stop_size bytes -- BUT ONLY WHEN parent_train == 0.

A record with parent_train != 0 is a DERIVED train: it reuses its parent's
route verbatim, shifted by minute_offset, and stores NO stop table of its own
(stop_count is still present and still reports the parent's stop count).
Honouring this is what makes the file parse to the byte -- 13,678 records
landing exactly on EOF. Treating every record as carrying its own stops
desynchronises the stream and silently loses whole runs of trains.

Each route point (12 bytes, or 14 in the older format):
          int16  station_id     -> whereismytrain.db station_info.gid
          uint16 time           time >> 1 = minutes since the previous point,
                                time  & 1 = missing_graph_link_end flag
          uint16 distance       km * 100, measured from the previous halt
          uint16 halt           halt & 1 = train actually stops here,
                                halt >> 1 = halt length in minutes
          int16  top_catchup            small signed correction, often 0
          int16  error_pct_catchup      small signed correction, often 0

TWO FORMAT VERSIONS EXIST, differing only in stop size:

  14-byte stops (what the smali reads, mul-int/lit8 0xe) append
        uint8 platform_no
        uint8 platform_char   0x21 '!' means "no platform"; otherwise the
                              platform is String.format("%d%c", no, char)
  12-byte stops drop those two bytes and carry no platform data at all.

The older patch (where_is_my_train/patch/patch_wimt/jingli, 2,662 records) uses
14; the current one (4.9 MB, 13,678 records) uses 12. detect_stride() picks the
one that lands exactly on EOF, which is unambiguous in practice -- the wrong
stride overruns long before the end.

Arrival time is cumulative: start from the record's minute_offset and add each
point's (time >> 1), modulo 1440 (the app uses `rem-int/lit16 ... 0x5a0`).
Field names come from the `kro` annotations on class nqa (station_id, seq_no,
arr_time_minute_offset, cum_distance, halt_in_secs, platform, ...).

Packed date (nqm.E):
    year = 2000 + (v & 0x7f), month = (v >>> 7) & 0xf, day = (v >>> 11) & 0x1f

Day indexing: BIT 0 == MONDAY, bit 6 == Sunday. Chain of evidence:
nnr.smali extracts bits 0..6 in order and feeds them positionally to
OperatingDays(ZZZZZZZ); that class's toString() names its fields a..g as
monday..sunday, and its a(LocalDate) switch maps java.time DayOfWeek.SUNDAY to
field g and MONDAY to field a. So bit 0 -> field a -> Monday.

NOTE this is the OPPOSITE origin from schedule.db, whose metro-side
TripCalendar.running_days_array is documented "idx 0->6: Sun..Sat". The two
datasets do not share a convention -- do not copy an index between them.

With the parent_train rule and the right stop size, the parse is exact for both
known files: every byte accounted for, ending precisely on EOF, no guessing and
no resynchronisation. decode() raises rather than returning a partial result,
because a short parse means a wrong assumption, not a damaged file.
"""

import struct
import sys

DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
HDR = ">iHHBIB"


def unpack_date(v):
    if v == 0:
        return None
    return (2000 + (v & 0x7F), (v >> 7) & 0xF, (v >> 11) & 0x1F)


def fmt_date(d):
    return "%04d-%02d-%02d" % d if d else "-"


def days_str(mask):
    if mask == 0x7F:
        return "Daily"
    return ",".join(DAYS[i] for i in range(7) if mask >> i & 1) or "(none)"


def plausible(buf, o):
    """Cheap validity gate used both for parsing and for resynchronising."""
    if o + 16 > len(buf):
        return False
    train, vfrom, vto, days, _packed, n_master = struct.unpack_from(HDR, buf, o)
    if not 0 < train <= 99999 or days > 0x7F or n_master > 8:
        return False
    for v in (vfrom, vto):
        if v == 0:
            continue
        y, m, d = (v & 0x7F), (v >> 7) & 0xF, (v >> 11) & 0x1F
        if not (20 <= y <= 40 and 1 <= m <= 12 and 1 <= d <= 31):
            return False
    return True


STOP = ">hHHHhh"
NO_PLATFORM = 0x21  # '!'


def _walk(buf, stop_size):
    """Walk the record stream with a given stop size; return (records, end)."""
    n, off, recs = len(buf), 0, []
    while off < n:
        if off + 16 > n:
            return recs, -1
        train, vfrom, vto, days, packed, n_master = struct.unpack_from(HDR, buf, off)
        parent = packed >> 11
        body = off + 14 + n_master * 8
        if body + 2 > n:
            return recs, -1
        (stops,) = struct.unpack_from(">H", buf, body)
        route_at = body + 2
        end = route_at + (stops * stop_size if parent == 0 else 0)
        if end > n:
            return recs, -1
        recs.append(
            {
                "train_no": "%05d" % train,
                "valid_from": unpack_date(vfrom),
                "valid_to": unpack_date(vto),
                "running_days": days,
                "parent_train": parent,
                "minute_offset": packed & 0x7FF,
                "stops": stops,
                "has_own_route": parent == 0,
                "offset": off,
                "route_at": route_at,
                "stop_size": stop_size,
            }
        )
        off = end
    return recs, off


def detect_stride(buf):
    """Return the stop size whose walk lands exactly on EOF."""
    for size in (12, 14):
        _recs, end = _walk(buf, size)
        if end == len(buf):
            return size
    raise ValueError("no stop size (12 or 14) parses this file to EOF")


def decode_route(buf, rec, start_minute=None):
    """Expand one record's own stop table into an ordered list of route points.
    Derived records have none -- resolve them against their parent first."""
    if not rec["has_own_route"]:
        return []
    off = rec["route_at"]
    size = rec["stop_size"]
    clock = rec["minute_offset"] if start_minute is None else start_minute
    route = []
    for seq in range(rec["stops"]):
        sid, time, dist, halt, top_catchup, err_catchup = struct.unpack_from(
            STOP, buf, off
        )
        platform = ""
        if size == 14:
            plat_no, plat_ch = struct.unpack_from(">BB", buf, off + 12)
            if plat_ch != NO_PLATFORM:
                platform = ("%d%c" % (plat_no, plat_ch)).strip()
        off += size
        clock = (clock + (time >> 1)) % 1440
        route.append(
            {
                "seq": seq,
                "station_id": sid,
                "arr_minute": clock,
                "arr": "%02d:%02d" % (clock // 60, clock % 60),
                "distance_km": dist / 100.0,
                "is_halt": bool(halt & 1),
                "halt_min": halt >> 1,
                "missing_graph_link": bool(time & 1),
                "top_catchup": top_catchup,
                "error_pct_catchup": err_catchup,
                "platform": platform,
            }
        )
    return route


def decode(path):
    """Parse the whole file, auto-detecting the stop size. Raises unless the
    stream lands exactly on EOF -- a clean parse is always achievable, so a
    short one means the format assumption is wrong."""
    buf = open(path, "rb").read()
    size = detect_stride(buf)
    recs, end = _walk(buf, size)
    if end != len(buf):
        raise ValueError("stream ended at %d, expected %d" % (end, len(buf)))
    return recs, size, len(buf)


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "jingli"
    wanted = set(sys.argv[2:])
    recs, skipped, total = decode(path)
    print(
        "recovered %d records; %d/%d bytes unparsed (%.1f%%)"
        % (len(recs), skipped, total, 100.0 * skipped / total)
    )
    rows = [r for r in recs if not wanted or r["train_no"] in wanted]
    print()
    print(
        "%-7s %-11s %-11s %-6s %-24s %s"
        % ("TRAIN", "VALID FROM", "VALID TO", "DEP", "RUNS ON", "STOPS")
    )
    for r in rows[:80]:
        h, m = divmod(r["minute_offset"], 60)
        print(
            "%-7s %-11s %-11s %02d:%02d  %-24s %d"
            % (
                r["train_no"],
                fmt_date(r["valid_from"]),
                fmt_date(r["valid_to"]),
                h % 24,
                m,
                days_str(r["running_days"]),
                r["stops"],
            )
        )


if __name__ == "__main__":
    main()
