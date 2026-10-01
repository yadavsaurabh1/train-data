#!/usr/bin/env python3
"""
make_cover.py -- draw the Kaggle cover image from the dataset itself.

    python3 kaggle/make_cover.py /Volumes/shit/trains --stats stats.json \
        --out kaggle/cover.png

Every row in the dataset carries station coordinates, so the map draws itself:
one dot per station, shaded by that station's median arrival delay. Nothing is
traced or imported, the outline of the network is what the data looks like.

The delay shading is a single-hue sequential ramp on a dark surface, so "near
zero" recedes toward the background and the late stations are the bright ones.
"""

import argparse
import csv
import json
import math
import os
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.font_manager import FontProperties

SURFACE = "#141413"
INK = "#ffffff"
MUTED = "#8a8980"
# Sequential blue, dark end nearest the surface, stepped for a dark background.
RAMP = ["#12365e", "#1c5cab", "#2a78d6", "#5598e7", "#9ec5f4", "#e4f0ff"]
CMAP = LinearSegmentedColormap.from_list("rail", RAMP)

# Mainland plus islands; keeps a stray bad coordinate from stretching the frame.
BOX = (67.0, 98.5, 6.0, 37.5)

# A minority of files start straight on a data row with no header. Same 24
# fields in the same order, so the header is supplied for those.
HEADER = [
    "date", "train_no", "train_name", "train_type", "seq", "station_code",
    "halts_here", "sch_arrival", "sch_arrival_date", "sch_departure",
    "sch_departure_date", "act_arrival", "act_arrival_date", "act_departure",
    "act_departure_date", "delay_arrival_min", "delay_departure_min",
    "platform", "distance_km", "lat", "lng", "departed", "reported",
    "reported_at",
]


def collect(root, n_files, min_obs):
    """Median arrival delay and a position for every station we can see."""
    paths = sorted(
        (os.path.join(root, f) for f in os.listdir(root)
         if f.endswith(".csv") and not f.startswith(".")),
        key=os.path.getsize, reverse=True)[:n_files]

    delays = defaultdict(list)
    pos = {}
    for i, p in enumerate(paths, 1):
        with open(p, newline="", encoding="utf-8", errors="replace") as fh:
            head = fh.readline()
            fh.seek(0)
            reader = (csv.DictReader(fh) if head.startswith("date,train_no")
                      else csv.DictReader(fh, fieldnames=HEADER))
            for row in reader:
                if row["reported"] != "1":
                    continue
                code, lat, lng = row["station_code"], row["lat"], row["lng"]
                if not (code and lat and lng):
                    continue
                d = row["delay_arrival_min"] or row["delay_departure_min"]
                if not d:
                    continue
                try:
                    lat, lng, d = float(lat), float(lng), float(d)
                except ValueError:
                    continue
                if not (BOX[2] <= lat <= BOX[3] and BOX[0] <= lng <= BOX[1]):
                    continue
                pos[code] = (lng, lat)
                delays[code].append(d)
        if i % 50 == 0:
            print("  %d/%d files" % (i, len(paths)), file=sys.stderr)

    out = []
    for code, ds in delays.items():
        if len(ds) < min_obs:
            continue
        ds.sort()
        med = ds[len(ds) // 2]
        lng, lat = pos[code]
        out.append((lng, lat, med, len(ds)))
    return out


def pick_font(size, weight="normal"):
    for fam in ("Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"):
        try:
            fp = FontProperties(family=fam, size=size, weight=weight)
            if fp.get_family():
                return fp
        except Exception:
            pass
    return FontProperties(size=size, weight=weight)


def commas(n):
    return "{:,}".format(int(n))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root")
    ap.add_argument("--stats", default="stats.json")
    ap.add_argument("--out", default="kaggle/cover.png")
    ap.add_argument("--files", type=int, default=450)
    ap.add_argument("--min-obs", type=int, default=25)
    args = ap.parse_args()

    pts = collect(args.root, args.files, args.min_obs)
    print("%d stations plotted" % len(pts), file=sys.stderr)
    lngs = [p[0] for p in pts]
    lats = [p[1] for p in pts]
    meds = [p[2] for p in pts]

    s = json.load(open(args.stats))["summary"] if os.path.exists(args.stats) else {}

    W, H, DPI = 1600, 840, 100
    fig = plt.figure(figsize=(W / DPI, H / DPI), dpi=DPI, facecolor=SURFACE)

    # Map on the right, type on the left. The frame is the data's own extent
    # with a small margin, so the network fills the panel instead of floating
    # inside a fixed lat/lng box.
    ax = fig.add_axes([0.50, 0.015, 0.49, 0.97])
    ax.set_facecolor(SURFACE)
    vmax = sorted(meds)[int(len(meds) * 0.92)]
    ax.scatter(lngs, lats, c=meds, cmap=CMAP, vmin=0, vmax=vmax,
               s=7.0, linewidths=0)

    # Degrees of longitude shrink away from the equator; without this the
    # subcontinent comes out too wide.
    mean_lat = sum(lats) / len(lats)
    ax.set_aspect(1.0 / math.cos(math.radians(mean_lat)))

    pad = 0.6
    ax.set_xlim(min(lngs) - pad, max(lngs) + pad)
    ax.set_ylim(min(lats) - pad, max(lats) + pad)
    ax.axis("off")

    def text(x, y, s_, size, color=INK, weight="normal", spacing=None):
        fp = pick_font(size, weight)
        t = fig.text(x, y, s_, fontproperties=fp, color=color, va="top")
        if spacing:
            t.set_fontstretch("expanded")
        return t

    text(0.055, 0.90, "INDIAN RAILWAYS", 19, MUTED, "bold")
    text(0.055, 0.79, "Every stop.", 54, INK, "bold")
    text(0.055, 0.655, "Every delay.", 54, INK, "bold")
    text(0.055, 0.50,
         "Scheduled against actual arrival and departure,\n"
         "station by station, since 2020.", 20, "#c3c2b7")

    figs = []
    if s:
        figs = [
            (commas(s["rows"]), "stop records"),
            (commas(s["files"]), "trains"),
            (commas(s["stations"]), "stations"),
        ]
    # Space the figures by how wide each one actually is rather than on a fixed
    # pitch: "204,074,293" is twice the width of "12,935" and a fixed pitch
    # runs them together.
    x = 0.055
    for big, small in figs:
        text(x, 0.29, big, 30, INK, "bold")
        text(x, 0.205, small, 16, MUTED)
        x += max(len(big), len(small)) * 0.0132 + 0.035

    # Legend for the shading, so the map is readable rather than decorative.
    lx, ly, lw, lh = 0.055, 0.075, 0.30, 0.022
    lax = fig.add_axes([lx, ly, lw, lh])
    lax.imshow([[i for i in range(256)]], aspect="auto", cmap=CMAP)
    lax.set_xticks([])
    lax.set_yticks([])
    for sp in lax.spines.values():
        sp.set_visible(False)
    text(lx, ly - 0.012, "on time", 14, MUTED)
    t = fig.text(lx + lw, ly - 0.012, "%d min late" % round(vmax),
                 fontproperties=pick_font(14), color=MUTED, va="top", ha="right")

    fig.savefig(args.out, facecolor=SURFACE, dpi=DPI)
    print("wrote %s (%dx%d)" % (args.out, W, H))


if __name__ == "__main__":
    main()
