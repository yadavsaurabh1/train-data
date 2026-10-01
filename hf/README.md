---
pretty_name: Indian Railways stop-level running and delay history
license: gpl-3.0
language:
  - en
size_categories:
  - "100M<n<1B"
task_categories:
  - time-series-forecasting
  - tabular-regression
tags:
  - trains
  - railways
  - india
  - transportation
  - punctuality
  - delays
  - time-series
  - geospatial
configs:
  # default reads both folders, so load_dataset() gets everything.
  # trains and trains_1 exist so the viewer builds a preview of each;
  # it only converts the start of a large subset, which for default
  # never reaches trains_1.
  - config_name: default
    default: true
    data_files:
      - split: train
        path:
          - "trains/*.csv"
          - "trains_1/*.csv"
    features:
      - name: date
        dtype: string
      - name: train_no
        dtype: string
      - name: train_name
        dtype: string
      - name: train_type
        dtype: string
      - name: seq
        dtype: float64
      - name: station_code
        dtype: string
      - name: halts_here
        dtype: int64
      - name: sch_arrival
        dtype: string
      - name: sch_arrival_date
        dtype: string
      - name: sch_departure
        dtype: string
      - name: sch_departure_date
        dtype: string
      - name: act_arrival
        dtype: string
      - name: act_arrival_date
        dtype: string
      - name: act_departure
        dtype: string
      - name: act_departure_date
        dtype: string
      - name: delay_arrival_min
        dtype: float64
      - name: delay_departure_min
        dtype: float64
      - name: platform
        dtype: string
      - name: distance_km
        dtype: float64
      - name: lat
        dtype: float64
      - name: lng
        dtype: float64
      - name: departed
        dtype: int64
      - name: reported
        dtype: int64
      - name: reported_at
        dtype: string
  - config_name: trains
    data_files:
      - split: train
        path:
          - "trains/*.csv"
    features:
      - name: date
        dtype: string
      - name: train_no
        dtype: string
      - name: train_name
        dtype: string
      - name: train_type
        dtype: string
      - name: seq
        dtype: float64
      - name: station_code
        dtype: string
      - name: halts_here
        dtype: int64
      - name: sch_arrival
        dtype: string
      - name: sch_arrival_date
        dtype: string
      - name: sch_departure
        dtype: string
      - name: sch_departure_date
        dtype: string
      - name: act_arrival
        dtype: string
      - name: act_arrival_date
        dtype: string
      - name: act_departure
        dtype: string
      - name: act_departure_date
        dtype: string
      - name: delay_arrival_min
        dtype: float64
      - name: delay_departure_min
        dtype: float64
      - name: platform
        dtype: string
      - name: distance_km
        dtype: float64
      - name: lat
        dtype: float64
      - name: lng
        dtype: float64
      - name: departed
        dtype: int64
      - name: reported
        dtype: int64
      - name: reported_at
        dtype: string
  - config_name: trains_1
    data_files:
      - split: train
        path:
          - "trains_1/*.csv"
    features:
      - name: date
        dtype: string
      - name: train_no
        dtype: string
      - name: train_name
        dtype: string
      - name: train_type
        dtype: string
      - name: seq
        dtype: float64
      - name: station_code
        dtype: string
      - name: halts_here
        dtype: int64
      - name: sch_arrival
        dtype: string
      - name: sch_arrival_date
        dtype: string
      - name: sch_departure
        dtype: string
      - name: sch_departure_date
        dtype: string
      - name: act_arrival
        dtype: string
      - name: act_arrival_date
        dtype: string
      - name: act_departure
        dtype: string
      - name: act_departure_date
        dtype: string
      - name: delay_arrival_min
        dtype: float64
      - name: delay_departure_min
        dtype: float64
      - name: platform
        dtype: string
      - name: distance_km
        dtype: float64
      - name: lat
        dtype: float64
      - name: lng
        dtype: float64
      - name: departed
        dtype: int64
      - name: reported
        dtype: int64
      - name: reported_at
        dtype: string
---

# Indian Railways: stop-level running and delay history

One row is one train, at one station, on one day. What time it was due, what
time it actually turned up, and how late that was.

One CSV per train, named by its five digit train number. `12624.csv` holds every
recorded run of train 12624, oldest first. Every file has the same 24 columns.

The files are split across two folders, because the Hub allows at most 10,000
files per folder. The split is by the train number's first digit:

| Folder | Train numbers | Files |
|---|---|---|
| `trains/` | `0xxxx` to `4xxxx` | 6,738 |
| `trains_1/` | `5xxxx` to `9xxxx` | 6,197 |

`load_dataset("yadavsaurabh/indian-railways-runs")` reads both as one table.
To load one half, name it: `load_dataset("yadavsaurabh/indian-railways-runs", "trains_1")`.

```python
import pandas as pd

one = pd.read_csv("trains/12624.csv", dtype={"train_no": str}, parse_dates=["date"])
obs = one[one.reported == 1]          # the rows that were actually observed
```

A journey is `(train_no, date)`. `date` is the day the train left its origin and
stays fixed across the whole run; the per-stop `*_date` columns are what move
past midnight.

## Read this before computing anything

`reported` is 1 when a stop filed a live status update and 0 when it never did.
A blank `act_arrival` is not an on-time arrival, it is an absence of evidence.
About 3 stops in 23 never report, and it is systematically the same quiet
stations, so filter on `reported == 1` first.

**Delays are never negative.** The source reports 0 for on time and 0 for early.
Punctuality figures from this data are a lower bound, not a symmetric measure.

**March 2020 to late 2021 is empty for most trains**, around 650 consecutive
days, because passenger services stopped. A few specials ran and they are here.
That gap is real history, not missing data.

## Columns

| Column | What it is |
|---|---|
| `date` | Journey start date at the origin, `YYYY-MM-DD`. Constant within a run. |
| `train_no` | Five digit train number, same as the filename. Keep it a string. |
| `train_name` | Service name, repeated on every row. |
| `train_type` | Service class, e.g. `MAIL/EXPRESS`, `SUPERFAST`. |
| `seq` | Position in the full route. Not contiguous, and blank on some rows. |
| `station_code` | Indian Railways station code. |
| `halts_here` | 1 for a scheduled halt, 0 for a pass-through. |
| `sch_arrival` / `sch_departure` | Scheduled times, `HH:MM`. |
| `sch_arrival_date` / `sch_departure_date` | Their calendar dates, `DD Mon YYYY`. |
| `act_arrival` / `act_departure` | Observed times. Blank where unreported. |
| `act_arrival_date` / `act_departure_date` | Their calendar dates. |
| `delay_arrival_min` / `delay_departure_min` | Minutes late, one decimal. Never negative. |
| `platform` | Platform used. A string: `2A` exists, and some stations report in Devanagari numerals (`१`, `२`). Often blank. |
| `distance_km` | Cumulative distance from the origin. |
| `lat` / `lng` | Station coordinates, six decimals. |
| `departed` | 1 if the train had left this stop at the last observation. |
| `reported` | 1 if this stop ever filed an update. |
| `reported_at` | When it did, ISO 8601 with a `+05:30` offset. |

## Where it came from

Harvested from the public `live_status` cache behind whereismytrain.in, one
request per train per day, flattened to one row per scheduled stop. Nothing is
imputed or smoothed; blank means the source returned nothing.

This is an independent third-party collection. It is not affiliated with or
endorsed by whereismytrain.in or Indian Railways.
