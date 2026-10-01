# Indian Railways: stop-level running and delay history

One row is one train, at one station, on one day. What time it was due, what
time it actually turned up, and how late that was.

One CSV per train, named by its five digit train number. `12624.csv` holds every
recorded run of train 12624, oldest first. Every file has the same 24 columns.

```python
import pandas as pd

one = pd.read_csv("12624.csv", dtype={"train_no": str}, parse_dates=["date"])
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
