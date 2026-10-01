# Indian Railways: stop-level running and delay history

One row is one train, at one station, on one day. What time it was due, what time
it actually turned up, and how late that was.

Most public Indian rail data is timetables. This is the other half: what the
trains did instead of what they were supposed to do, recorded stop by stop.

## What is in here

@@ROWS@@ stop records, covering @@RUNS@@ individual journeys by @@TRAINS@@
trains across @@STATIONS@@ stations, from @@FIRST@@ to @@LAST@@.

@@WITH_ACTUAL_PCT@@% of those rows carry an observed arrival or departure time.
The rest are stops the schedule promised but that nothing ever reported on. They
are kept deliberately, because a stop with no report is not the same thing as a
stop that ran on time, and throwing them out would quietly bias every delay
figure upward or downward depending on which stations happen to be silent.

## File layout

One CSV per train, named by its five digit train number. `12624.csv` holds every
recorded run of 12624, oldest first, every stop of every run.

Every file has the same 24 columns in the same order, with a header row, so
reading one is the same as reading all of them:<!--HL_INTRO-->

```python
import pandas as pd

one = pd.read_csv("12624.csv")                      # one train, whole history
runs = one.groupby("date")                          # one journey per group
```

A single journey is identified by `(train_no, date)`. `date` is the day the train
left its origin, and it stays fixed for the whole run. Journeys that cross
midnight are handled by the per-stop date columns, not by `date`.

## Columns

| Column | Type | What it is |
|---|---|---|
| `date` | date | Journey start date at the origin, `YYYY-MM-DD`. Constant within a run. |
| `train_no` | string | Five digit train number. Matches the filename. Keep it as a string, leading zeros are real. |
| `train_name` | string | Name of the service, repeated on every row so a file stands alone. |
| `train_type` | string | Service class. 24 distinct values in two competing vocabularies, see below. |
| `seq` | int | The stop's position in the train's full route. Not contiguous, because the route includes points the file does not list. |
| `station_code` | string | Indian Railways station code, for example `PUNE`, `NDLS`. |
| `halts_here` | 0/1 | 1 if the timetable gives this stop a scheduled halt, 0 if the train only passes through. |
| `sch_arrival` | time | Scheduled arrival, `HH:MM`. Blank at the origin. |
| `sch_arrival_date` | date | Calendar date of that scheduled arrival, `DD Mon YYYY`. This is what moves past midnight. |
| `sch_departure` | time | Scheduled departure, `HH:MM`. Blank at the destination. |
| `sch_departure_date` | date | Calendar date of that scheduled departure. |
| `act_arrival` | time | Observed arrival. Blank where the stop never reported. |
| `act_arrival_date` | date | Calendar date of the observed arrival. |
| `act_departure` | time | Observed departure. Blank where the stop never reported. |
| `act_departure_date` | date | Calendar date of the observed departure. |
| `delay_arrival_min` | float | Minutes late on arrival, one decimal place. Never negative, see the caveats. |
| `delay_departure_min` | float | Minutes late on departure, one decimal place. |
| `platform` | string | Platform number. A string, not a number, because `2A` exists. Often blank. |
| `distance_km` | float | Cumulative distance from the origin, three decimal places. |
| `lat` | float | Station latitude, six decimal places. |
| `lng` | float | Station longitude, six decimal places. |
| `departed` | 0/1 | 1 if the train had left this stop at the time of the last observation. |
| `reported` | 0/1 | 1 if this stop ever filed a status update. **This is the row filter you want.** |
| `reported_at` | timestamp | When that update was filed, ISO 8601 with a `+05:30` offset. |

`reported` is the column that matters most. Filter on it before you compute
anything about delays:

```python
observed = one[one.reported == 1]
```

## Where it came from

Harvested from the public `live_status` cache that backs whereismytrain.in:

```
https://whereismytrain.in/cache/live_status?date=DD-MM-YYYY&train_no=NNNNN
```

One HTTP request per train per day, walking backwards in time from the present,
for every train number in the fleet. The JSON response for a `(train, date)` pair
is flattened into one row per scheduled stop and written to that train's file.
The collector is idempotent: refetching a journey rewrites exactly those rows and
cannot duplicate them.

Which days got queried was decided by each train's timetabled running days, so a
train that only runs on Tuesdays was not asked about Wednesdays. That skips about
a third of all possible requests and costs nothing in coverage.

Nothing here is modelled, imputed or smoothed. Blank means the upstream API
returned nothing for that field.

## Caveats, and they matter

<!--HL_START-->**@@HEADERLESS@@ files have no header row.** Their first line is a data row.
`pd.read_csv` on one of those promotes that row to column names and loses it,
silently and with no error. Check before reading:

```python
def read(path):
    with open(path) as fh:
        has_header = fh.readline().startswith("date,train_no")
    if has_header:
        return pd.read_csv(path, dtype={"train_no": str}, parse_dates=["date"])
    return pd.read_csv(path, header=None, names=COLUMNS,
                       dtype={"train_no": str}, parse_dates=["date"])
```

`COLUMNS` is the column list in the table above, in that order. The affected
files are otherwise fine: same columns, same order, same content.

<!--HL_END-->**Line endings vary between files.** Some are LF, some CRLF. `pd.read_csv`
handles both; a hand-rolled parser might not.

**Delays are never negative.** The upstream API reports 0 for a train that is on
time and 0 for a train that is early. Early running is not recoverable from this
data, so the delay columns are a lower bound on punctuality, not a symmetric
measure. Do not read a mean delay of 12 minutes as "the average train is 12
minutes late", read it as "the average train is at least 12 minutes late".

**There is a hole from March 2020 to late 2021.** Passenger services stopped
during the pandemic. For most trains the gap runs from roughly 2020-03-22 into
late 2021, around 650 consecutive days with no data. A few specials did run, and
those are present. Treat the gap as real history rather than as missing data, but
do not let it into a time series model without handling it.

**Some stations never report.** On a typical 23 stop run, about 3 stops file no
update at all, and it tends to be the same quiet stations every time. That is
systematic, not random, so `reported == 0` rows are not missing at random either.

**A few (train, date) pairs are simply absent.** The upstream API returns a hard
500 for certain combinations, reproducibly rather than under load. Those were
recorded as errors and skipped instead of being retried forever.

**Train numbers are reused over the years.** `train_name` is stored per row
rather than per file for exactly this reason. If a file's name changes partway
through its history, that is the railway renaming or reassigning the number, not
a data error.

**`train_type` speaks two languages.** The upstream API changed its vocabulary
partway through collection and the dataset keeps both, so there are 24 distinct
values where there should be about 14. Short codes cover almost everything:

| Code | Means | Code | Means |
|---|---|---|---|
| `EMU` | Electric multiple unit, suburban | `RAJ` | Rajdhani |
| `MEX` | Mail / express | `SHT` | Shatabdi |
| `SUF` | Superfast | `JSH` | Jan Shatabdi |
| `PAS` | Passenger | `DRNT` | Duronto |
| `SUB` | Suburban | `GBR` | Garib Rath |
| `MEMU` / `DEMU` | Main line / diesel multiple unit | `TOD` | Special services |

The long labels (`MAIL/EXPRESS`, `SUPERFAST`, `PASSENGER`, `AC EXPRESS`,
`AC SUPERFAST`, `Humsafar`, `HILL TRAIN`, `EMU - DELHI`, `EMU - KOLKATA`,
`EMU - MUMBAI`) are the same categories under different names and account for
under 0.4% of rows. Normalise before grouping on this column or you will split
`SUF` and `SUPERFAST` into two classes.

**Train names are as supplied, and they are messy.** Some carry an origin
station prefix (`New Delhi RAJDHANI EXPRES`), some are truncated, and spelling
varies between runs. `train_no` is the reliable identifier; `train_name` is a
label, not a key.

**Coverage is not uniform.** Some trains have thousands of runs, others a handful.
File size is an honest proxy: the biggest files are daily long distance trains,
the smallest are specials that ran a few times and stopped.

## Things worth doing with it

- Punctuality by route, by zone, by season, by train class.
- Where delay accumulates along a route, and where it gets recovered. The
  `distance_km` and `delay_arrival_min` pair gives you a delay profile per run.
- Monsoon and fog effects. North Indian winter fog is the obvious one, and the
  `lat`/`lng` columns let you split by region without a separate station file.
- Whether the pandemic restart in late 2021 changed punctuality, using the gap as
  a natural break.
- Predicting arrival delay at stop N from delays at stops 1 to N-1.
- Station level maps. Every row carries coordinates, so no geocoding step is
  needed.

## A note on the source

This is a third party scrape of a public cache endpoint, not an official Indian
Railways release, and not affiliated with or endorsed by whereismytrain.in or
Indian Railways. It is published for research and education. If you plan to build
anything commercial on it, go to the source and check their terms first.
