# Overnight upload

One command. It repairs the 140 broken files, rebuilds the metadata, and
publishes a new dataset version carrying the file and column descriptions.

```sh
cd /Users/chiku/Projects/train_data
export KAGGLE_API_TOKEN=KGAT_...        # your token
./kaggle/upload_version.sh
```

Sanity check first, if you want. Takes a second and changes nothing:

```sh
./kaggle/upload_version.sh --check-only
```

## What it does

1. Prepends the header line to the 140 CSVs that lack one, listed in
   `headerless-files.txt`. Idempotent, so a second run is a no-op. Skip it with
   `--no-fix`.
2. Rescans the data if anything changed, because the row counts move.
3. Writes `dataset-metadata.json` into `/Volumes/shit/trains`.
4. Uploads a new version: 12,935 CSVs plus README.md, about 33 GB.
5. Reads the metadata back and reports.

## When it finishes

Read `kaggle/UPLOAD_STATUS`. It says OK or FAILED, and points at the full log.

```sh
cat kaggle/UPLOAD_STATUS
```

## Things worth knowing before you start

**It takes hours** and it cannot resume. The Kaggle client restarts from the
beginning if it dies, so `--retries=3` gives it three whole attempts. Run it
somewhere the machine will not sleep.

**File paths change.** The current version keeps files under `trains/`, because
it was published from `trains.zip` and Kaggle unpacked it. A folder upload puts
them at the dataset root, so `trains/12624.csv` becomes `12624.csv`. The
metadata is generated to match. The starter notebook finds the files either
way; any code of yours that hardcodes `trains/` would need updating.

**It publishes a new version, it does not delete the old one.** The previous
version stays available.

**`.regroup_state.json` is harvester state, not data.** It is a dotfile and the
uploader skips directories and dotfiles, so it should not end up in the new
version. It is in the current one.

## Why this is necessary at all

Kaggle's metadata API accepts file and column descriptions and throws them away.
`POST /datasets/metadata/...` answers `201 {"errors":[]}` and a read-back shows
nothing, for every shape tried: `data`, `resources`, `settings.data`, `files`.
Creating a version is the only route that applies `resources` from
`dataset-metadata.json`, and a version means sending the data again.

Everything that *can* be set without re-uploading already has been: title,
subtitle, description, license, keywords.
