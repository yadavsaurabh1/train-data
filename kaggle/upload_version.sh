#!/usr/bin/env bash
#
# upload_version.sh -- publish a new version of the Kaggle dataset, carrying the
# per-file and per-column descriptions that the metadata API silently refuses.
#
#   ./kaggle/upload_version.sh
#
# Credentials come from ~/.kaggle/kaggle.json, the file the Kaggle client reads
# anyway. KAGGLE_API_TOKEN in the environment overrides it.
#
# WHY A FULL RE-UPLOAD. Kaggle's metadata endpoint accepts file-level metadata
# and discards it: POST returns 201 {"errors":[]} and reading it back gives
# nothing, for every payload shape tried (data, resources, settings.data,
# files). The only path that applies `resources` from dataset-metadata.json is
# creating a dataset version, and that means sending the data again.
#
# WHAT IT DOES, in order:
#   1. Repairs the 140 CSVs that have no header row (skip with --no-fix).
#   2. Rebuilds dataset-metadata.json against the current contents.
#   3. Uploads a new version of all 12,935 CSVs plus README.md.
#
# It logs everything and writes UPLOAD_STATUS on the way out, so the result is
# readable after the fact without scrolling a terminal.
#
# NOTE ON PATHS. The current version has files under trains/, because it was
# published from trains.zip and Kaggle unpacked it. Uploading a folder puts
# them at the root instead, so this regenerates the metadata with no prefix to
# match. Anyone's existing code that hardcodes "trains/" would need the new
# path; the starter notebook finds the files either way.
#
# NOT RESUMABLE. The Kaggle client has no resume, so an interrupted upload
# starts over. --retries gives it a few attempts before giving up.

set -uo pipefail

PROJECT="/Users/chiku/Projects/train_data"
DATA="/Volumes/shit/trains"
STATS="$PROJECT/kaggle/stats.json"
PY="$PROJECT/.venv/bin/python"
KG="$PROJECT/.venv/bin/kaggle"
LOG="$PROJECT/kaggle/upload_$(date +%Y%m%d-%H%M%S).log"
STATUS="$PROJECT/kaggle/UPLOAD_STATUS"
MESSAGE="Add per-file and per-column descriptions; files moved to the dataset root"

FIX_HEADERS=1
RETRIES=3
CHECK_ONLY=0
for arg in "$@"; do
    case "$arg" in
        --no-fix) FIX_HEADERS=0 ;;
        --check-only) CHECK_ONLY=1 ;;
        --retries=*) RETRIES="${arg#*=}" ;;
        -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
        *) echo "unknown option: $arg" >&2; exit 2 ;;
    esac
done

exec > >(tee -a "$LOG") 2>&1

say() { echo "[$(date '+%H:%M:%S')] $*"; }
finish() {
    {
        echo "result:   $1"
        echo "detail:   $2"
        echo "finished: $(date '+%Y-%m-%d %H:%M:%S')"
        echo "log:      $LOG"
    } > "$STATUS"
    say "$1: $2"
    say "status written to $STATUS"
    exit "$3"
}

say "log: $LOG"

# ---------------------------------------------------------------- preflight
# The write path needs a bearer token in KAGGLE_API_TOKEN; the download path
# wants username/key. Both come out of kaggle.json, so nothing has to be typed
# or kept in shell history.
CREDS="${KAGGLE_CONFIG_DIR:-$HOME/.kaggle}/kaggle.json"
if [ -z "${KAGGLE_API_TOKEN:-}" ] && [ -f "$CREDS" ]; then
    KAGGLE_API_TOKEN=$("$PY" -c "import json,sys;print(json.load(open(sys.argv[1]))['key'])" "$CREDS" 2>/dev/null)
    KAGGLE_USERNAME=$("$PY" -c "import json,sys;print(json.load(open(sys.argv[1]))['username'])" "$CREDS" 2>/dev/null)
    export KAGGLE_API_TOKEN KAGGLE_USERNAME
    say "credentials loaded from $CREDS"
fi
[ -n "${KAGGLE_API_TOKEN:-}" ] || finish FAILED \
    "no credentials: put your token in $CREDS or set KAGGLE_API_TOKEN" 2
export KAGGLE_USERNAME="${KAGGLE_USERNAME:-yadavsaurabh5517}"
export KAGGLE_KEY="${KAGGLE_KEY:-$KAGGLE_API_TOKEN}"

[ -d "$DATA" ] || finish FAILED "$DATA is not mounted" 2
[ -f "$STATS" ] || finish FAILED "missing $STATS (run scan_dataset.py first)" 2
[ -x "$KG" ] || finish FAILED "kaggle client not found at $KG" 2

if ! "$PY" "$PROJECT/kaggle/patch_kaggle_client.py" --check 2>/dev/null \
     | grep -q "patched"; then
    finish FAILED "the kaggle client is unpatched, so a retry would restart \
from zero. Run: $PY $PROJECT/kaggle/patch_kaggle_client.py" 2
fi
say "kaggle client is patched, uploads can resume"

CSV_COUNT=$(find "$DATA" -maxdepth 1 -name '*.csv' | wc -l | tr -d ' ')
say "found $CSV_COUNT csv files in $DATA"
[ "$CSV_COUNT" -gt 12000 ] || finish FAILED "only $CSV_COUNT csv files; expected ~12935" 2

FREE_GB=$(df -g "$DATA" | awk 'NR==2 {print $4}')
say "free space on the volume: ${FREE_GB}G"

if [ "$CHECK_ONLY" = 1 ]; then
    say "preflight passed; --check-only, so stopping before touching anything"
    exit 0
fi

# ------------------------------------------------------- repair the headers
HDR='date,train_no,train_name,train_type,seq,station_code,halts_here,sch_arrival,sch_arrival_date,sch_departure,sch_departure_date,act_arrival,act_arrival_date,act_departure,act_departure_date,delay_arrival_min,delay_departure_min,platform,distance_km,lat,lng,departed,reported,reported_at'
LIST="$PROJECT/kaggle/headerless-files.txt"

if [ "$FIX_HEADERS" = 1 ] && [ -f "$LIST" ]; then
    say "repairing files with no header row"
    fixed=0; already=0; failed=0
    while read -r f; do
        [ -n "$f" ] || continue
        path="$DATA/$f"
        [ -f "$path" ] || { say "  missing: $f"; continue; }
        # Idempotent: a file that already starts with the header is left alone,
        # so re-running this never stacks a second header on top.
        if head -1 "$path" | grep -q '^date,train_no'; then
            already=$((already + 1)); continue
        fi
        if printf '%s\n' "$HDR" | cat - "$path" > "$path.tmp" 2>/dev/null \
           && mv "$path.tmp" "$path"; then
            fixed=$((fixed + 1))
        else
            rm -f "$path.tmp"; failed=$((failed + 1)); say "  FAILED: $f"
        fi
    done < "$LIST"
    say "headers: $fixed repaired, $already already correct, $failed failed"
    [ "$failed" -eq 0 ] || finish FAILED "$failed files could not be repaired" 1

    if [ "$fixed" -gt 0 ]; then
        say "rescanning, because the row counts just changed"
        "$PY" "$PROJECT/scan_dataset.py" "$DATA" --out "$STATS" --workers 10 \
            || finish FAILED "rescan failed" 1
    fi
else
    say "skipping the header repair"
fi

# ------------------------------------------------------------- metadata
# No prefix: a folder upload puts the files at the dataset root, so the
# resource paths must not carry the old trains/ folder.
say "building dataset-metadata.json"
"$PY" "$PROJECT/kaggle/build_metadata.py" \
    --stats "$STATS" --prefix "" --data-dir "$DATA" \
    --description "$PROJECT/kaggle/description.md" \
    --out "$DATA/dataset-metadata.json" \
    || finish FAILED "could not build the metadata" 1

"$PY" - "$DATA/dataset-metadata.json" <<'CHECK' || finish FAILED "metadata failed its own check" 1
import json, sys
m = json.load(open(sys.argv[1]))
assert m["id"] == "yadavsaurabh5517/indian-railways-dataset", m["id"]
assert m["licenses"] == [{"name": "GPL-3.0"}], m["licenses"]
assert "@@" not in m["description"], "description still has unreplaced tokens"
bad = [r["path"] for r in m["resources"] if "/" in r["path"]]
assert not bad, "resource paths must be flat, got %s" % bad[:3]
print("metadata ok: %d resources, %d with a column schema"
      % (len(m["resources"]), sum(1 for r in m["resources"] if r.get("schema"))))
CHECK

# exFAT grows a ._name sidecar whenever a file is written, and the uploader
# filters only the metadata filenames, so anything left here ships as a data
# file. Clear them after the last write, immediately before uploading.
rm -f "$DATA"/._* "$DATA"/.DS_Store 2>/dev/null
STRAY=$(find "$DATA" -maxdepth 1 -type f ! -name '*.csv' ! -name 'README.md' \
        ! -name 'dataset-metadata.json' | wc -l | tr -d ' ')
[ "$STRAY" = 0 ] || finish FAILED "$STRAY unexpected files in $DATA would be uploaded" 1
say "data folder is clean: csv files, README.md, dataset-metadata.json"

# --------------------------------------------------------------- upload
# Say up front how much of this is already done, so the run is legible.
"$PY" - "$DATA" <<'RESUME'
import glob, os, sys
from kaggle.api.kaggle_api_extended import ResumableUploadContext
from kagglesdk.blobs.types.blob_api_service import ApiStartBlobUploadRequest, ApiBlobType
ctx = ResumableUploadContext()
skip = fresh = 0
for p in sorted(glob.glob(os.path.join(sys.argv[1], "*.csv"))):
    r = ApiStartBlobUploadRequest()
    r.type = ApiBlobType.DATASET
    r.name = os.path.basename(p)
    r.content_length = os.path.getsize(p)
    r.last_modified_epoch_seconds = int(os.path.getmtime(p))
    fu = ctx.new_resumable_file_upload(p, r)
    if fu.upload_complete and fu.get_token():
        skip += 1
    else:
        fresh += 1
print("  %s files already uploaded and will be skipped" % format(skip, ","))
print("  %s files to upload" % format(fresh, ","))
RESUME

say "uploading a new version across $CSV_COUNT files"
attempt=1
while [ "$attempt" -le "$RETRIES" ]; do
    say "attempt $attempt of $RETRIES"
    start=$(date +%s)
    if "$KG" datasets version -p "$DATA" -m "$MESSAGE" -r skip; then
        say "upload finished in $(( ($(date +%s) - start) / 60 )) minutes"
        break
    fi
    say "attempt $attempt failed after $(( ($(date +%s) - start) / 60 )) minutes"
    attempt=$((attempt + 1))
    [ "$attempt" -le "$RETRIES" ] && { say "waiting 120s"; sleep 120; }
done
[ "$attempt" -le "$RETRIES" ] || finish FAILED "upload failed $RETRIES times" 1

# --------------------------------------------------------------- verify
say "waiting 60s for Kaggle to process the version"
sleep 60
"$PY" - <<'VERIFY'
import os, requests
tok = os.environ["KAGGLE_API_TOKEN"]
url = ("https://www.kaggle.com/api/v1/datasets/metadata/"
       "yadavsaurabh5517/indian-railways-dataset")
try:
    j = requests.get(url, headers={"Authorization": "Bearer %s" % tok},
                     timeout=60).json()["info"]
    print("  title      %s" % j.get("title"))
    print("  files with metadata %d" % len(j.get("data") or []))
    print("  usability  %.2f%%" % ((j.get("usabilityRating") or 0) * 100))
except Exception as e:
    print("  could not read the metadata back: %s" % e)
VERIFY

finish OK "new version published" 0
