#!/usr/bin/env python3
"""
fast_upload.py -- publish a new Kaggle dataset version from a folder, uploading
files in parallel and actually sending the per-file and per-column
descriptions.

    python3 kaggle/fast_upload.py                     # the real thing
    python3 kaggle/fast_upload.py --dry-run           # plan only, sends nothing
    python3 kaggle/fast_upload.py --workers 32

Replaces `kaggle datasets version` for this dataset, for two reasons.

SPEED. The CLI uploads one file at a time, each a full round trip, which came
to about one file every four seconds here: eleven hours for 12,935 files. But
every file is an independent blob upload that returns a token, and the version
is created afterwards by a single request listing the tokens. So the uploads
can run concurrently, the way `hf upload` does, and only the last step is
serial.

DESCRIPTIONS. The CLI reads `resources` from dataset-metadata.json and builds
each file's description and columns, then throws them away. Its
_api_dataset_new_file copies only the token into the request. The request type
has description and columns fields, so this fills them in.

RESUME. Each finished blob leaves a receipt in $TMPDIR/.kaggle/uploads, and a
rerun skips those files. That depends on patch_kaggle_client.py; this refuses
to run on an unpatched client. --fresh ignores receipts and uploads everything.

A 5xx IS NOT A REFUSAL. The version request for 12,935 files takes the server
longer than Kaggle's front end will wait, so it answers 500 or 502 while the
server carries on and creates the version anyway. The first run here treated
each of those as a refusal and retried with a smaller body, and every retry
became its own version: v3, v4 and v5 from one run. So after a 5xx this now
polls the dataset's version list for the notes it just sent, which carry a
unique run id, and only sends again once it is sure nothing was created.

The same run also showed that the full request, column descriptions on every
file at about 66 MB, goes through despite its 500. Smaller bodies are still
available with --budget-mb, but the default is the complete one, sent once.

Upload tokens can be reused: v3 to v5 were all created from one set. So a
version can be made again from banked receipts without re-uploading anything.
"""

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA = "/Volumes/shit/trains"
SKIP_NAMES = {"dataset-metadata.json", "datapackage.json"}

# How long to watch the version list after a 5xx before concluding the request
# really did fail. The runs that "failed" had their versions listed within a
# few minutes.
CONFIRM_WAIT_SECONDS = 20 * 60
CONFIRM_POLL_SECONDS = 30


def load_credentials():
    """Put the token where both halves of the client look for it."""
    path = os.path.join(os.environ.get("KAGGLE_CONFIG_DIR")
                        or os.path.expanduser("~/.kaggle"), "kaggle.json")
    if os.path.exists(path):
        creds = json.load(open(path))
        os.environ.setdefault("KAGGLE_USERNAME", creds["username"])
        os.environ.setdefault("KAGGLE_KEY", creds["key"])
        # The SDK client that uploads blobs and creates versions only accepts
        # a bearer token from this variable.
        os.environ.setdefault("KAGGLE_API_TOKEN", creds["key"])
    if not os.environ.get("KAGGLE_API_TOKEN"):
        sys.exit("no credentials: put your token in %s" % path)


def require_patch():
    # kaggle/__init__.py rebinds kaggle.api to a KaggleApi instance, so the
    # dotted `import kaggle.api.kaggle_api_extended as ext` form fails. Going
    # through importlib reads the real module out of sys.modules.
    import importlib
    ext = importlib.import_module("kaggle.api.kaggle_api_extended")
    src = open(ext.__file__).read()
    if "self.upload_complete = previous.upload_complete" not in src:
        sys.exit("the kaggle client is unpatched, so nothing can resume. Run:\n"
                 "  python3 %s" % os.path.join(HERE, "patch_kaggle_client.py"))


def human(n):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return "%.1f %s" % (n, unit)
        n /= 1024.0
    return "%.1f PB" % n


def clock(sec):
    sec = int(max(sec, 0))
    h, rem = divmod(sec, 3600)
    return "%d:%02d:%02d" % (h, rem // 60, rem % 60)


def list_files(folder):
    out = []
    for name in sorted(os.listdir(folder)):
        path = os.path.join(folder, name)
        # exFAT leaves ._name sidecars; nothing hidden belongs in the dataset.
        if name.startswith(".") or name in SKIP_NAMES or not os.path.isfile(path):
            continue
        out.append(path)
    return out


def already_uploaded(paths, ctx):
    """Which files have a finished upload receipt that still matches them."""
    from kagglesdk.blobs.types.blob_api_service import (
        ApiBlobType, ApiStartBlobUploadRequest)
    done = set()
    for p in paths:
        req = ApiStartBlobUploadRequest()
        req.type = ApiBlobType.DATASET
        req.name = os.path.basename(p)
        req.content_length = os.path.getsize(p)
        req.last_modified_epoch_seconds = int(os.path.getmtime(p))
        fu = ctx.new_resumable_file_upload(p, req)
        if fu.upload_complete and fu.get_token():
            done.add(p)
    return done


def upload_all(api, paths, ctx, workers, skip):
    """Upload every path in parallel; return {path: token}."""
    from kagglesdk.blobs.types.blob_api_service import ApiBlobType

    todo = [p for p in paths if p not in skip]
    total_bytes = sum(os.path.getsize(p) for p in todo)
    tokens, failed = {}, []
    lock = threading.Lock()
    state = {"files": 0, "bytes": 0}
    start = time.time()

    def one(path):
        token = api._upload_blob(path, True, ApiBlobType.DATASET, ctx)
        return path, token

    def report(final=False):
        el = time.time() - start
        rate = state["bytes"] / el if el else 0
        left = (total_bytes - state["bytes"]) / rate if rate else 0
        print("  %s/%s files  %s of %s  %s/s  %s elapsed%s"
              % (format(state["files"], ","), format(len(todo), ","),
                 human(state["bytes"]), human(total_bytes), human(rate),
                 clock(el), "" if final else ", about %s left" % clock(left)),
              flush=True)

    # Skipped files still need their tokens; these return from the receipt
    # without touching the network.
    for p in paths:
        if p in skip:
            _, tok = one(p)
            if tok:
                tokens[p] = tok
            else:
                todo.append(p)

    print("uploading %s files (%s) with %d workers"
          % (format(len(todo), ","), human(total_bytes), workers), flush=True)
    last = 0.0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(one, p): p for p in todo}
        for fut in as_completed(futures):
            path = futures[fut]
            try:
                _, token = fut.result()
            except Exception as e:  # one bad file must not sink the batch
                print("  failed %s: %s" % (os.path.basename(path), e), flush=True)
                token = None
            with lock:
                state["files"] += 1
                state["bytes"] += os.path.getsize(path)
                if token:
                    tokens[path] = token
                else:
                    failed.append(path)
                if time.time() - last > 15:
                    report()
                    last = time.time()
    report(final=True)
    return tokens, failed


def column(field, order):
    from kagglesdk.datasets.types.dataset_api_service import ApiDatasetColumn
    c = ApiDatasetColumn()
    c.order = order
    c.name = field["name"]
    if field.get("description"):
        c.description = field["description"]
    original = (field.get("type") or "").lower()
    if original:
        c.original_type = original
        # The CLI's own mapping, plus integer, which it passes through
        # unmapped; numeric is a type the server is known to accept.
        if original in ("string", "date", "time", "yearmonth", "duration",
                        "geopoint", "geojson"):
            c.type = "string"
        elif original in ("numeric", "number", "year", "integer"):
            c.type = "numeric"
        else:
            c.type = original
    return c


def build_files(paths, tokens, resources, budget_mb):
    """ApiDatasetNewFile per path, trimming columns to fit the budget."""
    from kagglesdk.datasets.types.dataset_api_service import ApiDatasetNewFile

    def make(path, with_columns):
        f = ApiDatasetNewFile()
        f.token = tokens[path]
        res = resources.get(os.path.basename(path)) or {}
        if res.get("description"):
            f.description = res["description"]
        fields = (res.get("schema") or {}).get("fields") or []
        if with_columns and fields:
            f.columns = [column(fd, i) for i, fd in enumerate(fields)]
        return f

    def size_mb(files):
        return len(json.dumps([ApiDatasetNewFile.to_dict(f) for f in files])) / 1024.0**2

    files = [make(p, True) for p in paths]
    if budget_mb is None:
        return files, sum(1 for f in files if f.columns), size_mb(files)

    # Smallest first, so the files people open most keep their columns.
    order = sorted(range(len(paths)), key=lambda i: os.path.getsize(paths[i]))
    current = size_mb(files)
    per_file = {}
    for i in order:
        if current <= budget_mb:
            break
        if files[i].columns:
            per_file.setdefault(i, len(json.dumps(ApiDatasetNewFile.to_dict(files[i]))))
            files[i] = make(paths[i], False)
            current -= (per_file[i] - len(json.dumps(
                ApiDatasetNewFile.to_dict(files[i])))) / 1024.0**2
    return files, sum(1 for f in files if f.columns), size_mb(files)


def list_versions(ref):
    """The dataset's versions, newest first, straight from the view endpoint."""
    import requests
    r = requests.get("https://www.kaggle.com/api/v1/datasets/view/%s" % ref,
                     headers={"Authorization": "Bearer %s"
                              % os.environ["KAGGLE_API_TOKEN"]}, timeout=60)
    r.raise_for_status()
    return r.json().get("versions") or []


def wait_for_version(ref, notes, before):
    """Poll until a new version carrying exactly these notes shows up."""
    deadline = time.time() + CONFIRM_WAIT_SECONDS
    while time.time() < deadline:
        time.sleep(CONFIRM_POLL_SECONDS)
        try:
            for v in list_versions(ref):
                if (v.get("versionNumber") not in before
                        and (v.get("versionNotes") or "").strip() == notes):
                    return v
        except Exception as e:  # a flaky poll is not an answer either way
            print("    (version list unavailable: %s)" % e, flush=True)
    return None


def create_version(api, meta, files, notes):
    from kagglesdk.datasets.types.dataset_api_service import (
        ApiCreateDatasetVersionRequest, ApiCreateDatasetVersionRequestBody)
    owner, slug = meta["id"].split("/")
    body = ApiCreateDatasetVersionRequestBody()
    body.version_notes = notes
    if meta.get("subtitle"):
        body.subtitle = meta["subtitle"]
    if meta.get("description"):
        body.description = meta["description"]
    body.category_ids = meta.get("keywords") or []
    body.delete_old_versions = False
    body.files = files
    req = ApiCreateDatasetVersionRequest()
    req.owner_slug, req.dataset_slug, req.body = owner, slug, body
    with api.build_kaggle_client() as kaggle:
        return kaggle.datasets.dataset_api_client.create_dataset_version(req)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--data", default=DEFAULT_DATA)
    ap.add_argument("--metadata",
                    help="dataset-metadata.json (default: the one in --data)")
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--message", default="Parallel re-upload with per-file and "
                    "per-column descriptions")
    ap.add_argument("--budget-mb", type=float, default=None,
                    help="cap the version request at this size by dropping "
                         "column descriptions from the smallest files first. "
                         "Default: none, every file keeps its columns")
    ap.add_argument("--fresh", action="store_true",
                    help="ignore upload receipts and send every file again")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    load_credentials()
    require_patch()
    from kaggle.api.kaggle_api_extended import KaggleApi, ResumableUploadContext

    meta_path = args.metadata or os.path.join(args.data, "dataset-metadata.json")
    if not os.path.exists(meta_path):
        sys.exit("missing %s; build it with kaggle/build_metadata.py" % meta_path)
    meta = json.load(open(meta_path))
    resources = {r["path"]: r for r in meta.get("resources") or []}

    paths = list_files(args.data)
    names = {os.path.basename(p) for p in paths}
    undescribed = sorted(names - set(resources))
    unknown = sorted(set(resources) - names)
    print("dataset   %s" % meta["id"])
    print("files     %s in %s" % (format(len(paths), ","), args.data))
    if undescribed:
        print("warning   %d files have no description: %s"
              % (len(undescribed), undescribed[:3]))
    if unknown:
        sys.exit("%d described files are missing from the folder: %s"
                 % (len(unknown), unknown[:3]))

    ctx = ResumableUploadContext(no_resume=args.fresh)
    skip = set() if args.fresh else already_uploaded(paths, ctx)
    todo_bytes = sum(os.path.getsize(p) for p in paths if p not in skip)
    print("resume    %s already uploaded, %s to send (%s)"
          % (format(len(skip), ","), format(len(paths) - len(skip), ","),
             human(todo_bytes)))
    if args.dry_run:
        print("\ndry run: nothing sent")
        return

    api = KaggleApi()
    api.authenticate()

    tokens, failed = upload_all(api, paths, ctx, args.workers, skip)
    for attempt in (2, 3):
        if not failed:
            break
        print("retrying %d failed files (round %d)" % (len(failed), attempt))
        more, failed = upload_all(api, failed, ctx, max(4, args.workers // 2),
                                  set())
        tokens.update(more)
    if failed:
        sys.exit("%d files would not upload, e.g. %s. Rerun to retry; finished "
                 "files are skipped." % (len(failed),
                                         [os.path.basename(p) for p in failed[:3]]))

    print("all %s files uploaded; creating the version" % format(len(tokens), ","))
    files, with_cols, mb = build_files(paths, tokens, resources, args.budget_mb)
    # A unique tag in the notes is how a version created behind a 5xx gets
    # recognised as ours rather than someone else's or an older run's.
    notes = "%s [run %s]" % (args.message, time.strftime("%Y%m%d-%H%M%S"))
    before = {v.get("versionNumber") for v in list_versions(meta["id"])}
    print("  %.1f MB request: descriptions on all %s files, columns on %s"
          % (mb, format(len(files), ","), format(with_cols, ",")), flush=True)

    for attempt in (1, 2):
        try:
            resp = create_version(api, meta, files, notes)
        except Exception as e:
            code = getattr(getattr(e, "response", None), "status_code", None)
            if code not in (500, 502, 503, 504):
                raise
            print("  HTTP %s. That usually means Kaggle's front end stopped "
                  "waiting, not that the request failed. Watching the version "
                  "list for up to %d minutes." % (code, CONFIRM_WAIT_SECONDS // 60),
                  flush=True)
            found = wait_for_version(meta["id"], notes, before)
            if found:
                print("\nthe version was created: v%s, status %s"
                      % (found.get("versionNumber"), found.get("status")))
                print("column descriptions on %s of %s files"
                      % (format(with_cols, ","), format(len(files), ",")))
                return
            if attempt == 2:
                sys.exit("no version appeared after two sends, so nothing was "
                         "created. Rerunning is safe: uploads are banked.")
            print("  no version appeared, so the request really failed; "
                  "sending once more", flush=True)
            continue
        if getattr(resp, "error", None):
            sys.exit("Kaggle says: %s" % resp.error)
        print("\nversion created: %s" % (resp.url or resp.ref))
        print("status: %s" % resp.status)
        if resp.invalid_tags:
            print("tags Kaggle rejected: %s" % resp.invalid_tags)
        print("column descriptions on %s of %s files"
              % (format(with_cols, ","), format(len(files), ",")))
        return


if __name__ == "__main__":
    main()
