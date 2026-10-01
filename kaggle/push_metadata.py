#!/usr/bin/env python3
"""
push_metadata.py -- send dataset-metadata.json to Kaggle, including the per-file
and per-column descriptions.

    KAGGLE_USERNAME=... KAGGLE_KEY=... \
        python3 kaggle/push_metadata.py --metadata kaggle/dataset-metadata.json

WHY NOT `kaggle datasets metadata --update`. That path cannot carry file or
column descriptions, for two separate reasons in kaggle 1.7.4.5:

  * It reads the file with json.load() and then calls json.loads() on the
    result, so it only accepts a file that is a JSON *string* of JSON. A normal
    metadata file raises TypeError before anything is sent.
  * It assigns metadata['data'] straight to DatasetSettings.data, whose setter
    requires DatasetSettingsFile objects and rejects plain dicts.

So this builds the settings objects itself and calls the same endpoint. It also
translates the standard `resources` key, which is what `kaggle datasets init`
and the Kaggle docs use, into the `data` field the endpoint actually wants.

--limit sends only the first N files, to check the shape against the live site
before committing to the full list.

SIZE. The endpoint returns a 500 above roughly 25 MB of request body. Measured
against this dataset: 23.2 MB (6,000 files) succeeds, 29.0 MB (7,500) fails.
That matters because 24 columns x 12,936 files costs about 23 MB in JSON keys
and punctuation alone, before any description text, so every file cannot carry
the column schema.

--max-mb handles it: every file keeps its own description, and the column
schema is dropped from the smallest files first until the body fits. Smallest
first because file size tracks how much history a train has, so the ones people
actually open keep their columns documented.
"""

import argparse
import json
import os
import sys

from kaggle.api.kaggle_api_extended import KaggleApi
from kagglesdk.datasets.types.dataset_api_service import (
    ApiUpdateDatasetMetadataRequest,
)
from kagglesdk.datasets.types.dataset_types import (
    DatasetSettings,
    DatasetSettingsFile,
    DatasetSettingsFileColumn,
    SettingsLicense,
)


def body_mb(meta, resources):
    base = {k: v for k, v in meta.items() if k != "resources"}
    body = json.dumps({**base, "resources": resources}, separators=(",", ":"))
    return len(body) / 1024.0**2


def trim(resources, max_mb, meta):
    """Drop schemas from the smallest files until the body fits the budget."""
    if not max_mb or body_mb(meta, resources) <= max_mb:
        return resources, 0

    # Smallest first, so the biggest files keep their column descriptions.
    order = sorted(range(len(resources)),
                   key=lambda i: resources[i].get("totalBytes") or 0)
    kept = [dict(r) for r in resources]
    dropped = 0
    for i in order:
        if "schema" not in kept[i]:
            continue
        del kept[i]["schema"]
        dropped += 1
        if dropped % 250 == 0 and body_mb(meta, kept) <= max_mb:
            break
    while body_mb(meta, kept) > max_mb:
        for i in order:
            if "schema" in kept[i]:
                del kept[i]["schema"]
                dropped += 1
                break
        else:
            break
    return kept, dropped


def build_file(res):
    f = DatasetSettingsFile()
    f.name = res["path"]
    if res.get("description"):
        f.description = res["description"]
    # total_bytes is deliberately not set. The SDK serializes it as
    # "totalbytes" rather than "totalBytes", and the server drops the whole
    # data block over that unknown field: the update returns "updated" and
    # nothing lands. The size is Kaggle's own anyway.
    cols = []
    for field in (res.get("schema") or {}).get("fields") or []:
        c = DatasetSettingsFileColumn()
        c.name = field["name"]
        if field.get("description"):
            c.description = field["description"]
        if field.get("type"):
            c.type = field["type"]
        cols.append(c)
    if cols:
        f.columns = cols
    return f


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--metadata", default="kaggle/dataset-metadata.json")
    ap.add_argument("--limit", type=int, default=0,
                    help="send only the first N files (0 = all)")
    ap.add_argument("--max-mb", type=float, default=22.0,
                    help="request body budget; schemas are dropped from the "
                         "smallest files first to stay under it (0 = no trim)")
    ap.add_argument("--verify", action="store_true",
                    help="check resource paths against the dataset's real file "
                         "list before sending")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    meta = json.load(open(args.metadata))
    owner_slug, dataset_slug = meta["id"].split("/")

    resources = meta.get("resources") or []
    if args.limit:
        resources = resources[:args.limit]

    settings = DatasetSettings()
    settings.title = meta.get("title") or ""
    settings.subtitle = meta.get("subtitle") or ""
    settings.description = meta.get("description") or ""
    settings.is_private = bool(meta.get("isPrivate") or False)
    settings.keywords = meta.get("keywords") or []
    if meta.get("licenses"):
        lics = []
        for l in meta["licenses"]:
            sl = SettingsLicense()
            sl.name = l["name"]
            lics.append(sl)
        settings.licenses = lics
    resources, dropped = trim(resources, args.max_mb, meta)
    settings.data = [build_file(r) for r in resources]

    cols = sum(len(f.columns) for f in settings.data)
    with_schema = sum(1 for f in settings.data if f.columns)
    print("dataset   %s/%s" % (owner_slug, dataset_slug))
    print("title     %s" % settings.title)
    print("licenses  %s" % [l.name for l in settings.licenses])
    print("keywords  %s" % settings.keywords)
    print("files     %d" % len(settings.data))
    print("columns   %d across %d files (%d had the schema dropped to fit)"
          % (cols, with_schema, dropped))
    print("payload   %.1f MB" % body_mb(meta, resources))

    if args.verify:
        # Kaggle matches a resource to a file by exact path and silently drops
        # anything that does not match, still answering "updated". So check the
        # names against the real listing rather than trusting the response.
        api = KaggleApi()
        api.authenticate()
        live, token = set(), None
        while True:
            page = api.dataset_list_files(
                meta["id"], page_token=token, page_size=200)
            names = [f.name if hasattr(f, "name") else f.ref
                     for f in (page.files or [])]
            live.update(names)
            token = getattr(page, "nextPageToken", None) or getattr(
                page, "next_page_token", None)
            if not token or not names:
                break
        ours = {r["path"] for r in resources}
        missing = sorted(ours - live)
        print("\nlive files %d, ours %d, unmatched %d"
              % (len(live), len(ours), len(missing)))
        if missing:
            print("unmatched examples:", missing[:5])
            print("ABORTING: those resources would be ignored.", file=sys.stderr)
            sys.exit(2)
        print("every resource path matches a real file.")

    if args.dry_run:
        print("\ndry run, nothing sent")
        return

    api = KaggleApi()
    api.authenticate()

    request = ApiUpdateDatasetMetadataRequest()
    request.owner_slug = owner_slug
    request.dataset_slug = dataset_slug
    request.settings = settings

    print("\nsending...")
    with api.build_kaggle_client() as kaggle:
        response = kaggle.datasets.dataset_api_client.update_dataset_metadata(
            request)

    errors = list(response.errors or [])
    if errors:
        for e in errors:
            print("ERROR:", e.get("message") if isinstance(e, dict) else e,
                  file=sys.stderr)
        sys.exit(1)
    print("updated.")


if __name__ == "__main__":
    main()
