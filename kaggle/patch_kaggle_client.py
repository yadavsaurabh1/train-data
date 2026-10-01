#!/usr/bin/env python3
"""
patch_kaggle_client.py -- make `kaggle datasets version` actually resume.

    python3 kaggle/patch_kaggle_client.py            # apply
    python3 kaggle/patch_kaggle_client.py --revert   # restore the original
    python3 kaggle/patch_kaggle_client.py --check    # report, change nothing

This matters because file and column descriptions can only be attached to a
blob at upload time, matched by exact filename, so they need a per-file upload
of all 12,935 CSVs. That upload takes about eleven hours and the first attempt
died on a Bad Gateway at hour nine. Without resume, every retry starts over.

The client writes a receipt per finished file into $TMPDIR/.kaggle/uploads and
means to skip those next time. Four separate bugs in 1.7.4.5 stop it:

1. from_dict builds empty objects. KaggleObject.from_dict is a classmethod
   that RETURNS a new instance; the client calls it as if it mutated in place
   (`req = Cls(); req.from_dict(d)`), discards the result, then rebuilds with
   `Cls(**d)`. Those classes take no constructor arguments, so it raises
   "__init__() got an unexpected keyword argument 'type'", caught and printed
   as "Error while trying to load upload info". Nothing is ever loaded.

2. _is_previous_valid compares two KaggleObject instances with ==, and
   KaggleObject defines no __eq__. That is identity comparison between two
   distinct objects, so it is always False and can_resume is never set.

3. _load_previous_if_any never copies upload_complete off the previous record,
   so even a valid receipt leaves the file looking unfinished and it uploads
   again.

4. _upload_blob returns the ResumableFileUpload object on the
   already-complete path, where every other path returns a token string. The
   caller assigns that straight to upload_file.token.

The original file is backed up alongside as .orig; --revert restores it, and
reinstalling the package also undoes all of this.
"""

import argparse
import os
import shutil
import sys

TARGET = "kaggle/api/kaggle_api_extended.py"

EDITS = [
    (
        "from_dict: build real objects",
        """    req = ApiStartBlobUploadRequest()
    req.from_dict(other['start_blob_upload_request'])
    new = ResumableFileUpload(
        other['path'],
        ApiStartBlobUploadRequest(**other['start_blob_upload_request']),
        context)""",
        """    req = ApiStartBlobUploadRequest.from_dict(
        other['start_blob_upload_request'])
    new = ResumableFileUpload(other['path'], req, context)""",
    ),
    (
        "from_dict: build a real response",
        """      new.start_blob_upload_response = ApiStartBlobUploadResponse(
          **start_blob_upload_response)""",
        """      new.start_blob_upload_response = ApiStartBlobUploadResponse.from_dict(
          start_blob_upload_response)""",
    ),
    (
        "_is_previous_valid: compare by value",
        """  def _is_previous_valid(self, previous):
    return previous.path == self.path and \\
      previous.start_blob_upload_request == self.start_blob_upload_request and \\
      previous.timestamp > time.time() - ResumableFileUpload.RESUMABLE_UPLOAD_EXPIRY_SECONDS""",
        """  def _is_previous_valid(self, previous):
    # KaggleObject has no __eq__, so comparing the request objects directly is
    # an identity check that is always False. Compare their serialized form.
    def as_dict(obj):
      return obj.__class__.to_dict(obj, ignore_defaults=False)

    return previous.path == self.path and \\
      as_dict(previous.start_blob_upload_request) == as_dict(self.start_blob_upload_request) and \\
      previous.timestamp > time.time() - ResumableFileUpload.RESUMABLE_UPLOAD_EXPIRY_SECONDS""",
    ),
    (
        "_load_previous_if_any: carry upload_complete",
        """        if self._is_previous_valid(previous):
          self.start_blob_upload_response = previous.start_blob_upload_response
          self.timestamp = previous.timestamp
          self.can_resume = True""",
        """        if self._is_previous_valid(previous):
          self.start_blob_upload_response = previous.start_blob_upload_response
          self.timestamp = previous.timestamp
          self.can_resume = True
          # Without this a finished file looks unfinished and uploads again.
          self.upload_complete = previous.upload_complete""",
    ),
    (
        "_upload_blob: return a token, not the object",
        """      if file_upload.upload_complete:
        return file_upload
""",
        """      if file_upload.upload_complete:
        return file_upload.get_token()
""",
    ),
]


def find_target():
    import kaggle
    root = os.path.dirname(os.path.dirname(kaggle.__file__))
    path = os.path.join(root, TARGET)
    if not os.path.exists(path):
        sys.exit("could not find %s" % path)
    return path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--revert", action="store_true")
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()

    path = find_target()
    backup = path + ".orig"

    if args.revert:
        if not os.path.exists(backup):
            sys.exit("no backup at %s" % backup)
        shutil.copy2(backup, path)
        print("reverted %s" % path)
        return

    src = open(path).read()

    if args.check:
        for name, old, new in EDITS:
            state = "patched" if new in src else (
                "patchable" if old in src else "NOT FOUND")
            print("  %-44s %s" % (name, state))
        return

    if all(new in src for _, _, new in EDITS):
        print("already patched: %s" % path)
        return

    if not os.path.exists(backup):
        shutil.copy2(path, backup)
        print("backed up to %s" % backup)

    for name, old, new in EDITS:
        if new in src:
            print("  %-44s already patched" % name)
            continue
        if old not in src:
            sys.exit("could not find the code for %r; this client may not be "
                     "version 1.7.4.5" % name)
        src = src.replace(old, new, 1)
        print("  %-44s patched" % name)

    open(path, "w").write(src)
    print("patched %s" % path)


if __name__ == "__main__":
    main()
