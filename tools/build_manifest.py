#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import socket
import subprocess
import sys
import hashlib
from pathlib import Path


def command_output(args):
    try:
        return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unavailable"


def read_sums(path):
    sums = {}
    for line in path.read_text().splitlines():
        parts = line.split(None, 1)
        if len(parts) == 2:
            sums[parts[1].lstrip("*")] = parts[0]
    return sums


def file_sha256(path):
    h = hashlib.sha256()
    if not path.is_file():
        return None
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main():
    parser = argparse.ArgumentParser(description="Write operator artifact manifest")
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--artifact-id", required=True)
    parser.add_argument("--git-sha", required=True)
    parser.add_argument("--git-dirty", action="store_true")
    parser.add_argument("--target", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--package", required=True)
    parser.add_argument("--binary", action="append", default=[])
    parser.add_argument("--check", action="append", default=[])
    args = parser.parse_args()

    artifact_dir = Path(args.artifact_dir)
    sums = read_sums(artifact_dir / "SHA256SUMS")
    binaries = []
    for item in args.binary:
        name, source_package = item.split(":", 1)
        if name not in sums:
            print(f"missing SHA256SUMS entry for {name}", file=sys.stderr)
            return 1
        binaries.append(
            {
                "name": name,
                "source_package": source_package,
                "sha256": sums[name],
            }
        )

    manifest = {
        "schema_version": 1,
        "artifact_id": args.artifact_id,
        "git_sha": args.git_sha,
        "git_dirty": args.git_dirty,
        "git_branch": command_output(["git", "rev-parse", "--abbrev-ref", "HEAD"]),
        "cargo_lock_sha256": file_sha256(Path("Cargo.lock")),
        "built_at": dt.datetime.now(dt.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "builder_host": socket.gethostname(),
        "rustc": command_output(["rustc", "--version"]),
        "cargo": command_output(["cargo", "--version"]),
        "target": args.target,
        "profile": args.profile,
        "package": args.package,
        "checks": args.check,
        "binaries": binaries,
    }
    (artifact_dir / "build-manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
