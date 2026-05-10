#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import socket
import subprocess
import sys
import hashlib
import tarfile
from pathlib import Path, PurePosixPath


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
            name = parts[1].lstrip("*")
            if name in sums:
                raise ValueError(f"duplicate SHA256SUMS entry for {name}")
            sums[name] = parts[0]
    return sums


def file_sha256(path):
    h = hashlib.sha256()
    if not path.is_file():
        return None
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def migration_bundle_files(path):
    names = []
    seen = set()
    with tarfile.open(path, "r:gz") as archive:
        for member in archive.getmembers():
            name = member.name
            if member.isdir() and name in ("migrations", "migrations/"):
                continue
            canonical = PurePosixPath(name).as_posix()
            if (
                name.startswith("/")
                or name.startswith("./")
                or canonical != name
                or ".." in PurePosixPath(name).parts
                or member.issym()
                or member.islnk()
            ):
                raise ValueError(f"unsafe migration bundle member: {name}")
            if not member.isfile() or not name.startswith("migrations/") or not name.endswith(".sql"):
                raise ValueError(f"unexpected migration bundle member: {name}")
            if name in seen:
                raise ValueError(f"duplicate migration bundle member: {name}")
            seen.add(name)
            names.append(name)
    if not names:
        raise ValueError("migration bundle contains no .sql files")
    return sorted(names)


def main():
    parser = argparse.ArgumentParser(description="Write operator artifact manifest")
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--artifact-id", required=True)
    parser.add_argument("--git-sha", required=True)
    parser.add_argument("--git-dirty", action="store_true")
    parser.add_argument("--target", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--package", required=True)
    parser.add_argument("--migration-bundle")
    parser.add_argument("--binary", action="append", default=[])
    parser.add_argument("--expected-binary", action="append", default=[])
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
    migration_bundle = None
    if args.migration_bundle:
        if args.migration_bundle != "migrations.tar.gz":
            print(f"unsupported migration bundle name: {args.migration_bundle}", file=sys.stderr)
            return 1
        if args.migration_bundle not in sums:
            print(f"missing SHA256SUMS entry for {args.migration_bundle}", file=sys.stderr)
            return 1
        if not (artifact_dir / args.migration_bundle).is_file():
            print(f"missing migration bundle file: {args.migration_bundle}", file=sys.stderr)
            return 1
        try:
            files = migration_bundle_files(artifact_dir / args.migration_bundle)
        except (OSError, tarfile.TarError, ValueError) as exc:
            print(f"invalid migration bundle: {exc}", file=sys.stderr)
            return 1
        migration_bundle = {
            "name": args.migration_bundle,
            "sha256": sums[args.migration_bundle],
            "files": files,
        }

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
        "expected_binaries": sorted(set(args.expected_binary)),
        "migration_bundle": migration_bundle,
        "binaries": binaries,
    }
    (artifact_dir / "build-manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
