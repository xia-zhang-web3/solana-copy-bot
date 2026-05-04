#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path

HEX40_RE = re.compile(r"^[0-9a-f]{40}$")
SAFE_PACKAGE_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
SAFE_TARGETS = {"x86_64-unknown-linux-gnu", "aarch64-apple-darwin"}


def read_sums(path):
    sums = {}
    for line in path.read_text().splitlines():
        parts = line.split(None, 1)
        if len(parts) != 2:
            raise ValueError(f"invalid checksum line: {line!r}")
        name = parts[1].lstrip("*")
        if "/" in name or name.startswith(".") or ".." in name:
            raise ValueError(f"unsafe checksum artifact name: {name}")
        sums[name] = parts[0].lower()
    return sums


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main():
    parser = argparse.ArgumentParser(description="Verify operator artifact manifest and checksums")
    parser.add_argument("artifact_dir")
    parser.add_argument("--expect-package", default="copybot-discovery-v2")
    parser.add_argument("--expect-profile", default="operator-release")
    parser.add_argument("--allow-dirty", action="store_true")
    args = parser.parse_args()

    artifact_dir = Path(args.artifact_dir)
    manifest_path = artifact_dir / "build-manifest.json"
    sums_path = artifact_dir / "SHA256SUMS"
    if not manifest_path.is_file():
        print(f"missing {manifest_path}", file=sys.stderr)
        return 1
    if not sums_path.is_file():
        print(f"missing {sums_path}", file=sys.stderr)
        return 1

    manifest = json.loads(manifest_path.read_text())
    sums = read_sums(sums_path)
    if not SAFE_PACKAGE_RE.match(args.expect_package):
        print(f"unsafe expected package: {args.expect_package}", file=sys.stderr)
        return 1
    if manifest.get("schema_version") != 1:
        print("unsupported artifact manifest schema_version", file=sys.stderr)
        return 1
    git_sha = manifest.get("git_sha", "")
    if not HEX40_RE.match(git_sha):
        print(f"invalid git_sha: {git_sha}", file=sys.stderr)
        return 1
    artifact_id = manifest.get("artifact_id", "")
    expected_artifact_ids = {
        git_sha,
        f"{git_sha}-dirty",
        f"{args.expect_package}-{git_sha}",
        f"{args.expect_package}-{git_sha}-dirty",
    }
    if artifact_id not in expected_artifact_ids:
        print(f"artifact_id does not match git_sha/dirty state: {artifact_id}", file=sys.stderr)
        return 1
    if manifest.get("package") != args.expect_package:
        print(f"unexpected package: {manifest.get('package')}", file=sys.stderr)
        return 1
    if manifest.get("profile") != args.expect_profile:
        print(f"unexpected profile: {manifest.get('profile')}", file=sys.stderr)
        return 1
    if manifest.get("target") not in SAFE_TARGETS:
        print(f"unexpected target: {manifest.get('target')}", file=sys.stderr)
        return 1
    if manifest.get("git_dirty") and not args.allow_dirty:
        print("refusing dirty artifact without --allow-dirty", file=sys.stderr)
        return 1
    if not manifest.get("git_dirty") and not manifest.get("checks"):
        print("production artifact manifest has no checks", file=sys.stderr)
        return 1

    seen = set()
    for item in manifest.get("binaries", []):
        name = item.get("name")
        expected = item.get("sha256", "").lower()
        if not name or "/" in name or name.startswith(".") or ".." in name:
            print(f"unsafe manifest binary name: {name}", file=sys.stderr)
            return 1
        if name in seen:
            print(f"duplicate binary in manifest: {name}", file=sys.stderr)
            return 1
        seen.add(name)
        if item.get("source_package") != args.expect_package:
            print(f"unexpected source package for {name}: {item.get('source_package')}", file=sys.stderr)
            return 1
        if sums.get(name) != expected:
            print(f"checksum mismatch between manifest and SHA256SUMS for {name}", file=sys.stderr)
            return 1
        binary_path = artifact_dir / name
        if not os.access(binary_path, os.X_OK):
            print(f"artifact binary is not executable: {name}", file=sys.stderr)
            return 1
        actual = sha256(binary_path)
        if actual != expected:
            print(f"checksum mismatch for {name}: {actual} != {expected}", file=sys.stderr)
            return 1
    if not seen:
        print("manifest has no binaries", file=sys.stderr)
        return 1

    print(f"verified {artifact_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
