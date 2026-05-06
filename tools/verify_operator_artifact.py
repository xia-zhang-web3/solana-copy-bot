#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HEX40_RE = re.compile(r"^[0-9a-f]{40}$")
SAFE_PACKAGE_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
SAFE_TARGETS = {"x86_64-unknown-linux-gnu"}


def default_profile_for_package(package):
    if package == "copybot-app":
        return "release"
    return "operator-release"


def safe_binary_set(names, source):
    expected = set()
    for name in names:
        if not name or "/" in name or name.startswith(".") or ".." in name:
            raise ValueError(f"unsafe binary name in {source}: {name}")
        expected.add(name)
    return expected


def expected_binaries_from_workspace(package):
    script = ROOT / "tools" / "package_bins.py"
    try:
        result = subprocess.run(
            [sys.executable, str(script), "--package", package],
            cwd=ROOT,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as exc:
        raise ValueError(
            f"failed deriving workspace package binaries for {package}"
        ) from exc
    return safe_binary_set(result.stdout.split(), "workspace package metadata")


def expected_binaries_for_package(package, manifest, skip_workspace_check):
    manifest_expected = manifest.get("expected_binaries")
    if not manifest_expected:
        raise ValueError("artifact manifest missing expected_binaries")
    manifest_expected = safe_binary_set(manifest_expected, "artifact manifest")
    if skip_workspace_check:
        return manifest_expected
    workspace_expected = expected_binaries_from_workspace(package)
    if manifest_expected != workspace_expected:
        raise ValueError(
            "artifact manifest expected_binaries does not match workspace package metadata: "
            f"manifest={sorted(manifest_expected)} workspace={sorted(workspace_expected)}"
        )
    return workspace_expected


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
    parser.add_argument("--expect-target", default="x86_64-unknown-linux-gnu")
    parser.add_argument("--allow-dirty", action="store_true")
    parser.add_argument(
        "--skip-workspace-bin-check",
        action="store_true",
        help="trust manifest expected_binaries; intended only for rollback of installed immutable releases",
    )
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
    expected_profile = default_profile_for_package(args.expect_package)
    if args.expect_profile != expected_profile:
        print(
            f"{args.expect_package} artifacts must use profile {expected_profile}, got {args.expect_profile}",
            file=sys.stderr,
        )
        return 1
    try:
        expected_binaries = expected_binaries_for_package(
            args.expect_package,
            manifest,
            args.skip_workspace_bin_check,
        )
    except ValueError as exc:
        print(f"failed resolving expected package binaries: {exc}", file=sys.stderr)
        return 1
    if expected_binaries is None:
        print(f"unknown expected package: {args.expect_package}", file=sys.stderr)
        return 1
    if not expected_binaries:
        print(f"expected package has no binary targets: {args.expect_package}", file=sys.stderr)
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
        f"{args.expect_package}-{git_sha}",
        f"{args.expect_package}-{git_sha}-dirty",
    }
    if artifact_id not in expected_artifact_ids:
        print(
            f"artifact_id does not match package-prefixed git_sha/dirty state: {artifact_id}",
            file=sys.stderr,
        )
        return 1
    if manifest.get("package") != args.expect_package:
        print(f"unexpected package: {manifest.get('package')}", file=sys.stderr)
        return 1
    if manifest.get("profile") != args.expect_profile:
        print(f"unexpected profile: {manifest.get('profile')}", file=sys.stderr)
        return 1
    target = manifest.get("target")
    if target not in SAFE_TARGETS:
        print(f"unexpected target: {manifest.get('target')}", file=sys.stderr)
        return 1
    if target != args.expect_target:
        print(f"unexpected target: {target}; expected {args.expect_target}", file=sys.stderr)
        return 1
    if manifest.get("git_dirty") and not args.allow_dirty:
        print("refusing dirty artifact without --allow-dirty", file=sys.stderr)
        return 1
    if not manifest.get("git_dirty"):
        checks = set(manifest.get("checks", []))
        required_checks = {
            "tools/architecture_guard.sh --changed",
            "tools/architecture_guard.sh --all",
        }
        missing = sorted(required_checks - checks)
        if missing:
            print(
                "production artifact manifest missing required checks: "
                + ", ".join(missing),
                file=sys.stderr,
            )
            return 1
        test_prefix = f"cargo test --locked -p {args.expect_package} "
        if not any(check.startswith(test_prefix) for check in checks):
            print(
                "production artifact manifest missing locked package test check: "
                + test_prefix,
                file=sys.stderr,
            )
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
    if seen != expected_binaries:
        print(
            "manifest binary set does not match expected package binaries: "
            f"got {sorted(seen)}, expected {sorted(expected_binaries)}",
            file=sys.stderr,
        )
        return 1

    print(f"verified {artifact_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
