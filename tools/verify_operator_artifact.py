#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import subprocess
import sys
import tarfile
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools" / "lib"))

from operator_artifact_verify import (  # noqa: E402
    HEX40_RE,
    SAFE_PACKAGE_RE,
    SAFE_TARGETS,
    default_profile_for_package,
    expected_binaries_for_package,
    read_sums,
    reject_linked_artifact_entry,
    sha256,
    workspace_integration_tests,
)


def safe_migration_member_name(name):
    canonical = PurePosixPath(name).as_posix()
    parts = PurePosixPath(name).parts
    return (
        name
        and canonical == name
        and not name.startswith("/")
        and not name.startswith("./")
        and ".." not in parts
        and all(part not in ("", ".", "..") for part in parts)
    )


def migration_bundle_sql_files(bundle_path):
    return sorted(migration_bundle_sql_hashes(bundle_path))


def migration_bundle_sql_hashes(bundle_path):
    files = []
    hashes = {}
    seen = set()
    with tarfile.open(bundle_path, "r:gz") as archive:
        for member in archive.getmembers():
            name = member.name
            if member.isdir() and name in ("migrations", "migrations/"):
                continue
            if not safe_migration_member_name(name):
                raise ValueError(f"unsafe migration bundle member: {name}")
            if member.issym() or member.islnk():
                raise ValueError(f"migration bundle contains linked member: {name}")
            if not member.isfile() or not name.startswith("migrations/") or not name.endswith(".sql"):
                raise ValueError(f"unexpected migration bundle member: {name}")
            if name in seen:
                raise ValueError(f"duplicate migration bundle member: {name}")
            extracted = archive.extractfile(member)
            if extracted is None:
                raise ValueError(f"failed reading migration bundle member: {name}")
            payload = extracted.read()
            seen.add(name)
            files.append(name)
            hashes[name] = hashlib.sha256(payload).hexdigest()
    if not files:
        raise ValueError("migration bundle contains no .sql files")
    return hashes


def workspace_migration_files():
    migrations_dir = ROOT / "migrations"
    return sorted(f"migrations/{path.name}" for path in migrations_dir.glob("*.sql"))


def workspace_migration_hashes():
    migrations_dir = ROOT / "migrations"
    return {
        f"migrations/{path.name}": hashlib.sha256(path.read_bytes()).hexdigest()
        for path in migrations_dir.glob("*.sql")
    }


def installed_migration_hashes(migrations_dir):
    if migrations_dir.is_symlink():
        raise ValueError(f"installed migrations path must not be a symlink: {migrations_dir}")
    if not migrations_dir.is_dir():
        raise ValueError(f"installed migrations directory missing: {migrations_dir}")
    files = []
    hashes = {}
    for path in migrations_dir.rglob("*"):
        try:
            reject_linked_artifact_entry(path, "installed migration entry")
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
        if path.is_dir():
            continue
        rel = path.relative_to(migrations_dir.parent).as_posix()
        if not safe_migration_member_name(rel) or not rel.startswith("migrations/") or not rel.endswith(".sql"):
            raise ValueError(f"unexpected installed migration file: {rel}")
        files.append(rel)
        hashes[rel] = hashlib.sha256(path.read_bytes()).hexdigest()
    if not files:
        raise ValueError("installed migrations directory contains no .sql files")
    if len(hashes) != len(files):
        raise ValueError("installed migrations directory contains duplicate paths")
    return hashes

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
        help="rollback-only compatibility flag; requires INSTALL_COMPLETE in an installed release",
    )
    parser.add_argument(
        "--enforce-workspace-bin-check",
        action="store_true",
        help="also compare manifest expected_binaries and executed checks against the current checkout",
    )
    parser.add_argument(
        "--installed-release-root",
        help="required with --skip-workspace-bin-check; limits rollback verification to installed releases under this directory",
    )
    args = parser.parse_args()
    if args.skip_workspace_bin_check and args.enforce_workspace_bin_check:
        print(
            "--skip-workspace-bin-check and --enforce-workspace-bin-check are mutually exclusive",
            file=sys.stderr,
        )
        return 2

    artifact_dir = Path(args.artifact_dir)
    if artifact_dir.is_symlink():
        print(f"artifact directory must not be a symlink: {artifact_dir}", file=sys.stderr)
        return 1
    manifest_path = artifact_dir / "build-manifest.json"
    sums_path = artifact_dir / "SHA256SUMS"
    if not manifest_path.is_file():
        print(f"missing {manifest_path}", file=sys.stderr)
        return 1
    if not sums_path.is_file():
        print(f"missing {sums_path}", file=sys.stderr)
        return 1
    for child in artifact_dir.iterdir():
        try:
            reject_linked_artifact_entry(child, "artifact directory entry")
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 1

    manifest = json.loads(manifest_path.read_text())
    try:
        sums = read_sums(sums_path)
    except ValueError as exc:
        print(f"failed reading SHA256SUMS: {exc}", file=sys.stderr)
        return 1
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
            args.enforce_workspace_bin_check,
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
    migration_bundle = manifest.get("migration_bundle")
    migration_bundle_name = None
    migration_bundle_files = None
    if migration_bundle is not None:
        if not isinstance(migration_bundle, dict):
            print("artifact manifest migration_bundle must be an object or null", file=sys.stderr)
            return 1
        migration_bundle_name = migration_bundle.get("name")
        if migration_bundle_name != "migrations.tar.gz":
            print(
                f"unsupported migration bundle name: {migration_bundle_name}",
                file=sys.stderr,
            )
            return 1
        if args.expect_package != "copybot-app":
            print("only copybot-app artifacts may include migrations.tar.gz", file=sys.stderr)
            return 1
        migration_bundle_files = migration_bundle.get("files")
        if (
            not isinstance(migration_bundle_files, list)
            or not migration_bundle_files
            or any(not isinstance(item, str) for item in migration_bundle_files)
        ):
            print("artifact manifest migration_bundle.files must be a non-empty string list", file=sys.stderr)
            return 1
        if len(set(migration_bundle_files)) != len(migration_bundle_files):
            print("artifact manifest migration_bundle.files contains duplicates", file=sys.stderr)
            return 1
        migration_bundle_files = sorted(migration_bundle_files)
    elif args.expect_package == "copybot-app":
        print("copybot-app artifact manifest missing required migrations bundle", file=sys.stderr)
        return 1
    allowed_artifact_entries = expected_binaries | {"build-manifest.json", "SHA256SUMS"}
    expected_checksum_entries = set(expected_binaries)
    if migration_bundle_name:
        allowed_artifact_entries.add(migration_bundle_name)
        expected_checksum_entries.add(migration_bundle_name)
    if args.skip_workspace_bin_check:
        allowed_artifact_entries.add("INSTALL_COMPLETE")
        if migration_bundle_name:
            allowed_artifact_entries.add("migrations")
    actual_artifact_entries = {child.name for child in artifact_dir.iterdir()}
    unexpected_entries = sorted(actual_artifact_entries - allowed_artifact_entries)
    if unexpected_entries:
        print(
            "artifact directory contains unexpected entries: "
            + ", ".join(unexpected_entries),
            file=sys.stderr,
        )
        return 1
    install_complete = artifact_dir / "INSTALL_COMPLETE"
    if args.skip_workspace_bin_check and not install_complete.is_file():
        print(
            "--skip-workspace-bin-check is only allowed for installed rollback releases with INSTALL_COMPLETE",
            file=sys.stderr,
        )
        return 1
    if install_complete.exists():
        if not args.skip_workspace_bin_check:
            print("INSTALL_COMPLETE is only allowed for installed rollback releases", file=sys.stderr)
            return 1
        if install_complete.read_text().strip() != manifest.get("artifact_id"):
            print("INSTALL_COMPLETE does not match artifact_id", file=sys.stderr)
            return 1
    if args.skip_workspace_bin_check:
        if not args.installed_release_root:
            print(
                "--skip-workspace-bin-check requires --installed-release-root",
                file=sys.stderr,
            )
            return 1
        release_root = Path(args.installed_release_root)
        try:
            artifact_parent = artifact_dir.resolve().parent
            expected_parent = release_root.resolve()
        except OSError as exc:
            print(f"failed resolving installed release path: {exc}", file=sys.stderr)
            return 1
        if artifact_parent != expected_parent:
            print(
                "--skip-workspace-bin-check is only allowed for direct children of installed release root",
                file=sys.stderr,
            )
            return 1
        if artifact_dir.name != manifest.get("artifact_id"):
            print("installed release directory name does not match artifact_id", file=sys.stderr)
            return 1
    if set(sums) != expected_checksum_entries:
        print(
            "SHA256SUMS entries do not match expected artifact payload: "
            f"got {sorted(sums)}, expected {sorted(expected_checksum_entries)}",
            file=sys.stderr,
        )
        return 1
    if manifest.get("schema_version") != 1:
        print("unsupported artifact manifest schema_version", file=sys.stderr)
        return 1
    git_sha = manifest.get("git_sha", "")
    if not HEX40_RE.match(git_sha):
        print(f"invalid git_sha: {git_sha}", file=sys.stderr)
        return 1
    artifact_id = manifest.get("artifact_id", "")
    git_dirty = manifest.get("git_dirty")
    if not isinstance(git_dirty, bool):
        print("artifact manifest git_dirty must be boolean", file=sys.stderr)
        return 1
    expected_artifact_id = f"{args.expect_package}-{git_sha}{'-dirty' if git_dirty else ''}"
    if artifact_id != expected_artifact_id:
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
    if git_dirty and not args.allow_dirty:
        print("refusing dirty artifact without --allow-dirty", file=sys.stderr)
        return 1
    if not args.skip_workspace_bin_check:
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
        check_commands = [
            command.strip()
            for check in checks
            for command in check.split(";")
            if command.strip()
        ]
        runnable_test_checks = [
            command
            for command in check_commands
            if command.startswith(test_prefix) and "--no-run" not in command
        ]
        if not runnable_test_checks:
            print(
                "production artifact manifest missing locked package test check: "
                + test_prefix,
                file=sys.stderr,
            )
            return 1
        if args.expect_package == "copybot-app":
            required_app_test = (
                "cargo test --locked -p copybot-app --bin copybot-app -- --test-threads=1"
            )
            if required_app_test not in check_commands:
                print(
                    "copybot-app artifact manifest missing executed app test check: "
                    + required_app_test,
                    file=sys.stderr,
                )
                return 1
        package_lib_test = (
            f"cargo test --locked -p {args.expect_package} --lib -- --test-threads=1"
        )
        package_tests = (
            f"cargo test --locked -p {args.expect_package} --tests -- --test-threads=1"
        )
        if package_lib_test in check_commands and package_tests not in check_commands:
            print(
                "production artifact manifest missing executed integration test check: "
                + package_tests,
                file=sys.stderr,
            )
            return 1
        if args.enforce_workspace_bin_check:
            try:
                integration_tests = workspace_integration_tests(args.expect_package)
            except (subprocess.CalledProcessError, ValueError) as exc:
                print(f"failed resolving package integration tests: {exc}", file=sys.stderr)
                return 1
            if integration_tests and package_tests not in check_commands:
                print(
                    "production artifact manifest missing executed integration test check: "
                    + package_tests,
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
        try:
            reject_linked_artifact_entry(binary_path, "artifact binary")
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        if not binary_path.is_file():
            print(f"artifact binary is not a regular file: {name}", file=sys.stderr)
            return 1
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

    if migration_bundle_name:
        expected = migration_bundle.get("sha256", "").lower()
        if sums.get(migration_bundle_name) != expected:
            print(
                "checksum mismatch between manifest and SHA256SUMS for migrations.tar.gz",
                file=sys.stderr,
            )
            return 1
        bundle_path = artifact_dir / migration_bundle_name
        try:
            reject_linked_artifact_entry(bundle_path, "migration bundle")
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        if not bundle_path.is_file():
            print("migration bundle is not a regular file: migrations.tar.gz", file=sys.stderr)
            return 1
        actual = sha256(bundle_path)
        if actual != expected:
            print(
                f"checksum mismatch for migrations.tar.gz: {actual} != {expected}",
                file=sys.stderr,
            )
            return 1
        try:
            actual_bundle_hashes = migration_bundle_sql_hashes(bundle_path)
        except (OSError, tarfile.TarError, ValueError) as exc:
            print(f"invalid migration bundle contents: {exc}", file=sys.stderr)
            return 1
        actual_bundle_files = sorted(actual_bundle_hashes)
        if actual_bundle_files != migration_bundle_files:
            print(
                "migration bundle contents do not match manifest files: "
                f"got {actual_bundle_files}, expected {migration_bundle_files}",
                file=sys.stderr,
            )
            return 1
        if args.enforce_workspace_bin_check:
            expected_workspace_hashes = workspace_migration_hashes()
            expected_workspace_files = sorted(expected_workspace_hashes)
            if actual_bundle_files != expected_workspace_files:
                print(
                    "migration bundle contents do not match workspace migrations: "
                    f"got {actual_bundle_files}, expected {expected_workspace_files}",
                    file=sys.stderr,
                )
                return 1
            if actual_bundle_hashes != expected_workspace_hashes:
                print(
                    "migration bundle SQL content does not match workspace migrations",
                    file=sys.stderr,
                )
                return 1
        if args.skip_workspace_bin_check and (artifact_dir / "migrations").exists():
            try:
                installed_hashes = installed_migration_hashes(artifact_dir / "migrations")
            except ValueError as exc:
                print(f"invalid installed migrations directory: {exc}", file=sys.stderr)
                return 1
            installed_files = sorted(installed_hashes)
            if installed_files != migration_bundle_files:
                print(
                    "installed migrations directory does not match manifest files: "
                    f"got {installed_files}, expected {migration_bundle_files}",
                    file=sys.stderr,
                )
                return 1
            if installed_hashes != actual_bundle_hashes:
                print(
                    "installed migrations directory content does not match migrations.tar.gz",
                    file=sys.stderr,
                )
                return 1

    print(f"verified {artifact_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
