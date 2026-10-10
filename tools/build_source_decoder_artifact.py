#!/usr/bin/env python3
"""Build and seal one clean, offline ingestion lib-test harness on macOS arm64."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import struct
import subprocess
import sys
import tarfile
import time


PACKAGE = "copybot-ingestion"
TARGET = "aarch64-apple-darwin"
BINARY = "source_decoder_native_tests"
PROFILE = "test"
BUILD_BUDGET_SECONDS = 480
CHECK_FILTERS = (
    "source::tests::quote_sol_tests::",
    "source::tests::source_selection_replay::",
    "source::tests::ammv4_tests::",
    "source::http_recovery::tests::float_roundtrip_tests::",
)


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def command(argv, root, timeout=30, env=None):
    """A timeout stops the Cargo/rustc process group as well as its parent."""
    started = time.monotonic()
    process = subprocess.Popen(
        argv, cwd=root, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.communicate()
        raise ValueError(f"command_budget_exceeded:{timeout}:{argv[0]}") from None
    require(process.returncode == 0,
            f"command_failed:{argv[0]}:{process.returncode}:{stderr[-8000:]}:{stdout[-8000:]}")
    return stdout, time.monotonic() - started


def clean_source(root, expected):
    require(re.fullmatch(r"[0-9a-f]{40}", expected), "invalid_expected_sha")
    head, _ = command(["git", "rev-parse", "HEAD"], root)
    require(head.strip() == expected, "unexpected_source_sha")
    dirty, _ = command(["git", "status", "--porcelain=v1", "--untracked-files=normal"], root)
    require(not dirty.strip(), "dirty_source_tree")


def source_bindings(root):
    tracked, _ = command(["git", "ls-files", "-z"], root)
    selected = []
    for name in tracked.split("\0"):
        if not name:
            continue
        if (name.endswith(".rs") or name.endswith("Cargo.toml")
                or name == "Cargo.lock" or name.startswith("rust-toolchain")
                or name.startswith(".cargo/")
                or name.startswith("crates/ingestion/src/source_tests/quote_sol/fixtures/")
                or name.startswith("crates/ingestion/src/source_tests/quote_sol/legacy_buy/fixtures/")
                or name.startswith("crates/ingestion/src/source_tests/ammv4/fixtures/")
                or name.startswith("crates/ingestion/src/source_tests/target_ata/fixtures/")
                or name in {"tools/build_source_decoder_artifact.py",
                            "tools/tests/test_source_decoder_artifact.py",
                            ".github/workflows/operator-artifacts.yml"}):
            selected.append({"path": name, "sha256": sha256(root / name)})
    require(any(row["path"] == "Cargo.lock" for row in selected), "lockfile_binding_missing")
    return sorted(selected, key=lambda row: row["path"])


def select_harness(stdout):
    candidates = []
    for line in stdout.splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (row.get("reason") == "compiler-artifact"
                and row.get("target", {}).get("name") == "copybot_ingestion"
                and row.get("target", {}).get("kind") == ["lib"]
                and row.get("profile", {}).get("test") is True
                and row.get("executable")):
            candidates.append(row)
    require(len(candidates) == 1, "decoder_harness_not_unique")
    return Path(candidates[0]["executable"]), candidates[0]["package_id"]


def verify_macho(path):
    with path.open("rb") as stream:
        header = stream.read(16)
    require(len(header) == 16, "decoder_header_truncated")
    magic, cpu, _, filetype = struct.unpack("<IIII", header)
    require(magic == 0xFEEDFACF and cpu == 0x0100000C and filetype == 2,
            "decoder_not_arm64_macho_executable")


def check_result(stdout):
    counts = re.search(
        r"test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;", stdout,
    )
    require(counts is not None, "decoder_check_summary_missing")
    passed, failed, ignored = map(int, counts.groups())
    require(passed > 0 and failed == 0, "decoder_check_did_not_pass")
    return {"passed": passed, "failed": failed, "ignored": ignored}


def package(root, harness, git_sha, checks, bindings, output_root, rustc, cargo, package_id):
    artifact_id = f"source-decoder-{git_sha}"
    destination = output_root / artifact_id
    require(not destination.exists(), "decoder_artifact_already_exists")
    destination.mkdir(parents=True)
    binary = destination / BINARY
    shutil.copy2(harness, binary)
    binary.chmod(0o755)
    verify_macho(binary)
    binary_sha = sha256(binary)
    manifest = {
        "artifact_id": artifact_id, "git_sha": git_sha, "clean_tree": True,
        "package": PACKAGE, "profile": PROFILE, "target": TARGET,
        "artifact_kind": "offline-lib-test-harness", "expected_binaries": [BINARY],
        "binaries": [{"name": BINARY, "source_package": PACKAGE, "sha256": binary_sha,
                      "size_bytes": binary.stat().st_size}],
        "source_bindings": bindings, "checks": checks, "cargo_package_id": package_id,
        "rustc_version": rustc.strip(), "cargo_version": cargo.strip(),
        "ci": {"run_id": os.environ.get("GITHUB_RUN_ID"),
               "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
               "repository": os.environ.get("GITHUB_REPOSITORY")},
        "replay_entry": "source::tests::source_selection_replay::replay_saved_rpc_corpus",
        "limitations": ["Offline decoder only; no provider, daemon, signer or trading proof"],
    }
    manifest_path = destination / "build-manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    sums = destination / "SHA256SUMS"
    sums.write_text(f"{binary_sha}  {BINARY}\n{sha256(manifest_path)}  build-manifest.json\n")
    archive = output_root / f"{artifact_id}.tar.gz"
    require(not archive.exists(), "decoder_archive_already_exists")
    with tarfile.open(archive, "w:gz") as bundle:
        bundle.add(destination, arcname=artifact_id)
    archive_sha = sha256(archive)
    archive.with_suffix(archive.suffix + ".sha256").write_text(f"{archive_sha}  {archive.name}\n")
    clean_source(root, git_sha)
    require(bindings == source_bindings(root), "source_changed_during_build")
    return {"artifact": str(destination), "archive_sha256": archive_sha,
            "binary_sha256": binary_sha, "git_sha": git_sha}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expect-sha", required=True)
    parser.add_argument("--output-root", type=Path, default=Path("artifacts/macos-arm64"))
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    require(platform.system() == "Darwin" and platform.machine() == "arm64", "macos_arm64_host_required")
    clean_source(root, args.expect_sha)
    bindings = source_bindings(root)
    rustc, _ = command(["rustc", "-vV"], root)
    cargo, _ = command(["cargo", "--version"], root)
    require(f"host: {TARGET}" in rustc.splitlines(), "rust_host_target_mismatch")
    env = os.environ.copy()
    env.setdefault("CARGO_TARGET_DIR", str(root / "target/artifacts/copybot-ingestion-host"))
    cache = Path(env["CARGO_TARGET_DIR"])
    if not cache.is_absolute():
        cache = root / cache
    warm = any((cache / TARGET / "debug/deps").glob("*.rlib"))
    budget = 90 if warm else BUILD_BUDGET_SECONDS
    print(json.dumps({"phase": "HOST_DECODER_BUILD", "warm_cache": warm,
                      "build_budget_seconds": budget,
                      "disk_free_bytes": shutil.disk_usage(root).free}), flush=True)
    build = ["cargo", "test", "--locked", "-p", PACKAGE, "--lib", "--target", TARGET,
             "--no-run", "--message-format=json"]
    stdout, elapsed = command(build, root, timeout=budget, env=env)
    harness, package_id = select_harness(stdout)
    verify_macho(harness)
    checks = [{"name": "locked-lib-test-build", "command": build,
               "status": "PASS", "elapsed_seconds": round(elapsed, 3),
               "budget_seconds": budget, "warm_cache": warm}]
    for name in CHECK_FILTERS:
        argv = [str(harness), name, "--skip", "replay_private_corpus", "--skip", "replay_saved_rpc_corpus"]
        output, duration = command(argv, root, timeout=90, env=env)
        counts = check_result(output)
        checks.append({"name": name, "command": argv, "status": "PASS", **counts,
                       "elapsed_seconds": round(duration, 3)})
    clean_source(root, args.expect_sha)
    result = package(root, harness, args.expect_sha, checks, bindings,
                     root / args.output_root, rustc, cargo, package_id)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError) as error:
        print(f"source_decoder_artifact_failed:{error}", file=sys.stderr)
        sys.exit(1)
