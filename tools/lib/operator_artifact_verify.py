import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HEX40_RE = re.compile(r"^[0-9a-f]{40}$")
SAFE_PACKAGE_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
SAFE_TARGETS = {"x86_64-unknown-linux-gnu"}
EXPECTED_PACKAGE_BINARIES = {
    "copybot-app": {"copybot-app"},
    "copybot-discovery-v2": {
        "discovery_v2_prepare_quality",
        "discovery_v2_publish",
        "discovery_v2_status",
        "discovery_v2_wallet_report",
    },
    "copybot-discovery-ops": {
        "discovery_recent_raw_snapshot",
        "discovery_runtime_export",
        "discovery_v2_watchdog",
    },
    "copybot-live-ops": {
        "copybot_live_service_control_wrapper",
        "copybot_operator_emergency_stop",
    },
    "copybot-operators": {
        "copybot_execution_ata_sweep",
        "copybot_execution_canary_quote_pnl",
        "copybot_execution_canary_readiness",
        "copybot_execution_tiny_economics",
        "copybot_execution_tiny_writeoff",
        "copybot_yellowstone_source_probe",
    },
    "copybot-storage-ops": {
        "copybot_runtime_sqlite_retention_maintenance",
        "copybot_runtime_sqlite_wal_maintenance",
        "copybot_runtime_sqlite_wal_pressure_report",
    },
}


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


def expected_binaries_for_package(
    package, manifest, enforce_workspace_check, allow_installed_rollback_manifest
):
    manifest_expected = manifest.get("expected_binaries")
    if not manifest_expected:
        raise ValueError("artifact manifest missing expected_binaries")
    manifest_expected = safe_binary_set(manifest_expected, "artifact manifest")
    if allow_installed_rollback_manifest:
        return manifest_expected
    static_expected = EXPECTED_PACKAGE_BINARIES.get(package)
    if static_expected is None:
        raise ValueError(f"unknown expected package: {package}")
    if manifest_expected != static_expected:
        raise ValueError(
            "artifact manifest expected_binaries does not match verifier package contract: "
            f"manifest={sorted(manifest_expected)} contract={sorted(static_expected)}"
        )
    if not enforce_workspace_check:
        return static_expected
    workspace_expected = expected_binaries_from_workspace(package)
    if static_expected != workspace_expected:
        raise ValueError(
            "verifier package contract does not match workspace package metadata: "
            f"contract={sorted(static_expected)} workspace={sorted(workspace_expected)}"
        )
    return static_expected


def workspace_integration_tests(package):
    raw = subprocess.check_output(
        ["cargo", "metadata", "--locked", "--format-version=1", "--no-deps"],
        cwd=ROOT,
        text=True,
    )
    metadata = json.loads(raw)
    for workspace_package in metadata.get("packages", []):
        if workspace_package.get("name") != package:
            continue
        return {
            target.get("name")
            for target in workspace_package.get("targets", [])
            if "test" in target.get("kind", [])
        }
    raise ValueError(f"unknown workspace package: {package}")


def read_sums(path):
    sums = {}
    for line in path.read_text().splitlines():
        parts = line.split(None, 1)
        if len(parts) != 2:
            raise ValueError(f"invalid checksum line: {line!r}")
        name = parts[1].lstrip("*")
        if "/" in name or name.startswith(".") or ".." in name:
            raise ValueError(f"unsafe checksum artifact name: {name}")
        if name in sums:
            raise ValueError(f"duplicate checksum artifact name: {name}")
        sums[name] = parts[0].lower()
    return sums


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def reject_linked_artifact_entry(path, label):
    if path.is_symlink():
        raise ValueError(f"{label} is a symlink: {path.name}")
    try:
        stat_result = path.stat()
    except OSError as exc:
        raise ValueError(f"failed to stat {label}: {path.name}: {exc}") from exc
    if path.is_dir():
        return
    if stat_result.st_nlink != 1:
        raise ValueError(f"{label} is a hardlink or multiply-linked file: {path.name}")
