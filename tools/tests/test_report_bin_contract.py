"""Default artifacts exclude retired reports without breaking historical rollback."""
import sys
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools"))

from package_bins import load_metadata, package_bins
from lib.operator_artifact_verify import (
    EXPECTED_PACKAGE_BINARIES,
    expected_binaries_for_package,
)
import test_operator_artifact_verifier_rollback as artifact_fixture

PACKAGE = "copybot-operators"
LEGACY = {
    "copybot_entry_side_filter_backtest",
    "copybot_exit_policy_shadow_quote_report",
    "copybot_exit_policy_sim",
}
ACTIVE = {
    "copybot_execution_ata_sweep",
    "copybot_execution_canary_quote_pnl",
    "copybot_execution_canary_readiness",
    "copybot_execution_tiny_economics",
    "copybot_execution_tiny_writeoff",
    "copybot_first_sell_full_exit_report",
    "copybot_leader_copyability_report",
    "copybot_track_b_entry_quote_report",
    "copybot_universe_de_risk_report",
    "copybot_yellowstone_source_probe",
}


class ReportBinContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.metadata = load_metadata()

    def test_default_operator_artifact_has_only_active_bins(self):
        self.assertEqual(set(package_bins(self.metadata, PACKAGE)), ACTIVE)

    def test_retired_targets_require_explicit_legacy_feature(self):
        package = next(p for p in self.metadata["packages"] if p["name"] == PACKAGE)
        targets = {t["name"]: t for t in package["targets"] if "bin" in t["kind"]}
        self.assertEqual(set(targets), ACTIVE | LEGACY)
        for name in LEGACY:
            self.assertEqual(targets[name].get("required-features"), ["legacy-reports"])
            self.assertIn("/src/legacy-bin/", targets[name]["src_path"])

    def test_default_features_expand_transitively_and_handle_cycles(self):
        metadata = {"packages": [{
            "name": "fixture",
            "features": {"default": ["a"], "a": ["b"], "b": ["a"]},
            "targets": [
                {"name": "base", "kind": ["bin"]},
                {"name": "enabled", "kind": ["bin"], "required-features": ["a", "b"]},
                {"name": "legacy", "kind": ["bin"], "required-features": ["legacy"]},
                {"name": "mixed", "kind": ["bin"], "required-features": ["a", "legacy"]},
                {"name": "not-a-bin", "kind": ["lib"]},
            ],
        }]}
        self.assertEqual(package_bins(metadata, "fixture"), ["base", "enabled"])

    def test_all_default_package_sets_match_verifier_contract(self):
        for package, expected in EXPECTED_PACKAGE_BINARIES.items():
            with self.subTest(package=package):
                self.assertEqual(set(package_bins(self.metadata, package)), expected)

    def test_current_artifact_rejects_retired_or_missing_binary(self):
        for names in (ACTIVE | LEGACY, ACTIVE - {"copybot_execution_canary_quote_pnl"}):
            with self.subTest(names=names), self.assertRaises(ValueError):
                expected_binaries_for_package(
                    PACKAGE, {"expected_binaries": sorted(names)}, False, False
                )

    def test_current_artifact_accepts_exact_default_set(self):
        self.assertEqual(
            expected_binaries_for_package(
                PACKAGE, {"expected_binaries": sorted(ACTIVE)}, True, False
            ),
            ACTIVE,
        )

    def test_installed_historical_rollback_preserves_old_bin_set(self):
        self.assertEqual(
            expected_binaries_for_package(
                PACKAGE, {"expected_binaries": sorted(ACTIVE | LEGACY)}, False, True
            ),
            ACTIVE | LEGACY,
        )

    def write_artifact(self, directory, binaries):
        artifact_fixture.OperatorArtifactVerifierRollbackTests().write_artifact(
            directory, PACKAGE, sorted(binaries), profile="operator-release",
            checks=[
                "tools/architecture_guard.sh --changed",
                "tools/architecture_guard.sh --all",
                f"cargo test --locked -p {PACKAGE} --lib -- --test-threads=1",
                f"cargo test --locked -p {PACKAGE} --tests -- --test-threads=1",
            ],
        )

    def verify_artifact(self, directory, *extra):
        return subprocess.run(
            [sys.executable, str(ROOT / "tools/verify_operator_artifact.py"),
             str(directory), "--expect-package", PACKAGE, *extra],
            cwd=ROOT, text=True, capture_output=True, timeout=30,
        )

    def test_default_artifact_cli_accepts_bytes_and_rejects_tampering(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp) / (PACKAGE + "-" + "a" * 40)
            self.write_artifact(directory, ACTIVE)
            result = self.verify_artifact(directory, "--enforce-workspace-bin-check")
            self.assertEqual(result.returncode, 0, result.stderr)
            (directory / "copybot_execution_canary_quote_pnl").write_text("tampered")
            result = self.verify_artifact(directory, "--enforce-workspace-bin-check")
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("checksum", result.stderr.lower())

    def test_legacy_artifact_cli_only_allowed_as_completed_installed_rollback(self):
        with tempfile.TemporaryDirectory() as temp:
            releases = Path(temp) / "releases"
            releases.mkdir()
            directory = releases / (PACKAGE + "-" + "a" * 40)
            self.write_artifact(directory, ACTIVE | LEGACY)
            result = self.verify_artifact(directory)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("package contract", result.stderr)
            rollback_args = ("--skip-workspace-bin-check", "--installed-release-root", str(releases))
            result = self.verify_artifact(directory, *rollback_args)
            self.assertNotEqual(result.returncode, 0)
            (directory / "INSTALL_COMPLETE").write_text(directory.name + "\n")
            result = self.verify_artifact(directory, *rollback_args)
            self.assertEqual(result.returncode, 0, result.stderr)
            (directory / "copybot_exit_policy_sim").write_text("tampered")
            result = self.verify_artifact(directory, *rollback_args)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("checksum", result.stderr.lower())


if __name__ == "__main__":
    unittest.main()
