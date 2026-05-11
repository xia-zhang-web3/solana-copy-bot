#!/usr/bin/env python3
import json
import hashlib
import io
import os
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
TOOLS_LIB = REPO_ROOT / "tools" / "lib"
SUBPROCESS_TIMEOUT_SECONDS = 30
sys.path.insert(0, str(TOOLS_LIB))

from ingestion_ab_report_metrics import evaluate_telemetry, load_db_metrics  # noqa: E402


def iso(minutes_ago: int = 0) -> str:
    value = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    return value.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


class IngestionAbReportMetricsTests(unittest.TestCase):
    def test_load_db_metrics_counts_windowed_capture(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "runtime.db"
            con = sqlite3.connect(db_path)
            con.executescript(
                """
                CREATE TABLE observed_swaps(
                    ts TEXT NOT NULL,
                    token_in TEXT NOT NULL,
                    token_out TEXT NOT NULL
                );
                CREATE TABLE copy_signals(
                    ts TEXT NOT NULL,
                    side TEXT NOT NULL,
                    status TEXT NOT NULL
                );
                CREATE TABLE risk_events(
                    ts TEXT NOT NULL,
                    type TEXT NOT NULL
                );
                """
            )
            sol = "So11111111111111111111111111111111111111112"
            con.executemany(
                "INSERT INTO observed_swaps(ts, token_in, token_out) VALUES (?, ?, ?)",
                [
                    (iso(1), sol, "TOKEN_A"),
                    (iso(2), "TOKEN_B", sol),
                    (iso(600), sol, "OLD_TOKEN"),
                ],
            )
            con.executemany(
                "INSERT INTO copy_signals(ts, side, status) VALUES (?, ?, ?)",
                [
                    (iso(1), "buy", "accepted"),
                    (iso(2), "sell", "rejected"),
                    (iso(600), "buy", "old"),
                ],
            )
            con.execute("INSERT INTO risk_events(ts, type) VALUES (?, ?)", (iso(1), "cap"))
            con.commit()
            con.close()

            metrics = load_db_metrics(str(db_path), 60)
            self.assertEqual(metrics["observed_total"], 2)
            self.assertEqual(metrics["observed_buy"], 1)
            self.assertEqual(metrics["observed_sell"], 1)
            self.assertEqual(metrics["signals_buy"], 1)
            self.assertEqual(metrics["signals_sell"], 1)
            self.assertEqual(metrics["risk_event_counts"], {"cap": 1})

    def test_evaluate_telemetry_parses_ingestion_and_sqlite_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "journal.log"
            rows = [
                (
                    "2026-05-05T10:00:00Z ingestion pipeline metrics "
                    + json.dumps(
                        {
                            "ingestion_lag_ms_p95": 100,
                            "ingestion_lag_ms_p99": 200,
                            "ws_notifications_enqueued": 1000,
                            "ws_notifications_replaced_oldest": 0,
                            "reconnect_count": 0,
                            "parse_rejected_total": 0,
                            "grpc_decode_errors": 0,
                            "grpc_message_total": 1000,
                        }
                    )
                ),
                (
                    "2026-05-05T10:01:00Z ingestion pipeline metrics "
                    + json.dumps(
                        {
                            "ingestion_lag_ms_p95": 150,
                            "ingestion_lag_ms_p99": 250,
                            "ws_notifications_enqueued": 1800,
                            "ws_notifications_replaced_oldest": 5,
                            "reconnect_count": 0,
                            "parse_rejected_total": 0,
                            "grpc_decode_errors": 0,
                            "grpc_message_total": 1800,
                        }
                    )
                ),
                (
                    "2026-05-05T10:00:00Z sqlite contention counters "
                    + json.dumps({"sqlite_write_retry_total": 1, "sqlite_busy_error_total": 2})
                ),
                (
                    "2026-05-05T10:01:00Z sqlite contention counters "
                    + json.dumps({"sqlite_write_retry_total": 3, "sqlite_busy_error_total": 5})
                ),
            ]
            log_path.write_text("\n".join(rows) + "\n", encoding="utf-8")

            telemetry = evaluate_telemetry(str(log_path))
            self.assertTrue(telemetry["available"])
            self.assertEqual(telemetry["sample_count"], 2)
            self.assertEqual(telemetry["lag_p95_pass_ratio"], 1.0)
            self.assertEqual(telemetry["sqlite_write_retry_delta"], 2.0)
            self.assertEqual(telemetry["sqlite_busy_error_delta"], 3.0)


class IngestionFailoverWatchdogTests(unittest.TestCase):
    def test_runtime_snapshot_failure_is_fail_closed_and_read_only(self) -> None:
        if shutil.which("jq") is None:
            self.skipTest("jq is required by ingestion_failover_watchdog.sh")

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            policy = tmp_path / "policy.toml"
            policy.write_text("lag_p95_threshold_ms = 1\n", encoding="utf-8")
            snapshot = tmp_path / "runtime_snapshot.sh"
            snapshot.write_text(
                "#!/usr/bin/env bash\necho snapshot unavailable >&2\nexit 7\n",
                encoding="utf-8",
            )
            snapshot.chmod(0o755)

            state_dir = tmp_path / "state"
            env = os.environ.copy()
            env.update(
                {
                    "POLICY_FILE": str(policy),
                    "STATE_FILE": str(state_dir / "state.json"),
                    "COOLDOWN_FILE": str(state_dir / "cooldown.json"),
                    "OVERRIDE_FILE": str(state_dir / "override.env"),
                    "CONFIG_PATH": str(tmp_path / "missing.toml"),
                    "RUNTIME_SNAPSHOT_SCRIPT": str(snapshot),
                    "DRY_RUN": "true",
                }
            )

            result = subprocess.run(
                [str(REPO_ROOT / "tools" / "ingestion_failover_watchdog.sh")],
                cwd=REPO_ROOT,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                timeout=SUBPROCESS_TIMEOUT_SECONDS,
            )

            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertIn("refusing fail-open skip", result.stderr)
            self.assertFalse(state_dir.exists())

    def test_dry_run_uses_temp_state_and_writes_no_state_override_or_cooldown(self) -> None:
        if shutil.which("jq") is None:
            self.skipTest("jq is required by ingestion_failover_watchdog.sh")

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            policy = tmp_path / "policy.toml"
            policy.write_text(
                "\n".join(
                    [
                        "lag_p95_threshold_ms = 1",
                        "lag_breach_consecutive = 1",
                        "cooldown_minutes = 15",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            snapshot = tmp_path / "runtime_snapshot.sh"
            snapshot.write_text(
                "\n".join(
                    [
                        "#!/usr/bin/env bash",
                        "echo 'ingestion_lag_ms_p95: 1000'",
                        "echo 'replaced_ratio_last_interval: 0'",
                        "echo 'reconnect_count: 0'",
                        "echo 'parse_rejected_total: 0'",
                        "echo 'grpc_decode_errors: 0'",
                        "echo 'grpc_message_total: 1000'",
                        "echo 'ws_notifications_enqueued: 1000'",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            snapshot.chmod(0o755)

            state_dir = tmp_path / "state"
            env = os.environ.copy()
            env.update(
                {
                    "POLICY_FILE": str(policy),
                    "STATE_FILE": str(state_dir / "state.json"),
                    "COOLDOWN_FILE": str(state_dir / "cooldown.json"),
                    "OVERRIDE_FILE": str(state_dir / "override.env"),
                    "CONFIG_PATH": str(tmp_path / "unused.toml"),
                    "RUNTIME_SNAPSHOT_SCRIPT": str(snapshot),
                    "DRY_RUN": "true",
                }
            )

            result = subprocess.run(
                [str(REPO_ROOT / "tools" / "ingestion_failover_watchdog.sh")],
                cwd=REPO_ROOT,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                timeout=SUBPROCESS_TIMEOUT_SECONDS,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("dry-run trigger reason=lag_p95_above_threshold", result.stdout)
            self.assertFalse(state_dir.exists())


class OperatorArtifactVerifierTests(unittest.TestCase):
    def write_artifact(
        self,
        artifact_dir: Path,
        package: str,
        binaries: list[str],
        *,
        profile: str,
        git_dirty: bool = False,
        migration_bundle: bool = False,
        checks=None,
    ) -> None:
        artifact_dir.mkdir()
        sums = {}
        for name in binaries:
            path = artifact_dir / name
            path.write_text(f"#!/usr/bin/env bash\necho {name}\n", encoding="utf-8")
            path.chmod(0o755)
            sums[name] = hashlib.sha256(path.read_bytes()).hexdigest()
        bundle = None
        if migration_bundle:
            bundle_path = artifact_dir / "migrations.tar.gz"
            bundle_path.write_bytes(b"fake migration bundle")
            sums[bundle_path.name] = hashlib.sha256(bundle_path.read_bytes()).hexdigest()
            bundle = {"name": bundle_path.name, "sha256": sums[bundle_path.name]}

        git_sha = "a" * 40
        manifest = {
            "schema_version": 1,
            "git_sha": git_sha,
            "artifact_id": artifact_dir.name,
            "package": package,
            "profile": profile,
            "target": "x86_64-unknown-linux-gnu",
            "git_dirty": git_dirty,
            "checks": checks or [],
            "expected_binaries": binaries,
            "migration_bundle": bundle,
            "binaries": [
                {"name": name, "source_package": package, "sha256": sums[name]}
                for name in binaries
            ],
        }
        (artifact_dir / "build-manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n",
            encoding="utf-8",
        )
        (artifact_dir / "SHA256SUMS").write_text(
            "".join(f"{digest}  {name}\n" for name, digest in sums.items()),
            encoding="utf-8",
        )

    def test_verifier_rejects_dirty_artifact_without_required_checks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / ("copybot-discovery-v2-" + "a" * 40 + "-dirty")
            binaries = [
                "discovery_v2_prepare_quality",
                "discovery_v2_publish",
                "discovery_v2_status",
            ]
            self.write_artifact(
                artifact_dir,
                "copybot-discovery-v2",
                binaries,
                profile="operator-release",
                git_dirty=True,
            )

            result = subprocess.run(
                [
                    sys.executable,
                    str(REPO_ROOT / "tools" / "verify_operator_artifact.py"),
                    str(artifact_dir),
                    "--expect-package",
                    "copybot-discovery-v2",
                    "--expect-profile",
                    "operator-release",
                    "--expect-target",
                    "x86_64-unknown-linux-gnu",
                    "--allow-dirty",
                ],
                cwd=REPO_ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                timeout=SUBPROCESS_TIMEOUT_SECONDS,
            )

            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertIn("missing required checks", result.stderr)

    def test_verifier_rejects_dirty_suffix_when_manifest_is_clean(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / ("copybot-discovery-v2-" + "a" * 40 + "-dirty")
            self.write_artifact(
                artifact_dir,
                "copybot-discovery-v2",
                [
                    "discovery_v2_prepare_quality",
                    "discovery_v2_publish",
                    "discovery_v2_status",
                ],
                profile="operator-release",
                git_dirty=False,
            )

            result = subprocess.run(
                [
                    sys.executable,
                    str(REPO_ROOT / "tools" / "verify_operator_artifact.py"),
                    str(artifact_dir),
                    "--expect-package",
                    "copybot-discovery-v2",
                    "--expect-profile",
                    "operator-release",
                    "--expect-target",
                    "x86_64-unknown-linux-gnu",
                    "--allow-dirty",
                ],
                cwd=REPO_ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                timeout=SUBPROCESS_TIMEOUT_SECONDS,
            )

            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertIn("artifact_id does not match", result.stderr)

    def test_verifier_requires_copybot_app_migration_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / ("copybot-app-" + "a" * 40)
            checks = [
                "tools/architecture_guard.sh --changed",
                "tools/architecture_guard.sh --all",
                "cargo test --locked -p copybot-app --bin copybot-app -- --test-threads=1",
            ]
            self.write_artifact(
                artifact_dir,
                "copybot-app",
                ["copybot-app"],
                profile="release",
                checks=checks,
            )

            result = subprocess.run(
                [
                    sys.executable,
                    str(REPO_ROOT / "tools" / "verify_operator_artifact.py"),
                    str(artifact_dir),
                    "--expect-package",
                    "copybot-app",
                    "--expect-profile",
                    "release",
                    "--expect-target",
                    "x86_64-unknown-linux-gnu",
                ],
                cwd=REPO_ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                timeout=SUBPROCESS_TIMEOUT_SECONDS,
            )

            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertIn("missing required migrations bundle", result.stderr)

    def test_verifier_rejects_duplicate_migration_bundle_members(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / ("copybot-app-" + "a" * 40)
            checks = [
                "tools/architecture_guard.sh --changed",
                "tools/architecture_guard.sh --all",
                "cargo test --locked -p copybot-app --bin copybot-app -- --test-threads=1",
            ]
            self.write_artifact(
                artifact_dir,
                "copybot-app",
                ["copybot-app"],
                profile="release",
                checks=checks,
            )
            bundle_path = artifact_dir / "migrations.tar.gz"
            with tarfile.open(bundle_path, "w:gz") as archive:
                payload = b"SELECT 1;\n"
                for _ in range(2):
                    info = tarfile.TarInfo("migrations/0001_init.sql")
                    info.size = len(payload)
                    archive.addfile(info, io.BytesIO(payload))
            bundle_sha = hashlib.sha256(bundle_path.read_bytes()).hexdigest()
            manifest_path = artifact_dir / "build-manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["migration_bundle"] = {
                "name": "migrations.tar.gz",
                "sha256": bundle_sha,
                "files": ["migrations/0001_init.sql"],
            }
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
            sums_path = artifact_dir / "SHA256SUMS"
            sums_path.write_text(
                sums_path.read_text(encoding="utf-8")
                + f"{bundle_sha}  migrations.tar.gz\n",
                encoding="utf-8",
            )

            result = subprocess.run(
                [
                    sys.executable,
                    str(REPO_ROOT / "tools" / "verify_operator_artifact.py"),
                    str(artifact_dir),
                    "--expect-package",
                    "copybot-app",
                    "--expect-profile",
                    "release",
                    "--expect-target",
                    "x86_64-unknown-linux-gnu",
                ],
                cwd=REPO_ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                timeout=SUBPROCESS_TIMEOUT_SECONDS,
            )

            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertIn("duplicate migration bundle member", result.stderr)

if __name__ == "__main__":
    unittest.main()
