#!/usr/bin/env python3
import hashlib
import io
import json
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


class OperatorArtifactVerifierRollbackTests(unittest.TestCase):
    def write_artifact(
        self,
        artifact_dir: Path,
        package: str,
        binaries: list[str],
        *,
        profile: str,
        checks=None,
    ) -> None:
        artifact_dir.mkdir()
        sums = {}
        for name in binaries:
            path = artifact_dir / name
            path.write_text(f"#!/usr/bin/env bash\necho {name}\n", encoding="utf-8")
            path.chmod(0o755)
            sums[name] = hashlib.sha256(path.read_bytes()).hexdigest()

        manifest = {
            "schema_version": 1,
            "git_sha": "a" * 40,
            "artifact_id": artifact_dir.name,
            "package": package,
            "profile": profile,
            "target": "x86_64-unknown-linux-gnu",
            "git_dirty": False,
            "checks": checks or [],
            "expected_binaries": binaries,
            "migration_bundle": None,
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

    def app_artifact_checks(self) -> list[str]:
        return [
            "tools/architecture_guard.sh --changed",
            "tools/architecture_guard.sh --all",
            "cargo test --locked -p copybot-app --bin copybot-app -- --test-threads=1",
        ]

    def test_verifier_rejects_migration_bundle_path_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / ("copybot-app-" + "a" * 40)
            self.write_artifact(
                artifact_dir,
                "copybot-app",
                ["copybot-app"],
                profile="release",
                checks=self.app_artifact_checks(),
            )
            bundle_path = artifact_dir / "migrations.tar.gz"
            with tarfile.open(bundle_path, "w:gz") as archive:
                payload = b"SELECT 1;\n"
                info = tarfile.TarInfo("migrations/./0001_init.sql")
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
            )

            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertIn("unsafe migration bundle member", result.stderr)

    def test_rollback_verifier_rejects_tampered_extracted_migrations(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            release_root = Path(tmp) / "releases"
            release_root.mkdir()
            artifact_id = "copybot-app-" + "a" * 40
            artifact_dir = release_root / artifact_id
            self.write_artifact(
                artifact_dir,
                "copybot-app",
                ["copybot-app"],
                profile="release",
            )
            bundle_path = artifact_dir / "migrations.tar.gz"
            with tarfile.open(bundle_path, "w:gz") as archive:
                payload = b"SELECT 1;\n"
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
            migrations_dir = artifact_dir / "migrations"
            migrations_dir.mkdir()
            (migrations_dir / "0001_init.sql").write_text("SELECT 2;\n", encoding="utf-8")
            (artifact_dir / "INSTALL_COMPLETE").write_text(artifact_id + "\n", encoding="utf-8")

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
                    "--skip-workspace-bin-check",
                    "--installed-release-root",
                    str(release_root),
                ],
                cwd=REPO_ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertIn("content does not match migrations.tar.gz", result.stderr)

    def test_verifier_rejects_workspace_migration_content_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / ("copybot-app-" + "a" * 40)
            self.write_artifact(
                artifact_dir,
                "copybot-app",
                ["copybot-app"],
                profile="release",
                checks=self.app_artifact_checks(),
            )
            workspace_files = sorted(
                f"migrations/{path.name}"
                for path in (REPO_ROOT / "migrations").glob("*.sql")
            )
            bundle_path = artifact_dir / "migrations.tar.gz"
            with tarfile.open(bundle_path, "w:gz") as archive:
                payload = b"-- tampered migration payload\nSELECT 1;\n"
                for name in workspace_files:
                    info = tarfile.TarInfo(name)
                    info.size = len(payload)
                    archive.addfile(info, io.BytesIO(payload))
            bundle_sha = hashlib.sha256(bundle_path.read_bytes()).hexdigest()
            manifest_path = artifact_dir / "build-manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["migration_bundle"] = {
                "name": "migrations.tar.gz",
                "sha256": bundle_sha,
                "files": workspace_files,
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
                    "--enforce-workspace-bin-check",
                ],
                cwd=REPO_ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertIn("SQL content does not match workspace migrations", result.stderr)


if __name__ == "__main__":
    unittest.main()
