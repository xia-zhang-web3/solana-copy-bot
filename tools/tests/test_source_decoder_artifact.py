#!/usr/bin/env python3
"""Local checks of artifact selection, identity, rejection and bounded processes."""

import importlib.util
import json
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location(
    "builder", Path(__file__).resolve().parents[1] / "build_source_decoder_artifact.py",
)
builder = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(builder)


class DecoderArtifactTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def repository(self):
        def git(*args):
            return subprocess.check_output(["git", *args], cwd=self.root, text=True).strip()
        git("init", "-q")
        git("config", "user.email", "fixture@example.invalid")
        git("config", "user.name", "Artifact fixture")
        (self.root / ".gitignore").write_text("/artifacts\n/harness\n")
        (self.root / "Cargo.lock").write_text("locked fixture\n")
        (self.root / "Cargo.toml").write_text("manifest fixture\n")
        (self.root / "decoder.rs").write_text("pub fn accepted() {}\n")
        git("add", ".")
        git("commit", "-qm", "fixture")
        return git("rev-parse", "HEAD")

    def harness_row(self):
        return {"reason": "compiler-artifact", "target": {"name": "copybot_ingestion", "kind": ["lib"]},
                "profile": {"test": True}, "executable": "/fixture/harness",
                "package_id": "path+file:///fixture/ingestion#copybot-ingestion@0.1.0"}

    def test_exact_test_harness_selection(self):
        path, package = builder.select_harness("non-json line\n" + json.dumps(self.harness_row()))
        self.assertEqual(str(path), "/fixture/harness")
        self.assertIn("copybot-ingestion", package)

    def test_duplicate_and_non_test_harness_rejected(self):
        row = self.harness_row()
        for data in [json.dumps(row) + "\n" + json.dumps(row), json.dumps({**row, "profile": {"test": False}})]:
            with self.assertRaisesRegex(ValueError, "decoder_harness_not_unique"):
                builder.select_harness(data)

    def test_macho_architecture_and_file_type(self):
        path = self.root / "header"
        path.write_bytes(struct.pack("<IIII", 0xFEEDFACF, 0x0100000C, 0, 2))
        builder.verify_macho(path)
        for cpu, kind in [(0x01000007, 2), (0x0100000C, 6)]:
            path.write_bytes(struct.pack("<IIII", 0xFEEDFACF, cpu, 0, kind))
            with self.assertRaisesRegex(ValueError, "decoder_not_arm64"):
                builder.verify_macho(path)
        path.write_bytes(b"bad")
        with self.assertRaisesRegex(ValueError, "decoder_header_truncated"):
            builder.verify_macho(path)

    def test_clean_commit_and_wrong_sha_or_dirty_source(self):
        sha = self.repository()
        builder.clean_source(self.root, sha)
        with self.assertRaisesRegex(ValueError, "unexpected_source_sha"):
            builder.clean_source(self.root, "0" * 40)
        (self.root / "decoder.rs").write_text("changed\n")
        with self.assertRaisesRegex(ValueError, "dirty_source_tree"):
            builder.clean_source(self.root, sha)

    def test_bindings_include_locked_and_rust_sources(self):
        self.repository()
        bindings = builder.source_bindings(self.root)
        self.assertEqual([row["path"] for row in bindings], ["Cargo.lock", "Cargo.toml", "decoder.rs"])
        self.assertTrue(all(len(row["sha256"]) == 64 for row in bindings))

    def test_ammv4_fixture_bindings_and_executable_check_filter(self):
        fixture = "crates/ingestion/src/source_tests/ammv4/fixtures/wallet-06-01.json"
        bindings = "crates/ingestion/src/source_tests/ammv4/fixtures/BINDINGS.json"
        for name in (fixture, bindings):
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('{"accepted_raw":"7848635"}\n')
        self.repository()
        rows = {row["path"]: row["sha256"] for row in builder.source_bindings(self.root)}
        for name in (fixture, bindings):
            self.assertEqual(rows[name], builder.sha256(self.root / name))
        self.assertIn("source::tests::ammv4_tests::", builder.CHECK_FILTERS)

    def test_target_ata_inputs_and_bindings_are_manifest_bound(self):
        folder = "crates/ingestion/src/source_tests/target_ata/fixtures/"
        for name in ("wallet-08-03.json", "BINDINGS.json"):
            path = self.root / folder / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('{"executed_sol_raw":"5259299"}\n')
        self.repository()
        rows = {row["path"]: row["sha256"] for row in builder.source_bindings(self.root)}
        for name in ("wallet-08-03.json", "BINDINGS.json"):
            self.assertEqual(rows[folder + name], builder.sha256(self.root / folder / name))

    def test_legacy_buy_fixtures_are_bound_and_share_existing_filter(self):
        folder = "crates/ingestion/src/source_tests/quote_sol/legacy_buy/fixtures/"
        for name in ("wallet-05-03.json", "BINDINGS.json"):
            path = self.root / folder / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('{"executed_base_raw":"19607189"}\n')
        self.repository()
        rows = {row["path"]: row["sha256"] for row in builder.source_bindings(self.root)}
        for name in ("wallet-05-03.json", "BINDINGS.json"):
            self.assertEqual(rows[folder + name], builder.sha256(self.root / folder / name))
        self.assertEqual(builder.CHECK_FILTERS.count("source::tests::quote_sol_tests::"), 1)
        self.assertFalse(any("legacy_buy" in item for item in builder.CHECK_FILTERS))

    def test_float_roundtrip_checks_are_packaged_once(self):
        self.assertEqual(builder.CHECK_FILTERS.count(
            "source::http_recovery::tests::float_roundtrip_tests::"), 1)

    def test_zero_tests_or_failed_summary_rejected(self):
        counts = builder.check_result("test result: ok. 17 passed; 0 failed; 0 ignored;")
        self.assertEqual(counts["passed"], 17)
        for output in ["test result: ok. 0 passed; 0 failed; 0 ignored;", "test result: FAILED. 0 passed; 1 failed; 0 ignored;"]:
            with self.assertRaises(ValueError):
                builder.check_result(output)

    def test_command_timeout_is_an_error(self):
        with self.assertRaisesRegex(ValueError, "command_budget_exceeded"):
            builder.command([sys.executable, "-c", "import time; time.sleep(20)"], self.root, timeout=0.03)

    def test_package_seals_binary_manifest_archive_and_sources(self):
        sha = self.repository()
        binary = self.root / "harness"
        binary.write_bytes(struct.pack("<IIII", 0xFEEDFACF, 0x0100000C, 0, 2))
        checks = [{"name": "fixture", "status": "PASS"}]
        result = builder.package(self.root, binary, sha, checks, builder.source_bindings(self.root),
                                 self.root / "artifacts", "host fixture", "cargo fixture", "package fixture")
        path = Path(result["artifact"])
        manifest = json.loads((path / "build-manifest.json").read_text())
        self.assertEqual((manifest["git_sha"], manifest["target"], manifest["profile"]),
                         (sha, "aarch64-apple-darwin", "test"))
        self.assertEqual(manifest["binaries"][0]["sha256"], builder.sha256(path / builder.BINARY))
        for line in (path / "SHA256SUMS").read_text().splitlines():
            digest, name = line.split("  ")
            self.assertEqual(digest, builder.sha256(path / name))
        archive = self.root / "artifacts" / f"source-decoder-{sha}.tar.gz"
        self.assertEqual(result["archive_sha256"], builder.sha256(archive))
        self.assertIn(builder.sha256(archive), archive.with_suffix(".gz.sha256").read_text())


if __name__ == "__main__":
    unittest.main()
