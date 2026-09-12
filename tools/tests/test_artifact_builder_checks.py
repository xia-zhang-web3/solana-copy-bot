"""Causal tests of the existing builder; Cargo/guards are explicitly synthetic."""
import hashlib
import json
import shlex
import tarfile
import unittest

from artifact_builder_fixture import BINARIES, BuilderFixture, DASHBOARD, LIB_PACKAGE


class ArtifactBuilderChecks(unittest.TestCase):
    def fixture(self, package=DASHBOARD, **settings):
        fixture = BuilderFixture(package, **settings)
        self.addCleanup(fixture.close)
        return fixture

    def commands(self, fixture):
        return [e['argv'] for e in fixture.events()
                if e['tool'] == 'cargo' and e['argv'][0] == 'test']

    def assert_checked(self, fixture, selectors):
        expected = [['test', '--locked', '-p', fixture.package, *selector,
                     '--', '--test-threads=1'] for selector in selectors]
        self.assertEqual(self.commands(fixture), expected,
                         'builder must actually execute every required test selector before build')
        guards = [e['argv'] for e in fixture.events() if e['tool'] == 'guard']
        self.assertEqual(guards, [['--changed'], ['--all']])
        relevant = [e for e in fixture.events() if e['tool'] == 'guard' or
                    (e['tool'] == 'cargo' and e['argv'][0] in ('test', 'build'))]
        self.assertEqual([e['tool'] if e['tool'] == 'guard' else e['argv'][0] for e in relevant],
                         ['guard', 'guard', *(['test'] * len(selectors)), 'build'])
        checks = ['tools/architecture_guard.sh --changed', 'tools/architecture_guard.sh --all',
                  '; '.join(shlex.join(['cargo', *args]) for args in expected)]
        self.assertEqual(fixture.manifest()['checks'], checks)

    def assert_package(self, fixture):
        manifest = fixture.manifest()
        self.assertFalse(manifest['git_dirty'])
        self.assertEqual(manifest['git_sha'], fixture.sha)
        self.assertEqual(manifest['profile'], fixture.profile)
        self.assertEqual(manifest['target'], 'x86_64-unknown-linux-gnu')
        self.assertEqual(manifest['expected_binaries'], BINARIES[fixture.package])
        archive = fixture.output.with_suffix('.tar.gz')
        self.assertTrue(archive.is_file())
        self.assertEqual(archive.with_suffix('.gz.sha256').read_text().split()[0],
                         hashlib.sha256(archive.read_bytes()).hexdigest())
        with tarfile.open(archive) as bundle:
            self.assertEqual(json.load(bundle.extractfile(f'{fixture.output.name}/build-manifest.json')), manifest)
        for name in fixture.bins:
            self.assertEqual((fixture.output / name).read_bytes()[:4], b'\x7fELF')

    def test_dashboard_runs_integrations_and_truthful_manifest_passes_verifier(self):
        f = self.fixture()
        result = f.build()
        self.assertEqual(result.returncode, 0, result.stderr)
        verification = f.verify()  # Preserve the old verifier's causal refusal in RED evidence too.
        self.assert_checked(f, [['--bins'], ['--tests']])
        self.assert_package(f)
        self.assertEqual(verification.returncode, 0, verification.stderr)

    def test_integration_failure_stops_before_build_manifest_or_archive(self):
        f = self.fixture(integration_exit=37)
        result = f.build()
        self.assertEqual(result.returncode, 37, result.stderr)
        self.assertEqual(self.commands(f), [
            ['test', '--locked', '-p', DASHBOARD, '--bins', '--', '--test-threads=1'],
            ['test', '--locked', '-p', DASHBOARD, '--tests', '--', '--test-threads=1']])
        self.assertFalse(any(e['tool'] == 'cargo' and e['argv'][0] == 'build' for e in f.events()))
        self.assertFalse(f.target.exists())
        self.assertFalse(f.artifacts.exists())

    def test_lib_control_preserves_order_arguments_checks_and_packaging(self):
        f = self.fixture(LIB_PACKAGE)
        result = f.build()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assert_checked(f, [['--lib'], ['--tests']])
        self.assert_package(f)
        result = f.verify()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_app_control_preserves_single_bin_tests_and_migration_bundle(self):
        f = self.fixture('copybot-app')
        result = f.build()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assert_checked(f, [['--bin', 'copybot-app']])
        self.assert_package(f)
        self.assertEqual(f.manifest()['migration_bundle']['files'], ['migrations/0001_fixture.sql'])
        result = f.verify()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_unchecked_smoke_does_not_execute_or_claim_guards_and_tests(self):
        f = self.fixture()
        result = f.build(checks=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(self.commands(f))
        self.assertFalse(any(e['tool'] == 'guard' for e in f.events()))
        self.assertEqual(f.manifest()['checks'], [])
        self.assert_package(f)
        result = f.verify()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('missing required checks', result.stderr)

    def test_fake_build_failure_does_not_produce_package(self):
        f = self.fixture(build_exit=71)
        result = f.build()
        self.assertEqual(result.returncode, 71, result.stderr)
        self.assertFalse(f.artifacts.exists())
        self.assertFalse(f.target.exists())

    def test_unknown_cargo_arguments_cannot_pass_or_fall_back(self):
        f = self.fixture()
        result = f.command(['cargo', 'unexpected-fixture-command'])
        f.capture('unexpected-cargo-control', result, ['cargo', 'unexpected-fixture-command'])
        self.assertEqual(result.returncode, 86)
        self.assertIn('unexpected synthetic cargo argv', result.stderr)
        with self.assertRaisesRegex(AssertionError, 'unexpected command or stub error'):
            f.assert_valid_stubs()

    def test_broken_cargo_stub_is_not_accepted_as_builder_success(self):
        f = self.fixture()
        f.settings_path.write_text('invalid JSON')
        with self.assertRaisesRegex(AssertionError, 'stub_error'):
            f.build()
        self.assertFalse(f.artifacts.exists())
