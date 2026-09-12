"""Prerequisite failures must stop the actual builder before test/build/package."""
import sys
import unittest

from artifact_builder_fixture import BuilderFixture, LIB_PACKAGE


class ArtifactBuilderPrerequisites(unittest.TestCase):
    def fixture(self, **settings):
        fixture = BuilderFixture(LIB_PACKAGE, **settings)
        self.addCleanup(fixture.close)
        return fixture

    def assert_stopped(self, f, result, guards, metadata_calls, reason):
        self.assertNotEqual(result.returncode, 0, 'prerequisite error must not produce a successful artifact')
        self.assertIn(reason, result.stderr)
        events = f.events()
        self.assertEqual([e['argv'] for e in events if e['tool'] == 'guard'], guards)
        cargo = [e['argv'] for e in events if e['tool'] == 'cargo']
        self.assertEqual(cargo, [['metadata', '--locked', '--format-version=1', '--no-deps']] * metadata_calls,
                         'no tests, build, version query, or metadata retries after prerequisite failure')
        self.assertFalse(any(e['tool'] == 'rustc' for e in events))
        self.assertFalse(f.target.exists())
        self.assertFalse(f.artifacts.exists())
        self.assertFalse((f.output / 'build-manifest.json').exists())
        self.assertFalse(f.output.with_suffix('.tar.gz').exists())

    def guard_unavailable(self, missing):
        f = self.fixture()
        guard = f.repo / 'tools/architecture_guard.sh'
        if missing:
            guard.unlink()
        else:
            guard.chmod(0o644)
        result = f.build(allow_dirty=True)
        if result.returncode == 0:  # Record the inherited false success and verifier boundary in RED.
            verification = f.verify(allow_dirty=True)
            self.assertNotEqual(verification.returncode, 0)
            self.assertIn('missing required checks', verification.stderr)
        self.assert_stopped(f, result, [], 1, 'tools/architecture_guard.sh')

    def test_missing_guard_stops_checked_builder(self):
        self.guard_unavailable(missing=True)

    def test_nonexecutable_guard_stops_checked_builder(self):
        self.guard_unavailable(missing=False)

    def test_changed_guard_failure_prevents_all_and_later_stages(self):
        f = self.fixture(guard_failure='--changed')
        self.assert_stopped(f, f.build(), [['--changed']], 1, 'synthetic guard failure: --changed')

    def test_all_guard_failure_prevents_metadata_tests_and_build(self):
        f = self.fixture(guard_failure='--all')
        self.assert_stopped(f, f.build(), [['--changed'], ['--all']], 1, 'synthetic guard failure: --all')

    def metadata_failure(self, mode, reason):
        f = self.fixture(second_metadata=mode)
        result = f.build()
        responses = [e for e in f.events() if e['tool'] == 'metadata_response']
        self.assertEqual(responses, [{'tool': 'metadata_response', 'call': 1, 'mode': 'valid'},
                                     {'tool': 'metadata_response', 'call': 2, 'mode': mode}])
        if result.returncode == 0:
            verification = f.verify()  # Retain causal false-green evidence without modifying the verifier.
            self.assertEqual(verification.returncode, 0, verification.stderr)
        self.assert_stopped(f, result, [['--changed'], ['--all']], 2, reason)
        self.assertNotIn('Traceback', result.stderr)

    def test_second_metadata_nonzero_is_not_nonlib_success(self):
        self.metadata_failure('failure', 'metadata')

    def test_second_metadata_malformed_json_stops(self):
        self.metadata_failure('malformed', 'metadata')

    def test_second_metadata_unknown_package_stops(self):
        self.metadata_failure('unknown-package', 'unknown package')

    def test_second_metadata_missing_targets_is_not_nonlib_success(self):
        self.metadata_failure('invalid-shape', 'metadata')

    def test_second_metadata_invalid_target_kind_stops(self):
        self.metadata_failure('invalid-kind', 'metadata')

    def test_invalid_classification_output_is_not_nonlib_success(self):
        for value in ['2', '', '1\n0']:
            with self.subTest(value=value):
                f = self.fixture()
                # Only the stdin Python classification invocation is substituted. Other Python
                # tools run unchanged; Cargo/rustc retain the closed synthetic PATH.
                wrapper = f.bin / 'python3'
                wrapper.unlink()
                wrapper.write_text(f'#!{sys.executable}\nimport json, os, sys\n'
                                   'if sys.argv[1:2] == ["-"]:\n'
                                   '    with open(os.environ["FIXTURE_TRACE"], "a") as out:\n'
                                   f'        out.write(json.dumps({{"tool": "classification_response", "value": {value!r}}}) + "\\n")\n'
                                   f'    print({value!r})\nelse:\n'
                                   f'    os.execv({sys.executable!r}, [{sys.executable!r}, *sys.argv[1:]])\n')
                wrapper.chmod(0o755)
                result = f.build()
                self.assertEqual([e['value'] for e in f.events() if e['tool'] == 'classification_response'], [value])
                self.assert_stopped(f, result, [['--changed'], ['--all']], 1, 'invalid package type')

    def test_missing_guard_smoke_has_no_checks_and_is_not_production_proof(self):
        f = self.fixture()
        (f.repo / 'tools/architecture_guard.sh').unlink()
        result = f.build(checks=False, allow_dirty=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(f.manifest()['checks'], [])
        self.assertTrue(f.manifest()['git_dirty'])
        self.assertTrue(f.output.with_suffix('.tar.gz').exists())
        self.assertFalse(any(e['tool'] == 'guard' or (e['tool'] == 'cargo' and e['argv'][0] == 'test')
                             for e in f.events()))
        self.assertEqual(len([e for e in f.events() if e['tool'] == 'metadata_response']), 1)
        result = f.verify()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('refusing dirty artifact', result.stderr)
        result = f.verify(allow_dirty=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('missing required checks', result.stderr)
