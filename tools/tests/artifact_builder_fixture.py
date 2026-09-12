"""Disposable Git checkout running the real builder/manifest/verifier with fake Cargo."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
DASHBOARD = 'copybot-ops-dashboard'
LIB_PACKAGE = 'copybot-discovery-v2'
BINARIES = {
    DASHBOARD: ['copybot_ops_dashboard', 'copybot_ops_dashboard_snapshot_export'],
    LIB_PACKAGE: ['discovery_v2_prepare_quality', 'discovery_v2_publish',
                  'discovery_v2_status', 'discovery_v2_wallet_report'],
    'copybot-app': ['copybot-app'],
}
REAL_TOOLS = ['build_operator_artifacts.sh', 'build_manifest.py', 'package_bins.py',
              'verify_operator_artifact.py', 'lib/operator_artifact_verify.py']


class BuilderFixture:
    def __init__(self, package=DASHBOARD, **settings):
        self.temporary = tempfile.TemporaryDirectory(prefix='artifact-builder-')
        self.directory = Path(self.temporary.name)
        self.repo = self.directory / 'repo'
        self.repo.mkdir()
        self.bin = self.directory / 'bin'
        self.bin.mkdir()
        self.trace = self.directory / 'commands.jsonl'
        self.settings_path = self.directory / 'settings.json'
        self.artifacts = self.directory / 'artifacts'
        self.target = self.directory / 'target'
        self.package = package
        self.bins = BINARIES[package]
        self.profile = 'release' if package == 'copybot-app' else 'operator-release'
        targets = [{'name': name, 'kind': ['bin']} for name in self.bins]
        if package == LIB_PACKAGE:
            targets += [{'name': 'copybot_discovery_v2', 'kind': ['lib']},
                        {'name': 'synthetic_lib_integration', 'kind': ['test']}]
        if package == DASHBOARD:
            targets += [{'name': name, 'kind': ['test']} for name in
                        ['cash_settlement_export', 'failed_expense_export', 'fee_coverage_export']]
        self.settings = dict(package=package, bins=self.bins, has_lib=package == LIB_PACKAGE,
                             metadata={'packages': [{'name': package, 'features': {}, 'targets': targets}]},
                             **settings)
        self.settings_path.write_text(json.dumps(self.settings))
        for name in REAL_TOOLS:
            destination = self.repo / 'tools' / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / 'tools' / name, destination)
        self.builder_sha = hashlib.sha256((self.repo / 'tools/build_operator_artifacts.sh').read_bytes()).hexdigest()
        guard = self.repo / 'tools/architecture_guard.sh'
        guard.write_text('#!/bin/sh\ncase "$1" in --changed|--all) ;; *) exit 88;; esac\n'
                         'printf \'{"tool":"guard","argv":["%s"]}\\n\' "$1" >> "$FIXTURE_TRACE"\n'
                         'if [ "${FIXTURE_GUARD_FAILURE:-}" = "$1" ]; then\n'
                         '  echo "synthetic guard failure: $1" >&2; exit 39\nfi\n')
        guard.chmod(0o755)
        (self.repo / 'Cargo.toml').write_text('[workspace]\nmembers = []\n')
        (self.repo / 'Cargo.lock').write_text('version = 4\n')
        if package == 'copybot-app':
            (self.repo / 'migrations').mkdir()
            (self.repo / 'migrations/0001_fixture.sql').write_text('SELECT 1;\n')
        # A closed utility PATH: neither cargo nor rustc can fall through to a real compiler.
        for name in ['bash', 'git', 'python3', 'echo', 'xargs', 'tr', 'sed', 'sort', 'rmdir', 'mkdir',
                     'cp', 'tar', 'shasum', 'sha256sum', 'basename', 'dirname', 'rm', 'cat']:
            executable = sys.executable if name == 'python3' else shutil.which(name)
            if executable:
                (self.bin / name).symlink_to(executable)
        fake = (ROOT / 'tools/tests/artifact_builder_fake_cargo.py').read_text()
        for name in ['cargo', 'rustc']:
            path = self.bin / name
            path.write_text(f'#!{sys.executable}\n' + fake)
            path.chmod(0o755)
        self.env = dict(os.environ)
        for key in ['GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'PROFILE', 'WANTED_BINS',
                    'RUN_CHECKS', 'ALLOW_DIRTY', 'FORCE', 'BASH_ENV', 'ENV']:
            self.env.pop(key, None)
        self.env.update(PATH=str(self.bin), PYTHONPATH='', PYTHONDONTWRITEBYTECODE='1',
                        CARGO_NET_OFFLINE='true', CARGO_TARGET_DIR=str(self.target),
                        PACKAGE=package, ARTIFACT_ROOT=str(self.artifacts), ARTIFACT_ARCH='linux-x86_64',
                        TARGET='x86_64-unknown-linux-gnu', FIXTURE_TRACE=str(self.trace),
                        FIXTURE_GUARD_FAILURE=settings.get('guard_failure', ''),
                        FIXTURE_SETTINGS=str(self.settings_path), GIT_CONFIG_NOSYSTEM='1',
                        GIT_CONFIG_GLOBAL=os.devnull, GIT_TERMINAL_PROMPT='0')
        for args in [['init', '-q'], ['add', '--', '.'],
                     ['-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
                      '-c', 'commit.gpgsign=false', '-c', 'core.hooksPath=/dev/null',
                      'commit', '-qm', 'synthetic fixture']]:
            self.command(['git', *args], check=True)
        self.sha = self.command(['git', 'rev-parse', 'HEAD'], check=True).stdout.strip()
        self.output = self.artifacts / 'linux-x86_64' / f'{package}-{self.sha}'

    def close(self):
        self.temporary.cleanup()

    def command(self, args, check=False, **env):
        return subprocess.run(args, cwd=self.repo, env=dict(self.env, **env),
                              stdin=subprocess.DEVNULL, text=True, capture_output=True,
                              timeout=20, check=check)

    def events(self):
        return [json.loads(line) for line in self.trace.read_text().splitlines()] if self.trace.exists() else []

    def assert_valid_stubs(self):
        errors = [e for e in self.events() if e['tool'] in ('unexpected', 'stub_error')]
        if errors:
            raise AssertionError(f'fixture had an unexpected command or stub error: {errors}')

    def manifest(self):
        return json.loads((self.output / 'build-manifest.json').read_text())

    def capture(self, phase, result, argv):
        evidence = os.environ.get('ARTIFACT_BUILDER_EVIDENCE')
        if evidence:
            manifest = self.manifest() if (self.output / 'build-manifest.json').exists() else None
            with open(evidence, 'a') as output:
                output.write(json.dumps(dict(phase=phase, package=self.package, builder_sha256=self.builder_sha,
                                             argv=argv, exit_code=result.returncode, stdout=result.stdout,
                                             stderr=result.stderr, events=self.events(), manifest=manifest,
                                             settings=self.settings, output_exists=self.output.exists(),
                                             target_exists=self.target.exists(), artifacts_exist=self.artifacts.exists(),
                                             archive_exists=self.output.with_suffix('.tar.gz').exists())) + '\n')

    def build(self, checks=True, allow_dirty=False):
        assert not self.artifacts.exists() and not self.target.exists(), 'fresh fixture outputs required'
        if allow_dirty and self.command(['git', 'status', '--porcelain'], check=True).stdout:
            self.output = self.output.with_name(self.output.name + '-dirty')
        argv = ['bash', 'tools/build_operator_artifacts.sh']
        result = self.command(argv, RUN_CHECKS='1' if checks else '0', ALLOW_DIRTY='1' if allow_dirty else '0')
        self.capture('builder-checked' if checks else 'builder-smoke', result, argv)
        self.assert_valid_stubs()
        return result

    def verify(self, allow_dirty=False):
        argv = [sys.executable, 'tools/verify_operator_artifact.py', str(self.output),
                '--expect-package', self.package, '--expect-profile', self.profile,
                '--expect-target', 'x86_64-unknown-linux-gnu', '--enforce-workspace-bin-check']
        if allow_dirty:
            argv += ['--allow-dirty']
        result = self.command(argv)
        self.capture('real-verifier-synthetic-payload', result, argv)
        self.assert_valid_stubs()
        return result
