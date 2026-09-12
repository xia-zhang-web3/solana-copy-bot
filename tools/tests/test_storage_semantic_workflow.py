"""Check this workflow's small fixed shape and execute its actual Cargo step."""
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / '.github/workflows/storage-semantic.yml'
ARTIFACT = ROOT / '.github/workflows/operator-artifacts.yml'
EXPECTED_ARGS = ['test', '--locked', '-p', 'copybot-storage-core', '--lib', '--tests']


def parts():
    # Deliberately support only the reviewed, single-job workflow layout.
    source = WORKFLOW.read_text()
    if source.count('jobs:\n') != 1:
        raise AssertionError('expected one jobs section')
    header, job = source.split('jobs:\n')
    if job.count('    steps:\n') != 1:
        raise AssertionError('expected one steps section')
    settings, steps = job.split('    steps:\n')
    return source, header, settings, steps


def cargo_step():
    steps = parts()[3]
    matches = re.findall(
        r'^      - name: Storage semantic tests\n'
        r'        shell: bash\n'
        r'        run: (.+)\n?\Z', steps, re.M)
    if len(matches) != 1:
        raise AssertionError('expected an unconditional final Cargo step using bash')
    return matches[0]


class StorageSemanticWorkflow(unittest.TestCase):
    def test_all_changes_trigger_read_only_workflow(self):
        source, header, _, _ = parts()
        self.assertEqual(header, 'name: Storage Semantic\n\non:\n  pull_request:\n'
                         '  push:\n  workflow_dispatch:\n\npermissions:\n  contents: read\n\n')
        self.assertEqual(source.count('permissions:'), 1)

    def test_one_unconditional_hosted_check_has_ten_minute_budget(self):
        source, _, settings, steps = parts()
        self.assertEqual(settings,
                         '  storage-semantic:\n    name: storage-semantic\n'
                         '    runs-on: ubuntu-latest\n    timeout-minutes: 10\n'
                         '    env:\n      CARGO_TERM_COLOR: always\n'
                         '      CARGO_TARGET_DIR: target/storage-semantic\n')
        self.assertEqual(re.findall(r'^      - (.+)$', steps, re.M),
                         ['uses: actions/checkout@v4', 'name: Rust versions',
                          'name: Cache cargo state', 'name: Workflow contract',
                          'name: Storage semantic tests'])
        self.assertNotRegex(source, r'\b(if|continue-on-error|needs|secrets|strategy):')
        self.assertNotIn('cache-hit', source)
        self.assertEqual(source.count('timeout-minutes:'), 1)

    def test_rust_and_cache_follow_artifact_flow_with_separate_target(self):
        source, _, _, steps = parts()
        artifact = ARTIFACT.read_text()
        for shared in ('actions/checkout@v4', 'actions/cache@v4',
                       'rustc --version\n          cargo --version',
                       '~/.cargo/git\n            ~/.cargo/registry',
                       "hashFiles('Cargo.lock', 'rust-toolchain*', '.cargo/**')"):
            self.assertIn(shared, artifact)
            self.assertIn(shared, source)
        setup = steps.split('      - name: Storage semantic tests\n')[0]
        self.assertEqual(setup.count('        run:'), 2)
        self.assertIn('      - name: Rust versions\n        shell: bash\n'
                      '        run: |\n          rustc --version\n          cargo --version\n', setup)
        self.assertIn('            target/storage-semantic\n', setup)
        self.assertIn('          key: cargo-storage-semantic-${{ runner.os }}-', setup)
        self.assertIn('          restore-keys: |\n            cargo-storage-semantic-${{ runner.os }}-\n', setup)
        self.assertIn('      - name: Workflow contract\n        shell: bash\n'
                      '        run: python3 -B -m unittest discover -s tools/tests '
                      '-p test_storage_semantic_workflow.py -v\n', setup)
        self.assertNotIn('target/artifacts', source)

    def test_actual_entrypoint_runs_complete_locked_package(self):
        command = cargo_step()
        self.assertEqual(command, 'cargo test --locked -p copybot-storage-core --lib --tests')
        self.assertEqual(shlex.split(command), ['cargo', *EXPECTED_ARGS])
        self.assertEqual(parts()[0].count('        run:'), 3)

    def run_step(self, cargo_exit):
        command = cargo_step()
        bash = shutil.which('bash')
        self.assertIsNotNone(bash)
        with tempfile.TemporaryDirectory(prefix='storage-semantic-step-') as directory:
            root = Path(directory)
            bindir = root / 'bin'
            bindir.mkdir()
            marker = root / 'cargo-argv.txt'
            if cargo_exit is not None:
                cargo = bindir / 'cargo'
                cargo.write_text('#!/bin/sh\nprintf "%s\\n" "$@" >> "$STORAGE_CARGO_ARGV"\n'
                                 f'exit {cargo_exit}\n')
                cargo.chmod(0o755)
            script = root / 'step.sh'
            script.write_text(command + '\n')
            # GitHub's explicit `shell: bash` fail-fast/pipefail template.
            argv = [bash, '--noprofile', '--norc', '-e', '-o', 'pipefail', str(script)]
            result = subprocess.run(argv, cwd=root,
                                    env={'PATH': str(bindir), 'STORAGE_CARGO_ARGV': str(marker)},
                                    stdin=subprocess.DEVNULL, text=True, capture_output=True, timeout=5)
            actual_args = marker.read_text().splitlines() if marker.exists() else None
            evidence = os.environ.get('STORAGE_SEMANTIC_EVIDENCE')
            if evidence:
                with open(evidence, 'a') as output:
                    output.write(json.dumps({'workflow': str(WORKFLOW), 'command': command,
                                             'argv': argv, 'fake_cargo_exit': cargo_exit,
                                             'actual_cargo_args': actual_args,
                                             'exit_code': result.returncode, 'stdout': result.stdout,
                                             'stderr': result.stderr}) + '\n')
            return result, actual_args

    def test_actual_step_propagates_cargo_success_and_failures(self):
        for cargo_exit in (0, 37, 101):
            with self.subTest(cargo_exit=cargo_exit):
                result, args = self.run_step(cargo_exit)
                self.assertEqual(args, EXPECTED_ARGS, 'Cargo must execute exactly once on every run')
                self.assertEqual(result.returncode, cargo_exit, result.stderr)

    def test_actual_step_rejects_missing_cargo(self):
        result, args = self.run_step(None)
        self.assertIsNone(args)
        self.assertEqual(result.returncode, 127, result.stderr)
        self.assertIn('cargo: command not found', result.stderr)
