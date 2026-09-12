"""R1 regressions: cache faults, exact diagnostic paths and external stdin."""
import json
import os
from pathlib import Path
import shlex
import shutil
import sys
import subprocess

from architecture_guard_fixtures.harness import GuardCase, OLD, TOOLS


class Revision(GuardCase):
    def record(self, label, result):
        if os.environ.get('ARCH_GUARD_REVISION_EVIDENCE'):
            record = {'case': self.id(), 'label': label, 'exit': result.returncode,
                      'stdout': result.stdout.replace(str(self.root), '<fixture>'),
                      'stderr': result.stderr.replace(str(self.root), '<fixture>')}
            with open(os.environ['ARCH_GUARD_REVISION_EVIDENCE'], 'a') as out:
                out.write(json.dumps(record) + '\n')
        return result

    def wrapper(self, name, body):
        directory = self.root / '.git/fault-bin'
        directory.mkdir(exist_ok=True)
        path = directory / name
        path.write_text('#!/bin/sh\n' + body + '\nexec ' + shlex.quote(shutil.which(name)) + ' "$@"\n')
        path.chmod(0o755)
        return dict(self.env, PATH=str(directory) + os.pathsep + self.env['PATH'])

    def fault_run(self, label, env, mode='--changed', tools=TOOLS):
        return self.record(label, self.run_guard(tools, mode, env))

    def closed(self, result):
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn('[architecture:guard] PASS', result.stdout)

    def test_curator_cache_read_failure(self):
        self.append('README.md', '\x35\x2dday forbidden marker probe\n')
        self.pair(expected=1)
        env = self.wrapper('cat', 'case "$*" in */main-3) echo cache-read-failed >&2; exit 74;; esac')
        self.closed(self.fault_run('old_fault', env, tools=OLD))
        self.closed(self.fault_run('new_fault', env))

    def test_used_cache_streams_partial_read_and_cleanup(self):
        start = self.git('rev-parse', 'HEAD')
        self.append('README.md', 'committed\n')
        end = self.commit()
        self.append('README.md', 'staged\n')
        self.git('add', 'README.md')
        self.append('README.md', 'unstaged\n')
        real = shlex.quote(shutil.which('cat'))
        for key in ('main-3', 'cached-0', 'cached-3', 'local-0', 'local-3'):
            with self.subTest(key=key):
                # Emit the actual cache bytes, then simulate a failed final read.
                body = 'case "$*" in */' + key + ')\nprintf "%s\\n" "$1" >> .git/cache-reads\n' + real + ' "$@"\nexit 74;; esac'
                env = self.wrapper('cat', body)
                env['ARCH_GUARD_DIFF_RANGE'] = start + '..' + end
                result = self.fault_run(key, env)
                self.closed(result)
                reads = (self.root / '.git/cache-reads').read_text().splitlines()
                self.assertTrue(reads)
                self.assertTrue(all(not Path(p).parent.exists() for p in reads))
                (self.root / '.git/cache-reads').unlink()

    def test_unused_cache_streams_and_modes(self):
        start = self.git('rev-parse', 'HEAD')
        self.append('README.md', 'ordinary\n')
        end = self.commit()
        env = self.wrapper('cat', 'case "$*" in */cached-*|*/local-*) exit 74;; esac')
        self.pair(ARCH_GUARD_DIFF_RANGE=start + '..' + end, GITHUB_ACTIONS='true', PATH=env['PATH'])
        self.pair('--all', PATH=env['PATH'])
        self.pair()

    def test_literal_scanner_failure(self):
        env = self.wrapper('grep', 'case "$*" in "-v -F -- +++") exit 73;; esac')
        self.closed(self.fault_run('literal_consumer', env))

    def test_regex_scanner_failure(self):
        env = self.wrapper('grep', 'case "$*" in *published_scoring_source*) exit 73;; esac')
        self.closed(self.fault_run('regex_consumer', env))

    def test_normalized_scanner_failure(self):
        # Fail only the normalized scanner process; all other Python work is real.
        env = self.wrapper('python3', 'case "$*" in *"hunk_old = []"*|*" normalized-"*) exit 75;; esac')
        self.closed(self.fault_run('normalized_consumer', env))

    def test_scanner_failure_cannot_masquerade_as_no_match(self):
        for status in (1, 0):
            with self.subTest(status=status):
                env = self.wrapper('python3', 'case "$*" in *diff_scan.py*) echo invalid-result; exit ' + str(status) + ';; esac')
                result = self.fault_run('scanner_protocol_exit_' + str(status), env)
                self.closed(result)
                self.assertIn('failed to scan architecture diff cache', result.stderr)

    def test_large_early_match_and_no_match(self):
        # Real multi-hunk diff, larger than pipe buffers, with an early match.
        lines = ['// ordinary context\n'] * 18000
        self.write('scan.txt', ''.join(lines))
        self.commit()
        lines[1] = '\x35\x2dday\n'
        lines[-2] = 'last hunk\n'
        self.write('scan.txt', ''.join(lines))
        result = self.fault_run('large_early_match', self.env)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn('split forbidden legacy Stage 3 marker', result.stderr)
        self.assertNotIn('failed to', result.stderr)
        lines[1] = 'ordinary replacement\n'
        # Keep a large added hunk, beyond an early normalized match boundary.
        lines.extend(['ordinary tail\n'] * 18000)
        self.write('scan.txt', ''.join(lines))
        self.pair()
        lines[1] = '\x35\x2dday\n'
        self.write('scan.txt', ''.join(lines))
        result = self.fault_run('large_tail_early_match', self.env)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn('split forbidden legacy Stage 3 marker', result.stderr)
        self.assertNotIn('failed to', result.stderr)

    def test_exact_leading_and_regular_paths(self):
        for path in ('regular.py', ' leading.py', '  two spaces.py'):
            with self.subTest(path=path):
                self.write(path, '# line\n' * 601)
                try:
                    self.pair(expected=1, contains=('FAIL ' + path + ' exceeds hard size limit (601 > 600 LOC)',))
                finally:
                    (self.root / path).unlink()

    def test_empty_tree_with_external_stdin(self):
        env = dict(self.env, PYTHONPATH=str(TOOLS / 'tests'))
        # The child pair is a stdin regression, not another differential corpus case.
        env.pop('ARCH_GUARD_TEST_EVIDENCE', None)
        for payload in ('', 'not-a-tree'):
            with self.subTest(payload=payload):
                result = subprocess.run([sys.executable, '-B', '-m', 'unittest',
                                  'test_architecture_guard_modes.Modes.test_empty_tree_first_commit', '-v'],
                                 cwd=TOOLS.parent, env=env, check=False, timeout=20, input=payload,
                                 text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                self.record('external_stdin_' + ('empty' if not payload else 'nonempty'), result)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
