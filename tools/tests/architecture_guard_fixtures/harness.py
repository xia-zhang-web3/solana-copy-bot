"""Two guards, one synthetic repository: HEAD/refs/index/worktree stay identical."""
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import tempfile
import unittest

TOOLS = Path(__file__).resolve().parents[2]
OLD = Path(os.environ.get('ARCH_GUARD_OLD_TOOLS', Path(__file__).parent / 'frozen/tools'))
POLICIES = ('BUILD_POLICY.md', 'BUILD_REFACTOR_ROADMAP.md', 'ARTIFACT_DEPLOY.md', 'ARCHITECTURE_WAIVERS.md')


def command(args, root, env=None, check=True, timeout=15, input=None):
    return subprocess.run(args, cwd=root, env=env, check=check, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout, input=input)


class GuardCase(unittest.TestCase):
    def setUp(self):
        def expired(signum, frame):
            raise TimeoutError('architecture guard fixture exceeded 60 seconds')
        previous_alarm = signal.signal(signal.SIGALRM, expired)
        signal.alarm(60)
        self.addCleanup(signal.signal, signal.SIGALRM, previous_alarm)
        self.addCleanup(signal.alarm, 0)
        self.temp = tempfile.TemporaryDirectory(prefix='architecture-guard-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = dict(os.environ, GIT_OPTIONAL_LOCKS='0', CARGO_NET_OFFLINE='true', CARGO_TARGET_DIR=str(self.root / '.git/target'), PYTHONDONTWRITEBYTECODE='1')
        self.env.pop('ARCH_GUARD_DIFF_RANGE', None)
        self.env.pop('GITHUB_ACTIONS', None)
        self.git('init', '-q')
        self.git('config', 'user.email', 'fixture@example.invalid')
        self.git('config', 'user.name', 'Guard Fixture')
        self.git('config', 'commit.gpgsign', 'false')
        self.write('.gitignore', 'ignored/\n')
        self.write('Cargo.toml', '[workspace]\nmembers = ["crates/*"]\nresolver = "2"\n[profile.operator-release]\ninherits = "release"\n')
        for name in ('app', 'operators', 'storage-core'):
            self.package(name)
        for path in POLICIES:
            self.write(path, 'tools/architecture_guard.sh --changed\n')
        self.write('README.md', 'Architecture fixture.\n')
        self.lock()
        self.commit()

    def write(self, path, content):
        out = self.root / path
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(content if isinstance(content, bytes) else content.encode())

    def append(self, path, content):
        with (self.root / path).open('ab') as handle:
            handle.write(content.encode())

    def package(self, name, manifest=''):
        self.write(f'crates/{name}/Cargo.toml', f'[package]\nname = "copybot-{name}"\nversion = "0.1.0"\nedition = "2021"\n' + manifest)
        self.write(f'crates/{name}/src/lib.rs', '// fixture\n')

    def git(self, *args, input=None):
        return command(['git', *args], self.root, self.env, input=input).stdout.strip()

    def lock(self):
        command(['cargo', 'tree', '--offline', '--edges', 'normal,build,dev'], self.root, self.env)

    def commit(self):
        self.git('add', '.')
        self.git('commit', '-qm', 'fixture')
        return self.git('rev-parse', 'HEAD')

    def state(self):
        paths = command(['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], self.root).stdout.split('\0')
        return (self.git('show-ref', '--head'), self.git('ls-files', '--stage', '-z'), {p: hashlib.sha256((self.root / p).read_bytes()).hexdigest() for p in paths if p and (self.root / p).is_file()})

    def run_guard(self, tools, mode, env):
        lines = (tools / 'architecture_guard.sh').read_text().splitlines(True)
        # Only relocate SCRIPT_DIR. The actual entrypoint body and CLI are used.
        source = 'set -euo pipefail\nSCRIPT_DIR=' + shlex.quote(str(tools)) + '\nROOT_DIR="$PWD"\n' + ''.join(lines[6:])
        prefix, suffix = source.rsplit('if ((failures > 0)); then', 1)
        source = prefix + 'printf \'[fixture] checked paths: %s\\n\' \"$file_count\"\nif ((failures > 0)); then' + suffix
        return command(['bash', '-c', source, 'architecture_guard.sh', mode], self.root, env, check=False, timeout=20)

    def pair(self, mode='--changed', expected=0, contains=(), **environment):
        env = dict(self.env, **environment)
        before = self.state()
        index_bytes = (self.root / '.git/index').read_bytes()
        old = self.run_guard(OLD, mode, env)
        self.assertEqual(before, self.state(), 'old guard mutated corpus')
        # Cargo/libgit2 can refresh index stat fields; restore identical bytes.
        (self.root / '.git/index').write_bytes(index_bytes)
        new = self.run_guard(TOOLS, mode, env)
        self.assertEqual(before, self.state(), 'new guard mutated corpus')
        if os.environ.get('ARCH_GUARD_TEST_EVIDENCE'):
            record = {'case': self.id(), 'mode': mode, 'environment': environment}
            for label, result in (('old', old), ('new', new)):
                record[label] = {'exit': result.returncode, 'stdout': result.stdout.replace(str(self.root), '<fixture>'), 'stderr': result.stderr.replace(str(self.root), '<fixture>')}
            with open(os.environ['ARCH_GUARD_TEST_EVIDENCE'], 'a') as out:
                out.write(json.dumps(record) + '\n')
        self.assertEqual(old.returncode, new.returncode, (old.stderr, new.stderr))
        # A multiset changes only independent diagnostic order; duplicates/counts,
        # full paths, messages, DEBT, final status and file counts are retained.
        self.assertEqual(Counter(old.stdout.splitlines()), Counter(new.stdout.splitlines()))
        self.assertEqual(Counter(old.stderr.splitlines()), Counter(new.stderr.splitlines()))
        self.assertEqual(new.returncode, expected, new.stdout + new.stderr)
        for message in contains:
            self.assertIn(message, new.stdout + new.stderr)
        return new
