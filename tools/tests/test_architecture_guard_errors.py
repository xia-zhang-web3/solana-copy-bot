"""Infrastructure failures must never become an empty successful scan."""
from pathlib import Path
import os
import shlex
import shutil

from architecture_guard_fixtures.harness import GuardCase, TOOLS


class Errors(GuardCase):
    def wrapper(self, name, source):
        directory = self.root / '.git/fault-bin'
        directory.mkdir(exist_ok=True)
        path = directory / name
        path.write_text('#!/bin/sh\n' + source)
        path.chmod(0o755)
        return dict(self.env, PATH=str(directory) + os.pathsep + self.env['PATH'])

    def assert_closed(self, env, tools=TOOLS):
        result = self.run_guard(tools, '--changed', env)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn('[architecture:guard] PASS', result.stdout)
        return result

    def test_git_selection_error(self):
        real = shlex.quote(shutil.which('git'))
        env = self.wrapper('git', 'if [ "$1" = ls-files ]; then exit 72; fi\nexec ' + real + ' "$@"\n')
        self.assert_closed(env)

    def test_git_diff_content_error(self):
        real = shlex.quote(shutil.which('git'))
        env = self.wrapper('git', 'case "$*" in *--unified=*) exit 72;; esac\nexec ' + real + ' "$@"\n')
        self.assert_closed(env)

    def test_git_baseline_error(self):
        real = shlex.quote(shutil.which('git'))
        env = self.wrapper('git', 'if [ "$1" = cat-file ]; then exit 72; fi\nexec ' + real + ' "$@"\n')
        self.append('crates/app/src/lib.rs', '// touched\n')
        self.assert_closed(env)

    def test_cargo_metadata_error_and_invalid_output(self):
        self.assert_closed(self.wrapper('cargo', 'exit 73\n'))
        self.assert_closed(self.wrapper('cargo', 'echo invalid-json\n'))

    def test_cargo_tree_error(self):
        real = shlex.quote(shutil.which('cargo'))
        env = self.wrapper('cargo', 'if [ "$1" = tree ]; then exit 73; fi\nexec ' + real + ' "$@"\n')
        result = self.assert_closed(env)
        self.assertIn('failed to inspect dependency graph', result.stderr)

    def test_helper_error_and_no_persistent_pass(self):
        copied = self.root / '.git/new-tools'
        shutil.copytree(TOOLS / 'lib/architecture_guard', copied / 'lib/architecture_guard')
        shutil.copy2(TOOLS / 'architecture_guard.sh', copied / 'architecture_guard.sh')
        (copied / 'lib/architecture_guard/file_batch.py').write_text('raise SystemExit(74)\n')
        self.assert_closed(self.env, copied)
        self.pair()
        self.write('new.py', '# line\n' * 601)
        self.pair(expected=1, contains=('601 > 600',))
