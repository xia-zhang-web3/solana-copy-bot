from architecture_guard_fixtures.harness import GuardCase


class Modes(GuardCase):
    def test_clean_changed_still_checks_workspace(self):
        self.pair(contains=('changed files: 0',))
        self.pair('--all')
        (self.root / 'BUILD_POLICY.md').unlink()
        self.pair(expected=1, contains=('missing required architecture file',))

    def test_unstaged_staged_untracked_combined_ignored(self):
        self.append('crates/app/src/lib.rs', '// unstaged\n')
        self.write('untracked.py', '# new\n')
        self.write('ignored/too_big.py', '# ignored\n' * 610)
        self.pair(contains=('changed files: 2',))
        self.git('add', 'crates/app/src/lib.rs')
        self.pair(contains=('changed files: 2',))
        self.append('crates/app/src/lib.rs', '// both\n')
        self.pair(contains=('changed files: 2',))
        self.git('add', '-f', 'ignored/too_big.py')
        self.pair(expected=1, contains=('610 > 600',))

    def test_rename_deletion(self):
        self.write('notes.py', '# fixture\n')
        self.commit()
        self.git('mv', 'notes.py', 'renamed.py')
        (self.root / 'README.md').unlink()
        self.pair(contains=('changed files: 1',))
        self.pair('--all')

    def test_ranges_and_dirty_ci(self):
        a = self.git('rev-parse', 'HEAD')
        self.write('notes.py', '# committed\n')
        b = self.commit()
        self.pair(ARCH_GUARD_DIFF_RANGE=a + '..' + b, contains=('changed files: 1',))
        self.append('README.md', 'dirty\n')
        self.write('staged.py', '# staged\n')
        self.git('add', 'staged.py')
        self.write('new.py', '# new\n')
        self.pair(ARCH_GUARD_DIFF_RANGE=a + '..' + b, contains=('changed files: 4',))
        self.pair(ARCH_GUARD_DIFF_RANGE=a + '..' + b, GITHUB_ACTIONS='true', contains=('changed files: 1',))
        self.pair('--all', ARCH_GUARD_DIFF_RANGE=a + '..' + b, GITHUB_ACTIONS='true')

    def test_divergent_triple_dot_uses_literal_left_baseline(self):
        origin = self.git('rev-parse', 'HEAD')
        inline = '#[cfg(test)]\nmod checks {\nfn check() {}\n}\n'
        self.append('crates/app/src/lib.rs', inline)
        left = self.commit()
        self.git('checkout', '-qb', 'right', origin)
        self.append('crates/app/src/lib.rs', inline)
        self.write('right.py', '# distinct branch\n')
        right = self.commit()
        self.pair(ARCH_GUARD_DIFF_RANGE=left + '...' + right)
        self.pair('--all', ARCH_GUARD_DIFF_RANGE=left + '...' + right, contains=('grandfathered inline',))

    def test_empty_tree_first_commit(self):
        empty = self.git('hash-object', '-t', 'tree', '--stdin', input='')
        self.pair(ARCH_GUARD_DIFF_RANGE=empty + '..HEAD')

    def test_invalid_ranges(self):
        self.git('tag', 'same-a')
        self.git('tag', 'same-b')
        for value in ('HEAD..HEAD', 'HEAD...HEAD', 'same-a..same-b', 'missing..HEAD', '..HEAD', 'HEAD..', 'HEAD'):
            with self.subTest(value=value):
                self.pair(expected=1, ARCH_GUARD_DIFF_RANGE=value)

    def test_invalid_cli(self):
        self.pair('--invalid', expected=2, contains=('usage:',))

    def test_space_and_pathspec_filenames(self):
        self.write('trackedA.txt', 'ordinary\n')
        self.commit()
        self.write('name with spaces.py', '# new\n')
        self.write('tracked*.txt', '\x35\x2dday\n')
        self.pair()
        self.pair('--all', expected=1)
