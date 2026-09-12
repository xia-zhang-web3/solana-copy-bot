from architecture_guard_fixtures.harness import GuardCase


class Files(GuardCase):
    def test_size_roles_and_newlines(self):
        paths = {'plain.py': 600, 'script.sh': 600, 'crates/app/src/module.rs': 600,
                 'crates/operators/src/bin/tool.rs': 300, 'crates/operators/src/legacy-bin/tool.rs': 300,
                 'tests/large.rs': 800, 'guide.md': 800}
        for path, limit in paths.items():
            self.write(path, '// line\n' * limit + '// no final newline')
        self.lock()
        self.pair()
        for path in paths:
            self.append(path, '\n')
        self.pair(expected=1, contains=tuple(f'{p} exceeds hard size limit' for p in paths))

    def test_inline_growth_priority_and_debt(self):
        path = 'crates/app/src/lib.rs'
        self.append(path, '#[cfg(test)]\nmod checks {\nfn test_one() {}\n}\n')
        self.pair(expected=1, contains=('module count (1 > 0)',))
        self.commit()
        self.pair('--all', contains=('grandfathered inline',))
        self.write(path, '#[cfg(test)]\nmod checks {\nfn test_one() {}\nfn test_two() {}\n}\n')
        self.pair(expected=1, contains=('body size (4 > 3 lines)',))
        self.write(path, '#[cfg(test)]\nmod checks {\n#[test] fn test_one() {}\n}\n')
        self.pair(expected=1, contains=('test item count (1 > 0)',))

    def test_cfg_forms_attributes_and_test_exemptions(self):
        body = '#[cfg(all(unix, test))]\npub(crate) async fn x() {}\n#[tokio::test] async fn y() {}\n'
        self.write('crates/app/src/new.rs', body)
        self.write('crates/app/src/not.rs', '#[cfg(not(test))]\nmod x {}\n')
        for path in ('tests/fixture.rs', 'crates/app/tests/test.rs', 'crates/app/src/tests.rs',
                     'crates/app/src/a_tests.rs', 'crates/app/src/a_tests/check.rs',
                     'crates/app/src/deep/tests.rs', 'crates/app/src/deep/tests/a.rs',
                     'crates/app/src/deep/tests_more.rs'):
            self.write(path, body)
        self.pair(expected=1, contains=('module count (1 > 0)',))
        (self.root / 'crates/app/src/new.rs').unlink()
        self.pair()

    def test_include_growth_and_debt(self):
        self.append('crates/app/src/lib.rs', 'include!("part.rs");\n')
        self.pair(expected=1, contains=('include! facade sharding (1 > 0)',))
        self.commit()
        self.pair('--all', contains=('1 grandfathered include! shards',))
        self.append('crates/app/src/lib.rs', '// include!("comment");\n/* include!("comment"); */\n')
        self.pair()
        self.append('crates/app/src/lib.rs', 'include ! ("part_two.rs");\n')
        self.pair(expected=1, contains=('include! facade sharding (2 > 1)',))

    def test_quarantined_bin_and_declaration(self):
        self.write('crates/app/src/bin/tool.rs', 'fn main() {}\n')
        self.pair(expected=1, contains=('new bin in a quarantined crate',))
        (self.root / 'crates/app/src/bin/tool.rs').unlink()
        self.append('crates/app/Cargo.toml', '[[bin]]\nname="app-command"\npath="src/lib.rs"\n')
        self.pair(expected=1, contains=('declares bin targets in a quarantined crate',))

    def test_unused_non_utf8_baseline_is_not_decoded(self):
        path = 'crates/app/src/lib.rs'
        self.write(path, b'// old byte \xff\n')
        self.commit()
        self.write(path, '// valid current source\n')
        self.pair()
        self.pair('--all')
