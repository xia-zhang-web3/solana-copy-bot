from architecture_guard_fixtures.harness import GuardCase

PATTERNS = ('\x35\x2dday', '\x66ull\x2dwindow\x20proof', '\x61ccepted\x20scoring')
RAW = 'raw_\x77indow'
PREFIX = 'published_scoring_' + 'source'
BUILD = 'cargo ' + 'build --release'


class Legacy(GuardCase):
    def test_literal_split_normalized_untracked_binary(self):
        for i, pattern in enumerate(PATTERNS):
            self.write(f'notes{i}.txt', pattern + '\n')
            self.write(f'split{i}.txt', '"' + '",\n"'.join(pattern.upper()) + '"\n')
            self.write(f'binary{i}.data', b'\x00\xff' + ('"' + '",\n"'.join(pattern) + '"').encode())
        self.pair(expected=1, contains=('adds a forbidden legacy', 'adds a split forbidden legacy'))
        self.pair('--all', expected=1, contains=('binary0.data contains a split',))

    def test_unchanged_and_new_markers_in_diffs(self):
        self.write('notes.txt', PATTERNS[0] + '\n')
        self.commit()
        self.append('notes.txt', 'ordinary new text\n')
        self.pair()
        self.append('notes.txt', PATTERNS[1] + '\n')
        self.pair(expected=1, contains=('changed diff adds a forbidden',))
        self.git('add', 'notes.txt')
        self.pair(expected=1)
        self.pair('--all', expected=1)

    def test_split_diff_and_range_dirty(self):
        self.write('notes.txt', 'ordinary\n')
        a = self.commit()
        self.append('notes.txt', '"' + '",\n"'.join(PATTERNS[2].split()) + '"\n')
        b = self.commit()
        self.pair(expected=1, ARCH_GUARD_DIFF_RANGE=a + '..' + b)
        self.write('extra.txt', PATTERNS[0] + '\n')
        self.pair(expected=1, ARCH_GUARD_DIFF_RANGE=a + '..' + b)
        self.pair(expected=1, ARCH_GUARD_DIFF_RANGE=a + '..' + b, GITHUB_ACTIONS='true')

    def test_raw_source_windows_and_test_exemption(self):
        for gap in (14, 15):
            self.write(f'window{gap}.txt', PREFIX + '\n' + 'x\n' * gap + RAW + '\n')
        self.write('tests/raw.rs', PREFIX + ' = "' + RAW + '";\n')
        self.pair(expected=1)
        result = self.pair('--all', expected=1, contains=('window14.txt contains split',))
        self.assertNotIn('window15.txt contains', result.stderr)
        self.assertNotIn('tests/raw.rs contains', result.stderr)

    def test_raw_source_diff_binary_and_prefix_splits(self):
        self.write('raw.txt', 'ordinary\n')
        self.commit()
        self.append('raw.txt', PREFIX + ' = "' + RAW + '";\n')
        self.write('binary.data', b'\x00\xff' + (PREFIX + ' = "' + RAW + '";\n').encode())
        self.write('split.txt', '"published_scoring_",\n"source"\n"' + RAW + '"\n')
        self.pair(expected=1, contains=('publication-source readiness wiring',))
        self.pair('--all', expected=1)

    def test_document_added_wording_and_exceptions(self):
        self.append('README.md', 'Run ' + BUILD + '\n')
        self.pair(expected=1, contains=('adds production-local',))
        self.git('add', 'README.md')
        self.pair(expected=1)
        self.write('README.md', 'artifact ' + BUILD + '\n')
        self.git('add', 'README.md')
        contexts = ('emergency', 'fallback', 'artifact', 'builder', 'CI', 'forbidden',
                    'rejected', 'off production', 'off-server', '--profile operator-release')
        for index, context in enumerate(contexts):
            self.write(f'docs/context{index}.md', context + ' ' + BUILD + '\n')
        self.pair()
        self.write('ops/new.md', 'Run ' + BUILD + '\n')
        self.write('audit/historical.md', 'Run ' + BUILD + '\n')
        self.pair(expected=1, contains=('ops/new.md contains',))

    def test_document_old_wording_changed_and_range_ci(self):
        self.append('README.md', 'Run ' + BUILD + '\n')
        a = self.commit()
        self.append('README.md', 'ordinary\n')
        b = self.commit()
        self.pair()
        self.pair(ARCH_GUARD_DIFF_RANGE=a + '..' + b)
        self.pair('--all', expected=1)
        self.append('README.md', 'Again ' + BUILD + '\n')
        self.pair(expected=1, ARCH_GUARD_DIFF_RANGE=a + '..' + b)
        self.pair(ARCH_GUARD_DIFF_RANGE=a + '..' + b, GITHUB_ACTIONS='true')
