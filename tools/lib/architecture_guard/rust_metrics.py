"""The existing textual Rust heuristics, evaluated together on one read."""
from fnmatch import fnmatchcase
import re

TEST_PATHS = (
    'tests/*.rs', '*/tests/*.rs', '*/src/tests.rs', '*/src/*_tests.rs',
    '*/src/*_tests/*.rs', '*/src/*/tests.rs', '*/src/*/tests/*.rs',
    '*/src/*/tests_*.rs',
)
BIN_PATHS = (
    'crates/*/src/main.rs', '*/src/bin/*.rs', '*/src/bin/*/main.rs',
    '*/src/legacy-bin/*.rs',
)
CFG = re.compile(r'#\s*\[\s*cfg\s*\((?P<body>.*?)\)\s*\]', re.S)
ITEM = re.compile(
    r'(?:#\s*\[[^\]]*\]\s*)*(?:(?:pub(?:\s*\([^)]*\))?\s+)?'
    r'(?:async\s+)?(?:mod|fn)\s+[A-Za-z_][A-Za-z0-9_]*)', re.S,
)


def matches(path, patterns):
    return any(fnmatchcase(path, pattern) for pattern in patterns)


def is_test(path):
    return matches(path, TEST_PATHS)


def limit(path):
    if matches(path, BIN_PATHS):
        return 300
    return 800 if is_test(path) or path.endswith('.md') else 600


def production(path):
    return fnmatchcase(path, 'crates/*/src/*.rs') and not is_test(path)


def matching_brace_end(value, brace_index):
    depth = 0
    for index in range(brace_index, len(value)):
        if value[index] == '{':
            depth += 1
        elif value[index] == '}':
            depth -= 1
            if depth == 0:
                return index
    return len(value)


def inline_metrics(text):
    count = body_lines = 0
    for match in CFG.finditer(text):
        compact = re.sub(r'\s+', '', match.group('body'))
        if 'not(test)' in compact or not re.search(r'(^|[(),])test($|[(),])', compact):
            continue
        tail_offset = match.end()
        stripped = text[tail_offset:].lstrip()
        if not ITEM.match(stripped):
            continue
        count += 1
        item_start = tail_offset + len(text[tail_offset:]) - len(stripped)
        semicolon = text.find(';', item_start)
        brace = text.find('{', item_start)
        if brace == -1 or (semicolon != -1 and semicolon < brace):
            continue
        end = matching_brace_end(text, brace)
        body_lines += text[item_start:end + 1].count('\n') + 1
    tests = 0
    for match in re.finditer(r'#\s*\[(.*?)\]', text, flags=re.S):
        attr = re.sub(r'\s+', '', match.group(1))
        if attr.startswith('test') or attr.startswith('tokio::test'):
            tests += 1
    return count, body_lines, tests


def include_count(text):
    text = re.sub(r'//.*?$', '', text, flags=re.M)
    text = re.sub(r'/\*.*?\*/', '', text, flags=re.S)
    text = re.sub(r'"(?:\\.|[^"\\])*"', '""', text)
    text = re.sub(r'r#*\".*?\"#*', 'r""', text, flags=re.S)
    return len(re.findall(r'(?<![A-Za-z0-9_])include\s*!\s*\(', text))
