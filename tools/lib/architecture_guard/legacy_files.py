"""Read the legacy file corpus once; preserve grep and normalized scanners."""
from pathlib import Path
import re
import subprocess
import sys

from batch_io import emit, git, paths_from_stdin, run, text_content
from rust_metrics import is_test

PATTERNS = ('\x35\x2dday', '\x66ull\x2dwindow\x20proof', '\x61ccepted\x20scoring')
RAW = 'raw_\x77indow'
RAW_GREP = 'published_scoring_source.*' + RAW + '([^[:alnum:]_]|$)'
RAW_NORMALIZED = re.compile('published_scoring_source.*' + RAW + '[^a-z0-9_]')
NORMALIZE = re.compile(r'[\s"+,()]')


def norm(value):
    return NORMALIZE.sub('', value).replace(chr(39), '').lower()


def grep(pattern, path, fixed=False):
    args = ['grep', '-IF' if fixed else '-E', pattern, path]
    result = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    if result.returncode not in (0, 1):
        raise RuntimeError(f'grep failed for {path}: exit {result.returncode}')
    return result.returncode == 0


def normalized_raw(lines):
    # A matching window must contain the unchanged literal prefix. Only omit
    # impossible windows; keep every start within the original 16-line bound.
    prefix = 'published_scoring_source'
    if prefix not in ''.join(lines):
        return False
    # The prefix itself can be split across lines, so inspect all windows here.
    for index in range(len(lines)):
        if RAW_NORMALIZED.search(''.join(lines[index:index + 16]) + '!'):
            return True
    return False


def is_tracked(path, tracked):
    if path.encode() in tracked:
        return True
    # Preserve git ls-files pathspec behavior for unusual literal filenames.
    if any(char in path for char in '*?[\\:'):
        result = subprocess.run(['git', 'ls-files', '--error-unmatch', path], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        if result.returncode not in (0, 1):
            raise RuntimeError('failed to inspect tracked pathspec')
        return result.returncode == 0
    return False


def main():
    mode = sys.argv[1]
    paths = paths_from_stdin()
    tracked = set(git('ls-files', '-z').split(b'\0')) if mode == '--changed' else set()
    findings = [[] for _ in range(len(PATTERNS) + 1)]
    verb = 'adds' if mode == '--changed' else 'contains'
    for path in paths:
        if not Path(path).is_file() or (mode == '--changed' and is_tracked(path, tracked)):
            continue
        data = Path(path).read_bytes()
        text = text_content(data, errors='ignore')
        normalized = norm(text) + '!'
        for index, pattern in enumerate(PATTERNS):
            if pattern.encode() in data and grep(pattern, path, fixed=True):
                findings[index].append(f'{path} {verb} a forbidden legacy Stage 3 marker')
            if norm(pattern) in normalized:
                findings[index].append(f'{path} {verb} a split forbidden legacy Stage 3 marker')
        if mode == '--all' and is_test(path):
            continue
        if b'published_scoring_source' in data and RAW.encode() in data and grep(RAW_GREP, path):
            findings[-1].append(f'{path} {verb} legacy publication-source readiness wiring')
        # Iterating a text file splits on newline, unlike str.splitlines().
        lines = [norm(line) for line in text.split('\n')]
        if text.endswith('\n'):
            lines.pop()
        if normalized_raw(lines):
            findings[-1].append(f'{path} {verb} split legacy publication-source readiness wiring')
    for group in findings:
        for message in group:
            emit('FAIL', message)


if __name__ == '__main__':
    run(main)
