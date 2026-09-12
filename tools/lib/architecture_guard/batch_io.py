"""Invocation-local file and Git inputs. No persisted verdicts or timestamps."""
from pathlib import Path
import subprocess
import sys


def git(*args):
    return subprocess.check_output(['git', *args], stderr=subprocess.PIPE)


def paths_from_stdin():
    return [p.decode() for p in sys.stdin.buffer.read().split(b'\0') if p]


def text_content(data, errors='strict'):
    # Match Python's existing text-mode scanners, including universal newlines.
    return data.decode('utf-8', errors=errors).replace('\r\n', '\n').replace('\r', '\n')


def baseline_files(ref, paths):
    entries = {}
    for entry in git('ls-tree', '-r', '-z', ref).split(b'\0'):
        if entry:
            header, path = entry.split(b'\t', 1)
            entries[path.decode('utf-8', errors='surrogateescape')] = header.split()[2]
    wanted = {p: entries[p] for p in paths if p in entries}
    # Object IDs avoid cat-file's line-based path quoting protocol entirely.
    objects = list(dict.fromkeys(wanted.values()))
    result = subprocess.run(['git', 'cat-file', '--batch'], input=b''.join(o + b'\n' for o in objects), stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    offset = 0
    contents = {}
    for oid in objects:
        end = result.stdout.index(b'\n', offset)
        actual, kind, size = result.stdout[offset:end].split()
        if actual != oid or kind != b'blob':
            raise ValueError('unexpected Git baseline object')
        offset = end + 1
        contents[oid] = result.stdout[offset:offset + int(size)]
        offset += int(size) + 1
    if offset != len(result.stdout):
        raise ValueError('unexpected Git baseline batch output')
    return entries, {p: contents[oid] for p, oid in wanted.items()}


def emit(kind, message):
    print(f'{kind} {message}')


def run(main):
    try:
        main()
    except (OSError, ValueError, UnicodeError, RuntimeError, subprocess.SubprocessError) as error:
        print(f'[architecture:guard] helper error: {error}', file=sys.stderr)
        raise SystemExit(2)
