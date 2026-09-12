"""Checked cache reader: exit 0 plus match/no-match; every failure is nonzero."""
import io
import re
import subprocess
import sys


def output(args, content=None, allowed=(0,)):
    result = subprocess.run(args, input=content, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode not in allowed:
        raise RuntimeError(f'{args[0]} exited {result.returncode}')
    return result.stdout


def literal_match(content, pattern, regex):
    # Preserve grep's literal/ERE and binary-input behavior. Fully buffered steps
    # keep a consumer's normal early exit from interrupting the cache reader.
    content = output(['grep', '-v', '-F', '--', '+++'], content, (0, 1))
    content = output(['grep', '-v', '-F', '--', '---'], content, (0, 1))
    if not regex:
        content = output(['grep', '-F', pattern], content, (0, 1))
    expression = r'^\+[^+].*' + pattern if regex else r'^\+[^+]'
    result = subprocess.run(['grep', '-E', expression], input=content,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode not in (0, 1):
        raise RuntimeError(f'grep exited {result.returncode}')
    return result.returncode == 0


def norm(value):
    return re.sub(r'[\s"+,()]', '', value).replace(chr(39), '').lower()


def normalized_match(content, pattern, regex):
    pattern = norm(pattern)
    expression = re.compile(pattern) if regex else None
    hunk_old = []
    hunk_new = []

    def flush_hunk():
        old = norm(''.join(hunk_old)) + '!'
        new = norm(''.join(hunk_new)) + '!'
        if expression is not None:
            return bool(expression.search(new) and not expression.search(old))
        return bool(pattern and pattern in new and pattern not in old)

    # Match the former Python stdin decoder and universal newline handling.
    for line in io.StringIO(content.decode(sys.stdin.encoding, sys.stdin.errors), newline=None):
        if line.startswith(('diff ', 'index ', '---', '+++')):
            continue
        if line.startswith('@@'):
            if flush_hunk():
                return True
            hunk_old = []
            hunk_new = []
            continue
        if line.startswith(' '):
            hunk_old.append(line[1:])
            hunk_new.append(line[1:])
        elif line.startswith('-'):
            hunk_old.append(line[1:])
        elif line.startswith('+'):
            hunk_new.append(line[1:])
        else:
            if flush_hunk():
                return True
            hunk_old = []
            hunk_new = []
    return flush_hunk()


def main():
    path, scanner, pattern = sys.argv[1:]
    # Check the actual read, including a failure after partial output, before any
    # scanner can return. A readable/existing-file precheck cannot replace this.
    content = output(['cat', path])
    if scanner in ('literal', 'regex'):
        found = literal_match(content, pattern, scanner == 'regex')
    elif scanner in ('normalized-pattern', 'normalized-regex'):
        found = normalized_match(content, pattern, scanner == 'normalized-regex')
    else:
        raise ValueError('unknown scanner')
    print("match" if found else "no-match")
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError) as error:
        # Do not expose raw diff, pattern, helper stderr or file contents.
        print(f'[architecture:guard] diff scanner error ({type(error).__name__})', file=sys.stderr)
        raise SystemExit(2)
