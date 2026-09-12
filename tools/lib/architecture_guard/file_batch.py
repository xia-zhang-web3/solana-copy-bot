"""Batch the size, inline-test, include and quarantined-bin checks."""
from pathlib import Path
import subprocess
import sys

from batch_io import baseline_files, emit, paths_from_stdin, run, text_content
from rust_metrics import include_count, inline_metrics, limit, production


def main():
    mode, ref = sys.argv[1:]
    paths = paths_from_stdin()
    current = {}
    wanted = []
    for path in paths:
        if not Path(path).is_file():
            continue
        if path.endswith(('.rs', '.md', '.sh', '.py')):
            data = Path(path).read_bytes()
            current[path] = data
            if path.endswith('.rs'):
                wanted.append(path)
    entries, baseline = baseline_files(ref, wanted)
    for path in paths:
        if path in current:
            data = current[path]
            lines = data.count(b'\n')  # wc -l, not splitlines / logical lines.
            maximum = limit(path)
            if lines > maximum:
                emit('FAIL', f'{path} exceeds hard size limit ({lines} > {maximum} LOC)')
            if path.endswith('.rs'):
                text = text_content(data)
                old = baseline.get(path, b'')
                if production(path):
                    check_inline(path, text, old, mode)
                count = include_count(text)
                if count:
                    previous = include_count(text_content(old))
                    if count > previous:
                        emit('FAIL', f'{path} increases include! facade sharding ({count} > {previous})')
                    elif mode == '--all':
                        emit('DEBT', f'{path} has {count} grandfathered include! shards')
        if any(path.startswith(f'crates/{crate}/src/bin/') for crate in ('app', 'discovery', 'storage')) and path not in entries:
            emit('FAIL', f'{path} is a new bin in a quarantined crate; operators must live outside app/discovery/storage monoliths')
        if path in {f'crates/{crate}/Cargo.toml' for crate in ('app', 'discovery', 'storage')} and Path(path).is_file():
            result = subprocess.run(['grep', '-Eq', r'^[[:space:]]*\[\[[[:space:]]*bin[[:space:]]*\]\]', path], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            if result.returncode not in (0, 1):
                raise RuntimeError(f'failed to scan Cargo bin declarations: {path}')
            if result.returncode == 0:
                emit('FAIL', f'{path} declares bin targets in a quarantined crate')


def check_inline(path, text, old, mode):
    count, lines, tests = inline_metrics(text)
    if not (count or lines or tests):
        return
    previous, old_lines, old_tests = inline_metrics(text_content(old))
    if count > previous:
        emit('FAIL', f'{path} increases inline #[cfg(test)] module count ({count} > {previous})')
    elif lines > old_lines:
        emit('FAIL', f'{path} increases inline #[cfg(test)] body size ({lines} > {old_lines} lines)')
    elif tests > old_tests:
        emit('FAIL', f'{path} increases inline test item count ({tests} > {old_tests})')
    elif mode == '--all':
        emit('DEBT', f'{path} has {count} grandfathered inline test module(s), {lines} inline test body line(s), {tests} inline test item(s)')


if __name__ == '__main__':
    run(main)
