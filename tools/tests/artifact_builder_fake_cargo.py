"""Strict synthetic Cargo/rustc for artifact builder command-flow tests only."""
import json
import os
from pathlib import Path
import struct
import sys


def record(tool, **fields):
    with open(os.environ['FIXTURE_TRACE'], 'a') as output:
        output.write(json.dumps({'tool': tool, **fields}) + '\n')


def main():
    tool, args = Path(sys.argv[0]).name, sys.argv[1:]
    record(tool, argv=args)
    settings = json.loads(Path(os.environ['FIXTURE_SETTINGS']).read_text())
    if tool == 'rustc' and args == ['--version']:
        print('rustc synthetic-fixture (no compiler)')
        return 0
    if tool != 'cargo':
        return unexpected(tool, args)
    if args == ['metadata', '--locked', '--format-version=1', '--no-deps']:
        history = [json.loads(line) for line in Path(os.environ['FIXTURE_TRACE']).read_text().splitlines()]
        count = sum(e['tool'] == 'cargo' and e.get('argv') == args for e in history)
        mode = settings.get('second_metadata', 'valid') if count == 2 else 'valid'
        record('metadata_response', call=count, mode=mode)
        if mode == 'failure':
            print('synthetic classification metadata failure (41)', file=sys.stderr)
            return 41
        if mode == 'malformed':
            print('{invalid JSON')
            return 0
        if mode == 'unknown-package':
            print(json.dumps({'packages': []}))
            return 0
        if mode == 'invalid-shape':
            print(json.dumps({'packages': [{'name': settings['package']}]}))
            return 0
        if mode == 'invalid-kind':
            print(json.dumps({'packages': [{'name': settings['package'], 'targets': [{'kind': 'bin'}]}]}))
            return 0
        if mode != 'valid':
            return unexpected(tool, args + [mode])
        print(json.dumps(settings['metadata']))
        return 0
    if args == ['--version']:
        print('cargo synthetic-fixture (no compilation)')
        return 0
    package = settings['package']
    prefix = ['test', '--locked', '-p', package]
    selectors = [['--bin', 'copybot-app']] if package == 'copybot-app' else [
        ['--lib' if settings['has_lib'] else '--bins'], ['--tests']]
    if settings['has_lib']:
        selectors += [['--bins']]  # Cargo permits this; tests must detect silent misclassification.
    if args in [prefix + selector + ['--', '--test-threads=1'] for selector in selectors]:
        return settings.get('integration_exit', 0) if '--tests' in args else 0
    profile = 'release' if package == 'copybot-app' else 'operator-release'
    build = ['build', '--locked', '--target-dir', os.environ['CARGO_TARGET_DIR'],
             '--profile', profile, '--target', 'x86_64-unknown-linux-gnu', '-p', package]
    for binary in settings['bins']:
        build += ['--bin', binary]
    if args != build:
        return unexpected(tool, args)
    if settings.get('build_exit'):
        return settings['build_exit']
    destination = Path(os.environ['CARGO_TARGET_DIR']) / 'x86_64-unknown-linux-gnu' / profile
    destination.mkdir(parents=True)
    # ELF64 x86-64 header plus a label, never executed or represented as release proof.
    ident = b'\x7fELF\x02\x01\x01' + b'\0' * 9
    payload = struct.pack('<16sHHIQQQIHHHHHH', ident, 2, 62, 1, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0)
    for binary in settings['bins']:
        path = destination / binary
        path.write_bytes(payload + b'SYNTHETIC COMMAND-FLOW FIXTURE\n')
        path.chmod(0o755)
    return 0


def unexpected(tool, args):
    record('unexpected', executable=tool, argv=args)
    print(f'unexpected synthetic {tool} argv: {args!r}', file=sys.stderr)
    return 86


if __name__ == '__main__':
    try:
        code = main()
    except Exception as error:
        record('stub_error', error=f'{type(error).__name__}: {error}')
        print(f'synthetic Cargo stub error: {error}', file=sys.stderr)
        code = 87
    raise SystemExit(code)
