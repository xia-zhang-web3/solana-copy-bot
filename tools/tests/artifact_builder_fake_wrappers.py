"""Closed-PATH env/timeout controls for the synthetic artifact builder tests."""
import json
import os
from pathlib import Path
import subprocess
import sys


def record(tool, argv):
    with open(os.environ['FIXTURE_TRACE'], 'a') as output:
        output.write(json.dumps({'tool': tool, 'argv': argv}) + '\n')


def main():
    tool, argv = Path(sys.argv[0]).name, sys.argv[1:]
    record(tool, argv)
    if tool == 'env':
        expected = ['-u', 'FRACTIONAL_FIXTURE_ROOT', 'RUST_MIN_STACK=8388608',
                    'RUST_TEST_NOCAPTURE=1', 'RUST_BACKTRACE=1', 'timeout']
        if argv[:len(expected)] != expected:
            return invalid(tool, argv)
        child_env = dict(os.environ)
        child_env.pop('FRACTIONAL_FIXTURE_ROOT', None)
        child_env.update(RUST_MIN_STACK='8388608', RUST_TEST_NOCAPTURE='1',
                         RUST_BACKTRACE='1')
        return subprocess.run(argv[5:], env=child_env, check=False).returncode
    if tool == 'timeout':
        if argv[:3] != ['--signal=KILL', '1200s', 'cargo']:
            return invalid(tool, argv)
        try:
            return subprocess.run(argv[2:], timeout=1200, check=False).returncode
        except subprocess.TimeoutExpired:
            return 124
    return invalid(tool, argv)


def invalid(tool, argv):
    record('unexpected', [tool, *argv])
    print(f'unexpected synthetic {tool} argv: {argv!r}', file=sys.stderr)
    return 86


if __name__ == '__main__':
    raise SystemExit(main())
