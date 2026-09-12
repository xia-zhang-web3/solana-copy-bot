#!/usr/bin/env python3
"""Discover all tools tests, partition by sorted ID, and execute one bounded shard."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--shard-index', type=int, required=True)
    parser.add_argument('--shard-count', type=int, required=True)
    parser.add_argument('--start-directory', type=Path, default=ROOT / 'tools/tests')
    parser.add_argument('--inventory-only', action='store_true')
    parser.add_argument('--inventory', type=Path)
    parser.add_argument('--timeout-seconds', type=float, default=240)
    args = parser.parse_args(argv)
    if args.shard_count <= 0 or not 0 <= args.shard_index < args.shard_count:
        parser.error('require shard-count > 0 and 0 <= shard-index < shard-count')
    if not 0 < args.timeout_seconds <= 240:
        parser.error('timeout-seconds must be in (0, 240]')
    return args


def instances(suite):
    for test in suite:
        if isinstance(test, unittest.TestSuite):
            yield from instances(test)
        else:
            yield test


def discover(directory):
    loader = unittest.TestLoader()
    suite = loader.discover(str(directory.resolve()), pattern='test_*.py')
    tests = list(instances(suite))
    if loader.errors or any(isinstance(t, unittest.loader._FailedTest) for t in tests):
        raise ValueError('unittest discovery failed: ' + '\n'.join(loader.errors))
    if not tests:
        raise ValueError('unittest discovery found an empty full corpus')
    ids = [test.id() for test in tests]
    if any(not isinstance(value, str) or not value for value in ids):
        raise ValueError('test IDs must be nonempty strings')
    if len(ids) != len(set(ids)):
        raise ValueError('duplicate test IDs make the inventory ambiguous')
    return sorted(zip(ids, tests), key=lambda entry: entry[0])


def partition(tests, count):
    # Strict policy: reject a small corpus that would leave ANY shard empty.
    if count > len(tests):
        raise ValueError('shard count exceeds corpus size; empty shards are forbidden')
    return [tests[index::count] for index in range(count)]


def worker():
    try:
        args = arguments()
        shards = partition(discover(args.start_directory), args.shard_count)
        inventory = {'total_count': sum(map(len, shards)), 'shard_count': args.shard_count,
                     'shard_index': args.shard_index,
                     'shards': [[identifier for identifier, test in shard] for shard in shards]}
        data = json.dumps(inventory, indent=2) + '\n'
        if args.inventory:
            args.inventory.write_text(data)
        print(data, end='', flush=True)
        if args.inventory_only:
            return 0
        selected = [test for identifier, test in shards[args.shard_index]]
        result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(selected))
        if result.testsRun != len(selected):
            raise ValueError('runner did not execute every selected test instance')
        return 0 if result.wasSuccessful() else 1
    except BaseException as error:
        # Even SystemExit(0) raised while importing a test is a failed discovery.
        print(f'tools-test-shard error: {type(error).__name__}: {error}', file=sys.stderr)
        return 2


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    args = arguments(argv)
    # Supervise a separate process group: tests may use SIGALRM themselves and
    # must not replace/cancel the shard deadline or leave subprocesses running.
    code = ('import sys; sys.path.insert(0, sys.argv.pop(1)); '
            'from run_tool_tests import worker; raise SystemExit(worker())')
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1')
    child = subprocess.Popen([sys.executable, '-B', '-c', code, str(Path(__file__).parent), *argv],
                             env=env, start_new_session=True)
    try:
        return child.wait(timeout=args.timeout_seconds)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        child.wait()
        print(f'tools-test-shard timeout after {args.timeout_seconds:g}s', file=sys.stderr)
        return 124


if __name__ == '__main__':
    raise SystemExit(main())
