"""Small subprocess fixtures for the real tools shard CLI."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / 'tools/run_tool_tests.py'


def record(label, result):
    path = os.environ.get('TOOL_SHARD_CONTRACT_EVIDENCE')
    if path:
        with open(path, 'a') as out:
            out.write(json.dumps({'label': label, 'exit': result.returncode,
                                  'stdout': result.stdout, 'stderr': result.stderr}) + '\n')
    return result


class ShardCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='tools-shard-contract-')
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1', CARGO_NET_OFFLINE='true')

    def write(self, source, name='test_sample.py'):
        (self.directory / name).write_text(source)

    def run_cli(self, *args):
        result = subprocess.run([sys.executable, '-B', str(RUNNER), '--start-directory',
                               str(self.directory), *args], cwd=ROOT, env=self.env,
                              input='', text=True, capture_output=True, timeout=10)
        return record(list(args), result)

    def shard(self, index=0, count=1, *args):
        return self.run_cli('--shard-index', str(index), '--shard-count', str(count), *args)

    def inventory(self, index=0, count=1):
        result = self.shard(index, count, '--inventory-only')
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)
