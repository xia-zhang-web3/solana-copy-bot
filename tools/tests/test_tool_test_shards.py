"""Discovery, exact partition and causal runner success/failure contracts."""
import json
import os
import subprocess
import time

from tool_shard_fixture import ShardCase


class ToolTestShards(ShardCase):
    def sample(self, body='pass'):
        self.write('import unittest\nclass Sample(unittest.TestCase):\n    def test_run(self):\n        ' + body + '\n')

    def test_partition_and_automatic_new_tests_execute_once(self):
        log = self.directory / 'executed.jsonl'
        source = 'import unittest\nclass Cases(unittest.TestCase):\n'
        for index in range(7):
            source += f'    def test_{index}(self):\n        with open({str(log)!r}, "a") as out: out.write(self.id() + "\\n")\n'
        self.write(source)
        inventory = self.inventory(0, 3)
        self.assertFalse(log.exists(), 'inventory-only executed test bodies')
        self.assertEqual(inventory['total_count'], 7)
        for index in range(3):
            repeated = self.inventory(index, 3)
            self.assertEqual(repeated['shards'], inventory['shards'])
            result = self.shard(index, 3)
            self.assertEqual(result.returncode, 0, result.stderr)
        actual = log.read_text().splitlines()
        expected = sorted(value for shard in inventory['shards'] for value in shard)
        self.assertEqual(sorted(actual), expected)
        self.assertEqual(len(actual), len(set(actual)))
        self.write('import unittest\nclass Extra(unittest.TestCase):\n    def test_added(self): pass\n', 'test_added.py')
        self.assertEqual(self.inventory(0, 3)['total_count'], 8)
        self.assertEqual(len(expected), 7)

    def test_real_failure_and_positive_control(self):
        marker = self.directory / 'executed'
        self.sample(f'open({str(marker)!r}, "w").close(); self.fail("causal failure")')
        result = self.shard()
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertTrue(marker.exists())
        self.assertIn('causal failure', result.stderr)
        marker.unlink()
        self.sample(f'open({str(marker)!r}, "w").close(); self.assertEqual(2 + 2, 4)')
        result = self.shard()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(marker.exists())
        self.assertIn('Ran 1 test', result.stderr)

    def test_test_error_is_not_success(self):
        self.sample('raise RuntimeError("causal error")')
        result = self.shard()
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn('FAILED (errors=1)', result.stderr)

    def test_import_syntax_and_system_exit_errors(self):
        for source in ('raise ImportError("broken import")\n', 'def broken(:\n', 'raise SystemExit(0)\n'):
            with self.subTest(source=source):
                self.write(source)
                result = self.shard(0, 1, '--inventory-only')
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('tools-test-shard error:', result.stderr)

    def test_loader_errors_are_not_ignored(self):
        self.write('import unittest\ndef load_tests(loader, suite, pattern):\n    loader.errors.append("loader error")\n    return suite\n')
        result = self.shard()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('discovery failed', result.stderr)

    def test_failed_test_without_loader_errors_is_rejected(self):
        self.write('import unittest\ndef load_tests(loader, suite, pattern):\n    return unittest.TestSuite([unittest.loader._FailedTest("broken", RuntimeError("broken"))])\n')
        result = self.shard(0, 1, '--inventory-only')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('discovery failed', result.stderr)

    def test_duplicate_ids_fail_instead_of_dedup(self):
        self.sample()
        with (self.directory / 'test_sample.py').open('a') as out:
            out.write('def load_tests(loader, suite, pattern):\n    return unittest.TestSuite([suite, suite])\n')
        result = self.shard(0, 1, '--inventory-only')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('duplicate test IDs', result.stderr)

    def test_empty_corpus_and_small_corpus_policy(self):
        result = self.shard(0, 1, '--inventory-only')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('empty full corpus', result.stderr)
        self.sample()
        for index in (0, 1):
            result = self.shard(index, 2, '--inventory-only')
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('empty shards are forbidden', result.stderr)
        self.assertEqual(self.inventory()['total_count'], 1)

    def test_invalid_arguments(self):
        self.sample()
        for args in ((), ('--shard-index', '0'), ('--shard-index', '-1', '--shard-count', '3'),
                     ('--shard-index', '3', '--shard-count', '3'), ('--shard-index', '0', '--shard-count', '0'),
                     ('--shard-index', 'one', '--shard-count', '3')):
            with self.subTest(args=args):
                self.assertEqual(self.run_cli(*args).returncode, 2)
        for value in ('0', '-1', '241', 'nan', 'inf'):
            self.assertEqual(self.shard(0, 1, '--timeout-seconds', value).returncode, 2)

    def test_inventory_write_failure_is_not_success(self):
        self.sample()
        result = self.shard(0, 1, '--inventory', str(self.directory / 'missing/inventory.json'))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('tools-test-shard error:', result.stderr)

    def test_nested_suites_and_inventory_file(self):
        self.sample()
        with (self.directory / 'test_sample.py').open('a') as out:
            out.write('def load_tests(loader, suite, pattern):\n    return unittest.TestSuite([unittest.TestSuite([suite])])\n')
        path = self.directory / 'inventory.json'
        result = self.shard(0, 1, '--inventory', str(path))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(path.read_text()), json.loads(result.stdout))
        self.assertIn('Ran 1 test', result.stderr)

    def test_timeout_stops_real_test_and_its_descendant(self):
        pidfile = self.directory / 'child.pid'
        self.sample('import subprocess, sys, time; '
                    'child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(100)"]); '
                    f'open({str(pidfile)!r}, "w").write(str(child.pid)); time.sleep(100)')
        started = time.monotonic()
        result = self.shard(0, 1, '--timeout-seconds', '1.5')
        self.assertEqual(result.returncode, 124, result.stderr)
        self.assertLess(time.monotonic() - started, 6)
        self.assertTrue(pidfile.exists(), 'the real test body did not start')
        pid = int(pidfile.read_text())
        state = subprocess.run(['ps', '-o', 'stat=', '-p', str(pid)], capture_output=True, text=True).stdout.strip()
        self.assertTrue(not state or state.startswith('Z'), 'test descendant is still running: ' + state)
