"""Contracts execute the aggregate shell extracted from the actual workflow."""
import itertools
import json
import os
import re
import subprocess
import unittest

from tool_shard_fixture import ROOT, record

WORKFLOW = ROOT / '.github/workflows/architecture-guard.yml'
BEFORE = ROOT / 'tools/tests/tool_shard_fixtures/architecture_guard_before.yml'


def job(source, name):
    match = re.search(r'^  ' + re.escape(name) + r':\n.*?(?=^  \S|\Z)', source, re.M | re.S)
    if not match:
        raise AssertionError('missing job: ' + name)
    return match.group().rstrip()


class ToolWorkflow(unittest.TestCase):
    def test_triggers_permissions_and_exact_guard_job_are_preserved(self):
        before, after = BEFORE.read_text(), WORKFLOW.read_text()
        self.assertEqual(before.split('jobs:\n')[0], after.split('jobs:\n')[0])
        original = job(before, 'architecture-guard').split('      - name: Tool regression tests')[0].rstrip()
        self.assertEqual(job(after, 'guard'), original.replace('  architecture-guard:', '  guard:', 1))
        self.assertEqual(after.count('permissions:'), 1)
        self.assertNotIn('continue-on-error', after)

    def test_matrix_uses_real_runner_and_preserves_job_budgets(self):
        source = WORKFLOW.read_text()
        self.assertEqual(re.findall(r'^  ([\w-]+):$', source.split('jobs:\n')[1], re.M),
                         ['guard', 'tools-tests', 'architecture-guard'])
        self.assertEqual(re.findall(r'^    timeout-minutes: (.+)$', source, re.M), ['5', '5', '5'])
        tests = job(source, 'tools-tests')
        self.assertIn('      fail-fast: false\n      matrix:\n        shard: [0, 1, 2]', tests)
        self.assertIn('      - uses: actions/checkout@v4', tests)
        commands = re.findall(r'^        run: (.+)$', tests, re.M)
        self.assertEqual(commands, ['python3 tools/run_tool_tests.py --shard-index ${{ matrix.shard }} --shard-count 3'])
        self.assertNotIn('if:', tests)
        self.assertNotIn('--inventory-only', tests)

    def test_required_check_identity_and_all_needs_are_wired(self):
        aggregate = job(WORKFLOW.read_text(), 'architecture-guard')
        for field in ('    name: architecture-guard', '    if: ${{ always() }}',
                      '    needs: [guard, tools-tests]', '          NEEDS_JSON: ${{ toJSON(needs) }}',
                      '        shell: bash'):
            self.assertIn(field, aggregate)
        self.assertEqual(aggregate.count('      - name:'), 1)

    def test_actual_aggregate_script_rejects_every_non_success_result(self):
        aggregate = job(WORKFLOW.read_text(), 'architecture-guard')
        body = aggregate.split('        run: |\n')[1]
        self.assertTrue(all(not line or line.startswith('          ') for line in body.splitlines()))
        script = '\n'.join(line[10:] for line in body.splitlines()) + '\n'
        statuses = ('success', 'failure', 'skipped', 'cancelled', 'unknown', '', None)
        for guard, tests in itertools.product(statuses, repeat=2):
            with self.subTest(guard=guard, tests=tests):
                needs = {'guard': {'result': guard}, 'tools-tests': {'result': tests}}
                result = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', script],
                                        env=dict(os.environ, NEEDS_JSON=json.dumps(needs)),
                                        text=True, capture_output=True, timeout=5)
                record(needs, result)
                self.assertEqual(result.returncode, 0 if guard == tests == 'success' else 1, result.stderr)
        for value in ('{}', '{"guard":{"result":"success"}}', 'invalid-json',
                      '{"guard":{"result":"success"},"tools-tests":{"result":"success"},"extra":{}}'):
            result = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', script],
                                    env=dict(os.environ, NEEDS_JSON=value), text=True, capture_output=True, timeout=5)
            record(value, result)
            self.assertNotEqual(result.returncode, 0)
