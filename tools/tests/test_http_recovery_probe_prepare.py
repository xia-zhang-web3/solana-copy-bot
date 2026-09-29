"""Preparation preserves current carryover and copies an existing HTTP config once."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SOURCE = Path(__file__).resolve().parents[1] / 'http_recovery_probe/prepare.py'
spec = importlib.util.spec_from_file_location('http_probe_prepare', SOURCE)
prep = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prep)


class Preparation(unittest.TestCase):
    def test_current_cumulative_costs_are_derived_without_reset_or_duplicate_config(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            previous, source, target = [root / part for part in ['consumed', 'source', 'unused']]
            for directory in [previous / 'control', previous / 'ca', previous / 'config', source / 'control']:
                directory.mkdir(parents=True)
            for name in ['hosts', 'resolv.conf']:
                (previous / 'control' / name).write_text('offline fixture')
            (previous / 'control/settings.json').write_text('{"run_id":"consumed","max_connections":3}')
            (previous / 'ca/public-roots.pem').write_text('fixture identity')
            config = '[ingestion.yellowstone_http_recovery]\nbroker_url="http://127.0.0.1:18765/rpc"\n'
            (previous / 'config/read-only.toml').write_text(config)
            (previous / 'SCOPE.json').write_text('{"permission":"OWNER_DECISION_PENDING"}')
            (source / 'control/policy.json').write_text('{"rpc_key_sha256":"synthetic","financial_secret":"must_not_copy"}')
            baseline = root / 'baseline.json'
            carry = {'cumulative_model_usd': 7.960362665, 'http_model_usd': 0.00999075,
                     'http_requests': 997, 'rpc_cu': 19030, 'cumulative_stream_bytes': 85366468402,
                     'history_files_sha256': {}}
            baseline.write_text(json.dumps(carry))
            repository = SOURCE.parents[2]
            prep.prepare(previous, source, target, baseline, repository)
            policy = json.loads((target / 'control/http-policy.json').read_text())
            self.assertEqual((policy['prior_model_nano_usd'], policy['prior_http_nano_usd']), (7960362665, 9990750))
            self.assertNotIn('financial_secret', policy)
            self.assertEqual(json.loads((target / 'CARRYOVER.json').read_text()), carry)
            self.assertEqual((target / 'config/read-only.toml').read_text(), config)
            self.assertFalse(any((target / 'state').iterdir()))
            self.assertTrue(all((target / 'control' / name).is_file() for name in ['STOP', 'STREAM_STOP', 'HTTP_STOP']))
            with self.assertRaisesRegex(ValueError, 'new_unused_package_required'):
                prep.prepare(previous, source, target, baseline, repository)


if __name__ == '__main__':
    unittest.main()
