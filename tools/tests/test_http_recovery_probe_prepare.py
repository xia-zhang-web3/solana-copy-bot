"""Preparation preserves current carryover and copies an existing HTTP config once."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

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
            config = ('[other]\ntimeout_ms=42\n[ingestion.yellowstone_association]\n'
                      'input_bytes=8388608\nqueue={count=4,bytes=8388608}\n'
                      '[ingestion.yellowstone_http_recovery]\nbroker_url="http://127.0.0.1:18765/rpc"\n'
                      'max_response_bytes=8388608\ntimeout_ms=5000\n')
            (previous / 'config/read-only.toml').write_text(config)
            (previous / 'SCOPE.json').write_text('{"permission":"OWNER_DECISION_PENDING"}')
            (source / 'control/policy.json').write_text('{"rpc_key_sha256":"synthetic","financial_secret":"must_not_copy"}')
            baseline = root / 'baseline.json'
            carry = {'cumulative_model_usd': 7.999928734, 'http_model_usd': 0.01008,
                     'http_requests': 1002, 'rpc_cu': 19200, 'cumulative_stream_bytes': 85790347518,
                     'history_files_sha256': {}}
            baseline.write_text(json.dumps(carry))
            repository = SOURCE.parents[2]
            with mock.patch.object(prep, 'prepare_uds', return_value={'status':'offline-controlled'}) as uds:
                prep.prepare(previous, source, target, baseline, repository)
                uds.assert_called_once_with(prep.RUN, target / 'evidence')
            policy = json.loads((target / 'control/http-policy.json').read_text())
            self.assertEqual((policy['prior_model_nano_usd'], policy['prior_http_nano_usd']), (7999928734, 10080000))
            self.assertNotIn('financial_secret', policy)
            self.assertEqual(json.loads((target / 'CARRYOVER.json').read_text()), carry)
            self.assertEqual((target / 'config/read-only.toml').read_text(),
                             config.replace('timeout_ms=5000', 'timeout_ms=15000')
                             .replace('max_response_bytes=8388608', 'max_response_bytes=16777216')
                             .replace('input_bytes=8388608', 'input_bytes = 16777216')
                             .replace('queue={count=4,bytes=8388608}', 'queue = { count = 4, bytes = 67110912 }'))
            self.assertEqual((previous / 'config/read-only.toml').read_text(), config)
            self.assertFalse(any((target / 'state').iterdir()))
            self.assertTrue((target / 'scripts/uds_volume.py').is_file())
            self.assertTrue(all((target / 'control' / name).is_file() for name in ['STOP', 'STREAM_STOP', 'HTTP_STOP']))
            with self.assertRaisesRegex(ValueError, 'new_unused_package_required'):
                prep.prepare(previous, source, target, baseline, repository)


if __name__ == '__main__':
    unittest.main()
