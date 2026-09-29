"""Fresh local authority rereads; no provider replay or lease grace period."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).parent))
from http_recovery_delivery_server import Fixture
from test_http_recovery_delivery import post
from budget import Refused
from control_reader import check_control, safe_control_fact
from diagnostics import broker_fact
from policy import Gate


class ControlReaderChecks(unittest.TestCase):
    def control(self, directory):
        directory.mkdir(exist_ok=True)
        now = time.time()
        (directory/'STOP').touch()
        self.save(directory/'LEASE.json', dict(generation=1, expires_unix=now+15))
        self.save(directory/'PROBE_CLOCK.json', dict(run_id='local-control', duration_seconds=480,
                  first_attempt_unix=now, deadline_unix=now+480))
        return dict(profile='read_only_http_recovery_v1', run_id='local-control')

    def save(self, path, value):
        path.write_text(json.dumps(value))

    def delayed(self, directory, value=None, stop=None):
        (directory/'LEASE.json').unlink()
        def publish():
            time.sleep(.025)
            if stop:
                (directory/stop).touch()
            if value is not None:
                self.save(directory/'LEASE.json', value)
        thread = threading.Thread(target=publish)
        thread.start()
        return thread

    def test_missing_visibility_reread_uses_fresh_unexpired_lease_and_original_clock(self):
        with tempfile.TemporaryDirectory(prefix='cbctrl-') as name:
            d = Path(name)
            policy = self.control(d)
            clock = (d/'PROBE_CLOCK.json').read_bytes()
            thread = self.delayed(d, dict(generation=1, expires_unix=time.time()+15))
            gate = Gate(d, policy)
            try:
                gate.check()
            finally:
                thread.join()
            fact = gate.first_read_recovery
            self.assertEqual(fact['gate_predicate'], 'local_visibility_recovered')
            self.assertGreater(fact['local_read_attempts'], 1)
            self.assertLessEqual(fact['local_read_attempts'], 6)
            self.assertLess(fact['local_read_elapsed_ms'], 200)
            self.assertEqual(fact['first_missing_snapshot']['control_cause_type'], 'FileNotFoundError')
            gate.check()
            self.assertEqual(gate.first_read_recovery, fact)
            self.assertEqual((d/'PROBE_CLOCK.json').read_bytes(), clock)

    def test_fresh_denial_after_missing_read_never_uses_old_authority(self):
        for mode, predicate in [('expired', 'lease_expiry'), ('generation', 'lease_generation'),
                                ('stop', 'read_only_stop'), ('deadline', 'clock_deadline')]:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory(prefix='cbctrl-') as name:
                d = Path(name)
                policy = self.control(d)
                if mode == 'deadline':
                    end = time.time()+.020
                    self.save(d/'PROBE_CLOCK.json', dict(run_id='local-control', duration_seconds=480,
                              first_attempt_unix=end-480, deadline_unix=end))
                lease = dict(generation=2 if mode=='generation' else 1,
                             expires_unix=time.time()+(-1 if mode=='expired' else 15))
                thread = self.delayed(d, lease, 'HTTP_STOP' if mode=='stop' else None)
                try:
                    with self.assertRaises(Refused) as caught:
                        check_control(d, policy)
                finally:
                    thread.join()
                fact = caught.exception.control_fact
                self.assertEqual(fact['gate_predicate'], predicate)
                self.assertEqual(fact['first_missing_snapshot']['control_cause_type'], 'FileNotFoundError')

    def test_missing_exhaustion_is_finite_and_preserves_first_cause(self):
        with tempfile.TemporaryDirectory(prefix='cbctrl-') as name:
            d = Path(name)
            policy = self.control(d)
            (d/'LEASE.json').unlink()
            start = time.monotonic()
            with self.assertRaises(Refused) as caught:
                Gate(d, policy).check()
            self.assertLess(time.monotonic()-start, .6)
            fact = caught.exception.control_fact
            self.assertEqual(fact['gate_predicate'], 'lease_visibility_exhausted')
            self.assertEqual(fact['control_file'], 'LEASE.json')
            self.assertEqual(fact['control_cause_type'], 'FileNotFoundError')
            self.assertLessEqual(fact['local_read_attempts'], 6)
            self.assertEqual(fact['first_missing_snapshot']['control_file'], 'LEASE.json')
            self.assertFalse((d/'broker-ledger.sqlite3').exists())

    def test_parse_generation_clock_and_nonmissing_io_denials_are_not_retried(self):
        scenarios = [('malformed', 'lease_parse'), ('generation', 'lease_generation'),
                     ('clock-parse', 'clock_parse'), ('clock-run', 'clock_binding'),
                     ('clock-duration', 'clock_binding'), ('clock-missing', 'clock_read'),
                     ('permission', 'lease_read'), ('expired', 'lease_expiry')]
        for mode, expected in scenarios:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory(prefix='cbctrl-') as name:
                d = Path(name)
                policy = self.control(d)
                if mode == 'malformed':
                    (d/'LEASE.json').write_text('FAKE_SECRET_DO_NOT_EMIT')
                elif mode == 'generation':
                    self.save(d/'LEASE.json', dict(generation=True, expires_unix=time.time()+15))
                elif mode == 'clock-parse':
                    (d/'PROBE_CLOCK.json').write_text('[]')
                elif mode in {'clock-run', 'clock-duration'}:
                    clock = json.loads((d/'PROBE_CLOCK.json').read_text())
                    clock['run_id' if mode=='clock-run' else 'duration_seconds'] = 'foreign' if mode=='clock-run' else 481
                    self.save(d/'PROBE_CLOCK.json', clock)
                elif mode == 'clock-missing':
                    (d/'PROBE_CLOCK.json').unlink()
                elif mode == 'expired':
                    self.save(d/'LEASE.json', dict(generation=1, expires_unix=time.time()-1))
                original = Path.read_text
                reads = []
                def read(path, *args, **kwargs):
                    if path.name == 'LEASE.json':
                        reads.append(1)
                        if mode == 'permission':
                            raise PermissionError('FAKE_SECRET_DO_NOT_EMIT')
                    return original(path, *args, **kwargs)
                with mock.patch.object(Path, 'read_text', read), self.assertRaises(Refused) as caught:
                    check_control(d, policy)
                fact = broker_fact('refused', 'getBlock', None, 'gate', caught.exception,
                                   request={'request_id': 71, 'slot': 123})
                self.assertEqual(fact['gate_predicate'], expected)
                self.assertLessEqual(len(reads), 1)
                self.assertEqual((fact['request_id'], fact['slot'], fact['reservation_id']), (71, 123, None))
                self.assertNotIn('FAKE_SECRET', json.dumps(fact))

    def test_actual_front_backend_denial_preserves_original_cause_after_finalize(self):
        with tempfile.TemporaryDirectory(prefix='cbctrl-') as name:
            d = Path(name)
            corpus = d/'corpus'
            corpus.mkdir()
            self.save(corpus/'100.json', dict(jsonrpc='2.0', id=1, result=dict(transactions=[])))
            fixture = Fixture(d/'control', corpus).start()
            try:
                expired = time.time()-1
                self.save(fixture.directory/'LEASE.json', dict(generation=1, expires_unix=expired))
                response = post(fixture, 100, identity=77)
                fact = response['body']['broker_error']
                self.assertEqual((response['status'], fact['gate_predicate'], fact['kind']),
                                 (429, 'lease_expiry', 'refused'))
                self.assertEqual((fact['request_id'], fact['slot'], fact['reservation_id']), (77, 100, None))
                self.assertEqual(fact['observed_lease_expiry_unix_ms'], int(expired*1000))
                saved = next((fixture.directory/'http-evidence').glob('failure-*.json'))
                before = saved.read_bytes()
                self.save(fixture.directory/'LEASE.json', dict(generation=1, expires_unix=0))
                self.assertEqual(saved.read_bytes(), before)
                self.assertEqual(json.loads(before), fact)
                self.assertEqual(sum(fixture.counts.values()), 0)
                self.assertFalse((fixture.directory/'broker-ledger.sqlite3').exists())
                if os.getenv('COPYBOT_CONTROL_DENIAL_REPORT'):
                    Path(os.environ['COPYBOT_CONTROL_DENIAL_REPORT']).write_text(json.dumps(dict(
                        fact=fact, original_failure_sha256=hashlib.sha256(before).hexdigest(),
                        provider_calls=0, ledger_created=False, original_snapshot_retained=True), indent=2))
            finally:
                fixture.close()

    def test_safe_snapshot_is_allowlisted_and_one_level_bounded(self):
        fact = safe_control_fact(dict(control_file='LEASE.json', control_cause_type='FileNotFoundError',
                                     secret='FAKE_SECRET', first_missing_snapshot=dict(
                                     control_file='PROBE_CLOCK.json', first_missing_snapshot={'secret':'FAKE_SECRET'})))
        self.assertNotIn('FAKE_SECRET', json.dumps(fact))
        self.assertNotIn('first_missing_snapshot', fact['first_missing_snapshot'])


if __name__ == '__main__':
    unittest.main()
