"""Bounded causal read-only probe controls; every network endpoint is loopback."""
import json
from pathlib import Path
import socket
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[1] / 'transport_probe'
sys.path.insert(0, str(SOURCE))
from probe_relay import ProbeRelay
import probe_once
import probe_docker


def write(path, value):
    path.write_text(json.dumps(value))


def fixture(root):
    write(root / 'settings.json', dict(run_id='local-probe', generation=1,
        host='127.0.0.1', port=1, role='read_only_transport_probe', max_connections=3))
    write(root / 'LEASE.json', dict(generation=1, granted_bytes=16 * 1024**2,
        expires_unix=time.time() + 600))
    (root / 'STOP').touch()


class ProbeControlTests(unittest.TestCase):
    def test_financial_stop_is_permanent_and_stream_stop_is_independent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture(root)
            relay = ProbeRelay('backend', root, root / 'transport.sock')
            self.assertTrue(relay.valid())
            (root / 'STREAM_STOP').touch()
            self.assertFalse(relay.valid())
            self.assertTrue((root / 'STOP').is_file())
            self.assertFalse((root / 'PROBE_CLOCK.json').exists())

    def test_first_attempt_clock_does_not_reset_and_expired_clock_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture(root)
            relay = ProbeRelay('backend', root, root / 'transport.sock')
            relay.before_upstream_attempt()
            first = (root / 'PROBE_CLOCK.json').read_bytes()
            second = ProbeRelay('backend', root, root / 'transport.sock')
            second.before_upstream_attempt()
            self.assertEqual((root / 'PROBE_CLOCK.json').read_bytes(), first)
            value = json.loads(first)
            self.assertEqual(value['deadline_unix'] - value['first_attempt_unix'], 480)
            value['deadline_unix'] = time.time() - 1
            write(root / 'PROBE_CLOCK.json', value)
            self.assertFalse(second.valid())
            self.assertTrue((root / 'STOP').is_file())

    def test_three_attempt_cap_is_enforced_before_fourth_local_upstream_connect(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture(root)
            upstream = socket.socket()
            upstream.bind(('127.0.0.1', 0))
            upstream.listen(8)
            upstream.settimeout(.2)
            settings = json.loads((root / 'settings.json').read_text())
            settings['port'] = upstream.getsockname()[1]
            write(root / 'settings.json', settings)
            accepted = []
            done = threading.Event()
            def server():
                while not done.is_set():
                    try:
                        client, _ = upstream.accept()
                    except socket.timeout:
                        continue
                    accepted.append(1)
                    with client:
                        client.settimeout(1)
                        data = bytearray()
                        while True:
                            part = client.recv(64)
                            if not part:
                                break
                            data.extend(part)
                        client.sendall(bytes(data))
            source = threading.Thread(target=server)
            source.start()
            path = root / 'transport.sock'
            relay = ProbeRelay('backend', root, path)
            pump = threading.Thread(target=relay.run)
            pump.start()
            until = time.monotonic() + 2
            while not path.exists() and time.monotonic() < until:
                time.sleep(.005)
            for _ in range(4):
                with socket.socket(socket.AF_UNIX) as client:
                    client.settimeout(2)
                    client.connect(str(path))
                    client.sendall(b'x')
                    client.shutdown(socket.SHUT_WR)
                    try:
                        while client.recv(64):
                            pass
                    except ConnectionResetError:
                        pass  # fourth downstream attempt is intentionally refused
            pump.join(2)
            done.set()
            source.join(2)
            upstream.close()
            self.assertFalse(pump.is_alive())
            self.assertEqual(len(accepted), 3)
            self.assertEqual(relay.state['connect_attempts'], 3)
            self.assertEqual(relay.state['connection_headroom_bytes'], 3 * 1024**2)
            self.assertTrue((root / 'STREAM_STOP').is_file())
            self.assertTrue((root / 'STOP').is_file())
            self.assertEqual(json.loads((root / 'PROBE_CLOCK.json').read_text())['duration_seconds'], 480)


class ProbeObservationTests(unittest.TestCase):
    def test_reader_sees_committed_wal_and_unknown_does_not_become_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'state').mkdir()
            (root / 'evidence').mkdir()
            writer = sqlite3.connect(root / 'state/live_runtime.db')
            writer.execute('PRAGMA journal_mode=WAL')
            writer.execute('CREATE TABLE orders(value INTEGER)')
            writer.execute('INSERT INTO orders VALUES(1)')
            writer.commit()
            with patch.object(probe_once, 'ROOT', root):
                self.assertEqual(probe_once.financial_counts()['orders'], 1)
                with patch.object(probe_once, '_financial_counts', side_effect=sqlite3.OperationalError('database is locked')):
                    with patch.object(probe_once.time, 'sleep'):
                        unknown = probe_once.financial_counts()
                self.assertEqual(unknown['status'], 'UNKNOWN')
                self.assertIsNone(unknown['orders'])
                self.assertEqual(len(json.loads((root / 'evidence/SQLITE_READER_ERRORS.json').read_text())), 1)
            writer.close()

    def test_stream_and_global_budget_boundaries_preserve_old_charge(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'evidence').mkdir()
            prior = dict(stream_accounted_bytes=83722698801, http_attempts=996,
                         rpc_cu=19020, http_usd_nano=9985500)
            write(root / 'CARRYOVER.json', prior)
            with patch.object(probe_once, 'ROOT', root):
                value = probe_once.ledger(dict(received_bytes=4 * 1024**3 - 3 * 1024**2,
                    connection_headroom_bytes=3 * 1024**2, connect_attempts=3))
                self.assertEqual(value['probe_model_usd'], 0.4)
                self.assertEqual(value['cumulative_stream_bytes'], 83722698801 + 4 * 1024**3)
                self.assertEqual(value['http_usd_nano'], 9985500)
                with self.assertRaisesRegex(ValueError, 'budget_boundary'):
                    probe_once.ledger(dict(received_bytes=4 * 1024**3 + 1, connection_headroom_bytes=0, connect_attempts=0))
                with self.assertRaisesRegex(ValueError, 'budget_boundary'):
                    probe_once.ledger(dict(received_bytes=0, connection_headroom_bytes=0, connect_attempts=4))
                prior['stream_accounted_bytes'] = 500 * 1024**3
                write(root / 'CARRYOVER.json', prior)
                with self.assertRaisesRegex(ValueError, 'cumulative_50usd'):
                    probe_once.ledger(dict(received_bytes=0, connection_headroom_bytes=0, connect_attempts=0))


class ProbeStopTests(unittest.TestCase):
    def test_stop_failure_attempts_every_role_and_optional_errors_keep_terminal_result(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'control').mkdir()
            (root / 'evidence').mkdir()
            (root / 'control/STOP').touch()
            cids = dict(zip(['observation-app', 'stream-front', 'stream-backend'], ['app-cid', 'front-cid', 'backend-cid']))
            attempted = []
            def docker(args):
                attempted.append(args[-1])
                if args[-1] == 'app-cid':
                    raise ValueError('probe_docker_stop_exit_1')
                return ''
            owned = lambda cid: {'State': {'Running': False}}
            primary = dict(type='ValueError', reason='original_transport_failure')
            write(root / 'CARRYOVER.json', dict(stream_accounted_bytes=83722698801, http_usd_nano=9985500))
            with patch.object(probe_docker, 'ROOT', root), patch.object(probe_once, 'ROOT', root):
                with patch.object(probe_docker, 'docker', side_effect=docker), patch.object(probe_docker, 'verify_container', side_effect=owned):
                    with patch.object(probe_once, 'verify_container', side_effect=owned):
                        with patch.object(probe_once, 'observation', side_effect=ValueError('probe_docker_logs_exit_1')):
                            with patch.object(probe_once, 'financial_counts', side_effect=sqlite3.OperationalError('reader unavailable')):
                                value = probe_once.finalize(cids, 'READ_ONLY_FAILED', primary, 0)
            self.assertEqual(attempted, ['app-cid', 'front-cid', 'backend-cid'])
            self.assertEqual(value['primary_failure'], primary)
            self.assertEqual(value['stop_errors'][0]['role'], 'observation-app')
            self.assertEqual(value['result'], 'READ_ONLY_FAILED')
            self.assertEqual(value['ledger']['stream_usage_status'], 'UNKNOWN')
            self.assertIsNone(value['ledger']['new_observed_bytes'])
            self.assertEqual(value['ledger']['new_accounted_bytes'], 4 * 1024**3)
            self.assertIsNone(value['financial']['orders'])
            self.assertIsNone(value['observation']['transport'])
            stages = {entry['stage'] for entry in value['diagnostic_errors']}
            self.assertEqual(stages, {'backend_metrics', 'app_logs', 'financial_reader'})
            self.assertTrue((root / 'control/RESULT.json').is_file())
            self.assertTrue((root / 'evidence/RESULT.json').is_file())
            self.assertTrue((root / 'control/STREAM_STOP').is_file())
            self.assertTrue((root / 'control/STOP').is_file())

    def test_terminal_backend_missing_numeric_counters_reserves_full_grant(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'control').mkdir()
            (root / 'evidence').mkdir()
            (root / 'control/STOP').touch()
            write(root / 'control/backend-status.json', dict(phase='stopped'))
            write(root / 'CARRYOVER.json', dict(stream_accounted_bytes=83722698801, http_usd_nano=9985500))
            with patch.object(probe_once, 'ROOT', root), patch.object(probe_once, 'stop', return_value=[]):
                with patch.object(probe_once, 'observation', return_value={}):
                    with patch.object(probe_once, 'financial_counts', return_value={}):
                        value = probe_once.finalize({}, 'READ_ONLY_DEADLINE', None, 0)
            self.assertEqual(value['result'], 'READ_ONLY_DEADLINE')
            self.assertEqual(value['ledger']['stream_usage_status'], 'UNKNOWN')
            self.assertIsNone(value['ledger']['new_observed_bytes'])
            self.assertEqual(value['ledger']['new_accounted_bytes'], 4 * 1024**3)
            self.assertEqual(value['ledger']['cumulative_stream_bytes'], 83722698801 + 4 * 1024**3)
            self.assertEqual(value['ledger']['debit_basis'], 'FULL_GRANT_RESERVED')
            self.assertTrue(any(item['stage'] == 'ledger' and item['reason'] == 'probe_metrics_unknown'
                                for item in value['diagnostic_errors']))
            self.assertTrue((root / 'evidence/RESULT.json').is_file())

    def test_missing_owner_permission_refuses_before_any_preflight_or_start(self):
        with patch.object(probe_once, 'preflight', side_effect=AssertionError('must not run')):
            with self.assertRaisesRegex(ValueError, 'separate_owner_read_only_permission_required'):
                probe_once.run(False)


if __name__ == '__main__':
    unittest.main()
