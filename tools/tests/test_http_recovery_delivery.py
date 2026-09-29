"""Causal delivery checks using trusted local HTTPS and the actual broker."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import json
import os
from pathlib import Path
import resource
import shutil
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).parent))
from http_recovery_delivery_server import Fixture
from transport import perform_upstream

CONF = dict(commitment='confirmed', encoding='json', transactionDetails='full',
            maxSupportedTransactionVersion=1, rewards=True)


def post(fixture, slot, identity=1, timeout=15):
    start = time.monotonic()
    connection = http.client.HTTPConnection('127.0.0.1', fixture.front.server_port, timeout=timeout)
    try:
        connection.request('POST', '/rpc', json.dumps(dict(jsonrpc='2.0', id=identity,
             method='getBlock', params=[slot, CONF])), {'Content-Type': 'application/json'})
        response = connection.getresponse()
        body = response.read()
        return dict(status=response.status, body=json.loads(body), bytes=len(body),
                    elapsed_ms=round((time.monotonic()-start)*1000), headers=dict(response.getheaders()))
    finally:
        connection.close()


class DeliveryChecks(unittest.TestCase):
    def test_existing_transport_without_delivery_preserves_five_second_socket_contract(self):
        with tempfile.TemporaryDirectory(prefix='cbdeliver-') as name:
            root = Path(name)
            fixture = Fixture(root/'control', self.corpus(root)).start()
            seen = []
            original = http.client.HTTPSConnection.__init__

            def recorded(instance, *args, **kwargs):
                seen.append(kwargs.get('timeout'))
                original(instance, *args, **kwargs)

            try:
                (fixture.directory/'PROBE_CLOCK.json').unlink()
                body = json.dumps(dict(jsonrpc='2.0', id=7, method='getBlock', params=[100, CONF])).encode()
                with mock.patch.object(http.client.HTTPSConnection, '__init__', recorded):
                    status, headers, response = perform_upstream(fixture.policy, 'rpc', 'getBlock',
                                                                'POST', '/rpc', body, {})
                self.assertEqual(status, 200)
                self.assertEqual(json.loads(response)['result'], fixture.corpus[100]['result'])
                self.assertEqual(seen, [5])
                self.assertEqual(headers, {'content-type': 'application/json'})
            finally:
                fixture.close()

    def corpus(self, directory, real=False):
        path = directory / 'corpus'
        path.mkdir()
        source = os.getenv('COPYBOT_SAVED_HTTP_DELIVERY') if real else None
        if source:
            entries = []
            for meta in sorted(Path(source).glob('response-*.meta.json')):
                value = json.loads(meta.read_text())
                if value['method'] == 'getBlock':
                    body = meta.with_name(meta.name.replace('.meta.json', '.json'))
                    entries.append((body.stat().st_size, value['request']['params'][0], body))
            for _, slot, body in sorted(entries, reverse=True)[:4]:
                shutil.copyfile(body, path / f'{slot}.json')
        else:
            for slot in range(100, 104):
                (path / f'{slot}.json').write_text(json.dumps(dict(jsonrpc='2.0', id=1,
                                                      result=dict(parentSlot=slot-1, transactions=[]))))
        return path

    def wait_records(self, directory, count):
        end = time.monotonic()+3
        while len(list((directory / 'http-evidence').glob('delivery-*.front.json'))) < count:
            if time.monotonic() >= end:
                self.fail('front_completion_records_missing')
            time.sleep(.01)

    @unittest.skipUnless(os.getenv('COPYBOT_SAVED_HTTP_DELIVERY'), 'opt-in private saved corpus')
    def test_saved_large_four_concurrent_delayed_headers_and_client_baseline(self):
        with tempfile.TemporaryDirectory(prefix='cbdeliver-') as name:
            root = Path(name)
            corpus = self.corpus(root, real=True)
            report = dict(provider_calls=0, signatures=0, submissions=0,
                          background_loadavg=os.getloadavg(), disk_free_bytes=shutil.disk_usage(root).free)
            before = resource.getrusage(resource.RUSAGE_SELF)
            fixture = Fixture(root/'baseline', corpus, 'delay5').start()
            try:
                with self.assertRaises(TimeoutError):
                    post(fixture, min(fixture.corpus), timeout=5)
                self.wait_records(fixture.directory, 1)
                record = json.loads(next((fixture.directory/'http-evidence').glob('delivery-*.front.json')).read_text())
                self.assertEqual(record['outcome'], 'failed')
                self.assertEqual(record['broker_error']['stage'], 'front_body')
                report['client_5s_baseline'] = record
            finally:
                fixture.close()
            fixture = Fixture(root/'repaired', corpus, 'delay5').start()
            try:
                with ThreadPoolExecutor(max_workers=4) as pool:
                    results = list(pool.map(lambda item: post(fixture, item[1], identity=item[0]+10),
                                            enumerate(sorted(fixture.corpus))))
                self.wait_records(fixture.directory, 4)
                self.assertTrue(all(x['status'] == 200 for x in results))
                for item, slot in zip(results, sorted(fixture.corpus)):
                    self.assertEqual(item['body']['result'], fixture.corpus[slot]['result'])
                with sqlite3.connect(fixture.directory/'broker-ledger.sqlite3') as db:
                    self.assertEqual(db.execute('SELECT attempts,rpc_cu FROM head').fetchone(), (4, 160))
                evidence = fixture.directory/'http-evidence'
                records = [json.loads(p.read_text()) for p in sorted(evidence.glob('delivery-*.json'))]
                self.assertTrue(all(x['outcome']=='response' for x in records))
                for meta in evidence.glob('response-*.meta.json'):
                    value = json.loads(meta.read_text())
                    body = meta.with_name(meta.name.replace('.meta.json', '.json')).read_bytes()
                    self.assertEqual(hashlib.sha256(body).hexdigest(), value['response_sha256'])
                    self.assertEqual(len(body), value['bytes'])
                report['repaired_4_concurrent'] = dict(client_results=[{k:v for k,v in x.items() if k!='body'}
                                                                    for x in results], delivery_records=records)
                after = resource.getrusage(resource.RUSAGE_SELF)
                report['resource_usage'] = dict(user_seconds=after.ru_utime-before.ru_utime,
                     system_seconds=after.ru_stime-before.ru_stime, max_rss_platform_units=after.ru_maxrss)
                if os.getenv('COPYBOT_HTTP_DELIVERY_REPORT'):
                    Path(os.environ['COPYBOT_HTTP_DELIVERY_REPORT']).write_text(json.dumps(report, indent=2))
            finally:
                fixture.close()

    def test_midbody_close_remains_reserved_and_next_read_gets_new_reservation(self):
        with tempfile.TemporaryDirectory(prefix='cbdeliver-') as name:
            root = Path(name)
            corpus = self.corpus(root)
            # Ensure the injected partial body cannot accidentally be complete.
            value = json.loads((corpus/'100.json').read_text())
            value['result']['padding'] = 'x'*32768
            (corpus/'100.json').write_text(json.dumps(value))
            fixture = Fixture(root/'control', corpus, 'close-midbody').start()
            try:
                failed = post(fixture, 100)
                fact = failed['body']['broker_error']
                self.assertEqual((failed['status'], fact['reservation_id'], fact['stage'],
                                  fact['cause_type']), (502, 1, 'upstream_body', 'IncompleteRead'))
                self.assertEqual(fact['reason'], 'http_protocol')
                recovered = post(fixture, 100, identity=2)
                self.assertEqual(recovered['status'], 200)
                self.assertEqual(recovered['headers']['X-Copybot-Reservation-Id'], '2')
                self.assertEqual(recovered['body']['result'], value['result'])
                with sqlite3.connect(fixture.directory/'broker-ledger.sqlite3') as db:
                    self.assertEqual(db.execute('SELECT attempts,rpc_cu FROM head').fetchone(), (2, 80))
                    self.assertEqual(db.execute('SELECT n FROM attempts ORDER BY n').fetchall(), [(1,), (2,)])
                archived = json.loads((fixture.directory/'http-evidence/failure-000001.json').read_text())
                self.assertEqual(archived, fact)
            finally:
                fixture.close()

    def test_owner_session_clips_read_and_expired_request_never_reaches_upstream(self):
        with tempfile.TemporaryDirectory(prefix='cbdeliver-') as name:
            root = Path(name)
            fixture = Fixture(root/'control', self.corpus(root), 'delay5',
                              delay_seconds=.6, session_seconds=.2).start()
            try:
                clock = (fixture.directory/'PROBE_CLOCK.json').read_bytes()
                failed = post(fixture, 100)
                self.assertEqual(failed['status'], 502)
                fact = failed['body']['broker_error']
                self.assertEqual(fact['reason'], 'session_deadline_exhausted')
                absolute = failed['headers']['X-Copybot-Session-Deadline-Unix-Ms']
                later = post(fixture, 101, identity=2)
                refusal = later['body']['broker_error']
                self.assertEqual(later['status'], 403)
                self.assertEqual(refusal['reason'], 'read_only_clock_or_lease_invalid')
                self.assertEqual(refusal['gate_predicate'], 'clock_deadline')
                self.assertEqual(refusal['control_file'], 'PROBE_CLOCK.json')
                self.assertEqual((refusal['request_id'], refusal['slot'], refusal['reservation_id']),
                                 (2, 101, None))
                self.assertEqual(refusal['observed_deadline_unix_ms'], int(absolute))
                self.assertGreaterEqual(refusal['observed_now_unix_ms'], int(absolute))
                self.assertEqual((fixture.directory/'PROBE_CLOCK.json').read_bytes(), clock)
                with sqlite3.connect(fixture.directory/'broker-ledger.sqlite3') as db:
                    self.assertEqual(db.execute('SELECT attempts,rpc_cu FROM head').fetchone(), (1, 40))
                self.assertEqual(sum(fixture.counts.values()), 1)
            finally:
                fixture.close()

    def test_actual_eight_second_upstream_total_expires_before_client_delivery_limit(self):
        with tempfile.TemporaryDirectory(prefix='cbdeliver-') as name:
            root = Path(name)
            fixture = Fixture(root/'control', self.corpus(root), 'slow-always', delay_seconds=8.2).start()
            try:
                failed = post(fixture, 100)
                fact = failed['body']['broker_error']
                self.assertEqual((failed['status'], fact['reservation_id'], fact['stage'], fact['reason'],
                                  fact['cause_type']),
                                 (502, 1, 'upstream_headers', 'deadline_exhausted', 'DeadlineExceededTimeout'))
                self.assertGreaterEqual(failed['elapsed_ms'], 7900)
                self.assertLess(failed['elapsed_ms'], 12000)
                self.assertEqual(fact['deadline_ms'], 12000)
                self.assertEqual(json.loads((fixture.directory/'http-evidence/failure-000001.json').read_text()), fact)
                with sqlite3.connect(fixture.directory/'broker-ledger.sqlite3') as db:
                    self.assertEqual(db.execute('SELECT attempts,rpc_cu FROM head').fetchone(), (1, 40))
                if os.getenv('COPYBOT_HTTP_DELIVERY_LIMIT_REPORT'):
                    Path(os.environ['COPYBOT_HTTP_DELIVERY_LIMIT_REPORT']).write_text(json.dumps(failed, indent=2))
            finally:
                fixture.close()


if __name__ == '__main__':
    unittest.main()
