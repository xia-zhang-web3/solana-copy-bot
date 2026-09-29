"""Actual HTTPS body delay beyond the old limit; no provider or signer exists."""
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sqlite3
import ssl
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, str(Path(__file__).parent))
from http_recovery_tls_server import certificates, save
from broker import BackendServer, FrontServer
from delivery import UPSTREAM_SECONDS, DELIVERY_SECONDS
from size_contract import RECOVERY_PROFILE

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'http_recovery_probe'))
from config_bounds import CLIENT_TIMEOUT_MS

CONF = dict(commitment='confirmed', encoding='json', transactionDetails='full',
            maxSupportedTransactionVersion=1, rewards=True)


class BodyDelay:
    def __init__(self, directory, body, delay_seconds, session_seconds=480):
        self.directory, self.body = directory, body
        self.delay_seconds, self.outbound = delay_seconds, 0
        self.release = threading.Event()
        self.previous = {key: os.environ.get(key) for key in ['BROKER_OFFLINE_TEST', 'SSL_CERT_FILE']}
        directory.mkdir()
        certificates(directory)
        os.environ['BROKER_OFFLINE_TEST'] = '1'
        os.environ['SSL_CERT_FILE'] = str(directory/'trusted.pem')
        owner = self

        class HTTPS(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'
            def log_message(self, *_):
                pass
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                owner.outbound += 1
                try:
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Content-Length', str(len(owner.body)))
                    self.send_header('Connection', 'close')
                    self.end_headers()
                    self.wfile.write(owner.body[:65536])
                    self.wfile.flush()
                    owner.release.wait(owner.delay_seconds)
                    self.wfile.write(owner.body[65536:])
                except OSError:
                    pass
                self.close_connection = True

        self.https = ThreadingHTTPServer(('127.0.0.1', 0), HTTPS)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(directory/'server.pem', directory/'server.key')
        self.https.socket = context.wrap_socket(self.https.socket, server_side=True)
        deadline = time.time()+session_seconds
        (directory/'STOP').touch()
        save(directory/'LEASE.json', dict(generation=1, expires_unix=time.time()+15))
        save(directory/'PROBE_CLOCK.json', dict(run_id='offline-catchup', duration_seconds=480,
             first_attempt_unix=deadline-480, deadline_unix=deadline))
        policy = dict(run_id='offline-catchup', profile=RECOVERY_PROFILE, offline_stub=True,
             rpc_upstream=f'https://localhost:{self.https.server_port}/rpc', max_rpc_attempts=1024,
             max_rpc_cu=40960, prior_http_nano_usd=10080000, prior_model_nano_usd=8035079857,
             stream_reserved_nano_usd=400000000)
        self.sockets = tempfile.TemporaryDirectory(prefix='cbcu-')
        socket_path = Path(self.sockets.name)/'http.sock'
        self.backend = BackendServer(socket_path, directory, directory/'broker-ledger.sqlite3', policy)
        self.front = FrontServer(('127.0.0.1', 0), socket_path, directory, profile=RECOVERY_PROFILE)
        self.servers = [self.https, self.backend, self.front]
        for server in self.servers:
            threading.Thread(target=server.serve_forever, daemon=True).start()

    def post(self, identity, slot):
        start = time.monotonic()
        client = http.client.HTTPConnection('127.0.0.1', self.front.server_port,
                                           timeout=CLIENT_TIMEOUT_MS/1000)
        try:
            client.request('POST', '/rpc', json.dumps(dict(jsonrpc='2.0', id=identity,
                method='getBlock', params=[slot, CONF])), {'Content-Type':'application/json'})
            response = client.getresponse()
            return dict(status=response.status, body=response.read(), headers=dict(response.getheaders()),
                        elapsed_ms=round((time.monotonic()-start)*1000))
        finally:
            client.close()

    def close(self):
        self.release.set()
        for server in reversed(self.servers):
            server.shutdown()
            server.server_close()
        self.sockets.cleanup()
        for key, value in self.previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


class CatchupDeadlines(unittest.TestCase):
    @unittest.skipUnless(os.getenv('COPYBOT_SAVED07_CATCHUP_BODY'), 'private saved07 body opt-in')
    def test_saved_full_body_after_nine_seconds_reaches_archive_and_client_exactly(self):
        body = Path(os.environ['COPYBOT_SAVED07_CATCHUP_BODY']).read_bytes()
        value = json.loads(body)
        self.assertEqual(len(body), 10223194)
        self.assertEqual(hashlib.sha256(body).hexdigest(),
                         '2dde54f7a08bbccde239b7bb0377b40f48b3e5d9908c9bb8222528d28f25a5f7')
        self.assertEqual((UPSTREAM_SECONDS, DELIVERY_SECONDS, CLIENT_TIMEOUT_MS), (20, 25, 30000))
        with tempfile.TemporaryDirectory(prefix='cbcu-') as name:
            fixture = BodyDelay(Path(name)/'control', body, 9)
            try:
                clock = (fixture.directory/'PROBE_CLOCK.json').read_bytes()
                result = fixture.post(value['id'], 451679663)
                self.assertEqual(result['status'], 200)
                self.assertEqual(result['body'], body)
                self.assertGreaterEqual(result['elapsed_ms'], 9000)
                self.assertLess(result['elapsed_ms'], 25000)
                self.assertEqual(result['headers']['X-Copybot-Reservation-Id'], '1')
                evidence = fixture.directory/'http-evidence'
                saved = evidence/'response-000001.json'
                meta = json.loads((evidence/'response-000001.meta.json').read_text())
                self.assertEqual(saved.read_bytes(), body)
                self.assertEqual((meta['bytes'], meta['response_sha256']),
                                 (len(body), hashlib.sha256(body).hexdigest()))
                until = time.monotonic()+2
                while not (evidence/'delivery-000001.front.json').exists():
                    if time.monotonic() >= until:
                        self.fail('front_delivery_record_missing')
                    time.sleep(.01)
                records = {side:json.loads((evidence/f'delivery-000001.{side}.json').read_text())
                           for side in ['backend', 'front']}
                self.assertTrue(all(record['outcome']=='response' for record in records.values()))
                self.assertGreaterEqual(records['backend']['timings_ms']['upstream_body'], 9000)
                self.assertEqual((fixture.directory/'PROBE_CLOCK.json').read_bytes(), clock)
                with sqlite3.connect(fixture.directory/'broker-ledger.sqlite3') as db:
                    self.assertEqual(db.execute('SELECT attempts,rpc_cu FROM head').fetchone(), (1,40))
                self.assertEqual(fixture.outbound, 1)
                report = dict(provider_calls=0, signatures=0, submissions=0,
                    upstream_seconds=UPSTREAM_SECONDS, delivery_seconds=DELIVERY_SECONDS,
                    client_timeout_ms=CLIENT_TIMEOUT_MS, bytes=len(body),
                    archive_client_sha256=meta['response_sha256'], elapsed_ms=result['elapsed_ms'],
                    ledger_attempts=1, ledger_rpc_cu=40, outbound=1, delivery_records=records)
                if os.getenv('COPYBOT_CATCHUP_DEADLINE_REPORT'):
                    Path(os.environ['COPYBOT_CATCHUP_DEADLINE_REPORT']).write_text(json.dumps(report,indent=2))
            finally:
                fixture.close()

    def test_original_session_deadline_clips_body_and_later_request_is_unreserved(self):
        body = json.dumps(dict(jsonrpc='2.0', id=15, result=dict(padding='x'*65536))).encode()
        with tempfile.TemporaryDirectory(prefix='cbcu-') as name:
            fixture = BodyDelay(Path(name)/'control', body, .8, session_seconds=.3)
            try:
                clock = (fixture.directory/'PROBE_CLOCK.json').read_bytes()
                failed = fixture.post(15, 451679663)
                fact = json.loads(failed['body'])['broker_error']
                self.assertEqual((failed['status'], fact['reason']), (502,'session_deadline_exhausted'))
                # Front may expire before the backend's reserved failure frame arrives.
                # Its clock cannot wait beyond the original session to learn that id.
                self.assertIn(fact['reservation_id'], [None, 1])
                path = fixture.directory/'http-evidence/failure-000001.json'
                until = time.monotonic()+2
                while not path.exists():
                    if time.monotonic() >= until:
                        self.fail('reserved_backend_failure_missing')
                    time.sleep(.01)
                backend = json.loads(path.read_text())
                self.assertEqual((backend['reservation_id'], backend['reason'], backend['stage']),
                                 (1,'session_deadline_exhausted','upstream_body'))
                self.assertLess(failed['elapsed_ms'], 1500)
                denied = fixture.post(16, 451679664)
                refusal = json.loads(denied['body'])['broker_error']
                self.assertEqual((denied['status'], refusal['reservation_id'], refusal['gate_predicate']),
                                 (403,None,'clock_deadline'))
                self.assertEqual((fixture.directory/'PROBE_CLOCK.json').read_bytes(), clock)
                with sqlite3.connect(fixture.directory/'broker-ledger.sqlite3') as db:
                    self.assertEqual(db.execute('SELECT attempts,rpc_cu FROM head').fetchone(), (1,40))
                self.assertEqual(fixture.outbound, 1)
                if os.getenv('COPYBOT_CATCHUP_DEADLINE_REPORT'):
                    output = Path(os.environ['COPYBOT_CATCHUP_DEADLINE_REPORT'])
                    output.with_name(output.stem+'_SESSION_DEADLINE_REPORT.json').write_text(json.dumps(dict(
                            provider_calls=0, signatures=0, submissions=0, outbound=1,
                            ledger_attempts=1, ledger_rpc_cu=40, clock_unchanged=True,
                            front_error=fact, backend_error=backend, later_error=refusal),indent=2))
            finally:
                fixture.close()


if __name__ == '__main__':
    unittest.main()
