"""Real front→UDS→common reservation→offline HTTP→immutable raw response."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import http.client
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
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'http_recovery_broker'))
from broker import BackendServer, FrontServer
from budget import Ledger, Refused
from policy import validate_route

CONF = {'commitment': 'confirmed', 'encoding': 'json', 'transactionDetails': 'full',
        'maxSupportedTransactionVersion': 1, 'rewards': True}

class RecoveryBroker(unittest.TestCase):
    def test_postreservation_stop_and_key_refusal_keep_original_phase_and_obligation(self):
        for scenario in ['stop', 'changed-key']:
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory(prefix='cb-http-') as name:
                d = Path(name)
                (d / 'STOP').touch()
                start = time.time()
                (d / 'LEASE.json').write_text(json.dumps({'generation': 1, 'expires_unix': start+480}))
                (d / 'PROBE_CLOCK.json').write_text(json.dumps({'run_id':'offline', 'duration_seconds':480,
                    'first_attempt_unix':start, 'deadline_unix':start+480}))
                policy = dict(run_id='offline', profile='read_only_http_recovery_v1',
                    rpc_upstream='https://solana-mainnet.g.alchemy.com/v2', max_rpc_attempts=1,
                    max_rpc_cu=10, prior_http_nano_usd=9990750, prior_model_nano_usd=7960362665,
                    stream_reserved_nano_usd=400000000)
                backend = BackendServer(d/'http.sock', d, d/'broker-ledger.sqlite3', policy)
                front = FrontServer(('127.0.0.1',0), d/'http.sock')
                for server in [backend,front]:
                    threading.Thread(target=server.serve_forever, daemon=True).start()
                reason = 'read_only_stop_present' if scenario=='stop' else 'provider_key_identity_changed'
                target = urlsplit('https://localhost:1/rpc')
                failure = Refused(reason) if scenario=='changed-key' else None
                try:
                    with mock.patch('broker.upstream', return_value=target), mock.patch(
                            'transport.upstream', return_value=target, side_effect=failure), mock.patch(
                            'broker.Gate.outbound_attempt', side_effect=Refused(reason) if scenario=='stop' else None):
                        c = http.client.HTTPConnection('127.0.0.1', front.server_port, timeout=3)
                        c.request('POST','/rpc',json.dumps({'jsonrpc':'2.0','id':7,'method':'getBlocks',
                                  'params':[1,2,{'commitment':'confirmed'}]}),{'Content-Type':'application/json'})
                        response = c.getresponse()
                        fact = json.loads(response.read())['broker_error']
                        self.assertEqual(response.status, 429 if scenario=='stop' else 502)
                        c.close()
                    self.assertEqual((fact['reservation_id'],fact['stage'],fact['reason'],fact['http_status']),
                                     (1,'gate' if scenario=='stop' else 'outbound',reason,None))
                    self.assertEqual(json.loads((d/'http-evidence/failure-000001.json').read_text()), fact)
                    with sqlite3.connect(d/'broker-ledger.sqlite3') as db:
                        self.assertEqual(db.execute('SELECT rpc_cu,attempts FROM head').fetchone(),(10,1))
                finally:
                    for server in [front,backend]:
                        server.shutdown(); server.server_close()

    def test_real_reservation_raw_error_and_method_budget_stop_boundaries(self):
        os.environ['BROKER_OFFLINE_TEST'] = '1'
        class Upstream(BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                data = b'{"jsonrpc":"2.0","id":7,"error":{"code":-32007,"message":"controlled missing block"}}'
                self.send_response(200); self.send_header('Content-Length', str(len(data)))
                self.end_headers(); self.wfile.write(data)
        upstream = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
        threading.Thread(target=upstream.serve_forever, daemon=True).start()
        with tempfile.TemporaryDirectory(prefix='cb-http-') as name:
            d = Path(name); (d / 'STOP').touch()
            (d / 'LEASE.json').write_text(json.dumps({'generation': 1, 'expires_unix': time.time()+600}))
            start = time.time()
            (d / 'PROBE_CLOCK.json').write_text(json.dumps({'run_id': 'offline', 'first_attempt_unix': start,
                'duration_seconds': 480, 'deadline_unix': start+480}))
            policy = {'run_id': 'offline', 'profile': 'read_only_http_recovery_v1', 'offline_stub': True,
                'rpc_upstream': f'http://127.0.0.1:{upstream.server_port}', 'max_rpc_attempts': 2,
                'max_rpc_cu': 50, 'prior_http_nano_usd': 9_985_500,
                'prior_model_nano_usd': 7_928_686_412, 'stream_reserved_nano_usd': 400_000_000}
            backend = BackendServer(d/'http.sock',d,d/'broker-ledger.sqlite3',policy)
            front = FrontServer(('127.0.0.1',0), d/'http.sock')
            for server in [backend,front]: threading.Thread(target=server.serve_forever,daemon=True).start()
            def post(method, params):
                c=http.client.HTTPConnection('127.0.0.1', front.server_port, timeout=3)
                c.request('POST','/rpc',json.dumps({'jsonrpc':'2.0','id':7,'method':method,'params':params}),{'Content-Type':'application/json'})
                r=c.getresponse(); status=r.status; data=r.read(); c.close(); return status,data
            try:
                status, body=post('getBlock',[123,CONF]); self.assertEqual(status,200)
                self.assertEqual(json.loads(body), {'jsonrpc':'2.0','id':7,'error':{'code':-32007}})
                self.assertEqual((d/'http-evidence/response-000001.json').read_bytes(),body)
                meta=json.loads((d/'http-evidence/response-000001.meta.json').read_text())
                self.assertEqual(meta['request'], {'jsonrpc':'2.0','id':7,'method':'getBlock','params':[123,CONF]})
                self.assertTrue(meta['rpc_error_message_redacted'])
                with Ledger(d/'broker-ledger.sqlite3',policy).db as sql:
                    self.assertEqual(sql.execute('SELECT rpc_cu,attempts FROM head').fetchone(),(40,1))
                self.assertEqual(post('sendTransaction',[])[0],403)
                self.assertEqual(post('getBlock',[123,dict(CONF,commitment='processed')])[0],403)
                self.assertEqual(post('getBlocks',[1,1025,{'commitment':'confirmed'}])[0],403)
                self.assertEqual(post('getBlocks',[123,124,{'commitment':'confirmed'}])[0],200)
                status, body = post('getBlock',[125,CONF])
                self.assertEqual(status,429)
                self.assertEqual(json.loads(body)['broker_error']['reason'], 'http_or_rpc_cap_exhausted')
                (d/'HTTP_STOP').touch()
                self.assertEqual(post('getBlock',[125,CONF])[0],429)
                sql=sqlite3.connect(d/'broker-ledger.sqlite3')
                self.assertEqual(sql.execute('SELECT rpc_cu,attempts FROM head').fetchone(),(50,2));sql.close()
                failures = [json.loads(p.read_text()) for p in (d/'http-evidence').glob('failure-*.json')]
                self.assertTrue(any(f['reason']=='http_or_rpc_cap_exhausted' and f['reservation_id'] is None for f in failures))
                self.assertTrue(any(f['reason']=='read_only_stop_present' for f in failures))
            finally:
                for server in [front,backend]: server.shutdown();server.server_close()
        upstream.shutdown(); upstream.server_close()
        os.environ.pop('BROKER_OFFLINE_TEST', None)

if __name__ == '__main__': unittest.main()
