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

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'http_recovery_broker'))
from broker import BackendServer, FrontServer
from budget import Ledger, Refused
from policy import validate_route

CONF = {'commitment': 'confirmed', 'encoding': 'json', 'transactionDetails': 'full',
        'maxSupportedTransactionVersion': 1, 'rewards': True}

class RecoveryBroker(unittest.TestCase):
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
                self.assertIn(b'controlled missing block',body)
                self.assertEqual((d/'http-evidence/response-000001.json').read_bytes(),body)
                meta=json.loads((d/'http-evidence/response-000001.meta.json').read_text())
                self.assertEqual(meta['request'], {'jsonrpc':'2.0','id':7,'method':'getBlock','params':[123,CONF]})
                with Ledger(d/'broker-ledger.sqlite3',policy).db as sql:
                    self.assertEqual(sql.execute('SELECT rpc_cu,attempts FROM head').fetchone(),(40,1))
                self.assertEqual(post('sendTransaction',[])[0],403)
                self.assertEqual(post('getBlock',[123,dict(CONF,commitment='processed')])[0],403)
                self.assertEqual(post('getBlocks',[1,1025,{'commitment':'confirmed'}])[0],403)
                self.assertEqual(post('getBlocks',[123,124,{'commitment':'confirmed'}])[0],200)
                self.assertEqual(post('getBlock',[125,CONF])[0],429)
                (d/'HTTP_STOP').touch()
                self.assertEqual(post('getBlock',[125,CONF])[0],429)
                sql=sqlite3.connect(d/'broker-ledger.sqlite3')
                self.assertEqual(sql.execute('SELECT rpc_cu,attempts FROM head').fetchone(),(50,2));sql.close()
            finally:
                for server in [front,backend]: server.shutdown();server.server_close()
        upstream.shutdown(); upstream.server_close()
        os.environ.pop('BROKER_OFFLINE_TEST', None)

if __name__ == '__main__': unittest.main()
