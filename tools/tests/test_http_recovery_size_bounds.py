"""Actual archive/frame/front bounds, opt-in saved bytes, and legacy limits."""
import base64
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import sqlite3
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'http_recovery_broker'))
from broker import BackendServer, FrontServer
from diagnostics import broker_fact
from evidence import archive
from frames import read_frame, write_frame
from size_contract import (RECOVERY_PROFILE, RECOVERY_BLOCK_BYTES, RECOVERY_BASE64_BYTES,
    RECOVERY_FRAME_BYTES, FRAME_ENVELOPE_BYTES, FORWARDED_HEADER_BYTES, response_limit, frame_limit,
    check_headers)

CONF = dict(commitment='confirmed', encoding='json', transactionDetails='full',
            maxSupportedTransactionVersion=1, rewards=True)


class Pipeline:
    def __init__(self, directory, front_profile=RECOVERY_PROFILE):
        self.directory, self.reply, self.mode, self.attempts = directory, b'{}', 'declared', 0
        self.previous_offline = os.environ.get('BROKER_OFFLINE_TEST')
        os.environ['BROKER_OFFLINE_TEST'] = '1'
        owner = self

        class Upstream(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'
            def log_message(self, *_):
                pass
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                owner.attempts += 1
                try:
                    self.send_response(200)
                    self.send_header('Content-Type', 'x'*1025 if owner.mode=='header-over' else 'application/json')
                    if owner.mode == 'chunked':
                        self.send_header('Transfer-Encoding', 'chunked')
                    else:
                        self.send_header('Content-Length', str(len(owner.reply)))
                    self.send_header('Connection', 'close')
                    self.end_headers()
                    if owner.mode == 'chunked':
                        for offset in range(0, len(owner.reply), 65536):
                            chunk = owner.reply[offset:offset+65536]
                            self.wfile.write(f'{len(chunk):x}\r\n'.encode()+chunk+b'\r\n')
                        self.wfile.write(b'0\r\n\r\n')
                    else:
                        self.wfile.write(owner.reply)
                except OSError:
                    pass
                self.close_connection = True

        self.upstream = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
        now = time.time()
        directory.mkdir()
        (directory/'STOP').touch()
        (directory/'LEASE.json').write_text(json.dumps(dict(generation=1, expires_unix=now+15)))
        (directory/'PROBE_CLOCK.json').write_text(json.dumps(dict(run_id='offline-size', duration_seconds=480,
            first_attempt_unix=now, deadline_unix=now+480)))
        self.policy = dict(run_id='offline-size', profile=RECOVERY_PROFILE, offline_stub=True,
            rpc_upstream=f'http://127.0.0.1:{self.upstream.server_port}/rpc', max_rpc_attempts=64,
            max_rpc_cu=2560, prior_http_nano_usd=10000000, prior_model_nano_usd=8073614162,
            stream_reserved_nano_usd=400000000)
        self.socket_directory = tempfile.TemporaryDirectory(prefix='cbsz-')
        sock = Path(self.socket_directory.name)/'http.sock'
        self.backend = BackendServer(sock, directory, directory/'broker-ledger.sqlite3', self.policy)
        if front_profile is None:
            self.front = FrontServer(('127.0.0.1', 0), sock, directory)
        else:
            self.front = FrontServer(('127.0.0.1', 0), sock, directory, profile=front_profile)
        self.servers = [self.upstream, self.backend, self.front]
        for server in self.servers:
            threading.Thread(target=server.serve_forever, daemon=True).start()

    def post(self, slot=451658617):
        client = http.client.HTTPConnection('127.0.0.1', self.front.server_port, timeout=15)
        try:
            client.request('POST', '/rpc', json.dumps(dict(jsonrpc='2.0', id=12,
                method='getBlock', params=[slot, CONF])), {'Content-Type':'application/json'})
            response = client.getresponse()
            return response.status, response.read()
        finally:
            client.close()

    def close(self):
        for server in reversed(self.servers):
            server.shutdown()
            server.server_close()
        self.socket_directory.cleanup()
        if self.previous_offline is None:
            os.environ.pop('BROKER_OFFLINE_TEST', None)
        else:
            os.environ['BROKER_OFFLINE_TEST'] = self.previous_offline


class SizeBounds(unittest.TestCase):
    def test_profile_route_caps_and_header_envelope_are_finite(self):
        self.assertEqual(response_limit('rpc', 'getBlock', RECOVERY_PROFILE), 16777216)
        self.assertEqual(RECOVERY_BASE64_BYTES, 22369624)
        self.assertEqual(RECOVERY_FRAME_BYTES, 22435160)
        self.assertEqual(RECOVERY_FRAME_BYTES, RECOVERY_BASE64_BYTES+FRAME_ENVELOPE_BYTES)
        for profile in [None, 'financial', 'read_only_other']:
            self.assertEqual(response_limit('rpc','getBlock',profile), 8388608)
            self.assertEqual(frame_limit('rpc','getBlock',profile), 12000000)
        for route, method in [('rpc','getBlocks'), ('quote','quote'), ('swap','swap'), ('rpc','sendTransaction')]:
            self.assertEqual(response_limit(route,method,RECOVERY_PROFILE), 4194304)
            self.assertEqual(frame_limit(route,method,RECOVERY_PROFILE), 8500000)
        headers = {'content-type':'\x7f'*FORWARDED_HEADER_BYTES,
                   'content-encoding':'\x7f'*FORWARDED_HEADER_BYTES}
        self.assertEqual(check_headers(headers, RECOVERY_PROFILE), headers)
        packet = dict(kind='response', status=200, headers=headers,
                      body='A'*RECOVERY_BASE64_BYTES, broker_trace={'schema':'http_recovery_delivery_v1'})
        self.assertLess(len(json.dumps(packet,separators=(',',':')).encode()), RECOVERY_FRAME_BYTES)
        with self.assertRaises(ValueError) as caught:
            check_headers({'content-type':'x'*(FORWARDED_HEADER_BYTES+1)}, RECOVERY_PROFILE)
        self.assertEqual(caught.exception.size_fact['size_layer'], 'upstream_header')
        check_headers({'content-type':'x'*100000}, None)  # Legacy header behavior unchanged.
        check_headers({'content-type':'x'*100000}, RECOVERY_PROFILE, 'rpc', 'getBlocks')

    @unittest.skipUnless(os.getenv('COPYBOT_SAVED06_SIZE_BODY'), 'private saved06 body opt-in')
    def test_saved_bytes_old_limit_plus_one_and_new_boundary_reach_exact_archive_and_client(self):
        raw = Path(os.environ['COPYBOT_SAVED06_SIZE_BODY']).read_bytes()
        self.assertEqual(len(raw), 7795854)
        self.assertEqual(hashlib.sha256(raw).hexdigest(), '29c855b72a4039bcf6cd9cbf5c2baec437ea5f8580bb0a40cfbf1b04462a5830')
        report = dict(original_bytes=len(raw), original_sha256=hashlib.sha256(raw).hexdigest(), cases=[],
                      provider_calls=0, signatures=0, submissions=0)
        with tempfile.TemporaryDirectory(prefix='cbsz-') as name:
            fixture = Pipeline(Path(name)/'control')
            try:
                for index, (size, mode) in enumerate([(len(raw),'declared'), (8388609,'declared'),
                    (RECOVERY_BLOCK_BYTES-1,'declared'), (RECOVERY_BLOCK_BYTES,'chunked')], 1):
                    body = raw+b' '*(size-len(raw))  # Facts unchanged; measured bytes enlarge fixture only.
                    fixture.reply, fixture.mode = body, mode
                    status, client = fixture.post()
                    self.assertEqual(status, 200)
                    self.assertEqual(client, body)
                    self.assertEqual(json.loads(client), json.loads(raw))
                    directory = fixture.directory/'http-evidence'
                    saved = directory/f'response-{index:06}.json'
                    meta = json.loads((directory/f'response-{index:06}.meta.json').read_text())
                    self.assertEqual(saved.read_bytes(), body)
                    self.assertEqual(meta['bytes'], size)
                    self.assertEqual(meta['response_sha256'], hashlib.sha256(client).hexdigest())
                    report['cases'].append(dict(mode=mode, bytes=size, archive_client_sha256=meta['response_sha256']))
                with sqlite3.connect(fixture.directory/'broker-ledger.sqlite3') as db:
                    self.assertEqual(db.execute('SELECT attempts,rpc_cu FROM head').fetchone(), (4,160))
                self.assertEqual(fixture.attempts, 4)
            finally:
                fixture.close()
        if os.getenv('COPYBOT_SIZE_BOUND_REPORT'):
            Path(os.environ['COPYBOT_SIZE_BOUND_REPORT']).write_text(json.dumps(report,indent=2))

    def test_upstream_declared_chunked_and_header_overflows_keep_one_reservation(self):
        with tempfile.TemporaryDirectory(prefix='cbsz-') as name:
            fixture = Pipeline(Path(name)/'control')
            try:
                for index, (mode, layer, observation) in enumerate([
                    ('declared','upstream_declared','declared'), ('chunked','upstream_body','lower_bound'),
                    ('header-over','upstream_header','exact')], 1):
                    fixture.mode = mode
                    fixture.reply = b' '*(RECOVERY_BLOCK_BYTES+1) if mode!='header-over' else b'{}'
                    status, body = fixture.post()
                    fact = json.loads(body)['broker_error']
                    self.assertEqual((status, fact['kind'], fact['cause_type'], fact['reason']),
                                     (502,'failed','ValueError','upstream_response_too_large'))
                    self.assertEqual((fact['size_layer'], fact['size_observation']), (layer,observation))
                    self.assertEqual((fact['request_id'],fact['slot'],fact['reservation_id']), (12,451658617,index))
                    limit = FORWARDED_HEADER_BYTES if mode=='header-over' else RECOVERY_BLOCK_BYTES
                    self.assertEqual((fact['observed_bytes'],fact['size_limit_bytes']), (limit+1,limit))
                    self.assertEqual(json.loads((fixture.directory/f'http-evidence/failure-{index:06}.json').read_text()),fact)
                    self.assertFalse((fixture.directory/f'http-evidence/response-{index:06}.json').exists())
                with sqlite3.connect(fixture.directory/'broker-ledger.sqlite3') as db:
                    self.assertEqual(db.execute('SELECT attempts,rpc_cu FROM head').fetchone(), (3,120))
                self.assertEqual(fixture.attempts, 3)
            finally:
                fixture.close()

    def test_frame_read_rejects_declared_size_without_payload_and_write_never_sends_over_limit(self):
        left, right = socket.socketpair()
        with left, right:
            left.sendall(f'{RECOVERY_FRAME_BYTES+1:016x}'.encode())
            with self.assertRaises(ValueError) as caught:
                read_frame(right, RECOVERY_FRAME_BYTES)
            self.assertEqual(caught.exception.size_fact, dict(size_layer='frame',
                observed_bytes=RECOVERY_FRAME_BYTES+1,size_limit_bytes=RECOVERY_FRAME_BYTES,size_observation='declared'))
        class Sink:
            writes = 0
            def sendall(self, data):
                self.writes += 1
        sink = Sink()
        with self.assertRaises(ValueError) as caught:
            write_frame(sink, {'body':'A'*RECOVERY_FRAME_BYTES}, RECOVERY_FRAME_BYTES)
        self.assertEqual(sink.writes, 0)
        fact = broker_fact('failed','getBlock',1,'unix_send',caught.exception)
        self.assertEqual(fact['reason'], 'frame_too_large')
        self.assertGreater(fact['observed_bytes'], fact['size_limit_bytes'])

    def test_archive_over_limit_rejects_before_writing_and_legacy_cap_is_unchanged(self):
        request = json.dumps(dict(jsonrpc='2.0',id=12,method='getBlock',params=[451658617,CONF])).encode()
        with tempfile.TemporaryDirectory(prefix='cbsz-') as name:
            d = Path(name)
            with self.assertRaises(ValueError) as caught:
                archive(d,1,'getBlock',request,200,b' '*(RECOVERY_BLOCK_BYTES+1),profile=RECOVERY_PROFILE)
            self.assertEqual(caught.exception.size_fact['size_layer'], 'archive')
            self.assertFalse((d/'http-evidence').exists())
            with self.assertRaises(ValueError) as legacy:
                archive(d,1,'getBlock',request,200,b' '*(8388608+1),profile='financial')
            self.assertEqual(legacy.exception.size_fact['size_limit_bytes'], 8388608)

    def test_default_front_remains_legacy_and_rejects_body_over_eight_mib(self):
        with tempfile.TemporaryDirectory(prefix='cbsz-') as name:
            fixture = Pipeline(Path(name)/'control', front_profile=None)
            try:
                self.assertIsNone(fixture.front.profile)
                fixture.reply = b'{"jsonrpc":"2.0","id":12,"result":null}'+b' '*(8388609-39)
                # Length is set exactly; profile-sensitive frontend receives a valid framed reply.
                fixture.reply = fixture.reply[:8388609].ljust(8388609, b' ')
                status, body = fixture.post()
                fact = json.loads(body)['broker_error']
                self.assertEqual((status,fact['reason'],fact['size_layer']), (502,'response_too_large','front_body'))
                self.assertEqual((fact['observed_bytes'],fact['size_limit_bytes']), (8388609,8388608))
                self.assertEqual(fixture.attempts, 1)
            finally:
                fixture.close()


if __name__ == '__main__':
    unittest.main()
