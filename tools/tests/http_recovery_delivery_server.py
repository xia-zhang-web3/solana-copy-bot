"""Actual local HTTPS/backend/front delivery, with saved results and bounded faults.

Corpus contains <slot>.json original JSON-RPC envelopes. Only request id is
rebound; result facts stay equivalent. No upstream/provider keys exist.
"""
import argparse
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import socket
import ssl
import sys
import tempfile
import threading
import time

from http_recovery_tls_server import certificates, save

CODE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CODE / 'tools/http_recovery_broker'))
from broker import BackendServer, FrontServer

SCENARIOS = ['normal', 'delay5', 'close-once', 'close-always', 'close-midbody', 'slow-always', 'wrong-id']


class Fixture:
    def __init__(self, directory, corpus, scenario='normal', delay_seconds=5.15, session_seconds=480,
                 fault_slot=None):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.corpus = {int(p.stem): json.loads(p.read_bytes()) for p in Path(corpus).glob('*.json')
                       if p.stem.isdecimal()}
        if not self.corpus:
            raise ValueError('empty_offline_corpus')
        self.scenario, self.delay_seconds = scenario, delay_seconds
        self.fault_slot = min(self.corpus) if fault_slot is None else fault_slot
        self.counts, self.lock = Counter(), threading.Lock()
        certificates(self.directory)
        os.environ['BROKER_OFFLINE_TEST'] = '1'
        os.environ['SSL_CERT_FILE'] = str((self.directory / 'trusted.pem').resolve())
        fixture = self

        class HTTPS(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'
            def log_message(self, *_):
                pass

            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                method, slot = request['method'], request['params'][0]
                with fixture.lock:
                    fixture.counts[(method, slot)] += 1
                    count = fixture.counts[(method, slot)]
                    with (fixture.directory / 'upstream-requests.jsonl').open('a') as log:
                        log.write(json.dumps(dict(method=method, slot=slot, request_id=request['id'],
                                                  attempt=count, at_unix=time.time())) + '\n')
                target = (method == 'getBlock' and slot == fixture.fault_slot and
                          not (fixture.directory / 'REPAIRED_TEST').exists())
                if target and (fixture.scenario == 'close-always' or
                               fixture.scenario == 'close-once' and count == 1):
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.close_connection = True
                    return
                if target and (fixture.scenario == 'slow-always' or
                               fixture.scenario == 'delay5' and count == 1):
                    time.sleep(fixture.delay_seconds)
                if method == 'getBlocks':
                    end = request['params'][1]
                    result = [n for n in sorted(fixture.corpus) if slot <= n <= end]
                else:
                    result = fixture.corpus[slot]['result']
                identity = request['id'] + (1 if fixture.scenario == 'wrong-id' else 0)
                body = json.dumps(dict(jsonrpc='2.0', id=identity, result=result),
                                  separators=(',', ':')).encode()
                try:
                    self.send_response(200)
                    self.send_header('Content-Length', str(len(body)))
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Connection', 'close')
                    self.end_headers()
                    if target and fixture.scenario == 'close-midbody' and count == 1:
                        self.wfile.write(body[:16384])
                        self.wfile.flush()
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.close_connection = True
                        return
                    self.wfile.write(body)
                except OSError:
                    pass
                self.close_connection = True

        self.https = ThreadingHTTPServer(('127.0.0.1', 0), HTTPS)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(self.directory / 'server.pem', self.directory / 'server.key')
        self.https.socket = context.wrap_socket(self.https.socket, server_side=True)
        self.sockets = tempfile.TemporaryDirectory(prefix='cbdel-')
        socket_path = Path(self.sockets.name) / 'http.sock'
        start = time.time() - (480-session_seconds)
        (self.directory / 'STOP').touch()
        save(self.directory / 'LEASE.json', dict(generation=1, expires_unix=start+480))
        save(self.directory / 'PROBE_CLOCK.json', dict(run_id='offline-delivery',
             first_attempt_unix=start, duration_seconds=480, deadline_unix=start+480))
        self.policy = dict(run_id='offline-delivery', profile='read_only_http_recovery_v1',
             offline_stub=True, rpc_upstream=f'https://localhost:{self.https.server_port}/rpc',
             max_rpc_attempts=1024, max_rpc_cu=40960, prior_http_nano_usd=10080000,
             prior_model_nano_usd=8035079857, stream_reserved_nano_usd=400000000)
        self.backend = BackendServer(socket_path, self.directory,
                                     self.directory / 'broker-ledger.sqlite3', self.policy)
        self.front = FrontServer(('127.0.0.1', 0), socket_path, self.directory)
        self.servers = [self.https, self.backend, self.front]

    def start(self):
        for server in self.servers:
            threading.Thread(target=server.serve_forever, daemon=True).start()
        self.url = f'http://127.0.0.1:{self.front.server_port}/rpc'
        save(self.directory / 'ready.json', dict(front_url=self.url, upstream_port=self.https.server_port,
             scenario=self.scenario, slots=sorted(self.corpus), provider_calls=0,
             signatures=0, submissions=0, request_log=str(self.directory / 'upstream-requests.jsonl')))
        return self

    def close(self):
        for server in reversed(self.servers):
            server.shutdown()
            server.server_close()
        self.sockets.cleanup()


def serve(args):
    fixture = Fixture(args.directory, args.corpus, args.scenario, args.delay_seconds,
                      args.session_seconds, args.fault_slot).start()
    stop = threading.Event()
    for number in [signal.SIGINT, signal.SIGTERM]:
        signal.signal(number, lambda *_: stop.set())
    try:
        deadline = time.monotonic() + 120
        while not stop.wait(.05) and not (args.directory / 'STOP_TEST').exists():
            if time.monotonic() >= deadline:
                raise TimeoutError('offline_fixture_deadline')
    finally:
        fixture.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--serve', action='store_true', required=True)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--corpus', type=Path, required=True)
    parser.add_argument('--scenario', choices=SCENARIOS, default='normal')
    parser.add_argument('--delay-seconds', type=float, default=5.15)
    parser.add_argument('--session-seconds', type=float, default=480)
    parser.add_argument('--fault-slot', type=int)
    serve(parser.parse_args())
