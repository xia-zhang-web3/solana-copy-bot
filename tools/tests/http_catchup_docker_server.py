"""Full representative local gap: delayed reads and actual truncated HTTPS body."""
import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import re
import signal
import ssl
import threading
import time

import http_size_docker_server as size
from http_size_metrics import Metrics, save

FIRST = 451679658
SLOW = FIRST + 2


def upstream(args, metrics):
    lock, counts = threading.Lock(), {}
    conf = {'commitment': 'confirmed', 'encoding': 'json', 'transactionDetails': 'full',
            'maxSupportedTransactionVersion': 1, 'rewards': True}

    def record(value):
        with lock:
            with (args.directory / 'upstream-events.jsonl').open('a') as log:
                log.write(json.dumps(dict(at_unix=time.time(), **value)) + '\n')

    class HTTPS(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'
        def log_message(self, *_):
            pass

        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            method, slot, rid = request['method'], request['params'][0], request['id']
            if method == 'getBlock' and request['params'][1] != conf:
                self.send_error(403)
                return
            with lock:
                counts[slot] = counts.get(slot, 0) + 1
                attempt = counts[slot]
                with (args.directory / 'upstream-requests.jsonl').open('a') as log:
                    log.write(json.dumps(dict(method=method, slot=slot, request_id=rid,
                                              attempt=attempt, at_unix=time.time())) + '\n')
            record(dict(stage='request', method=method, slot=slot, request_id=rid, attempt=attempt))
            if method == 'getBlocks':
                slots = sorted(int(p.stem) for p in args.corpus.glob('*.json')
                               if p.stem.isdecimal() and slot <= int(p.stem) <= request['params'][1])
                body = json.dumps(dict(jsonrpc='2.0', id=rid, result=slots), separators=(',', ':')).encode()
            else:
                # Exact original bytes except envelope id, never reserialize transaction facts.
                original = (args.corpus / f'{slot}.json').read_bytes()
                body, matches = re.subn(rb'"id":\d+', b'"id":'+str(rid).encode(), original, count=1)
                assert matches == 1
                del original
            wire_hash = hashlib.sha256(body).hexdigest()
            truncate = method == 'getBlock' and slot == SLOW+1 and attempt == 1
            delay = 9 if method == 'getBlock' and (slot == SLOW or (slot == SLOW+1 and attempt == 2)) else 0
            try:
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.send_header('Connection', 'close')
                self.end_headers()
                self.wfile.flush()
                record(dict(stage='headers', method=method, slot=slot, request_id=rid,
                            attempt=attempt, bytes=len(body), sha256=wire_hash, delay_seconds=delay))
                if delay:
                    time.sleep(delay)
                if truncate:
                    self.wfile.write(body[:16384])
                    self.wfile.flush()
                    record(dict(stage='truncated', method=method, slot=slot, request_id=rid,
                                attempt=attempt, sent_bytes=16384, declared_bytes=len(body)))
                else:
                    for start in range(0, len(body), 65536):
                        self.wfile.write(memoryview(body)[start:start+65536])
                    self.wfile.flush()
                    record(dict(stage='complete', method=method, slot=slot, request_id=rid,
                                attempt=attempt, bytes=len(body), sha256=wire_hash))
            except OSError as error:
                record(dict(stage='peer_closed', slot=slot, request_id=rid,
                            cause_type=type(error).__name__))
            self.close_connection = True

    server = ThreadingHTTPServer(('127.0.0.1', 18766), HTTPS)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(args.directory / 'server.pem', args.directory / 'server.key')
    server.socket = tls.wrap_socket(server.socket, server_side=True)
    return server


def run(args):
    metrics = Metrics(args.directory, args.role)
    server = {'upstream': upstream, 'backend': size.backend, 'front': size.front}[args.role](args, metrics)
    stopped = threading.Event()
    for number in [signal.SIGINT, signal.SIGTERM]:
        signal.signal(number, lambda *_: stopped.set())
    threading.Thread(target=server.serve_forever, daemon=True).start()
    save(args.directory / (args.role+'-ready.json'), dict(pid=__import__('os').getpid(), ready=True))
    try:
        deadline = time.monotonic()+args.duration
        while not stopped.wait(.05) and not (args.directory / 'STOP_TEST').exists():
            if time.monotonic() >= deadline:
                raise TimeoutError('offline_catchup_role_deadline')
    finally:
        server.shutdown()
        server.server_close()
        metrics.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--role', choices=['upstream', 'backend', 'front'], required=True)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--corpus', type=Path, required=True)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--duration', type=float, default=390)
    run(parser.parse_args())
