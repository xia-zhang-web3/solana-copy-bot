"""Saved06/dense modeled JSON through real HTTPS and separate production roles."""
import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import socket
import ssl
import sys
import threading
import time

CODE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CODE / 'tools/http_recovery_broker'))
import broker
from size_contract import RECOVERY_PROFILE, RECOVERY_BLOCK_BYTES, RECOVERY_FRAME_BYTES
from http_size_metrics import Metrics, save


def upstream(args, metrics):
    directory = args.directory
    lock = threading.Lock()
    conf = {'commitment': 'confirmed', 'encoding': 'json', 'transactionDetails': 'full',
            'maxSupportedTransactionVersion': 1, 'rewards': True}

    class HTTPS(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'
        def log_message(self, *_):
            pass

        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            method, slot = request['method'], request['params'][0]
            if method == 'getBlock' and request['params'][1] != conf:
                self.send_error(403)
                return
            with lock:
                with (directory / 'upstream-requests.jsonl').open('a') as log:
                    log.write(json.dumps(dict(method=method, slot=slot,
                         request_id=request['id'], at_unix=time.time())) + '\n')
            mode = (directory / 'MODE').read_text().strip() if (directory / 'MODE').exists() else 'normal'
            if method == 'getBlocks':
                result = sorted(int(p.stem) for p in args.corpus.glob('*.json')
                                if p.stem.isdecimal() and slot <= int(p.stem) <= request['params'][1])
            else:
                result = json.loads((args.corpus / f'{slot}.json').read_bytes())['result']
            body = json.dumps(dict(jsonrpc='2.0', id=request['id'], result=result),
                              separators=(',', ':'), ensure_ascii=False).encode()
            del result
            if method == 'getBlock' and mode == 'exact':
                # Valid JSON trailing whitespace tests exact byte boundary only;
                # dense resident/protobuf waves use unchanged modeled log fields.
                body += b' ' * (RECOVERY_BLOCK_BYTES-len(body))
            elif method == 'getBlock' and mode == 'chunked-over':
                body += b' ' * (RECOVERY_BLOCK_BYTES+1-len(body))
            if method == 'getBlock':
                metrics.hold('upstream', len(body))
            try:
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                if mode == 'declared-over' and method == 'getBlock':
                    self.send_header('Content-Length', str(RECOVERY_BLOCK_BYTES+1))
                elif mode == 'chunked-over' and method == 'getBlock':
                    self.send_header('Transfer-Encoding', 'chunked')
                else:
                    self.send_header('Content-Length', str(len(body)))
                self.send_header('Connection', 'close')
                self.end_headers()
                if mode != 'declared-over':
                    for i in range(0, len(body), 65536):
                        chunk = memoryview(body)[i:i+65536]
                        if mode == 'chunked-over':
                            self.wfile.write(f'{len(chunk):x}\r\n'.encode())
                        self.wfile.write(chunk)
                        if mode == 'chunked-over':
                            self.wfile.write(b'\r\n')
                    if mode == 'chunked-over':
                        self.wfile.write(b'0\r\n\r\n')
            except OSError:
                pass
            self.close_connection = True

    server = ThreadingHTTPServer(('127.0.0.1', 18766), HTTPS)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(directory / 'server.pem', directory / 'server.key')
    server.socket = tls.wrap_socket(server.socket, server_side=True)
    return server


def backend(args, metrics):
    os.environ['BROKER_OFFLINE_TEST'] = '1'
    os.environ['SSL_CERT_FILE'] = str(args.directory / 'trusted.pem')
    original_archive, original_write = broker.archive, broker.write_frame

    def measured_archive(*parameters, **keywords):
        if parameters[2] == 'getBlock':
            metrics.hold('archive', len(parameters[5]))
        return original_archive(*parameters, **keywords)

    def measured_frame(sock, packet, *parameters):
        if packet.get('kind') == 'response' and packet.get('broker_trace', {}).get('method') == 'getBlock':
            metrics.hold('frame', len(packet['body']))
            mode = args.directory / 'MODE'
            if mode.exists() and mode.read_text().strip() == 'frame-over':
                # Deliberately malformed test peer: only the over-declared
                # header crosses UDS; production front must reject before body.
                sock.sendall(f'{RECOVERY_FRAME_BYTES+1:016x}'.encode())
                return
        return original_write(sock, packet, *parameters)
    broker.archive, broker.write_frame = measured_archive, measured_frame
    policy = json.loads((args.directory / 'policy.json').read_text())
    return broker.BackendServer('/relay/http.sock', args.directory,
                                args.directory / 'broker-ledger.sqlite3', policy)


def front(args, metrics):
    original = broker.deliver_response

    def measured_delivery(handler, packet, route, method):
        if method == 'getBlock':
            metrics.hold('front', len(packet['body']))
        return original(handler, packet, route, method)
    broker.deliver_response = measured_delivery
    return broker.FrontServer(('0.0.0.0', args.port), '/relay/http.sock',
                              args.directory, profile=RECOVERY_PROFILE)


def run(args):
    metrics = Metrics(args.directory, args.role)
    server = {'upstream': upstream, 'backend': backend, 'front': front}[args.role](args, metrics)
    stopped = threading.Event()
    for number in [signal.SIGINT, signal.SIGTERM]:
        signal.signal(number, lambda *_: stopped.set())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    save(args.directory / (args.role + '-ready.json'), dict(pid=os.getpid(), ready=True))
    last = None
    try:
        until = time.monotonic()+160
        while not stopped.wait(.05) and not (args.directory / 'STOP_TEST').exists():
            if time.monotonic() >= until:
                raise TimeoutError('offline_role_deadline')
            marker = args.directory / 'MEMORY_SNAPSHOT'
            if marker.exists() and marker.read_text() != last:
                last = marker.read_text()
                metrics.snapshot(last)
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
    run(parser.parse_args())
