"""Local HTTPS plus the actual broker, reusable by causal daemon parser tests.

`--serve --directory DIR --mode MODE` writes ready.json with front_url. All
upstreams are loopback; STOP_TEST or SIGTERM ends it. No provider keys exist.
"""
import argparse
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import ssl
import subprocess
import sys
import tempfile
import threading
import time

CODE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CODE / 'tools/http_recovery_broker'))
from broker import BackendServer, FrontServer

MODES = ['trusted', 'untrusted', 'hostname', 'status', 'wrong-id']
SECRET_MARKER = 'FAKE_OFFLINE_URL_CREDENTIAL_MUST_NOT_ARCHIVE'


def save(path, value):
    Path(path).write_text(json.dumps(value, sort_keys=True))


def certificates(directory):
    openssl = '/opt/homebrew/opt/openssl@3/bin/openssl'
    if not Path(openssl).exists():
        openssl = 'openssl'

    def run(*args):
        subprocess.run([openssl, *args], check=True, capture_output=True, timeout=10)

    for name in ['trusted', 'untrusted']:
        run('req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
            '-subj', '/CN=copybot-local-' + name, '-addext', 'basicConstraints=critical,CA:TRUE',
            '-keyout', str(directory / (name + '.key')), '-out', str(directory / (name + '.pem')))
    run('req', '-newkey', 'rsa:2048', '-nodes', '-subj', '/CN=localhost',
        '-keyout', str(directory / 'server.key'), '-out', str(directory / 'server.csr'))
    (directory / 'server.ext').write_text('subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n')
    run('x509', '-req', '-in', str(directory / 'server.csr'), '-days', '1',
        '-CA', str(directory / 'trusted.pem'), '-CAkey', str(directory / 'trusted.key'),
        '-CAcreateserial', '-extfile', str(directory / 'server.ext'), '-out', str(directory / 'server.pem'))
    for path in directory.iterdir():
        if path.is_file():
            path.chmod(0o600)


def upstream(directory, port=0):
    class LocalHTTPS(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            if self.path == '/status':
                status, body = 503, SECRET_MARKER.encode()
            else:
                offset = 1 if self.path == '/wrong-id' else 0
                status = 200
                body = json.dumps({'jsonrpc': '2.0', 'id': request['id'] + offset,
                                   'result': [request['params'][0]]}).encode()
            self.send_response(status)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(('127.0.0.1', port), LocalHTTPS)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(directory / 'server.pem', directory / 'server.key')
    server.socket = context.wrap_socket(server.socket, server_side=True)
    return server


def policy(directory, mode, port):
    directory.mkdir(exist_ok=True)
    start = time.time()
    (directory / 'STOP').touch()
    save(directory / 'LEASE.json', {'generation': 1, 'expires_unix': start + 480})
    save(directory / 'PROBE_CLOCK.json', {'run_id': 'offline-tls', 'first_attempt_unix': start,
         'duration_seconds': 480, 'deadline_unix': start + 480})
    hostname = '127.0.0.1' if mode == 'hostname' else 'localhost'
    path = '/' + mode if mode in ['status', 'wrong-id'] else '/rpc'
    value = {'run_id': 'offline-tls', 'profile': 'read_only_http_recovery_v1', 'offline_stub': True,
             'rpc_upstream': f'https://{hostname}:{port}{path}', 'max_rpc_attempts': 2,
             'max_rpc_cu': 20, 'prior_http_nano_usd': 9_990_750,
             'prior_model_nano_usd': 7_960_362_665, 'stream_reserved_nano_usd': 400_000_000}
    save(directory / 'http-policy.json', value)
    return value


def serve(directory, mode):
    directory.mkdir(parents=True, exist_ok=True)
    certificates(directory)
    os.environ['BROKER_OFFLINE_TEST'] = '1'
    ca = directory / ('untrusted.pem' if mode == 'untrusted' else 'trusted.pem')
    os.environ['SSL_CERT_FILE'] = str(ca.resolve())
    https = upstream(directory)
    sockets = tempfile.TemporaryDirectory(prefix='cbht-')  # AF_UNIX paths are short on macOS.
    socket_path = Path(sockets.name) / 'http.sock'
    backend = BackendServer(socket_path, directory, directory / 'broker-ledger.sqlite3',
                            policy(directory, mode, https.server_port))
    front = FrontServer(('127.0.0.1', 0), socket_path, directory)
    servers = [https, backend, front]
    for server in servers:
        threading.Thread(target=server.serve_forever, daemon=True).start()
    save(directory / 'ready.json', {'front_url': f'http://127.0.0.1:{front.server_port}/rpc',
                                  'upstream_port': https.server_port, 'mode': mode})
    stop = threading.Event()
    for number in [signal.SIGINT, signal.SIGTERM]:
        signal.signal(number, lambda *_: stop.set())
    try:
        deadline = time.monotonic() + 120
        while not stop.wait(.05) and not (directory / 'STOP_TEST').exists():
            if time.monotonic() >= deadline:
                raise TimeoutError('offline_fixture_deadline')
    finally:
        for server in reversed(servers):
            server.shutdown()
            server.server_close()
        sockets.cleanup()


def client():
    connection = http.client.HTTPConnection('127.0.0.1', 18765, timeout=10)
    request = {'jsonrpc': '2.0', 'id': 7, 'method': 'getBlocks',
               'params': [123, 124, {'commitment': 'confirmed'}]}
    connection.request('POST', '/rpc', json.dumps(request), {'Content-Type': 'application/json'})
    response = connection.getresponse()
    print(json.dumps({'status': response.status, 'body': json.loads(response.read())}, sort_keys=True))
    connection.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--directory', type=Path)
    parser.add_argument('--mode', choices=MODES, default='trusted')
    parser.add_argument('--serve', action='store_true')
    parser.add_argument('--role', choices=['upstream', 'client'])
    args = parser.parse_args()
    if args.serve:
        serve(args.directory, args.mode)
    elif args.role == 'client':
        client()
    else:
        server = upstream(args.directory, 18766)
        save(args.directory / 'upstream-ready.json', {'ready': True})
        server.serve_forever()
