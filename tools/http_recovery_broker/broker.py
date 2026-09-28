"""Two-ended, fixed-route HTTP broker for one local canary window.

Front shares app's network-none namespace. Backend alone has bridge access.
Only framed requests cross their private Unix socket. No request/response logging.
"""
import base64
import binascii
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import socketserver
import sqlite3
import sys
import time
from urllib.parse import urlsplit

from evidence import archive
from budget import Ledger, Refused
from policy import Gate, MAX_REQUEST, read_secret, response_limit, upstream, validate_route
from diagnostics import REASONS, exception_reason

SOCKET_PATH = '/relay/http.sock'
FRONT_PORT = 18765
HOLD_UNKNOWN_SECONDS = 30  # Accepted app submit timeout is 3 seconds.
MAX_FRAME = 8_500_000
# 8 MiB raw body becomes 11,184,812 base64 bytes. Keep bounded space for
# the JSON envelope and the two forwarded upstream response headers.
MAX_GETBLOCK_FRAME = 12_000_000


def frame_limit(route, rpc_method):
    return MAX_GETBLOCK_FRAME if (route, rpc_method) == ('rpc', 'getBlock') else MAX_FRAME


def read_frame(sock, maximum=MAX_FRAME):
    head = bytearray()
    while len(head) < 16:
        part = sock.recv(16 - len(head))
        if not part:
            raise EOFError()
        head.extend(part)
    size = int(head.decode('ascii'), 16)
    if size > maximum:
        raise ValueError('frame_too_large')
    data = bytearray()
    while len(data) < size:
        part = sock.recv(min(65536, size-len(data)))
        if not part:
            raise EOFError()
        data.extend(part)
    return json.loads(data)


def write_frame(sock, value, maximum=MAX_FRAME):
    data = json.dumps(value, separators=(',', ':')).encode()
    if len(data) > maximum:
        raise ValueError('frame_too_large')
    sock.sendall(f'{len(data):016x}'.encode() + data)


def perform_upstream(policy, route, rpc_method, method, path, body, headers):
    # In test mode only, use a fake loopback upstream; never present in live binding.
    if policy.get('offline_stub'):
        if os.getenv('BROKER_OFFLINE_TEST') != '1':
            raise Refused('test_upstream_forbidden')
        target = urlsplit(policy[route + '_upstream'])
        if target.scheme != 'http' or target.hostname != '127.0.0.1':
            raise Refused('test_upstream_forbidden')
        connection = http.client.HTTPConnection(target.hostname, target.port, timeout=5)
    else:
        target = upstream(policy, route)
        connection = http.client.HTTPSConnection(target.hostname, target.port or 443, timeout=5)
    if route == 'rpc':
        target_path = target.path
    else:
        suffix = urlsplit(path).path[len('/swap/v1'):]
        target_path = target.path.rstrip('/') + suffix
    query = urlsplit(path).query
    if query:
        target_path += '?' + query
    forwarded = {'Content-Type': headers.get('content-type', 'application/json'),
                 'Accept': headers.get('accept', 'application/json'),
                 'Connection': 'close'}
    if route == 'quote':
        if policy.get('quote_key_path'):
            forwarded['x-api-key'] = read_secret(policy, 'quote')
        elif policy.get('offline_stub'):
            if headers.get('x-api-key'):
                forwarded['x-api-key'] = headers['x-api-key']
        else:
            raise Refused('quote_key_unbound')
    try:
        connection.request(method, target_path, body=body if method == 'POST' else None,
                           headers=forwarded)
        response = connection.getresponse()
        maximum = response_limit(route, rpc_method)
        if response.length is not None and response.length > maximum:
            raise ValueError('upstream_response_too_large')
        raw = response.read(maximum+1)
        if len(raw) > maximum:
            raise ValueError('upstream_response_too_large')
        response_headers = {}
        for key in ('content-type', 'content-encoding'):
            value = response.getheader(key)
            if value:
                response_headers[key] = value
        return response.status, response_headers, raw
    finally:
        connection.close()


class BackendHandler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            packet = read_frame(self.request)
            raw = base64.b64decode(packet['body'], validate=True)
            route, rpc_method = validate_route(packet['method'], packet['path'], raw)
            if len(raw) > MAX_REQUEST or packet['route'] != route:
                raise Refused('request_invalid')
            if route == 'quote':
                prices = self.server.policy.get('quote_price_nano', {})
                price = prices.get(packet['method'] + ' ' + urlsplit(packet['path']).path)
            else:
                price = None
            if self.server.policy.get('offline_stub'):
                target = urlsplit(self.server.policy.get(route + '_upstream', ''))
                if (os.getenv('BROKER_OFFLINE_TEST') != '1' or target.scheme != 'http'
                        or target.hostname != '127.0.0.1'):
                    raise Refused('test_upstream_forbidden')
            else:
                upstream(self.server.policy, route)
            gate = Gate(self.server.control, self.server.policy)
            gate.check()
            # The ledger commit is durable before a possible billable outbound attempt.
            ledger = Ledger(self.server.ledger_path, self.server.policy)
            try:
                reservation = ledger.reserve(route, rpc_method, price)
            finally:
                ledger.close()
            gate.outbound_attempt()
            # A STOP arriving after reservation still prevents this attempt.
            gate.check()
        except Refused as error:
            write_frame(self.request, {'kind': 'refused', 'reason': str(error)})
            return
        except (ValueError, KeyError, TypeError, OSError, EOFError, sqlite3.Error):
            write_frame(self.request, {'kind': 'refused', 'reason': 'invalid_or_unavailable'})
            return
        is_submit = route == 'rpc' and rpc_method == 'sendTransaction'
        try:
            status, response_headers, response_body = perform_upstream(
                self.server.policy, route, rpc_method, packet['method'], packet['path'], raw,
                packet.get('headers', {}))
            archive(self.server.control, reservation, rpc_method, raw, status, response_body)
            write_frame(self.request, {'kind': 'response', 'status': status,
                                       'headers': response_headers,
                                       'body': base64.b64encode(response_body).decode()},
                        frame_limit(route, rpc_method))
        except (OSError, EOFError, TimeoutError, ValueError, http.client.HTTPException, Refused) as error:
            # After sendTransaction may have crossed the boundary, no synthetic
            # HTTP/JSON-RPC response may cause the accepted app to record NotSent.
            write_frame(self.request, {'kind': 'uncertain' if is_submit else 'failed',
                                       'reason': exception_reason(error)})


class BackendServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
    allow_reuse_address = False
    def __init__(self, socket_path, control, ledger_path, policy):
        self.control = Path(control)
        self.ledger_path = Path(ledger_path)
        self.policy = policy
        socket_path = Path(socket_path)
        if socket_path.exists():
            socket_path.unlink()
        super().__init__(str(socket_path), BackendHandler)
        os.chmod(socket_path, 0o600)


class FrontHandler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def log_message(self, *_args):
        pass
    def do_CONNECT(self):
        self._error(405, 'connect_denied')
    def do_GET(self):
        self._forward()
    def do_POST(self):
        self._forward()
    def do_PUT(self):
        self._error(405, 'method_denied')
    def _error(self, status, reason):
        data = json.dumps({'error': reason}).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(data)
        self.close_connection = True
    def _forward(self):
        raw_length = self.headers.get('Content-Length', '0')
        if not raw_length.isdecimal() or int(raw_length) > MAX_REQUEST:
            self._error(413, 'request_too_large')
            return
        if self.headers.get('Transfer-Encoding') or self.headers.get('Upgrade'):
            self._error(400, 'transfer_or_upgrade_denied')
            return
        if self.headers.get('Host') not in ('127.0.0.1:'+str(self.server.server_port),
                                            'localhost:'+str(self.server.server_port)):
            self._error(400, 'host_denied')
            return
        body = self.rfile.read(int(raw_length))
        try:
            route, rpc_method = validate_route(self.command, self.path, body)
        except Refused as error:
            self._error(403, str(error))
            return
        is_submit = route == 'rpc' and rpc_method == 'sendTransaction'
        packet = {'route': route, 'method': self.command, 'path': self.path,
                  'body': base64.b64encode(body).decode(),
                  'headers': {k: self.headers[k] for k in ('Content-Type','Accept','x-api-key')
                              if k in self.headers}}
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as peer:
                peer.settimeout(10)
                peer.connect(self.server.socket_path)
                write_frame(peer, packet)
                result = read_frame(peer, frame_limit(route, rpc_method))
        except (OSError, EOFError, ValueError):
            result = {'kind': 'uncertain' if is_submit else 'failed'}
        if result['kind'] == 'uncertain' and is_submit:
            # No headers, no body, no close until after the app's 3 s timeout.
            time.sleep(HOLD_UNKNOWN_SECONDS)
            self.close_connection = True
            return
        if result['kind'] == 'refused':
            self._error(429, result['reason'])
            return
        if result['kind'] != 'response':
            reason = result.get('reason')
            self._error(502, reason if isinstance(reason, str) and reason in REASONS else 'upstream_unavailable')
            return
        try:
            data = base64.b64decode(result['body'], validate=True)
            if len(data) > response_limit(route, rpc_method):
                raise ValueError('response_too_large')
        except (ValueError, KeyError, TypeError, binascii.Error):
            self._error(502, 'upstream_unavailable')
            return
        self.send_response(result['status'])
        for key, value in result['headers'].items():
            self.send_header(key, value)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(data)
        self.close_connection = True


class FrontServer(ThreadingHTTPServer):
    daemon_threads = True
    def __init__(self, address, socket_path):
        self.socket_path = str(socket_path)
        super().__init__(address, FrontHandler)


def main():
    os.umask(0o077)
    role = sys.argv[1]
    control = Path('/control')
    if role == 'backend':
        policy = json.loads(Path(sys.argv[2]).read_text())
        server = BackendServer(SOCKET_PATH, control, control/'broker-ledger.sqlite3', policy)
    elif role == 'front':
        server = FrontServer(('127.0.0.1', FRONT_PORT), SOCKET_PATH)
    else:
        raise SystemExit('role_denied')
    try:
        server.serve_forever(poll_interval=.1)
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
