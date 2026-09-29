"""Two-ended, fixed-route HTTP broker for one local canary window.

Front shares app's network-none namespace. Backend alone has bridge access.
Only framed requests cross their private Unix socket. No request/response logging.
"""
import base64
from contextlib import nullcontext
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

from evidence import archive, archive_failure, archive_delivery
from budget import Ledger, Refused
from policy import Gate, MAX_REQUEST, offline_target, response_limit, upstream, validate_route
from delivery import Delivery
from front_delivery import deliver_response
from frames import read_frame, write_frame
from diagnostics import broker_fact
from transport import HTTPStatus, UpstreamFailure, perform_upstream, rpc_error_redacted, verified_context

SOCKET_PATH = '/relay/http.sock'
FRONT_PORT = 18765
HOLD_UNKNOWN_SECONDS = 30  # Accepted app submit timeout is 3 seconds.
MAX_FRAME = 8_500_000
# 8 MiB raw body becomes 11,184,812 base64 bytes. Keep bounded space for
# the JSON envelope and the two forwarded upstream response headers.
MAX_GETBLOCK_FRAME = 12_000_000


def phase(delivery, name):
    return delivery.phase(name) if delivery is not None else nullcontext()


def frame_limit(route, rpc_method):
    return MAX_GETBLOCK_FRAME if (route, rpc_method) == ('rpc', 'getBlock') else MAX_FRAME




class BackendHandler(socketserver.BaseRequestHandler):
    def handle(self):
        rpc_method, reservation, stage = None, None, 'request'
        self.delivery = None
        try:
            packet = read_frame(self.request)
            raw = base64.b64decode(packet['body'], validate=True)
            stage = 'route'
            route, rpc_method = validate_route(packet['method'], packet['path'], raw)
            if len(raw) > MAX_REQUEST or packet['route'] != route:
                raise Refused('request_invalid')
            if route == 'quote':
                prices = self.server.policy.get('quote_price_nano', {})
                price = prices.get(packet['method'] + ' ' + urlsplit(packet['path']).path)
            else:
                price = None
            if self.server.policy.get('offline_stub'):
                offline_target(self.server.policy, route)
            else:
                upstream(self.server.policy, route)
            gate = Gate(self.server.control, self.server.policy)
            stage = 'gate'
            gate.check()
            if (route == 'rpc' and rpc_method in {'getBlock', 'getBlocks'} and
                    self.server.policy.get('profile') == 'read_only_http_recovery_v1'):
                self.delivery = Delivery(self.server.control, raw)
            # The ledger commit is durable before a possible billable outbound attempt.
            stage = 'reservation'
            with phase(self.delivery, stage):
                ledger = Ledger(self.server.ledger_path, self.server.policy)
                try:
                    reservation = ledger.reserve(route, rpc_method, price)
                finally:
                    ledger.close()
            stage = 'gate'
            with phase(self.delivery, stage):
                gate.outbound_attempt()
                # A STOP arriving after reservation still prevents this attempt.
                gate.check()
                if self.delivery is not None:
                    self.delivery.remaining()
        except Refused as error:
            self.failure('refused', rpc_method, reservation, stage, error)
            return
        except (ValueError, KeyError, TypeError, OSError, EOFError, sqlite3.Error) as error:
            self.failure('refused', rpc_method, reservation, stage, error)
            return
        is_submit = route == 'rpc' and rpc_method == 'sendTransaction'
        status = None
        try:
            stage = 'outbound'
            status, response_headers, response_body = perform_upstream(
                self.server.policy, route, rpc_method, packet['method'], packet['path'], raw,
                packet.get('headers', {}), self.delivery)
            if not 200 <= status <= 299:
                self.failure('failed', rpc_method, reservation, 'http_response',
                             HTTPStatus(), status, response_body)
                return
            with phase(self.delivery, 'response_sanitize'):
                redacted = rpc_error_redacted(response_body)
            stage = 'archive'
            if self.delivery is not None:
                self.delivery.remaining()
            with phase(self.delivery, stage):
                archive(self.server.control, reservation, rpc_method, raw, status, redacted,
                        response_body if redacted != response_body else None, self.delivery)
            if self.delivery is not None:
                self.delivery.remaining()
            stage = 'unix_send'
            with phase(self.delivery, 'frame_encode'):
                packet = {'kind': 'response', 'status': status, 'headers': response_headers,
                          'body': base64.b64encode(redacted).decode()}
            if self.delivery is not None:
                packet['broker_trace'] = self.delivery.trace(rpc_method, reservation)
            write_frame(self.request, packet,
                        frame_limit(route, rpc_method), self.delivery)
            if self.delivery is not None:
                archive_delivery(self.server.control, reservation, 'backend',
                                 self.delivery.trace(rpc_method, reservation), 'response')
        except UpstreamFailure as failure:
            self.failure('failed', rpc_method, reservation, failure.stage, failure.error, failure.status)
        except (OSError, EOFError, TimeoutError, ValueError, http.client.HTTPException, Refused) as error:
            # After sendTransaction may have crossed the boundary, no synthetic
            # HTTP/JSON-RPC response may cause the accepted app to record NotSent.
            self.failure('failed', rpc_method, reservation, stage, error, status)

    def failure(self, kind, method, reservation, stage, error, status=None, response=None):
        fact = broker_fact(kind, method, reservation, stage, error, status, self.delivery)
        archive_failure(self.server.control, fact, response)
        try:
            self.request.settimeout(1)
            write_frame(self.request, {'kind': kind, 'broker_error': fact})
        except (OSError, EOFError):
            pass  # The durable failure and original reservation remain visible.


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
    def _trace_headers(self, reservation=None):
        if getattr(self, 'delivery', None) is not None:
            self.send_header('X-Copybot-Session-Deadline-Unix-Ms',
                             str(self.delivery.session_deadline_unix_ms))
        if type(reservation) is int and reservation > 0:
            self.send_header('X-Copybot-Reservation-Id', str(reservation))

    def _broker_error(self, status, fact, persist=False):
        if persist:
            archive_failure(self.server.control, fact)
        data = json.dumps({'broker_error': fact}, separators=(',', ':')).encode()
        self.send_response(status)
        self._trace_headers(fact.get('reservation_id'))
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Connection', 'close')
        self.end_headers()
        try:
            self.wfile.write(data)
        except OSError:
            pass
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
        self.delivery = None
        try:
            route, rpc_method = validate_route(self.command, self.path, body)
            if route == 'rpc' and rpc_method in {'getBlock', 'getBlocks'}:
                self.delivery = Delivery(self.server.control, body)
        except Refused as error:
            self._broker_error(403, broker_fact('refused', None, None, 'route', error), persist=True)
            return
        is_submit = route == 'rpc' and rpc_method == 'sendTransaction'
        packet = {'route': route, 'method': self.command, 'path': self.path,
                  'body': base64.b64encode(body).decode(),
                  'headers': {k: self.headers[k] for k in ('Content-Type','Accept','x-api-key')
                              if k in self.headers}}
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as peer:
                peer.settimeout(self.delivery.remaining() if self.delivery is not None else 10)
                peer.connect(self.server.socket_path)
                write_frame(peer, packet)
                with phase(self.delivery, 'unix_receive'):
                    result = read_frame(peer, frame_limit(route, rpc_method), self.delivery)
                if self.delivery is not None:
                    self.delivery.remaining()
        except (OSError, EOFError, ValueError, Refused) as error:
            if isinstance(error, TimeoutError) and self.delivery is not None:
                try:
                    self.delivery.remaining()
                except (TimeoutError, Refused) as expired:
                    error = expired
            result = {'kind': 'uncertain' if is_submit else 'failed',
                      'broker_error': broker_fact('failed', rpc_method, None, 'unix_receive', error, delivery=self.delivery)}
            archive_failure(self.server.control, result['broker_error'])
        if result['kind'] == 'uncertain' and is_submit:
            # No headers, no body, no close until after the app's 3 s timeout.
            time.sleep(HOLD_UNKNOWN_SECONDS)
            self.close_connection = True
            return
        if result['kind'] == 'refused':
            self._broker_error(429, result['broker_error'])
            return
        if result['kind'] != 'response':
            self._broker_error(502, result['broker_error'])
            return
        deliver_response(self, result, route, rpc_method)


class FrontServer(ThreadingHTTPServer):
    daemon_threads = True
    def __init__(self, address, socket_path, control=None):
        self.socket_path = str(socket_path)
        self.control = Path(control) if control else Path(socket_path).parent
        super().__init__(address, FrontHandler)


def main():
    os.umask(0o077)
    role = sys.argv[1]
    control = Path('/control')
    if role == 'backend':
        policy = json.loads(Path(sys.argv[2]).read_text())
        verified_context()  # Validate effective trust before accepting any request/reservation.
        server = BackendServer(SOCKET_PATH, control, control/'broker-ledger.sqlite3', policy)
    elif role == 'front':
        server = FrontServer(('127.0.0.1', FRONT_PORT), SOCKET_PATH, control)
    else:
        raise SystemExit('role_denied')
    try:
        server.serve_forever(poll_interval=.1)
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
