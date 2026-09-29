"""Fixed upstream HTTPS transport with explicit mounted trust and closed failures."""
import http.client
import json
import os
from pathlib import Path
import ssl
import time
from urllib.parse import urlsplit

from budget import Refused
from delivery import Delivery, UPSTREAM_SECONDS
from policy import offline_target, read_secret, response_limit, upstream


def verified_context():
    """The cached Python image has no usable default trust; pin the mounted file."""
    cafile = os.environ.get('SSL_CERT_FILE', '')
    if not cafile or not Path(cafile).is_absolute():
        raise Refused('ca_binding_missing')
    context = ssl.create_default_context(cafile=cafile)
    if not context.check_hostname or context.verify_mode != ssl.CERT_REQUIRED or not context.get_ca_certs():
        raise Refused('ca_binding_invalid')
    return context


class UpstreamFailure(Exception):
    def __init__(self, error, stage, status):
        self.error, self.stage, self.status = error, stage, status


class HTTPStatus(Exception):
    pass


def rpc_error_redacted(body):
    """Upstream error messages/data can echo a credential-bearing URL."""
    try:
        value = json.loads(body)
        error = value.get('error') if isinstance(value, dict) else None
        if isinstance(error, dict):
            code = error.get('code')
            identity = value.get('id')
            sanitized = {'jsonrpc': '2.0' if value.get('jsonrpc') == '2.0' else None,
                         'id': identity if type(identity) is int else None,
                         'error': {'code': code if type(code) is int else None}}
            return json.dumps(sanitized, separators=(',', ':')).encode()
    except (ValueError, TypeError):
        pass
    return body


def perform_upstream(policy, route, rpc_method, method, path, body, headers, delivery=None):
    # In test mode only, use a fake loopback upstream; never present in live binding.
    if policy.get('offline_stub'):
        target = offline_target(policy, route)
    else:
        target = upstream(policy, route)
    connection, stage, status = None, 'tls', None
    upstream_end = time.monotonic() + UPSTREAM_SECONDS

    def remaining():
        if delivery is not None:
            return delivery.remaining(upstream_end)
        return 5  # Unchanged non-recovery socket-timeout contract.

    def phase(name):
        from contextlib import nullcontext
        return delivery.phase(name) if delivery is not None else nullcontext()
    try:
        if target.scheme == 'https':
            connection = http.client.HTTPSConnection(target.hostname, target.port or 443,
                                                      timeout=remaining(), context=verified_context())
        else:
            connection = http.client.HTTPConnection(target.hostname, target.port, timeout=remaining())
    except (OSError, ValueError, ssl.SSLError, Refused) as error:
        raise UpstreamFailure(error, stage, status) from None
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
        stage = 'connect'
        with phase('connect'):
            connection.connect()
        stage = 'upstream_headers' if delivery is not None else 'outbound'
        connection.sock.settimeout(remaining())
        with phase(stage):
            connection.request(method, target_path, body=body if method == 'POST' else None,
                               headers=forwarded)
            wire_socket = connection.sock
            response = connection.getresponse()
        stage, status = ('upstream_body' if delivery is not None else 'http_response'), response.status
        maximum = response_limit(route, rpc_method)
        if response.length is not None and response.length > maximum:
            raise ValueError('upstream_response_too_large')
        if delivery is None:
            raw = response.read(maximum+1)
            if len(raw) > maximum:
                raise ValueError('upstream_response_too_large')
        else:
            parts, size, expected = [], 0, response.length
            with phase(stage):
                while not response.isclosed():
                    wire_socket.settimeout(remaining())
                    chunk = response.read1(min(65536, maximum + 1 - size))
                    if not chunk:
                        break
                    parts.append(chunk)
                    size += len(chunk)
                    if size > maximum:
                        raise ValueError('upstream_response_too_large')
                if expected is not None and size != expected:
                    raise http.client.IncompleteRead(b'', expected-size)
            remaining()
            raw = b''.join(parts)
        response_headers = {}
        for key in ('content-type', 'content-encoding'):
            value = response.getheader(key)
            if value:
                response_headers[key] = value
        return response.status, response_headers, raw
    except (OSError, EOFError, TimeoutError, ValueError, http.client.HTTPException, Refused) as error:
        if isinstance(error, TimeoutError) and delivery is not None:
            try:
                delivery.remaining(upstream_end)
            except (TimeoutError, Refused) as expired:
                error = expired
        stage = 'tls' if isinstance(error, ssl.SSLError) else stage
        raise UpstreamFailure(error, stage, status) from None
    finally:
        connection.close()
