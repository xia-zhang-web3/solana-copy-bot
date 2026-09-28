"""Closed local routes and authority checks; secrets never enter output."""
import json
import hashlib
import os
from pathlib import Path
import stat
import time
from urllib.parse import quote, urlsplit
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'helpers'))
from budget import CU, Refused

RPC_PATH = '/rpc'
MAX_REQUEST = 1_048_576
MAX_RESPONSE = 4_194_304
MAX_GETBLOCK_RESPONSE = 8_388_608


def response_limit(route, rpc_method):
    return MAX_GETBLOCK_RESPONSE if (route, rpc_method) == ('rpc', 'getBlock') else MAX_RESPONSE


def validate_route(http_method, path, body):
    parsed = urlsplit(path)
    if parsed.scheme or parsed.netloc or parsed.fragment:
        raise Refused('absolute_or_fragment_url_denied')
    if parsed.path == RPC_PATH and not parsed.query and http_method == 'POST':
        try:
            data = json.loads(body)
            method = data['method']
            if (not isinstance(data, dict) or not isinstance(method, str)
                    or method not in ('getBlock', 'getBlocks') or data.get('jsonrpc') != '2.0'
                    or not isinstance(data.get('params', []), list)):
                raise ValueError()
        except (ValueError, KeyError, TypeError):
            raise Refused('bad_or_unpriced_rpc') from None
        params = data.get('params', [])
        if method == 'getBlocks':
            if (len(params) != 3 or type(params[0]) is not int or type(params[1]) is not int
                    or params[0] <= 0 or not params[0] <= params[1] <= params[0] + 1023
                    or params[2] != {'commitment': 'confirmed'}):
                raise Refused('bounded_confirmed_range_required')
        elif (len(params) != 2 or type(params[0]) is not int or params[0] <= 0
                or params[1] != {'commitment': 'confirmed', 'encoding': 'json',
                                'transactionDetails': 'full', 'maxSupportedTransactionVersion': 1,
                                'rewards': True}):
            raise Refused('confirmed_full_version1_required')
        return 'rpc', method
    raise Refused('route_denied')


class Gate:
    def __init__(self, directory, policy):
        self.directory = Path(directory)
        self.policy = policy

    def check(self):
        d, p = self.directory, self.policy
        if p.get('profile') != 'read_only_http_recovery_v1' or not (d / 'STOP').is_file():
            raise Refused('permanent_financial_stop_required')
        if (d / 'HTTP_STOP').exists() or (d / 'STREAM_STOP').exists():
            raise Refused('read_only_stop_present')
        try:
            lease = json.loads((d / 'LEASE.json').read_text())
            clock = json.loads((d / 'PROBE_CLOCK.json').read_text())
            if (lease.get('generation') != 1 or lease.get('expires_unix', 0) <= time.time()
                    or clock.get('run_id') != p['run_id'] or clock.get('duration_seconds') != 480
                    or clock.get('deadline_unix') != clock.get('first_attempt_unix', 0) + 480
                    or time.time() >= clock['deadline_unix']):
                raise Refused('read_only_clock_or_lease_invalid')
        except (OSError, ValueError, KeyError, TypeError):
            raise Refused('read_only_clock_or_lease_invalid') from None

    def outbound_attempt(self):
        self.check()  # The first stream attempt owns the shared immutable deadline.


def upstream(policy, route):
    target = policy.get(route + '_upstream', '')
    parsed = urlsplit(target)
    if parsed.scheme != 'https' or not parsed.hostname or parsed.fragment or parsed.username:
        raise Refused('upstream_unbound')
    expected = ('solana-mainnet.g.alchemy.com' if route == 'rpc' else 'api.jup.ag')
    if parsed.hostname != expected or parsed.port not in (None, 443):
        raise Refused('upstream_host_denied')
    if route == 'rpc' and parsed.path != '/v2':
        raise Refused('rpc_path_unbound')
    if route == 'quote' and parsed.path.rstrip('/') != '/swap/v1':
        raise Refused('quote_path_unbound')
    if parsed.query:
        raise Refused('upstream_query_denied')
    if route == 'rpc':
        key = read_secret(policy, 'rpc')
        return parsed._replace(path='/v2/' + quote(key, safe=''))
    read_secret(policy, 'quote')
    return parsed


def read_secret(policy, route):
    name = {'rpc': '/run/provider/alchemy-api-key',
            'quote': '/run/provider/jupiter-api-key'}[route]
    if policy.get('offline_stub') and os.getenv('BROKER_OFFLINE_TEST') == '1':
        name = policy.get(route + '_key_path', '')
    if policy.get(route + '_key_path') != name:
        raise Refused('provider_key_path_unbound')
    path = Path(name)
    try:
        if stat.S_IMODE(path.stat().st_mode) != 0o600:
            raise Refused('provider_key_permissions')
        raw = path.read_bytes()
    except OSError:
        raise Refused('provider_key_missing') from None
    if (hashlib.sha256(raw).hexdigest() != policy.get(route + '_key_sha256')
            or not raw.strip() or b'\n' in raw.strip()):
        raise Refused('provider_key_identity_changed')
    try:
        return raw.decode('ascii').strip()
    except UnicodeDecodeError:
        raise Refused('provider_key_format') from None
