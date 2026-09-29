"""Closed diagnostic facts. Never persist provider bodies, URLs or exception text."""
import http.client
import json
import socket
import ssl
import subprocess
from control_reader import safe_control_fact
from size_contract import safe_size_fact

METHODS = {'getGenesisHash', 'getBalance', 'getAccountInfo',
           'getMinimumBalanceForRentExemption', 'getTokenAccountsByOwner'}
REASONS = {
    'http_status', 'response_too_large', 'invalid_json', 'invalid_rpc_envelope',
    'rpc_error', 'wrong_genesis', 'bad_balance', 'mint_classic_owner',
    'mint_initialized_decimals', 'bad_rent', 'inventory_too_large',
    'inventory_owner', 'existing_nonzero_token_inventory', 'existing_wsol_account',
    'funding_or_native_decline_cap', 'invalid_wallet_data',
    'tls_certificate', 'tls_protocol', 'dns', 'timeout', 'connection_refused',
    'connection_reset', 'http_protocol', 'io_error', 'upstream_unavailable',
    'upstream_response_too_large', 'stop_present', 'lease_or_clock_invalid',
    'clock_unavailable', 'invalid_or_unavailable', 'transport_error',
    'unclassified_failure', 'helper_exit', 'helper_timeout', 'helper_invalid_json',
    'tls_hostname', 'ca_binding_missing', 'ca_binding_invalid',
    'test_upstream_forbidden', 'absolute_or_fragment_url_denied', 'bad_or_unpriced_rpc',
    'bounded_confirmed_range_required', 'confirmed_full_version1_required', 'route_denied',
    'permanent_financial_stop_required', 'read_only_stop_present', 'read_only_clock_or_lease_invalid',
    'upstream_unbound', 'upstream_host_denied', 'rpc_path_unbound', 'quote_path_unbound',
    'upstream_query_denied', 'provider_key_path_unbound', 'provider_key_permissions',
    'provider_key_missing', 'provider_key_identity_changed', 'provider_key_format',
    'ledger_lost_after_first_open', 'ledger_binding_changed', 'ledger_marker_binding_changed',
    'read_only_recovery_method_required', 'unknown_rpc_method_or_price', 'unknown_quote_price',
    'unknown_route', 'http_or_rpc_cap_exhausted', 'request_invalid', 'quote_key_unbound',
    'deadline_exhausted', 'session_deadline_exhausted',
    'frame_too_large',
}
BROKER_METHODS = {'getBlocks', 'getBlock'}
BROKER_STAGES = {'request', 'route', 'gate', 'reservation', 'outbound',
                 'connect', 'tls', 'http_response', 'archive', 'transport',
                 'upstream_headers', 'upstream_body', 'unix_send', 'unix_receive',
                 'frame_encode', 'front_headers', 'front_body', 'front_decode'}
CAUSE_TYPES = {'Refused', 'ValueError', 'KeyError', 'TypeError', 'OSError',
               'EOFError', 'SSLCertVerificationError', 'SSLError', 'gaierror',
               'TimeoutError', 'ConnectionRefusedError', 'ConnectionResetError',
               'RemoteDisconnected', 'IncompleteRead', 'BadStatusLine',
               'HTTPException', 'HTTPStatus', 'OperationalError', 'IntegrityError', 'Error',
               'DeadlineExceededTimeout', 'BrokenPipeError'}


def broker_fact(kind, method, reservation, stage, error, status=None, delivery=None, request=None):
    """Closed wire facts; raw exception text may contain provider credentials."""
    reason = exception_reason(error)
    verify = getattr(error, 'verify_code', None) if isinstance(error, ssl.SSLCertVerificationError) else None
    verify = verify if type(verify) is int and 0 <= verify <= 2147483647 else None
    if verify in {62, 64}:
        reason = 'tls_hostname'
    if type(error).__name__ == 'Refused' and len(error.args) == 1:
        candidate = error.args[0]
        reason = candidate if candidate in REASONS else 'invalid_or_unavailable'
    value = {'schema': 'http_recovery_broker_v1',
            'kind': kind if kind in {'failed', 'refused'} else 'failed',
            'method': method if method in BROKER_METHODS else None,
            'reservation_id': reservation if type(reservation) is int and reservation > 0 else None,
            'stage': stage if stage in BROKER_STAGES else 'transport',
            'reason': reason, 'cause_type': type(error).__name__ if type(error).__name__ in CAUSE_TYPES else 'Error',
            'http_status': status if type(status) is int and 100 <= status <= 599 else None,
            'verify_code': verify}
    if delivery is not None:
        value.update(delivery.facts())
    if isinstance(request, dict):
        for key in ('request_id', 'slot'):
            item = request.get(key)
            if type(item) is int and 0 <= item <= 2**64-1:
                value[key] = item
    value.update(safe_control_fact(getattr(error, 'control_fact', None)))
    value.update(safe_size_fact(getattr(error, 'size_fact', None)))
    return value


def exception_reason(error):
    if type(error).__name__ == 'HTTPStatus':
        return 'http_status'
    if type(error).__name__ == 'DeadlineExceededTimeout':
        return 'deadline_exhausted'
    # Ordering matters: TLS and DNS failures are also OSError instances.
    for kind, reason in (
        (ssl.SSLCertVerificationError, 'tls_certificate'), (ssl.SSLError, 'tls_protocol'),
        (socket.gaierror, 'dns'), ((TimeoutError, subprocess.TimeoutExpired), 'timeout'),
        (ConnectionRefusedError, 'connection_refused'),
        (ConnectionResetError, 'connection_reset'),
        (http.client.HTTPException, 'http_protocol'), (OSError, 'io_error'),
    ):
        if isinstance(error, kind):
            return reason
    if type(error) is ValueError and error.args in {('upstream_response_too_large',), ('frame_too_large',),
                                                  ('response_too_large',)}:
        return error.args[0]
    return 'unclassified_failure'


def clean_fact(value):
    if not isinstance(value, dict):
        return {}
    fact = {}
    for key, allowed in (('stage', {'rpc', 'evaluate', 'helper', 'supervisor'}),
                         ('reason', REASONS), ('method', METHODS), ('broker_reason', REASONS)):
        item = value.get(key)
        if isinstance(item, str) and item in allowed:
            fact[key] = item
    for key, lo, hi in (('http_status', 100, 599), ('rpc_error_code', -2147483648, 2147483647),
                        ('returncode', -255, 255)):
        number = value.get(key)
        if type(number) is int and lo <= number <= hi:
            fact[key] = number
    return fact


class DiagnosticFailure(ValueError):
    def __init__(self, fact):
        self.fact = clean_fact(fact)
        super().__init__(self.fact.get('reason', 'unclassified_failure'))


def helper_failure(returncode, stdout):
    fact = {'stage': 'helper', 'reason': 'helper_exit', 'returncode': returncode}
    # stdout is untrusted even when it came from an accepted helper.
    try:
        if len(stdout) > 16384:
            raise ValueError()
        value = json.loads(stdout)
        diagnostic = clean_fact(value.get('diagnostic')) if isinstance(value, dict) else {}
        if diagnostic.get('reason'):
            fact.update(diagnostic)
    except (ValueError, TypeError):
        pass
    return DiagnosticFailure(fact)
