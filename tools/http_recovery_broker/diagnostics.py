"""Closed diagnostic facts. Never persist provider bodies, URLs or exception text."""
import http.client
import json
import socket
import ssl
import subprocess

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
}


def exception_reason(error):
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
    if type(error) is ValueError and error.args == ('upstream_response_too_large',):
        return 'upstream_response_too_large'
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
