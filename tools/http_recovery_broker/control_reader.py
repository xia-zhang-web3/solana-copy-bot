"""Fresh read-only authority facts; retry only a missing local LEASE inode."""
import json
import math
from pathlib import Path
import time

from budget import Refused

VISIBILITY_SECONDS = .200
READ_ATTEMPTS = 6
BACKOFF_SECONDS = (.010, .020, .040, .080, .050)
PREDICATES = {'clock_read', 'clock_parse', 'clock_binding', 'clock_deadline',
              'lease_read', 'lease_parse', 'lease_generation', 'lease_expiry',
              'lease_visibility_exhausted', 'local_visibility_recovered',
              'financial_stop_required', 'read_only_stop'}
FILES = {'LEASE.json', 'PROBE_CLOCK.json', 'STOP', 'HTTP_STOP', 'STREAM_STOP'}
CAUSES = {'FileNotFoundError', 'PermissionError', 'OSError', 'UnicodeDecodeError',
          'JSONDecodeError', 'ValueError', 'predicate'}


def numeric(value):
    return type(value) in {int, float} and math.isfinite(value)


def millis(value):
    return int(value*1000) if numeric(value) and 0 <= value <= 2**53/1000 else None


def denied(predicate, fact, cause=None, reason='read_only_clock_or_lease_invalid'):
    error = Refused(reason)
    error.control_fact = dict(fact, gate_predicate=predicate,
                              control_cause_type=type(cause).__name__ if cause is not None else 'predicate')
    raise error from None


def read_json(directory, name, fact):
    fact['control_file'] = name
    try:
        raw = (Path(directory)/name).read_text()
    except FileNotFoundError:
        raise  # Only the LEASE caller owns a bounded visibility reread.
    except OSError as error:
        denied('lease_read' if name == 'LEASE.json' else 'clock_read', fact, error)
    except UnicodeDecodeError as error:
        denied('lease_parse' if name == 'LEASE.json' else 'clock_parse', fact, error)
    try:
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError()
        return value
    except (ValueError, TypeError) as error:
        denied('lease_parse' if name == 'LEASE.json' else 'clock_parse', fact, error)


def read_clock(directory, fact=None, run_id=None):
    fact = {} if fact is None else fact
    fact['observed_now_unix_ms'] = millis(time.time())
    try:
        clock = read_json(directory, 'PROBE_CLOCK.json', fact)
    except FileNotFoundError as error:
        denied('clock_read', fact, error)
    deadline, start = clock.get('deadline_unix'), clock.get('first_attempt_unix')
    fact['observed_deadline_unix_ms'] = millis(deadline)
    if (not numeric(deadline) or not numeric(start) or clock.get('duration_seconds') != 480
            or deadline != start+480):
        denied('clock_binding', fact)
    if run_id is not None and clock.get('run_id') != run_id:
        denied('clock_binding', fact)
    if time.time() >= deadline:
        denied('clock_deadline', fact)
    return clock


def stops(directory, policy, fact):
    d = Path(directory)
    fact['observed_now_unix_ms'] = millis(time.time())
    if policy.get('profile') != 'read_only_http_recovery_v1' or not (d/'STOP').is_file():
        fact['control_file'] = 'STOP'
        denied('financial_stop_required', fact, reason='permanent_financial_stop_required')
    for name in ('HTTP_STOP', 'STREAM_STOP'):
        if (d/name).exists():
            fact['control_file'] = name
            denied('read_only_stop', fact, reason='read_only_stop_present')


def check_control(directory, policy):
    """Return fresh facts; six missing reads cannot renew authority or its clock."""
    start = time.monotonic()
    fact = {'local_read_attempts': 0, 'local_read_elapsed_ms': 0}
    for attempt in range(READ_ATTEMPTS):
        fact['local_read_elapsed_ms'] = round((time.monotonic()-start)*1000)
        stops(directory, policy, fact)
        clock = read_clock(directory, fact, policy['run_id'])
        fact['local_read_attempts'] = attempt+1
        try:
            lease = read_json(directory, 'LEASE.json', fact)
        except FileNotFoundError as error:
            fact.setdefault('first_missing_snapshot', dict(control_file='LEASE.json',
                control_cause_type='FileNotFoundError', observed_now_unix_ms=millis(time.time()),
                observed_deadline_unix_ms=millis(clock['deadline_unix'])))
            fact['local_read_elapsed_ms'] = round((time.monotonic()-start)*1000)
            remaining = min(VISIBILITY_SECONDS-(time.monotonic()-start), clock['deadline_unix']-time.time())
            if attempt+1 >= READ_ATTEMPTS or remaining <= 0:
                stops(directory, policy, fact)
                read_clock(directory, fact, policy['run_id'])
                fact['control_file'] = 'LEASE.json'
                denied('lease_visibility_exhausted', fact, error)
            time.sleep(min(BACKOFF_SECONDS[attempt], remaining))
            continue
        # Renew publication succeeded: check current STOP and immutable clock again.
        stops(directory, policy, fact)
        clock = read_clock(directory, fact, policy['run_id'])
        now = time.time()
        fact.update(control_file='LEASE.json', observed_now_unix_ms=millis(now),
                    observed_lease_expiry_unix_ms=millis(lease.get('expires_unix')),
                    observed_deadline_unix_ms=millis(clock['deadline_unix']),
                    observed_lease_generation=lease.get('generation'),
                    local_read_elapsed_ms=round((time.monotonic()-start)*1000))
        if type(lease.get('generation')) is not int or lease['generation'] != 1:
            denied('lease_generation', fact)
        if not numeric(lease.get('expires_unix')):
            denied('lease_parse', fact)
        if lease['expires_unix'] <= now:
            denied('lease_expiry', fact)
        if now >= clock['deadline_unix']:
            denied('clock_deadline', fact)
        if attempt and time.monotonic()-start >= VISIBILITY_SECONDS:
            denied('lease_visibility_exhausted', fact)
        if attempt:
            fact['gate_predicate'] = 'local_visibility_recovered'
        return fact


def request_facts(raw):
    value = json.loads(raw)
    identity, params = value.get('id'), value.get('params', [])
    return {'request_id': identity if type(identity) is int and 0 <= identity <= 2**64-1 else None,
            'slot': params[0] if params and type(params[0]) is int and 0 < params[0] <= 2**64-1 else None}


def safe_control_fact(value, include_first=True):
    """Allowlisted immutable wire snapshot; arbitrary file contents never pass."""
    if not isinstance(value, dict):
        return {}
    fact = {}
    for key, allowed in [('gate_predicate', PREDICATES), ('control_file', FILES),
                         ('control_cause_type', CAUSES)]:
        if value.get(key) in allowed:
            fact[key] = value[key]
    for key in ['observed_now_unix_ms', 'observed_lease_expiry_unix_ms', 'observed_deadline_unix_ms',
                'observed_lease_generation', 'local_read_attempts', 'local_read_elapsed_ms']:
        if type(value.get(key)) is int and 0 <= value[key] <= 2**53:
            fact[key] = value[key]
    if include_first and isinstance(value.get('first_missing_snapshot'), dict):
        fact['first_missing_snapshot'] = safe_control_fact(value['first_missing_snapshot'], False)
    return fact
