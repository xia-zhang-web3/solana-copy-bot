"""Immutable bounded HTTP evidence before client delivery, with no credentials."""
import hashlib
import json
import os
from pathlib import Path
import time
import uuid


def persist(directory, name, data, timings=None):
    directory = Path(directory) / 'http-evidence'
    directory.mkdir(mode=0o700, exist_ok=True)
    fd = os.open(directory / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        start = time.monotonic()
        stream.write(data)
        stream.flush()
        if timings is not None:
            timings['archive_write'] = timings.get('archive_write', 0) + round((time.monotonic()-start)*1000)
        start = time.monotonic()
        os.fsync(stream.fileno())
        if timings is not None:
            timings['archive_fsync'] = timings.get('archive_fsync', 0) + round((time.monotonic()-start)*1000)
    fd = os.open(directory, os.O_RDONLY)
    try:
        start = time.monotonic()
        os.fsync(fd)
        if timings is not None:
            timings['archive_fsync'] = timings.get('archive_fsync', 0) + round((time.monotonic()-start)*1000)
    finally:
        os.close(fd)


def archive_failure(directory, fact, response=None):
    """Refusals before reservation use an opaque unique identity, never a URL."""
    reservation = fact['reservation_id']
    identity = f'{reservation:06}' if reservation is not None else uuid.uuid4().hex
    value = dict(fact)
    if response is not None:
        value.update(response_bytes=len(response), response_sha256=hashlib.sha256(response).hexdigest())
    persist(directory, f'failure-{identity}.json', json.dumps(value, sort_keys=True).encode())


def archive(directory, reservation, method, request, status, response, original=None, delivery=None):
    value = {'method': method, 'request': json.loads(request), 'request_sha256': hashlib.sha256(request).hexdigest(),
             'status': status, 'bytes': len(response),
             'response_sha256': hashlib.sha256(response).hexdigest()}
    if original is not None:
        value.update(original_response_bytes=len(original),
                     original_response_sha256=hashlib.sha256(original).hexdigest(),
                     rpc_error_message_redacted=True)
    if delivery is not None:
        value['broker_trace'] = delivery.trace(method, reservation)
    start = time.monotonic()
    encoded = json.dumps(value).encode()
    timings = delivery.timings if delivery is not None else None
    if timings is not None:
        timings['archive_meta_encode'] = round((time.monotonic()-start)*1000)
    for suffix, data in [('.json', response), ('.meta.json', encoded)]:
        persist(directory, f'response-{reservation:06}{suffix}', data, timings)


def archive_delivery(directory, reservation, side, trace, outcome, backend_trace=None, fact=None):
    """Separate immutable completion record; raw response/meta stay unchanged."""
    identity = f'{reservation:06}' if type(reservation) is int and reservation > 0 else uuid.uuid4().hex
    assert side in {'backend', 'front'} and outcome in {'response', 'failed', 'refused'}
    value = dict(trace, side=side, outcome=outcome)
    if backend_trace is not None:
        value['backend_trace'] = backend_trace
    if fact is not None:
        value['broker_error'] = fact
    persist(directory, f'delivery-{identity}.{side}.json', json.dumps(value, sort_keys=True).encode())
