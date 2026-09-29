"""Immutable bounded HTTP evidence before client delivery, with no credentials."""
import hashlib
import json
import os
from pathlib import Path
import uuid


def persist(directory, name, data):
    directory = Path(directory) / 'http-evidence'
    directory.mkdir(mode=0o700, exist_ok=True)
    fd = os.open(directory / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
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


def archive(directory, reservation, method, request, status, response, original=None):
    value = {'method': method, 'request': json.loads(request), 'request_sha256': hashlib.sha256(request).hexdigest(),
             'status': status, 'bytes': len(response),
             'response_sha256': hashlib.sha256(response).hexdigest()}
    if original is not None:
        value.update(original_response_bytes=len(original),
                     original_response_sha256=hashlib.sha256(original).hexdigest(),
                     rpc_error_message_redacted=True)
    for suffix, data in [('.json', response), ('.meta.json', json.dumps(value).encode())]:
        persist(directory, f'response-{reservation:06}{suffix}', data)
