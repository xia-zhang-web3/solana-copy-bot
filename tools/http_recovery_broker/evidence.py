"""Immutable bounded HTTP evidence before client delivery, with no credentials."""
import hashlib
import json
import os
from pathlib import Path


def archive(directory, reservation, method, request, status, response):
    directory = Path(directory) / 'http-evidence'
    directory.mkdir(mode=0o700, exist_ok=True)
    value = {'method': method, 'request': json.loads(request), 'request_sha256': hashlib.sha256(request).hexdigest(),
             'status': status, 'bytes': len(response),
             'response_sha256': hashlib.sha256(response).hexdigest()}
    for suffix, data in [('.json', response), ('.meta.json', json.dumps(value).encode())]:
        path = directory / f'response-{reservation:06}{suffix}'
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
