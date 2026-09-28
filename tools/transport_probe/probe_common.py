"""Private bounded read-only probe bindings; imports do not start anything."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
RUN = 'copybot-run15-transport-probe-01'
PY_IMAGE = 'sha256:09ecaa87c6799c8d8ee0dfb779905d97e5f667ab97d866cc47ff9f949ffb7b3b'
APP_IMAGE = 'docker.io/library/ubuntu@sha256:224a1869083a311ef3f13648a154ba79832fbef6364d31493642ca03082da254'
HOST = 'solana-mainnet.streaming.alchemy.com'
CAP = 4 * 1024**3
SECONDS = 480
LABEL = 'copybot.transport-probe'


def read(path, default=None):
    try:
        return json.loads(Path(path).read_text())
    except FileNotFoundError:
        return default


def save(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    temporary.chmod(0o600)
    temporary.replace(path)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def docker(args, timeout=30, include_stderr=False):
    attempts = 3 if args[0] in ('inspect', 'logs', 'image') else 1
    for attempt in range(attempts):
        try:
            process = subprocess.run(['docker'] + args, capture_output=True, text=True, timeout=timeout)
            if process.returncode == 0:
                return (process.stdout + (process.stderr if include_stderr else '')).strip()
            reason = 'exit_' + str(process.returncode)
        except subprocess.TimeoutExpired:
            reason = 'timeout'
        if attempt + 1 < attempts:
            time.sleep(.1 * (attempt + 1))
    raise ValueError('probe_docker_' + args[0] + '_' + reason)


def verify_container(cid):
    value = json.loads(docker(['inspect', cid]))[0]
    if value['Id'] != cid or value['Config']['Labels'].get(LABEL) != RUN:
        raise ValueError('probe_container_ownership')
    return value


def unconsumed():
    for name in ['ATTEMPT.json', 'PROBE_CLOCK.json', 'LEASE.json', 'RESULT.json']:
        if (ROOT / 'control' / name).exists():
            raise ValueError('probe_consumed:' + name)
    if not (ROOT / 'control/STOP').is_file() or not (ROOT / 'control/STREAM_STOP').is_file():
        raise ValueError('probe_stops_missing')
