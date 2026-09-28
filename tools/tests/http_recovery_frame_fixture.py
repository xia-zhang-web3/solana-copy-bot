"""Offline 8 MiB/4-client broker boundary under the installed role memory caps.

The four cached-image containers have no external network or provider mounts.
Backend and front keep their own 256 MiB production budgets; the fake HTTP
server and clients are separate, so fixture memory is not counted as broker RSS.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time

CODE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CODE / 'tools/http_recovery_broker'))
from broker import BackendServer, FrontServer

IMAGE = 'sha256:09ecaa87c6799c8d8ee0dfb779905d97e5f667ab97d866cc47ff9f949ffb7b3b'
BODY_BYTES = 8 * 1024**2
CLIENTS = 4
CONF = {'commitment': 'confirmed', 'encoding': 'json', 'transactionDetails': 'full',
        'maxSupportedTransactionVersion': 1, 'rewards': True}
PREFIX = b'{"jsonrpc":"2.0","id":7,"result":{"transactions":[],"padding":"'
SUFFIX = b'"}}'


def payload():
    return PREFIX + b'x' * (BODY_BYTES - len(PREFIX) - len(SUFFIX)) + SUFFIX


def save(path, value):
    Path(path).write_text(json.dumps(value, sort_keys=True))


def resident():
    """Linux cgroup v2 peak is broker-role whole-container memory, not only RSS."""
    value = Path('/sys/fs/cgroup/memory.peak')
    return int(value.read_text()) if value.exists() else None


def fake_http(directory):
    data = payload()
    barrier = threading.Barrier(CLIENTS, timeout=15)
    lock = threading.Lock()
    state = {'calls': 0, 'active': 0, 'max_active': 0}

    class Fake(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            if request.get('method') != 'getBlock' or request.get('params', [0, None])[1] != CONF:
                self.send_error(403)
                return
            with lock:
                state['calls'] += 1
                state['active'] += 1
                state['max_active'] = max(state['max_active'], state['active'])
            try:
                barrier.wait()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                for start in range(0, len(data), 65536):
                    self.wfile.write(memoryview(data)[start:start + 65536])
            finally:
                with lock:
                    state['active'] -= 1
                    save(directory / 'fake-result.json', state)

    server = ThreadingHTTPServer(('127.0.0.1', 18766), Fake)
    save(directory / 'fake-ready.json', {'ready': True})
    server.serve_forever(poll_interval=.05)


def backend(directory):
    os.environ['BROKER_OFFLINE_TEST'] = '1'
    start = time.time()
    (directory / 'STOP').touch()
    save(directory / 'LEASE.json', {'generation': 1, 'expires_unix': start + 480})
    save(directory / 'PROBE_CLOCK.json', {'run_id': 'offline-frame-boundary',
         'first_attempt_unix': start, 'duration_seconds': 480, 'deadline_unix': start + 480})
    policy = {'run_id': 'offline-frame-boundary', 'profile': 'read_only_http_recovery_v1',
              'offline_stub': True, 'rpc_upstream': 'http://127.0.0.1:18766/rpc',
              'max_rpc_attempts': CLIENTS, 'max_rpc_cu': CLIENTS * 40,
              'prior_http_nano_usd': 9_985_500, 'prior_model_nano_usd': 7_928_686_412,
              'stream_reserved_nano_usd': 400_000_000}
    server = BackendServer(directory / 'http.sock', directory,
                           directory / 'broker-ledger.sqlite3', policy)
    save(directory / 'backend-ready.json', {'ready': True})
    server.serve_forever(poll_interval=.05)


def front(directory):
    server = FrontServer(('127.0.0.1', 18765), directory / 'http.sock')
    save(directory / 'front-ready.json', {'ready': True})
    server.serve_forever(poll_interval=.05)


def clients(directory):
    barrier = threading.Barrier(CLIENTS)

    def post(number):
        request = json.dumps({'jsonrpc': '2.0', 'id': 7, 'method': 'getBlock',
                              'params': [40 + number, CONF]})
        barrier.wait()
        connection = http.client.HTTPConnection('127.0.0.1', 18765, timeout=30)
        try:
            connection.request('POST', '/rpc', request, {'Content-Type': 'application/json'})
            response = connection.getresponse()
            digest = hashlib.sha256()
            size = 0
            while chunk := response.read(65536):
                digest.update(chunk)
                size += len(chunk)
            return {'status': response.status, 'bytes': size, 'sha256': digest.hexdigest()}
        finally:
            connection.close()

    with ThreadPoolExecutor(max_workers=CLIENTS) as executor:
        outcomes = list(executor.map(post, range(CLIENTS)))
    expected_hash = hashlib.sha256(payload()).hexdigest()
    assert all(o == {'status': 200, 'bytes': BODY_BYTES, 'sha256': expected_hash}
               for o in outcomes), outcomes
    assert json.loads((directory / 'fake-result.json').read_text())['max_active'] == CLIENTS
    for number in range(1, CLIENTS + 1):
        evidence = directory / 'http-evidence' / f'response-{number:06}.json'
        with evidence.open('rb') as stream:
            assert hashlib.file_digest(stream, 'sha256').hexdigest() == expected_hash
        metadata = json.loads(evidence.with_suffix('.meta.json').read_text())
        assert (metadata['bytes'], metadata['response_sha256']) == (BODY_BYTES, expected_hash)
    with sqlite3.connect(directory / 'broker-ledger.sqlite3') as db:
        assert db.execute('SELECT rpc_cu,attempts FROM head').fetchone() == (160, 4)
    save(directory / 'clients-result.json', {'status': 'PASS', 'response_bytes': BODY_BYTES,
         'parallel_clients': CLIENTS, 'responses': len(outcomes), 'archived_responses': CLIENTS,
         'mock_rpc_cu': 160, 'provider_calls': 0, 'signatures': 0, 'submissions': 0,
         'client_container_peak_bytes': resident()})


def docker_fixture():
    def docker(*arguments, check=True):
        result = subprocess.run(['docker', *arguments], check=check, capture_output=True,
                                text=True, timeout=40)
        return (result.stdout + (result.stderr if arguments[0] == 'logs' else '')).strip()

    identity = json.loads(docker('image', 'inspect', IMAGE))[0]
    assert (identity['Id'], identity['Os'], identity['Architecture']) == (IMAGE, 'linux', 'amd64')
    containers = {}
    with tempfile.TemporaryDirectory(prefix='copybot-offline-http-frame-') as temporary:
        directory = Path(temporary)
        assert directory.stat().st_uid == 501, 'fixture mount must belong to the installed role uid'
        directory.chmod(0o700)
        fixture_deadline = time.monotonic() + 60
        volume = directory.name + '-data'
        docker('volume', 'create', volume)
        docker('run', '--rm', '--platform', 'linux/amd64', '--network', 'none',
               '--memory', '64m', '--cpus', '1', '--read-only', '--cap-drop', 'ALL',
               '--cap-add', 'CHOWN', '--cap-add', 'FOWNER',
               '--mount', f'type=volume,src={volume},dst=/fixture', IMAGE,
               'python3', '-c', 'import os;os.chown("/fixture",501,20);os.chmod("/fixture",0o700)')

        def launch(role, memory, network):
            name = directory.name + '-' + role
            cid = docker('run', '-d', '--name', name, '--platform', 'linux/amd64',
                         '--network', network, '--memory', memory, '--cpus', '1',
                         '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
                         '--pids-limit', '64', '--user', '501:20',
                         '--mount', f'type=bind,src={CODE},dst=/code,readonly',
                         '--mount', f'type=volume,src={volume},dst=/fixture', IMAGE,
                         'python3', '-B', '/code/tools/tests/http_recovery_frame_fixture.py',
                         '--role', role, '--directory', '/fixture')
            containers[role] = cid
            return cid

        def ready(role):
            deadline = min(time.monotonic() + 15, fixture_deadline)
            while True:
                state = json.loads(docker('inspect', containers[role]))[0]['State']
                if not state['Running']:
                    raise RuntimeError(f'{role}_exited: {state}; {docker("logs", containers[role])}')
                exists = subprocess.run(['docker', 'exec', containers[role], 'test', '-f',
                                         f'/fixture/{role}-ready.json'], capture_output=True,
                                        timeout=5).returncode == 0
                if exists:
                    return
                if time.monotonic() > deadline:
                    raise TimeoutError(f'{role}_not_ready')
                time.sleep(.05)

        try:
            fake = launch('fake', '64m', 'none'); ready('fake')
            launch('backend', '256m', 'container:' + fake); ready('backend')
            front_id = launch('front', '256m', 'none'); ready('front')
            launch('clients', '128m', 'container:' + front_id)
            deadline = min(time.monotonic() + 40, fixture_deadline)
            while True:
                state = json.loads(docker('inspect', containers['clients']))[0]['State']
                if not state['Running']:
                    if state['ExitCode'] != 0:
                        raise RuntimeError(f'clients_exited: {state}; {docker("logs", containers["clients"])}')
                    break
                if time.monotonic() > deadline:
                    raise TimeoutError('clients_not_complete')
                time.sleep(.05)
            result = json.loads(docker('exec', containers['backend'], 'cat', '/fixture/clients-result.json'))
            result['roles'] = {}
            for role in ('backend', 'front'):
                state = json.loads(docker('inspect', containers[role]))[0]
                assert state['State']['Running'] and not state['State']['OOMKilled']
                peak = int(docker('exec', containers[role], 'cat', '/sys/fs/cgroup/memory.peak'))
                assert peak <= 256 * 1024**2
                result['roles'][role] = {'memory_cap_bytes': 256 * 1024**2,
                                         'peak_bytes': peak, 'oom_killed': False}
            print(json.dumps(result, sort_keys=True))
        finally:
            for cid in reversed(list(containers.values())):
                docker('rm', '-f', cid, check=False)
            docker('volume', 'rm', volume, check=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--role', choices=['fake', 'backend', 'front', 'clients'])
    parser.add_argument('--directory', type=Path)
    parser.add_argument('--docker', action='store_true')
    args = parser.parse_args()
    if args.docker:
        docker_fixture()
    else:
        {'fake': fake_http, 'backend': backend, 'front': front, 'clients': clients}[args.role](args.directory)
