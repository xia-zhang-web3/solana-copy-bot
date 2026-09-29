"""Actual cached backend command/image/mounts against loopback TLS, network none."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import time

from http_recovery_tls_server import CODE, MODES, SECRET_MARKER, certificates, policy, save

IMAGE = 'sha256:09ecaa87c6799c8d8ee0dfb779905d97e5f667ab97d866cc47ff9f949ffb7b3b'
CA_PATH = '/etc/ssl/certs/ca-certificates.crt'
BACKEND_COMMAND = ['-B', '/code/http_broker/broker.py', 'backend', '/control/http-policy.json']


def docker(*args, check=True):
    result = subprocess.run(['docker', *args], capture_output=True, text=True, timeout=30)
    if check and result.returncode:
        raise RuntimeError('offline_docker_' + args[0] + ':' + result.stderr)
    return result.stdout.strip()


def metadata(cid):
    return json.loads(docker('inspect', cid))[0]


def ready(cid, path):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        value = metadata(cid)
        if not value['State']['Running']:
            raise RuntimeError('offline_fixture_exited:' + docker('logs', cid, check=False))
        result = subprocess.run(['docker', 'exec', cid, 'test', '-e', path], capture_output=True, timeout=5)
        if result.returncode == 0:
            return
        time.sleep(.05)
    raise TimeoutError('offline_fixture_not_ready')


def launch(name, network, mounts, command, environment=()):
    args = ['run', '-d', '--name', name, '--pull', 'never', '--platform', 'linux/amd64',
            '--network', network, '--memory', '256m', '--cpus', '.25', '--pids-limit', '64',
            '--read-only', '--restart', 'no', '--cap-drop', 'ALL',
            '--security-opt', 'no-new-privileges', '--user', '501:20']
    for mount in mounts:
        args += ['--mount', mount]
    for value in environment:
        args += ['--env', value]
    return docker(*args, '--entrypoint', '/usr/bin/python3', IMAGE, *command)


def bind(source, target, readonly=True):
    return f'type=bind,src={source},dst={target}' + (',readonly' if readonly else '')


def fixture(public_ca):
    image = json.loads(docker('image', 'inspect', IMAGE))[0]
    assert (image['Id'], image['Os'], image['Architecture']) == (IMAGE, 'linux', 'amd64')
    results = {}
    containers = []
    with tempfile.TemporaryDirectory(prefix='copybot-offline-https-') as name:
        root = Path(name)
        assert root.stat().st_uid == 501, 'offline fixture uid must match production role'
        certificates(root)
        for name in ['trusted', 'untrusted']:
            path = root / (name + '.pem')
            path.write_bytes(public_ca.read_bytes() + b'\n' + path.read_bytes())
        volume = root.name + '-uds'
        docker('volume', 'create', volume)
        docker('run', '--rm', '--pull', 'never', '--platform', 'linux/amd64', '--network', 'none',
               '--memory', '64m', '--cpus', '1', '--read-only', '--cap-drop', 'ALL',
               '--cap-add', 'CHOWN', '--cap-add', 'FOWNER',
               '--mount', f'type=volume,src={volume},dst=/relay', IMAGE, 'python3', '-B', '-c',
               'import os;os.chown("/relay",501,20);os.chmod("/relay",0o700)')
        source = [bind(CODE, '/source'), bind(CODE / 'tools/http_recovery_broker', '/code/http_broker')]
        relay = f'type=volume,src={volume},dst=/relay'
        try:
            fake = launch(root.name + '-https', 'none', source + [bind(root, '/fixture', False)],
                          ['-B', '/source/tools/tests/http_recovery_tls_server.py',
                           '--role', 'upstream', '--directory', '/fixture'])
            containers.append(fake)
            ready(fake, '/fixture/upstream-ready.json')
            for mode in MODES:
                control = root / mode
                policy(control, mode, 18766)
                ca = root / ('untrusted.pem' if mode == 'untrusted' else 'trusted.pem')
                mounts = source + [relay, bind(control, '/control', False), bind(ca, CA_PATH)]
                backend = launch(root.name + '-' + mode + '-backend', 'container:' + fake,
                                 mounts, BACKEND_COMMAND,
                                 ['SSL_CERT_FILE=' + CA_PATH, 'BROKER_OFFLINE_TEST=1'])
                containers.append(backend)
                ready(backend, '/relay/http.sock')
                front = launch(root.name + '-' + mode + '-front', 'none', mounts,
                               ['-B', '/code/http_broker/broker.py', 'front'])
                containers.append(front)
                # Binding occurs after startup; retry only connection-refused during local readiness.
                deadline = time.monotonic() + 10
                while True:
                    client = subprocess.run(['docker', 'exec', front, '/usr/bin/python3', '-B',
                        '/source/tools/tests/http_recovery_tls_server.py', '--role', 'client'],
                        capture_output=True, text=True, timeout=15)
                    if client.returncode == 0:
                        result = json.loads(client.stdout)
                        break
                    if 'ConnectionRefusedError' not in client.stderr or time.monotonic() > deadline:
                        raise RuntimeError('offline_client_failure:' + client.stderr)
                    time.sleep(.05)
                cfg = metadata(backend)['Config']
                assert cfg['Entrypoint'] == ['/usr/bin/python3'] and cfg['Cmd'] == BACKEND_COMMAND
                context = json.loads(docker('exec', backend, '/usr/bin/python3', '-B', '-c',
                    'import sys,json;sys.path.insert(0,"/code/http_broker");from broker import verified_context;'
                    'c=verified_context();print(json.dumps({"verify_mode":c.verify_mode.name,'
                    '"check_hostname":c.check_hostname,"ca_certificates":len(c.get_ca_certs())}))'))
                assert context['verify_mode'] == 'CERT_REQUIRED' and context['check_hostname']
                with sqlite3.connect(control / 'broker-ledger.sqlite3') as db:
                    assert db.execute('SELECT rpc_cu,attempts FROM head').fetchone() == (10, 1)
                if mode in ['trusted', 'wrong-id']:
                    assert result == {'status': 200, 'body': {'jsonrpc': '2.0',
                        'id': 8 if mode == 'wrong-id' else 7, 'result': [123]}}
                    assert (control / 'http-evidence/response-000001.json').is_file()
                else:
                    assert result['status'] == 502
                    fact = result['body']['broker_error']
                    assert (fact['method'], fact['reservation_id'], fact['kind']) == ('getBlocks', 1, 'failed')
                    expected = {'untrusted': 'tls_certificate', 'hostname': 'tls_hostname', 'status': 'http_status'}[mode]
                    assert fact['reason'] == expected, fact
                    assert fact['http_status'] == (503 if mode == 'status' else None), fact
                    archive = json.loads((control / 'http-evidence/failure-000001.json').read_text())
                    assert all(archive[k] == v for k, v in fact.items())
                    if mode == 'status':
                        assert archive['response_sha256'] == hashlib.sha256(SECRET_MARKER.encode()).hexdigest()
                        assert archive['response_bytes'] == len(SECRET_MARKER)
                assert all(SECRET_MARKER.encode() not in p.read_bytes()
                           for p in (control / 'http-evidence').iterdir()), 'credential marker leaked'
                results[mode] = dict(result, effective_context=context, mock_rpc_cu=10,
                                     reservation_preserved=True, command=cfg['Cmd'], image=cfg['Image'])
                for cid in [front, backend]:
                    docker('stop', '--time', '1', cid)
                    state = metadata(cid)['State']
                    assert not state['Running'] and not state['OOMKilled']
                    docker('rm', cid)
                    containers.remove(cid)
            return dict(status='PASS', cases=results, backend_image=IMAGE,
                        backend_ssl_cert_file=CA_PATH, public_ca_sha256=hashlib.sha256(public_ca.read_bytes()).hexdigest(),
                        external_networks=False, provider_calls=0, signatures=0, submissions=0)
        finally:
            for cid in reversed(containers):
                docker('rm', '-f', cid, check=False)
            docker('volume', 'rm', volume, check=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--public-ca', type=Path, required=True)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = fixture(args.public_ca)
    if args.output:
        save(args.output, result)
    print(json.dumps(result, sort_keys=True))
