"""Three task-owned Docker roles, local HTTPS/UDS and no provider material."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import threading
import time

from http_recovery_tls_server import certificates
from http_size_metrics import save

CODE = Path(__file__).resolve().parents[2]
IMAGE = 'sha256:09ecaa87c6799c8d8ee0dfb779905d97e5f667ab97d866cc47ff9f949ffb7b3b'


def docker(*args, check=True):
    p = subprocess.run(['docker', *args], capture_output=True, text=True, timeout=25)
    if check and p.returncode:
        raise RuntimeError('docker_'+args[0]+'_failed:'+p.stderr[-1000:])
    return p.stdout


def run(args):
    root = args.root.resolve()
    control = root / 'control'
    control.mkdir()
    certificates(control)
    spec = importlib.util.spec_from_file_location('size_owned_controller', args.helper)
    common = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(common)
    common.ROOT = root
    now = time.time()
    clock = dict(run_id='offline-size', first_attempt_unix=now,
                 duration_seconds=480, deadline_unix=now+480)
    common.save(control / 'PROBE_CLOCK.json', clock)
    common.renew(clock)
    (control / 'STOP').touch()
    save(control / 'policy.json', dict(run_id='offline-size', profile='read_only_http_recovery_v1',
        offline_stub=True, rpc_upstream='https://localhost:18766/rpc',
        max_rpc_attempts=1024, max_rpc_cu=40960, prior_http_nano_usd=10977750,
        prior_model_nano_usd=8110692868, stream_reserved_nano_usd=400000000))
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    prefix = 'copybot-size-' + str(os.getpid())
    volume = prefix + '-uds'
    docker('volume', 'create', volume)
    docker('run', '--rm', '--pull=never', '--platform=linux/amd64', '--network=none',
           '--memory=64m', '--cpus=1', '--read-only', '--cap-drop=ALL', '--cap-add=CHOWN',
           '--cap-add=FOWNER', '--mount', f'type=volume,source={volume},target=/relay', IMAGE,
           'python3', '-c', 'import os;os.chown("/relay",501,20);os.chmod("/relay",0o700)')
    containers, renewals, rust_samples = {}, [], []
    stop = threading.Event()
    for sig in [signal.SIGTERM, signal.SIGINT]:
        signal.signal(sig, lambda *_: stop.set())

    def launch(role, network, memory):
        cmd = ['create', '--pull=never', '--platform=linux/amd64', '--name', prefix+'-'+role,
          '--label', 'copybot.size-fixture='+prefix, '--user=501:20', '--read-only',
          '--cap-drop=ALL', '--security-opt=no-new-privileges', '--restart=no',
          '--memory='+memory, '--memory-swap='+memory, '--cpus=1', '--pids-limit=128', '--network='+network,
          '--tmpfs', '/tmp:rw,nosuid,nodev,size=64m',
          '--mount', f'type=bind,source={CODE / "tools"},target={CODE / "tools"},readonly',
          '--mount', f'type=bind,source={args.corpus.resolve()},target={args.corpus.resolve()},readonly',
          '--mount', f'type=bind,source={control},target={control}',
          '--mount', f'type=volume,source={volume},target=/relay']
        if role == 'front':
            cmd += ['--publish', f'127.0.0.1:{port}:{port}']
        if role in {'front', 'backend'}:
            cmd += ['--env', 'MALLOC_ARENA_MAX=2']
        cmd += [IMAGE, 'python3', '-B', str(CODE / 'tools/tests/http_size_docker_server.py'),
                '--role', role, '--directory', str(control), '--corpus', str(args.corpus.resolve()),
                '--port', str(port)]
        cid = docker(*cmd).strip()
        containers[role] = cid
        docker('start', cid)
        limit = time.monotonic()+20
        while not (control / (role+'-ready.json')).exists():
            state = json.loads(docker('inspect', cid))[0]['State']
            if not state['Running']:
                raise RuntimeError(role+'_startup_exited:'+docker('logs', cid))
            if time.monotonic() >= limit:
                raise TimeoutError(role+'_ready_deadline')
            time.sleep(.05)
        return cid

    clock_bytes = (control / 'PROBE_CLOCK.json').read_bytes()
    try:
        up = launch('upstream', 'none', '768m')
        launch('backend', 'container:'+up, args.backend_memory)
        launch('front', 'bridge', args.front_memory)
        save(root / 'DOCKER_BINDING.json', {role: json.loads(docker('inspect', cid))[0]
                                          for role, cid in containers.items()})
        save(root / 'HOST_READY.json', dict(front_url=f'http://127.0.0.1:{port}/rpc',
              clock=clock, helper_sha256=hashlib.sha256(args.helper.read_bytes()).hexdigest(),
              clock_sha256=hashlib.sha256(clock_bytes).hexdigest(), provider_calls=0))
        deadline = time.monotonic()+145
        while not stop.wait(.10) and not (root / 'STOP_HOST_TEST').exists():
            if time.monotonic() >= deadline:
                raise TimeoutError('local_size_controller_deadline')
            if not renewals or time.monotonic()-renewals[-1]['monotonic'] >= 1:
                common.renew(clock)
                renewals.append(dict(monotonic=time.monotonic(), at_unix=time.time()))
            rss = subprocess.run(['ps', '-o', 'rss=', '-p', str(args.rust_pid)],
                                 capture_output=True, text=True, timeout=2).stdout.strip()
            if rss:
                rust_samples.append(dict(at_unix=time.time(), rss_bytes=int(rss)*1024))
    finally:
        (control / 'STOP_TEST').touch()
        stopped = {}
        for role, cid in reversed(list(containers.items())):
            docker('stop', '--time=5', cid, check=False)
            stopped[role] = json.loads(docker('inspect', cid))[0]
            logs = subprocess.run(['docker', 'logs', cid], capture_output=True, text=True, timeout=10)
            (root / (role+'.stdout.log')).write_text(logs.stdout)
            (root / (role+'.stderr.log')).write_text(logs.stderr)
            docker('rm', cid, check=False)
        docker('volume', 'rm', volume, check=False)
        save(root / 'STOPPED_DOCKER.json', stopped)
        save(root / 'HOST_METRICS.json', dict(renewals=renewals, rust_rss_samples=rust_samples,
             original_clock_unchanged=(control / 'PROBE_CLOCK.json').read_bytes() == clock_bytes,
             provider_calls=0, signatures=0, submissions=0))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--corpus', type=Path, required=True)
    parser.add_argument('--helper', type=Path, required=True)
    parser.add_argument('--rust-pid', type=int, required=True)
    parser.add_argument('--front-memory', default='1g')
    parser.add_argument('--backend-memory', default='1g')
    run(parser.parse_args())
