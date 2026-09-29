"""Host controller renews LEASE through actual Mac Docker bind while RPC runs.

Only disposable local CA, saved corpus and this task's control directory are
mounted. The private used probe is read once for its exact save/renew helpers.
"""
import argparse
import hashlib
import importlib.util
import inspect
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import threading
import time

from http_recovery_tls_server import certificates

IMAGE = 'sha256:09ecaa87c6799c8d8ee0dfb779905d97e5f667ab97d866cc47ff9f949ffb7b3b'
CODE = Path(__file__).resolve().parents[2]
LABEL = 'copybot.lease-docker-fixture'


def docker(args, timeout=20):
    result = subprocess.run(['docker', *args], capture_output=True, text=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError('docker_' + args[0] + '_failed:' + result.stderr[-1000:])
    return result.stdout


def controller(helper, root):
    spec = importlib.util.spec_from_file_location('owned_probe_control', helper)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.ROOT = root
    return module


def save_report(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def run(args):
    root, control = args.root.resolve(), args.root.resolve() / 'control'
    control.mkdir(parents=True, exist_ok=True)
    helper = args.helper.resolve()
    if not helper.name == 'probe_live_common.py' or root in helper.parents:
        raise ValueError('exact_controller_helper_required')
    common = controller(helper, root)
    certificates(control)
    started = time.time()
    clock = dict(run_id='offline-delivery', first_attempt_unix=started,
                 duration_seconds=480, deadline_unix=started+480)
    common.save(control / 'PROBE_CLOCK.json', clock)
    common.renew(clock)
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        port = listener.getsockname()[1]
    name = 'copybot-lease-fixture-' + str(os.getpid())
    command = ['create', '--pull=never', '--platform=linux/amd64', '--name', name,
        '--label', LABEL + '=' + name, '--user', '501:20', '--read-only', '--cap-drop', 'ALL',
        '--security-opt', 'no-new-privileges', '--restart', 'no', '--memory', '1g', '--cpus', '2',
        '--pids-limit', '128', '--network', 'bridge', '--publish', f'127.0.0.1:{port}:{port}',
        '--tmpfs', '/tmp:rw,nosuid,nodev,size=64m',
        '--mount', f'type=bind,source={CODE / "tools"},target={CODE / "tools"},readonly',
        '--mount', f'type=bind,source={args.corpus.resolve()},target={args.corpus.resolve()},readonly',
        '--mount', f'type=bind,source={control},target={control}', IMAGE,
        'python3', '-B', str(CODE / 'tools/tests/http_lease_docker_server.py'), '--directory',
        str(control), '--corpus', str(args.corpus.resolve()), '--front-port', str(port),
        '--fault-slot', str(args.fault_slot)]
    cid = docker(command).strip()
    stop = threading.Event()
    for number in [signal.SIGINT, signal.SIGTERM]:
        signal.signal(number, lambda *_: stop.set())
    clock_bytes = (control / 'PROBE_CLOCK.json').read_bytes()
    renewals = []
    try:
        docker(['start', cid])
        until = time.monotonic() + 20
        while not (control / 'ready.json').exists():
            if time.monotonic() >= until:
                raise TimeoutError('local_docker_startup_deadline')
            time.sleep(.05)
        assert (control / 'PROBE_CLOCK.json').read_bytes() == clock_bytes
        common.renew(clock)
        save_report(root / 'DOCKER_BINDING.json', json.loads(docker(['inspect', cid]))[0])
        save_report(root / 'CONTROLLER_PROVENANCE.json', {
            'helper_path': str(helper), 'helper_sha256': hashlib.sha256(helper.read_bytes()).hexdigest(),
            'save_sha256': hashlib.sha256(inspect.getsource(common.save).encode()).hexdigest(),
            'renew_sha256': hashlib.sha256(inspect.getsource(common.renew).encode()).hexdigest(),
            'original_clock_sha256': hashlib.sha256(clock_bytes).hexdigest(), 'container_id': cid,
            'image': IMAGE, 'host_uid': os.getuid(), 'container_uid': 501,
            'provider_calls': 0, 'provider_credentials_mounted': False})
        save_report(root / 'HOST_READY.json', {'front_url': f'http://127.0.0.1:{port}/rpc',
                                               'container_id': cid, 'clock': clock})
        until = time.monotonic() + 90
        last_command, paused, clock_mutated = None, False, False
        while not stop.wait(.01) and not (root / 'STOP_HOST_TEST').exists():
            if time.monotonic() >= until:
                raise TimeoutError('local_controller_deadline')
            # Exactly the accepted controller's renew/save function; rename,
            # file fsync and parent fsync occur on the Mac host's bound directory.
            command_path = root / 'TEST_COMMAND.json'
            if command_path.exists():
                command = json.loads(command_path.read_bytes())
                if command['id'] != last_command:
                    last_command = command['id']
                    mode = command['mode']
                    paused = mode != 'valid'
                    (control / 'TEST_MODE').write_text('control_change')
                    for marker in ['HTTP_STOP', 'STREAM_STOP']:
                        (control / marker).unlink(missing_ok=True)
                    if clock_mutated:
                        common.save(control / 'PROBE_CLOCK.json', clock)
                        clock_mutated = False
                    common.renew(clock)
                    if mode == 'expired':
                        common.renew(clock, expired=True)
                    elif mode == 'generation':
                        lease = json.loads((control / 'LEASE.json').read_bytes())
                        common.save(control / 'LEASE.json', dict(lease, generation=2))
                    elif mode == 'stop':
                        (control / 'HTTP_STOP').touch()
                    elif mode == 'clock_binding':
                        common.save(control / 'PROBE_CLOCK.json', dict(clock, run_id='wrong-fixture'))
                        clock_mutated = True
                    elif mode == 'clock_deadline':
                        expired = time.time()-1
                        common.save(control / 'PROBE_CLOCK.json', dict(clock,
                             first_attempt_unix=expired-480, deadline_unix=expired))
                        clock_mutated = True
                    elif mode in {'missing', 'pulse'}:
                        (control / 'LEASE.json').unlink()
                        if mode == 'pulse':
                            time.sleep(.04)
                            common.renew(clock)
                            paused = False
                    elif mode != 'valid':
                        raise ValueError('unknown_local_control_mode')
                    time.sleep(.1)
                    (control / 'TEST_MODE').write_text('restored' if mode == 'valid' else mode)
                    save_report(root / 'TEST_COMMAND_ACK.json', dict(id=last_command, mode=mode))
            if not paused and (not renewals or time.monotonic() - renewals[-1]['monotonic'] >= 1):
                common.renew(clock)
                renewals.append({'at_unix': time.time(), 'monotonic': time.monotonic(),
                                 'lease': json.loads((control / 'LEASE.json').read_bytes())})
        assert (control / 'PROBE_CLOCK.json').read_bytes() == clock_bytes
    finally:
        (control / 'STOP_TEST').touch()
        docker(['stop', '--time', '5', cid])
        # Preserve the original test clock even after a failed negative control;
        # the precise denial is independently durable in broker failure facts.
        if (control / 'PROBE_CLOCK.json').read_bytes() != clock_bytes:
            common.save(control / 'PROBE_CLOCK.json', clock)
        save_report(root / 'STOPPED_DOCKER.json', json.loads(docker(['inspect', cid]))[0])
        save_report(root / 'HOST_RENEWALS.json', {'renewals': renewals,
                    'original_clock_unchanged': clock_bytes is not None and
                        (control / 'PROBE_CLOCK.json').read_bytes() == clock_bytes})
        logs = subprocess.run(['docker', 'logs', cid], capture_output=True, text=True, timeout=10)
        (root / 'docker.stdout.log').write_text(logs.stdout)
        (root / 'docker.stderr.log').write_text(logs.stderr)
        docker(['rm', cid])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--corpus', type=Path, required=True)
    parser.add_argument('--helper', type=Path, required=True)
    parser.add_argument('--fault-slot', type=int, required=True)
    run(parser.parse_args())
