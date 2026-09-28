"""Two canonical task relays restricted to a loopback tonic fixture."""
import argparse
import json
from pathlib import Path
import socket
import sys
import threading
import time

HELPERS = Path(__file__).resolve().parents[1] / 'forward_runner' / 'helpers'
sys.path.insert(0, str(HELPERS))
from session_relay import Relay


def save(path, value):
    path.write_text(json.dumps(value))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--target-port', type=int, required=True)
    parser.add_argument('--seconds', type=int, default=60)
    parser.add_argument('--socket-path', type=Path)
    args = parser.parse_args()
    args.directory.mkdir(exist_ok=True)
    sockpath = args.socket_path or args.directory / 'relay.sock'
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    port = listener.getsockname()[1]
    listener.close()
    if not 1 <= args.seconds <= 600:
        raise ValueError('local_fixture_deadline_outside_bounds')
    until = time.time() + args.seconds
    dirs = {}
    for role, role_port in [('front', port), ('backend', args.target_port)]:
        directory = args.directory / role
        directory.mkdir(exist_ok=True)
        save(directory / 'settings.json', dict(run_id='local-only', generation='local-fixture', host='127.0.0.1', port=role_port))
        save(directory / 'LEASE.json', dict(generation='local-fixture', expires_unix=until,
             deadline_unix=until, granted_bytes=16 * 2**30))
        dirs[role] = directory
    workers = []
    for role in ['backend', 'front']:
        worker = threading.Thread(target=Relay(role, dirs[role], sockpath).run, daemon=True)
        worker.start()
        workers.append(worker)
    while time.time() < until:
        if all((dirs[role] / (role + '-status.json')).is_file() for role in dirs):
            states = [json.loads((dirs[role] / (role + '-status.json')).read_text()) for role in dirs]
            if all(v['phase'] == 'ready' for v in states):
                save(args.directory / 'ready.json', dict(port=port, external_endpoints=False))
                break
        time.sleep(.01)
    while time.time() < until and not (args.directory / 'DONE').exists():
        time.sleep(.01)
    for directory in dirs.values():
        (directory / 'STOP').touch()
    for worker in workers:
        worker.join(2)
    if any(worker.is_alive() for worker in workers):
        raise RuntimeError('local_relay_did_not_stop')


if __name__ == '__main__':
    main()
