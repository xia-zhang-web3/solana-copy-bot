"""Exact bound Linux app: local-only producer and broker, no signer."""
import argparse
import hashlib
import json
from pathlib import Path
import socket
import subprocess
import time

from http_size_docker_fixture import docker, IMAGE as PYTHON_IMAGE
from http_size_metrics import save
from http_catchup_app_config import prepare

APP_IMAGE = 'docker.io/library/ubuntu@sha256:224a1869083a311ef3f13648a154ba79832fbef6364d31493642ca03082da254'
EXPECTED_BINARY = 'b216993597f83ae53d1d66f4946885ffeddf594a88410d86ec38ec61088be53d'

def sha256_hex(value):
    if len(value) != 64 or any(c not in '0123456789abcdef' for c in value):
        raise argparse.ArgumentTypeError('sha256_hex_required')
    return value


def main(args):
    root = args.root.resolve()
    bindings = json.loads((root / 'DOCKER_BINDING.json').read_text())
    namespace = 'container:'+bindings['front']['Id']
    port = args.grpc_port
    # Validated loopback native tonic reachability only, before daemon execution.
    code = 'import socket;s=socket.create_connection(("host.docker.internal",'+str(port)+'),2);s.close()'
    docker('run', '--rm', '--pull=never', '--platform=linux/amd64', '--network='+namespace,
           '--memory=64m', '--memory-swap=64m', '--cpus=1', '--read-only', '--cap-drop=ALL',
           '--security-opt=no-new-privileges', PYTHON_IMAGE, 'python3', '-c', code)
    save(root / (args.phase+'-TRANSPORT_READY.json'), dict(host_loopback_tonic_port=port,
         container_network=namespace, external_connections=0, provider_calls=0))
    config_path = root / (args.phase+'-config.toml')
    config = prepare(args.config, config_path, f'http://host.docker.internal:{port}',
                     json.loads((root/'HOST_READY.json').read_text())['front_url'], args.wallet,
                     args.blocks_bytes)
    save(root / (args.phase+'-CONFIG_READY.json'), dict(path=str(config_path),
         blocks_bytes=args.blocks_bytes, blocks_count=config['ingestion']['yellowstone_association']['blocks']['count'],
         block_ttl_ms=config['ingestion']['yellowstone_association']['block_ttl_ms']))
    until = time.monotonic()+20
    while not (root / (args.phase+'-CONFIG_VALID')).exists():
        if (root / (args.phase+'-APP_STOP')).exists() or time.monotonic() >= until:
            raise RuntimeError('offline_typed_config_not_validated')
        time.sleep(.05)
    # Same namespace: broker URL published host port is the identical internal port.
    assert config['ingestion']['yellowstone_http_recovery']['broker_url'].startswith('http://127.0.0.1:')
    binary = args.install / 'bin/copybot-app'
    assert hashlib.sha256(binary.read_bytes()).hexdigest() == args.expected_binary_sha256
    manifest_path = args.install / 'bin/operator-artifact-current-copybot-app.json'
    if manifest_path.exists():
        manifest_bytes = manifest_path.read_bytes()
        manifest = json.loads(manifest_bytes)
        assert manifest['package'] == 'copybot-app' and manifest['profile'] == 'release'
        assert manifest['git_dirty'] is False
        assert any(item['name'] == 'copybot-app' and item['sha256'] == args.expected_binary_sha256
                   for item in manifest['binaries'])
        manifest_binding = dict(git_sha=manifest['git_sha'],
                                manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
                                manifest_path=str(manifest_path), profile='release')
    else:
        assert args.allow_unmanifested_dev, 'matching manifest required'
        manifest_binding = dict(profile='unmanifested_local_dev_only', git_sha=None,
                                manifest_sha256=None, manifest_path=None)
    prefix = 'copybot-catchup-'+str(__import__('os').getpid())+'-'+args.phase
    cmd = ['create', '--name', prefix, '--platform=linux/amd64', '--pull=never',
        '--network='+namespace, '--read-only', '--cap-drop=ALL', '--security-opt=no-new-privileges',
        '--user=501:20', '--cpus=2', '--memory=3g', '--memory-swap=3g', '--pids-limit=128',
        '--restart=no', '--label', 'copybot.offline-catchup='+prefix,
        '--tmpfs', '/tmp:rw,noexec,nosuid,size=64m',
        '--mount', f'type=bind,source={args.install.resolve()},target=/opt/copybot,readonly',
        '--mount', f'type=bind,source={root / "app-state"},target=/opt/copybot/state',
        '--mount', f'type=bind,source={config_path},target=/run/offline.toml,readonly',
        '--mount', f'type=bind,source={root / "control"},target=/control,readonly',
        '--workdir', '/opt/copybot', '--entrypoint', '/usr/bin/env', APP_IMAGE, '-i',
        *(['MALLOC_ARENA_MAX=2'] if args.app_arenas == '2' else []),
        '/opt/copybot/bin/copybot-app', '--config', '/run/offline.toml']
    cid = docker(*cmd).strip()
    save(root / (args.phase+'-APP_BINDING.json'), json.loads(docker('inspect', cid))[0])
    samples, failure = [], None
    try:
        docker('start', cid)
        save(root / (args.phase+'-APP_READY.json'), dict(container_id=cid, binary_sha256=args.expected_binary_sha256,
             clock_sha256=hashlib.sha256((root/'control/PROBE_CLOCK.json').read_bytes()).hexdigest(),
             provider_calls=0, financial_flags=False, credential_mounts=False,
             app_arenas=args.app_arenas, blocks_bytes=args.blocks_bytes, **manifest_binding))
        limit = time.monotonic()+310
        while not (root / (args.phase+'-APP_STOP')).exists():
            state = json.loads(docker('inspect', cid))[0]['State']
            if not state['Running']:
                raise RuntimeError('offline_app_exited_'+str(state['ExitCode']))
            if time.monotonic() >= limit:
                raise TimeoutError('offline_app310s_bound')
            readings = docker('exec', cid, '/bin/cat', '/sys/fs/cgroup/memory.current',
                        '/sys/fs/cgroup/memory.peak', '/proc/1/status', '/proc/1/smaps_rollup', '/proc/meminfo').splitlines()
            sample = dict(at_unix=time.time(), memory_current=int(readings[0]), memory_peak=int(readings[1]))
            for line in readings[2:]:
                if line.startswith(('VmRSS:', 'VmHWM:')):
                    key, number, _ = line.split();sample[key[:-1]+'_bytes'] = int(number)*1024
                elif line.startswith(('Rss:', 'Pss:', 'MemAvailable:', 'MemTotal:')):
                    key, number, _ = line.split();sample[key[:-1]+'_bytes'] = int(number)*1024
            samples.append(sample)
            p = subprocess.run(['docker', 'logs', cid], capture_output=True, text=True, timeout=10)
            (root / (args.phase+'-APP.log')).write_text(p.stdout+p.stderr)
            save(root / (args.phase+'-APP_METRICS.json'), dict(samples=samples, provider_calls=0))
            time.sleep(.5)
    except Exception as error:
        failure = dict(cause_type=type(error).__name__, reason=str(error))
        raise
    finally:
        docker('stop', '--time=5', cid, check=False)
        p = subprocess.run(['docker', 'logs', cid], capture_output=True, text=True, timeout=10)
        (root / (args.phase+'-APP.log')).write_text(p.stdout+p.stderr)
        save(root / (args.phase+'-APP_STOPPED.json'), json.loads(docker('inspect', cid))[0])
        save(root / (args.phase+'-APP_METRICS.json'), dict(samples=samples, failure=failure, provider_calls=0))
        docker('rm', cid, check=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--install', type=Path, required=True)
    parser.add_argument('--expected-binary-sha256', default=EXPECTED_BINARY, type=sha256_hex)
    parser.add_argument('--allow-unmanifested-dev', action='store_true')
    parser.add_argument('--grpc-port', type=int, required=True)
    parser.add_argument('--wallet', required=True)
    parser.add_argument('--phase', choices=['main', 'restart'], required=True)
    parser.add_argument('--app-arenas', choices=['default', '2'], default='default')
    parser.add_argument('--blocks-bytes', type=int, choices=[805306368, 1744830464],
                        default=805306368)
    main(parser.parse_args())
