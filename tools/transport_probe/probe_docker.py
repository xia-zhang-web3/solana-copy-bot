"""Only task-labelled containers; stream has no HTTP broker or signer path."""
from probe_common import *


def mount(source, target, writable=False, volume=False):
    return ['--mount', 'type=' + ('volume' if volume else 'bind') + ',src=' + str(source)
            + ',dst=' + target + ('' if writable else ',readonly')]


def base(role):
    memory = '3g' if role == 'observation-app' else '256m'
    return ['--platform', 'linux/amd64', '--pull=never', '--restart', 'no', '--read-only',
        '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges', '--user', '501:20',
        '--cpus', '2' if role == 'observation-app' else '0.25', '--memory', memory,
        '--memory-swap', memory, '--pids-limit', '128' if role == 'observation-app' else '16',
        '--label', LABEL + '=' + RUN, '--label', 'copybot.transport-probe-role=' + role,
        '--log-driver', 'local', '--log-opt', 'max-size=16m', '--log-opt', 'max-file=4']


def commands(front='<EXACT_PROBE_FRONT_CID>'):
    volume = RUN + '-uds'
    values = {}
    for role in ['stream-backend', 'stream-front']:
        args = ['docker', 'create', '--name', RUN + '-' + role] + base(role)
        args += ['--network', 'none' if role == 'stream-front' else 'bridge']
        if role == 'stream-front':
            args += ['--sysctl', 'net.ipv4.ip_unprivileged_port_start=0']
        args += mount(ROOT / 'scripts', '/code') + mount(ROOT / 'control', '/control', True)
        args += mount(volume, '/relay', True, True)
        args += ['--entrypoint', '/usr/bin/python3', PY_IMAGE, '-B', '/code/probe_relay.py',
                 'front' if role == 'stream-front' else 'backend']
        values[role] = args
    args = ['docker', 'create', '--name', RUN + '-observation-app'] + base('observation-app')
    args += ['--network', 'container:' + front, '--tmpfs', '/tmp:rw,noexec,nosuid,size=64m']
    args += mount(ROOT / 'install', '/opt/copybot') + mount(ROOT / 'state', '/opt/copybot/state', True)
    args += mount(ROOT / 'config', '/run/probe-config') + mount(ROOT / 'control', '/control')
    args += mount(ROOT / 'control/hosts', '/etc/hosts') + mount(ROOT / 'control/resolv.conf', '/etc/resolv.conf')
    args += mount(ROOT / 'ca/public-roots.pem', '/etc/ssl/certs/ca-certificates.crt')
    args += ['--workdir', '/opt/copybot', '--entrypoint', '/usr/bin/env', APP_IMAGE,
        '-i', 'SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt',
        'SOLANA_COPY_BOT_EXECUTION_ENABLED=false', 'SOLANA_COPY_BOT_EXECUTION_CANARY_TINY_SUBMIT_ENABLED=false',
        '/opt/copybot/bin/copybot-app', '--config', '/run/probe-config/read-only.toml']
    values['observation-app'] = args
    return values


def create():
    if (ROOT / 'CONTAINERS.json').exists():
        raise ValueError('probe_containers_already_created')
    docker(['volume', 'create', '--label', LABEL + '=' + RUN, RUN + '-uds'])
    cids = {}
    for role in ['stream-backend', 'stream-front', 'observation-app']:
        args = commands(cids.get('stream-front', '<EXACT_PROBE_FRONT_CID>'))[role]
        cid = docker(args[1:])
        value = verify_container(cid)
        if value['State']['Status'] != 'created' or value['State']['Running']:
            raise ValueError('probe_container_not_inactive')
        cids[role] = cid
        save(ROOT / 'CONTAINERS.PARTIAL.json', cids)
    save(ROOT / 'CONTAINERS.json', cids)
    return cids


def init_volume():
    args = ['run', '--rm', '--network', 'none', '--platform', 'linux/amd64', '--pull=never',
        '--label', LABEL + '=' + RUN, '--read-only', '--cap-drop', 'ALL', '--cap-add', 'CHOWN']
    args += mount(RUN + '-uds', '/relay', True, True)
    args += ['--entrypoint', '/usr/bin/python3', PY_IMAGE, '-c',
             'import os;os.chmod("/relay",0o700);os.chown("/relay",501,20)']
    docker(args)


def stop(cids):
    errors = []
    try:
        (ROOT / 'control/STREAM_STOP').touch(mode=0o600)
    except OSError as error:
        errors.append(dict(role='stream-control', error_type=type(error).__name__, reason='stream_stop_file_failed'))
    for role in ['observation-app', 'stream-front', 'stream-backend']:
        cid = cids.get(role)
        if cid:
            try:
                verify_container(cid)
                docker(['stop', '-t', '5', cid])
            except Exception as error:
                errors.append(dict(role=role, error_type=type(error).__name__, reason=str(error)[:160]))
    return errors
