"""Exact task-labelled Docker objects; creation never starts app or provider relays."""
import json
from pathlib import Path
import re
import subprocess
import time
from session_common import R, L, T, RUN, now, read, save
from session_prepare import INSTALL, SOURCE_ENV, CA, G

CLI = ['/usr/local/bin/docker', '--host', 'unix:///Users/tigranambarcumyan/.docker/run/docker.sock',
       '--config', str(T/'docker-cli')]
ENV = dict(PATH='/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin', LANG='C')
RUNTIME = 'docker.io/library/ubuntu@sha256:224a1869083a311ef3f13648a154ba79832fbef6364d31493642ca03082da254'
RUNTIME_ID = 'sha256:a61567bd31828687156d735ea8eb01ba4e37636e225dd6a48ba94136a70d9d61'
PYTHON_ID = 'sha256:09ecaa87c6799c8d8ee0dfb779905d97e5f667ab97d866cc47ff9f949ffb7b3b'
LABEL = 'copybot.forward.run_id'


def docker(args, timeout=30):
    result = subprocess.run(CLI+args, env=ENV, capture_output=True, text=True, timeout=timeout)
    if result.returncode:
        # Error text can carry operational details; keep it private, no credential-bearing argv values.
        save(T/'checks'/('docker-error-'+str(time.time_ns())+'.json'),
             dict(args=args, exit_code=result.returncode, stdout=result.stdout, stderr=result.stderr))
        raise RuntimeError('task_docker_'+args[0]+'_exit_'+str(result.returncode))
    return result.stdout.strip()


def image_bindings():
    values = {}
    for role, ref, expected in [('runtime', RUNTIME, RUNTIME_ID), ('python', PYTHON_ID, PYTHON_ID)]:
        obj = json.loads(docker(['image', 'inspect', '--platform', 'linux/amd64', ref]))[0]
        if obj['Id'] != expected or obj['Os'] != 'linux' or obj['Architecture'] != 'amd64':
            raise ValueError('task_image_identity_or_platform_mismatch')
        candidates = obj.get('RepoDigests', [])
        selected = RUNTIME if role == 'runtime' else next((s for s in candidates if s.endswith('@'+PYTHON_ID)), None)
        if not selected:
            raise ValueError('task_python_canonical_repository_digest_missing')
        values[role] = dict(reference=selected, image_id=obj['Id'], os='linux', architecture='amd64')
    save(T/'IMAGE_BINDINGS.json', values)
    return values


def registry():
    state = read(L/'CONTAINERS.json', dict(run_id=RUN, generations=[], containers={}, volumes={}))
    if state['run_id'] != RUN:
        raise ValueError('container_registry_wrong_run')
    return state


def labels(role, generation):
    return {LABEL: RUN, 'copybot.forward.role': role, 'copybot.forward.generation': str(generation)}


def label_args(role, generation):
    out = []
    for key, value in labels(role, generation).items():
        out += ['--label', key+'='+value]
    return out


def cid(value):
    if not re.fullmatch(r'[0-9a-f]{64}', value):
        raise ValueError('exact_container_id_required')
    return value


def inspect_owned(container_id):
    container_id = cid(container_id)
    row = registry()['containers'].get(container_id)
    if not row or row.get('removed'):
        raise ValueError('container_not_registered_to_task')
    obj = json.loads(docker(['inspect', container_id]))[0]
    actual = obj['Config'].get('Labels') or {}
    if obj['Id'] != container_id or any(actual.get(k) != v for k, v in labels(row['role'], row['generation']).items()):
        raise ValueError('container_label_ownership_mismatch')
    return obj


def mount(source, destination, readonly=True, volume=False):
    return ['--mount', 'type='+('volume' if volume else 'bind')+',src='+str(source)+',dst='+destination+(',readonly' if readonly else '')]


def mount_source(row):
    if row['Type']=='volume':return row.get('Name')
    source=row['Source']
    return source[len('/host_mnt'):] if source.startswith('/host_mnt/Users/') else source


def base(role, generation):
    resources=read(T/'config/capacity.disabled.json',{}).get('resource_proposal',{})
    common = ['--platform', 'linux/amd64', '--pull=never', '--restart', 'no', '--read-only',
              '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges']+label_args(role, generation)
    if role == 'sqlite':
        memory=str(resources.get('controller_memory_bytes',256*2**20)+resources.get('workers_memory_aggregate_bytes',2*2**30))
        return common+['--cpus', '1', '--memory', memory, '--memory-swap', memory,
                      '--pids-limit', '64', '--log-driver', 'none']
    if role == 'app':
        memory=str(resources.get('app_memory_bytes',4*2**30))
        return common+['--cpus', '2', '--memory', memory, '--memory-swap', memory, '--pids-limit', '128',
            '--tmpfs', '/tmp:rw,noexec,nosuid,size=64m', '--log-driver', 'local',
            '--log-opt', 'max-size=16m', '--log-opt', 'max-file=4']
    memory=str(resources.get('relay_memory_each_bytes',256*2**20)) if role in ('front','backend') else '64m'
    return common+['--cpus', '0.125', '--memory', memory, '--memory-swap', memory,
                  '--pids-limit', '16', '--log-driver', 'none']


def register_created(container_id, role, generation, name):
    state = registry()
    state['containers'][container_id] = dict(role=role, generation=generation, name=name, created_at=now(), removed=False)
    save(L/'CONTAINERS.json', state)
    return inspect_owned(container_id)


def create_container(role, generation, args):
    name = ('copybot-'+RUN+'-g'+str(generation)+'-'+role).lower()
    container_id = cid(docker(['create', '--name', name]+base(role, generation)+args))
    obj = register_created(container_id, role, generation, name)
    if obj['State']['Running'] or obj['State']['Status'] != 'created':
        raise ValueError('container_unexpected_started_state')
    return container_id


def verify_group(group):
    cids = group['cids']
    for role, container_id in cids.items():
        obj = inspect_owned(container_id); host = obj['HostConfig']; config = obj['Config']
        expected_network = 'container:'+cids['front'] if role == 'app' else 'none' if role == 'front' else 'bridge'
        if host['NetworkMode'] != expected_network or host['PortBindings']:
            raise ValueError('task_container_network_mismatch')
        if host['RestartPolicy']['Name'] != 'no' or not host['ReadonlyRootfs'] or host['CapDrop'] != ['ALL']:
            raise ValueError('task_container_lifecycle_or_privilege_mismatch')
        if not any(s.startswith('no-new-privileges') for s in host['SecurityOpt']):
            raise ValueError('task_container_privilege_escalation_possible')
        if config['User'] != '501:20':
            raise ValueError('task_container_user_mismatch')
        if role == 'app':
            expected = {G: (str(INSTALL), False), G+'/configs': (str(L/'configs'), False),
                G+'/state': (read(L/'SQLITE_STATE.json')['volume'], True), G+'/state/launch-ingestion-source.env': (str(SOURCE_ENV), False),
                '/etc/hosts': (str(L/'control/hosts'), False), '/etc/resolv.conf': (str(L/'control/resolv.conf'), False),
                '/etc/ssl/certs/ca-certificates.crt': (str(CA), False)}
            actual = {m['Destination']: (mount_source(m), m['RW']) for m in obj['Mounts']}
            if actual != expected or '/relay' in actual:
                raise ValueError('task_app_mount_binding_mismatch')
            logs = host['LogConfig']
            if logs['Type'] != 'local' or logs['Config'].get('max-size') != '16m' or logs['Config'].get('max-file') != '4':
                raise ValueError('task_app_log_rotation_mismatch')
            if config['Entrypoint'] != ['/usr/bin/env'] or config['Cmd'] != app_command():
                raise ValueError('task_app_disabled_command_mismatch')
        elif host['LogConfig']['Type'] != 'none':
            raise ValueError('task_relay_log_driver_mismatch')
    return True


def app_command():
    return ['-i', 'SOLANA_COPY_BOT_CONFIG='+G+'/configs/live.disabled.toml',
        'SOLANA_COPY_BOT_INGESTION_OVERRIDE_FILE='+G+'/state/launch-ingestion-source.env',
        'SOLANA_COPY_BOT_EXECUTION_ENABLED=false', 'SOLANA_COPY_BOT_EXECUTION_CANARY_TINY_SUBMIT_ENABLED=false',
        G+'/bin/copybot-app', '--config', G+'/configs/live.disabled.toml']


def create_generation(generation):
    if type(generation) is not int or generation < 1:
        raise ValueError('generation_must_be_positive_integer')
    images = image_bindings(); state = registry()
    if any(g['generation'] == generation for g in state['generations']):
        raise ValueError('generation_already_created_use_existing_ids')
    if not read(T/'PREPARE_BINDING.json'):
        raise ValueError('task_preparation_not_bound')
    volume = ('copybot-'+RUN+'-g'+str(generation)+'-uds').lower()
    group = dict(run_id=RUN, generation=generation, cids={}, volume=volume, directory=str(L/'control'), created_at=now())
    state['generations'].append(group)
    save(L/'CONTAINERS.json', state)
    docker(['volume', 'create']+label_args('uds', generation)+[volume])
    state = registry(); state['volumes'][volume] = dict(generation=generation, removed=False)
    save(L/'CONTAINERS.json', state)
    init = create_container('volume-init', generation,
        ['--network', 'none', '--cap-add', 'CHOWN']+mount(volume, '/relay', False, True)+
        ['--entrypoint', '/usr/bin/python3', images['python']['reference'], '-c',
         'import os; os.chmod("/relay",0o700); os.chown("/relay",501,20)'])
    start_owned(init, attach=True)
    if inspect_owned(init)['State']['ExitCode'] != 0:
        raise ValueError('task_volume_initialization_failed')
    group['init_cid'] = init
    for role in ('backend', 'front'):
        args = ['--user', '501:20']+mount(T/'helpers', '/helpers')+mount(L/'control', '/control', False)+mount(volume, '/relay', False, True)
        if role == 'front':
            args += ['--sysctl', 'net.ipv4.ip_unprivileged_port_start=0']
        args += ['--network', 'none' if role == 'front' else 'bridge', '--entrypoint', '/usr/bin/python3',
                 images['python']['reference'], '-B', '/helpers/session_relay.py', role]
        group['cids'][role] = create_container(role, generation, args)
    args = ['--user', '501:20', '--workdir', G, '--network', 'container:'+group['cids']['front']]
    args += mount(INSTALL, G)+mount(L/'configs', G+'/configs')+mount(read(L/'SQLITE_STATE.json')['volume'], G+'/state', False, True)
    args += mount(SOURCE_ENV, G+'/state/launch-ingestion-source.env')+mount(L/'control/hosts', '/etc/hosts')
    args += mount(L/'control/resolv.conf', '/etc/resolv.conf')+mount(CA, '/etc/ssl/certs/ca-certificates.crt')
    args += ['--entrypoint', '/usr/bin/env', images['runtime']['reference']]+app_command()
    group['cids']['app'] = create_container('app', generation, args)
    verify_group(group)
    state = registry()
    state['generations'] = [group if g['generation'] == generation else g for g in state['generations']]
    save(L/'CONTAINERS.json', state)
    save(T/'checks'/('GENERATION_'+str(generation)+'_CREATED.json'), dict(group=group, all_runtime_containers_stopped=True,
        local_volume_init_only_started=True, provider_connections=0, app_starts=0, verified=True))
    return group


def start_owned(container_id, attach=False):
    obj = inspect_owned(container_id)
    if obj['State']['Running']:
        return obj
    docker(['start']+(['-a'] if attach else [])+[cid(container_id)])
    return inspect_owned(container_id)


def stop_owned(container_id):
    obj = inspect_owned(container_id)
    if obj['State']['Running']:
        docker(['stop', '--time', '5', cid(container_id)], timeout=15)
    result = inspect_owned(container_id)
    if result['State']['Running']:
        raise RuntimeError('task_container_stop_not_confirmed')
    return result


def cleanup_generation(group):
    generation = group['generation']; state = registry()
    rows = [(key, row) for key, row in state['containers'].items() if row['generation'] == generation and not row.get('removed')]
    rows.sort(key=lambda item: 0 if item[1]['role'] == 'app' else 1)
    for container_id, _ in rows:
        stop_owned(container_id)
        docker(['rm', container_id])
        state = registry(); state['containers'][container_id]['removed'] = True
        save(L/'CONTAINERS.json', state)
    volume = group['volume']
    data = json.loads(docker(['volume', 'inspect', volume]))[0]
    if any((data.get('Labels') or {}).get(k) != v for k, v in labels('uds', generation).items()):
        raise ValueError('volume_ownership_mismatch')
    docker(['volume', 'rm', volume])
    state = registry(); state['volumes'][volume]['removed'] = True
    save(L/'CONTAINERS.json', state)


prepare_generation = create_generation
