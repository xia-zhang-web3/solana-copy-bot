"""Keep every live app SQLite connection in Docker's Linux kernel."""
import json
from pathlib import Path
import sqlite3
import threading
from session_common import L, T, RUN, read, save, now
import session_docker as docker

LOCK = threading.Lock()
CLOSING = False
LIVE_NAMES = ('live_runtime.db', 'discovery_recent_raw.db', 'capture.db', 'virtual.db')


def ensure_bridge():
    with LOCK:
        if CLOSING:
            raise RuntimeError('sqlite_bridge_stopping')
        storage = read(L/'SQLITE_STATE.json')
        if not storage or storage['run_id'] != RUN:
            raise ValueError('linux_sqlite_state_not_migrated')
        volume = storage['volume']
        labels = json.loads(docker.docker(['volume','inspect',volume]))[0].get('Labels') or {}
        if any(labels.get(k) != v for k,v in docker.labels('sqlite-state',0).items()):
            raise ValueError('linux_sqlite_volume_ownership_changed')
        binding = read(L/'SQLITE_BRIDGE.json')
        if binding:
            if binding['run_id'] != RUN:
                raise ValueError('sqlite_bridge_wrong_session')
            cid = binding['cid']
        else:
            images = docker.image_bindings()
            (L/'discovery-work').mkdir(mode=0o700, exist_ok=True)
            args = ['--user', '501:20', '--network', 'none']
            args += docker.mount(T/'helpers', '/helpers')
            args += docker.mount(read(T/'CONTEXT.json')['source'], '/repo')
            args += docker.mount(T, '/evidence', False)
            args += docker.mount(volume, '/state', False, True)
            args += docker.mount(L/'discovery-work', '/exports', False)
            args += ['--entrypoint', '/usr/bin/python3', images['python']['reference'],
                     '-B', '-c', 'import time; time.sleep(86400)']
            cid = docker.create_container('sqlite', 0, args)
            binding = dict(run_id=RUN, cid=cid, created_at=now())
            save(L/'SQLITE_BRIDGE.json', binding)
        obj = docker.inspect_owned(cid)
        expected = {'/helpers': (str(T/'helpers'), False),
                    '/repo': (read(T/'CONTEXT.json')['source'], False),
                    '/evidence': (str(T), True),
                    '/state': (volume, True),
                    '/exports': (str(L/'discovery-work'), True)}
        actual = {m['Destination']: (docker.mount_source(m), m['RW']) for m in obj['Mounts']}
        host = obj['HostConfig']
        if (actual != expected or host['NetworkMode'] != 'none' or host['PortBindings']
                or obj['Image'] != docker.PYTHON_ID or obj['Config']['User'] != '501:20'
                or host['RestartPolicy']['Name'] != 'no' or not host['ReadonlyRootfs']):
            raise ValueError('sqlite_bridge_binding_changed')
        if not obj['State']['Running']:
            docker.start_owned(cid)
        return cid


def request(payload):
    cid = ensure_bridge()
    output = docker.docker(['exec', cid, '/usr/bin/python3', '-B',
                            '/helpers/session_sqlite_linux.py', json.dumps(payload)], timeout=45)
    value = json.loads(output)
    if value.get('ok') is not True:
        kind = value.get('error_type')
        if kind in ('OperationalError', 'DatabaseError', 'IntegrityError'):
            cls = getattr(sqlite3, kind)
            error = cls(value.get('error', 'linux_sqlite_error'))
            if value.get('sqlite_errorcode') is not None:
                error.sqlite_errorcode = value['sqlite_errorcode']
            raise error
        raise ValueError('linux_sqlite_' + str(kind))
    return value['result']


def inspect_live(name, full=False):
    if name not in LIVE_NAMES:
        raise ValueError('sqlite_live_name_invalid')
    return request(dict(op='inspect', name=name, full=full))


def observe(cursor):
    return request(dict(op='observe', cursor=cursor))


def backup(source, target):
    source, target = Path(source), Path(target)
    if source.parent != L/'state' or source.name not in LIVE_NAMES:
        raise ValueError('sqlite_backup_source_invalid')
    relative = target.relative_to(L/'discovery-work')
    if '..' in relative.parts or target.is_symlink() or target.exists():
        raise ValueError('sqlite_backup_target_invalid')
    return request(dict(op='backup', name=source.name, target='/exports/'+relative.as_posix()))


def stop_bridge():
    global CLOSING
    with LOCK:
        CLOSING = True
        binding = read(L/'SQLITE_BRIDGE.json')
        if not binding:
            return None
        obj = docker.stop_owned(binding['cid'])
        result = dict(at=now(), cid=binding['cid'], state=obj['State'])
        save(L/'SQLITE_BRIDGE_STOPPED.json', result)
        return result
