"""Fresh task-labelled Linux SQLite volume; no provider sockets or daemon start."""
import json
from pathlib import Path
from session_common import L,T,RUN,CTX,now,save,read


def initialize_state(state, seed, source):
    """Executed only inside the network-none Linux init container."""
    import os
    import shutil
    import sys
    state, seed = Path(state), Path(seed)
    if any(state.iterdir()):
        raise ValueError('fresh_linux_volume_not_empty')
    shutil.copyfile(seed/'live_runtime.db',state/'live_runtime.db')
    sys.path.insert(0,str(Path(source)/'tools'))
    from capture_virtual import CaptureControl, CaptureVirtualAdapter
    from virtual_engine import VirtualLedger
    control = CaptureControl.create(state/'capture.db',max_rows=2_000_000,max_bytes=48*2**30)
    ledger = VirtualLedger(state/'virtual.db')
    try:
        CaptureVirtualAdapter(control,ledger)
    finally:
        ledger.close();control.close()
    (state/'launch-ingestion-source.env').touch(mode=0o600)
    for path in state.iterdir():
        os.chmod(path,0o600);os.chown(path,501,20)
    os.chmod(state,0o700);os.chown(state,501,20)


def prepare_linux(origin):
    import session_docker as d
    if read(L/'SQLITE_STATE.json'):
        raise ValueError('linux_state_already_exists_no_reinitialize')
    if Path(origin['origin']) != L/'preparation/live_runtime.db' or not origin['all_business_tables_empty']:
        raise ValueError('fresh_schema_seed_required')
    from session_origin import file_hash
    if file_hash(origin['origin']) != origin['origin_sha256']:
        raise ValueError('fresh_schema_seed_changed')
    images = d.image_bindings();volume = 'copybot-'+RUN+'-state'
    existing = d.docker(['volume','ls','--filter','name=^'+volume+'$','--format','{{.Name}}'])
    if existing:
        raise ValueError('named_volume_exists_no_overwrite')
    d.docker(['volume','create']+d.label_args('sqlite-state',0)+[volume])
    actual = json.loads(d.docker(['volume','inspect',volume]))[0]
    if any((actual.get('Labels') or {}).get(k)!=v for k,v in d.labels('sqlite-state',0).items()):
        raise ValueError('new_volume_ownership_mismatch')
    args = ['--network','none','--cap-add','CHOWN','--cap-add','DAC_OVERRIDE','--cap-add','FOWNER']
    args += d.mount(volume,'/state',False,True)+d.mount(L/'preparation','/seed')
    args += d.mount(T/'helpers','/helpers')+d.mount(CTX['source'],'/repo')
    args += ['--env','PYTHONPATH=/helpers','--entrypoint','/usr/bin/python3',images['python']['reference'],
             '-B','-c','from session_prepare_linux import initialize_state; initialize_state("/state","/seed","/repo")']
    # Import session_common inside Linux with an explicit context; all paths it uses
    # during this initializer are unused except imports, so no host DB is opened.
    context = dict(CTX,repo='/repo',runtime='/state',evidence='/evidence')
    context_path = L/'preparation/linux-context.json';save(context_path,context)
    entry = args.index('--entrypoint')
    args[entry:entry] = ['--env','FORWARD_CONTEXT=/seed/linux-context.json']
    cid = d.create_container('state-init',0,args);d.start_owned(cid,attach=True)
    if d.inspect_owned(cid)['State']['ExitCode'] != 0:
        raise ValueError('linux_state_initialization_failed_preserve_evidence')
    result = dict(volume=volume,run_id=RUN,created_at=now(),init_cid=cid,
                  host_opens_virtual_db=False,capture_max_rows=2_000_000,capture_max_bytes=48*2**30)
    save(L/'SQLITE_STATE.json',result)
    return result
