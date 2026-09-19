"""Restart preflight and exact task-owned process identities."""
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import time
from session_common import L,T,RUN,read,save,digest,now,event

def schema_hash(c):
    return digest(json.dumps(c.execute("SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name").fetchall()).encode())

def initialize():
    if(L/'IDENTITY.json').exists():verify();return
    from session_budget import Budget
    from session_http import Broker
    Budget();Broker();event('session_initialized',run_id=RUN)
    from session_schedule import initialize as initialize_schedule
    initialize_schedule()
    from session_deadline import anchor
    anchor()
    paths=[L/'state'/n for n in('budget.db','http.db','control.db')]
    databases={}
    for p in paths:
        entry={'inode':p.stat().st_ino}
        if p.name!='live_runtime.db':
            c=sqlite3.connect(p)
            try:
                c.execute('CREATE TABLE session_identity(run_id TEXT PRIMARY KEY)');c.execute('INSERT INTO session_identity VALUES(?)',(RUN,));c.commit()
                entry['schema_sha256']=schema_hash(c)
                entry['sentinel_run_id']=RUN
            finally:c.close()
        databases[str(p)]=entry
    from session_sqlite import inspect_live
    for name in ('live_runtime.db','capture.db','virtual.db'):
        result=inspect_live(name,True)
        if result['quick_check']!=[['ok']]:raise ValueError('linux_initial_integrity_failed')
        databases[str(L/'state'/name)]={'linux_stat':result['stat'],'schema_sha256':result['schema_sha256']}
    save(L/'IDENTITY.json',{'run_id':RUN,'created_at':now(),'databases':databases,
        'config_sha256':digest((L/'configs/live.disabled.toml').read_bytes()),
        'helpers':{str(p):digest(p.read_bytes())for p in sorted((T/'helpers').glob('*.py'))}})

def verify(full=False):
    identity=read(L/'IDENTITY.json')
    if not identity or identity['run_id']!=RUN:raise ValueError('session_identity_missing_or_foreign')
    for name,meta in identity['databases'].items():
        p=Path(name)
        if p.name in ('live_runtime.db','discovery_recent_raw.db','capture.db','virtual.db'):
            from session_sqlite import inspect_live
            result=inspect_live(p.name,full)
            if result['stat']!=meta.get('linux_stat'):raise ValueError('session_linux_database_identity_changed')
            if full and result['quick_check']!=[['ok']]:raise ValueError('session_database_integrity_failed')
            if meta.get('schema_sha256')and result['schema_sha256']!=meta['schema_sha256']:raise ValueError('session_state_schema_changed')
            continue
        if not p.is_file()or p.stat().st_ino!=meta['inode']:raise ValueError('session_database_identity_changed')
        if full or meta.get('schema_sha256'):
            c=sqlite3.connect(p.as_uri()+'?mode=ro',uri=True,timeout=2)
            try:
                if full and c.execute('PRAGMA quick_check').fetchall()!=[('ok',)]:raise ValueError('session_database_integrity_failed')
                if meta.get('schema_sha256')and schema_hash(c)!=meta['schema_sha256']:raise ValueError('session_state_schema_changed')
                if meta.get('sentinel_run_id')and c.execute('SELECT run_id FROM session_identity').fetchall()!=[(RUN,)]:raise ValueError('session_state_sentinel_changed')
            finally:c.close()
    if digest((L/'configs/live.disabled.toml').read_bytes())!=identity['config_sha256']:raise ValueError('config_changed_after_preflight')
    for name,h in identity['helpers'].items():
        if not Path(name).is_file()or digest(Path(name).read_bytes())!=h:raise ValueError('helper_changed_after_preflight')
    recent=L/'state/discovery_recent_raw.db'
    if recent.exists()and str(recent)not in identity['databases']:
        from session_sqlite import inspect_live
        result=inspect_live(recent.name)
        if {'observed_swaps','recent_raw_journal_state','observed_retention_boundary'}<=set(result['tables']):
            identity['databases'][str(recent)]={'linux_stat':result['stat'],'schema_sha256':result['schema_sha256']};save(L/'IDENTITY.json',identity)
    return identity

def process_identity(pid):
    r=subprocess.run(['/bin/ps','-p',str(pid),'-o','lstart=','-o','command='],capture_output=True,text=True)
    return r.stdout.strip()if r.returncode==0 else None

def operator_callback(row):
    values=read(L/'OPERATORS.json',{})
    if row['action']=='started':row['process_identity']=process_identity(row['pid'])
    elif str(row['pid'])in values:row['process_identity']=values[str(row['pid'])].get('process_identity')
    values[str(row['pid'])]=row;save(L/'OPERATORS.json',values)

def stop_operators():
    for key,row in read(L/'OPERATORS.json',{}).items():
        if row.get('action')!='started':continue
        pid=int(key);actual=process_identity(pid)
        if actual is None:continue
        if actual!=row.get('process_identity')or str(L/'discovery-work')not in actual:raise ValueError('operator_process_ownership_unknown')
        os.killpg(pid,signal.SIGTERM)
        until=time.monotonic()+2
        while process_identity(pid)==actual and time.monotonic()<until:time.sleep(.05)
        if process_identity(pid)==actual:os.killpg(pid,signal.SIGKILL)
    save(L/'OPERATORS_STOPPED.json',{'at':now()})
