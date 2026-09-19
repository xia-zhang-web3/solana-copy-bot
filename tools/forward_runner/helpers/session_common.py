"""Paths and atomic metadata for one explicit eight-hour experiment."""
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time

T=Path(__file__).resolve().parents[1]
CTX=json.loads(Path(os.environ.get('FORWARD_CONTEXT',str(T/'CONTEXT.json'))).read_text())
T=Path(CTX.get('evidence',str(T)))
R=Path(CTX['repo']);L=Path(CTX['runtime']);RUN=L.name
UTC=dt.timezone.utc

def now():return dt.datetime.now(UTC).isoformat()
def epoch(s):return dt.datetime.fromisoformat(s).timestamp()
def digest(data):return hashlib.sha256(data).hexdigest()
def read(path,default=None):
    try:return json.loads(Path(path).read_text())
    except FileNotFoundError:return default
def save(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    tmp=path.with_name(path.name+'.'+str(os.getpid())+'.'+str(time.time_ns())+'.tmp')
    with tmp.open('x')as f:
        os.chmod(tmp,0o600);json.dump(value,f,ensure_ascii=False,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
    os.replace(tmp,path)
def connection(path):
    path=Path(path);identity=read(L/'IDENTITY.json',{})
    expected=identity.get('databases',{}).get(str(path))
    if expected and (not path.is_file()or path.stat().st_ino!=expected['inode']):raise ValueError('session_database_replaced_or_missing')
    c=sqlite3.connect(path.as_uri()+'?mode=rw',uri=True,timeout=5)if expected else sqlite3.connect(path,timeout=5)
    c.row_factory=sqlite3.Row
    c.execute('PRAGMA journal_mode=WAL');c.execute('PRAGMA synchronous=FULL');c.execute('PRAGMA busy_timeout=5000');return c
def stopped():return(L/'STOP_REQUEST.json').exists()
def stop_request(reason):
    value={'run_id':RUN,'reason':reason,'requested_at':now()}
    save(L/'STOP_REQUEST.json',value);save(L/'control/STOP_REQUEST.json',value)
def event(kind,**data):
    c=connection(L/'state/control.db')
    with c:
        c.execute('CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY,ts TEXT,kind TEXT,payload TEXT)')
        c.execute('INSERT INTO events(ts,kind,payload)VALUES(?,?,?)',(now(),kind,json.dumps(data)))
    c.close()
