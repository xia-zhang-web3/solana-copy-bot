"""Task-owned SQLite storage, never the app runtime database or execution tables."""
from contextlib import contextmanager
from fractions import Fraction
import json
import sqlite3
import threading
import time


def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'))
def decoded(value):return json.loads(value) if value else {}
def now():return time.time()

SCHEMA='''
CREATE TABLE IF NOT EXISTS decisions(id TEXT PRIMARY KEY,available REAL NOT NULL,expires REAL NOT NULL,admissible INTEGER NOT NULL,wallets TEXT NOT NULL,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY,key TEXT UNIQUE NOT NULL,signature TEXT NOT NULL,wallet TEXT NOT NULL,mint TEXT NOT NULL,side TEXT NOT NULL,detected REAL NOT NULL,decision_id TEXT,status TEXT NOT NULL,reason TEXT,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS lots(id INTEGER PRIMARY KEY,event_id INTEGER UNIQUE NOT NULL,wallet TEXT NOT NULL,mint TEXT NOT NULL,account TEXT,initial_raw TEXT NOT NULL DEFAULT '0',remaining_raw TEXT NOT NULL DEFAULT '0',remaining_cost TEXT NOT NULL DEFAULT '0',quote_status TEXT NOT NULL DEFAULT 'PENDING',proof_status TEXT NOT NULL DEFAULT 'PENDING',risk TEXT NOT NULL DEFAULT '',proceeds TEXT NOT NULL DEFAULT '0');
CREATE TABLE IF NOT EXISTS jobs(id INTEGER PRIMARY KEY,event_id INTEGER NOT NULL,kind TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'PENDING',created REAL NOT NULL,started REAL,completed REAL,payload TEXT NOT NULL,outcome TEXT,UNIQUE(event_id,kind));
CREATE TABLE IF NOT EXISTS proofs(event_id INTEGER NOT NULL,kind TEXT NOT NULL,status TEXT NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(event_id,kind));
CREATE TABLE IF NOT EXISTS allocations(event_id INTEGER NOT NULL,lot_id INTEGER NOT NULL,raw TEXT NOT NULL,cost TEXT NOT NULL,PRIMARY KEY(event_id,lot_id));
CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
'''

class Store:
    def __init__(self,path):
        self.path=str(path);self.lock=threading.RLock()
        self.db=sqlite3.connect(self.path,timeout=10,isolation_level=None,check_same_thread=False)
        self.db.row_factory=sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL');self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript(SCHEMA)
        columns={r[1] for r in self.db.execute('PRAGMA table_info(jobs)')}
        for name,definition in [('next_retry_at','REAL NOT NULL DEFAULT 0'),('attempts','INTEGER NOT NULL DEFAULT 0')]:
            if name not in columns:self.db.execute('ALTER TABLE jobs ADD COLUMN '+name+' '+definition)
        with self.atomic():
            self.db.execute("UPDATE jobs SET state='PENDING',started=NULL WHERE state='RUNNING'")
    @contextmanager
    def atomic(self):
        with self.lock:
            self.db.execute('BEGIN IMMEDIATE')
            try:yield;self.db.execute('COMMIT')
            except BaseException:self.db.execute('ROLLBACK');raise
    def close(self):
        with self.lock:self.db.close()
    def event(self,event_id):
        r=self.db.execute('SELECT * FROM events WHERE id=?',(event_id,)).fetchone()
        return dict(r) if r else None
    def job(self,job_id):
        r=self.db.execute('SELECT * FROM jobs WHERE id=?',(job_id,)).fetchone()
        if not r:return None
        d=dict(r);d['payload']=decoded(d['payload']);d['outcome']=decoded(d['outcome']);return d
    def put_job(self,event_id,kind,payload):
        self.db.execute('INSERT OR IGNORE INTO jobs(event_id,kind,created,payload) VALUES(?,?,?,?)',(event_id,kind,now(),encoded(payload)))
    def put_proof(self,event_id,kind,result):
        self.db.execute('INSERT OR REPLACE INTO proofs VALUES(?,?,?,?)',(event_id,kind,result['status'],encoded(result)))
    def proof(self,event_id,kind):
        r=self.db.execute('SELECT payload FROM proofs WHERE event_id=? AND kind=?',(event_id,kind)).fetchone()
        return decoded(r[0]) if r else None
    def set_event(self,event_id,status,reason=None):
        self.db.execute('UPDATE events SET status=?,reason=? WHERE id=?',(status,reason,event_id))
    def meta(self,key,default=None):
        # Observer status reads also share this connection with worker transactions.
        with self.lock:
            r=self.db.execute('SELECT value FROM metadata WHERE key=?',(key,)).fetchone();return decoded(r[0]) if r else default
    def set_meta(self,key,value):self.db.execute('INSERT OR REPLACE INTO metadata VALUES(?,?)',(key,encoded(value)))
    def advance_cursor(self,cursor):
        previous=self.meta('cursor',cursor)
        self.set_meta('cursor',max(previous,cursor) if isinstance(previous,int) and isinstance(cursor,int) else cursor)
    def pending_jobs(self,limit=100):
        with self.lock:
            ids=[r[0] for r in self.db.execute("SELECT id FROM jobs WHERE state='PENDING' ORDER BY CASE kind WHEN 'SELL_QUOTE' THEN 0 WHEN 'BUY_QUOTE' THEN 1 ELSE 2 END,id LIMIT ?",(limit,))]
            return [self.job(i) for i in ids]
    def claim_job(self,kind=None):
        with self.atomic():
            query="SELECT id FROM jobs WHERE state='PENDING' AND next_retry_at<=?";args=(now(),)
            if kind:query+=' AND kind=?';args+=(kind,)
            row=self.db.execute(query+" ORDER BY CASE kind WHEN 'SELL_QUOTE' THEN 0 WHEN 'BUY_QUOTE' THEN 1 ELSE 2 END,id LIMIT 1",args).fetchone()
            if not row:return None
            self.db.execute("UPDATE jobs SET state='RUNNING',started=?,attempts=attempts+1 WHERE id=?",(now(),row[0]));return self.job(row[0])
    def finish_job(self,job_id,outcome):
        j=self.job(job_id)
        if not j or j['state'] in ('DONE','STOPPED'):return None
        self.db.execute("UPDATE jobs SET state='DONE',completed=?,outcome=? WHERE id=?",(now(),encoded(outcome),job_id))
        return j
    def exposure(self):
        capital=sum(Fraction(r[0]) for r in self.db.execute("SELECT remaining_cost FROM lots WHERE quote_status='OK'"))
        self.set_meta('peak_open_cost_lamports',max(Fraction(self.meta('peak_open_cost_lamports','0')),capital).__str__())
        pending=self.db.execute("SELECT count(*) FROM lots WHERE quote_status='PENDING'").fetchone()[0]
        self.set_meta('peak_open_plus_pending_lamports',str(max(Fraction(self.meta('peak_open_plus_pending_lamports','0')),capital+pending*10000000)))

    def retry_job(self,job_id,outcome,not_before):
        with self.atomic():
            j=self.job(job_id)
            if not j or j['state'] in ('DONE','STOPPED'):return
            self.db.execute("UPDATE jobs SET state='PENDING',next_retry_at=?,outcome=? WHERE id=?",(float(not_before),encoded(outcome),job_id))
    def stop_pending(self,reason='session_deadline'):
        with self.atomic():
            self.set_meta('admissions_stopped',reason)
            self.db.execute("UPDATE jobs SET state='STOPPED',completed=?,outcome=? WHERE state IN ('PENDING','RUNNING')",(now(),encoded({'status':'UNQUERIED','reason':reason})))
            self.db.execute("UPDATE events SET status='STOPPED',reason=? WHERE status IN ('BUY_PENDING','SELL_PENDING','SELL_QUOTE_PENDING')",(reason,))
            self.db.execute("UPDATE lots SET risk=? WHERE quote_status='PENDING' OR proof_status='PENDING'",(reason,))
            self.exposure()
