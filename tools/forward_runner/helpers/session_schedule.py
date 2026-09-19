"""A database-anchored deadline; JSON is a recoverable status mirror only."""
from contextlib import closing
import datetime as dt
import json
import threading
from session_common import L,connection,read,save,now

LOCK=threading.RLock()

def initialize():
    with closing(connection(L/'state/control.db'))as c,c:
        c.execute('CREATE TABLE IF NOT EXISTS session_schedule(id INTEGER PRIMARY KEY CHECK(id=1),payload TEXT NOT NULL)')

def schedule():
    with LOCK,closing(connection(L/'state/control.db'))as c:
        row=c.execute('SELECT payload FROM session_schedule WHERE id=1').fetchone()
        mirror=read(L/'SCHEDULE.json')
        if not row:
            if mirror or c.execute("SELECT 1 FROM events WHERE kind='fresh_ingress_confirmed' LIMIT 1").fetchone():
                raise ValueError('established_schedule_anchor_missing')
            return None
        value=json.loads(row[0])
        if mirror is None:save(L/'SCHEDULE.json',value)
        elif mirror!=value:raise ValueError('schedule_mirror_conflicts_with_database_anchor')
        return value

def establish(first,state):
    with LOCK:
        existing=schedule()
        if existing:return existing
        from session_deadline import deadline
        start=now()
        value={'started_at':start,'ends_at':dt.datetime.fromtimestamp(deadline(),dt.timezone.utc).isoformat(),
            'anchor':'first observer confirmation of freshly durably recorded ingress; exact earlier host arrival unavailable',
            'first_provider_created_at':first['ts'],'first_durable_batch_completed_at':state.get('last_batch_completed_at'),
            'first_signature':first['signature']}
        with closing(connection(L/'state/control.db'))as c,c:
            c.execute('INSERT INTO session_schedule VALUES(1,?)',(json.dumps(value),))
            c.execute('INSERT INTO events(ts,kind,payload)VALUES(?,?,?)',(start,'fresh_ingress_confirmed',json.dumps(value)))
        save(L/'SCHEDULE.json',value)
        return value
