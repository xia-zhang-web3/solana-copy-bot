"""One durable overall deadline, including bootstrap and all reconnect attempts."""
from contextlib import closing
import time
from session_common import L,connection


def anchor(seconds=28800):
    if seconds!=28800:
        raise ValueError('duration_outside_current_eight_hour_proposal')
    with closing(connection(L/'state/control.db')) as db,db:
        db.execute('CREATE TABLE IF NOT EXISTS session_deadline(id INTEGER PRIMARY KEY CHECK(id=1),ends REAL NOT NULL)')
        db.execute('INSERT OR IGNORE INTO session_deadline VALUES(1,?)',(time.time()+seconds,))
        return db.execute('SELECT ends FROM session_deadline WHERE id=1').fetchone()[0]


def deadline():
    with closing(connection(L/'state/control.db')) as db:
        row=db.execute('SELECT ends FROM session_deadline WHERE id=1').fetchone()
        if row is None:raise ValueError('global_deadline_missing')
        return row[0]
