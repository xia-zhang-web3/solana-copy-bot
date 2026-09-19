"""Durable shared dollar reservation; grants are money-derived, not an extra byte cap."""
from decimal import Decimal,ROUND_CEILING
from contextlib import closing
import json
from session_common import L,RUN,connection,now,save

LIMIT=100_000_000_000  # nano USD, including historical charges; owner-approved expansion.
STREAM_RATE=Decimal('0.10')/Decimal(2**30)
CU={'getBalance':10,'getTokenAccountsByOwner':10,'getTransaction':40,'getBlock':40,
    'getSignaturesForAddress':40,'getTokenAccountsByOwnerAtSlot':40,'getMultipleAccounts':20,'getSlot':20,'getAccountInfo':10}
class Exhausted(Exception):pass

class Budget:
    def __init__(self,path=None):
        self.path=path or L/'state/budget.db'
        with closing(connection(self.path))as c,c:
            c.execute('CREATE TABLE IF NOT EXISTS charges(id INTEGER PRIMARY KEY,ts TEXT,kind TEXT,units INTEGER,nano_usd INTEGER,identity TEXT UNIQUE,payload TEXT)')
    def reserve(self,kind,units,identity,payload=None):
        identity=RUN+':'+identity
        if units<0:raise ValueError('negative_units')
        if kind=='stream':amount=Decimal(units)*STREAM_RATE
        elif kind=='jupiter':amount=Decimal(0)
        elif kind in CU:amount=Decimal(CU[kind])*Decimal('0.525')/1000000
        else:raise ValueError('method_not_allowed')
        nano=int((amount*1000000000).to_integral_value(rounding=ROUND_CEILING))
        c=connection(self.path)
        try:
            c.execute('BEGIN IMMEDIATE')
            old=c.execute('SELECT * FROM charges WHERE identity=?',(identity,)).fetchone()
            if old:
                if old['kind']!=kind or old['units']!=units:raise ValueError('reservation_identity_collision')
                c.commit();return old['id']
            used=c.execute('SELECT COALESCE(SUM(nano_usd),0)FROM charges').fetchone()[0]
            if used+nano>LIMIT:raise Exhausted('dollar_budget_exhausted')
            cur=c.execute('INSERT INTO charges(ts,kind,units,nano_usd,identity,payload)VALUES(?,?,?,?,?,?)',
                (now(),kind,units,nano,identity,json.dumps(payload or {})))
            c.commit();return cur.lastrowid
        finally:c.close()
    def summary(self):
        c=connection(self.path)
        rows=[dict(x)for x in c.execute('SELECT kind,count(*) attempts,sum(units) units,sum(nano_usd) nano_usd FROM charges GROUP BY kind')];c.close()
        used=sum(x['nano_usd']for x in rows)
        return {'budget_usd':str(Decimal(LIMIT)/1000000000),'reserved_upper_usd':str(Decimal(used)/1000000000),
            'remaining_usd':str(Decimal(LIMIT-used)/1000000000),'by_kind':rows,'billing_statement':False}
    def stream_grant(self,generation,consumed):
        c=connection(self.path)
        granted=c.execute("SELECT COALESCE(SUM(units),0)FROM charges WHERE kind='stream' AND identity LIKE ?",(f'{RUN}:stream:{generation}:%',)).fetchone()[0];c.close()
        if granted-consumed<32*2**20:
            self.reserve('stream',64*2**20,f'stream:{generation}:{granted}')
            granted+=64*2**20
        return granted
