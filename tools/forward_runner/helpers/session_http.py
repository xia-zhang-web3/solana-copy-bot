"""Read-only explicit HTTP attempts with durable cost and cached successful responses."""
import http.client
from contextlib import closing
import json
from pathlib import Path
import ssl
import threading
import time
import tomllib
from urllib.parse import urlsplit,urlencode
from session_common import T,R,L,connection,now,digest,save,stopped
from session_budget import Budget,CU

Q=R/'audit/2026-09-14/candidate-route-economics-20260914T065601Z'
class Broker:
    def __init__(self,budget=None):
        self.budget=budget or Budget();self.jup_lock=threading.Lock();self.next_jup=0
        self.rpc_lock=threading.Lock();self.next_rpc=0
        p=read_policy();self.rpc_url=Path(p['alchemy_private_url_path']).read_text().strip()
        cfg=tomllib.loads(Path(p['intended_toml_path']).read_text())['execution']
        self.jup_url=cfg['quote_canary_base_url'].strip().rstrip('/')
        if not self.jup_url.endswith('/quote'):self.jup_url+='/quote'
        self.jup_key=cfg.get('quote_canary_api_key','')
        assert urlsplit(self.rpc_url).hostname=='solana-mainnet.g.alchemy.com'
        assert urlsplit(self.jup_url).hostname=='api.jup.ag'
        self.path=L/'state/http.db'
        with closing(connection(self.path))as c,c:
            c.execute('CREATE TABLE IF NOT EXISTS attempts(id INTEGER PRIMARY KEY,cache_key TEXT,started_at TEXT,received_at TEXT,provider TEXT,method TEXT,request_json TEXT,status TEXT,http_status INTEGER,response_path TEXT,response_sha256 TEXT,elapsed_ms REAL,bytes INTEGER)')
            c.execute('CREATE TABLE IF NOT EXISTS cache_exclusions(attempt_id INTEGER PRIMARY KEY,reason TEXT,excluded_at TEXT)')
    def exclude_cache(self,ids,reason):
        with closing(connection(self.path))as c,c:
            c.executemany('INSERT OR IGNORE INTO cache_exclusions VALUES(?,?,?)',[(i,reason,now())for i in ids])
    def once(self,provider,method,params,cache_key):
        with closing(connection(self.path))as c:
            return self._once(c,provider,method,params,cache_key)
    def _once(self,c,provider,method,params,cache_key):
        if stopped():return {'status':'session_stopping'}
        if provider=='alchemy'and method not in CU:raise ValueError('rpc_not_allowed')
        if provider=='jupiter'and method!='quote':raise ValueError('quote_only')
        old=c.execute("SELECT * FROM attempts WHERE cache_key=? AND status='ok' AND id NOT IN(SELECT attempt_id FROM cache_exclusions) ORDER BY id DESC LIMIT 1",(cache_key,)).fetchone()
        if old:
            old=dict(old)
            if old['provider']!=provider or old['method']!=method or json.loads(old['request_json'])!=params:raise ValueError('cache_request_binding_mismatch')
            raw=Path(old['response_path']).read_bytes();assert digest(raw)==old['response_sha256'];c.close()
            return {**old,'body':json.loads(raw),'cached':True}
        lock=self.jup_lock if provider=='jupiter'else self.rpc_lock
        with lock:
            attr='next_jup'if provider=='jupiter'else'next_rpc';delay=max(0,getattr(self,attr)-time.monotonic())
            if delay:time.sleep(delay)
            setattr(self,attr,time.monotonic()+(2.1 if provider=='jupiter'else .1))
            with c:
                cur=c.execute('INSERT INTO attempts(cache_key,started_at,provider,method,request_json,status)VALUES(?,?,?,?,?,?)',
                    (cache_key,now(),provider,method,json.dumps(params),'reserved'));seq=cur.lastrowid
            self.budget.reserve('jupiter'if provider=='jupiter'else method,1,f'http:{seq}',{'provider':provider,'method':method})
            # Jupiter requests remain serialized through response; RPC lock only rate-limits starts.
            if provider=='jupiter':result=self.transport(provider,method,params,seq)
        if provider=='alchemy':result=self.transport(provider,method,params,seq)
        raw=result.pop('raw');dest=T/'private/http'/f'{seq:08}.json';dest.parent.mkdir(exist_ok=True,mode=0o700)
        with dest.open('xb')as f:f.write(raw);f.flush();__import__('os').fsync(f.fileno())
        result.update(response_path=str(dest),response_sha256=digest(raw),bytes=len(raw))
        save(dest.with_suffix('.meta.json'),{k:v for k,v in result.items()if k!='body'})
        with c:
            c.execute('UPDATE attempts SET received_at=?,status=?,http_status=?,response_path=?,response_sha256=?,elapsed_ms=?,bytes=?WHERE id=?',
                (result['received_at'],result['status'],result['http_status'],str(dest),digest(raw),result['elapsed_ms'],len(raw),seq))
        a=dict(c.execute('SELECT * FROM attempts WHERE id=?',(seq,)).fetchone());c.close()
        return {**a,'body':result.get('body'),'retry_after':result.get('retry_after'),'cached':False}
    def transport(self,provider,method,params,seq):
        u=urlsplit(self.jup_url if provider=='jupiter'else self.rpc_url)
        headers={'Accept':'application/json','Accept-Encoding':'identity'};data=None
        if provider=='jupiter':
            path=u.path+'?'+urlencode(params);verb='GET';timeout=1.5
            if self.jup_key:headers['x-api-key']=self.jup_key
        else:
            path=u.path;verb='POST';timeout=30;headers['Content-Type']='application/json'
            data=json.dumps({'jsonrpc':'2.0','id':seq,'method':method,'params':params}).encode()
        conn=http.client.HTTPSConnection(u.hostname,u.port or 443,timeout=timeout,context=ssl.create_default_context());start=time.monotonic()
        cancel=threading.Event()
        def watch():
            while not cancel.wait(.05):
                if stopped()or time.monotonic()-start>=timeout:
                    try:
                        if conn.sock:conn.sock.shutdown(2)
                    except OSError:pass
                    conn.close();return
        watcher=threading.Thread(target=watch,daemon=True);watcher.start()
        status='transport_error';http_status=None;raw=b'';body=None;retry_after=None
        try:
            conn.request(verb,path,body=data,headers=headers);resp=conn.getresponse();http_status=resp.status
            retry_after=resp.getheader('Retry-After');raw=resp.read()
            if http_status in (401,403):status='auth_failure'
            elif http_status==429:status='rate_limit'
            elif http_status>=500:status='server_error'
            elif 300<=http_status<400:status='redirect_refused'
            else:
                body=json.loads(raw)
                status=classify_body(provider,http_status,body,seq,params)
        except (TimeoutError,__import__('socket').timeout):status='timeout'
        except json.JSONDecodeError:status='malformed_response'
        except (OSError,http.client.HTTPException):status='transport_error'
        finally:cancel.set();conn.close()
        return {'status':status,'http_status':http_status,'raw':raw,'body':body,'received_at':now(),
            'elapsed_ms':(time.monotonic()-start)*1000,'retry_after':retry_after}

def read_policy():
    p=json.loads((Q/'private/BINDING_CANDIDATE.json').read_text())
    for key in ('alchemy_private_url_path','intended_toml_path'):
        if not Path(p[key]).is_absolute():p[key]=str(R/p[key])
    return p

def classify_body(provider,http_status,body,seq,params):
    if not isinstance(body,dict):return 'malformed_response'
    if provider=='alchemy':
        if body.get('jsonrpc')!='2.0'or body.get('id')!=seq:return 'rpc_envelope_error'
        if body.get('error'):
            error=body['error'];code=error.get('code')if isinstance(error,dict)else None
            return 'rate_limit'if code==429 else'server_error'if code in(-32005,-32603)else'rpc_error'
        if 'result'not in body:return 'rpc_envelope_error'
        if body['result']is None:return 'not_available_yet'
    if http_status!=200:return body.get('errorCode','provider_rejection')
    if provider=='jupiter':
        from quote_validation import validate_quote,InvalidQuote
        if body.get('errorCode'):return body['errorCode']
        try:validate_quote(body,params)
        except (InvalidQuote,KeyError,TypeError,ValueError):return 'invalid_quote'
    return 'ok'
