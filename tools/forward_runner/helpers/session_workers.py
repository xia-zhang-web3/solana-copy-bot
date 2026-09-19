"""Observer, Discovery and quote/proof workers run independently of stream transport."""
import datetime as dt
import json
import sqlite3
import threading
import time
from session_common import L,T,R,now,epoch,read,save,event,stop_request
from session_budget import Exhausted
from session_proof_data import transaction,history,inventory,Retry,Unresolved,TRANSIENT
from virtual_proof import WSOL
from session_schedule import schedule as get_schedule,establish

def bootstrap_raw_once():
    """Raw ingress establishes Discovery schedule only; never virtual positions."""
    if get_schedule():return
    from session_sqlite import observe
    observed=observe(0)
    fresh=[r for r in observed['rows'] if abs(time.time()-epoch(r['ts']))<60]
    if fresh:establish(fresh[0],observed['state'])

def observe_loop(runtime,stop):
    while not stop.is_set():
        bootstrap_raw_once()
        result=runtime.observe_once()
        save(L/'OBSERVER.json',{'updated_at':now(),**result,'queue_is_independent_of_network':True})
        if result['status'] in ('IDLE','WAITING_RECEIVED'):stop.wait(.25)

def discovery_loop(runtime,endpoint,stop,on_process=None):
    from discovery_adapter import run_cycle
    while not stop.is_set():
        effective=runtime.poll()
        if effective.get("decision"):
            save(L/"MEMBERSHIP.json",{"decision":effective["decision"],"effective":effective})
        schedule=get_schedule()
        if not schedule:stop.wait(.5);continue
        state=read(L/'DISCOVERY_SCHEDULER.json',{'next_due':epoch(schedule['started_at']),'cycles':0})
        if time.time()<state['next_due']:stop.wait(min(1,state['next_due']-time.time()));continue
        n=state['cycles']+1;cycle=f'cycle-{n:04}-{time.time_ns()}'
        save(L/'DISCOVERY_SCHEDULER.json',{**state,'phase':'calculating','cycle_id':cycle,'started_at':now()})
        decision=run_cycle({'runtime_db':str(L/'state/live_runtime.db'),'config_path':str(L/'configs/live.disabled.toml'),
            'evidence_dir':str(T),'runtime_dir':str(L),'cycle_id':cycle,'session_started_at':schedule['started_at'],'on_process':on_process},endpoint,stop)
        if stop.is_set():return
        effective=runtime.publish(decision);save(L/'MEMBERSHIP.json',{'decision':decision,'effective':effective})
        due=max(state['next_due']+600,epoch(schedule['started_at'])+(int((time.time()-epoch(schedule['started_at']))//600)+1)*600)
        save(L/'DISCOVERY_SCHEDULER.json',{'next_due':due,'cycles':n,'last_cycle_id':cycle,'last_completed_at':now(),'phase':'waiting'})

def job_worker(ledger,broker,stop,quotes=False):
    kinds=('SELL_QUOTE','BUY_QUOTE')if quotes else('SELL_PROOF','INVENTORY_PROOF','HISTORY_PROOF','BUY_PROOF')
    while not stop.is_set():
        job=next((j for k in kinds if(j:=ledger.claim_job(k))),None)
        if not job:stop.wait(.1);continue
        try:
            p=job['payload'];kind=job['kind']
            if kind.endswith('_QUOTE'):
                params={k:p[k]for k in('inputMint','outputMint','amount','slippageBps')}
                params.update(swapMode='ExactIn',instructionVersion='V2')
                a=broker.once('jupiter','quote',params,'virtual-quote:'+str(job['id']))
                if a['status']in TRANSIENT:raise Retry(a['status'])
                ledger.apply_quote(job['id'],{'status':'OK'if a['status']=='ok'else a['status'],'quote':a.get('body'),
                    'request_started_at':a.get('started_at'),'response_received_at':a.get('received_at'),
                    'evidence':{k:v for k,v in a.items()if k!='body'}})
            elif kind in('BUY_PROOF','SELL_PROOF'):
                raw,evidence=transaction(broker,p['signature']);ledger.apply_transaction(job['id'],raw,evidence)
            elif kind=='HISTORY_PROOF':ledger.apply_history(job['id'],history(broker,p))
            elif kind=='INVENTORY_PROOF':ledger.apply_inventory(job['id'],inventory(broker,p))
            else:raise ValueError('unknown_job_kind')
        except Retry as e:
            retries=job.get('attempts',0)+1;ledger.retry_job(job['id'],{'status':'retry','reason':str(e)},time.time()+min(60,2**min(retries,6)))
        except Unresolved as e:ledger.apply_failure(job['id'],str(e))
        except Exhausted:
            ledger.apply_failure(job['id'],'budget_exhausted');stop_request('budget_exhausted');stop.set()
        except Exception as e:
            event('job_helper_failure',job_id=job['id'],kind=job['kind'],error_type=type(e).__name__)
            ledger.apply_failure(job['id'],'helper_'+type(e).__name__)
        ledger.progress_sells()

def start_workers(ledger,broker,endpoint,stop,on_process=None,*,remote_jobs=False):
    targets=[('observer',lambda:observe_loop(ledger,stop)),('discovery',lambda:discovery_loop(ledger,endpoint,stop,on_process)),
        ]
    if not remote_jobs:targets += [('quotes',lambda:job_worker(ledger.ledger,broker,stop,True)),('proof-1',lambda:job_worker(ledger.ledger,broker,stop)),('proof-2',lambda:job_worker(ledger.ledger,broker,stop))]
    workers={}
    for name,fn in targets:
        def guarded(task=fn,label=name):
            while not stop.is_set():
                try:task();return
                except (sqlite3.DatabaseError,ValueError)as e:
                    event('worker_integrity_stop',worker=label,error_type=type(e).__name__);stop_request('integrity_'+label);stop.set();return
                except Exception as e:
                    event('worker_recoverable_failure',worker=label,error_type=type(e).__name__);stop.wait(2)
        thread=threading.Thread(target=guarded,daemon=True,name=name);thread.start();workers[name]=thread
    return workers
