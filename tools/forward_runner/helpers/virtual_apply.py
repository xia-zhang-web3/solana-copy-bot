"""Idempotent application of externally supplied RPC/quote outcomes."""
from fractions import Fraction
from virtual_store import decoded,encoded
from virtual_proof import transaction_proof,history_proof,Unresolved
from virtual_inventory import inventory_proof


def proof_result(fn):
    try:return fn()
    except (ValueError,KeyError,TypeError,IndexError,StopIteration) as exc:
        return {'status':'UNRESOLVED','reason':str(exc)[:240]}

def mark_risk(s,event_id,reason):
    event=s.event(event_id)
    s.db.execute("UPDATE lots SET risk=CASE WHEN risk IN ('','exit_quote_unavailable') THEN ? ELSE risk END WHERE wallet=? AND mint=? AND event_id<? AND (remaining_raw!='0' OR quote_status='PENDING')",(reason,event['wallet'],event['mint'],event_id))
    s.set_event(event_id,'UNRESOLVED',reason)

def progress_sells(s):
    from virtual_sells import progress_sells as progress
    progress(s)

def apply_transaction(s,job_id,body,evidence=None):
    with s.atomic():
        j=s.job(job_id)
        if not j or j['state'] in ('DONE','STOPPED'):return
        event=decoded(s.event(j['event_id'])['payload'])
        result=proof_result(lambda:transaction_proof(event,body));result['evidence']=evidence or {}
        s.finish_job(job_id,result);s.put_proof(j['event_id'],'TRANSACTION',result)
        if j['kind']=='BUY_PROOF':
            s.db.execute("UPDATE lots SET account=?,proof_status=?,risk=CASE WHEN ?='' THEN risk ELSE ? END WHERE event_id=?",(result.get('account'),result['status'],'' if result['status']=='PROVEN' else 'buy_origin_unresolved','' if result['status']=='PROVEN' else 'buy_origin_unresolved',j['event_id']))
        elif j['kind']=='SELL_PROOF' and result['status']!='PROVEN':mark_risk(s,j['event_id'],result.get('reason','sell_proof_unresolved'))
        progress_sells(s);s.exposure()

def apply_inventory(s,job_id,context):
    with s.atomic():
        j=s.job(job_id)
        if not j or j['state'] in ('DONE','STOPPED'):return
        target=s.proof(j['event_id'],'TRANSACTION')
        result=proof_result(lambda:inventory_proof(target,context))
        result['evidence']=context.get('evidence',{})
        s.finish_job(job_id,result);s.put_proof(j['event_id'],'INVENTORY',result)
        progress_sells(s)

def apply_history(s,job_id,context):
    with s.atomic():
        j=s.job(job_id)
        if not j or j['state'] in ('DONE','STOPPED'):return
        target=s.proof(j['event_id'],'TRANSACTION')
        anchor=s.proof(j['payload']['anchor_event_id'],'TRANSACTION')
        known={r[0] for r in s.db.execute("SELECT e.signature FROM events e JOIN proofs p ON p.event_id=e.id AND p.kind='TRANSACTION' AND p.status='PROVEN' WHERE e.wallet=? AND e.mint=? AND e.side='SELL' AND e.id<? AND json_extract(e.payload,'$.capture_seq') IS NOT NULL AND json_extract(e.payload,'$.capture_epoch') IS NOT NULL",(target['wallet'],target['mint'],j['event_id']))}
        result=proof_result(lambda:history_proof(anchor,target,context,known))
        result['evidence']=context.get('evidence',{})
        s.finish_job(job_id,result);s.put_proof(j['event_id'],'HISTORY',result)
        progress_sells(s)

def apply_failure(s,job_id,reason):
    with s.atomic():
        j=s.finish_job(job_id,{'status':'UNRESOLVED','reason':reason})
        if not j:return
        if j['kind']=='BUY_QUOTE':
            s.db.execute("UPDATE lots SET quote_status='UNRESOLVED',risk=? WHERE event_id=?",(reason,j['event_id']))
            s.set_event(j['event_id'],'BUY_QUOTE_UNAVAILABLE',reason)
        elif j['kind']=='BUY_PROOF':
            s.db.execute("UPDATE lots SET proof_status='UNRESOLVED',risk=? WHERE event_id=?",(reason,j['event_id']))
            s.put_proof(j['event_id'],'TRANSACTION',{'status':'UNRESOLVED','reason':reason})
        elif j['kind']=='SELL_QUOTE':mark_risk(s,j['event_id'],'exit_quote_unavailable')
        else:mark_risk(s,j['event_id'],reason)
        progress_sells(s);s.exposure()

def apply_quote(s,job_id,outcome):
    with s.atomic():
        j=s.job(job_id)
        if not j or j['state'] in ('DONE','STOPPED'):return
        q=outcome.get('quote',outcome.get('result',{}));payload=j['payload']
        ok=outcome.get('status')=='OK' and isinstance(q,dict)
        ok=ok and all(q.get(key)==payload[key] for key in ('inputMint','outputMint'))
        ok=ok and str(q.get('inAmount'))==str(payload['amount']) and str(q.get('outAmount','')).isdigit() and int(q['outAmount'])>0
        if not ok:
            s.finish_job(job_id,outcome)
            if j['kind']=='BUY_QUOTE':
                s.db.execute("UPDATE lots SET quote_status='UNRESOLVED',risk='buy_quote_unavailable' WHERE event_id=?",(j['event_id'],))
                s.set_event(j['event_id'],'BUY_QUOTE_UNAVAILABLE',outcome.get('reason','invalid_or_unavailable_quote'))
            else:mark_risk(s,j['event_id'],'exit_quote_unavailable')
            progress_sells(s);s.exposure();return
        amount=int(q['outAmount']);event_id=j['event_id']
        if j['kind']=='BUY_QUOTE':
            s.db.execute("UPDATE lots SET initial_raw=?,remaining_raw=?,remaining_cost='10000000',quote_status='OK' WHERE event_id=?",(str(amount),str(amount),event_id))
            s.set_event(event_id,'BUY_QUOTED')
        elif j['kind']=='SELL_QUOTE':
            allocations=list(s.db.execute('SELECT * FROM allocations WHERE event_id=?',(event_id,)))
            total=sum(int(a['raw']) for a in allocations)
            if total!=int(payload['amount']):raise ValueError('sell_allocation_quote_binding')
            for a in allocations:
                lot=s.db.execute('SELECT * FROM lots WHERE id=?',(a['lot_id'],)).fetchone()
                remaining=int(lot['remaining_raw'])-int(a['raw']);cost=Fraction(lot['remaining_cost'])-Fraction(a['cost'])
                if remaining<0 or cost<0:raise ValueError('virtual_oversell')
                proceeds=Fraction(lot['proceeds'])+Fraction(amount*int(a['raw']),total)
                s.db.execute('UPDATE lots SET remaining_raw=?,remaining_cost=?,proceeds=? WHERE id=?',(str(remaining),str(cost),str(proceeds),lot['id']))
            s.set_event(event_id,'SELL_QUOTED')
        else:raise ValueError('not_a_quote_job')
        s.finish_job(job_id,outcome);progress_sells(s);s.exposure()
