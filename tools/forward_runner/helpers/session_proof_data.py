"""Read-only evidence collection for primary wallet fraction and source-account continuity."""
import json
from session_common import digest

TRANSIENT={'timeout','rate_limit','server_error','transport_error','not_available_yet'}
class Retry(Exception):pass
class Unresolved(Exception):pass

def rpc(broker,method,params,key):
    bound=key+':'+digest(json.dumps(params,sort_keys=True).encode())
    a=broker.once('alchemy',method,params,bound)
    body=a.get('body');error=body.get('error')if isinstance(body,dict)else None
    if(method=='getBlock'and a['status']=='rpc_error'and a.get('http_status')==200
            and isinstance(error,dict)and error.get('code')==-32004):
        raise Retry('getBlock_block_not_available_-32004')
    if a['status']in TRANSIENT:raise Retry(a['status'])
    if a['status']!='ok':raise Unresolved(a['status'])
    return a['body'],{k:a[k]for k in ('id','started_at','received_at','response_path','response_sha256','elapsed_ms')}

def transaction(broker,signature):
    return rpc(broker,'getTransaction',[signature,{'encoding':'json','commitment':'finalized','maxSupportedTransactionVersion':0}],'tx:'+signature)

def block(broker,slot,detail):
    return rpc(broker,'getBlock',[slot,{'encoding':'json','commitment':'finalized','maxSupportedTransactionVersion':0,'transactionDetails':detail,'rewards':False}],f'block:{slot}:{detail}')

def inventory(broker,p):
    raw,ev=block(broker,p['target_slot'],'full');b=raw['result'];parent=b['parentSlot']
    pages=[];keys=[];evidence=[ev];key=None;seen=set()
    while True:
        cfg={'slot':parent,'pageLimit':1000}
        if key:cfg['pageKey']=key
        page,e=rpc(broker,'getTokenAccountsByOwnerAtSlot',[p['wallet'],{'mint':p['mint']},cfg],f"inventory:{p['wallet']}:{p['mint']}:{parent}")
        pages.append(page);keys.append(key);evidence.append(e);key=page['result'].get('pageKey')
        if not key:break
        if key in seen:raise Unresolved('inventory_pagination_cycle')
        seen.add(key)
    return {'slot':p['target_slot'],'target_signature':p['target_signature'],'historical_pages':pages,
        'historical_page_keys':keys,'pagination_complete':True,'block':raw,'evidence':evidence}

def history(broker,p):
    key=None;seen=set();signatures={};evidence=[]
    while True:
        cfg={'limit':1000,'until':p['anchor_signature'],'commitment':'finalized'}
        if key:cfg['before']=key
        raw,ev=rpc(broker,'getSignaturesForAddress',[p['account'],cfg],f"history:{p['account']}:{p['anchor_signature']}:{p['target_signature']}")
        evidence.append(ev);rows=raw['result']
        if not isinstance(rows,list):raise Unresolved('history_malformed')
        for row in rows:signatures[row['signature']]=row['slot']
        if len(rows)<1000:break
        key=rows[-1]['signature']
        if key in seen:raise Unresolved('history_pagination_cycle')
        seen.add(key)
    if p['target_signature']not in signatures:
        broker.exclude_cache([e['id']for e in evidence],'history_target_not_indexed')
        raise Retry('history_target_not_indexed')
    signatures[p['anchor_signature']]=p['anchor_slot']
    signatures={s:slot for s,slot in signatures.items()if p['anchor_slot']<=slot<=p['target_slot']}
    orders={}
    for slot in set(signatures.values()):
        if list(signatures.values()).count(slot)>1:
            raw,ev=block(broker,slot,'signatures');evidence.append(ev);orders[str(slot)]=raw['result']['signatures']
    def order(item):
        s,slot=item
        if str(slot)in orders:
            try:index=orders[str(slot)].index(s)
            except ValueError:raise Unresolved('history_signature_absent_from_block')
        else:index=0
        return(slot,index)
    ordered=[s for s,_ in sorted(signatures.items(),key=order)]
    try:ordered=ordered[ordered.index(p['anchor_signature']):ordered.index(p['target_signature'])+1]
    except ValueError:raise Unresolved('history_anchor_target_missing')
    bodies=[]
    for s in ordered:
        raw,ev=transaction(broker,s);bodies.append(raw);evidence.append(ev)
    return {**p,'complete':True,'pagination_complete':True,'transactions':bodies,
        'ordered_signatures':ordered,'block_orders':orders,'evidence':evidence}
