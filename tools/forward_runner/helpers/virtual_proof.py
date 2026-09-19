"""Pure saved-transaction proof for virtual scenarios; no RPC or runtime writes."""
import hashlib
import importlib.util
import json
from pathlib import Path

# Resolve against audit date, preserving the accepted B helpers without modification.
B = Path(__file__).resolve().parent / 'accepted_proof'
spec = importlib.util.spec_from_file_location('_rolling_accepted_b_decode', B/'helpers/tx_decode.py')
D = importlib.util.module_from_spec(spec)
spec.loader.exec_module(D)
WSOL = D.WSOL

class Unresolved(ValueError):
    pass

def need(value, reason):
    if not value: raise Unresolved(reason)

def unwrap(body):
    result=body.get('result', body)
    need(isinstance(result,dict) and isinstance(result.get('meta'),dict),'missing_transaction_meta')
    return result

def account_bounds(result, account, wallet, mint):
    keys,_=D.instructions(result)
    need(account in keys,'account_not_in_transaction')
    idx=keys.index(account); answer={}
    for side in ('pre','post'):
        rows=[r for r in result['meta'][side+'TokenBalances'] if r['accountIndex']==idx]
        need(len(rows)<=1,'duplicate_account_balance')
        if rows:
            r=rows[0];need(r.get('owner')==wallet and r['mint']==mint,'account_identity_changed')
            answer[side]=int(r['uiTokenAmount']['amount'])
        else:answer[side]=0
    return answer

def transaction_proof(event, body):
    r=unwrap(body);meta=r['meta'];tx=r['transaction'];wallet=event['wallet'];mint=event['mint']
    need(tx['signatures'][0]==event['signature'],'signature_mismatch')
    need(meta.get('err') is None and 'err' in meta,'transaction_unsuccessful')
    need(r.get('slot')==event['slot'],'slot_mismatch')
    keys,flat=D.instructions(r)
    need(wallet in keys[:tx['message']['header']['numRequiredSignatures']],'wallet_not_signer')
    swaps=[x for x in flat if 'swap' in x]
    need(len(swaps)==1,'swap_count_ambiguous')
    swap=swaps[0];n=swap['named']
    need(n['user']==wallet and {n['base_mint'],n['quote_mint']}=={mint,WSOL},'swap_identity_mismatch')
    role='base' if n['base_mint']==mint else 'quote';account=n['user_'+role+'_token_account']
    facts=D.token_facts(meta,flat);fact=facts.get(keys.index(account),{})
    need(fact.get('owner')==wallet and fact.get('mint')==mint,'owner_not_proven')
    transfers=D.transfers(flat,facts)
    own=[x for x in transfers if x['mint']==mint and (x['source']==account or x['destination']==account)]
    leg=[x for x in own if x['parent_swap']==swap['path']]
    need(bool(leg),'swap_transfer_missing')
    for t in leg:
        if t['source']==account:need(t['authority']==wallet,'wrong_outgoing_authority')
        if t['destination']==account:need(t['source']==n['pool_'+role+'_token_account'] and t['authority']==n['pool'],'wrong_pool_inflow')
    delta=sum(t['amount_raw']*((t['destination']==account)-(t['source']==account)) for t in leg)
    need(delta>0 if event['side']=='BUY' else delta<0,'swap_direction_mismatch')
    bounds=account_bounds(r,account,wallet,mint)
    total=sum(t['amount_raw']*((t['destination']==account)-(t['source']==account)) for t in own)
    need(bounds['post']-bounds['pre']==total,'unexplained_account_movements')
    pos={x['path']:i for i,x in enumerate(flat)}
    before=[t for t in own if pos[t['path']]<pos[swap['path']]]
    pre_swap=bounds['pre']+sum(t['amount_raw']*((t['destination']==account)-(t['source']==account)) for t in before)
    external_out=[t for t in own if t['source']==account and t['parent_swap']!=swap['path'] and t['destination']!=account]
    owned_accounts={keys[i] for i,f in facts.items() if f.get('owner')==wallet and f.get('mint')==mint}
    owner_before_delta=sum(t['amount_raw']*((t['destination'] in owned_accounts)-(t['source'] in owned_accounts)) for t in transfers if t['mint']==mint and pos[t['path']]<pos[swap['path']])
    for idx,f in facts.items():
        if f.get('owner')!=wallet or f.get('mint')!=mint:continue
        need(('pre' in f or f.get('initialized')) and ('post' in f or f.get('closed')),'owned_token_balance_missing')
        addr=keys[idx]
        net=sum(t['amount_raw']*((t['destination']==addr)-(t['source']==addr)) for t in transfers if t['mint']==mint)
        need(f.get('post',0)-f.get('pre',0)==net,'other_owner_token_movements_unexplained')
        for ix in flat:
            if ix['program'] in (D.TOKEN,D.TOKEN22) and idx in ix['account_indices']:
                need(ix['data'] and ix['data'][0] in (1,3,9,12,16,17,18,21,22),'unsupported_owner_token_instruction')
    need(event['side']!='SELL' or 0<-delta<=pre_swap,'invalid_sell_fraction')
    need(f'Program {D.PUMP} success' in meta.get('logMessages',[]),'swap_success_missing')
    return {'status':'PROVEN','signature':event['signature'],'slot':event['slot'],'wallet':wallet,'mint':mint,'side':event['side'],
            'account':account,'pre_raw':str(bounds['pre']),'post_raw':str(bounds['post']),'pre_swap_raw':str(pre_swap),
            'swap_token_raw':str(abs(delta)),'decimals':fact['decimals'],'external_outgoing':external_out,
            'owner_before_swap_delta_raw':str(owner_before_delta),'instruction_path':swap['path'],
            'body_sha256':hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',',':')).encode()).hexdigest()}

def history_proof(anchor, target, context, known_sells):
    """Root broker supplies complete address-history pages and ordered saved bodies."""
    account=anchor['account'];wallet=anchor['wallet'];mint=anchor['mint']
    need(target['account']==account,'different_source_account')
    need(context.get('complete') is True and context.get('pagination_complete') is True,'account_history_gap')
    need(context.get('account')==account and context.get('anchor_signature')==anchor['signature'] and context.get('target_signature')==target['signature'],'history_binding_mismatch')
    entries=context.get('transactions',[]);need(bool(entries),'history_empty')
    signatures=[unwrap(x)['transaction']['signatures'][0] for x in entries]
    need(signatures[0]==anchor['signature'] and signatures[-1]==target['signature'],'history_endpoints_missing')
    need(len(signatures)==len(set(signatures)),'duplicate_history_transaction')
    need(context.get('ordered_signatures')==signatures,'history_signature_coverage_mismatch')
    previous=int(anchor['post_raw']);last_slot=anchor['slot'];last_signature=anchor['signature'];incoming=0
    for body in entries[1:]:
        r=unwrap(body);signature=r['transaction']['signatures'][0];slot=r['slot']
        need(slot>=last_slot,'history_order_regression')
        if slot==last_slot:
            order=context.get('block_orders',{}).get(str(slot),[])
            need(last_signature in order and signature in order and order.index(last_signature)<order.index(signature),'same_slot_order_unknown')
        bounds=account_bounds(r,account,wallet,mint)
        need(bounds['pre']==previous,'history_balance_gap')
        need('err' in r['meta'],'history_transaction_status_missing')
        if r['meta']['err'] is not None:
            need(bounds['pre']==bounds['post'],'failed_history_transaction_changed_tokens')
            previous=bounds['post'];last_slot=slot;last_signature=signature
            continue
        keys,flat=D.instructions(r);facts=D.token_facts(r['meta'],flat);transfers=D.transfers(flat,facts)
        own=[t for t in transfers if t['mint']==mint and (t['source']==account or t['destination']==account)]
        delta=sum(t['amount_raw']*((t['destination']==account)-(t['source']==account)) for t in own)
        need(bounds['post']-bounds['pre']==delta,'history_unexplained_movement')
        for t in own:
            if t['source']==account and t['destination']!=account:
                need(t['parent_swap'] is not None,'outgoing_transfer_provenance_unresolved')
                need(signature==target['signature'] or signature in known_sells,'unobserved_leader_sell')
            if t['destination']==account and t['source']!=account and t['parent_swap'] is None:incoming+=t['amount_raw']
        previous=bounds['post'];last_slot=slot;last_signature=signature
    need(previous==int(target['post_raw']),'history_target_balance_mismatch')
    return {'status':'PROVEN','account':account,'anchor_signature':anchor['signature'],'target_signature':target['signature'],
            'checked_transactions':len(entries),'incoming_transfer_raw':str(incoming),'outgoing_transfer_unresolved':False}
