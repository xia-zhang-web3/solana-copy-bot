"""Historical whole-wallet raw inventory oracle over parent-slot pages + target block."""
import base64
from virtual_proof import D, Unresolved, need, unwrap


def read_account(item, wallet, mint):
    address=item.get('pubkey',item.get('address'))
    account=item.get('account',item)
    need(isinstance(address,str),'inventory_account_address_missing')
    need(account.get('owner') in (D.TOKEN,D.TOKEN22),'inventory_token_program_unknown')
    data=account.get('data')
    if isinstance(data,dict) and 'parsed' in data:
        info=data['parsed']['info']
        need(info['owner']==wallet and info['mint']==mint,'inventory_identity_mismatch')
        raw=info['tokenAmount']['amount']
    else:
        need(isinstance(data,list) and data[1]=='base64','inventory_encoding_unsupported')
        decoded=base64.b64decode(data[0],validate=True)
        need(len(decoded)>=165 and decoded[108] in (1,2),'inventory_uninitialized_account')
        need(D.b58encode(decoded[:32])==mint and D.b58encode(decoded[32:64])==wallet,'inventory_raw_identity_mismatch')
        raw=str(int.from_bytes(decoded[64:72],'little'))
    need(isinstance(raw,str) and raw.isdigit(),'inventory_amount_invalid')
    return address,int(raw)


def inventory_proof(target, context):
    wallet,mint=target['wallet'],target['mint'];slot=target['slot']
    need(context.get('slot')==slot and context.get('target_signature')==target['signature'],'inventory_context_binding')
    block=context.get('block',{}).get('result',context.get('block',{}))
    need(isinstance(block,dict) and isinstance(block.get('parentSlot'),int),'historical_parent_slot_missing')
    pages=context.get('historical_pages',[])
    need(bool(pages) and context.get('pagination_complete') is True,'inventory_pagination_missing')
    page_keys=context.get('historical_page_keys')
    need(isinstance(page_keys,list) and len(page_keys)==len(pages) and page_keys[0] is None,'inventory_request_page_chain_missing')
    need(len({x for x in page_keys if x is not None})==len(page_keys)-1,'inventory_page_cursor_repeated')
    accounts={}
    for index,page in enumerate(pages):
        result=page.get('result',page)
        need(result.get('context',{}).get('slot')==block['parentSlot'],'inventory_historical_slot_mismatch')
        values=result.get('value');need(isinstance(values,list),'inventory_values_missing')
        for item in values:
            address,amount=read_account(item,wallet,mint)
            need(address not in accounts,'duplicate_inventory_account')
            accounts[address]=amount
        more=result.get('pageKey')
        if index+1<len(pages):need(page_keys[index+1]==more,'inventory_page_request_binding')
        need(bool(more) if index<len(pages)-1 else not more,'inventory_page_chain_incomplete')
    transactions=block.get('transactions');need(isinstance(transactions,list),'complete_block_transactions_missing')
    reached=False;prior=0
    for item in transactions:
        r=dict(item);r['slot']=slot
        tx=r.get('transaction',{})
        signatures=tx.get('signatures',[]);need(bool(signatures),'block_signature_missing')
        if signatures[0]==target['signature']:
            bounds=[]
            keys=tx['message']['accountKeys']
            keys=[x['pubkey'] if isinstance(x,dict) else x for x in keys]
            loaded=r.get('meta',{}).get('loadedAddresses') or {}
            if all(isinstance(x,str) for x in tx['message']['accountKeys']):keys+=loaded.get('writable',[])+loaded.get('readonly',[])
            for bal in r['meta']['preTokenBalances']:
                if bal.get('owner')==wallet and bal['mint']==mint:
                    address=keys[bal['accountIndex']];amount=int(bal['uiTokenAmount']['amount'])
                    need(accounts.get(address,0)==amount,'target_pre_inventory_conflict')
                    bounds.append(address)
            need(target['account'] in bounds or int(target['pre_raw'])==0,'target_account_not_in_inventory')
            target_pre=[b for b in r['meta']['preTokenBalances'] if keys[b['accountIndex']]==target['account']]
            target_post=[b for b in r['meta']['postTokenBalances'] if keys[b['accountIndex']]==target['account']]
            need((int(target_pre[0]['uiTokenAmount']['amount']) if target_pre else 0) == int(target['pre_raw']),'target_pre_response_conflict')
            need((int(target_post[0]['uiTokenAmount']['amount']) if target_post else 0)==int(target['post_raw']),'target_post_response_conflict')
            need(r['meta'].get('err') is None,'target_block_transaction_failed')
            total=sum(accounts.values())+int(target['owner_before_swap_delta_raw'])
            need(total>=int(target['swap_token_raw'])>0,'invalid_total_sell_denominator')
            reached=True
            break
        meta=r.get('meta');need(isinstance(meta,dict),'block_meta_missing')
        message=tx['message'];keys=message['accountKeys'];parsed=bool(keys and isinstance(keys[0],dict))
        keys=[x['pubkey'] if isinstance(x,dict) else x for x in keys]
        loaded=meta.get('loadedAddresses') or {}
        if not parsed:keys+=loaded.get('writable',[])+loaded.get('readonly',[])
        pre={};post={}
        for field,out in [('preTokenBalances',pre),('postTokenBalances',post)]:
            need(isinstance(meta.get(field),list),'block_token_balances_missing')
            for bal in meta[field]:
                if bal['mint']!=mint:continue
                address=keys[bal['accountIndex']]
                if address in accounts:need(bal.get('owner')==wallet,'inventory_owner_change')
                if bal.get('owner')==wallet:
                    need(address not in out,'duplicate_block_token_balance')
                    out[address]=int(bal['uiTokenAmount']['amount'])
        need('err' in meta,'block_transaction_status_missing')
        if meta['err'] is not None:need(pre==post,'failed_block_transaction_changed_tokens')
        for address in set(pre)|set(post):
            need(accounts.get(address,0)==pre.get(address,0),'block_pre_inventory_conflict')
            accounts[address]=post.get(address,0)
        prior+=1
    need(reached,'target_not_in_full_block')
    return {'status':'PROVEN','basis':'wallet_total_raw_before_swap','denominator_raw':str(total),
            'numerator_raw':target['swap_token_raw'],'parent_slot':block['parentSlot'],'slot':slot,
            'historical_accounts':len(accounts),'prior_block_transactions':prior,'target_signature':target['signature']}
