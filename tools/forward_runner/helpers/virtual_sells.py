"""Plan exits only after proof, preceding quotes, and exact fractional allocation."""
from fractions import Fraction
from virtual_store import decoded
from virtual_proof import WSOL


def unresolved(s,event,reason):
    from virtual_apply import mark_risk
    mark_risk(s,event['id'],reason)

def progress_sells(s):
    events=list(s.db.execute("SELECT * FROM events WHERE side='SELL' AND status='SELL_PENDING' ORDER BY id"))
    for event in events:
        event=dict(event);event_id=event['id']
        previous=s.db.execute("SELECT count(*) FROM events WHERE wallet=? AND mint=? AND side='SELL' AND id<? AND status IN ('SELL_PENDING','SELL_QUOTE_PENDING')",(event['wallet'],event['mint'],event_id)).fetchone()[0]
        if previous:continue
        lots=list(s.db.execute("SELECT * FROM lots WHERE wallet=? AND mint=? AND event_id<? AND (quote_status='PENDING' OR remaining_raw!='0') ORDER BY event_id",(event['wallet'],event['mint'],event_id)))
        if any(l['quote_status']=='PENDING' or l['proof_status']=='PENDING' for l in lots):continue
        lots=[l for l in lots if l['quote_status']=='OK' and int(l['remaining_raw'])>0]
        if not lots:s.set_event(event_id,'SKIPPED','no_successful_prior_buy_quote');continue
        if any((l['risk'] and l['risk']!='exit_quote_unavailable') or l['proof_status']!='PROVEN' for l in lots):unresolved(s,event,'prior_lot_provenance_or_exit_unresolved');continue
        target=s.proof(event_id,'TRANSACTION')
        if not target:continue
        if target['status']!='PROVEN':unresolved(s,event,'sell_transaction_unresolved');continue
        if target['external_outgoing']:unresolved(s,event,'same_transaction_outgoing_transfer');continue
        if any(l['account']!=target['account'] for l in lots):unresolved(s,event,'multiple_or_different_lot_source_accounts');continue
        anchor_event=lots[0]['event_id'];anchor=s.proof(anchor_event,'TRANSACTION')
        binding={**decoded(event['payload']),'event_id':event_id,'decision_id':event['decision_id'],'detected_at':event['detected']}
        s.put_job(event_id,'HISTORY_PROOF',{**binding,'account':target['account'],'anchor_event_id':anchor_event,'anchor_signature':anchor['signature'],'anchor_slot':anchor['slot'],'target_signature':target['signature'],'target_slot':target['slot']})
        s.put_job(event_id,'INVENTORY_PROOF',{**binding,'target_signature':target['signature'],'target_slot':target['slot']})
        history=s.proof(event_id,'HISTORY');inventory=s.proof(event_id,'INVENTORY')
        if not history or not inventory:continue
        if history['status']!='PROVEN' or inventory['status']!='PROVEN':unresolved(s,event,'history_or_wallet_inventory_unresolved');continue
        numerator=int(inventory['numerator_raw']);denominator=int(inventory['denominator_raw'])
        if numerator<=0 or denominator<numerator:unresolved(s,event,'invalid_fraction');continue
        total=0
        for lot in lots:
            remaining=int(lot['remaining_raw']);quantity=remaining*numerator//denominator
            if quantity==0:continue
            cost=Fraction(lot['remaining_cost'])*quantity/remaining
            s.db.execute('INSERT OR IGNORE INTO allocations VALUES(?,?,?,?)',(event_id,lot['id'],str(quantity),str(cost)))
            total+=quantity
        if total==0:s.set_event(event_id,'SKIPPED','fraction_rounds_to_zero_lots_retained');continue
        s.put_job(event_id,'SELL_QUOTE',{**binding,'inputMint':event['mint'],'outputMint':WSOL,'amount':str(total),'slippageBps':500,'fraction_numerator_raw':str(numerator),'fraction_denominator_raw':str(denominator),'fraction_basis':'wallet_total_raw_before_swap_with_account_history','scenario':'fixed_0.01_SOL_quote_lots'})
        s.set_event(event_id,'SELL_QUOTE_PENDING')
