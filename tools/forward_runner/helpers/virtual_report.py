"""Deterministic virtual evidence summaries; open/unresolved risk remains visible."""
from collections import Counter,defaultdict
from fractions import Fraction
from virtual_store import decoded


def amount(value):
    fraction=Fraction(value)
    return {'exact_lamports_fraction':str(fraction),'SOL':float(fraction/1000000000)}

def summary(s):
    events=[dict(x) for x in s.db.execute('SELECT * FROM events ORDER BY id')]
    lots=[dict(x) for x in s.db.execute('SELECT * FROM lots ORDER BY id')]
    jobs=[dict(x) for x in s.db.execute('SELECT * FROM jobs ORDER BY id')]
    sell_ids={e['id'] for e in events if e['status']=='SELL_QUOTED'}
    allocations=[dict(x) for x in s.db.execute('SELECT * FROM allocations ORDER BY event_id,lot_id') if x['event_id'] in sell_ids]
    closed_cost=sum((Fraction(x['cost']) for x in allocations),Fraction(0))
    proceeds=sum((Fraction(l['proceeds']) for l in lots),Fraction(0))
    opened=[l for l in lots if int(l['remaining_raw'])>0 or l['quote_status']=='PENDING']
    by_mint=defaultdict(lambda:{'open_cost_lamports':Fraction(0),'lots':0})
    for l in opened:by_mint[l['mint']]['open_cost_lamports']+=Fraction(l['remaining_cost']);by_mint[l['mint']]['lots']+=1
    for value in by_mint.values():value['open_cost_lamports']=str(value['open_cost_lamports'])
    timing=[]
    event_by_id={e['id']:e for e in events}
    for j in jobs:
        if j['kind'] not in ('BUY_QUOTE','SELL_QUOTE'):continue
        e=event_by_id[j['event_id']];payload=decoded(e['payload']);outcome=decoded(j['outcome'])
        timing.append({'job_id':j['id'],'event_id':e['id'],'side':e['side'],'state':j['state'],
                       'source_ts':payload.get('source_ts'),'ingress_ts':payload.get('ingress_ts'),'durable_ts':payload.get('durable_ts'),
                       'observer_detected_at':e['detected'],'job_created_at':j['created'],'job_started_at':j['started'],'job_completed_at':j['completed'],
                       'request_started_at':outcome.get('request_started_at'),'response_received_at':outcome.get('response_received_at'),
                       'detection_to_job_start_seconds':None if j['started'] is None else j['started']-e['detected'],
                       'full_leader_to_follower_latency_claimed':False})
    fees=[];buy_count=sum(l['quote_status']=='OK' for l in lots);sell_count=len(sell_ids)
    for label,unit in [('base_reference',5000),('historical_priority_reference',27000),('tiny_total_fee_cap_sensitivity',100000)]:
        allocated_buy_fee=closed_cost/Fraction(10000000)*unit
        closed_fee=allocated_buy_fee+sell_count*unit
        fees.append({'scenario':label,'per_leg_lamports':unit,'closed_allocated_network_fee':amount(closed_fee),
                     'closed_quote_pnl_after_network_scenario':amount(proceeds-closed_cost-closed_fee),
                     'all_quoted_leg_network_cost_lamports':(buy_count+sell_count)*unit,
                     'priority_already_in_unit':True,'DEX_fees_not_subtracted_again':True,'setup_transactions_unknown':True})
    return {'experiment_only':True,'execution_fills_created':0,'signals':len(events),'event_statuses':dict(Counter(e['status'] for e in events)),
            'job_states':dict(Counter(j['state'] for j in jobs)),'jobs':len(jobs),'successful_buy_quotes':buy_count,'successful_sell_quotes':sell_count,
            'open_lots':opened,'open_lot_count':len(opened),'unresolved_lots':[l for l in lots if l['risk']],
            'current_open_cost':amount(sum((Fraction(l['remaining_cost']) for l in opened),Fraction(0))),
            'peak_open_cost':amount(s.meta('peak_open_cost_lamports','0')),'peak_open_plus_pending_cost':amount(s.meta('peak_open_plus_pending_lamports','0')),
            'assumed_initial_deposit':None,'closed_quote_proceeds':amount(proceeds),'closed_allocated_entry_cost':amount(closed_cost),
            'closed_quote_difference_before_network':amount(proceeds-closed_cost),'fee_scenarios':fees,
            'rent_capital_references_lamports':{'classic_temporary_WSOL':1488440,'token2022_ATA':1513840,'actual_setup_unknown':True,'deducted_as_irreversible_expense':False},
            'mint_concentration':dict(by_mint),'timings':timing,'cursor':s.meta('cursor'),
            'overall_positive_result_claimed':False,'unclosed_positions_marked_zero':False,'provider_expense_accounted_separately':True,
            'limitations':['quotes are not fills or landing proof','pool impact of our prior virtual trades is not modelled','open/unquoted positions remain unvalued','fee references are scenarios not actual paid fees']}
