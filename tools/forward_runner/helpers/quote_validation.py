"""Strict quote identity, integer threshold and route-flow validation."""
from collections import defaultdict
from decimal import Decimal, InvalidOperation

WSOL='So11111111111111111111111111111111111111112'

class InvalidQuote(ValueError):pass

def uint(value,positive=False):
    if not isinstance(value,str) or not value.isdigit():raise InvalidQuote('raw_integer_required')
    n=int(value)
    if n>2**64-1 or (positive and n==0):raise InvalidQuote('raw_integer_range')
    return n

def check(condition,reason):
    if not condition:raise InvalidQuote(reason)


def validate_quote(q,params):
    check(isinstance(q,dict),'response_not_object')
    for key in ('inputMint','outputMint','swapMode','slippageBps'):
        check(q.get(key)==params[key],'wrong_'+key)
    check(uint(q.get('inAmount'),True)==int(params['amount']),'wrong_inAmount')
    out=uint(q.get('outAmount'),True);minimum=uint(q.get('otherAmountThreshold'),True)
    bps=params['slippageBps'];check(isinstance(bps,int) and 0<=bps<10000,'invalid_slippage')
    check(minimum<=out,'threshold_above_output')
    # Jupiter ExactIn threshold is floor(out * (10000-bps)/10000), with integer-rounding tolerance1.
    expected=out*(10000-bps)//10000
    check(abs(minimum-expected)<=1,'threshold_slippage_mismatch')
    check(isinstance(q.get('contextSlot'),int) and q['contextSlot']>0,'missing_context_slot')
    try:impact=Decimal(str(q.get('priceImpactPct')))
    except InvalidOperation:raise InvalidQuote('invalid_price_impact')
    check(impact.is_finite(),'nonfinite_price_impact')
    plan=q.get('routePlan');check(isinstance(plan,list) and len(plan)>0,'missing_route')
    flows=defaultdict(int);edges=[];fees=[]
    for i,leg in enumerate(plan):
        check(isinstance(leg,dict),'invalid_route_leg')
        swap=leg.get('swapInfo',{})
        check(isinstance(swap,dict),'invalid_swap_info')
        a,b=swap.get('inputMint'),swap.get('outputMint')
        check(isinstance(a,str) and isinstance(b,str) and a!=b,'bad_route_mints')
        check(isinstance(swap.get('ammKey'),str) and bool(swap.get('label')),'missing_route_identity')
        ina,outa=uint(swap.get('inAmount'),True),uint(swap.get('outAmount'),True)
        weight=leg.get('bps',leg.get('percent'))
        check(isinstance(weight,int) and 0<weight<=(10000 if 'bps'in leg else 100),'invalid_route_weight')
        flows[a]-=ina;flows[b]+=outa;edges.append((a,b))
        if swap.get('feeAmount') is not None:
            fee=uint(swap['feeAmount']);check(isinstance(swap.get('feeMint'),str),'missing_fee_mint')
            fees.append({'route_leg':i,'fee_raw':str(fee),'fee_mint':swap['feeMint'],'label':swap['label']})
    start,finish=params['inputMint'],params['outputMint']
    check(flows[start]==-int(params['amount']),'route_input_sum_mismatch')
    pf=q.get('platformFee');platform_amount=0
    if pf is not None:
        check(isinstance(pf,dict),'invalid_platform_fee')
        platform_amount=uint(pf.get('amount'))
    check(flows[finish]-platform_amount==out,'route_output_sum_mismatch')
    check(all(v==0 for m,v in flows.items() if m not in (start,finish)),'unbalanced_intermediate_route')
    reachable={start}
    for _ in edges:
        reachable.update(b for a,b in edges if a in reachable)
    check(finish in reachable and all(a in reachable for a,b in edges),'disconnected_route')
    return {'input_raw':str(int(params['amount'])),'output_raw':str(out),'minimum_output_raw':str(minimum),
            'threshold_expected_floor_raw':str(expected),'threshold_rounding_delta':minimum-expected,
            'context_slot':q['contextSlot'],'price_impact_api_value':str(impact),
            'route_labels':[x['swapInfo']['label'] for x in plan],'route_legs':len(plan),
            'explicit_route_fees':fees,'fee_amount_coverage':'complete' if len(fees)==len(plan) else 'not_fully_exposed',
            'platform_fee':pf,'fees_already_in_quoted_amounts':True}


def validate_link(parent,params,source_field,parent_sha,actual_sha):
    check(parent_sha==actual_sha,'parent_hash_mismatch')
    check(source_field in ('outAmount','otherAmountThreshold'),'invalid_parent_amount_source')
    check(params['inputMint']==parent['outputMint'] and params['outputMint']==parent['inputMint'],'lost_parent_mint_link')
    check(params['amount']==parent[source_field],'lost_parent_amount_link')
    return True
