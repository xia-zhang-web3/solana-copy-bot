"""Deterministic complete CSV evidence tables; no row truncation for final artifacts."""
import csv
import datetime as dt
import json
import os
from pathlib import Path
from virtual_store import decoded


def csv_table(path, rows, columns):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with path.open('w', newline='') as out:
        path.chmod(0o600); writer = csv.DictWriter(out, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(row.get(key), sort_keys=True, ensure_ascii=False) if isinstance(row.get(key), (dict,list)) else row.get(key) for key in columns})
        out.flush(); os.fsync(out.fileno())
    return dict(path=str(path), rows=len(rows), columns=columns)


def epoch(value):
    if value is None: return None
    if isinstance(value, (int,float)): return float(value)
    return dt.datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()


def gap(a, b):
    start, finish = epoch(a), epoch(b)
    return None if start is None or finish is None else finish-start


def build_tables(root, export, summary, http, budget):
    root = Path(root); tables = {}
    def write(name, rows, columns):
        tables[name] = csv_table(root/(name+'.csv'), rows, columns)
    events = []; by_event = {row['id']: row for row in export['events']}
    for row in export['events']:
        payload = decoded(row['payload'])
        events.append({**row, 'source_ts':payload.get('source_ts'), 'source_ts_kind':payload.get('source_ts_kind'),
                       'ingress_ts':payload.get('ingress_ts'), 'durable_ts':payload.get('durable_ts'),
                       'durable_batch_upper_bound':payload.get('durable_batch_upper_bound')})
    write('EVENTS', events, ['id','key','signature','wallet','mint','side','detected','decision_id','status','reason','source_ts','source_ts_kind','ingress_ts','durable_ts','durable_batch_upper_bound','payload'])
    cohorts = []
    for row in export['decisions']:
        payload = decoded(row['payload'])
        cohorts.append({**row,'wallets':decoded(row['wallets']), 'input_cutoff_at':payload.get('input_cutoff_at'),
            'status_now':payload.get('status_now'),'window_start':payload.get('window_start'),
            'calculation_completed_at':payload.get('calculation_completed_at'),
            'policy_fingerprint_sha256':payload.get('policy_fingerprint_sha256'),
            'reasons':payload.get('reasons'),'native_result_path':payload.get('native_result_path')})
    write('COHORTS', cohorts, ['id','available','expires','admissible','wallets','input_cutoff_at','status_now','window_start','calculation_completed_at','policy_fingerprint_sha256','reasons','native_result_path','payload'])
    lot_columns = ['id','event_id','wallet','mint','account','initial_raw','remaining_raw','remaining_cost','quote_status','proof_status','risk','proceeds']
    write('LOTS', export['lots'], lot_columns)
    write('OPEN_LOTS', summary['open_lots'], lot_columns)
    write('UNVALUED_LOTS', summary['unvalued_lots'], lot_columns+['unvalued_reason'])
    write('ALLOCATIONS', summary['conditional_allocations'], ['event_id','lot_id','raw','cost','economically_applied'])
    write('MINT_CONCENTRATION', summary['concentration_all_signals'], ['mint','signals','wallets','virtual_lots','open_lots','unvalued_lots','remaining_cost_lamports'])
    quotes = []; latencies = []
    for row in export['jobs']:
        if row['kind'] not in ('BUY_QUOTE','SELL_QUOTE'): continue
        payload = decoded(row['payload']); outcome = decoded(row.get('outcome')); q = outcome.get('quote') or {}
        e = by_event[row['event_id']]; ev = decoded(e['payload']); evidence = outcome.get('evidence') or {}
        timing = dict(job_id=row['id'],event_id=e['id'],side=e['side'],state=row['state'],
            source_ts=ev.get('source_ts'),ingress_ts=ev.get('ingress_ts'),durable_ts=ev.get('durable_ts'),
            durable_batch_upper_bound=ev.get('durable_batch_upper_bound'),observer_detected_at=e['detected'],
            job_created_at=row['created'],job_started_at=row.get('started'),job_completed_at=row.get('completed'),
            request_started_at=outcome.get('request_started_at'),response_received_at=outcome.get('response_received_at'),
            detection_to_job_start_seconds=gap(e['detected'],row.get('started')),
            detection_to_request_seconds=gap(e['detected'],outcome.get('request_started_at')),
            request_duration_seconds=gap(outcome.get('request_started_at'),outcome.get('response_received_at')),
            detection_to_response_seconds=gap(e['detected'],outcome.get('response_received_at')),
            full_leader_to_follower_latency_claimed=False)
        latencies.append(timing)
        routes = q.get('routePlan') or []
        quotes.append(dict(job_id=row['id'],event_id=e['id'],decision_id=e['decision_id'],wallet=e['wallet'],mint=e['mint'],
            side=e['side'],state=row['state'],status=outcome.get('status'),reason=outcome.get('reason'),
            requested_raw=payload.get('amount'),out_raw=q.get('outAmount'),minimum_raw=q.get('otherAmountThreshold'),
            price_impact_pct=q.get('priceImpactPct'),route_labels=[r.get('swapInfo',{}).get('label') for r in routes],
            route_fees=[{k:r.get('swapInfo',{}).get(k) for k in ('feeAmount','feeMint')} for r in routes],
            platform_fee=q.get('platformFee'),raw_response_path=evidence.get('response_path'),raw_response_sha256=evidence.get('response_sha256'),
            fraction_numerator_raw=payload.get('fraction_numerator_raw'),fraction_denominator_raw=payload.get('fraction_denominator_raw'),
            request_started_at=timing['request_started_at'],response_received_at=timing['response_received_at'],
            full_outcome=outcome))
    write('QUOTES',quotes,['job_id','event_id','decision_id','wallet','mint','side','state','status','reason','requested_raw','out_raw','minimum_raw','price_impact_pct','route_labels','route_fees','platform_fee','raw_response_path','raw_response_sha256','fraction_numerator_raw','fraction_denominator_raw','request_started_at','response_received_at','full_outcome'])
    columns = list(latencies[0]) if latencies else ['job_id','event_id','observer_detected_at','request_started_at','response_received_at','detection_to_request_seconds','request_duration_seconds','detection_to_response_seconds','full_leader_to_follower_latency_claimed']
    write('LATENCY',latencies,columns)
    write('PROOFS',export['proofs'],['event_id','kind','status','payload'])
    write('JOBS',export['jobs'],['id','event_id','kind','state','created','started','completed','attempts','next_retry_at','payload','outcome'])
    write('FEE_SCENARIOS',summary['fee_scenarios'],['scenario','per_leg_lamports','closed_allocated_network_fee','closed_quote_pnl_after_network_scenario','all_quoted_leg_network_cost_lamports','priority_already_in_unit','DEX_fees_not_subtracted_again','setup_transactions_unknown'])
    write('PROVIDER_COSTS',(budget or {}).get('by_kind',[]),['kind','attempts','units','nano_usd'])
    write('HTTP_MANIFEST',http.get('attempts',[]),['id','cache_key','started_at','received_at','provider','method','status','http_status','response_path','response_sha256','request_params_sha256','elapsed_ms','bytes','path_bound','body_present','hash_matches'])
    directory = os.open(root, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return tables
