"""Read-only final data extraction; never opens a mutating VirtualLedger/Store."""
from collections import Counter, defaultdict
from contextlib import closing
from decimal import Decimal
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import sqlite3
from virtual_store import SCHEMA, decoded
from virtual_report import summary
from session_budget import LIMIT

TABLES = ('decisions', 'events', 'lots', 'jobs', 'proofs', 'allocations', 'metadata')


def read_export(path):
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError('virtual_ledger_missing')
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True)) as db:
        db.execute('PRAGMA query_only=ON'); db.row_factory = sqlite3.Row
        if db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
            raise ValueError('virtual_ledger_integrity_failed')
        return {name: [dict(row) for row in db.execute('SELECT * FROM '+name)] for name in TABLES}


def ordered(export):
    result = {}
    for name in TABLES:
        rows = export.get(name, [])
        if not isinstance(rows, list):
            raise ValueError('virtual_export_rows_invalid')
        keys = {'decisions': ('available', 'id'), 'proofs': ('event_id', 'kind'),
                'allocations': ('event_id', 'lot_id'), 'metadata': ('key',)}.get(name, ('id',))
        result[name] = sorted(rows, key=lambda row: tuple(row[k] for k in keys))
    return result


class ExportView:
    """Disposable in-memory read facade solely for the existing pure report math."""
    def __init__(self, export):
        self.db = sqlite3.connect(':memory:'); self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
        for name in TABLES:
            columns = [r[1] for r in self.db.execute('PRAGMA table_info('+name+')')]
            for row in export[name]:
                present = [c for c in columns if c in row]
                self.db.execute('INSERT INTO '+name+'('+','.join(present)+') VALUES('+','.join('?' for _ in present)+')', tuple(row[c] for c in present))
        self.db.commit(); self.db.execute('PRAGMA query_only=ON')
    def meta(self, key, default=None):
        row = self.db.execute('SELECT value FROM metadata WHERE key=?', (key,)).fetchone()
        return decoded(row[0]) if row else default


def analyze(export):
    export = ordered(export)
    view = ExportView(export)
    try:
        result = summary(view)
    finally:
        view.db.close()
    lots = export['lots']; events = export['events']; jobs = export['jobs']
    metadata = {row['key']: decoded(row['value']) for row in export['metadata']}
    risky = [lot for lot in lots if lot['risk'] or lot['quote_status'] != 'OK' or lot['proof_status'] != 'PROVEN']
    open_ids = {lot['id'] for lot in result['open_lots']}
    unvalued = [dict(lot, unvalued_reason=(lot['risk'] or 'exit_not_observed_or_not_quoted') if lot['id'] in open_ids else lot['risk'] or 'entry_or_provenance_unresolved') for lot in lots if lot['id'] in open_ids or lot in risky]
    successful_sell_ids = {event['id'] for event in events if event['status'] == 'SELL_QUOTED'}
    allocations = [dict(row, economically_applied=row['event_id'] in successful_sell_ids) for row in export['allocations']]
    all_mints = defaultdict(lambda: dict(signals=0, wallets=set(), virtual_lots=0, open_lots=0, unvalued_lots=0))
    for event in events:
        all_mints[event['mint']]['signals'] += 1; all_mints[event['mint']]['wallets'].add(event['wallet'])
    unvalued_ids = {r['id'] for r in unvalued}
    costs_by_mint = defaultdict(Fraction)
    for lot in lots:
        costs_by_mint[lot['mint']] += Fraction(lot['remaining_cost'])
        value = all_mints[lot['mint']]; value['virtual_lots'] += 1
        value['open_lots'] += lot['id'] in open_ids
        value['unvalued_lots'] += lot['id'] in unvalued_ids
    concentration = [dict(mint=mint, **{k: sorted(v) if isinstance(v, set) else v for k, v in values.items()},
        remaining_cost_lamports=str(costs_by_mint[mint])) for mint, values in sorted(all_mints.items())]
    result.update(observer_non_sol_pair_count=metadata.get('non_sol_pair_count', 0),
        admissions_stopped_reason=metadata.get('admissions_stopped'),
        unvalued_lots=unvalued, unvalued_lot_count=len(unvalued),
        complete_closed_lot_count=sum(l['quote_status'] == 'OK' and l['remaining_raw'] == '0' for l in lots),
        conditional_allocations=allocations, concentration_all_signals=concentration,
        event_reasons=dict(sorted(Counter(e.get('reason') or '(none)' for e in events).items())),
        proof_statuses=dict(sorted(Counter(p['status'] for p in export['proofs']).items())),
        quote_failure_jobs=[j for j in jobs if j['kind'] in ('BUY_QUOTE', 'SELL_QUOTE') and decoded(j.get('outcome')).get('status') != 'OK'],
        economic_verdict='UNDETERMINED_EXECUTION_PROFITABILITY', observations_sufficient_for_profitability=False,
        final_independent_data_review='pending owner return; no automatic LLM review',
        overall_positive_result_claimed=False, omissions_from_economic_result=0)
    return export, result


def http_manifest(path, evidence_root):
    path, evidence_root = Path(path), Path(evidence_root).resolve()
    if not path.is_file():
        return dict(available=False, attempts=[], limitation='http_db_missing')
    rows = []
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True)) as db:
        db.execute('PRAGMA query_only=ON'); db.row_factory = sqlite3.Row
        for row in db.execute('SELECT * FROM attempts ORDER BY id'):
            value = dict(row)
            entry = {k: value.get(k) for k in ('id','cache_key','started_at','received_at','provider','method','status','http_status','response_path','response_sha256','elapsed_ms','bytes')}
            request = value.get('request_json')
            entry['request_params_sha256'] = hashlib.sha256((request or '').encode()).hexdigest()
            p = Path(value['response_path']) if value.get('response_path') else None
            bound = bool(p and p.is_absolute() and not p.is_symlink() and p.resolve().is_relative_to(evidence_root/'private/http'))
            entry['path_bound'] = bound; entry['body_present'] = bool(bound and p.is_file())
            entry['hash_matches'] = False
            if entry['body_present']:
                digest = hashlib.sha256()
                with p.open('rb') as f:
                    while chunk := f.read(1024*1024): digest.update(chunk)
                entry['hash_matches'] = digest.hexdigest() == value['response_sha256']
            rows.append(entry)
    complete = all(row['body_present'] and row['hash_matches'] for row in rows)
    return dict(available=True, attempts=rows, attempt_count=len(rows), body_bytes=sum(r.get('bytes') or 0 for r in rows),
                all_recorded_bodies_verified=complete, status_counts=dict(sorted(Counter(r['status'] for r in rows).items())),
                response_path_binding='task/private/http only', endpoints_or_headers_exported=False)


def readonly_budget(path):
    path = Path(path)
    if not path.is_file(): return None
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row; db.execute('PRAGMA query_only=ON')
        rows = [dict(row) for row in db.execute('SELECT kind,count(*) attempts,sum(units) units,sum(nano_usd) nano_usd FROM charges GROUP BY kind ORDER BY kind')]
    used = sum(r['nano_usd'] for r in rows)
    return dict(budget_usd=str(Decimal(LIMIT)/1000000000), reserved_upper_usd=str(Decimal(used)/1000000000),
                remaining_usd=str(Decimal(LIMIT-used)/1000000000), by_kind=rows, billing_statement=False)
