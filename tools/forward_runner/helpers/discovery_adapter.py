"""One rolling Discovery cycle; scheduling, shared provider budget and ingress are external."""
import fcntl
from contextlib import closing
import json
from pathlib import Path
import re
import shutil
import sqlite3
from discovery_io import (check_stop, dump, operator_bindings, parse, raw_identity,
                          run_operator, scrub_config, sha, snapshot, utc)
from discovery_decision import evaluate, policy


class EmptyWindow(Exception):
    """Native quality preparation cannot materialize an empty observed window."""


def run_cycle(context, rpc_endpoint, stop_event):
    """Return a durable prospective decision, RED on recoverable cycle failures.

    Required context: runtime_db, config_path, evidence_dir, runtime_dir,
    cycle_id, session_started_at. Optional on_process(dict), resource_check().
    Call in a worker thread/process, never on the ingress/exit-consumer loop.
    Root must publish into its ledger atomically at actual publication time;
    recorded availability here is the adapter's handoff time, never backdating.
    """
    runtime = Path(context['runtime_dir']).resolve()
    source = Path(context['runtime_db'])
    evidence = Path(context['evidence_dir']).resolve()
    cycle_id = str(context['cycle_id'])
    if not re.fullmatch(r'[A-Za-z0-9_.-]{1,100}', cycle_id):
        raise ValueError('discovery_cycle_id_invalid')
    if source.is_symlink() or not source.resolve().is_relative_to(runtime):
        raise ValueError('discovery_live_db_outside_task')
    stage = evidence/'discovery'/cycle_id
    stage.mkdir(parents=True, exist_ok=False, mode=0o700)
    work_base = runtime/'discovery-work'
    work_base.mkdir(parents=True, exist_ok=True, mode=0o700)
    work = work_base/cycle_id
    decision = dict(decision_id=cycle_id, id=cycle_id, started_at=utc(), green=False,
                    admissible=False, wallets=[], candidate_wallets=[], reasons=[],
                    build='NONE', publication_committed=False, execution_enabled=False)
    lock = (runtime/'discovery.lock').open('a+')
    locked = False
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        locked = True
        check_stop(stop_event)
        if context.get('resource_check'):
            context['resource_check']()
        # Only this session's obsolete scratch paths; never touch historical evidence.
        for old in work_base.iterdir():
            if old.is_dir() and not old.is_symlink():
                shutil.rmtree(old)
            else:
                raise ValueError('discovery_unknown_scratch_entry')
        work.mkdir(mode=0o700)
        bound = operator_bindings()
        dump(stage/'OPERATOR_BINDINGS.json', bound)
        decision['config_sha256'] = sha(context['config_path'])
        db = work/'snapshot.sqlite'
        before = snapshot(source, db, stop_event)
        # Fresh origin is bound by root preflight; reject clearly imported old rows.
        if before['earliest_ts'] and parse(before['earliest_ts']) < parse(context['session_started_at']):
            decision['source_before_ingress_note'] = 'first event source timestamp may precede transport start; root origin identity is authoritative'
        dump(stage/'INPUT_SNAPSHOT.json', before)
        online, offline = work/'prepare.toml', work/'offline.toml'
        dump(stage/'CONFIG_BINDINGS.json', dict(
            prepare=scrub_config(context['config_path'], online, rpc_endpoint),
            offline=scrub_config(context['config_path'], offline)))
        callback = context.get('on_process')
        prepared = run_operator(bound['discovery_v2_prepare_quality']['path'],
                                ['--commit', '--materialize-status'], online, db,
                                stage/'prepare', stop_event, rpc_endpoint, callback)
        if prepared['exit_code'] != 0:
            raise RuntimeError('discovery_prepare_failed')
        report = prepared['report']
        materialized = report.get('materialized_status')
        if not materialized:
            if report.get('rows_scanned') == 0 and report.get('committed') is False:
                decision.update(warmup=True, reasons=report.get('blockers', []) or ['observed_window_empty'],
                                snapshot=before, input_cutoff_at=before['input_cutoff_at'],
                                input_max_rowid=before['max_rowid'], status_now=report.get('now'),
                                window_start=report.get('window_start'), window_minutes=report.get('window_minutes'))
                raise EmptyWindow()
            raise RuntimeError('discovery_materialization_missing')
        policy(materialized)
        with closing(sqlite3.connect(db)) as conn:
            native = json.loads(conn.execute('SELECT status_json FROM discovery_v2_status_snapshot WHERE id=1').fetchone()[0])
        dump(stage/'MATERIALIZED_STATUS.json', native)
        status = run_operator(bound['discovery_v2_status']['path'], [], offline, db,
                              stage/'status', stop_event, callback=callback)
        if status['exit_code'] != 0:
            raise RuntimeError('discovery_status_failed')
        policy(status['report'])
        published = run_operator(bound['discovery_v2_publish']['path'],
                                 ['--dry-run', '--materialized-status'], offline, db,
                                 stage/'publish', stop_event, callback=callback)
        if published['exit_code'] != 0:
            raise RuntimeError('discovery_publish_dry_run_failed')
        with closing(sqlite3.connect(db)) as conn:
            after = raw_identity(conn)
        if any(before[k] != after[k] for k in after):
            raise ValueError('discovery_scratch_raw_or_publication_surface_changed')
        dump(stage/'POST_OPERATOR_IDENTITY.json', after)
        completed_at = utc()
        decision.update(evaluate(published['report'], materialized, before, completed_at))
        decision['calculation_completed_at'] = completed_at
        decision['candidate_sources'] = published['report']['status'].get('candidate_wallet_sources', [])
        if {row['wallet_id'] for row in decision['candidate_sources']} != set(decision['candidate_wallets']):
            raise ValueError('discovery_candidate_sources_incomplete')
        metrics = native['wallet_metrics']
        candidates = set(decision['candidate_wallets'])
        candidate_metrics = [m for m in metrics if m['wallet_id'] in candidates]
        if {m['wallet_id'] for m in candidate_metrics} != candidates:
            raise ValueError('discovery_candidate_metric_missing')
        dump(stage/'WALLET_DECISIONS.json', dict(candidates=candidate_metrics,
             retained_wallet_metrics=metrics, native_filters=native['filters'],
             wallet_metrics_total=native['wallet_metrics_total'],
             wallet_metrics_returned=native['wallet_metrics_returned'],
             wallet_metrics_truncated=native['wallet_metrics_truncated'],
             limitation='native retained rows plus aggregate rejection counts; omitted individual wallets not invented'))
        decision.update(snapshot=before, native_result_path=str(stage/'publish/stdout.json'),
                        wallet_decisions_path=str(stage/'WALLET_DECISIONS.json'),
                        materialized_status_path=str(stage/'MATERIALIZED_STATUS.json'))
    except EmptyWindow:
        pass
    except Exception as exc:
        decision.update(green=False, admissible=False, wallets=[],
                        reasons=sorted(set(decision.get('reasons', [])+[str(exc) if isinstance(exc, (ValueError, RuntimeError, InterruptedError)) else type(exc).__name__])),
                        failure_type=type(exc).__name__)
        # No endpoint/body/error repr is emitted; detailed operator stderr stays task-private.
    finally:
        if locked and work.exists():
            shutil.rmtree(work)
        if locked:
            fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close()
    available_at = utc()
    # Time spent persisting/cleaning up cannot extend the native freshness window.
    if decision.get('admissible') and parse(available_at) > parse(decision['valid_until']):
        decision.update(green=False, admissible=False, wallets=[], reasons=['handoff_after_native_expiry'])
    decision.update(decision_available_at=available_at, available_at=available_at,
                    calculation_completed_at=decision.get('calculation_completed_at', available_at),
                    expires_at=decision.get('expires_at', available_at),
                    valid_until=decision.get('valid_until', available_at),
                    snapshot_cleaned=not work.exists(), completed_at=utc())
    dump(stage/'DECISION.json', decision)
    decision['decision_path'] = str(stage/'DECISION.json')
    return decision
