"""One owner-approved read-only stream. Preparation never calls this paid path."""
import argparse
import sqlite3
import time
from probe_common import *
from probe_docker import create, init_volume, stop
from probe_preflight import preflight
from probe_observation import summarize


def _financial_counts():
    path = ROOT / 'state/live_runtime.db'
    counts = {'orders': None, 'positions': None, 'execution_canary_receipt_facts': None}
    if not path.exists():
        return dict(status='NOT_CREATED', **counts)
    with sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=2) as database:
        database.execute('PRAGMA query_only=ON')
        tables = {r[0] for r in database.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        for name in counts:
            if name in tables:
                counts[name] = database.execute('SELECT count(*) FROM ' + name).fetchone()[0]
    return dict(status='AVAILABLE' if all(v is not None for v in counts.values()) else 'PARTIAL_SCHEMA', **counts)


def financial_counts():
    last = None
    for attempt in range(3):
        try:
            return _financial_counts()
        except (sqlite3.Error, OSError) as error:
            last = dict(status='UNKNOWN', error_type=type(error).__name__,
                        sqlite_errorname=getattr(error, 'sqlite_errorname', None))
            if attempt < 2:
                time.sleep(.1 * (attempt + 1))
    history = read(ROOT / 'evidence/SQLITE_READER_ERRORS.json', [])
    save(ROOT / 'evidence/SQLITE_READER_ERRORS.json', (history + [last])[-8:])
    return dict(last, orders=None, positions=None, execution_canary_receipt_facts=None)


def observation(cid):
    text = docker(['logs', '--tail', '2000', cid], include_stderr=True)
    (ROOT / 'evidence/app-tail.log').write_text(text)
    (ROOT / 'evidence/app-tail.log').chmod(0o600)
    return summarize(text)


def seal():
    value = read(ROOT / 'READY_FOR_OWNER_READ_ONLY_PROBE.json')
    if not value or value.get('status') != 'INDEPENDENT_STOPPED_READ_ONLY_ACCEPTED' or value['run_id'] != RUN:
        raise ValueError('probe_independent_seal_pending')
    for relative, expected in value['files_sha256'].items():
        if digest(ROOT / relative) != expected:
            raise ValueError('probe_seal_file_changed:' + relative)


def ledger(backend):
    prior = read(ROOT / 'CARRYOVER.json')
    if any(type(backend.get(key)) is not int for key in ['received_bytes', 'connection_headroom_bytes', 'connect_attempts']):
        raise ValueError('probe_metrics_unknown')
    observed = backend['received_bytes']
    headroom = backend['connection_headroom_bytes']
    attempts = backend['connect_attempts']
    if observed < 0 or headroom < 0 or observed + headroom > CAP or attempts > 3:
        raise ValueError('probe_budget_boundary')
    cumulative_bytes = prior['stream_accounted_bytes'] + observed + headroom
    cumulative_usd_nano = prior['http_usd_nano'] + (cumulative_bytes * 100_000_000 + 1024**3 - 1) // 1024**3
    if cumulative_usd_nano > 50_000_000_000:
        raise ValueError('probe_cumulative_50usd_boundary')
    value = dict(prior, run_id=RUN, new_observed_bytes=observed, new_headroom_bytes=headroom,
        new_accounted_bytes=observed + headroom, new_upstream_attempts=attempts,
        cumulative_stream_bytes=cumulative_bytes, cumulative_model_usd_nano=cumulative_usd_nano,
        probe_model_usd=(observed + headroom) * 0.10 / 1024**3,
        http_rpc_requests=0, additional_rpc_cu=0, signatures=0, submissions=0)
    save(ROOT / 'evidence/LEDGER.json', value)
    return value


def unresolved_ledger(backend):
    prior = read(ROOT / 'CARRYOVER.json')
    cumulative_bytes = prior['stream_accounted_bytes'] + CAP
    cumulative_usd_nano = prior['http_usd_nano'] + (cumulative_bytes * 100_000_000 + 1024**3 - 1) // 1024**3
    if cumulative_usd_nano > 50_000_000_000:
        raise ValueError('probe_cumulative_50usd_boundary')
    value = dict(prior, run_id=RUN, stream_usage_status='UNKNOWN', debit_basis='FULL_GRANT_RESERVED',
        new_observed_bytes=(backend or {}).get('received_bytes'),
        new_headroom_bytes=(backend or {}).get('connection_headroom_bytes'),
        new_upstream_attempts=(backend or {}).get('connect_attempts'), new_accounted_bytes=CAP,
        cumulative_stream_bytes=cumulative_bytes, cumulative_model_usd_nano=cumulative_usd_nano,
        probe_model_usd=0.4, actual_probe_cost_confirmed=False,
        http_rpc_requests=0, additional_rpc_cu=0, signatures=0, submissions=0)
    save(ROOT / 'evidence/LEDGER.json', value)
    return value


def run(owner_authorized):
    if not owner_authorized:
        raise ValueError('separate_owner_read_only_permission_required')
    preflight();seal()
    cids = read(ROOT / 'CONTAINERS.json') or create()
    init_volume()  # network-none one-off, no upstream connection
    fd = os.open(ROOT / 'control/ATTEMPT.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as output:
        json.dump(dict(run_id=RUN, owner_approved_read_only=True, at_unix=time.time()), output)
    started = time.monotonic()
    save(ROOT / 'control/LEASE.json', dict(generation=1, granted_bytes=CAP,
        expires_unix=time.time() + SECONDS + 60))
    (ROOT / 'control/STREAM_STOP').unlink()
    result = 'UNKNOWN'
    primary_failure = None
    restarts = 0
    try:
        for role in ['stream-backend', 'stream-front']:
            docker(['start', cids[role]])
        until = time.monotonic() + 15
        while time.monotonic() < until:
            statuses = [read(ROOT / 'control' / (role + '-status.json'), {}) for role in ['front', 'backend']]
            if all(value.get('phase') == 'ready' for value in statuses):
                break
            time.sleep(.05)
        else:
            raise ValueError('probe_front_not_ready')
        docker(['start', cids['observation-app']])
        next_progress = 0
        while True:
            if not (ROOT / 'control/STOP').is_file():
                raise ValueError('probe_financial_stop_removed')
            backend = read(ROOT / 'control/backend-status.json', {})
            try:
                budget = ledger(backend)
            except ValueError as error:
                if str(error) != 'probe_metrics_unknown':
                    raise
                budget = None
            clock = read(ROOT / 'control/PROBE_CLOCK.json')
            if clock and time.time() >= clock['deadline_unix']:
                result = 'READ_ONLY_DEADLINE';break
            if (ROOT / 'control/STREAM_STOP').exists():
                result = 'READ_ONLY_RELAY_LIMIT';break
            if not clock and time.monotonic() - started >= 60:
                result = 'NO_STREAM_WITHIN_STARTUP_WINDOW';break
            app = verify_container(cids['observation-app'])
            if not app['State']['Running']:
                latest = observation(cids['observation-app'])
                # One bounded restart preserves the same DB and checkpoint. The
                # backend attempt cap, byte credit and first clock never reset.
                if restarts == 0 and type(backend.get('connect_attempts')) is int and backend['connect_attempts'] < 3:
                    restarts += 1
                    docker(['start', cids['observation-app']]);continue
                result = 'READ_ONLY_APP_EXITED';break
            if time.monotonic() >= next_progress:
                latest = observation(cids['observation-app'])
                save(ROOT / 'evidence/PROGRESS.json', dict(run_id=RUN, phase='READ_ONLY_RUNNING',
                    financial=financial_counts(), backend=backend, observation=latest, ledger=budget))
                if any(isinstance(value, int) and value > 0 for value in financial_counts().values()):
                    raise ValueError('probe_unexpected_financial_row')
                print(json.dumps(dict(phase='READ_ONLY_RUNNING', run_id=RUN,
                    upstream_attempts=backend.get('connect_attempts'),
                    observed_bytes=backend.get('received_bytes'), transport=latest['transport'],
                    last_durable_parent=(latest.get('ingress') or {}).get('last_durably_stored_parent_slot'))), flush=True)
                next_progress = time.monotonic() + 15
            time.sleep(.25)
    except KeyboardInterrupt:
        result = 'READ_ONLY_STOPPED_BY_OWNER'
    except Exception as error:
        primary_failure = dict(type=type(error).__name__, reason=str(error)[:240])
        save(ROOT / 'evidence/FAILURE.json', primary_failure)
        result = 'READ_ONLY_FAILED'
    finally:
        finalize(cids, result, primary_failure, restarts)
    print(json.dumps(dict(run_id=RUN, result=result, evidence=str(ROOT / 'evidence/RESULT.json'))), flush=True)

def finalize(cids, result, primary_failure, restarts):
    from probe_outcome import finalize as record
    return record(cids, result, primary_failure, restarts)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--owner-authorized-read-only', action='store_true')
    args = parser.parse_args()
    run(args.owner_authorized_read_only)


if __name__ == '__main__':
    main()
