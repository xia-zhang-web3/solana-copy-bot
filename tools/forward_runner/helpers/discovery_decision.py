"""Prospective eligibility from unchanged native dry-run plus current wall time."""
import datetime as dt
import hashlib
from discovery_io import POLICY_SHA, parse


def policy(report):
    fingerprint = report['policy_fingerprint']
    if hashlib.sha256(fingerprint.encode()).hexdigest() != POLICY_SHA:
        raise ValueError('discovery_policy_fingerprint_changed')
    return fingerprint


def evaluate(published, materialized, snapshot, available_at):
    if published.get('dry_run') is not True or published.get('committed') is not False:
        raise ValueError('discovery_publication_not_dry_run')
    fingerprint = policy(published)
    status = published['status']
    if policy(status) != fingerprint or policy(materialized) != fingerprint:
        raise ValueError('discovery_status_policy_disagrees')
    now = parse(available_at)
    candidates = status['candidate_wallets']
    if len(candidates) != len(set(candidates)):
        raise ValueError('discovery_duplicate_candidates')
    if status['execution_enabled'] is not False or status['execution_disabled'] is not True:
        raise ValueError('discovery_execution_not_disabled')
    reasons = list(status['blockers'])
    cursor = (status.get('tail') or {}).get('cursor')
    captured = snapshot.get('tail')
    if cursor and captured:
        if (cursor['signature'], cursor['slot'], parse(cursor['ts_utc'])) != (captured['signature'], captured['slot'], parse(captured['ts'])):
            raise ValueError('discovery_snapshot_tail_binding_mismatch')
    elif cursor or captured:
        raise ValueError('discovery_snapshot_tail_presence_mismatch')
    max_tail_lag = status['max_tail_lag_seconds']
    if max_tail_lag != 600 or status['window_minutes'] != 120:
        raise ValueError('discovery_window_or_freshness_policy_changed')
    tail_age = (now-parse(cursor['ts_utc'])).total_seconds() if cursor else None
    tail_fresh = tail_age is not None and 0 <= tail_age <= max_tail_lag
    if not tail_fresh:
        reasons.append('actual_decision_time_tail_stale_or_missing')
    status_age = (now-parse(status['now'])).total_seconds()
    max_age = materialized['max_status_age_seconds']
    if max_age != 5400:
        raise ValueError('discovery_materialized_age_policy_changed')
    if status_age < 0 or status_age > max_age:
        reasons.append('actual_decision_time_materialization_stale_or_future')
    coverage = status.get('coverage_sample')
    if not coverage or not coverage.get('covers_window_start'):
        reasons.append('window_coverage_incomplete')
    if not candidates:
        reasons.append('empty_top')
    elif len(candidates) < 8:
        reasons.append('below_native_publish_floor')
    if not status.get('production_green'):
        reasons.append('native_publication_not_green')
    expires = parse(status['now'])+dt.timedelta(seconds=max_age)
    if cursor:
        expires = min(expires, parse(cursor['ts_utc'])+dt.timedelta(seconds=max_tail_lag))
    return dict(green=not reasons, admissible=not reasons,
                candidate_wallets=candidates, wallets=candidates if not reasons else [],
                decision_available_at=available_at, available_at=available_at,
                expires_at=expires.isoformat(), valid_until=expires.isoformat(),
                reasons=sorted(set(reasons)), policy_fingerprint=fingerprint,
                policy_fingerprint_sha256=POLICY_SHA, status_now=status['now'],
                window_start=status['window_start'], window_minutes=status['window_minutes'],
                input_cutoff_at=snapshot['input_cutoff_at'], input_max_rowid=snapshot['max_rowid'],
                coverage=coverage, freshness=dict(tail_age_seconds=tail_age, tail_fresh=tail_fresh,
                max_tail_lag_seconds=max_tail_lag, status_age_seconds=status_age,
                max_status_age_seconds=max_age), portfolio=status.get('live_portfolio'),
                native_filters=status.get('filters'), native_scan=status.get('scan'),
                native_reason=published.get('reason'), publication_dry_run=True,
                publication_committed=False, execution_enabled=False)


def admission(decision, wallet, detected_at):
    """Detection time is immutable: later refresh must not back-admit an older BUY."""
    if not decision or not decision.get('admissible'):
        return False
    detected = parse(detected_at)
    return (parse(decision['available_at']) <= detected <= parse(decision['valid_until'])
            and wallet in decision['wallets'])
