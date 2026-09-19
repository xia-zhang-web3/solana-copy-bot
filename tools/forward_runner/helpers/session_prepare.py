"""Fresh disabled config and matching artifact binding; never starts provider ingress."""
from contextlib import closing
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import tomllib
from urllib.parse import urlsplit
from session_common import R, L, T, RUN, CTX, now, save, read
from session_origin import A, file_hash, fingerprints, prepare_origin
from session_budget import LIMIT

INSTALL = L/'install'
SOURCE_ENV = R/'.local-launch/batch138/state/launch-ingestion-source.env'
CONFIG_SOURCE = A/'private/configs/live.disabled.toml'
CA = R/'audit/2026-09-13/alchemy-ca-preparation-20260913T165256Z/trust/ca-certificates.crt'
CA_SHA = '9dae8d76e55cb08991f2b672d58999ea15560d910759c16b544f843bdffbb994'
G = '/opt/copybot-local'
BUDGET_SOURCE = R/'.local-launch/capture-live-20260919T083509Z/state/budget.db'
BUDGET_RESULT = R/'audit/2026-09-19/capture-live-20260919T083509Z/evidence/SESSION_RESULT.json'


def derived_config(raw):
    original = tomllib.loads(raw)
    # Change exactly two ingestion fields; preserve strategy/economic settings.
    section = re.search(r'(?ms)^\[ingestion\]\s*\n(.*?)(?=^\[|\Z)', raw)
    if not section:
        raise ValueError('ingestion_section_missing')
    body = re.sub(r'(?m)^(capture_scope_db|yellowstone_delivery_mode)\s*=.*\n?', '', section[1])
    body = 'capture_scope_db = "state/capture.db"\nyellowstone_delivery_mode = "legacy"\n' + body
    result = raw[:section.start(1)] + body + raw[section.end(1):]
    value = tomllib.loads(result)
    expected = copy.deepcopy(original)
    expected['ingestion'].update(capture_scope_db='state/capture.db', yellowstone_delivery_mode='legacy')
    if value != expected:
        raise ValueError('unexpected_config_semantic_change')
    ex = value['execution']
    if ex['enabled'] or ex['canary_tiny_submit_enabled'] or ex['tiny_experiment']['activate']:
        raise ValueError('financial_disabled_flags_not_preserved')
    if value['ingestion']['source'] != 'yellowstone_grpc' or value['sqlite']['path'] != 'state/live_runtime.db':
        raise ValueError('intended_ingestion_or_database_path_changed')
    return result, value


def config_binding():
    text, value = derived_config(CONFIG_SOURCE.read_text())
    endpoint = urlsplit(value['ingestion']['yellowstone_grpc_url'])
    if endpoint.scheme != 'https' or endpoint.hostname != 'solana-mainnet.streaming.alchemy.com' or endpoint.port not in (None,443):
        raise ValueError('accepted_stream_binding_changed')
    target = L/'configs/live.disabled.toml'
    if target.exists() and target.read_text() != text:
        raise ValueError('task_config_existing_identity_mismatch')
    target.write_text(text); target.chmod(0o600)
    return dict(source=str(CONFIG_SOURCE), path=str(target), sha256=file_hash(target),
        source_sha256=file_hash(CONFIG_SOURCE), only_changed=['ingestion.capture_scope_db','ingestion.yellowstone_delivery_mode'],
        filters_unchanged=True, disabled=dict(execution=False,canary_tiny_submit_enabled=False,tiny_experiment_activate=False),
        refresh_seconds=value['discovery']['refresh_seconds'], stream_host=endpoint.hostname, stream_port=443,
        source_env_path=str(SOURCE_ENV), source_env_sha256=file_hash(SOURCE_ENV))


def artifact_binding():
    expected = read(T/'MATCHING_ARTIFACT.json')
    if not expected or not re.fullmatch(r'[0-9a-f]{40}', expected.get('git_sha','')) or not re.fullmatch(r'[0-9a-f]{64}', expected.get('binary_sha256','')):
        raise ValueError('explicit_matching_artifact_required_no_legacy_fallback')
    manifest_path = INSTALL/'bin/operator-artifact-current-copybot-app.json'
    manifest = json.loads(manifest_path.read_text()); binary = INSTALL/'bin/copybot-app'
    if manifest['git_sha'] != expected['git_sha'] or manifest['profile'] != 'release' or file_hash(binary) != expected['binary_sha256']:
        raise ValueError('matching_app_artifact_identity_mismatch')
    if manifest['target'] != 'x86_64-unknown-linux-gnu' or manifest['git_dirty']:
        raise ValueError('matching_app_artifact_target_or_dirty')
    if CA.is_symlink() or file_hash(CA) != CA_SHA:
        raise ValueError('accepted_ca_identity_mismatch')
    source = Path(CTX['source'])/'migrations'
    installed = {p.name: file_hash(p) for p in (INSTALL/'migrations').glob('*.sql')}
    wanted = {p.name: file_hash(p) for p in source.glob('*.sql')}
    if not wanted or installed != wanted:
        raise ValueError('matching_migration_set_or_content_mismatch')
    return dict(install=str(INSTALL), artifact_manifest=str(manifest_path), git_sha=expected['git_sha'],
        binary=str(binary), binary_sha256=expected['binary_sha256'], profile='release',
        artifact_target=manifest['target'], migrations=[dict(name=n,sha256=h) for n,h in sorted(installed.items())],
        ca_path=str(CA), ca_sha256=CA_SHA, builds=0,pulls=0,installs=0)


def prepare_budget():
    stopped = read(BUDGET_RESULT)
    if not stopped or stopped.get('stopped',{}).get('stopped') is not True:
        raise ValueError('budget_source_not_recorded_stopped')
    wal = Path(str(BUDGET_SOURCE)+'-wal')
    if BUDGET_SOURCE.is_symlink() or (wal.exists() and wal.stat().st_size):
        raise ValueError('budget_source_not_checkpointed')
    before = fingerprints(BUDGET_SOURCE); target = L/'state/budget.db'
    with closing(sqlite3.connect(BUDGET_SOURCE.as_uri()+'?mode=ro&immutable=1',uri=True)) as src:
        prior = src.execute('SELECT COALESCE(sum(nano_usd),0) FROM charges').fetchone()[0]
        if prior != 7263765750:
            raise ValueError('budget_reserve_not_exact_7263765750')
        source_rows = src.execute('SELECT * FROM charges ORDER BY id').fetchall()
        rows_sha = hashlib.sha256(json.dumps(source_rows,separators=(',',':')).encode()).hexdigest()
        tables = {row[0] for row in src.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        prior_identity = src.execute('SELECT * FROM session_identity').fetchall() if 'session_identity' in tables else []
        existed = target.exists()
        with closing(sqlite3.connect(target)) as dst:
            if existed:
                if dst.execute('SELECT * FROM charges ORDER BY id').fetchall() != source_rows:
                    raise ValueError('prepared_budget_charges_changed_no_overwrite')
            else:
                src.backup(dst)
            # The new run gets its own sentinel; historical source stays immutable.
            dst.execute('DROP TABLE IF EXISTS session_identity');dst.commit()
            copied_sha = hashlib.sha256(json.dumps(dst.execute('SELECT * FROM charges ORDER BY id').fetchall(),separators=(',',':')).encode()).hexdigest()
            if copied_sha != rows_sha:
                raise ValueError('budget_charges_changed_during_identity_reset')
            if dst.execute('SELECT COALESCE(sum(nano_usd),0) FROM charges').fetchone()[0] != prior:
                raise ValueError('budget_copy_reserve_mismatch')
    target.chmod(0o600)
    if fingerprints(BUDGET_SOURCE) != before:
        raise ValueError('budget_source_changed')
    return dict(prior_nano_usd=prior, remaining_nano_usd=LIMIT-prior,
                new_charges=0, source_files=before, target=str(target),
                source_session_identity=prior_identity, charges_rows_sha256=rows_sha,
                copied_charges_sha256=copied_sha, fresh_session_identity_pending=True)


def prepare():
    os.umask(0o077)
    if (L/'IDENTITY.json').exists() or (T/'PREPARE_BINDING.json').exists():
        raise ValueError('already_prepared_session_no_reinitialize')
    artifact = artifact_binding()  # Missing artifact refuses before any filesystem/DB/Docker mutation.
    for p in (L/'state',L/'configs',L/'control',L/'private',L/'logs',L/'preparation',T/'checks',T/'docker-cli',INSTALL/'state',INSTALL/'configs'):
        p.mkdir(parents=True,exist_ok=True,mode=0o700)
    (T/'docker-cli/config.json').write_text('{"auths":{}}\n')
    cfg = config_binding(); budget = prepare_budget(); origin = read(T/'ORIGIN_READY.json')
    if origin is None:
        origin = prepare_origin()
    elif origin.get('origin') != str(L/'preparation/live_runtime.db') or file_hash(origin['origin']) != origin['origin_sha256']:
        raise ValueError('prepared_schema_origin_changed')
    from session_prepare_linux import prepare_linux
    storage = prepare_linux(origin)
    save(L/'control/settings.json',dict(run_id=RUN,host=cfg['stream_host'],port=443,generation=0,
                                      max_connections=4,duration_seconds=28800))
    (L/'control/hosts').write_text('127.0.0.1 localhost '+cfg['stream_host']+'\n::1 localhost\n')
    (L/'control/resolv.conf').write_text('nameserver 127.0.0.1\noptions attempts:1 timeout:1\n')
    result = dict(run_id=RUN,prepared_at=now(),config=cfg,artifact=artifact,origin=origin,
                  storage=storage,budget=budget,provider_connections=0,app_starts=0,build='NONE')
    save(T/'PREPARE_BINDING.json',result)
    return result


initialize = prepare
if __name__ == '__main__':
    print(json.dumps(prepare()))
