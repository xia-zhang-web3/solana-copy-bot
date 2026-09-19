"""Owned snapshot/operator I/O. Live SQLite backup runs in Linux; host reads only closed snapshots."""
import copy
from contextlib import closing
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import sqlite3
import subprocess
import time
import tomllib
from urllib.parse import urlsplit

from session_common import R
BOOT = R/'audit/2026-09-13/discovery-v2-bootstrap-evidence-20260913T174537Z/build/RESULT.json'
POLICY_SHA = 'aa1af1d75b219f60b2c11396327a478a4cc0b9013586fac29b4dfe4c894d085a'
NAMES = ('discovery_v2_prepare_quality', 'discovery_v2_status', 'discovery_v2_publish')


def utc():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def parse(value):
    return dt.datetime.fromisoformat(value.replace('Z', '+00:00'))


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        while chunk := stream.read(1024*1024):
            digest.update(chunk)
    return digest.hexdigest()


def dump(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temp = path.with_suffix(path.suffix+'.tmp')
    with temp.open('w') as out:
        os.chmod(temp, 0o600)
        json.dump(value, out, indent=2, ensure_ascii=False)
        out.write('\n')
        out.flush()
        os.fsync(out.fileno())
    os.replace(temp, path)


def check_stop(stop_event):
    if stop_event.is_set():
        raise InterruptedError('discovery_stop_requested')


def operator_bindings():
    rows = json.loads(BOOT.read_text())['binaries']
    bound = {r['name']: r for r in rows if r['name'] in NAMES}
    if set(bound) != set(NAMES):
        raise ValueError('discovery_operator_binding_missing')
    for row in bound.values():
        if sha(row['path']) != row['sha256']:
            raise ValueError('discovery_operator_artifact_hash_mismatch')
    return bound


def snapshot(source, target, stop_event):
    """Each backup step releases source read lock; no explicit long transaction."""
    check_stop(stop_event)
    source, target = Path(source), Path(target)
    # Live source exists only in the Linux volume; backup validates its owned path.
    if source.is_symlink() or target.exists():
        raise ValueError('discovery_snapshot_path_invalid')
    started = utc()
    from session_sqlite import backup
    backup(source, target)
    finished = utc()
    check_stop(stop_event)
    with closing(sqlite3.connect(target)) as db:
        if db.execute('PRAGMA quick_check').fetchall() != [('ok',)]:
            raise ValueError('discovery_snapshot_integrity_failed')
        summary = raw_identity(db)
        tables = {x[0] for x in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        cleared = 0
        if 'discovery_v2_status_snapshot' in tables:
            cleared = db.execute('SELECT count(*) FROM discovery_v2_status_snapshot').fetchone()[0]
            # Cached decisions in the scratch snapshot must never skip this cycle's work.
            db.execute('DELETE FROM discovery_v2_status_snapshot')
        db.commit()
    return dict(started_at=started, completed_at=finished, input_cutoff_at=finished,
                cutoff_semantics='online backup completion; max_rowid/raw tail identify included set',
                method='linux_sqlite_online_backup_closed_delete_journal', cached_snapshot_rows_cleared=cleared,
                snapshot_bytes=target.stat().st_size, **summary)


def raw_identity(db):
    raw = db.execute('SELECT count(*),max(rowid),min(ts),max(ts),min(slot),max(slot) FROM observed_swaps').fetchone()
    tail = db.execute('SELECT ts,slot,signature FROM observed_swaps ORDER BY ts DESC,slot DESC,signature DESC LIMIT 1').fetchone()
    tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    surfaces = {}
    for table in ('followlist', 'discovery_strategy_state', 'discovery_runtime_state'):
        if table in tables:
            rows = db.execute('SELECT * FROM '+table+' ORDER BY rowid').fetchall()
            surfaces[table] = hashlib.sha256(json.dumps(rows, default=str).encode()).hexdigest()
    return dict(raw_rows=raw[0], max_rowid=raw[1], earliest_ts=raw[2], latest_ts=raw[3],
                min_slot=raw[4], max_slot=raw[5],
                tail=dict(ts=tail[0], slot=tail[1], signature=tail[2]) if tail else None,
                publication_surface_hashes=surfaces)


def scrub_config(source, target, endpoint=None):
    text = Path(source).read_text()
    original = tomllib.loads(text)
    execution = original['execution']
    if (execution['enabled'] or execution['canary_tiny_submit_enabled'] or
            execution.get('tiny_experiment', {}).get('activate', False)):
        raise ValueError('discovery_config_trading_enabled')
    if original['discovery']['refresh_seconds'] != 600:
        raise ValueError('discovery_cadence_changed')
    if endpoint:
        u = urlsplit(endpoint)
        if u.scheme != 'http' or u.hostname != '127.0.0.1' or not u.port:
            raise ValueError('discovery_broker_not_loopback')
    expected = copy.deepcopy(original)
    section, lines, changed = '', [], []
    for line in text.splitlines():
        header = re.match(r'^\s*\[([^]]+)\]\s*$', line)
        if header:
            section = header[1]
        match = re.match(r'^(\s*)([A-Za-z0-9_]+)\s*=.*$', line)
        if match:
            key = match[2]
            data = original
            dest = expected
            for part in section.split('.'):
                data = data.get(part, {})
                dest = dest.get(part, {})
            is_binding = key.endswith(('_url', '_urls', '_api_key')) or key == 'yellowstone_x_token'
            if key in data and isinstance(data[key], (str, list)) and is_binding:
                value = endpoint if (section, key) == ('discovery', 'helius_http_url') and endpoint else ([] if isinstance(data[key], list) else '')
                line = match[1]+key+' = '+json.dumps(value)
                dest[key] = value
                if value != data[key]:
                    changed.append(section+'.'+key)
        lines.append(line)
    output = '\n'.join(lines)+'\n'
    if tomllib.loads(output) != expected:
        raise ValueError('discovery_policy_scrub_changed_nonbinding')
    urls = re.findall(r'https?://[^\s"\]]+', output)
    if urls != ([endpoint] if endpoint else []):
        raise ValueError('discovery_unexpected_http_fallback')
    Path(target).write_text(output)
    Path(target).chmod(0o600)
    return dict(source_sha256=sha(source), derived_sha256=sha(target),
                changed_binding_fields=changed, policy_operands_preserved=True)


def sandbox(path, work, stage, binary, port=None):
    q = json.dumps
    rules = ['(version 1)', '(allow default)', '(deny network*)', '(deny file-write*)',
             '(allow file-write* (subpath '+q(str(work))+') (subpath '+q(str(stage))+') (literal "/dev/null"))',
             '(deny file-read* (subpath '+q(str(R.parent))+'))',
             '(allow file-read-metadata '+' '.join('(literal '+q(str(p))+')' for p in work.parents)+')',
             '(allow file-read* (subpath '+q(str(work))+') (subpath '+q(str(stage))+') (subpath '+q(str(Path(binary).parent))+'))']
    if port:
        rules.append('(allow network-outbound (remote ip "localhost:'+str(port)+'"))')
    path.write_text('\n'.join(rules)+'\n')
    path.chmod(0o600)


def run_operator(binary, args, config, db, stage, stop_event, endpoint=None, callback=None):
    name = Path(binary).name
    allowed = {'discovery_v2_prepare_quality': ['--commit', '--materialize-status'],
               'discovery_v2_status': [],
               'discovery_v2_publish': ['--dry-run', '--materialized-status']}
    if allowed.get(name) != args:
        raise ValueError('discovery_operator_command_out_of_scope')
    check_stop(stop_event)
    stage.mkdir(parents=True, mode=0o700)
    profile = stage/'operator.sb'
    sandbox(profile, db.parent, stage, binary, urlsplit(endpoint).port if endpoint else None)
    argv = ['/usr/bin/sandbox-exec', '-f', str(profile), str(binary),
            '--config', str(config), '--db-path', str(db)]+args
    env = dict(PATH='/usr/bin:/bin:/usr/sbin:/sbin', LANG='C', TMPDIR=str(db.parent/'tmp'))
    (db.parent/'tmp').mkdir(exist_ok=True)
    row = dict(name=name, started_at=utc(), argv=argv, network='exact_loopback_broker' if endpoint else 'denied')
    process = None
    started = time.monotonic()
    try:
        with (stage/'stdout.json').open('wb') as out, (stage/'stderr.log').open('wb') as err:
            process = subprocess.Popen(argv, cwd=db.parent, env=env, stdout=out, stderr=err, start_new_session=True)
            row['pid'] = process.pid
            dump(stage/'PROCESS.json', row)
            if callback:
                callback(dict(row, action='started'))
            while process.poll() is None:
                if stop_event.wait(0.1):
                    raise InterruptedError('discovery_operator_stopped')
            row['exit_code'] = process.returncode
        if process.returncode != 0:
            return dict(exit_code=process.returncode, report=None)
        return dict(exit_code=0, report=json.loads((stage/'stdout.json').read_text()))
    finally:
        if process and process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=2)
        row.update(finished_at=utc(), elapsed_seconds=time.monotonic()-started,
                   exit_code=process.returncode if process else None)
        dump(stage/'COMMAND.json', row)
        if callback and process:
            callback(dict(row, action='finished'))
