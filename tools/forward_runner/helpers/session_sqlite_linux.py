"""Allowlisted Linux-side reads and closed exports of app-owned WAL databases."""
from contextlib import closing
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import signal
import tempfile
import time


STATE = Path('/state')
EXPORTS = Path('/exports')
NAMES = frozenset(('live_runtime.db', 'discovery_recent_raw.db', 'capture.db', 'virtual.db'))


def database(name):
    if not isinstance(name, str) or name not in NAMES:
        raise ValueError('database_name_not_allowlisted')
    path = STATE / name
    if STATE.is_symlink() or path.is_symlink() or not path.is_file():
        raise ValueError('database_missing_or_symlink')
    return path


def connect(name):
    conn = sqlite3.connect(database(name).as_uri() + '?mode=ro',
                           uri=True, timeout=1)
    try:
        conn.execute('PRAGMA query_only=ON')
        return conn
    except Exception:
        conn.close()
        raise


def observe(cursor):
    if type(cursor) is not int or not 0 <= cursor <= 9223372036854775807:
        raise ValueError('cursor_must_be_nonnegative_sqlite_integer')
    source = STATE / 'discovery_recent_raw.db'
    if not source.exists() and not source.is_symlink():
        return {'rows': [], 'state': {}, 'phase': 'awaiting_raw'}
    with closing(connect('discovery_recent_raw.db')) as conn:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if not tables.intersection({'observed_swaps', 'recent_raw_journal_state'}):
            return {'rows': [], 'state': {}, 'phase': 'awaiting_raw_schema'}
        conn.row_factory = sqlite3.Row
        rows = [dict(row) for row in conn.execute(
            'SELECT rowid AS cursor,* FROM observed_swaps '
            'WHERE rowid>? ORDER BY rowid LIMIT 512', (cursor,))]
        state = conn.execute(
            'SELECT * FROM recent_raw_journal_state WHERE id=1').fetchone()
        return {'rows': rows, 'state': dict(state) if state else {}}


def inspect(name, full):
    if type(full) is not bool:
        raise ValueError('full_must_be_boolean')
    path = database(name)
    before = path.stat()
    with closing(connect(name)) as conn:
        # Keep exactly session_identity.schema_hash's tuple order/JSON defaults.
        schema = conn.execute(
            "SELECT type,name,sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name").fetchall()
        tables = [row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        checks = conn.execute('PRAGMA quick_check').fetchall() if full else None
        after = database(name).stat()
        if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
            raise ValueError('database_identity_changed_during_inspect')
        return {'schema_sha256': hashlib.sha256(
                    json.dumps(schema).encode()).hexdigest(),
                'tables': tables, 'quick_check': checks,
                'stat': {'device': before.st_dev, 'inode': before.st_ino}}


def reserve_export(value):
    if not isinstance(value, str) or not value.startswith('/'):
        raise ValueError('export_path_must_be_absolute')
    if any(part in ('.', '..') for part in value.split('/')):
        raise ValueError('export_path_traversal')
    path = Path(value)
    try:
        relative = path.relative_to(EXPORTS)
    except ValueError:
        raise ValueError('export_path_outside_allowlisted_root') from None
    if not relative.parts:
        raise ValueError('export_requires_filename')
    parent = EXPORTS
    if parent.is_symlink() or not parent.is_dir():
        raise ValueError('export_root_missing_or_symlink')
    for part in relative.parts[:-1]:
        parent = parent / part
        if parent.is_symlink() or not parent.is_dir():
            raise ValueError('export_parent_missing_or_symlink')
    if path.is_symlink():
        raise ValueError('export_target_symlink')
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                         getattr(os, 'O_NOFOLLOW', 0), 0o600)
    try:
        identity = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    return path, (identity.st_dev, identity.st_ino)


def check_export_identity(path, expected):
    actual = path.lstat()
    if path.is_symlink() or (actual.st_dev, actual.st_ino) != expected:
        raise ValueError('export_target_identity_changed')


def backup(name, target):
    """Pin a WAL read snapshot, copy locally, then hand off closed file bytes."""
    deadline = time.monotonic() + 35
    def check_deadline(*_):
        if time.monotonic() >= deadline:
            raise TimeoutError('sqlite_export_deadline')
    def alarm(*_):
        raise TimeoutError('sqlite_export_deadline')
    old_handler = signal.signal(signal.SIGALRM, alarm)
    old_timer = signal.setitimer(signal.ITIMER_REAL, 35)
    started = time.monotonic()
    path = identity = None
    success = False
    try:
        # A stable WAL reader permits app commits and prevents endless backup
        # restarts when the source changes between sqlite3_backup_step calls.
        with closing(connect(name)) as source:
            source.execute('BEGIN')
            source.execute('SELECT rootpage FROM sqlite_master LIMIT 1').fetchone()
            snapshot_at = dt.datetime.now(dt.timezone.utc).isoformat()
            scratch = STATE / '.snapshot-scratch'
            if scratch.is_symlink():
                raise ValueError('snapshot_scratch_symlink')
            scratch.mkdir(mode=0o700, exist_ok=True)
            if not scratch.is_dir():
                raise ValueError('snapshot_scratch_not_directory')
            with tempfile.TemporaryDirectory(prefix='export-', dir=scratch) as temp:
                local = Path(temp) / 'snapshot.sqlite'
                with closing(sqlite3.connect(local)) as destination:
                    destination.execute('PRAGMA synchronous=FULL')
                    source.backup(destination, pages=1024, sleep=0.005,
                                  progress=check_deadline)
                    destination.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchall()
                    mode = destination.execute('PRAGMA journal_mode=DELETE').fetchone()[0]
                    if mode.lower() != 'delete':
                        raise ValueError('export_did_not_leave_wal_mode')
                    destination.commit()
                source.rollback()
                local_seconds = time.monotonic() - started
                if any(Path(str(local)+suffix).exists() for suffix in ('-wal','-shm')):
                    raise ValueError('closed_local_export_has_sidecar')
                check_deadline()
                path, identity = reserve_export(target)
                with local.open('rb') as src, path.open('r+b') as out:
                    check_export_identity(path, identity)
                    while chunk := src.read(1024*1024):
                        check_deadline()
                        out.write(chunk)
                    out.flush()
                    os.fsync(out.fileno())
                check_export_identity(path, identity)
                check_deadline()
                if path.stat().st_size != local.stat().st_size:
                    raise ValueError('export_byte_count_mismatch')
        if any(Path(str(path)+suffix).exists() for suffix in ('-wal','-shm')):
            raise ValueError('closed_export_has_sqlite_sidecar')
        success = True
        return {'target': str(path), 'source_name': name,
                'bytes': path.stat().st_size, 'journal_mode': 'delete',
                'snapshot_read_started_at': snapshot_at,
                'local_backup_seconds': local_seconds,
                'elapsed_seconds': time.monotonic()-started,
                'method': 'linux_pinned_wal_snapshot_then_closed_byte_export',
                'completed_at': dt.datetime.now(dt.timezone.utc).isoformat()}
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old_handler)
        if old_timer[0] or old_timer[1]:
            signal.setitimer(signal.ITIMER_REAL, *old_timer)
        if path is not None and not success:
            check_export_identity(path, identity)
            path.unlink()


def dispatch(request):
    if not isinstance(request, dict):
        raise ValueError('request_must_be_object')
    op = request.get('op')
    allowed = {'observe': {'op', 'cursor'},
               'inspect': {'op', 'name', 'full'},
               'backup': {'op', 'name', 'target'}}
    if op not in allowed or set(request) != allowed[op]:
        raise ValueError('operation_or_request_fields_invalid')
    if op == 'observe':
        return observe(request['cursor'])
    if op == 'inspect':
        return inspect(request['name'], request['full'])
    return backup(request['name'], request['target'])


def envelope(request):
    try:
        return {'ok': True, 'result': dispatch(request)}
    except Exception as exc:
        return {'ok': False, 'error_type': type(exc).__name__, 'error': str(exc),
                'sqlite_errorcode': getattr(exc, 'sqlite_errorcode', None),
                'sqlite_errorname': getattr(exc, 'sqlite_errorname', None)}


def main():
    os.umask(0o077)
    try:
        if len(sys.argv) != 2:
            raise ValueError('expected_one_json_argument')
        result = envelope(json.loads(sys.argv[1]))
    except Exception as exc:
        result = {'ok': False, 'error_type': type(exc).__name__, 'error': str(exc),
                  'sqlite_errorcode': None, 'sqlite_errorname': None}
    print(json.dumps(result, separators=(',', ':')))


if __name__ == '__main__':
    main()
