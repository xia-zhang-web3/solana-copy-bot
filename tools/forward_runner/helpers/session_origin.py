"""Reconstruct a fresh schema without copying historical business rows or DB files."""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
from session_common import R, L, T, save, CTX

A = R/'audit/2026-09-13/discovery-v2-input-acquisition-restart-evidence-20260913T194130Z'
SEED = A/'private/poststop-work/live_runtime.db'


def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as f:
        while chunk := f.read(1024*1024):
            digest.update(chunk)
    return digest.hexdigest()


def fingerprints(path):
    result = []
    for suffix in ('', '-wal', '-shm'):
        p = Path(str(path)+suffix)
        if p.exists():
            result.append(dict(path=str(p), bytes=p.stat().st_size, sha256=file_hash(p)))
    return result


def quoted(name):
    return '"'+name.replace('"', '""')+'"'


def schema_identity(db):
    rows = db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name").fetchall()
    return hashlib.sha256(json.dumps(rows, separators=(',', ':')).encode()).hexdigest()


def create_origin(source=SEED, target=None, migration_names=None):
    """immutable is allowed only on this explicitly stopped, zero-WAL seed."""
    source = Path(source)
    target = Path(target) if target else L/'preparation/live_runtime.db'
    wal = Path(str(source)+'-wal')
    if source.is_symlink() or not source.is_file() or (wal.exists() and wal.stat().st_size):
        raise ValueError('schema_seed_not_static_checkpointed_database')
    if target.exists():
        raise ValueError('new_origin_already_exists_use_recovery_without_reinitialization')
    before = fingerprints(source)
    with closing(sqlite3.connect(source.as_uri()+'?mode=ro&immutable=1', uri=True)) as src:
        src.execute('PRAGMA query_only=ON')
        schema = src.execute("SELECT type,name,sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY CASE type WHEN 'table' THEN 0 WHEN 'index' THEN 1 WHEN 'view' THEN 2 ELSE 3 END,name").fetchall()
        migrations = src.execute('SELECT * FROM schema_migrations ORDER BY version').fetchall()
        migration_columns = [r[1] for r in src.execute('PRAGMA table_info(schema_migrations)')]
        source_identity = schema_identity(src)
    if migration_names is not None and {row[0] for row in migrations} != set(migration_names):
        raise ValueError('schema_migration_identity_differs_from_accepted_artifact')
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with closing(sqlite3.connect(target)) as dst:
        target.chmod(0o600)
        dst.execute('PRAGMA foreign_keys=OFF')
        with dst:
            for _, _, sql in schema:
                dst.execute(sql)
            columns = ','.join(quoted(c) for c in migration_columns)
            placeholders = ','.join('?' for _ in migration_columns)
            dst.executemany('INSERT INTO schema_migrations('+columns+') VALUES('+placeholders+')', migrations)
        tables = [row[1] for row in schema if row[0] == 'table']
        counts = {name: dst.execute('SELECT count(*) FROM '+quoted(name)).fetchone()[0] for name in tables}
        if any(n for name, n in counts.items() if name != 'schema_migrations'):
            raise ValueError('origin_business_rows_not_empty')
        if schema_identity(dst) != source_identity:
            raise ValueError('origin_schema_identity_changed')
        if dst.execute('PRAGMA integrity_check').fetchall() != [('ok',)] or dst.execute('PRAGMA foreign_key_check').fetchall():
            raise ValueError('origin_schema_integrity_failed')
        dst.execute('PRAGMA journal_mode=WAL')
    after = fingerprints(source)
    if after != before:
        raise ValueError('historical_schema_seed_modified')
    return dict(origin=str(target), source_schema=str(source), schema_sha256=source_identity,
        origin_sha256=file_hash(target), source_files=before, source_files_unchanged=True,
        seed_read_mode='immutable only for stopped checkpointed seed with zero/absent WAL',
        copied_objects='schema SQL and migration metadata only', migration_count=len(migrations),
        migration_records_sha256=hashlib.sha256(json.dumps(migrations).encode()).hexdigest(),
        counts=counts, all_business_tables_empty=True, historical_business_rows_transferred=0)


def prepare_origin():
    names = {p.name for p in (Path(CTX['source'])/'migrations').glob('*.sql')}
    if not names:
        raise ValueError('task_source_migrations_missing')
    result = create_origin(migration_names=names)
    save(T/'ORIGIN_READY.json', result)
    return result
