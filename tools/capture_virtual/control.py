"""Bounded publication requests; only the Rust consumer can acknowledge a scope."""

from contextlib import contextmanager
from datetime import datetime
import json
import math
import os
from pathlib import Path
import sqlite3
import threading

SCHEMA = Path(__file__).resolve().parents[2] / "crates/storage-core/src/capture_scope/schema.sql"
TABLES = {"capture_meta", "capture_requests", "capture_members", "capture_events",
          "capture_obligations", "capture_delivery", "capture_binding"}
BASE58 = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def epoch(value):
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("timestamp_requires_timezone")
        value = parsed.timestamp()
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError("invalid_timestamp")
    return float(value)


def validate_bounds(max_rows, max_bytes):
    for value, limit in ((max_rows, 4_000_000), (max_bytes, 68_719_476_736)):
        if type(value) is not int or not 0 < value <= limit:
            raise ValueError("invalid_capture_bounds")


class CaptureControl:
    def __init__(self, path):
        self.path = Path(path).resolve(strict=True)
        self.lock = threading.RLock()
        self._outcome_unknown = False
        self.db = sqlite3.connect(self.path.as_uri() + "?mode=rw", uri=True,
                                  timeout=2, isolation_level=None, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        try:
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.execute("PRAGMA foreign_keys=ON")
            tables = {r[0] for r in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not TABLES <= tables:
                raise ValueError("capture_schema_missing")
            meta = self.db.execute("SELECT * FROM capture_meta WHERE id=1").fetchone()
            if meta is None:
                raise ValueError("capture_limits_missing")
            validate_bounds(meta["max_rows"], meta["max_bytes"])
        except BaseException:
            self.db.close()
            raise

    @classmethod
    def create(cls, path, *, max_rows, max_bytes):
        """Create exclusively. Never migrate another runtime, capture or ledger DB."""
        validate_bounds(max_rows, max_bytes)
        path = Path(path).absolute()
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
        db = None
        try:
            db = sqlite3.connect(path, isolation_level=None)
            db.executescript(SCHEMA.read_text() + "\n" + SCHEMA.with_name("recovery_schema.sql").read_text())
            db.execute("INSERT INTO capture_meta(id,max_rows,max_bytes) VALUES(1,?,?)",
                       (max_rows, max_bytes))
        except BaseException:
            if db:
                db.close()
            for suffix in ("", "-wal", "-shm"):
                Path(str(path) + suffix).unlink(missing_ok=True)
            raise
        db.close()
        return cls(path)

    def require_committed_read(self):
        if self._outcome_unknown:
            raise RuntimeError("capture_transaction_outcome_unknown_reopen_and_reconcile")

    @contextmanager
    def atomic(self):
        with self.lock:
            self.require_committed_read()
            self.db.execute("BEGIN IMMEDIATE")
            committing = False
            try:
                yield
                committing = True
                self.db.execute("COMMIT")
            except BaseException:
                # Preserve the primary error. A failed reply after COMMIT cannot
                # establish whether it committed; this connection stays fenced.
                if committing and not self.db.in_transaction:
                    self._outcome_unknown = True
                if self.db.in_transaction:
                    try:
                        self.db.execute("ROLLBACK")
                    except BaseException:
                        self._outcome_unknown = True
                raise

    def close(self):
        with self.lock:
            self.db.close()

    def request(self, decision):
        d = dict(decision)
        key = d.get("decision_id", d.get("id"))
        if not isinstance(key, str) or not key or len(key) > 256:
            raise ValueError("invalid_decision_id")
        wallets = d.get("wallets")
        if (type(d.get("admissible")) is not bool or not isinstance(wallets, list)
                or len(wallets) > 128
                or any(not isinstance(w, str) or not 32 <= len(w) <= 44
                       or not set(w) <= BASE58 for w in wallets)
                or len(set(wallets)) != len(wallets)):
            raise ValueError("invalid_capture_members")
        proposed, expires = epoch(d["available_at"]), epoch(d["valid_until"])
        if expires <= proposed:
            raise ValueError("invalid_decision_window")
        payload = encoded(d)
        size = len(payload.encode()) + 4096
        if size > 65_536:
            raise ValueError("capture_request_size_bound")
        with self.atomic():
            prior = self.db.execute("SELECT id,payload FROM capture_requests WHERE request_key=?",
                                    (key,)).fetchone()
            if prior:
                if prior["payload"] != payload:
                    raise ValueError("decision_id_reused_with_different_payload")
                return prior["id"]
            full = self.db.execute("""SELECT
                (SELECT count(*) FROM capture_requests)>=max_rows OR used_bytes+?>max_bytes
                OR (SELECT count(*) FROM capture_requests WHERE state='PENDING')>=128
                FROM capture_meta WHERE id=1""", (size,)).fetchone()[0]
            if full:
                raise ValueError("capture_request_capacity")
            cur = self.db.execute("INSERT INTO capture_requests(request_key,payload,expires) VALUES(?,?,?)",
                                  (key, payload, expires))
            request_id = cur.lastrowid
            protected = wallets if d["admissible"] else []
            self.db.executemany("INSERT INTO capture_members(request_id,wallet) VALUES(?,?)",
                                ((request_id, w) for w in protected))
            self.db.execute("UPDATE capture_meta SET used_bytes=used_bytes+? WHERE id=1", (size,))
            return request_id
