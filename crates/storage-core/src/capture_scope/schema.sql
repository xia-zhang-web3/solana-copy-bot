PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
CREATE TABLE IF NOT EXISTS capture_meta (
 id INTEGER PRIMARY KEY CHECK(id=1), epoch INTEGER NOT NULL DEFAULT 0,
 gap INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'stopped',
 reason TEXT NOT NULL DEFAULT 'not_started', max_rows INTEGER NOT NULL,
 max_bytes INTEGER NOT NULL, used_bytes INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS capture_requests (
 id INTEGER PRIMARY KEY AUTOINCREMENT, request_key TEXT NOT NULL UNIQUE,
 payload TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'PENDING',
 epoch INTEGER, gap INTEGER, ack_seq INTEGER, ack_at REAL,
 available_seq INTEGER, available_at REAL, expires REAL NOT NULL);
CREATE TABLE IF NOT EXISTS capture_members (
 request_id INTEGER NOT NULL REFERENCES capture_requests(id), wallet TEXT NOT NULL,
 PRIMARY KEY(request_id,wallet));
CREATE TABLE IF NOT EXISTS capture_events (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, signature TEXT UNIQUE, wallet TEXT NOT NULL,
 slot TEXT NOT NULL, epoch INTEGER NOT NULL, request_id INTEGER,
 received_at REAL NOT NULL, source_at REAL, raw BLOB NOT NULL, fingerprint TEXT NOT NULL,
 stage TEXT NOT NULL DEFAULT 'RECEIVED', reason TEXT, event_json TEXT);
CREATE TABLE IF NOT EXISTS capture_obligations (
 event_seq INTEGER PRIMARY KEY REFERENCES capture_events(seq), wallet TEXT NOT NULL,
 mint TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'PENDING',
 ledger_event_id INTEGER, settlement TEXT);
CREATE INDEX IF NOT EXISTS capture_risk_wallet ON capture_obligations(wallet,state);
CREATE TABLE IF NOT EXISTS capture_delivery (
 event_seq INTEGER PRIMARY KEY REFERENCES capture_events(seq), state TEXT NOT NULL,
 reason TEXT, ledger_event_id INTEGER);
CREATE TABLE IF NOT EXISTS capture_binding (
 id INTEGER PRIMARY KEY CHECK(id=1), ledger_path TEXT NOT NULL, ledger_identity TEXT NOT NULL);
