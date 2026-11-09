-- Fresh observation-only capture databases include the recovery boundary.
-- Existing consumed databases are never implicitly migrated by CaptureStore::open.
CREATE TABLE IF NOT EXISTS capture_scope_revisions (
 revision INTEGER PRIMARY KEY AUTOINCREMENT, epoch INTEGER NOT NULL,
 request_id INTEGER, wallets_json TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('PLANNED','ACTIVE','RETIRED')),
 baseline_json TEXT, ack_gap INTEGER);
CREATE UNIQUE INDEX IF NOT EXISTS capture_scope_active ON capture_scope_revisions(state)
 WHERE state='ACTIVE';
CREATE UNIQUE INDEX IF NOT EXISTS capture_scope_planned ON capture_scope_revisions(state)
 WHERE state='PLANNED';
CREATE TABLE IF NOT EXISTS capture_recovery_evidence (
 evidence_id TEXT PRIMARY KEY, scope_revision INTEGER NOT NULL,
 source_kind TEXT NOT NULL, slot TEXT NOT NULL,
 raw BLOB NOT NULL, raw_hash TEXT NOT NULL, provenance_json TEXT NOT NULL,
 FOREIGN KEY(scope_revision) REFERENCES capture_scope_revisions(revision));
CREATE TABLE IF NOT EXISTS capture_recovery (
 id INTEGER PRIMARY KEY AUTOINCREMENT, identity_key TEXT NOT NULL UNIQUE,
 signature TEXT UNIQUE, wallet TEXT NOT NULL, slot TEXT NOT NULL,
 scope_revision INTEGER NOT NULL, event_json TEXT NOT NULL,
 info_json TEXT NOT NULL, provenance_json TEXT NOT NULL,
 observation_json TEXT NOT NULL, raw BLOB NOT NULL, raw_hash TEXT NOT NULL,
 evidence_id TEXT NOT NULL, live_seq INTEGER,
 FOREIGN KEY(scope_revision) REFERENCES capture_scope_revisions(revision),
 FOREIGN KEY(evidence_id) REFERENCES capture_recovery_evidence(evidence_id),
 FOREIGN KEY(live_seq) REFERENCES capture_events(seq));
CREATE TABLE IF NOT EXISTS capture_recovery_outbox (
 recovery_id INTEGER PRIMARY KEY REFERENCES capture_recovery(id),
 state TEXT NOT NULL DEFAULT 'PENDING' CHECK(state IN ('PENDING','MISSED','REJECTED')),
 reason TEXT, disposition_json TEXT, disposition_sha256 TEXT, acknowledged_sha256 TEXT);
CREATE TABLE IF NOT EXISTS capture_recovery_links (
 recovery_id INTEGER NOT NULL REFERENCES capture_recovery(id),
 evidence_id TEXT NOT NULL REFERENCES capture_recovery_evidence(evidence_id),
 source_kind TEXT NOT NULL, provenance_json TEXT NOT NULL,
 PRIMARY KEY(recovery_id,evidence_id));
CREATE TABLE IF NOT EXISTS capture_recovery_meta (
 id INTEGER PRIMARY KEY CHECK(id=1), generation TEXT NOT NULL UNIQUE);
INSERT OR IGNORE INTO capture_recovery_meta(id,generation) VALUES(1,lower(hex(randomblob(16))));
CREATE TABLE IF NOT EXISTS capture_recovery_anchor_links (
 evidence_sha256 TEXT NOT NULL REFERENCES capture_recovery_evidence(evidence_id),
 other_evidence_sha256 TEXT NOT NULL, metadata_json TEXT NOT NULL,
 PRIMARY KEY(evidence_sha256,other_evidence_sha256));
CREATE UNIQUE INDEX IF NOT EXISTS capture_recovery_live_seq ON capture_recovery(live_seq)
 WHERE live_seq IS NOT NULL;
CREATE INDEX IF NOT EXISTS capture_recovery_anchor_other ON capture_recovery_anchor_links(other_evidence_sha256);
CREATE INDEX IF NOT EXISTS capture_recovery_pending ON capture_recovery_outbox(recovery_id)
 WHERE state='PENDING';
CREATE INDEX IF NOT EXISTS capture_recovery_forward_unverified ON capture_recovery_outbox(recovery_id)
 WHERE state!='PENDING' AND (acknowledged_sha256 IS NULL OR acknowledged_sha256 IS NOT disposition_sha256);
CREATE TRIGGER IF NOT EXISTS capture_recovery_forward_changed
 AFTER UPDATE OF state,reason,disposition_json,disposition_sha256 ON capture_recovery_outbox
 WHEN NEW.acknowledged_sha256 IS NOT NULL
 BEGIN UPDATE capture_recovery_outbox SET acknowledged_sha256=NULL WHERE recovery_id=NEW.recovery_id; END;
