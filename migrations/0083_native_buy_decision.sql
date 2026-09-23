-- Offline-only BUY authority. A fence belongs to exactly one durable intake session.
CREATE TABLE native_buy_fences (
    session TEXT PRIMARY KEY,
    processed_slot INTEGER NOT NULL CHECK(processed_slot > 0),
    sampled_at TEXT NOT NULL,
    genesis_hash TEXT NOT NULL,
    policy_identity TEXT NOT NULL
);
CREATE TRIGGER native_buy_fence_immutable BEFORE UPDATE ON native_buy_fences
BEGIN SELECT RAISE(ABORT, 'native BUY fence immutable'); END;
CREATE TRIGGER native_buy_fence_no_delete BEFORE DELETE ON native_buy_fences
BEGIN SELECT RAISE(ABORT, 'native BUY fence immutable'); END;
CREATE TABLE native_buy_session_state (
    id INTEGER PRIMARY KEY CHECK(id=1),
    active_session TEXT REFERENCES native_buy_fences(session)
);

-- Only the first checked admission may create this row. Finality is written once.
CREATE TABLE native_buy_decisions (
    signature TEXT PRIMARY KEY,
    decision_id TEXT NOT NULL UNIQUE,
    signal_id TEXT NOT NULL UNIQUE,
    first_session TEXT NOT NULL REFERENCES native_buy_fences(session),
    first_sequence INTEGER NOT NULL,
    admission TEXT NOT NULL,
    admitted_at TEXT NOT NULL,
    wallet TEXT NOT NULL,
    mint TEXT NOT NULL,
    source_slot INTEGER NOT NULL,
    amount_lamports INTEGER NOT NULL CHECK(amount_lamports > 0),
    follow_id INTEGER NOT NULL,
    follow_added_at TEXT NOT NULL,
    source_cohort TEXT NOT NULL,
    cohort_window_start TEXT NOT NULL,
    cohort_updated_at TEXT NOT NULL,
    publication_fingerprint TEXT NOT NULL,
    publication_published_at TEXT NOT NULL,
    finalized_at TEXT,
    finalized_slot INTEGER,
    late INTEGER NOT NULL DEFAULT 0 CHECK(late IN (0,1)),
    CHECK((finalized_at IS NULL) = (finalized_slot IS NULL))
);
CREATE INDEX idx_native_buy_decisions_pending ON native_buy_decisions(finalized_at, admitted_at);
CREATE TRIGGER native_buy_decision_immutable BEFORE UPDATE ON native_buy_decisions
WHEN NEW.signature != OLD.signature OR NEW.decision_id != OLD.decision_id
 OR NEW.signal_id != OLD.signal_id OR NEW.first_session != OLD.first_session
 OR NEW.first_sequence != OLD.first_sequence OR NEW.admission != OLD.admission
 OR NEW.admitted_at != OLD.admitted_at OR NEW.wallet != OLD.wallet
 OR NEW.mint != OLD.mint OR NEW.source_slot != OLD.source_slot
 OR NEW.amount_lamports != OLD.amount_lamports OR NEW.follow_id != OLD.follow_id
 OR NEW.follow_added_at != OLD.follow_added_at OR NEW.source_cohort != OLD.source_cohort
 OR NEW.cohort_window_start != OLD.cohort_window_start
 OR NEW.cohort_updated_at != OLD.cohort_updated_at
 OR NEW.publication_fingerprint != OLD.publication_fingerprint
 OR NEW.publication_published_at != OLD.publication_published_at
 OR (OLD.finalized_at IS NOT NULL AND (NEW.finalized_at IS NOT OLD.finalized_at OR NEW.finalized_slot IS NOT OLD.finalized_slot))
 OR NEW.late < OLD.late
BEGIN SELECT RAISE(ABORT, 'native BUY decision immutable'); END;
CREATE TRIGGER native_buy_decision_no_delete BEFORE DELETE ON native_buy_decisions
BEGIN SELECT RAISE(ABORT, 'native BUY decision immutable'); END;
