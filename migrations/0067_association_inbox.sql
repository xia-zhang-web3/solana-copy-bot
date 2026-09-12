-- object association_inbox_identities
CREATE TABLE association_inbox_identities (
    signature TEXT PRIMARY KEY NOT NULL,
    admission TEXT NOT NULL,
    candidate TEXT NOT NULL,
    first_session TEXT NOT NULL,
    first_sequence INTEGER NOT NULL,
    terminal TEXT,
    conflict INTEGER NOT NULL CHECK (conflict IN (0,1)),
    recovery INTEGER NOT NULL CHECK (recovery IN (0,1)),
    provenance TEXT NOT NULL DEFAULT 'app_dequeue_before_sqlite_await;queue_delay_unknown;canonical_fork_unknown;event_time_unknown;trade_authority_none'
    CHECK (provenance = 'app_dequeue_before_sqlite_await;queue_delay_unknown;canonical_fork_unknown;event_time_unknown;trade_authority_none')
);
-- object association_inbox_events
CREATE TABLE association_inbox_events (
    session TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    delivery TEXT NOT NULL,
    PRIMARY KEY (session, sequence)
);
-- object association_inbox_identity_immutable
CREATE TRIGGER association_inbox_identity_immutable BEFORE UPDATE ON association_inbox_identities
WHEN NEW.signature != OLD.signature OR NEW.admission != OLD.admission
 OR NEW.provenance != OLD.provenance
 OR NEW.candidate != OLD.candidate OR NEW.first_session != OLD.first_session
 OR NEW.first_sequence != OLD.first_sequence
 OR (OLD.terminal IS NOT NULL AND NEW.terminal IS NOT OLD.terminal)
 OR NEW.conflict < OLD.conflict OR NEW.recovery < OLD.recovery
BEGIN SELECT RAISE(ABORT, 'association inbox immutable first identity/state'); END;
-- object association_inbox_event_immutable
CREATE TRIGGER association_inbox_event_immutable BEFORE UPDATE ON association_inbox_events
BEGIN SELECT RAISE(ABORT, 'association inbox immutable event'); END;
