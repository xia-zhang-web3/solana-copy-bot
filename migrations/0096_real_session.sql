-- Explicit new normal-network authority. No migration/import/rearm of old grants.
CREATE TABLE real_session_sessions (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), session_id TEXT NOT NULL UNIQUE,
 identity_json TEXT NOT NULL, cohort_json TEXT NOT NULL, scope_json TEXT NOT NULL
);
CREATE TRIGGER real_session_identity_immutable BEFORE UPDATE ON real_session_sessions
BEGIN SELECT RAISE(ABORT,'real_session_identity_immutable'); END;
CREATE TRIGGER real_session_identity_no_delete BEFORE DELETE ON real_session_sessions
BEGIN SELECT RAISE(ABORT,'real_session_identity_immutable'); END;
CREATE TABLE real_session_cycles (
 session_id TEXT NOT NULL REFERENCES real_session_sessions(session_id),
 cycle_no INTEGER NOT NULL CHECK(cycle_no BETWEEN 1 AND 32), cycle_id TEXT NOT NULL UNIQUE,
 scope_json TEXT NOT NULL, source_buy_signature TEXT NOT NULL UNIQUE, source_sell_signature TEXT UNIQUE,
 state TEXT NOT NULL CHECK(state IN ('planned','active','closed')),
 buy_order_id TEXT UNIQUE, position_id TEXT UNIQUE, token TEXT, last_decision_at TEXT NOT NULL, closed_at TEXT,
 PRIMARY KEY(session_id,cycle_no)
);
CREATE TRIGGER real_session_cycle_identity BEFORE UPDATE ON real_session_cycles
WHEN NEW.session_id!=OLD.session_id OR NEW.cycle_no!=OLD.cycle_no OR NEW.cycle_id!=OLD.cycle_id
 OR NEW.scope_json!=OLD.scope_json OR NEW.source_buy_signature!=OLD.source_buy_signature
 OR (OLD.source_sell_signature IS NOT NULL AND NEW.source_sell_signature IS NOT OLD.source_sell_signature)
 OR (OLD.buy_order_id IS NOT NULL AND NEW.buy_order_id IS NOT OLD.buy_order_id)
 OR (OLD.state='closed' AND NEW.state!='closed')
BEGIN SELECT RAISE(ABORT,'real_session_cycle_identity'); END;
CREATE TRIGGER real_session_cycle_no_delete BEFORE DELETE ON real_session_cycles
BEGIN SELECT RAISE(ABORT,'real_session_cycle_immutable'); END;
CREATE TABLE real_session_orders (
 order_id TEXT PRIMARY KEY REFERENCES execution_canary_dispatch(order_id),
 cycle_id TEXT NOT NULL REFERENCES real_session_cycles(cycle_id), side TEXT NOT NULL CHECK(side IN ('buy','sell')),
 UNIQUE(cycle_id,side)
);
CREATE TABLE real_session_stops (
 session_id TEXT PRIMARY KEY REFERENCES real_session_sessions(session_id), reason TEXT NOT NULL, stopped_at TEXT NOT NULL
);
CREATE TABLE real_session_source_receipts (
 signature TEXT PRIMARY KEY, cycle_id TEXT NOT NULL REFERENCES real_session_cycles(cycle_id),
 side TEXT NOT NULL CHECK(side IN ('buy','sell')), slot INTEGER NOT NULL CHECK(slot>0),
 message_sha256 TEXT NOT NULL, transaction_sha256 TEXT NOT NULL, receipt_sha256 TEXT NOT NULL,
 amount_raw TEXT NOT NULL, finalized_at TEXT NOT NULL, UNIQUE(cycle_id,side)
);
CREATE TRIGGER real_session_receipt_immutable BEFORE UPDATE ON real_session_source_receipts
BEGIN SELECT RAISE(ABORT,'real_session_receipt_immutable'); END;
CREATE TRIGGER real_session_receipt_no_delete BEFORE DELETE ON real_session_source_receipts
BEGIN SELECT RAISE(ABORT,'real_session_receipt_immutable'); END;
CREATE TABLE real_session_native_decisions (
 signature TEXT PRIMARY KEY REFERENCES native_buy_decisions(signature), run_id TEXT NOT NULL REFERENCES real_session_sessions(session_id),
 cycle_id TEXT NOT NULL UNIQUE REFERENCES real_session_cycles(cycle_id), fence_epoch_id INTEGER NOT NULL REFERENCES native_buy_fence_epochs(epoch_id),
 wallet TEXT NOT NULL, mint TEXT NOT NULL, admitted_at TEXT NOT NULL, policy_identity TEXT NOT NULL
);
CREATE TRIGGER real_session_native_immutable BEFORE UPDATE ON real_session_native_decisions
BEGIN SELECT RAISE(ABORT,'real_session_native_immutable'); END;
CREATE TRIGGER real_session_native_no_delete BEFORE DELETE ON real_session_native_decisions
BEGIN SELECT RAISE(ABORT,'real_session_native_immutable'); END;
