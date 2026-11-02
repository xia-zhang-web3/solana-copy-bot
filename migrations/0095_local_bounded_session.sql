-- Non-default LOCAL capability. No activation, backfill or legacy grant conversion.
CREATE TABLE local_bounded_sessions (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 session_id TEXT NOT NULL UNIQUE,
 identity_json TEXT NOT NULL,
 cohort_json TEXT NOT NULL,
 scope_json TEXT NOT NULL
);
CREATE TRIGGER local_bounded_identity_immutable BEFORE UPDATE ON local_bounded_sessions
BEGIN SELECT RAISE(ABORT,'local_session_identity_immutable'); END;
CREATE TABLE local_bounded_cycles (
 session_id TEXT NOT NULL REFERENCES local_bounded_sessions(session_id),
 cycle_no INTEGER NOT NULL CHECK(cycle_no BETWEEN 1 AND 2),
 cycle_id TEXT NOT NULL UNIQUE,
 scope_json TEXT NOT NULL,
 source_buy_signature TEXT NOT NULL UNIQUE,
 source_sell_signature TEXT NOT NULL UNIQUE,
 state TEXT NOT NULL CHECK(state IN ('planned','active','closed')),
 buy_order_id TEXT UNIQUE,
 position_id TEXT UNIQUE,
 token TEXT,
 last_decision_at TEXT NOT NULL,
 closed_at TEXT,
 PRIMARY KEY(session_id,cycle_no)
);
CREATE TABLE local_bounded_orders (
 order_id TEXT PRIMARY KEY REFERENCES execution_canary_dispatch(order_id),
 cycle_id TEXT NOT NULL REFERENCES local_bounded_cycles(cycle_id),
 side TEXT NOT NULL CHECK(side IN ('buy','sell')),
 UNIQUE(cycle_id,side)
);
CREATE TABLE local_bounded_stops (
 session_id TEXT PRIMARY KEY REFERENCES local_bounded_sessions(session_id),
 reason TEXT NOT NULL,
 stopped_at TEXT NOT NULL
);
CREATE TABLE local_bounded_source_receipts (
 signature TEXT PRIMARY KEY,
 cycle_id TEXT NOT NULL REFERENCES local_bounded_cycles(cycle_id),
 side TEXT NOT NULL CHECK(side IN ('buy','sell')),
 slot INTEGER NOT NULL CHECK(slot>0),
 message_sha256 TEXT NOT NULL,
 transaction_sha256 TEXT NOT NULL,
 receipt_sha256 TEXT NOT NULL,
 finalized_at TEXT NOT NULL,
 UNIQUE(cycle_id,side)
);
-- Preserve the legacy once FK/UNIQUE(run_id); LOCAL cycles have separate bindings.
CREATE TABLE local_bounded_native_decisions (
 signature TEXT PRIMARY KEY REFERENCES native_buy_decisions(signature),
 run_id TEXT NOT NULL REFERENCES local_bounded_sessions(session_id),
 cycle_id TEXT NOT NULL UNIQUE REFERENCES local_bounded_cycles(cycle_id),
 fence_epoch_id INTEGER NOT NULL REFERENCES native_buy_fence_epochs(epoch_id),
 wallet TEXT NOT NULL,
 mint TEXT NOT NULL,
 admitted_at TEXT NOT NULL,
 policy_identity TEXT NOT NULL
);
CREATE TRIGGER local_bounded_native_binding_immutable BEFORE UPDATE ON local_bounded_native_decisions
BEGIN SELECT RAISE(ABORT,'local_session_native_binding_immutable'); END;
CREATE TRIGGER local_bounded_native_binding_no_delete BEFORE DELETE ON local_bounded_native_decisions
BEGIN SELECT RAISE(ABORT,'local_session_native_binding_no_delete'); END;
