-- Explicit one-BUY technical cohort, separate from Discovery publication.
CREATE TABLE native_buy_technical_cohort (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    run_id TEXT NOT NULL UNIQUE,
    wallet_ids_json TEXT NOT NULL,
    mint_policy TEXT NOT NULL CHECK(mint_policy='classic_spl_mint_v1'),
    activated_at TEXT NOT NULL,
    deadline TEXT NOT NULL,
    max_buy_count INTEGER NOT NULL CHECK(max_buy_count=1),
    policy_identity TEXT NOT NULL
);
CREATE TRIGGER native_buy_technical_cohort_immutable BEFORE UPDATE ON native_buy_technical_cohort
BEGIN SELECT RAISE(ABORT, 'technical cohort authority immutable'); END;
CREATE TRIGGER native_buy_technical_cohort_no_delete BEFORE DELETE ON native_buy_technical_cohort
BEGIN SELECT RAISE(ABORT, 'technical cohort authority immutable'); END;

-- Each sample is immutable. A decision pins one sample already recorded at admission.
CREATE TABLE native_buy_fence_epochs (
    epoch_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session TEXT NOT NULL REFERENCES native_buy_fences(session),
    processed_slot INTEGER NOT NULL CHECK(processed_slot > 0),
    sampled_at TEXT NOT NULL,
    genesis_hash TEXT NOT NULL,
    policy_identity TEXT NOT NULL
);
CREATE INDEX idx_native_buy_fence_epochs_session ON native_buy_fence_epochs(session,epoch_id);
CREATE TRIGGER native_buy_fence_epoch_immutable BEFORE UPDATE ON native_buy_fence_epochs
BEGIN SELECT RAISE(ABORT, 'native BUY fence epoch immutable'); END;
CREATE TRIGGER native_buy_fence_epoch_no_delete BEFORE DELETE ON native_buy_fence_epochs
BEGIN SELECT RAISE(ABORT, 'native BUY fence epoch immutable'); END;

CREATE TABLE native_buy_cohort_decisions (
    signature TEXT PRIMARY KEY REFERENCES native_buy_decisions(signature),
    run_id TEXT NOT NULL UNIQUE REFERENCES native_buy_technical_cohort(run_id),
    fence_epoch_id INTEGER NOT NULL REFERENCES native_buy_fence_epochs(epoch_id),
    wallet TEXT NOT NULL,
    mint TEXT NOT NULL,
    admitted_at TEXT NOT NULL,
    policy_identity TEXT NOT NULL
);
CREATE TRIGGER native_buy_cohort_decision_immutable BEFORE UPDATE ON native_buy_cohort_decisions
BEGIN SELECT RAISE(ABORT, 'native BUY cohort decision immutable'); END;
CREATE TRIGGER native_buy_cohort_decision_no_delete BEFORE DELETE ON native_buy_cohort_decisions
BEGIN SELECT RAISE(ABORT, 'native BUY cohort decision immutable'); END;
