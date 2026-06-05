use super::*;
use copybot_storage_core::SqliteBatchedDeleteSummary;

#[test]
fn partial_duration_budget_delete_still_attempts_wal_checkpoint() {
    let summary = SqliteBatchedDeleteSummary {
        deleted_rows: 10_000,
        batches: 1,
    };

    assert!(should_checkpoint_after_observed_swap_retention(
        false, summary
    ));
}

#[test]
fn runtime_pressure_partial_delete_still_attempts_wal_checkpoint() {
    let summary = SqliteBatchedDeleteSummary {
        deleted_rows: 10_000,
        batches: 1,
    };

    assert!(should_checkpoint_after_observed_swap_retention(
        false, summary
    ));
}

#[test]
fn completed_sweep_keeps_periodic_wal_checkpoint_even_without_deletes() {
    assert!(should_checkpoint_after_observed_swap_retention(
        true,
        SqliteBatchedDeleteSummary::default()
    ));
}

#[test]
fn retention_delete_budget_boosts_observed_swaps_only() {
    let observed_swap_rows_per_run = OBSERVED_SWAP_RETENTION_DELETE_BATCH_SIZE
        .saturating_mul(OBSERVED_SWAP_RETENTION_MAX_RAW_DELETE_BATCHES_PER_RUN);

    assert!(observed_swap_rows_per_run >= 100_000);
}

#[test]
fn retention_duration_budget_allows_backlog_catchup_batches() {
    assert!(OBSERVED_SWAP_RETENTION_MAX_DURATION_PER_RUN >= StdDuration::from_secs(60));
}
