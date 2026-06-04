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
fn retention_delete_budgets_are_large_enough_for_backlog_catchup() {
    let observed_swap_rows_per_run = OBSERVED_SWAP_RETENTION_DELETE_BATCH_SIZE
        .saturating_mul(OBSERVED_SWAP_RETENTION_MAX_RAW_DELETE_BATCHES_PER_RUN);
    let recent_raw_rows_per_run = RECENT_RAW_JOURNAL_RETENTION_DELETE_BATCH_SIZE
        .saturating_mul(RECENT_RAW_JOURNAL_RETENTION_MAX_DELETE_BATCHES_PER_RUN);

    assert!(observed_swap_rows_per_run >= 100_000);
    assert!(recent_raw_rows_per_run >= 200_000);
}
