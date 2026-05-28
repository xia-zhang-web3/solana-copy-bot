use super::*;
use copybot_storage_core::SqliteBatchedDeleteSummary;

#[test]
fn partial_duration_budget_delete_still_attempts_wal_checkpoint() {
    let summary = SqliteBatchedDeleteSummary {
        deleted_rows: 10_000,
        batches: 1,
    };

    assert!(should_checkpoint_after_observed_swap_retention(
        false,
        Some("duration_budget"),
        summary
    ));
}

#[test]
fn runtime_pressure_partial_delete_skips_wal_checkpoint() {
    let summary = SqliteBatchedDeleteSummary {
        deleted_rows: 10_000,
        batches: 1,
    };

    assert!(!should_checkpoint_after_observed_swap_retention(
        false,
        Some("runtime_pressure"),
        summary
    ));
}

#[test]
fn completed_sweep_keeps_periodic_wal_checkpoint_even_without_deletes() {
    assert!(should_checkpoint_after_observed_swap_retention(
        true,
        None,
        SqliteBatchedDeleteSummary::default()
    ));
}
