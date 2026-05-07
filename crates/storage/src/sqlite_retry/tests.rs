use super::{is_fatal_sqlite_anyhow_error, is_retryable_sqlite_anyhow_error};
use anyhow::anyhow;

#[test]
fn fatal_sqlite_classifier_matches_xshmmap_io_error() {
    let error = anyhow!(
        "failed to commit observed swap batch write transaction: disk I/O error: Error code 4874: I/O error within the xShmMap method"
    );
    assert!(is_fatal_sqlite_anyhow_error(&error));
    assert!(!is_retryable_sqlite_anyhow_error(&error));
}

#[test]
fn fatal_sqlite_classifier_does_not_match_busy_lock_contention() {
    let error = anyhow!("database is locked");
    assert!(!is_fatal_sqlite_anyhow_error(&error));
    assert!(is_retryable_sqlite_anyhow_error(&error));
}
