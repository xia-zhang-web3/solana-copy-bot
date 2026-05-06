use copybot_storage_core::is_fatal_sqlite_anyhow_error;

pub(super) fn observed_swap_retention_checkpoint_error_requires_abort(
    primary_error: Option<&anyhow::Error>,
    fallback_error: Option<&anyhow::Error>,
) -> bool {
    primary_error.is_some_and(is_fatal_sqlite_anyhow_error)
        || fallback_error.is_some_and(is_fatal_sqlite_anyhow_error)
}
