//! One bounded outcome per admission/completion; logging does not own claim state.
pub(crate) fn record(signal_id: &str, reason: &'static str, active: usize, pending: usize) {
    tracing::info!(signal_id, reason, active, pending, "hot quote job outcome");
}

pub(crate) fn failure(
    signal_id: &str,
    reason: &'static str,
    error: &anyhow::Error,
    active: usize,
    pending: usize,
) {
    let detail = crate::execution_quote_canary_helpers::short_error(error);
    tracing::warn!(signal_id, reason, active, pending, error = %detail, "hot quote job outcome");
}
