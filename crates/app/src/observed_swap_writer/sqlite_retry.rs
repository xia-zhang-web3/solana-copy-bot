use anyhow::Result;
use copybot_storage::{is_fatal_sqlite_anyhow_error, is_retryable_sqlite_anyhow_error};
use std::thread;
use std::time::Duration as StdDuration;
use tracing::{info, warn};

use super::{
    elapsed_ms_ceil, OBSERVED_SWAP_DISCOVERY_SCORING_REPLAY_SQLITE_LOCK_INITIAL_BACKOFF,
    OBSERVED_SWAP_DISCOVERY_SCORING_REPLAY_SQLITE_LOCK_MAX_ATTEMPTS,
    OBSERVED_SWAP_DISCOVERY_SCORING_REPLAY_SQLITE_LOCK_MAX_BACKOFF,
};

pub(super) fn observed_swap_retention_checkpoint_error_requires_abort(
    primary_error: Option<&anyhow::Error>,
    fallback_error: Option<&anyhow::Error>,
) -> bool {
    primary_error.is_some_and(is_fatal_sqlite_anyhow_error)
        || fallback_error.is_some_and(is_fatal_sqlite_anyhow_error)
}

pub(super) fn observed_swap_writer_discovery_scoring_error_requires_abort(
    error: &anyhow::Error,
) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

pub(super) fn observed_swap_writer_discovery_scoring_replay_apply_error_is_retryable(
    error: &anyhow::Error,
) -> bool {
    is_retryable_sqlite_anyhow_error(error) && !is_fatal_sqlite_anyhow_error(error)
}

pub(super) fn observed_swap_writer_discovery_scoring_covered_through_update_error_is_retryable(
    error: &anyhow::Error,
) -> bool {
    is_retryable_sqlite_anyhow_error(error) && !is_fatal_sqlite_anyhow_error(error)
}

pub(super) fn observed_swap_writer_discovery_scoring_rug_finalize_error_is_retryable(
    error: &anyhow::Error,
) -> bool {
    is_retryable_sqlite_anyhow_error(error) && !is_fatal_sqlite_anyhow_error(error)
}

pub(super) fn discovery_scoring_replay_sqlite_lock_backoff(attempt: usize) -> StdDuration {
    let shift = attempt.saturating_sub(1).min(16) as u32;
    let multiplier = 1_u32.checked_shl(shift).unwrap_or(u32::MAX);
    OBSERVED_SWAP_DISCOVERY_SCORING_REPLAY_SQLITE_LOCK_INITIAL_BACKOFF
        .saturating_mul(multiplier)
        .min(OBSERVED_SWAP_DISCOVERY_SCORING_REPLAY_SQLITE_LOCK_MAX_BACKOFF)
}

pub(super) fn run_discovery_scoring_replay_stage_with_sqlite_lock_retry<T>(
    phase: &'static str,
    reason: &'static str,
    mut operation: impl FnMut() -> Result<T>,
    is_retryable: fn(&anyhow::Error) -> bool,
) -> Result<T> {
    for attempt in 1..=OBSERVED_SWAP_DISCOVERY_SCORING_REPLAY_SQLITE_LOCK_MAX_ATTEMPTS {
        match operation() {
            Ok(value) => {
                if attempt > 1 {
                    info!(
                        phase,
                        attempt,
                        max_attempts =
                            OBSERVED_SWAP_DISCOVERY_SCORING_REPLAY_SQLITE_LOCK_MAX_ATTEMPTS,
                        reason,
                        "discovery scoring replay sqlite lock retry succeeded"
                    );
                }
                return Ok(value);
            }
            Err(error)
                if is_retryable(&error)
                    && attempt
                        < OBSERVED_SWAP_DISCOVERY_SCORING_REPLAY_SQLITE_LOCK_MAX_ATTEMPTS =>
            {
                let delay = discovery_scoring_replay_sqlite_lock_backoff(attempt);
                warn!(
                    phase,
                    attempt,
                    max_attempts =
                        OBSERVED_SWAP_DISCOVERY_SCORING_REPLAY_SQLITE_LOCK_MAX_ATTEMPTS,
                    delay_ms = elapsed_ms_ceil(delay),
                    reason,
                    error = %error,
                    "discovery scoring replay stage hit retryable sqlite lock; retrying after bounded backoff"
                );
                thread::sleep(delay);
            }
            Err(error) => {
                if is_retryable(&error) {
                    warn!(
                        phase,
                        attempt,
                        max_attempts =
                            OBSERVED_SWAP_DISCOVERY_SCORING_REPLAY_SQLITE_LOCK_MAX_ATTEMPTS,
                        reason,
                        error = %error,
                        "discovery scoring replay stage exhausted retryable sqlite lock retries"
                    );
                }
                return Err(error);
            }
        }
    }
    unreachable!("bounded replay sqlite-lock retry loop always returns from final attempt")
}

pub(super) fn observed_swap_retention_protection_load_error_requires_abort(
    error: &anyhow::Error,
) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}
