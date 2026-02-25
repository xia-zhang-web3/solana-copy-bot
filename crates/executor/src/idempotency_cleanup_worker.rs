use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{debug, warn};

use crate::AppState;

pub(crate) const MIN_RESPONSE_CLEANUP_WORKER_TICK_SEC: u64 = 15;
pub(crate) const MAX_RESPONSE_CLEANUP_WORKER_TICK_SEC: u64 = 300;

pub(crate) fn spawn_response_cleanup_worker(state: AppState) -> JoinHandle<()> {
    let response_retention_sec = state.config.idempotency_response_retention_sec;
    let response_cleanup_batch_size = state.config.idempotency_response_cleanup_batch_size;
    let response_cleanup_max_batches_per_run =
        state.config.idempotency_response_cleanup_max_batches_per_run;
    let tick_sec = state.config.idempotency_response_cleanup_worker_tick_sec;
    tokio::spawn(async move {
        let initial_delay_sec = response_cleanup_worker_initial_delay_sec(tick_sec);
        debug!(
            tick_sec,
            initial_delay_sec,
            "background idempotency response cleanup worker started"
        );
        let mut ticker = tokio::time::interval_at(
            Instant::now() + Duration::from_secs(initial_delay_sec),
            Duration::from_secs(tick_sec),
        );
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let idempotency = state.idempotency.clone();
            let cleanup_outcome = tokio::task::spawn_blocking(move || {
                idempotency.run_response_cleanup_if_due_nonblocking(
                    response_retention_sec,
                    response_cleanup_batch_size,
                    response_cleanup_max_batches_per_run,
                )
            })
            .await;
            match cleanup_outcome {
                Ok(Ok(true)) => {}
                Ok(Ok(false)) => {
                    debug!(
                        response_cleanup_batch_size,
                        response_cleanup_max_batches_per_run,
                        "background idempotency response cleanup tick skipped: idempotency store lock busy"
                    );
                }
                Ok(Err(error)) => {
                    warn!(
                        error = %error,
                        response_retention_sec,
                        response_cleanup_batch_size,
                        response_cleanup_max_batches_per_run,
                        "background idempotency response cleanup tick failed"
                    );
                }
                Err(error) => {
                    warn!(
                        error = %error,
                        response_retention_sec,
                        response_cleanup_batch_size,
                        response_cleanup_max_batches_per_run,
                        "background idempotency response cleanup blocking task join failed"
                    );
                }
            }
        }
    })
}

pub(crate) fn response_cleanup_worker_tick_sec(response_retention_sec: u64) -> u64 {
    (response_retention_sec / 8).clamp(
        MIN_RESPONSE_CLEANUP_WORKER_TICK_SEC,
        MAX_RESPONSE_CLEANUP_WORKER_TICK_SEC,
    )
}

fn response_cleanup_worker_initial_delay_sec(tick_sec: u64) -> u64 {
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let folded_nanos = (now_nanos as u64) ^ ((now_nanos >> 64) as u64);
    let seed = folded_nanos ^ u64::from(std::process::id()).rotate_left(17);
    response_cleanup_worker_initial_delay_sec_from_seed(tick_sec, seed)
}

fn response_cleanup_worker_initial_delay_sec_from_seed(tick_sec: u64, seed: u64) -> u64 {
    let tick = tick_sec.max(1);
    (seed % tick).saturating_add(1)
}

#[cfg(test)]
mod tests {
    use super::{
        response_cleanup_worker_initial_delay_sec_from_seed, response_cleanup_worker_tick_sec,
    };

    #[test]
    fn response_cleanup_worker_tick_sec_clamps_bounds() {
        assert_eq!(response_cleanup_worker_tick_sec(1), 15);
        assert_eq!(response_cleanup_worker_tick_sec(120), 15);
        assert_eq!(response_cleanup_worker_tick_sec(2_400), 300);
        assert_eq!(response_cleanup_worker_tick_sec(40_000), 300);
    }

    #[test]
    fn response_cleanup_worker_initial_delay_sec_from_seed_is_within_tick_bounds() {
        assert_eq!(
            response_cleanup_worker_initial_delay_sec_from_seed(1, 12345),
            1
        );

        let tick = 300;
        let delay_a = response_cleanup_worker_initial_delay_sec_from_seed(tick, 42);
        let delay_b = response_cleanup_worker_initial_delay_sec_from_seed(tick, 43);
        assert!((1..=tick).contains(&delay_a));
        assert!((1..=tick).contains(&delay_b));
        assert_eq!(delay_a, response_cleanup_worker_initial_delay_sec_from_seed(tick, 42));
    }
}
