use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::warn;

use crate::AppState;

const MIN_RESPONSE_CLEANUP_WORKER_TICK_SEC: u64 = 15;
const MAX_RESPONSE_CLEANUP_WORKER_TICK_SEC: u64 = 300;

pub(crate) fn spawn_response_cleanup_worker(state: AppState) -> JoinHandle<()> {
    let response_retention_sec = state.config.idempotency_response_retention_sec;
    let tick_sec = response_cleanup_worker_tick_sec(response_retention_sec);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval_at(
            Instant::now() + Duration::from_secs(tick_sec),
            Duration::from_secs(tick_sec),
        );
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Err(error) = state
                .idempotency
                .run_response_cleanup_if_due(response_retention_sec)
            {
                warn!(
                    error = %error,
                    response_retention_sec,
                    "background idempotency response cleanup tick failed"
                );
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

#[cfg(test)]
mod tests {
    use super::response_cleanup_worker_tick_sec;

    #[test]
    fn response_cleanup_worker_tick_sec_clamps_bounds() {
        assert_eq!(response_cleanup_worker_tick_sec(1), 15);
        assert_eq!(response_cleanup_worker_tick_sec(120), 15);
        assert_eq!(response_cleanup_worker_tick_sec(2_400), 300);
        assert_eq!(response_cleanup_worker_tick_sec(40_000), 300);
    }
}
