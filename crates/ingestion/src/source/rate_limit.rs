use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time;

#[derive(Debug)]
struct TokenBucketState {
    tokens: f64,
    last_refill: Instant,
}

#[derive(Debug)]
pub(super) struct TokenBucketLimiter {
    tokens_per_second: f64,
    burst: f64,
    state: AsyncMutex<TokenBucketState>,
}

impl TokenBucketLimiter {
    pub(super) fn new(tokens_per_second: u64, burst: u64) -> Option<Arc<Self>> {
        if tokens_per_second == 0 {
            return None;
        }
        let burst = burst.max(tokens_per_second).max(1) as f64;
        Some(Arc::new(Self {
            tokens_per_second: tokens_per_second as f64,
            burst,
            state: AsyncMutex::new(TokenBucketState {
                tokens: burst,
                last_refill: Instant::now(),
            }),
        }))
    }

    pub(super) async fn acquire(&self) {
        loop {
            let wait_duration = {
                let mut guard = self.state.lock().await;
                let now = Instant::now();
                let elapsed = now.duration_since(guard.last_refill).as_secs_f64();
                if elapsed > 0.0 {
                    guard.tokens =
                        (guard.tokens + elapsed * self.tokens_per_second).min(self.burst);
                    guard.last_refill = now;
                }
                if guard.tokens >= 1.0 {
                    guard.tokens -= 1.0;
                    None
                } else {
                    let deficit = (1.0 - guard.tokens).max(0.0);
                    let wait_seconds = (deficit / self.tokens_per_second).max(0.001);
                    Some(Duration::from_secs_f64(wait_seconds))
                }
            };
            if let Some(wait) = wait_duration {
                time::sleep(wait).await;
                continue;
            }
            return;
        }
    }
}

#[derive(Debug)]
pub(super) struct HeliusEndpoint {
    pub(super) url: String,
    pub(super) limiter: Option<Arc<TokenBucketLimiter>>,
}
