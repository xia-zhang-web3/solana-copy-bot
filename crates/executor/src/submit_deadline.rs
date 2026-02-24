use std::time::{Duration, Instant};

use crate::Reject;

#[derive(Clone, Copy, Debug)]
pub(crate) struct SubmitDeadline {
    started_at: Instant,
    total_budget_ms: u64,
}

impl SubmitDeadline {
    pub(crate) fn new(total_budget_ms: u64) -> Self {
        Self {
            started_at: Instant::now(),
            total_budget_ms: total_budget_ms.max(1),
        }
    }

    fn elapsed_ms(&self) -> u64 {
        let millis = self.started_at.elapsed().as_millis();
        if millis > u128::from(u64::MAX) {
            u64::MAX
        } else {
            millis as u64
        }
    }

    pub(crate) fn remaining_timeout(&self, stage: &str) -> std::result::Result<Duration, Reject> {
        let elapsed_ms = self.elapsed_ms();
        if elapsed_ms >= self.total_budget_ms {
            return Err(Reject::retryable(
                "executor_submit_timeout_budget_exceeded",
                format!(
                    "submit timeout budget exceeded before stage={} elapsed_ms={} budget_ms={}",
                    stage, elapsed_ms, self.total_budget_ms
                ),
            ));
        }
        Ok(Duration::from_millis(
            self.total_budget_ms.saturating_sub(elapsed_ms).max(1),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::SubmitDeadline;

    #[test]
    fn submit_deadline_remaining_timeout_returns_positive_duration() {
        let deadline = SubmitDeadline::new(10);
        let remaining = deadline
            .remaining_timeout("test_stage")
            .expect("new deadline should have remaining timeout");
        assert!(remaining.as_millis() >= 1);
    }

    #[test]
    fn submit_deadline_remaining_timeout_rejects_when_budget_exhausted() {
        let deadline = SubmitDeadline::new(1);
        std::thread::sleep(std::time::Duration::from_millis(2));
        let reject = deadline
            .remaining_timeout("test_stage")
            .expect_err("expired deadline should reject");
        assert!(reject.retryable);
        assert_eq!(reject.code, "executor_submit_timeout_budget_exceeded");
    }
}
