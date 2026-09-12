use super::*;

impl SourceSellStaging {
    pub(crate) fn recovery_interval() -> tokio::time::Interval {
        let mut ticker = tokio::time::interval(SOURCE_SELL_RECOVERY_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker
    }

    /// One indexed durable visit per call, ACTIVE_LIMIT1. A wrap delays the next
    /// pass, so an unrepaired transient fault cannot create a tight retry loop.
    /// Called by the intake loop at startup, completion and its own periodic tick.
    pub(crate) fn recover(&mut self, store: &SqliteStore, sqlite_path: &str) -> Result<()> {
        if let Some(signature) = self.recover_next(store, sqlite_path)? {
            record(&signature, StageNotice::Scheduled, None);
        }
        Ok(())
    }

    /// Shared arbitration after ACK and from loop recovery. During wrap cooldown
    /// ingress keeps its existing fast path; once due, neither path skips a visit.
    pub(super) fn recover_next(
        &mut self,
        store: &SqliteStore,
        sqlite_path: &str,
    ) -> Result<Option<String>> {
        self.reap_ready()?;
        if !self.is_empty() || std::time::Instant::now() < self.next_recovery {
            return Ok(None);
        }
        let Some(job) = store.advance_source_sell_handoff()? else {
            self.next_recovery = std::time::Instant::now() + SOURCE_SELL_RECOVERY_INTERVAL;
            return Ok(None);
        };
        let position_id = job
            .original_position_id
            .ok_or_else(|| anyhow::anyhow!("pending handoff has unknown generation"))?;
        let captured = CapturedSourceSell {
            swap: job.event,
            position_id,
            observation: OriginalObservation::Inserted,
        };
        let signature = captured.swap.signature.clone();
        self.spawn(captured, sqlite_path);
        Ok(Some(signature))
    }
}
