//! Owned async quote tasks share the shadow scheduler's active/pending budgets.
use crate::execution_quote_canary::job::{HotQuoteAdmission, HotQuoteOrigin, HotQuoteOutput};
use std::collections::{HashMap, VecDeque};
use tokio::task::{Id, JoinSet};

#[derive(Default)]
pub(crate) struct HotQuotes {
    tasks: JoinSet<HotQuoteOutput>,
    pub(super) origins: HashMap<Id, HotQuoteOrigin>,
    pub(super) pending: VecDeque<HotQuoteAdmission>,
    closed: bool,
    pub(super) completed: Option<Completion>,
}
pub(crate) struct Completion {
    pub(crate) origin: HotQuoteOrigin,
    pub(crate) output: Result<HotQuoteOutput, &'static str>,
}
impl HotQuotes {
    pub(crate) fn active(&self) -> usize {
        self.tasks.len() + usize::from(self.completed.is_some())
    }
    pub(crate) fn pending(&self) -> usize {
        self.pending.len()
    }
    pub(crate) fn admit(
        &mut self,
        admission: HotQuoteAdmission,
        shadow_active: usize,
        buffered: usize,
    ) -> Result<(), HotQuoteAdmission> {
        if self.closed {
            return Err(admission);
        }
        if self.pending.is_empty()
            && shadow_active + self.active() < crate::SHADOW_MAX_CONCURRENT_WORKERS
        {
            self.start(admission);
        } else if buffered < crate::SHADOW_PENDING_TASK_CAPACITY {
            self.pending.push_back(admission);
        } else {
            return Err(admission);
        }
        Ok(())
    }
    fn start(&mut self, admission: HotQuoteAdmission) {
        let HotQuoteAdmission { origin, job } = admission;
        let handle = self.tasks.spawn(job.run());
        self.origins.insert(handle.id(), origin);
    }
    pub(crate) fn start_ready(&mut self, shadow_active: usize) {
        while !self.closed && shadow_active + self.active() < crate::SHADOW_MAX_CONCURRENT_WORKERS {
            let Some(job) = self.pending.pop_front() else {
                break;
            };
            self.start(job);
        }
    }
    pub(crate) fn evict_pending(&mut self) -> Option<HotQuoteAdmission> {
        self.pending.pop_back()
    }
    pub(crate) async fn finish_next(&mut self) -> Option<Completion> {
        let result = self.tasks.join_next_with_id().await?;
        let (id, output) = match result {
            Ok((id, output)) => (id, Ok(output)),
            Err(error) => (
                error.id(),
                Err(if error.is_cancelled() {
                    "hot_quote_cancelled"
                } else {
                    "hot_quote_panicked"
                }),
            ),
        };
        Some(Completion {
            origin: self.origins.remove(&id).expect("owned quote task identity"),
            output,
        })
    }
    pub(crate) async fn shutdown(&mut self) {
        self.closed = true;
        if let Some(completion) = self.completed.take() {
            crate::telemetry::hot_quote::record(
                &completion.origin.signal_id(),
                "hot_quote_shutdown",
                self.active(),
                self.pending(),
            );
        }
        while let Some(job) = self.pending.pop_front() {
            crate::telemetry::hot_quote::record(
                &job.origin.signal_id(),
                "hot_quote_shutdown",
                self.active(),
                self.pending(),
            );
        }
        self.tasks.abort_all();
        while let Some(completion) = self.finish_next().await {
            crate::telemetry::hot_quote::record(
                &completion.origin.signal_id(),
                completion.output.err().unwrap_or("hot_quote_shutdown"),
                self.active(),
                0,
            );
        }
        assert!(self.origins.is_empty());
    }
}

impl super::ShadowScheduler {
    pub(crate) fn active_task_count(&self) -> usize {
        self.shadow_workers.len() + self.hot_quotes.active()
    }
    pub(crate) fn admit_hot_quote(
        &mut self,
        job: HotQuoteAdmission,
    ) -> Result<(), HotQuoteAdmission> {
        let buffered = self.buffered_shadow_task_count();
        self.hot_quotes
            .admit(job, self.shadow_workers.len(), buffered)
    }
}

#[path = "app_tests/b70_r1_scheduler_tests.rs"]
mod b70_r1_scheduler_checks;
#[path = "app_tests/b70_scheduler_priv_tests.rs"]
mod b70_scheduler_checks;
