//! Fixed-size notification measurements; a wake is not transport readiness.
use super::receive_stage_ledger::{Distribution, DURATION_BOUNDS_NS};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub(crate) const COUNTER_NAMES: [&str; 22] = [
    "polls",
    "pending",
    "ready",
    "notifications",
    "first_notifications",
    "repeated_notifications",
    "pre_pending_notifications",
    "superseded_notifications",
    "closed_notifications",
    "repolls_pending",
    "repolls_ready",
    "repolls_without_wake",
    "pre_pending_repolls",
    "valid_pairs",
    "invalid_ordering",
    "unobserved_polls",
    "cancelled_futures",
    "cancelled_pending",
    "ready_poll_notified",
    "state_unavailable",
    "generation_exhausted",
    "pending_epochs",
];
#[derive(Clone, Copy)]
#[repr(usize)]
pub(crate) enum Count {
    Polls,
    Pending,
    Ready,
    Notifications,
    FirstNotifications,
    RepeatedNotifications,
    PrePendingNotifications,
    SupersededNotifications,
    ClosedNotifications,
    RepollsPending,
    RepollsReady,
    RepollsWithoutWake,
    PrePendingRepolls,
    ValidPairs,
    InvalidOrdering,
    UnobservedPolls,
    CancelledFutures,
    CancelledPending,
    ReadyPollNotified,
    StateUnavailable,
    GenerationExhausted,
    PendingEpochs,
}
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct WakeSnapshot {
    pub counts: [u64; 22],
    pub pending_to_first_wake_ns: Distribution,
    pub first_wake_to_repoll_ns: Distribution,
    pub pre_pending_wake_to_repoll_ns: Distribution,
    pub active_pending: u64,
}
impl WakeSnapshot {
    pub fn bump(&mut self, count: Count) {
        let value = &mut self.counts[count as usize];
        *value = value.saturating_add(1);
    }
    pub fn record_pair(&mut self, before: Duration, after: Duration) {
        self.bump(Count::ValidPairs);
        self.pending_to_first_wake_ns
            .record(ns(before), &DURATION_BOUNDS_NS);
        self.first_wake_to_repoll_ns
            .record(ns(after), &DURATION_BOUNDS_NS);
    }
    pub fn record_pre_pending(&mut self, after: Duration) {
        self.bump(Count::PrePendingRepolls);
        self.pre_pending_wake_to_repoll_ns
            .record(ns(after), &DURATION_BOUNDS_NS);
    }
}
fn ns(value: Duration) -> u64 {
    value.as_nanos().min(u128::from(u64::MAX)) as u64
}
#[derive(Debug, Clone, Default)]
pub(crate) struct WakeMetrics(Arc<Mutex<WakeSnapshot>>);
impl WakeMetrics {
    pub fn update(&self, f: impl FnOnce(&mut WakeSnapshot)) {
        if let Ok(mut state) = self.0.lock() {
            f(&mut state);
        }
    }
    pub fn snapshot(&self) -> Option<WakeSnapshot> {
        self.0.lock().ok().map(|state| *state)
    }
}
