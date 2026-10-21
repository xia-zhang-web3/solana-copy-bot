//! Diagnostic write accounting. It never owns or authorizes the business flag.
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct HoldStamp {
    pub hold: Option<bool>,
    pub epoch: u64,
    pub revision: u64,
    pub inflight: u64,
    pub overflow: bool,
    pub coherent: bool,
}
#[derive(Default)]
pub(in crate::source) struct HoldTracker {
    pub(super) epoch: AtomicU64,
    pub(super) revision: AtomicU64,
    inflight: AtomicU64,
    overflow: AtomicBool,
}
pub(in crate::source) struct Mutation<'a>(&'a HoldTracker);
impl HoldTracker {
    pub(super) fn increment(&self, counter: &AtomicU64) {
        if counter
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| v.checked_add(1))
            .is_err()
        {
            self.overflow.store(true, Ordering::SeqCst);
        }
    }
    pub(in crate::source) fn phase(&self) -> Mutation<'_> {
        self.increment(&self.inflight);
        self.increment(&self.revision);
        Mutation(self)
    }
    pub(in crate::source) fn write(&self, flag: &AtomicBool, value: bool, order: Ordering) {
        let _phase = self.phase();
        if value {
            self.increment(&self.epoch);
        }
        // Even missing/overflowed observation state cannot skip this store.
        flag.store(value, order);
    }
    pub(in crate::source) fn sample(&self, flag: Option<&Arc<AtomicBool>>) -> HoldStamp {
        let before = self.revision.load(Ordering::SeqCst);
        let active_before = self.inflight.load(Ordering::SeqCst);
        let epoch = self.epoch.load(Ordering::SeqCst);
        let hold = flag.map(|f| f.load(Ordering::Acquire));
        let active_after = self.inflight.load(Ordering::SeqCst);
        let after = self.revision.load(Ordering::SeqCst);
        let overflow = self.overflow.load(Ordering::SeqCst);
        HoldStamp {
            hold,
            epoch,
            revision: after,
            inflight: active_before.max(active_after),
            overflow,
            coherent: !overflow
                && before == after
                && active_before == 0
                && active_after == 0
                && hold.is_some(),
        }
    }
}
impl Drop for Mutation<'_> {
    fn drop(&mut self) {
        self.0.increment(&self.0.revision);
        if self
            .0
            .inflight
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| v.checked_sub(1))
            .is_err()
        {
            self.0.overflow.store(true, Ordering::SeqCst);
        }
    }
}
