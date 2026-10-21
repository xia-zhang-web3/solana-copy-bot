//! Private-field fault injection only; no production override or business gate.
use crate::source::durable::hold_observation::HoldTracker;
use std::sync::atomic::{AtomicBool, Ordering};
impl HoldTracker {
    pub(in crate::source) fn test_overflow(&self) {
        self.revision.store(u64::MAX, Ordering::SeqCst);
    }
    pub(in crate::source) fn test_paused_assertion(&self, flag: &AtomicBool, pause: impl FnOnce()) {
        let _phase = self.phase();
        self.increment(&self.epoch);
        pause();
        flag.store(true, Ordering::Release);
    }
}
