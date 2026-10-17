//! Synthetic test helpers extracted from continuity production source.
use super::{ClockStamp, ReaderObservation, TransitionKind, TransitionStart};

impl ReaderObservation {
    #[cfg(test)]
    pub(in crate::source) fn begin(&self, began: u64, hold: Option<bool>, interrupted: bool) -> TransitionStart {
        self.begin_observed((began, ClockStamp::default()), hold, interrupted)
    }
    #[cfg(test)]
    pub(in crate::source) fn finish(&self, kind: TransitionKind, start: TransitionStart, ended: u64, hold: Option<bool>, interrupted: bool) {
        self.finish_observed(kind, start, (ended, ClockStamp::default()), hold, interrupted)
    }
}
