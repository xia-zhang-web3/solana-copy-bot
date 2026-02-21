use chrono::{DateTime, Utc};
use copybot_core_types::SwapEvent;
use std::cmp::Ordering;
use std::collections::{HashSet, VecDeque};

#[derive(Debug, Default)]
pub(super) struct DiscoveryWindowState {
    pub(super) swaps: VecDeque<SwapEvent>,
    pub(super) signatures: HashSet<String>,
    pub(super) high_watermark_ts: Option<DateTime<Utc>>,
}

impl DiscoveryWindowState {
    pub(super) fn evict_before(&mut self, window_start: DateTime<Utc>) {
        while let Some(front) = self.swaps.front() {
            if front.ts_utc >= window_start {
                break;
            }
            let expired = self.swaps.pop_front().expect("checked front exists above");
            self.signatures.remove(&expired.signature);
        }
    }
}

pub(super) fn cmp_swap_order(a: &SwapEvent, b: &SwapEvent) -> Ordering {
    a.ts_utc
        .cmp(&b.ts_utc)
        .then_with(|| a.slot.cmp(&b.slot))
        .then_with(|| a.signature.cmp(&b.signature))
}
