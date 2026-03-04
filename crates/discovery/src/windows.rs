use chrono::{DateTime, Utc};
use copybot_core_types::SwapEvent;
use std::cmp::Ordering;
use std::collections::{HashSet, VecDeque};

#[derive(Debug, Clone)]
pub(super) struct DiscoveryCursor {
    pub(super) ts_utc: DateTime<Utc>,
    pub(super) slot: u64,
    pub(super) signature: String,
}

impl DiscoveryCursor {
    pub(super) fn bootstrap(window_start: DateTime<Utc>) -> Self {
        Self {
            ts_utc: window_start,
            slot: 0,
            signature: String::new(),
        }
    }

    pub(super) fn from_swap(swap: &SwapEvent) -> Self {
        Self {
            ts_utc: swap.ts_utc,
            slot: swap.slot,
            signature: swap.signature.clone(),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct DiscoveryWindowState {
    pub(super) swaps: VecDeque<SwapEvent>,
    pub(super) signatures: HashSet<String>,
    pub(super) cursor: Option<DiscoveryCursor>,
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

    pub(super) fn enforce_max_swaps(&mut self, max_swaps: usize) -> usize {
        let max_swaps = max_swaps.max(1);
        let mut evicted = 0usize;
        while self.swaps.len() > max_swaps {
            let Some(expired) = self.swaps.pop_front() else {
                break;
            };
            self.signatures.remove(expired.signature.as_str());
            evicted = evicted.saturating_add(1);
        }
        evicted
    }
}

pub(super) fn cmp_swap_order(a: &SwapEvent, b: &SwapEvent) -> Ordering {
    a.ts_utc
        .cmp(&b.ts_utc)
        .then_with(|| a.slot.cmp(&b.slot))
        .then_with(|| a.signature.cmp(&b.signature))
}
