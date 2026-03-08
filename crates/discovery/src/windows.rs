use super::DiscoverySummary;
use chrono::{DateTime, Utc};
use copybot_core_types::SwapEvent;
use std::cmp::Ordering;
use std::collections::{HashSet, VecDeque};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
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
    pub(super) cap_truncation_floor: Option<DiscoveryCursor>,
    pub(super) bootstrap_from_persisted_metrics: bool,
    pub(super) last_snapshot_bucket: Option<DateTime<Utc>>,
    pub(super) last_summary: Option<DiscoverySummary>,
    pub(super) last_publish_at: Option<DateTime<Utc>>,
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

    pub(super) fn clear_cap_truncation_if_window_caught_up(&mut self, window_start: DateTime<Utc>) {
        let Some(floor) = self.cap_truncation_floor.as_ref() else {
            return;
        };
        if DiscoveryCursor::bootstrap(window_start) > floor.clone() {
            self.cap_truncation_floor = None;
        }
    }

    pub(super) fn push_swap_capped(&mut self, swap: SwapEvent, max_swaps: usize) -> usize {
        self.swaps.push_back(swap);
        self.enforce_max_swaps(max_swaps)
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
        if evicted > 0 {
            self.cap_truncation_floor = self.swaps.front().map(DiscoveryCursor::from_swap);
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
