use chrono::Utc;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use super::{FetchedObservation, RawSwapObservation};

#[derive(Debug)]
struct ReorderEntry {
    raw: RawSwapObservation,
    enqueued_at: Instant,
}

#[derive(Debug)]
pub(super) struct ReorderRelease {
    pub(super) raw: RawSwapObservation,
    pub(super) hold_ms: u64,
    pub(super) lag_ms: u64,
}

#[derive(Debug)]
pub(super) struct ReorderBuffer {
    hold_ms: u64,
    max_buffer: usize,
    entries: BTreeMap<(u64, u64, String), ReorderEntry>,
}

impl ReorderBuffer {
    pub(super) fn new(hold_ms: u64, max_buffer: usize) -> Self {
        Self {
            hold_ms,
            max_buffer,
            entries: BTreeMap::new(),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub(super) fn set_max_buffer(&mut self, max_buffer: usize) {
        self.max_buffer = max_buffer;
    }

    pub(super) fn push(&mut self, fetched: FetchedObservation) {
        let key = (
            fetched.raw.slot,
            fetched.arrival_seq,
            fetched.raw.signature.clone(),
        );
        self.entries.entry(key).or_insert(ReorderEntry {
            raw: fetched.raw,
            enqueued_at: Instant::now(),
        });
    }

    pub(super) fn pop_ready(&mut self) -> Option<ReorderRelease> {
        if self.entries.is_empty() {
            return None;
        }

        let first_key = self.entries.keys().next()?.clone();
        let first_entry = self.entries.get(&first_key)?;
        let hold_elapsed = first_entry.enqueued_at.elapsed();
        let hold_target = Duration::from_millis(self.hold_ms.max(1));

        let should_release = self.entries.len() > self.max_buffer || hold_elapsed >= hold_target;
        if !should_release {
            return None;
        }

        self.pop_by_key(first_key)
    }

    pub(super) fn pop_earliest(&mut self) -> Option<ReorderRelease> {
        let first_key = self.entries.keys().next()?.clone();
        self.pop_by_key(first_key)
    }

    pub(super) fn wait_duration(&self) -> Option<Duration> {
        let first_entry = self.entries.values().next()?;
        if self.entries.len() > self.max_buffer {
            return Some(Duration::from_millis(0));
        }

        let hold_target = Duration::from_millis(self.hold_ms.max(1));
        let elapsed = first_entry.enqueued_at.elapsed();
        if elapsed >= hold_target {
            Some(Duration::from_millis(0))
        } else {
            Some(hold_target - elapsed)
        }
    }

    fn pop_by_key(&mut self, key: (u64, u64, String)) -> Option<ReorderRelease> {
        let entry = self.entries.remove(&key)?;
        let hold_ms = entry.enqueued_at.elapsed().as_millis() as u64;
        let lag_ms = (Utc::now() - entry.raw.ts_utc).num_milliseconds().max(0) as u64;
        Some(ReorderRelease {
            raw: entry.raw,
            hold_ms,
            lag_ms,
        })
    }
}
