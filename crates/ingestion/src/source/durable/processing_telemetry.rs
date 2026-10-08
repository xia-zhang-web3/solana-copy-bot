//! Constant-space processing timings. These observations never gate admission.
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TimingSnapshot {
    pub count: u64,
    pub total_us: u64,
    pub max_us: u64,
}

#[derive(Default)]
struct Timing {
    count: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
}
impl Timing {
    fn note(&self, elapsed: Duration) {
        let us = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
        self.count.fetch_add(1, Ordering::Relaxed);
        let _ = self
            .total_us
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |total| {
                Some(total.saturating_add(us))
            });
        self.max_us.fetch_max(us, Ordering::Relaxed);
    }
    fn snapshot(&self) -> TimingSnapshot {
        TimingSnapshot {
            count: self.count.load(Ordering::Relaxed),
            total_us: self.total_us.load(Ordering::Relaxed),
            max_us: self.max_us.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IngressProcessingSnapshot {
    pub http_recovery: HttpRecoverySnapshot,
    /// Provider-filtered transaction updates skipped by validated signer scope;
    /// their full-block copies still pass complete-chain validation.
    pub filtered_foreign_transactions: u64,
    /// Synchronous facts decoding/association, excluding delivery waits.
    pub association: TimingSnapshot,
    /// Whole update, including output queue waits and recovery validation.
    pub update: TimingSnapshot,
    pub block_update: TimingSnapshot,
    pub transaction_update: TimingSnapshot,
    /// Ordered history transform wall time; the independent anchor stays on
    /// its accepted path. This includes descheduling, not only OS on-CPU time.
    pub http_normalization: TimingSnapshot,
    /// Scheduling/join wait outside the owning transform's execution.
    pub http_normalization_wait: TimingSnapshot,
    /// Time between reader enqueue and processor dequeue.
    pub input_age: TimingSnapshot,
    /// Delivery envelope construction through committed consumer acknowledgement.
    pub durable_ack: TimingSnapshot,
    pub input_queue_count: u64,
    pub input_queue_bytes: u64,
    pub input_queue_count_max: u64,
    pub input_queue_bytes_max: u64,
    /// Logical encoded bytes retained by the association block cache.
    pub block_cache_count: u64,
    pub block_cache_encoded_bytes: u64,
    /// Charged captured-envelope bytes, including per-envelope allocation
    /// headroom. This is not the relay's observed or billed stream byte count.
    pub input_received_bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HttpRecoverySnapshot {
    /// HTTP blocks validated by the processor; durability is reported separately.
    pub recovered_blocks: u64,
    pub first_from_slot: u64,
    pub live_anchor_slot: u64,
    pub recovered_slot: u64,
    pub durable_completed_slot: u64,
    /// Slot spans, not a count of produced blocks or missing trades.
    pub initial_backlog_slots: u64,
    pub max_backlog_slots: u64,
    pub current_backlog_slots: u64,
    pub caught_up_to_anchor: bool,
}

#[derive(Default)]
struct HttpRecovery {
    recovered_blocks: AtomicU64,
    first_from_slot: AtomicU64,
    live_anchor_slot: AtomicU64,
    recovered_slot: AtomicU64,
    durable_completed_slot: AtomicU64,
    latest_live_slot: AtomicU64,
    initial_backlog_slots: AtomicU64,
    max_backlog_slots: AtomicU64,
    current_backlog_slots: AtomicU64,
    caught_up: AtomicU64,
}
impl HttpRecovery {
    fn progress(&self, from: u64, anchor: u64, recovered: u64, live: u64, completed: bool) {
        if self
            .first_from_slot
            .compare_exchange(0, from, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            self.initial_backlog_slots
                .store(live.saturating_sub(from), Ordering::Relaxed);
        }
        let previous_anchor = self.live_anchor_slot.fetch_max(anchor, Ordering::Relaxed);
        if anchor > previous_anchor {
            self.caught_up.store(0, Ordering::Relaxed);
        }
        self.recovered_slot.fetch_max(recovered, Ordering::Relaxed);
        self.latest_live_slot.fetch_max(live, Ordering::Relaxed);
        if completed {
            self.caught_up.store(1, Ordering::Relaxed);
            self.durable(recovered, live);
        } else {
            self.recovered_blocks.fetch_add(1, Ordering::Relaxed);
            self.backlog(from.max(self.durable_completed_slot.load(Ordering::Relaxed)));
        }
    }
    fn backlog(&self, durable: u64) {
        let span = self
            .latest_live_slot
            .load(Ordering::Relaxed)
            .saturating_sub(durable);
        self.max_backlog_slots.fetch_max(span, Ordering::Relaxed);
        self.current_backlog_slots.store(span, Ordering::Relaxed);
    }
    fn durable(&self, slot: u64, live: u64) {
        if self.first_from_slot.load(Ordering::Relaxed) == 0 {
            return;
        }
        self.latest_live_slot.fetch_max(live, Ordering::Relaxed);
        self.durable_completed_slot
            .fetch_max(slot, Ordering::Relaxed);
        self.backlog(self.durable_completed_slot.load(Ordering::Relaxed));
    }
    fn snapshot(&self) -> HttpRecoverySnapshot {
        let get = |v: &AtomicU64| v.load(Ordering::Relaxed);
        HttpRecoverySnapshot {
            recovered_blocks: get(&self.recovered_blocks),
            first_from_slot: get(&self.first_from_slot),
            live_anchor_slot: get(&self.live_anchor_slot),
            recovered_slot: get(&self.recovered_slot),
            durable_completed_slot: get(&self.durable_completed_slot),
            initial_backlog_slots: get(&self.initial_backlog_slots),
            max_backlog_slots: get(&self.max_backlog_slots),
            current_backlog_slots: get(&self.current_backlog_slots),
            caught_up_to_anchor: get(&self.caught_up) != 0,
        }
    }
}

#[derive(Default)]
pub(crate) struct IngressProcessingTelemetry {
    http_recovery: HttpRecovery,
    filtered_foreign_transactions: AtomicU64,
    association: Timing,
    update: Timing,
    block_update: Timing,
    transaction_update: Timing,
    http_normalization: Timing,
    http_normalization_wait: Timing,
    input_age: Timing,
    durable_ack: Timing,
    input_count: AtomicU64,
    input_bytes: AtomicU64,
    input_count_max: AtomicU64,
    input_bytes_max: AtomicU64,
    block_cache_count: AtomicU64,
    block_cache_encoded_bytes: AtomicU64,
    input_received_bytes: AtomicU64,
}
impl IngressProcessingTelemetry {
    pub(crate) fn block_cache(&self, count: usize, bytes: usize) {
        self.block_cache_count
            .store(u64::try_from(count).unwrap_or(u64::MAX), Ordering::Relaxed);
        self.block_cache_encoded_bytes
            .store(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
    }
    pub(crate) fn filtered_foreign_transaction(&self) {
        self.filtered_foreign_transactions
            .fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn http_progress(
        &self,
        from: u64,
        anchor: u64,
        recovered: u64,
        live: u64,
        completed_durable: bool,
    ) {
        self.http_recovery
            .progress(from, anchor, recovered, live, completed_durable);
    }
    pub(crate) fn durable_parent(&self, slot: u64, live: u64) {
        self.http_recovery.durable(slot, live);
    }
    pub(crate) fn association(&self, elapsed: Duration) {
        self.association.note(elapsed);
    }
    pub(crate) fn update(&self, elapsed: Duration) {
        self.update.note(elapsed);
    }
    pub(crate) fn update_kind(&self, block: bool, elapsed: Duration) {
        self.update(elapsed);
        if block {
            self.block_update.note(elapsed);
        } else {
            self.transaction_update.note(elapsed);
        }
    }
    pub(crate) fn durable_ack(&self, elapsed: Duration) {
        self.durable_ack.note(elapsed);
    }
    pub(crate) fn http_normalization(&self, execution: Duration, waiting: Duration) {
        self.http_normalization.note(execution);
        self.http_normalization_wait.note(waiting);
    }
    pub(crate) fn input_enqueued(&self, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        let count = self
            .input_count
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let total = self
            .input_bytes
            .fetch_add(bytes, Ordering::Relaxed)
            .saturating_add(bytes);
        self.input_count_max.fetch_max(count, Ordering::Relaxed);
        self.input_bytes_max.fetch_max(total, Ordering::Relaxed);
        let _ =
            self.input_received_bytes
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |total| {
                    Some(total.saturating_add(bytes))
                });
    }
    pub(crate) fn input_dequeued(&self, bytes: usize, age: Duration) {
        self.input_age.note(age);
        self.input_released(bytes);
    }
    pub(crate) fn input_released(&self, bytes: usize) {
        self.input_count.fetch_sub(1, Ordering::Relaxed);
        self.input_bytes
            .fetch_sub(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
    }
    pub(crate) fn snapshot(&self) -> IngressProcessingSnapshot {
        let get = |v: &AtomicU64| v.load(Ordering::Relaxed);
        IngressProcessingSnapshot {
            http_recovery: self.http_recovery.snapshot(),
            filtered_foreign_transactions: self
                .filtered_foreign_transactions
                .load(Ordering::Relaxed),
            association: self.association.snapshot(),
            update: self.update.snapshot(),
            block_update: self.block_update.snapshot(),
            transaction_update: self.transaction_update.snapshot(),
            http_normalization: self.http_normalization.snapshot(),
            http_normalization_wait: self.http_normalization_wait.snapshot(),
            input_age: self.input_age.snapshot(),
            durable_ack: self.durable_ack.snapshot(),
            input_queue_count: get(&self.input_count),
            input_queue_bytes: get(&self.input_bytes),
            input_queue_count_max: get(&self.input_count_max),
            input_queue_bytes_max: get(&self.input_bytes_max),
            block_cache_count: get(&self.block_cache_count),
            block_cache_encoded_bytes: get(&self.block_cache_encoded_bytes),
            input_received_bytes: get(&self.input_received_bytes),
        }
    }
}
