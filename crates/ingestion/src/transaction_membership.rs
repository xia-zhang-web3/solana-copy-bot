//! Opt-in process-local membership at decoded receive and post-COMMIT receipt.
//! Missing records are never a loss assertion; an absent terminal snapshot is incomplete.
use copybot_core_types::SwapEvent;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub const ENV: &str = "COPYBOT_TRANSACTION_MEMBERSHIP_V1";
pub const CAPACITY: usize = 1024;
pub const MAX_CALLS: u64 = 2_000_000;
pub const MAX_ROWS_PER_RECEIPT: usize = 4096;
pub const MAX_OUTPUT_BYTES: usize = 512 * 1024;
const CLOSED: u64 = 1 << 63;
static GLOBAL: OnceLock<Capture> = OnceLock::new();

#[derive(Clone, Copy)]
pub enum JournalReceipt {
    Committed { processed: usize, inserted: usize },
    Unknown,
}

#[derive(Clone, Default, Serialize)]
struct Seen {
    count: u64,
    first_sequence: u64,
    first_elapsed_ms: u64,
}

#[derive(Clone, Serialize)]
struct Row {
    signature: String,
    slot: u64,
    decoded: Seen,
    journal_inserted: Seen,
    journal_batch_unattributed: Seen,
}

#[derive(Default)]
struct State {
    sequence: u64,
    rows: BTreeMap<([u8; 64], u64), Row>,
}

/// Explicit instances also support finite local boundary tests without global/env resets.
pub struct Capture {
    enabled: bool,
    capacity: usize,
    started: Instant,
    started_unix_ms: Option<u128>,
    state: Mutex<State>,
    admission: AtomicU64,
    sealed: AtomicBool,
    closed_elapsed_ms: AtomicU64,
    calls: AtomicU64,
    gaps: AtomicU64,
    overflow: AtomicU64,
}

struct Active<'a>(&'a AtomicU64);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Capture {
    pub fn new(enabled: bool, capacity: usize) -> Self {
        Self {
            enabled,
            capacity: capacity.min(CAPACITY),
            started: Instant::now(),
            started_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()
                .map(|d| d.as_millis()),
            state: Mutex::new(State::default()),
            admission: AtomicU64::new(0),
            sealed: AtomicBool::new(false),
            closed_elapsed_ms: AtomicU64::new(0),
            calls: AtomicU64::new(0),
            gaps: AtomicU64::new(0),
            overflow: AtomicU64::new(0),
        }
    }

    fn enter(&self) -> Option<Active<'_>> {
        if !self.enabled {
            return None;
        }
        // One atomic admission point: close cannot miss a pre-cut in-flight call.
        let before = self.admission.fetch_add(1, Ordering::SeqCst);
        let active = Active(&self.admission);
        if before & CLOSED != 0 {
            return None;
        }
        if self.calls.fetch_add(1, Ordering::Relaxed) >= MAX_CALLS {
            self.overflow.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        Some(active)
    }

    pub(crate) fn gap(&self) {
        self.gaps.fetch_add(1, Ordering::SeqCst);
    }

    pub fn observe_update(&self, update: &yellowstone_grpc_proto::prelude::SubscribeUpdate) {
        let Some(_active) = self.enter() else {
            return;
        };
        crate::source::transaction_membership::decoded(self, update);
    }

    pub(crate) fn decoded(&self, signature: &str, slot: u64) {
        self.record(signature, slot, 0);
    }

    pub fn observe_journal(&self, swaps: &[SwapEvent], receipt: JournalReceipt) {
        let Some(_active) = self.enter() else {
            return;
        };
        let JournalReceipt::Committed {
            processed,
            inserted,
        } = receipt
        else {
            self.gap();
            return;
        };
        if processed > swaps.len() || inserted > processed {
            self.gap();
            return;
        }
        if processed > MAX_ROWS_PER_RECEIPT {
            self.overflow.fetch_add(1, Ordering::Relaxed);
            return;
        }
        // All submitted rows inserted proves each prefix identity; OR IGNORE otherwise
        // cannot distinguish duplicate, conflicting slot, or which individual row inserted.
        let stage = if inserted == processed { 1 } else { 2 };
        for swap in &swaps[..processed] {
            self.record(&swap.signature, swap.slot, stage);
        }
    }

    fn record(&self, signature: &str, slot: u64, stage: u8) {
        let Some(key) = identity(signature, slot) else {
            self.gap();
            return;
        };
        if !selected_bytes(&key.0, slot) {
            return;
        }
        let Ok(mut state) = self.state.try_lock() else {
            self.gap();
            return;
        };
        if self.admission.load(Ordering::SeqCst) & CLOSED != 0 {
            return;
        }
        if !state.rows.contains_key(&key) && state.rows.len() >= self.capacity {
            self.overflow.fetch_add(1, Ordering::Relaxed);
            return;
        }
        state.sequence += 1;
        let sequence = state.sequence;
        let row = state.rows.entry(key).or_insert_with(|| Row {
            signature: signature.to_owned(),
            slot,
            decoded: Seen::default(),
            journal_inserted: Seen::default(),
            journal_batch_unattributed: Seen::default(),
        });
        let seen = match stage {
            0 => &mut row.decoded,
            1 => &mut row.journal_inserted,
            _ => &mut row.journal_batch_unattributed,
        };
        if seen.count == 0 {
            seen.first_sequence = sequence;
            seen.first_elapsed_ms = self.started.elapsed().as_millis().min(u64::MAX as u128) as u64;
        }
        seen.count += 1;
    }

    /// Closes this observation window, not the producer/DB lifecycle. Never waits.
    pub fn close(&self) {
        let before = self.admission.fetch_or(CLOSED, Ordering::SeqCst);
        if before & CLOSED != 0 {
            return;
        }
        if before != 0 {
            self.gap();
        }
        self.closed_elapsed_ms.store(
            self.started.elapsed().as_millis().min(u64::MAX as u128) as u64,
            Ordering::SeqCst,
        );
        self.sealed.store(true, Ordering::SeqCst);
    }

    pub fn snapshot(&self) -> serde_json::Value {
        self.value(true)
    }

    fn value(&self, include_records: bool) -> serde_json::Value {
        let Ok(state) = self.state.try_lock() else {
            self.gap();
            return serde_json::json!({"schema":1,"enabled":self.enabled,"complete":false,"reason":"capture_lock_unavailable"});
        };
        let gaps = self.gaps.load(Ordering::SeqCst);
        let overflow = self.overflow.load(Ordering::Relaxed);
        let closed = self.sealed.load(Ordering::SeqCst);
        serde_json::json!({"schema":1,"enabled":self.enabled,"selector":"sha256-domain-v1-sig64-slotLE-byte0=0",
            "sampling_denominator":256,"capacity":self.capacity,"max_calls":MAX_CALLS,"max_rows_per_receipt":MAX_ROWS_PER_RECEIPT,
            "started_unix_ms":self.started_unix_ms,"closed":closed,
            "closed_elapsed_ms":closed.then(||self.closed_elapsed_ms.load(Ordering::SeqCst)),
            "complete":self.enabled && include_records && closed && gaps==0 && overflow==0 && self.started_unix_ms.is_some(),
            "sequence_cut":state.sequence,"gaps":gaps,"overflow":overflow,
            "absence_is_loss":false,"row_outcome_unknown_is_insert":false,
            "record_count":state.rows.len(),"records_included":include_records,
            "records":include_records.then(|| state.rows.values().collect::<Vec<_>>())})
    }
}

fn identity(signature: &str, slot: u64) -> Option<([u8; 64], u64)> {
    if signature.len() > 88 || slot == 0 {
        return None;
    }
    let mut bytes = [0; 64];
    (bs58::decode(signature).onto(&mut bytes).ok()? == 64).then_some((bytes, slot))
}

fn selected_bytes(signature: &[u8; 64], slot: u64) -> bool {
    let mut hash = Sha256::new();
    hash.update(b"copybot-transaction-membership-v1\0");
    hash.update(signature);
    hash.update(slot.to_le_bytes());
    hash.finalize()[0] == 0
}

pub fn selected(signature: &str, slot: u64) -> bool {
    identity(signature, slot).is_some_and(|(bytes, slot)| selected_bytes(&bytes, slot))
}

pub fn initialize_from_env() {
    GLOBAL.get_or_init(|| Capture::new(std::env::var(ENV).ok().as_deref() == Some("1"), CAPACITY));
}

pub fn decoded_update(update: &yellowstone_grpc_proto::prelude::SubscribeUpdate) {
    if let Some(capture) = GLOBAL.get() {
        capture.observe_update(update);
    }
}

pub fn journal_receipt(swaps: &[SwapEvent], receipt: JournalReceipt) {
    if let Some(capture) = GLOBAL.get() {
        capture.observe_journal(swaps, receipt);
    }
}

pub fn snapshot_json() -> String {
    let value = GLOBAL.get().map(Capture::snapshot).unwrap_or_else(||
        serde_json::json!({"schema":1,"enabled":false,"complete":false,"reason":"not_initialized"}));
    let text = value.to_string();
    if text.len() <= MAX_OUTPUT_BYTES {
        text
    } else {
        serde_json::json!({"schema":1,"enabled":true,"complete":false,"reason":"output_bound_exceeded"}).to_string()
    }
}

/// Small status only in the existing consumer cadence; ID output is shutdown-only.
pub fn status_json() -> String {
    GLOBAL.get().map(|c| c.value(false)).unwrap_or_else(||
        serde_json::json!({"schema":1,"enabled":false,"complete":false,"reason":"not_initialized"})
    ).to_string()
}

pub fn finish() {
    if let Some(capture) = GLOBAL.get().filter(|c| c.enabled) {
        capture.close();
        tracing::info!(transaction_membership = %snapshot_json(), "transaction membership capture closed");
    }
}

#[cfg(test)]
#[path = "transaction_membership_tests.rs"]
mod tests;
