//! Bounded observation of the durable ingress. Counters never affect admission.
use crate::source::yellowstone_association::{Admission, NotCheckedReason};
use crate::source::yellowstone_facts::DecodeMiss;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MISS_COUNT: usize = 8;
const STAGE_COUNT: usize = 5;
const CLASS_COUNT: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum TransportStage {
    Connect,
    Subscribe,
    Stream,
    Ping,
    End,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum TransportClass {
    Unavailable,
    Deadline,
    Unauthenticated,
    Permission,
    Resource,
    InvalidArgument,
    Internal,
    Other,
    End,
    DataLoss,
}
impl TransportStage {
    fn index(self) -> usize {
        self as usize
    }
}
impl TransportClass {
    fn index(self) -> usize {
        self as usize
    }
    pub(crate) fn status(error: &tonic::Status) -> Self {
        use tonic::Code;
        match error.code() {
            Code::Unavailable => Self::Unavailable,
            Code::DeadlineExceeded => Self::Deadline,
            Code::Unauthenticated => Self::Unauthenticated,
            Code::PermissionDenied => Self::Permission,
            Code::ResourceExhausted => Self::Resource,
            Code::InvalidArgument => Self::InvalidArgument,
            Code::Internal => Self::Internal,
            Code::DataLoss => Self::DataLoss,
            _ => Self::Other,
        }
    }
    pub(crate) fn error(error: &anyhow::Error) -> Self {
        for cause in error.chain() {
            if let Some(status) = cause.downcast_ref::<tonic::Status>() {
                return Self::status(status);
            }
            if let Some(builder) =
                cause.downcast_ref::<yellowstone_grpc_client::GeyserGrpcBuilderError>()
            {
                if matches!(
                    builder,
                    yellowstone_grpc_client::GeyserGrpcBuilderError::MetadataValueError(_)
                ) {
                    return Self::InvalidArgument;
                }
            }
            if let Some(io) = cause.downcast_ref::<std::io::Error>() {
                use std::io::ErrorKind;
                return match io.kind() {
                    ErrorKind::TimedOut => Self::Deadline,
                    ErrorKind::ConnectionRefused
                    | ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::NotConnected => Self::Unavailable,
                    _ => Self::Other,
                };
            }
        }
        Self::Other
    }
    pub(crate) fn subscribe(error: &yellowstone_grpc_client::GeyserGrpcClientError) -> Self {
        match error {
            yellowstone_grpc_client::GeyserGrpcClientError::TonicStatus(status) => {
                Self::status(status)
            }
            yellowstone_grpc_client::GeyserGrpcClientError::SubscribeSendError(_) => Self::Other,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableIngressSnapshot {
    pub processing: super::processing_telemetry::IngressProcessingSnapshot,
    pub received_transactions: u64,
    pub received_blocks: u64,
    /// Supported swap facts classified by this adapter (excludes duplicate short-circuit).
    pub decoded_swaps: u64,
    /// Supported swap facts with a signer in the selected source set.
    pub selected_source: u64,
    /// Supported swap facts signed by the configured bot.
    pub selected_bot: u64,
    pub admissions: u64,
    pub duplicates: u64,
    pub foreign_signer: u64,
    pub decode_errors: u64,
    /// Vote, failed, other program, no attributed swap, invalid amount, unsupported
    /// PumpSwap, unclassified, unsupported message config.
    pub decode_misses: [u64; MISS_COUNT],
    pub admission_rejections: u64,
    pub last_transaction_slot: u64,
    /// Last full block received, before association and queue waits.
    pub last_received_block_slot: u64,
    /// Last Parent emitted to the delivery queue, before SQLite acknowledgement.
    pub last_parent_slot: u64,
    /// Parent acknowledged only after the runtime committed its SQLite envelope.
    pub last_durably_stored_parent_slot: u64,
    pub queue_wait_count: u64,
    pub queue_wait_ms_total: u64,
    pub queue_wait_ms_max: u64,
    pub queue_wait_over_100ms: u64,
    pub reconnects: u64,
    pub reconnect_stages: [u64; STAGE_COUNT],
    pub reconnect_classes: [u64; CLASS_COUNT],
}

#[derive(Default)]
pub(crate) struct DurableIngressTelemetry {
    pub(crate) processing: super::processing_telemetry::IngressProcessingTelemetry,
    received_transactions: AtomicU64,
    received_blocks: AtomicU64,
    decoded_swaps: AtomicU64,
    selected_source: AtomicU64,
    selected_bot: AtomicU64,
    admissions: AtomicU64,
    duplicates: AtomicU64,
    foreign_signer: AtomicU64,
    decode_errors: AtomicU64,
    decode_misses: [AtomicU64; MISS_COUNT],
    admission_rejections: AtomicU64,
    last_transaction_slot: AtomicU64,
    last_received_block_slot: AtomicU64,
    last_parent_slot: AtomicU64,
    last_durably_stored_parent_slot: AtomicU64,
    queue_wait_count: AtomicU64,
    queue_wait_ms_total: AtomicU64,
    queue_wait_ms_max: AtomicU64,
    queue_wait_over_100ms: AtomicU64,
    reconnects: AtomicU64,
    reconnect_stages: [AtomicU64; STAGE_COUNT],
    reconnect_classes: [AtomicU64; CLASS_COUNT],
    last_report_ms: AtomicU64,
}
impl DurableIngressTelemetry {
    fn inc(value: &AtomicU64) {
        value.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn received_transaction(&self, slot: u64) {
        Self::inc(&self.received_transactions);
        self.last_transaction_slot.store(slot, Ordering::Relaxed);
    }
    pub(crate) fn received_block(&self, slot: u64) {
        Self::inc(&self.received_blocks);
        self.last_received_block_slot.store(slot, Ordering::Relaxed);
    }
    pub(crate) fn parent(&self, slot: u64) {
        self.last_parent_slot.store(slot, Ordering::Relaxed);
    }
    pub(crate) fn acknowledge_parent(&self, slot: u64) {
        self.last_durably_stored_parent_slot
            .fetch_max(slot, Ordering::Relaxed);
        self.processing
            .durable_parent(slot, self.last_received_block_slot.load(Ordering::Relaxed));
    }
    pub(crate) fn admission(&self, result: &Admission, bot: bool, scoped: bool) {
        match result {
            Admission::Transaction(_) => {
                Self::inc(&self.decoded_swaps);
                if scoped {
                    Self::inc(if bot {
                        &self.selected_bot
                    } else {
                        &self.selected_source
                    });
                }
                Self::inc(&self.admissions);
            }
            Admission::Duplicate(_) => Self::inc(&self.duplicates),
            Admission::NotChecked { reason, .. } => match reason {
                NotCheckedReason::ForeignSigner => {
                    Self::inc(&self.decoded_swaps);
                    Self::inc(&self.foreign_signer);
                }
                NotCheckedReason::Decode(reason) => {
                    Self::inc(&self.decode_misses[*reason as usize]);
                }
            },
            _ => {}
        }
    }
    pub(crate) fn rejected(&self, decode: bool) {
        Self::inc(&self.admission_rejections);
        if decode {
            Self::inc(&self.decode_errors);
        }
    }
    pub(crate) fn queue_wait(&self, elapsed: Duration) {
        let ms = elapsed.as_millis().min(u128::from(u64::MAX)) as u64;
        Self::inc(&self.queue_wait_count);
        self.queue_wait_ms_total.fetch_add(ms, Ordering::Relaxed);
        self.queue_wait_ms_max.fetch_max(ms, Ordering::Relaxed);
        if ms > 100 {
            Self::inc(&self.queue_wait_over_100ms);
        }
    }
    pub(crate) fn reconnect(&self, stage: TransportStage, class: TransportClass) {
        Self::inc(&self.reconnects);
        Self::inc(&self.reconnect_stages[stage.index()]);
        Self::inc(&self.reconnect_classes[class.index()]);
        tracing::warn!(?stage, ?class, "durable ingress reconnect");
    }
    pub(crate) fn snapshot(&self) -> DurableIngressSnapshot {
        let get = |v: &AtomicU64| v.load(Ordering::Relaxed);
        DurableIngressSnapshot {
            processing: self.processing.snapshot(),
            received_transactions: get(&self.received_transactions),
            received_blocks: get(&self.received_blocks),
            decoded_swaps: get(&self.decoded_swaps),
            selected_source: get(&self.selected_source),
            selected_bot: get(&self.selected_bot),
            admissions: get(&self.admissions),
            duplicates: get(&self.duplicates),
            foreign_signer: get(&self.foreign_signer),
            decode_errors: get(&self.decode_errors),
            decode_misses: std::array::from_fn(|i| get(&self.decode_misses[i])),
            admission_rejections: get(&self.admission_rejections),
            last_transaction_slot: get(&self.last_transaction_slot),
            last_received_block_slot: get(&self.last_received_block_slot),
            last_parent_slot: get(&self.last_parent_slot),
            last_durably_stored_parent_slot: get(&self.last_durably_stored_parent_slot),
            queue_wait_count: get(&self.queue_wait_count),
            queue_wait_ms_total: get(&self.queue_wait_ms_total),
            queue_wait_ms_max: get(&self.queue_wait_ms_max),
            queue_wait_over_100ms: get(&self.queue_wait_over_100ms),
            reconnects: get(&self.reconnects),
            reconnect_stages: std::array::from_fn(|i| get(&self.reconnect_stages[i])),
            reconnect_classes: std::array::from_fn(|i| get(&self.reconnect_classes[i])),
        }
    }
    pub(crate) fn maybe_report(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        let prior = self.last_report_ms.load(Ordering::Relaxed);
        if now.saturating_sub(prior) < 30_000
            || self
                .last_report_ms
                .compare_exchange(prior, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
        {
            return;
        }
        let s = self.snapshot();
        tracing::info!(
            processing = ?s.processing,
            received_transactions = s.received_transactions,
            received_blocks = s.received_blocks,
            decoded_swaps = s.decoded_swaps,
            selected_source = s.selected_source,
            selected_bot = s.selected_bot,
            admissions = s.admissions,
            duplicates = s.duplicates,
            foreign_signer = s.foreign_signer,
            decode_errors = s.decode_errors,
            decode_vote = s.decode_misses[DecodeMiss::Vote as usize],
            decode_failed = s.decode_misses[DecodeMiss::Failed as usize],
            decode_other_program = s.decode_misses[DecodeMiss::UninterestedProgram as usize],
            decode_no_attributed_swap = s.decode_misses[DecodeMiss::NoAttributedSwap as usize],
            decode_invalid_amount = s.decode_misses[DecodeMiss::InvalidAmount as usize],
            decode_unsupported_pumpswap = s.decode_misses[DecodeMiss::UnsupportedPumpSwap as usize],
            decode_unclassified = s.decode_misses[DecodeMiss::Unclassified as usize],
            decode_unsupported_message_config =
                s.decode_misses[DecodeMiss::UnsupportedMessageConfig as usize],
            admission_rejections = s.admission_rejections,
            last_transaction_slot = s.last_transaction_slot,
            last_received_block_slot = s.last_received_block_slot,
            last_parent_slot = s.last_parent_slot,
            last_durably_stored_parent_slot = s.last_durably_stored_parent_slot,
            queue_wait_ms_max = s.queue_wait_ms_max,
            queue_wait_over_100ms = s.queue_wait_over_100ms,
            reconnects = s.reconnects,
            reconnect_connect = s.reconnect_stages[TransportStage::Connect as usize],
            reconnect_subscribe = s.reconnect_stages[TransportStage::Subscribe as usize],
            reconnect_stream = s.reconnect_stages[TransportStage::Stream as usize],
            reconnect_ping = s.reconnect_stages[TransportStage::Ping as usize],
            reconnect_end = s.reconnect_stages[TransportStage::End as usize],
            reconnect_unavailable = s.reconnect_classes[TransportClass::Unavailable as usize],
            reconnect_deadline = s.reconnect_classes[TransportClass::Deadline as usize],
            reconnect_resource = s.reconnect_classes[TransportClass::Resource as usize],
            reconnect_unauthenticated =
                s.reconnect_classes[TransportClass::Unauthenticated as usize],
            reconnect_permission = s.reconnect_classes[TransportClass::Permission as usize],
            reconnect_invalid_argument =
                s.reconnect_classes[TransportClass::InvalidArgument as usize],
            reconnect_data_loss = s.reconnect_classes[TransportClass::DataLoss as usize],
            reconnect_internal = s.reconnect_classes[TransportClass::Internal as usize],
            reconnect_other = s.reconnect_classes[TransportClass::Other as usize],
            reconnect_end_class = s.reconnect_classes[TransportClass::End as usize],
            "durable ingress funnel"
        );
    }
}
