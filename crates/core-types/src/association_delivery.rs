//! Observation inbox contract, version 1. No event time, canonicality or trade permission.
use crate::ExactSwapAmounts;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InfoIdentity {
    /// Re-encoded full known Info in pinned proto12; unknown wire fields are not recovered.
    pub encoded: Vec<u8>,
    /// Every optional token UI double, including signed zero/NaN payloads omitted by Prost.
    pub float_bits: Vec<Option<u64>>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckedFacts {
    pub signature: String,
    pub slot: u64,
    pub wallet: String,
    pub token_in: String,
    pub token_out: String,
    pub amount_in_bits: u64,
    pub amount_out_bits: u64,
    pub exact_amounts: Option<ExactSwapAmounts>,
    pub programs: Vec<String>,
    pub dex: String,
    pub program_fallback: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageTime {
    CreatedAt { seconds: i64, nanos: u32 },
    Missing,
    InvalidNanos(i32),
    OutOfRangeSeconds(i64),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockTime {
    Seconds(i64),
    Missing,
    OutOfRange(i64),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderAssertion {
    pub slot: u64,
    pub blockhash: String,
    pub signature: String,
    pub transaction_index: u64,
    pub block_time: BlockTime,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Unresolved {
    Expired,
    PendingCapacity,
    EndOfStream,
    SessionReset,
    ConflictingTransaction,
    Association(String),
    ConflictingAssertions,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Terminal {
    ProviderAsserted(ProviderAssertion),
    Unresolved(Unresolved),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Late {
    ConflictingTransaction,
    Association(String),
    ProviderAssertion(ProviderAssertion),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionFacts {
    pub facts: CheckedFacts,
    pub info: InfoIdentity,
    pub message_time: MessageTime,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionGap {
    StartedContinuityUnknown,
    End,
    Reset,
    Rejected(String),
    Transport,
    Recovery,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeliveryEvent {
    Parent(crate::association_parent::ParentObservation),
    Admission(AdmissionFacts),
    Duplicate {
        original: AdmissionFacts,
        observed_info: InfoIdentity,
        observed_slot: u64,
        message_time: MessageTime,
    },
    Terminal {
        signature: String,
        expected: AdmissionFacts,
        result: Terminal,
    },
    Late {
        signature: String,
        original: Terminal,
        evidence: Late,
    },
    Session(SessionGap),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Delivery {
    pub session: String,
    pub sequence: u64,
    pub arrival_offset_ns: u64,
    pub event: DeliveryEvent,
}
/// Observed at the first app dequeue, before the SQLite write await. Queue delay
/// precedes this observation. This does not prove on-chain ordering or ownership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CandidateGeneration {
    Unknown,
    AppObserved {
        position_id: String,
        opened_ts: String,
        token: String,
    },
}
