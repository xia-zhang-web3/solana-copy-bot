//! Protected input evidence. Availability is a receive fact, never execution time.
use crate::association_delivery::{CheckedFacts, InfoIdentity, MessageTime};
use serde::{Deserialize, Serialize};

/// An actual transaction frame with a genuine provider-created timestamp.
/// Complete-block/HTTP recovery never constructs this ordinary intake packet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrdinaryLive {
    pub facts: CheckedFacts,
    pub message_time: MessageTime,
    /// Same checked normalized Info fingerprint as the protected identity store.
    pub info_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtectedSourceKind {
    Stream,
    CompleteLiveBlock,
    ConfirmedHttp,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtectedEvidence {
    pub source_kind: ProtectedSourceKind,
    pub slot: u64,
    /// Stream: SubscribeUpdate; live block: SubscribeUpdateBlock re-encoding;
    /// HTTP: exact bounded JSON-RPC response bytes.
    /// Neither representation claims the original unknown protobuf wire fields.
    pub raw: Vec<u8>,
    pub sha256: String,
    pub available_at_micros: i64,
    /// Provider block metadata, independent from transaction created_at.
    pub block_time: Option<i64>,
    #[serde(default)]
    pub anchor: Option<ProtectedAnchorLink>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtectedAnchorLink {
    pub other_evidence_sha256: String,
    pub comparison: String,
    pub selected_source: ProtectedSourceKind,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtectedTokenQuantity {
    pub side: String,
    pub mint: String,
    pub raw: String,
    pub decimals: u8,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtectedBlockIdentity {
    pub blockhash: String,
    pub parent_slot: u64,
    pub parent_blockhash: String,
    pub transaction_index: u64,
    pub executed_transaction_count: u64,
    pub block_time: Option<i64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtectedObservation {
    pub signature: Option<String>,
    pub wallet: String,
    pub slot: u64,
    pub facts: Option<CheckedFacts>,
    /// The checked non-SOL leg can be exact even when the native SOL leg is not.
    #[serde(default)]
    pub token_quantity: Option<ProtectedTokenQuantity>,
    pub info: InfoIdentity,
    /// Normalized checked Info identity, never a hash claiming raw wire equality.
    pub info_hash: String,
    pub message_time: MessageTime,
    pub source_kind: ProtectedSourceKind,
    pub evidence_sha256: String,
    pub block: Option<ProtectedBlockIdentity>,
    /// Stream fingerprint excludes SubscribeUpdate.created_at, matching legacy receive.
    pub transaction_fingerprint: String,
}
