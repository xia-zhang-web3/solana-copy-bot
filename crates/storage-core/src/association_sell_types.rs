//! Versioned observation DTOs. None of these states grants trade authority.
use crate::ProvenBuyContributor;
use copybot_core_types::association_delivery::{AdmissionFacts, CandidateGeneration};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Reason {
    HistoricalNoSnapshot,
    MissingShadowOrigin,
    ShadowOriginConflict,
    InitialCandidateUnknown,
    GenerationChanged,
    LookupBound,
    NoLeaderContributor,
    MalformedSignal,
    FinancialSetChanged,
    WitnessChanged,
    MissingAnchor,
    MissingTerminal,
    UnresolvedTerminal,
    AnchorConflict,
    Recovery,
    AnchorIdentityChanged,
    SignatureSubstitution,
    IdentityConflict,
    AmountConflict,
    DecimalsConflict,
    MissingExactAmounts,
    MissingSourceFacts,
    SourceFactsConflict,
    CrossSlot,
    DifferentBlockhash,
    EmptyBlockhash,
    NonIncreasingIndex,
    ParentGap,
    ParentMissingData,
    ParentMalformed,
    ParentConflict,
    ParentHashSlotConflict,
    ParentEndpointMalformed,
    ParentBranchMismatch,
    ParentNotAncestor,
    ParentTraversalBound,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Check {
    Unknown(Reason),
    Blocked(Reason),
    ProviderOrderedWithinBlock,
    ProviderOrderedAcrossBlocks,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnchorIdentity {
    pub admission: AdmissionFacts,
    pub first_session: String,
    pub first_sequence: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptAnchor {
    pub contributor: ProvenBuyContributor,
    pub slot: u64,
    pub wallet: String,
    pub token: String,
    pub raw: String,
    pub decimals: u8,
    /// Exact versioned representation, not a hash with collision risk.
    pub receipt_fingerprint: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Witness {
    pub receipt: ReceiptAnchor,
    pub source_signature: String,
    pub durable_source: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FirstWitness {
    Unknown(Reason),
    Selected(Witness),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageClockCheck {
    HistoricalUnknown,
    Missing,
    Invalid,
    FutureVsAppDequeue,
    NotFutureVsAppDequeue,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FirstBinding {
    /// App clock is diagnostic provenance only, never a transaction timestamp.
    pub app_dequeue_clock: Option<(i64, u32)>,
    pub message_clock_check: MessageClockCheck,
    pub version: u8,
    pub sell: AnchorIdentity,
    pub candidate: CandidateGeneration,
    pub witness: FirstWitness,
    pub contributors: Vec<ReceiptAnchor>,
    /// Lossless ordered JSON fingerprint. Includes unproven and pending links.
    pub contributors_fingerprint: Option<String>,
    pub unproven_links: Vec<String>,
    pub pending_buys: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnchorEvidence {
    pub signature: String,
    pub identity: Option<AnchorIdentity>,
    pub terminal: Option<copybot_core_types::association_delivery::Terminal>,
    pub conflict: bool,
    pub recovery: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContributorOrder {
    pub order_id: String,
    pub receipt_signature: String,
    pub relative_to_sell: Check,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Evaluation {
    /// None in pre-0071 history means historical Unknown, never an empty lot set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow: Option<crate::association_sell_shadow_types::ShadowEvidence>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parent_paths: Vec<ProviderPath>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parent_dependencies: Vec<String>,
    pub selected_chain: Check,
    pub anchors: Vec<AnchorEvidence>,
    pub contributor_orders: Vec<ContributorOrder>,
    pub current_contributors: Vec<ReceiptAnchor>,
    pub contributors_fingerprint: Option<String>,
    pub unproven_links: Vec<String>,
    pub pending_buys: Vec<String>,
    pub limitations: Vec<String>,
    pub trade_authority: String,
}
/// Historical values are never exposed as current validation by this API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedPreparation {
    pub first: FirstBinding,
    pub historical_initial: Evaluation,
    pub historical_latest: Evaluation,
    pub current: Evaluation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParentEdgeRef {
    pub child: copybot_core_types::association_parent::BlockKey,
    pub parent: copybot_core_types::association_parent::BlockKey,
    pub session: String,
    pub sequence: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderPath {
    pub earlier_signature: String,
    pub later_signature: String,
    pub earlier: copybot_core_types::association_parent::BlockKey,
    pub later: copybot_core_types::association_parent::BlockKey,
    pub edges: Vec<ParentEdgeRef>,
}
