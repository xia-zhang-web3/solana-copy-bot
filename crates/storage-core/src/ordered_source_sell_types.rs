//! Timeless observation intent. No owned amount, event timestamp or execution permit.
use crate::association_sell_preparation::{Check, Evaluation, FirstBinding, Reason};
use crate::association_sell_shadow_types::ShadowRelation;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const PROVIDER_ORDER_STRICT_V1: &str = "provider_order_strict_v1";
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderedSourceSellIntent {
    pub version: u8,
    /// Same canonical identity as legacy: source-sell:{signature}.
    pub intent_id: String,
    pub policy: String,
    /// Exact immutable preparation identity, including original generation/witness
    /// and checked SELL facts. Message clocks in it are provenance only.
    pub first: FirstBinding,
    pub staged_evaluation: Evaluation,
    /// Local write time only. Never copied into a SwapEvent or CopySignalRow.
    pub staged_at: DateTime<Utc>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderedSellReason {
    UnsupportedPolicy,
    MissingPreparation,
    MissingIntent,
    PreparationChanged,
    NoPositivePosition,
    FirstGenerationUnknown,
    MissingFinancialSet,
    EmptyContributors,
    ContributorSetMismatch,
    UnprovenLinks,
    PendingBuys,
    SelectedChain(Check),
    ContributorOrder {
        order_id: String,
        check: Check,
    },
    ShadowScan(Reason),
    ShadowLot {
        lot_id: i64,
        relation: ShadowRelation,
    },
    SourceSignatureClaimed,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderedSellDecision {
    /// Valid only in this completed snapshot, never after an await or mutation.
    ValidatedNow,
    Unknown(OrderedSellReason),
    Blocked(OrderedSellReason),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderedSellStage {
    /// Fresh validation plus atomic insert/readback, no runnable signal.
    Inserted(Box<OrderedSourceSellIntent>),
    /// Same immutable intent, with all predicates freshly checked again.
    Existing(Box<OrderedSourceSellIntent>),
    Unknown(OrderedSellReason),
    Blocked(OrderedSellReason),
}
