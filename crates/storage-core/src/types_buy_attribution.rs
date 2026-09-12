use serde::{Deserialize, Serialize};
/// A contributor proves an original BUY, never ownership of a remaining quantity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvenBuyContributor {
    pub fill_id: i64,
    pub order_id: String,
    pub signal_id: String,
    pub source_wallet: String,
    pub tx_signature: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuyAttributionCoverage {
    /// At least one validated linked BUY. History is never claimed complete.
    ProvenSubset,
    /// No contributor can currently be proven. This does not mean no source exists.
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuyAttributionIssue {
    LegacySchema,
    MissingDestination,
    UnprovenDestination,
    DanglingDestination,
    MissingOrder,
    MissingSignal,
    MissingSourceWallet,
    MissingReceiptFacts,
    ReceiptIdentityConflict,
    /// Multiple order claims for the same execution wallet and transaction signature.
    AmbiguousReceipt,
    ReceiptOperandsConflict,
    IdentityConflict,
    PositionConflict,
    NotConfirmed,
    NoProvenContributors,
}
impl std::fmt::Display for BuyAttributionIssue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BUY attribution {self:?}")
    }
}
impl std::error::Error for BuyAttributionIssue {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnprovenBuyLink {
    /// None denotes a position-level coverage limitation.
    pub order_id: Option<String>,
    pub reason: BuyAttributionIssue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenPositionBuyAttribution {
    pub position_id: String,
    pub token: String,
    pub proven_contributors: Vec<ProvenBuyContributor>,
    pub coverage: BuyAttributionCoverage,
    /// Invalid linked rows and unassigned same-token rows are diagnostics only;
    /// they are not attributed to this generation.
    pub unproven_links: Vec<UnprovenBuyLink>,
}

/// Read-only evidence for a later SELL-routing decision. Absence from the subset
/// is absence of proof, not proof of absence. No per-source inventory allocation,
/// full-history guarantee, or automatic SELL authorization is provided.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionCanaryBuyAttribution {
    NoOpenPosition,
    Open(OpenPositionBuyAttribution),
}
