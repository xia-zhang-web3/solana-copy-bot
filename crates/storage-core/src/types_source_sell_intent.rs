use crate::ProvenBuyContributor;
use chrono::{DateTime, Utc};
use copybot_core_types::SwapEvent;

/// Immutable preparation, not a runnable signal or permission to sell inventory.
/// The event's original amounts are source observations, never owned SELL sizing.
#[derive(Debug, Clone)]
pub struct ExecutionSourceSellIntent {
    pub intent_id: String,
    pub event: SwapEvent,
    pub position_id: String,
    pub buy_witness: ProvenBuyContributor,
    pub buy_execution_wallet: String,
    pub staged_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ExecutionSourceSellReject {
    InvalidSell,
    ObservedEventMismatch,
    NoOwnedPosition,
    GenerationMismatch,
    SellBeforePosition,
    SellBeforeLatestBuy,
    ShadowRiskPresent,
    SignalAlreadyExists,
    SourceNotProven,
    StagedEventConflict,
    WitnessNoLongerProven,
}

#[derive(Debug, Clone)]
pub enum ExecutionSourceSellOutcome {
    Inserted(ExecutionSourceSellIntent),
    Existing(ExecutionSourceSellIntent),
    Rejected(ExecutionSourceSellReject),
}
