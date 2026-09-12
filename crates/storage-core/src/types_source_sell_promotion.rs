use crate::ExecutionSourceSellReject;
use chrono::{DateTime, Utc};

/// Durable association, not fresh permission to execute or restart an old signal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionSourceSellPromotion {
    pub signal_id: String,
    pub intent_id: String,
    pub promoted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionSourceSellPromotionReject {
    StagedMissing,
    BindingConflict,
    SignalMissing,
    SignalConflict,
    SignalAlreadyExists,
    Validation(ExecutionSourceSellReject),
}

#[derive(Debug, Clone)]
pub enum ExecutionSourceSellPromotionOutcome {
    Inserted(ExecutionSourceSellPromotion),
    Existing(ExecutionSourceSellPromotion),
    Rejected(ExecutionSourceSellPromotionReject),
}
