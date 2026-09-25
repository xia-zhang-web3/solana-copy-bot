//! Explicit offline BUY authority from a first durable admission and a session fence.
//! This never changes the observation-only meaning of the association inbox.
use chrono::{DateTime, Utc};

#[path = "native_buy_capture.rs"]
pub(crate) mod capture;
#[path = "native_buy_verify.rs"]
mod verify;
#[path = "native_buy_promote.rs"]
mod promote;
#[path = "native_buy_cohort.rs"]
pub(crate) mod cohort;
pub(crate) use promote::activation_current;

pub const STATUS: &str = "native_buy_fenced_v1";
pub const SPL_TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub const CLASSIC_SPL_MINT_POLICY: &str = "classic_spl_mint_v1";

/// Preselected, one-BUY test authority. This never represents Discovery GREEN.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TechnicalCohortAuthority {
    pub run_id: String,
    pub wallet_ids: Vec<String>,
    pub mint_policy: String,
    pub activated_at: DateTime<Utc>,
    pub deadline: DateTime<Utc>,
    pub max_buy_count: u32,
    pub policy_identity: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeBuyFence {
    pub session: String,
    pub processed_slot: u64,
    pub sampled_at: DateTime<Utc>,
    pub genesis_hash: String,
    pub policy_identity: String,
}

/// First-activation identity, checked under the same SQLite write lock as the
/// protected native anchor. Constructed only after the runner's external checks.
#[derive(Debug, Clone)]
pub struct NativeBuyActivationBinding {
    pub signal_id: String,
    pub decision_id: String,
    pub policy_identity: String,
    pub max_age_seconds: u64,
    pub order_id: String,
    pub client_order_id: String,
    pub attempt: u32,
    pub route: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeBuyPending {
    pub signature: String,
    pub slot: u64,
    pub wallet: String,
    pub mint: String,
    pub first_session: String,
    pub decision_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeBuyCandidate {
    pub signature: String,
    pub signal_id: String,
    pub decision_id: String,
    pub wallet: String,
    pub mint: String,
    pub slot: u64,
    pub amount_lamports: u64,
    pub admitted_at: DateTime<Utc>,
}

/// Price operands pinned in the first durable native admission. This is an
/// availability-bound source observation, never a fabricated source UTC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeBuySourceAmounts {
    pub amount_in_lamports: u64,
    pub amount_out_raw: u64,
    pub amount_out_decimals: u8,
}
