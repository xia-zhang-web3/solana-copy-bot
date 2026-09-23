//! Explicit offline BUY authority from a first durable admission and a session fence.
//! This never changes the observation-only meaning of the association inbox.
use chrono::{DateTime, Utc};

#[path = "native_buy_capture.rs"]
pub(crate) mod capture;
#[path = "native_buy_verify.rs"]
mod verify;
#[path = "native_buy_promote.rs"]
mod promote;

pub const STATUS: &str = "native_buy_fenced_v1";
pub const SPL_TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeBuyFence {
    pub session: String,
    pub processed_slot: u64,
    pub sampled_at: DateTime<Utc>,
    pub genesis_hash: String,
    pub policy_identity: String,
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
