//! Quote-only DTOs: no signal timestamp, permit, price/PnL or execution selector ID.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuoteBinding {
    pub version: u8,
    pub intent_id: String,
    pub policy: String,
    pub position_id: String,
    pub position_opened_ts: String,
    pub source_signature: String,
    pub source_wallet: String,
    pub mint: String,
    pub output_mint: String,
    pub side: String,
    pub provider: String,
    pub endpoint: String,
    pub raw: u64,
    pub decimals: u8,
    /// Exact compact snapshot of validated references, order, financial/Shadow state.
    /// Anchor payloads remain in the original inbox; their identity is revalidated there.
    pub snapshot_version: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuoteOutcome {
    /// Binding and HTTP clocks checked at completion. Every later use must revalidate.
    Current,
    Stale,
    Unknown,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuoteObservation {
    pub version: u8,
    pub binding: Option<QuoteBinding>,
    pub outcome: QuoteOutcome,
    pub reason: Option<String>,
    pub http_started: Option<DateTime<Utc>>,
    pub http_response: Option<DateTime<Utc>>,
    /// Local successful body/decode completion; http_response still means headers.
    #[serde(default)]
    pub quote_response_available_ts: Option<DateTime<Utc>>,
    pub http_ended: DateTime<Utc>,
    pub response_in_raw: Option<String>,
    pub response_out_raw: Option<String>,
    pub response_sha256: Option<String>,
    /// Unproven event clock and event-to-HTTP delay stay Unknown.
    pub event_time: Option<DateTime<Utc>>,
    pub event_delay_ns: Option<u64>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuoteClaim {
    pub intent_id: String,
    pub attempt: i64,
    pub owner: String,
    pub binding: QuoteBinding,
    pub lease_until: DateTime<Utc>,
}
#[derive(Debug)]
pub enum QuoteClaimStep {
    Empty,
    Skipped,
    CapacityRefused(QuoteCapacityRefusal),
    Claimed(QuoteClaim),
}

/// Can tighten, never raise the independent production quote retention limits.
#[derive(Debug, Clone, Copy)]
pub struct QuoteCapacity {
    pub count: usize,
    pub bytes: usize,
}
impl QuoteCapacity {
    pub const PRODUCTION: Self = Self {
        count: 4096,
        bytes: 16 << 20,
    };
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuoteCapacityRefusal {
    pub intent_id: String,
    pub new_row: bool,
    pub dimension: String,
    pub projected_count: usize,
    pub projected_bytes: usize,
}
