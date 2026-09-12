//! Current Shadow evidence is separate from execution contributors and never authorizes SELL.
use crate::{
    association_sell_preparation::{AnchorEvidence, Check, Reason},
    shadow_lot_origin::ShadowLotOrigin,
};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShadowScan {
    Complete,
    Unknown(Reason),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShadowRelation {
    BeforeSell,
    AfterSell,
    Unknown(Reason),
    Conflict(Reason),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShadowLotEvidence {
    pub lot_id: i64,
    pub qty_bits: u64,
    pub qty_raw: Option<String>,
    pub qty_decimals: Option<u8>,
    pub risk_context: String,
    pub origin: Option<ShadowLotOrigin>,
    pub anchor: Option<AnchorEvidence>,
    pub origin_to_sell: Check,
    pub sell_to_origin: Check,
    pub relation: ShadowRelation,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShadowEvidence {
    /// Complete refers only to this wallet/token's open lot scan, not provider coverage.
    pub scan: ShadowScan,
    pub lots: Vec<ShadowLotEvidence>,
}
