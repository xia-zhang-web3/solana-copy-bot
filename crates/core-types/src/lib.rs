use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{fmt, str::FromStr};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope<T> {
    pub event_id: Uuid,
    pub correlation_id: Uuid,
    pub source_component: String,
    pub ts_utc: DateTime<Utc>,
    pub payload: T,
}

impl<T> EventEnvelope<T> {
    pub fn new(source_component: impl Into<String>, payload: T) -> Self {
        let correlation_id = Uuid::new_v4();
        Self {
            event_id: Uuid::new_v4(),
            correlation_id,
            source_component: source_component.into(),
            ts_utc: Utc::now(),
            payload,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapEvent {
    pub wallet: String,
    pub dex: String,
    pub token_in: String,
    pub token_out: String,
    pub amount_in: f64,
    pub amount_out: f64,
    pub signature: String,
    pub slot: u64,
    pub ts_utc: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalSide {
    Buy,
    Sell,
}

impl SignalSide {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        }
    }
}

impl fmt::Display for SignalSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SignalSide {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "buy" => Ok(Self::Buy),
            "sell" => Ok(Self::Sell),
            other => Err(format!("unsupported signal side: {other}")),
        }
    }
}

impl Serialize for SignalSide {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for SignalSide {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopyIntent {
    pub leader_wallet: String,
    pub side: SignalSide,
    pub token: String,
    pub notional_sol: f64,
    pub max_delay_sec: u64,
}

#[derive(Debug, Clone)]
pub struct WalletMetricRow {
    pub wallet_id: String,
    pub window_start: DateTime<Utc>,
    pub pnl: f64,
    pub win_rate: f64,
    pub trades: u32,
    pub closed_trades: u32,
    pub hold_median_seconds: i64,
    pub score: f64,
    pub buy_total: u32,
    pub tradable_ratio: f64,
    pub rug_ratio: f64,
}

#[derive(Debug, Clone)]
pub struct WalletUpsertRow {
    pub wallet_id: String,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct CopySignalRow {
    pub signal_id: String,
    pub wallet_id: String,
    pub side: String,
    pub token: String,
    pub notional_sol: f64,
    pub ts: DateTime<Utc>,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct ExecutionOrderRow {
    pub order_id: String,
    pub signal_id: String,
    pub client_order_id: String,
    pub route: String,
    pub applied_tip_lamports: Option<u64>,
    pub ata_create_rent_lamports: Option<u64>,
    pub network_fee_lamports_hint: Option<u64>,
    pub base_fee_lamports_hint: Option<u64>,
    pub priority_fee_lamports_hint: Option<u64>,
    pub submit_ts: DateTime<Utc>,
    pub confirm_ts: Option<DateTime<Utc>>,
    pub status: String,
    pub err_code: Option<String>,
    pub tx_signature: Option<String>,
    pub simulation_status: Option<String>,
    pub simulation_error: Option<String>,
    pub attempt: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertExecutionOrderPendingOutcome {
    Inserted,
    Duplicate,
}

pub const EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS: &str =
    "execution_submitted_reconcile_pending";

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ExecutionConfirmStateSnapshot {
    pub total_exposure_sol: f64,
    pub token_exposure_sol: f64,
    pub open_positions: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FinalizeExecutionConfirmOutcome {
    Applied(ExecutionConfirmStateSnapshot),
    AlreadyConfirmed,
}

#[derive(Debug, Clone)]
pub struct TokenQualityCacheRow {
    pub mint: String,
    pub holders: Option<u64>,
    pub liquidity_sol: Option<f64>,
    pub token_age_seconds: Option<u64>,
    pub fetched_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct TokenQualityRpcRow {
    pub holders: Option<u64>,
    pub liquidity_sol: Option<f64>,
    pub token_age_seconds: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::SignalSide;

    #[test]
    fn signal_side_serializes_as_canonical_lowercase() {
        assert_eq!(
            serde_json::to_string(&SignalSide::Buy).expect("serialize buy"),
            "\"buy\""
        );
        assert_eq!(
            serde_json::to_string(&SignalSide::Sell).expect("serialize sell"),
            "\"sell\""
        );
    }

    #[test]
    fn signal_side_deserializes_case_insensitively() {
        assert_eq!(
            serde_json::from_str::<SignalSide>("\"buy\"").expect("deserialize buy"),
            SignalSide::Buy
        );
        assert_eq!(
            serde_json::from_str::<SignalSide>("\"Buy\"").expect("deserialize Buy"),
            SignalSide::Buy
        );
        assert_eq!(
            serde_json::from_str::<SignalSide>("\" SELL \"").expect("deserialize padded SELL"),
            SignalSide::Sell
        );
    }

    #[test]
    fn signal_side_rejects_invalid_values() {
        let error =
            serde_json::from_str::<SignalSide>("\"hold\"").expect_err("invalid side should fail");
        assert!(
            error.to_string().contains("unsupported signal side"),
            "unexpected error: {error}"
        );
    }
}
