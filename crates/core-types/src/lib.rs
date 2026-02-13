use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SignalSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopyIntent {
    pub leader_wallet: String,
    pub side: SignalSide,
    pub token: String,
    pub notional_sol: f64,
    pub max_delay_sec: u64,
}
