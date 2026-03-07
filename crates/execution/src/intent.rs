use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::Lamports;
use copybot_storage::CopySignalRow;

use crate::money::sol_to_lamports_ceil;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionSide {
    Buy,
    Sell,
}

impl ExecutionSide {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        }
    }
}

impl TryFrom<&str> for ExecutionSide {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "buy" => Ok(Self::Buy),
            "sell" => Ok(Self::Sell),
            other => Err(anyhow!("unsupported execution side: {other}")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionIntent {
    pub signal_id: String,
    pub leader_wallet: String,
    pub side: ExecutionSide,
    pub token: String,
    pub notional_sol: f64,
    pub signal_ts: DateTime<Utc>,
}

impl ExecutionIntent {
    pub fn notional_lamports(&self) -> Result<Lamports> {
        sol_to_lamports_ceil(self.notional_sol, "execution intent notional_sol")
    }
}

impl TryFrom<CopySignalRow> for ExecutionIntent {
    type Error = anyhow::Error;

    fn try_from(row: CopySignalRow) -> Result<Self> {
        if row.notional_sol <= 0.0 || !row.notional_sol.is_finite() {
            return Err(anyhow!(
                "invalid notional_sol={} for signal {}",
                row.notional_sol,
                row.signal_id
            ));
        }

        let intent = Self {
            signal_id: row.signal_id,
            leader_wallet: row.wallet_id,
            side: ExecutionSide::try_from(row.side.as_str())?,
            token: row.token,
            notional_sol: row.notional_sol,
            signal_ts: row.ts,
        };
        intent.notional_lamports()?;
        Ok(intent)
    }
}
