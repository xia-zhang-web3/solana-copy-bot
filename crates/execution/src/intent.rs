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
    pub notional_lamports: Lamports,
    pub signal_ts: DateTime<Utc>,
}

impl ExecutionIntent {
    pub fn notional_lamports(&self) -> Result<Lamports> {
        Ok(self.notional_lamports)
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
            notional_lamports: match row.notional_lamports {
                Some(value) => value,
                None => sol_to_lamports_ceil(row.notional_sol, "execution intent notional_sol")?,
            },
            signal_ts: row.ts,
        };
        intent.notional_lamports()?;
        Ok(intent)
    }
}

#[cfg(test)]
mod tests {
    use super::{ExecutionIntent, ExecutionSide};
    use chrono::Utc;
    use copybot_storage::CopySignalRow;

    #[test]
    fn execution_intent_prefers_exact_copy_signal_notional_when_present() {
        let intent = ExecutionIntent::try_from(CopySignalRow {
            signal_id: "signal-1".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.1000000009,
            notional_lamports: Some(super::Lamports::new(100_000_000)),
            ts: Utc::now(),
            status: "shadow_recorded".to_string(),
        })
        .expect("intent should parse");

        assert_eq!(intent.side, ExecutionSide::Buy);
        assert_eq!(intent.notional_lamports, super::Lamports::new(100_000_000));
    }

    #[test]
    fn execution_intent_derives_exact_notional_when_legacy_signal_has_none() {
        let intent = ExecutionIntent::try_from(CopySignalRow {
            signal_id: "signal-2".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "sell".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.1,
            notional_lamports: None,
            ts: Utc::now(),
            status: "shadow_recorded".to_string(),
        })
        .expect("intent should parse");

        assert_eq!(intent.side, ExecutionSide::Sell);
        assert_eq!(intent.notional_lamports, super::Lamports::new(100_000_000));
    }

    #[test]
    fn execution_intent_uses_exact_notional_even_when_float_mirror_overflows_conversion() {
        let intent = ExecutionIntent::try_from(CopySignalRow {
            signal_id: "signal-3".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: (u64::MAX as f64) / 1_000_000_000.0,
            notional_lamports: Some(super::Lamports::new(100_000_000)),
            ts: Utc::now(),
            status: "shadow_recorded".to_string(),
        })
        .expect("exact sidecar should win over overflowing float-to-lamports fallback");

        assert_eq!(intent.side, ExecutionSide::Buy);
        assert_eq!(intent.notional_lamports, super::Lamports::new(100_000_000));
    }
}
