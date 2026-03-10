use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::{
    Lamports, COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
};
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
        if self.notional_lamports.as_u64() == 0 {
            return Err(anyhow!(
                "signal {} has zero notional_lamports",
                self.signal_id
            ));
        }
        Ok(self.notional_lamports)
    }

    pub fn from_signal_row(row: CopySignalRow, exact_money_cutover_active: bool) -> Result<Self> {
        Self::from_signal_row_with_policy(row, exact_money_cutover_active, false)
    }

    pub fn from_existing_submitted_signal_row(
        row: CopySignalRow,
        exact_money_cutover_active: bool,
    ) -> Result<Self> {
        Self::from_signal_row_with_policy(row, exact_money_cutover_active, true)
    }

    fn from_signal_row_with_policy(
        row: CopySignalRow,
        exact_money_cutover_active: bool,
        allow_existing_order_legacy_fallback: bool,
    ) -> Result<Self> {
        if row.notional_sol <= 0.0 || !row.notional_sol.is_finite() {
            return Err(anyhow!(
                "invalid notional_sol={} for signal {}",
                row.notional_sol,
                row.signal_id
            ));
        }

        match row.notional_origin.as_str() {
            COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS
            | COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE => {}
            other => {
                return Err(anyhow!(
                    "unsupported notional_origin={} for signal {}",
                    other,
                    row.signal_id
                ));
            }
        }

        let notional_lamports = match row.notional_lamports {
            Some(value) if value.as_u64() > 0 => value,
            Some(_) if allow_existing_order_legacy_fallback => sol_to_lamports_ceil(
                row.notional_sol,
                "execution intent legacy submitted-order notional_sol",
            )?,
            Some(_) => {
                return Err(anyhow!(
                    "signal {} has zero notional_lamports for notional_origin={}",
                    row.signal_id,
                    row.notional_origin
                ));
            }
            None if !exact_money_cutover_active || allow_existing_order_legacy_fallback => {
                sol_to_lamports_ceil(row.notional_sol, "execution intent notional_sol")?
            }
            None => {
                return Err(anyhow!(
                    "post-cutover signal {} missing notional_lamports for notional_origin={}",
                    row.signal_id,
                    row.notional_origin
                ));
            }
        };

        let intent = Self {
            signal_id: row.signal_id,
            leader_wallet: row.wallet_id,
            side: ExecutionSide::try_from(row.side.as_str())?,
            token: row.token,
            notional_sol: row.notional_sol,
            notional_lamports,
            signal_ts: row.ts,
        };
        intent.notional_lamports()?;
        Ok(intent)
    }
}

impl TryFrom<CopySignalRow> for ExecutionIntent {
    type Error = anyhow::Error;

    fn try_from(row: CopySignalRow) -> Result<Self> {
        Self::from_signal_row(row, false)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ExecutionIntent, ExecutionSide, COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE,
        COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
    };
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
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
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
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
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
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
            ts: Utc::now(),
            status: "shadow_recorded".to_string(),
        })
        .expect("exact sidecar should win over overflowing float-to-lamports fallback");

        assert_eq!(intent.side, ExecutionSide::Buy);
        assert_eq!(intent.notional_lamports, super::Lamports::new(100_000_000));
    }

    #[test]
    fn execution_intent_uses_durable_approximate_notional_mirror_without_recomputing_from_float() {
        let intent = ExecutionIntent::from_signal_row(
            CopySignalRow {
                signal_id: "signal-approx".to_string(),
                wallet_id: "wallet-1".to_string(),
                side: "buy".to_string(),
                token: "token-a".to_string(),
                notional_sol: 0.1000000009,
                notional_lamports: Some(super::Lamports::new(100_000_001)),
                notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
                ts: Utc::now(),
                status: "shadow_recorded".to_string(),
            },
            true,
        )
        .expect("stored approximate lamport mirror should satisfy post-cutover intent");

        assert_eq!(intent.notional_lamports, super::Lamports::new(100_000_001));
    }

    #[test]
    fn execution_intent_rejects_post_cutover_signal_without_durable_notional_lamports() {
        let error = ExecutionIntent::from_signal_row(
            CopySignalRow {
                signal_id: "signal-4".to_string(),
                wallet_id: "wallet-1".to_string(),
                side: "buy".to_string(),
                token: "token-a".to_string(),
                notional_sol: 0.1,
                notional_lamports: None,
                notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
                ts: Utc::now(),
                status: "shadow_recorded".to_string(),
            },
            true,
        )
        .expect_err("post-cutover signal without lamport mirror must fail closed");

        assert!(
            error.to_string().contains("missing notional_lamports"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn execution_intent_rejects_post_cutover_signal_with_zero_durable_notional_lamports() {
        let error = ExecutionIntent::from_signal_row(
            CopySignalRow {
                signal_id: "signal-zero".to_string(),
                wallet_id: "wallet-1".to_string(),
                side: "buy".to_string(),
                token: "token-a".to_string(),
                notional_sol: 0.1,
                notional_lamports: Some(super::Lamports::ZERO),
                notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
                ts: Utc::now(),
                status: "shadow_recorded".to_string(),
            },
            true,
        )
        .expect_err("post-cutover signal with zero lamport mirror must fail closed");

        assert!(
            error.to_string().contains("zero notional_lamports"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn execution_intent_allows_existing_submitted_signal_to_fallback_to_legacy_notional() {
        let intent = ExecutionIntent::from_existing_submitted_signal_row(
            CopySignalRow {
                signal_id: "signal-rescue".to_string(),
                wallet_id: "wallet-1".to_string(),
                side: "buy".to_string(),
                token: "token-a".to_string(),
                notional_sol: 0.1,
                notional_lamports: None,
                notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
                ts: Utc::now(),
                status: "execution_submitted".to_string(),
            },
            true,
        )
        .expect("existing submitted order should preserve confirm/reconcile pollability");

        assert_eq!(intent.notional_lamports, super::Lamports::new(100_000_000));
    }
}
