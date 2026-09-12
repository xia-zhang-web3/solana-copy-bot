use anyhow::{ensure, Result};
use copybot_core_types::{Lamports, SignedLamports};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailedExpenseCoverage {
    Known,
    Missing,
    Invalid,
    Unsupported,
}

/// Only a validated failed getTransaction supplies these facts; success facts stay separate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailedTransactionFacts {
    pub tx_signature: String,
    pub wallet: String,
    pub slot: u64,
    pub commitment: String,
    pub transaction_error: serde_json::Value,
    pub transaction_fee_lamports: Option<String>,
    pub fee_coverage: FailedExpenseCoverage,
    pub payer: Option<String>,
    pub payer_coverage: FailedExpenseCoverage,
    pub wallet_native_pre_lamports: Option<String>,
    pub wallet_native_post_lamports: Option<String>,
    pub native_coverage: FailedExpenseCoverage,
}
impl FailedTransactionFacts {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            matches!(self.commitment.as_str(), "confirmed" | "finalized")
                && super::evidence::proven_failure(&self.transaction_error),
            "invalid failed receipt evidence"
        );
        ensure!(
            !self.tx_signature.is_empty() && !self.wallet.is_empty(),
            "failed receipt identity missing"
        );
        ensure!(
            self.transaction_fee_lamports.is_some()
                == (self.fee_coverage == FailedExpenseCoverage::Known),
            "failed fee coverage mismatch"
        );
        if let Some(v) = &self.transaction_fee_lamports {
            lamports(v)?;
        }
        ensure!(
            self.payer.is_some() == (self.payer_coverage == FailedExpenseCoverage::Known),
            "failed payer coverage mismatch"
        );
        ensure!(
            self.payer.as_ref().is_none_or(|v| !v.is_empty()),
            "empty failed payer"
        );
        let native = self.native_coverage == FailedExpenseCoverage::Known;
        ensure!(
            self.wallet_native_pre_lamports.is_some() == native
                && self.wallet_native_post_lamports.is_some() == native,
            "failed native coverage mismatch"
        );
        if native {
            self.native_delta()?;
        }
        Ok(())
    }
    pub fn wallet_fee(&self) -> Result<Option<Lamports>> {
        self.transaction_fee_lamports
            .as_ref()
            .zip(self.payer.as_ref())
            .map(|(fee, payer)| {
                Ok(if payer == &self.wallet {
                    lamports(fee)?
                } else {
                    Lamports::new(0)
                })
            })
            .transpose()
    }
    pub fn native_delta(&self) -> Result<Option<SignedLamports>> {
        self.wallet_native_pre_lamports
            .as_ref()
            .zip(self.wallet_native_post_lamports.as_ref())
            .map(|(pre, post)| {
                Ok(SignedLamports::new(
                    i128::from(lamports(post)?.as_u64()) - i128::from(lamports(pre)?.as_u64()),
                ))
            })
            .transpose()
    }
    pub fn unexplained_delta(&self) -> Result<Option<SignedLamports>> {
        self.native_delta()?
            .zip(self.wallet_fee()?)
            .map(|(delta, fee)| {
                delta
                    .checked_add(SignedLamports::new(i128::from(fee.as_u64())))
                    .ok_or_else(|| anyhow::anyhow!("failed receipt residual overflow"))
            })
            .transpose()
    }
}
pub(super) fn lamports(value: &str) -> Result<Lamports> {
    let raw: u64 = value.parse()?;
    ensure!(
        raw.to_string() == value,
        "noncanonical failed expense lamports"
    );
    Ok(Lamports::new(raw))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailedExpenseTask {
    pub order_id: String,
    pub tx_signature: String,
    pub attempt: u32,
    pub route: String,
    pub wallet: String,
    pub token: String,
    pub side: String,
    /// Immutable original order submit time; receipt retrieval never changes its report window.
    pub operation_at: String,
    pub detected_at: String,
    /// Time work was durably reserved; a crash may precede actual receipt I/O.
    pub last_attempt_at: Option<String>,
    pub failure_source: String,
    pub failure_error_json: String,
    pub commitment: String,
    pub slot: Option<u64>,
    pub status: String,
    pub reason: String,
}
