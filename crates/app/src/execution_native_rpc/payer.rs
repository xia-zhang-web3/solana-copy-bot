//! Closed ordinary native payer observation for first protected-capital preparation.
use super::{
    request::{observe, Method},
    response,
    types::AccountObservation,
    NativeFundingRpcClient,
};
use crate::execution_solana_tx::PubkeyBytes;
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use serde_json::json;
use std::time::Duration;

pub(crate) struct PayerObservation {
    wallet: PubkeyBytes,
    lamports: u64,
    slot: u64,
    observed_at: DateTime<Utc>,
}
impl PayerObservation {
    pub(crate) fn bound(&self, wallet: PubkeyBytes) -> Result<(u64, u64, DateTime<Utc>)> {
        ensure!(self.wallet == wallet, "tiny_capital_payer_binding");
        Ok((self.lamports, self.slot, self.observed_at))
    }
}
impl NativeFundingRpcClient {
    pub(crate) async fn collect_payer(
        &self,
        endpoint: &str,
        timeout: Duration,
        wallet: PubkeyBytes,
    ) -> Result<PayerObservation> {
        ensure!(
            !timeout.is_zero() && timeout <= Duration::from_secs(30),
            "native_rpc_timeout_bounds"
        );
        let rows = observe(&self.http, endpoint, timeout, Method::Accounts,
            json!([[bs58::encode(wallet).into_string()], {"encoding":"base64","commitment":"confirmed"}]),
            None, |value| response::accounts(value, &[wallet])).await?;
        ensure!(
            rows.timing.elapsed <= timeout && rows.timing.completed_at >= rows.timing.started_at,
            "tiny_capital_observation_time"
        );
        let payer = rows.value.first().context("tiny_capital_payer_missing")?;
        ensure!(payer.pubkey == wallet, "tiny_capital_payer_binding");
        let lamports = match &payer.account {
            AccountObservation::Present {
                lamports,
                owner_program,
                executable,
                data,
            } if *owner_program == [0; 32] && !executable && data.is_empty() => *lamports,
            _ => anyhow::bail!("tiny_capital_payer_unavailable"),
        };
        Ok(PayerObservation {
            wallet,
            lamports,
            slot: rows.slot,
            observed_at: rows.timing.completed_at.into(),
        })
    }
}
