//! One closed RPC observation bound to exact signed bytes, without account/rent I/O.
use super::{
    request::{observe, Method},
    response, NativeFundingRpcClient,
};
use crate::execution_transaction_wire::MessageBinding;
use anyhow::{anyhow, ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::json;
use std::time::Duration;

pub(crate) struct TotalFeeObservation {
    binding: MessageBinding,
    total: u64,
    slot: u64,
}
impl TotalFeeObservation {
    pub(crate) fn bound_fee(&self, binding: &MessageBinding) -> Result<(u64, u64)> {
        ensure!(&self.binding == binding, "tiny_budget_fee_binding");
        Ok((self.total, self.slot))
    }
}
impl NativeFundingRpcClient {
    pub(crate) async fn collect_fee_only(
        &self,
        endpoint: &str,
        timeout: Duration,
        payload: &str,
    ) -> Result<TotalFeeObservation> {
        ensure!(
            !timeout.is_zero() && timeout <= Duration::from_secs(30),
            "native_rpc_timeout_bounds"
        );
        ensure!(payload.len() <= 1644, "native_rpc_payload_too_large");
        let binding =
            crate::execution_transaction_wire::decode_message(payload, |_| Ok(()))?.binding;
        let url = reqwest::Url::parse(endpoint).map_err(|_| anyhow!("native_rpc_endpoint"))?;
        ensure!(
            matches!(url.scheme(), "http" | "https"),
            "native_rpc_endpoint"
        );
        let fee = tokio::time::timeout(
            timeout,
            observe(
                &self.http,
                endpoint,
                timeout,
                Method::Fee,
                json!([STANDARD.encode(&binding.message_bytes),{"commitment":"confirmed"}]),
                None,
                response::fee,
            ),
        )
        .await
        .map_err(|_| anyhow!("native_rpc_timeout"))??;
        Ok(TotalFeeObservation {
            binding,
            total: fee
                .value
                .ok_or_else(|| anyhow!("tiny_budget_fee_unknown"))?,
            slot: fee.slot,
        })
    }
}
