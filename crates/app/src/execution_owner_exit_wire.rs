//! Owner authority wrapper; common wire extraction preserves its existing scope.
use crate::execution_submit_adapter::ExecutionSubmitRequest;
pub(crate) use crate::execution_usdc_sell_wire::UsdcSellWireProof as OwnerExitWireProof;
use anyhow::{ensure, Context, Result};
const AMOUNT: u64 = 1_167_085;

pub(crate) fn verify(
    request: &ExecutionSubmitRequest,
    payload: &str,
) -> Result<OwnerExitWireProof> {
    ensure!(
        request.signal_id.starts_with("owner-exit:")
            && (request.wallet_id.is_empty() || request.wallet_pubkey == request.wallet_id),
        "owner_exit_wire_request"
    );
    let recovery=request.signal_id.strip_prefix("owner-exit:")
        .is_some_and(copybot_storage_core::source_recovery_exit_id);
    if recovery {
        ensure!(request.metadata.controlled_exit_wire.as_ref().is_some_and(|p|p.matches(request)),
            "source_recovery_wire_authority");
    }
    let amount = if request.signal_id.starts_with("owner-exit:execution-only:") || recovery {
        request
            .metadata
            .quote_in_amount_raw
            .as_deref()
            .context("owner_exit_wire_amount")?
            .parse::<u64>()?
    } else {
        AMOUNT
    };
    crate::execution_usdc_sell_wire::verify(request, payload, amount, true)
}
