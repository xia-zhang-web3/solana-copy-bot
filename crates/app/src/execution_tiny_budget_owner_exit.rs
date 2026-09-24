//! Fresh fee and floor proof for a one-shot exit, independent of the expired BUY budget.
use super::super::SubmitState;
use crate::execution_canary_submit_contract::ExecutionTinySubmitGate;
use crate::execution_native_rpc::NativeFundingRpcClient;
use crate::execution_signing_envelope::ExecutionSigningEnvelope;
use crate::execution_submit_adapter::{ExecutionSubmitIntent, ExecutionSubmitRequest,
    RpcExecutionSubmitTransport};
use anyhow::{anyhow, ensure, Result};
use chrono::{DateTime, Utc};
use copybot_storage_core::{OwnerExitIntent, SqliteStore, TinyBudgetClaim};
use std::time::Duration;

pub(super) async fn prepare(
    store: &SqliteStore, request: &ExecutionSubmitRequest,
    signed: &ExecutionSubmitIntent, envelope: &ExecutionSigningEnvelope,
    gate: &ExecutionTinySubmitGate, transport: &RpcExecutionSubmitTransport,
    state: &SubmitState, tick: DateTime<Utc>, owner: &OwnerExitIntent,
) -> Result<(TinyBudgetClaim, DateTime<Utc>)> {
    let config = gate.buy_safety_config.as_ref()
        .ok_or_else(|| anyhow!("owner_exit_config_missing"))?;
    ensure!(gate.allow_rpc_submit && config.canary_tiny_submit_enabled
        && request.wallet_pubkey == owner.wallet
        && gate.execution_wallet_pubkey == owner.wallet,
        "owner_exit_budget_identity");
    let now = crate::execution_canary_safety::risk_clock::decision_time(tick)
        .ok_or_else(|| anyhow!("owner_exit_clock"))?;
    crate::execution_owner_exit_authority::current(config, store, owner, now)?;
    crate::execution_owner_exit_authority::fresh_quote(request, now)?;
    let (message, priority) = crate::execution_priority_fee_wire::decode_priority_fee_message(
        &signed.signed_transaction_base64)?;
    crate::execution_owner_exit_wire::verify(request, &signed.signed_transaction_base64)?;
    let client = NativeFundingRpcClient::new()?;
    let (total_fee, fee_slot) = client.collect_fee_only(
        transport.rpc_endpoint(), Duration::from_millis(gate.submit_timeout_ms),
        &signed.signed_transaction_base64,
    ).await?.bound_fee(&message.binding)?;
    ensure!(total_fee <= 100_000 && priority.total <= owner.max_priority_fee_lamports,
        "owner_exit_fee_cap");
    // The new package has exactly this one inherited BUY and no new BUY
    // authority. Even if the SELL yielded zero SOL, its total wallet cash loss
    // cannot exceed the confirmed BUY debit plus this bounded SELL fee.
    let buy = store.load_execution_canary_receipt_facts(&owner.buy_order_id)?
        .ok_or_else(|| anyhow!("owner_exit_buy_receipt_missing"))?;
    ensure!(buy.tx_signature == owner.buy_receipt_signature
        && buy.wallet_pubkey == owner.wallet && buy.side == "buy"
        && buy.wallet_native_delta.as_i128() < 0,
        "owner_exit_buy_cash_identity");
    let worst_cash_loss = u64::try_from(-buy.wallet_native_delta.as_i128())?
        .checked_add(total_fee)
        .ok_or_else(|| anyhow!("owner_exit_loss_overflow"))?;
    ensure!(worst_cash_loss <= owner.max_daily_loss_lamports,
        "owner_exit_daily_loss_cap");
    crate::execution_tiny_submit_state::unchanged(state, store, request)
        .map_err(|reason| anyhow!(reason))?;
    let now = crate::execution_canary_safety::risk_clock::decision_time(tick)
        .ok_or_else(|| anyhow!("owner_exit_clock"))?;
    crate::execution_owner_exit_authority::current(config, store, owner, now)?;
    crate::execution_owner_exit_authority::fresh_quote(request, now)?;
    crate::execution_priority_fee_proof::validate_submit(store, request, envelope,
        signed, gate.pretrade_max_priority_fee_lamports)?;
    crate::execution_native_floor_policy::verify_submit_payload(request,
        &signed.signed_transaction_base64, gate.pretrade_min_sol_reserve,
        &gate.execution_wallet_pubkey)?;
    let identity = super::super::dispatch::identity(request, signed)?;
    Ok((TinyBudgetClaim {
        experiment_id: owner.run_id.clone(), wallet: owner.wallet.clone(),
        tx_signature: identity.tx_signature,
        message_sha256: identity.message_sha256,
        transaction_sha256: identity.transaction_sha256,
        buy_lamports: Some(0), protected_capital: None,
        total_fee, priority_fee: priority.total, fee_slot,
    }, now))
}
