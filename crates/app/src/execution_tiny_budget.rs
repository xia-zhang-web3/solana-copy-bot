//! Final tiny-only policy; all I/O completes before the synchronous durable claim.
#[path = "execution_tiny_budget_amount.rs"]
mod amount;
#[path = "execution_tiny_budget_owner_exit.rs"]
mod owner_exit;
use super::SubmitState;
use crate::execution_canary_submit_contract::ExecutionTinySubmitGate;
use crate::execution_native_rpc::NativeFundingRpcClient;
use crate::execution_signing_envelope::ExecutionSigningEnvelope;
use crate::execution_submit_adapter::{
    ExecutionSubmitIntent, ExecutionSubmitRequest, RpcExecutionSubmitTransport,
};
use anyhow::{anyhow, ensure, Result};
use chrono::{DateTime, Utc};
use copybot_storage_core::{SqliteStore, TinyBudgetClaim};
use std::{sync::OnceLock, time::Duration};
static CLIENT: OnceLock<Result<NativeFundingRpcClient, ()>> = OnceLock::new();

pub(crate) async fn prepare(
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
    intent: &ExecutionSubmitIntent,
    envelope: &ExecutionSigningEnvelope,
    gate: &ExecutionTinySubmitGate,
    transport: &RpcExecutionSubmitTransport,
    state: &SubmitState,
    tick: DateTime<Utc>,
    native: Option<&crate::execution_canary_route::NativeBuyGuard>,
) -> Result<(TinyBudgetClaim, DateTime<Utc>)> {
    if let SubmitState::OwnerExit { intent: owner, .. } = state {
        return owner_exit::prepare(store, request, intent, envelope, gate,
            transport, state, tick, owner).await;
    }
    if matches!(state, SubmitState::Owned(_)) {
        return crate::execution_owned_sell_prepare::submit::guard::budget(
            store, request, intent, envelope, gate, transport,
        )
        .await;
    }
    let config = gate
        .buy_safety_config
        .as_ref()
        .ok_or_else(|| anyhow!("tiny_budget_config_missing"))?;
    let policy = &config.tiny_experiment;
    policy.validate(&config.canary_wallet_pubkey)?;
    let id = policy
        .id
        .as_deref()
        .ok_or_else(|| anyhow!("tiny_budget_inactive"))?;
    ensure!(
        config.canary_tiny_submit_enabled
            && gate.allow_rpc_submit
            && config.canary_wallet_pubkey == request.wallet_pubkey
            && gate.execution_wallet_pubkey == request.wallet_pubkey,
        "tiny_budget_identity"
    );
    let activation = crate::execution_canary_safety::risk_clock::decision_time(tick)
        .ok_or_else(|| anyhow!("tiny_budget_clock"))?;
    if let SubmitState::OwnerTechnicalBuy { intent, .. } = state {
        crate::execution_owner_buy_authority::current(config, store, intent, activation)?;
        crate::execution_owner_buy_authority::fresh_quote(request, activation)?;
    }
    if policy.activate && !crate::execution_native_floor_policy::protected::enabled(config) {
        store.activate_tiny_experiment(id, &request.wallet_pubkey, activation)?;
    }
    let experiment = store
        .load_tiny_experiment(activation)?
        .ok_or_else(|| anyhow!("tiny_budget_inactive"))?;
    ensure!(
        experiment.id == id && experiment.wallet == request.wallet_pubkey,
        "tiny_budget_identity"
    );
    ensure!(experiment.state == "active", "tiny_budget_stopped");
    let (message, priority) = crate::execution_priority_fee_wire::decode_priority_fee_message(
        &intent.signed_transaction_base64,
    )?;
    let owner_wire = if crate::execution_owner_buy_wire::required(request) {
        Some(crate::execution_owner_buy_wire::verify(
            request, &intent.signed_transaction_base64,
        )?)
    } else { None };
    #[cfg(test)]
    let mocked = native.and_then(|guard| guard.mock_io());
    #[cfg(not(test))]
    let mocked: Option<&()> = None;
    let (total_fee, fee_slot) = if mocked.is_some() {
        #[cfg(test)]
        {
            let mock = mocked.expect("mock checked");
            ensure!(
                message.binding.message_sha256 == mock.expected_message_sha256,
                "native_buy_mock_fee_binding"
            );
            mock.count(|counts| counts.fee += 1);
            tokio::task::yield_now().await;
            (mock.fee_lamports, mock.fee_slot)
        }
        #[cfg(not(test))]
        unreachable!()
    } else {
        let client = CLIENT
            .get_or_init(|| NativeFundingRpcClient::new().map_err(|_| ()))
            .as_ref()
            .map_err(|_| anyhow!("tiny_budget_rpc_client"))?;
        client
            .collect_fee_only(
                transport.rpc_endpoint(),
                Duration::from_millis(gate.submit_timeout_ms),
                &intent.signed_transaction_base64,
            )
            .await?
            .bound_fee(&message.binding)?
    };
    if let Some(guard) = native {
        ensure!(guard.check(store)?, "native_buy_decision_changed");
    }
    // Re-read authoritative guards after the new await. No SQLite transaction is held.
    crate::execution_tiny_submit_state::unchanged(state, store, request)
        .map_err(|reason| anyhow!(reason))?;
    if let SubmitState::OwnerTechnicalBuy { intent, .. } = state {
        let now = crate::execution_canary_safety::risk_clock::decision_time(tick)
            .ok_or_else(|| anyhow!("tiny_budget_clock"))?;
        crate::execution_owner_buy_authority::current(
            config, store, intent, now,
        )?;
        crate::execution_owner_buy_authority::fresh_quote(request, now)?;
    }
    crate::execution_source_sell_guard::request(
        store,
        request,
        &[copybot_storage_core::EXECUTION_STATUS_CANARY_SIMULATED],
    )?;
    crate::execution_priority_fee_proof::validate_submit(
        store,
        request,
        envelope,
        intent,
        gate.pretrade_max_priority_fee_lamports,
    )?;
    crate::execution_native_floor_policy::verify_submit_payload(
        request,
        &intent.signed_transaction_base64,
        gate.pretrade_min_sol_reserve,
        &gate.execution_wallet_pubkey,
    )?;
    if request.side.eq_ignore_ascii_case("buy") {
        let safety =
            crate::execution_canary_safety::live_pre_submit_safety_snapshot(config, store, tick)?;
        ensure!(
            safety.blocked_reason.is_none(),
            "{}",
            safety.blocked_reason.unwrap_or("tiny_budget_safety")
        );
    }
    let identity = super::dispatch::identity(request, intent)?;
    let protected =
        crate::execution_native_floor_policy::protected::current_gate(store, gate, request, tick)?;
    let (buy_lamports, protected_capital) = if let Some(proof) = protected {
        let floor = proof.floor(&request.wallet_pubkey, gate.pretrade_min_sol_reserve)?;
        proof.verify_floor(
            &intent.signed_transaction_base64,
            crate::execution_pumpswap_accounts::parse_pubkey(
                &request.wallet_pubkey,
                "tiny_capital_wallet",
            )?,
            floor,
        )?;
        (owner_wire.as_ref().map(|proof| proof.decoded_amount_lamports()),
            Some(proof.claim(floor)?))
    } else if request.side.eq_ignore_ascii_case("buy") {
        (Some(amount::buy(&message, &request.wallet_pubkey)?), None)
    } else {
        (Some(0), None)
    };
    let now = crate::execution_canary_safety::risk_clock::decision_time(tick)
        .ok_or_else(|| anyhow!("tiny_budget_clock"))?;
    // Persist a stop even if the subsequent claim refuses (its transaction rolls back).
    store.load_tiny_experiment(now)?;
    Ok((
        TinyBudgetClaim {
            experiment_id: id.into(),
            wallet: request.wallet_pubkey.clone(),
            tx_signature: identity.tx_signature,
            message_sha256: identity.message_sha256,
            transaction_sha256: identity.transaction_sha256,
            buy_lamports,
            protected_capital,
            total_fee,
            priority_fee: priority.total,
            fee_slot,
        },
        now,
    ))
}
