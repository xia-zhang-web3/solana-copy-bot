//! The only new await before BUY send; all post-I/O decisions below are synchronous.
use crate::execution_canary_submit_contract::{
    record_submit_plan_failure, ExecutionSubmitPlanOutcome, ExecutionTinySubmitGate,
};
use crate::execution_signing_envelope::ExecutionSigningEnvelope;
use crate::execution_submit_adapter::{
    ExecutionSubmitIntent, ExecutionSubmitRequest, RpcExecutionSubmitTransport,
};
use crate::execution_tiny_submit_state::{reject, unchanged, SubmitState};
use anyhow::{anyhow, ensure};
use chrono::{DateTime, Utc};
use copybot_storage_core::SqliteStore;

pub(crate) async fn before_send(
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
    envelope: &ExecutionSigningEnvelope,
    intent: &ExecutionSubmitIntent,
    gate: &ExecutionTinySubmitGate,
    transport: &RpcExecutionSubmitTransport,
    state: &SubmitState,
    tick_at: DateTime<Utc>,
) -> Option<ExecutionSubmitPlanOutcome> {
    before_send_guarded(
        store, request, envelope, intent, gate, transport, state, tick_at, None,
    )
    .await
}

pub(crate) async fn before_send_guarded(
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
    envelope: &ExecutionSigningEnvelope,
    intent: &ExecutionSubmitIntent,
    gate: &ExecutionTinySubmitGate,
    transport: &RpcExecutionSubmitTransport,
    state: &SubmitState,
    tick_at: DateTime<Utc>,
    native: Option<&crate::execution_canary_route::NativeBuyGuard>,
) -> Option<ExecutionSubmitPlanOutcome> {
    let owner_exit = request.signal_id.starts_with("owner-exit:")
        && request.side.eq_ignore_ascii_case("sell");
    if !request.side.eq_ignore_ascii_case("buy") && !owner_exit {
        return None;
    }
    if owner_exit {
        return owner_exit_before_send(store, request, envelope, intent, gate,
            transport, state, tick_at).await;
    }
    let result = Box::pin(async {
        let config = gate
            .buy_safety_config
            .as_ref()
            .ok_or_else(|| anyhow!("initial_sol_policy_unavailable"))?;
        ensure!(
            config.canary_tiny_submit_enabled,
            "initial_sol_policy_unavailable"
        );
        let protected = crate::execution_native_floor_policy::protected::current(
            store, config, request, tick_at,
        )?;
        let reserve = match &protected {
            Some(proof) => {
                proof.floor(&gate.execution_wallet_pubkey, gate.pretrade_min_sol_reserve)?
            }
            None => crate::execution_native_floor_policy::reserve_lamports(
                gate.pretrade_min_sol_reserve,
            )?,
        };
        let wallet = crate::execution_pumpswap_accounts::parse_pubkey(
            &gate.execution_wallet_pubkey,
            "initial_sol_wallet",
        )?;
        // Keep the collector future out of the already nested daemon retry/tick stack.
        #[cfg(test)]
        if let Some(mock) = native.and_then(|guard| guard.mock_io()) {
            mock.count(|counts| counts.initial_sol += 1);
            tokio::task::yield_now().await;
            return crate::execution_initial_sol::check_with_policy(
                &intent.signed_transaction_base64,
                wallet,
                reserve,
                &mock.initial_sol,
                protected.as_ref(),
            );
        }
        Box::pin(crate::execution_initial_sol::collect_with_policy(
            transport.rpc_endpoint(),
            gate.submit_timeout_ms,
            &intent.signed_transaction_base64,
            wallet,
            reserve,
            protected.as_ref(),
        ))
        .await
    })
    .await;
    // Applies equally to success and collector error: never fail/erase a changed order.
    if native.is_some_and(|guard| !guard.check(store).unwrap_or(false)) {
        return Some(refuse(request, "native_buy_decision_changed"));
    }
    if let Err(reason) = unchanged(state, store, request) {
        return Some(refuse(request, reason));
    }
    if let Some(config) = &gate.buy_safety_config {
        match crate::execution_canary_safety::live_pre_submit_safety_snapshot(
            config, store, tick_at,
        ) {
            Ok(safety) => {
                if let Some(reason) = safety.blocked_reason {
                    return Some(refuse(request, reason));
                }
            }
            Err(_) => return Some(refuse(request, "initial_sol_safety_unavailable")),
        }
    }
    let result = result.and_then(|_| {
        crate::execution_native_floor_policy::protected::current_gate(
            store, gate, request, tick_at,
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
        )
    });
    match result {
        Ok(()) => None,
        Err(error) => Some(safe_failure(store, request, tick_at, error)),
    }
}

async fn owner_exit_before_send(
    store: &SqliteStore, request: &ExecutionSubmitRequest,
    envelope: &ExecutionSigningEnvelope, intent: &ExecutionSubmitIntent,
    gate: &ExecutionTinySubmitGate, transport: &RpcExecutionSubmitTransport,
    state: &SubmitState, tick_at: DateTime<Utc>,
) -> Option<ExecutionSubmitPlanOutcome> {
    let result = async {
        let config = gate.buy_safety_config.as_ref()
            .ok_or_else(|| anyhow!("owner_exit_policy_unavailable"))?;
        let exit = crate::execution_owner_exit_authority::request(store, request,
            &[copybot_storage_core::EXECUTION_STATUS_CANARY_SIMULATED])?
            .ok_or_else(|| anyhow!("owner_exit_intent_missing"))?;
        let check = || crate::execution_owner_exit_authority::current(config, store,
            &exit, crate::execution_canary_safety::risk_clock::decision_time(tick_at)
                .ok_or_else(|| anyhow!("owner_exit_clock"))?);
        check()?;
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never()).build()?;
        crate::execution_owner_exit_quote::verify_balance(&http, config, &exit, &check).await?;
        let body: serde_json::Value = http.post(transport.rpc_endpoint())
            .json(&serde_json::json!({"jsonrpc":"2.0","id":"owner-exit-native",
                "method":"getBalance","params":[exit.wallet,{"commitment":"confirmed"}]}))
            .timeout(std::time::Duration::from_millis(gate.submit_timeout_ms.max(1)))
            .send().await?.error_for_status()?.json().await?;
        check()?;
        ensure!(body["jsonrpc"] == "2.0" && body["id"] == "owner-exit-native"
            && body.get("error").is_none()
            && body["result"]["context"]["slot"].as_u64().is_some_and(|v| v > 0),
            "owner_exit_native_rpc");
        let balance = body["result"]["value"].as_u64()
            .ok_or_else(|| anyhow!("owner_exit_native_balance_missing"))?;
        let rent_body: serde_json::Value = http.post(transport.rpc_endpoint())
            .json(&serde_json::json!({"jsonrpc":"2.0","id":"owner-exit-rent",
                "method":"getMinimumBalanceForRentExemption","params":[165]}))
            .timeout(std::time::Duration::from_millis(gate.submit_timeout_ms.max(1)))
            .send().await?.error_for_status()?.json().await?;
        check()?;
        ensure!(rent_body["jsonrpc"] == "2.0" && rent_body["id"] == "owner-exit-rent"
            && rent_body.get("error").is_none(), "owner_exit_rent_rpc");
        let rent = rent_body["result"].as_u64()
            .ok_or_else(|| anyhow!("owner_exit_rent_unknown"))?;
        // The signed route is a USDC input. Reserve a complete transaction-fee
        // bound and one classic WSOL ATA rent even if that ATA already exists.
        let needed = exit.min_reserve_lamports
            .checked_add(copybot_storage_core::TINY_TRANSACTION_FEE)
            .ok_or_else(|| anyhow!("owner_exit_native_overflow"))?
            .checked_add(rent)
            .ok_or_else(|| anyhow!("owner_exit_native_overflow"))?;
        ensure!(balance >= needed, "owner_exit_native_floor_insufficient");
        check()?;
        Ok::<(), anyhow::Error>(())
    }.await;
    if let Err(reason) = unchanged(state, store, request) {
        return Some(refuse(request, reason));
    }
    let result = result.and_then(|_| {
        crate::execution_priority_fee_proof::validate_submit(store, request, envelope,
            intent, gate.pretrade_max_priority_fee_lamports)?;
        crate::execution_native_floor_policy::verify_submit_payload(request,
            &intent.signed_transaction_base64, gate.pretrade_min_sol_reserve,
            &gate.execution_wallet_pubkey)
    });
    match result { Ok(()) => None,
        Err(error) => Some(safe_failure(store, request, tick_at, error)) }
}

fn safe_failure(
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
    now: DateTime<Utc>,
    error: anyhow::Error,
) -> ExecutionSubmitPlanOutcome {
    record_submit_plan_failure(store, request, now, error.to_string())
        .unwrap_or_else(|_| refuse(request, "initial_sol_failure_record_unavailable"))
}

fn refuse(request: &ExecutionSubmitRequest, reason: &'static str) -> ExecutionSubmitPlanOutcome {
    let mut outcome = reject(reason);
    outcome.pre_submit_refusal = Some(
        crate::execution_submit_refusal::PreSubmitRefusal::after_collection(
            &request.order_id,
            reason,
        ),
    );
    outcome
}
